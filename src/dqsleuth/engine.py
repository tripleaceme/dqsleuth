"""Read-only investigation engine built on an in-memory DuckDB connection.

Everything the MCP tools do goes through :class:`Workspace`. It never writes to a
source: files are exposed as views, DuckDB databases and Postgres are attached
``READ_ONLY``, and free-form SQL is rejected unless every statement is a SELECT.
"""

from __future__ import annotations

import datetime as dt
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import duckdb

FILE_READERS = {
    ".csv": "read_csv_auto",
    ".tsv": "read_csv_auto",
    ".parquet": "read_parquet",
    ".ndjson": "read_json_auto",
    ".jsonl": "read_json_auto",
}
DUCKDB_EXTS = {".duckdb", ".ddb"}
SKIP_DIRS = {".git", ".venv", "venv", "node_modules", "__pycache__", "dbt_packages", "logs", ".tox"}
MAX_DISCOVERED = 60
SENTINELS = ("", "null", "none", "n/a", "na", "nan", "-", "?", "unknown", "undefined", "#n/a")
GRAINS = ("hour", "day", "week", "month")
SERIES_ROWS = 60  # most recent periods returned in full; summaries cover the whole range


class DQError(ValueError):
    """An error whose message is meant to be shown to the agent as-is."""


def qident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _jsonable(v: Any) -> Any:
    if isinstance(v, (dt.datetime, dt.date, dt.time)):
        return v.isoformat()
    if isinstance(v, dt.timedelta):
        return str(v)
    if isinstance(v, float) and v != v:  # NaN
        return None
    if isinstance(v, (int, float, str, bool)) or v is None:
        return v
    try:
        import decimal

        if isinstance(v, decimal.Decimal):
            return float(v)
    except Exception:  # pragma: no cover
        pass
    return str(v)


def _slug(text: str) -> str:
    s = re.sub(r"[^0-9a-zA-Z_]+", "_", text).strip("_").lower()
    if not s or s[0].isdigit():
        s = "t_" + s
    return s


@dataclass
class Source:
    name: str
    kind: str
    location: str
    note: str = ""


@dataclass
class Workspace:
    root: Path
    con: duckdb.DuckDBPyConnection = field(default_factory=lambda: duckdb.connect(":memory:"))
    sources: dict[str, Source] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    allowed: set[str] = field(default_factory=set)
    sealed: bool = False

    # ------------------------------------------------------------------ sandbox
    def seal(self) -> None:
        """Confine all file access to the project root and explicitly registered sources.

        After this, even a SELECT such as read_text('~/.ssh/id_rsa') fails, and the setting cannot
        be changed back: DuckDB refuses to re-enable external access on a live connection.
        """
        if self.sealed:
            return
        dirs = sorted({str(self.root)} | self.allowed)
        listing = ", ".join(self._lit(d.rstrip("/") + "/") for d in dirs)
        self.con.execute(f"SET allowed_directories = [{listing}]")
        self.con.execute("SET enable_external_access = false")
        self.sealed = True

    def _inside_allowed(self, p: Path) -> bool:
        roots = [self.root, *map(Path, self.allowed)]
        return any(p == r or r in p.parents for r in roots)

    # ------------------------------------------------------------------ sources
    def add_path(self, path: str, name: str | None = None) -> list[Source]:
        """Register a file, glob, directory or DuckDB database. Returns what was added."""
        p = Path(os.path.expanduser(path))
        if not p.is_absolute():
            p = self.root / p
        p = p.resolve()
        text = str(p)
        if self.sealed and not self._inside_allowed(p if p.exists() else p.parent):
            raise DQError(
                f"{text} is outside the project root ({self.root}). For safety dqsleuth only reads inside it; "
                "restart the server with --source <path> (or DQSLEUTH_SOURCES) to allow another location."
            )
        if not self.sealed:
            anchor = p.parent if (p.is_file() or any(ch in text for ch in "*?[")) else p
            while any(ch in str(anchor) for ch in "*?["):
                anchor = anchor.parent
            self.allowed.add(str(anchor))
        if any(ch in text for ch in "*?["):
            ext = Path(text).suffix.lower()
            if ext not in FILE_READERS:
                raise DQError(f"Glob must end in one of {sorted(FILE_READERS)}; got '{path}'.")
            return [self._add_file_view(text, ext, name or _slug(Path(text).parent.name + "_" + Path(text).stem.replace("*", "all")))]
        if not p.exists():
            raise DQError(f"Path not found: {p}. Paths are resolved relative to {self.root}.")
        if p.is_dir():
            added = [self._register(f) for f in self._discover(p)]
            return [s for s in added if s]
        ext = p.suffix.lower()
        if ext in DUCKDB_EXTS:
            return [self._attach_duckdb(p, name)]
        if ext in FILE_READERS:
            return [self._add_file_view(str(p), ext, name or _slug(p.stem))]
        raise DQError(f"Unsupported file type '{ext}'. Supported: {sorted(FILE_READERS) + sorted(DUCKDB_EXTS)}.")

    def attach_postgres(self, url: str, alias: str = "pg") -> Source:
        """Attach Postgres READ_ONLY. The password is handed to libpq via PGPASSWORD, so it never
        appears in the attach string that duckdb_databases() would show to a SELECT."""
        from urllib.parse import unquote, urlsplit, urlunsplit

        parts = urlsplit(url)
        if parts.password:
            os.environ["PGPASSWORD"] = unquote(parts.password)
            netloc = (parts.username or "") + "@" + (parts.hostname or "") + (f":{parts.port}" if parts.port else "")
            url = urlunsplit(parts._replace(netloc=netloc))
        try:
            self.con.execute("INSTALL postgres; LOAD postgres;")
            self.con.execute(f"ATTACH {self._lit(url)} AS {qident(alias)} (TYPE postgres, READ_ONLY)")
        except duckdb.Error as e:
            raise DQError(f"Could not attach Postgres read-only: {e}") from e
        src = Source(alias, "postgres", url, "attached READ_ONLY")
        self.sources[alias] = src
        return src

    def discover_root(self) -> None:
        for f in self._discover(self.root):
            try:
                self._register(f)
            except DQError as e:
                self.warnings.append(str(e))

    def _discover(self, base: Path) -> list[Path]:
        found: list[Path] = []
        for dirpath, dirnames, filenames in os.walk(base):
            rel_depth = len(Path(dirpath).relative_to(base).parts)
            dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS and not d.startswith(".") and rel_depth < 4]
            for fn in sorted(filenames):
                ext = Path(fn).suffix.lower()
                if ext in FILE_READERS or ext in DUCKDB_EXTS:
                    found.append(Path(dirpath) / fn)
                    if len(found) >= MAX_DISCOVERED:
                        self.warnings.append(
                            f"Stopped discovery at {MAX_DISCOVERED} files; use dq_add_source for others."
                        )
                        return found
        return found

    def _register(self, f: Path) -> Source | None:
        ext = f.suffix.lower()
        if ext in DUCKDB_EXTS:
            return self._attach_duckdb(f, None)
        return self._add_file_view(str(f), ext, _slug(f.stem))

    def _unique(self, name: str) -> str:
        base, n = name, 2
        while name in self.sources:
            name, n = f"{base}_{n}", n + 1
        return name

    def _add_file_view(self, location: str, ext: str, name: str) -> Source:
        name = self._unique(_slug(name))
        reader = FILE_READERS[ext]
        opts = ", delim='\\t'" if ext == ".tsv" else ""
        try:
            self.con.execute(
                f"CREATE VIEW {qident(name)} AS SELECT * FROM {reader}({self._lit(location)}{opts})"
            )
        except duckdb.Error as e:
            raise DQError(f"Could not read {location}: {e}") from e
        src = Source(name, "file", location)
        self.sources[name] = src
        return src

    def _attach_duckdb(self, f: Path, name: str | None) -> Source:
        alias = self._unique(_slug(name or f.stem))
        try:
            self.con.execute(f"ATTACH {self._lit(str(f))} AS {qident(alias)} (READ_ONLY)")
        except duckdb.Error as e:
            raise DQError(
                f"Could not attach {f} read-only ({e}). If another process (e.g. a running dbt build) "
                "holds a write lock, retry when it finishes."
            ) from e
        src = Source(alias, "duckdb", str(f), "attached READ_ONLY")
        self.sources[alias] = src
        return src

    @staticmethod
    def _lit(s: str) -> str:
        return "'" + s.replace("'", "''") + "'"

    # ------------------------------------------------------------------ catalog
    def tables(self) -> list[dict[str, Any]]:
        rows = self.con.execute(
            """
            SELECT table_catalog, table_schema, table_name, table_type
            FROM information_schema.tables
            WHERE table_catalog NOT IN ('system', 'temp')
              AND table_schema NOT IN ('information_schema', 'pg_catalog')
            ORDER BY 1, 2, 3
            """
        ).fetchall()
        out = []
        for cat, schema, tbl, ttype in rows:
            ident = tbl if (cat == "memory" and schema == "main") else f"{cat}.{schema}.{tbl}"
            out.append({"table": ident, "type": ttype.lower(), "database": cat})
        return out

    def resolve(self, table: str) -> str:
        """Map a user-supplied table reference to a safely quoted identifier."""
        parts = [p.strip().strip('"') for p in table.split(".")]
        if not 1 <= len(parts) <= 3 or not all(parts):
            raise DQError(f"Invalid table reference '{table}'. Use 'name' or 'database.schema.name'.")
        cond, params = ["lower(table_name) = lower(?)"], [parts[-1]]
        if len(parts) >= 2:
            cond.append("lower(table_schema) = lower(?)")
            params.append(parts[-2])
        if len(parts) == 3:
            cond.append("lower(table_catalog) = lower(?)")
            params.append(parts[0])
        rows = self.con.execute(
            f"SELECT table_catalog, table_schema, table_name FROM information_schema.tables WHERE {' AND '.join(cond)}",
            params,
        ).fetchall()
        if not rows:
            known = ", ".join(t["table"] for t in self.tables()[:25]) or "none registered"
            raise DQError(f"Table '{table}' not found. Known tables: {known}. Use dq_add_source to register more.")
        if len(rows) > 1 and len(parts) == 1:
            rows = [r for r in rows if r[0] == "memory"] or rows
            if len(rows) > 1:
                opts = ", ".join(f"{c}.{s}.{t}" for c, s, t in rows)
                raise DQError(f"'{table}' is ambiguous; qualify it as one of: {opts}.")
        c, s, t = rows[0]
        return ".".join(qident(x) for x in (c, s, t))

    def columns(self, rel: str) -> dict[str, str]:
        return {r[0]: r[1] for r in self.con.execute(f"DESCRIBE SELECT * FROM {rel}").fetchall()}

    def col(self, rel: str, name: str) -> str:
        cols = self.columns(rel)
        for c in cols:
            if c.lower() == name.lower():
                return qident(c)
        raise DQError(f"Column '{name}' not in {rel}. Columns: {', '.join(cols)}.")

    def _rows(self, sql: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
        cur = self.con.execute(sql, params or [])
        names = [d[0] for d in cur.description]
        return [{k: _jsonable(v) for k, v in zip(names, row)} for row in cur.fetchall()]

    # ------------------------------------------------------------------ SQL
    def run_select(self, sql: str, limit: int = 50) -> dict[str, Any]:
        try:
            stmts = duckdb.extract_statements(sql)
        except duckdb.Error as e:
            raise DQError(f"SQL did not parse: {e}") from e
        if len(stmts) != 1:
            raise DQError("Send exactly one statement.")
        if stmts[0].type != duckdb.StatementType.SELECT:
            raise DQError(f"Only SELECT queries are allowed (got {stmts[0].type.name}). dqsleuth is read-only.")
        try:
            cur = self.con.execute(stmts[0].query)
        except duckdb.Error as e:
            raise DQError(f"Query failed: {e}") from e
        names = [d[0] for d in cur.description]
        rows = cur.fetchmany(limit + 1)
        truncated = len(rows) > limit
        return {
            "columns": names,
            "rows": [{k: _jsonable(v) for k, v in zip(names, r)} for r in rows[:limit]],
            "truncated": truncated,
        }

    # ------------------------------------------------------------------ checks
    def profile(self, table: str, max_columns: int = 60) -> dict[str, Any]:
        rel = self.resolve(table)
        n = self.con.execute(f"SELECT count(*) FROM {rel}").fetchone()[0]
        summary = self._rows(f"SUMMARIZE SELECT * FROM {rel}")[:max_columns]
        cols, flags = [], []
        for s in summary:
            name, ctype = s["column_name"], s["column_type"]
            nullp = float(s.get("null_percentage") or 0)
            approx = s.get("approx_unique") or 0
            col = {
                "column": name,
                "type": ctype,
                "null_pct": round(nullp, 2),
                "approx_distinct": approx,
                "min": s.get("min"),
                "max": s.get("max"),
            }
            if s.get("avg") is not None:
                col["avg"] = s["avg"]
            cols.append(col)
            if n and nullp >= 100:
                flags.append(f"{name}: entirely NULL")
            elif nullp >= 20:
                flags.append(f"{name}: {nullp:.1f}% NULL")
            if n > 1 and approx == 1 and nullp < 100:
                flags.append(f"{name}: constant (one distinct value)")
            if "VARCHAR" in ctype.upper():
                q = qident(name)
                placeholders = self.con.execute(
                    f"SELECT count(*) FROM {rel} WHERE lower(trim({q})) IN ({','.join('?' * len(SENTINELS))})",
                    list(SENTINELS),
                ).fetchone()[0]
                if placeholders:
                    col["placeholder_values"] = placeholders
                    flags.append(f"{name}: {placeholders} placeholder strings ('', 'N/A', 'null', ...) that are not NULL")
            if n and approx >= n * 0.98 and nullp == 0 and n > 20:
                col["likely_key"] = True
        return {"table": table, "row_count": n, "columns": cols, "flags": flags}

    def nulls(self, table: str, columns: list[str] | None, time_column: str | None, grain: str, periods: int) -> dict[str, Any]:
        rel = self.resolve(table)
        cols = [self.col(rel, c) for c in columns] if columns else [qident(c) for c in self.columns(rel)]
        exprs = ", ".join(f"count(*) - count({c}) AS {c}" for c in cols)
        total, *counts = self.con.execute(f"SELECT count(*), {exprs} FROM {rel}").fetchone()
        overall = [
            {"column": c.strip('"'), "nulls": k, "null_pct": round(100 * k / total, 2) if total else 0}
            for c, k in zip(cols, counts)
        ]
        overall.sort(key=lambda r: -r["nulls"])
        out: dict[str, Any] = {"table": table, "row_count": total, "columns": overall}
        if time_column:
            tcol = self.col(rel, time_column)
            focus = [qident(r["column"]) for r in overall if r["nulls"]][:8]
            if focus:
                bucket = f"date_trunc('{self._grain(grain)}', CAST({tcol} AS TIMESTAMP))"
                rate = ", ".join(f"round(100.0 * (count(*) - count({c})) / count(*), 2) AS {c}" for c in focus)
                out["null_pct_by_period"] = self._rows(
                    f"SELECT {bucket} AS period, count(*) AS rows, {rate} FROM {rel} "
                    f"GROUP BY 1 ORDER BY 1 DESC LIMIT {int(periods)}"
                )
                out["hint"] = "Look for the period where a column's null_pct jumps: that is when the upstream change landed."
        return out

    def duplicates(self, table: str, key_columns: list[str], sample: int) -> dict[str, Any]:
        rel = self.resolve(table)
        keys = [self.col(rel, k) for k in key_columns]
        k = ", ".join(keys)
        total, distinct_keys = self.con.execute(
            f"SELECT count(*), count(DISTINCT ({k})) FROM {rel}"
        ).fetchone()
        null_keys = self.con.execute(
            f"SELECT count(*) FROM {rel} WHERE {' OR '.join(f'{c} IS NULL' for c in keys)}"
        ).fetchone()[0]
        all_cols = ", ".join(qident(c) for c in self.columns(rel))
        exact_dupes = self.con.execute(
            f"SELECT coalesce(sum(n - 1), 0) FROM (SELECT count(*) AS n FROM {rel} GROUP BY {all_cols} HAVING n > 1)"
        ).fetchone()[0]
        groups = self.con.execute(
            f"SELECT count(*), coalesce(sum(n - 1), 0) FROM (SELECT count(*) AS n FROM {rel} GROUP BY {k} HAVING n > 1)"
        ).fetchone()
        samples = self._rows(
            f"SELECT {k}, count(*) AS copies FROM {rel} GROUP BY {k} HAVING count(*) > 1 ORDER BY copies DESC LIMIT {int(sample)}"
        )
        extra = int(groups[1])
        verdict = "unique"
        if extra:
            verdict = (
                "exact duplicate rows: the same record was loaded more than once (re-run, overlapping incremental window, or append without dedupe)"
                if exact_dupes >= extra
                else "conflicting duplicates: same key, different values; the grain is not what the key implies, or a join fanned out"
            )
        return {
            "table": table,
            "key": key_columns,
            "row_count": total,
            "distinct_keys": distinct_keys,
            "rows_with_null_key": null_keys,
            "duplicate_key_groups": groups[0],
            "extra_rows": extra,
            "exact_duplicate_extra_rows": int(exact_dupes),
            "verdict": verdict,
            "sample_duplicate_keys": samples,
        }

    def freshness(self, table: str, time_column: str, grain: str, periods: int) -> dict[str, Any]:
        rel = self.resolve(table)
        tcol = self.col(rel, time_column)
        g = self._grain(grain)
        ts = f"CAST({tcol} AS TIMESTAMP)"
        lo, hi, future = self.con.execute(
            f"SELECT min({ts}), max({ts}), count(*) FILTER (WHERE {ts} > now()::TIMESTAMP) FROM {rel}"
        ).fetchone()
        if hi is None:
            raise DQError(f"{time_column} has no non-null values in {table}.")
        now = dt.datetime.now()
        series = self._rows(
            f"""
            WITH buckets AS (
              SELECT unnest(generate_series(date_trunc('{g}', ?::TIMESTAMP) - INTERVAL {int(periods) - 1} {g},
                                            date_trunc('{g}', ?::TIMESTAMP), INTERVAL 1 {g})) AS period
            ), counts AS (
              SELECT date_trunc('{g}', {ts}) AS period, count(*) AS rows FROM {rel} GROUP BY 1
            )
            SELECT b.period, coalesce(c.rows, 0) AS rows FROM buckets b LEFT JOIN counts c USING (period) ORDER BY 1
            """,
            [hi, hi],
        )
        vols = [r["rows"] for r in series]
        nonzero = sorted(v for v in vols if v)
        median = nonzero[len(nonzero) // 2] if nonzero else 0
        gaps = [r["period"] for r in series if r["rows"] == 0]
        low = [r["period"] for r in series if r["rows"] and median and r["rows"] < 0.5 * median]
        shown = series[-SERIES_ROWS:]
        return {
            "table": table,
            "time_column": time_column,
            "min": _jsonable(lo),
            "max": _jsonable(hi),
            "lag_from_now": str(now - hi) if isinstance(hi, dt.datetime) else None,
            "rows_in_future": future,
            "periods_checked": len(series),
            "volume_by_period": shown,
            "volume_rows_omitted": len(series) - len(shown),
            "median_rows_per_period": median,
            "empty_period_count": len(gaps),
            "empty_periods": gaps[-50:],
            "low_volume_period_count": len(low),
            "low_volume_periods": low[-50:],
        }

    def relationship(self, child_table: str, child_key: str, parent_table: str, parent_key: str, sample: int) -> dict[str, Any]:
        crel, prel = self.resolve(child_table), self.resolve(parent_table)
        ck, pk = self.col(crel, child_key), self.col(prel, parent_key)
        child_rows, null_fk = self.con.execute(f"SELECT count(*), count(*) - count({ck}) FROM {crel}").fetchone()
        orphans = self.con.execute(
            f"SELECT count(*), count(DISTINCT c.{ck}) FROM {crel} c WHERE c.{ck} IS NOT NULL "
            f"AND NOT EXISTS (SELECT 1 FROM {prel} p WHERE p.{pk} = c.{ck})"
        ).fetchone()
        parent_rows, parent_distinct = self.con.execute(f"SELECT count(*), count(DISTINCT {pk}) FROM {prel}").fetchone()
        joined = self.con.execute(f"SELECT count(*) FROM {crel} c LEFT JOIN {prel} p ON p.{pk} = c.{ck}").fetchone()[0]
        samples = self._rows(
            f"SELECT c.{ck} AS orphan_key, count(*) AS rows FROM {crel} c WHERE c.{ck} IS NOT NULL "
            f"AND NOT EXISTS (SELECT 1 FROM {prel} p WHERE p.{pk} = c.{ck}) GROUP BY 1 ORDER BY 2 DESC LIMIT {int(sample)}"
        )
        findings = []
        if orphans[0]:
            findings.append(f"{orphans[0]} child rows ({orphans[1]} distinct keys) reference a {parent_key} missing from {parent_table}; an INNER JOIN drops them.")
        if parent_distinct < parent_rows:
            findings.append(
                f"{parent_table}.{parent_key} is not unique ({parent_rows - parent_distinct} extra rows); "
                f"joining fans out {child_table} from {child_rows} to {joined} rows."
            )
        if null_fk:
            findings.append(f"{null_fk} child rows have a NULL {child_key}.")
        return {
            "child_rows": child_rows,
            "orphan_rows": orphans[0],
            "orphan_distinct_keys": orphans[1],
            "orphan_pct": round(100 * orphans[0] / child_rows, 2) if child_rows else 0,
            "null_foreign_keys": null_fk,
            "parent_rows": parent_rows,
            "parent_key_distinct": parent_distinct,
            "rows_after_left_join": joined,
            "fan_out_factor": round(joined / child_rows, 3) if child_rows else None,
            "findings": findings or ["Relationship holds: no orphans, no fan-out, no null keys."],
            "sample_orphans": samples,
        }

    def metric_change(
        self,
        table: str,
        time_column: str,
        metric: str,
        dimensions: list[str],
        current_start: str | None,
        current_end: str | None,
        baseline_windows: int,
        top: int,
    ) -> dict[str, Any]:
        rel = self.resolve(table)
        tcol = self.col(rel, time_column)
        ts = f"CAST({tcol} AS TIMESTAMP)"
        agg = self._metric_expr(rel, metric)
        if current_start:
            start = dt.datetime.fromisoformat(current_start)
            end = dt.datetime.fromisoformat(current_end) if current_end else start + dt.timedelta(days=1)
        else:
            hi = self.con.execute(f"SELECT max({ts}) FROM {rel}").fetchone()[0]
            if hi is None:
                raise DQError(f"{time_column} has no values.")
            start = dt.datetime.combine(hi.date(), dt.time())
            end = start + dt.timedelta(days=1)
        if end <= start:
            raise DQError("current_end must be after current_start.")
        width = end - start
        k = int(baseline_windows)
        b_start = start - width * k

        def window_value(where: str, params: list[Any]) -> float:
            v = self.con.execute(f"SELECT {agg} FROM {rel} WHERE {where}", params).fetchone()[0]
            return float(v or 0)

        cur = window_value(f"{ts} >= ? AND {ts} < ?", [start, end])
        base = window_value(f"{ts} >= ? AND {ts} < ?", [b_start, start]) / k
        delta = cur - base
        out: dict[str, Any] = {
            "metric": metric,
            "current_window": [start.isoformat(), end.isoformat()],
            "baseline": f"mean of the {k} preceding window(s) of the same length",
            "current": round(cur, 4),
            "baseline_value": round(base, 4),
            "change": round(delta, 4),
            "change_pct": round(100 * delta / base, 2) if base else None,
            "by_dimension": {},
        }
        explanations = []
        for d in dimensions:
            dcol = self.col(rel, d)
            rows = self._rows(
                f"""
                SELECT coalesce(CAST({dcol} AS VARCHAR), '(null)') AS segment,
                       coalesce({agg} FILTER (WHERE {ts} >= ?), 0) AS current,
                       coalesce({agg} FILTER (WHERE {ts} < ?), 0) / {k} AS baseline
                FROM {rel} WHERE {ts} >= ? AND {ts} < ?
                GROUP BY 1
                """,
                [start, start, b_start, end],
            )
            for r in rows:
                r["current"], r["baseline"] = round(float(r["current"]), 4), round(float(r["baseline"]), 4)
                r["change"] = round(r["current"] - r["baseline"], 4)
                r["share_of_total_change_pct"] = round(100 * r["change"] / delta, 1) if delta else None
                if r["baseline"] and not r["current"]:
                    r["flag"] = "missing in current window"
                elif r["current"] and not r["baseline"]:
                    r["flag"] = "new in current window"
            rows.sort(key=lambda r: -abs(r["change"]))
            out["by_dimension"][d] = rows[:top]
            if rows and delta:
                lead = rows[0]
                explanations.append(
                    f"{d}={lead['segment']} accounts for {lead['share_of_total_change_pct']}% of the change"
                    + (f" ({lead['flag']})" if lead.get("flag") else "")
                )
        out["summary"] = explanations
        out["hint"] = (
            "A segment that is 'missing in current window' usually means a feed or partition did not land; "
            "check its freshness before treating the drop as real."
        )
        return out

    # ------------------------------------------------------------------ helpers
    def _metric_expr(self, rel: str, metric: str) -> str:
        m = metric.strip().lower()
        if m in ("count", "rows", "count(*)"):
            return "count(*)"
        match = re.fullmatch(r"(sum|avg|min|max|count_distinct)\s*[:(]\s*([^)]+?)\s*\)?", metric.strip(), re.I)
        if not match:
            raise DQError("metric must be 'count' or one of 'sum:col', 'avg:col', 'min:col', 'max:col', 'count_distinct:col'.")
        fn, column = match.group(1).lower(), self.col(rel, match.group(2))
        return f"count(DISTINCT {column})" if fn == "count_distinct" else f"{fn}({column})"

    @staticmethod
    def _grain(grain: str) -> str:
        g = grain.lower()
        if g not in GRAINS:
            raise DQError(f"grain must be one of {GRAINS}.")
        return g
