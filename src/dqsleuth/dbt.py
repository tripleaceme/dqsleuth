"""Read dbt artifacts (manifest.json + run_results.json) without importing dbt."""

from __future__ import annotations

import json
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .engine import DQError

PROBLEM_STATUSES = ("fail", "error", "warn")


@dataclass
class DbtArtifacts:
    target: Path
    manifest: dict[str, Any] = field(default_factory=dict)
    run_results: dict[str, Any] = field(default_factory=dict)
    _mtimes: tuple[float, float] = (0.0, 0.0)

    @classmethod
    def find(cls, root: Path, explicit: str | None = None) -> DbtArtifacts | None:
        candidates = [Path(explicit).expanduser()] if explicit else []
        candidates += [root / "target", *sorted(root.glob("*/target")), *sorted(root.glob("*/*/target"))]
        for c in candidates:
            if not c.is_absolute():
                c = root / c
            if (c / "manifest.json").is_file():
                return cls(c)
        return None

    def load(self) -> None:
        """(Re)load artifacts if they changed on disk since the last read (dbt rewrites them each run)."""
        m, r = self.target / "manifest.json", self.target / "run_results.json"
        mt = (m.stat().st_mtime if m.exists() else 0.0, r.stat().st_mtime if r.exists() else 0.0)
        if mt == self._mtimes:
            return
        if not m.exists():
            raise DQError(f"No manifest.json in {self.target}. Run `dbt compile` or `dbt build` first.")
        self.manifest = json.loads(m.read_text())
        self.run_results = json.loads(r.read_text()) if r.exists() else {}
        self._mtimes = mt

    # ------------------------------------------------------------------ lookup
    def _all_nodes(self) -> dict[str, dict[str, Any]]:
        out: dict[str, dict[str, Any]] = {}
        for key in ("nodes", "sources", "exposures", "metrics", "semantic_models", "unit_tests"):
            out.update(self.manifest.get(key, {}) or {})
        return out

    def resolve_node(self, ref: str) -> str:
        self.load()
        nodes = self._all_nodes()
        if ref in nodes:
            return ref
        r = ref.lower()
        matches = [
            uid
            for uid, n in nodes.items()
            if n.get("name", "").lower() == r
            or f"{n.get('source_name', '')}.{n.get('name', '')}".lower() == r
            or uid.lower().endswith("." + r)
        ]
        if not matches:
            raise DQError(f"No dbt node matches '{ref}'. Use a model/test/source name or a unique_id.")
        if len(matches) > 1:
            pref = [m for m in matches if m.startswith("model.")]
            if len(pref) == 1:
                return pref[0]
            raise DQError(f"'{ref}' is ambiguous: {', '.join(matches[:10])}.")
        return matches[0]

    def _results_by_id(self) -> dict[str, dict[str, Any]]:
        return {r["unique_id"]: r for r in self.run_results.get("results", [])}

    def _brief(self, uid: str) -> dict[str, Any]:
        n = self._all_nodes().get(uid, {})
        d: dict[str, Any] = {"unique_id": uid, "name": n.get("name"), "type": n.get("resource_type")}
        mat = (n.get("config") or {}).get("materialized")
        if mat:
            d["materialized"] = mat
        if n.get("relation_name"):
            d["relation"] = n["relation_name"]
        return d

    # ------------------------------------------------------------------ tools
    def test_results(self, statuses: list[str], limit: int) -> dict[str, Any]:
        self.load()
        if not self.run_results:
            raise DQError(f"No run_results.json in {self.target}. Run `dbt test` or `dbt build` first.")
        nodes = self._all_nodes()
        tally: dict[str, int] = {}
        rows = []
        for r in self.run_results.get("results", []):
            uid = r["unique_id"]
            if not uid.startswith(("test.", "unit_test.")):
                continue
            tally[r["status"]] = tally.get(r["status"], 0) + 1
            if r["status"] not in statuses:
                continue
            n = nodes.get(uid, {})
            meta = n.get("test_metadata") or {}
            parents = [p for p in (n.get("depends_on") or {}).get("nodes", []) if not p.startswith("macro.")]
            rows.append(
                {
                    "test": n.get("name", uid),
                    "unique_id": uid,
                    "status": r["status"],
                    "failures": r.get("failures"),
                    "test_type": meta.get("name") or ("unit_test" if uid.startswith("unit_test.") else "singular"),
                    "column": n.get("column_name") or (meta.get("kwargs") or {}).get("column_name"),
                    "models": parents,
                    "severity": (n.get("config") or {}).get("severity"),
                    "message": (r.get("message") or "")[:300],
                }
            )
        rows.sort(key=lambda x: (PROBLEM_STATUSES.index(x["status"]) if x["status"] in PROBLEM_STATUSES else 9, -(x["failures"] or 0)))
        meta = self.run_results.get("metadata", {})
        return {
            "target": str(self.target),
            "generated_at": meta.get("generated_at"),
            "invocation_id": meta.get("invocation_id"),
            "status_counts": tally,
            "tests": rows[:limit],
            "truncated": len(rows) > limit,
        }

    def test_detail(self, ref: str) -> dict[str, Any]:
        uid = self.resolve_node(ref)
        n = self._all_nodes()[uid]
        r = self._results_by_id().get(uid, {})
        parents = [p for p in (n.get("depends_on") or {}).get("nodes", []) if not p.startswith("macro.")]
        compiled = r.get("compiled_code") or n.get("compiled_code")
        detail = {
            "unique_id": uid,
            "name": n.get("name"),
            "type": n.get("resource_type"),
            "status": r.get("status", "not in last run"),
            "failures": r.get("failures"),
            "message": r.get("message"),
            "test_metadata": n.get("test_metadata"),
            "config": {k: v for k, v in (n.get("config") or {}).items() if k in ("severity", "where", "warn_if", "error_if", "limit", "store_failures")},
            "tested_nodes": [self._brief(p) for p in parents],
            "file": n.get("original_file_path"),
            "raw_code": n.get("raw_code"),
            "compiled_code": compiled,
        }
        if compiled:
            detail["next_step"] = (
                "compiled_code returns the failing rows. If the warehouse is attached (e.g. the dbt-duckdb "
                "database file via dq_add_source), run it with dq_run_sql to inspect them."
            )
        elif n.get("resource_type") == "test":
            detail["next_step"] = "No compiled SQL in the artifacts; run `dbt compile` to populate it."
        return detail

    def lineage(self, ref: str, direction: str, depth: int) -> dict[str, Any]:
        uid = self.resolve_node(ref)
        results = self._results_by_id()
        child_map = self.manifest.get("child_map") or {}
        parent_map = self.manifest.get("parent_map") or {}
        tests_on: dict[str, list[str]] = {}
        for tid, parents in parent_map.items():
            if tid.startswith(("test.", "unit_test.")):
                for p in parents:
                    tests_on.setdefault(p, []).append(tid)

        def walk(graph: dict[str, list[str]]) -> list[dict[str, Any]]:
            seen, out, q = {uid}, [], deque([(uid, 0)])
            while q:
                cur, d = q.popleft()
                if d >= depth:
                    continue
                for nxt in graph.get(cur, []):
                    if nxt in seen or nxt.startswith(("test.", "unit_test.")):
                        continue
                    seen.add(nxt)
                    item = self._brief(nxt) | {"distance": d + 1}
                    res = results.get(nxt)
                    if res:
                        item["last_run_status"] = res["status"]
                    bad = [
                        {"test": self._all_nodes().get(t, {}).get("name", t), "status": results[t]["status"], "failures": results[t].get("failures")}
                        for t in tests_on.get(nxt, [])
                        if t in results and results[t]["status"] in PROBLEM_STATUSES
                    ]
                    if bad:
                        item["problem_tests"] = bad
                    out.append(item)
                    q.append((nxt, d + 1))
            return out

        out: dict[str, Any] = {"node": self._brief(uid)}
        if direction in ("upstream", "both"):
            out["upstream"] = walk(parent_map)
        if direction in ("downstream", "both"):
            out["downstream"] = walk(child_map)
        own = [
            {"test": self._all_nodes().get(t, {}).get("name", t), "status": results.get(t, {}).get("status", "not run")}
            for t in tests_on.get(uid, [])
        ]
        out["tests_on_node"] = own
        flagged = [i["name"] for k in ("upstream", "downstream") for i in out.get(k, []) if i.get("problem_tests")]
        out["hint"] = (
            f"Nodes with failing tests in this lineage: {', '.join(flagged)}. The furthest-upstream one is the likeliest root cause."
            if flagged
            else "No failing tests elsewhere in this lineage."
        )
        return out
