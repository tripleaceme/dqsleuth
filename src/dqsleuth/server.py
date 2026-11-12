"""dqsleuth MCP server: read-only tools for investigating data quality problems."""

from __future__ import annotations

import argparse
import os
from functools import wraps
from pathlib import Path
from typing import Annotated, Any, Callable, Literal

from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp_types import ToolAnnotations
from pydantic import Field

from . import __version__
from .dbt import DbtArtifacts
from .engine import DQError, Workspace

INSTRUCTIONS = """\
dqsleuth investigates data quality problems. It is read-only: it cannot modify any source.

Investigate like an analyst, not a test runner:
1. dq_list_sources to see what is reachable (local CSV/Parquet/DuckDB files, attached Postgres, dbt artifacts).
2. Form a hypothesis from the symptom, then test it with the narrowest check:
   - "metric dropped/spiked" -> dq_explain_metric_change, then dq_check_freshness on the table.
   - "join gives too many/too few rows" -> dq_check_relationship.
   - "duplicates" -> dq_check_duplicates (it tells re-loads apart from grain problems).
   - "dbt test failing" -> dq_dbt_test_results, dq_dbt_test_detail, dq_dbt_lineage (walk upstream).
3. Confirm with dq_run_sql, then report: symptom, evidence, root cause, and the fix, in that order.
Never present a hypothesis as a finding without a query result behind it.
"""

READ_ONLY = ToolAnnotations(readOnlyHint=True, destructiveHint=False, idempotentHint=True, openWorldHint=False)

mcp = MCPServer("dqsleuth", instructions=INSTRUCTIONS, version=__version__)

_state: dict[str, Any] = {}


def ws() -> Workspace:
    if "ws" not in _state:
        configure(Path(os.environ.get("DQSLEUTH_ROOT") or os.getcwd()))
    return _state["ws"]


def dbt() -> DbtArtifacts:
    ws()
    art = _state.get("dbt")
    if art is None:
        raise ToolError(
            "No dbt project found (looked for target/manifest.json under the project root). "
            "Set DQSLEUTH_DBT_TARGET or pass --dbt-target to point at a dbt target/ directory."
        )
    return art


def configure(root: Path, sources: list[str] | None = None, dbt_target: str | None = None,
              postgres_url: str | None = None, discover: bool = True) -> Workspace:
    w = Workspace(root.resolve())
    if discover:
        w.discover_root()
    for s in sources or []:
        try:
            w.add_path(s)
        except DQError as e:
            w.warnings.append(str(e))
    if postgres_url:
        try:
            w.attach_postgres(postgres_url)
        except DQError as e:
            w.warnings.append(str(e))
    w.seal()
    _state["ws"] = w
    _state["dbt"] = DbtArtifacts.find(w.root, dbt_target)
    return w


def tool(fn: Callable[..., Any]) -> Callable[..., Any]:
    """Register a read-only tool and turn engine errors into agent-readable tool errors."""

    @wraps(fn)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            return fn(*args, **kwargs)
        except (DQError, ValueError) as e:
            raise ToolError(str(e)) from e

    return mcp.tool(name=fn.__name__, annotations=READ_ONLY)(wrapper)


Table = Annotated[str, Field(description="Table name from dq_list_sources, e.g. 'orders' or 'warehouse.main.orders'.")]
Grain = Annotated[Literal["hour", "day", "week", "month"], Field(description="Time bucket size.")]


@tool
def dq_list_sources() -> dict[str, Any]:
    """List every table dqsleuth can read, where each came from, and whether dbt artifacts were found.

    Call this first. Tables come from CSV/Parquet/NDJSON files and DuckDB databases discovered under
    the project root, sources added with dq_add_source, and an optional read-only Postgres attachment.
    """
    w = ws()
    art = _state.get("dbt")
    return {
        "root": str(w.root),
        "tables": w.tables(),
        "sources": [vars(s) for s in w.sources.values()],
        "dbt_target": str(art.target) if art else None,
        "warnings": w.warnings,
    }


@tool
def dq_add_source(
    path: Annotated[str, Field(description="File, glob (e.g. 'data/orders_*.parquet'), directory, or .duckdb database. Relative to the project root.")],
    name: Annotated[str | None, Field(description="Optional table/alias name. Defaults to the file stem.")] = None,
) -> dict[str, Any]:
    """Register more data for investigation. Files become views; DuckDB databases are attached READ_ONLY.

    Use for data outside the auto-discovered folders, or to read many partition files as one table via a glob.
    """
    added = ws().add_path(path, name)
    return {"added": [vars(s) for s in added], "tables": ws().tables()}


@tool
def dq_profile_table(table: Table) -> dict[str, Any]:
    """Profile a table: row count, and per column the type, null %, distinct count, min/max and mean.

    Also flags investigation leads: fully-null or constant columns, high null rates, placeholder strings
    ('N/A', 'null', '') that hide missing values from NULL checks, and likely key columns.
    """
    return ws().profile(table)


@tool
def dq_check_nulls(
    table: Table,
    columns: Annotated[list[str] | None, Field(description="Columns to check. Omit for all columns.")] = None,
    time_column: Annotated[str | None, Field(description="Timestamp/date column. When given, shows null % per period so you can see when nulls started.")] = None,
    grain: Grain = "day",
    periods: Annotated[int, Field(ge=1, le=366, description="How many recent periods to show.")] = 14,
) -> dict[str, Any]:
    """Count NULLs per column, optionally broken down over time to pinpoint when a column started going null."""
    return ws().nulls(table, columns, time_column, grain, periods)


@tool
def dq_check_duplicates(
    table: Table,
    key_columns: Annotated[list[str], Field(min_length=1, description="Columns that should uniquely identify a row, e.g. ['order_id'].")],
    sample: Annotated[int, Field(ge=1, le=100)] = 10,
) -> dict[str, Any]:
    """Check whether key_columns are unique, and diagnose what kind of duplication it is.

    Separates exact duplicate rows (the same record loaded twice: a re-run or overlapping incremental
    window) from conflicting duplicates (same key, different values: wrong grain or a join fan-out).
    """
    return ws().duplicates(table, key_columns, sample)


@tool
def dq_check_freshness(
    table: Table,
    time_column: Annotated[str, Field(description="Event or load timestamp column.")],
    grain: Grain = "day",
    periods: Annotated[int, Field(ge=2, le=2000, description="How many periods back from the latest timestamp, e.g. 720 hours = 30 days.")] = 14,
) -> dict[str, Any]:
    """Check how recent the data is and whether volume is steady: latest timestamp, lag from now,
    rows per period, empty periods (missing loads) and periods with under half the median volume.

    Gaps and low-volume periods are computed over the whole range; only the latest 60 periods are
    listed row by row. To check one segment (e.g. a single provider), use dq_run_sql with a GROUP BY."""
    return ws().freshness(table, time_column, grain, periods)


@tool
def dq_check_relationship(
    child_table: Table,
    child_key: Annotated[str, Field(description="Foreign key column in the child table, e.g. orders.customer_id.")],
    parent_table: Table,
    parent_key: Annotated[str, Field(description="Key column in the parent table, e.g. customers.customer_id.")],
    sample: Annotated[int, Field(ge=1, le=100)] = 10,
) -> dict[str, Any]:
    """Test a join before trusting it: orphaned child keys (rows an INNER JOIN silently drops), NULL
    foreign keys, and a non-unique parent key (which makes a LEFT JOIN fan out and inflate totals)."""
    return ws().relationship(child_table, child_key, parent_table, parent_key, sample)


@tool
def dq_explain_metric_change(
    table: Table,
    time_column: Annotated[str, Field(description="Timestamp/date column that defines the windows.")],
    metric: Annotated[str, Field(description="'count', or 'sum:col', 'avg:col', 'min:col', 'max:col', 'count_distinct:col'. E.g. 'sum:revenue'.")] = "count",
    dimensions: Annotated[list[str], Field(description="Columns to segment by, e.g. ['country', 'channel', 'source_system']. Each is analysed separately.")] = [],
    current_start: Annotated[str | None, Field(description="ISO date/time where the window under investigation starts. Defaults to the latest day in the data.")] = None,
    current_end: Annotated[str | None, Field(description="ISO date/time where it ends (exclusive). Defaults to one day after current_start.")] = None,
    baseline_windows: Annotated[int, Field(ge=1, le=52, description="Baseline = mean of this many preceding windows of the same length. Use 7 with a 1-day window for a weekly-smoothed baseline.")] = 1,
    top: Annotated[int, Field(ge=1, le=50, description="Segments to return per dimension.")] = 10,
) -> dict[str, Any]:
    """Explain why a metric moved: compare a window with its baseline, then attribute the change to segments.

    For "revenue dropped 35% yesterday": gives the overall change and, per dimension, which segments
    account for it. A segment flagged 'missing in current window' usually means a feed did not load,
    which is a data problem rather than a business one.
    """
    return ws().metric_change(table, time_column, metric, dimensions, current_start, current_end, baseline_windows, top)


@tool
def dq_run_sql(
    sql: Annotated[str, Field(description="A single SELECT (DuckDB dialect). Table names as listed by dq_list_sources.")],
    limit: Annotated[int, Field(ge=1, le=1000)] = 50,
) -> dict[str, Any]:
    """Run one read-only SELECT to confirm a hypothesis. Any other statement type is rejected."""
    return ws().run_select(sql, limit)


@tool
def dq_dbt_test_results(
    statuses: Annotated[list[Literal["fail", "error", "warn", "pass", "skipped"]], Field(description="Which test statuses to return.")] = ["fail", "error", "warn"],
    limit: Annotated[int, Field(ge=1, le=500)] = 50,
) -> dict[str, Any]:
    """Summarise the last dbt test/build run from target/run_results.json: counts by status, and each
    failing test with its type, column, tested model and failure count, worst first."""
    return dbt().test_results(list(statuses), limit)


@tool
def dq_dbt_test_detail(
    test: Annotated[str, Field(description="Test name or unique_id from dq_dbt_test_results.")],
) -> dict[str, Any]:
    """Everything needed to explain one dbt test: its definition, config (severity, where), the model it
    tests, and the compiled SQL that returns the failing rows."""
    return dbt().test_detail(test)


@tool
def dq_dbt_lineage(
    node: Annotated[str, Field(description="Model, source ('source_name.table'), or any unique_id.")],
    direction: Literal["upstream", "downstream", "both"] = "both",
    depth: Annotated[int, Field(ge=1, le=20)] = 5,
) -> dict[str, Any]:
    """Walk dbt lineage from a node, marking every upstream/downstream node that has failing tests.

    Upstream answers "where did this break?"; downstream answers "what does this break?",
    including exposures (dashboards) that would show wrong numbers.
    """
    return dbt().lineage(node, direction, depth)


@mcp.prompt(name="investigate", description="Run a structured data quality investigation of a symptom.")
def investigate(symptom: str) -> str:
    return (
        f"Investigate this data quality symptom with the dqsleuth tools: {symptom}\n\n"
        "Start with dq_list_sources. Write down two or three hypotheses, test each with the narrowest "
        "check, and discard the ones the evidence rules out. Finish with: Symptom, Evidence (the numbers), "
        "Root cause, Fix, and Prevention (the test or monitor that would have caught it)."
    )


def main() -> None:
    p = argparse.ArgumentParser(prog="dqsleuth", description="Read-only MCP server for investigating data quality issues.")
    p.add_argument("--root", default=os.environ.get("DQSLEUTH_ROOT") or os.getcwd(), help="Project directory to discover data in.")
    p.add_argument("--source", action="append", default=[], help="Extra file/glob/dir/.duckdb to register (repeatable).")
    p.add_argument("--dbt-target", default=os.environ.get("DQSLEUTH_DBT_TARGET"), help="dbt target/ directory.")
    p.add_argument("--no-discover", action="store_true", help="Do not scan the root for data files.")
    p.add_argument("--version", action="version", version=f"dqsleuth {__version__}")
    a = p.parse_args()
    root = a.root if a.root and "${" not in a.root else os.getcwd()  # tolerate an unexpanded placeholder
    extra = [s for s in os.environ.get("DQSLEUTH_SOURCES", "").split(os.pathsep) if s] + a.source
    configure(Path(root), extra, a.dbt_target, os.environ.get("DQSLEUTH_POSTGRES_URL"), not a.no_discover)
    mcp.run()


if __name__ == "__main__":
    main()
