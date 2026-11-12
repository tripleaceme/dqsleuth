"""Each test asserts dqsleuth finds one fault planted by examples/make_demo.py."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import duckdb
import pytest

from dqsleuth.dbt import DbtArtifacts
from dqsleuth.engine import DQError, Workspace

SHOP = Path(__file__).parents[1] / "examples" / "shop"


@pytest.fixture(scope="session", autouse=True)
def demo_data() -> None:
    if not (SHOP / "orders.csv").exists():
        subprocess.run([sys.executable, str(SHOP.parent / "make_demo.py")], check=True)


@pytest.fixture()
def ws() -> Workspace:
    w = Workspace(SHOP)
    w.discover_root()
    return w


def test_discovers_files(ws: Workspace) -> None:
    assert {t["table"] for t in ws.tables()} == {"customers", "orders"}


def test_metric_drop_attributed_to_missing_feed(ws: Workspace) -> None:
    r = ws.metric_change("orders", "ordered_at", "sum:amount_ngn", ["payment_provider", "channel"], None, None, 7, 5)
    assert r["current_window"][0].startswith("2026-10-30")
    assert r["change_pct"] < -25
    top = r["by_dimension"]["payment_provider"][0]
    assert top["segment"] == "paystack" and top["flag"] == "missing in current window"


def test_exact_duplicates_from_rerun(ws: Workspace) -> None:
    r = ws.duplicates("orders", ["order_id"], 5)
    assert r["extra_rows"] == 25
    assert r["exact_duplicate_extra_rows"] == 25
    assert r["verdict"].startswith("exact duplicate")


def test_conflicting_duplicates(ws: Workspace) -> None:
    r = ws.duplicates("customers", ["customer_id"], 5)
    assert r["extra_rows"] == 1 and r["exact_duplicate_extra_rows"] == 0
    assert r["verdict"].startswith("conflicting")


def test_orphans_and_fan_out(ws: Workspace) -> None:
    r = ws.relationship("orders", "customer_id", "customers", "customer_id", 20)
    assert r["orphan_rows"] == 12
    assert r["parent_key_distinct"] == r["parent_rows"] - 1
    assert r["rows_after_left_join"] > r["child_rows"]
    assert {o["orphan_key"] for o in r["sample_orphans"]} == set(range(9001, 9013))


def test_profile_flags_placeholder_emails(ws: Workspace) -> None:
    r = ws.profile("customers")
    email = next(c for c in r["columns"] if c["column"] == "email")
    assert email["placeholder_values"] == 40
    assert any("placeholder" in f for f in r["flags"])


def test_freshness_shows_low_volume_last_day(ws: Workspace) -> None:
    r = ws.freshness("orders", "ordered_at", "day", 7)
    assert r["max"].startswith("2026-10-30")
    assert len(r["volume_by_period"]) == 7
    assert r["volume_by_period"][-1]["rows"] < r["median_rows_per_period"]


def test_freshness_long_hourly_range_is_summarised(ws: Workspace) -> None:
    r = ws.freshness("orders", "ordered_at", "hour", 720)
    assert r["periods_checked"] == 720
    assert len(r["volume_by_period"]) == 60 and r["volume_rows_omitted"] == 660


def test_nulls_by_period(ws: Workspace) -> None:
    r = ws.nulls("orders", None, "ordered_at", "day", 3)
    assert r["row_count"] > 5000 and all(c["nulls"] == 0 for c in r["columns"])


@pytest.mark.parametrize(
    "sql",
    [
        "DROP VIEW orders",
        "CREATE TABLE x AS SELECT 1",
        "COPY orders TO 'out.csv'",
        "SELECT 1; DROP VIEW orders",
        "ATTACH 'x.duckdb' AS x",
        "INSTALL httpfs",
    ],
)
def test_run_sql_rejects_non_select(ws: Workspace, sql: str) -> None:
    with pytest.raises(DQError):
        ws.run_select(sql)
    assert "orders" in {t["table"] for t in ws.tables()}


def test_sealed_workspace_cannot_read_outside_root(ws: Workspace, tmp_path: Path) -> None:
    secret = tmp_path / "secret.txt"
    secret.write_text("hunter2")
    ws.seal()
    with pytest.raises(DQError, match="Permission"):
        ws.run_select(f"SELECT content FROM read_text('{secret}')")
    with pytest.raises(DQError, match="outside the project root"):
        ws.add_path(str(secret.with_suffix(".csv")))
    with pytest.raises(DQError):
        ws.run_select("SET enable_external_access = true")
    assert ws.run_select("SELECT count(*) AS n FROM orders")["rows"][0]["n"] > 5000


def test_run_sql_limits_rows(ws: Workspace) -> None:
    r = ws.run_select("SELECT * FROM orders", limit=3)
    assert len(r["rows"]) == 3 and r["truncated"]


def test_unknown_identifiers_rejected(ws: Workspace) -> None:
    with pytest.raises(DQError, match="not found"):
        ws.profile('orders"; DROP VIEW orders; --')
    with pytest.raises(DQError, match="not in"):
        ws.duplicates("orders", ['order_id") FROM orders; --'], 1)


def test_attached_duckdb_is_read_only(tmp_path: Path) -> None:
    db = tmp_path / "wh.duckdb"
    con = duckdb.connect(str(db))
    con.execute("CREATE TABLE t AS SELECT 1 AS id")
    con.close()
    w = Workspace(tmp_path)
    w.add_path("wh.duckdb")
    assert w.run_select("SELECT count(*) AS n FROM wh.main.t")["rows"][0]["n"] == 1
    with pytest.raises(duckdb.Error):
        w.con.execute("INSERT INTO wh.main.t VALUES (2)")


def test_dbt_failures_and_lineage() -> None:
    art = DbtArtifacts.find(SHOP)
    assert art is not None
    res = art.test_results(["fail", "error", "warn"], 10)
    assert res["status_counts"] == {"fail": 2, "pass": 1}
    assert [t["failures"] for t in res["tests"]] == [25, 12]
    detail = art.test_detail("unique_stg_orders_order_id")
    assert "having count(*) > 1" in detail["compiled_code"]
    lin = art.lineage("fct_revenue", "both", 5)
    assert {n["name"] for n in lin["upstream"]} >= {"stg_orders", "raw.orders".split(".")[-1]}
    assert any(n["name"] == "revenue_dashboard" for n in lin["downstream"])
    assert "stg_orders" in lin["hint"]
