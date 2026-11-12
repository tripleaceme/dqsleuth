"""End-to-end: launch the real `dqsleuth` console script over stdio, as Claude Code does."""

from __future__ import annotations

import shutil
import sys
from pathlib import Path

import anyio
import pytest
from mcp import Client
from mcp.client.stdio import StdioServerParameters

SHOP = Path(__file__).parents[1] / "examples" / "shop"
EXE = shutil.which("dqsleuth", path=str(Path(sys.executable).parent))


@pytest.mark.skipif(EXE is None, reason="console script not installed")
def test_stdio_round_trip() -> None:
    async def run() -> None:
        params = StdioServerParameters(command=EXE, args=["--root", str(SHOP)])
        async with Client(params) as c:
            tools = {t.name: t for t in (await c.list_tools()).tools}
            assert len(tools) == 12
            assert all(t.annotations and t.annotations.read_only_hint for t in tools.values())

            r = await c.call_tool("dq_check_duplicates", {"table": "orders", "key_columns": ["order_id"]})
            assert not r.is_error and r.structured_content["extra_rows"] == 25

            r = await c.call_tool("dq_run_sql", {"sql": "DROP VIEW orders"})
            assert r.is_error and "Only SELECT" in r.content[0].text

            r = await c.call_tool("dq_profile_table", {"table": "nope"})
            assert r.is_error and "Known tables" in r.content[0].text

            r = await c.call_tool("dq_dbt_lineage", {"node": "stg_orders", "direction": "downstream"})
            assert any(n["name"] == "revenue_dashboard" for n in r.structured_content["downstream"])

    anyio.run(run)
