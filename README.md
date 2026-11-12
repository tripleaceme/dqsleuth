# dqsleuth

<!-- mcp-name: io.github.tripleaceme/dqsleuth -->

dqsleuth is an MCP server for investigating data quality problems. It gives Claude (or any MCP client) read-only tools to find out *why* the data is wrong, not just *that* a test failed.

Ask it things like:

- "Revenue dropped 35% yesterday. Is that real?"
- "Why does joining orders to customers give me more rows than orders has?"
- "Our `unique` test on `stg_orders` is failing. Where did it break and what does it affect?"

It runs locally on DuckDB. It reads CSV, Parquet and NDJSON files, DuckDB databases (including a dbt-duckdb warehouse), an optional Postgres database, and dbt's `target/` artifacts. It never writes to any of them.

## Install

### Claude Code (plugin)

```text
/plugin install dqsleuth --marketplace tripleaceme/dqsleuth
```

On Claude Code older than 2.1.275, add the marketplace first:

```text
/plugin marketplace add tripleaceme/dqsleuth
/plugin install dqsleuth@dqsleuth
```

The plugin bundles the MCP server and an `/dqsleuth:investigate` skill. The skill gives Claude a structured method: hypotheses, evidence, root cause, fix, prevention. It needs [uv](https://docs.astral.sh/uv/) on your PATH, because the server runs through `uvx`.

### Any other MCP client

```json
{
  "mcpServers": {
    "dqsleuth": {
      "command": "uvx",
      "args": ["dqsleuth", "--root", "/path/to/your/project"]
    }
  }
}
```

Or install it with `pip install dqsleuth` and run `dqsleuth --root .`.

## Try it on the demo data

The repo ships a small shop dataset with six planted faults. Clone it, open Claude Code in `examples/shop`, and ask:

```text
/dqsleuth:investigate revenue on 30 Oct 2026 dropped. Is it real?
```

Claude should find that Paystack payments are completely missing that day. That makes it a feed problem, not lost sales. Along the way it should also find the 25 orders that were loaded twice and the two failing dbt tests. The faults are listed at the top of [`examples/make_demo.py`](examples/make_demo.py), and [`tests/test_engine.py`](tests/test_engine.py) checks that dqsleuth finds each one.

## Tools

| Tool | What it answers |
|---|---|
| `dq_list_sources` | What can I read? Tables, where they came from, whether dbt artifacts were found. |
| `dq_add_source` | Register another file, glob (`data/orders_*.parquet`), folder or `.duckdb` file. |
| `dq_profile_table` | Row count and per-column nulls, distinct values, min/max. Flags constant columns, high null rates, and placeholder strings like `'N/A'` that hide from NULL checks. |
| `dq_check_nulls` | Null counts per column. With a time column, null % per period, which shows when nulls started. |
| `dq_check_duplicates` | Is this key unique? If not, are the extra rows exact copies (a re-run or overlapping incremental load) or conflicting values (wrong grain or a fan-out)? |
| `dq_check_freshness` | Latest timestamp, lag from now, rows per period, empty periods and low-volume periods. |
| `dq_check_relationship` | Orphaned foreign keys (rows an INNER JOIN drops), NULL keys, and a non-unique parent key that makes a LEFT JOIN inflate totals. |
| `dq_explain_metric_change` | Compares a window with its baseline and attributes the change to segments. Flags segments that disappeared, which usually means a feed did not land. |
| `dq_run_sql` | One read-only SELECT to confirm a hypothesis. |
| `dq_dbt_test_results` | Failing, erroring and warning tests from the last run, worst first. |
| `dq_dbt_test_detail` | One test's definition, config, tested model and compiled SQL. |
| `dq_dbt_lineage` | Upstream and downstream of a node, marking every node with failing tests. Downstream includes exposures. |

There is also an `investigate` MCP prompt for clients that support prompts.

## Configuration

| Setting | Flag | Environment variable |
|---|---|---|
| Project root to scan for data | `--root` | `DQSLEUTH_ROOT` |
| Extra files, globs or folders | `--source` (repeatable) | `DQSLEUTH_SOURCES` (`:`-separated) |
| dbt `target/` directory | `--dbt-target` | `DQSLEUTH_DBT_TARGET` |
| Postgres connection URL | — | `DQSLEUTH_POSTGRES_URL` |
| Skip scanning the root | `--no-discover` | — |

On start, dqsleuth scans the root up to four levels deep. It registers up to 60 data files and finds `target/manifest.json`. It skips `.git`, virtualenvs, `node_modules` and `dbt_packages`. dbt artifacts are re-read whenever dbt rewrites them, so a new `dbt build` shows up without a restart.

The Claude Code plugin sets the root to your project directory. To point it at Postgres, export `DQSLEUTH_POSTGRES_URL` before starting Claude Code. Use a read-only database role.

## Safety

dqsleuth is built to be safe to hand to an agent:

- **No writes.** Files are exposed as views. DuckDB databases and Postgres are attached `READ_ONLY`. `dq_run_sql` rejects anything that isn't a single SELECT.
- **No reads outside the project.** After start-up, DuckDB's external access is switched off for everything except the project root and paths you passed with `--source`. A query like `read_text('~/.ssh/id_rsa')` fails, and the setting cannot be switched back on the live connection.
- **No SQL injection through names.** Table and column names are checked against the catalog before they go into SQL.
- **No secrets in results.** The Postgres password goes to libpq through `PGPASSWORD`, so it never appears in the connection string that a SELECT could see.
- **No network calls**, except installing DuckDB's Postgres extension the first time you use Postgres.

Results are capped (`limit` on every listing tool) so a large table can't flood the agent's context.

## Development

```text
uv sync
uv run python examples/make_demo.py
uv run pytest
```

`tests/test_stdio.py` starts the real server over stdio and calls it through an MCP client, the same way Claude Code does.

## Roadmap

- Read Soda and Great Expectations results alongside dbt's.
- OpenMetadata lineage and ownership, so an investigation can name who owns the broken table.
- Snowflake and BigQuery through DuckDB community extensions.
- Distribution checks: a numeric column whose mean or spread shifted between windows.

Contributions are welcome. New checks go in `src/dqsleuth/engine.py` with a planted fault in `examples/make_demo.py` and a test that finds it.

## License

MIT
