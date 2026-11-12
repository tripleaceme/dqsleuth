---
name: investigate
description: Investigate a data quality problem (a metric that dropped or spiked, duplicate rows, a join returning the wrong row count, a stale table, a failing dbt test) using the dqsleuth MCP tools. Use when the user reports a symptom in their data and wants the root cause, not just a test result.
argument-hint: "<symptom, e.g. revenue dropped 35% yesterday>"
---

# Data quality investigation

Symptom to investigate: $ARGUMENTS

You have read-only dqsleuth tools (`dq_*`). Work like an analyst: form hypotheses, test each with the narrowest check, and let the numbers decide.

## 1. Orient

Call `dq_list_sources`. Note which tables exist, whether dbt artifacts were found, and any warnings. If the data the user means is not listed, register it with `dq_add_source` (a file, glob, folder or `.duckdb` file inside the project).

## 2. Hypothesise

Write down two or three plausible causes before running checks. Common ones, by symptom:

| Symptom | Likely causes | First tool |
|---|---|---|
| Metric dropped or spiked | a feed/partition did not land; duplicate load; a filter or mapping change; a real business change | `dq_explain_metric_change` with the dimensions that identify sources (provider, region, source_system, channel) |
| Too many rows after a join | the parent key is not unique (fan-out) | `dq_check_relationship` |
| Too few rows after a join | orphaned foreign keys dropped by an INNER JOIN | `dq_check_relationship` |
| Duplicates | pipeline re-run or overlapping incremental window (exact copies) vs wrong grain (conflicting values) | `dq_check_duplicates` |
| Dashboard looks stale | late or missing load | `dq_check_freshness` |
| Nulls appearing | upstream schema or mapping change at a specific date | `dq_check_nulls` with `time_column` |
| dbt test failing | bad data upstream, or a test that no longer matches the model's grain | `dq_dbt_test_results`, `dq_dbt_test_detail`, `dq_dbt_lineage` |

## 3. Test

- One hypothesis at a time. State what result would confirm or rule it out, then run the check.
- When a segment is flagged `missing in current window`, check that table's freshness next: a missing segment is usually missing data, not lost business.
- For dbt failures, walk `dq_dbt_lineage` upstream. The furthest-upstream node with a failing test is the likeliest root cause. Use the test's `compiled_code` with `dq_run_sql` when the warehouse is attached.
- Confirm with `dq_run_sql` (one SELECT at a time) before you call anything a finding.

## 4. Report

Use exactly these headings:

- **Symptom**: what the user saw, restated with the measured numbers.
- **Evidence**: the checks you ran and what each showed (numbers, not adjectives).
- **Root cause**: the most likely cause, and how confident you are. Name the hypotheses you ruled out and why.
- **Fix**: the immediate remediation (reload a partition, dedupe, fix a join key).
- **Prevention**: the test or monitor that would have caught it (e.g. a dbt `unique` or `relationships` test, a freshness check, a row-count anomaly check per source), with its YAML or SQL.

Never present a hypothesis as a finding without a query result behind it. If the evidence is inconclusive, say so and name the data you would need.
