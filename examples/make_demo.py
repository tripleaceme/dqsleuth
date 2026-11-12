"""Generate the demo shop dataset with known, planted data quality faults.

Planted faults (the tests assert dqsleuth finds each one):
  1. Paystack payments are missing on the last day (2026-10-30) -> revenue "drops".
  2. 25 orders from 2026-10-29 were loaded twice (exact duplicates, a pipeline re-run).
  3. 12 orders reference customer_ids 9001-9012 that are not in customers (orphans).
  4. customer_id 77 appears twice in customers with different emails (join fan-out).
  5. 40 customers have the placeholder email 'N/A' instead of NULL.
  6. A dbt run where unique(orders.order_id) and relationships(orders.customer_id) fail.

Run: python examples/make_demo.py
"""

from __future__ import annotations

import csv
import datetime as dt
import json
import random
from pathlib import Path

HERE = Path(__file__).parent / "shop"
START = dt.date(2026, 10, 1)
DAYS = 30


def main() -> None:
    rng = random.Random(42)
    HERE.mkdir(parents=True, exist_ok=True)

    customers = []
    for cid in range(1, 501):
        email = "N/A" if cid % 12 == 0 and len([c for c in customers if c[2] == "N/A"]) < 40 else f"user{cid}@example.com"
        customers.append((cid, rng.choice(["Lagos", "Abuja", "Ibadan", "Kano"]), email))
    customers.append((77, "Abuja", "user77+alt@example.com"))  # fault 4
    with open(HERE / "customers.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["customer_id", "city", "email"])
        w.writerows(customers)

    orders, oid = [], 100000
    for d in range(DAYS):
        day = START + dt.timedelta(days=d)
        for _ in range(200):
            oid += 1
            provider = rng.choices(["paystack", "flutterwave", "cash"], [0.5, 0.3, 0.2])[0]
            if day == START + dt.timedelta(days=DAYS - 1) and provider == "paystack":
                continue  # fault 1: the paystack feed did not land
            ts = dt.datetime.combine(day, dt.time()) + dt.timedelta(seconds=rng.randrange(86400))
            orders.append([oid, rng.randint(1, 500), ts.isoformat(sep=" "), rng.choice(["web", "mobile", "pos"]),
                           provider, round(rng.uniform(2000, 60000), 2)])
    for i, cid in enumerate(range(9001, 9013)):  # fault 3
        orders[1000 + i * 37][1] = cid
    rerun = [list(o) for o in orders if o[2].startswith("2026-10-29")][:25]  # fault 2
    orders.extend(rerun)
    with open(HERE / "orders.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["order_id", "customer_id", "ordered_at", "channel", "payment_provider", "amount_ngn"])
        w.writerows(orders)

    write_dbt_artifacts(HERE / "target")
    print(f"wrote {len(customers)} customers, {len(orders)} orders, dbt artifacts -> {HERE}")


def write_dbt_artifacts(target: Path) -> None:
    target.mkdir(exist_ok=True)
    P = "shop"
    src_o, src_c = f"source.{P}.raw.orders", f"source.{P}.raw.customers"
    stg_o, stg_c = f"model.{P}.stg_orders", f"model.{P}.stg_customers"
    fct = f"model.{P}.fct_revenue"
    expo = f"exposure.{P}.revenue_dashboard"
    t_uniq = f"test.{P}.unique_stg_orders_order_id.1a2b3c"
    t_rel = f"test.{P}.relationships_stg_orders_customer_id__customer_id__ref_stg_customers_.4d5e6f"
    t_nn = f"test.{P}.not_null_fct_revenue_revenue_date.7a8b9c"

    def model(uid: str, name: str, mat: str, deps: list[str], sql: str) -> dict:
        return {"unique_id": uid, "name": name, "resource_type": "model", "config": {"materialized": mat},
                "depends_on": {"nodes": deps, "macros": []}, "relation_name": f'"shop"."main"."{name}"',
                "original_file_path": f"models/{name}.sql", "raw_code": sql}

    def test(uid: str, name: str, kind: str, col: str, dep: list[str], compiled: str, kwargs: dict) -> dict:
        return {"unique_id": uid, "name": name, "resource_type": "test", "column_name": col,
                "test_metadata": {"name": kind, "kwargs": {"column_name": col, **kwargs}},
                "config": {"severity": "ERROR"}, "depends_on": {"nodes": dep, "macros": [f"macro.dbt.test_{kind}"]},
                "original_file_path": "models/schema.yml", "compiled_code": compiled}

    nodes = {
        stg_o: model(stg_o, "stg_orders", "view", [src_o], "select * from {{ source('raw','orders') }}"),
        stg_c: model(stg_c, "stg_customers", "view", [src_c], "select * from {{ source('raw','customers') }}"),
        fct: model(fct, "fct_revenue", "table", [stg_o, stg_c], "select ... from {{ ref('stg_orders') }} join {{ ref('stg_customers') }} ..."),
        t_uniq: test(t_uniq, "unique_stg_orders_order_id", "unique", "order_id", [stg_o],
                     "select order_id as unique_field, count(*) as n_records from orders group by order_id having count(*) > 1", {}),
        t_rel: test(t_rel, "relationships_stg_orders_customer_id__customer_id__ref_stg_customers_", "relationships", "customer_id", [stg_c, stg_o],
                    "select o.customer_id from orders o left join customers c on o.customer_id = c.customer_id where o.customer_id is not null and c.customer_id is null",
                    {"to": "ref('stg_customers')", "field": "customer_id"}),
        t_nn: test(t_nn, "not_null_fct_revenue_revenue_date", "not_null", "revenue_date", [fct],
                   "select * from fct_revenue where revenue_date is null", {}),
    }
    sources = {
        s: {"unique_id": s, "name": s.split(".")[-1], "source_name": "raw", "resource_type": "source",
            "relation_name": f'"shop"."raw"."{s.split(".")[-1]}"'} for s in (src_o, src_c)
    }
    exposures = {expo: {"unique_id": expo, "name": "revenue_dashboard", "resource_type": "exposure",
                        "depends_on": {"nodes": [fct]}}}
    parent_map = {src_o: [], src_c: [], stg_o: [src_o], stg_c: [src_c], fct: [stg_o, stg_c],
                  t_uniq: [stg_o], t_rel: [stg_c, stg_o], t_nn: [fct], expo: [fct]}
    child_map: dict[str, list[str]] = {k: [] for k in parent_map}
    for child, parents in parent_map.items():
        for p in parents:
            child_map[p].append(child)
    manifest = {"metadata": {"dbt_version": "1.10.0", "project_name": P}, "nodes": nodes, "sources": sources,
                "exposures": exposures, "parent_map": parent_map, "child_map": child_map}
    gen = "2026-10-31T06:00:00Z"
    results = [
        {"unique_id": stg_o, "status": "success", "failures": None, "message": "OK"},
        {"unique_id": stg_c, "status": "success", "failures": None, "message": "OK"},
        {"unique_id": fct, "status": "success", "failures": None, "message": "OK"},
        {"unique_id": t_uniq, "status": "fail", "failures": 25, "message": "Got 25 results, configured to fail if != 0"},
        {"unique_id": t_rel, "status": "fail", "failures": 12, "message": "Got 12 results, configured to fail if != 0"},
        {"unique_id": t_nn, "status": "pass", "failures": 0, "message": None},
    ]
    for r in results:
        r["compiled_code"] = nodes.get(r["unique_id"], {}).get("compiled_code")
    (target / "manifest.json").write_text(json.dumps(manifest, indent=1))
    (target / "run_results.json").write_text(json.dumps(
        {"metadata": {"generated_at": gen, "invocation_id": "demo-0001"}, "results": results}, indent=1))


if __name__ == "__main__":
    main()
