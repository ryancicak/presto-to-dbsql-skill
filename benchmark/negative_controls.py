"""Negative controls: prove each fix is LOAD-BEARING by running the NAIVE (unfixed) Databricks
conversion and showing it FAILS or DIVERGES from Trino. If the naive form already matched, the
rule would be cosmetic. Trino truth is fetched live; DBSQL naive form run on the new workspace.
"""
import json, os, re, subprocess, sys

PROFILE = os.environ.get("DATABRICKS_PROFILE", "")
WAREHOUSE = os.environ["DBSQL_WAREHOUSE_ID"]
TRINO_SERVER = os.environ.get("TRINO_SERVER", "http://localhost:18889")

# (id, trino_sql, naive_dbsql, expected_failure_mode)
CONTROLS = [
    ("year_of_week_naive_name",
     "SELECT year_of_week(DATE '2005-01-02')",
     "SELECT year_of_week(DATE '2005-01-02')",
     "naive keeps year_of_week() -> DBSQL has no such function -> ERROR"),
    ("date_add_month_naive_dateadd",
     "SELECT date_add('month', 1, DATE '2020-01-31')",
     "SELECT dateadd(MONTH, 1, DATE '2020-01-31')",
     "naive dateadd(MONTH,..) on a DATE -> widens to TIMESTAMP (type drift) or wrong render"),
    ("date_add_month_naive_type",
     "SELECT typeof(date_add('month', 1, DATE '2020-01-31'))",
     "SELECT typeof(dateadd(MONTH, 1, DATE '2020-01-31'))",
     "Trino says 'date'; naive DBSQL says 'timestamp' -> type drift proven"),
    ("date_trunc_date_naive",
     "SELECT typeof(date_trunc('month', DATE '2024-05-17'))",
     "SELECT typeof(date_trunc('MONTH', DATE '2024-05-17'))",
     "Trino 'date'; naive DBSQL date_trunc -> 'timestamp' (type drift)"),
    ("format_number_naive_scale",
     "SELECT format_number(CAST(1234567 AS DOUBLE))",
     "SELECT format_number(CAST(1234567 AS DOUBLE), 2)",
     "naive appends scale -> '1,234,567.00' vs Trino '1.23M' (semantic collision)"),
    ("lca_in_window_naive_alias",
     "SELECT g, row_number() OVER (PARTITION BY g ORDER BY CAST(src AS DATE)) AS rn "
     "FROM (VALUES (1,'2024-01-02'),(1,'2024-01-01')) AS t(g, src)",
     "SELECT CAST(src AS DATE) AS dt, row_number() OVER (PARTITION BY g ORDER BY dt) AS rn "
     "FROM (VALUES (1,'2024-01-02'),(1,'2024-01-01')) AS t(g, src)",
     "naive keeps alias 'dt' in OVER() -> LATERAL_COLUMN_ALIAS_IN_WINDOW at runtime"),
]

def run_trino(sql):
    r = subprocess.run(["trino", "--server", TRINO_SERVER, "--catalog", "system",
                        "--schema", "runtime", "--output-format", "TSV", "--execute", sql],
                       capture_output=True, text=True, timeout=90)
    if r.returncode != 0:
        return None, "TRINO_ERR"
    return [l.split("\t") for l in r.stdout.strip().splitlines() if l], None

def run_dbsql(sql):
    payload = {"warehouse_id": WAREHOUSE, "statement": sql, "wait_timeout": "50s"}
    cmd = ["databricks", "api", "post", "/api/2.0/sql/statements"]
    if PROFILE:
        cmd += ["-p", PROFILE]
    cmd += ["--json", json.dumps(payload)]
    r = subprocess.run(cmd, capture_output=True, text=True)
    try:
        resp = json.loads(r.stdout)
    except Exception:
        return None, (r.stderr or r.stdout)[:120]
    st = resp.get("status", {})
    if st.get("state") != "SUCCEEDED":
        msg = (st.get("error", {}) or {}).get("message", "") or json.dumps(st)
        return None, msg[:120]
    return resp.get("result", {}).get("data_array") or [], None

def canon(rows):
    out = []
    for row in rows:
        cells = []
        for c in row:
            s = str(c).strip()
            m = re.match(r"^(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})(?:\.\d+)?Z?$", s)
            cells.append(f"{m.group(1)} {m.group(2)}" if m else s)
        out.append("|".join(cells))
    return sorted(out)

confirmed = 0
for cid, tsql, naive, mode in CONTROLS:
    trows, terr = run_trino(tsql)
    drows, derr = run_dbsql(naive)
    tval = canon(trows) if trows is not None else f"ERR({terr})"
    if derr:
        print(f"[CONFIRMED-FAIL ] {cid}")
        print(f"    naive DBSQL ERROR: {derr}")
        print(f"    trino truth: {tval}   ({mode})")
        confirmed += 1
    else:
        dval = canon(drows)
        if dval != (tval if isinstance(tval, list) else None):
            print(f"[CONFIRMED-DIFF ] {cid}")
            print(f"    naive DBSQL: {dval}")
            print(f"    trino truth: {tval}   ({mode})")
            confirmed += 1
        else:
            print(f"[NOT-LOADBEARING] {cid}: naive already matched trino {tval} -> rule may be cosmetic")
print(f"\n{confirmed}/{len(CONTROLS)} fixes proven load-bearing (naive form fails or diverges)")
