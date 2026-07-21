"""Deterministic, offline regression test for sqlglot_rules.py.

Runs the actual shipped converter end to end on small Presto fixtures and asserts the
converted Databricks SQL. No warehouse or network needed - this is the fast guard that
the deterministic rules keep doing what they claim. For the live semantic benchmark
(converted SQL run against a real warehouse and graded vs Trino ground truth), see
run_benchmark.py.

Run: python benchmark/regression_new_rules.py     (from the repo root)
"""
import os
import subprocess
import sys
import tempfile

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONVERTER = os.path.join(REPO, "sqlglot_rules.py")

# (label, presto_sql, [substrings that MUST appear], [substrings that must NOT appear], env)
CASES = [
    # --- existing rules: guard against regression ---
    ("rule1 TRY(CAST)",
     "SELECT TRY(CAST(x AS INT)) AS v FROM t",
     ["TRY_CAST(x AS INT)"], ["TRY("], {}),
    ("rule2 UNNEST inline",
     "SELECT p.a, p.b FROM t CROSS JOIN UNNEST(t.arr) AS p (a, b)",
     ["LATERAL VIEW", "INLINE(t.arr)"], ["UNNEST"], {}),

    # --- rule 6: date_add DATE-preserving ---
    ("rule6 month->add_months",
     "SELECT date_add('month', 3, d) AS v FROM t",
     ["ADD_MONTHS(d, 3)"], ["DATEADD(MONTH"], {}),
    ("rule6 year->add_months*12",
     "SELECT date_add('year', 1, d) AS v FROM t",
     ["ADD_MONTHS(d, 12 * 1)"], ["DATEADD(YEAR"], {}),
    ("rule6 quarter->add_months*3",
     "SELECT date_add('quarter', 2, d) AS v FROM t",
     ["ADD_MONTHS(d, 3 * 2)"], ["DATEADD(QUARTER"], {}),
    ("rule6 week->date_add*7",
     "SELECT date_add('week', 2, d) AS v FROM t",
     ["DATE_ADD(d, 7 * 2)"], ["DATEADD(WEEK"], {}),
    ("rule6 day->date_add",
     "SELECT date_add('day', 5, d) AS v FROM t",
     ["DATE_ADD(d, 5)"], ["DATEADD(DAY"], {}),

    # --- rule 6b: date_trunc on a named DATE column -> trunc (DATE-preserving) ---
    ("rule6b date_trunc DATE col -> trunc",
     "SELECT date_trunc('month', order_date) AS m FROM t",
     ["TRUNC(order_date, 'MONTH')"], ["DATE_TRUNC"], {"DATE_COLS": "order_date"}),
    ("rule6b provably-DATE arg already trunc (no DATE_COLS)",
     "SELECT date_trunc('month', CAST(x AS DATE)) AS m FROM t",
     ["TRUNC(CAST(x AS DATE), 'MONTH')"], [], {}),
    ("rule6b bare col left alone without DATE_COLS",
     "SELECT date_trunc('month', ts_col) AS m FROM t",
     ["DATE_TRUNC('MONTH', ts_col)"], [], {}),

    # --- rule 7: year_of_week / yow ---
    ("rule7 year_of_week",
     "SELECT year_of_week(event_date) AS y FROM t",
     ["EXTRACT(YEAROFWEEK FROM event_date)"], ["YEAR_OF_WEEK"], {}),
    ("rule7 yow alias",
     "SELECT yow(event_ts) AS y FROM t",
     ["EXTRACT(YEAROFWEEK FROM event_ts)"], ["YOW("], {}),

    # --- rule 8: lateral column alias inside a window gets inlined ---
    ("rule8 alias in window ORDER BY inlined",
     "SELECT cast(ts AS date) AS dt, row_number() OVER (ORDER BY dt) AS rn FROM t",
     ["ORDER BY CAST(ts AS DATE)"], [], {}),
    ("rule8 alias in PARTITION+ORDER inlined",
     "SELECT amt * rate AS rev, sum(rev) OVER (PARTITION BY rev ORDER BY rev) AS s FROM t",
     ["PARTITION BY amt * rate", "SUM(amt * rate) OVER"], [], {}),
    ("rule8 non-window alias reuse left alone",
     "SELECT a + 1 AS b, row_number() OVER (ORDER BY real_col) AS rn FROM t",
     ["ORDER BY real_col"], [], {}),
    ("rule8 nondeterministic alias NOT inlined (flagged)",
     "SELECT rand() AS r, row_number() OVER (ORDER BY r) AS rn FROM t",
     ["ORDER BY r"], ["ORDER BY RAND()"], {}),
]


def convert(presto_sql, env):
    with tempfile.NamedTemporaryFile("w", suffix=".sql", delete=False) as f:
        f.write(presto_sql)
        inp = f.name
    outp = inp + ".out"
    run_env = dict(os.environ, **env)
    r = subprocess.run([sys.executable, CONVERTER, inp, outp],
                       capture_output=True, text=True, env=run_env)
    if r.returncode != 0:
        return None, r.stderr[-400:]
    return open(outp).read(), None


def main():
    passed = failed = 0
    for label, sql, must, mustnot, env in CASES:
        out, err = convert(sql, env)
        if out is None:
            print(f"FAIL {label}: converter raised\n     {err}")
            failed += 1
            continue
        up = out.upper()
        miss = [m for m in must if m.upper() not in up]
        bad = [m for m in mustnot if m.upper() in up]
        if miss or bad:
            print(f"FAIL {label}")
            if miss:
                print(f"     missing: {miss}")
            if bad:
                print(f"     present but forbidden: {bad}")
            print(f"     got: {out.strip()}")
            failed += 1
        else:
            print(f"PASS {label}")
            passed += 1
    print(f"\n{passed} passed, {failed} failed")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
