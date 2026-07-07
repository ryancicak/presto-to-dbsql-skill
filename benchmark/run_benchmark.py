"""Adversarial Presto/Trino -> DBSQL benchmark.
Each case: self-contained Trino SQL (inline VALUES, no tables) + documented Trino result.
Lanes: sqlglot(RAISE), LLM-naive (one batched FMAPI call), raw-paste (hazard meter).
Grade per lane: CORRECT / WRONG_SILENT / LOUD_FAIL / (LLM) per-case.
"""
import json, re, subprocess, sys, time

CASES = [
    ("int_div", "SELECT 7 / 2", "3", "integer division truncates in Trino"),
    ("array_concat_pipes", "SELECT ARRAY[1,2] || ARRAY[3,4]", "[1,2,3,4]", "|| concatenates arrays"),
    ("array_elem_pipes", "SELECT ARRAY[1,2] || 3", "[1,2,3]", "|| appends element"),
    ("row_compare", "SELECT ROW(1,2) < ROW(1,3)", "true", "row comparison is positional"),
    ("array_compare", "SELECT ARRAY[1,2] < ARRAY[1,3]", "true", "array lexicographic comparison"),
    ("element_at_oob", "SELECT element_at(ARRAY[1,2], 5)", "null", "NULL on out-of-bounds"),
    ("try_overflow", "SELECT TRY(CAST(2147483647 AS INTEGER) + CAST(1 AS INTEGER))", "null",
     "TRY catches integer overflow"),
    ("repeat_array", "SELECT repeat('x', 3)", "[x,x,x]", "repeat returns ARRAY in Trino"),
    ("date_add_type", "SELECT typeof(date_add('day', 1, DATE '2020-06-01'))", "date",
     "date_add on DATE returns DATE (sqlglot issue 5108)"),
    ("slice_1based", "SELECT slice(ARRAY[1,2,3,4], 2, 2)", "[2,3]", "slice is 1-based"),
    ("unicode_literal", "SELECT U&'\\0041B'", "AB", "unicode string literal"),
    ("strpos_instance", "SELECT strpos('ababab', 'ab', 2)", "3",
     "3-arg strpos = position of Nth occurrence"),
    ("double_div_zero", "SELECT 1e0 / 0e0", "Infinity", "IEEE double division by zero"),
    ("recursive_cte",
     "WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t WHERE n < 5) SELECT sum(n) FROM t",
     "15", "recursive CTE"),
    ("match_recognize",
     "SELECT m FROM (VALUES 1,2,3) AS t(v) MATCH_RECOGNIZE (ORDER BY v MEASURES MATCH_NUMBER() AS m PATTERN (A B) DEFINE B AS B.v > PREV(B.v)) AS mr",
     "[1]", "row pattern recognition"),
    ("grouping_sets",
     "SELECT coalesce(k, 'ALL') AS g, grouping(k) AS gk, sum(v) AS s FROM (VALUES ('a',1),('a',2),('b',3)) AS t(k,v) GROUP BY GROUPING SETS ((k),()) ORDER BY 1",
     "[ALL,1,6];[a,0,3];[b,0,3]", "grouping sets + grouping()"),
    ("unnest_then_join",
     "SELECT t.id, u.x, j.y FROM (VALUES (1, ARRAY[10,20])) AS t(id, arr) CROSS JOIN UNNEST(t.arr) AS u(x) INNER JOIN (VALUES (1,'y1')) AS j(id, y) ON j.id = t.id ORDER BY u.x",
     "[1,10,y1];[1,20,y1]", "UNNEST followed by JOIN (sqlglot issue 1426)"),
    ("approx_pct_array",
     "SELECT approx_percentile(v, ARRAY[0.5]) FROM (VALUES 1,2,3) AS t(v)",
     "[2]", "array-form approx_percentile"),
    ("at_timezone",
     "SELECT format_datetime(TIMESTAMP '2020-01-01 12:00:00 UTC' AT TIME ZONE 'America/Chicago', 'yyyy-MM-dd HH:mm')",
     "2020-01-01 06:00", "AT TIME ZONE + Joda format_datetime"),
    ('agg_filter', 'SELECT count(*) FILTER (WHERE v > 1) FROM (VALUES 1,2,3) AS t(v)', '2', 'aggregate FILTER clause'),
    ('ignore_nulls', 'SELECT lead(v) IGNORE NULLS OVER (ORDER BY o) FROM (VALUES (1,10),(2,CAST(NULL AS INTEGER)),(3,30)) AS t(o,v)', '[30];[30];[null]', 'IGNORE NULLS in window navigation'),
    ('intersect_all', 'SELECT * FROM ((VALUES 1,1,2) INTERSECT ALL (VALUES 1,1,3)) AS r(v) ORDER BY v', '[1];[1]', 'INTERSECT ALL bag semantics'),
    ('chained_concat', 'SELECT ARRAY[1] || ARRAY[2] || ARRAY[3]', '[1,2,3]', 'chained array ||'),
    ('try_div_zero', 'SELECT TRY(1 / 0)', 'null', 'TRY catches division by zero'),
    ('json_nested', 'SELECT json_extract_scalar(\'{"a":{"b":"c"}}\', \'$.a.b\')', 'c', 'nested JSON path'),
    ('unixtime_format', "SELECT date_format(from_unixtime(86400), '%Y-%m-%d')", '1970-01-02', 'from_unixtime + MySQL-style pattern (UTC session)'),
    ('split_literal', "SELECT split('a.b.c', '.')", '[a,b,c]', 'Presto split takes a LITERAL delimiter; Spark split takes a REGEX'),
]

def norm(v):
    s = str(v).strip().lower()
    s = re.sub(r'["\s]', "", s)
    return s.replace("none", "null")

def run_dbsql(stmt):
    payload = {"warehouse_id": WAREHOUSE, "catalog": CATALOG,
               "statement": stmt, "wait_timeout": "40s"}
    r = subprocess.run(["databricks", "api", "post", "/api/2.0/sql/statements",
                        "-p", PROFILE, "--json", json.dumps(payload)],
                       capture_output=True, text=True)
    try:
        resp = json.loads(r.stdout)
    except Exception:
        return None, r.stderr[:150]
    st = resp.get("status", {})
    if st.get("state") != "SUCCEEDED":
        return None, (st.get("error", {}).get("message") or "unknown")[:150]
    rows = resp.get("result", {}).get("data_array") or []
    return ";".join("[" + ",".join(norm(c) for c in row) + "]" for row in rows), None

def grade(got, err, expected):
    if err is not None:
        return "LOUD_FAIL"
    want = expected if (";" in expected or expected.startswith("[")) else "[" + expected + "]"
    return "CORRECT" if norm(got) == norm(want) else "WRONG_SILENT"

def lane_sqlglot():
    import sqlglot
    out = {}
    for cid, sql, exp, _ in CASES:
        try:
            conv = sqlglot.transpile(sql, read="trino", write="databricks",
                                     unsupported_level=sqlglot.ErrorLevel.RAISE)[0]
        except Exception as e:
            out[cid] = ("LOUD_FAIL(transpile)", str(e)[:80])
            continue
        got, err = run_dbsql(conv)
        out[cid] = (grade(got, err, exp), (err or got or "")[:90])
    return out

def lane_raw():
    out = {}
    for cid, sql, exp, _ in CASES:
        got, err = run_dbsql(sql)
        out[cid] = (grade(got, err, exp), (err or got or "")[:90])
    return out

def lane_llm():
    from databricks.sdk.core import Config
    tok = Config(profile=PROFILE).authenticate()["Authorization"].split(" ", 1)[1]
    numbered = "\n".join(f"-- CASE {cid}\n{sql};" for cid, sql, _, _ in CASES)
    prompt = ("Convert each of the following Presto/Trino SQL statements to Databricks SQL, "
              "preserving semantics EXACTLY (same values, same rows). If a construct has no "
              "DBSQL equivalent, rewrite it semantically. Return a JSON object mapping each "
              "case id to the converted SQL string. Return ONLY JSON.\n\n" + numbered)
    payload = {"messages": [{"role": "user", "content": prompt}], "max_tokens": 16000}
    open("hard_llm_payload.json", "w").write(json.dumps(payload))
    r = subprocess.run(["curl", "-s", "--max-time", "560", "-X", "POST",
                        f"{HOST}/serving-endpoints/databricks-claude-opus-4-7/invocations",
                        "-H", f"Authorization: Bearer {tok}", "-H", "Content-Type: application/json",
                        "-d", "@hard_llm_payload.json"], capture_output=True, text=True, timeout=580)
    content = json.loads(r.stdout)["choices"][0]["message"]["content"]
    m = re.search(r"\{.*\}", content, re.DOTALL)
    mapping = json.loads(m.group(0))
    open("hard_llm_mapping.json", "w").write(json.dumps(mapping, indent=1))
    out = {}
    for cid, _, exp, _ in CASES:
        conv = mapping.get(cid)
        if not conv:
            out[cid] = ("MISSING", "")
            continue
        got, err = run_dbsql(conv)
        out[cid] = (grade(got, err, exp), (err or got or "")[:90])
    return out

if __name__ == "__main__":
    which = sys.argv[1]
    res = {"sqlglot": lane_sqlglot, "raw": lane_raw, "llm": lane_llm}[which]()
    json.dump(res, open(f"hard_{which}_results.json", "w"), indent=1)
    for cid, (verdict, detail) in res.items():
        print(f"{cid:22} {verdict:22} {detail}")
