-- Presto/Trino hard cases for conversion (expected values documented in run_benchmark.py,
-- all verified against a live Trino 479 cluster)

-- CASE int_div: integer division truncates in Trino
SELECT 7 / 2;

-- CASE array_concat_pipes: || concatenates arrays
SELECT ARRAY[1,2] || ARRAY[3,4];

-- CASE array_elem_pipes: || appends element
SELECT ARRAY[1,2] || 3;

-- CASE row_compare: row comparison is positional
SELECT ROW(1,2) < ROW(1,3);

-- CASE array_compare: array lexicographic comparison
SELECT ARRAY[1,2] < ARRAY[1,3];

-- CASE element_at_oob: NULL on out-of-bounds
SELECT element_at(ARRAY[1,2], 5);

-- CASE try_overflow: TRY catches integer overflow
SELECT TRY(CAST(2147483647 AS INTEGER) + CAST(1 AS INTEGER));

-- CASE repeat_array: repeat returns ARRAY in Trino
SELECT repeat('x', 3);

-- CASE date_add_type: date_add on DATE returns DATE (sqlglot issue 5108)
SELECT typeof(date_add('day', 1, DATE '2020-06-01'));

-- CASE slice_1based: slice is 1-based
SELECT slice(ARRAY[1,2,3,4], 2, 2);

-- CASE unicode_literal: unicode string literal
SELECT U&'\0041B';

-- CASE strpos_instance: 3-arg strpos = position of Nth occurrence
SELECT strpos('ababab', 'ab', 2);

-- CASE double_div_zero: IEEE double division by zero
SELECT 1e0 / 0e0;

-- CASE recursive_cte: recursive CTE
WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t WHERE n < 5) SELECT sum(n) FROM t;

-- CASE match_recognize: row pattern recognition
SELECT m FROM (VALUES 1,2,3) AS t(v) MATCH_RECOGNIZE (ORDER BY v MEASURES MATCH_NUMBER() AS m PATTERN (A B) DEFINE B AS B.v > PREV(B.v)) AS mr;

-- CASE grouping_sets: grouping sets + grouping()
SELECT coalesce(k, 'ALL') AS g, grouping(k) AS gk, sum(v) AS s FROM (VALUES ('a',1),('a',2),('b',3)) AS t(k,v) GROUP BY GROUPING SETS ((k),()) ORDER BY 1;

-- CASE unnest_then_join: UNNEST followed by JOIN (sqlglot issue 1426)
SELECT t.id, u.x, j.y FROM (VALUES (1, ARRAY[10,20])) AS t(id, arr) CROSS JOIN UNNEST(t.arr) AS u(x) INNER JOIN (VALUES (1,'y1')) AS j(id, y) ON j.id = t.id ORDER BY u.x;

-- CASE approx_pct_array: array-form approx_percentile
SELECT approx_percentile(v, ARRAY[0.5]) FROM (VALUES 1,2,3) AS t(v);

-- CASE at_timezone: AT TIME ZONE + Joda format_datetime
SELECT format_datetime(TIMESTAMP '2020-01-01 12:00:00 UTC' AT TIME ZONE 'America/Chicago', 'yyyy-MM-dd HH:mm');

-- CASE agg_filter: aggregate FILTER clause
SELECT count(*) FILTER (WHERE v > 1) FROM (VALUES 1,2,3) AS t(v);

-- CASE ignore_nulls: IGNORE NULLS in window navigation
SELECT lead(v) IGNORE NULLS OVER (ORDER BY o) FROM (VALUES (1,10),(2,CAST(NULL AS INTEGER)),(3,30)) AS t(o,v);

-- CASE intersect_all: INTERSECT ALL bag semantics
SELECT * FROM ((VALUES 1,1,2) INTERSECT ALL (VALUES 1,1,3)) AS r(v) ORDER BY v;

-- CASE chained_concat: chained array ||
SELECT ARRAY[1] || ARRAY[2] || ARRAY[3];

-- CASE try_div_zero: TRY catches division by zero
SELECT TRY(1 / 0);

-- CASE json_nested: nested JSON path
SELECT json_extract_scalar('{"a":{"b":"c"}}', '$.a.b');

-- CASE unixtime_format: from_unixtime + MySQL-style pattern (UTC session)
SELECT date_format(from_unixtime(86400), '%Y-%m-%d');

-- CASE split_literal: Presto split takes a LITERAL delimiter; Spark split takes a REGEX
SELECT split('a.b.c', '.');
