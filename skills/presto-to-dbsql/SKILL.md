---
name: presto-to-dbsql
description: Convert Presto/Trino SQL to Databricks SQL with exact semantic preservation. Use whenever asked to translate, convert, or migrate Presto or Trino queries to Databricks/Spark SQL.
---

# Presto/Trino -> Databricks SQL conversion

All rules verified against Databricks SQL (serverless) as of July 2026. Engine behaviors evolve - re-verify [M] items periodically.

Convert preserving semantics EXACTLY: same rows, same values, same column names. Apply the verified
rules below. When a construct is not covered here and has no clean equivalent, STOP and say so
explicitly rather than producing a plausible rewrite - flag it for human review.

## Deterministic rules (verified on live DBSQL)

1. TRY(CAST(x AS t)) -> TRY_CAST(x AS t). TRY(struct.field) -> struct.field (Spark null-struct
   access already returns NULL). TRY(a + b) or arithmetic -> try_add/try_subtract/try_multiply/
   try_divide. Never leave a bare TRY().
2. CROSS JOIN UNNEST(arr) AS t -> LATERAL VIEW EXPLODE(arr) t_lv AS t (refs stay t.field).
   CROSS JOIN UNNEST(arr) AS p (c1, ..., cn) -> LATERAL VIEW INLINE(arr) p AS c1, ..., cn.
   WITH ORDINALITY -> LATERAL VIEW POSEXPLODE (ordinal is 0-based in Spark: add +1).
   LATERAL VIEW clauses must come AFTER the table and BEFORE any JOIN clauses.
   Trino exposes unnested row-fields as bare columns; Spark does not - qualify them with the alias.
3. Struct constructors feeding ARRAY_INTERSECT / ARRAY_EXCEPT / arrays_overlap: Presto ROW() is
   anonymous (positional); Spark structs carry field names and set-ops require matching names.
   Give both sides' structs IDENTICAL field names (named_struct with unified names).
4. Integer division: Presto a / b on integers TRUNCATES. Use DIV or CAST + FLOOR to preserve.
5. repeat('x', 3) returns an ARRAY in Presto -> array_repeat('x', 3) (Spark repeat makes a string).
6. date_add(unit, n, d) on a DATE returns DATE in Presto. Preserve the DATE type per unit:
   'day' -> date_add(d, n); 'week' -> date_add(d, 7*n) (both 2-arg date_add, returns DATE);
   'month'/'quarter'/'year' -> add_months(d, n / 3*n / 12*n) (returns DATE, and its non-sticky
   month-end clamp matches Trino, e.g. Jan 31 + 1 month = Feb 29 in a leap year). NEVER use the
   3-arg dateadd(MONTH, n, d)/timestampadd for a DATE input - it returns TIMESTAMP (silent type
   drift). sub-day units (hour/minute/second) only apply to TIMESTAMP inputs; there dateadd is fine.
7. year_of_week(x) / yow(x) -> extract(YEAROFWEEK FROM x). Spark/DBSQL has NO year_of_week or yow
   function; a transpiler that keeps the name emits a call that fails at runtime. YEAROFWEEK is the
   ISO-8601 week-numbering year (same semantics as Trino). Do NOT confuse with weekofyear(x) (that
   is the week NUMBER 1-53) or year(x) (calendar year - differs on boundary dates).
8. Lateral column alias in a window: DBSQL allows referencing a SELECT-list alias later in the same
   SELECT list, but NOT inside a window OVER(...). Referencing one raises
   UNSUPPORTED_FEATURE.LATERAL_COLUMN_ALIAS_IN_WINDOW even though the transpile looked fine. Fix by
   inlining the full expression into the OVER clause (which is what the faithful Presto source does -
   Presto has no lateral aliasing). Only inline when the alias is referenced INSIDE the window, and
   NOT for non-deterministic expressions (rand(), current_timestamp()) - duplicating those changes
   results; flag those for human review instead.

## Function map (all verified)

cardinality->size; array_agg->collect_list (ORDER BY inside array_agg is LOST - post-sort with
array_sort; collect_list also SKIPS NULL elements where array_agg keeps them); arbitrary->any_value;
approx_distinct->approx_count_distinct; approx_percentile->percentile_approx; reduce->aggregate;
strpos(s,x)->locate(x,s) (ARGS SWAP; 3-arg strpos = Nth occurrence has NO direct equivalent -
rewrite carefully or flag); contains->array_contains; element_at->try_element_at; row()->
named_struct/struct; json_extract_scalar->get_json_object; date_diff('day',a,b)->datediff(DAY,a,b);
from_unixtime->cast(from_unixtime(..) as timestamp); md5->unhex(md5(..)); sha256->sha2(x,256);
zip->arrays_zip; any_match->exists; all_match->forall; none_match->NOT exists; codepoint->ascii;
infinity()->double('Infinity'); nan()->double('NaN'); geometric_mean->exp(avg(ln(x)));
format_datetime (Joda) -> date_format (convert pattern); date_parse (%-patterns) -> to_timestamp
(Spark patterns); split(s, delim) -> split(s, quoted-regex) (Spark split takes a REGEX);
last_day_of_month(x)->last_day(x) (exact); year_of_week(x)/yow(x)->extract(YEAROFWEEK FROM x)
(NOT a scalar fn in DBSQL - see rule 7).

Two FALSE FRIENDS that need care, not a rename:
- date_trunc(unit, x): TYPE-DEPENDENT. date_trunc('MONTH', <timestamp>) maps 1:1 (both unit-first,
  returns TIMESTAMP). But for a DATE input Trino returns DATE while DBSQL date_trunc ALWAYS returns
  TIMESTAMP - use trunc(x, 'MONTH') to keep DATE (note trunc REVERSES the args: date first, fmt
  second; trunc only supports YEAR/QUARTER/MONTH/WEEK). A converter handles provably-DATE args
  (DATE literals, CAST AS DATE) automatically; for a bare DATE column it needs the column type from
  the catalog (sqlglot_rules.py takes a DATE_COLS list). This drift is easy to miss because it only
  shows up as a column TYPE difference in the output, not a value difference - a full-result
  reconcile against the source engine catches it where spot checks do not.
- format_number: OPPOSITE meaning. Presto format_number(x) is 1-arg and returns a magnitude string
  (measured on Trino 479: 3 significant figures, trailing zeros stripped, unit suffix - 1500->'1.5K',
  1234->'1.23K', 12345->'12.3K', 1000->'1K', 1050000->'1.05M'). DBSQL format_number(x, d) is 2-arg
  fixed-decimal with thousands separators ('1,234.50'). No 1:1 equivalent - FLAG for human review.
  Auto-appending a scale silently returns the wrong string. Even a hand-built CASE with
  format_number(x/1e3, 2) gets the magnitude right but the precision wrong (emits '1.50K' where
  Trino emits '1.5K'), so a human must confirm the intended precision/suffix set or supply a UDF.

## Semantic traps (do not miss)

- Array subscripts: Presto arr[1] is 1-based; Spark is 0-based. Rewrite indexes.
- arr || arr -> concat(a, b); arr || elem -> concat only works array||array: use array_append.
- map['missing'] : Presto ERRORS, Spark returns NULL. Use element_at semantics deliberately.
- ORDER BY defaults: Trino ASC = NULLS LAST; Spark ASC = NULLS FIRST. Emit explicit NULLS LAST/FIRST.
- 1e0/0e0: Trino = Infinity; Spark ANSI = error. Use try_divide or double('Infinity') logic.
- xxhash64 returns BIGINT in Spark, VARBINARY in Trino.
- Window frame EXCLUDE (SQL:2011): supported by NEITHER engine (verified on Trino 479 and DBSQL). If you see it, the source is not Trino.
- MATCH_RECOGNIZE: NOT available in DBSQL (feature under development). DO NOT attempt a window
  rewrite - flag for human review.
- CAST(ROW(...) AS ROW(...)) -> CAST(STRUCT(...) AS STRUCT<...>) is positional and may rename.
- AT TIME ZONE -> from_utc_timestamp/to_utc_timestamp + date_format.
- Lateral column alias inside OVER(): a transpile can succeed and still fail at RUNTIME with
  UNSUPPORTED_FEATURE.LATERAL_COLUMN_ALIAS_IN_WINDOW. Inline the aliased expression into the window
  (see rule 8). This is a transpile-clean / runtime-fail case - EXPLAIN, don't just parse-check.
- date_trunc / date_add on a DATE silently WIDEN to TIMESTAMP under a naive rename. Keep the DATE
  type (rules 6 and the date_trunc note above) or a downstream DATE-typed column/partition breaks.

## Verification

After converting, recommend: EXPLAIN the result against real tables, then run source and converted
versions on one partition and reconcile with EXCEPT ALL (sort array columns first - agg order is
nondeterministic in both engines).
