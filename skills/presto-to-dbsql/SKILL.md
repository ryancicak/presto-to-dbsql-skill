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
6. date_add('day', n, d) on a DATE returns DATE in Presto -> use Spark date_add(d, n) (returns
   DATE), NOT dateadd/timestampadd (returns TIMESTAMP).

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
(Spark patterns); split(s, delim) -> split(s, quoted-regex) (Spark split takes a REGEX).

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

## Verification

After converting, recommend: EXPLAIN the result against real tables, then run source and converted
versions on one partition and reconcile with EXCEPT ALL (sort array columns first - agg order is
nondeterministic in both engines).
