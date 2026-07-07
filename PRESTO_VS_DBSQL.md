# Presto/Trino vs Databricks SQL - the complete difference catalog

Compiled 2026-07-07. All [M] items verified against Databricks SQL (serverless) as of July 2026.
Provenance tags on every item:
- **[M]** = measured live (real DBSQL warehouse runs, a large production analytics job (all patterns reproduced in examples/synthetic_job_presto.sql), or probe queries)
- **[B]** = verified via the sqlglot 30.12 transpile battery (144 expressions; see function_mapping.md)
- **[D]** = documented/known behavior - include in the A/B reconcile fixture before relying on it

The five classes, ordered by how much they hurt:

---

## Class 1 - SAME NAME, DIFFERENT ANSWER (the silent killers)

No error, no warning, wrong results. These are what reconciliation exists for.

| Construct | Presto/Trino | Databricks SQL | Fix |
|---|---|---|---|
| **Integer division** `7 / 2` | `3` (truncates) [D] | `3.5` **[M]** | `div` operator or CAST |
| **`repeat('x', 3)`** | `ARRAY['x','x','x']` [D] | `'xxx'` (string) **[M]** | `array_repeat(x, 3)` |
| **Array subscript base** `arr[1]` | first element (1-based) [D] | second element (0-based) **[B]** - sqlglot rewrites the index | automated |
| **`ROW()` vs `STRUCT()` in set ops** | anonymous, compares positionally | field names part of the type; `ARRAY_INTERSECT` on differently-named structs = `BINARY_ARRAY_DIFF_TYPES` **[M - found in a real production job]** | Rule 3: positional `_f0.._fn` rename |
| **`map['missing_key']`** | runtime ERROR [D] | `NULL` **[M]** | behavior diff both directions; use `element_at`/`try_element_at` explicitly |
| **ORDER BY null placement** | ASC = NULLS LAST default [D] | ASC = NULLS FIRST default [D] - flips ROW_NUMBER dedup winners | sqlglot emits explicit `NULLS LAST` when transpiling **[B]**; hand-ported SQL must do it manually |
| **`xxhash64`** | returns VARBINARY [D] | returns BIGINT **[M]** | affects checksum comparisons across engines |
| **`typeof()` strings** | `'integer'`, `'varchar'` | `'int'`, `'string'` **[M]** | anything branching on type names |
| **Regex engine** | Joni/RE2J [D] | Java `java.util.regex` [D] | mostly compatible; lookbehind/posix-class edges differ. the test job: only literal `'-'`, zero exposure **[M]** |
| **Datetime format patterns** | MySQL-style `%Y-%m-%d` (date_format/date_parse) + Joda (format_datetime) [D] | Spark datetime patterns `yyyy-MM-dd` | sqlglot converts `%`-patterns **[B]**; Joda `format_datetime`/`parse_datetime` pass through UNCONVERTED and don't exist in DBSQL [D] |
| **Timestamp model** | `TIMESTAMP(3)` no tz + `TIMESTAMP WITH TIME ZONE` [D] | `TIMESTAMP` = session-tz-relative, `TIMESTAMP_NTZ` = no-tz; microsecond precision [D] | pin session tz (`SET TIME ZONE`) in the A/B; `at_timezone`/`with_timezone` need rewrites |
| **`array_agg(x ORDER BY y)`** | honors ordering [D] | transpiles to `COLLECT_LIST(x)` - **ORDER BY silently dropped** **[B]** | post-sort with `array_sort`, or window-based construction |
| **`COLLECT_LIST` vs `array_agg` NULL elements** | array_agg keeps NULLs [D] | collect_list SKIPS NULLs [D] | can't trigger in the test job (aggregated structs never NULL) **[M]**; fixture case for others |

## Class 2 - CONSTRUCTS THAT DON'T TRANSPILE (loud if you configure it right)

Stock sqlglot drops these with only a log warning -> **always run with `unsupported_level=RAISE`** [M].

| Presto/Trino | Databricks SQL | Status |
|---|---|---|
| `TRY(expr)` | no generic equivalent | Rule 1 **[M]**: `TRY(CAST(..))` -> `TRY_CAST(..)`; `TRY(struct.field)` -> drop wrapper (Spark null-struct access already yields NULL); `TRY(a/b)` -> `try_divide`; else `ANSI_MODE=false` blanket |
| `CROSS JOIN UNNEST(arr) AS t` | silently dropped join = wrong results | Rule 2 **[M]**: `LATERAL VIEW EXPLODE` (bare alias) / `LATERAL VIEW INLINE` (column list). Same empty/NULL-array row-dropping semantics **[M - scenario-tested]** |
| `UNNEST ... WITH ORDINALITY` | - | `LATERAL VIEW POSEXPLODE` **[B]** (note: ordinal is 0-based in Spark, 1-based in Trino [D]) |
| Bare unnested row-field refs | Trino exposes row fields as bare columns | Spark EXPLODE-as-struct doesn't; Rule 2b qualifies them (needs element schema from catalog) **[M]** |
| Dynamic partition overwrite (`partitionOverwriteMode=dynamic`) | Spark-only feature | `INSERT INTO ... REPLACE USING (cols)` (works from SELECT, not VALUES) |

## Class 3 - FUNCTIONS MISSING IN DBSQL (pass through, fail at runtime)

Probed live **[M]**: `UNRESOLVED_ROUTINE`. sqlglot does NOT map these - rule-pack candidates:

| Presto/Trino | DBSQL replacement |
|---|---|
| `zip(a, b)` | `arrays_zip(a, b)` (field names differ: 0,1 vs a,b - feeds Class 1 struct naming) |
| `any_match(arr, p)` | `exists(arr, p)` |
| `all_match(arr, p)` | `forall(arr, p)` |
| `none_match(arr, p)` | `NOT exists(arr, p)` |
| `json_size(j, p)` | `json_array_length(get_json_object(j, p))` (arrays) |
| `histogram(x)` | `map_from_entries` over `GROUP BY` / `count` |
| `multimap_agg(k, v)` | `map_from_entries(collect_list(struct(k, v)))`-family rewrite |
| `checksum(x)` | no direct; `sum(xxhash64(..))` convention |
| `geometric_mean(x)` | `exp(avg(ln(x)))` |
| `infinity()` / `nan()` | `double('Infinity')` / `double('NaN')` |
| `is_finite(x)` | `NOT isnan(x) AND abs(x) != double('Infinity')` |
| `codepoint(c)` | `ascii(c)` |
| `format_datetime` / `parse_datetime` (Joda) | `date_format` / `to_timestamp` + pattern conversion [D] |

## Class 4 - AUTOMATIC RENAMES (sqlglot handles; 62 verified in battery [B])

Top mappings (full 144-row table in function_mapping.md):
`cardinality`->`SIZE` - `array_agg`->`COLLECT_LIST` - `arbitrary`->`ANY_VALUE` - `approx_distinct`->`APPROX_COUNT_DISTINCT` - `approx_percentile`->`PERCENTILE_APPROX` - `reduce`->`AGGREGATE` - `strpos(s,x)`->`LOCATE(x,s)` (arg swap!) - `contains`->`ARRAY_CONTAINS` - `element_at`->`TRY_ELEMENT_AT` (matches Presto's NULL-on-missing) - `row()`->`STRUCT()` - `CAST(ROW .. AS ROW ..)`->`CAST(STRUCT .. AS STRUCT<..>)` (positional, incl. field renames - 52-field case verified **[M]**) - `json_extract`->`j:a` (variant path) - `json_extract_scalar`->`GET_JSON_OBJECT` - `date_diff('day',a,b)`->`DATEDIFF(DAY,a,b)` - `date_add('day',n,d)`->`DATEADD(DAY,n,d)` - `from_iso8601_*`->`CAST` - `md5`->`UNHEX(MD5(..))` (type-true) - `sha256`->`SHA2(x,256)` - `to/from_hex`->`HEX/UNHEX` - `to/from_base64`->`BASE64/UNBASE64` - `bitwise_*`->operators - `mod`->`%` - `truncate`->`CAST AS BIGINT` - `random`->`RAND` - `now`->`CURRENT_TIMESTAMP()` - `day_of_week`->`((DAYOFWEEK(d)%7)+1)` (Monday=1 alignment) - `last_day_of_month`->`LAST_DAY` - `levenshtein_distance`->`LEVENSHTEIN` - `starts_with`->`STARTSWITH` - `format`->`FORMAT_STRING` - `map()`->`MAP_FROM_ARRAYS` - `split`->`SPLIT` + regex-quoting (Presto splits on literal, Spark on regex - sqlglot injects `\Q..\E` **[B]**) - `FETCH FIRST n ROWS`->`LIMIT n` - `TABLESAMPLE BERNOULLI(10)`->`TABLESAMPLE (10 PERCENT)` - `regexp_extract` group-arg normalization.

## Class 5 - VERIFIED IDENTICAL (no action; 81 in battery [B], spot-probed live [M])

Higher-order functions (`filter`, `transform` + lambdas), `coalesce`, `nullif`, `if`, `greatest/least`,
`split_part`, `width_bucket`, `chr`, `arrays_overlap`, `slice` (both 1-based), `sequence`, `shuffle`,
`array_position` (0 = not found, both), `count_if`, `bool_and/or`, `max_by/min_by`, `concat_ws`,
`regexp_like/extract/replace` (pattern dialect caveat above), `lpad/rpad`, `trim`, `crc32`, most math.

---

## Hard-set additions (adversarial benchmark, measured live 7/7 - see hard_cases.py)

| Construct | Trino | DBSQL | Verdict |
|---|---|---|---|
| `date_add('day',1,DATE..)` return type | DATE | sqlglot's DATEADD returns TIMESTAMP **[M]** (sqlglot issue 5108 class) | silent type drift - rule-pack #6 |
| `1e0 / 0e0` | Infinity (IEEE) [D] | `DIVIDE_BY_ZERO` error under ANSI **[M]** | loud, needs `try_divide` |
| `arr \|\| element` (append) | `[1,2,3]` [D] | transpiles to concat -> loud type error **[M]** | rule-pack candidate |
| `ROW(1,2) < ROW(1,3)` / `ARRAY[1,2] < ARRAY[1,3]` | true | struct/array comparison WORKS, sqlglot converts correctly **[M]** | no action |
| `U&'\0041'` unicode literals | supported | sqlglot converts correctly **[M]** | no action |
| `WITH RECURSIVE` | supported | **SUPPORTED on DBSQL - measured, ran** **[M]** | no action |
| Window frame `EXCLUDE CURRENT ROW` | NOT supported either - verified on Trino 479 **[M]** (a SQL:2011 feature of PostgreSQL/DuckDB, wrongly assumed Trino) | not supported **[M]** | non-issue for this migration; case removed from benchmark |
| `MATCH_RECOGNIZE` | supported | `FEATURE_UNDER_DEVELOPMENT` **[M]** - the error message says feature under development | wait or manual; NOTE: LLM produced a plausible-but-WRONG rewrite **[M]** |
| `strpos(s, sub, N)` (Nth occurrence) | supported | no equivalent; sqlglot raises **[M]**; LLM rewrote correctly | LLM/manual lane |
| `TRY(int overflow)` | NULL | rule needed (`try_add`/`try_multiply`); LLM handled **[M]** | rule-pack #5 extension |
| `GROUPING SETS` + `grouping()` | - | transpiles AND matches row-for-row **[M]** | no action |
| `UNNEST` followed by `JOIN` | - | sqlglot orders LATERAL VIEW + JOIN correctly now **[M]** (old issue 1426 fixed); the LLM got the clause ORDER wrong (loud parse error) | prefer deterministic here |

| Implicit string/number coercion | STRICT: `CASE WHEN .. THEN varchar ELSE 0 END` and `varchar - bigint` are type errors **[M - Trino 479]** | PERMISSIVE: Spark coerces silently and runs **[M]** | converted SQL can run on DBSQL even where the source fails on Trino - masks schema-type mistakes, so certify with real data |

## Environment-level differences (not per-expression)

- **ANSI mode**: DBSQL default = errors on bad cast/overflow/div-zero; Presto errors similarly EXCEPT
  where TRY() was used. `ANSI_MODE=false` gets Trino-like NULL-on-error globally during migration [D].
- **Session timezone** drives TIMESTAMP rendering/arithmetic - pin identically on both sides of the A/B [D].
- **Ordinal GROUP BY / positional args**: both support; verified in job run **[M]**.
- **Query hierarchy**: catalog.schema.table on both, but Trino catalogs map to connectors; EG's two-part
  names resolve via the session catalog - set `catalog` in the DBSQL session to mirror **[M - harness]**.
- **Error taxonomy**: OOB array subscript errors on both (Spark: `INVALID_ARRAY_INDEX` under ANSI) **[M]**.

## How this catalog was built (and how to extend it)

1. sqlglot 30.12 battery (`diff_catalog.py`) - 144 expressions transpiled trino->databricks, classified.
2. Live probes (`probe_identical.py`) on a serverless warehouse for every pass-through function.
3. A large production job: transpiled (3 rules), executed end-to-end on shadow schemas,
   12/12 scenario suite covering filters, empty/NULL/null-element arrays, join hit+miss,
   time-window branches, and array_intersect pair-matching (`gen_scenarios.py`, `assert_scenarios.py`).
4. Anything tagged [D] belongs in the reconcile fixture before you trust it on prod data.
