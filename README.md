# presto-to-dbsql-skill

I put this together while helping a team move a large Presto workload over to Databricks SQL.
It's three things in one repo: a [Genie Code skill](https://docs.databricks.com/aws/en/genie-code/skills)
that teaches the assistant the Presto-to-DBSQL dialect differences, the catalog of those
differences with how each one was verified, and a benchmark you can run yourself.

One thing I want to be upfront about: I didn't write this from documentation. Every rule in here
came from actually running SQL against a live Databricks SQL serverless warehouse (July 2026) and
checking the results against documented Trino behavior. The catalog tags every item with how it
was verified: [M] means measured live, [B] means verified through a
[sqlglot](https://github.com/tobymao/sqlglot) transpile battery of 144 expressions, [D] means
it's documented behavior I haven't personally executed.

## Why bother

The two dialects overlap at maybe 90% of the function surface, and the last 10% is the part that
will hurt you. Not because it errors, but because it doesn't. `SELECT 7 / 2` gives you 3 on
Presto and 3.5 on Databricks. `repeat('x', 3)` gives you an array on one and the string 'xxx' on
the other. Array subscripts are 1-based on one side and 0-based on the other. `array_agg(x ORDER
BY y)` quietly loses its ordering when it becomes `collect_list`. None of these fail. They just
return different numbers.

LLM converters (Genie Code included) handle most of this surprisingly well. Where they slip is
what I'd call plausible-wrong: the model writes a confident comment explaining a dialect fact
that isn't true, and the converted query runs fine and returns wrong rows. I measured this on a
20-case adversarial benchmark, running each converter's output against a live warehouse and
grading against documented Trino ground truth:

| Converter | Correct | Silently wrong | Flagged for review | Loud fail |
|---|---|---|---|---|
| Genie Code, no skill | 13 | 1 (+1 silently omitted) | 0 | 5 |
| Genie Code with this skill | 18 | 0 | 1 | 1 |
| stock sqlglot (RAISE mode) | 10 | 3 | 0 | 7 |
| sqlglot + rules in `sqlglot_rules.py` | 13 | 0 | 0 | 7 |

The skill isn't making the model smarter. It pins down verified dialect facts so the model stops
guessing, and it tells the model to flag constructs that have no DBSQL equivalent (like
`MATCH_RECOGNIZE`) instead of inventing a rewrite that runs and returns the wrong answer. That
flagging behavior is why the "silently wrong" column goes to zero, and it's the whole point.

The benchmark ships with 27 cases, every expected value verified against a live Trino 479 cluster. The table above was measured on the original 20-case set, which included one case (window frame EXCLUDE) that real-Trino testing later proved was never valid Trino SQL in the first place; it has been removed. That correction is a good example of why the verification loop exists.

## What's in here

- `skills/presto-to-dbsql/SKILL.md` is the skill itself: six deterministic rules, a function
  map, the semantic traps, and a verification workflow.
- `PRESTO_VS_DBSQL.md` is the full difference catalog with the provenance tags.
- `sqlglot_rules.py` is the deterministic path for bulk conversion: sqlglot reading trino and
  writing databricks, plus custom AST rules for the constructs stock sqlglot drops silently
  (`TRY()`, `CROSS JOIN UNNEST`, struct field names inside array set operations). It runs with
  `unsupported_level=RAISE` so anything unmapped fails loudly instead of disappearing.
- `benchmark/` has the 27 hard cases and a grader that runs converted SQL against your own
  warehouse. Configure it with `DATABRICKS_PROFILE`, `DATABRICKS_HOST`, `DBSQL_WAREHOUSE_ID`,
  and `DBSQL_CATALOG`.
- `examples/` has a fully synthetic job you can use to see the whole thing work end to end.

## Installing the skill

Workspace-wide, so every Genie Code user picks it up:

```bash
databricks workspace mkdirs "/Workspace/.assistant/skills/presto-to-dbsql"
databricks workspace import "/Workspace/.assistant/skills/presto-to-dbsql/SKILL.md" \
  --file skills/presto-to-dbsql/SKILL.md --format AUTO
```

You can also install it per-user under `/Users/<you>/.assistant/skills/presto-to-dbsql/`. Genie
Code finds it on its own the next time someone asks it to convert Presto SQL, or you can
@-mention it.

## Trying it end to end

`examples/synthetic_job_presto.sql` is a made-up analytics job (fictional streaming service)
that packs in every conversion-relevant pattern I've seen in real production Presto jobs: deep
CTE chains, both `CROSS JOIN UNNEST` alias forms, `ARRAY_AGG` over typed ROW casts with
positional renames, lambdas with nested `TRY()`, epoch-window FILTERs, `array_intersect` on
anonymous ROW pairs, ordinal GROUP BY. Convert it:

```bash
ELEM_FIELDS=item_id,slot_number python sqlglot_rules.py \
  examples/synthetic_job_presto.sql examples/synthetic_job_databricks.sql
```

Then run `examples/shadow_schemas.sql` on a warehouse. It creates the three fictional tables,
seeds a few rows, and documents the exact two rows the converted job should produce. If you get
those two rows back, you've just run the whole verification loop. I also ran this example
cross-engine: the Presto source on a real Trino 479 cluster and the converted output on
Databricks SQL, against the same Unity Catalog Iceberg tables, return identical rows. The
conversion script itself is deterministic (byte-identical output across runs), and as a scale
check, all 99 TPC-DS queries pass through it and come out accepted by the Databricks SQL parser.

## How I'd actually use this on a migration

1. Inventory your Presto corpus at the function level and diff it against the catalog, so you
   know your exposure before converting anything.
2. Bulk-convert with `sqlglot_rules.py`. It's deterministic, takes milliseconds, and fails
   loudly on anything it can't map.
3. Send the long tail through Genie Code with the skill installed.
4. Certify every job: `EXPLAIN` against the real tables, then run the source and converted
   versions on one partition and diff with `EXCEPT ALL`. Sort array columns first, because
   `array_agg` and `collect_list` ordering is nondeterministic on both engines. Don't skip this
   step because a conversion looks right. Looking right is exactly what the failure mode looks
   like.

## Caveats

- Personal project, not an official Databricks product, no affiliation with the Presto or Trino
  projects.
- Engines move. `MATCH_RECOGNIZE` currently errors as feature-under-development on DBSQL, which
  means it's presumably coming. The [M] items were true as of July 2026. Re-verify before
  betting production on them.
- The benchmark numbers are single runs. Agentic converters don't give you the same output twice.

## License

MIT
