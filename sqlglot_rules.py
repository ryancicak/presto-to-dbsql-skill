"""Presto/Trino -> Databricks SQL transpile with custom gap rules.

Usage: python sqlglot_rules.py <input_presto.sql> <output_databricks.sql>

Rule 1: TRY(CAST(x AS t)) -> TRY_CAST(x AS t); TRY(<field access>) -> <field access>
        (Spark returns NULL for field access on NULL structs, so the wrapper is redundant)
Rule 2: CROSS JOIN UNNEST(arr) AS t              -> LATERAL VIEW EXPLODE(arr) t_lv AS t
        CROSS JOIN UNNEST(arr) AS p (c1, ..., cn) -> LATERAL VIEW INLINE(arr) p AS c1, ..., cn
Rule 2b: Trino exposes unnested row-fields as bare columns; Spark's EXPLODE-as-struct does not.
        Qualify bare references to element fields with the unnest alias in affected scopes.
        Field list comes from the job's own ROW type decls here; a production pipeline should
        introspect the array column's element schema from the catalog.
Rule 3: struct field-name alignment inside ARRAY_INTERSECT / ARRAY_EXCEPT / ARRAY_UNION.
Rule 6: date_add(unit, n, d) on a DATE -> DATE-preserving Databricks form. sqlglot emits
        dateadd(MONTH, n, d) which silently widens DATE -> TIMESTAMP. Route month/quarter/year
        to add_months (DATE, month-end clamp matches Trino) and day/week to 2-arg date_add.
Rule 6b: date_trunc on a DATE input -> trunc() to keep DATE (DBSQL date_trunc always returns
        TIMESTAMP). sqlglot already does this for provably-DATE args (DATE literals, CAST AS DATE);
        for bare columns, name the DATE ones via DATE_COLS env (like ELEM_FIELDS).
Rule 7: year_of_week(x) / yow(x) -> extract(YEAROFWEEK FROM x). sqlglot otherwise emits a
        literal YEAR_OF_WEEK() call, which is NOT a Databricks function (silent-wrong at runtime).
Rule 8: lateral column alias in a window -> inline the aliased expression into OVER(). DBSQL
        raises UNSUPPORTED_FEATURE.LATERAL_COLUMN_ALIAS_IN_WINDOW when a SELECT-list alias is
        referenced inside a window spec; a faithful Presto source repeats the full expression
        there anyway. Non-deterministic aliases are flagged, not duplicated.
(Rules 4-5 - integer division and repeat()->array_repeat - are handled by stock sqlglot; see
 SKILL.md for the full numbered rule list. This script implements the AST passes 1-3 and 6-8.)
"""
import sys
import sqlglot
from sqlglot import exp

# empirical check: how does sqlglot model LATERAL VIEW in Spark?
probe = sqlglot.parse_one(
    "SELECT t.a FROM tbl LATERAL VIEW EXPLODE(arr) t_lv AS t", read="databricks"
)
print("LATERAL VIEW AST shape:", repr(probe.args.get("laterals")) is not None and "laterals" in probe.args)
lat_node = probe.args["laterals"][0] if probe.args.get("laterals") else None
print("Lateral node:", lat_node.sql("databricks") if lat_node is not None else "NOT FOUND")

src = open(sys.argv[1] if len(sys.argv) > 1 else "input_presto.sql").read()
tree = sqlglot.parse_one(src, read="trino")

# ---- Rule 1: TRY ----
try_cast_count = 0
try_drop_count = 0

# TRYs nest inside other TRYs' subtrees, so replace to fixpoint without copying
# (a copy would detach not-yet-processed inner TRY nodes from the live tree)
while True:
    t = tree.find(exp.Try)
    if t is None:
        break
    inner = t.this
    if isinstance(inner, exp.Cast):
        t.replace(exp.TryCast(this=inner.this, to=inner.args["to"]))
        try_cast_count += 1
    else:
        t.replace(inner)
        try_drop_count += 1

survivors = len(list(tree.find_all(exp.Try)))
print(f"\nRule 1: TRY(CAST)->TRY_CAST: {try_cast_count}, TRY(field)->field: {try_drop_count}, survivors: {survivors}")

# ---- Rule 2: CROSS JOIN UNNEST -> LATERAL VIEW ----
# Rule 2b needs the unnested array's element field names: Trino exposes them as bare
# columns, Spark does not. Provide them via ELEM_FIELDS env var (comma-separated), or
# introspect information_schema in a production pipeline.
import os
ELEM_FIELDS = set(filter(None, os.environ.get("ELEM_FIELDS", "").split(",")))
unnest_fixed = 0
bare_qualified = 0
for select in tree.find_all(exp.Select):
    joins = select.args.get("joins") or []
    keep_joins = []
    for join in joins:
        unnest = join.find(exp.Unnest)
        if unnest is None:
            keep_joins.append(join)
            continue
        arr = unnest.expressions[0].copy()
        alias = unnest.args.get("alias")
        tbl_alias = alias.name if alias else "u"
        col_names = [c.name for c in (alias.columns if alias else [])]
        if col_names:
            fn = exp.Anonymous(this="INLINE", expressions=[arr])
            lat_alias = exp.TableAlias(
                this=exp.to_identifier(tbl_alias),
                columns=[exp.to_identifier(c) for c in col_names],
            )
        else:
            fn = exp.Explode(this=arr)
            lat_alias = exp.TableAlias(
                this=exp.to_identifier(f"{tbl_alias}_lv"),
                columns=[exp.to_identifier(tbl_alias)],
            )
            # Rule 2b: bare element-field refs in this scope must become t.<field>
            for col in select.find_all(exp.Column):
                if not col.table and col.name in ELEM_FIELDS:
                    col.set("table", exp.to_identifier(tbl_alias))
                    bare_qualified += 1
        lateral = exp.Lateral(this=fn, view=True, alias=lat_alias)
        select.args.setdefault("laterals", []).append(lateral)
        unnest_fixed += 1
    select.set("joins", keep_joins)

print(f"Rule 2: CROSS JOIN UNNEST -> LATERAL VIEW: {unnest_fixed}")
print(f"Rule 2b: bare element-field refs qualified: {bare_qualified}")

# ---- Rule 3: positional struct-field names in array set-ops ----
# Presto ROW() is anonymous and compares positionally; Spark STRUCT() carries field
# names derived from source columns, and ARRAY_INTERSECT/EXCEPT refuse arrays whose
# struct field names differ. Rename fields positionally (_f0.._fn) on every struct
# constructor feeding an array set-op, on all sides.
SET_OPS = {"ARRAY_INTERSECT", "ARRAY_EXCEPT", "ARRAY_UNION", "ARRAYS_OVERLAP"}
set_op_nodes = [n for n in tree.find_all(exp.Anonymous) if str(n.this).upper() in SET_OPS]
for cls_name in ("ArrayIntersect", "ArrayExcept", "ArrayUnion"):
    cls = getattr(exp, cls_name, None)
    if cls is not None:
        set_op_nodes += list(tree.find_all(cls))
structs_renamed = 0
for node in set_op_nodes:
    for st in node.find_all(exp.Struct):
        new_exprs = []
        for i, e in enumerate(st.expressions):
            inner = e.this if isinstance(e, exp.Alias) else e
            new_exprs.append(exp.alias_(inner.copy(), f"_f{i}"))
        st.set("expressions", new_exprs)
        structs_renamed += 1
print(f"Rule 3: struct constructors positionally renamed in array set-ops: {structs_renamed}")

# ---- Rule 6: date_add(unit, n, d) on DATE -> DATE-preserving form ----
# Trino date_add returns the input type (DATE in -> DATE out). sqlglot renders the databricks
# 3-arg DATEADD(unit, n, d), whose declared input is TIMESTAMP, so a DATE silently widens to
# TIMESTAMP. Route month/quarter/year to add_months (returns DATE, non-sticky month-end clamp
# matches Trino's Joda semantics), and day/week to the 2-arg date_add (returns DATE).
DATEADD_MONTH_MULT = {"MONTH": 1, "QUARTER": 3, "YEAR": 12}
dateadd_fixed = 0
for da in list(tree.find_all(exp.DateAdd)):
    unit = (da.args.get("unit").name if da.args.get("unit") else "DAY").upper()
    x = da.this.copy()
    n = da.expression.copy()
    if unit in DATEADD_MONTH_MULT:
        k = DATEADD_MONTH_MULT[unit]
        months = n if k == 1 else exp.Mul(this=exp.Literal.number(k), expression=n)
        da.replace(exp.Anonymous(this="add_months", expressions=[x, months]))
        dateadd_fixed += 1
    elif unit in ("DAY", "WEEK"):
        days = n if unit == "DAY" else exp.Mul(this=exp.Literal.number(7), expression=n)
        da.replace(exp.Anonymous(this="date_add", expressions=[x, days]))
        dateadd_fixed += 1
    # sub-day units (hour/minute/...) only occur on TIMESTAMP inputs; leave those to sqlglot
print(f"Rule 6: date_add unit forms rewritten (DATE-preserving): {dateadd_fixed}")

# ---- Rule 6b: date_trunc on a DATE input -> trunc() to preserve the DATE type ----
# Trino date_trunc is type-preserving (DATE in -> DATE out); DBSQL date_trunc ALWAYS returns
# TIMESTAMP, so a DATE column silently widens. sqlglot ALREADY emits trunc() when the argument is
# provably DATE (a DATE literal or CAST(.. AS DATE)). The remaining gap is a BARE COLUMN whose
# type sqlglot can't see: name those via DATE_COLS (comma-separated), like ELEM_FIELDS, sourced
# from information_schema in a real pipeline. Only month/quarter/year/week have a DATE-returning
# trunc(); finer units stay date_trunc (they imply a TIMESTAMP input anyway).
DATE_COLS = set(filter(None, os.environ.get("DATE_COLS", "").split(",")))
TRUNC_UNITS = {"YEAR", "QUARTER", "MONTH", "WEEK"}
dtrunc_fixed = 0
if DATE_COLS:
    for tt in list(tree.find_all(exp.TimestampTrunc)):
        arg = tt.this
        unit = (tt.args.get("unit").name if tt.args.get("unit") else "").upper()
        if isinstance(arg, exp.Column) and arg.name in DATE_COLS and unit in TRUNC_UNITS:
            tt.replace(exp.Anonymous(this="trunc", expressions=[arg.copy(), exp.Literal.string(unit)]))
            dtrunc_fixed += 1
print(f"Rule 6b: date_trunc on named DATE columns -> trunc (DATE-preserving): {dtrunc_fixed}")

# ---- Rule 7: year_of_week / yow -> extract(YEAROFWEEK FROM x) ----
# sqlglot maps year_of_week to a YearOfWeek node that renders as YEAR_OF_WEEK(x) in the
# databricks dialect - but Spark/DBSQL has no such function. The equivalent is the
# YEAROFWEEK extract field (same ISO-8601 week-numbering-year semantics, verified in docs).
yow_fixed = 0
for n in list(tree.find_all(exp.YearOfWeek)):
    n.replace(exp.Extract(this=exp.var("YEAROFWEEK"), expression=n.this.copy()))
    yow_fixed += 1
for n in list(tree.find_all(exp.Anonymous)):
    if str(n.this).lower() == "yow" and n.expressions:
        n.replace(exp.Extract(this=exp.var("YEAROFWEEK"), expression=n.expressions[0].copy()))
        yow_fixed += 1
print(f"Rule 7: year_of_week/yow -> extract(YEAROFWEEK FROM ..): {yow_fixed}")

# ---- Rule 8: lateral column alias inside a window -> inline the expression ----
# DBSQL raises UNSUPPORTED_FEATURE.LATERAL_COLUMN_ALIAS_IN_WINDOW when a SELECT-list alias is
# referenced inside OVER(...). Only inline when the bare column name matches a peer alias AND
# appears inside a window; leave real-table columns and non-window alias reuse alone. Inlining
# duplicates the expression, so flag (do not duplicate) non-deterministic sources.
NONDETERMINISTIC = {"rand", "random", "randn", "uuid", "current_timestamp", "now",
                    "current_date", "shuffle"}

def _is_nondeterministic(node):
    for fn in node.find_all(exp.Func, exp.Anonymous):
        name = str(getattr(fn, "this", "")).lower() if isinstance(fn, exp.Anonymous) else fn.key
        if name in NONDETERMINISTIC:
            return True
    for cls_name in ("CurrentTimestamp", "CurrentDate", "Rand"):
        cls = getattr(exp, cls_name, None)
        if cls is not None and node.find(cls):
            return True
    return False

lca_inlined, lca_flagged = 0, 0
for select in tree.find_all(exp.Select):
    alias_map = {p.alias: p.this for p in select.expressions if isinstance(p, exp.Alias)}
    if not alias_map:
        continue
    for win in select.find_all(exp.Window):
        for col in list(win.find_all(exp.Column)):
            if col.table:
                continue
            target = alias_map.get(col.name)
            if target is None:
                continue
            if _is_nondeterministic(target):
                lca_flagged += 1
                continue
            col.replace(target.copy())
            lca_inlined += 1
print(f"Rule 8: lateral column aliases inlined into windows: {lca_inlined} (nondeterministic flagged: {lca_flagged})")
if lca_flagged:
    print("  WARNING: a non-deterministic SELECT-list alias is referenced inside a window; "
          "DBSQL will reject it and inlining would duplicate the expression - FLAG FOR HUMAN REVIEW.")

# ---- Generate with unsupported constructs raising loudly ----
out = tree.sql(dialect="databricks", pretty=True, unsupported_level=sqlglot.ErrorLevel.RAISE)
open(sys.argv[2] if len(sys.argv) > 2 else "output_databricks.sql", "w").write(out + ";\n")
print(f"\nGenerated {len(out)} chars with unsupported_level=RAISE (no silent drops)")

# ---- Validate ----
reparsed = sqlglot.parse(out, read="databricks")
print(f"Round-trip parse as Databricks dialect: OK ({len(reparsed)} statement)")

import re
for marker in ("TRY (", "TRY(", "UNNEST"):
    hits = len(re.findall(re.escape(marker), out, re.IGNORECASE))
    print(f"Residual '{marker}' in output: {hits}")
for marker in ("TRY_CAST", "LATERAL VIEW", "INLINE(", "EXPLODE("):
    hits = len(re.findall(re.escape(marker), out, re.IGNORECASE))
    print(f"Introduced '{marker}': {hits}")
