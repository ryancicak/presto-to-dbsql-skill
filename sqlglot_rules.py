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
