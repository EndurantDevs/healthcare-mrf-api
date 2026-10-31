# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Read fixed legacy bound prerequisites; never install constraints or grant custody."""

from copy import deepcopy

from process import provider_directory_cms_history_conversion as history
from process.provider_directory_cms_preparation_custody import _decoded

_BOUND_JSON_SQL = """pg_catalog.jsonb_build_object(
 'standard_strings',pg_catalog.current_setting('standard_conforming_strings')='on',
 'column',(SELECT pg_catalog.jsonb_build_object('number',attnum,'type',atttypid::bigint,
  'modifier',atttypmod,'required',attnotnull,'generated',attgenerated::text)
  FROM pg_catalog.pg_attribute WHERE attrelid=c.oid AND attname='dataset_id' AND NOT attisdropped),
 'constraints',COALESCE((SELECT pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
  'oid',k.oid::bigint,'kind',k.contype::text,'validated',k.convalidated,
  'enforced',COALESCE((pg_catalog.to_jsonb(k)->>'conenforced')::boolean,true),
  'local',k.conislocal,'inherited',k.coninhcount,'parent_oid',k.conparentid::bigint,
  'no_inherit',k.connoinherit,'deferrable',k.condeferrable,'deferred',k.condeferred,
  'columns',COALESCE(k.conkey,ARRAY[]::smallint[]),'tree',k.conbin::text,
  'expression',pg_catalog.pg_get_expr(k.conbin,k.conrelid,true)) ORDER BY k.oid)
  FROM pg_catalog.pg_constraint k WHERE k.conrelid=c.oid AND k.conname='cms_history_bound'),'[]'::jsonb))"""
_READ_SQL = f"""SELECT c.oid::bigint,c.relname,n.nspname,c.relowner::bigint,c.relkind::text,
 {_BOUND_JSON_SQL} AS bound FROM pg_catalog.pg_class c
 JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
 WHERE c.oid=ANY($1::oid[]) ORDER BY c.oid"""


def _require(condition, code):
    if not condition:
        raise ValueError("cms_history_bounds_" + code)


def _expression(dataset_ids, standard_strings):
    """Match the fixed varchar(96) IN expression deparsed by PostgreSQL 18."""
    history._bounds(dataset_ids)
    _require(type(standard_strings) is bool, "setting_changed")
    literals = [
        "'" + (value if standard_strings else value.replace("\\", "\\\\")).replace("'", "''") + "'"
        for value in dataset_ids
    ]
    if not literals:
        return "false"
    if len(literals) == 1:
        return "dataset_id::text = " + literals[0] + "::text"
    return (
        "dataset_id::text = ANY (ARRAY[" + ", ".join(value + "::character varying" for value in literals) + "]::text[])"
    )


def require_cms_history_bound(observed, dataset_ids):
    """Require an enforced, validated, exact fixed CHECK; return only its stable native pin."""
    observed = _decoded(observed)
    _require(
        type(observed) is dict and set(observed) == {"standard_strings", "column", "constraints"}, "record_required"
    )
    column = observed["column"]
    _require(
        type(column) is dict
        and set(column) == {"number", "type", "modifier", "required", "generated"}
        and type(column["number"]) is int
        and column["number"] > 0
        and column["type"] == 1043
        and column["modifier"] == 100
        and column["required"] is True
        and column["generated"] == "",
        "column_changed",
    )
    entries = observed["constraints"]
    _require(type(entries) is list and len(entries) == 1 and type(entries[0]) is dict, "validated_bound_required")
    bound = entries[0]
    expected_by_field = dict(
        kind="c",
        validated=True,
        enforced=True,
        local=True,
        inherited=0,
        parent_oid=0,
        no_inherit=False,
        deferrable=False,
        deferred=False,
        columns=[column["number"]] if dataset_ids else [],
        expression=_expression(dataset_ids, observed["standard_strings"]),
    )
    _require(
        set(bound) == set(expected_by_field) | {"oid", "tree"}
        and all(
            type(bound[key]) is type(expected_value) and bound[key] == expected_value
            for key, expected_value in expected_by_field.items()
        )
        and all(type(number) is int for number in bound["columns"])
        and type(bound["tree"]) is str
        and bool(bound["tree"]),
        "validated_bound_changed",
    )
    history._oid(bound["oid"])
    return {
        "column": deepcopy(column),
        "constraint": {key: deepcopy(bound_value) for key, bound_value in bound.items() if key != "expression"},
    }


async def read_cms_history_bounds(connection, plan):
    """Read two authenticated old OIDs before COPY; caller owns authority, custody and deadline."""
    _require(type(plan) is dict and "request" in plan, "plan_required")
    _require(plan == history.plan_cms_history_conversion(plan["request"]), "plan_changed")
    request = plan["request"]
    old_by_oid = {entry["old"]["oid"]: entry for entry in request["heaps"].values()}
    rows = await connection.fetch(_READ_SQL, sorted(old_by_oid))
    _require(len(rows) == 2 and {row["oid"] for row in rows} == set(old_by_oid), "inventory_changed")
    proofs_by_oid = {}
    for row in rows:
        old = old_by_oid[row["oid"]]["old"]
        _require(
            row["relname"] == old["name"]
            and row["nspname"] == request["catalog_schema"]
            and row["relowner"] == old["owner_oid"]
            and row["relkind"] == "r",
            "identity_changed",
        )
        proofs_by_oid[str(row["oid"])] = require_cms_history_bound(row["bound"], old_by_oid[row["oid"]]["dataset_ids"])
    return proofs_by_oid
