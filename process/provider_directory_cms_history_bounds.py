# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Read fixed legacy bound prerequisites; never install constraints or grant custody."""

from copy import deepcopy

from db.plan_release_storage import storage_identifier
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
_SOURCE_READ_SQL = f"""WITH requested(root,old_oid) AS (
 SELECT * FROM ROWS FROM (pg_catalog.unnest($1::text[]),pg_catalog.unnest($2::oid[])))
 SELECT requested.root,c.oid::bigint,c.relname,n.nspname,c.relowner::bigint,c.relkind::text,
 ARRAY(SELECT inhparent::bigint FROM pg_catalog.pg_inherits WHERE inhrelid=c.oid ORDER BY inhparent) AS parents,
 NOT EXISTS(SELECT FROM pg_catalog.pg_inherits WHERE inhparent=c.oid) AS leaf,
 canonical.oid::bigint AS canonical_oid,canonical.relowner::bigint AS canonical_owner,
 canonical.relkind::text AS canonical_kind,
 ARRAY(SELECT inhparent::bigint FROM pg_catalog.pg_inherits
  WHERE inhrelid=canonical.oid ORDER BY inhparent) AS canonical_parents,
 {_BOUND_JSON_SQL} AS bound
 FROM requested JOIN pg_catalog.pg_class c ON c.oid=requested.old_oid
 JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
 JOIN pg_catalog.pg_class canonical ON canonical.relnamespace=c.relnamespace AND canonical.relname=requested.root
 WHERE n.nspname=$3 ORDER BY requested.root"""


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


def require_cms_history_bound(observed, dataset_ids, *, validated=True):
    """Require the exact enforced CHECK state; pending validation is only a prework data pin."""
    _require(type(validated) is bool, "validation_state_required")
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
        and type(column["type"]) is int
        and column["type"] == 1043
        and type(column["modifier"]) is int
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
        validated=validated,
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


def require_cms_history_expectations(expectations_by_root):
    """Validate and detach the two bound data pins; this does not establish custody or authority."""
    _require(
        type(expectations_by_root) is dict and set(expectations_by_root) == set(history.ROOTS), "expectations_required"
    )
    expected = deepcopy(expectations_by_root)
    old_oids, constraint_oids = set(), set()
    for entry in expected.values():
        _require(type(entry) is dict and set(entry) == {"old_oid", "dataset_ids", "native"}, "expectations_required")
        history._oid(entry["old_oid"])
        history._bounds(entry["dataset_ids"])
        native = entry["native"]
        _require(
            type(native) is dict
            and set(native) == {"column", "constraint"}
            and type(native["constraint"]) is dict
            and "expression" not in native["constraint"],
            "native_pin_required",
        )
        checked = require_cms_history_bound(
            {
                "standard_strings": True,
                "column": native["column"],
                "constraints": [{**native["constraint"], "expression": _expression(entry["dataset_ids"], True)}],
            },
            entry["dataset_ids"],
        )
        _require(checked == native, "native_pin_changed")
        old_oids.add(entry["old_oid"])
        constraint_oids.add(checked["constraint"]["oid"])
    _require(len(old_oids) == len(constraint_oids) == len(history.ROOTS), "oid_collision")
    return expected


async def require_cms_source_history_bounds(connection, *, catalog_schema, expected, owner_oid=None):
    """Check bound pins in fixed source-root context; caller authenticates root ownership and custody.

    A retained name is accepted only beside its same-owner canonical partitioned
    root, with its exact planned parent (or no parent for an empty bound).
    """
    storage_identifier(catalog_schema)
    if owner_oid is not None:
        history._oid(owner_oid)
    expected = require_cms_history_expectations(expected)
    bound_rows = await connection.fetch(
        _SOURCE_READ_SQL, list(history.ROOTS), [expected[root]["old_oid"] for root in history.ROOTS], catalog_schema
    )
    _require(
        len(bound_rows) == len(history.ROOTS) and {bound_row["root"] for bound_row in bound_rows} == set(history.ROOTS),
        "inventory_changed",
    )
    for bound_row in bound_rows:
        root = bound_row["root"]
        entry = expected[root]
        for key in ("oid", "relowner", "canonical_oid", "canonical_owner"):
            history._oid(bound_row[key])
        is_original = (
            bound_row["relname"] == root
            and bound_row["canonical_oid"] == entry["old_oid"]
            and bound_row["canonical_kind"] == "r"
            and bound_row["parents"] == []
        )
        is_converted = (
            bound_row["relname"] == "cms_history_" + str(entry["old_oid"])
            and bound_row["canonical_oid"] != entry["old_oid"]
            and bound_row["canonical_kind"] == "p"
            and bound_row["parents"] == ([bound_row["canonical_oid"]] if entry["dataset_ids"] else [])
        )
        _require(
            bound_row["oid"] == entry["old_oid"]
            and bound_row["nspname"] == catalog_schema
            and bound_row["relowner"] == bound_row["canonical_owner"]
            and (owner_oid is None or bound_row["relowner"] == owner_oid)
            and bound_row["relkind"] == "r"
            and bound_row["leaf"] is True
            and bound_row["canonical_parents"] == []
            and (is_original or is_converted),
            "identity_changed",
        )
        _require(
            require_cms_history_bound(bound_row["bound"], entry["dataset_ids"]) == entry["native"], "native_pin_changed"
        )
    return expected


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
