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
_READ_SQL = f"""WITH requested(name) AS (SELECT unnest($1::text[]))
 SELECT c.relname,n.nspname,c.relkind::text,{_BOUND_JSON_SQL} AS bound
 FROM requested JOIN pg_catalog.pg_namespace n ON n.nspname=$2
 JOIN pg_catalog.pg_class c ON c.relnamespace=n.oid AND c.relname=requested.name ORDER BY c.relname"""
_SOURCE_READ_SQL = f"""WITH requested(root,name) AS (
 SELECT * FROM ROWS FROM (unnest($1::text[]),unnest($2::text[])))
 SELECT requested.root,c.relname,n.nspname,c.relkind::text,
 ARRAY(SELECT pn.nspname||'.'||p.relname FROM pg_inherits h JOIN pg_class p ON p.oid=h.inhparent
 JOIN pg_namespace pn ON pn.oid=p.relnamespace WHERE h.inhrelid=c.oid ORDER BY pn.nspname,p.relname) AS parents,
 NOT EXISTS(SELECT FROM pg_catalog.pg_inherits WHERE inhparent=c.oid) AS leaf,
 canonical.relkind::text AS canonical_kind,
 ARRAY(SELECT pn.nspname||'.'||p.relname FROM pg_inherits h JOIN pg_class p ON p.oid=h.inhparent
 JOIN pg_namespace pn ON pn.oid=p.relnamespace WHERE h.inhrelid=canonical.oid ORDER BY pn.nspname,p.relname) AS canonical_parents,
 {_BOUND_JSON_SQL} AS bound FROM requested
 JOIN pg_catalog.pg_namespace n ON n.nspname=$3
 JOIN pg_catalog.pg_class c ON c.relnamespace=n.oid AND c.relname=requested.name
 JOIN pg_catalog.pg_class canonical ON canonical.relnamespace=n.oid AND canonical.relname=requested.root
 ORDER BY requested.root"""


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
    return {
        "column": deepcopy(column),
        "constraint": {
            key: deepcopy(bound_value) for key, bound_value in bound.items() if key not in {"expression", "oid", "tree"}
        },
    }


def require_cms_history_expectations(expectations_by_root):
    """Detach declared history names and complete logical dataset bounds.

    Existing stored mappings may carry obsolete native metadata alongside a
    declared relation; that metadata is never compared with live physical IDs.
    An OID alone cannot identify a portable snapshot.
    """
    _require(
        type(expectations_by_root) is dict and set(expectations_by_root) == set(history.ROOTS), "expectations_required"
    )
    expected_by_root = {}
    for root, entry in expectations_by_root.items():
        _require(
            type(entry) is dict
            and {"relation", "dataset_ids"} <= set(entry)
            and set(entry) <= {"relation", "dataset_ids", "native", "old_oid"},
            "expectations_required",
        )
        relation = entry["relation"]
        _require(type(relation) is dict and set(relation) == {"schema", "name"}, "relation_required")
        for value in relation.values():
            storage_identifier(value)
        history._bounds(entry["dataset_ids"])
        expected_by_root[root] = {"relation": dict(relation), "dataset_ids": list(entry["dataset_ids"])}
    _require(
        len({(entry["relation"]["schema"], entry["relation"]["name"]) for entry in expected_by_root.values()}) == 2,
        "name_collision",
    )
    return expected_by_root


async def require_cms_source_history_bounds(connection, *, catalog_schema, expected, owner_oid=None):
    """Check the declared heap's current enforced bound and native parent edge.

    The legacy owner keyword is ignored; PostgreSQL controls access. Neither
    table IDs nor owner IDs form part of the stored or returned contract.
    """
    storage_identifier(catalog_schema)
    expected = require_cms_history_expectations(expected)
    _require(all(entry["relation"]["schema"] == catalog_schema for entry in expected.values()), "identity_changed")
    bound_rows = await connection.fetch(
        _SOURCE_READ_SQL,
        list(history.ROOTS),
        [expected[root]["relation"]["name"] for root in history.ROOTS],
        catalog_schema,
    )
    _require(
        len(bound_rows) == 2 and {bound_row["root"] for bound_row in bound_rows} == set(history.ROOTS),
        "inventory_changed",
    )
    for bound_row in bound_rows:
        root = bound_row["root"]
        entry = expected[root]
        is_original = bound_row["relname"] == root and bound_row["canonical_kind"] == "r" and bound_row["parents"] == []
        is_converted = (
            bound_row["relname"] != root
            and bound_row["canonical_kind"] == "p"
            and bound_row["parents"] == ([catalog_schema + "." + root] if entry["dataset_ids"] else [])
        )
        _require(
            bound_row["relname"] == entry["relation"]["name"]
            and bound_row["nspname"] == catalog_schema
            and bound_row["relkind"] == "r"
            and bound_row["leaf"] is True
            and bound_row["canonical_parents"] == []
            and (is_original or is_converted),
            "identity_changed",
        )
        require_cms_history_bound(bound_row["bound"], entry["dataset_ids"])
    return expected


async def read_cms_history_bounds(connection, plan):
    """Read enforced bounds by declared name without retaining physical IDs."""
    _require(type(plan) is dict and "request" in plan, "plan_required")
    _require(plan == history.plan_cms_history_conversion(plan["request"]), "plan_changed")
    request = plan["request"]
    old_by_name = {entry["old"]["name"]: entry for entry in request["heaps"].values()}
    rows = await connection.fetch(_READ_SQL, sorted(old_by_name), request["catalog_schema"])
    _require(len(rows) == 2 and {row["relname"] for row in rows} == set(old_by_name), "inventory_changed")
    proofs_by_name = {}
    for row in rows:
        _require(row["nspname"] == request["catalog_schema"] and row["relkind"] == "r", "identity_changed")
        proofs_by_name[row["relname"]] = require_cms_history_bound(
            row["bound"], old_by_name[row["relname"]]["dataset_ids"]
        )
    return proofs_by_name
