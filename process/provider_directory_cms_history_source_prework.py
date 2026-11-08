# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Compare fixed history prework with an authenticated, unmodified Source record."""

import asyncio
import json
from copy import deepcopy

from process import provider_directory_cms_history_bounds as bounds
from process import provider_directory_source_storage_binding as storage
from process.provider_directory_source_callable_custody import _READ_IDENTITY_SQL

_BOUND_SQL = f"""SELECT native.*,
 pg_catalog.pg_get_constraintdef((native.bound->'constraints'->0->>'oid')::pg_catalog.oid) AS bound_definition
 FROM ({bounds._SOURCE_READ_SQL}) native"""


def _require(condition, code):
    if not condition:
        raise ValueError("cms_history_source_prework_" + code)


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def _expectations(expected, validated):
    _require(type(validated) is bool, "validation_state_required")
    _require(type(expected) is dict and set(expected) == set(bounds.history.ROOTS), "expectations_required")
    old_oids, constraint_oids = set(), set()
    for entry in expected.values():
        _require(type(entry) is dict and set(entry) == {"old_oid", "dataset_ids", "native"}, "expectations_required")
        bounds.history._oid(entry["old_oid"])
        bounds.history._bounds(entry["dataset_ids"])
        native = entry["native"]
        _require(
            type(native) is dict
            and set(native) == {"column", "constraint"}
            and type(native["constraint"]) is dict
            and "expression" not in native["constraint"],
            "expectations_required",
        )
        proof = bounds.require_cms_history_bound(
            dict(
                standard_strings=True,
                column=native["column"],
                constraints=[{**native["constraint"], "expression": bounds._expression(entry["dataset_ids"], True)}],
            ),
            entry["dataset_ids"],
            validated=validated,
        )
        _require(_canonical(proof) == _canonical(native), "expectations_required")
        old_oids.add(entry["old_oid"])
        constraint_oids.add(proof["constraint"]["oid"])
    _require(len(old_oids) == len(constraint_oids) == len(expected), "identity_changed")


async def _native_bounds(connection, coordinates_by_field, expected, validated):
    roots = sorted(expected)
    rows = await connection.fetch(
        _BOUND_SQL, roots, [expected[root]["old_oid"] for root in roots], coordinates_by_field["catalog_schema"]
    )
    _require(len(rows) == len(roots), "history_identity_changed")
    definitions_by_root = {}
    for row in rows:
        root = row["root"]
        _require(root in expected and root not in definitions_by_root, "history_identity_changed")
        entry = expected[root]
        _require(
            row["oid"] == row["canonical_oid"] == entry["old_oid"]
            and row["relname"] == root
            and row["nspname"] == coordinates_by_field["catalog_schema"]
            and row["relowner"] == row["canonical_owner"] == coordinates_by_field["namespace_owner_oid"]
            and row["relkind"] == row["canonical_kind"] == "r"
            and row["parents"] == row["canonical_parents"] == []
            and row["leaf"] is True,
            "history_identity_changed",
        )
        native = bounds.require_cms_history_bound(row["bound"], entry["dataset_ids"], validated=validated)
        _require(_canonical(native) == _canonical(entry["native"]), "history_bound_changed")
        _require(type(row["bound_definition"]) is str and bool(row["bound_definition"]), "history_bound_changed")
        definitions_by_root[root] = row["bound_definition"]
    return definitions_by_root


def _without_bounds(evidence, catalog_schema, expected, definitions_by_root):
    projected, removed = deepcopy(evidence), set()
    for layout in projected["layouts"]:
        root = layout["relname"]
        checks = layout["constraints"]
        selected_checks = [check for check in checks if check[0] == "cms_history_bound"]
        if layout["nspname"] != catalog_schema or root not in expected:
            _require(not selected_checks, "unrelated_history_bound")
            continue
        entry = expected[root]
        proof = entry["native"]["constraint"]
        _require(layout["oid"] == entry["old_oid"] and root not in removed, "history_identity_changed")
        expected_check_fields = [
            "cms_history_bound",
            "c",
            definitions_by_root[root],
            proof["validated"],
            False,
            False,
            True,
            proof["oid"],
        ]
        _require(_canonical(selected_checks) == _canonical([expected_check_fields]), "history_layout_changed")
        layout["constraints"] = [check for check in checks if check[0] != "cms_history_bound"]
        removed.add(root)
    _require(removed == set(expected), "history_layout_changed")
    return projected


async def require_cms_history_source_prework(connection, policy, *, source_document, history_bounds, bounds_validated):
    """Check pending or validated additions without issuing Source custody or changing its parser.

    The caller authenticates the genuine Source/profile and OriginalAdmin scope,
    retains the history/DDL fences, and owns the finite phase and commit clocks.
    Supplied bound pins are data; every native identity and Source field is rechecked.
    """
    _require(connection.is_in_transaction() is True and not connection.is_closed(), "transaction_required")
    source_document, history_bounds = deepcopy((source_document, history_bounds))
    _expectations(history_bounds, bounds_validated)
    original = source_document["storage_custody"]
    _require(
        source_document["version"] == 2 and set(original["canonical_authority"]) == {"admission", "metadata"},
        "unbound_source_required",
    )
    declaration, publication = source_document["policy"], source_document["publication"]
    catalog_schema = original["coordinates"]["catalog_schema"]
    task, actor, roles, coordinates_by_field = await storage._isolated_storage_identity(
        connection,
        policy,
        catalog_schema,
        declaration["migration_role"],
        declaration["namespace_owner_role"],
        publication,
    )
    coordinates_by_field = dict(
        catalog_schema=catalog_schema, storage_schema=policy.schema, **dict(coordinates_by_field)
    )
    _require(_canonical(coordinates_by_field) == _canonical(original["coordinates"]), "coordinates_changed")
    definitions_by_root = await _native_bounds(connection, coordinates_by_field, history_bounds, bounds_validated)
    evidence = await storage._isolated_catalog_rows(
        connection, policy, catalog_schema, roles, expected_history_bounds=history_bounds
    )
    projected = _without_bounds(evidence, catalog_schema, history_bounds, definitions_by_root)
    observed_by_field = dict(
        version=1,
        coordinates=coordinates_by_field,
        roles=roles,
        inventory=storage._isolated_inventory(policy, catalog_schema, roles, projected, expected_history_bounds=None),
        application_checks=storage._isolated_application_checks(projected),
        canonical_authority=await storage._isolated_canonical_authority(
            connection, policy, catalog_schema, coordinates_by_field["catalog_schema_oid"], roles, projected, None
        ),
    )
    await storage._isolated_access_closure(
        connection, observed_by_field["inventory"], roles, policy, declaration["migration_role"], publication
    )
    _require(_canonical(observed_by_field) == _canonical(original), "storage_changed")
    _require(
        await _native_bounds(connection, coordinates_by_field, history_bounds, bounds_validated) == definitions_by_root,
        "history_bound_changed",
    )
    _require(
        connection.is_in_transaction() is True
        and not connection.is_closed()
        and asyncio.current_task() is task
        and dict(await connection.fetchrow(_READ_IDENTITY_SQL)) == actor,
        "transaction_changed",
    )
    return json.loads(_canonical(observed_by_field))
