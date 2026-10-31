# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Compose exact existing body upgrades on the caller's original Admin transport.

The caller authenticates complete routine custody, dispositions old consumers and
retains the common owner/catalog locks and deadline through commit. This grants
no authority and never issues a profile or commits a transaction.
"""

from copy import deepcopy

from sqlalchemy.ext.asyncio import AsyncConnection

from db import migration_cms_noncurrent_seal as lifecycle
from db import migration_cms_ptg_json_resolution as ptg_json
from db import migration_cms_replay_resolution as replay
from db import migration_cms_retirement_resolution as retirement
from db import migration_cms_snapshot_allocator as allocator
from db import migration_cms_terminal_resolution as terminal
from db import migration_generic_source_candidates as copy_guard
from process import provider_directory_cms_transition_routines as transition


def _require(condition, code):
    if not condition:
        raise ValueError("cms_routine_bodies_" + code)


def _targets(schema, preparation_schema):
    old_allocator, previous_allocator = allocator.allocator_bodies(schema)
    bound_allocator = allocator.bound_cms_allocator_body(schema)
    generic = copy_guard._canonical_copy_guard_bodies(schema, preparation_schema)[1]
    cms = copy_guard._cms_canonical_copy_guard_body(schema, preparation_schema)
    pairs_by_name = {
        lifecycle.GUARD: lifecycle.lifecycle_guard_bodies(schema),
        "guard_pd_generic_dataset_finalization": allocator.generic_finalization_bodies(schema),
        replay.GUARD: replay.replay_guard_bodies(schema),
        terminal.GUARD: terminal.terminal_guard_bodies(schema),
        ptg_json.FUNCTION: ptg_json.ptg_json_bodies(schema),
        **retirement.retirement_resolution_bodies(schema),
    }
    targets_by_name = {name: (tuple(bodies), bodies[1]) for name, bodies in pairs_by_name.items()}
    targets_by_name.update(
        prepare_pd_generic_dataset_storage=((old_allocator, previous_allocator, bound_allocator), bound_allocator),
        guard_pd_dataset_candidate_parent=((generic, cms), cms),
    )
    return targets_by_name


def cms_terminal_body_postimage(routines, *, catalog_schema, catalog_schema_oid, preparation_schema):
    """Derive only thirteen source-defined prosrc changes; all other raw fields stay exact."""
    transition.require_cms_transition_routine_definitions(
        routines,
        catalog_schema=catalog_schema,
        catalog_schema_oid=catalog_schema_oid,
        preparation_schema=preparation_schema,
    )
    targets_by_name = _targets(catalog_schema, preparation_schema)
    expected, matched_names = deepcopy(routines), set()
    for routine in expected:
        native = routine["native"]
        definition = native["definition"]
        name, original = definition["proname"], definition["prosrc"]
        if name not in targets_by_name:
            continue
        allowed, target_body = targets_by_name[name]
        _require(name not in matched_names and original in allowed, "predecessor_changed")
        _require(native["body"].count(original) == 1, "declaration_changed")
        definition["prosrc"] = target_body
        native["body"] = native["body"].replace(original, target_body, 1)
        matched_names.add(name)
    _require(matched_names == targets_by_name.keys(), "body_inventory_incomplete")
    transition.require_cms_transition_routine_definitions(
        expected,
        catalog_schema=catalog_schema,
        catalog_schema_oid=catalog_schema_oid,
        preparation_schema=preparation_schema,
    )
    return expected


async def cms_body_admin_driver(admin_connection):
    """Borrow the active real SQLAlchemy Admin connection; never open another transport."""
    _require(
        isinstance(admin_connection, AsyncConnection)
        and not admin_connection.closed
        and not admin_connection.invalidated
        and admin_connection.in_transaction()
        and not admin_connection.in_nested_transaction(),
        "original_admin_connection_required",
    )
    driver = (await admin_connection.get_raw_connection()).driver_connection
    _require(not driver.is_closed() and driver.is_in_transaction(), "original_native_transaction_required")
    return driver


def install_cms_terminal_bodies(connection, schema, preparation_schema, routines):
    """Reuse the fixed installers in one savepoint; the caller retains outer rollback."""
    targets_by_name = _targets(schema, preparation_schema)
    pins_by_name = {
        row["native"]["definition"]["proname"]: {
            "oid": row["oid"],
            "owner_oid": int(row["native"]["definition"]["proowner"]),
        }
        for row in routines
        if row["native"]["definition"]["proname"] in targets_by_name
    }
    _require(pins_by_name.keys() == targets_by_name.keys(), "body_inventory_incomplete")
    with connection.begin_nested():
        allocator.upgrade_cms_snapshot_allocator(
            connection, schema, expected=pins_by_name["prepare_pd_generic_dataset_storage"]
        )
        lifecycle.upgrade_cms_noncurrent_seal(connection, schema, expected=pins_by_name[lifecycle.GUARD])
        allocator.upgrade_cms_generic_finalization_guard(
            connection, schema, expected=pins_by_name["guard_pd_generic_dataset_finalization"]
        )
        replay.upgrade_cms_replay_guard_resolution(connection, schema, expected=pins_by_name[replay.GUARD])
        terminal.upgrade_cms_terminal_guard_resolution(connection, schema, expected=pins_by_name[terminal.GUARD])
        ptg_json.upgrade_cms_ptg_json_resolution(connection, schema, expected=pins_by_name[ptg_json.FUNCTION])
        retirement.upgrade_cms_retirement_resolution(
            connection, schema, expected={name: pins_by_name[name] for name in retirement.ORIGINAL_SHA256}
        )
        copy_guard.upgrade_generic_source_canonical_copy_guard(
            connection, schema, preparation_schema=preparation_schema, include_cms=True
        )
