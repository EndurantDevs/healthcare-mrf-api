# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Recheck existing protected selections on the ordinary candidate's actual Builder."""

import asyncio
import importlib

from process import entity_address_dependency_bindings as bindings
from process import entity_address_ordinary_preparation as ordinary
from process import provider_directory_serving_recovery_receipts as receipts
from process import tiger_held_inputs as held
from process.provider_directory_serving_semantic_inputs import capture_semantic_dependencies


def _require(condition):
    if not condition:
        raise RuntimeError("entity_address_ordinary_dependencies_changed")


async def _owner(database, schema, dependency_bindings, issued, phases):
    ordinary.require_original(issued, phases)
    directory = importlib.import_module("process.provider_directory_fhir")
    capture = issued.capture
    capture._require_owner(directory)
    _require(database is issued.database and schema == directory._schema())
    session, _driver, actor = await receipts.ordinary(database)
    _require(actor["owner"] == capture.scope_payload["owner_oid"])
    _require(actor["database_oid"] == issued.admission.lease.database_oid)
    native = capture.predecessor["native"]
    actual_bindings = bindings.projection.validate_projection_dependency_bindings(schema, dependency_bindings)
    _require(actual_bindings == native["current_dependency_bindings"])
    return session, actor, native, actual_bindings


async def _same_owner(database, session, actor):
    current_session, _driver, current_actor = await receipts.ordinary(database)
    _require(current_session is session and current_actor == actor and session.in_transaction())


async def _lock_bindings(session, schema, native, actual_bindings):
    from process.entity_address_prepared_doctors import _assert_bindings

    selected = native["selected_relations"]
    for canonical in actual_bindings:
        name = canonical.split(".")[1]
        if name not in selected:
            continue
        expected = selected[name]
        relation = '"' + expected["schema_name"] + '"."' + name + '"'
        actual = await held._protected_relation(session, relation, expected_owner=expected["owner_oid"])
        _require(actual == expected)
    await _assert_bindings(session, schema, actual_bindings)


async def refresh_dependencies(database, schema, dependency_bindings, issued, *, applied=False):
    """Reobserve selection and published authorities at capture, finalization and the common cut."""
    phases = {"prepared"} if applied else {"building", "prepared"}
    session, actor, native, actual_bindings = await _owner(database, schema, dependency_bindings, issued, phases)
    _require(issued.task is asyncio.current_task())
    selected = await bindings.capture_selected_publication_bindings(
        session, schema, node_id=issued.capture._execution.attestation.node_id
    )
    selected_relations, current_bindings = (
        selected
        if selected is not None
        else ({}, await bindings.capture_publication_dependency_bindings(session, schema, {}))
    )
    _require(selected_relations == native["selected_relations"] and current_bindings == actual_bindings)
    _require(
        await capture_semantic_dependencies(session, schema, selected_relations) == native["semantic_dependency_proofs"]
    )
    await _lock_bindings(session, schema, native, actual_bindings)
    await _same_owner(database, session, actor)
    return actual_bindings


async def lock_dependencies(database, schema, dependency_bindings, issued):
    """Keep each executing backend on exact protected heaps without rescanning their full source graph."""
    session, actor, native, actual_bindings = await _owner(database, schema, dependency_bindings, issued, {"building"})
    await _lock_bindings(session, schema, native, actual_bindings)
    await _same_owner(database, session, actor)
