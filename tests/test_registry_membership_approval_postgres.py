# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Approved membership never composes unapproved provider or office heads."""

import json
from dataclasses import replace
from uuid import UUID, uuid4

import pytest

from process.registry_approval_preview import preview_registry_approval
from process.registry_approval_store import RegistryApprovalConflict
from process.registry_record_store import RegistryRecordCommand
from tests.test_manual_location_identity_store_postgres import _create as location_command
from tests.test_manual_provider_identity_store_postgres import _apply, provider_db, serving_schema
from tests.test_manual_provider_identity_store_postgres import _create as provider_command
from tests.test_registry_approval_store_postgres import _actor, _approve, _command, _create

pytestmark = pytest.mark.asyncio


async def _records(fixture):
    connection, schema, sessions, _ = fixture
    actor = _actor()
    network = await _apply(sessions, schema, _create("network"), actor)
    provider = await _apply(sessions, schema, provider_command(), actor)
    location = await _apply(sessions, schema, location_command(), actor)
    membership = await _apply(
        sessions,
        schema,
        RegistryRecordCommand(
            "membership",
            network["record_id"],
            "create",
            0,
            {
                "memberships_json": [
                    {
                        "network_id": network["record_id"],
                        "provider_system": "manual",
                        "provider_id": provider["record_id"],
                        "location_id": location["record_id"],
                        "evidence_id": "selected-office-a",
                    }
                ]
            },
            "Explicit provider and office",
            uuid4().hex,
        ),
        actor,
    )
    return actor, network, provider, location, membership


async def test_preview_reports_unapproved_identities_and_approval_is_atomic(provider_db):
    connection, schema, _, _ = provider_db
    actor, network, provider, location, membership = await _records(provider_db)
    command = await _command(connection, schema, membership)
    async with connection.transaction():
        preview = await preview_registry_approval(connection, command, actor, control_schema=schema)
    assert preview["unresolved_count"] == 1
    assert preview["membership_diagnostics"] == {
        "membership_rows": 1,
        "unresolved_count": 1,
        "unapproved_network_count": 1,
        "unapproved_provider_count": 1,
        "unapproved_location_count": 1,
    }
    with pytest.raises(RegistryApprovalConflict, match="membership_unresolved"):
        await _approve(connection, schema, command, actor)
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_approval_history') == 0
    complete = await _command(connection, schema, network, provider, location, membership)
    async with connection.transaction():
        preview = await preview_registry_approval(connection, complete, actor, control_schema=schema)
    assert preview["unresolved_count"] == 0
    approved = await _approve(connection, schema, complete, actor)
    assert approved["selected_count"] == 4


async def test_archiving_used_office_requires_membership_removal_in_same_selection(provider_db):
    connection, schema, sessions, _ = provider_db
    actor, network, provider, location, membership = await _records(provider_db)
    await _approve(
        connection, schema, await _command(connection, schema, network, provider, location, membership), actor
    )
    archive = RegistryRecordCommand(
        "location", UUID(location["record_id"]), "archive", 1, {}, "Office retired", uuid4().hex
    )
    archived = await _apply(sessions, schema, archive, actor)
    command = await _command(connection, schema, archived)
    with pytest.raises(RegistryApprovalConflict, match="membership_unresolved"):
        await _approve(connection, schema, command, actor)
    cleared = await _apply(
        sessions,
        schema,
        RegistryRecordCommand(
            "membership",
            network["record_id"],
            "correct",
            1,
            {"memberships_json": []},
            "Remove exact office",
            uuid4().hex,
        ),
        actor,
    )
    approved = await _approve(connection, schema, await _command(connection, schema, archived, cleared), actor)
    approved_records = await connection.fetch(
        f'SELECT record_kind,record_json FROM "{schema}".registry_approved_record WHERE approved_revision=$1',
        approved["approved_revision"],
    )
    documents_by_kind = {
        approved_record["record_kind"]: json.loads(approved_record["record_json"])
        for approved_record in approved_records
    }
    assert documents_by_kind["location"]["archived"] and documents_by_kind["membership"]["memberships_json"] == []
