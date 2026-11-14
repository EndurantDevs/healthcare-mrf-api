# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Historical queued address pins stay exact without making wire IDs authoritative."""

import json
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from process import network_address_artifact as artifact
from process import registry_membership_refresh_runtime as runtime
from process.network_address_projection import PinnedAddressSource
from process.registry_approval_store import RegistryApprovalConflict
from process.registry_membership_refresh_queue import (
    _current_heads,
    _require_address,
    validated_membership_refresh_request_record,
)
from process.registry_publication_queue import _request_metadata
from tests.test_network_address_artifact_postgres import address_db as address_db
from tests.test_registry_membership_refresh_contract import envelope
from tests.test_registry_membership_refresh_queue_postgres import refresh_queue_db as refresh_queue_db

pytestmark = pytest.mark.asyncio


async def legacy_descriptor(fixture):
    wire_value = await fixture.admin.fetchval(
        f'SELECT relation_oids FROM "{fixture.schema}".entity_address_result_generation WHERE singleton'
    )
    descriptor = json.loads(artifact._json(fixture.descriptor))
    descriptor["authority"]["relation_oids"] = wire_value
    descriptor["generation_id"] = artifact._digest(descriptor["authority"])
    descriptor["descriptor_sha256"] = artifact._digest(
        {key: value for key, value in descriptor.items() if key != "descriptor_sha256"}
    )
    return artifact._descriptor(descriptor)


@pytest.mark.parametrize("copied_wire", [False, True])
async def test_capture_accepts_genuine_old_generation_and_exact_command_digest(address_db, copied_wire):
    if copied_wire:
        await address_db.admin.execute(
            f'UPDATE "{address_db.schema}".entity_address_result_generation SET relation_oids=$1::bigint[]',
            list(range(1, 8)),
        )
    legacy = await legacy_descriptor(address_db)
    legacy_pin = PinnedAddressSource(address_db.schema, "entity_address_unified", legacy["generation_id"])
    async with address_db.reader.transaction(isolation="repeatable_read", readonly=True):
        assert (
            await artifact.capture_network_address_artifact(
                address_db.reader, legacy_pin, runtime_roles=(address_db.roles["reader"],)
            )
            == legacy
        )
        assert (
            await artifact.capture_network_address_artifact(
                address_db.reader,
                address_db.source,
                runtime_roles=(address_db.roles["reader"],),
                expected_artifact_sha256=legacy["descriptor_sha256"],
            )
            == legacy
        )
    assert address_db.descriptor["authority"]["relation_oids"] is None


@pytest.mark.parametrize("damage", ["generation", "layout", "digest"])
async def test_queued_old_command_upgrade_keeps_record_and_refuses_drift(refresh_queue_db, damage):
    fixture = refresh_queue_db
    legacy = await legacy_descriptor(fixture.source)
    command_by_field = {**fixture.command, "address_artifact_sha256": legacy["descriptor_sha256"]}
    stored_command_by_field = {
        key: field_value for key, field_value in command_by_field.items() if key != "idempotency_key"
    }
    metadata = _request_metadata(
        stored_command_by_field, envelope()["actor"], None, command_by_field["idempotency_key"]
    )
    await fixture.admin.execute(
        f'INSERT INTO "{fixture.schema}".registry_publication_request '
        "(request_id,actor_key,session_token_sha256,actor_json,command_json,idempotency_key,request_sha256) "
        "VALUES($1,$2,NULL,$3::jsonb,$4::jsonb,$5,$6)",
        uuid4(),
        metadata["actor_key"],
        metadata["actor_json"],
        metadata["command_json"],
        command_by_field["idempotency_key"],
        metadata["request_sha256"],
    )
    request_record_by_field = dict(
        await fixture.admin.fetchrow(f'SELECT * FROM "{fixture.schema}".registry_publication_request')
    )
    saved_command_by_field = validated_membership_refresh_request_record(request_record_by_field)["command"]
    async with fixture.connection.transaction(isolation="repeatable_read"):
        current = await _current_heads(fixture.connection, saved_command_by_field, '"' + fixture.schema + '"')
        await _require_address(fixture.connection, saved_command_by_field, current, fixture.profile, fixture.schema)
    assert (
        dict(await fixture.admin.fetchrow(f'SELECT * FROM "{fixture.schema}".registry_publication_request'))
        == request_record_by_field
    )

    if damage == "generation":
        await fixture.admin.execute(
            f'UPDATE "{fixture.source.schema}".entity_address_result_generation SET local_generation=local_generation+1'
        )
    elif damage == "layout":
        await fixture.admin.execute(
            f'ALTER TABLE "{fixture.source.schema}".entity_address_unified ADD COLUMN changed int'
        )
    else:
        saved_command_by_field = {**saved_command_by_field, "address_artifact_sha256": "f" * 64}
    async with fixture.connection.transaction(isolation="repeatable_read"):
        with pytest.raises(RegistryApprovalConflict):
            await _require_address(fixture.connection, saved_command_by_field, current, fixture.profile, fixture.schema)
    assert (
        dict(await fixture.admin.fetchrow(f'SELECT * FROM "{fixture.schema}".registry_publication_request'))
        == request_record_by_field
    )


async def test_owned_reader_returns_the_historical_generation_for_an_old_command(refresh_queue_db):
    fixture = refresh_queue_db
    legacy = await legacy_descriptor(fixture.source)
    engine = create_async_engine(
        fixture.source.reader_url.set(drivername="postgresql+asyncpg"), pool_size=1, max_overflow=0
    )
    try:
        resources = runtime._Resources(
            settings=SimpleNamespace(control_schema=fixture.schema, address_profile=fixture.profile),
            fence_sessions=None,
            source_sessions=None,
            address_sessions=async_sessionmaker(engine),
            identity=await runtime._native_identity(fixture.connection, fixture.schema),
            readers=None,
        )
        async with resources.address(expected_artifact_sha256=legacy["descriptor_sha256"]) as (
            _driver,
            pin,
            descriptor,
        ):
            assert descriptor == legacy and pin.generation_id == legacy["generation_id"]
        assert engine.pool.checkedout() == 0
    finally:
        await engine.dispose()


async def test_copied_hash_only_command_without_original_descriptor_is_not_guessed(refresh_queue_db):
    fixture = refresh_queue_db
    original = await legacy_descriptor(fixture.source)
    original["database"] = {"oid": 1, "name": "previous_database"}
    copied_hash = artifact._digest({key: value for key, value in original.items() if key != "descriptor_sha256"})
    saved_command_by_field = {**fixture.command, "address_artifact_sha256": copied_hash}
    async with fixture.connection.transaction(isolation="repeatable_read"):
        current = await _current_heads(fixture.connection, saved_command_by_field, '"' + fixture.schema + '"')
        with pytest.raises(RegistryApprovalConflict):
            await _require_address(fixture.connection, saved_command_by_field, current, fixture.profile, fixture.schema)
