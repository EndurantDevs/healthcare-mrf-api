# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Independent output proof can reuse authenticated legacy source memberships."""

from uuid import uuid4

import pytest

from process.network_approved_membership_source import pin_approved_membership_source
from process.network_membership_candidate_lifecycle import create_network_candidate
from process.registry_candidate_composition import (
    _complete_composition,
    _composition_retention,
    _stage_source,
    _target,
)
from process.registry_source_retention import (
    SUMMARY_KEY,
    RegistrySourceRetentionError,
    registry_source_retention_sha256,
)
from tests.test_network_compact_publication_postgres import _bind_offices, _candidate, _prepare, _publish, _read, _seal
from tests.test_network_compact_publication_postgres import compact_publication_db as compact_publication_db

pytestmark = pytest.mark.asyncio


async def _retained_candidate(fixture, previous):
    connection, request_id = fixture.execution_connection, uuid4()
    async with connection.transaction(isolation="repeatable_read"):
        approved = await pin_approved_membership_source(
            connection,
            approved_revision=0,
            control_schema=fixture.control_schema,
        )
        source_stage = await _stage_source(connection, previous, request_id, 2, fixture.control_schema)
        commands, retention = await _composition_retention(
            connection,
            approved,
            request_id,
            source_stage,
            fixture.control_schema,
        )
        generations_by_source = {
            "unified_address": fixture.source.generation_id,
            "custom_membership": approved.generation_id,
            "retained_network_source": previous.manifest_sha256,
            SUMMARY_KEY: registry_source_retention_sha256(retention),
        }
        copy_target = _target(request_id, 0, previous.generation_id, generations_by_source)
        await create_network_candidate(
            connection,
            copy_target,
            source_generations=generations_by_source,
            approved_custom_revision=0,
            expected_head=previous.generation_id,
            expected_rows=retention["effective_rows"],
            source_retention=retention,
            control_schema=fixture.control_schema,
        )
        locations = await connection.fetch(
            f'SELECT location_key,entity_type,entity_id,address_key FROM "{fixture.address.schema}".entity_address_unified'
        )
        await _bind_offices(connection, copy_target, locations)
    prepared = copy_target, fixture.source, "open", source_stage, commands, fixture.writer_roles
    return (await _complete_composition(connection, prepared, previous, fixture.control_schema))[0]


async def test_legacy_source_rows_are_independently_compared_before_new_seal(compact_publication_db):
    fixture = compact_publication_db
    first = await _candidate(fixture, (42, 88), 0)
    await _prepare(fixture, first)
    await _seal(fixture, first)
    await _publish(fixture, first)
    previous, expected_rows = await _read(fixture)
    second = await _retained_candidate(fixture, previous)
    await _prepare(fixture, second)
    original = await fixture.connection.fetchval(
        f'SELECT evidence_id FROM "{second.schema_name}".network_membership_source ORDER BY evidence_id LIMIT 1'
    )
    for table in ("network_membership_source", "network_membership"):
        await fixture.connection.execute(
            f'UPDATE "{second.schema_name}".{table} SET evidence_id=$1 WHERE evidence_id=$2',
            "b" * 64,
            original,
        )
    with pytest.raises(RegistrySourceRetentionError, match="parity_invalid"):
        await _seal(fixture, second)
    for table in ("network_membership_source", "network_membership"):
        await fixture.connection.execute(
            f'UPDATE "{second.schema_name}".{table} SET evidence_id=$1 WHERE evidence_id=$2',
            original,
            "b" * 64,
        )
    await _seal(fixture, second)
    await _publish(fixture, second)
    assert (await _read(fixture))[1] == expected_rows
