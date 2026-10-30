# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Manual offices publish compactly while old readers retain their exact generation."""

from dataclasses import replace
from types import SimpleNamespace
from uuid import uuid4

import pytest

from process.network_membership_pipeline import prepare_and_publish_network_candidate
from process.registry_candidate_composition import (
    RegistryCompositionAddressSources,
    compose_registry_membership_candidate,
)
from process.registry_publication_execution import _source_rollback_address_sources
from tests.test_network_compact_publication_postgres import _assert_readonly_replay, _read, _seal
from tests.test_network_compact_publication_postgres import compact_publication_db as compact_publication_db
from tests.test_network_custom_address_source_postgres import _draft, _seed
from tests.test_registry_approval_store_postgres import _approve, _command

pytestmark = pytest.mark.asyncio


async def _approved_candidate(fixture, revision, expected_head, source_manifest=None):
    sources = (
        await _source_rollback_address_sources(fixture.execution_connection, source_manifest, fixture.control_schema)
        if source_manifest is not None
        else RegistryCompositionAddressSources(fixture.source, compact=True)
    )
    copy_target, source = await compose_registry_membership_candidate(
        fixture.execution_connection,
        request_id=uuid4(),
        approved_revision=revision,
        expected_head=expected_head,
        source_manifest=source_manifest,
        address_sources=sources,
        writer_roles=fixture.writer_roles,
        control_schema=fixture.control_schema,
    )
    assert source == fixture.source
    members = await fixture.connection.fetch(f'SELECT * FROM "{copy_target.schema_name}".network_membership')
    return copy_target, [dict(member) for member in members]


async def _publish_registered(fixture, copy_target):
    async def seal(target):
        return await _seal(fixture, target)

    return await prepare_and_publish_network_candidate(
        fixture.execution_connection,
        copy_target,
        fixture.source,
        **fixture.writer_roles,
        control_schema=fixture.control_schema,
        seal_candidate=seal,
    )


async def test_manual_office_publication_removal_and_pinned_old_reader(compact_publication_db):
    fixture = compact_publication_db
    await fixture.connection.execute(f'CREATE TABLE "{fixture.address.schema}".npi(npi bigint PRIMARY KEY)')
    approval = SimpleNamespace(
        connection=fixture.connection,
        engine=fixture.engine,
        control_schema=fixture.control_schema,
        source_schema=fixture.address.schema,
    )
    seed = await _seed(approval)
    before = await fixture.connection.fetch(f'SELECT * FROM "{fixture.address.schema}".entity_address_unified')
    first, members = await _approved_candidate(fixture, seed.revision, 0)
    network = members[0]["network_id"]
    publication = await _publish_registered(fixture, first)
    assert await fixture.connection.fetchval(f'SELECT count(*) FROM "{first.schema_name}".entity_address_unified') == 1
    pin, office_rows = await _read(fixture, network_ids=(network,))
    assert len(office_rows) == 1 and office_rows[0]["entity_type"] == "manual"
    assert office_rows[0]["npi"] is None and office_rows[0]["canonical_network_ids"] == [network]
    assert office_rows[0]["second_line"] == "Suite 2"
    assert pin.compact_layout.custom.table_oid != pin.compact_layout.base.table_oid
    await _assert_readonly_replay(fixture, first, publication)
    corrected = await _draft(
        approval,
        replace(
            seed.membership_command,
            operation="correct",
            expected_revision=1,
            fields={"memberships_json": []},
            idempotency_key=uuid4().hex,
        ),
        seed.actor,
    )
    approved = await _approve(
        fixture.connection,
        fixture.control_schema,
        await _command(fixture.connection, fixture.control_schema, corrected),
        seed.actor,
    )
    second, members = await _approved_candidate(fixture, approved["approved_revision"], pin.generation_id, pin)
    assert members == []
    await _publish_registered(fixture, second)
    new_pin, new_rows = await _read(fixture, network_ids=(network,))
    assert new_rows == [] and new_pin.compact_layout.custom is None
    assert (await _read(fixture, generation_id=pin.generation_id, network_ids=(network,)))[1] == office_rows
    assert (
        await fixture.connection.fetchval("SELECT to_regclass($1)", second.schema_name + ".entity_address_unified")
        is None
    )
    assert await fixture.connection.fetch(f'SELECT * FROM "{fixture.address.schema}".entity_address_unified') == before
