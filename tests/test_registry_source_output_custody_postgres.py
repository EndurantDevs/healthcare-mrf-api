# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Source row accounting is checked before a candidate is published."""

from uuid import UUID, uuid4, uuid5

import pytest

from process.network_address_projection import PinnedAddressSource
from process.network_membership_pipeline import prepare_and_publish_network_candidate, prepare_network_candidate
from process.registry_candidate_composition import (
    RegistryCompositionAddressSources,
    compose_registry_membership_candidate,
)
from process.registry_source_retention import RegistrySourceRetentionError
from tests.test_network_provider_only_read_postgres import cms_recipe_database as cms_recipe_database
from tests.test_network_provider_only_read_postgres import cms_recipe_template as cms_recipe_template
from tests.test_network_provider_only_read_postgres import custom_db as custom_db
from tests.test_network_provider_only_read_postgres import provider_only_db as provider_only_db
from tests.test_network_provider_only_read_postgres import provider_only_source as provider_only_source
from tests.test_network_provider_only_read_postgres import serving_schema as serving_schema

pytestmark = pytest.mark.asyncio


async def _prepare_pending(context):
    fixture, source = context.fixture, context.manifest
    request_id = uuid4()
    context.request_ids.append(request_id)
    copy_target, address = await compose_registry_membership_candidate(
        fixture.connection,
        request_id=request_id,
        approved_revision=source.approved_custom_revision,
        expected_head=source.generation_id,
        source_manifest=source,
        address_sources=RegistryCompositionAddressSources(
            PinnedAddressSource(source.schema_name, "entity_address_unified", source.manifest_sha256)
        ),
        writer_roles=fixture.writer_roles,
        control_schema=fixture.control_schema,
    )
    context.copy_targets.append(copy_target)
    fixture.composition_ids.append(uuid5(request_id, "address:" + source.source_generations["custom_membership"]))
    await prepare_network_candidate(
        fixture.connection, copy_target, address, **fixture.writer_roles, control_schema=fixture.control_schema
    )
    return copy_target, address


async def _add_source_row(context, copy_target):
    connection = context.fixture.connection
    source_table = f'"{copy_target.schema_name}".network_membership_source'
    await connection.execute(f"INSERT INTO {source_table} SELECT * FROM {source_table}")
    assert await connection.fetchval(f"SELECT count(*) FROM {source_table}") == 2


async def _remove_added_source_row(context, copy_target):
    source_table = f'"{copy_target.schema_name}".network_membership_source'
    await context.fixture.connection.execute(
        f"DELETE FROM {source_table} WHERE ctid=(SELECT max(ctid) FROM {source_table})"
    )


async def test_seal_rejects_a_changed_source_row_count(provider_only_db):
    context, fixture = provider_only_db, provider_only_db.fixture
    copy_target, address = await _prepare_pending(context)
    await _add_source_row(context, copy_target)
    with pytest.raises(RegistrySourceRetentionError, match="accounting_invalid"):
        await fixture.seal_candidate(copy_target)
    assert (
        await fixture.connection.fetchval(
            f'SELECT state FROM "{fixture.control_schema}".network_membership_candidate WHERE candidate_id=$1',
            UUID(copy_target.candidate_id),
        )
        != "published"
    )
    assert (
        await fixture.connection.fetchval(
            f'SELECT generation_id FROM "{fixture.control_schema}".network_serving_control WHERE id=1'
        )
        == context.manifest.generation_id
    )
    await _remove_added_source_row(context, copy_target)
    await fixture.seal_candidate(copy_target)
    await prepare_and_publish_network_candidate(
        fixture.connection,
        copy_target,
        address,
        **fixture.writer_roles,
        control_schema=fixture.control_schema,
        seal_candidate=fixture.seal_candidate,
    )
    assert (
        await fixture.connection.fetchval(
            f'SELECT state FROM "{fixture.control_schema}".network_membership_candidate WHERE candidate_id=$1',
            UUID(copy_target.candidate_id),
        )
        == "published"
    )
    assert (
        await fixture.connection.fetchval(
            f'SELECT generation_id FROM "{fixture.control_schema}".network_serving_control WHERE id=1'
        )
        > context.manifest.generation_id
    )
