# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Independent mapped-source and manual-effective parity beyond fact hashes/counts."""

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


async def _corrupt_outputs(context, copy_target, mutation):
    connection = context.fixture.connection
    source_table = f'"{copy_target.schema_name}".network_membership_source'
    effective_table = f'"{copy_target.schema_name}".network_membership'
    original = await connection.fetchrow(f"SELECT network_id,evidence_id FROM {source_table}")
    if mutation == "source_network":
        await connection.execute(f"UPDATE {source_table} SET network_id=$1", context.other_network_id)
    if mutation in {"source_evidence", "both_evidence"}:
        await connection.execute(f"UPDATE {source_table} SET evidence_id=$1", "f" * 64)
    if mutation in {"effective_evidence", "both_evidence"}:
        await connection.execute(f"UPDATE {effective_table} SET evidence_id=$1", "f" * 64)
    assert await connection.fetchval(f"SELECT count(*) FROM {source_table}") == 1
    assert await connection.fetchval(f"SELECT count(*) FROM {effective_table}") == 1
    return original


async def _restore_outputs(context, copy_target, original):
    for table in ("network_membership_source", "network_membership"):
        await context.fixture.connection.execute(
            f'UPDATE "{copy_target.schema_name}".{table} SET network_id=$1,evidence_id=$2',
            original["network_id"],
            original["evidence_id"],
        )


@pytest.mark.parametrize("mutation", ["source_network", "source_evidence", "effective_evidence", "both_evidence"])
async def test_independent_seal_rejects_same_count_mapped_output_forgery(provider_only_db, mutation):
    context, fixture = provider_only_db, provider_only_db.fixture
    copy_target, address = await _prepare_pending(context)
    original = await _corrupt_outputs(context, copy_target, mutation)
    reason = "effective_parity_invalid" if mutation == "effective_evidence" else "retention_parity_invalid"
    with pytest.raises(RegistrySourceRetentionError, match=reason):
        await fixture.seal_candidate(copy_target)
    assert (
        await fixture.connection.fetchval(
            f'SELECT count(*) FROM "{fixture.control_schema}".network_candidate_custody WHERE candidate_id=$1',
            UUID(copy_target.candidate_id),
        )
        == 0
    )
    assert (
        await fixture.connection.fetchval(
            f'SELECT generation_id FROM "{fixture.control_schema}".network_serving_control WHERE id=1'
        )
        == context.manifest.generation_id
    )
    await _restore_outputs(context, copy_target, original)
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
            f'SELECT count(*) FROM "{fixture.control_schema}".network_candidate_custody WHERE candidate_id=$1',
            UUID(copy_target.candidate_id),
        )
        == 1
    )
