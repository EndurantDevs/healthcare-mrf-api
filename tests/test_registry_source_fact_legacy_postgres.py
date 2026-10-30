# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""An already registered source-v1 candidate finishes without rewriting its contract."""

import json
from uuid import UUID, uuid4

import pytest

from process import registry_source_recipe_composition as composition
from process.network_address_projection import PinnedAddressSource
from process.network_approved_membership_source import pin_approved_membership_source
from process.network_membership_candidate_lifecycle import (
    admit_network_membership_batch,
    create_network_candidate,
    seal_network_candidate,
)
from process.network_membership_copy import MembershipCopyTarget
from process.network_membership_pipeline import prepare_and_publish_network_candidate, prepare_network_candidate
from process.network_serving_read import resolve_network_serving_manifest
from process.registry_membership_composition_stage import (
    build_registry_source_retention,
    drop_registry_command_stage,
    materialize_registry_composition,
    stage_approved_membership_commands,
)
from process.registry_source_contribution_store import (
    SUMMARY_KEY,
    contribution_fact_sha256,
    registry_source_contribution_receipt,
    registry_source_contribution_sha256,
    retain_registry_source_contributions,
    verify_registry_source_contributions,
    verify_registry_source_contributions_rows,
)
from process.registry_source_recipe_composition import SourceRecipeReadContext
from process.registry_source_recipe_store import RegistrySourceMembershipRecipe
from process.registry_source_retention import retain_registry_source_memberships
from process.registry_source_selection_receipt import record_registry_source_selection
from tests.test_network_provider_only_read_postgres import _read
from tests.test_network_provider_only_read_postgres import cms_recipe_database as cms_recipe_database
from tests.test_network_provider_only_read_postgres import cms_recipe_template as cms_recipe_template
from tests.test_network_provider_only_read_postgres import custom_db as custom_db
from tests.test_network_provider_only_read_postgres import provider_only_db as provider_only_db
from tests.test_network_provider_only_read_postgres import provider_only_source as provider_only_source
from tests.test_network_provider_only_read_postgres import serving_schema as serving_schema

pytestmark = pytest.mark.asyncio


async def _legacy_stage(context):
    fixture = context.fixture
    approved = await pin_approved_membership_source(
        fixture.connection,
        approved_revision=context.manifest.approved_custom_revision,
        control_schema=fixture.control_schema,
    )
    recipe = RegistrySourceMembershipRecipe(context.source[0][2], context.source[4])
    stage = await composition.stage_registry_source_recipes(
        fixture.connection,
        (recipe,),
        approved,
        request_id=uuid4(),
        control_schema=fixture.control_schema,
        contribution_revision=1,
        read_context=SourceRecipeReadContext(source_manifest=context.manifest),
    )
    assert stage.contribution_revision == 1 and stage.copied_reused_fact_rows == stage.own_fact_rows > 0
    return recipe, approved, stage


async def _register_pending(context, copy_target, address):
    fixture, connection = context.fixture, context.fixture.connection
    async with connection.transaction(isolation="repeatable_read"):
        recipe, approved, stage = await _legacy_stage(context)
        commands = await stage_approved_membership_commands(
            connection, approved, uuid4(), control_schema=fixture.control_schema
        )
        retention = await build_registry_source_retention(connection, stage, commands)
        try:
            await create_network_candidate(
                connection,
                copy_target,
                source_generations={
                    "unified_address": address.generation_id,
                    "custom_membership": approved.generation_id,
                    "registry_source_membership": stage.generation_sha256,
                },
                source_recipes=(recipe,),
                source_retention=retention,
                source_contributions=registry_source_contribution_receipt(stage),
                approved_custom_revision=approved.approved_revision,
                expected_head=context.manifest.generation_id,
                expected_rows=stage.membership_rows,
                control_schema=fixture.control_schema,
            )
            await connection.execute(
                f'CREATE TABLE "{copy_target.schema_name}".provider_location_binding '
                f'(LIKE "{context.manifest.schema_name}".provider_location_binding INCLUDING ALL)'
            )
            await connection.execute(
                f'INSERT INTO "{copy_target.schema_name}".provider_location_binding '
                f'SELECT * FROM "{context.manifest.schema_name}".provider_location_binding'
            )
            await retain_registry_source_contributions(
                connection, copy_target, stage, control_schema=fixture.control_schema
            )
            await record_registry_source_selection(
                connection, copy_target, (recipe,), approved, stage, control_schema=fixture.control_schema
            )
            await retain_registry_source_memberships(
                connection, copy_target, stage, control_schema=fixture.control_schema
            )
            await materialize_registry_composition(
                connection, copy_target, commands, control_schema=fixture.control_schema
            )
            await connection.execute(f'DROP INDEX "{copy_target.schema_name}".source_provider_only_idx')
            await seal_network_candidate(
                connection,
                copy_target,
                source_stage=stage,
                command_stage=commands,
                control_schema=fixture.control_schema,
            )
        finally:
            await drop_registry_command_stage(connection, commands)
            await connection.execute(f'DROP TABLE pg_temp."{stage.table_name}"')
            await connection.execute(f'DROP TABLE pg_temp."{stage.contribution_table_name}"')


async def _candidate(context, copy_target):
    return dict(
        await context.fixture.connection.fetchrow(
            f'SELECT * FROM "{context.fixture.control_schema}".network_membership_candidate WHERE candidate_id=$1',
            UUID(copy_target.candidate_id),
        )
    )


async def _forge_receipt(context, copy_target, original):
    fixture, connection = context.fixture, context.fixture.connection
    receipt = verify_registry_source_contributions(original)
    assert receipt["revision"] == 1
    await connection.execute(f'''UPDATE "{copy_target.schema_name}".network_source_contribution SET fact_json=jsonb_set(
        fact_json,'{{provider_id}}','"1999999999"') WHERE fact_json->>'omission_reason' IS NULL''')
    receipt["sources"][0]["fact_sha256"], _ = await contribution_fact_sha256(
        connection, f'"{copy_target.schema_name}".network_source_contribution', receipt["sources"][0]["recipe_sha256"]
    )
    generations = json.loads(original["source_generations"])
    generations[SUMMARY_KEY] = registry_source_contribution_sha256(receipt)
    report = json.loads(original["validation_json"])
    report[SUMMARY_KEY] = receipt
    report["source_generations"] = generations
    await connection.execute(
        f'UPDATE "{fixture.control_schema}".network_membership_candidate '
        "SET source_generations=$2::jsonb,validation_json=$3::jsonb WHERE candidate_id=$1",
        UUID(copy_target.candidate_id),
        json.dumps(generations),
        json.dumps(report),
    )
    async with connection.transaction(isolation="repeatable_read"):
        await verify_registry_source_contributions_rows(
            connection, copy_target, await _candidate(context, copy_target), control_schema=fixture.control_schema
        )


async def _restore_pending(context, copy_target, original):
    connection = context.fixture.connection
    await connection.execute(
        f'UPDATE "{copy_target.schema_name}".network_source_contribution local SET fact_json=source.fact_json '
        f'FROM "{context.manifest.schema_name}".network_source_contribution source '
        "WHERE (local.recipe_sha256,local.ordinal)=(source.recipe_sha256,source.ordinal)"
    )
    await connection.execute(
        f'UPDATE "{context.fixture.control_schema}".network_membership_candidate '
        "SET source_generations=$2::jsonb,validation_json=$3::jsonb WHERE candidate_id=$1",
        UUID(copy_target.candidate_id),
        original["source_generations"],
        original["validation_json"],
    )


async def test_registered_v1_pending_candidate_rejects_forgery_and_finishes_exactly(provider_only_db, monkeypatch):
    context, fixture = provider_only_db, provider_only_db.fixture
    identity = uuid4()
    copy_target = MembershipCopyTarget(
        *(str(uuid4()) for _ in range(3)), str(identity), "network_candidate_" + identity.hex
    )
    context.copy_targets.append(copy_target)
    address = PinnedAddressSource(
        context.manifest.schema_name, "entity_address_unified", context.manifest.manifest_sha256
    )

    async def forbidden(*args, **kwargs):
        pytest.fail("legacy pending resume reread unchanged FHIR payloads")

    monkeypatch.setattr(composition, "_read_recipe_page", forbidden)
    await _register_pending(context, copy_target, address)
    await prepare_network_candidate(
        fixture.connection, copy_target, address, **fixture.writer_roles, control_schema=fixture.control_schema
    )
    original = await _candidate(context, copy_target)
    await _forge_receipt(context, copy_target, original)
    with pytest.raises(ValueError):
        await fixture.seal_candidate(copy_target)
    assert (
        await fixture.connection.fetchval(
            f'SELECT count(*) FROM "{fixture.control_schema}".network_candidate_custody WHERE candidate_id=$1', identity
        )
        == 0
    )
    await _restore_pending(context, copy_target, original)
    await fixture.seal_candidate(copy_target)
    await prepare_and_publish_network_candidate(
        fixture.connection,
        copy_target,
        address,
        **fixture.writer_roles,
        control_schema=fixture.control_schema,
        seal_candidate=fixture.seal_candidate,
    )
    async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
        current = await resolve_network_serving_manifest(fixture.connection, control_schema=fixture.control_schema)
    assert current.candidate_id == copy_target.candidate_id
    restored = await _candidate(context, copy_target)
    assert restored["source_generations"] == original["source_generations"]
    assert verify_registry_source_contributions(restored)["revision"] == 1
    assert (await _read(context, manifest=current))["total_count"] == 2
