# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real inline retained-source publication followed by a typed replacement."""

from contextlib import aclosing, asynccontextmanager
from dataclasses import replace
from uuid import UUID, uuid4

import pytest

from tests import test_network_fhir_source_witness_publication_postgres as publication
from tests import test_network_fhir_typed_source_postgres as typed_fixture

pytestmark = pytest.mark.asyncio


@asynccontextmanager
async def _legacy_bootstrap(context, monkeypatch):
    from db.models import EntityAddressUnified
    from tests.test_network_custom_address_source_postgres import (
        _bootstrap_prepare,
        _bootstrap_specification,
        bootstrap_db,
    )

    async with aclosing(bootstrap_db.__wrapped__(context.serving, monkeypatch)) as generator:
        fixture = await anext(generator)
        columns_sql = ",".join('"' + name + '"' for name in EntityAddressUnified.__table__.columns.keys())
        await fixture.connection.execute(f'DELETE FROM "{fixture.source_schema}".entity_address_unified')
        await fixture.connection.execute(
            f'INSERT INTO "{fixture.source_schema}".entity_address_unified({columns_sql}) '
            f'SELECT {columns_sql} FROM "{context.address.schema_name}".entity_address_unified'
        )
        await fixture.connection.execute(f'INSERT INTO "{fixture.source_schema}".npi(npi) VALUES(1234567893)')
        receipt = await _bootstrap_prepare(
            fixture, await _bootstrap_specification(fixture), runtime_roles=context.address_reader_roles
        )
        monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "mrf")
        monkeypatch.setenv("DB_SCHEMA", "mrf")
        context.address = receipt.address_source
        yield receipt


async def _stage_legacy_candidate(context, writer, recipe, roles_by_group):
    from process.network_initial_cms_office_bindings import copy_verified_retained_offices
    from process.network_membership_candidate_lifecycle import create_network_candidate
    from process.registry_candidate_composition import _composition_retention, _copy_composition, _target
    from process.registry_source_recipe_composition import SourceRecipeReadContext, stage_registry_source_recipes

    schema, request = context.worker.fixture.control_schema, uuid4()
    stage = await stage_registry_source_recipes(
        writer,
        (recipe,),
        context.approved,
        request_id=request,
        control_schema=schema,
        contribution_revision=1,
        read_context=SourceRecipeReadContext(verified_office_inputs=(context.address, context.address_reader_roles)),
    )
    assert not stage.contribution_receipts and stage.office_matches is not None
    command_stage, retention = await _composition_retention(writer, context.approved, request, stage, schema)
    generations_by_source = {
        "legacy_source": stage.generation_sha256,
        "custom_membership": context.approved.generation_id,
        "unified_address": context.address.generation_id,
    }
    copy_target = _target(request, context.approved.approved_revision, 0, generations_by_source)
    candidate = await create_network_candidate(
        writer,
        copy_target,
        source_generations=generations_by_source,
        approved_custom_revision=context.approved.approved_revision,
        expected_head=0,
        expected_rows=retention["effective_rows"],
        control_schema=schema,
        source_recipes=(recipe,),
        source_retention=retention,
    )
    assert candidate["state"] == "open"
    await copy_verified_retained_offices(writer, stage.office_matches, copy_target)
    await _copy_composition(writer, copy_target, schema, stage, command_stage, roles_by_group)
    return copy_target


async def _require_legacy_manifest(context, recipe):
    from process.network_membership_publication import _candidate_custody
    from process.registry_source_contribution_store import verify_registry_source_contributions
    from process.registry_source_recipe_store import resolve_retained_registry_source_recipes

    schema = context.worker.fixture.control_schema
    async with (
        publication._login(context.worker, "api") as reader,
        reader.transaction(isolation="repeatable_read", readonly=True),
    ):
        manifest = await publication.resolve_network_serving_manifest(reader, control_schema=schema, api_read=True)
        candidate = await reader.fetchrow(
            f'SELECT * FROM "{schema}".network_membership_candidate WHERE candidate_id=$1', UUID(manifest.candidate_id)
        )
        assert await _candidate_custody(reader, candidate, schema) is None
        assert verify_registry_source_contributions(candidate) is None
        assert await resolve_retained_registry_source_recipes(
            reader, manifest, control_schema=schema, api_read=True
        ) == (recipe,)
        assert manifest.candidate_relation_pins and not hasattr(manifest, "_custody_seal_sha256")
        return manifest


async def _publish_inline_legacy(context, recipe):
    """Use the supported legacy low-level writer; never delete a modern seal."""
    from process.network_membership_pipeline import prepare_and_publish_network_candidate

    async with publication._consumer(context.worker) as (arguments, _):
        assert await publication.consume_registry_publication_once(**arguments) is None
        roles_by_group = arguments["config"].writer_roles
    async with publication._login(context.worker, "execution") as writer:
        async with writer.transaction(isolation="repeatable_read"):
            copy_target = await _stage_legacy_candidate(context, writer, recipe, roles_by_group)
        await prepare_and_publish_network_candidate(
            writer, copy_target, context.address, **roles_by_group, control_schema=context.worker.fixture.control_schema
        )
    return await _require_legacy_manifest(context, recipe)


def _thin_reference(prepared):
    from process.network_fhir_typed_recipe import TypedFHIRRecipeReference

    capture = prepared.capture
    return TypedFHIRRecipeReference(
        capture["capture_id"],
        capture["observed_at"],
        capture["capture_sha256"],
        prepared.recipe.source_pin.catalog_generation,
    )


async def _assert_replacement_refusals(context, first, source_inputs, previous_recipe, prepared):
    from process.registry_composition_addresses import _resolve_source_recipe_additions
    from process.registry_fhir_serving_preservation import fhir_vector_succession

    assert fhir_vector_succession((previous_recipe,), (prepared.recipe,))
    with pytest.raises(ValueError):
        fhir_vector_succession((previous_recipe,), ())
    async with (
        publication._login(context.worker, "execution") as reader,
        reader.transaction(isolation="repeatable_read", readonly=True),
    ):
        fake_reference = replace(source_inputs.replacement_source_recipes[0], capture_id=str(uuid4()))
        with pytest.raises(ValueError):
            await _resolve_source_recipe_additions(
                reader,
                replace(source_inputs, replacement_source_recipes=(fake_reference,)),
                first,
                first.generation_id,
                context.worker.fixture.control_schema,
            )


async def _office_memberships(connection, manifest):
    members = await connection.fetch(
        f'SELECT network_id,provider_system,provider_id,location_id FROM "{manifest.schema_name}".network_membership ORDER BY 1,2,3,4'
    )
    return tuple(map(tuple, members))


async def _assert_publication_readers(context, first, source_inputs):
    schema = context.worker.fixture.control_schema
    async with (
        publication._login(context.worker, "api") as reader,
        reader.transaction(isolation="repeatable_read", readonly=True),
    ):
        original_rows = await _office_memberships(reader, first)
        assert len(original_rows) == 1
        second = await publication._publish_once(context, head=first.generation_id, sources=source_inputs)
        assert await publication.resolve_network_serving_manifest(reader, control_schema=schema, api_read=True) == first
        assert await _office_memberships(reader, first) == original_rows
    async with (
        publication._login(context.worker, "api") as reader,
        reader.transaction(isolation="repeatable_read", readonly=True),
    ):
        current = await publication.resolve_network_serving_manifest(reader, control_schema=schema, api_read=True)
        assert current == second and hasattr(current, "_custody_seal_sha256")
        assert await _office_memberships(reader, second) == original_rows
    return second


async def _assert_modern_typed_seal_required(context, manifest, recipe):
    from process.registry_typed_retained_source import retained_typed_source_proofs

    admin, schema = context.worker.fixture.connection, context.worker.fixture.control_schema
    transaction = admin.transaction(isolation="repeatable_read")
    await transaction.start()
    try:
        assert (
            await admin.execute(
                f'DELETE FROM "{schema}".network_candidate_custody WHERE candidate_id=$1', UUID(manifest.candidate_id)
            )
            == "DELETE 1"
        )
        with pytest.raises(ValueError):
            await retained_typed_source_proofs(admin, manifest, (recipe,), schema)
    finally:
        await transaction.rollback()
    async with (
        publication._login(context.worker, "api") as reader,
        reader.transaction(isolation="repeatable_read", readonly=True),
    ):
        assert (
            await publication.resolve_network_serving_manifest(reader, control_schema=schema, api_read=True) == manifest
        )


async def test_unsealed_inline_retained_source_to_typed_reference_preserves_readers(monkeypatch):
    """Actual legacy native closure remains readable while its all-typed successor publishes."""
    async with (
        publication._publication_case(monkeypatch) as context,
        _legacy_bootstrap(context, monkeypatch) as bootstrap,
    ):
        context.replay_actors = []
        retained_capture = await publication.capture_retained_source_for_roles(
            context.case, context.runtime.runtime_roles
        )
        retained, context.approved, network = await publication._approved_capture_recipe(
            context.case.fixture, context.serving, retained_capture
        )
        first = await _publish_inline_legacy(context, retained)
        prepared = await typed_fixture._make_typed_publication_recipe(
            context.case, context.serving, context.runtime.runtime_roles, target_network_id=network
        )
        context.approved = prepared.approved
        source_inputs = replace(
            publication._initial_sources(context),
            npi_source=bootstrap.npi_source,
            initial_source_recipes=(),
            replacement_source_recipes=(_thin_reference(prepared),),
        )
        await _assert_replacement_refusals(context, first, source_inputs, retained, prepared)
        second = await _assert_publication_readers(context, first, source_inputs)
        await _assert_modern_typed_seal_required(context, second, prepared.recipe)
