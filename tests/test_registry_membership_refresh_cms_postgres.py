# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Accepted CMS edition refresh preserves genuine original enrollment and physical evidence."""

import asyncio
from contextlib import AsyncExitStack
from dataclasses import replace
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from sqlalchemy import text

from process import provider_directory_generic_source_policy as source_policy
from process import registry_membership_refresh_sources as sources
from process.network_cms_registry_address_capture import native_driver
from process.network_fhir_membership_source import read_fhir_membership_fact_batch
from process.network_registry_cms_source_attempt import RegistryCMSSourceAttempt
from process.registry_company_approval_fence import registry_company_approval_transaction
from tests import cms_registry_native_producer_postgres_support as producer
from tests.test_registry_cms_refresh_anchors_postgres import (
    _accepted_source,
)
from tests.test_registry_cms_refresh_anchors_postgres import (
    cms_recipe_template as cms_recipe_template,
)
from tests.test_registry_membership_refresh_sources_postgres import _factory

pytestmark = [pytest.mark.asyncio, pytest.mark.usefixtures(cms_recipe_template.__name__)]


async def _publish_next(state, reader, monkeypatch, tmp_path):
    with monkeypatch.context() as source_settings:
        source_settings.setenv("HLTHPRT_DB_SCHEMA", state.pair.recipe.source_pin.schema_name)
        source_settings.setenv("DB_SCHEMA", state.pair.recipe.source_pin.schema_name)
        return await _publish_native_next(state, reader, source_settings, tmp_path)


async def _publish_native_next(state, reader, monkeypatch, tmp_path):
    while True:
        async with state.database.session_factory() as session, session.begin():
            await session.execute(text("SET TRANSACTION READ ONLY"))
            remaining = await session.scalar(
                text(
                    "SELECT extract(epoch FROM max(expires_at)-clock_timestamp()) "
                    "FROM mrf.provider_directory_profile_capacity_lease_consumption"
                )
            )
        if remaining is None or remaining <= 0:
            break
        assert remaining <= 960, "unexpected capacity reservation in the owned fixture"
        # Preserve immutable consumption history and wait before issuing the next permit.
        await asyncio.sleep(min(float(remaining) + 0.1, 30))
    original = state.pair
    acquired = await producer.acquire_native_source(
        state.database, monkeypatch, tmp_path / "second", revision="second", run_id="refresh-second"
    )
    run_id = "run_" + uuid4().hex
    attempt = RegistryCMSSourceAttempt(run_id, run_id + ":" + uuid4().hex, datetime.now(UTC).isoformat())
    state.store = replace(state.store, source_attempt=attempt, reader_roles=reader.reader_roles)
    async with source_policy.bind_generic_source_transaction_factory(state.database._source_component.factory):
        execution = await producer.native.attested_execution(state.database, acquired, monkeypatch, generation=2)
        job = await producer.issued_job(
            state.database, acquired, execution, state.roles, state.retention_sessions, state.store
        )
        state.captures.append(job.capture_id)

        async def notify(session, *, receipt_id, pair):
            notice = await state.store.notify_receipt(session, receipt_id=receipt_id, pair=pair)
            state.notices.append(notice)
            state.pair = pair
            return notice

        await producer.publish_native_execution(
            state.database, execution, monkeypatch, job=replace(job, notify_receipt=notify), batch_size=32
        )
    assert state.pair.recipe.source_pin.dataset_id != original.recipe.source_pin.dataset_id
    assert state.pair.proof.binding_coordinates.producer_id == original.proof.binding_coordinates.producer_id
    return original


async def _grant_registry_reader(state, sessions):
    role = sessions.kw["bind"].url.username
    async with state.database.engine.begin() as setup:
        namespace = state.pair.recipe.source_pin.schema_name
        await setup.execute(text(f'REVOKE SELECT ON ALL TABLES IN SCHEMA "{namespace}" FROM "{role}"'))
        for name in (
            "provider_directory_cms_serving_receipt",
            "provider_directory_profile_serving_generation",
            "entity_address_result_generation",
            "reference_family_result_generation",
            "address_alias_state_v1",
            "address_alias_artifact_state_v1",
        ):
            await setup.execute(text(f'GRANT SELECT ON "{namespace}"."{name}" TO "{role}"'))
        assert not await setup.scalar(
            text("SELECT has_table_privilege(:role,:relation,'SELECT')"),
            {"role": role, "relation": f'"{namespace}".provider_directory_dataset_proof_shard'},
        )
        await setup.execute(text(f'GRANT SELECT ON ALL TABLES IN SCHEMA "{state.anchor_control_schema}" TO "{role}"'))


async def _resolve(outer, state, readers, *, observe=False):
    resolver = (
        sources.observe_registry_membership_refresh_sources
        if observe
        else sources.resolve_registry_membership_refresh_sources
    )
    async with registry_company_approval_transaction(outer, control_schema=state.anchor_control_schema) as session:
        return await resolver(
            session,
            [state.anchor_enrollment],
            approved_revision=state.anchor_approved.approved_revision,
            readers=readers,
        )


async def _preflight_source_custody(state, monkeypatch):
    from process.network_fhir_source_epoch import require_retained_cms_fhir_source_epoch
    from process.provider_directory_source_proof_control import verify_proof_control

    component = state.database._source_component
    with monkeypatch.context() as source_settings:
        source_settings.setenv("HLTHPRT_DB_SCHEMA", component.policy.schema)
        source_settings.setenv("DB_SCHEMA", component.policy.schema)
        async with source_policy.bind_generic_source_transaction_factory(component.factory):
            async with source_policy.source_transaction(component.database, isolation="repeatable read") as session:
                driver = await native_driver(session)
                await verify_proof_control(driver, component.policy)
                await require_retained_cms_fhir_source_epoch(
                    driver,
                    state.pair.proof.source_pin,
                    state.pair.recipe.source_pin.retained_epoch,
                    verify_content=False,
                )


async def _assert_physical_fact_evidence(state, sessions, recipe):
    async with sessions() as session, session.begin():
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
        batch = await read_fhir_membership_fact_batch(
            await native_driver(session),
            recipe.source_pin,
            registry_schema=state.anchor_control_schema,
            approved_source=state.anchor_approved,
            binding_coordinates=recipe.binding_coordinates,
        )
        import json

        facts = json.loads(batch.source_fact_bytes)
        assert facts and all(fact["evidence_basis"][0] == state.pair.recipe.source_pin.dataset_id for fact in facts)
        # The fixture affiliation has no network reference; it must not become an office membership.
        assert (batch.membership_rows, batch.unresolved_rows) == (3, 1)
        assert [
            (outcome.resource_type, outcome.resource_id, outcome.reason)
            for outcome in batch.source_outcomes
            if outcome.outcome == "unresolved"
        ] == [("OrganizationAffiliation", "affiliation-1", "network_unresolved")]


async def _refresh_transport(state, monkeypatch):
    with monkeypatch.context() as source_settings:
        source_settings.setenv("HLTHPRT_DB_SCHEMA", state.pair.recipe.source_pin.schema_name)
        source_settings.setenv("DB_SCHEMA", state.pair.recipe.source_pin.schema_name)
        await producer.refresh_native_receipt_head(state, source_settings)


async def test_current_cms_edition_keeps_original_scope_and_pins_real_successor(monkeypatch, tmp_path):
    async with _accepted_source(monkeypatch, tmp_path) as (state, reader, child, _bindings), AsyncExitStack() as stack:
        await _grant_registry_reader(state, child)
        await _preflight_source_custody(state, monkeypatch)
        outer = await _factory(stack, child.kw["bind"].url)
        readers = sources.RegistryMembershipRefreshSourceReaders(state.anchor_control_schema, child, cms_reader=reader)
        initial = await _resolve(outer, state, readers)
        assert initial.recipes == (state.pair.recipe,)
        assert initial.accepted_sources[0]["capture_generation"] == state.pair.recipe.source_pin.generation_id
        original = await _publish_next(state, reader, monkeypatch, tmp_path)
        current = await _resolve(outer, state, readers)
        assert await _resolve(outer, state, readers, observe=True) == current
        assert current.accepted_sources != initial.accepted_sources
        recipe = current.recipes[0]
        assert recipe.binding_coordinates == original.recipe.binding_coordinates
        assert replace(recipe.source_pin, retained_successor=None) == original.recipe.source_pin
        assert recipe.source_pin.retained_successor == state.pair.recipe.source_pin
        assert current.accepted_sources[0]["capture_generation"] == state.pair.recipe.source_pin.generation_id
        await _assert_physical_fact_evidence(state, child, recipe)
        await _refresh_transport(state, monkeypatch)
        assert await _resolve(outer, state, readers) == current
        async with state.database.session_factory() as session:
            assert (
                await session.scalar(
                    text(f'SELECT count(*) FROM "{state.anchor_control_schema}".network_serving_manifest')
                )
                == 0
            )
            assert (
                await session.scalar(
                    text(f'SELECT count(*) FROM "{reader.control_schema}".registry_cms_source_notification')
                )
                == 2
            )
