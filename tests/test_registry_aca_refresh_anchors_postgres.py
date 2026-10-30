# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Original ACA anchors from genuine native admission and accepted receipts."""

import os
from contextlib import aclosing, asynccontextmanager
from dataclasses import asdict, replace
from uuid import uuid4

import pytest
from sqlalchemy import text

from process import registry_aca_refresh_anchors as anchors
from process.network_approved_membership_source import pin_approved_membership_source
from process.network_cms_registry_address_capture import native_driver
from process.registry_ptg_graph_reader import RegistryPTGGraphReadBudget
from process.registry_refresh_anchor_contract import RegistryRefreshAnchorMismatch, RegistryRefreshAnchorUnavailable
from process.registry_refresh_anchor_service import read_approved_refresh_bindings
from tests import test_network_serving_schema_postgres as registry
from tests.registry_aca_refresh_anchor_postgres_support import accepted_aca_anchor
from tests.test_network_fhir_membership_source_postgres import _reviewed_binding
from tests.test_registry_approval_store_postgres import _actor, _approve, _command, _create, _draft

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.skipif(
        not os.getenv("NETWORK_REGISTRY_TEST_DSN"),
        reason="NETWORK_REGISTRY_TEST_DSN must explicitly select an isolated database",
    ),
]


async def _approved(connection, schema, engine, case):
    actor = _actor()
    network = await _draft(engine, schema, _create("network"), actor)
    binding_by_field = dict(
        binding_id=str(uuid4()),
        **asdict(case.recipe.binding_coordinates),
        source_key="12345VA0000001",
        source_scope_json=dict(
            issuer_id="12345", state="VA", plan_id="12345VA0000001", plan_year=2026, checksum_network=42
        ),
        network_id=network["record_id"],
        evidence_id="reviewed-original",
        evidence_sha256="e" * 64,
        operation="bind",
        expected_revision=0,
        expected_network_id=None,
    )
    binding = (await _reviewed_binding(connection, schema, actor, [binding_by_field]))["records"][0]
    approval = await _approve(connection, schema, await _command(connection, schema, network, binding), actor)
    async with connection.transaction(isolation="repeatable_read"):
        approved = await pin_approved_membership_source(
            connection, approved_revision=approval["approved_revision"], control_schema=schema
        )
    return binding_by_field, approved


@asynccontextmanager
async def _binding(case, monkeypatch):
    engine = case.admin.kw["bind"]
    monkeypatch.setenv(
        "NETWORK_REGISTRY_TEST_DSN", engine.url.set(drivername="postgresql").render_as_string(hide_password=False)
    )
    async with aclosing(registry.serving_schema.__wrapped__()) as generator:
        connection, schema, registry_engine = await anext(generator)
        binding_by_field, approved = await _approved(connection, schema, registry_engine, case)
        async with case.admin() as session, session.begin():
            enrollment_map = dict(
                binding_id=binding_by_field["binding_id"],
                binding_revision=1,
                network_id=binding_by_field["network_id"],
                continuity_policy="retained_successor",
                source_scope=dict(
                    source_system="aca",
                    source_id=binding_by_field["source_id"],
                    dataset_schema=binding_by_field["dataset_schema"],
                    producer_id=binding_by_field["producer_id"],
                    source_key=binding_by_field["source_key"],
                    approved_binding_sha256="0" * 64,
                    capture_generation="0" * 64,
                ),
            )
            bindings = await read_approved_refresh_bindings(
                await native_driver(session), approved, [enrollment_map], control_schema=schema
            )
            assert await session.scalar(text(f'SELECT count(*) FROM "{schema}".network_serving_manifest')) == 0
        yield bindings


async def _resolve(case, bindings, *, budget=None):
    async with case.sessions() as session, session.begin():
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
        return await anchors.resolve_aca_refresh_anchors(
            session,
            bindings,
            store=case.store,
            read_budget=budget or RegistryPTGGraphReadBudget(16 * 1024 * 1024),
        )


async def _assert_original_scope(case, bindings):
    binding = bindings[0]
    expected_scope_map = dict(
        source_system="aca",
        source_id=case.recipe.binding_coordinates.source_id,
        dataset_schema=case.recipe.binding_coordinates.dataset_schema,
        producer_id=case.recipe.binding_coordinates.producer_id,
        source_key=binding.record_json["source_key"],
        approved_binding_sha256=binding.approved_binding_sha256,
        capture_generation=case.recipe.source_pin.generation_id,
    )
    assert await _resolve(case, bindings) == {binding.binding_id: expected_scope_map}
    assert expected_scope_map["capture_generation"] != case.recipe.source_pin.prepared.manifest.source_metadata_sha256
    foreign = replace(binding, record_json={**binding.record_json, "edition_id": "other-edition"})
    with pytest.raises(RegistryRefreshAnchorMismatch):
        await _resolve(case, (foreign,))
    absent = replace(binding, record_json={**binding.record_json, "dataset_id": str(uuid4())})
    with pytest.raises(RegistryRefreshAnchorUnavailable):
        await _resolve(case, (absent,))
    with pytest.raises(RegistryRefreshAnchorUnavailable):
        await _resolve(case, bindings, budget=RegistryPTGGraphReadBudget(1))


async def test_aca_anchor_uses_original_accepted_capture_without_network_serving_head(monkeypatch):
    async with accepted_aca_anchor(monkeypatch) as case, _binding(case, monkeypatch) as bindings:
        await _assert_original_scope(case, bindings)
        async with case.sessions() as session, session.begin():
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
            accepted = await case.store.accepted(session, case.recipe.source_pin.prepared.ownership.dataset_id)
            assert accepted["recipe"] == case.recipe
            assert await session.scalar(text("SELECT current_user=session_user AND current_setting('role')='none'"))
