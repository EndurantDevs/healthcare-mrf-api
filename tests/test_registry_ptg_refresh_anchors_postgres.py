# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact refresh scopes from real protected PTG captures and native source proof."""

import asyncio
from dataclasses import replace

import pytest
from sqlalchemy import text

from process import registry_ptg_refresh_anchors as anchors
from process.network_approved_membership_source import pin_approved_membership_source
from process.registry_company_approval_fence import registry_company_approval_transaction
from process.registry_ptg_graph_reader import RegistryPTGGraphReadBudget
from process.registry_ptg_producer_scope import RegistryPTGProducerScopeError
from process.registry_refresh_anchor_contract import (
    RegistryRefreshAnchorMismatch,
    RegistryRefreshAnchorUnavailable,
)
from process.registry_refresh_anchor_service import read_approved_refresh_bindings
from tests.test_registry_candidate_custody_ptg_postgres import (
    cms_recipe_database as cms_recipe_database,
)
from tests.test_registry_candidate_custody_ptg_postgres import (
    cms_recipe_template as cms_recipe_template,
)
from tests.test_registry_candidate_custody_ptg_postgres import (
    coordinator_db as coordinator_db,
)
from tests.test_registry_candidate_custody_ptg_postgres import (
    custom_db as custom_db,
)
from tests.test_registry_candidate_custody_ptg_postgres import (
    mixed_ptg_custody as mixed_ptg_custody,
)
from tests.test_registry_candidate_custody_ptg_postgres import (
    office_db as office_db,
)
from tests.test_registry_candidate_custody_ptg_postgres import (
    office_publisher as office_publisher,
)
from tests.test_registry_candidate_custody_ptg_postgres import (
    reviewed_cms_source as reviewed_cms_source,
)
from tests.test_registry_candidate_custody_ptg_postgres import (
    reviewed_ptg_offices as reviewed_ptg_offices,
)
from tests.test_registry_candidate_custody_ptg_postgres import (
    serving_schema as serving_schema,
)

pytestmark = pytest.mark.asyncio


async def _native_binding(session, case):
    namespace = '"' + case.fixture.control_schema + '"'
    native = (
        (
            await session.execute(
                text(f"""SELECT record_key,record_revision,(record_json->>'network_id')::integer AS network_id,
                    record_json->>'source_key' AS source_key
                  FROM {namespace}.registry_approved_record
                  WHERE approved_revision=:revision AND record_kind='network_binding'
                    AND record_json->>'source_system'='ptg'
                    AND record_json->'office_recipe_json'->>'capture_id'=:capture"""),
                {"revision": case.approved_revision, "capture": case.ptg_recipe.source_pin.capture_id},
            )
        )
        .mappings()
        .one()
    )
    driver = (await (await session.connection()).get_raw_connection()).driver_connection
    approved = await pin_approved_membership_source(
        driver, approved_revision=case.approved_revision, control_schema=case.fixture.control_schema
    )
    pin = case.ptg_recipe.source_pin
    enrollment_by_field = dict(
        binding_id=native["record_key"],
        binding_revision=native["record_revision"],
        network_id=native["network_id"],
        continuity_policy="exact_capture",
        source_scope=dict(
            source_system="ptg",
            source_id=pin.coordinates.source_id,
            dataset_schema=pin.coordinates.dataset_schema,
            producer_id=pin.coordinates.producer_id,
            source_key=native["source_key"],
            approved_binding_sha256="0" * 64,
            capture_generation=pin.generation_id,
        ),
    )
    bindings = await read_approved_refresh_bindings(
        driver,
        approved,
        [enrollment_by_field],
        control_schema=case.fixture.control_schema,
    )
    assert len(bindings) == 1
    assert bindings[0].approved_binding_sha256 != enrollment_by_field["source_scope"]["approved_binding_sha256"]
    return bindings[0], approved


async def test_native_ptg_anchor_refuses_partial_or_echoed_scope(mixed_ptg_custody, monkeypatch):
    """Derive native capture scope and reject partial, copied or unauthenticated inputs."""
    case = mixed_ptg_custody
    original = anchors.require_ptg_office_recipe
    calls = []

    async def observed(*args, **kwargs):
        calls.append(args[1].source_pin.capture_id)
        return await original(*args, **kwargs)

    monkeypatch.setattr(anchors, "require_ptg_office_recipe", observed)
    async with registry_company_approval_transaction(
        case.producer_sessions, control_schema=case.fixture.control_schema
    ) as session:
        binding, approved = await _native_binding(session, case)
        options_by_name = dict(
            store=case.config.ptg.store,
            read_budget=RegistryPTGGraphReadBudget(16 * 1024 * 1024),
            approved_source=approved,
        )
        actual = await anchors.resolve_ptg_refresh_anchors(session, (binding, binding), **options_by_name)
        pin = case.ptg_recipe.source_pin
        assert calls == [pin.capture_id]
        assert actual == {
            binding.binding_id: {
                "source_system": "ptg",
                "source_id": pin.coordinates.source_id,
                "dataset_schema": pin.coordinates.dataset_schema,
                "producer_id": pin.coordinates.producer_id,
                "source_key": binding.record_json["source_key"],
                "approved_binding_sha256": binding.approved_binding_sha256,
                "capture_generation": pin.generation_id,
            }
        }
        assert (
            actual[binding.binding_id]["capture_generation"]
            != binding.record_json["office_recipe_json"]["recipe_sha256"]
        )
        await _assert_native_ptg_refusals(session, binding, pin, options_by_name, calls, observed, monkeypatch)
    async with case.producer_sessions() as session, session.begin():
        with pytest.raises(RegistryRefreshAnchorUnavailable):
            await anchors.resolve_ptg_refresh_anchors(session, (binding,), **options_by_name)
    with pytest.raises(RegistryRefreshAnchorUnavailable):
        await anchors.resolve_ptg_refresh_anchors(object(), (binding,), **options_by_name)


async def _assert_native_ptg_refusals(session, binding, pin, options_by_name, calls, observed, monkeypatch):
    """Preserve native scope, missing capability, task and bounded-budget refusals."""
    for field in ("source_key", "dataset_id", "edition_id"):
        changed = replace(binding, record_json=binding.record_json | {field: "different-native-scope"})
        with pytest.raises(RegistryRefreshAnchorMismatch):
            await anchors.resolve_ptg_refresh_anchors(session, (binding, changed), **options_by_name)
    changed = replace(
        binding,
        record_json=binding.record_json
        | {"office_recipe_json": binding.record_json["office_recipe_json"] | {"recipe_sha256": pin.generation_id}},
    )
    with pytest.raises(RegistryRefreshAnchorMismatch):
        await anchors.resolve_ptg_refresh_anchors(session, (changed,), **options_by_name)
    assert calls == [pin.capture_id]
    for code, failure in (
        ("registry_ptg_scope_source_changed", RegistryRefreshAnchorMismatch),
        ("registry_ptg_scope_company_unapproved", RegistryRefreshAnchorMismatch),
        ("registry_ptg_scope_store_unprotected", RegistryRefreshAnchorUnavailable),
    ):

        async def native_refusal(*args, **kwargs):
            raise RegistryPTGProducerScopeError(code)

        monkeypatch.setattr(anchors, "require_ptg_office_recipe", native_refusal)
        with pytest.raises(failure):
            await anchors.resolve_ptg_refresh_anchors(session, (binding,), **options_by_name)
    monkeypatch.setattr(anchors, "require_ptg_office_recipe", observed)
    for field in ("store", "read_budget", "approved_source"):
        with pytest.raises(RegistryRefreshAnchorUnavailable):
            await anchors.resolve_ptg_refresh_anchors(session, (binding,), **(options_by_name | {field: None}))
    with pytest.raises(RegistryRefreshAnchorUnavailable):
        await asyncio.create_task(anchors.resolve_ptg_refresh_anchors(session, (binding,), **options_by_name))
    with pytest.raises(RegistryRefreshAnchorUnavailable):
        await anchors.resolve_ptg_refresh_anchors(
            session, (binding,), **(options_by_name | {"read_budget": RegistryPTGGraphReadBudget(1)})
        )
