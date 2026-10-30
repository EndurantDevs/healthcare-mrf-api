# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""V2 policy and lineage orchestration; mocks confer no native authority."""

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process import network_initial_cms_office_bindings as cms
from process import network_source_contribution_custody as custody
from process import registry_compact_composition as compact
from process import registry_initial_source_composition as office_records
from process import registry_membership_refresh_runtime as runtime
from process import registry_source_office_custody as office_custody
from process import registry_source_recipe_composition as composition
from process import registry_verified_witness_offices as offices
from tests.test_verified_inventory_v2 import context


@pytest.mark.parametrize("aliases", [False, True])
async def test_global_policy_keeps_legacy_distinction_and_explicit_v2_alias_identity(monkeypatch, aliases):
    connection = SimpleNamespace(execute=AsyncMock())
    monkeypatch.setattr(cms, "_verified_office_match_keys", AsyncMock(return_value="winners"))
    await cms._finish_verified_office_matches(
        connection,
        "inputs",
        "matches",
        SimpleNamespace(schema_name="addresses", table_name="entity_address_unified"),
        {},
        source_aliases=aliases,
    )
    query = connection.execute.await_args.args[0]
    location_clause, source_clause = query.split("ambiguous_sources AS MATERIALIZED")
    expected = "jsonb_build_array(office->>'provider_system',office->>'provider_id'"
    assert (
        (expected + "))>1") in location_clause
        if aliases
        else (expected + ",office->>'location_id'))>1") in location_clause
    )
    assert "office->>'location_id' AS location_id" in source_clause
    assert "count(DISTINCT office->>'location_key')>1" in source_clause


@pytest.mark.parametrize("flag", [None, 1, "true"])
async def test_alias_policy_cannot_be_selected_by_coerced_flag(flag):
    connection = SimpleNamespace(execute=AsyncMock())
    with pytest.raises(cms.InitialCMSOfficeBindingError):
        await cms._finish_verified_office_matches(connection, "inputs", "matches", None, {}, source_aliases=flag)
    connection.execute.assert_not_awaited()


@pytest.mark.parametrize("conflicting", [False, True])
async def test_witness_copy_checks_provider_aliases(monkeypatch, conflicting):
    from process import network_initial_source_office_bindings as bindings

    documents = [
        dict(provider_system="npi", provider_id="1234567893", location_id=str(UUID(int=index)), location_key="a" * 64)
        for index in (1, 2)
    ]
    monkeypatch.setattr(offices, "_matched_page", AsyncMock(return_value=json.dumps(documents)))
    copier = AsyncMock()
    monkeypatch.setattr(bindings, "_copy_bindings", copier)
    connection = SimpleNamespace(fetchval=AsyncMock(return_value=conflicting))
    operation = offices.copy_witness_offices(
        connection, SimpleNamespace(page_proofs_by_key={"page": (2, "x")}), SimpleNamespace(schema_name="candidate")
    )
    if conflicting:
        with pytest.raises(cms.InitialCMSOfficeBindingError, match="ambiguous"):
            await operation
    else:
        await operation
    assert [row[2] for row in copier.await_args.args[2]] == [UUID(int=1), UUID(int=2)]
    assert (
        "count(DISTINCT (provider_system,provider_id,entity_type,entity_id))>1"
        in connection.fetchval.await_args.args[0]
    )


async def test_compact_v2_preserves_direct_origin(monkeypatch):
    pin = SimpleNamespace(generation_id="a" * 64)
    manifest = SimpleNamespace(
        manifest_sha256="b" * 64,
        compact_layout=SimpleNamespace(base=SimpleNamespace(table_oid=12)),
        source_generations={"unified_address": "a" * 64},
    )
    monkeypatch.setattr(
        compact,
        "resolve_compact_address_origin",
        AsyncMock(return_value=({"descriptor_sha256": "c" * 64}, SimpleNamespace(table_oid=12))),
    )
    verified = AsyncMock()
    strict = AsyncMock()
    monkeypatch.setattr(compact, "_verify_witness_stage", verified)
    monkeypatch.setattr(compact, "prepare_source_offices", strict)
    artifact, summary = await compact.prepare_compact_composition_source(
        object(),
        SimpleNamespace(base_source=pin),
        object(),
        {"loader_roles": ("loader",), "reader_roles": ("reader",)},
        (object(),),
        SimpleNamespace(source_manifest=manifest, control_schema="registry"),
        verified_stage=object(),
    )
    assert artifact.origin_manifest_sha256 is None and summary is None
    verified.assert_awaited_once()
    strict.assert_not_awaited()


async def test_independent_v2_reuse_follows_declared_source_not_publication_fence(monkeypatch):
    previous = SimpleNamespace(generation_id=UUID(int=1))
    address_inputs = (object(), ("reader",))
    monkeypatch.setattr(office_records, "verify_initial_source_office_receipt", lambda candidate: {"revision": 5})
    monkeypatch.setattr(office_custody, "_office_inputs", lambda candidate: address_inputs)
    lineage = AsyncMock(return_value=(previous, object()))
    head = AsyncMock()
    stage = AsyncMock(return_value=object())
    monkeypatch.setattr(custody, "resolve_registry_retained_generation", lineage)
    monkeypatch.setattr(custody, "resolve_network_serving_manifest", head)
    monkeypatch.setattr(custody, "stage_registry_source_recipes", stage)
    candidate_by_field = {"candidate_id": str(UUID(int=3)), "expected_head": str(UUID(int=2))}
    await custody._independent_source_stage(
        object(),
        SimpleNamespace(candidate_id=str(UUID(int=3))),
        candidate_by_field,
        object(),
        expected={"revision": 4},
        control_schema="registry",
        office_authority=None,
        inventory=(object(), (object(),), ()),
    )
    read_context = stage.await_args.kwargs["read_context"]
    assert read_context.source_manifest is previous
    assert read_context.verified_office_inputs == address_inputs
    head.assert_not_awaited()


@pytest.mark.parametrize(
    "failure", [None, PermissionError("source denied"), ValueError("census differs"), asyncio.CancelledError()]
)
async def test_readiness_checks_policy_without_staging_and_propagates_every_failure(monkeypatch, failure):
    from process import network_approved_source_bindings as bindings
    from process import network_fhir_membership_source as fhir

    session = SimpleNamespace(begin_nested=AsyncMock())
    driver = object()
    monkeypatch.setattr(runtime, "_driver", AsyncMock(return_value=driver))
    stage = AsyncMock()
    monkeypatch.setattr(composition, "stage_registry_source_recipes", stage)
    policy = AsyncMock(return_value=(context(), ()), side_effect=failure)
    monkeypatch.setattr(offices, "_policy_context", policy)
    reviewed_arguments = []
    monkeypatch.setattr(fhir, "_reviewed_descriptor", lambda *arguments: reviewed_arguments.append(arguments))
    bound = AsyncMock()
    monkeypatch.setattr(bindings, "require_approved_network_source_bindings", bound)
    settings = SimpleNamespace(control_schema="registry", address_profile=SimpleNamespace(runtime_roles=("reader",)))
    service = runtime.RegistryMembershipRefreshInputsService(SimpleNamespace(settings=settings), None)
    recipe = SimpleNamespace(source_pin=object(), binding_coordinates=object())
    selected, manifest, pin = SimpleNamespace(recipes=(recipe,)), object(), object()
    approved = object()
    is_ready_operation = service._has_witness_office_correspondence(session, approved, selected, pin, manifest)
    if failure is None:
        assert await is_ready_operation is True
    else:
        with pytest.raises(type(failure)) as observed:
            await is_ready_operation
        assert observed.value is failure
    policy.assert_awaited_once_with(driver, pin, ("reader",), approved, "registry")
    assert reviewed_arguments == [(recipe.source_pin, approved, recipe.binding_coordinates, True)]
    bound.assert_awaited_once_with(driver, approved, recipe.binding_coordinates, control_schema="registry")
    session.begin_nested.assert_not_awaited()
    stage.assert_not_awaited()
    assert service._readiness_key is None and service._is_ready is False


@pytest.mark.parametrize(
    "failure", [None, PermissionError("policy denied"), ValueError("policy differs"), asyncio.CancelledError()]
)
async def test_witness_policy_requires_full_source_fence_and_never_caches_failure(monkeypatch, failure):
    from tests.registry_membership_refresh_runtime_support import ObservationCase
    from tests.test_registry_membership_refresh_runtime import scope

    case = ObservationCase(monkeypatch)

    async def witness_policy(session, approved, selected, pin, manifest=None):
        return await case.service._has_witness_office_correspondence(session, approved, selected, pin, manifest)

    monkeypatch.setattr(case.service, "_has_office_correspondence", witness_policy)

    async def check_policy(*_coordinates):
        assert case.has_source_fence
        assert case.events[-1] == "address_fenced"
        if failure is not None:
            raise failure
        return context(), ()

    policy = AsyncMock(side_effect=check_policy)
    monkeypatch.setattr(offices, "_policy_context", policy)
    if failure is None:
        assert (await case.service._observe(object(), [scope(1)]))["ready_address_generation"] == case.pin.generation_id
        await case.service._observe(object(), [scope(1)])
        policy.assert_awaited_once()
    else:
        for _attempt in range(2):
            with pytest.raises(type(failure)) as observed:
                await case.service._observe(object(), [scope(1)])
            assert observed.value is failure
            assert case.service._readiness_key is None
            assert case.service._is_ready is False
        assert policy.await_count == 2
    assert not case.has_source_fence


@pytest.mark.parametrize("damage", ["coordinates", "bindings"])
async def test_witness_readiness_retains_complete_binding_guards_before_policy(monkeypatch, damage):
    from dataclasses import replace

    from process import network_approved_source_bindings as bindings
    from process import network_fhir_membership_source as fhir
    from process.network_approved_membership_source import ApprovedMembershipSource
    from tests.test_verified_inventory_v2 import rows

    recipe, _, _ = rows()
    approved = ApprovedMembershipSource(7, "a" * 64)
    if damage == "coordinates":
        recipe = SimpleNamespace(
            source_pin=recipe.source_pin, binding_coordinates=replace(recipe.binding_coordinates, dataset_id="wrong")
        )
    bound = AsyncMock(side_effect=bindings.ApprovedNetworkSourceBindingError("invalid scoped map"))
    monkeypatch.setattr(bindings, "require_approved_network_source_bindings", bound)
    policy = AsyncMock()
    monkeypatch.setattr(offices, "_policy_context", policy)
    driver = object()
    monkeypatch.setattr(runtime, "_driver", AsyncMock(return_value=driver))
    settings = SimpleNamespace(control_schema="registry", address_profile=SimpleNamespace(runtime_roles=("reader",)))
    service = runtime.RegistryMembershipRefreshInputsService(SimpleNamespace(settings=settings), None)
    with pytest.raises(fhir.FHIRMembershipSourceError):
        await service._has_witness_office_correspondence(
            object(), approved, SimpleNamespace(recipes=(recipe,)), object(), None
        )
    assert bound.await_count == int(damage == "bindings")
    policy.assert_not_awaited()
    assert service._readiness_key is None and not service._is_ready


@pytest.mark.parametrize("damage", ["vector", "recipe"])
async def test_witness_readiness_refuses_changed_full_source_before_policy(monkeypatch, damage):
    from contextlib import asynccontextmanager
    from dataclasses import replace

    from process import registry_membership_refresh_sources as sources
    from tests.registry_membership_refresh_runtime_support import ObservationCase
    from tests.test_registry_membership_refresh_runtime import scope
    from tests.test_verified_inventory_v2 import rows

    case = ObservationCase(monkeypatch)
    if damage == "vector":
        full = replace(
            case.selected, accepted_sources=({**case.selected.accepted_sources[0], "capture_generation": "changed"},)
        )
    else:
        full = replace(case.selected, recipes=(rows()[0],))

    @asynccontextmanager
    async def full_pin(*arguments, **keywords):
        async with case.full(*arguments, **keywords):
            yield full

    monkeypatch.setattr(sources, "pin_registry_membership_refresh_sources", full_pin)
    policy = AsyncMock()
    monkeypatch.setattr(offices, "_policy_context", policy)
    with pytest.raises(runtime.RegistryMembershipRefreshUnavailable):
        await case.service._observe(object(), [scope(1)])
    policy.assert_not_awaited()
    case.prepare.assert_not_awaited()
    assert "full_source_pin" in case.events and "address_fenced" not in case.events
    assert case.service._readiness_key is None and not case.service._is_ready
