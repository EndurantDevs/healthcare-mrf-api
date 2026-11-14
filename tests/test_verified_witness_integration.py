# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""V2 policy and lineage orchestration; mocks confer no native authority."""

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
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
from process import registry_verified_witness_stage as witness_stage
from process.network_approved_membership_source import ApprovedMembershipSource
from tests.test_verified_inventory_v2 import context, rows


@pytest.mark.parametrize("aliases", [False, True])
async def test_global_policy_keeps_legacy_distinction_and_explicit_v2_alias_identity(monkeypatch, aliases):
    connection = SimpleNamespace(execute=AsyncMock(), transaction=Mock(return_value=AsyncMock()))
    monkeypatch.setattr(cms, "_verified_office_match_keys", AsyncMock(return_value="winners"))
    await cms._finish_verified_office_matches(
        connection,
        "inputs",
        "matches",
        SimpleNamespace(schema_name="addresses", table_name="entity_address_unified"),
        {},
        source_aliases=aliases,
    )
    hydration_sql = " ".join(connection.execute.await_args_list[0].args[0].split())
    assert 'FROM pg_temp."winners" winner JOIN LATERAL (' in hydration_sql
    staged_sql, address_sql = hydration_sql.split(") input ON true JOIN LATERAL (", 1)
    assert "SELECT staged.page_key,staged.office,staged.canonical" in staged_sql
    assert "WHERE staged.page_key=winner.page_key AND staged.ordinal=winner.ordinal OFFSET 0" in staged_sql
    assert 'FROM "addresses"."entity_address_unified" source_address' in address_sql
    assert "WHERE source_address.location_key=winner.location_key OFFSET 0" in address_sql
    assert "to_jsonb(address) AS address" in hydration_sql
    statements = [call.args[0] for call in connection.execute.await_args_list]
    identity_query = next(statement for statement in statements if statement.startswith("INSERT INTO"))
    assert "SELECT ctid AS row_id,page_key,office->>'provider_system' AS provider_system" in identity_query
    expected = "jsonb_build_array(provider_system,provider_id"
    assert (expected + ")::text") in identity_query if aliases else (expected + ",location_id)::text") in identity_query
    assert "office->>'location_id' AS location_id" in identity_query
    assert "office->>'location_key' AS location_key" in identity_query
    assert any('provider_identity text COLLATE "C"' in statement for statement in statements)
    assert statements[-1].startswith("DROP TABLE")


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
        compact_layout=SimpleNamespace(
            base=SimpleNamespace(schema_name="retained", table_name="entity_address_unified")
        ),
        source_generations={"unified_address": "a" * 64},
    )
    monkeypatch.setattr(
        compact,
        "resolve_compact_address_origin",
        AsyncMock(
            return_value=(
                {"descriptor_sha256": "c" * 64},
                SimpleNamespace(schema_name="retained", table_name="entity_address_unified"),
            )
        ),
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
    recipe, _, _ = rows()
    monkeypatch.setattr(office_records, "verify_initial_source_office_receipt", lambda candidate: {"revision": 5})
    lineage = AsyncMock(return_value=(previous, object()))
    head = AsyncMock()

    async def require_scope(_connection, recipes, approved, **options):
        witness_stage._require_witness_scope(recipes, approved, options["read_context"].verified_office_inputs)
        return object()

    stage = AsyncMock(side_effect=require_scope)
    monkeypatch.setattr(custody, "resolve_registry_retained_generation", lineage)
    monkeypatch.setattr(custody, "resolve_network_serving_manifest", head)
    monkeypatch.setattr(custody, "stage_registry_source_recipes", stage)
    candidate_by_field = {
        "candidate_id": str(UUID(int=3)),
        "expected_head": str(UUID(int=2)),
        "validation_json": {
            "address_source": {
                "schema_name": "named_addresses",
                "table_name": "entity_address_unified",
                "generation_id": "b" * 64,
            }
        },
    }
    address_inputs = office_custody._office_inputs(candidate_by_field)
    assert address_inputs[1] == ()
    await custody._independent_source_stage(
        object(),
        SimpleNamespace(candidate_id=str(UUID(int=3))),
        candidate_by_field,
        ApprovedMembershipSource(7, "a" * 64),
        expected={"revision": 4},
        control_schema="registry",
        office_authority=None,
        inventory=(object(), (recipe,), ()),
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
    policy = AsyncMock(return_value=context(), side_effect=failure)
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
        return context()

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


@pytest.mark.parametrize("damage", [None, "schema", "table", "generation"])
async def test_compact_origin_binds_named_relation_and_logical_generation(monkeypatch, damage):
    from dataclasses import replace

    from process.network_address_projection import PinnedAddressSource
    from process.network_compact_layout import CompactAddressRelation

    address = PinnedAddressSource("retained_addresses", "entity_address_unified", "a" * 64)
    relation = CompactAddressRelation(address.schema_name, address.table_name)
    base = replace(relation, schema_name="another_snapshot") if damage == "schema" else relation
    base = replace(base, table_name="other_addresses") if damage == "table" else base
    manifest = SimpleNamespace(
        manifest_sha256="b" * 64,
        compact_layout=SimpleNamespace(base=base),
        source_generations={"unified_address": "c" * 64 if damage == "generation" else address.generation_id},
    )
    resolve = AsyncMock(return_value=({"descriptor_sha256": "d" * 64}, relation))
    monkeypatch.setattr(compact, "resolve_compact_address_origin", resolve)
    artifact, summary = await compact.prepare_compact_composition_source(
        object(),
        SimpleNamespace(base_source=address),
        object(),
        {"loader_roles": (), "reader_roles": ()},
        (),
        SimpleNamespace(source_manifest=manifest, control_schema="registry"),
    )
    assert artifact.origin_manifest_sha256 == (manifest.manifest_sha256 if damage is None else None)
    assert artifact.address_source == address and summary is None
    assert resolve.await_args.args[1] == {
        "source_generations": {
            "unified_address": address.generation_id,
            "retained_network_source": manifest.manifest_sha256,
        },
        "validation_json": {},
    }
