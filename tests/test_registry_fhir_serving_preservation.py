# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed proof and exact vector refusals; native serving acceptance is connected separately."""

import asyncio
import hashlib
import json
import os
from dataclasses import asdict, replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID, uuid4, uuid5

import asyncpg
import pytest

from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.network_fhir_membership_source import PinnedFHIRMembershipSource
from process.registry_fhir_serving_preservation import (
    FHIRServingPreservationError,
    fhir_preservation_obligation,
    fhir_serving_preservation_sha256,
    fhir_vector_succession,
    require_fhir_source_event_delta,
    validate_fhir_serving_preservation,
    verify_fhir_serving_preservation,
)
from process.registry_publication_execution import (
    RegistrySourceRollbackPublicationAuthority,
    require_registry_source_rollback_publication_authority,
)
from process.registry_source_recipe_store import RegistrySourceMembershipRecipe, canonical_registry_source_recipes


def recipe(edition="one", endpoint="endpoint-one"):
    source = PinnedFHIRMembershipSource(
        "source_example",
        "source-example",
        endpoint,
        "dataset-" + edition,
        "a" * 64,
        "release-" + edition,
        123,
        "medical",
        "2026-10-01",
    )
    coordinates = RegistryNetworkSourceCoordinates(
        "fhir",
        source.source_id,
        source.schema_name,
        source.dataset_id,
        "producer-example",
        edition,
    )
    return RegistrySourceMembershipRecipe(source, coordinates)


def receipt():
    return {
        "component": "registry_fhir_serving_preservation",
        "revision": 1,
        "predecessor_generation": 2,
        "predecessor_candidate_id": str(UUID(int=2)),
        "predecessor_manifest_sha256": "a" * 64,
        "previous_recipes_sha256": "b" * 64,
        "current_recipes_sha256": "c" * 64,
        "coverage_sha256": "d" * 64,
        "missing_networks_sha256": "e" * 64,
        "membership_sha256": "f" * 64,
        "bindings_sha256": "1" * 64,
        "addresses_sha256": "2" * 64,
        "approved_revision": 4,
        "expected_head": 2,
        "missing_networks": 1,
        "carried_rows": 3,
        "binding_rows": 2,
        "address_rows": 2,
        "request_id": str(UUID(int=4)),
        "operation": "forward",
        "operation_sha256": hashlib.sha256(b"forward").hexdigest(),
    }


@pytest.mark.parametrize(
    "change",
    [
        {"extra": 1},
        {"revision": True},
        {"carried_rows": True},
        {"missing_networks": 0},
        {"predecessor_generation": 0},
        {"predecessor_candidate_id": str(UUID(int=0))},
        {"request_id": "invalid"},
        {"coverage_sha256": "D" * 64},
        {"operation": "reactivate"},
        {"expected_head": -1},
        {"address_rows": 2**63},
    ],
)
def test_preservation_receipt_is_closed_and_typed(change):
    with pytest.raises(FHIRServingPreservationError, match="receipt_invalid"):
        validate_fhir_serving_preservation(receipt() | change)


def test_preservation_receipt_requires_both_report_and_scope():
    proof = receipt()
    candidate_by_field = {
        "validation_json": {proof["component"]: proof},
        "source_generations": {proof["component"]: fhir_serving_preservation_sha256(proof)},
        "approved_custom_revision": proof["approved_revision"],
    }
    assert verify_fhir_serving_preservation(candidate_by_field) == proof
    for field in ("validation_json", "source_generations"):
        with pytest.raises(FHIRServingPreservationError):
            verify_fhir_serving_preservation(candidate_by_field | {field: {}})


def test_vector_preserves_exact_endpoint_slots_and_all_untouched_recipes():
    first, second = recipe(), recipe("one", "endpoint-two")
    successor = recipe("two")
    assert fhir_vector_succession((first, second), (second, successor)) == ((first, successor), (second, second))
    for selected in ((successor,), (second, recipe("two", "endpoint-three")), (first, first, second)):
        with pytest.raises(FHIRServingPreservationError, match="vector_incomplete"):
            fhir_vector_succession((first, second), selected)


@pytest.mark.parametrize("damage", ["old", "new", "digest", "second-change", "omission", "other-family"])
def test_source_checkpoint_cannot_substitute_for_derived_delta(damage):
    first, untouched, successor = recipe(), recipe("one", "endpoint-two"), recipe("two")
    checkpoint_by_field = {
        "replaced_coordinates": asdict(first.binding_coordinates),
        "coordinates": asdict(successor.binding_coordinates),
        "recipe_sha256": hashlib.sha256(canonical_registry_source_recipes((successor,)).encode()).hexdigest(),
    }
    assertion_by_field = {"kind": "cms", "checkpoint": checkpoint_by_field}
    current = (successor, untouched)
    require_fhir_source_event_delta((first, untouched), current, assertion_by_field)
    changes_by_case = {
        "old": {"replaced_coordinates": asdict(replace(first.binding_coordinates, edition_id="other"))},
        "new": {"coordinates": asdict(first.binding_coordinates)},
        "digest": {"recipe_sha256": "f" * 64},
    }
    checkpoint_by_field.update(changes_by_case.get(damage, {}))
    current_by_case = {"second-change": (successor, recipe("two", "endpoint-two")), "omission": (successor,)}
    current = current_by_case.get(damage, current)
    if damage == "other-family":
        assertion_by_field["kind"] = "aca"
    with pytest.raises(FHIRServingPreservationError):
        require_fhir_source_event_delta((first, untouched), current, assertion_by_field)


@pytest.mark.asyncio
async def test_constructed_rollback_context_has_no_authority(monkeypatch):
    from process import registry_publication_execution as execution

    connection, target = object(), object()
    read = AsyncMock(side_effect=ValueError("registry_queue_lease_conflict"))
    monkeypatch.setattr(execution, "publication_request", read)
    with pytest.raises(ValueError, match="authority_required"):
        await require_registry_source_rollback_publication_authority(
            connection, SimpleNamespace(request_id=uuid4()), target, {}, control_schema="example_control"
        )
    assert not read.called
    forged = RegistrySourceRollbackPublicationAuthority(uuid4(), uuid4(), AsyncMock())
    with pytest.raises(ValueError, match="lease_conflict"):
        await require_registry_source_rollback_publication_authority(
            connection, forged, target, {}, control_schema="example_control"
        )
    read.assert_awaited_once()


@pytest.mark.asyncio
async def test_mismatched_reviewed_rollback_context_is_refused(monkeypatch):
    from process import registry_publication_execution as execution

    authority = RegistrySourceRollbackPublicationAuthority(uuid4(), uuid4(), AsyncMock())
    monkeypatch.setattr(execution, "publication_request", AsyncMock(return_value={"request_id": authority.request_id}))
    authenticate = AsyncMock(return_value={"preview_request_id": str(uuid4())})
    monkeypatch.setattr(execution, "_fresh_retained_operation_authority", authenticate)
    monkeypatch.setattr(
        execution,
        "_publication_readiness",
        lambda _: (None, None, None, {"owner_role": "loading_owner", "loader_roles": [], "reader_roles": ["reader"]}),
    )
    monkeypatch.setattr(execution, "_verify_source_rollback_publication_target", AsyncMock(return_value="other-target"))
    with pytest.raises(ValueError, match="candidate_changed"):
        await require_registry_source_rollback_publication_authority(
            object(), authority, "target", {}, control_schema="example_control"
        )
    assert authenticate.await_args.args[0] is authority.authorize


@pytest.mark.asyncio
async def test_reviewed_pre_fhir_target_sets_historical_obligation_baseline(monkeypatch):
    """A verified historical target may precede FHIR even when the current head contains it."""
    from process import registry_fhir_serving_preservation as preservation

    historical = SimpleNamespace(
        generation_id=1,
        candidate_id=str(UUID(int=1)),
        source_generations={},
        manifest_sha256="a" * 64,
        _custody_seal_sha256="d" * 64,
    )
    current = SimpleNamespace(
        generation_id=2,
        candidate_id=str(UUID(int=2)),
        source_generations={"registry_source_recipes": "b" * 64},
        manifest_sha256="c" * 64,
        _custody_seal_sha256="e" * 64,
    )
    candidate_by_field = {
        "expected_head": 2,
        "approved_custom_revision": 3,
        "source_generations": {"retained_network_source": historical.manifest_sha256},
        "validation_json": {},
        "source_recipes_json": [],
    }
    resolve = AsyncMock(
        side_effect=lambda _connection, *, generation_id, control_schema: historical if generation_id == 1 else current
    )
    monkeypatch.setattr(preservation, "resolve_network_serving_manifest", resolve)
    monkeypatch.setattr(preservation, "resolve_retained_registry_source_recipes", AsyncMock(return_value=(recipe(),)))
    connection = SimpleNamespace(fetchrow=AsyncMock(return_value={"validation_json": {}, "source_generations": {}}))
    with pytest.raises(FHIRServingPreservationError, match="proof_required"):
        await fhir_preservation_obligation(connection, candidate_by_field, control_schema="example_control")
    resolve.reset_mock()
    assert (
        await fhir_preservation_obligation(
            connection, candidate_by_field, control_schema="example_control", reviewed_source_manifest=historical
        )
        is None
    )
    assert resolve.await_args.kwargs["generation_id"] == historical.generation_id


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["absent", "paired", "mismatch", "null"])
async def test_mapping_marker_requires_paired_receipt_and_native_final_input(monkeypatch, mode):
    from process import network_custom_address_source as addresses
    from process import network_membership_publication as publication
    from process import registry_fhir_serving_preservation as preservation
    from process import registry_initial_source_composition as offices
    from process.network_address_projection import PinnedAddressSource
    from process.registry_source_recipe_composition import OFFICE_MAPPING_KEY

    final_source = PinnedAddressSource("final_addresses", "entity_address_unified", "a" * 64)
    from tests.test_network_fhir_retained_capture import pin, retained

    retained_pin = pin(retained())
    coordinates = replace(
        recipe().binding_coordinates,
        source_id=retained_pin.source_id,
        dataset_schema=retained_pin.schema_name,
        dataset_id=retained_pin.dataset_id,
    )
    recipes = (RegistrySourceMembershipRecipe(retained_pin, coordinates),)
    generations_by_name = {} if mode == "absent" else {OFFICE_MAPPING_KEY: None if mode == "null" else "b" * 64}
    candidate_by_field = {
        "source_generations": generations_by_name,
        "validation_json": {"address_source": asdict(final_source)},
    }
    summary_by_field = {"correspondence_sha256": "c" * 64 if mode == "mismatch" else "b" * 64}
    monkeypatch.setattr(offices, "verify_initial_source_office_receipt", lambda _: summary_by_field)
    monkeypatch.setattr(
        publication,
        "publication_candidate_readiness",
        lambda _: (None, None, None, {"loader_roles": ["loader"], "reader_roles": ["reader"]}),
    )
    authenticate = AsyncMock()
    monkeypatch.setattr(addresses, "verify_artifact_dependency_identity", authenticate)
    connection = object()
    if mode in {"mismatch", "null"}:
        with pytest.raises(FHIRServingPreservationError, match="source_changed"):
            await preservation._current_office_inputs(
                connection, candidate_by_field, recipes, SimpleNamespace(total_rows=0), "control"
            )
        authenticate.assert_not_awaited()
    else:
        actual = await preservation._current_office_inputs(
            connection, candidate_by_field, recipes, SimpleNamespace(total_rows=0), "control"
        )
        assert actual == (None if mode == "absent" else (final_source, ("loader", "reader")))
        if mode == "paired":
            authenticate.assert_awaited_once_with(
                connection, final_source, ("loader", "reader"), address=True, control_schema="control"
            )
        else:
            authenticate.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["none", "receipt", "marker", "raw_generation"])
async def test_replayed_mapping_binds_full_office_receipt_and_raw_generation(monkeypatch, damage):
    from process import registry_fhir_serving_preservation as preservation
    from process import registry_initial_source_composition as offices
    from process.registry_source_recipe_composition import OFFICE_MAPPING_KEY

    summary_by_field = {"correspondence_sha256": "a" * 64, "office_rows": 0}
    actual_summary = {**summary_by_field, "office_rows": 1} if damage == "receipt" else summary_by_field
    replay = AsyncMock(return_value=actual_summary)
    monkeypatch.setattr(offices, "verified_retained_source_office_summary", replay)
    monkeypatch.setattr(offices, "verify_initial_source_office_receipt", lambda _: summary_by_field)
    candidate_by_field = {
        "source_generations": {
            OFFICE_MAPPING_KEY: "b" * 64 if damage == "marker" else "a" * 64,
            "initial_source_recipe_0": "d" * 64 if damage == "raw_generation" else "c" * 64,
        }
    }
    stage = SimpleNamespace(office_matches=object(), raw_source_generations=("c" * 64,))
    arguments = (object(), candidate_by_field, (), object(), stage, (object(), ("reader",)), "control")
    if damage == "none":
        await preservation._verify_current_offices(*arguments)
    else:
        with pytest.raises(FHIRServingPreservationError, match="source_changed"):
            await preservation._verify_current_offices(*arguments)
    assert replay.await_args.args[-1] is stage.office_matches


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["generation", "multiset"])
async def test_current_mapping_keeps_generation_and_full_multiset_guards(damage):
    from process import registry_fhir_serving_preservation as preservation

    connection = SimpleNamespace(fetchval=AsyncMock(return_value=damage == "multiset"))
    candidate_by_field = {"source_generations": {"registry_source_membership": "a" * 64}, "schema_name": "candidate"}
    stage = SimpleNamespace(
        table_name="current_stage", generation_sha256="b" * 64 if damage == "generation" else "a" * 64
    )
    with pytest.raises(FHIRServingPreservationError, match="source_changed|membership_parity"):
        await preservation._current_raw_parity(
            connection,
            candidate_by_field,
            SimpleNamespace(membership_query="SELECT * FROM exact_carry"),
            stage,
            (1, 1),
            "control",
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("is_prepared,compact", [(False, False), (True, False), (False, True), (True, True)])
async def test_current_mapping_keeps_base_and_prepared_artifact_stays_separate(monkeypatch, is_prepared, compact):
    from process import registry_candidate_composition as composition
    from process.network_address_projection import PinnedAddressSource
    from tests.test_registry_ptg_address_carry import _artifact

    artifact = replace(
        _artifact(), address_source=PinnedAddressSource("final_fixture", "entity_address_unified", "d" * 64)
    )
    if is_prepared and compact:
        with pytest.raises(composition.RegistryCompositionError, match="address_artifact_invalid"):
            composition.RegistryCompositionAddressSources(
                artifact.base_source, prepared_address_artifact=artifact, compact=True
            )
        return
    address_sources = composition.RegistryCompositionAddressSources(
        artifact.base_source, prepared_address_artifact=artifact if is_prepared else None, compact=compact
    )
    authenticate = AsyncMock()
    monkeypatch.setattr(composition, "verify_custom_address_source", authenticate)
    roles_by_name = {"loader_roles": (), "reader_roles": ("reader",)}
    actual = composition._verified_office_inputs(address_sources, roles_by_name)
    expected = (
        None
        if is_prepared
        else (
            artifact.base_source
            if compact
            else composition._retained_address_artifact(artifact.base_source, ()).address_source,
            ("reader",),
        )
    )
    assert actual == expected
    authenticate.assert_not_awaited()


@pytest.mark.asyncio
async def test_preservation_is_recorded_before_current_v5_selected_stage(monkeypatch):
    from process import registry_candidate_composition as composition
    from process import registry_initial_source_composition as offices

    events = []
    selected = object()
    stage = SimpleNamespace(
        office_matches=object(), preservation=SimpleNamespace(receipt=receipt()), selected_stage=selected
    )

    async def recorded(*_args):
        events.append("preservation")

    connection = SimpleNamespace(execute=AsyncMock(side_effect=recorded))
    artifact = SimpleNamespace(address_source=object(), runtime_roles=("reader",))

    async def current_offices(*args, **kwargs):
        assert args[6] is selected and events == ["preservation"]
        events.append("current_offices")

    monkeypatch.setattr(offices, "record_verified_witness_offices", current_offices)
    await composition._record_source_receipts(
        connection,
        SimpleNamespace(candidate_id=str(uuid4())),
        (),
        object(),
        stage,
        artifact,
        ({"revision": 5}, object(), {}, object()),
        "control",
    )
    assert events == ["preservation", "current_offices"]


async def _native_custody_lifecycle_readiness(case):
    from process.network_approved_membership_source import pin_approved_membership_source
    from process.network_serving_read import resolve_network_serving_manifest
    from process.registry_fhir_catalog_grant import build_fhir_catalog_publication_runtime
    from process.registry_publication_consumer import consume_registry_publication_once
    from tests.fhir_catalog_serving_rollover_support import grant_pair_read, publish_directory

    async with case.fixture.connection.transaction(isolation="repeatable_read", readonly=True):
        approved = await pin_approved_membership_source(
            case.fixture.connection, approved_revision=case.seed.revision, control_schema=case.fixture.control_schema
        )
        predecessor = await resolve_network_serving_manifest(
            case.fixture.connection, control_schema=case.fixture.control_schema
        )
    baseline_readers = SimpleNamespace(runtime_roles=(case.fixture.roles["reader"],))
    await grant_pair_read(case, baseline_readers)
    await publish_directory(
        case,
        baseline_readers,
        {"approved_revision": approved.approved_revision, "generation_id": approved.generation_id},
        predecessor,
        ordinary=True,
    )
    serving_lifecycle = case.worker.roles_by_kind["lifecycle"]
    metadata_lifecycle = case.config.consumer.lifecycle_role
    assert serving_lifecycle != metadata_lifecycle
    ledger = f'"{case.fixture.control_schema}".network_candidate_custody'
    assert await case.fixture.connection.fetchval(
        "SELECT has_table_privilege($1::name,$2,'SELECT')", serving_lifecycle, ledger
    )
    assert not await case.fixture.connection.fetchval(
        "SELECT has_table_privilege($1::name,$2,'SELECT')", metadata_lifecycle, ledger
    )
    runtime = build_fhir_catalog_publication_runtime(case.config.grant, None)
    consumer = replace(case.config.consumer, metadata_runtime=runtime, metadata_only=True)
    lifecycle_dsn = case.config.lifecycle_url.set(drivername="postgresql").render_as_string(hide_password=False)
    execution_dsn = case.config.execution_url.set(drivername="postgresql").render_as_string(hide_password=False)
    async with (
        asyncpg.create_pool(lifecycle_dsn, min_size=1, max_size=1) as lifecycle_pool,
        asyncpg.create_pool(execution_dsn, min_size=1, max_size=1) as execution_pool,
    ):
        assert (
            await consume_registry_publication_once(
                lifecycle_pool=lifecycle_pool,
                execution_pool=execution_pool,
                config=consumer,
                authorize=runtime.authorize_metadata,
            )
            is None
        )


@pytest.mark.asyncio
async def test_connected_fixture_has_real_extendable_company_group_history(monkeypatch, tmp_path):
    if not os.getenv("NETWORK_REGISTRY_TEST_DSN"):
        pytest.skip("NETWORK_REGISTRY_TEST_DSN must select an owned PostgreSQL fixture")
    from tests.fhir_catalog_serving_rollover_support import approve_edition
    from tests.fhir_catalog_sync_integration_support import native_catalog_case
    from tests.test_registry_approval_store_postgres import _approve, _command, _create, _draft

    async with native_catalog_case(monkeypatch, tmp_path, serving_custody=True) as case:
        await _native_custody_lifecycle_readiness(case)
        network_record = await _draft(
            case.fixture.engine, case.fixture.control_schema, _create("network"), case.seed.actor
        )
        network_approval = await _approve(
            case.fixture.connection,
            case.fixture.control_schema,
            await _command(case.fixture.connection, case.fixture.control_schema, network_record),
            case.seed.actor,
        )
        assert network_approval["approved_revision"] > case.seed.revision
        source_recipe = recipe()
        source_recipe = replace(
            source_recipe,
            source_pin=replace(source_recipe.source_pin, schema_name=case.fixture.control_schema),
            binding_coordinates=replace(source_recipe.binding_coordinates, dataset_schema=case.fixture.control_schema),
        )
        await case.fixture.connection.execute(
            f'CREATE TABLE "{case.fixture.control_schema}".provider_directory_insurance_network_source_binding '
            "(source_id text NOT NULL,resource_type text NOT NULL,resource_id text NOT NULL,network_id uuid NOT NULL)"
        )
        await case.fixture.connection.execute(
            f'INSERT INTO "{case.fixture.control_schema}".provider_directory_insurance_network_source_binding '
            "VALUES($1,'Organization','network-example',$2)",
            source_recipe.source_pin.source_id,
            uuid4(),
        )
        binding_approval = await approve_edition(
            case, SimpleNamespace(recipe=source_recipe), {"network-example": network_record["record_id"]}
        )
        assert binding_approval["approved_revision"] > network_approval["approved_revision"]


@pytest.mark.parametrize(
    "change",
    [
        {"component": "unrelated"},
        {"binding_rows": 4},
        {"predecessor_generation": 3},
        {"operation_sha256": "a" * 64},
    ],
)
def test_preservation_receipt_cannot_change_its_component_counts_or_forward_predecessor(change):
    with pytest.raises(FHIRServingPreservationError, match="receipt_invalid"):
        validate_fhir_serving_preservation(receipt() | change)


def test_cms_requires_actual_delta_while_other_family_accepts_unchanged_vector():
    previous = (recipe(),)
    require_fhir_source_event_delta(previous, previous, {"kind": "aca"})
    with pytest.raises(FHIRServingPreservationError, match="event_delta"):
        require_fhir_source_event_delta(previous, previous, {"kind": "cms", "checkpoint": {}})


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", [False, True])
async def test_preserved_memberships_append_in_one_statement_with_complete_accounting(damage):
    from process import registry_fhir_serving_preservation as preservation

    stage = SimpleNamespace(table_name="selected", membership_rows=2, generation_sha256="a" * 64)
    carried = SimpleNamespace(membership_query="SELECT * FROM exact_carry", receipt=receipt())
    connection = SimpleNamespace(execute=AsyncMock(), fetchval=AsyncMock(return_value=4 if damage else 5))
    if damage:
        with pytest.raises(FHIRServingPreservationError, match="membership_accounting"):
            await preservation.append_fhir_preservation_memberships(connection, stage, carried)
    else:
        from process.registry_source_recipe_composition import RegistrySourceRecipeStage

        # Use the real stage contract so replacement keeps all caller-owned fields.
        fields_by_name = {name: None for name in RegistrySourceRecipeStage.__dataclass_fields__}
        fields_by_name.update(table_name="selected", membership_rows=2, generation_sha256="a" * 64)
        stage = RegistrySourceRecipeStage(**fields_by_name)
        result = await preservation.append_fhir_preservation_memberships(connection, stage, carried)
        assert result.membership_rows == 5 and result.selected_stage is stage and result.preservation is carried
        assert result.generation_sha256 != stage.generation_sha256
    assert connection.execute.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, ValueError, asyncio.CancelledError])
async def test_current_fhir_replay_cleans_its_stage_after_success_failure_or_cancel(monkeypatch, failure):
    from process import network_source_contribution_custody as custody
    from process import registry_fhir_serving_preservation as preservation
    from process import registry_source_recipe_composition as composition

    connection, candidate, carried, approved, stage = object(), {}, object(), object(), object()
    offices = (object(), ("reader",))
    monkeypatch.setattr(preservation, "_current_office_inputs", AsyncMock(return_value=offices))
    staging = AsyncMock(return_value=stage)
    monkeypatch.setattr(composition, "stage_registry_source_recipes", staging)
    office_check = AsyncMock()
    monkeypatch.setattr(preservation, "_verify_current_offices", office_check)
    error = failure("replay interrupted") if failure is not None else None
    parity = AsyncMock(side_effect=error)
    monkeypatch.setattr(preservation, "_current_raw_parity", parity)
    cleanup = AsyncMock()
    monkeypatch.setattr(custody, "_cleanup_independent_source_stage", cleanup)
    arguments = (connection, candidate, carried, (recipe(),), approved, "control")
    if failure is None:
        await preservation._verify_current_fhir_rows(*arguments)
    else:
        with pytest.raises(failure) as caught:
            await preservation._verify_current_fhir_rows(*arguments)
        assert caught.value is error
    assert staging.await_args.kwargs["read_context"].verified_office_inputs is offices
    office_check.assert_awaited_once()
    cleanup.assert_awaited_once_with(connection, stage, None, error)


@pytest.mark.asyncio
async def test_mixed_vector_parity_scopes_comparison_to_both_approved_fhir_cohorts():
    from process import registry_fhir_serving_preservation as preservation

    connection = SimpleNamespace(fetchval=AsyncMock(return_value=False))
    candidate_by_name = {"schema_name": "candidate", "source_generations": {}, "approved_custom_revision": 9}
    carried = SimpleNamespace(
        membership_query="SELECT * FROM retained_facts", predecessor=SimpleNamespace(approved_custom_revision=7)
    )
    await preservation._current_raw_parity(
        connection, candidate_by_name, carried, SimpleNamespace(table_name="current"), (1, 2), "control"
    )
    sql, revisions = connection.fetchval.await_args.args
    assert revisions == [7, 9]
    assert "EXCEPT ALL" in sql and "JOIN cohort USING(network_id)" in sql
    assert "bool_and" in sql and "insurance_plan_id" in sql


@pytest.mark.asyncio
@pytest.mark.parametrize("bound_failure", [False, True])
async def test_carry_copy_reduces_batch_then_resumes_exact_keyset(monkeypatch, bound_failure):
    from process import network_bootstrap_sources as bootstrap
    from process import registry_fhir_serving_preservation as preservation

    first_by_name = {"row_count": 2, "last_key": "office-b", "native_bytes": bootstrap._COPY_BATCH_BYTES}
    small_by_name = {"row_count": 1, "last_key": "office-a", "native_bytes": 1}
    pages = [
        first_by_name,
        small_by_name,
        {"row_count": 1, "last_key": "office-b", "native_bytes": 1},
        {"row_count": 0},
    ]
    if bound_failure:
        first_by_name["native_bytes"] = 1
    copy = AsyncMock(side_effect=[bootstrap._CopyBatchTooLarge(), None, None] if bound_failure else None)
    monkeypatch.setattr(bootstrap, "_copy_native_batch", copy)
    connection = SimpleNamespace(fetchrow=AsyncMock(side_effect=pages))
    assert await preservation._copy_carry_address_rows(connection, "candidate", "SELECT a.* FROM offices a", (3,)) == 2
    assert connection.fetchrow.await_args_list[2].args[1:] == (3, "office-a")
    assert "a.location_key>$2" in connection.fetchrow.await_args_list[2].args[0]
    assert copy.await_args.args[2] == (3, "office-a", "office-b")


@pytest.mark.asyncio
@pytest.mark.parametrize("native_overflow", [False, True])
async def test_single_oversized_carry_row_refuses_instead_of_unbounded_retry(monkeypatch, native_overflow):
    from process import network_bootstrap_sources as bootstrap
    from process import registry_fhir_serving_preservation as preservation

    connection = SimpleNamespace(
        fetchrow=AsyncMock(
            return_value={
                "row_count": 1,
                "last_key": "office",
                "native_bytes": 1 if native_overflow else bootstrap._COPY_BATCH_BYTES,
            }
        )
    )
    copy = AsyncMock(side_effect=bootstrap._CopyBatchTooLarge())
    monkeypatch.setattr(bootstrap, "_copy_native_batch", copy)
    with pytest.raises(FHIRServingPreservationError, match="address_copy_bound"):
        await preservation._copy_carry_address_rows(connection, "candidate", "SELECT a.* FROM offices a", (3,))
    assert connection.fetchrow.await_count == 1
    assert copy.await_count == int(native_overflow)


@pytest.mark.parametrize("field", ["validation_json", "source_generations"])
def test_preservation_metadata_cannot_decode_an_array_as_a_registered_object(field):
    proof = receipt()
    candidate_by_field = {
        "validation_json": {proof["component"]: proof},
        "source_generations": {proof["component"]: fhir_serving_preservation_sha256(proof)},
        "approved_custom_revision": proof["approved_revision"],
    }
    candidate_by_field[field] = json.dumps([proof])
    with pytest.raises(FHIRServingPreservationError, match="receipt_invalid"):
        verify_fhir_serving_preservation(candidate_by_field)


@pytest.mark.parametrize("inventory", [False, True])
def test_source_checkpoint_cannot_hide_an_additional_endpoint_recipe(inventory):
    previous, successor = recipe(), recipe("two")
    untouched = recipe("one", "endpoint-two")
    assertion_by_field = {
        "kind": "cms",
        "checkpoint": {
            "replaced_coordinates": asdict(previous.binding_coordinates),
            "coordinates": asdict(successor.binding_coordinates),
            "recipe_sha256": hashlib.sha256(canonical_registry_source_recipes((successor,)).encode()).hexdigest(),
        },
    }
    with pytest.raises(FHIRServingPreservationError, match="event_delta"):
        require_fhir_source_event_delta(
            (previous, untouched),
            (successor, untouched, recipe("one", "endpoint-three")),
            assertion_by_field,
            inventory=inventory,
        )


def _initial_preservation_candidate():
    from process.network_membership_copy import MembershipCopyTarget

    identities = tuple(str(uuid4()) for _ in range(4))
    target = MembershipCopyTarget(*identities, "network_candidate_" + UUID(identities[-1]).hex)
    candidate_by_field = {
        name: getattr(target, name)
        for name in ("dataset_id", "schema_id", "producer_id", "candidate_id", "schema_name")
    }
    candidate_by_field.update(expected_head=0, approved_custom_revision=4, validation_json={}, source_generations={})
    return target, candidate_by_field


@pytest.mark.asyncio
async def test_initial_candidate_and_absent_carry_leave_source_state_untouched():
    from process import registry_fhir_serving_preservation as preservation

    target, candidate = _initial_preservation_candidate()
    connection = SimpleNamespace(execute=AsyncMock(), fetchval=AsyncMock(), fetchrow=AsyncMock())
    stage = object()
    assert await preservation.append_fhir_preservation_memberships(connection, stage, None) is stage
    assert (
        await preservation.verify_fhir_preservation_rows(connection, target, candidate, control_schema="registry")
        is None
    )
    await preservation.verify_fhir_preservation_target_rows(
        connection, target, candidate, None, control_schema="registry"
    )
    await preservation._verify_current_fhir_rows(connection, candidate, None, (), object(), "registry")
    connection.execute.assert_not_awaited()
    connection.fetchval.assert_not_awaited()
    connection.fetchrow.assert_not_awaited()


@pytest.mark.asyncio
async def test_initial_candidate_cannot_borrow_a_predecessor_preservation_receipt():
    _, candidate = _initial_preservation_candidate()
    proof = receipt()
    candidate["validation_json"] = {proof["component"]: proof}
    candidate["source_generations"] = {proof["component"]: fhir_serving_preservation_sha256(proof)}
    connection = SimpleNamespace(fetchrow=AsyncMock())
    with pytest.raises(FHIRServingPreservationError, match="predecessor_changed"):
        await fhir_preservation_obligation(connection, candidate, control_schema="registry")
    connection.fetchrow.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["dataset_id", "schema_id", "producer_id", "candidate_id", "schema_name"])
async def test_preservation_rederivation_binds_every_target_coordinate_before_source_reads(monkeypatch, field):
    from process import registry_fhir_serving_preservation as preservation

    target, candidate = _initial_preservation_candidate()
    candidate[field] = "other_candidate" if field == "schema_name" else str(uuid4())
    obligation = AsyncMock()
    monkeypatch.setattr(preservation, "fhir_preservation_obligation", obligation)
    with pytest.raises(FHIRServingPreservationError, match="source_changed"):
        await preservation.rederive_fhir_serving_preservation(object(), target, candidate, control_schema="registry")
    obligation.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["missing_relation", "ambiguous_identity", "changed_selection", "unproved_selection"])
async def test_predecessor_selection_requires_existing_unambiguous_and_proven_raw_facts(fault):
    from process import registry_fhir_serving_preservation as preservation

    async def cursor(*_arguments, **_options):
        for row in ():
            yield row

    replies_by_fault = {
        "missing_relation": [False],
        "ambiguous_identity": [True, True],
        "changed_selection": [True, False],
        "unproved_selection": [True, False, True],
    }
    connection = SimpleNamespace(
        fetchval=AsyncMock(side_effect=replies_by_fault[fault]), execute=AsyncMock(), cursor=cursor
    )
    tables = preservation._IdentityTables(
        *(f"pg_temp.{name}" for name in ("inventory", "coverage", "subjects", "current", "reviewed", "missing"))
    )
    expected_by_fault = {
        "missing_relation": "baseline_unavailable",
        "ambiguous_identity": "identity_ambiguous",
        "changed_selection": "prior_selection_changed",
        "unproved_selection": "prior_selection_unproven",
    }
    with pytest.raises(FHIRServingPreservationError, match=expected_by_fault[fault]):
        await preservation._previous_selection(
            connection,
            SimpleNamespace(schema_name="retained"),
            tables,
            receipt() if fault == "changed_selection" else None,
        )
    assert connection.fetchval.await_count == len(replies_by_fault[fault])
    assert connection.execute.await_count == int(fault != "missing_relation")


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["current", "overlay"])
@pytest.mark.parametrize("cancelled", [False, True])
async def test_legacy_replay_failure_preserves_primary_error_and_cleans_exact_carry_tables(
    monkeypatch, phase, cancelled
):
    from process import network_approved_membership_source as approved_sources
    from process import registry_fhir_serving_preservation as preservation
    from process.network_approved_membership_source import ApprovedMembershipSource

    copy_target, candidate = _initial_preservation_candidate()
    proof = receipt()
    request_id = UUID(proof["request_id"])
    copy_target = replace(
        copy_target, **{name + "_id": str(uuid5(request_id, name)) for name in ("dataset", "schema", "producer")}
    )
    candidate.update(
        **asdict(copy_target),
        expected_head=proof["expected_head"],
        validation_json={proof["component"]: proof},
        source_generations={
            proof["component"]: fhir_serving_preservation_sha256(proof),
            "retained_network_source": proof["predecessor_manifest_sha256"],
        },
    )
    relation = 'pg_temp."fhir_preserve_missing_' + UUID(int=1).hex + '"'
    carried = preservation.FHIRServingPreservation(
        object(), relation, "SELECT * FROM retained_memberships", "", "", proof, temporary_tables=(relation,)
    )
    monkeypatch.setattr(preservation, "rederive_fhir_serving_preservation", AsyncMock(return_value=carried))
    monkeypatch.setattr(preservation, "verify_fhir_preservation_target_rows", AsyncMock())
    monkeypatch.setattr(preservation, "_candidate_recipes", AsyncMock(return_value=((recipe(),), None)))
    approved = ApprovedMembershipSource(candidate["approved_custom_revision"], "a" * 64)
    monkeypatch.setattr(approved_sources, "pin_approved_membership_source", AsyncMock(return_value=approved))
    error = asyncio.CancelledError("cancelled during replay") if cancelled else TimeoutError("replay timed out")
    replay = AsyncMock(side_effect=error if phase == "current" else None)
    overlay = AsyncMock(side_effect=error if phase == "overlay" else None)
    monkeypatch.setattr(preservation, "_verify_current_fhir_rows", replay)
    monkeypatch.setattr(preservation, "_verify_current_overlay", overlay)
    cleanup_error = RuntimeError("drop refused")
    connection = SimpleNamespace(execute=AsyncMock(side_effect=cleanup_error))
    with pytest.raises(type(error)) as rejected:
        await preservation.verify_fhir_preservation_rows(connection, copy_target, candidate, control_schema="registry")
    assert rejected.value is error and error._registry_preservation_cleanup_error is cleanup_error
    connection.execute.assert_awaited_once_with(f"DROP TABLE IF EXISTS {relation}")
    replay.assert_awaited_once_with(connection, candidate, carried, (recipe(),), approved, "registry")
    assert overlay.await_count == int(phase == "overlay")
