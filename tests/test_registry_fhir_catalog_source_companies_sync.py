# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Source labels refresh metadata while committed source facts and identities stay pinned."""

from contextlib import asynccontextmanager
from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import registry_fhir_catalog_sync as sync
from process import registry_fhir_plan_tier as plan_tier
from process.registry_fhir_catalog_observation import (
    CATALOG_OBSERVATION_CONTRACT,
    catalog_observation_descriptor_digest,
)
from process.registry_fhir_catalog_sources import validate_fhir_catalog_source_descriptor
from process.uhc_provider_file_source_identity import UHC_PROVIDER_FILE_SOURCE_ID
from tests.test_registry_fhir_catalog_grant import snapshot
from tests.test_registry_fhir_catalog_source_companies import source_company
from tests.test_registry_fhir_catalog_source_producer_sync import _preparation, reconciliation_case, source_case


def _source_reads(case):
    return None, None, {sync.canonical_json(case.accepted.descriptor["native_read_profile"]): case.export}


def _mapping(monkeypatch):
    monkeypatch.setattr(
        sync, "select_fhir_catalog_mapping", AsyncMock(return_value=(6, {"metadata": "c" * 64, "membership": "b" * 64}))
    )


def _typed_source(case):
    descriptor = deepcopy(case.accepted.descriptor)
    descriptor["coordinates"]["source_id"] = UHC_PROVIDER_FILE_SOURCE_ID
    descriptor["source_aliases"] = [UHC_PROVIDER_FILE_SOURCE_ID]
    digest = catalog_observation_descriptor_digest(descriptor)
    descriptor.update(
        accepted_receipt_id=CATALOG_OBSERVATION_CONTRACT + ":" + digest,
        accepted_receipt_sha256=digest,
    )
    return replace(
        case.accepted,
        descriptor=validate_fhir_catalog_source_descriptor(descriptor),
        source_aliases=(UHC_PROVIDER_FILE_SOURCE_ID,),
        alias_count=1,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("fail_verification", [False, True])
async def test_label_refresh_verifies_registry_reader_before_writer_commit(monkeypatch, source_case, fail_verification):
    events = []

    @asynccontextmanager
    async def registry_reads(writer):
        assert writer is source_case.loader
        events.append("reader-open")
        try:
            yield
        finally:
            events.append("reader-close")

    @asynccontextmanager
    async def writer_transaction(**_options):
        events.append("writer-open")
        try:
            yield
        except ValueError:
            events.append("writer-rollback")
            raise
        else:
            events.append("writer-commit")

    async def verify_reader():
        events.append("reader-verify")
        if fail_verification:
            raise ValueError("registry_fhir_catalog_registry_reader_changed")

    source_case.loader.transaction = writer_transaction
    monkeypatch.setattr(sync, "fhir_catalog_registry_reads", registry_reads)
    monkeypatch.setattr(sync, "verify_fhir_catalog_registry_reads", verify_reader)
    monkeypatch.setattr(sync, "_stage_catalog_candidate", AsyncMock(return_value=(None, {}, {}, None)))
    operation = sync.prepare_fhir_catalog_snapshot(
        source_case.config,
        source_case.loader,
        None,
        source_case.sealer_pool,
        source_export=source_case.factory,
        source_companies_only=True,
    )
    if fail_verification:
        with pytest.raises(ValueError, match="registry_reader_changed"):
            await operation
    else:
        assert await operation == (None, {}, {})
    assert events == [
        "reader-open",
        "writer-open",
        "reader-verify",
        "writer-rollback" if fail_verification else "writer-commit",
        "reader-close",
    ]
    source_case.export["inventory"].assert_not_called()
    source_case.export["replay"].assert_not_awaited()


@pytest.mark.asyncio
async def test_normal_company_sync_keeps_retained_inputs_on_phase_reader(monkeypatch, source_case):
    reader, manifest = object(), object()
    retained = object()
    monkeypatch.setattr(sync, "fhir_catalog_registry_connection", AsyncMock(return_value=reader))
    monkeypatch.setattr(sync, "_retained_catalog_sources", AsyncMock(return_value=[retained]))
    selected = await sync._preparation_catalog_sources(source_case.config, source_case.loader, manifest, None, [])
    assert selected == [retained]
    sync._retained_catalog_sources.assert_awaited_once_with(reader, source_case.config.grant, manifest)


@pytest.mark.asyncio
async def test_normal_old_export_stays_absent_and_changed_labels_only_change_metadata(monkeypatch, source_case):
    _mapping(monkeypatch)
    accepted, grant = source_case.accepted, source_case.config.grant
    generation = sync.fhir_catalog_source_generation(accepted.descriptor)
    labels = await sync._collect_catalog_source_companies(_source_reads(source_case), [accepted], None)
    assert labels == {}
    original, _ = await sync._catalog_snapshot_inputs(
        source_case.loader, grant, {"approved_revision": 6}, None, None, [accepted]
    )
    source_case.export["source_companies"] = AsyncMock(return_value=(source_company(),))
    labels = await sync._collect_catalog_source_companies(_source_reads(source_case), [accepted], original)
    labeled, _ = await sync._catalog_snapshot_inputs(
        source_case.loader, grant, {"approved_revision": 6}, None, original, [accepted], source_companies=labels
    )
    assert labeled["source_vector_json"][0]["source_companies"] == [source_company()]
    assert labeled["metadata_generation"] != original["metadata_generation"]
    assert labeled["source_vector_json"][0]["source_generation"] == generation
    assert sync.fhir_catalog_source_generation(accepted.descriptor) == generation
    source_case.export["source_companies"].assert_awaited_once_with(accepted.descriptor)
    source_case.export["inventory"].assert_not_called()
    source_case.export["replay"].assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("labels", [[], [source_company()]])
async def test_normal_sync_cannot_silently_downgrade_a_previously_labeled_current_source(source_case, labels):
    previous = snapshot(source_case.config.grant, (source_case.accepted.descriptor,))
    previous["source_vector_json"][0]["source_companies"] = labels
    with pytest.raises(ValueError, match="source_companies_unavailable"):
        await sync._collect_catalog_source_companies(_source_reads(source_case), [source_case.accepted], previous)


@pytest.mark.asyncio
async def test_old_export_preserves_empty_labels_across_alias_changes(source_case):
    previous = snapshot(source_case.config.grant, (source_case.accepted.descriptor,))
    previous["source_vector_json"][0]["source_companies"] = []
    descriptor = deepcopy(source_case.accepted.descriptor)
    descriptor["coordinates"]["source_id"] = "alias-c"
    descriptor["source_aliases"] = ["alias-c"]
    digest = catalog_observation_descriptor_digest(descriptor)
    descriptor.update(accepted_receipt_id=CATALOG_OBSERVATION_CONTRACT + ":" + digest, accepted_receipt_sha256=digest)
    accepted = replace(
        source_case.accepted,
        descriptor=validate_fhir_catalog_source_descriptor(descriptor),
        source_aliases=("alias-c",),
        alias_count=1,
    )
    with pytest.raises(ValueError, match="source_companies_unavailable"):
        await sync._collect_catalog_source_companies(_source_reads(source_case), [accepted], previous)


@pytest.mark.asyncio
async def test_last_good_labels_are_carried_exactly_without_refresh(monkeypatch, source_case):
    _mapping(monkeypatch)
    previous = snapshot(source_case.config.grant, (source_case.accepted.descriptor,))
    labels = [source_company()]
    previous["source_vector_json"][0]["source_companies"] = labels
    source_case.export["source_companies"] = AsyncMock(side_effect=AssertionError("historical labels refreshed"))
    assert await sync._collect_catalog_source_companies(_source_reads(source_case), [], previous) == {}
    selected, _ = await sync._catalog_snapshot_inputs(
        source_case.loader, source_case.config.grant, {"approved_revision": 6}, None, previous, []
    )
    assert selected["source_vector_json"][0] == {**previous["source_vector_json"][0], "status": "last_good"}
    assert selected["source_vector_json"][0]["source_companies"] is labels
    source_case.export["source_companies"].assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("projector_version", ["fhir-catalog/v1", plan_tier.TRANSFORM_VERSION])
async def test_metadata_only_inputs_preserve_prior_extractor_and_exact_vector_identity(
    monkeypatch, source_case, projector_version
):
    _mapping(monkeypatch)
    accepted = _typed_source(source_case)
    previous = snapshot(source_case.config.grant, (accepted.descriptor,), projector_version=projector_version)
    if projector_version == plan_tier.TRANSFORM_VERSION:
        previous["source_vector_json"][0]["extraction_transform"] = projector_version
        previous["metadata_generation"] = sync.metadata_generation(previous)
    generation = sync.fhir_catalog_source_generation(accepted.descriptor)
    selected, _ = await sync._catalog_snapshot_inputs(
        source_case.loader,
        source_case.config.grant,
        {"approved_revision": 6},
        None,
        previous,
        [accepted],
        source_companies={generation: [source_company(source_id=UHC_PROVIDER_FILE_SOURCE_ID)]},
        source_companies_only=True,
    )
    assert selected["projector_version"] == projector_version
    assert {
        field: value for field, value in selected["source_vector_json"][0].items() if field != "source_companies"
    } == previous["source_vector_json"][0]


@pytest.mark.asyncio
@pytest.mark.parametrize("missing", [False, True])
async def test_metadata_only_descriptors_come_from_verified_committed_rows(monkeypatch, source_case, missing):
    previous = snapshot(source_case.config.grant, (source_case.accepted.descriptor,))
    generation = previous["source_vector_json"][0]["source_generation"]
    monkeypatch.setattr(
        sync,
        "load_fhir_catalog_source_descriptors",
        AsyncMock(return_value={generation: source_case.accepted.descriptor}),
    )
    source_case.loader.fetch.return_value = (
        [] if missing else [{"source_generation": generation, "accepted_at": datetime(2026, 1, 1, tzinfo=timezone.utc)}]
    )
    if missing:
        with pytest.raises(ValueError, match="source_companies_history_changed"):
            await sync._committed_catalog_sources(source_case.loader, source_case.config.grant, previous)
    else:
        selected = await sync._committed_catalog_sources(source_case.loader, source_case.config.grant, previous)
        assert len(selected) == 1 and selected[0].descriptor == source_case.accepted.descriptor
        assert selected[0].accepted_at == datetime(2026, 1, 1, tzinfo=timezone.utc)
    sync.load_fhir_catalog_source_descriptors.assert_awaited_once_with(
        source_case.loader, previous, previous["source_vector_json"]
    )
    source_case.export["inventory"].assert_not_called()
    source_case.export["replay"].assert_not_awaited()


@pytest.mark.asyncio
async def test_metadata_only_preparation_forbids_discovery_and_forwards_mode_through_seal(monkeypatch, source_case):
    selected = _preparation(monkeypatch, source_case)
    previous = snapshot(source_case.config.grant, (source_case.accepted.descriptor,), metadata_approved_revision=5)
    sync._catalog_registry_head.return_value = ({"approved_revision": 6}, None, previous)
    source_case.export["source_companies"] = AsyncMock(return_value=(source_company(),))
    monkeypatch.setattr(sync, "_committed_catalog_sources", AsyncMock(return_value=[source_case.accepted]))
    monkeypatch.setattr(sync, "_discover_catalog_sources", AsyncMock(side_effect=AssertionError("Source inventory")))
    monkeypatch.setattr(sync, "_build_source_catalog_candidate", AsyncMock(return_value={"copied": True}))
    monkeypatch.setattr(sync, "_seal_catalog_snapshot", AsyncMock())
    result, prior, _ = await sync.prepare_fhir_catalog_snapshot(
        source_case.config,
        source_case.loader,
        None,
        source_case.sealer_pool,
        source_export=source_case.factory,
        source_companies_only=True,
    )
    assert result is selected and prior is previous
    assert sync._catalog_snapshot_inputs.await_args.kwargs == {
        "source_companies": {previous["source_vector_json"][0]["source_generation"]: [source_company()]},
        "source_companies_only": True,
    }
    assert sync._build_source_catalog_candidate.await_args.kwargs == {"source_companies_only": True}
    assert sync._seal_catalog_snapshot.await_args.kwargs["source_companies_only"] is True
    source_case.export["inventory"].assert_not_called()
    source_case.export["replay"].assert_not_awaited()
    sync._retained_catalog_sources.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("typed", [False, True])
async def test_metadata_only_projection_reuses_ids_and_cached_observations(monkeypatch, source_case, typed):
    accepted = _typed_source(source_case) if typed else source_case.accepted
    previous = snapshot(source_case.config.grant, (accepted.descriptor,))
    if typed:
        previous["source_vector_json"][0]["extraction_transform"] = plan_tier.TRANSFORM_VERSION
        previous["metadata_generation"] = sync.metadata_generation(previous)
    generation = previous["source_vector_json"][0]["source_generation"]
    current_by_generation = {generation: accepted}
    relationships = AsyncMock(return_value=2)
    monkeypatch.setattr(sync, "project_fhir_catalog_relationships", relationships)
    tier_relationships = AsyncMock(return_value=3)
    monkeypatch.setattr(sync, "_carried_plan_tier_relationships", tier_relationships)
    monkeypatch.setattr(sync, "stage_fhir_catalog_source", AsyncMock(side_effect=AssertionError("fresh replay")))
    monkeypatch.setattr(
        sync, "project_fhir_catalog_source", AsyncMock(side_effect=AssertionError("identity allocation"))
    )
    copy_target = SimpleNamespace(schema_name="candidate_example")
    receipt = await sync._project_catalog_sources(
        source_case.config,
        source_case.loader,
        _source_reads(source_case),
        copy_target,
        previous,
        previous,
        current_by_generation,
        source_companies_only=True,
    )
    assert receipt == [
        {"extraction": {"reused_observations": True}, "projection": {"relationships": 5 if typed else 2}}
    ]
    assert relationships.await_args.kwargs["status"] == "current"
    assert tier_relationships.await_count == int(typed)
    if typed:
        projection_context = tier_relationships.await_args.args[3]
        assert projection_context.source_generation == generation
        assert projection_context.metadata_generation == previous["metadata_generation"]
        assert projection_context.coordinates.source_id == UHC_PROVIDER_FILE_SOURCE_ID
        assert projection_context.status == "current"
    sync.stage_fhir_catalog_source.assert_not_awaited()
    sync.project_fhir_catalog_source.assert_not_awaited()
    source_case.export["legacy_bindings"].assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("history", ["absent", "different_source", "old_extractor"])
async def test_metadata_only_projection_refuses_missing_or_incompatible_history_before_writes(
    monkeypatch, source_case, history
):
    accepted = _typed_source(source_case)
    generation = sync.fhir_catalog_source_generation(accepted.descriptor)
    selected = snapshot(source_case.config.grant, (accepted.descriptor,))
    selected["source_vector_json"][0]["extraction_transform"] = plan_tier.TRANSFORM_VERSION
    selected["metadata_generation"] = sync.metadata_generation(selected)
    prior_descriptors = (source_case.accepted.descriptor,) if history == "different_source" else (accepted.descriptor,)
    prior = (
        None
        if history == "absent"
        else snapshot(source_case.config.grant, prior_descriptors, projector_version="fhir-catalog/v1")
    )
    monkeypatch.setattr(sync, "project_fhir_catalog_relationships", AsyncMock())
    monkeypatch.setattr(sync, "_carried_plan_tier_relationships", AsyncMock())
    with pytest.raises(ValueError, match="source_companies_history_changed"):
        await sync._project_catalog_sources(
            source_case.config,
            source_case.loader,
            _source_reads(source_case),
            object(),
            selected,
            prior,
            {generation: accepted},
            source_companies_only=True,
        )
    sync.project_fhir_catalog_relationships.assert_not_awaited()
    sync._carried_plan_tier_relationships.assert_not_awaited()
    source_case.loader.execute.assert_not_awaited()
    source_case.export["replay"].assert_not_awaited()


@pytest.mark.asyncio
async def test_metadata_only_refresh_refuses_a_changed_endpoint_scope(monkeypatch, source_case):
    previous = snapshot(source_case.config.grant, (source_case.accepted.descriptor,))
    source_case.config.source_endpoint_ids = ("another-endpoint",)
    monkeypatch.setattr(sync, "_committed_catalog_sources", AsyncMock(return_value=[source_case.accepted]))
    with pytest.raises(ValueError, match="source_endpoint_scope_changed"):
        await sync._preparation_catalog_sources(
            source_case.config, source_case.loader, None, previous, [], source_companies_only=True
        )
    source_case.loader.execute.assert_not_awaited()
    source_case.export["inventory"].assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("missing", [False, True])
async def test_retained_locator_refresh_requires_the_existing_source_and_preserves_accepted_time(
    monkeypatch, source_case, missing
):
    from tests.test_registry_fhir_catalog_retained import descriptor_fixture

    accepted, _, _ = await descriptor_fixture(monkeypatch, source_uris={"endpoint-one": "https://example.test/fhir"})
    generation = sync.fhir_catalog_source_generation(accepted.descriptor)
    accepted_at = datetime(2025, 12, 1, tzinfo=timezone.utc)
    source_case.loader.fetchval = AsyncMock(return_value=None if missing else accepted_at)
    operation = sync._refresh_retained_descriptor(
        source_case.loader, SimpleNamespace(schema_name="candidate_example"), generation, accepted
    )
    if missing:
        with pytest.raises(ValueError, match="retained_source_missing"):
            await operation
    else:
        refreshed = await operation
        assert refreshed == replace(accepted, accepted_at=accepted_at)
        assert sync.fhir_catalog_source_generation(refreshed.descriptor) == generation
    assert source_case.loader.fetchval.await_args.args[1:] == (generation, sync.canonical_json(accepted.descriptor))
    source_case.loader.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_metadata_only_without_source_callback_or_history_refuses(source_case):
    with pytest.raises(ValueError, match="source_companies_unavailable"):
        await sync.prepare_fhir_catalog_snapshot(
            source_case.config, source_case.loader, None, source_case.sealer_pool, source_companies_only=True
        )
    with pytest.raises(ValueError, match="source_companies_history_required"):
        await sync._committed_catalog_sources(source_case.loader, source_case.config.grant, None)


@pytest.mark.asyncio
async def test_metadata_only_candidate_copies_existing_snapshot_and_pins_all_projection(monkeypatch, source_case):
    previous = snapshot(
        source_case.config.grant, (source_case.accepted.descriptor,), projector_version="fhir-catalog/v1"
    )
    typed_source_by_field = {
        "profile": "published_normalized",
        "coordinates": {"source_id": UHC_PROVIDER_FILE_SOURCE_ID},
    }
    checked_projections = []

    async def project(*_args, **_kwargs):
        assert not plan_tier.supports_plan_tiers(typed_source_by_field)
        checked_projections.append(True)
        return {}

    for name in ("create_fhir_catalog_candidate", "create_fhir_catalog_indexes", "project_manual_fhir_catalog"):
        monkeypatch.setattr(sync, name, AsyncMock(return_value={}))
    copied = AsyncMock(return_value={"rows": 3})
    monkeypatch.setattr(sync, "copy_fhir_catalog_snapshot", copied)
    for name in (
        "_project_catalog_sources",
        "refresh_carried_fhir_catalog_relationships",
        "project_reviewed_fhir_catalog_products",
    ):
        monkeypatch.setattr(sync, name, AsyncMock(side_effect=project))
    await sync._build_source_catalog_candidate(
        source_case.config,
        source_case.loader,
        _source_reads(source_case),
        SimpleNamespace(schema_name="candidate_example"),
        previous,
        previous,
        {previous["source_vector_json"][0]["source_generation"]: source_case.accepted},
        source_companies_only=True,
    )
    assert checked_projections == [True] * 3
    copied.assert_awaited_once()
    assert plan_tier.supports_plan_tiers(typed_source_by_field)


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", [False, True])
async def test_reused_labeled_candidate_still_gets_an_independent_fresh_label_read(monkeypatch, source_case, drift):
    selected = _preparation(monkeypatch, source_case)
    previous = snapshot(source_case.config.grant, (source_case.accepted.descriptor,), metadata_approved_revision=5)
    selected["source_vector_json"][0]["source_companies"] = [source_company()]
    selected["metadata_generation"] = sync.metadata_generation(selected)
    sync._catalog_registry_head.return_value = ({"approved_revision": 6}, None, previous)
    sync._reusable_catalog_snapshot.return_value = selected
    generation = selected["source_vector_json"][0]["source_generation"]
    monkeypatch.setattr(
        sync,
        "load_fhir_catalog_source_descriptors",
        AsyncMock(return_value={generation: source_case.accepted.descriptor}),
    )
    callback = AsyncMock(
        side_effect=[
            (source_company(),),
            (source_company(display_name="Example Mutual") if drift else source_company(),),
        ]
    )
    source_case.export["source_companies"] = callback
    if drift:
        with pytest.raises(ValueError, match="source_companies_changed"):
            await sync.prepare_fhir_catalog_snapshot(
                source_case.config, source_case.loader, None, source_case.sealer_pool, source_export=source_case.factory
            )
    else:
        resumed, _, accounting = await sync.prepare_fhir_catalog_snapshot(
            source_case.config, source_case.loader, None, source_case.sealer_pool, source_export=source_case.factory
        )
        assert resumed is selected and accounting["resumed_snapshot"] is True
    assert source_case.factory.call_count == callback.await_count == 2
    source_case.export["replay"].assert_not_awaited()
    sync._build_source_catalog_candidate.assert_not_awaited()


@pytest.mark.asyncio
async def test_metadata_only_reconciliation_forwards_mode_to_authority_and_preparation(reconciliation_case):
    case, events = reconciliation_case
    sync.prepare_fhir_catalog_snapshot.return_value = (None, {}, {"accepted_sources": 1})
    assert await sync.reconcile_fhir_catalog(case.config, source_export=case.factory, source_companies_only=True) == {
        "state": "unchanged",
        "accepted_sources": 1,
    }
    sync.build_fhir_catalog_publication_runtime.assert_called_once_with(
        case.config.grant, None, source_export=case.factory, source_companies_only=True
    )
    assert sync.prepare_fhir_catalog_snapshot.await_args.kwargs == {
        "source_export": case.factory,
        "source_companies_only": True,
    }
    sync._publish_catalog_snapshot.assert_not_awaited()
    assert events == ["opened"] * 4 + ["closed"] * 4


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "endpoint_ids", [("endpoint-one",), ("missing-endpoint",), ("endpoint-one", "missing-endpoint")]
)
async def test_scoped_inventory_requires_every_requested_endpoint_before_registry_work(source_case, endpoint_ids):
    source_case.config.source_endpoint_ids = endpoint_ids
    if endpoint_ids == ("endpoint-one",):
        assert await sync._preparation_source_inventory(source_case.config, _source_reads(source_case), False) == (
            [source_case.accepted],
            [],
        )
    else:
        reason = "scope_changed" if endpoint_ids == ("missing-endpoint",) else "unavailable"
        with pytest.raises(ValueError, match=f"source_endpoint_{reason}$"):
            await sync._preparation_source_inventory(source_case.config, _source_reads(source_case), False)
    source_case.loader.transaction.assert_not_called()
    source_case.loader.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_reused_unlabeled_candidate_needs_no_label_reader_or_rebuild(monkeypatch, source_case):
    selected = _preparation(monkeypatch, source_case)
    sync._reusable_catalog_snapshot.return_value = selected
    monkeypatch.setattr(sync, "load_fhir_catalog_source_descriptors", AsyncMock())
    resumed, previous, accounting = await sync.prepare_fhir_catalog_snapshot(
        source_case.config, source_case.loader, None, source_case.sealer_pool, source_export=source_case.factory
    )
    assert resumed is selected and previous is None and accounting["resumed_snapshot"] is True
    sync.load_fhir_catalog_source_descriptors.assert_not_awaited()
    sync._build_source_catalog_candidate.assert_not_awaited()
    sync._seal_catalog_snapshot.assert_not_awaited()
    assert source_case.sealer_pool.acquire.call_count == 1
    source_case.export["replay"].assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("cleanup_fails", [False, True])
async def test_failed_seal_cleans_only_its_candidate_and_preserves_original_error(
    monkeypatch, source_case, cleanup_fails
):
    _preparation(monkeypatch, source_case)
    original = ValueError("independent seal refused")
    sync._seal_catalog_snapshot.side_effect = original
    if cleanup_fails:
        sync.discard_fhir_catalog_candidate.side_effect = PermissionError("cleanup refused")
    with pytest.raises(ValueError) as caught:
        await sync.prepare_fhir_catalog_snapshot(
            source_case.config, source_case.loader, None, source_case.sealer_pool, source_export=source_case.factory
        )
    assert caught.value is original
    target = sync._build_source_catalog_candidate.await_args.args[3]
    sync.discard_fhir_catalog_candidate.assert_awaited_once_with(
        source_case.loader,
        target,
        loader_role=source_case.config.loader_role,
        control_schema=source_case.config.grant.control_schema,
    )
    assert getattr(original, "__notes__", []) == (
        ["Unsealed catalog candidate cleanup failed: " + target.schema_name] if cleanup_fails else []
    )
    assert source_case.events[-1] == ("exit",)


@pytest.mark.asyncio
async def test_retained_labels_and_last_good_status_survive_committed_metadata_refresh(monkeypatch, source_case):
    from tests.test_registry_fhir_catalog_retained import descriptor_fixture

    retained, _, _ = await descriptor_fixture(monkeypatch)
    grant = replace(source_case.config.grant, retained_source_recipes=True)
    generation = sync.fhir_catalog_source_generation(retained.descriptor)
    labels = [source_company(source_id=retained.descriptor["coordinates"]["source_id"])]
    previous = snapshot(grant, (source_case.accepted.descriptor,), status="last_good")
    previous["source_vector_json"].append(
        {
            "source_generation": generation,
            "source_id": retained.descriptor["coordinates"]["source_id"],
            "status": "last_good",
            "source_companies": labels,
        }
    )
    previous["source_vector_json"].sort(key=lambda entry: (entry["source_id"], entry["source_generation"]))
    previous["metadata_generation"] = sync.metadata_generation(previous)
    descriptors_by_generation = {
        sync.fhir_catalog_source_generation(source_case.accepted.descriptor): source_case.accepted.descriptor,
        generation: retained.descriptor,
    }
    accepted_at = datetime(2025, 12, 1, tzinfo=timezone.utc)
    source_case.loader.fetch.return_value = [
        {"source_generation": key, "accepted_at": accepted_at} for key in descriptors_by_generation
    ]
    monkeypatch.setattr(sync, "load_fhir_catalog_source_descriptors", AsyncMock(return_value=descriptors_by_generation))
    committed = await sync._committed_catalog_sources(source_case.loader, grant, previous)
    assert committed == [replace(retained, accepted_at=accepted_at)]
    _mapping(monkeypatch)
    selected, current = await sync._catalog_snapshot_inputs(
        source_case.loader, grant, {"approved_revision": 6}, None, previous, committed
    )
    assert current == {generation: committed[0]}
    assert next(entry for entry in selected["source_vector_json"] if entry["source_generation"] == generation) == next(
        entry for entry in previous["source_vector_json"] if entry["source_generation"] == generation
    )
    assert (
        await sync._collect_catalog_source_companies(_source_reads(source_case), committed, previous, required=True)
        == {}
    )
    source_case.export["inventory"].assert_not_called()
    source_case.export["replay"].assert_not_awaited()


@pytest.mark.asyncio
async def test_scoped_source_export_still_requires_and_pins_its_native_reader(monkeypatch, source_case):
    from tests.test_registry_fhir_catalog_source_producer_grant import scoped_observed_descriptor
    from tests.test_registry_fhir_catalog_source_producer_sync import _scope

    selected, profile = scoped_observed_descriptor()
    grant = replace(source_case.config.grant, source_profiles=(profile,))
    reader = SimpleNamespace(execute=AsyncMock(), transaction=lambda **_options: _scope())
    pool = SimpleNamespace(acquire=lambda: _scope(reader))
    workspace = object()
    monkeypatch.setattr(sync, "fhir_catalog_reference_workspace", lambda actual: _scope(workspace))
    async with sync._catalog_source_reads(pool, source_case.sealer_pool, grant, source_case.factory) as reads:
        assert reads == (reader, workspace, {})
        assert sync._source_catalog_export(selected, reads[2]) is None
    source_case.factory.assert_not_called()
    source_case.sealer_pool.acquire.assert_not_called()
    with pytest.raises(ValueError, match="published_reader_required"):
        await sync._published_read(None, selected)
    with pytest.raises(ValueError, match="published_reader_required"):
        await sync._legacy_identity_inputs(reader, source_case.loader, selected, ("network-one",), '"catalog_control"')
    source_case.loader.fetch.assert_not_awaited()


@pytest.mark.asyncio
async def test_preparation_without_reader_or_export_refuses_before_inventory(source_case):
    with pytest.raises(ValueError, match="source_reader_required"):
        await sync.prepare_fhir_catalog_snapshot(source_case.config, source_case.loader, None, source_case.sealer_pool)
    source_case.loader.transaction.assert_not_called()
    source_case.sealer_pool.acquire.assert_not_called()


@pytest.mark.asyncio
async def test_pending_publication_does_not_report_a_completed_snapshot(monkeypatch, source_case):
    from process.registry_fhir_catalog_publication import FHIRCatalogPublicationRuntime
    from process.registry_publication_consumer import RegistryPublicationConsumerConfig

    source_case.config.consumer = RegistryPublicationConsumerConfig(
        "control_example", "lifecycle_example", "execution_example", "api_example", "queue_example", "owner_example"
    )
    request_id = "11111111-1111-4111-8111-111111111111"
    monkeypatch.setattr(
        sync,
        "enqueue_fhir_catalog_publication",
        AsyncMock(
            return_value={
                "request_id": request_id,
                "state": "pending",
            }
        ),
    )
    monkeypatch.setattr(sync, "consume_registry_publication_once", AsyncMock(return_value=None))
    runtime = FHIRCatalogPublicationRuntime(AsyncMock(), AsyncMock(), AsyncMock())
    with pytest.raises(ValueError, match="publication_in_progress"):
        await sync._publish_catalog_snapshot(
            source_case.config,
            source_case.loader,
            snapshot(source_case.config.grant),
            None,
            runtime,
            object(),
            object(),
        )
    consumer = sync.consume_registry_publication_once.await_args.kwargs["config"]
    assert str(consumer.only_request_id) == request_id and consumer.metadata_only is True
