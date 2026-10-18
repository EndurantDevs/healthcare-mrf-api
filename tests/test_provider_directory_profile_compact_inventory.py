# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Sparse compact plans retain complete normalized inputs and finalized coverage."""

import asyncio
import importlib
import re
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_profile as profile
from process import provider_directory_profile_compact_inventory as inventory

fhir = importlib.import_module("process.provider_directory_fhir")
_CHILD_BINDING = {
    "tables": {
        "provider_directory_dataset_resource": {"name": "closed_resource"},
        "provider_directory_dataset_affiliation_organization": {"name": "closed_edges"},
    }
}


def _directory(monkeypatch, buckets=(0, 399)):
    """Use the genuine projection compilers with controlled input and catalog responses."""
    database = SimpleNamespace(
        all=AsyncMock(return_value=[(bucket,) for bucket in buckets]),
        status=AsyncMock(),
        scalar=AsyncMock(return_value=True),
    )
    directory = SimpleNamespace(
        db=database,
        RESOURCE_MODELS_BY_TYPE=fhir.RESOURCE_MODELS_BY_TYPE,
        RESOURCE_TYPES_BY_MODEL=fhir.RESOURCE_TYPES_BY_MODEL,
        _qt=fhir._qt,
        _unscoped_qt=fhir._unscoped_qt,
        _provider_directory_artifact_resource_select_sql=fhir._provider_directory_artifact_resource_select_sql,
        _artifact_scope_resource_datasets=fhir._artifact_scope_resource_datasets,
        _provider_directory_artifact_source_select_sql=fhir._provider_directory_artifact_source_select_sql,
        _provider_directory_profile_build_ref=fhir._provider_directory_profile_build_ref,
        _PROVIDER_DIRECTORY_PROFILE_SELECTION_EXECUTION=SimpleNamespace(
            get=lambda: SimpleNamespace(attestation=SimpleNamespace(desired_profile_as_of="2026-01-02"))
        ),
    )
    for _name, model_name in inventory._ROOTS:
        setattr(directory, model_name, getattr(fhir, model_name))
    for model_name in (
        "ProviderDirectorySource",
        "ProviderDirectoryDatasetResource",
        "ProviderDirectoryDatasetAffiliationOrganization",
    ):
        setattr(directory, model_name, getattr(fhir, model_name))
    dataset = fhir.ProviderDirectoryArtifactDataset(
        source_id="synthetic",
        endpoint_id="endpoint",
        dataset_id="dataset",
        evidence_run_id="root",
        selected_resources=tuple(fhir.RESOURCE_MODELS_BY_TYPE),
    )
    fence = fhir.ProviderDirectoryArtifactDatasetFence((dataset,))
    materialization = SimpleNamespace(source_ids=["synthetic"], dataset_ids=["dataset"], serving_state=None)
    monkeypatch.setattr(inventory.source, "is_source_storage_required", lambda: True)
    monkeypatch.setattr(
        inventory.inputs,
        "binding",
        lambda _directory, _dataset: _CHILD_BINDING,
    )
    events = []

    @asynccontextmanager
    async def transaction(_directory):
        events.append("enter")
        try:
            yield
        finally:
            events.append("exit")

    monkeypatch.setattr(inventory.scope_copy, "input_transaction", transaction)
    monkeypatch.setattr(inventory.scope_copy, "prepared_read_transaction", transaction)
    return directory, fence, materialization, events


@pytest.mark.asyncio
async def test_census_uses_all_normalized_facts_and_closed_children_with_exact_parameters(monkeypatch):
    directory, fence, materialization, events = _directory(monkeypatch)
    ranges = await inventory.compact_ranges(
        directory, "synthetic", fence, materialization, has_existing_artifacts=False
    )
    assert ranges == (
        (profile.NPI_MIN, profile.NPI_MIN + profile.PROFILE_NPI_BATCH_SIZE),
        (2_995_000_000, 3_000_000_000),
    )
    statement = directory.db.all.await_args.args[0]
    parameters = directory.db.all.await_args.kwargs
    assert '"synthetic"."closed_resource"' in statement and '"synthetic"."closed_edges"' in statement
    assert 'FROM "synthetic"."provider_directory_dataset_resource"' not in statement
    assert "SELECT DISTINCT npi FROM normalized_facts" in statement
    assert "fact_type =" not in statement and "hashtextextended" not in statement
    assert "NULL::compact_" not in statement
    assert 'NULL::"synthetic"."provider_directory_practitioner"' in statement
    for _name, model_name in inventory._ROOTS:
        native_type = directory._qt("synthetic", getattr(directory, model_name).__tablename__)
        assert "jsonb_populate_record(NULL::" + native_type + ",to_jsonb(compact_projected))" in statement
    assert 'SELECT * FROM "synthetic"."closed_resource"' not in statement
    assert 'SELECT * FROM "synthetic"."closed_edges"' not in statement
    bound_names = set(re.findall(r"(?<!:):([a-z_][a-z_0-9]*)\b", statement))
    assert bound_names <= parameters.keys()
    for number, (_name, model_name) in enumerate(inventory._ROOTS):
        model = getattr(directory, model_name)
        assert parameters[f"m{number}_d0_resource_type"] == directory.RESOURCE_TYPES_BY_MODEL[model]
        assert parameters[f"m{number}_d0_evidence_run_ids"] == ["root"]
    assert parameters["source_ids"] == ["synthetic"] and parameters["dataset_ids"] == ["dataset"]
    assert parameters["profile_as_of"] == "2026-01-02"
    assert events == ["enter", "exit"]


@pytest.mark.asyncio
async def test_empty_resource_families_use_native_composite_types_without_payload_scans(monkeypatch):
    directory, fence, materialization, _events = _directory(monkeypatch, ())
    fence = replace(fence, datasets=(replace(fence.datasets[0], selected_resources=("Practitioner",)),))
    assert (
        await inventory.compact_ranges(directory, "synthetic", fence, materialization, has_existing_artifacts=False)
        == ()
    )
    statement = directory.db.all.await_args.args[0]
    assert 'SELECT (NULL::"synthetic"."provider_directory_practitioner_role").* WHERE FALSE' in statement


@pytest.mark.asyncio
async def test_incumbent_census_locks_exact_pair_and_includes_all_npis(monkeypatch):
    directory, fence, materialization, _events = _directory(monkeypatch, (2,))
    materialization.serving_state = SimpleNamespace(evidence_target_oid=11, profile_target_oid=12)
    directory._qt = lambda schema, name: f'"{schema}"."pinned_{name}"'
    expected_refs = [
        directory._qt("synthetic", name) for name in (profile.PROFILE_EVIDENCE_TABLE, profile.PROFILE_TABLE)
    ]
    directory.db.all.side_effect = [list(zip(expected_refs, (11, 12), strict=True)), [(399,)], [(2,)]]
    assert await inventory.compact_ranges(
        directory, "synthetic", fence, materialization, has_existing_artifacts=True
    ) == (inventory._range(2), inventory._range(399))
    directory.db.status.assert_awaited_once_with(
        "LOCK TABLE " + ",".join(expected_refs) + " IN ACCESS SHARE MODE NOWAIT"
    )
    statement = directory.db.all.await_args_list[1].args[0]
    for reference in expected_refs:
        assert "SELECT npi FROM " + reference in statement


@pytest.mark.asyncio
@pytest.mark.parametrize("observed", [[], [("wrong", 11), ("wrong", 12)]])
async def test_missing_or_replaced_incumbent_refuses_before_census(monkeypatch, observed):
    directory, fence, materialization, events = _directory(monkeypatch)
    materialization.serving_state = SimpleNamespace(evidence_target_oid=11, profile_target_oid=12)
    directory.db.all.return_value = observed
    with pytest.raises(RuntimeError, match="compact_inventory_changed"):
        await inventory.compact_ranges(directory, "synthetic", fence, materialization, has_existing_artifacts=True)
    assert directory.db.all.await_count == 1 and events == ["enter", "exit"]


@pytest.mark.asyncio
@pytest.mark.parametrize("buckets", [(True,), (1.0,), ("1",), (-1,), (400,), (0, 0), (1, 0), tuple(range(401))])
async def test_census_rejects_noncanonical_or_ambiguous_bucket_results(monkeypatch, buckets):
    directory, fence, materialization, _events = _directory(monkeypatch, buckets)
    with pytest.raises(RuntimeError, match="compact_inventory_changed"):
        await inventory.compact_ranges(directory, "synthetic", fence, materialization, has_existing_artifacts=False)


@pytest.mark.asyncio
@pytest.mark.parametrize("legacy", [False, True])
async def test_unproved_existing_or_legacy_inputs_preserve_full_grid_fallback(monkeypatch, legacy):
    directory, fence, materialization, events = _directory(monkeypatch)
    monkeypatch.setattr(inventory.source, "is_source_storage_required", lambda: not legacy)
    assert (
        await inventory.compact_ranges(directory, "synthetic", fence, materialization, has_existing_artifacts=True)
        is None
    )
    directory.db.all.assert_not_awaited()
    assert events == []


@pytest.mark.asyncio
async def test_census_cancellation_propagates_after_releasing_input_scope(monkeypatch):
    directory, fence, materialization, events = _directory(monkeypatch)
    directory.db.all.side_effect = asyncio.CancelledError
    with pytest.raises(asyncio.CancelledError):
        await inventory.compact_ranges(directory, "synthetic", fence, materialization, has_existing_artifacts=False)
    assert events == ["enter", "exit"]


@pytest.mark.asyncio
async def test_initial_without_proof_bound_date_preserves_full_grid_before_reading(monkeypatch):
    directory, fence, materialization, events = _directory(monkeypatch)
    directory._PROVIDER_DIRECTORY_PROFILE_SELECTION_EXECUTION.get = lambda: None
    assert (
        await inventory.compact_ranges(directory, "synthetic", fence, materialization, has_existing_artifacts=False)
        is None
    )
    assert events == []
    directory.db.all.assert_not_awaited()


@pytest.mark.asyncio
async def test_large_selection_chunks_complete_datasets_in_one_authenticated_transaction(monkeypatch):
    directory, fence, materialization, events = _directory(monkeypatch)
    datasets = tuple(
        replace(
            fence.datasets[0],
            source_id=f"source-{index}",
            dataset_id=f"dataset-{index}",
            evidence_run_id=f"root-{index}",
        )
        for index in range(65)
    )
    fence = replace(fence, datasets=datasets)
    materialization.source_ids = [dataset.source_id for dataset in datasets]
    materialization.dataset_ids = [dataset.dataset_id for dataset in datasets]
    directory.db.all.side_effect = [[(0,), (4,)], [(4,), (399,)]]
    observed = await inventory.compact_ranges(
        directory, "synthetic", fence, materialization, has_existing_artifacts=False
    )
    assert observed == tuple(inventory._range(bucket) for bucket in (0, 4, 399))
    assert events == ["enter", "exit"]
    assert directory.db.all.await_count == 2
    selected_pairs = []
    for call in directory.db.all.await_args_list:
        parameters = call.kwargs
        selected_pairs.extend(zip(parameters["source_ids"], parameters["dataset_ids"], strict=True))
        assert len(parameters) < 32_767
        assert len(parameters["source_ids"]) <= inventory._DATASETS_PER_QUERY
        assert set(re.findall(r"(?<!:):([a-z_][a-z_0-9]*)\b", call.args[0])) <= parameters.keys()
        assert parameters["m0_d0_evidence_run_ids"] == ["root-" + parameters["dataset_ids"][0].split("-")[-1]]
    assert selected_pairs == list(zip(materialization.source_ids, materialization.dataset_ids, strict=True))


@pytest.mark.asyncio
async def test_chunking_rejects_multiple_datasets_for_the_same_source(monkeypatch):
    directory, fence, materialization, events = _directory(monkeypatch)
    materialization.source_ids *= 2
    materialization.dataset_ids = ["dataset", "other-dataset"]
    with pytest.raises(RuntimeError, match="compact_inventory_changed"):
        await inventory.compact_ranges(directory, "synthetic", fence, materialization, has_existing_artifacts=False)
    assert events == []


def _build_and_plan(*, existing=True, buckets=(0,), mode="source_delta"):
    build = SimpleNamespace(
        schema="synthetic",
        evidence_stage="new_evidence",
        profile_stage="new_profile",
        affected_npi_stage="affected",
        materialization_mode=mode,
        source_ids=["synthetic"],
        removed_source_ids=["removed"],
    )
    batches = tuple(
        SimpleNamespace(kind="npi", npi_start=inventory._range(bucket)[0], npi_end=inventory._range(bucket)[1])
        for bucket in buckets
    )
    return build, SimpleNamespace(has_existing_artifacts=existing, compact_batches=batches)


@pytest.mark.asyncio
@pytest.mark.parametrize("complete", [True, False, None, 1])
async def test_final_coverage_checks_all_stages_and_incumbents_before_compaction(monkeypatch, complete):
    directory, _fence, _materialization, events = _directory(monkeypatch)
    build, plan = _build_and_plan()
    directory.db.scalar.return_value = complete
    if complete is True:
        await inventory.require_complete_inventory(directory, build, plan)
    else:
        with pytest.raises(RuntimeError, match="compact_inventory_changed"):
            await inventory.require_complete_inventory(directory, build, plan)
    statement = directory.db.scalar.await_args.args[0]
    for name in ("new_evidence", "new_profile", "affected", profile.PROFILE_EVIDENCE_TABLE, profile.PROFILE_TABLE):
        assert 'SELECT npi FROM "synthetic"."' + name + '"' in statement
    assert "NOT EXISTS" in statement and "=ANY(CAST(:compact_planned_buckets AS integer[]))" in statement
    assert directory.db.scalar.await_args.kwargs == {"compact_planned_buckets": [0]}
    assert events == ["enter", "exit"]


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["legacy", "full_grid", "no_op_delta"])
async def test_full_grid_legacy_and_noop_skip_coverage_query(monkeypatch, mode):
    directory, _fence, _materialization, _events = _directory(monkeypatch)
    build, plan = _build_and_plan(buckets=range(400) if mode == "full_grid" else ())
    if mode == "legacy":
        monkeypatch.setattr(inventory.source, "is_source_storage_required", lambda: False)
    if mode == "no_op_delta":
        build.source_ids = build.removed_source_ids = []
    await inventory.require_complete_inventory(directory, build, plan)
    directory.db.scalar.assert_not_awaited()


def test_inventory_renderer_and_sql_contracts_are_read_only_and_complete():
    contracts = inventory.sql_contracts()
    assert contracts == inventory.sql_contracts()
    assert "jsonb_populate_record" in contracts["compact_inventory_native_rows"]
    statement = contracts["compact_inventory_evidence"]
    assert "SELECT DISTINCT npi FROM normalized_facts" in statement
    assert all(word not in statement for word in ("INSERT INTO", "ON CONFLICT", "CREATE ", "UPDATE "))
    reference_by_name = {name + "_ref": "fixture_" + name for name, _model in inventory._ROOTS}
    reference_by_name["source_ref"] = "fixture_source"
    with pytest.raises(ValueError, match="complete evidence"):
        profile.profile_evidence_npi_inventory_sql(**reference_by_name, fact_type="name")
