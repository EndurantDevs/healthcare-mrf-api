# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Explicit named source coordinates remain usable after a current-pointer change."""

import importlib
from contextlib import asynccontextmanager
from dataclasses import asdict, replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_profile_selection_snapshot as snapshot
from process.provider_directory_cms_preparation import desired_fence_hash

fhir = importlib.import_module("process.provider_directory_fhir")


def _dataset(schema="directory_edition_" + "a" * 32):
    return fhir.ProviderDirectoryArtifactDataset(
        "source-a",
        "endpoint-a",
        "dataset-a",
        "root-a",
        storage_schema=schema,
    )


def test_named_resource_and_header_reads_keep_the_selected_schema(monkeypatch):
    dataset = _dataset()
    named_current = AsyncMock(side_effect=AssertionError("an issued input cannot resolve the latest pointer"))
    from process import provider_directory_named_edition

    monkeypatch.setattr(provider_directory_named_edition, "named_current_editions", named_current)
    statement = fhir._artifact_resource_batch_select_sql(fhir.ProviderDirectoryPractitionerRole, "mrf", dataset)
    assert f'"{dataset.storage_schema}"."provider_directory_dataset_resource"' in statement
    assert f'"{dataset.storage_schema}"."provider_directory_endpoint_dataset"' in statement
    assert '"mrf"."provider_directory_dataset_resource"' not in statement
    assert "LIMIT CAST(:scope_batch_size AS integer)" in statement
    assert "resource.resource_id" in statement
    named_current.assert_not_called()


def test_named_schema_is_part_of_the_frozen_batch_and_dataset_lineage():
    dataset = _dataset()
    batch = fhir._ProviderDirectoryArtifactScopeBatchProjection(
        1,
        dataset.source_id,
        dataset.dataset_id,
        dataset.evidence_run_id,
        "PractitionerRole",
        None,
        "role-1",
        1,
        123,
        storage_schema=dataset.storage_schema,
    )
    assert batch.canonical_payload()["storage_schema"] == dataset.storage_schema
    state = SimpleNamespace(expected_batch_number=1, after_resource_id=None)
    fhir._validate_artifact_resource_batch_coordinate(batch, state, dataset, "PractitionerRole")
    with pytest.raises(RuntimeError, match="scope_batch_projection_invalid"):
        fhir._validate_artifact_resource_batch_coordinate(
            batch,
            state,
            replace(dataset, storage_schema="other_edition"),
            "PractitionerRole",
        )
    fence = fhir.ProviderDirectoryArtifactDatasetFence((dataset,))
    other = replace(fence, datasets=(replace(dataset, storage_schema="other_edition"),))
    assert desired_fence_hash(fence) != desired_fence_hash(other)
    legacy = replace(dataset, storage_schema=None)
    original_by_field = {key: field_value for key, field_value in asdict(legacy).items() if key != "storage_schema"}
    assert fhir._canonical_profile_datasets((legacy,)) == [original_by_field]
    assert "storage_schema" not in replace(batch, storage_schema=None).canonical_payload()


def test_profile_input_attests_the_named_input_schema():
    dataset = _dataset()
    result = snapshot._profile_input(
        {"source_id": "source-a", "dataset_id": "dataset-a"},
        {"storage_schema": dataset.storage_schema, "resource_count": 3},
        {},
    )
    assert result["storage_schema"] == dataset.storage_schema
    legacy = snapshot._profile_input({"source_id": "source-a", "dataset_id": "dataset-a"}, {}, {})
    assert "storage_schema" not in legacy


def test_named_fence_is_read_only_and_uses_saved_schema():
    statement = fhir._artifact_fence_dataset_rows_sql(
        for_update=True,
        dataset_schema="published_edition",
        lock_rows=False,
    )
    assert '"published_edition"."provider_directory_endpoint_dataset"' in statement
    assert "FOR UPDATE" not in statement
    assert "dataset.dataset_id = ANY" in statement


@pytest.mark.asyncio
@pytest.mark.parametrize("binding", ["standalone", "reader", "transaction"])
@pytest.mark.parametrize("isolation", ["repeatable read", "serializable", "read committed"])
async def test_profile_selection_snapshot_reuses_existing_transactions(monkeypatch, binding, isolation):
    session = object()
    entries = []

    @asynccontextmanager
    async def transaction():
        entries.append("transaction")
        yield session

    @asynccontextmanager
    async def existing_session():
        entries.append("session")
        yield session

    status = AsyncMock()
    scalar = AsyncMock(return_value=isolation)
    monkeypatch.setattr(snapshot.db, "has_reader_session", lambda: binding == "reader")
    monkeypatch.setattr(
        snapshot.db,
        "_transaction_binding",
        lambda: SimpleNamespace(session=session) if binding == "transaction" else None,
    )
    monkeypatch.setattr(snapshot.db, "transaction", transaction)
    monkeypatch.setattr(snapshot.db, "session", existing_session)
    monkeypatch.setattr(snapshot.db, "status", status)
    monkeypatch.setattr(snapshot.db, "scalar", scalar)
    async with snapshot.profile_selection_snapshot() as selected:
        assert selected is session
    assert entries == ({"standalone": ["transaction"], "reader": ["session"], "transaction": []}[binding])
    assert status.await_count == int(binding == "standalone")
    scalar.assert_not_awaited()


def _named_serving_input(damage):
    """Build published semantic metadata and vary only the requested invalid field."""
    from copy import deepcopy

    from tests.test_provider_directory_source_summary_backfill import _dataset as summary_dataset
    from tests.test_provider_directory_source_summary_backfill import (
        _publication_metadata_with_summary,
        _relation_proofs,
        _semantic_metadata_record,
    )

    dataset = replace(summary_dataset(), storage_schema="published_edition")
    metadata = deepcopy(_semantic_metadata_record()["publication_metadata_json"])
    metadata.update(_publication_metadata_with_summary())
    proof_by_name = _relation_proofs()
    for relation_proof in proof_by_name.values():
        relation_proof.update(version=1, acquisition_root_run_id=dataset.evidence_run_id)
    metadata.update(proof_by_name)
    metadata_key = fhir.PROVIDER_DIRECTORY_DATASET_NETWORK_PLAN_METADATA_KEY
    match damage:
        case "root":
            metadata[metadata_key]["acquisition_root_run_id"] = "other-root"
        case "incomplete":
            metadata[metadata_key]["complete"] = False
        case "negative":
            metadata[metadata_key]["edge_count"] = -1
        case "missing":
            metadata.pop(metadata_key)
        case "summary":
            metadata[fhir.SOURCE_SUMMARY_METADATA_KEY]["summary_sha256"] = "f" * 64
        case "outcome":
            metadata[fhir.PROVIDER_DIRECTORY_OUTCOME_RESOURCE_COUNTS_METADATA_KEY]["resource_count"] += 1
    return dataset, metadata, proof_by_name


def _install_named_serving_read(monkeypatch, dataset, metadata):
    """Allow exact header reads while refusing all writes and payload rebuilds."""

    @asynccontextmanager
    async def transaction():
        yield

    async def first(query, **parameters):
        assert 'FROM "published_edition"."provider_directory_endpoint_dataset"' in query
        assert parameters["dataset_id"] == dataset.dataset_id
        return {"acquisition_root_run_id": dataset.evidence_run_id, "publication_metadata_json": metadata}

    monkeypatch.setattr(fhir.db, "transaction", transaction)
    monkeypatch.setattr(fhir.db, "first", first)
    monkeypatch.setattr(
        fhir.db, "status", AsyncMock(side_effect=AssertionError("published input must remain read-only"))
    )
    verifier = AsyncMock()
    monkeypatch.setattr(fhir, "_lock_and_verify_artifact_dataset_fence", verifier)
    for collaborator_name in ("_rebuild_one_current_dataset_relation_set", "_endpoint_dataset_content_proof"):
        monkeypatch.setattr(
            fhir, collaborator_name, AsyncMock(side_effect=AssertionError("immutable input must not be rebuilt"))
        )
    return verifier


@pytest.mark.asyncio
@pytest.mark.parametrize("targets", [(True, True), (True, False), (False, True)])
@pytest.mark.parametrize("damage", [None, "root", "incomplete", "negative", "missing", "summary", "outcome"])
async def test_named_serving_preparation_reuses_immutable_proofs_without_writes(monkeypatch, targets, damage):
    """Publish selected relation proofs without any mutation of the immutable input."""
    dataset, metadata, proof_by_name = _named_serving_input(damage)
    verifier = _install_named_serving_read(monkeypatch, dataset, metadata)
    fence = fhir.ProviderDirectoryArtifactDatasetFence((dataset,))
    arguments_by_name = dict(
        build_run_id="later-build",
        should_rebuild_network_plan=targets[0],
        should_rebuild_affiliation_organization=targets[1],
        should_rebuild_source_summary=all(targets),
    )
    if damage:
        with pytest.raises(fhir.ProviderDirectoryArtifactBuildStale, match="(named_|source_summary_invalid)"):
            await fhir._rebuild_one_current_dataset_artifacts(dataset, fence, **arguments_by_name)
    else:
        relation_by_name, summary = await fhir._rebuild_one_current_dataset_artifacts(
            dataset, fence, **arguments_by_name
        )
        assert relation_by_name == {
            metadata_key: relation_proof
            for (metadata_key, relation_proof), enabled in zip(proof_by_name.items(), targets, strict=True)
            if enabled
        }
        assert summary == (metadata[fhir.SOURCE_SUMMARY_METADATA_KEY] if all(targets) else None)
    verifier.assert_awaited_once_with(fence, fhir.db)


@pytest.mark.asyncio
@pytest.mark.parametrize("has_named_input", [False, True])
@pytest.mark.parametrize("has_selection_locks", [False, True])
@pytest.mark.parametrize("isolation", ["repeatable read", "serializable", "read committed"])
async def test_profile_dataset_reads_keep_existing_locks_or_repeatable_snapshot(
    monkeypatch, has_named_input, has_selection_locks, isolation
):
    from process import provider_directory_named_edition as editions

    @asynccontextmanager
    async def existing_session():
        yield object()

    edition = SimpleNamespace(
        catalog_schema="mrf", endpoint_id="endpoint-a", dataset_id="dataset-a", schema_name="published_edition"
    )
    reader = AsyncMock(return_value=(edition,) if has_named_input else ())
    dataset_rows = AsyncMock(return_value=[{"dataset_id": "dataset-a"}])
    monkeypatch.setattr(snapshot, "_schema", lambda: "mrf")
    monkeypatch.setattr(snapshot.db, "has_reader_session", lambda: False)
    monkeypatch.setattr(snapshot.db, "_transaction_binding", lambda: SimpleNamespace(session=object()))
    monkeypatch.setattr(snapshot.db, "session", existing_session)
    monkeypatch.setattr(snapshot.db, "scalar", AsyncMock(return_value=isolation))
    monkeypatch.setattr(editions, "named_editions_for_session", reader)
    monkeypatch.setattr(snapshot, "load_profile_selection_dataset_rows", dataset_rows)
    if has_named_input and not has_selection_locks and isolation == "read committed":
        with pytest.raises(RuntimeError, match="selection_snapshot_required"):
            await snapshot._selection_dataset_rows(has_selection_locks=has_selection_locks)
        dataset_rows.assert_not_awaited()
    else:
        expected_row_by_field = {"dataset_id": "dataset-a"}
        if has_named_input:
            expected_row_by_field["storage_schema"] = "published_edition"
        assert await snapshot._selection_dataset_rows(has_selection_locks=has_selection_locks) == [
            expected_row_by_field
        ]
        relation = dataset_rows.await_args.kwargs["endpoint_dataset_ref"]
        assert ('"published_edition".' in relation) is has_named_input
    reader.assert_awaited_once()


def test_virtual_overlay_projects_each_selected_named_and_legacy_dataset():
    from process import provider_directory_cms_overlay_projection as overlay

    named = replace(_dataset(), selected_resources=("PractitionerRole",))
    legacy = replace(named, source_id="legacy-source", dataset_id="legacy-dataset", storage_schema=None)
    fence = fhir.ProviderDirectoryArtifactDatasetFence((named, legacy))
    _name, statement = overlay._resource_cte(fhir, "mrf", fence, "PractitionerRole")
    assert f'"{named.storage_schema}"."provider_directory_dataset_resource"' in statement
    assert f'"{named.storage_schema}"."provider_directory_endpoint_dataset"' in statement
    assert '"mrf"."provider_directory_dataset_resource"' in statement
    assert "UNION ALL" in statement
    assert "legacy-dataset" in statement and named.dataset_id in statement


@pytest.mark.asyncio
async def test_ordinary_overlay_pages_use_the_issued_named_dataset(monkeypatch):
    from process import provider_directory_generic_source_artifacts as artifacts

    dataset = _dataset()
    selected = SimpleNamespace(_mapping={"resource_id": "role-1"})
    queries = AsyncMock(side_effect=[[selected], []])
    monkeypatch.setattr(fhir.db, "all", queries)
    monkeypatch.setattr(
        artifacts,
        "_loading_projection",
        lambda _fhir, _root, sql, params, _enrichment: (
            sql,
            sql,
            params,
            None,
            {},
        ),
    )

    async def pages(_db, sql, _params, _model):
        assert f'"{dataset.storage_schema}"."provider_directory_dataset_resource"' in sql
        yield [{"resource_id": "role-1"}]

    monkeypatch.setattr(artifacts, "projected_pages", pages)
    assert [
        page
        async for page in artifacts._resource_pages(
            fhir,
            fhir.ProviderDirectoryPractitionerRole,
            dataset,
            {},
        )
    ] == [[{"resource_id": "role-1"}]]
    assert queries.await_args_list[1].kwargs["after_resource_id"] == "role-1"


def test_legacy_header_input_preserves_existing_relation_routing(monkeypatch):
    monkeypatch.setattr(fhir, "_qt", lambda schema, root: "issued_" + schema + "." + root)
    assert fhir._artifact_resource_header_relation("mrf", None) == "issued_mrf.provider_directory_endpoint_dataset"
    dataset = _dataset()
    assert fhir._artifact_resource_header_relation("mrf", dataset) == (
        f'"{dataset.storage_schema}"."provider_directory_endpoint_dataset"'
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("has_named_input", [False, True])
async def test_artifact_selection_preserves_legacy_read_committed_only_without_named_inputs(
    monkeypatch, has_named_input
):
    from process import provider_directory_named_edition as editions

    @asynccontextmanager
    async def transaction():
        yield object()

    database = SimpleNamespace(
        has_reader_session=lambda: False,
        _transaction_binding=lambda: object(),
        transaction=transaction,
        scalar=AsyncMock(return_value="read committed"),
        status=AsyncMock(),
    )
    directory = SimpleNamespace(db=database, _schema=lambda: "catalog")
    probe = AsyncMock(return_value=has_named_input)
    reader = AsyncMock(side_effect=AssertionError("headers cannot be read outside the repeatable snapshot"))
    monkeypatch.setattr(editions, "has_named_artifact_inputs", probe)
    monkeypatch.setattr(editions, "named_editions_for_session", reader)
    if has_named_input:
        with pytest.raises(RuntimeError, match="artifact_selection_snapshot_required"):
            async with editions.artifact_selection_snapshot(directory, ["source"]):
                pytest.fail("selected named headers require the repeatable snapshot")
    else:
        async with editions.artifact_selection_snapshot(directory, ["source"]) as selected:
            assert selected == ()
    probe.assert_awaited_once_with(directory, ["source"])
    reader.assert_not_awaited()
    database.status.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("pointer", [None, {"dataset_id": "successor", "schema_name": "successor_schema"}])
async def test_named_new_publication_fence_checks_pointer_while_old_data_stays_readable(monkeypatch, pointer):
    from process import provider_directory_named_edition as editions

    dataset = _dataset()
    executor = SimpleNamespace(
        all=AsyncMock(
            return_value=[
                {
                    "dataset_id": dataset.dataset_id,
                    "endpoint_id": dataset.endpoint_id,
                    "locked_current_dataset_ids": [dataset.dataset_id],
                }
            ]
        ),
        first=AsyncMock(return_value=pointer),
    )
    rows = await editions.artifact_fence_dataset_rows(
        fhir,
        executor,
        (dataset,),
        {},
        for_update=True,
    )
    assert rows[0]["locked_current_dataset_ids"] == []
    with pytest.raises(fhir.ProviderDirectoryArtifactBuildStale, match="current_changed"):
        fhir._assert_locked_artifact_fence_datasets(fhir.ProviderDirectoryArtifactDatasetFence((dataset,)), rows)
    statement = fhir._artifact_resource_batch_select_sql(fhir.ProviderDirectoryPractitionerRole, "mrf", dataset)
    assert f'"{dataset.storage_schema}"."provider_directory_dataset_resource"' in statement
    assert "current_edition" not in statement


def test_named_payload_relation_has_one_arm_per_logical_dataset():
    original = _dataset()
    alias = replace(original, source_id="another-alias")
    successor = replace(original, dataset_id="other-dataset")
    model = fhir.ProviderDirectoryDatasetResource
    once = fhir._artifact_payload_relation("mrf", model, (original,))
    assert fhir._artifact_payload_relation("mrf", model, (original, alias)) == once
    assert fhir._artifact_payload_relation("mrf", model, (alias, original)) == once
    mixed = fhir._artifact_payload_relation("mrf", model, (original, alias, successor))
    assert mixed.count('FROM "' + original.storage_schema + '"."provider_directory_dataset_resource"') == 2
