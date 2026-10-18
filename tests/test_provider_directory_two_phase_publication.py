# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import contextlib
import hashlib
import json
from dataclasses import replace

import pytest

from db.connection import Database
from process.provider_directory_generic_source_adapters import persist_source_rows
from process.provider_directory_generic_source_policy import source_transaction
from tests import provider_directory_semantic_proof_v3_postgres_support as semantic
from tests.provider_directory_dataset_artifact_pg_support import (
    seal_validated_dataset,
)
from tests.provider_directory_source_postgres_support import source_component_templates
from tests.test_provider_directory_artifact_cutover import (
    _artifact_relation_value,
    _assert_artifact_relation_values,
    _create_artifact_bundle_relations,
    _fail_stage_indexes,
    _keep_stage_indexes,
    _prepared_bundle_stage,
    importer,
)
from tests.test_provider_directory_dataset_artifact_db import (
    _dataset_database,
    _insert_validated_shared_dataset,
)


@contextlib.contextmanager
def _active_dataset_fence(dataset_fence):
    fence_token = importer._PROVIDER_DIRECTORY_ARTIFACT_DATASET_FENCE.set(dataset_fence)
    try:
        yield
    finally:
        importer._PROVIDER_DIRECTORY_ARTIFACT_DATASET_FENCE.reset(fence_token)


def _second_candidate_metadata() -> str:
    return json.dumps(
        {
            "acquisition_root_run_id": "root-z-candidate",
            "selected_resources": ["Location"],
            "expected_resources": ["Location"],
            "source_ids": ["source_z"],
            "resource_diagnostics": {
                "Location": {
                    "complete": True,
                    "bounded": False,
                    "error": None,
                    "next_url_remaining": False,
                }
            },
        }
    )


async def _insert_second_endpoint_identity(
    database,
    schema: str,
    candidate_metadata: str,
) -> None:
    source_metadata = json.dumps(
        {
            "provider_directory_supported_resources": ["Location"],
            "provider_directory_fully_enumerable_resources": ["Location"],
        }
    )
    await database.status(f"INSERT INTO {schema}.provider_directory_api_endpoint (endpoint_id) VALUES ('endpoint_z');")
    await database.status(
        f"INSERT INTO {schema}.provider_directory_source ("
        "source_id, org_name, endpoint_id, requires_registration, "
        "requires_api_key, metadata_json"
        ") VALUES ('source_z', 'Source Z', 'endpoint_z', false, false, "
        "CAST(:metadata AS json));",
        metadata=source_metadata,
    )
    await database.status(
        f"INSERT INTO {schema}.provider_directory_endpoint_dataset ("
        "dataset_id, endpoint_id, import_run_id, acquisition_root_run_id, "
        "status, is_current, published_at, publication_metadata_json"
        ") VALUES ("
        "'dataset_z_current', 'endpoint_z', 'run-z-current', "
        "'root-z-current', :published_status, true, now(), "
        "CAST(:current_metadata AS json)), ("
        "'dataset_z_candidate', 'endpoint_z', 'run-z-candidate', "
        "'root-z-candidate', :validated_status, false, NULL, "
        "CAST(:candidate_metadata AS json));",
        published_status=importer.ENDPOINT_DATASET_PUBLISHED,
        validated_status=importer.ENDPOINT_DATASET_VALIDATED,
        current_metadata=json.dumps({"selected_resources": ["Location"]}),
        candidate_metadata=candidate_metadata,
    )


async def _insert_second_validated_endpoint(database, schema: str) -> None:
    candidate_metadata = _second_candidate_metadata()
    await _insert_second_endpoint_identity(
        database,
        schema,
        candidate_metadata,
    )
    await database.status(
        f"UPDATE {schema}.provider_directory_endpoint_dataset "
        "SET previous_dataset_id = 'dataset_z_current', "
        "dataset_hash = :dataset_hash, resource_count = 1, "
        "validated_at = now() "
        "WHERE dataset_id = 'dataset_z_candidate';",
        dataset_hash="9" * 64,
    )
    await database.status(
        f"INSERT INTO {schema}.provider_directory_dataset_resource ("
        "dataset_id, resource_type, resource_id, payload_hash, payload_json"
        ") VALUES ("
        "'dataset_z_candidate', 'Location', 'location-z', :payload_hash, "
        "CAST(:payload AS json));",
        payload_hash="8" * 64,
        payload=json.dumps({"status": "active", "name": "Clinic Z"}),
    )
    await seal_validated_dataset(database, schema, "dataset_z_candidate")


async def _artifact_bundle_stages(
    schema: str,
    *,
    should_fail_second: bool = False,
):
    second_index_handler = _fail_stage_indexes if should_fail_second else _keep_stage_indexes
    return (
        await _prepared_bundle_stage(
            schema,
            "artifact_stage_a",
            "artifact_target_a",
            _keep_stage_indexes,
        ),
        await _prepared_bundle_stage(
            schema,
            "artifact_stage_b",
            "artifact_target_b",
            second_index_handler,
        ),
    )


async def _selected_dataset_states(database, schema: str):
    dataset_state_rows = await database.all(
        f"SELECT dataset_id, status, is_current "
        f"FROM {schema}.provider_directory_endpoint_dataset "
        "WHERE dataset_id IN ('dataset_shared', 'dataset_candidate') "
        "ORDER BY dataset_id;"
    )
    return [tuple(dataset_state_row) for dataset_state_row in dataset_state_rows]


async def _published_dataset_pointers(database, schema: str):
    pointer_rows = await database.all(
        f"SELECT endpoint_id, dataset_id "
        f"FROM {schema}.provider_directory_endpoint_dataset "
        "WHERE endpoint_id IN ('endpoint_shared', 'endpoint_z') "
        "AND status = :published_status AND is_current = true "
        "ORDER BY endpoint_id;",
        published_status=importer.ENDPOINT_DATASET_PUBLISHED,
    )
    return [tuple(pointer_row) for pointer_row in pointer_rows]


def _install_pointer_pause(monkeypatch):
    promotion_entered = importer.asyncio.Event()
    release_promotion = importer.asyncio.Event()
    promote_datasets = importer._promote_provider_directory_artifact_datasets

    async def pause_before_pointer_updates(dataset_fence):
        promotion_entered.set()
        await release_promotion.wait()
        await promote_datasets(dataset_fence)

    monkeypatch.setattr(
        importer,
        "_promote_provider_directory_artifact_datasets",
        pause_before_pointer_updates,
    )
    return promotion_entered, release_promotion


@contextlib.asynccontextmanager
async def _publication_observer():
    observer = Database()
    await observer.connect()
    try:
        yield observer
    finally:
        await observer.disconnect()


async def _settle_publication_tasks(
    release_promotion,
    promotion_task,
    relation_reader_task,
) -> None:
    release_promotion.set()
    if not promotion_task.done():
        await promotion_task
    if relation_reader_task is not None and not relation_reader_task.done():
        await relation_reader_task


@pytest.mark.asyncio
async def test_full_scope_uses_candidate_while_targeted_uses_current(monkeypatch):
    async with _dataset_database(monkeypatch) as (database, schema):
        await _insert_validated_shared_dataset(database, schema)
        full_fence = await importer._resolve_provider_directory_artifact_datasets(
            ["source_primary"],
            should_select_validated_candidates=True,
        )
        targeted_fence = await importer._resolve_provider_directory_artifact_datasets(
            ["source_primary"],
            should_select_validated_candidates=False,
        )
        assert full_fence.dataset_id_by_endpoint_id == {"endpoint_shared": "dataset_candidate"}
        assert [dataset.dataset_id for dataset in full_fence.promotion_datasets] == ["dataset_candidate"]
        assert targeted_fence.dataset_id_by_endpoint_id == {"endpoint_shared": "dataset_shared"}
        assert targeted_fence.promotion_datasets == []
        async with importer._provider_directory_artifact_dataset_scope(
            run_id="artifact-candidate",
            source_ids=["source_primary"],
            fence=full_fence,
            resource_types=frozenset({"Location"}),
        ):
            location_scope = importer._PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES.get()[
                "provider_directory_location"
            ]
            scoped_names = await database.all(
                f"SELECT source_id, name FROM {schema}.{location_scope} ORDER BY source_id;"
            )
            assert [tuple(scope_name_row) for scope_name_row in scoped_names] == [
                ("source_primary", "Candidate Clinic"),
            ]


@pytest.mark.asyncio
async def test_full_selection_rejects_multiple_validated_candidates(monkeypatch):
    async with _dataset_database(monkeypatch) as (database, schema):
        await _insert_validated_shared_dataset(database, schema)
        await _insert_validated_shared_dataset(
            database,
            schema,
            dataset_id="dataset_candidate_b",
            root_run_id="root-candidate-b",
        )
        with pytest.raises(
            RuntimeError,
            match="provider_directory_artifact_validated_candidate_ambiguous",
        ):
            await importer._resolve_provider_directory_artifact_datasets(
                ["source_primary"],
                should_select_validated_candidates=True,
            )


@pytest.mark.asyncio
async def test_explicit_selection_rejects_same_endpoint_unrelated_candidate(
    monkeypatch,
):
    async with _dataset_database(monkeypatch) as (database, schema):
        await _insert_validated_shared_dataset(database, schema, seal=False)
        await database.status(
            f"UPDATE {schema}.provider_directory_endpoint_dataset "
            "SET publication_metadata_json = CAST(jsonb_set("
            "publication_metadata_json::jsonb, '{source_ids}', "
            "'[\"source_sibling\"]'::jsonb) AS json) "
            "WHERE dataset_id = 'dataset_candidate';"
        )

        fence = await importer._resolve_provider_directory_artifact_datasets(
            ["source_primary"],
            should_select_validated_candidates=True,
        )
        assert fence.dataset_id_by_endpoint_id == {"endpoint_shared": "dataset_shared"}
        assert fence.promotion_datasets == []


@pytest.mark.asyncio
async def test_candidate_fence_rejects_root_change(monkeypatch):
    async with _dataset_database(monkeypatch) as (database, schema):
        await _insert_validated_shared_dataset(database, schema)
        dataset_fence = await importer._resolve_provider_directory_artifact_datasets(
            ["source_primary"],
            should_select_validated_candidates=True,
        )
        await database.status(
            f"UPDATE {schema}.provider_directory_endpoint_dataset "
            "SET acquisition_root_run_id = 'root-changed' "
            "WHERE dataset_id = 'dataset_candidate';"
        )
        with pytest.raises(
            importer.ProviderDirectoryArtifactBuildStale,
            match="provider_directory_endpoint_dataset_metadata_changed",
        ):
            async with database.transaction():
                await importer._lock_and_verify_artifact_dataset_fence(
                    dataset_fence,
                    database,
                )


@pytest.mark.asyncio
async def test_bundle_failure_rolls_back_relations_and_pointers(monkeypatch):
    async with _dataset_database(monkeypatch) as (database, schema):
        await _insert_validated_shared_dataset(database, schema)
        dataset_fence = await importer._resolve_provider_directory_artifact_datasets(
            ["source_primary"],
            should_select_validated_candidates=True,
        )
        await _create_artifact_bundle_relations(database, schema)
        failing_stages = await _artifact_bundle_stages(
            schema,
            should_fail_second=True,
        )
        with _active_dataset_fence(dataset_fence):
            with pytest.raises(RuntimeError, match="second target failed"):
                await importer._promote_provider_directory_artifact_bundle_transaction(failing_stages)
            assert await _selected_dataset_states(database, schema) == [
                ("dataset_candidate", importer.ENDPOINT_DATASET_VALIDATED, False),
                ("dataset_shared", importer.ENDPOINT_DATASET_PUBLISHED, True),
            ]
            await _assert_artifact_relation_values(
                database,
                schema,
                {
                    "artifact_target_a": "old-a",
                    "artifact_target_b": "old-b",
                    "artifact_stage_a": "new-a",
                    "artifact_stage_b": "new-b",
                },
            )
            successful_stages = await _artifact_bundle_stages(schema)
            await importer._promote_provider_directory_artifact_bundle_transaction(successful_stages)
        await _assert_artifact_relation_values(
            database,
            schema,
            {"artifact_target_a": "new-a", "artifact_target_b": "new-b"},
        )
        assert await _selected_dataset_states(database, schema) == [
            ("dataset_candidate", importer.ENDPOINT_DATASET_PUBLISHED, True),
            ("dataset_shared", importer.ENDPOINT_DATASET_SUPERSEDED, False),
        ]


@pytest.mark.asyncio
async def test_multi_endpoint_reader_sees_atomic_bundle_and_pointers(monkeypatch):
    async with _dataset_database(monkeypatch) as (database, schema):
        await _insert_validated_shared_dataset(database, schema)
        await _insert_second_validated_endpoint(database, schema)
        await _create_artifact_bundle_relations(database, schema)
        dataset_fence = await importer._resolve_provider_directory_artifact_datasets(
            ["source_primary", "source_z"],
            should_select_validated_candidates=True,
        )
        assert [dataset.endpoint_id for dataset in dataset_fence.promotion_datasets] == [
            "endpoint_shared",
            "endpoint_z",
        ]
        artifact_stages = await _artifact_bundle_stages(schema)
        promotion_entered, release_promotion = _install_pointer_pause(monkeypatch)
        async with _publication_observer() as observer:
            with _active_dataset_fence(dataset_fence):
                assert await _artifact_relation_value(observer, schema, "artifact_target_a") == "old-a"
                promotion_task = importer.asyncio.create_task(
                    importer._promote_provider_directory_artifact_bundle_transaction(artifact_stages)
                )
                relation_reader_task = None
                try:
                    await promotion_entered.wait()
                    assert await _published_dataset_pointers(observer, schema) == [
                        ("endpoint_shared", "dataset_shared"),
                        ("endpoint_z", "dataset_z_current"),
                    ]
                    relation_reader_task = importer.asyncio.create_task(
                        _artifact_relation_value(observer, schema, "artifact_target_a")
                    )
                    await importer.asyncio.sleep(0.05)
                    assert not relation_reader_task.done()
                    release_promotion.set()
                    await promotion_task
                    assert await relation_reader_task == "new-a"
                    assert await _published_dataset_pointers(observer, schema) == [
                        ("endpoint_shared", "dataset_candidate"),
                        ("endpoint_z", "dataset_z_candidate"),
                    ]
                finally:
                    await _settle_publication_tasks(
                        release_promotion,
                        promotion_task,
                        relation_reader_task,
                    )


async def _retry_source_binding(database, schema, dataset_id):
    async with source_transaction(database):
        binding = await database.scalar(
            f"SELECT candidate_storage_json FROM {schema}.provider_directory_endpoint_dataset "
            "WHERE dataset_id=:dataset_id",
            dataset_id=dataset_id,
        )
    return json.loads(binding) if isinstance(binding, str) else binding


async def _publish_retry_dataset(database, schema, dataset_id):
    """Use genuine acquisition, checkpoint, proof and immutable Source attachment."""
    await semantic._insert_parent(database, schema, dataset_id)
    candidate = database.semantic_candidates[dataset_id]
    context = importer.PaginationCheckpointContext(
        canonical_api_base=semantic.CANONICAL_BASE,
        source_scope_hash=hashlib.sha256(dataset_id.encode()).hexdigest(),
        source_ids=(semantic.SOURCE_ID,),
        owner_run_id=candidate.import_run_id,
        acquisition_root_run_id=candidate.acquisition_root_run_id,
        endpoint_id=candidate.endpoint_id,
        dataset_id=dataset_id,
    )
    await importer._load_or_initialize_pagination_checkpoint(
        context, "Practitioner", semantic.CANONICAL_BASE + "/Practitioner"
    )
    await semantic._write_page(dataset_id, semantic._observation("Alex Example", page_number=1))
    await persist_source_rows(
        importer,
        [
            dict(
                dataset_id=dataset_id,
                resource_type="LU:Practitioner:pass:1",
                resource_id="pass-row",
                payload_hash=hashlib.sha256(b"{}").hexdigest(),
                payload_json={},
            )
        ],
        resource_hash_contract=None,
        semantic_projection_as_of=None,
    )
    binding = await _retry_source_binding(database, schema, dataset_id)
    inputs = [binding["tables"][key]["name"] for key in ("resource_input", "witness_input")]
    async with source_transaction(database):
        assert (
            await database.scalar(
                f"SELECT count(*) FROM {schema}.{inputs[0]} WHERE resource_type='LU:Practitioner:pass:1'"
            )
            == 1
        )
    await semantic._publish_semantic_source(database, dataset_id)
    assert (await _retry_source_binding(database, schema, dataset_id))["phase"] == "published"
    async with source_transaction(database):
        assert all(
            [await database.scalar("SELECT to_regclass(:name)", name=f"{schema}.{name}") is None for name in inputs]
        )
        selected = await importer._resolve_provider_directory_artifact_datasets(
            [semantic.SOURCE_ID],
            should_select_validated_candidates=False,
        )
    return selected.datasets[0]


@contextlib.asynccontextmanager
async def _published_retry_database(monkeypatch, *, peer=False):
    """Each cleanup case starts with real published payload and owned retry cursors."""
    async with source_component_templates(), semantic._semantic_database(monkeypatch) as (database, schema):
        datasets = []
        if peer:
            datasets.append(await _publish_retry_dataset(database, schema, "dataset_z_current"))
        candidate = await _publish_retry_dataset(database, schema, "dataset_candidate")
        database.retry_publication_fence = importer.ProviderDirectoryArtifactDatasetFence(
            (replace(candidate, promote_on_cutover=True), *datasets),
            should_select_validated_candidates=True,
        )
        database.retry_payloads_before = await _retry_payloads(database, schema)
        assert len(database.retry_payloads_before) == 1 + int(peer)
        assert await _retry_cleanup_counts(database, schema) == (0, 1)
        yield database, schema


async def _retry_payloads(database, schema):
    async with source_transaction(database):
        rows = await database.all(
            f"SELECT dataset_id,resource_type,resource_id,payload_hash,payload_json::text "
            f"FROM {schema}.provider_directory_dataset_resource ORDER BY dataset_id,resource_type,resource_id"
        )
    return [tuple(row) for row in rows]


async def _retry_cleanup_counts(database, schema):
    async with source_transaction(database):
        row = await database.first(
            "SELECT "
            f"(SELECT count(*) FROM {schema}.provider_directory_dataset_resource "
            "WHERE dataset_id='dataset_candidate' AND resource_type LIKE 'LU:%:pass:%'), "
            f"(SELECT count(*) FROM {schema}.provider_directory_pagination_checkpoint "
            "WHERE dataset_id='dataset_candidate')"
        )
    return tuple(row)


@pytest.mark.asyncio
async def test_retry_cleanup_rolls_back_and_recovers_after_publication(monkeypatch):
    """A real checkpoint delete rolls back after failure and can be replayed exactly."""
    async with _published_retry_database(monkeypatch) as (database, schema):
        original_status = database.status
        delete_results = []

        async def fail_after_delete(statement, *args, **kwargs):
            result = await original_status(statement, *args, **kwargs)
            if statement.lstrip().startswith("DELETE FROM") and "provider_directory_pagination_checkpoint" in statement:
                delete_results.append(result)
                raise RuntimeError("forced retry cleanup failure")
            return result

        with monkeypatch.context() as fault:
            fault.setattr(database, "status", fail_after_delete)
            assert await importer._clear_promoted_endpoint_dataset_retry_state(["dataset_candidate"]) == []
        assert len(delete_results) == 1
        assert await _retry_cleanup_counts(database, schema) == (0, 1)
        assert await _retry_payloads(database, schema) == database.retry_payloads_before
        assert await importer._clear_promoted_endpoint_dataset_retry_state(["dataset_candidate"]) == [
            "dataset_candidate"
        ]
        assert await _retry_cleanup_counts(database, schema) == (0, 0)
        assert await _retry_payloads(database, schema) == database.retry_payloads_before
