# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import contextlib
import json

import pytest

from db.connection import Database
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
    fence_token = importer._PROVIDER_DIRECTORY_ARTIFACT_DATASET_FENCE.set(
        dataset_fence
    )
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
    await database.status(
        f"INSERT INTO {schema}.provider_directory_api_endpoint (endpoint_id) "
        "VALUES ('endpoint_z');"
    )
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


async def _artifact_bundle_stages(
    schema: str,
    *,
    should_fail_second: bool = False,
):
    second_index_handler = (
        _fail_stage_indexes if should_fail_second else _keep_stage_indexes
    )
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
        assert full_fence.dataset_id_by_endpoint_id == {
            "endpoint_shared": "dataset_candidate"
        }
        assert [
            dataset.dataset_id for dataset in full_fence.promotion_datasets
        ] == ["dataset_candidate"]
        assert targeted_fence.dataset_id_by_endpoint_id == {
            "endpoint_shared": "dataset_shared"
        }
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
                f"SELECT source_id, name FROM {schema}.{location_scope} "
                "ORDER BY source_id;"
            )
            assert [tuple(scope_name_row) for scope_name_row in scoped_names] == [
                ("source_primary", "Candidate Clinic"),
                ("source_sibling", "Candidate Clinic"),
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
                await importer._promote_provider_directory_artifact_bundle_transaction(
                    failing_stages
                )
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
            await importer._promote_provider_directory_artifact_bundle_transaction(
                successful_stages
            )
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
        assert [
            dataset.endpoint_id for dataset in dataset_fence.promotion_datasets
        ] == ["endpoint_shared", "endpoint_z"]
        artifact_stages = await _artifact_bundle_stages(schema)
        promotion_entered, release_promotion = _install_pointer_pause(monkeypatch)
        async with _publication_observer() as observer:
            with _active_dataset_fence(dataset_fence):
                assert await _artifact_relation_value(
                    observer, schema, "artifact_target_a"
                ) == "old-a"
                promotion_task = importer.asyncio.create_task(
                    importer._promote_provider_directory_artifact_bundle_transaction(
                        artifact_stages
                    )
                )
                relation_reader_task = None
                try:
                    await promotion_entered.wait()
                    assert await _published_dataset_pointers(observer, schema) == [
                        ("endpoint_shared", "dataset_shared"),
                        ("endpoint_z", "dataset_z_current"),
                    ]
                    relation_reader_task = importer.asyncio.create_task(
                        _artifact_relation_value(
                            observer, schema, "artifact_target_a"
                        )
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


async def _prepare_retry_cleanup_failure(database, schema):
    """Create finalized retry state whose second delete fails."""
    await _insert_validated_shared_dataset(database, schema)
    dataset_fence = await importer._resolve_provider_directory_artifact_datasets(
        ["source_primary"],
        should_select_validated_candidates=True,
    )
    await database.status(
        f"UPDATE {schema}.provider_directory_endpoint_dataset "
        "SET status = :superseded_status, is_current = false, "
        "superseded_at = now() WHERE dataset_id = 'dataset_shared';",
        superseded_status=importer.ENDPOINT_DATASET_SUPERSEDED,
    )
    await database.status(
        f"UPDATE {schema}.provider_directory_endpoint_dataset "
        "SET status = :published_status, is_current = true, "
        "published_at = now() WHERE dataset_id = 'dataset_candidate';",
        published_status=importer.ENDPOINT_DATASET_PUBLISHED,
    )
    await database.status(
        f"CREATE TABLE {schema}.provider_directory_pagination_checkpoint ("
        "dataset_id varchar(96) NOT NULL);"
    )
    await database.status(
        f"INSERT INTO {schema}.provider_directory_pagination_checkpoint "
        "VALUES ('dataset_candidate');"
    )
    await database.status(
        f"INSERT INTO {schema}.provider_directory_dataset_resource ("
        "dataset_id, resource_type, resource_id, payload_hash, payload_json"
        ") VALUES ('dataset_candidate', 'LU:Location:pass:1', 'pass-row', "
        ":payload_hash, CAST('{}' AS json));",
        payload_hash="7" * 64,
    )
    await database.status(
        f"CREATE FUNCTION {schema}.fail_retry_cleanup() RETURNS trigger "
        "LANGUAGE plpgsql AS $$ BEGIN "
        "RAISE EXCEPTION 'forced retry cleanup failure'; END; $$;"
    )
    await database.status(
        f"CREATE TRIGGER fail_retry_cleanup BEFORE DELETE ON "
        f"{schema}.provider_directory_pagination_checkpoint "
        f"FOR EACH STATEMENT EXECUTE FUNCTION {schema}.fail_retry_cleanup();"
    )
    return dataset_fence


async def _retry_cleanup_counts(database, schema):
    count_row = await database.first(
        "SELECT "
        f"(SELECT count(*) FROM {schema}.provider_directory_dataset_resource "
        "WHERE dataset_id = 'dataset_candidate' "
        "AND resource_type LIKE 'LU:%:pass:%') AS pass_rows, "
        f"(SELECT count(*) FROM {schema}.provider_directory_pagination_checkpoint "
        "WHERE dataset_id = 'dataset_candidate') AS checkpoints;"
    )
    return tuple(count_row)


@pytest.mark.asyncio
async def test_retry_cleanup_rolls_back_and_recovers_after_publication(monkeypatch):
    """A cleanup failure cannot half-delete state and a replay can finish it."""
    async with _dataset_database(monkeypatch) as (database, schema):
        dataset_fence = await _prepare_retry_cleanup_failure(database, schema)
        assert await importer._clear_promoted_endpoint_dataset_retry_state(
            dataset_fence
        ) == []
        assert await _retry_cleanup_counts(database, schema) == (1, 1)
        await database.status(
            f"DROP TRIGGER fail_retry_cleanup ON "
            f"{schema}.provider_directory_pagination_checkpoint;"
        )
        assert await importer._clear_promoted_endpoint_dataset_retry_state(
            dataset_fence
        ) == ["dataset_candidate"]
        assert await _retry_cleanup_counts(database, schema) == (0, 0)
