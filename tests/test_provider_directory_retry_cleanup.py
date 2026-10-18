# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import contextlib

import pytest
from sqlalchemy import event, text

from process.provider_directory_generic_source_policy import source_transaction
from tests.test_provider_directory_two_phase_publication import (
    _published_retry_database,
    _retry_cleanup_counts,
    _retry_payloads,
    importer,
)


def test_retry_cleanup_resource_types_are_exact_supported_markers(monkeypatch):
    expected_resource_types = [
        f"LU:{resource_type}:pass:{pass_number}"
        for resource_type in importer.DEFAULT_RESOURCES
        for pass_number in (1, 2)
    ]
    assert importer._last_updated_partition_retry_resource_types() == expected_resource_types

    monkeypatch.setattr(
        importer,
        "DEFAULT_RESOURCES",
        (*importer.DEFAULT_RESOURCES, "SyntheticResource"),
    )
    assert importer._last_updated_partition_retry_resource_types()[-2:] == [
        "LU:SyntheticResource:pass:1",
        "LU:SyntheticResource:pass:2",
    ]


@contextlib.asynccontextmanager
async def _locked_candidate_dataset(database, schema):
    """Hold only the exact task-owned dataset through the independent administrator."""
    async with database.semantic_component.admin_engine.begin() as connection:
        await connection.execute(
            text(
                f"SELECT dataset_id FROM {schema}.provider_directory_endpoint_dataset "
                "WHERE dataset_id='dataset_candidate' FOR UPDATE"
            )
        )
        yield


@pytest.mark.asyncio
async def test_cleanup_only_clears_promoted_candidate_in_mixed_fence(monkeypatch):
    """Retain the unchanged current endpoint's cursor and both immutable payloads."""
    async with _published_retry_database(monkeypatch, peer=True) as (database, schema):
        async with source_transaction(database):
            rows = await database.all(
                f"SELECT dataset_id,status,is_current FROM {schema}.provider_directory_endpoint_dataset ORDER BY dataset_id"
            )
        assert [tuple(row) for row in rows] == [
            ("dataset_candidate", importer.ENDPOINT_DATASET_PUBLISHED, True),
            ("dataset_z_current", importer.ENDPOINT_DATASET_PUBLISHED, True),
        ]
        metrics = await importer._record_artifact_promotion_metrics(
            database.retry_publication_fence,
            {},
            promoted_stage_count=1,
        )
        assert metrics["artifact_promoted_dataset_ids"] == ["dataset_candidate"]
        assert metrics["artifact_retry_state_cleared_dataset_ids"] == ["dataset_candidate"]
        assert metrics["artifact_retry_state_pending_dataset_ids"] == []
        async with source_transaction(database):
            rows = await database.all(
                f"SELECT dataset_id FROM {schema}.provider_directory_pagination_checkpoint ORDER BY dataset_id"
            )
        assert [row[0] for row in rows] == ["dataset_z_current"]
        assert await _retry_payloads(database, schema) == database.retry_payloads_before


@pytest.mark.asyncio
async def test_cleanup_lock_contention_is_bounded_and_recoverable(monkeypatch):
    """A concurrent dataset lock leaves its real cursor pending without blocking."""
    async with _published_retry_database(monkeypatch) as (database, schema):
        async with _locked_candidate_dataset(database, schema):
            started_at = asyncio.get_running_loop().time()
            assert await importer._clear_promoted_endpoint_dataset_retry_state(["dataset_candidate"]) == []
            assert asyncio.get_running_loop().time() - started_at < (
                importer.PROVIDER_DIRECTORY_ARTIFACT_CUTOVER_TRANSACTION_TIMEOUT_SECONDS
            )
            assert await _retry_cleanup_counts(database, schema) == (0, 1)
        assert await importer._clear_promoted_endpoint_dataset_retry_state(["dataset_candidate"]) == [
            "dataset_candidate"
        ]
        assert await _retry_cleanup_counts(database, schema) == (0, 0)
        assert await _retry_payloads(database, schema) == database.retry_payloads_before


@pytest.mark.asyncio
async def test_cleanup_preserves_payload_and_budget(monkeypatch):
    """Cleanup reads the control header and deletes cursors without scanning payload."""
    async with _published_retry_database(monkeypatch) as (database, schema):
        statements = []

        def record_statement(_connection, _cursor, statement, _parameters, _context, _many):
            statements.append(statement)

        engine = database.engine.sync_engine
        event.listen(engine, "before_cursor_execute", record_statement)
        try:
            assert await importer._clear_promoted_endpoint_dataset_retry_state(["dataset_candidate"]) == [
                "dataset_candidate"
            ]
        finally:
            event.remove(engine, "before_cursor_execute", record_statement)
        assert importer.PROVIDER_DIRECTORY_ARTIFACT_CUTOVER_STATEMENT_TIMEOUT == "1000ms"
        assert any(
            "DELETE FROM" in statement and "provider_directory_pagination_checkpoint" in statement
            for statement in statements
        )
        assert not any("provider_directory_dataset_resource" in statement for statement in statements)
        assert await _retry_cleanup_counts(database, schema) == (0, 0)
        assert await _retry_payloads(database, schema) == database.retry_payloads_before
