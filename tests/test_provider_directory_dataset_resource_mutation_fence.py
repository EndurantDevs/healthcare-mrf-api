# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import contextlib
import dataclasses
import importlib
from typing import Any
from unittest.mock import AsyncMock

import pytest


importer = importlib.import_module("process.provider_directory_fhir")


class _RecordingExecutor:
    def __init__(self, parent_rows: list[dict[str, Any]]):
        self.parent_rows = parent_rows
        self.events: list[str] = []
        self.all_calls: list[tuple[str, dict[str, Any]]] = []
        self.status_calls: list[tuple[Any, dict[str, Any]]] = []

    async def all(self, sql: str, **params: Any):
        self.events.append("lock")
        self.all_calls.append((str(sql), params))
        return self.parent_rows

    async def status(self, statement: Any, **params: Any):
        self.events.append("write")
        self.status_calls.append((statement, params))
        return 1


class _RecordingSession:
    def __init__(self, events: list[str]):
        self.events = events

    async def execute(self, _statement):
        self.events.append("write")


class _BoundSessionTransactionHarness:
    def __init__(self):
        self.events: list[str] = []
        self.bound_sessions: list[Any | None] = [None]
        self.session = _RecordingSession(self.events)

    @contextlib.asynccontextmanager
    async def transaction(self):
        assert self.bound_sessions[0] is None
        self.bound_sessions[0] = self.session
        self.events.append("begin")
        try:
            yield self.session
        except BaseException:
            self.events.append("rollback")
            raise
        else:
            self.events.append("commit")
        finally:
            self.bound_sessions[0] = None

    async def locked_parents(self, _sql, **params):
        self.events.append("lock:" + ",".join(params["dataset_ids"]))
        return [
            {
                "dataset_id": dataset_id,
                "status": importer.ENDPOINT_DATASET_ACQUIRING,
            }
            for dataset_id in params["dataset_ids"]
        ]

    def bound_session(self):
        return self.bound_sessions[0]


def _dataset_resource_row(
    dataset_id: str | None,
    resource_id: str,
) -> dict[str, Any]:
    return {
        "dataset_id": dataset_id,
        "resource_type": "Practitioner",
        "resource_id": resource_id,
        "payload_hash": resource_id.rjust(64, "0")[-64:],
        "payload_json": {"id": resource_id},
    }


def _candidate(dataset_id: str = "dataset_sealed"):
    return importer.EndpointDatasetCandidate(
        endpoint_id="endpoint_1",
        dataset_id=dataset_id,
        acquisition_root_run_id="root_1",
        source_ids=("source_1",),
        selected_resources=("Practitioner",),
        expected_resources=("Practitioner",),
        import_run_id="run_1",
        previous_dataset_id=None,
    )


def _checkpoint_context(dataset_id: str = "dataset_sealed"):
    return importer.PaginationCheckpointContext(
        canonical_api_base="https://payer.example/fhir",
        source_scope_hash="scope_1",
        source_ids=("source_1",),
        owner_run_id="run_1",
        acquisition_root_run_id="root_1",
        dataset_id=dataset_id,
    )


@pytest.mark.asyncio
async def test_parent_lock_is_exact_sorted_and_accepts_mutable_statuses():
    executor = _RecordingExecutor(
        [
            {
                "dataset_id": "dataset_a",
                "status": importer.ENDPOINT_DATASET_ACQUIRING,
            },
            {
                "dataset_id": "dataset_b",
                "status": importer.ENDPOINT_DATASET_INCOMPLETE,
            },
        ]
    )

    locked_ids = await importer._lock_mutable_endpoint_dataset_resource_parents(
        executor,
        ["dataset_b", "dataset_a", "dataset_b"],
    )

    assert locked_ids == ["dataset_a", "dataset_b"]
    lock_sql, lock_params = executor.all_calls[0]
    assert "ORDER BY dataset_id" in lock_sql
    assert "FOR SHARE" in lock_sql
    assert "FOR UPDATE" not in lock_sql
    assert lock_params["dataset_ids"] == ["dataset_a", "dataset_b"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "immutable_status",
    importer.IMMUTABLE_ENDPOINT_DATASET_STATUSES,
)
async def test_parent_lock_rejects_every_immutable_status(immutable_status):
    executor = _RecordingExecutor(
        [{"dataset_id": "dataset_sealed", "status": immutable_status}]
    )

    with pytest.raises(
        RuntimeError,
        match=(
            "provider_directory_endpoint_dataset_resource_parent_immutable:"
            f"dataset_sealed={immutable_status}"
        ),
    ):
        await importer._lock_mutable_endpoint_dataset_resource_parents(
            executor,
            ["dataset_sealed"],
        )


@pytest.mark.asyncio
async def test_parent_lock_rejects_missing_parent_and_unscoped_row():
    executor = _RecordingExecutor(
        [{"dataset_id": None, "status": importer.ENDPOINT_DATASET_ACQUIRING}]
    )
    with pytest.raises(RuntimeError, match="parent_missing:dataset_missing"):
        await importer._lock_mutable_endpoint_dataset_resource_parents(
            executor,
            ["dataset_missing"],
        )
    with pytest.raises(RuntimeError, match="parent_id_missing"):
        importer._endpoint_dataset_resource_parent_ids(
            [_dataset_resource_row(None, "practitioner_1")]
        )


@pytest.mark.asyncio
async def test_normalized_dataset_upsert_requires_transaction(monkeypatch):
    guard = AsyncMock()
    monkeypatch.setattr(importer, "_bound_upsert_session", lambda: None)
    monkeypatch.setattr(
        importer,
        "_lock_mutable_endpoint_dataset_resource_parents",
        guard,
    )

    with pytest.raises(RuntimeError, match="resource_transaction_missing"):
        await importer._upsert_normalized_rows(
            importer.ProviderDirectoryDatasetResource,
            [_dataset_resource_row("dataset_a", "practitioner_1")],
            [],
            [],
            skip_unchanged=False,
        )
    guard.assert_not_awaited()


@pytest.mark.asyncio
async def test_generic_upsert_locks_all_parents_once_for_all_value_batches(
    monkeypatch,
):
    """One generic call holds one fence across every values batch."""
    harness = _BoundSessionTransactionHarness()
    monkeypatch.setattr(importer.db, "transaction", harness.transaction)
    monkeypatch.setattr(importer.db, "all", harness.locked_parents)
    monkeypatch.setattr(
        importer,
        "_bound_upsert_session",
        harness.bound_session,
    )
    monkeypatch.setattr(importer, "_is_copy_upsert_enabled", lambda: False)
    monkeypatch.setattr(importer, "_max_rows_per_statement", lambda _count: 1)

    written = await importer._upsert_rows(
        importer.ProviderDirectoryDatasetResource,
        [
            _dataset_resource_row("dataset_b", "practitioner_2"),
            _dataset_resource_row("dataset_a", "practitioner_1"),
        ],
    )

    assert written == 2
    assert harness.events == [
        "begin",
        "lock:dataset_a,dataset_b",
        "write",
        "write",
        "commit",
    ]


@pytest.mark.asyncio
async def test_generic_copy_upsert_uses_savepoint_inside_parent_fence(monkeypatch):
    events: list[str] = []
    transaction_depths = [0]
    session = object()

    @contextlib.asynccontextmanager
    async def transaction():
        transaction_depths[0] += 1
        label = "begin" if transaction_depths[0] == 1 else "savepoint"
        events.append(label)
        try:
            yield session
        except BaseException:
            events.append(label + "_rollback")
            raise
        else:
            events.append(label + "_commit")
        finally:
            transaction_depths[0] -= 1

    async def locked_parents(_sql, **params):
        events.append("lock")
        return [
            {
                "dataset_id": params["dataset_ids"][0],
                "status": importer.ENDPOINT_DATASET_ACQUIRING,
            }
        ]

    async def copy_upsert(*_args, **_kwargs):
        events.append("copy")
        return 1

    monkeypatch.setattr(importer.db, "transaction", transaction)
    monkeypatch.setattr(importer.db, "all", locked_parents)
    monkeypatch.setattr(
        importer,
        "_bound_upsert_session",
        lambda: session if transaction_depths[0] else None,
    )
    monkeypatch.setattr(importer, "_is_copy_upsert_enabled", lambda: True)
    monkeypatch.setattr(importer, "_copy_upsert_min_rows", lambda: 1)
    monkeypatch.setattr(importer, "_copy_upsert_rows", copy_upsert)

    written = await importer._upsert_rows(
        importer.ProviderDirectoryDatasetResource,
        [_dataset_resource_row("dataset_a", "practitioner_1")],
    )

    assert written == 1
    assert events == [
        "begin",
        "lock",
        "savepoint",
        "copy",
        "savepoint_commit",
        "begin_commit",
    ]


@pytest.mark.asyncio
async def test_direct_connection_upsert_locks_once_before_all_batches(monkeypatch):
    executor = _RecordingExecutor(
        [
            {
                "dataset_id": "dataset_a",
                "status": importer.ENDPOINT_DATASET_FAILED,
            },
            {
                "dataset_id": "dataset_b",
                "status": importer.ENDPOINT_DATASET_ACQUIRING,
            },
        ]
    )
    monkeypatch.setattr(importer, "_max_rows_per_statement", lambda _count: 1)

    await importer._upsert_dataset_resource_rows_on_connection(
        executor,
        [
            _dataset_resource_row("dataset_b", "practitioner_2"),
            _dataset_resource_row("dataset_a", "practitioner_1"),
        ],
    )

    assert executor.events == ["lock", "write", "write"]
    assert executor.all_calls[0][1]["dataset_ids"] == [
        "dataset_a",
        "dataset_b",
    ]


@pytest.mark.asyncio
async def test_candidate_initial_clear_rejects_sealed_parent_before_delete(
    monkeypatch,
):
    candidate = _candidate()
    executor = _RecordingExecutor(
        [
            {
                "dataset_id": candidate.dataset_id,
                "status": importer.ENDPOINT_DATASET_VALIDATED,
            }
        ]
    )

    @contextlib.asynccontextmanager
    async def acquire():
        yield executor

    monkeypatch.setattr(importer.db, "acquire", acquire)
    monkeypatch.setattr(
        importer,
        "_lock_endpoint_dataset_candidate_admission",
        AsyncMock(),
    )
    monkeypatch.setattr(
        importer,
        "_assert_locked_endpoint_candidate",
        AsyncMock(),
    )
    monkeypatch.setattr(
        importer,
        "_assert_empty_endpoint_dataset_orphan",
        AsyncMock(),
    )
    monkeypatch.setattr(
        importer,
        "_assert_no_conflicting_endpoint_candidate",
        AsyncMock(return_value=candidate),
    )
    monkeypatch.setattr(
        importer,
        "_upsert_endpoint_dataset_candidate",
        AsyncMock(),
    )

    with pytest.raises(RuntimeError, match="parent_immutable"):
        await importer._ensure_endpoint_dataset_candidate(candidate)
    assert executor.status_calls == []


@pytest.mark.asyncio
async def test_uncheckpointed_clear_rejects_sealed_parent_before_delete(
    monkeypatch,
):
    candidate = _candidate()
    executor = _RecordingExecutor(
        [
            {
                "dataset_id": candidate.dataset_id,
                "status": importer.ENDPOINT_DATASET_PUBLISHED,
            }
        ]
    )

    @contextlib.asynccontextmanager
    async def transaction():
        yield object()

    monkeypatch.setattr(importer.db, "transaction", transaction)
    monkeypatch.setattr(importer.db, "all", executor.all)
    monkeypatch.setattr(importer.db, "status", executor.status)

    with pytest.raises(RuntimeError, match="parent_immutable"):
        await importer._clear_uncheckpointed_endpoint_dataset_candidate(
            candidate
        )
    assert executor.status_calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation_name", ["checkpoint_reset", "checkpoint_clear"])
async def test_checkpoint_deletes_reject_sealed_parent_before_dml(
    mutation_name,
):
    context = _checkpoint_context()
    executor = _RecordingExecutor(
        [
            {
                "dataset_id": context.dataset_id,
                "status": importer.ENDPOINT_DATASET_SUPERSEDED,
            }
        ]
    )

    with pytest.raises(RuntimeError, match="parent_immutable"):
        if mutation_name == "checkpoint_reset":
            await importer._reset_pagination_checkpoint(
                context,
                "Practitioner",
                "https://payer.example/fhir/Practitioner",
                "start_hash",
                database_connection=executor,
            )
        else:
            await importer._clear_checkpoint_dataset_resource_type(
                context,
                "Practitioner",
                database_connection=executor,
            )
    assert executor.status_calls == []


@pytest.mark.asyncio
async def test_finalized_retry_cleanup_remains_narrow_immutable_exception(
    monkeypatch,
):
    status = AsyncMock(return_value=1)
    guard = AsyncMock(
        side_effect=AssertionError("retry-state exception used mutable guard")
    )
    monkeypatch.setattr(importer.db, "status", status)
    monkeypatch.setattr(
        importer,
        "_lock_mutable_endpoint_dataset_resource_parents",
        guard,
    )

    await importer._delete_endpoint_dataset_retry_state(["dataset_final"])

    retry_resource_sql = status.await_args_list[0].args[0]
    assert "resource_type LIKE 'LU:%:pass:%'" in retry_resource_sql
    assert status.await_args_list[0].kwargs["dataset_ids"] == ["dataset_final"]
    guard.assert_not_awaited()


@pytest.mark.asyncio
async def test_matched_baseline_retirement_remains_proof_scoped_exception(
    monkeypatch,
):
    candidate = _candidate("dataset_successor")
    candidate = dataclasses.replace(
        candidate,
        requires_twin_root_verification=True,
        verification_campaign_id="campaign_1",
        verification_source_scope_hash="scope_1",
        verification_role=importer.TWIN_ROOT_VERIFICATION_CANDIDATE_ROLE,
        verification_baseline_dataset_id="dataset_baseline",
    )
    executor = _RecordingExecutor([])
    guard = AsyncMock(
        side_effect=AssertionError("baseline exception used mutable guard")
    )
    monkeypatch.setattr(
        importer,
        "_lock_mutable_endpoint_dataset_resource_parents",
        guard,
    )

    deleted = await importer._delete_matched_baseline_resource_rows(
        executor,
        candidate,
        "dataset_baseline",
    )

    delete_sql, params = executor.status_calls[0]
    assert "baseline.status = :baseline_status" in delete_sql
    assert "successor.status = :validated_status" in delete_sql
    assert "->> 'result' = 'matched'" in delete_sql
    assert params["baseline_dataset_id"] == "dataset_baseline"
    assert deleted == 1
    guard.assert_not_awaited()
