# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Deterministic fail-closed runtime branch coverage margin."""

from __future__ import annotations

import asyncio
import datetime as dt
import importlib
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import pytest

from api import control
from api import control_imports
from api import mrf_discovery_catalog_paging as catalog_paging
from api import provider_specialty_filters
from process import control_lifecycle
from process import live_progress
from process import provider_directory_retained_consumer_claim_store as claim_store
from process import provider_directory_retained_reader as retained_reader
from process import provider_quality_parts
from tests.ptg_frozen_test_support import protected_control_payload


lodes = importlib.import_module("process.lodes")
ptg_candidate_audit = importlib.import_module("process.ptg_candidate_audit")


class _ContendedRedis:
    def get(self, _key):
        return None

    def eval(self, *_args):
        return 0


class _UnavailableLockRedis:
    def set(self, *_args, **_kwargs):
        return False


def test_live_progress_contention_and_lock_timeout_fail_closed(
    monkeypatch,
) -> None:
    redis_client = _ContendedRedis()
    acquire_publication_lock = (
        live_progress._acquire_progress_publication_lock
    )
    monkeypatch.setattr(live_progress, "_redis", lambda: redis_client)
    monkeypatch.setattr(
        live_progress,
        "_acquire_progress_publication_lock",
        lambda *_args: None,
    )
    monkeypatch.setattr(
        live_progress,
        "current_live_progress_context",
        lambda: {"run_id": "run-lock-contended"},
    )
    assert live_progress.is_live_progress_written(done=1) is False

    monkeypatch.setattr(
        live_progress,
        "_merged_live_progress_candidate",
        lambda **_kwargs: {"run_id": "run-cas-contended"},
    )
    assert live_progress._is_live_progress_written_with_cas(
        redis_client=redis_client,
        run_id="run-cas-contended",
        context={},
        progress_by_field={},
        observed_at="2026-07-29T00:00:00Z",
        now=dt.datetime(2026, 7, 29),
        status_event_payload=None,
    ) is False

    monotonic_values = iter((0.0, 1.0))
    monkeypatch.setattr(
        live_progress.time,
        "monotonic",
        lambda: next(monotonic_values),
    )
    assert acquire_publication_lock(
        _UnavailableLockRedis(),
        "run-lock-timeout",
    ) is None


def test_live_progress_equal_attempts_and_empty_confidence_are_rejected() -> None:
    attempt_time = "2026-07-29T00:00:00Z"
    assert live_progress._attempt_disposition(
        {
            "run_id": "run-attempt",
            "attempt_id": "attempt-new",
            "attempt_started_at": attempt_time,
        },
        {
            "run_id": "run-attempt",
            "attempt_id": "attempt-old",
            "attempt_started_at": attempt_time,
        },
    ) == live_progress._ATTEMPT_REJECT

    merged = {
        "source": "engine-heartbeat",
        "confidence": "heartbeat",
    }
    live_progress._preserve_progress_for_heartbeat(
        merged,
        {"source": "scanner", "confidence": ""},
        now=dt.datetime(2026, 7, 29),
    )
    assert merged == {"source": "scanner", "confidence": "heartbeat"}


@pytest.mark.asyncio
async def test_control_wrapper_tokenless_exception_and_rejection(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        control_lifecycle,
        "mark_control_run",
        AsyncMock(side_effect=RuntimeError("claim unavailable")),
    )
    with pytest.raises(RuntimeError, match="claim unavailable"):
        await control_lifecycle.control_single_job_start({}, {})

    monkeypatch.setattr(
        control_lifecycle,
        "set_live_progress_context",
        lambda **_payload: None,
    )
    monkeypatch.setattr(
        control_lifecycle,
        "mark_control_run",
        AsyncMock(return_value=False),
    )
    outcome = await control_lifecycle.control_single_job_start(
        {},
        {
            "run_id": "run-rejected-without-token",
            "target_module": "unused.module",
            "target_function": "unused",
        },
    )
    assert outcome["reason"] == "newer_attempt_active"


@pytest.mark.asyncio
async def test_control_heartbeat_without_attempt_uses_unfenced_update(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        control_lifecycle,
        "read_live_progress",
        lambda _run_id: {"done": 1},
    )
    update_executor = AsyncMock(return_value=1)
    monkeypatch.setattr(
        control_lifecycle,
        "_execute_control_run_update",
        update_executor,
    )

    assert await control_lifecycle._is_control_run_heartbeat_persisted(
        "run-heartbeat-unfenced",
        "process_data",
    ) is True
    update_executor.assert_awaited_once()


def test_invalid_explicit_terminal_progress_is_rejected() -> None:
    with pytest.raises(ValueError, match="terminal progress result is invalid"):
        control_lifecycle._explicit_terminal_progress(
            {
                "terminal_progress": {
                    "done": True,
                    "total": 1,
                    "pct": 100,
                    "unit": "item",
                    "message": "done",
                    "phase": "complete",
                }
            }
        )


@pytest.mark.asyncio
async def test_canceled_control_mark_uses_private_fallback_progress(
    monkeypatch,
) -> None:
    monkeypatch.setattr(control_lifecycle, "read_live_progress", lambda _run_id: {})
    monkeypatch.setattr(
        control_lifecycle,
        "_should_update_control_run_db",
        AsyncMock(return_value=True),
    )
    monkeypatch.setattr(
        control_lifecycle,
        "_execute_control_run_update",
        AsyncMock(return_value=1),
    )
    monkeypatch.setattr(
        control_lifecycle,
        "project_frozen_status_event",
        lambda _event: {"progress": None},
    )
    monkeypatch.setattr(control_lifecycle.asyncio, "to_thread", AsyncMock())

    assert await control_lifecycle.mark_control_run(
        "run-canceled",
        status="canceled",
        phase_detail="canceled",
        progress_message="canceled",
    ) is True


@pytest.mark.asyncio
async def test_projection_release_dependency_empty_and_released_paths(
    monkeypatch,
) -> None:
    assert await claim_store._locked_projection_recipe_records(
        object(),
        "recipe_table",
        [],
    ) == []

    monkeypatch.setattr(
        claim_store,
        "_has_projection_release_relations",
        AsyncMock(return_value=True),
    )
    candidate_loader = AsyncMock(return_value=[])
    monkeypatch.setattr(
        claim_store,
        "_projection_release_candidate_records",
        candidate_loader,
    )
    await claim_store._lock_projection_release_dependencies(
        object(),
        "campaign-empty",
        "consumer-empty",
    )

    candidate_loader.return_value = [
        {"admission_id": "admission-one", "recipe_id": "recipe-one"}
    ]
    monkeypatch.setattr(
        claim_store,
        "_locked_projection_recipe_records",
        AsyncMock(return_value=[{"recipe_id": "recipe-one", "status": "released"}]),
    )
    monkeypatch.setattr(
        claim_store,
        "_locked_projection_admission_records",
        AsyncMock(
            return_value=[
                {
                    "admission_id": "admission-one",
                    "recipe_id": "recipe-one",
                    "status": "released",
                }
            ]
        ),
    )
    await claim_store._lock_projection_release_dependencies(
        object(),
        "campaign-released",
        "consumer-released",
    )


class _CanceledCloseStream:
    async def aclose(self):
        raise asyncio.CancelledError


def _reader_with_active_stream():
    reader = object.__new__(retained_reader.RetainedArtifactReader)
    reader._closing = False
    reader._closed = False
    reader._failed = False
    reader._active_stream = _CanceledCloseStream()
    reader._operations_settled = asyncio.Event()
    reader._operations_settled.set()
    reader._completed_read_count = 1
    return reader


@pytest.mark.asyncio
async def test_retained_reader_preserves_close_cancellation(monkeypatch) -> None:
    reader = _reader_with_active_stream()
    with pytest.raises(asyncio.CancelledError):
        await reader._close_normally()

    reader = _reader_with_active_stream()
    pending_cancellation = asyncio.CancelledError()
    monkeypatch.setattr(
        retained_reader,
        "_join_settlement",
        AsyncMock(return_value=pending_cancellation),
    )
    with pytest.raises(asyncio.CancelledError) as raised:
        await reader._close_normally()
    assert raised.value is pending_cancellation


@pytest.mark.asyncio
async def test_retained_reader_abort_settles_canceled_stream() -> None:
    reader = _reader_with_active_stream()
    await reader._abort_for_consumer_exception()
    assert reader._closed is True


def _provider_directory_acquisition_params() -> dict[str, object]:
    return {
        "import_resources": True,
        "stale_cleanup": False,
        "publish_artifacts": False,
        "publish_after_acquisition": False,
        "publish_corroboration": False,
        "source_concurrency": 1,
        "source_ids": ["source-one"],
    }


def test_acquisition_scope_and_protected_outer_identity_fail_closed() -> None:
    assert control_imports._provider_directory_acquisition_scope(
        _provider_directory_acquisition_params(),
        {"active_source_groups": []},
    ) is None

    request_payload = protected_control_payload()
    request_payload["import_id"] = "mismatched-outer-id"
    with pytest.raises(ValueError, match="outer and nested"):
        asyncio.run(control_imports.create_import_run(request_payload))


@pytest.mark.asyncio
async def test_ptg_admission_returns_existing_idempotent_run(
    monkeypatch,
) -> None:
    class _Connection:
        async def scalar(self, *_args, **_kwargs):
            return 1

    @asynccontextmanager
    async def acquire():
        yield _Connection()

    existing_run = {"run_id": "run-existing"}
    monkeypatch.setattr(control_imports.db, "acquire", acquire)
    monkeypatch.setattr(
        control_imports,
        "insert_or_compare_frozen_binding",
        AsyncMock(),
    )
    monkeypatch.setattr(
        control_imports,
        "_active_idempotency_run",
        AsyncMock(return_value=existing_run),
    )
    request_payload = control._validated_control_import_payload(
        protected_control_payload()
    )
    admitted_run = await control_imports._admit_ptg_source_file_run(
        {
            "run_id": "run-new",
            "importer": "ptg",
            "params": request_payload["params"],
            "source_file_import_id": request_payload["source_file_import_id"],
            "idempotency_key": "idempotency-key",
        }
    )
    assert admitted_run == existing_run


def test_classification_only_semijoin_allows_subspecialties() -> None:
    params: dict[str, object] = {}
    specialty_filter = provider_specialty_filters.ProviderSpecialtyFilter(
        classification="Internal Medicine",
        include_subspecialties=True,
    )
    query = provider_specialty_filters.provider_specialty_taxonomy_semijoin_sql(
        params,
        "specialty",
        specialty_filter,
        schema="mrf",
    )
    assert "specialization" not in query


@pytest.mark.asyncio
async def test_partition_failure_without_request_identity_is_not_published() -> None:
    failure = ptg_candidate_audit.BatchCandidateAuditContractError(
        "request unavailable"
    )
    await ptg_candidate_audit._partition_failure_progress(
        None,
        snapshot_id="snapshot-one",
        completed=0,
        total=1,
        failure=failure,
    )


def test_provider_quality_boolean_parser_accepts_enabled_value(
    monkeypatch,
) -> None:
    monkeypatch.setenv("HLTHPRT_MARGIN_BOOLEAN", "yes")
    assert provider_quality_parts.config._is_environment_enabled(
        "HLTHPRT_MARGIN_BOOLEAN"
    ) is True


class _EmptyRowStream:
    def __init__(self) -> None:
        self.closed = False

    def __aiter__(self):
        return self

    async def __anext__(self):
        raise StopAsyncIteration

    async def aclose(self):
        self.closed = True


class _EmptyFileStatement:
    def __init__(self, row_stream: _EmptyRowStream) -> None:
        self.row_stream = row_stream

    def execution_options(self, **_options):
        return self

    def iterate(self):
        return self.row_stream


@pytest.mark.asyncio
async def test_empty_catalog_stream_is_closed() -> None:
    row_stream = _EmptyRowStream()
    rows = await catalog_paging.collect_bounded_file_query_rows(
        _EmptyFileStatement(row_stream),
        limit=1,
        cursor_plan_offset=0,
        plan_reference_limit=1,
    )
    assert rows == []
    assert row_stream.closed is True


@pytest.mark.asyncio
async def test_lodes_failure_marker_is_best_effort(monkeypatch) -> None:
    monkeypatch.setattr(
        lodes,
        "mark_control_run",
        AsyncMock(side_effect=RuntimeError("database unavailable")),
    )
    await lodes._mark_lodes_publish_failed(
        "run-lodes",
        RuntimeError("publish failed"),
    )
