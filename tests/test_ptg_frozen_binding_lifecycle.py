# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Lifecycle ordering proofs for immutable frozen PTG bindings."""

from __future__ import annotations

import importlib
from contextlib import asynccontextmanager

import pytest

from api import control
from process import ptg_control, ptg_frozen_control
from process.ptg_parts.frozen_rate_binding import (
    FrozenRateFileBindingMismatchError,
)
from process.ptg_parts.ptg_source_attempt_guard import (
    source_file_import_id_from_payload,
)
from tests.ptg_frozen_test_support import protected_control_payload
from tests.ptg_singleton_direct_test_support import _direct_params


ptg = importlib.import_module("process.ptg")


@pytest.fixture(autouse=True)
def _admit_lifecycle_ptg_run(monkeypatch):
    async def admit_run(task_payload, *, run_id, **_kwargs):
        try:
            source_file_import_id_from_payload(task_payload, required=False)
        except ValueError:
            return {
                "status": "skipped",
                "run_id": run_id,
                "reason": "source_attempt_identity_mismatch",
            }
        return None

    monkeypatch.setattr(
        ptg_control,
        "guard_ptg_worker_start",
        admit_run,
    )


class _WorkerFailureHarness:
    def __init__(self) -> None:
        self.mark_calls: list[dict[str, object]] = []
        self.main_calls: list[bool] = []

    async def no_stale_run(self, _run_id):
        return None

    async def reject_binding(self, _params_by_name):
        raise FrozenRateFileBindingMismatchError("immutable retry drift")

    async def has_claimed_control_run(self, *_args, **kwargs):
        self.mark_calls.append(kwargs)
        return True

    async def flush_status(self, _run_id):
        return None

    async def run_main(self, **_kwargs):
        self.main_calls.append(True)


@pytest.mark.asyncio
async def test_worker_binding_recheck_is_terminal_inside_lifecycle(
    monkeypatch,
):
    """A worker recheck mismatch must be persisted as non-retryable."""

    harness = _WorkerFailureHarness()
    monkeypatch.setattr(
        ptg_control,
        "_stale_ptg_job_result",
        harness.no_stale_run,
    )
    monkeypatch.setattr(
        ptg_frozen_control,
        "recheck_frozen_binding",
        harness.reject_binding,
    )
    monkeypatch.setattr(
        ptg_control,
        "mark_control_run",
        harness.has_claimed_control_run,
    )
    monkeypatch.setattr(
        ptg_control,
        "_flush_terminal_status_events",
        harness.flush_status,
    )
    monkeypatch.setattr(ptg_control, "ptg_main", harness.run_main)
    request_payload_by_name = control._validated_control_import_payload(
        protected_control_payload()
    )

    with pytest.raises(
        FrozenRateFileBindingMismatchError,
        match="immutable retry drift",
    ):
        await ptg_control.ptg_control_start(
            {},
            {
                "run_id": "run-protected",
                "source_file_import_id": request_payload_by_name[
                    "source_file_import_id"
                ],
                "import_id": request_payload_by_name["import_id"],
                "params": request_payload_by_name["params"],
            },
        )

    assert harness.main_calls == []
    assert [
        mark_call["status"] for mark_call in harness.mark_calls
    ] == ["running", "failed"]
    assert harness.mark_calls[1]["error"] == {
        "code": "ptg_frozen_rate_file_contract_failed",
        "message": "immutable retry drift",
        "retryable": False,
    }
    assert harness.mark_calls[0]["attempt_id"] == (
        harness.mark_calls[1]["attempt_id"]
    )


@pytest.mark.asyncio
async def test_singleton_worker_rejects_existing_frozen_binding(
    monkeypatch,
):
    harness = _WorkerFailureHarness()
    monkeypatch.setattr(
        ptg_control,
        "_stale_ptg_job_result",
        harness.no_stale_run,
    )
    monkeypatch.setattr(
        ptg_control,
        "recheck_frozen_binding",
        harness.reject_binding,
    )
    monkeypatch.setattr(
        ptg_control,
        "mark_control_run",
        harness.has_claimed_control_run,
    )
    monkeypatch.setattr(
        ptg_control,
        "_flush_terminal_status_events",
        harness.flush_status,
    )
    monkeypatch.setattr(ptg_control, "ptg_main", harness.run_main)
    params_by_name = _direct_params()

    with pytest.raises(
        FrozenRateFileBindingMismatchError,
        match="immutable retry drift",
    ):
        await ptg_control.ptg_control_start(
            {},
            {
                "run_id": "run-direct",
                "source_file_import_id": params_by_name[
                    "source_file_import_id"
                ],
                "import_id": params_by_name["import_id"],
                "params": params_by_name,
            },
        )

    assert harness.main_calls == []
    assert [call["status"] for call in harness.mark_calls] == [
        "running",
        "failed",
    ]


@pytest.mark.asyncio
async def test_worker_outer_identity_mismatch_is_terminal_inside_lifecycle(
    monkeypatch,
):
    """An outer/nested ID mismatch must stop before worker admission."""

    harness = _WorkerFailureHarness()
    monkeypatch.setattr(
        ptg_control,
        "_stale_ptg_job_result",
        harness.no_stale_run,
    )
    monkeypatch.setattr(
        ptg_frozen_control,
        "recheck_frozen_binding",
        harness.reject_binding,
    )
    monkeypatch.setattr(
        ptg_control,
        "mark_control_run",
        harness.has_claimed_control_run,
    )
    monkeypatch.setattr(
        ptg_control,
        "_flush_terminal_status_events",
        harness.flush_status,
    )
    monkeypatch.setattr(ptg_control, "ptg_main", harness.run_main)
    request_payload_by_name = control._validated_control_import_payload(
        protected_control_payload()
    )

    lifecycle_response = await ptg_control.ptg_control_start(
        {},
        {
            "run_id": "run-protected",
            "source_file_import_id": "drifted-source-file-import",
            "import_id": request_payload_by_name["import_id"],
            "params": request_payload_by_name["params"],
        },
    )

    assert lifecycle_response == {
        "status": "skipped",
        "run_id": "run-protected",
        "reason": "source_attempt_identity_mismatch",
    }
    assert harness.main_calls == []
    assert harness.mark_calls == []


class _RejectedClaimHarness:
    def __init__(self) -> None:
        self.event_names: list[str] = []

    async def no_stale_run(self, _run_id):
        return None

    async def is_attempt_claimed(self, *_args, **_kwargs):
        self.event_names.append("claim")
        return False

    async def fail_if_validated(self, *_args, **_kwargs):
        raise AssertionError("a rejected attempt must not validate")

    def fail_if_heartbeat_starts(self, *_args, **_kwargs):
        raise AssertionError("a rejected attempt must not start a heartbeat")


def _install_rejected_claim_harness(
    monkeypatch,
    harness: _RejectedClaimHarness,
) -> None:
    monkeypatch.setattr(
        ptg_control,
        "_stale_ptg_job_result",
        harness.no_stale_run,
    )
    monkeypatch.setattr(
        ptg_control,
        "mark_control_run",
        harness.is_attempt_claimed,
    )
    monkeypatch.setattr(
        ptg_control,
        "validated_worker_frozen_rate_params",
        harness.fail_if_validated,
    )
    monkeypatch.setattr(
        ptg_control,
        "_start_threaded_ptg_heartbeat",
        harness.fail_if_heartbeat_starts,
    )


@pytest.mark.asyncio
async def test_worker_rejected_claim_skips_before_frozen_validation(
    monkeypatch,
):
    """A losing attempt must not validate, lease, select a lane, or run."""

    harness = _RejectedClaimHarness()
    _install_rejected_claim_harness(monkeypatch, harness)
    request_payload_by_name = control._validated_control_import_payload(
        protected_control_payload()
    )

    control_result = await ptg_control.ptg_control_start(
        {},
        {
            "run_id": "run-protected",
            "source_file_import_id": request_payload_by_name[
                "source_file_import_id"
            ],
            "import_id": request_payload_by_name["import_id"],
            "params": request_payload_by_name["params"],
        },
    )

    assert control_result == {
        "status": "skipped",
        "run_id": "run-protected",
        "reason": "newer_attempt_active",
    }
    assert harness.event_names == ["claim"]


class _DirectEngineOrderHarness:
    def __init__(
        self,
        expected_import_run_id: str = "ptg2:source-file-import-001",
    ) -> None:
        self.event_names: list[str] = []
        self.expected_import_run_id = expected_import_run_id

    async def ensure_database(self, _test_mode):
        self.event_names.append("database")

    async def ensure_tables(self):
        self.event_names.append("schema")

    @asynccontextmanager
    async def source_lock(self, _source_key):
        self.event_names.append("source_lock")
        try:
            yield
        finally:
            self.event_names.append("source_unlock")

    async def bind(self, params_by_name):
        self.event_names.append("binding_cas")
        assert params_by_name["source_file_import_id"] == (
            "source-file-import-001"
        )
        assert (
            ptg.current_live_progress_context()["import_run_id"]
            == self.expected_import_run_id
        )

    async def stop_at_snapshot_lookup(self, _source_key):
        self.event_names.append("snapshot_lookup")
        raise RuntimeError("stop after ordering proof")


@pytest.mark.asyncio
async def test_direct_engine_binding_precedes_snapshot_lookup(monkeypatch):
    """The engine CAS must follow schema and source lock, then precede lookup."""

    harness = _DirectEngineOrderHarness()
    request_payload_by_name = control._validated_control_import_payload(
        protected_control_payload()
    )
    monkeypatch.setattr(ptg, "ensure_database", harness.ensure_database)
    monkeypatch.setattr(
        ptg, "require_ptg2_runtime_schema_ready", harness.ensure_tables
    )
    monkeypatch.setattr(
        ptg,
        "_ptg2_source_import_lock",
        harness.source_lock,
    )
    monkeypatch.setattr(
        ptg,
        "insert_or_compare_frozen_binding_transaction",
        harness.bind,
    )
    monkeypatch.setattr(
        ptg,
        "_current_source_snapshot_id",
        harness.stop_at_snapshot_lookup,
    )
    params_by_name = request_payload_by_name["params"]

    with pytest.raises(RuntimeError, match="stop after ordering proof"):
        await ptg._main_with_artifact_lease(
            test_mode=True,
            source_file_import_id=request_payload_by_name[
                "source_file_import_id"
            ],
            import_id=request_payload_by_name["import_id"],
            source_key=params_by_name["source_key"],
            import_month=params_by_name["import_month"],
            plan_ids=params_by_name["plan_ids"],
            plan_market_types=params_by_name["plan_market_types"],
            frozen_rate_file_set_contract=params_by_name[
                "frozen_rate_file_set_contract"
            ],
            frozen_rate_files=params_by_name["frozen_rate_files"],
            frozen_rate_file_set_sha256=params_by_name[
                "frozen_rate_file_set_sha256"
            ],
            frozen_rate_file_count=params_by_name[
                "frozen_rate_file_count"
            ],
        )

    assert harness.event_names == [
        "database",
        "schema",
        "source_lock",
        "binding_cas",
        "snapshot_lookup",
        "source_unlock",
    ]


@pytest.mark.asyncio
async def test_anonymous_import_skips_frozen_binding_store(monkeypatch):
    """Imports without a control identity need no frozen-binding lookup."""

    harness = _DirectEngineOrderHarness()
    monkeypatch.setattr(ptg, "ensure_database", harness.ensure_database)
    monkeypatch.setattr(
        ptg, "require_ptg2_runtime_schema_ready", harness.ensure_tables
    )
    monkeypatch.setattr(
        ptg,
        "_ptg2_source_import_lock",
        harness.source_lock,
    )
    monkeypatch.setattr(
        ptg,
        "insert_or_compare_frozen_binding_transaction",
        pytest.fail,
    )
    monkeypatch.setattr(
        ptg,
        "_current_source_snapshot_id",
        harness.stop_at_snapshot_lookup,
    )

    with pytest.raises(RuntimeError, match="stop after ordering proof"):
        await ptg._main_with_artifact_lease(
            test_mode=True,
            import_id="markerless-import",
            source_key="markerless-source",
            import_month="2026-07",
        )

    assert harness.event_names == [
        "database",
        "schema",
        "source_lock",
        "snapshot_lookup",
        "source_unlock",
    ]


@pytest.mark.asyncio
async def test_markerless_control_import_checks_frozen_binding_store(
    monkeypatch,
):
    """A control identity still checks for prohibited frozen downgrades."""

    harness = _DirectEngineOrderHarness(
        expected_import_run_id="ptg2:markerless_import",
    )
    monkeypatch.setattr(ptg, "ensure_database", harness.ensure_database)
    monkeypatch.setattr(
        ptg, "require_ptg2_runtime_schema_ready", harness.ensure_tables
    )
    monkeypatch.setattr(
        ptg,
        "_ptg2_source_import_lock",
        harness.source_lock,
    )
    monkeypatch.setattr(
        ptg,
        "insert_or_compare_frozen_binding_transaction",
        harness.bind,
    )
    monkeypatch.setattr(
        ptg,
        "_current_source_snapshot_id",
        harness.stop_at_snapshot_lookup,
    )

    with pytest.raises(RuntimeError, match="stop after ordering proof"):
        await ptg._main_with_artifact_lease(
            test_mode=True,
            source_file_import_id="source-file-import-001",
            import_id="markerless-import",
            source_key="markerless-source",
            import_month="2026-07",
        )

    assert harness.event_names == [
        "database",
        "schema",
        "source_lock",
        "binding_cas",
        "snapshot_lookup",
        "source_unlock",
    ]
