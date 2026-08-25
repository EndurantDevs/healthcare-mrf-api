# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Reviewer-facing margin for frozen multipart control boundaries."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import control_imports
from process import control_lifecycle
from process.ptg_parts import frozen_rate_candidate_sources
from process.ptg_parts import frozen_rate_privacy
from process.ptg_parts import frozen_rate_runtime
from process.ptg_parts.frozen_rate_files import FrozenRateFileMismatchError
from tests.ptg_frozen_test_support import (
    frozen_descriptor_by_ordinal,
)


def test_private_projection_handles_nested_sequence_and_missing_count():
    """Strip nested evidence while preserving the public container shape."""

    private_url = "https://rates.example.test/private.json.gz"
    event_payload_by_field = {
        "params": {
            "nested": (
                {
                    "frozen_rate_files": [
                        {
                            "canonical_url": private_url,
                            "raw_sha256": "a" * 64,
                        }
                    ]
                },
            )
        },
        "metrics": {"message": f"processed {private_url}"},
    }

    assert frozen_rate_privacy.has_frozen_private_evidence(
        (event_payload_by_field,)
    )
    private_values = frozen_rate_privacy.frozen_private_scalar_values(
        event_payload_by_field
    )
    projected_event = frozen_rate_privacy.project_frozen_status_event(
        event_payload_by_field
    )

    assert private_url in private_values
    assert projected_event["params"] == {
        "nested": ({},),
        "frozen_rate_file_set_protected": True,
    }
    assert projected_event["metrics"] == {
        "message": "[protected frozen source]",
        "frozen_rate_file_set_protected": True,
    }
    assert frozen_rate_privacy.has_frozen_private_evidence(["ordinary"]) is False


def test_private_projection_finds_count_inside_list_and_tuple():
    """Carry only the bounded count through nested list and tuple payloads."""

    event_payload_by_field = {
        "params": {
            "items": [
                {
                    "frozen_rate_file_count": 2,
                    "frozen_rate_file_set_contract": "private-contract",
                    "canonical_url": "private-contract",
                }
            ]
        },
        "progress": (
            {"message": "private-contract"},
            {"message": "safe"},
        ),
    }

    projected_event = frozen_rate_privacy.project_frozen_status_event(
        event_payload_by_field
    )

    assert projected_event["params"]["frozen_rate_file_count"] == 2
    assert projected_event["params"]["items"] == [
        {
            "frozen_rate_file_count": 2,
            "canonical_url": "[protected frozen source]",
        }
    ]
    assert projected_event["progress"] == (
        {"message": "[protected frozen source]"},
        {"message": "safe"},
    )


def test_normalize_run_strips_marker_only_frozen_evidence(monkeypatch):
    """Prevent marker-only evidence from leaking through the public run route."""

    monkeypatch.setattr(
        control_imports,
        "_overlay_live_progress",
        lambda serialized_run: serialized_run,
    )
    normalized_run = control_imports.normalize_run(
        {
            "run_id": "run-marker-only",
            "importer": "ptg",
            "params": {
                "frozen_rate_file_set_contract": "private-contract",
                "frozen_rate_file_count": 1,
            },
            "metrics": {
                "frozen_rate_file_set_contract": "private-contract",
            },
        }
    )

    assert normalized_run["params"] == {
        "frozen_rate_file_count": 1,
        "frozen_rate_file_set_protected": True,
    }
    assert normalized_run["metrics"] == {}
    assert "private-contract" not in str(normalized_run)


def test_ordinary_status_event_is_returned_without_projection():
    """Leave a status event untouched when no frozen namespace is present."""

    status_event_by_field = {
        "status": "running",
        "progress": {"done": 1},
    }
    assert frozen_rate_privacy.project_frozen_status_event(
        status_event_by_field
    ) == status_event_by_field


def test_candidate_identity_rejects_missing_and_untyped_database_evidence():
    """Reject missing proof rows and non-strict live source field types."""

    descriptor_by_field = frozen_descriptor_by_ordinal(1)
    with pytest.raises(FrozenRateFileMismatchError, match="source evidence changed"):
        frozen_rate_candidate_sources._database_source_version_identity({}, None)
    with pytest.raises(FrozenRateFileMismatchError, match="source evidence changed"):
        frozen_rate_candidate_sources._database_source_version_identity(
            {"version_content_length": True},
            {
                "raw_byte_count": descriptor_by_field["content_length"],
                "logical_hash_deferred": False,
            },
        )
    with pytest.raises(FrozenRateFileMismatchError, match="ambiguous"):
        frozen_rate_candidate_sources._expected_source_version_identity(
            descriptor_by_field,
            {},
        )


def _candidate_source_row(descriptor_by_field):
    """Build one exact live database-source identity row."""

    return {
        "source_key": 0,
        "source_file_version_count": 1,
        "source_file_version_id": descriptor_by_field[
            "engine_source_file_version_id"
        ],
        "raw_container_sha256": descriptor_by_field["raw_sha256"],
        "version_raw_sha256": descriptor_by_field["raw_sha256"],
        "version_source_identity_hash": descriptor_by_field[
            "engine_source_identity_hash"
        ],
        "version_source_type": descriptor_by_field["source_type"],
        "version_canonical_url": descriptor_by_field["canonical_url"],
        "version_logical_sha256": descriptor_by_field["logical_sha256"],
        "version_logical_hash_deferred": descriptor_by_field[
            "logical_hash_deferred"
        ],
        "version_content_length": descriptor_by_field["content_length"],
        "version_etag": descriptor_by_field["etag"],
        "version_last_modified": descriptor_by_field["last_modified"],
        "version_verification_mode": "downloaded",
        "version_payload": {
            "raw_byte_count": descriptor_by_field["content_length"],
            "logical_hash_deferred": descriptor_by_field[
                "logical_hash_deferred"
            ],
        },
    }


def test_candidate_database_source_set_is_dense_unique_and_exact():
    """Require dense source ordinals and unique exact byte identities."""

    descriptor_by_field = frozen_descriptor_by_ordinal(1)
    proof_by_version_id = {
        descriptor_by_field["engine_source_file_version_id"]: {
            "verification_mode": "downloaded",
        }
    }
    source_row_by_field = _candidate_source_row(descriptor_by_field)

    frozen_rate_candidate_sources.validate_frozen_candidate_database_sources(
        [source_row_by_field],
        [descriptor_by_field],
        proof_by_version_id,
    )
    with pytest.raises(FrozenRateFileMismatchError, match="cardinality"):
        frozen_rate_candidate_sources.validate_frozen_candidate_database_sources(
            [],
            [descriptor_by_field],
            proof_by_version_id,
        )
    with pytest.raises(FrozenRateFileMismatchError, match="evidence changed"):
        frozen_rate_candidate_sources.validate_frozen_candidate_database_sources(
            [{**source_row_by_field, "source_key": True}],
            [descriptor_by_field],
            proof_by_version_id,
        )
    with pytest.raises(FrozenRateFileMismatchError, match="evidence changed"):
        frozen_rate_candidate_sources.validate_frozen_candidate_database_sources(
            [{**source_row_by_field, "source_key": 1}],
            [descriptor_by_field],
            proof_by_version_id,
        )
    assert frozen_rate_candidate_sources._mapping("not-a-mapping") is None


def test_runtime_rejects_observed_logical_hash_drift_for_deferred_input():
    """Bind deferred logical identity to the observed raw-container alias."""

    descriptor_by_field = frozen_descriptor_by_ordinal(1)
    descriptor_by_field["logical_hash_deferred"] = True
    descriptor_by_field["logical_sha256"] = None
    summary_by_field = {
        **descriptor_by_field,
        "logical_sha256": "f" * 64,
        "raw_byte_count": descriptor_by_field["content_length"],
        "verification_mode": "head_get_streamed_gzip_integrity",
    }

    with pytest.raises(FrozenRateFileMismatchError, match="logical_sha256"):
        frozen_rate_runtime._frozen_result_proof(
            descriptor_by_field,
            {"source_type": "in_network", "summary": summary_by_field},
        )


@pytest.mark.asyncio
async def test_control_wrapper_cleans_context_when_attempt_claim_raises(
    monkeypatch,
):
    """Reset wrapper-owned progress when the database claim raises."""

    reset_token_list = []
    monkeypatch.setattr(
        control_lifecycle,
        "set_live_progress_context",
        lambda **_payload: "live-token",
    )
    monkeypatch.setattr(
        control_lifecycle,
        "reset_live_progress_context",
        reset_token_list.append,
    )
    monkeypatch.setattr(
        control_lifecycle,
        "mark_control_run",
        AsyncMock(side_effect=RuntimeError("claim unavailable")),
    )

    with pytest.raises(RuntimeError, match="claim unavailable"):
        await control_lifecycle.control_single_job_start(
            {},
            {
                "run_id": "run-claim-error",
                "target_module": "unused.module",
                "target_function": "unused",
            },
        )

    assert reset_token_list == ["live-token"]


@pytest.mark.asyncio
async def test_control_wrapper_records_missing_target_before_raising(
    monkeypatch,
):
    """Publish a deterministic failure before rejecting a missing target."""

    lifecycle_update_list = []
    reset_token_list = []

    async def is_transition_recorded(_run_id, **fields_by_name):
        lifecycle_update_list.append(fields_by_name)
        return True

    monkeypatch.setattr(
        control_lifecycle,
        "set_live_progress_context",
        lambda **_payload: "live-token",
    )
    monkeypatch.setattr(
        control_lifecycle,
        "reset_live_progress_context",
        reset_token_list.append,
    )
    monkeypatch.setattr(
        control_lifecycle,
        "mark_control_run",
        is_transition_recorded,
    )
    terminal_flush = AsyncMock()
    monkeypatch.setattr(
        control_lifecycle,
        "_flush_terminal_status_events",
        terminal_flush,
    )

    with pytest.raises(RuntimeError, match="target_module"):
        await control_lifecycle.control_single_job_start(
            {},
            {"run_id": "run-missing-target"},
        )

    assert [
        update_by_field["status"]
        for update_by_field in lifecycle_update_list
    ] == ["running", "failed"]
    assert lifecycle_update_list[-1]["error"]["code"] == "control_target_missing"
    terminal_flush.assert_awaited_once()
    assert reset_token_list == ["live-token"]


@pytest.mark.asyncio
async def test_control_wrapper_requires_declared_shutdown(monkeypatch):
    """Fail a shutdown-enabled wrapper whose module has no shutdown hook."""

    lifecycle_update_list = []

    async def control_target(_context, _task_by_field):
        return {"rows": 1}

    async def is_transition_recorded(_run_id, **fields_by_name):
        lifecycle_update_list.append(fields_by_name)
        return True

    monkeypatch.setenv(
        "HLTHPRT_IMPORT_LIVE_PROGRESS_HEARTBEAT_SECONDS",
        "0",
    )
    monkeypatch.setattr(
        control_lifecycle,
        "mark_control_run",
        is_transition_recorded,
    )
    monkeypatch.setattr(
        control_lifecycle,
        "import_module",
        lambda _name: SimpleNamespace(process_data=control_target),
    )
    monkeypatch.setattr(
        control_lifecycle,
        "_flush_terminal_status_events",
        AsyncMock(),
    )

    with pytest.raises(RuntimeError, match="does not expose shutdown"):
        await control_lifecycle.control_single_job_start(
            {},
            {
                "run_id": "run-missing-shutdown",
                "target_module": "fake.module",
                "target_function": "process_data",
                "run_shutdown": True,
            },
        )

    assert [
        update_by_field["status"]
        for update_by_field in lifecycle_update_list
    ] == ["running", "failed"]


@pytest.mark.asyncio
async def test_control_helpers_filter_kwargs_and_terminal_shapes(monkeypatch):
    """Filter kwargs and shape scalar terminal progress deterministically."""

    accepted_value_list = []

    async def control_target(*, accepted):
        accepted_value_list.append(accepted)
        return accepted

    returned_value = await control_lifecycle._invoke_control_target(
        SimpleNamespace(target=control_target),
        target_function="target",
        call_style="kwargs",
        control_context={},
        target_task_by_field={"accepted": 7, "ignored": 9},
    )

    assert returned_value == 7
    assert accepted_value_list == [7]
    assert control_lifecycle._terminal_progress_from_result("target", 3)["done"] == 3
    assert control_lifecycle._terminal_progress_from_result("target", object()) is None
    monkeypatch.setenv(
        "HLTHPRT_IMPORT_STATUS_EVENT_TERMINAL_FLUSH_SECONDS",
        "0",
    )
    await control_lifecycle._flush_terminal_status_events("run")


@pytest.mark.asyncio
async def test_control_without_run_id_skips_ownership_and_heartbeat(monkeypatch):
    """Invoke an unregistered control target without lifecycle ownership."""

    lifecycle_update_list = []

    async def is_transition_recorded(run_id, **fields_by_name):
        lifecycle_update_list.append((run_id, fields_by_name["status"]))
        return True

    async def control_target(_context, task_by_field):
        return {"rows": len(task_by_field)}

    monkeypatch.setattr(
        control_lifecycle,
        "mark_control_run",
        is_transition_recorded,
    )
    monkeypatch.setattr(
        control_lifecycle,
        "import_module",
        lambda _name: SimpleNamespace(process_data=control_target),
    )

    outcome_by_field = await control_lifecycle.control_single_job_start(
        {},
        {
            "target_module": "fake.module",
            "target_function": "process_data",
        },
    )

    assert outcome_by_field["status"] == "succeeded"
    assert lifecycle_update_list == [("", "running"), ("", "succeeded")]


@pytest.mark.asyncio
async def test_heartbeat_publishes_only_after_database_ownership(monkeypatch):
    """Publish a heartbeat only after its matching attempt update succeeds."""

    sleep_count_list = [0]
    heartbeat_event_list = []

    async def finish_after_one_tick(_interval):
        sleep_count_list[0] += 1
        if sleep_count_list[0] > 1:
            raise asyncio.CancelledError

    monkeypatch.setenv(
        "HLTHPRT_IMPORT_LIVE_PROGRESS_HEARTBEAT_SECONDS",
        "1",
    )
    monkeypatch.setattr(
        control_lifecycle.asyncio,
        "sleep",
        finish_after_one_tick,
    )
    monkeypatch.setattr(
        control_lifecycle,
        "_is_control_run_heartbeat_persisted",
        AsyncMock(return_value=True),
    )
    monkeypatch.setattr(
        control_lifecycle,
        "enqueue_live_progress",
        lambda **event_by_field: heartbeat_event_list.append(event_by_field),
    )

    with pytest.raises(asyncio.CancelledError):
        await control_lifecycle._live_progress_heartbeat(
            "run-heartbeat",
            "ptg",
            "ptg_control_start",
            "2026-07-28T00:00:00+00:00",
        )

    assert heartbeat_event_list[0]["source"] == "engine-heartbeat"
