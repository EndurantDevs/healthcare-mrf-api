"""Reusable evidence builders for PTG V4 canary acceptance tests."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from scripts import ptg_v4_dev_canary_cold_capture as cold_capture
from scripts.ptg_v4_dev_canary_progress import PROGRESS_EVIDENCE_CONTRACT
from scripts.ptg_v4_dev_canary_support import (
    BoundedApiSpec,
    ElapsedBudget,
)


def elapsed_budget() -> ElapsedBudget:
    """Return the smallest deterministic budget used by timeline tests."""

    return ElapsedBudget(
        compressed_input_bytes=0,
        component_fact_count=0,
        fixed_seconds=60,
        seconds_per_input_gib=0,
        seconds_per_million_component_facts=0,
    )


def active_progress(
    *,
    observed_at: str,
    progressed_at: str,
    pct: float,
    progress_seq: int,
    **progress_overrides: object,
) -> dict[str, object]:
    """Build one sequenced, nonterminal progress sample."""

    progress_by_field = {
        "observed_at": observed_at,
        "progressed_at": progressed_at,
        "pct": pct,
        "done": int(pct),
        "total": 100,
        "unit": "items",
        "stage_id": "scan",
        "stage_ordinal": 1,
        "event_seq": progress_seq,
        "progress_seq": progress_seq,
        "compiler_phase": "read",
        **progress_overrides,
    }
    return {
        "observed_at": observed_at,
        "status": "running",
        "progress": progress_by_field,
    }


def terminal_progress(*, pct: float = 100) -> dict[str, object]:
    """Build one successful terminal progress sample."""

    return {
        "observed_at": "2026-07-23T00:00:10Z",
        "status": "succeeded",
        "progress": {
            "unit": "run",
            "done": 1,
            "total": 1,
            "pct": pct,
        },
    }


def timeline(progress_samples: list[dict[str, object]]) -> dict[str, object]:
    """Wrap progress samples in the persisted monitor contract."""

    return {
        "contract": PROGRESS_EVIDENCE_CONTRACT,
        "poll_interval_seconds": 5,
        "run": {
            "status": "succeeded",
            "started_at": "2026-07-23T00:00:00Z",
            "finished_at": "2026-07-23T00:00:10Z",
        },
        "samples": progress_samples,
    }


def successful_progress_samples() -> list[dict[str, object]]:
    """Return two active updates followed by terminal success."""

    return [
        active_progress(
            observed_at="2026-07-23T00:00:03Z",
            progressed_at="2026-07-23T00:00:03Z",
            pct=20,
            progress_seq=1,
        ),
        active_progress(
            observed_at="2026-07-23T00:00:07Z",
            progressed_at="2026-07-23T00:00:07Z",
            pct=70,
            progress_seq=2,
        ),
        terminal_progress(),
    ]


def bounded_spec(*, minimum_cold_samples: int = 20) -> BoundedApiSpec:
    """Build the fixed public CPT acceptance specification."""

    return BoundedApiSpec(
        snapshot_id="snapshot-1",
        npi=None,
        code_system="CPT",
        code="70553",
        limit=25,
        expected_item_count=1,
        expected_result_state="matched",
        cold_p95_limit_ms=50,
        warm_p95_limit_ms=50,
        minimum_cold_process_samples=minimum_cold_samples,
    )


def api_document() -> dict[str, object]:
    """Return one valid V4 response document."""

    return {
        "result_state": "matched",
        "items": [{"reported_code": "70553"}],
        "provenance": {
            "snapshot_id": "snapshot-1",
            "storage_generation": "shared_blocks_v4",
        },
    }


def cold_samples(sample_count: int) -> list[dict[str, object]]:
    """Build fresh-process persisted cold evidence."""

    return [
        {
            "process_identity": f"pod-{sample_index}",
            "process_started_at": f"2026-07-23T00:00:{sample_index:02d}Z",
            "image_identity": "image-v4",
            "snapshot_id": "snapshot-1",
            "first_v4_request": True,
            "document_valid": True,
            "graph_read_evidence": {
                "passed": True,
                "failures": [],
                "runtime_identity": {
                    "process_identity": f"pod-{sample_index}",
                    "process_started_at": (
                        f"2026-07-23T00:00:{sample_index:02d}Z"
                    ),
                    "image_identity": "image-v4",
                },
            },
            "latency_ms": 20 + sample_index,
        }
        for sample_index in range(sample_count)
    ]


def cold_capture_arguments() -> SimpleNamespace:
    """Return arguments for one isolated public cold-sample attempt."""

    return SimpleNamespace(
        snapshot_id="snapshot-1",
        page_limit=25,
        expected_item_count=1,
        minimum_cold_process_samples=20,
        code_system="CPT",
        code="70553",
        cold_p95_limit_ms=50,
        warm_p95_limit_ms=50,
        api_param=[],
        maximum_database_bytes=100,
        maximum_database_blocks=100,
        maximum_logical_lookups=100,
        component_fallback_mode="allowed",
        request_timeout_seconds=2,
        maximum_response_bytes=1024,
        output="unused.json",
        case_name="public",
        reference_evidence="reference.json",
    )


def configure_invalid_cold_capture(
    monkeypatch: Any, writes: list[object]
) -> None:
    """Make cold capture observe a semantic mismatch without touching disk."""

    runtime_identity_by_field = {
        "process_identity": "pod-1",
        "process_started_at": "2026-07-23T00:00:00Z",
        "image_identity": "image-1",
    }
    metric_snapshot_by_field = {
        "request_count": 0,
        "database_bytes": 0,
        "database_blocks": 0,
        "logical_lookups": 0,
        "component_fallbacks": 0,
    }

    async def fake_metrics(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        return {
            **metric_snapshot_by_field,
            "runtime_identity": dict(runtime_identity_by_field),
        }

    async def fake_identity_json(
        *_args: Any,
        **_kwargs: Any,
    ) -> tuple[dict[str, Any], float, dict[str, Any]]:
        return (
            {"result_state": "missing", "items": []},
            12.0,
            dict(runtime_identity_by_field),
        )

    monkeypatch.setattr(
        cold_capture,
        "service_inputs",
        lambda _args: ("https://api", "https://metrics", {}, {}),
    )
    monkeypatch.setattr(cold_capture, "get_metrics", fake_metrics)
    monkeypatch.setattr(cold_capture, "get_identity_json", fake_identity_json)
    monkeypatch.setattr(
        cold_capture,
        "load_json",
        lambda _path: {"page_digest": "reference"},
    )
    monkeypatch.setattr(
        cold_capture,
        "validate_reference_evidence",
        lambda _reference: [],
    )
    monkeypatch.setattr(
        cold_capture,
        "compare_v4_document_to_reference",
        lambda *_args: (["semantic mismatch"], {"page_digest": "actual"}),
    )
    monkeypatch.setattr(
        cold_capture, "write_json", lambda *_args: writes.append(object())
    )
