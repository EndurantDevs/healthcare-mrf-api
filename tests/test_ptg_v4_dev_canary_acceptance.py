# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from types import SimpleNamespace

import pytest

from scripts import ptg_v4_dev_canary_acceptance as acceptance
from scripts import ptg_v4_dev_canary_cold_capture as cold_capture
from scripts.ptg_v4_dev_canary_progress import (
    evaluate_progress_timeline,
)
from scripts.ptg_v4_dev_canary_support import (
    GraphReadLimits,
    evaluate_api_probe,
    evaluate_graph_metric_delta,
    parse_graph_metrics,
)
from tests.ptg_v4_dev_canary_acceptance_support import (
    active_progress as _active_progress,
    api_document as _api_document,
    bounded_spec as _bounded_spec,
    cold_capture_arguments,
    cold_samples as _cold_samples,
    configure_invalid_cold_capture,
    elapsed_budget as _elapsed_budget,
    successful_progress_samples as _successful_progress_samples,
    terminal_progress as _terminal_progress,
    timeline as _timeline,
)


@pytest.mark.asyncio
async def test_monitor_uses_live_progress_from_exact_control_route(
    monkeypatch,
) -> None:
    observed_urls: list[str] = []

    async def fake_get_json(_client, url, **_kwargs):
        observed_urls.append(url)
        return (
            {
                "run_id": "run_abc",
                "status": "succeeded",
                "started_at": "2026-07-23T00:00:00Z",
                "finished_at": "2026-07-23T00:00:10Z",
                "snapshot_id": "snapshot-1",
                "metrics": {
                    "snapshot_id": "snapshot-1",
                    "import_run_id": "ptg2:import-1",
                },
                "progress": {"unit": "run", "done": 1, "total": 1, "pct": 100},
                "normalized_progress": {"pct": 7},
            },
            1.0,
        )

    monkeypatch.setattr(acceptance, "_get_json", fake_get_json)
    monkeypatch.setattr(acceptance, "_write_json", lambda *_args: None)
    arguments = SimpleNamespace(
        control_base_url="https://control.test",
        allow_insecure_http=False,
        poll_interval_seconds=5,
        control_header_env=[],
        control_run_id="run_abc",
        maximum_monitor_seconds=30,
        request_timeout_seconds=2,
        output="unused.json",
    )

    evidence = await acceptance._monitor_progress(arguments)

    assert observed_urls == ["https://control.test/control/v1/imports/run_abc"]
    assert evidence["samples"][0]["progress"]["pct"] == 100
    assert "normalized_progress" not in evidence["samples"][0]
    assert evidence["run"]["terminal_identity"] == {
        "control_run_id": "run_abc",
        "snapshot_id": "snapshot-1",
        "import_run_id": "ptg2:import-1",
    }


def test_progress_accepts_unsequenced_terminal_database_summary() -> None:
    report = evaluate_progress_timeline(
        _timeline(_successful_progress_samples()),
        budget=_elapsed_budget(),
        maximum_update_gap_seconds=5,
    )

    assert report["passed"] is True
    assert report["distinct_progress_update_count"] == 2
    assert report["final_progress"] == {
        "unit": "run",
        "done": 1,
        "total": 1,
        "pct": 100,
    }


def test_progress_allows_done_reset_only_across_phase_context() -> None:
    samples = _successful_progress_samples()
    samples[0]["progress"]["done"] = 10
    samples[0]["progress"]["total"] = 10
    samples[1]["progress"].update(
        {
            "done": 1,
            "total": 20,
            "stage_id": "encode",
            "stage_ordinal": 2,
            "compiler_phase": "write",
        }
    )

    report = evaluate_progress_timeline(
        _timeline(samples),
        budget=_elapsed_budget(),
        maximum_update_gap_seconds=5,
    )

    assert report["passed"] is True


def test_progress_rejects_heartbeat_only_stall() -> None:
    first_sample = _successful_progress_samples()[0]
    repeated_sample_by_field = {
        **first_sample,
        "observed_at": "2026-07-23T00:00:07Z",
        "progress": {
            **first_sample["progress"],
            "observed_at": "2026-07-23T00:00:07Z",
            "event_seq": 2,
        },
    }
    report = evaluate_progress_timeline(
        _timeline([first_sample, repeated_sample_by_field, _terminal_progress()]),
        budget=_elapsed_budget(),
        maximum_update_gap_seconds=5,
    )

    assert report["passed"] is False
    assert "progress timeline has too few distinct updates" in report["failures"]


def test_progress_rejects_terminal_status_without_terminal_100() -> None:
    samples = _successful_progress_samples()
    samples[-1] = _terminal_progress(pct=0)

    report = evaluate_progress_timeline(
        _timeline(samples),
        budget=_elapsed_budget(),
        maximum_update_gap_seconds=5,
    )

    assert report["passed"] is False
    assert "terminal control progress did not reach 100 percent" in report["failures"]


def test_runtime_identity_must_match_api_and_both_metric_samples() -> None:
    identity_by_field = {
        "process_identity": "a" * 64,
        "process_started_at": "2026-07-23T00:00:00Z",
        "image_identity": "dev-main-example",
    }

    assert acceptance._require_same_runtime_identity(
        identity_by_field,
        dict(identity_by_field),
        dict(identity_by_field),
    ) == identity_by_field
    with pytest.raises(
        acceptance.CanaryInfrastructureError,
        match="different runtime processes",
    ):
        acceptance._require_same_runtime_identity(
            identity_by_field,
            {**identity_by_field, "process_identity": "b" * 64},
            identity_by_field,
        )


@pytest.mark.asyncio
async def test_record_cold_sample_never_persists_invalid_response(
    monkeypatch,
) -> None:
    """Reject a semantic mismatch before the cold evidence file is written."""

    writes: list[object] = []
    configure_invalid_cold_capture(monkeypatch, writes)

    with pytest.raises(
        acceptance.CanaryInfrastructureError,
        match="not a valid first V4 request",
    ):
        await cold_capture.record_cold_sample(cold_capture_arguments())

    assert writes == []


def test_public_probe_requires_at_least_twenty_unique_cold_processes() -> None:
    report = evaluate_api_probe(
        spec=_bounded_spec(),
        cold_sample_evidence=_cold_samples(19),
        warm_samples_ms=[10, 11, 12],
        response_documents=[_api_document()],
        graph_read_evidence={"passed": True, "failures": []},
    )

    assert report["passed"] is False
    assert (
        "cold p95 lacks the required unique fresh-process sample count"
        in report["failures"]
    )


def test_public_probe_reports_cold_and_warm_p95_with_unique_processes() -> None:
    report = evaluate_api_probe(
        spec=_bounded_spec(),
        cold_sample_evidence=_cold_samples(20),
        warm_samples_ms=[10, 11, 12, 13, 14],
        response_documents=[_api_document()],
        graph_read_evidence={"passed": True, "failures": []},
    )

    assert report["passed"] is True
    assert report["cold"]["sample_count"] == 20
    assert report["cold"]["unique_process_count"] == 20
    assert report["cold"]["p95_ms"] == 38
    assert report["warm"]["p95_ms"] == 14


def test_public_probe_rejects_poisoned_persisted_cold_evidence() -> None:
    cold_sample_evidence = _cold_samples(20)
    cold_sample_evidence[-1]["document_valid"] = False
    cold_sample_evidence[-2]["graph_read_evidence"] = {
        "passed": False,
        "failures": ["too many bytes"],
    }

    report = evaluate_api_probe(
        spec=_bounded_spec(),
        cold_sample_evidence=cold_sample_evidence,
        warm_samples_ms=[10, 11, 12],
        response_documents=[_api_document()],
        graph_read_evidence={"passed": True, "failures": []},
    )

    assert report["passed"] is False
    assert (
        "cold sample lacks valid fresh-process response and graph evidence"
        in report["failures"]
    )


@pytest.mark.parametrize(
    "missing_field",
    ("process_identity", "process_started_at", "image_identity"),
)
def test_public_probe_rejects_cold_sample_without_metrics_identity(
    missing_field: str,
) -> None:
    cold_sample_evidence = _cold_samples(20)
    graph_evidence = cold_sample_evidence[-1]["graph_read_evidence"]
    assert isinstance(graph_evidence, dict)
    runtime_identity = graph_evidence["runtime_identity"]
    assert isinstance(runtime_identity, dict)
    del runtime_identity[missing_field]

    report = evaluate_api_probe(
        spec=_bounded_spec(),
        cold_sample_evidence=cold_sample_evidence,
        warm_samples_ms=[10, 11, 12],
        response_documents=[_api_document()],
        graph_read_evidence={"passed": True, "failures": []},
    )

    assert report["passed"] is False
    assert (
        "cold sample API and metrics runtime identities differ"
        in report["failures"]
    )
    assert (
        "cold p95 lacks the required unique fresh-process sample count"
        in report["failures"]
    )


def test_explicit_snapshot_public_probe_requires_exactly_one_graph_scope() -> None:
    limits = GraphReadLimits(
        database_bytes=10_000,
        database_blocks=100,
        logical_lookups=100,
        minimum_request_scopes=1,
        maximum_request_scopes=1,
        component_fallback_mode="allowed",
    )
    metrics_before_by_field = {
        "request_count": 10,
        "database_bytes": 100,
        "database_blocks": 5,
        "logical_lookups": 8,
        "component_fallbacks": 0,
    }
    metrics_after_by_field = {
        "request_count": 12,
        "database_bytes": 200,
        "database_blocks": 6,
        "logical_lookups": 10,
        "component_fallbacks": 0,
    }

    report = evaluate_graph_metric_delta(
        metrics_before_by_field,
        metrics_after_by_field,
        limits=limits,
    )

    assert report["passed"] is False
    assert (
        "direct-pod V4 request_count delta is outside its bound"
        in report["failures"]
    )


def test_public_graph_collection_measures_without_operator_ceiling() -> None:
    limits = GraphReadLimits(
        database_bytes=None,
        database_blocks=None,
        logical_lookups=None,
        minimum_request_scopes=1,
        maximum_request_scopes=1,
        component_fallback_mode="allowed",
    )
    metrics_before_by_field = {
        "request_count": 0,
        "database_bytes": 0,
        "database_blocks": 0,
        "logical_lookups": 0,
        "component_fallbacks": 0,
    }
    metrics_after_by_field = {
        "request_count": 1,
        "database_bytes": 100_000_000,
        "database_blocks": 10_000,
        "logical_lookups": 1_000,
        "component_fallbacks": 0,
    }

    report = evaluate_graph_metric_delta(
        metrics_before_by_field,
        metrics_after_by_field,
        limits=limits,
    )

    assert report["passed"] is True
    assert report["maximums"] == {
        "database_bytes": None,
        "database_blocks": None,
        "logical_lookups": None,
    }


def test_prometheus_parser_reads_exact_runtime_metric_names() -> None:
    payload = "\n".join(
        (
            "hp_mrf_ptg_v4_graph_database_bytes_per_request_count 7",
            "hp_mrf_ptg_v4_graph_database_bytes_per_request_sum 4096",
            "hp_mrf_ptg_v4_graph_database_blocks_total 8",
            "hp_mrf_ptg_v4_graph_logical_lookups_total 9",
            "hp_mrf_ptg_v4_graph_component_fallback_sets_total 1",
            "hp_mrf_ptg_v4_provider_expansion_rate_rows_total 64",
            "hp_mrf_ptg_v4_provider_expansion_provider_sets_total 3",
            "hp_mrf_ptg_v4_provider_expansion_graph_batches_total 2",
            "hp_mrf_ptg_v4_provider_expansion_rejections_total 0",
        )
    )

    assert parse_graph_metrics(payload) == {
        "request_count": 7,
        "database_bytes": 4096,
        "database_blocks": 8,
        "logical_lookups": 9,
        "component_fallbacks": 1,
        "provider_expansion_rate_rows": 64,
        "provider_expansion_provider_sets": 3,
        "provider_expansion_graph_batches": 2,
        "provider_expansion_rejections": 0,
    }


def test_public_parameters_are_bounded_cost_ordered_and_have_no_npi() -> None:
    arguments = SimpleNamespace(
        snapshot_id="snapshot-1",
        page_limit=25,
        expected_item_count=25,
        minimum_cold_process_samples=20,
        code_system="CPT",
        code="70553",
        cold_p95_limit_ms=50,
        warm_p95_limit_ms=50,
        api_param=[],
    )

    parameters = acceptance._api_parameters(
        arguments,
        acceptance._api_spec(arguments),
    )

    assert parameters == {
        "snapshot_id": "snapshot-1",
        "code_system": "CPT",
        "code": "70553",
        "limit": "25",
        "offset": "0",
        "include_providers": "true",
        "order_by": "negotiated_rate",
        "order": "asc",
    }


def test_public_parameters_reject_any_filter_narrowing() -> None:
    arguments = SimpleNamespace(
        snapshot_id="snapshot-1",
        page_limit=25,
        expected_item_count=25,
        minimum_cold_process_samples=20,
        code_system="CPT",
        code="70553",
        cold_p95_limit_ms=50,
        warm_p95_limit_ms=50,
        api_param=["network_name=narrowed"],
    )

    with pytest.raises(
        acceptance.CanaryConfigurationError,
        match="fixed unfiltered CPT probe",
    ):
        acceptance._api_parameters(
            arguments,
            acceptance._api_spec(arguments),
        )
