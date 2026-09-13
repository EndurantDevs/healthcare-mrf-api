# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import pytest

from process.ptg_control_failures import ptg_failure_error
from process.ptg_graph_resource_admission import V4GraphResourceAdmissionError


@pytest.mark.parametrize("grouped", [False, True])
def test_resource_admission_is_terminal_with_numeric_evidence(grouped):
    error = V4GraphResourceAdmissionError(
        "resource_admission: estimated peak bytes 8192 exceeds configured limit 4096",
        input_bytes=100,
        factor_edges=200,
        factor_owners=10,
        options={"max_factor_edges": 1000, "max_estimated_model_bytes": 4096},
    )
    wrapped = ExceptionGroup("worker failures", [error]) if grouped else error
    payload = ptg_failure_error(wrapped)
    assert payload["code"] == "ptg_graph_resource_admission"
    assert payload["retryable"] is False
    assert payload["resource_admission"] == {
        "version": 1,
        "input_factor_bytes": 100,
        "factor_edge_count": 200,
        "factor_owner_count": 10,
        "max_factor_edges": 1000,
        "max_estimated_model_bytes": 4096,
        "estimated_peak_bytes": 8192,
    }


def test_generic_error_with_resource_text_keeps_generic_classification():
    payload = ptg_failure_error(RuntimeError("resource_admission: unavailable"))
    assert payload["code"] == "ptg_import_failed"
    assert "resource_admission" not in payload


def test_resource_evidence_excludes_invalid_numeric_values():
    error = V4GraphResourceAdmissionError(
        "resource_admission: factor edge count exceeds configured limit",
        input_bytes=-1,
        factor_edges=True,
        options={"max_factor_edges": "unknown", "max_estimated_model_bytes": None},
    )
    assert error.resource_admission == {"version": 1}


@pytest.mark.parametrize(
    "worker,suffix",
    [
        ("process.PTGLarge", "LARGE"),
        ("process.PTGHuge", "HUGE"),
        ("process.PTGNormal", None),
        ("process.PTGSmall", None),
        ("", None),
        ("unrecognized.PTGLarge", None),
    ],
)
def test_admission_policy_is_isolated_to_configured_worker(monkeypatch, worker, suffix):
    from process.ptg_parts.ptg2_v4_graph_compiler import _resource_admission_option_defaults

    monkeypatch.setenv("HLTHPRT_ACTIVE_WORKER_CLASS", worker)
    for name in ("MAX_ESTIMATED_MODEL_BYTES", "MAX_FACTOR_EDGES"):
        base = f"HLTHPRT_PTG2_V4_GRAPH_{name}"
        monkeypatch.setenv(base, "100")
        monkeypatch.setenv(base + "_LARGE", "200")
        monkeypatch.setenv(base + "_HUGE", "300")
    expected = {"LARGE": 200, "HUGE": 300}.get(suffix, 100)
    assert set(_resource_admission_option_defaults().values()) == {expected}


@pytest.mark.parametrize("value", ["", "0", "-1", "1.5", "true", "１２"])
def test_invalid_selected_lane_policy_fails_closed(monkeypatch, value):
    from process.ptg_graph_resource_admission import graph_admission_environment_name

    name = "HLTHPRT_PTG2_V4_GRAPH_MAX_FACTOR_EDGES"
    monkeypatch.setenv("HLTHPRT_ACTIVE_WORKER_CLASS", "process.PTGLarge")
    monkeypatch.setenv(name + "_LARGE", value)
    with pytest.raises(ValueError, match="positive decimal integer"):
        graph_admission_environment_name(name)


def test_unconfigured_lane_policy_retains_global_fallback(monkeypatch):
    from process.ptg_graph_resource_admission import graph_admission_environment_name

    name = "HLTHPRT_PTG2_V4_GRAPH_MAX_FACTOR_EDGES"
    monkeypatch.setenv("HLTHPRT_ACTIVE_WORKER_CLASS", "process.PTGLarge")
    monkeypatch.delenv(name + "_LARGE", raising=False)
    assert graph_admission_environment_name(name) == name
