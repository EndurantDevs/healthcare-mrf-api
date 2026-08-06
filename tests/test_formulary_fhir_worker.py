# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import pytest

import process
from api import control_imports, control_workers
from process.formulary_fhir import worker
from process.formulary_fhir.repository import SOURCE_ID


def test_worker_identity_and_control_adapter_are_single_job():
    assert process.FormularyFHIR.queue_name == "arq:FormularyFHIR"
    assert process.FormularyFHIR.max_jobs == 1
    assert process.FormularyFHIR.queue_read_limit == 1
    adapter = control_imports._SINGLE_JOB_ADAPTERS["formulary-fhir"]
    assert adapter == {
        "queue": "arq:FormularyFHIR",
        "function": "control_single_job_start",
        "payload": "control_wrapped",
        "target_module": "process.formulary_fhir.worker",
        "target_function": "process_data",
    }
    assert control_imports._IMPORTER_DEPENDENCIES["formulary-fhir"] == ["ndc"]
    spec = control_workers._BY_QUEUE["arq:FormularyFHIR"]
    assert spec.worker_class == "process.FormularyFHIR"
    assert spec.importers == ("formulary-fhir",)


@pytest.mark.parametrize("value", (1, 2, 4, 8))
def test_alias_concurrency_accepts_only_benchmark_values(monkeypatch, value):
    monkeypatch.setenv("HLTHPRT_FORMULARY_FHIR_ALIAS_CAP", "8")
    assert worker._concurrency(value) == value


@pytest.mark.parametrize("value", (True, 0, 3, 9, "invalid"))
def test_alias_concurrency_fails_closed(monkeypatch, value):
    monkeypatch.setenv("HLTHPRT_FORMULARY_FHIR_ALIAS_CAP", "8")
    with pytest.raises(ValueError, match="alias concurrency"):
        worker._concurrency(value)


def test_deadline_uses_requested_value_without_exceeding_mode_ceiling():
    assert worker._deadline_seconds(900, manual_seed=False) == 900
    assert worker._deadline_seconds(None, manual_seed=False) == 16 * 60 * 60
    assert worker._deadline_seconds(None, manual_seed=True) == 72 * 60 * 60
    with pytest.raises(ValueError, match="run-mode ceiling"):
        worker._deadline_seconds(16 * 60 * 60 + 1, manual_seed=False)
    with pytest.raises(ValueError, match="numeric"):
        worker._deadline_seconds(True, manual_seed=True)


@pytest.mark.asyncio
async def test_runtime_source_and_activation_gates_precede_database_access(
    monkeypatch,
):
    monkeypatch.delenv("HLTHPRT_FORMULARY_FHIR_AUTOMATION_ENABLED", raising=False)
    monkeypatch.delenv("HLTHPRT_FORMULARY_FHIR_PUBLISH_ENABLED", raising=False)

    with pytest.raises(ValueError, match="source_id"):
        await worker.process_data(
            {},
            {
                "source_id": "unapproved-source",
                "manual_seed": False,
                "publish": False,
            },
        )
    with pytest.raises(ValueError, match="manual_seed must be a boolean"):
        await worker.process_data(
            {},
            {"source_id": SOURCE_ID, "manual_seed": "false", "publish": False},
        )
    with pytest.raises(RuntimeError, match="automation is disabled"):
        await worker.process_data(
            {},
            {
                "source_id": SOURCE_ID,
                "manual_seed": False,
                "publication_proof": False,
                "publish": False,
            },
        )


@pytest.mark.asyncio
async def test_manual_seed_is_nonpublishing_even_when_publish_gate_is_open(
    monkeypatch,
):
    monkeypatch.setenv("HLTHPRT_FORMULARY_FHIR_PUBLISH_ENABLED", "true")

    with pytest.raises(RuntimeError, match="must be non-publishing"):
        await worker.process_data(
            {},
            {"source_id": SOURCE_ID, "manual_seed": True, "publish": True},
        )


@pytest.mark.asyncio
async def test_publication_proof_is_explicit_and_precedes_database_access(
    monkeypatch,
):
    monkeypatch.delenv(
        "HLTHPRT_FORMULARY_FHIR_AUTOMATION_ENABLED",
        raising=False,
    )
    monkeypatch.delenv(
        "HLTHPRT_FORMULARY_FHIR_PUBLISH_ENABLED",
        raising=False,
    )

    with pytest.raises(RuntimeError, match="must request publication"):
        await worker.process_data(
            {},
            {
                "source_id": SOURCE_ID,
                "manual_seed": False,
                "publication_proof": True,
                "publish": False,
            },
        )
    with pytest.raises(RuntimeError, match="publication is disabled"):
        await worker.process_data(
            {},
            {
                "source_id": SOURCE_ID,
                "manual_seed": False,
                "publication_proof": True,
                "publish": True,
            },
        )
