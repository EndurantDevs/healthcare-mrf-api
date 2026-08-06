# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from click.testing import CliRunner

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


def test_cli_exposes_the_exact_seed_selected_for_publication_proof():
    cli_result = CliRunner().invoke(process.formulary_fhir_command, ["--help"])

    assert cli_result.exit_code == 0
    assert "--seed-dataset-id" in cli_result.output
    assert "no-recrawl" in cli_result.output
    assert "publication proof" in cli_result.output


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
    with pytest.raises(ValueError, match="requires seed_dataset_id"):
        await worker.process_data(
            {},
            {
                "source_id": SOURCE_ID,
                "manual_seed": False,
                "publication_proof": True,
                "publish": True,
            },
        )


@pytest.mark.asyncio
async def test_publication_proof_reuses_verified_seed_without_fhir_requests(
    monkeypatch,
):
    dataset_id = "ffd_" + "a" * 48

    class _ForbiddenClient:
        def __init__(self):
            raise AssertionError("publication proof must not construct a FHIR client")

    monkeypatch.setattr(worker, "FHIRFormularyClient", _ForbiddenClient)
    formulary_repository = SimpleNamespace(
        verify_dataset=AsyncMock(
            return_value={
                "list_count": 1,
                "alias_count": 2,
                "medication_membership_count": 3,
                "coverage_hash": "c" * 64,
                "membership_hash": "m" * 64,
            }
        ),
        publish_verified_seed=AsyncMock(return_value=1),
    )
    validated_settings = worker._validated_run_settings(
        {},
        {
            "run_id": "synthetic-publication-proof",
            "source_id": SOURCE_ID,
            "manual_seed": False,
            "publication_proof": True,
            "seed_dataset_id": dataset_id,
            "publish": True,
        },
    )

    proof_result_by_field = await worker._execute_run(
        validated_settings,
        formulary_repository,
    )

    assert proof_result_by_field["dataset_id"] == dataset_id
    assert proof_result_by_field["publication_proof_dataset_reused"] is True
    assert proof_result_by_field["request_count"] == 0
    formulary_repository.verify_dataset.assert_awaited_once_with(dataset_id)
    formulary_repository.publish_verified_seed.assert_awaited_once_with(dataset_id)


@pytest.mark.parametrize(
    "task, message",
    [
        (
            {
                "manual_seed": False,
                "publication_proof": True,
                "seed_dataset_id": "not-a-dataset",
                "publish": True,
            },
            "seed_dataset_id is invalid",
        ),
        (
            {
                "manual_seed": True,
                "publication_proof": False,
                "seed_dataset_id": "ffd_" + "a" * 48,
                "publish": False,
            },
            "reserved for publication proof",
        ),
    ],
)
def test_seed_dataset_id_is_closed_to_the_publication_proof_mode(task, message):
    with pytest.raises(ValueError, match=message):
        worker._validated_run_settings({}, {"source_id": SOURCE_ID, **task})
