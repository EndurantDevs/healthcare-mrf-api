# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Register complete NYPP imports without exposing partial publication controls."""

import importlib
import json
import socket

import pytest
from arq.worker import Worker, get_kwargs
from click.testing import CliRunner

import process
from api import control_imports, control_workers
from process import control_lifecycle
from process import new_york_profile as profile


@pytest.fixture(autouse=True)
def deny_network(monkeypatch):
    def reject(*_args, **_kwargs):
        raise AssertionError("Registration checks forbid network access")

    monkeypatch.setattr(socket.socket, "connect", reject)
    monkeypatch.setattr(socket.socket, "connect_ex", reject)
    monkeypatch.setattr(socket, "create_connection", reject)


def test_catalog_cli_and_worker_register_only_complete_nypp_source():
    entries = control_imports.importer_registry()
    entry = next(entry for entry in entries if entry["name"] == profile.IMPORTER)
    assert entry["profile_source"] == {"source_key": "new-york-nypp", "display_name": "New York Physician Profile"}
    assert entry["family"] == "provider" and entry["depends_on"] == ["npi"]
    assert entry["cancelable"] and entry["schedulable"] and entry["retryable"]
    assert entry["params_schema"] == [] and entry["enqueue_adapter"] == "arq_single_job"
    assert profile.CATEGORIES == ("education", "training", "certifications")
    assert not any("nysed" in entry["name"] for entry in entries)
    adapter = control_imports._SINGLE_JOB_ADAPTERS[profile.IMPORTER]
    assert adapter == {
        "queue": "arq:NewYorkNYPPProfile",
        "function": "control_single_job_start",
        "payload": "control_wrapped",
        "target_module": "process.new_york_profile",
        "target_function": "import_profiles",
    }
    spec = next(spec for spec in control_workers._WORKERS if profile.IMPORTER in spec.importers)
    assert spec.queue == adapter["queue"] == process.NewYorkNYPPProfile.queue_name
    assert spec.worker_class == "process.NewYorkNYPPProfile"
    assert process.NewYorkNYPPProfile.max_jobs == process.NewYorkNYPPProfile.queue_read_limit == 1
    assert process.NewYorkNYPPProfile.functions == [process.control_single_job_start]
    assert process.NewYorkNYPPProfile.functions[0].max_tries == 1
    assert process.NewYorkNYPPProfile.on_startup is process.db_startup
    cli = CliRunner().invoke(process.process_group, [profile.IMPORTER])
    assert cli.exit_code == 2 and "managed import API" in cli.output


def test_adapter_does_not_add_a_provider_limit_or_partial_source_selection():
    payload = control_imports._adapter_payload(
        control_imports._SINGLE_JOB_ADAPTERS[profile.IMPORTER],
        {"run_id": "synthetic-control", "importer": profile.IMPORTER, "family": "provider"},
        {},
    )
    assert payload["task"] == {"test_mode": False}
    assert payload["call_style"] == "ctx_task" and payload["run_shutdown"] is False
    assert payload["target_module"] == "process.new_york_profile"
    assert payload["target_function"] == "import_profiles"


@pytest.mark.parametrize(
    "params",
    [
        {"max_providers": 100},
        {"max_providers": 0},
        {"resume_from": "a" * 64},
        {"sources": ["education"]},
        {"sources": []},
        {"professions": ["060"]},
        {"license_types": ["MD"]},
    ],
)
def test_partial_payload_reaches_existing_runner_refusal_without_becoming_complete(params):
    payload = control_imports._adapter_payload(
        control_imports._SINGLE_JOB_ADAPTERS[profile.IMPORTER],
        {"run_id": "synthetic-control", "importer": profile.IMPORTER},
        params,
    )
    assert all(payload["task"][key] == value for key, value in params.items())
    with pytest.raises(ValueError, match="complete_cohort_required"):
        profile._parameters({**payload["task"], "run_id": payload["run_id"]})


@pytest.mark.parametrize(
    "deadline,expected", [(None, 86400), ("invalid", 86400), ("0", 86400), ("3600", 86400), ("172800", 173100)]
)
async def test_arq_timeout_covers_configured_deadline_and_cleanup(monkeypatch, deadline, expected):
    try:
        with monkeypatch.context() as patch:
            if deadline is None:
                patch.delenv("HLTHPRT_NYPP_DEADLINE_SECONDS", raising=False)
            else:
                patch.setenv("HLTHPRT_NYPP_DEADLINE_SECONDS", deadline)
            settings = importlib.reload(process).NewYorkNYPPProfile
            arq_worker = Worker(**get_kwargs(settings), handle_signals=False)
            assert settings.job_timeout == arq_worker.job_timeout_s == expected
            assert arq_worker.functions["control_single_job_start"].max_tries == 1
            assert arq_worker.functions["control_single_job_start"].timeout_s is None
            assert arq_worker.max_jobs == arq_worker.queue_read_limit == 1
    finally:
        importlib.reload(process)


def test_worker_uses_existing_resource_configuration(monkeypatch):
    spec = next(spec for spec in control_workers._WORKERS if profile.IMPORTER in spec.importers)
    for name in (
        "HLTHPRT_WORKER_JOB_RESOURCE_PROFILES_JSON",
        "HLTHPRT_WORKER_JOB_CPU_REQUEST",
        "HLTHPRT_WORKER_JOB_MEMORY_REQUEST",
        "HLTHPRT_WORKER_JOB_CPU_LIMIT",
        "HLTHPRT_WORKER_JOB_MEMORY_LIMIT",
    ):
        monkeypatch.delenv(name, raising=False)
    assert control_workers._worker_job_resources(spec) == {}
    resources_by_kind = {"requests": {"cpu": "1", "memory": "2Gi"}, "limits": {"cpu": "2", "memory": "3Gi"}}
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_RESOURCE_PROFILES_JSON", json.dumps({spec.worker_class: resources_by_kind}))
    assert control_workers._worker_job_resources(spec) == resources_by_kind


def test_terminal_committed_result_preserves_source_publication_boundary():
    committed_by_field = {"published": True, "retained_source_records": 2}
    context_by_field = {
        "context": {"control_run_terminal_committed": True, "_control_committed_result": committed_by_field}
    }
    assert (
        control_lifecycle._committed_target_result(context_by_field, target_module="process.new_york_profile")
        is committed_by_field
    )
    assert (
        control_lifecycle._committed_target_result(context_by_field, target_module="process.new_york_nysed_profile")
        is None
    )
    context_by_field["context"]["control_run_terminal_committed"] = False
    assert (
        control_lifecycle._committed_target_result(context_by_field, target_module="process.new_york_profile") is None
    )
