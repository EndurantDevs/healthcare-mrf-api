from __future__ import annotations

import pytest

from api import control_workers


@pytest.fixture(autouse=True)
def isolate_ptg_capacity_from_worker_units(monkeypatch):
    """These units target source admission; capacity locking has its own tests."""

    async def unit_guard(
        worker_payload,
        *,
        run_id,
        importer,
        selected_specs,
    ):
        failure = await control_workers._admit_worker_ensure(
            worker_payload,
            run_id=run_id,
            importer=importer,
            selected_specs=selected_specs,
        )
        if failure is not None:
            return failure
        return await control_workers.asyncio.to_thread(
            control_workers.ensure_worker,
            worker_payload,
        )

    monkeypatch.setattr(control_workers, "_guarded_ptg_family_ensure", unit_guard)


@pytest.mark.asyncio
async def test_guarded_ptg_ensure_requires_exact_source_identity(monkeypatch):
    admitted_calls: list[dict[str, object]] = []

    async def admit(**kwargs):
        admitted_calls.append(kwargs)
        return {"importer": "ptg"}

    monkeypatch.setattr(
        control_workers,
        "admit_existing_outer_run_action",
        admit,
    )
    monkeypatch.setattr(
        control_workers,
        "ensure_worker",
        lambda _worker_payload: {"status": "already_running", "items": []},
    )

    response = await control_workers.guarded_ensure_worker(
        {
            "run_id": "run_source_attempt",
            "importer": "ptg",
            "source_file_import_id": "source-attempt-1",
            "import_id": "source-attempt-1",
        }
    )

    assert response["status"] == "already_running"
    assert admitted_calls[0]["expected_source_file_import_id"] == (
        "source-attempt-1"
    )


@pytest.mark.asyncio
async def test_guarded_ptg_ensure_rejects_missing_source_identity(monkeypatch):
    async def reject_missing_identity(**kwargs):
        assert kwargs["expected_source_file_import_id"] is None
        raise control_workers.PTGSourceAttemptIdentityError("mismatch")

    monkeypatch.setattr(
        control_workers,
        "admit_existing_outer_run_action",
        reject_missing_identity,
    )

    response = await control_workers.guarded_ensure_worker(
        {
            "run_id": "run_source_attempt",
            "importer": "ptg",
            "import_id": "ordinary-import-id",
        }
    )

    assert response["status"] == "failed"
    assert response["message"] == (
        "PTG source-attempt identity is invalid or changed"
    )


@pytest.mark.asyncio
async def test_guarded_ptg_ensure_rejects_conflicting_source_aliases(
    monkeypatch,
):
    async def fail_admission(**_kwargs):
        raise AssertionError("conflicting identity must fail before admission")

    monkeypatch.setattr(
        control_workers,
        "admit_existing_outer_run_action",
        fail_admission,
    )

    response = await control_workers.guarded_ensure_worker(
        {
            "run_id": "run_source_attempt",
            "importer": "ptg",
            "source_file_import_id": "source-attempt-1",
            "import_id": "source-attempt-2",
        }
    )

    assert response["status"] == "failed"
    assert response["message"] == (
        "PTG source-attempt identity is invalid or changed"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status",
    ("canceling", "succeeded", "failed", "canceled", "dead_letter"),
)
async def test_guarded_fhir_ensure_rejects_non_launchable_control_status(
    monkeypatch,
    status,
):
    async def admit(**kwargs):
        assert kwargs["worker_selection"].allowed_importers == frozenset(
            {"provider-directory-fhir"}
        )
        return {
            "importer": "provider-directory-fhir",
            "status": status,
        }

    def should_not_launch(_worker_payload):
        raise AssertionError("non-launchable FHIR run must not start a worker")

    monkeypatch.setattr(
        control_workers,
        "admit_existing_outer_run_action",
        admit,
    )
    monkeypatch.setattr(control_workers, "ensure_worker", should_not_launch)

    response = await control_workers.guarded_ensure_worker(
        {
            "run_id": "run_provider_directory_terminal",
            "importer": "provider-directory-fhir",
        }
    )

    assert response["status"] == "failed"
    assert response["message"] == f"control run is not launchable: {status}"


@pytest.mark.asyncio
async def test_guarded_fhir_ensure_allows_running_control_status(monkeypatch):
    async def admit(**_kwargs):
        return {
            "importer": "provider-directory-fhir",
            "status": "running",
        }

    def ensure(_worker_payload):
        return {"status": "already_running", "items": []}

    monkeypatch.setattr(
        control_workers,
        "admit_existing_outer_run_action",
        admit,
    )
    monkeypatch.setattr(control_workers, "ensure_worker", ensure)

    response = await control_workers.guarded_ensure_worker(
        {
            "run_id": "run_provider_directory_running",
            "importer": "provider-directory-fhir",
        }
    )

    assert response["status"] == "already_running"


@pytest.mark.asyncio
@pytest.mark.parametrize("request_importer", [None, "claims-pricing"])
async def test_ptg_run_rejects_non_ptg_worker_selector(
    monkeypatch,
    request_importer,
):
    async def reject_selector(**kwargs):
        selection = kwargs["worker_selection"]
        assert "ptg" not in selection.allowed_importers
        raise control_workers.PTGSourceAttemptIdentityError("selector")

    monkeypatch.setattr(
        control_workers,
        "admit_existing_outer_run_action",
        reject_selector,
    )
    worker_request_by_field = {
        "run_id": "run_source_attempt",
        "queue": "arq:ClaimsPricing",
    }
    if request_importer is not None:
        worker_request_by_field["importer"] = request_importer

    response = await control_workers.guarded_ensure_worker(
        worker_request_by_field
    )

    assert response["status"] == "failed"
    assert response["message"] == (
        "PTG source-attempt identity is invalid or changed"
    )


def test_worker_registry_exposes_shared_and_finish_workers():
    worker_specs = control_workers.worker_registry()
    by_importer = {
        importer: worker_spec
        for worker_spec in worker_specs
        for importer in worker_spec["importers"]
        if worker_spec["role"] == "start"
    }
    by_queue = {worker_spec["queue"]: worker_spec for worker_spec in worker_specs}

    assert by_importer["claims-procedures"]["worker_class"] == "process.ClaimsPricing"
    assert by_importer["entity-address-unified"]["worker_class"] == "process.EntityAddressUnified"
    assert by_importer["provider-directory-fhir"]["worker_class"] == "process.ProviderDirectoryFHIR"
    assert by_importer["florida-mqa-profile"]["worker_class"] == "process.FloridaMQAProfile"
    assert by_importer["ms-drg"]["worker_class"] == "process.MSDRG"
    assert by_importer["terminology-synonyms"]["worker_class"] == "process.TerminologySynonyms"
    assert by_importer["openaddresses"]["worker_class"] == "process.OpenAddresses"
    assert (
        by_importer["address-formatted-address"]["worker_class"]
        == "process.AddressArchive"
    )
    assert (
        by_importer["address-numeric-grid-alias"]["worker_class"]
        == "process.AddressArchive"
    )
    assert (
        by_importer["address-strict-source-backfill"]["worker_class"]
        == "process.AddressArchive"
    )
    assert (
        by_importer["address-numeric-grid-alias-revoke"]["worker_class"]
        == "process.AddressArchive"
    )
    assert by_importer["ptg-candidate-audit"]["worker_class"] == "process.PTGCandidateAudit"
    assert by_queue["arq:PTGCandidateAudit"]["role"] == "start"
    assert by_queue["arq:OpenAddresses"]["role"] == "start"
    assert by_queue["arq:ProviderDirectoryFHIR"]["role"] == "start"
    assert by_queue["arq:FloridaMQAProfile"]["role"] == "start"
    assert by_queue["arq:PTGSmall"]["worker_class"] == "process.PTGSmall"
    assert by_queue["arq:PTGNormal"]["worker_class"] == "process.PTGNormal"
    assert by_queue["arq:PTGLarge"]["worker_class"] == "process.PTGLarge"
    assert by_queue["arq:PTGHuge"]["worker_class"] == "process.PTGHuge"
    assert "entity-address-unified" in by_queue["arq:EntityAddressUnified"]["importers"]
    assert by_queue["arq:PartDFormularyNetwork_finish"]["role"] == "finish"


def test_resolve_specs_prefers_finish_role_over_stale_start_queue():
    specs = control_workers._resolve_specs(
        {"importer": "mrf", "role": "finish", "queue": "arq:MRF"}
    )

    assert [spec.queue for spec in specs] == ["arq:MRF_finish"]


def test_resolve_specs_prefers_finalizing_status_over_stale_start_queue():
    specs = control_workers._resolve_specs(
        {"importer": "mrf", "status": "finalizing", "queue": "arq:MRF"}
    )

    assert [spec.queue for spec in specs] == ["arq:MRF_finish"]


def test_ensure_worker_starts_registered_burst_worker(monkeypatch, tmp_path):
    captured_by_field: dict[str, object] = {}

    class FakeProcess:
        pid = 12345

    def fake_popen(cmd, *, cwd, env, stdout, stderr, start_new_session):
        captured_by_field.update(
            {
                "cmd": cmd,
                "cwd": cwd,
                "env": env,
                "stdout": stdout,
                "stderr": stderr,
                "start_new_session": start_new_session,
            }
        )
        return FakeProcess()

    monkeypatch.setenv("HLTHPRT_WORKER_STATE_DIR", str(tmp_path / "state"))
    monkeypatch.setenv("HLTHPRT_WORKER_LOG_DIR", str(tmp_path / "logs"))
    monkeypatch.setattr(control_workers.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(control_workers, "_is_pid_running", lambda pid: pid == FakeProcess.pid)
    monkeypatch.setattr(control_workers, "_is_pid_spec_match", lambda pid, spec: True)

    worker_response = control_workers.ensure_worker(
        {
            "importer": "claims-pricing",
            "import_id": "import_1",
            "run_id": "run_1",
        }
    )

    assert worker_response["status"] == "started"
    assert worker_response["contract_id"] == (
        control_workers.WORKER_ENSURE_RUN_IDENTITY_CONTRACT
    )
    assert worker_response["run_id"] == "run_1"
    assert worker_response["items"][0]["run_id"] == "run_1"
    assert worker_response["items"][0]["worker_class"] == "process.ClaimsPricing"
    assert captured_by_field["env"]["HLTHPRT_IMPORT_ID_OVERRIDE"] == "import_1"
    assert captured_by_field["env"]["HLTHPRT_CONTROL_RUN_ID"] == "run_1"
    assert captured_by_field["start_new_session"] is True


def test_ensure_spec_blocks_competing_finish_workers(monkeypatch):
    monkeypatch.setenv("HLTHPRT_WORKER_LAUNCHER", "process")
    drug_finish = control_workers._BY_QUEUE["arq:DrugClaims_finish"]
    claims_finish = control_workers._BY_QUEUE["arq:ClaimsPricing_finish"]
    cases = (
        (
            drug_finish,
            claims_finish,
            "ClaimsPricing_finish is already running",
        ),
        (
            claims_finish,
            drug_finish,
            "DrugClaims_finish is already running",
        ),
    )

    for requested_spec, running_spec, message in cases:
        monkeypatch.setattr(
            control_workers,
            "_worker_state",
            lambda spec, _payload=None, *, active=running_spec: {
                "running": spec == active,
                "worker_class": spec.worker_class,
            },
        )

        ensure_response = control_workers._ensure_spec(requested_spec, {})

        assert ensure_response["status"] == "blocked"
        assert ensure_response["message"] == message


def test_ensure_worker_uses_finish_role_for_finalizing_run(monkeypatch, tmp_path):
    class FakeProcess:
        pid = 456

    captured_by_field: dict[str, object] = {}

    monkeypatch.setenv("HLTHPRT_WORKER_STATE_DIR", str(tmp_path / "state"))
    monkeypatch.setenv("HLTHPRT_WORKER_LOG_DIR", str(tmp_path / "logs"))

    def fake_popen(cmd, **_kwargs):
        captured_by_field["cmd"] = cmd
        return FakeProcess()

    monkeypatch.setattr(control_workers.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(control_workers, "_is_pid_running", lambda pid: pid == FakeProcess.pid)
    monkeypatch.setattr(control_workers, "_is_pid_spec_match", lambda pid, spec: True)

    result = control_workers.ensure_worker({"importer": "partd-formulary-network", "status": "finalizing"})

    assert result["status"] == "started"
    assert result["items"][0]["role"] == "finish"
    assert captured_by_field["cmd"][-2:] == ["process.PartDFormularyNetwork_finish", "--burst"]


def test_ensure_worker_uses_explicit_ptg_lane(monkeypatch, tmp_path):
    class FakeProcess:
        pid = 789

    captured_by_field: dict[str, object] = {}

    monkeypatch.setenv("HLTHPRT_WORKER_STATE_DIR", str(tmp_path / "state"))
    monkeypatch.setenv("HLTHPRT_WORKER_LOG_DIR", str(tmp_path / "logs"))

    def fake_popen(cmd, *, env, **_kwargs):
        captured_by_field["cmd"] = cmd
        captured_by_field["env"] = env
        return FakeProcess()

    monkeypatch.setattr(control_workers.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(control_workers, "_is_pid_running", lambda pid: pid == FakeProcess.pid)
    monkeypatch.setattr(control_workers, "_is_pid_spec_match", lambda pid, spec: True)

    result = control_workers.ensure_worker(
        {"importer": "ptg", "queue": "arq:PTGSmall", "worker_class": "process.PTGSmall", "run_id": "run_ptg"}
    )

    assert result["status"] == "started"
    assert result["items"][0]["worker_class"] == "process.PTGSmall"
    assert captured_by_field["cmd"][-2:] == ["worker-once", "process.PTGSmall"]
    assert captured_by_field["env"]["HLTHPRT_ACTIVE_WORKER_QUEUE"] == "arq:PTGSmall"
    assert captured_by_field["env"]["HLTHPRT_ACTIVE_WORKER_CLASS"] == "process.PTGSmall"
    assert captured_by_field["env"]["HLTHPRT_WORKER_ONCE_TARGET_JOB_ID"] == "ptg_start_run_ptg"


def test_ensure_worker_targets_exact_npi_job(monkeypatch, tmp_path):
    class FakeProcess:
        pid = 790

    captured_by_field: dict[str, object] = {}

    monkeypatch.setenv("HLTHPRT_WORKER_STATE_DIR", str(tmp_path / "state"))
    monkeypatch.setenv("HLTHPRT_WORKER_LOG_DIR", str(tmp_path / "logs"))

    def fake_popen(cmd, *, env, **_kwargs):
        captured_by_field["cmd"] = cmd
        captured_by_field["env"] = env
        return FakeProcess()

    monkeypatch.setattr(control_workers.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(
        control_workers,
        "_is_pid_running",
        lambda pid: pid == FakeProcess.pid,
    )
    monkeypatch.setattr(control_workers, "_is_pid_spec_match", lambda pid, spec: True)

    worker_result_by_field = control_workers.ensure_worker(
        {
            "importer": "npi",
            "run_id": "run_npi",
            "job_id": "npi_start_run_npi",
        }
    )

    assert worker_result_by_field["status"] == "started"
    assert worker_result_by_field["items"][0]["worker_class"] == "process.NPI"
    assert captured_by_field["cmd"][-2:] == ["worker-once", "process.NPI"]
    assert captured_by_field["env"]["HLTHPRT_ACTIVE_WORKER_QUEUE"] == "arq:NPI"
    assert captured_by_field["env"]["HLTHPRT_ACTIVE_WORKER_CLASS"] == "process.NPI"
    assert (
        captured_by_field["env"]["HLTHPRT_WORKER_ONCE_TARGET_JOB_ID"]
        == "npi_start_run_npi"
    )


def test_ensure_worker_starts_entity_address_unified_shared_worker(monkeypatch, tmp_path):
    class FakeProcess:
        pid = 2468

    captured_by_field: dict[str, object] = {}

    monkeypatch.setenv("HLTHPRT_WORKER_STATE_DIR", str(tmp_path / "state"))
    monkeypatch.setenv("HLTHPRT_WORKER_LOG_DIR", str(tmp_path / "logs"))

    def fake_popen(cmd, *, env, **_kwargs):
        captured_by_field["cmd"] = cmd
        captured_by_field["env"] = env
        return FakeProcess()

    monkeypatch.setattr(control_workers.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(control_workers, "_is_pid_running", lambda pid: pid == FakeProcess.pid)
    monkeypatch.setattr(control_workers, "_is_pid_spec_match", lambda pid, spec: True)

    result = control_workers.ensure_worker({"importer": "entity-address-unified", "run_id": "run_refresh"})

    assert result["status"] == "started"
    assert result["items"][0]["worker_class"] == "process.EntityAddressUnified"
    assert captured_by_field["cmd"][-2:] == ["process.EntityAddressUnified", "--burst"]
    assert captured_by_field["env"]["HLTHPRT_ACTIVE_WORKER_QUEUE"] == "arq:EntityAddressUnified"
