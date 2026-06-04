# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import json
import types

import pytest
from sqlalchemy.exc import IntegrityError
from sanic.exceptions import Forbidden
from sanic.exceptions import BadRequest, NotFound

from api import control
from api.control import _require_control_auth
from api import control_imports
from api.control_imports import (
    _enqueue_import_start,
    _remove_queued_job,
    _set_cancel_flag,
    create_import_run,
    finalize_import_run,
    importer_registry,
    normalize_run,
    parse_ptg_toc_preview,
)

CONTROL_HEADERS = {"Authorization": "Bearer secret"}


def authed_request(**kwargs):
    kwargs.setdefault("headers", CONTROL_HEADERS)
    return types.SimpleNamespace(**kwargs)


def test_importer_registry_exposes_ptg_and_finish_lifecycle():
    items = {entry["name"]: entry for entry in importer_registry()}

    assert "ptg" in items
    assert items["ptg"]["kind"] == "discovered"
    assert items["claims-pricing"]["lifecycle"] == "start_finish"
    assert items["claims-pricing"]["enqueue_adapter"] == "arq_single_job"
    assert items["ptg"]["enqueue_adapter"] == "arq_single_job"
    assert items["code-sets"]["enqueue_adapter"] == "arq_single_job"
    assert items["clinical-reference"]["enqueue_adapter"] == "arq_single_job"
    assert items["geo"]["enqueue_adapter"] == "arq_single_job"
    assert items["geo-census"]["enqueue_adapter"] == "arq_single_job"
    assert items["plan-attributes"]["enqueue_adapter"] == "arq_single_job"
    assert items["npi"]["schedulable"] is True
    assert items["ptg"]["cancelable"] is True
    assert items["npi"]["cancelable"] is True
    assert items["claims-pricing"]["cancelable"] is False
    assert any(param["name"] == "toc_url" and param["multiple"] for param in items["ptg"]["params_schema"])


def test_normalize_run_accepts_plain_dict():
    row = {"run_id": "run_1", "status": "queued"}

    assert normalize_run(row) == row


def test_normalize_run_overlays_live_progress_for_any_active_importer(monkeypatch):
    monkeypatch.setattr(
        control_imports,
        "read_live_progress",
        lambda _run_id: {
            "phase": "compact in-network import",
            "pct": 42.5,
            "eta_seconds": 120,
            "estimated_finish_at": "2026-06-03T12:02:00Z",
            "updated_at": "2026-06-03T12:00:00Z",
            "message": "processing rows",
        },
    )

    result = normalize_run(
        {
            "run_id": "run_codesets",
            "importer": "code-sets",
            "status": "running",
            "progress": {"pct": 0, "message": "running"},
        }
    )

    assert result["phase_detail"] == "compact in-network import"
    assert result["progress"]["pct"] == 42.5
    assert result["progress"]["message"] == "processing rows"
    assert result["estimate"]["eta_seconds"] == 120


def test_parse_ptg_toc_preview_counts_filtered_file_entries():
    payload = {
        "toc_url": "https://example.test/index.json",
        "plan_ids": ["123"],
        "toc": {
            "reporting_entity_name": "Example Payer",
            "reporting_entity_type": "payer",
            "reporting_structure": [
                {
                    "reporting_plans": [
                        {"plan_id": "123", "plan_id_type": "ein", "plan_name": "Plan A", "plan_market_type": "group"},
                        {"plan_id": "999", "plan_id_type": "ein", "plan_name": "Plan B", "plan_market_type": "group"},
                    ],
                    "in_network_files": [{"location": "https://example.test/rates.json.gz", "description": "rates"}],
                }
            ],
        },
    }

    preview = parse_ptg_toc_preview(payload)

    assert preview["counts"]["plans"] == 1
    assert preview["counts"]["by_domain"]["in_network"] == 1
    assert any(item["source_type"] == "in-network" for item in preview["items"])


def test_ptg_import_file_payload_maps_contract_to_ptg_import():
    payload = control._ptg_import_file_payload(
        {
            "run_id": "run_ptg",
            "source_file_import_id": "source_file_import_1",
            "source_file_id": "file_1",
            "source_key": "asr_1208",
            "in_network_url": "https://example.com/rates.json.gz",
            "import_month": "2026-06",
            "idempotency_key": "ptg:file_1:2026-06",
        }
    )

    assert payload["run_id"] == "run_ptg"
    assert payload["importer"] == "ptg"
    assert payload["idempotency_key"] == "ptg:file_1:2026-06"
    assert payload["triggered_by"] == "source_file_import"
    assert payload["source_file_import_id"] == "source_file_import_1"
    assert payload["params"]["source_file_id"] == "file_1"
    assert payload["params"]["source_key"] == "asr_1208"
    assert payload["params"]["in_network_url"] == "https://example.com/rates.json.gz"


def test_control_auth_accepts_bearer_token(monkeypatch):
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "secret")
    request = types.SimpleNamespace(headers={"Authorization": "Bearer secret"})

    _require_control_auth(request)


def test_control_auth_rejects_missing_token(monkeypatch):
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "secret")
    request = types.SimpleNamespace(headers={})

    with pytest.raises(Forbidden):
        _require_control_auth(request)


def test_control_auth_rejects_unconfigured_token(monkeypatch):
    monkeypatch.delenv("HLTHPRT_CONTROL_API_TOKEN", raising=False)
    request = types.SimpleNamespace(headers={"Authorization": "Bearer anything"})

    with pytest.raises(Forbidden):
        _require_control_auth(request)


def test_control_error_payload_uses_contract_shape():
    request = types.SimpleNamespace(headers={"X-Request-ID": "req_control_1"}, id="fallback")
    bad_payload = control._error_payload(request, BadRequest("bad input"))
    missing_payload = control._error_payload(request, NotFound("missing run"))

    assert bad_payload["error"] == {
        "code": "invalid_request",
        "message": "bad input",
        "detail": {},
        "request_id": "req_control_1",
    }
    assert missing_payload["error"]["code"] == "not_found"
    assert missing_payload["error"]["request_id"] == "req_control_1"


@pytest.mark.asyncio
async def test_enqueue_import_start_uses_control_run_id(monkeypatch):
    calls = []

    class FakeJob:
        job_id = "job_1"

    class FakeRedis:
        async def enqueue_job(self, *args, **kwargs):
            calls.append((args, kwargs))
            return FakeJob()

    async def fake_create_pool(*_args, **_kwargs):
        return FakeRedis()

    monkeypatch.setattr("api.control_imports.create_pool", fake_create_pool)
    row = {
        "run_id": "run_control",
        "importer": "claims-pricing",
        "import_id": "202606",
        "params": {"test_mode": True},
    }

    result = await _enqueue_import_start(row)

    assert result["status"] == "queued"
    assert result["metrics"]["queue"] == "arq:ClaimsPricing"
    args, kwargs = calls[0]
    assert args[0] == "claims_pricing_start"
    assert args[1]["run_id"] == "run_control"
    assert args[1]["test_mode"] is True
    assert kwargs["_job_id"] == "claims_start_run_control"


@pytest.mark.asyncio
async def test_enqueue_import_start_enqueues_ptg_control_job(monkeypatch):
    calls = []

    class FakeJob:
        job_id = "job_ptg"

    class FakeRedis:
        async def enqueue_job(self, *args, **kwargs):
            calls.append((args, kwargs))
            return FakeJob()

    async def fake_create_pool(*_args, **_kwargs):
        return FakeRedis()

    monkeypatch.setattr("api.control_imports.create_pool", fake_create_pool)
    row = {
        "run_id": "run_ptg",
        "importer": "ptg",
        "source_file_import_id": "source_file_import_1",
        "params": {"source_key": "asr_1208", "in_network_url": "https://example.com/rates.json.gz"},
    }

    result = await _enqueue_import_start(row)

    assert result["status"] == "queued"
    assert result["metrics"]["queue"] == "arq:PTG"
    args, kwargs = calls[0]
    assert args[0] == "ptg_control_start"
    assert args[1]["run_id"] == "run_ptg"
    assert args[1]["source_file_import_id"] == "source_file_import_1"
    assert args[1]["params"]["source_key"] == "asr_1208"
    assert kwargs["_job_id"] == "ptg_start_run_ptg"


@pytest.mark.asyncio
async def test_enqueue_import_start_wraps_single_job_lifecycle(monkeypatch):
    calls = []

    class FakeJob:
        job_id = "job_nucc"

    class FakeRedis:
        async def enqueue_job(self, *args, **kwargs):
            calls.append((args, kwargs))
            return FakeJob()

    async def fake_create_pool(*_args, **_kwargs):
        return FakeRedis()

    monkeypatch.setattr("api.control_imports.create_pool", fake_create_pool)
    row = {
        "run_id": "run_nucc",
        "importer": "nucc",
        "params": {"test_mode": True},
    }

    result = await _enqueue_import_start(row)

    assert result["status"] == "queued"
    args, _kwargs = calls[0]
    assert args[0] == "control_single_job_start"
    assert args[1]["run_id"] == "run_nucc"
    assert args[1]["target_module"] == "process.nucc"
    assert args[1]["target_function"] == "process_data"
    assert args[1]["task"] == {"test_mode": True}


@pytest.mark.asyncio
async def test_enqueue_import_start_wraps_direct_process_importers(monkeypatch):
    calls = []

    class FakeJob:
        job_id = "job_lodes"

    class FakeRedis:
        async def enqueue_job(self, *args, **kwargs):
            calls.append((args, kwargs))
            return FakeJob()

    async def fake_create_pool(*_args, **_kwargs):
        return FakeRedis()

    monkeypatch.setattr("api.control_imports.create_pool", fake_create_pool)
    row = {
        "run_id": "run_lodes",
        "importer": "lodes",
        "params": {"test_mode": True, "import_id": "smoke_lodes"},
    }

    result = await _enqueue_import_start(row)

    assert result["status"] == "queued"
    args, _kwargs = calls[0]
    assert args[0] == "control_single_job_start"
    assert args[1]["run_id"] == "run_lodes"
    assert args[1]["target_module"] == "process.lodes"
    assert args[1]["target_function"] == "process_data"
    assert args[1]["task"] == {"test_mode": True, "import_id": "smoke_lodes"}


@pytest.mark.asyncio
async def test_enqueue_import_start_wraps_kwargs_importers(monkeypatch):
    calls = []

    class FakeJob:
        job_id = "job_codes"

    class FakeRedis:
        async def enqueue_job(self, *args, **kwargs):
            calls.append((args, kwargs))
            return FakeJob()

    async def fake_create_pool(*_args, **_kwargs):
        return FakeRedis()

    monkeypatch.setattr("api.control_imports.create_pool", fake_create_pool)
    row = {
        "run_id": "run_codes",
        "importer": "clinical-reference",
        "params": {"test_mode": True, "sources": "icd10cm", "import_id": "smoke_clinical"},
    }

    result = await _enqueue_import_start(row)

    assert result["status"] == "queued"
    args, _kwargs = calls[0]
    assert args[0] == "control_single_job_start"
    assert args[1]["run_id"] == "run_codes"
    assert args[1]["target_module"] == "process.clinical_reference"
    assert args[1]["target_function"] == "main"
    assert args[1]["call_style"] == "kwargs"
    assert args[1]["task"] == {"test_mode": True, "sources": "icd10cm", "import_id": "smoke_clinical"}


@pytest.mark.asyncio
async def test_enqueue_import_start_marks_failed_on_enqueue_error(monkeypatch):
    async def fake_create_pool(*_args, **_kwargs):
        raise RuntimeError("redis unavailable")

    monkeypatch.setattr("api.control_imports.create_pool", fake_create_pool)
    row = {"run_id": "run_npi", "importer": "npi", "params": {}}

    result = await _enqueue_import_start(row)

    assert result["status"] == "failed"
    assert result["error"]["code"] == "enqueue_failed"


@pytest.mark.asyncio
async def test_set_cancel_flag_writes_redis_key(monkeypatch):
    calls = []

    class FakeRedis:
        async def set(self, *args, **kwargs):
            calls.append((args, kwargs))

    async def fake_create_pool(*_args, **_kwargs):
        return FakeRedis()

    monkeypatch.setattr("api.control_imports.create_pool", fake_create_pool)

    result = await _set_cancel_flag("run_1")

    assert result["redis"] is True
    assert calls[0][0] == ("cancel:run_1", "1")
    assert calls[0][1]["ex"] > 0


@pytest.mark.asyncio
async def test_set_cancel_flag_reports_redis_failure(monkeypatch):
    async def fake_create_pool(*_args, **_kwargs):
        raise RuntimeError("redis down")

    monkeypatch.setattr("api.control_imports.create_pool", fake_create_pool)

    result = await _set_cancel_flag("run_1")

    assert result["redis"] is False
    assert "redis down" in result["error"]


@pytest.mark.asyncio
async def test_remove_queued_job_removes_arq_job(monkeypatch):
    calls = []

    class FakeRedis:
        async def zrem(self, *args):
            calls.append(("zrem", args))
            return 1

        async def delete(self, *args):
            calls.append(("delete", args))
            return 1

    async def fake_create_pool(*_args, **_kwargs):
        return FakeRedis()

    monkeypatch.setattr("api.control_imports.create_pool", fake_create_pool)

    result = await _remove_queued_job({"metrics": {"queue": "arq:NPI", "job_id": "job_1"}})

    assert result == {
        "redis": True,
        "queue": "arq:NPI",
        "job_id": "job_1",
        "removed": True,
        "deleted_job_key": True,
    }
    assert calls == [("zrem", ("arq:NPI", "job_1")), ("delete", ("arq:job:job_1",))]


@pytest.mark.asyncio
async def test_create_import_run_persists_enqueued_state(monkeypatch):
    statements = []

    class FakeDb:
        async def execute(self, statement):
            statements.append(statement)

    async def fake_find(_idempotency_key):
        return None

    async def fake_enqueue(row):
        return {
            "status": "queued",
            "phase_detail": "enqueued",
            "heartbeat_at": row["heartbeat_at"],
            "progress": {"message": "queued"},
            "metrics": {"enqueue_adapter": "arq_single_job", "queue": "arq:NPI"},
            "error": None,
        }

    monkeypatch.setattr(control_imports, "db", FakeDb())
    monkeypatch.setattr(control_imports, "find_active_run_by_idempotency_key", fake_find)
    monkeypatch.setattr(control_imports, "_enqueue_import_start", fake_enqueue)

    row, created = await create_import_run(
        {
            "run_id": "run_create",
            "importer": "npi",
            "params": {"test_mode": True},
            "idempotency_key": "idem-1",
        }
    )

    assert created is True
    assert row["run_id"] == "run_create"
    assert row["phase_detail"] == "enqueued"
    assert row["metrics"]["queue"] == "arq:NPI"
    assert len(statements) == 2


@pytest.mark.asyncio
async def test_create_import_run_returns_active_run_after_integrity_race(monkeypatch):
    calls = {"find": 0}
    active = {"run_id": "run_existing", "status": "queued"}

    class FakeDb:
        async def execute(self, _statement):
            raise IntegrityError("insert", {}, Exception("duplicate"))

    async def fake_find(_idempotency_key):
        calls["find"] += 1
        return None if calls["find"] == 1 else active

    monkeypatch.setattr(control_imports, "db", FakeDb())
    monkeypatch.setattr(control_imports, "find_active_run_by_idempotency_key", fake_find)

    row, created = await create_import_run(
        {
            "run_id": "run_duplicate",
            "importer": "npi",
            "idempotency_key": "idem-race",
        }
    )

    assert created is False
    assert row == active


@pytest.mark.asyncio
async def test_request_cancel_finishes_pending_adapter_run(monkeypatch):
    calls = {"get": 0}
    current = {
        "run_id": "run_pending",
        "status": "queued",
        "progress": {"pct": 0},
        "metrics": {"enqueue_adapter": "pending"},
        "finished_at": None,
    }

    class FakeResult:
        def scalar_one_or_none(self):
            return None

    class FakeDb:
        async def execute(self, _statement):
            return FakeResult()

    async def fake_get(_run_id):
        calls["get"] += 1
        if calls["get"] == 1:
            return current
        return {
            **current,
            "status": "canceled",
            "metrics": {
                "enqueue_adapter": "pending",
                "cancel_signal": {"redis": False, "pending_adapter": True},
            },
        }

    monkeypatch.setattr(control_imports, "db", FakeDb())
    monkeypatch.setattr(control_imports, "get_import_run", fake_get)

    result = await control_imports.request_cancel("run_pending")

    assert result["status"] == "canceled"
    assert result["metrics"]["cancel_signal"] == {"redis": False, "pending_adapter": True}


@pytest.mark.asyncio
async def test_request_cancel_finishes_queued_arq_run(monkeypatch):
    calls = {"get": 0}
    current = {
        "run_id": "run_queued",
        "status": "queued",
        "progress": {"pct": 0},
        "metrics": {"enqueue_adapter": "arq_single_job", "queue": "arq:NPI", "job_id": "job_1"},
        "finished_at": None,
    }

    class FakeResult:
        def scalar_one_or_none(self):
            return None

    class FakeDb:
        async def execute(self, _statement):
            return FakeResult()

    async def fake_get(_run_id):
        calls["get"] += 1
        if calls["get"] == 1:
            return current
        return {
            **current,
            "status": "canceled",
            "metrics": {
                **current["metrics"],
                "cancel_signal": {
                    "redis": True,
                    "queue": "arq:NPI",
                    "job_id": "job_1",
                    "removed": True,
                    "deleted_job_key": True,
                },
            },
        }

    async def fake_remove_queued_job(run):
        assert run == current
        return {"redis": True, "queue": "arq:NPI", "job_id": "job_1", "removed": True, "deleted_job_key": True}

    monkeypatch.setattr(control_imports, "db", FakeDb())
    monkeypatch.setattr(control_imports, "get_import_run", fake_get)
    monkeypatch.setattr(control_imports, "_remove_queued_job", fake_remove_queued_job)

    result = await control_imports.request_cancel("run_queued")

    assert result["status"] == "canceled"
    assert result["metrics"]["cancel_signal"]["removed"] is True


@pytest.mark.asyncio
async def test_request_cancel_marks_running_run_canceling(monkeypatch):
    calls = {"get": 0}
    current = {
        "run_id": "run_running",
        "importer": "npi",
        "status": "running",
        "progress": {"pct": 25},
        "metrics": {},
        "finished_at": None,
    }

    class FakeResult:
        def scalar_one_or_none(self):
            return None

    class FakeDb:
        async def execute(self, _statement):
            return FakeResult()

    async def fake_get(_run_id):
        calls["get"] += 1
        if calls["get"] == 1:
            return current
        return {
            **current,
            "status": "canceling",
            "metrics": {"cancel_signal": {"redis": True, "key": "cancel:run_running", "ttl_seconds": 10}},
        }

    async def fake_set_cancel_flag(run_id):
        return {"redis": True, "key": f"cancel:{run_id}", "ttl_seconds": 10}

    monkeypatch.setattr(control_imports, "db", FakeDb())
    monkeypatch.setattr(control_imports, "get_import_run", fake_get)
    monkeypatch.setattr(control_imports, "_set_cancel_flag", fake_set_cancel_flag)

    result = await control_imports.request_cancel("run_running")

    assert result["status"] == "canceling"
    assert result["metrics"]["cancel_signal"]["redis"] is True


@pytest.mark.asyncio
async def test_request_cancel_rejects_running_non_cancelable_importer(monkeypatch):
    async def fake_get(_run_id):
        return {
            "run_id": "run_claims",
            "importer": "claims-pricing",
            "status": "running",
            "progress": {"pct": 25},
            "metrics": {},
        }

    monkeypatch.setattr(control_imports, "get_import_run", fake_get)

    with pytest.raises(ValueError, match="does not support canceling active runs"):
        await control_imports.request_cancel("run_claims")


@pytest.mark.asyncio
async def test_finalize_import_run_enqueues_finish_and_marks_finalizing(monkeypatch):
    current = {
        "run_id": "run_claims",
        "importer": "claims-pricing",
        "status": "running",
        "params": {"test": True, "import_id": "20260603"},
        "metrics": {"total_chunks": 1},
        "import_id": None,
    }
    executed = []

    class FakeResult:
        def scalar_one_or_none(self):
            return current

    class FakeDb:
        async def execute(self, statement):
            executed.append(statement)
            return FakeResult()

    async def fake_finish_main(**kwargs):
        return {"ok": True, "queued": True, **kwargs}

    monkeypatch.setattr(control_imports, "db", FakeDb())
    monkeypatch.setattr(control_imports, "_finish_function", lambda _importer: fake_finish_main)

    result = await finalize_import_run("run_claims", {})

    assert result["run_id"] == "run_claims"
    update_values = executed[1].compile().params
    assert update_values["status"] == "finalizing"
    assert update_values["import_id"] == "20260603"
    assert update_values["metrics"]["finalize"]["run_id"] == "run_claims"


@pytest.mark.asyncio
async def test_control_importers_endpoint(monkeypatch):
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "secret")
    monkeypatch.setattr(
        control,
        "importer_registry",
        lambda: [{"name": "ptg", "engine": "healthcare-mrf-api"}],
    )

    response = await control.control_importers(authed_request())
    payload = json.loads(response.body)

    assert payload == {"items": [{"name": "ptg", "engine": "healthcare-mrf-api"}], "next_cursor": None}


@pytest.mark.asyncio
async def test_control_ptg_parse_toc_preview_endpoint(monkeypatch):
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "secret")
    monkeypatch.setattr(
        control,
        "parse_ptg_toc_preview",
        lambda _payload: {"status": "parsed", "counts": {"entries": 1}, "items": []},
    )
    request = authed_request(json={"toc": {}})

    response = await control.control_ptg_parse_toc_preview(request)
    payload = json.loads(response.body)

    assert payload["status"] == "parsed"


@pytest.mark.asyncio
async def test_control_create_import_returns_created(monkeypatch):
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "secret")
    async def fake_create(payload):
        return {"run_id": "run_1", "importer": payload["importer"], "status": "queued"}, True

    monkeypatch.setattr(control, "create_import_run", fake_create)
    request = authed_request(json={"importer": "ptg"})

    response = await control.control_create_import(request)
    payload = json.loads(response.body)

    assert response.status == 201
    assert payload["run_id"] == "run_1"
    assert payload["status"] == "queued"


@pytest.mark.asyncio
async def test_control_create_import_returns_conflict_for_duplicate(monkeypatch):
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "secret")
    async def fake_create(_payload):
        return {"run_id": "run_existing", "status": "queued"}, False

    monkeypatch.setattr(control, "create_import_run", fake_create)
    request = authed_request(json={"importer": "ptg"})

    response = await control.control_create_import(request)
    payload = json.loads(response.body)

    assert response.status == 409
    assert payload["run_id"] == "run_existing"


@pytest.mark.asyncio
async def test_control_list_imports_uses_cursor_list_envelope(monkeypatch):
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "secret")
    async def fake_list_import_runs(**_kwargs):
        return [{"run_id": "run_1", "status": "queued"}]

    monkeypatch.setattr(control, "list_import_runs", fake_list_import_runs)
    request = authed_request(args={})

    response = await control.control_list_imports(request)
    payload = json.loads(response.body)

    assert response.status == 200
    assert payload == {"items": [{"run_id": "run_1", "status": "queued"}], "next_cursor": None}


@pytest.mark.asyncio
async def test_control_cancel_import_returns_accepted(monkeypatch):
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "secret")
    async def fake_cancel(run_id):
        return {"run_id": run_id, "status": "canceling"}

    monkeypatch.setattr(control, "request_cancel", fake_cancel)

    response = await control.control_cancel_import(authed_request(), "run_1")
    payload = json.loads(response.body)

    assert response.status == 202
    assert payload == {"run_id": "run_1", "status": "canceling"}


@pytest.mark.asyncio
async def test_control_finalize_import_returns_accepted(monkeypatch):
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "secret")

    async def fake_finalize(run_id, payload):
        return {"run_id": run_id, "status": "finalizing", "params": payload}

    monkeypatch.setattr(control, "finalize_import_run", fake_finalize)

    response = await control.control_finalize_import(authed_request(json={"import_id": "20260603"}), "run_1")
    payload = json.loads(response.body)

    assert response.status == 202
    assert payload == {"run_id": "run_1", "status": "finalizing", "params": {"import_id": "20260603"}}
