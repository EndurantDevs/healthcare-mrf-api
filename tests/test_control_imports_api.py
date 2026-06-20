# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import importlib.util
import json
import types
from pathlib import Path

import pytest
import sqlalchemy as sa
from sqlalchemy.exc import IntegrityError
from sanic.exceptions import Forbidden
from sanic.exceptions import BadRequest, NotFound

from db.models import ImportRun

from api import control
from api import metrics as api_metrics
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
from process.ext import utils as process_utils

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
    assert items["mrf-source-discovery"]["family"] == "mrf"
    assert items["mrf-source-discovery"]["enqueue_adapter"] == "arq_single_job"
    assert items["mrf-source-discovery"]["schedulable"] is True
    assert items["address-archive-v2-migrate"]["family"] == "provider"
    assert items["address-archive-v2-migrate"]["enqueue_adapter"] == "arq_single_job"
    assert items["address-archive-v2-migrate"]["queue"] == "arq:AddressArchive"
    assert items["address-archive-v2-migrate"]["cancelable"] is True
    assert items["ptg-address"]["family"] == "provider"
    assert items["ptg-address"]["enqueue_adapter"] == "arq_single_job"
    assert items["ptg-address"]["queue"] == "arq:PTGAddress"
    assert items["code-sets"]["enqueue_adapter"] == "arq_single_job"
    assert items["ms-drg"]["family"] == "reference"
    assert items["ms-drg"]["enqueue_adapter"] == "arq_single_job"
    assert items["clinical-reference"]["enqueue_adapter"] == "arq_single_job"
    assert items["geo"]["enqueue_adapter"] == "arq_single_job"
    assert items["geo-census"]["enqueue_adapter"] == "arq_single_job"
    assert items["plan-attributes"]["enqueue_adapter"] == "arq_single_job"
    assert items["npi"]["schedulable"] is True
    assert items["npi"]["depends_on"] == ["nucc"]
    assert items["ptg"]["cancelable"] is True
    assert items["npi"]["cancelable"] is True
    assert items["claims-pricing"]["cancelable"] is False
    assert any(param["name"] == "toc_url" and param["multiple"] for param in items["ptg"]["params_schema"])
    assert any(param["name"] == "check_urls" and param["is_flag"] for param in items["mrf-source-discovery"]["params_schema"])
    assert any(param["name"] == "concurrency" and param["type"] == "integer" for param in items["mrf-source-discovery"]["params_schema"])
    assert any(param["name"] == "crawl_target_limit" and param["type"] == "integer" for param in items["mrf-source-discovery"]["params_schema"])
    assert any(param["name"] == "source_entity_types" and param["type"] == "text" for param in items["mrf-source-discovery"]["params_schema"])
    assert any(param["name"] == "source_payer_query" and param["type"] == "text" for param in items["mrf-source-discovery"]["params_schema"])
    assert any(param["name"] == "probe_files" and param["is_flag"] for param in items["mrf-source-discovery"]["params_schema"])
    assert any(param["name"] == "file_probe_limit" and param["type"] == "integer" for param in items["mrf-source-discovery"]["params_schema"])
    assert any(param["name"] == "file_probe_types" and param["type"] == "text" for param in items["mrf-source-discovery"]["params_schema"])
    assert any(param["name"] == "file_probe_entity_types" and param["type"] == "text" for param in items["mrf-source-discovery"]["params_schema"])
    assert any(param["name"] == "file_probe_payer_query" and param["type"] == "text" for param in items["mrf-source-discovery"]["params_schema"])
    assert any(param["name"] == "include_relationships" and param["type"] == "boolean" for param in items["ms-drg"]["params_schema"])
    assert any(param["name"] == "relationship_page_limit" and param["type"] == "integer" for param in items["ms-drg"]["params_schema"])
    assert any(param["name"] == "dry_run" and param["is_flag"] for param in items["address-archive-v2-migrate"]["params_schema"])
    assert any(param["name"] == "sample_limit" and param["type"] == "integer" for param in items["address-archive-v2-migrate"]["params_schema"])
    assert any(param["name"] == "source_concurrency" and param["type"] == "integer" for param in items["openaddresses"]["params_schema"])
    assert any(param["name"] == "backfill_concurrency" and param["type"] == "integer" for param in items["openaddresses"]["params_schema"])
    assert any(param["name"] == "backfill_zip_prefix_length" and param["type"] == "integer" for param in items["openaddresses"]["params_schema"])
    assert any(param["name"] == "zip_restore_concurrency" and param["type"] == "integer" for param in items["openaddresses"]["params_schema"])
    assert any(param["name"] == "zip_restore_shards" and param["type"] == "integer" for param in items["openaddresses"]["params_schema"])
    assert items["openaddresses"]["family"] == "geo"
    assert any(param["name"] == "import_id" and param["type"] == "text" for param in items["openaddresses"]["params_schema"])
    assert any(param["name"] == "local_files" and param["multiple"] for param in items["openaddresses"]["params_schema"])
    assert any(param["name"] == "resume_stage" and param["is_flag"] for param in items["openaddresses"]["params_schema"])


def test_control_wrapped_publish_importers_request_shutdown():
    facility_payload = control_imports._adapter_payload(
        control_imports._SINGLE_JOB_ADAPTERS["facility-anchors"],
        {"run_id": "run_facility", "importer": "facility-anchors", "family": "other"},
        {},
    )
    entity_payload = control_imports._adapter_payload(
        control_imports._SINGLE_JOB_ADAPTERS["entity-address-unified"],
        {"run_id": "run_entity", "importer": "entity-address-unified", "family": "provider"},
        {},
    )
    npi_payload = control_imports._adapter_payload(
        control_imports._SINGLE_JOB_ADAPTERS["npi"],
        {"run_id": "run_npi", "importer": "npi", "family": "provider"},
        {},
    )
    openaddresses_payload = control_imports._adapter_payload(
        control_imports._SINGLE_JOB_ADAPTERS["openaddresses"],
        {"run_id": "run_openaddresses", "importer": "openaddresses", "family": "provider"},
        {},
    )

    assert facility_payload["run_shutdown"] is True
    assert entity_payload["run_shutdown"] is True
    assert openaddresses_payload["run_shutdown"] is True
    assert npi_payload["run_shutdown"] is False


def test_openaddresses_adapter_preserves_parallel_load_params():
    payload = control_imports._adapter_payload(
        control_imports._SINGLE_JOB_ADAPTERS["openaddresses"],
        {"run_id": "run_openaddresses", "importer": "openaddresses", "family": "provider"},
        {
            "test_mode": True,
            "source_concurrency": 8,
            "max_files": 20,
            "local_files": ["/tmp/a.geojson", "/tmp/b.geojson"],
            "import_id": "oa_dev_20260619",
            "resume_stage": True,
            "load_only": True,
            "batch_size": 10000,
            "backfill_concurrency": 4,
            "backfill_zip_prefix_length": 2,
            "zip_restore_concurrency": 6,
            "zip_restore_shards": 48,
        },
    )

    assert payload["run_shutdown"] is True
    assert payload["task"] == {
        "test_mode": True,
        "source_concurrency": 8,
        "max_files": 20,
        "local_files": ["/tmp/a.geojson", "/tmp/b.geojson"],
        "import_id": "oa_dev_20260619",
        "resume_stage": True,
        "load_only": True,
        "batch_size": 10000,
        "backfill_concurrency": 4,
        "backfill_zip_prefix_length": 2,
        "zip_restore_concurrency": 6,
        "zip_restore_shards": 48,
    }


def test_mrf_adapter_preserves_chunking_params():
    payload = control_imports._adapter_payload(
        control_imports._SINGLE_JOB_ADAPTERS["mrf"],
        {"run_id": "run_mrf", "importer": "mrf", "family": "pricing"},
        {
            "test_mode": True,
            "mrf_file_chunking": "all",
            "mrf_chunk_target_mb": 128,
            "ignored": "value",
        },
    )

    assert payload == {
        "test_mode": True,
        "run_id": "run_mrf",
        "mrf_file_chunking": "all",
        "mrf_chunk_target_mb": 128,
    }


@pytest.mark.asyncio
async def test_node_health_reports_degraded_when_redis_fails(monkeypatch):
    async def fake_database_check():
        return {"ok": True}

    monkeypatch.setattr(control_imports, "_database_check", fake_database_check)
    monkeypatch.setattr(control_imports, "_redis_check", lambda: {"ok": False, "error": "redis unavailable"})
    monkeypatch.setattr(
        control_imports,
        "_worker_health",
        lambda: {"arq:PTG": {"worker_class": "process.PTG", "role": "start", "running": True}},
    )
    monkeypatch.setattr(control_imports, "_queue_depths", lambda: {"arq:PTG": 0})

    payload = await control_imports.node_health()

    assert payload["status"] == "degraded"
    assert payload["checks"]["redis"] == {"ok": False, "error": "redis unavailable"}
    assert payload["failing_checks"] == ["redis"]


def test_normalize_run_accepts_plain_dict():
    row = {"run_id": "run_1", "status": "queued"}

    assert normalize_run(row) == row


@pytest.mark.asyncio
async def test_render_prometheus_metrics_includes_engine_health_and_runs(monkeypatch):
    async def fake_node_health():
        return {
            "node_id": "local_mrf",
            "status": "ok",
            "disk": {"free": 123},
            "queue_depth": {"arq:PTG": 2},
            "workers": {"arq:PTG": {"running": True}},
        }

    async def fake_active_run_counts():
        return {"running": 1}, {"ptg": 1}

    monkeypatch.setattr(api_metrics, "node_health", fake_node_health)
    monkeypatch.setattr(api_metrics, "_active_run_counts", fake_active_run_counts)

    body = await api_metrics.render_prometheus_metrics()

    assert 'hp_mrf_api_node_health{node_id="local_mrf",status="ok"} 1.000000' in body
    assert 'hp_mrf_api_active_runs_by_status{status="running"} 1.000000' in body
    assert 'hp_mrf_api_active_runs_by_importer{importer="ptg"} 1.000000' in body
    assert 'hp_mrf_api_queue_depth{queue="arq:PTG"} 2.000000' in body
    assert 'hp_mrf_api_worker_running{queue="arq:PTG"} 1.000000' in body


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
    assert kwargs == {"_queue_name": "arq:ClaimsPricing", "_job_id": "claims_start_run_control"}


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
    assert kwargs == {"_queue_name": "arq:PTG", "_job_id": "ptg_start_run_ptg"}


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
    args, kwargs = calls[0]
    assert args[0] == "control_single_job_start"
    assert args[1]["run_id"] == "run_nucc"
    assert args[1]["target_module"] == "process.nucc"
    assert args[1]["target_function"] == "process_data"
    assert args[1]["task"] == {"test_mode": True}
    assert kwargs == {"_queue_name": "arq:NUCC", "_max_tries": 1}


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
    args, kwargs = calls[0]
    assert args[0] == "control_single_job_start"
    assert args[1]["run_id"] == "run_lodes"
    assert args[1]["target_module"] == "process.lodes"
    assert args[1]["target_function"] == "process_data"
    assert args[1]["task"] == {"test_mode": True, "import_id": "smoke_lodes"}
    assert kwargs == {"_queue_name": "arq:LODES", "_max_tries": 1}


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
    args, kwargs = calls[0]
    assert args[0] == "control_single_job_start"
    assert args[1]["run_id"] == "run_codes"
    assert args[1]["target_module"] == "process.clinical_reference"
    assert args[1]["target_function"] == "main"
    assert args[1]["call_style"] == "kwargs"
    assert args[1]["task"] == {"test_mode": True, "sources": "icd10cm", "import_id": "smoke_clinical"}
    assert kwargs == {"_queue_name": "arq:ClinicalReference", "_max_tries": 1}


@pytest.mark.asyncio
async def test_enqueue_import_start_wraps_ms_drg_importer(monkeypatch):
    calls = []

    class FakeJob:
        job_id = "job_ms_drg"

    class FakeRedis:
        async def enqueue_job(self, *args, **kwargs):
            calls.append((args, kwargs))
            return FakeJob()

    async def fake_create_pool(*_args, **_kwargs):
        return FakeRedis()

    monkeypatch.setattr("api.control_imports.create_pool", fake_create_pool)
    row = {
        "run_id": "run_ms_drg",
        "importer": "ms-drg",
        "params": {"test_mode": True, "include_relationships": True, "relationship_page_limit": 1},
    }

    result = await _enqueue_import_start(row)

    assert result["status"] == "queued"
    args, kwargs = calls[0]
    assert args[0] == "control_single_job_start"
    assert args[1]["run_id"] == "run_ms_drg"
    assert args[1]["target_module"] == "process.ms_drg"
    assert args[1]["target_function"] == "main"
    assert args[1]["call_style"] == "kwargs"
    assert args[1]["task"] == {"test_mode": True, "include_relationships": True, "relationship_page_limit": 1}
    assert kwargs == {"_queue_name": "arq:MSDRG", "_max_tries": 1}


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


def _load_import_run_migration():
    path = Path(__file__).resolve().parents[1] / "alembic" / "versions" / "20260610120000_add_import_run_table.py"
    spec = importlib.util.spec_from_file_location("migration_add_import_run_table", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _OpRecorder:
    def __init__(self):
        self.tables = {}
        self.indexes = {}

    def create_table(self, name, *items, schema=None):
        self.tables[name] = {
            "columns": [item for item in items if isinstance(item, sa.Column)],
            "schema": schema,
        }

    def create_index(self, name, table_name, columns, schema=None, unique=False, **kwargs):
        self.indexes[name] = {"table": table_name, "columns": columns, "schema": schema, "unique": unique, **kwargs}


def test_import_run_migration_mirrors_model(monkeypatch):
    module = _load_import_run_migration()
    assert module.revision == "20260610120000_add_import_run_table"
    assert module.down_revision == "20260319193000_extend_geo_zip_census_profile_metrics"

    monkeypatch.delenv("DB_SCHEMA", raising=False)
    recorder = _OpRecorder()
    monkeypatch.setattr(module, "op", recorder)

    module.upgrade()

    table = recorder.tables["import_run"]
    assert table["schema"] == ImportRun.__table__.schema
    assert [column.name for column in table["columns"]] == list(ImportRun.__table__.columns.keys())
    assert {name for name in recorder.indexes} == {spec["name"] for spec in ImportRun.__my_additional_indexes__}
    idempotency_idx = recorder.indexes["import_run_active_idempotency_idx"]
    assert idempotency_idx["table"] == "import_run"
    assert idempotency_idx["unique"] is True
    assert idempotency_idx["columns"] == ["idempotency_key"]
    assert str(idempotency_idx["postgresql_where"]) == (
        "status IN ('queued', 'starting', 'running', 'finalizing', 'canceling')"
    )


def test_control_blueprint_registers_import_run_ensure_listener():
    listeners = [
        item
        for item in control.blueprint._future_listeners
        if item.listener is control.control_ensure_import_run_table
    ]

    assert len(listeners) == 1
    assert listeners[0].event == "before_server_start"


@pytest.mark.asyncio
async def test_control_startup_listener_runs_import_run_ensure(monkeypatch):
    calls = []

    async def fake_ensure():
        calls.append(True)

    monkeypatch.setattr(control, "ensure_import_run_table", fake_ensure)

    await control.control_ensure_import_run_table(None, None)

    assert calls == [True]


@pytest.mark.asyncio
async def test_import_run_ensure_is_memoized_and_uses_advisory_lock(monkeypatch):
    calls = []

    class FakeConnection:
        async def execute(self, statement, params=None):
            calls.append(("execute", str(statement), params))

        async def run_sync(self, fn, **kwargs):
            calls.append(("run_sync", fn, kwargs))

    class FakeBegin:
        async def __aenter__(self):
            return FakeConnection()

        async def __aexit__(self, *_exc):
            return False

    class FakeEngine:
        def begin(self):
            return FakeBegin()

    class FakeDb:
        engine = FakeEngine()

        async def connect(self):
            calls.append(("connect",))

    monkeypatch.setattr(control_imports, "_IMPORT_RUN_ENSURED", False)
    monkeypatch.setattr(control_imports, "db", FakeDb())

    await control_imports.ensure_import_run_table()
    await control_imports.ensure_import_run_table()

    assert calls.count(("connect",)) == 1
    assert any(
        item[0] == "execute"
        and "pg_advisory_xact_lock" in item[1]
        and item[2] == {"lock_key": control_imports._IMPORT_RUN_ADVISORY_LOCK_KEY}
        for item in calls
    )
    assert len([item for item in calls if item[0] == "run_sync"]) == 1


@pytest.mark.asyncio
async def test_worker_db_startup_runs_import_run_ensure(monkeypatch):
    calls = []

    async def fake_init(_db):
        calls.append("init")

    async def fake_ensure():
        calls.append("ensure")

    monkeypatch.setattr(process_utils, "my_init_db", fake_init)
    monkeypatch.setattr(control_imports, "ensure_import_run_table", fake_ensure)

    await process_utils.db_startup({})

    assert calls == ["init", "ensure"]


@pytest.mark.asyncio
async def test_import_run_request_paths_do_not_run_ddl(monkeypatch):
    async def fail_ensure():
        raise AssertionError("ensure_import_run_table must not run on the request path")

    class FakeScalars:
        def all(self):
            return []

    class FakeResult:
        def scalars(self):
            return FakeScalars()

        def scalar_one_or_none(self):
            return None

    class FakeDb:
        async def execute(self, _statement):
            return FakeResult()

    async def fake_enqueue(row):
        return {
            "status": "queued",
            "phase_detail": "enqueued",
            "heartbeat_at": row["heartbeat_at"],
            "progress": {"message": "queued"},
            "metrics": {"enqueue_adapter": "arq_single_job", "queue": "arq:NPI"},
            "error": None,
        }

    monkeypatch.setattr(control_imports, "ensure_import_run_table", fail_ensure)
    monkeypatch.setattr(control_imports, "db", FakeDb())
    monkeypatch.setattr(control_imports, "_enqueue_import_start", fake_enqueue)

    assert await control_imports.list_import_runs() == []
    assert await control_imports.get_import_run("run_missing") is None
    assert await control_imports.find_active_run_by_idempotency_key("idem-1") is None
    row, created = await control_imports.create_import_run(
        {"importer": "npi", "params": {}, "idempotency_key": "idem-1"}
    )
    assert created is True
    assert row["status"] == "queued"


@pytest.mark.asyncio
async def test_list_import_runs_page_returns_next_cursor(monkeypatch):
    now = control_imports.utc_now()
    rows = [
        types.SimpleNamespace(
            run_id=f"run_{idx}",
            engine="healthcare-mrf-api",
            node_id="node_a",
            importer="ptg",
            family="ptg",
            status="succeeded",
            phase_detail="done",
            params={},
            idempotency_key=None,
            triggered_by="api",
            schedule_id=None,
            subscription_id=None,
            source_file_import_id=None,
            created_at=now - control_imports.dt.timedelta(minutes=idx),
            started_at=None,
            finished_at=None,
            heartbeat_at=now - control_imports.dt.timedelta(minutes=idx),
            progress={},
            metrics={},
            error=None,
            snapshot_id=None,
            import_id=None,
            retry_of_run_id=None,
        )
        for idx in range(3)
    ]

    class FakeScalars:
        def all(self):
            return rows

    class FakeResult:
        def scalars(self):
            return FakeScalars()

    class FakeDb:
        async def execute(self, _statement):
            return FakeResult()

    monkeypatch.setattr(control_imports, "db", FakeDb())

    page = await control_imports.list_import_runs_page(limit=2)
    cursor_created_at, cursor_run_id = control_imports._decode_import_run_cursor(page["next_cursor"])

    assert [item["run_id"] for item in page["items"]] == ["run_0", "run_1"]
    assert cursor_run_id == "run_1"
    assert cursor_created_at == rows[1].created_at


@pytest.mark.asyncio
async def test_create_import_run_persists_enqueued_state(monkeypatch):
    statements = []

    class FakeDb:
        async def execute(self, statement):
            statements.append(statement)

    async def fake_find(_idempotency_key):
        return None

    async def fake_find_importer(_importer):
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
    monkeypatch.setattr(control_imports, "find_active_run_by_importer", fake_find_importer)
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

    async def fake_find_importer(_importer):
        return None

    monkeypatch.setattr(control_imports, "db", FakeDb())
    monkeypatch.setattr(control_imports, "find_active_run_by_idempotency_key", fake_find)
    monkeypatch.setattr(control_imports, "find_active_run_by_importer", fake_find_importer)

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
async def test_create_import_run_returns_active_same_importer_run(monkeypatch):
    active = {"run_id": "run_active_npi", "status": "running", "importer": "npi"}

    async def fake_find_idem(_idempotency_key):
        return None

    async def fake_find_importer(_importer):
        return active

    async def fail_enqueue(_row):
        raise AssertionError("enqueue should not run when importer already has an active run")

    monkeypatch.setattr(control_imports, "find_active_run_by_idempotency_key", fake_find_idem)
    monkeypatch.setattr(control_imports, "find_active_run_by_importer", fake_find_importer)
    monkeypatch.setattr(control_imports, "_enqueue_import_start", fail_enqueue)

    row, created = await create_import_run({"run_id": "run_duplicate_npi", "importer": "npi"})

    assert created is False
    assert row == active


@pytest.mark.asyncio
async def test_create_import_run_allows_parallel_source_file_ptg(monkeypatch):
    statements = []
    importer_checks = []

    class FakeDb:
        async def execute(self, statement):
            statements.append(statement)

    async def fake_find_idem(_idempotency_key):
        return None

    async def fail_find_importer(importer):
        importer_checks.append(importer)
        raise AssertionError("source-file PTG runs should not use the importer-wide singleton")

    async def fake_enqueue(row):
        return {
            "status": "queued",
            "phase_detail": "enqueued",
            "heartbeat_at": row["heartbeat_at"],
            "progress": {"message": "queued"},
            "metrics": {"enqueue_adapter": "arq_single_job", "queue": "arq:PTG"},
            "error": None,
        }

    monkeypatch.setattr(control_imports, "db", FakeDb())
    monkeypatch.setattr(control_imports, "find_active_run_by_idempotency_key", fake_find_idem)
    monkeypatch.setattr(control_imports, "find_active_run_by_importer", fail_find_importer)
    monkeypatch.setattr(control_imports, "_enqueue_import_start", fake_enqueue)

    row, created = await create_import_run(
        {
            "run_id": "run_ptg_source_file",
            "importer": "ptg",
            "idempotency_key": "source-file-1",
            "source_file_import_id": "source-file-1",
        }
    )

    assert created is True
    assert row["run_id"] == "run_ptg_source_file"
    assert row["metrics"]["queue"] == "arq:PTG"
    assert row["source_file_import_id"] == "source-file-1"
    assert importer_checks == []
    assert len(statements) == 2


@pytest.mark.asyncio
async def test_create_import_run_serializes_unsourced_ptg(monkeypatch):
    active = {"run_id": "run_active_ptg", "status": "running", "importer": "ptg"}

    async def fake_find_idem(_idempotency_key):
        return None

    async def fake_find_importer(_importer):
        return active

    async def fail_enqueue(_row):
        raise AssertionError("enqueue should not run for unsourced PTG while PTG is active")

    monkeypatch.setattr(control_imports, "find_active_run_by_idempotency_key", fake_find_idem)
    monkeypatch.setattr(control_imports, "find_active_run_by_importer", fake_find_importer)
    monkeypatch.setattr(control_imports, "_enqueue_import_start", fail_enqueue)

    row, created = await create_import_run(
        {
            "run_id": "run_duplicate_ptg",
            "importer": "ptg",
            "idempotency_key": "manual-ptg",
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
    to_thread_calls = []
    monkeypatch.setattr(
        control,
        "parse_ptg_toc_preview",
        lambda _payload: {"status": "parsed", "counts": {"entries": 1}, "items": []},
    )
    async def fake_to_thread(func, payload):
        to_thread_calls.append(func)
        return func(payload)

    monkeypatch.setattr(control.asyncio, "to_thread", fake_to_thread)
    request = authed_request(json={"toc": {}})

    response = await control.control_ptg_parse_toc_preview(request)
    payload = json.loads(response.body)

    assert payload["status"] == "parsed"
    assert to_thread_calls == [control.parse_ptg_toc_preview]


@pytest.mark.asyncio
async def test_control_ptg_source_snapshot_promote_endpoint(monkeypatch):
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "secret")
    calls = []

    async def fake_promote(**kwargs):
        calls.append(kwargs)
        return {
            "source_key": kwargs["source_key"],
            "snapshot_id": kwargs["snapshot_id"],
            "previous_snapshot_id": kwargs["expected_current_snapshot_id"],
            "plan_source_count": 2,
        }

    monkeypatch.setattr(control, "promote_ptg2_source_snapshot", fake_promote)
    request = authed_request(
        json={
            "source_key": "source_a",
            "snapshot_id": "snap_new",
            "expected_current_snapshot_id": "snap_old",
        }
    )

    response = await control.control_ptg_source_snapshot_promote(request)
    payload = json.loads(response.body)

    assert response.status == 200
    assert payload["snapshot_id"] == "snap_new"
    assert payload["previous_snapshot_id"] == "snap_old"
    assert calls == [
        {
            "source_key": "source_a",
            "snapshot_id": "snap_new",
            "expected_current_snapshot_id": "snap_old",
        }
    ]


@pytest.mark.asyncio
async def test_control_ptg_source_snapshot_promote_endpoint_maps_stale_pointer_to_conflict(monkeypatch):
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "secret")

    async def fake_promote(**_kwargs):
        raise control.SourceSnapshotConflict("current source snapshot changed")

    monkeypatch.setattr(control, "promote_ptg2_source_snapshot", fake_promote)

    with pytest.raises(control.SanicException) as exc_info:
        await control.control_ptg_source_snapshot_promote(
            authed_request(json={"source_key": "source_a", "snapshot_id": "snap_new"})
        )

    assert exc_info.value.status_code == 409


@pytest.mark.asyncio
async def test_control_ptg_source_snapshot_remove_plan_endpoint(monkeypatch):
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "secret")
    calls = []

    async def fake_remove_plan(**kwargs):
        calls.append(kwargs)
        return {"snapshot_id": kwargs["snapshot_id"], "source_key": kwargs["source_key"], "removable": True}

    monkeypatch.setattr(control, "build_ptg2_source_snapshot_remove_plan", fake_remove_plan)

    response = await control.control_ptg_source_snapshot_remove_plan(
        authed_request(json={"snapshot_id": "snap_old", "source_key": "source_a"})
    )
    payload = json.loads(response.body)

    assert response.status == 200
    assert payload["removable"] is True
    assert calls == [{"snapshot_id": "snap_old", "source_key": "source_a"}]


@pytest.mark.asyncio
async def test_control_ptg_source_snapshot_remove_endpoint(monkeypatch):
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "secret")
    calls = []

    async def fake_remove(**kwargs):
        calls.append(kwargs)
        return {"snapshot_id": kwargs["snapshot_id"], "source_key": kwargs["source_key"], "executed": True}

    monkeypatch.setattr(control, "remove_ptg2_source_snapshot", fake_remove)

    response = await control.control_ptg_source_snapshot_remove(
        authed_request(json={"snapshot_id": "snap_old", "source_key": "source_a"})
    )
    payload = json.loads(response.body)

    assert response.status == 200
    assert payload["executed"] is True
    assert calls == [{"snapshot_id": "snap_old", "source_key": "source_a"}]


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
    calls = []

    async def fake_list_import_runs_page(**kwargs):
        calls.append(kwargs)
        return {"items": [{"run_id": "run_1", "status": "queued"}], "next_cursor": "cursor_2"}

    monkeypatch.setattr(control, "list_import_runs_page", fake_list_import_runs_page)
    request = authed_request(args={"limit": "1", "cursor": "cursor_1"})

    response = await control.control_list_imports(request)
    payload = json.loads(response.body)

    assert response.status == 200
    assert payload == {"items": [{"run_id": "run_1", "status": "queued"}], "next_cursor": "cursor_2"}
    assert calls == [{"status": None, "importer": None, "limit": 1, "cursor": "cursor_1"}]


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
