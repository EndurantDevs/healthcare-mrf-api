# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import importlib.util
import io
import json
import types
from contextlib import asynccontextmanager
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
    assert items["mrf-source-discovery"]["cancelable"] is True
    assert items["address-archive-v2-migrate"]["family"] == "provider"
    assert items["address-archive-v2-migrate"]["enqueue_adapter"] == "arq_single_job"
    assert items["address-archive-v2-migrate"]["queue"] == "arq:AddressArchive"
    assert items["address-archive-v2-migrate"]["cancelable"] is True
    assert items["provider-directory-fhir"]["family"] == "provider"
    assert items["provider-directory-fhir"]["enqueue_adapter"] == "arq_single_job"
    assert items["provider-directory-fhir"]["queue"] == "arq:ProviderDirectoryFHIR"
    assert items["provider-directory-fhir"]["schedulable"] is True
    assert items["provider-directory-fhir"]["cancelable"] is True
    assert any(param["name"] == "source_query" and param["type"] == "text" for param in items["provider-directory-fhir"]["params_schema"])
    assert any(param["name"] == "retest_results_path" and param["type"] == "text" for param in items["provider-directory-fhir"]["params_schema"])
    assert any(param["name"] == "retest_results_url" and param["type"] == "text" for param in items["provider-directory-fhir"]["params_schema"])
    assert any(param["name"] == "credential_config_file" and param["type"] == "text" for param in items["provider-directory-fhir"]["params_schema"])
    provider_directory_refresh_preset = next(
        param
        for param in items["provider-directory-fhir"]["params_schema"]
        if param["name"] == "refresh_preset"
    )
    assert provider_directory_refresh_preset["type"] == "choice"
    assert "monthly-full" in provider_directory_refresh_preset["choices"]
    assert any(param["name"] == "include_supplemental_catalogs" and param["type"] == "boolean" for param in items["provider-directory-fhir"]["params_schema"])
    assert any(param["name"] == "resource_limit" and param["type"] == "integer" for param in items["provider-directory-fhir"]["params_schema"])
    assert any(param["name"] == "resource_deadline_seconds" and param["type"] == "integer" for param in items["provider-directory-fhir"]["params_schema"])
    assert any(param["name"] == "linked_resource_deadline_seconds" and param["type"] == "integer" for param in items["provider-directory-fhir"]["params_schema"])
    assert any(param["name"] == "full_refresh" and param["type"] == "boolean" for param in items["provider-directory-fhir"]["params_schema"])
    assert any(param["name"] == "stale_cleanup" and param["type"] == "boolean" for param in items["provider-directory-fhir"]["params_schema"])
    assert any(param["name"] == "publish_artifacts" and param["type"] == "boolean" for param in items["provider-directory-fhir"]["params_schema"])
    assert any(param["name"] == "publish_after_acquisition" and param["type"] == "boolean" for param in items["provider-directory-fhir"]["params_schema"])
    assert any(param["name"] == "canonical_backfill_only" and param["type"] == "boolean" for param in items["provider-directory-fhir"]["params_schema"])
    assert any(param["name"] == "contact_backfill_only" and param["type"] == "boolean" for param in items["provider-directory-fhir"]["params_schema"])
    assert any(param["name"] == "publish_artifacts_only" and param["type"] == "boolean" for param in items["provider-directory-fhir"]["params_schema"])
    assert any(param["name"] == "publish_artifacts_targets" and param["type"] == "text" for param in items["provider-directory-fhir"]["params_schema"])
    assert any(param["name"] == "publish_corroboration" and param["type"] == "boolean" for param in items["provider-directory-fhir"]["params_schema"])
    assert any(param["name"] == "stream_batch_size" and param["type"] == "integer" for param in items["provider-directory-fhir"]["params_schema"])
    assert any(param["name"] == "bulk_export" and param["type"] == "boolean" for param in items["provider-directory-fhir"]["params_schema"])
    assert any(param["name"] == "source_concurrency" and param["type"] == "integer" for param in items["provider-directory-fhir"]["params_schema"])
    assert any(param["name"] == "concurrency" and param["type"] == "integer" for param in items["provider-directory-fhir"]["params_schema"])
    assert any(param["name"] == "timeout" and param["type"] == "integer" for param in items["provider-directory-fhir"]["params_schema"])
    assert any(param["name"] == "open_only" and param["type"] == "boolean" for param in items["provider-directory-fhir"]["params_schema"])
    assert any(param["name"] == "include_auth_required" and param["type"] == "boolean" for param in items["provider-directory-fhir"]["params_schema"])
    assert items["code-sets"]["enqueue_adapter"] == "arq_single_job"
    assert items["ms-drg"]["family"] == "reference"
    assert items["ms-drg"]["enqueue_adapter"] == "arq_single_job"
    assert items["clinical-reference"]["enqueue_adapter"] == "arq_single_job"
    assert items["terminology-synonyms"]["family"] == "reference"
    assert items["terminology-synonyms"]["enqueue_adapter"] == "arq_single_job"
    assert items["terminology-synonyms"]["queue"] == "arq:TerminologySynonyms"
    assert items["terminology-synonyms"]["depends_on"] == [
        "nucc",
        "code-sets",
        "clinical-reference",
        "claims-pricing",
        "drug-claims",
    ]
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
    assert any(param["name"] == "sync_import_control_catalog" and param["is_flag"] for param in items["mrf-source-discovery"]["params_schema"])
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
    assert any(
        param["name"] == "limit_per_source" and param["type"] == "integer"
        for param in items["entity-address-unified"]["params_schema"]
    )
    assert any(
        param["name"] == "publish" and param["type"] == "boolean"
        for param in items["entity-address-unified"]["params_schema"]
    )
    assert any(
        param["name"] == "serving_only_refresh" and param["type"] == "boolean"
        for param in items["entity-address-unified"]["params_schema"]
    )
    assert any(
        param["name"] == "refresh_mode" and param["type"] == "choice"
        for param in items["entity-address-unified"]["params_schema"]
    )
    entity_provider_directory_scope = next(
        param
        for param in items["entity-address-unified"]["params_schema"]
        if param["name"] == "provider_directory_partial_scope"
    )
    assert entity_provider_directory_scope["type"] == "choice"
    assert entity_provider_directory_scope["choices"] == ["latest-run", "all"]
    assert any(
        param["name"] == "provider_directory_source_batch_size" and param["type"] == "integer"
        for param in items["entity-address-unified"]["params_schema"]
    )
    entity_refresh_mode = next(
        param for param in items["entity-address-unified"]["params_schema"] if param["name"] == "refresh_mode"
    )
    assert "full" in entity_refresh_mode["choices"]
    assert "provider-directory-partial" in entity_refresh_mode["choices"]
    assert "ptg-partial" not in entity_refresh_mode["choices"]
    assert not any(param["name"] == "ptg_source_key" for param in items["entity-address-unified"]["params_schema"])


def test_provider_directory_runtime_contract_preflight_passes():
    from scripts.smoke import provider_directory_runtime_contract

    report = provider_directory_runtime_contract.build_report()

    assert report["ok"] is True, report
    assert report["checks"]["provider_directory_cli"]["ok"] is True
    assert report["checks"]["coverage_audit_cli"]["ok"] is True
    assert report["checks"]["provider_directory_harness_cli"]["ok"] is True
    assert report["checks"]["importer_schema"]["ok"] is True
    assert report["checks"]["monthly_preset"]["ok"] is True
    assert report["checks"]["serving_readiness_contract"]["ok"] is True


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
    provider_directory_payload = control_imports._adapter_payload(
        control_imports._SINGLE_JOB_ADAPTERS["provider-directory-fhir"],
        {"run_id": "run_provider_directory", "importer": "provider-directory-fhir", "family": "provider"},
        {
            "import_resources": True,
            "resource_limit": 100,
            "retest_results_url": "https://raw.githubusercontent.com/hltiunn/provider-directory-db/main/data/retest_results.json",
        },
    )

    assert facility_payload["run_shutdown"] is True
    assert entity_payload["run_shutdown"] is True
    assert openaddresses_payload["run_shutdown"] is True
    assert provider_directory_payload["run_shutdown"] is True
    assert provider_directory_payload["target_module"] == "process.provider_directory_fhir"
    assert provider_directory_payload["task"]["resource_limit"] == 100
    assert (
        provider_directory_payload["task"]["retest_results_url"]
        == "https://raw.githubusercontent.com/hltiunn/provider-directory-db/main/data/retest_results.json"
    )
    assert npi_payload["run_shutdown"] is False


def test_provider_directory_adapter_scopes_retry_lineage():
    adapter = control_imports._SINGLE_JOB_ADAPTERS["provider-directory-fhir"]
    ordinary_row_map = {
        "run_id": "run_ordinary",
        "importer": "provider-directory-fhir",
        "family": "provider",
    }
    retry_row_map = {
        **ordinary_row_map,
        "run_id": "run_retry",
        "retry_of_run_id": "run_original",
        "schedule_id": "schedule_monthly",
        "subscription_id": "subscription_1",
        "source_file_import_id": "source_file_1",
        "import_id": "import_1",
    }

    ordinary_payload = control_imports._adapter_payload(adapter, ordinary_row_map, {"resource_limit": 100})
    retry_payload = control_imports._adapter_payload(
        adapter,
        retry_row_map,
        {
            "resource_limit": 100,
            "provider_directory_pagination_root_run_id": "run_root",
        },
    )

    assert ordinary_payload["task"] == {"test_mode": False, "resource_limit": 100}
    assert retry_payload["task"] == {
        "test_mode": False,
        "resource_limit": 100,
        "retry_of_run_id": "run_original",
        "provider_directory_pagination_root_run_id": "run_root",
    }


@pytest.mark.asyncio
async def test_retry_import_run_merges_provider_directory_retry_params_into_child(monkeypatch):
    current_run_map = {
        "run_id": "run_parent",
        "importer": "provider-directory-fhir",
        "params": {"publish_artifacts": False, "resource_limit": 100},
        "schedule_id": "schedule_monthly",
        "subscription_id": "subscription_1",
        "source_file_import_id": None,
        "import_id": None,
    }
    created_payloads = []

    async def fake_get(run_id):
        assert run_id == "run_parent"
        return current_run_map

    async def fake_create(payload):
        created_payloads.append(payload)
        return payload, True

    monkeypatch.setattr(control_imports, "get_import_run", fake_get)
    monkeypatch.setattr(control_imports, "create_import_run", fake_create)

    child, created = await control_imports.retry_import_run(
        "run_parent",
        {
            "importer": "npi",
            "idempotency_key": "retry_provider_directory",
            "retry_params": {
                "retry_of_run_id": "run_parent",
                "provider_directory_pagination_root_run_id": "run_root",
            },
        },
    )

    assert created is True
    assert child == created_payloads[0]
    assert created_payloads == [
        {
            "importer": "provider-directory-fhir",
            "params": {
                "publish_artifacts": False,
                "resource_limit": 100,
                "retry_of_run_id": "run_parent",
                "provider_directory_pagination_root_run_id": "run_root",
            },
            "triggered_by": "api",
            "idempotency_key": "retry_provider_directory",
            "schedule_id": "schedule_monthly",
            "subscription_id": "subscription_1",
            "source_file_import_id": None,
            "import_id": None,
            "retry_of_run_id": "run_parent",
        }
    ]


@pytest.mark.asyncio
async def test_retry_import_run_without_retry_params_preserves_ordinary_child_params(monkeypatch):
    current_run_map = {
        "run_id": "run_parent",
        "importer": "npi",
        "params": {"test_mode": False, "resource_limit": 100},
        "schedule_id": None,
        "subscription_id": None,
        "source_file_import_id": None,
        "import_id": None,
    }
    created_payloads = []

    async def fake_get(_run_id):
        return current_run_map

    async def fake_create(payload):
        created_payloads.append(payload)
        return payload, True

    monkeypatch.setattr(control_imports, "get_import_run", fake_get)
    monkeypatch.setattr(control_imports, "create_import_run", fake_create)

    await control_imports.retry_import_run("run_parent", {"idempotency_key": "retry_ordinary"})

    assert created_payloads[0]["importer"] == "npi"
    assert created_payloads[0]["params"] == {"test_mode": False, "resource_limit": 100}


@pytest.mark.asyncio
async def test_provider_directory_retry_replaces_stale_param_lineage(monkeypatch):
    current_run_map = {
        "run_id": "run_parent",
        "importer": "provider-directory-fhir",
        "params": {
            "resource_limit": 0,
            "retry_of_run_id": "run_grandparent",
            "provider_directory_pagination_root_run_id": "run_root",
        },
        "schedule_id": None,
        "subscription_id": None,
        "source_file_import_id": None,
        "import_id": None,
    }
    created_payloads = []

    async def fake_get(_run_id):
        return current_run_map

    async def fake_create(payload):
        created_payloads.append(payload)
        return payload, True

    monkeypatch.setattr(control_imports, "get_import_run", fake_get)
    monkeypatch.setattr(control_imports, "create_import_run", fake_create)

    await control_imports.retry_import_run(
        "run_parent",
        {"idempotency_key": "retry_provider_directory_again"},
    )

    assert created_payloads[0]["params"] == {
        "resource_limit": 0,
        "retry_of_run_id": "run_parent",
        "provider_directory_pagination_root_run_id": "run_root",
    }


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


def test_ram_status_excludes_reserved_huge_pages_from_schedulable_memory(monkeypatch):
    meminfo = """\
MemTotal:       197461180 kB
MemAvailable:    83394928 kB
Hugetlb:         92274688 kB
"""

    monkeypatch.setattr("builtins.open", lambda *_args, **_kwargs: io.StringIO(meminfo))

    assert control_imports._ram_status() == {
        "total": 197461180 * 1024,
        "available": 83394928 * 1024,
        "schedulable": (197461180 - 92274688) * 1024,
    }


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
            "source_key": "example_source_a",
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
    assert payload["params"]["source_key"] == "example_source_a"
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
        "params": {"source_key": "example_source_a", "in_network_url": "https://example.com/rates.json.gz"},
    }

    result = await _enqueue_import_start(row)

    assert result["status"] == "queued"
    assert result["metrics"]["queue"] == "arq:PTG"
    args, kwargs = calls[0]
    assert args[0] == "ptg_control_start"
    assert args[1]["run_id"] == "run_ptg"
    assert args[1]["source_file_import_id"] == "source_file_import_1"
    assert args[1]["params"]["source_key"] == "example_source_a"
    assert kwargs["_job_id"] == "ptg_start_run_ptg"
    assert kwargs == {"_queue_name": "arq:PTG", "_job_id": "ptg_start_run_ptg"}


@pytest.mark.asyncio
async def test_enqueue_import_start_honors_ptg_lane(monkeypatch):
    calls = []

    class FakeJob:
        job_id = "job_ptg_lane"

    class FakeRedis:
        async def enqueue_job(self, *args, **kwargs):
            calls.append((args, kwargs))
            return FakeJob()

    async def fake_create_pool(*_args, **_kwargs):
        return FakeRedis()

    monkeypatch.setattr("api.control_imports.create_pool", fake_create_pool)
    row = {
        "run_id": "run_ptg_lane",
        "importer": "ptg",
        "source_file_import_id": "source_file_import_1",
        "params": {
            "source_key": "example_source_a",
            "_expected_queue": "arq:PTGSmall",
            "_expected_worker_class": "process.PTGSmall",
            "resource_class": "small",
        },
    }

    result = await _enqueue_import_start(row)

    assert result["status"] == "queued"
    assert result["metrics"]["queue"] == "arq:PTGSmall"
    assert result["metrics"]["worker_class"] == "process.PTGSmall"
    assert result["metrics"]["resource_class"] == "small"
    args, kwargs = calls[0]
    assert args[0] == "ptg_control_start"
    assert args[1]["params"]["_expected_queue"] == "arq:PTGSmall"
    assert kwargs == {"_queue_name": "arq:PTGSmall", "_job_id": "ptg_start_run_ptg_lane"}


@pytest.mark.asyncio
async def test_enqueue_import_start_rejects_invalid_ptg_lane():
    row = {
        "run_id": "run_bad_ptg_lane",
        "importer": "ptg",
        "params": {"_expected_queue": "arq:NotAQueue"},
    }

    result = await _enqueue_import_start(row)

    assert result["status"] == "failed"
    assert result["error"]["code"] == "invalid_enqueue_adapter"
    assert "unsupported PTG queue" in result["error"]["message"]


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
    assert kwargs == {"_queue_name": "arq:NUCC"}


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
    assert kwargs == {"_queue_name": "arq:LODES"}


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
    assert kwargs == {"_queue_name": "arq:ClinicalReference"}


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
    assert kwargs == {"_queue_name": "arq:MSDRG"}


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
        "deleted_keys": 1,
    }
    assert calls == [
        ("zrem", ("arq:NPI", "job_1")),
        ("delete", ("arq:job:job_1", "arq:retry:job_1", "arq:result:job_1")),
    ]


def _load_import_run_migration():
    path = Path(__file__).resolve().parents[1] / "alembic" / "versions" / "20260610120000_add_import_run_table.py"
    spec = importlib.util.spec_from_file_location("migration_add_import_run_table", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _load_provider_directory_retry_child_migration():
    path = (
        Path(__file__).resolve().parents[1]
        / "alembic"
        / "versions"
        / "20260713235000_import_run_provider_directory_retry_child.py"
    )
    spec = importlib.util.spec_from_file_location("migration_provider_directory_retry_child", path)
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
    assert {name for name in recorder.indexes} == {
        spec["name"]
        for spec in ImportRun.__my_additional_indexes__
        if spec["name"] != "import_run_provider_directory_retry_child_idx"
    }
    idempotency_idx = recorder.indexes["import_run_active_idempotency_idx"]
    assert idempotency_idx["table"] == "import_run"
    assert idempotency_idx["unique"] is True
    assert idempotency_idx["columns"] == ["idempotency_key"]
    assert str(idempotency_idx["postgresql_where"]) == (
        "status IN ('queued', 'starting', 'running', 'finalizing', 'canceling')"
    )


class _MigrationRows:
    def __init__(self, rows, scalar_value=None):
        self.rows = rows
        self.scalar_value = scalar_value

    def mappings(self):
        return self

    def all(self):
        return self.rows

    def scalar(self):
        return self.scalar_value


class _RetryChildMigrationRecorder:
    def __init__(self, duplicate_rows, *, table_exists=True):
        self.duplicate_rows = duplicate_rows
        self.table_exists = table_exists
        self.statements = []
        self.indexes = []

    def get_bind(self):
        return self

    def execute(self, statement, _params=None):
        self.statements.append(statement)
        if "to_regclass" in str(statement):
            table_name = "mrf.import_run" if self.table_exists else None
            return _MigrationRows([], scalar_value=table_name)
        return _MigrationRows(self.duplicate_rows)

    def create_index(self, name, table_name, columns, **kwargs):
        self.indexes.append((name, table_name, columns, kwargs))


def test_provider_directory_retry_child_index_metadata_and_migration(monkeypatch):
    module = _load_provider_directory_retry_child_migration()
    recorder = _RetryChildMigrationRecorder([])
    monkeypatch.setattr(module, "op", recorder)

    module.upgrade()

    model_index = next(
        spec
        for spec in ImportRun.__my_additional_indexes__
        if spec["name"] == "import_run_provider_directory_retry_child_idx"
    )
    assert model_index == {
        "index_elements": ("retry_of_run_id",),
        "name": "import_run_provider_directory_retry_child_idx",
        "unique": True,
        "where": "importer = 'provider-directory-fhir' AND retry_of_run_id IS NOT NULL",
    }
    assert len(recorder.indexes) == 1
    name, table_name, columns, options = recorder.indexes[0]
    assert name == model_index["name"]
    assert table_name == "import_run"
    assert columns == ["retry_of_run_id"]
    assert options["unique"] is True
    assert str(options["postgresql_where"]) == model_index["where"]
    assert "HAVING count(*) > 1" in str(recorder.statements[1])


def test_provider_directory_retry_child_migration_fails_on_duplicates(monkeypatch):
    module = _load_provider_directory_retry_child_migration()
    recorder = _RetryChildMigrationRecorder(
        [{"retry_of_run_id": "run_parent", "child_count": 2}]
    )
    monkeypatch.setattr(module, "op", recorder)

    with pytest.raises(RuntimeError, match=r"run_parent \(2 children\).+no data was deleted"):
        module.upgrade()

    assert recorder.indexes == []


def test_provider_directory_retry_child_migration_skips_partial_schema(monkeypatch):
    module = _load_provider_directory_retry_child_migration()
    recorder = _RetryChildMigrationRecorder([], table_exists=False)
    monkeypatch.setattr(module, "op", recorder)

    module.upgrade()

    assert recorder.indexes == []
    assert len(recorder.statements) == 1


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


def test_normalize_triggered_by_bounds_database_value():
    assert control_imports._normalize_triggered_by("") == "api"
    assert (
        control_imports._normalize_triggered_by("codex-retry-after-read-deadline-deploy")
        == "codex-retry-after-read-deadline"
    )
    assert len(control_imports._normalize_triggered_by("x" * 100)) == control_imports.MAX_TRIGGERED_BY_LENGTH


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
async def test_list_import_runs_page_filters_retry_parent(monkeypatch):
    statements = []

    class FakeScalars:
        def all(self):
            return []

    class FakeResult:
        def scalars(self):
            return FakeScalars()

    class FakeDb:
        async def execute(self, statement):
            statements.append(statement)
            return FakeResult()

    monkeypatch.setattr(control_imports, "db", FakeDb())

    page = await control_imports.list_import_runs_page(
        importer="provider-directory-fhir",
        retry_of_run_id="run_parent",
        limit=2,
    )

    assert page == {"items": [], "next_cursor": None}
    compiled_params = statements[0].compile().params
    assert compiled_params["importer_1"] == "provider-directory-fhir"
    assert compiled_params["retry_of_run_id_1"] == "run_parent"


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


def _provider_directory_acquisition_params(source_id, endpoint_scope, **overrides):
    params_by_name = {
        "import_resources": True,
        "source_ids": [source_id],
        "source_concurrency": 1,
        "provider_directory_endpoint_scope": endpoint_scope,
        "stale_cleanup": False,
        "publish_artifacts": False,
        "publish_after_acquisition": False,
        "publish_corroboration": False,
    }
    params_by_name.update(overrides)
    return params_by_name


def _provider_directory_artifact_params(source_id, **overrides):
    params_by_name = {
        "publish_artifacts_only": True,
        "source_ids": [source_id],
        "import_resources": False,
        "canonical_backfill_only": False,
        "contact_backfill_only": False,
        "seed_only": False,
        "stale_cleanup": False,
        "publish_after_acquisition": False,
        "publish_artifacts": False,
    }
    params_by_name.update(overrides)
    return params_by_name


def _provider_directory_active_run(run_id, source_id, endpoint_scope):
    return {
        "run_id": run_id,
        "status": "running",
        "importer": "provider-directory-fhir",
        "params": _provider_directory_acquisition_params(source_id, endpoint_scope),
    }


def _provider_directory_artifact_run(run_id, source_id):
    return {
        "run_id": run_id,
        "status": "running",
        "importer": "provider-directory-fhir",
        "params": _provider_directory_artifact_params(source_id),
    }


def _legacy_provider_directory_active_run(run_id, source_id, endpoint_scope):
    active_run = _provider_directory_active_run(run_id, source_id, endpoint_scope)
    active_run["params"].pop("provider_directory_endpoint_scope")
    active_run["metrics"] = {"active_source_groups": [{"api_base": endpoint_scope}]}
    return active_run


class _ProviderAdmissionDb:
    def __init__(self, query_rows=None):
        self.events = []
        self.committed = False
        self.query_rows = list(query_rows or [])

    @asynccontextmanager
    async def acquire(self):
        self.events.append(("acquire",))
        try:
            yield self
        finally:
            self.committed = True
            self.events.append(("commit",))

    async def scalar(self, statement, **params):
        self.events.append(("scalar", statement, params))

    async def status(self, statement):
        self.events.append(("status", statement))
        return 1

    async def all(self, statement):
        self.events.append(("all", statement))
        return self.query_rows

    async def execute(self, statement):
        self.events.append(("execute", statement))


class _ConcurrentProviderAdmissionDb(_ProviderAdmissionDb):
    def __init__(self):
        super().__init__()
        self.admission_lock = asyncio.Lock()
        self.retry_child_by_parent = {}

    @asynccontextmanager
    async def acquire(self):
        async with self.admission_lock:
            self.events.append(("acquire",))
            try:
                yield self
            finally:
                self.events.append(("commit",))

    async def status(self, statement):
        row_map = dict(statement.compile().params)
        self.retry_child_by_parent[row_map["retry_of_run_id"]] = row_map
        return await super().status(statement)


class _BlockingProviderEnqueue:
    def __init__(self):
        self.started = asyncio.Event()
        self.finish = asyncio.Event()
        self.calls = []

    async def __call__(self, row):
        self.calls.append(row["run_id"])
        self.started.set()
        await self.finish.wait()
        return {
            "status": "queued",
            "phase_detail": "enqueued",
            "heartbeat_at": row["heartbeat_at"],
            "progress": {"message": "queued"},
            "metrics": {"queue": "arq:ProviderDirectoryFHIR"},
            "error": None,
        }


def _provider_retry_request(run_id):
    return {
        "run_id": run_id,
        "importer": "provider-directory-fhir",
        "retry_of_run_id": "run_parent",
    }


def _install_provider_admission_stubs(monkeypatch, active_runs, *, idempotency_run=None):
    database = _ProviderAdmissionDb()

    async def active_idempotency(connection, _idempotency_key):
        assert connection is database
        return idempotency_run

    async def active_importers(connection, importer):
        assert connection is database
        assert importer == "provider-directory-fhir"
        return active_runs

    monkeypatch.setattr(control_imports, "db", database)
    monkeypatch.setattr(control_imports, "_active_idempotency_run", active_idempotency)
    monkeypatch.setattr(control_imports, "_active_importer_runs", active_importers)
    return database


@pytest.mark.parametrize(
    "artifact_options",
    [
        {},
        {"publish_artifacts_targets": "address_archive,entity_address"},
        {"publish_corroboration": True},
        {"publish_corroboration": False},
    ],
)
def test_fhir_scoped_artifact_classifier(artifact_options):
    params_by_name = _provider_directory_artifact_params("pdfhir_artifact", **artifact_options)

    operation_kind, source_ids, endpoint_scope = control_imports._provider_directory_operation(params_by_name)

    assert operation_kind == control_imports._PROVIDER_DIRECTORY_SCOPED_ARTIFACT
    assert source_ids == frozenset({"pdfhir_artifact"})
    assert endpoint_scope is None


@pytest.mark.parametrize(
    "unsafe_override",
    [
        {"source_ids": None},
        {"source_ids": []},
        {"source_ids": [""]},
        {"source_ids": ["pdfhir_one", "pdfhir_one"]},
        {"source_ids": [123]},
        {"import_resources": True},
        {"canonical_backfill_only": True},
        {"contact_backfill_only": True},
        {"refresh_preset": "monthly-full"},
        {"seed_only": True},
        {"stale_cleanup": True},
        {"publish_after_acquisition": True},
        {"publish_artifacts": True},
    ],
)
def test_fhir_exclusive_artifact_classifier(unsafe_override):
    params_by_name = _provider_directory_artifact_params("pdfhir_artifact", **unsafe_override)

    operation_kind, _, _ = control_imports._provider_directory_operation(params_by_name)

    assert operation_kind == control_imports._PROVIDER_DIRECTORY_EXCLUSIVE


def test_fhir_artifact_separate_capacity(monkeypatch):
    monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_MAX_ACTIVE", "2")
    first_acquisition = _provider_directory_active_run(
        "run_one",
        "pdfhir_one",
        "https://one.example.org/fhir",
    )
    second_acquisition = _provider_directory_active_run(
        "run_two",
        "pdfhir_two",
        "https://two.example.org/fhir",
    )
    artifact_run = _provider_directory_artifact_run("run_artifact", "pdfhir_artifact")

    artifact_blocker = control_imports._provider_directory_blocking_run(
        artifact_run["params"],
        [first_acquisition, second_acquisition],
    )
    acquisition_blocker = control_imports._provider_directory_blocking_run(
        second_acquisition["params"],
        [first_acquisition, artifact_run],
    )

    assert artifact_blocker is None
    assert acquisition_blocker is None


def test_fhir_artifact_conflicts():
    acquisition_run = _provider_directory_active_run(
        "run_acquisition",
        "pdfhir_shared",
        "https://shared.example.org/fhir",
    )
    artifact_run = _provider_directory_artifact_run("run_artifact", "pdfhir_artifact")
    overlapping_artifact = _provider_directory_artifact_params("pdfhir_shared")
    second_artifact = _provider_directory_artifact_params("pdfhir_other")
    overlapping_acquisition = _provider_directory_acquisition_params(
        "pdfhir_artifact",
        "https://other.example.org/fhir",
    )

    assert control_imports._provider_directory_blocking_run(overlapping_artifact, [acquisition_run]) == acquisition_run
    assert control_imports._provider_directory_blocking_run(second_artifact, [artifact_run]) == artifact_run
    assert control_imports._provider_directory_blocking_run(overlapping_acquisition, [artifact_run]) == artifact_run


def test_provider_directory_legacy_active_group_scope_controls_parallel_admission(monkeypatch):
    monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_MAX_ACTIVE", "6")
    active = _legacy_provider_directory_active_run(
        "run_cigna",
        "pdfhir_cigna",
        "https://fhir.cigna.com/ProviderDirectory/v1",
    )
    idaho_params = _provider_directory_acquisition_params(
        "pdfhir_idaho",
        "https://api-idmedicaid.safhir.io/v1/api/provider-directory",
    )
    same_base_params = _provider_directory_acquisition_params(
        "pdfhir_other",
        "https://fhir.cigna.com/ProviderDirectory/v1",
    )

    assert control_imports._provider_directory_blocking_run(idaho_params, [active]) is None
    assert control_imports._provider_directory_blocking_run(same_base_params, [active]) == active


@pytest.mark.asyncio
async def test_create_import_run_replays_provider_directory_idempotency_key(monkeypatch):
    active = _provider_directory_active_run(
        "run_idaho",
        "pdfhir_idaho",
        "https://idaho.example.org/fhir",
    )

    database = _install_provider_admission_stubs(monkeypatch, [], idempotency_run=active)

    replayed_run, created = await create_import_run(
        {"importer": "provider-directory-fhir", "idempotency_key": "idaho-acquisition"}
    )

    assert created is False
    assert replayed_run == active
    assert [event[0] for event in database.events] == ["acquire", "scalar", "commit"]


@pytest.mark.asyncio
async def test_create_import_run_replays_terminal_provider_directory_retry_child(monkeypatch):
    terminal_child_map = {
        "run_id": "run_child",
        "status": "failed",
        "importer": "provider-directory-fhir",
        "retry_of_run_id": "run_parent",
    }
    database = _ProviderAdmissionDb([terminal_child_map])

    async def fail_active_lookup(*_args):
        raise AssertionError("retry-child replay must precede active-run admission checks")

    async def fail_enqueue(_row):
        raise AssertionError("an existing terminal retry child must not enqueue again")

    monkeypatch.setattr(control_imports, "db", database)
    monkeypatch.setattr(control_imports, "_active_idempotency_run", fail_active_lookup)
    monkeypatch.setattr(control_imports, "_active_importer_runs", fail_active_lookup)
    monkeypatch.setattr(control_imports, "_enqueue_import_start", fail_enqueue)

    replayed_run, created = await create_import_run(
        {
            "run_id": "run_second_child",
            "importer": "provider-directory-fhir",
            "retry_of_run_id": "run_parent",
            "idempotency_key": "new-idempotency-key",
        }
    )

    assert created is False
    assert replayed_run == terminal_child_map
    assert [event[0] for event in database.events] == ["acquire", "scalar", "all", "commit"]
    retry_lookup = database.events[2][1].compile().params
    assert retry_lookup["importer_1"] == "provider-directory-fhir"
    assert retry_lookup["retry_of_run_id_1"] == "run_parent"


@pytest.mark.asyncio
async def test_concurrent_provider_directory_retries_converge_on_one_child(monkeypatch):
    """Serialize concurrent admissions and return the first direct child."""
    database = _ConcurrentProviderAdmissionDb()
    enqueue = _BlockingProviderEnqueue()

    async def retry_child(connection, retry_of_run_id):
        assert connection is database
        return database.retry_child_by_parent.get(retry_of_run_id)

    async def no_idempotency_run(_connection, _idempotency_key):
        return None

    async def no_active_runs(_connection, _importer):
        return []

    monkeypatch.setattr(control_imports, "db", database)
    monkeypatch.setattr(control_imports, "_provider_directory_retry_child", retry_child)
    monkeypatch.setattr(control_imports, "_active_idempotency_run", no_idempotency_run)
    monkeypatch.setattr(control_imports, "_active_importer_runs", no_active_runs)
    monkeypatch.setattr(control_imports, "_enqueue_import_start", enqueue)

    first_task = asyncio.create_task(
        create_import_run(_provider_retry_request("run_child_one"))
    )
    await enqueue.started.wait()
    second_result = await create_import_run(_provider_retry_request("run_child_two"))
    enqueue.finish.set()
    first_result = await first_task

    assert first_result[1] is True
    assert second_result[1] is False
    assert first_result[0]["run_id"] == second_result[0]["run_id"] == "run_child_one"
    assert enqueue.calls == ["run_child_one"]
    assert [event[0] for event in database.events].count("status") == 1


@pytest.mark.asyncio
async def test_create_import_run_allows_disjoint_provider_directory_acquisition(monkeypatch):
    active = _legacy_provider_directory_active_run(
        "run_cigna",
        "pdfhir_cigna",
        "https://fhir.cigna.com/ProviderDirectory/v1",
    )

    async def fake_enqueue(row):
        assert database.committed is True
        database.events.append(("enqueue", row["run_id"]))
        return {
            "status": "queued",
            "phase_detail": "enqueued",
            "heartbeat_at": row["heartbeat_at"],
            "progress": {"message": "queued"},
            "metrics": {"queue": "arq:ProviderDirectoryFHIR"},
            "error": None,
        }

    database = _install_provider_admission_stubs(monkeypatch, [active])
    monkeypatch.setattr(control_imports, "_enqueue_import_start", fake_enqueue)

    created_run, created = await create_import_run(
        {
            "run_id": "run_idaho",
            "importer": "provider-directory-fhir",
            "idempotency_key": "idaho-acquisition",
            "params": _provider_directory_acquisition_params(
                "pdfhir_idaho",
                "https://api-idmedicaid.safhir.io/v1/api/provider-directory",
            ),
        }
    )

    assert created is True
    assert created_run["run_id"] == "run_idaho"
    assert [event[0] for event in database.events] == [
        "acquire",
        "scalar",
        "status",
        "commit",
        "enqueue",
        "execute",
    ]
    lock_event = database.events[1]
    assert "pg_advisory_xact_lock" in str(lock_event[1])
    assert lock_event[2] == {"lock_key": control_imports._PROVIDER_DIRECTORY_ADMISSION_LOCK_KEY}
    assert control_imports._PROVIDER_DIRECTORY_ADMISSION_LOCK_KEY != control_imports._IMPORT_RUN_ADVISORY_LOCK_KEY


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("source_id", "endpoint_scope"),
    [
        ("pdfhir_cigna", "https://idaho.example.org/fhir"),
        ("pdfhir_idaho", "https://fhir.cigna.com/ProviderDirectory/v1"),
    ],
)
async def test_create_import_run_blocks_overlapping_provider_directory_scope(
    monkeypatch,
    source_id,
    endpoint_scope,
):
    active = _legacy_provider_directory_active_run(
        "run_cigna",
        "pdfhir_cigna",
        "https://fhir.cigna.com/ProviderDirectory/v1",
    )

    async def fail_enqueue(_row):
        raise AssertionError("overlapping provider-directory acquisition must not enqueue")

    _install_provider_admission_stubs(monkeypatch, [active])
    monkeypatch.setattr(control_imports, "_enqueue_import_start", fail_enqueue)

    row, created = await create_import_run(
        {
            "importer": "provider-directory-fhir",
            "params": _provider_directory_acquisition_params(source_id, endpoint_scope),
        }
    )

    assert created is False
    assert row == active


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "unsafe_override",
    [
        {"stale_cleanup": True},
        {"publish_artifacts": True},
        {"publish_after_acquisition": True},
        {"publish_corroboration": True},
        {"source_concurrency": 2},
        {"canonical_backfill_only": True},
        {"contact_backfill_only": True},
        {"publish_artifacts_only": True},
        {"seed_only": True},
        {"provider_directory_endpoint_scope": "https://user:secret@example.org/fhir"},
    ],
)
async def test_create_import_run_blocks_unsafe_provider_directory_parallel_flags(
    monkeypatch,
    unsafe_override,
):
    active = _provider_directory_active_run(
        "run_cigna",
        "pdfhir_cigna",
        "https://fhir.cigna.com/ProviderDirectory/v1",
    )

    async def fail_enqueue(_row):
        raise AssertionError("unsafe provider-directory acquisition must not enqueue")

    _install_provider_admission_stubs(monkeypatch, [active])
    monkeypatch.setattr(control_imports, "_enqueue_import_start", fail_enqueue)
    params_by_name = _provider_directory_acquisition_params(
        "pdfhir_idaho",
        "https://idaho.example.org/fhir",
    )
    params_by_name.update(unsafe_override)

    blocked_run, created = await create_import_run(
        {"importer": "provider-directory-fhir", "params": params_by_name}
    )

    assert created is False
    assert blocked_run == active


@pytest.mark.asyncio
async def test_create_import_run_blocks_third_provider_directory_acquisition(monkeypatch):
    active_runs = [
        _provider_directory_active_run("run_one", "pdfhir_one", "https://one.example.org/fhir"),
        _provider_directory_active_run("run_two", "pdfhir_two", "https://two.example.org/fhir"),
    ]

    async def fail_enqueue(_row):
        raise AssertionError("third provider-directory acquisition must not enqueue")

    _install_provider_admission_stubs(monkeypatch, active_runs)
    monkeypatch.setattr(control_imports, "_enqueue_import_start", fail_enqueue)

    row, created = await create_import_run(
        {
            "importer": "provider-directory-fhir",
            "params": _provider_directory_acquisition_params(
                "pdfhir_three",
                "https://three.example.org/fhir",
            ),
        }
    )

    assert created is False
    assert row == active_runs[0]


def test_provider_directory_capacity_override_allows_third_disjoint_acquisition(monkeypatch):
    monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_MAX_ACTIVE", "6")
    active_runs = [
        _provider_directory_active_run("run_one", "pdfhir_one", "https://one.example.org/fhir"),
        _provider_directory_active_run("run_two", "pdfhir_two", "https://two.example.org/fhir"),
    ]
    requested_params = _provider_directory_acquisition_params(
        "pdfhir_three",
        "https://three.example.org/fhir",
    )

    assert control_imports._provider_directory_blocking_run(requested_params, active_runs) is None


class _CancelUpdateResult:
    """Minimal SQLAlchemy result stub for cancel update tests."""

    def scalar_one_or_none(self):
        return None


class _CancelDbUpdateRecorder:
    """Capture update parameters written by request_cancel."""

    def __init__(self):
        self.update_parameters: list[dict[str, object]] = []
        self.lookup_attempts = 0

    async def execute(self, statement):
        self.update_parameters.append(statement.compile().params)
        return _CancelUpdateResult()

    def merged_run(self, source_run: dict[str, object]) -> dict[str, object]:
        latest_update = self.update_parameters[-1]
        return {
            **source_run,
            "status": latest_update["status"],
            "phase_detail": latest_update["phase_detail"],
            "finished_at": latest_update["finished_at"],
            "progress": latest_update["progress"],
            "metrics": latest_update["metrics"],
        }


def _running_ptg_cancel_run(
    run_id: str,
    *,
    pct: int,
    queue: str,
    worker_class: str,
    resource_class: str,
) -> dict[str, object]:
    """Build a running PTG run fixture for active-cancel tests."""
    return {
        "run_id": run_id,
        "importer": "ptg",
        "status": "running",
        "progress": {"pct": pct},
        "metrics": {"queue": queue, "worker_class": worker_class},
        "params": {"resource_class": resource_class},
        "finished_at": None,
    }


def _install_running_cancel_stubs(monkeypatch, source_run: dict[str, object], delete_worker_jobs):
    """Patch request_cancel dependencies for running-worker cancel tests."""
    database_recorder = _CancelDbUpdateRecorder()

    async def fake_get_import_run(_run_id):
        database_recorder.lookup_attempts += 1
        if database_recorder.lookup_attempts == 1:
            return source_run
        return database_recorder.merged_run(source_run)

    async def fake_set_cancel_flag(run_id):
        return {"redis": True, "key": f"cancel:{run_id}", "ttl_seconds": 10}

    monkeypatch.setattr(control_imports, "db", database_recorder)
    monkeypatch.setattr(control_imports, "get_import_run", fake_get_import_run)
    monkeypatch.setattr(control_imports, "_set_cancel_flag", fake_set_cancel_flag)
    monkeypatch.setattr(control_imports, "_delete_active_worker_jobs", delete_worker_jobs)
    return database_recorder


@pytest.mark.asyncio
async def test_sync_terminal_worker_failure_persists_oom_evidence(monkeypatch):
    source_run = _running_ptg_cancel_run(
        "run_ptg_oom",
        pct=80,
        queue="arq:PTGNormal",
        worker_class="process.PTGNormal",
        resource_class="normal",
    )
    terminal_state_map = {
        "status": "failed",
        "items": [
            {
                "queue": "arq:PTGNormal",
                "worker_class": "process.PTGNormal",
                "job_name": "hpw-mrf-process-ptgnormal-start-9d32f9a072",
                "job_status": "failed",
                "failure": {
                    "pod_name": "hpw-mrf-process-ptgnormal-start-9d32f9a072-72nnt",
                    "container": "worker",
                    "reason": "OOMKilled",
                    "exitCode": 137,
                },
            }
        ],
    }

    async def fake_active_worker_state(run):
        assert run == source_run
        return terminal_state_map

    database_recorder = _CancelDbUpdateRecorder()
    monkeypatch.setattr(control_imports, "db", database_recorder)
    monkeypatch.setattr(control_imports, "_active_worker_state", fake_active_worker_state)

    synced_run = await control_imports._sync_terminal_worker_failure(source_run)

    assert synced_run["status"] == "failed"
    assert synced_run["phase_detail"] == "worker job failed"
    assert synced_run["progress"] == {"unit": "run", "total": 1, "done": 1, "pct": 100, "message": "worker job failed"}
    assert synced_run["metrics"]["terminal_worker_state"] == terminal_state_map
    assert synced_run["error"]["code"] == "worker_job_failed"
    assert synced_run["error"]["reason"] == "OOMKilled"
    assert synced_run["error"]["exitCode"] == 137
    assert synced_run["error"]["kubernetes_evidence"]["items"][0]["failure"]["exitCode"] == 137
    assert database_recorder.update_parameters[-1]["status"] == "failed"
    assert database_recorder.update_parameters[-1]["error"]["reason"] == "OOMKilled"


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
async def test_request_cancel_records_kubernetes_worker_job_deletion(monkeypatch):
    source_run = _running_ptg_cancel_run(
        "run_ptg",
        pct=25,
        queue="arq:PTGLarge",
        worker_class="process.PTGLarge",
        resource_class="large",
    )

    async def fake_delete_active_worker_jobs(run):
        assert control_imports._active_worker_cancel_payload(run) == {
            "run_id": "run_ptg",
            "importer": "ptg",
            "status": "running",
            "queue": "arq:PTGLarge",
            "worker_class": "process.PTGLarge",
            "resource_class": "large",
        }
        return {"enabled": True, "namespace": "healthporta-dev", "deleted": 1}

    _install_running_cancel_stubs(monkeypatch, source_run, fake_delete_active_worker_jobs)

    cancel_response = await control_imports.request_cancel("run_ptg")

    assert cancel_response["status"] == "canceled"
    assert cancel_response["phase_detail"] == "canceled active worker"
    assert cancel_response["finished_at"] is not None
    assert cancel_response["progress"] == {"unit": "run", "total": 1, "done": 1, "pct": 100, "message": "canceled"}
    assert cancel_response["metrics"]["cancel_signal"]["redis"] is True
    assert cancel_response["metrics"]["cancel_signal"]["kubernetes"] == {
        "enabled": True,
        "namespace": "healthporta-dev",
        "deleted": 1,
    }


@pytest.mark.asyncio
async def test_request_cancel_finishes_when_kubernetes_worker_job_already_terminal(monkeypatch):
    source_run = _running_ptg_cancel_run(
        "run_ptg_terminal",
        pct=40,
        queue="arq:PTGNormal",
        worker_class="process.PTGNormal",
        resource_class="normal",
    )

    async def fake_delete_active_worker_jobs(_run):
        return {
            "enabled": True,
            "namespace": "healthporta-dev",
            "deleted": 0,
            "items": [
                {
                    "job_name": "hpw-mrf-process-ptgnormal-start-c5cded3c22",
                    "worker_class": "process.PTGNormal",
                    "deleted": False,
                    "reason": "terminal",
                }
            ],
        }

    _install_running_cancel_stubs(monkeypatch, source_run, fake_delete_active_worker_jobs)

    cancel_response = await control_imports.request_cancel("run_ptg_terminal")

    assert cancel_response["status"] == "canceled"
    assert cancel_response["phase_detail"] == "canceled active worker"
    assert cancel_response["finished_at"] is not None
    assert cancel_response["progress"] == {"unit": "run", "total": 1, "done": 1, "pct": 100, "message": "canceled"}
    assert cancel_response["metrics"]["cancel_signal"]["kubernetes"]["items"][0]["reason"] == "terminal"


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
async def test_control_ptg_source_snapshot_promote_endpoint_can_enqueue_address_refresh(monkeypatch):
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "secret")
    promote_calls = []
    import_calls = []

    async def fake_promote(**kwargs):
        promote_calls.append(kwargs)
        return {
            "source_key": kwargs["source_key"],
            "snapshot_id": kwargs["snapshot_id"],
            "previous_snapshot_id": kwargs["expected_current_snapshot_id"],
            "plan_source_count": 2,
        }

    async def fake_create_import_run(payload):
        import_calls.append(payload)
        return {"run_id": "run_refresh", "importer": payload["importer"], "params": payload["params"]}, True

    monkeypatch.setattr(control, "promote_ptg2_source_snapshot", fake_promote)
    monkeypatch.setattr(control, "create_import_run", fake_create_import_run)
    request = authed_request(
        json={
            "source_key": "source_a",
            "snapshot_id": "snap_new",
            "expected_current_snapshot_id": "snap_old",
            "refresh_addresses": True,
            "address_refresh": {
                "idempotency_key": "idem-refresh",
                "test_mode": True,
                "limit_per_source": 25,
                "publish": True,
            },
        }
    )

    response = await control.control_ptg_source_snapshot_promote(request)
    payload = json.loads(response.body)

    assert response.status == 200
    assert payload["snapshot_id"] == "snap_new"
    assert payload["address_refresh"]["created"] is True
    assert payload["address_refresh"]["run"]["run_id"] == "run_refresh"
    assert promote_calls == [
        {
            "source_key": "source_a",
            "snapshot_id": "snap_new",
            "expected_current_snapshot_id": "snap_old",
        }
    ]
    assert import_calls == [
        {
            "run_id": None,
            "importer": "entity-address-unified",
            "params": {
                "test_mode": True,
                "limit_per_source": 25,
                "publish": True,
                "refresh_mode": "full",
                "trigger_source_key": "source_a",
                "trigger_snapshot_id": "snap_new",
            },
            "idempotency_key": "idem-refresh",
            "triggered_by": "ptg_source_snapshot_promote",
            "schedule_id": None,
            "subscription_id": None,
            "import_id": None,
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
async def test_control_ptg_source_snapshot_retire_endpoint(monkeypatch):
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "secret")
    calls = []

    async def fake_retire(**kwargs):
        calls.append(kwargs)
        return {"snapshot_id": kwargs["snapshot_id"], "source_key": kwargs["source_key"], "retired": True}

    monkeypatch.setattr(control, "retire_ptg2_source_snapshot", fake_retire)

    response = await control.control_ptg_source_snapshot_retire(
        authed_request(json={"snapshot_id": "snap_live", "source_key": "source_a"})
    )
    payload = json.loads(response.body)

    assert response.status == 200
    assert payload["retired"] is True
    assert calls == [{"snapshot_id": "snap_live", "source_key": "source_a"}]


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
    request = authed_request(
        args={
            "limit": "1",
            "cursor": "cursor_1",
            "retry_of_run_id": "run_parent",
        }
    )

    response = await control.control_list_imports(request)
    response_payload = json.loads(response.body)

    assert response.status == 200
    assert response_payload == {
        "items": [{"run_id": "run_1", "status": "queued"}],
        "next_cursor": "cursor_2",
    }
    assert calls == [
        {
            "status": None,
            "importer": None,
            "retry_of_run_id": "run_parent",
            "limit": 1,
            "cursor": "cursor_1",
        }
    ]


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
