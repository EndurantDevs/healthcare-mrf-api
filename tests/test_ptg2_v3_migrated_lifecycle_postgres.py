# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import datetime
import hashlib
import importlib.util
import os
import re
import struct
import uuid
from pathlib import Path
from types import ModuleType
from typing import Any, Mapping

import pytest
from alembic.config import Config
from alembic.script import ScriptDirectory
from sanic import Blueprint, Sanic
from sqlalchemy.engine import make_url

from api import ptg2_serving, ptg2_snapshot, ptg2_tables
from api.control import blueprint as control_blueprint
from api.endpoint import pricing
from api.ptg2_candidate_audit import PTG2_CANDIDATE_AUDIT_HEADER
from db.connection import db
from process.ptg import (
    _mark_ptg2_import_failed,
    _ptg2_manifest_stage_table_names,
    _reused_shared_v3_serving_index,
)
from process.ptg_parts.import_rows import _ptg2_source_trace_rows
from process.ptg_parts.ptg2_lifecycle_lock import acquire_ptg2_lifecycle_lock
from process.ptg_parts.ptg2_manifest_artifacts import write_global_membership_sidecar
from process.ptg_parts.ptg2_manifest_publish import (
    _copy_price_atom_member_file,
    _copy_ptg2_manifest_price_atom_file,
    _create_ptg2_manifest_serving_stage_table,
    _ptg2_manifest_support_stage_table,
)
from process.ptg_parts.ptg2_shared_blocks import (
    reserve_shared_layout,
    shared_semantic_fingerprint,
)
from process.ptg_parts.ptg2_shared_finalize import (
    attach_v3_dictionary_contract,
    attach_v3_source_run_contract,
)
from process.ptg_parts.ptg2_shared_gc import sweep_ptg2_shared_blocks
from process.ptg_parts.ptg2_shared_reuse import (
    SharedPhysicalArtifactIdentity,
    SharedSnapshotSourceAssignment,
    shared_source_set_metadata,
)
from process.ptg_parts.ptg2_shared_snapshot_publish import (
    publish_shared_v3_snapshot_sources,
    publish_strict_shared_v3_layout,
    validate_reused_shared_v3_snapshot_sources,
)
from process.ptg_parts.snapshot_cleanup import _drop_ptg2_snapshot_table_names
from process.ptg_parts.source_pointers import _stage_ptg2_source_candidate


ROOT = Path(__file__).resolve().parents[1]
OPT_IN_DSN_ENV = "HLTHPRT_PTG2_V3_LIFECYCLE_POSTGRES_DSN"
DISPOSABLE_DATABASE_PATTERN = re.compile(
    r"^ptg2_v3_lifecycle_test_[a-z0-9][a-z0-9_]{7,}$"
)
SCHEMA_NAME = "mrf"
SCANNER_TEST_PATH = Path(__file__).with_name("test_ptg2_scanner_v3_runs.py")
SERVING_RECORD = struct.Struct(">16s16s16sI")
CONTROL_TOKEN = "ptg2-v3-lifecycle-control-token"
PLAN_A = "12-3456789"
PLAN_B = "98-7654321"
SOURCE_A = "lifecycle-source-a"
SOURCE_B = "lifecycle-source-b"
NPIS = (1234567890, 1234567891)
COVERAGE_SCOPE_ID = b"\xcc" * 32

REQUIRED_TABLES = {
    "alembic_version",
    "code_catalog",
    "import_run",
    "ptg2_artifact_manifest",
    "ptg2_current_plan_source",
    "ptg2_current_snapshot",
    "ptg2_current_source_snapshot",
    "ptg2_import_run",
    "ptg2_plan",
    "ptg2_plan_month",
    "ptg2_snapshot",
    "ptg2_source_trace",
    "ptg2_source_trace_set",
    "ptg2_v3_audit_occurrence",
    "ptg2_v3_block",
    "ptg2_v3_candidate_audit_attestation",
    "ptg2_v3_code",
    "ptg2_v3_gc_candidate",
    "ptg2_v3_graph_owner",
    "ptg2_v3_layout_fingerprint",
    "ptg2_v3_npi_scope",
    "ptg2_v3_price_attr",
    "ptg2_v3_provider_group",
    "ptg2_v3_provider_set",
    "ptg2_v3_snapshot_binding",
    "ptg2_v3_snapshot_block",
    "ptg2_v3_snapshot_layout",
    "ptg2_v3_snapshot_scope",
    "ptg2_v3_snapshot_source",
    "ptg2_v3_source_audit_witness",
}

EMPTY_BEFORE_WRITE_TABLES = (
    "ptg2_current_plan_source",
    "ptg2_current_snapshot",
    "ptg2_current_source_snapshot",
    "ptg2_snapshot",
    "ptg2_v3_block",
    "ptg2_v3_gc_candidate",
    "ptg2_v3_layout_fingerprint",
    "ptg2_v3_snapshot_binding",
    "ptg2_v3_snapshot_layout",
    "ptg2_v3_snapshot_scope",
    "ptg2_v3_snapshot_source",
    "ptg2_v3_source_audit_witness",
)

FINAL_EMPTY_TABLES = (
    "ptg2_current_plan_source",
    "ptg2_current_snapshot",
    "ptg2_current_source_snapshot",
    "ptg2_snapshot",
    "ptg2_v3_audit_occurrence",
    "ptg2_v3_block",
    "ptg2_v3_candidate_audit_attestation",
    "ptg2_v3_code",
    "ptg2_v3_gc_candidate",
    "ptg2_v3_graph_owner",
    "ptg2_v3_layout_fingerprint",
    "ptg2_v3_npi_scope",
    "ptg2_v3_price_attr",
    "ptg2_v3_provider_group",
    "ptg2_v3_provider_set",
    "ptg2_v3_snapshot_binding",
    "ptg2_v3_snapshot_block",
    "ptg2_v3_snapshot_layout",
    "ptg2_v3_snapshot_scope",
    "ptg2_v3_snapshot_source",
    "ptg2_v3_source_audit_witness",
)


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _quoted(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _load_module(path: Path, module_name: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(module_name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _configure_disposable_database(monkeypatch: pytest.MonkeyPatch, dsn: str) -> str:
    try:
        url = make_url(dsn)
    except Exception as exc:
        pytest.fail(f"{OPT_IN_DSN_ENV} is not a valid PostgreSQL DSN: {exc}")
    if not url.drivername.startswith("postgresql"):
        pytest.fail(f"{OPT_IN_DSN_ENV} must use PostgreSQL")
    database_name = str(url.database or "")
    if not DISPOSABLE_DATABASE_PATTERN.fullmatch(database_name):
        pytest.fail(
            f"refusing non-disposable PostgreSQL database {database_name!r}; "
            "the name must match ptg2_v3_lifecycle_test_<unique-suffix>"
        )
    requested_schema = str(url.query.get("schema") or SCHEMA_NAME)
    if requested_schema != SCHEMA_NAME:
        pytest.fail(
            f"refusing schema {requested_schema!r}; this migrated lifecycle test "
            f"requires the disposable database's {SCHEMA_NAME!r} schema"
        )
    if not url.host or not url.username:
        pytest.fail(f"{OPT_IN_DSN_ENV} must include an explicit host and user")

    monkeypatch.setenv("HLTHPRT_DB_DRIVER", "asyncpg")
    monkeypatch.setenv("HLTHPRT_DB_HOST", str(url.host))
    monkeypatch.setenv("HLTHPRT_DB_PORT", str(url.port or 5432))
    monkeypatch.setenv("HLTHPRT_DB_USER", str(url.username))
    monkeypatch.setenv("HLTHPRT_DB_PASSWORD", str(url.password or ""))
    monkeypatch.setenv("HLTHPRT_DB_DATABASE", database_name)
    monkeypatch.delenv("HLTHPRT_DB_DATABASE_OVERRIDE", raising=False)
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", SCHEMA_NAME)
    return database_name


async def _assert_migrated_empty_target(database_name: str) -> None:
    """Support the assert migrated empty target test fixture."""
    selected_database = await db.scalar("SELECT current_database()")
    assert selected_database == database_name

    alembic_config = Config(str(ROOT / "alembic.ini"))
    expected_heads = set(ScriptDirectory.from_config(alembic_config).get_heads())
    observed_heads = {
        str(version_record[0])
        for version_record in await db.all(
            f"SELECT version_num FROM {_quoted(SCHEMA_NAME)}.alembic_version"
        )
    }
    assert observed_heads == expected_heads, (
        "the disposable database must have migration head applied: "
        f"expected={sorted(expected_heads)}, observed={sorted(observed_heads)}"
    )

    observed_tables = {
        str(table_record[0])
        for table_record in await db.all(
            """
            SELECT table_name
              FROM information_schema.tables
             WHERE table_schema = :schema_name
            """,
            schema_name=SCHEMA_NAME,
        )
    }
    assert REQUIRED_TABLES <= observed_tables, (
        "the migrated disposable schema is missing production tables: "
        f"{sorted(REQUIRED_TABLES - observed_tables)}"
    )
    assert await db.scalar(
        """
        SELECT EXISTS (
            SELECT 1
              FROM pg_indexes
             WHERE schemaname = :schema_name
               AND tablename = 'ptg2_v3_candidate_audit_attestation'
               AND indexname =
                   'ptg2_v3_candidate_audit_attestation_snapshot_key_idx'
        )
        """,
        schema_name=SCHEMA_NAME,
    )
    jsonb_columns = {
        (str(column_record[0]), str(column_record[1]))
        for column_record in await db.all(
            """
            SELECT table_name, column_name
              FROM information_schema.columns
             WHERE table_schema = :schema_name
               AND data_type = 'jsonb'
               AND (table_name, column_name) IN (
                    ('ptg2_v3_snapshot_layout', 'layout_manifest'),
                    ('ptg2_v3_candidate_audit_attestation', 'report')
               )
            """,
            schema_name=SCHEMA_NAME,
        )
    }
    assert jsonb_columns == {
        ("ptg2_v3_snapshot_layout", "layout_manifest"),
        ("ptg2_v3_candidate_audit_attestation", "report"),
    }
    for table_name in EMPTY_BEFORE_WRITE_TABLES:
        assert await _count(table_name) == 0, (
            f"refusing to use non-empty disposable table {SCHEMA_NAME}.{table_name}"
        )


async def _count(table_name: str) -> int:
    assert table_name in REQUIRED_TABLES or table_name in FINAL_EMPTY_TABLES
    return int(
        await db.scalar(
            f"SELECT COUNT(*) FROM {_quoted(SCHEMA_NAME)}.{_quoted(table_name)}"
        )
        or 0
    )


async def _insert_logical_snapshot_prerequisites(
    *,
    snapshot_id: str,
    plan_id: str,
    assignment: SharedSnapshotSourceAssignment,
    source_trace_row: Mapping[str, Any],
) -> None:
    await db.status(
        f"""
        INSERT INTO {_quoted(SCHEMA_NAME)}.ptg2_snapshot
            (snapshot_id, import_run_id, import_month, status, created_at,
             validated_at, published_at, previous_snapshot_id, manifest)
        VALUES
            (:snapshot_id, :import_run_id, DATE '2026-07-01', 'building',
             timezone('UTC', clock_timestamp()), NULL, NULL, NULL, '{{}}'::json)
        """,
        snapshot_id=snapshot_id,
        import_run_id=f"run-{snapshot_id}",
    )
    assert source_trace_row["source_trace_hash"] in assignment.source_trace_hashes
    await db.status(
        f"""
        INSERT INTO {_quoted(SCHEMA_NAME)}.ptg2_source_trace
            (source_trace_hash, source_file_version_id, original_url,
             canonical_url, json_pointer, line_number, created_at)
        VALUES
            (:source_trace_hash, :source_file_version_id, :original_url,
             :canonical_url, :json_pointer, :line_number, :created_at)
        ON CONFLICT (source_trace_hash) DO NOTHING
        """,
        **dict(source_trace_row),
    )
    await db.status(
        f"""
        INSERT INTO {_quoted(SCHEMA_NAME)}.ptg2_source_trace_set
            (source_trace_set_hash, source_trace_hashes, created_at)
        VALUES
            (:source_trace_set_hash, CAST(:source_trace_hashes AS varchar[]),
             timezone('UTC', clock_timestamp()))
        ON CONFLICT (source_trace_set_hash) DO NOTHING
        """,
        source_trace_set_hash=assignment.source_trace_set_hash,
        source_trace_hashes=list(assignment.source_trace_hashes),
    )
    await publish_shared_v3_snapshot_sources(
        schema_name=SCHEMA_NAME,
        snapshot_id=snapshot_id,
        plan_id=plan_id,
        plan_market_type="group",
        coverage_scope_id=COVERAGE_SCOPE_ID,
        assignments=[assignment],
    )


async def _stage_candidate(
    *,
    snapshot_id: str,
    source_key: str,
    plan_id: str,
    snapshot_key: int,
    serving_index: Mapping[str, Any],
) -> None:
    timestamp = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
    staged = await _stage_ptg2_source_candidate(
        source_key=source_key,
        snapshot_id=snapshot_id,
        previous_snapshot_id=None,
        import_month=datetime.date(2026, 7, 1),
        updated_at=timestamp,
        snapshot_attributes={
            "snapshot_id": snapshot_id,
            "import_run_id": f"run-{snapshot_id}",
            "import_month": datetime.date(2026, 7, 1),
            "status": "validated",
            "created_at": timestamp,
            "validated_at": timestamp,
            "published_at": None,
            "previous_snapshot_id": None,
            "manifest": {
                "snapshot_id": snapshot_id,
                "serving_index": dict(serving_index),
                "serving_rates": int(serving_index["serving_rates"]),
                "rate_count": int(serving_index["rate_count"]),
            },
        },
        shared_snapshot_key=snapshot_key,
        coverage_scope_id=COVERAGE_SCOPE_ID,
        coverage_plan_id=plan_id,
        coverage_plan_market_type="group",
    )
    assert staged["status"] == "validated"
    assert staged["candidate_attributes"]["manifest"]["activation"] == {
        "contract": "ptg2_candidate_activation_v1",
        "state": "validated",
        "source_key": source_key,
        "expected_previous_snapshot_id": None,
    }


def _graph_artifacts(
    directory: Path,
    *,
    provider_set_id: bytes,
    provider_group_id: bytes,
) -> list[dict[str, object]]:
    npi_ids = tuple(
        b"\0" * 8 + int(npi).to_bytes(8, "big", signed=False) for npi in NPIS
    )
    graph_members_by_artifact = {
        "provider_group_npi": {provider_group_id: npi_ids},
        "provider_npi_group": {
            npi_id: (provider_group_id,) for npi_id in npi_ids
        },
        "provider_inverted": {provider_group_id: (provider_set_id,)},
        "provider_forward": {provider_set_id: (provider_group_id,)},
    }
    directory.mkdir(parents=True)
    entries: list[dict[str, object]] = []
    for name, mapping in graph_members_by_artifact.items():
        manifest = write_global_membership_sidecar(directory, name, mapping)
        sidecar_metadata_map = dict(manifest["sidecars"][0])
        sidecar_metadata_map.update(
            {
                "name": name,
                "path": str(
                    (directory / str(sidecar_metadata_map["path"])).resolve()
                ),
                "source_shard_id": "migrated-lifecycle-shard",
            }
        )
        entries.append(sidecar_metadata_map)
    return entries


def _release_report(
    *,
    snapshot_id: str,
    source_key: str,
    plan_id: str,
    sample_digest: str,
    source_set: Mapping[str, Any],
    source_witness: Mapping[str, Any],
    provider_identifier_quarantine: Mapping[str, Any],
) -> dict[str, Any]:
    """Build a passing release-audit report for one logical snapshot."""

    completed_at = datetime.datetime.now(datetime.timezone.utc).replace(
        microsecond=0
    )
    started_at = completed_at - datetime.timedelta(seconds=30)
    source_witness_map = dict(source_witness)
    occurrence_count = int(source_witness_map["occurrence_witness_count"])
    provider_count = int(source_witness_map["provider_witness_count"])
    total_count = int(source_witness_map["record_count"])
    return {
        "schema_version": 3,
        "harness": {
            "name": "ptg2_v3_fast_source_witness_audit",
            "version": "2.0.0",
            "contract": "ptg2_v3_fast_source_witness_audit_v2",
        },
        "runtime": {"http_client": "aiohttp", "event_loop": "uvloop"},
        "status": "pass",
        "profile": "release",
        "release_profile_enforced": True,
        "release_gate_eligible": True,
        "started_at": started_at.isoformat(),
        "completed_at": completed_at.isoformat(),
        "duration_seconds": 30.0,
        "target": {
            "expected_architecture": "postgres_binary_v3",
            "expected_storage_generation": "shared_blocks_v3",
            "expected_database_backend": "postgresql",
            "expected_snapshot_lifecycle": "validated",
            "architecture_assertion": "required_postgresql_session_evidence",
            "api_path_sha256": _sha256(
                "/api/v1/pricing/providers/audit-search-by-procedure"
            ),
            "api_audit_path_sha256": _sha256(
                "/api/v1/pricing/providers/audit-occurrences"
            ),
            "endpoint_contract": "pricing.providers.search_by_procedure",
            "audit_endpoint_contract": "persisted_served_occurrence_sample_v2",
            "snapshot_id_sha256": _sha256(snapshot_id),
            "source_key_sha256": _sha256(source_key),
            "plan_id_sha256": _sha256(plan_id),
            "market_type_sha256": _sha256("group"),
            "tls_verified": True,
            "transport_contract": "verified_https_v1",
        },
        "reproducibility": {},
        "source": {
            "source_count": int(source_set["source_count"]),
            "source_set_digest": source_set["raw_container_sha256_digest"],
            "witness": source_witness_map,
            "provider_identifier_quarantine": dict(
                provider_identifier_quarantine
            )
        },
        "coverage": {
            "failures": [],
            "selection_method": source_witness_map["selection_method"],
            "queryable_occurrence_population_count": source_witness_map[
                "queryable_occurrence_population_count"
            ],
            "emitted_rate_row_count": source_witness_map[
                "emitted_rate_row_count"
            ],
            "unqueryable_rate_row_count": source_witness_map[
                "unqueryable_rate_row_count"
            ],
            "unqueryable_rate_policy": source_witness_map[
                "unqueryable_rate_policy"
            ],
            "occurrence_sample_count": occurrence_count,
            "provider_sample_count": provider_count,
        },
        "checks": {
            "source_witnesses": total_count,
            "api_witnesses_matched": occurrence_count,
            "api_challenges_executed": occurrence_count,
            "provider_witnesses_validated": provider_count,
            "api_audit_occurrences_validated": 1,
        },
        "http": {
            "standard_api_actual_http_requests": occurrence_count + 1,
            "retry_count": 0,
            "max_concurrency": 32,
        },
        "random_api_requests": {
            "requested": occurrence_count,
            "executed": occurrence_count,
        },
        "latency": {},
        "api_audit_sample": {
            "sample_digest": sample_digest,
            "sample_digest_validated": True,
            "source_set_validated": True,
        },
        "failures": {"counts": {}, "examples": []},
        "limitations": [],
        "redaction": {
            "policy": "sensitive_identifiers_excluded",
            "excluded": [
                "source_paths",
                "source_file_names",
                "raw_source_hashes",
                "source_trace_URLs",
                "plan_and_snapshot_values",
                "auth_values",
                "HTTP_bodies",
                "network_names",
                "arbitrary_source_and_API_strings",
            ],
        },
    }


def _build_asgi_app() -> Sanic:
    app = Sanic(f"ptg2-v3-migrated-lifecycle-{uuid.uuid4().hex}")
    db.init_app(app)
    app.blueprint(control_blueprint)
    app.blueprint(
        Blueprint.group([pricing.blueprint], version_prefix="/api/v")
    )
    return app


def _clear_or_disable_in_process_caches(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(pricing, "ENABLE_PRICING_SCHEMA_CACHE", False)
    for cache_name in (
        "_PRICING_TABLE_EXISTS_CACHE",
        "_PRICING_TABLE_COLUMNS_CACHE",
        "_ZIP_RADIUS_ROWS_CACHE",
        "_PROCEDURE_TAXONOMY_EVIDENCE_CACHE",
    ):
        cache = getattr(pricing, cache_name, None)
        if hasattr(cache, "clear"):
            cache.clear()

    for module in (ptg2_snapshot, ptg2_tables, ptg2_serving):
        for name, value in vars(module).items():
            if "PTG2" not in name.upper() or "CACHE" not in name.upper():
                continue
            if hasattr(value, "clear"):
                value.clear()
                assert not value


async def _asgi_request(client, method: str, path: str, **kwargs: Any):
    result = await getattr(client, method.lower())(path, **kwargs)
    return result[1] if isinstance(result, tuple) else result


def _response_json(response, expected_status: int = 200) -> dict[str, Any]:
    assert response.status_code == expected_status, response.text
    payload = response.json
    assert isinstance(payload, dict)
    return payload


def _candidate_headers(snapshot_id: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {CONTROL_TOKEN}",
        PTG2_CANDIDATE_AUDIT_HEADER: snapshot_id,
    }


def _control_headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {CONTROL_TOKEN}"}


async def _cold_price_read(
    client,
    monkeypatch: pytest.MonkeyPatch,
    *,
    snapshot_id: str,
    source_key: str,
    plan_id: str,
    code: str,
    candidate: bool,
) -> dict[str, Any]:
    _clear_or_disable_in_process_caches(monkeypatch)
    route = (
        "/api/v1/pricing/providers/audit-search-by-procedure"
        if candidate
        else "/api/v1/pricing/providers/search-by-procedure"
    )
    response = await _asgi_request(
        client,
        "get",
        route,
        params={
            "snapshot_id": snapshot_id,
            "source_key": source_key,
            "plan_id": plan_id,
            "plan_market_type": "group",
            "code_system": "CPT",
            "code": code,
            "include_providers": "true",
            "limit": "10",
            "order_by": "negotiated_rate",
            "order": "asc",
        },
        headers=_candidate_headers(snapshot_id) if candidate else None,
    )
    return _response_json(response)


def _assert_exact_price_and_membership(
    payload: Mapping[str, Any],
    *,
    snapshot_id: str,
    plan_id: str,
    code: str,
    rate: int | float,
) -> None:
    assert payload["result_state"] == "matched"
    items = list(payload["items"])
    assert [int(item["npi"]) for item in items] == list(NPIS)
    assert all(item["plan_id"] == plan_id for item in items)
    assert all(item["reported_code"] == code for item in items)
    assert all(item["provider_count"] == len(NPIS) for item in items)
    assert all(item["negotiation_arrangement"] == "FFS" for item in items)
    assert all(
        [price["negotiated_rate"] for price in item["prices"]] == [rate]
        for item in items
    )
    provenance = payload["provenance"]
    assert provenance["snapshot_id"] == snapshot_id
    assert provenance["plan_id"] == plan_id
    assert provenance["arch_version"] == "postgres_binary_v3"
    assert provenance["storage_generation"] == "shared_blocks_v3"
    database_evidence = provenance["database_evidence"]
    assert database_evidence["contract"] == "postgresql_session_v1"
    assert database_evidence["database_selected"] is True
    assert database_evidence["backend_session_active"] is True
    assert database_evidence["transaction_snapshot_observed"] is True


async def _candidate_audit_occurrences(
    client,
    monkeypatch: pytest.MonkeyPatch,
    *,
    snapshot_id: str,
    source_key: str,
    plan_id: str,
) -> dict[str, Any]:
    _clear_or_disable_in_process_caches(monkeypatch)
    response = await _asgi_request(
        client,
        "get",
        "/api/v1/pricing/providers/audit-occurrences",
        params={
            "snapshot_id": snapshot_id,
            "source_key": source_key,
            "plan_id": plan_id,
            "plan_market_type": "group",
            "mode": "exact_source",
            "order_by": "occurrence_id",
            "order": "asc",
            "limit": "100",
            "offset": "0",
        },
        headers=_candidate_headers(snapshot_id),
    )
    return _response_json(response)


async def _physical_counts(snapshot_key: int) -> dict[str, int]:
    row = await db.first(
        f"""
        SELECT
            (SELECT COUNT(*) FROM {_quoted(SCHEMA_NAME)}.ptg2_v3_snapshot_layout
              WHERE snapshot_key = :snapshot_key) AS layouts,
            (SELECT COUNT(*) FROM {_quoted(SCHEMA_NAME)}.ptg2_v3_block) AS blocks,
            (SELECT COUNT(*) FROM {_quoted(SCHEMA_NAME)}.ptg2_v3_snapshot_block
              WHERE snapshot_key = :snapshot_key) AS mappings,
            (SELECT COUNT(*) FROM {_quoted(SCHEMA_NAME)}.ptg2_v3_code
              WHERE snapshot_key = :snapshot_key) AS codes,
            (SELECT COUNT(*) FROM {_quoted(SCHEMA_NAME)}.ptg2_v3_provider_set
              WHERE snapshot_key = :snapshot_key) AS provider_sets,
            (SELECT COUNT(*) FROM {_quoted(SCHEMA_NAME)}.ptg2_v3_npi_scope
              WHERE snapshot_key = :snapshot_key) AS npis
        """,
        snapshot_key=snapshot_key,
    )
    assert row is not None
    fields = row._mapping
    return {name: int(fields[name]) for name in fields}


async def _snapshot_state(snapshot_id: str) -> dict[str, Any]:
    row = await db.first(
        f"""
        SELECT snapshot.status,
               snapshot.manifest->'activation'->>'state' AS activation_state,
               source_pointer.snapshot_id AS source_pointer_snapshot_id,
               plan_pointer.snapshot_id AS plan_pointer_snapshot_id,
               global_pointer.snapshot_id AS global_pointer_snapshot_id,
               attestation.activated_at
          FROM {_quoted(SCHEMA_NAME)}.ptg2_snapshot snapshot
          LEFT JOIN {_quoted(SCHEMA_NAME)}.ptg2_current_source_snapshot source_pointer
            ON source_pointer.snapshot_id = snapshot.snapshot_id
          LEFT JOIN {_quoted(SCHEMA_NAME)}.ptg2_current_plan_source plan_pointer
            ON plan_pointer.snapshot_id = snapshot.snapshot_id
          LEFT JOIN {_quoted(SCHEMA_NAME)}.ptg2_current_snapshot global_pointer
            ON global_pointer.snapshot_id = snapshot.snapshot_id
           AND global_pointer.slot = 'current'
          LEFT JOIN {_quoted(SCHEMA_NAME)}.ptg2_v3_candidate_audit_attestation attestation
            ON attestation.snapshot_id = snapshot.snapshot_id
         WHERE snapshot.snapshot_id = :snapshot_id
        """,
        snapshot_id=snapshot_id,
    )
    assert row is not None
    return dict(row._mapping)


@pytest.mark.asyncio
async def test_v3_lifecycle_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Exercise the migrated strict-V3 lifecycle through fail-closed paths."""

    dsn = os.getenv(OPT_IN_DSN_ENV)
    if not dsn:
        pytest.skip(
            f"set {OPT_IN_DSN_ENV} to a pre-migrated disposable "
            "ptg2_v3_lifecycle_test_<unique-suffix> database"
        )

    database_name = _configure_disposable_database(monkeypatch, dsn)
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", CONTROL_TOKEN)
    monkeypatch.setenv("HLTHPRT_PTG2_SNAPSHOT_ARCH", "postgres_binary_v3")
    monkeypatch.setenv("HLTHPRT_PTG2_HASH_MODE", "sha256")
    monkeypatch.setenv("HLTHPRT_PTG2_MANIFEST_SERVING_LAYOUT", "lean_provider_key_v1")
    monkeypatch.setenv("HLTHPRT_PTG2_BINARY_IDS", "true")
    monkeypatch.setenv("HLTHPRT_PTG2_V3_FINALIZER_WORKERS", "1")
    monkeypatch.setenv(
        "HLTHPRT_PTG2_V3_FINALIZER_IDENTITY_MAP_MAX_BYTES", "67108864"
    )
    monkeypatch.setenv(
        "HLTHPRT_PTG2_V3_FINALIZER_TOTAL_SORT_MEMORY_BYTES", "16777216"
    )
    monkeypatch.setenv("HLTHPRT_PTG2_SERVING_BINARY_PAYLOAD_COMPRESSION", "none")
    monkeypatch.setenv("HLTHPRT_PTG2_SERVING_BINARY_BLOCK_BYTES", "65536")
    monkeypatch.setenv("HLTHPRT_PTG2_V3_COVERAGE_SCOPE_ID", COVERAGE_SCOPE_ID.hex())
    monkeypatch.setenv("HLTHPRT_PTG2_V3_SEALED_LEASE_SECONDS", "0")
    monkeypatch.setenv("HLTHPRT_PTG2_V3_BLOCK_GC_GRACE_SECONDS", "0")

    await db.disconnect()
    await db.connect()
    await _assert_migrated_empty_target(database_name)

    scanner_support = _load_module(
        SCANNER_TEST_PATH,
        f"ptg2_migrated_lifecycle_scanner_{uuid.uuid4().hex}",
    )
    scanner_binary = scanner_support._built_scanner_binary()
    monkeypatch.setenv("HLTHPRT_PTG2_RUST_SCANNER_BIN", str(scanner_binary))
    scan = scanner_support._run_scanner(
        scanner_binary,
        tmp_path,
        "migrated-lifecycle-source",
        arch="postgres_binary_v3",
        provider_references_first=True,
        grouped=False,
        multiple_prices=True,
        duplicate_first_price=False,
    )
    serving_records = [
        SERVING_RECORD.unpack_from(scan["partition_bytes"], offset)
        for offset in range(
            0,
            len(scan["partition_bytes"]),
            SERVING_RECORD.size,
        )
    ]
    assert len(serving_records) == 2
    provider_set_ids = {
        serving_record[1] for serving_record in serving_records
    }
    assert len(provider_set_ids) == 1
    provider_set_id = next(iter(provider_set_ids))

    artifact_digest = hashlib.sha256(scan["artifact"].read_bytes()).hexdigest()
    source_trace_row, source_trace_set_row = _ptg2_source_trace_rows(
        None,
        "https://example.test/migrated-lifecycle-source.json",
    )
    trace_hash = str(source_trace_row["source_trace_hash"])
    trace_set_hash = str(source_trace_set_row["source_trace_set_hash"])
    identity = SharedPhysicalArtifactIdentity(
        "in_network",
        "logical_json_sha256_v1",
        artifact_digest,
    )
    assignment = SharedSnapshotSourceAssignment(
        source_key=0,
        identity=identity,
        source_trace_set_hash=trace_set_hash,
        source_trace_hashes=(trace_hash,),
        raw_container_sha256=artifact_digest,
        logical_json_sha256=artifact_digest,
        logical_hash_deferred=False,
    )
    source_set = shared_source_set_metadata([artifact_digest])
    semantic_fingerprint = shared_semantic_fingerprint(
        {
            "contract": "migrated_lifecycle_fixture_v1",
            "source_identities": [identity.as_dict()],
            "source_count": 1,
            "coverage_scope_id": COVERAGE_SCOPE_ID.hex(),
        }
    )

    snapshot_a = f"ptg2-v3-lifecycle-a-{uuid.uuid4().hex}"
    snapshot_b = f"ptg2-v3-lifecycle-b-{uuid.uuid4().hex}"
    stage_table: str | None = None
    app = _build_asgi_app()
    client = app.asgi_client
    try:
        await db.status(
            f"""
            INSERT INTO {_quoted(SCHEMA_NAME)}.code_catalog
                (code_system, code, display_name, short_description)
            VALUES
                ('CPT', '99213', 'Office visit 99213', 'Office visit 99213'),
                ('CPT', '99214', 'Office visit 99214', 'Office visit 99214')
            ON CONFLICT (code_system, code) DO UPDATE SET
                display_name = EXCLUDED.display_name,
                short_description = EXCLUDED.short_description
            """
        )
        await _insert_logical_snapshot_prerequisites(
            snapshot_id=snapshot_a,
            plan_id=PLAN_A,
            assignment=assignment,
            source_trace_row=source_trace_row,
        )
        async with db.transaction() as session:
            first_reservation = await reserve_shared_layout(
                session,
                schema_name=SCHEMA_NAME,
                semantic_fingerprint=semantic_fingerprint,
                build_token="migrated-lifecycle-build-a",
            )
        assert first_reservation.reused is False

        stage_table = await _create_ptg2_manifest_serving_stage_table(
            f"lifecycle_{first_reservation.snapshot_key}_{uuid.uuid4().hex[:8]}"
        )
        for frame in scan["price_atom_frames"]:
            await _copy_ptg2_manifest_price_atom_file(
                Path(frame["path"]),
                target_table=_ptg2_manifest_support_stage_table(
                    stage_table,
                    "price_atom",
                ),
            )
        for frame in scan["price_set_atom_frames"]:
            await _copy_price_atom_member_file(
                Path(frame["path"]),
                target_table=_ptg2_manifest_support_stage_table(
                    stage_table,
                    "price_set_atom",
                ),
            )

        provider_set_metadata_path = tmp_path / "provider-set-metadata.copy"
        provider_set_metadata_path.write_text(
            f"{provider_set_id.hex()}\t{{}}\n",
            encoding="ascii",
        )
        graph_entries = _graph_artifacts(
            tmp_path / "migrated-lifecycle-graph",
            provider_set_id=provider_set_id,
            provider_group_id=bytes.fromhex("00112233445566778899aabbccddeeff"),
        )
        scanner_summary = scanner_support._single_frame(
            scan["frames"],
            "scanner_summary",
        )
        serving_run_entries = attach_v3_source_run_contract(
            scan["partition_frames"],
            source_identity=identity,
            scanner_summary=scanner_summary,
            scanner_config=scanner_support._single_frame(
                scan["frames"],
                "scanner_config",
            ),
        )
        code_dictionary_entries = attach_v3_dictionary_contract(
            scan["code_dictionary_frames"],
            source_identity=identity,
            source_run_contract_sha256=serving_run_entries[0][
                "source_run_contract_sha256"
            ],
            scanner_summary=scanner_summary,
        )
        publication = await publish_strict_shared_v3_layout(
            schema_name=SCHEMA_NAME,
            manifest_stage_table=stage_table,
            reserved_snapshot_key=first_reservation.snapshot_key,
            build_token="migrated-lifecycle-build-a",
            expected_coverage_scope_id=COVERAGE_SCOPE_ID,
            logical_snapshot_id=snapshot_a,
            expected_source_identities=[identity],
            serving_run_entries=serving_run_entries,
            code_dictionary_entries=code_dictionary_entries,
            provider_set_metadata_entries=(
                {
                    "path": str(provider_set_metadata_path),
                    "row_count": 1,
                },
            ),
            graph_artifact_entries=graph_entries,
            source_audit_witness_entries=(
                scanner_support._single_frame(
                    scan["frames"],
                    "source_audit_witness_file",
                ),
            ),
            expected_raw_source_sha256=(artifact_digest,),
            provider_identifier_quarantine=scanner_summary[
                "provider_identifier_quarantine"
            ],
            scratch_parent=tmp_path,
        )
        assert publication.snapshot_key == first_reservation.snapshot_key
        assert publication.layout_reused_at_seal is False
        assert publication.serving_index["arch_version"] == "postgres_binary_v3"
        assert publication.serving_index["storage_generation"] == "shared_blocks_v3"
        assert publication.serving_index["cold_lookup_contract"] == "ptg_v3_cold_v2"
        assert publication.serving_index["provider_graph"]["npi_count"] == len(NPIS)
        await _drop_ptg2_snapshot_table_names(
            _ptg2_manifest_stage_table_names(stage_table)
        )
        stage_table = None

        first_serving_index = {
            **dict(publication.serving_index),
            "source_key": SOURCE_A,
            "coverage_scope_id": COVERAGE_SCOPE_ID.hex(),
            "source_set": source_set,
            "source_trace_set_hash": trace_set_hash,
            "network_names": ["Migrated Lifecycle Network"],
        }
        await _stage_candidate(
            snapshot_id=snapshot_a,
            source_key=SOURCE_A,
            plan_id=PLAN_A,
            snapshot_key=publication.snapshot_key,
            serving_index=first_serving_index,
        )
        assert await _physical_counts(publication.snapshot_key) == {
            "layouts": 1,
            "blocks": len({reference.block_hash for reference in publication.references}),
            "mappings": len(publication.references),
            "codes": 2,
            "provider_sets": 1,
            "npis": len(NPIS),
        }

        candidate_prices = await _cold_price_read(
            client,
            monkeypatch,
            snapshot_id=snapshot_a,
            source_key=SOURCE_A,
            plan_id=PLAN_A,
            code="99213",
            candidate=True,
        )
        _assert_exact_price_and_membership(
            candidate_prices,
            snapshot_id=snapshot_a,
            plan_id=PLAN_A,
            code="99213",
            rate=125.5,
        )
        candidate_audit = await _candidate_audit_occurrences(
            client,
            monkeypatch,
            snapshot_id=snapshot_a,
            source_key=SOURCE_A,
            plan_id=PLAN_A,
        )
        assert candidate_audit["audit_sample"]["sample_digest"] == (
            publication.serving_index["audit_sample"]["sample_digest"]
        )
        assert candidate_audit["pagination"]["total"] == len(
            candidate_audit["items"]
        )
        assert {
            int(audit_item["tuple"]["npi"])
            for audit_item in candidate_audit["items"]
        } <= set(NPIS)
        assert {
            audit_item["tuple"]["negotiated_rate"]
            for audit_item in candidate_audit["items"]
        } == {125.5, 250}
        assert candidate_audit["provenance"]["snapshot_id"] == snapshot_a
        assert candidate_audit["source_set"] == source_set

        failed_promotion = await _asgi_request(
            client,
            "post",
            "/control/v1/ptg/source-snapshots/promote",
            json={
                "snapshot_id": snapshot_a,
                "source_key": SOURCE_A,
                "expected_current_snapshot_id": None,
            },
            headers=_control_headers(),
        )
        assert failed_promotion.status_code == 400
        state_before_attestation = await _snapshot_state(snapshot_a)
        assert state_before_attestation == {
            "status": "validated",
            "activation_state": "validated",
            "source_pointer_snapshot_id": None,
            "plan_pointer_snapshot_id": None,
            "global_pointer_snapshot_id": None,
            "activated_at": None,
        }

        report = _release_report(
            snapshot_id=snapshot_a,
            source_key=SOURCE_A,
            plan_id=PLAN_A,
            sample_digest=candidate_audit["audit_sample"]["sample_digest"],
            source_set=source_set,
            source_witness=publication.serving_index["source_witness"],
            provider_identifier_quarantine=publication.serving_index[
                "provider_identifier_quarantine"
            ],
        )
        attestation_response = await _asgi_request(
            client,
            "post",
            "/control/v1/ptg/source-snapshots/attest",
            json={
                "snapshot_id": snapshot_a,
                "source_key": SOURCE_A,
                "plan_id": PLAN_A,
                "plan_market_type": "group",
                "report": report,
            },
            headers=_control_headers(),
        )
        attestation = _response_json(attestation_response)
        assert attestation["status"] == "attested"
        assert attestation["snapshot_id"] == snapshot_a

        promotion_response = await _asgi_request(
            client,
            "post",
            "/control/v1/ptg/source-snapshots/promote",
            json={
                "snapshot_id": snapshot_a,
                "source_key": SOURCE_A,
                "expected_current_snapshot_id": None,
            },
            headers=_control_headers(),
        )
        promotion = _response_json(promotion_response)
        assert promotion == {
            "status": "promoted",
            "source_key": SOURCE_A,
            "snapshot_id": snapshot_a,
            "previous_snapshot_id": None,
            "plan_source_count": 1,
            "global_pointer": "reconciled",
        }
        state_after_activation = await _snapshot_state(snapshot_a)
        assert state_after_activation["status"] == "published"
        assert state_after_activation["activation_state"] == "activated"
        assert state_after_activation["source_pointer_snapshot_id"] == snapshot_a
        assert state_after_activation["plan_pointer_snapshot_id"] == snapshot_a
        assert state_after_activation["global_pointer_snapshot_id"] == snapshot_a
        assert state_after_activation["activated_at"] is not None

        published_prices_99213 = await _cold_price_read(
            client,
            monkeypatch,
            snapshot_id=snapshot_a,
            source_key=SOURCE_A,
            plan_id=PLAN_A,
            code="99213",
            candidate=False,
        )
        _assert_exact_price_and_membership(
            published_prices_99213,
            snapshot_id=snapshot_a,
            plan_id=PLAN_A,
            code="99213",
            rate=125.5,
        )
        published_prices_99214 = await _cold_price_read(
            client,
            monkeypatch,
            snapshot_id=snapshot_a,
            source_key=SOURCE_A,
            plan_id=PLAN_A,
            code="99214",
            candidate=False,
        )
        _assert_exact_price_and_membership(
            published_prices_99214,
            snapshot_id=snapshot_a,
            plan_id=PLAN_A,
            code="99214",
            rate=250,
        )

        physical_counts_before_reuse = await _physical_counts(publication.snapshot_key)
        await _insert_logical_snapshot_prerequisites(
            snapshot_id=snapshot_b,
            plan_id=PLAN_B,
            assignment=assignment,
            source_trace_row=source_trace_row,
        )
        async with db.transaction() as session:
            second_reservation = await reserve_shared_layout(
                session,
                schema_name=SCHEMA_NAME,
                semantic_fingerprint=semantic_fingerprint,
                build_token="migrated-lifecycle-build-b",
            )
        assert second_reservation.reused is True
        assert second_reservation.snapshot_key == publication.snapshot_key

        layout_manifest = await db.scalar(
            f"""
            SELECT layout_manifest
              FROM {_quoted(SCHEMA_NAME)}.ptg2_v3_snapshot_layout
             WHERE snapshot_key = :snapshot_key
            """,
            snapshot_key=publication.snapshot_key,
        )
        second_serving_index = _reused_shared_v3_serving_index(
            layout_manifest,
            source_key=SOURCE_B,
            shared_snapshot_key=publication.snapshot_key,
        )
        second_serving_index.update(
            {
                "coverage_scope_id": COVERAGE_SCOPE_ID.hex(),
                "source_set": source_set,
                "source_trace_set_hash": trace_set_hash,
                "network_names": ["Migrated Lifecycle Network"],
            }
        )
        reused_audit_sample = await validate_reused_shared_v3_snapshot_sources(
            schema_name=SCHEMA_NAME,
            snapshot_key=publication.snapshot_key,
            logical_snapshot_id=snapshot_b,
        )
        assert reused_audit_sample == publication.serving_index["audit_sample"]
        await _stage_candidate(
            snapshot_id=snapshot_b,
            source_key=SOURCE_B,
            plan_id=PLAN_B,
            snapshot_key=publication.snapshot_key,
            serving_index=second_serving_index,
        )
        assert await _physical_counts(publication.snapshot_key) == (
            physical_counts_before_reuse
        )
        bindings = await db.all(
            f"""
            SELECT snapshot_id, snapshot_key
              FROM {_quoted(SCHEMA_NAME)}.ptg2_v3_snapshot_binding
             ORDER BY snapshot_id
            """
        )
        assert [tuple(binding_record) for binding_record in bindings] == sorted(
            [
                (snapshot_a, publication.snapshot_key),
                (snapshot_b, publication.snapshot_key),
            ]
        )

        reused_candidate_prices = await _cold_price_read(
            client,
            monkeypatch,
            snapshot_id=snapshot_b,
            source_key=SOURCE_B,
            plan_id=PLAN_B,
            code="99213",
            candidate=True,
        )
        _assert_exact_price_and_membership(
            reused_candidate_prices,
            snapshot_id=snapshot_b,
            plan_id=PLAN_B,
            code="99213",
            rate=125.5,
        )
        await _mark_ptg2_import_failed(
            f"run-{snapshot_b}",
            snapshot_b,
            datetime.date(2026, 7, 1),
            datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None),
            "intentional terminal state for shared-layout removal coverage",
        )
        assert await db.scalar(
            f"""
            SELECT status
              FROM {_quoted(SCHEMA_NAME)}.ptg2_snapshot
             WHERE snapshot_id = :snapshot_id
            """,
            snapshot_id=snapshot_b,
        ) == "failed"

        first_removal_response = await _asgi_request(
            client,
            "post",
            "/control/v1/ptg/source-snapshots/remove",
            json={"snapshot_id": snapshot_b, "source_key": SOURCE_B},
            headers=_control_headers(),
        )
        first_removal = _response_json(first_removal_response)
        assert first_removal["deleted_v3_snapshot_scopes"] == 1
        assert first_removal["deleted_v3_snapshot_bindings"] == 1
        assert first_removal["deleted_snapshots"] == 1
        assert first_removal["released_shared_layouts"] == 0
        assert await _physical_counts(publication.snapshot_key) == (
            physical_counts_before_reuse
        )

        surviving_prices = await _cold_price_read(
            client,
            monkeypatch,
            snapshot_id=snapshot_a,
            source_key=SOURCE_A,
            plan_id=PLAN_A,
            code="99214",
            candidate=False,
        )
        _assert_exact_price_and_membership(
            surviving_prices,
            snapshot_id=snapshot_a,
            plan_id=PLAN_A,
            code="99214",
            rate=250,
        )

        refused_retirement = await _asgi_request(
            client,
            "post",
            "/control/v1/ptg/source-snapshots/retire",
            json={"snapshot_id": snapshot_a, "source_key": SOURCE_A},
            headers=_control_headers(),
        )
        assert refused_retirement.status_code == 400
        assert "global pointer" in refused_retirement.text.lower()

        async with db.transaction() as session:
            await acquire_ptg2_lifecycle_lock(session)
            deleted_global_pointer = await session.execute(
                db.text(
                    f"""
                    DELETE FROM {_quoted(SCHEMA_NAME)}.ptg2_current_snapshot
                     WHERE slot = 'current'
                       AND snapshot_id = :snapshot_id
                    RETURNING snapshot_id
                    """
                ),
                {"snapshot_id": snapshot_a},
            )
            assert deleted_global_pointer.scalar_one() == snapshot_a

        retirement_response = await _asgi_request(
            client,
            "post",
            "/control/v1/ptg/source-snapshots/retire",
            json={"snapshot_id": snapshot_a, "source_key": SOURCE_A},
            headers=_control_headers(),
        )
        retirement = _response_json(retirement_response)
        assert retirement["deleted_source_pointers"] == 1
        assert retirement["deleted_plan_pointers"] == 1
        assert retirement["current_references"] == {
            "global_slots": [],
            "source_keys": [],
            "plan_source_keys": [],
            "previous_global_slots": [],
            "previous_source_keys": [],
            "previous_plan_source_keys": [],
        }

        block_rows = await db.all(
            f"""
            SELECT block_hash, stored_byte_count
              FROM {_quoted(SCHEMA_NAME)}.ptg2_v3_block
             ORDER BY block_hash
            """
        )
        expected_block_size_by_hash = {
            bytes(block_record[0]): int(block_record[1])
            for block_record in block_rows
        }
        assert expected_block_size_by_hash

        final_removal_response = await _asgi_request(
            client,
            "post",
            "/control/v1/ptg/source-snapshots/remove",
            json={"snapshot_id": snapshot_a, "source_key": SOURCE_A},
            headers=_control_headers(),
        )
        final_removal = _response_json(final_removal_response)
        assert final_removal["deleted_v3_snapshot_scopes"] == 1
        assert final_removal["deleted_v3_snapshot_bindings"] == 1
        assert final_removal["deleted_snapshots"] == 1
        assert final_removal["released_shared_layouts"] == 1
        assert await _count("ptg2_v3_snapshot_layout") == 0
        assert await _count("ptg2_v3_block") == len(expected_block_size_by_hash)

        swept = await sweep_ptg2_shared_blocks(
            schema_name=SCHEMA_NAME,
            max_bytes=sum(expected_block_size_by_hash.values()),
            max_rows=len(expected_block_size_by_hash),
        )
        assert set(swept.selected_hashes) == set(expected_block_size_by_hash)
        assert swept.stored_bytes == sum(expected_block_size_by_hash.values())
        for table_name in FINAL_EMPTY_TABLES:
            assert await _count(table_name) == 0
        assert await _count("ptg2_source_trace") == 1
        assert await _count("ptg2_source_trace_set") == 1
    finally:
        if stage_table is not None:
            await _drop_ptg2_snapshot_table_names(
                _ptg2_manifest_stage_table_names(stage_table)
            )
        await db.disconnect()
