# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import binascii
import gzip
import importlib
import io
import json
import os
import struct
import subprocess
import sys
import threading
import time
import zipfile
from types import SimpleNamespace
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock, Mock

import pytest
from aiohttp import web


process_pkg = importlib.import_module("process")
process_ptg = importlib.import_module("process.ptg")
live_progress = importlib.import_module("process.live_progress")
ptg_allowed_amounts = importlib.import_module("process.ptg_parts.allowed_amounts")
ptg_artifacts = importlib.import_module("process.ptg_parts.artifacts")
ptg_artifact_streams = importlib.import_module("process.ptg_parts.artifact_streams")
ptg_canonical = importlib.import_module("process.ptg_parts.canonical")
ptg_compact_bulk = importlib.import_module("process.ptg_parts.compact_bulk")
ptg_compact_indexes = importlib.import_module("process.ptg_parts.compact_indexes")
ptg_compact_state = importlib.import_module("process.ptg_parts.compact_state")
ptg_compact_writes = importlib.import_module("process.ptg_parts.compact_writes")
ptg_copy_load = importlib.import_module("process.ptg_parts.copy_load")
ptg_db_tables = importlib.import_module("process.ptg_parts.db_tables")
ptg_domain = importlib.import_module("process.ptg_parts.domain")
ptg_import_rows = importlib.import_module("process.ptg_parts.import_rows")
ptg_json_streams = importlib.import_module("process.ptg_parts.json_streams")
ptg_live_progress = importlib.import_module("process.ptg_parts.live_progress")
ptg_progress = importlib.import_module("process.ptg_parts.progress")
ptg_config = importlib.import_module("process.ptg_parts.config")


def _write_deflate64_zip(path: Path, member_name: str, member_payload: bytes) -> None:
    inflate64 = pytest.importorskip("inflate64")
    name_bytes = member_name.encode("utf-8")
    deflater = inflate64.Deflater()
    compressed = deflater.deflate(member_payload) + deflater.flush()
    crc = binascii.crc32(member_payload) & 0xFFFFFFFF
    local_header = struct.pack(
        "<IHHHHHIIIHH",
        0x04034B50,
        45,
        0,
        9,
        0,
        0,
        crc,
        len(compressed),
        len(member_payload),
        len(name_bytes),
        0,
    )
    central_header = struct.pack(
        "<IHHHHHHIIIHHHHHII",
        0x02014B50,
        45,
        45,
        0,
        9,
        0,
        0,
        crc,
        len(compressed),
        len(member_payload),
        len(name_bytes),
        0,
        0,
        0,
        0,
        0,
        0,
    )
    central_offset = len(local_header) + len(name_bytes) + len(compressed)
    central_size = len(central_header) + len(name_bytes)
    end_record = struct.pack(
        "<IHHHHIIH",
        0x06054B50,
        0,
        0,
        1,
        1,
        central_size,
        central_offset,
        0,
    )
    path.write_bytes(local_header + name_bytes + compressed + central_header + name_bytes + end_record)
ptg_provider_references = importlib.import_module("process.ptg_parts.provider_references")
ptg_row_helpers = importlib.import_module("process.ptg_parts.row_helpers")
ptg_rust_publish = importlib.import_module("process.ptg_parts.rust_publish")
ptg_rust_scanner = importlib.import_module("process.ptg_parts.rust_scanner")
ptg_rust_stage = importlib.import_module("process.ptg_parts.rust_stage")
ptg_manifest_publish = importlib.import_module("process.ptg_parts.ptg2_manifest_publish")
ptg_manifest_cleanup = importlib.import_module("process.ptg_parts.ptg2_manifest_cleanup")
ptg_screen = importlib.import_module("process.ptg_parts.screen")
ptg_serving_index = importlib.import_module("process.ptg_parts.serving_index")


def test_default_ptg2_import_id_includes_source_inputs():
    month = process_ptg.normalize_import_month("2026-06")

    example_dental_id = process_ptg._default_ptg2_import_id(
        month,
        "ptg_d2fc90445ba744ef",
        allowed_url="https://example.com/example-dental-allowed.json.gz",
    )
    optum_id = process_ptg._default_ptg2_import_id(
        month,
        "ptg_39d5fb35eae985f2",
        in_network_url="https://example.com/optum-rates.json.gz",
    )
    materialized_id = process_ptg._default_ptg2_import_id(
        month,
        "ptg_39d5fb35eae985f2",
        in_network_url="https://example.com/optum-rates.json.gz",
        arch_variant="materialized_v1",
    )
    sidecar_id = process_ptg._default_ptg2_import_id(
        month,
        "ptg_39d5fb35eae985f2",
        in_network_url="https://example.com/optum-rates.json.gz",
        arch_variant="sidecar_scope_v1",
    )

    assert example_dental_id != "20260601"
    assert optum_id != "20260601"
    assert example_dental_id != optum_id
    assert materialized_id != optum_id
    assert sidecar_id != optum_id
    assert materialized_id != sidecar_id


def _snapshot_identity_test_options() -> dict[str, object]:
    return {
        "toc_urls": ["https://example.test/index.json"],
        "toc_list": None,
        "in_network_url": None,
        "allowed_url": None,
        "provider_ref_url": None,
        "source_key": "source_a",
        "plan_ids": [],
        "plan_name_contains": [],
        "plan_market_types": [],
        "file_url_contains": [],
        "source_network_names": [],
        "max_files": None,
        "max_items": None,
        "compact_import": True,
        "serving_only_import": True,
        "source_scoped_compact": True,
        "serving_storage": "manifest_snapshot",
        "snapshot_arch": "sidecar_scope_v1",
        "snapshot_arch_variant": None,
        "test_mode": False,
        "allow_partial_import": False,
        "hash_mode": "checksum64",
        "provider_bucket_count": 64,
        "provider_set_inline_npi_limit": 0,
        "id_storage": "uuid",
        "manifest_serving_layout": "",
        "manifest_price_atom_layout": "",
        "manifest_provider_group_location": True,
        "manifest_provider_set_component": False,
        "manifest_provider_group_rate_scope": False,
        "manifest_serving_sidecars": True,
        "manifest_drop_serving_table": True,
        "manifest_postgres_binary_natural_lean_stream": False,
        "manifest_provider_npi_sidecar": True,
    }


def test_v3_snapshot_identity_variant_requires_fenced_reimport():
    assert process_ptg._ptg2_snapshot_arch_identity_variant(
        "postgres_binary_v3",
        "postgres_binary_v3",
    ) == "postgres_binary_v3:membership_fences_v1"
    assert process_ptg._ptg2_snapshot_arch_identity_variant(
        "postgres_binary_v2",
        "postgres_binary_v2",
    ) == "postgres_binary_v2"


@pytest.mark.parametrize(
    ("option_name", "changed_value"),
    [
        ("plan_ids", ["plan-1"]),
        ("plan_name_contains", ["acme"]),
        ("plan_market_types", ["group"]),
        ("file_url_contains", ["rates"]),
        ("source_network_names", ["Network A"]),
        ("max_files", 1),
        ("max_items", 25),
        ("test_mode", True),
        ("snapshot_arch", "postgres_binary_v2"),
        ("snapshot_arch_variant", "candidate_v2"),
        ("serving_only_import", False),
        ("allow_partial_import", True),
        ("hash_mode", "sha256"),
        ("provider_bucket_count", 32),
        ("provider_set_inline_npi_limit", 10),
        ("id_storage", "hex"),
        ("manifest_serving_layout", "lean_provider_key_v1"),
        ("manifest_price_atom_layout", "lean_dict_v2"),
        ("manifest_provider_group_location", False),
        ("manifest_provider_set_component", True),
        ("manifest_provider_group_rate_scope", True),
        ("manifest_serving_sidecars", False),
        ("manifest_drop_serving_table", False),
        ("manifest_postgres_binary_natural_lean_stream", True),
        ("manifest_provider_npi_sidecar", False),
    ],
)
def test_snapshot_identity_changes_for_content_options(option_name, changed_value):
    month = process_ptg.normalize_import_month("2026-07")
    base_option_by_name = _snapshot_identity_test_options()
    changed_option_by_name = {**base_option_by_name, option_name: changed_value}

    base_snapshot_id = process_ptg._ptg2_deterministic_snapshot_id(
        import_month=month,
        import_id="delivery-a",
        option_by_name=base_option_by_name,
    )
    changed_snapshot_id = process_ptg._ptg2_deterministic_snapshot_id(
        import_month=month,
        import_id="delivery-a",
        option_by_name=changed_option_by_name,
    )

    assert changed_snapshot_id != base_snapshot_id


def test_snapshot_identity_normalizes_filters_and_ignores_operational_options(monkeypatch):
    month = process_ptg.normalize_import_month("2026-07")
    first_option_by_name = {
        **_snapshot_identity_test_options(),
        "plan_ids": ["PLAN-B", "plan-a", "plan-a"],
        "reuse_raw_artifacts": True,
        "keep_partial_artifacts": True,
        "async_write_tasks": 1,
        "fast_provider_union": False,
    }
    second_option_by_name = {
        **first_option_by_name,
        "plan_ids": ["plan-a", "plan-b"],
        "snapshot_arch_variant": "sidecar_scope_v1",
        "reuse_raw_artifacts": False,
        "keep_partial_artifacts": False,
        "async_write_tasks": 12,
        "fast_provider_union": True,
    }
    monkeypatch.setenv(process_ptg.PTG2_HASH_MODE_ENV, "blake2")

    first_snapshot_id = process_ptg._ptg2_deterministic_snapshot_id(
        import_month=month,
        import_id="delivery-a",
        option_by_name=first_option_by_name,
    )
    second_snapshot_id = process_ptg._ptg2_deterministic_snapshot_id(
        import_month=month,
        import_id="delivery-a",
        option_by_name=second_option_by_name,
    )

    assert second_snapshot_id == first_snapshot_id


@pytest.mark.parametrize(
    ("configured_mode", "effective_mode"),
    [
        ("sha256", "sha256"),
        ("blake2", "blake2_128"),
        ("blake2b", "blake2_128"),
        ("blake2_128", "blake2_128"),
        ("unexpected", "checksum64"),
    ],
)
def test_snapshot_identity_normalizes_effective_hash_mode(
    monkeypatch,
    configured_mode,
    effective_mode,
):
    monkeypatch.setenv(process_ptg.PTG2_HASH_MODE_ENV, configured_mode)

    assert process_ptg._ptg2_effective_hash_mode() == effective_mode


def test_candidate_serving_index_enumerates_every_possible_final_table():
    candidate_index = process_ptg._ptg2_candidate_serving_index(
        source_key="source_a",
        snapshot_id="ptg2:202607:candidate",
    )

    candidate_tables = set(process_ptg._snapshot_manifest_table_names(candidate_index))

    assert candidate_tables == {
        table_name.split(".", 1)[-1]
        for table_name in candidate_index["materialized_tables"].values()
    }
    assert process_ptg._ptg2_manifest_stage_table_names("manifest_stage") == [
        "manifest_stage",
        "ptg2_manifest_stage_price_atom_manifest_stage",
        "ptg2_manifest_stage_price_set_atom_manifest_stage",
        "ptg2_manifest_stage_provider_group_member_manifest_stage",
        "ptg2_manifest_stage_provider_npi_scope_manifest_stage",
        "ptg2_manifest_stage_code_count_manifest_stage",
        "ptg2_manifest_stage_provider_set_dictionary_manifest_stage",
    ]


def test_ptg2_auto_address_refresh_payload_defaults(monkeypatch):
    monkeypatch.delenv(process_ptg.PTG2_AUTO_ADDRESS_REFRESH_LIMIT_ENV, raising=False)
    monkeypatch.delenv(process_ptg.PTG2_AUTO_ADDRESS_REFRESH_PUBLISH_ENV, raising=False)

    payload = process_ptg._ptg2_auto_address_refresh_payload(
        source_key="source-alpha",
        snapshot_id="snap-new",
        import_run_id="run-source-alpha",
        test_mode=False,
    )

    assert payload == {
        "run_id": None,
        "importer": "entity-address-unified",
        "params": {
            "refresh_mode": "full",
            "trigger_source_key": "source-alpha",
            "trigger_snapshot_id": "snap-new",
            "publish": True,
        },
        "idempotency_key": "entity-address-unified:source-alpha:snap-new",
        "triggered_by": "ptg_import",
        "schedule_id": None,
        "subscription_id": None,
        "import_id": "entity-address-unified:run-source-alpha",
    }


def test_ptg2_auto_address_refresh_payload_honors_limit_and_publish_env(monkeypatch):
    monkeypatch.setenv(process_ptg.PTG2_AUTO_ADDRESS_REFRESH_LIMIT_ENV, "25")
    monkeypatch.setenv(process_ptg.PTG2_AUTO_ADDRESS_REFRESH_PUBLISH_ENV, "false")

    payload = process_ptg._ptg2_auto_address_refresh_payload(
        source_key="source-alpha",
        snapshot_id="snap-new",
        import_run_id="run-source-alpha",
        test_mode=True,
    )

    assert payload["params"]["publish"] is False
    assert payload["params"]["limit_per_source"] == 25
    assert payload["params"]["test_mode"] is True


def test_ptg2_auto_address_refresh_skips_test_mode_by_default(monkeypatch):
    monkeypatch.setenv(process_ptg.PTG2_AUTO_ADDRESS_REFRESH_ENV, "true")
    monkeypatch.delenv(process_ptg.PTG2_AUTO_ADDRESS_REFRESH_TEST_ENV, raising=False)

    result = asyncio.run(
        process_ptg._enqueue_ptg2_auto_address_refresh_after_import(
            source_key="source-alpha",
            snapshot_id="snap-new",
            import_run_id="run-source-alpha",
            has_serving_files=True,
            source_scoped_compact=True,
            test_mode=True,
        )
    )

    assert result == {"status": "skipped", "reason": "test-mode-disabled"}


def test_ptg2_auto_address_refresh_enqueues_control_run(monkeypatch):
    control_imports = importlib.import_module("api.control_imports")
    calls = []
    monkeypatch.setenv(process_ptg.PTG2_AUTO_ADDRESS_REFRESH_ENV, "true")

    async def fake_ensure_import_run_table():
        calls.append({"ensure": True})

    async def fake_create_import_run(payload):
        calls.append(payload)
        return {"run_id": "run-refresh", "importer": payload["importer"]}, True

    monkeypatch.setattr(control_imports, "ensure_import_run_table", fake_ensure_import_run_table)
    monkeypatch.setattr(control_imports, "create_import_run", fake_create_import_run)

    result = asyncio.run(
        process_ptg._enqueue_ptg2_auto_address_refresh_after_import(
            source_key="source-alpha",
            snapshot_id="snap-new",
            import_run_id="run-source-alpha",
            has_serving_files=True,
            source_scoped_compact=True,
            test_mode=False,
        )
    )

    assert result["status"] == "queued"
    assert result["created"] is True
    assert result["run_id"] == "run-refresh"
    assert calls[0] == {"ensure": True}
    assert calls[1]["importer"] == "entity-address-unified"
    assert calls[1]["idempotency_key"] == "entity-address-unified:source-alpha:snap-new"
    assert calls[1]["triggered_by"] == "ptg_import"
    assert calls[1]["params"]["refresh_mode"] == "full"
    assert calls[1]["params"]["trigger_source_key"] == "source-alpha"
    assert calls[1]["params"]["trigger_snapshot_id"] == "snap-new"


def test_ptg2_auto_address_refresh_reports_existing_or_enqueue_failure(monkeypatch):
    control_imports = importlib.import_module("api.control_imports")
    monkeypatch.setenv(process_ptg.PTG2_AUTO_ADDRESS_REFRESH_ENV, "true")

    async def fake_ensure_import_run_table():
        return None

    async def fake_existing_import_run(payload):
        return {"run_id": "run-existing", "importer": payload["importer"]}, False

    monkeypatch.setattr(control_imports, "ensure_import_run_table", fake_ensure_import_run_table)
    monkeypatch.setattr(control_imports, "create_import_run", fake_existing_import_run)
    existing = asyncio.run(
        process_ptg._enqueue_ptg2_auto_address_refresh_after_import(
            source_key="source-alpha",
            snapshot_id="snap-new",
            import_run_id="run-source-alpha",
            has_serving_files=True,
            source_scoped_compact=True,
            test_mode=False,
        )
    )

    async def fake_failed_import_run(_payload):
        raise RuntimeError("redis unavailable")

    monkeypatch.setattr(control_imports, "create_import_run", fake_failed_import_run)
    failed = asyncio.run(
        process_ptg._enqueue_ptg2_auto_address_refresh_after_import(
            source_key="source-alpha",
            snapshot_id="snap-new",
            import_run_id="run-source-alpha",
            has_serving_files=True,
            source_scoped_compact=True,
            test_mode=False,
        )
    )

    assert existing["status"] == "existing"
    assert existing["created"] is False
    assert existing["run_id"] == "run-existing"
    assert failed["status"] == "enqueue_failed"
    assert failed["error"] == "redis unavailable"


ptg_serving_maintenance = importlib.import_module("process.ptg_parts.serving_maintenance")
ptg_serving_rows = importlib.import_module("process.ptg_parts.serving_rows")
ptg_serving_only = importlib.import_module("process.ptg_parts.serving_only")
ptg_snapshot_cleanup = importlib.import_module("process.ptg_parts.snapshot_cleanup")
ptg_snapshot_artifacts = importlib.import_module("process.ptg_parts.snapshot_artifacts")
ptg_snapshot_tables = importlib.import_module("process.ptg_parts.snapshot_tables")
ptg_source_download = importlib.import_module("process.ptg_parts.source_download")
ptg_source_files = importlib.import_module("process.ptg_parts.source_files")
ptg_source_jobs = importlib.import_module("process.ptg_parts.source_jobs")
ptg_source_pointers = importlib.import_module("process.ptg_parts.source_pointers")
ptg_source_versions = importlib.import_module("process.ptg_parts.source_versions")
ptg_table_setup = importlib.import_module("process.ptg_parts.table_setup")
ptg_values = importlib.import_module("process.ptg_parts.values")


class _ClosedStream:
    def write(self, _value):
        raise OSError("closed")

    def flush(self):
        raise OSError("closed")


def test_screen_writer_ignores_closed_capture_stream(monkeypatch):
    monkeypatch.setattr(ptg_screen.sys, "stdout", _ClosedStream())

    ptg_screen._write_screen_line("stdout", "progress")


def test_canonical_split_keeps_facade_helpers_stable():
    assert process_ptg.normalize_money is ptg_canonical.normalize_money
    assert process_ptg.normalize_date is ptg_canonical.normalize_date
    assert process_ptg.semantic_hash is ptg_canonical.semantic_hash
    assert process_ptg.canonicalize_url is ptg_canonical.canonicalize_url
    assert process_ptg.normalize_import_month is ptg_canonical.normalize_import_month


def test_values_split_keeps_facade_helpers_stable():
    assert process_ptg.build_provider_set is ptg_values.build_provider_set
    assert process_ptg.build_price_set is ptg_values.build_price_set
    assert process_ptg.build_fact_chunk is ptg_values.build_fact_chunk
    assert process_ptg.build_rate_set is ptg_values.build_rate_set
    assert process_ptg.provider_hash_bucket is ptg_values.provider_hash_bucket


def test_row_helper_split_keeps_facade_helpers_stable():
    assert process_ptg._make_checksum is ptg_row_helpers._make_checksum
    assert process_ptg._as_int_list is ptg_row_helpers._as_int_list
    assert process_ptg._normalized_npi_list is ptg_row_helpers._normalized_npi_list
    assert process_ptg._provider_group_identity_hash is ptg_row_helpers._provider_group_identity_hash
    assert process_ptg._normalize_code_component is ptg_row_helpers._normalize_code_component


def test_progress_split_keeps_facade_helpers_stable():
    assert process_ptg._utcnow is ptg_progress._utcnow
    assert process_ptg._artifact_progress_position is ptg_progress._artifact_progress_position
    assert ptg_progress._scale_stage_progress_pct(50, 5, 20) == 12.5


def test_ptg_live_progress_uses_redis_ttl_and_enqueues_status_event(monkeypatch):
    writes = []
    events = []

    class FakeRedis:
        def setex(self, key, ttl, value):
            writes.append((key, ttl, value))

    monkeypatch.setattr(live_progress, "_redis", lambda: FakeRedis())
    monkeypatch.setattr(live_progress, "enqueue_status_event", events.append)
    token = ptg_live_progress.set_live_progress_context(run_id="run_ptg", snapshot_id="snap_1")
    try:
        ptg_live_progress.write_live_progress(phase="download", pct=12.5, eta_seconds=60, message="downloading")
    finally:
        ptg_live_progress.reset_live_progress_context(token)

    assert writes[0][0] == "import:progress:run_ptg"
    assert writes[0][1] == ptg_live_progress.PTG_LIVE_PROGRESS_TTL_SECONDS
    assert events[0]["run_id"] == "run_ptg"
    assert events[0]["phase_detail"] == "download"
    assert events[0]["progress"]["pct"] == 12.5
    assert events[0]["estimate"]["eta_seconds"] == 60
    assert process_ptg._format_duration is ptg_progress._format_duration
    assert process_ptg._maybe_log_artifact_progress is ptg_progress._maybe_log_artifact_progress


def test_live_progress_preserves_earliest_started_at(monkeypatch):
    writes = []
    previous = {
        "run_id": "run_any",
        "importer": "npi",
        "status": "running",
        "started_at": "2026-06-03T12:00:00Z",
        "updated_at": "2026-06-03T12:01:00Z",
    }

    class FakeRedis:
        def get(self, key):
            assert key == "import:progress:run_any"
            return json.dumps(previous).encode("utf-8")

        def setex(self, key, ttl, value):
            writes.append((key, ttl, value))

    monkeypatch.setattr(live_progress, "_redis", lambda: FakeRedis())
    monkeypatch.setattr(live_progress, "enqueue_status_event", lambda _event: None)

    live_progress.write_live_progress(
        run_id="run_any",
        importer="npi",
        status="running",
        started_at="2026-06-03T12:05:00Z",
        unit="records",
        done=5,
        total=10,
        message="parsing",
    )

    payload = json.loads(writes[0][2])
    assert payload["started_at"] == previous["started_at"]


def test_live_progress_heartbeat_preserves_recent_importer_progress(monkeypatch):
    store = {}

    class FakeRedis:
        def get(self, key):
            return store.get(key)

        def setex(self, key, _ttl, value):
            store[key] = value

    monkeypatch.setattr(live_progress, "_redis", lambda: FakeRedis())
    monkeypatch.setattr(live_progress, "enqueue_status_event", lambda _event: None)

    live_progress.write_live_progress(
        run_id="run_any",
        importer="entity-address-unified",
        status="running",
        phase="entity-address-unified building medication bridge",
        unit="steps",
        done=5,
        total=7,
        pct=97,
        message="building support table 5/7: medication bridge",
        source="import-live-progress",
        confidence="live",
    )
    live_progress.write_live_progress(
        run_id="run_any",
        importer="entity-address-unified",
        status="running",
        phase="process_data running",
        unit="run",
        done=0,
        total=1,
        pct=0,
        message="running",
        started_at="2026-06-03T12:00:00Z",
        source="import-control-heartbeat",
        confidence="heartbeat",
    )

    payload = json.loads(store["import:progress:run_any"])
    assert payload["phase"] == "entity-address-unified building medication bridge"
    assert payload["message"] == "building support table 5/7: medication bridge"
    assert payload["unit"] == "steps"
    assert payload["done"] == 5
    assert payload["total"] == 7
    assert payload["pct"] == 97
    assert payload["source"] == "import-live-progress"
    assert payload["confidence"] == "live"


def test_artifact_split_keeps_facade_helpers_stable():
    assert process_ptg.PTG2ArtifactStore is ptg_artifacts.PTG2ArtifactStore
    assert process_ptg.sha256_file is ptg_artifacts.sha256_file
    assert process_ptg.choose_reusable_raw_artifact is ptg_artifacts.choose_reusable_raw_artifact
    assert process_ptg.content_addressed_path is ptg_artifacts.content_addressed_path
    assert process_ptg.ptg2_temp_parent is ptg_artifacts.ptg2_temp_parent


def test_artifact_stream_split_keeps_facade_helpers_stable():
    assert process_ptg.open_json_artifact_stream is ptg_artifact_streams.open_json_artifact_stream
    assert process_ptg.logical_artifact_identity is ptg_artifact_streams.logical_artifact_identity
    assert process_ptg.stream_logical_artifact is ptg_artifact_streams.stream_logical_artifact
    assert process_ptg.load_json_artifact is ptg_artifact_streams.load_json_artifact


def test_allowed_amount_split_keeps_facade_helpers_stable():
    assert process_ptg._parse_allowed_amounts is ptg_allowed_amounts._parse_allowed_amounts
    assert process_ptg._process_allowed_amounts_file is ptg_allowed_amounts._process_allowed_amounts_file


def test_db_table_split_keeps_facade_helpers_stable():
    assert process_ptg._quote_ident is ptg_db_tables._quote_ident
    assert process_ptg._table_exists is ptg_db_tables._table_exists
    assert process_ptg._table_has_rows is ptg_db_tables._table_has_rows
    assert process_ptg._estimated_table_rows is ptg_db_tables._estimated_table_rows
    assert process_ptg._exact_table_rows is ptg_db_tables._exact_table_rows


def test_compact_index_split_keeps_facade_helpers_stable():
    assert process_ptg._PTG2_COMPACT_MODEL_BY_KIND is ptg_compact_indexes._PTG2_COMPACT_MODEL_BY_KIND
    assert process_ptg._ptg2_model_snapshot_index_role is ptg_compact_indexes._ptg2_model_snapshot_index_role
    assert process_ptg._ptg2_index_timestamp is ptg_compact_indexes._ptg2_index_timestamp
    assert process_ptg._ptg2_compact_serving_index_mode is ptg_compact_indexes._ptg2_compact_serving_index_mode
    assert process_ptg._ptg2_compact_dictionary_index_mode is ptg_compact_indexes._ptg2_compact_dictionary_index_mode
    assert (
        process_ptg._ptg2_compact_serving_reported_index_statement
        is ptg_compact_indexes._ptg2_compact_serving_reported_index_statement
    )
    assert process_ptg._ptg2_model_index_statements_for_table is ptg_compact_indexes._ptg2_model_index_statements_for_table
    assert process_ptg._run_ptg2_index_statement is ptg_compact_indexes._run_ptg2_index_statement
    assert process_ptg._index_snapshot_compact_table_entries is ptg_compact_indexes._index_snapshot_compact_table_entries
    assert process_ptg._index_snapshot_compact_tables is ptg_compact_indexes._index_snapshot_compact_tables


def test_compact_state_split_keeps_facade_helpers_stable():
    assert process_ptg._compact_state is ptg_compact_state._compact_state
    assert process_ptg._compact_streaming_dedupe_tables is ptg_compact_state._compact_streaming_dedupe_tables
    assert process_ptg._compact_add_unique is ptg_compact_state._compact_add_unique
    assert process_ptg._ptg2_price_atom_payload is ptg_compact_state._ptg2_price_atom_payload
    assert process_ptg._ptg2_source_trace_payload is ptg_compact_state._ptg2_source_trace_payload


def test_compact_bulk_split_keeps_facade_helpers_stable():
    assert process_ptg.prepare_ptg2_compact_bulk_load is ptg_compact_bulk.prepare_ptg2_compact_bulk_load
    assert process_ptg._flush_in_network_rows is ptg_compact_bulk._flush_in_network_rows


def test_compact_write_split_keeps_facade_helpers_stable():
    assert process_ptg._existing_price_set_hashes is ptg_compact_writes._existing_price_set_hashes
    assert process_ptg._flush_compact_rows is ptg_compact_writes._flush_compact_rows
    assert process_ptg._schedule_compact_write is ptg_compact_writes._schedule_compact_write
    assert process_ptg._drain_compact_writes is ptg_compact_writes._drain_compact_writes
    assert process_ptg._flush_compact_rate_pack_groups is ptg_compact_writes._flush_compact_rate_pack_groups


def test_copy_load_split_keeps_facade_helpers_stable():
    assert process_ptg._json_default is ptg_copy_load._json_default
    assert process_ptg._ptg2_conflict_targets is ptg_copy_load._ptg2_conflict_targets
    assert process_ptg._primary_key_column_names is ptg_copy_load._primary_key_column_names
    assert process_ptg._ptg2_json_columns is ptg_copy_load._ptg2_json_columns
    assert process_ptg._ptg2_copy_record is ptg_copy_load._ptg2_copy_record
    assert process_ptg._copy_upsert_ptg2_objects is ptg_copy_load._copy_upsert_ptg2_objects
    assert process_ptg._copy_insert_ptg2_objects is ptg_copy_load._copy_insert_ptg2_objects
    assert process_ptg._copy_ignore_ptg2_objects is ptg_copy_load._copy_ignore_ptg2_objects
    assert process_ptg._copy_stage_price_set_rows is ptg_copy_load._copy_stage_price_set_rows
    assert process_ptg._copy_stage_serving_rate_rows is ptg_copy_load._copy_stage_serving_rate_rows
    assert process_ptg._copy_compact_serving_rate_rows is ptg_copy_load._copy_compact_serving_rate_rows
    assert process_ptg._copy_compact_serving_rate_file is ptg_copy_load._copy_compact_serving_rate_file
    assert process_ptg._copy_compact_serving_rate_source is ptg_copy_load._copy_compact_serving_rate_source
    assert process_ptg._copy_ptg2_dictionary_file is ptg_copy_load._copy_ptg2_dictionary_file


def test_copy_load_strips_postgres_nuls_from_text_values():
    row = {
        "plain": "ab\0cd",
        "array": ["ef\0gh"],
        "payload": {"ij\0": ["kl\0mn"]},
    }

    record = ptg_copy_load._ptg2_copy_record(row, ["plain", "array", "payload"], {"payload"})

    assert record[0] == "abcd"
    assert record[1] == ["efgh"]
    assert json.loads(record[2]) == {"ij": ["klmn"]}


def test_import_row_split_keeps_facade_helpers_stable():
    assert process_ptg._normalize_import_id is ptg_import_rows._normalize_import_id
    assert process_ptg._ptg2_provider_group_rows is ptg_import_rows._ptg2_provider_group_rows
    assert process_ptg._build_provider_set_entry is ptg_import_rows._build_provider_set_entry
    assert process_ptg._combine_provider_set_entries is ptg_import_rows._combine_provider_set_entries
    assert process_ptg._fast_provider_entry_from_parts is ptg_import_rows._fast_provider_entry_from_parts
    assert process_ptg._fast_provider_entry_from_provider_refs is ptg_import_rows._fast_provider_entry_from_provider_refs
    assert process_ptg._ptg2_provider_set_row is ptg_import_rows._ptg2_provider_set_row
    assert process_ptg._ptg2_procedure_row is ptg_import_rows._ptg2_procedure_row
    assert process_ptg._ptg2_price_atom_row is ptg_import_rows._ptg2_price_atom_row
    assert process_ptg._ptg2_source_trace_rows is ptg_import_rows._ptg2_source_trace_rows
    assert process_ptg._ptg2_context_row is ptg_import_rows._ptg2_context_row
    assert process_ptg._ptg2_plan_rows is ptg_import_rows._ptg2_plan_rows


def test_snapshot_table_split_keeps_facade_helpers_stable():
    assert process_ptg._normalize_source_key is ptg_snapshot_tables._normalize_source_key
    assert process_ptg._ptg2_snapshot_table_token is ptg_snapshot_tables._ptg2_snapshot_table_token
    assert process_ptg._ptg2_snapshot_table_name is ptg_snapshot_tables._ptg2_snapshot_table_name
    assert process_ptg._ptg2_snapshot_index_name is ptg_snapshot_tables._ptg2_snapshot_index_name


def test_source_pointer_split_keeps_facade_helpers_stable():
    assert process_ptg._ptg2_plan_source_key is ptg_source_pointers._ptg2_plan_source_key
    assert process_ptg._current_source_snapshot_id is ptg_source_pointers._current_source_snapshot_id
    assert process_ptg._source_plan_rows is ptg_source_pointers._source_plan_rows
    assert process_ptg._publish_ptg2_source_pointers is ptg_source_pointers._publish_ptg2_source_pointers


def test_source_job_split_keeps_facade_helpers_stable():
    assert process_ptg._normalize_filter_values is ptg_source_jobs._normalize_filter_values
    assert process_ptg._dedupe_preserve is ptg_source_jobs._dedupe_preserve
    assert process_ptg._dedupe_rows_by is ptg_source_jobs._dedupe_rows_by
    assert process_ptg._plan_matches_filters is ptg_source_jobs._plan_matches_filters
    assert process_ptg._filter_reporting_plans is ptg_source_jobs._filter_reporting_plans
    assert process_ptg._normalize_plan_payload is ptg_source_jobs._normalize_plan_payload
    assert process_ptg.parse_toc_catalog_entries is ptg_source_jobs.parse_toc_catalog_entries
    assert process_ptg._load_toc_urls_from_file is ptg_source_jobs._load_toc_urls_from_file
    assert process_ptg._filter_jobs_by_url_contains is ptg_source_jobs._filter_jobs_by_url_contains
    assert process_ptg._ptg_job_identity is ptg_source_jobs._ptg_job_identity
    assert process_ptg._plan_identity is ptg_source_jobs._plan_identity
    assert process_ptg._merge_ptg_job is ptg_source_jobs._merge_ptg_job
    assert process_ptg._dedupe_ptg_jobs is ptg_source_jobs._dedupe_ptg_jobs


def test_source_download_split_keeps_facade_helpers_stable():
    assert process_ptg._format_eta_seconds is ptg_source_download._format_eta_seconds
    assert process_ptg._emit_download_progress is ptg_source_download._emit_download_progress
    assert process_ptg.fetch_head_metadata is ptg_source_download.fetch_head_metadata
    assert process_ptg._probe_http_range_support is ptg_source_download._probe_http_range_support
    assert process_ptg._download_raw_artifact_ranges is ptg_source_download._download_raw_artifact_ranges
    assert process_ptg._download_ptg_job_artifact is ptg_source_download._download_ptg_job_artifact
    assert process_ptg._download_ptg_job_artifact_sync is ptg_source_download._download_ptg_job_artifact_sync
    assert process_ptg._iter_downloaded_ptg_jobs is ptg_source_download._iter_downloaded_ptg_jobs
    assert process_ptg.download_raw_artifact is ptg_source_download.download_raw_artifact
    assert process_ptg.materialize_json_source is ptg_source_download.materialize_json_source


def test_source_download_progress_scales_to_overall_run_progress(monkeypatch):
    events = []
    monkeypatch.setattr(ptg_source_download, "write_live_progress", lambda **payload: events.append(payload))

    token = ptg_live_progress.set_live_progress_context(
        run_id="run_ptg",
        overall_progress_start_pct=5,
        overall_progress_end_pct=20,
    )
    try:
        ptg_source_download._emit_download_progress(
            url="https://example.com/rates.json.gz",
            bytes_read=50,
            total_bytes=100,
            started_at=time.monotonic() - 1,
            done=False,
        )
    finally:
        ptg_live_progress.reset_live_progress_context(token)

    assert events
    assert events[0]["pct"] == 12.5
    assert events[0]["phase_pct"] == 50.0
    assert events[0]["detail"].startswith("download 50.00%")


def test_download_worker_propagates_live_progress_context(monkeypatch):
    captured = {}

    def fake_downloader(job, **_kwargs):
        captured.update(ptg_live_progress.current_live_progress_context())
        return ptg_domain.PTG2DownloadedJob(job=dict(job), error="stubbed")

    monkeypatch.setattr(process_ptg, "_download_ptg_job_artifact_sync", fake_downloader)

    result = ptg_source_download._download_ptg_job_artifact_sync_from_facade(
        {"type": "in_network", "url": "https://example.com/rates.json.gz"},
        reuse_raw_artifacts=True,
        max_bytes=None,
        keep_partial_artifacts=True,
        live_progress_context={
            "run_id": "run_ptg",
            "overall_progress_start_pct": 5,
            "overall_progress_end_pct": 20,
        },
    )

    assert result.error == "stubbed"
    assert captured["run_id"] == "run_ptg"
    assert captured["overall_progress_start_pct"] == 5


def test_source_download_tls_override_is_host_scoped(monkeypatch):
    monkeypatch.delenv(ptg_source_download.INCOMPLETE_TLS_CHAIN_HOSTS_ENV, raising=False)

    assert ptg_source_download._request_ssl_kwargs(
        "https://api.midlandschoice.com/api/v1/fileshare/download?filename=synthetic.json.gz"
    ) == {"ssl": False}
    assert (
        ptg_source_download._request_ssl_kwargs(
            "https://example.com/api/v1/fileshare/download?filename=synthetic.json.gz"
        )
        == {}
    )

    monkeypatch.setenv(ptg_source_download.INCOMPLETE_TLS_CHAIN_HOSTS_ENV, "example.com")
    assert ptg_source_download._request_ssl_kwargs("https://api.midlandschoice.com/mrf") == {}
    assert ptg_source_download._request_ssl_kwargs("https://example.com/mrf") == {
        "ssl": False
    }


def test_download_ptg_job_artifact_keeps_zip_logical_path_after_prefetch(tmp_path, monkeypatch):
    artifact_root = tmp_path / "artifacts"
    monkeypatch.setenv("HLTHPRT_PTG2_ARTIFACT_DIR", str(artifact_root))

    raw_zip = tmp_path / "rates.zip"
    with zipfile.ZipFile(raw_zip, "w") as archive:
        archive.writestr("rates.json", json.dumps({"in_network": []}))
    raw_sha256, raw_size = ptg_artifacts.sha256_file(raw_zip)

    async def fake_download_raw_artifact(*_args, store=None, **_kwargs):
        store = store or ptg_artifacts.PTG2ArtifactStore()
        return ptg_domain.PTG2RawArtifact(
            original_url="https://example.com/rates.zip",
            canonical_url="https://example.com/rates.zip",
            raw_path=str(raw_zip),
            raw_storage_uri=store.storage_uri(raw_zip),
            raw_sha256=raw_sha256,
            byte_count=raw_size,
        )

    monkeypatch.setattr(ptg_source_download, "download_raw_artifact", fake_download_raw_artifact)

    downloaded = asyncio.run(
        ptg_source_download._download_ptg_job_artifact(
            {"type": "in_network", "url": "https://example.com/rates.zip"},
            reuse_raw_artifacts=True,
            max_bytes=None,
            keep_partial_artifacts=True,
        )
    )

    assert downloaded.error is None
    assert downloaded.logical_artifact is not None
    logical_path = Path(downloaded.logical_artifact.logical_path)
    assert logical_path.exists()
    assert artifact_root in logical_path.parents
    assert json.loads(logical_path.read_text()) == {"in_network": []}


def test_source_file_split_keeps_facade_helpers_stable():
    assert process_ptg._maybe_unzip is ptg_source_files._maybe_unzip
    assert process_ptg._extract_metadata_fields is ptg_source_files._extract_metadata_fields
    assert process_ptg._derive_plan_fields is ptg_source_files._derive_plan_fields
    assert process_ptg._build_file_row is ptg_source_files._build_file_row


def test_source_version_split_keeps_facade_helpers_stable():
    assert process_ptg._record_source_version is ptg_source_versions._record_source_version


def test_provider_reference_split_keeps_facade_helpers_stable():
    assert process_ptg._load_provider_references_from_file is ptg_provider_references._load_provider_references_from_file
    assert process_ptg._process_provider_reference_file is ptg_provider_references._process_provider_reference_file


def test_table_setup_split_keeps_facade_helpers_stable():
    assert process_ptg.PTG2_MODEL_CLASSES is ptg_table_setup.PTG2_MODEL_CLASSES
    assert process_ptg._ensure_indexes is ptg_table_setup._ensure_indexes
    assert process_ptg._ensure_ptg2_serving_rate_columns is ptg_table_setup._ensure_ptg2_serving_rate_columns
    assert process_ptg._ensure_ptg2_provider_set_columns is ptg_table_setup._ensure_ptg2_provider_set_columns
    assert process_ptg._ensure_ptg2_price_set_columns is ptg_table_setup._ensure_ptg2_price_set_columns
    assert process_ptg._ensure_ptg2_price_atom_columns is ptg_table_setup._ensure_ptg2_price_atom_columns
    assert process_ptg._drop_ptg2_columns is ptg_table_setup._drop_ptg2_columns
    assert process_ptg._ensure_ptg2_price_set_stage_table is ptg_table_setup._ensure_ptg2_price_set_stage_table
    assert process_ptg._ensure_ptg2_serving_rate_stage_table is ptg_table_setup._ensure_ptg2_serving_rate_stage_table
    assert process_ptg.ensure_ptg2_tables is ptg_table_setup.ensure_ptg2_tables
    assert process_ptg._prepare_ptg_tables is ptg_table_setup._prepare_ptg_tables


def test_snapshot_cleanup_split_keeps_facade_helpers_stable():
    assert process_ptg._snapshot_manifest_table_names is ptg_snapshot_cleanup._snapshot_manifest_table_names
    assert process_ptg._drop_ptg2_snapshot_table_names is ptg_snapshot_cleanup._drop_ptg2_snapshot_table_names
    assert process_ptg._drop_ptg2_snapshot_tables_for_manifest is ptg_snapshot_cleanup._drop_ptg2_snapshot_tables_for_manifest
    assert process_ptg._cleanup_old_ptg2_source_tables is ptg_snapshot_cleanup._cleanup_old_ptg2_source_tables


def test_snapshot_cleanup_collects_tables_for_any_storage():
    tables = ptg_snapshot_cleanup._snapshot_manifest_table_names(
        {
            "storage": "future_snapshot_layout",
            "table": "mrf.ptg2_serving_future",
            "serving_binary_table": "mrf.ptg2_serving_binary_future",
            "price_atom_table": "ptg2_price_atom_future",
            "provider_set_table": "mrf.plan",
            "provider_group_member_table": "ptg2_provider_group_member_future",
            "provider_npi_scope_table": "mrf.ptg2_provider_npi_scope_future",
            "materialized_tables": {
                "serving_binary": "mrf.ptg2_serving_binary_future",
                "provider_set_dictionary": "mrf.ptg2_provider_set_dict_future",
            },
        }
    )

    assert tables == [
        "ptg2_serving_future",
        "ptg2_serving_binary_future",
        "ptg2_price_atom_future",
        "ptg2_provider_group_member_future",
        "ptg2_provider_npi_scope_future",
        "ptg2_provider_set_dict_future",
    ]


def _old_snapshot_cleanup_rows():
    """Return mixed storage rows used to prove source cleanup is storage-agnostic."""
    return [
        {
            "snapshot_id": "snap-current",
            "manifest": {
                "serving_index": {
                    "storage": "manifest_snapshot",
                    "source_key": "ptg_source_alpha",
                    "table": "ptg2_serving_keep",
                }
            },
        },
        {
            "snapshot_id": "snap-compact-old",
            "manifest": {
                "serving_index": {
                    "storage": "db_compact_snapshot",
                    "source_key": "ptg_source_alpha",
                    "table": "ptg2_serving_compact_old",
                }
            },
        },
        {
            "snapshot_id": "snap-manifest-old",
            "manifest": {
                "serving_index": {
                    "storage": "manifest_snapshot",
                    "source_key": "ptg_source_alpha",
                    "price_atom_table": "ptg2_price_atom_manifest_old",
                    "provider_set_table": "mrf.ptg2_provider_set_manifest_old",
                }
            },
        },
        {
            "snapshot_id": "snap-future-old",
            "manifest": {
                "serving_index": {
                    "storage": "future_snapshot_layout",
                    "source_key": "ptg_source_alpha",
                    "provider_group_member_table": "ptg2_provider_group_member_future_old",
                    "provider_group_location_table": "mrf.unrelated_table",
                }
            },
        },
    ]


def test_cleanup_old_ptg2_source_tables_scans_all_storage_generations(monkeypatch):
    queries = []
    dropped_tables = []

    async def fake_all(statement, **params):
        queries.append((statement, params))
        assert params == {}
        return _old_snapshot_cleanup_rows()

    async def fake_status(statement, **_params):
        dropped_tables.append(statement)
        return "DROP TABLE"

    monkeypatch.setattr(ptg_snapshot_cleanup.db, "all", fake_all)
    monkeypatch.setattr(ptg_snapshot_cleanup.db, "status", fake_status)

    asyncio.run(
        ptg_snapshot_cleanup._cleanup_old_ptg2_source_tables(
            "ptg_source_alpha",
            {"snap-current"},
        )
    )

    cleanup_query = queries[0][0]
    assert "FROM \"mrf\".ptg2_snapshot" in cleanup_query
    assert "source_key" not in cleanup_query
    assert "db_compact_snapshot" not in cleanup_query
    assert "manifest_snapshot" not in cleanup_query
    assert "\n".join(dropped_tables) == "\n".join(
        [
            'DROP TABLE IF EXISTS "mrf"."ptg2_serving_compact_old";',
            'DROP TABLE IF EXISTS "mrf"."ptg2_price_atom_manifest_old";',
            'DROP TABLE IF EXISTS "mrf"."ptg2_provider_set_manifest_old";',
            'DROP TABLE IF EXISTS "mrf"."ptg2_provider_group_member_future_old";',
        ]
    )


def test_cleanup_old_ptg2_source_tables_preserves_cross_source_shared_table(monkeypatch):
    dropped_tables = []
    snapshot_rows = [
        {
            "snapshot_id": "source-a-current",
            "manifest": {
                "serving_index": {
                    "source_key": "source_a",
                    "table": "ptg2_serving_source_a_current",
                }
            },
        },
        {
            "snapshot_id": "source-a-old",
            "manifest": {
                "serving_index": {
                    "source_key": "source_a",
                    "table": "ptg2_serving_shared",
                    "price_atom_table": "ptg2_price_atom_source_a_old",
                }
            },
        },
        {
            "snapshot_id": "source-b-current",
            "manifest": {
                "serving_index": {
                    "source_key": "source_b",
                    "table": "ptg2_serving_shared",
                }
            },
        },
    ]

    async def fake_all(_statement, **params):
        assert params == {}
        return snapshot_rows

    async def fake_status(statement, **_params):
        dropped_tables.append(statement)
        return "DROP TABLE"

    monkeypatch.setattr(ptg_snapshot_cleanup.db, "all", fake_all)
    monkeypatch.setattr(ptg_snapshot_cleanup.db, "status", fake_status)

    asyncio.run(
        ptg_snapshot_cleanup._cleanup_old_ptg2_source_tables(
            "source_a",
            {"source-a-current"},
        )
    )

    assert dropped_tables == [
        'DROP TABLE IF EXISTS "mrf"."ptg2_price_atom_source_a_old";'
    ]


def test_source_snapshot_cleanup_retains_four_snapshot_lineage(monkeypatch):
    monkeypatch.delenv("HLTHPRT_PTG2_SOURCE_SNAPSHOT_RETAIN_LINEAGE", raising=False)
    rows = [
        {"snapshot_id": "snap-current", "previous_snapshot_id": "snap-previous"},
        {"snapshot_id": "snap-previous", "previous_snapshot_id": "snap-third"},
        {"snapshot_id": "snap-third", "previous_snapshot_id": "snap-fourth"},
        {"snapshot_id": "snap-fourth", "previous_snapshot_id": "snap-fifth"},
        {"snapshot_id": "snap-fifth", "previous_snapshot_id": None},
    ]

    keep_snapshot_ids = ptg_snapshot_cleanup._source_snapshot_keep_ids(rows, {"snap-current"})

    assert keep_snapshot_ids == {"snap-current", "snap-previous", "snap-third", "snap-fourth"}


def test_snapshot_artifact_split_keeps_facade_helpers_stable():
    assert process_ptg._row_mapping is ptg_snapshot_artifacts._row_mapping
    assert process_ptg.build_ptg2_snapshot_index_artifact is ptg_snapshot_artifacts.build_ptg2_snapshot_index_artifact
    assert (
        process_ptg.build_ptg2_compact_snapshot_index_artifact
        is ptg_snapshot_artifacts.build_ptg2_compact_snapshot_index_artifact
    )


def test_rust_publish_split_keeps_facade_helpers_stable():
    assert process_ptg._ptg2_publish_timestamp is ptg_rust_publish._ptg2_publish_timestamp
    assert process_ptg._publish_renamed_rust_dictionary_table is ptg_rust_publish._publish_renamed_rust_dictionary_table
    assert process_ptg._ptg2_serving_child_table_name is ptg_rust_publish._ptg2_serving_child_table_name
    assert process_ptg._publish_rust_serving_stage_tables is ptg_rust_publish._publish_rust_serving_stage_tables
    assert process_ptg._publish_rust_compact_snapshot_tables is ptg_rust_publish._publish_rust_compact_snapshot_tables


def test_rust_publish_skips_optional_provider_geo_index_when_postgis_is_missing(monkeypatch):
    status_calls = []

    async def fake_status(statement, **_params):
        status_calls.append(statement)
        if "st_makepoint" in statement:
            raise RuntimeError("function st_makepoint(double precision, double precision) does not exist")

    monkeypatch.setattr(ptg_rust_publish, "db", SimpleNamespace(status=fake_status))

    asyncio.run(
        ptg_rust_publish._create_optional_provider_geo_index(
            schema_name="mrf",
            provider_group_location_table="ptg2_provider_group_location_abc",
        )
    )

    assert status_calls
    assert any("geo_gist_idx" in statement for statement in status_calls)


def test_rust_publish_provider_group_location_table_flag(monkeypatch):
    monkeypatch.delenv(ptg_rust_publish.PTG2_PROVIDER_GROUP_LOCATION_TABLE_ENV, raising=False)
    assert ptg_rust_publish._provider_group_location_enabled() is True

    monkeypatch.setenv(ptg_rust_publish.PTG2_PROVIDER_GROUP_LOCATION_TABLE_ENV, "false")
    assert ptg_rust_publish._provider_group_location_enabled() is False

    monkeypatch.setenv(ptg_rust_publish.PTG2_PROVIDER_GROUP_LOCATION_TABLE_ENV, "true")
    assert ptg_rust_publish._provider_group_location_enabled() is True


def test_manifest_provider_group_location_indexes_default_to_lean_profile(monkeypatch):
    status_calls = []

    async def fake_status(statement, **_params):
        status_calls.append(statement)

    async def fake_all(statement, **_params):
        raise AssertionError("lean provider-location indexes should not query taxonomy rules")

    monkeypatch.delenv(
        ptg_manifest_publish.PTG2_MANIFEST_PROVIDER_GROUP_LOCATION_INDEX_PROFILE_ENV,
        raising=False,
    )
    monkeypatch.setattr(ptg_manifest_publish.db, "status", fake_status)
    monkeypatch.setattr(ptg_manifest_publish.db, "all", fake_all)

    asyncio.run(
        ptg_manifest_publish._index_provider_group_locations(
            schema_name="mrf",
            provider_group_location_table="ptg2_provider_group_location_snap",
        )
    )

    joined = "\n".join(status_calls)
    assert "zip_group_idx" in joined
    assert "(state_name, city_name, npi, provider_group_global_id_128)" in joined
    assert "(npi, provider_group_global_id_128)" in joined
    assert "USING gin (taxonomy_array gin__int_ops)" in joined
    assert "zip_type_cover_idx" not in joined
    assert "geo_gist_idx" not in joined
    assert "zip_taxonomy_rule_" not in joined


def test_manifest_provider_group_location_indexes_full_profile_includes_zip_covering_index(monkeypatch):
    status_calls = []

    async def fake_status(statement, **_params):
        status_calls.append(statement)

    async def fake_all(statement, **_params):
        assert "nucc_taxonomy" in statement
        return [{"int_code": 101}, {"int_code": 202}]

    monkeypatch.setenv(
        ptg_manifest_publish.PTG2_MANIFEST_PROVIDER_GROUP_LOCATION_INDEX_PROFILE_ENV,
        "full",
    )
    monkeypatch.setattr(ptg_manifest_publish.db, "status", fake_status)
    monkeypatch.setattr(ptg_manifest_publish.db, "all", fake_all)

    asyncio.run(
        ptg_manifest_publish._index_provider_group_locations(
            schema_name="mrf",
            provider_group_location_table="ptg2_provider_group_location_snap",
        )
    )

    joined = "\n".join(status_calls)
    assert "zip_type_cover_idx" in joined
    assert "(zip5, address_type, provider_group_global_id_128, npi, address_checksum)" in joined
    assert "INCLUDE (taxonomy_array) WHERE npi IS NOT NULL" in joined
    assert "zip_taxonomy_rule_" in joined
    assert "WHERE npi IS NOT NULL AND taxonomy_array && ARRAY[101,202]::integer[]" in joined


def test_ptg2_manifest_cleanup_plan_selects_legacy_snapshot_tables(monkeypatch):
    async def fake_all(statement, **_params):
        if "FROM pg_class" in statement:
            return [
                {"table_name": "ptg2_serving_abc123def4567890"},
                {"table_name": "ptg2_serving_binary_abc123def4567890"},
                {"table_name": "ptg2_serving_rate_compact_abc123def4567890"},
                {"table_name": "ptg2_price_set_atom_abc123def4567890"},
                {"table_name": "ptg2_provider_set_component_abc123def4567890"},
                {"table_name": "ptg2_manifest_serving_abc123def4567890"},
                {"table_name": "ptg2_plan"},
            ]
        if "FROM \"mrf\".ptg2_snapshot" in statement:
            return [{"snapshot_id": "legacy-snap"}]
        if "FROM \"mrf\".ptg2_artifact_manifest" in statement:
            return [{"artifact_id": "artifact-1"}]
        if "FROM \"mrf\".ptg2_current_snapshot" in statement:
            return [{"slot": "current"}]
        if "FROM \"mrf\".ptg2_current_source_snapshot" in statement:
            return [{"source_key": "legacy-source"}]
        if "FROM \"mrf\".ptg2_current_plan_source" in statement:
            return [{"plan_source_key": "legacy-plan"}]
        return []

    monkeypatch.setattr(ptg_manifest_cleanup.db, "all", fake_all)

    plan = asyncio.run(ptg_manifest_cleanup.build_ptg2_manifest_cleanup_plan(schema_name="mrf"))

    assert plan.tables == (
        "ptg2_serving_abc123def4567890",
        "ptg2_serving_binary_abc123def4567890",
        "ptg2_serving_rate_compact_abc123def4567890",
        "ptg2_price_set_atom_abc123def4567890",
        "ptg2_provider_set_component_abc123def4567890",
    )
    assert plan.snapshot_ids == ("legacy-snap",)
    assert plan.artifact_manifest_ids == ("artifact-1",)
    assert plan.current_snapshot_slots == ("current",)
    assert plan.current_source_rows == ("legacy-source",)
    assert plan.current_plan_rows == ("legacy-plan",)


def test_ptg2_manifest_cleanup_keeps_all_materialized_snapshot_tables():
    referenced_tables = ptg_manifest_cleanup._manifest_referenced_tables(
        {
            "serving_index": {
                "table": "mrf.ptg2_serving_abc123def4567890",
                "serving_binary_table": "mrf.ptg2_serving_binary_abc123def4567890",
                "provider_group_location_table": "mrf.ptg2_provider_group_location_abc123def4567890",
                "provider_set_component_table": "mrf.ptg2_provider_set_component_abc123def4567890",
                "materialized_tables": {
                    "code_count": "mrf.ptg2_code_count_abc123def4567890",
                    "price_atom_dictionary": "mrf.ptg2_price_atom_dict_abc123def4567890",
                },
            }
        }
    )

    assert referenced_tables == {
        "ptg2_serving_abc123def4567890",
        "ptg2_serving_binary_abc123def4567890",
        "ptg2_provider_group_location_abc123def4567890",
        "ptg2_provider_set_component_abc123def4567890",
        "ptg2_code_count_abc123def4567890",
        "ptg2_price_atom_dict_abc123def4567890",
    }


def test_rust_scanner_split_keeps_facade_helpers_stable():
    assert process_ptg._ptg2_rust_scanner_binary is ptg_rust_scanner._ptg2_rust_scanner_binary
    assert process_ptg._iter_top_level_object_bytes_rust is ptg_rust_scanner._iter_top_level_object_bytes_rust
    assert process_ptg._iter_compact_serving_records_rust is ptg_rust_scanner._iter_compact_serving_records_rust
    assert process_ptg._aiter_compact_serving_records_rust is ptg_rust_scanner._aiter_compact_serving_records_rust


def test_rust_scanner_frame_reader_retries_short_pipe_reads():
    class ShortReader(io.BytesIO):
        def read(self, size=-1):
            return super().read(min(size, 2))

    assert ptg_rust_scanner._read_exactly(ShortReader(b"abcdef"), 6) == b"abcdef"


def test_ptg2_rust_scanner_release_requirement_skips_debug_binary(monkeypatch, tmp_path):
    debug_binary = tmp_path / "support" / "ptg2_scanner" / "target" / "debug" / "ptg2_scanner"
    debug_binary.parent.mkdir(parents=True)
    debug_binary.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    debug_binary.chmod(0o755)

    monkeypatch.setenv(process_ptg.PTG2_RUST_SCANNER_BIN_ENV, str(debug_binary))
    monkeypatch.setenv(ptg_rust_scanner.PTG2_RUST_REQUIRE_RELEASE_ENV, "true")

    assert ptg_rust_scanner._ptg2_rust_scanner_binary() is None


def test_ptg2_rust_scanner_release_requirement_accepts_release_binary(monkeypatch, tmp_path):
    release_binary = tmp_path / "support" / "ptg2_scanner" / "target" / "release" / "ptg2_scanner"
    release_binary.parent.mkdir(parents=True)
    release_binary.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    release_binary.chmod(0o755)

    monkeypatch.setenv(process_ptg.PTG2_RUST_SCANNER_BIN_ENV, str(release_binary))
    monkeypatch.setenv(ptg_rust_scanner.PTG2_RUST_REQUIRE_RELEASE_ENV, "true")

    assert ptg_rust_scanner._ptg2_rust_scanner_binary() == release_binary


def test_async_rust_scanner_close_suppresses_generator_already_executing(monkeypatch, tmp_path):
    class BlockingIterator:
        def __init__(self):
            self.started = threading.Event()
            self.closed = threading.Event()

        def __iter__(self):
            return self

        def __next__(self):
            self.started.set()
            self.closed.wait(timeout=5)
            raise StopIteration

        def close(self):
            self.closed.set()
            raise ValueError("generator already executing")

    iterator = BlockingIterator()
    monkeypatch.setattr(
        ptg_rust_scanner,
        "_iter_compact_serving_records_rust",
        lambda *args, **kwargs: iterator,
    )

    async def run():
        records = ptg_rust_scanner._aiter_compact_serving_records_rust(
            tmp_path / "rates.json.gz",
            snapshot_id="snap",
            plan_id="plan",
            plan_month_id="month",
            source_trace_set_hash="trace",
        )
        task = asyncio.create_task(records.__anext__())
        assert await asyncio.to_thread(iterator.started.wait, 1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert iterator.closed.is_set()

    asyncio.run(run())


def test_rust_scanner_progress_line_updates_live_progress(monkeypatch):
    events = []

    monkeypatch.setattr(ptg_rust_scanner, "write_live_progress", lambda **payload: events.append(payload))

    ptg_rust_scanner._emit_scanner_live_progress(
        "PTG2_SCANNER_PROGRESS\t"
        "path=/work/raw/rates.json.gz\t"
        "compressed_bytes=1048576\t"
        "total_bytes=2097152\t"
        "percent=50.00\t"
        "compressed_mib_s=4.50\t"
        "elapsed_seconds=10\t"
        "eta_seconds=10\t"
        "objects=7\t"
        "provider_references=2\t"
        "in_network=5\t"
        "done=false",
        phase="compact-serving scanner",
        live_progress_context={"run_id": "run_ptg", "snapshot_id": "snap_1"},
    )

    assert events
    payload = events[0]
    assert payload["run_id"] == "run_ptg"
    assert payload["snapshot_id"] == "snap_1"
    assert payload["phase"] == "compact-serving scanner"
    assert payload["unit"] == "compressed_bytes"
    assert payload["done"] == 1048576
    assert payload["total"] == 2097152
    assert payload["pct"] == 50.0
    assert payload["eta_seconds"] == 10.0
    assert payload["source"] == "ptg2-scanner-progress"
    assert payload["scanner_objects"] == {"provider_references": 2, "in_network": 5}


def test_rust_scanner_progress_scales_to_overall_run_progress(monkeypatch):
    events = []

    monkeypatch.setattr(ptg_rust_scanner, "write_live_progress", lambda **payload: events.append(payload))

    ptg_rust_scanner._emit_scanner_live_progress(
        "PTG2_SCANNER_PROGRESS\t"
        "path=/work/raw/rates.json.gz\t"
        "compressed_bytes=1048576\t"
        "total_bytes=2097152\t"
        "percent=50.00\t"
        "compressed_mib_s=4.50\t"
        "elapsed_seconds=10\t"
        "eta_seconds=10\t"
        "objects=7\t"
        "done=false",
        phase="compact-serving scanner",
        live_progress_context={
            "run_id": "run_ptg",
            "overall_progress_start_pct": 20,
            "overall_progress_end_pct": 90,
        },
    )

    assert events
    assert events[0]["pct"] == 55.0
    assert events[0]["phase_pct"] == 50.0


def test_rust_scanner_progress_uses_object_throughput_when_bytes_are_deferred(monkeypatch):
    progress_events = []

    monkeypatch.setattr(
        ptg_rust_scanner,
        "write_live_progress",
        lambda **progress_payload: progress_events.append(progress_payload),
    )

    ptg_rust_scanner._emit_scanner_live_progress(
        "PTG2_SCANNER_PROGRESS\t"
        "path=/work/raw/rates.json.gz\t"
        "compressed_bytes=0\t"
        "total_bytes=14968051667\t"
        "percent=0.00\t"
        "compressed_mib_s=0.00\t"
        "elapsed_seconds=119\t"
        "eta_seconds=unknown\t"
        "objects=122007993\t"
        "negotiated_rates=121938948\t"
        "provider_references=63905\t"
        "in_network=5140\t"
        "done=false",
        phase="compact-serving scanner",
        live_progress_context={
            "run_id": "run_ptg",
            "overall_progress_start_pct": 20,
            "overall_progress_end_pct": 90,
        },
    )

    [progress_update] = progress_events
    assert progress_update["unit"] == "objects"
    assert progress_update["done"] == 122007993
    assert progress_update["total"] is None
    assert progress_update["pct"] == 20.0
    assert progress_update["phase_pct"] is None
    assert progress_update["eta_seconds"] is None
    assert progress_update["scanner_progress_basis"] == "objects"
    assert progress_update["scanner_object_rate"] == pytest.approx(1025277.2521)
    assert "rate stream" in progress_update["message"]
    assert "rates=121,938,948" in progress_update["message"]
    assert "provider refs=63,905" in progress_update["message"]
    assert "code groups=5,140" in progress_update["message"]
    assert "throughput=1.03M objects/s" in progress_update["message"]
    assert "elapsed=1m 59s" in progress_update["message"]
    assert "input=14,274.6 MiB" in progress_update["message"]


def test_async_rust_scanner_passes_live_progress_context(monkeypatch, tmp_path):
    captured = {}

    def fake_iter(*_args, **kwargs):
        captured.update(kwargs)
        return iter(())

    monkeypatch.setattr(ptg_rust_scanner, "_iter_compact_serving_records_rust", fake_iter)

    async def run():
        token = ptg_live_progress.set_live_progress_context(run_id="run_ptg", snapshot_id="snap_1")
        try:
            async for _record in ptg_rust_scanner._aiter_compact_serving_records_rust(
                tmp_path / "rates.json.gz",
                snapshot_id="snap",
                plan_id="plan",
                plan_month_id="month",
                source_trace_set_hash="trace",
            ):
                captured["records_seen"] = captured.get("records_seen", 0) + 1
        finally:
            ptg_live_progress.reset_live_progress_context(token)

    asyncio.run(run())

    assert captured["live_progress_context"]["run_id"] == "run_ptg"
    assert captured["live_progress_context"]["snapshot_id"] == "snap_1"


def test_rust_stage_split_keeps_facade_helpers_stable():
    assert process_ptg.PTG2_SERVING_STAGE_LANE_PREFIX == ptg_rust_stage.PTG2_SERVING_STAGE_LANE_PREFIX
    assert process_ptg._RUST_COPY_TABLE_SPECS is ptg_rust_stage._RUST_COPY_TABLE_SPECS
    assert process_ptg._ptg2_dictionary_select_columns is ptg_rust_stage._ptg2_dictionary_select_columns
    assert process_ptg._rust_copy_stage_table_name is ptg_rust_stage._rust_copy_stage_table_name
    assert process_ptg._serving_stage_lane_key is ptg_rust_stage._serving_stage_lane_key
    assert process_ptg._serving_stage_tables is ptg_rust_stage._serving_stage_tables
    assert process_ptg._serving_stage_table_for_copy is ptg_rust_stage._serving_stage_table_for_copy
    assert process_ptg._create_rust_copy_stage_tables is ptg_rust_stage._create_rust_copy_stage_tables
    assert process_ptg._merge_rust_copy_stage_tables is ptg_rust_stage._merge_rust_copy_stage_tables


def test_json_stream_split_keeps_facade_helpers_stable():
    assert process_ptg._json_loads is ptg_json_streams._json_loads
    assert process_ptg._iter_top_level_objects is ptg_json_streams._iter_top_level_objects
    assert process_ptg._iter_top_level_object_bytes is ptg_json_streams._iter_top_level_object_bytes
    assert process_ptg._iter_top_level_objects_jsondecoder is ptg_json_streams._iter_top_level_objects_jsondecoder
    assert process_ptg._iter_top_level_objects_fast is ptg_json_streams._iter_top_level_objects_fast


def test_serving_only_split_keeps_facade_helpers_stable():
    assert process_ptg._serving_only_rows_for_payload is ptg_serving_only._serving_only_rows_for_payload
    assert process_ptg._serving_only_price_payload is ptg_serving_only._serving_only_price_payload
    assert process_ptg._normalize_serving_price_payload is ptg_serving_only._normalize_serving_price_payload
    assert process_ptg._serving_only_hash_int_sets is ptg_serving_only._serving_only_hash_int_sets
    assert process_ptg._serving_only_hash_price_key is ptg_serving_only._serving_only_hash_price_key
    assert process_ptg._serving_only_hash_text is ptg_serving_only._serving_only_hash_text
    assert process_ptg._serving_only_merge_worker_result is ptg_serving_only._serving_only_merge_worker_result
    assert process_ptg._worker_payload_size is ptg_serving_only._worker_payload_size
    assert process_ptg._iter_worker_result_rows is ptg_serving_only._iter_worker_result_rows
    assert process_ptg._ptg2_worker_capacity_wait_needed is ptg_serving_only._ptg2_worker_capacity_wait_needed


def test_serving_row_split_keeps_facade_helpers_stable():
    assert process_ptg._ptg2_hp_procedure_code is ptg_serving_rows._ptg2_hp_procedure_code
    assert process_ptg._ptg2_serving_rate_row is ptg_serving_rows._ptg2_serving_rate_row
    assert process_ptg._ptg2_compact_serving_rate_row is ptg_serving_rows._ptg2_compact_serving_rate_row
    assert process_ptg._provider_group_member_rows is ptg_serving_rows._provider_group_member_rows
    assert process_ptg._provider_set_component_rows is ptg_serving_rows._provider_set_component_rows


def test_serving_maintenance_split_keeps_facade_helpers_stable():
    assert process_ptg._count_compact_serving_rate_rows is ptg_serving_maintenance._count_compact_serving_rate_rows
    assert process_ptg._copy_simple_rows is ptg_serving_maintenance._copy_simple_rows
    assert process_ptg._merge_staged_price_sets is ptg_serving_maintenance._merge_staged_price_sets
    assert process_ptg._merge_staged_serving_rates is ptg_serving_maintenance._merge_staged_serving_rates
    assert process_ptg._build_ptg2_provider_locations is ptg_serving_maintenance._build_ptg2_provider_locations


def test_serving_index_split_keeps_facade_helpers_stable():
    assert process_ptg._ptg2_table_available is ptg_serving_index._ptg2_table_available
    assert process_ptg.build_ptg2_db_serving_index is ptg_serving_index.build_ptg2_db_serving_index
    assert process_ptg.finalize_ptg2_incremental_serving_index is ptg_serving_index.finalize_ptg2_incremental_serving_index
    assert process_ptg.build_ptg2_stage_serving_index is ptg_serving_index.build_ptg2_stage_serving_index
    assert process_ptg.build_ptg2_compact_serving_index is ptg_serving_index.build_ptg2_compact_serving_index


def test_ptg2_source_trace_payload_keeps_source_file_version_id():
    payload = process_ptg._ptg2_source_trace_payload(
        {
            "source_file_version_id": "source-version-1",
            "original_url": "https://example.test/rates.json.gz",
            "canonical_url": "https://example.test/rates.json.gz",
        }
    )

    assert payload == [
        {
            "source_file_version_id": "source-version-1",
            "url": "https://example.test/rates.json.gz",
            "canonical_url": "https://example.test/rates.json.gz",
            "statement": "Published negotiated rate from Transparency in Coverage source file.",
        }
    ]


def test_filter_reporting_plans_matches_group_plan_id():
    plans = [
        {
            "plan_name": "Example Individual",
            "plan_id": "81974",
            "plan_market_type": "individual",
        },
        {
            "plan_name": "Example Employer Group Plan",
            "plan_id": "TESTPLAN001",
            "plan_sponsor_name": "Example Employer",
            "issuer_name": "WPS",
            "plan_market_type": "group",
        },
    ]

    result = process_ptg._filter_reporting_plans(
        plans,
        plan_ids=["TESTPLAN001"],
        plan_market_types=["group"],
    )

    assert result == [plans[1]]


def test_ptg2_filter_jobs_by_url_contains_keeps_matching_rate_file():
    jobs = [
        {"type": "in_network", "url": "https://example.test/CMC_CRS_MRRF_in-network-rates.json.gz"},
        {"type": "in_network", "url": "https://example.test/PS1-50_C2_in-network-rates.json.gz"},
    ]

    result = process_ptg._filter_jobs_by_url_contains(jobs, ["ps1-50_c2"])

    assert result == [jobs[1]]


def test_ptg2_dedupe_jobs_uses_canonical_url_and_merges_plans():
    jobs = [
        {
            "type": "in_network",
            "url": "HTTPS://Example.COM:443/path/rates.json.gz?sig=one&b=2&a=1",
            "plan_info": [{"plan_id": "A"}],
            "description": "first",
        },
        {
            "type": "in_network",
            "url": "https://example.com/path/rates.json.gz?a=1&b=2&Signature=two",
            "plan_info": [{"plan_id": "B"}],
            "description": "second",
        },
        {
            "type": "allowed_amounts",
            "url": "https://example.com/path/rates.json.gz?a=1&b=2&Signature=two",
            "plan_info": [{"plan_id": "C"}],
        },
    ]

    deduped, duplicate_count = process_ptg._dedupe_ptg_jobs(jobs)

    assert duplicate_count == 1
    assert len(deduped) == 2
    assert deduped[0]["url"] == jobs[0]["url"]
    assert deduped[0]["plan_info"] == [{"plan_id": "A"}, {"plan_id": "B"}]
    assert deduped[1]["type"] == "allowed_amounts"


def test_ptg2_stage_copy_dedupe_defaults_avoid_huge_edge_tables(monkeypatch):
    monkeypatch.delenv("HLTHPRT_PTG2_STAGE_COPY_DEDUPE", raising=False)

    assert process_ptg._ptg2_stage_copy_dedupe_enabled("price_code_set") is True
    assert process_ptg._ptg2_stage_copy_dedupe_enabled("provider_set") is True
    assert process_ptg._ptg2_stage_copy_dedupe_enabled("provider_group_member") is True
    assert process_ptg._ptg2_stage_copy_dedupe_enabled("price_set_entry") is True
    assert process_ptg._ptg2_stage_copy_dedupe_enabled("provider_set_component") is True
    assert process_ptg._ptg2_stage_copy_dedupe_enabled("provider_entry_component") is False

    monkeypatch.setenv("HLTHPRT_PTG2_STAGE_COPY_DEDUPE", "1")
    assert process_ptg._ptg2_stage_copy_dedupe_enabled("provider_entry_component") is True


def test_ptg2_compact_serving_index_defaults_to_reported_lookup(monkeypatch):
    monkeypatch.delenv("HLTHPRT_PTG2_COMPACT_SERVING_INDEX_MODE", raising=False)

    statements = process_ptg._ptg2_model_index_statements_for_table(
        process_ptg.PTG2ServingRateCompact,
        "mrf",
        "ptg2_serving_rate_compact_abc_p00",
    )

    assert len(statements) == 2
    statements_by_role = dict(statements)
    assert "reported_system_order_idx" in statements_by_role
    assert (
        "(snapshot_id, plan_id, reported_code_system, reported_code, provider_count DESC, serving_rate_id)"
        in statements_by_role["reported_system_order_idx"]
    )
    assert "procedure_order_idx" in statements_by_role
    assert (
        "(snapshot_id, plan_id, procedure_code, provider_count DESC, serving_rate_id)"
        in statements_by_role["procedure_order_idx"]
    )
    assert "WHERE procedure_code IS NOT NULL" in statements_by_role["procedure_order_idx"]


def test_ptg2_rust_compact_stage_preserves_serving_parent_shape_for_inheritance(monkeypatch):
    status_calls = []

    async def fake_status(statement, **_params):
        status_calls.append(statement)

    async def fake_all(_statement, **_params):
        return [
            {"column_name": "serving_rate_id"},
            {"column_name": "snapshot_id"},
            {"column_name": "plan_month_id"},
            {"column_name": "billing_code"},
            {"column_name": "billing_code_type"},
        ]

    monkeypatch.setattr(ptg_rust_stage.db, "status", fake_status)
    monkeypatch.setattr(ptg_rust_stage.db, "all", fake_all)

    asyncio.run(
        ptg_rust_stage._create_one_rust_copy_stage_table(
            kind="serving_rate_compact",
            schema_name="mrf",
            storage_mode="UNLOGGED ",
            stage_table="ptg2_rust_stage_serving_rate_compact_test",
            target_table="ptg2_serving_rate_compact",
            columns=["serving_rate_id", "snapshot_id"],
            conflict_targets=None,
        )
    )

    joined = "\n".join(status_calls)
    assert 'DROP COLUMN IF EXISTS "plan_month_id"' not in joined
    assert 'DROP COLUMN IF EXISTS "billing_code"' not in joined
    assert 'DROP COLUMN IF EXISTS "billing_code_type"' not in joined


def test_ptg2_compact_serving_index_full_mode_keeps_model_indexes(monkeypatch):
    monkeypatch.setenv("HLTHPRT_PTG2_COMPACT_SERVING_INDEX_MODE", "full")

    roles = [
        role
        for role, _ in process_ptg._ptg2_model_index_statements_for_table(
            process_ptg.PTG2ServingRateCompact,
            "mrf",
            "ptg2_serving_rate_compact_abc_p00",
        )
    ]

    assert "idx_primary" in roles
    assert "reported_order_idx" in roles


def test_ptg2_compact_dictionary_index_defaults_to_serving_indexes(monkeypatch):
    monkeypatch.delenv("HLTHPRT_PTG2_COMPACT_DICTIONARY_INDEX_MODE", raising=False)

    roles = [
        role
        for role, _ in process_ptg._ptg2_model_index_statements_for_table(
            process_ptg.PTG2PriceSetEntry,
            "mrf",
            "ptg2_price_set_entry_abc",
        )
    ]

    assert roles == ["idx_primary"]


def test_ptg2_compact_dictionary_index_full_mode_keeps_reverse_indexes(monkeypatch):
    monkeypatch.setenv("HLTHPRT_PTG2_COMPACT_DICTIONARY_INDEX_MODE", "full")

    roles = [
        role
        for role, _ in process_ptg._ptg2_model_index_statements_for_table(
            process_ptg.PTG2PriceSetEntry,
            "mrf",
            "ptg2_price_set_entry_abc",
        )
    ]

    assert "idx_primary" in roles
    assert "atom_idx" in roles


def test_ptg2_fast_object_iterator_yields_selected_top_level_arrays():
    payload = (
        b'{"version":"1.0","provider_references":[{"provider_group_id":7,'
        b'"provider_groups":[{"npi":[1],"note":"brace } in string"}]}],'
        b'"in_network":[{"negotiation_arrangement":"ffs","billing_code":"001",'
        b'"negotiated_rates":[{"negotiated_prices":[{"negotiated_rate":1.2}]}]}]}'
    )

    result = list(
        process_ptg._iter_top_level_objects_fast(
            io.BytesIO(payload),
            {
                "provider_reference": "provider_references.item",
                "in_network": "in_network.item",
            },
        )
    )

    assert [name for name, _ in result] == ["provider_reference", "in_network"]
    assert result[0][1]["provider_group_id"] == 7
    assert result[1][1]["billing_code"] == "001"


def test_filter_reporting_plans_matches_name_contains_case_insensitive():
    plans = [
        {
            "plan_name": "Example Employer Group Plan",
            "plan_id": "TESTPLAN002",
            "plan_market_type": "group",
        },
    ]

    result = process_ptg._filter_reporting_plans(
        plans,
        plan_name_contains=["example employer"],
        plan_market_types=["GROUP"],
    )

    assert result == plans


def test_filter_reporting_plans_name_contains_ignores_issuer_and_reporting_entity():
    plans = [
        {
            "plan_name": "Small Business PPO",
            "plan_id": "59-1031071",
            "issuer_name": "Cigna Health Life Insurance Company",
            "reporting_entity_name": "Cigna",
            "plan_market_type": "group",
        },
        {
            "plan_name": "Cigna MVP Health Care, Inc",
            "plan_id": "123",
            "plan_market_type": "group",
        },
    ]

    result = process_ptg._filter_reporting_plans(
        plans,
        plan_name_contains=["cigna"],
        plan_market_types=["group"],
    )

    assert result == [plans[1]]


def test_filter_reporting_plans_returns_original_without_filters():
    plans = [{"plan_name": "Any Plan", "plan_id": "1", "plan_market_type": "group"}]

    assert process_ptg._filter_reporting_plans(plans) == plans


def test_as_int_list_normalizes_npi_strings():
    assert process_ptg._as_int_list(["1053488122", 1093228306, "", None, "bad"]) == [
        1053488122,
        1093228306,
    ]


def test_normalized_npi_list_keeps_only_ten_digit_npis():
    assert process_ptg._normalized_npi_list(
        ["114911247", "1053488122", 1093228306, "", None, "bad", "10000000000"]
    ) == [
        1053488122,
        1093228306,
    ]


def test_provider_group_member_rows_skip_malformed_npis():
    rows = process_ptg._provider_group_member_rows(
        {"__hash__": "123", "npi": ["114911247", "1053488122", 1093228306]}
    )

    assert rows == [
        {"provider_group_hash": 123, "npi": 1053488122},
        {"provider_group_hash": 123, "npi": 1093228306},
    ]


def test_ptg2_semantic_hash_ignores_set_like_array_order():
    first = {
        "npi": [1093228306, 1053488122],
        "service_code": ["02", "01"],
        "billing_code_modifier": ["TC", "26"],
        "bundled_codes": [
            {"billing_code": "B", "billing_code_type": "HCPCS"},
            {"billing_code": "A", "billing_code_type": "CPT"},
        ],
        "negotiated_rate": Decimal("12.3400"),
    }
    second = {
        "npi": [1053488122, 1093228306],
        "service_code": ["01", "02"],
        "billing_code_modifier": ["26", "TC"],
        "bundled_codes": [
            {"billing_code_type": "CPT", "billing_code": "A"},
            {"billing_code_type": "HCPCS", "billing_code": "B"},
        ],
        "negotiated_rate": "12.34",
    }

    assert process_ptg.semantic_hash(first, domain="rate") == process_ptg.semantic_hash(second, domain="rate")


@pytest.mark.parametrize(
    "changed",
    [
        {"negotiated_rate": Decimal("12.35")},
        {"billing_code_modifier": ["26", "GT"]},
        {"setting": "inpatient"},
        {"context": {"plan_id": "different"}},
    ],
)
def test_ptg2_semantic_hash_changes_for_rate_context_modifier_and_setting(changed):
    base = {
        "negotiated_rate": Decimal("12.34"),
        "billing_code_modifier": ["26", "TC"],
        "setting": "outpatient",
        "context": {"plan_id": "TESTPLAN001"},
    }
    modified = dict(base)
    modified.update(changed)

    assert process_ptg.semantic_hash(base, domain="rate") != process_ptg.semantic_hash(modified, domain="rate")


def test_ptg2_semantic_hash_defaults_to_short_checksum_mode(monkeypatch):
    monkeypatch.delenv(process_ptg.PTG2_HASH_MODE_ENV, raising=False)

    value = {"a": 1, "b": [3, 2, 1]}
    result = process_ptg.semantic_hash(value, domain="x")

    assert len(result) == 16
    assert int(result, 16) >= 0


def test_ptg2_semantic_hash_can_switch_back_to_sha256(monkeypatch):
    monkeypatch.setenv(process_ptg.PTG2_HASH_MODE_ENV, "sha256")

    value = {"a": 1, "b": [3, 2, 1]}
    result = process_ptg.semantic_hash(value, domain="x")

    assert len(result) == 64
    assert int(result, 16) >= 0


def test_ptg2_rejects_float_money_values():
    with pytest.raises(TypeError):
        process_ptg.semantic_hash({"negotiated_rate": 12.34}, domain="rate")


def test_ptg2_modes_and_confidence_wording_are_explicit():
    assert process_ptg.normalize_ptg2_search_mode(None) == "product_search"
    assert process_ptg.normalize_ptg2_search_mode("exact_source") == "exact_source"
    with pytest.raises(ValueError):
        process_ptg.normalize_ptg2_search_mode("loose")
    assert "Published negotiated rate" in process_ptg.ptg2_confidence_statement("tic_rate_npi_tin")


def test_ptg2_domain_split_keeps_facade_symbols_stable():
    assert process_ptg.PTG2PriceAtomEvent is ptg_domain.PTG2PriceAtomEvent
    assert process_ptg.PTG2RawArtifact is ptg_domain.PTG2RawArtifact
    assert process_ptg.normalize_ptg2_search_mode is ptg_domain.normalize_ptg2_search_mode
    assert process_ptg.ptg2_confidence_statement is ptg_domain.ptg2_confidence_statement


def test_ptg2_runtime_checksum_uses_bigint_hash_space():
    hashes = [process_ptg._make_checksum("rate", idx) for idx in range(5000)]

    assert len(set(hashes)) == len(hashes)
    assert all(0 <= value < 2**63 for value in hashes)
    assert max(hashes) > 2**32


def test_ptg2_provider_group_identity_is_source_independent_and_order_insensitive():
    tin_a = {"type": "EIN", "value": "12-3456789"}
    tin_b = {"type": "ein", "value": "123456789"}

    first = process_ptg._provider_group_identity_hash(tin_a, [1053488122, 114911247, 1093228306])
    second = process_ptg._provider_group_identity_hash(tin_b, [1093228306, 1053488122])

    assert first == second


def test_ptg2_provider_set_entry_packs_groups_order_insensitively():
    groups_a = [
        {"tin": {"type": "ein", "value": "111"}, "npi": [1000000003, 1000000001]},
        {"tin": {"type": "ein", "value": "222"}, "npi": [1000000002]},
    ]
    groups_b = list(reversed(groups_a))

    entry_a, row_a = process_ptg._build_provider_set_entry(
        file_id=1,
        provider_group_ref=10,
        provider_groups=groups_a,
        network_names=["A"],
    )
    entry_b, row_b = process_ptg._build_provider_set_entry(
        file_id=2,
        provider_group_ref=20,
        provider_groups=groups_b,
        network_names=["B"],
    )

    assert entry_a["__hash__"] == entry_b["__hash__"]
    assert row_a["provider_group_hash"] == row_b["provider_group_hash"]
    assert row_a["npi"] == [1000000001, 1000000002, 1000000003]
    assert row_a["tin_type"] == "set"


def test_ptg2_provider_set_entry_does_not_materialize_direct_location_fields():
    entry, row = process_ptg._build_provider_set_entry(
        file_id=1,
        provider_group_ref=10,
        provider_groups=[
            {
                "tin": {"type": "ein", "value": "111"},
                "npi": [1000000001],
                "address": {
                    "street": "900 W Temple Ave",
                    "city": "Effingham",
                    "state": "IL",
                    "postal_code": "62401",
                },
                "phone": "217-540-2350",
            }
        ],
        network_names=["C2"],
    )

    assert entry["network_name"] == ["C2"]
    assert row["network_names"] == ["C2"]
    assert "address" not in entry
    assert "phone" not in entry
    assert "address" not in row
    assert "phone" not in row


def test_ptg2_combined_provider_set_entry_packs_rate_provider_refs():
    first, _ = process_ptg._build_provider_set_entry(
        file_id=1,
        provider_group_ref=10,
        provider_groups=[{"tin": {"type": "ein", "value": "111"}, "npi": [1000000001, 1000000002]}],
        network_names=["A"],
    )
    second, _ = process_ptg._build_provider_set_entry(
        file_id=1,
        provider_group_ref=11,
        provider_groups=[{"tin": {"type": "ein", "value": "222"}, "npi": [1000000003]}],
        network_names=["A"],
    )

    combined_a, row_a = process_ptg._combine_provider_set_entries(file_id=1, entries=[first, second])
    combined_b, row_b = process_ptg._combine_provider_set_entries(file_id=2, entries=[second, first])

    assert combined_a["__hash__"] == combined_b["__hash__"]
    assert row_a["provider_group_hash"] == row_b["provider_group_hash"]
    assert row_a["npi"] == [1000000001, 1000000002, 1000000003]
    assert row_a["tin_type"] == "set"


def test_ptg2_provider_group_rows_are_canonical_and_source_independent():
    groups_a = [{"tin": {"type": "ein", "value": "12-3456789"}, "npi": [1000000003, 1000000001, 1000000002]}]
    groups_b = [{"tin": {"type": "EIN", "value": "123456789"}, "npi": [1000000002, 1000000003, 1000000001]}]

    row_a = process_ptg._ptg2_provider_group_rows(provider_groups=groups_a)[0]
    row_b = process_ptg._ptg2_provider_group_rows(provider_groups=groups_b)[0]

    assert row_a["provider_group_hash"] == row_b["provider_group_hash"]
    assert row_a["npi"] == [1000000001, 1000000002, 1000000003]
    assert row_a["tin_type"] == "ein"
    assert row_a["tin_value"] == "123456789"


def test_ptg2_large_provider_set_row_omits_inline_npi_when_group_hashes_available(monkeypatch):
    monkeypatch.setenv(process_ptg.PTG2_PROVIDER_SET_INLINE_NPI_LIMIT_ENV, "2")
    first, _ = process_ptg._build_provider_set_entry(
        file_id=1,
        provider_group_ref=10,
        provider_groups=[{"tin": {"type": "ein", "value": "111"}, "npi": [1000000001, 1000000002]}],
    )
    second, _ = process_ptg._build_provider_set_entry(
        file_id=1,
        provider_group_ref=11,
        provider_groups=[{"tin": {"type": "ein", "value": "222"}, "npi": [1000000003]}],
    )
    combined, _ = process_ptg._combine_provider_set_entries(file_id=1, entries=[first, second])

    row = process_ptg._ptg2_provider_set_row(combined)

    assert row["provider_count"] == 3
    assert row["npi"] is None
    assert row["canonical_payload"]["npi_inline"] is False
    assert row["canonical_payload"]["provider_group_hashes"]


def test_ptg2_provider_set_row_omits_inline_npi_by_default_when_group_hashes_available(monkeypatch):
    monkeypatch.delenv(process_ptg.PTG2_PROVIDER_SET_INLINE_NPI_LIMIT_ENV, raising=False)
    entry, _ = process_ptg._build_provider_set_entry(
        file_id=1,
        provider_group_ref=10,
        provider_groups=[{"tin": {"type": "ein", "value": "111"}, "npi": [1000000001]}],
    )

    row = process_ptg._ptg2_provider_set_row(entry)

    assert row["provider_count"] == 1
    assert row["npi"] is None
    assert row["canonical_payload"]["npi_inline"] is False


def test_ptg2_procedure_identity_groups_display_text_variants():
    base = {
        "billing_code_type": "CPT",
        "billing_code_type_version": "2026",
        "billing_code": "99213",
        "negotiation_arrangement": "ffs",
        "name": "Office visit",
        "description": "First description",
    }
    variant = {**base, "name": "Established patient visit", "description": "Different description"}
    changed_arrangement = {**base, "negotiation_arrangement": "bundle"}

    assert process_ptg._ptg2_procedure_row(base)["procedure_hash"] == process_ptg._ptg2_procedure_row(variant)["procedure_hash"]
    assert (
        process_ptg._ptg2_procedure_row(base)["procedure_hash"]
        != process_ptg._ptg2_procedure_row(changed_arrangement)["procedure_hash"]
    )


def test_ptg2_canonicalize_url_strips_signed_params():
    url = (
        "HTTPS://Example.COM:443/path/file.json.gz?"
        "sig=secret&sv=2020&foo=bar&Signature=abc&Key-Pair-Id=k&b=2&a=1"
    )

    assert process_ptg.canonicalize_url(url) == "https://example.com/path/file.json.gz?a=1&b=2&foo=bar"


def test_ptg2_normalize_tic_source_url_unescapes_html_query_separators():
    url = "https://example.com/rates.json.gz?Expires=1&#38;Policy=abc"

    assert process_ptg.normalize_tic_source_url(url) == "https://example.com/rates.json.gz?Expires=1&Policy=abc"


def test_ptg2_toc_parser_handles_uhc_sponsor_typo_and_duplicate_signed_urls():
    toc = {
        "reporting_entity_name": "UHC",
        "reporting_entity_type": "payer",
        "reporting_structure": [
            {
                "reporting_plans": [
                    {
                        "plan_name": "Example Plan",
                        "plan_id": "TESTPLAN001",
                        "plan_sponser_name": "Example Sponsor Co",
                        "plan_market_type": "group",
                    }
                ],
                "in_network_files": [
                    {"location": "https://cdn.test/rates.json.gz?sig=a&foo=1"},
                    {"location": "https://cdn.test/rates.json.gz?sig=b&foo=1"},
                ],
            }
        ],
    }

    entries = process_ptg.parse_toc_catalog_entries(
        toc,
        "https://payer.test/toc.json",
        plan_ids=["TESTPLAN001"],
    )
    in_network_entries = [
        entry for entry in entries if entry.source_type == "in-network"
    ]

    assert len(in_network_entries) == 1
    assert in_network_entries[0].canonical_url == "https://cdn.test/rates.json.gz?foo=1"
    assert in_network_entries[0].plan_info[0]["plan_sponsor_name"] == "Example Sponsor Co"


def test_ptg2_toc_parser_applies_plan_predicate_before_file_expansion(monkeypatch):
    toc_payload_dict = {
        "reporting_entity_name": "Cigna",
        "reporting_entity_type": "payer",
        "reporting_structure": [
            {
                "reporting_plans": [
                    {
                        "plan_name": "OAP",
                        "plan_id": "111111111",
                        "plan_sponsor_name": "Unrelated Employer",
                        "plan_market_type": "group",
                    }
                ],
                "in_network_files": [
                    {"location": "https://cdn.test/unrelated-rates.json.gz"}
                ],
            },
            {
                "reporting_plans": [
                    {
                        "plan_name": "OAP",
                        "plan_id": "222222222",
                        "plan_sponsor_name": "Example Target Employer",
                        "plan_market_type": "group",
                    }
                ],
                "in_network_files": [
                    {"location": "https://cdn.test/target-rates.json.gz"}
                ],
            },
        ],
    }
    expanded_locations = []
    original_source_type = ptg_source_jobs._toc_body_source_type

    def tracked_source_type(default_source_type, location, description=None):
        expanded_locations.append(location)
        return original_source_type(default_source_type, location, description)

    monkeypatch.setattr(
        ptg_source_jobs,
        "_toc_body_source_type",
        tracked_source_type,
    )

    entries = process_ptg.parse_toc_catalog_entries(
        toc_payload_dict,
        "https://payer.test/toc.json",
        plan_predicate=lambda plan: "target employer"
        in plan["plan_sponsor_name"].lower(),
    )

    in_network_entries = [entry for entry in entries if entry.source_type == "in-network"]
    assert expanded_locations == ["https://cdn.test/target-rates.json.gz"]
    assert [entry.original_url for entry in in_network_entries] == [
        "https://cdn.test/target-rates.json.gz"
    ]


def test_ptg2_toc_parser_merges_shared_file_plans_in_linear_work(monkeypatch):
    plan_count = 250
    shared_url = "https://cdn.test/shared-rates.json.gz"
    toc_payload_dict = {
        "reporting_entity_name": "Cigna",
        "reporting_entity_type": "payer",
        "reporting_structure": [
            {
                "reporting_plans": [
                    {
                        "plan_name": f"Plan {index}",
                        "plan_id": str(index),
                        "plan_sponsor_name": f"Employer {index}",
                        "plan_market_type": "group",
                    }
                ],
                "in_network_files": [{"location": shared_url}],
            }
            for index in range(plan_count)
        ],
    }
    original_dumps = ptg_source_jobs.canonical_json_dumps
    serialized_plans = []

    def tracked_dumps(value):
        serialized_plans.append(value)
        return original_dumps(value)

    monkeypatch.setattr(ptg_source_jobs, "canonical_json_dumps", tracked_dumps)

    entries = process_ptg.parse_toc_catalog_entries(
        toc_payload_dict,
        "https://payer.test/toc.json",
    )

    in_network_entries = [
        entry for entry in entries if entry.source_type == "in-network"
    ]
    [in_network_entry] = in_network_entries
    assert in_network_entry.original_url == shared_url
    assert len(in_network_entry.plan_info) == plan_count
    assert len(serialized_plans) == plan_count


def test_ptg2_toc_parser_accepts_list_shaped_file_fields():
    toc = {
        "reporting_entity_name": "BCBS",
        "reporting_entity_type": "third-party administrator",
        "reporting_structure": [
            {
                "reporting_plans": [{"plan_name": "Group PPO", "plan_id": "12-3456789", "plan_market_type": "group"}],
                "in_network_files": {"location": "https://cdn.test/in-network-rates.json.gz"},
                "allowed_amount_file": [
                    {"location": "https://cdn.test/allowed-amounts-1.json.gz", "description": "Allowed 1"},
                    {"location": "https://cdn.test/allowed-amounts-2.json.gz", "description": "Allowed 2"},
                ],
            }
        ],
    }

    entries = process_ptg.parse_toc_catalog_entries(toc, "https://payer.test/toc.json")

    assert [entry.source_type for entry in entries] == [
        "table-of-contents",
        "in-network",
        "allowed-amounts",
        "allowed-amounts",
    ]
    assert entries[1].original_url == "https://cdn.test/in-network-rates.json.gz"
    assert entries[2].description == "Allowed 1"
    assert entries[3].description == "Allowed 2"


def test_ptg2_toc_parser_accepts_healthsparq_metadata_files():
    """HealthSparq metadata entries should become normal PTG catalog entries."""
    metadata_url = (
        "https://mrf.healthsparq.com/aetnacvs-egress.nophi.kyruushsq.com/"
        "prd/mrf/AETNACVS_I/ASA/latest_metadata.json"
    )
    metadata_payload_by_key = {
        "files": [
            {
                "reportingEntityName": "Aetna Signature Administrators",
                "reportingEntityType": "Third Party Vendor",
                "reportingPlans": [
                    {
                        "planId": "123456789",
                        "planIdType": "ein",
                        "planMarketType": "group",
                        "planName": "ASA_17_60289",
                    }
                ],
                "fileSchema": "IN_NETWORK_RATES",
                "fileName": "2026-07-05_pl-xp-tr18_Aetna-Signature-Administrators.json.gz",
                "filePath": "2026-07-05/inNetworkRates/2026-07-05_pl-xp-tr18_Aetna-Signature-Administrators.json.gz",
            }
        ]
    }

    catalog_entries = process_ptg.parse_toc_catalog_entries(
        metadata_payload_by_key,
        metadata_url,
        plan_market_types=["group"],
    )
    in_network_catalog_entries = [entry for entry in catalog_entries if entry.source_type == "in-network"]

    assert len(in_network_catalog_entries) == 1
    assert in_network_catalog_entries[0].original_url == (
        "https://mrf.healthsparq.com/aetnacvs-egress.nophi.kyruushsq.com/prd/mrf/"
        "AETNACVS_I/ASA/2026-07-05/inNetworkRates/"
        "2026-07-05_pl-xp-tr18_Aetna-Signature-Administrators.json.gz"
    )
    assert in_network_catalog_entries[0].description == "2026-07-05_pl-xp-tr18_Aetna-Signature-Administrators.json.gz"
    assert in_network_catalog_entries[0].plan_info[0]["plan_id"] == "123456789"


def test_ptg2_toc_parser_rejects_non_pricing_payload():
    payload = {
        "provider_urls": [
            "https://example.test/acadirectory/individualprovidersfile.json",
        ],
        "plan_urls": [
            "https://example.test/acadirectory/plansfile.json",
        ],
    }

    entries = process_ptg.parse_toc_catalog_entries(
        payload, "https://example.test/acadirectory/97176Index.json"
    )

    assert entries == []


def test_ptg2_toc_parser_accepts_plural_allowed_amount_files():
    toc = {
        "reporting_entity_name": "BCBS",
        "reporting_entity_type": "third-party administrator",
        "reporting_structure": [
            {
                "reporting_plans": [{"plan_name": "Group PPO", "plan_id": "12-3456789", "plan_market_type": "group"}],
                "allowed_amount_files": [
                    {"location": "https://cdn.test/allowed-amounts-1.json.gz", "description": "Allowed 1"},
                    {"location": "https://cdn.test/allowed-amounts-2.json.gz", "description": "Allowed 2"},
                ],
            }
        ],
    }

    entries = process_ptg.parse_toc_catalog_entries(toc, "https://payer.test/toc.json")

    assert [entry.source_type for entry in entries] == [
        "table-of-contents",
        "allowed-amounts",
        "allowed-amounts",
    ]
    assert entries[1].description == "Allowed 1"
    assert entries[2].description == "Allowed 2"


def test_ptg2_toc_parser_normalizes_asr_download_links():
    toc = {
        "reporting_entity_name": "ASR Health Benefits",
        "reporting_entity_type": "Third Party Administrator",
        "reporting_structure": [
            {
                "reporting_plans": [
                    {
                        "plan_name": "ASR Health Benefits - ASR1",
                        "plan_id": "TESTPLAN001",
                        "plan_market_type": "group",
                    }
                ],
                "in_network_files": [
                    {
                        "location": (
                            "https://www.asrhealthbenefits.com/home/umbraco/surface/"
                            "mrfdownload/index?g=1208&i=595&t=InNetwork"
                        )
                    }
                ],
            }
        ],
    }

    entries = process_ptg.parse_toc_catalog_entries(toc, "https://payer.test/toc.json")
    in_network_entries = [entry for entry in entries if entry.source_type == "in-network"]

    assert len(in_network_entries) == 1
    assert in_network_entries[0].original_url == (
        "https://www.asrhealthbenefits.com/umbraco/surface/mrfdownload"
        "?groupNumber=1208&fileType=InNetwork&fileId=595"
    )
    assert in_network_entries[0].canonical_url == (
        "https://www.asrhealthbenefits.com/umbraco/surface/mrfdownload"
        "?fileId=595&fileType=InNetwork&groupNumber=1208"
    )


def test_ptg2_toc_jobs_normalize_asr_download_links(monkeypatch):
    toc = {
        "reporting_entity_name": "ASR Health Benefits",
        "reporting_entity_type": "Third Party Administrator",
        "reporting_structure": [
            {
                "reporting_plans": [
                    {
                        "plan_name": "ASR Health Benefits - ASR1",
                        "plan_id": "TESTPLAN001",
                        "plan_market_type": "group",
                    }
                ],
                "in_network_files": [
                    {
                        "location": (
                            "https://www.asrhealthbenefits.com/home/umbraco/surface/"
                            "mrfdownload/index?g=1208&i=596&t=InNetwork"
                        ),
                    },
                ],
            },
        ],
    }
    pushed_file_rows = []

    async def fake_materialize(*_args, **_kwargs):
        artifact = SimpleNamespace(logical_path="/tmp/asr-index.json")
        return artifact, artifact

    async def fake_push_objects(rows, _cls, **_kwargs):
        pushed_file_rows.extend(rows)

    monkeypatch.setattr(process_ptg, "materialize_json_source", fake_materialize)
    monkeypatch.setattr(process_ptg, "load_json_artifact", lambda _path: toc)
    monkeypatch.setattr(process_ptg, "push_objects", fake_push_objects)
    monkeypatch.setattr(process_ptg, "flush_error_log", AsyncMock())

    jobs = asyncio.run(
        process_ptg._process_table_of_contents(
            "https://www.asrhealthbenefits.com/umbraco/surface/mrfdownload?fileType=TableOfContents&groupNumber=1208",
            {"PTGFile": object, "ImportLog": object},
            test_mode=False,
            plan_ids=["TESTPLAN001"],
            plan_market_types=["group"],
        )
    )

    expected_url = (
        "https://www.asrhealthbenefits.com/umbraco/surface/mrfdownload"
        "?groupNumber=1208&fileType=InNetwork&fileId=596"
    )
    assert jobs[0]["url"] == expected_url
    assert any(row["url"] == expected_url and row["file_type"] == "in-network" for row in pushed_file_rows)


def test_ptg2_toc_jobs_repair_ucare_json_and_allowed_amount_lists(
    monkeypatch, tmp_path
):
    """Verify this PTG import regression contract."""
    toc_path = tmp_path / "ucare_toc.json"
    toc_path.write_text(
        """
        {
          "reporting_entity_name": "UCare Minnesota",
          "reporting_entity_type": "Health Insurance Issuer",
          "version": "1.0.0",
          "reporting_structure": [
            {
              "reporting_plans": [
                {
                  "plan_name": "UCare IFP",
                  "plan_id_type": "hios",
                  "plan_id": "85736MN023",
                  "plan_market_type": "Individual"
                }
              ],
              "in_network_files": [
                {
                  "description": "rates for UCare network",
                  "location": "https://ucm-p-001.sitecorecontenthub.cloud/api/public/content/UCare_InNetwork.json?download=true"
                }
                {
                  "description": "rates for dental network",
                  "location": "https://ucm-p-001.sitecorecontenthub.cloud/api/public/content/Dental_InNetwork.json?download=true"
                }
              ],
              "allowed_amount_file": [
                {
                  "description": "UCare allowed amounts",
                  "location": "https://ucm-p-001.sitecorecontenthub.cloud/api/public/content/UCare_AllowedAmount.json?download=true"
                },
                {
                  "description": "Fulcrum allowed amounts",
                  "location": "https://ucm-p-001.sitecorecontenthub.cloud/api/public/content/Fulcrum_AllowedAmount.json?download=true"
                }
              ]
            }
          ]
        }
        """,
        encoding="utf-8",
    )
    pushed_file_rows = []

    async def fake_materialize(*_args, **_kwargs):
        artifact = SimpleNamespace(logical_path=toc_path)
        return artifact, artifact

    async def fake_push_objects(rows, _cls, **_kwargs):
        pushed_file_rows.extend(rows)

    monkeypatch.setattr(process_ptg, "materialize_json_source", fake_materialize)
    monkeypatch.setattr(process_ptg, "push_objects", fake_push_objects)
    monkeypatch.setattr(process_ptg, "flush_error_log", AsyncMock())

    jobs = asyncio.run(
        process_ptg._process_table_of_contents(
            "https://ucm-p-001.sitecorecontenthub.cloud/api/public/content/ucare_toc.json?download=true",
            {"PTGFile": object, "ImportLog": object},
            test_mode=False,
        )
    )

    assert [job["type"] for job in jobs] == [
        "in_network",
        "in_network",
        "allowed_amounts",
        "allowed_amounts",
    ]
    assert jobs[1]["url"].endswith("/Dental_InNetwork.json?download=true")
    assert jobs[3]["url"].endswith("/Fulcrum_AllowedAmount.json?download=true")
    assert [row["file_type"] for row in pushed_file_rows] == [
        "table-of-contents",
        "in-network",
        "in-network",
        "allowed-amounts",
        "allowed-amounts",
    ]


def _write_healthsparq_metadata_fixture(tmp_path, target_file_name: str) -> Path:
    """Write a minimal HealthSparq metadata document with one selected file."""
    base_file_metadata_by_key = {
        "reportingEntityName": "Aetna Signature Administrators",
        "reportingEntityType": "Third Party Vendor",
        "reportingPlans": [
            {
                "planId": "123456789",
                "planIdType": "ein",
                "planMarketType": "group",
                "planName": "ASA_17_60289",
            }
        ],
        "fileSchema": "IN_NETWORK_RATES",
    }
    metadata_document_path = tmp_path / "latest_metadata.json"
    metadata_document_path.write_text(
        json.dumps(
            {
                "files": [
                    {
                        **base_file_metadata_by_key,
                        "fileName": "unrelated.json.gz",
                        "filePath": "2026-07-05/inNetworkRates/unrelated.json.gz",
                    },
                    {
                        **base_file_metadata_by_key,
                        "fileName": target_file_name,
                        "filePath": f"2026-07-05/inNetworkRates/{target_file_name}",
                    },
                ]
            }
        ),
        encoding="utf-8",
    )
    return metadata_document_path


def test_ptg2_toc_jobs_accept_healthsparq_metadata_files(monkeypatch, tmp_path):
    """Targeted HealthSparq metadata imports should emit the matching rate-file job."""
    metadata_url = (
        "https://mrf.healthsparq.com/aetnacvs-egress.nophi.kyruushsq.com/"
        "prd/mrf/AETNACVS_I/ASA/latest_metadata.json"
    )
    target_file_name = "2026-07-05_pl-xp-tr18_Aetna-Signature-Administrators.json.gz"
    metadata_document_path = _write_healthsparq_metadata_fixture(tmp_path, target_file_name)
    pushed_ptg_file_rows = []

    async def fake_materialize(*_args, **_kwargs):
        artifact = SimpleNamespace(logical_path=metadata_document_path)
        return artifact, artifact

    async def fake_push_objects(ptg_rows_to_push, _cls, **_kwargs):
        pushed_ptg_file_rows.extend(ptg_rows_to_push)

    monkeypatch.setattr(process_ptg, "materialize_json_source", fake_materialize)
    monkeypatch.setattr(process_ptg, "push_objects", fake_push_objects)
    monkeypatch.setattr(process_ptg, "flush_error_log", AsyncMock())

    selected_ptg_jobs = asyncio.run(
        process_ptg._process_table_of_contents(
            metadata_url,
            {"PTGFile": object, "ImportLog": object},
            test_mode=False,
            file_url_contains=[target_file_name],
            max_files=1,
        )
    )

    assert [job["type"] for job in selected_ptg_jobs] == ["in_network"]
    assert selected_ptg_jobs[0]["url"] == (
        "https://mrf.healthsparq.com/aetnacvs-egress.nophi.kyruushsq.com/prd/mrf/"
        f"AETNACVS_I/ASA/2026-07-05/inNetworkRates/{target_file_name}"
    )
    assert selected_ptg_jobs[0]["plan_info"][0]["plan_name"] == "ASA_17_60289"
    assert [ptg_file_row["file_type"] for ptg_file_row in pushed_ptg_file_rows] == ["table-of-contents", "in-network"]


def _write_targeted_toc_fixture(tmp_path, source_file_locations):
    """Create a minimal TOC fixture with the requested source-file locations."""
    table_of_contents_path = tmp_path / "toc.json"
    table_of_contents_path.write_text(
        json.dumps(
            {
                "reporting_entity_name": "Cigna Health Life Insurance Company",
                "reporting_entity_type": "health insurance issuer",
                "reporting_structure": [
                    {
                        "reporting_plans": [{"plan_name": "OAP", "plan_id": "473435755", "plan_market_type": "group"}],
                        "in_network_files": [
                            {"location": source_file_location} for source_file_location in source_file_locations
                        ],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    return table_of_contents_path


def test_ptg2_toc_targeted_file_filter_skips_full_catalog_expansion(monkeypatch, tmp_path):
    """Targeted TOC imports avoid expanding all catalog entries before source-file filtering."""
    target_source_url = "https://cdn.example.test/target-473435755-in-network.json.gz"
    source_file_locations = [
        "https://cdn.example.test/large-unused-in-network.json.gz",
        target_source_url,
        "https://cdn.example.test/another-unused-in-network.json.gz",
    ]
    table_of_contents_path = _write_targeted_toc_fixture(tmp_path, source_file_locations)
    pushed_ptg_file_rows = []

    async def fake_materialize(*_args, **_kwargs):
        table_of_contents_artifact = SimpleNamespace(logical_path=table_of_contents_path)
        return table_of_contents_artifact, table_of_contents_artifact

    async def fake_push_objects(ptg_rows_to_push, _cls, **_kwargs):
        pushed_ptg_file_rows.extend(ptg_rows_to_push)

    def fail_catalog_expansion(*_args, **_kwargs):
        raise AssertionError("targeted source-file imports should not expand the full TOC catalog")

    monkeypatch.setattr(process_ptg, "materialize_json_source", fake_materialize)
    monkeypatch.setattr(process_ptg, "_record_source_version", AsyncMock())
    monkeypatch.setattr(process_ptg, "push_objects", fake_push_objects)
    monkeypatch.setattr(process_ptg, "flush_error_log", AsyncMock())
    monkeypatch.setattr(process_ptg, "parse_toc_catalog_entries", fail_catalog_expansion)

    filtered_toc_jobs = asyncio.run(
        process_ptg._process_table_of_contents(
            "https://cdn.example.test/cigna-index.json",
            {"PTGFile": object, "ImportLog": object},
            test_mode=False,
            import_run_id="ptg2:test",
            file_url_contains=["target-473435755"],
            max_files=1,
        )
    )

    assert [toc_job["url"] for toc_job in filtered_toc_jobs] == [target_source_url]
    assert [ptg_file_row["file_type"] for ptg_file_row in pushed_ptg_file_rows] == [
        "table-of-contents",
        "in-network",
    ]
    assert pushed_ptg_file_rows[1]["url"] == target_source_url


def test_ptg2_toc_jobs_skip_non_mrf_body_locations(monkeypatch, tmp_path):
    """Verify this PTG import regression contract."""
    toc_path = tmp_path / "toc.json"
    toc_path.write_text(
        json.dumps(
            {
                "reporting_entity_name": "Example Payer",
                "reporting_entity_type": "third-party administrator",
                "reporting_structure": [
                    {
                        "reporting_plans": [
                            {
                                "plan_name": "Example Plan",
                                "plan_id": "123",
                                "plan_market_type": "group",
                            }
                        ],
                        "in_network_files": [
                            {"location": "https://www.zelis.com/"},
                            {"location": "Missing file"},
                            {"location": "ftp://example.test/rates.json.gz"},
                            {
                                "location": (
                                    "https://cdn.example.test/"
                                    "2026-06_in-network-rates.json.gz"
                                )
                            },
                        ],
                        "allowed_amount_files": [
                            {"location": "https://example.test/"},
                            {
                                "location": (
                                    "https://cdn.example.test/"
                                    "2026-06_allowed-amounts.json.gz"
                                )
                            },
                        ],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    pushed_file_rows = []

    async def fake_materialize(*_args, **_kwargs):
        artifact = SimpleNamespace(logical_path=toc_path)
        return artifact, artifact

    async def fake_push_objects(rows, _cls, **_kwargs):
        pushed_file_rows.extend(rows)

    monkeypatch.setattr(process_ptg, "materialize_json_source", fake_materialize)
    monkeypatch.setattr(process_ptg, "push_objects", fake_push_objects)
    monkeypatch.setattr(process_ptg, "flush_error_log", AsyncMock())

    jobs = asyncio.run(
        process_ptg._process_table_of_contents(
            "https://payer.test/toc.json",
            {"PTGFile": object, "ImportLog": object},
            test_mode=False,
        )
    )

    assert [(job["type"], job["url"]) for job in jobs] == [
        (
            "in_network",
            "https://cdn.example.test/2026-06_in-network-rates.json.gz",
        ),
        (
            "allowed_amounts",
            "https://cdn.example.test/2026-06_allowed-amounts.json.gz",
        ),
    ]
    assert [row["file_type"] for row in pushed_file_rows] == [
        "table-of-contents",
        "in-network",
        "allowed-amounts",
    ]


def test_ptg2_toc_artifact_repair_preserves_string_literals(tmp_path):
    toc_path = tmp_path / "toc.json"
    toc_path.write_text(
        """
        {
          "reporting_entity_name": "Example Payer",
          "reporting_entity_type": "payer",
          "reporting_structure": [
            {
              "reporting_plans": [{"plan_name": "Example Plan"}],
              "in_network_files": [
                {
                  "description": "keep }{ literal",
                  "location": "https://files.example.test/a_in-network-rates.json.gz"
                }
                {
                  "description": "next file",
                  "location": "https://files.example.test/b_in-network-rates.json.gz"
                }
              ]
            }
          ]
        }
        """,
        encoding="utf-8",
    )

    toc = process_ptg._load_table_of_contents_artifact(toc_path)

    descriptions = [
        row["description"]
        for row in toc["reporting_structure"][0]["in_network_files"]
    ]
    assert descriptions == ["keep }{ literal", "next file"]


def test_ptg2_artifact_reuse_by_strong_etag_and_length(tmp_path):
    raw_path = tmp_path / "raw.json"
    raw_path.write_text('{"ok": true}', encoding="utf-8")
    raw_sha, byte_count = process_ptg.sha256_file(raw_path)
    store = process_ptg.PTG2ArtifactStore(tmp_path)
    candidate = {
        "artifact_kind": process_ptg.PTG2_ARTIFACT_RAW,
        "canonical_url": "https://example.test/raw.json",
        "raw_storage_uri": raw_path.resolve().as_uri(),
        "raw_sha256": raw_sha,
        "content_length": byte_count,
        "etag": '"strong"',
    }
    head = process_ptg.PTG2HeadMetadata(
        url="https://example.test/raw.json",
        status=200,
        etag='"strong"',
        content_length=byte_count,
        supports_head=True,
    )

    reused, mode = process_ptg.choose_reusable_raw_artifact([candidate], head, store=store)

    assert reused == candidate
    assert mode == "strong_etag_length"


def test_ptg2_artifact_reuse_skips_missing_metadata_candidate(tmp_path):
    missing_path = tmp_path / "missing.json.gz"
    store = process_ptg.PTG2ArtifactStore(tmp_path / "store")
    candidate = {
        "artifact_kind": process_ptg.PTG2_ARTIFACT_RAW,
        "canonical_url": "https://example.test/raw.json.gz",
        "raw_storage_uri": missing_path.resolve().as_uri(),
        "raw_sha256": "0" * 64,
        "content_length": 1024,
        "etag": '"strong"',
        "status": "available",
    }
    head = process_ptg.PTG2HeadMetadata(
        url="https://example.test/raw.json.gz",
        status=200,
        etag='"strong"',
        content_length=1024,
        supports_head=True,
    )

    reused, mode = process_ptg.choose_reusable_raw_artifact([candidate], head, store=store)

    assert reused is None
    assert mode is None


def test_download_raw_artifact_redownloads_bad_gzip_reuse_candidate(monkeypatch, tmp_path):
    payload = gzip.compress(b'{"ok": true}')
    bad_path = tmp_path / "bad.json.gz"
    bad_path.write_bytes(b"\0\0" + b"x" * (len(payload) - 2))
    bad_sha, bad_size = process_ptg.sha256_file(bad_path)
    store = process_ptg.PTG2ArtifactStore(tmp_path / "store")

    async def handle(request):
        if request.method == "HEAD":
            return web.Response(
                headers={
                    "Content-Length": str(len(payload)),
                    "ETag": '"reuse-test"',
                }
            )
        return web.Response(body=payload, headers={"Content-Length": str(len(payload))})

    async def run_download():
        app = web.Application()
        app.router.add_route("*", "/raw.json.gz", handle)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        url = f"http://127.0.0.1:{port}/raw.json.gz"
        store.record_manifest(
            {
                "artifact_kind": process_ptg.PTG2_ARTIFACT_RAW,
                "canonical_url": process_ptg.canonicalize_url(url),
                "raw_storage_uri": bad_path.resolve().as_uri(),
                "raw_sha256": bad_sha,
                "sha256": bad_sha,
                "content_length": len(payload),
                "byte_count": bad_size,
                "etag": '"reuse-test"',
                "status": "available",
            }
        )
        try:
            return await process_ptg.download_raw_artifact(url, store=store)
        finally:
            await runner.cleanup()

    monkeypatch.setenv("HLTHPRT_FETCH_ALLOW_LOCAL", "true")
    monkeypatch.setenv(process_ptg.PTG2_RANGE_DOWNLOADS_ENV, "false")

    artifact = asyncio.run(run_download())
    manifest_lines = (tmp_path / "store" / "manifest.jsonl").read_text(encoding="utf-8").splitlines()

    assert artifact.reused is False
    assert Path(artifact.raw_path).read_bytes() == payload
    assert any('"status": "corrupt"' in line for line in manifest_lines)
    assert any('"status": "available"' in line and artifact.raw_sha256 in line for line in manifest_lines)


def test_download_raw_artifact_redownloads_corrupt_gzip_reuse_candidate(monkeypatch, tmp_path):
    payload = gzip.compress(b'{"ok": true}' * 1024)
    bad_payload = bytearray(payload)
    bad_payload[len(bad_payload) // 2] ^= 0xFF
    bad_path = tmp_path / "bad-body.json.gz"
    bad_path.write_bytes(bad_payload)
    bad_sha, bad_size = process_ptg.sha256_file(bad_path)
    store = process_ptg.PTG2ArtifactStore(tmp_path / "store")

    async def handle(request):
        if request.method == "HEAD":
            return web.Response(
                headers={
                    "Content-Length": str(len(payload)),
                    "ETag": '"reuse-body-test"',
                }
            )
        return web.Response(body=payload, headers={"Content-Length": str(len(payload))})

    async def run_download():
        app = web.Application()
        app.router.add_route("*", "/raw.json.gz", handle)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        url = f"http://127.0.0.1:{port}/raw.json.gz"
        store.record_manifest(
            {
                "artifact_kind": process_ptg.PTG2_ARTIFACT_RAW,
                "canonical_url": process_ptg.canonicalize_url(url),
                "raw_storage_uri": bad_path.resolve().as_uri(),
                "raw_sha256": bad_sha,
                "sha256": bad_sha,
                "content_length": len(payload),
                "byte_count": bad_size,
                "etag": '"reuse-body-test"',
                "status": "available",
            }
        )
        try:
            return await process_ptg.download_raw_artifact(url, store=store)
        finally:
            await runner.cleanup()

    monkeypatch.setenv("HLTHPRT_FETCH_ALLOW_LOCAL", "true")
    monkeypatch.setenv(process_ptg.PTG2_RANGE_DOWNLOADS_ENV, "false")

    artifact = asyncio.run(run_download())
    manifest_lines = (tmp_path / "store" / "manifest.jsonl").read_text(encoding="utf-8").splitlines()

    assert artifact.reused is False
    assert Path(artifact.raw_path).read_bytes() == payload
    assert any('"status": "corrupt"' in line and "integrity check" in line for line in manifest_lines)
    assert any('"status": "available"' in line and artifact.raw_sha256 in line for line in manifest_lines)


def test_ptg2_range_download_assembles_artifact(monkeypatch, tmp_path):
    """Verify this PTG import regression contract."""
    payload = (b"0123456789abcdef" * 1024 * 1024)[:3 * 1024 * 1024]
    requests = []

    async def handle(request):
        if request.method == "HEAD":
            return web.Response(
                headers={
                    "Content-Length": str(len(payload)),
                    "ETag": '"range-test"',
                    "Accept-Ranges": "bytes",
                }
            )
        range_header = request.headers.get("Range")
        requests.append(range_header)
        if range_header:
            start_text, end_text = range_header.removeprefix("bytes=").split("-", 1)
            start = int(start_text)
            end = int(end_text)
            chunk = payload[start : end + 1]
            return web.Response(
                status=206,
                body=chunk,
                headers={
                    "Content-Length": str(len(chunk)),
                    "Content-Range": f"bytes {start}-{end}/{len(payload)}",
                    "ETag": '"range-test"',
                },
            )
        return web.Response(body=payload, headers={"Content-Length": str(len(payload))})

    async def run_download():
        app = web.Application()
        app.router.add_route("*", "/artifact.bin", handle)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        sockets = site._server.sockets
        assert sockets
        port = sockets[0].getsockname()[1]
        try:
            return await process_ptg.download_raw_artifact(
                f"http://127.0.0.1:{port}/artifact.bin",
                store=process_ptg.PTG2ArtifactStore(tmp_path),
            )
        finally:
            await runner.cleanup()

    monkeypatch.setenv("HLTHPRT_FETCH_ALLOW_LOCAL", "true")
    monkeypatch.setenv(process_ptg.PTG2_RANGE_DOWNLOADS_ENV, "true")
    monkeypatch.setenv(process_ptg.PTG2_RANGE_DOWNLOAD_MIN_BYTES_ENV, "1")
    monkeypatch.setenv(process_ptg.PTG2_RANGE_DOWNLOAD_CHUNK_BYTES_ENV, str(1024 * 1024))
    monkeypatch.setenv(process_ptg.PTG2_RANGE_DOWNLOAD_TASKS_ENV, "2")

    artifact = asyncio.run(run_download())

    assert Path(artifact.raw_path).read_bytes() == payload
    assert any(request == "bytes=0-0" for request in requests)
    assert sum(1 for request in requests if request and request != "bytes=0-0") >= 2
    assert not list((tmp_path / "partial-retained").glob("*.ranges.json"))


def test_ptg2_range_download_retries_short_chunk(monkeypatch, tmp_path):
    """Verify this PTG import regression contract."""
    payload = (b"0123456789abcdef" * 1024 * 1024)[:3 * 1024 * 1024]
    attempts_by_range = {}

    async def handle(request):
        if request.method == "HEAD":
            return web.Response(
                headers={
                    "Content-Length": str(len(payload)),
                    "ETag": '"range-retry-test"',
                    "Accept-Ranges": "bytes",
                }
            )
        range_header = request.headers.get("Range")
        if not range_header:
            return web.Response(body=payload, headers={"Content-Length": str(len(payload))})
        start_text, end_text = range_header.removeprefix("bytes=").split("-", 1)
        start = int(start_text)
        end = int(end_text)
        attempts_by_range[range_header] = attempts_by_range.get(range_header, 0) + 1
        chunk = payload[start : end + 1]
        if range_header == "bytes=1048576-2097151" and attempts_by_range[range_header] == 1:
            chunk = chunk[: len(chunk) // 2]
        return web.Response(
            status=206,
            body=chunk,
            headers={
                "Content-Length": str(len(chunk)),
                "Content-Range": f"bytes {start}-{end}/{len(payload)}",
                "ETag": '"range-retry-test"',
            },
        )

    async def run_download():
        app = web.Application()
        app.router.add_route("*", "/artifact.bin", handle)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        sockets = site._server.sockets
        assert sockets
        port = sockets[0].getsockname()[1]
        try:
            return await process_ptg.download_raw_artifact(
                f"http://127.0.0.1:{port}/artifact.bin",
                store=process_ptg.PTG2ArtifactStore(tmp_path),
            )
        finally:
            await runner.cleanup()

    monkeypatch.setenv("HLTHPRT_FETCH_ALLOW_LOCAL", "true")
    monkeypatch.setenv(process_ptg.PTG2_RANGE_DOWNLOADS_ENV, "true")
    monkeypatch.setenv(process_ptg.PTG2_RANGE_DOWNLOAD_MIN_BYTES_ENV, "1")
    monkeypatch.setenv(process_ptg.PTG2_RANGE_DOWNLOAD_CHUNK_BYTES_ENV, str(1024 * 1024))
    monkeypatch.setenv(process_ptg.PTG2_RANGE_DOWNLOAD_TASKS_ENV, "2")
    monkeypatch.setenv(process_ptg.PTG2_DOWNLOAD_RETRIES_ENV, "2")
    monkeypatch.setenv(process_ptg.PTG2_DOWNLOAD_RETRY_DELAY_SECONDS_ENV, "0")

    artifact = asyncio.run(run_download())

    assert Path(artifact.raw_path).read_bytes() == payload
    assert attempts_by_range["bytes=1048576-2097151"] == 2
    assert not list((tmp_path / "partial-retained").glob("*.ranges.json"))


def test_ptg2_packing_helpers_are_order_insensitive_for_sets():
    provider_a = process_ptg.build_provider_set([3, 1, 2, 2], tin_type="ein", tin_value="123")
    provider_b = process_ptg.build_provider_set([2, 3, 1], tin_type="ein", tin_value="123")
    price_set_a = process_ptg.build_price_set(["b", "a", "a"])
    price_set_b = process_ptg.build_price_set(["a", "b"])
    chunk_a = process_ptg.build_fact_chunk("ctx", "in_network", "proc", "0a", ["pack-b", "pack-a"])
    chunk_b = process_ptg.build_fact_chunk("ctx", "in_network", "proc", "0a", ["pack-a", "pack-b"])
    rate_set_a = process_ptg.build_rate_set("ctx", ["chunk-b", "chunk-a"])
    rate_set_b = process_ptg.build_rate_set("ctx", ["chunk-a", "chunk-b"])

    assert provider_a["provider_set_hash"] == provider_b["provider_set_hash"]
    assert provider_a["npi"] == [1, 2, 3]
    assert price_set_a["price_set_hash"] == price_set_b["price_set_hash"]
    assert chunk_a["fact_chunk_hash"] == chunk_b["fact_chunk_hash"]
    assert rate_set_a["rate_set_hash"] == rate_set_b["rate_set_hash"]
    assert process_ptg.provider_hash_bucket(provider_a["provider_set_hash"], bucket_count=16)


def test_ptg2_provider_bucket_count_is_configurable(monkeypatch):
    monkeypatch.setenv(process_ptg.PTG2_PROVIDER_BUCKET_COUNT_ENV, "16")

    assert process_ptg.ptg2_provider_bucket_count() == 16


def test_ptg2_rate_pack_group_is_order_insensitive_for_provider_sets():
    pack_a = process_ptg.build_rate_pack_group(
        "ctx",
        "in_network",
        "proc",
        ["provider-b", "provider-a", "provider-a"],
        "price",
        "source",
    )
    pack_b = process_ptg.build_rate_pack_group(
        "ctx",
        "in_network",
        "proc",
        ["provider-a", "provider-b"],
        "price",
        "source",
    )
    changed = process_ptg.build_rate_pack_group(
        "ctx",
        "in_network",
        "proc",
        ["provider-a"],
        "price",
        "source",
    )

    assert pack_a["rate_pack_hash"] == pack_b["rate_pack_hash"]
    assert pack_a["provider_set_hash"] == pack_b["provider_set_hash"]
    assert pack_a["canonical_payload"]["provider_set_hashes"] == ["provider-a", "provider-b"]
    assert pack_a["rate_pack_hash"] != changed["rate_pack_hash"]


def test_ptg2_rate_pack_procedure_group_is_order_insensitive():
    pack_a = process_ptg.build_rate_pack_procedure_group(
        "ctx",
        "in_network",
        ["proc-b", "proc-a", "proc-a"],
        ["provider-b", "provider-a"],
        "price",
        "source",
    )
    pack_b = process_ptg.build_rate_pack_procedure_group(
        "ctx",
        "in_network",
        ["proc-a", "proc-b"],
        ["provider-a", "provider-b"],
        "price",
        "source",
    )
    changed = process_ptg.build_rate_pack_procedure_group(
        "ctx",
        "in_network",
        ["proc-a"],
        ["provider-a", "provider-b"],
        "price",
        "source",
    )

    assert pack_a["rate_pack_hash"] == pack_b["rate_pack_hash"]
    assert pack_a["canonical_payload"]["procedure_hashes"] == ["proc-a", "proc-b"]
    assert pack_a["canonical_payload"]["provider_set_hashes"] == ["provider-a", "provider-b"]
    assert pack_a["rate_pack_hash"] != changed["rate_pack_hash"]


def test_ptg2_compact_rate_pack_flush_groups_procedures(monkeypatch):
    pushed = {}

    async def fake_push(rows, cls, **_kwargs):
        pushed.setdefault(getattr(cls, "__name__", str(cls)), []).extend(rows)

    monkeypatch.setattr(process_ptg, "_push_ptg2_objects", fake_push)
    state = process_ptg._compact_state(batch_rows=1)
    state["rate_pack_groups"] = {
        ("proc-a", "price", "source"): {"provider-a"},
        ("proc-b", "price", "source"): {"provider-a"},
    }

    asyncio.run(process_ptg._flush_compact_rate_pack_groups(state, "ctx"))

    assert state["rate_pack_groups"] == {}
    assert state["counts"]["rate_packs"] == 1
    assert len(pushed["PTG2RatePack"]) == 1
    assert pushed["PTG2RatePack"][0]["canonical_payload"]["procedure_hashes"] == ["proc-a", "proc-b"]
    assert sum(len(v) for v in state["chunk_rate_packs"].values()) == 2


def test_ptg2_compact_rate_pack_flush_writes_serving_rows(monkeypatch):
    pushed = {}

    async def fake_push(rows, cls, **_kwargs):
        pushed.setdefault(getattr(cls, "__name__", str(cls)), []).extend(rows)

    monkeypatch.setattr(process_ptg, "_push_ptg2_objects", fake_push)
    state = process_ptg._compact_state(batch_rows=1)
    state["snapshot_id"] = "snap"
    state["plan_fields"] = {"plan_id": "TESTPLAN001", "plan_name": "Example Plan"}
    state["procedure_payloads"] = {
        "proc-a": {
            "billing_code_type": "CPT",
            "billing_code": "00102",
            "name": "Anesthesia",
            "description": "Anesthesia description",
        }
    }
    state["price_payloads"] = {"price": [{"negotiated_rate": "50", "negotiated_type": "negotiated"}]}
    state["provider_set_counts"] = {"provider-a": 10, "provider-b": 20}
    state["provider_set_network_names"] = {"provider-a": ["C2"], "provider-b": ["C2", "Choice Plus"]}
    state["source_trace_payload"] = [
        {
            "source_file_version_id": "source-version-1",
            "original_url": "https://example.test/rates.json.gz",
        }
    ]
    state["rate_pack_groups"] = {("proc-a", "price", "source"): {"provider-a", "provider-b"}}

    asyncio.run(process_ptg._flush_compact_rate_pack_groups(state, "ctx"))

    serving_row = pushed["PTG2ServingRate"][0]
    assert serving_row["snapshot_id"] == "snap"
    assert serving_row["plan_id"] == "TESTPLAN001"
    assert serving_row["reported_code"] == "00102"
    assert serving_row["procedure_code"] == process_ptg.return_checksum(["CPT", "00102"])
    assert serving_row["provider_count"] == 30
    assert serving_row["prices"][0]["negotiated_rate"] == 50
    assert serving_row["source_trace_set_hash"] == "source"
    assert serving_row["network_names"] == ["C2", "Choice Plus"]
    assert serving_row["source_trace"][0]["source_file_version_id"] == "source-version-1"
    assert serving_row["source_trace"][0]["original_url"] == "https://example.test/rates.json.gz"


def test_ptg2_compact_rows_can_schedule_async_writes(monkeypatch):
    pushed = []

    class FakeClass:
        __name__ = "FakeClass"

    async def fake_push(rows, cls, **_kwargs):
        await asyncio.sleep(0)
        pushed.append((cls, rows))

    monkeypatch.setenv(process_ptg.PTG2_ASYNC_WRITE_TASKS_ENV, "2")
    monkeypatch.setattr(process_ptg, "_push_ptg2_objects", fake_push)
    async def run_writes():
        state = process_ptg._compact_state(batch_rows=1)
        await process_ptg._schedule_compact_write(state, [{"a": 1}], FakeClass)
        assert state["pending_writes"]
        await process_ptg._drain_compact_writes(state)
        assert not state["pending_writes"]

    asyncio.run(run_writes())
    assert pushed == [(FakeClass, [{"a": 1}])]


def test_ptg2_serving_only_worker_chunk_dedupes_rows(monkeypatch):
    rows_a = {
        "serving_rows": [{"serving_rate_id": "r1"}],
        "serving_rate_compact_rows": [{"serving_rate_id": "cr1"}],
        "provider_set_rows": [{"provider_set_hash": "ps1"}],
        "price_set_rows": [{"price_set_hash": "pr1"}],
        "provider_set_component_rows": [{"provider_set_hash": "ps1", "provider_group_hash": 10}],
        "provider_group_member_rows": [{"provider_group_hash": 10, "npi": 123}],
        "procedure_rows": [{"procedure_hash": "p1"}],
    }
    rows_b = {
        "serving_rows": [{"serving_rate_id": "r1"}, {"serving_rate_id": "r2"}],
        "serving_rate_compact_rows": [{"serving_rate_id": "cr1"}, {"serving_rate_id": "cr2"}],
        "provider_set_rows": [{"provider_set_hash": "ps1"}, {"provider_set_hash": "ps2"}],
        "price_set_rows": [{"price_set_hash": "pr1"}, {"price_set_hash": "pr2"}],
        "provider_set_component_rows": [
            {"provider_set_hash": "ps1", "provider_group_hash": 10},
            {"provider_set_hash": "ps2", "provider_group_hash": 11},
        ],
        "provider_group_member_rows": [
            {"provider_group_hash": 10, "npi": 123},
            {"provider_group_hash": 11, "npi": 456},
        ],
        "procedure_rows": [{"procedure_hash": "p1"}, {"procedure_hash": "p2"}],
    }
    it = iter([rows_a, rows_b])
    monkeypatch.setattr(process_ptg, "_serving_only_worker_process", lambda _payload: next(it))

    merged = process_ptg._serving_only_worker_process_chunk([b"a", b"b"])

    assert [row["serving_rate_id"] for row in merged["serving_rows"]] == ["r1", "r2"]
    assert [row["serving_rate_id"] for row in merged["serving_rate_compact_rows"]] == ["cr1", "cr2"]
    assert [row["provider_set_hash"] for row in merged["provider_set_rows"]] == ["ps1", "ps2"]
    assert [row["price_set_hash"] for row in merged["price_set_rows"]] == ["pr1", "pr2"]
    assert len(merged["provider_set_component_rows"]) == 2
    assert len(merged["provider_group_member_rows"]) == 2
    assert [row["procedure_hash"] for row in merged["procedure_rows"]] == ["p1", "p2"]


def test_ptg2_serving_only_worker_chunk_to_files_dedupes_and_iterates(monkeypatch):
    rows_a = {
        "serving_rows": [{"serving_rate_id": "r1"}],
        "provider_set_component_rows": [{"provider_set_hash": "ps1", "provider_group_hash": 10}],
    }
    rows_b = {
        "serving_rows": [{"serving_rate_id": "r1"}, {"serving_rate_id": "r2"}],
        "provider_set_component_rows": [
            {"provider_set_hash": "ps1", "provider_group_hash": 10},
            {"provider_set_hash": "ps1", "provider_group_hash": 11},
        ],
    }
    it = iter([rows_a, rows_b])
    monkeypatch.setattr(process_ptg, "_serving_only_worker_process", lambda _payload: next(it))

    result = process_ptg._serving_only_worker_process_chunk_to_files([b"a", b"b"])
    temp_dir = Path(result["temp_dir"])
    try:
        assert result["__worker_result_files__"] is True
        assert result["counts"]["serving_rows"] == 2
        assert result["counts"]["provider_set_component_rows"] == 2
        serving_rows = list(process_ptg._iter_worker_result_rows(result["paths"]["serving_rows"]))
        component_rows = list(process_ptg._iter_worker_result_rows(result["paths"]["provider_set_component_rows"]))
        assert [row["serving_rate_id"] for row in serving_rows] == ["r1", "r2"]
        assert [row["provider_group_hash"] for row in component_rows] == [10, 11]
    finally:
        for path in result.get("paths", {}).values():
            Path(path).unlink(missing_ok=True)
        temp_dir.rmdir()


def test_ptg2_provider_reference_cache_round_trips_numeric_and_string_refs(tmp_path):
    cache = process_ptg.PTG2ProviderReferenceCache(tmp_path / "provider_refs.sqlite")
    try:
        cache.put(123, [{"__hash__": 42, "npi": [1], "provider_group_id": 123}])
        cache.put("abc", [{"__hash__": 43, "npi": [2], "provider_group_id": "abc"}])
        cache.commit()

        assert cache.get("123")[0]["npi"] == [1]
        assert cache.get("abc")[0]["npi"] == [2]
        assert cache.provider_hashes == {42, 43}
    finally:
        cache.close()


def test_ptg2_provider_reference_cache_uses_bounded_memory_lru(tmp_path, monkeypatch):
    monkeypatch.setenv(process_ptg.PTG2_PROVIDER_CACHE_MEMORY_REFS_ENV, "1")
    cache = process_ptg.PTG2ProviderReferenceCache(tmp_path / "provider_refs.sqlite")
    try:
        cache.put(1, [{"__hash__": 1, "npi": [1]}])
        cache.put(2, [{"__hash__": 2, "npi": [2]}])
        cache.commit()

        assert cache.get(2)[0]["npi"] == [2]
        assert cache.get(1)[0]["npi"] == [1]

        stats = cache.stats()
        assert stats["provider_cache_memory_limit"] == 1
        assert stats["provider_cache_memory_size"] == 1
        assert stats["provider_cache_gets"] == 2
        assert stats["provider_cache_sqlite_hits"] == 1
        assert stats["provider_cache_memory_hits"] == 1
    finally:
        cache.close()


def test_ptg2_in_memory_provider_reference_cache_tracks_hits():
    cache = process_ptg.PTG2InMemoryProviderReferenceCache()

    cache.put(123, [{"__hash__": 42, "npi": [1], "provider_group_id": 123}])

    assert cache.get("123")[0]["npi"] == [1]
    assert cache.get("missing") == []
    assert cache.provider_hashes == {42}
    stats = cache.stats()
    assert stats["provider_cache_memory_hits"] == 1
    assert stats["provider_cache_misses"] == 1
    assert stats["provider_cache_sqlite_hits"] == 0


def test_ptg2_provider_combo_cache_is_order_insensitive_and_bounded():
    cache = process_ptg.OrderedDict()
    stats = {
        "provider_combo_cache_gets": 0,
        "provider_combo_cache_hits": 0,
        "provider_combo_cache_misses": 0,
        "provider_combo_cache_size": 0,
        "provider_combo_cache_limit": 1,
    }
    key_a = process_ptg._provider_combo_cache_key([2, "1", 1])
    key_b = process_ptg._provider_combo_cache_key(["1", 2])

    assert key_a == key_b
    assert process_ptg._provider_combo_cache_get(cache, key_a, stats) is None
    process_ptg._provider_combo_cache_put(cache, key_a, {"__hash__": 1}, stats, limit=1)
    assert process_ptg._provider_combo_cache_get(cache, key_b, stats)["__hash__"] == 1
    process_ptg._provider_combo_cache_put(cache, ("3",), {"__hash__": 3}, stats, limit=1)

    assert key_a not in cache
    assert stats["provider_combo_cache_hits"] == 1
    assert stats["provider_combo_cache_misses"] == 1


def test_ptg2_worker_capacity_waits_on_batch_count_or_bytes():
    assert process_ptg._ptg2_worker_capacity_wait_needed(
        pending_count=2,
        pending_input_bytes=8,
        next_batch_bytes=1,
        max_pending_batches=2,
        max_pending_bytes=100,
    )
    assert process_ptg._ptg2_worker_capacity_wait_needed(
        pending_count=1,
        pending_input_bytes=8,
        next_batch_bytes=4,
        max_pending_batches=4,
        max_pending_bytes=10,
    )
    assert not process_ptg._ptg2_worker_capacity_wait_needed(
        pending_count=0,
        pending_input_bytes=0,
        next_batch_bytes=20,
        max_pending_batches=4,
        max_pending_bytes=10,
    )
    assert not process_ptg._ptg2_worker_capacity_wait_needed(
        pending_count=1,
        pending_input_bytes=4,
        next_batch_bytes=4,
        max_pending_batches=4,
        max_pending_bytes=10,
    )


def test_ptg2_fast_provider_union_carries_count_without_npi_materialization(monkeypatch):
    monkeypatch.setenv(process_ptg.PTG2_FAST_PROVIDER_UNION_ENV, "true")

    combined_entry, _row = process_ptg._combine_provider_set_entries(
        file_id=1,
        entries=[
            {
                "__hash__": 11,
                "npi": [1, 2],
                "provider_count": 2,
                "provider_group_hashes": [11],
            },
            {
                "__hash__": 22,
                "npi": [2, 3],
                "provider_count": 2,
                "provider_group_hashes": [22],
            },
        ],
    )

    provider_set_row = process_ptg._ptg2_provider_set_row(combined_entry)

    assert combined_entry["npi"] == []
    assert combined_entry["provider_count"] == 4
    assert provider_set_row["provider_count"] == 4
    assert provider_set_row["npi"] is None
    assert provider_set_row["canonical_payload"]["provider_count_mode"] == "summed_provider_groups"


def test_ptg2_serving_only_provider_set_uses_real_provider_group_hashes():
    provider_ref = {
        "provider_group_id": 7,
        "provider_groups": [
            {"tin": {"type": "ein", "value": "12-3456789"}, "npi": [1111111111, 2222222222]},
            {"tin": {"type": "ein", "value": "98-7654321"}, "npi": [3333333333]},
        ],
    }
    provider_entry, _row = process_ptg._build_provider_set_entry(
        file_id=1,
        provider_group_ref=provider_ref["provider_group_id"],
        provider_groups=provider_ref["provider_groups"],
        network_names=["C2"],
    )
    assert provider_entry is not None
    expected_group_hashes = sorted(provider_entry["provider_group_hashes"])
    synthetic_entry_hash = int(provider_entry["__hash__"])
    assert synthetic_entry_hash not in expected_group_hashes

    result = process_ptg._serving_only_rows_for_payload(
        {
            "billing_code_type": "CPT",
            "billing_code": "99213",
            "negotiated_rates": [
                {
                    "provider_references": [7],
                    "negotiated_prices": [{"negotiated_type": "negotiated", "negotiated_rate": 100}],
                }
            ],
        },
        provider_map={7: [provider_entry]},
        plan_fields={"plan_id": "plan", "plan_market_type": "group"},
        snapshot_id="snapshot",
        plan_month_id="snapshot-month",
        source_trace_payload=[],
        compact_serving=True,
        source_trace_set_hash="source-trace",
        include_price_set_rows=True,
    )

    provider_set = result["provider_set_rows"][0]
    serving_row = result["serving_rows"][0]
    compact_row = result["serving_rate_compact_rows"][0]
    assert provider_set["provider_set_hash"] == process_ptg._serving_only_hash_int_sets(
        "serving_provider_set",
        expected_group_hashes,
    )
    assert serving_row["network_names"] == ["C2"]
    assert compact_row["network_names"] == ["C2"]


def test_ptg2_single_pass_in_network_parser_uses_provider_cache(tmp_path, monkeypatch):
    """Verify this PTG import regression contract."""
    raw_path = tmp_path / "rates.json.gz"
    payload = {
        "provider_references": [
            {
                "provider_group_id": 7,
                "provider_groups": [{"tin": {"type": "ein", "value": "123"}, "npi": ["1234567890"]}],
            }
        ],
        "in_network": [
            {
                "negotiation_arrangement": "ffs",
                "name": "Office visit",
                "billing_code_type": "CPT",
                "billing_code": "99213",
                "negotiated_rates": [
                    {
                        "provider_references": [7],
                        "negotiated_prices": [
                            {
                                "negotiated_type": "negotiated",
                                "negotiated_rate": 12.34,
                                "expiration_date": "2026-12-31",
                                "billing_class": "professional",
                                "service_code": ["11"],
                            }
                        ],
                    }
                ],
            }
        ],
    }
    with gzip.open(raw_path, "wb") as fp:
        fp.write(json.dumps(payload).encode("utf-8"))

    pushed = {}

    async def fake_push(rows, cls, **_kwargs):
        pushed.setdefault(cls, []).extend(rows)

    monkeypatch.setattr(process_ptg, "push_objects", fake_push)
    monkeypatch.setattr(process_ptg, "flush_error_log", AsyncMock())
    monkeypatch.setattr(process_ptg, "log_error", AsyncMock())

    cache = process_ptg.PTG2ProviderReferenceCache(tmp_path / "provider_refs.sqlite")
    classes = {
        "PTGProviderGroup": "providers",
        "PTGInNetworkItem": "items",
        "PTGBillingCode": "billing",
        "PTGNegotiatedRate": "rates",
        "PTGNegotiatedPrice": "prices",
    }
    try:
        asyncio.run(
            process_ptg._parse_in_network_file_single_pass(
                str(raw_path),
                99,
                {"plan_id": "TESTPLAN001"},
                None,
                cache,
                classes,
                False,
                "log",
                "file://rates.json.gz",
            )
        )
    finally:
        cache.close()

    assert pushed["providers"][0]["npi"] == [1234567890]
    assert pushed["items"][0]["billing_code"] == "99213"
    assert pushed["rates"][0]["provider_group_hash"] == pushed["providers"][0]["provider_group_hash"]
    assert pushed["prices"][0]["negotiated_rate"] == 12.34
    process_ptg.log_error.assert_not_awaited()


def test_ptg2_stream_logical_artifact_handles_gzip_without_loading_all(tmp_path):
    raw_path = tmp_path / "toc.json.gz"
    expected = b'{"reporting_structure":[]}'
    with gzip.open(raw_path, "wb") as fp:
        fp.write(expected)

    logical = process_ptg.stream_logical_artifact(raw_path, output_dir=tmp_path)

    assert logical.compression == "gzip"
    assert Path(logical.logical_path).read_bytes() == expected
    assert logical.logical_sha256 == process_ptg.sha256_bytes(expected)


def test_ptg2_logical_identity_streams_gzip_without_materializing_json(tmp_path):
    raw_path = tmp_path / "rates.json.gz"
    expected = b'{"in_network":[]}'
    with gzip.open(raw_path, "wb") as fp:
        fp.write(expected)

    logical = process_ptg.logical_artifact_identity(raw_path)

    assert logical.compression == "gzip"
    assert logical.logical_path == str(raw_path)
    assert logical.logical_sha256 == process_ptg.sha256_bytes(expected)
    assert not list(tmp_path.glob("*_logical.json"))
    with process_ptg.open_json_artifact_stream(raw_path) as fp:
        assert fp.read() == expected


@pytest.mark.parametrize("container", ["plain", "gzip", "zip"])
def test_ptg2_json_artifact_stream_strips_utf8_bom(tmp_path, container):
    expected = b'{"in_network":[]}'
    payload = b"\xef\xbb\xbf" + expected
    if container == "plain":
        raw_path = tmp_path / "rates.json"
        raw_path.write_bytes(payload)
    elif container == "gzip":
        raw_path = tmp_path / "rates.json.gz"
        with gzip.open(raw_path, "wb") as gzip_fp:
            gzip_fp.write(payload)
    else:
        raw_path = tmp_path / "rates.zip"
        with zipfile.ZipFile(raw_path, "w", compression=zipfile.ZIP_DEFLATED) as zip_ref:
            zip_ref.writestr("rates.json", payload)

    with process_ptg.open_json_artifact_stream(raw_path) as json_fp:
        assert json_fp.read(1) == expected[:1]
        assert json_fp.read() == expected[1:]


def test_ptg2_bom_reader_zero_length_reads_do_not_consume_source():
    expected = b'{"in_network":[]}'
    source = io.BytesIO(b"\xef\xbb\xbf" + expected)
    reader = ptg_artifact_streams._Utf8BomSkippingReader(source)

    assert reader.read(0) == b""
    assert reader.readinto(bytearray()) == 0
    assert source.tell() == 0
    assert reader.read() == expected


@pytest.mark.parametrize("container", ["plain", "gzip", "zip"])
def test_ptg2_bom_logical_identity_matches_materialized_artifact(tmp_path, container):
    expected = b'{"in_network":[]}'
    payload = b"\xef\xbb\xbf" + expected
    if container == "plain":
        raw_path = tmp_path / "rates.json"
        raw_path.write_bytes(payload)
    elif container == "gzip":
        raw_path = tmp_path / "rates.json.gz"
        with gzip.open(raw_path, "wb") as gzip_fp:
            gzip_fp.write(payload)
    else:
        raw_path = tmp_path / "rates.zip"
        with zipfile.ZipFile(raw_path, "w", compression=zipfile.ZIP_DEFLATED) as zip_ref:
            zip_ref.writestr("rates.json", payload)

    identified = process_ptg.logical_artifact_identity(raw_path)
    materialized = process_ptg.stream_logical_artifact(raw_path, output_dir=tmp_path / "logical")

    assert identified.logical_sha256 == process_ptg.sha256_bytes(expected)
    assert materialized.logical_sha256 == identified.logical_sha256
    assert materialized.byte_count == identified.byte_count == len(expected)
    with process_ptg.open_json_artifact_stream(materialized.logical_path) as logical_fp:
        assert logical_fp.read() == expected


def test_ptg2_metadata_extraction_accepts_utf8_bom(tmp_path):
    raw_path = tmp_path / "rates.json"
    raw_path.write_bytes(
        b'\xef\xbb\xbf{"reporting_entity_name":"Example Administrator",'
        b'"reporting_entity_type":"third-party administrator",'
        b'"last_updated_on":"2026-06-01",'
        b'"version":"1.0",'
        b'"in_network":[]}'
    )

    metadata = asyncio.run(process_ptg._extract_metadata_fields(str(raw_path)))

    assert metadata["reporting_entity_name"] == "Example Administrator"
    assert metadata["reporting_entity_type"] == "third-party administrator"
    assert metadata["last_updated_on"] == "2026-06-01"
    assert metadata["version"] == "1.0"


def test_ptg2_open_json_artifact_stream_reads_deflate64_zip(tmp_path):
    raw_path = tmp_path / "rates.zip"
    expected = b'{"in_network":[{"negotiated_rates":[]}]}'
    _write_deflate64_zip(raw_path, "nested/rates.json", expected)

    with process_ptg.open_json_artifact_stream(raw_path) as fp:
        assert fp.read(5) == expected[:5]
        assert fp.read() == expected[5:]


def test_ptg2_stream_logical_artifact_extracts_deflate64_zip(tmp_path):
    raw_path = tmp_path / "rates.zip"
    expected = b'{"in_network":[]}'
    _write_deflate64_zip(raw_path, "nested/rates.json", expected)

    logical = process_ptg.stream_logical_artifact(raw_path, output_dir=tmp_path / "logical")

    assert logical.compression == "zip"
    assert logical.member_name == "nested/rates.json"
    assert Path(logical.logical_path).read_bytes() == expected
    assert logical.logical_sha256 == process_ptg.sha256_bytes(expected)


def test_ptg2_materialize_json_source_extracts_zip_when_logical_deferral_requested(tmp_path, monkeypatch):
    raw_path = tmp_path / "rates.zip"
    expected = b'{"in_network":[]}'
    with zipfile.ZipFile(raw_path, "w", compression=zipfile.ZIP_DEFLATED) as zip_ref:
        zip_ref.writestr("nested/rates.json", expected)
    raw_sha256, byte_count = process_ptg.sha256_file(raw_path)

    async def fake_download_raw_artifact(url, **_kwargs):
        return process_ptg.PTG2RawArtifact(
            original_url=url,
            canonical_url=url,
            raw_path=str(raw_path),
            raw_storage_uri=f"file://{raw_path}",
            raw_sha256=raw_sha256,
            byte_count=byte_count,
        )

    monkeypatch.setattr(ptg_source_download, "download_raw_artifact", fake_download_raw_artifact)

    _raw, logical = asyncio.run(
        process_ptg.materialize_json_source(
            "https://example.test/rates.zip",
            tmp_path / "logical",
            materialize_logical=False,
        )
    )

    assert logical.compression == "zip"
    assert logical.member_name == "nested/rates.json"
    assert logical.logical_path != str(raw_path)
    assert Path(logical.logical_path).read_bytes() == expected


def test_ptg2_ensure_tables_uses_existing_db_create_table(monkeypatch):
    created = []

    async def fake_status(_statement):
        return None

    async def fake_create_table(table, **_kwargs):
        created.append(table.name)

    monkeypatch.setattr(process_ptg.db, "status", fake_status)
    monkeypatch.setattr(process_ptg.db, "create_table", fake_create_table)

    asyncio.run(process_ptg.ensure_ptg2_tables())

    assert "ptg2_import_run" in created
    assert "ptg2_current_snapshot" in created
    assert "ptg2_source_file_version" in created
    expected = {cls.__table__.name for cls in process_ptg.PTG2_MODEL_CLASSES}
    assert expected.issubset(set(created))


def test_ptg2_ensure_tables_fails_fast_on_create_error(monkeypatch):
    async def fake_status(_statement):
        return None

    async def fake_create_table(table, **_kwargs):
        if table.name == "ptg2_snapshot":
            raise RuntimeError("no permission")

    monkeypatch.setattr(process_ptg.db, "status", fake_status)
    monkeypatch.setattr(process_ptg.db, "create_table", fake_create_table)

    with pytest.raises(RuntimeError, match="ptg2_snapshot"):
        asyncio.run(process_ptg.ensure_ptg2_tables())


def test_ptg2_ensure_indexes_skips_duplicate_primary_unique_index(monkeypatch):
    statements = []

    async def fake_status(statement):
        statements.append(statement)

    async def is_table_present(_statement, **_params):
        return True

    model = SimpleNamespace(
        __tablename__="example_ptg_table",
        __my_index_elements__=["id"],
        __my_additional_indexes__=[],
        __table__=SimpleNamespace(primary_key=[SimpleNamespace(name="id")]),
    )
    monkeypatch.setattr(process_ptg.db, "scalar", is_table_present)
    monkeypatch.setattr(process_ptg.db, "status", fake_status)

    asyncio.run(process_ptg._ensure_indexes(model, "mrf"))

    assert not any("example_ptg_table_idx_primary" in statement for statement in statements)


def test_ptg2_ensure_indexes_ignores_concurrent_create_index_race(monkeypatch):
    async def fake_status(_statement):
        raise RuntimeError(
            'duplicate key value violates unique constraint "pg_class_relname_nsp_index" '
            "DETAIL: Key (relname, relnamespace)=(example_payment_idx, 123) already exists."
        )

    async def is_table_present(_statement, **_params):
        return True

    model = SimpleNamespace(
        __tablename__="example_ptg_table",
        __my_index_elements__=[],
        __my_additional_indexes__=[
            {"name": "example_payment_idx", "index_elements": ("payment_hash",)},
        ],
        __table__=SimpleNamespace(primary_key=[]),
    )
    monkeypatch.setattr(process_ptg.db, "scalar", is_table_present)
    monkeypatch.setattr(process_ptg.db, "status", fake_status)

    asyncio.run(process_ptg._ensure_indexes(model, "mrf"))


def test_ptg2_ensure_indexes_skips_missing_optional_table(monkeypatch):
    statements = []

    async def is_table_present(_statement, **_params):
        return False

    async def fake_status(statement):
        statements.append(statement)

    model = SimpleNamespace(
        __tablename__="missing_ptg_table",
        __my_index_elements__=[],
        __my_additional_indexes__=[
            {"name": "missing_ptg_table_idx", "index_elements": ("payment_hash",)},
        ],
        __table__=SimpleNamespace(primary_key=[]),
    )
    monkeypatch.setattr(process_ptg.db, "scalar", is_table_present)
    monkeypatch.setattr(process_ptg.db, "status", fake_status)

    asyncio.run(process_ptg._ensure_indexes(model, "mrf"))

    assert statements == []


def test_ptg2_in_network_download_failure_returns_failed_result(monkeypatch, tmp_path):
    async def fake_materialize(*_args, **_kwargs):
        raise RuntimeError("download failed")

    monkeypatch.setattr(process_ptg, "ptg2_temp_parent", lambda: tmp_path)
    monkeypatch.setattr(process_ptg, "materialize_json_source", fake_materialize)

    result = asyncio.run(
        process_ptg._process_in_network_file(
            {"type": "in_network", "url": "https://example.test/rates.json.gz"},
            {"PTGFile": "files", "ImportLog": "log"},
            {},
            False,
        )
    )

    assert result.success is False
    assert result.source_type == "in_network"
    assert result.url == "https://example.test/rates.json.gz"
    assert "download failed" in result.error


def test_ptg2_in_network_serving_only_zero_rows_returns_skipped_result(monkeypatch, tmp_path):
    artifact = tmp_path / "rates.json.gz"
    artifact.write_bytes(b"{}")
    raw_artifact = process_ptg.PTG2RawArtifact(
        original_url="https://example.test/rates.json.gz",
        canonical_url="https://example.test/rates.json.gz",
        raw_path=str(artifact),
        raw_storage_uri=str(artifact),
        raw_sha256="raw",
        byte_count=2,
        head=None,
    )
    logical_artifact = process_ptg.PTG2LogicalArtifact(
        logical_path=str(artifact),
        logical_sha256="logical",
        byte_count=2,
    )

    async def fake_materialize(*_args, **_kwargs):
        return raw_artifact, logical_artifact

    async def fake_source_version(**_kwargs):
        return None

    async def fake_parse(*_args, **_kwargs):
        return {
            "serving_only": True,
            "rust_compact_serving": True,
            "rust_records": 0,
            "serving_rates": 0,
        }

    monkeypatch.setenv(process_ptg.PTG2_SERVING_ONLY_IMPORT_ENV, "true")
    monkeypatch.setattr(process_ptg, "ptg2_temp_parent", lambda: tmp_path)
    monkeypatch.setattr(process_ptg, "materialize_json_source", fake_materialize)
    monkeypatch.setattr(process_ptg, "_extract_metadata_fields", AsyncMock(return_value={}))
    monkeypatch.setattr(process_ptg, "_push_ptg2_objects", AsyncMock())
    monkeypatch.setattr(process_ptg, "_record_source_version", fake_source_version)
    monkeypatch.setattr(process_ptg, "_parse_in_network_file_serving_only", fake_parse)

    result = asyncio.run(
        process_ptg._process_in_network_file(
            {"type": "in_network", "url": "https://example.test/rates.json.gz"},
            {"PTGFile": SimpleNamespace(__name__="PTGFile"), "ImportLog": "log"},
            {},
            False,
            compact_import=True,
        )
    )

    assert result.success is True
    assert result.skipped is True
    assert result.error is None
    assert result.summary["serving_rates"] == 0
    assert result.summary["skipped_reason"] == "parsed zero serving rates"


def test_ptg2_downloaded_jobs_are_prefetched_concurrently(monkeypatch):
    started = []

    def fake_download(job, **_kwargs):
        started.append(job["url"])
        if "slow" in job["url"]:
            time.sleep(0.05)
        return process_ptg.PTG2DownloadedJob(job=job)

    async def collect():
        results = []
        async for downloaded in process_ptg._iter_downloaded_ptg_jobs(
            [
                {"type": "in_network", "url": "https://example.test/slow.json.gz"},
                {"type": "in_network", "url": "https://example.test/fast.json.gz"},
            ],
            reuse_raw_artifacts=True,
            max_bytes=None,
            keep_partial_artifacts=None,
        ):
            results.append(downloaded.job["url"])
        return results

    monkeypatch.setenv(process_ptg.PTG2_DOWNLOAD_TASKS_ENV, "2")
    monkeypatch.setattr(process_ptg, "_download_ptg_job_artifact_sync", fake_download)

    yielded = asyncio.run(collect())

    assert started[:2] == [
        "https://example.test/slow.json.gz",
        "https://example.test/fast.json.gz",
    ]
    assert yielded[0] == "https://example.test/fast.json.gz"
    assert yielded[1] == "https://example.test/slow.json.gz"


def test_ptg2_main_processes_downloaded_files_concurrently_when_enabled(monkeypatch):
    """Verify this PTG import regression contract."""
    pushed = []
    concurrency_map = {"active": 0, "max_active": 0}
    processed_jobs = []

    async def fake_push(rows, cls, **_kwargs):
        pushed.extend((getattr(cls, "__name__", str(cls)), row) for row in rows)

    async def fake_toc(*_args, **_kwargs):
        return [
            {"type": "in_network", "url": "https://example.test/rates-a.json.gz"},
            {"type": "in_network", "url": "https://example.test/rates-b.json.gz"},
        ]

    async def fake_downloaded_jobs(jobs, **_kwargs):
        for job in jobs:
            yield process_ptg.PTG2DownloadedJob(
                job=job,
                raw_artifact=SimpleNamespace(
                    raw_sha256=str(job["url"]),
                    raw_storage_uri=f"/tmp/{Path(job['url']).name}",
                ),
                logical_artifact=SimpleNamespace(logical_path=f"/tmp/{Path(job['url']).name}"),
            )

    async def fake_process(job, *_args, **_kwargs):
        processed_jobs.append(dict(job))
        concurrency_map["active"] += 1
        concurrency_map["max_active"] = max(concurrency_map["max_active"], concurrency_map["active"])
        await asyncio.sleep(0.05)
        concurrency_map["active"] -= 1
        return process_ptg.PTG2FileProcessResult(
            "in_network",
            job["url"],
            True,
            file_id=len(job["url"]),
            summary={"serving_rates": 1, "manifest": {"copy_files": {}}},
        )

    async def fake_publish(*_args, **_kwargs):
        return {
            "storage": "manifest_snapshot",
            "type": "ptg2_manifest_serving",
            "rate_count": 2,
            "serving_rates": 2,
        }

    refresh_mock = AsyncMock(return_value={"status": "queued", "run_id": "run-refresh"})

    monkeypatch.setattr(process_ptg, "ensure_database", AsyncMock())
    monkeypatch.setattr(process_ptg, "ensure_ptg2_tables", AsyncMock())
    monkeypatch.setattr(process_ptg.db, "status", AsyncMock())
    monkeypatch.setattr(process_ptg, "_push_ptg2_objects", fake_push)
    monkeypatch.setattr(process_ptg, "_prepare_ptg_tables", AsyncMock(return_value={"ImportLog": "log"}))
    monkeypatch.setattr(process_ptg, "_create_ptg2_manifest_serving_stage_table", AsyncMock(return_value="manifest_stage"))
    monkeypatch.setattr(process_ptg, "_process_table_of_contents", fake_toc)
    monkeypatch.setattr(process_ptg, "_iter_downloaded_ptg_jobs", fake_downloaded_jobs)
    monkeypatch.setattr(process_ptg, "_process_in_network_file", fake_process)
    monkeypatch.setattr(process_ptg, "flush_error_log", AsyncMock())
    monkeypatch.setattr(process_ptg, "_publish_ptg2_manifest_serving_snapshot", fake_publish)
    monkeypatch.setattr(process_ptg, "_current_source_snapshot_id", AsyncMock(return_value=None))
    monkeypatch.setattr(process_ptg, "_publish_ptg2_source_pointers", AsyncMock())
    monkeypatch.setattr(process_ptg, "_cleanup_old_ptg2_source_tables", AsyncMock())
    monkeypatch.setattr(process_ptg, "_enqueue_ptg2_auto_address_refresh_after_import", refresh_mock)
    monkeypatch.setenv(process_ptg.PTG2_FILE_PROCESS_CONCURRENCY_ENV, "2")

    result = asyncio.run(
        process_ptg.main(
            toc_urls=["https://example.test/index.json"],
            import_month="2026-04",
            import_id="file_concurrency_test",
            source_key="file_concurrency_test",
            source_network_names=["C2"],
        )
    )

    assert concurrency_map["max_active"] in {2}
    assert [job["source_network_names"] for job in processed_jobs] == [["C2"], ["C2"]]
    assert result["files_processed"] == 2
    assert result["address_refresh"] == {"status": "queued", "run_id": "run-refresh"}
    refresh_mock.assert_awaited_once()
    refresh_kwargs = refresh_mock.await_args.kwargs
    assert refresh_kwargs["source_key"] == "file_concurrency_test"
    assert refresh_kwargs["snapshot_id"] == result["snapshot_id"]
    assert refresh_kwargs["import_run_id"] == "ptg2:file_concurrency_test"
    assert refresh_kwargs["has_serving_files"] is True
    assert refresh_kwargs["source_scoped_compact"] is True
    assert refresh_kwargs["test_mode"] is False
    import_run_rows = [row for cls_name, row in pushed if cls_name == "PTG2ImportRun"]
    assert import_run_rows[-1]["report"]["files_processed"] == 2
    assert import_run_rows[-1]["report"]["address_refresh"] == {"status": "queued", "run_id": "run-refresh"}


def test_ptg2_main_marks_claim_failed_when_import_run_start_fails(monkeypatch):
    pushed_entries = []
    final_table_cleanup = AsyncMock()
    create_stage = AsyncMock()

    async def push(object_entries, cls, **_kwargs):
        pushed_entries.extend((cls, entry) for entry in object_entries)
        entry = object_entries[0]
        if cls is process_ptg.PTG2Snapshot and entry["status"] == process_ptg.PTG2_STATUS_BUILDING:
            return {**entry, "snapshot_claim_status": "acquired"}
        if cls is process_ptg.PTG2ImportRun and entry["status"] == process_ptg.PTG2_STATUS_RUNNING:
            raise RuntimeError("import run write failed")
        return None

    monkeypatch.setattr(process_ptg, "ensure_database", AsyncMock())
    monkeypatch.setattr(process_ptg, "ensure_ptg2_tables", AsyncMock())
    monkeypatch.setattr(process_ptg, "_current_source_snapshot_id", AsyncMock(return_value="snap_previous"))
    monkeypatch.setattr(process_ptg, "_push_ptg2_objects", push)
    monkeypatch.setattr(process_ptg, "_drop_ptg2_snapshot_tables_for_manifest", final_table_cleanup)
    monkeypatch.setattr(process_ptg, "_drop_ptg2_snapshot_table_names", AsyncMock())
    monkeypatch.setattr(process_ptg, "_create_ptg2_manifest_serving_stage_table", create_stage)

    with pytest.raises(RuntimeError, match="import run write failed"):
        asyncio.run(
            process_ptg.main(
                in_network_url="https://example.test/rates.json.gz",
                import_month="2026-07",
                import_id="import_run_start_failure",
                source_key="source_a",
            )
        )

    snapshot_entries = [
        entry for cls, entry in pushed_entries if cls is process_ptg.PTG2Snapshot
    ]
    assert [entry["status"] for entry in snapshot_entries] == [
        process_ptg.PTG2_STATUS_BUILDING,
        process_ptg.PTG2_STATUS_FAILED,
    ]
    final_table_cleanup.assert_awaited_once_with(None)
    create_stage.assert_not_awaited()


def test_ptg2_main_cleans_complete_stage_family_when_stage_creation_fails(monkeypatch):
    pushed_entries = []
    stage_cleanup = AsyncMock()

    async def push(object_entries, cls, **_kwargs):
        pushed_entries.extend((cls, entry) for entry in object_entries)
        entry = object_entries[0]
        if cls is process_ptg.PTG2Snapshot and entry["status"] == process_ptg.PTG2_STATUS_BUILDING:
            return {**entry, "snapshot_claim_status": "acquired"}
        return None

    monkeypatch.setattr(process_ptg, "ensure_database", AsyncMock())
    monkeypatch.setattr(process_ptg, "ensure_ptg2_tables", AsyncMock())
    monkeypatch.setattr(process_ptg, "_current_source_snapshot_id", AsyncMock(return_value=None))
    monkeypatch.setattr(process_ptg, "_push_ptg2_objects", push)
    monkeypatch.setattr(process_ptg, "_drop_ptg2_snapshot_tables_for_manifest", AsyncMock())
    monkeypatch.setattr(process_ptg, "_drop_ptg2_snapshot_table_names", stage_cleanup)
    monkeypatch.setattr(
        process_ptg,
        "_create_ptg2_manifest_serving_stage_table",
        AsyncMock(side_effect=RuntimeError("stage creation failed")),
    )

    with pytest.raises(RuntimeError, match="stage creation failed"):
        asyncio.run(
            process_ptg.main(
                in_network_url="https://example.test/rates.json.gz",
                import_month="2026-07",
                import_id="stage_creation_failure",
                source_key="source_a",
            )
        )

    stage_table_names = stage_cleanup.await_args.args[0]
    assert len(stage_table_names) == 7
    assert stage_table_names[1:] == process_ptg._ptg2_manifest_stage_table_names(
        stage_table_names[0]
    )[1:]
    snapshot_entries = [
        entry for cls, entry in pushed_entries if cls is process_ptg.PTG2Snapshot
    ]
    assert snapshot_entries[-1]["status"] == process_ptg.PTG2_STATUS_FAILED


def test_ptg2_main_marks_failed_when_all_discovered_jobs_fail(monkeypatch):
    pushed = []

    async def fake_push(rows, cls, **_kwargs):
        pushed.extend((getattr(cls, "__name__", str(cls)), row) for row in rows)

    async def fake_downloaded_jobs(jobs, **_kwargs):
        for job in jobs:
            yield process_ptg.PTG2DownloadedJob(
                job=job,
                raw_artifact=SimpleNamespace(raw_sha256=str(job.get("url") or "")),
                logical_artifact=SimpleNamespace(logical_path="/tmp/rates.json.gz"),
            )

    async def fake_process(*_args, **_kwargs):
        return process_ptg.PTG2FileProcessResult(
            "in_network",
            "https://example.test/rates.json.gz",
            False,
            error="download failed",
        )

    monkeypatch.setattr(process_ptg, "ensure_database", AsyncMock())
    monkeypatch.setattr(process_ptg, "ensure_ptg2_tables", AsyncMock())
    monkeypatch.setattr(process_ptg, "_current_source_snapshot_id", AsyncMock(return_value=None))
    monkeypatch.setattr(process_ptg, "_push_ptg2_objects", fake_push)
    monkeypatch.setattr(process_ptg, "_prepare_ptg_tables", AsyncMock(return_value={"ImportLog": "log"}))
    monkeypatch.setattr(process_ptg, "_create_ptg2_manifest_serving_stage_table", AsyncMock(return_value="manifest_stage"))
    monkeypatch.setattr(process_ptg, "_iter_downloaded_ptg_jobs", fake_downloaded_jobs)
    monkeypatch.setattr(process_ptg, "_process_in_network_file", fake_process)
    monkeypatch.setattr(process_ptg, "flush_error_log", AsyncMock())
    monkeypatch.setattr(process_ptg, "build_ptg2_snapshot_index_artifact", AsyncMock())
    monkeypatch.setenv(process_ptg.PTG2_COMPACT_IMPORT_ENV, "false")

    with pytest.raises(RuntimeError, match="failed 1 of 1 attempted file"):
        asyncio.run(
            process_ptg.main(
                in_network_url="https://example.test/rates.json.gz",
                import_month="2026-04",
                import_id="state_machine_test",
                source_key="state_machine_test",
            )
        )

    import_run_rows = [row for cls_name, row in pushed if cls_name == "PTG2ImportRun"]
    snapshot_rows = [row for cls_name, row in pushed if cls_name == "PTG2Snapshot"]
    current_rows = [row for cls_name, row in pushed if cls_name == "PTG2CurrentSnapshot"]

    assert import_run_rows[-1]["status"] == process_ptg.PTG2_STATUS_FAILED
    assert import_run_rows[-1]["report"]["files_processed"] in {0}
    assert import_run_rows[-1]["report"]["files_failed"] in {1}
    assert "download failed" in import_run_rows[-1]["report"]["failed_files"][0]["error"]
    assert snapshot_rows[-1]["status"] == process_ptg.PTG2_STATUS_FAILED
    assert current_rows == []


def test_ptg2_main_publishes_allowed_amount_only_metadata_snapshot(monkeypatch):
    """Verify this PTG import regression contract."""
    pushed = []
    dropped_tables = []
    publish_serving = AsyncMock()
    publish_source_pointers = AsyncMock()

    async def fake_push(rows, cls, **_kwargs):
        pushed.extend((getattr(cls, "__name__", str(cls)), row) for row in rows)

    async def fake_downloaded_jobs(jobs, **_kwargs):
        for job in jobs:
            yield process_ptg.PTG2DownloadedJob(
                job=job,
                raw_artifact=SimpleNamespace(raw_sha256=str(job.get("url") or "")),
                logical_artifact=SimpleNamespace(logical_path="/tmp/allowed.json.gz"),
            )

    async def fake_allowed(*_args, **_kwargs):
        return process_ptg.PTG2FileProcessResult(
            "allowed_amounts",
            "https://example.test/allowed.json.gz",
            True,
            file_id=123,
        )

    async def fake_drop_tables(names):
        dropped_tables.extend(names)

    monkeypatch.setattr(process_ptg, "ensure_database", AsyncMock())
    monkeypatch.setattr(process_ptg, "ensure_ptg2_tables", AsyncMock())
    monkeypatch.setattr(process_ptg.db, "status", AsyncMock())
    monkeypatch.setattr(process_ptg, "_push_ptg2_objects", fake_push)
    monkeypatch.setattr(process_ptg, "_prepare_ptg_tables", AsyncMock(return_value={"ImportLog": "log"}))
    monkeypatch.setattr(process_ptg, "_ensure_ptg_dynamic_tables", AsyncMock())
    monkeypatch.setattr(process_ptg, "_create_ptg2_manifest_serving_stage_table", AsyncMock(return_value="manifest_stage"))
    monkeypatch.setattr(process_ptg, "_drop_ptg2_snapshot_table_names", fake_drop_tables)
    monkeypatch.setattr(process_ptg, "_iter_downloaded_ptg_jobs", fake_downloaded_jobs)
    monkeypatch.setattr(process_ptg, "_process_allowed_amounts_file", fake_allowed)
    monkeypatch.setattr(process_ptg, "flush_error_log", AsyncMock())
    monkeypatch.setattr(process_ptg, "_publish_ptg2_manifest_serving_snapshot", publish_serving)
    monkeypatch.setattr(process_ptg, "_current_source_snapshot_id", AsyncMock(return_value=None))
    monkeypatch.setattr(process_ptg, "_publish_ptg2_source_pointers", publish_source_pointers)
    monkeypatch.setattr(process_ptg, "_cleanup_old_ptg2_source_tables", AsyncMock())
    monkeypatch.setenv(process_ptg.PTG2_COMPACT_IMPORT_ENV, "false")

    asyncio.run(
        process_ptg.main(
            allowed_url="https://example.test/allowed.json.gz",
            import_month="2026-04",
            import_id="allowed_only_state_machine_test",
            source_key="allowed_only_state_machine_test",
        )
    )

    import_run_rows = [row for cls_name, row in pushed if cls_name == "PTG2ImportRun"]
    snapshot_rows = [row for cls_name, row in pushed if cls_name == "PTG2Snapshot"]
    current_rows = [row for cls_name, row in pushed if cls_name == "PTG2CurrentSnapshot"]
    final_report = import_run_rows[-1]["report"]

    assert import_run_rows[-1]["status"] == process_ptg.PTG2_STATUS_VALIDATED
    assert snapshot_rows[-1]["status"] == process_ptg.PTG2_STATUS_PUBLISHED
    assert final_report["files_processed"] in {1}
    assert final_report["files_failed"] in {0}
    assert final_report["serving_index"]["type"] == "allowed_amounts_only"
    assert final_report["serving_rates"] in {0}
    assert snapshot_rows[-1]["manifest"]["successful_files"][0]["source_type"] == "allowed_amounts"
    assert current_rows == []
    assert publish_serving.await_count in {0}
    assert publish_source_pointers.await_count in {0}
    assert "manifest_stage" in dropped_tables


def test_allowed_amount_metrics_from_results_sum_evidence_counts():
    metrics = process_ptg._allowed_amount_metrics_from_results(
        [
            {
                "source_type": "allowed_amounts",
                "summary": {
                    "allowed_amount_items": 2,
                    "allowed_amount_blocks": 3,
                    "allowed_amount_payments": 4,
                    "allowed_amount_provider_payments": 5,
                    "allowed_amount_npi_references": 8,
                    "allowed_amount_unique_tins": 2,
                },
            },
            {"source_type": "in_network", "summary": {"allowed_amount_items": 99}},
            {
                "source_type": "allowed_amounts",
                "summary": {
                    "allowed_amount_items": 1,
                    "allowed_amount_provider_payments": 7,
                },
            },
        ]
    )

    assert metrics["allowed_amount_items"] == 3
    assert metrics["allowed_amount_blocks"] == 3
    assert metrics["allowed_amount_payments"] == 4
    assert metrics["allowed_amount_provider_payments"] == 12
    assert metrics["allowed_amount_npi_references"] == 8
    assert metrics["allowed_amount_unique_tins"] == 2
    assert metrics["allowed_amount_evidence"] is True


def test_parse_allowed_amounts_filters_null_service_codes(monkeypatch, tmp_path):
    allowed_amount_payload_dict = {
        "out_of_network": [
            {
                "name": "Office visit",
                "billing_code_type": "CPT",
                "billing_code_type_version": "2026",
                "billing_code": "99203",
                "description": "Office visit",
                "allowed_amounts": [
                    {
                        "tin": {"type": "ein", "value": "371382997"},
                        "service_code": [None, "11"],
                        "billing_class": "professional",
                        "payments": [
                            {
                                "allowed_amount": 133.0,
                                "billing_code_modifier": [None, "AA"],
                                "providers": [{"billed_charge": 200.0, "npi": [1427166008]}],
                            }
                        ],
                    }
                ],
            }
        ]
    }
    allowed_path = tmp_path / "allowed.json"
    allowed_path.write_text(json.dumps(allowed_amount_payload_dict), encoding="utf-8")
    pushed_rows_by_class = {}

    async def fake_push(rows, cls):
        pushed_rows_by_class.setdefault(cls, []).extend(rows)

    monkeypatch.setattr(process_ptg, "push_objects", fake_push)
    monkeypatch.setattr(process_ptg, "flush_error_log", AsyncMock())

    metrics = asyncio.run(
        ptg_allowed_amounts._parse_allowed_amounts(
            str(allowed_path),
            123,
            {},
            [{"plan_id": "7862274fdc01bcc0", "plan_market_type": "group"}],
            {
                "PTGAllowedItem": "item",
                "PTGAllowedPayment": "payment",
                "PTGAllowedProviderPayment": "provider_payment",
            },
            False,
            "import_log",
            "https://example.test/allowed.json",
        )
    )

    assert metrics["allowed_amount_provider_payments"] == 1
    assert pushed_rows_by_class["payment"][0]["service_code"] == ["11"]
    assert pushed_rows_by_class["payment"][0]["billing_code_modifier"] == ["AA"]
    assert pushed_rows_by_class["provider_payment"][0]["npi"] == [1427166008]


def test_parse_allowed_amounts_persists_in_network_metadata(monkeypatch, tmp_path):
    allowed_amount_payload_dict = {
        "out_of_network": [
            {
                "name": "Office visit",
                "billing_code_type": "CPT",
                "billing_code_type_version": "2026",
                "billing_code": "99214",
                "description": "Office visit",
                "allowed_amounts": [
                    {
                        "tin": {"type": "ein", "value": "371382997"},
                        "service_code": ["11"],
                        "billing_class": "professional",
                        "payments": [
                            {
                                "allowed_amount": 133.0,
                                "providers": [{"billed_charge": 200.0, "npi": [1427166008]}],
                            }
                        ],
                    }
                ],
            }
        ]
    }
    allowed_path = tmp_path / "allowed.json"
    allowed_path.write_text(json.dumps(allowed_amount_payload_dict), encoding="utf-8")
    pushed_rows_by_class = {}

    async def fake_push(rows, cls):
        pushed_rows_by_class.setdefault(cls, []).extend(rows)

    monkeypatch.setattr(process_ptg, "push_objects", fake_push)
    monkeypatch.setattr(process_ptg, "flush_error_log", AsyncMock())

    asyncio.run(
        ptg_allowed_amounts._parse_allowed_amounts(
            str(allowed_path),
            123,
            {"network_status": "in_network"},
            [{"plan_id": "7862274fdc01bcc0", "plan_market_type": "group"}],
            {
                "PTGAllowedItem": "item",
                "PTGAllowedPayment": "payment",
                "PTGAllowedProviderPayment": "provider_payment",
            },
            False,
            "import_log",
            "https://example.test/allowed.json",
        )
    )

    assert pushed_rows_by_class["payment"][0]["network_status"] == "in_network"
    assert pushed_rows_by_class["payment"][0]["network_semantics"] == "in_network_historical_allowed_amounts"


def test_ptg2_main_publishes_allowed_amount_evidence_snapshot(monkeypatch):
    """Verify this PTG import regression contract."""
    pushed = []
    dropped_tables = []
    publish_serving = AsyncMock()

    async def fake_push(rows, cls, **_kwargs):
        pushed.extend((getattr(cls, "__name__", str(cls)), row) for row in rows)

    async def fake_downloaded_jobs(jobs, **_kwargs):
        for job in jobs:
            yield process_ptg.PTG2DownloadedJob(
                job=job,
                raw_artifact=SimpleNamespace(raw_sha256=str(job.get("url") or "")),
                logical_artifact=SimpleNamespace(logical_path="/tmp/allowed.json.gz"),
            )

    async def fake_allowed(*_args, **_kwargs):
        return process_ptg.PTG2FileProcessResult(
            "allowed_amounts",
            "https://example.test/allowed.json.gz",
            True,
            file_id=123,
            summary={
                "allowed_amount_items": 2,
                "allowed_amount_blocks": 3,
                "allowed_amount_payments": 4,
                "allowed_amount_provider_payments": 5,
                "allowed_amount_npi_references": 6,
                "allowed_amount_unique_tins": 1,
                "allowed_amount_evidence": True,
            },
        )

    async def fake_drop_tables(names):
        dropped_tables.extend(names)

    monkeypatch.setattr(process_ptg, "ensure_database", AsyncMock())
    monkeypatch.setattr(process_ptg, "ensure_ptg2_tables", AsyncMock())
    monkeypatch.setattr(process_ptg.db, "status", AsyncMock())
    monkeypatch.setattr(process_ptg, "_push_ptg2_objects", fake_push)
    monkeypatch.setattr(process_ptg, "_prepare_ptg_tables", AsyncMock(return_value={"ImportLog": "log"}))
    monkeypatch.setattr(process_ptg, "_create_ptg2_manifest_serving_stage_table", AsyncMock(return_value="manifest_stage"))
    monkeypatch.setattr(process_ptg, "_drop_ptg2_snapshot_table_names", fake_drop_tables)
    monkeypatch.setattr(process_ptg, "_iter_downloaded_ptg_jobs", fake_downloaded_jobs)
    monkeypatch.setattr(process_ptg, "_process_allowed_amounts_file", fake_allowed)
    monkeypatch.setattr(process_ptg, "_ensure_ptg_dynamic_tables", AsyncMock())
    monkeypatch.setattr(process_ptg, "flush_error_log", AsyncMock())
    monkeypatch.setattr(process_ptg, "_publish_ptg2_manifest_serving_snapshot", publish_serving)
    monkeypatch.setattr(process_ptg, "_current_source_snapshot_id", AsyncMock(return_value=None))
    monkeypatch.setattr(process_ptg, "_publish_ptg2_source_pointers", AsyncMock())
    monkeypatch.setattr(process_ptg, "_cleanup_old_ptg2_source_tables", AsyncMock())
    monkeypatch.setenv(process_ptg.PTG2_COMPACT_IMPORT_ENV, "false")

    result = asyncio.run(
        process_ptg.main(
            allowed_url="https://example.test/allowed.json.gz",
            import_month="2026-04",
            import_id="allowed_evidence_state_machine_test",
            source_key="allowed_evidence_state_machine_test",
        )
    )

    import_run_rows = [row for cls_name, row in pushed if cls_name == "PTG2ImportRun"]
    final_report = import_run_rows[-1]["report"]

    assert result["allowed_amount_provider_payments"] == 5
    assert final_report["serving_index"]["type"] == "allowed_amounts_evidence"
    assert final_report["serving_index"]["storage"] == "legacy_dynamic_tables"
    assert final_report["serving_index"]["allowed_amount_provider_payments"] == 5
    assert final_report["allowed_amount_evidence"] is True
    assert final_report["serving_rates"] in {0}
    assert publish_serving.await_count in {0}
    assert "manifest_stage" in dropped_tables


def test_ptg2_main_blocks_partial_publish_by_default(monkeypatch):
    """Verify this PTG import regression contract."""
    pushed = []

    async def fake_push(rows, cls, **_kwargs):
        pushed.extend((getattr(cls, "__name__", str(cls)), row) for row in rows)

    async def fake_downloaded_jobs(jobs, **_kwargs):
        for job in jobs:
            yield process_ptg.PTG2DownloadedJob(
                job=job,
                raw_artifact=SimpleNamespace(raw_sha256=str(job.get("url") or "")),
                logical_artifact=SimpleNamespace(logical_path="/tmp/rates.json.gz"),
            )

    async def fake_in_network(*_args, **_kwargs):
        return process_ptg.PTG2FileProcessResult(
            "in_network",
            "https://example.test/rates.json.gz",
            False,
            error="download failed",
        )

    async def fake_allowed(*_args, **_kwargs):
        return process_ptg.PTG2FileProcessResult(
            "allowed_amounts",
            "https://example.test/allowed.json.gz",
            True,
            file_id=123,
        )

    monkeypatch.setattr(process_ptg, "ensure_database", AsyncMock())
    monkeypatch.setattr(process_ptg, "ensure_ptg2_tables", AsyncMock())
    monkeypatch.setattr(process_ptg, "_current_source_snapshot_id", AsyncMock(return_value=None))
    monkeypatch.setattr(process_ptg, "_push_ptg2_objects", fake_push)
    monkeypatch.setattr(process_ptg, "_prepare_ptg_tables", AsyncMock(return_value={"ImportLog": "log"}))
    monkeypatch.setattr(process_ptg, "_create_ptg2_manifest_serving_stage_table", AsyncMock(return_value="manifest_stage"))
    monkeypatch.setattr(process_ptg, "_iter_downloaded_ptg_jobs", fake_downloaded_jobs)
    monkeypatch.setattr(process_ptg, "_process_in_network_file", fake_in_network)
    monkeypatch.setattr(process_ptg, "_process_allowed_amounts_file", fake_allowed)
    monkeypatch.setattr(process_ptg, "_ensure_ptg_dynamic_tables", AsyncMock())
    monkeypatch.setattr(process_ptg, "flush_error_log", AsyncMock())
    monkeypatch.setattr(process_ptg, "build_ptg2_snapshot_index_artifact", AsyncMock())
    monkeypatch.delenv("HLTHPRT_PTG2_ALLOW_PARTIAL_IMPORT", raising=False)
    monkeypatch.setenv(process_ptg.PTG2_COMPACT_IMPORT_ENV, "false")

    with pytest.raises(RuntimeError, match="failed 1 of 2 attempted"):
        asyncio.run(
            process_ptg.main(
                in_network_url="https://example.test/rates.json.gz",
                allowed_url="https://example.test/allowed.json.gz",
                import_month="2026-04",
                import_id="partial_state_machine_test",
                source_key="partial_state_machine_test",
            )
        )

    import_run_rows = [row for cls_name, row in pushed if cls_name == "PTG2ImportRun"]
    current_rows = [row for cls_name, row in pushed if cls_name == "PTG2CurrentSnapshot"]

    assert import_run_rows[-1]["status"] == process_ptg.PTG2_STATUS_FAILED
    assert "failed 1 of 2 attempted" in import_run_rows[-1]["error"]
    assert import_run_rows[-1]["report"]["files_processed"] in {1}
    assert import_run_rows[-1]["report"]["files_failed"] in {1}
    assert current_rows == []


def test_ptg2_main_marks_failed_when_toc_download_fails(monkeypatch):
    pushed = []

    async def fake_push(rows, cls, **_kwargs):
        pushed.extend((getattr(cls, "__name__", str(cls)), row) for row in rows)

    async def fake_toc(*_args, **_kwargs):
        raise RuntimeError("409 public access denied")

    publish_mock = AsyncMock()

    monkeypatch.setattr(process_ptg, "ensure_database", AsyncMock())
    monkeypatch.setattr(process_ptg, "ensure_ptg2_tables", AsyncMock())
    monkeypatch.setattr(process_ptg, "_current_source_snapshot_id", AsyncMock(return_value=None))
    monkeypatch.setattr(process_ptg, "_push_ptg2_objects", fake_push)
    monkeypatch.setattr(process_ptg, "_prepare_ptg_tables", AsyncMock(return_value={"ImportLog": "log"}))
    monkeypatch.setattr(process_ptg, "_create_ptg2_manifest_serving_stage_table", AsyncMock(return_value="manifest_stage"))
    monkeypatch.setattr(process_ptg, "_process_table_of_contents", fake_toc)
    monkeypatch.setattr(process_ptg, "_publish_rust_compact_snapshot_tables", publish_mock)
    monkeypatch.setenv(process_ptg.PTG2_COMPACT_IMPORT_ENV, "false")
    monkeypatch.delenv("HLTHPRT_PTG2_ALLOW_PARTIAL_IMPORT", raising=False)

    with pytest.raises(RuntimeError, match="table-of-contents"):
        asyncio.run(
            process_ptg.main(
                toc_urls=["https://example.test/index.json"],
                import_month="2026-04",
                import_id="toc_failure_state_machine_test",
                source_key="toc_failure_state_machine_test",
            )
        )

    import_run_rows = [row for cls_name, row in pushed if cls_name == "PTG2ImportRun"]
    snapshot_rows = [row for cls_name, row in pushed if cls_name == "PTG2Snapshot"]
    current_rows = [row for cls_name, row in pushed if cls_name == "PTG2CurrentSnapshot"]

    assert import_run_rows[-1]["status"] == process_ptg.PTG2_STATUS_FAILED
    assert import_run_rows[-1]["report"]["jobs_discovered"] in {0}
    assert import_run_rows[-1]["report"]["toc_failures"][0]["error"] == "409 public access denied"
    assert snapshot_rows[-1]["status"] == process_ptg.PTG2_STATUS_FAILED
    assert current_rows == []
    publish_mock.assert_not_awaited()


def test_ptg2_snapshot_artifact_builder_writes_serving_index(monkeypatch, tmp_path):
    """Verify this PTG import regression contract."""
    class Table:
        def __init__(self, name):
            self.schema = "mrf"
            self.name = name

    class Model:
        def __init__(self, name):
            self.__tablename__ = name
            self.__table__ = Table(name)

    rows = [
        {
            "plan_id": "TESTPLAN001",
            "plan_name": "Example Plan",
            "plan_id_type": "EIN",
            "plan_market_type": "group",
            "issuer_name": "Example Sponsor",
            "plan_sponsor_name": "Example Sponsor",
            "billing_code": "70551",
            "billing_code_type": "CPT",
            "procedure_name": "MRI brain",
            "procedure_description": "MRI brain",
            "provider_npi": [1234567890],
            "tin_type": "ein",
            "tin_value": "123",
            "tin_business_name": "Example Imaging",
            "negotiated_type": "negotiated",
            "negotiated_rate": "450.00",
            "expiration_date": "2026-12-31",
            "service_code": ["11"],
            "billing_class": "professional",
            "setting": "outpatient",
            "billing_code_modifier": [],
            "additional_information": None,
            "source_url": "https://example.test/rates.json.gz",
        }
    ]

    async def fake_all(_sql):
        return rows

    async def fake_push_objects(_payload, _cls, rewrite=False):
        return None

    monkeypatch.setenv("HLTHPRT_PTG2_ARTIFACT_DIR", str(tmp_path))
    monkeypatch.setattr(process_ptg.db, "all", fake_all)
    monkeypatch.setattr(process_ptg, "push_objects", fake_push_objects)
    classes = {
        "PTGInNetworkItem": Model("ptg_in_network_item_test"),
        "PTGNegotiatedRate": Model("ptg_negotiated_rate_test"),
        "PTGNegotiatedPrice": Model("ptg_negotiated_price_test"),
        "PTGProviderGroup": Model("ptg_provider_group_test"),
        "PTGFile": Model("ptg_file_test"),
    }

    result = asyncio.run(process_ptg.build_ptg2_snapshot_index_artifact(classes, "snap-test", "run-test"))

    assert result["plan_count"] in {1}
    artifact_path = tmp_path / "snapshot_index" / "snap-test.json"
    payload = json.loads(artifact_path.read_text(encoding="utf-8"))
    assert payload["rates"]["TESTPLAN001"]["70551"][0]["prices"][0]["negotiated_rate"] == 450


def test_ptg2_compact_snapshot_artifact_keeps_source_file_version_id(monkeypatch, tmp_path):
    observed = {}
    rows = [
        {
            "plan_id": "TESTPLAN001",
            "plan_name": "Example Plan",
            "plan_id_type": "EIN",
            "plan_market_type": "group",
            "issuer_name": "Example Sponsor",
            "plan_sponsor_name": "Example Sponsor",
            "billing_code": "29888",
            "billing_code_type": "CPT",
            "procedure_name": "ACL reconstruction",
            "procedure_description": "ACL reconstruction",
            "rate_pack_hash": "rate-pack-1",
            "provider_set_hash": "provider-set-1",
            "price_set_hash": "price-set-1",
            "rate_payload": {"provider_set_hashes": ["provider-set-1"]},
            "prices": [{"negotiated_type": "negotiated", "negotiated_rate": 1138.57}],
            "source_trace": [
                {
                    "source_file_version_id": "source-version-1",
                    "url": "https://payer.example.invalid/mrf/rates.json.gz",
                    "canonical_url": "https://payer.example.invalid/mrf/rates.json.gz",
                }
            ],
            "provider_set_count": 1,
            "provider_count": 2,
        }
    ]

    async def fake_all(sql, **params):
        observed["sql"] = sql
        observed["params"] = params
        return rows

    async def fake_push_objects(_payload, _cls, rewrite=False):
        return None

    monkeypatch.setenv("HLTHPRT_PTG2_ARTIFACT_DIR", str(tmp_path))
    monkeypatch.setattr(process_ptg.db, "all", fake_all)
    monkeypatch.setattr(ptg_snapshot_artifacts, "_push_ptg2_objects_from_facade", fake_push_objects)

    result = asyncio.run(process_ptg.build_ptg2_compact_snapshot_index_artifact("snap-test", "run-test"))

    assert result["provider_granularity"] == "provider_set"
    assert "'source_file_version_id', st.source_file_version_id" in observed["sql"]
    assert "unnest(COALESCE(sts.source_trace_hashes" in observed["sql"]
    assert "sts.canonical_payload::jsonb->'source_trace_hashes'" not in observed["sql"]
    artifact_path = tmp_path / "snapshot_index" / "snap-test.json"
    payload = json.loads(artifact_path.read_text(encoding="utf-8"))
    trace = payload["rates"]["TESTPLAN001"]["29888"][0]["source_trace"]
    assert trace[0]["source_file_version_id"] == "source-version-1"


def test_ptg2_db_serving_index_builder_materializes_table(monkeypatch, tmp_path):
    statuses = []

    async def fake_scalar(sql, **params):
        if "to_regclass" in sql:
            return params["table_name"]
        if "COUNT(*)" in sql:
            return 1
        if "COUNT(DISTINCT plan_id)" in sql:
            return 1
        if "COUNT(DISTINCT COALESCE" in sql:
            return 1
        if "SUM(provider_count)" in sql:
            return 123
        return None

    async def fake_status(sql, **params):
        statuses.append((sql, params))
        return 1

    async def fake_create_table(*_args, **_kwargs):
        return None

    monkeypatch.setenv("HLTHPRT_PTG2_ARTIFACT_DIR", str(tmp_path))
    monkeypatch.setattr(process_ptg.db, "scalar", fake_scalar)
    monkeypatch.setattr(process_ptg.db, "status", fake_status)
    monkeypatch.setattr(process_ptg.db, "create_table", fake_create_table)

    result = asyncio.run(process_ptg.build_ptg2_db_serving_index("snap-compact", "run-compact"))

    insert_sql = next(sql for sql, _params in statuses if "INSERT INTO mrf.ptg2_serving_rate" in sql)
    source_observed_catalog_sql = next(sql for sql, _params in statuses if "INSERT INTO mrf.code_catalog" in sql)
    source_observed_synonym_sql = next(sql for sql, _params in statuses if "INSERT INTO mrf.code_synonym" in sql)
    assert result["storage"] == "db"
    assert result["table"] == "mrf.ptg2_serving_rate"
    assert result["provider_granularity"] == "provider_set"
    assert result["procedure_consolidation"]["system"] == "HP_PROCEDURE_CODE"
    assert "code_crosswalk" in insert_sql
    assert "pricing_procedure" in insert_sql
    assert "'source_file_version_id', st.source_file_version_id" in insert_sql
    assert "unnest(COALESCE(sts.source_trace_hashes" in insert_sql
    assert "sts.canonical_payload::jsonb->'source_trace_hashes'" not in insert_sql
    assert "code_catalog" in insert_sql
    assert "WHEN 'REVENUE_CODE' THEN 'RC'" in insert_sql
    assert "regexp_replace(COALESCE(NULLIF(UPPER(BTRIM(proc.billing_code)), ''), ''), '[^0-9]', '', 'g')" in insert_sql
    assert "LPAD(" in insert_sql
    assert "AS reported_code,\n                proc.billing_code," in insert_sql
    assert "NULLIF(UPPER(BTRIM(proc.billing_code)), '') AS reported_code" not in insert_sql
    assert "code_system IN ('CPT', 'HCPCS', 'CDT', 'MS_DRG', 'RC')" in source_observed_catalog_sql
    assert "GROUP BY code_system, code" in source_observed_catalog_sql
    assert "source_attribution" in source_observed_catalog_sql
    assert "source_description" in source_observed_synonym_sql
    assert "snapshot_index" not in [part.name for part in tmp_path.iterdir()]


def test_ptg_cli_passes_plan_filters(monkeypatch):
    fake_initiate = AsyncMock()
    monkeypatch.setattr(process_pkg, "initiate_ptg", fake_initiate)

    def fake_run(coro):
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(coro)
        finally:
            loop.close()

    monkeypatch.setattr(process_pkg.asyncio, "run", fake_run)

    process_pkg.ptg.callback(
        toc_url=("https://example.test/toc.json",),
        toc_list=None,
        in_network_url=None,
        allowed_url=None,
        provider_ref_url=None,
        import_id="ptg_smoke",
        source_key="example_dental",
        import_month="2026-04-01",
        max_files=1,
        max_items=2,
        plan_id=("TESTPLAN001",),
        plan_name_contains=("example employer",),
        plan_market_type=("group",),
        file_url_contains=("ps1-50",),
        reuse_raw_artifacts=False,
        keep_partial_artifacts=True,
        test=True,
    )

    fake_initiate.assert_called_once_with(
        test_mode=True,
        toc_urls=["https://example.test/toc.json"],
        toc_list=None,
        in_network_url=None,
        allowed_url=None,
        provider_ref_url=None,
        import_id="ptg_smoke",
        source_key="example_dental",
        import_month="2026-04-01",
        max_files=1,
        max_items=2,
        plan_ids=["TESTPLAN001"],
        plan_name_contains=["example employer"],
        plan_market_types=["group"],
        file_url_contains=["ps1-50"],
        reuse_raw_artifacts=False,
        keep_partial_artifacts=True,
    )


def test_ptg_source_job_filter_ignores_scoped_parent_index_url():
    jobs = [
        {
            "url": "https://cdn.example.com/2026-07_111_11A0_in-network-rates.json.gz",
            "description": "111|11A0",
            "from_index_url": (
                "https://bcbsla.sapphiremrfhub.com/tocs/current/example-packaging-inc"
                "?hp_content_domain=in_network&hp_file_url_contains=2026-07_932_67B0-in-network-rates.json.gz"
            ),
        },
        {
            "url": "https://cdn.example.com/2026-07_932_67B0-in-network-rates_01_of_02.json.gz",
            "description": "932|67B0",
            "from_index_url": "https://bcbsla.sapphiremrfhub.com/tocs/current/example-packaging-inc",
        },
    ]

    filtered = ptg_source_jobs._filter_jobs_by_url_contains(
        jobs,
        ["2026-07_932_67B0-in-network-rates"],
    )

    assert filtered == [jobs[1]]


def test_ptg2_rust_scanner_emits_top_level_object_bytes(tmp_path):
    if process_ptg._ptg2_rust_scanner_binary() is None:
        pytest.skip("PTG2 Rust scanner binary is not built")
    artifact = tmp_path / "rates.json.gz"
    payload = {
        "provider_references": [
            {"provider_group_id": 1, "provider_groups": [{"npi": [123], "tin": {"type": "ein", "value": "1"}}]},
            {"provider_group_id": 2, "provider_groups": [{"npi": [456], "tin": {"type": "ein", "value": "2"}}]},
        ],
        "in_network": [
            {"billing_code": "99213", "negotiated_rates": []},
            {"billing_code": "70551", "negotiated_rates": []},
        ],
    }
    with gzip.open(artifact, "wb") as fp:
        fp.write(json.dumps(payload).encode("utf-8"))

    rows = list(process_ptg._iter_top_level_object_bytes_rust(artifact, {"provider_references", "in_network"}))

    assert [name for name, _raw in rows] == [
        "provider_references",
        "provider_references",
        "in_network",
        "in_network",
    ]
    assert json.loads(rows[0][1])["provider_group_id"] in {1}
    assert json.loads(rows[-1][1])["billing_code"] == "70551"


def test_ptg2_rust_compact_serving_mode_emits_copy_oriented_rows(tmp_path):
    """Verify this PTG import regression contract."""
    binary = process_ptg._ptg2_rust_scanner_binary()
    if binary is None:
        pytest.skip("PTG2 Rust scanner binary is not built")
    artifact = tmp_path / "rates.json.gz"
    payload = {
        "provider_references": [
            {
                "provider_group_id": 7,
                "provider_groups": [{"npi": [1234567890], "tin": {"type": "ein", "value": "12-3456789"}}],
            }
        ],
        "in_network": [
            {
                "billing_code_type": "CPT",
                "billing_code_type_version": "2026",
                "billing_code": "99213",
                "name": "Office visit",
                "description": "Established patient office visit",
                "negotiated_rates": [
                    {
                        "provider_references": [7],
                        "negotiated_prices": [
                            {
                                "negotiated_type": "negotiated",
                                "negotiated_rate": 100,
                                "expiration_date": "2026-12-31",
                                "service_code": ["11"],
                                "billing_class": "professional",
                            }
                        ],
                    }
                ],
            }
        ],
    }
    with gzip.open(artifact, "wb") as fp:
        fp.write(json.dumps(payload).encode("utf-8"))

    env = {
        **os.environ,
        "HLTHPRT_PTG2_COMPACT_SNAPSHOT_ID": "snapshot",
        "HLTHPRT_PTG2_COMPACT_PLAN_ID": "plan",
        "HLTHPRT_PTG2_COMPACT_PLAN_MONTH_ID": "plan-month",
        "HLTHPRT_PTG2_COMPACT_SOURCE_TRACE_SET_HASH": "source-trace",
    }
    completed = subprocess.run(
        [str(binary), "--compact-serving", str(artifact)],
        check=True,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    frames = []
    stream = io.BytesIO(completed.stdout)
    while True:
        header = stream.readline()
        if not header:
            break
        name, size = header.rstrip(b"\n").split(b"\t", 1)
        payload_bytes = stream.read(int(size))
        assert stream.read(1) == b"\n"
        frames.append((name.decode("utf-8"), json.loads(payload_bytes)))

    kinds = [kind for kind, _row in frames]
    assert "procedure" in kinds
    assert "price_set" not in kinds
    assert "provider_set" in kinds
    assert "serving_rate_compact" in kinds
    compact_row = [row for kind, row in frames if kind == "serving_rate_compact"][0]
    assert compact_row["snapshot_id"] == "snapshot"
    assert compact_row["plan_id"] == "plan"
    assert compact_row["billing_code"] == "99213"
    assert compact_row["provider_count"] in {1}
    assert b"PTG2_SCANNER_PROGRESS" in completed.stderr


def test_ptg2_rust_compact_serving_mode_can_write_copy_file(tmp_path):
    """The compact scanner can emit a complete COPY row instead of JSON frames."""
    binary = process_ptg._ptg2_rust_scanner_binary()
    if binary is None:
        pytest.skip("PTG2 Rust scanner binary is not built")
    artifact = tmp_path / "rates.json.gz"
    copy_path = tmp_path / "compact.copy"
    payload = {
        "provider_references": [
            {
                "provider_group_id": 7,
                "provider_groups": [{"npi": [1234567890], "tin": {"type": "ein", "value": "12-3456789"}}],
            }
        ],
        "in_network": [
            {
                "billing_code_type": "CPT",
                "billing_code": "99213",
                "negotiated_rates": [
                    {
                        "provider_references": [7],
                        "negotiated_prices": [{"negotiated_type": "negotiated", "negotiated_rate": 100}],
                    }
                ],
            }
        ],
    }
    with gzip.open(artifact, "wb") as fp:
        fp.write(json.dumps(payload).encode("utf-8"))
    env = {
        **os.environ,
        "HLTHPRT_PTG2_COMPACT_SNAPSHOT_ID": "snapshot",
        "HLTHPRT_PTG2_COMPACT_PLAN_ID": "plan",
        "HLTHPRT_PTG2_COMPACT_PLAN_MONTH_ID": "plan-month",
        "HLTHPRT_PTG2_COMPACT_SOURCE_TRACE_SET_HASH": "source-trace",
        "HLTHPRT_PTG2_COMPACT_SERVING_COPY_PATH": str(copy_path),
        "HLTHPRT_PTG2_RUST_WORKERS": "1",
    }
    completed = subprocess.run(
        [str(binary), "--compact-serving", str(artifact)],
        check=True,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    copy_files = [copy_path] if copy_path.exists() else sorted(tmp_path.glob(f"{copy_path.name}*"))
    assert copy_files
    copy_lines = [copy_line for copy_file in copy_files for copy_line in copy_file.read_text().splitlines()]
    assert len(copy_lines) in {1}
    fields = copy_lines[0].split("\t")
    assert fields[1] == "snapshot"
    assert fields[2] == "plan"
    assert fields[6] == "99213"
    assert fields[8] == "1"
    assert fields[11] == "{}"
    assert len(fields) == 12

    assert b"serving_rate_compact" not in completed.stdout
    assert b"compact_copy_file" in completed.stdout


def _inline_provider_worker_payload():
    inline_group_by_field = {"npi": [1234567890], "tin": {"type": "ein", "value": "12-3456789"}}
    return {
        "provider_references": [
            {
                "provider_group_id": 7,
                "provider_groups": [
                    {"npi": [1234567000], "tin": {"type": "ein", "value": "98-7654321"}}
                ],
            }
        ],
        "in_network": [
            {
                "billing_code_type": "CPT",
                "billing_code": "99213",
                "negotiated_rates": [
                    {
                        "provider_groups": [inline_group_by_field],
                        "negotiated_prices": [{"negotiated_type": "negotiated", "negotiated_rate": rate}],
                    }
                    for rate in (100, 125)
                ],
            }
        ],
    }


def test_ptg2_rust_compact_serving_copy_files_support_inline_provider_groups(tmp_path):
    """The default worker parser retains inline provider groups and both rates."""

    binary = process_ptg._ptg2_rust_scanner_binary()
    if binary is None:
        pytest.skip("PTG2 Rust scanner binary is not built")
    artifact = tmp_path / "rates.json.gz"
    serving_copy = tmp_path / "serving.copy"
    member_copy = tmp_path / "provider_group_member.copy"
    payload = _inline_provider_worker_payload()
    with gzip.open(artifact, "wb") as fp:
        fp.write(json.dumps(payload).encode("utf-8"))

    env = {
        **os.environ,
        "HLTHPRT_PTG2_COMPACT_SNAPSHOT_ID": "snapshot",
        "HLTHPRT_PTG2_COMPACT_PLAN_ID": "plan",
        "HLTHPRT_PTG2_COMPACT_PLAN_MONTH_ID": "plan-month",
        "HLTHPRT_PTG2_COMPACT_SOURCE_TRACE_SET_HASH": "source-trace",
        "HLTHPRT_PTG2_COMPACT_SERVING_COPY_PATH": str(serving_copy),
        "HLTHPRT_PTG2_PROVIDER_GROUP_MEMBER_COPY_PATH": str(member_copy),
        "HLTHPRT_PTG2_RUST_WORKERS": "2",
        "HLTHPRT_PTG2_RUST_PARSE_IN_WORKERS": "true",
        "HLTHPRT_PTG2_RUST_TOP_LEVEL_BYTE_SCAN": "true",
    }
    completed = subprocess.run(
        [str(binary), "--compact-serving", str(artifact)],
        check=True,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    serving_files = [serving_copy] if serving_copy.exists() else sorted(tmp_path.glob("serving.copy.worker*"))
    member_files = [member_copy] if member_copy.exists() else sorted(tmp_path.glob("provider_group_member.copy.worker*"))
    assert sum(len(copy_file.read_text().splitlines()) for copy_file in serving_files) == 2
    member_lines = [line for copy_file in member_files for line in copy_file.read_text().splitlines()]
    assert len(member_lines) in {1}
    assert b"parallel_top_level_bytes" in completed.stdout
    assert b"compact_copy_file" in completed.stdout
    assert b"provider_group_member_copy_file" in completed.stdout


def test_ptg2_rust_compact_serving_parallel_workers_write_shards(tmp_path):
    """Verify this PTG import regression contract."""
    binary = process_ptg._ptg2_rust_scanner_binary()
    if binary is None:
        pytest.skip("PTG2 Rust scanner binary is not built")
    artifact = tmp_path / "rates.json.gz"
    copy_path = tmp_path / "compact.copy"
    payload = {
        "provider_references": [
            {
                "provider_group_id": 7,
                "provider_groups": [{"npi": [1234567890], "tin": {"type": "ein", "value": "12-3456789"}}],
            }
        ],
        "in_network": [
            {
                "billing_code_type": "CPT",
                "billing_code": "99213",
                "negotiated_rates": [
                    {
                        "provider_references": [7],
                        "negotiated_prices": [{"negotiated_type": "negotiated", "negotiated_rate": 100}],
                    }
                ],
            },
            {
                "billing_code_type": "CPT",
                "billing_code": "99213",
                "negotiated_rates": [
                    {
                        "provider_references": [7],
                        "negotiated_prices": [{"negotiated_type": "negotiated", "negotiated_rate": 100}],
                    }
                ],
            },
        ],
    }
    with gzip.open(artifact, "wb") as fp:
        fp.write(json.dumps(payload).encode("utf-8"))

    env = {
        **os.environ,
        "HLTHPRT_PTG2_COMPACT_SNAPSHOT_ID": "snapshot",
        "HLTHPRT_PTG2_COMPACT_PLAN_ID": "plan",
        "HLTHPRT_PTG2_COMPACT_PLAN_MONTH_ID": "plan-month",
        "HLTHPRT_PTG2_COMPACT_SOURCE_TRACE_SET_HASH": "source-trace",
        "HLTHPRT_PTG2_COMPACT_SERVING_COPY_PATH": str(copy_path),
        "HLTHPRT_PTG2_RUST_SPLIT_NEGOTIATED_RATES": "1",
        "HLTHPRT_PTG2_RUST_WORKERS": "2",
        "HLTHPRT_PTG2_RUST_WORK_QUEUE": "2",
        "HLTHPRT_PTG2_COMPACT_SERVING_COPY_ROTATE_BYTES": "1",
    }
    completed = subprocess.run(
        [str(binary), "--compact-serving", str(artifact)],
        check=True,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    assert not copy_path.exists()
    shard_paths = sorted(tmp_path.glob("compact.copy.worker*"))
    assert shard_paths
    copy_lines = [
        line
        for shard_path in shard_paths
        for line in shard_path.read_text().splitlines()
    ]
    assert len(copy_lines) in {1}
    fields = copy_lines[0].split("\t")
    assert fields[6] == "99213"
    assert fields[8] == "1"
    assert b"serving_rate_compact" not in completed.stdout
    assert b"compact_copy_file" in completed.stdout
    assert b"dedupe_summary" in completed.stdout
    assert b"PTG2_DEDUPE_SUMMARY" in completed.stderr

    frames = []
    stream = io.BytesIO(completed.stdout)
    while True:
        header = stream.readline()
        if not header:
            break
        name, size = header.rstrip(b"\n").split(b"\t", 1)
        payload_bytes = stream.read(int(size))
        assert stream.read(1) == b"\n"
        frames.append((name.decode("utf-8"), json.loads(payload_bytes)))
    dedupe_summary = [row for kind, row in frames if kind == "dedupe_summary"][0]
    rotated_copy_events = [
        row
        for kind, row in frames
        if kind == "compact_copy_file" and row.get("final") is False
    ]
    assert rotated_copy_events
    assert all(str(row["path"]).endswith(".ready") for row in rotated_copy_events)
    assert dedupe_summary["negotiated_rates"] == 2
    assert dedupe_summary["serving_rate_attempted"] == 2
    assert dedupe_summary["serving_rate_unique"] in {1}
    assert dedupe_summary["serving_rate_duplicate"] in {1}
    assert dedupe_summary["serving_rate_reduction_pct"] == 50.0
    assert dedupe_summary["price_atom_attempted"] in {1}
    assert dedupe_summary["price_atom_unique"] == 1
    assert dedupe_summary["price_atom_duplicate"] == 0
    assert dedupe_summary["price_set_entry_attempted"] == 0
    assert dedupe_summary["price_set_entry_unique"] == 0
    assert dedupe_summary["price_set_entry_duplicate"] == 0
    assert dedupe_summary["provider_set_entry_attempted"] == 0
    assert dedupe_summary["provider_set_entry_unique"] == 0
    assert dedupe_summary["provider_set_entry_duplicate"] == 0
    assert dedupe_summary["provider_entry_component_attempted"] == 0
    assert dedupe_summary["provider_entry_component_unique"] == 0
    assert dedupe_summary["provider_entry_component_duplicate"] in {0}


def test_ptg2_rust_parse_in_workers_matches_default_on_large_in_network_chunk(tmp_path):
    """Verify this PTG import regression contract."""
    binary = process_ptg._ptg2_rust_scanner_binary()
    if binary is None:
        pytest.skip("PTG2 Rust scanner binary is not built")

    artifact = tmp_path / "rates.json.gz"
    negotiated_rates = [
        {
            "provider_references": [7],
            "negotiated_prices": [
                {
                    "negotiated_type": "negotiated",
                    "negotiated_rate": 100 + index,
                    "service_code": ["11"],
                    "billing_class": "professional",
                }
            ],
        }
        for index in range(8)
    ]
    payload = {
        "provider_references": [
            {
                "provider_group_id": 7,
                "provider_groups": [{"npi": [1234567890], "tin": {"type": "ein", "value": "12-3456789"}}],
            }
        ],
        "in_network": [
            {
                "billing_code_type": "CPT",
                "billing_code": "99213",
                "negotiated_rates": negotiated_rates,
            }
        ],
    }
    with gzip.open(artifact, "wb") as fp:
        fp.write(json.dumps(payload).encode("utf-8"))

    def run_scanner(parse_in_workers: bool, provider_refs_in_workers: bool = True):
        suffix = "provider-workers" if provider_refs_in_workers else "provider-serial"
        run_dir = tmp_path / f"{'raw-workers' if parse_in_workers else 'default'}-{suffix}"
        run_dir.mkdir()
        serving_copy = run_dir / "manifest_serving.copy"
        price_atom_copy = run_dir / "price_atom.copy"
        provider_group_member_copy = run_dir / "provider_group_member.copy"
        env = {
            **os.environ,
            "HLTHPRT_PTG2_COMPACT_SNAPSHOT_ID": "snapshot",
            "HLTHPRT_PTG2_COMPACT_PLAN_ID": "plan",
            "HLTHPRT_PTG2_COMPACT_PLAN_MONTH_ID": "plan-month",
            "HLTHPRT_PTG2_COMPACT_SOURCE_TRACE_SET_HASH": "source-trace",
            "HLTHPRT_PTG2_MANIFEST_SERVING_COPY_PATH": str(serving_copy),
            "HLTHPRT_PTG2_MANIFEST_PRICE_ATOM_COPY_PATH": str(price_atom_copy),
            "HLTHPRT_PTG2_MANIFEST_PROVIDER_GROUP_MEMBER_COPY_PATH": str(provider_group_member_copy),
            "HLTHPRT_PTG2_MANIFEST_ONLY": "true",
            "HLTHPRT_PTG2_RUST_SPLIT_NEGOTIATED_RATES": "2",
            "HLTHPRT_PTG2_RUST_WORKERS": "2",
            "HLTHPRT_PTG2_RUST_WORK_QUEUE": "2",
            "HLTHPRT_PTG2_RUST_PARSE_IN_WORKERS": "true" if parse_in_workers else "false",
            "HLTHPRT_PTG2_RUST_PROVIDER_REFS_IN_WORKERS": "true" if provider_refs_in_workers else "false",
            "HLTHPRT_PTG2_RUST_PROVIDER_REF_CHUNK_ITEMS": "1",
            "HLTHPRT_PTG2_RUST_PROVIDER_REF_RAW_CHUNK_BYTES": "1",
        }
        completed = subprocess.run(
            [str(binary), "--compact-serving", str(artifact)],
            check=True,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        frames = []
        stream = io.BytesIO(completed.stdout)
        while True:
            header = stream.readline()
            if not header:
                break
            name, size = header.rstrip(b"\n").split(b"\t", 1)
            payload_bytes = stream.read(int(size))
            assert stream.read(1) == b"\n"
            frames.append((name.decode("utf-8"), json.loads(payload_bytes)))
        copy_rows = {
            "serving": sorted(
                line
                for path in run_dir.glob("manifest_serving.copy*")
                for line in path.read_text().splitlines()
            ),
            "price_atom": sorted(
                line
                for path in run_dir.glob("price_atom.copy*")
                for line in path.read_text().splitlines()
            ),
            "provider_group_member": sorted(
                line
                for path in run_dir.glob("provider_group_member.copy*")
                for line in path.read_text().splitlines()
            ),
        }
        return frames, copy_rows

    serial_frames, serial_rows = run_scanner(False, False)
    default_frames, default_rows = run_scanner(False, True)
    worker_frames, worker_rows = run_scanner(True, True)

    assert default_rows == serial_rows
    assert worker_rows == default_rows
    assert len(worker_rows["serving"]) == 8
    worker_summary = [row for kind, row in worker_frames if kind == "scanner_summary"][0]
    worker_config = [row for kind, row in worker_frames if kind == "scanner_config"][0]
    default_config = [row for kind, row in default_frames if kind == "scanner_config"][0]
    serial_config = [row for kind, row in serial_frames if kind == "scanner_config"][0]
    default_summary = [row for kind, row in default_frames if kind == "scanner_summary"][0]
    assert serial_config["provider_refs_in_workers"] is False
    assert default_config["provider_refs_in_workers"] is True
    assert worker_config["parse_in_workers"] is True
    assert worker_config["provider_refs_in_workers"] is True
    assert worker_summary["parse_in_workers"] is True
    assert default_summary["parse_in_workers"] is False
    assert worker_summary["split_negotiated_rates"] == 2
    assert worker_summary["provider_ref_raw_chunk_count"] in {1}
    assert "producer_blocked_micros" in worker_summary


def test_ptg2_rust_top_level_byte_scan_matches_default_compact_output(tmp_path):
    """Verify this PTG import regression contract."""
    binary = process_ptg._ptg2_rust_scanner_binary()
    if binary is None:
        pytest.skip("PTG2 Rust scanner binary is not built")

    artifact = tmp_path / "rates.json.gz"
    payload = {
        "provider_references": [
            {
                "provider_group_id": 7,
                "provider_groups": [{"npi": [1234567890], "tin": {"type": "ein", "value": "12-3456789"}}],
            },
            {
                "provider_group_id": "8",
                "provider_groups": [{"npi": [1234567891], "tin": {"type": "npi", "value": "1234567891"}}],
            },
        ],
        "in_network": [
            {
                "billing_code_type": "CPT",
                "billing_code": "99213",
                "negotiated_rates": [
                    {
                        "provider_references": [7],
                        "negotiated_prices": [{"negotiated_type": "negotiated", "negotiated_rate": 100}],
                    },
                    {
                        "provider_references": [8],
                        "negotiated_prices": [{"negotiated_type": "negotiated", "negotiated_rate": 125}],
                    },
                ],
            },
            {
                "billing_code_type": "CPT",
                "billing_code": "99214",
                "negotiated_rates": [
                    {
                        "provider_references": [7, 8],
                        "negotiated_prices": [{"negotiated_type": "negotiated", "negotiated_rate": 200}],
                    }
                ],
            },
        ],
    }
    with gzip.open(artifact, "wb") as fp:
        fp.write(json.dumps(payload).encode("utf-8"))

    def run_scanner(name: str, top_level_byte_scan: bool):
        run_dir = tmp_path / name
        run_dir.mkdir()
        serving_copy = run_dir / "manifest_serving.copy"
        price_atom_copy = run_dir / "price_atom.copy"
        provider_group_member_copy = run_dir / "provider_group_member.copy"
        env = {
            **os.environ,
            "HLTHPRT_PTG2_COMPACT_SNAPSHOT_ID": "snapshot",
            "HLTHPRT_PTG2_COMPACT_PLAN_ID": "plan",
            "HLTHPRT_PTG2_COMPACT_PLAN_MONTH_ID": "plan-month",
            "HLTHPRT_PTG2_COMPACT_SOURCE_TRACE_SET_HASH": "source-trace",
            "HLTHPRT_PTG2_MANIFEST_SERVING_COPY_PATH": str(serving_copy),
            "HLTHPRT_PTG2_MANIFEST_PRICE_ATOM_COPY_PATH": str(price_atom_copy),
            "HLTHPRT_PTG2_MANIFEST_PROVIDER_GROUP_MEMBER_COPY_PATH": str(provider_group_member_copy),
            "HLTHPRT_PTG2_MANIFEST_ONLY": "true",
            "HLTHPRT_PTG2_RUST_WORKERS": "2",
            "HLTHPRT_PTG2_RUST_WORK_QUEUE": "2",
            "HLTHPRT_PTG2_RUST_PROVIDER_REF_WORKERS": "2",
            "HLTHPRT_PTG2_RUST_PROVIDER_REF_QUEUE": "2",
            "HLTHPRT_PTG2_RUST_PROVIDER_REF_CHUNK_ITEMS": "1",
            "HLTHPRT_PTG2_RUST_PROVIDER_REF_RAW_CHUNK_BYTES": "1",
            "HLTHPRT_PTG2_RUST_TOP_LEVEL_BYTE_SCAN": "true" if top_level_byte_scan else "false",
        }
        completed = subprocess.run(
            [str(binary), "--compact-serving", str(artifact)],
            check=True,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        frames = []
        stream = io.BytesIO(completed.stdout)
        while True:
            header = stream.readline()
            if not header:
                break
            name, size = header.rstrip(b"\n").split(b"\t", 1)
            payload_bytes = stream.read(int(size))
            assert stream.read(1) == b"\n"
            frames.append((name.decode("utf-8"), json.loads(payload_bytes)))
        copy_rows = {
            "serving": sorted(
                line
                for path in run_dir.glob("manifest_serving.copy*")
                for line in path.read_text().splitlines()
            ),
            "price_atom": sorted(
                line
                for path in run_dir.glob("price_atom.copy*")
                for line in path.read_text().splitlines()
            ),
            "provider_group_member": sorted(
                line
                for path in run_dir.glob("provider_group_member.copy*")
                for line in path.read_text().splitlines()
            ),
        }
        return frames, copy_rows

    default_frames, default_rows = run_scanner("default", False)
    top_level_frames, top_level_rows = run_scanner("top-level", True)

    assert top_level_rows == default_rows
    assert len(top_level_rows["serving"]) == 3
    assert len(top_level_rows["price_atom"]) == 3
    assert len(top_level_rows["provider_group_member"]) == 2
    default_summary = [row for kind, row in default_frames if kind == "scanner_summary"][0]
    top_level_config = [row for kind, row in top_level_frames if kind == "scanner_config"][0]
    top_level_summary = [row for kind, row in top_level_frames if kind == "scanner_summary"][0]
    assert "top_level_byte_scan" not in default_summary
    assert top_level_config["top_level_byte_scan"] is True
    assert top_level_config["raw_rate_byte_capture"] is True
    assert top_level_summary["top_level_byte_scan"] is True
    assert top_level_summary["raw_rate_byte_capture"] is True
    assert top_level_summary["provider_ref_raw_chunk_count"] == 2
    assert top_level_summary["provider_refs_seconds"] >= 0


def test_ptg2_manifest_copy_file_reads_fifo(tmp_path, monkeypatch):
    if os.name != "posix" or not hasattr(os, "mkfifo"):
        pytest.skip("FIFO copy test requires POSIX named pipes")

    fifo_path = tmp_path / "manifest.copy"
    os.mkfifo(fifo_path)
    copied_payloads = []

    class FakeDriver:
        async def copy_to_table(self, target_table, *, source, schema_name, columns, **copy_options):
            copied_payloads.append(
                {
                    "target_table": target_table,
                    "schema_name": schema_name,
                    "columns": columns,
                    "format": copy_options["format"],
                    "delimiter": copy_options["delimiter"],
                    "null": copy_options["null"],
                    "payload": source.read().decode("utf-8"),
                }
            )

    class FakeAcquire:
        async def __aenter__(self):
            return SimpleNamespace(raw_connection=SimpleNamespace(driver_connection=FakeDriver()))

        async def __aexit__(self, *_exc_info):
            return False

    def writer():
        with fifo_path.open("wb") as fp:
            fp.write(b"serving\tplan\n")

    monkeypatch.setattr(ptg_manifest_publish.db, "acquire", lambda: FakeAcquire())
    thread = threading.Thread(target=writer)
    thread.start()

    asyncio.run(
        ptg_manifest_publish._copy_ptg2_manifest_file(
            fifo_path,
            target_table="stage_table",
            columns=["serving_content_hash_128", "plan_id"],
        )
    )
    thread.join(timeout=2)

    assert not thread.is_alive()
    assert copied_payloads == [
        {
            "target_table": "stage_table",
            "schema_name": "mrf",
            "columns": ["serving_content_hash_128", "plan_id"],
            "format": "text",
            "delimiter": "\t",
            "null": "\\N",
            "payload": "serving\tplan\n",
        }
    ]


def test_ptg2_manifest_stream_merge_copies_from_fifo(tmp_path, monkeypatch):
    if os.name != "posix" or not hasattr(os, "mkfifo"):
        pytest.skip("streaming merge test requires POSIX named pipes")

    producer_script = tmp_path / "producer.py"
    producer_script.write_text(
        """
import json
import sys

fifo_path = sys.argv[1]
with open(fifo_path, "wb") as fp:
    fp.write(b"serving\\tplan\\n")
summary = {
    "kind": "manifest_serving",
    "input_files": 1,
    "input_rows": 2,
    "output_rows": 1,
    "dropped_rows": 1,
    "output_path": fifo_path,
}
payload = json.dumps(summary, sort_keys=True).encode("utf-8")
sys.stdout.buffer.write(b"manifest_copy_merge_summary\\t" + str(len(payload)).encode("ascii") + b"\\n")
sys.stdout.buffer.write(payload + b"\\n")
""".lstrip()
    )
    input_path = tmp_path / "input.copy"
    input_path.write_text("serving\tplan\nserving\tplan\n")
    copied_payloads = []

    async def fake_copy(copy_path, *, target_table):
        assert target_table == "stage_table"
        with copy_path.open("rb") as fp:
            copied_payloads.append(fp.read().decode("utf-8"))

    def fake_command(_kind, output_path, _existing_paths):
        return [sys.executable, str(producer_script), str(output_path)]

    monkeypatch.setattr(process_ptg, "ptg2_temp_parent", lambda: tmp_path)
    monkeypatch.setattr(process_ptg, "_ptg2_manifest_copy_merge_command", fake_command)

    metrics = asyncio.run(
        process_ptg._stream_ptg2_manifest_copy_merge(
            "manifest_serving",
            target_table="stage_table",
            input_paths=[input_path],
            copy_func=fake_copy,
        )
    )

    assert copied_payloads == ["serving\tplan\n"]
    assert metrics["kind"] == "manifest_serving"
    assert metrics["input_rows"] == 2
    assert metrics["output_rows"] == 1
    assert metrics["dropped_rows"] == 1


def test_ptg2_provider_membership_sidecar_builder_contract(tmp_path, monkeypatch):
    member_copy = tmp_path / "provider-group-member.copy"
    member_copy.write_text("00000000000000000000000000000001\t1003002106\n")
    group_npi = tmp_path / "provider-group-npi.ptg2sc"
    npi_group = tmp_path / "provider-npi-group.ptg2sc"
    npi_scope = tmp_path / "provider-npi-scope.copy"
    summary_json = json.dumps(
        {
            "input_rows": 1,
            "membership_count": 1,
            "provider_group_count": 1,
            "npi_count": 1,
            "npi_scope_count": 1,
        }
    ).encode()
    completed = subprocess.CompletedProcess(
        args=[],
        returncode=0,
        stdout=b"provider_membership_sidecars\t"
        + str(len(summary_json)).encode()
        + b"\n"
        + summary_json
        + b"\n",
        stderr=b"",
    )
    run_mock = Mock(return_value=completed)

    monkeypatch.setattr(process_ptg, "_ptg2_rust_scanner_binary", lambda: Path("/opt/ptg2_scanner"))
    monkeypatch.setattr(process_ptg.subprocess, "run", run_mock)

    summary = asyncio.run(
        process_ptg._build_ptg2_provider_membership_sidecars(
            provider_group_npi_path=group_npi,
            provider_npi_group_path=npi_group,
            provider_npi_scope_copy_path=npi_scope,
            input_paths=[member_copy],
        )
    )

    assert summary["membership_count"] == 1
    run_mock.assert_called_once_with(
        [
            "/opt/ptg2_scanner",
            "--provider-membership-sidecars",
            str(group_npi),
            str(npi_group),
            str(npi_scope),
            str(member_copy),
        ],
        check=True,
        capture_output=True,
    )


def test_ptg2_provider_membership_builder_allows_empty_scope(tmp_path, monkeypatch):
    summary_json = json.dumps({"membership_count": 0, "npi_scope_count": 0}).encode()
    completed = subprocess.CompletedProcess(
        args=[],
        returncode=0,
        stdout=b"provider_membership_sidecars\t"
        + str(len(summary_json)).encode()
        + b"\n"
        + summary_json
        + b"\n",
        stderr=b"",
    )
    run_mock = Mock(return_value=completed)
    monkeypatch.setattr(process_ptg, "_ptg2_rust_scanner_binary", lambda: Path("/opt/ptg2_scanner"))
    monkeypatch.setattr(process_ptg.subprocess, "run", run_mock)

    summary = asyncio.run(
        process_ptg._build_ptg2_provider_membership_sidecars(
            provider_group_npi_path=tmp_path / "provider-group-npi.ptg2sc",
            provider_npi_group_path=tmp_path / "provider-npi-group.ptg2sc",
            provider_npi_scope_copy_path=tmp_path / "provider-npi-scope.copy",
            input_paths=[],
        )
    )

    assert summary == {"membership_count": 0, "npi_scope_count": 0}
    assert run_mock.call_args.args[0] == [
        "/opt/ptg2_scanner",
        "--provider-membership-sidecars",
        str(tmp_path / "provider-group-npi.ptg2sc"),
        str(tmp_path / "provider-npi-group.ptg2sc"),
        str(tmp_path / "provider-npi-scope.copy"),
    ]


def test_ptg2_provider_membership_sidecars_round_trip_through_python_reader(tmp_path):
    binary = process_ptg._ptg2_rust_scanner_binary()
    if binary is None:
        pytest.skip("PTG2 Rust scanner binary is not built")
    artifact_helpers = importlib.import_module("process.ptg_parts.ptg2_manifest_artifacts")
    member_copy = tmp_path / "provider-group-member.copy"
    member_copy.write_text(
        "00000000000000000000000000000001\t1003002106\n"
        "00000000000000000000000000000001\t1003007311\n"
        "00000000000000000000000000000002\t1003002106\n"
    )
    group_npi = tmp_path / "provider-group-npi.ptg2sc"
    npi_group = tmp_path / "provider-npi-group.ptg2sc"
    npi_scope = tmp_path / "provider-npi-scope.copy"

    subprocess.run(
        [
            str(binary),
            "--provider-membership-sidecars",
            str(group_npi),
            str(npi_group),
            str(npi_scope),
            str(member_copy),
        ],
        check=True,
        capture_output=True,
    )

    npi_1003002106 = b"\x00" * 8 + (1003002106).to_bytes(8, "big")
    npi_1003007311 = b"\x00" * 8 + (1003007311).to_bytes(8, "big")
    assert artifact_helpers.lookup_global_sidecar_members(
        group_npi,
        "00000000000000000000000000000001",
    ) == (npi_1003002106, npi_1003007311)
    assert artifact_helpers.lookup_global_sidecar_members(
        npi_group,
        npi_1003002106.hex(),
    ) == (
        bytes.fromhex("00000000000000000000000000000001"),
        bytes.fromhex("00000000000000000000000000000002"),
    )
    assert npi_scope.read_text() == "1003002106\n1003007311\n"


def test_ptg2_rust_compact_price_sets_emit_normalized_membership(tmp_path):
    """Verify this PTG import regression contract."""
    binary = process_ptg._ptg2_rust_scanner_binary()
    if binary is None:
        pytest.skip("PTG2 Rust scanner binary is not built")
    artifact = tmp_path / "rates.json.gz"
    serving_copy = tmp_path / "serving.copy"
    price_code_set_copy = tmp_path / "price_code_set.copy"
    price_atom_copy = tmp_path / "price_atom.copy"
    price_set_entry_copy = tmp_path / "price_set_entry.copy"
    payload = {
        "provider_references": [
            {
                "provider_group_id": 7,
                "provider_groups": [{"npi": [1234567890], "tin": {"type": "ein", "value": "12-3456789"}}],
            }
        ],
        "in_network": [
            {
                "billing_code_type": "CPT",
                "billing_code": "99213",
                "negotiated_rates": [
                    {
                        "provider_references": [7],
                        "negotiated_prices": [
                            {
                                "negotiated_type": "negotiated",
                                "negotiated_rate": 100,
                                "service_code": [" 11 ", "11"],
                                "billing_code_modifier": ["tc", " TC "],
                            },
                            {
                                "negotiated_type": "derived",
                                "negotiated_rate": "125.50",
                                "service_code": ["22"],
                                "billing_code_modifier": ["26"],
                            },
                        ],
                    }
                ],
            }
        ],
    }
    with gzip.open(artifact, "wb") as fp:
        fp.write(json.dumps(payload).encode("utf-8"))

    env = {
        **os.environ,
        "HLTHPRT_PTG2_COMPACT_SNAPSHOT_ID": "snapshot",
        "HLTHPRT_PTG2_COMPACT_PLAN_ID": "plan",
        "HLTHPRT_PTG2_COMPACT_PLAN_MONTH_ID": "plan-month",
        "HLTHPRT_PTG2_COMPACT_SOURCE_TRACE_SET_HASH": "source-trace",
        "HLTHPRT_PTG2_COMPACT_SERVING_COPY_PATH": str(serving_copy),
        "HLTHPRT_PTG2_PRICE_CODE_SET_COPY_PATH": str(price_code_set_copy),
        "HLTHPRT_PTG2_PRICE_ATOM_COPY_PATH": str(price_atom_copy),
        "HLTHPRT_PTG2_PRICE_SET_ENTRY_COPY_PATH": str(price_set_entry_copy),
        "HLTHPRT_PTG2_RUST_SPLIT_NEGOTIATED_RATES": "1",
        "HLTHPRT_PTG2_RUST_WORKERS": "2",
        "HLTHPRT_PTG2_RUST_WORK_QUEUE": "2",
    }
    completed = subprocess.run(
        [str(binary), "--compact-serving", str(artifact)],
        check=True,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    price_atom_lines = [
        line
        for shard_path in sorted(tmp_path.glob("price_atom.copy*"))
        for line in shard_path.read_text().splitlines()
    ]
    price_code_set_lines = [
        line
        for shard_path in sorted(tmp_path.glob("price_code_set.copy*"))
        for line in shard_path.read_text().splitlines()
    ]
    price_set_entry_lines = [
        line
        for shard_path in sorted(tmp_path.glob("price_set_entry.copy*"))
        for line in shard_path.read_text().splitlines()
    ]
    assert not list(tmp_path.glob("price_set.copy*"))
    assert len(price_atom_lines) == 2
    assert len(price_code_set_lines) == 4
    assert len(price_set_entry_lines) == 2
    atom_fields = [line.split("\t") for line in price_atom_lines]
    assert all(len(fields) == 9 for fields in atom_fields)
    assert all(fields[4] != "\\N" and fields[7] != "\\N" for fields in atom_fields)
    code_set_payloads = {line.split("\t", 1)[1] for line in price_code_set_lines}
    assert '{"11"}' in code_set_payloads
    assert '{"TC"}' in code_set_payloads

    frames = []
    stream = io.BytesIO(completed.stdout)
    while True:
        header = stream.readline()
        if not header:
            break
        name, size = header.rstrip(b"\n").split(b"\t", 1)
        payload_bytes = stream.read(int(size))
        assert stream.read(1) == b"\n"
        frames.append((name.decode("utf-8"), json.loads(payload_bytes)))
    dedupe_summary = [row for kind, row in frames if kind == "dedupe_summary"][0]
    assert dedupe_summary["price_atom_unique"] == 2
    assert dedupe_summary["price_set_unique"] in {1}
    assert dedupe_summary["price_set_entry_unique"] == 2


def test_ptg2_rust_compact_provider_sets_use_real_group_hashes(tmp_path):
    """Verify this PTG import regression contract."""
    binary = process_ptg._ptg2_rust_scanner_binary()
    if binary is None:
        pytest.skip("PTG2 Rust scanner binary is not built")
    artifact = tmp_path / "rates.json.gz"
    serving_copy = tmp_path / "serving.copy"
    provider_set_copy = tmp_path / "provider_set.copy"
    provider_set_component_copy = tmp_path / "provider_set_component.copy"
    member_copy = tmp_path / "provider_group_member.copy"
    payload = {
        "provider_references": [
            {
                "provider_group_id": 7,
                "provider_groups": [
                    {"npi": [1111111111, 2222222222], "tin": {"type": "ein", "value": "12-3456789"}},
                    {"npi": [3333333333], "tin": {"type": "ein", "value": "98-7654321"}},
                ],
            }
        ],
        "in_network": [
            {
                "billing_code_type": "CPT",
                "billing_code": "99213",
                "negotiated_rates": [
                    {
                        "provider_references": [7],
                        "negotiated_prices": [{"negotiated_type": "negotiated", "negotiated_rate": 100}],
                    }
                ],
            }
        ],
    }
    with gzip.open(artifact, "wb") as fp:
        fp.write(json.dumps(payload).encode("utf-8"))

    env = {
        **os.environ,
        "HLTHPRT_PTG2_COMPACT_SNAPSHOT_ID": "snapshot",
        "HLTHPRT_PTG2_COMPACT_PLAN_ID": "plan",
        "HLTHPRT_PTG2_COMPACT_PLAN_MONTH_ID": "plan-month",
        "HLTHPRT_PTG2_COMPACT_SOURCE_TRACE_SET_HASH": "source-trace",
        "HLTHPRT_PTG2_COMPACT_SERVING_COPY_PATH": str(serving_copy),
        "HLTHPRT_PTG2_PROVIDER_SET_COPY_PATH": str(provider_set_copy),
        "HLTHPRT_PTG2_PROVIDER_SET_COMPONENT_COPY_PATH": str(provider_set_component_copy),
        "HLTHPRT_PTG2_PROVIDER_GROUP_MEMBER_COPY_PATH": str(member_copy),
        "HLTHPRT_PTG2_RUST_SPLIT_NEGOTIATED_RATES": "1",
        "HLTHPRT_PTG2_RUST_WORKERS": "2",
        "HLTHPRT_PTG2_RUST_WORK_QUEUE": "2",
    }
    subprocess.run(
        [str(binary), "--compact-serving", str(artifact)],
        check=True,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    provider_set_lines = [
        line
        for shard_path in sorted(tmp_path.glob("provider_set.copy*"))
        for line in shard_path.read_text().splitlines()
    ]
    assert len(provider_set_lines) in {1}
    provider_set_fields = provider_set_lines[0].split("\t")
    assert len(provider_set_fields) == 2
    assert int(provider_set_fields[1]) == 3
    provider_set_component_lines = [
        line
        for shard_path in sorted(tmp_path.glob("provider_set_component.copy*"))
        for line in shard_path.read_text().splitlines()
    ]
    component_group_hashes = {
        int(fields[1])
        for fields in (line.split("\t") for line in provider_set_component_lines)
    }
    member_group_hashes = {
        int(line.split("\t")[0])
        for shard_path in sorted(tmp_path.glob("provider_group_member.copy*"))
        for line in shard_path.read_text().splitlines()
    }

    assert len(member_group_hashes) == 2
    assert len(provider_set_component_lines) == 2
    assert component_group_hashes == member_group_hashes
    assert not list(tmp_path.glob("provider_set_entry.copy*"))
    assert not list(tmp_path.glob("provider_entry_component.copy*"))


def test_ptg2_rust_compact_uses_bounded_event_queue_default(monkeypatch, tmp_path):
    captured_env = {}

    class FakeProcess:
        stdout = io.BytesIO()
        stderr = None

        def poll(self):
            return 0

        def wait(self, timeout=None):
            return 0

    def fake_popen(_args, stdout, stderr, env):
        captured_env.update(env)
        return FakeProcess()

    monkeypatch.delenv(process_ptg.PTG2_RUST_EVENT_QUEUE_ENV, raising=False)
    monkeypatch.setattr(ptg_rust_scanner, "_ptg2_rust_scanner_binary", lambda: tmp_path / "ptg2_scanner")
    monkeypatch.setattr(ptg_rust_scanner.subprocess, "Popen", fake_popen)

    list(
        process_ptg._iter_compact_serving_records_rust(
            tmp_path / "rates.json.gz",
            snapshot_id="snap",
            plan_id="plan",
            plan_month_id="month",
            source_trace_set_hash="trace",
            compact_copy_path=tmp_path / "serving.copy",
            manifest_provider_forward_sidecar_path=tmp_path / "provider_forward.ptg2sc",
            manifest_provider_inverted_sidecar_path=tmp_path / "provider_inverted.ptg2sc",
            manifest_provider_npi_sidecar_path=tmp_path / "provider_npi.ptg2sc",
            manifest_price_forward_sidecar_path=tmp_path / "price_forward.ptg2sc",
            manifest_only=True,
        )
    )

    assert captured_env[process_ptg.PTG2_RUST_EVENT_QUEUE_ENV] == "32"
    assert captured_env["HLTHPRT_PTG2_MANIFEST_PROVIDER_FORWARD_SIDECAR_PATH"].endswith("provider_forward.ptg2sc")
    assert captured_env["HLTHPRT_PTG2_MANIFEST_PROVIDER_INVERTED_SIDECAR_PATH"].endswith("provider_inverted.ptg2sc")
    assert captured_env["HLTHPRT_PTG2_MANIFEST_PROVIDER_NPI_SIDECAR_PATH"].endswith("provider_npi.ptg2sc")
    assert captured_env["HLTHPRT_PTG2_MANIFEST_PRICE_FORWARD_SIDECAR_PATH"].endswith("price_forward.ptg2sc")
    assert captured_env["HLTHPRT_PTG2_MANIFEST_ONLY"] == "true"


def test_ptg2_rust_compact_retries_short_pipe_reads(monkeypatch, tmp_path):
    class ShortReader(io.BytesIO):
        def read(self, size=-1):
            return super().read(min(size, 2))

    class FakeProcess:
        stdout = ShortReader(b'scanner_summary\t7\n{"x":1}\n')
        stderr = None

        def poll(self):
            return 0

        def wait(self, timeout=None):
            return 0

    monkeypatch.setattr(ptg_rust_scanner, "_ptg2_rust_scanner_binary", lambda: tmp_path / "ptg2_scanner")
    monkeypatch.setattr(ptg_rust_scanner.subprocess, "Popen", lambda *args, **kwargs: FakeProcess())

    framed_records = list(
        process_ptg._iter_compact_serving_records_rust(
            tmp_path / "rates.json.gz",
            snapshot_id="snap",
            plan_id="plan",
            plan_month_id="month",
            source_trace_set_hash="trace",
            manifest_only=True,
        )
    )

    assert framed_records == [("scanner_summary", {"x": 1})]


def test_ptg2_rust_compact_reports_truncated_frame_process_status(monkeypatch, tmp_path):
    class FakeProcess:
        stdout = io.BytesIO(b'scanner_summary\t7\n{"x"')
        stderr = None

        def poll(self):
            return -9

        def wait(self, timeout=None):
            return -9

    monkeypatch.setattr(ptg_rust_scanner, "_ptg2_rust_scanner_binary", lambda: tmp_path / "ptg2_scanner")
    monkeypatch.setattr(ptg_rust_scanner.subprocess, "Popen", lambda *args, **kwargs: FakeProcess())

    with pytest.raises(RuntimeError) as error:
        list(
            process_ptg._iter_compact_serving_records_rust(
                tmp_path / "rates.json.gz",
                snapshot_id="snap",
                plan_id="plan",
                plan_month_id="month",
                source_trace_set_hash="trace",
                manifest_only=True,
            )
        )

    message = str(error.value)
    assert "kind=scanner_summary" in message
    assert "expected_bytes=7 received_bytes=4" in message
    assert "signal 9 (SIGKILL)" in message


def test_ptg2_rust_compact_can_omit_provider_npi_sidecar(monkeypatch, tmp_path):
    captured_env = {}

    class FakeProcess:
        stdout = io.BytesIO()
        stderr = None

        def poll(self):
            return 0

        def wait(self, timeout=None):
            return 0

    def fake_popen(_args, stdout, stderr, env):
        captured_env.update(env)
        return FakeProcess()

    monkeypatch.setattr(ptg_rust_scanner, "_ptg2_rust_scanner_binary", lambda: tmp_path / "ptg2_scanner")
    monkeypatch.setattr(ptg_rust_scanner.subprocess, "Popen", fake_popen)

    list(
        process_ptg._iter_compact_serving_records_rust(
            tmp_path / "rates.json.gz",
            snapshot_id="snap",
            plan_id="plan",
            plan_month_id="month",
            source_trace_set_hash="trace",
            manifest_provider_forward_sidecar_path=tmp_path / "provider_forward.ptg2sc",
            manifest_provider_inverted_sidecar_path=tmp_path / "provider_inverted.ptg2sc",
            manifest_provider_npi_sidecar_path=None,
            manifest_price_forward_sidecar_path=tmp_path / "price_forward.ptg2sc",
            manifest_only=True,
        )
    )

    assert captured_env["HLTHPRT_PTG2_MANIFEST_PROVIDER_FORWARD_SIDECAR_PATH"].endswith("provider_forward.ptg2sc")
    assert captured_env["HLTHPRT_PTG2_MANIFEST_PROVIDER_INVERTED_SIDECAR_PATH"].endswith("provider_inverted.ptg2sc")
    assert "HLTHPRT_PTG2_MANIFEST_PROVIDER_NPI_SIDECAR_PATH" not in captured_env
    assert captured_env["HLTHPRT_PTG2_MANIFEST_PRICE_FORWARD_SIDECAR_PATH"].endswith("price_forward.ptg2sc")


def test_ptg2_manifest_artifacts_skip_disabled_provider_npi_sidecar(tmp_path):
    provider_forward = tmp_path / "provider_forward.ptg2sc"
    provider_forward.write_bytes(struct.pack("<8sIQQ", b"PTG2MNDS", 1, 0, 0))
    empty_price_forward = tmp_path / "price_forward.ptg2sc"
    empty_price_forward.touch()

    artifacts = process_ptg._collect_ptg2_manifest_sidecar_artifacts(
        {
            "provider_forward": provider_forward,
            "provider_npi": None,
            "price_forward": empty_price_forward,
        }
    )

    assert set(artifacts) == {"provider_forward"}
    assert artifacts["provider_forward"]["name"] == "provider_forward"
    assert artifacts["provider_forward"]["record_format"] == process_ptg.PTG2_MANIFEST_DENSE_MEMBERSHIP_FORMAT
    assert artifacts["provider_forward"]["path"] == str(provider_forward)
    assert "owner_index_fence_owners" not in artifacts["provider_forward"]


def test_ptg2_manifest_artifacts_fallback_collects_from_summary_paths(tmp_path):
    price_forward = tmp_path / "price_forward.ptg2sc"
    price_forward.write_bytes(struct.pack("<8sIQ", b"PTG2MNSC", 1, 0))
    provider_forward = tmp_path / "provider_forward.ptg2sc"
    provider_forward.write_bytes(struct.pack("<8sIQQ", b"PTG2MNDS", 1, 0, 0))

    artifact_dict = process_ptg._collect_manifest_artifacts(
        [
            {
                "summary": {
                    "logical_sha256": "logical-shard-a",
                    "manifest": {
                        "sidecars": {},
                        "sidecar_paths": {
                            "price_forward": str(price_forward),
                            "provider_forward": str(provider_forward),
                        },
                    }
                }
            }
        ]
    )

    sidecar_map = {sidecar["name"]: sidecar for sidecar in artifact_dict["sidecars"]}
    assert set(sidecar_map) == {"price_forward", "provider_forward"}
    assert sidecar_map["price_forward"]["record_format"] == process_ptg.PTG2_MANIFEST_MEMBERSHIP_FORMAT
    assert sidecar_map["provider_forward"]["record_format"] == process_ptg.PTG2_MANIFEST_DENSE_MEMBERSHIP_FORMAT
    assert sidecar_map["provider_forward"]["path"] == str(provider_forward)
    assert sidecar_map["provider_forward"]["source_shard_id"] == "manifest:logical-shard-a"


@pytest.mark.asyncio
async def test_ptg2_manifest_publish_uploads_sidecars_to_db(monkeypatch, tmp_path):
    sidecar_path = tmp_path / "provider_forward.ptg2sc"
    sidecar_path.write_bytes(b"PTG2MNDS" + b"\0" * 32)
    calls = []
    progress_events = []

    async def fake_store(path, **kwargs):
        calls.append((Path(path), kwargs))
        metadata = dict(kwargs["metadata"])
        metadata.update({"storage_uri": "db://ptg2_artifact/artifact-1", "artifact_id": "artifact-1"})
        return metadata

    monkeypatch.setattr(ptg_manifest_publish, "ptg2_artifact_db_store_enabled", lambda: True)
    monkeypatch.setattr(ptg_manifest_publish, "store_ptg2_artifact_file_in_db", fake_store)
    monkeypatch.setattr(ptg_manifest_publish, "write_live_progress", lambda **payload: progress_events.append(payload))

    artifacts = await ptg_manifest_publish._store_ptg2_manifest_sidecar_artifacts_in_db(
        schema_name="mrf",
        snapshot_id="ptg2:test",
        sidecar_artifacts={
            "sidecars": [
                {
                    "name": "provider_forward",
                    "path": str(sidecar_path),
                    "sha256": "sha",
                    "byte_count": sidecar_path.stat().st_size,
                }
            ],
            "source_trace_set_hash": "trace-set",
        },
    )

    assert artifacts["source_trace_set_hash"] == "trace-set"
    assert artifacts["sidecars"][0]["storage_uri"] == "db://ptg2_artifact/artifact-1"
    assert calls[0][0] == sidecar_path
    assert calls[0][1]["snapshot_id"] == "ptg2:test"
    assert calls[0][1]["artifact_kind"] == "provider_forward"
    assert progress_events[-1]["publish_step"] == "artifact upload complete"
    assert progress_events[-1]["artifact_name"] == "provider_forward"


@pytest.mark.asyncio
async def test_ptg2_manifest_publish_reuses_bytes_without_collapsing_source_shards(monkeypatch, tmp_path):
    sidecar_path = tmp_path / "price_forward.ptg2sc"
    sidecar_path.write_bytes(b"shared-sidecar")
    store_calls = []

    async def fake_store(_path, **kwargs):
        store_calls.append(kwargs)
        metadata = dict(kwargs["metadata"])
        metadata.update({"storage_uri": "db://ptg2_artifact/shared", "artifact_id": "shared"})
        return metadata

    monkeypatch.setattr(ptg_manifest_publish, "ptg2_artifact_db_store_enabled", lambda: True)
    monkeypatch.setattr(ptg_manifest_publish, "store_ptg2_artifact_file_in_db", fake_store)

    artifacts = await ptg_manifest_publish._store_ptg2_manifest_sidecar_artifacts_in_db(
        schema_name="mrf",
        snapshot_id="ptg2:test",
        sidecar_artifacts={
            "sidecars": [
                {
                    "name": "price_forward",
                    "path": str(sidecar_path),
                    "sha256": "same-sha",
                    "byte_count": sidecar_path.stat().st_size,
                    "source_shard_id": source_shard_id,
                }
                for source_shard_id in ("source-a", "source-b")
            ]
        },
    )

    assert len(store_calls) == 1
    assert [entry["source_shard_id"] for entry in artifacts["sidecars"]] == ["source-a", "source-b"]


@pytest.mark.asyncio
async def test_ptg2_manifest_publish_omits_db_owned_local_paths(monkeypatch):
    monkeypatch.setattr(ptg_manifest_publish, "ptg2_artifact_db_store_enabled", lambda: True)
    monkeypatch.setattr(ptg_manifest_publish, "ptg2_artifact_db_retain_local_cache", lambda: False)

    artifacts = await ptg_manifest_publish._store_ptg2_manifest_sidecar_artifacts_in_db(
        schema_name="mrf",
        snapshot_id="ptg2:test",
        sidecar_artifacts={
            "sidecars": [
                {
                    "name": "provider_forward",
                    "path": "/work/ptg2-artifacts/serving/old/provider_forward.ptg2sc",
                    "cache_path": "/tmp/ptg2-cache/provider_forward.ptg2sc",
                    "storage": "postgresql_chunks_v1",
                    "storage_uri": "db://ptg2_artifact/provider-forward",
                }
            ]
        },
    )

    assert "path" not in artifacts["sidecars"][0]
    assert "cache_path" not in artifacts["sidecars"][0]
    assert artifacts["sidecars"][0]["storage_uri"] == "db://ptg2_artifact/provider-forward"


@pytest.mark.asyncio
async def test_ptg2_manifest_publish_uploads_base_artifact_sidecars_to_db(monkeypatch, tmp_path):
    provider_path = tmp_path / "provider_forward.ptg2sc"
    provider_path.write_bytes(b"PTG2MNDS" + b"\0" * 32)
    price_path = tmp_path / "price_forward.ptg2sc"
    price_path.write_bytes(b"PTG2MNSC" + b"\0" * 32)
    calls = []
    progress_events = []

    async def fake_store(path, **kwargs):
        calls.append((Path(path), kwargs))
        metadata = dict(kwargs["metadata"])
        metadata.update(
            {
                "storage_uri": f"db://ptg2_artifact/{kwargs['name']}",
                "artifact_id": kwargs["name"],
            }
        )
        return metadata

    monkeypatch.setattr(ptg_manifest_publish, "ptg2_artifact_db_store_enabled", lambda: True)
    monkeypatch.setattr(ptg_manifest_publish, "store_ptg2_artifact_file_in_db", fake_store)
    monkeypatch.setattr(ptg_manifest_publish, "write_live_progress", lambda **payload: progress_events.append(payload))

    base_artifacts, base_sidecars = ptg_manifest_publish._split_ptg2_manifest_base_artifacts(
        {
            "source_trace_set_hash": "trace-set",
            "sidecars": [
                {"name": "provider_forward", "path": str(provider_path), "sha256": "provider-sha"},
            ],
        }
    )
    combined = ptg_manifest_publish._merge_ptg2_manifest_sidecar_artifacts(
        base_sidecars,
        {"price_forward": {"name": "price_forward", "path": str(price_path), "sha256": "price-sha"}},
    )
    stored = await ptg_manifest_publish._store_ptg2_manifest_sidecar_artifacts_in_db(
        schema_name="mrf",
        snapshot_id="ptg2:test",
        sidecar_artifacts=combined,
    )
    manifest = ptg_manifest_publish._ptg2_manifest_artifacts_manifest(
        artifacts=base_artifacts,
        sidecar_artifacts=stored,
    )

    sidecars = {sidecar["name"]: sidecar for sidecar in manifest["sidecars"]}
    assert manifest["source_trace_set_hash"] == "trace-set"
    assert set(sidecars) == {"provider_forward", "price_forward"}
    assert sidecars["provider_forward"]["storage_uri"] == "db://ptg2_artifact/provider_forward"
    assert sidecars["price_forward"]["storage_uri"] == "db://ptg2_artifact/price_forward"
    assert [call[0] for call in calls] == [provider_path, price_path]
    assert [event["artifact_name"] for event in progress_events if event["publish_step"] == "artifact upload complete"] == [
        "provider_forward",
        "price_forward",
    ]


@pytest.mark.asyncio
async def test_ptg2_manifest_serving_sidecars_use_rust_copy_fast_path(monkeypatch, tmp_path):
    artifact_helpers = importlib.import_module("process.ptg_parts.ptg2_manifest_artifacts")
    copied_sql = []
    runner_calls = []
    progress_events = []

    async def fake_copy_query_to_file(sql, output_path):
        copied_sql.append(sql)
        if "ORDER BY code_key" in sql:
            output_path.write_text(
                "1\t2\t5\t11111111111111111111111111111111\n"
                "1\t3\t7\t22222222222222222222222222222222\n"
                "2\t2\t5\t11111111111111111111111111111111\n"
            )
        else:
            output_path.write_text(
                "2\t1\t5\t11111111111111111111111111111111\n"
                "2\t2\t5\t11111111111111111111111111111111\n"
                "3\t1\t7\t22222222222222222222222222222222\n"
            )

    def fake_runner(kind, copy_path, output_path):
        runner_calls.append((kind, Path(copy_path), Path(output_path)))
        rows = [line.split("\t") for line in Path(copy_path).read_text().splitlines()]
        if kind == "by_code":
            artifact_helpers.write_serving_by_code_sidecar(output_path, rows)
        else:
            artifact_helpers.write_serving_by_provider_set_sidecar(output_path, rows)

    monkeypatch.setattr(ptg_manifest_publish, "_ptg2_manifest_serving_sidecar_rust_enabled", lambda: True)
    monkeypatch.setattr(ptg_manifest_publish, "_ptg2_rust_scanner_binary", lambda: tmp_path / "ptg2_scanner")
    monkeypatch.setattr(ptg_manifest_publish, "_copy_ptg2_query_to_file", fake_copy_query_to_file)
    monkeypatch.setattr(ptg_manifest_publish, "_run_ptg2_serving_sidecar_from_key_copy", fake_runner)
    monkeypatch.setattr(ptg_manifest_publish, "write_live_progress", lambda **payload: progress_events.append(payload))

    sidecars = await ptg_manifest_publish._write_ptg2_manifest_serving_sidecars_rust(
        schema_name="mrf",
        final_table="ptg2_serving_test",
        artifact_dir=tmp_path,
        expected_row_count=3,
    )

    assert sidecars is not None
    assert set(sidecars) == {"serving_by_code", "serving_by_provider_set"}
    assert sidecars["serving_by_code"]["row_count"] == 3
    assert sidecars["serving_by_provider_set"]["row_count"] == 3
    assert [call[0] for call in runner_calls] == ["by_code", "by_provider_set"]
    assert len(copied_sql) == 2
    assert not any(path.name.startswith("serving_by_code_") and path.suffix == ".copy" for path in tmp_path.iterdir())
    assert "serving sidecars export by code" in [event["publish_step"] for event in progress_events]
    assert "serving sidecars encode reverse" in [event["publish_step"] for event in progress_events]
    assert progress_events[-1]["publish_step"] == "serving sidecars complete"


def test_ptg2_compact_finalize_defers_provider_locations_by_default(monkeypatch):
    status_calls = []
    scalar_values = iter([3, 1, 2])

    async def fake_status(statement, **_params):
        status_calls.append(statement)

    async def fake_scalar(_statement, **_params):
        return next(scalar_values)

    async def fail_provider_locations(_snapshot_id):
        raise AssertionError("provider locations should be deferred by default")

    monkeypatch.delenv(process_ptg.PTG2_DEFER_PROVIDER_LOCATIONS_ENV, raising=False)
    monkeypatch.setattr(process_ptg.db, "status", fake_status)
    monkeypatch.setattr(process_ptg.db, "scalar", fake_scalar)
    monkeypatch.setattr(process_ptg, "_build_ptg2_provider_locations", fail_provider_locations)

    result = asyncio.run(process_ptg.build_ptg2_compact_serving_index("snap", "run"))

    assert result["rate_count"] == 3
    assert any("ANALYZE mrf.ptg2_serving_rate_compact" in statement for statement in status_calls)


def test_ptg2_rust_snapshot_publish_renames_dictionary_stages_before_index(monkeypatch):
    """Verify this PTG import regression contract."""
    status_calls = []

    async def fake_status(statement, **_params):
        status_calls.append(statement)

    async def fake_all(statement, **_params):
        if "COUNT(DISTINCT plan_hash)" in statement:
            return [{"plans": 1}]
        if "COUNT(DISTINCT COALESCE" in statement:
            return [{"procedures": 2}]
        return []

    async def is_fake_table_present(_schema, _table):
        return True

    monkeypatch.setattr(process_ptg.db, "status", fake_status)
    monkeypatch.setattr(process_ptg.db, "all", fake_all)
    monkeypatch.setattr(ptg_rust_publish, "_table_exists", is_fake_table_present)
    monkeypatch.setattr(ptg_rust_publish, "_table_has_rows", AsyncMock(return_value=True))
    async def fake_estimated_rows(_schema, _table_name):
        return 123

    estimate_mock = AsyncMock(side_effect=fake_estimated_rows)
    exact_mock = AsyncMock(return_value=987)
    monkeypatch.setattr(ptg_rust_publish, "_estimated_table_rows", estimate_mock)
    monkeypatch.setattr(ptg_rust_publish, "_exact_table_rows", exact_mock)

    result = asyncio.run(
        process_ptg._publish_rust_compact_snapshot_tables(
            {
                "serving_rate_compact": "ptg2_rust_stage_serving_rate_compact_abc",
                "price_code_set": "ptg2_rust_stage_price_code_set_abc",
                "price_atom": "ptg2_rust_stage_price_atom_abc",
                "price_set": "ptg2_rust_stage_price_set_abc",
                "price_set_entry": "ptg2_rust_stage_price_set_entry_abc",
                "procedure": "ptg2_rust_stage_procedure_abc",
                "provider_set": "ptg2_rust_stage_provider_set_abc",
                "provider_set_component": "ptg2_rust_stage_provider_set_component_abc",
                "provider_group_member": "ptg2_rust_stage_provider_group_member_abc",
            },
            snapshot_id="ptg2:202604:snap",
            import_run_id="ptg2:run",
            source_key="example_dental",
        )
    )

    joined = "\n".join(status_calls)
    assert result["storage"] == "db_compact_snapshot"
    assert result["source_key"] == "example_dental"
    assert result["table"].startswith("mrf.ptg2_serving_rate_compact_")
    assert result["price_code_set_table"].startswith("mrf.ptg2_price_code_set_")
    assert result["price_atom_table"].startswith("mrf.ptg2_price_atom_")
    assert "price_table" not in result
    assert result["price_set_entry_table"].startswith("mrf.ptg2_price_set_entry_")
    assert result["provider_set_component_table"].startswith("mrf.ptg2_provider_set_component_")
    assert result["provider_set_entry_table"] is None
    assert result["provider_entry_component_table"] is None
    assert result["rate_count"] == 987
    assert result["serving_rates"] == 987
    assert result["row_count"] == 987
    assert "RENAME TO" in joined
    assert "INSERT INTO" not in joined
    assert "SELECT DISTINCT ON" not in joined
    assert "ptg2_price_set (" not in joined
    assert "ptg2_rust_stage_serving_rate_compact_abc" in joined
    assert "ptg2_rust_stage_price_code_set_abc" in joined
    assert "ptg2_rust_stage_price_atom_abc" in joined
    assert "ptg2_rust_stage_price_set_entry_abc" in joined
    assert "ptg2_rust_stage_provider_set_component_abc" in joined
    assert "provider_group_hashes" not in "\n".join(
        statement for statement in status_calls if "CREATE INDEX" in statement and "ptg2_provider_set_" in statement
    )
    assert "PTG2_PUBLISH_STAGE_INDEX_START" not in joined
    assert "CREATE INDEX IF NOT EXISTS" in joined
    exact_mock.assert_awaited_once()
    assert exact_mock.await_args.args[1].startswith("ptg2_serving_rate_compact_")
    serving_estimate_calls = [
        call for call in estimate_mock.await_args_list
        if call.args[1].startswith("ptg2_serving_rate_compact_")
    ]
    assert serving_estimate_calls == []


def test_ptg2_serving_stage_table_for_copy_uses_worker_lane():
    stage_tables = {
        "serving_rate_compact": "stage_w0000",
        process_ptg._serving_stage_lane_key(1): "stage_w0001",
        process_ptg._serving_stage_lane_key(7): "stage_w0007",
    }

    assert (
        process_ptg._serving_stage_table_for_copy(
            stage_tables,
            Path("/tmp/ptg2.copy.worker0001.part000002.ready"),
        )
        == "stage_w0001"
    )
    assert (
        process_ptg._serving_stage_table_for_copy(
            stage_tables,
            Path("/tmp/ptg2.copy.worker0000.part000002.ready"),
        )
        == "stage_w0000"
    )
    assert (
        process_ptg._serving_stage_table_for_copy(
            stage_tables,
            Path("/tmp/ptg2.copy.worker0009.part000002.ready"),
        )
        == "stage_w0000"
    )


def test_ptg2_rust_snapshot_publish_inherits_serving_stage_lanes(monkeypatch):
    status_calls = []

    async def fake_status(statement, **_params):
        status_calls.append(statement)

    async def fake_all(statement, **_params):
        if "COUNT(DISTINCT plan_hash)" in statement:
            return [{"plans": 1}]
        return []

    async def is_fake_table_present(_schema, _table):
        return True

    async def fake_table_has_rows(_schema, table):
        return table != "stage_empty_base"

    async def fake_estimated_rows(_schema, _table_name):
        return 0

    async def fake_exact_rows(_schema, table_name):
        if table_name.endswith("_p00"):
            return 100
        if table_name.endswith("_p01"):
            return 200
        return 0

    monkeypatch.setattr(process_ptg.db, "status", fake_status)
    monkeypatch.setattr(process_ptg.db, "all", fake_all)
    monkeypatch.setattr(ptg_rust_publish, "_table_exists", is_fake_table_present)
    monkeypatch.setattr(ptg_rust_publish, "_table_has_rows", fake_table_has_rows)
    monkeypatch.setattr(ptg_rust_publish, "_estimated_table_rows", fake_estimated_rows)
    monkeypatch.setattr(ptg_rust_publish, "_exact_table_rows", fake_exact_rows)

    result = asyncio.run(
        process_ptg._publish_rust_compact_snapshot_tables(
            {
                "serving_rate_compact": "stage_empty_base",
                process_ptg._serving_stage_lane_key(1): "stage_worker_0001",
                process_ptg._serving_stage_lane_key(2): "stage_worker_0002",
            },
            snapshot_id="ptg2:202604:snap",
            import_run_id="ptg2:run",
            source_key="example_dental",
        )
    )

    joined = "\n".join(status_calls)
    assert result["storage"] == "db_compact_snapshot"
    assert result["rate_count"] == 300
    assert result["table"].startswith("mrf.ptg2_serving_rate_compact_")
    assert "INHERIT" in joined
    assert "stage_worker_0001" in joined
    assert "stage_worker_0002" in joined
    assert "INSERT INTO" not in joined
    assert "_p00" in joined
    assert "_p01" in joined


def test_ptg2_manifest_snapshot_publish_direct_renames_and_indexes(monkeypatch):
    status_calls = []

    async def fake_status(statement, **_params):
        status_calls.append(statement)

    monkeypatch.setenv("HLTHPRT_PTG2_PUBLISH_DB_DEDUPE_FALLBACK", "false")
    monkeypatch.setattr(ptg_manifest_publish.db, "status", fake_status)
    monkeypatch.setattr(ptg_manifest_publish, "_table_exists", AsyncMock(return_value=True))
    monkeypatch.setattr(ptg_manifest_publish, "_table_has_rows", AsyncMock(return_value=True))
    monkeypatch.setattr(ptg_manifest_publish, "_exact_table_rows", AsyncMock(return_value=321))

    publish_manifest = asyncio.run(
        process_ptg._publish_ptg2_manifest_serving_snapshot(
            "ptg2_manifest_stage_serving_abc",
            snapshot_id="ptg2:202604:snap",
            source_key="example_dental",
            artifacts={"manifest_uri": "file:///tmp/snapshot.manifest.json"},
            sidecar_artifacts={
                "provider_set_members": {
                    "kind": "provider_set_members",
                    "path": "provider_set_members.global_membership.bin",
                    "sha256": "0" * 64,
                    "byte_count": 0,
                }
            },
        )
    )

    joined = "\n".join(status_calls)
    assert publish_manifest["storage"] == "manifest_snapshot"
    assert publish_manifest["type"] == "ptg2_serving"
    assert publish_manifest["id_storage"] == "uuid"
    assert publish_manifest["rate_count"] == 321
    assert publish_manifest["serving_rates"] == 321
    assert publish_manifest["table"].startswith("mrf.ptg2_serving_")
    assert publish_manifest["provider_group_location_table"].startswith("mrf.ptg2_provider_group_location_")
    assert publish_manifest["artifacts"]["manifest_uri"] == "file:///tmp/snapshot.manifest.json"
    assert publish_manifest["artifacts"]["sidecars"][0]["name"] == "provider_set_members"
    assert "RENAME TO" in joined
    assert "snapshot_id" not in joined
    assert "plan_id, reported_code_system, reported_code" in joined
    assert (
        "plan_id, reported_code_system, reported_code, provider_set_global_id_128, "
        "provider_count DESC NULLS LAST, serving_content_hash_128"
    ) in joined
    assert "CREATE UNLOGGED TABLE" in joined
    assert "provider_group_global_id_128 uuid NOT NULL" in joined
    assert "SELECT DISTINCT ON (serving_content_hash_128)" not in joined
    assert "SELECT DISTINCT ON (price_atom_global_id_128)" not in joined
    assert "SELECT DISTINCT ON (provider_group_global_id_128, npi)" not in joined
    assert "CREATE UNIQUE INDEX" in joined


def test_ptg2_manifest_snapshot_publish_can_use_lean_provider_key_layout(monkeypatch):
    status_calls = []
    monkeypatch.setenv("HLTHPRT_PTG2_SNAPSHOT_ARCH", "sidecar_scope_v1")

    async def fake_status(statement, **_params):
        status_calls.append(statement)

    async def is_fake_table_present(_schema, table):
        return table == "ptg2_manifest_stage_serving_abc"

    async def fake_all(statement, **_params):
        assert "GROUP BY source_trace_set_hash, network_names" in statement
        return [{"source_trace_set_hash": "trace-set-hash", "network_names": ["C2"]}]

    monkeypatch.setenv(
        "HLTHPRT_PTG2_MANIFEST_SERVING_LAYOUT",
        ptg_manifest_publish.PTG2_MANIFEST_SERVING_LAYOUT_LEAN_PROVIDER_KEY,
    )
    monkeypatch.setenv("HLTHPRT_PTG2_MANIFEST_LEAN_DIRECT_COPY", "false")
    monkeypatch.setenv("HLTHPRT_PTG2_PUBLISH_DB_DEDUPE_FALLBACK", "false")
    monkeypatch.setenv("HLTHPRT_PTG2_MANIFEST_SERVING_SIDECARS_ENABLED", "false")
    monkeypatch.setattr(ptg_manifest_publish.db, "status", fake_status)
    monkeypatch.setattr(ptg_manifest_publish.db, "all", fake_all)
    monkeypatch.setattr(ptg_manifest_publish, "_table_exists", is_fake_table_present)
    monkeypatch.setattr(ptg_manifest_publish, "_table_has_rows", AsyncMock(return_value=True))
    monkeypatch.setattr(ptg_manifest_publish, "_exact_table_rows", AsyncMock(return_value=321))
    component_mock = AsyncMock(side_effect=AssertionError("lean provider-key snapshots should use sidecars by default"))
    monkeypatch.setattr(ptg_manifest_publish, "_materialize_manifest_components", component_mock)

    publish_manifest = asyncio.run(
        process_ptg._publish_ptg2_manifest_serving_snapshot(
            "ptg2_manifest_stage_serving_abc",
            snapshot_id="ptg2:202604:snap",
            source_key="example_dental",
        )
    )

    joined = "\n".join(status_calls)
    assert publish_manifest["serving_table_layout"] == "lean_provider_key_v1"
    assert publish_manifest["provider_set_dictionary_table"].startswith("mrf.ptg2_provider_set_dict_")
    assert publish_manifest["provider_set_component_table"] is None
    assert publish_manifest["provider_group_location_table"] is None
    assert publish_manifest["provider_group_rate_scope_table"] is None
    assert publish_manifest["source_trace_set_hash"] == "trace-set-hash"
    assert publish_manifest["network_names"] == ["C2"]
    assert "provider_set_key" in joined
    assert "price_set_global_id_128" in joined
    assert "lean_code_idx" in joined
    assert "(reported_code_system, reported_code)" in joined
    assert "INCLUDE (code_key, plan_id, rate_count)" in joined
    assert "(code_key, provider_count DESC NULLS LAST)" not in joined
    assert "(code_key)" in joined
    assert "code_count.reported_code IS NOT DISTINCT FROM serving.reported_code" in joined
    assert "INCLUDE (provider_set_global_id_128)" in joined
    assert "plan_code_provider_set_idx" not in joined
    component_mock.assert_not_awaited()


def test_direct_lean_swap_keeps_null_reported_codes(monkeypatch):
    status_calls = []

    async def fake_status(statement, **_params):
        status_calls.append(statement)

    monkeypatch.setattr(ptg_manifest_publish.db, "status", fake_status)

    asyncio.run(
        ptg_manifest_publish._swap_direct_lean_stage(
            schema_name="mrf",
            final_table="ptg2_serving_source",
            lean_table="ptg2_serving_lean",
            code_count_table="ptg2_code_count",
            provider_set_dictionary_table="ptg2_provider_sets",
            storage_mode="UNLOGGED ",
        )
    )

    joined = "\n".join(status_calls)
    assert "code_count.reported_code_system IS NOT DISTINCT FROM serving.reported_code_system" in joined
    assert "code_count.reported_code IS NOT DISTINCT FROM serving.reported_code" in joined


def test_ptg2_manifest_snapshot_publish_attaches_serving_sidecars_and_drops_table(monkeypatch):
    """Verify this PTG import regression contract."""
    status_calls = []
    monkeypatch.setenv("HLTHPRT_PTG2_SNAPSHOT_ARCH", "sidecar_scope_v1")

    async def fake_status(statement, **_params):
        status_calls.append(statement)

    async def is_fake_table_present(_schema, table):
        return table == "ptg2_manifest_stage_serving_abc"

    async def fake_all(statement, **_params):
        assert "GROUP BY source_trace_set_hash, network_names" in statement
        return [{"source_trace_set_hash": "trace-set-hash", "network_names": ["C2"]}]

    async def fake_write_sidecars(**kwargs):
        assert kwargs["schema_name"] == "mrf"
        assert kwargs["final_table"].startswith("ptg2_serving_")
        return {
            "serving_by_code": {
                "name": "serving_by_code",
                "kind": "ptg2_serving_by_code",
                "path": "/tmp/serving_by_code_v1.ptg2sbc",
                "sha256": "a" * 64,
                "byte_count": 123,
                "row_count": 321,
            },
            "serving_by_provider_set": {
                "name": "serving_by_provider_set",
                "kind": "ptg2_serving_by_provider_set",
                "path": "/tmp/serving_by_provider_set_v1.ptg2sbp",
                "sha256": "b" * 64,
                "byte_count": 45,
                "row_count": 321,
            },
        }

    monkeypatch.setenv(
        "HLTHPRT_PTG2_MANIFEST_SERVING_LAYOUT",
        ptg_manifest_publish.PTG2_MANIFEST_SERVING_LAYOUT_LEAN_PROVIDER_KEY,
    )
    monkeypatch.setenv("HLTHPRT_PTG2_MANIFEST_LEAN_DIRECT_COPY", "false")
    monkeypatch.setenv("HLTHPRT_PTG2_PUBLISH_DB_DEDUPE_FALLBACK", "false")
    monkeypatch.setattr(ptg_manifest_publish.db, "status", fake_status)
    monkeypatch.setattr(ptg_manifest_publish.db, "all", fake_all)
    monkeypatch.setattr(ptg_manifest_publish, "_table_exists", is_fake_table_present)
    monkeypatch.setattr(ptg_manifest_publish, "_table_has_rows", AsyncMock(return_value=True))
    monkeypatch.setattr(ptg_manifest_publish, "_exact_table_rows", AsyncMock(return_value=321))
    monkeypatch.setattr(ptg_manifest_publish, "_write_ptg2_manifest_serving_sidecars", fake_write_sidecars)

    publish_manifest = asyncio.run(
        process_ptg._publish_ptg2_manifest_serving_snapshot(
            "ptg2_manifest_stage_serving_abc",
            snapshot_id="ptg2:202604:snap",
            source_key="example_dental",
        )
    )

    sidecar_names = [entry["name"] for entry in publish_manifest["artifacts"]["sidecars"]]
    assert "serving_by_code" in sidecar_names
    assert "serving_by_provider_set" in sidecar_names
    assert publish_manifest["serving_row_strategy"] == "sidecar"
    assert publish_manifest["serving_table_retained"] is False
    assert any("DROP TABLE \"mrf\".\"ptg2_serving_" in statement for statement in status_calls)


async def _fake_postgres_binary_write_result(**kwargs):
    assert kwargs["schema_name"] == "mrf"
    assert kwargs["source_table"].startswith("ptg2_serving_")
    assert kwargs["target_table"].startswith("ptg2_serving_binary_")
    return {
        "format": "ptg2_serving_binary_blocks_v1",
        "table": f"mrf.{kwargs['target_table']}",
        "writer": "rust_stream",
        "row_count": kwargs["expected_row_count"],
        "timing": {"total_seconds": 1.25, "index_seconds": 0.05},
        "build_elapsed_seconds": 1.25,
        "storage": {"total_bytes": 2048},
    }


def test_ptg2_manifest_snapshot_publish_postgres_binary_skips_serving_sidecars(monkeypatch):
    status_calls = []

    async def fake_binary_write(**kwargs):
        assert kwargs["expected_price_set_count"] == 29_122_132
        return await _fake_postgres_binary_write_result(**kwargs)

    async def fake_status(statement, **_params):
        status_calls.append(statement)

    async def is_fake_table_present(_schema, table):
        return table == "ptg2_manifest_stage_serving_abc"

    async def fake_all(statement, **_params):
        assert "GROUP BY source_trace_set_hash, network_names" in statement
        return [{"source_trace_set_hash": "trace-set-hash", "network_names": ["C2"]}]

    sidecar_mock = AsyncMock(side_effect=AssertionError("postgres_binary_v1 must not build serving sidecars"))

    monkeypatch.setenv("HLTHPRT_PTG2_SNAPSHOT_ARCH", "postgres_binary_v1")
    monkeypatch.setenv(
        "HLTHPRT_PTG2_MANIFEST_SERVING_LAYOUT",
        ptg_manifest_publish.PTG2_MANIFEST_SERVING_LAYOUT_LEAN_PROVIDER_KEY,
    )
    monkeypatch.setenv("HLTHPRT_PTG2_MANIFEST_LEAN_DIRECT_COPY", "false")
    monkeypatch.setenv("HLTHPRT_PTG2_PUBLISH_DB_DEDUPE_FALLBACK", "false")
    monkeypatch.setattr(ptg_manifest_publish.db, "status", fake_status)
    monkeypatch.setattr(ptg_manifest_publish.db, "all", fake_all)
    monkeypatch.setattr(ptg_manifest_publish, "_table_exists", is_fake_table_present)
    monkeypatch.setattr(ptg_manifest_publish, "_table_has_rows", AsyncMock(return_value=True))
    monkeypatch.setattr(ptg_manifest_publish, "_exact_table_rows", AsyncMock(return_value=321))
    monkeypatch.setattr(ptg_manifest_publish, "_write_ptg2_manifest_serving_sidecars", sidecar_mock)
    monkeypatch.setattr(ptg_manifest_publish, "write_ptg2_serving_binary_table", fake_binary_write)

    publish_manifest = asyncio.run(
        process_ptg._publish_ptg2_manifest_serving_snapshot(
            "ptg2_manifest_stage_serving_abc",
            snapshot_id="ptg2:202604:snap",
            source_key="example_dental",
            known_counts={"price_sets": 29_122_132},
        )
    )

    assert publish_manifest["arch_version"] == "postgres_binary_v1"
    assert publish_manifest["serving_row_strategy"] == "postgres_binary"
    assert publish_manifest["serving_table_retained"] is False
    assert publish_manifest["serving_binary_table"].startswith("mrf.ptg2_serving_binary_")
    assert publish_manifest["materialized_tables"]["serving_binary"] == publish_manifest["serving_binary_table"]
    assert "serving" not in publish_manifest["materialized_tables"]
    assert publish_manifest["serving_binary"]["storage"]["total_bytes"] == 2048
    assert publish_manifest["serving_binary"]["build_elapsed_seconds"] == 1.25
    assert publish_manifest["serving_binary"]["timing"]["index_seconds"] == 0.05
    sidecar_mock.assert_not_awaited()
    assert any("DROP TABLE \"mrf\".\"ptg2_serving_" in statement for statement in status_calls)


def test_ptg2_manifest_snapshot_publish_v2_uses_membership_graph(monkeypatch):
    """V2 publishes the graph scope even when the snapshot contains no NPIs."""
    status_calls = []
    expected_scope_table = ptg_manifest_publish._ptg2_snapshot_table_name(
        "provider_npi_scope",
        "example_dental",
        "ptg2:202604:snap",
    )
    async def fake_status(statement, **_params):
        status_calls.append(statement)
    async def is_fake_table_present(_schema, table):
        return table in {
            "ptg2_manifest_stage_serving_abc",
            "ptg2_manifest_stage_provider_npi_scope_abc",
            expected_scope_table,
        }

    async def fake_all(statement, **_params):
        assert "GROUP BY source_trace_set_hash, network_names" in statement
        return [{"source_trace_set_hash": "trace-set-hash", "network_names": ["C2"]}]

    monkeypatch.setenv("HLTHPRT_PTG2_SNAPSHOT_ARCH", "postgres_binary_v2")
    monkeypatch.setenv(
        "HLTHPRT_PTG2_MANIFEST_SERVING_LAYOUT",
        ptg_manifest_publish.PTG2_MANIFEST_SERVING_LAYOUT_LEAN_PROVIDER_KEY,
    )
    monkeypatch.setenv("HLTHPRT_PTG2_MANIFEST_LEAN_DIRECT_COPY", "false")
    monkeypatch.setenv("HLTHPRT_PTG2_PUBLISH_DB_DEDUPE_FALLBACK", "false")
    monkeypatch.setattr(ptg_manifest_publish.db, "status", fake_status)
    monkeypatch.setattr(ptg_manifest_publish.db, "all", fake_all)
    monkeypatch.setattr(ptg_manifest_publish, "_table_exists", is_fake_table_present)
    empty_scope_tables = {"ptg2_manifest_stage_provider_npi_scope_abc", expected_scope_table}
    monkeypatch.setattr(
        ptg_manifest_publish,
        "_table_has_rows",
        AsyncMock(side_effect=lambda _schema, table: table not in empty_scope_tables),
    )
    monkeypatch.setattr(ptg_manifest_publish, "_exact_table_rows", AsyncMock(return_value=321))
    monkeypatch.setattr(
        ptg_manifest_publish,
        "_write_ptg2_manifest_serving_sidecars",
        AsyncMock(side_effect=AssertionError("postgres_binary_v2 must not build serving sidecar files")),
    )
    monkeypatch.setattr(ptg_manifest_publish, "write_ptg2_serving_binary_table", _fake_postgres_binary_write_result)

    publish_manifest = asyncio.run(
        process_ptg._publish_ptg2_manifest_serving_snapshot(
            "ptg2_manifest_stage_serving_abc",
            snapshot_id="ptg2:202604:snap",
            source_key="example_dental",
        )
    )

    assert publish_manifest["arch_version"] == "postgres_binary_v2"
    assert publish_manifest["provider_group_member_table"] is None
    assert publish_manifest["provider_npi_scope_table"] == f"mrf.{expected_scope_table}"
    assert publish_manifest["materialized_tables"]["provider_npi_scope"] == publish_manifest["provider_npi_scope_table"]
    assert "provider_group_member" not in publish_manifest["materialized_tables"]
    assert any("DROP TABLE IF EXISTS \"mrf\".\"ptg2_manifest_stage_provider_group_member_abc\"" in statement for statement in status_calls)


def test_ptg2_manifest_snapshot_publish_can_use_direct_lean_source_layout(monkeypatch):
    status_calls = []
    monkeypatch.setenv("HLTHPRT_PTG2_SNAPSHOT_ARCH", "sidecar_scope_v1")

    async def fake_status(statement, **_params):
        status_calls.append(statement)

    async def is_fake_table_present(_schema, table):
        return table == "ptg2_manifest_stage_serving_abc"

    db_all_mock = AsyncMock(side_effect=AssertionError("direct lean publish should use artifact constants"))

    monkeypatch.setenv(
        "HLTHPRT_PTG2_MANIFEST_SERVING_LAYOUT",
        ptg_manifest_publish.PTG2_MANIFEST_SERVING_LAYOUT_LEAN_PROVIDER_KEY,
    )
    monkeypatch.setenv("HLTHPRT_PTG2_MANIFEST_LEAN_DIRECT_COPY", "true")
    monkeypatch.setenv("HLTHPRT_PTG2_PUBLISH_DB_DEDUPE_FALLBACK", "false")
    monkeypatch.setenv("HLTHPRT_PTG2_MANIFEST_SERVING_SIDECARS_ENABLED", "false")
    monkeypatch.setattr(ptg_manifest_publish.db, "status", fake_status)
    monkeypatch.setattr(ptg_manifest_publish.db, "all", db_all_mock)
    monkeypatch.setattr(ptg_manifest_publish, "_table_exists", is_fake_table_present)
    monkeypatch.setattr(ptg_manifest_publish, "_table_has_rows", AsyncMock(return_value=True))
    monkeypatch.setattr(ptg_manifest_publish, "_exact_table_rows", AsyncMock(return_value=321))
    component_mock = AsyncMock(side_effect=AssertionError("lean provider-key snapshots should use sidecars by default"))
    monkeypatch.setattr(ptg_manifest_publish, "_materialize_manifest_components", component_mock)

    publish_manifest = asyncio.run(
        process_ptg._publish_ptg2_manifest_serving_snapshot(
            "ptg2_manifest_stage_serving_abc",
            snapshot_id="ptg2:202604:snap",
            source_key="example_dental",
            artifacts={"source_trace_set_hash": "trace-set-hash", "network_names": ["C2"]},
        )
    )

    joined = "\n".join(status_calls)
    assert publish_manifest["serving_table_layout"] == "lean_provider_key_v1"
    assert publish_manifest["serving_stage_layout"] == "lean_source_v1"
    assert publish_manifest["provider_set_dictionary_table"].startswith("mrf.ptg2_provider_set_dict_")
    assert publish_manifest["source_trace_set_hash"] == "trace-set-hash"
    assert publish_manifest["network_names"] == ["C2"]
    assert "serving_content_hash_128" not in joined
    assert "procedure_global_id_128" not in joined
    assert "source_trace_set_hash" not in joined
    assert "GROUP BY source_trace_set_hash, network_names" not in joined
    assert "SELECT DISTINCT ON (serving_content_hash_128)" not in joined
    assert "CREATE UNIQUE INDEX" in joined
    for column in (
        "plan_id",
        "reported_code_system",
        "reported_code",
        "provider_set_global_id_128",
        "price_set_global_id_128",
    ):
        assert column in joined
    component_mock.assert_not_awaited()
    db_all_mock.assert_not_awaited()


def test_ptg2_manifest_snapshot_publish_direct_lean_sidecars_skip_final_serving_table(monkeypatch):
    """Verify this PTG import regression contract."""
    status_calls = []
    monkeypatch.setenv("HLTHPRT_PTG2_SNAPSHOT_ARCH", "sidecar_scope_v1")

    async def fake_status(statement, **_params):
        status_calls.append(statement)

    async def is_fake_table_present(_schema, table):
        return table == "ptg2_manifest_stage_serving_abc"

    async def fake_write_sidecars(**kwargs):
        assert kwargs["schema_name"] == "mrf"
        assert kwargs["final_table"] == "ptg2_manifest_stage_serving_abc"
        return {
            "serving_by_code": {
                "name": "serving_by_code",
                "kind": "ptg2_serving_by_code",
                "path": "/tmp/serving_by_code_v1.ptg2sbc",
                "sha256": "a" * 64,
                "byte_count": 123,
                "row_count": 321,
            },
            "serving_by_provider_set": {
                "name": "serving_by_provider_set",
                "kind": "ptg2_serving_by_provider_set",
                "path": "/tmp/serving_by_provider_set_v1.ptg2sbp",
                "sha256": "b" * 64,
                "byte_count": 45,
                "row_count": 321,
            },
        }

    exact_rows_mock = AsyncMock(return_value=321)

    monkeypatch.setenv(
        "HLTHPRT_PTG2_MANIFEST_SERVING_LAYOUT",
        ptg_manifest_publish.PTG2_MANIFEST_SERVING_LAYOUT_LEAN_PROVIDER_KEY,
    )
    monkeypatch.setenv("HLTHPRT_PTG2_MANIFEST_LEAN_DIRECT_COPY", "true")
    monkeypatch.setenv("HLTHPRT_PTG2_PUBLISH_DB_DEDUPE_FALLBACK", "false")
    monkeypatch.setenv("HLTHPRT_PTG2_MANIFEST_SERVING_SIDECARS_ENABLED", "true")
    monkeypatch.setenv("HLTHPRT_PTG2_MANIFEST_DROP_SERVING_TABLE_AFTER_SIDECARS", "true")
    monkeypatch.setattr(ptg_manifest_publish.db, "status", fake_status)
    monkeypatch.setattr(ptg_manifest_publish.db, "all", AsyncMock(side_effect=AssertionError("unused")))
    monkeypatch.setattr(ptg_manifest_publish, "_table_exists", is_fake_table_present)
    monkeypatch.setattr(ptg_manifest_publish, "_table_has_rows", AsyncMock(return_value=True))
    monkeypatch.setattr(ptg_manifest_publish, "_exact_table_rows", exact_rows_mock)
    monkeypatch.setattr(ptg_manifest_publish, "_write_ptg2_manifest_serving_sidecars", fake_write_sidecars)

    publish_manifest = asyncio.run(
        process_ptg._publish_ptg2_manifest_serving_snapshot(
            "ptg2_manifest_stage_serving_abc",
            snapshot_id="ptg2:202604:snap",
            source_key="example_dental",
            artifacts={"source_trace_set_hash": "trace-set-hash", "network_names": ["C2"]},
        )
    )

    stage_to_final_renames = [
        statement
        for statement in status_calls
        if 'ALTER TABLE "mrf"."ptg2_manifest_stage_serving_abc"' in statement
        and 'RENAME TO "ptg2_serving_' in statement
    ]
    assert stage_to_final_renames == []
    assert any('DROP TABLE "mrf"."ptg2_manifest_stage_serving_abc"' in statement for statement in status_calls)
    assert "lean_code_lookup_idx" not in "\n".join(status_calls)
    assert "lean_code_idx" in "\n".join(status_calls)
    assert 'ANALYZE "mrf"."ptg2_manifest_stage_serving_abc";' not in "\n".join(status_calls)
    assert publish_manifest["serving_row_strategy"] == "sidecar"
    assert publish_manifest["serving_table_retained"] is False
    assert publish_manifest["table"].startswith("mrf.ptg2_serving_")
    exact_rows_mock.assert_awaited_with("mrf", "ptg2_manifest_stage_serving_abc")


def test_postgres_binary_direct_lean_skips_guard(monkeypatch):
    """Verify this PTG import regression contract."""
    status_calls = []

    async def fake_status(statement, **_params):
        status_calls.append(statement)

    async def is_fake_table_present(_schema, table):
        return table in {
            "ptg2_manifest_stage_serving_abc",
            "ptg2_manifest_stage_code_count_abc",
            "ptg2_manifest_stage_provider_set_dictionary_abc",
        }

    async def fake_table_has_rows(_schema, table):
        return table in {
            "ptg2_manifest_stage_serving_abc",
            "ptg2_manifest_stage_code_count_abc",
            "ptg2_manifest_stage_provider_set_dictionary_abc",
        }

    async def fake_binary_write(**kwargs):
        assert kwargs["schema_name"] == "mrf"
        assert kwargs["source_table"] == "ptg2_manifest_stage_serving_abc"
        assert kwargs["target_table"].startswith("ptg2_serving_binary_")
        assert kwargs["source_layout"] == "natural_lean"
        assert kwargs["code_count_table"].startswith("ptg2_code_count_")
        assert kwargs["provider_set_dictionary_table"].startswith("ptg2_provider_set_dict_")
        return {
            "format": "ptg2_serving_binary_blocks_v1",
            "table": f"mrf.{kwargs['target_table']}",
            "writer": "rust_stream",
            "row_count": kwargs["expected_row_count"],
            "timing": {"total_seconds": 1.25, "index_seconds": 0.05},
            "build_elapsed_seconds": 1.25,
            "storage": {"total_bytes": 2048},
        }

    monkeypatch.setenv("HLTHPRT_PTG2_SNAPSHOT_ARCH", "postgres_binary_v1")
    monkeypatch.setenv(
        "HLTHPRT_PTG2_MANIFEST_SERVING_LAYOUT",
        ptg_manifest_publish.PTG2_MANIFEST_SERVING_LAYOUT_LEAN_PROVIDER_KEY,
    )
    monkeypatch.setenv("HLTHPRT_PTG2_MANIFEST_LEAN_DIRECT_COPY", "true")
    monkeypatch.setenv("HLTHPRT_PTG2_PUBLISH_DB_DEDUPE_FALLBACK", "false")
    monkeypatch.setenv("HLTHPRT_PTG2_MANIFEST_SERVING_SIDECARS_ENABLED", "false")
    monkeypatch.setenv("HLTHPRT_PTG2_MANIFEST_DROP_SERVING_TABLE_AFTER_SIDECARS", "true")
    monkeypatch.setattr(ptg_manifest_publish.db, "status", fake_status)
    monkeypatch.setattr(ptg_manifest_publish.db, "all", AsyncMock(side_effect=AssertionError("unused")))
    monkeypatch.setattr(ptg_manifest_publish, "_table_exists", is_fake_table_present)
    monkeypatch.setattr(ptg_manifest_publish, "_table_has_rows", fake_table_has_rows)
    monkeypatch.setattr(ptg_manifest_publish, "_exact_table_rows", AsyncMock(return_value=321))
    monkeypatch.setattr(ptg_manifest_publish, "write_ptg2_serving_binary_table", fake_binary_write)

    publish_manifest = asyncio.run(
        process_ptg._publish_ptg2_manifest_serving_snapshot(
            "ptg2_manifest_stage_serving_abc",
            snapshot_id="ptg2:202604:snap",
            source_key="example_dental",
            artifacts={"source_trace_set_hash": "trace-set-hash", "network_names": ["C2"]},
        )
    )

    joined = "\n".join(status_calls)
    assert "lean_src_uidx" not in joined
    assert "provider_set_global_id_128,\n                        price_set_global_id_128" not in joined
    assert 'FROM "mrf"."ptg2_manifest_stage_code_count_abc"' in joined
    assert 'FROM "mrf"."ptg2_manifest_stage_provider_set_dictionary_abc"' in joined
    assert 'CREATE TABLE "mrf"."ptg2_code_count_' in joined
    assert 'CREATE TABLE "mrf"."ptg2_provider_set_dict_' in joined
    assert 'CREATE UNLOGGED TABLE "mrf"."ptg2_code_count_' not in joined
    assert 'CREATE UNLOGGED TABLE "mrf"."ptg2_provider_set_dict_' not in joined
    assert 'DROP TABLE IF EXISTS "mrf"."ptg2_manifest_stage_code_count_abc"' in joined
    assert "COUNT(*)::bigint AS rate_count" not in joined
    assert "SELECT DISTINCT provider_set_global_id_128\n            FROM \"mrf\".\"ptg2_manifest_stage_serving_abc\"" not in joined
    assert any('DROP TABLE "mrf"."ptg2_manifest_stage_serving_abc"' in statement for statement in status_calls)
    assert 'ALTER TABLE "mrf"."ptg2_serving_binary_' in joined
    assert "SET LOGGED" in joined
    assert "PTG2 materialized table is missing" in joined
    assert "PTG2 materialized table is not logged" in joined
    assert publish_manifest["serving_row_strategy"] == "postgres_binary"
    assert publish_manifest["serving_table_retained"] is False
    assert publish_manifest["dedupe"]["serving"] == {"skipped": "scanner_dedupe_guarded_postgres_binary"}
    assert publish_manifest["dictionary_source"] == "scanner_support"


def test_ptg2_manifest_snapshot_publish_sidecar_arch_overrides_scope_table_env(monkeypatch):
    status_calls = []

    async def fake_status(statement, **_params):
        status_calls.append(statement)

    async def is_fake_table_present(_schema, table):
        return table == "ptg2_manifest_stage_serving_abc"

    async def fake_all(statement, **_params):
        assert "GROUP BY source_trace_set_hash, network_names" in statement
        return [{"source_trace_set_hash": "trace-set-hash", "network_names": ["C2"]}]

    component_mock = AsyncMock(side_effect=AssertionError("sidecar arch must not materialize components"))
    rate_scope_mock = AsyncMock(side_effect=AssertionError("sidecar arch must not materialize rate scope"))
    location_mock = AsyncMock(return_value="ptg2_provider_group_location_snap")

    monkeypatch.setenv("HLTHPRT_PTG2_SNAPSHOT_ARCH", "sidecar_scope_v1")
    monkeypatch.setenv(
        "HLTHPRT_PTG2_MANIFEST_SERVING_LAYOUT",
        ptg_manifest_publish.PTG2_MANIFEST_SERVING_LAYOUT_LEAN_PROVIDER_KEY,
    )
    monkeypatch.setenv("HLTHPRT_PTG2_MANIFEST_PROVIDER_GROUP_LOCATION_TABLE", "true")
    monkeypatch.setenv("HLTHPRT_PTG2_MANIFEST_PROVIDER_SET_COMPONENT_TABLE", "true")
    monkeypatch.setenv("HLTHPRT_PTG2_MANIFEST_PROVIDER_GROUP_RATE_SCOPE_TABLE", "true")
    monkeypatch.setenv("HLTHPRT_PTG2_PUBLISH_DB_DEDUPE_FALLBACK", "false")
    monkeypatch.setenv("HLTHPRT_PTG2_MANIFEST_SERVING_SIDECARS_ENABLED", "false")
    monkeypatch.setattr(ptg_manifest_publish.db, "status", fake_status)
    monkeypatch.setattr(ptg_manifest_publish.db, "all", fake_all)
    monkeypatch.setattr(ptg_manifest_publish, "_table_exists", is_fake_table_present)
    monkeypatch.setattr(ptg_manifest_publish, "_table_has_rows", AsyncMock(return_value=True))
    monkeypatch.setattr(ptg_manifest_publish, "_exact_table_rows", AsyncMock(return_value=321))
    monkeypatch.setattr(ptg_manifest_publish, "_materialize_manifest_components", component_mock)
    monkeypatch.setattr(ptg_manifest_publish, "_materialize_manifest_provider_group_rate_scope", rate_scope_mock)
    monkeypatch.setattr(ptg_manifest_publish, "_build_manifest_provider_group_location_table", location_mock)

    publish_manifest = asyncio.run(
        process_ptg._publish_ptg2_manifest_serving_snapshot(
            "ptg2_manifest_stage_serving_abc",
            snapshot_id="ptg2:202604:snap",
            source_key="example_dental",
        )
    )

    assert publish_manifest["arch_version"] == "sidecar_scope_v1"
    assert publish_manifest["provider_scope_strategy"] == "sidecar_provider_scope"
    assert publish_manifest["provider_set_component_table"] is None
    assert publish_manifest["provider_group_rate_scope_table"] is None
    assert publish_manifest["provider_group_location_table"] == "mrf.ptg2_provider_group_location_snap"
    assert publish_manifest["materialized_tables"]["provider_group_location"] == "mrf.ptg2_provider_group_location_snap"
    assert "provider_set_component" not in publish_manifest["materialized_tables"]
    assert "provider_group_rate_scope" not in publish_manifest["materialized_tables"]
    component_mock.assert_not_awaited()
    rate_scope_mock.assert_not_awaited()
    location_mock.assert_awaited_once()


def test_ptg2_snapshot_arch_rejects_postgres_posting_alias(monkeypatch):
    monkeypatch.setenv("HLTHPRT_PTG2_SNAPSHOT_ARCH", "posting")

    with pytest.raises(ValueError):
        ptg_config._ptg2_snapshot_arch_from_env()


def test_ptg2_snapshot_arch_defaults_to_durable_postgres_binary_v2(monkeypatch):
    monkeypatch.delenv("HLTHPRT_PTG2_SNAPSHOT_ARCH", raising=False)

    assert ptg_config._ptg2_snapshot_arch_from_env() == "postgres_binary_v2"


def test_ptg2_snapshot_arch_accepts_postgres_binary_alias(monkeypatch):
    monkeypatch.setenv("HLTHPRT_PTG2_SNAPSHOT_ARCH", "db_binary")

    assert ptg_config._ptg2_snapshot_arch_from_env() == "postgres_binary_v1"


def test_ptg2_snapshot_arch_accepts_postgres_binary_v2_alias(monkeypatch):
    monkeypatch.setenv("HLTHPRT_PTG2_SNAPSHOT_ARCH", "binary_v2")

    assert ptg_config._ptg2_snapshot_arch_from_env() == "postgres_binary_v2"
    assert ptg_config._is_postgres_binary_snapshot_arch("postgres_binary_v2") is True


def test_ptg2_manifest_snapshot_publish_materialized_arch_forces_scope_tables(monkeypatch):
    status_calls = []

    async def fake_status(statement, **_params):
        status_calls.append(statement)

    async def is_fake_table_present(_schema, table):
        return table == "ptg2_manifest_stage_serving_abc"

    async def fake_materialize_components(**kwargs):
        return kwargs["table_name"]

    async def fake_materialize_rate_scope(**kwargs):
        assert kwargs["provider_set_component_table"].startswith("ptg2_provider_set_component_")
        return kwargs["table_name"]

    location_mock = AsyncMock(return_value="ptg2_provider_group_location_snap")

    monkeypatch.setenv("HLTHPRT_PTG2_SNAPSHOT_ARCH", "materialized_v1")
    monkeypatch.setenv("HLTHPRT_PTG2_MANIFEST_PROVIDER_SET_COMPONENT_TABLE", "false")
    monkeypatch.setenv("HLTHPRT_PTG2_MANIFEST_PROVIDER_GROUP_RATE_SCOPE_TABLE", "false")
    monkeypatch.setenv("HLTHPRT_PTG2_PUBLISH_DB_DEDUPE_FALLBACK", "false")
    monkeypatch.setattr(ptg_manifest_publish.db, "status", fake_status)
    monkeypatch.setattr(ptg_manifest_publish, "_table_exists", is_fake_table_present)
    monkeypatch.setattr(ptg_manifest_publish, "_table_has_rows", AsyncMock(return_value=True))
    monkeypatch.setattr(ptg_manifest_publish, "_exact_table_rows", AsyncMock(return_value=321))
    monkeypatch.setattr(ptg_manifest_publish, "_materialize_manifest_components", fake_materialize_components)
    monkeypatch.setattr(ptg_manifest_publish, "_materialize_manifest_provider_group_rate_scope", fake_materialize_rate_scope)
    monkeypatch.setattr(ptg_manifest_publish, "_build_manifest_provider_group_location_table", location_mock)

    publish_manifest = asyncio.run(
        process_ptg._publish_ptg2_manifest_serving_snapshot(
            "ptg2_manifest_stage_serving_abc",
            snapshot_id="ptg2:202604:snap",
            source_key="example_dental",
        )
    )

    assert publish_manifest["arch_version"] == "materialized_v1"
    assert publish_manifest["provider_scope_strategy"] == "materialized_rate_scope"
    assert publish_manifest["provider_set_component_table"].startswith("mrf.ptg2_provider_set_component_")
    assert publish_manifest["provider_group_rate_scope_table"].startswith("mrf.ptg2_provider_group_rate_scope_")
    assert publish_manifest["provider_group_location_table"] == "mrf.ptg2_provider_group_location_snap"
    assert publish_manifest["materialized_tables"]["provider_set_component"].startswith("mrf.ptg2_provider_set_component_")
    assert publish_manifest["materialized_tables"]["provider_group_rate_scope"].startswith("mrf.ptg2_provider_group_rate_scope_")
    location_mock.assert_awaited_once()


def test_ptg2_manifest_snapshot_publish_lean_uses_price_atom_dictionary(monkeypatch):
    status_calls = []
    monkeypatch.setenv("HLTHPRT_PTG2_SNAPSHOT_ARCH", "sidecar_scope_v1")

    async def fake_status(statement, **_params):
        status_calls.append(statement)

    async def is_fake_table_present(_schema, table):
        return table in {
            "ptg2_manifest_stage_serving_abc",
            "ptg2_manifest_stage_price_atom_abc",
        } or table.startswith("ptg2_price_atom_")

    async def fake_all(statement, **_params):
        assert "GROUP BY source_trace_set_hash, network_names" in statement
        return [{"source_trace_set_hash": "trace-set-hash", "network_names": ["C2"]}]

    async def fake_scalar(statement, **_params):
        assert "collisions" in statement
        return 0

    monkeypatch.setenv(
        "HLTHPRT_PTG2_MANIFEST_SERVING_LAYOUT",
        ptg_manifest_publish.PTG2_MANIFEST_SERVING_LAYOUT_LEAN_PROVIDER_KEY,
    )
    monkeypatch.setenv("HLTHPRT_PTG2_PUBLISH_DB_DEDUPE_FALLBACK", "false")
    monkeypatch.setenv("HLTHPRT_PTG2_MANIFEST_SERVING_SIDECARS_ENABLED", "false")
    monkeypatch.setattr(ptg_manifest_publish.db, "status", fake_status)
    monkeypatch.setattr(ptg_manifest_publish.db, "scalar", fake_scalar)
    monkeypatch.setattr(ptg_manifest_publish.db, "all", fake_all)
    monkeypatch.setattr(ptg_manifest_publish, "_table_exists", is_fake_table_present)
    monkeypatch.setattr(ptg_manifest_publish, "_table_has_rows", AsyncMock(return_value=True))
    monkeypatch.setattr(ptg_manifest_publish, "_exact_table_rows", AsyncMock(return_value=321))

    publish_manifest = asyncio.run(
        process_ptg._publish_ptg2_manifest_serving_snapshot(
            "ptg2_manifest_stage_serving_abc",
            snapshot_id="ptg2:202604:snap",
            source_key="example_dental",
        )
    )

    joined = "\n".join(status_calls)
    assert publish_manifest["serving_table_layout"] == "lean_provider_key_v1"
    assert publish_manifest["price_atom_table_layout"] == "lean_dict_v1"
    assert publish_manifest["price_atom_dictionary_table"].startswith("mrf.ptg2_price_atom_dict_")
    assert 'CREATE TABLE "mrf"."ptg2_price_atom_dict_' in joined
    assert 'CREATE TABLE "mrf"."ptg2_price_atom_' in joined
    assert "price_atom.price_atom_global_id_128::uuid AS price_atom_global_id_128" in joined
    assert "negotiated_type.attr_key::integer AS negotiated_type_key" in joined
    assert "service_code.attr_key::integer AS service_code_key" in joined
    assert "billing_code_modifier.attr_key::integer AS billing_code_modifier_key" in joined
    assert "text_lookup_key" in joined and "array_lookup_key" in joined
    assert "WHERE text_lookup_key IS NOT NULL" in joined
    assert "WHERE array_lookup_key IS NOT NULL" in joined
    assert "md5(to_json(price_atom.service_code)::text)" in joined
    assert "INSERT INTO \"mrf\".\"ptg2_price_atom_" not in joined
    assert "ALTER COLUMN price_atom_global_id_128 SET NOT NULL" in joined
    assert "RENAME TO \"ptg2_price_atom_" in joined
    assert "CREATE UNIQUE INDEX" in joined


def test_materialized_publish_builds_component_table(monkeypatch):
    status_calls = []
    component_calls = []

    async def fake_status(statement, **_params):
        status_calls.append(statement)

    async def is_fake_table_present(_schema, table):
        return table == "ptg2_manifest_stage_serving_abc"

    async def fake_all(statement, **_params):
        assert "GROUP BY source_trace_set_hash, network_names" in statement
        return [{"source_trace_set_hash": "trace-set-hash", "network_names": ["C2"]}]

    async def fake_materialize_components(**kwargs):
        component_calls.append(kwargs)
        return kwargs["table_name"]

    monkeypatch.setenv(
        "HLTHPRT_PTG2_MANIFEST_SERVING_LAYOUT",
        ptg_manifest_publish.PTG2_MANIFEST_SERVING_LAYOUT_LEAN_PROVIDER_KEY,
    )
    monkeypatch.setenv("HLTHPRT_PTG2_SNAPSHOT_ARCH", "materialized_v1")
    monkeypatch.setenv("HLTHPRT_PTG2_MANIFEST_PROVIDER_SET_COMPONENT_TABLE", "true")
    monkeypatch.setenv("HLTHPRT_PTG2_PUBLISH_DB_DEDUPE_FALLBACK", "false")
    monkeypatch.setenv("HLTHPRT_PTG2_MANIFEST_SERVING_SIDECARS_ENABLED", "false")
    monkeypatch.setattr(ptg_manifest_publish.db, "status", fake_status)
    monkeypatch.setattr(ptg_manifest_publish.db, "all", fake_all)
    monkeypatch.setattr(ptg_manifest_publish, "_table_exists", is_fake_table_present)
    monkeypatch.setattr(ptg_manifest_publish, "_table_has_rows", AsyncMock(return_value=True))
    monkeypatch.setattr(ptg_manifest_publish, "_exact_table_rows", AsyncMock(return_value=321))
    monkeypatch.setattr(ptg_manifest_publish, "_materialize_manifest_components", fake_materialize_components)

    publish_manifest = asyncio.run(
        process_ptg._publish_ptg2_manifest_serving_snapshot(
            "ptg2_manifest_stage_serving_abc",
            snapshot_id="ptg2:202604:snap",
            source_key="example_dental",
        )
    )

    assert publish_manifest["serving_table_layout"] == "lean_provider_key_v1"
    assert publish_manifest["provider_set_component_table"].startswith("mrf.ptg2_provider_set_component_")
    assert len(component_calls) == 1
    assert component_calls[0]["table_name"].startswith("ptg2_provider_set_component_")
    assert "ANALYZE \"mrf\".\"ptg2_provider_set_component_" in "\n".join(status_calls)


def test_manifest_publish_materializes_components(tmp_path, monkeypatch):
    artifact_module = importlib.import_module("process.ptg_parts.ptg2_manifest_artifacts")
    status_calls = []
    copied_by_field = {}
    provider_group_id = bytes.fromhex("00000000000000000000000000000011")
    provider_set_id = bytes.fromhex("00000000000000000000000000000012")
    sidecar_manifest = artifact_module.write_global_membership_sidecar(
        tmp_path,
        "provider_inverted",
        {provider_group_id: [provider_set_id]},
    )
    sidecar_metadata_dict = dict(sidecar_manifest["sidecars"][0])
    sidecar_metadata_dict["name"] = "provider_inverted"
    manifest_path = tmp_path / "snapshot.manifest.json"
    manifest_path.write_text("{}", encoding="utf-8")

    async def fake_status(statement, **_params):
        status_calls.append(statement)

    async def fake_copy(copy_path, *, target_table):
        copied_by_field["target_table"] = target_table
        copied_by_field["lines"] = Path(copy_path).read_text(encoding="ascii").splitlines()

    monkeypatch.setattr(ptg_manifest_publish.db, "status", fake_status)
    monkeypatch.setattr(
        ptg_manifest_publish,
        "_copy_manifest_component_file",
        fake_copy,
    )

    materialized_table_name = asyncio.run(
        ptg_manifest_publish._materialize_manifest_components(
            schema_name="mrf",
            table_name="ptg2_provider_set_component_snap",
            artifacts={"manifest_uri": f"file://{manifest_path}", "sidecars": [sidecar_metadata_dict]},
            sidecar_artifacts=None,
        )
    )

    joined = "\n".join(status_calls)
    assert materialized_table_name == "ptg2_provider_set_component_snap"
    assert copied_by_field["target_table"] == "ptg2_provider_set_component_snap"
    assert copied_by_field["lines"] == [
        "00000000-0000-0000-0000-000000000012\t00000000-0000-0000-0000-000000000011"
    ]
    assert "provider_set_global_id_128 uuid NOT NULL" in joined
    assert "(provider_set_global_id_128, provider_group_global_id_128)" in joined
    assert "(provider_group_global_id_128, provider_set_global_id_128)" in joined


def test_manifest_publish_materializes_lean_provider_group_rate_scope(monkeypatch):
    status_calls = []

    async def fake_status(statement, **_params):
        status_calls.append(statement)

    async def is_fake_table_present(_schema, table):
        return table in {
            "ptg2_serving_snap",
            "ptg2_code_count_snap",
            "ptg2_provider_set_dict_snap",
            "ptg2_provider_set_component_snap",
        }

    async def fake_table_has_rows(_schema, table):
        return table == "ptg2_provider_group_rate_scope_snap"

    monkeypatch.setattr(ptg_manifest_publish.db, "status", fake_status)
    monkeypatch.setattr(ptg_manifest_publish, "_table_exists", is_fake_table_present)
    monkeypatch.setattr(ptg_manifest_publish, "_table_has_rows", fake_table_has_rows)

    materialized_table_name = asyncio.run(
        ptg_manifest_publish._materialize_manifest_provider_group_rate_scope(
            schema_name="mrf",
            table_name="ptg2_provider_group_rate_scope_snap",
            serving_table="ptg2_serving_snap",
            code_count_table="ptg2_code_count_snap",
            provider_set_component_table="ptg2_provider_set_component_snap",
            provider_set_dictionary_table="ptg2_provider_set_dict_snap",
            lean_provider_key_layout=True,
        )
    )

    joined = "\n".join(status_calls)
    assert materialized_table_name == "ptg2_provider_group_rate_scope_snap"
    assert "provider_group_global_id_128 uuid NOT NULL" in joined
    assert "JOIN \"mrf\".\"ptg2_code_count_snap\" code_count" in joined
    assert "JOIN \"mrf\".\"ptg2_provider_set_dict_snap\" provider_set_dictionary" in joined
    assert "JOIN \"mrf\".\"ptg2_provider_set_component_snap\" component" in joined
    assert "(plan_id, reported_code, reported_code_system, provider_group_global_id_128)" in joined
    assert "(provider_group_global_id_128, plan_id, reported_code, reported_code_system)" in joined


def test_ptg2_manifest_snapshot_publish_can_fallback_to_db_dedupe(monkeypatch):
    status_calls = []

    async def fake_status(statement, **_params):
        status_calls.append(statement)

    monkeypatch.setattr(ptg_manifest_publish.db, "status", fake_status)
    monkeypatch.setattr(ptg_manifest_publish, "_table_exists", AsyncMock(return_value=True))
    monkeypatch.setattr(ptg_manifest_publish, "_table_has_rows", AsyncMock(return_value=True))
    monkeypatch.setattr(ptg_manifest_publish, "_exact_table_rows", AsyncMock(return_value=321))

    asyncio.run(
        process_ptg._publish_ptg2_manifest_serving_snapshot(
            "ptg2_manifest_stage_serving_abc",
            snapshot_id="ptg2:202604:snap",
            source_key="example_dental",
        )
    )

    joined = "\n".join(status_calls)
    assert "SELECT DISTINCT ON (serving_content_hash_128)" in joined
    assert "SELECT DISTINCT ON (price_atom_global_id_128)" in joined
    assert "SELECT DISTINCT ON (provider_group_global_id_128, npi)" in joined


def test_ptg2_manifest_snapshot_publish_rescues_duplicate_serving_index(monkeypatch):
    status_calls = []
    monkeypatch.setenv("HLTHPRT_PTG2_SNAPSHOT_ARCH", "sidecar_scope_v1")
    unique_index_failures = {"seen": False}

    async def fake_status(statement, **_params):
        status_calls.append(statement)
        if (
            "CREATE UNIQUE INDEX" in statement
            and "content_uidx" in statement
            and "serving_content_hash_128" in statement
            and not unique_index_failures["seen"]
        ):
            unique_index_failures["seen"] = True
            raise RuntimeError(
                "asyncpg.exceptions.UniqueViolationError: could not create unique index; "
                "DETAIL: Key (serving_content_hash_128) is duplicated."
            )

    async def is_fake_table_present(_schema, table):
        return table == "ptg2_manifest_stage_serving_abc"

    monkeypatch.setattr(ptg_manifest_publish.db, "status", fake_status)
    monkeypatch.setattr(ptg_manifest_publish, "_table_exists", is_fake_table_present)
    monkeypatch.setattr(ptg_manifest_publish, "_table_has_rows", AsyncMock(return_value=True))
    monkeypatch.setattr(ptg_manifest_publish, "_exact_table_rows", AsyncMock(side_effect=[10, 9, 9]))

    result = asyncio.run(
        process_ptg._publish_ptg2_manifest_serving_snapshot(
            "ptg2_manifest_stage_serving_abc",
            snapshot_id="ptg2:202604:snap",
            source_key="example_dental",
            db_dedupe_fallback=False,
        )
    )

    joined = "\n".join(status_calls)
    serving_unique_attempts = [
        statement
        for statement in status_calls
        if "CREATE UNIQUE INDEX" in statement
        and "content_uidx" in statement
        and "serving_content_hash_128" in statement
    ]
    assert len(serving_unique_attempts) == 2
    assert "SELECT DISTINCT ON (serving_content_hash_128)" in joined
    assert result["rate_count"] == 9
    assert result["dedupe"]["db_dedupe"] is True
    assert result["dedupe"]["rescue"] is True
    assert result["dedupe"]["serving"] == {"before": 10, "after": 9, "dropped": 1}


def test_manifest_publish_uses_post_dedupe_serving_count():
    dedupe_metrics_map = {
        "serving": {"before": 10, "after": 9, "dropped": 1},
    }

    assert (
        ptg_manifest_publish._known_or_deduped_serving_rows(
            known_counts={"serving_rows": 10},
            dedupe_metrics=dedupe_metrics_map,
            serving_deduped=True,
        )
        == 9
    )
    assert (
        ptg_manifest_publish._known_or_deduped_serving_rows(
            known_counts={"serving_rows": 10},
            dedupe_metrics={},
            serving_deduped=False,
        )
        == 10
    )
    assert (
        ptg_manifest_publish._known_or_deduped_serving_rows(
            known_counts={"serving_rows": 10},
            dedupe_metrics={**dedupe_metrics_map, "rescue": True},
            serving_deduped=True,
        )
        is None
    )


def _manifest_copy_successful_files(source_files_by_kind):
    return [
        {
            "summary": {
                "manifest": {
                    "copy_files": {
                        kind: [{"path": str(path), "row_count": 1}]
                        for kind, path in source_files_by_kind.items()
                    }
                }
            }
        }
    ]


def _write_manifest_copy_files(tmp_path):
    source_files_by_kind = {}
    for kind in ("manifest_serving", "price_atom", "provider_group_member"):
        source_path = tmp_path / f"{kind}.copy"
        source_path.write_text("row\n", encoding="utf-8")
        source_files_by_kind[kind] = source_path
    return source_files_by_kind


def _fake_manifest_merge_recorder(merge_calls):
    async def fake_merge(kind, output_path, input_paths):
        merge_calls.append((kind, output_path, tuple(input_paths)))
        output_path.write_text(f"{kind}\n", encoding="utf-8")
        return {
            "kind": kind,
            "input_files": len(input_paths),
            "input_rows": 2,
            "output_rows": 1,
            "dropped_rows": 1,
        }

    return fake_merge


def _fake_manifest_copy_recorder(copy_calls):
    async def fake_copy(copy_path, *, target_table):
        copy_calls.append((copy_path.read_text(encoding="utf-8").strip(), target_table))

    return fake_copy


def _publish_steps(progress_events):
    return [event["publish_step"] for event in progress_events]


def test_ptg2_manifest_precopy_merge_copies_merged_files(monkeypatch, tmp_path):
    """Pre-copy merge emits progress while merging and copying manifest files."""
    monkeypatch.setenv("HLTHPRT_PTG2_SNAPSHOT_ARCH", "sidecar_scope_v1")
    merge_calls = []
    copy_calls = []
    progress_events = []
    source_files_by_kind = _write_manifest_copy_files(tmp_path)

    monkeypatch.setenv("HLTHPRT_PTG2_MANIFEST_STREAM_MERGE_COPY", "false")
    monkeypatch.setattr(process_ptg, "_run_ptg2_manifest_copy_merge", _fake_manifest_merge_recorder(merge_calls))
    monkeypatch.setattr(process_ptg, "_copy_ptg2_manifest_serving_file", _fake_manifest_copy_recorder(copy_calls))
    monkeypatch.setattr(process_ptg, "_copy_ptg2_manifest_price_atom_file", _fake_manifest_copy_recorder(copy_calls))
    monkeypatch.setattr(
        process_ptg,
        "_copy_ptg2_manifest_provider_group_member_file",
        _fake_manifest_copy_recorder(copy_calls),
    )
    monkeypatch.setattr(process_ptg, "write_live_progress", lambda **payload: progress_events.append(payload))

    metrics = asyncio.run(
        process_ptg._merge_and_copy_ptg2_manifest_files(
            successful_files=_manifest_copy_successful_files(source_files_by_kind),
            manifest_stage_table="ptg2_manifest_stage_serving_abc",
        )
    )

    assert metrics["enabled"] is True
    assert metrics["serving_rows"] in {1}
    assert [call[0] for call in merge_calls] == ["manifest_serving", "price_atom", "provider_group_member"]
    assert ("manifest_serving", "ptg2_manifest_stage_serving_abc") in copy_calls
    assert ("price_atom", "ptg2_manifest_stage_price_atom_abc") in copy_calls
    assert ("provider_group_member", "ptg2_manifest_stage_provider_group_member_abc") in copy_calls
    assert all(not path.exists() for path in source_files_by_kind.values())
    assert _publish_steps(progress_events) == [
        "merging manifest_serving",
        "merged manifest_serving",
        "merging price_atom",
        "merged price_atom",
        "merging provider_group_member",
        "merged provider_group_member",
        "copying manifest_serving",
        "copied manifest_serving",
        "copying price_atom",
        "copied price_atom",
        "copying provider_group_member",
        "copied provider_group_member",
    ]
    assert progress_events[0]["pct"] == 92.0
    assert progress_events[-1]["pct"] == 95.0
    assert progress_events[-1]["source"] == "ptg2-publish-progress"


def test_ptg2_manifest_precopy_merge_streams_when_enabled(monkeypatch, tmp_path):
    """Streaming pre-copy merge emits progress around each direct COPY stream."""
    monkeypatch.setenv("HLTHPRT_PTG2_SNAPSHOT_ARCH", "sidecar_scope_v1")
    stream_calls = []
    progress_events = []
    source_files_by_kind = _write_manifest_copy_files(tmp_path)

    async def fake_stream(kind, *, target_table, input_paths, copy_func):
        stream_calls.append((kind, target_table, tuple(input_paths), copy_func))
        return {
            "kind": kind,
            "input_files": len(input_paths),
            "input_rows": 2,
            "output_rows": 1,
            "dropped_rows": 1,
        }

    monkeypatch.setenv("HLTHPRT_PTG2_MANIFEST_STREAM_MERGE_COPY", "true")
    monkeypatch.setattr(process_ptg, "_stream_ptg2_manifest_copy_merge", fake_stream)
    monkeypatch.setattr(process_ptg, "write_live_progress", lambda **payload: progress_events.append(payload))

    metrics = asyncio.run(
        process_ptg._merge_and_copy_ptg2_manifest_files(
            successful_files=_manifest_copy_successful_files(source_files_by_kind),
            manifest_stage_table="ptg2_manifest_stage_serving_abc",
        )
    )

    assert metrics["enabled"] is True
    assert metrics["streamed_to_copy"] is True
    assert metrics["serving_rows"] == 1
    assert [call[0] for call in stream_calls] == ["manifest_serving", "price_atom", "provider_group_member"]
    assert stream_calls[0][1] == "ptg2_manifest_stage_serving_abc"
    assert stream_calls[1][1] == "ptg2_manifest_stage_price_atom_abc"
    assert stream_calls[2][1] == "ptg2_manifest_stage_provider_group_member_abc"
    assert all(not path.exists() for path in source_files_by_kind.values())
    assert _publish_steps(progress_events) == [
        "copying manifest_serving",
        "copied manifest_serving",
        "copying price_atom",
        "copied price_atom",
        "copying provider_group_member",
        "copied provider_group_member",
    ]
    assert progress_events[0]["message"] == "streaming manifest_serving merge into ptg2_manifest_stage_serving_abc"
    assert progress_events[-1]["output_rows"] == 1
    assert progress_events[-1]["streamed_to_copy"] is True


def test_ptg2_manifest_precopy_merge_streams_direct_lean_serving_kind(monkeypatch, tmp_path):
    stream_calls = []
    monkeypatch.setenv("HLTHPRT_PTG2_SNAPSHOT_ARCH", "sidecar_scope_v1")
    source_files_by_kind = {}
    for kind in ("manifest_lean_serving", "price_atom", "provider_group_member"):
        source_path = tmp_path / f"{kind}.copy"
        source_path.write_text("row\n", encoding="utf-8")
        source_files_by_kind[kind] = source_path

    async def fake_stream(kind, *, target_table, input_paths, copy_func):
        stream_calls.append((kind, target_table, tuple(input_paths), copy_func))
        return {
            "kind": kind,
            "input_files": len(input_paths),
            "input_rows": 2,
            "output_rows": 1,
            "dropped_rows": 1,
        }

    monkeypatch.setenv(
        "HLTHPRT_PTG2_MANIFEST_SERVING_LAYOUT",
        ptg_manifest_publish.PTG2_MANIFEST_SERVING_LAYOUT_LEAN_PROVIDER_KEY,
    )
    monkeypatch.setenv("HLTHPRT_PTG2_MANIFEST_LEAN_DIRECT_COPY", "true")
    monkeypatch.setenv("HLTHPRT_PTG2_MANIFEST_STREAM_MERGE_COPY", "true")
    monkeypatch.setattr(process_ptg, "_stream_ptg2_manifest_copy_merge", fake_stream)

    metrics = asyncio.run(
        process_ptg._merge_and_copy_ptg2_manifest_files(
            successful_files=[
                {
                    "summary": {
                        "manifest": {
                            "copy_files": {
                                kind: [{"path": str(path), "row_count": 1}]
                                for kind, path in source_files_by_kind.items()
                            }
                        }
                    }
                }
            ],
            manifest_stage_table="ptg2_manifest_stage_serving_abc",
        )
    )

    assert metrics["enabled"] is True
    assert metrics["streamed_to_copy"] is True
    assert metrics["serving_rows"] == 1
    assert [call[0] for call in stream_calls] == ["manifest_lean_serving", "price_atom", "provider_group_member"]
    assert stream_calls[0][1] == "ptg2_manifest_stage_serving_abc"
    assert stream_calls[0][3] is process_ptg._copy_lean_manifest_serving_file
    assert all(not path.exists() for path in source_files_by_kind.values())


def test_ptg2_manifest_precopy_direct_copies_files_for_binary_arch(monkeypatch, tmp_path):
    copy_calls = []
    source_files_by_kind = {}
    for kind in ("manifest_lean_serving", "price_atom", "price_set_atom", "provider_group_member"):
        source_path = tmp_path / f"{kind}.copy"
        source_path.write_text("row\n", encoding="utf-8")
        source_files_by_kind[kind] = source_path

    async def fail_merge(*_args, **_kwargs):
        raise AssertionError("binary direct-copy path should not run the merge producer")

    async def fake_copy(copy_path, *, target_table):
        copy_calls.append((copy_path.name, target_table))

    monkeypatch.setenv("HLTHPRT_PTG2_SNAPSHOT_ARCH", ptg_config.PTG2_SNAPSHOT_ARCH_POSTGRES_BINARY_V1)
    monkeypatch.setenv(
        "HLTHPRT_PTG2_MANIFEST_SERVING_LAYOUT",
        ptg_manifest_publish.PTG2_MANIFEST_SERVING_LAYOUT_LEAN_PROVIDER_KEY,
    )
    monkeypatch.setenv("HLTHPRT_PTG2_MANIFEST_LEAN_DIRECT_COPY", "true")
    monkeypatch.setattr(process_ptg, "_run_ptg2_manifest_copy_merge", fail_merge)
    monkeypatch.setattr(process_ptg, "_stream_ptg2_manifest_copy_merge", fail_merge)
    monkeypatch.setattr(process_ptg, "_copy_lean_manifest_serving_file", fake_copy)
    monkeypatch.setattr(process_ptg, "_copy_ptg2_manifest_price_atom_file", fake_copy)
    monkeypatch.setattr(process_ptg, "_copy_price_atom_member_file", fake_copy)
    monkeypatch.setattr(process_ptg, "_copy_ptg2_manifest_provider_group_member_file", fake_copy)

    metrics = asyncio.run(
        process_ptg._merge_and_copy_ptg2_manifest_files(
            successful_files=_manifest_copy_successful_files(source_files_by_kind),
            manifest_stage_table="ptg2_manifest_stage_serving_abc",
        )
    )

    assert metrics["enabled"] is True
    assert metrics["direct_to_copy"] is True
    assert metrics["serving_rows"] == 1
    assert metrics["kinds"]["manifest_lean_serving"]["direct_to_copy"] is True
    assert copy_calls == [
        ("manifest_lean_serving.copy", "ptg2_manifest_stage_serving_abc"),
        ("price_atom.copy", "ptg2_manifest_stage_price_atom_abc"),
        ("price_set_atom.copy", "ptg2_manifest_stage_price_set_atom_abc"),
        ("provider_group_member.copy", "ptg2_manifest_stage_provider_group_member_abc"),
    ]
    assert all(not path.exists() for path in source_files_by_kind.values())


def _assert_manifest_copy_throughput_metrics(metrics_by_name, *, expected_bytes):
    """Assert persisted direct-COPY counters are populated."""
    assert metrics_by_name["input_bytes"] == expected_bytes
    assert metrics_by_name["elapsed_seconds"] >= 0
    assert metrics_by_name["rows_per_second"] is not None
    assert metrics_by_name["bytes_per_second"] is not None


def test_ptg2_manifest_precopy_direct_copy_reports_parallel_tasks(monkeypatch, tmp_path):
    """Direct COPY metrics preserve file bytes, time, and task concurrency."""
    source_files_by_kind = {"manifest_serving": []}
    copy_calls = []
    for index in range(3):
        source_path = tmp_path / f"manifest_serving_{index}.copy"
        source_path.write_text(f"row-{index}\n", encoding="utf-8")
        source_files_by_kind["manifest_serving"].append(source_path)
    price_atom = tmp_path / "price_atom.copy"
    provider_group_member = tmp_path / "provider_group_member.copy"
    price_atom.write_text("price\n", encoding="utf-8")
    provider_group_member.write_text("provider\n", encoding="utf-8")

    async def fake_copy(copy_path, *, target_table):
        copy_calls.append((copy_path.name, target_table))

    monkeypatch.setenv("HLTHPRT_PTG2_SNAPSHOT_ARCH", ptg_config.PTG2_SNAPSHOT_ARCH_POSTGRES_BINARY_V1)
    monkeypatch.setenv("HLTHPRT_PTG2_MANIFEST_DIRECT_COPY_TASKS", "2")
    monkeypatch.setattr(process_ptg, "_copy_ptg2_manifest_serving_file", fake_copy)
    monkeypatch.setattr(process_ptg, "_copy_ptg2_manifest_price_atom_file", fake_copy)
    monkeypatch.setattr(process_ptg, "_copy_ptg2_manifest_provider_group_member_file", fake_copy)

    metrics = asyncio.run(
        process_ptg._merge_and_copy_ptg2_manifest_files(
            successful_files=[
                {
                    "summary": {
                        "manifest": {
                            "copy_files": {
                                "manifest_serving": [
                                    {"path": str(path), "row_count": 1}
                                    for path in source_files_by_kind["manifest_serving"]
                                ],
                                "price_atom": [{"path": str(price_atom), "row_count": 1}],
                                "provider_group_member": [
                                    {"path": str(provider_group_member), "row_count": 1}
                                ],
                            }
                        }
                    }
                }
            ],
            manifest_stage_table="ptg2_manifest_stage_serving_abc",
        )
    )

    assert metrics["direct_to_copy"] is True
    assert metrics["kinds"]["manifest_serving"]["copy_tasks"] == 2
    assert metrics["kinds"]["manifest_serving"]["output_rows"] == 3
    _assert_manifest_copy_throughput_metrics(metrics["kinds"]["manifest_serving"], expected_bytes=18)
    assert sorted(name for name, _target in copy_calls if name.startswith("manifest_serving_")) == [
        "manifest_serving_0.copy",
        "manifest_serving_1.copy",
        "manifest_serving_2.copy",
    ]
    assert not any(path.exists() for path in source_files_by_kind["manifest_serving"])
    assert not price_atom.exists()
    assert not provider_group_member.exists()


def test_manifest_copy_cleanup_removes_empty_worker_siblings(tmp_path):
    base_copy = tmp_path / "ptg2_manifest_provider_group_member_test.copy"
    empty_worker = tmp_path / "ptg2_manifest_provider_group_member_test.copy.worker0001"
    empty_provider_ref_worker = tmp_path / "ptg2_manifest_provider_group_member_test.copy.provider_refs.worker0002"
    nonempty_provider_ref_worker = tmp_path / "ptg2_manifest_provider_group_member_test.copy.provider_refs.worker0003"
    for path in (base_copy, empty_worker, empty_provider_ref_worker, nonempty_provider_ref_worker):
        path.touch()
    nonempty_provider_ref_worker.write_text("member-row\n", encoding="utf-8")

    process_ptg._cleanup_empty_manifest_copy_siblings(base_copy)

    assert not empty_worker.exists()
    assert not empty_provider_ref_worker.exists()
    assert nonempty_provider_ref_worker.exists()


def test_manifest_copy_family_cleanup_removes_nonempty_failed_shards(tmp_path):
    base_copy = tmp_path / "ptg2_manifest_serving_failed.copy"
    worker_copy = tmp_path / "ptg2_manifest_serving_failed.copy.worker0001"
    ready_copy = tmp_path / "ptg2_manifest_serving_failed.copy.worker0001.part000001.ready"
    unrelated_copy = tmp_path / "ptg2_manifest_serving_other.copy.worker0001"
    for path in (base_copy, worker_copy, ready_copy, unrelated_copy):
        path.write_text("row\n", encoding="utf-8")

    process_ptg._cleanup_manifest_copy_family(base_copy)

    assert not base_copy.exists()
    assert not worker_copy.exists()
    assert not ready_copy.exists()
    assert unrelated_copy.exists()


def test_single_scanner_dedupe_count_requires_one_valid_summary():
    serving_file_summary_map = {
        "summary": {
            "dedupe": {
                "price_set_unique": 29_122_132,
            }
        }
    }

    assert (
        process_ptg._single_scanner_dedupe_count(
            [serving_file_summary_map], "price_set_unique"
        )
        == 29_122_132
    )
    assert (
        process_ptg._single_scanner_dedupe_count(
            [serving_file_summary_map, serving_file_summary_map], "price_set_unique"
        )
        is None
    )
    assert (
        process_ptg._single_scanner_dedupe_count(
            [{"summary": {"dedupe": {"price_set_unique": "invalid"}}}],
            "price_set_unique",
        )
        is None
    )


def test_ptg2_manifest_stage_uses_uuid_ids_when_enabled(monkeypatch):
    status_calls = []
    monkeypatch.setenv("HLTHPRT_PTG2_SNAPSHOT_ARCH", "sidecar_scope_v1")

    async def fake_status(statement, **_params):
        status_calls.append(statement)

    monkeypatch.setenv("HLTHPRT_PTG2_BINARY_IDS", "true")
    monkeypatch.setattr(ptg_manifest_publish.db, "status", fake_status)

    asyncio.run(process_ptg._create_ptg2_manifest_serving_stage_table("abc"))

    joined = "\n".join(status_calls)
    assert "serving_content_hash_128 uuid NOT NULL" in joined
    assert "procedure_global_id_128 uuid NOT NULL" in joined
    assert "provider_set_global_id_128 uuid NOT NULL" in joined
    assert "price_set_global_id_128 uuid NOT NULL" in joined
    assert "network_names varchar[] NOT NULL DEFAULT '{}'" in joined
    assert "price_atom_global_id_128 uuid NOT NULL" in joined
    assert "provider_group_global_id_128 uuid NOT NULL" in joined
    assert "provider_npi_scope" not in joined
    assert ptg_manifest_publish.PTG2_MANIFEST_SERVING_COLUMNS[-1] == "network_names"


@pytest.mark.parametrize("snapshot_arch", ["postgres_binary_v2", "postgres_binary_v3"])
def test_ptg2_manifest_membership_stage_creates_npi_scope(monkeypatch, snapshot_arch):
    status_calls = []

    async def fake_status(statement, **_params):
        status_calls.append(statement)

    monkeypatch.setenv("HLTHPRT_PTG2_SNAPSHOT_ARCH", snapshot_arch)
    monkeypatch.setattr(ptg_manifest_publish.db, "status", fake_status)

    asyncio.run(process_ptg._create_ptg2_manifest_serving_stage_table("abc"))

    joined = "\n".join(status_calls)
    assert 'CREATE TABLE "mrf"."ptg2_manifest_stage_provider_npi_scope_abc"' in joined
    assert 'CREATE UNLOGGED TABLE "mrf"."ptg2_manifest_stage_provider_npi_scope_abc"' not in joined


def test_ptg2_manifest_stage_uses_direct_lean_source_columns(monkeypatch):
    status_calls = []

    async def fake_status(statement, **_params):
        status_calls.append(statement)

    monkeypatch.setenv(
        "HLTHPRT_PTG2_MANIFEST_SERVING_LAYOUT",
        ptg_manifest_publish.PTG2_MANIFEST_SERVING_LAYOUT_LEAN_PROVIDER_KEY,
    )
    monkeypatch.setenv("HLTHPRT_PTG2_MANIFEST_LEAN_DIRECT_COPY", "true")
    monkeypatch.setenv("HLTHPRT_PTG2_BINARY_IDS", "true")
    monkeypatch.setattr(ptg_manifest_publish.db, "status", fake_status)

    asyncio.run(process_ptg._create_ptg2_manifest_serving_stage_table("abc"))

    joined = "\n".join(status_calls)
    assert "plan_id varchar(64) NOT NULL" in joined
    assert "reported_code_system varchar(64)" in joined
    assert "reported_code varchar(64)" in joined
    assert "provider_set_global_id_128 uuid NOT NULL" in joined
    assert "provider_count integer" in joined
    assert "price_set_global_id_128 uuid NOT NULL" in joined
    assert "serving_content_hash_128" not in joined
    assert "procedure_global_id_128" not in joined
    assert "source_trace_set_hash" not in joined
    assert "network_names" not in joined


def test_ptg2_source_plan_rows_falls_back_to_serving_index_table(monkeypatch):
    calls = []

    async def fake_all(statement, **_params):
        calls.append(statement)
        if "FROM \"mrf\".\"ptg2_serving_rate_compact_exact\"" in statement:
            return [{"plan_id": "TESTPLAN001", "plan_market_type": ""}]
        return []

    monkeypatch.setattr(ptg_source_pointers.db, "all", fake_all)
    monkeypatch.setattr(ptg_source_pointers, "_table_exists", AsyncMock(return_value=True))
    updated_at = process_ptg._utcnow()

    rows = asyncio.run(
        process_ptg._source_plan_rows(
            snapshot_id="snap",
            source_key="example_dental",
            import_month=process_ptg.normalize_import_month("2026-04"),
            previous_snapshot_id="prev",
            updated_at=updated_at,
            serving_index={"table": "mrf.ptg2_serving_rate_compact_exact"},
        )
    )

    assert rows == [
        {
            "plan_source_key": process_ptg._ptg2_plan_source_key(
                "TESTPLAN001",
                "",
                process_ptg.normalize_import_month("2026-04"),
                "example_dental",
            ),
            "plan_id": "TESTPLAN001",
            "plan_market_type": "",
            "import_month": process_ptg.normalize_import_month("2026-04"),
            "source_key": "example_dental",
            "snapshot_id": "snap",
            "previous_snapshot_id": "prev",
            "updated_at": updated_at,
        }
    ]
    assert len(calls) == 2


def test_ptg2_source_plan_rows_uses_manifest_table_without_snapshot_column(monkeypatch):
    calls = []

    async def fake_all(statement, **_params):
        calls.append(statement)
        if "FROM \"mrf\".\"ptg2_manifest_serving_exact\"" in statement:
            assert "snapshot_id" not in statement
            return [{"plan_id": "TESTPLAN001", "plan_market_type": ""}]
        return []

    monkeypatch.setattr(ptg_source_pointers.db, "all", fake_all)
    monkeypatch.setattr(ptg_source_pointers, "_table_exists", AsyncMock(return_value=True))
    updated_at = process_ptg._utcnow()

    rows = asyncio.run(
        process_ptg._source_plan_rows(
            snapshot_id="snap",
            source_key="example_dental",
            import_month=process_ptg.normalize_import_month("2026-04"),
            previous_snapshot_id="prev",
            updated_at=updated_at,
            serving_index={"storage": "manifest_snapshot", "table": "mrf.ptg2_manifest_serving_exact"},
        )
    )

    assert rows[0]["plan_id"] == "TESTPLAN001"
    assert rows[0]["snapshot_id"] == "snap"


def test_ptg2_source_plan_rows_uses_code_count_for_lean_manifest(monkeypatch):
    calls = []

    async def fake_all(statement, **_params):
        calls.append(statement)
        if "FROM \"mrf\".\"ptg2_code_count_exact\"" in statement:
            assert "FROM \"mrf\".\"ptg2_manifest_serving_exact\"" not in statement
            return [{"plan_id": "TESTPLAN001", "plan_market_type": ""}]
        return []

    monkeypatch.setattr(ptg_source_pointers.db, "all", fake_all)
    monkeypatch.setattr(ptg_source_pointers, "_table_exists", AsyncMock(return_value=True))
    updated_at = process_ptg._utcnow()

    rows = asyncio.run(
        process_ptg._source_plan_rows(
            snapshot_id="snap",
            source_key="example_dental",
            import_month=process_ptg.normalize_import_month("2026-04"),
            previous_snapshot_id="prev",
            updated_at=updated_at,
            serving_index={
                "storage": "manifest_snapshot",
                "serving_table_layout": "lean_provider_key_v1",
                "table": "mrf.ptg2_manifest_serving_exact",
                "code_count_table": "mrf.ptg2_code_count_exact",
            },
        )
    )

    assert rows[0]["plan_id"] == "TESTPLAN001"
    assert rows[0]["snapshot_id"] == "snap"
    assert len(calls) == 2


def _build_published_snapshot_fields(import_month, updated_at):
    return {
        "snapshot_id": "snap",
        "import_run_id": "ptg2:run",
        "import_month": import_month,
        "status": "published",
        "created_at": updated_at,
        "validated_at": updated_at,
        "published_at": updated_at,
        "previous_snapshot_id": "prev",
        "manifest": {"serving_index": {"storage": "manifest_snapshot"}},
    }


def test_ptg2_source_pointer_publish_updates_source_and_plan_rows_transactionally(monkeypatch):
    executed = []
    updated_at = process_ptg._utcnow()
    import_month = process_ptg.normalize_import_month("2026-04")

    async def fake_source_plan_rows(**_kwargs):
        return [
            {
                "plan_source_key": "plan-source-key",
                "plan_id": "TESTPLAN001",
                "plan_market_type": "group",
                "import_month": import_month,
                "source_key": "example_dental",
                "snapshot_id": "snap",
                "previous_snapshot_id": "prev",
                "updated_at": updated_at,
            }
        ]

    class FakeSession:
        async def execute(self, statement, params=None):
            executed.append((str(statement), params or {}))

    class FakeTransaction:
        async def __aenter__(self):
            return FakeSession()

        async def __aexit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(ptg_source_pointers, "_source_plan_rows", fake_source_plan_rows)
    monkeypatch.setattr(ptg_source_pointers.db, "transaction", lambda: FakeTransaction())
    snapshot_by_field = _build_published_snapshot_fields(import_month, updated_at)

    promotion_result = asyncio.run(
        process_ptg._publish_ptg2_source_pointers(
            source_key="example_dental",
            snapshot_id="snap",
            previous_snapshot_id="prev",
            import_month=import_month,
            updated_at=updated_at,
            serving_index={"storage": "manifest_snapshot", "table": "mrf.ptg2_serving_exact"},
            snapshot_attributes=snapshot_by_field,
        )
    )

    joined = "\n".join(statement for statement, _params in executed)
    assert promotion_result["global_pointer"] == "reconciled"
    assert "INSERT INTO \"mrf\".ptg2_current_source_snapshot" in joined
    assert "UPDATE \"mrf\".ptg2_snapshot" in joined
    assert "updated_global_pointer" in joined
    assert "incumbent.published_at >=" in joined
    assert "DELETE FROM \"mrf\".ptg2_current_plan_source WHERE source_key = :source_key" in joined
    assert "INSERT INTO \"mrf\".ptg2_current_plan_source" in joined
    assert len(executed) == 6


def test_ptg2_source_pointer_publish_does_not_advance_source_when_plan_resolution_fails(monkeypatch):
    transaction_started_map = {"value": False}
    executed_statements = []

    async def fake_source_plan_rows(**_kwargs):
        assert transaction_started_map["value"] is True
        assert executed_statements and "pg_advisory_xact_lock" in executed_statements[0][0]
        raise RuntimeError("plan resolution failed")

    class FakeSession:
        async def execute(self, statement, params=None):
            executed_statements.append((str(statement), params or {}))

    class FakeTransaction:
        async def __aenter__(self):
            transaction_started_map["value"] = True
            return FakeSession()

        async def __aexit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(ptg_source_pointers, "_source_plan_rows", fake_source_plan_rows)
    monkeypatch.setattr(ptg_source_pointers.db, "transaction", lambda: FakeTransaction())

    with pytest.raises(RuntimeError, match="plan resolution failed"):
        asyncio.run(
            process_ptg._publish_ptg2_source_pointers(
                source_key="example_dental",
                snapshot_id="snap",
                previous_snapshot_id="prev",
                import_month=process_ptg.normalize_import_month("2026-04"),
                updated_at=process_ptg._utcnow(),
                serving_index={"storage": "manifest_snapshot", "table": "mrf.ptg2_serving_exact"},
            )
        )

    assert transaction_started_map["value"] is True
    assert len(executed_statements) == 1
    assert "pg_advisory_xact_lock" in executed_statements[0][0]
    assert executed_statements[0][1] == {
        "publish_lock_key": ptg_source_pointers.PTG2_SOURCE_POINTER_GC_LOCK_KEY
    }


def test_ptg2_global_publish_is_atomic(monkeypatch):
    executed_statements = []
    updated_at = process_ptg._utcnow()
    snapshot_attributes = _build_published_snapshot_fields(
        process_ptg.normalize_import_month("2026-04"),
        updated_at,
    )

    class FakeSession:
        async def execute(self, statement, params=None):
            executed_statements.append((str(statement), params or {}))

    class FakeTransaction:
        async def __aenter__(self):
            return FakeSession()

        async def __aexit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(ptg_source_pointers.db, "transaction", lambda: FakeTransaction())

    publication_outcome = asyncio.run(
        ptg_source_pointers._publish_ptg2_global_snapshot_pointer(
            snapshot_attributes=snapshot_attributes,
            updated_at=updated_at,
        )
    )

    assert publication_outcome == {
        "status": "promoted",
        "snapshot_id": "snap",
        "global_pointer": "reconciled",
    }
    assert len(executed_statements) == 3
    assert "pg_advisory_xact_lock" in executed_statements[0][0]
    assert "UPDATE \"mrf\".ptg2_snapshot" in executed_statements[1][0]
    assert "INSERT INTO \"mrf\".ptg2_current_snapshot" in executed_statements[2][0]
    assert executed_statements[0][1] == {
        "publish_lock_key": ptg_source_pointers.PTG2_SOURCE_POINTER_GC_LOCK_KEY
    }


def test_ptg2_global_snapshot_pointer_rejects_unpublished_snapshot_after_lock(monkeypatch):
    executed_statements = []

    class FakeSession:
        async def execute(self, statement, params=None):
            executed_statements.append((str(statement), params or {}))

    class FakeTransaction:
        async def __aenter__(self):
            return FakeSession()

        async def __aexit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(ptg_source_pointers.db, "transaction", lambda: FakeTransaction())

    with pytest.raises(ValueError, match="requires a published snapshot row"):
        asyncio.run(
            ptg_source_pointers._publish_ptg2_global_snapshot_pointer(
                snapshot_attributes={"snapshot_id": "snap", "status": "validated"},
                updated_at=process_ptg._utcnow(),
            )
        )

    assert len(executed_statements) == 1
    assert "pg_advisory_xact_lock" in executed_statements[0][0]


def test_ptg2_snapshot_manifest_table_names_allowlists_location_and_rejects_unsafe_names():
    names = process_ptg._snapshot_manifest_table_names(
        {
            "storage": "db_compact_snapshot",
            "table": "mrf.ptg2_serving_rate_compact_abc123",
            "provider_group_location_table": "mrf.ptg2_provider_group_location_abc123",
            "provider_group_member_table": "mrf.ptg2_provider_group_member_abc123",
            "provider_set_table": "mrf.ptg2_provider_set_abc123",
            "provider_set_dictionary_table": "mrf.ptg2_provider_set_dict_abc123",
            "provider_set_entry_table": "mrf.ptg2_provider_set_abc123",
            "price_atom_table": "mrf.ptg2_price_atom_bad-name",
            "procedure_table": "mrf.not_a_snapshot_table",
            "price_set_entry_table": "mrf.ptg2_price_set_entry_abc123;drop",
        }
    )

    assert names == [
        "ptg2_serving_rate_compact_abc123",
        "ptg2_provider_set_abc123",
        "ptg2_provider_set_dict_abc123",
        "ptg2_provider_group_member_abc123",
        "ptg2_provider_group_location_abc123",
    ]


def test_ptg2_source_scoped_report_uses_published_serving_rate_count(monkeypatch):
    """Verify this PTG import regression contract."""
    pushed = []
    publish_source_pointers = AsyncMock()

    async def fake_push(rows, cls, **_kwargs):
        pushed.extend((getattr(cls, "__name__", str(cls)), row) for row in rows)

    async def fake_downloaded_jobs(jobs, **_kwargs):
        for job in jobs:
            yield process_ptg.PTG2DownloadedJob(
                job=job,
                raw_artifact=SimpleNamespace(raw_sha256=str(job.get("url") or "")),
                logical_artifact=SimpleNamespace(logical_path="/tmp/rates.json.gz"),
            )

    async def fake_process(*_args, **_kwargs):
        return process_ptg.PTG2FileProcessResult(
            "in_network",
            "https://example.test/rates.json.gz",
            True,
            summary={"serving_rates": 111, "rust_compact_serving": True},
        )

    async def fake_publish(*_args, **_kwargs):
        return {
            "storage": "manifest_snapshot",
            "type": "ptg2_manifest_serving",
            "table": "mrf.ptg2_manifest_serving_exact",
            "rate_count": 987,
            "serving_rates": 987,
        }

    monkeypatch.setattr(process_ptg, "ensure_database", AsyncMock())
    monkeypatch.setattr(process_ptg, "ensure_ptg2_tables", AsyncMock())
    monkeypatch.setattr(process_ptg.db, "status", AsyncMock())
    monkeypatch.setattr(process_ptg, "_push_ptg2_objects", fake_push)
    monkeypatch.setattr(process_ptg, "_prepare_ptg_tables", AsyncMock(return_value={"ImportLog": "log"}))
    monkeypatch.setattr(process_ptg, "_create_ptg2_manifest_serving_stage_table", AsyncMock(return_value="manifest_stage"))
    monkeypatch.setattr(process_ptg, "_iter_downloaded_ptg_jobs", fake_downloaded_jobs)
    monkeypatch.setattr(process_ptg, "_process_in_network_file", fake_process)
    monkeypatch.setattr(process_ptg, "flush_error_log", AsyncMock())
    monkeypatch.setattr(process_ptg, "_publish_ptg2_manifest_serving_snapshot", fake_publish)
    monkeypatch.setattr(process_ptg, "_current_source_snapshot_id", AsyncMock(return_value=None))
    monkeypatch.setattr(
        process_ptg,
        "_publish_ptg2_source_pointers",
        publish_source_pointers,
    )
    monkeypatch.setattr(process_ptg, "_cleanup_old_ptg2_source_tables", AsyncMock())

    asyncio.run(
        process_ptg.main(
            in_network_url="https://example.test/rates.json.gz",
            import_month="2026-04",
            import_id="source_report_count",
            source_key="example_dental",
        )
    )

    import_run_rows = [row for cls_name, row in pushed if cls_name == "PTG2ImportRun"]
    final_report = import_run_rows[-1]["report"]
    assert final_report["serving_rates"] == 987
    assert final_report["rate_count"] == 987
    assert final_report["serving_index"]["serving_rates"] == 987
    assert final_report["successful_files"][0]["summary"]["serving_rates"] == 111
    published_snapshot = publish_source_pointers.await_args.kwargs["snapshot_attributes"]
    assert published_snapshot["manifest"]["serving_rates"] == 987


def test_ptg2_precopy_merge_keeps_publish_db_dedupe(monkeypatch):
    """Ensure precopy merge keeps the publish database dedupe fallback enabled."""
    publish_kwargs = []
    monkeypatch.setenv("HLTHPRT_PTG2_SNAPSHOT_ARCH", "sidecar_scope_v1")

    async def fake_push(_rows, _cls, **_kwargs):
        return None

    async def fake_downloaded_jobs(jobs, **_kwargs):
        for job in jobs:
            yield process_ptg.PTG2DownloadedJob(
                job=job,
                raw_artifact=SimpleNamespace(raw_sha256=str(job.get("url") or "")),
                logical_artifact=SimpleNamespace(logical_path="/tmp/rates.json.gz"),
            )

    async def fake_process(*_args, **_kwargs):
        return process_ptg.PTG2FileProcessResult(
            "in_network",
            "https://example.test/rates.json.gz",
            True,
            summary={"serving_rates": 111, "manifest": {"copy_files": {"manifest_serving": []}}},
        )

    async def fake_publish(*_args, **kwargs):
        publish_kwargs.append(kwargs)
        return {
            "storage": "manifest_snapshot",
            "type": "ptg2_manifest_serving",
            "table": "mrf.ptg2_manifest_serving_exact",
            "rate_count": 111,
            "serving_rates": 111,
        }

    monkeypatch.setenv(process_ptg.PTG2_RUST_PARSE_IN_WORKERS_ENV, "false")
    monkeypatch.setattr(process_ptg, "ensure_database", AsyncMock())
    monkeypatch.setattr(process_ptg, "ensure_ptg2_tables", AsyncMock())
    monkeypatch.setattr(process_ptg.db, "status", AsyncMock())
    monkeypatch.setattr(process_ptg, "_push_ptg2_objects", fake_push)
    monkeypatch.setattr(process_ptg, "_prepare_ptg_tables", AsyncMock(return_value={"ImportLog": "log"}))
    monkeypatch.setattr(process_ptg, "_create_ptg2_manifest_serving_stage_table", AsyncMock(return_value="manifest_stage"))
    monkeypatch.setattr(process_ptg, "_iter_downloaded_ptg_jobs", fake_downloaded_jobs)
    monkeypatch.setattr(process_ptg, "_process_in_network_file", fake_process)
    monkeypatch.setattr(process_ptg, "_merge_and_copy_ptg2_manifest_files", AsyncMock(return_value={"enabled": True}))
    monkeypatch.setattr(process_ptg, "flush_error_log", AsyncMock())
    monkeypatch.setattr(process_ptg, "_publish_ptg2_manifest_serving_snapshot", fake_publish)
    monkeypatch.setattr(process_ptg, "_current_source_snapshot_id", AsyncMock(return_value=None))
    monkeypatch.setattr(process_ptg, "_publish_ptg2_source_pointers", AsyncMock())
    monkeypatch.setattr(process_ptg, "_cleanup_old_ptg2_source_tables", AsyncMock())

    asyncio.run(
        process_ptg.main(
            in_network_url="https://example.test/rates.json.gz",
            import_month="2026-04",
            import_id="parse_worker_backstop",
            source_key="example_dental",
        )
    )

    assert publish_kwargs and publish_kwargs[-1]["db_dedupe_fallback"] is True


def test_ptg2_test_mode_uses_manifest_source_scoped_import(monkeypatch):
    pushed = []

    async def fake_push(rows, cls, **_kwargs):
        pushed.extend((getattr(cls, "__name__", str(cls)), row) for row in rows)

    async def fake_downloaded_jobs(jobs, **_kwargs):
        for job in jobs:
            yield process_ptg.PTG2DownloadedJob(
                job=job,
                raw_artifact=SimpleNamespace(raw_sha256=str(job.get("url") or "")),
                logical_artifact=SimpleNamespace(logical_path="/tmp/rates.json.gz"),
            )

    async def fake_process(*_args, **kwargs):
        assert kwargs.get("rust_stage_tables") is None
        assert kwargs.get("ptg2_manifest_stage_table") == "manifest_stage"
        return process_ptg.PTG2FileProcessResult(
            "in_network",
            "https://example.test/rates.json.gz",
            True,
            summary={"serving_rates": 111, "rust_compact_serving": True, "manifest": {"serving_rows": 111}},
        )

    create_stage_mock = AsyncMock(return_value="manifest_stage")
    publish_mock = AsyncMock(return_value={"storage": "manifest_snapshot", "rate_count": 222, "serving_rates": 222})

    monkeypatch.setattr(process_ptg, "ensure_database", AsyncMock())
    monkeypatch.setattr(process_ptg, "ensure_ptg2_tables", AsyncMock())
    monkeypatch.setattr(process_ptg.db, "status", AsyncMock())
    monkeypatch.setattr(process_ptg, "_push_ptg2_objects", fake_push)
    monkeypatch.setattr(process_ptg, "_prepare_ptg_tables", AsyncMock(return_value={"ImportLog": "log"}))
    monkeypatch.setattr(process_ptg, "_create_ptg2_manifest_serving_stage_table", create_stage_mock)
    monkeypatch.setattr(process_ptg, "_iter_downloaded_ptg_jobs", fake_downloaded_jobs)
    monkeypatch.setattr(process_ptg, "_process_in_network_file", fake_process)
    monkeypatch.setattr(process_ptg, "flush_error_log", AsyncMock())
    monkeypatch.setattr(process_ptg, "_publish_ptg2_manifest_serving_snapshot", publish_mock)
    monkeypatch.setattr(process_ptg, "_current_source_snapshot_id", AsyncMock(return_value=None))
    monkeypatch.setattr(process_ptg, "_publish_ptg2_source_pointers", AsyncMock())
    monkeypatch.setattr(process_ptg, "_cleanup_old_ptg2_source_tables", AsyncMock())

    asyncio.run(
        process_ptg.main(
            in_network_url="https://example.test/rates.json.gz",
            import_month="2026-04",
            import_id="test_mode_rust_env",
            source_key="example_dental",
            test_mode=True,
        )
    )

    create_stage_mock.assert_awaited_once()
    publish_mock.assert_awaited_once()
    import_run_rows = [row for cls_name, row in pushed if cls_name == "PTG2ImportRun"]
    assert import_run_rows[-1]["options"]["source_scoped_compact"] is True
    assert import_run_rows[-1]["options"]["serving_storage"] == "manifest_snapshot"
    assert import_run_rows[-1]["report"]["serving_rates"] == 222


def test_ptg2_ignores_legacy_v2_import_flags(monkeypatch):
    async def fake_push(*_args, **_kwargs):
        return None

    async def fake_downloaded_jobs(jobs, **_kwargs):
        for job in jobs:
            yield process_ptg.PTG2DownloadedJob(
                job=job,
                raw_artifact=SimpleNamespace(raw_sha256=str(job.get("url") or "")),
                logical_artifact=SimpleNamespace(logical_path="/tmp/rates.json.gz"),
            )

    async def fake_process(*_args, **kwargs):
        assert kwargs.get("rust_stage_tables") is None
        assert kwargs.get("ptg2_manifest_stage_table") == "manifest_stage"
        return process_ptg.PTG2FileProcessResult(
            "in_network",
            "https://example.test/rates.json.gz",
            True,
            summary={"serving_rates": 111, "rust_compact_serving": True, "manifest": {"serving_rows": 111}},
        )

    create_stage_mock = AsyncMock(return_value="manifest_stage")
    publish_mock = AsyncMock(return_value={"storage": "manifest_snapshot", "rate_count": 111, "serving_rates": 111})

    monkeypatch.setenv(process_ptg.PTG2_COMPACT_IMPORT_ENV, "false")
    monkeypatch.setenv(process_ptg.PTG2_COMPACT_SERVING_TABLE_ENV, "false")
    monkeypatch.setenv(process_ptg.PTG2_RUST_COMPACT_SERVING_ENV, "false")
    monkeypatch.setattr(process_ptg, "ensure_database", AsyncMock())
    monkeypatch.setattr(process_ptg, "ensure_ptg2_tables", AsyncMock())
    monkeypatch.setattr(process_ptg.db, "status", AsyncMock())
    monkeypatch.setattr(process_ptg, "_push_ptg2_objects", fake_push)
    monkeypatch.setattr(process_ptg, "_prepare_ptg_tables", AsyncMock(return_value={"ImportLog": "log"}))
    monkeypatch.setattr(process_ptg, "_create_ptg2_manifest_serving_stage_table", create_stage_mock)
    monkeypatch.setattr(process_ptg, "_iter_downloaded_ptg_jobs", fake_downloaded_jobs)
    monkeypatch.setattr(process_ptg, "_process_in_network_file", fake_process)
    monkeypatch.setattr(process_ptg, "flush_error_log", AsyncMock())
    monkeypatch.setattr(process_ptg, "_publish_ptg2_manifest_serving_snapshot", publish_mock)
    monkeypatch.setattr(process_ptg, "_current_source_snapshot_id", AsyncMock(return_value=None))
    monkeypatch.setattr(process_ptg, "_publish_ptg2_source_pointers", AsyncMock())
    monkeypatch.setattr(process_ptg, "_cleanup_old_ptg2_source_tables", AsyncMock())

    asyncio.run(
        process_ptg.main(
            in_network_url="https://example.test/rates.json.gz",
            import_month="2026-04",
            import_id="test_mode_source_scoped",
            source_key="example_dental",
            test_mode=True,
        )
    )

    create_stage_mock.assert_awaited_once()
    publish_mock.assert_awaited_once()


def test_ptg2_compact_finalize_builds_provider_locations_after_analyze_when_enabled(monkeypatch):
    calls = []
    scalar_values = iter([3, 1, 2])

    async def fake_status(statement, **_params):
        if "ANALYZE mrf.ptg2_serving_rate_compact" in statement:
            calls.append("analyze_compact")

    async def fake_scalar(_statement, **_params):
        return next(scalar_values)

    async def fake_provider_locations(snapshot_id):
        calls.append(f"provider_locations:{snapshot_id}")

    monkeypatch.setenv(process_ptg.PTG2_DEFER_PROVIDER_LOCATIONS_ENV, "false")
    monkeypatch.setattr(process_ptg.db, "status", fake_status)
    monkeypatch.setattr(process_ptg.db, "scalar", fake_scalar)
    monkeypatch.setattr(process_ptg, "_build_ptg2_provider_locations", fake_provider_locations)

    result = asyncio.run(process_ptg.build_ptg2_compact_serving_index("snap", "run"))

    assert result["rate_count"] == 3
    assert calls.index("analyze_compact") < calls.index("provider_locations:snap")


def test_ptg2_rust_scanner_enables_rust_compact_serving_by_default(monkeypatch):
    monkeypatch.setenv(process_ptg.PTG2_RUST_SCANNER_ENV, "true")
    monkeypatch.delenv(process_ptg.PTG2_RUST_COMPACT_SERVING_ENV, raising=False)

    assert process_ptg._use_rust_compact_serving() is True


def test_ptg2_rust_compact_serving_can_be_disabled_when_scanner_enabled(monkeypatch):
    monkeypatch.setenv(process_ptg.PTG2_RUST_SCANNER_ENV, "true")
    monkeypatch.setenv(process_ptg.PTG2_RUST_COMPACT_SERVING_ENV, "false")

    assert process_ptg._use_rust_compact_serving() is False


def test_download_raw_artifact_rejects_file_url(monkeypatch, tmp_path):
    secret = tmp_path / "secret.txt"
    secret.write_text("top-secret")
    monkeypatch.delenv("HLTHPRT_FETCH_ALLOW_LOCAL", raising=False)

    with pytest.raises(ValueError, match="http"):
        asyncio.run(
            process_ptg.download_raw_artifact(
                f"file://{secret}",
                store=process_ptg.PTG2ArtifactStore(tmp_path / "store"),
            )
        )


def test_download_raw_artifact_rejects_private_host(monkeypatch, tmp_path):
    monkeypatch.delenv("HLTHPRT_FETCH_ALLOW_LOCAL", raising=False)

    with pytest.raises(ValueError, match="non-public IP"):
        asyncio.run(
            process_ptg.download_raw_artifact(
                "http://169.254.169.254/latest/meta-data/",
                store=process_ptg.PTG2ArtifactStore(tmp_path / "store"),
            )
        )

    with pytest.raises(ValueError, match="non-public IP"):
        asyncio.run(
            process_ptg.download_raw_artifact(
                "http://127.0.0.1/index.json",
                store=process_ptg.PTG2ArtifactStore(tmp_path / "store"),
            )
        )


def test_ptg_http_redirect_target_is_validated_before_follow(monkeypatch):
    from process.ptg_parts import source_download as ptg_source_download

    calls: list[str] = []

    async def fake_assert_safe_url(url: str) -> None:
        if "169.254.169.254" in url:
            raise ValueError("non-public IP address is not allowed: 169.254.169.254")

    class _FakeResponse:
        status = 302
        headers = {"Location": "http://169.254.169.254/latest/meta-data/"}
        url = "https://public.example/index.json"

        def release(self) -> None:
            return None

    class _FakeSession:
        async def request(self, method: str, url: str, **_kwargs):
            calls.append(f"{method} {url}")
            return _FakeResponse()

    monkeypatch.setattr(ptg_source_download, "assert_safe_url", fake_assert_safe_url)

    with pytest.raises(ValueError, match="non-public IP"):
        asyncio.run(
            ptg_source_download._open_validated_request(
                _FakeSession(),
                "GET",
                "https://public.example/index.json",
            )
        )

    assert calls == ["GET https://public.example/index.json"]


def test_download_raw_artifact_aborts_oversize_body(monkeypatch, tmp_path):
    payload = b"x" * (256 * 1024)

    async def handle(request):
        if request.method == "HEAD":
            return web.Response(headers={"Content-Length": "1"})
        return web.Response(body=payload, headers={"Content-Length": str(len(payload))})

    async def run_download():
        app = web.Application()
        app.router.add_route("*", "/big.bin", handle)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        try:
            return await process_ptg.download_raw_artifact(
                f"http://127.0.0.1:{port}/big.bin",
                store=process_ptg.PTG2ArtifactStore(tmp_path / "store"),
                max_bytes=64 * 1024,
            )
        finally:
            await runner.cleanup()

    monkeypatch.setenv("HLTHPRT_FETCH_ALLOW_LOCAL", "true")
    monkeypatch.setenv(process_ptg.PTG2_RANGE_DOWNLOADS_ENV, "false")

    with pytest.raises(RuntimeError, match="max-bytes guard exceeded"):
        asyncio.run(run_download())


def test_filter_plans_ignores_issuer_names():
    plans = [
        {
            "plan_name": "Small Business PPO",
            "plan_id": "59-1031071",
            "issuer_name": "Cigna Health Life Insurance Company",
            "reporting_entity_name": "Cigna",
            "plan_market_type": "group",
        },
        {
            "plan_name": "Cigna MVP Health Care, Inc",
            "plan_id": "123",
            "plan_market_type": "group",
        },
    ]

    result = process_ptg._filter_reporting_plans(
        plans,
        plan_name_contains=["cigna"],
        plan_market_types=["group"],
    )

    assert result == [plans[1]]


def test_materialize_zip_when_deferred(tmp_path, monkeypatch):
    import zipfile

    raw_path = tmp_path / "rates.zip"
    expected = b'{"in_network":[]}'
    with zipfile.ZipFile(raw_path, "w", compression=zipfile.ZIP_DEFLATED) as zip_ref:
        zip_ref.writestr("nested/rates.json", expected)
    raw_sha256, byte_count = process_ptg.sha256_file(raw_path)

    async def fake_download_raw_artifact(url, **_kwargs):
        return process_ptg.PTG2RawArtifact(
            original_url=url,
            canonical_url=url,
            raw_path=str(raw_path),
            raw_storage_uri=f"file://{raw_path}",
            raw_sha256=raw_sha256,
            byte_count=byte_count,
        )

    monkeypatch.setattr(ptg_source_download, "download_raw_artifact", fake_download_raw_artifact)

    _raw, logical = asyncio.run(
        process_ptg.materialize_json_source(
            "https://example.test/rates.zip",
            tmp_path / "logical",
            materialize_logical=False,
        )
    )

    assert logical.compression == "zip"
    assert logical.member_name == "nested/rates.json"
    assert logical.logical_path != str(raw_path)
    assert Path(logical.logical_path).read_bytes() == expected


def _assert_recovered_worker_copy_summary(summary, recovered_paths_by_kind):
    """Check recovered serving, price-atom, and provider-member copy counts."""

    assert summary["serving_rates"] == 2
    assert summary["manifest"]["serving_rows"] == 2
    assert summary["manifest"]["copy_files"]["manifest_serving"] == [
        {"path": recovered_paths_by_kind["serving"], "row_count": 2}
    ]
    assert summary["manifest"]["copy_files"]["price_atom"] == [
        {"path": recovered_paths_by_kind["price"], "row_count": 1}
    ]
    assert summary["manifest"]["copy_files"]["provider_group_member"] == [
        {"path": recovered_paths_by_kind["member"], "row_count": 1}
    ]


def test_serving_only_import_recovers_unreported_worker_copy_files(tmp_path, monkeypatch):
    """Recover worker copy files that the scanner did not report directly."""
    artifact_dir = tmp_path / "artifacts"
    recovered_paths_by_kind = {}
    monkeypatch.setenv("HLTHPRT_PTG2_SNAPSHOT_ARCH", "sidecar_scope_v1")

    async def fake_push_ptg2_objects(*_args, **_kwargs):
        return None

    async def fake_flush_error_log(*_args, **_kwargs):
        return None

    async def fake_scanner(*_args, **kwargs):
        serving_worker = Path(f"{kwargs['manifest_serving_copy_path']}.worker0003")
        price_worker = Path(f"{kwargs['manifest_price_atom_copy_path']}.worker0003")
        member_worker = Path(f"{kwargs['manifest_provider_group_member_copy_path']}.provider_refs.worker0003")
        serving_worker.write_text("serving-1\nserving-2\n")
        price_worker.write_text("price-1\n")
        member_worker.write_text("member-1\n")
        recovered_paths_by_kind.update(
            serving=str(serving_worker),
            price=str(price_worker),
            member=str(member_worker),
        )
        yield "scanner_config", {"worker_count": 2}

    monkeypatch.setenv(process_ptg.PTG2_MANIFEST_PRECOPY_MERGE_ENV, "true")
    monkeypatch.setattr(process_ptg, "ptg2_temp_parent", lambda: tmp_path)
    monkeypatch.setattr(process_ptg, "resolve_ptg2_artifact_dir", lambda: artifact_dir)
    monkeypatch.setattr(process_ptg, "_push_ptg2_objects", fake_push_ptg2_objects)
    monkeypatch.setattr(process_ptg, "flush_error_log", fake_flush_error_log)
    monkeypatch.setattr(process_ptg, "_aiter_compact_serving_records_rust", fake_scanner)

    summary = asyncio.run(
        process_ptg._parse_in_network_file_serving_only(
            str(tmp_path / "rates.json.gz"),
            1,
            {"reporting_entity_name": "Example"},
            [{"plan_id": "plan", "plan_id_type": "ein", "plan_name": "Example PPO"}],
            provider_map={},
            test_mode=True,
            import_log_cls=SimpleNamespace(__name__="ImportLog"),
            source_url="https://example.test/rates.json.gz",
            source_version=None,
            snapshot_id="ptg2:test",
            import_month=process_ptg.normalize_import_month("2026-07"),
            ptg2_manifest_stage_table="ptg2_stage_test",
        )
    )

    _assert_recovered_worker_copy_summary(summary, recovered_paths_by_kind)


def test_scanner_error_labels_sigterm():
    message = ptg_rust_scanner._scanner_error_message(
        "PTG2 Rust compact scanner",
        -15,
        ["PTG2_DEDUPE_SUMMARY\tserving_rate_unique=146682732"],
    )

    assert "signal 15 (SIGTERM)" in message
    assert "PTG2_DEDUPE_SUMMARY" in message


def test_scanner_allows_sigterm_after_dedupe():
    assert ptg_rust_scanner._is_scanner_sigterm_after_dedupe(
        -15,
        ["PTG2_DEDUPE_SUMMARY\tserving_rate_unique=146682732"],
        has_stdout_terminal_summary=True,
    )
    assert not ptg_rust_scanner._is_scanner_sigterm_after_dedupe(
        -15,
        ["PTG2_DEDUPE_SUMMARY\tserving_rate_unique=146682732"],
    )
    assert not ptg_rust_scanner._is_scanner_sigterm_after_dedupe(
        -15,
        ["PTG2_SCANNER_PROGRESS\tpercent=99.66"],
        has_stdout_terminal_summary=True,
    )
    assert not ptg_rust_scanner._is_scanner_sigterm_after_dedupe(
        1,
        ["PTG2_DEDUPE_SUMMARY\tserving_rate_unique=146682732"],
        has_stdout_terminal_summary=True,
    )
