# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import binascii
import copy
import datetime
import gzip
import hashlib
import importlib
import io
import json
import os
import re
import struct
import subprocess
import sys
import threading
import time
import zipfile
from contextlib import asynccontextmanager
from types import SimpleNamespace
from decimal import Decimal
from pathlib import Path
from unittest.mock import ANY, AsyncMock, Mock

import pytest
from aiohttp import web
from tests.live_progress_atomic_redis import AtomicLiveProgressRedis


process_pkg = importlib.import_module("process")
process_ptg = importlib.import_module("process.ptg")
live_progress = importlib.import_module("process.live_progress")
ptg_allowed_amounts = importlib.import_module("process.ptg_parts.allowed_amounts")
ptg_artifacts = importlib.import_module("process.ptg_parts.artifacts")
ptg_artifact_streams = importlib.import_module("process.ptg_parts.artifact_streams")
ptg_canonical = importlib.import_module("process.ptg_parts.canonical")
ptg_copy_load = importlib.import_module("process.ptg_parts.copy_load")
ptg_db_tables = importlib.import_module("process.ptg_parts.db_tables")
ptg_domain = importlib.import_module("process.ptg_parts.domain")
healthsparq_source_jobs = importlib.import_module(
    "process.ptg_parts.healthsparq_source_jobs"
)
ptg_import_rows = importlib.import_module("process.ptg_parts.import_rows")
ptg_json_streams = importlib.import_module("process.ptg_parts.json_streams")
ptg_live_progress = importlib.import_module("process.ptg_parts.live_progress")
ptg_progress = importlib.import_module("process.ptg_parts.progress")
ptg_config = importlib.import_module("process.ptg_parts.config")
ptg_provider_quarantine = importlib.import_module(
    "process.ptg_parts.ptg2_provider_quarantine"
)


def test_candidate_audit_worker_requires_uvloop_before_startup(monkeypatch):
    if process_pkg.uvloop is None:
        pytest.fail("uvloop is a required runtime dependency")
    startup = AsyncMock()
    monkeypatch.setattr(process_pkg, "db_startup", startup)

    async def run_startup():
        await process_pkg._ptg_candidate_audit_startup({})

    process_pkg.uvloop.run(run_startup())

    startup.assert_awaited_once_with({})
    assert process_pkg.PTGCandidateAudit.on_startup is process_pkg._ptg_candidate_audit_startup


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
ptg_rust_scanner = importlib.import_module("process.ptg_parts.rust_scanner")
ptg_rust_stage = importlib.import_module("process.ptg_parts.rust_stage")
ptg_manifest_publish = importlib.import_module("process.ptg_parts.ptg2_manifest_publish")
ptg_screen = importlib.import_module("process.ptg_parts.screen")


def _empty_provider_identifier_quarantine_scanner_summary():
    return {
        "scanner": {
            "summary": {
                "provider_identifier_quarantine": (
                    ptg_provider_quarantine.provider_identifier_quarantine_payload({})
                )
            }
        }
    }


def _v4_empty_npi_normalization_payload(count):
    digest = hashlib.sha256()
    digest.update(
        process_ptg._V4_EMPTY_NPI_NORMALIZATION_HASH_DOMAIN
    )
    digest.update(int(count).to_bytes(8, "big"))
    return {
        "contract": process_ptg._V4_EMPTY_NPI_NORMALIZATION_CONTRACT,
        "source_shape": "empty_array",
        "canonical_equivalent": "zero_marker",
        "occurrence_count": count,
        "emitted_npi_edge_count": 0,
        "sha256": digest.hexdigest(),
    }


def test_shared_v4_empty_npi_evidence_is_exact_and_skips_duplicates():
    file_results = [
        {
            "summary": {
                "scanner": {
                    "summary": {
                        "empty_npi_tin_only_normalization": (
                            _v4_empty_npi_normalization_payload(2)
                        )
                    }
                }
            }
        },
        {
            "summary": {
                "scanner": {
                    "summary": {
                        "empty_npi_tin_only_normalization": (
                            _v4_empty_npi_normalization_payload(3)
                        )
                    }
                }
            }
        },
        {
            "skipped": True,
            "summary": {
                "scanner": {
                    "summary": {
                        "empty_npi_tin_only_normalization": (
                            _v4_empty_npi_normalization_payload(11)
                        )
                    }
                }
            },
        },
    ]

    assert process_ptg._sum_v4_tin_only_audits(
        file_results
    ) == 5


def test_shared_v4_empty_npi_evidence_rejects_digest_tamper():
    payload = _v4_empty_npi_normalization_payload(2)
    payload["occurrence_count"] = 3
    file_results = [
        {
            "summary": {
                "scanner": {
                    "summary": {
                        "empty_npi_tin_only_normalization": payload
                    }
                }
            }
        }
    ]

    with pytest.raises(RuntimeError, match="digest changed"):
        process_ptg._sum_v4_tin_only_audits(file_results)


@pytest.mark.parametrize(
    ("normalization_audit", "expected_error"),
    [
        (None, "omitted empty-NPI normalization evidence"),
        (
            {
                **_v4_empty_npi_normalization_payload(0),
                "occurrence_count": -1,
            },
            "normalization evidence is invalid",
        ),
    ],
)
def test_shared_v4_empty_npi_evidence_rejects_missing_or_negative_count(
    normalization_audit,
    expected_error,
):
    file_results = [
        {
            "summary": {
                "scanner": {
                    "summary": {
                        "empty_npi_tin_only_normalization": normalization_audit
                    }
                }
            }
        }
    ]

    with pytest.raises(RuntimeError, match=expected_error):
        process_ptg._sum_v4_tin_only_audits(file_results)


def test_shared_v3_quarantine_combines_scanners_and_skips_duplicates():
    first_quarantine = ptg_provider_quarantine.provider_identifier_quarantine_payload(
        {-17: 1, 123_456_789: 2}
    )
    second_quarantine = ptg_provider_quarantine.provider_identifier_quarantine_payload(
        {123_456_789: 3, 10_000_000_000: 4}
    )
    file_results = [
        {
            "summary": {
                "scanner": {
                    "summary": {
                        "provider_identifier_quarantine": first_quarantine,
                    }
                }
            }
        },
        {
            "summary": {
                "scanner": {
                    "summary": {
                        "provider_identifier_quarantine": second_quarantine,
                    }
                }
            }
        },
        {
            "skipped": True,
            "summary": {
                "scanner": {
                    "summary": {
                        "provider_identifier_quarantine": first_quarantine,
                    }
                }
            },
        },
    ]

    combined = process_ptg._shared_v3_provider_identifier_quarantine(file_results)

    assert combined == ptg_provider_quarantine.provider_identifier_quarantine_payload(
        {-17: 1, 123_456_789: 5, 10_000_000_000: 4}
    )


def test_shared_v3_quarantine_rejects_missing_active_evidence():
    file_results = [
        _empty_provider_identifier_quarantine_scanner_summary(),
        {"summary": {"scanner": {"summary": {}}}},
    ]

    with pytest.raises(
        RuntimeError,
        match="scanner omitted provider identifier quarantine evidence",
    ):
        process_ptg._shared_v3_provider_identifier_quarantine(file_results)


def test_default_ptg2_import_id_includes_source_inputs():
    month = process_ptg.normalize_import_month("2026-06")

    example_dental_id = process_ptg._default_ptg2_import_id(
        month,
        "ptg_d2fc90445ba744ef",
        allowed_url="https://example.com/example-dental-allowed.json.gz",
    )
    network_id = process_ptg._default_ptg2_import_id(
        month,
        "ptg_39d5fb35eae985f2",
        in_network_url="https://example.com/network-rates.json.gz",
    )
    shared_v2_id = process_ptg._default_ptg2_import_id(
        month,
        "ptg_39d5fb35eae985f2",
        in_network_url="https://example.com/network-rates.json.gz",
        arch_variant="shared_blocks_v3",
    )
    incompatible_generation_id = process_ptg._default_ptg2_import_id(
        month,
        "ptg_39d5fb35eae985f2",
        in_network_url="https://example.com/network-rates.json.gz",
        arch_variant="incompatible_test_generation",
    )

    assert example_dental_id != "20260601"
    assert network_id != "20260601"
    assert example_dental_id != network_id
    assert shared_v2_id != network_id
    assert incompatible_generation_id != network_id
    assert shared_v2_id != incompatible_generation_id


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

    enqueue_result = asyncio.run(
        process_ptg._enqueue_ptg2_auto_address_refresh_after_import(
            source_key="source-alpha",
            snapshot_id="snap-new",
            import_run_id="run-source-alpha",
            has_serving_files=True,
            source_scoped_compact=True,
            test_mode=False,
        )
    )

    assert enqueue_result["status"] == "queued"
    assert enqueue_result["created"] is True
    assert enqueue_result["run_id"] == "run-refresh"
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


ptg_snapshot_cleanup = importlib.import_module("process.ptg_parts.snapshot_cleanup")
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


def _weighted_progress_fixture(monkeypatch):
    """Create the two-file weighted progress fixture shared by focused tests."""

    events = []
    monkeypatch.setattr(
        ptg_progress,
        "write_live_progress",
        lambda **payload: events.append(payload),
    )
    coordinator = ptg_progress.PTGFileProgressCoordinator(
        [1, 99],
        ["https://payer.test/small.json.gz", "https://payer.test/large.json.gz"],
        stage_start_pct=20,
        stage_end_pct=90,
    )

    return coordinator, events


def test_weighted_file_progress_tracks_dominant_input(monkeypatch):
    """Weight stage progress by input size and expose the dominant file."""

    coordinator, events = _weighted_progress_fixture(monkeypatch)
    coordinator.observe(
        0,
        {
            "phase_pct": 80,
            "counters": {"provider_references": 100},
        },
    )
    coordinator.complete(0)
    assert events[-1]["pct"] == pytest.approx(20.7)
    assert events[-1]["counters"]["provider_references"] == 100
    coordinator.observe(
        1,
        {
            "phase": "compact-serving scanner",
            "phase_pct": 50,
            "counters": {
                "provider_references": 50,
                "indexed_rates_completed": 900,
            },
        },
    )
    assert events[-1]["pct"] == pytest.approx(55.35)
    assert events[-1]["file_index"] == 2
    assert events[-1]["file_count"] == 2
    assert events[-1]["file_name"] == "large.json.gz"
    assert events[-1]["dominant_file"] is True
    assert events[-1]["file_weight_pct"] == pytest.approx(99)
    assert events[-1]["counters"]["provider_references"] == 150
    assert events[-1]["counters"]["indexed_rates_completed"] == 900


def test_weighted_file_progress_does_not_regress(monkeypatch):
    """Keep progress and monotonic counters stable across delayed events."""

    coordinator, events = _weighted_progress_fixture(monkeypatch)
    coordinator.observe(0, {"phase_pct": 80, "counters": {"provider_references": 100}})
    coordinator.complete(0)
    coordinator.observe(
        1,
        {
            "phase": "compact-serving scanner",
            "phase_pct": 50,
            "counters": {
                "provider_references": 50,
                "indexed_rates_completed": 900,
            },
        },
    )
    coordinator.observe(
        1,
        {
            "phase_pct": 40,
            "message": "delayed",
            "counters": {"indexed_rates_completed": 800},
        },
    )
    assert events[-1]["pct"] == pytest.approx(55.35)
    assert events[-1]["counters"]["indexed_rates_completed"] == 900
    coordinator.observe(1, {"message": "scanner summary"})
    assert events[-1]["counters"]["provider_references"] == 150
    coordinator.observe(1, {"phase_pct": 100, "message": "scanner done"})
    assert events[-1]["pct"] < 90
    coordinator.complete(1)
    assert events[-1]["pct"] == 90
    assert events[-1]["done"] == events[-1]["total"]
    assert events[-1]["counters"]["indexed_rates_completed"] == 900


def test_weighted_file_progress_preserves_indeterminate_semantic_work(monkeypatch):
    """Do not turn an unfinished semantic counter into known compressed work."""

    coordinator, events = _weighted_progress_fixture(monkeypatch)
    for completed, chunks in ((65_536, 1), (131_072, 2)):
        coordinator.observe(
            1,
            {
                "phase": "compact-serving scanner",
                "phase_pct": 99,
                "pct": 99,
                "unit": "semantic_work",
                "basis": "semantic_work",
                "denominator_state": "unknown",
                "done": completed,
                "total": None,
                "work_done": completed,
                "work_total": None,
                "eta_seconds": 900,
                "counters": {"rate_chunks_completed": chunks},
            },
        )

    first, second = events
    assert [first["done"], second["done"]] == [65_536, 131_072]
    assert all(event["total"] is None for event in events)
    assert all(event["pct"] is None for event in events)
    assert all(event["phase_pct"] is None for event in events)
    assert all(event["stage_pct"] is None for event in events)
    assert all(event["unit"] == "semantic_work" for event in events)
    assert all(event["basis"] == "semantic_work" for event in events)
    assert all(event["denominator_state"] == "unknown" for event in events)
    assert all(event["eta_seconds"] is None for event in events)
    assert all(
        event["weighted_compressed_input_bytes_lower_bound"] == 0
        for event in events
    )
    assert all(event["pct_lower_bound"] == 20 for event in events)
    assert second["counters"]["rate_chunks_completed"] == 2


def test_weighted_file_progress_zero_weight_duplicate_does_not_add_work(
    monkeypatch,
):
    events = []
    monkeypatch.setattr(
        ptg_progress,
        "write_live_progress",
        lambda **payload: events.append(payload),
    )
    coordinator = ptg_progress.PTGFileProgressCoordinator(
        [100, 0],
        ["source.json.gz", "duplicate.json.gz"],
        stage_start_pct=30,
        stage_end_pct=90,
    )

    coordinator.complete(1)
    assert events[-1]["pct"] == 30
    assert events[-1]["total"] == 100
    assert events[-1]["counters"]["files_completed"] == 1


def test_ptg_live_progress_uses_redis_ttl_and_enqueues_status_event(monkeypatch):
    writes = []
    events = []

    fake_redis = AtomicLiveProgressRedis(
        on_progress_write=lambda key, ttl, value: writes.append(
            (key, ttl, value)
        )
    )
    monkeypatch.setattr(live_progress, "_redis", lambda: fake_redis)
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


def _bounded_progress_snapshot(
    run_id: str,
    attempt_started_at: str,
) -> dict[str, object]:
    """Return the determinate predecessor for the transition regression."""

    return {
        "run_id": run_id,
        "attempt_id": run_id,
        "attempt_started_at": attempt_started_at,
        "started_at": attempt_started_at,
        "status": "running",
        "source": "ptg-live-progress",
        "confidence": "live",
        "unit": "compressed_input_bytes",
        "basis": "weighted_compressed_input_bytes",
        "denominator_state": "known",
        "done": 50,
        "total": 100,
        "work_done": 50,
        "work_total": 100,
        "pct": 55,
        "stage_pct": 50,
        "phase_pct": 50,
        "eta_seconds": 60,
        "estimated_finish_at": "2026-07-23T10:01:00Z",
        "event_seq": 1,
        "progress_seq": 1,
    }


def test_live_progress_clears_stale_bounded_fields_for_semantic_work():
    """An indeterminate update must not inherit an older numeric percent."""

    run_id = "run-indeterminate-progress-fields"
    attempt_started_at = "2026-07-23T10:00:00Z"
    candidate = live_progress._merged_live_progress_candidate(
        run_id=run_id,
        context={
            "attempt_id": run_id,
            "attempt_started_at": attempt_started_at,
        },
        progress_by_field={
            "status": "running",
            "unit": "semantic_work",
            "basis": "semantic_work",
            "denominator_state": "unknown",
            "done": 65_536,
            "work_done": 65_536,
            "pct_lower_bound": 20,
            "stage_pct_lower_bound": 0,
            "weighted_compressed_input_bytes_lower_bound": 0,
            "counters": {"rate_chunks_completed": 1},
        },
        observed_at="2026-07-23T10:00:04Z",
        now=datetime.datetime(2026, 7, 23, 10, 0, 4),
        previous=_bounded_progress_snapshot(run_id, attempt_started_at),
    )

    assert candidate is not None
    for field_name in (
        "pct",
        "stage_pct",
        "phase_pct",
        "total",
        "work_total",
        "eta_seconds",
        "estimated_finish_at",
    ):
        assert field_name not in candidate
    assert candidate["done"] == 65_536
    assert candidate["denominator_state"] == "unknown"
    projected = live_progress.progress_payload_from_live(candidate)
    assert projected["pct_lower_bound"] == 20
    assert projected["stage_pct_lower_bound"] == 0
    assert projected["weighted_compressed_input_bytes_lower_bound"] == 0


def test_live_progress_preserves_earliest_started_at(monkeypatch):
    writes = []
    previous_map = {
        "run_id": "run_any",
        "importer": "npi",
        "status": "running",
        "started_at": "2026-06-03T12:00:00Z",
        "updated_at": "2026-06-03T12:01:00Z",
    }

    fake_redis = AtomicLiveProgressRedis(
        {
            "import:progress:run_any": json.dumps(previous_map).encode(
                "utf-8"
            )
        },
        on_progress_write=lambda key, ttl, value: writes.append(
            (key, ttl, value)
        ),
    )
    monkeypatch.setattr(live_progress, "_redis", lambda: fake_redis)
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

    progress_payload = json.loads(writes[0][2])
    assert progress_payload["started_at"] == previous_map["started_at"]


def test_live_progress_heartbeat_preserves_recent_importer_progress(monkeypatch):
    store_map = {}

    fake_redis = AtomicLiveProgressRedis(store_map)
    monkeypatch.setattr(live_progress, "_redis", lambda: fake_redis)
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
        source="engine-heartbeat",
        confidence="heartbeat",
    )

    progress_payload = json.loads(store_map["import:progress:run_any"])
    assert progress_payload["phase"] == "entity-address-unified building medication bridge"
    assert progress_payload["message"] == "building support table 5/7: medication bridge"
    assert progress_payload["unit"] == "steps"
    assert progress_payload["done"] == 5
    assert progress_payload["total"] == 7
    assert progress_payload["pct"] == 97
    assert progress_payload["source"] == "import-live-progress"
    assert progress_payload["confidence"] == "live"


def test_internal_ptg_import_heartbeat_updates_only_active_run(monkeypatch):
    calls = []

    async def immediate_sleep(_seconds):
        return None

    async def stop_after_update(statement, **params):
        calls.append((statement, params))
        raise asyncio.CancelledError

    monkeypatch.setattr(process_ptg.asyncio, "sleep", immediate_sleep)
    monkeypatch.setattr(process_ptg.db, "status", stop_after_update)

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(process_ptg._heartbeat_ptg2_import_run("ptg2:test-run"))

    assert len(calls) == 1
    statement, params = calls[0]
    assert 'UPDATE "mrf".ptg2_import_run' in statement
    assert "status IN ('pending', 'running', 'building')" in statement
    assert params == {"import_run_id": "ptg2:test-run"}


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


def test_db_table_split_keeps_facade_helpers_stable():
    assert process_ptg._quote_ident is ptg_db_tables._quote_ident
    assert process_ptg._table_exists is ptg_db_tables._table_exists
    assert process_ptg._has_rows_in_table is ptg_db_tables._has_rows_in_table
    assert process_ptg._estimated_table_rows is ptg_db_tables._estimated_table_rows
    assert process_ptg._exact_table_rows is ptg_db_tables._exact_table_rows






def test_copy_load_split_keeps_facade_helpers_stable():
    assert process_ptg._copy_upsert_ptg2_objects is ptg_copy_load._copy_upsert_ptg2_objects
    assert process_ptg._copy_insert_ptg2_objects is ptg_copy_load._copy_insert_ptg2_objects
    assert process_ptg._copy_ignore_ptg2_objects is ptg_copy_load._copy_ignore_ptg2_objects


def test_push_ptg2_objects_routes_snapshot_state_writes(monkeypatch):
    snapshot_by_field = {"snapshot_id": "ptg2:test"}
    stored_snapshot_by_field = {**snapshot_by_field, "status": "building"}
    preserve_snapshot = AsyncMock(return_value=stored_snapshot_by_field)
    monkeypatch.setattr(
        process_ptg,
        "_push_ptg2_snapshot_preserving_publication",
        preserve_snapshot,
    )

    result = asyncio.run(
        process_ptg._push_ptg2_objects(
            [snapshot_by_field],
            process_ptg.PTG2Snapshot,
        )
    )

    assert result == stored_snapshot_by_field
    preserve_snapshot.assert_awaited_once_with(snapshot_by_field)
    with pytest.raises(ValueError, match="exactly one row"):
        asyncio.run(
            process_ptg._push_ptg2_objects(
                [snapshot_by_field, snapshot_by_field],
                process_ptg.PTG2Snapshot,
            )
        )


def test_push_ptg2_objects_uses_each_specialized_copy_path(monkeypatch):
    copy_calls = []

    async def record_copy(copy_kind, object_entries, table_class):
        copy_calls.append((copy_kind, object_entries, table_class))

    async def copy_ignore(object_entries, table_class):
        await record_copy("ignore", object_entries, table_class)

    async def copy_insert(object_entries, table_class):
        await record_copy("insert", object_entries, table_class)

    async def copy_upsert(object_entries, table_class):
        await record_copy("upsert", object_entries, table_class)

    async def reject_generic_push(*_args, **_kwargs):
        raise AssertionError("specialized COPY should finish the write")

    class BulkTable:
        __tablename__ = "bulk_table"

    monkeypatch.setattr(process_ptg, "_env_bool", lambda *_args: True)
    monkeypatch.setattr(process_ptg, "_env_int", lambda *_args: 1)
    monkeypatch.setattr(process_ptg, "_copy_ignore_ptg2_objects", copy_ignore)
    monkeypatch.setattr(process_ptg, "_copy_insert_ptg2_objects", copy_insert)
    monkeypatch.setattr(process_ptg, "_copy_upsert_ptg2_objects", copy_upsert)
    monkeypatch.setattr(process_ptg, "push_objects", reject_generic_push)

    async def run_writes():
        await process_ptg._push_ptg2_objects(
            [{"price_set_hash": "price-set"}],
            process_ptg.PTG2PriceSet,
        )
        await process_ptg._push_ptg2_objects(
            [{"serving_rate_hash": "serving-rate"}],
            process_ptg.PTG2ServingRate,
        )
        await process_ptg._push_ptg2_objects([{"id": 1}], BulkTable)

    asyncio.run(run_writes())

    assert [copy_kind for copy_kind, _rows, _table in copy_calls] == [
        "ignore",
        "insert",
        "upsert",
    ]


def test_push_ptg2_objects_falls_back_to_legacy_push_signature(monkeypatch):
    push_calls = []

    async def fail_copy(*_args, **_kwargs):
        raise RuntimeError("COPY unavailable")

    async def legacy_push(object_entries, table_class, **kwargs):
        push_calls.append((object_entries, table_class, kwargs))
        if "use_copy" in kwargs:
            raise TypeError("unexpected keyword argument 'use_copy'")

    monkeypatch.setattr(process_ptg, "_env_bool", lambda *_args: True)
    monkeypatch.setattr(process_ptg, "_env_int", lambda *_args: 1)
    monkeypatch.setattr(process_ptg, "_copy_ignore_ptg2_objects", fail_copy)
    monkeypatch.setattr(process_ptg, "_copy_upsert_ptg2_objects", fail_copy)
    monkeypatch.setattr(process_ptg, "push_objects", legacy_push)

    object_entries = [{"price_set_hash": "price-set"}]
    asyncio.run(
        process_ptg._push_ptg2_objects(
            object_entries,
            process_ptg.PTG2PriceSet,
        )
    )

    assert push_calls == [
        (
            object_entries,
            process_ptg.PTG2PriceSet,
            {"rewrite": True, "use_copy": False},
        ),
        (object_entries, process_ptg.PTG2PriceSet, {"rewrite": True}),
    ]


def test_push_ptg2_objects_falls_back_after_direct_copy_failure(monkeypatch):
    push_objects = AsyncMock()
    monkeypatch.setattr(process_ptg, "_env_bool", lambda *_args: True)
    monkeypatch.setattr(process_ptg, "_env_int", lambda *_args: 250)
    monkeypatch.setattr(
        process_ptg,
        "_copy_insert_ptg2_objects",
        AsyncMock(side_effect=RuntimeError("COPY unavailable")),
    )
    monkeypatch.setattr(process_ptg, "push_objects", push_objects)

    object_entries = [{"serving_rate_hash": "serving-rate"}]
    asyncio.run(
        process_ptg._push_ptg2_objects(
            object_entries,
            process_ptg.PTG2ServingRate,
            rewrite=False,
        )
    )

    push_objects.assert_awaited_once_with(
        object_entries,
        process_ptg.PTG2ServingRate,
        rewrite=False,
        use_copy=False,
    )


def test_push_ptg2_objects_reraises_unrelated_type_errors(monkeypatch):
    async def fail_push(*_args, **_kwargs):
        raise TypeError("database adapter failed")

    class SmallTable:
        __tablename__ = "small_table"

    monkeypatch.setattr(process_ptg, "_env_bool", lambda *_args: False)
    monkeypatch.setattr(process_ptg, "_env_int", lambda *_args: 250)
    monkeypatch.setattr(process_ptg, "push_objects", fail_push)

    with pytest.raises(TypeError, match="database adapter failed"):
        asyncio.run(process_ptg._push_ptg2_objects([{"id": 1}], SmallTable))


def test_ptg2_copy_file_row_count_returns_zero_for_missing_file(tmp_path):
    assert process_ptg._ptg2_copy_file_row_count(tmp_path / "missing.copy") == 0


def test_row_mapping_prefers_sqlalchemy_mapping_attribute():
    row_object = SimpleNamespace(_mapping={"snapshot_id": "ptg2:test"})

    assert process_ptg._row_mapping(row_object) == {"snapshot_id": "ptg2:test"}


def test_copy_load_strips_postgres_nuls_from_text_values():
    row_map = {
        "plain": "ab\0cd",
        "array": ["ef\0gh"],
        "payload": {"ij\0": ["kl\0mn"]},
    }

    record = ptg_copy_load._ptg2_copy_record(row_map, ["plain", "array", "payload"], {"payload"})

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


def test_source_trace_uses_full_sha256_independent_of_compact_hash_mode(monkeypatch):
    source_url = "https://example.test/in-network-rates.json.gz"

    monkeypatch.setenv("HLTHPRT_PTG2_HASH_MODE", "checksum64")
    compact_row, compact_set = ptg_import_rows._ptg2_source_trace_rows(
        None,
        source_url,
    )
    monkeypatch.setenv("HLTHPRT_PTG2_HASH_MODE", "sha256")
    sha256_row, sha256_set = ptg_import_rows._ptg2_source_trace_rows(
        None,
        source_url,
    )

    assert compact_row["source_trace_hash"] == sha256_row["source_trace_hash"]
    assert re.fullmatch(r"[0-9a-f]{64}", compact_row["source_trace_hash"])
    assert compact_set["source_trace_set_hash"] == sha256_set["source_trace_set_hash"]
    assert re.fullmatch(r"[0-9a-f]{64}", compact_set["source_trace_set_hash"])


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


def test_multi_file_download_progress_preserves_stage_order(
    monkeypatch,
):
    events = []
    monkeypatch.setattr(
        ptg_source_download,
        "write_live_progress",
        lambda **payload: events.append(payload),
    )
    token = ptg_live_progress.set_live_progress_context(
        run_id="run_ptg",
        overall_progress_start_pct=5,
        overall_progress_end_pct=5,
        run_progress_hold_pct=5,
        file_count=2,
    )
    try:
        for completed_bytes in (90, 10):
            ptg_source_download._emit_download_progress(
                url=f"https://example.com/rates-{completed_bytes}.json.gz",
                bytes_read=completed_bytes,
                total_bytes=100,
                started_at=time.monotonic() - 1,
                done=False,
            )
    finally:
        ptg_live_progress.reset_live_progress_context(token)

    assert [event["pct"] for event in events] == [5, 5]
    assert [event["stage_pct"] for event in events] == [0, 0]
    assert [event["phase_pct"] for event in events] == [90, 10]


def test_download_worker_propagates_live_progress_context(monkeypatch):
    captured_map = {}

    def fake_downloader(job, **_kwargs):
        captured_map.update(ptg_live_progress.current_live_progress_context())
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
    assert captured_map["run_id"] == "run_ptg"
    assert captured_map["overall_progress_start_pct"] == 5


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
            raw_source_sha256="a" * 64,
            snapshot_id="snap",
            plan_id="plan",
            coverage_scope_id="c" * 64,
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

    monkeypatch.setattr(ptg_rust_scanner, "write_live_progress", lambda **progress_payload: events.append(progress_payload))

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
    progress_payload = events[0]
    assert progress_payload["run_id"] == "run_ptg"
    assert progress_payload["snapshot_id"] == "snap_1"
    assert progress_payload["phase"] == "compact-serving scanner"
    assert progress_payload["unit"] == "compressed_bytes"
    assert progress_payload["done"] == 1048576
    assert progress_payload["total"] == 2097152
    assert progress_payload["pct"] == 50.0
    assert progress_payload["eta_seconds"] == 10.0
    assert progress_payload["source"] == "ptg2-scanner-progress"
    assert progress_payload["scanner_objects"] == {"provider_references": 2, "in_network": 5}


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


@pytest.mark.parametrize(
    ("frame", "expected_basis", "expected_done", "expected_total", "counter_name"),
    (
        (
            "progress_basis=indexed_objects\tindexed_objects_completed=75\t"
            "indexed_objects_total=100\tindexed_rates_completed=900\t"
            "indexed_raw_bytes_completed=4096",
            "indexed_objects",
            75,
            100,
            "indexed_rates_completed",
        ),
        (
            "progress_basis=plain_reordered_objects\t"
            "in_network_objects_completed=30\tin_network_objects_total=40\t"
            "negotiated_rates_completed=800",
            "plain_reordered_objects",
            30,
            40,
            "negotiated_rates_completed",
        ),
    ),
)
def test_rust_scanner_progress_preserves_indexed_and_plain_denominators(
    monkeypatch,
    frame,
    expected_basis,
    expected_done,
    expected_total,
    counter_name,
):
    events = []
    monkeypatch.setattr(
        ptg_rust_scanner,
        "write_live_progress",
        lambda **payload: events.append(payload),
    )

    ptg_rust_scanner._emit_scanner_live_progress(
        "PTG2_SCANNER_PROGRESS\tpath=/work/raw/rates.json.gz\t"
        f"{frame}\tpercent=75.00\telapsed_seconds=3\teta_seconds=1\tdone=false",
        phase="compact-serving scanner",
        live_progress_context={
            "run_id": "run-ptg-progress-v2",
            "overall_progress_start_pct": 20,
            "overall_progress_end_pct": 90,
        },
    )

    [progress] = events
    assert progress["unit"] == expected_basis
    assert progress["basis"] == expected_basis
    assert progress["done"] == expected_done
    assert progress["total"] == expected_total
    assert progress["phase_pct"] == 75
    assert progress["pct"] == 72.5
    assert progress["counters"][counter_name] > 0
    assert "indexed_objects_completed" not in progress["counters"]
    assert "in_network_objects_completed" not in progress["counters"]


def test_rust_scanner_semantic_progress_advances_during_one_long_item(monkeypatch):
    events = []
    monkeypatch.setattr(
        ptg_rust_scanner,
        "write_live_progress",
        lambda **payload: events.append(payload),
    )

    for completed, chunks in ((65_536, 1), (131_072, 2)):
        ptg_rust_scanner._emit_scanner_live_progress(
            "PTG2_SCANNER_PROGRESS\tpath=/work/raw/reference-extreme.json.gz\t"
            "progress_basis=semantic_work\t"
            f"semantic_work_completed={completed}\t"
            "compressed_bytes=99\ttotal_bytes=100\tpercent=99.00\t"
            "elapsed_seconds=8\teta_seconds=unknown\tobjects=1\t"
            f"rate_chunks_completed={chunks}\tin_network=0\tdone=false",
            phase="compact-serving scanner",
            live_progress_context={
                "run_id": "run-semantic-long-item",
                "overall_progress_start_pct": 20,
                "overall_progress_end_pct": 90,
            },
        )

    assert [event["done"] for event in events] == [65_536, 131_072]
    assert all(event["total"] is None for event in events)
    assert all(event["pct"] is None for event in events)
    assert all(event["phase_pct"] is None for event in events)
    assert all(event["stage_pct"] is None for event in events)
    assert all(event["pct_lower_bound"] == 20 for event in events)
    assert all(event["denominator_state"] == "unknown" for event in events)
    assert all(event["eta_seconds"] is None for event in events)
    assert all(event["basis"] == "semantic_work" for event in events)
    assert [event["counters"]["rate_chunks_completed"] for event in events] == [1, 2]
    assert all(event["counters"]["in_network"] == 0 for event in events)
    assert all("semantic_work_completed" not in event["counters"] for event in events)

    now = datetime.datetime.now(datetime.UTC).replace(tzinfo=None)
    live_progress._sequence_progress(events[0], None, now=now, succeeded=False)
    live_progress._sequence_progress(events[1], events[0], now=now, succeeded=False)
    assert events[1]["progress_seq"] == events[0]["progress_seq"] + 1


def _install_timed_progress_capture(monkeypatch, elapsed_seconds):
    progress_writes = []
    base_time = datetime.datetime(2026, 7, 23, 10, 0, 0)
    fake_redis = AtomicLiveProgressRedis(
        on_progress_write=lambda _key, _ttl, encoded_progress: progress_writes.append(
            (elapsed_seconds[0], json.loads(encoded_progress))
        )
    )
    monkeypatch.setattr(process_ptg, "_ptg2_monotonic", lambda: elapsed_seconds[0])
    monkeypatch.setattr(live_progress, "_redis", lambda: fake_redis)
    monkeypatch.setattr(
        live_progress,
        "_utc_now",
        lambda: base_time + datetime.timedelta(seconds=elapsed_seconds[0]),
    )
    monkeypatch.setattr(live_progress, "enqueue_status_event", lambda _event: None)
    return progress_writes


def _slow_manifest_copy(elapsed_seconds):
    async def copy_file(unused_copy_path, *, target_table, progress_callback):
        del unused_copy_path
        assert target_table == "price_atom_stage"
        for _byte_index in range(10):
            elapsed_seconds[0] += 1.0
            progress_callback(1)

    return copy_file


def _assert_manifest_copy_cadence(progress_writes):
    progress_snapshots = [
        progress_snapshot for _at, progress_snapshot in progress_writes
    ]
    intermediate_bytes = [
        progress_snapshot["counters"]["manifest_copy_bytes"]
        for progress_snapshot in progress_snapshots
        if 0
        < int(
            (progress_snapshot.get("counters") or {}).get(
                "manifest_copy_bytes"
            )
            or 0
        )
        < 10
    ]
    progress_gaps = [
        later_at - earlier_at
        for (earlier_at, _earlier), (later_at, _later) in zip(
            progress_writes,
            progress_writes[1:],
        )
    ]
    assert intermediate_bytes == [4, 8]
    assert max(progress_gaps) <= 4.0
    assert [
        progress_snapshot["progress_seq"]
        for progress_snapshot in progress_snapshots
    ] == list(range(1, len(progress_snapshots) + 1))


@pytest.mark.asyncio
async def test_manifest_copy_reports_semantic_bytes_every_four_seconds(
    monkeypatch,
    tmp_path,
):
    """A single slow COPY must move progress_seq before the file completes."""

    copy_path = tmp_path / "price-atoms.copy"
    copy_path.write_bytes(b"0123456789")
    elapsed_seconds = [0.0]
    progress_writes = _install_timed_progress_capture(
        monkeypatch,
        elapsed_seconds,
    )

    token = ptg_live_progress.set_live_progress_context(
        run_id="run-slow-manifest-copy",
        attempt_id="attempt-1",
        attempt_started_at="2026-07-23T10:00:00Z",
    )
    try:
        await process_ptg._copy_manifest_files_direct_with_progress(
            "price_atom",
            target_table="price_atom_stage",
            input_paths=[copy_path],
            copy_func=_slow_manifest_copy(elapsed_seconds),
            completed_steps_before_copy=0,
            total_steps=1,
            emitted_rows=10,
        )
    finally:
        ptg_live_progress.reset_live_progress_context(token)

    _assert_manifest_copy_cadence(progress_writes)


def test_async_rust_scanner_passes_live_progress_context(monkeypatch, tmp_path):
    captured_map = {}

    def fake_iter(*_args, **kwargs):
        captured_map.update(kwargs)
        return iter(())

    monkeypatch.setattr(ptg_rust_scanner, "_iter_compact_serving_records_rust", fake_iter)

    async def run():
        token = ptg_live_progress.set_live_progress_context(run_id="run_ptg", snapshot_id="snap_1")
        try:
            async for _record in ptg_rust_scanner._aiter_compact_serving_records_rust(
                tmp_path / "rates.json.gz",
                raw_source_sha256="a" * 64,
                snapshot_id="snap",
                plan_id="plan",
                coverage_scope_id="c" * 64,
                plan_month_id="month",
                source_trace_set_hash="trace",
            ):
                captured_map["records_seen"] = captured_map.get("records_seen", 0) + 1
        finally:
            ptg_live_progress.reset_live_progress_context(token)

    asyncio.run(run())

    assert captured_map["live_progress_context"]["run_id"] == "run_ptg"
    assert captured_map["live_progress_context"]["snapshot_id"] == "snap_1"



def test_json_stream_split_keeps_facade_helpers_stable():
    assert process_ptg._json_loads is ptg_json_streams._json_loads
    assert process_ptg._iter_top_level_objects is ptg_json_streams._iter_top_level_objects
    assert process_ptg._iter_top_level_object_bytes is ptg_json_streams._iter_top_level_object_bytes
    assert process_ptg._iter_top_level_objects_jsondecoder is ptg_json_streams._iter_top_level_objects_jsondecoder
    assert process_ptg._iter_top_level_objects_fast is ptg_json_streams._iter_top_level_objects_fast



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


def test_ptg2_job_dedupe_merges_plans_in_linear_work(monkeypatch):
    plan_count = 250
    shared_url = "https://files.example.test/shared-rates.json.gz"
    source_jobs = [
        {
            "type": "in_network",
            "url": shared_url,
            "plan_info": [{"plan_id": f"PLAN-{plan_index}"}],
        }
        for plan_index in range(plan_count)
    ]
    original_dumps = ptg_source_jobs.canonical_json_dumps
    serialized_plan_payloads = []

    def tracked_dumps(plan_payload):
        serialized_plan_payloads.append(plan_payload)
        return original_dumps(plan_payload)

    monkeypatch.setattr(ptg_source_jobs, "canonical_json_dumps", tracked_dumps)

    deduped_jobs, duplicate_count = process_ptg._dedupe_ptg_jobs(source_jobs)

    assert duplicate_count == plan_count - 1
    assert len(deduped_jobs) == 1
    assert len(deduped_jobs[0]["plan_info"]) == plan_count
    assert len(serialized_plan_payloads) == plan_count




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





def test_ptg2_fast_object_iterator_yields_selected_top_level_arrays():
    payload = (
        b'{"version":"1.0","provider_references":[{"provider_group_id":7,'
        b'"provider_groups":[{"npi":[1],"note":"brace } in string"}]}],'
        b'"in_network":[{"negotiation_arrangement":"ffs","billing_code":"001",'
        b'"negotiated_rates":[{"negotiated_prices":[{"negotiated_rate":1.2}]}]}]}'
    )

    result_list = list(
        process_ptg._iter_top_level_objects_fast(
            io.BytesIO(payload),
            {
                "provider_reference": "provider_references.item",
                "in_network": "in_network.item",
            },
        )
    )

    assert [name for name, _ in result_list] == ["provider_reference", "in_network"]
    assert result_list[0][1]["provider_group_id"] == 7
    assert result_list[1][1]["billing_code"] == "001"


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
        ["123456789", "1053488122", 1093228306, "", None, "bad", "10000000000"]
    ) == [
        1053488122,
        1093228306,
    ]


def test_price_atom_normalizes_payer_joined_billing_modifiers():
    packed = process_ptg._ptg2_price_atom_row(
        {
            "negotiated_rate": "12.34",
            "billing_code_modifier": ["tc, 26", "26"],
        }
    )
    split = process_ptg._ptg2_price_atom_row(
        {
            "negotiated_rate": "12.34",
            "billing_code_modifier": ["26", "TC"],
        }
    )

    assert packed["billing_code_modifier"] == ["26", "TC"]
    assert packed["price_atom_hash"] == split["price_atom_hash"]



def test_ptg2_semantic_hash_ignores_set_like_array_order():
    first_map = {
        "npi": [1093228306, 1053488122],
        "service_code": ["02", "01"],
        "billing_code_modifier": ["TC", "26"],
        "bundled_codes": [
            {"billing_code": "B", "billing_code_type": "HCPCS"},
            {"billing_code": "A", "billing_code_type": "CPT"},
        ],
        "negotiated_rate": Decimal("12.3400"),
    }
    second_map = {
        "npi": [1053488122, 1093228306],
        "service_code": ["01", "02"],
        "billing_code_modifier": ["26", "TC"],
        "bundled_codes": [
            {"billing_code_type": "CPT", "billing_code": "A"},
            {"billing_code_type": "HCPCS", "billing_code": "B"},
        ],
        "negotiated_rate": "12.34",
    }

    assert process_ptg.semantic_hash(first_map, domain="rate") == process_ptg.semantic_hash(second_map, domain="rate")


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
    base_map = {
        "negotiated_rate": Decimal("12.34"),
        "billing_code_modifier": ["26", "TC"],
        "setting": "outpatient",
        "context": {"plan_id": "TESTPLAN001"},
    }
    modified_map = dict(base_map)
    modified_map.update(changed)

    assert process_ptg.semantic_hash(base_map, domain="rate") != process_ptg.semantic_hash(modified_map, domain="rate")


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
    tin_a_map = {"type": "EIN", "value": "12-3456789"}
    tin_b_map = {"type": "ein", "value": "123456789"}

    first = process_ptg._provider_group_identity_hash(tin_a_map, [1053488122, 123456789, 1093228306])
    second = process_ptg._provider_group_identity_hash(tin_b_map, [1093228306, 1053488122])

    assert first == second


def test_ptg2_provider_set_entry_packs_groups_order_insensitively():
    groups_a_list = [
        {"tin": {"type": "ein", "value": "111"}, "npi": [1000000003, 1000000001]},
        {"tin": {"type": "ein", "value": "222"}, "npi": [1000000002]},
    ]
    groups_b_list = list(reversed(groups_a_list))

    entry_a, row_a = process_ptg._build_provider_set_entry(
        file_id=1,
        provider_group_ref=10,
        provider_groups=groups_a_list,
        network_names=["A"],
    )
    entry_b, row_b = process_ptg._build_provider_set_entry(
        file_id=2,
        provider_group_ref=20,
        provider_groups=groups_b_list,
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
    groups_a_list = [{"tin": {"type": "ein", "value": "12-3456789"}, "npi": [1000000003, 1000000001, 1000000002]}]
    groups_b_list = [{"tin": {"type": "EIN", "value": "123456789"}, "npi": [1000000002, 1000000003, 1000000001]}]

    row_a = process_ptg._ptg2_provider_group_rows(provider_groups=groups_a_list)[0]
    row_b = process_ptg._ptg2_provider_group_rows(provider_groups=groups_b_list)[0]

    assert row_a["provider_group_hash"] == row_b["provider_group_hash"]
    assert row_a["npi"] == [1000000001, 1000000002, 1000000003]
    assert row_a["tin_type"] == "ein"
    assert row_a["tin_value"] == "123456789"


def test_ptg2_procedure_identity_groups_display_text_variants():
    base_map = {
        "billing_code_type": "CPT",
        "billing_code_type_version": "2026",
        "billing_code": "99213",
        "negotiation_arrangement": "ffs",
        "name": "Office visit",
        "description": "First description",
    }
    variant_map = {**base_map, "name": "Established patient visit", "description": "Different description"}
    changed_arrangement_map = {**base_map, "negotiation_arrangement": "bundle"}

    assert process_ptg._ptg2_procedure_row(base_map)["procedure_hash"] == process_ptg._ptg2_procedure_row(variant_map)["procedure_hash"]
    assert (
        process_ptg._ptg2_procedure_row(base_map)["procedure_hash"]
        != process_ptg._ptg2_procedure_row(changed_arrangement_map)["procedure_hash"]
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


def test_toc_parser_handles_uhc_duplicates():
    toc_map = {
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
        toc_map,
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
    toc_map = {
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

    entries = process_ptg.parse_toc_catalog_entries(toc_map, "https://payer.test/toc.json")

    assert [entry.source_type for entry in entries] == [
        "table-of-contents",
        "in-network",
        "allowed-amounts",
        "allowed-amounts",
    ]
    assert entries[1].original_url == "https://cdn.test/in-network-rates.json.gz"
    assert entries[2].description == "Allowed 1"
    assert entries[3].description == "Allowed 2"


def test_ptg2_toc_parser_accepts_healthsparq_metadata_files(monkeypatch):
    """HealthSparq metadata entries should become normal PTG catalog entries."""
    monkeypatch.setenv("HLTHPRT_PTG2_HASH_MODE", "checksum64")
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
    assert (
        in_network_catalog_entries[0].plan_info[0]["engine_plan_hash"]
        == "5c283e9b979b6d5c"
    )


@pytest.mark.parametrize("missing_key", ["planName", "planId", "planMarketType"])
def test_healthsparq_plan_engine_hash_requires_complete_identity(missing_key):
    source_file_values_by_field = {
        "reportingEntityName": "Example Health Administrator",
        "reportingEntityType": "Third Party Administrator_ABC123",
    }
    plan_values_by_field = {
        "planName": "Example Dental Plan",
        "planId": "123456789",
        "planIdType": "ein",
        "planMarketType": "group",
    }
    plan_values_by_field.pop(missing_key)

    assert (
        healthsparq_source_jobs.healthsparq_plan_engine_hash(
            source_file_values_by_field,
            plan_values_by_field,
        )
        is None
    )


@pytest.mark.parametrize(
    ("file_schema", "expected_source_type"),
    [
        ("IN_NETWORK_RATES", "in-network"),
        ("allowed amounts", "allowed-amounts"),
        ("table-of-contents", "table-of-contents"),
        ("prescription drug file", "payer-drug"),
        ("unknown", None),
    ],
)
def test_healthsparq_source_schema_families(
    file_schema,
    expected_source_type,
):
    source_type_and_domain = healthsparq_source_jobs._source_type_and_domain(
        file_schema
    )

    if expected_source_type is None:
        assert source_type_and_domain is None
    else:
        assert source_type_and_domain[0] == expected_source_type


def test_healthsparq_metadata_edge_shapes():
    metadata_file_by_field = {"filePath": "rates.json.gz"}

    assert healthsparq_source_jobs._metadata_files(
        {"data": {"files": [metadata_file_by_field]}}
    ) == [metadata_file_by_field]
    assert healthsparq_source_jobs._metadata_files(
        {"data": [metadata_file_by_field]}
    ) == [metadata_file_by_field]
    assert healthsparq_source_jobs._file_url("https://example.test/meta.json", {}) == ""
    assert healthsparq_source_jobs._file_url(
        "https://example.test/meta.json",
        {"filePath": "https://cdn.example.test/rates.json.gz"},
    ) == "https://cdn.example.test/rates.json.gz"


@pytest.mark.parametrize(
    ("location", "expected_result"),
    [
        ("", False),
        ("file:///tmp/rates.json.gz", False),
        ("https://example.test/download?file=rates.json.gz", True),
    ],
)
def test_healthsparq_download_location_validation(location, expected_result):
    assert (
        healthsparq_source_jobs._is_file_location_like_download(location)
        is expected_result
    )


@pytest.mark.parametrize(
    "source_file_values_by_field",
    [
        {"fileSchema": "unknown"},
        {"fileSchema": "IN_NETWORK_RATES", "filePath": ""},
        {
            "fileSchema": "IN_NETWORK_RATES",
            "filePath": "rates.json.gz",
            "reportingPlans": [],
        },
    ],
)
def test_healthsparq_catalog_entry_rejects_non_importable_files(
    source_file_values_by_field,
):
    assert (
        healthsparq_source_jobs._catalog_entry_from_file(
            source_file_values_by_field,
            "https://example.test/latest_metadata.json",
            {"reporting_entity_name": "Example", "reporting_entity_type": "payer"},
            plan_ids=None,
            plan_name_contains=None,
            plan_market_types=None,
        )
        is None
    )


def test_healthsparq_filter_mismatches():
    plan_values_by_field = {
        "plan_id": "123456789",
        "plan_market_type": "group",
        "plan_name": "Example Dental Plan",
        "plan_sponsor_name": "Example Employer",
    }

    assert not healthsparq_source_jobs._is_plan_matching_filters(
        plan_values_by_field,
        plan_ids=["987654321"],
        plan_name_contains=None,
        plan_market_types=None,
    )
    assert not healthsparq_source_jobs._is_plan_matching_filters(
        plan_values_by_field,
        plan_ids=None,
        plan_name_contains=None,
        plan_market_types=["individual"],
    )


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
    toc_map = {
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

    entries = process_ptg.parse_toc_catalog_entries(toc_map, "https://payer.test/toc.json")

    assert [entry.source_type for entry in entries] == [
        "table-of-contents",
        "allowed-amounts",
        "allowed-amounts",
    ]
    assert entries[1].description == "Allowed 1"
    assert entries[2].description == "Allowed 2"


def test_ptg2_toc_parser_accepts_flat_carrier_file_lists():
    toc_payload_by_key = {
        "lastupdated": "2026-07-01",
        "In-Network Negotiated Rates Files": [
            {
                "url": "https://cdn.example.test/region-in-network-rates.json.gz?Signature=random-rx-oon-token",
                "displayname": "region-in-network-rates.json.gz",
            }
        ],
        "Out-of-Network Allowed Amounts Files": [
            {
                "url": "https://cdn.example.test/allowed-amounts.json.gz?Signature=temporary",
                "displayname": "allowed-amounts.json.gz",
            }
        ],
        "Association Out-of-Area Rates Files": [
            {
                "url": "https://cdn.example.test/out-of-area-rates.json.gz?Signature=temporary",
                "displayname": "out-of-area-rates.json.gz",
            }
        ],
    }

    catalog_entries = process_ptg.parse_toc_catalog_entries(
        toc_payload_by_key,
        "https://payer.example.test/employer-index.json",
    )

    assert [entry.source_type for entry in catalog_entries] == [
        "table-of-contents",
        "in-network",
        "allowed-amounts",
        "in-network",
    ]
    assert catalog_entries[1].description == "region-in-network-rates.json.gz"
    assert catalog_entries[1].original_url.endswith("Signature=random-rx-oon-token")
    assert catalog_entries[1].plan_info == ()
    assert catalog_entries[2].domain == process_ptg.PTG2_DOMAIN_ALLOWED_AMOUNT
    assert catalog_entries[3].domain == process_ptg.PTG2_DOMAIN_IN_NETWORK


def test_ptg2_flat_toc_derives_group_scope_from_ein_index():
    """Treat a numeric EIN index filename as its logical group-plan scope."""

    catalog_entries = process_ptg.parse_toc_catalog_entries(
        {
            "In-Network Negotiated Rates Files": [
                {"url": "https://cdn.example.test/in-network-rates.json.gz"}
            ]
        },
        "https://payer.example.test/12-3456789.json",
    )

    assert catalog_entries[1].plan_info == (
        {
            "plan_id": "123456789",
            "plan_id_type": "EIN",
            "plan_market_type": "group",
        },
    )


def test_ptg2_toc_jobs_target_flat_carrier_file_lists(monkeypatch, tmp_path):
    """Carry flat-index EIN scope into selected jobs and persisted file rows."""

    target_file_name = "region-target-in-network-rates.json.gz"
    toc_document_path = tmp_path / "employer-index.json"
    toc_items = [
        {"url": "https://cdn.example.test/unused-in-network-rates.json.gz"},
        {
            "url": f"https://cdn.example.test/{target_file_name}?Signature=random-rx-oon-token",
            "displayname": target_file_name,
        },
    ]
    toc_document_path.write_text(
        json.dumps({"In-Network Negotiated Rates Files": toc_items}),
        encoding="utf-8",
    )
    pushed_file_rows = []

    async def fake_materialize(*_args, **_kwargs):
        toc_artifact = SimpleNamespace(logical_path=toc_document_path)
        return toc_artifact, toc_artifact

    async def fake_push_objects(file_rows, _model, **_kwargs):
        pushed_file_rows.extend(file_rows)

    monkeypatch.setattr(process_ptg, "materialize_json_source", fake_materialize)
    monkeypatch.setattr(process_ptg, "push_objects", fake_push_objects)
    monkeypatch.setattr(process_ptg, "flush_error_log", AsyncMock())

    selected_jobs = asyncio.run(
        process_ptg._process_table_of_contents(
            "https://payer.example.test/123456789.json",
            {"PTGFile": object, "ImportLog": object},
            test_mode=False,
            file_url_contains=[target_file_name],
            max_files=1,
        )
    )

    assert [job["type"] for job in selected_jobs] == ["in_network"]
    assert selected_jobs[0]["url"] == (
        f"https://cdn.example.test/{target_file_name}?Signature=random-rx-oon-token"
    )
    assert selected_jobs[0]["description"] == target_file_name
    assert selected_jobs[0]["plan_info"][0]["plan_id"] == "123456789"
    assert [file_row["file_type"] for file_row in pushed_file_rows] == [
        "table-of-contents",
        "in-network",
    ]
    assert pushed_file_rows[1]["plan_id"] == "123456789"


def test_ptg2_toc_parser_normalizes_asr_download_links():
    toc_map = {
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

    entries = process_ptg.parse_toc_catalog_entries(toc_map, "https://payer.test/toc.json")
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
    toc_map = {
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
    monkeypatch.setattr(process_ptg, "load_json_artifact", lambda _path: toc_map)
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
    assert any(job_row["url"] == expected_url and job_row["file_type"] == "in-network" for job_row in pushed_file_rows)


def test_toc_limit_merges_shared_file_plan_scopes(monkeypatch):
    """Keep every logical plan when one selected physical file is shared."""

    shared_url = "https://files.example.test/selected-rates.json.gz"
    toc_map = {
        "reporting_entity_name": "Example Payer",
        "reporting_entity_type": "payer",
        "reporting_structure": [
            {
                "reporting_plans": [
                    {
                        "plan_name": f"Example Plan {suffix}",
                        "plan_id": f"PLAN-{suffix}",
                        "plan_market_type": "group",
                    }
                ],
                "in_network_files": [{"location": shared_url}],
            }
            for suffix in ("A", "B")
        ],
    }
    pushed_file_rows = []

    async def fake_materialize(*_args, **_kwargs):
        artifact = SimpleNamespace(logical_path="/tmp/example-toc.json")
        return artifact, artifact

    async def fake_push_objects(file_rows_to_push, _cls, **_kwargs):
        pushed_file_rows.extend(file_rows_to_push)

    monkeypatch.setattr(process_ptg, "materialize_json_source", fake_materialize)
    monkeypatch.setattr(process_ptg, "load_json_artifact", lambda _path: toc_map)
    monkeypatch.setattr(process_ptg, "push_objects", fake_push_objects)
    monkeypatch.setattr(process_ptg, "flush_error_log", AsyncMock())

    selected_jobs = asyncio.run(
        process_ptg._process_table_of_contents(
            "https://files.example.test/toc.json",
            {"PTGFile": object, "ImportLog": object},
            test_mode=False,
            file_url_contains=["selected-rates"],
            max_files=1,
        )
    )

    deduped_jobs, duplicate_count = process_ptg._dedupe_ptg_jobs(selected_jobs)

    assert duplicate_count == 1
    assert len(deduped_jobs) == 1
    assert deduped_jobs[0]["url"] == shared_url
    assert {plan["plan_id"] for plan in deduped_jobs[0]["plan_info"]} == {
        "PLAN-A",
        "PLAN-B",
    }
    assert [file_row["plan_id"] for file_row in pushed_file_rows[1:]] == [
        "PLAN-A",
        "PLAN-B",
    ]


def test_ptg2_toc_repairs_missing_array_commas_and_ignores_unsupported_files(
    monkeypatch, tmp_path
):
    """Verify this PTG import regression contract."""
    toc_path = tmp_path / "issuer_toc.json"
    toc_path.write_text(
        """
        {
          "reporting_entity_name": "Example Health Issuer",
          "reporting_entity_type": "Health Insurance Issuer",
          "version": "1.0.0",
          "reporting_structure": [
            {
              "reporting_plans": [
                {
                  "plan_name": "Example Individual Plan",
                  "plan_id_type": "hios",
                  "plan_id": "85736MN023",
                  "plan_market_type": "Individual"
                }
              ],
              "in_network_files": [
                {
                  "description": "rates for primary network",
                  "location": "https://files.example.test/Primary_InNetwork.json?download=true"
                }
                {
                  "description": "rates for dental network",
                  "location": "https://files.example.test/Dental_InNetwork.json?download=true"
                }
              ],
              "allowed_amount_file": [
                {
                  "description": "primary allowed amounts",
                  "location": "https://files.example.test/Primary_AllowedAmount.json?download=true"
                },
                {
                  "description": "Fulcrum allowed amounts",
                  "location": "https://files.example.test/Secondary_AllowedAmount.json?download=true"
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
            "https://files.example.test/issuer_toc.json?download=true",
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
    assert jobs[3]["url"].endswith(
        "/Secondary_AllowedAmount.json?download=true"
    )
    assert [repaired_row["file_type"] for repaired_row in pushed_file_rows] == [
        "table-of-contents",
        "in-network",
        "in-network",
        "allowed-amounts",
        "allowed-amounts",
    ]


def _write_targeted_allowed_amount_toc_fixture(
    tmp_path,
    allowed_url: str,
) -> Path:
    toc_path = tmp_path / "toc.json"
    toc_path.write_text(
        json.dumps(
            {
                "reporting_entity_name": "Example Payer",
                "reporting_entity_type": "payer",
                "reporting_structure": [
                    {
                        "reporting_plans": [
                            {
                                "plan_name": "Example Group Plan",
                                "plan_id": "TESTPLAN001",
                                "plan_id_type": "ein",
                                "plan_market_type": "group",
                            }
                        ],
                        "in_network_files": [
                            {
                                "location": (
                                    "https://files.example.test/"
                                    "2026-07_in-network-rates.json.gz"
                                )
                            }
                        ],
                        "allowed_amount_file": {
                            "description": "historical payment evidence",
                            "location": allowed_url,
                        },
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    return toc_path


def test_ptg2_toc_jobs_emit_targeted_allowed_amount_file(monkeypatch, tmp_path):
    allowed_url = (
        "https://files.example.test/2026-07_target-allowed-amounts.json.gz"
    )
    toc_path = _write_targeted_allowed_amount_toc_fixture(
        tmp_path,
        allowed_url,
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
            "https://files.example.test/toc.json",
            {"PTGFile": object, "ImportLog": object},
            test_mode=False,
            file_url_contains=["target-allowed-amounts"],
            max_files=1,
        )
    )

    assert [(job["type"], job["url"]) for job in jobs] == [
        ("allowed_amounts", allowed_url)
    ]
    assert jobs[0]["plan_info"][0]["plan_id"] == "TESTPLAN001"
    assert [file_row["file_type"] for file_row in pushed_file_rows] == [
        "table-of-contents",
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
    assert [job_row["file_type"] for job_row in pushed_file_rows] == [
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
        repaired_row["description"]
        for repaired_row in toc["reporting_structure"][0]["in_network_files"]
    ]
    assert descriptions == ["keep }{ literal", "next file"]


def test_ptg2_artifact_reuse_by_strong_etag_and_length(tmp_path):
    raw_path = tmp_path / "raw.json"
    raw_path.write_text('{"ok": true}', encoding="utf-8")
    raw_sha, byte_count = process_ptg.sha256_file(raw_path)
    store = process_ptg.PTG2ArtifactStore(tmp_path)
    candidate_map = {
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

    reused, mode = process_ptg.choose_reusable_raw_artifact([candidate_map], head, store=store)

    assert reused == candidate_map
    assert mode == "strong_etag_length"


def test_ptg2_artifact_reuse_skips_missing_metadata_candidate(tmp_path):
    missing_path = tmp_path / "missing.json.gz"
    store = process_ptg.PTG2ArtifactStore(tmp_path / "store")
    candidate_map = {
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

    reused, mode = process_ptg.choose_reusable_raw_artifact([candidate_map], head, store=store)

    assert reused is None
    assert mode is None


def test_download_raw_artifact_redownloads_bad_gzip_reuse_candidate(monkeypatch, tmp_path):
    downloaded_bytes = gzip.compress(b'{"ok": true}')
    bad_path = tmp_path / "bad.json.gz"
    bad_path.write_bytes(b"\0\0" + b"x" * (len(downloaded_bytes) - 2))
    bad_sha, bad_size = process_ptg.sha256_file(bad_path)
    store = process_ptg.PTG2ArtifactStore(tmp_path / "store")

    async def handle(request):
        if request.method == "HEAD":
            return web.Response(
                headers={
                    "Content-Length": str(len(downloaded_bytes)),
                    "ETag": '"reuse-test"',
                }
            )
        return web.Response(body=downloaded_bytes, headers={"Content-Length": str(len(downloaded_bytes))})

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
                "content_length": len(downloaded_bytes),
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
    assert Path(artifact.raw_path).read_bytes() == downloaded_bytes
    assert any('"status": "corrupt"' in line for line in manifest_lines)
    assert any('"status": "available"' in line and artifact.raw_sha256 in line for line in manifest_lines)


def test_download_raw_artifact_redownloads_corrupt_gzip_reuse_candidate(monkeypatch, tmp_path):
    downloaded_bytes = gzip.compress(b'{"ok": true}' * 1024)
    bad_payload = bytearray(downloaded_bytes)
    bad_payload[len(bad_payload) // 2] ^= 0xFF
    bad_path = tmp_path / "bad-body.json.gz"
    bad_path.write_bytes(bad_payload)
    bad_sha, bad_size = process_ptg.sha256_file(bad_path)
    store = process_ptg.PTG2ArtifactStore(tmp_path / "store")

    async def handle(request):
        if request.method == "HEAD":
            return web.Response(
                headers={
                    "Content-Length": str(len(downloaded_bytes)),
                    "ETag": '"reuse-body-test"',
                }
            )
        return web.Response(body=downloaded_bytes, headers={"Content-Length": str(len(downloaded_bytes))})

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
                "content_length": len(downloaded_bytes),
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
    assert Path(artifact.raw_path).read_bytes() == downloaded_bytes
    assert any('"status": "corrupt"' in line and "integrity check" in line for line in manifest_lines)
    assert any('"status": "available"' in line and artifact.raw_sha256 in line for line in manifest_lines)


def test_ptg2_range_download_assembles_artifact(monkeypatch, tmp_path):
    """Verify this PTG import regression contract."""
    artifact_bytes = (b"0123456789abcdef" * 1024 * 1024)[:3 * 1024 * 1024]
    requests = []

    async def handle(request):
        if request.method == "HEAD":
            return web.Response(
                headers={
                    "Content-Length": str(len(artifact_bytes)),
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
            chunk = artifact_bytes[start : end + 1]
            return web.Response(
                status=206,
                body=chunk,
                headers={
                    "Content-Length": str(len(chunk)),
                    "Content-Range": f"bytes {start}-{end}/{len(artifact_bytes)}",
                    "ETag": '"range-test"',
                },
            )
        return web.Response(body=artifact_bytes, headers={"Content-Length": str(len(artifact_bytes))})

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

    assert Path(artifact.raw_path).read_bytes() == artifact_bytes
    assert any(request == "bytes=0-0" for request in requests)
    assert sum(1 for request in requests if request and request != "bytes=0-0") >= 2
    assert not list((tmp_path / "partial-retained").glob("*.ranges.json"))


def test_ptg2_range_download_retries_short_chunk(monkeypatch, tmp_path):
    """Verify this PTG import regression contract."""
    artifact_bytes = (b"0123456789abcdef" * 1024 * 1024)[:3 * 1024 * 1024]
    attempts_by_range = {}

    async def handle(request):
        if request.method == "HEAD":
            return web.Response(
                headers={
                    "Content-Length": str(len(artifact_bytes)),
                    "ETag": '"range-retry-test"',
                    "Accept-Ranges": "bytes",
                }
            )
        range_header = request.headers.get("Range")
        if not range_header:
            return web.Response(body=artifact_bytes, headers={"Content-Length": str(len(artifact_bytes))})
        start_text, end_text = range_header.removeprefix("bytes=").split("-", 1)
        start = int(start_text)
        end = int(end_text)
        attempts_by_range[range_header] = attempts_by_range.get(range_header, 0) + 1
        chunk = artifact_bytes[start : end + 1]
        if range_header == "bytes=1048576-2097151" and attempts_by_range[range_header] == 1:
            chunk = chunk[: len(chunk) // 2]
        return web.Response(
            status=206,
            body=chunk,
            headers={
                "Content-Length": str(len(chunk)),
                "Content-Range": f"bytes {start}-{end}/{len(artifact_bytes)}",
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

    assert Path(artifact.raw_path).read_bytes() == artifact_bytes
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
    stats_map = {
        "provider_combo_cache_gets": 0,
        "provider_combo_cache_hits": 0,
        "provider_combo_cache_misses": 0,
        "provider_combo_cache_size": 0,
        "provider_combo_cache_limit": 1,
    }
    key_a = process_ptg._provider_combo_cache_key([2, "1", 1])
    key_b = process_ptg._provider_combo_cache_key(["1", 2])

    assert key_a == key_b
    assert process_ptg._provider_combo_cache_get(cache, key_a, stats_map) is None
    process_ptg._provider_combo_cache_put(cache, key_a, {"__hash__": 1}, stats_map, limit=1)
    assert process_ptg._provider_combo_cache_get(cache, key_b, stats_map)["__hash__"] == 1
    process_ptg._provider_combo_cache_put(cache, ("3",), {"__hash__": 3}, stats_map, limit=1)

    assert key_a not in cache
    assert stats_map["provider_combo_cache_hits"] == 1
    assert stats_map["provider_combo_cache_misses"] == 1



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


def test_ptg2_logical_identity_never_defers_plain_json(monkeypatch, tmp_path):
    raw_path = tmp_path / "rates.json"
    expected = b'{"in_network":[]}'
    raw_path.write_bytes(expected)
    monkeypatch.setenv(ptg_artifact_streams.PTG2_DEFER_LOGICAL_HASH_BYTES_ENV, "1")

    logical = process_ptg.logical_artifact_identity(
        raw_path,
        raw_sha256=process_ptg.sha256_bytes(expected),
        raw_byte_count=len(expected),
        allow_deferred=True,
    )

    assert logical.compression is None
    assert logical.logical_hash_deferred is False
    assert logical.logical_sha256 == process_ptg.sha256_bytes(expected)


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
    created_list = []

    async def fake_status(_statement):
        return None

    async def fake_all(_statement, **params):
        return [
            {"table_name": table_name}
            for table_name in params["table_names"]
        ]

    async def fake_create_table(table, **_kwargs):
        created_list.append(table.name)

    monkeypatch.setattr(process_ptg.db, "status", fake_status)
    monkeypatch.setattr(process_ptg.db, "all", fake_all)
    monkeypatch.setattr(process_ptg.db, "create_table", fake_create_table)

    asyncio.run(process_ptg.ensure_ptg2_tables())

    assert "ptg2_import_run" in created_list
    assert "ptg2_current_snapshot" in created_list
    assert "ptg2_source_file_version" in created_list
    expected_set = {cls.__table__.name for cls in process_ptg.PTG2_MODEL_CLASSES}
    assert expected_set.issubset(set(created_list))


def test_runtime_setup_ignores_v3_migrations():
    runtime_owned_set = {
        model.__table__.name for model in process_ptg.PTG2_MODEL_CLASSES
    }

    assert not {
        table_name for table_name in runtime_owned_set if table_name.startswith("ptg2_v3_")
    }


def test_ptg2_ensure_tables_requires_v3_migration_before_runtime_ddl(monkeypatch):
    created_list = []

    async def fake_status(_statement):
        return None

    async def fake_all(_statement, **params):
        return [
            {"table_name": table_name}
            for table_name in params["table_names"]
            if table_name != "ptg2_v3_candidate_audit_attestation"
        ]

    async def fake_create_table(table, **_kwargs):
        created_list.append(table.name)

    monkeypatch.setattr(process_ptg.db, "status", fake_status)
    monkeypatch.setattr(process_ptg.db, "all", fake_all)
    monkeypatch.setattr(process_ptg.db, "create_table", fake_create_table)

    with pytest.raises(
        RuntimeError,
        match="ptg2_v3_candidate_audit_attestation.*alembic upgrade head",
    ):
        asyncio.run(process_ptg.ensure_ptg2_tables())

    assert created_list == []


def test_ptg2_ensure_tables_requires_allowed_amount_migration(monkeypatch):
    created_list = []

    async def fake_status(_statement):
        return None

    async def fake_all(_statement, **params):
        return [
            {"table_name": table_name}
            for table_name in params["table_names"]
            if table_name != "ptg2_allowed_amount_plan"
        ]

    async def fake_create_table(table, **_kwargs):
        created_list.append(table.name)

    monkeypatch.setattr(process_ptg.db, "status", fake_status)
    monkeypatch.setattr(process_ptg.db, "all", fake_all)
    monkeypatch.setattr(process_ptg.db, "create_table", fake_create_table)

    with pytest.raises(
        RuntimeError,
        match="ptg2_allowed_amount_plan.*alembic upgrade head",
    ):
        asyncio.run(process_ptg.ensure_ptg2_tables())

    assert created_list == []


def test_ptg2_ensure_tables_fails_fast_on_create_error(monkeypatch):
    async def fake_status(_statement):
        return None

    async def fake_all(_statement, **params):
        return [
            {"table_name": table_name}
            for table_name in params["table_names"]
        ]

    async def fake_create_table(table, **_kwargs):
        if table.name == "ptg2_snapshot":
            raise RuntimeError("no permission")

    monkeypatch.setattr(process_ptg.db, "status", fake_status)
    monkeypatch.setattr(process_ptg.db, "all", fake_all)
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
            False,
            coverage_scope_id="a" * 64,
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

    monkeypatch.setattr(process_ptg, "ptg2_temp_parent", lambda: tmp_path)
    monkeypatch.setattr(process_ptg, "materialize_json_source", fake_materialize)
    monkeypatch.setattr(process_ptg, "_extract_metadata_fields", AsyncMock(return_value={}))
    monkeypatch.setattr(process_ptg, "_push_ptg2_objects", AsyncMock())
    monkeypatch.setattr(process_ptg, "_record_source_version", fake_source_version)
    monkeypatch.setattr(process_ptg, "_parse_in_network_file_strict_v3", fake_parse)

    serving_result = asyncio.run(
        process_ptg._process_in_network_file(
            {"type": "in_network", "url": "https://example.test/rates.json.gz"},
            {"PTGFile": SimpleNamespace(__name__="PTGFile"), "ImportLog": "log"},
            False,
            coverage_scope_id="a" * 64,
        )
    )

    assert serving_result.success is True
    assert serving_result.skipped is True
    assert serving_result.error is None
    assert serving_result.summary["serving_rates"] == 0
    assert serving_result.summary["skipped_reason"] == "parsed zero serving rates"


def test_ptg2_downloaded_jobs_are_prefetched_concurrently(monkeypatch):
    started_list = []
    progress_contexts = []

    def fake_download(job, **_kwargs):
        started_list.append(job["url"])
        progress_contexts.append(
            ptg_live_progress.current_live_progress_context()
        )
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

    assert started_list[:2] == [
        "https://example.test/slow.json.gz",
        "https://example.test/fast.json.gz",
    ]
    assert yielded[0] == "https://example.test/fast.json.gz"
    assert yielded[1] == "https://example.test/slow.json.gz"
    assert {context["run_progress_hold_pct"] for context in progress_contexts} == {
        5.0
    }
    assert {context["file_index"] for context in progress_contexts} == {1, 2}
    assert all(
        context["overall_progress_start_pct"]
        == context["overall_progress_end_pct"]
        == 5.0
        for context in progress_contexts
    )


def test_cancel_and_wait_tasks_joins_cancelled_children():
    async def scenario() -> None:
        started = asyncio.Event()
        stopped = asyncio.Event()

        async def child() -> None:
            started.set()
            try:
                await asyncio.sleep(30)
            finally:
                await asyncio.sleep(0)
                stopped.set()

        task = asyncio.create_task(child())
        tasks = {task}
        await started.wait()

        await process_ptg._cancel_and_wait_tasks(tasks)

        assert stopped.is_set()
        assert task.done()
        assert tasks == set()

    asyncio.run(scenario())


def test_ptg2_main_processes_downloaded_files_concurrently_when_enabled(monkeypatch):
    """Verify this PTG import regression contract."""
    pushed_list = []
    concurrency_map = {"active": 0, "max_active": 0}
    processed_jobs = []
    publish_call_kwargs_by_name = {}

    async def fake_push(rows, cls, **_kwargs):
        pushed_list.extend((getattr(cls, "__name__", str(cls)), import_row) for import_row in rows)

    async def fake_toc(*_args, **_kwargs):
        return [
            {"type": "in_network", "url": "https://example.test/rates-a.json.gz"},
            {"type": "in_network", "url": "https://example.test/rates-b.json.gz"},
        ]

    async def fake_downloaded_jobs(jobs, **_kwargs):
        for job in jobs:
            yield _strict_v3_downloaded_job(job)

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
                summary={
                    "serving_rates": 1,
                    "manifest": {
                        "copy_files": {},
                        "source_trace_hash": "a" * 64,
                    },
                    **_empty_provider_identifier_quarantine_scanner_summary(),
                },
        )

    async def fake_publish(*_args, **kwargs):
        publish_call_kwargs_by_name.update(kwargs)
        return SimpleNamespace(
            snapshot_key=7,
            serving_index={
                "storage": "manifest_snapshot",
                "type": "ptg2_shared_blocks_v3",
                "shared_snapshot_key": 7,
                "rate_count": 2,
                "serving_rates": 2,
            },
            layout_reused_at_seal=False,
            stored_byte_count=128,
        )

    @asynccontextmanager
    async def fake_transaction():
        yield object()

    refresh_mock = AsyncMock(return_value={"status": "queued", "run_id": "run-refresh"})

    monkeypatch.setattr(process_ptg, "ensure_database", AsyncMock())
    monkeypatch.setattr(process_ptg, "ensure_ptg2_tables", AsyncMock())
    monkeypatch.setattr(process_ptg.db, "status", AsyncMock())
    monkeypatch.setattr(process_ptg, "_push_ptg2_objects", fake_push)
    monkeypatch.setattr(process_ptg, "_prepare_ptg_tables", AsyncMock(return_value={"ImportLog": "log"}))
    monkeypatch.setattr(process_ptg, "_create_serving_stage_table", AsyncMock(return_value="manifest_stage"))
    monkeypatch.setattr(process_ptg, "_process_table_of_contents", fake_toc)
    monkeypatch.setattr(process_ptg, "_iter_downloaded_ptg_jobs", fake_downloaded_jobs)
    monkeypatch.setattr(process_ptg, "_process_in_network_file", fake_process)
    monkeypatch.setattr(process_ptg, "flush_error_log", AsyncMock())
    monkeypatch.setattr(process_ptg.db, "transaction", fake_transaction)
    monkeypatch.setattr(
        process_ptg,
        "reserve_shared_layout",
        AsyncMock(
            return_value=SimpleNamespace(
                snapshot_key=7,
                reused=False,
                layout_manifest=None,
            )
        ),
    )
    monkeypatch.setattr(
        process_ptg,
        "_shared_v3_scanner_identity",
        lambda: {"contract_version": 1, "scanner_binary_sha256": "a" * 64},
    )
    monkeypatch.setattr(
        process_ptg,
        "_publish_shared_v3_source_dictionary",
        AsyncMock(),
    )
    monkeypatch.setattr(
        process_ptg,
        "_merge_and_copy_ptg2_manifest_files",
        AsyncMock(
            return_value={
                "enabled": True,
                "source_files_by_kind": {
                    "price_atom": 2,
                    "price_set_atom": 2,
                    "price_set_summary": 2,
                },
            }
        ),
    )
    monkeypatch.setattr(
        process_ptg,
        "publish_strict_shared_v3_layout",
        fake_publish,
    )
    _install_candidate_stage_mock(monkeypatch)
    monkeypatch.setattr(process_ptg, "_drop_ptg2_snapshot_table_names", AsyncMock())
    monkeypatch.setattr(process_ptg, "_current_source_snapshot_id", AsyncMock(return_value=None))
    monkeypatch.setattr(process_ptg, "_publish_ptg2_source_pointers", AsyncMock())
    monkeypatch.setattr(process_ptg, "_cleanup_old_ptg2_source_tables", AsyncMock())
    monkeypatch.setattr(process_ptg, "_enqueue_ptg2_auto_address_refresh_after_import", refresh_mock)
    monkeypatch.setenv(process_ptg.PTG2_FILE_PROCESS_CONCURRENCY_ENV, "2")
    monkeypatch.setattr(
        process_ptg,
        "_should_auto_activate_ptg2_candidates",
        lambda: True,
    )

    import_result = asyncio.run(
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
    assert import_result["files_processed"] == 2
    assert import_result["address_refresh"] == {"status": "queued", "run_id": "run-refresh"}
    assert (
        "empty_npi_tin_only_normalization_count"
        not in publish_call_kwargs_by_name
    )
    refresh_mock.assert_awaited_once()
    refresh_kwargs = refresh_mock.await_args.kwargs
    assert refresh_kwargs["source_key"] == "file_concurrency_test"
    assert refresh_kwargs["snapshot_id"] == import_result["snapshot_id"]
    assert refresh_kwargs["import_run_id"] == "ptg2:file_concurrency_test"
    assert refresh_kwargs["has_serving_files"] is True
    assert refresh_kwargs["source_scoped_compact"] is True
    assert refresh_kwargs["test_mode"] is False
    import_run_rows = [import_row for cls_name, import_row in pushed_list if cls_name == "PTG2ImportRun"]
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
    monkeypatch.setattr(process_ptg, "_create_serving_stage_table", create_stage)

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


def test_failed_snapshot_upsert_merges_existing_strict_v3_manifest():
    table = process_ptg.PTG2Snapshot.__table__
    statement = process_ptg.db.insert(table).values(
        {
            "snapshot_id": "snapshot-a",
            "status": process_ptg.PTG2_STATUS_FAILED,
            "manifest": {"error": "intentional failure"},
        }
    )
    update_values = process_ptg._ptg2_snapshot_conflict_update_values(
        statement,
        table,
        incoming_status=process_ptg.PTG2_STATUS_FAILED,
    )

    manifest_expression = str(update_values["manifest"])
    assert "ptg2_snapshot.manifest" in manifest_expression
    assert "excluded.manifest" in manifest_expression
    assert "||" in manifest_expression


def test_ptg2_main_cleans_complete_stage_family_when_stage_creation_fails(monkeypatch):
    pushed_entries = []
    stage_cleanup = AsyncMock()
    partially_created_tables = []

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
    monkeypatch.setattr(
        process_ptg,
        "_prepare_ptg_tables",
        AsyncMock(return_value={"ImportLog": object}),
    )
    monkeypatch.setattr(process_ptg, "_drop_ptg2_snapshot_tables_for_manifest", AsyncMock())
    monkeypatch.setattr(process_ptg, "_drop_ptg2_snapshot_table_names", stage_cleanup)

    async def create_partial_stage(stage_token):
        partially_created_tables.append(
            process_ptg._ptg2_manifest_stage_table_name(stage_token)
        )
        raise RuntimeError("stage creation failed")

    monkeypatch.setattr(
        process_ptg,
        "_create_serving_stage_table",
        AsyncMock(side_effect=create_partial_stage),
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
    assert partially_created_tables == [stage_table_names[0]]
    assert len(stage_table_names) == 4
    assert stage_table_names[1:] == process_ptg._ptg2_manifest_stage_table_names(
        stage_table_names[0]
    )[1:]
    snapshot_entries = [
        entry for cls, entry in pushed_entries if cls is process_ptg.PTG2Snapshot
    ]
    assert snapshot_entries[-1]["status"] == process_ptg.PTG2_STATUS_FAILED


def test_ptg2_main_marks_failed_when_all_discovered_jobs_fail(monkeypatch):
    pushed_list = []

    async def fake_push(rows, cls, **_kwargs):
        pushed_list.extend((getattr(cls, "__name__", str(cls)), import_row) for import_row in rows)

    async def fake_downloaded_jobs(jobs, **_kwargs):
        for job in jobs:
            yield _strict_v3_downloaded_job(job)

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
    monkeypatch.setattr(process_ptg, "_create_serving_stage_table", AsyncMock(return_value="manifest_stage"))
    monkeypatch.setattr(process_ptg, "_iter_downloaded_ptg_jobs", fake_downloaded_jobs)
    monkeypatch.setattr(process_ptg, "_process_in_network_file", fake_process)
    monkeypatch.setattr(process_ptg, "flush_error_log", AsyncMock())
    _install_strict_v3_publish_mocks(monkeypatch, serving_rates=1)
    with pytest.raises(RuntimeError, match="failed 1 of 1 attempted file"):
        asyncio.run(
            process_ptg.main(
                in_network_url="https://example.test/rates.json.gz",
                import_month="2026-04",
                import_id="state_machine_test",
                source_key="state_machine_test",
            )
        )

    import_run_rows = [import_row for cls_name, import_row in pushed_list if cls_name == "PTG2ImportRun"]
    snapshot_rows = [import_row for cls_name, import_row in pushed_list if cls_name == "PTG2Snapshot"]
    current_rows = [import_row for cls_name, import_row in pushed_list if cls_name == "PTG2CurrentSnapshot"]

    assert import_run_rows[-1]["status"] == process_ptg.PTG2_STATUS_FAILED
    assert import_run_rows[-1]["report"]["files_processed"] in {0}
    assert import_run_rows[-1]["report"]["files_failed"] in {1}
    assert "download failed" in import_run_rows[-1]["report"]["failed_files"][0]["error"]
    assert snapshot_rows[-1]["status"] == process_ptg.PTG2_STATUS_FAILED
    assert current_rows == []


def _assert_allowed_amount_parser_rows(metrics, pushed_rows_by_class):
    assert metrics["allowed_amount_provider_payments"] == 1
    assert metrics["allowed_amount_plans"] == 1
    plan_row = pushed_rows_by_class["PTG2AllowedAmountPlan"][0]
    payment_row = pushed_rows_by_class["PTG2AllowedAmountPayment"][0]
    provider_payment_row = pushed_rows_by_class[
        "PTG2AllowedAmountProviderPayment"
    ][0]
    item_row = pushed_rows_by_class["PTG2AllowedAmountItem"][0]
    assert plan_row["snapshot_id"] == "ptg2:202607:allowed-test"
    assert plan_row["source_file_version_id"] == "source-version-1"
    assert plan_row["file_id"] == 123
    assert plan_row["plan_id"] == "7862274FDC01BCC0"
    assert item_row["snapshot_id"] == "ptg2:202607:allowed-test"
    assert item_row["source_file_version_id"] == "source-version-1"
    assert payment_row["service_code"] == ["11"]
    assert payment_row["billing_code_modifier"] == ["AA", "BB"]
    assert provider_payment_row["npi"] == [1427166008]


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
                                "billing_code_modifier": [None, "AA, bb"],
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
    async def fake_push(rows, cls, **_kwargs):
        pushed_rows_by_class.setdefault(cls.__name__, []).extend(rows)
    monkeypatch.setattr(process_ptg, "_push_ptg2_objects", fake_push)
    monkeypatch.setattr(process_ptg, "flush_error_log", AsyncMock())
    metrics = asyncio.run(
        ptg_allowed_amounts._parse_allowed_amounts(
            str(allowed_path),
            123,
            "ptg2:202607:allowed-test",
            "source-version-1",
            {},
            [{"plan_id": "7862274fdc01bcc0", "plan_market_type": "group"}],
            False,
            "import_log",
            "https://example.test/allowed.json",
        )
    )
    _assert_allowed_amount_parser_rows(metrics, pushed_rows_by_class)


def test_allowed_amount_plan_rows_preserve_shared_file_plan_coverage():
    plan_rows = ptg_allowed_amounts._allowed_amount_plan_rows(
        snapshot_id="ptg2:202607:allowed-plans",
        source_file_version_id="source-version-plans",
        file_id=123,
        meta={},
        plan_info=[
            {
                "plan_name": "Example Plan Alpha",
                "plan_id": "plan-alpha",
                "plan_id_type": "hios",
                "plan_market_type": "Individual",
            },
            {
                "plan_name": "Example Plan Beta",
                "plan_id": "PLAN-BETA",
                "plan_id_type": "hios",
                "plan_market_type": "individual",
            },
        ],
    )

    assert {plan_row["plan_id"] for plan_row in plan_rows} == {
        "PLAN-ALPHA",
        "PLAN-BETA",
    }
    assert {plan_row["file_id"] for plan_row in plan_rows} == {123}
    assert {
        plan_row["plan_market_type"] for plan_row in plan_rows
    } == {"individual"}
    assert len({plan_row["plan_hash"] for plan_row in plan_rows}) == 2


def test_direct_dispatch_plan_info_preserves_multiple_logical_scopes():
    plan_info = process_ptg._direct_dispatch_plan_info(
        ["plan-alpha", "plan-beta"],
        ["Group"],
    )

    assert plan_info == [
        {"plan_id": "plan-alpha", "plan_market_type": "group"},
        {"plan_id": "plan-beta", "plan_market_type": "group"},
    ]


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

    async def fake_push(rows, cls, **_kwargs):
        pushed_rows_by_class.setdefault(cls.__name__, []).extend(rows)

    monkeypatch.setattr(process_ptg, "_push_ptg2_objects", fake_push)
    monkeypatch.setattr(process_ptg, "flush_error_log", AsyncMock())

    asyncio.run(
        ptg_allowed_amounts._parse_allowed_amounts(
            str(allowed_path),
            123,
            "ptg2:202607:allowed-network-test",
            "source-version-2",
            {"network_status": "in_network"},
            [{"plan_id": "7862274fdc01bcc0", "plan_market_type": "group"}],
            False,
            "import_log",
            "https://example.test/allowed.json",
        )
    )

    payment_row = pushed_rows_by_class["PTG2AllowedAmountPayment"][0]
    assert payment_row["network_status"] == "in_network"
    assert payment_row["network_semantics"] == (
        "in_network_historical_allowed_amounts"
    )


def _write_allowed_amount_batch_fixture(
    tmp_path: Path,
    filename: str,
    payment_rows: list[dict],
) -> Path:
    allowed_path = tmp_path / filename
    allowed_path.write_text(
        json.dumps(
            {
                "out_of_network": [
                    {
                        "name": "Office visit",
                        "billing_code_type": "CPT",
                        "billing_code": "99214",
                        "allowed_amounts": [
                            {
                                "tin": {
                                    "type": "ein",
                                    "value": "371382997",
                                },
                                "payments": payment_rows,
                            }
                        ],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    return allowed_path


class _AllowedAmountBatchOrderRecorder:
    def __init__(self) -> None:
        self.persisted_item_hashes: set[str] = set()
        self.persisted_payment_hashes: set[str] = set()
        self.payment_batch_sizes: list[int] = []
        self.provider_batch_sizes: list[int] = []

    async def push(self, object_rows, model_class, **_kwargs):
        if model_class is ptg_allowed_amounts.PTG2AllowedAmountItem:
            self.persisted_item_hashes.update(
                row_by_field["allowed_item_hash"]
                for row_by_field in object_rows
            )
        if model_class is ptg_allowed_amounts.PTG2AllowedAmountPayment:
            assert all(
                row_by_field["allowed_item_hash"]
                in self.persisted_item_hashes
                for row_by_field in object_rows
            )
            self.persisted_payment_hashes.update(
                row_by_field["payment_hash"]
                for row_by_field in object_rows
            )
            self.payment_batch_sizes.append(len(object_rows))
        if (
            model_class
            is ptg_allowed_amounts.PTG2AllowedAmountProviderPayment
        ):
            assert all(
                row_by_field["payment_hash"]
                in self.persisted_payment_hashes
                for row_by_field in object_rows
            )
            self.provider_batch_sizes.append(len(object_rows))


def test_parse_allowed_amounts_flushes_item_before_large_payment_batch(
    monkeypatch,
    tmp_path,
):
    """Persist each item before writing its bounded payment batches."""
    payment_count = 201
    payment_rows = [
        {
            "allowed_amount": payment_number,
            "providers": [],
        }
        for payment_number in range(payment_count)
    ]
    allowed_path = _write_allowed_amount_batch_fixture(
        tmp_path,
        "allowed-many-payments.json",
        payment_rows,
    )

    batch_recorder = _AllowedAmountBatchOrderRecorder()
    monkeypatch.setattr(
        process_ptg,
        "_push_ptg2_objects",
        batch_recorder.push,
    )
    monkeypatch.setattr(process_ptg, "flush_error_log", AsyncMock())

    metrics = asyncio.run(
        ptg_allowed_amounts._parse_allowed_amounts(
            str(allowed_path),
            123,
            "ptg2:202607:allowed-many-payments",
            "source-version-many-payments",
            {},
            None,
            False,
            "import_log",
            "https://example.test/allowed-many-payments.json",
        )
    )

    assert metrics["allowed_amount_payments"] == payment_count
    assert sum(batch_recorder.payment_batch_sizes) == payment_count
    assert max(batch_recorder.payment_batch_sizes) <= (
        ptg_allowed_amounts.ALLOWED_AMOUNT_PAYMENT_BATCH_SIZE
    )


def test_parse_allowed_amounts_flushes_payment_before_large_provider_batch(
    monkeypatch,
    tmp_path,
):
    """Persist each payment before writing bounded provider batches."""
    provider_count = 201
    provider_rows = [
        {
            "billed_charge": provider_number,
            "npi": [1000000000 + provider_number],
        }
        for provider_number in range(provider_count)
    ]
    allowed_path = _write_allowed_amount_batch_fixture(
        tmp_path,
        "allowed-many-provider-groups.json",
        [
            {
                "allowed_amount": 133,
                "providers": provider_rows,
            }
        ],
    )

    batch_recorder = _AllowedAmountBatchOrderRecorder()
    monkeypatch.setattr(
        process_ptg,
        "_push_ptg2_objects",
        batch_recorder.push,
    )
    monkeypatch.setattr(process_ptg, "flush_error_log", AsyncMock())

    metrics = asyncio.run(
        ptg_allowed_amounts._parse_allowed_amounts(
            str(allowed_path),
            123,
            "ptg2:202607:allowed-many-providers",
            "source-version-many-providers",
            {},
            None,
            False,
            "import_log",
            "https://example.test/allowed-many-providers.json",
        )
    )

    assert metrics["allowed_amount_provider_payments"] == provider_count
    assert sum(batch_recorder.provider_batch_sizes) == provider_count
    assert max(batch_recorder.provider_batch_sizes) <= (
        ptg_allowed_amounts.ALLOWED_AMOUNT_PROVIDER_PAYMENT_BATCH_SIZE
    )


def _build_allowed_only_toc_jobs() -> list[dict]:
    return [
        {
            "type": "allowed_amounts",
            "url": "https://example.test/allowed-amounts.json.gz",
            "plan_info": [
                {
                    "plan_id": "TESTPLAN001",
                    "plan_id_type": "ein",
                    "plan_market_type": "group",
                }
            ],
        }
    ]


def _build_allowed_amount_file_result(
    job_by_field: dict,
    *_args,
    **_kwargs,
) -> process_ptg.PTG2FileProcessResult:
    return process_ptg.PTG2FileProcessResult(
        "allowed_amounts",
        job_by_field["url"],
        True,
        file_id=123,
        summary={
            "engine_source_identity_hash": "a" * 64,
            "engine_source_file_version_id": "source-version-allowed",
            "allowed_amount_plans": 1,
            "allowed_amount_items": 1,
            "allowed_amount_blocks": 1,
            "allowed_amount_payments": 1,
            "allowed_amount_provider_payments": 1,
            "allowed_amount_npi_references": 1,
            "allowed_amount_unique_tins": 1,
            "allowed_amount_evidence": True,
        },
    )


def _build_allowed_only_push_recorder(
    pushed_rows: list[tuple[str, dict]],
):
    async def fake_push(object_rows, model_class, **_kwargs):
        class_name = getattr(model_class, "__name__", str(model_class))
        pushed_rows.extend(
            (class_name, copy.deepcopy(row_by_field))
            for row_by_field in object_rows
        )
        first_row_by_field = object_rows[0]
        if model_class is process_ptg.PTG2Snapshot:
            if first_row_by_field["status"] == process_ptg.PTG2_STATUS_BUILDING:
                return {
                    **first_row_by_field,
                    "snapshot_claim_status": "acquired",
                }
            return dict(first_row_by_field)
        return None

    return fake_push


async def _iter_allowed_only_downloaded_jobs(selected_jobs, **_kwargs):
    for job_by_field in selected_jobs:
        yield _strict_v3_downloaded_job(job_by_field)


def _install_allowed_only_import_mocks(monkeypatch, pushed_rows) -> None:
    monkeypatch.setattr(process_ptg, "ensure_database", AsyncMock())
    monkeypatch.setattr(process_ptg, "ensure_ptg2_tables", AsyncMock())
    monkeypatch.setattr(
        process_ptg,
        "_current_source_snapshot_id",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        process_ptg,
        "_push_ptg2_objects",
        _build_allowed_only_push_recorder(pushed_rows),
    )
    monkeypatch.setattr(
        process_ptg,
        "_prepare_ptg_tables",
        AsyncMock(return_value={"ImportLog": object, "PTGFile": object}),
    )
    monkeypatch.setattr(
        process_ptg,
        "_process_table_of_contents",
        AsyncMock(return_value=_build_allowed_only_toc_jobs()),
    )
    monkeypatch.setattr(
        process_ptg,
        "_iter_downloaded_ptg_jobs",
        _iter_allowed_only_downloaded_jobs,
    )
    monkeypatch.setattr(
        process_ptg,
        "_process_allowed_amounts_file",
        AsyncMock(side_effect=_build_allowed_amount_file_result),
    )
    monkeypatch.setattr(
        process_ptg,
        "_delete_allowed_snapshot_rows",
        AsyncMock(),
    )


def _install_allowed_only_publish_mocks(
    monkeypatch,
    manifest_stage,
    publish_source_pointers,
    publish_allowed_pointer,
) -> None:
    monkeypatch.setattr(
        process_ptg,
        "_create_serving_stage_table",
        manifest_stage,
    )
    monkeypatch.setattr(
        process_ptg,
        "_publish_ptg2_source_pointers",
        publish_source_pointers,
    )
    monkeypatch.setattr(
        process_ptg,
        "_publish_allowed_current_pointer",
        publish_allowed_pointer,
    )


def _install_allowed_only_main_mocks(monkeypatch):
    """Install the common allowed-only lifecycle collaborators."""
    pushed_rows: list[tuple[str, dict]] = []
    manifest_stage = AsyncMock(return_value="unexpected-stage")
    publish_source_pointers = AsyncMock()
    publish_allowed_pointer = AsyncMock(
        return_value={
            "status": "promoted",
            "source_key": "example_allowed_source_allowed_amounts",
        }
    )
    _install_allowed_only_import_mocks(monkeypatch, pushed_rows)
    _install_allowed_only_publish_mocks(
        monkeypatch,
        manifest_stage,
        publish_source_pointers,
        publish_allowed_pointer,
    )
    return (
        pushed_rows,
        manifest_stage,
        publish_source_pointers,
        publish_allowed_pointer,
    )


def test_ptg2_main_publishes_allowed_amount_only_snapshot(monkeypatch):
    (
        pushed_rows,
        manifest_stage,
        publish_source_pointers,
        publish_allowed_pointer,
    ) = (
        _install_allowed_only_main_mocks(monkeypatch)
    )
    import_result_by_field = asyncio.run(
        process_ptg.main(
            toc_urls=["https://example.test/index.json"],
            import_month="2026-07",
            import_id="allowed_amount_only",
            source_key="example_allowed_source",
        )
    )

    snapshot_rows = [
        row_by_field for class_name, row_by_field in pushed_rows
        if class_name == "PTG2Snapshot"
    ]
    final_snapshot = snapshot_rows[-1]
    import_run_rows = [
        row_by_field for class_name, row_by_field in pushed_rows
        if class_name == "PTG2ImportRun"
    ]
    final_report = import_run_rows[-1]["report"]

    assert import_result_by_field["status"] == "succeeded"
    assert import_result_by_field["publish_status"] == "published_allowed_amounts"
    assert import_result_by_field["snapshot_status"] == process_ptg.PTG2_STATUS_PUBLISHED
    assert import_result_by_field["activation_status"] == "not_applicable"
    assert import_result_by_field["serving_rates"] == 0
    assert import_result_by_field["allowed_amount_evidence"] is True
    assert [row_by_field["status"] for row_by_field in snapshot_rows] == [
        process_ptg.PTG2_STATUS_BUILDING,
        process_ptg.PTG2_STATUS_PUBLISHED,
    ]
    assert final_snapshot["manifest"]["source_key"] == "example_allowed_source"
    assert final_snapshot["manifest"]["allowed_amount_index"]["contract"] == (
        ptg_allowed_amounts.PTG2_ALLOWED_AMOUNT_CONTRACT
    )
    assert final_snapshot["manifest"]["allowed_amount_index"]["tables"] == {
        "plans": "mrf.ptg2_allowed_amount_plan",
        "items": "mrf.ptg2_allowed_amount_item",
        "payments": "mrf.ptg2_allowed_amount_payment",
        "provider_payments": "mrf.ptg2_allowed_amount_provider_payment",
    }
    assert final_snapshot["manifest"]["allowed_amount_index"][
        "current_source_key"
    ] == "example_allowed_source_allowed_amounts"
    assert final_report["allowed_amount_plans"] == 1
    assert final_report["allowed_amount_provider_payments"] == 1
    assert final_report["serving_rates"] == 0
    manifest_stage.assert_not_awaited()
    publish_source_pointers.assert_not_awaited()
    publish_allowed_pointer.assert_awaited_once()


def test_ptg2_allowed_current_pointer_replaces_previous_snapshot(monkeypatch):
    (
        pushed_rows,
        _manifest_stage,
        _publish_source_pointers,
        publish_allowed_pointer,
    ) = _install_allowed_only_main_mocks(monkeypatch)
    monkeypatch.setattr(
        process_ptg,
        "_current_allowed_snapshot_id",
        AsyncMock(return_value="ptg2:202606:old-allowed"),
    )

    import_result_by_field = asyncio.run(
        process_ptg.main(
            toc_urls=["https://example.test/index.json"],
            import_month="2026-07",
            import_id="allowed_amount_replacement",
            source_key="example_allowed_source",
        )
    )

    final_snapshot = [
        row_by_field
        for class_name, row_by_field in pushed_rows
        if class_name == "PTG2Snapshot"
    ][-1]
    allowed_index = final_snapshot["manifest"]["allowed_amount_index"]
    assert final_snapshot["previous_snapshot_id"] == "ptg2:202606:old-allowed"
    assert allowed_index["previous_snapshot_id"] == "ptg2:202606:old-allowed"
    publish_allowed_pointer.assert_awaited_once_with(
        source_key="example_allowed_source",
        snapshot_id=import_result_by_field["snapshot_id"],
        previous_snapshot_id="ptg2:202606:old-allowed",
        import_month=datetime.date(2026, 7, 1),
        updated_at=final_snapshot["published_at"],
    )


def _mixed_partial_allowed_jobs() -> list[dict]:
    return [
        {
            "type": "in_network",
            "url": "https://example.test/in-network.json.gz",
        },
        {
            "type": "allowed_amounts",
            "url": "https://example.test/allowed-ok.json.gz",
        },
        {
            "type": "allowed_amounts",
            "url": "https://example.test/allowed-failed.json.gz",
        },
    ]


def _partial_allowed_file_result(
    job_by_field: dict,
    *_args,
    **_kwargs,
):
    if "failed" in job_by_field["url"]:
        return process_ptg.PTG2FileProcessResult(
            "allowed_amounts",
            job_by_field["url"],
            False,
            error="invalid allowed evidence",
        )
    return _build_allowed_amount_file_result(job_by_field)


def _install_failed_import_cleanup_mocks(monkeypatch) -> None:
    for helper_name in (
        "_drop_ptg2_snapshot_tables_for_manifest",
        "_drop_ptg2_snapshot_table_names",
        "delete_ptg2_artifacts_for_snapshot",
        "delete_unpublished_snapshot_sources",
    ):
        monkeypatch.setattr(process_ptg, helper_name, AsyncMock())


def test_ptg2_mixed_import_fails_before_negotiated_stage_on_partial_evidence(
    monkeypatch,
):
    """Reject partial allowed evidence before negotiated staging begins."""

    (
        pushed_rows,
        manifest_stage,
        _publish_source_pointers,
        publish_allowed_pointer,
    ) = _install_allowed_only_main_mocks(monkeypatch)
    monkeypatch.setattr(
        process_ptg,
        "_process_table_of_contents",
        AsyncMock(return_value=_mixed_partial_allowed_jobs()),
    )
    monkeypatch.setattr(
        process_ptg,
        "_process_allowed_amounts_file",
        AsyncMock(side_effect=_partial_allowed_file_result),
    )
    _install_failed_import_cleanup_mocks(monkeypatch)

    with pytest.raises(RuntimeError, match="failed 1 of 2 attempted"):
        asyncio.run(
            process_ptg.main(
                toc_urls=["https://example.test/index.json"],
                import_month="2026-07",
                import_id="mixed_partial_allowed",
                source_key="example_allowed_source",
            )
        )

    snapshot_rows = [
        row_by_field
        for class_name, row_by_field in pushed_rows
        if class_name == "PTG2Snapshot"
    ]
    assert snapshot_rows[-1]["status"] == process_ptg.PTG2_STATUS_FAILED
    manifest_stage.assert_not_awaited()
    publish_allowed_pointer.assert_not_awaited()


def test_ptg2_main_blocks_partial_publish_by_default(monkeypatch):
    """Mixed evidence succeeds, then one failed negotiated file blocks publish."""
    pushed_list = []
    process_allowed_lane = AsyncMock(
        return_value=[
            {
                "source_type": "allowed_amounts",
                "source_url": "https://example.test/allowed.json.gz",
                "success": True,
                "summary": {
                    "allowed_amount_payments": 1,
                    "allowed_amount_provider_payments": 1,
                },
            }
        ]
    )

    async def fake_push(rows, cls, **_kwargs):
        pushed_list.extend((getattr(cls, "__name__", str(cls)), import_row) for import_row in rows)

    async def fake_downloaded_jobs(jobs, **_kwargs):
        for job in jobs:
            yield _strict_v3_downloaded_job(job)

    async def fake_toc(*_args, **_kwargs):
        return [
            {
                "type": "allowed_amounts",
                "url": "https://example.test/allowed.json.gz",
            },
            {"type": "in_network", "url": "https://example.test/rates-ok.json.gz"},
            {"type": "in_network", "url": "https://example.test/rates-failed.json.gz"},
        ]

    async def fake_in_network(job, *_args, **_kwargs):
        if "failed" in job["url"]:
            return process_ptg.PTG2FileProcessResult(
                "in_network",
                job["url"],
                False,
                error="download failed",
            )
        return process_ptg.PTG2FileProcessResult(
            "in_network",
            job["url"],
            True,
            file_id=123,
            summary={"serving_rates": 1, "manifest": {"copy_files": {}}},
        )

    monkeypatch.setattr(process_ptg, "ensure_database", AsyncMock())
    monkeypatch.setattr(process_ptg, "ensure_ptg2_tables", AsyncMock())
    monkeypatch.setattr(process_ptg, "_current_source_snapshot_id", AsyncMock(return_value=None))
    monkeypatch.setattr(process_ptg, "_push_ptg2_objects", fake_push)
    monkeypatch.setattr(process_ptg, "_prepare_ptg_tables", AsyncMock(return_value={"ImportLog": "log"}))
    monkeypatch.setattr(process_ptg, "_create_serving_stage_table", AsyncMock(return_value="manifest_stage"))
    monkeypatch.setattr(process_ptg, "_process_table_of_contents", fake_toc)
    monkeypatch.setattr(
        process_ptg,
        "_process_allowed_snapshot_files",
        process_allowed_lane,
    )
    monkeypatch.setattr(process_ptg, "_iter_downloaded_ptg_jobs", fake_downloaded_jobs)
    monkeypatch.setattr(process_ptg, "_process_in_network_file", fake_in_network)
    monkeypatch.setattr(process_ptg, "flush_error_log", AsyncMock())
    monkeypatch.setattr(process_ptg, "_drop_ptg2_snapshot_tables_for_manifest", AsyncMock())
    monkeypatch.setattr(process_ptg, "_drop_ptg2_snapshot_table_names", AsyncMock())
    monkeypatch.setattr(process_ptg, "delete_ptg2_artifacts_for_snapshot", AsyncMock())
    _install_strict_v3_publish_mocks(monkeypatch, serving_rates=1)

    with pytest.raises(RuntimeError, match="failed 1 of 2 attempted"):
        asyncio.run(
            process_ptg.main(
                toc_urls=["https://example.test/index.json"],
                import_month="2026-04",
                import_id="partial_state_machine_test",
                source_key="partial_state_machine_test",
            )
        )

    import_run_rows = [import_row for cls_name, import_row in pushed_list if cls_name == "PTG2ImportRun"]
    current_rows = [import_row for cls_name, import_row in pushed_list if cls_name == "PTG2CurrentSnapshot"]

    assert import_run_rows[-1]["status"] == process_ptg.PTG2_STATUS_FAILED
    assert "failed 1 of 2 attempted" in import_run_rows[-1]["error"]
    assert import_run_rows[-1]["report"]["files_processed"] in {1}
    assert import_run_rows[-1]["report"]["files_failed"] in {1}
    assert current_rows == []
    process_allowed_lane.assert_awaited_once()


def test_ptg2_main_marks_failed_when_toc_download_fails(monkeypatch):
    pushed_list = []

    async def fake_push(rows, cls, **_kwargs):
        pushed_list.extend((getattr(cls, "__name__", str(cls)), import_row) for import_row in rows)

    async def fake_toc(*_args, **_kwargs):
        raise RuntimeError("409 public access denied")

    monkeypatch.setattr(process_ptg, "ensure_database", AsyncMock())
    monkeypatch.setattr(process_ptg, "ensure_ptg2_tables", AsyncMock())
    monkeypatch.setattr(process_ptg, "_current_source_snapshot_id", AsyncMock(return_value=None))
    monkeypatch.setattr(process_ptg, "_push_ptg2_objects", fake_push)
    monkeypatch.setattr(process_ptg, "_prepare_ptg_tables", AsyncMock(return_value={"ImportLog": "log"}))
    monkeypatch.setattr(process_ptg, "_create_serving_stage_table", AsyncMock(return_value="manifest_stage"))
    monkeypatch.setattr(process_ptg, "_process_table_of_contents", fake_toc)
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

    import_run_rows = [import_row for cls_name, import_row in pushed_list if cls_name == "PTG2ImportRun"]
    snapshot_rows = [import_row for cls_name, import_row in pushed_list if cls_name == "PTG2Snapshot"]
    current_rows = [import_row for cls_name, import_row in pushed_list if cls_name == "PTG2CurrentSnapshot"]

    assert import_run_rows[-1]["status"] == process_ptg.PTG2_STATUS_FAILED
    assert import_run_rows[-1]["report"]["jobs_discovered"] in {0}
    assert import_run_rows[-1]["report"]["toc_failures"][0]["error"] == "409 public access denied"
    assert snapshot_rows[-1]["status"] == process_ptg.PTG2_STATUS_FAILED
    assert current_rows == []


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




def _strict_v3_scanner_frame_stream(
    *,
    summary: dict | None = None,
    factor_mode: bool | None = None,
) -> bytes:
    scanner_config_map = {
        "snapshot_arch": "postgres_binary_v3",
        "storage_generation": "shared_blocks_v3",
        "serving_row_semantics": "source_multiset_v1",
        "serving_run_format": "ptg2_v3_serving_run",
        "serving_run_version": 1,
    }
    scanner_summary_map = dict(summary or {})
    if factor_mode is not None:
        scanner_config_map["factor_mode"] = factor_mode
        scanner_config_map["provider_graph_v4_factor_mode"] = factor_mode
        scanner_summary_map["factor_mode"] = factor_mode
        scanner_summary_map["provider_graph_v4_factor_mode"] = factor_mode
        if factor_mode:
            scanner_summary_map["empty_npi_tin_only_normalization"] = (
                _v4_empty_npi_normalization_payload(0)
            )
    framed_payloads = (
        ("scanner_config", scanner_config_map),
        ("scanner_summary", scanner_summary_map),
    )
    stream = bytearray()
    for kind, frame_payload_by_field in framed_payloads:
        encoded = json.dumps(
            frame_payload_by_field,
            separators=(",", ":"),
        ).encode("utf-8")
        stream.extend(f"{kind}\t{len(encoded)}\n".encode("ascii"))
        stream.extend(encoded)
        stream.extend(b"\n")
    return bytes(stream)


def test_ptg2_rust_compact_uses_bounded_event_queue_default(monkeypatch, tmp_path):
    captured_env_map = {}

    class FakeProcess:
        stdout = io.BytesIO(_strict_v3_scanner_frame_stream())
        stderr = None

        def poll(self):
            return 0

        def wait(self, timeout=None):
            return 0

    def fake_popen(_args, stdout, stderr, env):
        captured_env_map.update(env)
        return FakeProcess()

    monkeypatch.delenv(process_ptg.PTG2_RUST_EVENT_QUEUE_ENV, raising=False)
    monkeypatch.setattr(ptg_rust_scanner, "_ptg2_rust_scanner_binary", lambda: tmp_path / "ptg2_scanner")
    monkeypatch.setattr(ptg_rust_scanner.subprocess, "Popen", fake_popen)

    list(
        process_ptg._iter_compact_serving_records_rust(
            tmp_path / "rates.json.gz",
            raw_source_sha256="a" * 64,
            snapshot_id="snap",
            plan_id="plan",
            coverage_scope_id="c" * 64,
            plan_month_id="month",
            source_trace_set_hash="trace",
            v3_serving_run_directory=tmp_path / "v3-runs",
            manifest_only=True,
        )
    )

    assert captured_env_map[process_ptg.PTG2_RUST_EVENT_QUEUE_ENV] == "128"
    assert captured_env_map["HLTHPRT_PTG2_V3_SERVING_RUN_DIR"].endswith("v3-runs")
    assert (
        captured_env_map["HLTHPRT_PTG2_MANIFEST_SPILL_DIR"]
        == captured_env_map["HLTHPRT_PTG2_V3_SERVING_RUN_DIR"]
    )
    assert "HLTHPRT_PTG2_MANIFEST_PROVIDER_FORWARD_SIDECAR_PATH" not in captured_env_map
    assert captured_env_map["HLTHPRT_PTG2_MANIFEST_ONLY"] == "true"


def test_v4_flag_enables_rust_factor_mode(
    monkeypatch,
    tmp_path,
):
    captured_env_map = {}

    class FakeProcess:
        stdout = io.BytesIO(_strict_v3_scanner_frame_stream(factor_mode=True))
        stderr = None

        def poll(self):
            return 0

        def wait(self, timeout=None):
            return 0

    def fake_popen(_args, stdout, stderr, env):
        captured_env_map.update(env)
        return FakeProcess()

    monkeypatch.setenv("HLTHPRT_PTG2_PROVIDER_GRAPH_V4", "1")
    monkeypatch.setenv("HLTHPRT_PTG2_PROVIDER_GRAPH_V4_FACTORS", "0")
    monkeypatch.setattr(
        ptg_rust_scanner,
        "_ptg2_rust_scanner_binary",
        lambda: tmp_path / "ptg2_scanner",
    )
    monkeypatch.setattr(ptg_rust_scanner.subprocess, "Popen", fake_popen)

    framed_records = list(
        process_ptg._iter_compact_serving_records_rust(
            tmp_path / "rates.json.gz",
            raw_source_sha256="a" * 64,
            snapshot_id="snap",
            plan_id="plan",
            coverage_scope_id="c" * 64,
            plan_month_id="month",
            source_trace_set_hash="trace",
            v3_serving_run_directory=tmp_path / "v3-runs",
            manifest_provider_set_component_sidecar_path=tmp_path
            / "set-component.ptg2sc",
            manifest_provider_component_group_sidecar_path=tmp_path
            / "component-group.ptg2sc",
            manifest_only=True,
        )
    )

    assert captured_env_map["HLTHPRT_PTG2_PROVIDER_GRAPH_V4"] == "true"
    assert captured_env_map["HLTHPRT_PTG2_PROVIDER_GRAPH_V4_FACTORS"] == "true"
    assert framed_records[0][1]["factor_mode"] is True
    assert framed_records[1][1]["factor_mode"] is True


@pytest.mark.parametrize("configured_factor_paths", [(False, False), (True, False)])
def test_ptg2_rust_compact_rejects_incomplete_v4_factor_paths(
    configured_factor_paths,
    monkeypatch,
    tmp_path,
):
    has_set_path, has_component_path = configured_factor_paths
    monkeypatch.setenv("HLTHPRT_PTG2_PROVIDER_GRAPH_V4", "1")
    monkeypatch.setattr(
        ptg_rust_scanner,
        "_ptg2_rust_scanner_binary",
        lambda: tmp_path / "ptg2_scanner",
    )
    monkeypatch.setattr(
        ptg_rust_scanner.subprocess,
        "Popen",
        lambda *args, **kwargs: pytest.fail("scanner must not start"),
    )

    with pytest.raises(RuntimeError, match="require.*both"):
        list(
            process_ptg._iter_compact_serving_records_rust(
                tmp_path / "rates.json.gz",
                raw_source_sha256="a" * 64,
                snapshot_id="snap",
                plan_id="plan",
                coverage_scope_id="c" * 64,
                plan_month_id="month",
                source_trace_set_hash="trace",
                v3_serving_run_directory=tmp_path / "v3-runs",
                manifest_provider_set_component_sidecar_path=(
                    tmp_path / "set-component.ptg2sc" if has_set_path else None
                ),
                manifest_provider_component_group_sidecar_path=(
                    tmp_path / "component-group.ptg2sc"
                    if has_component_path
                    else None
                ),
                manifest_only=True,
            )
        )


def test_v4_factor_frames_are_required_when_main_mode_is_expected():
    inactive_factor_map = {
        "factor_mode": False,
        "provider_graph_v4_factor_mode": False,
    }
    scanner_config_map = {
        "snapshot_arch": "postgres_binary_v3",
        "storage_generation": "shared_blocks_v3",
        "serving_row_semantics": "source_multiset_v1",
        "serving_run_format": "ptg2_v3_serving_run",
        "serving_run_version": 1,
        **inactive_factor_map,
    }
    with pytest.raises(RuntimeError, match="did not activate required V4 factor mode"):
        ptg_rust_scanner._validate_v3_scanner_config(
            scanner_config_map,
            expect_factor_mode=True,
        )
    with pytest.raises(RuntimeError, match="did not confirm required V4 factor mode"):
        ptg_rust_scanner._validate_v3_scanner_summary(
            inactive_factor_map,
            expect_factor_mode=True,
        )


def test_v4_factor_summary_rejects_tampered_empty_npi_audit():
    normalization_audit = _v4_empty_npi_normalization_payload(2)
    normalization_audit["occurrence_count"] = 3

    with pytest.raises(RuntimeError, match="digest changed"):
        ptg_rust_scanner._validate_v3_scanner_summary(
            {
                "factor_mode": True,
                "provider_graph_v4_factor_mode": True,
                "empty_npi_tin_only_normalization": normalization_audit,
            },
            expect_factor_mode=True,
        )


def test_v3_scanner_summary_rejects_v4_only_empty_npi_audit():
    with pytest.raises(RuntimeError, match="V4-only empty-NPI evidence"):
        ptg_rust_scanner._validate_v3_scanner_summary(
            {
                "empty_npi_tin_only_normalization": (
                    _v4_empty_npi_normalization_payload(0)
                ),
            },
        )


def test_ptg2_rust_compact_retries_short_pipe_reads(monkeypatch, tmp_path):
    class ShortReader(io.BytesIO):
        def read(self, size=-1):
            return super().read(min(size, 2))

    class FakeProcess:
        stdout = ShortReader(_strict_v3_scanner_frame_stream(summary={"x": 1}))
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
            raw_source_sha256="a" * 64,
            snapshot_id="snap",
            plan_id="plan",
            coverage_scope_id="c" * 64,
            plan_month_id="month",
            source_trace_set_hash="trace",
            v3_serving_run_directory=tmp_path / "v3-runs",
            manifest_only=True,
        )
    )

    assert framed_records == [
        (
            "scanner_config",
            {
                "snapshot_arch": "postgres_binary_v3",
                "storage_generation": "shared_blocks_v3",
                "serving_row_semantics": "source_multiset_v1",
                "serving_run_format": "ptg2_v3_serving_run",
                "serving_run_version": 1,
            },
        ),
        (
            "scanner_summary",
            {
                "x": 1,
                "serving_run_partition_files": [],
                "serving_run_code_dictionary_files": [],
            },
        )
    ]


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
                raw_source_sha256="a" * 64,
                snapshot_id="snap",
                plan_id="plan",
                coverage_scope_id="c" * 64,
                plan_month_id="month",
                source_trace_set_hash="trace",
                v3_serving_run_directory=tmp_path / "v3-runs",
                manifest_only=True,
            )
        )

    message = str(error.value)
    assert "kind=scanner_summary" in message
    assert "expected_bytes=7 received_bytes=4" in message
    assert "signal 9 (SIGKILL)" in message


def test_ptg2_manifest_artifacts_skip_disabled_provider_npi_sidecar(tmp_path):
    provider_forward = tmp_path / "provider_forward.ptg2sc"
    owner_global_id = (1).to_bytes(16, "big")
    member_global_id = (2).to_bytes(16, "big")
    provider_forward.write_bytes(
        struct.pack("<8sIQQ", b"PTG2MNDS", 1, 1, 1)
        + struct.pack("<16sQI", owner_global_id, 0, 1)
        + member_global_id
        + struct.pack("<I", 0)
    )
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
    assert artifacts["provider_forward"]["owner_count"] == 1
    assert artifacts["provider_forward"]["member_count"] == 1
    assert artifacts["provider_forward"]["member_global_count"] == 1
    assert artifacts["provider_forward"]["owner_index_fence_owners"] == [
        owner_global_id.hex()
    ]


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
    assert sidecar_map["provider_forward"]["owner_count"] == 0
    assert sidecar_map["provider_forward"]["member_count"] == 0
    assert sidecar_map["provider_forward"]["source_shard_id"] == "manifest:logical-shard-a"


def test_ptg2_manifest_artifacts_combine_all_logical_source_metadata():
    artifacts = process_ptg._collect_manifest_artifacts(
        [
            {
                "summary": {
                    "manifest": {
                        "source_trace_hash": "trace-b",
                        "network_names": ["network b", "network a"],
                    }
                }
            },
            {
                "summary": {
                    "manifest": {
                        "source_trace_hash": "trace-a",
                        "network_names": ["network c", "network a"],
                    }
                }
            },
        ]
    )

    assert artifacts["source_trace_set_hash"] == process_ptg.build_source_trace_set(
        ["trace-a", "trace-b"]
    )["source_trace_set_hash"]
    assert artifacts["network_names"] == ["network a", "network b", "network c"]


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

    artifacts = await ptg_manifest_publish._store_sidecar_artifacts_in_db(
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

    artifacts = await ptg_manifest_publish._store_sidecar_artifacts_in_db(
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

    artifacts = await ptg_manifest_publish._store_sidecar_artifacts_in_db(
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
    stored = await ptg_manifest_publish._store_sidecar_artifacts_in_db(
        schema_name="mrf",
        snapshot_id="ptg2:test",
        sidecar_artifacts=combined,
    )
    manifest = ptg_manifest_publish._ptg2_manifest_artifacts_manifest(
        artifacts=base_artifacts,
        sidecar_artifacts=stored,
    )

    sidecars_map = {sidecar["name"]: sidecar for sidecar in manifest["sidecars"]}
    assert manifest["source_trace_set_hash"] == "trace-set"
    assert set(sidecars_map) == {"provider_forward", "price_forward"}
    assert sidecars_map["provider_forward"]["storage_uri"] == "db://ptg2_artifact/provider_forward"
    assert sidecars_map["price_forward"]["storage_uri"] == "db://ptg2_artifact/price_forward"
    assert [call[0] for call in calls] == [provider_path, price_path]
    assert [event["artifact_name"] for event in progress_events if event["publish_step"] == "artifact upload complete"] == [
        "provider_forward",
        "price_forward",
    ]


@pytest.mark.asyncio
async def test_ptg2_manifest_serving_sidecars_use_rust_copy_fast_path(monkeypatch, tmp_path):
    artifact_helpers = importlib.import_module("process.ptg_parts.ptg2_manifest_artifacts")
    copied_sql_list = []
    runner_calls = []
    progress_events = []

    async def fake_copy_query_to_file(sql, output_path):
        copied_sql_list.append(sql)
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

    monkeypatch.setattr(ptg_manifest_publish, "_is_rust_sidecar_enabled", lambda: True)
    monkeypatch.setattr(ptg_manifest_publish, "_ptg2_rust_scanner_binary", lambda: tmp_path / "ptg2_scanner")
    monkeypatch.setattr(ptg_manifest_publish, "_copy_ptg2_query_to_file", fake_copy_query_to_file)
    monkeypatch.setattr(ptg_manifest_publish, "_run_serving_sidecar_from_key_copy", fake_runner)
    monkeypatch.setattr(ptg_manifest_publish, "write_live_progress", lambda **payload: progress_events.append(payload))

    sidecars = await ptg_manifest_publish._write_serving_sidecars_rust(
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
    assert len(copied_sql_list) == 2
    assert not any(path.name.startswith("serving_by_code_") and path.suffix == ".copy" for path in tmp_path.iterdir())
    assert "serving sidecars export by code" in [event["publish_step"] for event in progress_events]
    assert "serving sidecars encode reverse" in [event["publish_step"] for event in progress_events]
    assert progress_events[-1]["publish_step"] == "serving sidecars complete"





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


def test_ptg2_snapshot_arch_rejects_postgres_posting_alias(monkeypatch):
    monkeypatch.setenv("HLTHPRT_PTG2_SNAPSHOT_ARCH", "posting")

    with pytest.raises(ValueError):
        ptg_config._ptg2_snapshot_arch_from_env()


def test_snapshot_arch_uses_strict_v3(monkeypatch):
    monkeypatch.delenv("HLTHPRT_PTG2_SNAPSHOT_ARCH", raising=False)

    assert ptg_config._ptg2_snapshot_arch_from_env() == "postgres_binary_v3"


def test_ptg2_automatic_candidate_activation_is_rejected(monkeypatch):
    monkeypatch.setenv("HLTHPRT_PTG2_AUTO_ACTIVATE_CANDIDATES", "true")

    with pytest.raises(ValueError, match="audited control-plane activation"):
        ptg_config._should_auto_activate_ptg2_candidates()


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

    async def has_rows_in_fake_table(_schema, table):
        return table == "ptg2_provider_group_rate_scope_snap"

    monkeypatch.setattr(ptg_manifest_publish.db, "status", fake_status)
    monkeypatch.setattr(ptg_manifest_publish, "_table_exists", is_fake_table_present)
    monkeypatch.setattr(
        ptg_manifest_publish,
        "_has_rows_in_table",
        has_rows_in_fake_table,
    )

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


def test_strict_v3_precopy_rejects_partial_price_artifacts_per_source(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setenv("HLTHPRT_PTG2_SNAPSHOT_ARCH", "postgres_binary_v3")
    successful_files = []
    all_paths = []
    for source_index, kinds in enumerate(
        (
            ("price_atom", "price_set_atom", "price_set_summary"),
            ("price_atom", "price_set_atom"),
        )
    ):
        copy_entries_by_kind = {}
        for kind in kinds:
            path = tmp_path / f"source-{source_index}-{kind}.copy"
            path.write_text("row\n", encoding="utf-8")
            all_paths.append(path)
            copy_entries_by_kind[kind] = [{"path": str(path), "row_count": 1}]
        successful_files.append(
            {
                "url": f"https://example.test/source-{source_index}.json.gz",
                "summary": {
                    "manifest": {
                        "serving_rows": 1,
                        "copy_files": copy_entries_by_kind,
                    }
                },
            }
        )

    with pytest.raises(
        RuntimeError,
        match=r"source-1\.json\.gz.*price_set_summary",
    ):
        asyncio.run(
            process_ptg._merge_and_copy_ptg2_manifest_files(
                successful_files=successful_files,
                manifest_stage_table="ptg2_manifest_stage_serving_partial",
            )
        )

    assert not any(path.exists() for path in all_paths)


def test_completed_strict_v3_file_registers_scratch_before_batch_end(
    tmp_path,
    monkeypatch,
):
    """Register completed V3 scratch artifacts before their batch exits."""

    artifact_root = tmp_path / "artifacts"
    sidecar_path = artifact_root / "serving" / "attempt" / "provider.ptg2sc"
    sidecar_path.parent.mkdir(parents=True)
    sidecar_path.write_bytes(b"sidecar")
    copy_entries_by_kind = {}
    copy_paths = []
    for kind in (
        "serving_run",
        "serving_code_dictionary",
        "source_audit_witness",
        "provider_set_metadata",
        "price_atom",
        "price_set_atom",
        "price_set_summary",
    ):
        path = tmp_path / f"{kind}.copy"
        path.write_bytes(b"scratch")
        copy_paths.append(path)
        copy_entries_by_kind[kind] = [{"path": str(path), "row_count": 1}]
    pending = process_ptg._PendingStrictV3State({}, {})
    monkeypatch.setattr(
        process_ptg,
        "resolve_ptg2_artifact_dir",
        lambda: artifact_root,
    )

    file_result = process_ptg.PTG2FileProcessResult(
        "in_network",
        "https://example.test/in-network.json.gz",
        True,
        summary={
            "manifest": {
                "copy_files": copy_entries_by_kind,
                "sidecars": [{"path": str(sidecar_path)}],
            }
        },
    )

    async def exercise_done_but_undrained_cancellation_window():
        async def finish_file():
            process_ptg._claim_strict_v3_file_scratch(pending, file_result)
            return file_result

        task = asyncio.create_task(finish_file())
        await asyncio.sleep(0)
        assert task.done()
        tasks = {task}
        await process_ptg._cancel_and_wait_tasks(tasks)
        assert tasks == set()

    asyncio.run(exercise_done_but_undrained_cancellation_window())

    assert set(pending.copy_entries_by_kind) == set(copy_entries_by_kind)
    assert pending.graph_artifacts_map["sidecars"] == [
        {"path": str(sidecar_path)}
    ]
    process_ptg._cleanup_manifest_copy_entries(pending.copy_entries_by_kind)
    process_ptg._cleanup_strict_v3_graph_artifacts(pending.graph_artifacts_map)
    assert not any(path.exists() for path in copy_paths)
    assert not sidecar_path.exists()


def test_strict_v3_annotation_failure_still_claims_completed_scratch(
    tmp_path,
    monkeypatch,
):
    copy_path = tmp_path / "provider_set_metadata.copy"
    copy_path.write_bytes(b"scratch")
    pending = process_ptg._PendingStrictV3State({}, {})
    file_result = process_ptg.PTG2FileProcessResult(
        "in_network",
        "https://example.test/in-network.json.gz",
        True,
        summary={
            "manifest": {
                "copy_files": {
                    "provider_set_metadata": [
                        {"path": str(copy_path), "row_count": 1}
                    ]
                }
            }
        },
    )

    def fail_annotation(*_args, **_kwargs):
        raise RuntimeError("source contract attachment failed")

    monkeypatch.setattr(
        process_ptg,
        "_annotate_v3_file_result_source_identity",
        fail_annotation,
    )

    with pytest.raises(RuntimeError, match="source contract attachment failed"):
        process_ptg._claim_strict_v3_file_result(
            pending,
            file_result,
            process_ptg.SharedPhysicalArtifactIdentity(
                "in_network",
                "logical_json_sha256_v1",
                "a" * 64,
            ),
            {},
        )

    assert pending.copy_entries_by_kind["provider_set_metadata"] == [
        {"path": str(copy_path), "row_count": 1}
    ]
    assert all(
        not entries
        for kind, entries in pending.copy_entries_by_kind.items()
        if kind != "provider_set_metadata"
    )
    process_ptg._cleanup_manifest_copy_entries(pending.copy_entries_by_kind)
    assert not copy_path.exists()


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


def _assert_manifest_copy_throughput_metrics(metrics_by_name, *, expected_bytes):
    """Assert persisted direct-COPY counters are populated."""
    assert metrics_by_name["input_bytes"] == expected_bytes
    assert metrics_by_name["elapsed_seconds"] >= 0
    assert metrics_by_name["rows_per_second"] is not None
    assert metrics_by_name["bytes_per_second"] is not None


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


def test_manifest_copy_entry_cleanup_removes_registered_v3_runs(tmp_path):
    run_path = tmp_path / "serving-run.ready"
    dictionary_path = tmp_path / "code-dictionary.ready"
    run_path.write_bytes(b"run")
    dictionary_path.write_bytes(b"dictionary")

    process_ptg._cleanup_manifest_copy_entries(
        {
            "serving_run": [{"path": str(run_path)}],
            "serving_code_dictionary": [{"path": str(dictionary_path)}],
        }
    )

    assert not run_path.exists()
    assert not dictionary_path.exists()


def test_source_import_lock_serializes_different_snapshots_for_one_source(monkeypatch):
    """Verify source import lock serializes different snapshots for one source."""
    lock_state_map = {"held": False}
    real_sleep = asyncio.sleep

    class _Result:
        def __init__(self, value):
            self.value = value

        def scalar(self):
            return self.value

    class _Connection:
        async def execute(self, statement, _params):
            sql = str(statement)
            if "pg_try_advisory_lock" in sql:
                if lock_state_map["held"]:
                    return _Result(False)
                lock_state_map["held"] = True
                return _Result(True)
            if "pg_advisory_unlock" in sql:
                assert lock_state_map["held"]
                lock_state_map["held"] = False
                return _Result(True)
            raise AssertionError(sql)

        async def commit(self):
            return None

    class _ConnectionContext:
        async def __aenter__(self):
            return _Connection()

        async def __aexit__(self, *_args):
            return None

    class _Engine:
        def connect(self):
            return _ConnectionContext()

    fake_db = SimpleNamespace(
        engine=_Engine(),
        text=lambda statement: statement,
    )

    async def fast_sleep(_seconds):
        await real_sleep(0.001)

    monkeypatch.setattr(process_ptg, "db", fake_db)
    monkeypatch.setenv(process_ptg.PTG2_SOURCE_IMPORT_LOCK_ENABLED_ENV, "true")
    monkeypatch.setattr(process_ptg.asyncio, "sleep", fast_sleep)
    monkeypatch.setattr(process_ptg, "write_live_progress", Mock())

    async def scenario():
        first_entered = asyncio.Event()
        release_first = asyncio.Event()
        entered_list: list[str] = []

        async def first_import():
            async with process_ptg._ptg2_source_import_lock("source-a"):
                entered_list.append("first")
                first_entered.set()
                await release_first.wait()

        async def second_import():
            await first_entered.wait()
            async with process_ptg._ptg2_source_import_lock("source-a"):
                entered_list.append("second")

        first_task = asyncio.create_task(first_import())
        second_task = asyncio.create_task(second_import())
        await first_entered.wait()
        await real_sleep(0.01)
        assert entered_list == ["first"]
        release_first.set()
        await asyncio.gather(first_task, second_task)
        assert entered_list == ["first", "second"]
        assert lock_state_map["held"] is False

    asyncio.run(scenario())


def test_ptg2_source_plan_rows_uses_strict_plan_month_rows(monkeypatch):
    calls = []

    async def fake_all(statement, **_params):
        calls.append(statement)
        assert "ptg2_v3_snapshot_plan_scope" in statement
        assert "ptg2_plan_month AS plan_month" in statement
        return [{"plan_id": "TESTPLAN001", "plan_market_type": "group"}]

    monkeypatch.setattr(ptg_source_pointers.db, "all", fake_all)
    updated_at = process_ptg._utcnow()

    plan_rows = asyncio.run(
        process_ptg._source_plan_rows(
            snapshot_id="snap",
            source_key="example_dental",
            import_month=process_ptg.normalize_import_month("2026-04"),
            previous_snapshot_id="prev",
            updated_at=updated_at,
        )
    )

    assert plan_rows == [
        {
            "plan_source_key": process_ptg._ptg2_plan_source_key(
                "TESTPLAN001",
                "group",
                process_ptg.normalize_import_month("2026-04"),
                "example_dental",
            ),
            "plan_id": "TESTPLAN001",
            "plan_market_type": "group",
            "import_month": process_ptg.normalize_import_month("2026-04"),
            "source_key": "example_dental",
            "snapshot_id": "snap",
            "previous_snapshot_id": "prev",
            "updated_at": updated_at,
        }
    ]
    assert len(calls) == 1


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


def test_ptg2_candidate_stage_binds_layout_without_mutating_live_pointers(monkeypatch):
    """Verify ptg2 candidate stage binds layout without mutating live pointers."""
    executed_list = []
    updated_at = process_ptg._utcnow()
    import_month = process_ptg.normalize_import_month("2026-04")
    base_snapshot = _build_published_snapshot_fields(import_month, updated_at)
    bind_layout = AsyncMock()

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
            sql = str(statement)
            executed_list.append((sql, params or {}))
            if "RETURNING coverage_scope_id" in sql:
                return SimpleNamespace(first=lambda: (b"s" * 32,))
            if "SELECT plan_id, plan_market_type" in sql:
                return [
                    {
                        "plan_id": "TESTPLAN001",
                        "plan_market_type": "group",
                    }
                ]
            if "SELECT status FROM staged" in sql:
                return SimpleNamespace(first=lambda: ("validated",))
            return SimpleNamespace(first=lambda: None)

        async def scalar(self, statement, params=None):
            executed_list.append((str(statement), params or {}))
            return 1

    class FakeTransaction:
        async def __aenter__(self):
            return FakeSession()

        async def __aexit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(ptg_source_pointers, "bind_snapshot_to_shared_layout", bind_layout)
    monkeypatch.setattr(ptg_source_pointers, "_source_plan_rows", fake_source_plan_rows)
    monkeypatch.setattr(ptg_source_pointers.db, "transaction", lambda: FakeTransaction())

    stage_result = asyncio.run(
        ptg_source_pointers._stage_ptg2_source_candidate(
            source_key="example_dental",
            snapshot_id="snap",
            previous_snapshot_id="prev",
            import_month=import_month,
            updated_at=updated_at,
            snapshot_attributes=base_snapshot,
            shared_snapshot_key=7,
            coverage_scope_id=b"s" * 32,
            coverage_plan_scopes=[
                {
                    "plan_id": "TESTPLAN001",
                    "plan_market_type": "group",
                }
            ],
        )
    )

    candidate = stage_result["candidate_attributes"]
    assert stage_result["status"] == "validated"
    assert candidate["status"] == "validated"
    assert candidate["published_at"] is None
    assert candidate["manifest"]["activation"] == {
        "contract": "ptg2_candidate_activation_v1",
        "state": "validated",
        "source_key": "example_dental",
        "expected_previous_snapshot_id": "prev",
    }
    bind_layout.assert_awaited_once()
    joined = "\n".join(statement for statement, _params in executed_list)
    assert "ptg2_v3_snapshot_scope" in joined
    assert "ptg2_v3_snapshot_plan_scope" in joined
    assert "UPDATE \"mrf\".ptg2_snapshot" in joined
    assert "ptg2_current_source_snapshot" not in joined
    assert "ptg2_current_plan_source" not in joined
    assert "ptg2_current_snapshot" not in joined


def test_ptg2_source_pointer_publish_updates_source_and_plan_rows_transactionally(monkeypatch):
    executed_list = []
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
            sql = str(statement)
            executed_list.append((sql, params or {}))
            if "FOR UPDATE OF snapshot" in sql:
                return SimpleNamespace(first=lambda: snapshot_by_field)

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
            snapshot_attributes=snapshot_by_field,
        )
    )

    joined = "\n".join(statement for statement, _params in executed_list)
    assert promotion_result["global_pointer"] == "reconciled"
    assert "INSERT INTO \"mrf\".ptg2_current_source_snapshot" in joined
    assert "UPDATE \"mrf\".ptg2_snapshot" in joined
    assert "updated_global_pointer" in joined
    assert "incumbent.published_at >=" in joined
    assert "DELETE FROM \"mrf\".ptg2_current_plan_source WHERE source_key = :source_key" in joined
    assert "INSERT INTO \"mrf\".ptg2_current_plan_source" in joined
    assert len(executed_list) == 7
    assert "FOR UPDATE OF snapshot" in executed_list[1][0]


def test_pointer_stays_on_plan_failure(monkeypatch):
    transaction_started_map = {"value": False}
    executed_statements = []

    async def fake_source_plan_rows(**_kwargs):
        assert transaction_started_map["value"] is True
        assert executed_statements and "pg_advisory_xact_lock" in executed_statements[0][0]
        raise RuntimeError("plan resolution failed")

    class FakeSession:
        async def execute(self, statement, params=None):
            sql = str(statement)
            executed_statements.append((sql, params or {}))
            if "FOR UPDATE OF snapshot" in sql:
                return SimpleNamespace(
                    first=lambda: {
                        "snapshot_id": "snap",
                        "status": "published",
                        "manifest": {},
                    }
                )

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
            )
        )

    assert transaction_started_map["value"] is True
    assert len(executed_statements) == 2
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
            sql = str(statement)
            executed_statements.append((sql, params or {}))
            if "FOR UPDATE OF snapshot" in sql:
                return SimpleNamespace(first=lambda: snapshot_attributes)

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
    assert len(executed_statements) == 4
    assert "pg_advisory_xact_lock" in executed_statements[0][0]
    assert "FOR UPDATE OF snapshot" in executed_statements[1][0]
    assert "UPDATE \"mrf\".ptg2_snapshot" in executed_statements[2][0]
    assert "INSERT INTO \"mrf\".ptg2_current_snapshot" in executed_statements[3][0]
    assert executed_statements[0][1] == {
        "publish_lock_key": ptg_source_pointers.PTG2_SOURCE_POINTER_GC_LOCK_KEY
    }


def test_ptg2_global_snapshot_pointer_rejects_unpublished_snapshot_after_lock(monkeypatch):
    executed_statements = []

    class FakeSession:
        async def execute(self, statement, params=None):
            sql = str(statement)
            executed_statements.append((sql, params or {}))
            if "FOR UPDATE OF snapshot" in sql:
                return SimpleNamespace(
                    first=lambda: {
                        "snapshot_id": "snap",
                        "status": "validated",
                        "manifest": {},
                    }
                )

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

    assert len(executed_statements) == 2
    assert "pg_advisory_xact_lock" in executed_statements[0][0]
    assert "FOR UPDATE OF snapshot" in executed_statements[1][0]


def test_manifest_allowlists_location_tables():
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


def _install_candidate_stage_mock(monkeypatch):
    async def stage_candidate(**kwargs):
        candidate_attributes = ptg_source_pointers.candidate_snapshot_attributes(
            kwargs["snapshot_attributes"],
            source_key=kwargs["source_key"],
            previous_snapshot_id=kwargs["previous_snapshot_id"],
        )
        return {
            "status": "validated",
            "candidate_attributes": candidate_attributes,
        }

    stage = AsyncMock(side_effect=stage_candidate)
    monkeypatch.setattr(process_ptg, "_stage_ptg2_source_candidate", stage)
    return stage


def _install_strict_v3_publish_mocks(monkeypatch, *, serving_rates: int):
    """Install a successful strict V3 publisher and return its async mock."""

    monkeypatch.setattr(
        process_ptg,
        "_should_auto_activate_ptg2_candidates",
        lambda: True,
    )
    publication = SimpleNamespace(
        snapshot_key=7,
        serving_index={
            "storage": "manifest_snapshot",
            "type": "ptg2_shared_blocks_v3",
            "shared_snapshot_key": 7,
            "rate_count": serving_rates,
            "serving_rates": serving_rates,
        },
        layout_reused_at_seal=False,
        stored_byte_count=128,
    )
    publish = AsyncMock(return_value=publication)

    @asynccontextmanager
    async def fake_transaction():
        yield object()

    monkeypatch.setattr(process_ptg.db, "transaction", fake_transaction)
    monkeypatch.setattr(
        process_ptg,
        "reserve_shared_layout",
        AsyncMock(
            return_value=SimpleNamespace(
                snapshot_key=7,
                reused=False,
                layout_manifest=None,
            )
        ),
    )
    monkeypatch.setattr(
        process_ptg,
        "_shared_v3_scanner_identity",
        lambda: {"contract_version": 1, "scanner_binary_sha256": "a" * 64},
    )
    monkeypatch.setattr(
        process_ptg,
        "_merge_and_copy_ptg2_manifest_files",
        AsyncMock(
            return_value={
                "enabled": True,
                "source_files_by_kind": {
                    "price_atom": 1,
                    "price_set_atom": 1,
                    "price_set_summary": 1,
                },
            }
        ),
    )
    monkeypatch.setattr(process_ptg, "publish_strict_shared_v3_layout", publish)
    _install_candidate_stage_mock(monkeypatch)
    monkeypatch.setattr(process_ptg, "_drop_ptg2_snapshot_table_names", AsyncMock())
    monkeypatch.setattr(
        process_ptg,
        "_enqueue_ptg2_auto_address_refresh_after_import",
        AsyncMock(return_value={"status": "disabled"}),
    )
    return publish


def _reusable_source_witness():
    return {
        "contract": "ptg2_v3_source_witness_payload_v5",
        "format_version": 5,
        "selection_method": "bottom_k_independent_occurrence_provider_cohorts_v3",
        "population_semantics": "queryable_emitted_price_provider_occurrence_v1",
        "unqueryable_rate_policy": "count_but_exclude_from_npi_api_challenges_v1",
        "source_count": 1,
        "source_set_digest": "11" * 32,
        "occurrence_target": 10_000,
        "total_target": 11_000,
        "provider_quota": 1_000,
        "queryable_occurrence_population_count": 50_000,
        "provider_population_count": 5_000,
        "emitted_rate_row_count": 1_000,
        "unqueryable_rate_row_count": 0,
        "occurrence_witness_count": 10_000,
        "provider_witness_count": 1_000,
        "record_count": 11_000,
        "evidence_dictionary_count": 1_000,
        "evidence_dictionary_raw_bytes": 10_000,
        "evidence_dictionary_stored_bytes": 5_000,
        "sample_digest": "22" * 32,
        "payload_sha256": "33" * 32,
        "payload_bytes": 1_024,
        "compression": "per_record_zlib_shared_evidence_dictionary_v1",
    }


def _reusable_finalizer_block_copy():
    return {
        "contract": "selective_shared_block_copy_v1",
        "total": {
            "source_copy_bytes": 100,
            "staged_copy_bytes": 70,
            "reused_payload_bytes": 30,
        },
    }


def _reusable_v3_layout_manifest(provider_identifier_quarantine):
    """Build reusable physical layout evidence with stale logical fields."""

    source_witness_by_field = _reusable_source_witness()
    serving_index_by_field = {
        "storage": "manifest_snapshot",
        "type": "ptg2_shared_blocks_v3",
        "snapshot_scoped": True,
        "arch_version": "postgres_binary_v3",
        "storage_generation": "shared_blocks_v3",
        "shared_block_layout": "dense_shared_blocks_v3",
        "source_count": 1,
        "code_count": 17,
        "cold_lookup_contract": "ptg_v3_cold_v2",
        "price_membership_semantics": "multiset_v1",
        "serving_multiplicity_semantics": "source_multiset_v1",
        "shared_snapshot_key": 7,
        "serving_rates": 123,
        "provider_graph": {"owner_count": 4},
        "finalizer_block_copy": _reusable_finalizer_block_copy(),
        "source_witness": source_witness_by_field,
        "plan_id": "stale-plan",
        "source_file_versions": [{"source": "stale"}],
        "future_logical_provenance": {"owner": "stale"},
        "table": "mrf.retired_table",
    }
    if provider_identifier_quarantine is not None:
        serving_index_by_field["provider_identifier_quarantine"] = (
            provider_identifier_quarantine
        )
    return {"serving_index": serving_index_by_field}


def test_reused_v3_serving_index_copies_only_physical_contract_fields():
    quarantine = ptg_provider_quarantine.provider_identifier_quarantine_payload(
        {-17: 2, 123_456_789: 3}
    )

    serving_result = process_ptg._reused_shared_v3_serving_index(
        _reusable_v3_layout_manifest(quarantine),
        source_key="current-source",
        shared_snapshot_key=9,
    )

    assert serving_result["source_key"] == "current-source"
    assert serving_result["shared_snapshot_key"] == 9
    assert serving_result["code_count"] == 17
    assert serving_result["serving_rates"] == 123
    assert serving_result["provider_graph"] == {"owner_count": 4}
    assert serving_result["finalizer_block_copy"] == {
        "contract": "selective_shared_block_copy_v1",
        "total": {
            "source_copy_bytes": 100,
            "staged_copy_bytes": 70,
            "reused_payload_bytes": 30,
        },
    }
    assert serving_result["provider_identifier_quarantine"] == quarantine
    assert serving_result["table"] is None
    assert "plan_id" not in serving_result
    assert "source_file_versions" not in serving_result
    assert "future_logical_provenance" not in serving_result


def test_reused_v4_serving_index_requires_packed_graph_contract():
    quarantine = ptg_provider_quarantine.provider_identifier_quarantine_payload({})
    layout_manifest = _reusable_v3_layout_manifest(quarantine)
    serving_index = layout_manifest["serving_index"]
    serving_index.update(
        {
            "type": "ptg2_shared_blocks_v4",
            "storage_generation": "shared_blocks_v4",
            "shared_block_layout": "packed_snapshot_maps_v4",
            "provider_scope_strategy": "postgres_packed_graph_v4",
            "snapshot_map": {
                "contract": "ptg_v4_packed_snapshot_map_v1",
                "map_digest": "ab" * 32,
            },
        }
    )
    serving_index.setdefault("serving_binary", {})["provider_graph_v4"] = {
        "contract": "ptg2_provider_graph_v4",
        "representation": "pattern_v1",
        "npi_table": "ptg2_v4_npi_scope",
    }

    serving_result = process_ptg._reused_shared_v3_serving_index(
        layout_manifest,
        source_key="current-source",
        shared_snapshot_key=9,
        expected_generation="shared_blocks_v4",
    )

    assert serving_result["storage_generation"] == "shared_blocks_v4"
    assert serving_result["snapshot_map"]["map_digest"] == "ab" * 32
    assert serving_result["serving_binary"]["provider_graph_v4"]["contract"] == (
        "ptg2_provider_graph_v4"
    )


@pytest.mark.parametrize("evidence_failure", ["missing", "tampered"])
def test_reused_v3_serving_index_rejects_invalid_quarantine_evidence(
    evidence_failure,
):
    quarantine = ptg_provider_quarantine.provider_identifier_quarantine_payload(
        {123_456_789: 2}
    )
    if evidence_failure == "missing":
        quarantine = None
    else:
        quarantine["occurrence_count"] += 1

    with pytest.raises(
        RuntimeError,
        match="invalid provider identifier quarantine evidence",
    ):
        process_ptg._reused_shared_v3_serving_index(
            _reusable_v3_layout_manifest(quarantine),
            source_key="current-source",
            shared_snapshot_key=9,
        )


@pytest.mark.parametrize("witness_failure", ["missing", "incompatible"])
def test_reused_v3_serving_index_rejects_invalid_source_witness(
    witness_failure,
):
    quarantine = ptg_provider_quarantine.provider_identifier_quarantine_payload({})
    manifest = _reusable_v3_layout_manifest(quarantine)
    serving_index = manifest["serving_index"]
    if witness_failure == "missing":
        serving_index.pop("source_witness")
    else:
        serving_index["source_witness"]["contract"] = (
            "ptg2_v3_source_witness_payload_v1"
        )

    with pytest.raises(RuntimeError, match="incompatible source witness evidence"):
        process_ptg._reused_shared_v3_serving_index(
            manifest,
            source_key="current-source",
            shared_snapshot_key=9,
        )


def test_shared_v3_physical_identity_binds_source_witness_publisher(
    tmp_path,
    monkeypatch,
):
    scanner_binary = tmp_path / "ptg2_scanner"
    scanner_binary.write_bytes(b"scanner")
    monkeypatch.setattr(
        process_ptg,
        "_ptg2_rust_scanner_binary",
        lambda: scanner_binary,
    )
    baseline = process_ptg._shared_v3_scanner_identity()
    original_read_bytes = Path.read_bytes

    def changed_witness_source(path):
        source_bytes = original_read_bytes(path)
        if path.name == "ptg2_source_witness.py":
            return source_bytes + b"\n# physical-contract-change\n"
        return source_bytes

    monkeypatch.setattr(Path, "read_bytes", changed_witness_source)
    changed = process_ptg._shared_v3_scanner_identity()

    assert baseline["contract_version"] == 3
    assert changed["publisher_source_sha256"] != baseline["publisher_source_sha256"]


def test_scanner_fingerprint_isolates_v4_sources_when_disabled(
    tmp_path,
    monkeypatch,
):
    scanner_binary = tmp_path / "ptg2_scanner"
    compiler_binary = tmp_path / "ptg2_provider_graph_v4"
    scanner_binary.write_bytes(b"scanner")
    compiler_binary.write_bytes(b"compiler")
    monkeypatch.setattr(
        process_ptg,
        "_ptg2_rust_scanner_binary",
        lambda: scanner_binary,
    )
    monkeypatch.setattr(
        process_ptg,
        "_resolve_v4_graph_compiler_binary",
        lambda: compiler_binary,
    )
    monkeypatch.delenv("HLTHPRT_PTG2_PROVIDER_GRAPH_V4", raising=False)
    v3_baseline = process_ptg._shared_v3_scanner_identity()
    monkeypatch.setenv("HLTHPRT_PTG2_PROVIDER_GRAPH_V4", "1")
    v4_baseline = process_ptg._shared_v3_scanner_identity()

    original_read_bytes = Path.read_bytes

    def changed_v4_source(path):
        source_bytes = original_read_bytes(path)
        if path.name == "ptg2_v4_graph_compiler.py":
            return source_bytes + b"\n# v4-only-physical-contract-change\n"
        return source_bytes

    monkeypatch.setattr(Path, "read_bytes", changed_v4_source)
    monkeypatch.delenv("HLTHPRT_PTG2_PROVIDER_GRAPH_V4", raising=False)
    v3_after_v4_change = process_ptg._shared_v3_scanner_identity()
    monkeypatch.setenv("HLTHPRT_PTG2_PROVIDER_GRAPH_V4", "1")
    v4_after_v4_change = process_ptg._shared_v3_scanner_identity()

    assert v3_baseline["contract_version"] == 3
    assert v3_after_v4_change["publisher_source_sha256"] == (
        v3_baseline["publisher_source_sha256"]
    )
    assert v3_after_v4_change["publisher_source_bytes"] == (
        v3_baseline["publisher_source_bytes"]
    )
    assert v4_baseline["contract_version"] == 4
    assert v4_after_v4_change["publisher_source_sha256"] != (
        v4_baseline["publisher_source_sha256"]
    )
    assert v4_after_v4_change["publisher_source_bytes"] > (
        v4_baseline["publisher_source_bytes"]
    )


def _strict_v3_downloaded_job(job):
    artifact_digest = hashlib.sha256(str(job.get("url") or "").encode()).hexdigest()
    scoped_job_map = {
        **job,
        "meta": {
            "plan_id": "TEST-PLAN-001",
            "plan_id_type": "ein",
            "plan_market_type": "group",
        },
    }
    return process_ptg.PTG2DownloadedJob(
        job=scoped_job_map,
        raw_artifact=SimpleNamespace(
            raw_sha256=artifact_digest,
            raw_storage_uri=f"/tmp/{artifact_digest}.json.gz",
        ),
        logical_artifact=SimpleNamespace(
            logical_path="/tmp/rates.json.gz",
            logical_sha256=artifact_digest,
            byte_count=1024,
            logical_hash_deferred=False,
        ),
    )


def _patch_v4_identity_binaries(tmp_path, monkeypatch) -> None:
    scanner_binary = tmp_path / "ptg2_scanner"
    compiler_binary = tmp_path / "ptg2_provider_graph_v4"
    scanner_binary.write_bytes(b"scanner")
    compiler_binary.write_bytes(b"compiler")
    monkeypatch.setenv("HLTHPRT_PTG2_PROVIDER_GRAPH_V4", "1")
    monkeypatch.setattr(
        process_ptg,
        "_ptg2_rust_scanner_binary",
        lambda: scanner_binary,
    )
    monkeypatch.setattr(
        process_ptg,
        "_resolve_v4_graph_compiler_binary",
        lambda: compiler_binary,
    )


def _v4_identity_fingerprint(downloaded) -> str:
    return process_ptg.shared_physical_input_identity(
        [downloaded],
        options={},
        scanner_canon_version=process_ptg._shared_v3_scanner_identity(),
    ).semantic_fingerprint


def _expected_v4_graph_encoding_policy() -> dict[str, int]:
    return {
        "member_page_bytes": 16 * 1024,
        "locator_page_bytes": 16 * 1024,
        "heavy_owner_member_threshold": 4096,
        "heavy_bitmap_minimum_savings_bytes": 512,
        "max_set_patterns_per_set": 1024,
        "max_set_components_per_fallback_set": 4096,
        "max_online_group_keys_per_set": 4096,
        "max_online_source_owners_per_set": 4096,
        "max_online_source_members_per_set": 16_384,
        "max_online_source_pages_per_set": 64,
        "max_online_source_bytes_per_set": 1024 * 1024,
        "online_group_npi_batch_size": 32,
        "max_online_group_npi_members_per_set": 32_768,
        "max_online_group_npi_locator_pages_per_set": 16,
        "max_online_group_npi_member_pages_per_set": 128,
        "max_online_group_npi_bytes_per_set": 4 * 1024 * 1024,
        "max_online_group_npi_batches_per_set": 4,
        "provider_expansion_rate_page_rows": 64,
        "max_online_provider_expansion_rate_rows": 256,
        "max_online_provider_expansion_provider_sets": 64,
        "max_online_provider_expansion_graph_batches": 64,
        "npi_prefix_target": 201,
        "max_npi_prefix_override_owners": 50_000,
        "max_npi_prefix_override_bytes": 64 * 1024 * 1024,
    }


def test_v4_physical_identity_binds_encoding_policy_but_not_admission_caps(
    tmp_path,
    monkeypatch,
):
    """Bind physical identity to bytes-on-disk policy, not admission caps."""

    _patch_v4_identity_binaries(tmp_path, monkeypatch)
    downloaded = _strict_v3_downloaded_job(
        {
            "url": "https://example.test/rates",
            "type": "in_network",
        }
    )

    baseline = _v4_identity_fingerprint(downloaded)
    monkeypatch.setenv(
        "HLTHPRT_PTG2_V4_GRAPH_MEMBER_PAGE_BYTES",
        "32768",
    )
    changed_encoding = _v4_identity_fingerprint(downloaded)
    monkeypatch.delenv("HLTHPRT_PTG2_V4_GRAPH_MEMBER_PAGE_BYTES")
    monkeypatch.setenv(
        "HLTHPRT_PTG2_V4_GRAPH_MAX_ESTIMATED_MODEL_BYTES",
        "777777777",
    )
    changed_admission = _v4_identity_fingerprint(downloaded)

    assert changed_encoding != baseline
    assert changed_admission == baseline
    assert process_ptg._shared_v3_scanner_identity()[
        "provider_graph_encoding_policy"
    ] == _expected_v4_graph_encoding_policy()


def test_full_rebuild_proof_metrics_are_opt_in_and_count_raw_reuse():
    stage_counts = process_ptg.PTG2ArtifactStageCounts(
        artifacts_observed=2,
        raw_artifacts_total=2,
        raw_artifacts_reused=1,
        raw_artifacts_unique=2,
        raw_artifacts_duplicate_identities=0,
        logical_artifacts_total=2,
        logical_artifacts_reused=1,
        logical_artifacts_unique=2,
        logical_artifacts_duplicate_identities=0,
        logical_artifacts_deferred_hashes=0,
    )

    assert process_ptg._full_rebuild_proof_metrics(
        stage_counts,
        full_rebuild_scope_digest=None,
        shared_layout_reused=False,
        shared_layout_reused_at_seal=False,
    ) == {}
    reused_metrics_by_name = process_ptg._full_rebuild_proof_metrics(
        stage_counts,
        full_rebuild_scope_digest="1" * 64,
        shared_layout_reused=True,
        shared_layout_reused_at_seal=True,
    )
    assert reused_metrics_by_name == {
        "full_rebuild": True,
        "artifacts_observed": 2,
        "raw_artifacts_total": 2,
        "raw_artifacts_reused": 1,
        "raw_artifacts_unique": 2,
        "raw_artifacts_duplicate_identities": 0,
        "logical_artifacts_total": 2,
        "logical_artifacts_reused": 1,
        "logical_artifacts_unique": 2,
        "logical_artifacts_duplicate_identities": 0,
        "logical_artifacts_deferred_hashes": 0,
        "shared_layout_reused": True,
        "shared_layout_reused_at_seal": True,
    }
    with pytest.raises(RuntimeError, match="retained or duplicate work"):
        process_ptg._assert_full_rebuild_is_fresh(
            reused_metrics_by_name
        )


def test_full_rebuild_failure_metrics_reject_hostile_allowed_key_values():
    failure = RuntimeError("scanner failed")
    failure.ptg_full_rebuild_metrics_by_name = {
        "full_rebuild": True,
        "raw_artifacts_total": "private payload",
        "raw_artifacts_reused": -1,
        "logical_artifacts_total": 2,
        "shared_layout_reused": "false",
        "finalizer_block_source_copy_bytes": 100,
        "finalizer_block_staged_copy_bytes": "private payload",
        "unknown_metric": "private payload",
    }

    assert process_ptg.full_rebuild_failure_metrics(failure) == {
        "full_rebuild": True,
        "logical_artifacts_total": 2,
        "finalizer_block_source_copy_bytes": 100,
    }


def test_full_rebuild_proof_metrics_include_safe_finalizer_copy_totals():
    stage_counts = process_ptg.PTG2ArtifactStageCounts(
        artifacts_observed=0,
        raw_artifacts_total=0,
        raw_artifacts_reused=0,
        raw_artifacts_unique=0,
        raw_artifacts_duplicate_identities=0,
        logical_artifacts_total=0,
        logical_artifacts_reused=0,
        logical_artifacts_unique=0,
        logical_artifacts_duplicate_identities=0,
        logical_artifacts_deferred_hashes=0,
    )

    metrics_by_name = process_ptg._full_rebuild_proof_metrics(
        stage_counts,
        full_rebuild_scope_digest="1" * 64,
        shared_layout_reused=False,
        shared_layout_reused_at_seal=False,
        finalizer_block_copy={
            "contract": "selective_shared_block_copy_v1",
            "total": {
                "source_copy_bytes": 100,
                "staged_copy_bytes": 70,
                "source_payload_bytes": 50,
                "staged_payload_bytes": 20,
                "reused_payload_bytes": 30,
                "row_count": 4,
                "staged_payload_row_count": 2,
                "reused_payload_row_count": 2,
                "unique_block_count": 3,
                "existing_block_count": 1,
                "new_block_count": 2,
                "duplicate_block_row_count": 1,
                "copy_seconds": 0.5,
                "unexpected": "private payload",
            },
        },
    )

    assert metrics_by_name["finalizer_block_source_copy_bytes"] == 100
    assert metrics_by_name["finalizer_block_staged_copy_bytes"] == 70
    assert metrics_by_name["finalizer_block_reused_payload_bytes"] == 30
    assert metrics_by_name["finalizer_block_row_count"] == 4
    assert metrics_by_name["finalizer_block_unique_block_count"] == 3
    assert metrics_by_name["finalizer_block_duplicate_block_row_count"] == 1
    assert "copy_seconds" not in metrics_by_name
    assert "unexpected" not in metrics_by_name


def test_full_rebuild_scope_isolates_snapshot_and_audit_run_identity():
    """Keep old identities stable while every controlled attempt stays unique."""

    import_month = datetime.date(2026, 7, 1)
    import_id = "test-import"
    default_snapshot_id = process_ptg._ptg2_deterministic_snapshot_id(
        import_month=import_month,
        import_id=import_id,
        option_by_name={},
    )
    explicit_default_snapshot_id = process_ptg._ptg2_deterministic_snapshot_id(
        import_month=import_month,
        import_id=import_id,
        option_by_name={"full_rebuild_scope_digest": None},
    )
    first_scoped_snapshot_id = process_ptg._ptg2_deterministic_snapshot_id(
        import_month=import_month,
        import_id=import_id,
        option_by_name={"full_rebuild_scope_digest": "1" * 64},
    )
    second_scoped_snapshot_id = process_ptg._ptg2_deterministic_snapshot_id(
        import_month=import_month,
        import_id=import_id,
        option_by_name={"full_rebuild_scope_digest": "2" * 64},
    )

    assert default_snapshot_id == explicit_default_snapshot_id
    assert len(
        {
            default_snapshot_id,
            first_scoped_snapshot_id,
            second_scoped_snapshot_id,
        }
    ) == 3
    assert process_ptg._ptg2_import_run_id(import_id) == "ptg2:test-import"
    first_run_id = process_ptg._ptg2_import_run_id(
        "x" * 96,
        full_rebuild_scope_digest="1" * 64,
    )
    second_run_id = process_ptg._ptg2_import_run_id(
        "x" * 96,
        full_rebuild_scope_digest="2" * 64,
    )
    assert first_run_id != second_run_id
    assert len(first_run_id) == len(second_run_id) == 96


def _reused_mixed_allowed_context():
    allowed_result = vars(
        _build_allowed_amount_file_result(
            {"url": "https://example.test/allowed-amounts.json.gz"}
        )
    )
    lane_report_by_field = {
        "files_attempted": 1,
        "files_processed": 1,
        "files_failed": 0,
        "files_skipped": 0,
        "successful_files": [allowed_result],
        "failed_files": [],
    }
    state_by_name = {"published": False}
    return (
        process_ptg._ReusedSharedV3AllowedContext(
            successful_files=[allowed_result],
            lane_report_by_field=lane_report_by_field,
            previous_snapshot_id="ptg2:202606:previous-allowed",
            snapshot_state_by_name=state_by_name,
        ),
        state_by_name,
    )


def _reused_mixed_negotiated_provenance():
    source_version = ptg_domain.PTG2SourceVersion(
        source_identity_hash="b" * 64,
        source_file_version_id="source-version-negotiated",
        original_url="https://example.test/rates.json.gz",
        canonical_url="https://example.test/rates.json.gz",
        raw_sha256="c" * 64,
        logical_sha256="d" * 64,
    )
    return {
        "meta": {
            "plan_id": "TEST-PLAN-001",
            "plan_id_type": "ein",
            "plan_market_type": "group",
        },
        "file_row": {"file_id": 456},
        "source_version": source_version,
        "source_trace_hash": "e" * 64,
        "network_names": ["Example PPO"],
    }


def _install_reused_mixed_support_mocks(monkeypatch):
    transaction_session = object()

    @asynccontextmanager
    async def fake_transaction():
        yield transaction_session

    monkeypatch.setattr(
        process_ptg,
        "_record_in_network_file_provenance",
        AsyncMock(return_value=_reused_mixed_negotiated_provenance()),
    )
    monkeypatch.setattr(process_ptg, "_push_ptg2_objects", AsyncMock())
    monkeypatch.setattr(
        process_ptg,
        "_publish_shared_v3_source_dictionary",
        AsyncMock(),
    )
    monkeypatch.setattr(
        process_ptg,
        "validate_reused_snapshot_sources",
        AsyncMock(),
    )
    monkeypatch.setattr(
        process_ptg,
        "_cleanup_old_ptg2_source_tables",
        AsyncMock(),
    )
    monkeypatch.setattr(
        process_ptg,
        "_enqueue_ptg2_auto_address_refresh_after_import",
        AsyncMock(return_value={"status": "disabled"}),
    )
    monkeypatch.setattr(
        process_ptg,
        "_drop_ptg2_snapshot_table_names",
        AsyncMock(),
    )
    monkeypatch.setattr(
        process_ptg,
        "release_current_artifact_lease",
        Mock(),
    )
    monkeypatch.setattr(process_ptg.db, "transaction", fake_transaction)
    return transaction_session


def _install_reused_mixed_publish_mocks(monkeypatch):
    """Install the isolated lifecycle collaborators for reused mixed imports."""

    transaction_session = _install_reused_mixed_support_mocks(monkeypatch)
    candidate_stage = _install_candidate_stage_mock(monkeypatch)
    fallback_publish_source_pointers = AsyncMock(
        return_value={"status": "promoted"}
    )
    activate_candidate = AsyncMock(
        return_value={
            "status": "promoted",
            "source_key": "example_dental",
            "allowed_amount_pointer": {
                "status": "promoted",
                "source_key": "example_dental_allowed_amounts",
                "snapshot_id": "ptg2:202607:reused-mixed",
                "previous_snapshot_id": (
                    "ptg2:202606:previous-allowed"
                ),
            },
        }
    )
    persist_completion = AsyncMock()
    monkeypatch.setattr(
        process_ptg,
        "_publish_ptg2_source_pointers",
        fallback_publish_source_pointers,
    )
    monkeypatch.setattr(
        process_ptg,
        "_activate_ptg2_source_candidate_in_transaction",
        activate_candidate,
    )
    monkeypatch.setattr(
        process_ptg,
        "_acquire_source_pointer_gc_lock",
        AsyncMock(),
    )
    monkeypatch.setattr(
        process_ptg,
        "_persist_completed_ptg2_import_run",
        persist_completion,
    )
    return SimpleNamespace(
        transaction_session=transaction_session,
        candidate_stage=candidate_stage,
        fallback_publish_source_pointers=fallback_publish_source_pointers,
        activate_candidate=activate_candidate,
        persist_completion=persist_completion,
    )


def _run_reused_mixed_publish(
    monkeypatch,
    *,
    auto_activate: bool,
):
    lifecycle = _install_reused_mixed_publish_mocks(monkeypatch)
    allowed_context, allowed_state_by_name = (
        _reused_mixed_allowed_context()
    )
    downloaded = _strict_v3_downloaded_job(
        {
            "type": "in_network",
            "url": "https://example.test/rates.json.gz",
        }
    )
    publication_by_field = asyncio.run(
        process_ptg._publish_reused_shared_v3_snapshot(
            downloaded_jobs=[downloaded],
            shared_input_identity=SimpleNamespace(source_count=1),
            classes={"ImportLog": object},
            layout_manifest=_reusable_v3_layout_manifest(
                ptg_provider_quarantine.provider_identifier_quarantine_payload(
                    {}
                )
            ),
            shared_snapshot_key=7,
            semantic_fingerprint=b"\x11" * 32,
            coverage_scope_id=b"\x22" * 32,
            coverage_plan_scopes=[
                SimpleNamespace(
                    plan_id="TEST-PLAN-001",
                    plan_market_type="group",
                )
            ],
            snapshot_id="ptg2:202607:reused-mixed",
            import_run_id="reused-mixed",
            source_key="example_dental",
            import_month=datetime.date(2026, 7, 1),
            previous_snapshot_id="ptg2:202606:previous-negotiated",
            started_at=process_ptg._utcnow(),
            options={"auto_activate_candidates": auto_activate},
            allowed_context=allowed_context,
            manifest_stage_table="mrf.ptg2_stage_reused",
            test_mode=True,
            import_started_monotonic=process_ptg._ptg2_monotonic(),
        )
    )
    return SimpleNamespace(
        publication_by_field=publication_by_field,
        allowed_state_by_name=allowed_state_by_name,
        **vars(lifecycle),
    )


def _assert_reused_mixed_pointer_cutover(lifecycle):
    lifecycle.activate_candidate.assert_awaited_once_with(
        lifecycle.transaction_session,
        schema_name="mrf",
        source_key="example_dental",
        snapshot_id="ptg2:202607:reused-mixed",
        expected_current_snapshot_id=(
            "ptg2:202606:previous-negotiated"
        ),
    )
    lifecycle.fallback_publish_source_pointers.assert_not_awaited()


def test_reused_v3_mixed_pointer_cutover_fails_closed_on_allowed_cas(
    monkeypatch,
):
    """Rollback negotiated activation when the allowed pointer CAS fails."""

    transaction_session = object()
    rollback_events = []

    @asynccontextmanager
    async def failing_transaction():
        try:
            yield transaction_session
        except RuntimeError:
            rollback_events.append(True)
            raise

    activate_candidate = AsyncMock(
        side_effect=RuntimeError("allowed pointer conflict")
    )
    monkeypatch.setattr(process_ptg.db, "transaction", failing_transaction)
    monkeypatch.setattr(
        process_ptg,
        "_acquire_source_pointer_gc_lock",
        AsyncMock(),
    )
    monkeypatch.setattr(
        process_ptg,
        "_activate_ptg2_source_candidate_in_transaction",
        activate_candidate,
    )

    with pytest.raises(RuntimeError, match="allowed pointer conflict"):
        asyncio.run(
            process_ptg._publish_mixed_candidate_current_pointers(
                source_key="example_dental",
                snapshot_id="ptg2:202607:reused-mixed",
                previous_snapshot_id=(
                    "ptg2:202606:previous-negotiated"
                ),
                previous_allowed_snapshot_id=(
                    "ptg2:202606:previous-allowed"
                ),
                import_month=datetime.date(2026, 7, 1),
                updated_at=process_ptg._utcnow(),
            )
        )

    activate_candidate.assert_awaited_once()
    assert rollback_events == [True]


def test_reused_v3_mixed_auto_activation_publishes_allowed_evidence(
    monkeypatch,
):
    """Publish both mixed-evidence pointer families from a reused layout."""

    lifecycle = _run_reused_mixed_publish(
        monkeypatch,
        auto_activate=True,
    )

    candidate_manifest = lifecycle.candidate_stage.await_args.kwargs[
        "snapshot_attributes"
    ]["manifest"]
    allowed_index = candidate_manifest["allowed_amount_index"]
    source_types = {
        version["source_type"]
        for version in candidate_manifest["source_file_versions"]
    }

    assert candidate_manifest["data_domains"] == [
        process_ptg.PTG2_DOMAIN_IN_NETWORK,
        process_ptg.PTG2_DOMAIN_ALLOWED_AMOUNT,
    ]
    assert candidate_manifest["allowed_amount_lane"]["files_processed"] == 1
    assert candidate_manifest["allowed_amount_provider_payments"] == 1
    assert candidate_manifest["allowed_amount_evidence"] is True
    assert allowed_index["previous_snapshot_id"] == (
        "ptg2:202606:previous-allowed"
    )
    assert allowed_index["current_source_key"] == (
        "example_dental_allowed_amounts"
    )
    assert source_types == {"in_network", "allowed_amounts"}

    _assert_reused_mixed_pointer_cutover(lifecycle)
    assert lifecycle.allowed_state_by_name["published"] is True
    assert lifecycle.publication_by_field["activation_status"] == "activated"
    assert lifecycle.publication_by_field["allowed_amount_evidence"] is True
    assert {
        version["source_type"]
        for version in lifecycle.publication_by_field[
            "source_file_versions"
        ]
    } == {"in_network", "allowed_amounts"}
    completion_report = lifecycle.persist_completion.await_args.kwargs[
        "report_payload"
    ]
    assert completion_report["allowed_amount_pointer"]["status"] == (
        "promoted"
    )


def _reused_mixed_candidate_attributes(lifecycle):
    stage_call_by_field = lifecycle.candidate_stage.await_args.kwargs
    return ptg_source_pointers.candidate_snapshot_attributes(
        stage_call_by_field["snapshot_attributes"],
        source_key=stage_call_by_field["source_key"],
        previous_snapshot_id=stage_call_by_field["previous_snapshot_id"],
    )


def _activate_reused_mixed_candidate(monkeypatch, candidate_attributes):
    """Activate the candidate staged by the deferred reused-layout fixture."""

    monkeypatch.setattr(
        process_ptg,
        "_missing_snapshot_serving_resources",
        AsyncMock(return_value=([], [])),
    )
    activation_by_field = asyncio.run(
        process_ptg._resume_validated_candidate(
            snapshot_attributes=candidate_attributes,
            snapshot_id="ptg2:202607:reused-mixed",
            source_key="example_dental",
            import_month=datetime.date(2026, 7, 1),
            auto_activate=True,
        )
    )
    return activation_by_field


def test_reused_v3_mixed_deferred_activation_preserves_and_promotes_allowed(
    monkeypatch,
):
    """Keep mixed evidence dormant, then promote both pointer families."""

    lifecycle = _run_reused_mixed_publish(
        monkeypatch,
        auto_activate=False,
    )
    candidate_attributes = _reused_mixed_candidate_attributes(
        lifecycle
    )
    candidate_manifest = candidate_attributes["manifest"]

    assert lifecycle.publication_by_field["activation_status"] == "deferred"
    assert lifecycle.publication_by_field["allowed_amount_evidence"] is True
    assert candidate_manifest["allowed_amount_index"][
        "previous_snapshot_id"
    ] == "ptg2:202606:previous-allowed"
    assert candidate_manifest["allowed_amount_lane"]["successful_files"]
    assert {
        version["source_type"]
        for version in candidate_manifest["source_file_versions"]
    } == {"in_network", "allowed_amounts"}
    assert lifecycle.allowed_state_by_name["published"] is False
    lifecycle.activate_candidate.assert_not_awaited()

    activation_by_field = _activate_reused_mixed_candidate(
        monkeypatch,
        candidate_attributes,
    )
    _assert_reused_mixed_pointer_cutover(lifecycle)
    assert activation_by_field["activation_status"] == "activated"
    assert activation_by_field["allowed_amount_pointer"]["status"] == "promoted"
    assert activation_by_field["allowed_amount_provider_payments"] == 1
    assert {
        version["source_type"]
        for version in activation_by_field["source_file_versions"]
    } == {"in_network", "allowed_amounts"}


def test_ptg2_source_scoped_report_uses_published_serving_rate_count(monkeypatch):
    """Verify this PTG import regression contract."""
    pushed_list = []
    publish_source_pointers = AsyncMock()
    publish_source_dictionary = AsyncMock()

    async def fake_push(rows, cls, **_kwargs):
        pushed_list.extend((getattr(cls, "__name__", str(cls)), report_row) for report_row in rows)

    async def fake_downloaded_jobs(jobs, **_kwargs):
        assert len(jobs) == 1
        for job in jobs:
            assert job["plan_info"] == [
                {"plan_id": "plan-alpha", "plan_market_type": "group"},
                {"plan_id": "plan-beta", "plan_market_type": "group"},
            ]
            yield _strict_v3_downloaded_job(job)

    async def fake_process(*_args, **_kwargs):
        return process_ptg.PTG2FileProcessResult(
            "in_network",
            "https://example.test/rates.json.gz",
            True,
            summary={
                "serving_rates": 111,
                "manifest": {
                    "copy_files": {},
                    "source_trace_hash": "a" * 64,
                },
                **_empty_provider_identifier_quarantine_scanner_summary(),
            },
        )

    monkeypatch.setattr(process_ptg, "ensure_database", AsyncMock())
    monkeypatch.setattr(process_ptg, "ensure_ptg2_tables", AsyncMock())
    monkeypatch.setattr(process_ptg.db, "status", AsyncMock())
    monkeypatch.setattr(process_ptg, "_push_ptg2_objects", fake_push)
    monkeypatch.setattr(process_ptg, "_prepare_ptg_tables", AsyncMock(return_value={"ImportLog": "log"}))
    monkeypatch.setattr(process_ptg, "_create_serving_stage_table", AsyncMock(return_value="manifest_stage"))
    monkeypatch.setattr(process_ptg, "_iter_downloaded_ptg_jobs", fake_downloaded_jobs)
    monkeypatch.setattr(process_ptg, "_process_in_network_file", fake_process)
    monkeypatch.setattr(process_ptg, "flush_error_log", AsyncMock())
    monkeypatch.setattr(
        process_ptg,
        "_publish_shared_v3_source_dictionary",
        publish_source_dictionary,
    )
    _install_strict_v3_publish_mocks(monkeypatch, serving_rates=987)
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
            plan_ids=["plan-alpha", "plan-beta"],
            plan_market_types=["Group"],
        )
    )

    import_run_rows = [report_row for cls_name, report_row in pushed_list if cls_name == "PTG2ImportRun"]
    final_report = import_run_rows[-1]["report"]
    assert final_report["serving_rates"] == 987
    assert final_report["rate_count"] == 987
    assert final_report["serving_index"]["serving_rates"] == 987
    assert final_report["serving_index"]["source_set"]["contract"] == (
        "sorted_raw_container_sha256_bytes_v1"
    )
    assert final_report["serving_index"]["source_set"]["source_count"] == 1
    assert len(
        final_report["serving_index"]["source_set"][
            "raw_container_sha256_digest"
        ]
    ) == 64
    assert final_report["successful_files"][0]["summary"]["serving_rates"] == 111
    published_snapshot = publish_source_pointers.await_args.kwargs["snapshot_attributes"]
    assert published_snapshot["manifest"]["serving_rates"] == 987
    assert published_snapshot["manifest"]["serving_index"]["source_set"] == (
        final_report["serving_index"]["source_set"]
    )
    assert publish_source_dictionary.await_args.kwargs["expected_source_set"] == (
        final_report["serving_index"]["source_set"]
    )


def test_ptg2_import_defers_live_pointer_mutation_by_default(monkeypatch):
    """Verify ptg2 import defers live pointer mutation by default."""
    pushed_list = []
    publish_source_pointers = AsyncMock()
    cleanup_old_source_tables = AsyncMock()
    address_refresh = AsyncMock()

    async def fake_push(rows, cls, **_kwargs):
        pushed_list.extend((getattr(cls, "__name__", str(cls)), import_row) for import_row in rows)

    async def fake_downloaded_jobs(jobs, **_kwargs):
        for job in jobs:
            yield _strict_v3_downloaded_job(job)

    async def fake_process(*_args, **_kwargs):
        return process_ptg.PTG2FileProcessResult(
            "in_network",
            "https://example.test/rates.json.gz",
            True,
            summary={
                "serving_rates": 111,
                "manifest": {
                    "copy_files": {},
                    "source_trace_hash": "a" * 64,
                },
                **_empty_provider_identifier_quarantine_scanner_summary(),
            },
        )

    monkeypatch.delenv("HLTHPRT_PTG2_AUTO_ACTIVATE_CANDIDATES", raising=False)
    monkeypatch.setattr(process_ptg, "ensure_database", AsyncMock())
    monkeypatch.setattr(process_ptg, "ensure_ptg2_tables", AsyncMock())
    monkeypatch.setattr(process_ptg.db, "status", AsyncMock())
    monkeypatch.setattr(process_ptg, "_push_ptg2_objects", fake_push)
    monkeypatch.setattr(
        process_ptg,
        "_prepare_ptg_tables",
        AsyncMock(return_value={"ImportLog": "log"}),
    )
    monkeypatch.setattr(
        process_ptg,
        "_create_serving_stage_table",
        AsyncMock(return_value="manifest_stage"),
    )
    monkeypatch.setattr(process_ptg, "_iter_downloaded_ptg_jobs", fake_downloaded_jobs)
    monkeypatch.setattr(process_ptg, "_process_in_network_file", fake_process)
    monkeypatch.setattr(process_ptg, "flush_error_log", AsyncMock())
    monkeypatch.setattr(
        process_ptg,
        "_publish_shared_v3_source_dictionary",
        AsyncMock(),
    )
    _install_strict_v3_publish_mocks(monkeypatch, serving_rates=987)
    monkeypatch.setattr(
        process_ptg,
        "_should_auto_activate_ptg2_candidates",
        lambda: False,
    )
    candidate_stage = process_ptg._stage_ptg2_source_candidate
    monkeypatch.setattr(
        process_ptg,
        "_current_source_snapshot_id",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        process_ptg,
        "_publish_ptg2_source_pointers",
        publish_source_pointers,
    )
    monkeypatch.setattr(
        process_ptg,
        "_cleanup_old_ptg2_source_tables",
        cleanup_old_source_tables,
    )
    monkeypatch.setattr(
        process_ptg,
        "_enqueue_ptg2_auto_address_refresh_after_import",
        address_refresh,
    )

    import_result = asyncio.run(
        process_ptg.main(
            in_network_url="https://example.test/rates.json.gz",
            import_month="2026-04",
            import_id="deferred_candidate",
            source_key="example_dental",
        )
    )

    assert import_result["arch_version"] == "postgres_binary_v3"
    assert import_result["activation_status"] == "deferred"
    assert import_result["snapshot_status"] == process_ptg.PTG2_STATUS_VALIDATED
    candidate_stage.assert_awaited_once()
    publish_source_pointers.assert_not_awaited()
    cleanup_old_source_tables.assert_not_awaited()
    address_refresh.assert_not_awaited()
    import_run_rows = [import_row for cls_name, import_row in pushed_list if cls_name == "PTG2ImportRun"]
    final_report = import_run_rows[-1]["report"]
    assert final_report["activation_status"] == "deferred"
    assert final_report["address_refresh"] == {
        "status": "skipped",
        "reason": "candidate-activation-deferred",
    }


def test_ptg2_completion_timing_ends_after_post_publish_work(monkeypatch):
    """Verify ptg2 completion timing ends after post publish work."""
    clock_map = {"seconds": 0.0}
    pushed_list: list[tuple[str, dict]] = []
    completion_events: list[tuple[str, float]] = []
    validated_write_count_list = [0]

    def advance(seconds: float) -> None:
        clock_map["seconds"] += seconds

    async def fake_push(rows, cls, **_kwargs):
        class_name = getattr(cls, "__name__", str(cls))
        for import_row in rows:
            if (
                class_name == "PTG2ImportRun"
                and import_row.get("status") == process_ptg.PTG2_STATUS_VALIDATED
            ):
                validated_write_count_list[0] += 1
                advance(13.0 if validated_write_count_list[0] == 1 else 2.0)
                completion_events.append(
                    (f"validated_write_{validated_write_count_list[0]}", clock_map["seconds"])
                )
            pushed_list.append((class_name, copy.deepcopy(import_row)))

    async def fake_downloaded_jobs(jobs, **_kwargs):
        for job in jobs:
            yield _strict_v3_downloaded_job(job)

    async def fake_process(*_args, **_kwargs):
        advance(10.0)
        return process_ptg.PTG2FileProcessResult(
            "in_network",
            "https://example.test/rates.json.gz",
            True,
            summary={
                "serving_rates": 1,
                "manifest": {
                    "copy_files": {},
                    "source_trace_hash": "a" * 64,
                },
                **_empty_provider_identifier_quarantine_scanner_summary(),
            },
        )

    async def publish_source_pointers(**_kwargs):
        advance(3.0)

    async def cleanup_old_source_tables(*_args, **_kwargs):
        advance(5.0)

    async def enqueue_address_refresh(**_kwargs):
        advance(7.0)
        return {"status": "disabled"}

    monkeypatch.setattr(process_ptg, "_ptg2_monotonic", lambda: clock_map["seconds"])
    monkeypatch.setattr(process_ptg, "ensure_database", AsyncMock())
    monkeypatch.setattr(process_ptg, "ensure_ptg2_tables", AsyncMock())
    monkeypatch.setattr(process_ptg.db, "status", AsyncMock())
    monkeypatch.setattr(process_ptg, "_push_ptg2_objects", fake_push)
    monkeypatch.setattr(
        process_ptg,
        "_prepare_ptg_tables",
        AsyncMock(return_value={"ImportLog": "log"}),
    )
    monkeypatch.setattr(
        process_ptg,
        "_create_serving_stage_table",
        AsyncMock(return_value="manifest_stage"),
    )
    monkeypatch.setattr(process_ptg, "_iter_downloaded_ptg_jobs", fake_downloaded_jobs)
    monkeypatch.setattr(process_ptg, "_process_in_network_file", fake_process)
    monkeypatch.setattr(process_ptg, "flush_error_log", AsyncMock())
    monkeypatch.setattr(process_ptg, "_publish_shared_v3_source_dictionary", AsyncMock())
    publish = _install_strict_v3_publish_mocks(monkeypatch, serving_rates=1)
    publication = publish.return_value

    async def timed_publish(*_args, **_kwargs):
        advance(20.0)
        return publication

    publish.side_effect = timed_publish
    monkeypatch.setattr(
        process_ptg,
        "_current_source_snapshot_id",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        process_ptg,
        "_publish_ptg2_source_pointers",
        publish_source_pointers,
    )
    monkeypatch.setattr(
        process_ptg,
        "_cleanup_old_ptg2_source_tables",
        cleanup_old_source_tables,
    )
    monkeypatch.setattr(
        process_ptg,
        "_enqueue_ptg2_auto_address_refresh_after_import",
        enqueue_address_refresh,
    )
    monkeypatch.setattr(
        process_ptg,
        "_emit_screen_line",
        lambda line: completion_events.append((line, clock_map["seconds"])),
    )

    import_result = asyncio.run(
        process_ptg.main(
            in_network_url="https://example.test/rates.json.gz",
            import_month="2026-04",
            import_id="completion_timing_test",
            source_key="completion_timing_test",
        )
    )

    validated_rows = [
        import_row
        for class_name, import_row in pushed_list
        if class_name == "PTG2ImportRun"
        and import_row["status"] == process_ptg.PTG2_STATUS_VALIDATED
    ]
    assert len(validated_rows) == 2
    assert "total_seconds" not in validated_rows[0]["report"]["timings"]
    final_report = validated_rows[1]["report"]
    timings = final_report["timings"]
    assert timings["data_seconds"] == pytest.approx(10.0)
    assert timings["publish_seconds"] == pytest.approx(20.0)
    assert timings[
        "post_publish_logical_candidate_and_optional_pointer_cutover_seconds"
    ] == pytest.approx(3.0)
    assert timings["post_publish_old_state_cleanup_seconds"] == pytest.approx(5.0)
    assert timings["post_publish_address_refresh_seconds"] == pytest.approx(7.0)
    assert timings["post_publish_run_state_persistence_seconds"] == pytest.approx(13.0)
    assert timings["post_publish_seconds"] == pytest.approx(28.0)
    assert timings["total_seconds"] == pytest.approx(58.0)
    assert final_report["timing_contract"] == {
        "version": 2,
        "total_boundary": "after_required_run_state_persistence",
        "completion_metrics_write_excluded": True,
    }
    assert import_result["timings"]["total_seconds"] == pytest.approx(58.0)

    done_events = [
        event for event in completion_events if event[0].startswith("PTG2_IMPORT_DONE")
    ]
    assert len(done_events) == 1
    done_total_match = re.search(r"\ttotal_seconds=([0-9.]+)", done_events[0][0])
    assert done_total_match is not None
    assert float(done_total_match.group(1)) == pytest.approx(58.0)
    completion_labels = [event[0] for event in completion_events]
    assert completion_labels.index("validated_write_2") < next(
        index
        for index, label in enumerate(completion_labels)
        if label.startswith("PTG2_IMPORT_DONE")
    )
    assert done_events[0][1] == pytest.approx(60.0)


def _install_ptg2_publish_failure_mocks(monkeypatch):
    pushed_rows = []

    async def fake_push(rows, cls, **_kwargs):
        pushed_rows.extend((getattr(cls, "__name__", str(cls)), row) for row in rows)

    async def fake_downloaded_jobs(jobs, **_kwargs):
        for job in jobs:
            yield _strict_v3_downloaded_job(job)

    async def fake_process(*_args, **_kwargs):
        return process_ptg.PTG2FileProcessResult(
            "in_network",
            "https://example.test/rates.json.gz",
            True,
            summary={
                "serving_rates": 1,
                "manifest": {
                    "copy_files": {},
                    "source_trace_hash": "a" * 64,
                },
                **_empty_provider_identifier_quarantine_scanner_summary(),
            },
        )

    monkeypatch.setattr(process_ptg, "ensure_database", AsyncMock())
    monkeypatch.setattr(process_ptg, "ensure_ptg2_tables", AsyncMock())
    monkeypatch.setattr(process_ptg.db, "status", AsyncMock())
    monkeypatch.setattr(process_ptg, "_push_ptg2_objects", fake_push)
    monkeypatch.setattr(
        process_ptg,
        "_prepare_ptg_tables",
        AsyncMock(return_value={"ImportLog": "log"}),
    )
    monkeypatch.setattr(
        process_ptg,
        "_create_serving_stage_table",
        AsyncMock(return_value="manifest_stage"),
    )
    monkeypatch.setattr(process_ptg, "_iter_downloaded_ptg_jobs", fake_downloaded_jobs)
    monkeypatch.setattr(process_ptg, "_process_in_network_file", fake_process)
    monkeypatch.setattr(process_ptg, "flush_error_log", AsyncMock())
    monkeypatch.setattr(process_ptg, "_publish_shared_v3_source_dictionary", AsyncMock())
    publish = _install_strict_v3_publish_mocks(monkeypatch, serving_rates=1)
    publish.side_effect = RuntimeError("binary publish failed")
    abandon = AsyncMock(return_value=True)
    monkeypatch.setattr(process_ptg, "is_shared_layout_build_abandoned", abandon)
    monkeypatch.setattr(process_ptg, "_current_source_snapshot_id", AsyncMock(return_value=None))
    monkeypatch.setattr(process_ptg, "_drop_ptg2_snapshot_tables_for_manifest", AsyncMock())
    monkeypatch.setattr(process_ptg, "delete_ptg2_artifacts_for_snapshot", AsyncMock())
    monkeypatch.setattr(
        process_ptg,
        "delete_unpublished_snapshot_sources",
        AsyncMock(),
    )
    return pushed_rows, abandon


def test_ptg2_publish_failure_abandons_owned_shared_layout(monkeypatch):
    """A failed publisher removes only the physical layout owned by its token."""
    pushed_rows, abandon = _install_ptg2_publish_failure_mocks(monkeypatch)

    with pytest.raises(RuntimeError, match="binary publish failed"):
        asyncio.run(
            process_ptg.main(
                in_network_url="https://example.test/rates.json.gz",
                import_month="2026-04",
                import_id="shared_layout_failure",
                source_key="example_dental",
            )
        )

    abandon.assert_awaited_once()
    abandon_kwargs = abandon.await_args.kwargs
    assert abandon_kwargs["snapshot_key"] == 7
    assert len(abandon_kwargs["build_token"]) == 32
    failed_runs = [
        layout_row
        for cls_name, layout_row in pushed_rows
        if cls_name == "PTG2ImportRun" and layout_row["status"] == process_ptg.PTG2_STATUS_FAILED
    ]
    failed_report = failed_runs[-1]["report"]
    assert failed_report["shared_layout_abandoned"] is True
    assert failed_report["timing_contract"]["total_boundary"] == (
        "after_required_failure_state_persistence"
    )
    assert failed_report["timings"]["total_seconds"] >= 0
    assert failed_report["timings"]["failure_handling_seconds"] >= 0
    assert failed_report["timings"]["failure_state_persistence_seconds"] >= 0


def test_failed_shared_layout_abandonment_retries_transient_database_errors(monkeypatch):
    transaction_attempt_numbers: list[int] = []

    @asynccontextmanager
    async def flaky_transaction():
        transaction_attempt_numbers.append(len(transaction_attempt_numbers) + 1)
        if len(transaction_attempt_numbers) < 3:
            raise RuntimeError("temporary database failure")
        yield object()

    abandonment = AsyncMock(return_value=True)
    sleep = AsyncMock()
    monkeypatch.setattr(process_ptg.db, "transaction", flaky_transaction)
    monkeypatch.setattr(process_ptg, "is_shared_layout_build_abandoned", abandonment)
    monkeypatch.setattr(process_ptg.asyncio, "sleep", sleep)

    is_abandoned = asyncio.run(
        process_ptg._is_failed_shared_layout_abandoned(
            SimpleNamespace(snapshot_key=7, reused=False),
            build_token="attempt-7",
        )
    )

    assert is_abandoned is True
    assert transaction_attempt_numbers == [1, 2, 3]
    assert [call.args[0] for call in sleep.await_args_list] == [0.1, 0.2]
    abandonment.assert_awaited_once()
    assert len(abandonment.await_args.args) == 1
    assert abandonment.await_args.kwargs == {
        "schema_name": "mrf",
        "snapshot_key": 7,
        "build_token": "attempt-7",
    }


def test_failed_v4_layout_abandonment_uses_exact_owned_gc_path(monkeypatch):
    abandonment = AsyncMock(
        return_value=SimpleNamespace(logical_layout_count=1)
    )
    legacy_abandonment = AsyncMock()
    monkeypatch.setattr(
        process_ptg,
        "abandon_owned_v4_layout",
        abandonment,
    )
    monkeypatch.setattr(
        process_ptg,
        "is_shared_layout_build_abandoned",
        legacy_abandonment,
    )

    is_abandoned = asyncio.run(
        process_ptg._is_failed_shared_layout_abandoned(
            SimpleNamespace(snapshot_key=491, reused=False),
            build_token="owned-v4-attempt",
            expected_generation=process_ptg.PTG2_V4_SHARED_GENERATION,
        )
    )

    assert is_abandoned is True
    abandonment.assert_awaited_once_with(
        snapshot_key=491,
        build_token="owned-v4-attempt",
    )
    legacy_abandonment.assert_not_awaited()


def _manifest_push_mock(pushed_list):
    async def fake_push(rows, cls, **_kwargs):
        pushed_list.extend((getattr(cls, "__name__", str(cls)), import_row) for import_row in rows)

    return fake_push


def _manifest_download_mock(download_options_by_name):
    async def fake_downloaded_jobs(jobs, **kwargs):
        download_options_by_name.update(kwargs)
        for job in jobs:
            downloaded_job = _strict_v3_downloaded_job(job)
            stage_observer = kwargs.get("artifact_stage_observer")
            if stage_observer is not None:
                stage_observer(
                    ptg_domain.PTG2ArtifactStageObservation(
                        artifact_kind=ptg_domain.PTG2_ARTIFACT_RAW,
                        identity_sha256=downloaded_job.raw_artifact.raw_sha256,
                        byte_count=1024,
                    )
                )
                stage_observer(
                    ptg_domain.PTG2ArtifactStageObservation(
                        artifact_kind=ptg_domain.PTG2_ARTIFACT_LOGICAL_JSON,
                        identity_sha256=(
                            downloaded_job.logical_artifact.logical_sha256
                        ),
                        byte_count=downloaded_job.logical_artifact.byte_count,
                    )
                )
            yield downloaded_job

    return fake_downloaded_jobs


def _manifest_process_mock(create_stage_mock):
    async def fake_process(*_args, **kwargs):
        stage_token = create_stage_mock.await_args.args[0]
        assert kwargs.get("ptg2_manifest_stage_table") == (
            process_ptg._ptg2_manifest_stage_table_name(stage_token)
        )
        return process_ptg.PTG2FileProcessResult(
            "in_network",
            "https://example.test/rates.json.gz",
            True,
            summary={
                "serving_rates": 111,
                "manifest": {
                    "serving_rows": 111,
                    "copy_files": {},
                    "source_trace_hash": "a" * 64,
                },
                **_empty_provider_identifier_quarantine_scanner_summary(),
            },
        )

    return fake_process


def _install_manifest_case_mocks(
    monkeypatch,
    pushed_list,
    download_options_by_name,
):
    create_stage_mock = AsyncMock(return_value="manifest_stage")
    publish_mock = _install_strict_v3_publish_mocks(monkeypatch, serving_rates=222)

    monkeypatch.setattr(process_ptg, "ensure_database", AsyncMock())
    monkeypatch.setattr(process_ptg, "ensure_ptg2_tables", AsyncMock())
    monkeypatch.setattr(process_ptg.db, "status", AsyncMock())
    monkeypatch.setattr(
        process_ptg,
        "_push_ptg2_objects",
        _manifest_push_mock(pushed_list),
    )
    monkeypatch.setattr(process_ptg, "_prepare_ptg_tables", AsyncMock(return_value={"ImportLog": "log"}))
    monkeypatch.setattr(process_ptg, "_create_serving_stage_table", create_stage_mock)
    monkeypatch.setattr(
        process_ptg,
        "_iter_downloaded_ptg_jobs",
        _manifest_download_mock(download_options_by_name),
    )
    monkeypatch.setattr(
        process_ptg,
        "_process_in_network_file",
        _manifest_process_mock(create_stage_mock),
    )
    monkeypatch.setattr(process_ptg, "flush_error_log", AsyncMock())
    monkeypatch.setattr(
        process_ptg,
        "_publish_shared_v3_source_dictionary",
        AsyncMock(),
    )
    monkeypatch.setattr(process_ptg, "_current_source_snapshot_id", AsyncMock(return_value=None))
    monkeypatch.setattr(process_ptg, "_publish_ptg2_source_pointers", AsyncMock())
    monkeypatch.setattr(process_ptg, "_cleanup_old_ptg2_source_tables", AsyncMock())
    return create_stage_mock, publish_mock


def _run_manifest_import_case(
    monkeypatch,
    *,
    full_rebuild_scope_digest=None,
):
    """Run one isolated manifest-import case and expose its proof calls."""

    pushed_list = []
    download_options_by_name = {}
    create_stage_mock, publish_mock = _install_manifest_case_mocks(
        monkeypatch,
        pushed_list,
        download_options_by_name,
    )

    main_options_by_name = {
        "in_network_url": "https://example.test/rates.json.gz",
        "import_month": "2026-04",
        "import_id": "test_mode_rust_env",
        "source_key": "example_dental",
        "test_mode": True,
    }
    if full_rebuild_scope_digest is not None:
        main_options_by_name.update(
            {
                "reuse_raw_artifacts": True,
                "keep_partial_artifacts": True,
                "full_rebuild_scope_digest": full_rebuild_scope_digest,
            }
        )
    import_result = asyncio.run(
        process_ptg.main(**main_options_by_name)
    )

    create_stage_mock.assert_awaited_once()
    publish_mock.assert_awaited_once()
    import_run_rows = [import_row for cls_name, import_row in pushed_list if cls_name == "PTG2ImportRun"]
    return SimpleNamespace(
        import_result=import_result,
        import_run=import_run_rows[-1],
        publish_mock=publish_mock,
        download_options_by_name=download_options_by_name,
    )


def test_ptg2_test_mode_uses_manifest_source_scoped_import(monkeypatch):
    """Verify the legacy PTG main path retains its exact default behavior."""

    run = _run_manifest_import_case(monkeypatch)

    assert "full_rebuild_scope_digest" not in run.publish_mock.await_args.kwargs
    assert "full_rebuild_scope_digest" not in run.import_run["options"]
    assert run.import_run["options"]["snapshot_arch"] == "postgres_binary_v3"
    assert run.import_run["options"]["storage_generation"] == "shared_blocks_v3"
    assert run.import_run["options"]["test_mode"] is True
    assert run.import_run["report"]["serving_rates"] == 222
    assert "full_rebuild" not in run.import_result


def test_ptg2_full_rebuild_forces_fresh_manifest_source_import(monkeypatch):
    """Verify a controlled rebuild bypasses every physical reuse boundary."""

    rebuild_scope_digest = "1" * 64
    run = _run_manifest_import_case(
        monkeypatch,
        full_rebuild_scope_digest=rebuild_scope_digest,
    )

    assert run.download_options_by_name["reuse_raw_artifacts"] is False
    assert run.download_options_by_name["keep_partial_artifacts"] is False
    assert run.publish_mock.await_args.kwargs["full_rebuild_scope_digest"] == (
        rebuild_scope_digest
    )
    assert run.import_run["options"]["reuse_raw_artifacts"] is False
    assert run.import_run["options"]["keep_partial_artifacts"] is False
    assert run.import_run["options"]["full_rebuild_scope_digest"] == (
        rebuild_scope_digest
    )
    assert run.import_run["import_run_id"].endswith(
        f":rebuild-{rebuild_scope_digest[:24]}"
    )
    expected_rebuild_metrics_by_name = {
        "full_rebuild": True,
        "artifacts_observed": 1,
        "raw_artifacts_total": 1,
        "raw_artifacts_reused": 0,
        "raw_artifacts_unique": 1,
        "raw_artifacts_duplicate_identities": 0,
        "logical_artifacts_total": 1,
        "logical_artifacts_reused": 0,
        "logical_artifacts_unique": 1,
        "logical_artifacts_duplicate_identities": 0,
        "logical_artifacts_deferred_hashes": 0,
        "shared_layout_reused": False,
        "shared_layout_reused_at_seal": False,
    }
    assert {
        metric_name: run.import_run["report"][metric_name]
        for metric_name in expected_rebuild_metrics_by_name
    } == expected_rebuild_metrics_by_name
    assert {
        metric_name: run.import_result[metric_name]
        for metric_name in expected_rebuild_metrics_by_name
    } == expected_rebuild_metrics_by_name


def test_ptg2_full_rebuild_toc_freshness_failure_stops_later_tocs(monkeypatch):
    """Keep a repeated TOC stage on the specific fail-fast rebuild path."""

    pushed_list = []
    download_options_by_name = {}
    _install_manifest_case_mocks(
        monkeypatch,
        pushed_list,
        download_options_by_name,
    )
    toc_urls = [
        "https://example.test/first-index.json",
        "https://example.test/second-index.json",
    ]
    observed_toc_urls = []

    async def repeat_toc_stage(toc_url, *_args, **kwargs):
        observed_toc_urls.append(toc_url)
        stage_observer = kwargs["artifact_stage_observer"]
        observation = ptg_domain.PTG2ArtifactStageObservation(
            artifact_kind=ptg_domain.PTG2_ARTIFACT_RAW,
            identity_sha256="a" * 64,
            byte_count=1024,
        )
        stage_observer(observation)
        stage_observer(observation)
        raise AssertionError("duplicate stage observation must fail")

    monkeypatch.setattr(
        process_ptg,
        "_process_table_of_contents",
        repeat_toc_stage,
    )

    with pytest.raises(
        process_ptg.PTG2FullRebuildFreshnessError,
        match="repeated an artifact stage",
    ) as error_info:
        asyncio.run(
            process_ptg.main(
                toc_urls=toc_urls,
                import_month="2026-04",
                import_id="toc_freshness_test",
                source_key="toc_freshness_test",
                full_rebuild_scope_digest="1" * 64,
            )
        )

    assert observed_toc_urls == toc_urls[:1]
    assert error_info.value.metrics_by_name[
        "raw_artifacts_duplicate_identities"
    ] == 1
    failed_import_runs = [
        import_row
        for cls_name, import_row in pushed_list
        if cls_name == "PTG2ImportRun" and import_row["status"] == "failed"
    ]
    assert failed_import_runs[-1]["report"][
        "raw_artifacts_duplicate_identities"
    ] == 1





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
    response_bytes = b"x" * (256 * 1024)

    async def handle(request):
        if request.method == "HEAD":
            return web.Response(headers={"Content-Length": "1"})
        return web.Response(body=response_bytes, headers={"Content-Length": str(len(response_bytes))})

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


def test_serving_only_import_recovers_unreported_worker_copy_files(tmp_path, monkeypatch):
    """Count only genuinely unreported worker files during recovery."""
    artifact_dir = tmp_path / "artifacts"
    recovered_paths_by_kind = {}
    graph_input_paths = []
    row_counted_paths = []
    monkeypatch.setenv("HLTHPRT_PTG2_SNAPSHOT_ARCH", "postgres_binary_v3")

    async def fake_push_ptg2_objects(*_args, **_kwargs):
        return None

    async def fake_flush_error_log(*_args, **_kwargs):
        return None

    async def fake_scanner(*_args, **kwargs):
        price_worker = Path(f"{kwargs['manifest_price_atom_copy_path']}.worker0003")
        summary_worker = Path(
            f"{kwargs['manifest_price_set_summary_copy_path']}.worker0003"
        )
        member_worker = Path(f"{kwargs['manifest_provider_group_member_copy_path']}.provider_refs.worker0003")
        price_worker.write_text("price-1\n")
        summary_worker.write_text("price-set-1\t1.25\n")
        member_worker.write_text("member-1\n")
        recovered_paths_by_kind.update(
            price=str(price_worker),
            summary=str(summary_worker),
            member=str(member_worker),
        )
        yield "manifest_price_atom_copy_file", {
            "path": str(price_worker),
            "bytes": price_worker.stat().st_size,
            "row_count": 1,
            "final": True,
        }
        yield "scanner_config", {"worker_count": 2}
        yield "scanner_summary", {"serving_run_rows": 2}

    original_copy_file_row_count = process_ptg._ptg2_copy_file_row_count

    def tracked_copy_file_row_count(copy_path):
        row_counted_paths.append(str(copy_path))
        return original_copy_file_row_count(copy_path)

    async def fake_build_membership_sidecars(
        *,
        provider_group_npi_path,
        provider_npi_group_path,
        provider_npi_scope_copy_path,
        input_paths,
    ):
        graph_input_paths.extend(input_paths)
        empty_dense_graph = struct.pack("<8sIQQ", b"PTG2MNDS", 1, 0, 0)
        provider_group_npi_path.write_bytes(empty_dense_graph)
        provider_npi_group_path.write_bytes(empty_dense_graph)
        provider_npi_scope_copy_path.write_text("scope-1\n")
        return {"edge_count": 1}

    monkeypatch.setattr(process_ptg, "ptg2_temp_parent", lambda: tmp_path)
    monkeypatch.setattr(process_ptg, "resolve_ptg2_artifact_dir", lambda: artifact_dir)
    monkeypatch.setattr(process_ptg, "_push_ptg2_objects", fake_push_ptg2_objects)
    monkeypatch.setattr(process_ptg, "flush_error_log", fake_flush_error_log)
    monkeypatch.setattr(process_ptg, "_aiter_compact_serving_records_rust", fake_scanner)
    monkeypatch.setattr(
        process_ptg,
        "_ptg2_copy_file_row_count",
        tracked_copy_file_row_count,
    )
    monkeypatch.setattr(
        process_ptg,
        "_build_ptg2_provider_membership_sidecars",
        fake_build_membership_sidecars,
    )

    summary = asyncio.run(
        process_ptg._parse_in_network_file_strict_v3(
            str(tmp_path / "rates.json.gz"),
            1,
            {"reporting_entity_name": "Example"},
            [{"plan_id": "plan", "plan_id_type": "ein", "plan_name": "Example PPO"}],
            test_mode=True,
            import_log_cls=SimpleNamespace(__name__="ImportLog"),
            source_url="https://example.test/rates.json.gz",
            source_version=ptg_domain.PTG2SourceVersion(
                source_identity_hash="source-identity",
                source_file_version_id="source-version",
                original_url="https://example.test/rates.json.gz",
                canonical_url="https://example.test/rates.json.gz",
                raw_sha256="a" * 64,
            ),
            snapshot_id="ptg2:test",
            coverage_scope_id="a" * 64,
            import_month=process_ptg.normalize_import_month("2026-07"),
            ptg2_manifest_stage_table="ptg2_stage_test",
        )
    )

    assert summary["serving_rates"] == 2
    assert summary["manifest"]["serving_rows"] == 2
    assert summary["manifest"]["copy_files"]["price_atom"] == [
        {"path": recovered_paths_by_kind["price"], "row_count": 1}
    ]
    assert summary["manifest"]["copy_files"]["price_set_summary"] == [
        {"path": recovered_paths_by_kind["summary"], "row_count": 1}
    ]
    assert summary["manifest"]["copy_files"]["provider_group_member"] == []
    assert graph_input_paths == [Path(recovered_paths_by_kind["member"])]
    assert not Path(recovered_paths_by_kind["member"]).exists()
    assert set(row_counted_paths) == {
        recovered_paths_by_kind["summary"],
        recovered_paths_by_kind["member"],
    }
    assert summary["manifest"]["copy_file_accounting"] == {
        "scanner_reported_files": 1,
        "scanner_duplicate_files": 0,
        "recovery_candidates": 8,
        "recovery_already_reported_files": 1,
        "recovered_unreported_files": 2,
        "fallback_row_count_files": 2,
        "fallback_row_count_bytes": len("price-set-1\t1.25\nmember-1\n"),
    }
    assert set(summary["manifest"]["sidecars"]) == {
        "provider_group_npi",
        "provider_npi_group",
    }


def test_scanner_error_labels_sigterm():
    message = ptg_rust_scanner._scanner_error_message(
        "PTG2 Rust compact scanner",
        -15,
        ["PTG2_DEDUPE_SUMMARY\tserving_rate_unique=146682732"],
    )

    assert "signal 15 (SIGTERM)" in message
    assert "PTG2_DEDUPE_SUMMARY" in message


def test_scanner_typed_witness_limit_frame_survives_stderr_context():
    error = ptg_rust_scanner._scanner_failure_exception(
        "PTG2 Rust compact scanner",
        1,
        [
            "scanner diagnostic",
            'PTG2_SCANNER_ERROR\t{"code":"witness_payload_limit",'
            '"message":"source witness exceeded its fail-closed budget"}',
        ],
    )

    assert isinstance(error, ptg_rust_scanner.WitnessPayloadLimitError)
    assert "fail-closed budget" in str(error)


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
