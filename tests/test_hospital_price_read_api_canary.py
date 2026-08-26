# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest

from scripts.research import hospital_price_read_api_canary as canary
from scripts.research import hospital_price_canary_storage as storage_canary
from process.hospital_price_native import hospital_price_version_id


SOURCE_SHA256 = "b" * 64
VERSION_ID = hospital_price_version_id(SOURCE_SHA256)


class _StorageConnection:
    def __init__(self):
        self.closed = False

    async def fetchrow(self, query, *_args):
        if "content.byte_count" in query:
            return {
                "version_id": VERSION_ID,
                "content_sha256": SOURCE_SHA256,
                "byte_count": 1_000,
                "service_count": 1,
                "charge_count": 2,
                "fact_count": 3,
                "service_block_count": 2,
                "fact_block_count": 1,
                "code_selector_page_count": 1,
                "payer_plan_selector_page_count": 1,
            }
        return {
            "database_name": "healthporta",
            "database_oid": 42,
            "database_bytes": 9_000,
            "hospital_relation_bytes": 1_190,
            "target_root_count": 1,
            "comparison_root_count": 5,
            "comparison_root_digest": "c" * 32,
            "active_runs": 0,
            "active_attempts": 0,
        }

    async def fetch(self, _query, *_args):
        return [
            {"block_kind": 1, "block_count": 2, "payload_bytes": 100},
            {"block_kind": 2, "block_count": 1, "payload_bytes": 20},
            {"block_kind": 3, "block_count": 1, "payload_bytes": 30},
            {"block_kind": 4, "block_count": 1, "payload_bytes": 30},
        ]

    async def close(self):
        self.closed = True


def _args(**overrides):
    values_by_field = {
        "api_base_url": "https://api.example.test",
        "hospital_id": "hospital-000001",
        "code_type": "CPT",
        "code": "12345",
        "payer_name": None,
        "plan_name": None,
        "version_id": VERSION_ID,
        "limit": 25,
        "warmups": 1,
        "samples": 3,
        "minimum_scanned": 1,
        "minimum_items": 1,
        "timeout_seconds": 1.0,
        "header_env": [],
        "database_url_env": "HOSPITAL_PRICE_CANARY_DATABASE_URL",
        "database_schema": "mrf",
        "pre_import_receipt": Path("/unused/baseline.json"),
        "maximum_baseline_age_seconds": 21_600.0,
        "maximum_physical_storage_ratio": 0.2,
        "maximum_packed_payload_ratio": 0.2,
        "maximum_cold_ms": 100.0,
        "maximum_warm_p95_ms": 20.0,
        "allow_insecure_http": False,
    }
    values_by_field.update(overrides)
    return SimpleNamespace(**values_by_field)


def _baseline_receipt(**overrides):
    values_by_field = {
        "schema_version": 1,
        "contract": "hospital_price_quiescent_storage_baseline_v1",
        "captured_at": dt.datetime.now(dt.UTC).isoformat().replace("+00:00", "Z"),
        "database_name": "healthporta",
        "database_oid": 42,
        "database_schema": "mrf",
        "hospital_relation_bytes": 1_000,
        "comparison_root_count": 5,
        "comparison_root_digest": "c" * 32,
        "active_runs": 0,
        "active_attempts": 0,
        "expected_source_sha256": SOURCE_SHA256,
        "expected_source_bytes": 1_000,
        "expected_version_id": VERSION_ID,
    }
    values_by_field.update(overrides)
    return values_by_field


def _payload(*, payer=None, plan=None):
    has_payer_request = payer is not None
    return {
        "hospital_id": "hospital-000001",
        "version": {"version_id": VERSION_ID},
        "query": {
            "code_type": "CPT",
            "code": "12345",
            "payer_name": payer,
            "plan_name": plan,
            "negotiated_prices_requested": has_payer_request,
        },
        "pagination": {
            "unit": "charges",
            "limit": 25,
            "scanned": 1,
            "next_cursor": None,
        },
        "items": [
            {
                "service": {
                    "description": "Synthetic",
                    "codes": [{"code_type": "CPT", "code": "12345"}],
                },
                "charge": {"gross_charge": "100.00"},
                "negotiated_prices": (
                    [{"payer_name": payer, "plan_name": plan}]
                    if has_payer_request else []
                ),
            }
        ],
    }


def test_latency_receipt_is_populated_stable_and_charge_bounded(monkeypatch):
    bodies = [json.dumps(_payload()).encode()] * 5
    latencies = iter([50.0, 4.0, 5.0, 7.0, 9.0])

    def sample(_url, _headers, _timeout):
        return next(latencies), bodies.pop(0), {"Cache-Control": "private, no-store"}

    monkeypatch.setattr(canary, "_http_sample", sample)
    receipt = canary._latency_receipt(_args(), "https://example.test", {})

    assert receipt["cold_ms"] == 50.0
    assert receipt["samples"] == 3
    assert receipt["median_ms"] == 7.0
    assert receipt["p95_ms"] == 9.0
    assert receipt["minimum_scanned_charges"] == 1
    assert receipt["minimum_returned_charges"] == 1


def test_nested_facts_require_the_exact_payer_pair():
    args = _args(payer_name="Payer", plan_name="Plan")
    assert canary._response_values(
        _payload(payer="Payer", plan="Plan"), args=args
    ) == (VERSION_ID, 1, 1)

    malformed = _payload(payer="Payer", plan="Plan")
    malformed["items"][0]["negotiated_prices"][0]["plan_name"] = "Other"
    with pytest.raises(canary.CanaryError, match="nested fact"):
        canary._response_values(malformed, args=args)

    missing = _payload(payer="Payer", plan="Plan")
    missing["items"][0]["negotiated_prices"] = []
    with pytest.raises(canary.CanaryError, match="nested fact"):
        canary._response_values(missing, args=args)

    wrong_code = _payload(payer="Payer", plan="Plan")
    wrong_code["items"][0]["service"]["codes"][0]["code"] = "99999"
    with pytest.raises(canary.CanaryError, match="nested fact"):
        canary._response_values(wrong_code, args=args)


def _install_canary_observations(
    monkeypatch,
    *,
    physical_growth=190,
    packed_payload=250,
    cold_ms=50.0,
    warm_p95_ms=9.0,
):
    monkeypatch.setenv("HOSPITAL_PRICE_CANARY_DATABASE_URL", "postgresql://unused")
    monkeypatch.setattr(
        canary, "_load_baseline_receipt", lambda _path: _baseline_receipt()
    )
    monkeypatch.setattr(
        canary,
        "_latency_receipt",
        lambda *_args, **_kwargs: {
            "version_id": VERSION_ID,
            "cold_ms": cold_ms,
            "samples": 3,
            "p95_ms": warm_p95_ms,
        },
    )

    async def storage(*_args):
        return {
            "measurement": (
                "quiescent_pre_post_hospital_relations_including_heap_toast_and_indexes"
            ),
            "physical_growth_bytes": physical_growth,
            "unique_downloaded_source_bytes": 1_000,
            "source_content_bytes": 1_000,
            "packed_payload_bytes": packed_payload,
            "packed_payload_ratio_to_source": packed_payload / 1_000,
        }

    monkeypatch.setattr(canary, "_storage_receipt", storage)


def test_run_gates_physical_storage_not_payload_diagnostic(monkeypatch):
    _install_canary_observations(monkeypatch)
    receipt = canary.capture_canary_receipt(_args())

    assert receipt["status"] == "passed"
    assert receipt["contract"]["pagination_unit"] == "charges"
    assert receipt["contract"]["payer_omission"] == "charge_metadata_only"
    assert receipt["gates"]["physical_storage_ratio_passed"] is True
    assert receipt["gates"]["cold_latency_passed"] is True
    assert receipt["gates"]["warm_p95_latency_passed"] is True
    assert receipt["gates"]["packed_payload_ratio_diagnostic_passed"] is False
    assert "heap_toast_and_indexes" in receipt["storage"]["measurement"]

    _install_canary_observations(
        monkeypatch, physical_growth=201, packed_payload=100
    )
    failed = canary.capture_canary_receipt(_args())
    assert failed["status"] == "gate_failed"
    assert failed["gates"]["physical_storage_ratio_passed"] is False
    assert failed["gates"]["packed_payload_ratio_diagnostic_passed"] is True


def test_run_gates_cold_and_warm_latency(monkeypatch):
    _install_canary_observations(
        monkeypatch, cold_ms=101.0, warm_p95_ms=21.0
    )
    latency_failed = canary.capture_canary_receipt(_args())
    assert latency_failed["status"] == "gate_failed"
    assert latency_failed["gates"]["cold_latency_passed"] is False
    assert latency_failed["gates"]["warm_p95_latency_passed"] is False


@pytest.mark.asyncio
async def test_storage_receipt_binds_database_blocks_and_physical_growth(monkeypatch):
    connection = _StorageConnection()

    async def connect(dsn, **options):
        assert dsn == "postgresql://db"
        assert options == {"timeout": 2.0, "command_timeout": 2.0}
        return connection

    monkeypatch.setitem(sys.modules, "asyncpg", SimpleNamespace(connect=connect))
    receipt = await storage_canary.capture_storage_receipt(
        "postgresql+asyncpg://db",
        "mrf",
        VERSION_ID,
        _baseline_receipt(),
        2.0,
        21_600.0,
    )

    assert connection.closed is True
    assert receipt["version_id"] == VERSION_ID
    assert receipt["content_sha256"] == SOURCE_SHA256
    assert receipt["physical_growth_bytes"] == 190
    assert receipt["physical_storage_ratio_to_unique_source"] == 0.19
    assert receipt["packed_payload_ratio_to_source"] == 0.18


def test_storage_receipt_rejects_incomplete_or_unbound_evidence():
    version_by_field = {
        "version_id": VERSION_ID,
        "content_sha256": SOURCE_SHA256,
        "byte_count": 1_000,
        "service_block_count": 2,
        "fact_block_count": 1,
        "code_selector_page_count": 1,
        "payer_plan_selector_page_count": 1,
    }
    blocks = [
        {"block_kind": 1, "block_count": 2, "payload_bytes": 100},
        {"block_kind": 2, "block_count": 1, "payload_bytes": 20},
        {"block_kind": 3, "block_count": 1, "payload_bytes": 30},
        {"block_kind": 4, "block_count": 1, "payload_bytes": 30},
    ]
    physical_by_field = {
        "database_name": "healthporta",
        "database_oid": 42,
        "database_bytes": 9_000,
        "hospital_relation_bytes": 1_190,
        "target_root_count": 1,
        "comparison_root_count": 5,
        "comparison_root_digest": "c" * 32,
        "active_runs": 0,
        "active_attempts": 0,
    }

    cases = [
        (None, blocks, physical_by_field, _baseline_receipt(), "unavailable"),
        ({**version_by_field, "byte_count": 0}, blocks, physical_by_field,
         _baseline_receipt(), "byte counts"),
        (version_by_field, blocks, None, _baseline_receipt(), "physical"),
        (version_by_field, blocks[:-1], physical_by_field,
         _baseline_receipt(), "block counts"),
        (
            version_by_field, blocks, {**physical_by_field, "active_runs": 1},
            _baseline_receipt(), "quiescent",
        ),
        (
            version_by_field, blocks, physical_by_field,
            _baseline_receipt(expected_source_sha256="d" * 64), "bound",
        ),
        (
            version_by_field, blocks,
            {**physical_by_field, "hospital_relation_bytes": 1_000},
            _baseline_receipt(), "attributable",
        ),
    ]
    for version_case, blocks_case, physical_case, baseline_case, message in cases:
        with pytest.raises(storage_canary.CanaryError, match=message):
            storage_canary._validated_storage_receipt(
                version_case,
                blocks_case,
                physical_case,
                baseline_case,
                "mrf",
                21_600.0,
            )


@pytest.mark.asyncio
async def test_storage_baseline_is_source_new_quiescent_and_timeout_bounded(monkeypatch):
    connection = _StorageConnection()

    async def physical_only(query, *_args):
        assert "hospital_price_packed_root" in query
        evidence = await _StorageConnection().fetchrow(query)
        return {**evidence, "target_root_count": 0}

    connection.fetchrow = physical_only

    async def connect(dsn, **options):
        assert dsn == "postgresql://db"
        assert options == {"timeout": 3.0, "command_timeout": 3.0}
        return connection

    monkeypatch.setitem(sys.modules, "asyncpg", SimpleNamespace(connect=connect))
    receipt = await storage_canary.capture_storage_baseline(
        "postgresql://db", "mrf", SOURCE_SHA256, 1_000, 3.0
    )

    assert connection.closed is True
    assert receipt["expected_version_id"] == VERSION_ID
    assert receipt["expected_source_bytes"] == 1_000
    assert receipt["hospital_relation_bytes"] == 1_190


def test_pre_import_receipt_loader_fails_closed(tmp_path):
    valid = tmp_path / "baseline.json"
    valid.write_text(json.dumps(_baseline_receipt()), encoding="utf-8")
    assert canary._load_baseline_receipt(valid)["expected_version_id"] == VERSION_ID

    invalid = tmp_path / "invalid.json"
    invalid.write_text("[]", encoding="utf-8")
    with pytest.raises(canary.CanaryError, match="invalid"):
        canary._load_baseline_receipt(invalid)
