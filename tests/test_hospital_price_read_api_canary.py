# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from scripts.research import hospital_price_read_api_canary as canary


VERSION_ID = "a" * 64


def _args(**overrides):
    values_by_field = {
        "api_base_url": "https://api.example.test",
        "hospital_id": "hospital-000001",
        "code_type": "CPT",
        "code": "12345",
        "payer_name": None,
        "plan_name": None,
        "version_id": None,
        "limit": 25,
        "warmups": 1,
        "samples": 3,
        "minimum_scanned": 1,
        "minimum_items": 1,
        "timeout_seconds": 1.0,
        "header_env": [],
        "database_url_env": "HOSPITAL_PRICE_CANARY_DATABASE_URL",
        "database_schema": "mrf",
        "maximum_packed_payload_ratio": 0.2,
        "allow_insecure_http": False,
    }
    values_by_field.update(overrides)
    return SimpleNamespace(**values_by_field)


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
    bodies = [json.dumps(_payload()).encode()] * 4
    latencies = iter([50.0, 5.0, 7.0, 9.0])

    def sample(_url, _headers, _timeout):
        return next(latencies), bodies.pop(0), {"Cache-Control": "private, no-store"}

    monkeypatch.setattr(canary, "_http_sample", sample)
    receipt = canary._latency_receipt(_args(), "https://example.test", {})

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


def test_run_reports_payload_ratio_without_claiming_physical_overhead(
    monkeypatch,
):
    monkeypatch.setenv("HOSPITAL_PRICE_CANARY_DATABASE_URL", "postgresql://unused")
    monkeypatch.setattr(
        canary,
        "_latency_receipt",
        lambda *_args, **_kwargs: {
            "version_id": VERSION_ID,
            "samples": 3,
            "p95_ms": 9.0,
        },
    )

    async def storage(*_args):
        return {
            "measurement": (
                "version_scoped_native_block_payloads_excluding_heap_and_index_overhead"
            ),
            "source_content_bytes": 1_000,
            "packed_payload_bytes": 180,
            "packed_payload_ratio_to_source": 0.18,
        }

    monkeypatch.setattr(canary, "_storage_receipt", storage)
    receipt = canary.capture_canary_receipt(_args())

    assert receipt["status"] == "passed"
    assert receipt["contract"]["pagination_unit"] == "charges"
    assert receipt["contract"]["payer_omission"] == "charge_metadata_only"
    assert receipt["gates"]["packed_payload_ratio_passed"] is True
    assert "excluding_heap_and_index_overhead" in receipt["storage"]["measurement"]

    async def rounded_boundary(*_args):
        return {
            "measurement": (
                "version_scoped_native_block_payloads_excluding_heap_and_index_overhead"
            ),
            "source_content_bytes": 10_000_000,
            "packed_payload_bytes": 2_000_004,
            "packed_payload_ratio_to_source": 0.2,
        }

    monkeypatch.setattr(canary, "_storage_receipt", rounded_boundary)
    failed = canary.capture_canary_receipt(_args())
    assert failed["status"] == "gate_failed"
    assert failed["gates"]["packed_payload_ratio_passed"] is False
