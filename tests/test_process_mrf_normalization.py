# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import datetime
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import importlib
import os

from asyncpg.exceptions import DeadlockDetectedError
import pytest
from sqlalchemy import BigInteger

os.environ.setdefault("HLTHPRT_REDIS_ADDRESS", "redis://localhost")

process_pkg = importlib.import_module("process")
process_initial = importlib.import_module("process.initial")
process_npi = importlib.import_module("process.npi")
utils_module = importlib.import_module("process.ext.utils")

def test_parallel_download_disabled_host_matching(monkeypatch):
    monkeypatch.setenv(
        "HLTHPRT_PARALLEL_DOWNLOAD_DISABLED_HOSTS",
        "www22.elevancehealth.com,.blocked.example",
    )

    assert utils_module._parallel_download_disabled_for_url(
        "https://www22.elevancehealth.com/cms/PROVIDERS_TX_2_OF_2.json"
    )
    assert utils_module._parallel_download_disabled_for_url(
        "https://files.blocked.example/provider.json"
    )
    assert not utils_module._parallel_download_disabled_for_url(
        "https://www.example.com/provider.json"
    )


def test_extract_plan_years_from_years_array():
    payload = {"years": [2024, "2025", "2025.0", 2026.0, "bad", None]}
    assert process_initial._extract_plan_years(payload) == [2024, 2025, 2026]


def test_extract_plan_years_from_year_scalar():
    payload = {"year": "2026"}
    assert process_initial._extract_plan_years(payload) == [2026]


def test_extract_plan_years_invalid_or_missing():
    assert process_initial._extract_plan_years({"years": "bad"}) == []
    assert process_initial._extract_plan_years({}) == []


def test_normalize_marketplace_benefits_scalar_bool():
    rows = process_initial._normalize_marketplace_benefits(
        "12345XX9876543",
        2026,
        12345,
        [{"telemedicine": False}],
        datetime.datetime(2026, 1, 1),
    )

    assert len(rows) == 1
    row = rows[0]
    assert row["plan_id"] == "12345XX9876543"
    assert row["year"] == 2026
    assert row["issuer_id"] == 12345
    assert row["benefit_name"] == "telemedicine"
    assert row["benefit_value_bool"] is False
    assert row["benefit_value_text"] == "false"
    assert row["benefit_item_json"] == {"telemedicine": False}


def test_normalize_marketplace_benefits_named_value_shape():
    rows = process_initial._normalize_marketplace_benefits(
        "12345XX9876543",
        2026,
        12345,
        [{"name": "virtual_primary_care", "value": True, "label": "Virtual Primary Care"}],
        datetime.datetime(2026, 1, 1),
    )

    assert len(rows) == 1
    row = rows[0]
    assert row["benefit_name"] == "virtual_primary_care"
    assert row["benefit_label"] == "Virtual Primary Care"
    assert row["benefit_value_bool"] is True


def test_normalize_marketplace_address_entry_accepts_address2():
    row = process_initial._normalize_marketplace_address_entry(
        {
            "address": "123 Main St",
            "address2": "Suite 5",
            "city": "Austin",
            "state": "tx",
            "zip": "78701",
            "phone": "5125550000",
        }
    )

    assert row is not None
    assert row["first_line"] == "123 Main St"
    assert row["second_line"] == "Suite 5"
    assert row["city_name"] == "AUSTIN"
    assert row["state_name"] == "TX"
    assert row["postal_code"] == "78701"
    assert row["telephone_number"] == "5125550000"


def test_build_mrf_address_rows_creates_address_and_evidence():
    address_rows, evidence_rows = process_initial._build_mrf_address_rows(
        {
            "npi": "1234567890",
            "addresses": [
                {
                    "address": "123 Main St",
                    "address2": "Suite 5",
                    "city": "Austin",
                    "state": "TX",
                    "zip": "78701",
                    "phone": "5125550000",
                }
            ],
        },
        {
            1: {
                "issuer_id": 12345,
                "year": 2026,
                "checksum_network": 111,
                "network_tier": "PREFERRED",
            },
            2: {
                "issuer_id": 54321,
                "year": 2026,
                "checksum_network": 222,
                "network_tier": "NON-PREFERRED",
            },
        },
        "20260402",
        "https://issuer.example/providers.json",
        datetime.datetime(2026, 1, 1),
        issuer_lookup={12345: "Alpha Health Plan", 54321: "Beta Health Plan"},
    )

    assert len(address_rows) == 1
    assert len(evidence_rows) == 2
    address_row = address_rows[0]
    assert address_row["npi"] == 1234567890
    assert address_row["type"] == "practice"
    assert "source_count" not in address_row
    assert "address_sources" not in address_row
    assert "source_import_dates" not in address_row
    assert "source_issuer_ids" not in address_row
    assert "source_issuer_names" not in address_row
    assert "source_urls" not in address_row
    expected_address_key = process_initial.address_key_v1(
        "123 Main St",
        "Suite 5",
        "AUSTIN",
        "TX",
        "78701",
        "US",
    )
    assert address_row["address_key"] == expected_address_key
    evidence_row = sorted(evidence_rows, key=lambda item: item["issuer_id"])[0]
    assert evidence_row["issuer_name"] == "Alpha Health Plan"
    assert evidence_row["import_date"] == datetime.date(2026, 4, 2)
    assert evidence_row["address_key"] == expected_address_key


def _mrf_contact_batch_fixture():
    return (
        {
            "npi": "1234567890",
            "addresses": [
                {
                    "address": "123 Main St",
                    "city": "Austin",
                    "state": "TX",
                    "zip": "78701",
                    "phone": "512-555-0000",
                    "fax": "512-555-0199",
                },
                {
                    "address": "124 Main St",
                    "city": "Austin",
                    "state": "TX",
                    "zip": "78701",
                    "phone": "512-555-0001 x12",
                },
            ],
        },
        {
            1: {
                "issuer_id": 12345,
                "year": 2026,
                "checksum_network": 111,
                "network_tier": "PREFERRED",
            },
        },
    )


def test_build_mrf_address_rows_batches_contact_normalization(monkeypatch):
    """Verify build mrf address rows batches contact normalization."""
    seen_batches = []

    def fake_canonicalize_contact_batch(rows):
        rows = list(rows)
        seen_batches.append(rows)
        return [
            {
                "phone_number": "5125550000",
                "phone_extension": None,
                "fax_number_digits": "5125550199",
                "fax_extension": None,
            },
            {
                "phone_number": "5125550001",
                "phone_extension": "12",
                "fax_number_digits": None,
                "fax_extension": None,
            },
        ]

    monkeypatch.setattr(process_initial, "canonicalize_contact_batch", fake_canonicalize_contact_batch)

    provider_payload, plan_lookup = _mrf_contact_batch_fixture()
    address_rows, _evidence_rows = process_initial._build_mrf_address_rows(
        provider_payload,
        plan_lookup,
        "20260601000000",
        "https://example.test/provider.json",
        datetime.datetime(2026, 6, 1, 12, 0, 0),
    )

    assert seen_batches == [
        [
            ("512-555-0000", "512-555-0199", "US"),
            ("512-555-0001 x12", None, "US"),
        ]
    ]
    assert [source_row["phone_number"] for source_row in address_rows] == ["5125550000", "5125550001"]
    assert address_rows[1]["phone_extension"] == "12"


def test_build_mrf_address_rows_can_skip_deferred_aggregate_rows():
    provider_payload, plan_lookup = _mrf_contact_batch_fixture()

    address_rows, evidence_rows = process_initial._build_mrf_address_rows(
        provider_payload,
        plan_lookup,
        "20260601000000",
        "https://example.test/provider.json",
        datetime.datetime(2026, 6, 1, 12, 0, 0),
        include_address_rows=False,
    )

    assert address_rows == []
    assert len(evidence_rows) == 2


@pytest.mark.asyncio
async def test_push_mrf_address_rows_skips_aggregate_ingest_by_default(monkeypatch):
    calls = []

    async def fake_push_objects(rows, cls, **kwargs):
        calls.append((rows, cls, kwargs))

    monkeypatch.delenv("HLTHPRT_MRF_ADDRESS_AGGREGATE_DURING_INGEST", raising=False)
    monkeypatch.setattr(process_initial, "push_objects", fake_push_objects)

    await process_initial._push_mrf_address_rows(
        [{"npi": 1234567890, "type": "practice", "checksum": 1}],
        SimpleNamespace(__tablename__="mrf_address_20260612"),
    )

    assert calls == []


@pytest.mark.asyncio
async def test_push_mrf_address_rows_uses_insert_do_nothing_when_enabled(monkeypatch):
    calls = []

    async def fake_push_objects(source_rows, cls, **kwargs):
        calls.append((source_rows, cls, kwargs))

    monkeypatch.setenv("HLTHPRT_MRF_ADDRESS_AGGREGATE_DURING_INGEST", "1")
    monkeypatch.setattr(process_initial, "push_objects", fake_push_objects)

    cls = SimpleNamespace(__tablename__="mrf_address_20260612")
    source_rows = [
        {
            "npi": 1234567890,
            "type": "practice",
            "checksum": 1,
            "first_line": "123 Main St",
            "source_count": 2,
            "source_issuer_ids": [12345, 54321],
            "source_urls": ["https://issuer.example/providers.json"],
        }
    ]
    await process_initial._push_mrf_address_rows(source_rows, cls)

    assert calls == [
        (
            [{"npi": 1234567890, "type": "practice", "checksum": 1, "first_line": "123 Main St"}],
            cls,
            {"rewrite": False, "use_copy": False},
        )
    ]


@pytest.mark.asyncio
async def test_push_mrf_duplicate_tolerant_rows_uses_staged_copy(monkeypatch):
    calls = []

    async def fake_copy_ignore(rows, cls, **kwargs):
        calls.append((rows, cls, kwargs))

    monkeypatch.delenv("HLTHPRT_MRF_COPY_FIRST_DUPLICATE_TOLERANT_INSERTS", raising=False)
    monkeypatch.setattr(process_initial, "_copy_ignore_objects", fake_copy_ignore)

    cls = SimpleNamespace(__tablename__="plan_npi_raw_20260612")
    rows = [{"npi": 1234567890, "checksum_network": 42}]
    await process_initial._push_mrf_duplicate_tolerant_rows(rows, cls)

    assert calls == [(rows, cls, {"bind_sqlalchemy_types": True})]


@pytest.mark.asyncio
async def test_push_mrf_duplicate_tolerant_rows_falls_back_without_copy(monkeypatch):
    calls = []

    async def unavailable_copy(*_args, **_kwargs):
        raise NotImplementedError("copy unavailable")

    async def fake_push_objects(rows, cls, **kwargs):
        calls.append((rows, cls, kwargs))

    monkeypatch.delenv("HLTHPRT_MRF_COPY_FIRST_DUPLICATE_TOLERANT_INSERTS", raising=False)
    monkeypatch.setattr(process_initial, "_copy_ignore_objects", unavailable_copy)
    monkeypatch.setattr(process_initial, "push_objects", fake_push_objects)

    cls = SimpleNamespace(__tablename__="plan_npi_raw_20260612")
    rows = [{"npi": 1234567890, "checksum_network": 42}]
    await process_initial._push_mrf_duplicate_tolerant_rows(rows, cls)

    assert calls == [(rows, cls, {"rewrite": False, "use_copy": False})]


@pytest.mark.asyncio
async def test_push_mrf_duplicate_tolerant_rows_can_restore_copy_first(monkeypatch):
    calls = []

    async def fake_push_objects(rows, cls, **kwargs):
        calls.append((rows, cls, kwargs))

    monkeypatch.setenv("HLTHPRT_MRF_COPY_FIRST_DUPLICATE_TOLERANT_INSERTS", "1")
    monkeypatch.setattr(process_initial, "push_objects", fake_push_objects)

    cls = SimpleNamespace(__tablename__="plan_npi_raw_20260612")
    rows = [{"npi": 1234567890, "checksum_network": 42}]
    await process_initial._push_mrf_duplicate_tolerant_rows(rows, cls)

    assert calls == [(rows, cls, {})]


@pytest.mark.asyncio
async def test_save_mrf_data_skips_mrf_address_aggregate_ingest(monkeypatch):
    calls = []

    async def fake_push_objects(rows, cls, **kwargs):
        calls.append((cls.__tablename__, rows, kwargs))

    async def fake_ensure_database(_test_mode):
        return None

    def fake_make_class(cls, suffix, schema_override=None):
        return SimpleNamespace(__tablename__=f"{cls.__tablename__}_{suffix}")

    monkeypatch.setattr(process_initial, "push_objects", fake_push_objects)
    monkeypatch.setattr(process_initial, "ensure_database", fake_ensure_database)
    monkeypatch.setattr(process_initial, "make_class", fake_make_class)
    monkeypatch.delenv("HLTHPRT_MRF_ADDRESS_AGGREGATE_DURING_INGEST", raising=False)

    rows = [{"npi": 1234567890, "type": "practice", "checksum": 1}]
    await process_initial.save_mrf_data(
        {"context": {"import_date": "20260612", "test_mode": False}},
        {"mrf_address": rows, "mrf_address_evidence": [{"evidence_checksum": 2}]},
    )

    assert calls == [("mrf_address_evidence_20260612", [{"evidence_checksum": 2}], {})]
