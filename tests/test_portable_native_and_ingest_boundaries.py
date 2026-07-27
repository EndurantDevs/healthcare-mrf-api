# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Portable optional-native and provider-quality ingestion contracts."""

from __future__ import annotations

import importlib
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.ext import address_canon, address_fast

provider_quality = importlib.import_module("process.provider_quality")


def _address_row() -> tuple[str, str, str, str, str, str]:
    return ("123 Main St", "Suite 2", "Austin", "TX", "78701-1234", "US")


def test_address_fast_validates_optional_values_and_row_width() -> None:
    assert address_fast._as_optional_text(None) is None
    with pytest.raises(ValueError, match="must have 6 fields"):
        address_fast._row_tuple(("too", "short"))


def test_address_fast_module_accepts_only_matching_contract(monkeypatch) -> None:
    current_version = address_canon.current_canon_version()
    mismatch_module = SimpleNamespace(
        canon_version=lambda: {
            **current_version,
            "ruleset_version": current_version["ruleset_version"] - 1,
        }
    )
    monkeypatch.setattr(
        address_fast.importlib,
        "import_module",
        lambda _name: mismatch_module,
    )
    address_fast._fast_module.cache_clear()
    assert address_fast._fast_module() is None

    compatible_module = SimpleNamespace(canon_version=lambda: current_version)
    monkeypatch.setattr(
        address_fast.importlib,
        "import_module",
        lambda _name: compatible_module,
    )
    address_fast._fast_module.cache_clear()
    assert address_fast._fast_module() is compatible_module
    address_fast._fast_module.cache_clear()


def test_address_fast_native_success_and_failure_have_identical_fallback(
    monkeypatch,
) -> None:
    native_result_by_field = {"address_key": "native"}
    compatible_module = SimpleNamespace(
        canonicalize_batch=lambda _rows: [native_result_by_field],
    )
    monkeypatch.setattr(address_fast, "_fast_module", lambda: compatible_module)
    assert address_fast.canonicalize_batch([_address_row()]) == [
        native_result_by_field
    ]

    def failed_batch(_rows):
        raise RuntimeError("native failure")

    monkeypatch.setattr(
        address_fast,
        "_fast_module",
        lambda: SimpleNamespace(canonicalize_batch=failed_batch),
    )
    fallback_result = address_fast.canonicalize_batch([_address_row()])[0]
    assert fallback_result["zip4"] == "1234"
    assert fallback_result["country_code"] == "US"


@pytest.mark.asyncio
async def test_qpp_ingest_skips_invalid_rows_and_flushes_test_batch(
    monkeypatch,
    tmp_path,
) -> None:
    source_path = tmp_path / "qpp.csv"
    source_path.write_text(
        "npi,year,quality_score,cost_score,final_score\n"
        ",2024,1,2,3\n"
        "1234567890,2024,90%,80,85\n",
        encoding="utf-8",
    )
    pushed_rows = AsyncMock()
    monkeypatch.setattr(provider_quality, "_push_objects_with_retry", pushed_rows)
    monkeypatch.setattr(provider_quality, "IMPORT_BATCH_SIZE", 1)
    monkeypatch.setattr(provider_quality, "ROW_PROGRESS_INTERVAL_SECONDS", 0)
    monkeypatch.setattr(provider_quality, "PROVIDER_QUALITY_TEST_QPP_ROWS", 1)

    await provider_quality._load_qpp_rows(
        str(source_path),
        object,
        2024,
        test_mode=True,
    )

    assert pushed_rows.await_count == 1
    rows = pushed_rows.await_args.args[0]
    assert rows[0]["npi"] == 1234567890
    assert rows[0]["quality_score"] == 90.0


@pytest.mark.asyncio
async def test_qpp_ingest_flushes_remaining_production_rows(
    monkeypatch,
    tmp_path,
) -> None:
    source_path = tmp_path / "qpp.csv"
    source_path.write_text(
        "npi,year,quality_score,cost_score,final_score\n"
        "1234567890,,90,80,85\n",
        encoding="utf-8",
    )
    pushed_rows = AsyncMock()
    monkeypatch.setattr(provider_quality, "_push_objects_with_retry", pushed_rows)
    monkeypatch.setattr(provider_quality, "IMPORT_BATCH_SIZE", 10)
    monkeypatch.setattr(provider_quality, "ROW_PROGRESS_INTERVAL_SECONDS", 3600)

    await provider_quality._load_qpp_rows(
        str(source_path),
        object,
        2023,
        test_mode=False,
    )

    assert pushed_rows.await_count == 1
    assert pushed_rows.await_args.args[0][0]["year"] == 2023


@pytest.mark.asyncio
async def test_svi_ingest_records_missing_keys_then_flushes_valid_test_row(
    monkeypatch,
    tmp_path,
) -> None:
    source_path = tmp_path / "svi.csv"
    source_path.write_text(
        "ZCTA,year,RPL_THEMES,RPL_THEME1,RPL_THEME2,RPL_THEME3,RPL_THEME4\n"
        ",2024,0.1,0.2,0.3,0.4,0.5\n"
        "78701,2024,0.6,0.7,0.8,0.9,1.0\n",
        encoding="utf-8",
    )
    pushed_rows = AsyncMock()
    monkeypatch.setattr(provider_quality, "_push_objects_with_retry", pushed_rows)
    monkeypatch.setattr(provider_quality, "IMPORT_BATCH_SIZE", 1)
    monkeypatch.setattr(provider_quality, "ROW_PROGRESS_INTERVAL_SECONDS", 0)
    monkeypatch.setattr(provider_quality, "PROVIDER_QUALITY_TEST_SVI_ROWS", 1)

    await provider_quality._load_svi_rows(
        str(source_path),
        object,
        2024,
        test_mode=True,
    )

    assert pushed_rows.await_count == 1
    assert pushed_rows.await_args.args[0][0]["zcta"] == "78701"


@pytest.mark.asyncio
async def test_svi_ingest_reports_proven_empty_source(monkeypatch, tmp_path) -> None:
    source_path = tmp_path / "svi-empty.csv"
    source_path.write_text(
        "FIPS,year,RPL_THEMES\n"
        "invalid,2024,0.1\n",
        encoding="utf-8",
    )
    pushed_rows = AsyncMock()
    monkeypatch.setattr(provider_quality, "_push_objects_with_retry", pushed_rows)
    monkeypatch.setattr(provider_quality, "ROW_PROGRESS_INTERVAL_SECONDS", 3600)

    await provider_quality._load_svi_rows(
        str(source_path),
        object,
        2024,
        test_mode=False,
    )

    pushed_rows.assert_not_awaited()
