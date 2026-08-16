# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fail-closed edge coverage for canonical-address helpers."""

from __future__ import annotations

import re
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process.ext import address_canon


def _query_result(*, first=None):
    query_result = Mock()
    query_result.first.return_value = first
    return query_result


def test_unit_parser_rejects_unknown_prefix_suffix_and_street_value():
    pattern = re.compile(r"(\w+)\s+(\w+)(?:\s+(\w+))?")
    unknown_prefix = pattern.fullmatch("unknown 12")
    spaced_suffix = pattern.fullmatch("apt 12 a")
    street_value = pattern.fullmatch("apt avenue")
    assert unknown_prefix is not None
    assert spaced_suffix is not None
    assert street_value is not None

    assert address_canon._unit_from_match(unknown_prefix, 1, 2, 3) == ""
    assert address_canon._unit_from_match(spaced_suffix, 1, 2, 3) == ""
    assert address_canon._unit_from_match(street_value, 1, 2, 3) == ""


def test_floor_and_tail_helpers_fail_closed_on_empty_or_zero_values():
    assert address_canon._floor_value_norm(None) == ""
    assert address_canon._floor_value_norm("0007") == "7"
    assert address_canon._tail_unit(" 0 floor") is None
    assert address_canon._strip_duplicate_tail_unit(" 10 main st ", "") == (
        " 10 main st "
    )
    assert address_canon._repeated_line2_suffix_decision(
        "10 main street", "10 main street"
    ) is None
    assert address_canon._unit_decision("10 main street", "0 floor").unit == ""


def test_street_token_and_numeric_grid_edges_remain_structural():
    assert address_canon._street_token_norm("---") == ""
    assert address_canon._street_token_norm_context("---", 0, ["---"]) == ""
    assert address_canon.street_direction_token("10 Main Street", None) is None
    assert address_canon.numeric_grid_parts_v1("1548 4500", None) == (
        address_canon.NumericGridParts("1548", "", "4500", "")
    )
    assert address_canon.numeric_grid_parts_v1("1548 E 4500 Extra", None) is None


def test_sql_settings_and_gate_counts_reject_invalid_values(monkeypatch):
    with pytest.raises(ValueError, match="Unsafe SQL identifier"):
        address_canon._quote_ident("unsafe-name")
    with pytest.raises(ValueError, match="Unsafe PostgreSQL setting"):
        address_canon._setting_value("10s; reset all")
    monkeypatch.setenv("HLTHPRT_SYNTHETIC_POSITIVE", "0")
    with pytest.raises(ValueError, match="positive integer"):
        address_canon._positive_env_int("HLTHPRT_SYNTHETIC_POSITIVE", 1)

    assert address_canon._resolve_gate_violations(
        staged=1,
        distinct_keys=2,
        inserted=3,
        eligible_null_key_rows=1,
    ) == (
        "eligible_rows_missing_address_key",
        "distinct_keys_exceed_staged_rows",
        "inserted_rows_exceed_distinct_keys",
    )


@pytest.mark.asyncio
async def test_stamp_configuration_rejects_nonpositive_limits(monkeypatch):
    monkeypatch.setenv("HLTHPRT_ADDRESS_CANON_STAMP_SHARDS", "0")
    with pytest.raises(ValueError, match="STAMP_SHARDS"):
        await address_canon.stamp_address_keys("stage", {}, schema="mrf")

    monkeypatch.setenv("HLTHPRT_ADDRESS_CANON_STAMP_SHARDS", "1")
    monkeypatch.setenv("HLTHPRT_ADDRESS_CANON_STAMP_CONCURRENCY", "0")
    with pytest.raises(ValueError, match="STAMP_CONCURRENCY"):
        await address_canon.stamp_address_keys("stage", {}, schema="mrf")

    monkeypatch.setenv("HLTHPRT_ADDRESS_CANON_STAMP_CONCURRENCY", "1")
    monkeypatch.setenv("HLTHPRT_DB_POOL_MAX_SIZE", "0")
    with pytest.raises(ValueError, match="DB_POOL_MAX_SIZE"):
        await address_canon.stamp_address_keys("stage", {}, schema="mrf")


@pytest.mark.asyncio
async def test_child_key_propagation_rejects_nonpositive_limits(monkeypatch):
    monkeypatch.setenv("HLTHPRT_ADDRESS_CANON_STAMP_SHARDS", "0")
    with pytest.raises(ValueError, match="STAMP_SHARDS"):
        await address_canon.propagate_child_address_keys(
            "child", "parent", schema="mrf"
        )

    monkeypatch.setenv("HLTHPRT_ADDRESS_CANON_STAMP_SHARDS", "1")
    monkeypatch.setenv("HLTHPRT_ADDRESS_CANON_STAMP_CONCURRENCY", "0")
    with pytest.raises(ValueError, match="STAMP_CONCURRENCY"):
        await address_canon.propagate_child_address_keys(
            "child", "parent", schema="mrf"
        )


@pytest.mark.asyncio
async def test_zip_restore_short_circuits_and_validates_shards(monkeypatch):
    monkeypatch.setenv(address_canon.ADDRESS_ZIP_RESTORE_ENABLED_ENV, "0")
    assert (
        await address_canon.restore_missing_zip_from_tiger_zcta(
            "stage", {"zip": "postal_code"}, schema="mrf"
        )
        == 0
    )

    monkeypatch.setenv(address_canon.ADDRESS_ZIP_RESTORE_ENABLED_ENV, "1")
    assert (
        await address_canon.restore_missing_zip_from_tiger_zcta(
            "stage", {}, schema="mrf"
        )
        == 0
    )
    monkeypatch.setattr(address_canon.db, "transaction", Mock(), raising=False)
    monkeypatch.setenv(address_canon.ADDRESS_ZIP_RESTORE_SHARDS_ENV, "-1")
    with pytest.raises(ValueError, match="positive integer"):
        await address_canon.restore_missing_zip_from_tiger_zcta(
            "stage", {"zip": "postal_code"}, schema="mrf"
        )


def test_zip_restore_sql_scopes_keyed_rows_and_multiple_shards():
    sql = address_canon._zip_restore_sql(
        schema="mrf",
        staging_table="stage",
        restore_by_field={
            "zip_column": "postal_code",
            "latitude_column": "latitude",
            "longitude_column": "longitude",
            "state_expr": "state_name",
            "country_expr": "country_code",
        },
        shards=2,
        has_address_key=True,
        only_null_address_key=True,
    )

    assert sql.count("address_key IS NULL") == 2
    assert "mod(abs(hashtext(s.ctid::text)::bigint), :shards) = :shard" in sql


@pytest.mark.asyncio
async def test_rust_version_and_materializer_failures_fall_closed(monkeypatch, tmp_path):
    failed_process = SimpleNamespace(
        returncode=7,
        communicate=AsyncMock(return_value=(b"", b"synthetic failure")),
    )
    monkeypatch.setattr(
        address_canon.asyncio,
        "create_subprocess_exec",
        AsyncMock(return_value=failed_process),
    )
    with pytest.raises(RuntimeError, match="version check failed"):
        await address_canon._rust_canon_version(Path("scanner"))

    monkeypatch.setattr(address_canon, "_ptg2_rust_scanner_binary", Mock(return_value=None))
    with pytest.raises(FileNotFoundError, match="was not found"):
        await address_canon._run_rust_address_canonicalizer(
            tmp_path / "input", tmp_path / "output"
        )

    with pytest.raises(RuntimeError, match="canonicalizer failed"):
        await address_canon._run_rust_address_canonicalizer(
            tmp_path / "input", tmp_path / "output", binary=Path("scanner")
        )


@pytest.mark.asyncio
async def test_rust_version_mismatch_and_missing_copy_support_fall_back(monkeypatch):
    address_canon._RUST_CANON_VERSION_CACHE.clear()
    mismatched_version = address_canon.current_canon_version()
    mismatched_version["ruleset_version"] += 1
    monkeypatch.setattr(
        address_canon,
        "_rust_canon_version",
        AsyncMock(return_value=mismatched_version),
    )
    assert not await address_canon._is_rust_canon_version_current(Path("scanner"))

    monkeypatch.setenv(address_canon.ADDRESS_CANON_RUST_MATERIALIZE_ENV, "1")
    monkeypatch.setattr(
        address_canon,
        "_ptg2_rust_scanner_binary",
        Mock(return_value=Path("scanner")),
    )
    monkeypatch.setattr(
        address_canon,
        "_is_rust_canon_version_current",
        AsyncMock(return_value=True),
    )
    raw_connection = SimpleNamespace(driver_connection=SimpleNamespace())
    connection = SimpleNamespace(
        get_raw_connection=AsyncMock(return_value=raw_connection)
    )
    session = SimpleNamespace(connection=AsyncMock(return_value=connection))

    assert not await address_canon._has_rust_materialized_keys(
        session,
        keyed_table='"mrf"."keyed"',
        keyed_table_name="keyed",
        raw_copy_sql="SELECT 1",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("state_record", "error_message"),
    (
        (None, "singleton state is missing"),
        (SimpleNamespace(schema_version=3, active_ruleset_version=1), "schema version"),
        (SimpleNamespace(schema_version=2, active_ruleset_version=2), "ruleset"),
    ),
)
async def test_archive_alias_state_rejects_missing_or_unknown_contracts(
    state_record,
    error_message,
):
    session = SimpleNamespace(
        execute=AsyncMock(
            side_effect=(
                _query_result(),
                _query_result(first=state_record),
            )
        )
    )

    with pytest.raises(RuntimeError, match=error_message):
        await address_canon._validated_active_alias_state(session, "mrf")
