# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import importlib
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from process.ptg_parts import snapshot_cleanup
from process.ptg_parts import config as ptg_config


process_ptg = importlib.import_module("process.ptg")

_AUDIT_ROW = {
    "occurrence_id": b"o" * 32,
    "code_key": 1,
    "provider_set_key": 2,
    "price_key": 3,
    "source_key": 0,
    "npi": 1234567890,
    "atom_ordinal": 0,
    "atom_key": 4,
}


def _audit_rows():
    return [dict(_AUDIT_ROW)]


def _layout_query(rows):
    return AsyncMock(side_effect=[rows, _audit_rows()])


def test_snapshot_arch_accepts_only_the_exact_v3_name(monkeypatch):
    monkeypatch.delenv(ptg_config.PTG2_SNAPSHOT_ARCH_ENV, raising=False)
    assert ptg_config._ptg2_snapshot_arch_from_env() == "postgres_binary_v3"

    monkeypatch.setenv(ptg_config.PTG2_SNAPSHOT_ARCH_ENV, "postgres_binary_v3")
    assert ptg_config._ptg2_snapshot_arch_from_env() == "postgres_binary_v3"

    monkeypatch.setenv(ptg_config.PTG2_SNAPSHOT_ARCH_ENV, "postgres-binary-v3")
    with pytest.raises(ValueError, match="only 'postgres_binary_v3'"):
        ptg_config._ptg2_snapshot_arch_from_env()


def test_strict_v3_defaults_use_the_measured_high_throughput_profile():
    assert ptg_config.PTG2_DEFAULT_RUST_WORKERS == 16
    assert ptg_config.PTG2_DEFAULT_RUST_WORK_QUEUE == 32
    assert ptg_config.PTG2_DEFAULT_RUST_EVENT_QUEUE == 128
    assert ptg_config.PTG2_DEFAULT_RUST_SPLIT_NEGOTIATED_RATES == 8192
    assert ptg_config.PTG2_DEFAULT_MANIFEST_DIRECT_COPY_TASKS == 16


def test_retired_importer_modules_and_entry_points_are_absent():
    root = Path(__file__).resolve().parents[1]
    retired_paths = (
        "process/ptg_parts/ptg2_serving_binary.py",
        "process/ptg_parts/ptg2_serving_binary_v3_writer.py",
        "process/ptg_parts/serving_index.py",
    )
    assert not [path for path in retired_paths if (root / path).exists()]

    process_names = set(process_ptg._process_in_network_file.__code__.co_names)
    assert "_parse_in_network_file_strict_v3" in process_names
    assert not {
        "_parse_in_network_file_serving_only",
        "_parse_in_network_file_single_pass",
        "_parse_in_network_file_compact",
    } & process_names
    assert not hasattr(
        importlib.import_module("process.ptg_parts.ptg2_manifest_publish"),
        "_publish_ptg2_manifest_serving_snapshot",
    )


@pytest.mark.asyncio
async def test_strict_v3_import_rejects_retired_provider_lane_before_database_work():
    with pytest.raises(ValueError, match="provider_ref_url is not supported"):
        await process_ptg.main(
            source_key="source-a",
            provider_ref_url="https://example.invalid/providers.json.gz",
        )


def _serving_index(*, npi_count: int = 3) -> dict[str, object]:
    audit_sample = {
        "contract": "persisted_served_occurrence_sample_v2",
        "format_version": 2,
        "method": "publish_time_stratified_v1",
        "sample_count": 1,
        "maximum_rows": 2560,
        "complete_population": False,
        "sample_digest": snapshot_cleanup.persisted_audit_sample_digest(_audit_rows()),
        "serving_multiplicity_semantics": "source_multiset_v1",
    }
    return {
        "arch_version": "postgres_binary_v3",
        "storage_generation": "shared_blocks_v3",
        "cold_lookup_contract": "ptg_v3_cold_v2",
        "price_membership_semantics": "multiset_v1",
        "serving_multiplicity_semantics": "source_multiset_v1",
        "shared_snapshot_key": 17,
        "coverage_scope_id": "c" * 64,
        "serving_rates": 91,
        "atom_key_bits": 24,
        "audit_sample": audit_sample,
        "provider_graph": {
            "owner_count": 8,
            "provider_group_count": 5,
            "npi_count": npi_count,
        },
    }


def _layout_row(serving_index: dict[str, object]) -> dict[str, object]:
    return {
        "state": "sealed",
        "generation": "shared_blocks_v3",
        "layout_manifest": {"serving_index": dict(serving_index)},
        "mapping_digest": b"m" * 32,
        "support_digest": b"s" * 32,
        "mapping_count": 12,
        "resolved_mapping_count": 12,
        "has_graph_owner": True,
        "has_code": True,
        "code_count": 5,
        "code_scope_count": 1,
        "matching_code_scope_count": 5,
        "has_provider_group": True,
        "has_provider_set": True,
        "has_price_attr": True,
        "has_npi_scope": True,
        "scope_count": 1,
        "matching_scope_count": 1,
    }


@pytest.mark.asyncio
async def test_strict_v3_snapshot_validation_accepts_one_sealed_complete_binding(
    monkeypatch,
):
    serving_index = _serving_index()
    monkeypatch.setattr(snapshot_cleanup, "_table_exists", AsyncMock(return_value=True))
    query = _layout_query([_layout_row(serving_index)])
    monkeypatch.setattr(snapshot_cleanup.db, "all", query)

    missing_tables, contract_errors = (
        await snapshot_cleanup._missing_snapshot_serving_resources(
            "mrf",
            "ptg2:strict-v3",
            serving_index,
        )
    )

    assert missing_tables == []
    assert contract_errors == []
    assert query.await_args_list[0].kwargs == {
        "snapshot_id": "ptg2:strict-v3",
        "shared_snapshot_key": 17,
        "coverage_scope_id": b"\xcc" * 32,
    }
    assert query.await_args_list[1].kwargs == {
        "shared_snapshot_key": 17,
        "row_limit": 2561,
    }
    audit_query = query.await_args_list[1].args[0]
    assert "source_key, npi, atom_ordinal" in audit_query


@pytest.mark.asyncio
async def test_strict_v3_snapshot_validation_rejects_legacy_manifest_before_query(
    monkeypatch,
):
    table_exists = AsyncMock(return_value=True)
    query = AsyncMock()
    monkeypatch.setattr(snapshot_cleanup, "_table_exists", table_exists)
    monkeypatch.setattr(snapshot_cleanup.db, "all", query)

    missing_tables, contract_errors = (
        await snapshot_cleanup._missing_snapshot_serving_resources(
            "mrf",
            "ptg2:legacy",
            {"arch_version": "postgres_binary_v2"},
        )
    )

    assert missing_tables == []
    assert set(contract_errors) == {
        "arch_version",
        "storage_generation",
        "cold_lookup_contract",
        "price_membership_semantics",
        "serving_multiplicity_semantics",
        "shared_snapshot_key",
        "coverage_scope_id",
        "audit_sample",
    }
    query.assert_not_awaited()


@pytest.mark.asyncio
async def test_strict_v3_snapshot_validation_detects_incomplete_physical_layout(
    monkeypatch,
):
    serving_index = _serving_index()
    row = _layout_row(serving_index)
    row.update(
        {
            "resolved_mapping_count": 11,
            "has_price_attr": False,
            "layout_manifest": {
                "serving_index": {**serving_index, "serving_rates": 90}
            },
        }
    )
    monkeypatch.setattr(snapshot_cleanup, "_table_exists", AsyncMock(return_value=True))
    monkeypatch.setattr(snapshot_cleanup.db, "all", _layout_query([row]))

    _missing_tables, contract_errors = (
        await snapshot_cleanup._missing_snapshot_serving_resources(
            "mrf",
            "ptg2:broken-v3",
            serving_index,
        )
    )

    assert set(contract_errors) == {
        "unresolved_snapshot_blocks",
        "layout_manifest:serving_rates",
        "dense:price_attr",
    }


@pytest.mark.asyncio
async def test_strict_v3_snapshot_validation_allows_zero_npi_layout(
    monkeypatch,
):
    serving_index = _serving_index(npi_count=0)
    row = _layout_row(serving_index)
    row["has_npi_scope"] = False
    monkeypatch.setattr(snapshot_cleanup, "_table_exists", AsyncMock(return_value=True))
    monkeypatch.setattr(snapshot_cleanup.db, "all", _layout_query([row]))

    _missing_tables, contract_errors = (
        await snapshot_cleanup._missing_snapshot_serving_resources(
            "mrf",
            "ptg2:tin-only-v3",
            serving_index,
        )
    )

    assert contract_errors == []


@pytest.mark.asyncio
async def test_strict_v3_snapshot_validation_rejects_missing_or_wrong_scope_binding(
    monkeypatch,
):
    serving_index = _serving_index()
    row = _layout_row(serving_index)
    row.update({"scope_count": 2, "matching_scope_count": 0})
    monkeypatch.setattr(snapshot_cleanup, "_table_exists", AsyncMock(return_value=True))
    monkeypatch.setattr(snapshot_cleanup.db, "all", _layout_query([row]))

    _missing_tables, contract_errors = (
        await snapshot_cleanup._missing_snapshot_serving_resources(
            "mrf",
            "ptg2:wrong-scope-v3",
            serving_index,
        )
    )

    assert set(contract_errors) == {"snapshot_scope", "coverage_scope_binding"}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "coverage_scope_id",
    ["C" * 64, "c" * 63, f"{'c' * 64} "],
)
async def test_strict_v3_snapshot_validation_requires_canonical_manifest_scope(
    monkeypatch,
    coverage_scope_id,
):
    serving_index = _serving_index()
    serving_index["coverage_scope_id"] = coverage_scope_id
    query = AsyncMock()
    monkeypatch.setattr(snapshot_cleanup, "_table_exists", AsyncMock(return_value=True))
    monkeypatch.setattr(snapshot_cleanup.db, "all", query)

    _missing_tables, contract_errors = (
        await snapshot_cleanup._missing_snapshot_serving_resources(
            "mrf",
            "ptg2:noncanonical-scope-v3",
            serving_index,
        )
    )

    assert "coverage_scope_id" in contract_errors
    query.assert_not_awaited()


@pytest.mark.asyncio
async def test_strict_v3_snapshot_validation_rejects_missing_binding(monkeypatch):
    monkeypatch.setattr(snapshot_cleanup, "_table_exists", AsyncMock(return_value=True))
    monkeypatch.setattr(snapshot_cleanup.db, "all", AsyncMock(return_value=[]))

    _missing_tables, contract_errors = (
        await snapshot_cleanup._missing_snapshot_serving_resources(
            "mrf",
            "ptg2:missing-binding-v3",
            _serving_index(),
        )
    )

    assert contract_errors == ["snapshot_binding"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("updates", "expected_errors"),
    [
        (
            {
                "code_count": 5,
                "code_scope_count": 2,
                "matching_code_scope_count": 4,
            },
            {"coverage_scope_code"},
        ),
        (
            {
                "code_count": 5,
                "code_scope_count": 1,
                "matching_code_scope_count": 0,
            },
            {"coverage_scope_code"},
        ),
        (
            {
                "has_code": False,
                "code_count": 0,
                "code_scope_count": 0,
                "matching_code_scope_count": 0,
            },
            {"dense:code"},
        ),
    ],
    ids=["mixed-code-scope", "wrong-code-scope", "missing-code-rows"],
)
async def test_strict_v3_snapshot_validation_rejects_broken_code_scope_chain(
    monkeypatch,
    updates,
    expected_errors,
):
    serving_index = _serving_index()
    row = _layout_row(serving_index)
    row.update(updates)
    monkeypatch.setattr(snapshot_cleanup, "_table_exists", AsyncMock(return_value=True))
    monkeypatch.setattr(snapshot_cleanup.db, "all", _layout_query([row]))

    _missing_tables, contract_errors = (
        await snapshot_cleanup._missing_snapshot_serving_resources(
            "mrf",
            "ptg2:broken-code-scope-v3",
            serving_index,
        )
    )

    assert set(contract_errors) == expected_errors


@pytest.mark.asyncio
async def test_strict_v3_snapshot_validation_rejects_layout_manifest_scope_mismatch(
    monkeypatch,
):
    serving_index = _serving_index()
    row = _layout_row(serving_index)
    row["layout_manifest"] = {
        "serving_index": {**serving_index, "coverage_scope_id": "d" * 64}
    }
    monkeypatch.setattr(snapshot_cleanup, "_table_exists", AsyncMock(return_value=True))
    monkeypatch.setattr(snapshot_cleanup.db, "all", _layout_query([row]))

    _missing_tables, contract_errors = (
        await snapshot_cleanup._missing_snapshot_serving_resources(
            "mrf",
            "ptg2:layout-scope-mismatch-v3",
            serving_index,
        )
    )

    assert contract_errors == ["layout_manifest:coverage_scope_id"]


@pytest.mark.asyncio
async def test_strict_v3_snapshot_validation_allows_empty_code_layout(monkeypatch):
    serving_index = _serving_index()
    serving_index["serving_rates"] = 0
    row = _layout_row(serving_index)
    row.update(
        {
            "has_code": False,
            "code_count": 0,
            "code_scope_count": 0,
            "matching_code_scope_count": 0,
        }
    )
    monkeypatch.setattr(snapshot_cleanup, "_table_exists", AsyncMock(return_value=True))
    monkeypatch.setattr(snapshot_cleanup.db, "all", _layout_query([row]))

    _missing_tables, contract_errors = (
        await snapshot_cleanup._missing_snapshot_serving_resources(
            "mrf",
            "ptg2:empty-code-layout-v3",
            serving_index,
        )
    )

    assert contract_errors == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("audit_rows", "expected_error"),
    [
        ([], "audit_sample:row_count"),
        ([{**_AUDIT_ROW, "atom_key": 99}], "audit_sample:digest"),
    ],
)
async def test_strict_v3_snapshot_validation_checks_audit_rows(
    monkeypatch,
    audit_rows,
    expected_error,
):
    serving_index = _serving_index()
    query = AsyncMock(side_effect=[[_layout_row(serving_index)], audit_rows])
    monkeypatch.setattr(snapshot_cleanup, "_table_exists", AsyncMock(return_value=True))
    monkeypatch.setattr(snapshot_cleanup.db, "all", query)

    _missing_tables, contract_errors = (
        await snapshot_cleanup._missing_snapshot_serving_resources(
            "mrf",
            "ptg2:audit-corrupt-v3",
            serving_index,
        )
    )

    assert expected_error in contract_errors


@pytest.mark.asyncio
async def test_published_redelivery_does_not_replace_newer_source_pointer(
    monkeypatch,
):
    publish = AsyncMock()
    monkeypatch.setattr(
        process_ptg,
        "_current_source_snapshot_id",
        AsyncMock(return_value="ptg2:newer"),
    )
    monkeypatch.setattr(process_ptg, "_publish_ptg2_source_pointers", publish)

    result = await process_ptg._reconcile_already_published_snapshot(
        snapshot_attributes={
            "previous_snapshot_id": "ptg2:older",
            "manifest": {
                "serving_index": {
                    "storage": "manifest_snapshot",
                    **_serving_index(),
                }
            },
        },
        snapshot_id="ptg2:redelivered",
        source_key="source-a",
        import_month=process_ptg.normalize_import_month("2026-07"),
    )

    assert result == {
        "status": "superseded",
        "source_key": "source-a",
        "snapshot_id": "ptg2:redelivered",
        "current_snapshot_id": "ptg2:newer",
    }
    publish.assert_not_awaited()
