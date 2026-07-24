# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api.endpoint import npi as npi_endpoint


def _query_result(*rows):
    return SimpleNamespace(all=lambda: list(rows))


@pytest.mark.asyncio
async def test_ffs_summary_aggregates_optional_detail_tables_by_enrollment(
    monkeypatch,
):
    """Aggregate optional details by enrollment without cross-NPI leakage."""

    visible_rows_by_npi = {
        1000000001: [
            {"enrollment_id": " E1 ", "pecos_asct_cntl_id": "P1"},
            {"enrollment_id": "E2", "pecos_asct_cntl_id": "P2"},
            {"enrollment_id": "E2", "pecos_asct_cntl_id": "P2"},
        ],
        1000000002: [
            {"enrollment_id": "E3", "pecos_asct_cntl_id": "P3"},
        ],
        1000000003: [
            {"enrollment_id": None, "pecos_asct_cntl_id": None},
        ],
    }
    table_available = AsyncMock(return_value=True)
    execute = AsyncMock(
        side_effect=[
            _query_result(
                ("E1", 2000000001),
                ("E1", 2000000001),
                ("E2", None),
                ("missing", 2999999999),
            ),
            _query_result(
                ("E1", "12345", "Example City", "AA"),
                ("E2", "12345", "Example City", "AA"),
                ("E2", "54321", None, None),
                ("E3", None, "Other City", "BB"),
                ("missing", "99999", "Ignored", "ZZ"),
            ),
            _query_result(
                ("E1", "01", "Synthetic Specialty"),
                ("E2", "01", "Synthetic Specialty"),
                ("E2", None, None),
                ("missing", "99", "Ignored"),
            ),
            _query_result(
                ("E1", 2),
                ("E2", None),
                ("missing", 8),
            ),
            _query_result(
                ("E2", 3),
                ("E3", 1),
                ("missing", 8),
            ),
        ]
    )
    monkeypatch.setattr(npi_endpoint, "_is_table_available", table_available)
    monkeypatch.setattr(npi_endpoint, "_execute_stmt", execute)
    session = object()

    summaries = await npi_endpoint._fetch_ffs_summary_overrides(
        visible_rows_by_npi,
        session=session,
    )

    first = summaries[1000000001]
    assert first["ffs_enrollment_ids"] == ["E1", "E2"]
    assert first["ffs_pecos_asct_cntl_ids"] == ["P1", "P2"]
    assert first["ffs_related_npis"] == [2000000001]
    assert first["ffs_related_npi_count"] == 1
    assert first["ffs_practice_zip_codes"] == ["12345", "54321"]
    assert first["ffs_practice_cities"] == ["Example City"]
    assert first["ffs_practice_states"] == ["AA"]
    assert first["ffs_secondary_provider_type_codes"] == ["01"]
    assert first["ffs_secondary_provider_type_texts"] == ["Synthetic Specialty"]
    assert first["ffs_reassignment_out_count"] == 2
    assert first["ffs_reassignment_in_count"] == 3

    second = summaries[1000000002]
    assert second["ffs_practice_zip_codes"] == []
    assert second["ffs_practice_cities"] == ["Other City"]
    assert second["ffs_practice_states"] == ["BB"]
    assert second["ffs_reassignment_out_count"] == 0
    assert second["ffs_reassignment_in_count"] == 1

    empty = summaries[1000000003]
    assert empty["ffs_enrollment_ids"] == []
    assert empty["ffs_related_npis"] == []
    assert empty["ffs_practice_cities"] == []
    assert table_available.await_count == 4
    assert all(call.kwargs == {"session": session} for call in table_available.await_args_list)
    assert execute.await_count == 5


@pytest.mark.asyncio
async def test_ffs_summary_returns_defaults_when_no_enrollment_ids(monkeypatch):
    table_available = AsyncMock()
    execute = AsyncMock()
    monkeypatch.setattr(npi_endpoint, "_is_table_available", table_available)
    monkeypatch.setattr(npi_endpoint, "_execute_stmt", execute)

    summaries = await npi_endpoint._fetch_ffs_summary_overrides(
        {1000000001: [{"enrollment_id": "", "pecos_asct_cntl_id": None}]}
    )

    assert summaries[1000000001]["ffs_enrollment_ids"] == []
    assert summaries[1000000001]["ffs_related_npi_count"] == 0
    table_available.assert_not_awaited()
    execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_ffs_summary_keeps_defaults_when_detail_tables_are_unavailable(
    monkeypatch,
):
    table_available = AsyncMock(return_value=False)
    execute = AsyncMock()
    monkeypatch.setattr(npi_endpoint, "_is_table_available", table_available)
    monkeypatch.setattr(npi_endpoint, "_execute_stmt", execute)

    summaries = await npi_endpoint._fetch_ffs_summary_overrides(
        {
            1000000001: [
                {"enrollment_id": "E1", "pecos_asct_cntl_id": "P1"},
            ]
        }
    )

    assert summaries[1000000001] == {
        "ffs_enrollment_ids": ["E1"],
        "ffs_pecos_asct_cntl_ids": ["P1"],
        "ffs_secondary_provider_type_codes": [],
        "ffs_secondary_provider_type_texts": [],
        "ffs_practice_zip_codes": [],
        "ffs_practice_cities": [],
        "ffs_practice_states": [],
        "ffs_related_npis": [],
        "ffs_related_npi_count": 0,
        "ffs_reassignment_in_count": 0,
        "ffs_reassignment_out_count": 0,
    }
    assert table_available.await_count == 4
    execute.assert_not_awaited()
