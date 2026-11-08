# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Preventive metadata accounting and the ordinary pre-history publication path."""

from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_profile_serving_receipt as continuity
from tests.provider_directory_profile_execution_test_support import _wal_tracker_admission


@pytest.mark.asyncio
@pytest.mark.parametrize("table_exists", [False, True])
async def test_before_receipt_history_does_not_require_common_publication(monkeypatch, table_exists):
    session = SimpleNamespace(scalar=AsyncMock(return_value=False))
    monkeypatch.setattr(continuity, "_publication_session", lambda _database: session)
    importer = SimpleNamespace(db=SimpleNamespace(scalar=AsyncMock(return_value=table_exists)))
    is_applied = False
    async with continuity.ordinary_profile_receipt_continuity(importer, "synthetic", None, "1s", "5s"):
        is_applied = True
    assert is_applied
    assert session.scalar.await_count == int(table_exists)


@pytest.mark.asyncio
@pytest.mark.parametrize("data_cap", [1, 32 * 1024 * 1024])
async def test_receipt_projection_reserves_explicit_pool_and_combined_data(monkeypatch, data_cap):
    admission = _wal_tracker_admission()
    admission = replace(admission, geometry=replace(admission.geometry, metadata_data_upper_bound_bytes=data_cap))
    layout = SimpleNamespace(main_index_pages=(1,) * 6, toast_index_pages=(1,))
    monkeypatch.setattr(continuity, "_receipt_storage_layout", AsyncMock(return_value=layout))
    importer = SimpleNamespace(
        _profile_cutover_metadata_layouts=AsyncMock(
            return_value=dict.fromkeys(("build_checkpoint", "serving_generation", "delta_receipt"), layout)
        ),
        _reserve_provider_directory_profile_wal_budget=AsyncMock(),
    )
    if data_cap == 1:
        with pytest.raises(RuntimeError, match="metadata_data_exceeded"):
            await continuity._reserve_receipt_mutation(importer, "synthetic", admission)
        importer._reserve_provider_directory_profile_wal_budget.assert_not_awaited()
    else:
        wal = await continuity._reserve_receipt_mutation(importer, "synthetic", admission)
        assert wal > 0
        importer._reserve_provider_directory_profile_wal_budget.assert_awaited_once_with(
            admission, metadata_wal_bytes=wal + admission.geometry.postgres_block_size_bytes
        )
