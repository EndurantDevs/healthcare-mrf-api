# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Request-wide work accounting for complete forward reads."""

from __future__ import annotations

from unittest.mock import AsyncMock, Mock

import pytest

from api import ptg2_db_sidecars as sidecars
from process.ptg_parts.ptg2_manifest_artifacts import (
    PTG2ManifestArtifactError,
)


def _install_forward_reader_dependencies(monkeypatch):
    fragment_rows = (
        {"block_key": 701, "_decoded_payload": b"abc"},
        {"block_key": 702, "raw_payload_bytes": 5},
    )
    monkeypatch.setattr(
        sidecars,
        "_forward_shard_keys_for_read",
        AsyncMock(return_value=({7: (701, 702)}, frozenset({3}), True)),
    )
    monkeypatch.setattr(
        sidecars,
        "_shared_serving_binary_payload_rows_for_keys",
        AsyncMock(return_value=fragment_rows),
    )
    decode_rows = Mock(return_value=[(3, 1, 0)])
    monkeypatch.setattr(
        sidecars,
        "_decode_forward_shards_for_code",
        decode_rows,
    )
    monkeypatch.setattr(
        sidecars,
        "_lookup_forward_references",
        AsyncMock(return_value=({3: 1}, {1: "1" * 32})),
    )
    monkeypatch.setattr(
        sidecars,
        "_materialize_forward_rows",
        Mock(return_value=("materialized",)),
    )
    return decode_rows


@pytest.mark.asyncio
async def test_full_forward_reader_charges_every_fragment_across_calls(
    monkeypatch,
):
    """Charge decoded and raw fragments cumulatively before materialization."""

    decode_rows = _install_forward_reader_dependencies(monkeypatch)
    scan_budget = sidecars.ForwardReadBudget(
        maximum_fragments=3,
        maximum_raw_payload_bytes=16,
    )
    read_options_by_name = {
        "shared_snapshot_key": 1,
        "source_count": 1,
        "price_dictionary_item_count": 8,
        "price_dictionary_block_bytes": 128,
        "provider_set_keys": (3,),
        "scan_budget": scan_budget,
    }

    assert await sidecars.lookup_code_rows_from_db(
        object(),
        7,
        **read_options_by_name,
    ) == ("materialized",)
    assert scan_budget.fragment_count == 2
    assert scan_budget.raw_payload_bytes == 8

    with pytest.raises(
        sidecars.ForwardReadBudgetExceeded,
        match="physical scan budget",
    ):
        await sidecars.lookup_code_rows_from_db(
            object(),
            7,
            **read_options_by_name,
        )

    assert decode_rows.call_count == 1


@pytest.mark.parametrize("maximum_row_capacity", (True, 0, -1))
def test_forward_budget_rejects_invalid_row_capacity(maximum_row_capacity):
    with pytest.raises(PTG2ManifestArtifactError, match="row-capacity"):
        sidecars.ForwardReadBudget(
            1,
            1,
            maximum_row_capacity=maximum_row_capacity,
        )


@pytest.mark.parametrize(
    ("read_rows", "result_rows"),
    ((-1, 1), (1, -1), (2, 1), (1, 2)),
)
def test_forward_budget_rejects_invalid_or_excess_release(
    read_rows,
    result_rows,
):
    scan_budget = sidecars.ForwardReadBudget(
        1,
        1,
        maximum_row_capacity=2,
    )
    scan_budget.reserve_row_capacity(read_rows=1, result_rows=1)

    with pytest.raises(RuntimeError, match="release is invalid"):
        scan_budget.release_row_capacity(
            read_rows=read_rows,
            result_rows=result_rows,
        )

    assert scan_budget.active_read_row_capacity == 1
    assert scan_budget.active_result_row_capacity == 1
