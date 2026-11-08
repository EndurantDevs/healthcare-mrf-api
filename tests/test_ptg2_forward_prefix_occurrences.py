# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Dense membership scans retain the ordinary strict forward validation."""

from unittest.mock import AsyncMock

import pytest

from api import ptg2_db_sidecars as sidecars
from api.ptg2_shared_blocks import SharedBlockPayload
from tests.test_ptg2_db_sidecars_forward_guards import _grouped_forward_payload


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid", [None, "price", "source", "fragment"])
async def test_dense_prefix_preserves_all_occurrences_without_reference_hydration(monkeypatch, invalid):
    code_key = 7
    block_key = sidecars._forward_provider_shard_block_key(code_key, 0)
    forward_payload = _grouped_forward_payload(2, ((3, ((1, 0), (1, 1))), (4, ((2, 0),))))
    fragment = SharedBlockPayload(
        block_key=block_key, fragment_no=int(invalid == "fragment"), entry_count=2, payload=forward_payload
    )

    async def stream(*_args, **_kwargs):
        yield fragment

    monkeypatch.setattr(sidecars, "stream_shared_blocks", stream)
    monkeypatch.setattr(
        sidecars, "_forward_shard_keys_for_read", AsyncMock(return_value=({code_key: (block_key,)}, None, True))
    )
    hydrate = AsyncMock(side_effect=AssertionError("dense scan must not hydrate price IDs or provider counts"))
    monkeypatch.setattr(sidecars, "_lookup_forward_references", hydrate)
    budget = sidecars.ForwardReadBudget(maximum_fragments=1, maximum_raw_payload_bytes=len(forward_payload))
    options_by_name = dict(
        limit=4,
        shared_snapshot_key=19,
        source_count=1 if invalid == "source" else 2,
        price_dictionary_item_count=2 if invalid == "price" else 3,
        price_dictionary_block_bytes=32,
        scan_budget=budget,
    )
    if invalid:
        with pytest.raises(sidecars.PTG2ManifestArtifactError):
            await sidecars.lookup_code_prefix_occurrences_from_db(object(), code_key, **options_by_name)
    else:
        assert await sidecars.lookup_code_prefix_occurrences_from_db(object(), code_key, **options_by_name) == [
            (3, 1, 0),
            (3, 1, 1),
            (4, 2, 0),
        ]
        assert budget.raw_payload_bytes == len(forward_payload)
    hydrate.assert_not_awaited()


@pytest.mark.asyncio
async def test_ordinary_prefix_still_hydrates_same_dense_occurrences(monkeypatch):
    occurrences = [(3, 1, 0), (3, 1, 1)]
    scan = AsyncMock(return_value=occurrences)
    hydrate = AsyncMock(return_value=("existing rows",))
    monkeypatch.setattr(sidecars, "lookup_code_prefix_occurrences_from_db", scan)
    monkeypatch.setattr(sidecars, "_materialize_forward_prefix", hydrate)
    options_by_name = dict(
        limit=2,
        descending=True,
        shared_snapshot_key=19,
        source_count=2,
        price_dictionary_item_count=3,
        price_dictionary_block_bytes=32,
    )
    assert await sidecars.lookup_code_prefix_rows_from_db(object(), 7, **options_by_name) == ("existing rows",)
    assert hydrate.call_args.args[2:] == (7, occurrences, True)
