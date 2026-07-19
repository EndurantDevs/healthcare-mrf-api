from __future__ import annotations

from unittest.mock import AsyncMock, Mock

import pytest

from api import ptg2_db_serving_v3, ptg2_db_serving_v3_pages, ptg2_db_sidecars


def _logical_block() -> ptg2_db_sidecars._SharedLogicalBlock:
    return ptg2_db_sidecars._SharedLogicalBlock(
        payload=b"payload",
        entry_count=1,
        physical_hashes=(b"a" * 32,),
    )


@pytest.mark.asyncio
async def test_membership_reader_parses_once_and_rejects_nonempty_aliases(
    monkeypatch,
):
    block = _logical_block()
    membership_decoder = Mock(return_value={0: (4,)})
    block_fetch = AsyncMock(side_effect=[{0: block}, {0: block, 1: block}])
    monkeypatch.setattr(
        ptg2_db_sidecars,
        "_shared_logical_blocks_by_key",
        block_fetch,
    )
    monkeypatch.setattr(
        ptg2_db_sidecars,
        "_claim_logical_block_processing",
        Mock(),
    )
    monkeypatch.setattr(
        ptg2_db_serving_v3,
        "_decode_price_membership_block",
        membership_decoder,
    )

    assert await ptg2_db_sidecars.lookup_price_atom_memberships_from_db(
        None,
        12,
        (0,),
        block_span=1,
    ) == {0: (4,)}

    with pytest.raises(
        ptg2_db_sidecars.PTG2ManifestArtifactError,
        match="incompatible physical alias",
    ):
        await ptg2_db_sidecars.lookup_price_atom_memberships_from_db(
            None,
            12,
            (0, 1),
            block_span=1,
        )


@pytest.mark.asyncio
async def test_shared_code_page_reader_handles_absent_and_present_blocks(
    monkeypatch,
):
    block = _logical_block()
    block_fetch = AsyncMock(side_effect=[{}, {7: block}])
    decoder = Mock(return_value=("page",))
    monkeypatch.setattr(
        ptg2_db_sidecars,
        "_shared_logical_blocks_by_key",
        block_fetch,
    )
    monkeypatch.setattr(
        ptg2_db_sidecars,
        "_claim_logical_block_processing",
        Mock(),
    )
    monkeypatch.setattr(
        ptg2_db_serving_v3_pages,
        "_decode_code_page_block",
        decoder,
    )

    assert (
        await ptg2_db_sidecars.lookup_shared_code_page_from_db(None, 12, 7)
        is None
    )
    assert await ptg2_db_sidecars.lookup_shared_code_page_from_db(
        None,
        12,
        7,
    ) == ("page",)


@pytest.mark.asyncio
async def test_provider_page_reader_handles_absent_and_omitted_pages(monkeypatch):
    block = _logical_block()
    monkeypatch.setattr(
        ptg2_db_sidecars,
        "_shared_logical_blocks_by_key",
        AsyncMock(side_effect=[{}, {0: block}]),
    )
    monkeypatch.setattr(
        ptg2_db_sidecars,
        "_claim_logical_block_processing",
        Mock(),
    )
    monkeypatch.setattr(
        ptg2_db_serving_v3_pages,
        "_decode_provider_page_block",
        Mock(return_value={}),
    )

    assert (
        await ptg2_db_sidecars.lookup_shared_provider_pages_from_db(None, 12, (0,))
        is None
    )
    assert await ptg2_db_sidecars.lookup_shared_provider_pages_from_db(
        None,
        12,
        (0,),
    ) == {}


@pytest.mark.asyncio
async def test_provider_page_reader_fans_out_aliases_and_skips_unrequested_keys(
    monkeypatch,
):
    block = _logical_block()
    page = ptg2_db_serving_v3_pages.PTG2V3ProviderPage(
        entries=(
            ptg2_db_serving_v3_pages.PTG2V3PageRecord(
                code_key=7,
                provider_set_key=0,
                provider_count=2,
                price_key=8,
                source_key=0,
            ),
        ),
        total_row_count=1,
    )
    monkeypatch.setattr(
        ptg2_db_sidecars,
        "_shared_logical_blocks_by_key",
        AsyncMock(return_value={0: block, 1: block, 2: block}),
    )
    monkeypatch.setattr(
        ptg2_db_sidecars,
        "_claim_logical_block_processing",
        Mock(),
    )
    monkeypatch.setattr(
        ptg2_db_serving_v3_pages,
        "_decode_provider_page_block",
        Mock(return_value={0: page}),
    )

    projected = await ptg2_db_sidecars.lookup_shared_provider_pages_from_db(
        None,
        12,
        (0, 1),
    )

    assert projected is not None
    assert set(projected) == {0, 1}
    assert projected[0].entries[0].provider_set_key == 0
    assert projected[1].entries[0].provider_set_key == 1
