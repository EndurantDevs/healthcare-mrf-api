from __future__ import annotations

from unittest.mock import AsyncMock, Mock

import pytest

from api import ptg2_db_serving_v3, ptg2_db_serving_v3_pages, ptg2_db_sidecars
from process.ptg_parts import ptg2_serving_binary_v3 as codec


def _logical_block(
    payload: bytes = b"payload",
    *,
    entry_count: int = 1,
    physical_hash: bytes = b"a" * 32,
) -> ptg2_db_sidecars._SharedLogicalBlock:
    return ptg2_db_sidecars._SharedLogicalBlock(
        payload=payload,
        entry_count=entry_count,
        physical_hashes=(physical_hash,),
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
@pytest.mark.parametrize("entry_count", (0, 1))
async def test_membership_alias_preflight_rejects_only_nonempty_aliases(
    monkeypatch,
    entry_count: int,
) -> None:
    async def mapping_records(_session, request, **_kwargs):
        assert request.requires_all
        for block_key in request.block_keys:
            yield {
                "object_kind": "price_set_atom_memberships_v3",
                "block_key": block_key,
                "fragment_no": 0,
                "mapping_entry_count": entry_count,
                "block_hash": b"a" * 32,
            }

    monkeypatch.setattr(
        ptg2_db_sidecars,
        "_stream_shared_mapping_records",
        mapping_records,
    )
    call = ptg2_db_sidecars._preflight_price_membership_aliases_from_db(
        object(),
        12,
        (0, 1),
        block_span=1,
    )
    if entry_count:
        with pytest.raises(
            ptg2_db_sidecars.PTG2ManifestArtifactError,
            match="incompatible physical alias",
        ):
            await call
    else:
        await call


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("second_tail_hash", "rejected"),
    ((b"b" * 32, True), (b"c" * 32, False)),
)
async def test_membership_alias_preflight_compares_complete_fragment_identity(
    monkeypatch,
    second_tail_hash: bytes,
    rejected: bool,
) -> None:
    async def mapping_records(_session, request, **_kwargs):
        tail_hash_by_block = {0: b"b" * 32, 1: second_tail_hash}
        for block_key in request.block_keys:
            tail_hash = tail_hash_by_block[block_key]
            yield {
                "object_kind": "price_set_atom_memberships_v3",
                "block_key": block_key,
                "fragment_no": 0,
                "mapping_entry_count": 1,
                "block_hash": b"a" * 32,
            }
            yield {
                "object_kind": "price_set_atom_memberships_v3",
                "block_key": block_key,
                "fragment_no": 1,
                "mapping_entry_count": 0,
                "block_hash": tail_hash,
            }

    monkeypatch.setattr(
        ptg2_db_sidecars,
        "_stream_shared_mapping_records",
        mapping_records,
    )
    call = ptg2_db_sidecars._preflight_price_membership_aliases_from_db(
        object(),
        12,
        (0, 1),
        block_span=1,
    )
    if rejected:
        with pytest.raises(
            ptg2_db_sidecars.PTG2ManifestArtifactError,
            match="incompatible physical alias",
        ):
            await call
    else:
        await call


@pytest.mark.asyncio
async def test_membership_alias_preflight_requires_each_requested_block(
    monkeypatch,
) -> None:
    async def no_mapping_records(*_args, **_kwargs):
        if False:
            yield None

    monkeypatch.setattr(
        ptg2_db_sidecars,
        "_stream_shared_mapping_records",
        no_mapping_records,
    )
    with pytest.raises(
        ptg2_db_sidecars.PTG2ManifestArtifactError,
        match="missing block keys",
    ):
        await ptg2_db_sidecars._preflight_price_membership_aliases_from_db(
            object(), 12, (0,), block_span=1
        )


@pytest.mark.asyncio
async def test_membership_alias_preflight_preserves_mapping_read_limits(
    monkeypatch,
) -> None:
    async def limited_mapping_records(*_args, **_kwargs):
        raise ptg2_db_sidecars.SharedMappingReadLimitError("bounded metadata")
        yield None

    monkeypatch.setattr(
        ptg2_db_sidecars,
        "_stream_shared_mapping_records",
        limited_mapping_records,
    )
    with pytest.raises(
        ptg2_db_sidecars.ManifestReadLimitError,
        match="bounded metadata",
    ):
        await ptg2_db_sidecars._preflight_price_membership_aliases_from_db(
            object(), 12, (0,), block_span=1
        )


@pytest.mark.asyncio
async def test_membership_alias_preflight_releases_each_metadata_budget(
    monkeypatch,
) -> None:
    budgets = []
    mapping_calls = []
    budget_type = ptg2_db_sidecars.CandidateAuditDecodedRetentionBudget

    def tracking_budget(**kwargs):
        budget = budget_type(**kwargs)
        budgets.append(budget)
        return budget

    async def mapping_records(_session, request, **_kwargs):
        mapping_calls.append(request.block_keys)
        for block_key in request.block_keys:
            yield {
                "object_kind": "price_set_atom_memberships_v3",
                "block_key": block_key,
                "fragment_no": 0,
                "mapping_entry_count": 0,
                "block_hash": bytes([block_key + 1]) * 32,
            }

    monkeypatch.setattr(
        ptg2_db_sidecars,
        "CandidateAuditDecodedRetentionBudget",
        tracking_budget,
    )
    monkeypatch.setattr(
        ptg2_db_sidecars,
        "_stream_shared_mapping_records",
        mapping_records,
    )
    await ptg2_db_sidecars._preflight_price_membership_aliases_from_db(
        object(), 12, (0, 1), block_span=1
    )

    assert len(budgets) == 1
    assert mapping_calls == [(0, 1)]
    assert all(budget.retained_bytes == 0 for budget in budgets)
    assert all(
        budget.peak_retained_bytes
        <= ptg2_db_sidecars._SHARED_MAPPING_DEFAULT_MAX_RETAINED_BYTES
        for budget in budgets
    )


@pytest.mark.asyncio
async def test_membership_alias_preflight_reuses_transaction_cache(
    monkeypatch,
) -> None:
    mapping_calls = []

    async def mapping_records(_session, request, **_kwargs):
        mapping_calls.append(request.block_keys)
        yield {
            "object_kind": "price_set_atom_memberships_v3",
            "block_key": request.block_keys[0],
            "fragment_no": 0,
            "mapping_entry_count": 0,
            "block_hash": b"a" * 32,
        }

    monkeypatch.setattr(
        ptg2_db_sidecars,
        "_stream_shared_mapping_records",
        mapping_records,
    )
    identity_by_block = {}
    owner_by_identity = {}
    retained_record_count = (
        await ptg2_db_sidecars._preflight_price_membership_aliases_from_db(
            object(),
            12,
            (0,),
            block_span=1,
            identity_by_block=identity_by_block,
            owner_by_identity=owner_by_identity,
        )
    )
    repeated_record_count = (
        await ptg2_db_sidecars._preflight_price_membership_aliases_from_db(
            object(),
            12,
            (0,),
            block_span=1,
            identity_by_block=identity_by_block,
            owner_by_identity=owner_by_identity,
            retained_record_count=retained_record_count,
        )
    )

    assert mapping_calls == [(0,)]
    assert retained_record_count == repeated_record_count == 1


@pytest.mark.asyncio
async def test_membership_alias_preflight_finds_alias_across_cached_calls(
    monkeypatch,
) -> None:
    async def mapping_records(_session, request, **_kwargs):
        yield {
            "object_kind": "price_set_atom_memberships_v3",
            "block_key": request.block_keys[0],
            "fragment_no": 0,
            "mapping_entry_count": 1,
            "block_hash": b"a" * 32,
        }

    monkeypatch.setattr(
        ptg2_db_sidecars,
        "_stream_shared_mapping_records",
        mapping_records,
    )
    identity_by_block = {}
    owner_by_identity = {}
    retained_record_count = (
        await ptg2_db_sidecars._preflight_price_membership_aliases_from_db(
            object(),
            12,
            (0,),
            block_span=1,
            identity_by_block=identity_by_block,
            owner_by_identity=owner_by_identity,
        )
    )
    with pytest.raises(
        ptg2_db_sidecars.PTG2ManifestArtifactError,
        match="incompatible physical alias",
    ):
        await ptg2_db_sidecars._preflight_price_membership_aliases_from_db(
            object(),
            12,
            (1,),
            block_span=1,
            identity_by_block=identity_by_block,
            owner_by_identity=owner_by_identity,
            retained_record_count=retained_record_count,
        )


@pytest.mark.asyncio
async def test_membership_reader_enforces_one_atom_limit_across_physical_blocks(
    monkeypatch,
):
    first_block = _logical_block(
        codec.encode_price_memberships(((0, (10, 11)),), 24),
        physical_hash=b"a" * 32,
    )
    second_block = _logical_block(
        codec.encode_price_memberships(((1, (12, 13)),), 24),
        physical_hash=b"b" * 32,
    )
    monkeypatch.setattr(
        ptg2_db_sidecars,
        "_shared_logical_blocks_by_key",
        AsyncMock(return_value={0: first_block, 1: second_block}),
    )
    monkeypatch.setattr(
        ptg2_db_sidecars,
        "_claim_logical_block_processing",
        Mock(),
    )
    dense_decode = Mock(wraps=codec.decode_dense_keys)
    monkeypatch.setattr(codec, "decode_dense_keys", dense_decode)

    with pytest.raises(
        ptg2_db_sidecars.ManifestReadLimitError,
        match="atom limit",
    ):
        await ptg2_db_sidecars.lookup_price_atom_memberships_from_db(
            None,
            12,
            (0, 1),
            atom_key_bits=24,
            block_span=1,
            maximum_selected_atom_count=3,
        )

    assert dense_decode.call_count == 1


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
