# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded persistent alias checks for price-membership metadata."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from api import ptg2_db_sidecars


def _physical_identity(hashes: tuple[bytes, ...]) -> bytes:
    records = tuple(
        {
            "fragment_no": fragment_no,
            "mapping_entry_count": 1 if fragment_no == 0 else 0,
            "block_hash": physical_hash,
        }
        for fragment_no, physical_hash in enumerate(hashes)
    )
    return ptg2_db_sidecars._validated_price_membership_identity(
        records,
        "price_set_atom_memberships_v3",
        0,
    )[0]


def test_identity_digest_preserves_fragment_order() -> None:
    assert _physical_identity((b"a" * 32, b"b" * 32)) != (
        _physical_identity((b"b" * 32, b"a" * 32))
    )


@pytest.mark.asyncio
async def test_digest_collision_fails_closed(monkeypatch) -> None:
    class _Digest:
        def update(self, _value) -> None:
            return None

        def digest(self) -> bytes:
            return b"d" * 32

    async def mapping_records(_session, request, **_kwargs):
        for block_key in request.block_keys:
            yield {
                "object_kind": "price_set_atom_memberships_v3",
                "block_key": block_key,
                "fragment_no": 0,
                "mapping_entry_count": 1,
                "block_hash": bytes([block_key + 1]) * 32,
            }

    monkeypatch.setattr(ptg2_db_sidecars.hashlib, "sha256", _Digest)
    monkeypatch.setattr(
        ptg2_db_sidecars,
        "_stream_shared_mapping_records",
        mapping_records,
    )

    with pytest.raises(
        ptg2_db_sidecars.PTG2ManifestArtifactError,
        match="incompatible physical alias",
    ):
        await ptg2_db_sidecars._preflight_price_membership_aliases_from_db(
            object(), 12, (0, 1), block_span=1
        )


@pytest.mark.asyncio
async def test_transient_metadata_reads_are_adaptively_bisected(
    monkeypatch,
) -> None:
    calls = []

    async def bounded_identities(
        _session,
        _snapshot_key,
        _artifact_kind,
        block_keys,
        _schema_name,
        _maximum_records,
    ):
        calls.append(block_keys)
        if len(block_keys) > 1:
            raise ptg2_db_sidecars.ManifestReadLimitError("split metadata")
        return {block_keys[0]: (bytes([block_keys[0] + 1]) * 32, 0, 1)}

    monkeypatch.setattr(
        ptg2_db_sidecars,
        "_price_membership_block_identities",
        bounded_identities,
    )
    cache = ptg2_db_sidecars._PriceMembershipAliasCache()

    returned_cache = await ptg2_db_sidecars._preflight_price_membership_aliases_from_db(
        object(), 12, (0, 1), block_span=1, cache=cache
    )

    assert calls == [(0, 1), (0,), (1,)]
    assert returned_cache is cache
    assert cache.metadata_record_count == 2
    assert cache.maximum_fragment_count == 1
    assert len(cache.identity_by_block) == len(cache.owner_by_identity) == 2


@pytest.mark.asyncio
async def test_index_limit_precedes_reads_or_mutation(monkeypatch) -> None:
    block_reader = AsyncMock()
    monkeypatch.setattr(
        ptg2_db_sidecars,
        "_price_membership_block_identities",
        block_reader,
    )
    monkeypatch.setattr(
        ptg2_db_sidecars,
        "MAX_PRICE_MEMBERSHIP_CACHED_BLOCKS",
        1,
    )
    cache = ptg2_db_sidecars._PriceMembershipAliasCache()

    with pytest.raises(
        ptg2_db_sidecars.ManifestReadLimitError,
        match="identity index",
    ):
        await ptg2_db_sidecars._preflight_price_membership_aliases_from_db(
            object(), 12, (0, 1), block_span=1, cache=cache
        )

    block_reader.assert_not_awaited()
    assert cache.identity_by_block == {}
    assert cache.owner_by_identity == {}


def test_read_limit_reserves_the_exact_eighty_percent_envelope(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        ptg2_db_sidecars,
        "MAX_PRICE_MEMBERSHIP_ALIAS_RETAINED_BYTES",
        100,
    )
    monkeypatch.setattr(
        ptg2_db_sidecars,
        "PRICE_MEMBERSHIP_ALIAS_INDEX_RETAINED_BYTES_PER_BLOCK",
        10,
    )
    monkeypatch.setattr(
        ptg2_db_sidecars,
        "PRICE_MEMBERSHIP_TRANSIENT_BYTES_PER_FRAGMENT",
        2,
    )
    monkeypatch.setattr(
        ptg2_db_sidecars,
        "MAX_PRICE_MEMBERSHIP_CACHED_BLOCKS",
        10,
    )

    assert ptg2_db_sidecars._price_membership_read_record_limit(0, 7) == 5
    with pytest.raises(
        ptg2_db_sidecars.ManifestReadLimitError,
        match="identity index",
    ):
        ptg2_db_sidecars._price_membership_read_record_limit(0, 8)


def test_real_envelope_boundary_matches_the_source_model() -> None:
    maximum_records = ptg2_db_sidecars._price_membership_read_record_limit(0, 57_614)
    retained_index_bytes = (
        57_614 * ptg2_db_sidecars.PRICE_MEMBERSHIP_ALIAS_INDEX_RETAINED_BYTES_PER_BLOCK
    )
    transient_bytes = (
        maximum_records * ptg2_db_sidecars.PRICE_MEMBERSHIP_TRANSIENT_BYTES_PER_FRAGMENT
    )
    operational_bytes = (
        ptg2_db_sidecars.MAX_PRICE_MEMBERSHIP_ALIAS_RETAINED_BYTES * 4 // 5
    )

    assert maximum_records == 7_873
    assert retained_index_bytes + transient_bytes <= operational_bytes
    assert (
        retained_index_bytes
        + transient_bytes
        + ptg2_db_sidecars.PRICE_MEMBERSHIP_TRANSIENT_BYTES_PER_FRAGMENT
        > operational_bytes
    )
    assert ptg2_db_sidecars._price_membership_read_record_limit(0, 104_851) == 1
    with pytest.raises(ptg2_db_sidecars.ManifestReadLimitError):
        ptg2_db_sidecars._price_membership_read_record_limit(0, 104_852)


@pytest.mark.asyncio
async def test_index_growth_preserves_the_historical_singleton_peak(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        ptg2_db_sidecars,
        "MAX_PRICE_MEMBERSHIP_ALIAS_RETAINED_BYTES",
        100,
    )
    monkeypatch.setattr(
        ptg2_db_sidecars,
        "PRICE_MEMBERSHIP_ALIAS_INDEX_RETAINED_BYTES_PER_BLOCK",
        10,
    )
    monkeypatch.setattr(
        ptg2_db_sidecars,
        "PRICE_MEMBERSHIP_TRANSIENT_BYTES_PER_FRAGMENT",
        2,
    )
    monkeypatch.setattr(
        ptg2_db_sidecars,
        "MAX_PRICE_MEMBERSHIP_CACHED_BLOCKS",
        10,
    )
    block_reader = AsyncMock(return_value={0: (b"a" * 32, 0, 30)})
    monkeypatch.setattr(
        ptg2_db_sidecars,
        "_price_membership_block_identities",
        block_reader,
    )
    cache = ptg2_db_sidecars._PriceMembershipAliasCache()

    await ptg2_db_sidecars._preflight_price_membership_aliases_from_db(
        object(), 12, (0,), block_span=1, cache=cache
    )
    with pytest.raises(
        ptg2_db_sidecars.ManifestReadLimitError,
        match="identity index",
    ):
        await ptg2_db_sidecars._preflight_price_membership_aliases_from_db(
            object(), 12, (1, 2), block_span=1, cache=cache
        )

    block_reader.assert_awaited_once()
    assert cache.maximum_fragment_count == 30
    assert len(cache.identity_by_block) == 1


@pytest.mark.asyncio
async def test_singleton_read_limit_is_terminal(monkeypatch) -> None:
    block_reader = AsyncMock(
        side_effect=ptg2_db_sidecars.ManifestReadLimitError("singleton")
    )
    monkeypatch.setattr(
        ptg2_db_sidecars,
        "_price_membership_block_identities",
        block_reader,
    )
    cache = ptg2_db_sidecars._PriceMembershipAliasCache()

    with pytest.raises(
        ptg2_db_sidecars.ManifestReadLimitError,
        match="singleton",
    ):
        await ptg2_db_sidecars._preflight_price_membership_aliases_from_db(
            object(), 12, (0,), block_span=1, cache=cache
        )

    block_reader.assert_awaited_once()
    assert cache == ptg2_db_sidecars._PriceMembershipAliasCache()


@pytest.mark.asyncio
async def test_corruption_is_not_bisected(monkeypatch) -> None:
    block_reader = AsyncMock(
        side_effect=ptg2_db_sidecars.PTG2ManifestArtifactError("corrupt")
    )
    monkeypatch.setattr(
        ptg2_db_sidecars,
        "_price_membership_block_identities",
        block_reader,
    )
    cache = ptg2_db_sidecars._PriceMembershipAliasCache()

    with pytest.raises(
        ptg2_db_sidecars.PTG2ManifestArtifactError,
        match="corrupt",
    ):
        await ptg2_db_sidecars._preflight_price_membership_aliases_from_db(
            object(), 12, (0, 1), block_span=1, cache=cache
        )

    block_reader.assert_awaited_once()
    assert block_reader.await_args.args[3] == (0, 1)
    assert cache == ptg2_db_sidecars._PriceMembershipAliasCache()


@pytest.mark.asyncio
async def test_cumulative_fragment_limit_preserves_prior_cache(
    monkeypatch,
) -> None:
    artifact_kind = "price_set_atom_memberships_v3"
    old_key = ("mrf", 12, artifact_kind, 0)
    old_identity = (b"a" * 32, 1, 1)
    cache = ptg2_db_sidecars._PriceMembershipAliasCache(
        identity_by_block={old_key: old_identity},
        owner_by_identity={("mrf", 12, artifact_kind, old_identity[0]): (old_key, 1)},
        metadata_record_count=1,
        maximum_fragment_count=1,
    )
    identity_by_block_before = dict(cache.identity_by_block)
    owner_by_identity_before = dict(cache.owner_by_identity)
    monkeypatch.setattr(
        ptg2_db_sidecars,
        "MAX_PRICE_MEMBERSHIP_CACHED_FRAGMENTS",
        1,
    )
    monkeypatch.setattr(
        ptg2_db_sidecars,
        "_price_membership_block_identities",
        AsyncMock(return_value={1: (b"b" * 32, 1, 1)}),
    )

    with pytest.raises(
        ptg2_db_sidecars.ManifestReadLimitError,
        match="metadata exceeds",
    ):
        await ptg2_db_sidecars._preflight_price_membership_aliases_from_db(
            object(), 12, (1,), block_span=1, cache=cache
        )

    assert cache.identity_by_block == identity_by_block_before
    assert cache.owner_by_identity == owner_by_identity_before
    assert cache.metadata_record_count == cache.maximum_fragment_count == 1
