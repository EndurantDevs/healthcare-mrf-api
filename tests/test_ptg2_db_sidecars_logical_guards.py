# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from api import ptg2_db_serving_v3
from api import ptg2_db_sidecars as sidecars
from api.ptg2_shared_blocks import PTG2SharedBlockError, SharedBlockPayload


@pytest.mark.parametrize(
    ("fragments_by_number", "expected_message"),
    (
        ({1: {"_block_hash": b"h" * 32}}, "non-contiguous"),
        ({0: {}}, "physical block identity"),
        ({0: {"_block_hash": b""}}, "physical block identity"),
    ),
)
def test_logical_block_requires_contiguous_physical_identities(
    fragments_by_number,
    expected_message,
):
    with pytest.raises(sidecars.PTG2ManifestArtifactError, match=expected_message):
        sidecars._logical_block_physical_hashes(
            fragments_by_number,
            artifact_kind="test",
            block_key=0,
        )


def test_logical_fragment_index_rejects_duplicates():
    fragment_fields_by_name = {"block_key": 0, "block_no": 0}
    with pytest.raises(sidecars.PTG2ManifestArtifactError, match="invalid"):
        sidecars._fragments_by_logical_key(
            (fragment_fields_by_name, fragment_fields_by_name),
            (0,),
        )


def test_logical_block_assembly_skips_absent_fragments():
    assert sidecars._assembled_logical_blocks(
        {0: {}},
        artifact_kind="test",
    ) == {}


def test_logical_block_alias_requires_matching_entry_count(monkeypatch):
    block_hash = b"h" * 32
    monkeypatch.setattr(
        ptg2_db_serving_v3,
        "_logical_block_bytes",
        Mock(return_value=(b"x", 1)),
    )
    with pytest.raises(sidecars.PTG2ManifestArtifactError, match="entry counts"):
        sidecars._assembled_logical_blocks(
            {
                0: {0: {"entry_count": 1, "_block_hash": block_hash}},
                1: {0: {"entry_count": 2, "_block_hash": block_hash}},
            },
            artifact_kind="test",
        )


def _forward_fragment_view(
    block_hash: bytes,
    *,
    fragment_no: int,
) -> sidecars._ForwardBatchFragmentView:
    block_key = sidecars._forward_provider_shard_block_key(7, 0)
    return sidecars._ForwardBatchFragmentView(
        code_key=7,
        block_key=block_key,
        fragment_no=fragment_no,
        provider_key_min=0,
        provider_key_max=1024,
        provider_filter=None,
        source_filter=None,
        occurrence_filter=None,
        fragment_row={"_block_hash": block_hash},
    )


def _parsed_forward_fragment(
    provider_set_key: int,
    occurrence: tuple[int, int],
    *,
    source_count: int,
) -> sidecars._ParsedForwardFragment:
    return sidecars._ParsedForwardFragment(
        first_provider_set_key=provider_set_key,
        first_occurrence=occurrence,
        last_cursor=sidecars._ForwardFragmentCursor(
            provider_set_key=provider_set_key,
            occurrence=occurrence,
        ),
        source_count=source_count,
    )


def test_forward_physical_parse_requires_block_identity():
    fragment_view = _forward_fragment_view(b"", fragment_no=0)
    with pytest.raises(sidecars.PTG2ManifestArtifactError, match="identity"):
        sidecars._parse_physical_forward_fragment_once(
            (fragment_view,),
            sidecars._ForwardBatchOptions(
                shared_snapshot_key=1,
                source_count=2,
                price_dictionary_item_count=128,
                price_dictionary_block_bytes=32,
            ),
            128,
            {(fragment_view.code_key, fragment_view.block_key, 0): []},
        )


def test_forward_physical_parse_requires_first_occurrence(monkeypatch):
    fragment_view = _forward_fragment_view(b"f" * 32, fragment_no=0)
    monkeypatch.setattr(
        sidecars,
        "_visit_serving_binary_by_code_record",
        Mock(
            return_value=(
                sidecars._ForwardFragmentCursor(
                    provider_set_key=5,
                    occurrence=(8, 0),
                ),
                2,
            )
        ),
    )

    with pytest.raises(sidecars.PTG2ManifestArtifactError, match="no occurrences"):
        sidecars._parse_physical_forward_fragment_once(
            (fragment_view,),
            sidecars._ForwardBatchOptions(
                shared_snapshot_key=1,
                source_count=2,
                price_dictionary_item_count=128,
                price_dictionary_block_bytes=32,
            ),
            128,
            {(fragment_view.code_key, fragment_view.block_key, 0): []},
        )


@pytest.mark.parametrize(
    ("second_fragment", "expected_message"),
    (
        (_parsed_forward_fragment(5, (8, 0), source_count=2), "not ordered"),
        (_parsed_forward_fragment(6, (10, 0), source_count=3), "source_count"),
    ),
)
def test_forward_logical_fanout_rejects_cross_fragment_drift(
    second_fragment,
    expected_message,
):
    first_view = _forward_fragment_view(b"a" * 32, fragment_no=0)
    second_view = _forward_fragment_view(b"b" * 32, fragment_no=1)
    parsed_by_identity = {
        ("physical", b"a" * 32): _parsed_forward_fragment(
            5,
            (9, 1),
            source_count=2,
        ),
        ("physical", b"b" * 32): second_fragment,
    }
    retained_by_coordinate = {
        (first_view.code_key, first_view.block_key, 0): (),
        (second_view.code_key, second_view.block_key, 1): (),
    }

    with pytest.raises(sidecars.PTG2ManifestArtifactError, match=expected_message):
        sidecars._emit_forward_batch_logical_views(
            (first_view, second_view),
            parsed_by_identity,
            retained_by_coordinate,
            lambda *_occurrence: None,
        )


@pytest.mark.asyncio
async def test_price_atom_lookup_rejects_manifest_width_before_io():
    with pytest.raises(sidecars.PTG2ManifestArtifactError, match="atom key exceeds"):
        await sidecars.lookup_shared_price_atoms_from_db(
            object(),
            1,
            (1 << 24,),
            atom_key_bits=24,
        )


def test_price_atom_alias_fanout_skips_unrequested_alias(monkeypatch):
    price_atom = object()
    logical_block = sidecars._SharedLogicalBlock(
        payload=b"x",
        entry_count=1,
        physical_hashes=(b"h" * 32,),
    )
    monkeypatch.setattr(
        sidecars,
        "_claim_logical_block_processing",
        Mock(),
    )
    monkeypatch.setattr(
        sidecars,
        "_decode_price_atom_block",
        Mock(return_value={0: price_atom}),
    )

    assert sidecars._price_atoms_from_aliases(
        {0: logical_block, 1: logical_block},
        requested_key_set={0},
        block_span=10,
        schema_name="mrf",
    ) == {0: price_atom}


def _prefix_stream_fragment(provider_set_key: int) -> SharedBlockPayload:
    return SharedBlockPayload(
        block_key=sidecars._forward_provider_shard_block_key(
            7,
            provider_set_key,
        ),
        fragment_no=0,
        entry_count=1,
        payload=b"unused by mocked visitor",
    )


async def _lookup_prefix_with_mocked_stream(
    monkeypatch,
    *,
    provider_set_keys: tuple[int, ...],
    fragment_provider_keys: tuple[int, ...],
    observed_source_counts: tuple[int, ...],
):
    stream_fragments = tuple(
        _prefix_stream_fragment(provider_set_key)
        for provider_set_key in fragment_provider_keys
    )

    async def _stream_shared_fragments(*_args, **_kwargs):
        for stream_fragment in stream_fragments:
            yield stream_fragment

    fragment_cursor = sidecars._ForwardFragmentCursor(
        provider_set_key=provider_set_keys[-1],
        occurrence=(8, 0),
    )
    monkeypatch.setattr(
        sidecars,
        "stream_shared_blocks",
        _stream_shared_fragments,
    )
    monkeypatch.setattr(
        sidecars,
        "_visit_serving_binary_by_code_record",
        Mock(
            side_effect=tuple(
                (fragment_cursor, source_count)
                for source_count in observed_source_counts
            )
        ),
    )
    return await sidecars.lookup_code_prefix_rows_from_db(
        object(),
        7,
        limit=1,
        provider_set_keys=provider_set_keys,
        shared_snapshot_key=1,
        source_count=2,
        price_dictionary_item_count=128,
        price_dictionary_block_bytes=32,
    )


@pytest.mark.asyncio
async def test_prefix_stream_rejects_unexpected_shard(monkeypatch):
    with pytest.raises(sidecars.PTG2ManifestArtifactError, match="unexpected"):
        await _lookup_prefix_with_mocked_stream(
            monkeypatch,
            provider_set_keys=(5,),
            fragment_provider_keys=(1024,),
            observed_source_counts=(2,),
        )


@pytest.mark.asyncio
async def test_prefix_stream_rejects_descending_shards(monkeypatch):
    with pytest.raises(sidecars.PTG2ManifestArtifactError, match="not ordered"):
        await _lookup_prefix_with_mocked_stream(
            monkeypatch,
            provider_set_keys=(5, 1024),
            fragment_provider_keys=(1024, 5),
            observed_source_counts=(2, 2),
        )


@pytest.mark.asyncio
async def test_prefix_stream_rejects_source_count_drift(monkeypatch):
    with pytest.raises(sidecars.PTG2ManifestArtifactError, match="source_count"):
        await _lookup_prefix_with_mocked_stream(
            monkeypatch,
            provider_set_keys=(5, 1024),
            fragment_provider_keys=(5, 1024),
            observed_source_counts=(2, 3),
        )


@pytest.mark.asyncio
async def test_prefix_stream_returns_empty_when_filter_retains_nothing(monkeypatch):
    assert await _lookup_prefix_with_mocked_stream(
        monkeypatch,
        provider_set_keys=(5,),
        fragment_provider_keys=(5,),
        observed_source_counts=(2,),
    ) == ()


def test_source_filter_rejects_non_numeric_key():
    with pytest.raises(sidecars.PTG2ManifestArtifactError, match="source key"):
        sidecars._normalized_source_filter((object(),), None)


@pytest.mark.parametrize(
    ("normalizer_name", "options"),
    (
        (
            "_normalized_batch_source_filters",
            sidecars._ForwardBatchOptions(
                shared_snapshot_key=1,
                source_count=0,
                price_dictionary_item_count=128,
                price_dictionary_block_bytes=32,
                source_keys_by_code={7: (0,)},
            ),
        ),
        (
            "_normalized_batch_occurrence_filters",
            sidecars._ForwardBatchOptions(
                shared_snapshot_key=1,
                source_count=0,
                price_dictionary_item_count=128,
                price_dictionary_block_bytes=32,
                occurrence_keys=((7, 5, 0),),
            ),
        ),
    ),
)
def test_batch_filters_require_positive_source_count(normalizer_name, options):
    with pytest.raises(sidecars.PTG2ManifestArtifactError, match="source count"):
        getattr(sidecars, normalizer_name)(options, (7,))


def test_price_atom_decoder_wraps_invalid_payload():
    with pytest.raises(sidecars.PTG2ManifestArtifactError, match="corrupt"):
        sidecars._decode_price_atom_block(
            b"invalid",
            block_key=0,
            entry_count=1,
            block_span=512,
            requested_keys={0},
        )


@pytest.mark.asyncio
async def test_provider_page_existence_uses_shared_layout_query():
    query_result = SimpleNamespace(scalar=lambda: 1)
    session = SimpleNamespace(execute=AsyncMock(return_value=query_result))

    assert await sidecars.has_shared_provider_pages_in_db(session, 1) is True
    session.execute.assert_awaited_once()


@pytest.mark.asyncio
async def test_graph_reader_translates_shared_block_errors(monkeypatch):
    monkeypatch.setattr(
        sidecars,
        "fetch_shared_graph_members",
        AsyncMock(side_effect=PTG2SharedBlockError("graph block failed")),
    )
    with pytest.raises(
        sidecars.PTG2ManifestArtifactError,
        match="graph block failed",
    ):
        await sidecars.lookup_shared_graph_members_from_db(
            object(),
            1,
            0,
            (5,),
        )


@pytest.mark.asyncio
async def test_prefix_stream_translates_shared_block_errors(monkeypatch):
    async def _failing_stream(*_args, **_kwargs):
        if False:
            yield None
        raise PTG2SharedBlockError("forward block failed")

    monkeypatch.setattr(sidecars, "stream_shared_blocks", _failing_stream)
    with pytest.raises(
        sidecars.PTG2ManifestArtifactError,
        match="forward block failed",
    ):
        await sidecars.lookup_code_prefix_rows_from_db(
            object(),
            7,
            limit=1,
            provider_set_keys=(5,),
            shared_snapshot_key=1,
            source_count=2,
            price_dictionary_item_count=128,
            price_dictionary_block_bytes=32,
        )
