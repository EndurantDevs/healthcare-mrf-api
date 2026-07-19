# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import zlib
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from api import ptg2_db_sidecars as sidecars
from api.ptg2_shared_blocks import PTG2SharedBlockError


@pytest.mark.parametrize(
    ("operation", "operation_args"),
    (
        (sidecars._required_shared_snapshot_key, (True,)),
        (sidecars._required_shared_snapshot_key, (object(),)),
        (sidecars._required_shared_snapshot_key, (0,)),
        (sidecars._safe_qualified_table_name, ("unsafe",)),
        (sidecars._normalized_code_key, (True,)),
        (sidecars._normalized_code_key, (-1,)),
        (sidecars._normalized_price_item_count, (True,)),
        (sidecars._normalized_provider_set_filter, ((True,),)),
        (sidecars._normalized_provider_set_filter, ((-1,),)),
        (sidecars._normalized_provider_shard_span, (True,)),
    ),
)
def test_sidecar_scalar_guards_fail_closed(operation, operation_args):
    with pytest.raises(sidecars.PTG2ManifestArtifactError):
        operation(*operation_args)


def test_sidecar_safe_name_and_row_mapping_paths():
    assert sidecars._safe_qualified_table_name("mrf.blocks") == '"mrf"."blocks"'
    assert sidecars._row_mapping(SimpleNamespace(_mapping={"key": 1})) == {
        "key": 1
    }
    assert sidecars._row_mapping({"key": 2}) == {"key": 2}
    assert sidecars._row_mapping((("key", 3),)) == {"key": 3}
    assert sidecars._row_mapping(None) == {}


def test_sidecar_id_and_uvarint_guards():
    with pytest.raises(sidecars.PTG2ManifestArtifactError, match="16 bytes"):
        sidecars._id_text(b"short")
    assert sidecars._read_uvarint(b"\x81\x01", 0) == (129, 2)
    with pytest.raises(sidecars.PTG2ManifestArtifactError, match="inside"):
        sidecars._read_uvarint(b"\x80", 0)
    with pytest.raises(sidecars.PTG2ManifestArtifactError, match="too large"):
        sidecars._read_uvarint(b"\x80" * 10, 0)


def test_sidecar_payload_decode_guards():
    raw_payload = b"payload"
    assert sidecars._decode_serving_binary_payload(
        {
            "payload": zlib.compress(raw_payload),
            "payload_compression": "zlib",
            "raw_payload_bytes": len(raw_payload),
        }
    ) == raw_payload
    with pytest.raises(sidecars.PTG2ManifestArtifactError, match="corrupt"):
        sidecars._decode_serving_binary_payload(
            {"payload": b"bad-zlib", "payload_compression": "zlib"}
        )
    with pytest.raises(sidecars.PTG2ManifestArtifactError, match="unsupported"):
        sidecars._decode_serving_binary_payload(
            {"payload": b"", "payload_compression": "gzip"}
        )
    with pytest.raises(sidecars.PTG2ManifestArtifactError, match="byte count"):
        sidecars._decode_serving_binary_payload(
            {
                "payload": raw_payload,
                "payload_compression": "none",
                "raw_payload_bytes": len(raw_payload) + 1,
            }
        )


@pytest.mark.asyncio
async def test_forward_shard_discovery_empty_and_unexpected_rows():
    empty_session = SimpleNamespace(execute=AsyncMock())
    assert await sidecars._discover_forward_shard_keys(
        empty_session,
        shared_snapshot_key=1,
        schema_name="mrf",
        code_keys=(),
    ) == {}
    empty_session.execute.assert_not_awaited()

    unexpected_session = SimpleNamespace(
        execute=AsyncMock(return_value=({"code_key": 2, "block_key": 0},))
    )
    with pytest.raises(sidecars.PTG2ManifestArtifactError, match="unexpected"):
        await sidecars._discover_forward_shard_keys(
            unexpected_session,
            shared_snapshot_key=1,
            schema_name="mrf",
            code_keys=(1,),
        )


@pytest.mark.asyncio
async def test_forward_shard_discovery_rejects_duplicate_blocks():
    block_key = sidecars._forward_provider_shard_block_key(1, 0)
    duplicate_rows = (
        {"code_key": 1, "block_key": block_key},
        {"code_key": 1, "block_key": block_key},
    )
    session = SimpleNamespace(execute=AsyncMock(return_value=duplicate_rows))
    with pytest.raises(sidecars.PTG2ManifestArtifactError, match="duplicate"):
        await sidecars._discover_forward_shard_keys(
            session,
            shared_snapshot_key=1,
            schema_name="mrf",
            code_keys=(1,),
        )


@pytest.mark.asyncio
async def test_shared_fragment_errors_are_translated(monkeypatch):
    async def fail_shared_read(*_args, **_kwargs):
        raise PTG2SharedBlockError("physical block failed")

    monkeypatch.setattr(sidecars, "fetch_shared_blocks", fail_shared_read)
    with pytest.raises(
        sidecars.PTG2ManifestArtifactError,
        match="physical block failed",
    ):
        await sidecars._fetch_shared_block_fragments(
            object(),
            shared_snapshot_key=1,
            schema_name="mrf",
            artifact_kind="kind",
            block_keys=(0,),
        )


@pytest.mark.asyncio
async def test_shared_fragment_adapter_rejects_legacy_table(monkeypatch):
    with pytest.raises(sidecars.PTG2ManifestArtifactError, match="unsupported"):
        await sidecars._serving_binary_payload_rows_for_keys(
            object(),
            "mrf.legacy",
            artifact_kind="kind",
            block_keys=(0,),
            shared_snapshot_key=1,
        )

    expected_rows = [{"block_key": 0}]
    shared_reader = AsyncMock(return_value=expected_rows)
    monkeypatch.setattr(
        sidecars,
        "_shared_serving_binary_payload_rows_for_keys",
        shared_reader,
    )
    assert await sidecars._serving_binary_payload_rows_for_keys(
        object(),
        artifact_kind="kind",
        block_keys=(0,),
        shared_snapshot_key=1,
    ) == expected_rows


@pytest.mark.asyncio
async def test_shared_dictionary_front_guards():
    assert await sidecars._serving_binary_dictionary_values_for_keys(
        object(),
        shared_snapshot_key=1,
        artifact_kind="kind",
        item_keys=(),
        item_count=0,
        block_bytes=16,
    ) == {}
    with pytest.raises(sidecars.PTG2ManifestArtifactError, match="item count"):
        await sidecars._serving_binary_dictionary_values_for_keys(
            object(),
            shared_snapshot_key=1,
            artifact_kind="kind",
            item_keys=(0,),
            item_count=True,
            block_bytes=16,
        )
    with pytest.raises(sidecars.PTG2ManifestArtifactError, match="block size"):
        await sidecars._serving_binary_dictionary_values_for_keys(
            object(),
            shared_snapshot_key=1,
            artifact_kind="kind",
            item_keys=(0,),
            item_count=1,
            block_bytes=15,
        )


def _dictionary_fragment(**overrides):
    fragment_by_field = {
        "block_no": 0,
        "entry_count": 1,
        "payload": b"x" * 16,
        "payload_compression": "none",
        "raw_payload_bytes": 16,
        "_block_hash": b"h" * 32,
    }
    fragment_by_field.update(overrides)
    return fragment_by_field


def test_shared_dictionary_requires_physical_identity_and_keys():
    with pytest.raises(sidecars.PTG2ManifestArtifactError, match="identity"):
        sidecars._dictionary_fragments_by_hash(({},))
    with pytest.raises(sidecars.PTG2ManifestArtifactError, match="identity"):
        sidecars._dictionary_fragments_by_hash(({"_block_hash": b""},))
    with pytest.raises(sidecars.PTG2ManifestArtifactError, match="out of range"):
        sidecars._validate_dictionary_keys((-1,), 1)
    with pytest.raises(sidecars.PTG2ManifestArtifactError, match="missing"):
        sidecars._shared_dictionary_values_for_keys(
            (),
            (0,),
            item_count=1,
            entries_per_fragment=1,
            schema_name="mrf",
        )


def test_shared_dictionary_rejects_inconsistent_aliases():
    fragment_rows = (
        _dictionary_fragment(),
        _dictionary_fragment(block_no=1, entry_count=2),
    )
    with pytest.raises(sidecars.PTG2ManifestArtifactError, match="malformed"):
        sidecars._shared_dictionary_values_for_keys(
            fragment_rows,
            (0, 1),
            item_count=2,
            entries_per_fragment=1,
            schema_name="mrf",
        )


def test_shared_dictionary_rejects_unexpected_fragment():
    with pytest.raises(sidecars.PTG2ManifestArtifactError, match="unexpected"):
        sidecars._shared_dictionary_values_for_keys(
            (_dictionary_fragment(block_no=1),),
            (0,),
            item_count=2,
            entries_per_fragment=1,
            schema_name="mrf",
        )


def test_shared_dictionary_rejects_malformed_shape():
    malformed_fragment = _dictionary_fragment(payload=b"x", raw_payload_bytes=1)
    with pytest.raises(sidecars.PTG2ManifestArtifactError, match="malformed"):
        sidecars._dictionary_alias_values(
            b"h" * 32,
            [malformed_fragment],
            (),
            {0},
            item_count=1,
            entries_per_fragment=1,
            schema_name="mrf",
        )


def test_shared_dictionary_rejects_duplicate_physical_fragment_aliases():
    fragment_rows = (
        _dictionary_fragment(_block_hash=b"a" * 32),
        _dictionary_fragment(_block_hash=b"b" * 32),
    )
    with pytest.raises(sidecars.PTG2ManifestArtifactError, match="unexpected"):
        sidecars._shared_dictionary_values_for_keys(
            fragment_rows,
            (0,),
            item_count=1,
            entries_per_fragment=1,
            schema_name="mrf",
        )


def test_shared_dictionary_requires_every_requested_fragment():
    with pytest.raises(
        sidecars.PTG2ManifestArtifactError,
        match="missing a requested fragment",
    ):
        sidecars._shared_dictionary_values_for_keys(
            (_dictionary_fragment(),),
            (0, 1),
            item_count=2,
            entries_per_fragment=1,
            schema_name="mrf",
        )


def test_shared_dictionary_rebases_valid_physical_aliases():
    fragment_rows = (
        _dictionary_fragment(block_no=0),
        _dictionary_fragment(block_no=1),
    )
    values_by_key = sidecars._shared_dictionary_values_for_keys(
        fragment_rows,
        (0, 1),
        item_count=2,
        entries_per_fragment=1,
        schema_name="mrf",
    )

    assert set(values_by_key) == {0, 1}
    assert values_by_key[0] == values_by_key[1]


def test_shared_dictionary_checks_alias_decoder_contract(monkeypatch):
    monkeypatch.setattr(
        sidecars,
        "_dictionary_alias_values",
        Mock(return_value=({}, {0})),
    )
    with pytest.raises(sidecars.PTG2ManifestArtifactError, match="out of range"):
        sidecars._shared_dictionary_values_for_keys(
            (_dictionary_fragment(),),
            (0,),
            item_count=1,
            entries_per_fragment=1,
            schema_name="mrf",
        )


def test_forward_fragments_must_be_contiguous():
    with pytest.raises(sidecars.PTG2ManifestArtifactError, match="contiguous"):
        sidecars._ordered_forward_fragments(
            ({"block_no": 0}, {"block_no": 2})
        )


def test_forward_source_filter_is_checked_against_observed_header(monkeypatch):
    fragment_cursor = sidecars._ForwardFragmentCursor(
        provider_set_key=5,
        occurrence=(8, 0),
    )
    monkeypatch.setattr(
        sidecars,
        "_visit_serving_binary_by_code_record",
        Mock(return_value=(fragment_cursor, 2)),
    )
    with pytest.raises(sidecars.PTG2ManifestArtifactError, match="source key"):
        sidecars._visit_code_records_with_source_count(
            ({"block_no": 0},),
            provider_set_keys=None,
            occurrence_consumer=lambda *_occurrence: None,
            source_keys=(2,),
        )


def test_forward_fragments_must_agree_on_source_count(monkeypatch):
    fragment_cursor = sidecars._ForwardFragmentCursor(
        provider_set_key=5,
        occurrence=(8, 0),
    )
    monkeypatch.setattr(
        sidecars,
        "_visit_serving_binary_by_code_record",
        Mock(
            side_effect=(
                (fragment_cursor, 2),
                (fragment_cursor, 3),
            )
        ),
    )
    with pytest.raises(sidecars.PTG2ManifestArtifactError, match="source_count"):
        sidecars._visit_code_records_with_source_count(
            ({"block_no": 0}, {"block_no": 1}),
            provider_set_keys=None,
            occurrence_consumer=lambda *_occurrence: None,
        )


def test_forward_shards_reject_unexpected_blocks_and_source_drift(monkeypatch):
    first_block_key = sidecars._forward_provider_shard_block_key(7, 0)
    second_block_key = sidecars._forward_provider_shard_block_key(7, 1024)
    visit_options = sidecars._ForwardShardVisitOptions(
        code_key=7,
        expected_block_keys=(first_block_key,),
        provider_set_keys=None,
        expected_source_count=None,
        price_item_count=128,
    )
    with pytest.raises(sidecars.PTG2ManifestArtifactError, match="unexpected"):
        sidecars._visit_forward_shards_for_code(
            ({"block_key": second_block_key},),
            options=visit_options,
            occurrence_consumer=lambda *_occurrence: None,
        )

    monkeypatch.setattr(
        sidecars,
        "_visit_code_records_with_source_count",
        Mock(side_effect=(2, 3)),
    )
    with pytest.raises(sidecars.PTG2ManifestArtifactError, match="source_count"):
        sidecars._visit_forward_shards_for_code(
            (
                {"block_key": first_block_key},
                {"block_key": second_block_key},
            ),
            options=sidecars._ForwardShardVisitOptions(
                code_key=7,
                expected_block_keys=(first_block_key, second_block_key),
                provider_set_keys=None,
                expected_source_count=None,
                price_item_count=128,
            ),
            occurrence_consumer=lambda *_occurrence: None,
        )


def test_forward_fragment_grouping_rejects_unrequested_block():
    requested_block_key = sidecars._forward_provider_shard_block_key(7, 0)
    unrequested_block_key = sidecars._forward_provider_shard_block_key(7, 1024)
    with pytest.raises(sidecars.PTG2ManifestArtifactError, match="unexpected"):
        sidecars._group_forward_fragments_by_code(
            ({"block_key": unrequested_block_key},),
            {7: (requested_block_key,)},
        )


@pytest.mark.asyncio
async def test_single_forward_read_empty_paths_do_not_hydrate(monkeypatch):
    required_options_by_name = {
        "shared_snapshot_key": 1,
        "source_count": 2,
        "price_dictionary_item_count": 128,
        "price_dictionary_block_bytes": 32,
    }
    assert await sidecars.lookup_code_rows_from_db(
        object(),
        7,
        provider_set_keys=(),
        **required_options_by_name,
    ) == ()
    assert await sidecars.lookup_code_prefix_rows_from_db(
        object(),
        7,
        limit=1,
        provider_set_keys=(),
        **required_options_by_name,
    ) == ()

    fragment_reader = AsyncMock(return_value=[])
    monkeypatch.setattr(
        sidecars,
        "_shared_serving_binary_payload_rows_for_keys",
        fragment_reader,
    )
    assert await sidecars.lookup_code_rows_from_db(
        object(),
        7,
        provider_set_keys=(5,),
        **required_options_by_name,
    ) == ()
    fragment_reader.assert_awaited_once()


@pytest.mark.asyncio
async def test_nonempty_price_id_lookup_delegates_to_dictionary(monkeypatch):
    dictionary_reader = AsyncMock(return_value={8: "0" * 32})
    monkeypatch.setattr(
        sidecars,
        "_serving_binary_dictionary_values_for_keys",
        dictionary_reader,
    )
    assert await sidecars.lookup_price_ids_from_db(
        object(),
        (8,),
        shared_snapshot_key=1,
        price_dictionary_item_count=128,
        price_dictionary_block_bytes=32,
    ) == {8: "0" * 32}
    dictionary_reader.assert_awaited_once()
