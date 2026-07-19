# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import zlib
from types import SimpleNamespace
from unittest.mock import AsyncMock

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
