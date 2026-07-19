# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import pytest

from api import ptg2_db_serving_v3 as serving
from api import ptg2_db_serving_v3_pages as pages
from process.ptg_parts.ptg2_serving_binary_v3 import encode_price_memberships


def _uvarint(value: int) -> bytes:
    encoded = bytearray()
    remaining = int(value)
    while remaining >= 0x80:
        encoded.append((remaining & 0x7F) | 0x80)
        remaining >>= 7
    encoded.append(remaining)
    return bytes(encoded)


def _page_header(source_count: int, entry_count: int) -> bytes:
    source_bits = 0 if source_count == 1 else (source_count - 1).bit_length()
    return b"\x04" + _uvarint(source_count) + bytes((source_bits,)) + _uvarint(
        entry_count
    )


def _code_page(
    rows: tuple[tuple[int, int, int], ...],
) -> bytes:
    payload = bytearray(_page_header(1, len(rows)))
    for provider_set_key, provider_count, price_key in rows:
        payload.extend(_uvarint(provider_set_key))
        payload.extend(_uvarint(provider_count))
        payload.extend(_uvarint(price_key))
    return bytes(payload)


def _provider_page(
    rows: tuple[tuple[int, int], ...],
    *,
    provider_offset: int = 0,
    provider_count: int = 9,
    total_row_count: int | None = None,
) -> bytes:
    total_rows = len(rows) if total_row_count is None else total_row_count
    payload = bytearray(_page_header(1, 1))
    for value in (provider_offset, provider_count, total_rows, len(rows)):
        payload.extend(_uvarint(value))
    for code_delta, price_key in rows:
        payload.extend(_uvarint(code_delta))
        payload.extend(_uvarint(price_key))
    return bytes(payload)


def _provider_code_payload(
    rows: tuple[tuple[int, bytes], ...],
) -> bytes:
    payload = bytearray(_uvarint(len(rows)))
    for provider_offset, code_payload in rows:
        payload.extend(_uvarint(provider_offset))
        payload.extend(_uvarint(len(code_payload)))
        payload.extend(code_payload)
    return bytes(payload)


def test_serving_key_and_manifest_scalar_guards():
    with pytest.raises(serving.PTG2ManifestArtifactError, match="negative"):
        serving._requested_keys((1, -1))
    with pytest.raises(serving.PTG2ManifestArtifactError, match="24 or 32"):
        serving._expected_atom_key_bits(16)
    with pytest.raises(serving.PTG2ManifestArtifactError, match="span"):
        serving._effective_block_span(0, 512)


def test_logical_block_rejects_fragment_layout_and_payload_errors():
    with pytest.raises(serving.PTG2ManifestArtifactError, match="non-contiguous"):
        serving._logical_block_bytes(
            {1: {"entry_count": 0, "_decoded_payload": b"x"}},
            artifact_kind="test",
            block_key=0,
        )
    with pytest.raises(serving.PTG2ManifestArtifactError, match="entry counts"):
        serving._logical_block_bytes(
            {
                0: {"entry_count": 1, "_decoded_payload": b"x"},
                1: {"entry_count": 1, "_decoded_payload": b"y"},
            },
            artifact_kind="test",
            block_key=0,
        )
    with pytest.raises(serving.PTG2ManifestArtifactError, match="payload is corrupt"):
        serving._logical_block_bytes(
            {
                0: {
                    "entry_count": 1,
                    "payload": b"not-zlib",
                    "payload_compression": "zlib",
                }
            },
            artifact_kind="test",
            block_key=0,
        )
    with pytest.raises(serving.PTG2ManifestArtifactError, match="is empty"):
        serving._logical_block_bytes(
            {0: {"entry_count": 0, "_decoded_payload": b""}},
            artifact_kind="test",
            block_key=0,
        )


@pytest.mark.parametrize(
    ("payload", "entry_count"),
    (
        (_provider_code_payload(((1024, b""),)), 1),
        (_provider_code_payload(((1, b""), (1, b""))), 2),
        (_provider_code_payload(()), 1),
    ),
)
def test_provider_code_block_rejects_bounds_order_and_count(payload, entry_count):
    with pytest.raises(serving.PTG2ManifestArtifactError, match="is corrupt"):
        serving._decode_provider_code_block(
            payload,
            block_key=0,
            entry_count=entry_count,
            requested_provider_set_keys=set(),
        )


def test_price_membership_block_rejects_key_outside_block():
    payload = encode_price_memberships(((512, (7,)),), 24)

    with pytest.raises(serving.PTG2ManifestArtifactError, match="is corrupt"):
        serving._decode_price_membership_block(
            payload,
            block_key=0,
            entry_count=1,
            atom_key_bits=24,
            block_span=512,
            requested_price_keys={512},
        )


def test_provider_page_count_requires_projected_rows():
    with pytest.raises(pages.PTG2ManifestArtifactError, match="no projected rows"):
        pages.PTG2V3ProviderPage((), 1).provider_count

    record = pages.PTG2V3PageRecord(7, 3, 9, 10, 0)
    assert pages.PTG2V3ProviderPage((record,), 1).provider_count == 9


def test_code_page_rejects_count_limit_value_and_order_errors():
    corrupt_payloads = (
        (_page_header(1, 1), 2),
        (_page_header(1, 0), 0),
        (_code_page(((2**31, 1, 1),)), 1),
        (_code_page(((2, 1, 2), (1, 1, 1))), 2),
    )
    for payload, entry_count in corrupt_payloads:
        with pytest.raises(pages.PTG2ManifestArtifactError, match="is corrupt"):
            pages._decode_code_page_block(
                payload,
                code_key=7,
                entry_count=entry_count,
                expected_source_count=1,
            )


def test_provider_page_rejects_metadata_value_order_count_and_trailing_bytes():
    corrupt_payloads = (
        (_provider_page(((7, 10),), provider_offset=1), 1),
        (_provider_page(((2**31, 10),)), 1),
        (_provider_page(((2, 2), (0, 1))), 1),
        (_page_header(1, 0), 0),
        (_provider_page(((7, 10),)) + b"\x00", 1),
    )
    for payload, entry_count in corrupt_payloads:
        with pytest.raises(pages.PTG2ManifestArtifactError, match="is corrupt"):
            pages._decode_provider_page_block(
                payload,
                block_key=3,
                entry_count=entry_count,
                requested_provider_set_keys={3},
                expected_source_count=1,
            )


def test_provider_page_validates_unrequested_rows_without_materializing():
    payload = _provider_page(((7, 10),))

    assert pages._decode_provider_page_block(
        payload,
        block_key=3,
        entry_count=1,
        requested_provider_set_keys=set(),
        expected_source_count=1,
    ) == {}
