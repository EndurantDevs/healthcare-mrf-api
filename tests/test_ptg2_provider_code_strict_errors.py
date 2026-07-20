# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import pytest

from process.ptg_parts import ptg2_serving_binary_v3_code_intersection as filtered
from process.ptg_parts import ptg2_serving_binary_v3_code_sets as complete
from process.ptg_parts.ptg2_serving_binary_v3_primitives import append_uvarint


def _uvarint(source_value: int) -> bytes:
    encoded_value = bytearray()
    append_uvarint(encoded_value, source_value)
    return bytes(encoded_value)


def _provider_payload(
    containers,
    *,
    trailing_bytes: bytes = b"",
) -> bytes:
    encoded_payload = bytearray((1,))
    append_uvarint(encoded_payload, len(containers))
    for high_delta, container_kind, cardinality, body, declared_size in containers:
        append_uvarint(encoded_payload, high_delta)
        if container_kind is None:
            continue
        encoded_payload.append(container_kind)
        append_uvarint(encoded_payload, cardinality)
        append_uvarint(
            encoded_payload,
            len(body) if declared_size is None else declared_size,
        )
        encoded_payload.extend(body)
    encoded_payload.extend(trailing_bytes)
    return bytes(encoded_payload)


_BITMAP_ONE = b"\x01" + bytes((1 << 13) - 1)
_VALID_SPARSE = (0, 1, 1, b"\x01", None)


@pytest.mark.parametrize(
    ("encoded_payload", "message"),
    (
        (b"\x02", "version"),
        (_provider_payload((_VALID_SPARSE, (0, 1, 1, b"\x02", None))), "ordered"),
        (_provider_payload(((0x8000, 1, 1, b"\x01", None),)), "signed PostgreSQL"),
        (_provider_payload(((0, None, 0, b"", None),)), "kind is truncated"),
        (_provider_payload(((0, 1, 0, b"", None),)), "cannot be empty"),
        (_provider_payload(((0, 1, 1, b"\x01", 2),)), "body is truncated"),
        (_provider_payload((_VALID_SPARSE,), trailing_bytes=b"\x00"), "trailing bytes"),
        (_provider_payload(((0, 99, 1, b"\x01", None),)), "unknown"),
        (_provider_payload(((0, 3, 2, _BITMAP_ONE, None),)), "cardinality"),
        (_provider_payload(((0, 3, 1, _BITMAP_ONE, None),)), "not canonical"),
        (_provider_payload(((0, 1, 2, b"\x01\x00", None),)), "strictly ordered"),
        (
            _provider_payload(((0, 1, 2, _uvarint(0xFFFF) + b"\x01", None),)),
            "16-bit container",
        ),
        (_provider_payload(((0, 1, 1, b"\x01\x00", None),)), "trailing bytes"),
        (_provider_payload(((0, 2, 1, b"\x00", None),)), "cannot be empty"),
        (
            _provider_payload(((0, 2, 2, b"\x02\x00\x00\x00\x00", None),)),
            "not canonical",
        ),
        (
            _provider_payload(
                ((0, 2, 2, b"\x01" + _uvarint(0xFFFF) + b"\x01", None),)
            ),
            "16-bit container",
        ),
        (_provider_payload(((0, 2, 1, b"\x01\x00\x00\x00", None),)), "trailing bytes"),
        (_provider_payload(((0, 3, 1, b"\x01", None),)), "byte count"),
        (_provider_payload(((0, 1, 1, b"\x81\x00", None),)), "canonical"),
    ),
)
def test_provider_code_decoders_reject_strict_malformed_payloads(
    encoded_payload,
    message,
):
    with pytest.raises(ValueError, match=message):
        complete.decode_provider_code_set(encoded_payload)
    with pytest.raises(ValueError, match=message):
        filtered.intersect_provider_code_set(encoded_payload, (1,))


def test_provider_code_scanner_internal_invariants_fail_closed():
    ordered_scan = filtered._CodeContainerScan(())
    with pytest.raises(ValueError, match="16-bit container"):
        ordered_scan.add_key(-1)
    ordered_scan.add_key(2)
    with pytest.raises(ValueError, match="strictly ordered"):
        ordered_scan.add_key(1)

    run_scan = filtered._CodeContainerScan(())
    run_scan.add_run(0, 0, 0)
    with pytest.raises(ValueError, match="not canonical"):
        run_scan.add_run(1, 1, 0)
    with pytest.raises(ValueError, match="run state"):
        filtered._CodeContainerScan(())._extend_last_run()
    with pytest.raises(ValueError, match="cannot be negative"):
        filtered._uvarint_size(-1)


def test_complete_provider_code_guards_bounds_and_final_order(monkeypatch):
    with pytest.raises(ValueError, match="cannot be negative"):
        complete.encode_provider_code_set((-1,))
    with pytest.raises(ValueError, match="signed PostgreSQL"):
        complete.encode_provider_code_set((1 << 31,))
    assert complete._code_runs(()) == ()

    monkeypatch.setattr(
        complete,
        "_decode_code_container",
        lambda _kind, _body, _cardinality: (2, 1),
    )
    with pytest.raises(ValueError, match="not strictly ordered"):
        complete.decode_provider_code_set(
            _provider_payload(((0, 1, 2, b"\x00", None),))
        )


def test_filtered_provider_code_decoder_rechecks_uvarint_canonicality(monkeypatch):
    monkeypatch.setattr(filtered, "read_uvarint", lambda *_arguments: (0, 2))

    with pytest.raises(ValueError, match="uvarint is not canonical"):
        filtered._read_canonical_uvarint(b"\0\0", 0)
