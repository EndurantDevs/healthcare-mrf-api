# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import pytest

from process.ptg_parts.ptg2_serving_binary_v3 import (
    PTG2_V3_ATOM_KEY_24_BITS,
    PTG2_V3_ATOM_KEY_32_BITS,
    PTG2_V3_MAX_24_BIT_KEY_COUNT,
    PTG2V3PriceAtomRecord,
    decode_dense_keys,
    decode_price_atoms,
    decode_price_atoms_for_offsets,
    decode_price_memberships,
    decode_price_memberships_for_keys,
    decode_provider_code_set,
    encode_dense_keys,
    encode_price_atoms,
    encode_price_memberships,
    encode_provider_code_set,
    price_atom_entry_count,
    price_membership_entry_count,
    read_uvarint,
    select_atom_key_bits,
)


def test_v3_atom_key_width_switches_at_24_bit_boundary():
    assert select_atom_key_bits(0) == PTG2_V3_ATOM_KEY_24_BITS
    assert select_atom_key_bits(PTG2_V3_MAX_24_BIT_KEY_COUNT) == PTG2_V3_ATOM_KEY_24_BITS
    assert select_atom_key_bits(PTG2_V3_MAX_24_BIT_KEY_COUNT + 1) == PTG2_V3_ATOM_KEY_32_BITS
    assert select_atom_key_bits(1 << 32) == PTG2_V3_ATOM_KEY_32_BITS


@pytest.mark.parametrize("key_bits", [PTG2_V3_ATOM_KEY_24_BITS, PTG2_V3_ATOM_KEY_32_BITS])
def test_v3_dense_keys_round_trip_at_selected_width(key_bits):
    maximum_key = (1 << key_bits) - 1
    source_keys = (0, 1, 255, 65_535, maximum_key)

    encoded = encode_dense_keys(source_keys, key_bits)

    assert len(encoded) == len(source_keys) * (key_bits // 8)
    assert decode_dense_keys(encoded, key_bits) == source_keys


def test_v3_dense_key_rejects_overflow():
    with pytest.raises(ValueError, match="does not fit"):
        encode_dense_keys([1 << 24], PTG2_V3_ATOM_KEY_24_BITS)


def test_v3_uvarint_rejects_noncanonical_and_truncated_values():
    with pytest.raises(ValueError, match="not canonical"):
        read_uvarint(b"\x80\x00", 0)
    with pytest.raises(ValueError, match="truncated"):
        read_uvarint(b"\x80", 0)


@pytest.mark.parametrize("key_bits", [PTG2_V3_ATOM_KEY_24_BITS, PTG2_V3_ATOM_KEY_32_BITS])
def test_v3_price_memberships_are_byte_stable(key_bits):
    source_memberships = ((3, (0, 1, 257)), (9, (65_535,)))

    encoded = encode_price_memberships(source_memberships, key_bits)

    expected_payload_by_width = {
        24: bytes.fromhex("0203022001000000000003030000000100000101000601ffff00"),
        32: bytes.fromhex("0204022001000000000003030000000001000000010100000601ffff0000"),
    }
    assert encoded == expected_payload_by_width[key_bits]
    assert decode_price_memberships(encoded) == dict(source_memberships)


def test_v3_price_memberships_reject_bad_order_and_corruption():
    with pytest.raises(ValueError, match="strictly ordered"):
        encode_price_memberships(((2, (1,)), (2, (3,))), PTG2_V3_ATOM_KEY_24_BITS)
    with pytest.raises(ValueError, match="strictly ordered"):
        decode_price_memberships(bytes((1, 3, 2, 2, 1, 0, 0, 0, 0, 1, 1, 0, 0, 0)))
    with pytest.raises(ValueError, match="truncated"):
        decode_price_memberships(bytes((1, 3, 1, 0, 1, 0, 0)))


def test_v3_price_memberships_reject_invalid_version_and_key_width():
    with pytest.raises(ValueError, match="version"):
        decode_price_memberships(b"\x03\x03")
    with pytest.raises(ValueError, match="three or four"):
        decode_price_memberships(b"\x01\x02")


def test_v3_price_atoms_are_byte_stable_and_reject_corruption():
    source_atoms = (
        PTG2V3PriceAtomRecord("15.25", (0, None)),
        PTG2V3PriceAtomRecord(None, (1, 8)),
    )

    encoded = encode_price_atoms(source_atoms)

    assert encoded == bytes.fromhex("0202022001000000000631352e32350100000209")
    assert decode_price_atoms(encoded) == source_atoms
    with pytest.raises(ValueError, match="truncated"):
        decode_price_atoms(encoded[:-1])
    with pytest.raises(ValueError, match="version"):
        decode_price_atoms(b"\x03")


def test_v3_checkpointed_memberships_read_sparse_keys_and_legacy_v1():
    source_memberships = tuple((price_key, (price_key,)) for price_key in range(96))
    encoded = encode_price_memberships(source_memberships, PTG2_V3_ATOM_KEY_24_BITS)

    assert price_membership_entry_count(encoded) == 96
    assert decode_price_memberships_for_keys(encoded, (0, 31, 32, 95, 999)) == {
        0: (0,),
        31: (31,),
        32: (32,),
        95: (95,),
    }
    legacy_v1 = bytes.fromhex("01030203030000000100000101000601ffff00")
    assert decode_price_memberships_for_keys(legacy_v1, (9,)) == {9: (65_535,)}


def test_v3_checkpointed_atoms_read_sparse_offsets_and_legacy_v1():
    source_atoms = tuple(
        PTG2V3PriceAtomRecord(str(atom_key), (atom_key, None))
        for atom_key in range(96)
    )
    encoded = encode_price_atoms(source_atoms)

    assert price_atom_entry_count(encoded) == 96
    assert decode_price_atoms_for_offsets(encoded, (0, 31, 32, 95, 999)) == {
        atom_offset: source_atoms[atom_offset]
        for atom_offset in (0, 31, 32, 95)
    }
    legacy_v1 = b"\x01\x02\x02\x0615.25\x01\x00\x00\x02\x09"
    assert decode_price_atoms_for_offsets(legacy_v1, (1,)) == {
        1: PTG2V3PriceAtomRecord(None, (1, 8))
    }


def test_v3_provider_code_container_modes_are_byte_stable():
    sparse_codes = [1, 9, 65_000]
    run_codes = list(range(1 << 16, (1 << 16) + 4_000))
    bitmap_codes = [(2 << 16) + low_key for low_key in range(0, 1 << 16, 2)]
    source_codes = tuple(sparse_codes + run_codes + bitmap_codes)

    encoded, stats = encode_provider_code_set(source_codes)

    assert encoded[:20] == bytes.fromhex("0103000103050108dffb030102a01f0401009f1f")
    assert decode_provider_code_set(encoded) == source_codes
    assert stats.code_count == len(source_codes)
    assert stats.container_count == 3
    assert stats.sparse_container_count == 1
    assert stats.run_container_count == 1
    assert stats.bitmap_container_count == 1
    assert stats.encoded_bytes == len(encoded)


def test_v3_provider_code_set_rejects_noncanonical_order_and_truncation():
    with pytest.raises(ValueError, match="strictly ordered"):
        encode_provider_code_set([2, 1])
    with pytest.raises(ValueError, match="strictly ordered"):
        encode_provider_code_set([1, 1])
    encoded, _stats = encode_provider_code_set(range(100))
    with pytest.raises(ValueError, match="truncated"):
        decode_provider_code_set(encoded[:-1])
    with pytest.raises(ValueError, match="version"):
        decode_provider_code_set(b"\x02")
