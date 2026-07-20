# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import pytest

from process.ptg_parts import ptg2_serving_binary_v3 as codec
from process.ptg_parts.ptg2_serving_binary_v3_types import (
    PTG2V3PriceAtomRecord as Atom,
)


def _memberships(rows):
    return codec.encode_price_memberships(rows, 24)


def test_price_membership_full_decoder_rejects_record_corruption():
    encoded = _memberships(((0, (1,)),))
    records_offset = codec._price_membership_header(encoded).records_offset

    empty_membership = bytearray(encoded)
    empty_membership[records_offset + 1] = 0
    with pytest.raises(ValueError, match="cannot be empty"):
        codec.decode_price_memberships(empty_membership)

    unordered_atoms = bytearray(_memberships(((0, (1, 2)),)))
    records_offset = codec._price_membership_header(unordered_atoms).records_offset
    unordered_atoms[records_offset + 2 : records_offset + 8] = (
        codec.encode_dense_keys((2, 1), 24)
    )
    with pytest.raises(ValueError, match="atom keys are not ordered"):
        codec.decode_price_memberships(unordered_atoms)

    with pytest.raises(ValueError, match="trailing bytes"):
        codec.decode_price_memberships(encoded + b"\0")


def test_price_membership_sparse_decoder_guards_metadata_and_empty_requests():
    encoded = _memberships(((0, (1,)),))

    assert codec.decode_price_memberships_for_keys(b"", ()) == {}
    with pytest.raises(ValueError, match="entry count does not match"):
        codec.decode_price_memberships_for_keys(
            encoded,
            (0,),
            expected_entry_count=2,
        )
    with pytest.raises(ValueError, match="width disagrees"):
        codec.decode_price_memberships_for_keys(
            encoded,
            (0,),
            expected_atom_key_bits=32,
        )
    with pytest.raises(ValueError, match="payload version"):
        codec.decode_price_memberships(b"")


def test_price_membership_directory_rejects_invalid_checkpoints():
    encoded = _memberships(tuple((index, (index,)) for index in range(33)))

    invalid_key = bytearray(encoded)
    invalid_key[10] = 0
    with pytest.raises(ValueError, match="checkpoint key is invalid"):
        codec.price_membership_entry_count(invalid_key)

    invalid_start = bytearray(_memberships(((0, (1,)),)))
    invalid_start[5] = 1
    with pytest.raises(ValueError, match="must start at zero"):
        codec.price_membership_entry_count(invalid_start)

    invalid_offset = bytearray(encoded)
    invalid_offset[11:15] = (0).to_bytes(4, "little")
    with pytest.raises(ValueError, match="checkpoint offsets are invalid"):
        codec.price_membership_entry_count(invalid_offset)

    three_checkpoints = bytearray(
        _memberships(tuple((index, (index,)) for index in range(65)))
    )
    three_checkpoints[15] = 32
    with pytest.raises(ValueError, match="checkpoint keys are invalid"):
        codec.price_membership_entry_count(three_checkpoints)


def test_price_membership_sparse_decoder_rejects_segment_corruption():
    repeated_key = bytearray(_memberships(((0, (1,)), (1, (2,)))))
    records_offset = codec._price_membership_header(repeated_key).records_offset
    repeated_key[records_offset + 5] = 0
    with pytest.raises(ValueError, match="not strictly ordered"):
        codec.decode_price_memberships_for_keys(repeated_key, (0,))

    empty_membership = bytearray(_memberships(((0, (1,)),)))
    records_offset = codec._price_membership_header(empty_membership).records_offset
    empty_membership[records_offset + 1] = 0
    with pytest.raises(ValueError, match="cannot be empty"):
        codec.decode_price_memberships_for_keys(empty_membership, (0,))

    encoded = _memberships(((0, (1,)),))
    with pytest.raises(ValueError, match="atom keys are truncated"):
        codec.decode_price_memberships_for_keys(encoded[:-1], (0,))

    unordered_atoms = bytearray(_memberships(((0, (1, 2)),)))
    records_offset = codec._price_membership_header(unordered_atoms).records_offset
    unordered_atoms[records_offset + 2 : records_offset + 8] = (
        codec.encode_dense_keys((2, 1), 24)
    )
    with pytest.raises(ValueError, match="atom keys are not ordered"):
        codec.decode_price_memberships_for_keys(unordered_atoms, (0,))

    invalid_segment_end = bytearray(
        _memberships(tuple((index, (index,)) for index in range(33)))
    )
    invalid_segment_end[11:15] = (159).to_bytes(4, "little")
    with pytest.raises(ValueError, match="checkpoint offset is invalid"):
        codec.decode_price_memberships_for_keys(invalid_segment_end, (0,))


@pytest.mark.parametrize(
    ("rows", "message"),
    (
        (((-1, (0,)),), "cannot be negative"),
        (((0, ()),), "cannot be empty"),
        (((0, (1 << 24,)),), "does not fit"),
        (((0, (2, 1)),), "must be ordered"),
    ),
)
def test_price_membership_encoder_rejects_invalid_rows(rows, message):
    with pytest.raises(ValueError, match=message):
        _memberships(rows)


def test_price_atom_full_and_sparse_decoders_guard_metadata():
    encoded = codec.encode_price_atoms((Atom(None, ()),))

    with pytest.raises(ValueError, match="trailing bytes"):
        codec.decode_price_atoms(encoded + b"\0")
    assert codec.decode_price_atoms_for_offsets(b"", ()) == {}
    with pytest.raises(ValueError, match="count does not match"):
        codec.decode_price_atoms_for_offsets(
            encoded,
            (0,),
            expected_entry_count=2,
        )
    with pytest.raises(ValueError, match="count exceeds"):
        codec.decode_price_atoms_for_offsets(
            encoded,
            (0,),
            maximum_entry_count=0,
        )
    with pytest.raises(ValueError, match="payload version"):
        codec.decode_price_atoms(b"")


@pytest.mark.parametrize(
    ("atoms", "message"),
    (
        ((Atom(None, ()), Atom(None, (1,))), "same attribute-key count"),
        ((Atom(None, (-1,)),), "attribute keys cannot be negative"),
        ((Atom(1, ()),), "must be text or None"),
        ((Atom("\ud800", ()),), "text is not valid UTF-8"),
    ),
)
def test_price_atom_encoder_rejects_invalid_records(atoms, message):
    with pytest.raises(ValueError, match=message):
        codec.encode_price_atoms(atoms)


def test_price_atom_directory_and_sparse_decoder_reject_bad_offsets():
    encoded = codec.encode_price_atoms((Atom(None, ()),))
    invalid_start = bytearray(encoded)
    invalid_start[5:9] = (1).to_bytes(4, "little")
    with pytest.raises(ValueError, match="must start at zero"):
        codec.price_atom_entry_count(invalid_start)

    many_atoms = codec.encode_price_atoms(tuple(Atom(None, ()) for _ in range(33)))
    invalid_directory = bytearray(many_atoms)
    invalid_directory[9:13] = (0).to_bytes(4, "little")
    with pytest.raises(ValueError, match="checkpoint offsets are invalid"):
        codec.price_atom_entry_count(invalid_directory)

    invalid_segment_end = bytearray(many_atoms)
    invalid_segment_end[9:13] = (31).to_bytes(4, "little")
    with pytest.raises(ValueError, match="checkpoint offset is invalid"):
        codec.decode_price_atoms_for_offsets(invalid_segment_end, (0,))


@pytest.mark.parametrize(
    ("payload", "message"),
    (
        (b"\x01\x00\x01\x03A", "text is truncated"),
        (b"\x01\x00\x01\x02\xff", "text is not valid UTF-8"),
    ),
)
def test_price_atom_decoder_rejects_invalid_text(payload, message):
    with pytest.raises(ValueError, match=message):
        codec.decode_price_atoms(payload)
