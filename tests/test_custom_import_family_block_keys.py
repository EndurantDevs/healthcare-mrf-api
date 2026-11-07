# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Internal ranks preserve binary hash ties and the original collision scope."""

from collections import deque

import pytest

from process.custom_import.family_block_keys import (
    iter_key_dictionary,
    validate_key_hash_scopes,
    validate_key_text_scopes,
)


def _sorted_views(rows):
    return (
        sorted(rows, key=lambda row: (row[0], row[1], row[2], row[3].encode())),
        sorted(rows, key=lambda row: (row[0], row[1], row[3].encode(), row[2])),
    )


def test_equal_hashes_share_rank_and_exact_text_uses_only_a_variant():
    keys = ((b"a" * 32, "alpha"), (b"a" * 32, "alpha"), (b"a" * 32, "beta"), (b"b" * 32, "alpha"), (b"z" * 32, "omega"))
    rows = tuple(iter_key_dictionary(iter(keys)))
    assert [row.key_rank for row in rows] == [1, 1, 2, 3]
    assert [row.key_variant for row in rows] == [1, 2, 1, 1]
    for left in rows:
        for right in rows:
            assert (left.key_rank > right.key_rank) - (left.key_rank < right.key_rank) == (
                (left.child_key_sha256 > right.child_key_sha256) - (left.child_key_sha256 < right.child_key_sha256)
            )


def test_dictionary_and_scoped_guards_use_utf8_c_order():
    texts = ("A", "z", "é", "☃", "🙂")
    rows = tuple(iter_key_dictionary((b"a" * 32, value) for value in texts))
    assert tuple(row.canonical_child_key for row in rows) == texts
    assert tuple(row.key_variant for row in rows) == (1, 2, 3, 4, 5)


def test_collision_checks_do_not_reject_allowed_cross_entity_or_selection_variants():
    rows = [(1, 2024, b"a" * 32, "A"), (2, 2024, b"a" * 32, "B"), (1, 2025, b"a" * 32, "B"), (1, 2026, b"b" * 32, "A")]
    by_hash, by_text = _sorted_views(rows)
    assert validate_key_hash_scopes(iter(by_hash)) == len(rows)
    assert validate_key_text_scopes(iter(by_text)) == len(rows)
    assert len(tuple(iter_key_dictionary(sorted({(row[2], row[3]) for row in rows})))) == 3


@pytest.mark.parametrize("inverse", (False, True))
def test_both_original_collision_directions_refuse_only_inside_the_original_scope(inverse):
    rows = [(1, -2024, b"a" * 32, "A"), (1, -2024, b"b" * 32 if inverse else b"a" * 32, "A" if inverse else "B")]
    by_hash, by_text = _sorted_views(rows)
    rejecting, valid, bad_rows, good_rows = (
        (validate_key_text_scopes, validate_key_hash_scopes, by_text, by_hash)
        if inverse
        else (validate_key_hash_scopes, validate_key_text_scopes, by_hash, by_text)
    )
    assert valid(good_rows) == 2
    with pytest.raises(ValueError, match="collision"):
        rejecting(bad_rows)


def test_exact_repeated_projection_rows_are_not_a_collision():
    rows = [(1, 0, b"a" * 32, "A")] * 3
    assert validate_key_hash_scopes(rows) == validate_key_text_scopes(rows) == 3


@pytest.mark.parametrize("guard", (validate_key_hash_scopes, validate_key_text_scopes))
def test_scoped_view_order_is_checked(guard):
    with pytest.raises(ValueError, match="scope order"):
        guard(((2, 0, b"a" * 32, "A"), (1, 0, b"a" * 32, "A")))


def test_dictionary_rank_has_no_smallint_ceiling():
    rows = iter_key_dictionary((index.to_bytes(32, "big"), str(index)) for index in range(32769))
    last = deque(rows, maxlen=1).pop()
    assert last.key_rank == 32769 and last.key_variant == 1


def test_dictionary_rejects_out_of_order_keys():
    with pytest.raises(ValueError, match="hash/text order"):
        tuple(iter_key_dictionary(((b"b" * 32, "A"), (b"a" * 32, "A"))))


@pytest.mark.parametrize(
    "key", [(b"a", "A"), (b"a" * 32, ""), (b"a" * 32, "a\x00b"), (b"a" * 32, "a" * 8193), (b"a" * 32, "\ud800")]
)
def test_existing_projection_key_domain_is_preserved(key):
    with pytest.raises(ValueError):
        tuple(iter_key_dictionary((key,)))


def test_an_8192_byte_key_remains_valid():
    [row] = iter_key_dictionary(((b"a" * 32, "a" * 8192),))
    assert len(row.canonical_child_key) == 8192
