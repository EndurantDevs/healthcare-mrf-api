# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Raw family equality parity, protocol framing and admitted-byte boundaries."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime
from decimal import MAX_EMAX, MIN_ETINY, ROUND_FLOOR, Decimal, localcontext
from itertools import product
from types import SimpleNamespace

import pytest

from process.custom_import import family_raw_key
from process.custom_import.definition import ChildCollection, KeyPart, canonical_json
from process.custom_import.family import _key, _parent_key, normalize_source_decimal
from process.custom_import.family_raw_key import (
    RAW_FAMILY_KEY_CONTRACT,
    RawFamilyKeyError,
    raw_family_key_evidence,
)


def test_exact_document_and_domain_separated_digest():
    expected = (
        '{"contract":"custom-import/raw-family-key/v1","values":['
        '{"coefficient":"1","exponent":0,"sign":0,"type":"number"},'
        '{"type":"string","value":"1.0"}]}'
    )
    canonical, digest = raw_family_key_evidence((True, "1.0"), maximum_canonical_bytes=256)
    assert canonical == expected
    assert canonical_json(json.loads(canonical)) == canonical
    assert digest == hashlib.sha256(b"custom-import/raw-family-key/v1:" + expected.encode()).digest()
    assert len(digest) == 32


def test_python_scalar_tuple_equality_is_preserved():
    scalar_values = (
        False,
        0,
        Decimal("-0E+1000000"),
        True,
        1,
        Decimal("1.00"),
        Decimal("10E-1"),
        -1,
        Decimal("-1.000"),
        1000,
        Decimal("1E+3"),
        Decimal("1000.00"),
        Decimal("0.1"),
        Decimal("0.10"),
        "1",
        "1.0",
        "1.00",
        "",
        "\u00e9",
        "e\u0301",
        Decimal("1E+1000000"),
        Decimal("10E+999999"),
        Decimal("1E-1000000"),
        Decimal("10E-1000001"),
        10**5000,
        Decimal("1E+5000"),
    )
    evidence_entries = tuple(
        raw_family_key_evidence((scalar_value,), maximum_canonical_bytes=256) for scalar_value in scalar_values
    )
    for left, right in product(range(len(scalar_values)), repeat=2):
        is_equal = (scalar_values[left],) == (scalar_values[right],)
        assert (evidence_entries[left][0] == evidence_entries[right][0]) is is_equal
        assert (evidence_entries[left][1] == evidence_entries[right][1]) is is_equal


def test_raw_strings_stay_distinct_from_typed_decimal_aliases():
    assert normalize_source_decimal("1.0") == normalize_source_decimal("1.00")
    left = raw_family_key_evidence(("1.0",), maximum_canonical_bytes=256)
    right = raw_family_key_evidence(("1.00",), maximum_canonical_bytes=256)
    assert left != right
    assert raw_family_key_evidence((1,), maximum_canonical_bytes=256) != left


def test_numeric_tokens_cannot_alias_source_token_strings():
    number = raw_family_key_evidence((1,), maximum_canonical_bytes=512)
    token_text = canonical_json(json.loads(number[0])["values"][0])
    for value in ("1", "1e0", "0:1:0", token_text):
        assert raw_family_key_evidence((value,), maximum_canonical_bytes=512) != number


@pytest.mark.parametrize(
    ("root_value", "parent_value", "is_equal"),
    [
        ("1.0", "1.00", False),
        ("1.0", "1.0", True),
        (True, Decimal("1.00"), True),
        (1000, Decimal("1E+3"), True),
        ("\u00e9", "e\u0301", False),
    ],
)
def test_existing_root_and_parent_boundary_preserves_presence_identity(root_value, parent_value, is_equal):
    collection = ChildCollection(
        name="items",
        parent_key=(KeyPart(child_field="scope", root_field="scope"), KeyPart(child_field="parent", root_field="id")),
        child_key=("child",),
    )
    root_key = _key({"scope": "a", "id": root_value}, ("scope", "id"))
    parent_key = _parent_key({"scope": "a", "parent": parent_value}, collection)
    assert (root_key == parent_key) is is_equal
    root_evidence = raw_family_key_evidence(root_key, maximum_canonical_bytes=512)
    parent_evidence = raw_family_key_evidence(parent_key, maximum_canonical_bytes=512)
    assert (root_evidence == parent_evidence) is is_equal


@pytest.mark.parametrize(
    ("left", "right", "is_equal"),
    [
        (("a", "bc"), ("ab", "c"), False),
        (("x",), ("x", ""), False),
        ((1, "x"), ("x", 1), False),
        ((1, "x"), (True, "x"), True),
        (("1.0", Decimal("2.00")), ("1.0", 2), True),
        (("a\x00b", "c"), ("a", "b\x00c"), False),
    ],
)
def test_tuple_order_arity_and_scalar_framing(left, right, is_equal):
    left_evidence = raw_family_key_evidence(left, maximum_canonical_bytes=512)
    right_evidence = raw_family_key_evidence(right, maximum_canonical_bytes=512)
    assert (left_evidence == right_evidence) is is_equal


def test_decimal_context_precision_rounding_and_traps_cannot_change_evidence():
    values = (
        Decimal("123456789012345678901234567890.12345678901230000"),
        Decimal("-1.2300E-1000000"),
        Decimal((0, (1,), MAX_EMAX)),
        Decimal((0, (1,), MIN_ETINY)),
        Decimal((1, (0,), MIN_ETINY)),
        10**5000,
    )
    expected = raw_family_key_evidence(values, maximum_canonical_bytes=1024)
    with localcontext() as context:
        context.prec = 1
        context.rounding = ROUND_FLOOR
        context.Emax = 1
        context.Emin = -1
        context.clamp = 1
        context.capitals = 0
        for signal in context.traps:
            context.traps[signal] = True
        context.clear_flags()
        assert raw_family_key_evidence(values, maximum_canonical_bytes=1024) == expected
        assert not any(context.flags.values())


@pytest.mark.parametrize("exponent", [1000000, -1000000, MAX_EMAX, MIN_ETINY])
def test_large_exponents_are_stored_without_expansion(exponent):
    value = Decimal((1, (1,), exponent))
    canonical, _digest = raw_family_key_evidence((value,), maximum_canonical_bytes=256)
    assert len(canonical.encode("utf-8")) < 160
    assert json.loads(canonical)["values"] == [{"type": "number", "sign": 1, "coefficient": "1", "exponent": exponent}]


def test_large_integer_conversion_and_zero_trimming_ignore_string_digit_limit():
    integer_evidence = raw_family_key_evidence((10**5000,), maximum_canonical_bytes=256)
    assert integer_evidence == raw_family_key_evidence((Decimal("1E+5000"),), maximum_canonical_bytes=256)
    assert raw_family_key_evidence((Decimal("1." + "0" * 5000),), maximum_canonical_bytes=256) == (
        raw_family_key_evidence((True,), maximum_canonical_bytes=256)
    )


@pytest.mark.parametrize(
    "values",
    [
        ("plain",),
        ("\u00e9\U0001f642",),
        ('\x00\n\\"',),
        ("\u00e9" * 10, "\U0001f642" * 10, Decimal("100.00")),
    ],
)
def test_bound_includes_utf8_escaping_frame_and_all_tuple_components(values):
    expected = raw_family_key_evidence(values, maximum_canonical_bytes=2048)
    byte_count = len(expected[0].encode("utf-8"))
    assert raw_family_key_evidence(values, maximum_canonical_bytes=byte_count) == expected
    with pytest.raises(RawFamilyKeyError) as error:
        raw_family_key_evidence(values, maximum_canonical_bytes=byte_count - 1)
    assert str(error.value) == "raw family key exceeds the canonical byte limit"


@pytest.mark.parametrize(
    ("value", "limit"),
    [("x", 1), ("x" * 1000, 256), (Decimal("1" * 1000), 256), (10**5000 + 1, 256)],
    ids=["frame", "string", "coefficient", "integer"],
)
def test_oversized_coefficients_and_strings_fail_before_json_encoding(monkeypatch, value, limit):
    def unexpected_encoding(_document):
        pytest.fail("oversized raw value reached JSON encoding")

    monkeypatch.setattr(family_raw_key, "canonical_json", unexpected_encoding)
    with pytest.raises(RawFamilyKeyError) as error:
        raw_family_key_evidence((value,), maximum_canonical_bytes=limit)
    assert str(error.value) == "raw family key exceeds the canonical byte limit"


@pytest.mark.parametrize("value", [None, [], {}, set(), bytearray(b"x")])
def test_missing_unhashable_root_and_parent_keys_keep_existing_no_evidence(value):
    collection = ChildCollection(
        name="items", parent_key=(KeyPart(child_field="parent", root_field="root"),), child_key=("child",)
    )
    assert _key({"root": value}, ("root",)) is None
    assert _parent_key({"parent": value}, collection) is None
    assert raw_family_key_evidence(_key({"root": value}, ("root",)), maximum_canonical_bytes=256) is None
    assert raw_family_key_evidence(_parent_key({"parent": value}, collection), maximum_canonical_bytes=256) is None
    assert raw_family_key_evidence((value,), maximum_canonical_bytes=256) is None


def test_absent_fields_and_nested_unhashable_values_use_the_existing_boundary():
    assert raw_family_key_evidence(_key({}, ("missing",)), maximum_canonical_bytes=256) is None
    assert raw_family_key_evidence(_key({"key": ([],)}, ("key",)), maximum_canonical_bytes=256) is None
    assert raw_family_key_evidence(("x" * 1000, None), maximum_canonical_bytes=1) is None
    assert raw_family_key_evidence(None, maximum_canonical_bytes=1) is None


@pytest.mark.parametrize("limit", [0, -1, True, False, 1.0, "256", None])
def test_invalid_admitted_byte_limit_has_an_exact_error(limit):
    with pytest.raises(RawFamilyKeyError) as error:
        raw_family_key_evidence(None, maximum_canonical_bytes=limit)
    assert str(error.value) == "raw family key byte limit must be a positive integer"


@pytest.mark.parametrize("value", [(), [], {}, "x", 1])
def test_malformed_key_tuple_has_an_exact_error(value):
    with pytest.raises(RawFamilyKeyError) as error:
        raw_family_key_evidence(value, maximum_canonical_bytes=256)
    assert str(error.value) == "raw family key must be a non-empty tuple"


@pytest.mark.parametrize("value", [Decimal("NaN"), Decimal("sNaN"), Decimal("Infinity"), Decimal("-Infinity")])
def test_nonfinite_decimals_fail_without_context_signals(value):
    with localcontext() as context:
        for signal in context.traps:
            context.traps[signal] = True
        context.clear_flags()
        with pytest.raises(RawFamilyKeyError) as error:
            raw_family_key_evidence((value,), maximum_canonical_bytes=256)
        assert str(error.value) == "raw family key numbers must be finite"
        assert not any(context.flags.values())


@pytest.mark.parametrize(
    "value",
    [1.0, float("nan"), float("inf"), b"x", (1,), frozenset({1}), date(2020, 1, 1), datetime(2020, 1, 1, tzinfo=UTC)],
)
def test_other_adapter_types_do_not_enter_the_scalar_domain(value):
    with pytest.raises(RawFamilyKeyError) as error:
        raw_family_key_evidence((value,), maximum_canonical_bytes=256)
    assert str(error.value) == "raw family key contains an unsupported scalar"


def test_arbitrary_objects_are_neither_hashed_nor_serialized():
    class Unsupported:
        def __hash__(self):
            pytest.fail("unsupported object was hashed")

        def __str__(self):
            pytest.fail("unsupported object was serialized")

    with pytest.raises(RawFamilyKeyError) as error:
        raw_family_key_evidence((Unsupported(),), maximum_canonical_bytes=256)
    assert str(error.value) == "raw family key contains an unsupported scalar"


def test_scalar_subclasses_with_custom_equality_are_not_decoder_values():
    class CustomInteger(int):
        def __eq__(self, _other):
            pytest.fail("custom equality was invoked")

        __hash__ = int.__hash__

    class CustomString(str):
        pass

    class CustomDecimal(Decimal):
        pass

    for value in (CustomInteger(1), CustomString("x"), CustomDecimal("1")):
        with pytest.raises(RawFamilyKeyError) as error:
            raw_family_key_evidence((value,), maximum_canonical_bytes=256)
        assert str(error.value) == "raw family key contains an unsupported scalar"


def test_unencodable_string_has_no_source_text_in_its_error():
    with pytest.raises(RawFamilyKeyError) as error:
        raw_family_key_evidence(("\ud800",), maximum_canonical_bytes=256)
    assert str(error.value) == "raw family key must be valid UTF-8"
    assert error.value.__cause__ is None
    assert error.value.__suppress_context__


def test_caller_retains_documents_even_when_digest_buckets_collide(monkeypatch):
    monkeypatch.setattr(
        family_raw_key,
        "hashlib",
        SimpleNamespace(sha256=lambda _payload: SimpleNamespace(digest=lambda: b"x" * 32)),
    )
    left = raw_family_key_evidence(("first",), maximum_canonical_bytes=256)
    right = raw_family_key_evidence(("second",), maximum_canonical_bytes=256)
    assert left[1] == right[1]
    assert left[0] != right[0]
    assert json.loads(left[0])["contract"] == RAW_FAMILY_KEY_CONTRACT
