# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import hashlib
from copy import deepcopy

import pytest

from process.ptg_parts import ptg2_source_witness_fragments as fragments


def _digest(raw_token: bytes) -> str:
    return hashlib.sha256(raw_token).hexdigest()


def _staged_recipe(raw_token: bytes):
    evidence_by_sha256 = {}
    recipe = fragments.encode_fragment_recipe(raw_token, evidence_by_sha256.__setitem__)
    return recipe, evidence_by_sha256


@pytest.mark.parametrize("raw_token", (b"0", b"x" * 4096, b"x" * 4097, b"x" * 8192))
def test_fragment_recipe_round_trip_boundaries(raw_token):
    recipe, evidence_by_sha256 = _staged_recipe(raw_token)

    assert recipe == {
        "raw_sha256": _digest(raw_token),
        "raw_byte_count": len(raw_token),
        "fragment_sha256": [_digest(raw_token[offset : offset + 4096]) for offset in range(0, len(raw_token), 4096)],
    }
    assert fragments.decode_fragment_recipe(recipe, evidence_by_sha256.__getitem__) == raw_token
    assert fragments.SOURCE_WITNESS_FRAGMENT_BYTES == 4096


def test_fragment_recipe_preserves_exact_token_bytes():
    raw_token = b' \n{ "name":"' + b"x" * 4083 + "\u20ac\U0001f642".encode() + b'", "rate":1.2300e+02 }\t\r\n'
    recipe, evidence_by_sha256 = _staged_recipe(raw_token)

    assert evidence_by_sha256[recipe["fragment_sha256"][0]][-1:] == b"\xe2"
    assert fragments.decode_fragment_recipe(recipe, evidence_by_sha256.__getitem__) == raw_token


def test_distinct_tokens_share_fragments_without_whole_token_aliasing():
    common_fragment = b" " * 4096
    first_token = common_fragment * 2 + b'{"rate":1.00}'
    second_token = common_fragment * 2 + b'{"rate":2.000}'
    evidence_by_sha256 = {}
    staged_calls = []

    def stage_fragment(fragment_digest, raw_fragment):
        staged_calls.append((fragment_digest, raw_fragment))
        assert evidence_by_sha256.setdefault(fragment_digest, raw_fragment) == raw_fragment

    first_recipe = fragments.encode_fragment_recipe(first_token, stage_fragment)
    second_recipe = fragments.encode_fragment_recipe(second_token, stage_fragment)

    assert first_recipe["raw_sha256"] != second_recipe["raw_sha256"]
    assert first_recipe["fragment_sha256"][:2] == second_recipe["fragment_sha256"][:2]
    assert len(staged_calls) == 6
    assert len(evidence_by_sha256) == 3
    assert sum(map(len, evidence_by_sha256.values())) < len(first_token) + len(second_token)
    assert fragments.decode_fragment_recipe(first_recipe, evidence_by_sha256.__getitem__) == first_token
    assert fragments.decode_fragment_recipe(second_recipe, evidence_by_sha256.__getitem__) == second_token


@pytest.mark.parametrize(
    ("field_name", "invalid_value", "message"),
    (
        ("raw_byte_count", -1, "raw byte count"),
        ("raw_byte_count", True, "raw byte count"),
        ("raw_byte_count", "1", "raw byte count"),
        ("raw_sha256", "invalid", "raw digest"),
        ("raw_sha256", 1, "raw digest"),
        ("raw_sha256", "A" * 64, "raw digest"),
        ("raw_sha256", " " + "a" * 64, "raw digest"),
        ("fragment_sha256", [], "reference count"),
        ("fragment_sha256", ["a" * 64, "b" * 64], "reference count"),
        ("fragment_sha256", ("a" * 64,), "reference count"),
        ("fragment_sha256", ["invalid"], "fragment digest"),
        ("fragment_sha256", ["A" * 64], "fragment digest"),
        ("fragment_sha256", [{"fragment_sha256": []}], "fragment digest"),
    ),
)
def test_fragment_recipe_rejects_metadata_before_lookup(field_name, invalid_value, message):
    recipe, _evidence_by_sha256 = _staged_recipe(b"x")
    recipe[field_name] = invalid_value

    def forbidden_lookup(_digest):
        pytest.fail("invalid metadata must not reach fragment lookup")

    with pytest.raises(RuntimeError, match=message):
        fragments.decode_fragment_recipe(recipe, forbidden_lookup)


@pytest.mark.parametrize("invalid_recipe", (None, [], {}, {"unexpected": 1}))
def test_fragment_recipe_rejects_incomplete_field_sets(invalid_recipe):
    with pytest.raises(RuntimeError, match="fields are invalid"):
        fragments.decode_fragment_recipe(invalid_recipe, lambda _digest: pytest.fail("unexpected lookup"))


def test_fragment_recipe_rejects_unknown_fields():
    recipe, _evidence_by_sha256 = _staged_recipe(b"x")
    recipe["nested_recipe"] = recipe.copy()

    with pytest.raises(RuntimeError, match="fields are invalid"):
        fragments.decode_fragment_recipe(recipe, lambda _digest: pytest.fail("unexpected lookup"))


def test_fragment_recipe_rejects_empty_tokens_before_callbacks():
    recipe_by_field = {"raw_sha256": _digest(b""), "raw_byte_count": 0, "fragment_sha256": []}

    def forbidden_callback(*_arguments):
        pytest.fail("empty tokens must not reach callbacks")

    with pytest.raises(RuntimeError, match="byte count must be positive"):
        fragments.encode_fragment_recipe(b"", forbidden_callback)
    with pytest.raises(RuntimeError, match="byte count must be positive"):
        fragments.decode_fragment_recipe(recipe_by_field, forbidden_callback)


@pytest.mark.parametrize("invalid_fragment", (b"", b"xx", "x", bytearray(b"x"), memoryview(b"x")))
def test_fragment_recipe_rejects_fragment_lengths_and_types(invalid_fragment):
    recipe, _evidence_by_sha256 = _staged_recipe(b"x")

    with pytest.raises(RuntimeError, match="fragment length is invalid"):
        fragments.decode_fragment_recipe(recipe, lambda _digest: invalid_fragment)


def test_fragment_recipe_rejects_fragment_digest_tampering():
    recipe, _evidence_by_sha256 = _staged_recipe(b"x")

    with pytest.raises(RuntimeError, match="fragment digest is invalid"):
        fragments.decode_fragment_recipe(recipe, lambda _digest: b"y")


def test_fragment_recipe_rejects_fragment_order_tampering():
    recipe, evidence_by_sha256 = _staged_recipe(b"x" * 4096 + b"y" * 4096)
    recipe["fragment_sha256"].reverse()

    with pytest.raises(RuntimeError, match="reconstructed token digest is invalid"):
        fragments.decode_fragment_recipe(recipe, evidence_by_sha256.__getitem__)


def test_fragment_recipe_rejects_whole_digest_tampering():
    recipe, evidence_by_sha256 = _staged_recipe(b"x")
    recipe["raw_sha256"] = _digest(b"y")

    with pytest.raises(RuntimeError, match="reconstructed token digest is invalid"):
        fragments.decode_fragment_recipe(recipe, evidence_by_sha256.__getitem__)


@pytest.mark.parametrize("raw_byte_count", (4096, 4098))
def test_fragment_recipe_rejects_tail_length_tampering(raw_byte_count):
    recipe, evidence_by_sha256 = _staged_recipe(b"x" * 4096 + b"y")
    recipe["raw_byte_count"] = raw_byte_count

    with pytest.raises(RuntimeError, match="reference count|fragment length"):
        fragments.decode_fragment_recipe(recipe, evidence_by_sha256.__getitem__)


def test_fragment_recipe_rejects_large_full_fragment():
    recipe, evidence_by_sha256 = _staged_recipe(b"x" * 4096 + b"y")
    evidence_by_sha256[recipe["fragment_sha256"][0]] += b"x"

    with pytest.raises(RuntimeError, match="fragment length is invalid"):
        fragments.decode_fragment_recipe(recipe, evidence_by_sha256.__getitem__)


def test_fragment_recipe_snapshots_references_before_lookup():
    recipe, evidence_by_sha256 = _staged_recipe(b"x" * 4096 + b"y")
    original_recipe = deepcopy(recipe)

    def lookup_fragment(fragment_digest):
        recipe["fragment_sha256"].clear()
        return evidence_by_sha256[fragment_digest]

    assert fragments.decode_fragment_recipe(recipe, lookup_fragment) == b"x" * 4096 + b"y"
    assert len(original_recipe["fragment_sha256"]) == 2


@pytest.mark.parametrize("callback_error", (KeyError("missing"), OSError("unavailable"), asyncio.CancelledError()))
def test_fragment_recipe_propagates_callback_failures(callback_error):
    recipe, _evidence_by_sha256 = _staged_recipe(b"x")

    def fail_callback(*_arguments):
        raise callback_error

    with pytest.raises(type(callback_error)) as encoded_failure:
        fragments.encode_fragment_recipe(b"x", fail_callback)
    assert encoded_failure.value is callback_error
    with pytest.raises(type(callback_error)) as decoded_failure:
        fragments.decode_fragment_recipe(recipe, fail_callback)
    assert decoded_failure.value is callback_error


def test_fragment_recipe_enforces_token_bounds_before_callbacks(monkeypatch):
    assert fragments.PTG2_V3_SOURCE_WITNESS_MAX_DECODED_RECORD_BYTES == 64 * 1024 * 1024
    monkeypatch.setattr(fragments, "PTG2_V3_SOURCE_WITNESS_MAX_DECODED_RECORD_BYTES", 4096)
    recipe, evidence_by_sha256 = _staged_recipe(b"x" * 4096)
    assert fragments.decode_fragment_recipe(recipe, evidence_by_sha256.__getitem__) == b"x" * 4096
    recipe["raw_byte_count"] = 4097

    def forbidden_callback(*_arguments):
        pytest.fail("oversized tokens must not reach callbacks")

    with pytest.raises(fragments.WitnessPayloadLimitError, match="decoded byte bound"):
        fragments.encode_fragment_recipe(b"x" * 4097, forbidden_callback)
    with pytest.raises(fragments.WitnessPayloadLimitError, match="decoded byte bound"):
        fragments.decode_fragment_recipe(recipe, forbidden_callback)


@pytest.mark.parametrize("invalid_token", ("x", bytearray(b"x"), memoryview(b"x"), None))
def test_fragment_recipe_requires_byte_tokens(invalid_token):
    with pytest.raises(RuntimeError, match="token must be bytes"):
        fragments.encode_fragment_recipe(invalid_token, lambda *_arguments: pytest.fail("unexpected sink"))
