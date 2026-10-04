# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Authenticate bounded scanner fragment frames before token materialization."""

from __future__ import annotations

import hashlib
import zlib
from dataclasses import replace
from io import BytesIO

import pytest

from process.ptg_parts import ptg2_source_witness_fragment_bundle as bundle
from process.ptg_parts.ptg2_source_witness_contract import SourceWitnessBundleIdentity
from process.ptg_parts.ptg2_source_witness_primitives import U32

TOKEN = b'{"rate":1.2300e+02,"name":"\xe2\x82\xac"}'
DIGEST = hashlib.sha256(TOKEN).hexdigest()


def _frames(*, fragments=None, recipes=None):
    fragments = [(DIGEST, len(TOKEN), zlib.compress(TOKEN))] if fragments is None else fragments
    recipes = [(DIGEST, len(TOKEN), [DIGEST])] if recipes is None else recipes
    raw = bytearray(U32.pack(len(fragments)))
    for digest, length, compressed in fragments:
        raw.extend(bytes.fromhex(digest) + U32.pack(length) + U32.pack(len(compressed)) + compressed)
    raw.extend(U32.pack(len(recipes)))
    for digest, length, references in recipes:
        raw.extend(bytes.fromhex(digest) + U32.pack(length) + U32.pack(len(references)))
        raw.extend(b"".join(bytes.fromhex(reference) for reference in references))
    header_by_field = {
        "evidence_encoding": "fixed_byte_fragments_v1",
        "fragment_byte_count": 4096,
        "fragment_count": len(fragments),
        "recipe_reference_count": sum(len(references) for _, _, references in recipes),
        "evidence_reconstructed_bytes": sum(length for _, length, _ in recipes),
    }
    return bytes(raw), header_by_field


def _identity(raw):
    return SourceWitnessBundleIdentity("synthetic", hashlib.sha256(raw).hexdigest(), len(raw), 1, 2, 3)


def _read(raw, header, *, identity=None, maximum=2):
    return bundle.read_fragment_evidence_locators(
        BytesIO(raw), bundle_identity=identity or _identity(raw), maximum_evidence_count=maximum, header=header
    )


@pytest.mark.parametrize("field,value", [("evidence_encoding", "unknown"), ("fragment_byte_count", 2048)])
def test_scanner_rejects_unknown_fragment_encoding_before_reading(field, value):
    raw, header = _frames()
    with pytest.raises(RuntimeError, match="encoding"):
        _read(b"", {**header, field: value})


@pytest.mark.parametrize("field", ["fragment_count", "recipe_reference_count", "evidence_reconstructed_bytes"])
def test_scanner_rejects_metrics_not_bound_to_actual_frames(field):
    raw, header = _frames()
    with pytest.raises(RuntimeError, match="metrics"):
        _read(raw, {**header, field: header[field] + 1})


@pytest.mark.parametrize("corruption", ["duplicate", "zero_raw", "oversized_raw", "empty_stored", "truncated"])
def test_scanner_rejects_noncanonical_or_truncated_fragment_frames(corruption):
    fragments = [(DIGEST, len(TOKEN), zlib.compress(TOKEN))]
    if corruption == "duplicate":
        fragments *= 2
    elif corruption == "zero_raw":
        fragments[0] = (DIGEST, 0, fragments[0][2])
    elif corruption == "oversized_raw":
        fragments[0] = (DIGEST, 4097, fragments[0][2])
    elif corruption == "empty_stored":
        fragments[0] = (DIGEST, len(TOKEN), b"")
    raw, header = _frames(fragments=fragments)
    identity = replace(_identity(raw), byte_count=44) if corruption == "truncated" else _identity(raw)
    with pytest.raises(RuntimeError, match="fragment framing"):
        _read(raw, header, identity=identity)


@pytest.mark.parametrize(
    "recipes",
    [
        [(DIGEST, len(TOKEN), [DIGEST])] * 2,
        [(DIGEST, 0, [])],
        [(DIGEST, len(TOKEN), ["00" * 32])],
        [(DIGEST, len(TOKEN) + 1, [DIGEST])],
        [(DIGEST, len(TOKEN), [DIGEST, DIGEST])],
    ],
    ids=["duplicate", "zero_raw", "missing_reference", "wrong_length", "ref_count"],
)
def test_scanner_rejects_noncanonical_recipe_identity_and_reference_shape(recipes):
    raw, header = _frames(recipes=recipes)
    with pytest.raises(RuntimeError, match="recipe framing|fragment reference"):
        _read(raw, header)


@pytest.mark.parametrize(
    "constant",
    [
        "PTG2_V3_SOURCE_WITNESS_MAX_FRAGMENTS",
        "PTG2_V3_SOURCE_WITNESS_MAX_DECODED_TOTAL_BYTES",
        "PTG2_V3_SOURCE_WITNESS_MAX_RECORD_BYTES",
        "PTG2_V3_SOURCE_WITNESS_MAX_FRAGMENT_REFERENCES",
        "PTG2_V3_SOURCE_WITNESS_MAX_RECONSTRUCTED_BYTES",
    ],
)
def test_scanner_enforces_storage_and_reconstruction_budgets(monkeypatch, constant):
    raw, header = _frames()
    monkeypatch.setattr(bundle, constant, 0)
    with pytest.raises(RuntimeError, match="bound|framing"):
        _read(raw, header)


def test_scanner_rejects_recipe_count_truncation_and_unreferenced_fragments():
    raw, header = _frames()
    with pytest.raises(RuntimeError, match="recipe count"):
        _read(raw, header, maximum=0)
    with pytest.raises(RuntimeError, match="recipe framing"):
        _read(raw, header, identity=replace(_identity(raw), byte_count=len(raw) - 1))
    raw, header = _frames(recipes=[])
    with pytest.raises(RuntimeError, match="coverage"):
        _read(raw, header)


@pytest.mark.parametrize("corruption", ["no_recipe", "partial_reference", "missing_fragment", "digest"])
def test_materialization_refuses_missing_or_corrupt_authenticated_evidence(corruption):
    raw, header = _frames()
    locator = _read(raw, header)[DIGEST]
    if corruption == "no_recipe":
        locator = replace(locator, fragments_by_sha256=None)
    elif corruption == "partial_reference":
        locator = replace(locator, length=locator.length - 1)
    elif corruption == "missing_fragment":
        locator = replace(locator, fragments_by_sha256={})
    elif corruption == "digest":
        fragment = locator.fragments_by_sha256[DIGEST]
        corrupted = zlib.compress(b"x" * len(TOKEN))
        raw = raw[: fragment.offset] + corrupted + raw[fragment.offset + fragment.length :]
        locator = replace(
            locator,
            offset=locator.offset + len(corrupted) - fragment.length,
            fragments_by_sha256={DIGEST: replace(fragment, length=len(corrupted))},
        )
    with pytest.raises(RuntimeError, match="missing|truncated|digest"):
        bundle.read_fragment_locator_token(BytesIO(raw), _identity(raw), locator)


def test_materialization_is_lossless_and_does_not_consume_other_frames():
    raw, header = _frames()
    locators = _read(raw, header)
    with BytesIO(raw) as source:
        assert bundle.read_fragment_locator_token(source, _identity(raw), locators[DIGEST]) == TOKEN
        assert source.getvalue() == raw
        assert bundle.read_fragment_locator_token(source, _identity(raw), locators[DIGEST]) == TOKEN
