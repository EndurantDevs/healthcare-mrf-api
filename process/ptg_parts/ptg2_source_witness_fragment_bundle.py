# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Read bounded byte-fragment locators without retaining decoded source tokens."""

from __future__ import annotations

import hashlib
import os
from typing import Any, BinaryIO, Mapping

from process.ptg_parts.ptg2_source_witness_bundle import _decompress_evidence
from process.ptg_parts.ptg2_source_witness_contract import (
    PTG2_V3_SOURCE_WITNESS_FRAGMENT_BYTES,
    PTG2_V3_SOURCE_WITNESS_MAX_DECODED_RECORD_BYTES,
    PTG2_V3_SOURCE_WITNESS_MAX_DECODED_TOTAL_BYTES,
    PTG2_V3_SOURCE_WITNESS_MAX_FRAGMENT_REFERENCES,
    PTG2_V3_SOURCE_WITNESS_MAX_FRAGMENTS,
    PTG2_V3_SOURCE_WITNESS_MAX_RECONSTRUCTED_BYTES,
    PTG2_V3_SOURCE_WITNESS_MAX_RECORD_BYTES,
    SourceWitnessBundleIdentity,
    SourceWitnessEvidenceLocator,
)
from process.ptg_parts.ptg2_source_witness_locator_reader import (
    _read_exact_file,
    _read_u32_file,
)
from process.ptg_parts.ptg2_source_witness_primitives import nonnegative_int


def _bounded_fragment_locators(
    bundle_file: BinaryIO,
    identity: SourceWitnessBundleIdentity,
) -> dict[str, SourceWitnessEvidenceLocator]:
    count = _read_u32_file(bundle_file, field_name="fragment count")
    if count > PTG2_V3_SOURCE_WITNESS_MAX_FRAGMENTS:
        raise RuntimeError("source witness fragment count exceeds its bound")
    fragment_locator_by_sha256: dict[str, SourceWitnessEvidenceLocator] = {}
    decoded_bytes = 0
    previous_digest = ""
    for _ in range(count):
        digest = _read_exact_file(bundle_file, 32, field_name="fragment digest").hex()
        raw_length = _read_u32_file(bundle_file, field_name="fragment raw length")
        stored_length = _read_u32_file(bundle_file, field_name="fragment stored length")
        offset = bundle_file.tell()
        decoded_bytes += raw_length
        if (
            digest <= previous_digest
            or not 0 < raw_length <= PTG2_V3_SOURCE_WITNESS_FRAGMENT_BYTES
            or decoded_bytes > PTG2_V3_SOURCE_WITNESS_MAX_DECODED_TOTAL_BYTES
            or stored_length <= 0
            or stored_length + 40 > PTG2_V3_SOURCE_WITNESS_MAX_RECORD_BYTES
            or offset + stored_length > identity.byte_count
        ):
            raise RuntimeError("source witness fragment framing is invalid")
        fragment_locator_by_sha256[digest] = SourceWitnessEvidenceLocator(digest, raw_length, offset, stored_length)
        bundle_file.seek(stored_length, os.SEEK_CUR)
        previous_digest = digest
    return fragment_locator_by_sha256


def _read_canonical_recipe(
    bundle_file: BinaryIO,
    identity: SourceWitnessBundleIdentity,
    fragment_locator_by_sha256: Mapping[str, SourceWitnessEvidenceLocator],
    previous_digest: str,
    reference_count: int,
    reconstructed_bytes: int,
    referenced_fragments: set[str],
) -> SourceWitnessEvidenceLocator:
    """Bound one complete recipe frame before reading its canonical references."""

    digest = _read_exact_file(bundle_file, 32, field_name="recipe digest").hex()
    raw_length = _read_u32_file(bundle_file, field_name="recipe raw length")
    ref_count = _read_u32_file(bundle_file, field_name="recipe reference count")
    offset = bundle_file.tell()
    expected_count = (raw_length + PTG2_V3_SOURCE_WITNESS_FRAGMENT_BYTES - 1) // PTG2_V3_SOURCE_WITNESS_FRAGMENT_BYTES
    if (
        digest <= previous_digest
        or not 0 < raw_length <= PTG2_V3_SOURCE_WITNESS_MAX_DECODED_RECORD_BYTES
        or ref_count != expected_count
        or reference_count + ref_count > PTG2_V3_SOURCE_WITNESS_MAX_FRAGMENT_REFERENCES
        or reconstructed_bytes + raw_length > PTG2_V3_SOURCE_WITNESS_MAX_RECONSTRUCTED_BYTES
        or ref_count * 32 + 40 > PTG2_V3_SOURCE_WITNESS_MAX_RECORD_BYTES
        or offset + ref_count * 32 > identity.byte_count
    ):
        raise RuntimeError("source witness recipe framing or work bound is invalid")
    for index in range(ref_count):
        fragment_digest = _read_exact_file(bundle_file, 32, field_name="recipe reference").hex()
        fragment = fragment_locator_by_sha256.get(fragment_digest)
        expected_length = min(
            PTG2_V3_SOURCE_WITNESS_FRAGMENT_BYTES, raw_length - index * PTG2_V3_SOURCE_WITNESS_FRAGMENT_BYTES
        )
        if fragment is None or fragment.raw_byte_count != expected_length:
            raise RuntimeError("source witness recipe fragment reference is invalid")
        referenced_fragments.add(fragment_digest)
    return SourceWitnessEvidenceLocator(digest, raw_length, offset, ref_count * 32, fragment_locator_by_sha256)


def _validate_fragment_metrics(
    header: Mapping[str, Any],
    fragment_count: int,
    reference_count: int,
    reconstructed_bytes: int,
) -> None:
    for field, actual in (
        ("fragment_count", fragment_count),
        ("recipe_reference_count", reference_count),
        ("evidence_reconstructed_bytes", reconstructed_bytes),
    ):
        if nonnegative_int(header, field, error_field_name=field) != actual:
            raise RuntimeError("source witness fragment metrics are inconsistent")


def read_fragment_evidence_locators(
    bundle_file: BinaryIO,
    *,
    bundle_identity: SourceWitnessBundleIdentity,
    maximum_evidence_count: int,
    header: Mapping[str, Any],
) -> dict[str, SourceWitnessEvidenceLocator]:
    """Validate compact recipes and all fragment reference/count/length framing."""

    if (
        header.get("evidence_encoding") != "fixed_byte_fragments_v1"
        or header.get("fragment_byte_count") != PTG2_V3_SOURCE_WITNESS_FRAGMENT_BYTES
    ):
        raise RuntimeError("source witness fragment encoding is invalid")
    fragment_locator_by_sha256 = _bounded_fragment_locators(bundle_file, bundle_identity)
    count = _read_u32_file(bundle_file, field_name="recipe count")
    if count > maximum_evidence_count:
        raise RuntimeError("source witness recipe count exceeds its bound")
    recipe_locator_by_sha256: dict[str, SourceWitnessEvidenceLocator] = {}
    referenced_fragments: set[str] = set()
    reference_count = reconstructed_bytes = 0
    previous_digest = ""
    for _ in range(count):
        recipe = _read_canonical_recipe(
            bundle_file,
            bundle_identity,
            fragment_locator_by_sha256,
            previous_digest,
            reference_count,
            reconstructed_bytes,
            referenced_fragments,
        )
        recipe_locator_by_sha256[recipe.sha256] = recipe
        reference_count += recipe.length // 32
        reconstructed_bytes += recipe.raw_byte_count
        previous_digest = recipe.sha256
    _validate_fragment_metrics(header, len(fragment_locator_by_sha256), reference_count, reconstructed_bytes)
    if referenced_fragments != set(fragment_locator_by_sha256):
        raise RuntimeError("source witness fragment coverage is inconsistent")
    return recipe_locator_by_sha256


def read_fragment_locator_token(
    bundle_file: BinaryIO,
    identity: SourceWitnessBundleIdentity,
    locator: SourceWitnessEvidenceLocator,
) -> bytes:
    """Authenticate one exact original token from immutable fragment locators."""

    from process.ptg_parts.ptg2_source_witness_fragments import decode_fragment_recipe
    from process.ptg_parts.ptg2_source_witness_locator_materialize import _read_locator_payload

    fragment_locator_by_sha256 = locator.fragments_by_sha256
    if fragment_locator_by_sha256 is None:
        raise RuntimeError("source witness fragment recipe is missing")
    references = _read_locator_payload(
        bundle_file, identity, offset=locator.offset, length=locator.length, field_name="recipe"
    )
    if len(references) % 32:
        raise RuntimeError("source witness recipe references are truncated")

    def lookup(digest: str) -> bytes:
        """Read, decompress and authenticate one referenced immutable fragment."""

        fragment = fragment_locator_by_sha256.get(digest)
        if fragment is None:
            raise RuntimeError("source witness fragment is missing")
        compressed = _read_locator_payload(
            bundle_file, identity, offset=fragment.offset, length=fragment.length, field_name="fragment"
        )
        raw_fragment = _decompress_evidence(compressed, fragment.raw_byte_count)
        if hashlib.sha256(raw_fragment).hexdigest() != digest:
            raise RuntimeError("source witness fragment digest is invalid")
        return raw_fragment

    return decode_fragment_recipe(
        {
            "raw_sha256": locator.sha256,
            "raw_byte_count": locator.raw_byte_count,
            "fragment_sha256": [references[offset : offset + 32].hex() for offset in range(0, len(references), 32)],
        },
        lookup,
    )
