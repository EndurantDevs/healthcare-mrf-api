# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Authenticate v6 fragment witnesses without retaining decoded token caches."""

from __future__ import annotations

import hashlib
import json
import operator
import zlib
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
from types import MappingProxyType
from typing import Any, TypeVar

from process.ptg_parts.ptg2_source_witness_codec import (
    decode_persisted_record,
    decode_record_locator_fields,
)
from process.ptg_parts.ptg2_source_witness_contract import (
    FRAGMENT_PERSISTED_PAYLOAD_MAGIC,
    PTG2_V3_SOURCE_WITNESS_FRAGMENT_BYTES,
    PTG2_V3_SOURCE_WITNESS_FRAGMENT_COMPRESSION,
    PTG2_V3_SOURCE_WITNESS_FRAGMENT_PAYLOAD_CONTRACT,
    PTG2_V3_SOURCE_WITNESS_MAX_DECODED_RECORD_BYTES,
    PTG2_V3_SOURCE_WITNESS_MAX_DECODED_TOTAL_BYTES,
    PTG2_V3_SOURCE_WITNESS_MAX_FRAGMENT_REFERENCES,
    PTG2_V3_SOURCE_WITNESS_MAX_FRAGMENTS,
    PTG2_V3_SOURCE_WITNESS_MAX_PAYLOAD_BYTES,
    PTG2_V3_SOURCE_WITNESS_MAX_RECIPES,
    PTG2_V3_SOURCE_WITNESS_MAX_RECONSTRUCTED_BYTES,
    PTG2_V3_SOURCE_WITNESS_MAX_RECORD_BYTES,
    PTG2_V3_SOURCE_WITNESS_PAYLOAD_CONTRACT,
    PTG2_V3_SOURCE_WITNESS_TOTAL_TARGET,
    SourceWitnessRecord,
    WitnessPayloadLimitError,
    source_witness_targets,
)
from process.ptg_parts.ptg2_source_witness_fragments import decode_fragment_recipe
from process.ptg_parts.ptg2_source_witness_persisted_decode import (
    _read_header,
    _validate_header_contract,
    _validate_header_scope,
)
from process.ptg_parts.ptg2_source_witness_primitives import nonnegative_int, read_u32, sha256_hex

_MappedResult = TypeVar("_MappedResult")


@dataclass(frozen=True)
class FragmentWitnessView:
    """Expose authenticated metadata and repeatable, record-at-a-time decoding."""

    metadata: Mapping[str, Any]
    records: _FragmentRecords
    evidence_by_sha256: None = None

    def map_records(
        self, mapper: Callable[[SourceWitnessRecord, Mapping[str, Mapping[str, Any]]], _MappedResult]
    ) -> tuple[tuple[_MappedResult, ...], dict[str, int]]:
        """Map compact results in record order, retaining evidence for only one pair.

        The trusted callback must not retain raw records or parsed evidence. Its
        evidence map is shared within one pair group and cleared when it ends.
        Counters include corpus initialization and this pass, not earlier passes.
        """

        indexes_by_pair, work = _record_groups(self.records, _metric(self.metadata, "evidence_reconstructed_bytes"))
        results: list[Any] = [None] * len(self.records)
        for pair, indexes in indexes_by_pair.items():
            _map_record_group(self.records, pair, indexes, mapper, results)
        return tuple(results), _processing_io(self.metadata, work)


@dataclass(frozen=True)
class _Fragment:
    raw_byte_count: int
    offset: int
    length: int


@dataclass(frozen=True)
class _Recipe:
    raw_byte_count: int
    offset: int
    reference_count: int


def _bounded(value: int, maximum: int, field: str) -> None:
    if value > maximum:
        raise WitnessPayloadLimitError(f"source witness fragment {field} exceeds its bound")


def _metric(header: Mapping[str, Any], field: str) -> int:
    return nonnegative_int(header, field, error_field_name=field.replace("_", " "))


def _header(payload: bytes, sources: Sequence[str]) -> tuple[dict[str, Any], int]:
    length, _ = read_u32(payload, len(FRAGMENT_PERSISTED_PAYLOAD_MAGIC), field_name="fragment header")
    _bounded(length + 4, PTG2_V3_SOURCE_WITNESS_MAX_RECORD_BYTES, "header")
    header, offset = _read_header(payload)
    if (
        header.get("contract") != PTG2_V3_SOURCE_WITNESS_FRAGMENT_PAYLOAD_CONTRACT
        or type(header.get("format_version")) is not int
        or header["format_version"] != 6
        or header.get("compression") != PTG2_V3_SOURCE_WITNESS_FRAGMENT_COMPRESSION
        or _metric(header, "fragment_byte_count") != PTG2_V3_SOURCE_WITNESS_FRAGMENT_BYTES
    ):
        raise RuntimeError("source witness fragment contract is invalid")
    _validate_header_contract({**header, "contract": PTG2_V3_SOURCE_WITNESS_PAYLOAD_CONTRACT, "format_version": 5})
    _validate_header_scope(header, sources)
    limit_by_field = {
        "fragment_count": PTG2_V3_SOURCE_WITNESS_MAX_FRAGMENTS,
        "evidence_dictionary_count": PTG2_V3_SOURCE_WITNESS_MAX_RECIPES,
        "evidence_dictionary_raw_bytes": PTG2_V3_SOURCE_WITNESS_MAX_DECODED_TOTAL_BYTES,
        "evidence_dictionary_stored_bytes": PTG2_V3_SOURCE_WITNESS_MAX_PAYLOAD_BYTES,
        "evidence_reconstructed_bytes": PTG2_V3_SOURCE_WITNESS_MAX_RECONSTRUCTED_BYTES,
        "recipe_reference_count": PTG2_V3_SOURCE_WITNESS_MAX_FRAGMENT_REFERENCES,
        "record_count": PTG2_V3_SOURCE_WITNESS_TOTAL_TARGET,
    }
    for field, maximum in limit_by_field.items():
        _bounded(_metric(header, field), maximum, field)
    if _metric(header, "record_count") == 0:
        raise RuntimeError("source witness fragment record count is invalid")
    return header, offset


def _count(payload: bytes, offset: int, expected: int, maximum: int, minimum_frame: int, field: str) -> tuple[int, int]:
    count, offset = read_u32(payload, offset, field_name=field)
    _bounded(count, maximum, field)
    if count != expected or count * minimum_frame > len(payload) - offset:
        raise RuntimeError(f"source witness fragment {field} is invalid")
    return count, offset


def _digest(payload: bytes, offset: int, previous: str) -> tuple[str, int]:
    if offset + 32 > len(payload):
        raise RuntimeError("source witness fragment digest is truncated")
    digest = payload[offset : offset + 32].hex()
    if digest <= previous:
        raise RuntimeError("source witness fragment dictionary order is invalid")
    return digest, offset + 32


def _fragment_bytes(payload: bytes, digest: str, frame: _Fragment) -> bytes:
    decompressor = zlib.decompressobj()
    try:
        raw = decompressor.decompress(payload[frame.offset : frame.offset + frame.length], frame.raw_byte_count + 1)
        if len(raw) > frame.raw_byte_count or decompressor.unconsumed_tail:
            raise RuntimeError("source witness fragment decoded length is invalid")
        raw += decompressor.flush(frame.raw_byte_count - len(raw) + 1)
    except zlib.error as exc:
        raise RuntimeError("source witness fragment zlib framing is invalid") from exc
    if (
        len(raw) != frame.raw_byte_count
        or not decompressor.eof
        or decompressor.unused_data
        or decompressor.unconsumed_tail
        or hashlib.sha256(raw).hexdigest() != digest
    ):
        raise RuntimeError("source witness fragment length, digest or zlib framing is invalid")
    return raw


def _fragments(witness_payload: bytes, offset: int, header: Mapping[str, Any]) -> tuple[dict[str, _Fragment], int]:
    count, offset = _count(
        witness_payload,
        offset,
        _metric(header, "fragment_count"),
        PTG2_V3_SOURCE_WITNESS_MAX_FRAGMENTS,
        41,
        "fragment count",
    )
    fragment_by_sha256: dict[str, _Fragment] = {}
    previous = ""
    raw_bytes = stored_bytes = 0
    for _ in range(count):
        digest, offset = _digest(witness_payload, offset, previous)
        raw_length, offset = read_u32(witness_payload, offset, field_name="fragment raw length")
        stored_length, offset = read_u32(witness_payload, offset, field_name="fragment stored length")
        if not 0 < raw_length <= PTG2_V3_SOURCE_WITNESS_FRAGMENT_BYTES or stored_length == 0:
            raise RuntimeError("source witness fragment length is invalid")
        _bounded(stored_length + 40, PTG2_V3_SOURCE_WITNESS_MAX_RECORD_BYTES, "stored entry")
        if offset + stored_length > len(witness_payload):
            raise RuntimeError("source witness fragment entry is truncated")
        raw_bytes += raw_length
        _bounded(raw_bytes, PTG2_V3_SOURCE_WITNESS_MAX_DECODED_TOTAL_BYTES, "unique decoded bytes")
        frame = _Fragment(raw_length, offset, stored_length)
        _fragment_bytes(witness_payload, digest, frame)
        fragment_by_sha256[digest] = frame
        stored_bytes += stored_length
        offset += stored_length
        previous = digest
    if (raw_bytes, stored_bytes) != (
        _metric(header, "evidence_dictionary_raw_bytes"),
        _metric(header, "evidence_dictionary_stored_bytes"),
    ):
        raise RuntimeError("source witness fragment byte counts do not match")
    return fragment_by_sha256, offset


def _recipes(
    witness_payload: bytes, offset: int, header: Mapping[str, Any], fragments: Mapping[str, _Fragment]
) -> tuple[dict[str, _Recipe], int]:
    count, offset = _count(
        witness_payload,
        offset,
        _metric(header, "evidence_dictionary_count"),
        PTG2_V3_SOURCE_WITNESS_MAX_RECIPES,
        72,
        "recipe count",
    )
    recipe_by_sha256: dict[str, _Recipe] = {}
    used_fragments: set[str] = set()
    previous = ""
    reconstructed_bytes = references = 0
    for _ in range(count):
        digest, offset = _digest(witness_payload, offset, previous)
        raw_length, offset = read_u32(witness_payload, offset, field_name="recipe raw length")
        reference_count, offset = read_u32(witness_payload, offset, field_name="recipe reference count")
        _bounded(raw_length, PTG2_V3_SOURCE_WITNESS_MAX_DECODED_RECORD_BYTES, "token bytes")
        if (
            raw_length == 0
            or reference_count
            != (raw_length + PTG2_V3_SOURCE_WITNESS_FRAGMENT_BYTES - 1) // PTG2_V3_SOURCE_WITNESS_FRAGMENT_BYTES
        ):
            raise RuntimeError("source witness fragment recipe reference count is invalid")
        reconstructed_bytes += raw_length
        references += reference_count
        _bounded(reconstructed_bytes, PTG2_V3_SOURCE_WITNESS_MAX_RECONSTRUCTED_BYTES, "reconstruction work")
        _bounded(references, PTG2_V3_SOURCE_WITNESS_MAX_FRAGMENT_REFERENCES, "references")
        _bounded(40 + reference_count * 32, PTG2_V3_SOURCE_WITNESS_MAX_RECORD_BYTES, "stored recipe")
        end = offset + reference_count * 32
        if end > len(witness_payload):
            raise RuntimeError("source witness fragment recipe is truncated")
        recipe_by_sha256[digest] = _Recipe(raw_length, offset, reference_count)
        for index in range(reference_count):
            fragment_digest = witness_payload[offset + index * 32 : offset + (index + 1) * 32].hex()
            frame = fragments.get(fragment_digest)
            expected_length = min(
                PTG2_V3_SOURCE_WITNESS_FRAGMENT_BYTES, raw_length - index * PTG2_V3_SOURCE_WITNESS_FRAGMENT_BYTES
            )
            if frame is None or frame.raw_byte_count != expected_length:
                raise RuntimeError("source witness fragment recipe references invalid evidence")
            used_fragments.add(fragment_digest)
        offset = end
        previous = digest
    if (reconstructed_bytes, references) != (
        _metric(header, "evidence_reconstructed_bytes"),
        _metric(header, "recipe_reference_count"),
    ):
        raise RuntimeError("source witness fragment recipe metrics do not match")
    if used_fragments != set(fragments):
        raise RuntimeError("source witness fragment dictionary contains unused evidence")
    return recipe_by_sha256, offset


def _token(payload: bytes, digest: str, recipe: _Recipe, fragments: Mapping[str, _Fragment]) -> bytes:
    return decode_fragment_recipe(
        {
            "raw_sha256": digest,
            "raw_byte_count": recipe.raw_byte_count,
            "fragment_sha256": [
                payload[recipe.offset + index * 32 : recipe.offset + (index + 1) * 32].hex()
                for index in range(recipe.reference_count)
            ],
        },
        lambda fragment_digest: _fragment_bytes(payload, fragment_digest, fragments[fragment_digest]),
    )


def _parsed_token(raw: bytes) -> dict[str, Any]:
    try:
        value = json.loads(raw, parse_float=Decimal, parse_int=int)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError("source witness fragment token is invalid JSON") from exc
    if not isinstance(value, dict):
        raise RuntimeError("source witness fragment token must be a JSON object")
    return value


def _validate_tokens(payload: bytes, recipes: Mapping[str, _Recipe], fragments: Mapping[str, _Fragment]) -> None:
    for digest, recipe in recipes.items():
        raw = _token(payload, digest, recipe, fragments)
        value = _parsed_token(raw)
        del raw, value


@dataclass(frozen=True)
class _Record:
    source_sha256: str
    offset: int
    length: int
    raw_sha256: str
    linked_provider_sha256: str | None


def _records(
    witness_payload: bytes,
    offset: int,
    header: Mapping[str, Any],
    source_digests: set[str],
    recipes: Mapping[str, _Recipe],
) -> tuple[_Record, ...]:
    count, offset = _count(
        witness_payload,
        offset,
        _metric(header, "record_count"),
        PTG2_V3_SOURCE_WITNESS_TOTAL_TARGET,
        37,
        "record count",
    )
    record_frames = []
    used_recipes: set[str] = set()
    sample = hashlib.sha256()
    occurrences = providers = 0
    previous = None
    for _ in range(count):
        source_digest, offset = _digest(witness_payload, offset, "")
        if source_digest not in source_digests:
            raise RuntimeError("source witness fragment record references an unknown source")
        length, offset = read_u32(witness_payload, offset, field_name="record length")
        _bounded(length + 36, PTG2_V3_SOURCE_WITNESS_MAX_RECORD_BYTES, "stored record")
        if length == 0 or offset + length > len(witness_payload):
            raise RuntimeError("source witness fragment record framing is invalid")
        compressed = witness_payload[offset : offset + length]
        fields = decode_record_locator_fields(compressed)
        key = (fields.kind, fields.priority, fields.tie_breaker, source_digest)
        if previous is not None and key < previous:
            raise RuntimeError("source witness fragment record order is invalid")
        for digest in (fields.raw_sha256, fields.linked_provider_sha256):
            if digest is not None:
                if digest not in recipes:
                    raise RuntimeError("source witness fragment record recipe is missing")
                used_recipes.add(digest)
        occurrences += fields.kind == "rate_occurrence"
        providers += fields.kind == "provider_reference"
        record_frames.append(_Record(source_digest, offset, length, fields.raw_sha256, fields.linked_provider_sha256))
        sample.update(bytes.fromhex(source_digest))
        sample.update(compressed)
        offset += length
        previous = key
    expected = source_witness_targets(
        occurrence_population=_metric(header, "queryable_occurrence_population_count"),
        provider_population=_metric(header, "provider_population_count"),
    )
    declared = (_metric(header, "occurrence_witness_count"), _metric(header, "provider_witness_count"), count)
    if expected != declared or declared != (occurrences, providers, count):
        raise RuntimeError("source witness fragment coverage is incomplete")
    if offset != len(witness_payload) or used_recipes != set(recipes):
        raise RuntimeError("source witness fragment has trailing or unused evidence")
    if sample.hexdigest() != header.get("sample_digest"):
        raise RuntimeError("source witness fragment sample digest is inconsistent")
    return tuple(record_frames)


@dataclass(frozen=True)
class _FragmentRecords(Sequence[SourceWitnessRecord]):
    payload: bytes
    frames: tuple[_Record, ...]
    recipes: Mapping[str, _Recipe]
    fragments: Mapping[str, _Fragment]

    def __len__(self) -> int:
        return len(self.frames)

    def __getitem__(self, index: int | slice) -> SourceWitnessRecord | Sequence[SourceWitnessRecord]:
        if isinstance(index, slice):
            return _FragmentRecords(self.payload, self.frames[index], self.recipes, self.fragments)
        frame = self.frames[operator.index(index)]
        compressed = self.payload[frame.offset : frame.offset + frame.length]
        fields = decode_record_locator_fields(compressed)
        raw_evidence_by_sha256 = {
            digest: _token(self.payload, digest, self.recipes[digest], self.fragments)
            for digest in (fields.raw_sha256, fields.linked_provider_sha256)
            if digest is not None
        }
        return decode_persisted_record(compressed, frame.source_sha256, evidence_by_sha256=raw_evidence_by_sha256)


def _record_groups(
    records: _FragmentRecords, initialization_bytes: int
) -> tuple[dict[tuple[str, str | None], list[int]], dict[str, int]]:
    indexes_by_pair: dict[tuple[str, str | None], list[int]] = {}
    reference_count = 0
    for index, frame in enumerate(records.frames):
        pair = (frame.raw_sha256, frame.linked_provider_sha256)
        indexes_by_pair.setdefault(pair, []).append(index)
        reference_count += 1 + int(frame.linked_provider_sha256 is not None)
    token_count = fragment_count = byte_count = 0
    for pair in indexes_by_pair:
        for digest in dict.fromkeys(value for value in pair if value is not None):
            recipe = records.recipes[digest]
            token_count += 1
            fragment_count += recipe.reference_count
            byte_count += recipe.raw_byte_count
    _bounded(
        initialization_bytes + byte_count,
        PTG2_V3_SOURCE_WITNESS_MAX_RECONSTRUCTED_BYTES,
        "grouped reconstruction work",
    )
    return indexes_by_pair, {
        "record_count": len(records),
        "reference_count": reference_count,
        "token_count": token_count,
        "fragment_count": fragment_count,
        "byte_count": byte_count,
    }


def _map_record_group(
    records: _FragmentRecords,
    pair: tuple[str, str | None],
    indexes: Sequence[int],
    mapper: Callable[[SourceWitnessRecord, Mapping[str, Mapping[str, Any]]], _MappedResult],
    results: list[_MappedResult],
) -> None:
    raw_evidence_by_sha256: dict[str, bytes] = {}
    parsed_evidence_by_sha256: dict[str, Mapping[str, Any]] = {}
    try:
        for digest in dict.fromkeys(value for value in pair if value is not None):
            raw_evidence_by_sha256[digest] = _token(records.payload, digest, records.recipes[digest], records.fragments)
            parsed_evidence_by_sha256[digest] = _parsed_token(raw_evidence_by_sha256[digest])
        parsed_view = MappingProxyType(parsed_evidence_by_sha256)
        for index in indexes:
            frame = records.frames[index]
            compressed = records.payload[frame.offset : frame.offset + frame.length]
            record = decode_persisted_record(compressed, frame.source_sha256, evidence_by_sha256=raw_evidence_by_sha256)
            try:
                results[index] = mapper(record, parsed_view)
            finally:
                del record
    finally:
        raw_evidence_by_sha256.clear()
        parsed_evidence_by_sha256.clear()


def _processing_io(header: Mapping[str, Any], work: Mapping[str, int]) -> dict[str, int]:
    """Count actual evidence operations, including both strict fragment hashes."""

    fragment_count = _metric(header, "fragment_count")
    recipe_count = _metric(header, "evidence_dictionary_count")
    initialized_references = _metric(header, "recipe_reference_count")
    decompressions = fragment_count + initialized_references + work["fragment_count"]
    fragment_hashes = fragment_count + 2 * (initialized_references + work["fragment_count"])
    token_reconstructions = recipe_count + work["token_count"]
    reconstruction_bytes = _metric(header, "evidence_reconstructed_bytes") + work["byte_count"]
    return {
        "record_decodes": work["record_count"],
        "unique_evidence_entries": recipe_count,
        "evidence_decompressions": decompressions,
        "fragment_decompressions": decompressions,
        "fragment_sha256_hashes": fragment_hashes,
        "token_reconstructions": token_reconstructions,
        "evidence_sha256_hashes": fragment_hashes + token_reconstructions,
        "evidence_json_parses": token_reconstructions,
        "reconstruction_bytes": reconstruction_bytes,
        "decoded_evidence_bytes": _metric(header, "evidence_dictionary_raw_bytes") + reconstruction_bytes,
        "evidence_reuse_deliveries": work["reference_count"] - work["token_count"],
        "repeated_evidence_decompressions": decompressions - fragment_count,
        "repeated_evidence_sha256_hashes": fragment_hashes + token_reconstructions - fragment_count - recipe_count,
        "repeated_evidence_json_parses": work["token_count"],
    }


def decode_fragment_source_witness(
    payload: bytes,
    *,
    expected_raw_source_sha256: Sequence[str],
    expected_metadata: Mapping[str, Any] | None = None,
) -> FragmentWitnessView:
    """Authenticate the complete bounded v6 corpus, retaining only frame indexes."""

    if not isinstance(payload, (bytes, bytearray)) or not payload:
        raise RuntimeError("source witness fragment payload framing is invalid")
    _bounded(len(payload), PTG2_V3_SOURCE_WITNESS_MAX_PAYLOAD_BYTES, "payload bytes")
    immutable = bytes(payload)
    if not immutable.startswith(FRAGMENT_PERSISTED_PAYLOAD_MAGIC):
        raise RuntimeError("source witness fragment payload magic is invalid")
    sources = sorted(sha256_hex(value, field_name="expected raw source digest") for value in expected_raw_source_sha256)
    if not sources or len(sources) != len(set(sources)):
        raise RuntimeError("source witness fragment source set is invalid")
    header, offset = _header(immutable, sources)
    fragments, offset = _fragments(immutable, offset, header)
    recipes, offset = _recipes(immutable, offset, header, fragments)
    frames = _records(immutable, offset, header, set(sources), recipes)
    _validate_tokens(immutable, recipes, fragments)
    metadata = {**header, "payload_sha256": hashlib.sha256(immutable).hexdigest(), "payload_bytes": len(immutable)}
    if expected_metadata is not None and dict(expected_metadata) != metadata:
        raise RuntimeError("source witness fragment manifest fields changed")
    return FragmentWitnessView(
        MappingProxyType(metadata),
        _FragmentRecords(immutable, frames, MappingProxyType(recipes), MappingProxyType(fragments)),
    )


__all__ = ["FragmentWitnessView", "decode_fragment_source_witness"]
