# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Persist exact selected witnesses using disk-backed shared byte fragments."""

from __future__ import annotations

import hashlib
import json
import tempfile
from dataclasses import dataclass
from itertools import chain
from typing import BinaryIO, Sequence

from process.ptg_parts.ptg2_source_witness_bundle import _decompress_evidence
from process.ptg_parts.ptg2_source_witness_codec import externalize_source_evidence_record
from process.ptg_parts.ptg2_source_witness_contract import (
    FRAGMENT_PERSISTED_PAYLOAD_MAGIC,
    PTG2_V3_SOURCE_WITNESS_FRAGMENT_BYTES,
    PTG2_V3_SOURCE_WITNESS_FRAGMENT_COMPRESSION,
    PTG2_V3_SOURCE_WITNESS_FRAGMENT_PAYLOAD_CONTRACT,
    PTG2_V3_SOURCE_WITNESS_MAX_FRAGMENT_REFERENCES,
    PTG2_V3_SOURCE_WITNESS_MAX_FRAGMENTS,
    PTG2_V3_SOURCE_WITNESS_MAX_PAYLOAD_BYTES,
    PTG2_V3_SOURCE_WITNESS_MAX_RECIPES,
    PTG2_V3_SOURCE_WITNESS_MAX_RECONSTRUCTED_BYTES,
    PTG2_V3_SOURCE_WITNESS_MAX_RECORD_BYTES,
    CompressedSourceWitnessRecord,
    SourceWitnessCandidate,
    SourceWitnessRecordLocator,
)
from process.ptg_parts.ptg2_source_witness_fragment_bundle import read_fragment_locator_token
from process.ptg_parts.ptg2_source_witness_fragments import encode_fragment_recipe
from process.ptg_parts.ptg2_source_witness_locator_materialize import (
    _materialization_bundle_file,
    _read_locator_payload,
)
from process.ptg_parts.ptg2_source_witness_persisted_encode import SourceWitnessPayloadCounts
from process.ptg_parts.ptg2_source_witness_primitives import U32
from process.ptg_parts.ptg2_source_witness_streaming_encode import (
    _append_stage,
    _copy_staged_payload,
    _payload_header,
    _payload_limit_error,
    _stage_evidence,
    _stage_locator_record,
    _StagedEvidence,
    _StagedPayload,
    _StagedRecord,
    _StreamingBudget,
    _write_bounded,
)


@dataclass(frozen=True)
class _Recipe:
    digest: str
    raw_length: int
    references: _StagedPayload


class _FragmentStage:
    def __init__(self, file: BinaryIO) -> None:
        self.file = file
        self.budget = _StreamingBudget(stored_body_bytes=U32.size * 3)
        self.fragments: dict[str, _StagedEvidence] = {}
        self.recipes: dict[str, _Recipe] = {}
        self.reference_count = 0
        self.reconstructed_bytes = 0

    def fragment(self, digest: str, raw: bytes) -> None:
        """Stage a unique authenticated fragment under existing byte bounds."""
        existing = self.fragments.get(digest)
        if existing is not None:
            if existing.raw_byte_count != len(raw):
                raise RuntimeError("source witness fragment digest is inconsistent")
            return
        if len(self.fragments) >= PTG2_V3_SOURCE_WITNESS_MAX_FRAGMENTS:
            raise _payload_limit_error("fragment dictionary count exceeds its bound")
        # The existing staging helper reserves digest/length framing; v6 also
        # stores the decoded fragment length independently.
        self.budget.stored_body_bytes += U32.size
        _stage_evidence(self.file, self.fragments, self.budget, evidence_sha256=digest, raw_evidence=raw)

    def token(self, digest: str, raw: bytes) -> None:
        """Stage an exact token recipe after count and reconstruction checks."""
        if hashlib.sha256(raw).hexdigest() != digest:
            raise RuntimeError("source witness complete token digest is invalid")
        existing = self.recipes.get(digest)
        if existing is not None:
            if existing.raw_length != len(raw):
                raise RuntimeError("source witness recipe digest is inconsistent")
            return
        reference_count = (
            len(raw) + PTG2_V3_SOURCE_WITNESS_FRAGMENT_BYTES - 1
        ) // PTG2_V3_SOURCE_WITNESS_FRAGMENT_BYTES
        if (
            len(self.recipes) >= PTG2_V3_SOURCE_WITNESS_MAX_RECIPES
            or self.reference_count + reference_count > PTG2_V3_SOURCE_WITNESS_MAX_FRAGMENT_REFERENCES
            or self.reconstructed_bytes + len(raw) > PTG2_V3_SOURCE_WITNESS_MAX_RECONSTRUCTED_BYTES
        ):
            raise _payload_limit_error("recipe count or reconstruction work exceeds its bound")
        self.budget.stored_body_bytes += U32.size
        self.budget.reserve_stored_entry(reference_count * 32)
        recipe = encode_fragment_recipe(raw, self.fragment)
        references = b"".join(bytes.fromhex(value) for value in recipe["fragment_sha256"])
        self.recipes[digest] = _Recipe(digest, len(raw), _append_stage(self.file, references))
        self.reference_count += reference_count
        self.reconstructed_bytes += len(raw)


def _stage_compressed_candidate(stage: _FragmentStage, candidate: CompressedSourceWitnessRecord) -> _StagedRecord:
    """Externalize a legacy candidate without changing its authenticated bytes."""

    if candidate.evidence_by_sha256 is None:
        compressed, evidence_by_sha256 = externalize_source_evidence_record(
            candidate.compressed, candidate.raw_source_sha256
        )
    else:
        compressed, evidence_by_sha256 = candidate.compressed, candidate.evidence_by_sha256
    for digest, raw in evidence_by_sha256.items():
        stage.token(digest, raw)
    stage.budget.reserve_stored_entry(len(compressed))
    return _StagedRecord(candidate.raw_source_sha256, _append_stage(stage.file, compressed))


def _stage_bundle_candidates(stage, identity, indexed_locators, staged_by_index) -> None:
    """Authenticate each selected bundle token once before disk-backed staging."""

    with _materialization_bundle_file(identity) as bundle_file:
        validated_locators: set[tuple[str, int, int, int]] = set()
        for index, candidate in indexed_locators:
            staged_by_index[index] = _stage_locator_record(stage.file, candidate, bundle_file, stage.budget)
            for locator in candidate.evidence_by_sha256.values():
                key = (locator.sha256, locator.raw_byte_count, locator.offset, locator.length)
                if key in validated_locators:
                    continue
                if locator.fragments_by_sha256 is not None:
                    raw = read_fragment_locator_token(bundle_file, identity, locator)
                else:
                    compressed = _read_locator_payload(
                        bundle_file, identity, offset=locator.offset, length=locator.length, field_name="evidence"
                    )
                    raw = _decompress_evidence(compressed, locator.raw_byte_count)
                stage.token(locator.sha256, raw)
                validated_locators.add(key)


def _stage_records(stage: _FragmentStage, selected: Sequence[SourceWitnessCandidate]) -> tuple[_StagedRecord, ...]:
    """Keep selected ordering while grouping immutable bundle reads."""

    staged_by_index: list[_StagedRecord | None] = [None] * len(selected)
    indexed_locators_by_bundle: dict[object, list[tuple[int, SourceWitnessRecordLocator]]] = {}
    for index, candidate in enumerate(selected):
        if isinstance(candidate, SourceWitnessRecordLocator):
            indexed_locators_by_bundle.setdefault(candidate.bundle, []).append((index, candidate))
        elif isinstance(candidate, CompressedSourceWitnessRecord):
            staged_by_index[index] = _stage_compressed_candidate(stage, candidate)
        else:
            raise RuntimeError("source witness candidate is invalid")
    for identity, indexed_locators in indexed_locators_by_bundle.items():
        _stage_bundle_candidates(stage, identity, indexed_locators, staged_by_index)
    if any(staged is None for staged in staged_by_index):
        raise RuntimeError("source witness fragment staging is incomplete")
    return tuple(staged for staged in staged_by_index if staged is not None)


@dataclass
class _FragmentOutput:
    output_file: BinaryIO
    stage_file: BinaryIO
    written: int = 0

    def parts(self, *chunks: bytes) -> None:
        """Reserve and write each exact frame under the stored payload bound."""

        for chunk in chunks:
            self.written = _write_bounded(self.output_file, chunk, self.written)

    def copy(self, staged: _StagedPayload) -> None:
        """Copy one authenticated staged frame without retaining its bytes."""

        self.written = _copy_staged_payload(self.stage_file, self.output_file, staged, self.written)


def _validate_entry_frames(stage: _FragmentStage, records: Sequence[_StagedRecord], header_length: int) -> None:
    """Apply the reader's per-entry bound to complete frames, not only bodies."""

    framed_sizes = chain(
        (header_length + U32.size,),
        (fragment.payload.length + 40 for fragment in stage.fragments.values()),
        (recipe.references.length + 40 for recipe in stage.recipes.values()),
        (record.payload.length + 36 for record in records),
    )
    if any(size > PTG2_V3_SOURCE_WITNESS_MAX_RECORD_BYTES for size in framed_sizes):
        raise _payload_limit_error("framed entry exceeds its stored safety bound")


def _write_payload(stage: _FragmentStage, staged_records: Sequence[_StagedRecord], header: dict[str, object]) -> bytes:
    """Write exact fragment, recipe and record frames with verified accounting."""
    header_bytes = json.dumps(header, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("ascii")
    projected = len(FRAGMENT_PERSISTED_PAYLOAD_MAGIC) + U32.size + len(header_bytes) + stage.budget.stored_body_bytes
    if projected > PTG2_V3_SOURCE_WITNESS_MAX_PAYLOAD_BYTES:
        raise _payload_limit_error("fragment payload exceeds its logical byte bound")
    _validate_entry_frames(stage, staged_records, len(header_bytes))
    with tempfile.TemporaryFile(mode="w+b") as output_file:
        output = _FragmentOutput(output_file, stage.file)
        output.parts(
            FRAGMENT_PERSISTED_PAYLOAD_MAGIC, U32.pack(len(header_bytes)), header_bytes, U32.pack(len(stage.fragments))
        )
        for digest in sorted(stage.fragments):
            fragment = stage.fragments[digest]
            output.parts(bytes.fromhex(digest), U32.pack(fragment.raw_byte_count), U32.pack(fragment.payload.length))
            output.copy(fragment.payload)
        output.parts(U32.pack(len(stage.recipes)))
        for digest in sorted(stage.recipes):
            recipe = stage.recipes[digest]
            output.parts(bytes.fromhex(digest), U32.pack(recipe.raw_length), U32.pack(recipe.references.length // 32))
            output.copy(recipe.references)
        output.parts(U32.pack(len(staged_records)))
        for staged in staged_records:
            output.parts(bytes.fromhex(staged.raw_source_sha256), U32.pack(staged.payload.length))
            output.copy(staged.payload)
        if output.written != projected:
            raise RuntimeError("source witness fragment staging byte count changed")
        output_file.seek(0)
        witness_payload = output_file.read(projected + 1)
    if len(witness_payload) != projected:
        raise RuntimeError("source witness fragment staging file is truncated")
    return witness_payload


def encode_fragment_source_witness_candidates(
    selected_records: Sequence[SourceWitnessCandidate],
    counts: SourceWitnessPayloadCounts,
) -> tuple[bytes, dict[str, object]]:
    """Keep exact sample ordering/hashes while sharing lossless token fragments."""

    with tempfile.TemporaryFile(mode="w+b") as stage_file:
        stage = _FragmentStage(stage_file)
        staged_records = _stage_records(stage, selected_records)
        header_by_field = {
            **_payload_header(counts, staged_records, tuple(stage.fragments.values()), stage_file),
            "contract": PTG2_V3_SOURCE_WITNESS_FRAGMENT_PAYLOAD_CONTRACT,
            "format_version": 6,
            "compression": PTG2_V3_SOURCE_WITNESS_FRAGMENT_COMPRESSION,
            "evidence_dictionary_count": len(stage.recipes),
            "fragment_count": len(stage.fragments),
            "fragment_byte_count": PTG2_V3_SOURCE_WITNESS_FRAGMENT_BYTES,
            "evidence_reconstructed_bytes": stage.reconstructed_bytes,
            "recipe_reference_count": stage.reference_count,
        }
        witness_payload = _write_payload(stage, staged_records, header_by_field)
    return witness_payload, {
        **header_by_field,
        "payload_sha256": hashlib.sha256(witness_payload).hexdigest(),
        "payload_bytes": len(witness_payload),
        "compression": PTG2_V3_SOURCE_WITNESS_FRAGMENT_COMPRESSION,
    }
