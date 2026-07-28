# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Independent bounded readback verifier for a committed UHC semantic stage."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
import re
from typing import Any, Mapping
import zlib

import asyncpg

from process.uhc_provider_file_source_identity import UHC_PROVIDER_FILE_SOURCE_ID
from process.uhc_semantic_build_store import (
    UHC_SEMANTIC_CONTRACT_ID,
    UhcSemanticBuildClaim,
    UhcSemanticBuildError,
    UhcSemanticBuildIdentity,
)


_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_VERIFY_CHUNK_BYTES = 1024 * 1024
def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(_VERIFY_CHUNK_BYTES):
            digest.update(chunk)
    return digest.hexdigest()


def _json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode()


def _fact_identity(
    source_file_id: str,
    fact_type: str,
    occurrence_ordinal: int,
    payload_hash: str,
) -> bytes:
    digest = hashlib.sha256()
    for part in (
        UHC_SEMANTIC_CONTRACT_ID,
        source_file_id,
        fact_type,
        str(occurrence_ordinal),
    ):
        encoded = part.encode()
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
    fact_id = f"uhcf-{digest.hexdigest()[:48]}"
    return _json_bytes([fact_type, fact_id, payload_hash])


def _evidence_identity(row: Mapping[str, Any]) -> bytes:
    occurrence_ordinal = row["occurrence_ordinal"]
    npi = row["npi"]
    signature_pack = row["conflict_signature_pack"]
    if (
        isinstance(occurrence_ordinal, bool)
        or not isinstance(occurrence_ordinal, int)
        or occurrence_ordinal < 0
        or not isinstance(npi, str)
        or not isinstance(signature_pack, bytes)
        or len(signature_pack) != 9 * 32
    ):
        raise UhcSemanticBuildError("UHC semantic evidence identity is invalid")
    return _json_bytes([occurrence_ordinal, npi, signature_pack.hex()])


def _update_line_digest(
    digest: Any,
    count: int,
    payload: bytes,
) -> int:
    if count:
        digest.update(b"\n")
    digest.update(payload)
    return count + 1


class _FactBlockVerifier:
    def __init__(
        self,
        *,
        source_file_id: str,
        fact_type: str,
        record_start: int,
        expected_record_count: int,
        max_record_bytes: int,
        global_identity_digest: Any,
        global_identity_count: int,
    ) -> None:
        self.source_file_id = source_file_id
        self.fact_type = fact_type
        self.next_ordinal = record_start
        self.expected_record_count = expected_record_count
        self.max_record_bytes = max_record_bytes
        self.global_identity_digest = global_identity_digest
        self.global_identity_count = global_identity_count
        self.block_identity_digest = hashlib.sha256()
        self.block_identity_count = 0
        self.line_buffer = bytearray()

    def consume(self, decompressed_chunk: bytes) -> None:
        """Consume one bounded compressed-output chunk into exact facts."""

        self.line_buffer.extend(decompressed_chunk)
        while True:
            newline = self.line_buffer.find(b"\n")
            if newline < 0:
                break
            line = bytes(self.line_buffer[:newline])
            del self.line_buffer[: newline + 1]
            if not line or b"\r" in line:
                raise UhcSemanticBuildError(
                    "UHC semantic fact block framing is invalid"
                )
            try:
                decoded = json.loads(line)
            except (UnicodeDecodeError, json.JSONDecodeError) as error:
                raise UhcSemanticBuildError(
                    "UHC semantic fact block contains invalid JSON"
                ) from error
            if not isinstance(decoded, dict):
                raise UhcSemanticBuildError(
                    "UHC semantic fact payload is not an object"
                )
            identity = _fact_identity(
                self.source_file_id,
                self.fact_type,
                self.next_ordinal,
                hashlib.sha256(line).hexdigest(),
            )
            self.block_identity_count = _update_line_digest(
                self.block_identity_digest,
                self.block_identity_count,
                identity,
            )
            self.global_identity_count = _update_line_digest(
                self.global_identity_digest,
                self.global_identity_count,
                identity,
            )
            self.next_ordinal += 1
        if len(self.line_buffer) > self.max_record_bytes:
            raise UhcSemanticBuildError(
                "UHC semantic fact exceeds verifier memory bound"
            )

    def finish(self, record_start: int) -> str:
        """Seal the block proof after validating framing and record count."""

        if self.line_buffer:
            raise UhcSemanticBuildError(
                "UHC semantic fact block lacks final newline"
            )
        observed = self.next_ordinal - record_start
        if (
            observed != self.expected_record_count
            or self.block_identity_count != self.expected_record_count
        ):
            raise UhcSemanticBuildError(
                "UHC semantic fact block record count mismatch"
            )
        return self.block_identity_digest.hexdigest()


@dataclass(frozen=True)
class _FactBlockMetadata:
    range_ordinal: int
    record_start: int
    record_count: int
    compressed_bytes: int
    payload_hash: str
    semantic_hash: str


def _fact_block_metadata(metadata: Mapping[str, Any]) -> _FactBlockMetadata:
    block = _FactBlockMetadata(
        range_ordinal=int(metadata["range_ordinal"]),
        record_start=int(metadata["record_start"]),
        record_count=int(metadata["record_count"]),
        compressed_bytes=int(metadata["compressed_bytes"]),
        payload_hash=str(metadata["compressed_payload_sha256"]),
        semantic_hash=str(metadata["semantic_block_sha256"]),
    )
    if (
        min(block.range_ordinal, block.record_start) < 0
        or block.record_count <= 0
        or block.compressed_bytes <= 0
        or _SHA256_RE.fullmatch(block.payload_hash) is None
        or _SHA256_RE.fullmatch(block.semantic_hash) is None
    ):
        raise UhcSemanticBuildError("UHC semantic fact metadata is invalid")
    return block


def _consume_compressed_chunk(
    decompressor: zlib.Decompress,
    verifier: _FactBlockVerifier,
    compressed_chunk: bytes,
) -> None:
    pending = compressed_chunk
    while pending:
        before = len(pending)
        decoded = decompressor.decompress(
            pending,
            max_length=_VERIFY_CHUNK_BYTES,
        )
        verifier.consume(decoded)
        pending = decompressor.unconsumed_tail
        if not decoded and len(pending) == before:
            raise UhcSemanticBuildError(
                "UHC semantic compressed fact block made no progress"
            )


async def _read_compressed_fact_block(
    connection: asyncpg.Connection,
    stage_ref: str,
    block: _FactBlockMetadata,
    verifier: _FactBlockVerifier,
) -> str:
    decompressor = zlib.decompressobj()
    compressed_digest = hashlib.sha256()
    offset = 1
    consumed = 0
    while consumed < block.compressed_bytes:
        requested = min(_VERIFY_CHUNK_BYTES, block.compressed_bytes - consumed)
        compressed_chunk = await connection.fetchval(
            f"""
            SELECT substring(payload_bytes FROM $2::integer FOR $3::integer)
              FROM {stage_ref}
             WHERE row_kind=1 AND range_ordinal=$1
            """,
            block.range_ordinal,
            offset,
            requested,
        )
        if not isinstance(compressed_chunk, bytes) or len(compressed_chunk) != requested:
            raise UhcSemanticBuildError(
                "UHC semantic fact block ended during bounded readback"
            )
        compressed_digest.update(compressed_chunk)
        _consume_compressed_chunk(decompressor, verifier, compressed_chunk)
        consumed += len(compressed_chunk)
        offset += len(compressed_chunk)
    while decoded := decompressor.decompress(b"", max_length=_VERIFY_CHUNK_BYTES):
        verifier.consume(decoded)
    verifier.consume(decompressor.flush())
    if not decompressor.eof or decompressor.unused_data:
        raise UhcSemanticBuildError(
            "UHC semantic compressed fact block is incomplete or concatenated"
        )
    return compressed_digest.hexdigest()


async def _verify_fact_block(
    connection: asyncpg.Connection,
    stage_ref: str,
    metadata: Mapping[str, Any],
    *,
    source_file_id: str,
    fact_type: str,
    max_record_bytes: int,
    global_identity_digest: Any,
    global_identity_count: int,
) -> tuple[dict[str, Any], int]:
    """Independently stream and verify one committed fact block."""

    block = _fact_block_metadata(metadata)
    verifier = _FactBlockVerifier(
        source_file_id=source_file_id,
        fact_type=fact_type,
        record_start=block.record_start,
        expected_record_count=block.record_count,
        max_record_bytes=max_record_bytes,
        global_identity_digest=global_identity_digest,
        global_identity_count=global_identity_count,
    )
    compressed_digest = await _read_compressed_fact_block(
        connection,
        stage_ref,
        block,
        verifier,
    )
    if compressed_digest != block.payload_hash:
        raise UhcSemanticBuildError(
            "UHC semantic compressed fact payload hash mismatch"
        )
    semantic_hash = verifier.finish(block.record_start)
    if semantic_hash != block.semantic_hash:
        raise UhcSemanticBuildError(
            "UHC semantic fact block semantic hash mismatch"
        )
    return (
        {
            "range_ordinal": block.range_ordinal,
            "record_start": block.record_start,
            "record_count": block.record_count,
            "fact_count": block.record_count,
            "compressed_bytes": block.compressed_bytes,
            "compressed_payload_sha256": block.payload_hash,
            "semantic_block_sha256": semantic_hash,
        },
        verifier.global_identity_count,
    )


def _fact_set_sha256(blocks: list[dict[str, Any]]) -> str:
    digest = hashlib.sha256()
    for index, block in enumerate(blocks):
        if index:
            digest.update(b"\n")
        digest.update(
            _json_bytes(
                [
                    UHC_SEMANTIC_CONTRACT_ID,
                    block["range_ordinal"],
                    block["record_start"],
                    block["record_count"],
                    block["fact_count"],
                    block["compressed_payload_sha256"],
                    block["semantic_block_sha256"],
                ]
            )
        )
    return digest.hexdigest()


@dataclass
class _EvidenceRangeState:
    digest: Any = field(default_factory=hashlib.sha256)
    evidence_count: int = 0
    run_count: int = 0


@dataclass
class _EvidenceRunAccumulator:
    range_count: int
    range_states: list[_EvidenceRangeState]
    current_key: tuple[int, int] | None = None
    current_digest: Any = field(default_factory=hashlib.sha256)
    current_count: int = 0

    def switch_to(self, key: tuple[int, int]) -> None:
        """Finish the prior run and select the next observed run."""

        if self.current_key != key:
            self.finish()
            self.current_key = key

    def add_identity(self, identity: bytes) -> None:
        """Add one ordered evidence identity to the selected run."""

        self.current_count = _update_line_digest(
            self.current_digest,
            self.current_count,
            identity,
        )

    def finish(self) -> None:
        """Validate and commit the selected run into its range proof."""

        if self.current_key is None:
            return
        range_ordinal, run_ordinal = self.current_key
        if not 0 <= range_ordinal < self.range_count:
            raise UhcSemanticBuildError(
                "UHC semantic evidence range ordinal is invalid"
            )
        range_state = self.range_states[range_ordinal]
        if run_ordinal != range_state.run_count or self.current_count <= 0:
            raise UhcSemanticBuildError(
                "UHC semantic evidence runs are not contiguous"
            )
        layout = _json_bytes(
            [
                range_ordinal,
                run_ordinal,
                self.current_count,
                self.current_digest.hexdigest(),
            ]
        )
        if range_state.run_count:
            range_state.digest.update(b"\n")
        range_state.digest.update(layout)
        range_state.run_count += 1
        range_state.evidence_count += self.current_count
        self.current_key = None
        self.current_digest = hashlib.sha256()
        self.current_count = 0


async def _verify_evidence_identities(
    connection: asyncpg.Connection,
    stage_ref: str,
) -> tuple[int, str]:
    """Verify contiguous global evidence identities in occurrence order."""

    identity_digest = hashlib.sha256()
    identity_count = 0
    expected_occurrence = 0
    query = f"""
        SELECT occurrence_ordinal, npi, conflict_signature_pack
          FROM {stage_ref}
         WHERE row_kind=2
         ORDER BY occurrence_ordinal
    """
    async for evidence_row in connection.cursor(query, prefetch=128):
        if int(evidence_row["occurrence_ordinal"]) != expected_occurrence:
            raise UhcSemanticBuildError(
                "UHC semantic evidence occurrence ordinals are not contiguous"
            )
        identity_count = _update_line_digest(
            identity_digest,
            identity_count,
            _evidence_identity(evidence_row),
        )
        expected_occurrence += 1
    return identity_count, identity_digest.hexdigest()


async def _verify_evidence_ranges(
    connection: asyncpg.Connection,
    stage_ref: str,
    *,
    range_count: int,
) -> list[dict[str, Any]]:
    """Verify ordered evidence runs and return each range proof."""

    range_states = [_EvidenceRangeState() for _ in range(range_count)]
    accumulator = _EvidenceRunAccumulator(range_count, range_states)
    run_query = f"""
        SELECT range_ordinal, run_ordinal, occurrence_ordinal,
               npi, conflict_signature_pack
          FROM {stage_ref}
         WHERE row_kind=2
         ORDER BY range_ordinal, run_ordinal, npi, occurrence_ordinal
    """
    async for evidence_row in connection.cursor(run_query, prefetch=128):
        key = (
            int(evidence_row["range_ordinal"]),
            int(evidence_row["run_ordinal"]),
        )
        accumulator.switch_to(key)
        accumulator.add_identity(_evidence_identity(evidence_row))
    accumulator.finish()
    return [
        {
            "range_ordinal": range_ordinal,
            "evidence_count": range_state.evidence_count,
            "run_count": range_state.run_count,
            "layout_sha256": range_state.digest.hexdigest(),
        }
        for range_ordinal, range_state in enumerate(range_states)
    ]


def _evidence_layout_sha256(evidence_ranges: list[dict[str, Any]]) -> str:
    """Hash ordered evidence range proofs with unambiguous framing."""

    layout_digest = hashlib.sha256()
    for index, proof in enumerate(evidence_ranges):
        if index:
            layout_digest.update(b"\n")
        layout_digest.update(
            _json_bytes(
                [
                    proof["range_ordinal"],
                    proof["evidence_count"],
                    proof["run_count"],
                    proof["layout_sha256"],
                ]
            )
        )
    return layout_digest.hexdigest()


async def _verify_evidence(
    connection: asyncpg.Connection,
    stage_ref: str,
    *,
    range_count: int,
) -> tuple[int, str, str, list[dict[str, Any]]]:
    """Verify global evidence identity plus deterministic range/run layout."""

    identity_count, identity_sha256 = await _verify_evidence_identities(
        connection,
        stage_ref,
    )
    evidence_ranges = await _verify_evidence_ranges(
        connection,
        stage_ref,
        range_count=range_count,
    )
    return (
        identity_count,
        identity_sha256,
        _evidence_layout_sha256(evidence_ranges),
        evidence_ranges,
    )


def _verifier_inputs(
    identity: UhcSemanticBuildIdentity,
    native_report_by_field: Mapping[str, Any],
) -> tuple[int, list[Mapping[str, Any]], str]:
    max_record_bytes = native_report_by_field.get("max_record_bytes")
    if (
        isinstance(max_record_bytes, bool)
        or not isinstance(max_record_bytes, int)
        or not 1 <= max_record_bytes <= 64 * 1024 * 1024
    ):
        raise UhcSemanticBuildError(
            "UHC semantic native verifier record bound is invalid"
        )
    native_blocks = native_report_by_field.get("fact_blocks")
    if (
        not isinstance(native_blocks, list)
        or len(native_blocks) != identity.raw_range_count
        or not all(isinstance(block, Mapping) for block in native_blocks)
    ):
        raise UhcSemanticBuildError("UHC semantic native fact blocks are invalid")
    fact_type = (
        "ProviderMembershipRecord"
        if identity.collection_kind == "provider_membership"
        else "PlanReferenceRecord"
    )
    return max_record_bytes, native_blocks, fact_type


async def _verify_all_fact_blocks(
    connection: asyncpg.Connection,
    claim: UhcSemanticBuildClaim,
    identity: UhcSemanticBuildIdentity,
    native_blocks: list[Mapping[str, Any]],
    fact_type: str,
    max_record_bytes: int,
) -> tuple[list[dict[str, Any]], Any]:
    fact_identity_digest = hashlib.sha256()
    fact_identity_count = 0
    verified_blocks = []
    for expected_ordinal, metadata in enumerate(native_blocks):
        if metadata.get("range_ordinal") != expected_ordinal:
            raise UhcSemanticBuildError(
                "UHC semantic native fact blocks are unordered"
            )
        verified, fact_identity_count = await _verify_fact_block(
            connection,
            claim.stage_ref,
            metadata,
            source_file_id=identity.source_file_id,
            fact_type=fact_type,
            max_record_bytes=max_record_bytes,
            global_identity_digest=fact_identity_digest,
            global_identity_count=fact_identity_count,
        )
        verified_blocks.append(verified)
    return verified_blocks, fact_identity_digest


def _assert_verifier_agreement(
    verifier_report_by_field: dict[str, Any],
    native_report_by_field: Mapping[str, Any],
    verified_blocks: list[dict[str, Any]],
    evidence_ranges: list[dict[str, Any]],
) -> None:
    for field_name, verified_field in verifier_report_by_field.items():
        if field_name != "verifier_sha256" and verified_field != native_report_by_field.get(field_name):
            raise UhcSemanticBuildError(
                f"independent UHC semantic verifier disagrees on {field_name}"
            )
    if verified_blocks != native_report_by_field.get("fact_blocks"):
        raise UhcSemanticBuildError(
            "independent UHC semantic fact block proofs disagree"
        )
    if evidence_ranges != native_report_by_field.get("evidence_ranges"):
        raise UhcSemanticBuildError(
            "independent UHC semantic evidence range proofs disagree"
        )


async def _verified_evidence_fields(
    connection: asyncpg.Connection,
    claim: UhcSemanticBuildClaim,
    identity: UhcSemanticBuildIdentity,
    native_report_by_field: Mapping[str, Any],
    copy_observation: Mapping[str, Any] | None,
) -> tuple[int, str, str, list[dict[str, Any]]]:
    """Verify evidence from rows or accept an exactly bound COPY proof."""

    if copy_observation is None:
        return await _verify_evidence(
            connection,
            claim.stage_ref,
            range_count=identity.raw_range_count,
        )
    _assert_copy_observation(
        native_report_by_field,
        copy_observation,
        range_count=identity.raw_range_count,
    )
    return (
        int(native_report_by_field["evidence_count"]),
        str(native_report_by_field["evidence_identity_set_sha256"]),
        str(native_report_by_field["evidence_layout_set_sha256"]),
        list(native_report_by_field["evidence_ranges"]),
    )


async def verify_uhc_semantic_stage(
    connection: asyncpg.Connection,
    claim: UhcSemanticBuildClaim,
    identity: UhcSemanticBuildIdentity,
    native_report_by_field: Mapping[str, Any],
    *,
    copy_observation: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Recompute semantic proofs from committed rows with bounded buffers."""

    if claim.sealed_reuse:
        raise UhcSemanticBuildError("sealed UHC semantic build needs no verifier")
    max_record_bytes, native_blocks, fact_type = _verifier_inputs(
        identity,
        native_report_by_field,
    )
    async with connection.transaction():
        verified_blocks, fact_identity_digest = await _verify_all_fact_blocks(
            connection, claim, identity, native_blocks,
            fact_type, max_record_bytes,
        )
        (
            evidence_count,
            evidence_identity_set_sha256,
            evidence_layout_set_sha256,
            evidence_ranges,
        ) = await _verified_evidence_fields(
            connection,
            claim,
            identity,
            native_report_by_field,
            copy_observation,
        )

    fact_count = sum(block["fact_count"] for block in verified_blocks)
    verifier_report_by_field = {
        "fact_count": fact_count,
        "evidence_count": evidence_count,
        "fact_set_sha256": _fact_set_sha256(verified_blocks),
        "record_identity_set_sha256": fact_identity_digest.hexdigest(),
        "evidence_identity_set_sha256": evidence_identity_set_sha256,
        "evidence_layout_set_sha256": evidence_layout_set_sha256,
        **(
            dict(copy_observation)
            if copy_observation is not None
            else {}
        ),
        "verifier_sha256": _sha256_file(Path(__file__).resolve()),
    }
    _assert_verifier_agreement(
        verifier_report_by_field,
        native_report_by_field,
        verified_blocks,
        evidence_ranges,
    )
    return verifier_report_by_field


def _assert_copy_observation(
    native_report_by_field: Mapping[str, Any],
    copy_observation: Mapping[str, Any],
    *,
    range_count: int,
) -> None:
    expected_row_count = int(native_report_by_field.get("evidence_count") or 0) + (
        range_count
    )
    for field_name in ("output_bytes", "copy_row_count"):
        observed = copy_observation.get(field_name)
        native = native_report_by_field.get(field_name)
        if (
            isinstance(observed, bool)
            or not isinstance(observed, int)
            or observed <= 0
            or observed != native
        ):
            raise UhcSemanticBuildError(
                f"UHC semantic COPY {field_name} proof changed"
            )
    output_sha256 = copy_observation.get("output_sha256")
    if (
        not isinstance(output_sha256, str)
        or len(output_sha256) != 64
        or any(character not in "0123456789abcdef" for character in output_sha256)
        or output_sha256 != native_report_by_field.get("output_sha256")
        or copy_observation["copy_row_count"] != expected_row_count
    ):
        raise UhcSemanticBuildError("UHC semantic COPY stream proof changed")


def _assert_sealed_identity(
    identity: UhcSemanticBuildIdentity,
    build_row: Mapping[str, Any],
    max_record_bytes: int,
) -> None:
    if (
        build_row.get("status") != "sealed"
        or build_row.get("semantic_build_id") != identity.semantic_build_id
        or build_row.get("semantic_contract_id") != UHC_SEMANTIC_CONTRACT_ID
        or build_row.get("semantic_contract_version") != 2
        or build_row.get("encoder_sha256") != identity.encoder_sha256
        or isinstance(max_record_bytes, bool)
        or not 1 <= max_record_bytes <= 64 * 1024 * 1024
    ):
        raise UhcSemanticBuildError(
            "sealed UHC semantic build identity is invalid"
        )


def _decoded_sealed_field(
    build_row: Mapping[str, Any],
    field_name: str,
) -> Any:
    decoded_field = build_row.get(field_name + "_json")
    if isinstance(decoded_field, str):
        try:
            decoded_field = json.loads(decoded_field)
        except ValueError as error:
            raise UhcSemanticBuildError(
                f"sealed UHC semantic {field_name} is invalid"
            ) from error
    return decoded_field


def _sealed_native_report(
    identity: UhcSemanticBuildIdentity,
    build_row: Mapping[str, Any],
    max_record_bytes: int,
) -> dict[str, Any]:
    counters = _decoded_sealed_field(build_row, "counters")
    copy_proof = (
        counters.get("copy_proof")
        if isinstance(counters, Mapping)
        else None
    )
    if not isinstance(copy_proof, Mapping):
        raise UhcSemanticBuildError(
            "sealed UHC semantic COPY proof is missing"
        )
    return {
        "contract_id": UHC_SEMANTIC_CONTRACT_ID,
        "contract_version": 2,
        "copy_format_id": build_row.get("copy_format_id"),
        "source_id": UHC_PROVIDER_FILE_SOURCE_ID,
        "encoder_sha256": identity.encoder_sha256,
        "lineage": {
            "artifact_sha256": identity.artifact_sha256,
            "source_file_id": identity.source_file_id,
            "collection_kind": identity.collection_kind,
        },
        "counters": _decoded_sealed_field(build_row, "counters"),
        "fact_count": build_row.get("fact_count"),
        "evidence_count": build_row.get("evidence_count"),
        "fact_set_sha256": build_row.get("fact_set_sha256"),
        "record_identity_set_sha256": build_row.get(
            "record_identity_set_sha256"
        ),
        "evidence_identity_set_sha256": build_row.get(
            "evidence_identity_set_sha256"
        ),
        "evidence_layout_set_sha256": build_row.get(
            "evidence_layout_set_sha256"
        ),
        "fact_blocks": _decoded_sealed_field(build_row, "fact_blocks"),
        "evidence_ranges": _decoded_sealed_field(build_row, "evidence_ranges"),
        **dict(copy_proof),
        "max_record_bytes": max_record_bytes,
    }


def _sealed_readback_claim(
    identity: UhcSemanticBuildIdentity,
    build_row: Mapping[str, Any],
) -> UhcSemanticBuildClaim:
    return UhcSemanticBuildClaim(
        semantic_build_id=identity.semantic_build_id,
        lease_token="independent-sealed-readback",
        attempt_count=int(build_row.get("attempt_count") or 0),
        stage_schema=str(build_row.get("stage_schema") or ""),
        stage_relation=str(build_row.get("stage_relation") or ""),
        sealed_reuse=False,
    )


async def verify_sealed_uhc_semantic_build(
    connection: asyncpg.Connection,
    identity: UhcSemanticBuildIdentity,
    build_row: Mapping[str, Any],
    *,
    max_record_bytes: int = 64 * 1024 * 1024,
) -> dict[str, Any]:
    """Independently reread a SEALED stage before downstream publication."""

    _assert_sealed_identity(identity, build_row, max_record_bytes)
    native_report = _sealed_native_report(identity, build_row, max_record_bytes)
    return await verify_uhc_semantic_stage(
        connection,
        _sealed_readback_claim(identity, build_row),
        identity,
        native_report,
        copy_observation={
            field_name: native_report[field_name]
            for field_name in (
                "output_bytes",
                "output_sha256",
                "copy_row_count",
            )
        },
    )
