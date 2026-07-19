# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Strict successful-response and once-only I/O contracts for PTG audits."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Mapping

if TYPE_CHECKING:
    from process.ptg_parts.ptg2_candidate_audit_batch_contract import (
        AuditBatchRequest,
    )


_RESPONSE_FIELDS = frozenset(
    {
        "contract",
        "request_digest",
        "challenge_count",
        "unique_challenge_count",
        "matched_challenge_count",
        "persisted_audit_occurrence_count",
        "validated_persisted_audit_occurrence_count",
        "matched_challenge_digest",
        "duration_ms",
        "block_io",
        "witness_io",
        "candidate_processing_io",
    }
)
_BLOCK_IO_FIELDS = frozenset(
    {
        "logical_block_deliveries",
        "physical_mapping_references",
        "physical_mapping_aliases",
        "unique_physical_blocks",
        "physical_block_reads",
        "physical_block_decodes",
        "physical_payload_preparations",
        "expected_logical_payload_processes",
        "logical_payload_processes",
        "logical_payload_fragment_references",
        "logical_payload_fragment_aliases",
        "repeated_physical_reads",
        "repeated_physical_decodes",
        "repeated_physical_preparations",
        "repeated_logical_payload_processes",
        "peak_raw_bytes",
    }
)
_WITNESS_IO_FIELDS = frozenset(
    {
        "payload_reads",
        "payload_decodes",
        "record_decodes",
        "unique_evidence_entries",
        "evidence_decompressions",
        "evidence_sha256_hashes",
        "evidence_json_parses",
        "evidence_reuse_deliveries",
        "repeated_evidence_decompressions",
        "repeated_evidence_sha256_hashes",
        "repeated_evidence_json_parses",
    }
)
_CANDIDATE_PROCESSING_IO_FIELDS = frozenset(
    {
        "candidate_occurrence_deliveries",
        "unique_candidate_projections",
        "candidate_projection_builds",
        "candidate_projection_reuse_deliveries",
        "repeated_candidate_projection_builds",
        "availability_condition_count",
        "duplicate_availability_deliveries",
    }
)


@dataclass(frozen=True)
class AuditBatchResponse:
    """Strict successful response and truthful request-local I/O ledger."""

    request_digest: str
    challenge_count: int
    unique_challenge_count: int
    matched_challenge_count: int
    persisted_audit_occurrence_count: int
    validated_persisted_audit_occurrence_count: int
    matched_challenge_digest: str
    duration_ms: float
    block_io: Mapping[str, int]
    witness_io: Mapping[str, int]
    candidate_processing_io: Mapping[str, int]


@dataclass(frozen=True)
class _ResponseCounts:
    challenge_count: int
    unique_challenge_count: int
    matched_challenge_count: int
    persisted_count: int
    validated_persisted_count: int


def _lower_hex_digest(raw_digest: Any, *, field_name: str) -> str:
    normalized_digest = str(raw_digest or "")
    if len(normalized_digest) != 64 or any(
        character not in "0123456789abcdef" for character in normalized_digest
    ):
        raise ValueError(f"{field_name}_invalid")
    return normalized_digest


def _positive_int(raw_number: Any, *, field_name: str) -> int:
    if type(raw_number) is not int or raw_number < 1:
        raise ValueError(f"{field_name}_invalid")
    return raw_number


def _nonnegative_int(raw_number: Any, *, field_name: str) -> int:
    if type(raw_number) is not int or raw_number < 0:
        raise ValueError(f"{field_name}_invalid")
    return raw_number


def _counter_ledger_by_name(
    raw_ledger: Any,
    *,
    field_name: str,
    expected_fields: frozenset[str],
) -> dict[str, int]:
    if not isinstance(raw_ledger, Mapping) or set(raw_ledger) != expected_fields:
        raise ValueError(f"{field_name}_fields_invalid")
    return {
        counter_name: _nonnegative_int(
            raw_ledger[counter_name],
            field_name=f"{field_name}_{counter_name}",
        )
        for counter_name in sorted(expected_fields)
    }


def _validated_duration_ms(raw_duration: Any) -> float:
    if (
        isinstance(raw_duration, bool)
        or not isinstance(raw_duration, (int, float))
        or not math.isfinite(float(raw_duration))
        or float(raw_duration) < 0
    ):
        raise ValueError("audit_batch_duration_invalid")
    return float(raw_duration)


def _validate_block_io_once(block_io_by_name: Mapping[str, int]) -> None:
    unique_physical_blocks = block_io_by_name["unique_physical_blocks"]
    if (
        unique_physical_blocks < 1
        or block_io_by_name["logical_block_deliveries"] < 1
        or block_io_by_name["expected_logical_payload_processes"] < 1
        or block_io_by_name["peak_raw_bytes"] < 1
        or block_io_by_name["physical_mapping_references"]
        != block_io_by_name["logical_block_deliveries"]
        or block_io_by_name["physical_mapping_aliases"]
        != block_io_by_name["physical_mapping_references"]
        - unique_physical_blocks
        or block_io_by_name["physical_block_reads"] != unique_physical_blocks
        or block_io_by_name["physical_block_decodes"] != unique_physical_blocks
        or block_io_by_name["physical_payload_preparations"]
        != unique_physical_blocks
        or block_io_by_name["logical_payload_processes"]
        != block_io_by_name["expected_logical_payload_processes"]
        or block_io_by_name["logical_payload_fragment_references"]
        < block_io_by_name["expected_logical_payload_processes"]
        or block_io_by_name["logical_payload_fragment_aliases"]
        != block_io_by_name["logical_payload_fragment_references"]
        - unique_physical_blocks
        or block_io_by_name["repeated_physical_reads"] != 0
        or block_io_by_name["repeated_physical_decodes"] != 0
        or block_io_by_name["repeated_physical_preparations"] != 0
        or block_io_by_name["repeated_logical_payload_processes"] != 0
    ):
        raise ValueError("audit_batch_block_io_repeated")


def _validate_witness_io_once(witness_io_by_name: Mapping[str, int]) -> None:
    unique_evidence_entries = witness_io_by_name["unique_evidence_entries"]
    if (
        witness_io_by_name["record_decodes"] < 1
        or unique_evidence_entries < 1
        or witness_io_by_name["payload_reads"] != 1
        or witness_io_by_name["payload_decodes"] != 1
        or witness_io_by_name["evidence_decompressions"]
        != unique_evidence_entries
        or witness_io_by_name["evidence_sha256_hashes"]
        != unique_evidence_entries
        or witness_io_by_name["evidence_json_parses"] != unique_evidence_entries
        or witness_io_by_name["repeated_evidence_decompressions"] != 0
        or witness_io_by_name["repeated_evidence_sha256_hashes"] != 0
        or witness_io_by_name["repeated_evidence_json_parses"] != 0
    ):
        raise ValueError("audit_batch_witness_io_repeated")


def _validate_candidate_processing_once(
    candidate_io_by_name: Mapping[str, int],
) -> None:
    occurrence_deliveries = candidate_io_by_name[
        "candidate_occurrence_deliveries"
    ]
    availability_count = candidate_io_by_name["availability_condition_count"]
    if (
        occurrence_deliveries < 1
        or candidate_io_by_name["unique_candidate_projections"] < 1
        or availability_count < 1
        or availability_count > occurrence_deliveries
        or candidate_io_by_name["duplicate_availability_deliveries"]
        > occurrence_deliveries - availability_count
        or candidate_io_by_name["candidate_projection_builds"]
        != candidate_io_by_name["unique_candidate_projections"]
        or candidate_io_by_name["candidate_projection_reuse_deliveries"]
        != occurrence_deliveries
        - candidate_io_by_name["unique_candidate_projections"]
        or candidate_io_by_name["repeated_candidate_projection_builds"] != 0
    ):
        raise ValueError("audit_batch_candidate_processing_repeated")


def validate_audit_batch_once_ledgers(
    *,
    block_io: Any,
    witness_io: Any,
    candidate_processing_io: Any,
) -> tuple[dict[str, int], dict[str, int], dict[str, int]]:
    """Return strict ledgers only when every physical/logical repeat is zero."""

    validated_block_io_by_name = _counter_ledger_by_name(
        block_io,
        field_name="audit_batch_block_io",
        expected_fields=_BLOCK_IO_FIELDS,
    )
    validated_witness_io_by_name = _counter_ledger_by_name(
        witness_io,
        field_name="audit_batch_witness_io",
        expected_fields=_WITNESS_IO_FIELDS,
    )
    validated_candidate_io_by_name = _counter_ledger_by_name(
        candidate_processing_io,
        field_name="audit_batch_candidate_processing_io",
        expected_fields=_CANDIDATE_PROCESSING_IO_FIELDS,
    )
    _validate_block_io_once(validated_block_io_by_name)
    _validate_witness_io_once(validated_witness_io_by_name)
    _validate_candidate_processing_once(validated_candidate_io_by_name)
    return (
        validated_block_io_by_name,
        validated_witness_io_by_name,
        validated_candidate_io_by_name,
    )


def _response_counts(raw_response: Mapping[str, Any]) -> _ResponseCounts:
    return _ResponseCounts(
        challenge_count=_positive_int(
            raw_response.get("challenge_count"),
            field_name="audit_batch_response_challenge_count",
        ),
        unique_challenge_count=_positive_int(
            raw_response.get("unique_challenge_count"),
            field_name="audit_batch_response_unique_challenge_count",
        ),
        matched_challenge_count=_positive_int(
            raw_response.get("matched_challenge_count"),
            field_name="audit_batch_response_matched_challenge_count",
        ),
        persisted_count=_positive_int(
            raw_response.get("persisted_audit_occurrence_count"),
            field_name="audit_batch_response_persisted_count",
        ),
        validated_persisted_count=_positive_int(
            raw_response.get("validated_persisted_audit_occurrence_count"),
            field_name="audit_batch_response_validated_persisted_count",
        ),
    )


def _validate_response_binding(
    *,
    request: AuditBatchRequest,
    request_digest: str,
    matched_digest: str,
    counts: _ResponseCounts,
    witness_io_by_name: Mapping[str, int],
    expected_source_witness: Mapping[str, Any],
    expected_audit_sample: Mapping[str, Any],
) -> None:
    from process.ptg_parts.ptg2_candidate_audit_batch_contract import (
        matched_audit_batch_digest,
    )

    expected_persisted_count = _positive_int(
        expected_audit_sample.get("sample_count"),
        field_name="audit_batch_expected_persisted_count",
    )
    expected_record_count = _positive_int(
        expected_source_witness.get("record_count"),
        field_name="audit_batch_expected_witness_record_count",
    )
    expected_evidence_count = _positive_int(
        expected_source_witness.get("evidence_dictionary_count"),
        field_name="audit_batch_expected_evidence_count",
    )
    if (
        request_digest != request.request_digest
        or counts.challenge_count != request.challenge_count
        or counts.unique_challenge_count > counts.challenge_count
        or counts.matched_challenge_count != counts.challenge_count
        or counts.persisted_count != expected_persisted_count
        or counts.validated_persisted_count != expected_persisted_count
        or matched_digest
        != matched_audit_batch_digest(
            request_digest,
            counts.matched_challenge_count,
        )
        or witness_io_by_name["record_decodes"] != expected_record_count
        or witness_io_by_name["unique_evidence_entries"]
        != expected_evidence_count
    ):
        raise ValueError("audit_batch_response_candidate_mismatch")


def parse_audit_batch_response(
    raw_response: Any,
    *,
    request: AuditBatchRequest,
    expected_source_witness: Mapping[str, Any],
    expected_audit_sample: Mapping[str, Any],
) -> AuditBatchResponse:
    """Validate one successful response and every once-only ledger invariant."""

    from process.ptg_parts.ptg2_candidate_audit_batch_contract import (
        PTG2_AUDIT_BATCH_RESPONSE_CONTRACT,
    )

    if not isinstance(raw_response, Mapping) or set(raw_response) != _RESPONSE_FIELDS:
        raise ValueError("audit_batch_response_fields_invalid")
    if raw_response.get("contract") != PTG2_AUDIT_BATCH_RESPONSE_CONTRACT:
        raise ValueError("audit_batch_response_contract_invalid")
    request_digest = _lower_hex_digest(
        raw_response.get("request_digest"),
        field_name="audit_batch_response_request_digest",
    )
    matched_digest = _lower_hex_digest(
        raw_response.get("matched_challenge_digest"),
        field_name="audit_batch_matched_challenge_digest",
    )
    counts = _response_counts(raw_response)
    block_io_by_name, witness_io_by_name, candidate_io_by_name = (
        validate_audit_batch_once_ledgers(
            block_io=raw_response.get("block_io"),
            witness_io=raw_response.get("witness_io"),
            candidate_processing_io=raw_response.get("candidate_processing_io"),
        )
    )
    _validate_response_binding(
        request=request,
        request_digest=request_digest,
        matched_digest=matched_digest,
        counts=counts,
        witness_io_by_name=witness_io_by_name,
        expected_source_witness=expected_source_witness,
        expected_audit_sample=expected_audit_sample,
    )
    return AuditBatchResponse(
        request_digest=request_digest,
        challenge_count=counts.challenge_count,
        unique_challenge_count=counts.unique_challenge_count,
        matched_challenge_count=counts.matched_challenge_count,
        persisted_audit_occurrence_count=counts.persisted_count,
        validated_persisted_audit_occurrence_count=(
            counts.validated_persisted_count
        ),
        matched_challenge_digest=matched_digest,
        duration_ms=_validated_duration_ms(raw_response.get("duration_ms")),
        block_io=block_io_by_name,
        witness_io=witness_io_by_name,
        candidate_processing_io=candidate_io_by_name,
    )


# Keep runtime annotation introspection working without reintroducing the
# contract/response import cycle at module initialization time. Both modules
# have defined their public types by the time this deferred import executes.
from process.ptg_parts.ptg2_candidate_audit_batch_contract import (
    AuditBatchRequest,
)


__all__ = [
    "AuditBatchResponse",
    "parse_audit_batch_response",
    "validate_audit_batch_once_ledgers",
]
