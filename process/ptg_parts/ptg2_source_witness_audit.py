# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Derive compact audit values with version-appropriate evidence lifetimes."""

from __future__ import annotations

from typing import Any, Callable, Mapping, TypeVar

from process.ptg_parts.ptg2_source_witness_contract import LoadedSourceWitness, SourceWitnessRecord
from process.ptg_parts.ptg2_source_witness_fragment_decode import FragmentWitnessView

_Result = TypeVar("_Result")


def map_source_witness_records(
    witness: LoadedSourceWitness | FragmentWitnessView,
    mapper: Callable[[SourceWitnessRecord, Mapping[str, Mapping[str, Any]]], _Result],
) -> tuple[tuple[_Result, ...], dict[str, int]]:
    """Keep v5 reuse, or map v6 groups without retaining a full token cache."""

    if isinstance(witness, FragmentWitnessView):
        values, processing_io = witness.map_records(mapper)
        return values, {"payload_reads": 1, "payload_decodes": 1, **processing_io}
    values = []
    references = 0
    for record in witness.records:
        references += 1 + int(record.linked_provider_sha256 is not None)
        values.append(mapper(record, witness.evidence_by_sha256))
    unique = len(witness.evidence_by_sha256)
    return tuple(values), {
        "payload_reads": 1,
        "payload_decodes": 1,
        "record_decodes": len(witness.records),
        "unique_evidence_entries": unique,
        "evidence_decompressions": unique,
        "evidence_sha256_hashes": unique,
        "evidence_json_parses": unique,
        "evidence_reuse_deliveries": references - unique,
        "repeated_evidence_decompressions": 0,
        "repeated_evidence_sha256_hashes": 0,
        "repeated_evidence_json_parses": 0,
    }
