# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Select and publish exact-budget source witnesses for immutable PTG V3."""

from __future__ import annotations

from typing import Any, Mapping, Sequence

from process.ptg_parts.ptg2_source_witness_bundle import read_scanner_bundle
from process.ptg_parts.ptg2_source_witness_codec import decode_record
from process.ptg_parts.ptg2_source_witness_contract import (
    CompressedSourceWitnessRecord,
    LoadedSourceWitness,
    PTG2_V3_SOURCE_WITNESS_CONTRACT,
    PTG2_V3_SOURCE_WITNESS_MAX_RECORD_BYTES,
    PTG2_V3_SOURCE_WITNESS_PAYLOAD_CONTRACT,
    PTG2_V3_SOURCE_WITNESS_PROVIDER_QUOTA,
    PTG2_V3_SOURCE_WITNESS_RECORD_CONTRACT,
    PTG2_V3_SOURCE_WITNESS_SELECTION,
    PTG2_V3_SOURCE_WITNESS_TOTAL_TARGET,
    PTG2_V3_SOURCE_WITNESS_UNQUERYABLE_POLICY,
    SourceWitnessRecord,
)
from process.ptg_parts.ptg2_source_witness_persisted_decode import (
    decode_persisted_source_witness,
)
from process.ptg_parts.ptg2_source_witness_persisted_encode import (
    SourceWitnessPayloadCounts,
    encode_persisted_source_witness,
)
from process.ptg_parts.ptg2_source_witness_primitives import sha256_hex
from process.ptg_parts.ptg2_source_witness_selection import (
    select_source_witness_records,
    source_population,
    source_set_digest,
)


def _read_source_bundles(
    bundle_entries: Sequence[Mapping[str, Any]],
    expected_sources: Sequence[str],
) -> tuple[list[dict[str, Any]], list[CompressedSourceWitnessRecord]]:
    if not bundle_entries or len(bundle_entries) != len(expected_sources):
        raise RuntimeError("strict V3 source witness coverage is incomplete")
    bundle_headers: list[dict[str, Any]] = []
    candidate_records: list[CompressedSourceWitnessRecord] = []
    for bundle_entry in bundle_entries:
        bundle_header, bundle_records = read_scanner_bundle(bundle_entry)
        bundle_headers.append(bundle_header)
        candidate_records.extend(bundle_records)
    observed_sources = sorted(
        sha256_hex(
            bundle_header.get("raw_source_sha256"),
            field_name="observed raw source digest",
        )
        for bundle_header in bundle_headers
    )
    if observed_sources != list(expected_sources):
        raise RuntimeError("strict V3 source witness set does not match imported sources")
    return bundle_headers, candidate_records


def build_persisted_source_witness(
    bundle_entries: Sequence[Mapping[str, Any]],
    *,
    expected_raw_source_sha256: Sequence[str],
) -> tuple[bytes, dict[str, Any]]:
    """Merge per-source candidates into one exact-budget persisted payload."""

    expected_sources = sorted(
        sha256_hex(source_digest, field_name="expected raw source digest")
        for source_digest in expected_raw_source_sha256
    )
    bundle_headers, candidate_records = _read_source_bundles(
        bundle_entries,
        expected_sources,
    )
    population = source_population(bundle_headers)
    selected_records, occurrence_count, provider_count, record_count = (
        select_source_witness_records(
            candidate_records,
            occurrence_population=population.occurrence_count,
            provider_population=population.provider_count,
        )
    )
    if occurrence_count <= 0:
        raise RuntimeError("strict V3 source witness has no queryable occurrence evidence")
    return encode_persisted_source_witness(
        selected_records,
        SourceWitnessPayloadCounts(
            source_count=len(expected_sources),
            source_digest=source_set_digest(expected_sources),
            occurrence_population=population.occurrence_count,
            provider_population=population.provider_count,
            emitted_rate_rows=population.emitted_rate_rows,
            unqueryable_rate_rows=population.unqueryable_rate_rows,
            occurrence_count=occurrence_count,
            provider_count=provider_count,
            record_count=record_count,
        ),
    )


__all__ = [
    "LoadedSourceWitness",
    "PTG2_V3_SOURCE_WITNESS_CONTRACT",
    "PTG2_V3_SOURCE_WITNESS_MAX_RECORD_BYTES",
    "PTG2_V3_SOURCE_WITNESS_PAYLOAD_CONTRACT",
    "PTG2_V3_SOURCE_WITNESS_PROVIDER_QUOTA",
    "PTG2_V3_SOURCE_WITNESS_RECORD_CONTRACT",
    "PTG2_V3_SOURCE_WITNESS_SELECTION",
    "PTG2_V3_SOURCE_WITNESS_TOTAL_TARGET",
    "PTG2_V3_SOURCE_WITNESS_UNQUERYABLE_POLICY",
    "SourceWitnessRecord",
    "build_persisted_source_witness",
    "decode_persisted_source_witness",
    "source_set_digest",
]
