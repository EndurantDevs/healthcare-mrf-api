# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Contracts and typed records for strict V3 source-witness attestation."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping


PTG2_V3_SOURCE_WITNESS_CONTRACT = "ptg2_v3_source_witness_v3"
PTG2_V3_SOURCE_WITNESS_RECORD_CONTRACT = "ptg2_v3_source_witness_record_v2"
PTG2_V3_SOURCE_WITNESS_SELECTION = (
    "bottom_k_independent_occurrence_provider_cohorts_v3"
)
PTG2_V3_SOURCE_WITNESS_PAYLOAD_CONTRACT = "ptg2_v3_source_witness_payload_v5"
PTG2_V3_SOURCE_WITNESS_PAYLOAD_COMPRESSION = (
    "per_record_zlib_shared_evidence_dictionary_v1"
)
PTG2_V3_SOURCE_WITNESS_OCCURRENCE_TARGET = 10_000
PTG2_V3_SOURCE_WITNESS_PROVIDER_QUOTA = 1_000
PTG2_V3_SOURCE_WITNESS_TOTAL_TARGET = (
    PTG2_V3_SOURCE_WITNESS_OCCURRENCE_TARGET
    + PTG2_V3_SOURCE_WITNESS_PROVIDER_QUOTA
)
PTG2_V3_SOURCE_WITNESS_UNQUERYABLE_POLICY = (
    "count_but_exclude_from_npi_api_challenges_v1"
)
PTG2_V3_SOURCE_WITNESS_MAX_FILE_BYTES = 128 * 1024 * 1024
PTG2_V3_SOURCE_WITNESS_MAX_BUNDLE_BYTES = 512 * 1024 * 1024
PTG2_V3_SOURCE_WITNESS_MAX_RECORD_BYTES = 8 * 1024 * 1024
PTG2_V3_SOURCE_WITNESS_MAX_DECODED_RECORD_BYTES = 64 * 1024 * 1024
SOURCE_BUNDLE_MAGIC = b"PTG2SW02"
SOURCE_RECORD_MAGIC = b"PTG2SWR2"
PERSISTED_PAYLOAD_MAGIC = b"PTG2SWP5"


@dataclass(frozen=True)
class SourceWitnessPublication:
    """Describe one immutable PostgreSQL source-witness publication."""

    metadata: Mapping[str, Any]
    support_digest: bytes
    row_count: int
    stored_byte_count: int


@dataclass(frozen=True)
class SourceWitnessRecord:
    """Hold one independently verifiable source occurrence or provider record."""

    kind: str
    priority: int
    tie_breaker: str
    coordinate: tuple[int, int, int, int]
    raw_source_sha256: str
    raw_sha256: str
    linked_provider_sha256: str | None
    procedure: Mapping[str, Any] | None
    provider_evidence: Mapping[str, Any] | None
    expected: Mapping[str, Any]
    raw_json: bytes
    linked_provider_json: bytes | None


@dataclass(frozen=True)
class LoadedSourceWitness:
    """Expose a validated persisted witness and its immutable metadata."""

    metadata: Mapping[str, Any]
    records: tuple[SourceWitnessRecord, ...]
    evidence_by_sha256: Mapping[str, Mapping[str, Any]] = field(default_factory=dict)

    @property
    def occurrence_records(self) -> tuple[SourceWitnessRecord, ...]:
        """Return API-queryable atomic occurrence witnesses."""

        return tuple(
            witness_record
            for witness_record in self.records
            if witness_record.kind == "rate_occurrence"
        )

    @property
    def provider_records(self) -> tuple[SourceWitnessRecord, ...]:
        """Return independently sampled provider-reference witnesses."""

        return tuple(
            witness_record
            for witness_record in self.records
            if witness_record.kind == "provider_reference"
        )


@dataclass(frozen=True)
class CompressedSourceWitnessRecord:
    """Carry a scanner record through deterministic global selection."""

    kind: str
    priority: int
    tie_breaker: str
    raw_source_sha256: str
    compressed: bytes

    @property
    def selection_key(self) -> tuple[int, str, str]:
        """Return the source-stable global bottom-k ordering key."""

        return self.priority, self.tie_breaker, self.raw_source_sha256


def source_witness_targets(
    *,
    occurrence_population: int,
    provider_population: int,
) -> tuple[int, int, int]:
    """Return independent exact occurrence and provider witness counts."""

    if occurrence_population < 0 or provider_population < 0:
        raise RuntimeError("strict V3 source witness population is invalid")
    occurrence_target = min(
        occurrence_population,
        PTG2_V3_SOURCE_WITNESS_OCCURRENCE_TARGET,
    )
    provider_target = min(
        provider_population,
        PTG2_V3_SOURCE_WITNESS_PROVIDER_QUOTA,
    )
    total_target = occurrence_target + provider_target
    return occurrence_target, provider_target, total_target


def _strict_manifest_int(
    manifest: Mapping[str, Any],
    field_name: str,
    *,
    positive: bool = False,
) -> int:
    field_value = manifest.get(field_name)
    if type(field_value) is not int or field_value < int(positive):
        raise ValueError(f"invalid source witness {field_name}")
    return field_value


def _strict_manifest_digest(manifest: Mapping[str, Any], field_name: str) -> str:
    field_value = manifest.get(field_name)
    if not isinstance(field_value, str) or len(field_value) != 64 or any(
        character not in "0123456789abcdef" for character in field_value
    ):
        raise ValueError(f"invalid source witness {field_name}")
    return field_value


def _validate_manifest_contract(manifest_by_field: Mapping[str, Any]) -> None:
    expected_value_by_field = {
        "contract": PTG2_V3_SOURCE_WITNESS_PAYLOAD_CONTRACT,
        "format_version": 5,
        "selection_method": PTG2_V3_SOURCE_WITNESS_SELECTION,
        "population_semantics": "queryable_emitted_price_provider_occurrence_v1",
        "unqueryable_rate_policy": PTG2_V3_SOURCE_WITNESS_UNQUERYABLE_POLICY,
        "occurrence_target": PTG2_V3_SOURCE_WITNESS_OCCURRENCE_TARGET,
        "total_target": PTG2_V3_SOURCE_WITNESS_TOTAL_TARGET,
        "provider_quota": PTG2_V3_SOURCE_WITNESS_PROVIDER_QUOTA,
        "compression": PTG2_V3_SOURCE_WITNESS_PAYLOAD_COMPRESSION,
    }
    if any(
        manifest_by_field.get(field_name) != expected_value
        for field_name, expected_value in expected_value_by_field.items()
    ):
        raise ValueError("incompatible source witness contract")


def _manifest_populations(
    manifest_by_field: Mapping[str, Any],
) -> tuple[int, int]:
    occurrence_population = _strict_manifest_int(
        manifest_by_field,
        "queryable_occurrence_population_count",
    )
    if occurrence_population <= 0:
        raise ValueError("source witness has no queryable occurrence population")
    emitted_rate_rows = _strict_manifest_int(
        manifest_by_field,
        "emitted_rate_row_count",
        positive=True,
    )
    unqueryable_rate_rows = _strict_manifest_int(
        manifest_by_field,
        "unqueryable_rate_row_count",
    )
    if unqueryable_rate_rows > emitted_rate_rows:
        raise ValueError("source witness unqueryable rate count is invalid")
    provider_population = _strict_manifest_int(
        manifest_by_field,
        "provider_population_count",
    )
    return occurrence_population, provider_population


def _validate_manifest_counts(
    manifest_by_field: Mapping[str, Any],
    *,
    occurrence_population: int,
    provider_population: int,
) -> None:
    expected_count_tuple = source_witness_targets(
        occurrence_population=occurrence_population,
        provider_population=provider_population,
    )
    observed_count_tuple = (
        _strict_manifest_int(manifest_by_field, "occurrence_witness_count"),
        _strict_manifest_int(manifest_by_field, "provider_witness_count"),
        _strict_manifest_int(manifest_by_field, "record_count", positive=True),
    )
    if observed_count_tuple != expected_count_tuple:
        raise ValueError("incomplete source witness coverage")


def validate_source_witness_manifest(
    raw_manifest: Any,
    *,
    expected_source_count: int | None = None,
) -> dict[str, Any]:
    """Validate a reusable layout's complete source-witness contract."""

    if not isinstance(raw_manifest, Mapping):
        raise ValueError("missing source witness manifest")
    manifest_by_field = dict(raw_manifest)
    _validate_manifest_contract(manifest_by_field)
    source_count = _strict_manifest_int(
        manifest_by_field,
        "source_count",
        positive=True,
    )
    if expected_source_count is not None and source_count != expected_source_count:
        raise ValueError("source witness source_count mismatch")
    occurrence_population, provider_population = _manifest_populations(
        manifest_by_field
    )
    _validate_manifest_counts(
        manifest_by_field,
        occurrence_population=occurrence_population,
        provider_population=provider_population,
    )
    payload_bytes = _strict_manifest_int(
        manifest_by_field,
        "payload_bytes",
        positive=True,
    )
    if payload_bytes > PTG2_V3_SOURCE_WITNESS_MAX_FILE_BYTES:
        raise ValueError("source witness payload exceeds its bound")
    evidence_count = _strict_manifest_int(
        manifest_by_field,
        "evidence_dictionary_count",
    )
    record_count = _strict_manifest_int(
        manifest_by_field,
        "record_count",
        positive=True,
    )
    if evidence_count > record_count * 2:
        raise ValueError("source witness evidence dictionary count is invalid")
    _strict_manifest_int(
        manifest_by_field,
        "evidence_dictionary_raw_bytes",
    )
    _strict_manifest_int(
        manifest_by_field,
        "evidence_dictionary_stored_bytes",
    )
    for digest_field in (
        "source_set_digest",
        "sample_digest",
        "payload_sha256",
    ):
        _strict_manifest_digest(manifest_by_field, digest_field)
    return manifest_by_field


__all__ = [
    "CompressedSourceWitnessRecord",
    "LoadedSourceWitness",
    "PERSISTED_PAYLOAD_MAGIC",
    "PTG2_V3_SOURCE_WITNESS_CONTRACT",
    "PTG2_V3_SOURCE_WITNESS_MAX_BUNDLE_BYTES",
    "PTG2_V3_SOURCE_WITNESS_MAX_DECODED_RECORD_BYTES",
    "PTG2_V3_SOURCE_WITNESS_MAX_FILE_BYTES",
    "PTG2_V3_SOURCE_WITNESS_MAX_RECORD_BYTES",
    "PTG2_V3_SOURCE_WITNESS_OCCURRENCE_TARGET",
    "PTG2_V3_SOURCE_WITNESS_PAYLOAD_COMPRESSION",
    "PTG2_V3_SOURCE_WITNESS_PAYLOAD_CONTRACT",
    "PTG2_V3_SOURCE_WITNESS_PROVIDER_QUOTA",
    "PTG2_V3_SOURCE_WITNESS_RECORD_CONTRACT",
    "PTG2_V3_SOURCE_WITNESS_SELECTION",
    "PTG2_V3_SOURCE_WITNESS_TOTAL_TARGET",
    "PTG2_V3_SOURCE_WITNESS_UNQUERYABLE_POLICY",
    "SOURCE_BUNDLE_MAGIC",
    "SOURCE_RECORD_MAGIC",
    "SourceWitnessPublication",
    "SourceWitnessRecord",
    "source_witness_targets",
    "validate_source_witness_manifest",
]
