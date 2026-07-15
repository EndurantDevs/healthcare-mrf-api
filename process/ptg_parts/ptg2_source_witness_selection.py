# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Deterministic global selection for strict V3 source witnesses."""

from __future__ import annotations

import hashlib
import heapq
from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Sequence

from process.ptg_parts.ptg2_source_witness_contract import (
    CompressedSourceWitnessRecord,
    source_witness_targets,
)
from process.ptg_parts.ptg2_source_witness_primitives import (
    nonnegative_int,
    sha256_hex,
)


@dataclass(frozen=True)
class SourceWitnessPopulation:
    """Authoritative emitted populations used to calculate exact quotas."""

    occurrence_count: int
    provider_count: int
    emitted_rate_rows: int
    unqueryable_rate_rows: int


def source_set_digest(raw_source_sha256_values: Iterable[str]) -> str:
    """Hash the unique sorted physical source set into one stable digest."""

    source_digests = sorted(
        sha256_hex(source_digest, field_name="source digest")
        for source_digest in raw_source_sha256_values
    )
    if not source_digests or len(source_digests) != len(set(source_digests)):
        raise RuntimeError("strict V3 source witness requires a unique nonempty source set")
    digest = hashlib.sha256()
    for source_digest in source_digests:
        digest.update(bytes.fromhex(source_digest))
    return digest.hexdigest()


def _cohort_metric_sum(
    bundle_headers: Sequence[Mapping[str, Any]],
    cohort_name: str,
    metric_name: str,
) -> int:
    metric_total = 0
    for bundle_header in bundle_headers:
        cohort_metrics = bundle_header.get(cohort_name)
        if not isinstance(cohort_metrics, Mapping):
            raise RuntimeError("strict V3 source witness cohort metrics are invalid")
        metric_total += nonnegative_int(
            cohort_metrics,
            metric_name,
            error_field_name=f"{cohort_name} {metric_name}",
        )
    return metric_total


def source_population(
    bundle_headers: Sequence[Mapping[str, Any]],
) -> SourceWitnessPopulation:
    """Aggregate scanner populations and enforce the unqueryable-row policy."""

    population = SourceWitnessPopulation(
        occurrence_count=_cohort_metric_sum(
            bundle_headers,
            "rate_occurrence",
            "population_count",
        ),
        provider_count=_cohort_metric_sum(
            bundle_headers,
            "provider_reference",
            "population_count",
        ),
        emitted_rate_rows=_cohort_metric_sum(
            bundle_headers,
            "rate_occurrence",
            "emitted_rate_row_count",
        ),
        unqueryable_rate_rows=_cohort_metric_sum(
            bundle_headers,
            "rate_occurrence",
            "unqueryable_rate_row_count",
        ),
    )
    if population.unqueryable_rate_rows > population.emitted_rate_rows:
        raise RuntimeError("strict V3 unqueryable rate population is invalid")
    return population


def select_source_witness_records(
    candidate_records: Sequence[CompressedSourceWitnessRecord],
    *,
    occurrence_population: int,
    provider_population: int,
) -> tuple[list[CompressedSourceWitnessRecord], int, int, int]:
    """Select the exact deterministic occurrence/provider budget."""

    occurrence_target, provider_target, total_target = source_witness_targets(
        occurrence_population=occurrence_population,
        provider_population=provider_population,
    )
    selected_occurrences = heapq.nsmallest(
        occurrence_target,
        (
            witness_record
            for witness_record in candidate_records
            if witness_record.kind == "rate_occurrence"
        ),
        key=lambda witness_record: witness_record.selection_key,
    )
    selected_providers = heapq.nsmallest(
        provider_target,
        (
            witness_record
            for witness_record in candidate_records
            if witness_record.kind == "provider_reference"
        ),
        key=lambda witness_record: witness_record.selection_key,
    )
    if (
        len(selected_occurrences) != occurrence_target
        or len(selected_providers) != provider_target
    ):
        raise RuntimeError("strict V3 source witness global coverage is incomplete")
    selected_records = sorted(
        [*selected_occurrences, *selected_providers],
        key=lambda witness_record: (
            witness_record.kind,
            witness_record.selection_key,
        ),
    )
    if len(selected_records) != total_target:
        raise RuntimeError("strict V3 source witness exact total budget is incomplete")
    return selected_records, occurrence_target, provider_target, total_target


__all__ = [
    "SourceWitnessPopulation",
    "select_source_witness_records",
    "source_population",
    "source_set_digest",
]
