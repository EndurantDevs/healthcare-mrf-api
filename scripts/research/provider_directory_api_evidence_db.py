"""Indexed database sampling for Provider Directory API evidence checks."""

from __future__ import annotations

import re
from typing import Any, Iterable

from scripts.research.provider_directory_api_evidence_support import (
    OverlaySample,
    SourceSelection,
)


IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _quote_identifier(identifier: str, *, label: str) -> str:
    if not IDENTIFIER_RE.fullmatch(identifier):
        raise ValueError(f"invalid {label}")
    return f'"{identifier}"'


def overlay_sample_sql(schema: str, *, phone_required: bool = True) -> str:
    """Return an index-ordered current dataset/overlay probe."""
    quoted_schema = _quote_identifier(schema, label="database schema")
    phone_predicate = (
        "AND NULLIF(overlay.phone_number, '') IS NOT NULL" if phone_required else ""
    )
    return f"""
        WITH requested_sources AS MATERIALIZED (
            SELECT DISTINCT source_id
              FROM unnest($1::varchar[]) AS requested(source_id)
        ), current_sources AS MATERIALIZED (
            SELECT requested.source_id,
                   COALESCE(dataset.acquisition_root_run_id, dataset.import_run_id)::varchar AS run_id
              FROM requested_sources AS requested
              JOIN {quoted_schema}.provider_directory_source AS source
                ON source.source_id = requested.source_id
              JOIN {quoted_schema}.provider_directory_endpoint_dataset AS dataset
                ON dataset.endpoint_id = source.endpoint_id
             WHERE dataset.is_current IS TRUE
               AND dataset.status = 'published'
               AND dataset.published_at IS NOT NULL
               AND dataset.superseded_at IS NULL
               AND COALESCE(dataset.acquisition_root_run_id, dataset.import_run_id) IS NOT NULL
        )
        SELECT current_source.source_id, sampled.npi, sampled.phone_number
          FROM current_sources AS current_source
          CROSS JOIN LATERAL (
              SELECT overlay.npi, overlay.phone_number
                FROM {quoted_schema}.provider_directory_address_overlay AS overlay
               WHERE overlay.source_id = current_source.source_id
                 AND overlay.last_seen_run_id = current_source.run_id
                 AND overlay.npi IS NOT NULL
                 {phone_predicate}
               ORDER BY overlay.resource_type, overlay.resource_id
               LIMIT $2
          ) AS sampled
         ORDER BY current_source.source_id, sampled.npi;
    """


async def fetch_overlay_samples(
    conn: Any,
    *,
    schema: str,
    selections: Iterable[SourceSelection],
    samples_per_source: int,
) -> dict[str, list[OverlaySample]]:
    """Fetch up to five deterministic, de-duplicated samples per source."""
    if not 1 <= samples_per_source <= 5:
        raise ValueError("samples_per_source must be between one and five")
    source_ids = [selection.source_id for selection in selections]
    samples_by_source = {source_id: [] for source_id in source_ids}
    if not source_ids:
        return samples_by_source
    candidate_limit = samples_per_source * 20
    phone_candidate_rows = await conn.fetch(
        overlay_sample_sql(schema, phone_required=True), source_ids, candidate_limit
    )
    _append_overlay_samples(samples_by_source, phone_candidate_rows, samples_per_source)
    missing_source_ids = [
        source_id
        for source_id, samples in samples_by_source.items()
        if len(samples) < samples_per_source
    ]
    if missing_source_ids:
        fallback_rows = await conn.fetch(
            overlay_sample_sql(schema, phone_required=False),
            missing_source_ids,
            candidate_limit,
        )
        _append_overlay_samples(samples_by_source, fallback_rows, samples_per_source)
    return samples_by_source


def _append_overlay_samples(
    samples_by_source: dict[str, list[OverlaySample]],
    candidate_rows: Iterable[Any],
    samples_per_source: int,
) -> None:
    for candidate_row in candidate_rows:
        row_map = getattr(candidate_row, "_mapping", candidate_row)
        source_id = str(row_map.get("source_id") or "")
        npi_value = row_map.get("npi")
        if source_id not in samples_by_source or npi_value is None:
            continue
        npi = int(npi_value)
        current_samples = samples_by_source[source_id]
        if len(current_samples) >= samples_per_source:
            continue
        if any(sample.npi == npi for sample in current_samples):
            continue
        current_samples.append(
            OverlaySample(
                source_id,
                npi,
                _normalized_phone(row_map.get("phone_number")),
            )
        )


def _normalized_phone(phone_value: Any) -> str | None:
    digits = "".join(
        character for character in str(phone_value or "") if character.isdigit()
    )
    return digits if len(digits) >= 7 else None
