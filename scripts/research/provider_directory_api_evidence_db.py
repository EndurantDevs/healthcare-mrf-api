"""Indexed database sampling for Provider Directory API evidence checks."""

from __future__ import annotations

import re
from typing import Any, Iterable, Mapping

from scripts.research.provider_directory_api_completion import (
    _completion_proof_from_row,
    current_dataset_completion_sql,
    fetch_current_dataset_completion_proofs,
)
from scripts.research.provider_directory_api_evidence_support import (
    MappedEvidenceWitness,
    NetworkWitness,
    OverlaySample,
    SourceSelection,
)


IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
MAPPED_RESOURCE_TYPES = ("PractitionerRole", "OrganizationAffiliation")
MAX_EXPECTED_EVIDENCE_ITEMS = 20


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


def mapped_evidence_candidate_sql(schema: str) -> str:
    """Return a current-dataset-fenced, per-source bounded witness query."""
    quoted_schema = _quote_identifier(schema, label="database schema")
    return f"""
        WITH requested_resources AS MATERIALIZED (
            SELECT source_id, resource_type
              FROM unnest($1::varchar[], $2::varchar[])
                   AS requested(source_id, resource_type)
        ), requested_sources AS MATERIALIZED (
            SELECT DISTINCT source_id FROM requested_resources
        ), current_endpoint_counts AS MATERIALIZED (
            SELECT source.source_id, source.endpoint_id
              FROM requested_sources AS requested
              JOIN {quoted_schema}.provider_directory_source AS source
                ON source.source_id = requested.source_id
              JOIN {quoted_schema}.provider_directory_endpoint_dataset AS dataset
                ON dataset.endpoint_id = source.endpoint_id
             WHERE dataset.is_current IS TRUE
          GROUP BY source.source_id, source.endpoint_id
            HAVING COUNT(*) = 1
        ), current_sources AS MATERIALIZED (
            SELECT current_endpoint.source_id, dataset.dataset_id,
                   COALESCE(
                       dataset.acquisition_root_run_id, dataset.import_run_id
                   )::varchar AS run_id
              FROM current_endpoint_counts AS current_endpoint
              JOIN {quoted_schema}.provider_directory_endpoint_dataset AS dataset
                ON dataset.endpoint_id = current_endpoint.endpoint_id
             WHERE dataset.is_current IS TRUE
               AND dataset.status = 'published'
               AND dataset.published_at IS NOT NULL
               AND dataset.superseded_at IS NULL
               AND COALESCE(
                       dataset.acquisition_root_run_id, dataset.import_run_id
                   ) IS NOT NULL
        )
        SELECT requested.source_id, requested.resource_type,
               sampled.resource_id, sampled.npi
          FROM requested_resources AS requested
          JOIN current_sources AS current_source
            ON current_source.source_id = requested.source_id
         CROSS JOIN LATERAL (
              SELECT DISTINCT ON (overlay.resource_id)
                     overlay.resource_id, overlay.npi
                FROM {quoted_schema}.provider_directory_address_overlay AS overlay
                JOIN {quoted_schema}.provider_directory_dataset_resource AS resource
                  ON resource.dataset_id = current_source.dataset_id
                 AND resource.resource_type = requested.resource_type
                 AND resource.resource_id = overlay.resource_id
               WHERE overlay.source_id = current_source.source_id
                 AND overlay.last_seen_run_id = current_source.run_id
                 AND overlay.resource_type = requested.resource_type
                 AND overlay.npi IS NOT NULL
            ORDER BY overlay.resource_id, overlay.npi, overlay.address_key
               LIMIT $3
         ) AS sampled
      ORDER BY requested.source_id, requested.resource_type,
               sampled.resource_id, sampled.npi;
    """


def _asyncpg_role_evidence_sql(schema: str, *, has_affiliations: bool) -> str:
    from api.endpoint.npi import _provider_directory_role_evidence_sql

    sql = _provider_directory_role_evidence_sql(
        _quote_identifier(schema, label="database schema"),
        has_catalog=True,
        has_affiliations=has_affiliations,
    )
    return sql.replace(":source_ids", "$1").replace(":role_ids", "$2")


def _asyncpg_affiliation_evidence_sql(schema: str) -> str:
    from api.endpoint.npi import _provider_directory_affiliation_evidence_sql

    sql = _provider_directory_affiliation_evidence_sql(
        _quote_identifier(schema, label="database schema"),
        has_catalog=True,
    )
    return sql.replace(":source_ids", "$1").replace(":affiliation_ids", "$2")


async def fetch_mapped_evidence_witnesses(
    conn: Any,
    *,
    schema: str,
    selections: Iterable[SourceSelection],
    witnesses_per_resource: int,
) -> dict[str, list[MappedEvidenceWitness]]:
    """Fetch deterministic exact-key mapped witnesses with per-source bounds."""
    if not 1 <= witnesses_per_resource <= 5:
        raise ValueError("witnesses_per_resource must be between one and five")
    selection_list = list(selections)
    witnesses_by_source = {selection.source_id: [] for selection in selection_list}
    requested_pair_list = [
        (selection.source_id, resource_type)
        for selection in selection_list
        for resource_type in MAPPED_RESOURCE_TYPES
        if resource_type in selection.resources
    ]
    if not requested_pair_list:
        return witnesses_by_source
    candidate_row_list = await conn.fetch(
        mapped_evidence_candidate_sql(schema),
        [source_id for source_id, _resource_type in requested_pair_list],
        [resource_type for _source_id, resource_type in requested_pair_list],
        witnesses_per_resource,
    )
    candidate_by_source = _mapped_candidate_map(
        candidate_row_list, set(witnesses_by_source)
    )
    selection_by_source = {
        selection.source_id: selection for selection in selection_list
    }
    for source_id in sorted(candidate_by_source):
        witnesses_by_source[source_id] = await _fetch_source_witness_list(
            conn,
            schema,
            selection_by_source[source_id],
            candidate_by_source[source_id],
        )
    return witnesses_by_source


def _mapped_candidate_map(
    candidate_row_list: Iterable[Any], allowed_source_ids: set[str]
) -> dict[str, dict[str, list[tuple[int, str]]]]:
    candidate_by_source: dict[str, dict[str, list[tuple[int, str]]]] = {}
    for candidate_row in candidate_row_list:
        row_map = getattr(candidate_row, "_mapping", candidate_row)
        source_id = str(row_map.get("source_id") or "")
        resource_type = str(row_map.get("resource_type") or "")
        resource_id = str(row_map.get("resource_id") or "")
        npi_value = row_map.get("npi")
        if (
            source_id not in allowed_source_ids
            or resource_type not in MAPPED_RESOURCE_TYPES
            or not resource_id
            or npi_value is None
        ):
            continue
        resource_candidate_list = candidate_by_source.setdefault(
            source_id, {}
        ).setdefault(resource_type, [])
        candidate = (int(npi_value), resource_id)
        if candidate not in resource_candidate_list:
            resource_candidate_list.append(candidate)
    return candidate_by_source


async def _fetch_source_witness_list(
    conn: Any,
    schema: str,
    selection: SourceSelection,
    candidate_by_resource: Mapping[str, list[tuple[int, str]]],
) -> list[MappedEvidenceWitness]:
    witness_list = []
    role_candidate_list = candidate_by_resource.get("PractitionerRole", [])
    if role_candidate_list:
        role_row_list = await conn.fetch(
            _asyncpg_role_evidence_sql(
                schema,
                has_affiliations="OrganizationAffiliation" in selection.resources,
            ),
            [selection.source_id] * len(role_candidate_list),
            [resource_id for _npi, resource_id in role_candidate_list],
        )
        witness_list.extend(
            _mapped_witnesses(role_candidate_list, role_row_list, "PractitionerRole")
        )
    affiliation_candidate_list = candidate_by_resource.get(
        "OrganizationAffiliation", []
    )
    if affiliation_candidate_list:
        affiliation_row_list = await conn.fetch(
            _asyncpg_affiliation_evidence_sql(schema),
            [selection.source_id] * len(affiliation_candidate_list),
            [resource_id for _npi, resource_id in affiliation_candidate_list],
        )
        witness_list.extend(
            _mapped_witnesses(
                affiliation_candidate_list,
                affiliation_row_list,
                "OrganizationAffiliation",
            )
        )
    return witness_list


def _mapped_witnesses(
    candidate_list: list[tuple[int, str]],
    evidence_row_list: Iterable[Any],
    resource_type: str,
) -> list[MappedEvidenceWitness]:
    """Map exact production evidence rows into bounded API expectations."""
    id_field = "role_id" if resource_type == "PractitionerRole" else "affiliation_id"
    base_evidence_type = (
        "role" if resource_type == "PractitionerRole" else "affiliation"
    )
    rows_by_id: dict[str, list[Mapping[str, Any]]] = {}
    for evidence_row in evidence_row_list:
        row_map = getattr(evidence_row, "_mapping", evidence_row)
        evidence_id = str(row_map.get(id_field) or "")
        if evidence_id:
            rows_by_id.setdefault(evidence_id, []).append(row_map)

    witness_list = []
    for npi, resource_id in candidate_list:
        evidence_map_list = rows_by_id.get(resource_id, [])
        if not any(
            evidence_map.get("evidence_type") == base_evidence_type
            for evidence_map in evidence_map_list
        ):
            continue
        plan_id_list = sorted(
            {
                str(evidence_map.get("resource_id"))
                for evidence_map in evidence_map_list
                if evidence_map.get("evidence_type") == "insurance_plan"
                and evidence_map.get("resource_id")
            }
        )[:MAX_EXPECTED_EVIDENCE_ITEMS]
        network_list = _network_witnesses(evidence_map_list)
        source_id = str(evidence_map_list[0].get("source_id") or "")
        if not source_id:
            continue
        witness_list.append(
            MappedEvidenceWitness(
                source_id,
                npi,
                resource_type,
                resource_id,
                tuple(plan_id_list),
                network_list,
            )
        )
    return witness_list


def _network_witnesses(
    evidence_map_list: list[Mapping[str, Any]],
) -> tuple[NetworkWitness, ...]:
    network_map_by_id = {
        str(evidence_map.get("resource_id")): evidence_map
        for evidence_map in evidence_map_list
        if evidence_map.get("evidence_type") == "network"
        and evidence_map.get("resource_id")
    }
    return tuple(
        NetworkWitness(
            network_id,
            bool(str(network_map_by_id[network_id].get("name") or "").strip()),
            bool(str(network_map_by_id[network_id].get("reference") or "").strip()),
        )
        for network_id in sorted(network_map_by_id)[:MAX_EXPECTED_EVIDENCE_ITEMS]
    )


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
