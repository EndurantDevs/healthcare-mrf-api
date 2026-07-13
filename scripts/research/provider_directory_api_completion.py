"""Durable current-dataset completion proofs for Provider Directory evidence."""

from __future__ import annotations

import json
import re
from typing import Any, Iterable, Mapping

from scripts.research.provider_directory_api_evidence_support import SourceSelection


IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
COMPLETION_RESOURCE_TYPES = ("PractitionerRole", "OrganizationAffiliation")


def _quote_identifier(identifier: str, *, label: str) -> str:
    if not IDENTIFIER_RE.fullmatch(identifier):
        raise ValueError(f"invalid {label}")
    return f'"{identifier}"'


def _current_dataset_completion_ctes_sql(quoted_schema: str) -> str:
    return f"""
        WITH requested_resources AS MATERIALIZED (
            SELECT source_id, resource_type
              FROM unnest($1::varchar[], $2::varchar[])
                   AS requested(source_id, resource_type)
        ), requested_sources AS MATERIALIZED (
            SELECT DISTINCT source_id FROM requested_resources
        ), current_endpoint_counts AS MATERIALIZED (
            SELECT requested.source_id, source.endpoint_id
              FROM requested_sources AS requested
              JOIN {quoted_schema}.provider_directory_source AS source
                ON source.source_id = requested.source_id
              JOIN {quoted_schema}.provider_directory_endpoint_dataset AS dataset
                ON dataset.endpoint_id = source.endpoint_id
             WHERE dataset.is_current IS TRUE
          GROUP BY requested.source_id, source.endpoint_id
            HAVING COUNT(*) = 1
        ), current_datasets AS MATERIALIZED (
            SELECT current_endpoint.source_id, dataset.dataset_id,
                   COALESCE(
                       dataset.acquisition_root_run_id, dataset.import_run_id
                   )::varchar AS acquisition_root_run_id,
                   dataset.import_run_id::varchar AS terminal_run_id,
                   dataset.publication_metadata_json
              FROM current_endpoint_counts AS current_endpoint
              JOIN {quoted_schema}.provider_directory_endpoint_dataset AS dataset
                ON dataset.endpoint_id = current_endpoint.endpoint_id
             WHERE dataset.is_current IS TRUE
               AND dataset.status = 'published'
               AND dataset.published_at IS NOT NULL
               AND dataset.superseded_at IS NULL
               AND dataset.import_run_id IS NOT NULL
               AND COALESCE(dataset.acquisition_root_run_id,
                            dataset.import_run_id) IS NOT NULL
        )
    """


def _current_dataset_completion_select_sql(quoted_schema: str) -> str:
    return f"""
        SELECT requested.source_id, requested.resource_type,
               dataset.dataset_id, dataset.acquisition_root_run_id,
               dataset.terminal_run_id, dataset.publication_metadata_json,
               terminal_run.importer AS terminal_importer,
               terminal_run.status AS terminal_status,
               terminal_run.finished_at AS terminal_finished_at,
               terminal_run.params AS terminal_params,
               terminal_run.metrics AS terminal_metrics,
               terminal_run.error AS terminal_error,
               (
                   SELECT count(*)
                     FROM {quoted_schema}.provider_directory_dataset_resource AS resource
                    WHERE resource.dataset_id = dataset.dataset_id
                      AND resource.resource_type = requested.resource_type
               ) AS dataset_resource_count,
               CASE
                   WHEN requested.resource_type = 'OrganizationAffiliation'
                   THEN EXISTS (
                       SELECT 1
                         FROM {quoted_schema}.provider_directory_address_overlay AS overlay
                        WHERE overlay.source_id = requested.source_id
                          AND overlay.last_seen_run_id = dataset.acquisition_root_run_id
                          AND overlay.resource_type = requested.resource_type
                          AND overlay.npi IS NOT NULL
                   )
                   ELSE NULL::boolean
               END AS provider_surface_evidence_present
          FROM requested_resources AS requested
          JOIN current_datasets AS dataset
            ON dataset.source_id = requested.source_id
          LEFT JOIN {quoted_schema}.import_run AS terminal_run
            ON terminal_run.run_id = dataset.terminal_run_id
      ORDER BY requested.source_id, requested.resource_type;
    """


def current_dataset_completion_sql(schema: str) -> str:
    """Return one current-dataset completion record per source/resource."""
    quoted_schema = _quote_identifier(schema, label="database schema")
    return "\n".join(
        (
            _current_dataset_completion_ctes_sql(quoted_schema),
            _current_dataset_completion_select_sql(quoted_schema),
        )
    )


async def fetch_current_dataset_completion_proofs(
    conn: Any,
    *,
    schema: str,
    selections: Iterable[SourceSelection],
) -> dict[str, dict[str, dict[str, Any]]]:
    """Classify selected resources from durable current-dataset evidence."""
    selection_list = list(selections)
    completion_proof_by_source = {
        selection.source_id: {
            resource_type: {"state": "unproven"}
            for resource_type in COMPLETION_RESOURCE_TYPES
            if resource_type in selection.resources
        }
        for selection in selection_list
    }
    requested_pairs = [
        (selection.source_id, resource_type)
        for selection in selection_list
        for resource_type in COMPLETION_RESOURCE_TYPES
        if resource_type in selection.resources
    ]
    if not requested_pairs:
        return completion_proof_by_source
    completion_row_list = await conn.fetch(
        current_dataset_completion_sql(schema),
        [source_id for source_id, _resource_type in requested_pairs],
        [resource_type for _source_id, resource_type in requested_pairs],
    )
    for completion_row in completion_row_list:
        row_map = getattr(completion_row, "_mapping", completion_row)
        source_id = str(row_map.get("source_id") or "")
        resource_type = str(row_map.get("resource_type") or "")
        if resource_type not in completion_proof_by_source.get(source_id, {}):
            continue
        completion_proof_by_source[source_id][resource_type] = (
            _completion_proof_from_row(row_map, source_id, resource_type)
        )
    return completion_proof_by_source


def _mapping_value(value: Any) -> Mapping[str, Any] | None:
    if isinstance(value, Mapping):
        return value
    if not isinstance(value, str):
        return None
    try:
        decoded = json.loads(value)
    except (TypeError, ValueError):
        return None
    return decoded if isinstance(decoded, Mapping) else None


def _is_empty_json_value(value: Any) -> bool:
    """Accept database JSON null/empty-object representations only."""
    if value is None or value == {}:
        return True
    if not isinstance(value, str):
        return False
    try:
        decoded = json.loads(value)
    except (TypeError, ValueError):
        return False
    return decoded is None or decoded == {}


def _strict_nonnegative_int(value: Any) -> int | None:
    return (
        value
        if isinstance(value, int) and not isinstance(value, bool) and value >= 0
        else None
    )


def _is_exact_source_list(value: Any, source_id: str) -> bool:
    return isinstance(value, list) and value == [source_id]


def _is_resource_selected(value: Any, resource_type: str) -> bool:
    return isinstance(value, list) and resource_type in value


def _completion_diagnostic(
    metadata: Mapping[str, Any] | None,
    row_map: Mapping[str, Any],
    source_id: str,
    resource_type: str,
) -> Mapping[str, Any] | None:
    if not metadata:
        return None
    proof = metadata.get("completion_proof_v1")
    record = proof if isinstance(proof, Mapping) else metadata
    if (
        str(record.get("acquisition_root_run_id") or "")
        != str(row_map.get("acquisition_root_run_id") or "")
        or str(record.get("terminal_run_id") or row_map.get("terminal_run_id") or "")
        != str(row_map.get("terminal_run_id") or "")
        or not _is_exact_source_list(record.get("source_ids"), source_id)
        or not _is_resource_selected(record.get("selected_resources"), resource_type)
    ):
        return None
    diagnostics = record.get("resource_diagnostics")
    diagnostic = (
        diagnostics.get(resource_type) if isinstance(diagnostics, Mapping) else None
    )
    return diagnostic if isinstance(diagnostic, Mapping) else None


def _is_completed_empty_diagnostic(diagnostic: Mapping[str, Any]) -> bool:
    return bool(
        diagnostic.get("complete") is True
        and diagnostic.get("bounded") is False
        and diagnostic.get("error") in (None, "")
        and diagnostic.get("next_url_remaining") is False
        and _strict_nonnegative_int(diagnostic.get("rows_fetched")) == 0
        and _strict_nonnegative_int(diagnostic.get("rows_written")) == 0
    )


def _has_matching_terminal_run(
    row_map: Mapping[str, Any], source_id: str, resource_type: str
) -> bool:
    terminal_params = _mapping_value(row_map.get("terminal_params"))
    terminal_run_id = str(row_map.get("terminal_run_id") or "")
    acquisition_root_run_id = str(row_map.get("acquisition_root_run_id") or "")
    root_run_id = (
        str(terminal_params.get("provider_directory_pagination_root_run_id") or "")
        if terminal_params
        else terminal_run_id
    ) or terminal_run_id
    resources = terminal_params.get("resources") if terminal_params else None
    selected_resources = (
        tuple(part for part in resources.split(",") if part)
        if isinstance(resources, str)
        else tuple(resources) if isinstance(resources, list) else ()
    )
    return bool(
        terminal_run_id
        and acquisition_root_run_id
        and row_map.get("terminal_importer") == "provider-directory-fhir"
        and row_map.get("terminal_status") == "succeeded"
        and row_map.get("terminal_finished_at") is not None
        and _is_empty_json_value(row_map.get("terminal_error"))
        and root_run_id == acquisition_root_run_id
        and terminal_params is not None
        and _is_exact_source_list(terminal_params.get("source_ids"), source_id)
        and resource_type in selected_resources
    )


def _has_matching_terminal_metrics(
    row_map: Mapping[str, Any], source_id: str, resource_type: str
) -> bool:
    metrics = _mapping_value(row_map.get("terminal_metrics"))
    if not metrics or not _is_exact_source_list(metrics.get("source_ids"), source_id):
        return False
    resource_rows = metrics.get("resource_rows")
    completion = metrics.get("resource_fetch_completed_source_ids")
    stats_by_resource = metrics.get("resource_fetch_stats")
    stats = (
        stats_by_resource.get(resource_type)
        if isinstance(stats_by_resource, Mapping)
        else None
    )
    return bool(
        isinstance(resource_rows, Mapping)
        and _strict_nonnegative_int(resource_rows.get(resource_type)) == 0
        and isinstance(completion, Mapping)
        and _is_exact_source_list(completion.get(resource_type), source_id)
        and isinstance(stats, Mapping)
        and _strict_nonnegative_int(stats.get("sources_attempted")) == 1
        and _strict_nonnegative_int(stats.get("sources_completed")) == 1
        and _strict_nonnegative_int(stats.get("sources_bounded")) == 0
        and _strict_nonnegative_int(stats.get("sources_failed")) == 0
        and _strict_nonnegative_int(stats.get("sources_empty")) == 1
        and _strict_nonnegative_int(stats.get("rows_fetched")) == 0
    )


def _completion_proof_from_row(
    row_map: Mapping[str, Any], source_id: str, resource_type: str
) -> dict[str, Any]:
    resource_count = _strict_nonnegative_int(row_map.get("dataset_resource_count"))
    if resource_count is None:
        return {"state": "unproven"}
    if resource_count > 0:
        if (
            resource_type == "OrganizationAffiliation"
            and row_map.get("provider_surface_evidence_present") is False
        ):
            return {
                "state": "provider_surface_not_applicable",
                "dataset_id": str(row_map.get("dataset_id") or ""),
                "dataset_resource_count": resource_count,
            }
        return {
            "state": "positive",
            "dataset_id": str(row_map.get("dataset_id") or ""),
        }
    if not _has_matching_terminal_run(row_map, source_id, resource_type):
        return {"state": "unproven"}
    diagnostic = _completion_diagnostic(
        _mapping_value(row_map.get("publication_metadata_json")),
        row_map,
        source_id,
        resource_type,
    )
    if diagnostic is None or not _is_completed_empty_diagnostic(diagnostic):
        return {"state": "unproven"}
    if not _has_matching_terminal_metrics(row_map, source_id, resource_type):
        return {"state": "unproven"}
    return {
        "state": "completed_empty",
        "dataset_id": str(row_map.get("dataset_id") or ""),
        "terminal_run_id": str(row_map.get("terminal_run_id") or ""),
    }
