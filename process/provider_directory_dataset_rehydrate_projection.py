# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact compatibility projections for an isolated retained-dataset rebuild."""

from db.models import ProviderDirectoryCanonicalResource, ProviderDirectorySourceResource
from process.provider_directory_dataset_rehydrate_pin import PinnedDataset
from process.provider_directory_dataset_rehydrate_types import (
    DatasetRehydrationError,
    _quote_identifier,
    _table_ref,
)


def compatibility_projection(directory, pinned_source, model, resource_types):
    """Select a replacement, preserving rows outside the exact repaired scope.

    Execute this SELECT on a separate preservation Reader, closed before cut.
    The active SOURCE-only Reader continues to hold its exact physical pins.
    """
    selected_resource_types = tuple(resource_types)
    if (
        not selected_resource_types
        or len(set(selected_resource_types)) != len(selected_resource_types)
        or any(name not in directory.RESOURCE_MODELS_BY_TYPE for name in selected_resource_types)
    ):
        raise DatasetRehydrationError("provider_directory_dataset_rehydrate_resource_scope_invalid")
    resource_type_by_model = {directory.RESOURCE_MODELS_BY_TYPE[name]: name for name in selected_resource_types}
    if model in resource_type_by_model:
        selected_resource_types = (resource_type_by_model[model],)
        kind = "typed"
    elif model is ProviderDirectoryCanonicalResource:
        kind = "canonical"
    elif model is ProviderDirectorySourceResource:
        kind = "edge"
    else:
        raise DatasetRehydrationError("provider_directory_dataset_rehydrate_output_invalid")
    if type(pinned_source) is not PinnedDataset:
        raise DatasetRehydrationError("provider_directory_dataset_rehydrate_pin_required")
    source_relation = pinned_source.source_relation(directory)
    scope = pinned_source.scope
    params_by_name = {
        "source_id": scope.source_id,
        "dataset_id": scope.dataset_id,
        "root_run_id": scope.acquisition_root_run_id,
        "canonical_base": scope.canonical_api_base,
        "resource_types": list(selected_resource_types),
        "published_at": scope.published_at,
    }
    return _projection_sql(directory, model, kind, source_relation), params_by_name


def _projection_sql(directory, model, kind, source_relation):
    schema = directory._schema()
    incumbent = _table_ref(schema, model.__tablename__)
    columns = ",".join(_quote_identifier(column.name) for column in model.__table__.columns)
    json_columns = directory._json_columns(model.__table__)
    preserve = ",".join(
        _union_value("incumbent." + _quote_identifier(column.name), column.name, json_columns)
        for column in model.__table__.columns
    )
    incoming = ",".join(
        _union_value(_incoming_column(directory, column, kind), column.name, json_columns)
        for column in model.__table__.columns
    )
    type_match = "" if kind == "typed" else "AND repair.resource_type=incumbent.resource_type"
    scope_match = (
        "incumbent.canonical_api_base=:canonical_base "
        "AND incumbent.resource_type=ANY(CAST(:resource_types AS varchar[]))"
        if kind == "canonical"
        else "incumbent.source_id=:source_id"
        + (" AND incumbent.resource_type=ANY(CAST(:resource_types AS varchar[]))" if kind == "edge" else "")
    )
    previous = ""
    if kind == "canonical":
        previous = f"""LEFT JOIN {incumbent} previous
          ON previous.canonical_api_base=:canonical_base
         AND previous.resource_type=resource.resource_type
         AND previous.resource_id=resource.resource_id"""
    return f"""
        WITH retained AS NOT MATERIALIZED (
            SELECT resource_id, resource_type, payload_hash, payload_json
              FROM {source_relation}
             WHERE dataset_id=:dataset_id
               AND resource_type=ANY(CAST(:resource_types AS varchar[]))
        ), selected(source_id,dataset_id,evidence_run_id) AS (
            VALUES (CAST(:source_id AS varchar),CAST(:dataset_id AS varchar),CAST(:root_run_id AS varchar))
        ), dataset(dataset_id,published_at,validated_at,created_at) AS (
            VALUES (CAST(:dataset_id AS varchar),CAST(:published_at AS timestamp),
                    CAST(NULL AS timestamp),CAST(NULL AS timestamp))
        ), replacement ({columns}) AS (
            SELECT {preserve} FROM {incumbent} incumbent
              LEFT JOIN retained repair ON {scope_match}
               AND repair.resource_id=incumbent.resource_id {type_match}
             WHERE NOT ({scope_match}) OR (
                 incumbent.last_seen_run_id IS DISTINCT FROM :root_run_id AND repair.resource_id IS NULL
             )
            UNION ALL
            SELECT {incoming}
              FROM retained resource CROSS JOIN selected
              JOIN dataset ON dataset.dataset_id=selected.dataset_id
              {previous}
        )
        SELECT {columns} FROM replacement
    """


def _union_value(expression, name, json_columns):
    """Normalize both supported native JSON variants for SELECT-only transport."""
    return f"CAST({expression} AS jsonb)" if name in json_columns else expression


def _incoming_column(directory, column, kind):
    name = column.name
    if name == "canonical_api_base":
        return "CAST(:canonical_base AS text)"
    if name == "resource_type":
        return "resource.resource_type"
    if kind == "canonical":
        canonical_by_column = {
            "payload_hash": "resource.payload_hash",
            "payload_json": "CASE WHEN previous.payload_hash=resource.payload_hash THEN previous.payload_json END",
            "first_seen_run_id": "COALESCE(previous.first_seen_run_id,selected.evidence_run_id)",
        }
        if name in canonical_by_column:
            return canonical_by_column[name]
    return directory._provider_directory_artifact_payload_column_sql(column)
