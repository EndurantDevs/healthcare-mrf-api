# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Database reads for exact Profile dataset-variant selection."""

from __future__ import annotations

from typing import Any, Callable, Mapping

from process.provider_directory_dataset_scoped_publication import (
    LEGACY_PRACTITIONER_VARIANT,
)


_PROFILE_SELECTION_DATASET_SQL = """
SELECT dataset.endpoint_id, dataset.dataset_id,
       dataset.acquisition_root_run_id, dataset.dataset_hash,
       dataset.status, dataset.is_current, dataset.resource_count,
       dataset.validated_at, dataset.published_at,
       dataset.superseded_at, dataset.publication_metadata_json,
       CASE WHEN legacy.dataset_id IS NOT NULL
                 THEN {legacy_ready_expression}
            WHEN rooted.dataset_id IS NOT NULL
                 THEN {rooted_ready_expression}
            ELSE false
       END AS dataset_scoped_ready,
       CASE WHEN legacy.dataset_id IS NOT NULL
                 THEN '{legacy_variant}'
            ELSE rooted.publication_kind
       END AS dataset_scoped_variant,
       CASE WHEN legacy.dataset_id IS NOT NULL
                 THEN '{legacy_variant}'
            ELSE rooted.publication_kind
       END AS dataset_scoped_publication_kind,
       COALESCE(legacy.admission_id, rooted.admission_id)
           AS dataset_scoped_admission_id,
       COALESCE(
           legacy.semantic_projection_as_of,
           rooted.semantic_projection_as_of
       ) AS dataset_scoped_projection_as_of,
       COALESCE(
           legacy.source_authority_id,
           rooted.source_authority_id
       ) AS dataset_scoped_authority_id,
       COALESCE(legacy.operation_key, rooted.operation_key)
           AS dataset_scoped_operation_key,
       COALESCE(legacy.cohort_complete, rooted.cohort_complete)
           AS dataset_scoped_cohort_complete,
       rooted.rooted_graph_complete
           AS dataset_scoped_rooted_graph_complete,
       COALESCE(
           legacy.endpoint_collection_complete,
           rooted.endpoint_collection_complete
       ) AS dataset_scoped_endpoint_collection_complete,
       COALESCE(
           legacy.endpoint_complete,
           rooted.endpoint_complete
       ) AS dataset_scoped_endpoint_complete
  FROM {endpoint_dataset_ref} AS dataset
  LEFT JOIN {legacy_header_ref} AS legacy
    ON legacy.dataset_id = dataset.dataset_id
  LEFT JOIN {rooted_header_ref} AS rooted
    ON rooted.dataset_id = dataset.dataset_id
 WHERE dataset.status = 'published'
   AND dataset.is_current = true
   AND dataset.published_at IS NOT NULL
   AND dataset.superseded_at IS NULL
 ORDER BY dataset.published_at DESC, dataset.dataset_id DESC,
          dataset.endpoint_id DESC;
"""


def _profile_selection_dataset_sql(
    endpoint_dataset_ref: str,
    schema_ref: str,
    *,
    exact_readiness: bool = True,
) -> str:
    legacy_header_ref = (
        f'{schema_ref}."provider_directory_uhc_flex_practitioner_dataset"'
    )
    legacy_ready_ref = (
        f'{schema_ref}."provider_directory_uhc_flex_practitioner_dataset_ready"'
    )
    rooted_header_ref = f'{schema_ref}."provider_directory_rooted_graph_dataset"'
    rooted_ready_ref = f'{schema_ref}."provider_directory_rooted_graph_dataset_ready"'
    return _PROFILE_SELECTION_DATASET_SQL.format(
        endpoint_dataset_ref=endpoint_dataset_ref,
        legacy_header_ref=legacy_header_ref,
        legacy_ready_expression=(
            f"{legacy_ready_ref}(dataset.dataset_id)"
            if exact_readiness
            else "legacy.status = 'published' AND legacy.is_current IS TRUE"
        ),
        legacy_variant=LEGACY_PRACTITIONER_VARIANT,
        rooted_header_ref=rooted_header_ref,
        rooted_ready_expression=(
            f"{rooted_ready_ref}(dataset.dataset_id)"
            if exact_readiness
            else "rooted.status = 'published' AND rooted.is_current IS TRUE"
        ),
    )


async def load_profile_selection_dataset_rows(
    *,
    database: Any,
    endpoint_dataset_ref: str,
    schema_ref: str,
    row_mapping: Callable[[Any], Mapping[str, Any]],
    exact_readiness: bool = True,
) -> list[Mapping[str, Any]]:
    """Load current parents with exact or proposal readiness projections."""

    database_rows = await database.all(
        _profile_selection_dataset_sql(
            endpoint_dataset_ref,
            schema_ref,
            exact_readiness=exact_readiness,
        )
    )
    return [row_mapping(database_row) for database_row in database_rows]


__all__ = ("load_profile_selection_dataset_rows",)
