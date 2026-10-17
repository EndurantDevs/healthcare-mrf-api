# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Native index predicates for immutable dataset relation readiness."""

READINESS_INDEXES = (
    ("dataset_network_plan", "provider_directory_dataset_network_plan_ready_v1_idx"),
    ("dataset_affiliation_organization", "provider_directory_dataset_affiliation_ready_v1_idx"),
)


def dataset_relation_ready_predicate(metadata_key: str, *, alias: str = "", version: str = "1") -> str:
    """Match the complete, versioned proof to the same published dataset."""
    column = f"{alias}." if alias else ""
    metadata = f"{column}publication_metadata_json::jsonb -> '{metadata_key}'"
    return f"""
        {column}is_current IS TRUE
        AND {column}status = 'published'
        AND {column}published_at IS NOT NULL
        AND {column}superseded_at IS NULL
        AND COALESCE({column}acquisition_root_run_id, {column}import_run_id) IS NOT NULL
        AND COALESCE({metadata} ->> 'complete', 'false') = 'true'
        AND COALESCE({metadata} ->> 'version', '') = '{version}'
        AND COALESCE({metadata} ->> 'dataset_id', '') = {column}dataset_id"""
