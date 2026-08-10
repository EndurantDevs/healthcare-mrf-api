# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed resource profile for the reviewed mixed-terminal seal."""

from process.provider_directory_fhir_manual_catalog import (
    MANUAL_CURRENT_VERSION_CENSUS_RESOURCES,
)


STABLE_COMPLETE_DISPOSITION = "stable_complete"
COUNT_DRIFT_DISPOSITION = "terminal_count_drift"
RETRYABLE_HTTP_500_DISPOSITION = "retryable_http_500"
SOURCE_PROFILE_RESOURCE_TYPES = MANUAL_CURRENT_VERSION_CENSUS_RESOURCES
EXPECTED_RESOURCE_TYPES = tuple(sorted(SOURCE_PROFILE_RESOURCE_TYPES))
STABLE_COMPLETE_RESOURCE_TYPES = ("Organization", "Practitioner")
COUNT_DRIFT_RESOURCE_TYPES = ("Location",)
RETRYABLE_HTTP_500_RESOURCE_TYPES = (
    "HealthcareService",
    "InsurancePlan",
    "OrganizationAffiliation",
    "PractitionerRole",
)
EXPECTED_DISPOSITION_BY_RESOURCE_TYPE = {
    **{
        resource_type: STABLE_COMPLETE_DISPOSITION
        for resource_type in STABLE_COMPLETE_RESOURCE_TYPES
    },
    **{
        resource_type: COUNT_DRIFT_DISPOSITION
        for resource_type in COUNT_DRIFT_RESOURCE_TYPES
    },
    **{
        resource_type: RETRYABLE_HTTP_500_DISPOSITION
        for resource_type in RETRYABLE_HTTP_500_RESOURCE_TYPES
    },
}
DIAGNOSTIC_FIELDS = frozenset(
    {
        "complete",
        "collection_complete",
        "traversal_complete",
        "source_continuation_exhausted",
        "absence_semantics",
        "plan_graph_complete",
        "bounded",
        "error",
        "fetch_mode",
        "pages_fetched",
        "rows_fetched",
        "rows_written",
        "resource_scan_concurrency_effective",
        "resource_scan_concurrency_requested",
        "source_fetch_elapsed_ms",
        "stream_write_elapsed_seconds",
        "checkpoint_persist_elapsed_seconds",
        "page_prefetch_eligible",
        "page_prefetch_started",
        "page_prefetch_consumed",
        "page_prefetch_discarded",
        "page_prefetch_wait_seconds",
        "row_limit_reached",
        "page_limit_reached",
        "hard_page_limit_reached",
        "deadline_reached",
        "next_url_remaining",
        "retry_not_before",
        "source_fetch",
        "last_updated_completeness",
        "caresource_opaque_cursor_completeness",
        "current_version_census_completeness",
        "server_issued_subset_completeness",
        "server_issued_subset_coverage",
        "pagination_cooldown_retries",
        "pagination_cooldown_wait_seconds",
        "pagination_cooldown_recovered",
        "pagination_cooldown_exhausted",
        "pagination_cooldown_deadline_blocked",
    }
)
ACTIVE_PROOF_FIELDS = frozenset(
    {
        "campaign_id",
        "canonicalization_version",
        "completion_scopes",
        "continuation_hop_sha256",
        "continuation_shape_sha256",
        "contract_identity",
        "contract_version",
        "cutoff",
        "page_count",
        "page_entry_counts",
        "page_geometry",
        "pre_count",
        "resource_type",
        "semantics",
        "strategy_version",
        "traversal_version",
        "verified",
    }
)
STABLE_COMPLETE_PROOF_FIELDS = ACTIVE_PROOF_FIELDS | frozenset(
    {
        "advertised_post",
        "advertised_pre",
        "deficit",
        "post_count",
        "processed_rows",
        "returned_unique",
        "terminal_page_geometry",
        "terminal_reason",
        "unique_candidate_rows",
        "unreturned_count",
    }
)
COUNT_DRIFT_PROOF_FIELDS = STABLE_COMPLETE_PROOF_FIELDS | {"failure"}
TERMINAL_MARKER_FIELDS = frozenset(
    {
        "contract_version",
        "reason_code",
        "source_scope_sha256",
        "resource_types",
        "resource_dispositions",
        "checkpoint_count",
        "checkpoint_pages_processed",
        "diagnostic_pages_processed",
        "terminal_page_delta",
        "checkpoint_rows_processed",
        "resource_count",
        "proof_shard_count",
        "proof_row_count",
        "source_diagnostics_sha256",
        "source_import_sha256",
        "candidate_metadata_sha256",
    }
)
RESOURCE_DISPOSITION_FIELDS = frozenset(
    {
        "disposition",
        "checkpoint_state",
        "checkpoint_pages",
        "diagnostic_pages",
        "page_delta",
        "retained_rows",
        "advertised_pre",
        "advertised_post",
        "returned_unique",
        "deficit",
        "diagnostic_sha256",
        "checkpoint_proof_sha256",
        "start_url_sha256",
        "recent_cursor_hashes_sha256",
    }
)


__all__ = (
    "ACTIVE_PROOF_FIELDS",
    "COUNT_DRIFT_DISPOSITION",
    "COUNT_DRIFT_PROOF_FIELDS",
    "COUNT_DRIFT_RESOURCE_TYPES",
    "DIAGNOSTIC_FIELDS",
    "EXPECTED_DISPOSITION_BY_RESOURCE_TYPE",
    "EXPECTED_RESOURCE_TYPES",
    "RESOURCE_DISPOSITION_FIELDS",
    "RETRYABLE_HTTP_500_DISPOSITION",
    "RETRYABLE_HTTP_500_RESOURCE_TYPES",
    "STABLE_COMPLETE_DISPOSITION",
    "STABLE_COMPLETE_PROOF_FIELDS",
    "STABLE_COMPLETE_RESOURCE_TYPES",
    "SOURCE_PROFILE_RESOURCE_TYPES",
    "TERMINAL_MARKER_FIELDS",
)
