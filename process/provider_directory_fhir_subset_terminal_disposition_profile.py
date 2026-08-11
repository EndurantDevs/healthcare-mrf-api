# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed resource profiles for reviewed terminal-root seals."""

from process.provider_directory_fhir_manual_catalog import (
    MANUAL_CURRENT_VERSION_CENSUS_RESOURCES,
)


STABLE_COMPLETE_DISPOSITION = "stable_complete"
COUNT_DRIFT_DISPOSITION = "terminal_count_drift"
RETRYABLE_HTTP_500_DISPOSITION = "retryable_http_500"
VERIFIED_COMPLETE_DISPOSITION = "verified_complete"
TERMINAL_CENSUS_DRIFT_DISPOSITION = "terminal_census_drift"
TERMINAL_HTTP_410_DISPOSITION = "terminal_http_410"
DIRECT_V4_TERMINAL_DISPOSITION_ENABLED_ENV = (
    "HLTHPRT_PROVIDER_DIRECTORY_REVIEWED_SUBSET_DIRECT_V4_"
    "TERMINAL_DISPOSITION_ENABLED"
)
DIRECT_V4_CONTRACT_VERSION = (
    "healthporta.provider-directory.reviewed-subset-terminal-disposition.v2"
)
DIRECT_V4_REASON_CODE = "reviewed_current_version_census_drift"
DIRECT_V4_STRATEGY_VERSION = (
    "provider-directory-fhir-server-issued-traversal-subset-v4"
)
DIRECT_V4_TRAVERSAL_VERSION = (
    "provider-directory-fhir-smile-logical-offset-v3"
)
DIRECT_V4_CANONICALIZATION_VERSION = (
    "provider-directory-fhir-returned-resource-json-v2"
)
DIRECT_V4_CONTINUATION_STRATEGY = "smile-opaque-logical-offset-v3"
DIRECT_V4_SEMANTICS = "server-issued-traversal-subset"
DIRECT_V4_PROOF_CONTRACT_VERSION = 3
DIRECT_V4_COMPLETION_SCOPES = (
    "advertised-count-monotone-decrease-at-most-one",
    "source-issued-continuation",
    "returned-resource-content",
)
DIRECT_V4_CAMPAIGN_ID = "provider-directory-reviewed-subset-2026-08-10-v4"
DIRECT_V4_PAGE_COUNT = 250
DIRECT_V4_MAX_VERIFIED_DECREASE = 1
DIRECT_V4_TERMINAL_MARKER_SHA256 = (
    "e6f19eb70f8b5a84c76e61c19c379541bb6865b7de3114de01dd2a32181cb299"
)
DIRECT_V5_HTTP410_TERMINAL_DISPOSITION_ENABLED_ENV = (
    "HLTHPRT_PROVIDER_DIRECTORY_REVIEWED_SUBSET_DIRECT_V5_HTTP410_"
    "TERMINAL_DISPOSITION_ENABLED"
)
DIRECT_V5_CONTRACT_VERSION = (
    "healthporta.provider-directory.reviewed-subset-terminal-disposition.v3"
)
DIRECT_V5_REASON_CODE = "reviewed_current_version_census_http_410"
DIRECT_V5_STRATEGY_VERSION = (
    "provider-directory-fhir-server-issued-traversal-subset-v5"
)
DIRECT_V5_TRAVERSAL_VERSION = DIRECT_V4_TRAVERSAL_VERSION
DIRECT_V5_CANONICALIZATION_VERSION = DIRECT_V4_CANONICALIZATION_VERSION
DIRECT_V5_CONTINUATION_STRATEGY = DIRECT_V4_CONTINUATION_STRATEGY
DIRECT_V5_SEMANTICS = DIRECT_V4_SEMANTICS
DIRECT_V5_PROOF_CONTRACT_VERSION = 3
DIRECT_V5_COMPLETION_SCOPES = (
    "advertised-count-monotone-decrease-bounded-by-one-percent-and-twenty-pages",
    "terminal-logical-window-covers-advertised-pre",
    "source-issued-continuation",
    "returned-resource-content",
)
DIRECT_V5_CAMPAIGN_ID = "provider-directory-reviewed-subset-2026-08-10-v5"
DIRECT_V5_CUTOFF = "2026-08-10T21:12:54.000000Z"
DIRECT_V5_PAGE_COUNT = 250
DIRECT_V5_MAX_DECREASE_PAGES = 20
DIRECT_V5_MAX_DECREASE_BASIS_POINTS = 100
DIRECT_V5_TERMINAL_MARKER_SHA256 = (
    "87f1c25625562037f9544b30a62e8b1bbf625018c73076bb083b8680225b23d9"
)
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
DIRECT_V4_DRIFT_RESOURCE_TYPES = (
    "HealthcareService",
    "OrganizationAffiliation",
    "PractitionerRole",
)
DIRECT_V4_VERIFIED_RESOURCE_TYPES = tuple(
    resource_type
    for resource_type in EXPECTED_RESOURCE_TYPES
    if resource_type not in DIRECT_V4_DRIFT_RESOURCE_TYPES
)
DIRECT_V4_DISPOSITION_BY_RESOURCE_TYPE = {
    **{
        resource_type: VERIFIED_COMPLETE_DISPOSITION
        for resource_type in DIRECT_V4_VERIFIED_RESOURCE_TYPES
    },
    **{
        resource_type: TERMINAL_CENSUS_DRIFT_DISPOSITION
        for resource_type in DIRECT_V4_DRIFT_RESOURCE_TYPES
    },
}
DIRECT_V5_HTTP410_RESOURCE_TYPES = ("HealthcareService",)
DIRECT_V5_VERIFIED_RESOURCE_TYPES = tuple(
    resource_type
    for resource_type in EXPECTED_RESOURCE_TYPES
    if resource_type not in DIRECT_V5_HTTP410_RESOURCE_TYPES
)
DIRECT_V5_DISPOSITION_BY_RESOURCE_TYPE = {
    **{
        resource_type: VERIFIED_COMPLETE_DISPOSITION
        for resource_type in DIRECT_V5_VERIFIED_RESOURCE_TYPES
    },
    **{
        resource_type: TERMINAL_HTTP_410_DISPOSITION
        for resource_type in DIRECT_V5_HTTP410_RESOURCE_TYPES
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
DIRECT_V4_RESOURCE_DISPOSITION_FIELDS = RESOURCE_DISPOSITION_FIELDS | {
    "terminal_page_entry_count"
}
DIRECT_V4_LINEAGE_FIELDS = frozenset(
    {
        "checkpoint_retry_count",
        "competing_candidate_count",
        "current_dataset_count",
        "import_run_row_count",
        "owner_equals_root",
        "previous_dataset_present",
        "previous_reference_count",
    }
)
DIRECT_V4_TERMINAL_MARKER_FIELDS = TERMINAL_MARKER_FIELDS | {"direct_lineage"}
DIRECT_V5_TERMINAL_MARKER_FIELDS = TERMINAL_MARKER_FIELDS | {"direct_lineage"}


__all__ = (
    "ACTIVE_PROOF_FIELDS",
    "COUNT_DRIFT_DISPOSITION",
    "COUNT_DRIFT_PROOF_FIELDS",
    "COUNT_DRIFT_RESOURCE_TYPES",
    "DIRECT_V4_CAMPAIGN_ID",
    "DIRECT_V4_CANONICALIZATION_VERSION",
    "DIRECT_V4_COMPLETION_SCOPES",
    "DIRECT_V4_CONTINUATION_STRATEGY",
    "DIRECT_V4_CONTRACT_VERSION",
    "DIRECT_V4_DISPOSITION_BY_RESOURCE_TYPE",
    "DIRECT_V4_DRIFT_RESOURCE_TYPES",
    "DIRECT_V4_MAX_VERIFIED_DECREASE",
    "DIRECT_V4_PAGE_COUNT",
    "DIRECT_V4_PROOF_CONTRACT_VERSION",
    "DIRECT_V4_REASON_CODE",
    "DIRECT_V4_STRATEGY_VERSION",
    "DIRECT_V4_SEMANTICS",
    "DIRECT_V4_LINEAGE_FIELDS",
    "DIRECT_V4_RESOURCE_DISPOSITION_FIELDS",
    "DIRECT_V4_TERMINAL_MARKER_FIELDS",
    "DIRECT_V4_TERMINAL_DISPOSITION_ENABLED_ENV",
    "DIRECT_V4_TERMINAL_MARKER_SHA256",
    "DIRECT_V4_VERIFIED_RESOURCE_TYPES",
    "DIRECT_V4_TRAVERSAL_VERSION",
    "DIRECT_V5_CAMPAIGN_ID",
    "DIRECT_V5_CANONICALIZATION_VERSION",
    "DIRECT_V5_COMPLETION_SCOPES",
    "DIRECT_V5_CONTINUATION_STRATEGY",
    "DIRECT_V5_CONTRACT_VERSION",
    "DIRECT_V5_CUTOFF",
    "DIRECT_V5_DISPOSITION_BY_RESOURCE_TYPE",
    "DIRECT_V5_HTTP410_RESOURCE_TYPES",
    "DIRECT_V5_HTTP410_TERMINAL_DISPOSITION_ENABLED_ENV",
    "DIRECT_V5_MAX_DECREASE_BASIS_POINTS",
    "DIRECT_V5_MAX_DECREASE_PAGES",
    "DIRECT_V5_PAGE_COUNT",
    "DIRECT_V5_PROOF_CONTRACT_VERSION",
    "DIRECT_V5_REASON_CODE",
    "DIRECT_V5_SEMANTICS",
    "DIRECT_V5_STRATEGY_VERSION",
    "DIRECT_V5_TERMINAL_MARKER_FIELDS",
    "DIRECT_V5_TERMINAL_MARKER_SHA256",
    "DIRECT_V5_TRAVERSAL_VERSION",
    "DIRECT_V5_VERIFIED_RESOURCE_TYPES",
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
    "TERMINAL_CENSUS_DRIFT_DISPOSITION",
    "TERMINAL_HTTP_410_DISPOSITION",
    "VERIFIED_COMPLETE_DISPOSITION",
)
