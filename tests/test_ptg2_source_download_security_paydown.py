# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from tests.ptg2_source_download_security_request_cases import (
    test_container_validation_covers_unreadable_corrupt_empty_and_size_skip,
    test_resume_offset_and_progress_coercion_failure_paths,
    test_redirect_rewrites_303_and_rejects_missing_or_excessive_locations,
    test_validated_request_releases_response,
    test_fetch_head_metadata_handles_client_error,
    test_probe_range_response_shapes,
    test_probe_range_swallows_request_failure,
    test_range_download_streams_progress_and_cleans_sidecar,
    test_range_download_guards_and_rolls_back_failed_count,
    test_prepare_single_get_response_rejects_unsafe_resume_and_full_shapes,
    test_stream_single_get_enforces_declared_and_streamed_limits_and_reports,
    test_preserve_single_get_partial_writes_resume_or_resets,
)

from tests.ptg2_source_download_security_transfer_cases import (
    test_single_get_retry_and_terminal_error_branches,
    test_local_download_limit_preserves_partial_manifest,
    test_reuse_candidate_missing_external_unprotected_and_valid,
    test_should_materialize_handles_oserror,
    test_retained_logical_skips_invalid_size_and_external_candidates,
    test_facades_observers_and_worker_success_error_paths,
    test_sync_facade_resets_context_on_failure_and_plain_path,
    test_download_scheduler_assigns_monotonic_indexes_without_job_metadata,
    test_stage_tracker_logical_and_validation_failures,
    test_range_validation_etag_status_and_resume_sidecar,
    test_environment_host_and_small_container_branches,
)

from tests.ptg2_source_download_security_artifact_cases import (
    test_range_download_resets_oversized_file_and_advances_completed_progress,
    test_single_get_terminal_retry_sleep_and_incomplete_body,
    test_publish_downloaded_raw_rejects_post_publish_checksum,
    test_reuse_candidate_second_protection_failure_disables_reuse,
    test_local_download_emits_progress_and_nonpartial_failure_removes_stage,
    test_range_selection_limit_unsafe_and_success,
    test_materialize_json_source_and_sync_wrapper,
    test_gzip_stat_failure_and_misc_formatting_branches,
    test_range_download_retries_transient_failure,
    test_single_get_resumes_complete_prefix_and_advances_threshold,
)

from tests.ptg2_source_download_security_edge_cases import (
    test_corrupt_reuse_candidate_is_recorded_before_fresh_download,
    test_fresh_truncated_gzip_fails_full_integrity_check,
)

