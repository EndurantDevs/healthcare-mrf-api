from __future__ import annotations

from tests.ptg2_rust_scanner_security_process_cases import (
    test_live_process_probe_is_fail_closed,
    test_scratch_owner_parser_and_live_orphan_guard,
    test_scratch_prepare_tolerates_reap_error_and_cleans_creation_failure,
    test_signal_group_fallback_and_terminal_shortcuts,
    test_sync_group_kill_timeout_uses_last_resort_kill,
    test_async_group_kill_timeout_uses_last_resort_kill,
    test_failed_conversion_cleanup_handles_unavailable_spawn,
    test_default_binary_search_skips_debug_when_release_is_required,
    test_scanner_error_frames_and_progress_field_parser_cover_malformed_edges,
    test_progress_parsing_and_messages_cover_all_basis_and_counter_shapes,
    test_progress_and_metric_observer_failures_are_nonfatal,
    test_factor_mode_summary_validation,
    test_factor_mode_environment_requires_paired_outputs,
    test_top_level_scanner_malformed_frames_and_typed_failure,
)
from tests.ptg2_rust_scanner_security_frames_cases import (
    test_compact_scanner_all_optional_paths_and_factor_frames,
    test_compact_scanner_setup_failure_reaps_process_and_scratch,
    test_async_compact_bridge_forwards_records_and_reader_failure,
    test_return_code_labels_cover_unknown_signal,
    test_subprocess_session_process_group_and_control_edge_paths,
    test_async_terminate_terminal_shortcut,
    test_binary_profiles_configured_selection_and_strict_validators,
    test_json_fallback_and_progress_basis_formatting_branches,
    test_live_progress_derived_percent_and_metric_observer,
    test_top_level_scanner_success_stderr_and_missing_binary,
    test_compact_missing_binary_and_run_directory,
    test_compact_spawn_failure_tolerates_scratch_cleanup_error,
    test_compact_rejects_malformed_or_incomplete_frame_contract,
    test_compact_stderr_tail_failure_and_cleanup_warning,
)

from tests.ptg2_rust_scanner_security_compact_cases import (
    test_shared_graph_conversion_precondition_failures,
    test_shared_graph_converter_stderr_and_failure_paths,
    test_strict_shared_graph_scalar_and_path_validation,
    test_compact_no_stderr_and_setup_cleanup_paths,
    test_compact_setup_joins_started_thread_on_late_setup_failure,
    test_compact_mid_frame_timeout_reports_running_status,
    test_async_bridge_retries_full_queue_and_handles_iterators_without_close,
    test_async_bridge_close_value_error_contract,
    test_return_code_label_known_signal,
    test_remaining_process_branch_shortcuts,
    test_async_kill_timeout_skips_kill_for_finished_process,
    test_default_binary_search_selects_release,
    test_top_level_scanner_without_stderr,
)

from tests.ptg2_rust_scanner_security_graph_cases import (
    test_shared_graph_digest_directory_and_total_mismatch_guards,
)
