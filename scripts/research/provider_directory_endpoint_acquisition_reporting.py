"""Safe terminal summaries for Provider Directory acquisition reports."""

from __future__ import annotations

import datetime as dt
import re
import sys
from pathlib import Path
from typing import Any

try:
    from scripts.provider_directory_support_contract import RESOURCE_TYPES
    from scripts.provider_directory_verification_contract import VerificationUpdateError
    from scripts.research.provider_directory_endpoint_acquisition_support import (
        RUN_ID_PATTERN,
    )
except ModuleNotFoundError:
    scripts_dir = str(Path(__file__).resolve().parents[1])
    if scripts_dir not in sys.path:
        sys.path.insert(0, scripts_dir)
    from provider_directory_support_contract import RESOURCE_TYPES
    from provider_directory_verification_contract import VerificationUpdateError
    from provider_directory_endpoint_acquisition_support import RUN_ID_PATTERN

SENSITIVE_TEXT_PATTERN = re.compile(
    r"(?i)(?:bearer\s+\S+|token|secret|password|authorization|api[_-]?key|credential)"
)
SOURCE_ID_PATTERN = re.compile(r"pdfhir_[0-9a-f]{24}")
RAW_RUN_STATUSES = frozenset(
    {"queued", "starting", "running", "finalizing", "canceling", "succeeded", "failed", "canceled", "cancelled", "dead_letter"}
)
RESOURCE_STAT_FIELDS = frozenset(
    {
        "bulk_export_checkpoint_blocked_sources",
        "bulk_export_eligible_sources",
        "bulk_export_ineligible_sources",
        "bulk_export_requested_sources",
        "bulk_export_rest_fallback_sources",
        "bulk_export_sources",
        "caresource_opaque_cursor_post_count",
        "caresource_opaque_cursor_pre_count",
        "caresource_opaque_cursor_processed_rows",
        "caresource_opaque_cursor_sources",
        "caresource_opaque_cursor_unique_candidate_rows",
        "caresource_opaque_cursor_verified_sources",
        "collection_complete_sources",
        "last_updated_completeness_verified_sources",
        "last_updated_exact_leaf_count_sum",
        "last_updated_partition_sources",
        "last_updated_pass1_unique",
        "last_updated_pass2_unique",
        "last_updated_ranged_root_post",
        "last_updated_ranged_root_pre",
        "last_updated_staged_candidate_count",
        "last_updated_unfiltered_post",
        "last_updated_unfiltered_pre",
        "pages_fetched",
        "plan_graph_complete_sources",
        "rows_fetched",
        "sources_attempted",
        "sources_bounded",
        "sources_completed",
        "sources_empty",
        "sources_failed",
    }
)
ALWAYS_PRESERVED_RESOURCE_STAT_FIELDS = frozenset(
    {
        "rows_fetched",
        "sources_attempted",
        "sources_bounded",
        "sources_completed",
        "sources_failed",
    }
)
def _safe_timestamp(value: Any) -> str | None:
    if not isinstance(value, str) or SENSITIVE_TEXT_PATTERN.search(value):
        return None
    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return value if parsed.tzinfo is not None else None


def _safe_run_id(value: Any) -> str | None:
    return value if isinstance(value, str) and RUN_ID_PATTERN.fullmatch(value) else None


def _safe_count(value: Any) -> int | None:
    return value if type(value) is int and value >= 0 else None


def _safe_source_ids(value: Any) -> list[str] | None:
    if not isinstance(value, list) or not all(
        isinstance(source_id, str) and SOURCE_ID_PATTERN.fullmatch(source_id)
        for source_id in value
    ):
        return None
    return value


def _safe_resource_outcomes(metrics_by_name: dict[str, Any]) -> dict[str, Any]:
    raw_outcome_by_resource = metrics_by_name.get("resource_fetch_stats")
    if not isinstance(raw_outcome_by_resource, dict):
        return {}
    return {
        resource_type: {
            field_name: field_value
            for field_name, field_value in resource_outcome_by_field.items()
            if field_name in RESOURCE_STAT_FIELDS
            and _safe_count(field_value) is not None
            and (
                field_name in ALWAYS_PRESERVED_RESOURCE_STAT_FIELDS
                or field_value != 0
            )
        }
        for resource_type, resource_outcome_by_field in raw_outcome_by_resource.items()
        if resource_type in RESOURCE_TYPES
        and isinstance(resource_outcome_by_field, dict)
    }


def _terminal_error_summary(error: Any) -> dict[str, str] | None:
    if not isinstance(error, dict):
        return None
    safe_field_names = ("code", "type", "status")
    terminal_error_by_field = {
        name: str(error[name])[:500]
        for name in safe_field_names
        if isinstance(error.get(name), (str, int, float, bool))
        and "..." not in str(error[name])
        and not SENSITIVE_TEXT_PATTERN.search(str(error[name]))
    }
    return terminal_error_by_field or None


def run_summary(run_record: dict[str, Any]) -> dict[str, Any]:
    """Return a credential-safe summary of one importer run."""
    metrics = run_record.get("metrics") if isinstance(run_record.get("metrics"), dict) else {}
    params_by_name = run_record.get("params") if isinstance(run_record.get("params"), dict) else {}
    resource_outcomes = _safe_resource_outcomes(metrics)
    status = run_record.get("status")
    run_summary_dict = {
        "run_id": _safe_run_id(run_record.get("run_id")),
        "status": (
            status
            if isinstance(status, str) and status in RAW_RUN_STATUSES
            else None
        ),
        "created_at": _safe_timestamp(run_record.get("created_at")),
        "finished_at": _safe_timestamp(run_record.get("finished_at")),
        "retry_of_run_id": _safe_run_id(params_by_name.get("retry_of_run_id")),
        "source_ids": _safe_source_ids(metrics.get("source_ids")),
        "pagination_resume_required": (
            metrics.get("pagination_resume_required")
            if type(metrics.get("pagination_resume_required")) is bool
            else None
        ),
        "resource_outcomes": resource_outcomes,
    }
    run_summary_dict = {
        field_name: field_value
        for field_name, field_value in run_summary_dict.items()
        if field_value not in (None, {})
    }
    terminal_error = _terminal_error_summary(run_record.get("error"))
    if terminal_error:
        run_summary_dict["terminal_error"] = terminal_error
    return run_summary_dict


def validate_verification_update_metadata(
    report: dict[str, Any],
    entry_ids: set[str],
    terminal_statuses: set[str],
) -> set[str]:
    """Validate exact selected, terminal, and nonterminal report identities."""
    integration = report.get("verification_update")
    if integration is None:
        return set(report["entries"])
    if not isinstance(integration, dict):
        raise VerificationUpdateError("report verification_update must be an object")
    selected = integration.get("selected_entry_ids")
    terminal = integration.get("terminal_entry_ids")
    nonterminal = integration.get("nonterminal_entry_ids")
    eligible = integration.get("eligible")
    argv = integration.get("argv")
    entry_id_groups = (selected, terminal, nonterminal)
    if not all(isinstance(entry_id_group, list) for entry_id_group in entry_id_groups):
        raise VerificationUpdateError("report verification_update entry lists are required")
    if any(not isinstance(entry_id, str) for group in entry_id_groups for entry_id in group):
        raise VerificationUpdateError("report verification_update entry identities are invalid")
    if any(len(group) != len(set(group)) for group in entry_id_groups):
        raise VerificationUpdateError("report verification_update entry identities are duplicated")
    if not set(selected).issubset(entry_ids) or set(selected) != set(terminal) | set(nonterminal):
        raise VerificationUpdateError("report verification_update entry identities do not agree")
    report_entries = report["entries"]
    if not set(selected).issubset(report_entries) or any(
        not isinstance(report_entries[entry_id], dict) for entry_id in selected
    ):
        raise VerificationUpdateError("report verification_update entries are missing")
    if set(terminal) & set(nonterminal) or not isinstance(eligible, bool) or eligible != (not nonterminal):
        raise VerificationUpdateError("report verification_update eligibility is inconsistent")
    if (
        not isinstance(argv, list)
        or not all(isinstance(argument, str) for argument in argv)
        or "scripts/update_provider_directory_verification.py" not in argv
    ):
        raise VerificationUpdateError("report verification_update argv is invalid")
    expected_terminal_ids = {
        entry_id for entry_id in selected
        if report_entries[entry_id].get("status") in terminal_statuses
    }
    if set(terminal) != expected_terminal_ids:
        raise VerificationUpdateError("report verification_update terminal identities do not agree")
    return set(selected)


def validate_operator_attestation(
    report: dict[str, Any],
    report_entries: dict[str, Any],
    selected_entry_ids: set[str],
    environment: str,
    terminal_statuses: set[str],
) -> None:
    """Bind offline operator observations without claiming authenticated proof."""
    observation = report.get("observation")
    if not isinstance(observation, dict) or observation.get("method") != "operator-attested-read-only-export":
        return
    if not isinstance(report.get("verification_update"), dict):
        raise VerificationUpdateError(
            "operator-attested reports require verification update metadata"
        )
    if observation.get("environment") != environment:
        raise VerificationUpdateError("report observation environment does not match")
    operator_input_sha256 = observation.get("operator_input_sha256")
    if not isinstance(operator_input_sha256, str) or not re.fullmatch(
        r"[0-9a-f]{64}", operator_input_sha256
    ):
        raise VerificationUpdateError("report operator input identity is invalid")
    if any(
        report_entries[entry_id].get("access_verification") != "not_verified"
        for entry_id in selected_entry_ids
    ):
        raise VerificationUpdateError("operator-attested reports cannot claim verified access")
    nonterminal_entry_ids = {
        entry_id for entry_id in selected_entry_ids
        if report_entries[entry_id].get("status") not in terminal_statuses
    }
    if any(
        type(report_entries[entry_id].get("plan_bound")) is not bool
        for entry_id in nonterminal_entry_ids
    ):
        raise VerificationUpdateError(
            "operator-attested observations require plan binding state"
        )
