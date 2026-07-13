"""HTTP and external-run validation support for the endpoint acquisition harness."""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.parse
import urllib.request
from typing import Any


MONTHLY_FULL_REFRESH_PRESET = "monthly-full"
# Keep these explicit values aligned with import-control's audited monthly schedule.
MONTHLY_FULL_ADOPTION_OVERRIDES_BY_NAME = {
    "concurrency": 12,
    "open_only": False,
    "include_auth_required": True,
    "linked_resource_deadline_seconds": 1800,
}
MONTHLY_FULL_OPTIONAL_PARAM_NAMES = ("probe", "resources", "retest_results_url")
MONTHLY_FULL_FORBIDDEN_MODE_NAMES = (
    "canonical_backfill_only",
    "contact_backfill_only",
    "publish_artifacts_only",
    "seed_only",
)
SCAN_PROVIDER_DIRECTORY_BASE = "https://providerdirectory.scanhealthplan.com"
LAST_UPDATED_PROOF_METRIC_NAMES = (
    "last_updated_unfiltered_pre",
    "last_updated_ranged_root_pre",
    "last_updated_exact_leaf_count_sum",
    "last_updated_pass1_unique",
    "last_updated_pass2_unique",
    "last_updated_staged_candidate_count",
    "last_updated_ranged_root_post",
    "last_updated_unfiltered_post",
)


def _is_import_param_match(
    param_name: str,
    actual_value: Any,
    expected_value: Any,
) -> bool:
    """Compare stored import-control values using accepted importer forms."""
    if param_name == "resources" and isinstance(actual_value, list):
        return actual_value == expected_value.split(",")
    if isinstance(expected_value, int) and not isinstance(expected_value, bool):
        if isinstance(actual_value, bool):
            return False
        try:
            return int(actual_value) == expected_value
        except (TypeError, ValueError):
            return False
    return actual_value == expected_value


def _monthly_full_run_param_errors(
    entry: dict[str, Any],
    actual_params_by_name: dict[str, Any],
    expected_params_by_name: dict[str, Any],
) -> list[str]:
    """Validate the exact nonpublishing monthly acquisition schedule shape."""
    if entry["classification"] not in {"acquisition", "bulk_acquisition"}:
        return ["params.refresh_preset is not valid for this manifest entry"]
    raw_preset = actual_params_by_name.get("refresh_preset")
    normalized_preset = (
        raw_preset.strip().lower().replace("_", "-")
        if isinstance(raw_preset, str)
        else ""
    )
    if normalized_preset != MONTHLY_FULL_REFRESH_PRESET:
        return ["params.refresh_preset is not the audited monthly-full preset"]

    required_params_by_name = dict(expected_params_by_name)
    optional_params_by_name = {
        param_name: required_params_by_name.pop(param_name)
        for param_name in MONTHLY_FULL_OPTIONAL_PARAM_NAMES
    }
    required_params_by_name.update(MONTHLY_FULL_ADOPTION_OVERRIDES_BY_NAME)
    required_params_by_name["refresh_preset"] = MONTHLY_FULL_REFRESH_PRESET
    errors = [
        f"params.{param_name} does not match the audited monthly acquisition profile"
        for param_name, expected_value in required_params_by_name.items()
        if not _is_import_param_match(
            param_name,
            normalized_preset
            if param_name == "refresh_preset"
            else actual_params_by_name.get(param_name),
            expected_value,
        )
    ]
    for param_name, expected_value in optional_params_by_name.items():
        actual_value = actual_params_by_name.get(param_name)
        if actual_value not in (None, "") and not _is_import_param_match(
            param_name,
            actual_value,
            expected_value,
        ):
            errors.append(
                f"params.{param_name} does not match the audited monthly acquisition profile"
            )
    return errors


def run_param_errors(
    entry: dict[str, Any],
    actual_params_by_name: dict[str, Any],
    expected_params_by_name: dict[str, Any],
    probe_omitted_keys: set[str],
) -> list[str]:
    """Validate campaign or monthly run parameters against one manifest entry."""
    if actual_params_by_name.get("refresh_preset") is not None:
        errors = _monthly_full_run_param_errors(
            entry,
            actual_params_by_name,
            expected_params_by_name,
        )
    else:
        errors = [
            f"params.{param_name} does not match the manifest"
            for param_name, expected_value in expected_params_by_name.items()
            if actual_params_by_name.get(param_name) != expected_value
        ]
    actual_endpoint_scope = str(
        actual_params_by_name.get("provider_directory_endpoint_scope") or ""
    ).rstrip("/")
    expected_endpoint_scope = str(entry["canonical_base"]).rstrip("/")
    if actual_endpoint_scope and actual_endpoint_scope != expected_endpoint_scope:
        errors.append("params.provider_directory_endpoint_scope does not match the manifest")
    if actual_params_by_name.get("refresh_preset") is not None and not actual_endpoint_scope:
        errors.append("params.provider_directory_endpoint_scope is required for monthly adoption")
    if entry["classification"] == "probe_only":
        forbidden_keys = sorted(probe_omitted_keys.intersection(actual_params_by_name))
        if forbidden_keys:
            errors.append(
                "probe-only run contains resource/pagination params: "
                + ",".join(forbidden_keys)
            )
    for param_name in MONTHLY_FULL_FORBIDDEN_MODE_NAMES:
        if actual_params_by_name.get("refresh_preset") is not None and actual_params_by_name.get(param_name):
            errors.append(f"params.{param_name} is incompatible with acquisition")
    return errors


class ImportControlHttpClient:
    """Minimal credential-safe import-control JSON client."""

    def __init__(self, base_url: str, token_env: str, timeout_seconds: float = 60.0):
        parsed_url = urllib.parse.urlsplit(base_url.rstrip("/"))
        if parsed_url.scheme not in {"http", "https"} or not parsed_url.netloc or parsed_url.username or parsed_url.password:
            raise ValueError("control URL must be a credential-free HTTP(S) URL")
        self.base_url = base_url.rstrip("/")
        self.token_env = token_env
        self.timeout_seconds = timeout_seconds

    def _request_json(self, path: str, method: str = "GET", body: dict[str, Any] | None = None) -> dict[str, Any]:
        headers_by_name = {"Accept": "application/json"}
        token = os.getenv(self.token_env, "").strip()
        if token:
            headers_by_name["Authorization"] = f"Bearer {token}"
        encoded_body = None
        if body is not None:
            headers_by_name["Content-Type"] = "application/json"
            encoded_body = json.dumps(body).encode("utf-8")
        request = urllib.request.Request(self.base_url + path, data=encoded_body, headers=headers_by_name, method=method)
        try:
            with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:  # nosec B310
                decoded_dict = json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            raise RuntimeError(f"import-control returned HTTP {exc.code}") from exc
        except urllib.error.URLError as exc:
            raise RuntimeError("import-control request failed") from exc
        if not isinstance(decoded_dict, dict):
            raise RuntimeError("import-control returned a non-object response")
        return decoded_dict

    def list_runs(self) -> list[dict[str, Any]]:
        """List the current Provider Directory run mirror."""
        query = urllib.parse.urlencode({"importer": "provider-directory-fhir", "limit": 500})
        response_body = self._request_json(f"/v1/runs?{query}")
        run_list = response_body.get("items")
        return [run for run in run_list if isinstance(run, dict)] if isinstance(run_list, list) else []

    def get_run(self, run_id: str) -> dict[str, Any]:
        """Read one mirrored import-control run."""
        return self._request_json(f"/v1/runs/{urllib.parse.quote(run_id, safe='')}")

    def create_run(self, request_body: dict[str, Any]) -> dict[str, Any]:
        """Create one run; callers gate this behind explicit apply mode."""
        return self._request_json("/v1/runs", method="POST", body=request_body)


def _last_updated_completeness_errors(
    entry: dict[str, Any],
    resource_type: str,
    resource_stats: dict[str, Any],
) -> list[str]:
    canonical_base = str(entry.get("canonical_base") or "").rstrip("/")
    partition_source_count = resource_stats.get("last_updated_partition_sources")
    is_proof_required = (
        canonical_base == SCAN_PROVIDER_DIRECTORY_BASE
        or partition_source_count not in (None, 0)
    )
    if not is_proof_required:
        return []
    expected_source_count = len(entry["source_ids"])
    errors: list[str] = []
    if partition_source_count != expected_source_count:
        errors.append(
            f"{resource_type} lacks exact last-updated partition source metrics"
        )
    if (
        resource_stats.get("last_updated_completeness_verified_sources")
        != expected_source_count
    ):
        errors.append(
            f"{resource_type} lacks verified last-updated completeness proof"
        )
    proof_values = [
        resource_stats.get(metric_name)
        for metric_name in LAST_UPDATED_PROOF_METRIC_NAMES
    ]
    if any(
        isinstance(proof_value, bool) or not isinstance(proof_value, int)
        for proof_value in proof_values
    ) or len(set(proof_values)) != 1:
        errors.append(
            f"{resource_type} last-updated completeness counts do not reconcile"
        )
    return errors


def acquisition_metric_errors(entry: dict[str, Any], metrics: dict[str, Any]) -> list[str]:
    """Validate exact completion metrics for one REST acquisition entry."""

    errors: list[str] = []
    source_ids = list(entry["source_ids"])
    if metrics.get("source_ids") != source_ids:
        errors.append("metrics.source_ids does not match the endpoint")
    if metrics.get("source_import_sources_selected") != len(source_ids):
        errors.append("selected source count is not exact")
    if metrics.get("source_import_groups_attempted") != 1:
        errors.append("selected source group count is not one")
    completion = metrics.get("resource_fetch_completed_source_ids")
    completion_by_resource = completion if isinstance(completion, dict) else {}
    stats = metrics.get("resource_fetch_stats")
    stats_by_resource = stats if isinstance(stats, dict) else {}
    for resource_type in entry["resources"]:
        if completion_by_resource.get(resource_type) != source_ids:
            errors.append(f"{resource_type} did not complete for the exact source")
        resource_stats = stats_by_resource.get(resource_type)
        if not isinstance(resource_stats, dict) or resource_stats.get("sources_completed", 0) < 1:
            errors.append(f"{resource_type} lacks completed fetch metrics")
        elif resource_stats.get("sources_bounded", 0) or resource_stats.get("sources_failed", 0):
            errors.append(f"{resource_type} was bounded or failed")
        if isinstance(resource_stats, dict):
            errors.extend(
                _last_updated_completeness_errors(
                    entry,
                    resource_type,
                    resource_stats,
                )
            )
    return errors


def bulk_acquisition_metric_errors(entry: dict[str, Any], metrics: dict[str, Any]) -> list[str]:
    """Validate that every selected resource used the configured Bulk path."""

    bulk_export_mode = metrics.get("bulk_export_mode")
    if bulk_export_mode is None:
        return []
    if not isinstance(bulk_export_mode, dict) or bulk_export_mode.get("effective") is not True:
        return ["bulk_export_mode must be effective for every selected resource"]
    source_count = len(entry["source_ids"])
    expected_fetches = len(entry["resources"]) * source_count
    if bulk_export_mode.get("effective_resource_fetches") != expected_fetches:
        return ["bulk_export_mode effective resource fetch count is not exact"]
    stats_by_resource = metrics.get("resource_fetch_stats")
    if not isinstance(stats_by_resource, dict):
        return ["bulk_export_mode lacks resource fetch metrics"]
    return [
        f"{resource_type} was not effectively acquired through bulk export"
        for resource_type in entry["resources"]
        if not isinstance(stats_by_resource.get(resource_type), dict)
        or stats_by_resource[resource_type].get("bulk_export_sources") != source_count
    ]


def external_run_errors(entry: dict[str, Any], run_record: dict[str, Any]) -> list[str]:
    """Bind an externally completed acquisition to its audited endpoint and data."""
    errors: list[str] = []
    params_by_name = run_record.get("params") if isinstance(run_record.get("params"), dict) else {}
    metrics_by_name = run_record.get("metrics") if isinstance(run_record.get("metrics"), dict) else {}
    if run_record.get("importer") != "provider-directory-fhir":
        errors.append("external run importer does not match provider-directory-fhir")
    expected_source_ids = list(entry["source_ids"])
    if params_by_name.get("source_ids") != expected_source_ids:
        errors.append("external params.source_ids does not match the endpoint")
    if metrics_by_name.get("source_ids") != expected_source_ids:
        errors.append("external metrics.source_ids does not match the endpoint")
    if metrics_by_name.get("source_import_sources_selected") != len(expected_source_ids):
        errors.append("external selected source count is not exact")
    if metrics_by_name.get("source_import_groups_attempted") != 1:
        errors.append("external selected source group count is not one")
    resource_rows = metrics_by_name.get("resource_rows")
    if not isinstance(resource_rows, dict) or sum(max(0, int(row_count or 0)) for row_count in resource_rows.values()) <= 0:
        errors.append("external acquisition has no imported resource rows")
    resource_stats = metrics_by_name.get("resource_fetch_stats")
    if not isinstance(resource_stats, dict) or not resource_stats:
        errors.append("external acquisition has no resource completion metrics")
    elif any(
        not isinstance(stats, dict)
        or stats.get("sources_completed", 0) < 1
        or stats.get("sources_bounded", 0)
        or stats.get("sources_failed", 0)
        for stats in resource_stats.values()
    ):
        errors.append("external acquisition contains incomplete resource metrics")
    publication_flags = ("stale_cleanup", "publish_artifacts", "publish_after_acquisition", "publish_corroboration")
    for flag_name in publication_flags:
        if metrics_by_name.get(flag_name) is not False:
            errors.append(f"external metrics.{flag_name} must be false")
    return errors
