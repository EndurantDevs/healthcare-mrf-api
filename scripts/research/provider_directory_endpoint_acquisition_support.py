"""HTTP and external-run validation support for the endpoint acquisition harness."""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.parse
import urllib.request
from typing import Any


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
