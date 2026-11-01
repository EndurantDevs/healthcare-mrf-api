# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Source admission and bounded qualification for isolated acquisitions."""

import asyncio
import contextvars
import json
import posixpath
import urllib.parse
import urllib.request
from pathlib import Path

from process import provider_directory_generic_source_policy as source_policy

QUALIFICATION_API_BASE = contextvars.ContextVar("provider_directory_qualification_api_base", default=None)


class QualificationURLRejected(ValueError):
    """A qualification request would leave the selected endpoint scope."""


class _NoQualificationRedirects(urllib.request.HTTPRedirectHandler):
    """Keep qualification evidence and credentials at the admitted destination."""

    def redirect_request(self, *_args, **_kwargs):
        """Return the redirect response without contacting its destination."""
        return None


def require_qualification_url(directory, url):
    """Admit the effective path before resolving credentials or contacting a source."""
    api_base = QUALIFICATION_API_BASE.get()
    if api_base is None:
        return
    try:
        parsed = urllib.parse.urlsplit(url)
        normalized = parsed._replace(path=posixpath.normpath(urllib.parse.unquote(parsed.path))).geturl()
        is_scoped = directory._is_url_within_api_base(normalized, api_base)
    except ValueError:
        is_scoped = False
    if (
        not is_scoped
        or parsed.scheme not in {"http", "https"}
        or parsed.fragment
        or "\\" in url
        or any(ord(character) < 33 for character in url)
    ):
        raise QualificationURLRejected("provider_directory_qualification_url_outside_scope")


def open_source_request(directory, request, *, timeout):
    """Refuse qualification redirects while retaining ordinary source transport."""
    context = directory._ssl_context()
    if QUALIFICATION_API_BASE.get() is None:
        return directory.christus.open_source_request(request, timeout=timeout, context=context)
    opener = urllib.request.build_opener(urllib.request.HTTPSHandler(context=context), _NoQualificationRedirects())
    return opener.open(request, timeout=timeout)


async def resolve_probe_credentials(credential_lookup, *args, **kwargs):
    """Keep qualification credential I/O off-loop and ordinary probes unchanged."""
    if QUALIFICATION_API_BASE.get() is None:
        return credential_lookup(*args, **kwargs)
    return await asyncio.to_thread(credential_lookup, *args, **kwargs)


def open_token_request(directory, request, *, timeout):
    """Allow the configured token origin while refusing qualification redirects."""
    if QUALIFICATION_API_BASE.get() is None:
        return urllib.request.urlopen(request, timeout=timeout, context=directory._ssl_context())
    return open_source_request(directory, request, timeout=timeout)


def is_search_bundle_valid(directory, payload, resource_type):
    """Apply strict search validation while admitting informational search outcomes."""
    if not isinstance(payload, dict) or not isinstance(payload.get("entry", []), list):
        return False
    resource_entries = []
    for entry in payload.get("entry", []):
        resource = entry.get("resource") if isinstance(entry, dict) else None
        search = entry.get("search") if isinstance(entry, dict) else None
        if isinstance(resource, dict) and resource.get("resourceType") == "OperationOutcome":
            issues = resource.get("issue")
            if (
                isinstance(search, dict)
                and search.get("mode") == "outcome"
                and isinstance(issues, list)
                and issues
                and all(
                    isinstance(issue, dict) and isinstance(issue.get("code"), str) and issue["code"] for issue in issues
                )
                and not directory._has_operation_outcome_error(resource)
            ):
                continue
        resource_entries.append(entry)
    if payload.get("entry") and not resource_entries:
        return False
    return directory._current_version_census_bundle_error({**payload, "entry": resource_entries}, resource_type) is None


_ISOLATED_ACQUISITION_FLAGS = {
    "import_resources": True,
    "defer_typed_materialization": True,
    "full_refresh": True,
    "probe": False,
    "bulk_export": False,
    "stale_cleanup": False,
    "publish_artifacts": False,
    "publish_after_acquisition": False,
    "publish_corroboration": False,
}


def resource_import_selection_metrics(source_count: int) -> dict[str, int]:
    """Keep the existing selection counters stable for every acquisition path."""
    return {
        "source_import_sources_considered": source_count,
        "source_import_sources_selected": 0,
        "source_import_skipped_blocked_source": 0,
        "source_import_skipped_blocked_source_shared_backend_unverified": 0,
        "source_import_skipped_blocked_source_coverage_mode_probe_only": 0,
        "source_import_skipped_blocked_source_fully_enumerable_resources_empty": 0,
        "source_import_skipped_blocked_source_upstream_search_window_incomplete": 0,
        "source_import_skipped_missing_api_base": 0,
        "source_import_skipped_probe_not_valid": 0,
        "source_import_skipped_open_only": 0,
        "source_import_skipped_validation_status": 0,
        "source_import_skipped_auth_required_policy": 0,
        "source_import_skipped_manual_only": 0,
        "source_import_sources_selected_live_probe_valid": 0,
        "source_import_sources_selected_checkpoint_retry": 0,
        "source_import_sources_selected_declared_credentialed": 0,
        "source_import_sources_selected_auth_required_seed": 0,
    }


def _qualification_probe_timeout_seconds(directory, source_record, *, timeout):
    """Keep the qualification deadline above its synchronous request budget."""
    request_count = max(1, len(directory._candidate_metadata_urls(source_record))) * 2
    return max(
        directory._source_probe_hard_timeout_seconds(source_record, timeout=timeout),
        timeout * request_count + 2,
    )


async def _scoped_qualification_probe(directory, source_record, *, timeout, run_id):
    api_base = directory._canonical_base(source_record.get("canonical_api_base") or source_record.get("api_base"))
    if not api_base:
        raise QualificationURLRejected("provider_directory_qualification_url_outside_scope")
    token = QUALIFICATION_API_BASE.set(api_base)
    try:
        for resource_type in directory.RESOURCE_ENDPOINT_FIELDS:
            resource_url = directory._resource_start_url(source_record, resource_type, page_count=1)
            if resource_url is not None:
                require_qualification_url(directory, resource_url)
        return await asyncio.wait_for(
            directory._probe_source(source_record, timeout=timeout, run_id=run_id),
            timeout=_qualification_probe_timeout_seconds(directory, source_record, timeout=timeout),
        )
    finally:
        QUALIFICATION_API_BASE.reset(token)


async def qualify_stale_isolated_source(directory, source_rows, task, *, timeout, run_id):
    """Refresh one stale seed verdict with transport reads, without catalog probe writes."""
    if (
        not directory.is_source_storage_required()
        or getattr(source_policy._FACTORY.get(), "storage_binding", None) is None
        or any(task.get(name) is not required_value for name, required_value in _ISOLATED_ACQUISITION_FLAGS.items())
        or task.get("publish_source_dataset_only", False) is not False
        or task.get("seed_only")
        or task.get("test")
        or task.get("test_mode")
        or task.get("retry_of_run_id")
        or task.get("provider_directory_pagination_root_run_id")
        or len(source_rows) != 1
    ):
        return source_rows
    source_record = source_rows[0]
    if (
        task.get("source_ids") != [source_record["source_id"]]
        or directory._clean_text(source_record.get("last_validated_status")) != "valid_non_fhir"
        or directory.current_version_census_contract(source_record) is not None
        or directory._uses_alohr_graphql_connector(source_record)
        or directory._uses_uhc_official_file_connector(source_record)
    ):
        return source_rows
    decision = directory._resource_import_source_decision(
        source_record,
        valid_source_ids=None,
        open_only=bool(task.get("open_only", True)),
        include_auth_required=bool(task.get("include_auth_required", False)),
        checkpoint_retry_ids=set(),
        requested_resource_types=directory._selected_resources(task.get("resources")),
    )
    if decision.skip_metric != "source_import_skipped_validation_status":
        return source_rows
    directory._validate_provider_directory_endpoint_scope(source_rows, task.get("provider_directory_endpoint_scope"))
    try:
        probe, _capability = await _scoped_qualification_probe(directory, source_record, timeout=timeout, run_id=run_id)
    except TimeoutError, QualificationURLRejected:
        return source_rows
    resource_probe = probe.get("resource_probe") or {}
    expected_base = directory._canonical_base(source_record.get("canonical_api_base") or source_record.get("api_base"))
    if (
        probe.get("status") == "valid"
        and probe.get("http_status") == 200
        and probe.get("error") is None
        and directory._canonical_base(probe.get("api_base")) == expected_base
        and expected_base
        and directory._is_url_within_api_base(resource_probe.get("url") or "", expected_base)
        and resource_probe.get("status") == "reachable"
        and resource_probe.get("http_status") == 200
        and resource_probe.get("error") is None
        and resource_probe.get("search_bundle_valid") is True
    ):
        return [{**source_record, "last_validated_status": "valid"}]
    return source_rows


def candidate_metadata_urls(directory, source_record):
    """Resolve probe URLs without replacing an explicitly confirmed acquisition base."""
    census_contract = directory.current_version_census_contract(source_record)
    if census_contract is not None:
        api_base = directory._canonical_base(source_record.get("canonical_api_base") or source_record.get("api_base"))
        if api_base is None:
            return []
        return [
            (api_base, f"{api_base}/metadata?_format=json"),
            (api_base, f"{api_base}/metadata"),
        ]
    metadata_url_pairs: list[tuple[str, str]] = []
    seen_metadata_urls: set[str] = set()
    metadata = directory._source_metadata(source_record)
    for explicit_url in (
        metadata.get("provider_directory_confirmed_metadata_url"),
        metadata.get("metadata_url"),
    ):
        metadata_url = directory._clean_text(explicit_url)
        if not metadata_url or metadata_url in seen_metadata_urls:
            continue
        metadata_base = directory._provider_directory_base_from_metadata_url(metadata_url)
        api_base = directory._canonical_base(source_record.get("canonical_api_base") or source_record.get("api_base"))
        confirmed_base = directory._canonical_base(metadata.get("provider_directory_confirmed_base"))
        candidate_base = (
            api_base
            if api_base and confirmed_base == api_base
            else directory._canonical_base(metadata_base or api_base)
        )
        if not candidate_base:
            continue
        metadata_url_pairs.append((candidate_base, metadata_url))
        seen_metadata_urls.add(metadata_url)
    for api_base in directory._candidate_base_urls(source_record):
        for candidate_url in (
            f"{api_base}/metadata?_format=json",
            f"{api_base}/metadata",
        ):
            if candidate_url in seen_metadata_urls:
                continue
            metadata_url_pairs.append((api_base, candidate_url))
            seen_metadata_urls.add(candidate_url)
    return metadata_url_pairs


def configured_seed_rows():
    """Load baked public source metadata through the existing seed pipeline."""
    path = Path(__file__).resolve().parents[1] / "specs/provider_directory_supplemental_sources.json"
    rows = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
        raise ValueError("provider_directory_configured_sources_invalid")
    return rows
