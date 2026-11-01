# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Source admission and bounded qualification for isolated acquisitions."""

import asyncio
import contextvars
import posixpath
import urllib.parse
import urllib.request

from process import provider_directory_generic_source_policy as source_policy

QUALIFICATION_API_BASE = contextvars.ContextVar("provider_directory_qualification_api_base", default=None)


def capital_blue_cross_product_seed_rows(directory):
    """Keep the official public product contracts separate from the legacy candidate."""
    seed_rows = []
    for product, path in (("Medicare", "public-ma"), ("CHIP", "public-chip")):
        base = f"https://api.capitalbluecrossfhir.com/r4/{path}"
        seed_rows.append(
            {
                "id": f"capital-blue-cross-{product.lower()}-provider-directory",
                "org_name": "Capital Blue Cross",
                "plan_name": f"Capital Blue Cross {product} Provider Directory",
                "api_base": base,
                "auth_type": "none",
                "requires_registration": False,
                "fhir_version": "4.0.1",
                "source": "capital-blue-cross-public-directory",
                "source_detail": "official public product-specific Provider Directory FHIR API",
                "source_url": "https://www.capbluecross.com/wps/portal/cap/about/developer",
                "metadata_json": {
                    "provider_directory_confirmed_base": base,
                    "provider_directory_confirmed_metadata_url": f"{base}/metadata",
                    "provider_directory_confirmed_catalog_url": (
                        "https://www.capbluecross.com/wps/portal/cap/about/developer"
                    ),
                    "provider_directory_supported_resources": list(directory.DEFAULT_RESOURCES),
                    "provider_directory_fully_enumerable_resources": list(directory.DEFAULT_RESOURCES),
                    "provider_directory_expected_nonempty_resources": list(directory.PUBLIC_DIRECTORY_SEVEN_RESOURCES),
                    "provider_directory_resource_page_count_caps": {
                        resource: 100 for resource in directory.DEFAULT_RESOURCES
                    },
                    "provider_directory_coverage_mode": "full",
                    "provider_directory_acquisition_enabled": True,
                    "provider_directory_pagination_same_collection": True,
                    "provider_directory_bulk_export_auto_enabled": False,
                    "provider_directory_bulk_export_eligible": False,
                },
            }
        )
    return seed_rows


def pagination_host_key(directory, url_or_host: str | None) -> str | None:
    """Normalize only verified HTTPS origins for source continuation policy."""
    text = directory._clean_text(url_or_host)
    if not text:
        return None
    parsed = urllib.parse.urlsplit(text if "://" in text else f"https://{text}")
    if parsed.scheme.lower() != "https" or not parsed.hostname:
        return None
    try:
        port = parsed.port or 443
    except ValueError:
        return None
    return f"{parsed.hostname.lower()}:{port}"


def pagination_allowed_hosts(
    directory,
    source_record: dict,
    current_url: str,
) -> set[str]:
    """Keep configured host allowances within verified HTTPS origins."""
    api_base = directory._canonical_base(source_record.get("canonical_api_base") or source_record.get("api_base"))
    allowed_hosts = {
        host_key
        for candidate_url in (api_base, current_url)
        if (host_key := directory._pagination_host_key(candidate_url))
    }
    configured_hosts = directory._string_list(
        directory._source_metadata(source_record).get("provider_directory_pagination_allowed_hosts")
    )
    for configured_host in configured_hosts:
        if host_key := directory._pagination_host_key(configured_host):
            allowed_hosts.add(host_key)
    return allowed_hosts


def is_trusted_fhir_pagination_url(
    directory,
    source_record: dict,
    current_url: str,
    next_url: str,
) -> bool:
    """Retain the existing host policy and optional exact collection boundary."""
    parsed_next = urllib.parse.urlsplit(next_url)
    next_host_key = directory._pagination_host_key(next_url)
    return bool(
        next_host_key
        and next_host_key in directory._pagination_allowed_hosts(source_record, current_url)
        and parsed_next.username is None
        and parsed_next.password is None
        and not parsed_next.fragment
        and (
            directory._source_metadata(source_record).get("provider_directory_pagination_same_collection") is not True
            or parsed_next.path == urllib.parse.urlsplit(current_url).path
        )
    )


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
