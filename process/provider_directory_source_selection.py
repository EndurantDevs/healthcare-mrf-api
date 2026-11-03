# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Source admission and bounded qualification for isolated acquisitions."""

import asyncio
import contextvars
import os
import posixpath
import ssl
import urllib.parse
import urllib.request

from process import provider_directory_generic_source_policy as source_policy

QUALIFICATION_API_BASE = contextvars.ContextVar("provider_directory_qualification_api_base", default=None)
CREDENTIAL_REQUEST_LOCKS = contextvars.ContextVar("provider_directory_credential_request_locks", default=None)


def is_credentialed_rest_source(directory, source_record):
    """Keep authenticated connection reuse limited to the reviewed directory."""
    api_base = directory._canonical_base(source_record.get("canonical_api_base") or source_record.get("api_base"))
    return bool(api_base and api_base == getattr(getattr(directory, "anthem", None), "API_BASE", None))


def retry_after_limit(directory, source_record):
    """Retain the reviewed provider's full delay without changing other sources."""
    return None if is_credentialed_rest_source(directory, source_record) else 600.0


def credentialed_rest_request_allowed(directory, source_record, url):
    """Reject origin, path and encoded traversal changes before resolving credentials."""
    if not is_credentialed_rest_source(directory, source_record):
        return False
    try:
        parsed = urllib.parse.urlsplit(url)
        normalized = parsed._replace(path=posixpath.normpath(urllib.parse.unquote(parsed.path))).geturl()
        return bool(
            directory._is_url_within_api_base(normalized, directory.anthem.API_BASE)
            and "%" not in parsed.path
            and not any(segment in {".", ".."} for segment in parsed.path.split("/"))
            and not parsed.fragment
            and "\\" not in url
            and not any(ord(character) < 33 for character in url)
        )
    except ValueError:
        return False


def credentialed_rest_session(directory, source_record, url):
    """Reuse a cancellable connection without enabling credentialed page prefetch."""
    session = directory._SOURCE_HTTP_SESSION.get()
    context = getattr(getattr(session, "connector", None), "_ssl", None)
    return (
        session
        if session is not None
        and not session.closed
        and isinstance(context, ssl.SSLContext)
        and context.check_hostname
        and context.verify_mode == ssl.CERT_REQUIRED
        and credentialed_rest_request_allowed(directory, source_record, url)
        and directory._credential_spec_for_source(source_record)
        else None
    )


def require_credentialed_rest_spec(directory, source_record, spec):
    """Apply the reviewed token contract before every credential lookup path."""
    if not is_credentialed_rest_source(directory, source_record):
        return
    if spec.get("query_params"):
        raise ValueError("provider_directory_credential_query_parameters_forbidden")
    oauth = directory._oauth2_spec(spec)
    if oauth and directory._resolve_secret_text(oauth.get("token_url") or oauth.get("tokenUrl")) != (
        directory.anthem.TOKEN_URL
    ):
        raise ValueError("provider_directory_credential_token_url_unapproved")


async def credentialed_rest_options(directory, source_record, url):
    """Share token refreshes across scans while keeping token I/O off the event loop."""
    if not credentialed_rest_request_allowed(directory, source_record, url):
        raise QualificationURLRejected("provider_directory_credential_url_outside_scope")
    spec = directory._credential_spec_for_source(source_record)
    require_credentialed_rest_spec(directory, source_record, spec)
    oauth = directory._oauth2_spec(spec)
    locks = CREDENTIAL_REQUEST_LOCKS.get()
    if locks is None:
        return await asyncio.to_thread(directory._credential_request_options_for_source, source_record, url, spec=spec)
    key = directory._oauth2_cache_key(oauth)
    lock = locks.setdefault(key, asyncio.Lock())
    async with lock:
        return await asyncio.to_thread(directory._credential_request_options_for_source, source_record, url, spec=spec)


def is_anonymous_source_url(url: str) -> bool:
    """Reject URL user information before selecting the anonymous HTTP session."""
    try:
        parsed_url = urllib.parse.urlsplit(url)
    except ValueError:
        return False
    return parsed_url.username is None and parsed_url.password is None


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
    """Refuse qualification and continuation redirects before contacting another destination."""
    reviewed_base = getattr(getattr(directory, "anthem", None), "API_BASE", None)
    is_credentialed = bool(
        reviewed_base and credentialed_rest_request_allowed(directory, {"api_base": reviewed_base}, request.full_url)
    )
    context = directory._ssl_context(require_verified=True) if is_credentialed else directory._ssl_context()
    if (
        QUALIFICATION_API_BASE.get() is None
        and not directory._has_fhir_continuation_query(request.full_url)
        and not is_credentialed
    ):
        return directory.christus.open_source_request(request, timeout=timeout, context=context)
    opener = urllib.request.build_opener(urllib.request.HTTPSHandler(context=context), _NoQualificationRedirects())
    return opener.open(request, timeout=timeout)


async def resolve_probe_credentials(credential_lookup, *args, run_in_thread=False, **kwargs):
    """Keep qualification credential I/O off-loop and ordinary probes unchanged."""
    if QUALIFICATION_API_BASE.get() is None and not run_in_thread:
        return credential_lookup(*args, **kwargs)
    return await asyncio.to_thread(credential_lookup, *args, **kwargs)


def declared_probe_target(directory, source_record):
    """Share the fetchable collection and resolved base across request and deadline budgets."""
    resource_type = directory._source_metadata(source_record).get("provider_directory_probe_resource")
    if (
        not isinstance(resource_type, str)
        or resource_type not in directory.RESOURCE_ENDPOINT_FIELDS
        or directory.current_version_census_contract(source_record) is not None
    ):
        return None
    resource_url = directory._resource_start_url(source_record, resource_type, page_count=1)
    if resource_url is None:
        return None
    api_base = directory._canonical_base(source_record.get("canonical_api_base") or source_record.get("api_base"))
    api_base = api_base or directory._resource_or_metadata_parent_base(resource_url)
    return (resource_type, api_base) if api_base is not None else None


async def probe_declared_collection(directory, source_record, *, timeout, run_id):
    """Probe an explicitly configured collection when metadata is unavailable."""
    probe_target = declared_probe_target(directory, source_record)
    if probe_target is None:
        return None
    resource_type, api_base = probe_target
    timeout = directory._source_resource_timeout(
        {**source_record, "canonical_api_base": api_base}, resource_type, timeout
    )
    resource_probe = await directory._probe_resource_access(
        source_record, api_base, {}, timeout=timeout, resource_type=resource_type
    )
    if resource_probe is None:
        return None
    status = resource_probe["status"]
    error = resource_probe.get("error")
    if status == "reachable":
        status = "valid" if resource_probe.get("search_bundle_valid") is True else "resource_error"
        if status != "valid":
            error = "resource search did not return a valid search Bundle"
    credential = await resolve_probe_credentials(
        directory._credential_request_options_for_source,
        source_record,
        resource_probe["url"],
        run_in_thread=is_credentialed_rest_source(directory, source_record),
    )
    return {
        **resource_probe,
        "status": status,
        "error": error,
        "api_base": api_base,
        "run_id": run_id,
        "credential": credential["descriptor"],
        "resource_probe": resource_probe,
    }, None


def open_token_request(directory, request, *, timeout):
    """Allow the configured token origin while refusing qualification redirects."""
    if request.full_url == getattr(getattr(directory, "anthem", None), "TOKEN_URL", None):
        opener = urllib.request.build_opener(
            urllib.request.HTTPSHandler(context=directory._ssl_context(require_verified=True)),
            _NoQualificationRedirects(),
        )
        return opener.open(request, timeout=timeout)
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


def source_probe_hard_timeout_seconds(directory, source_record, *, timeout):
    """Bound the shared probe above its request budget unless explicitly overridden."""
    if directory._uses_uhc_official_file_connector(source_record):
        return max(timeout + 1, 35)
    request_count = max(1, len(directory._candidate_metadata_urls(source_record))) * 2
    probe_target = declared_probe_target(directory, source_record)
    if probe_target is not None:
        resource_type, api_base = probe_target
        timeout = directory._source_resource_timeout(
            {**source_record, "canonical_api_base": api_base}, resource_type, timeout
        )
        request_count = 1
    env_value = os.getenv("HLTHPRT_PROVIDER_DIRECTORY_SOURCE_PROBE_HARD_TIMEOUT")
    if env_value:
        try:
            return max(1, int(env_value))
        except ValueError:
            return max(timeout + 1, (timeout * request_count) + 2)
    return max(timeout + 1, (timeout * request_count) + 2)


def _qualification_probe_timeout_seconds(directory, source_record, *, timeout):
    """Keep the qualification deadline above its synchronous request budget."""
    target = declared_probe_target(directory, source_record)
    request_count = max(1, len(directory._candidate_metadata_urls(source_record))) * 2
    if target is not None:
        resource_type, api_base = target
        timeout = directory._source_resource_timeout(
            {**source_record, "canonical_api_base": api_base}, resource_type, timeout
        )
        request_count = 1
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


async def fetch_unpaced_source_json(directory, source_record, url, *, timeout):
    """Fetch through the admitted source transport without anonymous downgrade."""
    directory.source_selection.require_qualification_url(directory, url)
    proxy_run = directory.source_proxy.selected_run(directory, source_record)
    if proxy_run is not None:
        return await proxy_run.fetch(source_record, url, timeout=timeout)
    census_contract = directory.current_version_census_contract(source_record)
    if census_contract is not None:
        if directory._has_source_declared_credentialed_access(source_record) or directory._credential_spec_for_source(
            source_record
        ):
            raise RuntimeError("provider_directory_current_version_census_credentials_forbidden")
        census_session = directory._SOURCE_HTTP_SESSION.get()
        if census_session is None or census_session.closed:
            raise RuntimeError("provider_directory_current_version_census_session_required")
        return await directory._fetch_json_with_source_session(
            census_session,
            url,
            timeout=timeout,
            allow_redirects=False,
            preserve_url_bytes=True,
        )
    if directory.capped_partition.enabled(directory, source_record):
        capped_session = directory._SOURCE_HTTP_SESSION.get()
        if capped_session is None or capped_session.closed:
            raise RuntimeError("provider_directory_capped_search_session_required")
        if directory._has_source_declared_credentialed_access(source_record) or directory._credential_spec_for_source(
            source_record
        ):
            raise RuntimeError("provider_directory_capped_search_anonymous_required")
        return await directory._fetch_json_with_source_session(
            capped_session,
            url,
            timeout=timeout,
            allow_redirects=False,
            preserve_url_bytes=True,
        )
    if directory.source_selection.is_credentialed_rest_source(directory, source_record):
        return await fetch_credentialed_rest_json(directory, source_record, url, timeout=timeout)
    pinned_session = directory._SOURCE_HTTP_PINNED_ANONYMOUS_SESSION.get()
    if pinned_session is not None:
        return await directory._fetch_json_with_source_session(
            pinned_session,
            url,
            timeout=timeout,
            allow_redirects=True,
        )
    options = await directory.source_selection.resolve_probe_credentials(
        directory._credential_request_options_for_source, source_record, url
    )
    if not options["headers"] and not options["query_params"]:
        return await directory._fetch_json(url, timeout=timeout)
    return await directory._fetch_json_with_options(
        url,
        timeout=timeout,
        extra_headers=options["headers"],
        query_params_by_name=options["query_params"],
    )


async def fetch_credentialed_rest_json(directory, source_record, url, *, timeout):
    """Keep missing credentials and unverified sessions out of anonymous transport."""
    options = await directory.source_selection.credentialed_rest_options(directory, source_record, url)
    if not any(name.lower() == "authorization" and header_value for name, header_value in options["headers"].items()):
        return None, None, "provider_directory_credentials_unavailable", 0
    session = directory.source_selection.credentialed_rest_session(directory, source_record, url)
    if session is not None:
        return await directory._fetch_json_with_source_session(
            session,
            url,
            timeout=timeout,
            allow_redirects=False,
            preserve_url_bytes=True,
            extra_headers=options["headers"],
            allow_gzip=True,
        )
    return await directory._fetch_json_with_options(
        url, timeout=timeout, extra_headers=options["headers"], allow_gzip=True
    )


async def fetch_source_candidate(directory, source_record, candidate_url, *, timeout, is_last_candidate):
    """Fetch one page-size candidate and report whether to try a smaller page."""
    if directory.current_version_census_contract(source_record) is not None:
        fetch_result, retry_count = await directory._fetch_current_version_census_json(
            source_record,
            candidate_url,
            timeout=timeout,
        )
        return fetch_result, False, retry_count
    empty_collection_retry_delays = directory._cigna_empty_collection_retry_delays(source_record, candidate_url)
    attempt_count = max(directory._source_fetch_retry_attempts(), len(empty_collection_retry_delays) + 1)
    total_elapsed_ms = 0
    retry_count = 0
    fetch_result = (None, None, "request_not_attempted", 0)
    for attempt_index in range(attempt_count):
        (
            fetch_result,
            should_try_smaller_page,
            should_retry_fetch,
            should_retry_empty_collection,
        ) = await directory._fetch_source_json_attempt(
            source_record,
            candidate_url,
            timeout=timeout,
            empty_collection_retry_delays=empty_collection_retry_delays,
        )
        status_code, fhir_payload, fetch_error, elapsed_ms = fetch_result
        total_elapsed_ms += elapsed_ms
        if should_try_smaller_page and not is_last_candidate:
            return directory._source_fetch_result_with_elapsed(fetch_result, total_elapsed_ms), True, retry_count
        if not should_retry_fetch or attempt_index + 1 >= attempt_count:
            break
        retry_count += 1
        await directory.asyncio.sleep(
            directory._source_fetch_attempt_delay(
                status_code,
                fhir_payload,
                attempt_index,
                empty_collection_retry_delays,
                should_retry_empty_collection,
                max_delay_seconds=retry_after_limit(directory, source_record),
            )
        )
    return directory._source_fetch_result_with_elapsed(fetch_result, total_elapsed_ms), False, retry_count
