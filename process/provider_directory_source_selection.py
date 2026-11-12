# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Source admission and bounded qualification for isolated acquisitions."""

import asyncio
import contextlib
import contextvars
import json
import os
import posixpath
import ssl
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

from process import provider_directory_generic_source_policy as source_policy
from process import vsp_directory_integration as vsp_directory

QUALIFICATION_API_BASE = contextvars.ContextVar("provider_directory_qualification_api_base", default=None)
CREDENTIAL_REQUEST_LOCKS = contextvars.ContextVar("provider_directory_credential_request_locks", default=None)
CENSUS_REQUEST_DEFERRED_ERROR = "source_request_cooldown"
FHIR_CONTINUATION_QUERY_NAMES = frozenset(
    {
        "_continuationtoken",
        "_cursor",
        "_getpages",
        "_getpagesid",
        "_getpagesoffset",
        "_offset",
        "_page",
        "_page_token",
        "_searchid",
        "_skip",
        "cursor",
        "cursormark",
        "ct",
        "nexttoken",
        "page",
        "pagetoken",
        "starting_after",
    }
)


class CensusRequestDeferred(Exception):
    """A queued census request must retain its shared cooldown without sending."""


def is_credentialed_rest_source(directory, source_record):
    """Keep authenticated connection reuse limited to the reviewed directory."""
    api_base = directory._canonical_base(source_record.get("canonical_api_base") or source_record.get("api_base"))
    return bool(api_base and api_base == getattr(getattr(directory, "anthem", None), "API_BASE", None))


def retry_after_limit(directory, source_record):
    """Retain full server delays for reviewed transport and explicit census contracts."""
    return (
        None
        if is_credentialed_rest_source(directory, source_record) or uses_census_retry_policy(directory, source_record)
        else 600.0
    )


def uses_census_retry_policy(directory, source_record):
    """Identify explicit census contracts independently of legacy transport policy."""
    if directory._source_metadata(source_record).get("provider_directory_opaque_cursor_census") is True:
        return True
    return any(
        isinstance(settings_by_field, dict) and settings_by_field.get("census_mode") == directory.leaf_census.CONTRACT
        for settings_by_field in directory._last_updated_partition_resource_configs(source_record).values()
    )


def should_defer_census_retry(directory, source_record, response_by_field):
    """Let durable census callers retain server and shared cooldowns without sleeping."""
    if not uses_census_retry_policy(directory, source_record):
        return False
    return (
        directory._source_retry_after_seconds(response_by_field, max_delay_seconds=None) or 0
    ) > 0 or has_census_cooldown(directory, source_record)


def has_census_cooldown(directory, source_record):
    """Leave explicit census cooldowns to durable checkpoints, including queued sends."""
    if directory.current_version_census_contract(source_record) is not None or not uses_census_retry_policy(
        directory, source_record
    ):
        return False
    pacing_by_field = source_record.get("_provider_directory_request_pacing_state", source_record)
    return pacing_by_field.get("retry_after_monotonic", 0) > directory.time.monotonic()


def is_pagination_cooldown_failure(directory, status_code, fetch_error, *, source_record=None):
    """Classify cooldown failures under the reviewed source transport policy."""
    if status_code in {423, 429}:
        return True
    if (
        status_code == 500
        and fetch_error is None
        and source_record is not None
        and is_credentialed_rest_source(directory, source_record)
    ):
        return True
    if not fetch_error:
        return False
    normalized_error = fetch_error.lower()
    return "timeout" in normalized_error or "timed out" in normalized_error


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


def _uses_oauth_authorization(directory, spec):
    """Leave static bearer and explicit Authorization overrides on their existing path."""
    api_key_header = directory._api_key_header(spec)
    return bool(
        directory._oauth2_spec(spec)
        and not directory._resolve_secret_text(spec.get("bearer_token"))
        and not (api_key_header is not None and api_key_header[0].lower() == "authorization")
        and not any(
            str(name).lower() == "authorization" and directory._resolve_secret_text(value)
            for name, value in directory._mapping(spec.get("headers")).items()
        )
    )


async def _credentialed_oauth_token(directory, source_record, url, oauth, timeout):
    """Read a bounded cancellable token response using the existing request and cache contract."""
    request = directory._oauth2_token_request(oauth)
    if request is None:
        return None, 0
    key, now = directory._oauth2_cache_key(oauth), directory.time.time()
    cached = directory._OAUTH_TOKEN_CACHE.get(key)
    if cached and cached[1] > now:
        return cached
    session = credentialed_rest_session(directory, source_record, url)
    try:
        owner = (
            contextlib.nullcontext(session)
            if session is not None
            else directory._source_http_client_session(require_verified_tls=True)
        )
        async with owner as client:
            async with client.post(
                request.full_url,
                data=request.data,
                headers=dict(request.header_items()),
                timeout=directory.aiohttp.ClientTimeout(total=min(15, timeout)),
                allow_redirects=False,
            ) as response:
                if not 200 <= response.status < 300:
                    return None, 0
                body = await directory._read_source_http_response_body(response, timeout=timeout, max_bytes=1024 * 1024)
                payload_by_field = directory._decode_json_body(body)
    except Exception:
        return None, 0
    access_token = directory._clean_text((payload_by_field or {}).get("access_token"))
    if not access_token:
        return None, 0
    try:
        expires_in = int(payload_by_field.get("expires_in") or 3600)
    except Exception:
        expires_in = 3600
    expires_at = now + max(0, expires_in)
    if expires_at <= directory.time.time():
        return None, 0
    directory._OAUTH_TOKEN_CACHE[key] = (
        access_token,
        now + max(0, expires_in - directory.OAUTH_TOKEN_EXPIRY_SKEW_SECONDS),
    )
    return access_token, expires_at


async def credentialed_rest_options(directory, source_record, url, *, rejected_authorization=None, timeout=15):
    """Serialize token renewal and drain its cancellable HTTP cleanup under the credential lock."""
    if not credentialed_rest_request_allowed(directory, source_record, url):
        raise QualificationURLRejected("provider_directory_credential_url_outside_scope")
    require_qualification_url(directory, url)
    spec = directory._credential_spec_for_source(source_record)
    require_credentialed_rest_spec(directory, source_record, spec)
    oauth = directory._oauth2_spec(spec)

    async def resolve():
        """Discard only the rejected OAuth token before resolving authorized request options."""
        uses_oauth = _uses_oauth_authorization(directory, spec)
        if rejected_authorization is not None:
            if not uses_oauth:
                return None
            key = directory._oauth2_cache_key(oauth)
            cached = directory._OAUTH_TOKEN_CACHE.get(key)
            if cached is not None and rejected_authorization == f"Bearer {cached[0]}":
                directory._OAUTH_TOKEN_CACHE.pop(key)
        resolved_spec_by_field = {**spec, "oauth2": None}
        if uses_oauth:
            try:
                async with asyncio.TaskGroup() as requests:
                    token_request = requests.create_task(
                        _credentialed_oauth_token(directory, source_record, url, oauth, timeout)
                    )
            except ExceptionGroup as failure:
                raise failure.exceptions[0] from None
            token, expires_at = token_request.result()
            resolved_spec_by_field["bearer_token"] = token
        options = directory._credential_request_options_for_source(source_record, url, spec=resolved_spec_by_field)
        if uses_oauth and token:
            options["_oauth2_expiry"] = (directory._oauth2_cache_key(oauth), expires_at)
        return options

    locks = CREDENTIAL_REQUEST_LOCKS.get()
    if locks is None:
        return await resolve()
    key = directory._oauth2_cache_key(oauth)
    lock = locks.setdefault(key, asyncio.Lock())
    async with lock:
        return await resolve()


def has_current_credentialed_rest_options(directory, options):
    """Recheck token identity and expiry after pacing without token I/O under request permits."""
    expiry = options.get("_oauth2_expiry")
    if expiry is None:
        return True
    key, expires_at = expiry
    cached = directory._OAUTH_TOKEN_CACHE.get(key)
    return bool(
        cached
        and options["headers"].get("Authorization") == f"Bearer {cached[0]}"
        and expires_at > directory.time.time()
    )


def is_anonymous_source_url(url: str) -> bool:
    """Reject URL user information before selecting the anonymous HTTP session."""
    try:
        parsed_url = urllib.parse.urlsplit(url)
    except ValueError:
        return False
    return parsed_url.username is None and parsed_url.password is None


def requires_verified_session(directory, source_record):
    """Keep strict source transports independent of the ordinary TLS setting."""
    return bool(
        vsp_directory.is_source(source_record)
        or directory.current_version_census_contract(source_record) is not None
        or directory.capped_partition.enabled(directory, source_record)
        or uses_census_retry_policy(directory, source_record)
        or is_credentialed_rest_source(directory, source_record)
        or directory._source_metadata(source_record).get("provider_directory_pagination_same_collection") is True
    )


@contextlib.asynccontextmanager
async def verified_session_scope(directory, source_records, *, enabled=True, request_concurrency=100):
    """Open the existing verified session only for a strict source group."""
    if not enabled or not any(requires_verified_session(directory, record) for record in source_records):
        yield
        return
    slot_map = directory.SOURCE_HTTP_REQUEST_SLOTS.get()
    session = directory._SOURCE_HTTP_SESSION.get()
    if slot_map is not None and session is not None:
        slot_map.setdefault("total", asyncio.Semaphore(max(1, session.connector.limit)))
    async with directory._source_http_session_scope(
        force=True, require_verified_tls=True, request_concurrency=request_concurrency
    ):
        token = directory.SOURCE_HTTP_REQUEST_SLOTS.set(slot_map) if slot_map is not None else None
        try:
            yield
        finally:
            if token is not None:
                directory.SOURCE_HTTP_REQUEST_SLOTS.reset(token)


async def probe_sources_with_session(directory, source_records, *, census_contract, proxy_run, **options):
    """Keep one progress batch while isolating strict source transports."""
    strict_sources = [requires_verified_session(directory, record) for record in source_records]
    is_verified = census_contract is not None or all(strict_sources)
    async with directory._source_http_session_scope(
        force=is_verified or any(strict_sources) or bool(proxy_run.bindings), require_verified_tls=is_verified
    ):
        return await directory._run_source_probe_batch(source_records, **options)


async def probe_source(directory, source_record, *, timeout, run_id):
    """Qualify the selected source over its declared transport."""
    if vsp_directory.is_source(source_record):
        return await vsp_directory.probe(directory, source_record, timeout=timeout, run_id=run_id)
    if directory._uses_uhc_official_file_connector(source_record):
        return await directory._probe_uhc_official_file_connector(
            source_record,
            timeout=timeout,
            run_id=run_id,
        )
    if directory._uses_alohr_graphql_connector(source_record):
        return await directory._probe_alohr_graphql_connector(source_record, timeout=timeout, run_id=run_id)
    metadata_urls = directory._candidate_metadata_urls(source_record)
    if not metadata_urls:
        source_probe_map = {
            "status": "no_api",
            "http_status": None,
            "response_time_ms": 0,
            "url": None,
            "error": "missing api_base",
            "run_id": run_id,
        }
        return source_probe_map, None
    missing_credential_result = await resolve_probe_credentials(
        directory._credential_probe_short_circuit,
        source_record,
        metadata_urls,
        run_id=run_id,
        run_in_thread=is_credentialed_rest_source(directory, source_record),
    )
    if missing_credential_result:
        return missing_credential_result
    collection_probe = await probe_declared_collection(directory, source_record, timeout=timeout, run_id=run_id)
    if collection_probe is not None:
        return collection_probe
    best_probe: dict[str, Any] | None = None
    best_payload: dict[str, Any] | None = None
    for candidate_base, metadata_url in metadata_urls:
        candidate_probe_map, capability_payload = await directory._probe_metadata_candidate(
            source_record,
            candidate_base,
            metadata_url,
            timeout=timeout,
            run_id=run_id,
        )
        if candidate_probe_map["status"] == "valid":
            return candidate_probe_map, capability_payload
        if best_probe is None or candidate_probe_map.get("http_status") == 200:
            best_probe = candidate_probe_map
            best_payload = capability_payload
    assert best_probe is not None
    return best_probe, best_payload


async def probe_with_deadline(directory, source_record, *, isolate_tls, timeout, run_id, hard_timeout):
    """Keep the existing probe timeout inside its source's transport scope."""
    async with verified_session_scope(directory, [source_record], enabled=isolate_tls):
        return await asyncio.wait_for(
            directory._probe_source(source_record, timeout=timeout, run_id=run_id), timeout=hard_timeout
        )


def probe_flush_every():
    """Retain bounded capability and source update batches."""
    try:
        return max(1, int(os.getenv("HLTHPRT_PROVIDER_DIRECTORY_PROBE_FLUSH_EVERY", "50")))
    except ValueError:
        return 50


def resource_endpoint_signature(directory, source):
    """Keep ordinary endpoint identities stable and strict aliases in separate scans."""
    signature_items = tuple(
        (field, directory._clean_text(source.get(field)) or "")
        for field in sorted(directory.RESOURCE_ENDPOINT_FIELDS.values())
    )
    connector_contract = (
        vsp_directory.endpoint_contract()
        if vsp_directory.is_source(source)
        else directory._alohr_graphql_acquisition_contract(source)
    )
    if connector_contract is not None:
        signature_items += (
            (
                "connector_acquisition_contract",
                directory._stable_identity_json(connector_contract),
            ),
        )
    census_contract = directory.current_version_census_contract(source)
    if census_contract is not None:
        signature_items += (
            (
                "current_version_census_identity",
                directory._stable_identity_json(census_contract.identity()),
            ),
        )
    if directory._source_metadata(source).get("provider_directory_pagination_same_collection") is True:
        signature_items += (("provider_directory_pagination_same_collection", "true"),)
    return signature_items


def capital_blue_cross_product_seed_rows(directory):
    """Keep the official public product contracts separate from the legacy candidate."""
    seed_rows = []
    for product, path in (("Medicare", "public-ma"), ("CHIP", "public-chip")):
        base = f"https://api.capitalbluecrossfhir.com/r4/{path}"
        priority_resources = ["Location", "HealthcareService", "PractitionerRole", "OrganizationAffiliation"]
        if product == "CHIP":
            priority_resources = ["HealthcareService", "Location", "OrganizationAffiliation", "PractitionerRole"]
        priority_resources += ["Practitioner", "Organization", "InsurancePlan", "Endpoint"]
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
                    "provider_directory_resource_fetch_order": priority_resources,
                    "provider_directory_bulk_export_auto_enabled": False,
                    "provider_directory_bulk_export_eligible": False,
                },
            }
        )
    return seed_rows


async def fetch_same_collection(directory, source_record, url, *, timeout):
    """Keep opted-in anonymous requests byte-exact in an existing verified session."""
    session = directory._SOURCE_HTTP_PINNED_ANONYMOUS_SESSION.get()
    if session is None:
        session = directory._rest_page_prefetch_session(source_record, url)
    if session is None:
        raise RuntimeError("provider_directory_pagination_anonymous_session_required")
    return await directory._fetch_json_with_source_session(
        session, url, timeout=timeout, allow_redirects=False, preserve_url_bytes=True
    )


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
            not uses_census_retry_policy(directory, source_record)
            or census_pagination_url_allowed(directory, source_record, next_url)
        )
        and (
            directory._source_metadata(source_record).get("provider_directory_pagination_same_collection") is not True
            or parsed_next.path == urllib.parse.urlsplit(current_url).path
        )
    )


def census_pagination_url_allowed(directory, source_record, next_url):
    """Limit explicit directory census cursors to their exact public API and resource paths."""
    api_base = directory._canonical_base(source_record.get("canonical_api_base") or source_record.get("api_base"))
    if not api_base:
        return False
    resources = directory._source_supported_resource_types(source_record)
    return fhir_census_pagination_url_allowed(
        api_base=api_base,
        resource_types=directory.DEFAULT_RESOURCES if resources is None else resources,
        next_url=next_url,
    )


def has_fhir_continuation_query(url: str) -> bool:
    """Recognize existing FHIR cursor names without rewriting opaque query values."""
    query_names = {
        name.lower()
        for name, _value in urllib.parse.parse_qsl(urllib.parse.urlsplit(url).query, keep_blank_values=True)
    }
    return bool(FHIR_CONTINUATION_QUERY_NAMES.intersection(query_names))


def fhir_census_pagination_url_allowed(*, api_base, resource_types, next_url):
    """Admit exact resource paths or a source-issued cursor at the API base."""
    try:
        parsed_next = urllib.parse.urlsplit(next_url)
        parsed_base = urllib.parse.urlsplit(api_base)
        base_path = parsed_base.path.rstrip("/")
        allowed_paths = {base_path, base_path + "/", *(f"{base_path}/{resource}" for resource in resource_types)}
        return bool(
            parsed_base.scheme == "https"
            and parsed_base.hostname
            and parsed_next.scheme == "https"
            and parsed_next.hostname == parsed_base.hostname
            and (parsed_next.port if parsed_next.port is not None else 443)
            == (parsed_base.port if parsed_base.port is not None else 443)
            and parsed_next.username is None
            and parsed_next.password is None
            and not parsed_next.fragment
            and parsed_next.path in allowed_paths
            and (parsed_next.path not in {base_path, base_path + "/"} or has_fhir_continuation_query(next_url))
            and "%" not in parsed_next.path
            and "\\" not in next_url
            and not any(ord(character) < 33 for character in next_url)
        )
    except ValueError:
        return False


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
        async with verified_session_scope(directory, [source_record]):
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
        or vsp_directory.is_source(source_record)
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
    if directory._source_metadata(source_record).get("provider_directory_pagination_same_collection") is True:
        return await fetch_same_collection(directory, source_record, url, timeout=timeout)
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


def uses_credentialed_rest_transport(directory, source_record):
    """Keep credentialed admission behind existing proxy and anonymous transport contracts."""
    return bool(
        is_credentialed_rest_source(directory, source_record)
        and directory.source_proxy.selected_run(directory, source_record) is None
        and directory.current_version_census_contract(source_record) is None
        and not directory.capped_partition.enabled(directory, source_record)
        and directory._source_metadata(source_record).get("provider_directory_pagination_same_collection") is not True
    )


async def send_authenticated_page(directory, source_record, url, *, timeout, headers_by_name):
    """Send one authorized page with the existing verified transport and exact URL bytes."""
    session = directory.source_selection.credentialed_rest_session(directory, source_record, url)
    if session is not None:
        return await directory._fetch_json_with_source_session(
            session,
            url,
            timeout=timeout,
            allow_redirects=False,
            preserve_url_bytes=True,
            extra_headers=headers_by_name,
            allow_gzip=True,
        )
    return await directory._fetch_json_with_options(
        url, timeout=timeout, extra_headers=headers_by_name, allow_gzip=True
    )


async def fetch_credentialed_rest_json(directory, source_record, url, *, timeout, send=None):
    """Keep missing credentials and unverified sessions out of anonymous transport."""
    options = await directory.source_selection.credentialed_rest_options(directory, source_record, url, timeout=timeout)
    if not any(name.lower() == "authorization" and header_value for name, header_value in options["headers"].items()):
        return None, None, "provider_directory_credentials_unavailable", 0
    total_elapsed_ms = 0
    has_retried = False
    while True:
        if has_retried and send is None:
            try:
                await directory._pace_source_request(
                    source_record,
                    allow_census_defer=True,
                    ready=lambda: has_current_credentialed_rest_options(directory, options),
                )
            except CensusRequestDeferred:
                return None, None, CENSUS_REQUEST_DEFERRED_ERROR, total_elapsed_ms
        fetch_result = None
        if send is not None or has_current_credentialed_rest_options(directory, options):
            fetch_result = (
                await send(options)
                if send is not None
                else await send_authenticated_page(
                    directory, source_record, url, timeout=timeout, headers_by_name=options["headers"]
                )
            )
        if fetch_result is None:
            options = await directory.source_selection.credentialed_rest_options(
                directory, source_record, url, timeout=timeout
            )
            if not any(
                name.lower() == "authorization" and header_value for name, header_value in options["headers"].items()
            ):
                return None, None, "provider_directory_credentials_unavailable", total_elapsed_ms
            continue
        total_elapsed_ms += fetch_result[3]
        authorizations = [
            header_value
            for name, header_value in options["headers"].items()
            if name.lower() == "authorization" and header_value
        ]
        if fetch_result[0] != 401 or has_retried or len(authorizations) != 1:
            return directory._source_fetch_result_with_elapsed(fetch_result, total_elapsed_ms)
        options = await directory.source_selection.credentialed_rest_options(
            directory, source_record, url, rejected_authorization=authorizations[0], timeout=timeout
        )
        if not options or not any(
            name.lower() == "authorization" and header_value for name, header_value in options["headers"].items()
        ):
            return directory._source_fetch_result_with_elapsed(fetch_result, total_elapsed_ms)
        has_retried = True


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
        if fetch_error == CENSUS_REQUEST_DEFERRED_ERROR or should_defer_census_retry(
            directory, source_record, fhir_payload
        ):
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


def configured_seed_rows():
    """Load baked public source metadata through the existing seed pipeline."""
    path = Path(__file__).resolve().parents[1] / "specs/provider_directory_supplemental_sources.json"
    rows = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
        raise ValueError("provider_directory_configured_sources_invalid")
    return rows


def read_retest_results(
    path: Path,
) -> tuple[Any, list[Any]]:
    """Read and validate the outer shape of a retest report."""
    try:
        retest_document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"Provider Directory retest results could not be read from {path}: {exc}") from exc
    retest_results = retest_document.get("results") if isinstance(retest_document, dict) else retest_document
    if not isinstance(retest_results, list):
        raise RuntimeError(f"Provider Directory retest results at {path} must be a JSON object/list with results.")
    return retest_document, retest_results
