# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bind ordinary FHIR requests to a frozen, explicitly opted-in Source proxy."""

import asyncio
import contextvars
import datetime
import json
import os
from copy import deepcopy
from urllib.parse import parse_qsl, urlencode, urlsplit

from process.ext.source_http_proxy import POLICY_ENV, SourceProxyConfig, SourceProxyError, SourceProxyUnavailable

BINDING_ENV = "HLTHPRT_SOURCE_HTTP_PROXY_BINDING_JSON"
COOLDOWNS_ENV = "HLTHPRT_SOURCE_HTTP_PROXY_COOLDOWNS_JSON"
_RUN = contextvars.ContextVar("provider_directory_source_proxy_run", default=None)


def current_run():
    """Return the current task's frozen Source transport scope, when present."""
    return _RUN.get()


def enter_run(directory):
    """Freeze trusted policy and return a token for restoring the prior scope."""
    scope = SourceProxyRun(directory)
    return _RUN.set(scope), scope


def exit_run(token):
    """Restore the prior task-local transport scope after success or failure."""
    _RUN.reset(token)


def frozen_credentials(source_record):
    """Return prepared credentials, or None when ordinary resolution still applies."""
    scope = current_run()
    return scope.credentials.get(source_record.get("source_id")) if scope is not None else None


def selected_run(directory, source_record):
    """Return the prepared enabled scope; refuse a configured Source without one."""
    scope = current_run()
    if scope is not None:
        return scope if scope.enabled(source_record) else None
    if os.getenv(POLICY_ENV):
        base = directory._canonical_base(source_record.get("canonical_api_base") or source_record.get("api_base"))
        if SourceProxyConfig.from_environment().use_proxy(source_record.get("source_id"), base):
            raise SourceProxyError("source_proxy_scope_required")
    return None


def require_direct_transport(directory, source_record):
    """Refuse unsupported transports instead of silently bypassing Source policy."""
    if selected_run(directory, source_record) is not None:
        raise SourceProxyError("source_proxy_transport_unsupported")


def _binding_value(raw):
    try:
        value = json.loads(raw)
    except ValueError, TypeError:
        raise SourceProxyError("source_proxy_binding_invalid") from None
    if type(value) is not dict or set(value) != {"source_id", "endpoint_scope", "policy_revision", "pool_revision"}:
        raise SourceProxyError("source_proxy_binding_invalid")
    return value


def _freeze_secrets(directory, value):
    if isinstance(value, dict):
        return {key: _freeze_secrets(directory, child) for key, child in value.items()}
    if isinstance(value, list):
        return [_freeze_secrets(directory, child) for child in value]
    if isinstance(value, str) and value.startswith(directory.SECRET_ENV_PREFIX):
        return directory._resolve_secret_text(value)
    return deepcopy(value)


def _credential_url(url, pairs):
    """Append credentials without reparsing or re-encoding opaque cursor bytes."""
    if not pairs:
        return url
    existing = parse_qsl(urlsplit(url).query, keep_blank_values=True)
    missing_by_name = {}
    for name, value in pairs.items():
        matches = [observed_value for key, observed_value in existing if key == name]
        if matches and matches != [value]:
            raise SourceProxyError("source_proxy_query_credential_conflict")
        if not matches:
            missing_by_name[name] = value
    return url + ("&" if "?" in url else "?") + urlencode(missing_by_name) if missing_by_name else url


class SourceProxyRun:
    """Keep one run's policy, credentials and quota state outside imported metadata."""

    def __init__(self, directory):
        self.directory = directory
        self.config = SourceProxyConfig.from_environment()
        self.bindings = {}
        self.credentials = {}
        self._credential_locks = {}
        self.descriptors = {}
        self._last_quota = {}
        self._audit = {}
        raw = os.environ.get(BINDING_ENV, "").strip()
        self.expected_binding = _binding_value(raw) if raw else None
        raw_cooldowns = os.environ.get(COOLDOWNS_ENV, "").strip()
        try:
            self.prior_cooldowns = json.loads(raw_cooldowns) if raw_cooldowns else None
        except ValueError:
            raise SourceProxyError("source_proxy_cooldown_invalid") from None

    def _identity(self, source_record):
        base = self.directory._canonical_base(source_record.get("canonical_api_base") or source_record.get("api_base"))
        return source_record.get("source_id"), base

    async def prepare(self, source_records, *, endpoint_scope=None):
        """Reject mixed alias policies and unsupported contracts before any fetch."""
        directory = self.directory
        if self.config.policy_revision is None:
            if self.expected_binding is not None:
                raise SourceProxyError("source_proxy_binding_changed")
            return
        enabled_sources = [
            source_record for source_record in source_records if self.config.use_proxy(*self._identity(source_record))
        ]
        if not enabled_sources:
            if self.expected_binding is not None:
                raise SourceProxyError("source_proxy_binding_changed")
            return
        if any(
            directory._uses_uhc_official_file_connector(source_record)
            or directory._uses_alohr_graphql_connector(source_record)
            for source_record in enabled_sources
        ):
            raise SourceProxyError("source_proxy_transport_unsupported")
        if endpoint_scope is not None:
            directory._validate_provider_directory_endpoint_scope(enabled_sources, endpoint_scope)
        if any(
            directory.current_version_census_contract(source_record) is not None for source_record in enabled_sources
        ):
            raise SourceProxyError("source_proxy_census_contract_forbidden")
        policies_by_group = await asyncio.to_thread(self._group_policies, source_records)
        if any(len(flags) != 1 for flags in policies_by_group.values()):
            raise SourceProxyError("source_proxy_alias_policy_mismatch")
        for source_record in enabled_sources:
            source_id, base = self._identity(source_record)
            binding = self.config.bind(source_id, base)
            binding_by_field = {
                "source_id": source_id,
                "endpoint_scope": base,
                "policy_revision": binding.policy_revision,
                "pool_revision": binding.pool_revision,
            }
            if self.expected_binding != binding_by_field:
                raise SourceProxyError("source_proxy_binding_changed")
            if self.prior_cooldowns is None:
                raise SourceProxyError("source_proxy_cooldown_binding_missing")
            binding.restore_cooldowns(self.prior_cooldowns)
            self.bindings[source_id] = binding
            if self.prior_cooldowns["cooldowns"] and base == directory.MOLINA_PROVIDER_DIRECTORY_BASE:
                self._last_quota[source_id] = (403, {"message": "Out of call volume quota."}, None, 0)
            spec = await asyncio.to_thread(directory._credential_spec_for_source, source_record)
            self.credentials[source_id] = _freeze_secrets(directory, spec)
            self._credential_locks[source_id] = asyncio.Lock()
            self._audit = {"binding": binding_by_field, "cooldowns": binding.cooldown_snapshot()}

    def _group_policies(self, source_records):
        policies_by_group = {}
        for source_record in source_records:
            key = self.directory._resource_import_group_key(source_record)
            enabled = self.config.use_proxy(*self._identity(source_record))
            policies_by_group.setdefault(key, set()).add(enabled)
            if enabled:
                self.descriptors[source_record["source_id"]] = json.loads(key[1] or "{}")
        return policies_by_group

    def attach_audit(self, metrics):
        """Retain shared nonsecret state in the caller's durable terminal audit."""
        if self._audit:
            metrics["source_http_proxy"] = self._audit

    def enabled(self, source_record):
        """Require preparation before an enabled Source can dispatch a request."""
        if self.config.policy_revision is None:
            return False
        enabled = self.config.use_proxy(*self._identity(source_record))
        if enabled and source_record.get("source_id") not in self.bindings:
            raise SourceProxyError("source_proxy_scope_unprepared")
        return enabled

    async def _origin_options(self, source_record, url):
        """Keep anonymous/static authentication cheap and token refresh off the loop."""
        source_id = source_record["source_id"]
        spec = self.credentials[source_id]
        if not spec:
            return {"headers": {}, "query_params": {}, "descriptor": None}
        if self.directory._oauth2_spec(spec):
            async with self._credential_locks[source_id]:
                options = await asyncio.to_thread(
                    self.directory._credential_request_options_for_source, source_record, url, spec=spec
                )
        else:
            options = self.directory._credential_request_options_for_source(source_record, url, spec=spec)
        if any(key.lower() == "proxy-authorization" for key in options["headers"]):
            raise SourceProxyError("source_proxy_origin_header_forbidden")
        return options

    def _unavailable(self, binding, unavailable, last_result):
        directory = self.directory
        self._audit["cooldowns"] = binding.cooldown_snapshot()
        self._audit["retry_not_before"] = unavailable.retry_not_before
        if last_result is not None and directory._is_call_volume_quota_payload(last_result[1]):
            payload = {"message": "Out of call volume quota."}
            if unavailable.retry_not_before is not None:
                deadline = datetime.datetime.fromisoformat(unavailable.retry_not_before)
                delay = max(0, (deadline - datetime.datetime.now(datetime.UTC)).total_seconds())
                payload[directory.SOURCE_RETRY_AFTER_FIELD] = str(
                    max(0, delay - directory.MOLINA_QUOTA_RESET_GRACE_SECONDS)
                )
            return 403, payload, None, last_result[3]
        return last_result or (None, None, "source_proxy_unavailable", 0)

    async def fetch(self, source_record, url, *, timeout):
        """Retry an unchanged cursor only through explicitly eligible frozen routes."""
        directory = self.directory
        source_id = source_record["source_id"]
        binding = self.bindings[source_id]
        session = directory._SOURCE_HTTP_PINNED_ANONYMOUS_SESSION.get() or directory._SOURCE_HTTP_SESSION.get()
        if session is None or session.closed:
            raise SourceProxyError("source_proxy_async_session_required")
        base = self._identity(source_record)[1]
        relative_path = urlsplit(url).path[len(urlsplit(base).path.rstrip("/")) :].lstrip("/")
        cursor_key = relative_path.split("/", 1)[0] or "metadata"
        last_result = self._last_quota.get(source_id)
        try:
            binding.select(url, cursor_key)
        except SourceProxyUnavailable as unavailable:
            return self._unavailable(binding, unavailable, last_result)
        options = await self._origin_options(source_record, url)
        request_url = _credential_url(url, options["query_params"])
        attempted_proxy_ids = set()
        total_elapsed_ms = 0
        while True:
            try:
                selection = binding.select(url, cursor_key, exclude_ids=attempted_proxy_ids)
            except SourceProxyUnavailable as unavailable:
                try:
                    binding.select(url, cursor_key)
                except SourceProxyUnavailable as pool_unavailable:
                    unavailable = pool_unavailable
                else:
                    unavailable = SourceProxyUnavailable(datetime.datetime.now(datetime.UTC).isoformat())
                return self._unavailable(binding, unavailable, last_result)
            attempted_proxy_ids.add(selection.proxy_id)
            fetch_result = await directory._fetch_json_with_source_session(
                session,
                request_url,
                timeout=timeout,
                allow_redirects=False,
                preserve_url_bytes=True,
                proxy_options=selection.request_options,
                extra_headers=options["headers"],
            )
            total_elapsed_ms += fetch_result[3]
            fetch_result = directory._source_fetch_result_with_elapsed(fetch_result, total_elapsed_ms)
            is_quota = directory._is_molina_quota_response(source_record, fetch_result[0], fetch_result[1])
            if not is_quota and fetch_result[2] != "ClientProxyConnectionError":
                return fetch_result
            last_result = fetch_result
            if is_quota:
                binding.defer(
                    selection.proxy_id,
                    directory._molina_quota_retry_not_before(source_record, fetch_result[0], fetch_result[1]),
                )
                self._audit["cooldowns"] = binding.cooldown_snapshot()
                self._last_quota[source_id] = directory._source_fetch_result_with_elapsed(fetch_result, 0)
            await directory._pace_source_request(source_record)
