# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Acquire retained Guardian finder searches and explicit provider-at-practice details."""

import asyncio
import os
import re
import sqlite3
import uuid
from contextlib import asynccontextmanager, closing
from pathlib import Path
from tempfile import gettempdir

import aiohttp

from process.delta_dental_directory_http import _json_body, _remaining_cooldown, _retry_boundary, _selection
from process.delta_dental_directory_worker import FinderAdmission
from process.ext.source_http_proxy import SourceProxyError, SourceProxyUnavailable
from process.guardian_directory_store import GuardianCapture
from process.provider_directory_generic_source_policy import source_transaction as candidate_write_scope
from process.ptg_parts.artifacts import PTG2ArtifactStore
from process.vsp_directory_contract import canonical_json
from process.vsp_directory_http import strict_json
from process.vsp_directory_store import _directory

ORG_NAME = "Guardian"
PREFIX = "guardian_directory"
URN = "guardian"
COVERAGE_MODE = "first_party_employer_dental_windows"
SOURCE_ID = "guardian-dental-directory"
ENTRY_ID = "guardian-dental-provider-directory"
ADAPTER_ID = "guardian-dental-directory-json-v1"
TRANSPORT = "guardian_dental_directory_json"
ENDPOINT_SCOPE = "https://www.guardiananytime.com/fapapp"
RESOURCE_TYPES = ("Location", "Organization", "OrganizationAffiliation", "Practitioner", "PractitionerRole")
_ENDPOINTS = {
    "search": ENDPOINT_SCOPE + "/providersearch",
    "detail": ENDPOINT_SCOPE + "/providerdetails",
    "specialty": ENDPOINT_SCOPE + "/speciality",
}
_MANIFEST = Path("/var/run/provider-directory/guardian-dental/plan.json")


def _artifact_root():
    configured = Path(os.environ.get("HLTHPRT_PROVIDER_DIRECTORY_ARTIFACT_ROOT", ""))
    path = configured.resolve()
    if not configured.is_absolute() or path == Path("/") or path.is_relative_to(Path(gettempdir()).resolve()):
        raise ValueError("guardian_directory_artifact_root_invalid")
    return _directory(path / SOURCE_ID)


def load_search_plan():
    """Read the configured employer dental geography and limits."""
    from process.guardian_directory_sweep import load_plan

    return load_plan(_MANIFEST, _artifact_root())


async def _read_response(response, binding, route, admission, capture, kind, key):
    if response.status in {401, 403, 429, 503}:
        is_transient = response.status in {429, 503}
        boundary = _retry_boundary(response.headers.get("Retry-After")) if is_transient else None
        admission.record_hold(boundary, is_transient)
        binding.defer(route.proxy_id, boundary)
        if response.status == 429 and boundary is not None:
            capture.release_limited(kind, key)
        raise SourceProxyUnavailable(boundary)
    if response.status != 200:
        raise SourceProxyError("guardian_response_status_invalid")
    return await _json_body(response, max_bytes=8 * 1024 * 1024, error_prefix="guardian")


class GuardianRequests:
    """Share exact requests and pace them across a bounded pool of authenticated connections."""

    def __init__(self, plan, capture, binding, cancel_check, progress, http):
        self.capture = capture
        self.binding = binding
        self.progress = progress
        self.http = http
        self.admission = FinderAdmission(plan, cancel_check)
        self.limit = plan.max_concurrency
        self.slots = asyncio.Queue()
        for _ in range(self.limit):
            self.slots.put_nowait(None)
        self.pending = {}
        self.route_ids = set()
        self.active = self.peak = self.joined = 0
        self.failure = None

    def stats(self):
        """Report observed concurrency for this worker invocation."""
        return {
            "requested_concurrency": self.limit,
            "worker_peak_in_flight": self.peak,
            "worker_joined_requests": self.joined,
            "worker_distinct_proxy_routes": len(self.route_ids),
        }

    async def __call__(self, kind, fields):
        await self.admission.cancel_check()
        key = canonical_json(fields)
        identity = (kind, key)
        if task := self.pending.get(identity):
            self.joined += 1
        else:
            raw = self.capture.lookup(kind, key)
            if raw is not None:
                return strict_json(raw)
            task = self.pending[identity] = asyncio.create_task(self._fetch(kind, key, fields))
        return strict_json(await asyncio.shield(task))

    def _reserve(self, kind, key):
        if self.failure is not None:
            raise self.failure
        self.capture.reserve(kind, key)

    async def _fetch(self, kind, key, fields):
        route = await self.slots.get()
        try:
            await self.admission.check_egress()
            url = _ENDPOINTS[kind]
            if self.limit == 1 or route is None:
                cursor = SOURCE_ID if self.limit == 1 else None
                route = _selection(self.binding, url, cursor, error="guardian_authenticated_proxy_required")
            else:
                _remaining_cooldown(self.binding)
            await self.admission.admit(lambda: self._reserve(kind, key))
            self.route_ids.add(route.proxy_id)
            self.active += 1
            self.peak = max(self.peak, self.active)
            try:
                raw = await self._send(kind, key, fields, url, route)
            finally:
                self.active -= 1
            self.capture.retain(kind, key, raw)
            if self.capture.attempt_count() % 100 == 0:
                await self.progress({**self.capture.progress(), **self.stats()})
            return raw
        except Exception as error:
            if self.failure is None:
                self.failure = error
            raise
        finally:
            self.slots.put_nowait(route)
            del self.pending[kind, key]

    async def _send(self, kind, key, fields, url, route):
        method, body = ("GET", {"params": fields}) if kind == "specialty" else ("POST", {"json": fields})
        try:
            async with self.http.request(
                method,
                url,
                **body,
                headers={
                    "Accept": "application/json",
                    "Accept-Encoding": "identity",
                    "ga-request-id": str(uuid.uuid4()),
                },
                allow_redirects=False,
                **route.request_options,
            ) as response:
                return await _read_response(response, self.binding, route, self.admission, self.capture, kind, key)
        except aiohttp.ClientError, TimeoutError:
            raise SourceProxyError("guardian_request_failed") from None

    async def drain(self):
        """Retain responses already sent before the worker reports its terminal status."""
        if self.pending:
            await asyncio.gather(*self.pending.values(), return_exceptions=True)


@asynccontextmanager
async def _requester(plan, capture, binding, cancel_check, pause, progress):
    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(limit=plan.max_concurrency),
        cookie_jar=aiohttp.DummyCookieJar(),
        trust_env=False,
        auto_decompress=False,
        timeout=aiohttp.ClientTimeout(total=60),
    ) as http:
        request = GuardianRequests(plan, capture, binding, cancel_check, progress, http)
        try:
            yield request
        except BaseException as error:
            request.failure = error
            await request.drain()
            if isinstance(error, SourceProxyUnavailable) and not request.admission.held.is_set():
                await pause(error.retry_not_before, binding.cooldown_snapshot())
            await request.admission.report_hold(binding, pause, access_error="guardian_directory_access_denied")
            raise


@asynccontextmanager
async def acquire_directory(plan, root_run_id, binding, *, cancel_check, progress, pause, require_retained=False):
    """Expose a completed, validated capture inside the source-wide acquisition lock."""
    from process.guardian_directory_sweep import capture_directory

    if not isinstance(root_run_id, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", root_run_id):
        raise ValueError("guardian_acquisition_root_invalid")
    if binding is None:
        raise SourceProxyError("guardian_authenticated_proxy_required")
    root = plan.root
    lock = PTG2ArtifactStore(root).named_lock(SOURCE_ID, "source-acquisition")
    if lock.try_acquire() is None:
        raise ValueError("guardian_capture_active")
    try:
        path = root / root_run_id / "capture.sqlite3"
        is_fresh = not path.is_file()
        if is_fresh and require_retained:
            raise ValueError("guardian_retained_capture_required")
        _directory(path.parent)
        mode = "rwc" if is_fresh else "rw"
        with closing(sqlite3.connect(f"{path.as_uri()}?mode={mode}", uri=True)) as connection:
            capture = GuardianCapture(
                connection, canonical_json([ADAPTER_ID, plan.identity]), plan.max_origin_requests, fresh=is_fresh
            )
            async with _requester(plan, capture, binding, cancel_check, pause, progress) as request:
                try:
                    proof = await capture_directory(capture, plan, request, cancel_check, progress)
                except Exception:
                    await progress({**capture.progress(), **request.stats()})
                    raise
            yield capture, {**proof, **capture.summary(RESOURCE_TYPES), **request.stats(), "adapter_id": ADAPTER_ID}
    finally:
        lock.release()
