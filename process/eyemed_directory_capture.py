# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Capture the EyeMed network catalog and ranked ZIP searches."""

import asyncio
import hashlib
import os
import re
import sqlite3
import zlib
from contextlib import asynccontextmanager, closing
from dataclasses import dataclass
from itertools import product
from math import isfinite
from pathlib import Path
from tempfile import gettempdir

import aiohttp

from process.delta_dental_directory_http import _json_body, _retry_boundary, _selection
from process.delta_dental_directory_worker import FinderAdmission
from process.ext.source_http_proxy import SourceProxyError, SourceProxyUnavailable
from process.eyemed_directory_normalization import LOCATION_ARRAYS, network_resource, site_resources
from process.nomi_directory_normalization import _text
from process.provider_directory_generic_source_policy import source_transaction as candidate_write_scope
from process.ptg_parts.artifacts import PTG2ArtifactStore
from process.vsp_directory_contract import canonical_json
from process.vsp_directory_http import strict_json
from process.vsp_directory_store import _directory

ORG_NAME = "EyeMed"
URN = "eyemed"
PREFIX = "eyemed_directory"
COVERAGE_MODE = "configured_zip_windows"
SOURCE_ID = "eyemed-directory"
ENTRY_ID = "eyemed-provider-directory"
ADAPTER_ID = "eyemed-directory-json-v1"
TRANSPORT = "eyemed_directory_json"
ENDPOINT_SCOPE = "https://eyedoclocator.eyemedvisioncare.com/eyedoclocator/api/v1"
RESOURCE_TYPES = ("Location", "Organization", "OrganizationAffiliation", "Practitioner", "PractitionerRole")
MANIFEST_PATH = "/var/run/provider-directory/eyemed/plan.json"
_MAX_REPLY = 32 * 1024 * 1024


@dataclass(frozen=True)
class SearchPlan:
    root: Path
    zip_codes: tuple[str, ...]
    requests_per_second: float
    max_origin_requests: int
    max_concurrent_requests: int = 1

    def identity(self):
        """Bind the edition to the exact mounted scope and request budget."""
        identity_fields = [ADAPTER_ID, self.zip_codes, self.requests_per_second, self.max_origin_requests]
        if self.max_concurrent_requests > 1:
            identity_fields.append(self.max_concurrent_requests)
        return canonical_json(identity_fields)


def load_search_plan():
    """Read an immutable ZIP scope from the worker mount."""
    with Path(MANIFEST_PATH).open("rb") as stream:
        raw = stream.read(1024 * 1024 + 1)
    if len(raw) > 1024 * 1024:
        raise ValueError("eyemed_directory_plan_too_large")
    payload_by_field = strict_json(raw)
    if not isinstance(payload_by_field, dict) or payload_by_field.keys() - {"max_concurrent_requests"} != {
        "zip_codes",
        "requests_per_second",
        "max_origin_requests",
    }:
        raise ValueError("eyemed_directory_plan_invalid")
    concurrency = payload_by_field.get("max_concurrent_requests", 1)
    zips, rate, budget = (
        payload_by_field["zip_codes"],
        payload_by_field["requests_per_second"],
        payload_by_field["max_origin_requests"],
    )
    if (
        type(concurrency) is not int
        or not 1 <= concurrency <= 4
        or not isinstance(zips, list)
        or not zips
        or any(type(z) is not str or not re.fullmatch("[0-9]{5}", z) for z in zips)
        or len(set(zips)) != len(zips)
        or type(rate) not in {float, int}
        or not isfinite(rate)
        or not 0 < rate <= 10
        or type(budget) is not int
        or budget <= len(zips)
        or budget > 2**31 - 1
    ):
        raise ValueError("eyemed_directory_plan_invalid")
    root = Path(os.environ.get("HLTHPRT_PROVIDER_DIRECTORY_ARTIFACT_ROOT", ""))
    if (
        not root.is_absolute()
        or root.resolve() == Path("/")
        or root.resolve().is_relative_to(Path(gettempdir()).resolve())
    ):
        raise ValueError("eyemed_directory_artifact_root_invalid")
    return SearchPlan(_directory(root.resolve() / SOURCE_ID), tuple(sorted(zips)), rate, budget, concurrency)


def _networks(raw):
    payload = strict_json(raw)
    if not isinstance(payload, dict) or payload.get("messageCode") != "NTW001" or payload.get("nextParams") is not None:
        raise ValueError("eyemed_directory_network_catalog_invalid")
    rows = payload.get("networks")
    if not isinstance(rows, list) or not rows:
        raise ValueError("eyemed_directory_network_catalog_empty")
    networks_by_id = {}
    for network_row in rows:
        resource = network_resource(network_row)
        code = resource["identifier"][0]["value"]
        if code in networks_by_id:
            raise ValueError("eyemed_directory_network_repeated")
        networks_by_id[code] = resource
    return networks_by_id


def _payload(code, zip_code):
    query_by_name = {"networkId": code, "zip": zip_code, "tieredView": "false", "sortByZipClass": "true"}
    return {
        "tieredView": "false",
        "sortByZipClass": "true",
        "networkId": code,
        "clientId": "member",
        "address": {"zip": zip_code},
        "allParams": query_by_name,
    }


def _locations(payload_by_field, code, name):
    if not isinstance(payload_by_field, dict) or payload_by_field.get("messageCode") != "PRV001":
        raise ValueError("eyemed_directory_search_invalid")
    if (
        payload_by_field.get("networkId") != code
        or _text(payload_by_field, "networkName") != name
        or payload_by_field.get("defaultInd") is not False
    ):
        raise ValueError("eyemed_directory_network_mismatch")
    arrays = [payload_by_field.get(field) for field in LOCATION_ARRAYS]
    if not all(isinstance(items, list) for items in arrays):
        raise ValueError("eyemed_directory_location_list_invalid")
    for items in arrays:
        yield from items


class EyeMedCapture:
    def __init__(self, db, plan, is_retained):
        self.db = db
        self.plan = plan
        self.failure = None
        self.peak_in_flight = 0
        self.used_proxy_ids = set()
        if not is_retained:
            with db:
                db.execute("CREATE TABLE run(scope TEXT NOT NULL, attempts INTEGER NOT NULL, proxy_id TEXT)")
                db.execute("INSERT INTO run VALUES (?,0,NULL)", (plan.identity(),))
                db.execute("""CREATE TABLE reply(network TEXT, zip TEXT, body BLOB NOT NULL, done INTEGER NOT NULL, refreshed TEXT,
                              PRIMARY KEY(network,zip)) WITHOUT ROWID""")
                db.execute("CREATE TABLE practitioner_npi(id TEXT, npi TEXT, PRIMARY KEY(id,npi)) WITHOUT ROWID")
                db.execute(
                    """CREATE TABLE site_version(network TEXT, digest BLOB, PRIMARY KEY(network,digest)) WITHOUT ROWID"""
                )
                db.execute("""CREATE TABLE resource(family TEXT, id TEXT, network TEXT, quality INTEGER, body TEXT,
                              PRIMARY KEY(family,id)) WITHOUT ROWID""")
        if db.execute("SELECT scope FROM run").fetchall() != [(plan.identity(),)]:
            raise ValueError("eyemed_directory_retained_scope_changed")

    def reserve(self):
        """Charge each attempt against the durable run budget before egress."""
        with self.db:
            if not self.db.execute(
                "UPDATE run SET attempts=attempts+1 WHERE attempts<?", (self.plan.max_origin_requests,)
            ).rowcount:
                raise SourceProxyError("eyemed_directory_request_budget_exhausted")

    async def reply(self, code, zip_code, request):
        """Save successful reply bytes before parsing, including unexpected application errors."""
        match = self.db.execute("SELECT body FROM reply WHERE network=? AND zip=?", (code, zip_code)).fetchone()
        if match:
            return zlib.decompress(match[0])
        raw = await request("search" if code else "networks", _payload(code, zip_code) if code else None)
        with self.db:
            self.db.execute("INSERT INTO reply VALUES (?,?,?,0,NULL)", (code, zip_code, zlib.compress(raw, level=1)))
        return raw

    def store_resources(self, resources, code):
        """Store unique resources, preferring clinician records with a valid NPI."""

        def records():
            """Prepare bounded inserts for this office."""
            for resource in resources:
                family = resource["resourceType"]
                network = code if family in {"PractitionerRole", "OrganizationAffiliation"} else ""
                quality = len(resource.get("identifier", ())) if family == "Practitioner" else 0
                if family == "Practitioner" and quality > 1:
                    self.db.execute(
                        "INSERT OR IGNORE INTO practitioner_npi VALUES (?,?)",
                        (resource["id"], resource["identifier"][1]["value"]),
                    )
                yield family, resource["id"], network, quality, canonical_json(resource)

        self.db.executemany(
            """INSERT INTO resource VALUES (?,?,?,?,?) ON CONFLICT(family,id) DO UPDATE SET
               body=excluded.body, quality=excluded.quality WHERE excluded.quality>resource.quality"""
            + (
                " OR (excluded.quality=resource.quality AND excluded.body<resource.body)"
                if self.plan.max_concurrent_requests > 1
                else ""
            ),
            records(),
        )

    def accept_search(self, raw, code, zip_code, name):
        """Normalize and admit an entire search in one local transaction."""
        payload_by_field = strict_json(raw)
        if (
            isinstance(payload_by_field, dict)
            and payload_by_field.keys() == {"messageCode", "message", "nextParams"}
            and payload_by_field["messageCode"] in {"PRV002", "PRV007"}
            and payload_by_field["message"] == ""
            and isinstance(payload_by_field["nextParams"], str)
            and payload_by_field["nextParams"]
        ):
            with self.db:
                status = 2 if payload_by_field["messageCode"] == "PRV002" else 3
                self.db.execute("UPDATE reply SET done=? WHERE network=? AND zip=?", (status, code, zip_code))
            return
        with self.db:
            for site in _locations(payload_by_field, code, name):
                if not isinstance(site, dict):
                    raise ValueError("eyemed_directory_location_invalid")
                # Distance changes with the search origin. Identical office detail only needs projecting once.
                digest = hashlib.sha256(
                    canonical_json(
                        {key: field_value for key, field_value in site.items() if key != "distance"}
                    ).encode()
                ).digest()
                if self.db.execute("INSERT OR IGNORE INTO site_version VALUES (?,?)", (code, digest)).rowcount:
                    self.store_resources(site_resources(site, code), code)
            refreshed = payload_by_field.get("lastRefreshedDate")
            if refreshed is not None and (type(refreshed) is not str or len(refreshed) > 64):
                raise ValueError("eyemed_directory_refresh_date_invalid")
            self.db.execute(
                "UPDATE reply SET done=1,refreshed=? WHERE network=? AND zip=?", (refreshed, code, zip_code)
            )

    def iter_resources(self):
        """Stream canonical resources, withholding conflicting NPI identities."""
        query = """WITH ambiguous AS (SELECT id FROM practitioner_npi GROUP BY id HAVING count(*)>1)
                   SELECT r.body, a.id FROM resource r LEFT JOIN ambiguous a
                   ON r.family='Practitioner' AND r.id=a.id ORDER BY r.family,r.id"""
        for body, ambiguous in self.db.execute(query):
            resource = strict_json(body)
            if ambiguous:
                resource["identifier"] = [
                    item for item in resource["identifier"] if item["system"] == "urn:eyemed:provider-id"
                ]
            yield resource

    async def collect(self, request, cancel_check, progress):
        """Collect the remaining windows and summarize publisher evidence."""
        networks_by_id = _networks(await self.reply("", "", request))
        if len(networks_by_id) * len(self.plan.zip_codes) + 1 > self.plan.max_origin_requests:
            raise ValueError("eyemed_directory_network_scope_exceeds_budget")
        with self.db:
            self.store_resources(networks_by_id.values(), "")
        windows_by_network = await self._collect_windows(networks_by_id, request, cancel_check, progress)
        return self._summary(networks_by_id, windows_by_network)

    async def check_active(self, cancel_check):
        """Stop new starts while already admitted requests drain to durable storage."""
        if self.failure is None:
            await cancel_check()
        if self.failure is not None:
            raise RuntimeError("eyemed_directory_capture_stopped")

    async def _collect_windows(self, networks_by_id, request, cancel_check, progress):
        searches = product(self.plan.zip_codes, sorted(networks_by_id))
        windows_by_network = dict.fromkeys(networks_by_id, 0)
        planned = len(networks_by_id) * len(self.plan.zip_codes)

        async def consume_windows():
            """Each window is removed from the shared iterator before its first await."""
            try:
                for zip_code, code in searches:
                    await self.check_active(cancel_check)
                    accepted = self.db.execute(
                        "SELECT done FROM reply WHERE network=? AND zip=?", (code, zip_code)
                    ).fetchone()
                    if accepted not in {(1,), (2,), (3,)}:
                        raw = await self.reply(code, zip_code, request)
                        self.accept_search(raw, code, zip_code, networks_by_id[code]["name"])
                    windows_by_network[code] += 1
                    completed = sum(windows_by_network.values())
                    if completed % 100 == 0 or completed == planned:
                        await progress(
                            {
                                "completed_windows": completed,
                                "planned_windows": planned,
                                "completed_windows_by_network": dict(windows_by_network),
                                "requests_per_second": self.plan.requests_per_second,
                                "max_concurrent_requests": self.plan.max_concurrent_requests,
                                "peak_in_flight": self.peak_in_flight,
                                "distinct_routes": len(self.used_proxy_ids),
                                "origin_requests": self.db.execute("SELECT attempts FROM run").fetchone()[0],
                            }
                        )
            except (Exception, asyncio.CancelledError) as failure:
                if self.failure is None:
                    self.failure = failure

        workers = asyncio.gather(*(consume_windows() for _ in range(self.plan.max_concurrent_requests)))
        try:
            await asyncio.shield(workers)
        except asyncio.CancelledError as cancelled:
            self.failure = cancelled
            await workers
            raise
        if self.failure is not None:
            raise self.failure
        return windows_by_network

    def _summary(self, networks_by_id, windows_by_network):
        return {
            "adapter_id": ADAPTER_ID,
            "coverage_scope": COVERAGE_MODE,
            "max_concurrent_requests": self.plan.max_concurrent_requests,
            "requests_per_second": self.plan.requests_per_second,
            "peak_in_flight_this_worker": self.peak_in_flight,
            "distinct_routes_this_worker": len(self.used_proxy_ids),
            "scope_sha256": hashlib.sha256(self.plan.identity().encode()).hexdigest(),
            "publisher_refresh_dates": [
                {"network_id": code, "last_refreshed_date": date, "windows": count}
                for code, date, count in self.db.execute(
                    "SELECT network, refreshed, count(*) FROM reply WHERE done=1 GROUP BY network,refreshed ORDER BY network,refreshed"
                )
            ],
            "rejected_zips_by_network": dict(
                self.db.execute("SELECT network,count(*) FROM reply WHERE done=3 GROUP BY network")
            ),
            "windows_by_network": windows_by_network,
            "network_names": {code: network["name"] for code, network in networks_by_id.items()},
            "site_memberships": dict(
                self.db.execute(
                    "SELECT network, count(*) FROM resource WHERE family='OrganizationAffiliation' GROUP BY network"
                )
            ),
            "clinician_site_memberships": dict(
                self.db.execute(
                    "SELECT network, count(*) FROM resource WHERE family='PractitionerRole' GROUP BY network"
                )
            ),
            "publisher_prv002_by_network": dict(
                self.db.execute("SELECT network,count(*) FROM reply WHERE done=2 GROUP BY network")
            ),
            "ambiguous_practitioner_npis": self.db.execute(
                "SELECT count(*) FROM (SELECT id FROM practitioner_npi GROUP BY id HAVING count(*)>1)"
            ).fetchone()[0],
            "origin_requests": self.db.execute("SELECT attempts FROM run").fetchone()[0],
            "resource_counts": dict.fromkeys(RESOURCE_TYPES, 0)
            | dict(self.db.execute("SELECT family,count(*) FROM resource GROUP BY family")),
        }


async def _request(http, route, admission, binding, url, payload):
    options = {"json": payload} if payload is not None else {}
    async with http.request(
        "POST" if payload is not None else "GET",
        url,
        **options,
        headers={"Accept": "application/json", "Accept-Encoding": "gzip"},
        allow_redirects=False,
        **route.request_options,
    ) as response:
        if response.status in {401, 403, 429, 503}:
            is_transient = response.status in {429, 503}
            until = _retry_boundary(response.headers.get("Retry-After")) if is_transient else None
            admission.record_hold(until, is_transient)
            binding.defer(route.proxy_id, until)
            raise SourceProxyUnavailable(until)
        if response.status in {408, 500, 502, 504}:
            raise ConnectionError("eyemed_directory_temporary_failure")
        if response.status != 200:
            raise SourceProxyError(f"eyemed_directory_response_status_{response.status}")
        return await _json_body(response, max_bytes=_MAX_REPLY, error_prefix=PREFIX, allow_gzip=True)


class EyeMedTransport:
    """Share one destination budget and cooldown across all in-flight windows."""

    def __init__(self, capture, binding, session, cancel_check):
        self.capture = capture
        self.binding = binding
        self.session = session
        self.admission = FinderAdmission(capture.plan, lambda: capture.check_active(cancel_check))
        self.active_routes = set()
        self.routes_changed = asyncio.Condition()
        self.in_flight = 0
        self.sticky_proxy_id = capture.db.execute("SELECT proxy_id FROM run").fetchone()[0]

    def _select_route(self, url):
        if self.capture.plan.max_concurrent_requests > 1:
            return _selection(
                self.binding, url, None, self.active_routes, error="eyemed_directory_authenticated_proxy_required"
            )
        route = _selection(self.binding, url, SOURCE_ID, error="eyemed_directory_authenticated_proxy_required")
        if self.sticky_proxy_id is None:
            with self.capture.db:
                self.capture.db.execute("UPDATE run SET proxy_id=?", (route.proxy_id,))
            self.sticky_proxy_id = route.proxy_id
        elif route.proxy_id != self.sticky_proxy_id:
            raise SourceProxyError("eyemed_directory_proxy_route_changed")
        return route

    async def _available_route(self, url):
        async with self.routes_changed:
            while True:
                await self.admission.check_egress()
                try:
                    route = self._select_route(url)
                except SourceProxyUnavailable as unavailable:
                    if unavailable.retry_not_before or not self.active_routes or self.admission.held.is_set():
                        raise
                    await self.routes_changed.wait()
                    continue
                if route.proxy_id in self.active_routes:
                    raise SourceProxyError("eyemed_directory_proxy_route_already_active")
                self.active_routes.add(route.proxy_id)
                return route

    async def request(self, path, payload):
        """Count each request attempt against the search budget."""
        url = f"{ENDPOINT_SCOPE}/{path}"
        route = await self._available_route(url)
        try:
            for attempt in range(3):
                await self.admission.admit(self.capture.reserve)
                self.in_flight += 1
                self.capture.peak_in_flight = max(self.capture.peak_in_flight, self.in_flight)
                self.capture.used_proxy_ids.add(route.proxy_id)
                try:
                    return await _request(self.session, route, self.admission, self.binding, url, payload)
                except aiohttp.ClientError, TimeoutError, ConnectionError:
                    if attempt == 2:
                        raise SourceProxyError("eyemed_directory_transport_unavailable") from None
                finally:
                    self.in_flight -= 1
                await asyncio.sleep(2**attempt)
        finally:
            async with self.routes_changed:
                self.active_routes.remove(route.proxy_id)
                self.routes_changed.notify_all()


async def _collect(capture, binding, cancel_check, progress, pause):
    async with aiohttp.ClientSession(
        cookie_jar=aiohttp.DummyCookieJar(),
        trust_env=False,
        auto_decompress=False,
        timeout=aiohttp.ClientTimeout(total=90),
    ) as session:
        transport = EyeMedTransport(capture, binding, session, cancel_check)
        try:
            return await capture.collect(transport.request, cancel_check, progress)
        except (Exception, asyncio.CancelledError) as failure:
            await transport.admission.report_hold(binding, pause, access_error="eyemed_directory_access_denied")
            if isinstance(failure, SourceProxyUnavailable):
                await pause(failure.retry_not_before, binding.cooldown_snapshot())
            raise


@asynccontextmanager
async def acquire_directory(plan, root_run_id, binding, *, cancel_check, progress, pause, require_retained=False):
    """Hold the edition lock until the consumer finishes reading the capture."""
    if not isinstance(root_run_id, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", root_run_id):
        raise ValueError("eyemed_directory_acquisition_root_invalid")
    lock = PTG2ArtifactStore(plan.root).named_lock(SOURCE_ID, "source-acquisition")
    if lock.try_acquire() is None:
        raise ValueError("eyemed_directory_capture_already_active")
    try:
        path = plan.root / root_run_id / "capture.sqlite3"
        is_retained = path.is_file()
        if require_retained and not is_retained:
            raise ValueError("eyemed_directory_retained_edition_required")
        _directory(path.parent)
        with closing(sqlite3.connect(path)) as db:
            capture = EyeMedCapture(db, plan, is_retained)
            yield capture, await _collect(capture, binding, cancel_check, progress, pause)
    finally:
        lock.release()
