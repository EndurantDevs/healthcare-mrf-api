# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retain the approved Nomi search edition and normalize its completed coverage."""

import hashlib
import os
import re
import sqlite3
from contextlib import asynccontextmanager, closing
from dataclasses import dataclass
from math import isfinite
from pathlib import Path
from tempfile import gettempdir

import aiohttp

from process.delta_dental_directory_http import _json_body, _retry_boundary, _selection
from process.delta_dental_directory_worker import FinderAdmission
from process.ext.source_http_proxy import SourceProxyError, SourceProxyUnavailable
from process.nomi_directory_normalization import NETWORK, normalize_row
from process.ptg_parts.artifacts import PTG2ArtifactStore
from process.vsp_directory_contract import canonical_json
from process.vsp_directory_http import strict_json
from process.vsp_directory_store import _atomic, _directory

SOURCE_ID = "nomi-health-directory"
ENTRY_ID = "nomi-health-provider-directory"
ADAPTER_ID = "nomi-directory-json-v1"
TRANSPORT = "nomi_directory_json"
ENDPOINT_SCOPE = "https://member.nomihealth.com/b2b/daas/api/v1/services/providersearch"
RESOURCE_TYPES = ("Location", "Organization", "OrganizationAffiliation", "Practitioner", "PractitionerRole")
MANIFEST_PATH = "/var/run/provider-directory/nomi-health/plan.json"
_KINDS = ("facilities", "providers")
_MAX_MANIFEST_BYTES = 1024 * 1024
_MAX_PAGE_BYTES = 4 * 1024 * 1024


@dataclass(frozen=True)
class SearchPlan:
    root: Path
    group_id: str
    searches: tuple[tuple[str, int], ...]
    page_size: int
    requests_per_second: float
    max_origin_requests: int

    def payload(self, kind, search, page):
        """Construct the exact approved search request for one page."""
        zip_code, radius = search
        return {
            "groupId": self.group_id,
            "zipCode": zip_code,
            "distance": str(radius),
            "nbrOfRecords": self.page_size,
            "pageNbr": page,
            "sortColIdx": 1,
            "sortOrderDesc": 0,
            **({"providerType": "1,2,3"} if kind == "providers" else {}),
        }

    def identity(self):
        """Bind retained pages to the exact search and request budget."""
        return canonical_json(
            [
                ADAPTER_ID,
                self.group_id,
                self.searches,
                self.page_size,
                self.requests_per_second,
                self.max_origin_requests,
            ]
        ).encode()


def load_search_plan():
    """Read scope from the authorized worker mount, never from a dispatch body."""
    try:
        with Path(MANIFEST_PATH).open("rb") as stream:
            raw = stream.read(_MAX_MANIFEST_BYTES + 1)
        if len(raw) > _MAX_MANIFEST_BYTES:
            raise ValueError
        manifest = strict_json(raw)
    except (OSError, ValueError) as _error:
        raise ValueError("nomi_directory_authorized_manifest_invalid") from None
    fields = {"group_id", "searches", "page_size", "requests_per_second", "max_origin_requests"}
    if type(manifest) is not dict or manifest.keys() != fields:
        raise ValueError("nomi_directory_authorized_manifest_invalid")
    group = manifest["group_id"]
    rate = manifest["requests_per_second"]
    size = manifest["page_size"]
    budget = manifest["max_origin_requests"]
    searches = manifest["searches"]
    if (
        type(group) is not str
        or not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", group)
        or type(rate) not in (int, float)
        or not isfinite(rate)
        or not 0.01 <= rate <= 1
        or type(size) is not int
        or not 1 <= size <= 500
        or type(searches) is not list
        or not 0 < len(searches) <= 100_000
        or type(budget) is not int
        or not 2 * len(searches) <= budget <= 200_000
    ):
        raise ValueError("nomi_directory_authorized_manifest_invalid")
    windows = []
    for search in searches:
        if type(search) is not dict or search.keys() != {"zip_code", "radius_miles"}:
            raise ValueError("nomi_directory_search_scope_invalid")
        zip_code, miles = search["zip_code"], search["radius_miles"]
        if (
            type(zip_code) is not str
            or not re.fullmatch(r"[0-9]{5}", zip_code)
            or type(miles) is not int
            or miles not in {10, 25, 50, 100, 250}
        ):
            raise ValueError("nomi_directory_search_scope_invalid")
        windows.append((zip_code, miles))
    if len(windows) != len(set(windows)):
        raise ValueError("nomi_directory_duplicate_search")
    configured_root = os.environ.get("HLTHPRT_PROVIDER_DIRECTORY_ARTIFACT_ROOT", "")
    root = Path(configured_root)
    if (
        not root.is_absolute()
        or root.resolve() == Path("/")
        or root.resolve().is_relative_to(Path(gettempdir()).resolve())
    ):
        raise ValueError("nomi_directory_artifact_root_invalid")
    return SearchPlan(_directory(root.resolve() / SOURCE_ID), group, tuple(sorted(windows)), size, float(rate), budget)


def _integer(value):
    digits = str(value) if type(value) in (int, str) else ""
    if re.fullmatch(r"[0-9]{1,10}", digits):
        return int(digits)
    raise ValueError("nomi_directory_pagination_metadata_invalid")


def parse_page(raw, page, page_size):
    """Validate the application envelope and the page the publisher actually returned."""
    payload = strict_json(raw)
    if (
        type(payload) is not dict
        or type(payload.get("status")) is not int
        or payload["status"] != 0
        or any(payload.get(key) for key in ("error", "errors"))
    ):
        raise ValueError("nomi_directory_application_error")
    results = payload.get("result")
    if type(results) is not list or len(results) != 1 or type(results[0]) is not dict:
        raise ValueError("nomi_directory_result_invalid")
    result = results[0]
    rows = result.get("providers", [])
    if type(rows) is not list or len(rows) > page_size or any(type(row) is not dict for row in rows):
        raise ValueError("nomi_directory_rows_invalid")
    if _integer(result.get("pageNumber")) != page:
        raise ValueError("nomi_directory_returned_page_changed")
    reported_count = _integer(result.get("nbrOfRecords"))
    if reported_count != len(rows):
        raise ValueError("nomi_directory_returned_count_invalid")
    total = result.get("totalNumberOfRecords")
    return rows, _integer(total) if rows or total is not None else None


async def _origin_page(http, binding, route, admission, url, payload):
    """Read one search page through the admitted proxy, without redirects or rotation."""
    try:
        async with http.post(
            url,
            json=payload,
            headers={"Accept": "application/json", "Accept-Encoding": "identity"},
            allow_redirects=False,
            **route.request_options,
        ) as response:
            if response.status in {401, 403, 429, 503}:
                is_transient = response.status in {429, 503}
                boundary = _retry_boundary(response.headers.get("Retry-After")) if is_transient else None
                admission.record_hold(boundary, is_transient)
                binding.defer(route.proxy_id, boundary)
                raise SourceProxyUnavailable(boundary)
            if response.status != 200:
                raise SourceProxyError("nomi_directory_response_status_invalid")
            return await _json_body(response, max_bytes=_MAX_PAGE_BYTES, error_prefix="nomi_directory")
    except (aiohttp.ClientError, TimeoutError) as _error:
        raise SourceProxyError("nomi_directory_request_failed") from None


class NomiCapture:
    """One acquisition-root cache, with durable request budgeting and bounded normalized output."""

    def __init__(self, db, plan):
        self.db = db
        self.plan = plan
        db.executescript("""
            CREATE TABLE IF NOT EXISTS requests (attempts INTEGER NOT NULL);
            INSERT INTO requests SELECT 0 WHERE NOT EXISTS (SELECT 1 FROM requests);
            CREATE TABLE IF NOT EXISTS page (
                kind TEXT NOT NULL, window INTEGER NOT NULL, number INTEGER NOT NULL,
                body BLOB NOT NULL, digest TEXT NOT NULL, PRIMARY KEY (kind, window, number)
            );
            CREATE TABLE IF NOT EXISTS observation (
                kind TEXT NOT NULL, identity TEXT NOT NULL, PRIMARY KEY (kind, identity)
            );
            CREATE TABLE IF NOT EXISTS resource (
                family TEXT NOT NULL, id TEXT NOT NULL, payload TEXT NOT NULL, PRIMARY KEY (family, id)
            );
        """)
        self._write_resources([NETWORK])
        self.db.commit()

    def reserve_request(self):
        """Charge each admitted origin attempt across worker restarts."""
        with self.db:
            if not self.db.execute(
                "UPDATE requests SET attempts = attempts + 1 WHERE attempts < ?", (self.plan.max_origin_requests,)
            ).rowcount:
                raise SourceProxyError("nomi_directory_request_budget_exhausted")

    def lookup(self, kind, window, page):
        """Read and verify one previously accepted publisher page."""
        match = self.db.execute(
            "SELECT body, digest FROM page WHERE kind = ? AND window = ? AND number = ?", (kind, window, page)
        ).fetchone()
        if match is None:
            return None
        raw, digest = match
        if hashlib.sha256(raw).hexdigest() != digest:
            raise ValueError("nomi_directory_retained_page_changed")
        return raw

    def _write_resources(self, resources):
        self.db.executemany(
            "INSERT OR IGNORE INTO resource VALUES (?, ?, ?)",
            ((value["resourceType"], value["id"], canonical_json(value)) for value in resources),
        )

    def retain(self, kind, window, page, raw, observations):
        """Commit response and projected rows together before advancing the cursor."""
        with self.db:
            self.db.execute(
                "INSERT INTO page VALUES (?, ?, ?, ?, ?)",
                (kind, window, page, raw, hashlib.sha256(raw).hexdigest()),
            )
            self.db.executemany(
                "INSERT OR IGNORE INTO observation VALUES (?, ?)",
                ((kind, key) for key, _ in observations),
            )
            self._write_resources(resource for _, resources in observations for resource in resources)

    def iter_resources(self):
        """Stream the complete canonical resource order for batch COPY."""
        for (payload,) in self.db.execute("SELECT payload FROM resource ORDER BY family, id"):
            yield strict_json(payload)

    def summary(self, counts_by_endpoint):
        """Report exact row accounting within the configured searches."""
        return {
            "adapter_id": ADAPTER_ID,
            "scope_sha256": hashlib.sha256(self.plan.identity()).hexdigest(),
            "search_windows": len(self.plan.searches),
            "endpoints": counts_by_endpoint,
            "coverage_scope": "configured_search_windows",
            "distinct_source_rows": dict(self.db.execute("SELECT kind, count(*) FROM observation GROUP BY kind")),
            "origin_requests": self.db.execute("SELECT attempts FROM requests").fetchone()[0],
            "resource_counts": dict(self.db.execute("SELECT family, count(*) FROM resource GROUP BY family")),
        }


async def _capture_window(capture, kind, window, search, request_page, cancel_check):
    """Prove progress and completeness in one window before admitting its rows."""
    plan = capture.plan
    rows_seen = 0
    seen_keys = set()
    min_total = max_total = None
    page = 0
    while True:
        await cancel_check()
        page += 1
        raw = capture.lookup(kind, window, page)
        is_retained = raw is not None
        if not is_retained:
            raw = await request_page(kind, plan.payload(kind, search, page))
        observed_rows, reported = parse_page(raw, page, plan.page_size)
        observations = [
            (canonical_json(key), resources)
            for key, resources in (normalize_row(kind, observation) for observation in observed_rows)
        ]
        page_keys = {key for key, _ in observations}
        if page_keys and page_keys <= seen_keys:
            raise ValueError("nomi_directory_pagination_repeated")
        seen_keys.update(page_keys)
        rows_seen += len(observed_rows)
        if reported is not None:
            max_total = reported if max_total is None else max(max_total, reported)
            min_total = reported if min_total is None else min(min_total, reported)
        if not observed_rows and rows_seen < (max_total or 0):
            raise ValueError("nomi_directory_pagination_truncated")
        if not is_retained:
            capture.retain(kind, window, page, raw, observations)
        if not observed_rows:
            return page, rows_seen, len(seen_keys), min_total or 0, max_total or 0


def _capture_path(plan, root_run_id):
    """Keep an edition's resume scope immutable and prevent cross-group replacement."""
    if type(root_run_id) is not str or not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", root_run_id):
        raise ValueError("nomi_directory_acquisition_root_invalid")
    group_path = plan.root / "group.json"
    group = canonical_json(plan.group_id).encode()
    if group_path.exists() and group_path.read_bytes() != group:
        raise ValueError("nomi_directory_group_changed")
    _atomic(group_path, group, immutable=True)
    root = _directory(plan.root / root_run_id)
    path = root / "capture.sqlite3"
    identity = root / "scope.json"
    if identity.exists() and (not path.is_file() or identity.read_bytes() != plan.identity()):
        raise ValueError("nomi_directory_retained_edition_required")
    return path, identity


async def _capture_searches(capture, request_page, cancel_check, progress):
    """Track each endpoint's observed windows, totals, and distinct row keys."""
    counts_by_endpoint = {}
    for kind in _KINDS:
        counts_by_metric = {
            "windows": 0,
            "pages": 0,
            "rows": 0,
            "distinct_window_rows": 0,
            "windows_with_total_drift": 0,
        }
        counts_by_endpoint[kind] = counts_by_metric
        for window, search in enumerate(capture.plan.searches):
            pages, row_count, unique, min_total, max_total = await _capture_window(
                capture, kind, window, search, request_page, cancel_check
            )
            counts_by_metric["windows"] += 1
            counts_by_metric["pages"] += pages
            counts_by_metric["rows"] += row_count
            counts_by_metric["distinct_window_rows"] += unique
            counts_by_metric["windows_with_total_drift"] += int(min_total != max_total or max_total != row_count)
            await progress(counts_by_endpoint)
    return counts_by_endpoint


async def _capture_with_proxy(capture, binding, cancel_check, progress, pause):
    """Use one authenticated proxy identity for the lifetime of this acquisition."""
    admission = FinderAdmission(capture.plan, cancel_check)
    proxy_ids = set()
    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(limit=1),
        cookie_jar=aiohttp.DummyCookieJar(),
        trust_env=False,
        auto_decompress=False,
        timeout=aiohttp.ClientTimeout(total=60),
    ) as http:

        async def request_page(kind, payload):
            """Reserve one request through the pinned route after the destination check."""
            await admission.check_egress()
            url = f"{ENDPOINT_SCOPE}/{kind}"
            route = _selection(binding, url, SOURCE_ID, error="nomi_directory_authenticated_proxy_required")
            if proxy_ids and route.proxy_id not in proxy_ids:
                raise SourceProxyError("nomi_directory_proxy_route_changed")
            proxy_ids.add(route.proxy_id)
            await admission.admit(capture.reserve_request)
            return await _origin_page(http, binding, route, admission, url, payload)

        try:
            return await _capture_searches(capture, request_page, cancel_check, progress)
        except SourceProxyUnavailable as unavailable:
            await admission.report_hold(binding, pause, access_error="nomi_directory_access_denied")
            await pause(unavailable.retry_not_before, binding.cooldown_snapshot())
            raise


@asynccontextmanager
async def acquire_directory(plan, root_run_id, binding, *, cancel_check, progress, pause):
    """Collect every configured search before admitting normalized resources."""
    lock = PTG2ArtifactStore(plan.root).named_lock(SOURCE_ID, "source-acquisition")
    if lock.try_acquire() is None:
        raise ValueError("nomi_directory_capture_already_active")
    try:
        path, identity = _capture_path(plan, root_run_id)
        with closing(sqlite3.connect(path)) as connection:
            capture = NomiCapture(connection, plan)
            _atomic(identity, plan.identity(), immutable=True)
            counts_by_endpoint = await _capture_with_proxy(capture, binding, cancel_check, progress, pause)
            proof = capture.summary(counts_by_endpoint)
            if set(proof["resource_counts"]) != set(RESOURCE_TYPES):
                raise ValueError("nomi_directory_incomplete_resource_coverage")
            yield capture, proof
    finally:
        lock.release()
