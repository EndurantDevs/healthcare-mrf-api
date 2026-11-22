# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Run explicit Delta Dental observations outside the FHIR acquisition path."""

import asyncio
import hashlib
import json
import math
import os
import re
from collections import Counter
from contextlib import AsyncExitStack, aclosing, suppress
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from tempfile import NamedTemporaryFile, gettempdir
from time import monotonic, time

from process.control_cancel import raise_if_cancelled
from process.delta_dental_directory_capture import (
    COMPOSITE_SELECTOR,
    DEFAULT_REQUEST_BUDGET,
    FINDER_SELECTORS,
    NETWORK_LABELS,
    FinderCapturePlan,
    FinderPaginationUnresolved,
    FinderSearch,
    iter_finder_windows,
)
from process.delta_dental_directory_json import SOURCE_ENDPOINT, SOURCE_ID
from process.delta_dental_directory_observations import parse_retained_finder_page
from process.ext.source_http_proxy import SourceProxyError, SourceProxyUnavailable
from process.provider_directory_source_proxy import enter_run, exit_run
from process.ptg_parts.artifacts import PTG2ArtifactStore

MANIFEST_ENV = "HLTHPRT_DELTA_DENTAL_FINDER_MANIFEST_PATH"
MANIFEST_PATH = "/var/run/provider-directory/delta-dental/plan.json"
OBSERVATION_FLAG = "delta_dental_directory_observation"
_DISABLED_FLAGS = (
    "probe",
    "import_resources",
    "publish_artifacts",
    "publish_corroboration",
    "publish_after_acquisition",
    "full_refresh",
    "bulk_export",
    "stale_cleanup",
    "dataset_rehydrate_only",
)


def is_delta_observation(task):
    """Recognize the reserved identity even when its request is malformed."""
    if not isinstance(task, dict):
        return False
    source_ids = task.get("source_ids")
    if isinstance(source_ids, str):
        source_ids = source_ids.split(",")
    return OBSERVATION_FLAG in task or (
        isinstance(source_ids, (list, tuple))
        and any(isinstance(source_id, str) and source_id.strip() == SOURCE_ID for source_id in source_ids)
    )


def specialized_source_task(directory, ctx, task):
    """Select independent directory workflows before the generic FHIR task."""
    if is_delta_observation(task):
        return run_delta_observation(directory, ctx, task)
    if "rooted_graph" in (task or {}):
        from process.provider_directory_rooted_graph_worker import process_rooted_graph_task

        return process_rooted_graph_task(directory, ctx, task)
    return None


def require_observation_task(task):
    """Keep the explicit finder selector outside generic FHIR, Probe and Profile work."""
    if (
        task.get(OBSERVATION_FLAG) is not True
        or task.get("source_ids") != [SOURCE_ID]
        or task.get("provider_directory_endpoint_scope") != SOURCE_ENDPOINT
        or task.get("probe") is not False
        or "rooted_graph" in task
        or task.get("import_resources") is not False
        or any(task.get(name, False) is not False for name in _DISABLED_FLAGS)
        or any(
            task.get(name)
            for name in (
                "resources",
                "test",
                "test_mode",
                "publish_source_dataset_only",
                "publish_artifacts_only",
                "defer_typed_materialization",
                "source_query",
                "refresh_preset",
                "rehydrate_dataset_id",
                "validated_publication_candidate",
            )
        )
    ):
        raise ValueError("delta_dental_finder_observation_request_invalid")


def _searches_from_plan(plan):
    """Validate an explicit subset or expand the supplied ZIP inventory."""
    plan_by_field = {
        key: setting
        for key, setting in plan.items()
        if key not in {"concurrency", "requests_per_second", "max_origin_requests"}
    }
    identities = {"authorization_scope", "data_edition"}
    if (
        set(plan_by_field) == identities | {"searches"}
        and type(plan_by_field["searches"]) is list
        and plan_by_field["searches"]
    ):
        return tuple(FinderSearch(**search) for search in plan_by_field["searches"])
    inventory_fields = identities | {"zip_inventory", "radius_miles"}
    if (
        set(plan_by_field) in (inventory_fields, inventory_fields | {"selector_scope"})
        and type(plan_by_field["zip_inventory"]) is list
        and plan_by_field["zip_inventory"]
    ):
        selectors = {
            "primitive": NETWORK_LABELS,
            "named": NETWORK_LABELS | {COMPOSITE_SELECTOR},
            "named-and-all": FINDER_SELECTORS,
        }.get(plan_by_field.get("selector_scope", "primitive"))
        if selectors is None:
            raise ValueError("delta_dental_finder_selector_scope_invalid")
        return tuple(
            FinderSearch(network, zip_code, plan_by_field["radius_miles"])
            for zip_code in dict.fromkeys(plan_by_field["zip_inventory"])
            for network in sorted(selectors)
        )
    raise ValueError("delta_dental_finder_search_manifest_invalid")


def _origin_request_budget(value, rate, search_count):
    """Reject an oversized plan before binding its run or opening transport."""
    upper_bound = DEFAULT_REQUEST_BUDGET if rate is None else 200_000
    budget = value.get("max_origin_requests", DEFAULT_REQUEST_BUDGET)
    if type(budget) is not int or not 1 <= budget <= upper_bound or search_count > budget:
        raise ValueError("delta_dental_finder_origin_request_budget_invalid")
    return budget


def _acquisition_settings(manifest_by_field, search_count):
    """Validate the destination pacing and request budget for a protected plan."""
    concurrency = manifest_by_field.get("concurrency", 1)
    rate = manifest_by_field.get("requests_per_second")
    if type(concurrency) is not int or not 1 <= concurrency <= 8:
        raise ValueError
    if "requests_per_second" in manifest_by_field or concurrency > 1:
        if type(rate) not in (float, int) or not math.isfinite(rate) or rate <= 0 or not math.isfinite(1 / rate):
            raise ValueError
        rate = float(rate)
    budget = _origin_request_budget(manifest_by_field, rate, search_count)
    return concurrency, rate, budget


def _authorized_plan():
    """Read the protected authorization and operator-selected source edition."""
    manifest = os.environ.get(MANIFEST_ENV)
    if not manifest or not Path(manifest).is_absolute():
        raise ValueError("delta_dental_finder_authorized_manifest_required")
    with Path(manifest).open("rb") as manifest_file:
        raw = manifest_file.read(1024 * 1024 + 1)
    try:
        if len(raw) > 1024 * 1024:
            raise ValueError
        manifest_by_field = json.loads(raw)
        if type(manifest_by_field) is not dict:
            raise ValueError
        scope, edition = manifest_by_field["authorization_scope"], manifest_by_field["data_edition"]
        if not all(type(item) is str and 0 < len(item) <= 128 and item == item.strip() for item in (scope, edition)):
            raise ValueError
        searches = tuple(dict.fromkeys(_searches_from_plan(manifest_by_field)))
        concurrency, rate, budget = _acquisition_settings(manifest_by_field, len(searches))
        fingerprint = hashlib.sha256(json.dumps([scope, edition]).encode()).hexdigest()
    except (TypeError, ValueError, KeyError, OverflowError, RecursionError) as exc:
        raise ValueError("delta_dental_finder_authorized_manifest_invalid") from exc
    return fingerprint, searches, concurrency, rate, budget


def _artifact_root():
    """Resolve a durable, externally provisioned artifact directory."""
    configured = os.environ.get("HLTHPRT_PROVIDER_DIRECTORY_ARTIFACT_ROOT")
    if not configured or not Path(configured).is_absolute():
        raise ValueError("delta_dental_finder_artifact_root_required")
    root = Path(configured).resolve()
    if root == Path("/") or root.is_relative_to(Path(gettempdir()).resolve()) or not root.is_dir():
        raise ValueError("delta_dental_finder_artifact_root_required")
    return root / "delta-dental-directory"


def _bind_observation_edition(
    root, run_id, capture_id, task, searches=(), *, concurrency=1, rate=None, budget=DEFAULT_REQUEST_BUDGET
):
    """Refuse authorization or search-plan drift within a retry lineage."""
    is_retry = bool(task.get("retry_of_run_id"))
    lineage = task.get("provider_directory_pagination_root_run_id") or run_id
    if type(lineage) is not str or re.fullmatch(r"[A-Za-z0-9_-]{1,128}", lineage) is None:
        raise ValueError("delta_dental_finder_run_lineage_invalid")
    if (is_retry and lineage == run_id) or (not is_retry and lineage != run_id):
        raise ValueError("delta_dental_finder_run_lineage_invalid")
    root.mkdir(parents=True, exist_ok=True, mode=0o750)
    marker = root / f"run-{lineage}.edition"
    serialized_searches = [asdict(search) for search in dict.fromkeys(searches)]
    plan_hash = hashlib.sha256(json.dumps(serialized_searches, sort_keys=True).encode()).hexdigest()
    edition_binding = f"{capture_id}:{plan_hash}"
    if concurrency != 1 or rate is not None or budget != DEFAULT_REQUEST_BUDGET:
        edition_binding += f":{concurrency}:{rate}:{budget}"
    with PTG2ArtifactStore(root).named_lock("delta-dental-directory", "authorized-edition"):
        if marker.is_file():
            if marker.read_text(encoding="utf-8") != edition_binding:
                raise ValueError("delta_dental_finder_run_edition_changed")
            return lineage
        if is_retry:
            raise ValueError("delta_dental_finder_retained_edition_required")
        fd = os.open(marker, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o640)
        with os.fdopen(fd, "w", encoding="utf-8") as output:
            output.write(edition_binding)
            output.flush()
            os.fsync(output.fileno())
        _sync_artifact_directory(root)
    return lineage


_HOLD_NAME = "destination-hold.json"


def _read_destination_hold(root):
    """Read a durable publisher boundary, if present."""
    path = root / _HOLD_NAME
    try:
        with path.open("rb") as source:
            raw = source.read(1024)
    except FileNotFoundError:
        return None
    try:
        stop = json.loads(raw)["retry_not_before"]
        boundary = datetime.fromisoformat(stop) if stop is not None else None
        if boundary is not None and boundary.tzinfo is None:
            raise ValueError
    except (TypeError, ValueError, KeyError) as exc:
        raise ValueError("delta_dental_finder_destination_hold_invalid") from exc
    return (stop, boundary)


def _enforce_destination_hold(root):
    hold = _read_destination_hold(root)
    if hold is not None and (hold[1] is None or hold[1] > datetime.now(UTC)):
        raise SourceProxyUnavailable(hold[0])


def _sync_artifact_directory(root):
    """Make a newly written capture marker durable before sending a request."""
    directory_fd = os.open(root, os.O_RDONLY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def _persist_destination_hold(root, retry_not_before):
    """Atomically retain the destination hold before recording progress."""
    existing = _read_destination_hold(root)
    if existing is not None and (
        existing[1] is None
        or (retry_not_before is not None and existing[1] >= datetime.fromisoformat(retry_not_before))
    ):
        return existing[0]
    _write_destination_state(root, _HOLD_NAME, {"retry_not_before": retry_not_before})
    return retry_not_before


def _write_destination_state(root, name, value):
    path = root / name
    with NamedTemporaryFile(mode="w", encoding="utf-8", dir=root, delete=False) as staging:
        temporary = Path(staging.name)
        try:
            json.dump(value, staging)
            staging.flush()
            os.fsync(staging.fileno())
            os.replace(temporary, path)
            _sync_artifact_directory(root)
        finally:
            temporary.unlink(missing_ok=True)


def _observed_states(cards):
    """Count only states explicitly reported in an office address."""
    for card in cards:
        if card.address and (state := re.search(r"\b([A-Z]{2}) \d{5}(?:-\d{4})?$", card.address)):
            yield state[1]


async def _capture(plan, cancel_check, *, fetch_page, journal=False, report=None):
    """Stream retained source observations into bounded aggregate run metrics."""
    count_by_network = Counter()
    count_by_gap = Counter()
    count_by_state = Counter()
    searches_by_selector = Counter()
    incomplete_windows_by_selector = Counter()
    page_count = window_count = card_count = office_count = saved_pages = 0
    next_report = monotonic() + 300
    async with aclosing(
        iter_finder_windows(
            plan,
            fetch_page=fetch_page,
            parse_page=parse_retained_finder_page,
            cancel_check=cancel_check,
            journal=journal,
        )
    ) as windows:
        async for window in windows:
            window_count += 1
            searches_by_selector[window.search.selector] += 1
            if window.gaps:
                incomplete_windows_by_selector[window.search.selector] += 1
            page_count += len(window.pages)
            saved_pages += window.cached_pages
            count_by_gap.update(window.gaps)
            cards = tuple(card for observed_page in window.pages for card in observed_page.cards)
            card_count += len(cards)
            office_count += sum(card.address is not None for card in cards)
            count_by_network.update(network for card in cards for network in card.office_memberships)
            count_by_state.update(_observed_states(cards))
            if report is not None and monotonic() >= next_report:
                await report(window_count, page_count, saved_pages)
                next_report = monotonic() + 300
    return {
        "directory_complete": False,
        "retained_capture_id": plan.edition_id,
        "search_windows": window_count,
        "pages_retained": page_count,
        "pages_reused": saved_pages,
        "pages_fetched": page_count - saved_pages,
        "card_observations": card_count,
        "office_observations": office_count,
        "membership_observations": dict(count_by_network),
        "office_observations_by_address_state": dict(count_by_state),
        "search_windows_by_selector": dict(searches_by_selector),
        "windows_with_gaps_by_selector": dict(incomplete_windows_by_selector),
        "regional_directory_coverage_verified": False,
        "commercial_advantage_coverage": "separate_regional_source_unverified",
        "gaps": dict(count_by_gap),
    }


def _egress_marker(root, name):
    """Return the durable marker for an edition or lineage that began egress."""
    return root / f"{name}.started"


def _guard_retained_edition(plan):
    """Refuse to redownload an edition whose acquisition cache was lost."""
    if (
        _egress_marker(plan.root, f"edition-{plan.edition_id}").is_file()
        and not (plan.root / f"finder-{plan.edition_id}.sqlite3").is_file()
    ):
        raise ValueError("delta_dental_finder_retained_edition_required")


def _mark_egress_started(plan):
    """Persist the distinction between missing cache and never-started work."""
    _egress_marker(plan.root, f"edition-{plan.edition_id}").touch(mode=0o640)
    _egress_marker(plan.root, f"run-{plan.plan_id}").touch(mode=0o640)
    _sync_artifact_directory(plan.root)


_RATE_NAME = "destination-last-attempt.json"


def _previous_rate_delay(root, interval):
    """Carry previous admission spacing across runs on this artifact root."""
    try:
        with (root / _RATE_NAME).open("rb") as previous:
            started, prior_interval = json.load(previous)
        if not all(
            type(item) in (float, int) and math.isfinite(item) and item >= 0 for item in (started, prior_interval)
        ):
            raise ValueError
    except FileNotFoundError:
        return 0
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("delta_dental_finder_destination_rate_invalid") from exc
    return max(0, (started - time()) + max(interval, prior_interval))


class FinderAdmission:
    """Serialize request starts and retain the strongest destination hold."""

    def __init__(self, plan, cancel_check):
        self.root = plan.root
        self.cancel_check = cancel_check
        self.lock = asyncio.Lock()
        self.held = asyncio.Event()
        self.boundary = None
        self.has_access_denial = False
        self.hold_error = None
        self.next_start = None
        self.loop = asyncio.get_running_loop()
        self.interval = 0 if plan.requests_per_second is None else 1 / plan.requests_per_second

    def record_hold(self, retry_not_before, is_transient):
        """Fence new starts before durably merging the publisher boundary."""
        self.held.set()
        self.has_access_denial |= not is_transient
        try:
            self.boundary = _persist_destination_hold(self.root, retry_not_before)
        except (OSError, ValueError) as exc:
            self.hold_error = exc
            raise

    async def check_egress(self):
        """Check cancellation and holds before using the origin transport."""
        await self.cancel_check()
        if self.held.is_set():
            raise SourceProxyUnavailable(self.boundary)
        _enforce_destination_hold(self.root)

    async def admit(self, reserve_request):
        """Reserve one destination-wide request start without accruing burst credit."""
        async with self.lock:
            await self.check_egress()
            if self.next_start is None:
                self.next_start = self.loop.time() + _previous_rate_delay(self.root, self.interval)
            while (delay := self.next_start - self.loop.time()) > 0:
                with suppress(TimeoutError):
                    await asyncio.wait_for(self.held.wait(), timeout=min(delay, 1))
                await self.check_egress()
            reserve_request()
            _write_destination_state(self.root, _RATE_NAME, [time(), self.interval])
            self.next_start = self.loop.time() + self.interval

    async def report_hold(self, binding, pause):
        """Report one final hold after all admitted work drains."""
        if not self.held.is_set():
            return
        if self.hold_error is not None:
            raise self.hold_error
        await pause(self.boundary, binding.cooldown_snapshot())
        if self.has_access_denial:
            raise SourceProxyError("delta_dental_finder_access_denied") from None
        raise SourceProxyUnavailable(self.boundary) from None


async def _run_http_capture(binding, plan, cancel_check, pause, *, report=None):
    """Open the exact source proxy connection only for an uncached JSON search."""
    from process.delta_dental_directory_http import open_finder_http

    _guard_retained_edition(plan)
    counts_by_transport_action = {"origin_requests": 0}
    transport_lock = asyncio.Lock()
    admission = FinderAdmission(plan, cancel_check)
    async with AsyncExitStack() as resources:
        transports = []

        async def fetch_page(search, number, previous_page, **attempt):
            """Avoid combining a later Portal response with a retained HTML page."""
            if number != 1 or previous_page is not None:
                raise FinderPaginationUnresolved("legacy_window_remaining_page_unavailable")
            if not transports:
                async with transport_lock:
                    await admission.check_egress()
                    if not transports:
                        transports.append(
                            await resources.enter_async_context(
                                open_finder_http(
                                    binding,
                                    pause=pause,
                                    cancel_check=admission.check_egress,
                                    stats=counts_by_transport_action,
                                    concurrency=plan.concurrency,
                                    max_origin_requests=plan.max_origin_requests,
                                    before_start=admission.admit,
                                    on_hold=admission.record_hold,
                                )
                            )
                        )
                        _mark_egress_started(plan)
            return await transports[0](search, number, previous_page, **attempt)

        try:
            summary = await _capture(plan, cancel_check, fetch_page=fetch_page, journal=True, report=report)
        except Exception:
            await admission.report_hold(binding, pause)
            raise
    summary["transport"] = counts_by_transport_action
    return summary


def _require_run_id(ctx, task):
    """Require an exact non-test observation run and prevent cross-run replay."""
    run_id = task.get("run_id") or ctx.get("control_run_id")
    if not run_id or (task.get("run_id") and ctx.get("control_run_id") and run_id != ctx["control_run_id"]):
        raise ValueError("delta_dental_finder_control_run_required")
    if (ctx.get("context") or {}).get("test_mode"):
        raise ValueError("delta_dental_finder_observation_request_invalid")
    return run_id


def _make_capture_plan(root, run_id, task, authorized):
    """Bind the protected search subset and settings to its exact run lineage."""
    capture_id, searches, concurrency, rate, budget = authorized
    lineage = _bind_observation_edition(
        root, run_id, capture_id, task, searches, concurrency=concurrency, rate=rate, budget=budget
    )
    resume = bool(task.get("retry_of_run_id"))
    return FinderCapturePlan(
        root,
        capture_id,
        capture_id,
        searches,
        plan_id=lineage,
        resume=resume,
        allow_unstarted_resume=resume and not _egress_marker(root, f"run-{lineage}").is_file(),
        concurrency=concurrency,
        requests_per_second=rate,
        max_origin_requests=budget,
    )


def _observation_reporters(directory, run_id, scope, total):
    """Use the existing run progress writer for low-frequency counts and holds."""
    progress_by_metric = {"search_windows": 0}

    async def report(window_count, page_count, saved_pages):
        """Report bounded aggregate counts without exposing search inputs."""
        progress_by_metric.update(search_windows=window_count, pages_retained=page_count, pages_reused=saved_pages)
        await directory._mark_provider_directory_progress(
            run_id,
            phase="delta dental finder capture",
            done=window_count,
            total=total,
            message="capturing finder observations",
            metrics={"delta_dental_directory_observation": dict(progress_by_metric)},
        )

    async def pause(retry_not_before, cooldowns):
        """Publish the persisted pause boundary and latest aggregate progress."""
        await directory._mark_provider_directory_progress(
            run_id,
            phase="delta dental finder paused",
            done=progress_by_metric["search_windows"],
            total=total,
            message="publisher hold",
            metrics={
                "source_http_proxy": {
                    "binding": scope.expected_binding,
                    "retry_not_before": retry_not_before,
                    "cooldowns": cooldowns,
                }
            },
        )

    return pause, report


async def run_delta_observation(directory, ctx, task):
    """Require a scoped proxy before acquiring a protected directory edition."""
    require_observation_task(task)
    run_id = _require_run_id(ctx, task)
    authorized = _authorized_plan()
    capture_root = _artifact_root()
    token, scope = enter_run(directory)

    async def cancel_check():
        """Honor the existing per-run control-plane cancel flag."""
        await raise_if_cancelled(ctx, {"run_id": run_id})

    pause, report = _observation_reporters(directory, run_id, scope, len(authorized[1]))

    try:
        source_by_field = {"source_id": SOURCE_ID, "api_base": SOURCE_ENDPOINT, "canonical_api_base": SOURCE_ENDPOINT}
        await cancel_check()
        await scope.prepare([source_by_field], endpoint_scope=SOURCE_ENDPOINT)
        binding = scope.bindings.get(SOURCE_ID)
        if binding is None or scope.credentials.get(SOURCE_ID):
            raise ValueError("delta_dental_finder_source_proxy_required")
        plan = _make_capture_plan(capture_root, run_id, task, authorized)
        summary = await _run_http_capture(
            binding, plan, cancel_check, pause, report=report if plan.requests_per_second is not None else None
        )
        return {
            "status": "succeeded",
            "delta_dental_directory_observation": summary,
            "audit": ctx.setdefault("context", {}).setdefault("audit", {}),
        }
    finally:
        try:
            scope.attach_audit(ctx.setdefault("context", {}).setdefault("audit", {}))
        finally:
            exit_run(token)
