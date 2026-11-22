# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Run explicit Delta Dental observations outside the FHIR acquisition path."""

import hashlib
import json
import os
import re
from collections import Counter
from contextlib import AsyncExitStack, aclosing
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from tempfile import NamedTemporaryFile, gettempdir

from process.control_cancel import raise_if_cancelled
from process.delta_dental_directory_capture import (
    COMPOSITE_SELECTOR,
    FINDER_SELECTORS,
    NETWORK_LABELS,
    FinderCapturePlan,
    FinderPaginationUnresolved,
    FinderSearch,
    iter_finder_windows,
)
from process.delta_dental_directory_json import SOURCE_ENDPOINT, SOURCE_ID
from process.delta_dental_directory_observations import parse_retained_finder_page
from process.ext.source_http_proxy import SourceProxyUnavailable
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
    identities = {"authorization_scope", "data_edition"}
    if set(plan) == identities | {"searches"} and type(plan["searches"]) is list and plan["searches"]:
        return tuple(FinderSearch(**search) for search in plan["searches"])
    inventory_fields = identities | {"zip_inventory", "radius_miles"}
    if (
        set(plan) in (inventory_fields, inventory_fields | {"selector_scope"})
        and type(plan["zip_inventory"]) is list
        and plan["zip_inventory"]
    ):
        selectors = {
            "primitive": NETWORK_LABELS,
            "named": NETWORK_LABELS | {COMPOSITE_SELECTOR},
            "named-and-all": FINDER_SELECTORS,
        }.get(plan.get("selector_scope", "primitive"))
        if selectors is None:
            raise ValueError("delta_dental_finder_selector_scope_invalid")
        return tuple(
            FinderSearch(network, zip_code, plan["radius_miles"])
            for zip_code in dict.fromkeys(plan["zip_inventory"])
            for network in sorted(selectors)
        )
    raise ValueError("delta_dental_finder_search_manifest_invalid")


def _authorized_plan():
    """Read the protected authorization and operator-selected source edition."""
    manifest = os.environ.get(MANIFEST_ENV)
    if not manifest or not Path(manifest).is_absolute():
        raise ValueError("delta_dental_finder_authorized_manifest_required")
    with Path(manifest).open("rb") as source:
        raw = source.read(1024 * 1024 + 1)
    try:
        if len(raw) > 1024 * 1024:
            raise ValueError
        value = json.loads(raw)
        if type(value) is not dict:
            raise ValueError
        scope, edition = value["authorization_scope"], value["data_edition"]
        if not all(type(item) is str and 0 < len(item) <= 128 and item == item.strip() for item in (scope, edition)):
            raise ValueError
        searches = _searches_from_plan(value)
        fingerprint = hashlib.sha256(json.dumps([scope, edition]).encode()).hexdigest()
    except (TypeError, ValueError, KeyError, RecursionError) as exc:
        raise ValueError("delta_dental_finder_authorized_manifest_invalid") from exc
    return fingerprint, searches


def _artifact_root():
    """Resolve a durable, externally provisioned artifact directory."""
    configured = os.environ.get("HLTHPRT_PROVIDER_DIRECTORY_ARTIFACT_ROOT")
    if not configured or not Path(configured).is_absolute():
        raise ValueError("delta_dental_finder_artifact_root_required")
    root = Path(configured).resolve()
    if root == Path("/") or root.is_relative_to(Path(gettempdir()).resolve()) or not root.is_dir():
        raise ValueError("delta_dental_finder_artifact_root_required")
    return root / "delta-dental-directory"


def _bind_observation_edition(root, run_id, capture_id, task, searches=()):
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


def _enforce_destination_hold(root):
    """Block fresh transport across jobs while a publisher hold is active."""
    path = root / _HOLD_NAME
    try:
        with path.open("rb") as source:
            raw = source.read(1024)
    except FileNotFoundError:
        return
    try:
        stop = json.loads(raw)["retry_not_before"]
        boundary = datetime.fromisoformat(stop) if stop is not None else None
        if boundary is not None and boundary.tzinfo is None:
            raise ValueError
    except (TypeError, ValueError, KeyError) as exc:
        raise ValueError("delta_dental_finder_destination_hold_invalid") from exc
    if boundary is None or boundary > datetime.now(UTC):
        raise SourceProxyUnavailable(stop)


def _sync_artifact_directory(root):
    """Make a newly written capture marker durable before sending a request."""
    directory_fd = os.open(root, os.O_RDONLY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def _persist_destination_hold(root, retry_not_before):
    """Atomically retain the destination hold before recording progress."""
    path = root / _HOLD_NAME
    with NamedTemporaryFile(mode="w", encoding="utf-8", dir=root, delete=False) as staging:
        temporary = Path(staging.name)
        try:
            json.dump({"retry_not_before": retry_not_before}, staging)
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


async def _capture(plan, cancel_check, *, fetch_page):
    """Stream retained source observations into bounded aggregate run metrics."""
    count_by_network = Counter()
    count_by_gap = Counter()
    count_by_state = Counter()
    searches_by_selector = Counter()
    incomplete_windows_by_selector = Counter()
    page_count = window_count = card_count = office_count = saved_pages = 0
    async with aclosing(
        iter_finder_windows(
            plan,
            fetch_page=fetch_page,
            parse_page=parse_retained_finder_page,
            cancel_check=cancel_check,
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


async def _run_http_capture(binding, plan, cancel_check, pause):
    """Open the exact source proxy connection only for an uncached JSON search."""
    from process.delta_dental_directory_http import open_finder_http

    _guard_retained_edition(plan)
    counts_by_transport_action = {"origin_requests": 0}
    async with AsyncExitStack() as resources:
        transports = []

        async def check_before_egress():
            """Fence new origin access even when cached replay may proceed."""
            await cancel_check()
            _enforce_destination_hold(plan.root)

        async def fetch_page(search, number, previous_page):
            """Avoid combining a later Portal response with a retained HTML page."""
            if number != 1 or previous_page is not None:
                raise FinderPaginationUnresolved("legacy_window_remaining_page_unavailable")
            if not transports:
                await check_before_egress()
                _mark_egress_started(plan)
                transports.append(
                    await resources.enter_async_context(
                        open_finder_http(
                            binding, pause=pause, cancel_check=check_before_egress, stats=counts_by_transport_action
                        )
                    )
                )
            return await transports[0](search, number, previous_page)

        summary = await _capture(plan, cancel_check, fetch_page=fetch_page)
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


async def run_delta_observation(directory, ctx, task):
    """Require a scoped proxy before acquiring a protected directory edition."""
    require_observation_task(task)
    run_id = _require_run_id(ctx, task)
    capture_id, searches = _authorized_plan()
    capture_root = _artifact_root()
    token, scope = enter_run(directory)

    async def cancel_check():
        """Honor the existing per-run control-plane cancel flag."""
        await raise_if_cancelled(ctx, {"run_id": run_id})

    async def pause(retry_not_before, cooldowns):
        """Retain the destination hold before reporting the pause."""
        _persist_destination_hold(capture_root, retry_not_before)
        await directory._mark_provider_directory_progress(
            run_id,
            phase="delta dental finder paused",
            done=0,
            total=len(searches),
            message="publisher rate limit",
            metrics={
                "source_http_proxy": {
                    "binding": scope.expected_binding,
                    "retry_not_before": retry_not_before,
                    "cooldowns": cooldowns,
                }
            },
        )

    try:
        source_by_field = {"source_id": SOURCE_ID, "api_base": SOURCE_ENDPOINT, "canonical_api_base": SOURCE_ENDPOINT}
        await cancel_check()
        await scope.prepare([source_by_field], endpoint_scope=SOURCE_ENDPOINT)
        binding = scope.bindings.get(SOURCE_ID)
        if binding is None or scope.credentials.get(SOURCE_ID):
            raise ValueError("delta_dental_finder_source_proxy_required")
        lineage = _bind_observation_edition(capture_root, run_id, capture_id, task, searches)
        resume = bool(task.get("retry_of_run_id"))
        plan = FinderCapturePlan(
            capture_root,
            capture_id,
            capture_id,
            searches,
            plan_id=lineage,
            resume=resume,
            allow_unstarted_resume=resume and not _egress_marker(capture_root, f"run-{lineage}").is_file(),
        )
        summary = await _run_http_capture(binding, plan, cancel_check, pause)
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
