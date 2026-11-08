# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact unfiltered census reconciled against a disjoint, fully counted tree."""

import datetime
import hashlib
import re
import sys

from process import provider_directory_capped_partition as shared
from process.provider_directory_time_partition import CountObservation, WindowState, parse_utc_instant

CONTRACT = "unfiltered-leaf-sum-v1"


def mode(settings):
    """Validate the explicit contract without changing legacy configuration identities."""
    selected = settings.get("census_mode")
    if selected is not None and (selected != CONTRACT or settings.get("versioned_search_limit") is not None):
        raise ValueError("partition_census_mode_invalid")
    return selected


def handler(config):
    """Select a census extension only for its declared source contract."""
    if config.census_mode == CONTRACT:
        return sys.modules[__name__]
    return shared if config.versioned_search_limit is not None else None


def validate(census, plan=None):
    """Keep leaf evidence distinct from root observations and other contracts."""
    proof = census.leaf_sum
    if (
        not isinstance(proof, dict)
        or set(proof) != {"contract", "count_pre", "count_post", "refinements"}
        or proof["contract"] != CONTRACT
        or census.capped is not None
        or census.ranged_root_pre is not None
        or census.ranged_root_post is not None
        or not isinstance(proof["refinements"], dict)
    ):
        raise ValueError("leaf_census_contract_invalid")
    counts = [census.unfiltered_pre, proof["count_pre"], proof["count_post"], census.unfiltered_post]
    if any(count is not None and (type(count) is not int or count < 0) for count in counts):
        raise ValueError("leaf_census_count_invalid")
    if any(counts[index] is not None and counts[index - 1] is None for index in range(1, 4)):
        raise ValueError("leaf_census_predecessor_missing")
    if census.failure is None and len({count for count in counts if count is not None}) > 1:
        raise ValueError("leaf_census_counts_conflict")
    for window_id, evidence in proof["refinements"].items():
        if (
            not isinstance(window_id, str)
            or not isinstance(evidence, dict)
            or set(evidence) != {"error", "request_sha256", "retry_not_before"}
            or evidence["error"] != "http_503"
            or not isinstance(evidence["request_sha256"], str)
            or not re.fullmatch("[0-9a-f]{64}", evidence["request_sha256"])
        ):
            raise ValueError("leaf_census_refinement_invalid")
        parse_utc_instant(evidence["retry_not_before"])
    if plan is not None:
        _validate_plan(census, plan)
    return all(count is not None for count in counts) and census.failure is None


def _validate_plan(census, plan):
    proof = census.leaf_sum
    if census.unfiltered_pre is None and any(
        window.count is not None or window.split_reason for window in plan.windows.values()
    ):
        raise ValueError("leaf_census_unfiltered_pre_missing")
    for window_id in proof["refinements"]:
        window = plan.windows.get(window_id)
        if window is None or window.count is not None or window.split_reason not in (None, "planned"):
            raise ValueError("leaf_census_refinement_window_invalid")
    for window in plan.windows.values():
        if window.split_reason == "timeout" or window.overflow_lower_bound is not None:
            raise ValueError("leaf_census_foreign_split_evidence")
        if (
            window.split_reason == "planned"
            and window.window_id != "root"
            and window.window_id not in proof["refinements"]
        ):
            raise ValueError("leaf_census_planned_split_unproven")
    if proof["count_pre"] is not None and census.failure is None:
        if (
            any(window.count is None for window in plan.leaf_windows())
            or sum(window.count for window in plan.leaf_windows()) != proof["count_pre"]
        ):
            raise ValueError("leaf_census_frozen_sum_changed")


def restore(directory, census_by_field, plan):
    """Restore only matching census evidence and its retained partition tree."""
    fields = {"unfiltered_pre", "unfiltered_post", "ranged_root_pre", "ranged_root_post", "failure"}
    if (
        not isinstance(census_by_field, dict)
        or plan is None
        or set(census_by_field) not in (fields, fields | {"leaf_sum"})
        or ("leaf_sum" in census_by_field and census_by_field["leaf_sum"] is None)
    ):
        raise ValueError("leaf_census_checkpoint_invalid")
    census = directory.LastUpdatedCompletenessCensus(
        **{field: census_by_field.get(field) for field in ("unfiltered_pre", "unfiltered_post", "failure")},
        leaf_sum=census_by_field.get("leaf_sum"),
    )
    if any(census_by_field.get(field) is not None for field in ("ranged_root_pre", "ranged_root_post", "capped")):
        raise ValueError("leaf_census_mixed_evidence")
    if census.failure is not None and (not isinstance(census.failure, str) or not census.failure):
        raise ValueError("leaf_census_failure_invalid")
    if census.leaf_sum is None:
        if any(census_by_field.get(field) is not None for field in ("unfiltered_pre", "unfiltered_post")) or any(
            window.count is not None or window.split_reason is not None for window in plan.windows.values()
        ):
            raise ValueError("leaf_census_missing")
    else:
        validate(census, plan)
    return census


def _request_hash(directory, state, config, window):
    return hashlib.sha256(directory._last_updated_partition_count_url(state, config, window).encode()).hexdigest()


def record_deferred_count(directory, config, state, window, count):
    """Save real 503 provenance before the normal retry result saves its boundary."""
    if config.census_mode == CONTRACT and count.error == "http_503" and count.retry_not_before is not None:
        parse_utc_instant(count.retry_not_before)
        state.census.leaf_sum["refinements"][window.window_id] = {
            "error": count.error,
            "request_sha256": _request_hash(directory, state, config, window),
            "retry_not_before": count.retry_not_before,
        }
        state.pages_fetched += count.pages_fetched


async def _resume_refinements(directory, resource, model, config, state):
    for window_id, evidence in state.census.leaf_sum["refinements"].items():
        window = state.plan.windows[window_id]
        if evidence["request_sha256"] != _request_hash(directory, state, config, window):
            return await shared._failure(directory, resource, model, config, state, "leaf_count_request_changed")
        if window.state is not WindowState.NEEDS_COUNT:
            continue
        if parse_utc_instant(evidence["retry_not_before"]) > datetime.datetime.now(datetime.UTC):
            return await directory._saved_partition_retry_result(
                model, resource, config, state, evidence["error"], retry_not_before=evidence["retry_not_before"]
            )
        state.plan.observe_count(window_id, CountObservation.split("planned"))
        await shared._save(directory, resource, config, state)
        if state.plan.failure is not None:
            return directory._partition_failure_from_state(model, state, "planned_split_failed")
    return None


async def pre_census(directory, source_record, resource, model, config, state, options):
    """Count the whole endpoint, then plan disjoint leaves without a root count."""
    base = directory._canonical_base(source_record.get("canonical_api_base") or source_record.get("api_base"))
    if not shared._is_unfiltered_start(directory, source_record, resource, state.start_url, base):
        return await shared._failure(directory, resource, model, config, state, "leaf_unfiltered_contract_required")
    if state.census.leaf_sum is None:
        state.census.leaf_sum = {"contract": CONTRACT, "count_pre": None, "count_post": None, "refinements": {}}
    await shared._check_cancel(directory, options)
    request = directory.LastUpdatedCensusRequest(
        "unfiltered_pre", directory._last_updated_partition_unfiltered_count_url(state.start_url), None
    )
    failure = await directory._observe_partition_census_value(
        source_record, resource, model, config, state, options, request
    )
    if failure is not None:
        return failure
    root = state.plan.windows["root"]
    if root.state is WindowState.NEEDS_COUNT and root.end - root.start >= 2 * config.minimum_width:
        state.plan.observe_count("root", CountObservation.split("planned"))
        await shared._save(directory, resource, config, state)
    return await _resume_refinements(directory, resource, model, config, state)


async def reconcile(directory, resource, model, config, state):
    """Freeze the exact sum only after it matches the unfiltered census."""
    total = sum(window.count or 0 for window in state.plan.leaf_windows())
    if total != state.census.unfiltered_pre or state.census.leaf_sum["count_pre"] not in (None, total):
        return await shared._failure(directory, resource, model, config, state, "leaf_census_pre_sum_mismatch")
    state.census.leaf_sum["count_pre"] = total
    await shared._save(directory, resource, config, state)
    return None


async def post_census(directory, source_record, resource, model, config, state, options):
    """Recount each unchanged leaf before an exact unfiltered post observation."""
    failure = await shared.recount_leaves(directory, source_record, resource, model, config, state, options)
    if failure is not None:
        return failure
    state.census.leaf_sum["count_post"] = state.census.leaf_sum["count_pre"]
    await shared._save(directory, resource, config, state)
    request = directory.LastUpdatedCensusRequest(
        "unfiltered_post",
        directory._last_updated_partition_unfiltered_count_url(state.start_url),
        state.census.unfiltered_pre,
    )
    return await directory._observe_partition_census_value(
        source_record, resource, model, config, state, options, request
    )


def has_recorded_stats(directory, resource_stats, diagnostic):
    """Record explicit mode provenance and refuse incomplete proof counters."""
    proof = diagnostic.get("leaf_sum_census") if isinstance(diagnostic, dict) else None
    if proof is None:
        return True
    resource_stats["last_updated_leaf_sum_sources"] = resource_stats.get("last_updated_leaf_sum_sources", 0) + 1
    census = directory.LastUpdatedCompletenessCensus(
        **{
            field: diagnostic.get(field)
            for field in ("unfiltered_pre", "unfiltered_post", "ranged_root_pre", "ranged_root_post", "failure")
        },
        leaf_sum=proof,
        capped=diagnostic.get("capped_search"),
    )
    if diagnostic.get("verified") is not True or not validate(census):
        return False
    key = "last_updated_leaf_sum_verified_sources"
    resource_stats[key] = resource_stats.get(key, 0) + 1
    for field in ("count_pre", "count_post"):
        key = "last_updated_leaf_sum_" + field
        resource_stats[key] = resource_stats.get(key, 0) + proof[field]
    return True
