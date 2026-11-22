# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retain exact time-partition evidence when changing an isolated Source producer."""

import asyncio
import json
import re
from dataclasses import replace
from importlib import import_module

from db.plan_release_storage import storage_relation
from process.provider_directory_generic_source_storage import _is_resumable_transition_checkpoint, _json, _require
from process.provider_directory_time_partition import CountKind

_PRIOR_PLAN = "pre_data_census_renewal"


def _resume(directory, checkpoint, resource):
    """Reuse acquisition's strict config, plan and census decoders."""
    fields = json.loads(checkpoint["next_url"])["config"]
    _require(
        all(type(fields[key]) is int for key in ("minimum_width_microseconds", "boundary_precision_microseconds")),
        "transition_partition_checkpoint_changed",
    )
    config = directory._build_partition_config(
        {
            **fields,
            "minimum_width_seconds": fields["minimum_width_microseconds"] / 1_000_000,
            "boundary_precision_seconds": fields["boundary_precision_microseconds"] / 1_000_000,
        }
    )
    _require(
        config.identity() == fields
        and directory._last_updated_partition_start_hash(resource, config) == checkpoint["start_url_hash"],
        "transition_partition_checkpoint_changed",
    )
    resume = directory._last_updated_partition_resume_from_checkpoint(checkpoint["next_url"], config)
    root = resume.plan.windows["root"]
    _require(
        root.start == config.start and (config.end is None or root.end == config.end),
        "transition_partition_checkpoint_changed",
    )
    return config, resume


def _can_renew_census(directory, checkpoint, config, resume):
    """Keep renewal confined to Minnesota censuses with no committed passes."""
    plan, census = resume.plan, resume.census
    root = plan.windows["root"]
    return (
        checkpoint["canonical_api_base"] == directory.bcbs_minnesota.API_BASE
        and checkpoint["state"] == "active"
        and checkpoint["completed_at"] is None
        and checkpoint["rows_processed"] == 0
        and _json(checkpoint["completeness_json"]) == {}
        and config.end is None
        and config.census_mode is None
        and config.versioned_search_limit is None
        and all(not window.passes for window in plan.windows.values())
        and census.failure is None
        and census.unfiltered_pre == census.ranged_root_pre == root.count
        and root.count is not None
        and census.ranged_root_post is None
        and census.unfiltered_post is None
    )


def _renewed_partition_payload(directory, checkpoint, config, plan):
    """Keep the checkpoint identity and choose a new durable root."""
    new_plan = directory._new_last_updated_partition_plan(config, canonical_api_base=checkpoint["canonical_api_base"])
    if new_plan.windows["root"].end <= plan.windows["root"].end:
        return None
    return directory._last_updated_partition_checkpoint_payload(config, new_plan)


def _renew_highmark_pre_request_failure(directory, checkpoint, resource, config, resume):
    """Recover a pristine Highmark role plan rejected by the old anonymous-only guard."""
    base = checkpoint["canonical_api_base"]
    failure = directory.leaf_census.UNFILTERED_CONTRACT_REQUIRED
    if (
        resource != "PractitionerRole"
        or base not in directory.highmark.CHUNKED_API_BASES
        or config.census_mode != directory.leaf_census.CONTRACT
        or config.end is not None
        or resume.census.failure != failure
        or checkpoint["state"] != "active"
        or checkpoint["completed_at"] is not None
        or checkpoint["pages_processed"] != 0
        or checkpoint["rows_processed"] != 0
        or _json(checkpoint["completeness_json"]) != {}
    ):
        return None
    original_cutoff = resume.plan.windows["root"].end
    pristine_plan = directory._new_last_updated_partition_plan(replace(config, end=original_cutoff))
    rejected_census = directory.LastUpdatedCompletenessCensus(failure=failure)
    if checkpoint["next_url"] != directory._last_updated_partition_checkpoint_payload(
        config, pristine_plan, rejected_census
    ):
        return None
    return _renewed_partition_payload(directory, checkpoint, config, resume.plan)


def _verify_partition(directory, checkpoint, resource, counts):
    """Compare committed pass markers and candidate rows without declaring completion."""
    try:
        config, resume = _resume(directory, checkpoint, resource)
    except KeyError, TypeError, ValueError, RuntimeError:
        _require(False, "transition_partition_checkpoint_invalid")
    plan, census = resume.plan, resume.census
    can_renew = _can_renew_census(directory, checkpoint, config, resume)
    renewed_payload = _renew_highmark_pre_request_failure(directory, checkpoint, resource, config, resume)
    if can_renew and plan.failure is not None and plan.failure.code == "split_count_mismatch":
        renewed_payload = _renewed_partition_payload(directory, checkpoint, config, plan)
    expected_count = directory.capped_partition.expected_count(census)
    _require(
        renewed_payload is not None
        or plan.pending_count_windows()
        or expected_count is None
        or expected_count == sum(window.count or 0 for window in plan.leaf_windows()),
        "transition_partition_checkpoint_changed",
    )
    is_complete = (
        plan.status is directory.PlanStatus.SUCCEEDED and directory._is_last_updated_partition_census_complete(census)
    )
    _require(
        ((plan.failure is None and census.failure is None) or renewed_payload is not None)
        and checkpoint["state"] in {"active", "complete"}
        and (checkpoint["state"] == "complete") == is_complete
        and (checkpoint["completed_at"] is not None) == is_complete,
        "transition_partition_checkpoint_changed",
    )
    pass_counts = [
        sum(window.count or 0 for window in plan.leaf_windows() if number in window.passes) for number in (1, 2)
    ]
    _require(
        all(
            counts.get(directory._last_updated_partition_fingerprint_resource_type(resource, number)) == count
            for number, count in enumerate(pass_counts, 1)
        )
        and checkpoint["rows_processed"] == sum(pass_counts)
        and sum(bool(window.count) and 2 in window.passes for window in plan.leaf_windows())
        <= counts[resource]
        <= pass_counts[1],
        "transition_partition_input_changed",
    )
    return config, resume, can_renew, renewed_payload


async def _observe_pre_data_drift(directory, resource, checkpoint, config, plan, source_id):
    """Confirm drift with a bounded exact count before retiring an unfinished plan."""
    base = directory.bcbs_minnesota.API_BASE
    root = plan.windows["root"]
    url = directory._last_updated_partition_url(
        f"{base}/{resource}", root, page_count=config.page_count, count_only=True
    )
    try:
        async with asyncio.timeout(12):
            fetched = await directory._fetch_last_updated_partition_count(
                {"api_base": base, "canonical_api_base": base, "source_id": source_id}, url, timeout=10
            )
    except TimeoutError:
        return None
    observation = fetched.observation
    if (
        fetched.error
        or observation is None
        or observation.kind is not CountKind.EXACT
        or observation.count == root.count
    ):
        return None
    renewed_payload = _renewed_partition_payload(directory, checkpoint, config, plan)
    if renewed_payload is None:
        return None
    return resource, checkpoint, renewed_payload, observation.count


async def renew_verified_partition_checkpoint(connection, schema, checkpoint, renewed_payload, observed_count):
    """Archive the exact old plan and install its successor in the same transaction."""
    table = storage_relation(schema, "provider_directory_pagination_checkpoint")
    prior_plan_by_field = {
        "checkpoint": checkpoint["next_url"],
        "owner_run_id": checkpoint["owner_run_id"],
        "pages_processed": checkpoint["pages_processed"],
    }
    if observed_count is not None:
        prior_plan_by_field["observed_ranged_count"] = observed_count
    updated = await connection.execute(
        f"UPDATE {table} SET next_url=$1, updated_at=now(), completeness_json=$2::jsonb "
        "WHERE canonical_api_base=$3 AND resource_type=$4 AND source_scope_hash=$5 "
        "AND acquisition_root_run_id=$6 AND dataset_id=$7 AND owner_run_id=$8 "
        "AND next_url=$9 AND state='active' AND completed_at IS NULL "
        "AND pages_processed=$10 AND rows_processed=0 AND completeness_json::jsonb='{}'::jsonb",
        renewed_payload,
        json.dumps({_PRIOR_PLAN: prior_plan_by_field}),
        checkpoint["canonical_api_base"],
        checkpoint["resource_type"],
        checkpoint["source_scope_hash"],
        checkpoint["acquisition_root_run_id"],
        checkpoint["dataset_id"],
        checkpoint["owner_run_id"],
        checkpoint["next_url"],
        checkpoint["pages_processed"],
    )
    _require(updated == "UPDATE 1", "transition_partition_checkpoint_changed")


def _verify_ordinary_companion(checkpoint, count, run_status):
    _require(
        _is_resumable_transition_checkpoint(checkpoint)
        and (run_status == "failed" or (checkpoint["state"] == "complete" and not checkpoint["next_url"]))
        and checkpoint["pages_processed"] > 0
        and checkpoint["rows_processed"] == count,
        "transition_checkpoint_changed",
    )


def _verify_checkpoint_identity(directory, checkpoint, identity, source_ids):
    """Bind the checkpoint to the exact stopped acquisition and source."""
    _require(
        checkpoint["owner_run_id"] == identity["failed_run_id"]
        and _json(checkpoint["source_ids"]) == source_ids
        and type(checkpoint["source_scope_hash"]) is str
        and re.fullmatch(r"[0-9a-f]{64}", checkpoint["source_scope_hash"])
        and directory._is_safe_bulk_https_url(checkpoint["canonical_api_base"])
        and directory._canonical_base(checkpoint["canonical_api_base"]) == checkpoint["canonical_api_base"]
        and checkpoint["dataset_id"] == identity["dataset_id"]
        and checkpoint["acquisition_root_run_id"] == identity["acquisition_root_run_id"]
        and type(checkpoint["pages_processed"]) is int
        and checkpoint["pages_processed"] >= 0
        and type(checkpoint["rows_processed"]) is int
        and checkpoint["rows_processed"] >= 0,
        "transition_partition_checkpoint_changed",
    )


async def partition_checkpoint_renewals(checkpoints, identity, counts, source_ids, run_status):
    """Verify the partitioned capture and return renewals, or None for other modes."""
    suffix = ":LastUpdatedPartition"
    if not any(checkpoint_record["resource_type"].endswith(suffix) for checkpoint_record in checkpoints):
        return None
    _require(run_status in {"failed", "canceled"}, "transition_run_changed")
    directory = import_module("process.provider_directory_fhir")
    resources = {name for name in counts if not name.startswith("LU:")}
    partitioned_resources = {
        checkpoint_record["resource_type"].removesuffix(suffix)
        for checkpoint_record in checkpoints
        if checkpoint_record["resource_type"].endswith(suffix)
    }
    _require(
        len(checkpoints) == len(resources)
        and len(
            {
                (checkpoint_record["canonical_api_base"], checkpoint_record["source_scope_hash"])
                for checkpoint_record in checkpoints
            }
        )
        == 1
        and {checkpoint_record["resource_type"].removesuffix(suffix) for checkpoint_record in checkpoints} == resources
        and {name for name in counts if name.startswith("LU:")}
        == {
            directory._last_updated_partition_fingerprint_resource_type(resource, number)
            for resource in partitioned_resources
            for number in (1, 2)
        },
        "transition_partition_checkpoint_changed",
    )
    renewals = []
    count_checks = []
    for checkpoint_record in checkpoints:
        _verify_checkpoint_identity(directory, checkpoint_record, identity, source_ids)
        resource = checkpoint_record["resource_type"].removesuffix(suffix)
        if resource in partitioned_resources:
            config, resume, can_renew, renewed_payload = _verify_partition(
                directory, checkpoint_record, resource, counts
            )
            if renewed_payload is not None:
                renewals.append((resource, checkpoint_record, renewed_payload, None))
            elif can_renew and len(source_ids) == 1 and resource in directory.PUBLIC_DIRECTORY_SEVEN_RESOURCES:
                count_checks.append((resource, checkpoint_record, config, resume.plan))
        else:
            _verify_ordinary_companion(checkpoint_record, counts[resource], run_status)
    checked = await asyncio.gather(
        *(
            _observe_pre_data_drift(directory, resource, checkpoint, config, plan, source_ids[0])
            for resource, checkpoint, config, plan in count_checks
        )
    )
    renewals.extend(renewal for renewal in checked if renewal is not None)
    return renewals
