# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retain exact time-partition evidence when changing an isolated Source producer."""

import json
import re
from importlib import import_module

from process.provider_directory_generic_source_storage import _is_resumable_transition_checkpoint, _json, _require


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
    return resume


def _verify_partition(directory, checkpoint, resource, counts):
    """Compare committed pass markers and candidate rows without declaring completion."""
    try:
        resume = _resume(directory, checkpoint, resource)
    except KeyError, TypeError, ValueError, RuntimeError:
        _require(False, "transition_partition_checkpoint_invalid")
    plan, census = resume.plan, resume.census
    expected_count = directory.capped_partition.expected_count(census)
    _require(
        plan.pending_count_windows()
        or expected_count is None
        or expected_count == sum(window.count or 0 for window in plan.leaf_windows()),
        "transition_partition_checkpoint_changed",
    )
    is_complete = (
        plan.status is directory.PlanStatus.SUCCEEDED and directory._is_last_updated_partition_census_complete(census)
    )
    _require(
        plan.failure is None
        and census.failure is None
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


def _verify_ordinary_companion(checkpoint, count, run_status):
    _require(
        _is_resumable_transition_checkpoint(checkpoint)
        and (run_status == "failed" or (checkpoint["state"] == "complete" and not checkpoint["next_url"]))
        and checkpoint["pages_processed"] > 0
        and checkpoint["rows_processed"] == count,
        "transition_checkpoint_changed",
    )


def has_verified_partition_checkpoints(checkpoints, identity, counts, source_ids, run_status):
    """Select only the existing planner family; ordinary and bulk keep their guards."""
    suffix = ":LastUpdatedPartition"
    if not any(checkpoint_record["resource_type"].endswith(suffix) for checkpoint_record in checkpoints):
        return False
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
    for checkpoint_record in checkpoints:
        _require(
            checkpoint_record["owner_run_id"] == identity["failed_run_id"]
            and _json(checkpoint_record["source_ids"]) == source_ids
            and type(checkpoint_record["source_scope_hash"]) is str
            and re.fullmatch(r"[0-9a-f]{64}", checkpoint_record["source_scope_hash"])
            and directory._is_safe_bulk_https_url(checkpoint_record["canonical_api_base"])
            and directory._canonical_base(checkpoint_record["canonical_api_base"])
            == checkpoint_record["canonical_api_base"]
            and checkpoint_record["dataset_id"] == identity["dataset_id"]
            and checkpoint_record["acquisition_root_run_id"] == identity["acquisition_root_run_id"]
            and type(checkpoint_record["pages_processed"]) is int
            and checkpoint_record["pages_processed"] >= 0
            and type(checkpoint_record["rows_processed"]) is int
            and checkpoint_record["rows_processed"] >= 0,
            "transition_partition_checkpoint_changed",
        )
        resource = checkpoint_record["resource_type"].removesuffix(suffix)
        if resource in partitioned_resources:
            _verify_partition(directory, checkpoint_record, resource, counts)
        else:
            _verify_ordinary_companion(checkpoint_record, counts[resource], run_status)
    return True
