# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Time-partition succession retains proof rows and the original bounded planner."""

import importlib
import json
from copy import deepcopy

import pytest

from process.provider_directory_generic_candidate import GenericCandidateError
from tests.test_provider_directory_generic_source_transition import _include_completed_empty_collection, _transition
from tests.test_provider_directory_generic_source_transition import case as case

directory = importlib.import_module("process.provider_directory_fhir")


def _partition_case(case, phase):
    config = directory._build_partition_config(
        dict(
            start="2024-01-01T00:00:00Z",
            end="2024-01-02T00:00:00Z",
            ceiling=10,
            minimum_width_seconds=1,
            boundary_precision_seconds=1,
            page_count=10,
        )
    )
    plan = directory._new_last_updated_partition_plan(config)
    if phase:
        plan.observe_count("root", directory.CountObservation.exact(3))
        plan.windows["root"].passes = {number: directory.WindowPass({}) for number in range(1, min(phase, 2) + 1)}
    census = directory.LastUpdatedCompletenessCensus()
    if phase == 3:
        census = directory.LastUpdatedCompletenessCensus(
            unfiltered_pre=3,
            ranged_root_pre=3,
            ranged_root_post=3,
            unfiltered_post=3,
        )
    candidate_count = 3 if phase >= 2 else 0
    counts_by_resource = {
        "Organization": candidate_count,
        "LU:Organization:pass:1": 3 if phase else 0,
        "LU:Organization:pass:2": 3 if phase >= 2 else 0,
    }
    case.input_rows[:] = [
        dict(dataset_id=case.header["dataset_id"], resource_type=name, total=count, active=count)
        for name, count in counts_by_resource.items()
        if count
    ]
    checkpoint = case.checkpoints[0]
    checkpoint.update(
        source_ids=case.header["candidate_storage_json"]["identity"]["metadata"]["source_ids"],
        source_scope_hash="b" * 64,
    )
    checkpoint.update(
        resource_type="Organization:LastUpdatedPartition",
        state="complete" if phase == 3 else "active",
        completed_at="now" if phase == 3 else None,
        rows_processed=sum(counts_by_resource.values()) - candidate_count,
        pages_processed=phase,
        start_url_hash=directory._last_updated_partition_start_hash("Organization", config),
        next_url=directory._last_updated_partition_checkpoint_payload(config, plan, census),
    )
    return counts_by_resource


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", [0, 1, 2, 3])
@pytest.mark.parametrize("status", ["acquiring", "failed"])
@pytest.mark.parametrize("run_status", ["failed", "canceled"])
async def test_partition_successor_preserves_counting_passes_and_post_census(case, phase, status, run_status):
    case.runs[0]["status"] = run_status
    counts_by_resource = _partition_case(case, phase)
    case.header["status"] = status
    before = deepcopy((case.header, case.checkpoints, case.input_rows, case.runs))
    receipt = await _transition(case, counts_by_resource)
    assert receipt["rows"] == sum(counts_by_resource.values()) and receipt["already_applied"] is False
    assert case.header == {
        **before[0],
        "candidate_storage_json": {**before[0]["candidate_storage_json"], "storage": case.new},
    }
    assert (case.checkpoints, case.input_rows, case.runs) == before[1:]
    assert (await _transition(case, counts_by_resource))["already_applied"] is True
    assert len(case.writes) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault",
    [
        "json",
        "version",
        "config",
        "duplicate",
        "pass_order",
        "failure",
        "complete",
        "missing_marker",
        "foreign_marker",
        "marker_count",
        "row_count",
        "candidate_count",
        "input_count",
    ],
)
@pytest.mark.parametrize("run_status", ["failed", "canceled"])
async def test_partition_successor_refuses_changed_or_incomplete_evidence(case, fault, run_status):
    case.runs[0]["status"] = run_status
    counts_by_resource = _partition_case(case, 2)
    checkpoint = case.checkpoints[0]
    checkpoint_payload = json.loads(checkpoint["next_url"])
    match fault:
        case "version":
            checkpoint_payload["strategy_version"] = "foreign"
        case "config":
            checkpoint_payload["config"]["ceiling"] = 20
        case "duplicate":
            checkpoint_payload["control"]["windows"].append(deepcopy(checkpoint_payload["control"]["windows"][0]))
        case "pass_order":
            checkpoint_payload["control"]["windows"][0]["completed_passes"] = [2]
        case "failure":
            checkpoint_payload["control"]["census"]["failure"] = "changed"
        case "complete":
            checkpoint.update(state="complete", completed_at="now")
        case "missing_marker":
            counts_by_resource.pop("LU:Organization:pass:1")
        case "foreign_marker":
            counts_by_resource["LU:Location:pass:1"] = 0
        case "marker_count":
            counts_by_resource["LU:Organization:pass:1"] = 2
        case "row_count":
            checkpoint["rows_processed"] = 5
        case "candidate_count":
            counts_by_resource["Organization"] = 4
        case "input_count":
            case.input_rows[0]["total"] = 4
    checkpoint["next_url"] = "{invalid" if fault == "json" else json.dumps(checkpoint_payload)
    before = deepcopy((case.header, case.checkpoints, case.input_rows, case.runs))
    with pytest.raises(GenericCandidateError, match="transition_"):
        await _transition(case, counts_by_resource)
    assert (case.header, case.checkpoints, case.input_rows, case.runs) == before
    assert case.writes == []


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["validated", "published", "ready_for_freeze", "disposed"])
async def test_partition_successor_refuses_sealed_or_disposed_candidate(case, status):
    counts_by_resource = _partition_case(case, 2)
    case.header["status"] = status
    with pytest.raises(GenericCandidateError, match="transition_candidate_changed"):
        await _transition(case, counts_by_resource)
    assert case.writes == []


@pytest.mark.asyncio
@pytest.mark.parametrize("capped", [False, True])
async def test_partition_successor_refuses_census_that_contradicts_leaf_counts(case, capped):
    counts_by_resource = _partition_case(case, 3)
    checkpoint = case.checkpoints[0]
    checkpoint_payload = json.loads(checkpoint["next_url"])
    if capped:
        fields = checkpoint_payload["config"]
        fields["versioned_search_limit"] = 50
        config = directory._build_partition_config(
            {**fields, "minimum_width_seconds": 1, "boundary_precision_seconds": 1}
        )
        checkpoint["start_url_hash"] = directory._last_updated_partition_start_hash("Organization", config)
        checkpoint_payload["control"]["census"].update(
            unfiltered_pre=None, ranged_root_pre=None, ranged_root_post=None, unfiltered_post=None
        )
        checkpoint_payload["control"]["census"]["capped"] = {
            "contract": directory.capped_partition.CONTRACT,
            "versioned": True,
            **dict.fromkeys(directory.capped_partition.COUNT_FIELDS, 0),
        }
    else:
        checkpoint_payload["control"]["census"].update(
            unfiltered_pre=0, ranged_root_pre=0, ranged_root_post=0, unfiltered_post=0
        )
    checkpoint["next_url"] = json.dumps(checkpoint_payload)
    with pytest.raises(GenericCandidateError, match="transition_partition_checkpoint_changed"):
        await _transition(case, counts_by_resource)
    assert case.writes == []


@pytest.mark.asyncio
async def test_partition_successor_refuses_lost_candidates_with_retained_twin_proofs(case):
    counts_by_resource = _partition_case(case, 2)
    counts_by_resource["Organization"] = 0
    case.input_rows[:] = [row for row in case.input_rows if row["resource_type"] != "Organization"]
    with pytest.raises(GenericCandidateError, match="transition_partition_input_changed"):
        await _transition(case, counts_by_resource)
    assert case.writes == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [("source_ids", ["foreign"]), ("source_scope_hash", "invalid"), ("canonical_api_base", "http://example.test/fhir")],
)
async def test_partition_successor_refuses_changed_source_context(case, field, value):
    counts_by_resource = _partition_case(case, 2)
    case.checkpoints[0][field] = value
    with pytest.raises(GenericCandidateError, match="transition_partition_checkpoint_changed"):
        await _transition(case, counts_by_resource)
    assert case.writes == []


@pytest.mark.asyncio
@pytest.mark.parametrize("run_status", ["failed", "canceled"])
async def test_partition_successor_keeps_completed_collection_and_pending_census(case, run_status):
    case.runs[0]["status"] = run_status
    counts_by_resource = _partition_case(case, 2)
    _include_completed_empty_collection(case)
    case.checkpoints[1].update(state="complete", completed_at="now", next_url=None, pages_processed=1)
    counts_by_resource["InsurancePlan"] = 0
    case.header["status"] = "failed"
    before = deepcopy((case.header, case.checkpoints, case.input_rows))
    receipt = await _transition(case, counts_by_resource)
    assert receipt["rows"] == 9
    assert case.header == {
        **before[0],
        "candidate_storage_json": {**before[0]["candidate_storage_json"], "storage": case.new},
    }
    assert (case.checkpoints, case.input_rows) == before[1:]
    assert case.checkpoints[0]["state"] == "active"
    assert case.checkpoints[1]["state"] == "complete"


@pytest.mark.asyncio
@pytest.mark.parametrize("ordinary_state", ["active", "complete"])
async def test_canceled_partition_successor_refuses_an_ordinary_cursor(case, ordinary_state):
    counts_by_resource = _partition_case(case, 2)
    _include_completed_empty_collection(case)
    case.checkpoints[1].update(
        state=ordinary_state,
        completed_at="now" if ordinary_state == "complete" else None,
        next_url="https://example.test/fhir/InsurancePlan?cursor=retained",
        pages_processed=1,
    )
    counts_by_resource["InsurancePlan"] = 0
    case.runs[0]["status"] = "canceled"
    before = deepcopy((case.header, case.checkpoints, case.input_rows, case.runs))
    with pytest.raises(GenericCandidateError, match="transition_checkpoint_changed"):
        await _transition(case, counts_by_resource)
    assert (case.header, case.checkpoints, case.input_rows, case.runs) == before
    assert case.writes == []
    case.runs[0]["status"] = "failed"
    assert (await _transition(case, counts_by_resource))["rows"] == 9


@pytest.mark.asyncio
async def test_partition_successor_refuses_a_running_capture_even_with_valid_proofs(case):
    counts_by_resource = _partition_case(case, 2)
    case.runs[0]["status"] = "running"
    with pytest.raises(GenericCandidateError, match="transition_run_changed"):
        await _transition(case, counts_by_resource)
    assert case.writes == []


def _record_failed_candidate_metadata(case):
    identity = case.header["candidate_storage_json"]["identity"]
    candidate = directory.EndpointDatasetCandidate(
        dataset_id=identity["dataset_id"],
        endpoint_id=identity["endpoint_id"],
        acquisition_root_run_id=identity["acquisition_root_run_id"],
        source_ids=tuple(identity["metadata"]["source_ids"]),
        selected_resources=("Organization",),
        expected_resources=("Organization",),
        import_run_id=case.header["import_run_id"],
        previous_dataset_id=None,
        reused_from_checkpoint=True,
    )
    identity["metadata"] = directory._endpoint_dataset_candidate_metadata(candidate)
    case.header.update(
        status="failed",
        publication_metadata_json=directory._endpoint_dataset_publication_metadata(
            candidate,
            {"Organization": {"complete": False, "error": "bounded failure", "next_url_remaining": True}},
            error="bounded failure",
        ),
    )


@pytest.mark.asyncio
async def test_partition_successor_retains_the_real_failure_metadata_and_replays(case):
    counts_by_resource = _partition_case(case, 2)
    _record_failed_candidate_metadata(case)
    before = deepcopy((case.header, case.checkpoints, case.input_rows, case.runs))
    await _transition(case, counts_by_resource)
    assert case.header == {
        **before[0],
        "candidate_storage_json": {**before[0]["candidate_storage_json"], "storage": case.new},
    }
    assert (case.checkpoints, case.input_rows, case.runs) == before[1:]
    assert (await _transition(case, counts_by_resource))["already_applied"] is True
    assert len(case.writes) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["identity", "missing_nullable_identity", "non_object", "acquiring"])
async def test_partition_successor_rejects_failure_metadata_identity_drift(case, fault):
    counts_by_resource = _partition_case(case, 2)
    _record_failed_candidate_metadata(case)
    metadata = case.header["publication_metadata_json"]
    match fault:
        case "identity":
            metadata["source_ids"] = ["foreign"]
        case "missing_nullable_identity":
            assert metadata[directory.TWIN_ROOT_VERIFICATION_CAMPAIGN_KEY] is None
            del metadata[directory.TWIN_ROOT_VERIFICATION_CAMPAIGN_KEY]
        case "non_object":
            case.header["publication_metadata_json"] = []
        case "acquiring":
            case.header["status"] = "acquiring"
    before = deepcopy((case.header, case.checkpoints, case.input_rows, case.runs))
    with pytest.raises(GenericCandidateError, match="transition_candidate_changed"):
        await _transition(case, counts_by_resource)
    assert (case.header, case.checkpoints, case.input_rows, case.runs) == before
    assert not case.writes
