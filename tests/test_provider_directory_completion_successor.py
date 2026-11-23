# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""A failed no-fetch completion preserves its ordinary ancestor's checkpoint ownership."""

import importlib
from copy import deepcopy
from unittest.mock import Mock

import pytest

from db.models import ProviderDirectoryAPIEndpoint
from process.import_completion_policy import IMPORT_COMPLETION_POLICY_ID
from process.provider_directory_generic_candidate import GenericCandidateError
from tests.test_provider_directory_generic_source_transition import (
    DATASET,
    ENDPOINT,
    FAILED,
    ROOT,
    _include_active_collection,
    _transition,
)
from tests.test_provider_directory_generic_source_transition import case as case

BASE = "https://example.test/fhir"
SOURCE_IDS = ["synthetic-source"]
INTERMEDIATE = "run_" + "c" * 32
OBSERVATIONS = {"Organization": 3, "Location": 2}
RETAINED = {"Organization": 2, "Location": 2}


def _native_control_rows(case, monkeypatch):
    original_row = case.connection.fetchrow.side_effect
    original_value = case.connection.fetchval.side_effect
    case.control_reads, case.hidden_runs, case.endpoint_base = [], set(), BASE

    async def row(sql, *args):
        if 'FROM "catalog_schema"."import_run"' not in sql:
            return original_row(sql, *args)
        assert "FOR SHARE NOWAIT" in sql
        assert "engine='healthcare-mrf-api' AND importer='provider-directory-fhir'" in sql
        case.control_reads.append(args[0])
        return next(
            (deepcopy(run) for run in case.runs if run["run_id"] == args[0] and args[0] not in case.hidden_runs),
            None,
        )

    def value(sql, *args):
        if '"' + ProviderDirectoryAPIEndpoint.__tablename__ + '"' in sql:
            assert args == (ENDPOINT,) and "WHERE endpoint_id=$1 FOR SHARE NOWAIT" in sql
            return case.endpoint_base
        return original_value(sql, *args)

    monkeypatch.setattr(case.connection.fetchrow, "side_effect", row)
    monkeypatch.setattr(case.connection.fetchval, "side_effect", value)


def _duplicate_observation_cursor(case):
    original = case.connection.cursor
    directory = importlib.import_module("process.provider_directory_fhir")
    case.observed_resource_keys = []

    async def cursor(sql, *args, prefetch):
        async for resource in original(sql, *args, prefetch=prefetch):
            if resource["resource_type"] == "Organization" and resource["resource_id"] == "2":
                payload = {"resourceType": "Organization", "id": "0"}
                resource.update(
                    resource_id="0",
                    payload_json=payload,
                    payload_hash=directory.resource_payload_sha256_for_contract(
                        payload, directory.LEGACY_RESOURCE_HASH_CONTRACT, resource_type="Organization"
                    ),
                )
            case.observed_resource_keys.append((resource["resource_type"], resource["resource_id"]))
            yield resource

    return cursor


@pytest.fixture
def completion_case(case, monkeypatch):
    _include_active_collection(case)
    metadata = case.header["candidate_storage_json"]["identity"]["metadata"]
    metadata["source_ids"] = SOURCE_IDS
    case.header["publication_metadata_json"] = deepcopy(metadata)
    for run in case.runs:
        run["source_ids"] = SOURCE_IDS
        run["params"] = {
            "source_ids": SOURCE_IDS,
            "provider_directory_pagination_root_run_id": ROOT,
            "provider_directory_endpoint_scope": BASE,
            "allow_retained_pagination_reset": False,
            "retry_of_run_id": run["retry_of_run_id"],
        }
        run["metrics"] = {"resource_rows": OBSERVATIONS, "resource_fetch_stats": {}}
    case.runs[0]["params"]["import_completion"] = {
        "policy_id": IMPORT_COMPLETION_POLICY_ID,
        "candidate_id": DATASET,
        "acquisition_root_run_id": ROOT,
        "retry_of_run_id": ROOT,
        "retained_counts": RETAINED,
        "expected_counts": {"Organization": None, "Location": 2},
        "accept_unknown_coverage": True,
    }
    case.runs[0]["metrics"] = {"resource_rows": {}, "resource_fetch_stats": {}}
    for checkpoint in case.checkpoints:
        checkpoint.update(owner_run_id=ROOT, source_ids=SOURCE_IDS, source_scope_hash="a" * 64)
    _native_control_rows(case, monkeypatch)
    case.connection.cursor = _duplicate_observation_cursor(case)
    directory = importlib.import_module("process.provider_directory_fhir")
    monkeypatch.setattr(directory.db, "transaction", Mock(side_effect=AssertionError("use native admin transaction")))
    return case


def _add_completion_parent(case):
    ancestor = deepcopy(case.runs[0])
    ancestor["run_id"] = INTERMEDIATE
    case.runs.insert(1, ancestor)
    latest = case.runs[0]
    latest["retry_of_run_id"] = latest["params"]["retry_of_run_id"] = INTERMEDIATE
    latest["params"]["import_completion"]["retry_of_run_id"] = INTERMEDIATE


@pytest.mark.asyncio
@pytest.mark.parametrize("hops", [1, 2])
async def test_completion_successor_preserves_ordinary_owner_and_observation_census(completion_case, hops):
    case = completion_case
    if hops == 2:
        _add_completion_parent(case)
    before = deepcopy(case.checkpoints)
    result = await _transition(case, OBSERVATIONS)
    assert result["rows"] == sum(OBSERVATIONS.values()) > sum(RETAINED.values())
    assert len(case.observed_resource_keys) == sum(OBSERVATIONS.values())
    assert len(set(case.observed_resource_keys)) == sum(RETAINED.values())
    assert case.header["import_run_id"] == FAILED and case.checkpoints == before
    assert case.control_reads == ([FAILED, ROOT] if hops == 1 else [FAILED, INTERMEDIATE, ROOT])


def _change_no_fetch_evidence(metrics, damage):
    match damage:
        case "rows-written":
            metrics["resource_rows"] = {"Organization": 1}
        case "rows-missing":
            del metrics["resource_rows"]
        case "fetch-evidence":
            metrics["resource_fetch_stats"] = {"Organization": {"requests": 1}}
        case "fetch-missing":
            del metrics["resource_fetch_stats"]
        case "handoff":
            metrics["source_candidate_handoffs"] = {DATASET: {"document": {}}}


def _change_completion_evidence(case, damage):
    params, metrics = case.runs[0]["params"], case.runs[0]["metrics"]
    request = params["import_completion"]
    match damage:
        case "missing-row":
            case.hidden_runs.add(ROOT)
        case "missing-request":
            del params["import_completion"]
        case "malformed-request":
            params["import_completion"] = []
        case "wrong-dataset" | "wrong-root" | "wrong-resources":
            field = {"wrong-dataset": "candidate_id", "wrong-root": "acquisition_root_run_id"}.get(
                damage, "retained_counts"
            )
            request[field] = {"Other": 1} if damage == "wrong-resources" else "changed"
        case "wrong-source":
            params["source_ids"] = ["other-source"]
        case "wrong-endpoint-param":
            params["provider_directory_endpoint_scope"] = "https://other.test/fhir"
        case "wrong-endpoint-row":
            case.endpoint_base = "https://other.test/fhir"
        case "wrong-checkpoint-source":
            case.checkpoints[0]["source_ids"] = ["other-source"]
        case "reset":
            params["allow_retained_pagination_reset"] = True
        case "parent":
            params["retry_of_run_id"] = FAILED
        case "mixed-owner":
            case.checkpoints[0]["owner_run_id"] = FAILED
        case "alternate-owner":
            for checkpoint in case.checkpoints:
                checkpoint["owner_run_id"] = INTERMEDIATE
        case "cycle":
            case.runs[0]["retry_of_run_id"] = FAILED
        case _:
            _change_no_fetch_evidence(metrics, damage)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "damage",
    [
        "missing-row",
        "missing-request",
        "malformed-request",
        "wrong-dataset",
        "wrong-root",
        "wrong-resources",
        "wrong-source",
        "wrong-endpoint-param",
        "wrong-endpoint-row",
        "wrong-checkpoint-source",
        "rows-written",
        "rows-missing",
        "fetch-evidence",
        "fetch-missing",
        "handoff",
        "reset",
        "parent",
        "mixed-owner",
        "alternate-owner",
        "cycle",
    ],
)
async def test_completion_successor_refuses_changed_evidence_without_writing(completion_case, damage):
    case = completion_case
    _change_completion_evidence(case, damage)
    before = deepcopy((case.header, case.checkpoints, case.input_rows))
    with pytest.raises(GenericCandidateError):
        await _transition(case, OBSERVATIONS)
    assert not case.writes and (case.header, case.checkpoints, case.input_rows) == before


@pytest.mark.asyncio
async def test_completion_successor_does_not_skip_a_changed_historical_request(completion_case):
    _add_completion_parent(completion_case)
    completion_case.runs[1]["params"]["import_completion"]["retained_counts"] = {"Organization": 1, "Location": 2}
    with pytest.raises(GenericCandidateError):
        await _transition(completion_case, OBSERVATIONS)
    assert not completion_case.writes


@pytest.mark.asyncio
async def test_completion_successor_stops_at_first_ordinary_acquisition(completion_case):
    _add_completion_parent(completion_case)
    del completion_case.runs[1]["params"]["import_completion"]
    with pytest.raises(GenericCandidateError, match="transition_checkpoint_changed"):
        await _transition(completion_case, OBSERVATIONS)
    assert completion_case.control_reads == [FAILED, INTERMEDIATE] and not completion_case.writes


@pytest.mark.asyncio
async def test_matching_owner_keeps_ordinary_transition_without_new_control_reads(case):
    result = await _transition(case)
    assert result["rows"] == 3
    assert not any('"import_run"' in call.args[0] for call in case.connection.fetchrow.await_args_list)
    assert not any(
        ProviderDirectoryAPIEndpoint.__tablename__ in call.args[0] for call in case.connection.fetchval.await_args_list
    )
