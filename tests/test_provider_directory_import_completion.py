# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Partial acceptance remains bound to its native publication and exact diagnostics."""

from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.import_completion_policy import IMPORT_COMPLETION_POLICY_ID, completion_receipt
from process.provider_directory_generic_source_storage import (
    handoff_header,
    validate_handoff_completion,
    validate_handoff_header,
)
from process.provider_directory_import_completion import require_candidate_request, validate_metadata


@pytest.fixture
def partial_header():
    request_by_field = {
        "policy_id": IMPORT_COMPLETION_POLICY_ID,
        "candidate_id": "example-candidate",
        "acquisition_root_run_id": "example-root",
        "retry_of_run_id": "example-parent",
        "retained_counts": {"Organization": 199, "Location": 2},
        "expected_counts": {"Organization": None, "Location": 2},
        "accept_unknown_coverage": True,
    }
    receipt_by_field = completion_receipt(
        request_by_field,
        retained_counts=request_by_field["retained_counts"],
        content_hash="a" * 64,
        incomplete_partitions=["Organization"],
        retry_evidence={"Organization": {"retries": 2, "exhausted": False, "operator_stopped": True}},
    )
    return {
        "dataset_id": "example-candidate",
        "acquisition_root_run_id": "example-root",
        "dataset_hash": "a" * 64,
        "resource_count": 201,
        "previous_dataset_id": None,
        "completion_proof_required_version": None,
        "completion_proof_sha256": None,
        "content_proof_admission_sha256": "b" * 64,
        "publication_metadata_json": {
            "selected_resources": ["Location", "Organization"],
            "resource_diagnostics": {
                "Organization": {"complete": False, "error": "source_input_unavailable"},
                "Location": {"complete": True, "error": None},
            },
            "import_completion": receipt_by_field,
        },
    }


def test_receipt_is_part_of_logical_handoff(partial_header):
    admission = handoff_header(partial_header)
    assert admission["import_completion"] == partial_header["publication_metadata_json"]["import_completion"]
    validate_handoff_header(admission)
    assert validate_metadata(partial_header, retained_counts={"Organization": 199, "Location": 2}) == {"Organization"}
    admission["import_completion"]["decisions"]["Organization"]["disposition"] = "complete"
    with pytest.raises(ValueError, match="receipt_changed"):
        validate_handoff_header(admission)


@pytest.mark.parametrize("change", ["drop_receipt", "drop_diagnostic", "wrong_type", "fake_complete", "wrong_root"])
def test_partial_header_cannot_lose_coverage_evidence(partial_header, change):
    metadata = partial_header["publication_metadata_json"]
    if change == "drop_receipt":
        del metadata["import_completion"]
    elif change == "drop_diagnostic":
        del metadata["resource_diagnostics"]["Organization"]
    elif change == "wrong_type":
        metadata["resource_diagnostics"]["Location"] = None
    elif change == "fake_complete":
        metadata["resource_diagnostics"]["Organization"]["complete"] = True
    else:
        partial_header["acquisition_root_run_id"] = "different-root"
    with pytest.raises(ValueError):
        validate_metadata(partial_header)


def test_replay_requires_same_authorized_request(partial_header):
    metadata = partial_header["publication_metadata_json"]
    metadata["dataset_hash"] = partial_header["dataset_hash"]
    candidate = SimpleNamespace(
        dataset_id=partial_header["dataset_id"],
        acquisition_root_run_id=partial_header["acquisition_root_run_id"],
        selected_resources=("Location", "Organization"),
        already_validated=True,
        already_published=False,
        validated_metadata=metadata,
        published_metadata=None,
    )
    request_by_field = metadata["import_completion"]["request"]
    require_candidate_request(candidate, request_by_field)
    with pytest.raises(ValueError, match="replay_request_missing"):
        require_candidate_request(candidate, None)
    require_candidate_request(None, None)
    require_candidate_request(SimpleNamespace(already_validated=True, validated_metadata={}), None)
    changed = deepcopy(request_by_field)
    changed["retained_counts"]["Location"] = 1
    with pytest.raises(ValueError, match="replay_request_changed"):
        require_candidate_request(candidate, changed)
    changed["candidate_id"] = "different-candidate"
    with pytest.raises(ValueError, match="scope_changed"):
        require_candidate_request(candidate, changed)


def test_partial_timestamp_traversal_keeps_incomplete_receipt(partial_header):
    from process import provider_directory_timestamp_recovery as recovery

    partial_header["completion_proof_json"] = None
    diagnostic_by_field = partial_header["publication_metadata_json"]["resource_diagnostics"]["Organization"]
    diagnostic_by_field.update(
        fetch_mode=recovery.FETCH_MODE,
        next_url_remaining=True,
        **{recovery.KEY: {"version": 1, "complete": False}},
    )
    validate_handoff_completion(partial_header)
    assert diagnostic_by_field[recovery.KEY]["complete"] is False
    assert recovery.diagnostic_is_complete(diagnostic_by_field) is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "counts",
    [
        {"Organization": 199, "Location": 2},
        {"Organization": 198, "Location": 3},
        {"Organization": 199, "Location": 2, "Practitioner": 1},
    ],
)
async def test_partial_handoff_counts_the_bound_candidate_before_publication(partial_header, counts):
    from process import provider_directory_generic_source_completion as completion

    binding_by_field = {
        "identity": {"dataset_id": partial_header["dataset_id"]},
        "tables": {completion.storage.RESOURCE: {"schema": "candidate_schema", "name": "candidate_resources"}},
    }
    partial_header.update(status="validated", is_current=False, candidate_storage_json=binding_by_field)
    connection = SimpleNamespace(
        fetchrow=AsyncMock(return_value=partial_header),
        fetch=AsyncMock(
            return_value=[{"resource_type": family, "retained_count": total} for family, total in counts.items()]
        ),
    )
    publisher = SimpleNamespace(connection=connection, policy=SimpleNamespace(schema="catalog_schema"))
    if counts == partial_header["publication_metadata_json"]["import_completion"]["request"]["retained_counts"]:
        assert await completion._validated_header(publisher, binding_by_field) == partial_header
    else:
        with pytest.raises(ValueError, match="import_completion"):
            await completion._validated_header(publisher, binding_by_field)
    sql, dataset_id = connection.fetch.await_args.args
    assert '"candidate_schema"."candidate_resources"' in sql
    assert "WHERE dataset_id=$1 GROUP BY resource_type" in sql and dataset_id == partial_header["dataset_id"]
