# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Partial acceptance remains bound to its native publication and exact diagnostics."""

from contextlib import asynccontextmanager
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


def _control_attempt_rows(request_by_field):
    """Represent one acquisition and its failed, data-free finalization child."""
    params_by_name = {
        "source_ids": ["example-source"],
        "provider_directory_pagination_root_run_id": "example-root",
        "provider_directory_endpoint_scope": "https://example.invalid/fhir",
        "allow_retained_pagination_reset": False,
    }
    acquisition_by_field = {
        "run_id": "example-acquisition",
        "retry_of_run_id": None,
        "status": "failed",
        "finished_at": "2026-01-01T00:00:00Z",
        "params": deepcopy(params_by_name),
        "metrics": {"audit": {"resource_fetch_stats": {"Organization": {"pagination_cooldown_retries": 4}}}},
    }
    failed_by_field = {
        **acquisition_by_field,
        "run_id": request_by_field["retry_of_run_id"],
        "retry_of_run_id": acquisition_by_field["run_id"],
        "params": {
            **params_by_name,
            "retry_of_run_id": acquisition_by_field["run_id"],
            "import_completion": {**deepcopy(request_by_field), "retry_of_run_id": acquisition_by_field["run_id"]},
        },
        "metrics": {"audit": {"resource_rows": {}, "resource_fetch_stats": {}}},
    }
    row_by_run_id = {
        attempt_by_field["run_id"]: attempt_by_field for attempt_by_field in (failed_by_field, acquisition_by_field)
    }
    return acquisition_by_field, failed_by_field, row_by_run_id


@pytest.fixture
def control_attempts(partial_header):
    """Supply settled evidence and an ordinary, unbound control transaction."""
    request_by_field = deepcopy(partial_header["publication_metadata_json"]["import_completion"]["request"])
    acquisition_by_field, failed_by_field, row_by_run_id = _control_attempt_rows(request_by_field)
    session = SimpleNamespace(execute=AsyncMock())

    @asynccontextmanager
    async def transaction():
        yield session

    database = SimpleNamespace(
        first=AsyncMock(side_effect=lambda _query, *, run_id: row_by_run_id.get(run_id)),
        transaction=transaction,
        _reader_binding=lambda: None,
        _transaction_binding=lambda: None,
    )
    directory = SimpleNamespace(
        db=database,
        ImportRun=SimpleNamespace(__tablename__="import_run"),
        _schema=lambda: "example_catalog",
        _qt=lambda schema, table: f'"{schema}"."{table}"',
        _pagination_checkpoint_row_mapping=lambda row_by_field: row_by_field or {},
        _json_object=lambda raw_value: raw_value or {},
        _canonical_base=lambda api_base: api_base.rstrip("/"),
    )
    candidate = SimpleNamespace(
        source_ids=("example-source",),
        acquisition_root_run_id="example-root",
        checkpoint_context=SimpleNamespace(canonical_api_base="https://example.invalid/fhir"),
    )
    return SimpleNamespace(
        directory=directory,
        candidate=candidate,
        request=request_by_field,
        rows=row_by_run_id,
        failed=failed_by_field,
        acquisition=acquisition_by_field,
    )


async def _attempt_evidence(case):
    from process.provider_directory_import_completion import _settled_attempt_metrics

    return await _settled_attempt_metrics(
        case.directory, case.candidate, case.request["retry_of_run_id"], request=case.request
    )


@pytest.mark.asyncio
async def test_failed_finalization_uses_the_linked_acquisition_evidence(control_attempts):
    case = control_attempts
    status, metrics, path = await _attempt_evidence(case)
    assert status == "failed"
    assert metrics["resource_fetch_stats"]["Organization"]["pagination_cooldown_retries"] == 4
    assert path == [case.failed["run_id"], case.acquisition["run_id"]]
    assert case.request["retry_of_run_id"] == case.failed["run_id"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("scope_name", "field_name", "changed_value"),
    [
        ("acquisition_params", "source_ids", ["another-source"]),
        ("acquisition_params", "provider_directory_pagination_root_run_id", "another-root"),
        ("acquisition_params", "provider_directory_endpoint_scope", "https://another.invalid/fhir"),
        ("failed", "finished_at", None),
        ("failed", "retry_of_run_id", "another-parent"),
        ("failed", "metrics", {}),
        ("failed", "status", "canceled"),
        ("failed_params", "allow_retained_pagination_reset", True),
        ("failed_params", "retry_of_run_id", "another-parent"),
        ("completion_request", "candidate_id", "another-candidate"),
        ("retained_counts", "Organization", 198),
        ("failed_metrics", "resource_rows", {"Organization": 1}),
        ("failed_metrics", "resource_fetch_stats", {"Organization": {"rows_fetched": 1}}),
    ],
)
async def test_completion_ancestry_refuses_changed_or_unsettled_evidence(
    control_attempts, scope_name, field_name, changed_value
):
    from process.provider_directory_import_completion import CompletionNotAccepted

    case = control_attempts
    scope_by_name = {
        "acquisition_params": case.acquisition["params"],
        "failed": case.failed,
        "failed_params": case.failed["params"],
        "completion_request": case.failed["params"]["import_completion"],
        "retained_counts": case.failed["params"]["import_completion"]["retained_counts"],
        "failed_metrics": case.failed["metrics"]["audit"],
    }
    scope_by_name[scope_name][field_name] = changed_value
    with pytest.raises(CompletionNotAccepted, match="import_completion"):
        await _attempt_evidence(case)


@pytest.mark.asyncio
async def test_completion_ancestry_refuses_a_missing_parent(control_attempts):
    from process.provider_directory_import_completion import CompletionNotAccepted

    case = control_attempts
    del case.rows[case.acquisition["run_id"]]
    with pytest.raises(CompletionNotAccepted, match="previous_attempt_not_settled"):
        await _attempt_evidence(case)


@pytest.mark.asyncio
async def test_completion_ancestry_refuses_a_cycle(control_attempts):
    from process.provider_directory_import_completion import CompletionNotAccepted

    case = control_attempts
    case.acquisition.update(
        retry_of_run_id=case.failed["run_id"],
        params={
            **case.acquisition["params"],
            "retry_of_run_id": case.failed["run_id"],
            "import_completion": deepcopy(case.request),
        },
        metrics={"resource_rows": {}, "resource_fetch_stats": {}},
    )
    with pytest.raises(CompletionNotAccepted, match="completion_ancestor_cycle"):
        await _attempt_evidence(case)


@pytest.mark.asyncio
async def test_all_historical_scopes_must_match_the_actual_candidate(control_attempts):
    from process.provider_directory_import_completion import CompletionNotAccepted

    case = control_attempts
    for previous_by_field in case.rows.values():
        previous_by_field["params"]["provider_directory_endpoint_scope"] = "https://another.invalid/fhir"
    with pytest.raises(CompletionNotAccepted, match="completion_scope_changed"):
        await _attempt_evidence(case)


@pytest.mark.asyncio
async def test_legacy_unscoped_attempts_keep_their_retained_evidence(control_attempts):
    case = control_attempts
    for previous_by_field in case.rows.values():
        del previous_by_field["params"]["provider_directory_endpoint_scope"]
    _, metrics, path = await _attempt_evidence(case)
    assert metrics["resource_fetch_stats"]["Organization"]["pagination_cooldown_retries"] == 4
    assert path == [case.failed["run_id"], case.acquisition["run_id"]]


@pytest.mark.asyncio
async def test_weak_acquisition_evidence_cannot_be_skipped(control_attempts):
    from process.provider_directory_import_completion import CompletionNotAccepted, _resource_retry_evidence

    case = control_attempts
    case.acquisition["metrics"] = {}
    status, metrics, path = await _attempt_evidence(case)
    assert path[-1] == case.acquisition["run_id"]
    with pytest.raises(CompletionNotAccepted, match="retry_evidence_missing"):
        _resource_retry_evidence(metrics, "Organization", status)


@pytest.mark.asyncio
@pytest.mark.parametrize("binding", ["_reader_binding", "_transaction_binding"])
async def test_control_evidence_refuses_an_inherited_database_context(control_attempts, binding):
    from process.provider_directory_import_completion import CompletionNotAccepted

    case = control_attempts
    setattr(case.directory.db, binding, object)
    with pytest.raises(CompletionNotAccepted, match="control_reader_bound"):
        await _attempt_evidence(case)
    case.directory.db.first.assert_not_awaited()
