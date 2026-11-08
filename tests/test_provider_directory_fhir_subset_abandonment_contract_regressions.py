# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Evidence and authority boundaries for retained subset abandonment."""

from __future__ import annotations

import json
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_fhir_subset_abandonment as abandonment
from process.provider_directory_fhir_subset_abandonment_contract import (
    ABANDONMENT_ENABLED_ENV,
    ReviewedSubsetAbandonmentError,
    ReviewedSubsetAbandonmentResult,
    _quoted_relation,
    abandonment_result_json,
    validated_abandonment_marker,
)
from process.provider_directory_fhir_subset_abandonment_policy import (
    has_matching_reviewed_root_policy,
    reviewed_abandonment_source_state,
)
from process.provider_directory_fhir_subset_abandonment_selection import (
    selected_reviewed_subset_abandonment,
)
from tests.provider_directory_fhir_subset_abandonment_support import (
    RESOURCE_TYPES,
    AbandonmentDatabase,
)


@pytest.fixture
async def retained_selection():
    selection, _checkpoints = await selected_reviewed_subset_abandonment(
        AbandonmentDatabase(), "source-a", RESOURCE_TYPES
    )
    return selection


def _assert_no_evidence_writes(database):
    assert not any(
        method == "status" and statement.lstrip().startswith(("UPDATE", "INSERT", "DELETE"))
        for method, statement, _parameters in database.calls
    )


async def test_selector_accepts_serialized_candidate_and_checkpoint_evidence_from_mapping_rows():
    database = AbandonmentDatabase()
    expected_source_metadata = database.source_row["metadata_json"]
    expected_candidate_metadata = database.candidate_row["publication_metadata_json"]
    database.candidate_row["publication_metadata_json"] = json.dumps(expected_candidate_metadata)
    for checkpoint in database.checkpoint_rows:
        checkpoint["source_ids"] = json.dumps(checkpoint["source_ids"])
    original_all = database.all

    async def mapping_rows(statement, **parameters):
        return [SimpleNamespace(_mapping=row) for row in await original_all(statement, **parameters)]

    database.all = mapping_rows
    selection, checkpoints = await selected_reviewed_subset_abandonment(database, "source-a", RESOURCE_TYPES)

    assert selection.observed_metadata == expected_candidate_metadata
    assert selection.diagnostic_by_resource == expected_source_metadata["last_resource_import"]["resources"]
    assert selection.marker_by_field["resource_count"] == sum(row["rows_processed"] for row in checkpoints)
    _assert_no_evidence_writes(database)


@pytest.mark.parametrize("metadata", ['{"unfinished":', "[]", [], None])
async def test_selector_refuses_malformed_source_metadata_before_locking(metadata):
    database = AbandonmentDatabase()
    database.source_row["metadata_json"] = metadata

    with pytest.raises(ReviewedSubsetAbandonmentError) as error:
        await selected_reviewed_subset_abandonment(database, "source-a", RESOURCE_TYPES)

    assert str(error.value) == error.value.code == "evidence"
    assert [method for method, _statement, _parameters in database.calls] == ["all"]


@pytest.mark.parametrize("source_ids", ['["unfinished"', '{"source":"source-a"}', [""], [" source-a"], [1]])
async def test_selector_refuses_malformed_checkpoint_membership_without_writing(source_ids):
    database = AbandonmentDatabase()
    database.checkpoint_rows[0]["source_ids"] = source_ids

    with pytest.raises(ReviewedSubsetAbandonmentError) as error:
        await selected_reviewed_subset_abandonment(database, "source-a", RESOURCE_TYPES)

    assert str(error.value) == error.value.code == "evidence"
    _assert_no_evidence_writes(database)


@pytest.mark.parametrize("row", [None, SimpleNamespace(_mapping=None)])
async def test_selector_rejects_rows_without_named_columns(row):
    database = AbandonmentDatabase()
    database.all = AsyncMock(return_value=[row])

    with pytest.raises(ReviewedSubsetAbandonmentError) as error:
        await selected_reviewed_subset_abandonment(database, "source-a", RESOURCE_TYPES)

    assert str(error.value) == error.value.code == "state"
    assert database.calls == []


@pytest.mark.parametrize(
    ("runtime_schema", "legacy_schema"),
    [("candidate_a", "candidate_b"), ('candidate"; SELECT 1', None), (None, "candidate.schema")],
)
async def test_selector_rejects_ambiguous_or_unsafe_schema_before_querying(monkeypatch, runtime_schema, legacy_schema):
    for name, value in (("HLTHPRT_DB_SCHEMA", runtime_schema), ("DB_SCHEMA", legacy_schema)):
        if value is None:
            monkeypatch.delenv(name, raising=False)
        else:
            monkeypatch.setenv(name, value)
    database = AbandonmentDatabase()

    with pytest.raises(ReviewedSubsetAbandonmentError) as error:
        await selected_reviewed_subset_abandonment(database, "source-a", RESOURCE_TYPES)

    assert str(error.value) == error.value.code == "state"
    assert database.calls == []


def test_relation_identifier_cannot_escape_its_quoted_table():
    with pytest.raises(ReviewedSubsetAbandonmentError) as error:
        _quoted_relation('retained_rows"; SELECT 1')

    assert str(error.value) == error.value.code == "state"


@pytest.mark.parametrize(
    "change",
    [{"owner_run_id": ""}, {"prior_status": "published"}, {"observed_resource_count": True}],
)
async def test_selection_cannot_be_rebound_to_invalid_owner_or_serving_state(retained_selection, change):
    with pytest.raises(ReviewedSubsetAbandonmentError) as error:
        replace(retained_selection, **change)

    assert str(error.value) == error.value.code == "evidence"


@pytest.mark.parametrize("identity", ["source_scope_sha256", "resource_types"])
async def test_selection_must_keep_the_retained_marker_scope(retained_selection, identity):
    replacement = "3" * 64 if identity == "source_scope_sha256" else RESOURCE_TYPES[:-1]

    with pytest.raises(ReviewedSubsetAbandonmentError) as error:
        replace(retained_selection, **{identity: replacement})

    assert str(error.value) == error.value.code == "evidence"


@pytest.mark.parametrize("change", ["missing", "extra"])
async def test_terminal_diagnostics_must_cover_exactly_the_requested_resources(change):
    database = AbandonmentDatabase()
    diagnostics = database.source_row["metadata_json"]["last_resource_import"]["resources"]
    if change == "missing":
        diagnostics.pop(RESOURCE_TYPES[0])
    else:
        diagnostics["UnrequestedResource"] = dict(diagnostics[RESOURCE_TYPES[0]])

    with pytest.raises(ReviewedSubsetAbandonmentError) as error:
        await selected_reviewed_subset_abandonment(database, "source-a", RESOURCE_TYPES)

    assert str(error.value) == error.value.code == "evidence"
    _assert_no_evidence_writes(database)


@pytest.mark.parametrize(
    ("field", "value"),
    [("fetch_mode", "bounded"), ("complete", True), ("bounded", True), ("complete", 0)],
)
async def test_only_unbounded_incomplete_reviewed_cursor_failures_are_abandonable(field, value):
    database = AbandonmentDatabase()
    diagnostic = database.source_row["metadata_json"]["last_resource_import"]["resources"][RESOURCE_TYPES[0]]
    diagnostic[field] = value

    with pytest.raises(ReviewedSubsetAbandonmentError) as error:
        await selected_reviewed_subset_abandonment(database, "source-a", RESOURCE_TYPES)

    assert str(error.value) == error.value.code == "evidence"
    _assert_no_evidence_writes(database)


@pytest.mark.parametrize(
    "change",
    [
        {"contract_version": "unsupported"},
        {"source_scope_sha256": "invalid"},
        {"resource_types": list(reversed(RESOURCE_TYPES))},
        {"terminal_error_codes": dict.fromkeys(RESOURCE_TYPES, "http_503")},
    ],
)
async def test_retained_marker_refuses_changed_contract_scope_or_terminal_status(retained_selection, change):
    with pytest.raises(ReviewedSubsetAbandonmentError) as error:
        validated_abandonment_marker({**retained_selection.marker_by_field, **change})

    assert str(error.value) == error.value.code == "evidence"


@pytest.mark.parametrize("field", ["checkpoint_count", "rows_processed", "proof_row_count"])
async def test_retained_marker_preserves_checkpoint_and_row_accounting(retained_selection, field):
    marker_by_field = dict(retained_selection.marker_by_field)
    marker_by_field[field] += 1

    with pytest.raises(ReviewedSubsetAbandonmentError) as error:
        validated_abandonment_marker(marker_by_field)

    assert str(error.value) == error.value.code == "evidence"


@pytest.mark.parametrize("result", [{"abandoned": True}, SimpleNamespace(abandoned=True)])
def test_success_renderer_refuses_unvalidated_result_objects(result):
    with pytest.raises(ReviewedSubsetAbandonmentError) as error:
        abandonment_result_json(result)

    assert str(error.value) == error.value.code == "state"


@pytest.mark.parametrize(
    ("boundary", "count", "expected_code"),
    [
        ("initial_source", 0, "state"),
        ("initial_source", 2, "state"),
        ("endpoint", 0, "state"),
        ("locked_source", 2, "evidence"),
        ("candidate", 0, "evidence"),
        ("candidate", 2, "evidence"),
    ],
)
async def test_selector_never_guesses_between_missing_or_ambiguous_authority_rows(boundary, count, expected_code):
    database = AbandonmentDatabase()
    original_all = database.all
    statement_marker = {
        "initial_source": "LIMIT 2",
        "endpoint": "provider_directory_api_endpoint",
        "locked_source": "FOR UPDATE OF source",
        "candidate": "SELECT dataset.*",
    }[boundary]

    async def authority_rows(statement, **parameters):
        rows = await original_all(statement, **parameters)
        return rows * count if statement_marker in statement else rows

    database.all = authority_rows

    with pytest.raises(ReviewedSubsetAbandonmentError) as error:
        await selected_reviewed_subset_abandonment(database, "source-a", RESOURCE_TYPES)

    assert str(error.value) == error.value.code == expected_code
    _assert_no_evidence_writes(database)


async def test_selector_rechecks_serving_identity_after_acquiring_source_lock():
    database = AbandonmentDatabase()
    original_all = database.all

    async def changed_locked_source(statement, **parameters):
        rows = await original_all(statement, **parameters)
        if "FOR UPDATE OF source" in statement:
            rows[0]["endpoint_id"] = "endpoint-changed"
        return rows

    database.all = changed_locked_source

    with pytest.raises(ReviewedSubsetAbandonmentError) as error:
        await selected_reviewed_subset_abandonment(database, "source-a", RESOURCE_TYPES)

    assert str(error.value) == error.value.code == "evidence"
    _assert_no_evidence_writes(database)


async def test_selector_cannot_reuse_another_endpoints_pagination_guard():
    database = AbandonmentDatabase()

    with pytest.raises(ReviewedSubsetAbandonmentError) as error:
        await selected_reviewed_subset_abandonment(
            database,
            "source-a",
            RESOURCE_TYPES,
            held_pagination_guard_key="provider-directory-pagination:https://other.example.test/fhir",
        )

    assert str(error.value) == error.value.code == "state"
    assert [method for method, _statement, _parameters in database.calls] == ["all"]


@pytest.mark.parametrize("change", ["diagnostic_owner", "activated_source"])
async def test_new_abandonment_requires_pending_source_and_exact_diagnostic_owner(change):
    database = AbandonmentDatabase()
    metadata = database.source_row["metadata_json"]
    if change == "diagnostic_owner":
        metadata["last_resource_import"]["run_id"] = "another-owner"
    else:
        metadata.update(
            provider_directory_candidate_status="verified_two_matching_reviewed_subset_acquisitions",
            provider_directory_reviewed_subset_activation_v1={"contract_version": "synthetic-activation"},
        )

    with pytest.raises(ReviewedSubsetAbandonmentError) as error:
        await selected_reviewed_subset_abandonment(database, "source-a", RESOURCE_TYPES)

    assert str(error.value) == error.value.code == "evidence"
    _assert_no_evidence_writes(database)


@pytest.mark.parametrize("change", [{"owner_run_id": "another-owner"}, {"rows_processed": -1}])
async def test_checkpoint_lineage_and_counts_cannot_be_substituted(change):
    database = AbandonmentDatabase()
    database.checkpoint_rows[0].update(change)

    with pytest.raises(ReviewedSubsetAbandonmentError) as error:
        await selected_reviewed_subset_abandonment(database, "source-a", RESOURCE_TYPES)

    assert str(error.value) == error.value.code == "evidence"
    _assert_no_evidence_writes(database)


@pytest.mark.parametrize(
    "change",
    ["resource_count", "missing_proof_total", "ambiguous_proof_total", "proof_lineage", "proof_total", "proof_type"],
)
async def test_retained_evidence_requires_complete_matching_resource_and_proof_counts(change):
    database = AbandonmentDatabase()
    original_all = database.all

    async def changed_evidence(statement, **parameters):
        rows = await original_all(statement, **parameters)
        if change == "resource_count" and "SELECT resource_type, count(*)" in statement:
            rows[0]["resource_count"] += 1
        if "invalid_lineage_count" in statement:
            if change == "missing_proof_total":
                return []
            if change == "ambiguous_proof_total":
                return rows * 2
            if change == "proof_lineage":
                rows[0]["invalid_lineage_count"] = 1
            if change == "proof_total":
                rows[0]["proof_row_count"] += 1
        if change == "proof_type" and "raw_count.resource_type" in statement:
            rows[0]["resource_count"] += 1
        return rows

    database.all = changed_evidence

    with pytest.raises(ReviewedSubsetAbandonmentError) as error:
        await selected_reviewed_subset_abandonment(database, "source-a", RESOURCE_TYPES)

    assert str(error.value) == error.value.code == "evidence"
    _assert_no_evidence_writes(database)


@pytest.mark.parametrize(
    "metadata",
    [
        {},
        {
            "provider_directory_candidate_status": "pending_reviewed_subset_acquisition",
            "provider_directory_reviewed_root_policy_v1": {"policy_version": "unsupported", "required_root_count": 1},
        },
        {
            "provider_directory_candidate_status": "pending_two_matching_reviewed_subset_acquisitions",
            "provider_directory_reviewed_subset_activation_v1": {},
        },
        {"provider_directory_candidate_status": "verified_two_matching_reviewed_subset_acquisitions"},
    ],
)
def test_source_lifecycle_requires_matching_policy_and_activation_evidence(metadata):
    assert reviewed_abandonment_source_state(metadata) is None


def test_policy_binding_cannot_accept_two_equal_but_malformed_policy_documents():
    metadata = {
        "provider_directory_reviewed_root_policy_v1": {"policy_version": "unsupported", "required_root_count": 1}
    }

    assert not has_matching_reviewed_root_policy(metadata, dict(metadata))


@pytest.fixture
def enabled_abandonment_operator(monkeypatch):
    monkeypatch.setenv(ABANDONMENT_ENABLED_ENV, "true")
    monkeypatch.setattr(
        "process.provider_directory_fhir_manual_catalog.reviewed_manual_census_source_id", lambda: "source-a"
    )


async def test_operator_refuses_catalog_failure_without_starting_a_transaction(
    monkeypatch, enabled_abandonment_operator
):
    def unavailable_catalog():
        raise ValueError("synthetic-catalog-detail")

    monkeypatch.setattr(
        "process.provider_directory_fhir_manual_catalog.reviewed_manual_census_source_id", unavailable_catalog
    )
    transaction = AsyncMock()
    monkeypatch.setattr(abandonment, "sync_reviewed_subset_abandonment_transaction", transaction)

    with pytest.raises(ReviewedSubsetAbandonmentError) as error:
        await abandonment.abandon_reviewed_subset_expired_root(database=object())

    assert str(error.value) == error.value.code == "evidence"
    assert error.value.__suppress_context__
    transaction.assert_not_awaited()


async def test_operator_uses_application_database_only_when_no_database_was_supplied(
    monkeypatch, enabled_abandonment_operator
):
    application_database = object()
    monkeypatch.setattr("db.connection.db", application_database)
    result = ReviewedSubsetAbandonmentResult(abandoned=False)
    transaction = AsyncMock(return_value=result)
    monkeypatch.setattr(abandonment, "sync_reviewed_subset_abandonment_transaction", transaction)

    assert await abandonment.abandon_reviewed_subset_expired_root() is result
    transaction.assert_awaited_once_with(application_database, "source-a", RESOURCE_TYPES)


async def test_operator_redacts_unexpected_storage_failure(monkeypatch, enabled_abandonment_operator):
    transaction = AsyncMock(side_effect=RuntimeError("synthetic-storage-detail"))
    monkeypatch.setattr(abandonment, "sync_reviewed_subset_abandonment_transaction", transaction)

    with pytest.raises(ReviewedSubsetAbandonmentError) as error:
        await abandonment.abandon_reviewed_subset_expired_root(database=object())

    assert str(error.value) == error.value.code == "state"
    assert error.value.__suppress_context__


async def test_operator_preserves_an_explicit_abandonment_refusal(monkeypatch, enabled_abandonment_operator):
    refusal = ReviewedSubsetAbandonmentError("busy")
    transaction = AsyncMock(side_effect=refusal)
    monkeypatch.setattr(abandonment, "sync_reviewed_subset_abandonment_transaction", transaction)

    with pytest.raises(ReviewedSubsetAbandonmentError) as error:
        await abandonment.abandon_reviewed_subset_expired_root(database=object())

    assert error.value is refusal
