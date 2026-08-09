# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed-contract tests for reviewed subset abandonment."""

from __future__ import annotations

from copy import deepcopy
import importlib.util
from pathlib import Path

import pytest

from process import provider_directory_fhir_subset_abandonment as abandonment
from process.provider_directory_fhir_subset_abandonment_contract import (
    ABANDONED_STATUS,
    ABANDONMENT_ENABLED_ENV,
    ReviewedSubsetAbandonmentError,
    ReviewedSubsetAbandonmentResult,
    abandonment_marker,
    abandonment_result_json,
    require_reviewed_subset_abandonment_gate,
    terminal_error_code,
    validated_abandonment_marker,
    validated_terminal_diagnostics,
)
from process.provider_directory_fhir_subset_abandonment_selection import (
    selected_reviewed_subset_abandonment,
)
from process.provider_directory_fhir_subset_abandonment_store import (
    sync_reviewed_subset_abandonment_transaction,
)
from tests.provider_directory_fhir_subset_abandonment_support import (
    AbandonmentDatabase,
    RESOURCE_TYPES,
    SOURCE_SCOPE_SHA256,
    VERIFICATION_SCOPE_SHA256,
    abandonment_inputs,
)


def test_gate_is_exact_and_result_is_identifier_free(monkeypatch):
    for value in (None, "", "1", "TRUE", "yes"):
        if value is None:
            monkeypatch.delenv(ABANDONMENT_ENABLED_ENV, raising=False)
        else:
            monkeypatch.setenv(ABANDONMENT_ENABLED_ENV, value)
        with pytest.raises(ReviewedSubsetAbandonmentError) as error:
            require_reviewed_subset_abandonment_gate()
        assert error.value.code == "disabled"

    monkeypatch.setenv(ABANDONMENT_ENABLED_ENV, "true")
    require_reviewed_subset_abandonment_gate()
    assert (
        abandonment_result_json(ReviewedSubsetAbandonmentResult(abandoned=True))
        == '{"abandoned":true,"already_applied":false,"status":"ok"}'
    )


def test_marker_and_terminal_diagnostics_are_closed():
    source_row, _candidate_row, checkpoint_rows = abandonment_inputs()
    diagnostics = source_row["metadata_json"]["last_resource_import"]["resources"]
    validated = validated_terminal_diagnostics(diagnostics, RESOURCE_TYPES)
    resource_count = sum(
        checkpoint_row["rows_processed"] for checkpoint_row in checkpoint_rows
    )
    marker = abandonment_marker(
        source_scope_sha256=SOURCE_SCOPE_SHA256,
        resource_types=RESOURCE_TYPES,
        checkpoint_count=len(checkpoint_rows),
        pages_processed=sum(
            checkpoint_row["pages_processed"] for checkpoint_row in checkpoint_rows
        ),
        rows_processed=resource_count,
        resource_count=resource_count,
        proof_shard_count=2,
        proof_row_count=resource_count,
    )

    assert tuple(validated) == RESOURCE_TYPES
    assert validated_abandonment_marker(marker) == marker
    assert set(marker) == {
        "contract_version",
        "reason_code",
        "source_scope_sha256",
        "resource_types",
        "terminal_error_codes",
        "checkpoint_count",
        "pages_processed",
        "rows_processed",
        "resource_count",
        "proof_shard_count",
        "proof_row_count",
    }

    malformed = deepcopy(diagnostics)
    malformed["Location"]["error"] = "http_503"
    with pytest.raises(ReviewedSubsetAbandonmentError):
        validated_terminal_diagnostics(malformed, RESOURCE_TYPES)


@pytest.mark.parametrize(
    "error_text",
    (
        "garbage:http_410",
        "http_400",
        "http_404",
        "http_500",
        "http_503",
        410,
        None,
    ),
)
def test_terminal_error_code_admits_only_exact_gone_status(error_text):
    with pytest.raises(ReviewedSubsetAbandonmentError) as error:
        terminal_error_code({"error": error_text})
    assert error.value.code == "evidence"


def test_marker_rejects_open_fields_and_noninteger_counts():
    _source_row, _candidate_row, checkpoint_rows = abandonment_inputs()
    resource_count = sum(
        checkpoint_row["rows_processed"] for checkpoint_row in checkpoint_rows
    )
    marker = abandonment_marker(
        source_scope_sha256=SOURCE_SCOPE_SHA256,
        resource_types=RESOURCE_TYPES,
        checkpoint_count=len(checkpoint_rows),
        pages_processed=sum(
            checkpoint_row["pages_processed"] for checkpoint_row in checkpoint_rows
        ),
        rows_processed=resource_count,
        resource_count=resource_count,
        proof_shard_count=2,
        proof_row_count=resource_count,
    )
    malformed_markers = []
    for field_name in (
        "checkpoint_count",
        "pages_processed",
        "rows_processed",
        "resource_count",
        "proof_shard_count",
        "proof_row_count",
    ):
        malformed = deepcopy(marker)
        malformed[field_name] = str(malformed[field_name])
        malformed_markers.append(malformed)
    open_marker = deepcopy(marker)
    open_marker["private_id"] = "synthetic"
    malformed_markers.append(open_marker)

    for malformed in malformed_markers:
        with pytest.raises(ReviewedSubsetAbandonmentError) as error:
            validated_abandonment_marker(malformed)
        assert error.value.code == "evidence"


def test_result_json_validates_boolean_and_already_applied_shape():
    assert (
        abandonment_result_json(ReviewedSubsetAbandonmentResult(abandoned=False))
        == '{"abandoned":false,"already_applied":true,"status":"ok"}'
    )
    with pytest.raises(ReviewedSubsetAbandonmentError):
        ReviewedSubsetAbandonmentResult(abandoned=1)


@pytest.mark.asyncio
async def test_selector_locks_exact_evidence_and_builds_private_selection():
    database = AbandonmentDatabase()

    selection, checkpoint_rows = await selected_reviewed_subset_abandonment(
        database,
        "source-a",
        RESOURCE_TYPES,
    )

    assert selection.prior_status == "failed"
    assert selection.endpoint_id == "endpoint-a"
    assert selection.source_scope_sha256 == SOURCE_SCOPE_SHA256
    assert (
        database.candidate_row["publication_metadata_json"]
        ["verification_source_scope_hash"]
        == VERIFICATION_SCOPE_SHA256
    )
    assert selection.source_scope_sha256 != VERIFICATION_SCOPE_SHA256
    assert selection.resource_types == RESOURCE_TYPES
    expected_resource_count = sum(
        checkpoint_row["rows_processed"] for checkpoint_row in checkpoint_rows
    )
    assert selection.marker_by_field["resource_count"] == expected_resource_count
    assert len(checkpoint_rows) == len(RESOURCE_TYPES)
    call_text = " ".join(call[1] for call in database.calls)
    endpoint_lock_calls = [
        parameters
        for method, statement, parameters in database.calls
        if method == "scalar" and "hashtextextended" in statement
        and "endpoint_id" in parameters
    ]
    assert endpoint_lock_calls == [{"endpoint_id": "endpoint-a"}]
    source_lock_parameters = next(
        parameters
        for method, statement, parameters in database.calls
        if method == "all" and "SELECT source.*" in statement
        and "FOR UPDATE OF source" in statement
    )
    assert source_lock_parameters["endpoint_id"] == "endpoint-a"
    assert "provider-directory-pagination:" in str(database.calls)
    assert "LOCK TABLE" in call_text
    assert "FOR UPDATE OF dataset" in call_text
    assert "FOR UPDATE OF checkpoint" in call_text


@pytest.mark.asyncio
async def test_selector_rejects_collapsed_or_malformed_scope_domains():
    malformed_databases = []

    invalid_verification_scope = AbandonmentDatabase()
    invalid_verification_scope.candidate_row["publication_metadata_json"][
        "verification_source_scope_hash"
    ] = "invalid"
    malformed_databases.append(invalid_verification_scope)

    mismatched_campaign = AbandonmentDatabase()
    mismatched_campaign.candidate_row["publication_metadata_json"][
        "verification_campaign_id"
    ] = "campaign-b"
    malformed_databases.append(mismatched_campaign)

    mixed_checkpoint_scopes = AbandonmentDatabase()
    mixed_checkpoint_scopes.checkpoint_rows[0]["source_scope_hash"] = "3" * 64
    malformed_databases.append(mixed_checkpoint_scopes)

    collapsed_scope_domains = AbandonmentDatabase()
    collapsed_scope_domains.candidate_row["publication_metadata_json"][
        "verification_source_scope_hash"
    ] = SOURCE_SCOPE_SHA256
    malformed_databases.append(collapsed_scope_domains)

    for database in malformed_databases:
        with pytest.raises(ReviewedSubsetAbandonmentError) as error:
            await selected_reviewed_subset_abandonment(
                database,
                "source-a",
                RESOURCE_TYPES,
            )
        assert error.value.code == "evidence"


@pytest.mark.asyncio
async def test_store_seals_children_before_parent_and_forces_both_guards():
    database = AbandonmentDatabase()

    result = await sync_reviewed_subset_abandonment_transaction(
        database,
        "source-a",
        RESOURCE_TYPES,
    )

    assert result == ReviewedSubsetAbandonmentResult(abandoned=True)
    update_statements = [
        statement
        for method, statement, _parameters in database.calls
        if method == "status" and "UPDATE" in statement
    ]
    assert len(update_statements) == len(RESOURCE_TYPES) + 1
    assert all("pagination_checkpoint" in sql for sql in update_statements[:-1])
    assert "endpoint_dataset" in update_statements[-1]
    constraint_call = next(
        statement
        for method, statement, _parameters in database.calls
        if method == "status" and "SET CONSTRAINTS" in statement
    )
    assert "checkpoint_guard" in constraint_call
    assert "dataset_consistency_guard" in constraint_call


@pytest.mark.asyncio
async def test_store_exact_retry_is_validated_without_rewriting_evidence():
    database = AbandonmentDatabase(already_applied=True)

    result = await sync_reviewed_subset_abandonment_transaction(
        database,
        "source-a",
        RESOURCE_TYPES,
    )

    assert result == ReviewedSubsetAbandonmentResult(abandoned=False)
    assert not any(
        method == "status" and "UPDATE" in statement
        for method, statement, _parameters in database.calls
    )


@pytest.mark.asyncio
async def test_store_retry_remains_idempotent_after_source_activation():
    database = AbandonmentDatabase(already_applied=True)
    database.source_row["metadata_json"].update(
        provider_directory_candidate_status=(
            "verified_two_matching_reviewed_subset_acquisitions"
        ),
        provider_directory_reviewed_subset_activation_v1={
            "contract_version": "synthetic-activation"
        },
    )

    result = await sync_reviewed_subset_abandonment_transaction(
        database,
        "source-a",
        RESOURCE_TYPES,
    )

    assert result == ReviewedSubsetAbandonmentResult(abandoned=False)
    assert not any(
        method == "status" and "UPDATE" in statement
        for method, statement, _parameters in database.calls
    )


@pytest.mark.parametrize(
    "failure_boundary",
    ("isolation", "checkpoint_cas", "candidate_cas", "guard", "replay_guard"),
)
@pytest.mark.asyncio
async def test_store_fails_closed_at_each_transactional_boundary(
    monkeypatch,
    failure_boundary,
):
    """Reject isolation, CAS, and relational-validation drift without success."""

    database = AbandonmentDatabase(already_applied=failure_boundary == "replay_guard")
    if failure_boundary == "isolation":
        original_scalar = database.scalar

        async def scalar(statement, **parameters):
            if "transaction_isolation" in statement:
                database.calls.append(("scalar", statement, parameters))
                return "repeatable read"
            return await original_scalar(statement, **parameters)

        monkeypatch.setattr(database, "scalar", scalar)
    elif failure_boundary in {"guard", "replay_guard"}:
        database.valid = False
    else:
        original_status = database.status
        failed_relation = (
            "pagination_checkpoint"
            if failure_boundary == "checkpoint_cas"
            else "endpoint_dataset"
        )

        async def status(statement, **parameters):
            if "UPDATE" in statement and failed_relation in statement:
                database.calls.append(("status", statement, parameters))
                return 0
            return await original_status(statement, **parameters)

        monkeypatch.setattr(database, "status", status)

    with pytest.raises(ReviewedSubsetAbandonmentError) as error:
        await sync_reviewed_subset_abandonment_transaction(
            database,
            "source-a",
            RESOURCE_TYPES,
        )
    assert error.value.code == "state"


@pytest.mark.asyncio
async def test_operator_is_selector_free_and_gate_bound(monkeypatch):
    database = AbandonmentDatabase()
    monkeypatch.setenv(ABANDONMENT_ENABLED_ENV, "true")
    monkeypatch.setattr(
        "process.provider_directory_fhir_manual_catalog."
        "reviewed_manual_census_source_id",
        lambda: "source-a",
    )

    result = await abandonment.abandon_reviewed_subset_expired_root(database=database)

    assert result.abandoned is True
    assert all("source-a" not in field_name for field_name in vars(result))


def test_abandoned_status_does_not_expand_completion_terminal_contract():
    importer_source = __import__(
        "process.provider_directory_fhir",
        fromlist=["IMMUTABLE_ENDPOINT_DATASET_STATUSES"],
    )
    migration_path = (
        Path(__file__).resolve().parents[1]
        / "alembic/versions/20260808190000_provider_directory_subset_completion_proof.py"
    )
    module_spec = importlib.util.spec_from_file_location(
        "subset_completion_terminal_contract",
        migration_path,
    )
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)

    assert ABANDONED_STATUS in importer_source.IMMUTABLE_ENDPOINT_DATASET_STATUSES
    assert ABANDONED_STATUS not in migration._TERMINAL_STATUSES_SQL
