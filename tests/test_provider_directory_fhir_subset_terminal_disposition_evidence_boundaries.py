# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Evidence and selector boundaries for terminal root disposition."""

from __future__ import annotations

from copy import deepcopy

import pytest

from process import provider_directory_fhir_subset_terminal_disposition_evidence as evidence
from process import provider_directory_fhir_subset_terminal_disposition_contract as contract
from process import provider_directory_fhir_subset_terminal_disposition_selection as selection
from process import provider_directory_fhir_subset_terminal_disposition_source as source
from process.provider_directory_fhir_census_binding import CurrentVersionCensusContract
from process.provider_directory_fhir_subset_abandonment_contract import (
    ABANDONMENT_METADATA_KEY,
)
from process.provider_directory_fhir_subset_terminal_disposition_contract import (
    EXPECTED_RESOURCE_TYPES,
    TERMINAL_DISPOSITION_METADATA_KEY,
    ReviewedSubsetTerminalDispositionError,
)
from process.provider_directory_fhir_subset_terminal_disposition_store import (
    sync_reviewed_subset_terminal_disposition_transaction,
)
from tests.provider_directory_fhir_subset_terminal_disposition_support import (
    SOURCE_SCOPE_SHA256,
    TerminalDispositionDatabase,
    terminal_disposition_inputs,
)


def _fixture_evidence():
    source_row, candidate_row, checkpoint_rows = terminal_disposition_inputs()
    candidate_metadata = candidate_row["publication_metadata_json"]
    diagnostics = candidate_metadata["resource_diagnostics"]
    return source_row, candidate_row, checkpoint_rows, diagnostics


def _resource_evidence(resource_type):
    _source_row, _candidate_row, checkpoint_rows, diagnostics = _fixture_evidence()
    resource_index = EXPECTED_RESOURCE_TYPES.index(resource_type)
    return (
        deepcopy(diagnostics[resource_type]),
        deepcopy(checkpoint_rows[resource_index]),
    )


def test_proof_identity_rejects_wrong_contract_profile():
    _diagnostic, checkpoint = _resource_evidence("InsurancePlan")
    proof_by_field = checkpoint["completeness_json"]
    proof_by_field["contract_version"] = 4
    with pytest.raises(ReviewedSubsetTerminalDispositionError):
        evidence._proof_identity(proof_by_field, "InsurancePlan")


@pytest.mark.parametrize("mutation", ("shape", "prior_rows", "geometry"))
def test_terminal_sequence_rejects_shape_row_and_geometry_drift(mutation):
    _diagnostic, checkpoint = _resource_evidence("Location")
    proof_by_field = checkpoint["completeness_json"]
    if mutation == "shape":
        proof_by_field["terminal_page_geometry"] = None
    elif mutation == "prior_rows":
        proof_by_field["page_entry_counts"][0] -= 1
    else:
        proof_by_field["page_count"] = 0
    with pytest.raises(ReviewedSubsetTerminalDispositionError):
        evidence._validate_terminal_sequence(
            proof_by_field,
            checkpoint["pages_processed"],
            checkpoint["rows_processed"],
            terminal_checkpointed=False,
        )


@pytest.mark.parametrize("mutation", ("counts", "state"))
def test_completed_disposition_rejects_count_and_state_drift(mutation):
    diagnostic_by_field, checkpoint = _resource_evidence("Location")
    proof_by_field = checkpoint["completeness_json"]
    if mutation == "counts":
        proof_by_field["post_count"] -= 1
    else:
        diagnostic_by_field["error"] = "invalid"
    with pytest.raises(ReviewedSubsetTerminalDispositionError):
        evidence._completed_or_drift_disposition(
            diagnostic_by_field,
            checkpoint,
            proof_by_field,
        )


def test_retryable_disposition_translates_invalid_geometry():
    diagnostic_by_field, checkpoint = _resource_evidence("InsurancePlan")
    proof_by_field = checkpoint["completeness_json"]
    proof_by_field["page_entry_counts"] = []
    with pytest.raises(ReviewedSubsetTerminalDispositionError):
        evidence._retryable_disposition(
            diagnostic_by_field,
            checkpoint,
            proof_by_field,
        )


def test_resource_disposition_rejects_checkpoint_copy_mismatch():
    diagnostic_by_field, checkpoint = _resource_evidence("InsurancePlan")
    diagnostic_by_field["rows_fetched"] += 1
    with pytest.raises(ReviewedSubsetTerminalDispositionError):
        evidence._resource_disposition(
            "InsurancePlan",
            diagnostic_by_field,
            checkpoint,
            checkpoint["start_url_hash"],
        )


def test_resource_partition_rejects_missing_checkpoint_and_campaign():
    source_row, candidate_row, checkpoint_rows, diagnostics = _fixture_evidence()
    candidate_metadata = candidate_row["publication_metadata_json"]
    expected_starts = source.expected_terminal_start_hashes(
        source_row,
        candidate_metadata,
        diagnostics,
        SOURCE_SCOPE_SHA256,
    )
    with pytest.raises(ReviewedSubsetTerminalDispositionError):
        evidence.validated_resource_dispositions(
            diagnostics,
            checkpoint_rows[:-1],
            candidate_metadata,
            expected_start_hash_by_type=expected_starts,
        )

    changed_metadata = deepcopy(candidate_metadata)
    changed_metadata["verification_campaign_id"] = "different-campaign"
    with pytest.raises(ReviewedSubsetTerminalDispositionError):
        evidence.validated_resource_dispositions(
            diagnostics,
            checkpoint_rows,
            changed_metadata,
            expected_start_hash_by_type=expected_starts,
        )


class _CandidateCardinalityDatabase(TerminalDispositionDatabase):
    def __init__(self, *, disposed_count=0, failed_count=1):
        super().__init__()
        self.disposed_count = disposed_count
        self.failed_count = failed_count

    async def all(self, statement, **parameters):
        if "SELECT dataset.*" in statement:
            count = (
                self.disposed_count
                if "disposed_status" in parameters
                else self.failed_count
            )
            return [deepcopy(self.candidate_row) for _index in range(count)]
        return await super().all(statement, **parameters)


@pytest.mark.parametrize(
    "database",
    (
        _CandidateCardinalityDatabase(disposed_count=2),
        _CandidateCardinalityDatabase(failed_count=0),
    ),
)
@pytest.mark.asyncio
async def test_candidate_selection_rejects_ambiguous_cardinality(database):
    with pytest.raises(ReviewedSubsetTerminalDispositionError):
        await selection._locked_candidate_row(
            database,
            "source-a",
            "endpoint-a",
        )


@pytest.mark.asyncio
async def test_replay_rejects_cross_version_and_tampered_marker():
    database = TerminalDispositionDatabase()
    await sync_reviewed_subset_terminal_disposition_transaction(
        database,
        "source-a",
    )
    database.candidate_row["publication_metadata_json"][ABANDONMENT_METADATA_KEY] = {}
    with pytest.raises(ReviewedSubsetTerminalDispositionError):
        await selection.selected_reviewed_subset_terminal_disposition(
            database,
            "source-a",
        )

    database.candidate_row["publication_metadata_json"].pop(ABANDONMENT_METADATA_KEY)
    database.candidate_row["resource_count"] += 1
    with pytest.raises(ReviewedSubsetTerminalDispositionError):
        await selection.selected_reviewed_subset_terminal_disposition(
            database,
            "source-a",
        )


class _BulkCheckpointDatabase(TerminalDispositionDatabase):
    async def scalar(self, statement, **parameters):
        if "bulk_acquisition_checkpoint" in statement:
            return 1
        return await super().scalar(statement, **parameters)


@pytest.mark.parametrize(
    "database,expected_source_id",
    (
        (TerminalDispositionDatabase(), "different-source"),
        (_BulkCheckpointDatabase(), "source-a"),
    ),
)
@pytest.mark.asyncio
async def test_selector_translates_abandonment_evidence_errors(
    database,
    expected_source_id,
):
    with pytest.raises(ReviewedSubsetTerminalDispositionError):
        await selection.selected_reviewed_subset_terminal_disposition(
            database,
            expected_source_id,
        )


@pytest.mark.asyncio
async def test_new_selection_rejects_retained_count_mismatch(monkeypatch):
    database = TerminalDispositionDatabase()

    async def inconsistent_evidence(_database, _candidate_row, _source_row):
        resource_count = database.candidate_row["resource_count"] + 1
        return database.checkpoint_rows, SOURCE_SCOPE_SHA256, (
            resource_count,
            3,
            resource_count,
        )

    monkeypatch.setattr(
        selection,
        "_locked_terminal_evidence",
        inconsistent_evidence,
    )
    with pytest.raises(ReviewedSubsetTerminalDispositionError):
        await selection._new_terminal_selection(
            database,
            database.source_row,
            database.candidate_row,
            "source-a",
            "endpoint-a",
            "failed",
        )


def test_policy_helpers_reject_wrong_state_and_documents():
    assert source.is_policy_one_pending({}) is False
    assert source.is_policy_one_pending(
        {
            "provider_directory_candidate_status": "pending_reviewed_subset_acquisition",
            "provider_directory_reviewed_root_policy_v1": {},
        }
    ) is False
    assert source.is_candidate_policy_one(
        {"provider_directory_reviewed_root_policy_v1": {}}
    ) is False


@pytest.mark.parametrize("mutation", ("start_urls", "identity"))
def test_source_contract_rejects_invalid_profile_inputs(mutation):
    source_row, candidate_row, _checkpoint_rows, diagnostics = _fixture_evidence()
    if mutation == "start_urls":
        source_row["metadata_json"][
            "provider_directory_current_version_census_start_urls"
        ] = None
    else:
        source_row["metadata_json"][
            "provider_directory_current_version_census_contract_version"
        ] = "invalid"
    with pytest.raises(ReviewedSubsetTerminalDispositionError):
        source.expected_terminal_start_hashes(
            source_row,
            candidate_row["publication_metadata_json"],
            diagnostics,
            SOURCE_SCOPE_SHA256,
        )


def test_source_contract_translates_start_url_failure(monkeypatch):
    source_row, candidate_row, _checkpoint_rows, diagnostics = _fixture_evidence()

    def invalid_start_url(_contract, _resource_type, _page_count=None):
        raise ValueError("private detail")

    monkeypatch.setattr(CurrentVersionCensusContract, "start_url", invalid_start_url)
    with pytest.raises(ReviewedSubsetTerminalDispositionError):
        source.expected_terminal_start_hashes(
            source_row,
            candidate_row["publication_metadata_json"],
            diagnostics,
            SOURCE_SCOPE_SHA256,
        )


def test_translation_preserves_non_abandonment_errors():
    error = ValueError("synthetic")
    with pytest.raises(ValueError) as raised:
        selection._translate_evidence_error(error)
    assert raised.value is error


def test_marker_resource_shape_rejects_missing_field():
    database = TerminalDispositionDatabase()
    marker_by_field = database.candidate_row["publication_metadata_json"]
    assert TERMINAL_DISPOSITION_METADATA_KEY not in marker_by_field
    resource_by_field = {
        "disposition": "stable_complete",
    }
    with pytest.raises(ReviewedSubsetTerminalDispositionError):
        contract._validated_resource_disposition(resource_by_field)
