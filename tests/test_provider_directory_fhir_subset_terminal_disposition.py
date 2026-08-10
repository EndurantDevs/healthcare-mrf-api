# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed fake-DB tests for reviewed mixed-terminal disposition."""

from __future__ import annotations

from dataclasses import replace
import hashlib

import pytest

from process.provider_directory_fhir_census_execution import (
    current_version_census_proof_identity,
)
from process.provider_directory_fhir_subset_abandonment_contract import (
    ABANDONMENT_METADATA_KEY,
)
from process.provider_directory_fhir_subset_terminal_disposition_contract import (
    COUNT_DRIFT_DISPOSITION,
    EXPECTED_RESOURCE_TYPES,
    RETRYABLE_HTTP_500_DISPOSITION,
    STABLE_COMPLETE_DISPOSITION,
    TERMINAL_DISPOSITION_ENABLED_ENV,
    ReviewedSubsetTerminalDispositionError,
    ReviewedSubsetTerminalDispositionResult,
    require_reviewed_subset_terminal_disposition_gate,
    validated_terminal_disposition_marker,
)
from process.provider_directory_fhir_subset_terminal_disposition import (
    terminal_disposition_result_json,
)
from process.provider_directory_fhir_subset_terminal_disposition_selection import (
    selected_reviewed_subset_terminal_disposition,
)
from process.provider_directory_fhir_subset_terminal_disposition_evidence import (
    expected_subset_coverage,
)
from process.provider_directory_fhir_subset_completion import canonical_sha256
from process.provider_directory_fhir_subset_identity import (
    server_issued_subset_source_scope_payload,
)
from process.provider_directory_fhir_subset_terminal_disposition_profile import (
    SOURCE_PROFILE_RESOURCE_TYPES,
)
from process.provider_directory_resource_hash import (
    SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
)
from tests.provider_directory_fhir_subset_terminal_disposition_support import (
    CUTOFF,
    TerminalDispositionDatabase,
    _contract,
)


def test_gate_and_identifier_free_result_are_exact(monkeypatch):
    for value in (None, "", "1", "TRUE"):
        if value is None:
            monkeypatch.delenv(TERMINAL_DISPOSITION_ENABLED_ENV, raising=False)
        else:
            monkeypatch.setenv(TERMINAL_DISPOSITION_ENABLED_ENV, value)
        with pytest.raises(ReviewedSubsetTerminalDispositionError) as error:
            require_reviewed_subset_terminal_disposition_gate()
        assert error.value.code == "disabled"

    monkeypatch.setenv(TERMINAL_DISPOSITION_ENABLED_ENV, "true")
    require_reviewed_subset_terminal_disposition_gate()
    assert terminal_disposition_result_json(
        ReviewedSubsetTerminalDispositionResult(disposed=True)
    ) == '{"already_applied":false,"disposed":true,"status":"ok"}'


@pytest.mark.asyncio
async def test_selector_builds_exact_mixed_terminal_marker():
    selection, checkpoint_rows = (
        await selected_reviewed_subset_terminal_disposition(
            TerminalDispositionDatabase(),
            "source-a",
        )
    )

    marker = validated_terminal_disposition_marker(selection.marker_by_field)
    dispositions = [
        resource["disposition"]
        for resource in marker["resource_dispositions"].values()
    ]
    assert dispositions.count(STABLE_COMPLETE_DISPOSITION) == 2
    assert dispositions.count(COUNT_DRIFT_DISPOSITION) == 1
    assert dispositions.count(RETRYABLE_HTTP_500_DISPOSITION) == 4
    assert marker["resource_dispositions"]["Organization"]["disposition"] == (
        STABLE_COMPLETE_DISPOSITION
    )
    assert marker["resource_dispositions"]["Practitioner"]["disposition"] == (
        STABLE_COMPLETE_DISPOSITION
    )
    assert marker["resource_dispositions"]["Location"]["disposition"] == (
        COUNT_DRIFT_DISPOSITION
    )
    assert marker["checkpoint_count"] == len(EXPECTED_RESOURCE_TYPES)
    assert marker["terminal_page_delta"] == 1
    assert marker["resource_count"] == selection.observed_resource_count
    assert marker["proof_row_count"] == selection.observed_resource_count
    assert all(
        len(resource["start_url_sha256"]) == 64
        and len(resource["recent_cursor_hashes_sha256"]) == 64
        for resource in marker["resource_dispositions"].values()
    )
    assert len(checkpoint_rows) == len(EXPECTED_RESOURCE_TYPES)


@pytest.mark.asyncio
async def test_marker_rejects_disposition_swapped_between_resource_types():
    """Bind the observed partition to the exact retained resource names."""

    selection, _checkpoint_rows = (
        await selected_reviewed_subset_terminal_disposition(
            TerminalDispositionDatabase(),
            "source-a",
        )
    )
    marker_by_field = selection.marker_by_field
    resources_by_type = marker_by_field["resource_dispositions"]
    resources_by_type["Organization"], resources_by_type["InsurancePlan"] = (
        resources_by_type["InsurancePlan"],
        resources_by_type["Organization"],
    )

    with pytest.raises(ReviewedSubsetTerminalDispositionError):
        validated_terminal_disposition_marker(marker_by_field)


def _mutate_all_diagnostic_copies(database, mutator):
    metadata = database.candidate_row["publication_metadata_json"]
    candidate_diagnostics = metadata["resource_diagnostics"]
    completion_diagnostics = metadata["completion_proof_v1"][
        "resource_diagnostics"
    ]
    source_diagnostics = database.source_row["metadata_json"][
        "last_resource_import"
    ]["resources"]
    for diagnostics in (
        candidate_diagnostics,
        completion_diagnostics,
        source_diagnostics,
    ):
        mutator(diagnostics)


def _decrement_post_count(diagnostics_by_type, resource_type):
    proof_by_field = diagnostics_by_type[resource_type][
        "server_issued_subset_completeness"
    ]
    proof_by_field["post_count"] -= 1
    proof_by_field["advertised_post"] -= 1


def _mutate_post_count_copies(database, resource_index):
    resource_type = EXPECTED_RESOURCE_TYPES[resource_index]
    _mutate_all_diagnostic_copies(
        database,
        lambda diagnostics_by_type: _decrement_post_count(
            diagnostics_by_type,
            resource_type,
        ),
    )
    checkpoint_proof_by_field = database.checkpoint_rows[resource_index][
        "completeness_json"
    ]
    checkpoint_proof_by_field["post_count"] -= 1
    checkpoint_proof_by_field["advertised_post"] -= 1


def _mutate_source_copy(database):
    first_resource_type = EXPECTED_RESOURCE_TYPES[0]
    source_resources_by_type = database.source_row["metadata_json"][
        "last_resource_import"
    ]["resources"]
    source_resources_by_type[first_resource_type]["rows_fetched"] += 1


def _mutate_retry_cursor(database):
    database.checkpoint_rows[0]["next_url"] = None


def _mutate_policy_count(database):
    for metadata_by_field in (
        database.source_row["metadata_json"],
        database.candidate_row["publication_metadata_json"],
    ):
        metadata_by_field["provider_directory_reviewed_root_policy_v1"][
            "required_root_count"
        ] = 2


def _mutate_legacy_marker(database):
    database.candidate_row["publication_metadata_json"][
        ABANDONMENT_METADATA_KEY
    ] = {"contract_version": "synthetic"}


def _mutate_candidate_root_metadata(database):
    database.candidate_row["publication_metadata_json"][
        "acquisition_root_run_id"
    ] = "contradictory-root"


def _add_unexpected_diagnostic_field(database):
    _mutate_all_diagnostic_copies(
        database,
        lambda diagnostics_by_type: diagnostics_by_type[
            EXPECTED_RESOURCE_TYPES[0]
        ].update({"unexpected_terminal_state": True}),
    )


def _add_unexpected_proof_field(database):
    resource_type = EXPECTED_RESOURCE_TYPES[0]

    def mutate_diagnostic_proof(diagnostics_by_type):
        diagnostics_by_type[resource_type][
            "server_issued_subset_completeness"
        ]["unexpected_proof_state"] = True

    _mutate_all_diagnostic_copies(database, mutate_diagnostic_proof)
    database.checkpoint_rows[0]["completeness_json"][
        "unexpected_proof_state"
    ] = True


def _mutate_checkpoint_hash(database):
    database.checkpoint_rows[0]["start_url_hash"] = "not-a-sha256"


def _mutate_source_campaign(database):
    database.source_row["metadata_json"][
        "provider_directory_verification_campaign_id"
    ] = "different-campaign"


def _mutate_candidate_hash_contract(database):
    database.candidate_row["publication_metadata_json"][
        "resource_hash_contract"
    ] = SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT


def _mutate_reused_state(database):
    database.candidate_row["publication_metadata_json"][
        "reused_from_checkpoint"
    ] = False


def _mutate_coverage(database):
    _mutate_all_diagnostic_copies(
        database,
        lambda diagnostics_by_type: diagnostics_by_type[
            EXPECTED_RESOURCE_TYPES[0]
        ]["server_issued_subset_coverage"].update({"proof_state": "verified"}),
    )


def _mutate_recent_cursor_history(database):
    start_hash = database.checkpoint_rows[0]["start_url_hash"]
    database.checkpoint_rows[0]["recent_cursor_hashes"] = [
        start_hash,
        start_hash,
    ]


def _mutate_contract_identity(database):
    def mutate_proof(diagnostics_by_type):
        for resource_type in EXPECTED_RESOURCE_TYPES:
            diagnostics_by_type[resource_type][
                "server_issued_subset_completeness"
            ]["contract_identity"] = "z" * 64

    _mutate_all_diagnostic_copies(database, mutate_proof)
    for checkpoint in database.checkpoint_rows:
        checkpoint["completeness_json"]["contract_identity"] = "z" * 64


def _mutate_retryable_pre_count(database):
    resource_type = EXPECTED_RESOURCE_TYPES[0]

    def mutate_proof(diagnostics_by_type):
        diagnostics_by_type[resource_type][
            "server_issued_subset_completeness"
        ]["pre_count"] = 0

    _mutate_all_diagnostic_copies(database, mutate_proof)
    database.checkpoint_rows[0]["completeness_json"]["pre_count"] = 0


def _add_completion_envelope_field(database):
    database.candidate_row["publication_metadata_json"]["completion_proof_v1"][
        "unexpected"
    ] = True


def _add_source_import_envelope_field(database):
    database.source_row["metadata_json"]["last_resource_import"][
        "unexpected"
    ] = True


def _mutate_source_import_observed_at(database):
    database.source_row["metadata_json"]["last_resource_import"][
        "observed_at"
    ] = 17


def _mutate_source_import_calendar_date(database):
    database.source_row["metadata_json"]["last_resource_import"][
        "observed_at"
    ] = "2026-02-30T12:00:00Z"


def _mutate_source_auth(database):
    database.source_row["auth_type"] = "bearer"


def _mutate_coherent_source_start_url(database):
    resource_type = "Organization"
    source_metadata = database.source_row["metadata_json"]
    start_url_by_type = source_metadata[
        "provider_directory_current_version_census_start_urls"
    ]
    start_url_by_type[resource_type] = (
        "https://directory.example.test/alternate/Organization"
    )
    contract = replace(
        _contract(),
        start_urls=tuple(
            (name, start_url_by_type[name])
            for name in SOURCE_PROFILE_RESOURCE_TYPES
        ),
    )
    contract_identity = current_version_census_proof_identity(contract)

    def mutate_proof(diagnostics_by_type):
        for diagnostic in diagnostics_by_type.values():
            diagnostic["server_issued_subset_completeness"][
                "contract_identity"
            ] = contract_identity

    _mutate_all_diagnostic_copies(database, mutate_proof)
    source_scope_sha256 = canonical_sha256(
        server_issued_subset_source_scope_payload(
            database.source_row,
            ("source-a",),
            CUTOFF,
            database.source_row["canonical_api_base"],
        )
    )
    for checkpoint in database.checkpoint_rows:
        checkpoint["source_scope_hash"] = source_scope_sha256
        checkpoint["completeness_json"][
            "contract_identity"
        ] = contract_identity
        if checkpoint["resource_type"] == resource_type:
            start_hash = hashlib.sha256(
                contract.start_url(resource_type, contract.page_count).encode()
            ).hexdigest()
            checkpoint["start_url_hash"] = start_hash
            checkpoint["recent_cursor_hashes"][0] = start_hash


def _mutate_terminal_geometry(database, resource_type):
    def mutate_proof(diagnostics_by_type):
        diagnostic = diagnostics_by_type[resource_type]
        proof = diagnostic["server_issued_subset_completeness"]
        proof["terminal_page_geometry"]["version"] = 999
        diagnostic["server_issued_subset_coverage"] = expected_subset_coverage(
            proof
        )

    _mutate_all_diagnostic_copies(database, mutate_proof)
    resource_index = EXPECTED_RESOURCE_TYPES.index(resource_type)
    database.checkpoint_rows[resource_index]["completeness_json"][
        "terminal_page_geometry"
    ]["version"] = 999


_SELECTOR_MUTATION_BY_NAME = {
    "source_copy": _mutate_source_copy,
    "count_drift_two": lambda database: _mutate_post_count_copies(database, 2),
    "retry_without_cursor": _mutate_retry_cursor,
    "completed_unstable": lambda database: _mutate_post_count_copies(database, 3),
    "policy_two": _mutate_policy_count,
    "legacy_marker": _mutate_legacy_marker,
    "candidate_root_metadata": _mutate_candidate_root_metadata,
    "unexpected_diagnostic_field": _add_unexpected_diagnostic_field,
    "unexpected_proof_field": _add_unexpected_proof_field,
    "checkpoint_hash": _mutate_checkpoint_hash,
    "source_campaign": _mutate_source_campaign,
    "candidate_hash_contract": _mutate_candidate_hash_contract,
    "reused_state": _mutate_reused_state,
    "coverage": _mutate_coverage,
    "recent_cursor_history": _mutate_recent_cursor_history,
    "contract_identity": _mutate_contract_identity,
    "retryable_pre_count": _mutate_retryable_pre_count,
    "stable_terminal_geometry": lambda database: _mutate_terminal_geometry(
        database,
        "Organization",
    ),
    "drift_terminal_geometry": lambda database: _mutate_terminal_geometry(
        database,
        "Location",
    ),
    "completion_envelope": _add_completion_envelope_field,
    "source_import_envelope": _add_source_import_envelope_field,
    "source_import_observed_at": _mutate_source_import_observed_at,
    "source_import_calendar_date": _mutate_source_import_calendar_date,
    "source_auth": _mutate_source_auth,
    "coherent_source_start_url": _mutate_coherent_source_start_url,
}


@pytest.mark.parametrize(
    "mutation",
    (
        "source_copy",
        "count_drift_two",
        "retry_without_cursor",
        "completed_unstable",
        "policy_two",
        "legacy_marker",
        "candidate_root_metadata",
        "unexpected_diagnostic_field",
        "unexpected_proof_field",
        "checkpoint_hash",
        "source_campaign",
        "candidate_hash_contract",
        "reused_state",
        "coverage",
        "recent_cursor_history",
        "contract_identity",
        "retryable_pre_count",
        "stable_terminal_geometry",
        "drift_terminal_geometry",
        "completion_envelope",
        "source_import_envelope",
        "source_import_observed_at",
        "source_import_calendar_date",
        "source_auth",
        "coherent_source_start_url",
    ),
)
@pytest.mark.asyncio
async def test_selector_rejects_copy_partition_policy_and_marker_drift(mutation):
    database = TerminalDispositionDatabase()
    _SELECTOR_MUTATION_BY_NAME[mutation](database)

    with pytest.raises(ReviewedSubsetTerminalDispositionError) as error:
        await selected_reviewed_subset_terminal_disposition(
            database,
            "source-a",
        )
    assert error.value.code == "evidence"
