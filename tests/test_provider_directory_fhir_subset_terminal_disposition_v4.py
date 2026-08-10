# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Direct-v4 coverage for the existing terminal-disposition transaction."""

from __future__ import annotations

from copy import deepcopy

import pytest

from process.provider_directory_fhir_subset_terminal_disposition import (
    dispose_v4_census_drift_root,
    require_v4_disposition_gate,
)
from process import provider_directory_fhir_subset_terminal_disposition as facade
from process.provider_directory_fhir_subset_terminal_disposition_contract import (
    TERMINAL_DISPOSITION_METADATA_KEY,
    ReviewedSubsetTerminalDispositionError,
    ReviewedSubsetTerminalDispositionResult,
    validated_terminal_disposition_marker,
)
from process.provider_directory_fhir_subset_terminal_disposition_profile import (
    DIRECT_V4_CONTRACT_VERSION,
    DIRECT_V4_DISPOSITION_BY_RESOURCE_TYPE,
    DIRECT_V4_DRIFT_RESOURCE_TYPES,
    DIRECT_V4_TERMINAL_DISPOSITION_ENABLED_ENV,
    DIRECT_V4_TERMINAL_MARKER_SHA256,
    DIRECT_V4_VERIFIED_RESOURCE_TYPES,
    TERMINAL_CENSUS_DRIFT_DISPOSITION,
    VERIFIED_COMPLETE_DISPOSITION,
)
from process.provider_directory_fhir_subset_terminal_disposition_store import (
    sync_v4_terminal_disposition,
)
from process.provider_directory_fhir_subset_terminal_disposition_selection import (
    _locked_candidate_row as locked_legacy_candidate_row,
)
from process.provider_directory_fhir_subset_terminal_disposition_v4_selection import (
    selected_direct_v4_terminal_disposition,
)
from process.provider_directory_fhir_subset_terminal_disposition_v4_contract import (
    validated_direct_v4_terminal_marker,
)
from process import (
    provider_directory_fhir_subset_terminal_disposition_v4_selection
    as v4_selection,
)
from tests.provider_directory_fhir_subset_terminal_disposition_v4_support import (
    DirectV4TerminalDatabase,
)


_SYNTHETIC_MARKER_SHA256 = (
    "e75f1f8addca0bc3079bb164baa6dc7bf39e0e424a0c8f8c53d2a3cdeae96489"
)


@pytest.fixture(autouse=True)
def _bind_synthetic_marker(monkeypatch):
    """Bind neutral fixtures while production retains the live marker digest."""

    monkeypatch.setattr(
        v4_selection,
        "DIRECT_V4_TERMINAL_MARKER_SHA256",
        _SYNTHETIC_MARKER_SHA256,
    )


def _proof_copies(database: DirectV4TerminalDatabase, resource_type: str):
    candidate_metadata = database.candidate_row["publication_metadata_json"]
    source_metadata = database.source_row["metadata_json"]
    diagnostic_copies = (
        candidate_metadata["resource_diagnostics"],
        candidate_metadata["completion_proof_v1"]["resource_diagnostics"],
        source_metadata["last_resource_import"]["resources"],
    )
    for diagnostics_by_type in diagnostic_copies:
        yield diagnostics_by_type[resource_type][
            "server_issued_subset_completeness"
        ]
    checkpoint = next(
        row
        for row in database.checkpoint_rows
        if row["resource_type"] == resource_type
    )
    yield checkpoint["completeness_json"]


def test_direct_v4_profile_is_the_exact_final_four_three_partition():
    """Freeze the observed terminal acquisition outcome, not a generic rule."""

    assert DIRECT_V4_DRIFT_RESOURCE_TYPES == (
        "HealthcareService",
        "OrganizationAffiliation",
        "PractitionerRole",
    )
    assert len(DIRECT_V4_VERIFIED_RESOURCE_TYPES) == 4
    assert DIRECT_V4_DISPOSITION_BY_RESOURCE_TYPE == {
        **{
            resource_type: VERIFIED_COMPLETE_DISPOSITION
            for resource_type in DIRECT_V4_VERIFIED_RESOURCE_TYPES
        },
        **{
            resource_type: TERMINAL_CENSUS_DRIFT_DISPOSITION
            for resource_type in DIRECT_V4_DRIFT_RESOURCE_TYPES
        },
    }
    assert DIRECT_V4_TERMINAL_MARKER_SHA256 == (
        "e6f19eb70f8b5a84c76e61c19c379541bb6865b7de3114de01dd2a32181cb299"
    )


def test_direct_v4_gate_is_exact_and_default_off(monkeypatch):
    for value in (None, "", "1", "TRUE"):
        if value is None:
            monkeypatch.delenv(
                DIRECT_V4_TERMINAL_DISPOSITION_ENABLED_ENV,
                raising=False,
            )
        else:
            monkeypatch.setenv(
                DIRECT_V4_TERMINAL_DISPOSITION_ENABLED_ENV,
                value,
            )
        with pytest.raises(ReviewedSubsetTerminalDispositionError) as error:
            require_v4_disposition_gate()
        assert error.value.code == "disabled"

    monkeypatch.setenv(DIRECT_V4_TERMINAL_DISPOSITION_ENABLED_ENV, "true")
    require_v4_disposition_gate()


@pytest.mark.asyncio
async def test_selector_builds_exact_direct_v4_marker():
    database = DirectV4TerminalDatabase()

    selection, checkpoint_rows = await selected_direct_v4_terminal_disposition(
        database,
        "source-a",
    )

    marker = validated_terminal_disposition_marker(selection.marker_by_field)
    disposition_by_type = {
        resource_type: resource["disposition"]
        for resource_type, resource in marker["resource_dispositions"].items()
    }
    assert marker["contract_version"] == DIRECT_V4_CONTRACT_VERSION
    assert disposition_by_type == DIRECT_V4_DISPOSITION_BY_RESOURCE_TYPE
    assert marker["terminal_page_delta"] == 3
    assert marker["checkpoint_count"] == 7
    assert marker["checkpoint_rows_processed"] == 1_750
    assert marker["resource_count"] == 1_750
    assert marker["proof_row_count"] == 1_750
    assert marker["direct_lineage"] == {
        "checkpoint_retry_count": 0,
        "competing_candidate_count": 0,
        "current_dataset_count": 0,
        "import_run_row_count": 0,
        "owner_equals_root": True,
        "previous_dataset_present": False,
        "previous_reference_count": 0,
    }
    assert len(checkpoint_rows) == 7


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field_path", "replacement"),
    (
        (("checkpoint_count",), -1),
        (("resource_dispositions", "InsurancePlan"), None),
        (("resource_dispositions", "InsurancePlan", "diagnostic_sha256"), "bad"),
        (("resource_dispositions", "InsurancePlan", "checkpoint_state"), "active"),
        (("direct_lineage",), {}),
        (("direct_lineage", "import_run_row_count"), 1),
        (("contract_version",), "bad"),
        (("resource_dispositions",), {}),
        (("checkpoint_count",), 8),
        (("proof_shard_count",), 0),
        (("source_import_sha256",), "bad"),
    ),
)
async def test_marker_validator_rejects_closed_contract_mutations(
    field_path: tuple[str, ...],
    replacement: object,
):
    database = DirectV4TerminalDatabase()
    selection, _checkpoint_rows = await selected_direct_v4_terminal_disposition(
        database,
        "source-a",
    )
    invalid_marker_by_field = deepcopy(selection.marker_by_field)
    container_by_field = invalid_marker_by_field
    for field_name in field_path[:-1]:
        container_by_field = container_by_field[field_name]
    container_by_field[field_path[-1]] = replacement

    with pytest.raises(ReviewedSubsetTerminalDispositionError):
        validated_direct_v4_terminal_marker(invalid_marker_by_field)


@pytest.mark.asyncio
async def test_direct_v4_facade_uses_catalog_and_runtime_database(monkeypatch):
    from db import connection as connection_module
    from process import provider_directory_fhir_manual_catalog as catalog

    selected_database = object()
    expected_result = ReviewedSubsetTerminalDispositionResult(disposed=True)
    monkeypatch.setenv(DIRECT_V4_TERMINAL_DISPOSITION_ENABLED_ENV, "true")
    monkeypatch.setattr(connection_module, "db", selected_database)
    monkeypatch.setattr(catalog, "reviewed_manual_census_source_id", lambda: "source-a")

    async def sync(database, source_id):
        assert database is selected_database
        assert source_id == "source-a"
        return expected_result

    monkeypatch.setattr(facade, "sync_v4_terminal_disposition", sync)
    assert await dispose_v4_census_drift_root() == expected_result


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("raised_error", "expected_error"),
    (
        (TimeoutError(), TimeoutError),
        (ReviewedSubsetTerminalDispositionError("evidence"), ReviewedSubsetTerminalDispositionError),
        (RuntimeError("private"), ReviewedSubsetTerminalDispositionError),
    ),
)
async def test_direct_v4_facade_preserves_safe_error_boundary(
    monkeypatch,
    raised_error: Exception,
    expected_error: type[BaseException],
):
    from process import provider_directory_fhir_manual_catalog as catalog

    monkeypatch.setenv(DIRECT_V4_TERMINAL_DISPOSITION_ENABLED_ENV, "true")
    monkeypatch.setattr(catalog, "reviewed_manual_census_source_id", lambda: "source-a")

    async def fail(_database, _source_id):
        raise raised_error

    monkeypatch.setattr(facade, "sync_v4_terminal_disposition", fail)
    with pytest.raises(expected_error) as error:
        await dispose_v4_census_drift_root(database=object())
    if isinstance(error.value, ReviewedSubsetTerminalDispositionError):
        assert error.value.code in {"evidence", "state"}


@pytest.mark.asyncio
async def test_direct_v4_facade_redacts_catalog_failure(monkeypatch):
    from process import provider_directory_fhir_manual_catalog as catalog

    monkeypatch.setenv(DIRECT_V4_TERMINAL_DISPOSITION_ENABLED_ENV, "true")

    def fail_catalog():
        raise ValueError("private")

    monkeypatch.setattr(catalog, "reviewed_manual_census_source_id", fail_catalog)
    with pytest.raises(ReviewedSubsetTerminalDispositionError) as error:
        await dispose_v4_census_drift_root(database=object())
    assert error.value.code == "evidence"


@pytest.mark.asyncio
async def test_single_existing_transaction_disposes_then_replays():
    database = DirectV4TerminalDatabase()

    first = await sync_v4_terminal_disposition(
        database,
        "source-a",
    )
    second = await sync_v4_terminal_disposition(
        database,
        "source-a",
    )

    assert first.disposed is True
    assert second.disposed is False
    assert database.candidate_row["status"] == "acquisition_abandoned"
    marker = database.candidate_row["publication_metadata_json"][
        TERMINAL_DISPOSITION_METADATA_KEY
    ]
    assert marker["contract_version"] == DIRECT_V4_CONTRACT_VERSION
    assert sum(call[0] == "transaction" and call[1] == "begin" for call in database.calls) == 2


@pytest.mark.asyncio
async def test_historical_v1_disposition_cannot_mask_failed_v4_candidate():
    database = DirectV4TerminalDatabase()
    historical = deepcopy(database.candidate_row)
    historical["dataset_id"] = "historical-dataset"
    historical["status"] = "acquisition_abandoned"
    historical["publication_metadata_json"][TERMINAL_DISPOSITION_METADATA_KEY] = {
        "contract_version": (
            "healthporta.provider-directory.reviewed-subset-terminal-"
            "disposition.v1"
        )
    }
    database.candidate_rows.insert(0, historical)

    selection, checkpoint_rows = await selected_direct_v4_terminal_disposition(
        database,
        "source-a",
    )

    assert selection.dataset_id == "dataset-a"
    assert selection.prior_status == "failed"
    assert len(checkpoint_rows) == 7
    candidate_query = next(
        statement
        for call_type, statement, _parameters in database.calls
        if call_type == "all" and "SELECT dataset.*" in statement
    )
    assert "contract_version" in candidate_query
    assert "verification_campaign_id" in candidate_query


@pytest.mark.asyncio
async def test_v1_and_v2_replay_select_their_own_inner_contract():
    database = DirectV4TerminalDatabase()
    await sync_v4_terminal_disposition(
        database,
        "source-a",
    )
    historical = deepcopy(database.candidate_row)
    historical["dataset_id"] = "historical-dataset"
    historical["publication_metadata_json"][TERMINAL_DISPOSITION_METADATA_KEY] = {
        "contract_version": (
            "healthporta.provider-directory.reviewed-subset-terminal-"
            "disposition.v1"
        )
    }
    database.candidate_rows.insert(0, historical)

    direct_selection, direct_rows = (
        await selected_direct_v4_terminal_disposition(database, "source-a")
    )
    legacy_candidate = await locked_legacy_candidate_row(
        database,
        "source-a",
        "endpoint-a",
    )

    assert direct_selection.dataset_id == "dataset-a"
    assert direct_rows == ()
    assert legacy_candidate["dataset_id"] == "historical-dataset"


@pytest.mark.asyncio
async def test_duplicate_matching_v4_candidate_fails_closed():
    database = DirectV4TerminalDatabase()
    duplicate = deepcopy(database.candidate_row)
    duplicate["dataset_id"] = "duplicate-dataset"
    database.candidate_rows.append(duplicate)

    with pytest.raises(ReviewedSubsetTerminalDispositionError) as error:
        await selected_direct_v4_terminal_disposition(database, "source-a")

    assert error.value.code == "evidence"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation_name",
    ("row_count", "marker_hash", "candidate_shape"),
)
async def test_new_selection_fails_closed_at_independent_evidence_boundaries(
    monkeypatch,
    mutation_name: str,
):
    database = DirectV4TerminalDatabase()
    if mutation_name == "row_count":
        database.candidate_row["resource_count"] += 1
    elif mutation_name == "marker_hash":
        monkeypatch.setattr(
            v4_selection,
            "DIRECT_V4_TERMINAL_MARKER_SHA256",
            "f" * 64,
        )
    else:
        database.candidate_row["dataset_hash"] = "f" * 64

    with pytest.raises(ReviewedSubsetTerminalDispositionError):
        await selected_direct_v4_terminal_disposition(database, "source-a")


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation_name", ("marker_hash", "candidate_metadata"))
async def test_replay_fails_closed_at_marker_and_candidate_boundaries(
    monkeypatch,
    mutation_name: str,
):
    database = DirectV4TerminalDatabase()
    await sync_v4_terminal_disposition(database, "source-a")
    if mutation_name == "marker_hash":
        monkeypatch.setattr(
            v4_selection,
            "DIRECT_V4_TERMINAL_MARKER_SHA256",
            "f" * 64,
        )
    else:
        database.candidate_row["publication_metadata_json"]["error"] = "changed"

    with pytest.raises(ReviewedSubsetTerminalDispositionError):
        await selected_direct_v4_terminal_disposition(database, "source-a")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("resource_type", "post_count"),
    (
        ("HealthcareService", 501),
        ("InsurancePlan", 499),
    ),
)
async def test_profile_rejects_wrong_drift_boundary(
    resource_type: str,
    post_count: int,
):
    database = DirectV4TerminalDatabase()
    for proof_by_field in _proof_copies(database, resource_type):
        proof_by_field["post_count"] = post_count
        proof_by_field["advertised_post"] = post_count

    with pytest.raises(ReviewedSubsetTerminalDispositionError) as error:
        await selected_direct_v4_terminal_disposition(database, "source-a")

    assert error.value.code == "evidence"


@pytest.mark.asyncio
async def test_correlated_proof_duplicate_mismatch_is_rejected():
    database = DirectV4TerminalDatabase()
    for proof_by_field in _proof_copies(database, "HealthcareService"):
        proof_by_field["advertised_pre"] = 579

    with pytest.raises(ReviewedSubsetTerminalDispositionError) as error:
        await selected_direct_v4_terminal_disposition(database, "source-a")

    assert error.value.code == "evidence"


@pytest.mark.asyncio
async def test_extra_checkpoint_type_and_retry_lineage_fail_closed():
    extra_checkpoint = DirectV4TerminalDatabase()
    extra = deepcopy(extra_checkpoint.checkpoint_rows[0])
    extra["resource_type"] = "UnexpectedResource"
    extra_checkpoint.checkpoint_rows = (*extra_checkpoint.checkpoint_rows, extra)
    with pytest.raises(ReviewedSubsetTerminalDispositionError):
        await selected_direct_v4_terminal_disposition(
            extra_checkpoint,
            "source-a",
        )

    retry_lineage = DirectV4TerminalDatabase()
    retry_lineage.checkpoint_rows[0]["retry_of_run_id"] = "prior-run"
    with pytest.raises(ReviewedSubsetTerminalDispositionError):
        await selected_direct_v4_terminal_disposition(
            retry_lineage,
            "source-a",
        )


@pytest.mark.asyncio
async def test_malformed_proof_shard_stops_before_aggregate_casts():
    database = DirectV4TerminalDatabase()
    database.invalid_proof_shard_count = 1

    with pytest.raises(ReviewedSubsetTerminalDispositionError):
        await selected_direct_v4_terminal_disposition(database, "source-a")

    aggregate_calls = [
        statement
        for call_type, statement, _parameters in database.calls
        if call_type == "all" and "raw_count.resource_type" in statement
    ]
    assert aggregate_calls == []
    shard_query = next(
        statement
        for call_type, statement, _parameters in database.calls
        if call_type == "scalar"
        and "provider_directory_dataset_proof_shard" in statement
    )
    assert "jsonb_object_keys" in shard_query
    assert "jsonb_typeof(resource.value) <> 'number'" in shard_query
    assert "AS numeric" in shard_query
