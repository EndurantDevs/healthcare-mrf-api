# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Focused boundary coverage for the current-version census executor."""

from __future__ import annotations

import importlib
import json
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.provider_directory_fhir_census_binding import (
    CurrentVersionCensusContract,
    bind_current_version_census_contract,
)
from process.provider_directory_fhir_census_contract import (
    CURRENT_VERSION_CENSUS_CONTINUATION_STRATEGY_FIELD,
    CURRENT_VERSION_CENSUS_CONTRACT_FIELD,
    CURRENT_VERSION_CENSUS_METADATA_STRATEGY_FIELD,
    CURRENT_VERSION_CENSUS_SEMANTICS,
    CURRENT_VERSION_CENSUS_SMILE_CONTINUATION_STRATEGY,
    CURRENT_VERSION_CENSUS_START_URLS_FIELD,
    CurrentVersionCensusRequest,
)
from process.provider_directory_fhir_census_execution import (
    CURRENT_VERSION_CENSUS_FETCH_MODE,
    current_version_census_initial_proof,
    current_version_census_persisted_pre_count,
    resolved_current_version_census_next_url,
    validated_current_version_census_resume_url,
)


importer = importlib.import_module("process.provider_directory_fhir")
BASE = "https://directory.example.test/fhir"
CUTOFF = "2026-08-01T12:00:00.000000Z"
RESOURCE_TYPE = "Organization"


def _contract() -> CurrentVersionCensusContract:
    return CurrentVersionCensusContract(
        source_id="synthetic-source",
        cutoff=CUTOFF,
        resources=(RESOURCE_TYPE,),
        expected_nonempty_resources=(RESOURCE_TYPE,),
        start_urls=((RESOURCE_TYPE, f"{BASE}/{RESOURCE_TYPE}?active=true"),),
        continuation_strategy=CURRENT_VERSION_CENSUS_SMILE_CONTINUATION_STRATEGY,
    )


def _source_record() -> dict[str, object]:
    return {
        "source_id": "synthetic-source",
        "api_base": BASE,
        "canonical_api_base": BASE,
        "auth_type": "none",
        "last_validated_status": "valid",
        "metadata_json": {
            "provider_directory_manual_only": True,
            "provider_directory_supported_resources": [RESOURCE_TYPE],
            "provider_directory_fully_enumerable_resources": [RESOURCE_TYPE],
        },
        CURRENT_VERSION_CENSUS_CONTRACT_FIELD: _contract(),
    }


def _binding_source_record() -> dict[str, object]:
    source_record = _source_record()
    source_record.pop(CURRENT_VERSION_CENSUS_CONTRACT_FIELD)
    source_record["metadata_json"].update(
        {
            CURRENT_VERSION_CENSUS_METADATA_STRATEGY_FIELD: (
                CURRENT_VERSION_CENSUS_SEMANTICS
            ),
            CURRENT_VERSION_CENSUS_CONTINUATION_STRATEGY_FIELD: (
                CURRENT_VERSION_CENSUS_SMILE_CONTINUATION_STRATEGY
            ),
            "provider_directory_expected_nonempty_resources": [RESOURCE_TYPE],
            CURRENT_VERSION_CENSUS_START_URLS_FIELD: {
                RESOURCE_TYPE: f"{BASE}/{RESOURCE_TYPE}?active=true"
            },
        }
    )
    return source_record


def _request() -> CurrentVersionCensusRequest:
    return CurrentVersionCensusRequest(
        source_id="synthetic-source",
        cutoff=CUTOFF,
        resources=(RESOURCE_TYPE,),
    )


def _cursor(offset: int, *, origin: str = BASE) -> str:
    return (
        f"{origin}?_getpages=opaque&_getpagesoffset={offset}"
        "&_count=250&_pretty=true"
    )


def test_execution_proof_boundaries_reject_unbound_or_invalid_state():
    contract = _contract()
    with pytest.raises(ValueError, match="resource_not_bound"):
        current_version_census_initial_proof(contract, "Location", 0)
    proof_by_field = current_version_census_initial_proof(
        contract,
        RESOURCE_TYPE,
        1,
    )
    proof_by_field["pre_count"] = True
    with pytest.raises(ValueError, match="checkpoint_pre_count_invalid"):
        current_version_census_persisted_pre_count(
            proof_by_field,
            contract,
            RESOURCE_TYPE,
        )


def test_resume_url_boundaries_reject_each_invalid_state():
    contract = _contract()
    start_url = contract.start_url(RESOURCE_TYPE, 250)
    with pytest.raises(ValueError, match="resume_start_url_invalid"):
        validated_current_version_census_resume_url(
            contract, RESOURCE_TYPE, f"{BASE}/Location", start_url,
            pages_processed=0, rows_processed=0, expected_page_count=250,
        )
    with pytest.raises(ValueError, match="resume_url_invalid"):
        validated_current_version_census_resume_url(
            contract, RESOURCE_TYPE, start_url, None,
            pages_processed=0, rows_processed=0, expected_page_count=250,
        )
    with pytest.raises(ValueError, match="resume_url_invalid"):
        validated_current_version_census_resume_url(
            contract, RESOURCE_TYPE, start_url, start_url,
            pages_processed=1, rows_processed=1, expected_page_count=250,
        )
    with pytest.raises(ValueError, match="resume_offset_invalid"):
        validated_current_version_census_resume_url(
            contract, RESOURCE_TYPE, start_url, _cursor(2),
            pages_processed=1, rows_processed=1, expected_page_count=250,
        )


def test_resume_url_rejects_invalid_page_state_and_malformed_port():
    contract = _contract()
    start_url = contract.start_url(RESOURCE_TYPE, 250)
    with pytest.raises(ValueError, match="page_state_invalid"):
        validated_current_version_census_resume_url(
            contract, RESOURCE_TYPE, start_url, start_url,
            pages_processed=0, rows_processed=True, expected_page_count=250,
        )
    with pytest.raises(ValueError, match="untrusted_current_version"):
        validated_current_version_census_resume_url(
            contract,
            RESOURCE_TYPE,
            start_url,
            _cursor(1, origin="https://directory.example.test:bad/fhir"),
            pages_processed=1,
            rows_processed=1,
            expected_page_count=250,
        )


def test_continuation_rejects_an_unreviewed_strategy():
    contract = replace(_contract(), continuation_strategy="synthetic-cursor-v2")
    with pytest.raises(ValueError, match="continuation_unsupported"):
        resolved_current_version_census_next_url(
            contract,
            RESOURCE_TYPE,
            contract.start_url(RESOURCE_TYPE, 250),
            _cursor(250),
            page_entry_count=250,
            expected_page_count=250,
        )


def test_binding_rejects_an_unreviewed_continuation_strategy():
    source_record = _binding_source_record()
    source_record["metadata_json"][
        CURRENT_VERSION_CENSUS_CONTINUATION_STRATEGY_FIELD
    ] = "synthetic-cursor-v2"
    with pytest.raises(ValueError, match="continuation_strategy_not_reviewed"):
        bind_current_version_census_contract(_request(), [source_record])


def test_exact_source_helper_boundaries_fail_closed():
    assert importer._validate_current_version_census_source_transport({}) is None
    source_record = _source_record()
    source_record["metadata_json"].pop("provider_directory_supported_resources")
    assert importer._resource_start_url(
        source_record,
        "Location",
        page_count=250,
    ) is None
    assert importer._candidate_metadata_urls(
        {CURRENT_VERSION_CENSUS_CONTRACT_FIELD: _contract()}
    ) == []
    source_record["requires_api_key"] = True
    with pytest.raises(ValueError, match="credentials_forbidden"):
        importer._validate_current_version_census_source_transport(source_record)


@pytest.mark.parametrize(
    ("payload", "expected_error"),
    (
        ({"resourceType": "Bundle", "type": "collection"}, "searchset_required"),
        (
            {"resourceType": "Bundle", "type": "searchset", "entry": {}},
            "entries_invalid",
        ),
        (
            {"resourceType": "Bundle", "type": "searchset", "entry": [{}]},
            "entries_invalid",
        ),
    ),
)
def test_exact_bundle_shape_boundaries(payload, expected_error):
    assert expected_error in importer._current_version_census_bundle_error(
        payload,
        RESOURCE_TYPE,
    )


def test_exact_pagination_helper_boundaries_fail_closed():
    assert importer._current_version_census_next_link(
        {"link": [{"relation": "self", "url": BASE}]}
    ) is None
    with pytest.raises(ValueError, match="page_count_invalid"):
        importer._current_version_census_page_count(f"{BASE}/{RESOURCE_TYPE}")
    with pytest.raises(ValueError, match="page_state_required"):
        importer._resolved_current_version_census_page_url(
            _source_record(),
            _contract().start_url(RESOURCE_TYPE, 250),
            _cursor(250),
            None,
            250,
        )


@pytest.mark.asyncio
async def test_exact_one_shot_fetch_records_its_terminal_diagnostic(monkeypatch):
    source_record = _source_record()
    fetch_once = AsyncMock(return_value=(429, {}, None, 7))
    monkeypatch.setattr(importer, "_fetch_source_json_once", fetch_once)
    result = await importer._fetch_source_json(
        source_record,
        _contract().start_url(RESOURCE_TYPE, 250),
        timeout=3,
    )
    assert result == (429, {}, None, 7)
    assert source_record[importer.SOURCE_FETCH_DIAGNOSTIC_FIELD][
        "response_class"
    ] == "transient_rate_limited"
    fetch_once.assert_awaited_once()


@pytest.mark.asyncio
async def test_source_transport_and_search_helpers_fail_closed(monkeypatch):
    response = SimpleNamespace(status=200, headers={})
    monkeypatch.setattr(
        importer,
        "_read_source_http_response_body",
        AsyncMock(side_effect=RuntimeError("synthetic read failure")),
    )
    with pytest.raises(RuntimeError, match="synthetic read failure"):
        await importer._read_source_http_payload(response, timeout=3)

    assert importer._resource_search_payload_error(
        _source_record(),
        f"{BASE}/{RESOURCE_TYPE}",
        200,
        {"resourceType": "Parameters"},
    ) is None
    assert importer._has_operation_outcome_error({}) is True
    assert await importer._provider_directory_profile_delta_capacity_projection(
        None,
        {},
        None,
        pending_commit_items=0,
    ) is None


def test_generic_validation_helpers_reject_incomplete_boundaries():
    assert importer._provider_directory_profile_cutover_receipt_identity({}) is None
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="profile_cutover_run_missing",
    ):
        importer._validate_profile_cutover_semantics(
            {},
            None,
            None,
            {},
            {},
            "synthetic-hash",
        )
    assert importer._profile_selection_lineage(None) is None
    with pytest.raises(RuntimeError, match="serving_adoption_invalid"):
        importer._provider_directory_profile_adoption_result(
            "{}",
            generation_id="synthetic-generation",
        )


def test_exact_lineage_boundaries_cover_fresh_and_retry_roots():
    with pytest.raises(ValueError, match="run_id_required"):
        importer._validate_current_version_census_lineage(None, None, None)
    importer._validate_current_version_census_lineage("fresh", None, None)
    for retry_of_run_id, root_run_id in (
        ("retry", "root"),
        ("root", None),
        ("root", "retry"),
    ):
        with pytest.raises(ValueError, match="retry_lineage_invalid"):
            importer._validate_current_version_census_lineage(
                "retry",
                retry_of_run_id,
                root_run_id,
            )
    importer._validate_current_version_census_lineage("retry", "root", "root")


def test_exact_endpoint_and_terminal_metadata_are_preserved():
    census_identity_by_field = {"source_id": "synthetic-source", "cutoff": CUTOFF}
    endpoint_metadata = importer._provider_directory_endpoint_metadata(
        {"current_version_census_identity": json.dumps(census_identity_by_field)}
    )
    assert endpoint_metadata == {
        "identity_version": "resource-import-group-v3",
        "current_version_census_identity": census_identity_by_field,
    }
    diagnostics_by_resource = {
        RESOURCE_TYPE: {
            "fetch_mode": CURRENT_VERSION_CENSUS_FETCH_MODE,
            "error": f"{importer.CURRENT_VERSION_CENSUS_BLOCKED_ERROR}:synthetic",
        }
    }
    assert "synthetic" in importer._current_version_census_terminal_failure_details(
        diagnostics_by_resource,
        [RESOURCE_TYPE],
    )


@pytest.mark.asyncio
async def test_exact_finalized_replay_keeps_source_aware_validation(monkeypatch):
    replay_result = object()
    replay = AsyncMock(return_value=replay_result)
    cancellation = SimpleNamespace(check=AsyncMock())
    monkeypatch.setattr(
        importer,
        "_replay_finalized_candidate_and_clear_checkpoints",
        replay,
    )
    candidate = SimpleNamespace(dataset_id="dataset-1")
    source_records = [_source_record()]
    assert await importer._replay_finalized_candidate_after_cancellation(
        candidate,
        {},
        source_records,
        cancellation,
    ) is replay_result
    replay.assert_awaited_once_with(candidate, {}, source_records)


def test_exact_diagnostics_reject_ambiguous_sources_or_missing_proof():
    source_record = _source_record()
    with pytest.raises(RuntimeError, match="source_group_invalid"):
        importer._validate_current_version_census_diagnostics(
            [source_record, _source_record()],
            {},
        )
    with pytest.raises(RuntimeError, match="proof_incomplete"):
        importer._validate_current_version_census_diagnostics(
            [source_record],
            {
                RESOURCE_TYPE: {
                    "fetch_mode": "paged",
                    "rows_fetched": 0,
                    "pages_fetched": 0,
                }
            },
        )
