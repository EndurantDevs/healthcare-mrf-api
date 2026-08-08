# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Candidate/checkpoint atomicity and quarantine coverage for exact census."""

from __future__ import annotations

import importlib
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process.provider_directory_fhir_census_binding import (
    CurrentVersionCensusContract,
)
from process.provider_directory_fhir_census_contract import (
    CURRENT_VERSION_CENSUS_CONTRACT_FIELD,
    CURRENT_VERSION_CENSUS_SMILE_CONTINUATION_STRATEGY,
)


importer = importlib.import_module("process.provider_directory_fhir")
BASE = "https://directory.example.test/fhir"
CUTOFF = "2026-08-01T12:00:00.000000Z"
RESOURCE_TYPE = "Organization"
PAGE_COUNT = 250


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


def _checkpoint_context() -> importer.PaginationCheckpointContext:
    return importer.PaginationCheckpointContext(
        canonical_api_base=BASE,
        source_scope_hash="a" * 64,
        source_ids=("synthetic-source",),
        owner_run_id="run-1",
        retry_of_run_id=None,
        acquisition_root_run_id="run-1",
        dataset_id="dataset-1",
    )


def _start_url() -> str:
    return _contract().start_url(RESOURCE_TYPE, PAGE_COUNT)


def _cursor_url() -> str:
    return (
        f"{BASE}?_getpages=opaque&_getpagesoffset={PAGE_COUNT}"
        f"&_count={PAGE_COUNT}&_pretty=true"
    )


def _count_bundle(total: int) -> dict[str, object]:
    return {"resourceType": "Bundle", "type": "searchset", "total": total}


def _first_page() -> dict[str, object]:
    return {
        "resourceType": "Bundle",
        "type": "searchset",
        "link": [{"relation": "next", "url": _cursor_url()}],
        "entry": [
            {
                "fullUrl": f"{BASE}/{RESOURCE_TYPE}/org-1",
                "resource": {"resourceType": RESOURCE_TYPE, "id": "org-1"},
            }
        ],
    }


async def _run(spies: SimpleNamespace) -> importer.ResourceFetchResult:
    acquisition = await importer._fetch_resource_rows(
        _source_record(),
        RESOURCE_TYPE,
        per_resource_limit=0,
        page_limit=0,
        page_count=PAGE_COUNT,
        timeout=3,
        run_id="run-1",
        row_batch_handler=spies.rows,
        row_batch_size=100,
        retain_rows=False,
        pagination_checkpoint=_checkpoint_context(),
    )
    assert acquisition is not None
    return acquisition


def _install_checkpoint_failure_harness(
    monkeypatch: pytest.MonkeyPatch,
) -> SimpleNamespace:
    """Install a first-page write followed by a checkpoint failure."""

    spies = SimpleNamespace(
        fetch=AsyncMock(
            side_effect=(
                (200, _count_bundle(501), None, 1),
                (200, _first_page(), None, 1),
            )
        ),
        proof=AsyncMock(),
        checkpoint=AsyncMock(side_effect=RuntimeError("checkpoint failed")),
        rows=AsyncMock(return_value=1),
    )
    monkeypatch.setattr(
        importer,
        "_load_or_initialize_pagination_checkpoint",
        AsyncMock(
            return_value=importer.PaginationResumeState(
                next_url=_start_url(),
                pages_processed=0,
                rows_processed=0,
                recent_url_hashes=(),
            )
        ),
    )
    monkeypatch.setattr(importer, "_fetch_source_json", spies.fetch)
    monkeypatch.setattr(
        importer,
        "_save_pagination_checkpoint_completeness",
        spies.proof,
    )
    monkeypatch.setattr(importer, "_save_pagination_checkpoint", spies.checkpoint)
    monkeypatch.setattr(
        importer,
        "_caresource_unique_candidate_count",
        AsyncMock(return_value=1),
    )
    monkeypatch.setattr(
        importer,
        "parse_fhir_resource",
        Mock(
            return_value=(
                importer.ProviderDirectoryOrganization,
                {"source_id": "synthetic-source", "resource_id": "org-1"},
            )
        ),
    )
    return spies


def _install_ahead_candidate_resume(
    monkeypatch: pytest.MonkeyPatch,
    spies: SimpleNamespace,
    initial_proof: dict[str, object],
) -> None:
    """Replace the failed first attempt with its divergent resume state."""

    monkeypatch.setattr(
        importer,
        "_load_or_initialize_pagination_checkpoint",
        AsyncMock(
            return_value=importer.PaginationResumeState(
                next_url=_start_url(),
                pages_processed=0,
                rows_processed=0,
                recent_url_hashes=(),
                resumed=True,
                completeness=initial_proof,
            )
        ),
    )
    spies.fetch.reset_mock()
    spies.proof.reset_mock()
    spies.rows.reset_mock()
    spies.checkpoint.reset_mock(side_effect=True)


@pytest.mark.asyncio
async def test_checkpoint_failure_is_quarantined_before_retry_transport(
    monkeypatch: pytest.MonkeyPatch,
):
    """Block a retry when its candidate advanced beyond the checkpoint."""

    spies = _install_checkpoint_failure_harness(monkeypatch)

    with pytest.raises(RuntimeError, match="checkpoint failed"):
        await _run(spies)

    spies.rows.assert_awaited_once()
    initial_proof = spies.proof.await_args.args[2]
    _install_ahead_candidate_resume(monkeypatch, spies, initial_proof)

    retry_result = await _run(spies)

    assert "candidate_checkpoint_mismatch" in retry_result.error
    spies.fetch.assert_not_awaited()
    spies.proof.assert_not_awaited()
    spies.rows.assert_not_awaited()
    spies.checkpoint.assert_not_awaited()
