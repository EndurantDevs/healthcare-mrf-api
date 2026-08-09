# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Focused v2 logical-window persistence and failure-gate coverage."""

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
from process.provider_directory_fhir_census_execution import (
    CURRENT_VERSION_CENSUS_RETRYABLE_ERROR,
    current_version_census_checkpoint_proof,
    current_version_census_initial_proof,
    validated_current_version_census_resume_url,
)
from process.provider_directory_fhir_census_page_geometry import (
    validate_current_version_census_checkpoint_geometry,
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


def _cursor_url(offset: int) -> str:
    return (
        f"{BASE}?_getpages=opaque&_getpagesoffset={offset}"
        f"&_count={PAGE_COUNT}&_pretty=true"
    )


def _count_bundle(total: int) -> dict[str, object]:
    return {"resourceType": "Bundle", "type": "searchset", "total": total}


def _resource_bundle(
    resource_ids: tuple[str, ...],
    *,
    next_url: str | None = None,
) -> dict[str, object]:
    links = [] if next_url is None else [{"relation": "next", "url": next_url}]
    return {
        "resourceType": "Bundle",
        "type": "searchset",
        "link": links,
        "entry": [
            {
                "fullUrl": f"{BASE}/{RESOURCE_TYPE}/{resource_id}",
                "resource": {
                    "resourceType": RESOURCE_TYPE,
                    "id": resource_id,
                },
            }
            for resource_id in resource_ids
        ],
    }


def _parsed_row(resource_id: str) -> tuple[type, dict[str, str]]:
    return (
        importer.ProviderDirectoryOrganization,
        {"source_id": "synthetic-source", "resource_id": resource_id},
    )


def _pristine_resume() -> importer.PaginationResumeState:
    return importer.PaginationResumeState(
        next_url=_start_url(),
        pages_processed=0,
        rows_processed=0,
        recent_url_hashes=(),
    )


def _install_acquisition_spies(
    monkeypatch: pytest.MonkeyPatch,
    fetch_side_effect: tuple[object, ...],
) -> SimpleNamespace:
    operation_spies = SimpleNamespace(
        fetch=AsyncMock(side_effect=fetch_side_effect),
        proof_write=AsyncMock(),
        checkpoint_write=AsyncMock(),
        row_write=AsyncMock(return_value=0),
    )
    monkeypatch.setattr(
        importer,
        "_load_or_initialize_pagination_checkpoint",
        AsyncMock(return_value=_pristine_resume()),
    )
    monkeypatch.setattr(importer, "_fetch_source_json", operation_spies.fetch)
    monkeypatch.setattr(
        importer,
        "_save_pagination_checkpoint_completeness",
        operation_spies.proof_write,
    )
    monkeypatch.setattr(
        importer,
        "_save_pagination_checkpoint",
        operation_spies.checkpoint_write,
    )
    monkeypatch.setattr(
        importer,
        "_caresource_unique_candidate_count",
        AsyncMock(return_value=1),
    )
    return operation_spies


async def _run_acquisition(
    operation_spies: SimpleNamespace,
) -> importer.ResourceFetchResult:
    acquisition = await importer._fetch_resource_rows(
        _source_record(),
        RESOURCE_TYPE,
        per_resource_limit=0,
        page_limit=0,
        page_count=PAGE_COUNT,
        timeout=3,
        run_id="run-1",
        row_batch_handler=operation_spies.row_write,
        row_batch_size=100,
        retain_rows=False,
        pagination_checkpoint=_checkpoint_context(),
    )
    assert acquisition is not None
    return acquisition


def _sparse_checkpoint_proof(
    pages_processed: int,
    rows_processed: int,
) -> dict[str, object]:
    proof_by_field = current_version_census_initial_proof(
        _contract(),
        RESOURCE_TYPE,
        1000,
        expected_page_count=PAGE_COUNT,
    )
    for page_number in range(1, pages_processed + 1):
        page_entries = rows_processed if page_number == pages_processed else 0
        proof_by_field = current_version_census_checkpoint_proof(
            proof_by_field,
            pages_processed=page_number,
            rows_processed=page_entries,
            page_entry_count=page_entries,
            expected_page_count=PAGE_COUNT,
        )
    return proof_by_field


@pytest.mark.parametrize(("pages_processed", "rows_processed"), ((1, 0), (2, 1)))
def test_resume_accepts_sparse_page_row_shapes(
    pages_processed: int,
    rows_processed: int,
):
    next_url = _cursor_url(pages_processed * PAGE_COUNT)
    assert validated_current_version_census_resume_url(
        _contract(),
        RESOURCE_TYPE,
        _start_url(),
        next_url,
        pages_processed=pages_processed,
        rows_processed=rows_processed,
        expected_page_count=PAGE_COUNT,
        proof=_sparse_checkpoint_proof(pages_processed, rows_processed),
    ) == next_url


@pytest.mark.parametrize(
    ("pages_processed", "rows_processed"),
    ((0, 1), (1, 251), (4, 1)),
)
def test_resume_rejects_impossible_page_row_shapes(
    pages_processed: int,
    rows_processed: int,
):
    proof_by_field = current_version_census_initial_proof(
        _contract(),
        RESOURCE_TYPE,
        1000,
        expected_page_count=PAGE_COUNT,
    )
    proof_by_field["page_geometry"] = {
        "version": 2,
        "page_count": PAGE_COUNT,
        "checkpointed_pages": pages_processed,
        "checkpointed_rows": rows_processed,
        "logical_next_offset": pages_processed * PAGE_COUNT,
        "sparse_pages": 0,
        "empty_pages": 0,
    }
    with pytest.raises(ValueError, match="resume_state_invalid"):
        validated_current_version_census_resume_url(
            _contract(),
            RESOURCE_TYPE,
            _start_url(),
            _cursor_url(pages_processed * PAGE_COUNT),
            pages_processed=pages_processed,
            rows_processed=rows_processed,
            expected_page_count=PAGE_COUNT,
            proof=proof_by_field,
        )


@pytest.mark.parametrize(
    (
        "page_count",
        "pages_processed",
        "rows_processed",
        "sparse_pages",
        "empty_pages",
    ),
    ((250, 2, 500, 2, 0), (1, 1, 0, 1, 0)),
)
def test_checkpoint_geometry_rejects_impossible_sparse_aggregates(
    page_count: int,
    pages_processed: int,
    rows_processed: int,
    sparse_pages: int,
    empty_pages: int,
):
    proof_by_field = {
        "page_geometry": {
            "version": 2,
            "page_count": page_count,
            "checkpointed_pages": pages_processed,
            "checkpointed_rows": rows_processed,
            "logical_next_offset": pages_processed * page_count,
            "sparse_pages": sparse_pages,
            "empty_pages": empty_pages,
        }
    }

    with pytest.raises(ValueError, match="page_geometry_invalid"):
        validate_current_version_census_checkpoint_geometry(
            proof_by_field,
            pages_processed=pages_processed,
            rows_processed=rows_processed,
            expected_page_count=page_count,
        )


@pytest.mark.parametrize(
    ("field_name", "invalid_value"),
    (("page_count", "250"), ("sparse_pages", True), ("empty_pages", None)),
)
def test_checkpoint_geometry_rejects_noninteger_metrics(
    field_name: str,
    invalid_value: object,
):
    geometry_by_field: dict[str, object] = {
        "version": 2,
        "page_count": PAGE_COUNT,
        "checkpointed_pages": 1,
        "checkpointed_rows": PAGE_COUNT,
        "logical_next_offset": PAGE_COUNT,
        "sparse_pages": 0,
        "empty_pages": 0,
    }
    geometry_by_field[field_name] = invalid_value

    with pytest.raises(ValueError, match="page_geometry_invalid"):
        validate_current_version_census_checkpoint_geometry(
            {"page_geometry": geometry_by_field},
            pages_processed=1,
            rows_processed=PAGE_COUNT,
            expected_page_count=PAGE_COUNT,
        )


@pytest.mark.asyncio
async def test_empty_window_checkpoint_survives_transient_transport(
    monkeypatch: pytest.MonkeyPatch,
):
    operation_spies = _install_acquisition_spies(
        monkeypatch,
        (
            (200, _count_bundle(501), None, 1),
            (200, _resource_bundle((), next_url=_cursor_url(250)), None, 1),
            (503, {}, "transient", 1),
        ),
    )
    acquisition = await _run_acquisition(operation_spies)
    assert acquisition.error == (
        f"{CURRENT_VERSION_CENSUS_RETRYABLE_ERROR}:transient"
    )
    assert acquisition.next_url_remaining is True
    operation_spies.row_write.assert_not_awaited()
    operation_spies.checkpoint_write.assert_awaited_once()
    checkpoint_by_field = operation_spies.checkpoint_write.await_args.kwargs
    assert checkpoint_by_field["next_url"] == _cursor_url(250)
    assert checkpoint_by_field["pages_processed"] == 1
    assert checkpoint_by_field["rows_processed"] == 0
    assert checkpoint_by_field["completeness"]["page_geometry"][
        "empty_pages"
    ] == 1


@pytest.mark.asyncio
async def test_failed_link_stops_before_candidate_or_checkpoint_persistence(
    monkeypatch: pytest.MonkeyPatch,
):
    operation_spies = _install_acquisition_spies(
        monkeypatch,
        (
            (200, _count_bundle(501), None, 1),
            (
                200,
                _resource_bundle(("org-1",), next_url=_cursor_url(249)),
                None,
                1,
            ),
        ),
    )
    parser = Mock(return_value=_parsed_row("org-1"))
    monkeypatch.setattr(importer, "parse_fhir_resource", parser)
    acquisition = await _run_acquisition(operation_spies)
    assert "untrusted_current_version_census_pagination_link" in acquisition.error
    parser.assert_not_called()
    operation_spies.row_write.assert_not_awaited()
    operation_spies.checkpoint_write.assert_not_awaited()


@pytest.mark.asyncio
async def test_progressed_resume_blocks_candidate_checkpoint_mismatch(
    monkeypatch: pytest.MonkeyPatch,
):
    proof_by_field = current_version_census_initial_proof(
        _contract(),
        RESOURCE_TYPE,
        501,
        expected_page_count=PAGE_COUNT,
    )
    proof_by_field = current_version_census_checkpoint_proof(
        proof_by_field,
        pages_processed=1,
        rows_processed=1,
        page_entry_count=1,
        expected_page_count=PAGE_COUNT,
    )
    operation_spies = _install_acquisition_spies(monkeypatch, ())
    monkeypatch.setattr(
        importer,
        "_load_or_initialize_pagination_checkpoint",
        AsyncMock(
            return_value=importer.PaginationResumeState(
                next_url=_cursor_url(PAGE_COUNT),
                pages_processed=1,
                rows_processed=1,
                recent_url_hashes=(),
                resumed=True,
                completeness=proof_by_field,
            )
        ),
    )
    candidate_count = AsyncMock(return_value=2)
    monkeypatch.setattr(
        importer,
        "_caresource_unique_candidate_count",
        candidate_count,
    )

    acquisition = await _run_acquisition(operation_spies)

    assert acquisition.complete is False
    assert acquisition.next_url_remaining is False
    assert "candidate_checkpoint_mismatch" in acquisition.error
    candidate_count.assert_awaited_once()
    operation_spies.fetch.assert_not_awaited()
    operation_spies.proof_write.assert_not_awaited()
    operation_spies.row_write.assert_not_awaited()
    operation_spies.checkpoint_write.assert_not_awaited()


@pytest.mark.asyncio
async def test_transient_terminal_post_count_writes_no_terminal_rows(
    monkeypatch: pytest.MonkeyPatch,
):
    operation_spies = _install_acquisition_spies(
        monkeypatch,
        (
            (200, _count_bundle(1), None, 1),
            (200, _resource_bundle(("org-1",)), None, 1),
            (503, {}, "transient", 1),
        ),
    )
    monkeypatch.setattr(
        importer,
        "parse_fhir_resource",
        Mock(return_value=_parsed_row("org-1")),
    )

    acquisition = await _run_acquisition(operation_spies)

    assert acquisition.complete is False
    assert acquisition.next_url_remaining is True
    assert "post_census_transient" in acquisition.error
    operation_spies.row_write.assert_not_awaited()
    operation_spies.checkpoint_write.assert_not_awaited()
    assert operation_spies.proof_write.await_count == 2
    terminal_attempt = operation_spies.proof_write.await_args.args[2]
    assert terminal_attempt["processed_rows"] == 1
    assert terminal_attempt["verified"] is False


@pytest.mark.asyncio
async def test_sparse_terminal_deficit_remains_nonpublishing(
    monkeypatch: pytest.MonkeyPatch,
):
    operation_spies = _install_acquisition_spies(
        monkeypatch,
        (
            (200, _count_bundle(501), None, 1),
            (
                200,
                _resource_bundle(("org-1",), next_url=_cursor_url(250)),
                None,
                1,
            ),
            (200, _resource_bundle(()), None, 1),
            (200, _count_bundle(501), None, 1),
        ),
    )
    monkeypatch.setattr(
        importer,
        "parse_fhir_resource",
        Mock(return_value=_parsed_row("org-1")),
    )
    acquisition = await _run_acquisition(operation_spies)
    assert acquisition.complete is False
    assert acquisition.error.endswith(":cursor_loss")
    assert acquisition.fetch_diagnostic["verified"] is False
    assert acquisition.fetch_diagnostic["unreturned_count"] == 500
    assert operation_spies.checkpoint_write.await_count == 1
    assert operation_spies.checkpoint_write.await_args.kwargs["next_url"] == (
        _cursor_url(250)
    )
