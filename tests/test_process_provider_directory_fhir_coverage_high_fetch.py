# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from unittest.mock import AsyncMock, Mock

import pytest

from db.models import ProviderDirectoryLocation, ProviderDirectoryPractitioner
from tests.provider_directory_fhir_coverage_high_support import (
    importer,
    pagination_checkpoint_context,
    resource_fetch_result,
)


def _patch_paged_fetch_defaults(monkeypatch, start_urls):
    monkeypatch.setattr(importer, "_source_supported_resource_types", Mock(return_value=None))
    monkeypatch.setattr(importer, "_is_aetna_medicaid_targeted_source", Mock(return_value=False))
    monkeypatch.setattr(importer, "_is_source_bulk_export_effective", Mock(return_value=False))
    monkeypatch.setattr(importer, "_needs_practitioner_role_reverse_lookup", Mock(return_value=False))
    monkeypatch.setattr(importer, "_source_full_refresh_page_count", Mock(return_value=10))
    monkeypatch.setattr(importer, "_partitioned_resource_start_urls", Mock(return_value=start_urls))
    monkeypatch.setattr(importer, "_source_resource_timeout", Mock(return_value=3))
    monkeypatch.setattr(importer, "_uhc_adaptive_partition_child_urls", Mock(return_value=[]))
    monkeypatch.setattr(importer, "_is_uhc_partition_cap_exhausted", Mock(return_value=False))
    monkeypatch.setattr(importer, "_synthetic_position_page_identity", Mock(return_value=None))
    monkeypatch.setattr(importer, "_resolved_fhir_next_url", Mock(return_value=None))
    monkeypatch.setattr(importer, "_uhc_partition_residual_error", Mock(return_value=None))


async def _fetch_practitioner_rows(**overrides):
    field_map = {
        "per_resource_limit": 0,
        "page_limit": 0,
        "page_count": 10,
        "timeout": 3,
        "run_id": "root-run",
        "retain_rows": True,
    }
    field_map.update(overrides)
    return await importer._fetch_resource_rows(
        {
            "source_id": "source-1",
            "api_base": "https://example.test/fhir",
            "canonical_api_base": "https://example.test/fhir",
            "metadata_json": {},
        },
        "Practitioner",
        **field_map,
    )


@pytest.mark.asyncio
async def test_fetch_resource_rows_passes_checkpoint_to_bulk_export(monkeypatch):
    expected_outcome = resource_fetch_result(error=None)
    monkeypatch.setattr(importer, "_source_supported_resource_types", Mock(return_value=None))
    monkeypatch.setattr(importer, "_is_aetna_medicaid_targeted_source", Mock(return_value=False))
    monkeypatch.setattr(importer, "_is_source_bulk_export_effective", Mock(return_value=True))
    bulk_fetch = AsyncMock(return_value=expected_outcome)
    monkeypatch.setattr(importer, "_fetch_bulk_export_resource_rows", bulk_fetch)
    checkpoint_context = pagination_checkpoint_context()

    fetch_outcome = await _fetch_practitioner_rows(
        row_batch_handler=AsyncMock(return_value=0),
        pagination_checkpoint=checkpoint_context,
        retain_rows=False,
    )

    assert fetch_outcome is expected_outcome
    bulk_source = bulk_fetch.await_args.args[0]
    assert bulk_source["_pagination_checkpoint_context"] is checkpoint_context


@pytest.mark.asyncio
async def test_fetch_resource_rows_tracks_partition_non_bundle_failures(monkeypatch):
    _patch_paged_fetch_defaults(
        monkeypatch,
        ["https://example.test/one", "https://example.test/two"],
    )
    monkeypatch.setattr(
        importer,
        "_fetch_source_json",
        AsyncMock(
            return_value=(
                200,
                {"resourceType": "OperationOutcome"},
                None,
                0.01,
            )
        ),
    )

    fetch_outcome = await _fetch_practitioner_rows(per_resource_limit=100)

    assert fetch_outcome.complete is False
    assert fetch_outcome.error == "partition_errors_2_last_non_bundle_payload"
    assert fetch_outcome.pages_fetched == 0


@pytest.mark.asyncio
async def test_fetch_resource_rows_expands_children_and_skips_unusable_entries(monkeypatch):
    root_url = "https://example.test/root"
    child_url = "https://example.test/child"
    _patch_paged_fetch_defaults(monkeypatch, [root_url])
    monkeypatch.setattr(
        importer,
        "_uhc_adaptive_partition_child_urls",
        Mock(side_effect=lambda _source, _type, url, _body: [child_url] if url == root_url else []),
    )
    child_bundle_map = {
        "resourceType": "Bundle",
        "type": "searchset",
        "entry": [
            {"resource": {"resourceType": "Practitioner", "id": "skip"}},
            {"resource": {"resourceType": "Location", "id": "wrong-model"}},
        ],
    }
    monkeypatch.setattr(
        importer,
        "_fetch_source_json",
        AsyncMock(
            side_effect=[
                (200, {"resourceType": "Bundle", "type": "searchset"}, None, 0.01),
                (200, child_bundle_map, None, 0.01),
            ]
        ),
    )
    monkeypatch.setattr(
        importer,
        "parse_fhir_resource",
        Mock(side_effect=[None, (ProviderDirectoryLocation, {"resource_id": "wrong-model"})]),
    )

    fetch_outcome = await _fetch_practitioner_rows()

    assert fetch_outcome.complete is True
    assert fetch_outcome.rows_fetched == 0


@pytest.mark.asyncio
async def test_fetch_resource_rows_marks_limit_with_remaining_next_page(monkeypatch):
    _patch_paged_fetch_defaults(monkeypatch, ["https://example.test/root"])
    monkeypatch.setattr(
        importer,
        "_fetch_source_json",
        AsyncMock(
            return_value=(
                200,
                {
                    "resourceType": "Bundle",
                    "type": "searchset",
                    "entry": [
                        {
                            "resource": {
                                "resourceType": "Practitioner",
                                "id": "practitioner-1",
                            }
                        }
                    ],
                },
                None,
                0.01,
            )
        ),
    )
    monkeypatch.setattr(
        importer,
        "parse_fhir_resource",
        Mock(
            return_value=(
                ProviderDirectoryPractitioner,
                {"resource_id": "practitioner-1"},
            )
        ),
    )
    monkeypatch.setattr(
        importer,
        "_resolved_fhir_next_url",
        Mock(return_value="https://example.test/next"),
    )
    cancel_probe = AsyncMock()
    monkeypatch.setattr(importer, "_raise_if_resource_import_cancelled", cancel_probe)

    fetch_outcome = await _fetch_practitioner_rows(
        per_resource_limit=1,
        cancel_ctx={"context": {}},
    )

    assert fetch_outcome.row_limit_reached is True
    assert fetch_outcome.next_url_remaining is True
    cancel_probe.assert_awaited_once()


@pytest.mark.asyncio
async def test_fetch_resource_rows_surfaces_caresource_proof_retry(monkeypatch):
    _patch_paged_fetch_defaults(monkeypatch, ["https://example.test/root"])
    checkpoint_context = pagination_checkpoint_context()
    resume_state = importer.PaginationResumeState(
        next_url="https://example.test/root",
        pages_processed=0,
        rows_processed=0,
        recent_url_hashes=(),
    )
    monkeypatch.setattr(
        importer,
        "_load_or_initialize_pagination_checkpoint",
        AsyncMock(return_value=resume_state),
    )
    monkeypatch.setattr(importer, "_is_caresource_opaque_cursor_census", Mock(return_value=True))
    monkeypatch.setattr(
        importer,
        "_prepare_caresource_pre_census",
        AsyncMock(return_value={"pre_count": 0}),
    )
    monkeypatch.setattr(
        importer,
        "_fetch_source_json",
        AsyncMock(
            return_value=(
                200,
                {"resourceType": "Bundle", "type": "searchset"},
                None,
                0.01,
            )
        ),
    )
    monkeypatch.setattr(
        importer,
        "_finish_caresource_opaque_cursor_census",
        AsyncMock(
            return_value=importer.CareSourceOpaqueCursorOutcome(
                proof={"pre_count": 0, "verified": False},
                error="post_census_busy",
                retryable=True,
                retry_not_before="later",
            )
        ),
    )

    fetch_outcome = await _fetch_practitioner_rows(
        row_batch_handler=AsyncMock(return_value=0),
        pagination_checkpoint=checkpoint_context,
        retain_rows=False,
    )

    assert fetch_outcome.error == (
        f"{importer.CARESOURCE_OPAQUE_CURSOR_RETRYABLE_ERROR}:post_census_busy"
    )
    assert fetch_outcome.next_url_remaining is True
    assert fetch_outcome.retry_not_before == "later"
