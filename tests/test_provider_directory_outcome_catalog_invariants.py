# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import importlib
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest


importer = importlib.import_module("process.provider_directory_fhir")


def _context() -> importer.PaginationCheckpointContext:
    return importer.PaginationCheckpointContext(
        canonical_api_base="https://example.test/fhir",
        source_scope_hash="scope-1",
        source_ids=("source-1",),
        owner_run_id="run-1",
        acquisition_root_run_id="root-1",
        endpoint_id="endpoint-1",
        dataset_id="dataset-1",
        lineage_verified=True,
    )


def _candidate(**overrides) -> importer.EndpointDatasetCandidate:
    candidate_by_field = {
        "endpoint_id": "endpoint-1",
        "dataset_id": "dataset-1",
        "acquisition_root_run_id": "root-1",
        "source_ids": ("source-1",),
        "selected_resources": ("Organization", "Practitioner"),
        "expected_resources": ("Organization", "Practitioner"),
        "import_run_id": "run-1",
        "previous_dataset_id": "dataset-current",
    }
    candidate_by_field.update(overrides)
    return importer.EndpointDatasetCandidate(**candidate_by_field)


def test_resource_selection_handles_empty_json_object_and_scalar_input():
    assert importer._selected_resources("") == list(importer.DEFAULT_RESOURCES)
    with pytest.raises(ValueError, match="JSON value must be an array"):
        importer._selected_resources('{"resource": "Practitioner"}')
    assert importer._selected_resources("Practitioner,,Location,") == [
        "Practitioner",
        "Location",
    ]
    with pytest.raises(ValueError, match="got int"):
        importer._selected_resources(7)


def test_empty_dataset_repair_rejects_identity_root_and_missing_retry():
    with pytest.raises(RuntimeError, match="identity_collision"):
        importer._should_repair_empty_endpoint_dataset_orphan(
            {"endpoint_id": "other-endpoint"},
            "endpoint-1",
            "root-1",
            None,
            None,
            None,
        )
    with pytest.raises(RuntimeError, match="root_mismatch"):
        importer._should_repair_empty_endpoint_dataset_orphan(
            {
                "endpoint_id": "endpoint-1",
                "acquisition_root_run_id": "other-root",
            },
            "endpoint-1",
            "root-1",
            None,
            None,
            None,
        )
    with pytest.raises(RuntimeError, match="retry_candidate_missing"):
        importer._should_repair_empty_endpoint_dataset_orphan(
            {},
            "endpoint-1",
            "root-1",
            None,
            "run-before",
            _context(),
        )


def _validated_dataset(**overrides):
    dataset_by_field = {
        "endpoint_id": "endpoint-1",
        "acquisition_root_run_id": "root-1",
        "status": importer.ENDPOINT_DATASET_VALIDATED,
        "is_current": False,
        "dataset_hash": "a" * 64,
        "validated_at": object(),
        "publication_metadata_json": {},
    }
    dataset_by_field.update(overrides)
    return dataset_by_field


def _select_validated(dataset):
    return importer._validated_endpoint_dataset_selection(
        dataset,
        "dataset-1",
        "endpoint-1",
        "root-1",
        requires_twin_root_verification=False,
        verification_campaign_id=None,
        verification_source_scope_hash=None,
    )


def test_validated_dataset_selection_rejects_current_metadata_and_proof_gaps():
    with pytest.raises(RuntimeError, match="finalized_state_invalid"):
        _select_validated(_validated_dataset(is_current=True))
    with pytest.raises(RuntimeError, match="validated_metadata_invalid"):
        _select_validated(_validated_dataset(publication_metadata_json=None))
    with pytest.raises(RuntimeError, match="validated_proof_invalid"):
        _select_validated(_validated_dataset(dataset_hash=None))


def _replay_metadata(candidate):
    return {
        "acquisition_root_run_id": candidate.acquisition_root_run_id,
        "selected_resources": list(candidate.selected_resources),
        "expected_resources": list(candidate.expected_resources),
        "source_ids": list(candidate.source_ids),
        "resource_diagnostics": {},
    }


def test_finalized_dataset_replay_rejects_metadata_identity_and_diagnostics():
    with pytest.raises(RuntimeError, match="published_metadata_invalid"):
        importer._assert_finalized_endpoint_dataset_replay(_candidate())

    candidate = _candidate(published_metadata={})
    with pytest.raises(RuntimeError, match="published_identity_mismatch"):
        importer._assert_finalized_endpoint_dataset_replay(candidate)

    candidate = _candidate()
    metadata = _replay_metadata(candidate)
    metadata["resource_diagnostics"] = None
    candidate = _candidate(published_metadata=metadata)
    with pytest.raises(RuntimeError, match="published_diagnostics_invalid"):
        importer._assert_finalized_endpoint_dataset_replay(candidate)


@pytest.mark.asyncio
async def test_unexpected_empty_checkpoint_reset_is_guarded_and_exact(monkeypatch):
    saver = AsyncMock()
    monkeypatch.setattr(importer, "_save_pagination_checkpoint", saver)
    result = SimpleNamespace(error=importer.CIGNA_UNEXPECTED_EMPTY_RESOURCE_ERROR)
    await importer._reset_unexpected_empty_pagination_checkpoint(
        {}, "Practitioner", 100, result
    )
    saver.assert_not_awaited()

    source_by_field = {"_pagination_checkpoint_context": _context()}
    monkeypatch.setattr(
        importer,
        "_partitioned_resource_start_urls",
        lambda *_args, **_kwargs: ["one", "two"],
    )
    await importer._reset_unexpected_empty_pagination_checkpoint(
        source_by_field, "Practitioner", 100, result
    )
    saver.assert_not_awaited()

    monkeypatch.setattr(
        importer,
        "_partitioned_resource_start_urls",
        lambda *_args, **_kwargs: ["https://example.test/fhir/Practitioner"],
    )
    await importer._reset_unexpected_empty_pagination_checkpoint(
        source_by_field, "Practitioner", 100, result
    )
    saver.assert_awaited_once()


def test_bulk_dataset_freshness_requires_complete_and_monotonic_times(monkeypatch):
    monkeypatch.setattr(
        importer,
        "_assert_bulk_transaction_time_window",
        lambda *_args, **_kwargs: None,
    )
    with pytest.raises(RuntimeError, match="transaction_time_missing"):
        importer._assert_endpoint_dataset_bulk_freshness(
            {"Practitioner": {"fetch_mode": "checkpointed_bulk_export"}},
            {},
            {},
        )

    importer._assert_endpoint_dataset_bulk_freshness({}, {}, {})
    importer._assert_endpoint_dataset_bulk_freshness(
        {},
        {"Practitioner": "2026-07-20T10:00:00Z"},
        {"Practitioner": "2026-07-20T09:00:00Z"},
    )
    with pytest.raises(RuntimeError, match="snapshot_older_than_current"):
        importer._assert_endpoint_dataset_bulk_freshness(
            {},
            {"Practitioner": "2026-07-20T08:00:00Z"},
            {"Practitioner": "2026-07-20T09:00:00Z"},
        )


@pytest.mark.asyncio
async def test_dataset_validation_lock_rejects_missing_stale_and_changed_rows():
    candidate = _candidate()

    connection = AsyncMock()
    connection.first.return_value = None
    with pytest.raises(RuntimeError, match="api_endpoint_missing"):
        await importer._lock_endpoint_dataset_for_validation(connection, candidate)

    connection.first.side_effect = [{"endpoint_id": "endpoint-1"}, None]
    with pytest.raises(RuntimeError, match="candidate_missing"):
        await importer._lock_endpoint_dataset_for_validation(connection, candidate)

    connection.first.side_effect = [
        {"endpoint_id": "endpoint-1"},
        {
            "is_current": True,
            "status": importer.ENDPOINT_DATASET_ACQUIRING,
            "acquisition_root_run_id": "root-1",
        },
    ]
    with pytest.raises(RuntimeError, match="candidate_stale"):
        await importer._lock_endpoint_dataset_for_validation(connection, candidate)

    connection.first.side_effect = [
        {"endpoint_id": "endpoint-1"},
        {
            "is_current": False,
            "status": importer.ENDPOINT_DATASET_ACQUIRING,
            "acquisition_root_run_id": "root-1",
        },
        {"dataset_id": "different-current"},
    ]
    with pytest.raises(RuntimeError, match="current_changed"):
        await importer._lock_endpoint_dataset_for_validation(connection, candidate)


def test_linked_resource_urls_cover_reference_endpoint_and_api_fallbacks():
    assert importer._linked_resource_candidate_urls(
        {},
        "Organization",
        "org 1",
        reference="https://reference.test/Organization/org%201?old=1",
    ) == ["https://reference.test/Organization/org%201"]

    assert importer._linked_resource_candidate_urls(
        {"endpoint_organization": "Organization"},
        "Organization",
        "org-1",
    ) == []

    endpoint_urls = importer._linked_resource_candidate_urls(
        {
            "endpoint_organization": (
                "https://example.test/fhir/Organization?_id=existing"
            )
        },
        "Organization",
        "org-1",
    )
    assert endpoint_urls[0].endswith("?_id=existing&_count=1")

    fallback_urls = importer._linked_resource_candidate_urls(
        {
            "api_base": "https://example.test/fhir",
            "endpoint_organization": "https://example.invalid/{id}",
        },
        "Organization",
        "org 1",
    )
    assert "https://example.test/fhir/Organization/org%201" in fallback_urls


def test_address_overlay_components_clean_strings_duplicates_and_invalid_values():
    assert importer._clean_address_overlay_components(
        " practitioner_address,organization_address "
    ) == ("practitioner_address", "organization_address")
    assert importer._clean_address_overlay_components(
        ["", "practitioner_role", "practitioner_role"]
    ) == ("practitioner_role",)
    assert importer._clean_address_overlay_components([]) == (
        importer.ADDRESS_OVERLAY_COMPONENTS
    )
    with pytest.raises(ValueError, match="unknown Provider Directory"):
        importer._clean_address_overlay_components(b"practitioner_role")
    with pytest.raises(ValueError, match="unknown Provider Directory"):
        importer._clean_address_overlay_components(["unknown"])
