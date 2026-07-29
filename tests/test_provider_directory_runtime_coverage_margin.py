# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Provider Directory runtime-helper coverage margin."""

from __future__ import annotations

import importlib
from unittest.mock import AsyncMock, Mock

import pytest

from tests.provider_directory_fhir_coverage_high_support import (
    endpoint_dataset_candidate,
    last_updated_partition_config,
    pagination_checkpoint_context,
)


provider_directory_fhir = importlib.import_module(
    "process.provider_directory_fhir"
)


class _EmptyFingerprintDatabase:
    async def all(self, *_args, **_kwargs):
        return []


@pytest.mark.asyncio
async def test_partition_initialization_and_empty_fingerprint_proof(
    monkeypatch,
) -> None:
    context = pagination_checkpoint_context()
    monkeypatch.setattr(
        provider_directory_fhir,
        "_clear_checkpoint_dataset_resource_type",
        AsyncMock(),
    )
    monkeypatch.setattr(
        provider_directory_fhir,
        "_reset_pagination_checkpoint",
        AsyncMock(),
    )
    resume = await provider_directory_fhir._initialize_partition_plan(
        context,
        "Organization",
        last_updated_partition_config(),
        "a" * 64,
    )
    assert resume.plan is not None

    database = _EmptyFingerprintDatabase()
    fingerprints, candidate_hashes = (
        await provider_directory_fhir._load_last_updated_partition_window_proof(
            context,
            "Organization",
            "window-one",
            1,
            database_connection=database,
        )
    )
    assert fingerprints == {}
    assert candidate_hashes == {}
    assert await (
        provider_directory_fhir._load_last_updated_partition_window_fingerprints(
            context,
            "Organization",
            "window-one",
            1,
            database_connection=database,
        )
    ) == {}


def test_endpoint_and_uhc_terminal_metadata_helpers(monkeypatch) -> None:
    candidate = endpoint_dataset_candidate(
        verification_terminal_status="validated",
        verification_terminal_metadata={"proof": "verified"},
    )
    assert provider_directory_fhir._finalized_endpoint_dataset_metadata(
        candidate
    ) == ({"proof": "verified"}, "validated")
    publication_metadata = (
        provider_directory_fhir._endpoint_dataset_publication_metadata(
            candidate,
            {"Organization": {"complete": True}},
        )
    )
    assert publication_metadata["completion_proof_v1"]["terminal_run_id"] == (
        "run-new"
    )
    assert provider_directory_fhir._uhc_plan_graph_terminal_count_error(
        "Location",
        2,
        {"location-one": {}},
    ) == "uhc_plan_graph_location_row_count_mismatch"

    finalized_replay = Mock()
    monkeypatch.setattr(
        provider_directory_fhir,
        "_assert_finalized_endpoint_dataset_replay",
        finalized_replay,
    )
    provider_directory_fhir._assert_published_endpoint_dataset_replay(
        candidate
    )
    finalized_replay.assert_called_once_with(candidate)


@pytest.mark.asyncio
async def test_candidate_marker_persists_derived_metadata(monkeypatch) -> None:
    candidate = endpoint_dataset_candidate()
    status_writer = AsyncMock(return_value="UPDATE 1")
    monkeypatch.setattr(provider_directory_fhir.db, "status", status_writer)

    await provider_directory_fhir._mark_endpoint_dataset_candidate(
        candidate,
        "validated",
        {"Organization": {"complete": True}},
    )
    status_writer.assert_awaited_once()
