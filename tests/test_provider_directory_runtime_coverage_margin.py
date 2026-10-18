# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Provider Directory runtime-helper coverage margin."""

from __future__ import annotations

import importlib
from unittest.mock import AsyncMock, Mock, call

import pytest

from db.connection import ConnectionProxy
from process import provider_directory_generic_source_adapters as source_adapters
from process import provider_directory_generic_source_policy as source_policy
from tests.provider_directory_fhir_coverage_high_support import (
    endpoint_dataset_candidate,
    last_updated_partition_config,
    pagination_checkpoint_context,
)
from tests.test_provider_directory_generic_source_policy import synthetic_checkpoint_factory

provider_directory_fhir = importlib.import_module("process.provider_directory_fhir")


@pytest.mark.asyncio
async def test_partition_initialization_and_empty_fingerprint_proof(
    monkeypatch,
) -> None:
    """Empty retry reads retain their exact dataset, pass and Source transaction."""
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

    retry_rows = AsyncMock(return_value=[])
    monkeypatch.setattr(source_adapters, "source_retry_rows", retry_rows)
    async with (
        synthetic_checkpoint_factory(monkeypatch, provider_directory_fhir.db, schema=provider_directory_fhir._schema()),
        source_policy.source_transaction(provider_directory_fhir.db) as session,
    ):
        database = ConnectionProxy(provider_directory_fhir.db, session, None)
        fingerprints, candidate_hashes = await provider_directory_fhir._load_last_updated_partition_window_proof(
            context,
            "Organization",
            "window-one",
            1,
            database_connection=database,
        )
        assert fingerprints == candidate_hashes == {}
        assert (
            await provider_directory_fhir._load_last_updated_partition_window_fingerprints(
                context, "Organization", "window-one", 1, database_connection=database
            )
            == {}
        )
        assert source_policy.source_copy_session() is source_policy.bound_publisher().connection is session
    expected = call(
        provider_directory_fhir,
        context.dataset_id,
        provider_directory_fhir._last_updated_partition_fingerprint_resource_type("Organization", 1),
        payload_hash=provider_directory_fhir._last_updated_partition_window_proof_hash("Organization", "window-one", 1),
    )
    assert retry_rows.await_args_list == [expected, expected]


def test_endpoint_and_uhc_terminal_metadata_helpers(monkeypatch) -> None:
    candidate = endpoint_dataset_candidate(
        verification_terminal_status="validated",
        verification_terminal_metadata={"proof": "verified"},
    )
    assert provider_directory_fhir._finalized_endpoint_dataset_metadata(candidate) == (
        {"proof": "verified"},
        "validated",
    )
    publication_metadata = provider_directory_fhir._endpoint_dataset_publication_metadata(
        candidate,
        {"Organization": {"complete": True}},
    )
    assert publication_metadata["completion_proof_v1"]["terminal_run_id"] == ("run-new")
    assert (
        provider_directory_fhir._uhc_plan_graph_terminal_count_error(
            "Location",
            2,
            {"location-one": {}},
        )
        == "uhc_plan_graph_location_row_count_mismatch"
    )

    finalized_replay = Mock()
    monkeypatch.setattr(
        provider_directory_fhir,
        "_assert_finalized_endpoint_dataset_replay",
        finalized_replay,
    )
    provider_directory_fhir._assert_published_endpoint_dataset_replay(candidate)
    finalized_replay.assert_called_once_with(candidate)


@pytest.mark.asyncio
async def test_candidate_marker_persists_derived_metadata(monkeypatch) -> None:
    candidate = endpoint_dataset_candidate()
    from process import provider_directory_source_failure_control as failure_control

    status_writer = AsyncMock()
    monkeypatch.setattr(failure_control, "mark_source_candidate", status_writer)

    await provider_directory_fhir._mark_endpoint_dataset_candidate(
        candidate,
        "validated",
        {"Organization": {"complete": True}},
    )
    status_writer.assert_awaited_once()
