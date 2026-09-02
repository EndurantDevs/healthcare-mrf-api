# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Focused control-catalog projection and fallback contracts."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import provider_directory_control_catalog as control_catalog
from process import provider_directory_profile_selection as selection
from process import provider_directory_profile_selection_snapshot as snapshot
from tests.provider_directory_profile_uhc_flex_test_support import (
    _catalog as _uhc_catalog,
    _dataset_rows as _uhc_dataset_rows,
    _source_rows as _uhc_source_rows,
)


def _profile_selection_request() -> dict[str, object]:
    return {
        "contract_id": (
            "healthporta.provider-directory-profile-selection-"
            "attestation-request.v1"
        ),
        "node_id": "dev-node",
        "catalog_digest": "a" * 64,
        "selection_fingerprint": "b" * 64,
        "datasets": [
            {"source_id": "dataset-scoped", "dataset_id": "dataset-current"}
        ],
    }


@pytest.mark.asyncio
async def test_control_catalog_includes_exact_selection_projection(monkeypatch):
    static_map = {"catalog_digest": "a" * 64, "items": []}
    enriched_map = {"catalog_digest": "a" * 64, "items": [{}]}
    selection_payload = _profile_selection_request()
    monkeypatch.setattr(
        control_catalog,
        "provider_directory_source_catalog",
        lambda: static_map,
    )
    monkeypatch.setattr(
        control_catalog,
        "current_profile_selection_request",
        AsyncMock(return_value=selection_payload),
    )
    monkeypatch.setattr(
        control_catalog,
        "enrich_provider_directory_source_catalog",
        AsyncMock(return_value=enriched_map),
    )

    assert await control_catalog.provider_directory_control_catalog() == {
        **enriched_map,
        "profile_selection_request": selection_payload,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("failed_operation", ("selection", "outcomes"))
async def test_control_catalog_falls_back_on_optional_database_failures(
    monkeypatch,
    failed_operation,
):
    static_map = {"catalog_digest": "a" * 64, "items": []}
    enriched_map = {"catalog_digest": "a" * 64, "items": [{}]}
    selection_payload = _profile_selection_request()
    monkeypatch.setattr(
        control_catalog,
        "provider_directory_source_catalog",
        lambda: static_map,
    )
    monkeypatch.setattr(
        control_catalog,
        "current_profile_selection_request",
        AsyncMock(
            side_effect=RuntimeError("unavailable")
            if failed_operation == "selection"
            else None,
            return_value=selection_payload,
        ),
    )
    monkeypatch.setattr(
        control_catalog,
        "enrich_provider_directory_source_catalog",
        AsyncMock(
            side_effect=RuntimeError("unavailable")
            if failed_operation == "outcomes"
            else None,
            return_value=enriched_map,
        ),
    )

    catalog_map = await control_catalog.provider_directory_control_catalog()

    assert catalog_map == (
        enriched_map
        if failed_operation == "selection"
        else {
            **static_map,
            "profile_selection_request": selection_payload,
        }
    )


@pytest.mark.asyncio
async def test_control_catalog_bounds_optional_outcome_enrichment(monkeypatch):
    static_map = {"catalog_digest": "a" * 64, "items": []}
    selection_payload = _profile_selection_request()
    blocked = asyncio.Event()

    async def blocked_enrichment(_static_map):
        await blocked.wait()

    monkeypatch.setattr(
        control_catalog,
        "provider_directory_source_catalog",
        lambda: static_map,
    )
    monkeypatch.setattr(
        control_catalog,
        "current_profile_selection_request",
        AsyncMock(return_value=selection_payload),
    )
    monkeypatch.setattr(
        control_catalog,
        "enrich_provider_directory_source_catalog",
        blocked_enrichment,
    )
    monkeypatch.setattr(
        control_catalog,
        "_OUTCOME_ENRICHMENT_TIMEOUT_SECONDS",
        0.01,
    )

    catalog_map = await asyncio.wait_for(
        control_catalog.provider_directory_control_catalog(),
        timeout=1,
    )

    assert catalog_map == {
        **static_map,
        "profile_selection_request": selection_payload,
    }


@pytest.mark.asyncio
async def test_control_catalog_bounds_optional_selection_projection(monkeypatch):
    static_map = {"catalog_digest": "a" * 64, "items": []}
    enriched_map = {"catalog_digest": "a" * 64, "items": [{}]}
    selection_started = asyncio.Event()
    transaction_exited = asyncio.Event()
    never = asyncio.Event()

    @asynccontextmanager
    async def tracked_transaction() -> AsyncIterator[None]:
        try:
            yield
        finally:
            transaction_exited.set()

    async def blocked_selection(
        _static_map: object,
        *,
        node_id: str,
        lock_selection: bool,
        exact_readiness: bool,
    ) -> None:
        selection_started.set()
        await never.wait()

    monkeypatch.setattr(
        control_catalog,
        "provider_directory_source_catalog",
        lambda: static_map,
    )
    monkeypatch.setattr(selection.db, "transaction", tracked_transaction)
    monkeypatch.setattr(selection.db, "status", AsyncMock())
    monkeypatch.setattr(selection, "configured_node_id", lambda: "dev-node")
    monkeypatch.setattr(selection, "_compute_current_selection", blocked_selection)
    monkeypatch.setattr(
        control_catalog,
        "enrich_provider_directory_source_catalog",
        AsyncMock(return_value=enriched_map),
    )
    monkeypatch.setattr(
        control_catalog,
        "_SELECTION_PROJECTION_TIMEOUT_SECONDS",
        0.01,
    )

    catalog_map = await asyncio.wait_for(
        control_catalog.provider_directory_control_catalog(),
        timeout=1,
    )

    assert catalog_map == enriched_map
    assert selection_started.is_set()
    assert transaction_exited.is_set()


@asynccontextmanager
async def _transaction():
    yield


@pytest.mark.asyncio
async def test_current_selection_request_uses_repeatable_unlocked_snapshot(
    monkeypatch,
):
    selection_payload = _profile_selection_request()
    computed_selection = SimpleNamespace(
        request_projection=tuple(selection_payload["datasets"]),
        identity_payload={
            "catalog_digest": selection_payload["catalog_digest"],
            "selection_fingerprint": selection_payload["selection_fingerprint"],
        },
    )
    transaction_status = AsyncMock()
    monkeypatch.setattr(selection.db, "transaction", _transaction)
    monkeypatch.setattr(selection.db, "status", transaction_status)
    monkeypatch.setattr(selection, "configured_node_id", lambda: "dev-node")
    selection_reader = AsyncMock(return_value=computed_selection)
    monkeypatch.setattr(selection, "_compute_current_selection", selection_reader)

    catalog_map = {"catalog_digest": "a" * 64, "items": []}
    assert await selection.current_profile_selection_request(
        catalog_map
    ) == selection_payload
    transaction_status.assert_awaited_once_with(
        "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;"
    )
    selection_reader.assert_awaited_once_with(
        catalog_map,
        node_id="dev-node",
        lock_selection=False,
        exact_readiness=False,
    )


@pytest.mark.asyncio
async def test_attestation_rejects_stale_header_proposal(monkeypatch):
    proposed = snapshot._computed_selection_from_rows(
        _uhc_catalog(),
        node_id="dev-node",
        source_rows=_uhc_source_rows(),
        dataset_rows=_uhc_dataset_rows(ready=True),
    )
    exact = snapshot._computed_selection_from_rows(
        _uhc_catalog(),
        node_id="dev-node",
        source_rows=_uhc_source_rows(),
        dataset_rows=_uhc_dataset_rows(ready=False),
    )
    register = AsyncMock()
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "dev-node")
    monkeypatch.setattr(selection.db, "transaction", _transaction)
    monkeypatch.setattr(selection.db, "status", AsyncMock())
    monkeypatch.setattr(
        selection,
        "_compute_current_selection",
        AsyncMock(return_value=exact),
    )
    monkeypatch.setattr(selection, "_register_selection_proof", register)

    with pytest.raises(selection.ProviderDirectoryProfileSelectionDrift):
        await selection.attest_profile_selection(
            selection._expected_request(proposed, "dev-node"),
            _uhc_catalog(),
        )

    register.assert_not_awaited()


@pytest.mark.asyncio
async def test_attestation_replay_rejects_corrupt_proof(monkeypatch):
    computed = snapshot._computed_selection_from_rows(
        _uhc_catalog(),
        node_id="dev-node",
        source_rows=_uhc_source_rows(),
        dataset_rows=_uhc_dataset_rows(ready=True),
    )
    identity_digest = selection._input_identity_digest(computed.identity_payload)
    latest_map = {
        "input_identity_digest": identity_digest,
        "payload_json": selection._attestation_payload(
            computed.identity_payload,
            1,
        ),
    }
    monkeypatch.setattr(selection.db, "scalar", AsyncMock())
    monkeypatch.setattr(
        selection,
        "_ensure_selection_proof",
        AsyncMock(side_effect=RuntimeError("registry_corrupt")),
    )
    monkeypatch.setattr(
        selection,
        "_latest_registered_observation",
        AsyncMock(return_value=latest_map),
    )

    with pytest.raises(RuntimeError, match="registry_corrupt"):
        await selection._register_selection_proof(computed)
