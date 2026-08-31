# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Focused control-catalog projection and fallback contracts."""

from __future__ import annotations

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import provider_directory_control_catalog as control_catalog
from process import provider_directory_profile_selection as selection


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

    assert catalog_map == static_map


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
    )
