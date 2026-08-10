# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Serialized admission boundary for runtime-bound capacity leases."""

from __future__ import annotations

import importlib
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from tests.test_provider_directory_profile_selection_attestation import (
    _execution,
)


importer = importlib.import_module("process.provider_directory_fhir")
runtime = importlib.import_module(
    "process.provider_directory_profile_runtime_observation"
)


def _tracked_transaction(transaction_state_by_field):
    @asynccontextmanager
    async def transaction():
        transaction_state_by_field["active"] = True
        try:
            yield
        finally:
            transaction_state_by_field["active"] = False

    return transaction


def _patch_admission_dependencies(
    monkeypatch,
    transaction,
    serving_state,
    consume,
) -> None:
    monkeypatch.setattr(importer.db, "transaction", transaction)
    monkeypatch.setattr(importer.db, "status", AsyncMock())
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_selection_catalog",
        Mock(return_value=object()),
    )
    for dependency_name in (
        "_lock_provider_directory_profile_capacity_control_run",
        "assert_profile_selection_current_in_transaction",
        "_assert_admission_run_toast",
    ):
        monkeypatch.setattr(importer, dependency_name, AsyncMock())
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_serving_state",
        AsyncMock(return_value=serving_state),
    )
    monkeypatch.setattr(
        importer,
        "_assert_provider_directory_profile_capacity_serving_state",
        Mock(),
    )
    monkeypatch.setattr(
        importer,
        "_admission_database_guard",
        AsyncMock(return_value=object()),
    )
    monkeypatch.setattr(
        importer,
        "observe_profile_runtime",
        AsyncMock(return_value={"contract_id": "runtime"}),
    )
    monkeypatch.setattr(
        importer,
        "assert_runtime_observation_matches_geometry",
        Mock(),
    )
    monkeypatch.setattr(importer, "_consume_admission_values", consume)


@pytest.mark.asyncio
async def test_cross_runtime_lease_fails_inside_transaction_before_consume(
    monkeypatch,
):
    """Keep the runtime replay fence before the immutable consumption row."""

    transaction_state_by_field = {"active": False}
    serving_state = object()
    consume = AsyncMock()
    runtime_failure = runtime.ProviderDirectoryProfileRuntimeObservationError(
        "provider_directory_profile_runtime_observation_"
        "capacity_lease_runtime_mismatch"
    )

    def reject_runtime(_lease, _observation):
        assert transaction_state_by_field["active"] is True
        raise runtime_failure

    async def lock_preflight_state(_schema):
        assert transaction_state_by_field["active"] is True

    _patch_admission_dependencies(
        monkeypatch,
        _tracked_transaction(transaction_state_by_field),
        serving_state,
        consume,
    )
    preflight_state_lock = AsyncMock(side_effect=lock_preflight_state)
    monkeypatch.setattr(
        importer,
        "_lock_profile_capacity_preflight_state",
        preflight_state_lock,
    )
    monkeypatch.setattr(
        importer,
        "assert_capacity_lease_matches_runtime_observation",
        reject_runtime,
    )

    with pytest.raises(
        runtime.ProviderDirectoryProfileRuntimeObservationError,
        match="capacity_lease_runtime_mismatch",
    ):
        await importer._consume_admission_transaction(
            "run_" + "1" * 32,
            _execution(),
            SimpleNamespace(serving_state=serving_state),
            object(),
            object(),
            SimpleNamespace(
                database_identity=object(),
                control_wal_plan_input=object(),
            ),
            object(),
        )

    preflight_state_lock.assert_awaited_once_with(importer._schema())
    consume.assert_not_awaited()
