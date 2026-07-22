# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Admission persistence and lease-state boundary tests."""

from __future__ import annotations

import asyncio

import pytest

import process.provider_directory_projection_admission as admission_module
from process.provider_directory_projection_admission import (
    _claim_admission_attempt,
    _ensure_admission_mappings,
    _ensure_admission_streams,
    _locked_admission_fields,
    _reclaim_admission,
    _register_admission_transaction,
    _seal_admission,
    _validated_no_input_recipe_attempt,
    _validated_recipe_attempt,
)
from process.provider_directory_projection_types import (
    ProviderDirectoryProjectionBusy,
    ProviderDirectoryProjectionError,
)
from tests.provider_directory_projection_semantic_support import digest
from tests.test_provider_directory_projection_runtime_boundaries import (
    _RuntimeDatabase,
    _registration_fixture,
    _stored_recipe_fields,
)


def test_recipe_attempt_validation_rejects_missing_recipe_or_stored_workset(
    monkeypatch,
):
    recipe, registration = _registration_fixture()
    identity, block_fields, shard_fields, _mappings, _streams = registration
    with pytest.raises(ProviderDirectoryProjectionError, match="recipe_invalid"):
        asyncio.run(
            _validated_recipe_attempt(
                _RuntimeDatabase(first_values=({},)),
                "mrf",
                recipe,
                identity,
                digest("lease"),
                block_fields,
                shard_fields,
            )
        )

    async def mismatched_workset(*_args, **_kwargs):
        return [], []

    monkeypatch.setattr(admission_module, "_stored_workset", mismatched_workset)
    stored_recipe_fields = _stored_recipe_fields(recipe.physical)
    with pytest.raises(ProviderDirectoryProjectionError, match="workset_mismatch"):
        asyncio.run(
            _validated_recipe_attempt(
                _RuntimeDatabase(first_values=(stored_recipe_fields,)),
                "mrf",
                recipe,
                identity,
                digest("lease"),
                block_fields,
                shard_fields,
            )
        )


def test_no_input_recipe_validation_rejects_payload_or_existing_physical_state():
    _recipe, registration = _registration_fixture()
    identity = registration[0]
    no_input_identity = identity.__class__(
        **{
            **identity.__dict__,
            "recipe_id": None,
            "outcome_kind": "no_input",
        }
    )
    with pytest.raises(ProviderDirectoryProjectionError, match="no_input_outcome_invalid"):
        asyncio.run(
            _validated_no_input_recipe_attempt(
                _RuntimeDatabase(),
                "mrf",
                no_input_identity,
                ({"block": "unexpected"},),
                (),
            )
        )
    with pytest.raises(ProviderDirectoryProjectionError, match="recipe_conflict"):
        asyncio.run(
            _validated_no_input_recipe_attempt(
                _RuntimeDatabase(scalar_values=(1,)),
                "mrf",
                no_input_identity,
                (),
                (),
            )
        )


def test_admission_claim_returns_sealed_and_rejects_nonbuilding(monkeypatch):
    """Keep terminal and non-reclaimable states outside lease recovery."""

    _recipe, registration = _registration_fixture()
    identity = registration[0]
    token = digest("new-lease")

    async def locked_admission(*_args, **_kwargs):
        return locked_admission.field_map

    monkeypatch.setattr(admission_module, "_locked_admission_fields", locked_admission)
    locked_admission.field_map = {"status": "sealed"}
    assert (
        asyncio.run(
            _claim_admission_attempt(
                _RuntimeDatabase(), "admission", identity, 1, token, 900
            )
        )
        is None
    )
    locked_admission.field_map = {"status": "failed"}
    with pytest.raises(ProviderDirectoryProjectionError, match="not_reclaimable"):
        asyncio.run(
            _claim_admission_attempt(
                _RuntimeDatabase(), "admission", identity, 1, token, 900
            )
        )


def test_admission_claim_reuses_current_token_and_rejects_busy_lease(monkeypatch):
    """Reuse only the current token and reject another live generation."""

    _recipe, registration = _registration_fixture()
    identity = registration[0]
    token = digest("new-lease")

    async def locked_admission(*_args, **_kwargs):
        return locked_admission.field_map

    monkeypatch.setattr(admission_module, "_locked_admission_fields", locked_admission)
    locked_admission.field_map = {
        "status": "building",
        "attempt": 3,
        "lease_token": token,
    }
    assert (
        asyncio.run(
            _claim_admission_attempt(
                _RuntimeDatabase(), "admission", identity, 1, token, 900
            )
        )
        == 3
    )
    locked_admission.field_map = {
        "status": "building",
        "attempt": 3,
        "lease_token": digest("other-lease"),
        "lease_expires_at": "future",
    }
    with pytest.raises(ProviderDirectoryProjectionBusy, match="admission_busy"):
        asyncio.run(
            _claim_admission_attempt(
                _RuntimeDatabase(scalar_values=(False,)),
                "admission",
                identity,
                1,
                token,
                900,
            )
        )


def test_admission_claim_reclaims_expired_generation(monkeypatch):
    """Fence one expired admission generation to its replacement token."""

    _recipe, registration = _registration_fixture()
    identity = registration[0]
    token = digest("new-lease")

    async def locked_admission(*_args, **_kwargs):
        return {
            "status": "building",
            "attempt": 3,
            "lease_token": digest("other-lease"),
            "lease_expires_at": "expired",
        }

    reclaim_calls = []

    async def record_reclaim(*args, **kwargs):
        reclaim_calls.append((args, kwargs))

    monkeypatch.setattr(admission_module, "_locked_admission_fields", locked_admission)
    monkeypatch.setattr(admission_module, "_reclaim_admission", record_reclaim)
    assert (
        asyncio.run(
            _claim_admission_attempt(
                _RuntimeDatabase(scalar_values=(True,)),
                "admission",
                identity,
                1,
                token,
                900,
            )
        )
        == 3
    )
    assert reclaim_calls


def _accept_admission_persistence_actions(monkeypatch):
    async def accept_action(*_args, **_kwargs):
        return None

    async def accept_insert(*_args, **_kwargs):
        return None

    monkeypatch.setattr(admission_module, "set_local_projection_action", accept_action)
    monkeypatch.setattr(admission_module, "_insert_admission", accept_insert)


def test_locked_admission_rejects_identity_collision(monkeypatch):
    _recipe, registration = _registration_fixture()
    identity = registration[0]
    _accept_admission_persistence_actions(monkeypatch)
    with pytest.raises(ProviderDirectoryProjectionError, match="identity_collision"):
        asyncio.run(
            _locked_admission_fields(
                _RuntimeDatabase(first_values=({},)),
                "admission",
                identity,
                1,
                digest("lease"),
                900,
            )
        )


def test_admission_mapping_replay_requires_exact_rows():
    _recipe, registration = _registration_fixture()
    identity, _blocks, _shards, mappings, _streams = registration
    with pytest.raises(ProviderDirectoryProjectionError, match="mapping_mismatch"):
        asyncio.run(
            _ensure_admission_mappings(
                _RuntimeDatabase(all_values=([{"wrong": "mapping"}],)),
                "mrf",
                identity,
                mappings,
                1,
                1,
                digest("lease"),
            )
        )
    asyncio.run(
        _ensure_admission_mappings(
            _RuntimeDatabase(all_values=(mappings,)),
            "mrf",
            identity,
            mappings,
            1,
            1,
            digest("lease"),
        )
    )


def test_admission_stream_replay_requires_exact_rows():
    _recipe, registration = _registration_fixture()
    identity, _blocks, _shards, _mappings, streams = registration
    with pytest.raises(ProviderDirectoryProjectionError, match="stream_mismatch"):
        asyncio.run(
            _ensure_admission_streams(
                _RuntimeDatabase(all_values=([{"wrong": "stream"}],)),
                "mrf",
                identity,
                streams,
                1,
                1,
                digest("lease"),
            )
        )
    asyncio.run(
        _ensure_admission_streams(
            _RuntimeDatabase(all_values=(streams,)),
            "mrf",
            identity,
            streams,
            1,
            1,
            digest("lease"),
        )
    )


def test_admission_seal_detects_lost_generation(monkeypatch):
    _recipe, registration = _registration_fixture()
    identity = registration[0]
    _accept_admission_persistence_actions(monkeypatch)
    with pytest.raises(ProviderDirectoryProjectionError, match="seal_failed"):
        asyncio.run(
            _seal_admission(
                _RuntimeDatabase(status_values=(0,)),
                "admission",
                identity,
                1,
                1,
                digest("lease"),
            )
        )


def test_admission_reclaim_and_presealed_registration_fail_or_return_exactly(
    monkeypatch,
):
    recipe, registration = _registration_fixture()
    identity = registration[0]

    async def accept_action(*_args, **_kwargs):
        return None

    monkeypatch.setattr(admission_module, "set_local_projection_action", accept_action)
    with pytest.raises(ProviderDirectoryProjectionBusy, match="admission_busy"):
        asyncio.run(
            _reclaim_admission(
                _RuntimeDatabase(status_values=(0,)),
                "admission",
                identity,
                1,
                1,
                digest("lease"),
                900,
            )
        )
    asyncio.run(
        _reclaim_admission(
            _RuntimeDatabase(status_values=(1,)),
            "admission",
            identity,
            1,
            1,
            digest("lease"),
            900,
        )
    )

    async def valid_recipe_attempt(*_args, **_kwargs):
        return 1

    async def already_sealed(*_args, **_kwargs):
        return None

    monkeypatch.setattr(
        admission_module,
        "_validated_recipe_attempt",
        valid_recipe_attempt,
    )
    monkeypatch.setattr(admission_module, "_claim_admission_attempt", already_sealed)
    assert (
        asyncio.run(
            _register_admission_transaction(
                recipe,
                registration,
                900,
                _RuntimeDatabase(),
                "mrf",
            )
        )
        == identity.admission_id
    )
