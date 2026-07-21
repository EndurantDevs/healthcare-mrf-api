# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from process import provider_directory_profile_selection as selection

from .test_provider_directory_profile_selection_attestation import _computed


def _observation(computed_selection, attestation):
    return {
        "input_identity_digest": selection._input_identity_digest(
            computed_selection.identity_payload
        ),
        "payload_json": attestation.payload,
    }


def _changed_selection(computed_selection):
    changed_identity_map = dict(computed_selection.identity_payload)
    changed_identity_map["source_context_digest"] = "c" * 64
    return selection._ComputedProfileSelection(
        request_projection=computed_selection.request_projection,
        identity_payload=changed_identity_map,
    )


def _mock_authority_registry(monkeypatch):
    next_revision = AsyncMock(side_effect=[1, 2, 3])
    latest_observation = AsyncMock(return_value=None)
    ensure_proof = AsyncMock()
    store_observation = AsyncMock()
    monkeypatch.setattr(selection.db, "scalar", AsyncMock(return_value=None))
    monkeypatch.setattr(selection, "_next_authority_revision", next_revision)
    monkeypatch.setattr(
        selection,
        "_latest_registered_observation",
        latest_observation,
    )
    monkeypatch.setattr(selection, "_ensure_selection_proof", ensure_proof)
    monkeypatch.setattr(selection, "_store_observation", store_observation)
    return next_revision, latest_observation, ensure_proof, store_observation


@pytest.mark.asyncio
async def test_authority_revision_observes_a_b_a_and_replays_latest_identically(
    monkeypatch,
):
    computed_selection = _computed()
    changed_selection = _changed_selection(computed_selection)
    revisions, latest, ensure_proof, store = _mock_authority_registry(
        monkeypatch
    )

    first = await selection._register_selection_proof(computed_selection)
    latest.return_value = _observation(computed_selection, first)
    second = await selection._register_selection_proof(changed_selection)
    latest.return_value = _observation(changed_selection, second)
    third = await selection._register_selection_proof(computed_selection)
    latest.return_value = _observation(computed_selection, third)
    replay = await selection._register_selection_proof(computed_selection)

    assert [first.authority_revision, second.authority_revision] == [1, 2]
    assert third.authority_revision == 3
    assert first.proof_id != second.proof_id
    assert third.proof_id == first.proof_id
    assert replay.payload == third.payload
    assert revisions.await_count == 3
    assert ensure_proof.await_count == 3
    assert store.await_count == 3
