# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Named catalog resumes retain their exact lease and completed candidate."""

from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process import registry_fhir_catalog_store as catalog
from process import registry_named_preparation_store as preparations
from process.registry_named_preparation_records import PreparationClaim, SnapshotContractError, digest
from tests.test_registry_named_preparation import IDENTITY, ROLES, document


@asynccontextmanager
async def transaction():
    yield


@pytest.fixture
def resumed_catalog():
    claim = PreparationClaim(IDENTITY, str(UUID(int=2)), 3)
    preparation_by_field = {
        "preparation_id": UUID(IDENTITY),
        "generation_id": UUID(IDENTITY),
        "importer_id": "catalog",
        "admission": {"kind": "catalog"},
        "admission_sha256": "a" * 64,
        "handoff": None,
        "state": "frozen",
        "claim_token": UUID(claim.lease_token),
        "claim_attempt": claim.attempt,
        "claim_expires_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
        "cancellation_requested": False,
    }
    connection = SimpleNamespace(
        is_in_transaction=lambda: True,
        transaction=transaction,
        execute=AsyncMock(),
        fetchrow=AsyncMock(return_value=preparation_by_field),
        fetchval=AsyncMock(return_value=True),
        fetch=AsyncMock(),
    )
    return preparations.NamedPreparationStore(connection, ROLES, kind="catalog"), preparation_by_field, claim


def advancing_queries(connection):
    calls = (*connection.execute.await_args_list, *connection.fetchrow.await_args_list)
    return [call.args[0] for call in calls if call.args[0].startswith(("INSERT", "UPDATE", "DELETE"))]


@pytest.mark.asyncio
@pytest.mark.parametrize("replay", ["exact", "other_token", "other_duration", "expired_token"])
async def test_claim_replay_never_extends_the_original_expiry(resumed_catalog, replay):
    store, preparation_by_field, claim = resumed_catalog
    token, seconds = claim.lease_token, 30
    store.connection.fetchval.side_effect = [True, 30]
    if replay == "other_token":
        token = str(UUID(int=4))
    elif replay == "other_duration":
        seconds = 31
    elif replay == "expired_token":
        store.connection.fetchval.side_effect = [False, True]
    original_by_field = dict(preparation_by_field)
    if replay == "exact":
        assert (
            await store.claim(IDENTITY, preparation_by_field["admission_sha256"], token, lease_seconds=seconds)
            == original_by_field
        )
    else:
        reason_by_replay = {
            "other_token": "claim_busy",
            "other_duration": "claim_conflict",
            "expired_token": "claim_token_reused",
        }
        with pytest.raises(SnapshotContractError, match=reason_by_replay[replay]):
            await store.claim(IDENTITY, preparation_by_field["admission_sha256"], token, lease_seconds=seconds)
    assert preparation_by_field == original_by_field
    assert advancing_queries(store.connection) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["renewed", "expired", "canceled", "expires_before_update"])
async def test_renewal_cannot_revive_an_expired_or_canceled_claim(resumed_catalog, outcome):
    store, preparation_by_field, claim = resumed_catalog
    renewed_by_field = {
        **preparation_by_field,
        "claim_expires_at": preparation_by_field["claim_expires_at"] + timedelta(seconds=30),
    }
    store.connection.fetchrow.side_effect = [
        preparation_by_field,
        None if outcome == "expires_before_update" else renewed_by_field,
    ]
    if outcome == "expired":
        store.connection.fetchval.return_value = False
    elif outcome == "canceled":
        preparation_by_field["cancellation_requested"] = True
    if outcome == "renewed":
        actual = await store.renew(claim, lease_seconds=30)
        assert actual["claim_expires_at"] == renewed_by_field["claim_expires_at"]
        assert actual["claim_attempt"] == claim.attempt
    else:
        reason = (
            "claim_expired_or_changed"
            if outcome == "expires_before_update"
            else ("claim_expired" if outcome == "expired" else "preparation_canceled")
        )
        with pytest.raises(SnapshotContractError, match=reason):
            await store.renew(claim, lease_seconds=30)
    assert len(advancing_queries(store.connection)) == int(outcome in {"renewed", "expires_before_update"})
    assert preparation_by_field["claim_expires_at"] < renewed_by_field["claim_expires_at"]


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", [None, "token", "attempt", "reason"])
async def test_abandonment_replay_requires_the_same_claim_and_reason(resumed_catalog, changed):
    store, preparation_by_field, claim = resumed_catalog
    preparation_by_field.update(state="abandoned", abandonment_sha256="b" * 64)
    reason = preparation_by_field["abandonment_sha256"]
    if changed == "token":
        claim = PreparationClaim(IDENTITY, str(UUID(int=4)), claim.attempt)
    elif changed == "attempt":
        claim = PreparationClaim(IDENTITY, claim.lease_token, claim.attempt + 1)
    elif changed == "reason":
        reason = "c" * 64
    if changed is None:
        assert await store.abandon(claim, reason_sha256=reason) == preparation_by_field
    else:
        with pytest.raises(SnapshotContractError, match="abandonment_conflict"):
            await store.abandon(claim, reason_sha256=reason)
    assert advancing_queries(store.connection) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["admitted", "handed_off", "frozen", "validated"])
async def test_observation_requires_the_completed_freeze_and_reserved_names(resumed_catalog, state):
    store, preparation_by_field, claim = resumed_catalog
    inventory_by_field = {"relations": [{"schema_name": "candidate_rows", "relation_name": "snapshot_data"}]}
    preparation_by_field.update(
        state=state,
        intake_inventory=inventory_by_field,
        frozen_inventory=inventory_by_field,
        frozen_sha256=digest(inventory_by_field),
    )
    store.connection.fetch.side_effect = [inventory_by_field["relations"], []]
    if state in {"frozen", "validated"}:
        assert await store.observe_frozen(claim) == preparation_by_field
        assert store.connection.fetch.await_count == 2
    else:
        with pytest.raises(SnapshotContractError, match="administrative_freeze_required"):
            await store.observe_frozen(claim)
        store.connection.fetch.assert_not_awaited()
    assert advancing_queries(store.connection) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["missing", "frozen", "abandoned", "changed_ready"])
async def test_named_resume_reads_only_the_exact_ready_candidate(monkeypatch, resumed_catalog, state):
    store, preparation_by_field, _claim = resumed_catalog
    handoff = document()
    preparation_by_field.update(state="abandoned" if state == "abandoned" else "frozen", handoff=handoff)
    if state == "missing":
        store.connection.fetchrow.return_value = None
    ready = {**handoff, "snapshot": {"edition": "different"}} if state == "changed_ready" else handoff
    loader = AsyncMock(return_value=ready)
    monkeypatch.setattr(catalog, "load_named_catalog_preparation", loader)
    if state == "changed_ready":
        with pytest.raises(SnapshotContractError, match="named_ready_changed"):
            await store.read_named(IDENTITY)
    else:
        assert await store.read_named(IDENTITY) == (None if state == "missing" else preparation_by_field)
    assert loader.await_count == int(state in {"frozen", "changed_ready"})
    assert advancing_queries(store.connection) == []
