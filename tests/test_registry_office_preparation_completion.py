# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Office recovery distinguishes historical captures from unfinished new preparations."""

import json
import time
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process import registry_ptg_office_preparation as preparation
from tests.test_registry_ptg_office_preparation import _persisted_named_office


@pytest.mark.asyncio
@pytest.mark.parametrize("metadata_exists", [False, True])
async def test_historical_office_capture_remains_readable_without_a_new_preparation(monkeypatch, metadata_exists):
    handoff_by_field = {"storage_schema": "retained_office", "candidate_id": str(UUID(int=1))}
    monkeypatch.setattr(preparation, "load_named_office_preparation", AsyncMock(return_value=handoff_by_field))
    driver = SimpleNamespace(
        fetchval=AsyncMock(side_effect=[1 if metadata_exists else None, json.dumps({"contract": "historical"})]),
        fetchrow=AsyncMock(return_value=None),
    )
    assert await preparation.require_office_preparation_frozen(
        driver, handoff_by_field["candidate_id"], control_schema="registry"
    ) == (handoff_by_field, None)
    assert driver.fetchrow.await_count == int(metadata_exists)
    driver.fetchval.side_effect = [1 if metadata_exists else None, {"storage": {"schema": "new_office"}}]
    with pytest.raises(ValueError, match="freeze_required"):
        await preparation.require_office_preparation_frozen(
            driver, handoff_by_field["candidate_id"], control_schema="registry"
        )


@pytest.mark.asyncio
async def test_office_freeze_requires_the_same_handoff_and_a_completed_phase(monkeypatch):
    driver, request, _ = _persisted_named_office()
    handoff = await preparation.load_named_office_preparation(
        driver, str(request.capture_id), control_schema="registry"
    )
    monkeypatch.setattr(preparation, "load_named_office_preparation", AsyncMock(return_value=handoff))
    preparation_by_field = {"state": "handed_off", "handoff": json.dumps(handoff)}
    metadata = SimpleNamespace(
        fetchval=AsyncMock(return_value=1), fetchrow=AsyncMock(return_value=preparation_by_field)
    )
    for state in ("handed_off", "abandoned"):
        preparation_by_field["state"] = state
        with pytest.raises(ValueError, match="freeze_required"):
            await preparation.require_office_preparation_frozen(
                metadata, str(request.capture_id), control_schema="registry"
            )
    for state in ("frozen", "validated", "published"):
        preparation_by_field["state"] = state
        assert await preparation.require_office_preparation_frozen(
            metadata, str(request.capture_id), control_schema="registry"
        ) == (handoff, state)
    preparation_by_field["handoff"] = {**handoff, "storage_schema": "another_capture"}
    with pytest.raises(ValueError, match="manifest_changed"):
        await preparation.require_office_preparation_frozen(
            metadata, str(request.capture_id), control_schema="registry"
        )


@pytest.mark.asyncio
async def test_published_office_replay_refuses_changed_approval_without_republishing(monkeypatch):
    descriptor = SimpleNamespace(capture_id=UUID(int=1), manifest_sha256="a" * 64)
    context, driver, witness = SimpleNamespace(control_schema="registry"), object(), object()
    store = SimpleNamespace(validate_named=AsyncMock(), publish_named=AsyncMock())
    report_by_field = {
        "manifest_sha256": descriptor.manifest_sha256,
        "command_sha256": "b" * 64,
        "witness_sha256": "c" * 64,
    }
    preparation_by_field = {"state": "frozen", "publication_fence": UUID(int=2)}
    monkeypatch.setattr(preparation, "office_validation_report", lambda *_args: report_by_field)
    monkeypatch.setattr(preparation, "require_office_preparation_frozen", AsyncMock(return_value=({}, "frozen")))
    monkeypatch.setattr(
        preparation, "admit_office_preparation", AsyncMock(return_value=(store, preparation_by_field, "claim"))
    )
    await preparation.publish_office_preparation(driver, context, descriptor, witness, "d" * 64, "e" * 64)
    preparation_by_field.update(
        state="published",
        validation={"evidence": store.validate_named.await_args.args[1]},
        publication=store.publish_named.await_args.args[1],
    )
    await preparation.publish_office_preparation(driver, context, descriptor, witness, "d" * 64, "e" * 64)
    with pytest.raises(ValueError, match="publication_changed"):
        await preparation.publish_office_preparation(driver, context, descriptor, witness, "f" * 64, "e" * 64)
    assert store.publish_named.await_count == 1 and store.validate_named.await_count == 1
    preparation_by_field["state"] = "frozen"
    monkeypatch.setattr(
        preparation, "admit_office_preparation", AsyncMock(return_value=(store, preparation_by_field, None))
    )
    with pytest.raises(ValueError, match="freeze_required"):
        await preparation.publish_office_preparation(driver, context, descriptor, witness, "d" * 64, "e" * 64)


@pytest.mark.asyncio
async def test_retained_office_validation_is_authorized_and_replays_only_identical_evidence(monkeypatch):
    session = SimpleNamespace(execute=AsyncMock())
    service = SimpleNamespace(
        scope_service=SimpleNamespace(store=SimpleNamespace(control_schema="registry")),
        publisher_sessions=object(),
    )

    @asynccontextmanager
    async def transaction(factory, *, control_schema):
        assert factory is service.publisher_sessions and control_schema == "registry"
        yield session

    descriptor = SimpleNamespace(capture_id=UUID(int=1))
    envelope_by_field = {"selector": {}}
    report_by_field = {"manifest_sha256": "a" * 64, "command_sha256": "b" * 64, "witness_sha256": "c" * 64}
    store = SimpleNamespace(validate_named=AsyncMock(), yield_named_claim=AsyncMock())
    preparation_by_field = {"state": "frozen"}
    authorize = AsyncMock()
    monkeypatch.setattr(preparation, "registry_company_approval_transaction", transaction)
    monkeypatch.setattr(preparation, "_source_context", AsyncMock(return_value=("source-context", "command")))
    monkeypatch.setattr(preparation, "_authorize", authorize)
    monkeypatch.setattr(preparation.capture, "_driver", AsyncMock(return_value="driver"))
    monkeypatch.setattr(preparation, "require_office_preparation_frozen", AsyncMock(return_value=({}, "frozen")))
    monkeypatch.setattr(
        preparation, "admit_office_preparation", AsyncMock(return_value=(store, preparation_by_field, "claim"))
    )
    monkeypatch.setattr(preparation, "office_validation_report", lambda *_args: report_by_field)
    deadline = time.monotonic() + 30
    await preparation._retain_validation(service, envelope_by_field, descriptor, object(), deadline)
    store.yield_named_claim.assert_awaited_once_with("claim")
    preparation_by_field.update(state="published", validation={"evidence": store.validate_named.await_args.args[1]})
    await preparation._retain_validation(service, envelope_by_field, descriptor, object(), deadline)
    preparation_by_field["validation"] = {"evidence": {"different": True}}
    with pytest.raises(ValueError, match="validation_changed"):
        await preparation._retain_validation(service, envelope_by_field, descriptor, object(), deadline)
    preparation_by_field["state"] = "frozen"
    monkeypatch.setattr(
        preparation, "admit_office_preparation", AsyncMock(return_value=(store, preparation_by_field, None))
    )
    with pytest.raises(ValueError, match="freeze_required"):
        await preparation._retain_validation(service, envelope_by_field, descriptor, object(), deadline)
    monkeypatch.setattr(preparation, "require_office_preparation_frozen", AsyncMock(return_value=({}, None)))
    await preparation._retain_validation(service, envelope_by_field, descriptor, object(), deadline)
    assert store.validate_named.await_count == 1 and store.yield_named_claim.await_count == 1
    assert authorize.await_count == 5
