# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Locked evidence verifies the inverse without granting or spending rollback capacity."""

import asyncio
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_common_serving_recovery_observation as observation
from tests.test_provider_directory_common_serving_recovery_document import actor
from tests.test_provider_directory_common_serving_recovery_document import common as common


@pytest.fixture
def locked(common, monkeypatch):
    document = deepcopy(common.document)
    events_by_phase = {"published": {"document": document, "sha256": "a" * 64}}
    session, driver = object(), AsyncMock()
    current_actor = actor()
    database = SimpleNamespace()
    fhir = SimpleNamespace(db=database, _schema=lambda: "unit")
    ordinary = AsyncMock(return_value=(session, driver, current_actor))
    load = AsyncMock(return_value=events_by_phase)
    capture = AsyncMock(return_value=deepcopy(document["after"]))
    retained = AsyncMock(return_value=())
    posthash = AsyncMock(return_value=deepcopy(document["preparation"]["expected_postimages"]))
    freeze = AsyncMock()
    monkeypatch.setattr(observation.receipts, "ordinary", ordinary)
    monkeypatch.setattr(observation.recovery, "_load_history", load)
    monkeypatch.setattr(observation.journal, "capture_state", capture)
    monkeypatch.setattr(observation.inverse, "assert_retained_rows", retained)
    monkeypatch.setattr(observation.postimages, "capture_postimages", posthash)
    monkeypatch.setattr(observation.receipts, "_require_freeze", freeze)
    return SimpleNamespace(
        fhir=fhir,
        session=session,
        driver=driver,
        actor=current_actor,
        events=events_by_phase,
        ordinary=ordinary,
        load=load,
        capture=capture,
        retained=retained,
        posthash=posthash,
        freeze=freeze,
    )


async def observe(locked):
    return await observation.observe_locked_inverse(
        locked.fhir, locked.events["published"]["document"]["generation"], source_id="source-a"
    )


@pytest.mark.asyncio
async def test_exact_locked_observation_yields_all_existing_restore_plans_without_writes(locked):
    result = await observe(locked)
    assert result.session is locked.session and result.task is asyncio.current_task()
    assert len(result.row_restores) == 10 and not result.initial_restores and not result.initial_proofs
    assert result.document == locked.events["published"]["document"]
    assert await observation.recheck_locked_inverse(result) is result
    locked.posthash.assert_awaited_once()
    locked.retained.assert_awaited_once()
    assert locked.freeze.await_count == 33
    locked.driver.execute.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["metadata", "hash", "recovered", "source"])
async def test_forward_drift_or_stale_scope_refuses_before_mutation(locked, damage):
    if damage == "metadata":
        locked.capture.return_value["metadata"]["profile"]["generation_id"] = "changed"
    elif damage == "hash":
        locked.posthash.return_value["facility_candidate"]["data_sha256"] = "0" * 64
    elif damage == "recovered":
        locked.events["recovered"] = locked.events["published"]
    else:
        locked.events["published"]["document"]["source"]["source_id"] = "other"
    with pytest.raises(ValueError):
        await observe(locked)
    locked.driver.execute.assert_not_called()


@pytest.mark.asyncio
async def test_same_transaction_recheck_refuses_new_actor_or_changed_event(locked):
    result = await observe(locked)
    locked.ordinary.return_value = (locked.session, locked.driver, {**locked.actor, "xid": "new"})
    with pytest.raises(ValueError, match="observation_changed"):
        await observation.recheck_locked_inverse(result)
    locked.ordinary.return_value = (locked.session, locked.driver, locked.actor)
    locked.events["published"]["sha256"] = "b" * 64
    with pytest.raises(ValueError, match="observation_changed"):
        await observation.recheck_locked_inverse(result)


@pytest.mark.asyncio
async def test_a_sibling_task_cannot_reuse_the_original_locked_observation(locked):
    result = await observe(locked)
    with pytest.raises(ValueError, match="observation_changed"):
        await asyncio.create_task(observation.recheck_locked_inverse(result))


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", [None, "targets", "reference", "history"])
async def test_initial_witness_uses_exact_original_receipt_and_signed_targets(locked, monkeypatch, damage):
    import sys
    from contextlib import asynccontextmanager

    import process
    from tests.test_provider_directory_common_serving_inverse import initial_baseline_and_payload

    document = deepcopy(locked.events["published"]["document"])
    document["before"], document["preparation"]["retention"] = initial_baseline_and_payload()
    reference_by_field = dict(
        build_id="pdpb_" + "1" * 32,
        generation_id=document["preparation"]["profile_result"]["profile_generation_id"],
        run_id="run_" + "2" * 32,
        payload_sha256="3" * 64,
    )
    document["preparation"]["initial_receipt"] = reference_by_field
    receipt = deepcopy(reference_by_field)
    targets_by_field = deepcopy(document["before"]["initial_targets"])
    history_by_phase = {"published": {"document": deepcopy(document)}}
    if damage == "reference":
        receipt["payload_sha256"] = "4" * 64
    elif damage == "targets":
        targets_by_field["profile_rows"] += 1
    elif damage == "history":
        history_by_phase["published"]["document"]["generation"] = "different"
    context = object()

    @asynccontextmanager
    async def original(actual):
        assert actual is locked.fhir
        yield context

    witness = SimpleNamespace(
        _read_transaction=original,
        _receipt=AsyncMock(return_value=receipt),
        _admitted_targets=AsyncMock(return_value=targets_by_field),
        _history=AsyncMock(return_value=history_by_phase),
    )
    monkeypatch.setitem(sys.modules, "process.provider_directory_common_initial_witness", witness)
    monkeypatch.setattr(process, "provider_directory_common_initial_witness", witness, raising=False)
    if damage:
        with pytest.raises(ValueError, match="observation_changed"):
            await observation._initial_witness(locked.fhir, document)
    else:
        await observation._initial_witness(locked.fhir, document)
        witness._receipt.assert_awaited_once_with(context, "unit", reference_by_field["build_id"])
        witness._admitted_targets.assert_awaited_once_with(context, "unit", receipt)
    locked.driver.execute.assert_not_called()
