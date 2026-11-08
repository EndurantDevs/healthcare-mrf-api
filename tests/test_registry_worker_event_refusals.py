# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Idempotent worker requests cannot replace the event already admitted."""

from copy import deepcopy
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from api import control_imports
from tests.test_registry_aca_source_worker import event as aca_event
from tests.test_registry_ptg_source_worker import event as ptg_event


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "importer,event_factory", [("registry-aca-source-worker", aca_event), ("registry-ptg-source-worker", ptg_event)]
)
@pytest.mark.parametrize("outcome", ["new", "same", "different_event", "different_importer", "malformed_previous"])
async def test_idempotent_worker_replays_only_the_same_valid_event(monkeypatch, importer, event_factory, outcome):
    requested = event_factory()
    existing = None if outcome == "new" else {"importer": importer, "params": deepcopy(requested)}
    if outcome == "different_event":
        existing["params"]["capture_id"] = str(UUID(int=77))
    elif outcome == "different_importer":
        existing["importer"] = "different-worker"
    elif outcome == "malformed_previous":
        existing["params"]["unexpected"] = "unsupported"
    lookup = AsyncMock(return_value=existing)
    active_lookup = AsyncMock(side_effect=AssertionError("An event cannot fall back to an active-run lookup"))
    monkeypatch.setattr(control_imports, "find_importer_run_by_idempotency_key", lookup)
    monkeypatch.setattr(control_imports, "find_active_run_by_idempotency_key", active_lookup)
    before = deepcopy(existing)
    if outcome in {"new", "same"}:
        result = await control_imports._idempotent_import_run(importer, "synthetic-key", params=requested)
        assert result is existing
    else:
        with pytest.raises(ValueError):
            await control_imports._idempotent_import_run(importer, "synthetic-key", params=requested)
    lookup.assert_awaited_once_with(importer, "synthetic-key")
    active_lookup.assert_not_called()
    assert existing == before and requested == event_factory()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "importer,event_factory", [("registry-aca-source-worker", aca_event), ("registry-ptg-source-worker", ptg_event)]
)
async def test_invalid_event_never_becomes_a_durable_worker_request(monkeypatch, importer, event_factory):
    lookup = AsyncMock(side_effect=AssertionError("A malformed event reached durable admission"))
    enqueue = AsyncMock(side_effect=AssertionError("A malformed event reached the queue"))
    monkeypatch.setattr(control_imports, "_idempotent_import_run", lookup)
    monkeypatch.setattr(control_imports, "_enqueue_import_start", enqueue)
    with pytest.raises(ValueError):
        await control_imports.create_import_run(
            {"importer": importer, "params": {**event_factory(), "unexpected": "unsupported"}}
        )
    lookup.assert_not_called()
    enqueue.assert_not_called()
