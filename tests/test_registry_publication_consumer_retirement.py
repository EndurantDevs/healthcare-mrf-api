# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
from contextlib import asynccontextmanager
from types import SimpleNamespace
from uuid import uuid4

import pytest

from process import registry_publication_consumer as consumer

pytestmark = pytest.mark.asyncio


class _Pool:
    def __init__(self, error=None):
        self.error, self.acquired = error, 0

    @asynccontextmanager
    async def acquire(self):
        self.acquired += 1
        if self.error is not None:
            raise self.error
        yield SimpleNamespace(transaction=self._transaction)

    @asynccontextmanager
    async def _transaction(self):
        yield


async def _finished(*_args, **_kwargs):
    return None


@pytest.mark.parametrize("metadata_only", [True, False])
async def test_reject_retires_candidates_only_for_membership_publication(monkeypatch, metadata_only):
    retired_ids = []

    async def retire(_pool, _config, request_id):
        retired_ids.append(request_id)

    monkeypatch.setattr(consumer, "_verify_lifecycle", _finished)
    monkeypatch.setattr(consumer, "finish_registry_publication_request", _finished)
    monkeypatch.setattr(consumer, "_retire_rejected_candidates", retire)
    request_id = uuid4()
    config = SimpleNamespace(metadata_only=metadata_only, control_schema="registry")
    await consumer._reject(_Pool(), _Pool(), config, {"request_id": request_id, "lease_id": uuid4()})
    assert retired_ids == ([] if metadata_only else [request_id])


async def test_candidate_retirement_failure_keeps_the_committed_rejection():
    pool = _Pool(error=ConnectionError("synthetic"))
    config = SimpleNamespace(metadata_only=False, control_schema="registry")
    assert await consumer._retire_rejected_candidates(pool, config, uuid4()) is None
    assert pool.acquired == 1
