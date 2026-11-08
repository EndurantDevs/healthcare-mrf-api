# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Initial inventory rejects malformed authority and sanitizes queue failures."""

import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import asyncpg
import pytest
from sanic.exceptions import Forbidden
from sqlalchemy.exc import SQLAlchemyError

from api.endpoint import registry_publication as publication
from process.registry_record_store import RegistryRecordConflict


def _request(**changes):
    payload = {
        "operation": "publish_initial_inventory",
        "bootstrap_id": "22222222-2222-4222-8222-222222222222",
        "bootstrap_generation_sha256": "b" * 64,
        "expected_serving_generation": 0,
        "expected_approved_revision": 0,
        "reason": "Publish verified initial inventory",
        "idempotency_key": "synthetic-initial-request",
        "actor": {
            "kind": "platform_admin",
            "user_id": "11111111-1111-4111-8111-111111111111",
            "client_id": "system",
        },
        "session_token_sha256": "a" * 64,
        **changes,
    }
    return SimpleNamespace(
        body=json.dumps(payload).encode(),
        args={},
        query_string="",
        headers={"Authorization": "Bearer synthetic-registry-token"},
        ctx=SimpleNamespace(sa_session=SimpleNamespace(begin=lambda: pytest.fail("unexpected transaction"))),
    )


@pytest.fixture(autouse=True)
def registry_token(monkeypatch):
    monkeypatch.setenv("HLTHPRT_REGISTRY_CONTROL_API_TOKEN", "synthetic-registry-token")
    monkeypatch.delenv("HLTHPRT_CONTROL_API_TOKEN", raising=False)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changes",
    [
        {"actor": None},
        {"actor": []},
        {"actor": {"kind": "platform_admin"}},
        {"session_token_sha256": None},
        {"session_token_sha256": "A" * 64},
        {"session_token_sha256": "a" * 63},
    ],
)
async def test_initial_authority_shape_is_rejected_before_transaction(changes):
    response = await publication.queue_initial_inventory(_request(**changes))
    assert response.status == 400
    assert json.loads(response.body) == {"error": {"code": "registry_request_invalid"}}
    assert response.headers["Cache-Control"] == "private, no-store"


@pytest.mark.asyncio
async def test_initial_request_requires_service_auth_before_body_or_database():
    request = _request(actor=None)
    request.headers = {}
    with pytest.raises(Forbidden):
        await publication.queue_initial_inventory(request)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("failure", "status", "code"),
    [
        (RegistryRecordConflict, 409, "registry_revision_conflict"),
        (asyncpg.PostgresError, 503, "registry_unavailable"),
        (SQLAlchemyError, 503, "registry_unavailable"),
    ],
)
async def test_initial_queue_failure_rolls_back_and_hides_exception(monkeypatch, failure, status, code):
    events = []

    @asynccontextmanager
    async def transaction():
        events.append("begin")
        try:
            yield
        except BaseException:
            events.append("rollback")
            raise
        else:
            events.append("commit")

    request = _request()
    request.ctx.sa_session.begin = transaction
    driver = object()
    checkout = AsyncMock(return_value=driver)
    enqueue = AsyncMock(side_effect=failure("synthetic private database detail"))
    monkeypatch.setattr(publication, "_driver", checkout)
    monkeypatch.setattr(publication, "enqueue_registry_initial_inventory", enqueue)
    response = await publication.queue_initial_inventory(request)
    assert response.status == status
    assert json.loads(response.body) == {"error": {"code": code}}
    assert response.headers["Cache-Control"] == "private, no-store"
    assert b"synthetic private database detail" not in response.body
    assert events == ["begin", "rollback"]
    checkout.assert_awaited_once_with(request.ctx.sa_session)
    enqueue.assert_awaited_once()
    assert enqueue.await_args.args[0] is driver
    assert enqueue.await_args.kwargs["session_token_sha256"] == "a" * 64
