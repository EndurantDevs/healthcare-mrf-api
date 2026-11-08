# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Continuation control has separate credentials and closed, source-bound requests."""

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import control_build_continuation as control
from process.custom_import.execution import IdempotencyConflict
from tests.test_control_execution_stop import _payload, _Session
from tests.test_custom_import_build_continuation import retained as retained


def _request(document, *, bearer="synthetic-continuation", query=""):
    return SimpleNamespace(
        headers={"Authorization": "Bearer " + bearer}, body=json.dumps(document).encode(), query_string=query
    )


def _body(retained, *, authorize):
    body_by_field = {key: value for key, value in retained.pins.items() if key in control._PINS}
    body_by_field.update({"authorization": retained.authorization} if authorize else {"build_id": 29})
    return body_by_field


@pytest.mark.parametrize("authorize", [False, True])
async def test_separate_token_is_checked_before_body_or_database(monkeypatch, authorize):
    monkeypatch.setenv(control._TOKEN_ENV, "synthetic-continuation")
    reply = await control.serve_build_continuation(
        SimpleNamespace(headers={}, body=b"not-json"), None, authorize=authorize
    )
    assert reply.status == 403 and _payload(reply) == {"error": "forbidden"}


@pytest.mark.parametrize(
    "other",
    [
        "HLTHPRT_CONTROL_API_TOKEN",
        "HLTHPRT_CUSTOM_IMPORT_EXECUTION_STOP_TOKEN",
        "HLTHPRT_CUSTOM_IMPORT_EVIDENCE_READ_TOKEN",
    ],
)
def test_shared_control_credentials_fail_closed(monkeypatch, other):
    monkeypatch.setenv(control._TOKEN_ENV, "synthetic-continuation")
    monkeypatch.setenv(other, "synthetic-continuation")
    assert not control._authorized({"Authorization": "Bearer synthetic-continuation"})


@pytest.mark.parametrize("authorize", [False, True])
@pytest.mark.parametrize("bad", ["extra", "bool", "query", "duplicate", "large"])
async def test_closed_body_validation_precedes_session_use(monkeypatch, retained, authorize, bad):
    monkeypatch.setenv(control._TOKEN_ENV, "synthetic-continuation")
    request = _request(_body(retained, authorize=authorize))
    if bad == "extra":
        document = _body(retained, authorize=authorize)
        document["extra"] = True
        request.body = json.dumps(document).encode()
    elif bad == "bool":
        document = _body(retained, authorize=authorize)
        document["dataset_id"] = True
        request.body = json.dumps(document).encode()
    elif bad == "query":
        request.query_string = "extra=true"
    elif bad == "duplicate":
        request.body = b'{"dataset_id":7,"dataset_id":8}'
    else:
        request.body = b" " * 16385
    reply = await control.serve_build_continuation(request, None, authorize=authorize)
    assert reply.status == 400 and _payload(reply) == {"error": "invalid_request"}


@pytest.mark.parametrize("authorize", [False, True])
async def test_exact_configured_source_identity_is_forwarded_without_acquisition(monkeypatch, retained, authorize):
    monkeypatch.setenv(control._TOKEN_ENV, "synthetic-continuation")
    session = _Session()
    session.execute = AsyncMock()
    identity = AsyncMock(return_value=(13, b"r" * 32))
    operation = AsyncMock(return_value={"status": "synthetic"})
    monkeypatch.setattr(control, "_retained_identity", identity)
    monkeypatch.setattr(
        control, "authorize_build_continuation" if authorize else "inspect_build_continuation", operation
    )
    reply = await control.serve_build_continuation(
        _request(_body(retained, authorize=authorize)), session, authorize=authorize
    )
    assert reply.status == 200 and _payload(reply) == {"status": "synthetic"}
    identity.assert_awaited_once_with(session, 7, 11, 23)
    assert operation.await_args.kwargs == retained.pins | ({} if authorize else {"build_id": 29})
    if authorize:
        assert operation.await_args.args == (session, retained.authorization)
    else:
        assert "REPEATABLE READ READ ONLY" in str(session.execute.await_args_list[0].args[0])


@pytest.mark.parametrize(
    "error,status", [(IdempotencyConflict("synthetic-private"), 409), (RuntimeError("synthetic-private"), 503)]
)
async def test_control_failures_do_not_echo_context_or_credential(monkeypatch, retained, error, status):
    monkeypatch.setenv(control._TOKEN_ENV, "synthetic-continuation")
    session = _Session()
    session.execute = AsyncMock()
    monkeypatch.setattr(control, "_retained_identity", AsyncMock(side_effect=error))
    reply = await control.serve_build_continuation(_request(_body(retained, authorize=True)), session, authorize=True)
    assert reply.status == status and "synthetic-private" not in reply.body.decode()
