# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Shared synthetic HTTP state for exact billing-search boundary tests."""

from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from types import SimpleNamespace

from sqlalchemy.ext.asyncio import AsyncSession

from api import billing_search_http as billing_http
from api.billing_search_transport_contract import BILLING_SEARCH_TRANSPORT_PATH
from db import connection

TRUSTED_NOW = "2026-08-07T12:34:56Z"
SUCCESS_TRANSACTION_EVENTS = [
    "load_transport_keyring",
    "load_cursor_keyring",
    "authorize",
    "transaction_begin",
    "transaction_enter",
    (
        "execute",
        "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY",
    ),
    "service",
    "shape",
    "encode",
    ("transaction_exit", None),
]


class RecordingTransaction:
    def __init__(self, session: RecordingSession) -> None:
        self.session = session

    async def __aenter__(self):
        assert not self.session.active
        self.session.active = True
        self.session.events.append("transaction_enter")
        return self.session

    async def __aexit__(self, exc_type, _exc, _traceback):
        self.session.events.append(("transaction_exit", None if exc_type is None else exc_type.__name__))
        self.session.active = False
        return False


class RecordingSession:
    def __init__(self) -> None:
        self.active = False
        self.begin_count = 0
        self.events: list[object] = []

    def begin(self):
        self.begin_count += 1
        self.events.append("transaction_begin")
        return RecordingTransaction(self)

    def in_transaction(self):
        return self.active

    async def execute(self, statement):
        assert self.active
        self.events.append(("execute", str(statement)))
        return object()


class RetainedReaderSession(AsyncSession):
    """Use real transaction state without an engine or database connection."""

    def __init__(self):
        super().__init__()
        self.events: list[object] = []


@asynccontextmanager
async def bound_reader(session):
    """Mirror the current-task ownership established by the Reader middleware."""

    session.info["api_reader_verified"] = True
    reader_token = connection._READER.set(
        connection._TransactionBinding(id(connection.db), session, asyncio.current_task())
    )
    session_token = connection._SESSION.set(session)
    try:
        yield
    finally:
        connection._SESSION.reset(session_token)
        connection._READER.reset(reader_token)
        session.info.pop("api_reader_verified", None)


def make_request(
    *,
    path: str = BILLING_SEARCH_TRANSPORT_PATH,
    method: str = "GET",
):
    return SimpleNamespace(
        args={"billing_entity_ref": "be1_synthetic"},
        headers={"X-Synthetic": "value"},
        method=method,
        path=path,
    )


def response_payload(http_response) -> dict[str, object]:
    return json.loads(http_response.body)


def assert_private_response(http_response, *, status: int, code: str) -> None:
    assert http_response.status == status
    assert http_response.headers.get("Cache-Control") == "private, no-store"
    assert response_payload(http_response)["error"]["code"] == code


def install_authorized_boundary(monkeypatch, events: list[object]):
    transport_keyring = object()
    cursor_keyring = object()
    endpoint_access = object()

    def load_transport():
        events.append("load_transport_keyring")
        return transport_keyring

    def load_cursor():
        events.append("load_cursor_keyring")
        return cursor_keyring

    def authorize(parameters, headers, **kwargs):
        events.append("authorize")
        assert parameters == {"billing_entity_ref": "be1_synthetic"}
        assert headers == {"X-Synthetic": "value"}
        assert kwargs == {
            "method": "GET",
            "path": BILLING_SEARCH_TRANSPORT_PATH,
            "trusted_now": TRUSTED_NOW,
            "keyring": transport_keyring,
        }
        return endpoint_access

    monkeypatch.setattr(billing_http, "_trusted_now", lambda: TRUSTED_NOW)
    monkeypatch.setattr(billing_http, "_transport_keyring", load_transport)
    monkeypatch.setattr(billing_http, "_cursor_keyring", load_cursor)
    monkeypatch.setattr(billing_http, "authorize_billing_search_endpoint", authorize)
    return endpoint_access, cursor_keyring


def install_success_pipeline(
    monkeypatch,
    session: RecordingSession | RetainedReaderSession,
    endpoint_access: object,
    cursor_keyring: object,
    service_result: object,
) -> None:
    async def search(call_session, **kwargs):
        assert call_session is session
        assert session.in_transaction()
        session.events.append("service")
        assert kwargs == {
            "access": endpoint_access,
            "cursor_keyring": cursor_keyring,
            "trusted_now": TRUSTED_NOW,
        }
        return service_result

    def shape(call_endpoint_access, call_service_result, **kwargs):
        assert session.in_transaction()
        session.events.append("shape")
        assert call_endpoint_access is endpoint_access
        assert call_service_result is service_result
        assert kwargs == {
            "cursor_keyring": cursor_keyring,
            "trusted_now": TRUSTED_NOW,
        }
        return {"result_state": "matched", "items": []}

    original_dumps = billing_http.orjson.dumps

    def encode(response_payload_by_field):
        assert session.in_transaction()
        session.events.append("encode")
        return original_dumps(response_payload_by_field)

    monkeypatch.setattr(billing_http, "search_exact_billing_provider_page", search)
    monkeypatch.setattr(billing_http, "shape_billing_search_response", shape)
    monkeypatch.setattr(billing_http.orjson, "dumps", encode)
