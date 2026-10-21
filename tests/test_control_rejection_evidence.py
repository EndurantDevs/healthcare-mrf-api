# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Retained-row HTTP reads require their own audience and exact request pins."""

from __future__ import annotations

import asyncio
import json
import uuid
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest
from sanic import Sanic
from sqlalchemy.dialects.postgresql import dialect
from sqlalchemy.sql.elements import TextClause

from api import control_rejection_evidence as http
from process.custom_import import rejection_evidence as reader
from tests.test_custom_import_rejection_evidence import _PINS, _event, _Result, _retained, _Session

_TOKEN = "synthetic-rejection-read-token"
_REQUEST = {
    "dataset_id": 3,
    "definition_revision_id": 5,
    "idempotency_key": "synthetic-run",
    "build_id": None,
    "after_ordinal": None,
    "scope_sha256": None,
    "limit": 100,
}


def _request(document=None, *, body=None, headers=None, query=""):
    return SimpleNamespace(
        headers={"Authorization": "Bearer " + _TOKEN, "Content-Type": "application/json"}
        if headers is None
        else headers,
        body=json.dumps(_REQUEST if document is None else document).encode() if body is None else body,
        query_string=query,
    )


def _payload(reply, status=200):
    assert reply.status == status
    assert reply.headers["Cache-Control"] == "no-store"
    assert reply.content_type == "application/json"
    assert len(reply.body) <= http._MAX_PAGE_BYTES
    return json.loads(reply.body)


class _HttpSession(_Session):
    def __init__(self, scope_rows, collections, events):
        super().__init__(scope_rows, collections, events)
        self.executions = [SimpleNamespace(execution_id=17, dataset_id=3, state="failed")]
        self.builds = [SimpleNamespace(build_id=23)]
        self.transactions = 0

    @asynccontextmanager
    async def begin(self):
        assert not self.active
        self.transactions += 1
        async with super().begin():
            yield

    async def execute(self, statement, parameters=None):
        assert self.active
        if not isinstance(statement, TextClause):
            first = statement.column_descriptions[0]["name"]
            if first in {"execution_id", "build_id"}:
                self.statements.append(statement)
                if first == "execution_id":
                    return _Result(self.executions)
                build_id = statement.compile(dialect=dialect()).params.get("build_id_1")
                return _Result([build for build in self.builds if build_id is None or build.build_id == build_id])
        return await super().execute(statement, parameters)


@pytest.fixture
def retained(monkeypatch):
    monkeypatch.setenv(http._TOKEN_ENV, _TOKEN)
    for index, name in enumerate(http._OTHER_TOKEN_ENVS):
        monkeypatch.setenv(name, f"synthetic-other-token-{index}")
    definition, rows, collections = _retained()
    session = _HttpSession([rows], collections, [_event(definition)])
    return SimpleNamespace(definition=definition, rows=rows, session=session)


@pytest.mark.parametrize(
    "token", [None, "", "Bearer wrong", "Basic synthetic", "Bearer synthetic-rejection-read-token "]
)
async def test_auth_precedes_body_and_database(retained, token):
    class Unreadable:
        headers = {} if token is None else {"Authorization": token}

        @property
        def body(self):
            raise AssertionError("unauthorized body must not be read")

    reply = await http.serve_rejection_evidence(Unreadable(), retained.session)
    assert _payload(reply, 403) == {"error": "forbidden"}
    assert retained.session.transactions == 0 and retained.session.statements == []


@pytest.mark.parametrize("name", http._OTHER_TOKEN_ENVS)
async def test_other_credentials_cannot_read_rows_or_be_reused(retained, monkeypatch, name):
    monkeypatch.setenv(name, "synthetic-other-audience")
    request = _request(headers={"Authorization": "Bearer synthetic-other-audience"})
    assert _payload(await http.serve_rejection_evidence(request, retained.session), 403) == {"error": "forbidden"}
    monkeypatch.setenv(name, _TOKEN)
    assert _payload(await http.serve_rejection_evidence(_request(), retained.session), 403) == {"error": "forbidden"}
    assert retained.session.statements == []


@pytest.mark.parametrize("value", [None, "", " ", "invalid token"])
async def test_absent_or_invalid_rejection_credential_fails_closed(retained, monkeypatch, value):
    if value is None:
        monkeypatch.delenv(http._TOKEN_ENV)
    else:
        monkeypatch.setenv(http._TOKEN_ENV, value)
    assert _payload(await http.serve_rejection_evidence(_request(), retained.session), 403) == {"error": "forbidden"}
    assert retained.session.statements == []


class _RepeatedHeaders(dict):
    def __init__(self, duplicate):
        super().__init__({"Authorization": "Bearer " + _TOKEN, "Content-Type": "application/json"})
        self.duplicate = duplicate

    def getall(self, name, default):
        return [self[name]] * (2 if name == self.duplicate else 1)


@pytest.mark.parametrize("name,status", [("Authorization", 403), ("Content-Type", 400)])
async def test_duplicate_boundary_headers_are_rejected(retained, name, status):
    reply = await http.serve_rejection_evidence(_request(headers=_RepeatedHeaders(name)), retained.session)
    assert _payload(reply, status) == {"error": "forbidden" if status == 403 else "invalid_request"}
    assert retained.session.statements == []


@pytest.mark.parametrize(
    "changes",
    [
        {"dataset_id": True},
        {"dataset_id": 0},
        {"definition_revision_id": "5"},
        {"definition_revision_id": 2**63},
        {"build_id": False},
        {"build_id": 0},
        {"build_id": 2**63},
        {"idempotency_key": None},
        {"idempotency_key": "invalid key"},
        {"idempotency_key": "x" * 129},
        {"after_ordinal": 0},
        {"after_ordinal": True},
        {"after_ordinal": -1},
        {"after_ordinal": 2**63},
        {"after_ordinal": 0, "scope_sha256": "a" * 64},
        {"build_id": 23, "after_ordinal": 0},
        {"scope_sha256": "a" * 64},
        {"build_id": 23, "scope_sha256": "A" * 64},
        {"build_id": 23, "scope_sha256": 1},
        {"limit": True},
        {"limit": 0},
        {"limit": 101},
        {"limit": "1"},
        {"execution_id": 17},
        {"producer_fence": 2},
        {"relation": "synthetic"},
    ],
)
async def test_closed_selector_rejects_untrusted_fields_before_database(retained, changes):
    reply = await http.serve_rejection_evidence(_request(_REQUEST | changes), retained.session)
    assert _payload(reply, 400) == {"error": "invalid_request"}
    assert retained.session.statements == [] and retained.session.transactions == 0


@pytest.mark.parametrize(
    "body",
    [
        b"",
        b"[]",
        b"null",
        b"{}",
        b"{",
        b"\xff",
        b"[" * 2000,
        b'{"dataset_id":3,"dataset_id":3}',
        b" " * 4097,
        json.dumps(_REQUEST).encode("utf-16"),
    ],
)
async def test_malformed_oversized_or_duplicate_json_is_rejected(retained, body):
    reply = await http.serve_rejection_evidence(_request(body=body), retained.session)
    assert _payload(reply, 400) == {"error": "invalid_request"}
    assert retained.session.statements == []


@pytest.mark.parametrize(
    "content_type", [None, "text/plain", "application/json; charset=utf-16", "application/json; other=yes"]
)
async def test_request_must_be_utf8_json(retained, content_type):
    headers_by_name = {"Authorization": "Bearer " + _TOKEN}
    if content_type is not None:
        headers_by_name["Content-Type"] = content_type
    reply = await http.serve_rejection_evidence(_request(headers=headers_by_name), retained.session)
    assert _payload(reply, 400) == {"error": "invalid_request"}
    assert retained.session.statements == []


async def test_query_parameters_are_never_alternative_selectors(retained):
    reply = await http.serve_rejection_evidence(_request(query="limit=1"), retained.session)
    assert _payload(reply, 400) == {"error": "invalid_request"}
    assert retained.session.statements == []


async def test_lookup_and_typed_pages_share_one_owned_read_transaction(retained):
    retained.session.events.append(_event(retained.definition, 1, value="0000000002"))
    first = _payload(await http.serve_rejection_evidence(_request(_REQUEST | {"limit": 1}), retained.session))
    assert first["scope"]["build_id"] == "23" and first["scope"]["execution_id"] == "17"
    assert first["next_after_ordinal"] == "0" and not first["complete"]
    assert first["events"][0]["logical_key"] == {"npi": {"type": "string", "value": "0000000001"}}
    assert retained.session.transactions == 1 and not retained.session.active
    statements = retained.session.statements
    assert [str(statement) for statement in statements[:2]] == [
        "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY",
        "SET LOCAL statement_timeout = '5s'",
    ]
    lookup = statements[2].compile(dialect=dialect())
    assert lookup.params == {"definition_revision_id_1": 5, "idempotency_key_1": "synthetic-run", "param_1": 2}
    build_lookup = statements[3].compile(dialect=dialect())
    assert build_lookup.params == {"execution_id_1": 17, "param_1": 2}
    assert all(word not in str(build_lookup) for word in ("max(", "fence", "lease", "ORDER BY"))
    second = _payload(
        await http.serve_rejection_evidence(
            _request(
                _REQUEST
                | {
                    "build_id": 23,
                    "after_ordinal": 0,
                    "scope_sha256": first["scope_sha256"],
                    "limit": 1,
                }
            ),
            retained.session,
        )
    )
    assert second["scope"] == first["scope"] and second["scope_sha256"] == first["scope_sha256"]
    assert second["complete"] and second["next_after_ordinal"] is None
    assert [event["ordinal"] for event in second["events"]] == ["1"]
    assert retained.session.transactions == 2 and not retained.session.active
    assert "producer" not in json.dumps(first) and "canonical_evidence" not in json.dumps(first)


@pytest.mark.parametrize("foreign", [False, True])
async def test_missing_and_foreign_execution_locators_are_indistinguishable(retained, foreign):
    retained.session.executions = [SimpleNamespace(execution_id=17, dataset_id=99, state="failed")] if foreign else []
    reply = await http.serve_rejection_evidence(_request(), retained.session)
    assert _payload(reply, 404) == {"error": "not_found"}
    assert retained.session.scope_reads == 0 and not retained.session.active


@pytest.mark.parametrize("kind", ["executions", "builds", "missing_build", "foreign_build"])
async def test_ambiguous_or_unavailable_attempt_never_selects_the_newest_build(retained, kind):
    request = _request()
    if kind == "executions":
        retained.session.executions *= 2
    elif kind == "builds":
        retained.session.builds.append(SimpleNamespace(build_id=24))
    elif kind == "missing_build":
        retained.session.builds = []
    else:
        request = _request(_REQUEST | {"build_id": 99})
    reply = await http.serve_rejection_evidence(request, retained.session)
    assert _payload(reply, 503) == {"error": "evidence_unavailable"}
    assert retained.session.scope_reads == 0 and not retained.session.active


async def test_explicit_retained_build_does_not_follow_a_different_attempt(retained):
    retained.session.builds.append(SimpleNamespace(build_id=24))
    reply = await http.serve_rejection_evidence(_request(_REQUEST | {"build_id": 23}), retained.session)
    assert _payload(reply)["scope"]["build_id"] == "23"


async def test_nonterminal_execution_is_a_conflict_without_reading_storage(retained):
    retained.session.executions[0].state = "running"
    reply = await http.serve_rejection_evidence(_request(), retained.session)
    assert _payload(reply, 409) == {"error": "evidence_not_final"}
    assert len(retained.session.statements) == 3 and not retained.session.active


async def test_changed_retained_scope_is_unavailable_without_payload_read(retained):
    reply = await http.serve_rejection_evidence(
        _request(
            _REQUEST
            | {
                "build_id": 23,
                "after_ordinal": 0,
                "scope_sha256": "a" * 64,
            }
        ),
        retained.session,
    )
    assert _payload(reply, 503) == {"error": "evidence_unavailable"}
    assert retained.session.scope_reads == 2
    assert retained.session.statements[-1].column_descriptions[0]["name"] == "CustomImportExecution"


async def test_finalized_empty_retained_page_is_success(retained):
    retained.session.events = []
    payload = _payload(await http.serve_rejection_evidence(_request(), retained.session))
    assert payload["events"] == [] and payload["returned_rejection_events"] == 0
    assert payload["complete"] and payload["next_after_ordinal"] is None


async def test_http_reserves_envelope_bytes_without_changing_cli_budget(retained):
    retained.session.events = [_event(retained.definition, index, value="x" * 30_300) for index in range(2)]
    cli_page = await reader.read_rejection_page(retained.session, **_PINS)
    assert len(cli_page["events"]) == 2 and cli_page["complete"]
    assert http._MAX_PAGE_BYTES < len(reader._encoded(cli_page)) <= reader.MAX_PAGE_BYTES
    first = _payload(await http.serve_rejection_evidence(_request(), retained.session))
    assert len(first["events"]) == 1 and not first["complete"] and first["next_after_ordinal"] == "0"
    second = _payload(
        await http.serve_rejection_evidence(
            _request(
                _REQUEST
                | {
                    "build_id": 23,
                    "after_ordinal": 0,
                    "scope_sha256": first["scope_sha256"],
                }
            ),
            retained.session,
        )
    )
    assert second["complete"] and second["events"][0]["ordinal"] == "1"


async def test_single_http_oversized_event_is_unavailable_without_truncation(retained):
    retained.session.events = [_event(retained.definition, value="x" * 62_000)]
    cli_page = await reader.read_rejection_page(retained.session, **_PINS)
    assert cli_page["events"][0]["logical_key"]["npi"]["value"] == "x" * 62_000
    reply = await http.serve_rejection_evidence(_request(), retained.session)
    assert _payload(reply, 503) == {"error": "evidence_unavailable"}
    assert retained.session.statements[-1].column_descriptions[0]["name"] == "rejection_ordinal"


@pytest.mark.parametrize(
    "failure", [RuntimeError("synthetic storage detail"), PermissionError("synthetic denied role"), TimeoutError()]
)
async def test_storage_failures_are_sanitized_and_close_transaction(retained, failure):
    retained.session.failure = failure
    reply = await http.serve_rejection_evidence(_request(), retained.session)
    assert _payload(reply, 503) == {"error": "evidence_unavailable"}
    assert not retained.session.active


async def test_transaction_exit_failure_discards_the_buffered_page(retained, monkeypatch):
    begin = retained.session.begin

    @asynccontextmanager
    async def failed_exit():
        async with begin():
            yield
            raise RuntimeError("synthetic transaction cleanup failure")

    monkeypatch.setattr(retained.session, "begin", failed_exit)
    reply = await http.serve_rejection_evidence(_request(), retained.session)
    assert _payload(reply, 503) == {"error": "evidence_unavailable"}
    assert retained.session.scope_reads == 2 and not retained.session.active


async def test_caller_transaction_is_never_repurposed(retained):
    retained.session.active = True
    reply = await http.serve_rejection_evidence(_request(), retained.session)
    assert _payload(reply, 503) == {"error": "evidence_unavailable"}
    assert retained.session.active and retained.session.statements == [] and retained.session.transactions == 0


async def test_auth_precedes_missing_session(monkeypatch):
    monkeypatch.delenv(http._TOKEN_ENV, raising=False)
    reply = await http.rejection_page(SimpleNamespace(**vars(_request()), ctx=SimpleNamespace()))
    assert _payload(reply, 403) == {"error": "forbidden"}


async def test_authorized_missing_session_is_unavailable(retained):
    reply = await http.rejection_page(SimpleNamespace(**vars(_request()), ctx=SimpleNamespace()))
    assert _payload(reply, 503) == {"error": "evidence_unavailable"}


async def test_caller_cancellation_propagates_without_a_partial_page(retained):
    retained.session.failure = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        await http.serve_rejection_evidence(_request(), retained.session)
    assert not retained.session.active


@pytest.mark.parametrize(
    "method,suffix,headers,status",
    [
        ("POST", "", {"Authorization": "Bearer " + _TOKEN}, 200),
        ("POST", "", {}, 403),
        ("POST", "?limit=1", {"Authorization": "Bearer " + _TOKEN}, 400),
        ("GET", "", {"Authorization": "Bearer " + _TOKEN}, 405),
        ("POST", "/", {"Authorization": "Bearer " + _TOKEN}, 404),
    ],
)
async def test_fixed_http_route_keeps_all_responses_private(retained, method, suffix, headers, status):
    app = Sanic(f"synthetic-rejection-route-{uuid.uuid4().hex}")
    http.register_rejection_private_responses(app)
    app.blueprint(http.blueprint)

    @app.middleware("request")
    async def bind(request):
        request.ctx.sa_session = retained.session

    _, reply = await app.asgi_client.request(method, http._PAGE_PATH + suffix, json=_REQUEST, headers=headers)
    assert reply.status_code == status
    assert reply.headers["Cache-Control"] == "no-store"
    if status == 200:
        assert reply.json["scope"]["build_id"] == "23"
        assert retained.session.transactions == 1 and not retained.session.active
    else:
        assert retained.session.statements == [] and retained.session.transactions == 0
