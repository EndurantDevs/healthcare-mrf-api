# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Reader diagnostics retain actual scope without rendering request or SQL data."""

import asyncio
import json
import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from sanic.exceptions import ServiceUnavailable
from sqlalchemy.exc import DBAPIError

from api import custom_import_read_http as transport
from api import signed_request_reader as boundary
from db.connection import current_session
from tests import test_custom_import_read_http as signatures
from tests.test_api_reader_sessions import ReaderSession, reader_database

REQUEST_ID = "00000000-0000-4000-8000-000000000001"
CORE_ID = "00000000-0000-4000-8000-000000000002"


def _request(ordinary):
    return SimpleNamespace(
        headers={"X-Request-ID": REQUEST_ID},
        id=UUID(CORE_ID),
        ctx=SimpleNamespace(sa_session=ordinary, session=ordinary),
    )


def _database_failure():
    driver = RuntimeError("credential-sentinel")
    driver.sqlstate = "42501"
    return DBAPIError("sql-sentinel", {"token": "parameter-sentinel"}, driver)


def _logged(failure, caplog):
    caplog.clear()
    with caplog.at_level(logging.WARNING, logger=transport.logger.name):
        transport._log_failure(failure)
    record = caplog.records[-1]
    rendered = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s").format(record)
    assert not record.exc_info and not record.stack_info
    for sentinel in ("credential-sentinel", "sql-sentinel", "parameter-sentinel"):
        assert sentinel not in rendered
    return json.loads(record.getMessage().partition(" failed ")[2]), rendered


async def test_signed_failure_keeps_only_scalar_identity_after_restoration(monkeypatch, caplog):
    session = ReaderSession()
    database = reader_database(monkeypatch, [session])
    monkeypatch.setattr(boundary, "db", database)
    ordinary = SimpleNamespace()
    request = _request(ordinary)
    failure = _database_failure()
    with pytest.raises(DBAPIError) as caught:
        async with database.reader_session():
            with boundary._bind_request_reader(request, session):
                assert current_session() is session
                raise failure
    assert caught.value is failure and request.ctx.sa_session is ordinary and request.ctx.session is ordinary
    assert session.closed and not session.info and not database.has_reader_session()
    fields, rendered = _logged(failure, caplog)
    assert fields == {
        "boundary": "signed_read",
        "exception_class": "DBAPIError",
        "sqlstate": "42501",
        "request_id": REQUEST_ID,
        "request_id_source": "forwarded",
        "forwarded_request_id": REQUEST_ID,
        "core_request_id": CORE_ID,
        "reader": {
            "role_oid": 12345,
            "database_oid": 23456,
            "server_address": "192.0.2.1",
            "server_port": 5432,
            "backend_pid": 34567,
        },
    }
    assert "role_oid" in rendered


async def test_timeout_retains_identity_from_cancelled_reader_without_changing_cancellation(monkeypatch, caplog):
    session = ReaderSession()
    database = reader_database(monkeypatch, [session])
    monkeypatch.setattr(boundary, "db", database)
    with pytest.raises(TimeoutError) as caught:
        async with asyncio.timeout(0):
            async with database.reader_session():
                with boundary._bind_request_reader(_request(None), session):
                    await asyncio.sleep(0)
    assert isinstance(caught.value.__cause__, asyncio.CancelledError)
    assert session.rolled_back and session.closed and not session.info and not database.has_reader_session()
    fields, _ = _logged(caught.value, caplog)
    assert fields["reader"]["role_oid"] == 12345 and fields["request_id"] == REQUEST_ID
    assert fields["exception_class"] == "TimeoutError" and fields["sqlstate"] is None


async def test_unowned_or_unverified_sessions_cannot_supply_identity(monkeypatch, caplog):
    session = ReaderSession()
    database = reader_database(monkeypatch, [session])
    session.info.update(api_reader_verified=True, api_reader_identity={"role_oid": 999})
    failure = RuntimeError()
    boundary._remember_reader_failure(failure, _request(session), session, database, boundary="signed_read")
    fields, _ = _logged(failure, caplog)
    assert fields["reader"] is None


async def test_reader_acquisition_failure_keeps_verified_correlation_without_claiming_identity(monkeypatch, caplog):
    database = reader_database(monkeypatch, [])
    failure = _database_failure()
    connect = AsyncMock(side_effect=failure)
    monkeypatch.setattr(database, "_connect_reader", connect)
    monkeypatch.setattr(boundary, "db", database)
    ordinary = SimpleNamespace(in_transaction=lambda: False)
    verified_id = "00000000-0000-4000-8000-000000000004"
    with pytest.raises(DBAPIError) as caught:
        async with boundary.signed_request_reader(_request(ordinary), ordinary, verified_request_id=verified_id):
            pytest.fail("Failed acquisition cannot yield a Reader")
    assert caught.value is failure
    connect.assert_awaited_once()
    fields, _ = _logged(failure, caplog)
    assert fields["reader"] is None and fields["request_id"] == verified_id
    assert fields["request_id_source"] == "verified_transport" and fields["sqlstate"] == "42501"


async def test_reader_failure_metadata_does_not_cross_requests(monkeypatch):
    sessions = [ReaderSession(), ReaderSession()]
    database = reader_database(monkeypatch, sessions.copy())
    monkeypatch.setattr(boundary, "db", database)
    failures = [RuntimeError(), RuntimeError()]
    ids = [REQUEST_ID, CORE_ID]
    for session, failure, identifier in zip(sessions, failures, ids, strict=True):
        request = _request(None)
        request.headers["X-Request-ID"] = identifier
        with pytest.raises(RuntimeError):
            async with database.reader_session():
                with boundary._bind_request_reader(request, session):
                    raise failure
        assert not session.info
    assert [failure._api_reader_diagnostic["request_id"] for failure in failures] == ids
    assert failures[0]._api_reader_diagnostic["reader"] is not failures[1]._api_reader_diagnostic["reader"]


def test_log_fields_reject_opaque_attributes_and_unvalidated_identifiers(caplog):
    class OpaqueError(RuntimeError):
        @property
        def sqlstate(self):
            raise AssertionError("descriptor evaluated")

        def __str__(self):
            raise AssertionError("exception rendered")

    failure = OpaqueError("credential-sentinel")
    failure.__cause__ = failure
    failure._api_reader_diagnostic = {
        "boundary": "signed_read",
        "reader": {"role_oid": "credential-sentinel", "sql": "sql-sentinel"},
        "request_id": "credential-sentinel",
        "core_request_id": object(),
        "parameters": "parameter-sentinel",
    }
    fields, _ = _logged(failure, caplog)
    assert fields["reader"] is None and fields["request_id"] is None and fields["core_request_id"] is None
    assert fields["sqlstate"] is None and "parameters" not in fields


def test_logging_failure_cannot_replace_the_primary_error(monkeypatch):
    def unavailable(*_args, **_kwargs):
        raise OSError("credential-sentinel")

    monkeypatch.setattr(transport.logger, "warning", unavailable)
    assert transport._log_failure(_database_failure()) is None


def test_log_rejects_non_address_ipv6_scope_suffix(caplog):
    failure = RuntimeError()
    failure._api_reader_diagnostic = {
        "boundary": "signed_read",
        "reader": {
            "role_oid": 12345,
            "database_oid": 23456,
            "backend_pid": 34567,
            "server_address": "fe80::1%credential-sentinel",
            "server_port": 5432,
        },
    }
    fields, _ = _logged(failure, caplog)
    assert fields["reader"] is None


async def test_pinned_target_keeps_safe_code_without_restoring_suppressed_exception(caplog):
    failure = _database_failure()
    session = SimpleNamespace(execute=AsyncMock(side_effect=failure))
    with pytest.raises(transport.CustomImportReadUnavailableError) as caught:
        await transport._resolve_pinned_target(session, SimpleNamespace(dataset_key="example"))
    assert caught.value.__cause__ is None and caught.value.__suppress_context__ is True
    fields, _ = _logged(caught.value, caplog)
    assert fields["sqlstate"] == "42501" and fields["caught_exception_class"] == "DBAPIError"


@pytest.mark.parametrize("is_authorized", [True, False])
async def test_signed_failure_uses_verified_context_id_and_retains_closed_response(monkeypatch, caplog, is_authorized):
    verified_id = "00000000-0000-4000-8000-000000000004"
    session = ReaderSession()
    original_execute = session.execute

    async def execute(statement, params=None):
        if str(statement).startswith("SELECT session_user, current_user"):
            return await original_execute(statement, params)
        raise _database_failure()

    session.execute = execute
    signatures._install_keyring(monkeypatch, session)
    headers = signatures._headers(request_id=verified_id)
    headers["X-Request-ID"] = REQUEST_ID
    if not is_authorized:
        headers[transport.CUSTOM_IMPORT_READ_SIGNATURE_HEADER] = signatures._encoded(b"z" * 32)
    request = signatures._Request(signatures._BODY, headers)
    request.id = UUID(CORE_ID)
    with caplog.at_level(logging.WARNING, logger=transport.logger.name):
        reply = await transport.serve_custom_import_search(request, signatures.cold_caller())
    assert reply.body == transport._error(503 if is_authorized else 404).body
    fields = json.loads(caplog.records[-1].getMessage().partition(" failed ")[2])
    if is_authorized:
        assert fields["request_id"] == verified_id and fields["request_id_source"] == "verified_transport"
        assert fields["forwarded_request_id"] == REQUEST_ID and fields["core_request_id"] == CORE_ID
        assert fields["reader"]["role_oid"] == 12345 and fields["sqlstate"] == "42501"
    else:
        assert "request_id" not in fields and "reader" not in fields and not session.executed
    assert not session.info


async def test_claims_pin_logs_before_cleanup_and_preserves_closed_response(monkeypatch, caplog):
    session = ReaderSession()
    database = reader_database(monkeypatch, [session])
    failure = _database_failure()
    monkeypatch.setattr("api.reference_family_reads.pin_claims_reader", AsyncMock(side_effect=failure))
    middleware_by_kind = {}

    class App:
        def listener(self, _name):
            return lambda callback: callback

        def middleware(self, name):
            return lambda callback: middleware_by_kind.setdefault(name, callback)

    database.init_app(App())
    request = _request(None)
    request.path = "/control/v1/plan-pricing/code-projection"
    with caplog.at_level(logging.WARNING, logger=transport.logger.name):
        with pytest.raises(ServiceUnavailable) as caught:
            await middleware_by_kind["request"](request)
    assert caught.value.__cause__ is None and caught.value.__suppress_context__ is True
    assert str(caught.value) == "Pricing snapshot is temporarily unavailable"
    assert session.closed and not session.info and not database.has_reader_session()
    fields = json.loads(caplog.records[-1].getMessage().partition(" failed ")[2])
    assert fields["boundary"] == "claims_pin" and fields["reader"]["role_oid"] == 12345
    assert fields["sqlstate"] == "42501" and fields["request_id"] == REQUEST_ID
