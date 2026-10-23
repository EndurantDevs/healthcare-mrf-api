# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Failure boundaries around Reader ownership and safe request diagnostics."""

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from api import custom_import_read_http as transport
from api import signed_request_reader as boundary
from tests.signed_request_reader_support import cold_caller, install_reader_factory
from tests.test_api_reader_sessions import ReaderSession
from tests.test_reader_failure_diagnostics import CORE_ID, REQUEST_ID, _database_failure, _logged


@pytest.mark.parametrize("address,port", [(None, None), ("not-an-ip-address", 5432)])
async def test_reader_failure_logs_only_valid_native_identity(monkeypatch, caplog, address, port):
    session = ReaderSession()
    database = install_reader_factory(monkeypatch, session)
    ordinary = cold_caller()
    request = SimpleNamespace(path="/api/v1/extensions/custom-import/search", headers={})
    failure = RuntimeError("credential-sentinel")
    with pytest.raises(RuntimeError) as caught:
        async with boundary.signed_request_reader(request, ordinary, verified_request_id=REQUEST_ID):
            session.info["api_reader_identity"].update(server_address=address, server_port=port)
            raise failure
    assert caught.value is failure and not hasattr(request, "ctx")
    assert session.closed and session.rolled_back and not database.has_reader_session()
    fields, _ = _logged(failure, caplog)
    assert fields["request_id"] == REQUEST_ID and fields["request_id_source"] == "verified_transport"
    assert fields["reader"] == (
        {"role_oid": 12345, "database_oid": 23456, "backend_pid": 34567, "server_address": None, "server_port": None}
        if address is None
        else None
    )


@pytest.mark.parametrize("failure_type", [RuntimeError, asyncio.CancelledError])
@pytest.mark.parametrize("existing_context", [False, True])
async def test_diagnostic_failure_preserves_error_and_context(monkeypatch, failure_type, existing_context):
    class BrokenHeaders:
        def get(self, _name):
            raise ValueError("synthetic header failure")

    session = ReaderSession()
    database = install_reader_factory(monkeypatch, session)
    ordinary = cold_caller()
    request = SimpleNamespace(path="/api/v1/extensions/custom-import/search", headers=BrokenHeaders())
    context = SimpleNamespace(sa_session=ordinary, unrelated="retained")
    if existing_context:
        request.ctx = context
    failure = failure_type("synthetic payload failure")
    with pytest.raises(failure_type) as caught:
        async with boundary.signed_request_reader(request, ordinary, verified_request_id=REQUEST_ID) as reader:
            assert request.ctx.sa_session is reader and request.ctx.session is reader
            raise failure
    assert caught.value is failure and not hasattr(failure, "_api_reader_diagnostic")
    assert session.closed and session.rolled_back and not session.info
    assert not database.has_reader_session() and not ordinary.in_transaction()
    if existing_context:
        assert request.ctx is context and vars(context) == {"sa_session": ordinary, "unrelated": "retained"}
    else:
        assert not hasattr(request, "ctx")


@pytest.mark.parametrize(
    "verified,forwarded,expected,source",
    [
        ("x" * 36, REQUEST_ID, REQUEST_ID, "forwarded"),
        ("x" * 36, "y" * 36, CORE_ID, "core"),
        (
            "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA",
            REQUEST_ID,
            "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
            "verified_transport",
        ),
    ],
)
def test_invalid_correlation_ids_cannot_displace_valid_ids(caplog, verified, forwarded, expected, source):
    request = SimpleNamespace(headers={"x-request-id": forwarded}, id=UUID(CORE_ID))
    failure = RuntimeError("credential-sentinel")
    boundary._remember_reader_failure(
        failure, request, None, None, boundary="signed_read", verified_request_id=verified
    )
    fields, _ = _logged(failure, caplog)
    assert fields["request_id"] == expected and fields["request_id_source"] == source
    assert fields["core_request_id"] == CORE_ID and fields["reader"] is None
    assert fields["forwarded_request_id"] == (REQUEST_ID if forwarded == REQUEST_ID else None)


def test_unknown_diagnostic_boundary_does_not_read_request_or_modify_error():
    class UnreadableRequest:
        @property
        def headers(self):
            pytest.fail("unsupported boundary inspected request")

    failure = RuntimeError()
    boundary._remember_reader_failure(failure, UnreadableRequest(), None, None, boundary="unknown")
    assert not hasattr(failure, "_api_reader_diagnostic")


async def test_reader_marker_without_factory_ownership_is_rejected_and_closed(monkeypatch, caplog):
    reader = ReaderSession()
    reader.info["api_reader_verified"] = True
    closed_readers = []

    @asynccontextmanager
    async def unowned_reader(**options):
        assert options == {"independent": True, "prepare_plan_workspace": True}
        try:
            yield reader
        finally:
            closed_readers.append(reader)

    database = install_reader_factory(monkeypatch)
    monkeypatch.setattr(database, "reader_session", unowned_reader)
    ordinary = cold_caller()
    context = SimpleNamespace(sa_session=ordinary, session=ordinary)
    request = SimpleNamespace(ctx=context, headers={"X-Request-ID": CORE_ID})
    with pytest.raises(boundary.CustomImportReadUnavailableError, match="ownership is unavailable") as caught:
        async with boundary.signed_request_reader(
            request, ordinary, plan_workspace=True, verified_request_id=REQUEST_ID
        ):
            pytest.fail("unowned Reader reached payload")
    assert closed_readers == [reader] and request.ctx is context
    assert context.sa_session is ordinary and context.session is ordinary
    fields, _ = _logged(caught.value, caplog)
    assert fields["reader"] is None and fields["request_id"] == REQUEST_ID
    assert fields["request_id_source"] == "verified_transport"


async def test_target_diagnostic_formatter_failure_keeps_closed_redacted_error(monkeypatch, caplog):
    session = SimpleNamespace(execute=AsyncMock(side_effect=_database_failure()))

    def unavailable_diagnostic(_failure):
        raise ValueError("credential-sentinel")

    with monkeypatch.context() as diagnostic_patch:
        diagnostic_patch.setattr(transport, "database_sqlstate", unavailable_diagnostic)
        with pytest.raises(transport.CustomImportReadUnavailableError) as caught:
            await transport._resolve_pinned_target(session, SimpleNamespace(dataset_key="synthetic"))
    session.execute.assert_awaited_once()
    failure = caught.value
    assert str(failure) == "pinned dataset is unavailable"
    assert failure.__cause__ is None and failure.__suppress_context__ is True
    assert failure.sqlstate is None and failure._read_caught_class == "unknown"
    fields, _ = _logged(failure, caplog)
    assert fields["exception_class"] == "CustomImportReadUnavailableError"
    assert fields["caught_exception_class"] == "unknown"
