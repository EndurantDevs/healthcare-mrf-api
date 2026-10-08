# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Required-proof policy distinguishes CMS NPD serving from legacy CMS education."""

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sanic.exceptions import ServiceUnavailable
from sqlalchemy.exc import DBAPIError

from api import provider_profile_snapshot as snapshot


@pytest.mark.parametrize(
    "has_history_table,has_profile_table,answers,expected",
    [
        (False, False, [], False),
        (False, True, [False], False),
        (False, True, [True], True),
        (True, True, [False, False], False),
        (True, True, [False, True], True),
        (True, False, [True], True),
        (True, True, [True], True),
    ],
)
async def test_required_proof_survives_profile_withdrawal(has_history_table, has_profile_table, answers, expected):
    session = SimpleNamespace(scalar=AsyncMock(side_effect=answers))
    oid_by_relation = {
        "mrf.provider_directory_cms_serving_receipt": 1 if has_history_table else None,
        "mrf.provider_directory_profile_serving_generation": 2 if has_profile_table else None,
    }
    assert await snapshot._requires_cms_receipt(session, "mrf", oid_by_relation) is expected
    assert session.scalar.await_count == len(answers)


@pytest.mark.parametrize("has_receipt_table", [False, True])
async def test_missing_common_proof_is_unavailable(monkeypatch, has_receipt_table):
    read_receipt = AsyncMock(return_value=None)
    monkeypatch.setattr(snapshot.cms_serving_receipt, "read_serving_receipt", read_receipt)
    oid_by_relation = {"mrf.provider_directory_cms_serving_receipt": 1 if has_receipt_table else None}
    with pytest.raises(ServiceUnavailable, match="temporarily unavailable"):
        await snapshot._read_cms_receipt(SimpleNamespace(), "mrf", oid_by_relation)
    assert read_receipt.await_count == int(has_receipt_table)


@pytest.mark.parametrize(
    "borrowed,phase,sqlstate,failures,expected_attempts,unavailable",
    [
        (False, "setup", "55P03", 1, 2, False),
        (False, "setup", "55P03", 3, 3, True),
        (True, "setup", "55P03", 1, 1, True),
        (False, "setup", "57014", 1, 1, True),
        (False, "loader", "55P03", 1, 1, True),
        (False, "loader", "57014", 1, 1, True),
    ],
)
async def test_snapshot_lock_retry_owns_only_unyielded_sessions(
    monkeypatch, borrowed, phase, sqlstate, failures, expected_attempts, unavailable
):
    sessions, closed_sessions, loader_calls = [], [], []
    error = DBAPIError(None, None, SimpleNamespace(sqlstate=sqlstate))

    @asynccontextmanager
    async def reader_session():
        session = object()
        sessions.append(session)
        try:
            yield session
        finally:
            closed_sessions.append(session)

    @asynccontextmanager
    async def scope(session, _schema, **_options):
        assert sessions[:-1] == closed_sessions
        if phase == "setup" and len(sessions) <= failures:
            raise error
        yield

    database = SimpleNamespace(
        has_reader_session=lambda: borrowed, _transaction_binding=lambda: None, reader_session=reader_session
    )
    monkeypatch.setattr(snapshot, "_read_snapshot_scope", scope)
    try:
        async with snapshot.provider_profile_read_snapshot(database, "synthetic") as session:
            loader_calls.append(session)
            if phase == "loader":
                raise error
    except ServiceUnavailable as caught:
        assert unavailable and caught.__cause__ is error
    else:
        assert not unavailable
    assert len(sessions) == expected_attempts and sessions == closed_sessions
    assert len(loader_calls) == (1 if phase == "loader" or not unavailable else 0)
