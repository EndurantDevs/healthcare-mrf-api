# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Offline statement contracts; native retained storage is tested separately."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.registry_approval_store import RegistryApprovalConflict
from process.registry_approved_company_read import read_registry_approved_companies

ID = "11111111-1111-4111-8111-111111111111"


def test_read_uses_only_retained_current_map_and_has_more():
    connection = SimpleNamespace(
        fetchval=AsyncMock(return_value=3),
        fetch=AsyncMock(
            return_value=[
                {"company_id": ID, "display_name": "Approved"},
                {
                    "company_id": "22222222-2222-4222-8222-222222222222",
                    "display_name": "Next",
                },
            ]
        ),
    )
    result = asyncio.run(
        read_registry_approved_companies(connection, limit=1, approved_revision=3, schema="synthetic_registry")
    )
    assert result["items"] == [{"company_id": ID, "display_name": "Approved"}] and result["has_more"]
    statement, *args = connection.fetch.await_args.args
    assert "registry_approved_record" in statement and "registry_company" not in statement
    assert "record_json->'archived'='false'::jsonb" in statement and args == [3, 2, 0]


def test_stale_page_refuses_before_record_read():
    connection = SimpleNamespace(
        fetchval=AsyncMock(return_value=4),
        fetch=AsyncMock(side_effect=AssertionError("stale record read")),
    )
    with pytest.raises(RegistryApprovalConflict):
        asyncio.run(read_registry_approved_companies(connection, approved_revision=3))


@pytest.mark.parametrize(
    "query",
    [
        "limit=0",
        "limit=101",
        "offset=1",
        "approved_revision=9007199254740992",
        "limit=1&limit=2",
        "actor=admin",
    ],
)
def test_core_invalid_page_precedes_database(monkeypatch, query):
    from api.endpoint import registry_publication as core_routes

    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "synthetic-service")

    class DatabaseMustStayClosed:
        headers = {"Authorization": "Bearer synthetic-service"}
        body = b""
        query_string = query

        @property
        def ctx(self):
            raise AssertionError("invalid page opened a transaction")

    assert asyncio.run(core_routes.approved_companies(DatabaseMustStayClosed())).status == 400


def test_core_service_auth_precedes_database(monkeypatch):
    from sanic.exceptions import Forbidden

    from api.endpoint import registry_publication as core_routes

    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "synthetic-service")
    with pytest.raises(Forbidden):
        asyncio.run(core_routes.approved_companies(SimpleNamespace(headers={})))


def test_core_page_limit_matches_bounded_company_choices():
    connection = SimpleNamespace(fetchval=AsyncMock(return_value=3), fetch=AsyncMock(return_value=[]))
    result = asyncio.run(read_registry_approved_companies(connection, limit=40))
    assert result["limit"] == 40 and connection.fetch.await_args.args[2] == 41
    for limit in (41, 100):
        with pytest.raises(ValueError):
            asyncio.run(read_registry_approved_companies(connection, limit=limit))
    assert connection.fetchval.await_count == 1


def test_core_endpoint_refuses_limit_plus_one_before_database(monkeypatch):
    from api.endpoint import registry_publication as core_routes

    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "synthetic-service")

    class DatabaseMustStayClosed:
        headers = {"Authorization": "Bearer synthetic-service"}
        body = b""
        query_string = "limit=41"

        @property
        def ctx(self):
            raise AssertionError("invalid page opened a transaction")

    assert asyncio.run(core_routes.approved_companies(DatabaseMustStayClosed())).status == 400


def test_core_endpoint_accepts_exact_limit_with_pinned_read(monkeypatch):
    from api.endpoint import registry_publication as core_routes

    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "synthetic-service")
    driver = SimpleNamespace(fetchval=AsyncMock(return_value=3), fetch=AsyncMock(return_value=[]))
    monkeypatch.setattr(core_routes, "_driver", AsyncMock(return_value=driver))

    class Transaction:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

    session = SimpleNamespace(begin=Transaction, execute=AsyncMock())
    request = SimpleNamespace(
        headers={"Authorization": "Bearer synthetic-service"},
        body=b"",
        query_string="limit=40&approved_revision=3",
        ctx=SimpleNamespace(sa_session=session),
    )
    result = asyncio.run(core_routes.approved_companies(request))
    assert result.status == 200 and driver.fetch.await_args.args[2] == 41
    assert "REPEATABLE READ READ ONLY" in str(session.execute.await_args.args[0])
