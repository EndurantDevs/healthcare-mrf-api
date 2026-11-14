# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Named retained grant orchestration; PostgreSQL enforces the actual privileges."""

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import asyncpg
import pytest

from process import network_retained_read_access as retained
from process import network_serving_read as serving
from process.network_address_projection import PinnedAddressSource
from process.network_compact_layout import CompactAddressRelation

pytestmark = pytest.mark.asyncio
SCHEMA = "network_candidate_" + "a" * 32
MANIFEST = "b" * 64
PIN = PinnedAddressSource(SCHEMA, "entity_address_unified", MANIFEST)


class _Connection:
    def __init__(self):
        self.statements = []
        self.finished = []

    def is_in_transaction(self):
        return True

    async def fetchval(self, sql, *arguments):
        assert sql == "SHOW transaction_isolation"
        return "repeatable read"

    async def execute(self, sql):
        self.statements.append(sql)

    @asynccontextmanager
    async def transaction(self):
        try:
            yield
        except BaseException:
            self.finished.append("rollback")
            raise
        else:
            self.finished.append("commit")


@pytest.fixture
def grants(monkeypatch):
    relations = (
        CompactAddressRelation(SCHEMA, "network_membership"),
        CompactAddressRelation(SCHEMA, "provider_location_binding"),
        CompactAddressRelation("retained_base", "entity_address_unified"),
    )
    manifest = SimpleNamespace(
        schema_name=SCHEMA,
        manifest_sha256=MANIFEST,
        address_relation=relations[-1],
        candidate_relation_pins=relations,
    )
    resolve = AsyncMock(return_value=manifest)
    operator = AsyncMock(return_value='"administrator"')
    grant = AsyncMock()
    monkeypatch.setattr(serving, "resolve_retained_network_address_source", resolve)
    monkeypatch.setattr(retained, "_operator", operator)
    monkeypatch.setattr(retained, "_grant", grant)
    return SimpleNamespace(resolve=resolve, operator=operator, grant=grant)


@pytest.mark.parametrize("revoke", [False, True])
async def test_named_grant_uses_the_exact_pinned_query_relations(grants, revoke):
    connection = _Connection()
    operation = retained.revoke_retained_read_access if revoke else retained.install_retained_read_access
    result = await operation(connection, PIN, reader_role="reader", control_schema="control")
    grants.resolve.assert_awaited_once_with(connection, PIN, control_schema="control")
    assert [(call.args[1:]) for call in grants.operator.await_args_list] == [
        ("retained_base", ["entity_address_unified"]),
        (SCHEMA, ["network_membership", "provider_location_binding"]),
    ]
    assert [(call.args[1:], call.kwargs) for call in grants.grant.await_args_list] == [
        (("retained_base", ["entity_address_unified"], "reader"), {"revoke": revoke}),
        ((SCHEMA, ["network_membership", "provider_location_binding"], "reader"), {"revoke": revoke}),
    ]
    assert result == {"schema_name": SCHEMA, "manifest_sha256": MANIFEST, "reader_role": "reader"}
    assert connection.finished == ["commit"]
    assert connection.statements == ['SET LOCAL ROLE "administrator"'] * 2


async def test_unknown_generation_is_refused_before_any_grant(grants):
    grants.resolve.side_effect = serving.NetworkServingReadUnavailable("missing generation")
    connection = _Connection()
    with pytest.raises(serving.NetworkServingReadUnavailable, match="missing generation"):
        await retained.install_retained_read_access(connection, PIN, reader_role="reader", control_schema="control")
    grants.operator.assert_not_awaited()
    grants.grant.assert_not_awaited()
    assert connection.finished == ["rollback"]


@pytest.mark.parametrize(
    "error", [RuntimeError("grant failed"), asyncio.CancelledError(), asyncpg.InsufficientPrivilegeError()]
)
async def test_grant_failure_or_cancel_rolls_back_the_whole_change(grants, error):
    grants.grant.side_effect = error
    connection = _Connection()
    expected = retained.RetainedReadAccessError if isinstance(error, asyncpg.PostgresError) else type(error)
    with pytest.raises(expected):
        await retained.install_retained_read_access(connection, PIN, reader_role="reader", control_schema="control")
    assert connection.finished == ["rollback"]
    assert connection.statements == []
    assert grants.grant.await_count == 1


async def test_invalid_reader_never_resolves_or_grants(grants):
    with pytest.raises(ValueError):
        await retained.install_retained_read_access(
            _Connection(), PIN, reader_role="bad role", control_schema="control"
        )
    grants.resolve.assert_not_awaited()
    grants.grant.assert_not_awaited()
