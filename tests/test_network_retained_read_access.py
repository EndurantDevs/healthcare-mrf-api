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
    def __init__(self, kept=()):
        self.statements = []
        self.ledger = []
        self.finished = []
        self.kept = list(kept)

    def is_in_transaction(self):
        return True

    async def fetchval(self, sql, *arguments):
        if sql == "SHOW transaction_isolation":
            return "repeatable read"
        assert "unnest(relation_names)" in sql and arguments == ("reader",)
        return self.kept

    async def execute(self, sql, *arguments):
        # Parameterized statements are ledger writes; the rest are native DCL and role changes.
        if arguments:
            self.ledger.append((sql, arguments))
        else:
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
    change_receipt = await operation(connection, PIN, reader_role="reader", control_schema="control")
    grants.resolve.assert_awaited_once_with(connection, PIN, control_schema="control")
    assert [(call.args[1:]) for call in grants.operator.await_args_list] == [
        ("retained_base", ["entity_address_unified"]),
        (SCHEMA, ["network_membership", "provider_location_binding"]),
    ]
    assert change_receipt == {"schema_name": SCHEMA, "manifest_sha256": MANIFEST, "reader_role": "reader"}
    assert connection.finished == ["commit"]
    relation_names = [
        "retained_base.entity_address_unified",
        f"{SCHEMA}.network_membership",
        f"{SCHEMA}.provider_location_binding",
    ]
    assert connection.ledger[0] == (
        'INSERT INTO "control"."network_retained_read_grant_guard" (reader_role) VALUES ($1) '
        "ON CONFLICT (reader_role) DO UPDATE SET reader_role=EXCLUDED.reader_role",
        ("reader",),
    )
    ledger_key = ("reader", SCHEMA, MANIFEST)
    if revoke:
        grants.grant.assert_not_awaited()
        assert connection.ledger[1][0].startswith('DELETE FROM "control"."network_retained_read_grant"')
        assert connection.ledger[1][1] == ledger_key
        assert connection.statements == [
            'REVOKE SELECT ON "retained_base"."entity_address_unified" FROM "reader"',
            'REVOKE USAGE ON SCHEMA "retained_base" FROM "reader"',
            'SET LOCAL ROLE "administrator"',
            f'REVOKE SELECT ON "{SCHEMA}"."network_membership" FROM "reader"',
            f'REVOKE SELECT ON "{SCHEMA}"."provider_location_binding" FROM "reader"',
            f'REVOKE USAGE ON SCHEMA "{SCHEMA}" FROM "reader"',
            'SET LOCAL ROLE "administrator"',
        ]
    else:
        assert [(call.args[1:], call.kwargs) for call in grants.grant.await_args_list] == [
            (("retained_base", ["entity_address_unified"], "reader"), {}),
            ((SCHEMA, ["network_membership", "provider_location_binding"], "reader"), {}),
        ]
        assert connection.ledger[1][0].startswith('INSERT INTO "control"."network_retained_read_grant"')
        assert connection.ledger[1][1] == (*ledger_key, relation_names)
        assert connection.statements == ['SET LOCAL ROLE "administrator"'] * 2


async def test_revoke_keeps_a_shared_base_that_another_recorded_generation_names(grants):
    connection = _Connection(kept=["retained_base.entity_address_unified", "other_generation.network_membership"])
    await retained.revoke_retained_read_access(connection, PIN, reader_role="reader", control_schema="control")
    assert connection.statements == [
        'SET LOCAL ROLE "administrator"',
        f'REVOKE SELECT ON "{SCHEMA}"."network_membership" FROM "reader"',
        f'REVOKE SELECT ON "{SCHEMA}"."provider_location_binding" FROM "reader"',
        f'REVOKE USAGE ON SCHEMA "{SCHEMA}" FROM "reader"',
        'SET LOCAL ROLE "administrator"',
    ]


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


@pytest.mark.parametrize("revoke", [False, True])
async def test_reader_guard_conflict_reaches_the_outer_transaction_before_any_grants(grants, revoke):
    conflict = asyncpg.SerializationError("concurrent reader grant changed")
    connection = _Connection()
    connection.execute = AsyncMock(side_effect=conflict)
    operation = retained.revoke_retained_read_access if revoke else retained.install_retained_read_access
    with pytest.raises(asyncpg.SerializationError) as raised:
        await operation(connection, PIN, reader_role="reader", control_schema="control")
    assert raised.value is conflict and raised.value.sqlstate == "40001"
    assert connection.finished == ["rollback"]
    assert connection.execute.await_count == 1
    assert connection.execute.await_args.args[0].startswith('INSERT INTO "control"."network_retained_read_grant_guard"')
    assert connection.execute.await_args.args[1:] == ("reader",)
    grants.operator.assert_not_awaited()
    grants.grant.assert_not_awaited()
