# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Candidate custody releases only the exact temporary ownership it acquired."""

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from db.registry_aca_source_notification import INSERT_COLUMNS, TABLE
from tests import registry_candidate_custody_postgres_support as custody


class CustodyConnection:
    """Track native ownership changes without opening a PostgreSQL connection."""

    def __init__(self):
        self.owner_by_relation = {
            "network_serving_control": "migration",
            "network_membership_candidate": "migration",
            "network_candidate_custody": "migration",
            TABLE: "migration",
        }
        self.oid_by_role = {"migration": 7, "control_owner": 8, "custody_owner": 9}
        self.state = SimpleNamespace(schema_owner="migration")
        self.execute = AsyncMock(side_effect=self._execute)

    @asynccontextmanager
    async def transaction(self):
        yield

    async def _execute(self, statement):
        if statement.startswith('ALTER SCHEMA "control" OWNER TO '):
            self.state.schema_owner = statement.rsplit('"', 2)[1]
        for name in self.owner_by_relation:
            if statement.startswith(f'ALTER TABLE "control"."{name}" OWNER TO '):
                self.owner_by_relation[name] = statement.rsplit('"', 2)[1]

    async def fetchrow(self, statement, *parameters):
        if "h.relowner AS head_owner" in statement:
            return dict(
                oid=30,
                relname=TABLE,
                relowner=self.oid_by_role[self.owner_by_relation[TABLE]],
                head_owner=self.oid_by_role[self.owner_by_relation["network_serving_control"]],
                relkind="r",
                relpersistence="p",
                columns=(*INSERT_COLUMNS, "created_at"),
                relhastriggers=False,
                relhasrules=False,
                relrowsecurity=False,
                relforcerowsecurity=False,
            )
        if "FROM pg_namespace" in statement:
            return dict(oid=10, rolname=self.state.schema_owner)
        for name, oid in ((TABLE, 30), ("network_candidate_custody", 40), ("network_membership_candidate", 50)):
            if parameters == (f'"control"."{name}"',):
                return dict(oid=oid, rolname=self.owner_by_relation[name])
        raise AssertionError(parameters)

    async def fetchval(self, statement, *parameters):
        if "to_regnamespace" in statement:
            return 10
        if "current_database" in statement:
            return "fixture_database"
        assert parameters == (7,)
        return "migration"


@pytest.mark.asyncio
async def test_fixture_transfers_aca_inbox_with_head_and_restores_original_custody(monkeypatch):
    """Transfer the accepted inbox with its head, then retain its original owner and OID."""
    connection = CustodyConnection()
    fixture = SimpleNamespace(
        connection=connection,
        control_schema="control",
        roles={"owner": "control_owner", "reader": "reader"},
        custody_roles={
            "loading": "loading",
            "owner": "custody_owner",
            "execution": "execution",
            "custodian": "custodian",
        },
    )
    tables = [
        dict(oid=20, relname="network_serving_control"),
        dict(oid=40, relname="network_candidate_custody", relowner=7),
        dict(oid=50, relname="network_membership_candidate", relowner=7),
    ]
    monkeypatch.setattr(custody, "_catalog", AsyncMock(return_value=(tables, [])))
    grant = AsyncMock()
    monkeypatch.setattr(custody, "_grant_worker", grant)
    original_by_field = dict(oid=10, owner_oid=7, rolname="migration")

    await custody._install_grants(fixture, original_by_field)
    assert connection.owner_by_relation == {
        "network_serving_control": "control_owner",
        "network_membership_candidate": "control_owner",
        "network_candidate_custody": "custody_owner",
        TABLE: "control_owner",
    }
    assert (await custody._aca_controls(connection, "control"))[0]["oid"] == 30
    assert grant.await_args.args[2] == tables
    await custody._restore_control_owner(fixture, original_by_field)
    assert connection.state.schema_owner == connection.owner_by_relation[TABLE] == "migration"
    assert (await connection.fetchrow("restored inbox", f'"control"."{TABLE}"'))["oid"] == 30
    assert await connection.fetchrow("restored ledger", '"control"."network_candidate_custody"') == {
        "oid": 40,
        "rolname": "migration",
    }
    assert await connection.fetchrow("restored candidate", '"control"."network_membership_candidate"') == {
        "oid": 50,
        "rolname": "migration",
    }
    assert not any("DROP" in call.args[0] for call in connection.execute.await_args_list)
