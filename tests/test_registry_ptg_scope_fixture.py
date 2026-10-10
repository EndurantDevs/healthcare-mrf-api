# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Check actual fixture migration wiring without opening a database connection."""

import os

import pytest
from sqlalchemy import create_mock_engine

from tests import test_registry_ptg_scope_engine_postgres as fixture


class MigrationStatements:
    def __init__(self):
        self.statements = []
        self.connection = create_mock_engine("postgresql://", self.collect)
        self.connection.scalar = self.catalog_scalar

    def collect(self, statement, *_args, **_kwargs):
        self.statements.append(str(statement.compile(dialect=self.connection.dialect)))

    def catalog_scalar(self, statement, parameters):
        assert "canonical_network_ids" in str(statement)
        assert parameters == {"relation": '"scope_native_offline".entity_address_unified'}
        self.collect(statement)
        return None

    async def scalar(self, *_args, **_kwargs):
        return None

    async def exec_driver_sql(self, statement):
        self.statements.append(statement)

    async def run_sync(self, callback):
        return callback(self.connection)


@pytest.mark.asyncio
async def test_fixture_loads_company_assertions():
    connection = MigrationStatements()
    schema = "scope_native_offline"
    roles = ("scope_offline_owner", "scope_offline_approver", "scope_offline_reader")
    prior_by_setting = {key: os.environ.get(key) for key in ("HLTHPRT_DB_SCHEMA", "HLTHPRT_NETWORK_REGISTRY_SCHEMA")}
    await fixture._create_control_store(connection, schema, roles, {})
    statements = connection.statements
    assert any(
        statement.startswith('CREATE TABLE "scope_native_offline".company_registry_role_assertion')
        for statement in statements
    )
    assertion_ddl = fixture._migration("20261009020000_company_registry_assertions.py")._ddl(schema)
    first_assertion = statements.index(assertion_ddl[0])
    assert statements[first_assertion : first_assertion + len(assertion_ddl)] == assertion_ddl
    assert statements.index(fixture._migration()._ddl(schema)[0]) < first_assertion
    assert all("CREATE FUNCTION" not in statement and "CREATE TRIGGER" not in statement for statement in statements)
    assert (
        'GRANT SELECT ON "scope_native_offline"."registry_ptg_producer_scope" TO "scope_offline_reader"' in statements
    )
    assert not any("GRANT" in statement and "company_registry_role_assertion" in statement for statement in statements)
    assert {key: os.environ.get(key) for key in prior_by_setting} == prior_by_setting
