# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Administrative name-based grants and their transactional failure behavior."""

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import asyncpg
import pytest

from process import registry_management_permissions as management
from process import registry_publication_permissions as policy
from process import registry_publication_worker_permissions as workers


def _tables():
    return [
        {"relname": name, "relkind": "r", "relpersistence": "p", "columns": list(columns)}
        for name, columns in sorted(management._TABLES.items())
    ]


class PermissionConnection:
    """Record real installer SQL while returning only the fixed named metadata shape."""

    def __init__(self, failure=None):
        self.statements = []
        self.transactions = []
        self.failure = failure

    def is_in_transaction(self):
        return True

    @asynccontextmanager
    async def transaction(self):
        self.transactions.append("begin")
        try:
            yield
        except BaseException:
            self.transactions.append("rollback")
            raise
        else:
            self.transactions.append("commit")

    async def execute(self, sql, *arguments):
        self.statements.append((sql, arguments))
        if self.failure is not None:
            self.failure(sql)

    async def fetch(self, sql, *arguments):
        self.statements.append((sql, arguments))
        if "AS columns" in sql:
            return _tables()
        return [{"relname": name} for name in sorted(management._SEQUENCES)]

    async def fetchrow(self, sql, *arguments):
        self.statements.append((sql, arguments))
        return None

    async def fetchval(self, sql, *arguments):
        self.statements.append((sql, arguments))
        if "tableowner" in sql:
            return "ledger_owner"
        if "current_database" in sql:
            return "example_database"
        assert sql == "SELECT quote_ident(current_user)"
        return '"administrator"'


def _assert_no_custody_queries(statements):
    sql = "\n".join(statement for statement, _arguments in statements)
    for forbidden in (
        "pg_has_role",
        "pg_control_system",
        "has_table_privilege",
        "has_column_privilege",
        "has_schema_privilege",
        "database_oid",
        "schema_oid",
        "owner_oid",
        "pg_auth_members",
    ):
        assert forbidden not in sql


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", [None, "missing", "duplicate", "wrong-key", "columns", "sequence"])
async def test_metadata_catalog_requires_complete_keyed_named_shapes(fault):
    tables = _tables()
    sequences = [{"relname": name} for name in sorted(management._SEQUENCES)]
    match fault:
        case "missing":
            tables.pop()
        case "duplicate":
            tables[-1] = tables[0]
        case "wrong-key":
            tables[-1] = {**tables[-1], "relname": "another_relation"}
        case "columns":
            tables[-1]["columns"] = []
        case "sequence":
            sequences[-1] = {"relname": "another_sequence"}
    connection = SimpleNamespace(fetch=AsyncMock(side_effect=[tables, sequences]))
    if fault:
        with pytest.raises(management.RegistryManagementPermissionError, match="incomplete"):
            await management._catalog(connection, "example_registry")
    else:
        actual_tables, actual_sequences = await management._catalog(connection, "example_registry")
        assert actual_tables == [{"relname": row["relname"], "columns": row["columns"]} for row in tables]
        assert actual_sequences == sequences
    for invocation in connection.fetch.await_args_list:
        assert invocation.args[1] == "example_registry"
    _assert_no_custody_queries(
        [(invocation.args[0], invocation.args[1:]) for invocation in connection.fetch.await_args_list]
    )


@pytest.mark.asyncio
async def test_management_installer_keeps_precise_draft_grants_and_named_receipt():
    connection = PermissionConnection()
    receipt = await management.install_registry_management_permissions(
        connection, api_role="draft_api", owner_role="metadata_owner", control_schema="example_registry"
    )
    assert receipt["tables"] == sorted(management._TABLES)
    assert receipt["sequences"] == sorted(management._SEQUENCES)
    assert not any(name.endswith("oid") for name in receipt)
    statements = [statement for statement, _arguments in connection.statements]
    assert 'ALTER TABLE "example_registry"."network_candidate_custody" OWNER TO "metadata_owner"' in statements
    assert 'REVOKE "metadata_owner" FROM "draft_api"' in statements
    assert 'REVOKE CREATE ON SCHEMA "example_registry" FROM PUBLIC' in statements
    owner_switch = statements.index('SET LOCAL ROLE "metadata_owner"')
    assert 'GRANT USAGE ON SCHEMA "example_registry" TO "draft_api","metadata_owner"' in statements
    for statement in statements:
        if ' ON SCHEMA "example_registry" ' in statement:
            assert statements.index(statement) < owner_switch
    assert (
        'REVOKE ALL ON TABLE "example_registry"."registry_publication_request" FROM PUBLIC,"draft_api" CASCADE'
        in statements
    )
    assert (
        'GRANT INSERT("request_id","actor_key","session_token_sha256","actor_json","command_json",'
        '"idempotency_key","request_sha256") ON "example_registry"."registry_publication_request" TO "draft_api"'
    ) in statements
    assert not any(
        statement.startswith("GRANT UPDATE") and "registry_publication_request" in statement for statement in statements
    )
    assert statements[-1] == 'SET LOCAL ROLE "administrator"'
    assert connection.transactions == ["begin", "commit"]
    _assert_no_custody_queries(connection.statements)


@pytest.mark.asyncio
async def test_office_recipe_installer_changes_only_its_two_column_grants():
    connection = PermissionConnection()
    await management.install_registry_office_recipe_permissions(
        connection, api_role="draft_api", owner_role="metadata_owner", control_schema="example_registry"
    )
    grants = [sql for sql, _arguments in connection.statements if sql.startswith("GRANT")]
    assert grants == [
        "GRANT INSERT(office_recipe_json),UPDATE(office_recipe_json) ON "
        '"example_registry".registry_network_binding TO "draft_api"'
    ]
    assert connection.transactions == ["begin", "commit"]
    _assert_no_custody_queries(connection.statements)


@pytest.mark.asyncio
async def test_worker_installer_grants_named_preparation_control_only_to_execution():
    connection = PermissionConnection()
    receipt = await workers.install_registry_publication_worker_permissions(
        connection,
        lifecycle_role="lifecycle_app",
        execution_role="execution_app",
        api_role="draft_api",
        queue_owner_role="metadata_owner",
        physical_owner_role="candidate_builder",
        control_schema="example_registry",
    )
    statements = [statement for statement, _arguments in connection.statements]
    owner_switch = statements.index('SET LOCAL ROLE "metadata_owner"')
    assert 'GRANT USAGE ON SCHEMA "example_registry" TO "metadata_owner"' in statements
    for statement in statements:
        if ' ON SCHEMA "example_registry" ' in statement:
            assert statements.index(statement) < owner_switch
    assert receipt["control_schema"] == "example_registry"
    assert 'GRANT "candidate_builder" TO "execution_app" WITH ADMIN FALSE, INHERIT TRUE, SET TRUE' in statements
    grants = [
        sql
        for sql in statements
        if sql.startswith("GRANT SELECT,INSERT(")
        and sql.endswith('ON hp_snapshot_retention.preparation TO "execution_app"')
    ]
    assert len(grants) == 1 and "frozen_inventory" not in grants[0] and "frozen_sha256" not in grants[0]
    assert (
        "GRANT SELECT,INSERT ON hp_snapshot_retention.preparation_claim,"
        'hp_snapshot_retention.preparation_relation TO "execution_app"'
    ) in statements
    assert not any("hp_snapshot_retention" in statement and '"lifecycle_app"' in statement for statement in statements)
    assert (
        'GRANT INSERT("candidate_id","batch_id","row_count","input_sha256","copy_sha256",'
        '"input_bytes","copy_bytes") ON "example_registry"."network_membership_batch" TO "execution_app"'
    ) in statements
    assert not any(
        "network_membership_batch" in statement and 'TO "lifecycle_app"' in statement for statement in statements
    )
    queue_updates = [
        statement
        for statement in statements
        if statement.startswith("GRANT UPDATE") and "registry_publication_request" in statement
    ]
    assert len(queue_updates) == 2
    assert all('"command_json"' not in statement for statement in queue_updates)
    assert any(
        'UPDATE("accepted_rows","state","validation_json","index_ready")' in statement
        and 'TO "execution_app"' in statement
        for statement in statements
    )
    assert connection.transactions == ["begin", "commit"]
    _assert_no_custody_queries(connection.statements)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [RuntimeError("grant failed"), asyncio.CancelledError()])
async def test_administrative_role_restoration_preserves_original_interruption(failure):
    connection = SimpleNamespace(
        fetchval=AsyncMock(return_value='"administrator"'),
        execute=AsyncMock(side_effect=[None, RuntimeError("restore failed")]),
    )
    with pytest.raises(type(failure)) as caught:
        async with management._administrative_role(connection, "metadata_owner"):
            raise failure
    assert caught.value is failure
    assert "restoration" in failure.__notes__[-1]
    assert connection.execute.await_args.args == ('SET LOCAL ROLE "administrator"',)


@pytest.mark.asyncio
@pytest.mark.parametrize("installer", ["management", "worker"])
async def test_late_grant_error_rolls_back_without_permission_retry(installer):
    failure = asyncpg.InsufficientPrivilegeError("not permitted")

    def reject(sql):
        if sql.startswith("GRANT"):
            raise failure

    connection = PermissionConnection(reject)
    if installer == "management":
        with pytest.raises(asyncpg.InsufficientPrivilegeError) as caught:
            await management.install_registry_management_permissions(
                connection, api_role="draft_api", owner_role="metadata_owner", control_schema="example_registry"
            )
        assert caught.value is failure
    else:
        with pytest.raises(workers.RegistryPublicationWorkerPermissionError, match="refused"):
            await workers.install_registry_publication_worker_permissions(
                connection,
                lifecycle_role="lifecycle_app",
                execution_role="execution_app",
                api_role="draft_api",
                queue_owner_role="metadata_owner",
                physical_owner_role="candidate_builder",
                control_schema="example_registry",
            )
    assert connection.transactions == ["begin", "rollback"]


def test_queue_column_policy_retains_immutable_request_fields():
    assert policy.LIFECYCLE_COLUMNS == {"state", "lease_id", "lease_expires_at", "result_json", "updated_at"}
    assert policy.INSERT_COLUMNS == {
        "request_id",
        "actor_key",
        "session_token_sha256",
        "actor_json",
        "command_json",
        "idempotency_key",
        "request_sha256",
    }
    assert not policy.LIFECYCLE_COLUMNS & policy.INSERT_COLUMNS
