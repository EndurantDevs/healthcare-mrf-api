# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Reject unknown storage dependencies and preserve narrow writer privileges."""

import uuid
from contextlib import asynccontextmanager
from io import StringIO
from types import SimpleNamespace

import asyncpg
import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine

from tests.formulary_fhir_twin_admission_pg_support import (
    connect,
    database_url,
    drop_schema,
    load_migration,
    quoted,
    run_migration,
)
from tests.test_provider_directory_uhc_flex_practitioner_acquisition_postgres import (
    VERSIONS,
    _prepare_schema,
)


def _migration():
    return load_migration(
        VERSIONS / "20261005120000_practitioner_set_validation.py", "practitioner_migration_boundaries"
    )


@pytest.mark.parametrize(
    ("revision", "refusal"),
    (
        ("20261005100000_rooted_graph_set_validation", "rooted_graph"),
        ("20261005120000_practitioner_set_validation", "practitioner"),
        ("20261005130000_provider_dataset_candidates", "provider_dataset"),
    ),
)
def test_snapshot_migrations_require_online_alembic_context(monkeypatch, revision, refusal):
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "synthetic_migration_scope")
    monkeypatch.setenv("DB_SCHEMA", "synthetic_migration_scope")
    output = StringIO()
    context = MigrationContext.configure(dialect_name="postgresql", opts={"as_sql": True, "output_buffer": output})
    migration = load_migration(VERSIONS / (revision + ".py"), "synthetic_offline_" + refusal)
    monkeypatch.setattr(migration, "op", Operations(context))
    with pytest.raises(RuntimeError, match=refusal + "_migration_requires_online_connection"):
        migration.upgrade()
    assert output.getvalue() == ""


@pytest.mark.asyncio
@pytest.mark.parametrize("dependency_kind", ("foreign_key", "view"))
async def test_practitioner_dependency_change_rejected(monkeypatch, dependency_kind):
    url = database_url()
    schema = f"fhir_twin_test_{uuid.uuid4().hex}"
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.setenv("DB_SCHEMA", schema)
    engine = create_async_engine(url.set(drivername="postgresql+asyncpg"))
    connection = await connect(url)
    resource = f"{quoted(schema)}.provider_directory_uhc_flex_practitioner_resource"
    try:
        await _prepare_schema(engine, url, schema, latest=False)
        old_oid = await connection.fetchval("SELECT $1::regclass::oid", resource)
        dependency = f"{quoted(schema)}.synthetic_dependency"
        if dependency_kind == "view":
            await connection.execute(f"CREATE VIEW {dependency} AS SELECT * FROM {resource}")
        else:
            await connection.execute(
                f"CREATE TABLE {dependency}(acquisition_id text,npi bigint,attempt integer,resource_id text, "
                f"FOREIGN KEY(acquisition_id,npi,attempt,resource_id) REFERENCES {resource})"
            )
        with pytest.raises(DBAPIError, match="practitioner_resource_dependencies_changed"):
            await run_migration(engine, _migration(), "upgrade")
        assert await connection.fetchval("SELECT $1::regclass::oid", resource) == old_oid
        assert await connection.fetchval("SELECT to_regclass($1)", resource + "_legacy") is None
    finally:
        await connection.close()
        await drop_schema(engine, schema)
        await engine.dispose()


@pytest.mark.asyncio
async def test_practitioner_column_grants_remain_narrow(monkeypatch):
    url = database_url()
    schema = f"fhir_twin_test_{uuid.uuid4().hex}"
    role = "practitioner_column_" + uuid.uuid4().hex
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.setenv("DB_SCHEMA", schema)
    engine = create_async_engine(url.set(drivername="postgresql+asyncpg"))
    connection = await connect(url)
    resource = f"{quoted(schema)}.provider_directory_uhc_flex_practitioner_resource"
    try:
        await _prepare_schema(engine, url, schema, latest=False)
        await connection.execute(f"CREATE ROLE {quoted(role)}")
        await connection.execute(f"GRANT USAGE ON SCHEMA {quoted(schema)} TO {quoted(role)}")
        await connection.execute(
            f"GRANT SELECT(payload_json_text),INSERT(payload_json_text) ON {resource} TO {quoted(role)}"
        )
        await run_migration(engine, _migration(), "upgrade")
        assert not await connection.fetchval("SELECT has_table_privilege($1,$2,'SELECT')", role, resource)
        assert await connection.fetchval(
            "SELECT has_column_privilege($1,$2,'payload_json_text','SELECT')", role, resource
        )
        assert not await connection.fetchval(
            "SELECT has_column_privilege($1,$2,'resource_id','SELECT')", role, resource
        )
        assert not await connection.fetchval(
            "SELECT has_column_privilege($1,$2,'payload_json_text','INSERT')", role, resource
        )
        assert not await connection.fetchval(
            "SELECT has_function_privilege($1,$2,'EXECUTE')",
            role,
            f"{quoted(schema)}.admit_pd_uhc_flex_practitioner_stage(text,text,bigint,integer,text)",
        )
    finally:
        await drop_schema(engine, schema)
        await connection.execute(f"DROP ROLE IF EXISTS {quoted(role)}")
        await connection.close()
        await engine.dispose()


@asynccontextmanager
async def _legacy_scope(monkeypatch):
    url = database_url()
    schema = f"fhir_twin_test_{uuid.uuid4().hex}"
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.setenv("DB_SCHEMA", schema)
    engine = create_async_engine(url.set(drivername="postgresql+asyncpg"))
    connection = await connect(url)
    try:
        await _prepare_schema(engine, url, schema, latest=False)
        yield SimpleNamespace(connection=connection, engine=engine, schema=quoted(schema), migration=_migration())
    finally:
        await connection.execute("RESET ROLE")
        await drop_schema(engine, schema)
        await connection.close()
        await engine.dispose()


@pytest.mark.asyncio
async def test_practitioner_default_function_grants_do_not_authorize_admission(monkeypatch):
    role = "practitioner_acl_" + uuid.uuid4().hex
    administrator = await connect(database_url())
    try:
        await administrator.execute(f"CREATE ROLE {quoted(role)}")
        async with _legacy_scope(monkeypatch) as context:
            await context.connection.execute(f"GRANT USAGE ON SCHEMA {context.schema} TO {quoted(role)}")
            await context.connection.execute(
                f"ALTER DEFAULT PRIVILEGES IN SCHEMA {context.schema} GRANT EXECUTE ON FUNCTIONS TO {quoted(role)}"
            )
            await context.connection.execute(
                f"ALTER DEFAULT PRIVILEGES IN SCHEMA {context.schema} GRANT SELECT,UPDATE ON TABLES TO {quoted(role)}"
            )
            await run_migration(context.engine, context.migration, "upgrade")
            for suffix in ("work", "resource"):
                relation = f"{context.schema}.provider_directory_uhc_flex_practitioner_{suffix}"
                for privilege in ("SELECT", "UPDATE"):
                    assert not await context.connection.fetchval(
                        "SELECT has_table_privilege($1,$2,$3)", role, relation, privilege
                    )
            signatures = (
                "prepare_pd_uhc_flex_practitioner_work(text,text)",
                "prepare_pd_uhc_flex_practitioner_stage()",
                "admit_pd_uhc_flex_practitioner_stage(text,text,bigint,integer,text)",
                "initialize_pd_uhc_flex_practitioner_work(text,text)",
            )
            for signature in signatures:
                assert not await context.connection.fetchval(
                    "SELECT has_function_privilege($1,$2,'EXECUTE')",
                    role,
                    f"{context.schema}.{signature}",
                )
            await context.connection.execute(f"SET ROLE {quoted(role)}")
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await context.connection.execute(f"SELECT {context.schema}.prepare_pd_uhc_flex_practitioner_stage()")
    finally:
        await administrator.execute(f"DROP ROLE IF EXISTS {quoted(role)}")
        await administrator.close()


async def _introduce_drift(context, drift):
    resource = f"{context.schema}.provider_directory_uhc_flex_practitioner_resource"
    if drift == "row_security":
        statement = f"ALTER TABLE {resource} ENABLE ROW LEVEL SECURITY"
    elif drift == "rule":
        statement = f"CREATE RULE synthetic_rule AS ON DELETE TO {resource} DO INSTEAD NOTHING"
    elif drift == "row_guard":
        statement = (
            f"CREATE TRIGGER synthetic_guard BEFORE INSERT ON {resource} FOR EACH ROW "
            f"EXECUTE FUNCTION {context.schema}.guard_pd_uhc_flex_practitioner_resource()"
        )
    else:
        statement = f"ALTER TABLE {resource} ADD CONSTRAINT synthetic_unvalidated CHECK(attempt>0) NOT VALID"
    await context.connection.execute(statement)


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", ("row_security", "rule", "row_guard", "constraint"))
async def test_practitioner_storage_drift_fails_before_committed_fences(monkeypatch, drift):
    async with _legacy_scope(monkeypatch) as context:
        await _introduce_drift(context, drift)
        with pytest.raises(DBAPIError, match="practitioner_storage_"):
            await run_migration(context.engine, context.migration, "upgrade")
        assert (
            await context.connection.fetchval(
                "SELECT to_regclass($1)",
                f"{context.schema}.pd_practitioner_storage_migration",
            )
            is None
        )
        assert not await context.connection.fetchval(
            "SELECT EXISTS(SELECT 1 FROM pg_trigger WHERE tgname='pd_practitioner_migration_fence' AND tgrelid=$1::regclass)",
            f"{context.schema}.provider_directory_uhc_flex_practitioner_resource",
        )
