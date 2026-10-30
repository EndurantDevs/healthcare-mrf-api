# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Task-independent native fixtures for the structural custody boundary."""

import importlib.util
import os
import re
import secrets
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace

import asyncpg
import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import create_async_engine

from db.migration_plan_release_storage import upgrade_plan_release_storage
from db.migration_ptg2_v4_attempt_fence import install_lifecycle_lock_layer
from db.plan_release_storage import storage_relation
from process.plan_release_storage_custody import storage_roles
from process.plan_release_storage_provision import provision_plan_release_storage

OLD_REVISION = "hpserve_" + "0" * 26
NEW_REVISION = "hpserve_" + "1" * 26
RELEASE = "hprelease_" + "0" * 26
PLAN = "hpplan_" + "0" * 26


def _database_config():
    database = os.getenv("HLTHPRT_DB_DATABASE", "")
    if not (
        re.search(r"(?:^|[_-])test(?:[_-]|$)", database, re.I)
        or re.fullmatch(r"hc_network_registry_[0-9a-f]{32}", database)
    ):
        pytest.skip("native custody proof requires an explicit test database")
    return {
        "host": os.getenv("HLTHPRT_DB_HOST", "127.0.0.1"),
        "port": int(os.getenv("HLTHPRT_DB_PORT", "5432")),
        "user": os.getenv("HLTHPRT_DB_USER", "postgres"),
        "password": os.getenv("HLTHPRT_DB_PASSWORD") or None,
        "database": database,
    }


@dataclass
class StorageCase:
    schema: str
    preparation_schema: str
    owner: str
    publisher: str
    reader: str
    config: dict
    engine: object
    clients: list = field(default_factory=list)
    created_roles: list = field(default_factory=list)
    role_passwords: dict = field(default_factory=dict, repr=False)

    @property
    def profile(self):
        return {"owner_role": self.owner, "publisher_role": self.publisher, "reader_roles": (self.reader,)}

    async def connect(self, role=None):
        config_by_field = {**self.config, "user": role or self.config["user"]}
        if role in self.role_passwords:
            config_by_field["password"] = self.role_passwords[role]
        client = await asyncpg.connect(**config_by_field)
        self.clients.append(client)
        return client

    def relation(self, name):
        return storage_relation(self.schema, name)

    async def migrate(self):
        async with self.engine.begin() as connection:
            await connection.run_sync(lambda native: upgrade_plan_release_storage(native, self.schema))

    async def provision(self, *, verify_role_profile=None):
        admin = await self.connect()
        async with admin.transaction():
            return await provision_plan_release_storage(
                admin,
                self.schema,
                **self.profile,
                verify_role_profile=verify_role_profile or verify_native_test_profile,
            )


async def verify_native_test_profile(connection, schema_name, *, owner_role, publisher_role, reader_roles):
    """Exercise the callback seam with native role checks; not global policy proof.

    Deployment must supply its independently qualified complete role-profile
    verifier. This isolated fixture has no schema/database/SECDEF policy claim.
    """
    await storage_roles(connection, schema_name, owner_role, publisher_role, reader_roles)
    assert await connection.fetchval("SELECT current_user=session_user")
    assert not await connection.fetchval(
        "SELECT EXISTS(SELECT FROM pg_class WHERE relnamespace=$1::regnamespace "
        "AND relname=ANY($2::text[]) AND relowner<>(SELECT oid FROM pg_roles WHERE rolname=$3))",
        schema_name,
        [
            "plan_release_serving_revision",
            "plan_release_snapshot_binding",
            "plan_release_snapshot_binding_legacy",
            "plan_release_snapshot_pin",
        ],
        owner_role,
    )


def _legacy_migration(connection, schema):
    path = Path(__file__).resolve().parents[1] / "alembic/versions/20260722160000_plan_release_serving_projection.py"
    spec = importlib.util.spec_from_file_location("legacy_plan_release", path)
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    migration._schema = lambda: schema
    migration.op = Operations(MigrationContext.configure(connection))
    migration.upgrade()


def _em_control_tables(connection, schema):
    """Use both real control-table definitions; this is not full-history proof."""
    path = Path(__file__).resolve().parents[1] / "alembic/versions/20260901103000_plan_pricing_em_distance.py"
    spec = importlib.util.spec_from_file_location("plan_pricing_em_controls", path)
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    migration._schema = lambda: schema
    statements = []
    migration.op = SimpleNamespace(execute=statements.append)
    migration.upgrade()
    for name in ("plan_pricing_em_distance_candidate", "plan_pricing_em_distance_attachment"):
        prefix = f"CREATE TABLE {migration._table(schema, name)} ("
        definitions = [statement for statement in statements if statement.lstrip().startswith(prefix)]
        assert len(definitions) == 1
        connection.exec_driver_sql(definitions[0])


def install_legacy_guards(connection, schema):
    """Install unchanged historical guard definitions, without a new test policy."""
    path = Path(__file__).resolve().parents[1] / "alembic/versions/20260724100000_ptg2_v4_attempt_fence.py"
    spec = importlib.util.spec_from_file_location("original_plan_guards", path)
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    operations = Operations(MigrationContext.configure(connection))
    migration.op = operations
    tables = ("plan_release_snapshot_binding", "ptg2_snapshot_pin")
    install_lifecycle_lock_layer(operations, schema, tables)
    for attachment in migration.ATTEMPT_ATTACHMENTS:
        if attachment.table_name in tables:
            migration._create_attachment_triggers(schema, attachment)


async def _create_legacy(case):
    async with case.engine.begin() as connection:
        await connection.exec_driver_sql(f"CREATE SCHEMA {case.schema}")
        await connection.exec_driver_sql(
            f"CREATE TABLE {case.schema}.ptg2_snapshot(snapshot_id varchar(128) PRIMARY KEY,status text,manifest jsonb DEFAULT '{{}}')"
        )
        await connection.run_sync(lambda native: _legacy_migration(native, case.schema))
        await connection.run_sync(lambda native: _em_control_tables(native, case.schema))
        await connection.exec_driver_sql(
            f"CREATE TABLE {case.schema}.ptg2_v3_snapshot_plan_scope(snapshot_id text,plan_id text,plan_market_type text)"
        )
        for table, key in (
            ("ptg2_current_snapshot", "slot"),
            ("ptg2_current_source_snapshot", "source_key"),
            ("ptg2_current_plan_source", "plan_source_key"),
        ):
            extra = ",source_key text" if key == "plan_source_key" else ""
            await connection.exec_driver_sql(
                f"CREATE TABLE {case.schema}.{table}({key} text,snapshot_id text,previous_snapshot_id text{extra})"
            )
        await connection.exec_driver_sql(
            f"INSERT INTO {case.schema}.ptg2_snapshot VALUES('old-snapshot','published','{{}}')"
        )
        await connection.exec_driver_sql(
            f"INSERT INTO {case.schema}.plan_release_serving_revision(serving_revision_id,plan_release_id,"
            "healthporta_plan_id,release_month,release_status,serving_status,is_current,expected_binding_count,binding_set_digest) "
            f"VALUES('{OLD_REVISION}','{RELEASE}','{PLAN}','2026-10','published','published',true,1000,'{'a' * 64}')"
        )
        await connection.exec_driver_sql(
            f"INSERT INTO {case.schema}.plan_release_snapshot_binding(serving_revision_id,binding_ordinal,snapshot_id,source_key,plan_id,role) "
            f"SELECT '{OLD_REVISION}',ordinal,'old-snapshot','synthetic_source','logical-plan','in_network' FROM generate_series(0,999) ordinal"
        )
        await connection.exec_driver_sql(
            f"INSERT INTO {case.schema}.ptg2_snapshot_pin VALUES('plan_release_serving_revision','{OLD_REVISION}','old-snapshot','legacy',now())"
        )


async def _create_profile(case):
    admin = await case.connect()
    for role, login in ((case.owner, False), (case.publisher, True), (case.reader, True)):
        credential = ""
        if login:
            case.role_passwords[role] = secrets.token_hex(32)
            password = await admin.fetchval("SELECT quote_literal($1::text)", case.role_passwords[role])
            credential = " PASSWORD " + password
        await admin.execute(f'CREATE ROLE "{role}" ' + ("LOGIN" if login else "NOLOGIN") + credential)
        case.created_roles.append(role)
    await admin.execute(f'GRANT "{case.owner}" TO "{case.publisher}"')
    await admin.execute(f'CREATE SCHEMA "{case.preparation_schema}" AUTHORIZATION "{case.owner}"')
    names = ",".join('"' + role + '"' for role in (case.owner, case.publisher, case.reader))
    await admin.execute(f'GRANT USAGE ON SCHEMA "{case.schema}","{case.preparation_schema}" TO {names}')


@asynccontextmanager
async def storage_case(*, migrated=True, provisioned=False):
    config = _database_config()
    token = uuid.uuid4().hex[:16]
    url = URL.create(
        "postgresql+asyncpg",
        username=config["user"],
        password=config["password"],
        host=config["host"],
        port=config["port"],
        database=config["database"],
    )
    case = StorageCase(
        "plan_cohort_" + token,
        "plan_prepare_" + token,
        "plan_owner_" + token,
        "plan_publish_" + token,
        "plan_reader_" + token,
        config,
        create_async_engine(url),
    )
    try:
        await _create_legacy(case)
        await _create_profile(case)
        if migrated:
            await case.migrate()
        if provisioned:
            await case.provision()
        yield case
    finally:
        for client in reversed(case.clients):
            if not client.is_closed():
                await client.close()
        await case.engine.dispose()
        admin = await asyncpg.connect(**config)
        try:
            await admin.execute(f'DROP SCHEMA IF EXISTS "{case.schema}" CASCADE')
            await admin.execute(f'DROP SCHEMA IF EXISTS "{case.preparation_schema}" CASCADE')
            for role in reversed(case.created_roles):
                await admin.execute(f'DROP ROLE "{role}"')
        finally:
            await admin.close()
