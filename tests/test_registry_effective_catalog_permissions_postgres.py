# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Restricted direct logins may attach metadata but cannot edit published content."""

import json
import os
from types import SimpleNamespace
from uuid import uuid4

import asyncpg
import pytest

from process.registry_effective_network_catalog import pair_network_catalog_generation
from process.registry_fhir_catalog_permissions import _UPDATES
from process.registry_publication_worker_permissions import _UPDATE
from tests.effective_catalog_support import GENERATION, _seed, _tables


async def _grant_roles(admin, control, catalog, role_by_kind, created_roles):
    for kind, role in role_by_kind.items():
        quoted = '"' + role["name"] + '"'
        await admin.execute(
            f"CREATE ROLE {quoted} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '{role['password']}'"
        )
        created_roles.append(role["name"])
        await admin.execute(f"GRANT USAGE ON SCHEMA {control},{catalog} TO {quoted}")
        await admin.execute(f"GRANT SELECT ON ALL TABLES IN SCHEMA {control},{catalog} TO {quoted}")
        if kind == "publisher":
            for table in ("network_serving_control", "registry_fhir_catalog_snapshot"):
                columns = ",".join(_UPDATES[table])
                await admin.execute(f"GRANT UPDATE({columns}) ON {control}.{table} TO {quoted}")


async def _connect_roles(dsn, role_by_kind, connections):
    connection_by_kind = {}
    for kind, role in role_by_kind.items():
        connection = await asyncpg.connect(
            dsn.replace("postgresql+asyncpg://", "postgresql://"), user=role["name"], password=role["password"]
        )
        connections.append(connection)
        assert (
            await connection.fetchval("SELECT current_user=session_user AND current_setting('is_superuser')='off'")
            is True
        )
        connection_by_kind[kind] = connection
    return connection_by_kind


async def _cleanup_roles(admin, schemas, created_roles, connections, cleanup_receipt):
    for connection in connections:
        await connection.close()
    for schema in reversed(schemas):
        await admin.execute('DROP SCHEMA IF EXISTS "' + schema + '" CASCADE')
        assert await admin.fetchval("SELECT to_regnamespace($1)", schema) is None
    for role in created_roles:
        await admin.execute('DROP ROLE "' + role + '"')
        assert not await admin.fetchval("SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=$1)", role)
    cleanup_receipt.write_text(
        json.dumps({"cleaned_exact_schemas": schemas, "cleaned_exact_roles": created_roles}) + "\n"
    )
    await admin.close()


@pytest.fixture
async def association_roles(tmp_path):
    dsn = os.environ.get("NETWORK_REGISTRY_TEST_DSN")
    if not dsn:
        pytest.skip("NETWORK_REGISTRY_TEST_DSN must select an isolated database")
    admin = await asyncpg.connect(dsn.replace("postgresql+asyncpg://", "postgresql://"))
    schemas = ["effective_permissions_test_" + uuid4().hex, "fhir_catalog_" + uuid4().hex]
    role_by_kind = {
        kind: {"name": "catalog_" + kind + "_" + uuid4().hex, "password": uuid4().hex}
        for kind in ("publisher", "reader")
    }
    cleanup_receipt = tmp_path / "catalogue-permission-resources.json"
    cleanup_receipt.write_text(
        json.dumps(
            {
                "registered_before_creation": {
                    "schemas": schemas,
                    "role_by_kind": [role["name"] for role in role_by_kind.values()],
                }
            }
        )
        + "\n"
    )
    connections, created_roles = [], []
    try:
        control, catalog = '"' + schemas[0] + '"', '"' + schemas[1] + '"'
        await _tables(admin, control, catalog)
        await _seed(admin, control, catalog)
        await admin.execute(
            f"INSERT INTO {control}.network_serving_manifest VALUES(10,0,$1::jsonb,$2)",
            json.dumps({"fhir_catalog": GENERATION}),
            "f" * 64,
        )
        await admin.execute(f"UPDATE {control}.network_serving_control SET generation_id=10")
        await _grant_roles(admin, control, catalog, role_by_kind, created_roles)
        by_kind = await _connect_roles(dsn, role_by_kind, connections)
        yield SimpleNamespace(
            admin=admin,
            control=schemas[0],
            catalog=schemas[1],
            publisher=by_kind["publisher"],
            reader=by_kind["reader"],
        )
    finally:
        await _cleanup_roles(admin, schemas, created_roles, connections, cleanup_receipt)


@pytest.mark.asyncio
async def test_publisher_pairs_only_attachment_columns_and_published_rows_remain_readonly(association_roles):
    db = association_roles
    assert _UPDATE["registry_fhir_catalog_snapshot"] == _UPDATES["registry_fhir_catalog_snapshot"]
    async with db.publisher.transaction():
        await pair_network_catalog_generation(
            db.publisher,
            metadata_generation=GENERATION,
            generation_id=10,
            manifest_sha256="f" * 64,
            approved_revision=0,
            control_schema=db.control,
        )
    assert (
        await db.reader.fetchval(f'SELECT network_generation FROM "{db.control}".registry_fhir_catalog_snapshot') == 10
    )
    for connection in (db.publisher, db.reader):
        for statement in (
            f"UPDATE \"{db.control}\".registry_fhir_catalog_snapshot SET schema_name='changed'",
            f'UPDATE "{db.control}".registry_fhir_catalog_snapshot SET metadata_approved_revision=9',
            f"UPDATE \"{db.catalog}\".registry_fhir_catalog_network SET display_name='changed'",
            f'DELETE FROM "{db.catalog}".registry_fhir_catalog_binding',
            f"INSERT INTO \"{db.control}\".registry_approved_record VALUES(1,'network','1',1,1,'{{}}')",
        ):
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                async with connection.transaction():
                    await connection.execute(statement)
    with pytest.raises(asyncpg.InsufficientPrivilegeError):
        async with db.reader.transaction():
            await db.reader.execute(f'UPDATE "{db.control}".registry_fhir_catalog_snapshot SET network_generation=11')
