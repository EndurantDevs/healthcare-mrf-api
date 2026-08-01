# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import hashlib
import importlib.util
import os
from pathlib import Path
import re

from alembic.migration import MigrationContext
from alembic.operations import Operations
import asyncpg
import pytest
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import create_async_engine

from process.uhc_semantic_build_store import UhcSemanticBuildIdentity
from tests import test_uhc_semantic_build_store as semantic_store_proof


ROOT = Path(__file__).resolve().parents[1]
MIGRATION_PATHS = (
    ROOT / "alembic" / "versions" / "20260728120000_uhc_semantic_build_registry.py",
    ROOT / "alembic" / "versions" / "20260801010000_uhc_semantic_layout_identity.py",
)
DSN_ENV = "HLTHPRT_UHC_SEMANTIC_POSTGRES_DSN"
DATABASE_PATTERN = re.compile(
    r"^uhc_semantic_test_[a-z0-9][a-z0-9_]{7,}$"
)
SCHEMA = "mrf_uhc_semantic_proof"


def _digest(label: str) -> str:
    return hashlib.sha256(label.encode()).hexdigest()


def _database_url():
    dsn = os.getenv(DSN_ENV)
    if not dsn:
        pytest.skip(f"set {DSN_ENV} to run UHC semantic PostgreSQL proofs")
    database_url = make_url(dsn)
    database_name = str(database_url.database or "")
    if (
        not database_url.drivername.startswith("postgresql")
        or DATABASE_PATTERN.fullmatch(database_name) is None
        or not database_url.host
        or not database_url.username
    ):
        pytest.fail(f"refusing non-disposable PostgreSQL database {database_name!r}")
    return database_url


def _load_migration():
    modules = []
    for index, migration_path in enumerate(MIGRATION_PATHS):
        spec = importlib.util.spec_from_file_location(
            f"uhc_semantic_postgres_migration_{index}",
            migration_path,
        )
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        modules.append(module)
    return tuple(modules)


def _upgrade(sync_connection, migrations) -> None:
    migration_context = MigrationContext.configure(sync_connection)
    for migration in migrations:
        migration.op = Operations(migration_context)
        migration.upgrade()


def _downgrade(sync_connection, migrations) -> None:
    migration_context = MigrationContext.configure(sync_connection)
    for migration in reversed(migrations):
        migration.op = Operations(migration_context)
        migration.downgrade()


async def _install_schema(engine, migration) -> None:
    async with engine.begin() as connection:
        await connection.exec_driver_sql(f'DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE')
        await connection.exec_driver_sql(f'CREATE SCHEMA "{SCHEMA}"')
        await _create_registry_dependencies(connection)
        await connection.run_sync(
            lambda sync_connection: _upgrade(sync_connection, migration)
        )


async def _create_registry_dependencies(connection) -> None:
    """Create only the retained identities required by the registry."""

    await connection.exec_driver_sql(
        f"""
        CREATE TABLE "{SCHEMA}".provider_directory_uhc_source_binding (
            catalog_set_sha256 varchar(64) NOT NULL,
            source_file_id varchar(64) NOT NULL,
            artifact_sha256 varchar(64) NOT NULL,
            collection_kind varchar(32) NOT NULL,
            released_at timestamptz,
            PRIMARY KEY (catalog_set_sha256, source_file_id)
        )
        """
    )
    await connection.exec_driver_sql(
        f"""
        CREATE TABLE "{SCHEMA}".provider_directory_uhc_raw_layout (
            artifact_sha256 varchar(64) NOT NULL,
            contract_version integer NOT NULL,
            range_count integer NOT NULL,
            record_count bigint NOT NULL,
            producer_build_id varchar(256) NOT NULL,
            range_set_sha256 varchar(64) NOT NULL,
            manifest_sha256 varchar(64) NOT NULL,
            status varchar(16) NOT NULL,
            PRIMARY KEY (artifact_sha256, contract_version, range_count)
        )
        """
    )


async def _install_legacy_semantic_build(connection) -> dict[str, str]:
    """Seed one v2 quarantined build before the identity migration."""

    identity_by_field = {
        "build": _digest("legacy-build"),
        "catalog": _digest("legacy-catalog"),
        "source": _digest("legacy-source"),
        "artifact": _digest("legacy-artifact"),
        "manifest": _digest("legacy-manifest"),
        "ranges": _digest("legacy-ranges"),
        "encoder": _digest("legacy-encoder"),
    }
    await connection.exec_driver_sql(
        f"""
        INSERT INTO "{SCHEMA}".provider_directory_uhc_source_binding (
            catalog_set_sha256, source_file_id, artifact_sha256,
            collection_kind, released_at
        ) VALUES (
            '{identity_by_field["catalog"]}',
            '{identity_by_field["source"]}',
            '{identity_by_field["artifact"]}',
            'provider_membership', NULL
        )
        """
    )
    await connection.exec_driver_sql(
        f"""
        INSERT INTO "{SCHEMA}".provider_directory_uhc_raw_layout (
            artifact_sha256, contract_version, range_count, record_count,
            producer_build_id, range_set_sha256, manifest_sha256, status
        ) VALUES (
            '{identity_by_field["artifact"]}', 2, 4, 4,
            'legacy-producer-v2', '{identity_by_field["ranges"]}',
            '{identity_by_field["manifest"]}', 'verified'
        )
        """
    )
    await connection.exec_driver_sql(
        f"""
        INSERT INTO "{SCHEMA}".provider_directory_uhc_semantic_build (
            semantic_build_id, catalog_set_sha256, source_file_id,
            artifact_sha256, raw_contract_version, raw_range_count,
            collection_kind, semantic_contract_id,
            semantic_contract_version, copy_format_id, encoder_sha256,
            status, attempt_count, lease_token, lease_expires_at,
            heartbeat_at, stage_schema, stage_relation, failure_code,
            created_at, updated_at
        ) VALUES (
            '{identity_by_field["build"]}', '{identity_by_field["catalog"]}',
            '{identity_by_field["source"]}', '{identity_by_field["artifact"]}',
            2, 4, 'provider_membership',
            'healthporta.uhc.semantic-facts.v2', 2,
            'postgres-copy-binary-uhc-fact-evidence-v1',
            '{identity_by_field["encoder"]}', 'quarantined', 1,
            NULL, NULL, NULL, 'mrf', 'legacy_semantic_stage',
            'legacy_failure', now(), now()
        )
        """
    )
    return identity_by_field


@pytest.mark.asyncio
async def test_layout_identity_migration_backfills_legacy_build(
    monkeypatch,
) -> None:
    """The exact-layout migration preserves a real v2 registry row."""

    database_url = _database_url()
    engine = create_async_engine(
        database_url.set(drivername="postgresql+asyncpg")
    )
    migrations = _load_migration()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", SCHEMA)
    try:
        identity_by_field = await _migrate_legacy_semantic_build(
            engine,
            migrations,
        )
        await _assert_legacy_layout_backfill(engine, identity_by_field)
        await _assert_v3_requires_verifier(engine, identity_by_field)
        await _assert_downgrade_refuses_layout_collapse(
            engine,
            migrations,
            identity_by_field,
        )
        await _downgrade_legacy_semantic_build(engine, migrations)
    finally:
        async with engine.begin() as connection:
            await connection.exec_driver_sql(
                f'DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE'
            )
        await engine.dispose()


async def _migrate_legacy_semantic_build(engine, migrations):
    async with engine.begin() as connection:
        await connection.exec_driver_sql(
            f'DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE'
        )
        await connection.exec_driver_sql(f'CREATE SCHEMA "{SCHEMA}"')
        await _create_registry_dependencies(connection)
        await connection.run_sync(
            lambda sync_connection: _upgrade(
                sync_connection,
                migrations[:1],
            )
        )
        identity_by_field = await _install_legacy_semantic_build(connection)
        await connection.run_sync(
            lambda sync_connection: _upgrade(
                sync_connection,
                migrations[1:],
            )
        )
    return identity_by_field


async def _assert_legacy_layout_backfill(engine, identity_by_field) -> None:
    async with engine.begin() as connection:
        build_row_by_field = (
            await connection.exec_driver_sql(
                f"""
                SELECT semantic_contract_version, manifest_sha256,
                       range_set_sha256, raw_record_count,
                       raw_producer_build_id, semantic_verifier_sha256
                  FROM "{SCHEMA}".provider_directory_uhc_semantic_build
                 WHERE semantic_build_id='{identity_by_field["build"]}'
                """
            )
        ).mappings().one()
        assert build_row_by_field == {
            "semantic_contract_version": 2,
            "manifest_sha256": identity_by_field["manifest"],
            "range_set_sha256": identity_by_field["ranges"],
            "raw_record_count": 4,
            "raw_producer_build_id": "legacy-producer-v2",
            "semantic_verifier_sha256": None,
        }
        nullable_by_column = dict(
            (
                await connection.exec_driver_sql(
                    f"""
                    SELECT column_name, is_nullable
                      FROM information_schema.columns
                     WHERE table_schema='{SCHEMA}'
                       AND table_name='provider_directory_uhc_semantic_build'
                       AND column_name IN (
                           'manifest_sha256', 'range_set_sha256',
                           'raw_record_count', 'raw_producer_build_id',
                           'semantic_verifier_sha256'
                       )
                    """
                )
            ).all()
        )
        assert nullable_by_column == {
            "manifest_sha256": "NO",
            "range_set_sha256": "NO",
            "raw_record_count": "NO",
            "raw_producer_build_id": "NO",
            "semantic_verifier_sha256": "YES",
        }


async def _assert_v3_requires_verifier(engine, identity_by_field) -> None:
    async with engine.begin() as connection:
        failed_update = await connection.begin_nested()
        try:
            with pytest.raises(
                IntegrityError,
                match=(
                    "provider_directory_uhc_semantic_build_"
                    "layout_identity_check"
                ),
            ):
                await connection.exec_driver_sql(
                    f"""
                    UPDATE "{SCHEMA}".provider_directory_uhc_semantic_build
                       SET semantic_contract_version=3
                     WHERE semantic_build_id='{identity_by_field["build"]}'
                    """
                )
        finally:
            await failed_update.rollback()
        assert (
            await connection.exec_driver_sql(
                f"""
                SELECT semantic_contract_version
                  FROM "{SCHEMA}".provider_directory_uhc_semantic_build
                 WHERE semantic_build_id='{identity_by_field["build"]}'
                """
            )
        ).scalar_one() == 2


async def _insert_second_exact_layout(connection, identity_by_field):
    """Insert a second layout sharing the legacy artifact identity."""

    second_build_id = _digest("second exact layout")
    second_manifest = _digest("second manifest")
    second_ranges = _digest("second ranges")
    verifier_sha256 = _digest("v2 verifier")
    await connection.exec_driver_sql(
        f"""
        INSERT INTO "{SCHEMA}".provider_directory_uhc_raw_layout (
            artifact_sha256, contract_version, range_count, record_count,
            producer_build_id, range_set_sha256, manifest_sha256, status
        ) VALUES (
            '{identity_by_field["artifact"]}', 2, 5, 5,
            'second-layout-v2', '{second_ranges}', '{second_manifest}',
            'verified'
        )
        """
    )
    await connection.exec_driver_sql(
        f"""
        INSERT INTO "{SCHEMA}".provider_directory_uhc_semantic_build
        SELECT (jsonb_populate_record(
            NULL::"{SCHEMA}".provider_directory_uhc_semantic_build,
            to_jsonb(build) || jsonb_build_object(
                'semantic_build_id', '{second_build_id}',
                'raw_range_count', 5,
                'manifest_sha256', '{second_manifest}',
                'range_set_sha256', '{second_ranges}',
                'raw_record_count', 5,
                'raw_producer_build_id', 'second-layout-v2',
                'semantic_verifier_sha256', '{verifier_sha256}',
                'stage_relation', 'second_semantic_stage'
            )
        )).* FROM "{SCHEMA}".provider_directory_uhc_semantic_build AS build
         WHERE semantic_build_id='{identity_by_field["build"]}'
        """
    )
    return second_build_id


async def _assert_downgrade_refuses_layout_collapse(
    engine,
    migrations,
    identity_by_field,
) -> None:
    """Prove rollback refuses two layouts that the old key would collapse."""

    async with engine.begin() as connection:
        second_build_id = await _insert_second_exact_layout(
            connection,
            identity_by_field,
        )
        nested = await connection.begin_nested()
        try:
            with pytest.raises(DBAPIError, match="exact UHC semantic layouts"):
                await connection.run_sync(
                    lambda sync_connection: _downgrade(
                        sync_connection,
                        migrations[1:],
                    )
                )
        finally:
            await nested.rollback()
        await connection.exec_driver_sql(
            f'DELETE FROM "{SCHEMA}".provider_directory_uhc_semantic_build '
            f"WHERE semantic_build_id='{second_build_id}'"
        )
        await connection.exec_driver_sql(
            f'DELETE FROM "{SCHEMA}".provider_directory_uhc_raw_layout '
            "WHERE range_count=5"
        )


async def _downgrade_legacy_semantic_build(engine, migrations) -> None:
    async with engine.begin() as connection:
        await connection.run_sync(
            lambda sync_connection: _downgrade(
                sync_connection,
                migrations,
            )
        )


_semantic_fixture = semantic_store_proof._postgres_semantic_fixture
_binary_copy = semantic_store_proof._postgres_binary_copy
_seal_and_reuse_semantic_build = (
    semantic_store_proof._postgres_seal_and_reuse_semantic_build
)


async def _install_semantic_identity(connection, identity) -> None:
    await semantic_store_proof._postgres_install_semantic_identity(
        connection,
        identity,
        SCHEMA,
    )


async def _crash_and_recover_semantic_build(
    connection,
    identity,
    binary_copy_payload: bytes,
):
    return await semantic_store_proof._postgres_crash_and_recover_semantic_build(
        connection,
        identity,
        binary_copy_payload,
        SCHEMA,
    )
