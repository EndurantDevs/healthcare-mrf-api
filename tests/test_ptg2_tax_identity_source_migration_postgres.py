# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Opt-in PostgreSQL lifecycle proof for source-local PTG tax evidence."""

from __future__ import annotations

import importlib.util
import os
from pathlib import Path
import re
from typing import Any
import uuid

import pytest
import sqlalchemy as sa
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from tests.ptg2_provider_tax_identity_postgres_support import (
    create_prerequisites,
    drop_disposable_schema,
    insert_candidate_sidecar,
    load_migration as load_parent_migration,
    manifest_parameters,
    quoted,
    run_migration_action,
)

ROOT = Path(__file__).resolve().parents[1]
MIGRATION_PATH = (
    ROOT / "alembic" / "versions" / "20260806100000_ptg2_tax_identity_source.py"
)
POSTGRES_DSN_ENV = "HLTHPRT_PTG2_TAX_IDENTITY_SOURCE_POSTGRES_DSN"
_DISPOSABLE_DATABASE_RE = re.compile(
    r"(?:^test(?:[_-]|$)|(?:^|[_-])test(?:[_-]|$))",
    re.IGNORECASE,
)
_FOREIGN_KEYS_SQL = """
    SELECT constraint_record.conname
      FROM pg_constraint AS constraint_record
      JOIN pg_class AS relation
        ON relation.oid = constraint_record.conrelid
      JOIN pg_namespace AS namespace
        ON namespace.oid = relation.relnamespace
     WHERE namespace.nspname = :schema_name
       AND relation.relname = ANY(CAST(:table_names AS text[]))
       AND constraint_record.contype = 'f'
"""
_SOURCE_INDEXES_SQL = """
    SELECT indexname, indexdef
      FROM pg_indexes
     WHERE schemaname = :schema_name
       AND indexname IN (
           'ptg2_provider_group_tax_identity_source_tin_idx',
           'ptg2_provider_group_tax_identity_source_group_idx'
       )
"""
_TRIGGERS_SQL = """
    SELECT relation.relname, trigger_record.tgname,
           trigger_record.tgenabled,
           pg_get_triggerdef(trigger_record.oid)
      FROM pg_trigger AS trigger_record
      JOIN pg_class AS relation
        ON relation.oid = trigger_record.tgrelid
      JOIN pg_namespace AS namespace
        ON namespace.oid = relation.relnamespace
     WHERE namespace.nspname = :schema_name
       AND relation.relname = ANY(CAST(:table_names AS text[]))
       AND NOT trigger_record.tgisinternal
     ORDER BY relation.relname, trigger_record.tgname
"""


def _load_migration():
    module_spec = importlib.util.spec_from_file_location(
        "ptg2_tax_identity_source_postgres_proof",
        MIGRATION_PATH,
    )
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


def _async_database_url() -> sa.URL:
    raw_dsn = os.getenv(POSTGRES_DSN_ENV)
    if not raw_dsn:
        pytest.skip(f"set {POSTGRES_DSN_ENV} for the PostgreSQL proof")
    database_url = make_url(raw_dsn)
    database_name = str(database_url.database or "")
    if (
        not database_url.drivername.startswith("postgresql")
        or not database_name
        or not _DISPOSABLE_DATABASE_RE.search(database_name)
    ):
        pytest.fail(
            f"{POSTGRES_DSN_ENV} must target an explicit PostgreSQL test "
            "database; only a generated disposable schema is modified"
        )
    return database_url.set(drivername="postgresql+asyncpg")


async def _source_catalog(
    engine: AsyncEngine,
    schema_name: str,
) -> tuple[set[str], dict[str, str], list[Any]]:
    """Read the source-local constraints, hot index, and user triggers."""

    table_names = (
        "ptg2_provider_tax_identity_source_manifest",
        "ptg2_provider_tax_identity_source_binding",
        "ptg2_provider_group_tax_identity_source",
    )
    async with engine.begin() as connection:
        foreign_key_scalars = (
            await connection.execute(
                sa.text(_FOREIGN_KEYS_SQL),
                {"schema_name": schema_name, "table_names": list(table_names)},
            )
        ).scalars()
        foreign_key_names = set(foreign_key_scalars)
        index_records = (
            await connection.execute(
                sa.text(_SOURCE_INDEXES_SQL),
                {"schema_name": schema_name},
            )
        ).all()
        trigger_records = (
            await connection.execute(
                sa.text(_TRIGGERS_SQL),
                {"schema_name": schema_name, "table_names": list(table_names)},
            )
        ).all()
    index_definition_by_name = {
        str(index_record.indexname): str(index_record.indexdef)
        for index_record in index_records
    }
    return foreign_key_names, index_definition_by_name, list(trigger_records)


async def _assert_catalog(engine: AsyncEngine, schema_name: str) -> None:
    """Assert the real PostgreSQL catalog contract after upgrade."""

    foreign_key_names, index_definition_by_name, trigger_records = (
        await _source_catalog(engine, schema_name)
    )

    assert foreign_key_names == {
        "ptg2_provider_tax_identity_source_manifest_parent_fkey",
        "ptg2_provider_tax_identity_source_binding_manifest_fkey",
        "ptg2_provider_group_tax_identity_source_binding_fkey",
        "ptg2_provider_group_tax_identity_source_group_fkey",
        "ptg2_provider_group_tax_identity_source_tin_fkey",
    }
    assert set(index_definition_by_name) == {
        "ptg2_provider_group_tax_identity_source_tin_idx",
        "ptg2_provider_group_tax_identity_source_group_idx",
    }
    tin_index = index_definition_by_name[
        "ptg2_provider_group_tax_identity_source_tin_idx"
    ]
    group_index = index_definition_by_name[
        "ptg2_provider_group_tax_identity_source_group_idx"
    ]
    assert "snapshot_key, tin_key, source_key" in tin_index
    assert "tin_key IS NOT NULL" in tin_index
    assert "snapshot_key, provider_group_global_id_128, source_key" in group_index
    assert len(trigger_records) == 9
    assert {
        (
            trigger_record.tgenabled.decode("ascii")
            if isinstance(trigger_record.tgenabled, bytes)
            else str(trigger_record.tgenabled)
        )
        for trigger_record in trigger_records
    } == {"A"}
    assert (
        sum(
            "REFERENCING NEW TABLE" in str(trigger_record.pg_get_triggerdef)
            for trigger_record in trigger_records
        )
        == 3
    )


def _source_manifest_insert(schema_name: str, *, snapshot_key: int) -> sa.TextClause:
    schema = quoted(schema_name)
    return sa.text(f"""
        INSERT INTO {schema}.ptg2_provider_tax_identity_source_manifest (
            snapshot_key, contract, binding_contract, token_policy_id,
            token_policy_descriptor_sha256, source_count,
            provider_group_occurrence_count, matched_ein_count,
            missing_count, malformed_count, unsupported_type_count,
            content_digest
        ) VALUES (
            {snapshot_key},
            'ptg2_provider_group_tax_identity_source_v1',
            'ptg2_tax_identity_rate_source_binding_v1',
            :token_policy_id,
            decode(:token_policy_descriptor_sha256, 'hex'),
            1, 4, 1, 1, 1, 1,
            decode(repeat('61', 32), 'hex')
        )
        """)


def _source_binding_insert(
    schema_name: str,
    *,
    source_key: int,
    snapshot_key: int = 11,
    record_bytes: int = 65,
) -> sa.TextClause:
    schema = quoted(schema_name)
    return sa.text(f"""
        INSERT INTO {schema}.ptg2_provider_tax_identity_source_binding (
            snapshot_key, source_key, source_type, identity_kind,
            identity_sha256, token_policy_id,
            token_policy_descriptor_sha256, record_format,
            format_version, record_bytes, artifact_sha256,
            artifact_byte_count, provider_group_count, matched_ein_count,
            missing_count, malformed_count, unsupported_type_count
        ) VALUES (
            {snapshot_key}, {source_key},
            'in_network', 'logical_json_sha256_v1',
            repeat('{source_key + 1}', 64),
            CAST(:token_policy_id AS varchar(55)),
            decode(:token_policy_descriptor_sha256, 'hex'),
            'ptg2_provider_group_tax_identity_v1',
            1, {record_bytes}, decode(repeat('71', 32), 'hex'),
            13
                + octet_length(CAST(:token_policy_id AS varchar(55)))
                + (4 * {record_bytes}),
            4, 1, 1, 1, 1
        )
        """)


async def _insert_valid_source_evidence(
    engine: AsyncEngine,
    schema_name: str,
) -> None:
    schema = quoted(schema_name)
    policy = manifest_parameters()
    async with engine.begin() as connection:
        await connection.execute(
            _source_manifest_insert(schema_name, snapshot_key=11),
            policy,
        )
        await connection.execute(
            _source_binding_insert(schema_name, source_key=0),
            policy,
        )
        await connection.execute(sa.text(f"""
                INSERT INTO {schema}.ptg2_provider_group_tax_identity_source (
                    snapshot_key, source_key,
                    provider_group_global_id_128, source_record_ordinal,
                    tax_identity_state, tin_key
                )
                SELECT stored.snapshot_key, 0,
                       stored.provider_group_global_id_128,
                       provider_group.provider_group_key - 1,
                       stored.tax_identity_state, stored.tin_key
                  FROM {schema}.ptg2_provider_group_tax_identity AS stored
                  JOIN {schema}.ptg2_v3_provider_group AS provider_group
                    USING (snapshot_key, provider_group_global_id_128)
                 WHERE stored.snapshot_key = 11
                 ORDER BY provider_group.provider_group_key
                """))


async def _assert_policy_and_format_rejections(
    engine: AsyncEngine,
    schema_name: str,
) -> None:
    """Reject a parent-policy mismatch and invalid sidecar format."""

    policy = manifest_parameters()
    mismatched_policy_by_field = {
        **policy,
        "token_policy_id": "ptg-tin-hmac-sha256-v1:other",
        "token_policy_descriptor_sha256": "81" * 32,
    }
    with pytest.raises(DBAPIError, match="source_policy_mismatch"):
        async with engine.begin() as connection:
            await connection.execute(
                _source_manifest_insert(schema_name, snapshot_key=13),
                mismatched_policy_by_field,
            )
    with pytest.raises(DBAPIError, match="source_binding_format_check"):
        async with engine.begin() as connection:
            await connection.execute(
                _source_binding_insert(
                    schema_name,
                    source_key=1,
                    record_bytes=64,
                ),
                policy,
            )


async def _assert_matched_witness_rejection(
    engine: AsyncEngine,
    schema_name: str,
) -> None:
    """Reject a local match that disagrees with the merged group witness."""

    schema = quoted(schema_name)
    policy = manifest_parameters()
    with pytest.raises(DBAPIError, match="source_matched_witness_mismatch"):
        async with engine.begin() as connection:
            await connection.execute(
                _source_manifest_insert(schema_name, snapshot_key=13),
                policy,
            )
            await connection.execute(
                _source_binding_insert(
                    schema_name,
                    source_key=0,
                    snapshot_key=13,
                ),
                policy,
            )
            await connection.execute(sa.text(f"""
                    INSERT INTO
                        {schema}.ptg2_provider_group_tax_identity_source (
                            snapshot_key, source_key,
                            provider_group_global_id_128,
                            source_record_ordinal,
                            tax_identity_state, tin_key
                        )
                    SELECT 13, 0, provider_group_global_id_128,
                           0, 'matched_ein', 0
                      FROM {schema}.ptg2_v3_provider_group
                     WHERE snapshot_key = 13
                       AND provider_group_key = 2
                    """))


async def _assert_mutation_rejections(
    engine: AsyncEngine,
    schema_name: str,
) -> None:
    """Reject direct updates, deletes, and truncation."""

    schema = quoted(schema_name)
    direct_statements = (
        f"UPDATE {schema}.ptg2_provider_tax_identity_source_manifest "
        "SET content_digest = content_digest WHERE snapshot_key = 11",
        f"DELETE FROM {schema}.ptg2_provider_group_tax_identity_source "
        "WHERE snapshot_key = 11",
    )
    for statement in direct_statements:
        with pytest.raises(DBAPIError, match="source_immutable"):
            async with engine.begin() as connection:
                await connection.execute(sa.text(statement))
    with pytest.raises(DBAPIError, match="source_truncate_forbidden"):
        async with engine.begin() as connection:
            await connection.execute(
                sa.text(
                    f"TRUNCATE TABLE "
                    f"{schema}.ptg2_provider_group_tax_identity_source"
                )
            )


async def _seal_and_assert_insert_rejected(
    engine: AsyncEngine,
    schema_name: str,
) -> None:
    """Seal one root and prove a later source insert fails closed."""

    schema = quoted(schema_name)
    policy = manifest_parameters()
    async with engine.begin() as connection:
        await connection.execute(
            sa.text(
                f"UPDATE {schema}.ptg2_v4_snapshot_map_root "
                "SET state = 'complete' WHERE snapshot_key = 11"
            )
        )
    with pytest.raises(DBAPIError, match="source_not_building"):
        async with engine.begin() as connection:
            await connection.execute(
                _source_binding_insert(schema_name, source_key=1),
                policy,
            )


async def _assert_rejections(
    engine: AsyncEngine,
    schema_name: str,
) -> None:
    """Run every focused source-local rejection proof."""

    await _assert_policy_and_format_rejections(engine, schema_name)
    await _assert_matched_witness_rejection(engine, schema_name)
    await _assert_mutation_rejections(engine, schema_name)
    await _seal_and_assert_insert_rejected(engine, schema_name)


async def _assert_layout_cascade(
    engine: AsyncEngine,
    schema_name: str,
) -> None:
    schema = quoted(schema_name)
    source_tables = (
        "ptg2_provider_group_tax_identity_source",
        "ptg2_provider_tax_identity_source_binding",
        "ptg2_provider_tax_identity_source_manifest",
    )
    async with engine.begin() as connection:
        await connection.execute(
            sa.text(
                f"DELETE FROM {schema}.ptg2_v3_snapshot_layout "
                "WHERE snapshot_key = 11"
            )
        )
        remaining_counts = [
            int(
                await connection.scalar(
                    sa.text(f"SELECT COUNT(*) FROM {schema}.{table_name}")
                )
                or 0
            )
            for table_name in source_tables
        ]
    assert remaining_counts == [0, 0, 0]


@pytest.mark.asyncio
async def test_source_local_tax_postgres_lifecycle(monkeypatch) -> None:
    """Prove the empty migration and its build/immutability lifecycle."""

    engine = create_async_engine(
        _async_database_url(),
        pool_size=1,
        max_overflow=0,
    )
    schema_name = f"ptg2_tax_identity_test_{uuid.uuid4().hex}"
    parent_migration = load_parent_migration()
    migration = _load_migration()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema_name)
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    is_schema_created = False
    try:
        await create_prerequisites(engine, schema_name)
        is_schema_created = True
        await run_migration_action(engine, parent_migration, "upgrade")
        await insert_candidate_sidecar(
            engine,
            schema_name,
            snapshot_key=11,
            group_limit=4,
            bitmap_hex="01",
        )
        await insert_candidate_sidecar(
            engine,
            schema_name,
            snapshot_key=13,
            group_limit=4,
            bitmap_hex="01",
        )
        await run_migration_action(engine, migration, "upgrade")
        await _assert_catalog(engine, schema_name)
        await _insert_valid_source_evidence(engine, schema_name)
        await _assert_rejections(engine, schema_name)
        with pytest.raises(DBAPIError, match="downgrade_requires_empty_foundation"):
            await run_migration_action(engine, migration, "downgrade")
        await _assert_layout_cascade(engine, schema_name)
        await run_migration_action(engine, migration, "downgrade")
    finally:
        if is_schema_created:
            await drop_disposable_schema(engine, schema_name)
        await engine.dispose()
