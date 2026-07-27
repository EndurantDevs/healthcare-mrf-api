# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PostgreSQL proof for atomic V4 provider tax-identity publication."""

from __future__ import annotations

from dataclasses import replace
import hashlib
import struct
import uuid

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import create_async_engine

from process.ptg_parts import ptg2_shared_snapshot_publish as publisher
from tests.ptg2_provider_tax_identity_postgres_support import (
    async_database_url,
    create_prerequisites,
    drop_disposable_schema,
    load_migration,
    quoted,
    run_migration_action,
)


def _publication_contract() -> publisher._V4TaxIdentityContract:
    source_map = ({"shard_id": "shard-a", "ordinal": 0},)
    token_hmac = b"\x44" * 16 + b"\x55" * 16
    content_digest = hashlib.sha256()
    content_digest.update(b"PTG2V4TAXCONTENT\x01")
    content_digest.update(
        publisher._v4_tax_policy_descriptor(
            "ptg-tin-hmac-sha256-v1:release-1"
        )
    )
    content_digest.update(
        publisher._v4_tax_source_ordinal_digest(source_map)
    )
    content_digest.update(struct.pack(">Q", 1))
    content_digest.update(token_hmac)
    content_digest.update(struct.pack(">Q", 4))
    for ordinal, state_code, tin_key in (
        (1, 1, 0),
        (2, 2, None),
        (3, 3, None),
        (4, 4, None),
    ):
        content_digest.update(bytes([ordinal * 17]) * 16)
        content_digest.update(bytes([state_code]))
        if tin_key is None:
            content_digest.update(b"\x00")
        else:
            content_digest.update(b"\x01")
            content_digest.update(struct.pack(">I", tin_key))
        content_digest.update(struct.pack(">I", 1))
        content_digest.update(b"\x01")
    return publisher._V4TaxIdentityContract(
        token_policy_id="ptg-tin-hmac-sha256-v1:release-1",
        token_policy_descriptor_sha256=(
            publisher._v4_tax_policy_descriptor(
                "ptg-tin-hmac-sha256-v1:release-1"
            )
        ),
        source_ordinal_map=source_map,
        source_ordinal_map_digest=(
            publisher._v4_tax_source_ordinal_digest(source_map)
        ),
        source_shard_count=1,
        source_bitmap_bytes=1,
        provider_group_count=4,
        tax_identity_count=1,
        matched_ein_count=1,
        missing_count=1,
        malformed_count=1,
        unsupported_type_count=1,
        content_digest=content_digest.digest(),
    )


async def _create_publication_stages(connection, schema_name: str) -> None:
    """Create compact unlogged stages and populate deterministic proof rows."""

    schema = quoted(schema_name)
    await connection.exec_driver_sql(
        f"""
        CREATE UNLOGGED TABLE {schema}.graph_group_stage AS
        SELECT provider_group_key, provider_group_global_id_128
          FROM {schema}.ptg2_v3_provider_group
         WHERE snapshot_key = 11
        """
    )


async def _populate_group_tax_stage(connection, schema_name: str) -> None:
    """Populate all four tax states from the exact graph-group fixture."""

    schema = quoted(schema_name)
    await connection.exec_driver_sql(
        f"""
        CREATE UNLOGGED TABLE {schema}.tax_dictionary_stage (
            tin_key integer PRIMARY KEY,
            tin_id_128 bytea NOT NULL,
            tin_hmac_sha256 bytea NOT NULL
        )
        """
    )
    await connection.exec_driver_sql(
        f"""
        CREATE UNLOGGED TABLE {schema}.group_tax_stage (
            provider_group_global_id_128 bytea PRIMARY KEY,
            tax_identity_state text NOT NULL,
            tin_key integer,
            source_bitmap bytea NOT NULL
        )
        """
    )
    await connection.exec_driver_sql(
        f"""
        INSERT INTO {schema}.tax_dictionary_stage
            (tin_key, tin_id_128, tin_hmac_sha256)
        VALUES (
            0,
            decode(repeat('44', 16), 'hex'),
            decode(repeat('44', 16) || repeat('55', 16), 'hex')
        )
        """
    )
    await connection.exec_driver_sql(
        f"""
        INSERT INTO {schema}.group_tax_stage (
            provider_group_global_id_128,
            tax_identity_state,
            tin_key,
            source_bitmap
        )
        SELECT provider_group_global_id_128,
               CASE provider_group_key
                   WHEN 1 THEN 'matched_ein'
                   WHEN 2 THEN 'missing'
                   WHEN 3 THEN 'malformed'
                   WHEN 4 THEN 'unsupported_type'
               END,
               CASE WHEN provider_group_key = 1 THEN 0 END,
               decode('01', 'hex')
          FROM {schema}.ptg2_v3_provider_group
         WHERE snapshot_key = 11
        """
    )


async def _publish_relational_rows(connection, schema_name: str) -> None:
    schema = quoted(schema_name)
    token_stage = publisher._V4DenseDictionaryStage(
        stage_table="tax_dictionary_stage",
        key_name="tin_key",
        expected_count=1,
        target_table="ptg2_provider_tax_identity",
        columns=("tin_key", "tin_id_128", "tin_hmac_sha256"),
        value_predicate=(
            "octet_length(tin_id_128) = 16 "
            "AND tin_id_128 = substring(tin_hmac_sha256 FROM 1 FOR 16)"
        ),
    )
    await publisher._publish_v4_dictionary_stage_ranges(
        connection,
        schema=schema,
        snapshot_key=11,
        stage=token_stage,
        progress_callback=None,
    )
    await publisher._publish_v4_tax_group_ranges(
        connection,
        schema=schema,
        snapshot_key=11,
        stage_table="group_tax_stage",
        expected_count=4,
        progress_callback=None,
    )


async def _validate_staged_tax_content(connection, schema_name: str) -> None:
    """Recompute the exact content digest from staged PostgreSQL rows."""

    await publisher._validate_v4_tax_identity_stages(
        connection,
        schema=quoted(schema_name),
        group_dictionary_stage="graph_group_stage",
        tax_identity_stage="tax_dictionary_stage",
        group_tax_identity_stage="group_tax_stage",
        contract=_publication_contract(),
        progress_callback=None,
    )


async def _assert_interrupted_publication_rolls_back(
    engine,
    schema_name: str,
) -> None:
    """Prove one simulated interruption commits no manifest or child row."""

    schema = quoted(schema_name)
    with pytest.raises(RuntimeError, match="simulated publication"):
        async with engine.begin() as connection:
            await _validate_staged_tax_content(connection, schema_name)
            await publisher._publish_v4_tax_identity_manifest(
                connection,
                schema=schema,
                snapshot_key=11,
                contract=_publication_contract(),
            )
            await _publish_relational_rows(connection, schema_name)
            raise RuntimeError("simulated publication interruption")
    async with engine.begin() as connection:
        manifest_count = await connection.scalar(
            sa.text(
                f"SELECT COUNT(*) FROM "
                f"{schema}.ptg2_provider_tax_identity_manifest "
                "WHERE snapshot_key = 11"
            )
        )
    assert manifest_count == 0


async def _publish_exact_tax_fixture(engine, schema_name: str):
    """Publish the complete fixture and return its serving manifest."""

    async with engine.begin() as connection:
        await _validate_staged_tax_content(connection, schema_name)
        manifest = await publisher._publish_v4_tax_identity_manifest(
            connection,
            schema=quoted(schema_name),
            snapshot_key=11,
            contract=_publication_contract(),
        )
        await _publish_relational_rows(connection, schema_name)
    return manifest


async def _assert_exact_replay_and_conflict(
    engine,
    schema_name: str,
    manifest,
) -> None:
    """Prove exact replay and a mismatched content digest fail closed."""

    async with engine.begin() as connection:
        exact_replay = await publisher._publish_v4_tax_identity_manifest(
            connection,
            schema=quoted(schema_name),
            snapshot_key=11,
            contract=_publication_contract(),
        )
    assert exact_replay == manifest
    changed_contract = replace(
        _publication_contract(),
        content_digest=b"\x99" * 32,
    )
    with pytest.raises(RuntimeError, match="replay changed"):
        async with engine.begin() as connection:
            await publisher._publish_v4_tax_identity_manifest(
                connection,
                schema=quoted(schema_name),
                snapshot_key=11,
                contract=changed_contract,
            )


async def _assert_published_row_counts(engine, schema_name: str) -> None:
    """Prove the fixture publishes one token and all four group states."""

    schema = quoted(schema_name)
    async with engine.begin() as connection:
        counts = (
            await connection.execute(
                sa.text(
                    f"""
                    SELECT
                        (SELECT COUNT(*) FROM
                            {schema}.ptg2_provider_tax_identity),
                        (SELECT COUNT(*) FROM
                            {schema}.ptg2_provider_group_tax_identity)
                    """
                )
            )
        ).one()
        await connection.execute(
            sa.text(
                f"UPDATE {schema}.ptg2_v4_snapshot_map_root "
                "SET state = 'complete' WHERE snapshot_key = 11"
            )
        )
    assert tuple(counts) == (1, 4)


@pytest.mark.asyncio
async def test_v4_tax_identity_publication_is_atomic_and_replay_safe(
    monkeypatch,
) -> None:
    """A failed transaction leaves no manifest; exact replay rejects drift."""

    engine = create_async_engine(
        async_database_url(),
        pool_size=1,
        max_overflow=0,
    )
    schema_name = f"ptg2_tax_identity_test_{uuid.uuid4().hex}"
    migration = load_migration()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema_name)
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    is_schema_created = False
    try:
        await create_prerequisites(engine, schema_name)
        is_schema_created = True
        await run_migration_action(engine, migration, "upgrade")
        async with engine.begin() as connection:
            await _create_publication_stages(connection, schema_name)
            await _populate_group_tax_stage(connection, schema_name)
        await _assert_interrupted_publication_rolls_back(
            engine,
            schema_name,
        )
        manifest = await _publish_exact_tax_fixture(engine, schema_name)
        assert manifest["content_digest"] == (
            _publication_contract().content_digest.hex()
        )
        await _assert_exact_replay_and_conflict(
            engine,
            schema_name,
            manifest,
        )
        await _assert_published_row_counts(engine, schema_name)
    finally:
        if is_schema_created:
            await drop_disposable_schema(engine, schema_name)
        await engine.dispose()
