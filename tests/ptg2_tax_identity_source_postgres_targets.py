# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Target-side fixtures for source-local PostgreSQL projector proofs."""

from __future__ import annotations

import sqlalchemy as sa

from process.tin_npi_connector_security import token_policy_descriptor_sha256
from tests.ptg2_provider_tax_identity_postgres_support import quoted
from tests.ptg2_tax_identity_source_projection_fixture import (
    POLICY,
    ordinal_digest,
)

SOURCE_SNAPSHOT_ID = "synthetic-source-snapshot"
CONFLICTING_SOURCE_SNAPSHOT_ID = "conflicting-source-snapshot"


async def _create_logical_source_targets(connection, schema: str) -> None:
    """Create both exact and conflicting source-vector targets."""

    await _create_snapshot_table(connection, schema)
    await _insert_snapshot_targets(connection, schema)
    await _create_snapshot_scope_table(connection, schema)
    await _insert_snapshot_scopes(connection, schema)
    await _create_snapshot_source_table(connection, schema)
    await _insert_snapshot_source_targets(connection, schema)


async def _create_snapshot_table(connection, schema: str) -> None:
    await connection.execute(sa.text(f"""
        CREATE TABLE {schema}.ptg2_snapshot (
            snapshot_id varchar(96) PRIMARY KEY,
            status varchar(32)
        )
        """))


async def _insert_snapshot_targets(connection, schema: str) -> None:
    await connection.execute(
        sa.text(f"""
            INSERT INTO {schema}.ptg2_snapshot (snapshot_id, status)
            VALUES (:snapshot_id, 'building'),
                   (:conflicting_snapshot_id, 'building')
            """),
        {
            "snapshot_id": SOURCE_SNAPSHOT_ID,
            "conflicting_snapshot_id": CONFLICTING_SOURCE_SNAPSHOT_ID,
        },
    )


async def _create_snapshot_scope_table(connection, schema: str) -> None:
    await connection.execute(sa.text(f"""
        CREATE TABLE {schema}.ptg2_v3_snapshot_scope (
            snapshot_id varchar(96) PRIMARY KEY,
            FOREIGN KEY (snapshot_id)
                REFERENCES {schema}.ptg2_snapshot (snapshot_id)
                ON DELETE CASCADE
        )
        """))


async def _insert_snapshot_scopes(connection, schema: str) -> None:
    await connection.execute(
        sa.text(f"""
            INSERT INTO {schema}.ptg2_v3_snapshot_scope (snapshot_id)
            VALUES (:snapshot_id), (:conflicting_snapshot_id)
            """),
        {
            "snapshot_id": SOURCE_SNAPSHOT_ID,
            "conflicting_snapshot_id": CONFLICTING_SOURCE_SNAPSHOT_ID,
        },
    )


async def _create_snapshot_source_table(connection, schema: str) -> None:
    await connection.execute(sa.text(f"""
        CREATE TABLE {schema}.ptg2_v3_snapshot_source (
            snapshot_id varchar(96) NOT NULL,
            source_key integer NOT NULL,
            source_type varchar(32) NOT NULL,
            identity_kind varchar(64) NOT NULL,
            identity_sha256 varchar(64) NOT NULL,
            PRIMARY KEY (snapshot_id, source_key),
            FOREIGN KEY (snapshot_id)
                REFERENCES {schema}.ptg2_v3_snapshot_scope (snapshot_id)
                ON DELETE CASCADE
        )
        """))


async def _insert_snapshot_source_targets(connection, schema: str) -> None:
    await connection.execute(
        sa.text(f"""
            INSERT INTO {schema}.ptg2_v3_snapshot_source
                (snapshot_id, source_key, source_type, identity_kind,
                 identity_sha256)
            VALUES
                (:snapshot_id, 0, 'in_network',
                 'logical_json_sha256_v1', :first_identity),
                (:snapshot_id, 1, 'in_network',
                 'logical_json_sha256_v1', :second_identity),
                (:conflicting_snapshot_id, 0, 'in_network',
                 'logical_json_sha256_v1', :first_identity),
                (:conflicting_snapshot_id, 1, 'in_network',
                 'logical_json_sha256_v1', :conflicting_identity)
            """),
        {
            "snapshot_id": SOURCE_SNAPSHOT_ID,
            "conflicting_snapshot_id": CONFLICTING_SOURCE_SNAPSHOT_ID,
            "first_identity": "1" * 64,
            "second_identity": "2" * 64,
            "conflicting_identity": "9" * 64,
        },
    )


async def _insert_base_aggregate_manifest(connection, schema: str) -> None:
    """Insert the exact two-source aggregate manifest."""

    await connection.execute(
        sa.text(f"""
            INSERT INTO {schema}.ptg2_provider_tax_identity_manifest (
                snapshot_key, contract, token_policy_id,
                token_policy_descriptor_sha256, normalization_contract,
                hmac_contract, source_ordinal_contract, source_ordinal_map,
                source_ordinal_map_digest, source_shard_count,
                provider_group_count, tax_identity_count, matched_ein_count,
                missing_count, malformed_count, unsupported_type_count,
                content_digest
            ) VALUES (
                18, 'ptg2_provider_group_tax_identity_v1', :policy_id,
                decode(:policy_descriptor, 'hex'),
                'ein_ascii_digits_or_2_7_hyphen_v1',
                'hmac_sha256_ptg_tin_v1',
                'snapshot_shard_id_sorted_lsb0_bitmap_v1',
                CAST(:source_map AS jsonb), :ordinal_digest, 2, 4, 2, 2, 0, 1, 1,
                decode(repeat('33', 32), 'hex')
            )
            """),
        {
            "policy_id": POLICY,
            "policy_descriptor": token_policy_descriptor_sha256(POLICY),
            "ordinal_digest": ordinal_digest(("shard-a", "shard-b")),
            "source_map": (
                '[{"ordinal":0,"shard_id":"shard-a"},'
                '{"ordinal":1,"shard_id":"shard-b"}]'
            ),
        },
    )


async def _insert_group_count_mismatch_manifest(connection, schema: str) -> None:
    """Insert a manifest whose aggregate group count is inconsistent."""

    await connection.execute(sa.text(f"""
        INSERT INTO {schema}.ptg2_provider_tax_identity_manifest (
            snapshot_key, contract, token_policy_id,
            token_policy_descriptor_sha256, normalization_contract,
            hmac_contract, source_ordinal_contract, source_ordinal_map,
            source_ordinal_map_digest, source_shard_count,
            provider_group_count, tax_identity_count, matched_ein_count,
            missing_count, malformed_count, unsupported_type_count,
            content_digest
        )
        SELECT 19, contract, token_policy_id,
               token_policy_descriptor_sha256, normalization_contract,
               hmac_contract, source_ordinal_contract, source_ordinal_map,
               source_ordinal_map_digest, source_shard_count,
               5, tax_identity_count, matched_ein_count,
               1, malformed_count, unsupported_type_count, content_digest
          FROM {schema}.ptg2_provider_tax_identity_manifest
         WHERE snapshot_key = 18
        """))


async def _insert_source_map_mismatch_manifest(connection, schema: str) -> None:
    """Insert a different source map that claims the base map digest."""

    await connection.execute(
        sa.text(f"""
        INSERT INTO {schema}.ptg2_provider_tax_identity_manifest (
            snapshot_key, contract, token_policy_id,
            token_policy_descriptor_sha256, normalization_contract,
            hmac_contract, source_ordinal_contract, source_ordinal_map,
            source_ordinal_map_digest, source_shard_count,
            provider_group_count, tax_identity_count, matched_ein_count,
            missing_count, malformed_count, unsupported_type_count,
            content_digest
        )
        SELECT 20, contract, token_policy_id,
               token_policy_descriptor_sha256, normalization_contract,
               hmac_contract, source_ordinal_contract,
               CAST(:different_source_map AS jsonb),
               source_ordinal_map_digest, source_shard_count,
               provider_group_count, tax_identity_count, matched_ein_count,
               missing_count, malformed_count, unsupported_type_count,
               content_digest
         FROM {schema}.ptg2_provider_tax_identity_manifest
         WHERE snapshot_key = 18
        """),
        {
            "different_source_map": (
                '[{"ordinal":0,"shard_id":"shard-c"},'
                '{"ordinal":1,"shard_id":"shard-d"}]'
            )
        },
    )


async def _insert_aggregate_manifests(connection, schema: str) -> None:
    """Insert the exact aggregate and both authenticated mismatch cases."""

    await _insert_base_aggregate_manifest(connection, schema)
    await _insert_group_count_mismatch_manifest(connection, schema)
    await _insert_source_map_mismatch_manifest(connection, schema)


async def _insert_prefix_collision_identities(connection, schema: str) -> None:
    await connection.execute(sa.text(f"""
        INSERT INTO {schema}.ptg2_provider_tax_identity
            (snapshot_key, tin_key, tin_id_128, tin_hmac_sha256)
        VALUES
            (18, 0, decode(repeat('44', 16), 'hex'),
             decode(repeat('44', 16) || repeat('55', 16), 'hex')),
            (18, 1, decode(repeat('44', 16), 'hex'),
             decode(repeat('44', 16) || repeat('66', 16), 'hex'))
        """))


async def _insert_group_tax_reduction(connection, schema: str) -> None:
    await connection.execute(sa.text(f"""
        INSERT INTO {schema}.ptg2_provider_group_tax_identity
            (snapshot_key, provider_group_global_id_128,
             tax_identity_state, tin_key, source_bitmap)
        SELECT 18, provider_group_global_id_128,
               CASE provider_group_key
                   WHEN 1 THEN 'matched_ein'
                   WHEN 2 THEN 'matched_ein'
                   WHEN 3 THEN 'malformed'
                   ELSE 'unsupported_type'
               END,
               CASE provider_group_key WHEN 1 THEN 0 WHEN 2 THEN 1 END,
               decode('03', 'hex')
          FROM {schema}.ptg2_v3_provider_group
         WHERE snapshot_key = 18
         ORDER BY provider_group_key
        """))


async def insert_source_projection_targets(engine, schema_name: str) -> None:
    """Seed exact and adversarial target-side evidence for one proof schema."""

    schema = quoted(schema_name)
    async with engine.begin() as connection:
        await _create_logical_source_targets(connection, schema)
        await _insert_aggregate_manifests(connection, schema)
        await _insert_prefix_collision_identities(connection, schema)
        await _insert_group_tax_reduction(connection, schema)


__all__ = [
    "CONFLICTING_SOURCE_SNAPSHOT_ID",
    "SOURCE_SNAPSHOT_ID",
    "insert_source_projection_targets",
]
