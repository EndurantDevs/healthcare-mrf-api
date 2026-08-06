# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real PostgreSQL proof for opaque billing-reference group resolution."""

from __future__ import annotations

from typing import Any

import sqlalchemy as sa

from api.ptg2_billing_entity_group_resolution import (
    resolve_billing_entity_ref_group_scope,
)
from api.ptg2_billing_entity_refs import encode_billing_entity_ref
from process.tin_npi_connector_security import token_policy_descriptor_sha256
from tests.ptg2_provider_tax_identity_postgres_support import quoted

_SNAPSHOT_KEY = 27
_POLICY_ID = "ptg-tin-hmac-sha256-v1:2026-07"
_CREATE_LAYOUT_SQL = """
INSERT INTO {schema}.ptg2_v3_snapshot_layout
    (snapshot_key, generation, state)
VALUES (27, 'shared_blocks_v4', 'building')
"""
_CREATE_ROOT_SQL = """
INSERT INTO {schema}.ptg2_v4_snapshot_map_root
    (snapshot_key, state)
VALUES (27, 'building')
"""
_CREATE_GROUPS_SQL = """
INSERT INTO {schema}.ptg2_v3_provider_group (
    snapshot_key,
    provider_group_key,
    provider_group_global_id_128
)
SELECT 27,
       ordinal,
       decode(repeat(to_hex(ordinal), 32), 'hex')
  FROM generate_series(1, 5) AS ordinal
"""
_MANIFEST_SQL = """
INSERT INTO {schema}.ptg2_provider_tax_identity_manifest (
    snapshot_key, contract, token_policy_id,
    token_policy_descriptor_sha256, normalization_contract,
    hmac_contract, source_ordinal_contract, source_ordinal_map,
    source_ordinal_map_digest, source_shard_count,
    provider_group_count, tax_identity_count, matched_ein_count,
    missing_count, malformed_count, unsupported_type_count,
    content_digest
) VALUES (
    27,
    'ptg2_provider_group_tax_identity_v1',
    :token_policy_id,
    decode(:token_policy_descriptor_sha256, 'hex'),
    'ein_ascii_digits_or_2_7_hyphen_v1',
    'hmac_sha256_ptg_tin_v1',
    'snapshot_shard_id_sorted_lsb0_bitmap_v1',
    CAST(:source_ordinal_map AS jsonb),
    decode(repeat('22', 32), 'hex'),
    1, 5, 2, 2, 1, 1, 1,
    decode(repeat('33', 32), 'hex')
)
"""
_IDENTITIES_SQL = """
INSERT INTO {schema}.ptg2_provider_tax_identity (
    snapshot_key, tin_key, tin_id_128, tin_hmac_sha256
) VALUES
    (
        27, 0,
        decode(repeat('44', 16), 'hex'),
        decode(repeat('44', 16) || repeat('55', 16), 'hex')
    ),
    (
        27, 1,
        decode(repeat('44', 16), 'hex'),
        decode(repeat('44', 16) || repeat('66', 16), 'hex')
    )
"""
_GROUP_ASSOCIATIONS_SQL = """
INSERT INTO {schema}.ptg2_provider_group_tax_identity (
    snapshot_key,
    provider_group_global_id_128,
    tax_identity_state,
    tin_key,
    source_bitmap
)
SELECT 27,
       provider_group_global_id_128,
       CASE provider_group_key
           WHEN 1 THEN 'matched_ein'
           WHEN 2 THEN 'missing'
           WHEN 3 THEN 'malformed'
           WHEN 4 THEN 'unsupported_type'
           WHEN 5 THEN 'matched_ein'
       END,
       CASE provider_group_key
           WHEN 1 THEN 0
           WHEN 5 THEN 1
       END,
       decode('01', 'hex')
  FROM {schema}.ptg2_v3_provider_group
 WHERE snapshot_key = 27
"""
_SEAL_ROOT_SQL = """
UPDATE {schema}.ptg2_v4_snapshot_map_root
   SET state = 'complete'
 WHERE snapshot_key = 27
"""
_SEAL_LAYOUT_SQL = """
UPDATE {schema}.ptg2_v3_snapshot_layout
   SET state = 'sealed'
 WHERE snapshot_key = 27
"""


async def _install_collision_scope(connection: Any, schema_name: str) -> None:
    """Install and seal one test-only locator-collision sidecar."""

    schema = quoted(schema_name)
    for statement in (
        _CREATE_LAYOUT_SQL,
        _CREATE_ROOT_SQL,
        _CREATE_GROUPS_SQL,
        _MANIFEST_SQL,
        _IDENTITIES_SQL,
        _GROUP_ASSOCIATIONS_SQL,
        _SEAL_ROOT_SQL,
        _SEAL_LAYOUT_SQL,
    ):
        await connection.execute(
            sa.text(statement.format(schema=schema)),
            {
                "token_policy_id": _POLICY_ID,
                "token_policy_descriptor_sha256": (
                    token_policy_descriptor_sha256(_POLICY_ID)
                ),
                "source_ordinal_map": ('[{"ordinal":0,"shard_id":"shard-a"}]'),
            },
        )


def _billing_ref(full_hmac: bytes, *, snapshot_key: int = _SNAPSHOT_KEY) -> str:
    """Return one synthetic, snapshot-authenticated public reference."""

    return encode_billing_entity_ref(
        snapshot_key=snapshot_key,
        tin_id_128=full_hmac[:16],
        tin_hmac_sha256=full_hmac,
    )


async def _resolved_group_refs(
    connection: Any,
    schema_name: str,
    billing_entity_ref: str,
) -> tuple[str, ...] | None:
    """Resolve a reference and return only its stable group identifiers."""

    scope = await resolve_billing_entity_ref_group_scope(
        connection,
        schema_name=schema_name,
        snapshot_key=_SNAPSHOT_KEY,
        billing_entity_ref=billing_entity_ref,
    )
    return None if scope is None else scope.provider_group_refs


async def assert_billing_ref_resolver_postgres_contract(
    engine: Any,
    schema_name: str,
) -> None:
    """Prove collision selection, groups, and snapshot isolation."""

    locator = bytes.fromhex("44" * 16)
    first_hmac = locator + bytes.fromhex("55" * 16)
    second_hmac = locator + bytes.fromhex("66" * 16)
    async with engine.begin() as connection:
        await _install_collision_scope(connection, schema_name)
        first_groups = await _resolved_group_refs(
            connection,
            schema_name,
            _billing_ref(first_hmac),
        )
        second_groups = await _resolved_group_refs(
            connection,
            schema_name,
            _billing_ref(second_hmac),
        )
        wrong_snapshot_groups = await _resolved_group_refs(
            connection,
            schema_name,
            _billing_ref(first_hmac, snapshot_key=_SNAPSHOT_KEY + 1),
        )

    assert first_groups == ("1" * 32,)
    assert second_groups == ("5" * 32,)
    assert wrong_snapshot_groups is None
