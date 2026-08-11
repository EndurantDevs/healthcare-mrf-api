# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Migration-shaped layout-candidate and block-pin test schema helpers."""

from __future__ import annotations


_LAYOUT_BUILD_DDL_TEMPLATES = (
    """
    CREATE TABLE {schema}.ptg2_layout_build_candidate (
        snapshot_key bigint PRIMARY KEY REFERENCES
            {schema}.ptg2_v3_snapshot_layout(snapshot_key) ON DELETE CASCADE,
        semantic_fingerprint bytea NOT NULL,
        created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
        cleanup_pending_at timestamptz,
        canonical_snapshot_key bigint,
        CHECK (octet_length(semantic_fingerprint) = 32),
        CHECK (
            (cleanup_pending_at IS NULL
             AND canonical_snapshot_key IS NULL)
            OR
            (cleanup_pending_at IS NOT NULL
             AND canonical_snapshot_key IS NOT NULL
             AND canonical_snapshot_key <> snapshot_key)
        )
    )
    """,
    """
    CREATE INDEX ptg2_layout_build_candidate_fingerprint_idx
        ON {schema}.ptg2_layout_build_candidate
           (semantic_fingerprint, snapshot_key)
    """,
    """
    CREATE INDEX ptg2_layout_build_candidate_cleanup_pending_idx
        ON {schema}.ptg2_layout_build_candidate
           (cleanup_pending_at, snapshot_key)
     WHERE cleanup_pending_at IS NOT NULL
    """,
    """
    CREATE TABLE {schema}.ptg2_block_build_pin (
        snapshot_key bigint NOT NULL REFERENCES
            {schema}.ptg2_v3_snapshot_layout(snapshot_key) ON DELETE CASCADE,
        build_token varchar(96) NOT NULL,
        pin_token varchar(96) NOT NULL,
        block_hash bytea NOT NULL,
        created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
        heartbeat_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
        lease_until timestamptz NOT NULL,
        PRIMARY KEY (snapshot_key, pin_token, block_hash),
        CHECK (octet_length(block_hash) = 32)
    )
    """,
    """
    CREATE INDEX ptg2_block_build_pin_active_hash_idx
        ON {schema}.ptg2_block_build_pin (block_hash, lease_until)
    """,
    """
    CREATE INDEX ptg2_block_build_pin_lease_idx
        ON {schema}.ptg2_block_build_pin (lease_until, snapshot_key)
    """,
    """
    CREATE INDEX ptg2_block_build_pin_token_lease_idx
        ON {schema}.ptg2_block_build_pin
           (snapshot_key, build_token, pin_token, lease_until)
    """,
)


def layout_build_candidate_and_pin_ddl(schema: str) -> tuple[str, ...]:
    """Return exact candidate and pin DDL for one synthetic schema."""

    return tuple(
        statement.format(schema=schema)
        for statement in _LAYOUT_BUILD_DDL_TEMPLATES
    )


__all__ = ["layout_build_candidate_and_pin_ddl"]
