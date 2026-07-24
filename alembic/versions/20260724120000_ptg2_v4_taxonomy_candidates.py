"""Add snapshot-pinned PTG V4 inferred-taxonomy candidates.

Revision ID: 20260724120000_ptg2_v4_taxonomy_candidates
Revises: 20260724110000_ptg2_v4_attempt_fence_hardening
"""

from __future__ import annotations

import os

from alembic import op


revision = "20260724120000_ptg2_v4_taxonomy_candidates"
down_revision = "20260724110000_ptg2_v4_attempt_fence_hardening"
branch_labels = None
depends_on = None


def _schema() -> str:
    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise RuntimeError(
            "DB_SCHEMA and HLTHPRT_DB_SCHEMA must identify the same schema"
        )
    return runtime_schema or legacy_schema or "mrf"


def _q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _qt(schema: str, table: str) -> str:
    return f"{_q(schema)}.{_q(table)}"


def upgrade() -> None:
    """Install one packed immutable candidate vector per stable rule."""

    schema = _schema()
    candidate = _qt(schema, "ptg2_v4_inferred_taxonomy_candidate")
    root = _qt(schema, "ptg2_v4_snapshot_map_root")
    metadata_guard = (
        f"{_q(schema)}.{_q('guard_ptg2_v4_snapshot_metadata')}"
    )
    op.execute(
        f"""
        CREATE TABLE {candidate} (
            snapshot_key bigint NOT NULL,
            rule_digest bytea NOT NULL,
            catalog_contract varchar(64) NOT NULL,
            catalog_digest bytea NOT NULL,
            vector_format varchar(32) NOT NULL,
            member_count integer NOT NULL,
            member_digest bytea NOT NULL,
            member_keys bytea NOT NULL,
            representation varchar(16) NOT NULL,
            observe_reason varchar(48),
            observe_count_lower_bound bigint,
            pattern_count integer NOT NULL,
            pattern_member_count bigint NOT NULL,
            pattern_member_bytes bigint NOT NULL,
            pattern_member_digest bytea NOT NULL,
            pattern_member_payload bytea NOT NULL,
            created_at timestamptz NOT NULL DEFAULT now(),
            CONSTRAINT {_q('ptg2_v4_inferred_taxonomy_candidate_pkey')}
                PRIMARY KEY (snapshot_key, rule_digest),
            CONSTRAINT {_q('ptg2_v4_inferred_taxonomy_candidate_root_fkey')}
                FOREIGN KEY (snapshot_key)
                REFERENCES {root} (snapshot_key)
                ON DELETE CASCADE,
            CONSTRAINT {_q('ptg2_v4_inferred_taxonomy_candidate_rule_check')}
                CHECK (octet_length(rule_digest) = 32),
            CONSTRAINT {_q('ptg2_v4_inferred_taxonomy_candidate_catalog_check')}
                CHECK (
                    catalog_contract =
                        'snapshot_npi_live_catalog_individual_v1'
                    AND octet_length(catalog_digest) = 32
                ),
            CONSTRAINT {_q('ptg2_v4_inferred_taxonomy_candidate_vector_check')}
                CHECK (
                    vector_format = 'sorted_u32le_v1'
                    AND member_count >= 0
                    AND octet_length(member_digest) = 32
                    AND octet_length(member_keys) = member_count::bigint * 4
                )
            ,
            CONSTRAINT {_q('ptg2_v4_inferred_taxonomy_candidate_pattern_check')}
                CHECK (
                    representation IN (
                        'direct_v1',
                        'pattern_v1',
                        'observe_v1'
                    )
                    AND pattern_count >= 0
                    AND pattern_member_count >= 0
                    AND pattern_member_bytes >= 0
                    AND octet_length(pattern_member_digest) = 32
                    AND octet_length(pattern_member_payload) =
                        pattern_member_bytes
                    AND (
                        (
                            representation = 'direct_v1'
                            AND observe_reason IS NULL
                            AND observe_count_lower_bound IS NULL
                            AND pattern_count = 0
                            AND pattern_member_count = 0
                            AND pattern_member_bytes = 0
                        )
                        OR (
                            representation = 'pattern_v1'
                            AND observe_reason IS NULL
                            AND observe_count_lower_bound IS NULL
                            AND pattern_count > 0
                            AND pattern_member_count >= pattern_count
                            AND pattern_member_bytes = 24
                                + pattern_count::bigint * 8
                                + pattern_member_count * 4
                        )
                        OR (
                            representation = 'observe_v1'
                            AND (
                                (
                                    observe_reason =
                                        'candidate_cap_exceeded'
                                    AND member_count = 37001
                                    AND observe_count_lower_bound = 37001
                                )
                                OR (
                                    observe_reason =
                                        'pattern_projection_cap_exceeded'
                                    AND member_count <= 37000
                                    AND observe_count_lower_bound = 131073
                                )
                            )
                            AND pattern_count = 0
                            AND pattern_member_count = 0
                            AND pattern_member_bytes = 0
                        )
                    )
                )
        );
        """
    )
    op.execute(
        f"""
        CREATE TRIGGER {_q('ptg2_v4_inferred_taxonomy_candidate_guard')}
        BEFORE INSERT OR UPDATE OR DELETE ON {candidate}
        FOR EACH ROW
        EXECUTE FUNCTION {metadata_guard}();
        """
    )


def downgrade() -> None:
    """Remove only the additive sidecar and its independent sealed cap."""

    schema = _schema()
    candidate = _qt(schema, "ptg2_v4_inferred_taxonomy_candidate")
    op.execute(f"DROP TABLE IF EXISTS {candidate};")
