"""Add dormant source-local PTG tax-identity evidence tables.

Revision ID: 20260806100000_ptg2_tax_identity_source
Revises: 20260804100000_ptg2_raw_tin_vault_foundation
"""

from __future__ import annotations

import os

from alembic import op

revision = "20260806100000_ptg2_tax_identity_source"
down_revision = "20260804100000_ptg2_raw_tin_vault_foundation"
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


def _qf(schema: str, function: str) -> str:
    return f"{_q(schema)}.{_q(function)}"


def upgrade() -> None:
    """Install empty physical-layout evidence tables with no publisher."""

    schema = _schema()
    layout = _qt(schema, "ptg2_v3_snapshot_layout")
    root = _qt(schema, "ptg2_v4_snapshot_map_root")
    tax_manifest = _qt(schema, "ptg2_provider_tax_identity_manifest")
    tax_identity = _qt(schema, "ptg2_provider_tax_identity")
    group_identity = _qt(schema, "ptg2_provider_group_tax_identity")
    provider_group = _qt(schema, "ptg2_v3_provider_group")
    source_manifest = _qt(
        schema,
        "ptg2_provider_tax_identity_source_manifest",
    )
    source_binding = _qt(
        schema,
        "ptg2_provider_tax_identity_source_binding",
    )
    source_observation = _qt(
        schema,
        "ptg2_provider_group_tax_identity_source",
    )
    insert_guard = _qf(
        schema,
        "guard_ptg2_provider_tax_identity_source_insert",
    )
    mutation_guard = _qf(
        schema,
        "guard_ptg2_provider_tax_identity_source_mutation",
    )
    truncate_guard = _qf(
        schema,
        "guard_ptg2_provider_tax_identity_source_truncate",
    )

    op.execute(f"""
        CREATE TABLE {source_manifest} (
            snapshot_key bigint NOT NULL,
            contract varchar(64) NOT NULL,
            binding_contract varchar(64) NOT NULL,
            token_policy_id varchar(55) NOT NULL,
            token_policy_descriptor_sha256 bytea NOT NULL,
            source_count integer NOT NULL,
            provider_group_occurrence_count bigint NOT NULL,
            matched_ein_count bigint NOT NULL,
            missing_count bigint NOT NULL,
            malformed_count bigint NOT NULL,
            unsupported_type_count bigint NOT NULL,
            content_digest bytea NOT NULL,
            created_at timestamptz NOT NULL
                DEFAULT transaction_timestamp(),
            CONSTRAINT {_q('ptg2_provider_tax_identity_source_manifest_pkey')}
                PRIMARY KEY (snapshot_key),
            CONSTRAINT {_q('ptg2_provider_tax_identity_source_manifest_parent_fkey')}
                FOREIGN KEY (snapshot_key)
                REFERENCES {tax_manifest} (snapshot_key)
                ON DELETE CASCADE,
            CONSTRAINT {_q('ptg2_provider_tax_identity_source_manifest_policy_key')}
                UNIQUE (
                    snapshot_key,
                    token_policy_id,
                    token_policy_descriptor_sha256
                ),
            CONSTRAINT {_q('ptg2_provider_tax_identity_source_manifest_contract_check')}
                CHECK (
                    contract =
                        'ptg2_provider_group_tax_identity_source_v1'
                    AND binding_contract =
                        'ptg2_tax_identity_rate_source_binding_v1'
                ),
            CONSTRAINT {_q('ptg2_provider_tax_identity_source_manifest_policy_check')}
                CHECK (
                    token_policy_id ~
                        '^ptg-tin-hmac-sha256-v1:[a-z0-9]'
                        '[a-z0-9._-]{{0,31}}$'
                    AND octet_length(token_policy_id) <= 55
                    AND octet_length(token_policy_descriptor_sha256) = 32
                ),
            CONSTRAINT {_q('ptg2_provider_tax_identity_source_manifest_count_check')}
                CHECK (
                    source_count > 0
                    AND provider_group_occurrence_count >= 0
                    AND matched_ein_count >= 0
                    AND missing_count >= 0
                    AND malformed_count >= 0
                    AND unsupported_type_count >= 0
                    AND provider_group_occurrence_count =
                        matched_ein_count
                        + missing_count
                        + malformed_count
                        + unsupported_type_count
                ),
            CONSTRAINT {_q('ptg2_provider_tax_identity_source_manifest_digest_check')}
                CHECK (octet_length(content_digest) = 32)
        );
        """)

    op.execute(f"""
        CREATE TABLE {source_binding} (
            snapshot_key bigint NOT NULL,
            source_key integer NOT NULL,
            source_type varchar(32) NOT NULL,
            identity_kind varchar(64) NOT NULL,
            identity_sha256 varchar(64) NOT NULL,
            token_policy_id varchar(55) NOT NULL,
            token_policy_descriptor_sha256 bytea NOT NULL,
            record_format varchar(64) NOT NULL,
            format_version smallint NOT NULL,
            record_bytes smallint NOT NULL,
            artifact_sha256 bytea NOT NULL,
            artifact_byte_count bigint NOT NULL,
            provider_group_count bigint NOT NULL,
            matched_ein_count bigint NOT NULL,
            missing_count bigint NOT NULL,
            malformed_count bigint NOT NULL,
            unsupported_type_count bigint NOT NULL,
            created_at timestamptz NOT NULL
                DEFAULT transaction_timestamp(),
            CONSTRAINT {_q('ptg2_provider_tax_identity_source_binding_pkey')}
                PRIMARY KEY (snapshot_key, source_key),
            CONSTRAINT {_q('ptg2_provider_tax_identity_source_binding_identity_key')}
                UNIQUE (
                    snapshot_key,
                    source_type,
                    identity_kind,
                    identity_sha256
                ),
            CONSTRAINT {_q('ptg2_provider_tax_identity_source_binding_manifest_fkey')}
                FOREIGN KEY (
                    snapshot_key,
                    token_policy_id,
                    token_policy_descriptor_sha256
                )
                REFERENCES {source_manifest} (
                    snapshot_key,
                    token_policy_id,
                    token_policy_descriptor_sha256
                )
                ON DELETE CASCADE,
            CONSTRAINT {_q('ptg2_provider_tax_identity_source_binding_source_check')}
                CHECK (
                    source_key >= 0
                    AND source_type = 'in_network'
                    AND identity_kind IN (
                        'logical_json_sha256_v1',
                        'raw_container_sha256_v1'
                    )
                    AND identity_sha256 ~ '^[0-9a-f]{{64}}$'
                ),
            CONSTRAINT {_q('ptg2_provider_tax_identity_source_binding_format_check')}
                CHECK (
                    record_format =
                        'ptg2_provider_group_tax_identity_v1'
                    AND format_version = 1
                    AND record_bytes = 65
                ),
            CONSTRAINT {_q('ptg2_provider_tax_identity_source_binding_artifact_check')}
                CHECK (
                    octet_length(artifact_sha256) = 32
                    AND artifact_byte_count =
                        13
                        + octet_length(token_policy_id)
                        + (provider_group_count * record_bytes)
                ),
            CONSTRAINT {_q('ptg2_provider_tax_identity_source_binding_count_check')}
                CHECK (
                    provider_group_count >= 0
                    AND matched_ein_count >= 0
                    AND missing_count >= 0
                    AND malformed_count >= 0
                    AND unsupported_type_count >= 0
                    AND provider_group_count =
                        matched_ein_count
                        + missing_count
                        + malformed_count
                        + unsupported_type_count
                )
        );
        """)

    op.execute(f"""
        CREATE TABLE {source_observation} (
            snapshot_key bigint NOT NULL,
            source_key integer NOT NULL,
            provider_group_global_id_128 bytea NOT NULL,
            source_record_ordinal bigint NOT NULL,
            tax_identity_state text NOT NULL,
            tin_key integer,
            CONSTRAINT {_q('ptg2_provider_group_tax_identity_source_pkey')}
                PRIMARY KEY (
                    snapshot_key,
                    source_key,
                    provider_group_global_id_128
                ),
            CONSTRAINT {_q('ptg2_provider_group_tax_identity_source_ordinal_key')}
                UNIQUE (
                    snapshot_key,
                    source_key,
                    source_record_ordinal
                ),
            CONSTRAINT {_q('ptg2_provider_group_tax_identity_source_binding_fkey')}
                FOREIGN KEY (snapshot_key, source_key)
                REFERENCES {source_binding} (snapshot_key, source_key)
                ON DELETE CASCADE,
            CONSTRAINT {_q('ptg2_provider_group_tax_identity_source_group_fkey')}
                FOREIGN KEY (
                    snapshot_key,
                    provider_group_global_id_128
                )
                REFERENCES {provider_group} (
                    snapshot_key,
                    provider_group_global_id_128
                )
                ON DELETE CASCADE,
            CONSTRAINT {_q('ptg2_provider_group_tax_identity_source_tin_fkey')}
                FOREIGN KEY (snapshot_key, tin_key)
                REFERENCES {tax_identity} (snapshot_key, tin_key)
                ON DELETE CASCADE,
            CONSTRAINT {_q('ptg2_provider_group_tax_identity_source_group_check')}
                CHECK (octet_length(provider_group_global_id_128) = 16),
            CONSTRAINT {_q('ptg2_provider_group_tax_identity_source_ordinal_check')}
                CHECK (source_record_ordinal >= 0),
            CONSTRAINT {_q('ptg2_provider_group_tax_identity_source_state_check')}
                CHECK (
                    tax_identity_state IN (
                        'matched_ein',
                        'missing',
                        'malformed',
                        'unsupported_type'
                    )
                    AND (
                        (
                            tax_identity_state = 'matched_ein'
                            AND tin_key IS NOT NULL
                        )
                        OR (
                            tax_identity_state IN (
                                'missing',
                                'malformed',
                                'unsupported_type'
                            )
                            AND tin_key IS NULL
                        )
                    )
                )
        );
        """)
    op.execute(f"""
        CREATE INDEX {_q('ptg2_provider_group_tax_identity_source_tin_idx')}
            ON {source_observation} (
                snapshot_key,
                tin_key,
                source_key,
                provider_group_global_id_128
            )
            WHERE tin_key IS NOT NULL;
        """)
    op.execute(f"""
        CREATE INDEX {_q('ptg2_provider_group_tax_identity_source_group_idx')}
            ON {source_observation} (
                snapshot_key,
                provider_group_global_id_128,
                source_key
            );
        """)

    op.execute(f"""
        CREATE FUNCTION {insert_guard}()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $function$
        DECLARE
            candidate_snapshot_key bigint;
            root_state varchar(16);
            layout_generation varchar(32);
            layout_state varchar(16);
        BEGIN
            FOR candidate_snapshot_key IN
                SELECT DISTINCT inserted.snapshot_key
                  FROM new_rows AS inserted
                 ORDER BY inserted.snapshot_key
            LOOP
                root_state := NULL;
                layout_generation := NULL;
                layout_state := NULL;
                SELECT candidate.state,
                       candidate_layout.generation,
                       candidate_layout.state
                  INTO root_state, layout_generation, layout_state
                  FROM {root} AS candidate
                  JOIN {layout} AS candidate_layout
                    ON candidate_layout.snapshot_key =
                           candidate.snapshot_key
                 WHERE candidate.snapshot_key = candidate_snapshot_key
                 FOR UPDATE OF candidate, candidate_layout;
                IF root_state IS NULL THEN
                    RAISE EXCEPTION 'ptg2_v4_snapshot_map_root_missing'
                        USING ERRCODE = '23503';
                END IF;
                IF root_state <> 'building'
                   OR layout_generation <> 'shared_blocks_v4'
                   OR layout_state <> 'building' THEN
                    RAISE EXCEPTION
                        'ptg2_provider_tax_identity_source_not_building'
                        USING ERRCODE = '55000';
                END IF;
            END LOOP;
            IF TG_TABLE_NAME =
                    'ptg2_provider_tax_identity_source_manifest' THEN
                IF EXISTS (
                    SELECT 1
                      FROM new_rows AS inserted
                      LEFT JOIN {tax_manifest} AS parent
                        ON parent.snapshot_key = inserted.snapshot_key
                     WHERE parent.snapshot_key IS NULL
                        OR inserted.token_policy_id <>
                           parent.token_policy_id
                        OR inserted.token_policy_descriptor_sha256 <>
                           parent.token_policy_descriptor_sha256
                ) THEN
                    RAISE EXCEPTION
                        'ptg2_provider_tax_identity_source_policy_mismatch'
                        USING ERRCODE = '23514';
                END IF;
            END IF;
            IF TG_TABLE_NAME =
                    'ptg2_provider_group_tax_identity_source' THEN
                IF EXISTS (
                    SELECT 1
                      FROM new_rows AS inserted
                     WHERE inserted.tax_identity_state = 'matched_ein'
                       AND NOT EXISTS (
                            SELECT 1
                              FROM {group_identity} AS merged
                             WHERE merged.snapshot_key =
                                       inserted.snapshot_key
                               AND merged.provider_group_global_id_128 =
                                       inserted.provider_group_global_id_128
                               AND merged.tax_identity_state = 'matched_ein'
                               AND merged.tin_key = inserted.tin_key
                       )
                ) THEN
                    RAISE EXCEPTION
                        'ptg2_provider_tax_identity_source_matched_witness_mismatch'
                        USING ERRCODE = '23514';
                END IF;
            END IF;
            RETURN NULL;
        END;
        $function$;
        """)
    op.execute(f"""
        CREATE FUNCTION {mutation_guard}()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $function$
        BEGIN
            IF TG_OP = 'DELETE' AND pg_trigger_depth() > 1 THEN
                RETURN OLD;
            END IF;
            RAISE EXCEPTION
                'ptg2_provider_tax_identity_source_immutable'
                USING ERRCODE = '55000';
        END;
        $function$;
        """)
    op.execute(f"""
        CREATE FUNCTION {truncate_guard}()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $function$
        BEGIN
            RAISE EXCEPTION
                'ptg2_provider_tax_identity_source_truncate_forbidden'
                USING ERRCODE = '55000';
        END;
        $function$;
        """)
    for table_name in (
        "ptg2_provider_tax_identity_source_manifest",
        "ptg2_provider_tax_identity_source_binding",
        "ptg2_provider_group_tax_identity_source",
    ):
        table = _qt(schema, table_name)
        table_insert_trigger = _q(f"{table_name}_insert_guard")
        table_mutation_trigger = _q(f"{table_name}_mutation_guard")
        table_truncate_trigger = _q(f"{table_name}_truncate_guard")
        op.execute(f"""
            CREATE TRIGGER {table_insert_trigger}
            AFTER INSERT ON {table}
            REFERENCING NEW TABLE AS new_rows
            FOR EACH STATEMENT EXECUTE FUNCTION {insert_guard}();
            """)
        op.execute(
            f"ALTER TABLE {table} ENABLE ALWAYS TRIGGER " f"{table_insert_trigger};"
        )
        op.execute(f"""
            CREATE TRIGGER {table_mutation_trigger}
            BEFORE UPDATE OR DELETE ON {table}
            FOR EACH ROW EXECUTE FUNCTION {mutation_guard}();
            """)
        op.execute(
            f"ALTER TABLE {table} ENABLE ALWAYS TRIGGER " f"{table_mutation_trigger};"
        )
        op.execute(f"""
            CREATE TRIGGER {table_truncate_trigger}
            BEFORE TRUNCATE ON {table}
            FOR EACH STATEMENT EXECUTE FUNCTION {truncate_guard}();
            """)
        op.execute(
            f"ALTER TABLE {table} ENABLE ALWAYS TRIGGER " f"{table_truncate_trigger};"
        )
    op.execute(f"REVOKE ALL ON FUNCTION {insert_guard}() FROM PUBLIC;")
    op.execute(f"REVOKE ALL ON FUNCTION {mutation_guard}() FROM PUBLIC;")
    op.execute(f"REVOKE ALL ON FUNCTION {truncate_guard}() FROM PUBLIC;")


def downgrade() -> None:
    """Remove only a still-empty dormant source-local foundation."""

    schema = _schema()
    table_names = (
        "ptg2_provider_tax_identity_source_manifest",
        "ptg2_provider_tax_identity_source_binding",
        "ptg2_provider_group_tax_identity_source",
    )
    tables = tuple(_qt(schema, table_name) for table_name in table_names)
    insert_guard = _qf(
        schema,
        "guard_ptg2_provider_tax_identity_source_insert",
    )
    mutation_guard = _qf(
        schema,
        "guard_ptg2_provider_tax_identity_source_mutation",
    )
    truncate_guard = _qf(
        schema,
        "guard_ptg2_provider_tax_identity_source_truncate",
    )

    for table in tables:
        op.execute(f"LOCK TABLE {table} IN ACCESS EXCLUSIVE MODE;")
    op.execute(f"""
        DO $block$
        BEGIN
            IF EXISTS (SELECT 1 FROM {tables[0]} LIMIT 1)
               OR EXISTS (SELECT 1 FROM {tables[1]} LIMIT 1)
               OR EXISTS (SELECT 1 FROM {tables[2]} LIMIT 1) THEN
                RAISE EXCEPTION
                    'ptg2_provider_tax_identity_source_downgrade_requires_empty_foundation'
                    USING ERRCODE = '55000';
            END IF;
        END;
        $block$;
        """)
    for table in reversed(tables):
        op.execute(f"DROP TABLE IF EXISTS {table};")
    op.execute(f"DROP FUNCTION IF EXISTS {truncate_guard}();")
    op.execute(f"DROP FUNCTION IF EXISTS {mutation_guard}();")
    op.execute(f"DROP FUNCTION IF EXISTS {insert_guard}();")
