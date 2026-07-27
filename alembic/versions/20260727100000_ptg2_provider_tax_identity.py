"""Add immutable provider-group tax-identity sidecars.

Revision ID: 20260727100000_ptg2_provider_tax_identity
Revises: 20260724120000_ptg2_v4_taxonomy_candidates
"""

from __future__ import annotations

import os

from alembic import op


revision = "20260727100000_ptg2_provider_tax_identity"
down_revision = "20260724120000_ptg2_v4_taxonomy_candidates"
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
    """Install the token-only provider-group tax-identity foundation."""

    schema = _schema()
    layout = _qt(schema, "ptg2_v3_snapshot_layout")
    root = _qt(schema, "ptg2_v4_snapshot_map_root")
    provider_group = _qt(schema, "ptg2_v3_provider_group")
    manifest = _qt(schema, "ptg2_provider_tax_identity_manifest")
    tax_identity = _qt(schema, "ptg2_provider_tax_identity")
    group_identity = _qt(schema, "ptg2_provider_group_tax_identity")
    legacy_layout = _qt(
        schema,
        "ptg2_provider_tax_identity_legacy_layout",
    )
    guard = f"{_q(schema)}.{_q('guard_ptg2_provider_tax_identity')}"
    legacy_guard = (
        f"{_q(schema)}."
        f"{_q('guard_ptg2_provider_tax_identity_legacy_layout')}"
    )
    completion_guard = (
        f"{_q(schema)}."
        f"{_q('guard_ptg2_provider_tax_identity_completion')}"
    )

    op.execute(
        f"""
        CREATE TABLE {manifest} (
            snapshot_key bigint NOT NULL,
            contract varchar(64) NOT NULL,
            token_policy_id varchar(64) NOT NULL,
            token_policy_descriptor_sha256 bytea NOT NULL,
            normalization_contract varchar(48) NOT NULL,
            hmac_contract varchar(48) NOT NULL,
            source_ordinal_contract varchar(48) NOT NULL,
            source_ordinal_map jsonb NOT NULL,
            source_ordinal_map_digest bytea NOT NULL,
            source_shard_count integer NOT NULL,
            provider_group_count bigint NOT NULL,
            tax_identity_count bigint NOT NULL,
            matched_ein_count bigint NOT NULL,
            missing_count bigint NOT NULL,
            malformed_count bigint NOT NULL,
            unsupported_type_count bigint NOT NULL,
            content_digest bytea NOT NULL,
            created_at timestamptz NOT NULL DEFAULT now(),
            CONSTRAINT {_q('ptg2_provider_tax_identity_manifest_pkey')}
                PRIMARY KEY (snapshot_key),
            CONSTRAINT {_q('ptg2_provider_tax_identity_manifest_layout_fkey')}
                FOREIGN KEY (snapshot_key)
                REFERENCES {layout} (snapshot_key)
                ON DELETE CASCADE,
            CONSTRAINT {_q('ptg2_provider_tax_identity_manifest_contract_check')}
                CHECK (
                    contract = 'ptg2_provider_group_tax_identity_v1'
                    AND normalization_contract =
                        'ein_ascii_digits_or_2_7_hyphen_v1'
                    AND hmac_contract = 'hmac_sha256_ptg_tin_v1'
                    AND source_ordinal_contract =
                        'snapshot_shard_id_sorted_lsb0_bitmap_v1'
                ),
            CONSTRAINT {_q('ptg2_provider_tax_identity_manifest_policy_check')}
                CHECK (
                    token_policy_id ~
                        '^ptg-tin-hmac-sha256-v1:[a-z0-9][a-z0-9._-]{{0,31}}$'
                    AND octet_length(token_policy_id) <= 55
                    AND octet_length(token_policy_descriptor_sha256) = 32
                ),
            CONSTRAINT {_q('ptg2_provider_tax_identity_manifest_source_check')}
                CHECK (
                    source_shard_count > 0
                    AND jsonb_typeof(source_ordinal_map) = 'array'
                    AND jsonb_array_length(source_ordinal_map)
                        = source_shard_count
                    AND octet_length(source_ordinal_map_digest) = 32
                ),
            CONSTRAINT {_q('ptg2_provider_tax_identity_manifest_count_check')}
                CHECK (
                    provider_group_count >= 0
                    AND tax_identity_count >= 0
                    AND matched_ein_count >= 0
                    AND missing_count >= 0
                    AND malformed_count >= 0
                    AND unsupported_type_count >= 0
                    AND tax_identity_count <= matched_ein_count
                    AND provider_group_count =
                        matched_ein_count
                        + missing_count
                        + malformed_count
                        + unsupported_type_count
                ),
            CONSTRAINT {_q('ptg2_provider_tax_identity_manifest_digest_check')}
                CHECK (octet_length(content_digest) = 32)
        );
        """
    )

    op.execute(
        f"""
        CREATE TABLE {tax_identity} (
            snapshot_key bigint NOT NULL,
            tin_key integer NOT NULL,
            tin_id_128 bytea NOT NULL,
            tin_hmac_sha256 bytea NOT NULL,
            CONSTRAINT {_q('ptg2_provider_tax_identity_pkey')}
                PRIMARY KEY (snapshot_key, tin_key),
            CONSTRAINT {_q('ptg2_provider_tax_identity_manifest_fkey')}
                FOREIGN KEY (snapshot_key)
                REFERENCES {manifest} (snapshot_key)
                ON DELETE CASCADE,
            CONSTRAINT {_q('ptg2_provider_tax_identity_key_check')}
                CHECK (tin_key >= 0),
            CONSTRAINT {_q('ptg2_provider_tax_identity_token_check')}
                CHECK (
                    octet_length(tin_id_128) = 16
                    AND octet_length(tin_hmac_sha256) = 32
                    AND tin_id_128 =
                        substring(tin_hmac_sha256 FROM 1 FOR 16)
                )
        );
        """
    )
    op.execute(
        f"""
        CREATE UNIQUE INDEX {_q('ptg2_provider_tax_identity_locator_idx')}
            ON {tax_identity} (
                snapshot_key,
                tin_id_128,
                tin_hmac_sha256
            )
            INCLUDE (tin_key);
        """
    )

    op.execute(
        f"""
        CREATE TABLE {group_identity} (
            snapshot_key bigint NOT NULL,
            provider_group_global_id_128 bytea NOT NULL,
            tax_identity_state text NOT NULL,
            tin_key integer,
            source_bitmap bytea NOT NULL,
            CONSTRAINT {_q('ptg2_provider_group_tax_identity_pkey')}
                PRIMARY KEY (
                    snapshot_key,
                    provider_group_global_id_128
                ),
            CONSTRAINT {_q('ptg2_provider_group_tax_identity_manifest_fkey')}
                FOREIGN KEY (snapshot_key)
                REFERENCES {manifest} (snapshot_key)
                ON DELETE CASCADE,
            CONSTRAINT {_q('ptg2_provider_group_tax_identity_group_fkey')}
                FOREIGN KEY (
                    snapshot_key,
                    provider_group_global_id_128
                )
                REFERENCES {provider_group} (
                    snapshot_key,
                    provider_group_global_id_128
                )
                ON DELETE CASCADE,
            CONSTRAINT {_q('ptg2_provider_group_tax_identity_tin_fkey')}
                FOREIGN KEY (snapshot_key, tin_key)
                REFERENCES {tax_identity} (snapshot_key, tin_key)
                ON DELETE CASCADE,
            CONSTRAINT {_q('ptg2_provider_group_tax_identity_group_check')}
                CHECK (octet_length(provider_group_global_id_128) = 16),
            CONSTRAINT {_q('ptg2_provider_group_tax_identity_state_check')}
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
                ),
            CONSTRAINT {_q('ptg2_provider_group_tax_identity_source_check')}
                CHECK (octet_length(source_bitmap) > 0)
        );
        """
    )
    op.execute(
        f"""
        CREATE INDEX {_q('ptg2_provider_group_tax_identity_tin_group_idx')}
            ON {group_identity} (
                snapshot_key,
                tin_key,
                provider_group_global_id_128
            )
            WHERE tax_identity_state = 'matched_ein';
        """
    )
    for table, column_name, comment in (
        (
            tax_identity,
            "tin_key",
            "Snapshot-local dense key; never a cross-snapshot identity",
        ),
        (
            tax_identity,
            "tin_hmac_sha256",
            "Full policy-scoped HMAC verified after tin_id_128 lookup",
        ),
        (
            group_identity,
            "tax_identity_state",
            "matched_ein, missing, malformed, or unsupported_type",
        ),
        (
            group_identity,
            "source_bitmap",
            "Bits use the authenticated manifest source ordinal map",
        ),
    ):
        op.execute(
            f"COMMENT ON COLUMN {table}.{_q(column_name)} "
            f"IS '{comment}';"
        )

    # Hold writers from the adoption SELECT through trigger installation so a
    # new root cannot slip into the legacy exemption window.
    op.execute(f"LOCK TABLE {root} IN SHARE ROW EXCLUSIVE MODE;")
    op.execute(
        f"""
        CREATE TABLE {legacy_layout} (
            snapshot_key bigint NOT NULL,
            CONSTRAINT {_q('ptg2_provider_tax_identity_legacy_layout_pkey')}
                PRIMARY KEY (snapshot_key),
            CONSTRAINT {_q('ptg2_provider_tax_identity_legacy_layout_fkey')}
                FOREIGN KEY (snapshot_key)
                REFERENCES {layout} (snapshot_key)
                ON DELETE CASCADE
        );
        """
    )
    op.execute(
        f"""
        INSERT INTO {legacy_layout} (snapshot_key)
        SELECT candidate.snapshot_key
          FROM {root} AS candidate
          JOIN {layout} AS existing_layout
            ON existing_layout.snapshot_key = candidate.snapshot_key
         WHERE existing_layout.generation = 'shared_blocks_v4'
        ON CONFLICT DO NOTHING;
        """
    )

    op.execute(
        f"""
        CREATE FUNCTION {guard}()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $function$
        DECLARE
            root_state varchar(16);
            layout_generation varchar(32);
            layout_state varchar(16);
        BEGIN
            IF TG_OP = 'DELETE' THEN
                IF pg_trigger_depth() = 1 THEN
                    RAISE EXCEPTION 'ptg2_provider_tax_identity_immutable'
                        USING ERRCODE = '55000';
                END IF;
                RETURN OLD;
            END IF;
            IF TG_OP = 'UPDATE' THEN
                RAISE EXCEPTION 'ptg2_provider_tax_identity_immutable'
                    USING ERRCODE = '55000';
            END IF;
            SELECT candidate.state, layout.generation, layout.state
              INTO root_state, layout_generation, layout_state
              FROM {root} AS candidate
              JOIN {layout} AS layout
                ON layout.snapshot_key = candidate.snapshot_key
             WHERE candidate.snapshot_key = NEW.snapshot_key
             FOR UPDATE OF candidate, layout;
            IF root_state IS NULL THEN
                RAISE EXCEPTION 'ptg2_v4_snapshot_map_root_missing'
                    USING ERRCODE = '23503';
            END IF;
            IF root_state <> 'building'
               OR layout_generation <> 'shared_blocks_v4'
               OR layout_state <> 'building' THEN
                RAISE EXCEPTION 'ptg2_provider_tax_identity_not_building'
                    USING ERRCODE = '55000';
            END IF;
            RETURN NEW;
        END;
        $function$;
        """
    )
    op.execute(
        f"""
        CREATE FUNCTION {legacy_guard}()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $function$
        BEGIN
            IF TG_OP = 'DELETE' AND pg_trigger_depth() > 1 THEN
                RETURN OLD;
            END IF;
            RAISE EXCEPTION
                'ptg2_provider_tax_identity_legacy_layout_immutable'
                USING ERRCODE = '55000';
        END;
        $function$;
        """
    )
    op.execute(
        f"""
        CREATE TRIGGER {_q('ptg2_provider_tax_identity_legacy_layout_guard')}
        BEFORE INSERT OR UPDATE OR DELETE ON {legacy_layout}
        FOR EACH ROW
        EXECUTE FUNCTION {legacy_guard}();
        """
    )
    for table_name in (
        "ptg2_provider_tax_identity_manifest",
        "ptg2_provider_tax_identity",
        "ptg2_provider_group_tax_identity",
    ):
        op.execute(
            f"""
            CREATE TRIGGER {_q(table_name + '_guard')}
            BEFORE INSERT OR UPDATE OR DELETE ON {_qt(schema, table_name)}
            FOR EACH ROW
            EXECUTE FUNCTION {guard}();
            """
        )

    op.execute(
        f"""
        CREATE FUNCTION {completion_guard}()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $function$
        DECLARE
            declared_source_shard_count integer;
            declared_source_ordinal_map jsonb;
            declared_provider_group_count bigint;
            declared_tax_identity_count bigint;
            declared_matched_ein_count bigint;
            declared_missing_count bigint;
            declared_malformed_count bigint;
            declared_unsupported_type_count bigint;
            observed_provider_group_count bigint;
            observed_tax_identity_count bigint;
            observed_group_identity_count bigint;
            observed_matched_ein_count bigint;
            observed_missing_count bigint;
            observed_malformed_count bigint;
            observed_unsupported_type_count bigint;
            observed_referenced_identity_count bigint;
            invalid_source_ordinal_count bigint;
            invalid_source_bitmap_count bigint;
            legacy_layout_count bigint;
        BEGIN
            IF NEW.state <> 'complete' OR OLD.state = 'complete' THEN
                RETURN NEW;
            END IF;
            SELECT source_shard_count,
                   source_ordinal_map,
                   provider_group_count,
                   tax_identity_count,
                   matched_ein_count,
                   missing_count,
                   malformed_count,
                   unsupported_type_count
              INTO declared_source_shard_count,
                   declared_source_ordinal_map,
                   declared_provider_group_count,
                   declared_tax_identity_count,
                   declared_matched_ein_count,
                   declared_missing_count,
                   declared_malformed_count,
                   declared_unsupported_type_count
              FROM {manifest}
             WHERE snapshot_key = NEW.snapshot_key;
            IF NOT FOUND THEN
                SELECT COUNT(*)
                  INTO legacy_layout_count
                  FROM {legacy_layout}
                 WHERE snapshot_key = NEW.snapshot_key;
                IF legacy_layout_count = 1 THEN
                    RETURN NEW;
                END IF;
                RAISE EXCEPTION
                    'ptg2_provider_tax_identity_manifest_missing'
                    USING ERRCODE = '23514';
            END IF;
            SELECT COUNT(*)
              INTO invalid_source_ordinal_count
              FROM (
                    SELECT source_entry,
                           ordinal_position,
                           lag(source_entry ->> 'shard_id') OVER (
                               ORDER BY ordinal_position
                           ) AS previous_shard_id
                      FROM jsonb_array_elements(
                               declared_source_ordinal_map
                           ) WITH ORDINALITY
                           AS source_entries(
                               source_entry,
                               ordinal_position
                           )
                   ) AS ordered_sources
             WHERE jsonb_typeof(source_entry) <> 'object'
                OR (
                    SELECT COUNT(*)
                      FROM jsonb_object_keys(
                               CASE
                                   WHEN jsonb_typeof(source_entry) = 'object'
                                   THEN source_entry
                                   ELSE '{{}}'::jsonb
                               END
                           )
                   ) <> 2
                OR NOT (source_entry ? 'shard_id')
                OR NOT (source_entry ? 'ordinal')
                OR COALESCE(
                       jsonb_typeof(source_entry -> 'shard_id'),
                       ''
                   ) <> 'string'
                OR COALESCE(source_entry ->> 'shard_id', '') = ''
                OR CASE
                       WHEN jsonb_typeof(source_entry -> 'ordinal') = 'number'
                        AND source_entry ->> 'ordinal'
                            ~ '^(0|[1-9][0-9]*)$'
                       THEN (source_entry ->> 'ordinal')::numeric
                            <> ordinal_position - 1
                       ELSE TRUE
                   END
                OR (
                    previous_shard_id IS NOT NULL
                    AND convert_to(
                            source_entry ->> 'shard_id',
                            'UTF8'
                        )
                        <= convert_to(previous_shard_id, 'UTF8')
                   );
            SELECT COUNT(*)
              INTO observed_provider_group_count
              FROM {provider_group}
             WHERE snapshot_key = NEW.snapshot_key;
            SELECT COUNT(*)
              INTO observed_tax_identity_count
              FROM {tax_identity}
             WHERE snapshot_key = NEW.snapshot_key;
            SELECT COUNT(*),
                   COUNT(*) FILTER (
                       WHERE tax_identity_state = 'matched_ein'
                   ),
                   COUNT(*) FILTER (WHERE tax_identity_state = 'missing'),
                   COUNT(*) FILTER (WHERE tax_identity_state = 'malformed'),
                   COUNT(*) FILTER (
                       WHERE tax_identity_state = 'unsupported_type'
                   ),
                   COUNT(DISTINCT tin_key)
                       FILTER (WHERE tax_identity_state = 'matched_ein'),
                   COUNT(*) FILTER (
                       WHERE octet_length(source_bitmap)
                             <> (declared_source_shard_count + 7) / 8
                          OR source_bitmap = decode(
                              repeat(
                                  '00',
                                  (declared_source_shard_count + 7) / 8
                              ),
                              'hex'
                          )
                          OR CASE
                                 WHEN declared_source_shard_count % 8 <> 0
                                  AND octet_length(source_bitmap)
                                      = (
                                          declared_source_shard_count + 7
                                        ) / 8
                                 THEN get_byte(
                                          source_bitmap,
                                          octet_length(source_bitmap) - 1
                                      ) >= (
                                          1 << (
                                              declared_source_shard_count % 8
                                          )
                                      )
                                 ELSE FALSE
                             END
                   )
              INTO observed_group_identity_count,
                   observed_matched_ein_count,
                   observed_missing_count,
                   observed_malformed_count,
                   observed_unsupported_type_count,
                   observed_referenced_identity_count,
                   invalid_source_bitmap_count
              FROM {group_identity}
             WHERE snapshot_key = NEW.snapshot_key;
            IF declared_provider_group_count
                    <> observed_provider_group_count
               OR declared_provider_group_count
                    <> observed_group_identity_count
               OR declared_tax_identity_count
                    <> observed_tax_identity_count
               OR declared_tax_identity_count
                    <> observed_referenced_identity_count
               OR declared_matched_ein_count
                    <> observed_matched_ein_count
               OR declared_missing_count <> observed_missing_count
               OR declared_malformed_count <> observed_malformed_count
               OR declared_unsupported_type_count
                    <> observed_unsupported_type_count
               OR invalid_source_ordinal_count <> 0
               OR invalid_source_bitmap_count <> 0 THEN
                RAISE EXCEPTION
                    'ptg2_provider_tax_identity_summary_mismatch'
                    USING ERRCODE = '23514';
            END IF;
            RETURN NEW;
        END;
        $function$;
        """
    )
    op.execute(
        f"""
        CREATE TRIGGER {_q('ptg2_provider_tax_identity_completion_guard')}
        BEFORE UPDATE OF state ON {root}
        FOR EACH ROW
        EXECUTE FUNCTION {completion_guard}();
        """
    )


def downgrade() -> None:
    """Remove only the additive token-only tax-identity foundation."""

    schema = _schema()
    op.execute(
        f"DROP TRIGGER IF EXISTS "
        f"{_q('ptg2_provider_tax_identity_completion_guard')} "
        f"ON {_qt(schema, 'ptg2_v4_snapshot_map_root')};"
    )
    for table_name in (
        "ptg2_provider_group_tax_identity",
        "ptg2_provider_tax_identity",
        "ptg2_provider_tax_identity_manifest",
        "ptg2_provider_tax_identity_legacy_layout",
    ):
        op.execute(f"DROP TABLE IF EXISTS {_qt(schema, table_name)};")
    op.execute(
        f"DROP FUNCTION IF EXISTS "
        f"{_q(schema)}.{_q('guard_ptg2_provider_tax_identity')}();"
    )
    op.execute(
        f"DROP FUNCTION IF EXISTS "
        f"{_q(schema)}."
        f"{_q('guard_ptg2_provider_tax_identity_completion')}();"
    )
    op.execute(
        f"DROP FUNCTION IF EXISTS "
        f"{_q(schema)}."
        f"{_q('guard_ptg2_provider_tax_identity_legacy_layout')}();"
    )
