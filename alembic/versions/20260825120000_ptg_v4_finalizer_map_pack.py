# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Add an explicit packed finalizer-map storage contract.

Revision ID: 20260825120000_ptg_v4_finalizer_map_pack
Revises: 20260825090000_geo_assurance_projection
"""

from __future__ import annotations

import os

from alembic import op


revision = "20260825120000_ptg_v4_finalizer_map_pack"
down_revision = "20260825090000_geo_assurance_projection"
branch_labels = None
depends_on = None


_FINALIZER_KINDS = (
    "by_code_price_dictionary",
    "by_code_price_page_v4",
    "by_code_provider_shard_v1",
    "provider_set_codes_v3",
    "provider_set_count_dictionary",
    "provider_set_page_v3_s2",
)


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


def _kind_sql(column: str) -> str:
    values = ", ".join(f"'{value}'" for value in _FINALIZER_KINDS)
    return f"{column} IN ({values})"


_UPGRADE_SQL = (
    """
        CREATE TABLE {root} (
            snapshot_key bigint NOT NULL,
            state varchar(16) NOT NULL,
            contract varchar(48) NOT NULL,
            map_format varchar(32) NOT NULL,
            map_digest bytea,
            canonical_mapping_digest bytea,
            canonical_byte_count bigint NOT NULL DEFAULT 0,
            target_identity_digest bytea,
            object_kind_count integer NOT NULL DEFAULT 6,
            map_pack_count bigint NOT NULL DEFAULT 0,
            coordinate_count bigint NOT NULL DEFAULT 0,
            entry_count bigint NOT NULL DEFAULT 0,
            logical_byte_count bigint NOT NULL DEFAULT 0,
            stored_map_byte_count bigint NOT NULL DEFAULT 0,
            target_block_count bigint NOT NULL DEFAULT 0,
            created_at timestamptz NOT NULL DEFAULT now(),
            completed_at timestamptz,
            CONSTRAINT "ptg2_v4_finalizer_map_root_pkey" PRIMARY KEY (snapshot_key),
            CONSTRAINT "ptg2_v4_finalizer_map_root_layout_fkey"
                FOREIGN KEY (snapshot_key) REFERENCES {layout} (snapshot_key)
                ON DELETE CASCADE,
            CONSTRAINT "ptg2_v4_finalizer_map_root_state_check"
                CHECK (state IN ('building', 'complete')),
            CONSTRAINT "ptg2_v4_finalizer_map_root_contract_check"
                CHECK (contract = 'packed_finalizer_map_v2'),
            CONSTRAINT "ptg2_v4_finalizer_map_root_format_check"
                CHECK (map_format = 'packed_coordinate_hash_v1'),
            CONSTRAINT "ptg2_v4_finalizer_map_root_digest_check"
                CHECK (
                    (map_digest IS NULL OR octet_length(map_digest) = 32)
                    AND (
                        canonical_mapping_digest IS NULL
                        OR octet_length(canonical_mapping_digest) = 32
                    )
                    AND (
                        target_identity_digest IS NULL
                        OR octet_length(target_identity_digest) = 32
                    )
                ),
            CONSTRAINT "ptg2_v4_finalizer_map_root_counts_check" CHECK (
                object_kind_count = 6 AND map_pack_count >= 0
                AND coordinate_count >= 0 AND entry_count >= 0
                AND logical_byte_count >= 0 AND stored_map_byte_count >= 0
                AND target_block_count >= 0 AND canonical_byte_count >= 0
            ),
            CONSTRAINT "ptg2_v4_finalizer_map_root_receipt_check" CHECK (
                (
                    state = 'building'
                    AND canonical_mapping_digest IS NULL
                    AND canonical_byte_count = 0
                    AND target_identity_digest IS NULL
                ) OR (
                    state = 'complete'
                    AND canonical_mapping_digest IS NOT NULL
                    AND canonical_byte_count > 0
                    AND target_identity_digest IS NOT NULL
                )
            ),
            CONSTRAINT "ptg2_v4_finalizer_map_root_completion_check" CHECK (
                (
                    state = 'building' AND map_digest IS NULL
                    AND map_pack_count = 0 AND coordinate_count = 0
                    AND entry_count = 0 AND logical_byte_count = 0
                    AND stored_map_byte_count = 0 AND target_block_count = 0
                    AND completed_at IS NULL
                ) OR (
                    state = 'complete' AND map_digest IS NOT NULL
                    AND map_pack_count > 0 AND coordinate_count > 0
                    AND target_block_count > 0 AND completed_at IS NOT NULL
                )
            )
        );
    """,
    """
        CREATE TABLE {pack} (
            snapshot_key bigint NOT NULL,
            object_kind varchar(64) NOT NULL,
            pack_no integer NOT NULL,
            first_block_key bigint NOT NULL,
            first_fragment_no integer NOT NULL,
            last_block_key bigint NOT NULL,
            last_fragment_no integer NOT NULL,
            coordinate_count integer NOT NULL,
            entry_count bigint NOT NULL,
            logical_byte_count bigint NOT NULL,
            map_block_hash bytea NOT NULL,
            created_at timestamptz NOT NULL DEFAULT now(),
            CONSTRAINT "ptg2_v4_finalizer_map_pack_pkey"
                PRIMARY KEY (snapshot_key, object_kind, pack_no),
            CONSTRAINT "ptg2_v4_finalizer_map_pack_start_key" UNIQUE (
                snapshot_key, object_kind, first_block_key, first_fragment_no
            ),
            CONSTRAINT "ptg2_v4_finalizer_map_pack_root_fkey"
                FOREIGN KEY (snapshot_key) REFERENCES {root} (snapshot_key)
                ON DELETE CASCADE,
            CONSTRAINT "ptg2_v4_finalizer_map_pack_block_fkey"
                FOREIGN KEY (map_block_hash) REFERENCES {block} (block_hash)
                ON DELETE RESTRICT,
            CONSTRAINT "ptg2_v4_finalizer_map_pack_kind_check"
                CHECK ({kind_object_kind}),
            CONSTRAINT "ptg2_v4_finalizer_map_pack_number_check"
                CHECK (pack_no >= 0),
            CONSTRAINT "ptg2_v4_finalizer_map_pack_range_check" CHECK (
                first_block_key >= 0 AND first_fragment_no >= 0
                AND last_block_key >= 0 AND last_fragment_no >= 0
                AND ROW(first_block_key, first_fragment_no)
                    <= ROW(last_block_key, last_fragment_no)
            ),
            CONSTRAINT "ptg2_v4_finalizer_map_pack_counts_check" CHECK (
                coordinate_count BETWEEN 1 AND 256
                AND entry_count >= 0 AND logical_byte_count >= 0
            ),
            CONSTRAINT "ptg2_v4_finalizer_map_pack_hash_check"
                CHECK (octet_length(map_block_hash) = 32)
        );
    """,
    """
        CREATE INDEX "ptg2_v4_finalizer_map_pack_block_hash_idx"
            ON {pack} (map_block_hash);
    """,
    """
        CREATE TABLE {target} (
            snapshot_key bigint NOT NULL,
            block_hash bytea NOT NULL,
            CONSTRAINT "ptg2_v4_finalizer_map_target_pkey"
                PRIMARY KEY (snapshot_key, block_hash),
            CONSTRAINT "ptg2_v4_finalizer_map_target_root_fkey"
                FOREIGN KEY (snapshot_key) REFERENCES {root} (snapshot_key)
                ON DELETE CASCADE,
            CONSTRAINT "ptg2_v4_finalizer_map_target_block_fkey"
                FOREIGN KEY (block_hash) REFERENCES {block} (block_hash)
                ON DELETE RESTRICT,
            CONSTRAINT "ptg2_v4_finalizer_map_target_hash_check"
                CHECK (octet_length(block_hash) = 32)
        );
    """,
    """
        CREATE INDEX "ptg2_v4_finalizer_map_target_block_hash_idx"
            ON {target} (block_hash);
    """,
    """
        CREATE FUNCTION {root_guard}()
        RETURNS trigger LANGUAGE plpgsql
        AS $function$ DECLARE
            layout_generation varchar(32); layout_state varchar(16);
            observed_kind_count bigint; observed_pack_count bigint;
            observed_coordinate_count bigint; observed_entry_count bigint;
            observed_logical_byte_count bigint;
            observed_stored_map_byte_count bigint;
            resolved_map_block_count bigint; observed_target_block_count bigint;
            resolved_target_block_count bigint;
        BEGIN
            IF TG_OP = 'DELETE' THEN
                IF OLD.state = 'complete' AND pg_trigger_depth() = 1 THEN
                    RAISE EXCEPTION 'ptg2_v4_finalizer_map_root_sealed_delete'
                        USING ERRCODE = '55000';
                END IF;
                RETURN OLD;
            END IF;
            SELECT candidate.generation, candidate.state
              INTO layout_generation, layout_state
              FROM {layout} AS candidate
             WHERE candidate.snapshot_key = NEW.snapshot_key
             FOR UPDATE;
            IF layout_generation IS NULL THEN
                RAISE EXCEPTION 'ptg2_v4_finalizer_map_layout_missing'
                    USING ERRCODE = '23503';
            END IF;
            IF layout_generation <> 'shared_blocks_v4'
               OR layout_state <> 'building' THEN
                RAISE EXCEPTION 'ptg2_v4_finalizer_map_layout_not_building'
                    USING ERRCODE = '55000';
            END IF;
            IF TG_OP = 'INSERT' THEN
                IF NEW.state <> 'building' THEN
                    RAISE EXCEPTION 'ptg2_v4_finalizer_map_root_transition_invalid'
                        USING ERRCODE = '55000';
                END IF;
                RETURN NEW;
            END IF;
            IF OLD.state <> 'building' OR NEW.state <> 'complete' THEN
                RAISE EXCEPTION 'ptg2_v4_finalizer_map_root_transition_invalid'
                    USING ERRCODE = '55000';
            END IF;
            IF OLD.snapshot_key IS DISTINCT FROM NEW.snapshot_key
               OR OLD.contract IS DISTINCT FROM NEW.contract
               OR OLD.map_format IS DISTINCT FROM NEW.map_format
               OR OLD.object_kind_count IS DISTINCT FROM NEW.object_kind_count
               OR OLD.created_at IS DISTINCT FROM NEW.created_at THEN
                RAISE EXCEPTION 'ptg2_v4_finalizer_map_root_identity_changed'
                    USING ERRCODE = '55000';
            END IF;
            SELECT COUNT(DISTINCT mapping.object_kind), COUNT(*),
                   COALESCE(SUM(mapping.coordinate_count), 0),
                   COALESCE(SUM(mapping.entry_count), 0),
                   COALESCE(SUM(mapping.logical_byte_count), 0),
                   COALESCE(SUM(cas.stored_byte_count), 0),
                   COUNT(cas.block_hash) FILTER (
                       WHERE cas.format_version = 2
                         AND cas.object_kind = 'snapshot_coordinate_map_v1'
                         AND cas.codec = 'none'
                         AND cas.entry_count = mapping.coordinate_count
                         AND cas.raw_byte_count = cas.stored_byte_count
                         AND octet_length(cas.payload) = cas.stored_byte_count
                   )
              INTO observed_kind_count, observed_pack_count,
                   observed_coordinate_count, observed_entry_count,
                   observed_logical_byte_count, observed_stored_map_byte_count,
                   resolved_map_block_count
              FROM {pack} AS mapping
              LEFT JOIN {block} AS cas ON cas.block_hash = mapping.map_block_hash
             WHERE mapping.snapshot_key = NEW.snapshot_key;
            SELECT COUNT(*), COUNT(cas.block_hash) FILTER (
                       WHERE cas.format_version = 2
                         AND {kind_cas_object_kind}
                         AND cas.codec IN ('none', 'zlib')
                         AND cas.entry_count >= 0 AND cas.raw_byte_count >= 0
                         AND cas.stored_byte_count >= 0
                         AND octet_length(cas.payload) = cas.stored_byte_count
                         AND (
                             cas.codec <> 'none'
                             OR cas.raw_byte_count = cas.stored_byte_count
                         )
                   )
              INTO observed_target_block_count, resolved_target_block_count
              FROM {target} AS anchor
              LEFT JOIN {block} AS cas ON cas.block_hash = anchor.block_hash
             WHERE anchor.snapshot_key = NEW.snapshot_key;
            IF observed_kind_count <> 6
               OR observed_pack_count <> resolved_map_block_count
               OR NEW.object_kind_count <> observed_kind_count
               OR NEW.map_pack_count <> observed_pack_count
               OR NEW.coordinate_count <> observed_coordinate_count
               OR NEW.entry_count <> observed_entry_count
               OR NEW.logical_byte_count <> observed_logical_byte_count
               OR NEW.stored_map_byte_count <> observed_stored_map_byte_count
               OR NEW.target_block_count <> observed_target_block_count
               OR observed_target_block_count <> resolved_target_block_count THEN
                RAISE EXCEPTION 'ptg2_v4_finalizer_map_root_summary_mismatch'
                    USING ERRCODE = '23514';
            END IF;
            IF EXISTS (
                SELECT 1 FROM (
                    SELECT mapping.object_kind, mapping.pack_no,
                           mapping.first_block_key, mapping.first_fragment_no,
                           ROW_NUMBER() OVER (
                               PARTITION BY mapping.object_kind
                               ORDER BY mapping.pack_no
                           ) - 1 AS expected_pack_no,
                           LAG(mapping.last_block_key) OVER (
                               PARTITION BY mapping.object_kind
                               ORDER BY mapping.pack_no
                           ) AS previous_last_block_key,
                           LAG(mapping.last_fragment_no) OVER (
                               PARTITION BY mapping.object_kind
                               ORDER BY mapping.pack_no
                           ) AS previous_last_fragment_no
                      FROM {pack} AS mapping
                     WHERE mapping.snapshot_key = NEW.snapshot_key
                ) AS ordered
                WHERE ordered.pack_no <> ordered.expected_pack_no OR (
                    ordered.previous_last_block_key IS NOT NULL
                    AND ROW(
                            ordered.first_block_key,
                            ordered.first_fragment_no
                        ) <= ROW(
                            ordered.previous_last_block_key,
                            ordered.previous_last_fragment_no
                        )
                )
            ) THEN
                RAISE EXCEPTION 'ptg2_v4_finalizer_map_pack_sequence_invalid'
                    USING ERRCODE = '23514';
            END IF;
            IF EXISTS (
                SELECT 1 FROM {legacy_mapping}
                 WHERE snapshot_key = NEW.snapshot_key
                   AND {kind_object_kind}
            ) THEN
                RAISE EXCEPTION 'ptg2_v4_finalizer_map_mixed_storage'
                    USING ERRCODE = '23514';
            END IF;
            RETURN NEW;
        END;
        $function$;
    """,
    """
        CREATE TRIGGER "ptg2_v4_finalizer_map_root_guard"
        BEFORE INSERT OR UPDATE OR DELETE ON {root}
        FOR EACH ROW
        EXECUTE FUNCTION {root_guard}();
    """,
    """
        CREATE FUNCTION {pack_insert_guard}()
        RETURNS trigger LANGUAGE plpgsql
        AS $function$ BEGIN
            PERFORM candidate.snapshot_key
              FROM {root} AS candidate
              JOIN {layout} AS layout
                ON layout.snapshot_key = candidate.snapshot_key
              JOIN (SELECT DISTINCT snapshot_key FROM new_rows) AS inserted
                ON inserted.snapshot_key = candidate.snapshot_key
             ORDER BY candidate.snapshot_key
             FOR UPDATE OF candidate, layout;
            IF EXISTS (
                SELECT 1
                  FROM (SELECT DISTINCT snapshot_key FROM new_rows) AS inserted
                  LEFT JOIN {root} AS candidate
                    ON candidate.snapshot_key = inserted.snapshot_key
                  LEFT JOIN {layout} AS layout
                    ON layout.snapshot_key = inserted.snapshot_key
                 WHERE candidate.state IS DISTINCT FROM 'building'
                    OR layout.generation IS DISTINCT FROM 'shared_blocks_v4'
                    OR layout.state IS DISTINCT FROM 'building'
            ) THEN
                RAISE EXCEPTION 'ptg2_v4_finalizer_map_root_not_building'
                    USING ERRCODE = '55000';
            END IF;
            RETURN NULL;
        END;
        $function$;
    """,
    """
        CREATE TRIGGER "ptg2_v4_finalizer_map_pack_insert_guard"
        AFTER INSERT ON {pack}
        REFERENCING NEW TABLE AS new_rows
        FOR EACH STATEMENT
        EXECUTE FUNCTION {pack_insert_guard}();
    """,
    """
        CREATE FUNCTION {pack_mutation_guard}()
        RETURNS trigger LANGUAGE plpgsql
        AS $function$ BEGIN
            IF pg_trigger_depth() = 1 THEN
                RAISE EXCEPTION 'ptg2_v4_finalizer_map_pack_immutable'
                    USING ERRCODE = '55000';
            END IF;
            RETURN NULL;
        END;
        $function$;
    """,
    """
        CREATE TRIGGER "ptg2_v4_finalizer_map_pack_mutation_guard"
        BEFORE UPDATE OR DELETE ON {pack}
        FOR EACH STATEMENT
        EXECUTE FUNCTION {pack_mutation_guard}();
    """,
    """
        CREATE FUNCTION {target_insert_guard}()
        RETURNS trigger LANGUAGE plpgsql
        AS $function$ BEGIN
            PERFORM candidate.snapshot_key
              FROM {root} AS candidate
              JOIN {layout} AS layout
                ON layout.snapshot_key = candidate.snapshot_key
              JOIN (SELECT DISTINCT snapshot_key FROM new_rows) AS inserted
                ON inserted.snapshot_key = candidate.snapshot_key
             ORDER BY candidate.snapshot_key
             FOR UPDATE OF candidate, layout;
            IF EXISTS (
                SELECT 1
                  FROM (SELECT DISTINCT snapshot_key FROM new_rows) AS inserted
                  LEFT JOIN {root} AS candidate
                    ON candidate.snapshot_key = inserted.snapshot_key
                  LEFT JOIN {layout} AS layout
                    ON layout.snapshot_key = inserted.snapshot_key
                 WHERE candidate.state IS DISTINCT FROM 'building'
                    OR layout.generation IS DISTINCT FROM 'shared_blocks_v4'
                    OR layout.state IS DISTINCT FROM 'building'
            ) THEN
                RAISE EXCEPTION 'ptg2_v4_finalizer_map_target_not_building'
                    USING ERRCODE = '55000';
            END IF;
            RETURN NULL;
        END;
        $function$;
    """,
    """
        CREATE TRIGGER "ptg2_v4_finalizer_map_target_insert_guard"
        AFTER INSERT ON {target}
        REFERENCING NEW TABLE AS new_rows
        FOR EACH STATEMENT
        EXECUTE FUNCTION {target_insert_guard}();
    """,
    """
        CREATE FUNCTION {target_mutation_guard}()
        RETURNS trigger LANGUAGE plpgsql
        AS $function$ BEGIN
            IF pg_trigger_depth() = 1 THEN
                RAISE EXCEPTION 'ptg2_v4_finalizer_map_target_immutable'
                    USING ERRCODE = '55000';
            END IF;
            RETURN NULL;
        END;
        $function$;
    """,
    """
        CREATE TRIGGER "ptg2_v4_finalizer_map_target_mutation_guard"
        BEFORE UPDATE OR DELETE ON {target}
        FOR EACH STATEMENT
        EXECUTE FUNCTION {target_mutation_guard}();
    """,
    """
        CREATE FUNCTION {truncate_guard}()
        RETURNS trigger LANGUAGE plpgsql
        AS $function$ BEGIN
            RAISE EXCEPTION 'ptg2_v4_finalizer_map_truncate_forbidden'
                USING ERRCODE = '55000';
        END;
        $function$;
    """,
    """
        CREATE TRIGGER "ptg2_v4_finalizer_map_root_truncate_guard"
        BEFORE TRUNCATE ON {root}
        FOR EACH STATEMENT
        EXECUTE FUNCTION {truncate_guard}();
    """,
    """
        CREATE TRIGGER "ptg2_v4_finalizer_map_pack_truncate_guard"
        BEFORE TRUNCATE ON {pack}
        FOR EACH STATEMENT
        EXECUTE FUNCTION {truncate_guard}();
    """,
    """
        CREATE TRIGGER "ptg2_v4_finalizer_map_target_truncate_guard"
        BEFORE TRUNCATE ON {target}
        FOR EACH STATEMENT
        EXECUTE FUNCTION {truncate_guard}();
    """,
)


def _migration_names(schema: str) -> dict[str, str]:
    names = {
        "layout": _qt(schema, "ptg2_v3_snapshot_layout"),
        "block": _qt(schema, "ptg2_v3_block"),
        "legacy_mapping": _qt(schema, "ptg2_v3_snapshot_block"),
        "root": _qt(schema, "ptg2_v4_finalizer_map_root"),
        "pack": _qt(schema, "ptg2_v4_finalizer_map_pack"),
        "target": _qt(schema, "ptg2_v4_finalizer_map_target"),
        "kind_object_kind": _kind_sql("object_kind"),
        "kind_cas_object_kind": _kind_sql("cas.object_kind"),
    }
    for guard in (
        "root",
        "pack_insert",
        "pack_mutation",
        "target_insert",
        "target_mutation",
        "truncate",
    ):
        function = f"guard_ptg2_v4_finalizer_map_{guard}"
        names[f"{guard}_guard"] = f"{_q(schema)}.{_q(function)}"
    return names


def upgrade() -> None:
    """Add packed finalizer roots, coordinate packs, and target anchors."""

    names = _migration_names(_schema())
    for statement in _UPGRADE_SQL:
        op.execute(statement.format_map(names))


def downgrade() -> None:
    """Remove the optional packed finalizer-map contract when unused."""

    schema = _schema()
    names = _migration_names(schema)
    op.execute(f"LOCK TABLE {names['root']} IN SHARE ROW EXCLUSIVE MODE;")
    op.execute(
        f"""
        DO $block$
        BEGIN
            IF EXISTS (SELECT 1 FROM {names['root']}) THEN
                RAISE EXCEPTION 'ptg2_v4_finalizer_map_downgrade_requires_empty_root'
                    USING ERRCODE = '55000';
            END IF;
        END;
        $block$;
        """
    )
    op.execute(f"DROP TABLE IF EXISTS {names['target']};")
    op.execute(f"DROP TABLE IF EXISTS {names['pack']};")
    op.execute(f"DROP TABLE IF EXISTS {names['root']};")
    for function_name in (
        "guard_ptg2_v4_finalizer_map_truncate",
        "guard_ptg2_v4_finalizer_map_target_mutation",
        "guard_ptg2_v4_finalizer_map_target_insert",
        "guard_ptg2_v4_finalizer_map_pack_mutation",
        "guard_ptg2_v4_finalizer_map_pack_insert",
        "guard_ptg2_v4_finalizer_map_root",
    ):
        op.execute(
            f"DROP FUNCTION IF EXISTS {_q(schema)}.{_q(function_name)}();"
        )
