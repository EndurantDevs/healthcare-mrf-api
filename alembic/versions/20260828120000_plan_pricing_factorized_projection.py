"""Add bounded factorized pricing-card and packed aggregate storage.

Revision ID: 20260828120000_plan_pricing_factorized_projection
Revises: 20260829100000_activate_import_run_idempotency_scope
"""

from __future__ import annotations

import os

from alembic import op


revision = "20260828120000_plan_pricing_factorized_projection"
down_revision = "20260829100000_activate_import_run_idempotency_scope"
branch_labels = None
depends_on = None


V2_CONTRACT = "plan_pricing_card_v2"
V3_CONTRACT = "plan_pricing_factorized_v3"


def _q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _schema() -> str:
    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise RuntimeError(
            "DB_SCHEMA and HLTHPRT_DB_SCHEMA must identify the same schema"
        )
    return runtime_schema or legacy_schema or "mrf"


def _table(schema: str, name: str) -> str:
    return f"{_q(schema)}.{_q(name)}"


def _candidate_guard_sql(schema: str) -> str:
    candidate = _table(schema, "plan_pricing_projection_candidate")
    card = _table(schema, "plan_pricing_card")
    aggregate = _table(schema, "plan_pricing_cell_aggregate")
    provider_membership = _table(
        schema, "plan_pricing_provider_membership"
    )
    provider_cell = _table(schema, "plan_pricing_provider_cell")
    rate_profile = _table(schema, "plan_pricing_rate_profile")
    aggregate_pack = _table(schema, "plan_pricing_aggregate_pack")
    prewarm_shape = _table(schema, "plan_pricing_prewarm_shape")
    guard = _table(schema, "plan_pricing_projection_candidate_guard")
    return f"""
        CREATE OR REPLACE FUNCTION {guard}() RETURNS trigger
        LANGUAGE plpgsql AS $$
        DECLARE
            actual_card_rows bigint;
            actual_aggregate_rows bigint;
            actual_fragment_bytes bigint;
            actual_provider_memberships bigint;
            actual_provider_cells bigint;
            actual_provider_bytes bigint;
            actual_rate_profiles bigint;
            actual_aggregate_entries bigint;
            actual_aggregate_packs bigint;
            actual_aggregate_raw_bytes bigint;
            actual_aggregate_stored_bytes bigint;
            actual_prewarm_shapes bigint;
        BEGIN
            IF TG_OP <> 'INSERT' AND OLD.state = 'ready' THEN
                RAISE EXCEPTION 'ready plan-pricing projections are immutable';
            END IF;
            IF TG_OP = 'DELETE' THEN
                RETURN OLD;
            END IF;
            IF NEW.state = 'ready' AND NEW.contract_version = '{V2_CONTRACT}' THEN
                SELECT COUNT(*), COALESCE(SUM(octet_length(fragment)), 0)
                  INTO actual_card_rows, actual_fragment_bytes
                  FROM {card}
                 WHERE projection_id = NEW.projection_id;
                SELECT COUNT(*),
                       actual_fragment_bytes
                           + COALESCE(SUM(octet_length(fragment)), 0)
                  INTO actual_aggregate_rows, actual_fragment_bytes
                  FROM {aggregate}
                 WHERE projection_id = NEW.projection_id;
                IF NEW.card_row_count IS DISTINCT FROM actual_card_rows
                   OR NEW.aggregate_row_count IS DISTINCT FROM actual_aggregate_rows
                   OR NEW.fragment_byte_count IS DISTINCT FROM actual_fragment_bytes
                THEN
                    RAISE EXCEPTION 'plan-pricing projection receipt counts do not match rows';
                END IF;
            ELSIF NEW.state = 'ready' AND NEW.contract_version = '{V3_CONTRACT}' THEN
                SELECT COUNT(*) INTO actual_provider_memberships
                  FROM {provider_membership}
                 WHERE projection_id = NEW.projection_id;
                SELECT COUNT(*), COALESCE(SUM(octet_length(fragment)), 0)
                  INTO actual_provider_cells, actual_provider_bytes
                  FROM {provider_cell}
                 WHERE projection_id = NEW.projection_id;
                SELECT COUNT(*) INTO actual_rate_profiles
                  FROM {rate_profile}
                 WHERE projection_id = NEW.projection_id;
                SELECT COALESCE(SUM(entry_count), 0), COUNT(*),
                       COALESCE(SUM(raw_byte_count), 0),
                       COALESCE(SUM(stored_byte_count), 0)
                  INTO actual_aggregate_entries, actual_aggregate_packs,
                       actual_aggregate_raw_bytes, actual_aggregate_stored_bytes
                  FROM {aggregate_pack}
                 WHERE projection_id = NEW.projection_id;
                SELECT COUNT(*) INTO actual_prewarm_shapes
                  FROM {prewarm_shape}
                 WHERE projection_id = NEW.projection_id;
                IF NEW.provider_membership_count IS DISTINCT FROM actual_provider_memberships
                   OR NEW.provider_cell_count IS DISTINCT FROM actual_provider_cells
                   OR NEW.provider_fragment_byte_count IS DISTINCT FROM actual_provider_bytes
                   OR NEW.rate_profile_count IS DISTINCT FROM actual_rate_profiles
                   OR NEW.aggregate_entry_count IS DISTINCT FROM actual_aggregate_entries
                   OR NEW.aggregate_pack_count IS DISTINCT FROM actual_aggregate_packs
                   OR NEW.aggregate_raw_byte_count IS DISTINCT FROM actual_aggregate_raw_bytes
                   OR NEW.aggregate_stored_byte_count IS DISTINCT FROM actual_aggregate_stored_bytes
                   OR NEW.prewarm_shape_count IS DISTINCT FROM actual_prewarm_shapes
                THEN
                    RAISE EXCEPTION 'factorized plan-pricing projection receipt counts do not match rows';
                END IF;
            END IF;
            RETURN NEW;
        END
        $$
    """


def _v2_candidate_guard_sql(schema: str) -> str:
    candidate = _table(schema, "plan_pricing_projection_candidate")
    card = _table(schema, "plan_pricing_card")
    aggregate = _table(schema, "plan_pricing_cell_aggregate")
    guard = _table(schema, "plan_pricing_projection_candidate_guard")
    return f"""
        CREATE OR REPLACE FUNCTION {guard}() RETURNS trigger
        LANGUAGE plpgsql AS $$
        DECLARE
            actual_card_rows bigint;
            actual_aggregate_rows bigint;
            actual_fragment_bytes bigint;
        BEGIN
            IF TG_OP <> 'INSERT' AND OLD.state = 'ready' THEN
                RAISE EXCEPTION 'ready plan-pricing projections are immutable';
            END IF;
            IF TG_OP = 'DELETE' THEN
                RETURN OLD;
            END IF;
            IF NEW.state = 'ready' THEN
                SELECT COUNT(*), COALESCE(SUM(octet_length(fragment)), 0)
                  INTO actual_card_rows, actual_fragment_bytes
                  FROM {card}
                 WHERE projection_id = NEW.projection_id;
                SELECT COUNT(*),
                       actual_fragment_bytes
                           + COALESCE(SUM(octet_length(fragment)), 0)
                  INTO actual_aggregate_rows, actual_fragment_bytes
                  FROM {aggregate}
                 WHERE projection_id = NEW.projection_id;
                IF NEW.card_row_count IS DISTINCT FROM actual_card_rows
                   OR NEW.aggregate_row_count IS DISTINCT FROM actual_aggregate_rows
                   OR NEW.fragment_byte_count IS DISTINCT FROM actual_fragment_bytes
                THEN
                    RAISE EXCEPTION 'plan-pricing projection receipt counts do not match rows';
                END IF;
            END IF;
            RETURN NEW;
        END
        $$
    """


def upgrade() -> None:
    """Add v3 storage while retaining every v2 row and reader."""

    schema_name = _schema()
    candidate = _table(schema_name, "plan_pricing_projection_candidate")
    provider_membership = _table(
        schema_name, "plan_pricing_provider_membership"
    )
    provider_cell = _table(schema_name, "plan_pricing_provider_cell")
    rate_profile = _table(schema_name, "plan_pricing_rate_profile")
    aggregate_pack = _table(schema_name, "plan_pricing_aggregate_pack")
    prewarm_shape = _table(schema_name, "plan_pricing_prewarm_shape")
    child_guard = _table(schema_name, "plan_pricing_projection_child_guard")
    truncate_guard = _table(
        schema_name, "plan_pricing_projection_truncate_guard"
    )

    op.execute(
        f"""ALTER TABLE {candidate}
        ADD COLUMN provider_membership_count bigint,
        ADD COLUMN provider_cell_count bigint,
        ADD COLUMN provider_fragment_byte_count bigint,
        ADD COLUMN rate_profile_count bigint,
        ADD COLUMN aggregate_entry_count bigint,
        ADD COLUMN aggregate_pack_count bigint,
        ADD COLUMN aggregate_raw_byte_count bigint,
        ADD COLUMN aggregate_stored_byte_count bigint,
        ADD COLUMN prewarm_shape_count integer,
        DROP CONSTRAINT plan_pricing_projection_contract_ck,
        DROP CONSTRAINT plan_pricing_projection_ready_ck,
        ADD CONSTRAINT plan_pricing_projection_contract_ck
          CHECK (contract_version IN ('{V2_CONTRACT}', '{V3_CONTRACT}')),
        ADD CONSTRAINT plan_pricing_projection_ready_ck CHECK (
          state <> 'ready' OR (
            content_digest IS NOT NULL
            AND content_digest ~ '^[0-9a-f]{{64}}$'
            AND build_seconds IS NOT NULL
            AND build_seconds >= 0
            AND completed_at IS NOT NULL
            AND (
              (contract_version = '{V2_CONTRACT}'
                AND card_row_count IS NOT NULL
                AND card_row_count >= 0
                AND aggregate_row_count IS NOT NULL
                AND aggregate_row_count >= 0
                AND fragment_byte_count IS NOT NULL
                AND fragment_byte_count >= 0)
              OR
              (contract_version = '{V3_CONTRACT}'
                AND provider_membership_count IS NOT NULL
                AND provider_membership_count >= 0
                AND provider_cell_count IS NOT NULL
                AND provider_cell_count >= 0
                AND provider_fragment_byte_count IS NOT NULL
                AND provider_fragment_byte_count >= 0
                AND rate_profile_count IS NOT NULL
                AND rate_profile_count >= 0
                AND aggregate_entry_count IS NOT NULL
                AND aggregate_entry_count >= 0
                AND aggregate_pack_count IS NOT NULL
                AND aggregate_pack_count >= 0
                AND aggregate_raw_byte_count IS NOT NULL
                AND aggregate_raw_byte_count >= 0
                AND aggregate_stored_byte_count IS NOT NULL
                AND aggregate_stored_byte_count >= 0
                AND prewarm_shape_count IS NOT NULL
                AND prewarm_shape_count BETWEEN 0 AND 768)
            )
          )
        )"""
    )
    op.execute(
        f"""CREATE TABLE {provider_membership} (
          projection_id varchar(64) NOT NULL REFERENCES {candidate}
            (projection_id) ON DELETE CASCADE,
          binding_ordinal integer NOT NULL,
          provider_set_key bigint NOT NULL,
          npi bigint NOT NULL,
          PRIMARY KEY (
            projection_id, binding_ordinal, provider_set_key, npi
          ),
          CONSTRAINT plan_pricing_provider_membership_ordinal_ck
            CHECK (binding_ordinal >= 0),
          CONSTRAINT plan_pricing_provider_membership_set_ck
            CHECK (provider_set_key >= 0),
          CONSTRAINT plan_pricing_provider_membership_npi_ck CHECK (npi > 0)
        )"""
    )
    op.execute(
        f"""CREATE INDEX plan_pricing_provider_membership_npi_idx
        ON {provider_membership} (
          projection_id, npi, binding_ordinal, provider_set_key
        )"""
    )
    op.execute(
        f"""CREATE TABLE {provider_cell} (
          projection_id varchar(64) NOT NULL REFERENCES {candidate}
            (projection_id) ON DELETE CASCADE,
          geo_cell varchar(5) NOT NULL,
          npi bigint NOT NULL,
          entity_type_code smallint,
          taxonomy_codes varchar[] NOT NULL,
          fragment bytea NOT NULL,
          PRIMARY KEY (projection_id, geo_cell, npi),
          CONSTRAINT plan_pricing_provider_cell_zip_ck
            CHECK (geo_cell ~ '^[0-9]{{5}}$'),
          CONSTRAINT plan_pricing_provider_cell_npi_ck CHECK (npi > 0),
          CONSTRAINT plan_pricing_provider_cell_fragment_ck
            CHECK (octet_length(fragment) BETWEEN 2 AND 2048)
        )"""
    )
    op.execute(
        f"""CREATE INDEX plan_pricing_provider_cell_npi_idx
        ON {provider_cell} (projection_id, npi, geo_cell)"""
    )
    op.execute(
        f"""CREATE TABLE {rate_profile} (
          projection_id varchar(64) NOT NULL REFERENCES {candidate}
            (projection_id) ON DELETE CASCADE,
          code_system varchar(64) NOT NULL,
          code varchar(128) NOT NULL,
          binding_ordinal integer NOT NULL,
          provider_set_key bigint NOT NULL,
          membership_count integer NOT NULL,
          minimum_negotiated_rate numeric NOT NULL,
          maximum_negotiated_rate numeric NOT NULL,
          rate_count bigint NOT NULL,
          negotiated_rates numeric[] NOT NULL,
          rate_multiplicities bigint[] NOT NULL,
          PRIMARY KEY (
            projection_id, code_system, code,
            binding_ordinal, provider_set_key
          ),
          CONSTRAINT plan_pricing_rate_profile_ordinal_ck
            CHECK (binding_ordinal >= 0),
          CONSTRAINT plan_pricing_rate_profile_set_ck
            CHECK (provider_set_key >= 0),
          CONSTRAINT plan_pricing_rate_profile_membership_ck
            CHECK (membership_count BETWEEN 1 AND 16384),
          CONSTRAINT plan_pricing_rate_profile_rates_ck CHECK (
            rate_count > 0
            AND cardinality(negotiated_rates) BETWEEN 1 AND 65536
            AND cardinality(negotiated_rates)
              = cardinality(rate_multiplicities)
            AND minimum_negotiated_rate = negotiated_rates[1]
            AND maximum_negotiated_rate
              = negotiated_rates[cardinality(negotiated_rates)]
            AND 0 < ALL(rate_multiplicities)
          )
        )"""
    )
    op.execute(
        f"""CREATE INDEX plan_pricing_rate_profile_cost_idx
        ON {rate_profile} (
          projection_id, code_system, code, minimum_negotiated_rate,
          binding_ordinal, provider_set_key
        ) INCLUDE (membership_count)"""
    )
    op.execute(
        f"""CREATE TABLE {aggregate_pack} (
          projection_id varchar(64) NOT NULL REFERENCES {candidate}
            (projection_id) ON DELETE CASCADE,
          code_system varchar(64) NOT NULL,
          code varchar(128) NOT NULL,
          zip_prefix_2 varchar(2) NOT NULL,
          entry_count integer NOT NULL,
          raw_byte_count integer NOT NULL,
          stored_byte_count integer NOT NULL,
          logical_digest varchar(64) NOT NULL,
          payload_sha256 bytea NOT NULL,
          payload bytea NOT NULL,
          PRIMARY KEY (projection_id, code_system, code, zip_prefix_2),
          CONSTRAINT plan_pricing_aggregate_pack_prefix_ck
            CHECK (zip_prefix_2 ~ '^[0-9]{{2}}$'),
          CONSTRAINT plan_pricing_aggregate_pack_entry_count_ck
            CHECK (entry_count BETWEEN 1 AND 1000),
          CONSTRAINT plan_pricing_aggregate_pack_raw_size_ck
            CHECK (raw_byte_count BETWEEN entry_count * 2 AND 557056),
          CONSTRAINT plan_pricing_aggregate_pack_stored_size_ck
            CHECK (stored_byte_count = octet_length(payload)
              AND stored_byte_count BETWEEN 45 AND 558124),
          CONSTRAINT plan_pricing_aggregate_pack_digest_ck
            CHECK (logical_digest ~ '^[0-9a-f]{{64}}$'),
          CONSTRAINT plan_pricing_aggregate_pack_payload_digest_ck
            CHECK (octet_length(payload_sha256) = 32
              AND payload_sha256 = pg_catalog.sha256(payload)),
          CONSTRAINT plan_pricing_aggregate_pack_frame_ck CHECK (
            CASE WHEN octet_length(payload) >= 44 THEN
              substring(payload FROM 1 FOR 8)
                = pg_catalog.decode('4850414747303100', 'hex')
              AND raw_byte_count = (
                pg_catalog.get_byte(payload, 8)::bigint * 16777216
                + pg_catalog.get_byte(payload, 9)::bigint * 65536
                + pg_catalog.get_byte(payload, 10)::bigint * 256
                + pg_catalog.get_byte(payload, 11)::bigint
              )
            ELSE FALSE END
          )
        )"""
    )
    op.execute(
        f"""CREATE TABLE {prewarm_shape} (
          projection_id varchar(64) NOT NULL REFERENCES {candidate}
            (projection_id) ON DELETE CASCADE,
          shape_rank smallint NOT NULL,
          code_system varchar(64) NOT NULL,
          code varchar(128) NOT NULL,
          geo_cell varchar(5) NOT NULL,
          provider_count bigint NOT NULL,
          PRIMARY KEY (projection_id, shape_rank),
          UNIQUE (projection_id, code_system, code, geo_cell),
          CONSTRAINT plan_pricing_prewarm_shape_rank_ck
            CHECK (shape_rank BETWEEN 1 AND 768),
          CONSTRAINT plan_pricing_prewarm_shape_zip_ck
            CHECK (geo_cell ~ '^[0-9]{{5}}$'),
          CONSTRAINT plan_pricing_prewarm_shape_provider_count_ck
            CHECK (provider_count > 0),
          CONSTRAINT plan_pricing_prewarm_shape_em_ck CHECK (NOT (
            code_system IN ('CPT', 'HCPCS')
            AND code ~ '^992(0[2-9]|1[0-5])$'
          ))
        )"""
    )
    op.execute(_candidate_guard_sql(schema_name))
    for table_name in (
        "plan_pricing_provider_membership",
        "plan_pricing_provider_cell",
        "plan_pricing_rate_profile",
        "plan_pricing_aggregate_pack",
        "plan_pricing_prewarm_shape",
    ):
        qualified_table = _table(schema_name, table_name)
        op.execute(
            f"""CREATE TRIGGER {table_name}_guard_trg
            BEFORE INSERT OR UPDATE OR DELETE ON {qualified_table}
            FOR EACH ROW EXECUTE FUNCTION {child_guard}()"""
        )
        op.execute(
            f"""CREATE TRIGGER {table_name}_truncate_guard_trg
            BEFORE TRUNCATE ON {qualified_table}
            FOR EACH STATEMENT EXECUTE FUNCTION {truncate_guard}()"""
        )


def downgrade() -> None:
    """Remove v3 storage only when no immutable v3 candidate exists."""

    schema_name = _schema()
    candidate = _table(schema_name, "plan_pricing_projection_candidate")
    op.execute(
        f"""DO $$
        BEGIN
          IF EXISTS (
            SELECT 1 FROM {candidate}
             WHERE contract_version = '{V3_CONTRACT}'
          ) THEN
            RAISE EXCEPTION 'cannot downgrade while factorized pricing projections exist';
          END IF;
        END $$"""
    )
    op.execute(f"DROP TABLE {_table(schema_name, 'plan_pricing_prewarm_shape')}")
    op.execute(f"DROP TABLE {_table(schema_name, 'plan_pricing_aggregate_pack')}")
    op.execute(f"DROP TABLE {_table(schema_name, 'plan_pricing_rate_profile')}")
    op.execute(
        f"DROP TABLE {_table(schema_name, 'plan_pricing_provider_cell')}"
    )
    op.execute(
        f"DROP TABLE {_table(schema_name, 'plan_pricing_provider_membership')}"
    )
    op.execute(
        f"""ALTER TABLE {candidate}
        DROP CONSTRAINT plan_pricing_projection_contract_ck,
        DROP CONSTRAINT plan_pricing_projection_ready_ck,
        DROP COLUMN aggregate_stored_byte_count,
        DROP COLUMN prewarm_shape_count,
        DROP COLUMN aggregate_raw_byte_count,
        DROP COLUMN aggregate_pack_count,
        DROP COLUMN aggregate_entry_count,
        DROP COLUMN provider_fragment_byte_count,
        DROP COLUMN rate_profile_count,
        DROP COLUMN provider_cell_count,
        DROP COLUMN provider_membership_count,
        ADD CONSTRAINT plan_pricing_projection_contract_ck
          CHECK (contract_version = '{V2_CONTRACT}'),
        ADD CONSTRAINT plan_pricing_projection_ready_ck CHECK (
          state <> 'ready' OR (
            content_digest IS NOT NULL
            AND content_digest ~ '^[0-9a-f]{{64}}$'
            AND card_row_count IS NOT NULL
            AND card_row_count >= 0
            AND aggregate_row_count IS NOT NULL
            AND aggregate_row_count >= 0
            AND fragment_byte_count IS NOT NULL
            AND fragment_byte_count >= 0
            AND build_seconds IS NOT NULL
            AND build_seconds >= 0
            AND completed_at IS NOT NULL
          )
        )"""
    )
    op.execute(_v2_candidate_guard_sql(schema_name))
