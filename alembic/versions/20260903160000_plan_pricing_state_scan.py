"""Add exact state-scan children to the factorized pricing projection.

Revision ID: 20260903160000_plan_pricing_state_scan
Revises: 20260903130000_hospital_price_csv_v1_labels
"""

from __future__ import annotations

import os

from alembic import op


revision = "20260903160000_plan_pricing_state_scan"
down_revision = "20260903130000_hospital_price_csv_v1_labels"
branch_labels = None
depends_on = None

V2_CONTRACT = "plan_pricing_card_v2"
V3_CONTRACT = "plan_pricing_factorized_v3"
V4_CONTRACT = "plan_pricing_factorized_v4"
MAX_PROVIDER_STATE_FRAGMENT_BYTES = 16 * 1024


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
    membership = _table(schema, "plan_pricing_provider_membership")
    provider_cell = _table(schema, "plan_pricing_provider_cell")
    provider_state = _table(schema, "plan_pricing_provider_state")
    rate_profile = _table(schema, "plan_pricing_rate_profile")
    rate_occurrence = _table(schema, "plan_pricing_rate_occurrence")
    aggregate_pack = _table(schema, "plan_pricing_aggregate_pack")
    prewarm = _table(schema, "plan_pricing_prewarm_shape")
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
            actual_provider_states bigint;
            actual_provider_state_bytes bigint;
            actual_rate_profiles bigint;
            actual_rate_occurrences bigint;
            actual_aggregate_entries bigint;
            actual_aggregate_packs bigint;
            actual_aggregate_raw_bytes bigint;
            actual_aggregate_stored_bytes bigint;
            actual_prewarm_shapes bigint;
        BEGIN
            IF TG_OP <> 'INSERT' AND OLD.state = 'ready' THEN
                RAISE EXCEPTION 'ready plan-pricing projections are immutable';
            END IF;
            IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
            IF NEW.state = 'ready' AND NEW.contract_version = '{V2_CONTRACT}' THEN
                SELECT COUNT(*), COALESCE(SUM(octet_length(fragment)), 0)
                  INTO actual_card_rows, actual_fragment_bytes
                  FROM {card} WHERE projection_id = NEW.projection_id;
                SELECT COUNT(*), actual_fragment_bytes
                       + COALESCE(SUM(octet_length(fragment)), 0)
                  INTO actual_aggregate_rows, actual_fragment_bytes
                  FROM {aggregate} WHERE projection_id = NEW.projection_id;
                IF NEW.card_row_count IS DISTINCT FROM actual_card_rows
                   OR NEW.aggregate_row_count IS DISTINCT FROM actual_aggregate_rows
                   OR NEW.fragment_byte_count IS DISTINCT FROM actual_fragment_bytes
                THEN
                    RAISE EXCEPTION 'plan-pricing projection receipt counts do not match rows';
                END IF;
            ELSIF NEW.state = 'ready'
              AND NEW.contract_version IN ('{V3_CONTRACT}', '{V4_CONTRACT}') THEN
                SELECT COUNT(*) INTO actual_provider_memberships
                  FROM {membership} WHERE projection_id = NEW.projection_id;
                SELECT COUNT(*), COALESCE(SUM(octet_length(fragment)), 0)
                  INTO actual_provider_cells, actual_provider_bytes
                  FROM {provider_cell} WHERE projection_id = NEW.projection_id;
                IF NEW.contract_version = '{V4_CONTRACT}' THEN
                    SELECT COUNT(*),
                           COALESCE(SUM(octet_length(provider_fragment)), 0)
                      INTO actual_provider_states, actual_provider_state_bytes
                      FROM {provider_state}
                     WHERE projection_id = NEW.projection_id;
                    actual_provider_bytes := actual_provider_bytes
                        + actual_provider_state_bytes;
                END IF;
                SELECT COUNT(*) INTO actual_rate_profiles
                  FROM {rate_profile} WHERE projection_id = NEW.projection_id;
                SELECT COALESCE(SUM(entry_count), 0), COUNT(*),
                       COALESCE(SUM(raw_byte_count), 0),
                       COALESCE(SUM(stored_byte_count), 0)
                  INTO actual_aggregate_entries, actual_aggregate_packs,
                       actual_aggregate_raw_bytes, actual_aggregate_stored_bytes
                  FROM {aggregate_pack} WHERE projection_id = NEW.projection_id;
                SELECT COUNT(*) INTO actual_prewarm_shapes
                  FROM {prewarm} WHERE projection_id = NEW.projection_id;
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
                IF NEW.contract_version = '{V4_CONTRACT}' THEN
                    SELECT COUNT(*) INTO actual_rate_occurrences
                      FROM {rate_occurrence} WHERE projection_id = NEW.projection_id;
                    IF NEW.provider_state_count IS DISTINCT FROM actual_provider_states
                       OR NEW.rate_occurrence_count IS DISTINCT FROM actual_rate_occurrences
                    THEN
                        RAISE EXCEPTION 'state-scan projection receipt counts do not match rows';
                    END IF;
                    IF EXISTS (
                        (SELECT DISTINCT upper(convert_from(fragment, 'UTF8')::jsonb ->> 'state') AS state, npi
                           FROM {provider_cell}
                          WHERE projection_id = NEW.projection_id
                            AND upper(convert_from(fragment, 'UTF8')::jsonb ->> 'state') ~ '^[A-Z]{{2}}$'
                         EXCEPT
                         SELECT state, npi FROM {provider_state}
                          WHERE projection_id = NEW.projection_id)
                        UNION ALL
                        (SELECT state, npi FROM {provider_state}
                          WHERE projection_id = NEW.projection_id
                         EXCEPT
                         SELECT DISTINCT upper(convert_from(fragment, 'UTF8')::jsonb ->> 'state'), npi
                           FROM {provider_cell}
                          WHERE projection_id = NEW.projection_id
                            AND upper(convert_from(fragment, 'UTF8')::jsonb ->> 'state') ~ '^[A-Z]{{2}}$')
                    ) THEN
                        RAISE EXCEPTION 'provider-state index is incomplete';
                    END IF;
                    IF EXISTS (
                        SELECT 1 FROM {rate_occurrence} occurrence
                        LEFT JOIN {rate_profile} profile
                          ON profile.projection_id = occurrence.projection_id
                         AND profile.code_system = occurrence.code_system
                         AND profile.code = occurrence.code
                         AND profile.binding_ordinal = occurrence.binding_ordinal
                         AND profile.provider_set_key = occurrence.provider_set_key
                       WHERE occurrence.projection_id = NEW.projection_id
                         AND profile.projection_id IS NULL
                    ) OR EXISTS (
                        SELECT 1 FROM {rate_profile} profile
                        LEFT JOIN {rate_occurrence} occurrence
                          ON occurrence.projection_id = profile.projection_id
                         AND occurrence.code_system = profile.code_system
                         AND occurrence.code = profile.code
                         AND occurrence.binding_ordinal = profile.binding_ordinal
                         AND occurrence.provider_set_key = profile.provider_set_key
                       WHERE profile.projection_id = NEW.projection_id
                         AND occurrence.projection_id IS NULL
                    ) THEN
                        RAISE EXCEPTION 'rate-occurrence index is incomplete';
                    END IF;
                END IF;
            END IF;
            RETURN NEW;
        END
        $$
    """


def _v3_candidate_guard_sql(schema: str) -> str:
    """Restore the exact v2/v3 guard before removing v4 relations."""

    candidate = _table(schema, "plan_pricing_projection_candidate")
    card = _table(schema, "plan_pricing_card")
    aggregate = _table(schema, "plan_pricing_cell_aggregate")
    membership = _table(schema, "plan_pricing_provider_membership")
    provider_cell = _table(schema, "plan_pricing_provider_cell")
    rate_profile = _table(schema, "plan_pricing_rate_profile")
    aggregate_pack = _table(schema, "plan_pricing_aggregate_pack")
    prewarm = _table(schema, "plan_pricing_prewarm_shape")
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
            IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
            IF NEW.state = 'ready' AND NEW.contract_version = '{V2_CONTRACT}' THEN
                SELECT COUNT(*), COALESCE(SUM(octet_length(fragment)), 0)
                  INTO actual_card_rows, actual_fragment_bytes
                  FROM {card} WHERE projection_id = NEW.projection_id;
                SELECT COUNT(*), actual_fragment_bytes
                       + COALESCE(SUM(octet_length(fragment)), 0)
                  INTO actual_aggregate_rows, actual_fragment_bytes
                  FROM {aggregate} WHERE projection_id = NEW.projection_id;
                IF NEW.card_row_count IS DISTINCT FROM actual_card_rows
                   OR NEW.aggregate_row_count IS DISTINCT FROM actual_aggregate_rows
                   OR NEW.fragment_byte_count IS DISTINCT FROM actual_fragment_bytes
                THEN
                    RAISE EXCEPTION 'plan-pricing projection receipt counts do not match rows';
                END IF;
            ELSIF NEW.state = 'ready' AND NEW.contract_version = '{V3_CONTRACT}' THEN
                SELECT COUNT(*) INTO actual_provider_memberships
                  FROM {membership} WHERE projection_id = NEW.projection_id;
                SELECT COUNT(*), COALESCE(SUM(octet_length(fragment)), 0)
                  INTO actual_provider_cells, actual_provider_bytes
                  FROM {provider_cell} WHERE projection_id = NEW.projection_id;
                SELECT COUNT(*) INTO actual_rate_profiles
                  FROM {rate_profile} WHERE projection_id = NEW.projection_id;
                SELECT COALESCE(SUM(entry_count), 0), COUNT(*),
                       COALESCE(SUM(raw_byte_count), 0),
                       COALESCE(SUM(stored_byte_count), 0)
                  INTO actual_aggregate_entries, actual_aggregate_packs,
                       actual_aggregate_raw_bytes, actual_aggregate_stored_bytes
                  FROM {aggregate_pack} WHERE projection_id = NEW.projection_id;
                SELECT COUNT(*) INTO actual_prewarm_shapes
                  FROM {prewarm} WHERE projection_id = NEW.projection_id;
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


def upgrade() -> None:
    schema = _schema()
    candidate = _table(schema, "plan_pricing_projection_candidate")
    provider_state = _table(schema, "plan_pricing_provider_state")
    occurrence = _table(schema, "plan_pricing_rate_occurrence")
    child_guard = _table(schema, "plan_pricing_projection_child_guard")
    truncate_guard = _table(schema, "plan_pricing_projection_truncate_guard")
    op.execute(
        f"""ALTER TABLE {candidate}
        ADD COLUMN provider_state_count bigint,
        ADD COLUMN rate_occurrence_count bigint,
        DROP CONSTRAINT plan_pricing_projection_contract_ck,
        DROP CONSTRAINT plan_pricing_projection_ready_ck,
        ADD CONSTRAINT plan_pricing_projection_contract_ck CHECK (
          contract_version IN ('{V2_CONTRACT}', '{V3_CONTRACT}', '{V4_CONTRACT}')
        ),
        ADD CONSTRAINT plan_pricing_projection_ready_ck CHECK (
          state <> 'ready' OR (
            content_digest IS NOT NULL
            AND content_digest ~ '^[0-9a-f]{{64}}$'
            AND build_seconds IS NOT NULL AND build_seconds >= 0
            AND completed_at IS NOT NULL
            AND (
              (contract_version = '{V2_CONTRACT}'
                AND card_row_count IS NOT NULL AND card_row_count >= 0
                AND aggregate_row_count IS NOT NULL AND aggregate_row_count >= 0
                AND fragment_byte_count IS NOT NULL AND fragment_byte_count >= 0)
              OR
              (contract_version IN ('{V3_CONTRACT}', '{V4_CONTRACT}')
                AND provider_membership_count IS NOT NULL AND provider_membership_count >= 0
                AND provider_cell_count IS NOT NULL AND provider_cell_count >= 0
                AND provider_fragment_byte_count IS NOT NULL AND provider_fragment_byte_count >= 0
                AND rate_profile_count IS NOT NULL AND rate_profile_count >= 0
                AND aggregate_entry_count IS NOT NULL AND aggregate_entry_count >= 0
                AND aggregate_pack_count IS NOT NULL AND aggregate_pack_count >= 0
                AND aggregate_raw_byte_count IS NOT NULL AND aggregate_raw_byte_count >= 0
                AND aggregate_stored_byte_count IS NOT NULL AND aggregate_stored_byte_count >= 0
                AND prewarm_shape_count IS NOT NULL AND prewarm_shape_count BETWEEN 0 AND 768
                AND (contract_version <> '{V4_CONTRACT}' OR (
                  provider_state_count IS NOT NULL AND provider_state_count >= 0
                  AND rate_occurrence_count IS NOT NULL AND rate_occurrence_count >= 0
                )))
            )
          )
        )"""
    )
    op.execute(
        f"""CREATE TABLE {provider_state} (
          projection_id varchar(64) NOT NULL REFERENCES {candidate}
            (projection_id) ON DELETE CASCADE,
          state varchar(2) NOT NULL,
          npi bigint NOT NULL,
          provider_fragment bytea NOT NULL,
          PRIMARY KEY (projection_id, state, npi),
          CONSTRAINT plan_pricing_provider_state_state_ck
            CHECK (state ~ '^[A-Z]{{2}}$'),
          CONSTRAINT plan_pricing_provider_state_npi_ck CHECK (npi > 0),
          CONSTRAINT plan_pricing_provider_state_fragment_ck CHECK (
            octet_length(provider_fragment) BETWEEN 2
            AND {MAX_PROVIDER_STATE_FRAGMENT_BYTES}
          )
        )"""
    )
    op.execute(
        f"""CREATE TABLE {occurrence} (
          projection_id varchar(64) NOT NULL REFERENCES {candidate}
            (projection_id) ON DELETE CASCADE,
          code_system varchar(64) NOT NULL,
          code varchar(128) NOT NULL,
          binding_ordinal integer NOT NULL,
          occurrence_ordinal integer NOT NULL,
          provider_set_key bigint NOT NULL,
          provider_set_ref varchar(32) NOT NULL,
          price_key bigint NOT NULL,
          price_set_ref varchar(32) NOT NULL,
          rate_pack_ref varchar(32) NOT NULL,
          source_artifact_key bigint NOT NULL,
          provider_count integer NOT NULL,
          group_fragment jsonb NOT NULL,
          occurrence_multiplicity bigint NOT NULL,
          PRIMARY KEY (
            projection_id, code_system, code,
            binding_ordinal, occurrence_ordinal
          ),
          CONSTRAINT plan_pricing_rate_occurrence_ordinal_ck CHECK (
            binding_ordinal >= 0 AND occurrence_ordinal >= 0
          ),
          CONSTRAINT plan_pricing_rate_occurrence_keys_ck CHECK (
            provider_set_key >= 0 AND price_key >= 0
            AND source_artifact_key >= 0 AND provider_count >= 0
            AND occurrence_multiplicity > 0
          ),
          CONSTRAINT plan_pricing_rate_occurrence_refs_ck CHECK (
            provider_set_ref ~ '^[0-9a-f]{{32}}$'
            AND price_set_ref ~ '^[0-9a-f]{{32}}$'
            AND rate_pack_ref ~ '^[0-9a-f]{{32}}$'
          ),
          CONSTRAINT plan_pricing_rate_occurrence_fragment_ck CHECK (
            jsonb_typeof(group_fragment) = 'object'
            AND octet_length(group_fragment::text) BETWEEN 2 AND 8192
          )
        )"""
    )
    op.execute(
        f"""CREATE INDEX plan_pricing_rate_occurrence_set_idx
        ON {occurrence} (
          projection_id, code_system, code,
          binding_ordinal, provider_set_key, occurrence_ordinal
        )"""
    )
    op.execute(_candidate_guard_sql(schema))
    for table_name in (
        "plan_pricing_provider_state",
        "plan_pricing_rate_occurrence",
    ):
        qualified = _table(schema, table_name)
        op.execute(
            f"""CREATE TRIGGER {table_name}_guard_trg
            BEFORE INSERT OR UPDATE OR DELETE ON {qualified}
            FOR EACH ROW EXECUTE FUNCTION {child_guard}()"""
        )
        op.execute(
            f"""CREATE TRIGGER {table_name}_truncate_guard_trg
            BEFORE TRUNCATE ON {qualified}
            FOR EACH STATEMENT EXECUTE FUNCTION {truncate_guard}()"""
        )


def downgrade() -> None:
    schema = _schema()
    candidate = _table(schema, "plan_pricing_projection_candidate")
    op.execute(
        f"""DO $$ BEGIN
          IF EXISTS (SELECT 1 FROM {candidate}
                      WHERE contract_version = '{V4_CONTRACT}') THEN
            RAISE EXCEPTION 'cannot downgrade while v4 pricing projections exist';
          END IF;
        END $$"""
    )
    op.execute(_v3_candidate_guard_sql(schema))
    op.execute(f"DROP TABLE {_table(schema, 'plan_pricing_rate_occurrence')}")
    op.execute(f"DROP TABLE {_table(schema, 'plan_pricing_provider_state')}")
    op.execute(
        f"""ALTER TABLE {candidate}
        DROP CONSTRAINT plan_pricing_projection_contract_ck,
        DROP CONSTRAINT plan_pricing_projection_ready_ck,
        DROP COLUMN provider_state_count,
        DROP COLUMN rate_occurrence_count,
        ADD CONSTRAINT plan_pricing_projection_contract_ck CHECK (
          contract_version IN ('{V2_CONTRACT}', '{V3_CONTRACT}')
        ),
        ADD CONSTRAINT plan_pricing_projection_ready_ck CHECK (
          state <> 'ready' OR (
            content_digest IS NOT NULL
            AND content_digest ~ '^[0-9a-f]{{64}}$'
            AND build_seconds IS NOT NULL AND build_seconds >= 0
            AND completed_at IS NOT NULL
            AND (
              (contract_version = '{V2_CONTRACT}'
                AND card_row_count IS NOT NULL AND card_row_count >= 0
                AND aggregate_row_count IS NOT NULL AND aggregate_row_count >= 0
                AND fragment_byte_count IS NOT NULL AND fragment_byte_count >= 0)
              OR
              (contract_version = '{V3_CONTRACT}'
                AND provider_membership_count IS NOT NULL AND provider_membership_count >= 0
                AND provider_cell_count IS NOT NULL AND provider_cell_count >= 0
                AND provider_fragment_byte_count IS NOT NULL AND provider_fragment_byte_count >= 0
                AND rate_profile_count IS NOT NULL AND rate_profile_count >= 0
                AND aggregate_entry_count IS NOT NULL AND aggregate_entry_count >= 0
                AND aggregate_pack_count IS NOT NULL AND aggregate_pack_count >= 0
                AND aggregate_raw_byte_count IS NOT NULL AND aggregate_raw_byte_count >= 0
                AND aggregate_stored_byte_count IS NOT NULL AND aggregate_stored_byte_count >= 0
                AND prewarm_shape_count IS NOT NULL AND prewarm_shape_count BETWEEN 0 AND 768)
            )
          )
        )"""
    )
