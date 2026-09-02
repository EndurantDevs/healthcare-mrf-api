# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Add an immutable serving-revision-bound E&M distance projection.

Revision ID: 20260901103000_plan_pricing_em_distance
Revises: 20260901000000_hospital_price_csv_short_v2
"""

from __future__ import annotations

import os

from alembic import op


revision = "20260901103000_plan_pricing_em_distance"
down_revision = "20260901000000_hospital_price_csv_short_v2"
branch_labels = None
depends_on = None


CONTRACT = "plan_pricing_em_distance_v1"
IDEMPOTENCY_INDEX_NAME = "import_run_plan_pricing_idempotency_idx"


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


def upgrade() -> None:
    """Install dormant projection storage without changing a serving pointer."""

    schema = _schema()
    import_run = _table(schema, "import_run")
    revision_table = _table(schema, "plan_release_serving_revision")
    candidate = _table(schema, "plan_pricing_em_distance_candidate")
    attachment = _table(schema, "plan_pricing_em_distance_attachment")
    rate = _table(schema, "plan_pricing_em_distance_rate")
    location = _table(schema, "plan_pricing_em_distance_location")
    rates_valid = _table(schema, "plan_pricing_em_distance_rates_valid")
    candidate_guard = _table(
        schema, "plan_pricing_em_distance_candidate_guard"
    )
    child_guard = _table(schema, "plan_pricing_em_distance_child_guard")
    attachment_guard = _table(
        schema, "plan_pricing_em_distance_attachment_guard"
    )
    truncate_guard = _table(
        schema, "plan_pricing_em_distance_truncate_guard"
    )

    op.execute("CREATE EXTENSION IF NOT EXISTS btree_gist WITH SCHEMA public")
    op.execute(f"DROP INDEX IF EXISTS {_table(schema, IDEMPOTENCY_INDEX_NAME)}")
    op.execute(
        f"""
        CREATE UNIQUE INDEX {IDEMPOTENCY_INDEX_NAME}
            ON {import_run} (importer, idempotency_key)
         WHERE importer IN (
                   'plan-pricing-projection',
                   'plan-pricing-prewarm',
                   'plan-pricing-em-distance'
               )
           AND idempotency_key IS NOT NULL
        """
    )
    statements = (
        f"""
        CREATE TABLE {candidate} (
            projection_id varchar(64) PRIMARY KEY,
            contract_version varchar(64) NOT NULL,
            plan_release_id varchar(64) NOT NULL,
            serving_revision_id varchar(64) NOT NULL REFERENCES
                {revision_table} (serving_revision_id) ON DELETE RESTRICT,
            binding_set_digest varchar(64) NOT NULL,
            provider_signature varchar(64) NOT NULL,
            state varchar(16) NOT NULL,
            content_digest varchar(64),
            rate_row_count bigint,
            location_row_count bigint,
            build_seconds numeric,
            created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
            completed_at timestamptz,
            CONSTRAINT plan_pricing_em_distance_candidate_id_ck
                CHECK (projection_id ~ '^[0-9a-f]{{64}}$'),
            CONSTRAINT plan_pricing_em_distance_candidate_contract_ck
                CHECK (contract_version = '{CONTRACT}'),
            CONSTRAINT plan_pricing_em_distance_candidate_release_ck
                CHECK (
                    plan_release_id
                        ~ '^hprelease_[0-9A-HJKMNP-TV-Z]{{26}}$'
                ),
            CONSTRAINT plan_pricing_em_distance_candidate_revision_ck
                CHECK (
                    serving_revision_id
                        ~ '^hpserve_[0-9A-HJKMNP-TV-Z]{{26}}$'
                ),
            CONSTRAINT plan_pricing_em_distance_candidate_binding_ck
                CHECK (binding_set_digest ~ '^[0-9a-f]{{64}}$'),
            CONSTRAINT plan_pricing_em_distance_candidate_provider_ck
                CHECK (provider_signature ~ '^[0-9a-f]{{64}}$'),
            CONSTRAINT plan_pricing_em_distance_candidate_state_ck
                CHECK (state IN ('building', 'ready')),
            CONSTRAINT plan_pricing_em_distance_candidate_lifecycle_ck CHECK (
                (state = 'building'
                    AND content_digest IS NULL
                    AND rate_row_count IS NULL
                    AND location_row_count IS NULL
                    AND build_seconds IS NULL
                    AND completed_at IS NULL)
                OR
                (state = 'ready'
                    AND content_digest IS NOT NULL
                    AND content_digest ~ '^[0-9a-f]{{64}}$'
                    AND rate_row_count > 0
                    AND location_row_count > 0
                    AND build_seconds IS NOT NULL
                    AND build_seconds >= 0
                    AND completed_at IS NOT NULL)
            ),
            CONSTRAINT plan_pricing_em_distance_candidate_identity_uq UNIQUE (
                contract_version,
                serving_revision_id,
                binding_set_digest,
                provider_signature
            )
        )
        """,
        f"""
        CREATE FUNCTION {rates_valid}(
            minimums numeric[], maximums numeric[], counts bigint[],
            mask_value smallint
        ) RETURNS boolean
        LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $$
            SELECT cardinality(minimums) = 6
               AND cardinality(maximums) = 6
               AND cardinality(counts) = 6
               AND array_ndims(minimums) = 1
               AND array_ndims(maximums) = 1
               AND array_ndims(counts) = 1
               AND array_lower(minimums, 1) = 1
               AND array_lower(maximums, 1) = 1
               AND array_lower(counts, 1) = 1
               AND mask_value BETWEEN 1 AND 63
               AND NOT EXISTS (
                    SELECT 1
                      FROM generate_series(1, 6) AS slot(position)
                     WHERE (
                         (mask_value::integer
                            & (1 << (slot.position - 1))) <> 0
                     ) IS DISTINCT FROM (
                         minimums[slot.position] IS NOT NULL
                     )
                        OR (
                            (mask_value::integer
                                & (1 << (slot.position - 1))) <> 0
                        ) IS DISTINCT FROM (
                            maximums[slot.position] IS NOT NULL
                        )
                        OR (
                            (mask_value::integer
                                & (1 << (slot.position - 1))) <> 0
                        ) IS DISTINCT FROM (
                            counts[slot.position] IS NOT NULL
                        )
                        OR (
                            (mask_value::integer
                                & (1 << (slot.position - 1))) <> 0
                            AND NOT (
                                minimums[slot.position] >= 0
                                AND maximums[slot.position]
                                    >= minimums[slot.position]
                                AND counts[slot.position] > 0
                                AND minimums[slot.position]::text NOT IN (
                                    'NaN', 'Infinity', '-Infinity'
                                )
                                AND maximums[slot.position]::text NOT IN (
                                    'NaN', 'Infinity', '-Infinity'
                                )
                            )
                        )
               )
        $$
        """,
        f"""
        CREATE TABLE {rate} (
            projection_id varchar(64) NOT NULL REFERENCES {candidate}
                (projection_id) ON DELETE CASCADE,
            npi bigint NOT NULL,
            code_mask smallint NOT NULL,
            minimum_rates numeric[] NOT NULL,
            maximum_rates numeric[] NOT NULL,
            rate_counts bigint[] NOT NULL,
            PRIMARY KEY (projection_id, npi),
            CONSTRAINT plan_pricing_em_distance_rate_npi_ck CHECK (npi > 0),
            CONSTRAINT plan_pricing_em_distance_rate_values_ck CHECK (
                {rates_valid}(
                    minimum_rates, maximum_rates, rate_counts, code_mask
                )
            )
        )
        """,
        f"""
        CREATE TABLE {location} (
            projection_id varchar(64) NOT NULL REFERENCES {candidate}
                (projection_id) ON DELETE CASCADE,
            npi bigint NOT NULL,
            location_key varchar(64) NOT NULL,
            address_checksum bigint NOT NULL,
            address_type_rank smallint NOT NULL,
            geo_evidence_level varchar(64) NOT NULL,
            address_precision varchar(32) NOT NULL,
            point public.geography(Point, 4326) NOT NULL,
            provider_name text NOT NULL,
            entity_type_code smallint,
            credential text,
            taxonomy_code varchar(32),
            primary_specialty text,
            classification text,
            city text,
            state varchar(2),
            zip5 varchar(5),
            PRIMARY KEY (projection_id, npi, location_key),
            CONSTRAINT plan_pricing_em_distance_location_npi_ck
                CHECK (npi > 0),
            CONSTRAINT plan_pricing_em_distance_location_key_ck
                CHECK (btrim(location_key) <> ''),
            CONSTRAINT plan_pricing_em_distance_location_rank_ck
                CHECK (address_type_rank BETWEEN 0 AND 4),
            CONSTRAINT plan_pricing_em_distance_location_evidence_ck CHECK (
                geo_evidence_level IN (
                    'nppes_registry_address',
                    'multi_issuer_marketplace_address',
                    'cms_doctors_source_with_nppes_identity_anchor'
                )
            ),
            CONSTRAINT plan_pricing_em_distance_location_precision_ck CHECK (
                btrim(address_precision) <> ''
                AND address_precision <> 'city_zip'
            ),
            CONSTRAINT plan_pricing_em_distance_location_point_ck CHECK (
                NOT public.ST_IsEmpty(point::public.geometry)
                AND public.ST_SRID(point::public.geometry) = 4326
                AND public.ST_X(point::public.geometry) BETWEEN -180 AND 180
                AND public.ST_Y(point::public.geometry) BETWEEN -90 AND 90
            ),
            CONSTRAINT plan_pricing_em_distance_location_name_ck
                CHECK (btrim(provider_name) <> ''),
            CONSTRAINT plan_pricing_em_distance_location_entity_ck
                CHECK (entity_type_code IS NULL OR entity_type_code IN (1, 2)),
            CONSTRAINT plan_pricing_em_distance_location_state_ck
                CHECK (state IS NULL OR state ~ '^[A-Z]{{2}}$'),
            CONSTRAINT plan_pricing_em_distance_location_zip_ck
                CHECK (zip5 IS NULL OR zip5 ~ '^[0-9]{{5}}$')
        )
        """,
        f"""
        CREATE INDEX plan_pricing_em_distance_location_point_idx
            ON {location} USING gist (projection_id, point)
        """,
        f"""
        CREATE TABLE {attachment} (
            serving_revision_id varchar(64) PRIMARY KEY REFERENCES
                {revision_table} (serving_revision_id) ON DELETE RESTRICT,
            projection_id varchar(64) NOT NULL UNIQUE REFERENCES {candidate}
                (projection_id) ON DELETE RESTRICT,
            attached_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
            CONSTRAINT plan_pricing_em_distance_attachment_revision_ck CHECK (
                serving_revision_id
                    ~ '^hpserve_[0-9A-HJKMNP-TV-Z]{{26}}$'
            )
        )
        """,
        f"""
        CREATE FUNCTION {candidate_guard}() RETURNS trigger
        LANGUAGE plpgsql AS $$
        DECLARE
            actual_rate_rows bigint;
            actual_location_rows bigint;
        BEGIN
            IF TG_OP = 'DELETE' THEN
                IF OLD.state = 'ready' THEN
                    RAISE EXCEPTION
                        'ready E&M distance projections are immutable'
                        USING ERRCODE = '55000';
                END IF;
                RETURN OLD;
            END IF;
            IF TG_OP = 'UPDATE' THEN
                IF OLD.state = 'ready' THEN
                    RAISE EXCEPTION
                        'ready E&M distance projections are immutable'
                        USING ERRCODE = '55000';
                END IF;
                IF ROW(
                    NEW.projection_id,
                    NEW.contract_version,
                    NEW.plan_release_id,
                    NEW.serving_revision_id,
                    NEW.binding_set_digest,
                    NEW.provider_signature
                ) IS DISTINCT FROM ROW(
                    OLD.projection_id,
                    OLD.contract_version,
                    OLD.plan_release_id,
                    OLD.serving_revision_id,
                    OLD.binding_set_digest,
                    OLD.provider_signature
                ) THEN
                    RAISE EXCEPTION
                        'E&M distance projection identity is immutable'
                        USING ERRCODE = '55000';
                END IF;
            END IF;
            IF NOT EXISTS (
                SELECT 1
                  FROM {revision_table} revision_record
                 WHERE revision_record.serving_revision_id
                           = NEW.serving_revision_id
                   AND revision_record.plan_release_id = NEW.plan_release_id
                   AND revision_record.binding_set_digest
                           = NEW.binding_set_digest
            ) THEN
                RAISE EXCEPTION
                    'E&M distance projection serving lineage does not match'
                    USING ERRCODE = '23514';
            END IF;
            IF NEW.state = 'ready' THEN
                SELECT COUNT(*) INTO actual_rate_rows
                  FROM {rate}
                 WHERE projection_id = NEW.projection_id;
                SELECT COUNT(*) INTO actual_location_rows
                  FROM {location}
                 WHERE projection_id = NEW.projection_id;
                IF NEW.rate_row_count IS DISTINCT FROM actual_rate_rows
                   OR NEW.location_row_count
                        IS DISTINCT FROM actual_location_rows
                THEN
                    RAISE EXCEPTION
                        'E&M distance projection receipt counts do not match rows'
                        USING ERRCODE = '23514';
                END IF;
            END IF;
            RETURN NEW;
        END
        $$
        """,
        f"""
        CREATE FUNCTION {child_guard}() RETURNS trigger
        LANGUAGE plpgsql AS $$
        DECLARE
            parent_state text;
        BEGIN
            IF TG_OP = 'INSERT' THEN
                SELECT state INTO parent_state
                  FROM {candidate}
                 WHERE projection_id = NEW.projection_id
                   FOR UPDATE;
            ELSIF TG_OP = 'DELETE' THEN
                SELECT state INTO parent_state
                  FROM {candidate}
                 WHERE projection_id = OLD.projection_id
                   FOR UPDATE;
            ELSE
                PERFORM 1
                  FROM {candidate}
                 WHERE projection_id IN (
                           OLD.projection_id, NEW.projection_id
                       )
                 ORDER BY projection_id
                   FOR UPDATE;
                IF EXISTS (
                    SELECT 1
                      FROM {candidate}
                     WHERE state = 'ready'
                       AND projection_id IN (
                               OLD.projection_id, NEW.projection_id
                           )
                ) THEN
                    RAISE EXCEPTION
                        'ready E&M distance projection rows are immutable'
                        USING ERRCODE = '55000';
                END IF;
                RETURN NEW;
            END IF;
            IF parent_state = 'ready' THEN
                RAISE EXCEPTION
                    'ready E&M distance projection rows are immutable'
                    USING ERRCODE = '55000';
            END IF;
            IF TG_OP = 'DELETE' THEN
                RETURN OLD;
            END IF;
            RETURN NEW;
        END
        $$
        """,
        f"""
        CREATE FUNCTION {attachment_guard}() RETURNS trigger
        LANGUAGE plpgsql AS $$
        DECLARE
            candidate_state text;
        BEGIN
            IF TG_OP <> 'INSERT' THEN
                RAISE EXCEPTION
                    'E&M distance projection attachments are immutable'
                    USING ERRCODE = '55000';
            END IF;
            SELECT candidate_record.state INTO candidate_state
              FROM {candidate} candidate_record
              JOIN {revision_table} revision_record
                ON revision_record.serving_revision_id
                       = NEW.serving_revision_id
             WHERE candidate_record.projection_id = NEW.projection_id
               AND candidate_record.serving_revision_id
                       = NEW.serving_revision_id
               AND candidate_record.plan_release_id
                       = revision_record.plan_release_id
               AND candidate_record.binding_set_digest
                       = revision_record.binding_set_digest
               FOR SHARE OF candidate_record, revision_record;
            IF candidate_state IS DISTINCT FROM 'ready' THEN
                RAISE EXCEPTION
                    'E&M distance projection attachment requires an exact ready candidate'
                    USING ERRCODE = '23514';
            END IF;
            RETURN NEW;
        END
        $$
        """,
        f"""
        CREATE FUNCTION {truncate_guard}() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM {candidate} WHERE state = 'ready'
            ) OR EXISTS (SELECT 1 FROM {attachment}) THEN
                RAISE EXCEPTION
                    'ready E&M distance projections cannot be truncated'
                    USING ERRCODE = '55000';
            END IF;
            RETURN NULL;
        END
        $$
        """,
        f"""
        CREATE TRIGGER plan_pricing_em_distance_candidate_guard_trg
        BEFORE INSERT OR UPDATE OR DELETE ON {candidate}
        FOR EACH ROW EXECUTE FUNCTION {candidate_guard}()
        """,
        f"""
        CREATE TRIGGER plan_pricing_em_distance_rate_guard_trg
        BEFORE INSERT OR UPDATE OR DELETE ON {rate}
        FOR EACH ROW EXECUTE FUNCTION {child_guard}()
        """,
        f"""
        CREATE TRIGGER plan_pricing_em_distance_location_guard_trg
        BEFORE INSERT OR UPDATE OR DELETE ON {location}
        FOR EACH ROW EXECUTE FUNCTION {child_guard}()
        """,
        f"""
        CREATE TRIGGER plan_pricing_em_distance_attachment_guard_trg
        BEFORE INSERT OR UPDATE OR DELETE ON {attachment}
        FOR EACH ROW EXECUTE FUNCTION {attachment_guard}()
        """,
    )
    for statement in statements:
        op.execute(statement)
    for table_name in (
        "plan_pricing_em_distance_candidate",
        "plan_pricing_em_distance_rate",
        "plan_pricing_em_distance_location",
        "plan_pricing_em_distance_attachment",
    ):
        op.execute(
            f"""
            CREATE TRIGGER {table_name}_truncate_guard_trg
            BEFORE TRUNCATE ON {_table(schema, table_name)}
            FOR EACH STATEMENT EXECUTE FUNCTION {truncate_guard}()
            """
        )


def downgrade() -> None:
    """Refuse removal once any projection lifecycle row exists."""

    schema = _schema()
    import_run = _table(schema, "import_run")
    candidate = _table(schema, "plan_pricing_em_distance_candidate")
    attachment = _table(schema, "plan_pricing_em_distance_attachment")
    rate = _table(schema, "plan_pricing_em_distance_rate")
    location = _table(schema, "plan_pricing_em_distance_location")
    op.execute(
        f"""
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM {candidate})
               OR EXISTS (SELECT 1 FROM {attachment})
               OR EXISTS (SELECT 1 FROM {rate})
               OR EXISTS (SELECT 1 FROM {location})
            THEN
                RAISE EXCEPTION
                    'cannot downgrade while E&M distance projections exist'
                    USING ERRCODE = '55000';
            END IF;
        END
        $$
        """
    )
    for table_name in (
        "plan_pricing_em_distance_attachment",
        "plan_pricing_em_distance_location",
        "plan_pricing_em_distance_rate",
        "plan_pricing_em_distance_candidate",
    ):
        op.execute(f"DROP TABLE {_table(schema, table_name)}")
    for function_name, arguments in (
        ("plan_pricing_em_distance_attachment_guard", ""),
        ("plan_pricing_em_distance_truncate_guard", ""),
        ("plan_pricing_em_distance_child_guard", ""),
        ("plan_pricing_em_distance_candidate_guard", ""),
        (
            "plan_pricing_em_distance_rates_valid",
            "numeric[], numeric[], bigint[], smallint",
        ),
    ):
        op.execute(
            f"DROP FUNCTION {_table(schema, function_name)}({arguments})"
        )
    op.execute(f"DROP INDEX {_table(schema, IDEMPOTENCY_INDEX_NAME)}")
    op.execute(
        f"""
        CREATE UNIQUE INDEX {IDEMPOTENCY_INDEX_NAME}
            ON {import_run} (importer, idempotency_key)
         WHERE importer IN (
                   'plan-pricing-projection', 'plan-pricing-prewarm'
               )
           AND idempotency_key IS NOT NULL
        """
    )
