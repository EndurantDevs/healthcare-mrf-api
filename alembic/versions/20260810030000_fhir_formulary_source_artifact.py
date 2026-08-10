"""Persist immutable source artifacts for exact formulary replay.

Revision ID: 20260810030000_fhir_formulary_source_artifact
Revises: 20260810030000_provider_directory_organization_name_variants
"""

from __future__ import annotations

import os
import re

from alembic import op
import sqlalchemy as sa

from db.migration_index_adoption import create_index_if_missing


revision = "20260810030000_fhir_formulary_source_artifact"
down_revision = "20260810030000_provider_directory_organization_name_variants"
branch_labels = None
depends_on = None


_SET_TABLE = "fhir_formulary_source_artifact_set"
_OBSERVATION_TABLE = "fhir_formulary_source_artifact_observation"
_TABLE = "fhir_formulary_source_artifact"
_SET_GUARD = "guard_fhir_formulary_source_artifact_set"
_OBSERVATION_GUARD = "guard_fhir_formulary_source_artifact_observation"
_GUARD = "guard_fhir_formulary_source_artifact"
_CENSUS_GUARD = "validate_fhir_formulary_source_artifact_census"
_SET_TRIGGER = "fhir_formulary_source_artifact_set_guard"
_OBSERVATION_TRIGGER = "fhir_formulary_source_artifact_observation_guard"
_TRIGGER = "fhir_formulary_source_artifact_guard"
_SET_CENSUS_TRIGGER = "fhir_formulary_source_artifact_set_census"
_ARTIFACT_CENSUS_TRIGGER = "fhir_formulary_source_artifact_census"
_SET_HASH_FUNCTION = "fhir_formulary_source_artifact_set_sha256"


def _schema() -> str:
    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise RuntimeError("DB_SCHEMA and HLTHPRT_DB_SCHEMA must match")
    schema = runtime_schema or legacy_schema or "mrf"
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema):
        raise RuntimeError("FHIR formulary database schema is invalid")
    return schema


def _quoted(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _qualified(schema: str, identifier: str) -> str:
    return f"{_quoted(schema)}.{_quoted(identifier)}"


def _guard_function_sql(schema: str) -> str:
    function_ref = _qualified(schema, _GUARD)
    return f"""
    CREATE FUNCTION {function_ref}()
    RETURNS trigger
    LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = pg_catalog
    AS $guard$
    BEGIN
        IF TG_OP = 'INSERT' THEN
            IF NEW.status <> 'pending'
               OR NEW.artifact_sha256 IS NOT NULL
               OR NEW.artifact_byte_count IS NOT NULL
               OR NEW.verified_at IS NOT NULL
               OR NEW.created_at IS DISTINCT FROM transaction_timestamp() THEN
                RAISE EXCEPTION 'fhir_formulary_source_artifact_immutable'
                    USING ERRCODE = '55000';
            END IF;
            RETURN NEW;
        END IF;
        IF TG_OP IN ('DELETE', 'TRUNCATE') THEN
            RAISE EXCEPTION 'fhir_formulary_source_artifact_immutable'
                USING ERRCODE = '55000';
        END IF;
        IF OLD.status <> 'pending'
           OR NEW.status <> 'verified'
           OR ROW(
                NEW.source_id,
                NEW.source_file_set_sha256,
                NEW.source_file_id,
                NEW.raw_listing_projection_sha256,
                NEW.family,
                NEW.file_name,
                NEW.source_url,
                NEW.catalog_modified_at,
                NEW.catalog_entry_sha256,
                NEW.expected_byte_count,
                NEW.created_at
              ) IS DISTINCT FROM ROW(
                OLD.source_id,
                OLD.source_file_set_sha256,
                OLD.source_file_id,
                OLD.raw_listing_projection_sha256,
                OLD.family,
                OLD.file_name,
                OLD.source_url,
                OLD.catalog_modified_at,
                OLD.catalog_entry_sha256,
                OLD.expected_byte_count,
                OLD.created_at
              )
           OR OLD.artifact_sha256 IS NOT NULL
           OR OLD.artifact_byte_count IS NOT NULL
           OR OLD.verified_at IS NOT NULL
           OR NEW.artifact_sha256 IS NULL
           OR NEW.artifact_byte_count IS NULL
           OR NEW.verified_at IS DISTINCT FROM transaction_timestamp() THEN
            RAISE EXCEPTION 'fhir_formulary_source_artifact_immutable'
                USING ERRCODE = '55000';
        END IF;
        RETURN NEW;
    END;
    $guard$;
    """


def _set_guard_function_sql(schema: str) -> str:
    function_ref = _qualified(schema, _SET_GUARD)
    return f"""
    CREATE FUNCTION {function_ref}()
    RETURNS trigger
    LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = pg_catalog
    AS $guard$
    BEGIN
        IF TG_OP = 'INSERT' THEN
            IF NEW.created_at IS DISTINCT FROM transaction_timestamp() THEN
                RAISE EXCEPTION 'fhir_formulary_source_artifact_set_immutable'
                    USING ERRCODE = '55000';
            END IF;
            RETURN NEW;
        END IF;
        RAISE EXCEPTION 'fhir_formulary_source_artifact_set_immutable'
            USING ERRCODE = '55000';
    END;
    $guard$;
    """


def _observation_guard_function_sql(schema: str) -> str:
    function_ref = _qualified(schema, _OBSERVATION_GUARD)
    return f"""
    CREATE FUNCTION {function_ref}()
    RETURNS trigger
    LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = pg_catalog
    AS $guard$
    BEGIN
        IF TG_OP = 'INSERT' THEN
            IF NEW.created_at IS DISTINCT FROM transaction_timestamp() THEN
                RAISE EXCEPTION 'fhir_formulary_source_artifact_observation_immutable'
                    USING ERRCODE = '55000';
            END IF;
            RETURN NEW;
        END IF;
        RAISE EXCEPTION 'fhir_formulary_source_artifact_observation_immutable'
            USING ERRCODE = '55000';
    END;
    $guard$;
    """


def _census_guard_function_sql(schema: str) -> str:
    function_ref = _qualified(schema, _CENSUS_GUARD)
    set_ref = _qualified(schema, _SET_TABLE)
    artifact_ref = _qualified(schema, _TABLE)
    return f"""
    CREATE FUNCTION {function_ref}()
    RETURNS trigger
    LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = pg_catalog
    AS $guard$
    DECLARE
        expected_count integer;
        actual_count bigint;
    BEGIN
        SELECT expected_file_count INTO expected_count
          FROM {set_ref}
         WHERE source_id = NEW.source_id
           AND source_file_set_sha256 = NEW.source_file_set_sha256;
        SELECT count(*) INTO actual_count
          FROM {artifact_ref}
         WHERE source_id = NEW.source_id
           AND source_file_set_sha256 = NEW.source_file_set_sha256;
        IF expected_count IS NULL OR actual_count <> expected_count THEN
            RAISE EXCEPTION 'fhir_formulary_source_artifact_census_invalid'
                USING ERRCODE = '23514';
        END IF;
        RETURN NULL;
    END;
    $guard$;
    """


def _artifact_set_sha256_function_sql(schema: str) -> str:
    function_ref = _qualified(schema, _SET_HASH_FUNCTION)
    artifact_ref = _qualified(schema, _TABLE)
    return f"""
    CREATE FUNCTION {function_ref}(
        candidate_source_id text,
        candidate_source_file_set_sha256 text
    )
    RETURNS text
    LANGUAGE sql
    STABLE
    STRICT
    SECURITY DEFINER
    SET search_path = pg_catalog
    AS $function$
        SELECT CASE
            WHEN pg_catalog.count(*) = 0
              OR pg_catalog.count(*) FILTER (
                     WHERE artifact.status = 'verified'
                 ) <> pg_catalog.count(*)
            THEN NULL
            ELSE pg_catalog.encode(
                pg_catalog.sha256(
                    pg_catalog.convert_to(
                        'fhir-formulary-source-artifact-set-v1'
                        || pg_catalog.chr(10)
                        || '['
                        || pg_catalog.string_agg(
                            '{{"artifact_byte_count":'
                            || artifact.artifact_byte_count::text
                            || ',"artifact_sha256":'
                            || pg_catalog.to_json(
                                artifact.artifact_sha256
                            )::text
                            || ',"catalog_entry_sha256":'
                            || pg_catalog.to_json(
                                artifact.catalog_entry_sha256
                            )::text
                            || ',"catalog_modified_at":'
                            || pg_catalog.to_json(
                                artifact.catalog_modified_at
                            )::text
                            || ',"expected_byte_count":'
                            || CASE
                                WHEN artifact.expected_byte_count IS NULL
                                THEN 'null'
                                ELSE artifact.expected_byte_count::text
                               END
                            || ',"family":'
                            || pg_catalog.to_json(artifact.family)::text
                            || ',"file_name":'
                            || pg_catalog.to_json(artifact.file_name)::text
                            || ',"raw_listing_projection_sha256":'
                            || pg_catalog.to_json(
                                artifact.raw_listing_projection_sha256
                            )::text
                            || ',"source_file_id":'
                            || pg_catalog.to_json(
                                artifact.source_file_id
                            )::text
                            || ',"source_file_set_sha256":'
                            || pg_catalog.to_json(
                                artifact.source_file_set_sha256
                            )::text
                            || ',"source_id":'
                            || pg_catalog.to_json(artifact.source_id)::text
                            || ',"source_url":'
                            || pg_catalog.to_json(artifact.source_url)::text
                            || '}}',
                            ',' ORDER BY
                                pg_catalog.convert_to(
                                    artifact.family,
                                    'UTF8'
                                ),
                                pg_catalog.convert_to(
                                    artifact.file_name,
                                    'UTF8'
                                ),
                                pg_catalog.convert_to(
                                    artifact.source_file_id,
                                    'UTF8'
                                )
                        )
                        || ']',
                        'UTF8'
                    )
                ),
                'hex'
            )
        END
        FROM {artifact_ref} AS artifact
        WHERE artifact.source_id = candidate_source_id
          AND artifact.source_file_set_sha256 =
              candidate_source_file_set_sha256;
    $function$;
    """


def _guard_install_statements(
    schema: str,
    *,
    table_name: str,
    function_name: str,
    trigger_name: str,
    guard_insert: bool,
) -> tuple[str, ...]:
    table_ref = _qualified(schema, table_name)
    function_ref = _qualified(schema, function_name)
    row_events = (
        "INSERT OR UPDATE OR DELETE" if guard_insert else "UPDATE OR DELETE"
    )
    return (
        f"REVOKE ALL ON FUNCTION {function_ref}() FROM PUBLIC;",
        f"CREATE TRIGGER {_quoted(trigger_name)} BEFORE {row_events} "
        f"ON {table_ref} FOR EACH ROW EXECUTE FUNCTION {function_ref}();",
        f"ALTER TABLE {table_ref} ENABLE ALWAYS TRIGGER "
        f"{_quoted(trigger_name)};",
        f"CREATE TRIGGER {_quoted(trigger_name + '_truncate')} BEFORE TRUNCATE "
        f"ON {table_ref} FOR EACH STATEMENT EXECUTE FUNCTION {function_ref}();",
        f"ALTER TABLE {table_ref} ENABLE ALWAYS TRIGGER "
        f"{_quoted(trigger_name + '_truncate')};",
    )


def _downgrade_fence_sql(schema: str) -> str:
    set_ref = _qualified(schema, _SET_TABLE)
    observation_ref = _qualified(schema, _OBSERVATION_TABLE)
    artifact_ref = _qualified(schema, _TABLE)
    return f"""
    DO $downgrade$
    BEGIN
        IF EXISTS (SELECT 1 FROM {set_ref} LIMIT 1)
           OR EXISTS (SELECT 1 FROM {observation_ref} LIMIT 1)
           OR EXISTS (SELECT 1 FROM {artifact_ref} LIMIT 1) THEN
            RAISE EXCEPTION 'fhir_formulary_source_artifact_downgrade_blocked'
                USING ERRCODE = '55000';
        END IF;
    END;
    $downgrade$;
    """


def _downgrade_lock_sql(schema: str) -> str:
    return "LOCK TABLE " + ", ".join(
        (
            _qualified(schema, _TABLE),
            _qualified(schema, _OBSERVATION_TABLE),
            _qualified(schema, _SET_TABLE),
        )
    ) + " IN ACCESS EXCLUSIVE MODE;"


def _census_trigger_statements(schema: str) -> tuple[str, ...]:
    function_ref = _qualified(schema, _CENSUS_GUARD)
    set_ref = _qualified(schema, _SET_TABLE)
    artifact_ref = _qualified(schema, _TABLE)
    return (
        f"REVOKE ALL ON FUNCTION {function_ref}() FROM PUBLIC;",
        f"CREATE CONSTRAINT TRIGGER {_quoted(_SET_CENSUS_TRIGGER)} "
        f"AFTER INSERT ON {set_ref} DEFERRABLE INITIALLY DEFERRED "
        f"FOR EACH ROW EXECUTE FUNCTION {function_ref}();",
        f"ALTER TABLE {set_ref} ENABLE ALWAYS TRIGGER "
        f"{_quoted(_SET_CENSUS_TRIGGER)};",
        f"CREATE CONSTRAINT TRIGGER {_quoted(_ARTIFACT_CENSUS_TRIGGER)} "
        f"AFTER INSERT ON {artifact_ref} DEFERRABLE INITIALLY DEFERRED "
        f"FOR EACH ROW EXECUTE FUNCTION {function_ref}();",
        f"ALTER TABLE {artifact_ref} ENABLE ALWAYS TRIGGER "
        f"{_quoted(_ARTIFACT_CENSUS_TRIGGER)};",
    )


def upgrade() -> None:
    schema = _schema()
    op.create_table(
        _SET_TABLE,
        sa.Column("source_id", sa.String(length=64), nullable=False),
        sa.Column(
            "source_file_set_sha256", sa.String(length=64), nullable=False
        ),
        sa.Column(
            "raw_listing_projection_sha256",
            sa.String(length=64),
            nullable=False,
        ),
        sa.Column("expected_file_count", sa.Integer(), nullable=False),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("transaction_timestamp()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["source_id"],
            [f"{schema}.fhir_formulary_source.source_id"],
            name="fhir_formulary_source_artifact_set_source_fkey",
        ),
        sa.PrimaryKeyConstraint(
            "source_id",
            "source_file_set_sha256",
            name="fhir_formulary_source_artifact_set_pkey",
        ),
        sa.UniqueConstraint(
            "source_id",
            "source_file_set_sha256",
            "raw_listing_projection_sha256",
            name="fhir_formulary_source_artifact_set_projection_key",
        ),
        sa.CheckConstraint(
            "source_file_set_sha256 ~ '^[0-9a-f]{64}$' AND "
            "raw_listing_projection_sha256 ~ '^[0-9a-f]{64}$' AND "
            "expected_file_count > 0 AND expected_file_count <= 100000",
            name="fhir_formulary_source_artifact_set_identity_check",
        ),
        schema=schema,
    )
    op.create_table(
        _OBSERVATION_TABLE,
        sa.Column("source_id", sa.String(length=64), nullable=False),
        sa.Column(
            "source_observation_sha256",
            sa.String(length=64),
            nullable=False,
        ),
        sa.Column(
            "source_file_set_sha256", sa.String(length=64), nullable=False
        ),
        sa.Column(
            "raw_listing_projection_sha256",
            sa.String(length=64),
            nullable=False,
        ),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("transaction_timestamp()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["source_id"],
            [f"{schema}.fhir_formulary_source.source_id"],
            name="fhir_formulary_source_artifact_observation_source_fkey",
        ),
        sa.ForeignKeyConstraint(
            [
                "source_id",
                "source_file_set_sha256",
                "raw_listing_projection_sha256",
            ],
            [
                f"{schema}.{_SET_TABLE}.source_id",
                f"{schema}.{_SET_TABLE}.source_file_set_sha256",
                f"{schema}.{_SET_TABLE}.raw_listing_projection_sha256",
            ],
            name="fhir_formulary_source_artifact_observation_set_fkey",
        ),
        sa.PrimaryKeyConstraint(
            "source_id",
            "source_observation_sha256",
            name="fhir_formulary_source_artifact_observation_pkey",
        ),
        sa.CheckConstraint(
            "source_observation_sha256 ~ '^[0-9a-f]{64}$' AND "
            "source_file_set_sha256 ~ '^[0-9a-f]{64}$' AND "
            "raw_listing_projection_sha256 ~ '^[0-9a-f]{64}$'",
            name="fhir_formulary_source_artifact_observation_identity_check",
        ),
        schema=schema,
    )
    op.create_table(
        _TABLE,
        sa.Column("source_id", sa.String(length=64), nullable=False),
        sa.Column(
            "source_file_set_sha256", sa.String(length=64), nullable=False
        ),
        sa.Column("source_file_id", sa.String(length=64), nullable=False),
        sa.Column(
            "raw_listing_projection_sha256",
            sa.String(length=64),
            nullable=False,
        ),
        sa.Column("family", sa.String(length=32), nullable=False),
        sa.Column("file_name", sa.String(length=256), nullable=False),
        sa.Column("source_url", sa.Text(), nullable=False),
        sa.Column("catalog_modified_at", sa.String(length=64), nullable=False),
        sa.Column("catalog_entry_sha256", sa.String(length=64), nullable=False),
        sa.Column("expected_byte_count", sa.BigInteger()),
        sa.Column("artifact_sha256", sa.String(length=64)),
        sa.Column("artifact_byte_count", sa.BigInteger()),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("transaction_timestamp()"),
            nullable=False,
        ),
        sa.Column("verified_at", sa.TIMESTAMP(timezone=True)),
        sa.ForeignKeyConstraint(
            ["source_id"],
            [f"{schema}.fhir_formulary_source.source_id"],
            name="fhir_formulary_source_artifact_source_fkey",
        ),
        sa.ForeignKeyConstraint(
            [
                "source_id",
                "source_file_set_sha256",
                "raw_listing_projection_sha256",
            ],
            [
                f"{schema}.{_SET_TABLE}.source_id",
                f"{schema}.{_SET_TABLE}.source_file_set_sha256",
                f"{schema}.{_SET_TABLE}.raw_listing_projection_sha256",
            ],
            name="fhir_formulary_source_artifact_set_fkey",
        ),
        sa.PrimaryKeyConstraint(
            "source_id",
            "source_file_set_sha256",
            "source_file_id",
            name="fhir_formulary_source_artifact_pkey",
        ),
        sa.UniqueConstraint(
            "source_id",
            "source_file_set_sha256",
            "family",
            "file_name",
            name="fhir_formulary_source_artifact_logical_key",
        ),
        sa.CheckConstraint(
            "source_file_set_sha256 ~ '^[0-9a-f]{64}$' AND "
            "source_file_id ~ '^[0-9a-f]{64}$' AND "
            "raw_listing_projection_sha256 ~ '^[0-9a-f]{64}$' AND "
            "catalog_entry_sha256 ~ '^[0-9a-f]{64}$' AND "
            "family ~ '^[a-z0-9][a-z0-9_-]{0,31}$' AND "
            "length(file_name) > 0 AND length(source_url) > 0 AND "
            "length(catalog_modified_at) > 0 AND "
            "(expected_byte_count IS NULL OR expected_byte_count > 0)",
            name="fhir_formulary_source_artifact_identity_check",
        ),
        sa.CheckConstraint(
            "(status = 'pending' AND artifact_sha256 IS NULL AND "
            "artifact_byte_count IS NULL AND verified_at IS NULL) OR "
            "(status = 'verified' AND "
            "artifact_sha256 ~ '^[0-9a-f]{64}$' AND "
            "artifact_byte_count > 0 AND verified_at IS NOT NULL AND "
            "(expected_byte_count IS NULL OR "
            "artifact_byte_count = expected_byte_count))",
            name="fhir_formulary_source_artifact_state_check",
        ),
        schema=schema,
    )
    create_index_if_missing(
        op,
        "fhir_formulary_source_artifact_pending_idx",
        _TABLE,
        ["source_id", "source_file_set_sha256", "status", "family"],
        schema=schema,
    )
    op.execute(_set_guard_function_sql(schema))
    for statement in _guard_install_statements(
        schema,
        table_name=_SET_TABLE,
        function_name=_SET_GUARD,
        trigger_name=_SET_TRIGGER,
        guard_insert=True,
    ):
        op.execute(statement)
    op.execute(_observation_guard_function_sql(schema))
    for statement in _guard_install_statements(
        schema,
        table_name=_OBSERVATION_TABLE,
        function_name=_OBSERVATION_GUARD,
        trigger_name=_OBSERVATION_TRIGGER,
        guard_insert=True,
    ):
        op.execute(statement)
    op.execute(_guard_function_sql(schema))
    for statement in _guard_install_statements(
        schema,
        table_name=_TABLE,
        function_name=_GUARD,
        trigger_name=_TRIGGER,
        guard_insert=True,
    ):
        op.execute(statement)
    op.execute(_census_guard_function_sql(schema))
    for statement in _census_trigger_statements(schema):
        op.execute(statement)
    op.execute(_artifact_set_sha256_function_sql(schema))
    op.execute(
        f"REVOKE ALL ON FUNCTION "
        f"{_qualified(schema, _SET_HASH_FUNCTION)}(text, text) FROM PUBLIC;"
    )


def downgrade() -> None:
    schema = _schema()
    op.execute(_downgrade_lock_sql(schema))
    op.execute(_downgrade_fence_sql(schema))
    op.execute(
        f"DROP FUNCTION "
        f"{_qualified(schema, _SET_HASH_FUNCTION)}(text, text);"
    )
    op.drop_table(_TABLE, schema=schema)
    op.drop_table(_OBSERVATION_TABLE, schema=schema)
    op.drop_table(_SET_TABLE, schema=schema)
    op.execute(f"DROP FUNCTION {_qualified(schema, _CENSUS_GUARD)}();")
    op.execute(f"DROP FUNCTION {_qualified(schema, _GUARD)}();")
    op.execute(f"DROP FUNCTION {_qualified(schema, _OBSERVATION_GUARD)}();")
    op.execute(f"DROP FUNCTION {_qualified(schema, _SET_GUARD)}();")
