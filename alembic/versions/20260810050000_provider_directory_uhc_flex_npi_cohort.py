# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Persist the exact official UHC Practitioner NPI cohort for Flex.

Revision ID: 20260810050000_provider_directory_uhc_flex_npi_cohort
Revises: 20260810040000_fhir_formulary_uhc_admission_receipt
"""

from __future__ import annotations

import os
import re

from alembic import op
import sqlalchemy as sa


revision = "20260810050000_provider_directory_uhc_flex_npi_cohort"
down_revision = "20260810040000_fhir_formulary_uhc_admission_receipt"
branch_labels = None
depends_on = None


_COHORT = "provider_directory_uhc_flex_npi_cohort"
_MEMBER = "provider_directory_uhc_flex_npi_member"
_COHORT_GUARD = "guard_provider_directory_uhc_flex_npi_cohort"
_MEMBER_GUARD = "guard_provider_directory_uhc_flex_npi_member"
_MEMBER_INSERT_GUARD = "validate_provider_directory_uhc_flex_npi_members"
_CONTRACT = (
    "healthporta.provider-directory.uhc-flex-official-practitioner-"
    "npi-cohort.v1"
)
_AUTHORITY = "unitedhealthcare"
_SOURCE_ID = "pdfhir_2754e999dd691175821ec26e"
_CONTENT_PROOF_KEY = "uhc_canonical_content_proof_v1"
_CONTENT_PROOF_CONTRACT = "healthporta.uhc.canonical-content-proof.v1"
_NPI_SYSTEM = "http://hl7.org/fhir/sid/us-npi"


def _schema() -> str:
    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise RuntimeError("DB_SCHEMA and HLTHPRT_DB_SCHEMA must match")
    schema = runtime_schema or legacy_schema or "mrf"
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema):
        raise RuntimeError("Provider Directory database schema is invalid")
    return schema


def _quoted(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _qualified(schema: str, identifier: str) -> str:
    return f"{_quoted(schema)}.{_quoted(identifier)}"


def _header_guard_dataset_sql(schema: str) -> str:
    source = _qualified(schema, "provider_directory_source")
    dataset = _qualified(schema, "provider_directory_endpoint_dataset")
    return f"""
        SELECT source.endpoint_id, dataset.endpoint_id,
               dataset.acquisition_root_run_id, dataset.dataset_hash,
               dataset.resource_count,
               dataset.publication_metadata_json::jsonb
                 -> '{_CONTENT_PROOF_KEY}'
          INTO source_endpoint_id, dataset_endpoint_id,
               acquisition_root_run_id, dataset_hash, dataset_resource_count,
               content_proof
          FROM {source} AS source
          JOIN {dataset} AS dataset
            ON dataset.dataset_id = NEW.official_dataset_id
         WHERE source.source_id = '{_SOURCE_ID}'
           AND dataset.status = 'published'
           AND dataset.is_current IS TRUE
         FOR SHARE OF source, dataset;
        IF NOT FOUND
           OR source_endpoint_id IS DISTINCT FROM NEW.official_endpoint_id
           OR dataset_endpoint_id IS DISTINCT FROM NEW.official_endpoint_id
           OR acquisition_root_run_id IS DISTINCT FROM
              NEW.official_acquisition_root_run_id
           OR dataset_hash IS DISTINCT FROM NEW.official_dataset_hash THEN
            RAISE EXCEPTION 'provider_directory_uhc_flex_cohort_invalid'
                USING ERRCODE = '23514';
        END IF;
    """


def _header_guard_proof_sql() -> str:
    return f"""
        IF content_proof IS NULL
           OR content_proof ->> 'contract_id' IS DISTINCT FROM
              '{_CONTENT_PROOF_CONTRACT}'
           OR content_proof -> 'complete' IS DISTINCT FROM 'true'::jsonb
           OR content_proof ->> 'source_id' IS DISTINCT FROM '{_SOURCE_ID}'
           OR content_proof ->> 'dataset_id' IS DISTINCT FROM
              NEW.official_dataset_id
           OR content_proof ->> 'endpoint_id' IS DISTINCT FROM
              NEW.official_endpoint_id
           OR content_proof ->> 'acquisition_root_run_id' IS DISTINCT FROM
              NEW.official_acquisition_root_run_id
           OR content_proof ->> 'dataset_hash' IS DISTINCT FROM
              NEW.official_dataset_hash
           OR content_proof ->> 'proof_sha256' IS DISTINCT FROM
              NEW.official_content_proof_sha256
           OR COALESCE(
                content_proof ->> 'resource_count' ~ '^[0-9]+$', FALSE
              ) IS NOT TRUE
           OR COALESCE(
                content_proof -> 'resource_counts' ->> 'Practitioner'
                    ~ '^[0-9]+$', FALSE
              ) IS NOT TRUE THEN
            RAISE EXCEPTION 'provider_directory_uhc_flex_cohort_invalid'
                USING ERRCODE = '23514';
        END IF;
        IF (content_proof ->> 'resource_count')::bigint IS DISTINCT FROM
               dataset_resource_count
           OR (content_proof -> 'resource_counts' ->> 'Practitioner')::bigint
               IS DISTINCT FROM NEW.practitioner_resource_count THEN
            RAISE EXCEPTION 'provider_directory_uhc_flex_cohort_invalid'
                USING ERRCODE = '23514';
        END IF;
    """


def _header_guard_resource_sql(schema: str) -> str:
    resource = _qualified(schema, "provider_directory_dataset_resource")
    npi_valid = _qualified(schema, "public_evidence_npi_valid")
    return f"""
        SELECT count(*),
               count(*) FILTER (WHERE resource.resource_type = 'Practitioner')
          INTO actual_resource_count, actual_practitioner_count
          FROM {resource} AS resource
         WHERE resource.dataset_id = NEW.official_dataset_id;
        SELECT count(*)
          INTO invalid_practitioner_count
          FROM {resource} AS resource
         WHERE resource.dataset_id = NEW.official_dataset_id
           AND resource.resource_type = 'Practitioner'
           AND (
                pg_catalog.jsonb_typeof(resource.payload_json::jsonb -> 'npi')
                    = 'number'
                AND resource.payload_json::jsonb ->> 'npi' ~ '^[0-9]{{10}}$'
                AND {npi_valid}(resource.payload_json::jsonb ->> 'npi')
                AND pg_catalog.jsonb_typeof(
                    resource.payload_json::jsonb -> 'identifiers'
                ) = 'array'
                AND 1 = (
                    SELECT count(*)
                      FROM pg_catalog.jsonb_array_elements(
                        CASE WHEN pg_catalog.jsonb_typeof(
                            resource.payload_json::jsonb -> 'identifiers'
                        ) = 'array' THEN
                            resource.payload_json::jsonb -> 'identifiers'
                        ELSE '[]'::jsonb END
                      ) AS identifier
                     WHERE identifier ->> 'system' = '{_NPI_SYSTEM}'
                )
                AND 1 = (
                    SELECT count(*)
                      FROM pg_catalog.jsonb_array_elements(
                        CASE WHEN pg_catalog.jsonb_typeof(
                            resource.payload_json::jsonb -> 'identifiers'
                        ) = 'array' THEN
                            resource.payload_json::jsonb -> 'identifiers'
                        ELSE '[]'::jsonb END
                      ) AS identifier
                     WHERE identifier ->> 'system' = '{_NPI_SYSTEM}'
                       AND pg_catalog.jsonb_typeof(identifier -> 'value')
                           = 'string'
                       AND identifier ->> 'value' =
                           resource.payload_json::jsonb ->> 'npi'
                )
           ) IS NOT TRUE;
        IF actual_resource_count IS DISTINCT FROM dataset_resource_count
           OR actual_practitioner_count IS DISTINCT FROM
              NEW.practitioner_resource_count
           OR invalid_practitioner_count <> 0 THEN
            RAISE EXCEPTION 'provider_directory_uhc_flex_cohort_invalid'
                USING ERRCODE = '23514';
        END IF;
    """


def _header_guard_member_sql(schema: str) -> str:
    member = _qualified(schema, _MEMBER)
    resource = _qualified(schema, "provider_directory_dataset_resource")
    return f"""
        SELECT count(*)
          INTO actual_npi_count
          FROM {member} AS member
         WHERE member.cohort_id = NEW.cohort_id;
        IF actual_npi_count IS DISTINCT FROM NEW.npi_count
           OR EXISTS (
                SELECT member.npi FROM {member} AS member
                 WHERE member.cohort_id = NEW.cohort_id
                EXCEPT
                SELECT DISTINCT (resource.payload_json::jsonb ->> 'npi')::bigint
                  FROM {resource} AS resource
                 WHERE resource.dataset_id = NEW.official_dataset_id
                   AND resource.resource_type = 'Practitioner'
           ) OR EXISTS (
                SELECT DISTINCT (resource.payload_json::jsonb ->> 'npi')::bigint
                  FROM {resource} AS resource
                 WHERE resource.dataset_id = NEW.official_dataset_id
                   AND resource.resource_type = 'Practitioner'
                EXCEPT
                SELECT member.npi FROM {member} AS member
                 WHERE member.cohort_id = NEW.cohort_id
           ) THEN
            RAISE EXCEPTION 'provider_directory_uhc_flex_cohort_invalid'
                USING ERRCODE = '23514';
        END IF;
        expected_cohort_id := 'pdufc_' || pg_catalog.substr(
            pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
                NEW.contract_id || pg_catalog.chr(31) || NEW.authority_id
                || pg_catalog.chr(31) || NEW.official_source_id
                || pg_catalog.chr(31) || NEW.official_endpoint_id
                || pg_catalog.chr(31) || NEW.official_dataset_id
                || pg_catalog.chr(31) || NEW.official_acquisition_root_run_id
                || pg_catalog.chr(31) || NEW.official_dataset_hash
                || pg_catalog.chr(31) || NEW.official_content_proof_sha256
                || pg_catalog.chr(31) || NEW.resource_type
                || pg_catalog.chr(31) || NEW.practitioner_resource_count::text
                || pg_catalog.chr(31) || NEW.npi_count::text
                || pg_catalog.chr(31) || 'true'
                || pg_catalog.chr(31) || 'false'
                || pg_catalog.chr(31) || 'false', 'UTF8'
            )), 'hex'), 1, 48);
        IF expected_cohort_id IS DISTINCT FROM NEW.cohort_id THEN
            RAISE EXCEPTION 'provider_directory_uhc_flex_cohort_invalid'
                USING ERRCODE = '23514';
        END IF;
    """


def _cohort_guard_function_sql(schema: str) -> str:
    function_ref = _qualified(schema, _COHORT_GUARD)
    member_ref = _qualified(schema, _MEMBER)
    return f"""
    CREATE FUNCTION {function_ref}() RETURNS trigger LANGUAGE plpgsql
    SECURITY DEFINER SET search_path = pg_catalog AS $guard$
    DECLARE
        source_endpoint_id text; dataset_endpoint_id text;
        acquisition_root_run_id text; dataset_hash text;
        dataset_resource_count bigint; content_proof jsonb;
        actual_resource_count bigint; actual_practitioner_count bigint;
        invalid_practitioner_count bigint; actual_npi_count bigint;
        expected_cohort_id text;
    BEGIN
        IF TG_OP <> 'INSERT'
           OR NEW.created_at IS DISTINCT FROM transaction_timestamp() THEN
            RAISE EXCEPTION 'provider_directory_uhc_flex_cohort_immutable'
                USING ERRCODE = '55000';
        END IF;
        LOCK TABLE {member_ref} IN SHARE ROW EXCLUSIVE MODE;
        {_header_guard_dataset_sql(schema)}
        {_header_guard_proof_sql()}
        {_header_guard_resource_sql(schema)}
        {_header_guard_member_sql(schema)}
        RETURN NEW;
    END;
    $guard$;
    """


def _member_guard_function_sql(schema: str) -> str:
    function_ref = _qualified(schema, _MEMBER_GUARD)
    return f"""
    CREATE FUNCTION {function_ref}() RETURNS trigger LANGUAGE plpgsql
    SECURITY DEFINER SET search_path = pg_catalog AS $guard$
    BEGIN
        RAISE EXCEPTION 'provider_directory_uhc_flex_member_immutable'
            USING ERRCODE = '55000';
    END;
    $guard$;
    """


def _member_insert_guard_function_sql(schema: str) -> str:
    function_ref = _qualified(schema, _MEMBER_INSERT_GUARD)
    cohort_ref = _qualified(schema, _COHORT)
    npi_valid = _qualified(schema, "public_evidence_npi_valid")
    return f"""
    CREATE FUNCTION {function_ref}() RETURNS trigger LANGUAGE plpgsql
    SECURITY DEFINER SET search_path = pg_catalog AS $guard$
    BEGIN
        IF EXISTS (
            SELECT 1 FROM new_rows AS member
             WHERE member.created_at IS DISTINCT FROM transaction_timestamp()
                OR {npi_valid}(member.npi::text) IS NOT TRUE
                OR EXISTS (SELECT 1 FROM {cohort_ref} AS cohort
                            WHERE cohort.cohort_id = member.cohort_id)
        ) THEN
            RAISE EXCEPTION 'provider_directory_uhc_flex_member_immutable'
                USING ERRCODE = '55000';
        END IF;
        RETURN NULL;
    END;
    $guard$;
    """


def _guard_statements(schema: str) -> tuple[str, ...]:
    cohort_ref = _qualified(schema, _COHORT)
    member_ref = _qualified(schema, _MEMBER)
    cohort_guard = _qualified(schema, _COHORT_GUARD)
    member_guard = _qualified(schema, _MEMBER_GUARD)
    insert_guard = _qualified(schema, _MEMBER_INSERT_GUARD)
    return (
        *(f"REVOKE ALL ON FUNCTION {ref}() FROM PUBLIC;" for ref in
          (cohort_guard, member_guard, insert_guard)),
        f"REVOKE ALL ON TABLE {cohort_ref} FROM PUBLIC;",
        f"REVOKE ALL ON TABLE {member_ref} FROM PUBLIC;",
        f"CREATE TRIGGER pd_uhc_flex_npi_cohort_guard BEFORE INSERT OR UPDATE "
        f"OR DELETE ON {cohort_ref} FOR EACH ROW EXECUTE FUNCTION {cohort_guard}();",
        f"CREATE TRIGGER pd_uhc_flex_npi_cohort_guard_truncate BEFORE TRUNCATE "
        f"ON {cohort_ref} FOR EACH STATEMENT EXECUTE FUNCTION {cohort_guard}();",
        f"CREATE TRIGGER pd_uhc_flex_npi_member_guard BEFORE UPDATE OR DELETE "
        f"ON {member_ref} FOR EACH ROW EXECUTE FUNCTION {member_guard}();",
        f"CREATE TRIGGER pd_uhc_flex_npi_member_guard_truncate BEFORE TRUNCATE "
        f"ON {member_ref} FOR EACH STATEMENT EXECUTE FUNCTION {member_guard}();",
        f"CREATE TRIGGER pd_uhc_flex_npi_member_insert_guard AFTER INSERT ON "
        f"{member_ref} REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT "
        f"EXECUTE FUNCTION {insert_guard}();",
        *(f"ALTER TABLE {table} ENABLE ALWAYS TRIGGER {_quoted(trigger)};"
          for table, trigger in (
              (cohort_ref, "pd_uhc_flex_npi_cohort_guard"),
              (cohort_ref, "pd_uhc_flex_npi_cohort_guard_truncate"),
              (member_ref, "pd_uhc_flex_npi_member_guard"),
              (member_ref, "pd_uhc_flex_npi_member_guard_truncate"),
              (member_ref, "pd_uhc_flex_npi_member_insert_guard"),
          )),
    )


def _create_cohort_table(schema: str) -> None:
    op.create_table(
        _COHORT,
        sa.Column("cohort_id", sa.String(54), nullable=False),
        sa.Column("contract_id", sa.String(96), nullable=False),
        sa.Column("authority_id", sa.String(64), nullable=False),
        sa.Column("official_source_id", sa.String(64), nullable=False),
        sa.Column("official_endpoint_id", sa.String(64), nullable=False),
        sa.Column("official_dataset_id", sa.String(96), nullable=False),
        sa.Column("official_acquisition_root_run_id", sa.String(64), nullable=False),
        sa.Column("official_dataset_hash", sa.String(64), nullable=False),
        sa.Column("official_content_proof_sha256", sa.String(64), nullable=False),
        sa.Column("resource_type", sa.String(64), nullable=False),
        sa.Column("practitioner_resource_count", sa.BigInteger(), nullable=False),
        sa.Column("npi_count", sa.BigInteger(), nullable=False),
        sa.Column("cohort_complete", sa.Boolean(), nullable=False),
        sa.Column("endpoint_collection_complete", sa.Boolean(), nullable=False),
        sa.Column("endpoint_complete", sa.Boolean(), nullable=False),
        sa.Column(
            "created_at", sa.TIMESTAMP(timezone=True), nullable=False,
            server_default=sa.text("transaction_timestamp()"),
        ),
        sa.PrimaryKeyConstraint("cohort_id", name="pd_uhc_flex_npi_cohort_pkey"),
        sa.UniqueConstraint(
            "official_dataset_id", name="pd_uhc_flex_npi_cohort_dataset_key"
        ),
        sa.ForeignKeyConstraint(
            ["official_source_id"],
            [f"{schema}.provider_directory_source.source_id"],
            name="pd_uhc_flex_npi_cohort_source_fkey",
        ),
        sa.ForeignKeyConstraint(
            ["official_endpoint_id"],
            [f"{schema}.provider_directory_api_endpoint.endpoint_id"],
            name="pd_uhc_flex_npi_cohort_endpoint_fkey",
        ),
        sa.ForeignKeyConstraint(
            ["official_dataset_id"],
            [f"{schema}.provider_directory_endpoint_dataset.dataset_id"],
            name="pd_uhc_flex_npi_cohort_dataset_fkey",
        ),
        sa.CheckConstraint(
            f"cohort_id ~ '^pdufc_[0-9a-f]{{48}}$' AND contract_id = "
            f"'{_CONTRACT}' AND authority_id = '{_AUTHORITY}' AND "
            f"official_source_id = '{_SOURCE_ID}' AND "
            "official_acquisition_root_run_id <> '' AND "
            "official_dataset_hash ~ '^[0-9a-f]{64}$' AND "
            "official_content_proof_sha256 ~ '^[0-9a-f]{64}$' AND "
            "resource_type = 'Practitioner' AND "
            "practitioner_resource_count > 0 AND npi_count > 0 AND "
            "npi_count <= practitioner_resource_count AND "
            "cohort_complete IS TRUE AND "
            "endpoint_collection_complete IS FALSE AND endpoint_complete IS FALSE",
            name="pd_uhc_flex_npi_cohort_identity_check",
        ),
        schema=schema,
    )


def _create_member_table(schema: str) -> None:
    op.create_table(
        _MEMBER,
        sa.Column("cohort_id", sa.String(54), nullable=False),
        sa.Column("npi", sa.BigInteger(), nullable=False),
        sa.Column(
            "created_at", sa.TIMESTAMP(timezone=True), nullable=False,
            server_default=sa.text("transaction_timestamp()"),
        ),
        sa.PrimaryKeyConstraint(
            "cohort_id", "npi", name="pd_uhc_flex_npi_member_pkey"
        ),
        sa.ForeignKeyConstraint(
            ["cohort_id"], [f"{schema}.{_COHORT}.cohort_id"],
            name="pd_uhc_flex_npi_member_cohort_fkey",
            deferrable=True, initially="DEFERRED",
        ),
        sa.CheckConstraint(
            "npi BETWEEN 1000000000 AND 2999999999",
            name="pd_uhc_flex_npi_member_npi_check",
        ),
        schema=schema,
    )
    op.create_index(
        "pd_uhc_flex_npi_member_npi_idx", _MEMBER,
        ["npi", "cohort_id"], schema=schema,
    )


def _downgrade_fence_sql(schema: str) -> str:
    cohort_ref = _qualified(schema, _COHORT)
    member_ref = _qualified(schema, _MEMBER)
    return f"""
    DO $downgrade$ BEGIN
        IF EXISTS (SELECT 1 FROM {cohort_ref} LIMIT 1)
           OR EXISTS (SELECT 1 FROM {member_ref} LIMIT 1) THEN
            RAISE EXCEPTION 'provider_directory_uhc_flex_cohort_downgrade_blocked'
                USING ERRCODE = '55000';
        END IF;
    END; $downgrade$;
    """


def _downgrade_lock_sql(schema: str) -> str:
    return "LOCK TABLE " + ", ".join(
        (_qualified(schema, _MEMBER), _qualified(schema, _COHORT))
    ) + " IN ACCESS EXCLUSIVE MODE;"


def upgrade() -> None:
    schema = _schema()
    _create_cohort_table(schema)
    _create_member_table(schema)
    op.execute(_cohort_guard_function_sql(schema))
    op.execute(_member_guard_function_sql(schema))
    op.execute(_member_insert_guard_function_sql(schema))
    for statement in _guard_statements(schema):
        op.execute(statement)


def downgrade() -> None:
    schema = _schema()
    op.execute(_downgrade_lock_sql(schema))
    op.execute(_downgrade_fence_sql(schema))
    op.drop_index("pd_uhc_flex_npi_member_npi_idx", table_name=_MEMBER, schema=schema)
    op.drop_table(_MEMBER, schema=schema)
    op.drop_table(_COHORT, schema=schema)
    op.execute(
        f"DROP FUNCTION {_qualified(schema, _MEMBER_INSERT_GUARD)}();"
    )
    op.execute(
        f"DROP FUNCTION {_qualified(schema, _MEMBER_GUARD)}();"
    )
    op.execute(
        f"DROP FUNCTION {_qualified(schema, _COHORT_GUARD)}();"
    )
