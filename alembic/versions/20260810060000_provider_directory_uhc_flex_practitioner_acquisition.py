# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Add dormant exact-cohort Flex Practitioner acquisition storage.

Revision ID: 20260810060000_provider_directory_uhc_flex_practitioner_acquisition
Revises: 20260810050000_provider_directory_uhc_flex_npi_cohort
"""

from __future__ import annotations

import os
import re

from alembic import op
import sqlalchemy as sa


revision = "20260810060000_provider_directory_uhc_flex_practitioner_acquisition"
down_revision = "20260810050000_provider_directory_uhc_flex_npi_cohort"
branch_labels = None
depends_on = None


_ACQUISITION = "provider_directory_uhc_flex_practitioner_acquisition"
_WORK = "provider_directory_uhc_flex_practitioner_work"
_RESOURCE = "provider_directory_uhc_flex_practitioner_resource"
_COHORT = "provider_directory_uhc_flex_npi_cohort"
_MEMBER = "provider_directory_uhc_flex_npi_member"
_ACQUISITION_GUARD = "guard_pd_uhc_flex_practitioner_acquisition"
_WORK_GUARD = "guard_pd_uhc_flex_practitioner_work"
_RESOURCE_GUARD = "guard_pd_uhc_flex_practitioner_resource"
_TERMINAL_SET_FUNCTION = "pd_uhc_flex_practitioner_terminal_set_sha256"
_STORAGE_CONTRACT = (
    "healthporta.provider-directory.uhc-flex-practitioner-acquisition.v1"
)
_TERMINAL_RECORD_CONTRACT = (
    "healthporta.provider-directory.uhc-flex-practitioner-terminal-record.v1"
)
_TERMINAL_SET_CONTRACT = (
    "healthporta.provider-directory.uhc-flex-practitioner-terminal-set.v1"
)
_SOURCE_ID = "pdfhir_1ceb7c0986c320b7eb924881"
_CONNECTOR_ID = (
    "pdufpc_16ebdbf260dc9815ae38830a6991fea5d6533ab8db7389da"
)
_QUERY_CONTRACT = (
    "healthporta.provider-directory.uhc-flex-practitioner-exact-npi.v1"
)
_NPI_SYSTEM = "http://hl7.org/fhir/sid/us-npi"
_ACTION_SETTING = "healthporta.uhc_flex_practitioner_action"
_ACQUISITION_SETTING = "healthporta.uhc_flex_practitioner_acquisition"
_LEASE_SETTING = "healthporta.uhc_flex_practitioner_lease"


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


def _terminal_set_function_sql(schema: str) -> str:
    function_ref = _qualified(schema, _TERMINAL_SET_FUNCTION)
    work_ref = _qualified(schema, _WORK)
    return f"""
    CREATE FUNCTION {function_ref}(candidate_acquisition_id text)
    RETURNS text LANGUAGE sql STABLE STRICT PARALLEL SAFE
    SECURITY DEFINER SET search_path = pg_catalog AS $function$
        WITH leaf_bucket AS (
            SELECT work.npi / 1000 AS bucket_id,
                   count(*)::bigint AS row_count,
                   pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
                       pg_catalog.string_agg(
                           work.npi::text || pg_catalog.chr(31)
                           || work.terminal_record_sha256,
                           pg_catalog.chr(30) ORDER BY work.npi
                       ), 'UTF8'
                   )), 'hex') AS bucket_sha256
              FROM {work_ref} AS work
             WHERE work.acquisition_id = candidate_acquisition_id
               AND work.status IN ('matched', 'unmatched', 'error')
             GROUP BY work.npi / 1000
        ), middle_bucket AS (
            SELECT leaf.bucket_id / 1000 AS bucket_id,
                   sum(leaf.row_count)::bigint AS row_count,
                   pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
                       pg_catalog.string_agg(
                           leaf.bucket_id::text || pg_catalog.chr(31)
                           || leaf.row_count::text || pg_catalog.chr(31)
                           || leaf.bucket_sha256,
                           pg_catalog.chr(30) ORDER BY leaf.bucket_id
                       ), 'UTF8'
                   )), 'hex') AS bucket_sha256
              FROM leaf_bucket AS leaf
             GROUP BY leaf.bucket_id / 1000
        )
        SELECT pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
            '{_TERMINAL_SET_CONTRACT}' || pg_catalog.chr(31)
            || candidate_acquisition_id || pg_catalog.chr(31)
            || COALESCE(pg_catalog.string_agg(
                middle.bucket_id::text || pg_catalog.chr(31)
                || middle.row_count::text || pg_catalog.chr(31)
                || middle.bucket_sha256,
                pg_catalog.chr(30) ORDER BY middle.bucket_id
            ), ''), 'UTF8'
        )), 'hex')
          FROM middle_bucket AS middle;
    $function$;
    """


def _acquisition_guard_function_sql(schema: str) -> str:
    function_ref = _qualified(schema, _ACQUISITION_GUARD)
    cohort_ref = _qualified(schema, _COHORT)
    member_ref = _qualified(schema, _MEMBER)
    work_ref = _qualified(schema, _WORK)
    resource_ref = _qualified(schema, _RESOURCE)
    terminal_set_ref = _qualified(schema, _TERMINAL_SET_FUNCTION)
    return f"""
    CREATE FUNCTION {function_ref}() RETURNS trigger LANGUAGE plpgsql
    SECURITY DEFINER SET search_path = pg_catalog AS $guard$
    DECLARE
        parent_npi_count bigint; parent_cohort_complete boolean;
        parent_collection_complete boolean; parent_endpoint_complete boolean;
        expected_acquisition_id text; actual_member_count bigint;
        actual_work_count bigint; actual_pending_count bigint;
        actual_leased_count bigint; actual_matched_count bigint;
        actual_unmatched_count bigint; actual_error_count bigint;
        actual_resource_count bigint; actual_terminal_set_sha256 text;
    BEGIN
        IF TG_OP IN ('DELETE', 'TRUNCATE') THEN
            RAISE EXCEPTION 'provider_directory_uhc_flex_acquisition_immutable'
                USING ERRCODE = '55000';
        END IF;
        IF TG_OP = 'INSERT' THEN
            SELECT cohort.npi_count, cohort.cohort_complete,
                   cohort.endpoint_collection_complete, cohort.endpoint_complete
              INTO parent_npi_count, parent_cohort_complete,
                   parent_collection_complete, parent_endpoint_complete
              FROM {cohort_ref} AS cohort
             WHERE cohort.cohort_id = NEW.cohort_id
             FOR SHARE;
            expected_acquisition_id := 'pdufpa_' || pg_catalog.substr(
                pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
                    NEW.storage_contract_id || pg_catalog.chr(31)
                    || NEW.cohort_id || pg_catalog.chr(31)
                    || NEW.acquisition_role || pg_catalog.chr(31)
                    || NEW.source_id || pg_catalog.chr(31)
                    || NEW.connector_id || pg_catalog.chr(31)
                    || NEW.query_contract_id || pg_catalog.chr(31)
                    || NEW.run_id || pg_catalog.chr(31)
                    || NEW.dataset_intent_id || pg_catalog.chr(31)
                    || NEW.expected_npi_count::text || pg_catalog.chr(31)
                    || 'false' || pg_catalog.chr(31) || 'false', 'UTF8'
                )), 'hex'), 1, 48);
            IF NEW.created_at IS DISTINCT FROM transaction_timestamp()
               OR NEW.updated_at IS DISTINCT FROM transaction_timestamp()
               OR NEW.acquisition_id IS DISTINCT FROM expected_acquisition_id
               OR parent_npi_count IS DISTINCT FROM NEW.expected_npi_count
               OR parent_cohort_complete IS DISTINCT FROM TRUE
               OR parent_collection_complete IS DISTINCT FROM FALSE
               OR parent_endpoint_complete IS DISTINCT FROM FALSE THEN
                RAISE EXCEPTION 'provider_directory_uhc_flex_acquisition_invalid'
                    USING ERRCODE = '23514';
            END IF;
            RETURN NEW;
        END IF;
        IF ROW(OLD.acquisition_id, OLD.storage_contract_id, OLD.cohort_id,
               OLD.acquisition_role, OLD.source_id, OLD.connector_id,
               OLD.query_contract_id, OLD.run_id, OLD.dataset_intent_id,
               OLD.expected_npi_count, OLD.endpoint_collection_complete,
               OLD.endpoint_complete, OLD.created_at)
           IS DISTINCT FROM
           ROW(NEW.acquisition_id, NEW.storage_contract_id, NEW.cohort_id,
               NEW.acquisition_role, NEW.source_id, NEW.connector_id,
               NEW.query_contract_id, NEW.run_id, NEW.dataset_intent_id,
               NEW.expected_npi_count, NEW.endpoint_collection_complete,
               NEW.endpoint_complete, NEW.created_at)
           OR OLD.status IS DISTINCT FROM 'building'
           OR NEW.status IS DISTINCT FROM 'sealed'
           OR OLD.cohort_complete IS DISTINCT FROM FALSE
           OR NEW.cohort_complete IS DISTINCT FROM TRUE
           OR OLD.pending_count IS NOT NULL OR OLD.leased_count IS NOT NULL
           OR OLD.matched_count IS NOT NULL OR OLD.unmatched_count IS NOT NULL
           OR OLD.error_count IS NOT NULL OR OLD.resource_count IS NOT NULL
           OR OLD.terminal_set_sha256 IS NOT NULL OR OLD.sealed_at IS NOT NULL
           OR NEW.updated_at IS DISTINCT FROM transaction_timestamp()
           OR NEW.sealed_at IS DISTINCT FROM transaction_timestamp() THEN
            RAISE EXCEPTION 'provider_directory_uhc_flex_acquisition_immutable'
                USING ERRCODE = '55000';
        END IF;
        LOCK TABLE {work_ref}, {resource_ref} IN SHARE MODE;
        SELECT count(*) INTO actual_member_count
          FROM {member_ref} AS member WHERE member.cohort_id = NEW.cohort_id;
        SELECT count(*)::bigint,
               count(*) FILTER (WHERE work.status = 'pending')::bigint,
               count(*) FILTER (WHERE work.status = 'leased')::bigint,
               count(*) FILTER (WHERE work.status = 'matched')::bigint,
               count(*) FILTER (WHERE work.status = 'unmatched')::bigint,
               count(*) FILTER (WHERE work.status = 'error')::bigint
          INTO actual_work_count, actual_pending_count, actual_leased_count,
               actual_matched_count, actual_unmatched_count, actual_error_count
          FROM {work_ref} AS work
         WHERE work.acquisition_id = NEW.acquisition_id;
        SELECT count(*)::bigint INTO actual_resource_count
          FROM {resource_ref} AS resource
          JOIN {work_ref} AS work
            ON work.acquisition_id = resource.acquisition_id
           AND work.cohort_id = resource.cohort_id AND work.npi = resource.npi
           AND work.attempt_count = resource.attempt
         WHERE resource.acquisition_id = NEW.acquisition_id
           AND work.status = 'matched';
        actual_terminal_set_sha256 := {terminal_set_ref}(NEW.acquisition_id);
        IF actual_member_count IS DISTINCT FROM NEW.expected_npi_count
           OR actual_work_count IS DISTINCT FROM NEW.expected_npi_count
           OR actual_pending_count <> 0 OR actual_leased_count <> 0
           OR actual_error_count <> 0
           OR actual_matched_count + actual_unmatched_count
              IS DISTINCT FROM NEW.expected_npi_count
           OR EXISTS (
                SELECT member.npi FROM {member_ref} AS member
                 WHERE member.cohort_id = NEW.cohort_id
                EXCEPT
                SELECT work.npi FROM {work_ref} AS work
                 WHERE work.acquisition_id = NEW.acquisition_id
           ) OR EXISTS (
                SELECT work.npi FROM {work_ref} AS work
                 WHERE work.acquisition_id = NEW.acquisition_id
                EXCEPT
                SELECT member.npi FROM {member_ref} AS member
                 WHERE member.cohort_id = NEW.cohort_id
           )
           OR NEW.pending_count IS DISTINCT FROM actual_pending_count
           OR NEW.leased_count IS DISTINCT FROM actual_leased_count
           OR NEW.matched_count IS DISTINCT FROM actual_matched_count
           OR NEW.unmatched_count IS DISTINCT FROM actual_unmatched_count
           OR NEW.error_count IS DISTINCT FROM actual_error_count
           OR NEW.resource_count IS DISTINCT FROM actual_resource_count
           OR NEW.terminal_set_sha256 IS DISTINCT FROM
              actual_terminal_set_sha256 THEN
            RAISE EXCEPTION 'provider_directory_uhc_flex_acquisition_incomplete'
                USING ERRCODE = '23514';
        END IF;
        RETURN NEW;
    END;
    $guard$;
    """


def _work_guard_function_sql(schema: str) -> str:
    function_ref = _qualified(schema, _WORK_GUARD)
    acquisition_ref = _qualified(schema, _ACQUISITION)
    resource_ref = _qualified(schema, _RESOURCE)
    return f"""
    CREATE FUNCTION {function_ref}() RETURNS trigger LANGUAGE plpgsql
    SECURITY DEFINER SET search_path = pg_catalog AS $guard$
    DECLARE
        action text; action_acquisition text; action_lease text;
        actual_resource_count bigint; expected_result_sha256 text;
        expected_terminal_record_sha256 text;
    BEGIN
        IF TG_OP IN ('DELETE', 'TRUNCATE') THEN
            RAISE EXCEPTION 'provider_directory_uhc_flex_work_immutable'
                USING ERRCODE = '55000';
        END IF;
        action := pg_catalog.current_setting('{_ACTION_SETTING}', TRUE);
        action_acquisition := pg_catalog.current_setting(
            '{_ACQUISITION_SETTING}', TRUE
        );
        action_lease := pg_catalog.current_setting('{_LEASE_SETTING}', TRUE);
        IF TG_OP = 'INSERT' THEN
            IF action IS DISTINCT FROM 'initialize'
               OR action_acquisition IS DISTINCT FROM NEW.acquisition_id
               OR NEW.created_at IS DISTINCT FROM transaction_timestamp()
               OR NEW.updated_at IS DISTINCT FROM transaction_timestamp()
               OR NOT EXISTS (
                    SELECT 1 FROM {acquisition_ref} AS acquisition
                     WHERE acquisition.acquisition_id = NEW.acquisition_id
                       AND acquisition.cohort_id = NEW.cohort_id
                       AND acquisition.status = 'building'
               ) THEN
                RAISE EXCEPTION 'provider_directory_uhc_flex_work_invalid'
                    USING ERRCODE = '23514';
            END IF;
            RETURN NEW;
        END IF;
        IF ROW(OLD.acquisition_id, OLD.cohort_id, OLD.npi, OLD.created_at)
           IS DISTINCT FROM
           ROW(NEW.acquisition_id, NEW.cohort_id, NEW.npi, NEW.created_at)
           OR action_acquisition IS DISTINCT FROM NEW.acquisition_id
           OR NEW.updated_at IS DISTINCT FROM transaction_timestamp() THEN
            RAISE EXCEPTION 'provider_directory_uhc_flex_work_immutable'
                USING ERRCODE = '55000';
        END IF;
        IF NOT EXISTS (
            SELECT 1 FROM {acquisition_ref} AS acquisition
             WHERE acquisition.acquisition_id = NEW.acquisition_id
               AND acquisition.status = 'building'
        ) THEN
            RAISE EXCEPTION 'provider_directory_uhc_flex_acquisition_sealed'
                USING ERRCODE = '55000';
        END IF;
        IF action = 'claim' THEN
            IF NOT (
                (OLD.status = 'pending' OR (
                    OLD.status = 'leased'
                    AND OLD.lease_expires_at <= clock_timestamp()
                ))
                AND NEW.status = 'leased'
                AND NEW.attempt_count = OLD.attempt_count + 1
                AND NEW.lease_token = action_lease
                AND NEW.lease_token ~ '^[0-9a-f]{{64}}$'
                AND NEW.lease_token IS DISTINCT FROM OLD.lease_token
                AND NEW.lease_expires_at > clock_timestamp()
                AND NEW.lease_heartbeat_at IS NOT NULL
                AND NEW.result_sha256 IS NULL AND NEW.resource_count IS NULL
                AND NEW.error_code IS NULL
                AND NEW.terminal_record_sha256 IS NULL
                AND NEW.terminal_at IS NULL
            ) THEN
                RAISE EXCEPTION 'provider_directory_uhc_flex_claim_invalid'
                    USING ERRCODE = '55000';
            END IF;
            RETURN NEW;
        END IF;
        IF action = 'heartbeat' THEN
            IF NOT (
                OLD.status = 'leased' AND OLD.lease_expires_at > clock_timestamp()
                AND NEW.status = 'leased'
                AND NEW.attempt_count = OLD.attempt_count
                AND NEW.lease_token = OLD.lease_token
                AND action_lease = OLD.lease_token
                AND NEW.lease_expires_at > OLD.lease_expires_at
                AND NEW.lease_heartbeat_at >= OLD.lease_heartbeat_at
                AND ROW(NEW.result_sha256, NEW.resource_count, NEW.error_code,
                        NEW.terminal_record_sha256, NEW.terminal_at)
                    IS NOT DISTINCT FROM
                    ROW(OLD.result_sha256, OLD.resource_count, OLD.error_code,
                        OLD.terminal_record_sha256, OLD.terminal_at)
            ) THEN
                RAISE EXCEPTION 'provider_directory_uhc_flex_lease_lost'
                    USING ERRCODE = '55000';
            END IF;
            RETURN NEW;
        END IF;
        IF action = 'release' THEN
            IF NOT (
                OLD.status = 'leased' AND OLD.lease_expires_at > clock_timestamp()
                AND action_lease = OLD.lease_token
                AND NEW.status = 'pending'
                AND NEW.attempt_count = OLD.attempt_count
                AND NEW.lease_token IS NULL AND NEW.lease_expires_at IS NULL
                AND NEW.lease_heartbeat_at IS NULL
                AND NEW.result_sha256 IS NULL AND NEW.resource_count IS NULL
                AND NEW.error_code IS NULL
                AND NEW.terminal_record_sha256 IS NULL AND NEW.terminal_at IS NULL
                AND NOT EXISTS (
                    SELECT 1 FROM {resource_ref} AS resource
                     WHERE resource.acquisition_id = OLD.acquisition_id
                       AND resource.cohort_id = OLD.cohort_id
                       AND resource.npi = OLD.npi
                       AND resource.attempt = OLD.attempt_count
                )
            ) THEN
                RAISE EXCEPTION 'provider_directory_uhc_flex_lease_lost'
                    USING ERRCODE = '55000';
            END IF;
            RETURN NEW;
        END IF;
        IF action IS DISTINCT FROM 'terminal'
           OR OLD.status IS DISTINCT FROM 'leased'
           OR OLD.lease_expires_at <= clock_timestamp()
           OR action_lease IS DISTINCT FROM OLD.lease_token
           OR NEW.status NOT IN ('matched', 'unmatched', 'error')
           OR NEW.attempt_count IS DISTINCT FROM OLD.attempt_count
           OR NEW.lease_token IS NOT NULL OR NEW.lease_expires_at IS NOT NULL
           OR NEW.lease_heartbeat_at IS NOT NULL
           OR NEW.terminal_at IS DISTINCT FROM transaction_timestamp() THEN
            RAISE EXCEPTION 'provider_directory_uhc_flex_lease_lost'
                USING ERRCODE = '55000';
        END IF;
        SELECT count(*)::bigint,
               pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
                   '{{"contract_id":' || pg_catalog.to_json(
                       '{_QUERY_CONTRACT}'::text
                   )::text || ',"outcome":' || pg_catalog.to_json(
                       NEW.status::text
                   )::text || ',"requested_npi":' || NEW.npi::text
                   || ',"resources":[' || COALESCE(pg_catalog.string_agg(
                       '{{"resource_id":' || pg_catalog.to_json(
                           resource.resource_id
                       )::text || ',"sha256":' || pg_catalog.to_json(
                           resource.payload_sha256
                       )::text || '}}', ',' ORDER BY resource.resource_id
                   ), '') || ']}}', 'UTF8'
               )), 'hex')
          INTO actual_resource_count, expected_result_sha256
          FROM {resource_ref} AS resource
         WHERE resource.acquisition_id = NEW.acquisition_id
           AND resource.cohort_id = NEW.cohort_id
           AND resource.npi = NEW.npi AND resource.attempt = NEW.attempt_count;
        IF (NEW.status = 'matched' AND NOT (
                actual_resource_count BETWEEN 1 AND 16
                AND NEW.resource_count = actual_resource_count
                AND NEW.result_sha256 = expected_result_sha256
                AND NEW.error_code IS NULL
           )) OR (NEW.status = 'unmatched' AND NOT (
                actual_resource_count = 0 AND NEW.resource_count = 0
                AND NEW.result_sha256 = expected_result_sha256
                AND NEW.error_code IS NULL
           )) OR (NEW.status = 'error' AND NOT (
                actual_resource_count = 0 AND NEW.resource_count = 0
                AND NEW.result_sha256 IS NULL
                AND NEW.error_code ~ '^[a-z][a-z0-9_]{{0,127}}$'
           )) THEN
            RAISE EXCEPTION 'provider_directory_uhc_flex_result_invalid'
                USING ERRCODE = '23514';
        END IF;
        expected_terminal_record_sha256 := pg_catalog.encode(
            pg_catalog.sha256(pg_catalog.convert_to(
                '{_TERMINAL_RECORD_CONTRACT}' || pg_catalog.chr(31)
                || NEW.acquisition_id || pg_catalog.chr(31)
                || NEW.npi::text || pg_catalog.chr(31)
                || NEW.status || pg_catalog.chr(31)
                || COALESCE(NEW.result_sha256, '') || pg_catalog.chr(31)
                || NEW.resource_count::text || pg_catalog.chr(31)
                || COALESCE(NEW.error_code, ''), 'UTF8'
            )), 'hex'
        );
        IF NEW.terminal_record_sha256 IS DISTINCT FROM
           expected_terminal_record_sha256 THEN
            RAISE EXCEPTION 'provider_directory_uhc_flex_result_invalid'
                USING ERRCODE = '23514';
        END IF;
        RETURN NEW;
    END;
    $guard$;
    """


def _resource_guard_function_sql(schema: str) -> str:
    function_ref = _qualified(schema, _RESOURCE_GUARD)
    work_ref = _qualified(schema, _WORK)
    return f"""
    CREATE FUNCTION {function_ref}() RETURNS trigger LANGUAGE plpgsql
    SECURITY DEFINER SET search_path = pg_catalog AS $guard$
    DECLARE payload jsonb;
    BEGIN
        IF TG_OP <> 'INSERT' THEN
            RAISE EXCEPTION 'provider_directory_uhc_flex_resource_immutable'
                USING ERRCODE = '55000';
        END IF;
        BEGIN
            payload := NEW.payload_json_text::jsonb;
        EXCEPTION WHEN OTHERS THEN
            RAISE EXCEPTION 'provider_directory_uhc_flex_resource_invalid'
                USING ERRCODE = '23514';
        END;
        IF pg_catalog.current_setting('{_ACTION_SETTING}', TRUE)
               IS DISTINCT FROM 'resource'
           OR pg_catalog.current_setting('{_ACQUISITION_SETTING}', TRUE)
               IS DISTINCT FROM NEW.acquisition_id
           OR NEW.created_at IS DISTINCT FROM transaction_timestamp()
           OR NEW.payload_sha256 IS DISTINCT FROM pg_catalog.encode(
                pg_catalog.sha256(pg_catalog.convert_to(
                    NEW.payload_json_text, 'UTF8'
                )), 'hex'
           )
           OR payload ->> 'resourceType' IS DISTINCT FROM 'Practitioner'
           OR payload ->> 'id' IS DISTINCT FROM NEW.resource_id
           OR pg_catalog.jsonb_typeof(payload -> 'identifier') <> 'array'
           OR NOT EXISTS (
                SELECT 1 FROM pg_catalog.jsonb_array_elements(
                    payload -> 'identifier'
                ) AS identifier
                 WHERE identifier ->> 'system' = '{_NPI_SYSTEM}'
                   AND pg_catalog.jsonb_typeof(identifier -> 'value') = 'string'
                   AND identifier ->> 'value' = NEW.npi::text
           )
           OR EXISTS (
                SELECT 1 FROM pg_catalog.jsonb_array_elements(
                    payload -> 'identifier'
                ) AS identifier
                 WHERE identifier ->> 'system' = '{_NPI_SYSTEM}'
                   AND (pg_catalog.jsonb_typeof(identifier -> 'value') <> 'string'
                        OR identifier ->> 'value' <> NEW.npi::text)
           )
           OR NOT EXISTS (
                SELECT 1 FROM {work_ref} AS work
                 WHERE work.acquisition_id = NEW.acquisition_id
                   AND work.cohort_id = NEW.cohort_id AND work.npi = NEW.npi
                   AND work.attempt_count = NEW.attempt
                   AND work.status = 'leased'
                   AND work.lease_token = pg_catalog.current_setting(
                       '{_LEASE_SETTING}', TRUE
                   )
                   AND work.lease_expires_at > clock_timestamp()
           ) THEN
            RAISE EXCEPTION 'provider_directory_uhc_flex_resource_invalid'
                USING ERRCODE = '23514';
        END IF;
        RETURN NEW;
    END;
    $guard$;
    """


def _create_acquisition_table(schema: str) -> None:
    op.create_table(
        _ACQUISITION,
        sa.Column("acquisition_id", sa.String(55), nullable=False),
        sa.Column("storage_contract_id", sa.String(96), nullable=False),
        sa.Column("cohort_id", sa.String(54), nullable=False),
        sa.Column("acquisition_role", sa.String(16), nullable=False),
        sa.Column("source_id", sa.String(64), nullable=False),
        sa.Column("connector_id", sa.String(64), nullable=False),
        sa.Column("query_contract_id", sa.String(96), nullable=False),
        sa.Column("run_id", sa.String(55), nullable=False),
        sa.Column("dataset_intent_id", sa.String(55), nullable=False),
        sa.Column("expected_npi_count", sa.BigInteger(), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("cohort_complete", sa.Boolean(), nullable=False),
        sa.Column("endpoint_collection_complete", sa.Boolean(), nullable=False),
        sa.Column("endpoint_complete", sa.Boolean(), nullable=False),
        sa.Column("pending_count", sa.BigInteger()),
        sa.Column("leased_count", sa.BigInteger()),
        sa.Column("matched_count", sa.BigInteger()),
        sa.Column("unmatched_count", sa.BigInteger()),
        sa.Column("error_count", sa.BigInteger()),
        sa.Column("resource_count", sa.BigInteger()),
        sa.Column("terminal_set_sha256", sa.String(64)),
        sa.Column(
            "created_at", sa.TIMESTAMP(timezone=True), nullable=False,
            server_default=sa.text("transaction_timestamp()"),
        ),
        sa.Column(
            "updated_at", sa.TIMESTAMP(timezone=True), nullable=False,
            server_default=sa.text("transaction_timestamp()"),
        ),
        sa.Column("sealed_at", sa.TIMESTAMP(timezone=True)),
        sa.PrimaryKeyConstraint(
            "acquisition_id", name="pd_uhc_flex_practitioner_acquisition_pkey"
        ),
        sa.UniqueConstraint(
            "acquisition_id", "cohort_id",
            name="pd_uhc_flex_practitioner_acquisition_cohort_key",
        ),
        sa.UniqueConstraint(
            "cohort_id", "dataset_intent_id", "acquisition_role",
            name="pd_uhc_flex_practitioner_intent_role_key",
        ),
        sa.UniqueConstraint(
            "run_id", name="pd_uhc_flex_practitioner_run_key"
        ),
        sa.ForeignKeyConstraint(
            ["cohort_id"], [f"{schema}.{_COHORT}.cohort_id"],
            name="pd_uhc_flex_practitioner_acquisition_cohort_fkey",
        ),
        sa.CheckConstraint(
            "acquisition_id ~ '^pdufpa_[0-9a-f]{48}$' AND "
            f"storage_contract_id = '{_STORAGE_CONTRACT}' AND "
            "acquisition_role IN ('baseline', 'candidate') AND "
            f"source_id = '{_SOURCE_ID}' AND connector_id = '{_CONNECTOR_ID}' "
            f"AND query_contract_id = '{_QUERY_CONTRACT}' AND "
            "run_id ~ '^pdufpr_[0-9a-f]{48}$' AND "
            "dataset_intent_id ~ '^pdufdi_[0-9a-f]{48}$' AND "
            "expected_npi_count > 0 AND endpoint_collection_complete IS FALSE "
            "AND endpoint_complete IS FALSE",
            name="pd_uhc_flex_practitioner_acquisition_identity_check",
        ),
        sa.CheckConstraint(
            "(status = 'building' AND cohort_complete IS FALSE AND "
            "pending_count IS NULL AND leased_count IS NULL AND "
            "matched_count IS NULL AND unmatched_count IS NULL AND "
            "error_count IS NULL AND resource_count IS NULL AND "
            "terminal_set_sha256 IS NULL AND sealed_at IS NULL) OR "
            "(status = 'sealed' AND cohort_complete IS TRUE AND "
            "pending_count = 0 AND leased_count = 0 AND matched_count >= 0 "
            "AND unmatched_count >= 0 AND error_count = 0 AND "
            "matched_count + unmatched_count = "
            "expected_npi_count AND resource_count >= 0 AND "
            "terminal_set_sha256 ~ '^[0-9a-f]{64}$' AND sealed_at IS NOT NULL)",
            name="pd_uhc_flex_practitioner_acquisition_state_check",
        ),
        schema=schema,
    )


def _create_work_table(schema: str) -> None:
    op.create_table(
        _WORK,
        sa.Column("acquisition_id", sa.String(55), nullable=False),
        sa.Column("cohort_id", sa.String(54), nullable=False),
        sa.Column("npi", sa.BigInteger(), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("attempt_count", sa.Integer(), nullable=False),
        sa.Column("lease_token", sa.String(64)),
        sa.Column("lease_expires_at", sa.TIMESTAMP(timezone=True)),
        sa.Column("lease_heartbeat_at", sa.TIMESTAMP(timezone=True)),
        sa.Column("result_sha256", sa.String(64)),
        sa.Column("resource_count", sa.Integer()),
        sa.Column("error_code", sa.String(128)),
        sa.Column("terminal_record_sha256", sa.String(64)),
        sa.Column(
            "created_at", sa.TIMESTAMP(timezone=True), nullable=False,
            server_default=sa.text("transaction_timestamp()"),
        ),
        sa.Column(
            "updated_at", sa.TIMESTAMP(timezone=True), nullable=False,
            server_default=sa.text("transaction_timestamp()"),
        ),
        sa.Column("terminal_at", sa.TIMESTAMP(timezone=True)),
        sa.PrimaryKeyConstraint(
            "acquisition_id", "npi",
            name="pd_uhc_flex_practitioner_work_pkey",
        ),
        sa.UniqueConstraint(
            "acquisition_id", "cohort_id", "npi",
            name="pd_uhc_flex_practitioner_work_cohort_key",
        ),
        sa.ForeignKeyConstraint(
            ["acquisition_id", "cohort_id"],
            [f"{schema}.{_ACQUISITION}.acquisition_id",
             f"{schema}.{_ACQUISITION}.cohort_id"],
            name="pd_uhc_flex_practitioner_work_acquisition_fkey",
        ),
        sa.ForeignKeyConstraint(
            ["cohort_id", "npi"],
            [f"{schema}.{_MEMBER}.cohort_id", f"{schema}.{_MEMBER}.npi"],
            name="pd_uhc_flex_practitioner_work_member_fkey",
        ),
        sa.CheckConstraint(
            "npi BETWEEN 1000000000 AND 2999999999 AND attempt_count >= 0 "
            "AND (lease_token IS NULL OR lease_token ~ '^[0-9a-f]{64}$') "
            "AND (result_sha256 IS NULL OR result_sha256 ~ '^[0-9a-f]{64}$') "
            "AND (terminal_record_sha256 IS NULL OR "
            "terminal_record_sha256 ~ '^[0-9a-f]{64}$')",
            name="pd_uhc_flex_practitioner_work_value_check",
        ),
        sa.CheckConstraint(
            "(status = 'pending' AND attempt_count >= 0 AND lease_token IS NULL "
            "AND lease_expires_at IS NULL AND lease_heartbeat_at IS NULL AND "
            "result_sha256 IS NULL AND resource_count IS NULL AND "
            "error_code IS NULL AND terminal_record_sha256 IS NULL AND "
            "terminal_at IS NULL) OR "
            "(status = 'leased' AND attempt_count > 0 AND lease_token IS NOT NULL "
            "AND lease_expires_at IS NOT NULL AND lease_heartbeat_at IS NOT NULL "
            "AND result_sha256 IS NULL AND resource_count IS NULL AND "
            "error_code IS NULL AND terminal_record_sha256 IS NULL AND "
            "terminal_at IS NULL) OR "
            "(status IN ('matched', 'unmatched') AND attempt_count > 0 AND "
            "lease_token IS NULL AND lease_expires_at IS NULL AND "
            "lease_heartbeat_at IS NULL AND result_sha256 IS NOT NULL AND "
            "resource_count >= 0 AND error_code IS NULL AND "
            "terminal_record_sha256 IS NOT NULL AND terminal_at IS NOT NULL) OR "
            "(status = 'error' AND attempt_count > 0 AND lease_token IS NULL "
            "AND lease_expires_at IS NULL AND lease_heartbeat_at IS NULL AND "
            "result_sha256 IS NULL AND resource_count = 0 AND "
            "error_code ~ '^[a-z][a-z0-9_]{0,127}$' AND "
            "terminal_record_sha256 IS NOT NULL AND terminal_at IS NOT NULL)",
            name="pd_uhc_flex_practitioner_work_state_check",
        ),
        schema=schema,
    )
    op.create_index(
        "pd_uhc_flex_practitioner_work_claim_idx", _WORK,
        ["acquisition_id", "status", "lease_expires_at", "npi"],
        schema=schema,
    )


def _create_resource_table(schema: str) -> None:
    op.create_table(
        _RESOURCE,
        sa.Column("acquisition_id", sa.String(55), nullable=False),
        sa.Column("cohort_id", sa.String(54), nullable=False),
        sa.Column("npi", sa.BigInteger(), nullable=False),
        sa.Column("attempt", sa.Integer(), nullable=False),
        sa.Column("resource_id", sa.String(64), nullable=False),
        sa.Column("payload_sha256", sa.String(64), nullable=False),
        sa.Column("payload_json_text", sa.Text(), nullable=False),
        sa.Column(
            "created_at", sa.TIMESTAMP(timezone=True), nullable=False,
            server_default=sa.text("transaction_timestamp()"),
        ),
        sa.PrimaryKeyConstraint(
            "acquisition_id", "npi", "attempt", "resource_id",
            name="pd_uhc_flex_practitioner_resource_pkey",
        ),
        sa.ForeignKeyConstraint(
            ["acquisition_id", "cohort_id", "npi"],
            [f"{schema}.{_WORK}.acquisition_id",
             f"{schema}.{_WORK}.cohort_id", f"{schema}.{_WORK}.npi"],
            name="pd_uhc_flex_practitioner_resource_work_fkey",
        ),
        sa.CheckConstraint(
            "npi BETWEEN 1000000000 AND 2999999999 AND attempt > 0 AND "
            "resource_id ~ '^[A-Za-z0-9.-]{1,64}$' AND "
            "payload_sha256 ~ '^[0-9a-f]{64}$' AND "
            "octet_length(payload_json_text) BETWEEN 2 AND 1048576",
            name="pd_uhc_flex_practitioner_resource_value_check",
        ),
        schema=schema,
    )


def _install_guards(schema: str) -> None:
    acquisition_ref = _qualified(schema, _ACQUISITION)
    work_ref = _qualified(schema, _WORK)
    resource_ref = _qualified(schema, _RESOURCE)
    acquisition_guard = _qualified(schema, _ACQUISITION_GUARD)
    work_guard = _qualified(schema, _WORK_GUARD)
    resource_guard = _qualified(schema, _RESOURCE_GUARD)
    terminal_set_ref = _qualified(schema, _TERMINAL_SET_FUNCTION)
    statements = (
        f"REVOKE ALL ON FUNCTION {terminal_set_ref}(text) FROM PUBLIC;",
        *(f"REVOKE ALL ON FUNCTION {ref}() FROM PUBLIC;" for ref in
          (acquisition_guard, work_guard, resource_guard)),
        *(f"REVOKE ALL ON TABLE {ref} FROM PUBLIC;" for ref in
          (acquisition_ref, work_ref, resource_ref)),
        f"CREATE TRIGGER pd_uhc_flex_practitioner_acquisition_guard "
        f"BEFORE INSERT OR UPDATE OR DELETE ON {acquisition_ref} FOR EACH ROW "
        f"EXECUTE FUNCTION {acquisition_guard}();",
        f"CREATE TRIGGER pd_uhc_flex_practitioner_acquisition_truncate "
        f"BEFORE TRUNCATE ON {acquisition_ref} FOR EACH STATEMENT "
        f"EXECUTE FUNCTION {acquisition_guard}();",
        f"CREATE TRIGGER pd_uhc_flex_practitioner_work_guard "
        f"BEFORE INSERT OR UPDATE OR DELETE ON {work_ref} FOR EACH ROW "
        f"EXECUTE FUNCTION {work_guard}();",
        f"CREATE TRIGGER pd_uhc_flex_practitioner_work_truncate "
        f"BEFORE TRUNCATE ON {work_ref} FOR EACH STATEMENT "
        f"EXECUTE FUNCTION {work_guard}();",
        f"CREATE TRIGGER pd_uhc_flex_practitioner_resource_guard "
        f"BEFORE INSERT OR UPDATE OR DELETE ON {resource_ref} FOR EACH ROW "
        f"EXECUTE FUNCTION {resource_guard}();",
        f"CREATE TRIGGER pd_uhc_flex_practitioner_resource_truncate "
        f"BEFORE TRUNCATE ON {resource_ref} FOR EACH STATEMENT "
        f"EXECUTE FUNCTION {resource_guard}();",
        *(f"ALTER TABLE {table_ref} ENABLE ALWAYS TRIGGER {_quoted(trigger)};"
          for table_ref, trigger in (
              (acquisition_ref, "pd_uhc_flex_practitioner_acquisition_guard"),
              (acquisition_ref, "pd_uhc_flex_practitioner_acquisition_truncate"),
              (work_ref, "pd_uhc_flex_practitioner_work_guard"),
              (work_ref, "pd_uhc_flex_practitioner_work_truncate"),
              (resource_ref, "pd_uhc_flex_practitioner_resource_guard"),
              (resource_ref, "pd_uhc_flex_practitioner_resource_truncate"),
          )),
    )
    for statement in statements:
        op.execute(statement)


def _downgrade_lock_sql(schema: str) -> str:
    return "LOCK TABLE " + ", ".join(
        _qualified(schema, table)
        for table in (_RESOURCE, _WORK, _ACQUISITION)
    ) + " IN ACCESS EXCLUSIVE MODE;"


def _downgrade_fence_sql(schema: str) -> str:
    return f"""
    DO $downgrade$ BEGIN
        IF EXISTS (SELECT 1 FROM {_qualified(schema, _ACQUISITION)} LIMIT 1)
           OR EXISTS (SELECT 1 FROM {_qualified(schema, _WORK)} LIMIT 1)
           OR EXISTS (SELECT 1 FROM {_qualified(schema, _RESOURCE)} LIMIT 1)
        THEN
            RAISE EXCEPTION 'provider_directory_uhc_flex_acquisition_downgrade_blocked'
                USING ERRCODE = '55000';
        END IF;
    END; $downgrade$;
    """


def upgrade() -> None:
    schema = _schema()
    _create_acquisition_table(schema)
    _create_work_table(schema)
    _create_resource_table(schema)
    op.execute(_terminal_set_function_sql(schema))
    op.execute(_acquisition_guard_function_sql(schema))
    op.execute(_work_guard_function_sql(schema))
    op.execute(_resource_guard_function_sql(schema))
    _install_guards(schema)


def downgrade() -> None:
    schema = _schema()
    op.execute(_downgrade_lock_sql(schema))
    op.execute(_downgrade_fence_sql(schema))
    op.drop_table(_RESOURCE, schema=schema)
    op.drop_index(
        "pd_uhc_flex_practitioner_work_claim_idx",
        table_name=_WORK,
        schema=schema,
    )
    op.drop_table(_WORK, schema=schema)
    op.drop_table(_ACQUISITION, schema=schema)
    for function_name in (
        _RESOURCE_GUARD, _WORK_GUARD, _ACQUISITION_GUARD
    ):
        op.execute(f"DROP FUNCTION {_qualified(schema, function_name)}();")
    op.execute(
        f"DROP FUNCTION {_qualified(schema, _TERMINAL_SET_FUNCTION)}(text);"
    )
