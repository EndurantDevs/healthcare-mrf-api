# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Add complete, scalable NPPES registry and listing-chain admission.

Revision ID: 20260808220000_public_evidence_nppes_registry_admission
Revises: 20260808210000_provider_directory_subset_payload_guard_repair
"""

from __future__ import annotations

import os

from alembic import op


revision = "20260808220000_public_evidence_nppes_registry_admission"
down_revision = "20260808210000_provider_directory_subset_payload_guard_repair"
branch_labels = None
depends_on = None

_COMMON = "public_evidence_record"
_LINK = "public_evidence_record_source_link"
_TYPED = "public_evidence_npi_enumeration"
_SOURCE = "public_evidence_source_record"
_RELEASE = "public_evidence_source_release"
_ADMISSION = "public_evidence_nppes_registry_admission"
_ADMISSION_SEAL = "public_evidence_nppes_registry_admission_seal"
_MEMBER = "public_evidence_nppes_registry_member"
_CHAIN = "public_evidence_nppes_registry_chain_admission"
_CHAIN_SEAL = "public_evidence_nppes_registry_chain_admission_seal"
_CHAIN_ARCHIVE = "public_evidence_nppes_registry_chain_archive"
_OLD_RECORD_TABLES = (_COMMON, _LINK, _TYPED)
_NEW_TABLES = (
    _ADMISSION,
    _ADMISSION_SEAL,
    _MEMBER,
    _CHAIN,
    _CHAIN_SEAL,
    _CHAIN_ARCHIVE,
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


def _qf(schema: str, function: str) -> str:
    return f"{_q(schema)}.{_q(function)}"


def _require_empty_legacy_nppes(schema: str) -> None:
    common = _qt(schema, _COMMON)
    link = _qt(schema, _LINK)
    typed = _qt(schema, _TYPED)
    source = _qt(schema, _SOURCE)
    release = _qt(schema, _RELEASE)
    op.execute(
        "LOCK TABLE "
        + ", ".join((common, link, typed, source, release))
        + " IN ACCESS EXCLUSIVE MODE;"
    )
    op.execute(
        f"""
        DO $block$ BEGIN
            IF EXISTS (SELECT 1 FROM {common} LIMIT 1)
               OR EXISTS (SELECT 1 FROM {link} LIMIT 1)
               OR EXISTS (SELECT 1 FROM {typed} LIMIT 1)
               OR EXISTS (SELECT 1 FROM {source}
                           WHERE record_kind='nppes_registry_record' LIMIT 1)
               OR EXISTS (SELECT 1 FROM {release}
                           WHERE source_kind='nppes_entity_address' LIMIT 1)
            THEN
                RAISE EXCEPTION 'nppes_registry_admission_upgrade_requires_empty_slice'
                    USING ERRCODE='55000';
            END IF;
        END; $block$;
        """
    )


def _create_hash_helpers(schema: str) -> None:
    framed = _qf(schema, "public_evidence_nppes_framed_digest")
    payload_digest = _qf(schema, "nppes_registry_payload_digest")
    archive_period = _qf(schema, "public_evidence_nppes_archive_period")
    primary_period = _qf(schema, "public_evidence_nppes_primary_period")
    tree_node = _qf(schema, "public_evidence_nppes_tree_node")
    merkle_sfunc = _qf(schema, "public_evidence_nppes_merkle_sfunc")
    merkle_final = _qf(schema, "public_evidence_nppes_merkle_final")
    merkle = _qf(schema, "public_evidence_nppes_merkle_root")
    member_valid = _qf(schema, "public_evidence_nppes_member_valid")
    record_digest = _qf(schema, "public_evidence_record_digest")
    statements = (
        f"""
        CREATE FUNCTION {framed}(candidate_kind text, candidate_payload text)
        RETURNS bytea LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE
        SET search_path=pg_catalog AS $function$
        DECLARE domain_bytes bytea;
        BEGIN
            domain_bytes := CASE candidate_kind
                WHEN 'payload' THEN convert_to(
                    'HEALTHPORTA_NPPES_REGISTRY_PAYLOAD_V1', 'UTF8')
                    || decode('00', 'hex')
                WHEN 'leaf' THEN convert_to(
                    'HEALTHPORTA_NPPES_REGISTRY_LEAF_V1', 'UTF8')
                    || decode('00', 'hex')
                WHEN 'manifest' THEN convert_to(
                    'HEALTHPORTA_NPPES_REGISTRY_MANIFEST_V1', 'UTF8')
                    || decode('00', 'hex')
                ELSE NULL
            END;
            IF domain_bytes IS NULL THEN
                RAISE EXCEPTION 'public_evidence_nppes_digest_kind_invalid'
                    USING ERRCODE='22023';
            END IF;
            RETURN sha256(domain_bytes
                || int8send(octet_length(convert_to(candidate_payload, 'UTF8'))::bigint)
                || convert_to(candidate_payload, 'UTF8'));
        END;
        $function$;
        """,
        f"""
        CREATE FUNCTION {payload_digest}(
            candidate_npi text, candidate_entity_type_code text,
            candidate_provider_enumeration_date date,
            candidate_last_update_date date,
            candidate_npi_deactivation_date date,
            candidate_npi_reactivation_date date
        ) RETURNS bytea LANGUAGE sql IMMUTABLE PARALLEL SAFE
        SET search_path=pg_catalog AS $function$
            SELECT CASE
                WHEN candidate_npi IS NULL
                  OR NOT {_qf(schema, 'public_evidence_npi_valid')}(candidate_npi)
                  OR candidate_entity_type_code IS NOT NULL
                     AND candidate_entity_type_code NOT IN ('1', '2')
                THEN NULL
                ELSE {framed}(
                    'payload',
                    '{{"contract":"healthporta_nppes_registry_csv_row_payload_sha256_v1"' ||
                    ',"entity_type_code":' || COALESCE(to_json(candidate_entity_type_code)::text, 'null') ||
                    ',"last_update_date":' || COALESCE(to_json(to_char(candidate_last_update_date, 'YYYY-MM-DD'))::text, 'null') ||
                    ',"npi":' || to_json(candidate_npi)::text ||
                    ',"npi_deactivation_date":' || COALESCE(to_json(to_char(candidate_npi_deactivation_date, 'YYYY-MM-DD'))::text, 'null') ||
                    ',"npi_reactivation_date":' || COALESCE(to_json(to_char(candidate_npi_reactivation_date, 'YYYY-MM-DD'))::text, 'null') ||
                    ',"provider_enumeration_date":' || COALESCE(to_json(to_char(candidate_provider_enumeration_date, 'YYYY-MM-DD'))::text, 'null') || '}}'
                )
            END;
        $function$;
        """,
        f"""
        CREATE FUNCTION {archive_period}(candidate_name text)
        RETURNS date[] LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE
        SET search_path=pg_catalog AS $function$
        DECLARE
            matched text[]; start_date date; end_date date;
            month_number integer; year_number integer;
        BEGIN
            matched := regexp_match(candidate_name,
                '^NPPES_Data_Dissemination_(January|February|March|April|May|June|July|August|September|October|November|December)_([0-9]{{4}})_V2[.]zip$');
            IF matched IS NOT NULL THEN
                month_number := array_position(
                    ARRAY['January','February','March','April','May','June',
                          'July','August','September','October','November','December'],
                    matched[1]);
                year_number := matched[2]::integer;
                IF year_number NOT BETWEEN 1 AND 9999 THEN
                    RETURN NULL;
                END IF;
                RETURN ARRAY[make_date(year_number, month_number, 1), NULL::date];
            END IF;
            matched := regexp_match(candidate_name,
                '^NPPES_Data_Dissemination_([0-9]{{6}})_([0-9]{{6}})_Weekly_V2[.]zip$');
            IF matched IS NULL THEN
                RETURN NULL;
            END IF;
            start_date := make_date(
                CASE WHEN substring(matched[1], 5, 2)::integer >= 69
                     THEN 1900 ELSE 2000 END
                    + substring(matched[1], 5, 2)::integer,
                substring(matched[1], 1, 2)::integer,
                substring(matched[1], 3, 2)::integer);
            end_date := make_date(
                CASE WHEN substring(matched[2], 5, 2)::integer >= 69
                     THEN 1900 ELSE 2000 END
                    + substring(matched[2], 5, 2)::integer,
                substring(matched[2], 1, 2)::integer,
                substring(matched[2], 3, 2)::integer);
            IF to_char(start_date, 'MMDDYY') <> matched[1]
               OR to_char(end_date, 'MMDDYY') <> matched[2]
               OR end_date < start_date THEN
                RETURN NULL;
            END IF;
            RETURN ARRAY[start_date, end_date];
        EXCEPTION WHEN OTHERS THEN
            RETURN NULL;
        END;
        $function$;
        """,
        f"""
        CREATE FUNCTION {primary_period}(candidate_name text)
        RETURNS date[] LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE
        SET search_path=pg_catalog AS $function$
        DECLARE matched text[]; start_date date; end_date date;
        BEGIN
            matched := regexp_match(candidate_name,
                '^npidata_pfile_([0-9]{{8}})-([0-9]{{8}})[.]csv$');
            IF matched IS NULL THEN
                RETURN NULL;
            END IF;
            start_date := make_date(
                substring(matched[1], 1, 4)::integer,
                substring(matched[1], 5, 2)::integer,
                substring(matched[1], 7, 2)::integer);
            end_date := make_date(
                substring(matched[2], 1, 4)::integer,
                substring(matched[2], 5, 2)::integer,
                substring(matched[2], 7, 2)::integer);
            IF to_char(start_date, 'YYYYMMDD') <> matched[1]
               OR to_char(end_date, 'YYYYMMDD') <> matched[2]
               OR end_date < start_date THEN
                RETURN NULL;
            END IF;
            RETURN ARRAY[start_date, end_date];
        EXCEPTION WHEN OTHERS THEN
            RETURN NULL;
        END;
        $function$;
        """,
        f"""
        CREATE FUNCTION {tree_node}(left_sha256 bytea, right_sha256 bytea)
        RETURNS bytea LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
        SET search_path=pg_catalog AS $function$
            SELECT CASE
                WHEN octet_length(left_sha256)=32
                 AND octet_length(right_sha256)=32
                THEN sha256(convert_to(
                    'HEALTHPORTA_NPPES_REGISTRY_NODE_V1', 'UTF8')
                    || decode('0001', 'hex') || left_sha256 || right_sha256)
                ELSE NULL
            END;
        $function$;
        """,
        f"""
        CREATE FUNCTION {merkle_sfunc}(
            candidate_state bytea[], candidate_ordinal bigint, candidate_leaf bytea
        ) RETURNS bytea[] LANGUAGE plpgsql IMMUTABLE PARALLEL UNSAFE
        SET search_path=pg_catalog AS $function$
        DECLARE
            state bytea[] := COALESCE(candidate_state, ARRAY[]::bytea[]);
            accepted_count bigint;
            frontier_index integer := 2;
            node bytea := candidate_leaf;
        BEGIN
            IF cardinality(state)=0 THEN
                state := ARRAY[convert_to('0', 'UTF8')];
            END IF;
            accepted_count := convert_from(state[1], 'UTF8')::bigint;
            IF candidate_ordinal IS DISTINCT FROM accepted_count + 1
               OR octet_length(candidate_leaf) IS DISTINCT FROM 32 THEN
                RAISE EXCEPTION 'public_evidence_nppes_merkle_input_invalid'
                    USING ERRCODE='23514';
            END IF;
            WHILE (accepted_count & 1)=1 LOOP
                IF cardinality(state) < frontier_index
                   OR state[frontier_index] IS NULL THEN
                    RAISE EXCEPTION 'public_evidence_nppes_merkle_state_invalid'
                        USING ERRCODE='23514';
                END IF;
                node := {tree_node}(state[frontier_index], node);
                state[frontier_index] := NULL;
                accepted_count := accepted_count >> 1;
                frontier_index := frontier_index + 1;
            END LOOP;
            IF cardinality(state) < frontier_index THEN
                state := state || ARRAY[node];
            ELSE
                state[frontier_index] := node;
            END IF;
            state[1] := convert_to(candidate_ordinal::text, 'UTF8');
            RETURN state;
        END;
        $function$;
        """,
        f"""
        CREATE FUNCTION {merkle_final}(candidate_state bytea[])
        RETURNS bytea LANGUAGE plpgsql IMMUTABLE PARALLEL UNSAFE
        SET search_path=pg_catalog AS $function$
        DECLARE frontier_index integer; root bytea;
        BEGIN
            IF candidate_state IS NULL OR cardinality(candidate_state) < 2
               OR convert_from(candidate_state[1], 'UTF8')::bigint < 1 THEN
                RETURN NULL;
            END IF;
            FOR frontier_index IN 2..cardinality(candidate_state) LOOP
                IF candidate_state[frontier_index] IS NOT NULL THEN
                    root := CASE WHEN root IS NULL
                        THEN candidate_state[frontier_index]
                        ELSE {tree_node}(candidate_state[frontier_index], root)
                    END;
                END IF;
            END LOOP;
            RETURN root;
        END;
        $function$;
        """,
        f"""
        CREATE AGGREGATE {merkle}(bigint, bytea) (
            SFUNC={merkle_sfunc}, STYPE=bytea[], FINALFUNC={merkle_final},
            INITCOND='{{}}', PARALLEL=UNSAFE
        );
        """,
        f"""
        CREATE FUNCTION {member_valid}(
            candidate_contract text, candidate_admission_ref text,
            candidate_source_release_ref text,
            candidate_source_release_contract_sha256 bytea,
            candidate_source_kind text, candidate_source_row_ordinal bigint,
            candidate_npi text, candidate_entity_type_code text,
            candidate_provider_enumeration_date date,
            candidate_last_update_date date,
            candidate_npi_deactivation_date date,
            candidate_npi_reactivation_date date,
            candidate_source_record_ref text, candidate_record_kind text,
            candidate_identity_contract_id text,
            candidate_record_hmac_sha256 bytea, candidate_payload_sha256 bytea,
            candidate_leaf_sha256 bytea, candidate_projection_state text,
            candidate_exclusion_reason text, candidate_evidence_ref text,
            candidate_row_sha256 bytea
        ) RETURNS boolean LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE
        SET search_path=pg_catalog AS $function$
        DECLARE leaf_json text; member_json text;
        BEGIN
            leaf_json := '{{"identity_contract_id":' || to_json(candidate_identity_contract_id)::text ||
                ',"payload_sha256":' || to_json(encode(candidate_payload_sha256, 'hex'))::text ||
                ',"record_hmac_sha256":' || to_json(encode(candidate_record_hmac_sha256, 'hex'))::text ||
                ',"record_kind":' || to_json(candidate_record_kind)::text ||
                ',"source_row_ordinal":' || candidate_source_row_ordinal::text || '}}';
            member_json := '{{"admission_ref":' || to_json(candidate_admission_ref)::text ||
                ',"contract":' || to_json(candidate_contract)::text ||
                ',"entity_type_code":' || COALESCE(to_json(candidate_entity_type_code)::text, 'null') ||
                ',"evidence_ref":' || COALESCE(to_json(candidate_evidence_ref)::text, 'null') ||
                ',"exclusion_reason":' || COALESCE(to_json(candidate_exclusion_reason)::text, 'null') ||
                ',"identity_contract_id":' || to_json(candidate_identity_contract_id)::text ||
                ',"last_update_date":' || COALESCE(to_json(to_char(candidate_last_update_date, 'YYYY-MM-DD'))::text, 'null') ||
                ',"leaf_sha256":' || to_json(encode(candidate_leaf_sha256, 'hex'))::text ||
                ',"npi":' || to_json(candidate_npi)::text ||
                ',"npi_deactivation_date":' || COALESCE(to_json(to_char(candidate_npi_deactivation_date, 'YYYY-MM-DD'))::text, 'null') ||
                ',"npi_reactivation_date":' || COALESCE(to_json(to_char(candidate_npi_reactivation_date, 'YYYY-MM-DD'))::text, 'null') ||
                ',"payload_sha256":' || to_json(encode(candidate_payload_sha256, 'hex'))::text ||
                ',"projection_state":' || to_json(candidate_projection_state)::text ||
                ',"provider_enumeration_date":' || COALESCE(to_json(to_char(candidate_provider_enumeration_date, 'YYYY-MM-DD'))::text, 'null') ||
                ',"record_hmac_sha256":' || to_json(encode(candidate_record_hmac_sha256, 'hex'))::text ||
                ',"record_kind":' || to_json(candidate_record_kind)::text ||
                ',"source_kind":' || to_json(candidate_source_kind)::text ||
                ',"source_record_ref":' || to_json(candidate_source_record_ref)::text ||
                ',"source_release_contract_sha256":' || to_json(encode(candidate_source_release_contract_sha256, 'hex'))::text ||
                ',"source_release_ref":' || to_json(candidate_source_release_ref)::text ||
                ',"source_row_ordinal":' || candidate_source_row_ordinal::text || '}}';
            RETURN candidate_payload_sha256 IS NOT DISTINCT FROM
                    {payload_digest}(
                        candidate_npi, candidate_entity_type_code,
                        candidate_provider_enumeration_date,
                        candidate_last_update_date,
                        candidate_npi_deactivation_date,
                        candidate_npi_reactivation_date)
               AND candidate_leaf_sha256 IS NOT DISTINCT FROM
                    {framed}('leaf', leaf_json)
               AND candidate_row_sha256 IS NOT DISTINCT FROM
                    {record_digest}('nppes_registry_member_row', member_json);
        END;
        $function$;
        """,
    )
    for statement in statements:
        op.execute(statement)


def _create_admission_table(schema: str) -> None:
    admission = _qt(schema, _ADMISSION)
    release = _qt(schema, _RELEASE)
    op.execute(
        f"""
        CREATE TABLE {admission} (
            admission_ref varchar(50) NOT NULL,
            contract varchar(64) NOT NULL,
            contract_sha256 bytea NOT NULL,
            source_release_ref varchar(50) NOT NULL,
            source_release_contract_sha256 bytea NOT NULL,
            source_kind varchar(48) NOT NULL,
            source_url text NOT NULL,
            archive_name text NOT NULL,
            primary_member_name text NOT NULL,
            artifact_sha256 bytea NOT NULL,
            artifact_byte_count bigint NOT NULL,
            zip_member_count integer NOT NULL,
            zip_member_census_sha256 bytea NOT NULL,
            header_sha256 bytea NOT NULL,
            payload_contract_id varchar(96) NOT NULL,
            record_identity_contract_id varchar(96) NOT NULL,
            tree_contract_id varchar(96) NOT NULL,
            manifest_contract varchar(64) NOT NULL,
            manifest_sha256 bytea NOT NULL,
            source_record_count bigint NOT NULL,
            projected_record_count bigint NOT NULL,
            excluded_record_count bigint NOT NULL,
            effective_start_not_disclosed_count bigint NOT NULL,
            entity_type_not_disclosed_count bigint NOT NULL,
            evidence_root_sha256 bytea NOT NULL,
            minimum_effective_start_at timestamptz NOT NULL,
            snapshot_at timestamptz NOT NULL,
            rights_proof_sha256 bytea NOT NULL,
            admission_state varchar(32) NOT NULL,
            serving_authority varchar(16) NOT NULL,
            publication_enabled boolean NOT NULL,
            created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
            CONSTRAINT public_evidence_nppes_registry_admission_pkey
                PRIMARY KEY (admission_ref),
            CONSTRAINT public_evidence_nppes_registry_admission_release_key
                UNIQUE (source_release_ref),
            CONSTRAINT public_evidence_nppes_registry_admission_owner_key
                UNIQUE (admission_ref, source_release_ref,
                        source_release_contract_sha256, source_kind),
            CONSTRAINT public_evidence_nppes_registry_admission_chain_owner_key
                UNIQUE (admission_ref, source_release_ref, archive_name,
                        snapshot_at, artifact_sha256, manifest_sha256,
                        source_record_count, projected_record_count,
                        excluded_record_count),
            CONSTRAINT public_evidence_nppes_registry_admission_release_fkey
                FOREIGN KEY (source_release_ref,
                    source_release_contract_sha256, source_kind)
                REFERENCES {release} (
                    source_release_ref, contract_sha256, source_kind)
                ON DELETE RESTRICT,
            CONSTRAINT public_evidence_nppes_registry_admission_shape_check
                CHECK ((
                    admission_ref ~ '^penpa1_[A-Za-z0-9_-]{{43}}$'
                    AND contract = 'healthporta.nppes-registry-admission.v1'
                    AND source_kind = 'nppes_entity_address'
                    AND source_url =
                        'https://download.cms.gov/nppes/' || archive_name
                    AND {_qf(schema, 'public_evidence_nppes_archive_period')}(
                            archive_name) IS NOT NULL
                    AND {_qf(schema, 'public_evidence_nppes_primary_period')}(
                            primary_member_name) IS NOT NULL
                    AND (snapshot_at AT TIME ZONE 'UTC')::date =
                        ({_qf(schema, 'public_evidence_nppes_primary_period')}(
                            primary_member_name))[2]
                    AND (
                        (({_qf(schema, 'public_evidence_nppes_archive_period')}(
                                archive_name))[2] IS NULL
                         AND extract(year FROM
                                (snapshot_at AT TIME ZONE 'UTC')) =
                             extract(year FROM
                                ({_qf(schema, 'public_evidence_nppes_archive_period')}(
                                    archive_name))[1])
                         AND extract(month FROM
                                (snapshot_at AT TIME ZONE 'UTC')) =
                             extract(month FROM
                                ({_qf(schema, 'public_evidence_nppes_archive_period')}(
                                    archive_name))[1]))
                        OR
                        (({_qf(schema, 'public_evidence_nppes_archive_period')}(
                                archive_name))[2] IS NOT NULL
                         AND (snapshot_at AT TIME ZONE 'UTC')::date =
                             ({_qf(schema, 'public_evidence_nppes_archive_period')}(
                                archive_name))[2])
                    )
                    AND payload_contract_id =
                        'healthporta_nppes_registry_csv_row_payload_sha256_v1'
                    AND record_identity_contract_id =
                        'healthporta_nppes_public_artifact_row_hmac_sha256_v1'
                    AND tree_contract_id =
                        'healthporta_nppes_registry_source_order_rfc6962_shape_sha256_v1'
                    AND manifest_contract =
                        'healthporta.nppes-registry-manifest.v1'
                    AND artifact_byte_count BETWEEN 1 AND 9223372036854775807
                    AND zip_member_count BETWEEN 1 AND 4096
                    AND source_record_count BETWEEN 1 AND 9007199254740991
                    AND projected_record_count BETWEEN 0 AND source_record_count
                    AND excluded_record_count =
                        source_record_count - projected_record_count
                    AND effective_start_not_disclosed_count >= 0
                    AND entity_type_not_disclosed_count >= 0
                    AND excluded_record_count =
                        effective_start_not_disclosed_count
                        + entity_type_not_disclosed_count
                    AND admission_state = 'verified_complete_disabled'
                    AND serving_authority = 'none'
                    AND NOT publication_enabled
                    AND octet_length(contract_sha256) = 32
                    AND octet_length(source_release_contract_sha256) = 32
                    AND octet_length(artifact_sha256) = 32
                    AND octet_length(zip_member_census_sha256) = 32
                    AND octet_length(header_sha256) = 32
                    AND octet_length(manifest_sha256) = 32
                    AND octet_length(evidence_root_sha256) = 32
                    AND octet_length(rights_proof_sha256) = 32
                    AND minimum_effective_start_at <= snapshot_at
                    AND date_trunc('second', minimum_effective_start_at) =
                        minimum_effective_start_at
                    AND date_trunc('day', snapshot_at AT TIME ZONE 'UTC') =
                        snapshot_at AT TIME ZONE 'UTC'
                ) IS TRUE)
        );
        """
    )


def _create_admission_seal_table(schema: str) -> None:
    admission = _qt(schema, _ADMISSION)
    op.execute(
        f"""
        CREATE TABLE {_qt(schema, _ADMISSION_SEAL)} (
            admission_ref varchar(50) NOT NULL,
            sealed_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
            CONSTRAINT public_evidence_nppes_registry_admission_seal_pkey
                PRIMARY KEY (admission_ref),
            CONSTRAINT public_evidence_nppes_registry_admission_seal_parent_fkey
                FOREIGN KEY (admission_ref)
                REFERENCES {admission} (admission_ref) ON DELETE RESTRICT,
            CONSTRAINT public_evidence_nppes_registry_admission_seal_shape_check
                CHECK ((admission_ref ~ '^penpa1_[A-Za-z0-9_-]{{43}}$') IS TRUE)
        );
        """
    )


def _alter_storage_tables(schema: str) -> None:
    source = _qt(schema, _SOURCE)
    common = _qt(schema, _COMMON)
    link = _qt(schema, _LINK)
    typed = _qt(schema, _TYPED)
    admission = _qt(schema, _ADMISSION)
    for table_name in _OLD_RECORD_TABLES:
        op.execute(
            f"DROP TRIGGER {_q(table_name + '_integrity_guard')} "
            f"ON {_qt(schema, table_name)};"
        )
    op.execute(f"ALTER TABLE {source} ADD COLUMN nppes_admission_ref varchar(50);")
    for table in (common, link, typed):
        op.execute(
            f"ALTER TABLE {table} ADD COLUMN nppes_admission_ref varchar(50) NOT NULL;"
        )
    storage_statements = (
        f"""
        ALTER TABLE {source}
          ADD CONSTRAINT public_evidence_source_record_nppes_shape_check
            CHECK (((source_kind = 'nppes_entity_address'
                     AND nppes_admission_ref IS NOT NULL)
                    OR (source_kind <> 'nppes_entity_address'
                        AND nppes_admission_ref IS NULL)) IS TRUE),
          ADD CONSTRAINT public_evidence_source_record_admission_owner_key
            UNIQUE (source_record_ref, source_release_ref,
                    source_release_contract_sha256, source_kind,
                    nppes_admission_ref),
          ADD CONSTRAINT public_evidence_source_record_admission_fkey
            FOREIGN KEY (nppes_admission_ref, source_release_ref,
                         source_release_contract_sha256, source_kind)
            REFERENCES {admission} (admission_ref, source_release_ref,
                source_release_contract_sha256, source_kind)
            ON DELETE RESTRICT;
        """,
        f"""
        ALTER TABLE {common}
          ADD CONSTRAINT public_evidence_record_admission_owner_key
            UNIQUE (evidence_ref, source_release_ref,
                    source_release_contract_sha256, source_kind,
                    nppes_admission_ref),
          ADD CONSTRAINT public_evidence_record_admission_fkey
            FOREIGN KEY (nppes_admission_ref, source_release_ref,
                         source_release_contract_sha256, source_kind)
            REFERENCES {admission} (admission_ref, source_release_ref,
                source_release_contract_sha256, source_kind)
            ON DELETE RESTRICT;
        """,
        f"""
        ALTER TABLE {link}
          DROP CONSTRAINT public_evidence_record_source_link_record_fkey,
          DROP CONSTRAINT public_evidence_record_source_link_source_fkey,
          ADD CONSTRAINT public_evidence_record_source_link_evidence_key
            UNIQUE (evidence_ref),
          ADD CONSTRAINT public_evidence_record_source_link_record_fkey
            FOREIGN KEY (evidence_ref, source_release_ref,
                         source_release_contract_sha256, source_kind,
                         nppes_admission_ref)
            REFERENCES {common} (evidence_ref, source_release_ref,
                source_release_contract_sha256, source_kind,
                nppes_admission_ref) ON DELETE RESTRICT,
          ADD CONSTRAINT public_evidence_record_source_link_source_fkey
            FOREIGN KEY (source_record_ref, source_release_ref,
                         source_release_contract_sha256, source_kind,
                         nppes_admission_ref)
            REFERENCES {source} (source_record_ref, source_release_ref,
                source_release_contract_sha256, source_kind,
                nppes_admission_ref) ON DELETE RESTRICT;
        """,
        f"""
        ALTER TABLE {typed}
          DROP CONSTRAINT public_evidence_npi_enumeration_record_fkey,
          ADD CONSTRAINT public_evidence_npi_enumeration_record_fkey
            FOREIGN KEY (evidence_ref, source_release_ref,
                         source_release_contract_sha256, source_kind,
                         nppes_admission_ref)
            REFERENCES {common} (evidence_ref, source_release_ref,
                source_release_contract_sha256, source_kind,
                nppes_admission_ref) ON DELETE RESTRICT;
        """,
    )
    for statement in storage_statements:
        op.execute(statement)
    op.execute(
        f"CREATE INDEX public_evidence_source_record_admission_idx ON {source} "
        "(nppes_admission_ref, source_record_ref) "
        "WHERE nppes_admission_ref IS NOT NULL;"
    )
    op.execute(
        f"CREATE INDEX public_evidence_record_admission_idx ON {common} "
        "(nppes_admission_ref, evidence_ref);"
    )
    op.execute(
        f"CREATE INDEX public_evidence_record_source_link_admission_idx ON {link} "
        "(nppes_admission_ref, evidence_ref, source_record_ordinal);"
    )
    op.execute(
        f"CREATE INDEX public_evidence_npi_enumeration_admission_idx ON {typed} "
        "(nppes_admission_ref, evidence_ref);"
    )
    op.execute(
        f"CREATE INDEX public_evidence_npi_enumeration_lookup_idx ON {typed} "
        "(npi, source_release_ref, evidence_ref);"
    )


def _create_member_table(schema: str) -> None:
    member = _qt(schema, _MEMBER)
    admission = _qt(schema, _ADMISSION)
    source = _qt(schema, _SOURCE)
    common = _qt(schema, _COMMON)
    member_valid = _qf(schema, "public_evidence_nppes_member_valid")
    op.execute(
        f"""
        CREATE TABLE {member} (
            contract varchar(64) NOT NULL,
            admission_ref varchar(50) NOT NULL,
            source_release_ref varchar(50) NOT NULL,
            source_release_contract_sha256 bytea NOT NULL,
            source_kind varchar(48) NOT NULL,
            source_row_ordinal bigint NOT NULL,
            npi varchar(10) NOT NULL,
            entity_type_code varchar(1),
            provider_enumeration_date date,
            last_update_date date,
            npi_deactivation_date date,
            npi_reactivation_date date,
            source_record_ref varchar(49) NOT NULL,
            record_kind varchar(64) NOT NULL,
            identity_contract_id varchar(96) NOT NULL,
            record_hmac_sha256 bytea NOT NULL,
            payload_sha256 bytea NOT NULL,
            leaf_sha256 bytea NOT NULL,
            projection_state varchar(16) NOT NULL,
            exclusion_reason varchar(64),
            evidence_ref varchar(49),
            row_sha256 bytea NOT NULL,
            created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
            CONSTRAINT public_evidence_nppes_registry_member_pkey
                PRIMARY KEY (admission_ref, source_row_ordinal),
            CONSTRAINT public_evidence_nppes_registry_member_npi_key
                UNIQUE (source_release_ref, npi),
            CONSTRAINT public_evidence_nppes_registry_member_source_key
                UNIQUE (source_release_ref, source_record_ref),
            CONSTRAINT public_evidence_nppes_registry_member_evidence_key
                UNIQUE (source_release_ref, evidence_ref),
            CONSTRAINT public_evidence_nppes_registry_member_admission_fkey
                FOREIGN KEY (admission_ref, source_release_ref,
                    source_release_contract_sha256, source_kind)
                REFERENCES {admission} (admission_ref, source_release_ref,
                    source_release_contract_sha256, source_kind)
                ON DELETE RESTRICT,
            CONSTRAINT public_evidence_nppes_registry_member_source_fkey
                FOREIGN KEY (source_record_ref, source_release_ref,
                    source_release_contract_sha256, source_kind, admission_ref)
                REFERENCES {source} (source_record_ref, source_release_ref,
                    source_release_contract_sha256, source_kind,
                    nppes_admission_ref) ON DELETE RESTRICT,
            CONSTRAINT public_evidence_nppes_registry_member_evidence_fkey
                FOREIGN KEY (evidence_ref, source_release_ref,
                    source_release_contract_sha256, source_kind, admission_ref)
                REFERENCES {common} (evidence_ref, source_release_ref,
                    source_release_contract_sha256, source_kind,
                    nppes_admission_ref) ON DELETE RESTRICT,
            CONSTRAINT public_evidence_nppes_registry_member_shape_check
                CHECK ((
                    contract = 'healthporta.nppes-registry-member.v1'
                    AND source_kind = 'nppes_entity_address'
                    AND source_row_ordinal BETWEEN 1 AND 9007199254740991
                    AND record_kind = 'nppes_registry_record'
                    AND identity_contract_id =
                        'healthporta_nppes_public_artifact_row_hmac_sha256_v1'
                    AND {_qf(schema, 'public_evidence_npi_valid')}(npi)
                    AND (entity_type_code IS NULL
                         OR entity_type_code IN ('1', '2'))
                    AND (provider_enumeration_date IS NULL
                         OR provider_enumeration_date >= DATE '0001-01-01')
                    AND (last_update_date IS NULL
                         OR last_update_date >= DATE '0001-01-01')
                    AND (npi_deactivation_date IS NULL
                         OR npi_deactivation_date >= DATE '0001-01-01')
                    AND (npi_reactivation_date IS NULL
                         OR npi_reactivation_date >= DATE '0001-01-01')
                    AND projection_state IN ('projected_v1', 'excluded_v1')
                    AND ((projection_state = 'projected_v1'
                          AND exclusion_reason IS NULL
                          AND evidence_ref IS NOT NULL
                          AND entity_type_code IS NOT NULL)
                         OR (projection_state = 'excluded_v1'
                             AND exclusion_reason IN (
                                 'effective_start_not_disclosed',
                                 'entity_type_not_disclosed')
                             AND evidence_ref IS NULL))
                    AND octet_length(source_release_contract_sha256) = 32
                    AND octet_length(record_hmac_sha256) = 32
                    AND octet_length(payload_sha256) = 32
                    AND octet_length(leaf_sha256) = 32
                    AND octet_length(row_sha256) = 32
                ) IS TRUE),
            CONSTRAINT public_evidence_nppes_registry_member_digest_check
                CHECK ({member_valid}(
                    contract, admission_ref, source_release_ref,
                    source_release_contract_sha256, source_kind,
                    source_row_ordinal, npi, entity_type_code,
                    provider_enumeration_date, last_update_date,
                    npi_deactivation_date, npi_reactivation_date,
                    source_record_ref, record_kind, identity_contract_id,
                    record_hmac_sha256, payload_sha256, leaf_sha256,
                    projection_state, exclusion_reason, evidence_ref,
                    row_sha256
                ) IS TRUE)
        );
        """
    )


def _create_chain_tables(schema: str) -> None:
    chain = _qt(schema, _CHAIN)
    child = _qt(schema, _CHAIN_ARCHIVE)
    admission = _qt(schema, _ADMISSION)
    chain_statements = (
        f"""
        CREATE TABLE {chain} (
            chain_ref varchar(50) NOT NULL,
            contract varchar(64) NOT NULL,
            contract_sha256 bytea NOT NULL,
            listing_sha256 bytea NOT NULL,
            listing_byte_count bigint NOT NULL,
            listing_candidate_names text[] NOT NULL,
            archive_count integer NOT NULL,
            source_record_count bigint NOT NULL,
            projected_record_count bigint NOT NULL,
            excluded_record_count bigint NOT NULL,
            admission_state varchar(32) NOT NULL,
            serving_authority varchar(16) NOT NULL,
            publication_enabled boolean NOT NULL,
            row_sha256 bytea NOT NULL,
            created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
            CONSTRAINT public_evidence_nppes_registry_chain_admission_pkey
                PRIMARY KEY (chain_ref),
            CONSTRAINT public_evidence_nppes_registry_chain_admission_owner_key
                UNIQUE (chain_ref, archive_count),
            CONSTRAINT public_evidence_nppes_registry_chain_listing_key
                UNIQUE (listing_sha256),
            CONSTRAINT public_evidence_nppes_registry_chain_shape_check
                CHECK ((
                    chain_ref ~ '^penpc1_[A-Za-z0-9_-]{{43}}$'
                    AND contract =
                        'healthporta.nppes-public-evidence-import-chain.v1'
                    AND listing_byte_count BETWEEN 1 AND 4194304
                    AND archive_count BETWEEN 1 AND 4096
                    AND array_ndims(listing_candidate_names) = 1
                    AND array_lower(listing_candidate_names, 1) = 1
                    AND cardinality(listing_candidate_names) >= archive_count
                    AND source_record_count BETWEEN 1 AND 9007199254740991
                    AND projected_record_count BETWEEN 0 AND source_record_count
                    AND excluded_record_count =
                        source_record_count - projected_record_count
                    AND admission_state = 'verified_complete_disabled'
                    AND serving_authority = 'none'
                    AND NOT publication_enabled
                    AND octet_length(contract_sha256) = 32
                    AND octet_length(listing_sha256) = 32
                    AND octet_length(row_sha256) = 32
                ) IS TRUE)
        );
        """,
        f"""
        CREATE TABLE {_qt(schema, _CHAIN_SEAL)} (
            chain_ref varchar(50) NOT NULL,
            sealed_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
            CONSTRAINT public_evidence_nppes_registry_chain_admission_seal_pkey
                PRIMARY KEY (chain_ref),
            CONSTRAINT public_evidence_nppes_registry_chain_admission_seal_parent_fkey
                FOREIGN KEY (chain_ref)
                REFERENCES {chain} (chain_ref) ON DELETE RESTRICT,
            CONSTRAINT public_evidence_nppes_registry_chain_admission_seal_shape_check
                CHECK ((chain_ref ~ '^penpc1_[A-Za-z0-9_-]{{43}}$') IS TRUE)
        );
        """,
        f"""
        CREATE TABLE {child} (
            chain_ref varchar(50) NOT NULL,
            archive_ordinal integer NOT NULL,
            archive_count integer NOT NULL,
            archive_name text NOT NULL,
            snapshot_at timestamptz NOT NULL,
            admission_ref varchar(50) NOT NULL,
            source_release_ref varchar(50) NOT NULL,
            artifact_sha256 bytea NOT NULL,
            manifest_sha256 bytea NOT NULL,
            source_record_count bigint NOT NULL,
            projected_record_count bigint NOT NULL,
            excluded_record_count bigint NOT NULL,
            row_sha256 bytea NOT NULL,
            created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
            CONSTRAINT public_evidence_nppes_registry_chain_archive_pkey
                PRIMARY KEY (chain_ref, archive_ordinal),
            CONSTRAINT public_evidence_nppes_registry_chain_archive_admission_key
                UNIQUE (chain_ref, admission_ref),
            CONSTRAINT public_evidence_nppes_registry_chain_archive_release_key
                UNIQUE (chain_ref, source_release_ref),
            CONSTRAINT public_evidence_nppes_registry_chain_archive_name_key
                UNIQUE (chain_ref, archive_name),
            CONSTRAINT public_evidence_nppes_registry_chain_archive_artifact_key
                UNIQUE (chain_ref, artifact_sha256),
            CONSTRAINT public_evidence_nppes_registry_chain_archive_manifest_key
                UNIQUE (chain_ref, manifest_sha256),
            CONSTRAINT public_evidence_nppes_registry_chain_archive_parent_fkey
                FOREIGN KEY (chain_ref, archive_count)
                REFERENCES {chain} (chain_ref, archive_count)
                ON DELETE RESTRICT,
            CONSTRAINT public_evidence_nppes_registry_chain_archive_admission_fkey
                FOREIGN KEY (admission_ref, source_release_ref, archive_name,
                    snapshot_at, artifact_sha256, manifest_sha256,
                    source_record_count, projected_record_count,
                    excluded_record_count)
                REFERENCES {admission} (admission_ref, source_release_ref,
                    archive_name, snapshot_at, artifact_sha256,
                    manifest_sha256, source_record_count,
                    projected_record_count, excluded_record_count)
                ON DELETE RESTRICT,
            CONSTRAINT public_evidence_nppes_registry_chain_archive_shape_check
                CHECK ((
                    archive_ordinal BETWEEN 0 AND archive_count - 1
                    AND archive_count BETWEEN 1 AND 4096
                    AND archive_name ~
                        '^NPPES_Data_Dissemination_[A-Za-z0-9_]+_V2[.]zip$'
                    AND date_trunc('day', snapshot_at AT TIME ZONE 'UTC') =
                        snapshot_at AT TIME ZONE 'UTC'
                    AND source_record_count BETWEEN 1 AND 9007199254740991
                    AND projected_record_count BETWEEN 0 AND source_record_count
                    AND excluded_record_count =
                        source_record_count - projected_record_count
                    AND octet_length(artifact_sha256) = 32
                    AND octet_length(manifest_sha256) = 32
                    AND octet_length(row_sha256) = 32
                ) IS TRUE)
        );
        """,
    )
    for statement in chain_statements:
        op.execute(statement)


def _create_admission_validator(schema: str) -> None:
    function = _qf(schema, "validate_public_evidence_nppes_registry_admission")
    admission = _qt(schema, _ADMISSION)
    seal = _qt(schema, _ADMISSION_SEAL)
    member = _qt(schema, _MEMBER)
    release = _qt(schema, _RELEASE)
    source = _qt(schema, _SOURCE)
    common = _qt(schema, _COMMON)
    link = _qt(schema, _LINK)
    typed = _qt(schema, _TYPED)
    digest = _qf(schema, "public_evidence_record_digest")
    reference = _qf(schema, "public_evidence_record_ref")
    framed = _qf(schema, "public_evidence_nppes_framed_digest")
    merkle = _qf(schema, "public_evidence_nppes_merkle_root")
    op.execute(
        f"""
        CREATE FUNCTION {function}() RETURNS trigger LANGUAGE plpgsql
        SECURITY DEFINER SET search_path=pg_catalog AS $function$
        DECLARE
            admitted RECORD; released RECORD;
            admission_json text; exclusion_json text; manifest_json text;
            member_count bigint; source_count bigint; common_count bigint;
            link_count bigint; typed_count bigint; projected_count bigint;
            effective_start_missing_count bigint;
            entity_type_missing_count bigint; minimum_effective_date date;
            first_ordinal bigint; last_ordinal bigint; evidence_root bytea;
            invalid boolean;
        BEGIN
            PERFORM pg_advisory_xact_lock(hashtextextended(
                'healthporta.public-evidence-nppes-admission:' || NEW.admission_ref,
                0));
            SELECT * INTO admitted FROM {admission}
             WHERE admission_ref=NEW.admission_ref;
            IF admitted IS NULL THEN
                RAISE EXCEPTION 'public_evidence_nppes_admission_invalid'
                    USING ERRCODE='23514';
            END IF;
            SELECT * INTO released FROM {release}
             WHERE source_release_ref=admitted.source_release_ref
               AND contract_sha256=admitted.source_release_contract_sha256
               AND source_kind=admitted.source_kind;
            IF released IS NULL THEN
                RAISE EXCEPTION 'public_evidence_nppes_admission_invalid'
                    USING ERRCODE='23514';
            END IF;

            admission_json := '{{"admission_state":' || to_json(admitted.admission_state)::text ||
                ',"archive_name":' || to_json(admitted.archive_name)::text ||
                ',"artifact_byte_count":' || admitted.artifact_byte_count::text ||
                ',"artifact_sha256":' || to_json(encode(admitted.artifact_sha256, 'hex'))::text ||
                ',"contract":' || to_json(admitted.contract)::text ||
                ',"effective_start_not_disclosed_count":' || admitted.effective_start_not_disclosed_count::text ||
                ',"entity_type_not_disclosed_count":' || admitted.entity_type_not_disclosed_count::text ||
                ',"evidence_root_sha256":' || to_json(encode(admitted.evidence_root_sha256, 'hex'))::text ||
                ',"excluded_record_count":' || admitted.excluded_record_count::text ||
                ',"header_sha256":' || to_json(encode(admitted.header_sha256, 'hex'))::text ||
                ',"manifest_contract":' || to_json(admitted.manifest_contract)::text ||
                ',"manifest_sha256":' || to_json(encode(admitted.manifest_sha256, 'hex'))::text ||
                ',"minimum_effective_start_at":' || to_json(to_char(admitted.minimum_effective_start_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'))::text ||
                ',"payload_contract_id":' || to_json(admitted.payload_contract_id)::text ||
                ',"primary_member_name":' || to_json(admitted.primary_member_name)::text ||
                ',"projected_record_count":' || admitted.projected_record_count::text ||
                ',"publication_enabled":' || admitted.publication_enabled::text ||
                ',"record_identity_contract_id":' || to_json(admitted.record_identity_contract_id)::text ||
                ',"rights_proof_sha256":' || to_json(encode(admitted.rights_proof_sha256, 'hex'))::text ||
                ',"serving_authority":' || to_json(admitted.serving_authority)::text ||
                ',"snapshot_at":' || to_json(to_char(admitted.snapshot_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'))::text ||
                ',"source_kind":' || to_json(admitted.source_kind)::text ||
                ',"source_record_count":' || admitted.source_record_count::text ||
                ',"source_release_contract_sha256":' || to_json(encode(admitted.source_release_contract_sha256, 'hex'))::text ||
                ',"source_release_ref":' || to_json(admitted.source_release_ref)::text ||
                ',"source_url":' || to_json(admitted.source_url)::text ||
                ',"tree_contract_id":' || to_json(admitted.tree_contract_id)::text ||
                ',"zip_member_census_sha256":' || to_json(encode(admitted.zip_member_census_sha256, 'hex'))::text ||
                ',"zip_member_count":' || admitted.zip_member_count::text || '}}';

            exclusion_json := '[' || concat_ws(',',
                CASE WHEN admitted.effective_start_not_disclosed_count > 0
                    THEN '{{"reason":"effective_start_not_disclosed","record_count":' ||
                        admitted.effective_start_not_disclosed_count::text || '}}' END,
                CASE WHEN admitted.entity_type_not_disclosed_count > 0
                    THEN '{{"reason":"entity_type_not_disclosed","record_count":' ||
                        admitted.entity_type_not_disclosed_count::text || '}}' END
            ) || ']';
            manifest_json := '{{"archive_name":' || to_json(admitted.archive_name)::text ||
                ',"artifact_byte_count":' || admitted.artifact_byte_count::text ||
                ',"artifact_sha256":' || to_json(encode(admitted.artifact_sha256, 'hex'))::text ||
                ',"contract":' || to_json(admitted.manifest_contract)::text ||
                ',"evidence_root_sha256":' || to_json(encode(admitted.evidence_root_sha256, 'hex'))::text ||
                ',"excluded_record_count":' || admitted.excluded_record_count::text ||
                ',"exclusion_counts":' || exclusion_json ||
                ',"header_sha256":' || to_json(encode(admitted.header_sha256, 'hex'))::text ||
                ',"minimum_effective_start_at":' || to_json(to_char(admitted.minimum_effective_start_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'))::text ||
                ',"primary_member_name":' || to_json(admitted.primary_member_name)::text ||
                ',"projected_record_count":' || admitted.projected_record_count::text ||
                ',"record_identity_contract_id":' || to_json(admitted.record_identity_contract_id)::text ||
                ',"rights_proof_sha256":' || to_json(encode(admitted.rights_proof_sha256, 'hex'))::text ||
                ',"snapshot_at":' || to_json(to_char(admitted.snapshot_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'))::text ||
                ',"source_record_count":' || admitted.source_record_count::text ||
                ',"source_release_contract_sha256":' || to_json(encode(admitted.source_release_contract_sha256, 'hex'))::text ||
                ',"source_release_ref":' || to_json(admitted.source_release_ref)::text ||
                ',"source_url":' || to_json(admitted.source_url)::text || '}}';

            SELECT count(*), min(source_row_ordinal), max(source_row_ordinal),
                   count(*) FILTER (WHERE projection_state='projected_v1'),
                   count(*) FILTER (
                       WHERE exclusion_reason='effective_start_not_disclosed'),
                   count(*) FILTER (
                       WHERE exclusion_reason='entity_type_not_disclosed'),
                   min(CASE
                       WHEN npi_deactivation_date IS NOT NULL
                        AND npi_reactivation_date IS NULL
                       THEN npi_deactivation_date
                       ELSE COALESCE(
                           npi_reactivation_date,
                           provider_enumeration_date)
                   END),
                   {merkle}(source_row_ordinal, leaf_sha256 ORDER BY source_row_ordinal)
              INTO member_count, first_ordinal, last_ordinal,
                   projected_count, effective_start_missing_count,
                   entity_type_missing_count, minimum_effective_date,
                   evidence_root
              FROM {member} WHERE admission_ref=admitted.admission_ref;
            SELECT count(*) INTO source_count FROM {source}
             WHERE nppes_admission_ref=admitted.admission_ref;
            SELECT count(*) INTO common_count FROM {common}
             WHERE nppes_admission_ref=admitted.admission_ref;
            SELECT count(*) INTO link_count FROM {link}
             WHERE nppes_admission_ref=admitted.admission_ref;
            SELECT count(*) INTO typed_count FROM {typed}
             WHERE nppes_admission_ref=admitted.admission_ref;

            invalid := admitted.contract_sha256 IS DISTINCT FROM
                    {digest}('nppes_registry_admission_contract', admission_json)
                OR admitted.admission_ref IS DISTINCT FROM
                    {reference}('penpa1_', 'nppes_registry_admission', admission_json)
                OR admitted.manifest_sha256 IS DISTINCT FROM
                    {framed}('manifest', manifest_json)
                OR ROW(released.artifact_content_sha256,
                       released.expected_record_count,
                       released.observed_record_count,
                       released.evidence_root_sha256,
                       released.rights_proof_sha256,
                       released.observed_start_at,
                       released.observed_end_at,
                       released.effective_start_at,
                       released.effective_end_at)
                   IS DISTINCT FROM ROW(admitted.artifact_sha256,
                       admitted.source_record_count,
                       admitted.source_record_count,
                       admitted.evidence_root_sha256,
                       admitted.rights_proof_sha256,
                       admitted.snapshot_at, admitted.snapshot_at,
                       admitted.minimum_effective_start_at,
                       admitted.snapshot_at)
                OR member_count IS DISTINCT FROM admitted.source_record_count
                OR source_count IS DISTINCT FROM admitted.source_record_count
                OR projected_count IS DISTINCT FROM admitted.projected_record_count
                OR effective_start_missing_count IS DISTINCT FROM
                    admitted.effective_start_not_disclosed_count
                OR entity_type_missing_count IS DISTINCT FROM
                    admitted.entity_type_not_disclosed_count
                OR (minimum_effective_date::timestamp AT TIME ZONE 'UTC')
                    IS DISTINCT FROM admitted.minimum_effective_start_at
                OR common_count IS DISTINCT FROM admitted.projected_record_count
                OR link_count IS DISTINCT FROM admitted.projected_record_count
                OR typed_count IS DISTINCT FROM admitted.projected_record_count
                OR first_ordinal IS DISTINCT FROM 1
                OR last_ordinal IS DISTINCT FROM admitted.source_record_count
                OR evidence_root IS DISTINCT FROM admitted.evidence_root_sha256;

            IF NOT invalid THEN
                SELECT EXISTS (
                    SELECT 1 FROM {member} AS member_row
                    JOIN {source} AS source_row
                      ON source_row.source_record_ref=member_row.source_record_ref
                     AND source_row.nppes_admission_ref=member_row.admission_ref
                    WHERE member_row.admission_ref=admitted.admission_ref
                      AND ROW(source_row.source_release_ref,
                              source_row.source_release_contract_sha256,
                              source_row.source_kind, source_row.record_kind,
                              source_row.identity_contract_id,
                              source_row.record_hmac_sha256,
                              source_row.payload_sha256)
                          IS DISTINCT FROM ROW(member_row.source_release_ref,
                              member_row.source_release_contract_sha256,
                              member_row.source_kind, member_row.record_kind,
                              member_row.identity_contract_id,
                              member_row.record_hmac_sha256,
                              member_row.payload_sha256)
                ) INTO invalid;
            END IF;
            IF NOT invalid THEN
                SELECT EXISTS (
                    SELECT 1 FROM {member} AS member_row
                    LEFT JOIN {common} AS common_row
                      ON common_row.evidence_ref=member_row.evidence_ref
                     AND common_row.nppes_admission_ref=member_row.admission_ref
                    LEFT JOIN {link} AS link_row
                      ON link_row.evidence_ref=member_row.evidence_ref
                     AND link_row.nppes_admission_ref=member_row.admission_ref
                    LEFT JOIN {typed} AS typed_row
                      ON typed_row.evidence_ref=member_row.evidence_ref
                     AND typed_row.nppes_admission_ref=member_row.admission_ref
                    WHERE member_row.admission_ref=admitted.admission_ref
                      AND (
                        (member_row.projection_state='projected_v1' AND (
                            common_row.evidence_ref IS NULL
                            OR link_row.evidence_ref IS NULL
                            OR typed_row.evidence_ref IS NULL
                            OR link_row.source_record_ref IS DISTINCT FROM
                                member_row.source_record_ref
                            OR link_row.source_record_ordinal IS DISTINCT FROM 0
                            OR typed_row.npi IS DISTINCT FROM member_row.npi
                            OR typed_row.npi_entity_type IS DISTINCT FROM CASE
                                member_row.entity_type_code
                                WHEN '1' THEN 'individual_type_1'
                                WHEN '2' THEN 'organization_type_2' END
                            OR typed_row.enumeration_state IS DISTINCT FROM CASE
                                WHEN member_row.npi_deactivation_date IS NOT NULL
                                 AND member_row.npi_reactivation_date IS NULL
                                THEN 'deactivated' ELSE 'active' END
                            OR common_row.observed_at IS DISTINCT FROM admitted.snapshot_at
                            OR common_row.effective_start_at IS DISTINCT FROM
                                (CASE WHEN member_row.npi_deactivation_date IS NOT NULL
                                           AND member_row.npi_reactivation_date IS NULL
                                    THEN member_row.npi_deactivation_date
                                    ELSE COALESCE(member_row.npi_reactivation_date,
                                                  member_row.provider_enumeration_date)
                                 END)::timestamp AT TIME ZONE 'UTC'
                            OR common_row.effective_end_at IS DISTINCT FROM admitted.snapshot_at
                        ))
                        OR (member_row.projection_state='excluded_v1'
                            AND (common_row.evidence_ref IS NOT NULL
                                 OR link_row.evidence_ref IS NOT NULL
                                 OR typed_row.evidence_ref IS NOT NULL))
                        OR member_row.provider_enumeration_date >
                            (admitted.snapshot_at AT TIME ZONE 'UTC')::date
                        OR member_row.last_update_date >
                            (admitted.snapshot_at AT TIME ZONE 'UTC')::date
                        OR member_row.npi_deactivation_date >
                            (admitted.snapshot_at AT TIME ZONE 'UTC')::date
                        OR member_row.npi_reactivation_date >
                            (admitted.snapshot_at AT TIME ZONE 'UTC')::date
                        OR (member_row.npi_reactivation_date IS NOT NULL AND
                            (member_row.npi_deactivation_date IS NULL OR
                             member_row.npi_reactivation_date <=
                                member_row.npi_deactivation_date))
                        OR (member_row.provider_enumeration_date IS NOT NULL
                            AND member_row.npi_deactivation_date IS NOT NULL
                            AND member_row.provider_enumeration_date >
                                member_row.npi_deactivation_date)
                        OR (member_row.provider_enumeration_date IS NOT NULL
                            AND member_row.npi_reactivation_date IS NOT NULL
                            AND member_row.provider_enumeration_date >
                                member_row.npi_reactivation_date)
                        OR member_row.exclusion_reason IS DISTINCT FROM CASE
                            WHEN (CASE WHEN member_row.npi_deactivation_date IS NOT NULL
                                           AND member_row.npi_reactivation_date IS NULL
                                      THEN member_row.npi_deactivation_date
                                      ELSE COALESCE(member_row.npi_reactivation_date,
                                                    member_row.provider_enumeration_date)
                                  END) IS NULL
                            THEN 'effective_start_not_disclosed'
                            WHEN member_row.entity_type_code IS NULL
                            THEN 'entity_type_not_disclosed'
                            ELSE NULL END
                      )
                ) INTO invalid;
            END IF;
            IF NOT invalid THEN
                SELECT EXISTS (
                    WITH base AS (
                        SELECT common_row AS common_value,
                               link_row AS link_value,
                               typed_row AS typed_value,
                               source_row AS source_value
                        FROM {member} AS member_row
                        JOIN {common} AS common_row
                          ON common_row.evidence_ref=member_row.evidence_ref
                         AND common_row.nppes_admission_ref=member_row.admission_ref
                        JOIN {link} AS link_row
                          ON link_row.evidence_ref=member_row.evidence_ref
                         AND link_row.nppes_admission_ref=member_row.admission_ref
                        JOIN {typed} AS typed_row
                          ON typed_row.evidence_ref=member_row.evidence_ref
                         AND typed_row.nppes_admission_ref=member_row.admission_ref
                        JOIN {source} AS source_row
                          ON source_row.source_record_ref=member_row.source_record_ref
                         AND source_row.nppes_admission_ref=member_row.admission_ref
                        WHERE member_row.admission_ref=admitted.admission_ref
                          AND member_row.projection_state='projected_v1'
                    ), pieces AS (
                        SELECT base.*,
                            replace(replace(
                              '{{"adapter_execution_authority":"none",' ||
                              '"address_selection_authority":"none",' ||
                              '"confidence_claimed":"F","current_pointer_authority":"none",' ||
                              '"database_io_authority":"none","deletion_enabled":"F",' ||
                              '"employment_claimed":"F","exact_rate_site_claimed":"F",' ||
                              '"executor_authority":"none","facility_ownership_claimed":"F",' ||
                              '"independence_claimed":"F","legal_ownership_claimed":"F",' ||
                              '"lifecycle_state":"normalized_record_only",' ||
                              '"payer_confirmed_site_claimed":"F","positive_evidence_only":"T",' ||
                              '"publication_enabled":"F","replacement_enabled":"F",' ||
                              '"retirement_enabled":"F","serving_authority":"none",' ||
                              '"site_match_claimed":"F","supersession_enabled":"F"}}',
                              '"F"', 'false'), '"T"', 'true') AS authority_json,
                            '[{{"row_sha256":' ||
                                to_json(encode((link_value).row_sha256, 'hex'))::text ||
                                ',"source_record_ordinal":' ||
                                (link_value).source_record_ordinal::text ||
                                ',"source_record_ref":' ||
                                to_json((link_value).source_record_ref)::text || '}}]' AS link_json,
                            '[{{"identity_contract_id":' ||
                                to_json((source_value).identity_contract_id)::text ||
                                ',"payload_sha256":' ||
                                to_json(encode((source_value).payload_sha256, 'hex'))::text ||
                                ',"record_hmac_sha256":' ||
                                to_json(encode((source_value).record_hmac_sha256, 'hex'))::text ||
                                ',"record_kind":' || to_json((source_value).record_kind)::text ||
                                ',"source_record_ref":' || to_json((source_value).source_record_ref)::text ||
                                ',"source_release_ref":' || to_json((source_value).source_release_ref)::text || '}}]' AS source_json,
                            '{{"enumeration_state":' || to_json((typed_value).enumeration_state)::text ||
                                ',"npi":' || to_json((typed_value).npi)::text ||
                                ',"npi_entity_type":' || to_json((typed_value).npi_entity_type)::text ||
                                ',"relationship_class":' || to_json((typed_value).relationship_class)::text || '}}' AS evidence_json,
                            '{{"end_at":' || CASE WHEN (common_value).effective_end_at IS NULL
                                THEN 'null' ELSE to_json(to_char((common_value).effective_end_at AT TIME ZONE 'UTC',
                                'YYYY-MM-DD"T"HH24:MI:SS"Z"'))::text END ||
                                ',"start_at":' || to_json(to_char((common_value).effective_start_at AT TIME ZONE 'UTC',
                                'YYYY-MM-DD"T"HH24:MI:SS"Z"'))::text || '}}' AS effective_json
                        FROM base
                    ), assembled AS (
                        SELECT pieces.*,
                            '{{"links":' || link_json || ',"ordering_contract_id":' ||
                                to_json((common_value).source_link_ordering_contract_id)::text ||
                                ',"source_record_count":' || (common_value).source_record_count::text || '}}' AS vector_json,
                            '{{"authority_state_sha256":' ||
                                to_json(encode((common_value).authority_state_sha256, 'hex'))::text ||
                                ',"current_pointer_authority":' || to_json((common_value).current_pointer_authority)::text ||
                                ',"database_io_authority":' || to_json((common_value).database_io_authority)::text ||
                                ',"effective_end_at":' || CASE WHEN (common_value).effective_end_at IS NULL THEN 'null'
                                    ELSE to_json(to_char((common_value).effective_end_at AT TIME ZONE 'UTC',
                                    'YYYY-MM-DD"T"HH24:MI:SS"Z"'))::text END ||
                                ',"effective_start_at":' || to_json(to_char((common_value).effective_start_at AT TIME ZONE 'UTC',
                                    'YYYY-MM-DD"T"HH24:MI:SS"Z"'))::text ||
                                ',"evidence_ref":' || to_json((common_value).evidence_ref)::text ||
                                ',"foundation_scope":' || to_json((common_value).foundation_scope)::text ||
                                ',"lifecycle_state":' || to_json((common_value).lifecycle_state)::text ||
                                ',"observed_at":' || to_json(to_char((common_value).observed_at AT TIME ZONE 'UTC',
                                    'YYYY-MM-DD"T"HH24:MI:SS"Z"'))::text ||
                                ',"positive_evidence_only":' || (common_value).positive_evidence_only::text ||
                                ',"publication_enabled":' || (common_value).publication_enabled::text ||
                                ',"record_contract":' || to_json((common_value).record_contract)::text ||
                                ',"record_contract_sha256":' || to_json(encode((common_value).record_contract_sha256, 'hex'))::text ||
                                ',"record_type":' || to_json((common_value).record_type)::text ||
                                ',"relationship_class":' || to_json((common_value).relationship_class)::text ||
                                ',"serving_authority":' || to_json((common_value).serving_authority)::text ||
                                ',"source_kind":' || to_json((common_value).source_kind)::text ||
                                ',"source_link_ordering_contract_id":' || to_json((common_value).source_link_ordering_contract_id)::text ||
                                ',"source_link_vector_sha256":' || to_json(encode((common_value).source_link_vector_sha256, 'hex'))::text ||
                                ',"source_record_count":' || (common_value).source_record_count::text ||
                                ',"source_release_contract_sha256":' || to_json(encode((common_value).source_release_contract_sha256, 'hex'))::text ||
                                ',"source_release_ref":' || to_json((common_value).source_release_ref)::text ||
                                ',"typed_row_sha256":' || to_json(encode((common_value).typed_row_sha256, 'hex'))::text || '}}' AS common_json
                        FROM pieces
                    ), final_payload AS (
                        SELECT assembled.*,
                            '{{"authority_state":' || authority_json ||
                                ',"contract":' || to_json((common_value).record_contract)::text ||
                                ',"effective_interval":' || effective_json ||
                                ',"evidence":' || evidence_json ||
                                ',"foundation_scope":' || to_json((common_value).foundation_scope)::text ||
                                ',"observed_at":' || to_json(to_char((common_value).observed_at AT TIME ZONE 'UTC',
                                    'YYYY-MM-DD"T"HH24:MI:SS"Z"'))::text ||
                                ',"record_type":' || to_json((common_value).record_type)::text ||
                                ',"source_kind":' || to_json((common_value).source_kind)::text ||
                                ',"source_records":' || source_json ||
                                ',"source_release_contract_sha256":' || to_json(encode((common_value).source_release_contract_sha256, 'hex'))::text ||
                                ',"source_release_ref":' || to_json((common_value).source_release_ref)::text || '}}' AS record_json
                        FROM assembled
                    )
                    SELECT 1 FROM final_payload
                    WHERE (common_value).typed_row_sha256 IS DISTINCT FROM
                            (typed_value).row_sha256
                       OR (common_value).source_link_vector_sha256 IS DISTINCT FROM
                            {digest}('persistence_candidate_source_link_vector', vector_json)
                       OR (common_value).authority_state_sha256 IS DISTINCT FROM
                            {digest}('persistence_candidate_record_authority_state', authority_json)
                       OR (common_value).row_sha256 IS DISTINCT FROM
                            {digest}('persistence_candidate_common_row', common_json)
                       OR (common_value).record_contract_sha256 IS DISTINCT FROM
                            {digest}('evidence_record_contract', record_json)
                       OR (common_value).evidence_ref IS DISTINCT FROM
                            {reference}('peev1_', 'evidence_record', record_json)
                ) INTO invalid;
            END IF;
            IF invalid THEN
                RAISE EXCEPTION 'public_evidence_nppes_admission_invalid'
                    USING ERRCODE='23514';
            END IF;
            INSERT INTO {seal} (admission_ref) VALUES (admitted.admission_ref);
            RETURN NULL;
        END;
        $function$;
        """
    )


def _create_chain_validator(schema: str) -> None:
    function = _qf(schema, "validate_public_evidence_nppes_chain_admission")
    chain = _qt(schema, _CHAIN)
    child = _qt(schema, _CHAIN_ARCHIVE)
    seal = _qt(schema, _CHAIN_SEAL)
    admission = _qt(schema, _ADMISSION)
    digest = _qf(schema, "public_evidence_record_digest")
    reference = _qf(schema, "public_evidence_record_ref")
    op.execute(
        f"""
        CREATE FUNCTION {function}() RETURNS trigger LANGUAGE plpgsql
        SECURITY DEFINER SET search_path=pg_catalog AS $function$
        DECLARE
            parent_row RECORD; child_count bigint; first_ordinal integer;
            last_ordinal integer; source_count bigint; projected_count bigint;
            excluded_count bigint; archive_json text; chain_json text;
            parent_json text; invalid boolean;
        BEGIN
            PERFORM pg_advisory_xact_lock(hashtextextended(
                'healthporta.public-evidence-nppes-chain:' || NEW.chain_ref, 0));
            SELECT * INTO parent_row FROM {chain} WHERE chain_ref=NEW.chain_ref;
            IF parent_row IS NULL THEN
                RAISE EXCEPTION 'public_evidence_nppes_chain_invalid'
                    USING ERRCODE='23514';
            END IF;
            SELECT count(*), min(archive_ordinal), max(archive_ordinal),
                   COALESCE(sum(source_record_count), 0),
                   COALESCE(sum(projected_record_count), 0),
                   COALESCE(sum(excluded_record_count), 0),
                   '[' || string_agg(
                     '{{"admission_ref":' || to_json(admission_ref)::text ||
                     ',"archive_name":' || to_json(archive_name)::text ||
                     ',"artifact_sha256":' || to_json(encode(artifact_sha256, 'hex'))::text ||
                     ',"excluded_record_count":' || excluded_record_count::text ||
                     ',"manifest_sha256":' || to_json(encode(manifest_sha256, 'hex'))::text ||
                     ',"projected_record_count":' || projected_record_count::text ||
                     ',"snapshot_at":' || to_json(to_char(snapshot_at AT TIME ZONE 'UTC',
                        'YYYY-MM-DD"T"HH24:MI:SS"Z"'))::text ||
                     ',"source_record_count":' || source_record_count::text ||
                     ',"source_release_ref":' || to_json(source_release_ref)::text || '}}',
                     ',' ORDER BY archive_ordinal) || ']'
              INTO child_count, first_ordinal, last_ordinal, source_count,
                   projected_count, excluded_count, archive_json
              FROM {child} WHERE chain_ref=parent_row.chain_ref;
            chain_json := '{{"archives":' || archive_json ||
                ',"contract":' || to_json(parent_row.contract)::text ||
                ',"listing_byte_count":' || parent_row.listing_byte_count::text ||
                ',"listing_candidate_names":' || to_json(parent_row.listing_candidate_names)::text ||
                ',"listing_sha256":' || to_json(encode(parent_row.listing_sha256, 'hex'))::text || '}}';
            parent_json := '{{"admission_state":' || to_json(parent_row.admission_state)::text ||
                ',"archive_count":' || parent_row.archive_count::text ||
                ',"chain_ref":' || to_json(parent_row.chain_ref)::text ||
                ',"contract":' || to_json(parent_row.contract)::text ||
                ',"contract_sha256":' || to_json(encode(parent_row.contract_sha256, 'hex'))::text ||
                ',"excluded_record_count":' || parent_row.excluded_record_count::text ||
                ',"listing_byte_count":' || parent_row.listing_byte_count::text ||
                ',"listing_candidate_names":' || to_json(parent_row.listing_candidate_names)::text ||
                ',"listing_sha256":' || to_json(encode(parent_row.listing_sha256, 'hex'))::text ||
                ',"projected_record_count":' || parent_row.projected_record_count::text ||
                ',"publication_enabled":' || parent_row.publication_enabled::text ||
                ',"serving_authority":' || to_json(parent_row.serving_authority)::text ||
                ',"source_record_count":' || parent_row.source_record_count::text || '}}';
            invalid := child_count IS DISTINCT FROM parent_row.archive_count
                OR first_ordinal IS DISTINCT FROM 0
                OR last_ordinal IS DISTINCT FROM parent_row.archive_count - 1
                OR source_count IS DISTINCT FROM parent_row.source_record_count
                OR projected_count IS DISTINCT FROM parent_row.projected_record_count
                OR excluded_count IS DISTINCT FROM parent_row.excluded_record_count
                OR parent_row.contract_sha256 IS DISTINCT FROM
                    {digest}('nppes_public_evidence_import_chain', chain_json)
                OR parent_row.chain_ref IS DISTINCT FROM
                    {reference}('penpc1_', 'nppes_public_evidence_import_chain', chain_json)
                OR parent_row.row_sha256 IS DISTINCT FROM
                    {digest}('nppes_chain_admission_row', parent_json)
                OR array_ndims(parent_row.listing_candidate_names)
                    IS DISTINCT FROM 1
                OR array_lower(parent_row.listing_candidate_names, 1)
                    IS DISTINCT FROM 1
                OR array_position(parent_row.listing_candidate_names, NULL) IS NOT NULL
                OR EXISTS (
                    SELECT 1 FROM unnest(parent_row.listing_candidate_names)
                        AS candidate_name
                    WHERE {_qf(schema, 'public_evidence_nppes_archive_period')}(
                            candidate_name) IS NULL
                )
                OR cardinality(parent_row.listing_candidate_names) IS DISTINCT FROM (
                    SELECT count(DISTINCT candidate_name)
                    FROM unnest(parent_row.listing_candidate_names) AS candidate_name
                )
                OR EXISTS (
                    SELECT 1 FROM {child} AS child_row
                    LEFT JOIN {admission} AS admitted
                      ON ROW(admitted.admission_ref, admitted.source_release_ref,
                             admitted.archive_name, admitted.snapshot_at,
                             admitted.artifact_sha256, admitted.manifest_sha256,
                             admitted.source_record_count,
                             admitted.projected_record_count,
                             admitted.excluded_record_count)
                       = ROW(child_row.admission_ref, child_row.source_release_ref,
                             child_row.archive_name, child_row.snapshot_at,
                             child_row.artifact_sha256, child_row.manifest_sha256,
                             child_row.source_record_count,
                             child_row.projected_record_count,
                             child_row.excluded_record_count)
                    WHERE child_row.chain_ref=parent_row.chain_ref
                      AND (admitted.admission_ref IS NULL
                           OR NOT child_row.archive_name = ANY(
                                parent_row.listing_candidate_names)
                           OR child_row.row_sha256 IS DISTINCT FROM {digest}(
                                'nppes_chain_archive_row',
                                '{{"admission_ref":' || to_json(child_row.admission_ref)::text ||
                                ',"archive_count":' || child_row.archive_count::text ||
                                ',"archive_name":' || to_json(child_row.archive_name)::text ||
                                ',"archive_ordinal":' || child_row.archive_ordinal::text ||
                                ',"artifact_sha256":' || to_json(encode(child_row.artifact_sha256, 'hex'))::text ||
                                ',"chain_ref":' || to_json(child_row.chain_ref)::text ||
                                ',"excluded_record_count":' || child_row.excluded_record_count::text ||
                                ',"manifest_sha256":' || to_json(encode(child_row.manifest_sha256, 'hex'))::text ||
                                ',"projected_record_count":' || child_row.projected_record_count::text ||
                                ',"snapshot_at":' || to_json(to_char(child_row.snapshot_at AT TIME ZONE 'UTC',
                                    'YYYY-MM-DD"T"HH24:MI:SS"Z"'))::text ||
                                ',"source_record_count":' || child_row.source_record_count::text ||
                                ',"source_release_ref":' || to_json(child_row.source_release_ref)::text || '}}'))
                );
            IF NOT invalid THEN
                WITH parsed AS (
                    SELECT candidate_name, candidate_ordinal,
                           {_qf(schema, 'public_evidence_nppes_archive_period')}(
                               candidate_name) AS candidate_period
                    FROM unnest(parent_row.listing_candidate_names)
                         WITH ORDINALITY
                         AS listed(candidate_name, candidate_ordinal)
                ), canonical AS (
                    SELECT candidate_name, candidate_ordinal,
                           row_number() OVER (
                               ORDER BY
                                 CASE WHEN candidate_period[2] IS NULL
                                      THEN 0 ELSE 1 END,
                                 candidate_period[1],
                                 COALESCE(candidate_period[2],
                                          candidate_period[1]),
                                 candidate_name
                           ) AS canonical_ordinal
                    FROM parsed
                )
                SELECT EXISTS (
                    SELECT 1 FROM canonical
                    WHERE candidate_ordinal IS DISTINCT FROM canonical_ordinal
                ) INTO invalid;
            END IF;
            IF NOT invalid THEN
                WITH parsed AS (
                    SELECT candidate_name,
                           {_qf(schema, 'public_evidence_nppes_archive_period')}(
                               candidate_name) AS candidate_period
                    FROM unnest(parent_row.listing_candidate_names)
                         AS listed(candidate_name)
                ), base AS (
                    SELECT archive_name,
                           (snapshot_at AT TIME ZONE 'UTC')::date AS snapshot_date
                    FROM {child}
                    WHERE chain_ref=parent_row.chain_ref
                      AND archive_ordinal=0
                ), latest_monthly AS (
                    SELECT parsed.candidate_name
                    FROM parsed
                    WHERE candidate_period[2] IS NULL
                    ORDER BY candidate_period[1] DESC, candidate_name DESC
                    LIMIT 1
                ), later_weeklies AS (
                    SELECT parsed.candidate_name,
                           candidate_period[1] AS period_start,
                           candidate_period[2] AS period_end,
                           row_number() OVER (
                               ORDER BY candidate_period[1],
                                        candidate_period[2], candidate_name
                           ) AS weekly_ordinal,
                           lag(candidate_period[2]) OVER (
                               ORDER BY candidate_period[1],
                                        candidate_period[2], candidate_name
                           ) AS previous_end,
                           base.snapshot_date
                    FROM parsed CROSS JOIN base
                    WHERE candidate_period[2] IS NOT NULL
                      AND candidate_period[2] > base.snapshot_date
                ), expected AS (
                    SELECT 0::bigint AS archive_ordinal,
                           candidate_name AS archive_name
                    FROM latest_monthly
                    UNION ALL
                    SELECT weekly_ordinal, candidate_name
                    FROM later_weeklies
                ), actual AS (
                    SELECT archive_ordinal::bigint, archive_name
                    FROM {child}
                    WHERE chain_ref=parent_row.chain_ref
                )
                SELECT
                    NOT EXISTS (SELECT 1 FROM latest_monthly)
                    OR EXISTS (
                        SELECT 1 FROM later_weeklies
                        WHERE (weekly_ordinal=1
                               AND period_start IS DISTINCT FROM snapshot_date + 1)
                           OR (weekly_ordinal>1
                               AND period_start IS DISTINCT FROM previous_end + 1)
                    )
                    OR EXISTS (
                        SELECT 1
                        FROM expected FULL JOIN actual USING (archive_ordinal)
                        WHERE expected.archive_name IS DISTINCT FROM
                              actual.archive_name
                    )
                INTO invalid;
            END IF;
            IF invalid THEN
                RAISE EXCEPTION 'public_evidence_nppes_chain_invalid'
                    USING ERRCODE='23514';
            END IF;
            INSERT INTO {seal} (chain_ref) VALUES (parent_row.chain_ref);
            RETURN NULL;
        END;
        $function$;
        """
    )


def _create_append_guard(
    schema: str,
    table_name: str,
    parent_table_name: str,
    *,
    source_only: bool = False,
) -> None:
    function_name = f"guard_{table_name}_admission_append"
    function = _qf(schema, function_name)
    table = _qt(schema, table_name)
    parent = _qt(schema, parent_table_name)
    parent_key = "chain_ref" if parent_table_name == _CHAIN else "admission_ref"
    if parent_table_name == _CHAIN:
        child_key = "chain_ref"
    elif table_name == _MEMBER:
        child_key = "admission_ref"
    else:
        child_key = "nppes_admission_ref"
    source_filter = (
        "inserted.source_kind='nppes_entity_address' AND " if source_only else ""
    )
    seal_table_name = {
        _ADMISSION: _ADMISSION_SEAL,
        _CHAIN: _CHAIN_SEAL,
    }.get(parent_table_name)
    if seal_table_name is not None:
        seal_join = (
            f"LEFT JOIN {_qt(schema, seal_table_name)} AS sealed_row "
            f"ON sealed_row.{_q(parent_key)}=parent_row.{_q(parent_key)}"
        )
        sealed_condition = f" OR sealed_row.{_q(parent_key)} IS NOT NULL"
    else:
        seal_join = ""
        sealed_condition = ""
    op.execute(
        f"""
        CREATE FUNCTION {function}() RETURNS trigger LANGUAGE plpgsql
        SECURITY DEFINER SET search_path=pg_catalog AS $function$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM inserted_rows AS inserted
                LEFT JOIN {parent} AS parent_row
                  ON parent_row.{_q(parent_key)}=inserted.{_q(child_key)}
                {seal_join}
                WHERE {source_filter}(
                    parent_row.{_q(parent_key)} IS NULL
                    {sealed_condition})
            ) THEN
                RAISE EXCEPTION 'public_evidence_nppes_append_outside_admission'
                    USING ERRCODE='23514';
            END IF;
            RETURN NULL;
        END;
        $function$;
        """
    )
    op.execute(
        f"""
        CREATE TRIGGER {_q(table_name + '_admission_append_guard')}
          AFTER INSERT ON {table}
          REFERENCING NEW TABLE AS inserted_rows
          FOR EACH STATEMENT EXECUTE FUNCTION {function}();
        """
    )
    op.execute(
        f"""
        ALTER TABLE {table} ENABLE ALWAYS TRIGGER
          {_q(table_name + '_admission_append_guard')};
        """
    )


def _install_guards(schema: str) -> None:
    immutable = _qf(schema, "guard_public_evidence_immutable_catalog")
    admission_validator = _qf(
        schema, "validate_public_evidence_nppes_registry_admission"
    )
    chain_validator = _qf(
        schema, "validate_public_evidence_nppes_chain_admission"
    )
    parent_guard_statements = (
        f"""
        CREATE CONSTRAINT TRIGGER public_evidence_nppes_registry_admission_integrity_guard
          AFTER INSERT ON {_qt(schema, _ADMISSION)}
          DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
          EXECUTE FUNCTION {admission_validator}();
        """,
        f"""
        ALTER TABLE {_qt(schema, _ADMISSION)} ENABLE ALWAYS TRIGGER
          public_evidence_nppes_registry_admission_integrity_guard;
        """,
        f"""
        CREATE CONSTRAINT TRIGGER public_evidence_nppes_registry_chain_integrity_guard
          AFTER INSERT ON {_qt(schema, _CHAIN)}
          DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
          EXECUTE FUNCTION {chain_validator}();
        """,
        f"""
        ALTER TABLE {_qt(schema, _CHAIN)} ENABLE ALWAYS TRIGGER
          public_evidence_nppes_registry_chain_integrity_guard;
        """,
    )
    for statement in parent_guard_statements:
        op.execute(statement)
    _create_append_guard(schema, _SOURCE, _ADMISSION, source_only=True)
    for table_name in (_COMMON, _LINK, _TYPED, _MEMBER):
        _create_append_guard(schema, table_name, _ADMISSION)
    _create_append_guard(schema, _CHAIN_ARCHIVE, _CHAIN)
    for table_name in _NEW_TABLES:
        table = _qt(schema, table_name)
        mutation = _q(table_name + "_mutation_guard")
        truncate = _q(table_name + "_truncate_guard")
        op.execute(
            f"CREATE TRIGGER {mutation} BEFORE UPDATE OR DELETE ON {table} "
            f"FOR EACH ROW EXECUTE FUNCTION {immutable}();"
        )
        op.execute(f"ALTER TABLE {table} ENABLE ALWAYS TRIGGER {mutation};")
        op.execute(
            f"CREATE TRIGGER {truncate} BEFORE TRUNCATE ON {table} "
            f"FOR EACH STATEMENT EXECUTE FUNCTION {immutable}();"
        )
        op.execute(f"ALTER TABLE {table} ENABLE ALWAYS TRIGGER {truncate};")


def _install_indexes_and_acl(schema: str) -> None:
    op.execute(
        f"CREATE INDEX public_evidence_nppes_registry_member_projection_idx "
        f"ON {_qt(schema, _MEMBER)} "
        "(admission_ref, projection_state, source_row_ordinal);"
    )
    op.execute(
        f"CREATE INDEX public_evidence_nppes_registry_member_evidence_idx "
        f"ON {_qt(schema, _MEMBER)} (source_release_ref, evidence_ref) "
        "WHERE evidence_ref IS NOT NULL;"
    )
    op.execute(
        f"CREATE INDEX public_evidence_nppes_registry_chain_archive_admission_idx "
        f"ON {_qt(schema, _CHAIN_ARCHIVE)} (admission_ref, chain_ref);"
    )
    for table_name in _NEW_TABLES:
        op.execute(f"REVOKE ALL ON TABLE {_qt(schema, table_name)} FROM PUBLIC;")
    function_signatures = (
        ("public_evidence_nppes_framed_digest", "text,text"),
        ("nppes_registry_payload_digest", "text,text,date,date,date,date"),
        ("public_evidence_nppes_archive_period", "text"),
        ("public_evidence_nppes_primary_period", "text"),
        ("public_evidence_nppes_tree_node", "bytea,bytea"),
        ("public_evidence_nppes_merkle_sfunc", "bytea[],bigint,bytea"),
        ("public_evidence_nppes_merkle_final", "bytea[]"),
        (
            "public_evidence_nppes_member_valid",
            "text,text,text,bytea,text,bigint,text,text,date,date,date,date,"
            "text,text,text,bytea,bytea,bytea,text,text,text,bytea",
        ),
        ("validate_public_evidence_nppes_registry_admission", ""),
        ("validate_public_evidence_nppes_chain_admission", ""),
    )
    append_tables = (_SOURCE, _COMMON, _LINK, _TYPED, _MEMBER, _CHAIN_ARCHIVE)
    function_signatures += tuple(
        (f"guard_{table_name}_admission_append", "")
        for table_name in append_tables
    )
    for function_name, signature in function_signatures:
        op.execute(
            f"REVOKE ALL ON FUNCTION {_qf(schema, function_name)}"
            f"({signature}) FROM PUBLIC;"
        )
    op.execute(
        f"REVOKE ALL ON FUNCTION {_qf(schema, 'public_evidence_nppes_merkle_root')}"
        "(bigint,bytea) FROM PUBLIC;"
    )


def upgrade() -> None:
    """Install complete dormant NPPES admission without per-record deferral."""

    schema = _schema()
    _require_empty_legacy_nppes(schema)
    _create_hash_helpers(schema)
    _create_admission_table(schema)
    _create_admission_seal_table(schema)
    _alter_storage_tables(schema)
    _create_member_table(schema)
    _create_chain_tables(schema)
    _create_admission_validator(schema)
    _create_chain_validator(schema)
    _install_guards(schema)
    _install_indexes_and_acl(schema)


def _require_empty_downgrade(schema: str) -> None:
    lock_tables = tuple(
        _qt(schema, table_name)
        for table_name in (
            _CHAIN_ARCHIVE,
            _CHAIN_SEAL,
            _CHAIN,
            _MEMBER,
            _ADMISSION_SEAL,
            _ADMISSION,
            _LINK,
            _TYPED,
            _COMMON,
            _SOURCE,
            _RELEASE,
        )
    )
    op.execute(
        "LOCK TABLE " + ", ".join(lock_tables) + " IN ACCESS EXCLUSIVE MODE;"
    )
    checks = " OR ".join(
        f"EXISTS (SELECT 1 FROM {_qt(schema, table_name)} LIMIT 1)"
        for table_name in (
            _CHAIN_ARCHIVE,
            _CHAIN_SEAL,
            _CHAIN,
            _MEMBER,
            _ADMISSION_SEAL,
            _ADMISSION,
        )
    )
    op.execute(
        f"""
        DO $block$ BEGIN
            IF ({checks})
               OR EXISTS (SELECT 1 FROM {_qt(schema, _COMMON)} LIMIT 1)
               OR EXISTS (SELECT 1 FROM {_qt(schema, _LINK)} LIMIT 1)
               OR EXISTS (SELECT 1 FROM {_qt(schema, _TYPED)} LIMIT 1)
               OR EXISTS (SELECT 1 FROM {_qt(schema, _SOURCE)}
                           WHERE nppes_admission_ref IS NOT NULL LIMIT 1)
            THEN
                RAISE EXCEPTION 'nppes_registry_admission_downgrade_requires_empty_slice'
                    USING ERRCODE='55000';
            END IF;
        END; $block$;
        """
    )


def _drop_new_guards_and_functions(schema: str) -> None:
    op.execute(
        f"DROP TRIGGER public_evidence_nppes_registry_admission_integrity_guard "
        f"ON {_qt(schema, _ADMISSION)};"
    )
    op.execute(
        f"DROP TRIGGER public_evidence_nppes_registry_chain_integrity_guard "
        f"ON {_qt(schema, _CHAIN)};"
    )
    for table_name in (_SOURCE, _COMMON, _LINK, _TYPED, _MEMBER, _CHAIN_ARCHIVE):
        op.execute(
            f"DROP TRIGGER {_q(table_name + '_admission_append_guard')} "
            f"ON {_qt(schema, table_name)};"
        )
        op.execute(
            f"DROP FUNCTION {_qf(schema, 'guard_' + table_name + '_admission_append')}();"
        )
    op.execute(
        f"DROP FUNCTION {_qf(schema, 'validate_public_evidence_nppes_registry_admission')}();"
    )
    op.execute(
        f"DROP FUNCTION {_qf(schema, 'validate_public_evidence_nppes_chain_admission')}();"
    )


def _restore_legacy_storage(schema: str) -> None:
    source = _qt(schema, _SOURCE)
    common = _qt(schema, _COMMON)
    link = _qt(schema, _LINK)
    typed = _qt(schema, _TYPED)
    op.execute(f"DROP TABLE {_qt(schema, _CHAIN_ARCHIVE)};")
    op.execute(f"DROP TABLE {_qt(schema, _CHAIN_SEAL)};")
    op.execute(f"DROP TABLE {_qt(schema, _CHAIN)};")
    op.execute(f"DROP TABLE {_qt(schema, _MEMBER)};")
    op.execute(f"DROP TABLE {_qt(schema, _ADMISSION_SEAL)};")
    for index_name in (
        "public_evidence_source_record_admission_idx",
        "public_evidence_npi_enumeration_lookup_idx",
        "public_evidence_npi_enumeration_admission_idx",
        "public_evidence_record_source_link_admission_idx",
        "public_evidence_record_admission_idx",
    ):
        op.execute(f"DROP INDEX {_q(schema)}.{_q(index_name)};")
    restore_statements = (
        f"""
        ALTER TABLE {link}
          DROP CONSTRAINT public_evidence_record_source_link_record_fkey,
          DROP CONSTRAINT public_evidence_record_source_link_source_fkey,
          DROP CONSTRAINT public_evidence_record_source_link_evidence_key;
        """,
        f"""
        ALTER TABLE {typed}
          DROP CONSTRAINT public_evidence_npi_enumeration_record_fkey;
        """,
        f"""
        ALTER TABLE {source}
          DROP CONSTRAINT public_evidence_source_record_admission_fkey,
          DROP CONSTRAINT public_evidence_source_record_admission_owner_key,
          DROP CONSTRAINT public_evidence_source_record_nppes_shape_check;
        """,
        f"""
        ALTER TABLE {common}
          DROP CONSTRAINT public_evidence_record_admission_fkey,
          DROP CONSTRAINT public_evidence_record_admission_owner_key;
        """,
        f"ALTER TABLE {link} DROP COLUMN nppes_admission_ref;",
        f"ALTER TABLE {typed} DROP COLUMN nppes_admission_ref;",
        f"ALTER TABLE {source} DROP COLUMN nppes_admission_ref;",
        f"ALTER TABLE {common} DROP COLUMN nppes_admission_ref;",
        f"DROP TABLE {_qt(schema, _ADMISSION)};",
        f"""
        ALTER TABLE {link}
          ADD CONSTRAINT public_evidence_record_source_link_record_fkey
            FOREIGN KEY (evidence_ref, source_release_ref,
                         source_release_contract_sha256, source_kind)
            REFERENCES {common} (evidence_ref, source_release_ref,
                source_release_contract_sha256, source_kind)
            ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
          ADD CONSTRAINT public_evidence_record_source_link_source_fkey
            FOREIGN KEY (source_record_ref, source_release_ref,
                         source_release_contract_sha256, source_kind)
            REFERENCES {source} (source_record_ref, source_release_ref,
                source_release_contract_sha256, source_kind)
            ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED;
        """,
        f"""
        ALTER TABLE {typed}
          ADD CONSTRAINT public_evidence_npi_enumeration_record_fkey
            FOREIGN KEY (evidence_ref, source_release_ref,
                         source_release_contract_sha256, source_kind)
            REFERENCES {common} (evidence_ref, source_release_ref,
                source_release_contract_sha256, source_kind)
            ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED;
        """,
    )
    for statement in restore_statements:
        op.execute(statement)
    validator = _qf(schema, "validate_public_evidence_npi_record")
    for table_name in _OLD_RECORD_TABLES:
        table = _qt(schema, table_name)
        trigger = _q(table_name + "_integrity_guard")
        op.execute(
            f"CREATE CONSTRAINT TRIGGER {trigger} AFTER INSERT ON {table} "
            "DEFERRABLE INITIALLY DEFERRED FOR EACH ROW "
            f"EXECUTE FUNCTION {validator}();"
        )
        op.execute(f"ALTER TABLE {table} ENABLE ALWAYS TRIGGER {trigger};")


def _drop_hash_helpers(schema: str) -> None:
    op.execute(
        f"DROP AGGREGATE {_qf(schema, 'public_evidence_nppes_merkle_root')}"
        "(bigint,bytea);"
    )
    op.execute(
        f"DROP FUNCTION {_qf(schema, 'public_evidence_nppes_member_valid')}"
        "(text,text,text,bytea,text,bigint,text,text,date,date,date,date,"
        "text,text,text,bytea,bytea,bytea,text,text,text,bytea);"
    )
    op.execute(
        f"DROP FUNCTION {_qf(schema, 'public_evidence_nppes_merkle_final')}(bytea[]);"
    )
    op.execute(
        f"DROP FUNCTION {_qf(schema, 'public_evidence_nppes_merkle_sfunc')}"
        "(bytea[],bigint,bytea);"
    )
    op.execute(
        f"DROP FUNCTION {_qf(schema, 'public_evidence_nppes_tree_node')}(bytea,bytea);"
    )
    op.execute(
        f"DROP FUNCTION {_qf(schema, 'public_evidence_nppes_primary_period')}(text);"
    )
    op.execute(
        f"DROP FUNCTION {_qf(schema, 'public_evidence_nppes_archive_period')}(text);"
    )
    op.execute(
        f"DROP FUNCTION {_qf(schema, 'nppes_registry_payload_digest')}"
        "(text,text,date,date,date,date);"
    )
    op.execute(
        f"DROP FUNCTION {_qf(schema, 'public_evidence_nppes_framed_digest')}(text,text);"
    )


def downgrade() -> None:
    """Remove only a completely empty NPPES registry-admission slice."""

    schema = _schema()
    _require_empty_downgrade(schema)
    _drop_new_guards_and_functions(schema)
    _restore_legacy_storage(schema)
    _drop_hash_helpers(schema)
