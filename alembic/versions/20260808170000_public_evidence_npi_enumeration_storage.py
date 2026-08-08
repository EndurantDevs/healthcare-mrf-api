# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Add dormant normalized-record storage for public NPI enumeration evidence.

Revision ID: 20260808170000_public_evidence_npi_enumeration_storage
Revises: 20260808160000_fhir_formulary_serving_index
"""

from __future__ import annotations

import os

from alembic import op

revision = "20260808170000_public_evidence_npi_enumeration_storage"
down_revision = "20260808160000_fhir_formulary_serving_index"
branch_labels = None
depends_on = None

_TABLES = (
    "public_evidence_record",
    "public_evidence_record_source_link",
    "public_evidence_npi_enumeration",
)
_FUNCTIONS = (
    ("public_evidence_record_digest", "text,text"),
    ("public_evidence_record_ref", "text,text,text"),
    ("public_evidence_npi_valid", "text"),
    ("validate_public_evidence_npi_record", ""),
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


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _create_digest_helpers(schema: str) -> None:
    digest = _qf(schema, "public_evidence_record_digest")
    reference = _qf(schema, "public_evidence_record_ref")
    npi_valid = _qf(schema, "public_evidence_npi_valid")
    statements = (
        f"""
        CREATE FUNCTION {digest}(candidate_purpose text, candidate_payload text)
        RETURNS bytea LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
        SET search_path = pg_catalog AS $function$
            SELECT sha256(
                convert_to('HEALTHPORTA_PUBLIC_EVIDENCE_RECORD_DIGEST_V1', 'UTF8')
                || decode('00', 'hex')
                || int2send(octet_length(convert_to(candidate_purpose, 'UTF8'))::smallint)
                || convert_to(candidate_purpose, 'UTF8')
                || int8send(octet_length(convert_to(candidate_payload, 'UTF8'))::bigint)
                || convert_to(candidate_payload, 'UTF8')
            );
        $function$;
        """,
        f"""
        CREATE FUNCTION {reference}(
            candidate_prefix text, candidate_purpose text, candidate_payload text
        ) RETURNS text LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
        SET search_path = pg_catalog AS $function$
            SELECT candidate_prefix || translate(rtrim(encode(sha256(
                convert_to('HEALTHPORTA_PUBLIC_EVIDENCE_RECORD_REFERENCE_V1', 'UTF8')
                || decode('00', 'hex')
                || int2send(octet_length(convert_to(candidate_purpose, 'UTF8'))::smallint)
                || convert_to(candidate_purpose, 'UTF8')
                || int8send(octet_length(convert_to(candidate_payload, 'UTF8'))::bigint)
                || convert_to(candidate_payload, 'UTF8')
            ), 'base64'), '='), '+/', '-_');
        $function$;
        """,
        f"""
        CREATE FUNCTION {npi_valid}(candidate_npi text)
        RETURNS boolean LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
        SET search_path = pg_catalog AS $function$
            SELECT CASE WHEN candidate_npi ~ '^[0-9]{{10}}$' THEN
                CASE WHEN candidate_npi::bigint BETWEEN 1000000000 AND 2999999999
                THEN mod(24 + (
                    SELECT sum(CASE
                        WHEN ordinal < 10 AND mod(ordinal, 2) = 1
                        THEN digit * 2 - CASE WHEN digit >= 5 THEN 9 ELSE 0 END
                        ELSE digit END)
                    FROM unnest(string_to_array(candidate_npi, NULL))
                        WITH ORDINALITY AS item(value, ordinal)
                    CROSS JOIN LATERAL (SELECT value::integer AS digit) AS parsed
                ), 10) = 0 ELSE false END
            ELSE false END;
        $function$;
        """,
    )
    for statement in statements:
        op.execute(statement)

def _create_tables(schema: str) -> None:
    common = _qt(schema, _TABLES[0])
    link = _qt(schema, _TABLES[1])
    typed = _qt(schema, _TABLES[2])
    release = _qt(schema, "public_evidence_source_release")
    source_record = _qt(schema, "public_evidence_source_record")
    digest = _qf(schema, "public_evidence_record_digest")
    npi_valid = _qf(schema, "public_evidence_npi_valid")
    statements = (
        f"""
        CREATE TABLE {common} (
            evidence_ref varchar(49) CONSTRAINT public_evidence_record_pkey PRIMARY KEY,
            record_contract varchar(64) NOT NULL,
            record_contract_sha256 bytea NOT NULL,
            foundation_scope varchar(64) NOT NULL,
            source_release_ref varchar(50) NOT NULL,
            source_release_contract_sha256 bytea NOT NULL,
            source_kind varchar(48) NOT NULL,
            observed_at timestamptz NOT NULL,
            effective_start_at timestamptz NOT NULL,
            effective_end_at timestamptz,
            record_type varchar(64) NOT NULL,
            relationship_class varchar(96) NOT NULL,
            source_record_count smallint NOT NULL,
            source_link_ordering_contract_id varchar(96) NOT NULL,
            source_link_vector_sha256 bytea NOT NULL,
            typed_row_sha256 bytea NOT NULL,
            authority_state_sha256 bytea NOT NULL,
            lifecycle_state varchar(32) NOT NULL,
            positive_evidence_only boolean NOT NULL,
            serving_authority varchar(16) NOT NULL,
            current_pointer_authority varchar(16) NOT NULL,
            database_io_authority varchar(16) NOT NULL,
            publication_enabled boolean NOT NULL,
            row_sha256 bytea NOT NULL,
            created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
            CONSTRAINT public_evidence_record_owner_key UNIQUE (
                evidence_ref, source_release_ref,
                source_release_contract_sha256, source_kind
            ),
            CONSTRAINT public_evidence_record_release_fkey FOREIGN KEY (
                source_release_ref, source_release_contract_sha256, source_kind
            ) REFERENCES {release} (
                source_release_ref, contract_sha256, source_kind
            ) ON DELETE RESTRICT,
            CONSTRAINT public_evidence_record_shape_check CHECK ((
                evidence_ref ~ '^peev1_[A-Za-z0-9_-]{{43}}$'
                AND record_contract = 'healthporta.public-evidence-record.v1'
                AND foundation_scope = 'phase_1_public_source_neutral_foundation'
                AND source_kind = 'nppes_entity_address'
                AND record_type = 'npi_enumeration'
                AND relationship_class = 'nppes_npi_enumeration'
                AND source_record_count = 1
                AND source_link_ordering_contract_id =
                    'healthporta_public_evidence_source_record_ref_utf8_byte_ascending_zero_based_v1'
                AND lifecycle_state = 'normalized_record_only'
                AND positive_evidence_only AND serving_authority = 'none'
                AND current_pointer_authority = 'none'
                AND database_io_authority = 'none' AND NOT publication_enabled
                AND octet_length(record_contract_sha256) = 32
                AND octet_length(source_release_contract_sha256) = 32
                AND octet_length(source_link_vector_sha256) = 32
                    AND octet_length(typed_row_sha256) = 32
                AND octet_length(authority_state_sha256) = 32
                    AND octet_length(row_sha256) = 32
                AND observed_at >= TIMESTAMPTZ '0001-01-01 00:00:00+00'
                    AND observed_at < TIMESTAMPTZ '10000-01-01 00:00:00+00'
                AND effective_start_at >= TIMESTAMPTZ '0001-01-01 00:00:00+00'
                    AND effective_start_at < TIMESTAMPTZ '10000-01-01 00:00:00+00'
                AND (effective_end_at IS NULL OR effective_end_at >= effective_start_at
                    AND effective_end_at < TIMESTAMPTZ '10000-01-01 00:00:00+00')
                AND date_trunc('second', observed_at) = observed_at
                AND date_trunc('second', effective_start_at) = effective_start_at
                AND (effective_end_at IS NULL OR
                    date_trunc('second', effective_end_at) = effective_end_at)
            ) IS TRUE)
        );
        """,
        f"""
        CREATE TABLE {link} (
            evidence_ref varchar(49) NOT NULL,
            source_release_ref varchar(50) NOT NULL,
            source_release_contract_sha256 bytea NOT NULL,
            source_kind varchar(48) NOT NULL,
            source_record_ordinal smallint NOT NULL,
            source_record_ref varchar(49) NOT NULL,
            record_kind varchar(64) NOT NULL,
            row_sha256 bytea NOT NULL,
            created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
            CONSTRAINT public_evidence_record_source_link_pkey PRIMARY KEY (
                evidence_ref, source_record_ordinal),
            CONSTRAINT public_evidence_record_source_link_ref_key
                UNIQUE (evidence_ref, source_record_ref),
            CONSTRAINT public_evidence_record_source_link_record_fkey FOREIGN KEY (
                evidence_ref, source_release_ref,
                source_release_contract_sha256, source_kind
            ) REFERENCES {common} (
                evidence_ref, source_release_ref,
                source_release_contract_sha256, source_kind
            ) ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
            CONSTRAINT public_evidence_record_source_link_source_fkey FOREIGN KEY (
                source_record_ref, source_release_ref,
                source_release_contract_sha256, source_kind
            ) REFERENCES {source_record} (
                source_record_ref, source_release_ref,
                source_release_contract_sha256, source_kind
            ) ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
            CONSTRAINT public_evidence_record_source_link_shape_check CHECK ((
                source_kind = 'nppes_entity_address'
                AND source_record_ordinal BETWEEN 0 AND 15
                AND source_record_ref ~ '^pesr1_[A-Za-z0-9_-]{{43}}$'
                AND record_kind = 'nppes_registry_record'
                AND octet_length(source_release_contract_sha256) = 32
                    AND octet_length(row_sha256) = 32
                AND row_sha256 = {digest}('persistence_candidate_source_link_row',
                    '{{"evidence_ref":' || to_json(evidence_ref)::text ||
                    ',"record_kind":' || to_json(record_kind)::text ||
                    ',"source_kind":' || to_json(source_kind)::text ||
                    ',"source_record_ordinal":' || source_record_ordinal::text ||
                    ',"source_record_ref":' || to_json(source_record_ref)::text ||
                    ',"source_release_contract_sha256":' ||
                        to_json(encode(source_release_contract_sha256, 'hex'))::text ||
                    ',"source_release_ref":' || to_json(source_release_ref)::text || '}}')
            ) IS TRUE)
        );
        """,
        f"""
        CREATE TABLE {typed} (
            evidence_ref varchar(49) CONSTRAINT public_evidence_npi_enumeration_pkey
                PRIMARY KEY,
            source_release_ref varchar(50) NOT NULL,
            source_release_contract_sha256 bytea NOT NULL,
            source_kind varchar(48) NOT NULL,
            record_type varchar(64) NOT NULL,
            relationship_class varchar(96) NOT NULL,
            npi varchar(10) NOT NULL,
            npi_entity_type varchar(24) NOT NULL,
            enumeration_state varchar(16) NOT NULL,
            row_sha256 bytea NOT NULL,
            created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
            CONSTRAINT public_evidence_npi_enumeration_record_fkey FOREIGN KEY (
                evidence_ref, source_release_ref,
                source_release_contract_sha256, source_kind
            ) REFERENCES {common} (
                evidence_ref, source_release_ref,
                source_release_contract_sha256, source_kind
            ) ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
            CONSTRAINT public_evidence_npi_enumeration_shape_check CHECK ((
                source_kind = 'nppes_entity_address'
                AND record_type = 'npi_enumeration'
                AND relationship_class = 'nppes_npi_enumeration'
                AND npi_entity_type IN ('individual_type_1', 'organization_type_2')
                AND enumeration_state IN ('active', 'deactivated')
                AND {npi_valid}(npi)
                AND octet_length(source_release_contract_sha256) = 32
                    AND octet_length(row_sha256) = 32
                AND row_sha256 = {digest}('persistence_candidate_typed_row',
                    '{{"enumeration_state":' || to_json(enumeration_state)::text ||
                    ',"evidence_ref":' || to_json(evidence_ref)::text ||
                    ',"npi":' || to_json(npi)::text ||
                    ',"npi_entity_type":' || to_json(npi_entity_type)::text ||
                    ',"record_type":' || to_json(record_type)::text ||
                    ',"relationship_class":' || to_json(relationship_class)::text ||
                    ',"source_kind":' || to_json(source_kind)::text ||
                    ',"source_release_contract_sha256":' ||
                        to_json(encode(source_release_contract_sha256, 'hex'))::text ||
                    ',"source_release_ref":' || to_json(source_release_ref)::text || '}}')
            ) IS TRUE)
        );
        """,
    )
    for statement in statements:
        op.execute(statement)


def _create_validator(schema: str) -> None:
    function = _qf(schema, "validate_public_evidence_npi_record")
    common, link, typed = (_qt(schema, table) for table in _TABLES)
    release = _qt(schema, "public_evidence_source_release")
    source_record = _qt(schema, "public_evidence_source_record")
    digest = _qf(schema, "public_evidence_record_digest")
    reference = _qf(schema, "public_evidence_record_ref")
    lock_domain = _literal(f"healthporta.public-evidence-npi-record:{schema}:")
    op.execute(
        f"""
        CREATE FUNCTION {function}() RETURNS trigger LANGUAGE plpgsql
        SECURITY DEFINER SET search_path = pg_catalog AS $function$
        DECLARE
            common_row RECORD; typed_row RECORD; release_row RECORD;
            link_count bigint; link_json text; source_json text;
            authority_json text; common_json text; record_json text;
            vector_json text; evidence_json text; effective_json text;
        BEGIN
            PERFORM pg_advisory_xact_lock(hashtextextended(
                {lock_domain} || NEW.evidence_ref, 0
            ));
            SELECT * INTO common_row FROM {common} WHERE evidence_ref = NEW.evidence_ref;
            SELECT * INTO typed_row FROM {typed} WHERE evidence_ref = NEW.evidence_ref;
            IF common_row IS NULL OR typed_row IS NULL THEN
                RAISE EXCEPTION 'public_evidence_npi_record_invalid' USING ERRCODE='23514';
            END IF;
            SELECT * INTO release_row FROM {release}
            WHERE source_release_ref = common_row.source_release_ref
              AND contract_sha256 = common_row.source_release_contract_sha256
              AND source_kind = common_row.source_kind;
            SELECT count(*), '[' || string_agg(
                '{{"row_sha256":' || to_json(encode(item.row_sha256, 'hex'))::text ||
                ',"source_record_ordinal":' || item.source_record_ordinal::text ||
                ',"source_record_ref":' || to_json(item.source_record_ref)::text || '}}',
                ',' ORDER BY item.source_record_ordinal) || ']'
            INTO link_count, link_json FROM {link} AS item
            WHERE item.evidence_ref = NEW.evidence_ref;
            SELECT '[' || string_agg(
                '{{"identity_contract_id":' || to_json(root.identity_contract_id)::text ||
                ',"payload_sha256":' || to_json(encode(root.payload_sha256, 'hex'))::text ||
                ',"record_hmac_sha256":' || to_json(encode(root.record_hmac_sha256, 'hex'))::text ||
                ',"record_kind":' || to_json(root.record_kind)::text ||
                ',"source_record_ref":' || to_json(root.source_record_ref)::text ||
                ',"source_release_ref":' || to_json(root.source_release_ref)::text || '}}',
                ',' ORDER BY convert_to(item.source_record_ref, 'UTF8')) || ']'
            INTO source_json FROM {link} AS item JOIN {source_record} AS root
              ON (root.source_record_ref, root.source_release_ref,
                  root.source_release_contract_sha256, root.source_kind) =
                 (item.source_record_ref, item.source_release_ref,
                  item.source_release_contract_sha256, item.source_kind)
            WHERE item.evidence_ref = NEW.evidence_ref;
            authority_json := replace(replace('{{"adapter_execution_authority":"none",' ||
                '"address_selection_authority":"none","confidence_claimed":"F",' ||
                '"current_pointer_authority":"none","database_io_authority":"none",' ||
                '"deletion_enabled":"F","employment_claimed":"F","exact_rate_site_claimed":"F",' ||
                '"executor_authority":"none","facility_ownership_claimed":"F",' ||
                '"independence_claimed":"F","legal_ownership_claimed":"F",' ||
                '"lifecycle_state":"normalized_record_only","payer_confirmed_site_claimed":"F",' ||
                '"positive_evidence_only":"T","publication_enabled":"F",' ||
                '"replacement_enabled":"F","retirement_enabled":"F","serving_authority":"none",' ||
                '"site_match_claimed":"F","supersession_enabled":"F"}}',
                '"F"', 'false'), '"T"', 'true');
            vector_json := '{{"links":' || link_json || ',"ordering_contract_id":' ||
                to_json(common_row.source_link_ordering_contract_id)::text ||
                ',"source_record_count":' || common_row.source_record_count::text || '}}';
            evidence_json := '{{"enumeration_state":' || to_json(typed_row.enumeration_state)::text ||
                ',"npi":' || to_json(typed_row.npi)::text || ',"npi_entity_type":' ||
                to_json(typed_row.npi_entity_type)::text || ',"relationship_class":' ||
                to_json(typed_row.relationship_class)::text || '}}';
            effective_json := '{{"end_at":' || CASE WHEN common_row.effective_end_at IS NULL
                THEN 'null' ELSE to_json(to_char(common_row.effective_end_at AT TIME ZONE 'UTC',
                    'YYYY-MM-DD"T"HH24:MI:SS"Z"'))::text END || ',"start_at":' ||
                to_json(to_char(common_row.effective_start_at AT TIME ZONE 'UTC',
                    'YYYY-MM-DD"T"HH24:MI:SS"Z"'))::text || '}}';
            common_json := '{{"authority_state_sha256":' ||
                to_json(encode(common_row.authority_state_sha256, 'hex'))::text ||
                ',"current_pointer_authority":' || to_json(common_row.current_pointer_authority)::text ||
                ',"database_io_authority":' || to_json(common_row.database_io_authority)::text ||
                ',"effective_end_at":' || CASE WHEN common_row.effective_end_at IS NULL THEN 'null'
                    ELSE to_json(to_char(common_row.effective_end_at AT TIME ZONE 'UTC',
                        'YYYY-MM-DD"T"HH24:MI:SS"Z"'))::text END ||
                ',"effective_start_at":' || to_json(to_char(common_row.effective_start_at AT TIME ZONE 'UTC',
                    'YYYY-MM-DD"T"HH24:MI:SS"Z"'))::text ||
                ',"evidence_ref":' || to_json(common_row.evidence_ref)::text ||
                ',"foundation_scope":' || to_json(common_row.foundation_scope)::text ||
                ',"lifecycle_state":' || to_json(common_row.lifecycle_state)::text ||
                ',"observed_at":' || to_json(to_char(common_row.observed_at AT TIME ZONE 'UTC',
                    'YYYY-MM-DD"T"HH24:MI:SS"Z"'))::text ||
                ',"positive_evidence_only":' || common_row.positive_evidence_only::text ||
                ',"publication_enabled":' || common_row.publication_enabled::text ||
                ',"record_contract":' || to_json(common_row.record_contract)::text ||
                ',"record_contract_sha256":' || to_json(encode(common_row.record_contract_sha256, 'hex'))::text ||
                ',"record_type":' || to_json(common_row.record_type)::text ||
                ',"relationship_class":' || to_json(common_row.relationship_class)::text ||
                ',"serving_authority":' || to_json(common_row.serving_authority)::text ||
                ',"source_kind":' || to_json(common_row.source_kind)::text ||
                ',"source_link_ordering_contract_id":' ||
                    to_json(common_row.source_link_ordering_contract_id)::text ||
                ',"source_link_vector_sha256":' ||
                    to_json(encode(common_row.source_link_vector_sha256, 'hex'))::text ||
                ',"source_record_count":' || common_row.source_record_count::text ||
                ',"source_release_contract_sha256":' ||
                    to_json(encode(common_row.source_release_contract_sha256, 'hex'))::text ||
                ',"source_release_ref":' || to_json(common_row.source_release_ref)::text ||
                ',"typed_row_sha256":' || to_json(encode(common_row.typed_row_sha256, 'hex'))::text || '}}';
            record_json := '{{"authority_state":' || authority_json || ',"contract":' ||
                to_json(common_row.record_contract)::text || ',"effective_interval":' || effective_json ||
                ',"evidence":' || evidence_json || ',"foundation_scope":' ||
                to_json(common_row.foundation_scope)::text || ',"observed_at":' ||
                to_json(to_char(common_row.observed_at AT TIME ZONE 'UTC',
                    'YYYY-MM-DD"T"HH24:MI:SS"Z"'))::text || ',"record_type":' ||
                to_json(common_row.record_type)::text || ',"source_kind":' ||
                to_json(common_row.source_kind)::text || ',"source_records":' || source_json ||
                ',"source_release_contract_sha256":' ||
                to_json(encode(common_row.source_release_contract_sha256, 'hex'))::text ||
                ',"source_release_ref":' || to_json(common_row.source_release_ref)::text || '}}';
            IF release_row IS NULL OR link_count IS DISTINCT FROM common_row.source_record_count
               OR link_count IS DISTINCT FROM 1 OR link_json IS NULL OR source_json IS NULL
               OR common_row.typed_row_sha256 IS DISTINCT FROM typed_row.row_sha256
               OR ROW(typed_row.source_release_ref, typed_row.source_release_contract_sha256,
                      typed_row.source_kind, typed_row.record_type, typed_row.relationship_class)
                    IS DISTINCT FROM ROW(common_row.source_release_ref,
                      common_row.source_release_contract_sha256, common_row.source_kind,
                      common_row.record_type, common_row.relationship_class)
               OR EXISTS (SELECT 1 FROM {link} AS owned_link
                    WHERE owned_link.evidence_ref = NEW.evidence_ref AND ROW(owned_link.source_release_ref,
                      owned_link.source_release_contract_sha256, owned_link.source_kind)
                    IS DISTINCT FROM ROW(common_row.source_release_ref,
                      common_row.source_release_contract_sha256, common_row.source_kind))
               OR common_row.source_link_vector_sha256 IS DISTINCT FROM {digest}(
                    'persistence_candidate_source_link_vector', vector_json)
               OR common_row.authority_state_sha256 IS DISTINCT FROM {digest}(
                    'persistence_candidate_record_authority_state', authority_json)
               OR common_row.row_sha256 IS DISTINCT FROM {digest}(
                    'persistence_candidate_common_row', common_json)
               OR common_row.record_contract_sha256 IS DISTINCT FROM {digest}(
                    'evidence_record_contract', record_json)
               OR common_row.evidence_ref IS DISTINCT FROM {reference}(
                    'peev1_', 'evidence_record', record_json)
               OR common_row.observed_at NOT BETWEEN release_row.observed_start_at
                    AND release_row.observed_end_at
               OR common_row.effective_start_at < release_row.effective_start_at
               OR release_row.effective_end_at IS NOT NULL AND
                    (common_row.effective_end_at IS NULL OR
                     common_row.effective_end_at > release_row.effective_end_at)
               OR typed_row.enumeration_state = 'deactivated' AND
                    common_row.effective_end_at IS NULL
               OR EXISTS (SELECT 1 FROM (
                    SELECT item.source_record_ordinal,
                        row_number() OVER (ORDER BY convert_to(item.source_record_ref, 'UTF8')) - 1
                            AS expected_ordinal
                    FROM {link} AS item WHERE item.evidence_ref = NEW.evidence_ref
               ) AS ordered WHERE source_record_ordinal IS DISTINCT FROM expected_ordinal)
            THEN RAISE EXCEPTION 'public_evidence_npi_record_invalid' USING ERRCODE='23514';
            END IF;
            RETURN NULL;
        END;
        $function$;
        """
    )


def _install_guards(schema: str) -> None:
    validator = _qf(schema, "validate_public_evidence_npi_record")
    immutable_guard = _qf(schema, "guard_public_evidence_immutable_catalog")
    for table_name in _TABLES:
        table = _qt(schema, table_name)
        op.execute(f"CREATE CONSTRAINT TRIGGER {_q(table_name + '_integrity_guard')} "
                   f"AFTER INSERT ON {table} DEFERRABLE INITIALLY DEFERRED FOR EACH ROW "
                   f"EXECUTE FUNCTION {validator}();")
        op.execute(f"CREATE TRIGGER {_q(table_name + '_mutation_guard')} BEFORE UPDATE OR DELETE "
                   f"ON {table} FOR EACH ROW EXECUTE FUNCTION {immutable_guard}();")
        op.execute(f"CREATE TRIGGER {_q(table_name + '_truncate_guard')} BEFORE TRUNCATE ON {table} "
                   f"FOR EACH STATEMENT EXECUTE FUNCTION {immutable_guard}();")
        for suffix in ("integrity", "mutation", "truncate"):
            op.execute(f"ALTER TABLE {table} ENABLE ALWAYS TRIGGER "
                       f"{_q(table_name + '_' + suffix + '_guard')};")
        op.execute(f"REVOKE ALL ON TABLE {table} FROM PUBLIC;")


def upgrade() -> None:
    """Install an empty, publication-disabled NPI record family."""

    schema = _schema()
    _create_digest_helpers(schema)
    _create_tables(schema)
    _create_validator(schema)
    _install_guards(schema)
    for function_name, argument_types in _FUNCTIONS:
        op.execute(f"REVOKE ALL ON FUNCTION {_qf(schema, function_name)}"
                   f"({argument_types}) FROM PUBLIC;")

def downgrade() -> None:
    """Remove only an empty NPI-enumeration persistence slice."""

    schema = _schema()
    tables = tuple(_qt(schema, table) for table in _TABLES)
    parents = (_qt(schema, "public_evidence_source_release"),
               _qt(schema, "public_evidence_source_record"))
    op.execute("LOCK TABLE " + ", ".join((*tables, *parents)) + " IN ACCESS EXCLUSIVE MODE;")
    checks = " OR ".join(f"EXISTS (SELECT 1 FROM {table} LIMIT 1)" for table in tables)
    op.execute(
        f"""
        DO $block$ BEGIN IF ({checks}) IS TRUE THEN
            RAISE EXCEPTION 'public_evidence_downgrade_requires_empty_npi_records'
                USING ERRCODE='55000';
        END IF; END; $block$;
        """
    )
    for table_name in reversed(_TABLES):
        op.execute(f"DROP TABLE {_qt(schema, table_name)};")
    for function_name, argument_types in reversed(_FUNCTIONS):
        op.execute(f"DROP FUNCTION {_qf(schema, function_name)}({argument_types});")
