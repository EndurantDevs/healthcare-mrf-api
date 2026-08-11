"""Add asymmetric receipt epochs and fresh-V12 abandonment evidence.

Revision ID: 20260810110000_ptg_wave_receipt_authority
Revises: 20260810100000_provider_directory_terminal_root_retirement_resource_count_repair

Legacy v1-v5 wave and materialized-preclaim recovery contracts remain unchanged.
Only fresh v6 admissions may persist the new receipt-key and RSA receipt shape.
"""

from __future__ import annotations

import os

from alembic import op


revision = "20260810110000_ptg_wave_receipt_authority"
down_revision = (
    "20260810100000_provider_directory_terminal_root_retirement_resource_count_repair"
)
branch_labels = None
depends_on = None

_V6 = "healthporta.ptg-import-wave-attestation.v6"
_LINKAGE = "healthporta.ptg-wave-linkage-receipt.v2"
_PROOF = (
    "healthporta.ptg-wave.v12-pristine-materialized-abandonment-proof.v1"
)
_ABANDONMENT = "healthporta.ptg-wave-abandonment-receipt.v2"
_ORDINARY_TERMINAL = (
    "healthporta.ptg-wave-ordinary-terminal-receipt.v1"
)
_COORDINATE_DOMAIN = (
    "healthporta.ptg-wave-ordinary-terminal-coordinate.v1"
)
_SCOPE_DOMAIN = "healthporta.ptg-wave-ordinary-terminal-scope.v1"
_TERMINAL_RESULT_DOMAIN = (
    "healthporta.ptg-wave-ordinary-terminal-result.v1"
)
_RUN_PARAMS_DOMAIN = (
    "healthporta.ptg-wave-ordinary-terminal-run-params.v1"
)
_RUN_METRICS_DOMAIN = (
    "healthporta.ptg-wave-ordinary-terminal-run-metrics.v1"
)
_ENGINE_OPTIONS_DOMAIN = (
    "healthporta.ptg-wave-ordinary-terminal-engine-options.v1"
)
_ENGINE_REPORT_DOMAIN = (
    "healthporta.ptg-wave-ordinary-terminal-engine-report.v1"
)
_SNAPSHOT_MANIFEST_DOMAIN = (
    "healthporta.ptg-wave-ordinary-terminal-snapshot-manifest.v1"
)
_LEGACY_BASIS = "materialized_preclaim_failure"
_V12_BASIS = "v12_pristine_materialized_cutover"
_ADMISSION_LOCK = "import-run-admission:ptg-source-file"
_CAPACITY_STATES = (
    "admitted",
    "materializing",
    "slots_waiting",
    "redis_releasing",
    "released",
    "executing",
    "awaiting_linkage",
    "terminalizing",
    "cleaning",
    "uncertain",
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


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _domain_json_digest_sql(
    schema: str,
    domain: str,
    json_expression: str,
) -> str:
    canonical = _qt(schema, "ptg_wave_canonical_json_ascii_v1")
    return f"""
        encode(
            sha256(
                convert_to({_literal(domain)}, 'UTF8')
                || decode('00', 'hex')
                || convert_to({canonical}({json_expression}), 'UTF8')
            ),
            'hex'
        )
    """


def _expected_admission_sql(alias: str) -> str:
    attestation = f"{alias}.cohort_attestation::jsonb"
    return f"""
        jsonb_build_object(
            'attestation_schema', '{_V6}',
            'receipt_key_id', {alias}.receipt_key_id,
            'receipt_public_modulus_hex',
                {alias}.receipt_public_modulus_hex,
            'receipt_public_exponent', {alias}.receipt_public_exponent,
            'wave_id', {alias}.wave_id,
            'wave_digest', {alias}.wave_digest,
            'request_digest', {alias}.request_digest,
            'cohort_attestation_digest',
                {alias}.cohort_attestation_digest,
            'cohort_signature_digest', {alias}.cohort_signature_digest,
            'authorization_digest',
                {attestation} #>> '{{snapshot,authorization_digest}}',
            'snapshot_digest',
                {attestation} #>> '{{snapshot,snapshot_digest}}',
            'membership_digest',
                {attestation} #>> '{{snapshot,membership_digest}}',
            'inventory_digest',
                {attestation} #>> '{{snapshot,inventory_digest}}',
            'subscription_coverage_digest',
                {attestation} #>> '{{snapshot,subscription_coverage_digest}}',
            'entitlement_coverage_digest',
                {attestation} #>> '{{snapshot,entitlement_coverage_digest}}',
            'entitlement_coverage_count',
                ({attestation} #>> '{{snapshot,entitlement_coverage_count}}')::integer,
            'catalog_generation',
                {attestation} #>> '{{snapshot,catalog_generation}}',
            'physical_coordinate_digest', {alias}.physical_coordinate_digest,
            'imported_coordinate_digest', {alias}.imported_coordinate_digest,
            'reused_coordinate_digest', {alias}.reused_coordinate_digest,
            'partition_digest', {alias}.partition_digest,
            'physical_coordinate_count', {alias}.physical_coordinate_count,
            'imported_coordinate_count', {alias}.imported_coordinate_count,
            'reused_coordinate_count', {alias}.reused_coordinate_count,
            'intent_count', {alias}.intent_count,
            'jobs_digest', {alias}.jobs_digest,
            'manifest_digest', {alias}.manifest_digest
        )
    """


def _replace_effective_owner_function(
    *,
    schema: str,
    include_v12: bool,
) -> None:
    wave = _qt(schema, "ptg_import_wave")
    quarantine = _qt(schema, "ptg_import_wave_quarantine")
    supersession = _qt(schema, "ptg_import_wave_supersession")
    function = _qt(schema, "ptg_import_wave_effective_owner_guard")
    states = ", ".join(_literal(state) for state in _CAPACITY_STATES)
    bases = [_LEGACY_BASIS]
    if include_v12:
        bases.append(_V12_BASIS)
    basis_sql = ", ".join(_literal(basis) for basis in bases)
    op.execute(
        f"""
        CREATE OR REPLACE FUNCTION {function}()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        DECLARE
            effective_owner_count integer;
        BEGIN
            PERFORM pg_catalog.pg_advisory_xact_lock(
                pg_catalog.hashtextextended(
                    {_literal(f'ptg-import-wave-effective-owner:{schema}')}, 0
                )
            );
            SELECT count(*) INTO effective_owner_count
              FROM {wave} AS candidate
             WHERE candidate.state IN ({states})
               AND NOT EXISTS (
                   SELECT 1 FROM {supersession} AS retired
                    WHERE retired.predecessor_wave_id = candidate.wave_id
               )
               AND NOT EXISTS (
                   SELECT 1 FROM {quarantine} AS abandoned
                    WHERE abandoned.predecessor_wave_id = candidate.wave_id
                      AND abandoned.recovery_basis IN ({basis_sql})
               );
            IF effective_owner_count > 1 THEN
                RAISE EXCEPTION 'PTG_IMPORT_WAVE_EFFECTIVE_OWNER_CONFLICT'
                    USING ERRCODE = 'P0001';
            END IF;
            RETURN NULL;
        END;
        $$
        """
    )


def _install_receipt_verification_functions(schema: str) -> None:
    """Install bounded canonical JSON and RSA verification primitives."""

    ascii_text = _qt(schema, "ptg_wave_json_ascii_text_v1")
    canonical_json = _qt(schema, "ptg_wave_canonical_json_ascii_v1")
    hex_numeric = _qt(schema, "ptg_wave_hex_to_numeric_v1")
    rsa_verify = _qt(schema, "ptg_wave_rsa2048_pkcs1_sha256_verify_v1")
    signed_receipt = _qt(schema, "ptg_wave_is_valid_signed_receipt_v1")
    op.execute(
        f"""
        CREATE FUNCTION {ascii_text}(value text)
        RETURNS text LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE STRICT
        SET search_path = pg_catalog, {_q(schema)} AS $$
        DECLARE
            escaped text := '';
            character_value text;
            code_point integer;
            supplemental integer;
            high_surrogate integer;
            low_surrogate integer;
        BEGIN
            FOR position IN 1..char_length(value) LOOP
                character_value := substr(value, position, 1);
                code_point := ascii(character_value);
                IF code_point BETWEEN 32 AND 126 THEN
                    escaped := escaped || character_value;
                ELSIF code_point <= 65535 THEN
                    escaped := escaped || E'\\\\u'
                        || lpad(lower(to_hex(code_point)), 4, '0');
                ELSE
                    supplemental := code_point - 65536;
                    high_surrogate := 55296 + supplemental / 1024;
                    low_surrogate := 56320 + supplemental % 1024;
                    escaped := escaped || E'\\\\u'
                        || lpad(lower(to_hex(high_surrogate)), 4, '0')
                        || E'\\\\u'
                        || lpad(lower(to_hex(low_surrogate)), 4, '0');
                END IF;
            END LOOP;
            RETURN escaped;
        END;
        $$
        """
    )
    op.execute(
        f"""
        CREATE FUNCTION {canonical_json}(payload jsonb)
        RETURNS text LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE STRICT
        SET search_path = pg_catalog, {_q(schema)} AS $$
        DECLARE
            canonical_value text;
        BEGIN
            CASE jsonb_typeof(payload)
                WHEN 'object' THEN
                    SELECT '{{' || COALESCE(
                        string_agg(
                            {ascii_text}(to_jsonb(entry.key)::text)
                            || ':' || {canonical_json}(entry.value),
                            ',' ORDER BY entry.key COLLATE "C"
                        ),
                        ''
                    ) || '}}'
                      INTO canonical_value
                      FROM jsonb_each(payload) AS entry;
                    RETURN canonical_value;
                WHEN 'array' THEN
                    SELECT '[' || COALESCE(
                        string_agg(
                            {canonical_json}(entry.value),
                            ',' ORDER BY entry.ordinality
                        ),
                        ''
                    ) || ']'
                      INTO canonical_value
                      FROM jsonb_array_elements(payload) WITH ORDINALITY
                           AS entry(value, ordinality);
                    RETURN canonical_value;
                WHEN 'string' THEN
                    RETURN {ascii_text}(payload::text);
                ELSE
                    RETURN payload::text;
            END CASE;
        END;
        $$
        """
    )
    op.execute(
        f"""
        CREATE FUNCTION {hex_numeric}(hex_value text)
        RETURNS numeric LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE STRICT
        SET search_path = pg_catalog, {_q(schema)} AS $$
        DECLARE
            result numeric := 0;
            position integer;
            digit integer;
        BEGIN
            IF length(hex_value) < 1
               OR length(hex_value) > 512
               OR hex_value !~ '^[0-9a-f]+$' THEN
                RAISE EXCEPTION 'hex integer is invalid'
                    USING ERRCODE = '22023';
            END IF;
            FOR position IN 1..length(hex_value) LOOP
                digit := strpos(
                    '0123456789abcdef',
                    substr(hex_value, position, 1)
                ) - 1;
                result := result * 16 + digit;
            END LOOP;
            RETURN result;
        END;
        $$
        """
    )
    op.execute(
        f"""
        CREATE FUNCTION {rsa_verify}(
            signature_hex text,
            modulus_hex text,
            exponent_value integer,
            message_bytes bytea
        ) RETURNS boolean
        LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE STRICT
        SET search_path = pg_catalog, {_q(schema)} AS $$
        DECLARE
            signature_number numeric;
            modulus_number numeric;
            base_number numeric;
            result_number numeric := 1;
            remaining_exponent integer := exponent_value;
            expected_hex text;
        BEGIN
            IF length(signature_hex) <> 512
               OR signature_hex !~ '^[0-9a-f]+$'
               OR length(modulus_hex) <> 512
               OR modulus_hex !~ '^[0-9a-f]+$'
               OR left(modulus_hex, 1) !~ '^[89a-f]$'
               OR right(modulus_hex, 1) !~ '^[13579bdf]$'
               OR exponent_value <> 65537 THEN
                RETURN FALSE;
            END IF;
            signature_number := {hex_numeric}(signature_hex);
            modulus_number := {hex_numeric}(modulus_hex);
            IF signature_number >= modulus_number THEN
                RETURN FALSE;
            END IF;
            base_number := mod(signature_number, modulus_number);
            WHILE remaining_exponent > 0 LOOP
                IF mod(remaining_exponent, 2) = 1 THEN
                    result_number := mod(
                        result_number * base_number,
                        modulus_number
                    );
                END IF;
                remaining_exponent := remaining_exponent / 2;
                IF remaining_exponent > 0 THEN
                    base_number := mod(
                        base_number * base_number,
                        modulus_number
                    );
                END IF;
            END LOOP;
            expected_hex := '0001' || repeat('ff', 202) || '00'
                || '3031300d060960864801650304020105000420'
                || encode(pg_catalog.sha256(message_bytes), 'hex');
            RETURN result_number = {hex_numeric}(expected_hex);
        EXCEPTION
            WHEN data_exception OR invalid_text_representation
                 OR numeric_value_out_of_range THEN
                RETURN FALSE;
        END;
        $$
        """
    )
    op.execute(
        f"""
        CREATE FUNCTION {signed_receipt}(
            receipt jsonb,
            expected_schema text,
            expected_payload jsonb,
            expected_key_id text,
            expected_modulus text,
            expected_exponent integer
        ) RETURNS boolean
        LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE STRICT
        SET search_path = pg_catalog, {_q(schema)} AS $$
        DECLARE
            issued_at text;
            message_bytes bytea;
        BEGIN
            IF jsonb_typeof(receipt) IS DISTINCT FROM 'object'
               OR (SELECT count(*) FROM jsonb_object_keys(receipt)) <> 6
               OR receipt - ARRAY[
                    'schema', 'key_id', 'issued_at', 'payload',
                    'payload_digest', 'signature'
               ]::text[] <> '{{}}'::jsonb
               OR receipt->>'schema' IS DISTINCT FROM expected_schema
               OR receipt->>'key_id' IS DISTINCT FROM expected_key_id
               OR receipt->'payload' IS DISTINCT FROM expected_payload
               OR receipt->>'payload_digest' !~ '^[0-9a-f]{{64}}$'
               OR length(receipt->>'signature') <> 512
               OR receipt->>'signature' !~ '^[0-9a-f]+$' THEN
                RETURN FALSE;
            END IF;
            issued_at := receipt->>'issued_at';
            IF issued_at !~
               '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}T[0-9]{{2}}:[0-9]{{2}}:[0-9]{{2}}\\.[0-9]{{6}}Z$'
               OR to_char(
                    issued_at::timestamptz AT TIME ZONE 'UTC',
                    'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
               ) <> issued_at THEN
                RETURN FALSE;
            END IF;
            message_bytes := convert_to(expected_schema, 'UTF8')
                || decode('00', 'hex')
                || convert_to(
                    {canonical_json}(
                        jsonb_build_object(
                            'key_id', expected_key_id,
                            'issued_at', issued_at,
                            'payload', expected_payload
                        )
                    ),
                    'UTF8'
                );
            RETURN receipt->>'payload_digest'
                    = encode(pg_catalog.sha256(message_bytes), 'hex')
               AND {rsa_verify}(
                    receipt->>'signature',
                    expected_modulus,
                    expected_exponent,
                    message_bytes
               );
        EXCEPTION
            WHEN data_exception OR invalid_text_representation
                 OR datetime_field_overflow THEN
                RETURN FALSE;
        END;
        $$
        """
    )


def upgrade() -> None:
    """Install v6 receipt storage, proof checks, and immutable fences."""

    schema = _schema()
    wave = _qt(schema, "ptg_import_wave")
    quarantine = _qt(schema, "ptg_import_wave_quarantine")
    supersession = _qt(schema, "ptg_import_wave_supersession")
    rollback = _qt(schema, "ptg_import_wave_admission_rollback")
    intent = _qt(schema, "ptg_import_wave_intent")
    claim = _qt(schema, "ptg_import_wave_claim")
    outcome = _qt(schema, "ptg_import_wave_outcome")
    run = _qt(schema, "import_run")
    event = _qt(schema, "ptg_source_attempt_event")
    engine_run = _qt(schema, "ptg2_import_run")
    engine_snapshot = _qt(schema, "ptg2_snapshot")
    ordinary_terminal = _qt(
        schema, "ptg_import_wave_ordinary_terminal_receipt"
    )

    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute(
        f"LOCK TABLE {wave}, {quarantine}, {supersession}, {rollback}, "
        f"{intent}, {claim}, {outcome}, {run}, {event}, "
        f"{engine_run}, {engine_snapshot} "
        "IN SHARE ROW EXCLUSIVE MODE"
    )

    op.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {ordinary_terminal} (
            wave_id varchar(64) NOT NULL,
            member_ordinal integer NOT NULL,
            source_file_import_id varchar(64) NOT NULL,
            run_id varchar(64) NOT NULL,
            receipt_key_id varchar(64) NOT NULL,
            receipt jsonb NOT NULL,
            payload_digest varchar(64) NOT NULL,
            issued_at timestamptz NOT NULL,
            created_at timestamptz NOT NULL,
            CONSTRAINT {_q('ptg_wave_ordinary_terminal_receipt_pkey')}
                PRIMARY KEY (wave_id, member_ordinal),
            CONSTRAINT {_q('ptg_wave_ordinary_terminal_member_fkey')}
                FOREIGN KEY (wave_id, member_ordinal)
                REFERENCES {intent} (wave_id, ordinal) ON DELETE RESTRICT,
            CONSTRAINT {_q('ptg_wave_ordinary_terminal_run_fkey')}
                FOREIGN KEY (run_id) REFERENCES {run} (run_id)
                ON DELETE RESTRICT,
            CONSTRAINT {_q('ptg_wave_ordinary_terminal_source_import_key')}
                UNIQUE (source_file_import_id),
            CONSTRAINT {_q('ptg_wave_ordinary_terminal_run_id_key')}
                UNIQUE (run_id),
            CONSTRAINT {_q('ptg_wave_ordinary_terminal_receipt_check')}
                CHECK (
                    member_ordinal >= 0
                    AND length(source_file_import_id) BETWEEN 1 AND 64
                    AND length(run_id) BETWEEN 1 AND 64
                    AND receipt_key_id
                        ~ '^[A-Za-z0-9][A-Za-z0-9._-]{{0,63}}$'
                    AND jsonb_typeof(receipt) = 'object'
                    AND receipt->>'schema' = '{_ORDINARY_TERMINAL}'
                    AND receipt->>'key_id' = receipt_key_id
                    AND receipt->>'payload_digest' = payload_digest
                    AND receipt#>>'{{payload,wave_id}}' = wave_id
                    AND (receipt#>>'{{payload,member_ordinal}}')::integer
                        = member_ordinal
                    AND receipt#>>'{{payload,source_file_import_id}}'
                        = source_file_import_id
                    AND receipt#>>'{{payload,run_id}}' = run_id
                    AND payload_digest ~ '^[0-9a-f]{{64}}$'
                    AND length(receipt->>'signature') = 512
                    AND receipt->>'signature' ~ '^[0-9a-f]+$'
                )
        )
        """
    )
    op.execute(
        f"LOCK TABLE {ordinary_terminal} IN ACCESS EXCLUSIVE MODE"
    )
    op.execute(
        f"""
        CREATE TEMP TABLE ptg_wave_ordinary_terminal_expected_20260810110000 (
            wave_id varchar(64) NOT NULL,
            member_ordinal integer NOT NULL,
            source_file_import_id varchar(64) NOT NULL,
            run_id varchar(64) NOT NULL,
            receipt_key_id varchar(64) NOT NULL,
            receipt jsonb NOT NULL,
            payload_digest varchar(64) NOT NULL,
            issued_at timestamptz NOT NULL,
            created_at timestamptz NOT NULL,
            CONSTRAINT ptg_wave_ordinary_terminal_expected_check CHECK (
                member_ordinal >= 0
                AND length(source_file_import_id) BETWEEN 1 AND 64
                AND length(run_id) BETWEEN 1 AND 64
                AND receipt_key_id
                    ~ '^[A-Za-z0-9][A-Za-z0-9._-]{{0,63}}$'
                AND jsonb_typeof(receipt) = 'object'
                AND receipt->>'schema' = '{_ORDINARY_TERMINAL}'
                AND receipt->>'key_id' = receipt_key_id
                AND receipt->>'payload_digest' = payload_digest
                AND receipt#>>'{{payload,wave_id}}' = wave_id
                AND (receipt#>>'{{payload,member_ordinal}}')::integer
                    = member_ordinal
                AND receipt#>>'{{payload,source_file_import_id}}'
                    = source_file_import_id
                AND receipt#>>'{{payload,run_id}}' = run_id
                AND payload_digest ~ '^[0-9a-f]{{64}}$'
                AND length(receipt->>'signature') = 512
                AND receipt->>'signature' ~ '^[0-9a-f]+$'
            )
        ) ON COMMIT DROP
        """
    )
    op.execute(
        f"""
        DO $$
        DECLARE
            actual_columns jsonb;
            expected_columns jsonb;
            actual_check text;
            expected_check text;
        BEGIN
            IF EXISTS (SELECT 1 FROM {ordinary_terminal} LIMIT 1) THEN
                RAISE EXCEPTION
                    'PTG_WAVE_ORDINARY_TERMINAL_ADOPTION_NONEMPTY'
                    USING ERRCODE = 'P0001';
            END IF;

            SELECT jsonb_agg(
                       jsonb_build_array(
                           attribute.attname,
                           pg_catalog.format_type(
                               attribute.atttypid, attribute.atttypmod
                           ),
                           attribute.attnotnull,
                           pg_catalog.pg_get_expr(
                               column_default.adbin,
                               column_default.adrelid
                           )
                       ) ORDER BY attribute.attnum
                   )
              INTO actual_columns
              FROM pg_catalog.pg_attribute AS attribute
              LEFT JOIN pg_catalog.pg_attrdef AS column_default
                ON column_default.adrelid = attribute.attrelid
               AND column_default.adnum = attribute.attnum
             WHERE attribute.attrelid = '{ordinary_terminal}'::regclass
               AND attribute.attnum > 0
               AND NOT attribute.attisdropped;
            SELECT jsonb_agg(
                       jsonb_build_array(
                           attribute.attname,
                           pg_catalog.format_type(
                               attribute.atttypid, attribute.atttypmod
                           ),
                           attribute.attnotnull,
                           pg_catalog.pg_get_expr(
                               column_default.adbin,
                               column_default.adrelid
                           )
                       ) ORDER BY attribute.attnum
                   )
              INTO expected_columns
              FROM pg_catalog.pg_attribute AS attribute
              LEFT JOIN pg_catalog.pg_attrdef AS column_default
                ON column_default.adrelid = attribute.attrelid
               AND column_default.adnum = attribute.attnum
             WHERE attribute.attrelid =
                    'pg_temp.ptg_wave_ordinary_terminal_expected_20260810110000'
                        ::regclass
               AND attribute.attnum > 0
               AND NOT attribute.attisdropped;
            IF actual_columns IS DISTINCT FROM expected_columns THEN
                RAISE EXCEPTION
                    'PTG_WAVE_ORDINARY_TERMINAL_ADOPTION_SHAPE_INVALID'
                    USING ERRCODE = 'P0001';
            END IF;

            SELECT pg_catalog.pg_get_expr(
                       constraint_row.conbin,
                       constraint_row.conrelid
                   )
              INTO actual_check
              FROM pg_catalog.pg_constraint AS constraint_row
             WHERE constraint_row.conrelid = '{ordinary_terminal}'::regclass
               AND constraint_row.conname =
                    'ptg_wave_ordinary_terminal_receipt_check'
               AND constraint_row.contype = 'c';
            SELECT pg_catalog.pg_get_expr(
                       constraint_row.conbin,
                       constraint_row.conrelid
                   )
              INTO expected_check
              FROM pg_catalog.pg_constraint AS constraint_row
             WHERE constraint_row.conrelid =
                    'pg_temp.ptg_wave_ordinary_terminal_expected_20260810110000'
                        ::regclass
               AND constraint_row.conname =
                    'ptg_wave_ordinary_terminal_expected_check'
               AND constraint_row.contype = 'c';

            IF actual_check IS DISTINCT FROM expected_check
               OR (SELECT count(*) FROM pg_catalog.pg_constraint
                    WHERE conrelid = '{ordinary_terminal}'::regclass
                      AND contype <> 'n') <> 6
               OR NOT EXISTS (
                    SELECT 1 FROM pg_catalog.pg_constraint
                     WHERE conrelid = '{ordinary_terminal}'::regclass
                       AND conname IN (
                            'ptg_wave_ordinary_terminal_receipt_pkey',
                            'ptg_import_wave_ordinary_terminal_receipt_pkey'
                       )
                       AND contype = 'p' AND conkey = ARRAY[1, 2]::smallint[]
               )
               OR NOT EXISTS (
                    SELECT 1 FROM pg_catalog.pg_constraint
                     WHERE conrelid = '{ordinary_terminal}'::regclass
                       AND conname =
                            'ptg_wave_ordinary_terminal_source_import_key'
                       AND contype = 'u' AND conkey = ARRAY[3]::smallint[]
               )
               OR NOT EXISTS (
                    SELECT 1 FROM pg_catalog.pg_constraint
                     WHERE conrelid = '{ordinary_terminal}'::regclass
                       AND conname =
                            'ptg_wave_ordinary_terminal_run_id_key'
                       AND contype = 'u' AND conkey = ARRAY[4]::smallint[]
               )
               OR NOT EXISTS (
                    SELECT 1
                      FROM pg_catalog.pg_constraint AS foreign_key
                     WHERE foreign_key.conrelid =
                            '{ordinary_terminal}'::regclass
                       AND foreign_key.conname =
                            'ptg_wave_ordinary_terminal_member_fkey'
                       AND foreign_key.contype = 'f'
                       AND foreign_key.conkey = ARRAY[1, 2]::smallint[]
                       AND foreign_key.confrelid = '{intent}'::regclass
                       AND foreign_key.confdeltype = 'r'
                       AND (
                           SELECT array_agg(attribute.attname ORDER BY key.ord)
                             FROM unnest(foreign_key.confkey)
                                  WITH ORDINALITY AS key(attnum, ord)
                             JOIN pg_catalog.pg_attribute AS attribute
                               ON attribute.attrelid = foreign_key.confrelid
                              AND attribute.attnum = key.attnum
                       ) = ARRAY['wave_id', 'ordinal']::name[]
               )
               OR NOT EXISTS (
                    SELECT 1
                      FROM pg_catalog.pg_constraint AS foreign_key
                     WHERE foreign_key.conrelid =
                            '{ordinary_terminal}'::regclass
                       AND foreign_key.conname =
                            'ptg_wave_ordinary_terminal_run_fkey'
                       AND foreign_key.contype = 'f'
                       AND foreign_key.conkey = ARRAY[4]::smallint[]
                       AND foreign_key.confrelid = '{run}'::regclass
                       AND foreign_key.confdeltype = 'r'
                       AND (
                           SELECT array_agg(attribute.attname ORDER BY key.ord)
                             FROM unnest(foreign_key.confkey)
                                  WITH ORDINALITY AS key(attnum, ord)
                             JOIN pg_catalog.pg_attribute AS attribute
                               ON attribute.attrelid = foreign_key.confrelid
                              AND attribute.attnum = key.attnum
                       ) = ARRAY['run_id']::name[]
               )
               OR (SELECT count(*) FROM pg_catalog.pg_index
                    WHERE indrelid = '{ordinary_terminal}'::regclass) <> 3
               OR EXISTS (
                    SELECT 1 FROM pg_catalog.pg_class AS relation
                     WHERE relation.oid = '{ordinary_terminal}'::regclass
                       AND (
                           relation.relkind <> 'r'
                           OR relation.relpersistence <> 'p'
                           OR relation.relrowsecurity
                           OR relation.relforcerowsecurity
                       )
               ) THEN
                RAISE EXCEPTION
                    'PTG_WAVE_ORDINARY_TERMINAL_ADOPTION_CONTRACT_INVALID'
                    USING ERRCODE = 'P0001';
            END IF;
        END;
        $$
        """
    )
    op.execute(
        "DROP TABLE pg_temp."
        "ptg_wave_ordinary_terminal_expected_20260810110000"
    )

    for column_sql in (
        "receipt_key_id varchar(64)",
        "receipt_public_modulus_hex varchar(512)",
        "receipt_public_exponent integer",
        "linkage_receipt json",
        "linkage_receipt_payload_digest varchar(64)",
        "linkage_receipt_issued_at timestamptz",
    ):
        op.execute(f"ALTER TABLE {wave} ADD COLUMN {column_sql}")
    for column_sql in (
        "receipt_key_id varchar(64)",
        "abandonment_receipt jsonb",
        "abandonment_receipt_payload_digest varchar(64)",
        "abandonment_receipt_issued_at timestamptz",
    ):
        op.execute(f"ALTER TABLE {quarantine} ADD COLUMN {column_sql}")

    op.execute(
        f"ALTER TABLE {quarantine} DROP CONSTRAINT "
        f"{_q('ptg_import_wave_quarantine_reason_check')}"
    )
    op.execute(
        f"ALTER TABLE {quarantine} ADD CONSTRAINT "
        f"{_q('ptg_import_wave_quarantine_reason_check')} CHECK (reason IN ("
        "'legacy_uncertain_slots_waiting_pre_receipt', "
        f"'{_LEGACY_BASIS}', '{_V12_BASIS}'))"
    )
    op.execute(
        f"ALTER TABLE {quarantine} DROP CONSTRAINT "
        f"{_q('ptg_import_wave_quarantine_abandonment_evidence_check')}"
    )
    op.execute(
        f"""
        ALTER TABLE {quarantine} ADD CONSTRAINT
            {_q('ptg_import_wave_quarantine_abandonment_evidence_check')}
        CHECK (
            (
                recovery_basis IS NULL
                AND successor_wave_id IS NULL
                AND recovery_evidence IS NULL
                AND recovery_evidence_canonical IS NULL
                AND recovery_evidence_sha256 IS NULL
            ) OR (
                reason = '{_LEGACY_BASIS}'
                AND recovery_basis = '{_LEGACY_BASIS}'
                AND successor_wave_id IS NOT NULL
                AND successor_wave_id <> predecessor_wave_id
                AND jsonb_typeof(recovery_evidence) = 'object'
                AND recovery_evidence_sha256 ~ '^[0-9a-f]{{64}}$'
                AND octet_length(recovery_evidence_canonical) > 0
                AND encode(sha256(recovery_evidence_canonical), 'hex')
                    = recovery_evidence_sha256
                AND convert_from(recovery_evidence_canonical, 'UTF8')::jsonb
                    = recovery_evidence - 'proof_digest'
            ) OR (
                reason = '{_V12_BASIS}'
                AND recovery_basis = '{_V12_BASIS}'
                AND successor_wave_id IS NOT NULL
                AND successor_wave_id <> predecessor_wave_id
                AND jsonb_typeof(recovery_evidence) = 'object'
                AND recovery_evidence->>'schema_version' = '{_PROOF}'
                AND recovery_evidence_sha256 ~ '^[0-9a-f]{{64}}$'
                AND octet_length(recovery_evidence_canonical) > 0
                AND encode(
                    sha256(
                        convert_to('{_PROOF}', 'UTF8')
                        || decode('00', 'hex')
                        || recovery_evidence_canonical
                    ),
                    'hex'
                ) = recovery_evidence_sha256
                AND convert_from(recovery_evidence_canonical, 'UTF8')::jsonb
                    = recovery_evidence - 'proof_digest'
            )
        )
        """
    )

    op.execute(
        f"""
        ALTER TABLE {wave} ADD CONSTRAINT
            {_q('ptg_import_wave_receipt_key_epoch_check')}
        CHECK (
            (
                cohort_attestation::jsonb->>'schema_version' = '{_V6}'
                AND receipt_key_id IS NOT NULL
                AND receipt_key_id ~ '^[A-Za-z0-9][A-Za-z0-9._-]{{0,63}}$'
                AND cohort_attestation::jsonb->>'receipt_key_id'
                    = receipt_key_id
                AND receipt_public_modulus_hex IS NOT NULL
                AND length(receipt_public_modulus_hex) = 512
                AND receipt_public_modulus_hex ~ '^[0-9a-f]+$'
                AND left(receipt_public_modulus_hex, 1) ~ '^[89a-f]$'
                AND right(receipt_public_modulus_hex, 1) ~ '^[13579bdf]$'
                AND receipt_public_exponent IS NOT NULL
                AND receipt_public_exponent = 65537
                AND cohort_attestation::jsonb
                    ->>'receipt_public_modulus_hex'
                    = receipt_public_modulus_hex
                AND (cohort_attestation::jsonb
                    ->>'receipt_public_exponent')::integer
                    = receipt_public_exponent
            ) OR (
                cohort_attestation::jsonb->>'schema_version' <> '{_V6}'
                AND receipt_key_id IS NULL
                AND receipt_public_modulus_hex IS NULL
                AND receipt_public_exponent IS NULL
            )
        )
        """
    )
    op.execute(
        f"""
        ALTER TABLE {wave} ADD CONSTRAINT
            {_q('ptg_import_wave_linkage_receipt_check')}
        CHECK (
            (
                linkage_receipt IS NULL
                AND linkage_receipt_payload_digest IS NULL
                AND linkage_receipt_issued_at IS NULL
            ) OR (
                cohort_attestation::jsonb->>'schema_version' = '{_V6}'
                AND linkage_ack IS NOT NULL
                AND linkage_ack_digest ~ '^[0-9a-f]{{64}}$'
                AND json_typeof(linkage_receipt) = 'object'
                AND linkage_receipt->>'schema' = '{_LINKAGE}'
                AND linkage_receipt->>'key_id' = receipt_key_id
                AND linkage_receipt->>'payload_digest'
                    = linkage_receipt_payload_digest
                AND length(linkage_receipt->>'signature') = 512
                AND linkage_receipt->>'signature' ~ '^[0-9a-f]+$'
                AND linkage_receipt#>>'{{payload,wave_id}}' = wave_id
                AND linkage_receipt#>>'{{payload,wave_digest}}' = wave_digest
                AND linkage_receipt#>>'{{payload,linkage_ack_digest}}'
                    = linkage_ack_digest
                AND linkage_receipt_payload_digest ~ '^[0-9a-f]{{64}}$'
                AND linkage_receipt_issued_at IS NOT NULL
            )
        )
        """
    )
    op.execute(
        f"""
        ALTER TABLE {wave} ADD CONSTRAINT
            {_q('ptg_import_wave_v6_linkage_receipt_required_check')}
        CHECK (
            cohort_attestation::jsonb->>'schema_version' <> '{_V6}'
            OR linkage_ack IS NULL
            OR linkage_receipt IS NOT NULL
        )
        """
    )
    op.execute(
        f"""
        ALTER TABLE {quarantine} ADD CONSTRAINT
            {_q('ptg_import_wave_quarantine_receipt_check')}
        CHECK (
            (
                recovery_basis IS DISTINCT FROM '{_V12_BASIS}'
                AND receipt_key_id IS NULL
                AND abandonment_receipt IS NULL
                AND abandonment_receipt_payload_digest IS NULL
                AND abandonment_receipt_issued_at IS NULL
            ) OR (
                reason = '{_V12_BASIS}'
                AND recovery_basis = '{_V12_BASIS}'
                AND recovery_evidence->>'schema_version' = '{_PROOF}'
                AND receipt_key_id ~ '^[A-Za-z0-9][A-Za-z0-9._-]{{0,63}}$'
                AND jsonb_typeof(abandonment_receipt) = 'object'
                AND abandonment_receipt->>'schema' = '{_ABANDONMENT}'
                AND abandonment_receipt->>'key_id' = receipt_key_id
                AND abandonment_receipt->>'payload_digest'
                    = abandonment_receipt_payload_digest
                AND length(abandonment_receipt->>'signature') = 512
                AND abandonment_receipt->>'signature' ~ '^[0-9a-f]+$'
                AND abandonment_receipt#>>'{{payload,wave_id}}'
                    = predecessor_wave_id
                AND abandonment_receipt#>>'{{payload,cutover_id}}'
                    = successor_wave_id
                AND abandonment_receipt
                    #>>'{{payload,recovery_evidence_sha256}}'
                    = recovery_evidence_sha256
                AND abandonment_receipt_payload_digest ~ '^[0-9a-f]{{64}}$'
                AND abandonment_receipt_issued_at IS NOT NULL
            )
        )
        """
    )

    _install_receipt_verification_functions(schema)
    _install_wave_receipt_guard(schema)
    _install_v12_abandonment_guard(schema)
    _install_v12_immutability_guards(schema)
    _install_ordinary_terminal_receipt_guard(schema)
    _replace_effective_owner_function(schema=schema, include_v12=True)


def _install_wave_receipt_guard(schema: str) -> None:
    wave = _qt(schema, "ptg_import_wave")
    function = _qt(schema, "ptg_import_wave_receipt_guard")
    receipt_verifier = _qt(schema, "ptg_wave_is_valid_signed_receipt_v1")
    op.execute(
        f"""
        CREATE FUNCTION {function}()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        DECLARE
            receipt jsonb;
            payload jsonb;
            expected_payload jsonb;
            expected_cutover_id text;
        BEGIN
            IF TG_OP = 'UPDATE' AND (
                NEW.receipt_key_id IS DISTINCT FROM OLD.receipt_key_id
                OR NEW.receipt_public_modulus_hex
                    IS DISTINCT FROM OLD.receipt_public_modulus_hex
                OR NEW.receipt_public_exponent
                    IS DISTINCT FROM OLD.receipt_public_exponent
            ) THEN
                RAISE EXCEPTION 'PTG_IMPORT_WAVE_RECEIPT_KEY_IMMUTABLE'
                    USING ERRCODE = 'P0001';
            END IF;
            IF NEW.cohort_attestation::jsonb->>'schema_version' = '{_V6}' THEN
                IF NEW.receipt_key_id IS NULL
                   OR NEW.receipt_key_id IS DISTINCT FROM
                        NEW.cohort_attestation::jsonb->>'receipt_key_id'
                   OR NEW.receipt_public_modulus_hex IS DISTINCT FROM
                        NEW.cohort_attestation::jsonb
                            ->>'receipt_public_modulus_hex'
                   OR NEW.receipt_public_exponent IS DISTINCT FROM
                        (NEW.cohort_attestation::jsonb
                            ->>'receipt_public_exponent')::integer
                   OR (SELECT count(*) FROM jsonb_object_keys(
                            NEW.cohort_attestation::jsonb)) <> 10
                   OR NEW.cohort_attestation::jsonb - ARRAY[
                        'schema_version', 'wave_id', 'idempotency_key',
                        'snapshot', 'partition', 'intents', 'receipt_key_id',
                        'receipt_public_modulus_hex',
                        'receipt_public_exponent', 'signature'
                   ]::text[] <> '{{}}'::jsonb THEN
                    RAISE EXCEPTION 'PTG_IMPORT_WAVE_V6_ADMISSION_INVALID'
                        USING ERRCODE = 'P0001';
                END IF;
            ELSIF NEW.receipt_key_id IS NOT NULL
               OR NEW.receipt_public_modulus_hex IS NOT NULL
               OR NEW.receipt_public_exponent IS NOT NULL
               OR NEW.linkage_receipt IS NOT NULL
               OR NEW.linkage_receipt_payload_digest IS NOT NULL
               OR NEW.linkage_receipt_issued_at IS NOT NULL THEN
                RAISE EXCEPTION 'PTG_IMPORT_WAVE_RECEIPT_DOWNGRADE_INVALID'
                    USING ERRCODE = 'P0001';
            END IF;

            IF TG_OP = 'UPDATE' THEN
                IF OLD.linkage_receipt IS NOT NULL AND (
                    NEW.linkage_receipt::jsonb
                        IS DISTINCT FROM OLD.linkage_receipt::jsonb
                    OR NEW.linkage_receipt_payload_digest
                        IS DISTINCT FROM OLD.linkage_receipt_payload_digest
                    OR NEW.linkage_receipt_issued_at
                        IS DISTINCT FROM OLD.linkage_receipt_issued_at
                ) THEN
                    RAISE EXCEPTION 'PTG_IMPORT_WAVE_LINKAGE_RECEIPT_IMMUTABLE'
                        USING ERRCODE = 'P0001';
                END IF;
            END IF;

            IF NEW.linkage_receipt IS NULL THEN
                IF NEW.linkage_receipt_payload_digest IS NOT NULL
                   OR NEW.linkage_receipt_issued_at IS NOT NULL
                   OR (
                       NEW.cohort_attestation::jsonb->>'schema_version' = '{_V6}'
                       AND NEW.linkage_ack IS NOT NULL
                   ) THEN
                    RAISE EXCEPTION 'PTG_IMPORT_WAVE_LINKAGE_RECEIPT_REQUIRED'
                        USING ERRCODE = 'P0001';
                END IF;
                RETURN NEW;
            END IF;

            receipt := NEW.linkage_receipt::jsonb;
            payload := receipt->'payload';
            expected_cutover_id := encode(
                sha256(convert_to(
                    'ptg-ordinary-cutover-id-v1:' || NEW.wave_id,
                    'UTF8'
                )),
                'hex'
            );
            expected_payload := jsonb_build_object(
                'operation_id', NEW.wave_id,
                'cutover_id', expected_cutover_id,
                'wave_id', NEW.wave_id,
                'wave_digest', NEW.wave_digest,
                'request_digest', NEW.request_digest,
                'cohort_attestation_digest', NEW.cohort_attestation_digest,
                'cohort_signature_digest', NEW.cohort_signature_digest,
                'receipt_public_modulus_hex',
                    NEW.receipt_public_modulus_hex,
                'receipt_public_exponent', NEW.receipt_public_exponent,
                'authorization_digest', NEW.cohort_attestation::jsonb
                    #>>'{{snapshot,authorization_digest}}',
                'snapshot_digest', NEW.cohort_attestation::jsonb
                    #>>'{{snapshot,snapshot_digest}}',
                'membership_digest', NEW.cohort_attestation::jsonb
                    #>>'{{snapshot,membership_digest}}',
                'inventory_digest', NEW.cohort_attestation::jsonb
                    #>>'{{snapshot,inventory_digest}}',
                'subscription_coverage_digest', NEW.cohort_attestation::jsonb
                    #>>'{{snapshot,subscription_coverage_digest}}',
                'entitlement_coverage_digest', NEW.cohort_attestation::jsonb
                    #>>'{{snapshot,entitlement_coverage_digest}}',
                'entitlement_coverage_count',
                    (NEW.cohort_attestation::jsonb
                        #>>'{{snapshot,entitlement_coverage_count}}')::integer,
                'catalog_generation', NEW.cohort_attestation::jsonb
                    #>>'{{snapshot,catalog_generation}}',
                'physical_coordinate_digest', NEW.physical_coordinate_digest,
                'imported_coordinate_digest', NEW.imported_coordinate_digest,
                'reused_coordinate_digest', NEW.reused_coordinate_digest,
                'partition_digest', NEW.partition_digest,
                'physical_coordinate_count', NEW.physical_coordinate_count,
                'imported_coordinate_count', NEW.imported_coordinate_count,
                'reused_coordinate_count', NEW.reused_coordinate_count,
                'intent_count', NEW.intent_count,
                'jobs_digest', NEW.jobs_digest,
                'manifest_digest', NEW.manifest_digest,
                'outcomes_digest', NEW.outcomes_digest,
                'mapping_digest', NEW.linkage_ack::jsonb->>'mapping_digest',
                'linkage_ack_digest', NEW.linkage_ack_digest
            );
            IF NEW.cohort_attestation::jsonb->>'schema_version'
                    IS DISTINCT FROM '{_V6}'
               OR jsonb_typeof(receipt) IS DISTINCT FROM 'object'
               OR (SELECT count(*) FROM jsonb_object_keys(receipt)) <> 6
               OR receipt - ARRAY[
                    'schema', 'key_id', 'issued_at', 'payload',
                    'payload_digest', 'signature'
               ]::text[] <> '{{}}'::jsonb
               OR receipt->>'schema' IS DISTINCT FROM '{_LINKAGE}'
               OR receipt->>'key_id' IS DISTINCT FROM NEW.receipt_key_id
               OR receipt->>'issued_at'
                    !~ '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}T[0-9]{{2}}:[0-9]{{2}}:[0-9]{{2}}\\.[0-9]{{6}}Z$'
               OR receipt->>'payload_digest'
                    IS DISTINCT FROM NEW.linkage_receipt_payload_digest
               OR receipt->>'payload_digest' !~ '^[0-9a-f]{{64}}$'
               OR length(receipt->>'signature') IS DISTINCT FROM 512
               OR receipt->>'signature' !~ '^[0-9a-f]+$'
               OR payload IS DISTINCT FROM expected_payload
               OR {receipt_verifier}(
                    receipt,
                    '{_LINKAGE}',
                    expected_payload,
                    NEW.receipt_key_id,
                    NEW.receipt_public_modulus_hex,
                    NEW.receipt_public_exponent
               ) IS DISTINCT FROM TRUE
               OR to_char(
                    NEW.linkage_receipt_issued_at AT TIME ZONE 'UTC',
                    'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
               ) IS DISTINCT FROM receipt->>'issued_at' THEN
                RAISE EXCEPTION 'PTG_IMPORT_WAVE_LINKAGE_RECEIPT_INVALID'
                    USING ERRCODE = 'P0001';
            END IF;
            RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        f"CREATE TRIGGER {_q('ptg_import_wave_receipt_guard')} "
        f"BEFORE INSERT OR UPDATE ON {wave} FOR EACH ROW "
        f"EXECUTE FUNCTION {function}()"
    )
    op.execute(
        f"ALTER TABLE {wave} ENABLE ALWAYS TRIGGER "
        f"{_q('ptg_import_wave_receipt_guard')}"
    )


def _install_v12_abandonment_guard(schema: str) -> None:
    wave = _qt(schema, "ptg_import_wave")
    quarantine = _qt(schema, "ptg_import_wave_quarantine")
    supersession = _qt(schema, "ptg_import_wave_supersession")
    rollback = _qt(schema, "ptg_import_wave_admission_rollback")
    intent = _qt(schema, "ptg_import_wave_intent")
    claim = _qt(schema, "ptg_import_wave_claim")
    outcome = _qt(schema, "ptg_import_wave_outcome")
    run = _qt(schema, "import_run")
    event = _qt(schema, "ptg_source_attempt_event")
    function = _qt(schema, "ptg_import_wave_v12_abandonment_guard")
    receipt_verifier = _qt(schema, "ptg_wave_is_valid_signed_receipt_v1")
    expected_admission = _expected_admission_sql("predecessor")
    op.execute(
        f"""
        CREATE FUNCTION {function}()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        DECLARE
            predecessor record;
            proof jsonb := NEW.recovery_evidence;
            admission jsonb;
            database_proof jsonb;
            kubernetes_proof jsonb;
            redis_proof jsonb;
            receipt jsonb := NEW.abandonment_receipt;
            intent_count integer;
            run_count integer;
            pristine_run_count integer;
            claim_count integer;
            outcome_count integer;
            worker_start_event_count integer;
            expected_cutover_id text;
            expected_receipt_payload jsonb;
        BEGIN
            PERFORM pg_catalog.pg_advisory_xact_lock(
                pg_catalog.hashtextextended('{_ADMISSION_LOCK}', 0)
            );
            LOCK TABLE {wave}, {intent}, {claim}, {outcome}, {run}, {event},
                {supersession}, {rollback} IN SHARE ROW EXCLUSIVE MODE;
            SELECT * INTO predecessor FROM {wave}
             WHERE wave_id = NEW.predecessor_wave_id FOR UPDATE;
            IF NOT FOUND THEN
                RAISE EXCEPTION 'PTG_IMPORT_WAVE_V12_ABANDONMENT_WAVE_MISSING'
                    USING ERRCODE = 'P0001';
            END IF;
            SELECT count(*), count(admitted.run_id), count(*) FILTER (
                WHERE admitted.engine = 'healthcare-mrf-api'
                  AND admitted.node_id IS NULL
                  AND admitted.importer = 'ptg'
                  AND admitted.family = 'pricing'
                  AND admitted.status = 'queued'
                  AND admitted.phase_detail =
                        'wave admitted; controller materialization pending'
                  AND admitted.params::jsonb = member.params::jsonb
                  AND admitted.idempotency_key = member.run_idempotency_key
                  AND admitted.triggered_by = 'api'
                  AND admitted.schedule_id IS NULL
                  AND admitted.subscription_id IS NULL
                  AND admitted.source_file_import_id
                        = member.source_file_import_id
                  AND admitted.created_at = predecessor.created_at
                  AND admitted.started_at IS NULL
                  AND admitted.finished_at IS NULL
                  AND admitted.heartbeat_at = predecessor.created_at
                  AND admitted.progress::jsonb = jsonb_build_object(
                        'unit', 'run', 'total', 1, 'done', 0, 'pct', 0,
                        'message',
                            'wave admitted; controller materialization pending'
                  )
                  AND admitted.metrics::jsonb = jsonb_build_object(
                        'wave_id', predecessor.wave_id,
                        'queue', predecessor.release_queue,
                        'base_queue', predecessor.queue,
                        'worker_class', predecessor.worker_class,
                        'resource_class', predecessor.resource_class,
                        'worker_limit', predecessor.worker_limit,
                        'job_id', member.job_id,
                        'ordinal', member.ordinal,
                        'wave_digest', predecessor.wave_digest
                  )
                  AND admitted.error IS NULL
                  AND admitted.snapshot_id IS NULL
                  AND admitted.import_id = member.source_file_import_id
                  AND admitted.retry_of_run_id IS NULL
            ) INTO intent_count, run_count, pristine_run_count
              FROM {intent} AS member
              LEFT JOIN {run} AS admitted ON admitted.run_id = member.run_id
             WHERE member.wave_id = predecessor.wave_id;
            SELECT count(*) INTO claim_count FROM {claim}
             WHERE wave_id = predecessor.wave_id;
            SELECT count(*) INTO outcome_count FROM {outcome}
             WHERE wave_id = predecessor.wave_id;
            SELECT count(*) INTO worker_start_event_count
              FROM {event} AS started
              JOIN {intent} AS member ON member.run_id = started.outer_run_id
             WHERE member.wave_id = predecessor.wave_id
               AND started.event_kind = 'worker_start_admitted';
            expected_cutover_id := encode(
                sha256(convert_to(
                    'ptg-ordinary-cutover-id-v1:' || predecessor.wave_id,
                    'UTF8'
                )),
                'hex'
            );
            admission := proof->'admission';
            database_proof := proof->'database';
            kubernetes_proof := proof->'kubernetes';
            redis_proof := proof->'redis';

            IF NEW.reason IS DISTINCT FROM '{_V12_BASIS}'
               OR NEW.recovery_basis IS DISTINCT FROM '{_V12_BASIS}'
               OR NEW.successor_wave_id IS DISTINCT FROM expected_cutover_id
               OR predecessor.state IS DISTINCT FROM 'slots_waiting'
               OR predecessor.uncertainty_resume_state IS NOT NULL
               OR predecessor.cohort_attestation::jsonb->>'schema_version'
                    IS DISTINCT FROM '{_V6}'
               OR predecessor.receipt_key_id IS DISTINCT FROM NEW.receipt_key_id
               OR predecessor.k8s_post_ticket IS NULL
               OR predecessor.k8s_post_started_at IS NULL
               OR predecessor.kubernetes_job_uid IS NULL
               OR predecessor.kubernetes_job_receipt_digest
                    !~ '^[0-9a-f]{{64}}$'
               OR predecessor.kubernetes_job_receipt::jsonb
                    IS DISTINCT FROM jsonb_build_object(
                        'wave_digest', predecessor.wave_digest,
                        'job_uid', predecessor.kubernetes_job_uid,
                        'manifest_identity',
                            predecessor.kubernetes_manifest_identity,
                        'config_identity', predecessor.kubernetes_config_identity,
                        'pinned_image_reference',
                            predecessor.pinned_image_reference,
                        'pinned_image_digest', predecessor.pinned_image_digest,
                        'runtime_image_identity',
                            predecessor.runtime_image_identity
                    )
               OR predecessor.kubernetes_ready_attestation IS NOT NULL
               OR predecessor.kubernetes_ready_attestation_digest IS NOT NULL
               OR predecessor.redis_release_ticket IS NOT NULL
               OR predecessor.redis_release_started_at IS NOT NULL
               OR predecessor.redis_release_attestation IS NOT NULL
               OR predecessor.redis_release_attestation_digest IS NOT NULL
               OR predecessor.failure_receipt IS NOT NULL
               OR predecessor.failure_receipt_digest IS NOT NULL
               OR predecessor.outcomes_digest IS NOT NULL
               OR predecessor.linkage_ack IS NOT NULL
               OR predecessor.linkage_ack_digest IS NOT NULL
               OR predecessor.linkage_receipt IS NOT NULL
               OR predecessor.linkage_receipt_payload_digest IS NOT NULL
               OR predecessor.linkage_receipt_issued_at IS NOT NULL
               OR predecessor.terminal_evidence_digest IS NOT NULL
               OR predecessor.redis_cleanup_ticket IS NOT NULL
               OR predecessor.kubernetes_delete_ticket IS NOT NULL
               OR predecessor.cleanup_evidence_digest IS NOT NULL
               OR predecessor.resolved_at IS NOT NULL
               OR intent_count IS DISTINCT FROM predecessor.intent_count
               OR run_count IS DISTINCT FROM predecessor.intent_count
               OR pristine_run_count IS DISTINCT FROM predecessor.intent_count
               OR claim_count <> 0
               OR outcome_count <> 0
               OR worker_start_event_count <> 0
               OR EXISTS (
                    SELECT 1 FROM {supersession}
                     WHERE predecessor_wave_id = predecessor.wave_id
                        OR successor_wave_id = predecessor.wave_id
               )
               OR EXISTS (
                    SELECT 1 FROM {rollback}
                     WHERE predecessor_wave_id = predecessor.wave_id
                        OR successor_wave_id = predecessor.wave_id
               ) THEN
                RAISE EXCEPTION 'PTG_IMPORT_WAVE_V12_ABANDONMENT_NOT_PRISTINE'
                    USING ERRCODE = 'P0001';
            END IF;

            IF jsonb_typeof(proof) IS DISTINCT FROM 'object'
               OR (SELECT count(*) FROM jsonb_object_keys(proof)) <> 9
               OR proof - ARRAY[
                    'schema_version', 'recovery_basis', 'operation_id',
                    'cutover_id', 'admission', 'database', 'kubernetes',
                    'redis', 'proof_digest'
               ]::text[] <> '{{}}'::jsonb
               OR proof->>'schema_version' IS DISTINCT FROM '{_PROOF}'
               OR proof->>'recovery_basis' IS DISTINCT FROM '{_V12_BASIS}'
               OR proof->>'operation_id' IS DISTINCT FROM predecessor.wave_id
               OR proof->>'cutover_id' IS DISTINCT FROM expected_cutover_id
               OR proof->>'proof_digest'
                    IS DISTINCT FROM NEW.recovery_evidence_sha256
               OR proof->>'proof_digest' !~ '^[0-9a-f]{{64}}$'
               OR admission IS DISTINCT FROM ({expected_admission}) THEN
                RAISE EXCEPTION 'PTG_IMPORT_WAVE_V12_ABANDONMENT_PROOF_INVALID'
                    USING ERRCODE = 'P0001';
            END IF;

            IF jsonb_typeof(database_proof) IS DISTINCT FROM 'object'
               OR (SELECT count(*) FROM jsonb_object_keys(database_proof)) <> 11
               OR database_proof - ARRAY[
                    'state', 'intent_count', 'run_count',
                    'pristine_run_count', 'unassigned_run_count',
                    'claim_count', 'outcome_count',
                    'worker_start_event_count', 'member_rows_digest',
                    'intent_rows_digest', 'run_rows_digest'
               ]::text[] <> '{{}}'::jsonb
               OR database_proof->>'state' IS DISTINCT FROM 'slots_waiting'
               OR database_proof->>'intent_count'
                    IS DISTINCT FROM predecessor.intent_count::text
               OR database_proof->>'run_count'
                    IS DISTINCT FROM predecessor.intent_count::text
               OR database_proof->>'pristine_run_count'
                    IS DISTINCT FROM predecessor.intent_count::text
               OR database_proof->>'unassigned_run_count'
                    IS DISTINCT FROM predecessor.intent_count::text
               OR database_proof->>'claim_count' <> '0'
               OR database_proof->>'outcome_count' <> '0'
               OR database_proof->>'worker_start_event_count' <> '0'
               OR database_proof->>'member_rows_digest'
                    !~ '^[0-9a-f]{{64}}$'
               OR database_proof->>'intent_rows_digest'
                    !~ '^[0-9a-f]{{64}}$'
               OR database_proof->>'run_rows_digest'
                    !~ '^[0-9a-f]{{64}}$' THEN
                RAISE EXCEPTION 'PTG_IMPORT_WAVE_V12_DATABASE_PROOF_INVALID'
                    USING ERRCODE = 'P0001';
            END IF;

            IF kubernetes_proof IS DISTINCT FROM jsonb_build_object(
                    'job_name', 'hpw-ptg-wave-'
                        || left(predecessor.wave_digest, 40),
                    'job_uid', predecessor.kubernetes_job_uid,
                    'job_receipt_digest',
                        predecessor.kubernetes_job_receipt_digest,
                    'completion_mode', 'Indexed',
                    'completions', 12,
                    'parallelism', 12,
                    'backoff_limit', 0,
                    'failed', 12,
                    'active', 0,
                    'succeeded', 0,
                    'ready', 0,
                    'terminating', 0,
                    'failed_condition', true,
                    'complete_condition', false
               )
               OR jsonb_typeof(redis_proof) IS DISTINCT FROM 'object'
               OR (SELECT count(*) FROM jsonb_object_keys(redis_proof)) <> 9
               OR redis_proof->>'unclaimed_attestation_digest'
                    !~ '^[0-9a-f]{{64}}$'
               OR redis_proof - 'unclaimed_attestation_digest'
                    IS DISTINCT FROM jsonb_build_object(
                        'ready_slot_count', 0,
                        'release_present', false,
                        'queued_ordinal_count', 0,
                        'job_ordinal_count', 0,
                        'result_ordinal_count', 0,
                        'retry_ordinal_count', 0,
                        'in_progress_ordinal_count', 0,
                        'health_check_present', false
                    ) THEN
                RAISE EXCEPTION 'PTG_IMPORT_WAVE_V12_EXTERNAL_PROOF_INVALID'
                    USING ERRCODE = 'P0001';
            END IF;

            expected_receipt_payload := jsonb_build_object(
                'operation_id', predecessor.wave_id,
                'cutover_id', expected_cutover_id,
                'wave_id', predecessor.wave_id,
                'wave_digest', predecessor.wave_digest,
                'state', 'abandoned',
                'quarantine_reason', '{_V12_BASIS}',
                'recovery_schema', '{_PROOF}',
                'recovery_evidence_sha256',
                    NEW.recovery_evidence_sha256,
                'admission', admission,
                'database', database_proof,
                'kubernetes', kubernetes_proof,
                'redis', redis_proof
            );
            IF jsonb_typeof(receipt) IS DISTINCT FROM 'object'
               OR (SELECT count(*) FROM jsonb_object_keys(receipt)) <> 6
               OR receipt - ARRAY[
                    'schema', 'key_id', 'issued_at', 'payload',
                    'payload_digest', 'signature'
               ]::text[] <> '{{}}'::jsonb
               OR receipt->>'schema' IS DISTINCT FROM '{_ABANDONMENT}'
               OR receipt->>'key_id' IS DISTINCT FROM NEW.receipt_key_id
               OR receipt->'payload' IS DISTINCT FROM expected_receipt_payload
               OR {receipt_verifier}(
                    receipt,
                    '{_ABANDONMENT}',
                    expected_receipt_payload,
                    predecessor.receipt_key_id,
                    predecessor.receipt_public_modulus_hex,
                    predecessor.receipt_public_exponent
               ) IS DISTINCT FROM TRUE
               OR receipt->>'payload_digest'
                    IS DISTINCT FROM NEW.abandonment_receipt_payload_digest
               OR receipt->>'payload_digest' !~ '^[0-9a-f]{{64}}$'
               OR length(receipt->>'signature') IS DISTINCT FROM 512
               OR receipt->>'signature' !~ '^[0-9a-f]+$'
               OR receipt->>'issued_at'
                    !~ '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}T[0-9]{{2}}:[0-9]{{2}}:[0-9]{{2}}\\.[0-9]{{6}}Z$'
               OR to_char(
                    NEW.abandonment_receipt_issued_at AT TIME ZONE 'UTC',
                    'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
               ) IS DISTINCT FROM receipt->>'issued_at'
               OR NEW.created_at
                    IS DISTINCT FROM NEW.abandonment_receipt_issued_at THEN
                RAISE EXCEPTION 'PTG_IMPORT_WAVE_V12_RECEIPT_INVALID'
                    USING ERRCODE = 'P0001';
            END IF;
            RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        f"CREATE TRIGGER {_q('ptg_import_wave_v12_abandonment_guard')} "
        f"BEFORE INSERT ON {quarantine} FOR EACH ROW WHEN "
        f"(NEW.recovery_basis = '{_V12_BASIS}') EXECUTE FUNCTION {function}()"
    )
    op.execute(
        f"ALTER TABLE {quarantine} ENABLE ALWAYS TRIGGER "
        f"{_q('ptg_import_wave_v12_abandonment_guard')}"
    )


def _install_v12_immutability_guards(schema: str) -> None:
    quarantine = _qt(schema, "ptg_import_wave_quarantine")
    intent = _qt(schema, "ptg_import_wave_intent")
    run = _qt(schema, "import_run")
    event = _qt(schema, "ptg_source_attempt_event")
    wave = _qt(schema, "ptg_import_wave")
    child_function = _qt(schema, "ptg_import_wave_v12_abandoned_child_guard")
    run_function = _qt(schema, "ptg_import_wave_v12_abandoned_run_guard")
    event_function = _qt(schema, "ptg_import_wave_v12_abandoned_event_guard")
    truncate_function = _qt(
        schema,
        "ptg_import_wave_v12_abandoned_truncate_guard",
    )
    op.execute(
        f"""
        CREATE FUNCTION {child_function}()
        RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE
            old_wave_id text;
            new_wave_id text;
        BEGIN
            IF TG_OP <> 'INSERT' THEN old_wave_id := OLD.wave_id; END IF;
            IF TG_OP <> 'DELETE' THEN new_wave_id := NEW.wave_id; END IF;
            IF EXISTS (
                SELECT 1 FROM {quarantine}
                 WHERE recovery_basis = '{_V12_BASIS}'
                   AND predecessor_wave_id IN (old_wave_id, new_wave_id)
            ) THEN
                PERFORM pg_catalog.pg_advisory_xact_lock(
                    pg_catalog.hashtextextended('{_ADMISSION_LOCK}', 0)
                );
                IF EXISTS (
                    SELECT 1 FROM {quarantine}
                     WHERE recovery_basis = '{_V12_BASIS}'
                       AND predecessor_wave_id IN (old_wave_id, new_wave_id)
                ) THEN
                    RAISE EXCEPTION 'PTG_IMPORT_WAVE_V12_ABANDONED_IMMUTABLE'
                        USING ERRCODE = 'P0001';
                END IF;
            END IF;
            IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
            RETURN NEW;
        END;
        $$
        """
    )
    for table_name in (
        "ptg_import_wave_intent",
        "ptg_import_wave_claim",
        "ptg_import_wave_outcome",
    ):
        table = _qt(schema, table_name)
        trigger = _q(f"{table_name}_v12_abandoned_guard")
        op.execute(
            f"CREATE TRIGGER {trigger} BEFORE INSERT OR UPDATE OR DELETE "
            f"ON {table} FOR EACH ROW EXECUTE FUNCTION {child_function}()"
        )
        op.execute(f"ALTER TABLE {table} ENABLE ALWAYS TRIGGER {trigger}")

    op.execute(
        f"""
        CREATE FUNCTION {run_function}()
        RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE
            candidate_run_ids text[] := ARRAY[]::text[];
            candidate_wave_ids text[] := ARRAY[]::text[];
            candidate_wave_digests text[] := ARRAY[]::text[];
        BEGIN
            IF TG_OP <> 'INSERT' THEN
                candidate_run_ids := array_append(candidate_run_ids, OLD.run_id);
                candidate_wave_ids := candidate_wave_ids || ARRAY[
                    OLD.params::jsonb->>'_wave_id',
                    OLD.metrics::jsonb->>'wave_id'
                ];
                candidate_wave_digests := candidate_wave_digests || ARRAY[
                    OLD.params::jsonb->>'_wave_digest',
                    OLD.metrics::jsonb->>'wave_digest'
                ];
            END IF;
            IF TG_OP <> 'DELETE' THEN
                candidate_run_ids := array_append(candidate_run_ids, NEW.run_id);
                candidate_wave_ids := candidate_wave_ids || ARRAY[
                    NEW.params::jsonb->>'_wave_id',
                    NEW.metrics::jsonb->>'wave_id'
                ];
                candidate_wave_digests := candidate_wave_digests || ARRAY[
                    NEW.params::jsonb->>'_wave_digest',
                    NEW.metrics::jsonb->>'wave_digest'
                ];
            END IF;
            IF EXISTS (
                SELECT 1 FROM {quarantine} AS retired
                JOIN {wave} AS predecessor
                  ON predecessor.wave_id = retired.predecessor_wave_id
               WHERE retired.recovery_basis = '{_V12_BASIS}'
                 AND (
                    retired.predecessor_wave_id = ANY(candidate_wave_ids)
                    OR predecessor.wave_digest = ANY(candidate_wave_digests)
                    OR EXISTS (
                        SELECT 1 FROM {intent} AS member
                         WHERE member.wave_id = retired.predecessor_wave_id
                           AND member.run_id = ANY(candidate_run_ids)
                    )
                 )
            ) THEN
                PERFORM pg_catalog.pg_advisory_xact_lock(
                    pg_catalog.hashtextextended('{_ADMISSION_LOCK}', 0)
                );
                RAISE EXCEPTION 'PTG_IMPORT_WAVE_V12_ABANDONED_IMMUTABLE'
                    USING ERRCODE = 'P0001';
            END IF;
            IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
            RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        f"CREATE TRIGGER {_q('ptg_import_wave_v12_abandoned_run_guard')} "
        f"BEFORE INSERT OR UPDATE OR DELETE ON {run} FOR EACH ROW "
        f"EXECUTE FUNCTION {run_function}()"
    )
    op.execute(
        f"ALTER TABLE {run} ENABLE ALWAYS TRIGGER "
        f"{_q('ptg_import_wave_v12_abandoned_run_guard')}"
    )

    op.execute(
        f"""
        CREATE FUNCTION {event_function}()
        RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE
            old_run_id text;
            new_run_id text;
        BEGIN
            IF TG_OP <> 'INSERT' THEN old_run_id := OLD.outer_run_id; END IF;
            IF TG_OP <> 'DELETE' THEN new_run_id := NEW.outer_run_id; END IF;
            IF EXISTS (
                SELECT 1 FROM {intent} AS member
                JOIN {quarantine} AS retired
                  ON retired.predecessor_wave_id = member.wave_id
                 AND retired.recovery_basis = '{_V12_BASIS}'
               WHERE member.run_id IN (old_run_id, new_run_id)
            ) THEN
                PERFORM pg_catalog.pg_advisory_xact_lock(
                    pg_catalog.hashtextextended('{_ADMISSION_LOCK}', 0)
                );
                RAISE EXCEPTION 'PTG_IMPORT_WAVE_V12_ABANDONED_IMMUTABLE'
                    USING ERRCODE = 'P0001';
            END IF;
            IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
            RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        f"CREATE TRIGGER {_q('ptg_import_wave_v12_abandoned_event_guard')} "
        f"BEFORE INSERT OR UPDATE OR DELETE ON {event} FOR EACH ROW "
        f"EXECUTE FUNCTION {event_function}()"
    )
    op.execute(
        f"ALTER TABLE {event} ENABLE ALWAYS TRIGGER "
        f"{_q('ptg_import_wave_v12_abandoned_event_guard')}"
    )

    op.execute(
        f"""
        CREATE FUNCTION {truncate_function}()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM {quarantine}
                 WHERE recovery_basis = '{_V12_BASIS}'
            ) THEN
                RAISE EXCEPTION 'PTG_IMPORT_WAVE_V12_ABANDONED_IMMUTABLE'
                    USING ERRCODE = 'P0001';
            END IF;
            RETURN NULL;
        END;
        $$
        """
    )
    for table_name in (
        "ptg_import_wave_intent",
        "ptg_import_wave_claim",
        "ptg_import_wave_outcome",
        "import_run",
        "ptg_source_attempt_event",
    ):
        table = _qt(schema, table_name)
        trigger = _q(f"{table_name}_v12_abandoned_truncate_guard")
        op.execute(
            f"CREATE TRIGGER {trigger} BEFORE TRUNCATE ON {table} "
            f"FOR EACH STATEMENT EXECUTE FUNCTION {truncate_function}()"
        )
        op.execute(f"ALTER TABLE {table} ENABLE ALWAYS TRIGGER {trigger}")


def _install_ordinary_terminal_receipt_guard(schema: str) -> None:
    """Verify first-write RSA receipts and freeze their exact engine rows."""

    wave = _qt(schema, "ptg_import_wave")
    intent = _qt(schema, "ptg_import_wave_intent")
    quarantine = _qt(schema, "ptg_import_wave_quarantine")
    run = _qt(schema, "import_run")
    engine_run = _qt(schema, "ptg2_import_run")
    engine_snapshot = _qt(schema, "ptg2_snapshot")
    receipt_table = _qt(
        schema, "ptg_import_wave_ordinary_terminal_receipt"
    )
    verifier = _qt(schema, "ptg_wave_is_valid_signed_receipt_v1")
    receipt_guard = _qt(
        schema, "ptg_wave_ordinary_terminal_receipt_guard"
    )
    run_guard = _qt(
        schema, "ptg_wave_ordinary_terminal_run_immutable_guard"
    )
    engine_guard = _qt(
        schema, "ptg_wave_ordinary_terminal_engine_immutable_guard"
    )
    truncate_guard = _qt(
        schema, "ptg_wave_ordinary_terminal_truncate_guard"
    )

    coordinate_digest = _domain_json_digest_sql(
        schema, _COORDINATE_DOMAIN, "coordinate"
    )
    scope_digest = _domain_json_digest_sql(
        schema, _SCOPE_DOMAIN, "scope_payload"
    )
    params_digest = _domain_json_digest_sql(
        schema, _RUN_PARAMS_DOMAIN, "run_params"
    )
    metrics_digest = _domain_json_digest_sql(
        schema, _RUN_METRICS_DOMAIN, "run_metrics"
    )
    options_digest = _domain_json_digest_sql(
        schema, _ENGINE_OPTIONS_DOMAIN, "engine_options"
    )
    report_digest = _domain_json_digest_sql(
        schema, _ENGINE_REPORT_DOMAIN, "engine_report"
    )
    manifest_digest = _domain_json_digest_sql(
        schema, _SNAPSHOT_MANIFEST_DOMAIN, "snapshot_manifest"
    )
    terminal_digest = _domain_json_digest_sql(
        schema, _TERMINAL_RESULT_DOMAIN, "terminal_result"
    )

    op.execute(
        f"""
        CREATE FUNCTION {receipt_guard}()
        RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE
            predecessor record;
            member record;
            retired record;
            ordinary_run record;
            durable_run record;
            durable_snapshot record;
            direct_input jsonb;
            run_params jsonb;
            run_metrics jsonb;
            engine_options jsonb;
            engine_report jsonb;
            snapshot_manifest jsonb;
            coordinate jsonb;
            scope_payload jsonb;
            terminal_result jsonb;
            expected_payload jsonb;
            expected_cutover_id text;
            expected_source_key text;
            expected_import_month text;
            expected_snapshot_id text;
            expected_engine_run_id text;
            expected_selector_name text;
            expected_selector_value text;
            expected_finished_at text;
            expected_issued_at text;
        BEGIN
            IF TG_OP <> 'INSERT' THEN
                RAISE EXCEPTION 'PTG_WAVE_ORDINARY_TERMINAL_IMMUTABLE'
                    USING ERRCODE = 'P0001';
            END IF;

            SELECT * INTO predecessor FROM {wave}
             WHERE wave_id = NEW.wave_id;
            IF NOT FOUND THEN
                RAISE EXCEPTION 'PTG_WAVE_ORDINARY_TERMINAL_WAVE_MISSING'
                    USING ERRCODE = 'P0001';
            END IF;
            SELECT * INTO member FROM {intent}
             WHERE wave_id = NEW.wave_id
               AND ordinal = NEW.member_ordinal;
            IF NOT FOUND THEN
                RAISE EXCEPTION 'PTG_WAVE_ORDINARY_TERMINAL_MEMBER_MISSING'
                    USING ERRCODE = 'P0001';
            END IF;
            SELECT * INTO retired FROM {quarantine}
             WHERE predecessor_wave_id = NEW.wave_id;
            IF NOT FOUND THEN
                RAISE EXCEPTION 'PTG_WAVE_ORDINARY_TERMINAL_NOT_ABANDONED'
                    USING ERRCODE = 'P0001';
            END IF;
            SELECT * INTO ordinary_run FROM {run}
             WHERE run_id = NEW.run_id
             FOR UPDATE;
            IF NOT FOUND THEN
                RAISE EXCEPTION 'PTG_WAVE_ORDINARY_TERMINAL_RUN_MISSING'
                    USING ERRCODE = 'P0001';
            END IF;

            direct_input := member.params::jsonb
                ->'direct_rate_file_intent';
            run_params := ordinary_run.params::jsonb;
            run_metrics := ordinary_run.metrics::jsonb;
            expected_cutover_id := encode(
                sha256(convert_to(
                    'ptg-ordinary-cutover-id-v1:' || predecessor.wave_id,
                    'UTF8'
                )),
                'hex'
            );
            expected_source_key := direct_input->>'source_key';
            expected_import_month := member.params::jsonb->>'import_month';
            expected_snapshot_id := run_metrics->>'snapshot_id';
            expected_engine_run_id := run_metrics->>'import_run_id';
            expected_selector_name := CASE direct_input->>'source_type'
                WHEN 'allowed_amounts' THEN 'allowed_url'
                WHEN 'in_network' THEN 'in_network_url'
                ELSE NULL
            END;
            expected_selector_value := direct_input->>'canonical_url';

            IF predecessor.cohort_attestation::jsonb->>'schema_version'
                    IS DISTINCT FROM '{_V6}'
               OR predecessor.receipt_key_id
                    IS DISTINCT FROM NEW.receipt_key_id
               OR predecessor.receipt_key_id IS NULL
               OR retired.reason IS DISTINCT FROM '{_V12_BASIS}'
               OR retired.recovery_basis IS DISTINCT FROM '{_V12_BASIS}'
               OR retired.successor_wave_id IS DISTINCT FROM expected_cutover_id
               OR retired.receipt_key_id
                    IS DISTINCT FROM predecessor.receipt_key_id
               OR retired.abandonment_receipt IS NULL
               OR retired.abandonment_receipt_payload_digest IS NULL
               OR retired.recovery_evidence_sha256 IS NULL
               OR jsonb_typeof(direct_input) IS DISTINCT FROM 'object'
               OR direct_input->>'source_file_import_id'
                    IS DISTINCT FROM member.source_file_import_id
               OR direct_input->>'content_version'
                    IS DISTINCT FROM member.content_version
               OR expected_source_key IS NULL
               OR expected_import_month !~ '^[0-9]{{4}}-[0-9]{{2}}$'
               OR expected_selector_name IS NULL
               OR expected_selector_value IS NULL
               OR jsonb_typeof(run_params) IS DISTINCT FROM 'object'
               OR jsonb_typeof(run_metrics) IS DISTINCT FROM 'object'
               OR ordinary_run.run_id IS NOT DISTINCT FROM member.run_id
               OR NEW.source_file_import_id
                    IS NOT DISTINCT FROM member.source_file_import_id
               OR ordinary_run.engine IS DISTINCT FROM 'healthcare-mrf-api'
               OR ordinary_run.importer IS DISTINCT FROM 'ptg'
               OR ordinary_run.status IS DISTINCT FROM 'succeeded'
               OR ordinary_run.node_id IS DISTINCT FROM
                    member.params::jsonb->>'node_id'
               OR ordinary_run.source_file_import_id
                    IS DISTINCT FROM NEW.source_file_import_id
               OR ordinary_run.import_id
                    IS DISTINCT FROM NEW.source_file_import_id
               OR ordinary_run.error IS NOT NULL
               OR ordinary_run.finished_at IS NULL
               OR run_params->>'source_file_import_id'
                    IS DISTINCT FROM NEW.source_file_import_id
               OR run_params->>'import_id'
                    IS DISTINCT FROM NEW.source_file_import_id
               OR run_params->>'ordinary_cutover_operation_id'
                    IS DISTINCT FROM NEW.wave_id
               OR run_params->>'ordinary_cutover_id'
                    IS DISTINCT FROM expected_cutover_id
               OR run_params->'ordinary_cutover_member_ordinal'
                    IS DISTINCT FROM to_jsonb(NEW.member_ordinal)
               OR run_params->>'ordinary_cutover_direct_input_digest'
                    IS DISTINCT FROM member.params::jsonb
                        ->>'direct_rate_file_intent_sha256'
               OR run_params->>'source_key'
                    IS DISTINCT FROM expected_source_key
               OR run_params->>'import_month'
                    IS DISTINCT FROM expected_import_month
               OR run_params->>expected_selector_name
                    IS DISTINCT FROM expected_selector_value
               OR run_params ? (CASE expected_selector_name
                     WHEN 'allowed_url' THEN 'in_network_url'
                     ELSE 'allowed_url'
                   END)
               OR run_params->'max_files' IS DISTINCT FROM '1'::jsonb
               OR jsonb_typeof(run_params->'plan_ids')
                    IS DISTINCT FROM 'array'
               OR jsonb_array_length(run_params->'plan_ids') < 1
               OR run_params->'plan_market_types'
                    IS DISTINCT FROM '["group"]'::jsonb
               OR run_metrics->>'status' IS DISTINCT FROM 'succeeded'
               OR run_metrics->>'source_key'
                    IS DISTINCT FROM expected_source_key
               OR left(run_metrics->>'import_month', 7)
                    IS DISTINCT FROM expected_import_month
               OR expected_snapshot_id IS NULL
               OR length(expected_snapshot_id) NOT BETWEEN 1 AND 96
               OR expected_engine_run_id IS NULL
               OR length(expected_engine_run_id) NOT BETWEEN 1 AND 96
               OR ordinary_run.snapshot_id IS NOT NULL
                    AND ordinary_run.snapshot_id
                        IS DISTINCT FROM expected_snapshot_id THEN
                RAISE EXCEPTION 'PTG_WAVE_ORDINARY_TERMINAL_BINDING_INVALID'
                    USING ERRCODE = 'P0001';
            END IF;

            SELECT * INTO durable_run FROM {engine_run}
             WHERE import_run_id = expected_engine_run_id
             FOR UPDATE;
            IF NOT FOUND THEN
                RAISE EXCEPTION 'PTG_WAVE_ORDINARY_TERMINAL_RESULT_MISSING'
                    USING ERRCODE = 'P0001';
            END IF;
            SELECT * INTO durable_snapshot FROM {engine_snapshot}
             WHERE snapshot_id = expected_snapshot_id
             FOR UPDATE;
            IF NOT FOUND THEN
                RAISE EXCEPTION 'PTG_WAVE_ORDINARY_TERMINAL_SNAPSHOT_MISSING'
                    USING ERRCODE = 'P0001';
            END IF;
            engine_options := durable_run.options::jsonb;
            engine_report := durable_run.report::jsonb;
            snapshot_manifest := durable_snapshot.manifest::jsonb;
            IF durable_run.status IS DISTINCT FROM 'validated'
               OR durable_run.finished_at IS NULL
               OR durable_run.error IS NOT NULL
               OR to_char(durable_run.import_month, 'YYYY-MM')
                    IS DISTINCT FROM expected_import_month
               OR jsonb_typeof(engine_options) IS DISTINCT FROM 'object'
               OR jsonb_typeof(engine_report) IS DISTINCT FROM 'object'
               OR jsonb_typeof(snapshot_manifest) IS DISTINCT FROM 'object'
               OR engine_options->>'source_key'
                    IS DISTINCT FROM expected_source_key
               OR engine_options->'plan_ids'
                    IS DISTINCT FROM run_params->'plan_ids'
               OR engine_options->'plan_market_types'
                    IS DISTINCT FROM run_params->'plan_market_types'
               OR engine_report->>'snapshot_id'
                    IS DISTINCT FROM expected_snapshot_id
               OR durable_snapshot.import_run_id
                    IS DISTINCT FROM expected_engine_run_id
               OR to_char(durable_snapshot.import_month, 'YYYY-MM')
                    IS DISTINCT FROM expected_import_month
               OR durable_snapshot.status NOT IN ('validated', 'published')
               OR run_metrics->>'snapshot_status'
                    IS DISTINCT FROM durable_snapshot.status THEN
                RAISE EXCEPTION 'PTG_WAVE_ORDINARY_TERMINAL_RESULT_INVALID'
                    USING ERRCODE = 'P0001';
            END IF;

            coordinate := jsonb_build_object(
                'source_file_id', direct_input->>'source_file_id',
                'content_version', direct_input->>'content_version',
                'import_month', expected_import_month,
                'historical_source_file_import_id',
                    member.source_file_import_id,
                'direct_input_digest', member.params::jsonb
                    ->>'direct_rate_file_intent_sha256'
            );
            scope_payload := jsonb_build_object(
                'plan_ids', run_params->'plan_ids',
                'plan_market_types', run_params->'plan_market_types',
                'admission_plan_ids', member.params::jsonb->'plan_ids',
                'admission_plan_market_types',
                    member.params::jsonb->'plan_market_types',
                'authorization_digest', predecessor.cohort_attestation::jsonb
                    #>> '{{snapshot,authorization_digest}}',
                'membership_digest', predecessor.cohort_attestation::jsonb
                    #>> '{{snapshot,membership_digest}}',
                'subscription_coverage_digest',
                    predecessor.cohort_attestation::jsonb
                        #>> '{{snapshot,subscription_coverage_digest}}',
                'entitlement_coverage_digest',
                    predecessor.cohort_attestation::jsonb
                        #>> '{{snapshot,entitlement_coverage_digest}}',
                'entitlement_coverage_count',
                    (predecessor.cohort_attestation::jsonb
                        #>> '{{snapshot,entitlement_coverage_count}}')::integer
            );
            expected_finished_at := to_char(
                ordinary_run.finished_at AT TIME ZONE 'UTC',
                'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
            );
            terminal_result := jsonb_build_object(
                'engine', 'healthcare-mrf-api',
                'importer', 'ptg',
                'status', 'succeeded',
                'engine_result_status', 'validated',
                'source_file_import_id', NEW.source_file_import_id,
                'run_id', NEW.run_id,
                'node_id', ordinary_run.node_id,
                'source_key', expected_source_key,
                'snapshot_id', expected_snapshot_id,
                'engine_import_run_id', expected_engine_run_id,
                'import_month', expected_import_month,
                'finished_at', expected_finished_at,
                'run_params_digest', {params_digest},
                'run_metrics_digest', {metrics_digest},
                'engine_options_digest', {options_digest},
                'engine_report_digest', {report_digest},
                'snapshot_manifest_digest', {manifest_digest}
            );
            expected_payload := jsonb_build_object(
                'operation_id', predecessor.wave_id,
                'cutover_id', expected_cutover_id,
                'wave_id', predecessor.wave_id,
                'wave_digest', predecessor.wave_digest,
                'member_ordinal', NEW.member_ordinal,
                'source_file_import_id', NEW.source_file_import_id,
                'run_id', NEW.run_id,
                'node_id', ordinary_run.node_id,
                'source_key', expected_source_key,
                'snapshot_id', expected_snapshot_id,
                'coordinate', coordinate,
                'coordinate_digest', {coordinate_digest},
                'scope', scope_payload,
                'scope_digest', {scope_digest},
                'terminal_result', terminal_result,
                'terminal_result_digest', {terminal_digest},
                'abandonment_receipt_payload_digest',
                    retired.abandonment_receipt_payload_digest,
                'recovery_evidence_sha256',
                    retired.recovery_evidence_sha256
            );
            expected_issued_at := to_char(
                NEW.issued_at AT TIME ZONE 'UTC',
                'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
            );
            IF NEW.created_at IS DISTINCT FROM NEW.issued_at
               OR NEW.receipt->>'issued_at'
                    IS DISTINCT FROM expected_issued_at
               OR NEW.payload_digest
                    IS DISTINCT FROM NEW.receipt->>'payload_digest'
               OR {verifier}(
                    NEW.receipt,
                    '{_ORDINARY_TERMINAL}',
                    expected_payload,
                    predecessor.receipt_key_id,
                    predecessor.receipt_public_modulus_hex,
                    predecessor.receipt_public_exponent
               ) IS DISTINCT FROM true THEN
                RAISE EXCEPTION 'PTG_WAVE_ORDINARY_TERMINAL_RECEIPT_INVALID'
                    USING ERRCODE = 'P0001';
            END IF;
            RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        f"CREATE TRIGGER {_q('ptg_wave_ordinary_terminal_receipt_guard')} "
        f"BEFORE INSERT OR UPDATE OR DELETE ON {receipt_table} FOR EACH ROW "
        f"EXECUTE FUNCTION {receipt_guard}()"
    )
    op.execute(
        f"ALTER TABLE {receipt_table} ENABLE ALWAYS TRIGGER "
        f"{_q('ptg_wave_ordinary_terminal_receipt_guard')}"
    )

    op.execute(
        f"""
        CREATE FUNCTION {run_guard}()
        RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE
            candidate_run_id text;
        BEGIN
            candidate_run_id := CASE WHEN TG_OP = 'INSERT'
                THEN NEW.run_id ELSE OLD.run_id END;
            IF EXISTS (
                SELECT 1 FROM {receipt_table}
                 WHERE run_id = candidate_run_id
            ) THEN
                RAISE EXCEPTION 'PTG_WAVE_ORDINARY_TERMINAL_RUN_IMMUTABLE'
                    USING ERRCODE = 'P0001';
            END IF;
            IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
            RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        f"CREATE TRIGGER {_q('ptg_wave_ordinary_terminal_run_guard')} "
        f"BEFORE UPDATE OR DELETE ON {run} FOR EACH ROW "
        f"EXECUTE FUNCTION {run_guard}()"
    )
    op.execute(
        f"ALTER TABLE {run} ENABLE ALWAYS TRIGGER "
        f"{_q('ptg_wave_ordinary_terminal_run_guard')}"
    )

    op.execute(
        f"""
        CREATE FUNCTION {engine_guard}()
        RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE
            candidate_identity text;
            candidate_row jsonb;
        BEGIN
            candidate_row := CASE WHEN TG_OP = 'DELETE'
                THEN to_jsonb(OLD) ELSE to_jsonb(NEW) END;
            candidate_identity := candidate_row ->> CASE
                WHEN TG_TABLE_NAME = 'ptg2_import_run' THEN 'import_run_id'
                ELSE 'snapshot_id'
            END;
            IF EXISTS (
                SELECT 1 FROM {receipt_table} AS terminal
                 WHERE CASE WHEN TG_TABLE_NAME = 'ptg2_import_run'
                    THEN terminal.receipt
                        #>>'{{payload,terminal_result,engine_import_run_id}}'
                    ELSE terminal.receipt#>>'{{payload,snapshot_id}}'
                 END = candidate_identity
            ) THEN
                RAISE EXCEPTION 'PTG_WAVE_ORDINARY_TERMINAL_RESULT_IMMUTABLE'
                    USING ERRCODE = 'P0001';
            END IF;
            IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
            RETURN NEW;
        END;
        $$
        """
    )
    for table_name in ("ptg2_import_run", "ptg2_snapshot"):
        table = _qt(schema, table_name)
        trigger_name = f"{table_name}_ordinary_terminal_guard"
        op.execute(
            f"CREATE TRIGGER {_q(trigger_name)} BEFORE UPDATE OR DELETE "
            f"ON {table} FOR EACH ROW EXECUTE FUNCTION {engine_guard}()"
        )
        op.execute(
            f"ALTER TABLE {table} ENABLE ALWAYS TRIGGER {_q(trigger_name)}"
        )

    op.execute(
        f"""
        CREATE FUNCTION {truncate_guard}()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            IF EXISTS (SELECT 1 FROM {receipt_table}) THEN
                RAISE EXCEPTION 'PTG_WAVE_ORDINARY_TERMINAL_TRUNCATE_BLOCKED'
                    USING ERRCODE = 'P0001';
            END IF;
            RETURN NULL;
        END;
        $$
        """
    )
    for table_name in (
        "ptg_import_wave_ordinary_terminal_receipt",
        "ptg2_import_run",
        "ptg2_snapshot",
    ):
        table = _qt(schema, table_name)
        trigger_name = f"{table_name}_ordinary_terminal_truncate_guard"
        op.execute(
            f"CREATE TRIGGER {_q(trigger_name)} BEFORE TRUNCATE ON {table} "
            f"FOR EACH STATEMENT EXECUTE FUNCTION {truncate_guard}()"
        )
        op.execute(
            f"ALTER TABLE {table} ENABLE ALWAYS TRIGGER {_q(trigger_name)}"
        )


def downgrade() -> None:
    """Remove the receipt shape only when no v6 authority was used."""

    schema = _schema()
    wave = _qt(schema, "ptg_import_wave")
    quarantine = _qt(schema, "ptg_import_wave_quarantine")
    ordinary_terminal = _qt(
        schema, "ptg_import_wave_ordinary_terminal_receipt"
    )
    engine_run = _qt(schema, "ptg2_import_run")
    engine_snapshot = _qt(schema, "ptg2_snapshot")
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute(
        f"LOCK TABLE {wave}, {quarantine}, {ordinary_terminal}, "
        f"{engine_run}, {engine_snapshot} IN ACCESS EXCLUSIVE MODE"
    )
    op.execute(
        f"""
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM {wave}
                 WHERE receipt_key_id IS NOT NULL
                    OR linkage_receipt IS NOT NULL
            ) OR EXISTS (
                SELECT 1 FROM {quarantine}
                 WHERE recovery_basis = '{_V12_BASIS}'
                    OR receipt_key_id IS NOT NULL
                    OR abandonment_receipt IS NOT NULL
            ) OR EXISTS (
                SELECT 1 FROM {ordinary_terminal}
            ) THEN
                RAISE EXCEPTION 'PTG_WAVE_RECEIPT_AUTHORITY_DOWNGRADE_BLOCKED'
                    USING ERRCODE = '55000';
            END IF;
        END;
        $$
        """
    )
    _replace_effective_owner_function(schema=schema, include_v12=False)
    for table_name in (
        "ptg_import_wave_ordinary_terminal_receipt",
        "ptg2_import_run",
        "ptg2_snapshot",
    ):
        op.execute(
            f"DROP TRIGGER "
            f"{_q(f'{table_name}_ordinary_terminal_truncate_guard')} "
            f"ON {_qt(schema, table_name)}"
        )
    op.execute(
        f"DROP FUNCTION "
        f"{_qt(schema, 'ptg_wave_ordinary_terminal_truncate_guard')}()"
    )
    for table_name in ("ptg2_import_run", "ptg2_snapshot"):
        op.execute(
            f"DROP TRIGGER {_q(f'{table_name}_ordinary_terminal_guard')} "
            f"ON {_qt(schema, table_name)}"
        )
    op.execute(
        f"DROP FUNCTION "
        f"{_qt(schema, 'ptg_wave_ordinary_terminal_engine_immutable_guard')}()"
    )
    op.execute(
        f"DROP TRIGGER {_q('ptg_wave_ordinary_terminal_run_guard')} "
        f"ON {_qt(schema, 'import_run')}"
    )
    op.execute(
        f"DROP FUNCTION "
        f"{_qt(schema, 'ptg_wave_ordinary_terminal_run_immutable_guard')}()"
    )
    op.execute(
        f"DROP TRIGGER {_q('ptg_wave_ordinary_terminal_receipt_guard')} "
        f"ON {ordinary_terminal}"
    )
    op.execute(
        f"DROP FUNCTION "
        f"{_qt(schema, 'ptg_wave_ordinary_terminal_receipt_guard')}()"
    )
    op.execute(f"DROP TABLE {ordinary_terminal}")
    for table_name in (
        "ptg_import_wave_intent",
        "ptg_import_wave_claim",
        "ptg_import_wave_outcome",
        "import_run",
        "ptg_source_attempt_event",
    ):
        op.execute(
            f"DROP TRIGGER {_q(f'{table_name}_v12_abandoned_truncate_guard')} "
            f"ON {_qt(schema, table_name)}"
        )
    op.execute(
        f"DROP FUNCTION "
        f"{_qt(schema, 'ptg_import_wave_v12_abandoned_truncate_guard')}()"
    )
    for table_name in (
        "ptg_import_wave_intent",
        "ptg_import_wave_claim",
        "ptg_import_wave_outcome",
    ):
        op.execute(
            f"DROP TRIGGER {_q(f'{table_name}_v12_abandoned_guard')} "
            f"ON {_qt(schema, table_name)}"
        )
    op.execute(
        f"DROP TRIGGER {_q('ptg_import_wave_v12_abandoned_run_guard')} "
        f"ON {_qt(schema, 'import_run')}"
    )
    op.execute(
        f"DROP TRIGGER {_q('ptg_import_wave_v12_abandoned_event_guard')} "
        f"ON {_qt(schema, 'ptg_source_attempt_event')}"
    )
    for function_name in (
        "ptg_import_wave_v12_abandoned_child_guard",
        "ptg_import_wave_v12_abandoned_run_guard",
        "ptg_import_wave_v12_abandoned_event_guard",
    ):
        op.execute(f"DROP FUNCTION {_qt(schema, function_name)}()")
    op.execute(
        f"DROP TRIGGER {_q('ptg_import_wave_v12_abandonment_guard')} "
        f"ON {quarantine}"
    )
    op.execute(
        f"DROP FUNCTION {_qt(schema, 'ptg_import_wave_v12_abandonment_guard')}()"
    )
    op.execute(
        f"DROP TRIGGER {_q('ptg_import_wave_receipt_guard')} ON {wave}"
    )
    op.execute(
        f"DROP FUNCTION {_qt(schema, 'ptg_import_wave_receipt_guard')}()"
    )
    op.execute(
        f"DROP FUNCTION {_qt(schema, 'ptg_wave_is_valid_signed_receipt_v1')}"
        "(jsonb, text, jsonb, text, text, integer)"
    )
    op.execute(
        f"DROP FUNCTION {_qt(schema, 'ptg_wave_rsa2048_pkcs1_sha256_verify_v1')}"
        "(text, text, integer, bytea)"
    )
    op.execute(
        f"DROP FUNCTION {_qt(schema, 'ptg_wave_hex_to_numeric_v1')}(text)"
    )
    op.execute(
        f"DROP FUNCTION {_qt(schema, 'ptg_wave_canonical_json_ascii_v1')}(jsonb)"
    )
    op.execute(
        f"DROP FUNCTION {_qt(schema, 'ptg_wave_json_ascii_text_v1')}(text)"
    )
    for constraint_name in (
        "ptg_import_wave_quarantine_receipt_check",
        "ptg_import_wave_quarantine_abandonment_evidence_check",
    ):
        op.execute(
            f"ALTER TABLE {quarantine} DROP CONSTRAINT {_q(constraint_name)}"
        )
    op.execute(
        f"ALTER TABLE {quarantine} DROP CONSTRAINT "
        f"{_q('ptg_import_wave_quarantine_reason_check')}"
    )
    op.execute(
        f"ALTER TABLE {quarantine} ADD CONSTRAINT "
        f"{_q('ptg_import_wave_quarantine_reason_check')} CHECK (reason IN ("
        "'legacy_uncertain_slots_waiting_pre_receipt', "
        f"'{_LEGACY_BASIS}'))"
    )
    op.execute(
        f"""
        ALTER TABLE {quarantine} ADD CONSTRAINT
            {_q('ptg_import_wave_quarantine_abandonment_evidence_check')}
        CHECK (
            (
                recovery_basis IS NULL
                AND successor_wave_id IS NULL
                AND recovery_evidence IS NULL
                AND recovery_evidence_canonical IS NULL
                AND recovery_evidence_sha256 IS NULL
            ) OR (
                reason = '{_LEGACY_BASIS}'
                AND recovery_basis = '{_LEGACY_BASIS}'
                AND successor_wave_id IS NOT NULL
                AND successor_wave_id <> predecessor_wave_id
                AND jsonb_typeof(recovery_evidence) = 'object'
                AND recovery_evidence_sha256 ~ '^[0-9a-f]{{64}}$'
                AND octet_length(recovery_evidence_canonical) > 0
                AND encode(sha256(recovery_evidence_canonical), 'hex')
                    = recovery_evidence_sha256
                AND convert_from(recovery_evidence_canonical, 'UTF8')::jsonb
                    = recovery_evidence - 'proof_digest'
            )
        )
        """
    )
    for constraint_name in (
        "ptg_import_wave_v6_linkage_receipt_required_check",
        "ptg_import_wave_linkage_receipt_check",
        "ptg_import_wave_receipt_key_epoch_check",
    ):
        op.execute(
            f"ALTER TABLE {wave} DROP CONSTRAINT {_q(constraint_name)}"
        )
    for column_name in (
        "abandonment_receipt_issued_at",
        "abandonment_receipt_payload_digest",
        "abandonment_receipt",
        "receipt_key_id",
    ):
        op.execute(
            f"ALTER TABLE {quarantine} DROP COLUMN {_q(column_name)}"
        )
    for column_name in (
        "linkage_receipt_issued_at",
        "linkage_receipt_payload_digest",
        "linkage_receipt",
        "receipt_public_exponent",
        "receipt_public_modulus_hex",
        "receipt_key_id",
    ):
        op.execute(f"ALTER TABLE {wave} DROP COLUMN {_q(column_name)}")
