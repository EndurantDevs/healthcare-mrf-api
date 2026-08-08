# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Add immutable, publication-disabled public-evidence reference roots.

Revision ID: 20260808100000_public_evidence_reference_roots
Revises: 20260808090000_public_evidence_storage_foundation
"""

from __future__ import annotations

import os

from alembic import op


revision = "20260808100000_public_evidence_reference_roots"
down_revision = "20260808090000_public_evidence_storage_foundation"
branch_labels = None
depends_on = None


_SOURCE_RECORD_KINDS_BY_SOURCE = {
    "tic": ("tic_provider_group_occurrence",),
    "public_provider_directory_fhir": (
        "fhir_insurance_plan",
        "fhir_location",
        "fhir_network",
        "fhir_npi_resource",
        "fhir_organization",
        "fhir_practitioner_role",
    ),
    "nppes_entity_address": ("nppes_registry_record",),
    "public_hpt": ("hpt_hospital_record",),
}

_SOURCE_ENTITY_KIND_BY_SOURCE = {
    "public_hpt": "hpt_hospital_entity",
    "public_provider_directory_fhir": "fhir_organization",
}


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


def _source_record_matrix() -> str:
    clauses = []
    for source_kind, record_kinds in _SOURCE_RECORD_KINDS_BY_SOURCE.items():
        allowed = ",".join(_literal(record_kind) for record_kind in record_kinds)
        clauses.append(
            f"(source_kind = {_literal(source_kind)} "
            f"AND record_kind IN ({allowed}))"
        )
    return " OR ".join(clauses)


def _source_entity_matrix() -> str:
    return " OR ".join(
        f"(source_kind = {_literal(source_kind)} "
        f"AND entity_kind = {_literal(entity_kind)})"
        for source_kind, entity_kind in _SOURCE_ENTITY_KIND_BY_SOURCE.items()
    )


def _create_reference_function(
    schema: str,
    *,
    function_name: str,
    arguments: str,
    prefix: str,
    purpose: str,
    payload_sql: str,
) -> None:
    function = _qf(schema, function_name)
    op.execute(
        f"""
        CREATE FUNCTION {function}({arguments})
        RETURNS text
        LANGUAGE sql
        IMMUTABLE
        STRICT
        PARALLEL SAFE
        SET search_path = pg_catalog
        AS $function$
            WITH payload(value) AS (
                SELECT {payload_sql}
            )
            SELECT {_literal(prefix)} || translate(
                rtrim(
                    encode(
                        sha256(
                            convert_to(
                                'HEALTHPORTA_PUBLIC_EVIDENCE_RECORD_REFERENCE_V1',
                                'UTF8'
                            ) || decode('00', 'hex') ||
                            int2send(
                                octet_length(
                                    convert_to({_literal(purpose)}, 'UTF8')
                                )::smallint
                            ) ||
                            convert_to({_literal(purpose)}, 'UTF8') ||
                            int8send(
                                octet_length(convert_to(payload.value, 'UTF8'))
                                ::bigint
                            ) ||
                            convert_to(payload.value, 'UTF8')
                        ),
                        'base64'
                    ),
                    '='
                ),
                '+/',
                '-_'
            )
            FROM payload;
        $function$;
        """
    )


def _create_reference_functions(schema: str) -> None:
    _create_reference_function(
        schema,
        function_name="public_evidence_source_record_ref",
        arguments=(
            "candidate_source_release_ref text, candidate_record_kind text, "
            "candidate_identity_contract_id text, "
            "candidate_record_hmac_sha256 bytea, candidate_payload_sha256 bytea"
        ),
        prefix="pesr1_",
        purpose="source_record",
        payload_sql=(
            "'{\"identity_contract_id\":' || "
            "to_json(candidate_identity_contract_id)::text || "
            "',\"payload_sha256\":' || "
            "to_json(encode(candidate_payload_sha256, 'hex'))::text || "
            "',\"record_hmac_sha256\":' || "
            "to_json(encode(candidate_record_hmac_sha256, 'hex'))::text || "
            "',\"record_kind\":' || to_json(candidate_record_kind)::text || "
            "',\"source_release_ref\":' || "
            "to_json(candidate_source_release_ref)::text || '}'"
        ),
    )
    _create_reference_function(
        schema,
        function_name="public_evidence_source_entity_ref",
        arguments=(
            "candidate_source_release_ref text, candidate_entity_kind text, "
            "candidate_identity_contract_id text, candidate_identity_sha256 bytea"
        ),
        prefix="peent1_",
        purpose="source_entity",
        payload_sql=(
            "'{\"entity_kind\":' || to_json(candidate_entity_kind)::text || "
            "',\"identity_contract_id\":' || "
            "to_json(candidate_identity_contract_id)::text || "
            "',\"identity_sha256\":' || "
            "to_json(encode(candidate_identity_sha256, 'hex'))::text || "
            "',\"source_release_ref\":' || "
            "to_json(candidate_source_release_ref)::text || '}'"
        ),
    )
    _create_reference_function(
        schema,
        function_name="public_evidence_provider_group_ref",
        arguments=(
            "candidate_source_release_ref text, "
            "candidate_identity_contract_id text, candidate_identity_sha256 bytea"
        ),
        prefix="pegrp1_",
        purpose="provider_group",
        payload_sql=(
            "'{\"identity_contract_id\":' || "
            "to_json(candidate_identity_contract_id)::text || "
            "',\"identity_sha256\":' || "
            "to_json(encode(candidate_identity_sha256, 'hex'))::text || "
            "',\"source_release_ref\":' || "
            "to_json(candidate_source_release_ref)::text || '}'"
        ),
    )


def _create_source_record_table(schema: str) -> None:
    table = _qt(schema, "public_evidence_source_record")
    release = _qt(schema, "public_evidence_source_release")
    ref_function = _qf(schema, "public_evidence_source_record_ref")
    op.execute(
        f"""
        CREATE TABLE {table} (
            source_record_ref varchar(49) NOT NULL,
            source_release_ref varchar(50) NOT NULL,
            source_release_contract_sha256 bytea NOT NULL,
            source_kind varchar(48) NOT NULL,
            record_kind varchar(64) NOT NULL,
            identity_contract_id text NOT NULL,
            record_hmac_sha256 bytea NOT NULL,
            payload_sha256 bytea NOT NULL,
            created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
            CONSTRAINT {_q('public_evidence_source_record_pkey')}
                PRIMARY KEY (source_record_ref),
            CONSTRAINT {_q('public_evidence_source_record_owner_key')}
                UNIQUE (
                    source_record_ref,
                    source_release_ref,
                    source_release_contract_sha256,
                    source_kind
                ),
            CONSTRAINT {_q('public_evidence_source_record_release_fkey')}
                FOREIGN KEY (
                    source_release_ref,
                    source_release_contract_sha256,
                    source_kind
                ) REFERENCES {release} (
                    source_release_ref,
                    contract_sha256,
                    source_kind
                )
                ON DELETE RESTRICT,
            CONSTRAINT {_q('public_evidence_source_record_shape_check')}
                CHECK (
                    source_record_ref ~ '^pesr1_[A-Za-z0-9_-]{{43}}$'
                    AND record_kind ~ '^[a-z][a-z0-9_]{{1,63}}$'
                    AND identity_contract_id ~
                        '^[a-z][a-z0-9_.:-]{{1,94}}_v[1-9][0-9]*$'
                    AND octet_length(source_release_contract_sha256) = 32
                    AND octet_length(record_hmac_sha256) = 32
                    AND octet_length(payload_sha256) = 32
                    AND ({_source_record_matrix()})
                    AND source_record_ref = {ref_function}(
                        source_release_ref,
                        record_kind,
                        identity_contract_id,
                        record_hmac_sha256,
                        payload_sha256
                    ) IS TRUE
                )
        );
        """
    )
    op.execute(
        f"""
        CREATE INDEX {_q('public_evidence_source_record_release_idx')}
            ON {table} (
                source_release_ref,
                source_release_contract_sha256,
                record_kind,
                source_record_ref
            );
        """
    )


def _create_provider_group_table(schema: str) -> None:
    table = _qt(schema, "public_evidence_provider_group")
    release = _qt(schema, "public_evidence_source_release")
    ref_function = _qf(schema, "public_evidence_provider_group_ref")
    op.execute(
        f"""
        CREATE TABLE {table} (
            provider_group_ref varchar(50) NOT NULL,
            source_release_ref varchar(50) NOT NULL,
            source_release_contract_sha256 bytea NOT NULL,
            source_kind varchar(48) NOT NULL,
            identity_contract_id text NOT NULL,
            identity_sha256 bytea NOT NULL,
            created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
            CONSTRAINT {_q('public_evidence_provider_group_pkey')}
                PRIMARY KEY (provider_group_ref),
            CONSTRAINT {_q('public_evidence_provider_group_owner_key')}
                UNIQUE (
                    provider_group_ref,
                    source_release_ref,
                    source_release_contract_sha256,
                    source_kind
                ),
            CONSTRAINT {_q('public_evidence_provider_group_release_fkey')}
                FOREIGN KEY (
                    source_release_ref,
                    source_release_contract_sha256,
                    source_kind
                ) REFERENCES {release} (
                    source_release_ref,
                    contract_sha256,
                    source_kind
                )
                ON DELETE RESTRICT,
            CONSTRAINT {_q('public_evidence_provider_group_shape_check')}
                CHECK (
                    provider_group_ref ~ '^pegrp1_[A-Za-z0-9_-]{{43}}$'
                    AND source_kind = 'tic'
                    AND identity_contract_id ~
                        '^[a-z][a-z0-9_.:-]{{1,94}}_v[1-9][0-9]*$'
                    AND octet_length(source_release_contract_sha256) = 32
                    AND octet_length(identity_sha256) = 32
                    AND provider_group_ref = {ref_function}(
                        source_release_ref,
                        identity_contract_id,
                        identity_sha256
                    ) IS TRUE
                )
        );
        """
    )
    op.execute(
        f"""
        CREATE INDEX {_q('public_evidence_provider_group_release_idx')}
            ON {table} (
                source_release_ref,
                source_release_contract_sha256,
                provider_group_ref
            );
        """
    )


def _create_source_entity_table(schema: str) -> None:
    table = _qt(schema, "public_evidence_source_entity")
    release = _qt(schema, "public_evidence_source_release")
    ref_function = _qf(schema, "public_evidence_source_entity_ref")
    op.execute(
        f"""
        CREATE TABLE {table} (
            source_entity_ref varchar(50) NOT NULL,
            source_release_ref varchar(50) NOT NULL,
            source_release_contract_sha256 bytea NOT NULL,
            source_kind varchar(48) NOT NULL,
            entity_kind varchar(64) NOT NULL,
            identity_contract_id text NOT NULL,
            identity_sha256 bytea NOT NULL,
            created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
            CONSTRAINT {_q('public_evidence_source_entity_pkey')}
                PRIMARY KEY (source_entity_ref),
            CONSTRAINT {_q('public_evidence_source_entity_owner_key')}
                UNIQUE (
                    source_entity_ref,
                    source_release_ref,
                    source_release_contract_sha256,
                    source_kind
                ),
            CONSTRAINT {_q('public_evidence_source_entity_release_fkey')}
                FOREIGN KEY (
                    source_release_ref,
                    source_release_contract_sha256,
                    source_kind
                ) REFERENCES {release} (
                    source_release_ref,
                    contract_sha256,
                    source_kind
                )
                ON DELETE RESTRICT,
            CONSTRAINT {_q('public_evidence_source_entity_shape_check')}
                CHECK (
                    source_entity_ref ~ '^peent1_[A-Za-z0-9_-]{{43}}$'
                    AND entity_kind ~ '^[a-z][a-z0-9_]{{1,63}}$'
                    AND identity_contract_id ~
                        '^[a-z][a-z0-9_.:-]{{1,94}}_v[1-9][0-9]*$'
                    AND octet_length(source_release_contract_sha256) = 32
                    AND octet_length(identity_sha256) = 32
                    AND ({_source_entity_matrix()})
                    AND source_entity_ref = {ref_function}(
                        source_release_ref,
                        entity_kind,
                        identity_contract_id,
                        identity_sha256
                    ) IS TRUE
                )
        );
        """
    )
    op.execute(
        f"""
        CREATE INDEX {_q('public_evidence_source_entity_release_idx')}
            ON {table} (
                source_release_ref,
                source_release_contract_sha256,
                entity_kind,
                source_entity_ref
            );
        """
    )


def _install_immutable_guards(schema: str) -> None:
    function = _qf(schema, "guard_public_evidence_immutable_catalog")
    for table_name in (
        "public_evidence_source_record",
        "public_evidence_provider_group",
        "public_evidence_source_entity",
    ):
        table = _qt(schema, table_name)
        mutation_trigger = _q(f"{table_name}_mutation_guard")
        truncate_trigger = _q(f"{table_name}_truncate_guard")
        op.execute(
            f"""
            CREATE TRIGGER {mutation_trigger}
            BEFORE UPDATE OR DELETE ON {table}
            FOR EACH ROW EXECUTE FUNCTION {function}();
            """
        )
        op.execute(f"ALTER TABLE {table} ENABLE ALWAYS TRIGGER {mutation_trigger};")
        op.execute(
            f"""
            CREATE TRIGGER {truncate_trigger}
            BEFORE TRUNCATE ON {table}
            FOR EACH STATEMENT EXECUTE FUNCTION {function}();
            """
        )
        op.execute(f"ALTER TABLE {table} ENABLE ALWAYS TRIGGER {truncate_trigger};")
        op.execute(f"REVOKE ALL ON TABLE {table} FROM PUBLIC;")


def _revoke_reference_functions(schema: str) -> None:
    for function_name, argument_types in (
        (
            "public_evidence_source_record_ref",
            "text,text,text,bytea,bytea",
        ),
        (
            "public_evidence_source_entity_ref",
            "text,text,text,bytea",
        ),
        (
            "public_evidence_provider_group_ref",
            "text,text,bytea",
        ),
    ):
        op.execute(
            f"REVOKE ALL ON FUNCTION {_qf(schema, function_name)}"
            f"({argument_types}) FROM PUBLIC;"
        )


def upgrade() -> None:
    """Install three empty immutable roots with no readers or writers."""

    schema = _schema()
    release_table = _qt(schema, "public_evidence_source_release")
    op.execute(
        f"""
        ALTER TABLE {release_table}
        ADD CONSTRAINT {_q('public_evidence_source_release_kind_owner_key')}
        UNIQUE (source_release_ref, contract_sha256, source_kind);
        """
    )
    _create_reference_functions(schema)
    _create_source_record_table(schema)
    _create_provider_group_table(schema)
    _create_source_entity_table(schema)
    _install_immutable_guards(schema)
    _revoke_reference_functions(schema)
    for table_name, comment in (
        (
            "public_evidence_source_record",
            "Opaque source-record references; no source payloads or serving authority",
        ),
        (
            "public_evidence_provider_group",
            "Opaque TiC provider-group references; no legal ownership claim",
        ),
        (
            "public_evidence_source_entity",
            "Opaque source-entity references; no exact rate-site claim",
        ),
    ):
        op.execute(
            f"COMMENT ON TABLE {_qt(schema, table_name)} IS {_literal(comment)};"
        )


def downgrade() -> None:
    """Remove only still-empty publication-disabled reference roots."""

    schema = _schema()
    table_names = (
        "public_evidence_source_record",
        "public_evidence_provider_group",
        "public_evidence_source_entity",
    )
    tables = tuple(_qt(schema, table_name) for table_name in table_names)
    release_table = _qt(schema, "public_evidence_source_release")
    op.execute(
        "LOCK TABLE "
        + ", ".join((*tables, release_table))
        + " IN ACCESS EXCLUSIVE MODE;"
    )
    nonempty_checks = " OR ".join(
        f"EXISTS (SELECT 1 FROM {table} LIMIT 1)" for table in tables
    )
    op.execute(
        f"""
        DO $block$
        BEGIN
            IF {nonempty_checks} THEN
                RAISE EXCEPTION
                    'public_evidence_downgrade_requires_empty_reference_roots'
                    USING ERRCODE = '55000';
            END IF;
        END;
        $block$;
        """
    )
    for table_name in reversed(table_names):
        op.execute(f"DROP TABLE {_qt(schema, table_name)};")
    for function_name, argument_types in (
        (
            "public_evidence_provider_group_ref",
            "text,text,bytea",
        ),
        (
            "public_evidence_source_entity_ref",
            "text,text,text,bytea",
        ),
        (
            "public_evidence_source_record_ref",
            "text,text,text,bytea,bytea",
        ),
    ):
        op.execute(f"DROP FUNCTION {_qf(schema, function_name)}({argument_types});")
    op.execute(
        f"ALTER TABLE {release_table} DROP CONSTRAINT "
        f"{_q('public_evidence_source_release_kind_owner_key')};"
    )
