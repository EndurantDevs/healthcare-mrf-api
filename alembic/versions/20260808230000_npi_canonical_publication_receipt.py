# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Add immutable canonical-NPI publication receipts.

Revision ID: 20260808230000_npi_canonical_publication_receipt
Revises: 20260808220000_public_evidence_nppes_registry_admission
"""

from __future__ import annotations

import os

from alembic import op
import sqlalchemy as sa


revision = "20260808230000_npi_canonical_publication_receipt"
down_revision = "20260808220000_public_evidence_nppes_registry_admission"
branch_labels = None
depends_on = None

_TABLE = "npi_canonical_publication_receipt"
_SEAL = "npi_canonical_publication_receipt_seal"
_CHAIN_SEAL = "public_evidence_nppes_registry_chain_admission_seal"
_IMPORT_RUN = "import_run"
_VALIDATOR = "validate_npi_canonical_publication_receipt"
_RUN_GUARD = "guard_npi_canonical_publication_run"
_CANONICAL_GUARD = "guard_npi_canonical_publication_after_seal"
_CANONICAL_TABLES = (
    "npi",
    "npi_address",
    "npi_taxonomy",
    "npi_taxonomy_group",
    "npi_other_identifier",
    "npi_phone_staffing",
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


def _existing_canonical_tables(schema: str) -> tuple[str, ...]:
    """Accept either the complete legacy NPI family or a fresh empty schema."""

    if op.get_context().as_sql:
        return _CANONICAL_TABLES
    connection = op.get_bind()
    existing_tables = tuple(
        table_name
        for table_name in _CANONICAL_TABLES
        if connection.execute(
            sa.text("SELECT to_regclass(:relation_name) IS NOT NULL"),
            {"relation_name": f"{schema}.{table_name}"},
        ).scalar_one()
    )
    if existing_tables and existing_tables != _CANONICAL_TABLES:
        raise RuntimeError(
            "canonical NPI tables must be all present or all absent"
        )
    return existing_tables


def upgrade() -> None:
    """Install an append-only receipt for the canonical legacy API cutover."""

    schema = _schema()
    table = _qt(schema, _TABLE)
    immutable = f"{_q(schema)}.{_q('guard_public_evidence_immutable_catalog')}"
    op.execute(
        f"""
        CREATE TABLE {table} (
            publication_generation bigint GENERATED ALWAYS AS IDENTITY,
            publication_ref varchar(50) NOT NULL
                CONSTRAINT npi_canonical_publication_receipt_pkey PRIMARY KEY,
            contract varchar(64) NOT NULL,
            contract_sha256 bytea NOT NULL,
            run_id varchar(64) NOT NULL,
            attempt_id text NOT NULL,
            attempt_started_at timestamptz NOT NULL,
            chain_ref varchar(50) NOT NULL,
            import_date date NOT NULL,
            npi_table_oid oid NOT NULL,
            npi_address_table_oid oid NOT NULL,
            npi_taxonomy_table_oid oid NOT NULL,
            npi_taxonomy_group_table_oid oid NOT NULL,
            npi_other_identifier_table_oid oid NOT NULL,
            npi_phone_staffing_table_oid oid NOT NULL,
            npi_row_count bigint NOT NULL,
            npi_address_row_count bigint NOT NULL,
            npi_taxonomy_row_count bigint NOT NULL,
            npi_taxonomy_group_row_count bigint NOT NULL,
            npi_other_identifier_row_count bigint NOT NULL,
            npi_phone_staffing_row_count bigint NOT NULL,
            publication_state varchar(32) NOT NULL,
            evidence_serving_authority varchar(16) NOT NULL,
            evidence_publication_enabled boolean NOT NULL,
            created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
            CONSTRAINT npi_canonical_publication_receipt_generation_key
                UNIQUE (publication_generation),
            CONSTRAINT npi_canonical_publication_receipt_run_key UNIQUE (run_id),
            CONSTRAINT npi_canonical_publication_receipt_run_fkey
                FOREIGN KEY (run_id) REFERENCES {_qt(schema, _IMPORT_RUN)} (run_id)
                ON DELETE RESTRICT,
            CONSTRAINT npi_canonical_publication_receipt_chain_fkey
                FOREIGN KEY (chain_ref) REFERENCES {_qt(schema, _CHAIN_SEAL)} (chain_ref)
                ON DELETE RESTRICT,
            CONSTRAINT npi_canonical_publication_receipt_shape_check CHECK ((
                publication_generation BETWEEN 1 AND 9007199254740991
                AND publication_ref ~ '^nppub1_[A-Za-z0-9_-]{{43}}$'
                AND contract = 'healthporta.npi-canonical-publication.v1'
                AND octet_length(contract_sha256) = 32
                AND run_id ~ '^[ -~]{{1,64}}$'
                AND length(attempt_id) = length(run_id) + 33
                AND left(attempt_id, length(run_id) + 1) = run_id || ':'
                AND substring(attempt_id FROM length(run_id) + 2)
                    ~ '^[0-9a-f]{{32}}$'
                AND chain_ref ~ '^penpc1_[A-Za-z0-9_-]{{43}}$'
                AND import_date BETWEEN DATE '0001-01-01' AND DATE '9999-12-31'
                AND npi_table_oid::bigint > 0
                AND npi_address_table_oid::bigint > 0
                AND npi_taxonomy_table_oid::bigint > 0
                AND npi_taxonomy_group_table_oid::bigint > 0
                AND npi_other_identifier_table_oid::bigint > 0
                AND npi_phone_staffing_table_oid::bigint > 0
                AND npi_row_count BETWEEN 0 AND 9007199254740991
                AND npi_address_row_count BETWEEN 0 AND 9007199254740991
                AND npi_taxonomy_row_count BETWEEN 0 AND 9007199254740991
                AND npi_taxonomy_group_row_count BETWEEN 0 AND 9007199254740991
                AND npi_other_identifier_row_count BETWEEN 0 AND 9007199254740991
                AND npi_phone_staffing_row_count BETWEEN 0 AND 9007199254740991
                AND publication_state = 'canonical_api_published'
                AND evidence_serving_authority = 'none'
                AND NOT evidence_publication_enabled
                AND isfinite(attempt_started_at)
                AND isfinite(created_at)
                AND attempt_started_at AT TIME ZONE 'UTC'
                    >= TIMESTAMP '0001-01-01 00:00:00'
                AND attempt_started_at AT TIME ZONE 'UTC'
                    < TIMESTAMP '10000-01-01 00:00:00'
                AND created_at AT TIME ZONE 'UTC'
                    >= TIMESTAMP '0001-01-01 00:00:00'
                AND created_at AT TIME ZONE 'UTC'
                    < TIMESTAMP '10000-01-01 00:00:00'
                AND attempt_started_at <= created_at
            ) IS TRUE)
        );
        """
    )
    seal = _qt(schema, _SEAL)
    op.execute(
        f"""
        CREATE TABLE {seal} (
            publication_ref varchar(50) NOT NULL
                CONSTRAINT npi_canonical_publication_receipt_seal_pkey
                PRIMARY KEY,
            sealed_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
            CONSTRAINT npi_canonical_publication_receipt_seal_parent_fkey
                FOREIGN KEY (publication_ref) REFERENCES {table} (publication_ref)
                ON DELETE RESTRICT,
            CONSTRAINT npi_canonical_publication_receipt_seal_shape_check
                CHECK ((isfinite(sealed_at)) IS TRUE)
        );
        """
    )
    op.execute(
        f"CREATE TRIGGER npi_canonical_publication_receipt_mutation_guard "
        f"BEFORE UPDATE OR DELETE ON {table} FOR EACH ROW "
        f"EXECUTE FUNCTION {immutable}();"
    )
    op.execute(
        f"ALTER TABLE {table} ENABLE ALWAYS TRIGGER "
        "npi_canonical_publication_receipt_mutation_guard;"
    )
    op.execute(
        f"CREATE TRIGGER npi_canonical_publication_receipt_truncate_guard "
        f"BEFORE TRUNCATE ON {table} FOR EACH STATEMENT "
        f"EXECUTE FUNCTION {immutable}();"
    )
    op.execute(
        f"ALTER TABLE {table} ENABLE ALWAYS TRIGGER "
        "npi_canonical_publication_receipt_truncate_guard;"
    )
    op.execute(
        f"CREATE TRIGGER npi_canonical_publication_receipt_seal_mutation_guard "
        f"BEFORE UPDATE OR DELETE ON {seal} FOR EACH ROW "
        f"EXECUTE FUNCTION {immutable}();"
    )
    op.execute(
        f"ALTER TABLE {seal} ENABLE ALWAYS TRIGGER "
        "npi_canonical_publication_receipt_seal_mutation_guard;"
    )
    op.execute(
        f"CREATE TRIGGER npi_canonical_publication_receipt_seal_truncate_guard "
        f"BEFORE TRUNCATE ON {seal} FOR EACH STATEMENT "
        f"EXECUTE FUNCTION {immutable}();"
    )
    op.execute(
        f"ALTER TABLE {seal} ENABLE ALWAYS TRIGGER "
        "npi_canonical_publication_receipt_seal_truncate_guard;"
    )
    _create_canonical_mutation_guard(
        schema,
        canonical_tables=_existing_canonical_tables(schema),
    )
    _create_receipt_validator(schema)
    _create_import_run_guard(schema)
    op.execute(f"REVOKE ALL ON TABLE {table} FROM PUBLIC;")
    op.execute(f"REVOKE ALL ON TABLE {seal} FROM PUBLIC;")
    op.execute(
        f"REVOKE ALL ON SEQUENCE {_q(schema)}."
        f"{_q(_TABLE + '_publication_generation_seq')} FROM PUBLIC;"
    )


def _create_canonical_mutation_guard(
    schema: str,
    *,
    canonical_tables: tuple[str, ...],
) -> None:
    """Reject canonical-table writes after this transaction seals a receipt."""

    guard = _qf(schema, _CANONICAL_GUARD)
    op.execute(
        f"""
        CREATE FUNCTION {guard}() RETURNS trigger LANGUAGE plpgsql
        SECURITY DEFINER SET search_path=pg_catalog AS $function$
        BEGIN
            IF current_setting(
                'healthporta.npi_canonical_publication_sealed', true
            ) = '1' THEN
                RAISE EXCEPTION 'npi_canonical_publication_is_sealed'
                    USING ERRCODE='55000';
            END IF;
            RETURN NULL;
        END; $function$;
        """
    )
    op.execute(f"REVOKE ALL ON FUNCTION {guard}() FROM PUBLIC;")
    for table_name in canonical_tables:
        canonical_table = _qt(schema, table_name)
        op.execute(
            f"CREATE TRIGGER npi_canonical_publication_postseal_write_guard "
            f"BEFORE INSERT OR UPDATE OR DELETE ON {canonical_table} "
            f"FOR EACH STATEMENT EXECUTE FUNCTION {guard}();"
        )
        op.execute(
            f"ALTER TABLE {canonical_table} ENABLE ALWAYS TRIGGER "
            "npi_canonical_publication_postseal_write_guard;"
        )
        op.execute(
            f"CREATE TRIGGER npi_canonical_publication_postseal_truncate_guard "
            f"BEFORE TRUNCATE ON {canonical_table} FOR EACH STATEMENT "
            f"EXECUTE FUNCTION {guard}();"
        )
        op.execute(
            f"ALTER TABLE {canonical_table} ENABLE ALWAYS TRIGGER "
            "npi_canonical_publication_postseal_truncate_guard;"
        )


def _create_receipt_validator(schema: str) -> None:
    table = _qt(schema, _TABLE)
    seal = _qt(schema, _SEAL)
    import_run = _qt(schema, _IMPORT_RUN)
    validator = _qf(schema, _VALIDATOR)
    digest = _qf(schema, "public_evidence_record_digest")
    reference = _qf(schema, "public_evidence_record_ref")
    schema_literal = _literal(schema)
    count_statements = "\n".join(
        f"SELECT count(*)::bigint INTO count_{table_name} "
        f"FROM {_qt(schema, table_name)};"
        for table_name in _CANONICAL_TABLES
    )
    canonical_locks = ", ".join(
        _qt(schema, table_name) for table_name in _CANONICAL_TABLES
    )
    op.execute(
        f"""
        CREATE FUNCTION {validator}() RETURNS trigger LANGUAGE plpgsql
        SECURITY DEFINER SET search_path=pg_catalog AS $function$
        DECLARE
            receipt_row RECORD;
            run_row RECORD;
            publication_json text;
            relations_valid boolean;
            invalid boolean;
            count_npi bigint;
            count_npi_address bigint;
            count_npi_taxonomy bigint;
            count_npi_taxonomy_group bigint;
            count_npi_other_identifier bigint;
            count_npi_phone_staffing bigint;
        BEGIN
            PERFORM pg_advisory_xact_lock(hashtextextended(
                'healthporta.npi-canonical-publication:' || NEW.publication_ref,
                0
            ));
            SELECT * INTO receipt_row
              FROM {table}
             WHERE publication_ref=NEW.publication_ref;
            IF receipt_row IS NULL THEN
                RAISE EXCEPTION 'npi_canonical_publication_invalid'
                    USING ERRCODE='23514';
            END IF;
            SELECT * INTO run_row
              FROM {import_run}
             WHERE run_id=receipt_row.run_id
             FOR UPDATE;
            IF run_row IS NULL THEN
                RAISE EXCEPTION 'npi_canonical_publication_invalid'
                    USING ERRCODE='23514';
            END IF;
            LOCK TABLE {canonical_locks} IN SHARE MODE;
            publication_json :=
                '{{"attempt_id":' || to_json(receipt_row.attempt_id)::text ||
                ',"attempt_started_at":' || to_json(
                    to_char(receipt_row.attempt_started_at AT TIME ZONE 'UTC',
                        'YYYY-MM-DD"T"HH24:MI:SS.US') || '+00:00')::text ||
                ',"chain_ref":' || to_json(receipt_row.chain_ref)::text ||
                ',"contract":' || to_json(receipt_row.contract)::text ||
                ',"evidence_publication_enabled"' || ':' || 'false' ||
                ',"evidence_serving_authority":"none"' ||
                ',"import_date":' || to_json(
                    to_char(receipt_row.import_date, 'YYYY-MM-DD'))::text ||
                ',"publication_state":"canonical_api_published"' ||
                ',"relation_oids":{{"npi":' ||
                    receipt_row.npi_table_oid::bigint::text ||
                ',"npi_address":' || receipt_row.npi_address_table_oid::bigint::text ||
                ',"npi_other_identifier":' ||
                    receipt_row.npi_other_identifier_table_oid::bigint::text ||
                ',"npi_phone_staffing":' ||
                    receipt_row.npi_phone_staffing_table_oid::bigint::text ||
                ',"npi_taxonomy":' || receipt_row.npi_taxonomy_table_oid::bigint::text ||
                ',"npi_taxonomy_group":' ||
                    receipt_row.npi_taxonomy_group_table_oid::bigint::text || '}}' ||
                ',"row_counts":{{"npi":' || receipt_row.npi_row_count::text ||
                ',"npi_address":' || receipt_row.npi_address_row_count::text ||
                ',"npi_other_identifier":' ||
                    receipt_row.npi_other_identifier_row_count::text ||
                ',"npi_phone_staffing":' ||
                    receipt_row.npi_phone_staffing_row_count::text ||
                ',"npi_taxonomy":' || receipt_row.npi_taxonomy_row_count::text ||
                ',"npi_taxonomy_group":' ||
                    receipt_row.npi_taxonomy_group_row_count::text || '}}' ||
                ',"run_id":' || to_json(receipt_row.run_id)::text || '}}';

            SELECT count(*)=6 AND count(DISTINCT relation.oid)=6
              INTO relations_valid
              FROM (VALUES
                    (receipt_row.npi_table_oid, 'npi'),
                    (receipt_row.npi_address_table_oid, 'npi_address'),
                    (receipt_row.npi_taxonomy_table_oid, 'npi_taxonomy'),
                    (receipt_row.npi_taxonomy_group_table_oid, 'npi_taxonomy_group'),
                    (receipt_row.npi_other_identifier_table_oid, 'npi_other_identifier'),
                    (receipt_row.npi_phone_staffing_table_oid, 'npi_phone_staffing')
                   ) AS expected(oid, relation_name)
              JOIN pg_catalog.pg_class AS relation
                ON relation.oid=expected.oid
               AND relation.relname=expected.relation_name
               AND relation.relkind IN ('r', 'p')
              JOIN pg_catalog.pg_namespace AS namespace
                ON namespace.oid=relation.relnamespace
               AND namespace.nspname={schema_literal};

            {count_statements}

            invalid := NOT relations_valid
                OR run_row.importer IS DISTINCT FROM 'npi'
                OR run_row.status IS DISTINCT FROM 'succeeded'
                OR run_row.phase_detail IS DISTINCT FROM 'npi published'
                OR run_row.error IS NOT NULL
                OR run_row.snapshot_id IS DISTINCT FROM receipt_row.publication_ref
                OR run_row.heartbeat_at IS DISTINCT FROM
                    transaction_timestamp() AT TIME ZONE 'UTC'
                OR run_row.finished_at IS DISTINCT FROM
                    transaction_timestamp() AT TIME ZONE 'UTC'
                OR run_row.progress::jsonb IS DISTINCT FROM jsonb_build_object(
                    'unit', 'rows',
                    'done', receipt_row.npi_address_row_count,
                    'total', receipt_row.npi_address_row_count,
                    'pct', 100,
                    'message', 'succeeded',
                    'phase', 'npi published',
                    'attempt_id', receipt_row.attempt_id,
                    'attempt_started_at',
                        to_char(receipt_row.attempt_started_at AT TIME ZONE 'UTC',
                            'YYYY-MM-DD"T"HH24:MI:SS.US') || '+00:00'
                )
                OR run_row.metrics::jsonb->'npi_canonical_publication'
                    IS DISTINCT FROM jsonb_build_object(
                        'publication_generation',
                            receipt_row.publication_generation,
                        'publication_ref', receipt_row.publication_ref,
                        'chain_ref', receipt_row.chain_ref,
                        'row_counts', jsonb_build_object(
                            'npi', receipt_row.npi_row_count,
                            'npi_address', receipt_row.npi_address_row_count,
                            'npi_taxonomy', receipt_row.npi_taxonomy_row_count,
                            'npi_taxonomy_group',
                                receipt_row.npi_taxonomy_group_row_count,
                            'npi_other_identifier',
                                receipt_row.npi_other_identifier_row_count,
                            'npi_phone_staffing',
                                receipt_row.npi_phone_staffing_row_count
                        )
                    )
                OR run_row.metrics::jsonb->'nppes_public_evidence'->>'chain_ref'
                    IS DISTINCT FROM receipt_row.chain_ref
                OR receipt_row.created_at IS DISTINCT FROM transaction_timestamp()
                OR receipt_row.contract_sha256 IS DISTINCT FROM
                    {digest}('npi_canonical_publication', publication_json)
                OR receipt_row.publication_ref IS DISTINCT FROM
                    {reference}(
                        'nppub1_', 'npi_canonical_publication', publication_json)
                OR receipt_row.npi_row_count IS DISTINCT FROM count_npi
                OR receipt_row.npi_address_row_count IS DISTINCT FROM count_npi_address
                OR receipt_row.npi_taxonomy_row_count IS DISTINCT FROM count_npi_taxonomy
                OR receipt_row.npi_taxonomy_group_row_count IS DISTINCT FROM
                    count_npi_taxonomy_group
                OR receipt_row.npi_other_identifier_row_count IS DISTINCT FROM
                    count_npi_other_identifier
                OR receipt_row.npi_phone_staffing_row_count IS DISTINCT FROM
                    count_npi_phone_staffing;
            IF invalid THEN
                RAISE EXCEPTION 'npi_canonical_publication_invalid'
                    USING ERRCODE='23514';
            END IF;
            PERFORM set_config(
                'healthporta.npi_canonical_publication_sealed', '1', true
            );
            INSERT INTO {seal} (publication_ref)
            VALUES (receipt_row.publication_ref);
            RETURN NEW;
        END; $function$;
        """
    )
    op.execute(f"REVOKE ALL ON FUNCTION {validator}() FROM PUBLIC;")
    op.execute(
        f"CREATE CONSTRAINT TRIGGER npi_canonical_publication_receipt_integrity_guard "
        f"AFTER INSERT ON {table} DEFERRABLE INITIALLY DEFERRED FOR EACH ROW "
        f"EXECUTE FUNCTION {validator}();"
    )
    op.execute(
        f"ALTER TABLE {table} ENABLE ALWAYS TRIGGER "
        "npi_canonical_publication_receipt_integrity_guard;"
    )


def _create_import_run_guard(schema: str) -> None:
    import_run = _qt(schema, _IMPORT_RUN)
    table = _qt(schema, _TABLE)
    seal = _qt(schema, _SEAL)
    guard = _qf(schema, _RUN_GUARD)
    op.execute(
        f"""
        CREATE FUNCTION {guard}() RETURNS trigger LANGUAGE plpgsql
        SECURITY DEFINER SET search_path=pg_catalog AS $function$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM {table} AS receipt
                JOIN {seal} AS sealed USING (publication_ref)
                WHERE receipt.run_id=OLD.run_id
            ) THEN
                RAISE EXCEPTION 'npi_canonical_publication_run_is_immutable'
                    USING ERRCODE='55000';
            END IF;
            IF TG_OP='DELETE' THEN
                RETURN OLD;
            END IF;
            RETURN NEW;
        END; $function$;
        """
    )
    op.execute(f"REVOKE ALL ON FUNCTION {guard}() FROM PUBLIC;")
    op.execute(
        f"CREATE TRIGGER npi_canonical_publication_import_run_guard "
        f"BEFORE UPDATE OR DELETE ON {import_run} FOR EACH ROW "
        f"EXECUTE FUNCTION {guard}();"
    )
    op.execute(
        f"ALTER TABLE {import_run} ENABLE ALWAYS TRIGGER "
        "npi_canonical_publication_import_run_guard;"
    )


def downgrade() -> None:
    """Remove the publication receipt only while it remains empty."""

    schema = _schema()
    table = _qt(schema, _TABLE)
    seal = _qt(schema, _SEAL)
    import_run = _qt(schema, _IMPORT_RUN)
    existing_canonical_tables = _existing_canonical_tables(schema)
    lock_tables = (import_run,) + tuple(
        _qt(schema, table_name) for table_name in existing_canonical_tables
    ) + (table, seal)
    op.execute(
        f"LOCK TABLE {', '.join(lock_tables)} "
        "IN ACCESS EXCLUSIVE MODE;"
    )
    op.execute(
        f"""
        DO $block$ BEGIN
            IF EXISTS (SELECT 1 FROM {table} LIMIT 1)
               OR EXISTS (SELECT 1 FROM {seal} LIMIT 1) THEN
                RAISE EXCEPTION 'npi_canonical_publication_downgrade_requires_empty'
                    USING ERRCODE='55000';
            END IF;
        END; $block$;
        """
    )
    op.execute(
        f"DROP TRIGGER npi_canonical_publication_import_run_guard "
        f"ON {import_run};"
    )
    op.execute(f"DROP FUNCTION {_qf(schema, _RUN_GUARD)}();")
    for table_name in existing_canonical_tables:
        canonical_table = _qt(schema, table_name)
        op.execute(
            "DROP TRIGGER npi_canonical_publication_postseal_write_guard "
            f"ON {canonical_table};"
        )
        op.execute(
            "DROP TRIGGER npi_canonical_publication_postseal_truncate_guard "
            f"ON {canonical_table};"
        )
    op.execute(f"DROP TABLE {seal};")
    op.execute(f"DROP TABLE {table};")
    op.execute(f"DROP FUNCTION {_qf(schema, _VALIDATOR)}();")
    op.execute(f"DROP FUNCTION {_qf(schema, _CANONICAL_GUARD)}();")
