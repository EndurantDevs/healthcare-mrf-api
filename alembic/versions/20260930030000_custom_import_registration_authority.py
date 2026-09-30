# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Retain immutable registration capabilities and their terminal evidence."""

from __future__ import annotations

import os

from alembic import op

revision = "20260930030000_custom_import_registration_authority"
down_revision = "20260930020000_cms_npd_serving_coverage"
branch_labels = None
depends_on = None

_TABLE = "custom_import_registration_authority"
_GUARD = "guard_custom_import_registration_authority"


def _schema() -> str:
    """Use the existing migration schema contract without accepting drift."""

    runtime = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy = os.getenv("DB_SCHEMA")
    if runtime and legacy and runtime != legacy:
        raise RuntimeError("DB_SCHEMA and HLTHPRT_DB_SCHEMA must match")
    return runtime or legacy or "mrf"


def _qualified(schema: str, identifier: str) -> str:
    """Quote both identifiers independently for migration-owned objects."""

    return '"' + schema.replace('"', '""') + '"."' + identifier.replace('"', '""') + '"'


def upgrade() -> None:
    """Create an empty authority relation and irreversible transition guards."""

    schema = _schema()
    table = _qualified(schema, _TABLE)
    guard = _qualified(schema, _GUARD)
    op.execute(f"""CREATE TABLE {table} (
        authority_id VARCHAR(128) NOT NULL,
        input_sha256 BYTEA,
        token_sha256 BYTEA,
        expires_at TIMESTAMP WITH TIME ZONE,
        created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT clock_timestamp(),
        revoked_at TIMESTAMP WITH TIME ZONE,
        result_receipt TEXT,
        CONSTRAINT custom_import_reg_authority_pkey PRIMARY KEY (authority_id),
        CONSTRAINT custom_import_reg_authority_id_check
            CHECK (authority_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{{0,127}}$'),
        CONSTRAINT custom_import_reg_authority_pins_check CHECK (
            (input_sha256 IS NOT NULL AND token_sha256 IS NOT NULL AND expires_at IS NOT NULL
             AND octet_length(input_sha256) = 32 AND octet_length(token_sha256) = 32)
            OR (input_sha256 IS NULL AND token_sha256 IS NULL AND expires_at IS NULL
                AND revoked_at IS NOT NULL AND result_receipt IS NULL)
        ),
        CONSTRAINT custom_import_reg_authority_result_check CHECK (
            result_receipt IS NULL OR
            (input_sha256 IS NOT NULL AND octet_length(result_receipt) BETWEEN 2 AND 4096)
        )
    )""")
    op.execute(f"REVOKE ALL ON TABLE {table} FROM PUBLIC")
    op.execute(f"""CREATE FUNCTION {guard}() RETURNS trigger
        LANGUAGE plpgsql SET search_path = pg_catalog AS $guard$
        BEGIN
            IF TG_OP IN ('DELETE', 'TRUNCATE') THEN
                RAISE EXCEPTION 'custom_import_registration_authority_retained'
                    USING ERRCODE = 'P0001';
            END IF;
            IF NEW.authority_id IS DISTINCT FROM OLD.authority_id
               OR NEW.input_sha256 IS DISTINCT FROM OLD.input_sha256
               OR NEW.token_sha256 IS DISTINCT FROM OLD.token_sha256
               OR NEW.expires_at IS DISTINCT FROM OLD.expires_at
               OR NEW.created_at IS DISTINCT FROM OLD.created_at
               OR (OLD.revoked_at IS NOT NULL AND NEW.revoked_at IS DISTINCT FROM OLD.revoked_at)
               OR (OLD.result_receipt IS NOT NULL AND NEW.result_receipt IS DISTINCT FROM OLD.result_receipt)
            THEN
                RAISE EXCEPTION 'custom_import_registration_authority_immutable'
                    USING ERRCODE = 'P0001';
            END IF;
            RETURN NEW;
        END; $guard$""")
    op.execute(f"REVOKE ALL ON FUNCTION {guard}() FROM PUBLIC")
    op.execute(f"""CREATE TRIGGER custom_import_reg_authority_row_guard
        BEFORE UPDATE OR DELETE ON {table}
        FOR EACH ROW EXECUTE FUNCTION {guard}()""")
    op.execute(f"""CREATE TRIGGER custom_import_reg_authority_truncate_guard
        BEFORE TRUNCATE ON {table}
        FOR EACH STATEMENT EXECUTE FUNCTION {guard}()""")
    for trigger in ("custom_import_reg_authority_row_guard", "custom_import_reg_authority_truncate_guard"):
        op.execute(f"ALTER TABLE {table} ENABLE ALWAYS TRIGGER {trigger}")


def downgrade() -> None:
    """Remove only an unused authority relation; preserve every retained row."""

    schema = _schema()
    table = _qualified(schema, _TABLE)
    op.execute(f"""DO $guard$
        BEGIN
            LOCK TABLE {table} IN ACCESS EXCLUSIVE MODE;
            IF EXISTS (SELECT 1 FROM {table}) THEN
                RAISE EXCEPTION 'custom_import_registration_authority_downgrade_blocked'
                    USING ERRCODE = 'P0001';
            END IF;
        END; $guard$""")
    op.execute(f"DROP TABLE {table}")
    op.execute(f"DROP FUNCTION {_qualified(schema, _GUARD)}()")
