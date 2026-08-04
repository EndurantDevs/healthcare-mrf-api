"""Add the inactive encrypted raw-TIN vault foundation.

Revision ID: 20260804100000_ptg2_raw_tin_vault_foundation
Revises: 20260731190000_tiger_zcta5_zip_index
"""

from __future__ import annotations

import os

from alembic import op


revision = "20260804100000_ptg2_raw_tin_vault_foundation"
down_revision = "20260731190000_tiger_zcta5_zip_index"
branch_labels = None
depends_on = None


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


def upgrade() -> None:
    """Install one empty ciphertext-only table with no readers or writers."""

    schema = _schema()
    vault = _qt(schema, "ptg2_raw_tin_vault_entry")
    policy_descriptor = _qf(
        schema,
        "tin_npi_connector_token_policy_descriptor_sha256",
    )
    mutation_guard = _qf(schema, "guard_ptg2_raw_tin_vault_entry")

    op.execute(
        f"""
        CREATE TABLE {vault} (
            token_policy_id varchar(55) NOT NULL,
            token_policy_descriptor_sha256 bytea NOT NULL,
            tin_hmac_sha256 bytea NOT NULL,
            tin_type varchar(8) NOT NULL,
            encryption_contract varchar(48) NOT NULL,
            binding_contract varchar(48) NOT NULL,
            encryption_key_id varchar(32) NOT NULL,
            ciphertext varchar(256) NOT NULL,
            created_at timestamptz NOT NULL
                DEFAULT transaction_timestamp(),
            updated_at timestamptz NOT NULL
                DEFAULT transaction_timestamp(),
            CONSTRAINT {_q('ptg2_raw_tin_vault_entry_pkey')}
                PRIMARY KEY (token_policy_id, tin_hmac_sha256),
            CONSTRAINT {_q('ptg2_raw_tin_vault_policy_check')}
                CHECK (
                    token_policy_id ~
                        '^ptg-tin-hmac-sha256-v1:[a-z0-9]'
                        '[a-z0-9._-]{{0,31}}$'
                    AND octet_length(token_policy_id) <= 55
                    AND octet_length(token_policy_descriptor_sha256) = 32
                    AND token_policy_descriptor_sha256 =
                        {policy_descriptor}(token_policy_id)
                ),
            CONSTRAINT {_q('ptg2_raw_tin_vault_identity_check')}
                CHECK (
                    tin_type = 'ein'
                    AND octet_length(tin_hmac_sha256) = 32
                ),
            CONSTRAINT {_q('ptg2_raw_tin_vault_contract_check')}
                CHECK (
                    encryption_contract = 'fernet_hmac_sha256_bound_v1'
                    AND binding_contract =
                        'token_policy_full_hmac_ein_v1'
                ),
            CONSTRAINT {_q('ptg2_raw_tin_vault_ciphertext_check')}
                CHECK (
                    encryption_key_id ~
                        '^[a-z0-9][a-z0-9._-]{{0,31}}$'
                    AND octet_length(ciphertext) BETWEEN 64 AND 256
                    AND ciphertext ~
                        '^hptinv1:[a-z0-9][a-z0-9._-]{{0,31}}:'
                        '[A-Za-z0-9_-]+={{0,2}}$'
                    AND split_part(ciphertext, ':', 2) = encryption_key_id
                ),
            CONSTRAINT {_q('ptg2_raw_tin_vault_timestamp_check')}
                CHECK (updated_at >= created_at)
        );
        """
    )
    op.execute(
        f"""
        CREATE INDEX {_q('ptg2_raw_tin_vault_encryption_key_idx')}
            ON {vault} (encryption_key_id, token_policy_id);
        """
    )
    op.execute(
        f"""
        CREATE FUNCTION {mutation_guard}()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $function$
        BEGIN
            IF TG_OP = 'TRUNCATE' THEN
                RAISE EXCEPTION 'ptg2_raw_tin_vault_truncate_forbidden'
                    USING ERRCODE = '55000';
            END IF;
            IF TG_OP = 'DELETE' THEN
                RAISE EXCEPTION 'ptg2_raw_tin_vault_delete_forbidden'
                    USING ERRCODE = '55000';
            END IF;
            IF ROW(
                NEW.token_policy_id,
                NEW.token_policy_descriptor_sha256,
                NEW.tin_hmac_sha256,
                NEW.tin_type,
                NEW.encryption_contract,
                NEW.binding_contract,
                NEW.created_at
            ) IS DISTINCT FROM ROW(
                OLD.token_policy_id,
                OLD.token_policy_descriptor_sha256,
                OLD.tin_hmac_sha256,
                OLD.tin_type,
                OLD.encryption_contract,
                OLD.binding_contract,
                OLD.created_at
            ) THEN
                RAISE EXCEPTION 'ptg2_raw_tin_vault_identity_immutable'
                    USING ERRCODE = '55000';
            END IF;
            IF ROW(NEW.encryption_key_id, NEW.ciphertext)
                IS NOT DISTINCT FROM
               ROW(OLD.encryption_key_id, OLD.ciphertext) THEN
                RAISE EXCEPTION 'ptg2_raw_tin_vault_rewrap_required'
                    USING ERRCODE = '55000';
            END IF;
            NEW.updated_at := transaction_timestamp();
            RETURN NEW;
        END;
        $function$;
        """
    )
    op.execute(
        f"""
        CREATE TRIGGER {_q('ptg2_raw_tin_vault_mutation_guard')}
        BEFORE UPDATE OR DELETE ON {vault}
        FOR EACH ROW EXECUTE FUNCTION {mutation_guard}();
        """
    )
    op.execute(
        f"ALTER TABLE {vault} ENABLE ALWAYS TRIGGER "
        f"{_q('ptg2_raw_tin_vault_mutation_guard')};"
    )
    op.execute(
        f"""
        CREATE TRIGGER {_q('ptg2_raw_tin_vault_truncate_guard')}
        BEFORE TRUNCATE ON {vault}
        FOR EACH STATEMENT EXECUTE FUNCTION {mutation_guard}();
        """
    )
    op.execute(
        f"ALTER TABLE {vault} ENABLE ALWAYS TRIGGER "
        f"{_q('ptg2_raw_tin_vault_truncate_guard')};"
    )
    op.execute(f"REVOKE ALL ON TABLE {vault} FROM PUBLIC;")
    op.execute(f"REVOKE ALL ON FUNCTION {mutation_guard}() FROM PUBLIC;")
    op.execute(
        f"COMMENT ON TABLE {vault} IS "
        "'Inactive ciphertext-only EIN vault; no runtime reader or writer';"
    )


def downgrade() -> None:
    """Remove only the still-empty inactive foundation."""

    schema = _schema()
    vault = _qt(schema, "ptg2_raw_tin_vault_entry")
    mutation_guard = _qf(schema, "guard_ptg2_raw_tin_vault_entry")
    op.execute(f"LOCK TABLE {vault} IN ACCESS EXCLUSIVE MODE;")
    op.execute(
        f"""
        DO $block$
        BEGIN
            IF EXISTS (SELECT 1 FROM {vault} LIMIT 1) THEN
                RAISE EXCEPTION
                    'ptg2_raw_tin_vault_downgrade_requires_empty_foundation'
                    USING ERRCODE = '55000';
            END IF;
        END;
        $block$;
        """
    )
    op.execute(
        f"DROP TRIGGER IF EXISTS {_q('ptg2_raw_tin_vault_mutation_guard')} "
        f"ON {vault};"
    )
    op.execute(
        f"DROP TRIGGER IF EXISTS {_q('ptg2_raw_tin_vault_truncate_guard')} "
        f"ON {vault};"
    )
    op.execute(f"DROP TABLE IF EXISTS {vault};")
    op.execute(f"DROP FUNCTION IF EXISTS {mutation_guard}();")
