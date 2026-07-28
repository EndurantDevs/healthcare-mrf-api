# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Migration helpers for immutable frozen PTG source-file bindings."""

from __future__ import annotations

from typing import Any


BINDING_TABLE = "ptg2_frozen_source_file_binding"
BINDING_GUARD_FUNCTION = "guard_ptg2_frozen_source_file_binding"
BINDING_ROW_GUARD_TRIGGER = "ptg2_frozen_source_file_binding_row_guard"
BINDING_TRUNCATE_GUARD_TRIGGER = (
    "ptg2_frozen_source_file_binding_truncate_guard"
)
_CREATE_BINDING_TABLE_SQL = """
CREATE TABLE {table} (
    source_file_import_id varchar(64) PRIMARY KEY,
    internal_run_id varchar(96) NOT NULL UNIQUE,
    binding_contract varchar(64) NOT NULL,
    frozen_rate_file_set_contract varchar(64) NOT NULL,
    frozen_rate_file_set_sha256 varchar(64) NOT NULL,
    frozen_rate_file_count integer NOT NULL,
    source_key varchar(96) NOT NULL,
    import_month date NOT NULL,
    plan_ids jsonb NOT NULL,
    plan_market_types jsonb NOT NULL,
    binding_sha256 varchar(64) NOT NULL,
    binding_payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT {text_check}
        CHECK (
            btrim(source_file_import_id) <> ''
            AND btrim(source_key) <> ''
            AND internal_run_id = 'ptg2:' || source_file_import_id
            AND binding_contract = 'ptg_frozen_source_file_binding_v1'
            AND frozen_rate_file_set_contract =
                'ptg_frozen_rate_file_set_v1'
        ),
    CONSTRAINT {digest_check}
        CHECK (
            frozen_rate_file_set_sha256 ~ '^[0-9a-f]{{64}}$'
            AND binding_sha256 ~ '^[0-9a-f]{{64}}$'
        ),
    CONSTRAINT {count_check}
        CHECK (frozen_rate_file_count BETWEEN 2 AND 128),
    CONSTRAINT {month_check}
        CHECK (import_month = date_trunc('month', import_month)::date),
    CONSTRAINT {array_check}
        CHECK (
            jsonb_typeof(plan_ids) = 'array'
            AND jsonb_typeof(plan_market_types) = 'array'
        ),
    CONSTRAINT {payload_check}
        CHECK (
            binding_payload->>'contract' = binding_contract
            AND binding_payload->>'source_file_import_id' =
                source_file_import_id
            AND binding_payload->>'frozen_rate_file_set_contract' =
                frozen_rate_file_set_contract
            AND binding_payload->>'frozen_rate_file_set_sha256' =
                frozen_rate_file_set_sha256
            AND (binding_payload->>'frozen_rate_file_count')::integer =
                frozen_rate_file_count
            AND binding_payload->>'source_key' = source_key
            AND (binding_payload->>'import_month')::date = import_month
            AND binding_payload->'plan_ids' = plan_ids
            AND binding_payload->'plan_market_types' = plan_market_types
        )
)
"""


def _q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _qt(schema: str, table: str) -> str:
    return f"{_q(schema)}.{_q(table)}"


def _create_binding_table(operations: Any, schema: str) -> None:
    """Create the durable relation with coordinate and payload invariants."""

    table = _qt(schema, BINDING_TABLE)
    operations.execute(
        _CREATE_BINDING_TABLE_SQL.format(
            table=table,
            text_check=_q("ptg2_frozen_binding_text_check"),
            digest_check=_q("ptg2_frozen_binding_digest_check"),
            count_check=_q("ptg2_frozen_binding_count_check"),
            month_check=_q("ptg2_frozen_binding_month_check"),
            array_check=_q("ptg2_frozen_binding_array_check"),
            payload_check=_q("ptg2_frozen_binding_payload_check"),
        )
    )


def _create_immutable_guards(operations: Any, schema: str) -> None:
    table = _qt(schema, BINDING_TABLE)
    guard_function = _qt(schema, BINDING_GUARD_FUNCTION)
    operations.execute(
        f"""
        CREATE FUNCTION {guard_function}()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            RAISE EXCEPTION
                'PTG2_FROZEN_SOURCE_FILE_BINDING_IMMUTABLE'
                USING ERRCODE = 'P0001';
        END;
        $$
        """
    )
    operations.execute(
        f"""
        CREATE TRIGGER {_q(BINDING_ROW_GUARD_TRIGGER)}
        BEFORE UPDATE OR DELETE ON {table}
        FOR EACH ROW EXECUTE FUNCTION {guard_function}()
        """
    )
    operations.execute(
        f"""
        ALTER TABLE {table}
        ENABLE ALWAYS TRIGGER {_q(BINDING_ROW_GUARD_TRIGGER)}
        """
    )
    operations.execute(
        f"""
        CREATE TRIGGER {_q(BINDING_TRUNCATE_GUARD_TRIGGER)}
        BEFORE TRUNCATE ON {table}
        FOR EACH STATEMENT EXECUTE FUNCTION {guard_function}()
        """
    )
    operations.execute(
        f"""
        ALTER TABLE {table}
        ENABLE ALWAYS TRIGGER {_q(BINDING_TRUNCATE_GUARD_TRIGGER)}
        """
    )


def install_frozen_source_file_binding(
    operations: Any,
    schema: str,
) -> None:
    """Create the immutable source-file binding relation and guards."""

    _create_binding_table(operations, schema)
    _create_immutable_guards(operations, schema)


def uninstall_frozen_source_file_binding(
    operations: Any,
    schema: str,
) -> None:
    """Remove an unused relation during an explicit downgrade only."""

    table = _qt(schema, BINDING_TABLE)
    guard_function = _qt(schema, BINDING_GUARD_FUNCTION)
    operations.execute(f"LOCK TABLE {table} IN ACCESS EXCLUSIVE MODE")
    operations.execute(
        f"""
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM {table}) THEN
                RAISE EXCEPTION
                    'PTG2_FROZEN_SOURCE_FILE_BINDING_DOWNGRADE_REFUSED'
                    USING ERRCODE = '55000';
            END IF;
        END;
        $$
        """
    )
    operations.execute(
        f"DROP TRIGGER {_q(BINDING_TRUNCATE_GUARD_TRIGGER)} ON {table}"
    )
    operations.execute(
        f"DROP TRIGGER {_q(BINDING_ROW_GUARD_TRIGGER)} ON {table}"
    )
    operations.execute(f"DROP FUNCTION {guard_function}()")
    operations.execute(f"DROP TABLE {table}")


__all__ = [
    "BINDING_GUARD_FUNCTION",
    "BINDING_ROW_GUARD_TRIGGER",
    "BINDING_TABLE",
    "BINDING_TRUNCATE_GUARD_TRIGGER",
    "install_frozen_source_file_binding",
    "uninstall_frozen_source_file_binding",
]
