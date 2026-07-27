# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Migration helpers for the immutable predecessor-retirement audit."""

from __future__ import annotations

from typing import Any


AUDIT_TABLE = "ptg2_predecessor_retirement_audit"
AUDIT_GUARD_FUNCTION = "guard_ptg2_predecessor_retirement_audit"
AUDIT_ROW_GUARD_TRIGGER = "ptg2_predecessor_retirement_audit_row_guard"
AUDIT_TRUNCATE_GUARD_TRIGGER = (
    "ptg2_predecessor_retirement_audit_truncate_guard"
)


def _q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _qt(schema: str, table: str) -> str:
    return f"{_q(schema)}.{_q(table)}"


def _create_predecessor_retirement_audit_table(
    operations: Any,
    schema: str,
) -> None:
    """Create the standalone audit table without snapshot foreign keys."""
    table = _qt(schema, AUDIT_TABLE)
    operations.execute(
        f"""
        CREATE TABLE {table} (
            idempotency_key varchar(160) PRIMARY KEY,
            request_digest varchar(64) NOT NULL,
            source_key varchar(96) NOT NULL,
            current_snapshot_id varchar(96) NOT NULL,
            predecessor_snapshot_id varchar(96) NOT NULL,
            rollback_pin_mode varchar(16) NOT NULL,
            rollback_owner_id varchar(96),
            actor varchar(128) NOT NULL,
            reason varchar(512) NOT NULL,
            retired_at timestamptz NOT NULL,
            cleared_source_pointer_count integer NOT NULL,
            cleared_plan_pointer_count integer NOT NULL,
            cleared_global_pointer_count integer NOT NULL,
            deleted_rollback_pin_count integer NOT NULL,
            CONSTRAINT {_q("ptg2_predecessor_retirement_audit_digest_check")}
                CHECK (request_digest ~ '^[0-9a-f]{{64}}$'),
            CONSTRAINT {_q("ptg2_predecessor_retirement_audit_text_check")}
                CHECK (
                    btrim(idempotency_key) <> ''
                    AND btrim(source_key) <> ''
                    AND btrim(current_snapshot_id) <> ''
                    AND btrim(predecessor_snapshot_id) <> ''
                    AND btrim(actor) <> ''
                    AND btrim(reason) <> ''
                ),
            CONSTRAINT {_q("ptg2_predecessor_retirement_audit_pair_check")}
                CHECK (current_snapshot_id <> predecessor_snapshot_id),
            CONSTRAINT {_q("ptg2_predecessor_retirement_audit_counts_check")}
                CHECK (
                    cleared_source_pointer_count = 1
                    AND cleared_plan_pointer_count > 0
                    AND cleared_global_pointer_count IN (0, 1)
                    AND (
                        (
                            rollback_pin_mode = 'owned'
                            AND rollback_owner_id IS NOT NULL
                            AND btrim(rollback_owner_id) <> ''
                            AND deleted_rollback_pin_count = 1
                        )
                        OR (
                            rollback_pin_mode = 'absent'
                            AND rollback_owner_id IS NULL
                            AND deleted_rollback_pin_count = 0
                        )
                    )
                )
        )
        """
    )


def _create_predecessor_retirement_audit_guard(
    operations: Any,
    schema: str,
) -> None:
    """Create the shared rejection function for every audit mutation."""

    guard_function = _qt(schema, AUDIT_GUARD_FUNCTION)
    operations.execute(
        f"""
        CREATE FUNCTION {guard_function}()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            RAISE EXCEPTION
                'PTG2_PREDECESSOR_RETIREMENT_AUDIT_IMMUTABLE'
                USING ERRCODE = 'P0001';
        END;
        $$
        """
    )


def _install_predecessor_retirement_audit_row_guard(
    operations: Any,
    schema: str,
) -> None:
    """Reject update and delete, even when ordinary triggers are disabled."""

    table = _qt(schema, AUDIT_TABLE)
    guard_function = _qt(schema, AUDIT_GUARD_FUNCTION)
    operations.execute(
        f"""
        CREATE TRIGGER {_q(AUDIT_ROW_GUARD_TRIGGER)}
        BEFORE UPDATE OR DELETE ON {table}
        FOR EACH ROW EXECUTE FUNCTION {guard_function}()
        """
    )
    operations.execute(
        f"""
        ALTER TABLE {table}
        ENABLE ALWAYS TRIGGER {_q(AUDIT_ROW_GUARD_TRIGGER)}
        """
    )


def _install_predecessor_retirement_audit_truncate_guard(
    operations: Any,
    schema: str,
) -> None:
    """Reject whole-table truncation of immutable audit evidence."""

    table = _qt(schema, AUDIT_TABLE)
    guard_function = _qt(schema, AUDIT_GUARD_FUNCTION)
    operations.execute(
        f"""
        CREATE TRIGGER {_q(AUDIT_TRUNCATE_GUARD_TRIGGER)}
        BEFORE TRUNCATE ON {table}
        FOR EACH STATEMENT EXECUTE FUNCTION {guard_function}()
        """
    )
    operations.execute(
        f"""
        ALTER TABLE {table}
        ENABLE ALWAYS TRIGGER {_q(AUDIT_TRUNCATE_GUARD_TRIGGER)}
        """
    )


def install_predecessor_retirement_audit(
    operations: Any,
    schema: str,
) -> None:
    """Create the standalone audit table and reject all later mutations."""

    _create_predecessor_retirement_audit_table(operations, schema)
    _create_predecessor_retirement_audit_guard(operations, schema)
    _install_predecessor_retirement_audit_row_guard(operations, schema)
    _install_predecessor_retirement_audit_truncate_guard(operations, schema)


def uninstall_predecessor_retirement_audit(
    operations: Any,
    schema: str,
) -> None:
    """Drop the standalone audit contract during an explicit downgrade."""

    table = _qt(schema, AUDIT_TABLE)
    guard_function = _qt(schema, AUDIT_GUARD_FUNCTION)
    operations.execute(f"LOCK TABLE {table} IN ACCESS EXCLUSIVE MODE")
    operations.execute(
        f"""
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM {table}) THEN
                RAISE EXCEPTION
                    'PTG2_PREDECESSOR_RETIREMENT_AUDIT_DOWNGRADE_REFUSED'
                    USING ERRCODE = '55000';
            END IF;
        END;
        $$
        """
    )
    operations.execute(
        f"DROP TRIGGER {_q(AUDIT_TRUNCATE_GUARD_TRIGGER)} ON {table}"
    )
    operations.execute(
        f"DROP TRIGGER {_q(AUDIT_ROW_GUARD_TRIGGER)} ON {table}"
    )
    operations.execute(f"DROP FUNCTION {guard_function}()")
    operations.execute(f"DROP TABLE {table}")


__all__ = [
    "AUDIT_GUARD_FUNCTION",
    "AUDIT_ROW_GUARD_TRIGGER",
    "AUDIT_TABLE",
    "AUDIT_TRUNCATE_GUARD_TRIGGER",
    "install_predecessor_retirement_audit",
    "uninstall_predecessor_retirement_audit",
]
