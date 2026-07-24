# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact immutable-trigger contract for the PTG V4 attempt fence."""

from __future__ import annotations

from typing import Any

import sqlalchemy as sa

from db.migration_expression_adoption import _normalized_expression
from db.ptg2_v4_attempt_schema import ATTEMPT_FENCE_TABLE


_AUDIT_TRIGGER = "ptg2_v4_attempt_fence_audit_guard"
_AUDIT_FUNCTION = "guard_ptg2_v4_attempt_audit"
_LEGACY_AUDIT_BODY = """
BEGIN
    IF TG_OP = 'DELETE' AND OLD.state = 'reconciled' THEN
        RAISE EXCEPTION
            'PTG2_RECONCILIATION_AUDIT_IMMUTABLE'
            USING ERRCODE = 'P0001';
    END IF;
    IF TG_OP = 'UPDATE' THEN
        IF OLD.state = 'reconciled' THEN
            RAISE EXCEPTION
                'PTG2_RECONCILIATION_AUDIT_IMMUTABLE'
                USING ERRCODE = 'P0001';
        END IF;
        IF NEW.state NOT IN ('active', 'reconciled') THEN
            RAISE EXCEPTION
                'PTG2_RECONCILIATION_AUDIT_STATE'
                USING ERRCODE = 'P0001';
        END IF;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END;
""".strip()
_FINAL_AUDIT_BODY = """
BEGIN
    IF TG_OP = 'DELETE' AND OLD.state = 'reconciled' THEN
        RAISE EXCEPTION
            'PTG2_RECONCILIATION_AUDIT_IMMUTABLE'
            USING ERRCODE = 'P0001';
    END IF;
    IF TG_OP = 'UPDATE' THEN
        IF OLD.snapshot_id IS DISTINCT FROM NEW.snapshot_id
           OR OLD.internal_run_id IS DISTINCT FROM NEW.internal_run_id
           OR OLD.fence_nonce IS DISTINCT FROM NEW.fence_nonce
           OR OLD.created_at IS DISTINCT FROM NEW.created_at THEN
            RAISE EXCEPTION
                'PTG2_RECONCILIATION_AUDIT_IDENTITY_IMMUTABLE'
                USING ERRCODE = 'P0001';
        END IF;
        IF OLD.state = 'reconciled' THEN
            RAISE EXCEPTION
                'PTG2_RECONCILIATION_AUDIT_IMMUTABLE'
                USING ERRCODE = 'P0001';
        END IF;
        IF NEW.state NOT IN ('active', 'reconciled') THEN
            RAISE EXCEPTION
                'PTG2_RECONCILIATION_AUDIT_STATE'
                USING ERRCODE = 'P0001';
        END IF;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END;
""".strip()


def _q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _trigger_catalog(
    connection: sa.engine.Connection,
    schema: str,
) -> list[dict[str, Any]]:
    trigger_result = connection.execute(
        sa.text(
            """
            SELECT trigger_record.tgname,
                   trigger_record.tgtype,
                   trigger_record.tgenabled,
                   function_record.proname,
                   function_schema.nspname AS function_schema,
                   language_record.lanname,
                   function_record.prosrc
              FROM pg_trigger AS trigger_record
              JOIN pg_class AS relation_record
                ON relation_record.oid = trigger_record.tgrelid
              JOIN pg_namespace AS namespace_record
                ON namespace_record.oid = relation_record.relnamespace
              JOIN pg_proc AS function_record
                ON function_record.oid = trigger_record.tgfoid
              JOIN pg_namespace AS function_schema
                ON function_schema.oid = function_record.pronamespace
              JOIN pg_language AS language_record
                ON language_record.oid = function_record.prolang
             WHERE namespace_record.nspname = :schema
               AND relation_record.relname = :table
               AND NOT trigger_record.tgisinternal
            """
        ),
        {"schema": schema, "table": ATTEMPT_FENCE_TABLE},
    )
    return [dict(trigger_mapping) for trigger_mapping in trigger_result.mappings()]


def _is_expected_trigger(
    trigger_mapping: dict[str, Any],
    schema: str,
    expected_body: str,
) -> bool:
    return (
        trigger_mapping["tgname"] == _AUDIT_TRIGGER
        and int(trigger_mapping["tgtype"]) == 27
        and trigger_mapping["tgenabled"] in {"O", b"O"}
        and trigger_mapping["proname"] == _AUDIT_FUNCTION
        and trigger_mapping["function_schema"] == schema
        and trigger_mapping["lanname"] == "plpgsql"
        and _normalized_expression(trigger_mapping["prosrc"])
        == _normalized_expression(expected_body)
    )


def validate_attempt_audit_trigger(
    connection: sa.engine.Connection,
    schema: str,
    *,
    is_legacy: bool,
) -> None:
    """Accept only the exact legacy or nonce-bound audit trigger."""

    trigger_catalog = _trigger_catalog(connection, schema)
    expected_body = _LEGACY_AUDIT_BODY if is_legacy else _FINAL_AUDIT_BODY
    if (
        len(trigger_catalog) != 1
        or not _is_expected_trigger(
            trigger_catalog[0],
            schema,
            expected_body,
        )
    ):
        raise RuntimeError(f"ptg2_v4_attempt_audit_trigger_unknown:{schema}")


def install_attempt_audit_guard(operations: Any, schema: str) -> None:
    """Install the nonce-bound immutable audit trigger."""

    quoted_schema = _q(schema)
    quoted_fence = f"{quoted_schema}.{_q(ATTEMPT_FENCE_TABLE)}"
    qualified_function = f"{quoted_schema}.{_q(_AUDIT_FUNCTION)}"
    operations.execute(
        f"""
        CREATE OR REPLACE FUNCTION {qualified_function}()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        {_FINAL_AUDIT_BODY}
        $$
        """
    )
    operations.execute(
        f"DROP TRIGGER IF EXISTS {_q(_AUDIT_TRIGGER)} ON {quoted_fence}"
    )
    operations.execute(
        f"""
        CREATE TRIGGER {_q(_AUDIT_TRIGGER)}
        BEFORE UPDATE OR DELETE ON {quoted_fence}
        FOR EACH ROW EXECUTE FUNCTION {qualified_function}()
        """
    )


__all__ = [
    "install_attempt_audit_guard",
    "validate_attempt_audit_trigger",
]
