# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Database contract for terminal legacy PTG V3 metadata reconciliation."""

from __future__ import annotations

from typing import Any

from db.migration_ptg2_legacy_v3_guard_sql import (
    common_attempt_guard_sql,
    source_attempt_guard_sql,
)
from db.migration_ptg2_legacy_v3_table_sql import (
    audit_table_sql,
    capability_table_statements,
    event_table_sql,
)


PTG_SOURCE_ATTEMPT_PROTOCOL = "ptg_source_attempt_fence_v1"
PTG_SOURCE_ATTEMPT_LOCK_NAMESPACE = "ptg-source-import:-1-attempt"
LEGACY_V3_RECONCILE_CONTRACT = "ptg2_legacy_v3_metadata_reconcile_v1"
CAPABILITY_TABLE = "ptg_source_attempt_guard_capability"
EVENT_TABLE = "ptg_source_attempt_event"
AUDIT_TABLE = "ptg2_legacy_v3_metadata_reconcile_audit"
HEALTHCARE_SERVICE_NAME = "healthcare-mrf-api"
ATTEMPT_AUTHORITY_SERVICE_NAME = "source-attempt-authority"


def _q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _qt(schema: str, table_name: str) -> str:
    return f"{_q(schema)}.{_q(table_name)}"


def _function(schema: str, function_name: str) -> str:
    return f"{_q(schema)}.{_q(function_name)}"


def _append_only_guard_sql(schema: str) -> str:
    function_name = _function(
        schema,
        "guard_ptg_source_attempt_append_only",
    )
    return f"""
        CREATE OR REPLACE FUNCTION {function_name}()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            RAISE EXCEPTION 'PTG_SOURCE_ATTEMPT_AUDIT_IMMUTABLE'
                USING ERRCODE = 'P0001';
        END;
        $$
    """


def _install_append_only_triggers(
    operations: Any,
    schema: str,
    table_name: str,
) -> None:
    table = _qt(schema, table_name)
    function_name = _function(
        schema,
        "guard_ptg_source_attempt_append_only",
    )
    row_trigger = _q(f"{table_name}_append_only_row_guard")
    truncate_trigger = _q(f"{table_name}_append_only_truncate_guard")
    operations.execute(
        f"DROP TRIGGER IF EXISTS {row_trigger} ON {table}"
    )
    operations.execute(
        f"""
        CREATE TRIGGER {row_trigger}
        BEFORE UPDATE OR DELETE ON {table}
        FOR EACH ROW EXECUTE FUNCTION {function_name}()
        """
    )
    operations.execute(
        f"ALTER TABLE {table} ENABLE ALWAYS TRIGGER {row_trigger}"
    )
    operations.execute(
        f"DROP TRIGGER IF EXISTS {truncate_trigger} ON {table}"
    )
    operations.execute(
        f"""
        CREATE TRIGGER {truncate_trigger}
        BEFORE TRUNCATE ON {table}
        FOR EACH STATEMENT EXECUTE FUNCTION {function_name}()
        """
    )
    operations.execute(
        f"ALTER TABLE {table} ENABLE ALWAYS TRIGGER {truncate_trigger}"
    )


def _install_capability_table(operations: Any, schema: str) -> None:
    """Publish healthcare's exact shared-attempt capability row."""

    capability = _qt(schema, CAPABILITY_TABLE)
    statements = capability_table_statements(
        capability=capability,
        protocol=PTG_SOURCE_ATTEMPT_PROTOCOL,
        lock_namespace=PTG_SOURCE_ATTEMPT_LOCK_NAMESPACE,
        healthcare_service=HEALTHCARE_SERVICE_NAME,
        attempt_authority_service=ATTEMPT_AUTHORITY_SERVICE_NAME,
        constraint_by_name={
            "primary_key": _q(CAPABILITY_TABLE + "_pkey"),
            "service_check": _q(CAPABILITY_TABLE + "_service_check"),
            "protocol_check": _q(CAPABILITY_TABLE + "_protocol_check"),
            "namespace_check": _q(CAPABILITY_TABLE + "_namespace_check"),
            "seed_check": _q(CAPABILITY_TABLE + "_seed_check"),
        },
    )
    for statement in statements:
        operations.execute(statement)


def _install_event_table(operations: Any, schema: str) -> None:
    event = _qt(schema, EVENT_TABLE)
    operations.execute(
        event_table_sql(
            event=event,
            protocol=PTG_SOURCE_ATTEMPT_PROTOCOL,
            constraint_by_name={
                "primary_key": _q(EVENT_TABLE + "_pkey"),
                "protocol_check": _q(EVENT_TABLE + "_protocol_check"),
                "source_check": _q(EVENT_TABLE + "_source_check"),
                "outer_run_check": _q(EVENT_TABLE + "_outer_run_check"),
                "kind_check": _q(EVENT_TABLE + "_kind_check"),
                "digest_check": _q(EVENT_TABLE + "_state_digest_check"),
            },
        )
    )
    operations.execute(
        f"CREATE INDEX {_q(EVENT_TABLE + '_source_event_idx')} "
        f"ON {event} (source_file_import_id, event_id)"
    )
    _install_append_only_triggers(operations, schema, EVENT_TABLE)


def _install_audit_table(operations: Any, schema: str) -> None:
    """Install the immutable audit table with digest-bound markers."""

    audit = _qt(schema, AUDIT_TABLE)
    operations.execute(
        audit_table_sql(
            audit=audit,
            contract=LEGACY_V3_RECONCILE_CONTRACT,
            constraint_by_name={
                "primary_key": _q(AUDIT_TABLE + "_pkey"),
                "source_key": _q(AUDIT_TABLE + "_source_key"),
                "snapshot_key": _q(AUDIT_TABLE + "_snapshot_key"),
                "run_key": _q(AUDIT_TABLE + "_run_key"),
                "contract_check": _q(AUDIT_TABLE + "_contract_check"),
                "source_check": _q(AUDIT_TABLE + "_source_check"),
                "digest_check": _q(AUDIT_TABLE + "_digest_check"),
                "event_check": _q(AUDIT_TABLE + "_event_mark_check"),
                "marker_check": _q(AUDIT_TABLE + "_marker_check"),
            },
        )
    )
    _install_append_only_triggers(operations, schema, AUDIT_TABLE)


def _install_attempt_guard_function(operations: Any, schema: str) -> None:
    audit = _qt(schema, AUDIT_TABLE)
    function_name = _function(schema, "guard_ptg_source_attempt")
    operations.execute(
        source_attempt_guard_sql(
            function_name=function_name,
            audit=audit,
            lock_namespace=PTG_SOURCE_ATTEMPT_LOCK_NAMESPACE,
        )
    )


def _install_common_attempt_guard(operations: Any, schema: str) -> None:
    """Extend the common coordinate guard without changing V4 behavior."""

    snapshot = _qt(schema, "ptg2_snapshot")
    internal_run = _qt(schema, "ptg2_import_run")
    fence = _qt(schema, "ptg2_v4_attempt_fence")
    legacy_audit = _qt(schema, AUDIT_TABLE)
    guard = _function(schema, "guard_ptg2_v4_attempt")
    operations.execute(
        common_attempt_guard_sql(
            guard=guard,
            legacy_audit=legacy_audit,
            snapshot=snapshot,
            internal_run=internal_run,
            fence=fence,
        )
    )


def install_legacy_v3_reconcile_contract(
    operations: Any,
    schema: str,
) -> None:
    """Install immutable ledgers and the source-attempt writer fence."""

    operations.execute(_append_only_guard_sql(schema))
    _install_capability_table(operations, schema)
    _install_event_table(operations, schema)
    _install_audit_table(operations, schema)
    _install_attempt_guard_function(operations, schema)
    _install_common_attempt_guard(operations, schema)


def refuse_legacy_v3_downgrade(
    operations: Any,
    schema: str,
) -> None:
    """Refuse removal whenever immutable evidence has been recorded."""

    event = _qt(schema, EVENT_TABLE)
    audit = _qt(schema, AUDIT_TABLE)
    capability = _qt(schema, CAPABILITY_TABLE)
    operations.execute(
        f"LOCK TABLE {event}, {audit}, {capability} "
        "IN SHARE ROW EXCLUSIVE MODE"
    )
    operations.execute(
        f"""
        DO $block$
        DECLARE
            event_count bigint;
            audit_count bigint;
            evidence_digest text;
        BEGIN
            SELECT COUNT(*) INTO event_count FROM {event};
            SELECT COUNT(*) INTO audit_count FROM {audit};
            SELECT md5(event_count::text || ':' || audit_count::text)
              INTO evidence_digest;
            IF event_count <> 0 OR audit_count <> 0 THEN
                RAISE EXCEPTION
                    'PTG_SOURCE_ATTEMPT_DOWNGRADE_REFUSED events=%, audits=%, evidence=%',
                    event_count,
                    audit_count,
                    evidence_digest
                    USING ERRCODE = '55000';
            END IF;
            IF EXISTS (
                SELECT 1 FROM {capability}
                 WHERE service_name = '{ATTEMPT_AUTHORITY_SERVICE_NAME}'
            ) THEN
                RAISE EXCEPTION
                    'PTG_SOURCE_ATTEMPT_DOWNGRADE_REFUSED peer capability active'
                    USING ERRCODE = '55000';
            END IF;
        END;
        $block$
        """
    )


__all__ = [
    "AUDIT_TABLE",
    "ATTEMPT_AUTHORITY_SERVICE_NAME",
    "CAPABILITY_TABLE",
    "EVENT_TABLE",
    "HEALTHCARE_SERVICE_NAME",
    "LEGACY_V3_RECONCILE_CONTRACT",
    "PTG_SOURCE_ATTEMPT_LOCK_NAMESPACE",
    "PTG_SOURCE_ATTEMPT_PROTOCOL",
    "install_legacy_v3_reconcile_contract",
    "refuse_legacy_v3_downgrade",
]
