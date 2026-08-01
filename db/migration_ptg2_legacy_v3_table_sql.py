# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Rendered table SQL for the legacy V3 reconciliation authority."""

from __future__ import annotations

import json


_CAPABILITY_TABLE_TEMPLATE = """
CREATE TABLE IF NOT EXISTS {capability} (
    service_name varchar(32) NOT NULL,
    protocol_version varchar(64) NOT NULL,
    lock_namespace varchar(96) NOT NULL,
    hash_seed integer NOT NULL,
    database_name text NOT NULL,
    installed_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    CONSTRAINT {primary_key} PRIMARY KEY (service_name),
    CONSTRAINT {service_check}
        CHECK (
            service_name IN (
                '{healthcare_service}',
                '{attempt_authority_service}'
            )
        ),
    CONSTRAINT {protocol_check}
        CHECK (protocol_version = '{protocol}'),
    CONSTRAINT {namespace_check}
        CHECK (lock_namespace = '{lock_namespace}'),
    CONSTRAINT {seed_check} CHECK (hash_seed = 0)
)
"""

_CAPABILITY_INSERT_TEMPLATE = """
INSERT INTO {capability} (
    service_name,
    protocol_version,
    lock_namespace,
    hash_seed,
    database_name
)
VALUES (
    '{healthcare_service}',
    '{protocol}',
    '{lock_namespace}',
    0,
    current_database()
)
ON CONFLICT (service_name) DO NOTHING
"""

_CAPABILITY_VALIDATE_TEMPLATE = """
DO $block$
BEGIN
    IF NOT EXISTS (
        SELECT 1
          FROM {capability}
         WHERE service_name = '{healthcare_service}'
           AND protocol_version = '{protocol}'
           AND lock_namespace = '{lock_namespace}'
           AND hash_seed = 0
           AND database_name = current_database()
    ) THEN
        RAISE EXCEPTION 'PTG_SOURCE_ATTEMPT_CAPABILITY_CONFLICT'
            USING ERRCODE = '55000';
    END IF;
END;
$block$
"""


_CAPABILITY_COLUMN_CONTRACT = [
    ["service_name", "character varying(32)", True, ""],
    ["protocol_version", "character varying(64)", True, ""],
    ["lock_namespace", "character varying(96)", True, ""],
    ["hash_seed", "integer", True, ""],
    ["database_name", "text", True, ""],
    [
        "installed_at",
        "timestamp with time zone",
        True,
        "statement_timestamp()",
    ],
]


_CAPABILITY_LOCK_TEMPLATE = """
LOCK TABLE {capability} IN SHARE ROW EXCLUSIVE MODE
"""


_CAPABILITY_SHAPE_VALIDATE_TEMPLATE = """
DO $block$
DECLARE
    table_oid oid := to_regclass({capability_regclass});
    actual_columns jsonb;
    actual_constraint_definitions jsonb;
BEGIN
    IF table_oid IS NULL OR NOT EXISTS (
        SELECT 1
          FROM pg_class
         WHERE oid = table_oid
           AND relkind = 'r'
           AND relpersistence = 'p'
           AND NOT relrowsecurity
           AND NOT relforcerowsecurity
    ) THEN
        RAISE EXCEPTION 'PTG_SOURCE_ATTEMPT_CAPABILITY_SHAPE_CONFLICT'
            USING ERRCODE = '55000';
    END IF;

    SELECT jsonb_agg(
               jsonb_build_array(
                   attribute.attname,
                   format_type(attribute.atttypid, attribute.atttypmod),
                   attribute.attnotnull,
                   COALESCE(
                       pg_get_expr(default_value.adbin, default_value.adrelid),
                       ''
                   )
               )
               ORDER BY attribute.attnum
           )
      INTO actual_columns
      FROM pg_attribute AS attribute
      LEFT JOIN pg_attrdef AS default_value
        ON default_value.adrelid = attribute.attrelid
       AND default_value.adnum = attribute.attnum
     WHERE attribute.attrelid = table_oid
       AND attribute.attnum > 0
       AND NOT attribute.attisdropped;
    IF actual_columns IS DISTINCT FROM {expected_columns}::jsonb THEN
        RAISE EXCEPTION 'PTG_SOURCE_ATTEMPT_CAPABILITY_SHAPE_CONFLICT'
            USING ERRCODE = '55000';
    END IF;

    SELECT jsonb_object_agg(
               constraint_row.conname,
               btrim(
                   regexp_replace(
                       pg_get_constraintdef(constraint_row.oid, false),
                       '[[:space:]]+',
                       ' ',
                       'g'
                   )
               )
           )
      INTO actual_constraint_definitions
      FROM pg_constraint AS constraint_row
     WHERE constraint_row.conrelid = table_oid
       AND constraint_row.contype IN ('p', 'c');
    IF actual_constraint_definitions IS DISTINCT FROM
       {expected_constraints}::jsonb THEN
        RAISE EXCEPTION 'PTG_SOURCE_ATTEMPT_CAPABILITY_SHAPE_CONFLICT'
            USING ERRCODE = '55000';
    END IF;

    IF (
        SELECT COUNT(*)
          FROM pg_constraint
         WHERE conrelid = table_oid
           AND contype IN ('p', 'c')
    ) <> 5 OR EXISTS (
        SELECT 1
          FROM pg_constraint
         WHERE conrelid = table_oid
           AND contype NOT IN ('p', 'c', 'n')
    ) OR EXISTS (
        SELECT 1
          FROM pg_constraint
         WHERE conrelid = table_oid
           AND contype IN ('p', 'c')
           AND NOT convalidated
    ) OR (
        SELECT COUNT(*)
          FROM pg_index
         WHERE indrelid = table_oid
    ) <> 1 OR EXISTS (
        SELECT 1
          FROM pg_trigger
         WHERE tgrelid = table_oid
           AND NOT tgisinternal
    ) THEN
        RAISE EXCEPTION 'PTG_SOURCE_ATTEMPT_CAPABILITY_SHAPE_CONFLICT'
            USING ERRCODE = '55000';
    END IF;
END;
$block$
"""

_EVENT_TABLE_TEMPLATE = """
CREATE TABLE {event} (
    event_id bigint GENERATED ALWAYS AS IDENTITY,
    protocol_version varchar(64) NOT NULL,
    source_file_import_id varchar(64) NOT NULL,
    event_kind varchar(32) NOT NULL,
    outer_run_id varchar(64) NOT NULL,
    attempt_id varchar(160),
    state_digest varchar(64) NOT NULL,
    created_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    CONSTRAINT {primary_key} PRIMARY KEY (event_id),
    CONSTRAINT {protocol_check}
        CHECK (protocol_version = '{protocol}'),
    CONSTRAINT {source_check}
        CHECK (source_file_import_id = btrim(source_file_import_id)
               AND source_file_import_id <> ''),
    CONSTRAINT {outer_run_check}
        CHECK (outer_run_id = btrim(outer_run_id)
               AND outer_run_id <> ''),
    CONSTRAINT {kind_check}
        CHECK (event_kind IN (
            'start_admitted',
            'retry_admitted',
            'ensure_admitted',
            'finalize_admitted',
            'worker_start_admitted'
        )),
    CONSTRAINT {digest_check}
        CHECK (state_digest ~ '^[0-9a-f]{{64}}$')
)
"""

_AUDIT_TABLE_TEMPLATE = """
CREATE TABLE {audit} (
    reconciliation_id varchar(64) NOT NULL,
    contract varchar(64) NOT NULL,
    source_file_import_id varchar(64) NOT NULL,
    snapshot_id varchar(96) NOT NULL,
    internal_run_id varchar(96) NOT NULL,
    outer_run_id varchar(64) NOT NULL,
    target_digest varchar(64) NOT NULL,
    plan_digest varchar(64) NOT NULL,
    attachment_digest varchar(64) NOT NULL,
    catalog_digest varchar(64) NOT NULL,
    event_high_water_mark bigint NOT NULL,
    marker jsonb NOT NULL,
    reconciled_at timestamptz NOT NULL DEFAULT statement_timestamp(),
    CONSTRAINT {primary_key} PRIMARY KEY (reconciliation_id),
    CONSTRAINT {source_key} UNIQUE (source_file_import_id),
    CONSTRAINT {snapshot_key} UNIQUE (snapshot_id),
    CONSTRAINT {run_key} UNIQUE (internal_run_id),
    CONSTRAINT {contract_check} CHECK (contract = '{contract}'),
    CONSTRAINT {source_check}
        CHECK (source_file_import_id = btrim(source_file_import_id)
               AND source_file_import_id <> ''),
    CONSTRAINT {digest_check}
        CHECK (
            reconciliation_id ~ '^[0-9a-f]{{64}}$'
            AND target_digest ~ '^[0-9a-f]{{64}}$'
            AND plan_digest ~ '^[0-9a-f]{{64}}$'
            AND attachment_digest ~ '^[0-9a-f]{{64}}$'
            AND catalog_digest ~ '^[0-9a-f]{{64}}$'
        ),
    CONSTRAINT {event_check} CHECK (event_high_water_mark >= 0),
    CONSTRAINT {marker_check}
        CHECK (
            jsonb_typeof(marker) = 'object'
            AND COALESCE((
                marker->>'contract' = '{contract}'
                AND marker->>'source_file_import_id' = source_file_import_id
                AND marker->>'snapshot_id' = snapshot_id
                AND marker->>'internal_run_id' = internal_run_id
                AND marker->>'outer_run_id' = outer_run_id
                AND marker->>'target_digest' = target_digest
                AND marker->>'plan_digest' = plan_digest
                AND marker->>'attachment_digest' = attachment_digest
                AND marker->>'catalog_digest' = catalog_digest
                AND (marker->>'event_high_water_mark')::bigint
                    = event_high_water_mark
                AND marker->>'retained_state_digest'
                    ~ '^[0-9a-f]{{64}}$'
                AND marker->>'preserved_row_digest'
                    ~ '^[0-9a-f]{{64}}$'
            ), false)
        )
)
"""


def _capability_constraint_definitions(
    constraint_by_name: dict[str, str],
    *,
    protocol: str,
    lock_namespace: str,
    healthcare_service: str,
    attempt_authority_service: str,
) -> dict[str, str]:
    """Return the exact PostgreSQL constraint shape by physical name."""

    constraint_name_by_role = {
        role: quoted_name[1:-1].replace('""', '"')
        for role, quoted_name in constraint_by_name.items()
    }
    return {
        constraint_name_by_role["namespace_check"]: (
            "CHECK (((lock_namespace)::text = "
            f"'{lock_namespace}'::text))"
        ),
        constraint_name_by_role["primary_key"]: "PRIMARY KEY (service_name)",
        constraint_name_by_role["protocol_check"]: (
            "CHECK (((protocol_version)::text = "
            f"'{protocol}'::text))"
        ),
        constraint_name_by_role["seed_check"]: "CHECK ((hash_seed = 0))",
        constraint_name_by_role["service_check"]: (
            "CHECK (((service_name)::text = ANY "
            f"((ARRAY['{healthcare_service}'::character varying, "
            f"'{attempt_authority_service}'::character varying])::text[])))"
        ),
    }


def capability_table_statements(
    *,
    capability: str,
    protocol: str,
    lock_namespace: str,
    healthcare_service: str,
    attempt_authority_service: str,
    constraint_by_name: dict[str, str],
) -> tuple[str, ...]:
    """Render creation, publication, and exact-row validation SQL."""

    substitution_by_name = {
        "capability": capability,
        "protocol": protocol,
        "lock_namespace": lock_namespace,
        "healthcare_service": healthcare_service,
        "attempt_authority_service": attempt_authority_service,
        **constraint_by_name,
    }
    expected_definition_by_name = _capability_constraint_definitions(
        constraint_by_name,
        protocol=protocol,
        lock_namespace=lock_namespace,
        healthcare_service=healthcare_service,
        attempt_authority_service=attempt_authority_service,
    )
    shape_validation = _CAPABILITY_SHAPE_VALIDATE_TEMPLATE.format(
        capability=capability,
        capability_regclass=_sql_literal(capability),
        expected_columns=_sql_literal(
            json.dumps(_CAPABILITY_COLUMN_CONTRACT, separators=(",", ":"))
        ),
        expected_constraints=_sql_literal(
            json.dumps(
                expected_definition_by_name,
                sort_keys=True,
                separators=(",", ":"),
            )
        ),
    )
    return (
        _CAPABILITY_TABLE_TEMPLATE.format(**substitution_by_name),
        _CAPABILITY_LOCK_TEMPLATE.format(capability=capability),
        shape_validation,
        _CAPABILITY_INSERT_TEMPLATE.format(**substitution_by_name),
        _CAPABILITY_VALIDATE_TEMPLATE.format(**substitution_by_name),
    )


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def event_table_sql(
    *,
    event: str,
    protocol: str,
    constraint_by_name: dict[str, str],
) -> str:
    """Render the append-only action-event catalog."""

    return _EVENT_TABLE_TEMPLATE.format(
        event=event,
        protocol=protocol,
        **constraint_by_name,
    )


def audit_table_sql(
    *,
    audit: str,
    contract: str,
    constraint_by_name: dict[str, str],
) -> str:
    """Render the append-only reconciliation catalog."""

    return _AUDIT_TABLE_TEMPLATE.format(
        audit=audit,
        contract=contract,
        **constraint_by_name,
    )


__all__ = [
    "audit_table_sql",
    "capability_table_statements",
    "event_table_sql",
]
