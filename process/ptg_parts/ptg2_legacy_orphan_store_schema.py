# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Authority-schema validation for legacy PTG cleanup."""

from __future__ import annotations

import re
from typing import Any, Mapping

from process.ptg_parts.ptg2_legacy_orphan_contract import (
    LEGACY_SWEEP_AUDIT_TABLE,
    canonical_sha256,
)
from process.ptg_parts.ptg2_legacy_orphan_models import (
    LEGACY_SWEEP_MAX_RELATIONS,
)
from process.ptg_parts.ptg2_legacy_orphan_store_common import (
    _CONTROL_REQUIRED_TABLES,
    _EMBEDDED_RELATION_PATTERN,
    _MRF_REQUIRED_TABLES,
    _catalog_json_value,
    _catalog_text,
    _row_mapping,
    _schema_table,
    LegacyAuthorityCatalog,
)

_AUTHORITY_CATALOG_SQL = """
    SELECT namespace_record.nspname AS table_schema,
           relation_record.relname AS table_name,
           relation_record.oid::bigint AS relation_oid,
           relation_record.relowner::bigint AS owner_oid,
           relation_record.relkind,
           relation_record.relpersistence,
           COALESCE(
               (
                   SELECT jsonb_agg(
                              jsonb_build_array(
                                  attribute_record.attnum,
                                  attribute_record.attname,
                                  attribute_record.atttypid::bigint,
                                  attribute_record.atttypmod,
                                  attribute_record.attnotnull
                              )
                              ORDER BY attribute_record.attnum
                          )
                     FROM pg_attribute AS attribute_record
                    WHERE attribute_record.attrelid = relation_record.oid
                      AND attribute_record.attnum > 0
                      AND NOT attribute_record.attisdropped
               ),
               '[]'::jsonb
           ) AS column_shape
      FROM pg_class AS relation_record
      JOIN pg_namespace AS namespace_record
        ON namespace_record.oid = relation_record.relnamespace
     WHERE (
            namespace_record.nspname = :schema_name
            AND relation_record.relname = ANY(CAST(:mrf_tables AS text[]))
           )
        OR (
            namespace_record.nspname = :control_schema_name
            AND relation_record.relname = ANY(CAST(:control_tables AS text[]))
           )
     ORDER BY table_schema, table_name
"""

_AUDIT_TRIGGER_SQL = """
    SELECT trigger_record.tgname,
           trigger_record.tgtype,
           trigger_record.tgenabled,
           function_record.oid::bigint AS function_oid,
           function_record.proname,
           function_schema.nspname AS function_schema,
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
     WHERE namespace_record.nspname = :schema_name
       AND relation_record.relname = :audit_table
       AND NOT trigger_record.tgisinternal
     ORDER BY trigger_record.tgname
"""

_EXPECTED_AUDIT_TRIGGER_BODY = re.sub(
    r"\s+",
    "",
    """
    BEGIN
        RAISE EXCEPTION 'PTG2_LEGACY_SWEEP_AUDIT_IMMUTABLE'
            USING ERRCODE = 'P0001';
    END;
    """,
)


def _validate_authority_relations(
    relation_rows: list[Any],
    *,
    schema_name: str,
    control_schema_name: str,
) -> None:
    present_names = {
        (
            str(_row_mapping(relation_row)["table_schema"]),
            str(_row_mapping(relation_row)["table_name"]),
        )
        for relation_row in relation_rows
    }
    expected_names = {
        *((schema_name, table_name) for table_name in _MRF_REQUIRED_TABLES),
        *(
            (control_schema_name, table_name)
            for table_name in _CONTROL_REQUIRED_TABLES
        ),
    }
    missing_names = sorted(expected_names - present_names)
    if missing_names:
        formatted_names = ", ".join(
            f"{schema}.{table}" for schema, table in missing_names
        )
        raise RuntimeError(
            f"legacy_sweep_required_relations_missing:{formatted_names}"
        )
    invalid_names = [
        f"{mapping['table_schema']}.{mapping['table_name']}"
        for relation_row in relation_rows
        for mapping in (_row_mapping(relation_row),)
        if _catalog_text(mapping["relkind"]) not in {"p", "r"}
        or _catalog_text(mapping["relpersistence"]) not in {"p", "u"}
    ]
    if invalid_names:
        raise RuntimeError(
            "legacy_sweep_authority_catalog_invalid:"
            + ",".join(sorted(invalid_names))
        )


def _validated_audit_triggers(
    trigger_rows: list[Any],
    schema_name: str,
) -> tuple[dict[str, Any], ...]:
    if len(trigger_rows) != 2:
        raise RuntimeError("legacy_sweep_audit_guard_invalid")
    expected_types_by_name = {
        "ptg2_legacy_orphan_sweep_audit_row_guard": 27,
        "ptg2_legacy_orphan_sweep_audit_truncate_guard": 34,
    }
    validated_triggers: list[dict[str, Any]] = []
    for trigger_row in trigger_rows:
        trigger_by_field = dict(_row_mapping(trigger_row))
        expected_type = expected_types_by_name.get(
            str(trigger_by_field["tgname"])
        )
        is_valid = (
            expected_type is not None
            and int(trigger_by_field["tgtype"]) == expected_type
            and _catalog_text(trigger_by_field["tgenabled"]) == "A"
            and trigger_by_field["proname"]
            == "guard_ptg2_legacy_orphan_sweep_audit"
            and trigger_by_field["function_schema"] == schema_name
            and re.sub(r"\s+", "", str(trigger_by_field["prosrc"]))
            == _EXPECTED_AUDIT_TRIGGER_BODY
        )
        if not is_valid:
            raise RuntimeError("legacy_sweep_audit_guard_invalid")
        validated_triggers.append(trigger_by_field)
    if {trigger["tgname"] for trigger in validated_triggers} != set(
        expected_types_by_name
    ):
        raise RuntimeError("legacy_sweep_audit_guard_invalid")
    return tuple(validated_triggers)


async def require_legacy_sweep_schema(
    executor: Any,
    *,
    schema_name: str,
    control_schema_name: str,
) -> LegacyAuthorityCatalog:
    """Require every lifecycle authority before inspecting candidates."""

    relation_rows = await executor.all(
        _AUTHORITY_CATALOG_SQL,
        schema_name=schema_name,
        control_schema_name=control_schema_name,
        mrf_tables=list(_MRF_REQUIRED_TABLES),
        control_tables=list(_CONTROL_REQUIRED_TABLES),
    )
    _validate_authority_relations(
        relation_rows,
        schema_name=schema_name,
        control_schema_name=control_schema_name,
    )
    trigger_rows = await executor.all(
        _AUDIT_TRIGGER_SQL,
        schema_name=schema_name,
        audit_table=LEGACY_SWEEP_AUDIT_TABLE,
    )
    trigger_rows_by_field = _validated_audit_triggers(
        trigger_rows,
        schema_name,
    )
    authority_payload_by_field = {
        "relations": [
            _catalog_json_value(dict(_row_mapping(relation_row)))
            for relation_row in relation_rows
        ],
        "audit_guards": _catalog_json_value(trigger_rows_by_field),
    }
    return LegacyAuthorityCatalog(
        catalog_digest=canonical_sha256(authority_payload_by_field),
        relation_oids=tuple(
            sorted(
                int(_row_mapping(relation_row)["relation_oid"])
                for relation_row in relation_rows
            )
        ),
    )


async def _base_catalog_identity(
    executor: Any,
    schema_name: str,
) -> tuple[int, int]:
    row = await executor.first(
        """
        SELECT namespace_record.oid::bigint AS namespace_oid,
               relation_record.relowner::bigint AS owner_oid
          FROM pg_class AS relation_record
          JOIN pg_namespace AS namespace_record
            ON namespace_record.oid = relation_record.relnamespace
         WHERE namespace_record.nspname = :schema_name
           AND relation_record.relname = 'ptg2_snapshot'
           AND relation_record.relkind = 'r'
        """,
        schema_name=schema_name,
    )
    if row is None:
        raise RuntimeError(
            f"legacy_sweep_snapshot_catalog_missing:{schema_name}"
        )
    mapping = _row_mapping(row)
    return int(mapping["namespace_oid"]), int(mapping["owner_oid"])


async def _relation_catalog_rows(
    executor: Any,
    schema_name: str,
) -> list[Mapping[str, Any]]:
    rows = await executor.all(
        """
        SELECT relation_record.oid::bigint AS relation_oid,
               namespace_record.oid::bigint AS namespace_oid,
               relation_record.relname,
               relation_record.relkind,
               relation_record.relpersistence,
               relation_record.relowner::bigint AS owner_oid,
               pg_total_relation_size(relation_record.oid)::bigint
                   AS total_bytes
          FROM pg_class AS relation_record
          JOIN pg_namespace AS namespace_record
            ON namespace_record.oid = relation_record.relnamespace
         WHERE namespace_record.nspname = :schema_name
           AND relation_record.relname ~ :relation_pattern
         ORDER BY relation_record.relname, relation_record.oid
         LIMIT :catalog_row_limit
        """,
        schema_name=schema_name,
        relation_pattern=_EMBEDDED_RELATION_PATTERN,
        catalog_row_limit=LEGACY_SWEEP_MAX_RELATIONS + 1,
    )
    if len(rows) > LEGACY_SWEEP_MAX_RELATIONS:
        raise RuntimeError("legacy_sweep_relation_catalog_limit_exceeded")
    return [dict(_row_mapping(row)) for row in rows]
