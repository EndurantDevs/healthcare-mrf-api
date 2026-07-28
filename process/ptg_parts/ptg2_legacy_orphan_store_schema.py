# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Authority-schema validation for legacy PTG cleanup."""

from __future__ import annotations

import re
from typing import Any, Mapping

from process.ptg_parts.ptg2_legacy_orphan_contract import (
    LEGACY_SWEEP_AUDIT_TABLE,
    canonical_sha256,
)
from process.ptg_parts.ptg2_legacy_orphan_store_common import (
    _CONTROL_REQUIRED_TABLES,
    _MRF_OPTIONAL_TABLES,
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

_OPTIONAL_AUTHORITY_CATALOG_SQL = """
    WITH optional_input AS (
        SELECT optional_name.table_name,
               optional_name.qualified_name
          FROM unnest(
                   CAST(:optional_tables AS text[]),
                   CAST(:optional_qualified_names AS text[])
               ) AS optional_name(table_name, qualified_name)
    ),
    resolved_optional AS (
        SELECT optional_input.table_name AS expected_table_name,
               optional_input.qualified_name,
               to_regclass(optional_input.qualified_name) AS relation_oid
          FROM optional_input
    )
    SELECT resolved_optional.expected_table_name,
           resolved_optional.qualified_name,
           relation_record.oid IS NOT NULL AS relation_present,
           namespace_record.nspname AS table_schema,
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
      FROM resolved_optional
      LEFT JOIN pg_class AS relation_record
        ON relation_record.oid = resolved_optional.relation_oid
      LEFT JOIN pg_namespace AS namespace_record
        ON namespace_record.oid = relation_record.relnamespace
     ORDER BY resolved_optional.expected_table_name
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


def _present_optional_authority_payload_entry(
    mapping: Mapping[str, Any],
    *,
    expected_name: str,
    qualified_name: str,
    schema_name: str,
) -> tuple[dict[str, Any], str, int]:
    """Validate and fingerprint one present optional authority relation."""

    is_catalog_valid = (
        str(mapping.get("table_schema") or "") == schema_name
        and str(mapping.get("table_name") or "") == expected_name
        and mapping.get("relation_oid") is not None
        and mapping.get("owner_oid") is not None
        and _catalog_text(mapping.get("relkind")) == "r"
        and _catalog_text(mapping.get("relpersistence")) in {"p", "u"}
    )
    if not is_catalog_valid:
        raise RuntimeError(
            "legacy_sweep_optional_relations_catalog_invalid:"
            + expected_name
        )
    relation_oid = int(mapping["relation_oid"])
    return (
        {
            "table_name": expected_name,
            "qualified_name": qualified_name,
            "present": True,
            "catalog": _catalog_json_value(
                {
                    "table_schema": mapping["table_schema"],
                    "table_name": mapping["table_name"],
                    "relation_oid": relation_oid,
                    "owner_oid": int(mapping["owner_oid"]),
                    "relkind": _catalog_text(mapping["relkind"]),
                    "relpersistence": _catalog_text(
                        mapping["relpersistence"]
                    ),
                    "column_shape": mapping["column_shape"],
                }
            ),
        },
        expected_name,
        relation_oid,
    )


def _optional_authority_payload_entry(
    mapping: Mapping[str, Any],
    *,
    schema_name: str,
) -> tuple[dict[str, Any], str | None, int | None]:
    """Validate and normalize one optional relation catalog result."""

    expected_name = str(mapping["expected_table_name"])
    qualified_name = str(mapping["qualified_name"])
    if qualified_name != _schema_table(schema_name, expected_name):
        raise RuntimeError("legacy_sweep_optional_relations_probe_invalid")
    if bool(mapping["relation_present"]):
        return _present_optional_authority_payload_entry(
            mapping,
            expected_name=expected_name,
            qualified_name=qualified_name,
            schema_name=schema_name,
        )
    catalog_fields = (
        "table_schema",
        "table_name",
        "relation_oid",
        "owner_oid",
        "relkind",
        "relpersistence",
    )
    if any(mapping.get(field_name) is not None for field_name in catalog_fields):
        raise RuntimeError("legacy_sweep_optional_relations_probe_invalid")
    return (
        {
            "table_name": expected_name,
            "qualified_name": qualified_name,
            "present": False,
        },
        None,
        None,
    )


def _validated_optional_authority(
    relation_rows: list[Any],
    *,
    schema_name: str,
) -> tuple[tuple[dict[str, Any], ...], tuple[str, ...], tuple[int, ...]]:
    """Validate the exact optional probe inventory and catalog identity."""

    expected_names = set(_MRF_OPTIONAL_TABLES)
    actual_names = [
        str(_row_mapping(relation_row).get("expected_table_name") or "")
        for relation_row in relation_rows
    ]
    if len(actual_names) != len(expected_names) or set(actual_names) != expected_names:
        raise RuntimeError("legacy_sweep_optional_relations_probe_invalid")
    validated_entries = [
        _optional_authority_payload_entry(
            _row_mapping(relation_row),
            schema_name=schema_name,
        )
        for relation_row in relation_rows
    ]
    return (
        tuple(
            sorted(
                (entry[0] for entry in validated_entries),
                key=lambda entry: str(entry["table_name"]),
            )
        ),
        tuple(sorted(entry[1] for entry in validated_entries if entry[1])),
        tuple(sorted(entry[2] for entry in validated_entries if entry[2])),
    )


async def _load_optional_authority(
    executor: Any,
    *,
    schema_name: str,
) -> tuple[tuple[dict[str, Any], ...], tuple[str, ...], tuple[int, ...]]:
    """Resolve optional names with to_regclass and fingerprint present rows."""

    optional_relation_rows = await executor.all(
        _OPTIONAL_AUTHORITY_CATALOG_SQL,
        optional_tables=list(_MRF_OPTIONAL_TABLES),
        optional_qualified_names=[
            _schema_table(schema_name, table_name)
            for table_name in _MRF_OPTIONAL_TABLES
        ],
    )
    return _validated_optional_authority(
        optional_relation_rows,
        schema_name=schema_name,
    )


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
    (
        optional_authority_payload,
        present_optional_table_names,
        present_optional_oids,
    ) = await _load_optional_authority(
        executor,
        schema_name=schema_name,
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
        "optional_relations": optional_authority_payload,
        "audit_guards": _catalog_json_value(trigger_rows_by_field),
    }
    return LegacyAuthorityCatalog(
        catalog_digest=canonical_sha256(authority_payload_by_field),
        relation_oids=tuple(
            sorted(
                {
                    *(
                        int(_row_mapping(relation_row)["relation_oid"])
                        for relation_row in relation_rows
                    ),
                    *present_optional_oids,
                }
            )
        ),
        present_optional_table_names=present_optional_table_names,
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

