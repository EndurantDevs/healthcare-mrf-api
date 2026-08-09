# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact runtime catalog fence for NPPES public-evidence admission."""

from __future__ import annotations

from process.nppes_public_evidence_chain_rows import (
    CHAIN_ADMISSION_COLUMNS,
    CHAIN_ARCHIVE_COLUMNS,
)
from process.nppes_public_evidence_rows import (
    ADMISSION_COLUMNS,
    COMMON_COLUMNS,
    MEMBER_COLUMNS,
    NPI_ENUMERATION_COLUMNS,
    SOURCE_IDENTITY_COLUMNS,
    SOURCE_LINK_COLUMNS,
    SOURCE_RECORD_COLUMNS,
    SOURCE_RELEASE_COLUMNS,
)
from process.nppes_public_evidence_writer_contract import writer_error


_ADMISSION = "public_evidence_nppes_registry_admission"
_ADMISSION_SEAL = "public_evidence_nppes_registry_admission_seal"
_MEMBER = "public_evidence_nppes_registry_member"
_CHAIN = "public_evidence_nppes_registry_chain_admission"
_CHAIN_SEAL = "public_evidence_nppes_registry_chain_admission_seal"
_CHAIN_ARCHIVE = "public_evidence_nppes_registry_chain_archive"
_ADMISSION_VALIDATOR = (
    "validate_public_evidence_nppes_registry_admission_lifecycle_v2"
)
_CHAIN_VALIDATOR = "validate_public_evidence_nppes_chain_admission"
_SOURCE_IDENTITY = "public_evidence_source_identity"
_SOURCE_RELEASE = "public_evidence_source_release"
_SOURCE_RECORD = "public_evidence_source_record"
_COMMON = "public_evidence_record"
_SOURCE_LINK = "public_evidence_record_source_link"
_TYPED = "public_evidence_npi_enumeration"
_TABLE_COLUMNS = {
    _SOURCE_IDENTITY: (*SOURCE_IDENTITY_COLUMNS, "created_at"),
    _SOURCE_RELEASE: (*SOURCE_RELEASE_COLUMNS, "created_at"),
    _SOURCE_RECORD: (*SOURCE_RECORD_COLUMNS[:-1], "created_at", "nppes_admission_ref"),
    _COMMON: (*COMMON_COLUMNS[:-1], "created_at", "nppes_admission_ref"),
    _SOURCE_LINK: (*SOURCE_LINK_COLUMNS[:-1], "created_at", "nppes_admission_ref"),
    _TYPED: (*NPI_ENUMERATION_COLUMNS[:-1], "created_at", "nppes_admission_ref"),
    _ADMISSION: (*ADMISSION_COLUMNS, "created_at"),
    _ADMISSION_SEAL: ("admission_ref", "sealed_at"),
    _MEMBER: (*MEMBER_COLUMNS, "created_at"),
    _CHAIN: (*CHAIN_ADMISSION_COLUMNS, "created_at"),
    _CHAIN_SEAL: ("chain_ref", "sealed_at"),
    _CHAIN_ARCHIVE: (*CHAIN_ARCHIVE_COLUMNS, "created_at"),
}
_NEW_TABLES = (
    _ADMISSION,
    _ADMISSION_SEAL,
    _MEMBER,
    _CHAIN,
    _CHAIN_SEAL,
    _CHAIN_ARCHIVE,
)
_APPEND_TABLES = (_SOURCE_RECORD, _COMMON, _SOURCE_LINK, _TYPED, _MEMBER, _CHAIN_ARCHIVE)
_NEW_TYPE_BY_COLUMN = {
    "admission_ref": "character varying(50)",
    "admission_state": "character varying(32)",
    "archive_count": "integer",
    "archive_name": "text",
    "archive_ordinal": "integer",
    "artifact_byte_count": "bigint",
    "artifact_sha256": "bytea",
    "chain_ref": "character varying(50)",
    "contract": "character varying(64)",
    "contract_sha256": "bytea",
    "created_at": "timestamp with time zone",
    "effective_start_not_disclosed_count": "bigint",
    "entity_type_code": "character varying(1)",
    "entity_type_not_disclosed_count": "bigint",
    "evidence_ref": "character varying(49)",
    "evidence_root_sha256": "bytea",
    "excluded_record_count": "bigint",
    "exclusion_reason": "character varying(64)",
    "header_sha256": "bytea",
    "identity_contract_id": "character varying(96)",
    "last_update_date": "date",
    "leaf_sha256": "bytea",
    "listing_byte_count": "bigint",
    "listing_candidate_names": "text[]",
    "listing_sha256": "bytea",
    "manifest_contract": "character varying(64)",
    "manifest_sha256": "bytea",
    "minimum_effective_start_at": "timestamp with time zone",
    "npi": "character varying(10)",
    "npi_deactivation_date": "date",
    "npi_reactivation_date": "date",
    "payload_contract_id": "character varying(96)",
    "payload_sha256": "bytea",
    "primary_member_name": "text",
    "projected_record_count": "bigint",
    "projection_state": "character varying(16)",
    "provider_enumeration_date": "date",
    "publication_enabled": "boolean",
    "record_hmac_sha256": "bytea",
    "record_identity_contract_id": "character varying(96)",
    "record_kind": "character varying(64)",
    "rights_proof_sha256": "bytea",
    "row_sha256": "bytea",
    "sealed_at": "timestamp with time zone",
    "serving_authority": "character varying(16)",
    "snapshot_at": "timestamp with time zone",
    "source_kind": "character varying(48)",
    "source_record_count": "bigint",
    "source_record_ref": "character varying(49)",
    "source_release_contract_sha256": "bytea",
    "source_release_ref": "character varying(50)",
    "source_row_ordinal": "bigint",
    "source_url": "text",
    "tree_contract_id": "character varying(96)",
    "zip_member_census_sha256": "bytea",
    "zip_member_count": "integer",
}
_NULLABLE_MEMBER_COLUMNS = {
    "entity_type_code",
    "provider_enumeration_date",
    "last_update_date",
    "npi_deactivation_date",
    "npi_reactivation_date",
    "exclusion_reason",
    "evidence_ref",
}
_REQUIRED_NEW_CONSTRAINTS = {
    "public_evidence_nppes_registry_admission_pkey",
    "public_evidence_nppes_registry_admission_release_key",
    "public_evidence_nppes_registry_admission_owner_key",
    "public_evidence_nppes_registry_admission_chain_owner_key",
    "public_evidence_nppes_registry_admission_release_fkey",
    "public_evidence_nppes_registry_admission_shape_check",
    "public_evidence_nppes_registry_admission_seal_pkey",
    "public_evidence_nppes_registry_admission_seal_parent_fkey",
    "public_evidence_nppes_registry_admission_seal_shape_check",
    "public_evidence_nppes_registry_member_pkey",
    "public_evidence_nppes_registry_member_npi_key",
    "public_evidence_nppes_registry_member_source_key",
    "public_evidence_nppes_registry_member_evidence_key",
    "public_evidence_nppes_registry_member_admission_fkey",
    "public_evidence_nppes_registry_member_source_fkey",
    "public_evidence_nppes_registry_member_evidence_fkey",
    "public_evidence_nppes_registry_member_shape_check",
    "public_evidence_nppes_registry_member_digest_check",
    "public_evidence_nppes_registry_chain_admission_pkey",
    "public_evidence_nppes_registry_chain_admission_owner_key",
    "public_evidence_nppes_registry_chain_listing_key",
    "public_evidence_nppes_registry_chain_shape_check",
    "public_evidence_nppes_registry_chain_admission_seal_pkey",
    "public_evidence_nppes_registry_chain_admission_seal_parent_fkey",
    "public_evidence_nppes_registry_chain_admission_seal_shape_check",
    "public_evidence_nppes_registry_chain_archive_pkey",
    "public_evidence_nppes_registry_chain_archive_admission_key",
    "public_evidence_nppes_registry_chain_archive_release_key",
    "public_evidence_nppes_registry_chain_archive_name_key",
    "public_evidence_nppes_registry_chain_archive_artifact_key",
    "public_evidence_nppes_registry_chain_archive_manifest_key",
    "public_evidence_nppes_registry_chain_archive_parent_fkey",
    "public_evidence_nppes_registry_chain_archive_admission_fkey",
    "public_evidence_nppes_registry_chain_archive_shape_check",
}
_REWIRED_FKS = {
    "public_evidence_source_record_admission_fkey",
    "public_evidence_record_admission_fkey",
    "public_evidence_record_source_link_record_fkey",
    "public_evidence_record_source_link_source_fkey",
    "public_evidence_npi_enumeration_record_fkey",
}


async def _column_records(connection: object, schema: str) -> tuple[object, ...]:
    return tuple(
        await connection.fetch(
            "SELECT relation.relname, attribute.attname, "
            "format_type(attribute.atttypid, attribute.atttypmod) AS data_type, "
            "attribute.attnotnull, "
            "pg_get_expr(default_value.adbin, default_value.adrelid) AS default_expr "
            "FROM pg_class AS relation JOIN pg_namespace AS namespace "
            "ON namespace.oid=relation.relnamespace JOIN pg_attribute AS attribute "
            "ON attribute.attrelid=relation.oid LEFT JOIN pg_attrdef AS default_value "
            "ON default_value.adrelid=relation.oid AND default_value.adnum=attribute.attnum "
            "WHERE namespace.nspname=$1 AND relation.relname=ANY($2::text[]) "
            "AND relation.relkind='r' AND attribute.attnum>0 "
            "AND NOT attribute.attisdropped ORDER BY relation.relname, attribute.attnum",
            schema,
            list(_TABLE_COLUMNS),
        )
    )


def _has_exact_columns(column_records: tuple[object, ...]) -> bool:
    columns_by_table = {
        table_name: tuple(
            column_record["attname"]
            for column_record in column_records
            if column_record["relname"] == table_name
        )
        for table_name in _TABLE_COLUMNS
    }
    if columns_by_table != _TABLE_COLUMNS:
        return False
    for column_record in column_records:
        table_name = column_record["relname"]
        column_name = column_record["attname"]
        if table_name in _NEW_TABLES:
            if column_record["data_type"] != _NEW_TYPE_BY_COLUMN[column_name]:
                return False
            is_nullable = table_name == _MEMBER and column_name in _NULLABLE_MEMBER_COLUMNS
            if column_record["attnotnull"] is is_nullable:
                return False
            expected_default = (
                "transaction_timestamp()"
                if column_name in {"created_at", "sealed_at"}
                else None
            )
            if column_record["default_expr"] != expected_default:
                return False
        if column_name == "nppes_admission_ref":
            if column_record["data_type"] != "character varying(50)":
                return False
            if column_record["attnotnull"] is (table_name == _SOURCE_RECORD):
                return False
    return True


async def _has_exact_constraints(connection: object, schema: str) -> bool:
    constraint_records = await connection.fetch(
        "SELECT relation.relname, constraint_record.conname, "
        "constraint_record.contype::text, constraint_record.condeferrable, "
        "constraint_record.condeferred, constraint_record.convalidated, "
        "constraint_record.conenforced FROM pg_constraint AS constraint_record "
        "JOIN pg_class AS relation ON relation.oid=constraint_record.conrelid "
        "JOIN pg_namespace AS namespace ON namespace.oid=relation.relnamespace "
        "WHERE namespace.nspname=$1 AND (relation.relname=ANY($2::text[]) "
        "OR constraint_record.conname=ANY($3::text[])) "
        "AND constraint_record.contype NOT IN ('n','t')",
        schema,
        list(_NEW_TABLES),
        list(_REWIRED_FKS),
    )
    new_names = {
        constraint_record["conname"]
        for constraint_record in constraint_records
        if constraint_record["relname"] in _NEW_TABLES
    }
    rewired_records = [
        constraint_record
        for constraint_record in constraint_records
        if constraint_record["conname"] in _REWIRED_FKS
    ]
    return (
        new_names == _REQUIRED_NEW_CONSTRAINTS
        and {
            constraint_record["conname"]
            for constraint_record in rewired_records
        } == _REWIRED_FKS
        and all(
            constraint_record["convalidated"]
            and constraint_record["conenforced"]
            for constraint_record in constraint_records
        )
        and all(
            not constraint_record["condeferrable"]
            and not constraint_record["condeferred"]
            for constraint_record in constraint_records
            if constraint_record["contype"] == "f"
        )
    )


def _classify_trigger_records(
    trigger_records: tuple[object, ...] | list[object],
) -> tuple[list[object], list[object], list[object], list[object]]:
    """Partition task triggers into integrity, append, and immutable groups."""

    task_trigger_records = [
        trigger_record
        for trigger_record in trigger_records
        if trigger_record["relname"] in _NEW_TABLES
        or trigger_record["relname"] in _APPEND_TABLES
    ]
    integrity_trigger_records = [
        trigger_record
        for trigger_record in task_trigger_records
        if "integrity_guard" in trigger_record["tgname"]
    ]
    append_trigger_records = [
        trigger_record
        for trigger_record in task_trigger_records
        if trigger_record["relname"] in _APPEND_TABLES
        and trigger_record["tgtype"] == 4
    ]
    immutable_trigger_records = [
        trigger_record
        for trigger_record in task_trigger_records
        if trigger_record["relname"] in _NEW_TABLES
        and trigger_record["tgtype"] in {27, 34}
    ]
    return (
        task_trigger_records,
        integrity_trigger_records,
        append_trigger_records,
        immutable_trigger_records,
    )


async def _trigger_records(connection: object, schema: str) -> list[object]:
    return list(await connection.fetch(
        "SELECT relation.relname, trigger_record.tgname, trigger_record.tgtype::integer, "
        "trigger_record.tgenabled::text, trigger_record.tgdeferrable, "
        "trigger_record.tginitdeferred, procedure_namespace.nspname AS function_schema, "
        "procedure.proname AS function_name, procedure.prosecdef AS security_definer, "
        "procedure.proconfig = ARRAY['search_path=pg_catalog']::text[] "
        "AS exact_search_path "
        "FROM pg_trigger AS trigger_record JOIN pg_class AS relation "
        "ON relation.oid=trigger_record.tgrelid JOIN pg_namespace AS namespace "
        "ON namespace.oid=relation.relnamespace JOIN pg_proc AS procedure "
        "ON procedure.oid=trigger_record.tgfoid JOIN pg_namespace AS procedure_namespace "
        "ON procedure_namespace.oid=procedure.pronamespace WHERE namespace.nspname=$1 "
        "AND NOT trigger_record.tgisinternal",
        schema,
    ))


async def _has_exact_triggers(connection: object, schema: str) -> bool:
    """Return whether all NPPES admission triggers match the exact catalog."""

    trigger_records = await _trigger_records(connection, schema)
    (
        task_trigger_records,
        integrity_trigger_records,
        append_trigger_records,
        immutable_trigger_records,
    ) = _classify_trigger_records(trigger_records)
    old_names = {
        f"{table_name}_integrity_guard"
        for table_name in (_COMMON, _SOURCE_LINK, _TYPED)
    }
    expected_integrity_function_by_table = {
        _ADMISSION: _ADMISSION_VALIDATOR,
        _CHAIN: _CHAIN_VALIDATOR,
    }
    return (
        len(integrity_trigger_records) == 2
        and len(append_trigger_records) == len(_APPEND_TABLES)
        and {
            trigger_record["relname"]
            for trigger_record in append_trigger_records
        } == set(_APPEND_TABLES)
        and len(immutable_trigger_records) == 2 * len(_NEW_TABLES)
        and not any(
            trigger_record["tgname"] in old_names
            for trigger_record in trigger_records
        )
        and all(
            trigger_record["tgenabled"] == "A"
            for trigger_record in task_trigger_records
        )
        and all(
            trigger_record["function_schema"] == schema
            for trigger_record in task_trigger_records
        )
        and all(
            trigger_record["tgtype"] == 5
            and trigger_record["tgdeferrable"]
            and trigger_record["tginitdeferred"]
            and trigger_record["security_definer"]
            and trigger_record["exact_search_path"]
            for trigger_record in integrity_trigger_records
        )
        and {
            trigger_record["relname"]: trigger_record["function_name"]
            for trigger_record in integrity_trigger_records
        } == expected_integrity_function_by_table
    )


async def _has_private_acl(connection: object, schema: str) -> bool:
    is_public_denied = await connection.fetchval(
        "SELECT bool_and(NOT has_table_privilege('public', "
        "format('%I.%I', $1::text, table_name), privilege)) "
        "FROM unnest($2::text[]) AS table_name "
        "CROSS JOIN unnest($3::text[]) AS privilege",
        schema,
        list(_NEW_TABLES),
        ["SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER", "MAINTAIN"],
    )
    public_helper_count = await connection.fetchval(
        "SELECT count(*) FROM pg_proc AS procedure JOIN pg_namespace AS namespace "
        "ON namespace.oid=procedure.pronamespace WHERE namespace.nspname=$1 "
        "AND (procedure.proname LIKE 'public_evidence_nppes_%' "
        "OR procedure.proname='nppes_registry_payload_digest' "
        "OR procedure.proname LIKE 'validate_public_evidence_nppes_%' "
        "OR procedure.proname LIKE 'guard_%_admission_append') "
        "AND has_function_privilege('public', procedure.oid, 'EXECUTE')",
        schema,
    )
    return is_public_denied is True and public_helper_count == 0


async def _has_writer_maintain_privilege(connection: object, schema: str) -> bool:
    """Require the PostgreSQL 18 privilege used by transactional ANALYZE."""

    has_maintain = await connection.fetchval(
        "SELECT bool_and(has_table_privilege(current_user, "
        "format('%I.%I', $1::text, table_name), 'MAINTAIN')) "
        "FROM unnest($2::text[]) AS table_name",
        schema,
        list((_SOURCE_RECORD, _MEMBER, _COMMON, _SOURCE_LINK, _TYPED)),
    )
    return has_maintain is True


async def assert_nppes_admission_catalog(connection: object, schema: str) -> None:
    """Reject any stale or partially installed NPPES admission catalog."""

    try:
        column_records = await _column_records(connection, schema)
        if (
            not _has_exact_columns(column_records)
            or not await _has_exact_constraints(connection, schema)
            or not await _has_exact_triggers(connection, schema)
            or not await _has_private_acl(connection, schema)
            or not await _has_writer_maintain_privilege(connection, schema)
        ):
            raise writer_error()
    except Exception:
        normalized_error = writer_error()
    else:
        return
    raise normalized_error


__all__ = ("assert_nppes_admission_catalog",)
