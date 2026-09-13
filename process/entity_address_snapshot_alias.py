# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Portable active-alias evidence for unified-address archive workers."""

from __future__ import annotations

import hashlib
import re
import struct
from dataclasses import dataclass
from typing import Any, Mapping

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from process.ext import address_alias_sql


CONTRACT = "entity_address_alias_semantic_receipt.v1"
RECEIPT_VERSION = "entity_address_alias_semantic_receipt.v1"
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_CHUNK_ROWS = 4096
_DIGEST_WORK_MEM = "64MB"
_STATE_TABLE = address_alias_sql.ADDRESS_ALIAS_STATE_TABLE
_ALIAS_TABLE = address_alias_sql.ADDRESS_ALIAS_TABLE
_SEMANTIC_COLUMNS = (
    "source_address_key",
    "source_identity_key",
    "target_address_key",
    "target_identity_key",
    "alias_kind",
    "ruleset_version",
    "target_strict_source_bits",
    "target_strict_source_count",
    "candidate_count",
)
_SUPPORTED_ALIAS_KINDS = (
    address_alias_sql.EVIDENCE_ADDRESS_MATCH_ALIAS_KIND,
    address_alias_sql.NUMERIC_GRID_ALIAS_KIND,
)
_STATE_COLUMNS = (
    ("singleton", "boolean", True, ""),
    ("schema_version", "smallint", True, ""),
    ("active_ruleset_version", "smallint", True, ""),
    ("generation", "bigint", True, ""),
    ("updated_at", "timestamp with time zone", True, ""),
)
_ALIAS_COLUMNS = (
    ("alias_id", "bigint", True, "a"),
    ("source_address_key", "uuid", True, ""),
    ("source_identity_key", "text", True, ""),
    ("target_address_key", "uuid", True, ""),
    ("target_identity_key", "text", True, ""),
    ("alias_kind", "character varying(64)", True, ""),
    ("ruleset_version", "smallint", True, ""),
    ("target_strict_source_bits", "integer", True, ""),
    ("target_strict_source_count", "smallint", True, ""),
    ("candidate_count", "integer", True, ""),
    ("shadow_run_id", "uuid", True, ""),
    ("apply_run_id", "uuid", True, ""),
    ("reviewed_candidate_digest", "character varying(64)", True, ""),
    ("applied_at", "timestamp with time zone", True, ""),
    ("revoked_at", "timestamp with time zone", False, ""),
    ("revoked_reason", "text", False, ""),
    ("revoked_by", "character varying(256)", False, ""),
    ("revoke_run_id", "uuid", False, ""),
    ("created_at", "timestamp with time zone", True, ""),
    ("updated_at", "timestamp with time zone", True, ""),
)


class EntityAddressSnapshotAliasError(ValueError):
    """Active address aliases cannot produce the reviewed portable receipt."""


@dataclass(frozen=True)
class EntityAddressAliasSemanticReceipt:
    """Local alias state plus a generation-independent active-set identity."""

    alias_schema_version: int
    active_ruleset_version: int
    local_generation: int
    active_alias_count: int
    active_alias_sha256: str

    def portable_identity(self) -> dict[str, Any]:
        """Return only fields whose equality has cross-cluster meaning."""

        return {
            "contract": CONTRACT,
            "receipt_version": RECEIPT_VERSION,
            "alias_schema_version": self.alias_schema_version,
            "active_ruleset_version": self.active_ruleset_version,
            "active_alias_count": self.active_alias_count,
            "active_alias_sha256": self.active_alias_sha256,
        }

    def as_dict(self) -> dict[str, Any]:
        """Return the bounded persisted form, retaining the local concurrency token."""

        return {**self.portable_identity(), "local_generation": self.local_generation}


def _schema_name(value: object) -> str:
    if not isinstance(value, str) or _IDENTIFIER.fullmatch(value) is None or len(value.encode("utf-8")) > 63:
        raise EntityAddressSnapshotAliasError("entity-address alias receipt schema is invalid")
    return value


def _quoted(value: str) -> str:
    return f'"{value}"'


def _require_caller_transaction(session: Any) -> None:
    if not callable(getattr(session, "in_transaction", None)) or not session.in_transaction():
        raise EntityAddressSnapshotAliasError("entity-address alias receipt requires a caller transaction")


async def _lock_alias_relations(session: Any, schema_name: str) -> None:
    """Follow writer ordering, then freeze the two relations through capture."""

    await session.execute(text(address_alias_sql.alias_advisory_xact_lock_sql()))
    relations = ", ".join(
        f"{_quoted(schema_name)}.{_quoted(table_name)}" for table_name in (_STATE_TABLE, _ALIAS_TABLE)
    )
    try:
        await session.execute(text(f"LOCK TABLE {relations} IN SHARE MODE"))
    except SQLAlchemyError as error:
        raise EntityAddressSnapshotAliasError("entity-address alias receipt relations are unavailable") from error


async def _relation_oid(session: Any, schema_name: str, table_name: str) -> int:
    relation_metadata = (
        (
            await session.execute(
                text(
                    "SELECT relation.oid, relation.relkind::text AS relkind, "
                    "relation.relpersistence::text AS relpersistence, relation.relrowsecurity, "
                    "relation.relforcerowsecurity FROM pg_catalog.pg_class AS relation "
                    "JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = relation.relnamespace "
                    "WHERE namespace.nspname = :schema_name AND relation.relname = :table_name"
                ),
                {"schema_name": schema_name, "table_name": table_name},
            )
        )
        .mappings()
        .one_or_none()
    )
    if (
        relation_metadata is None
        or relation_metadata["relkind"] != "r"
        or relation_metadata["relpersistence"] != "p"
        or relation_metadata["relrowsecurity"]
        or relation_metadata["relforcerowsecurity"]
    ):
        raise EntityAddressSnapshotAliasError("entity-address alias receipt relation shape is unsupported")
    return int(relation_metadata["oid"])


async def _catalog_column_shapes(
    session: Any,
    relation_oid: int,
) -> tuple[tuple[str, str, bool, str], ...]:
    column_records = (
        await session.execute(
            text(
                "SELECT attribute.attname, "
                "pg_catalog.format_type(attribute.atttypid, attribute.atttypmod) AS formatted_type, "
                "attribute.attnotnull, attribute.attidentity::text AS attidentity "
                "FROM pg_catalog.pg_attribute AS attribute "
                "WHERE attribute.attrelid = :relation_oid AND attribute.attnum > 0 "
                "AND NOT attribute.attisdropped ORDER BY attribute.attnum"
            ),
            {"relation_oid": relation_oid},
        )
    ).mappings()
    return tuple(
        (
            str(column_record["attname"]),
            str(column_record["formatted_type"]),
            bool(column_record["attnotnull"]),
            str(column_record["attidentity"]),
        )
        for column_record in column_records
    )


async def _primary_key_columns(session: Any, relation_oid: int) -> tuple[str, ...]:
    key_column_records = (
        await session.execute(
            text(
                "SELECT attribute.attname FROM pg_catalog.pg_constraint AS constraint_row "
                "JOIN LATERAL pg_catalog.unnest(constraint_row.conkey) WITH ORDINALITY "
                "AS key_column(attnum, ordinal) ON true "
                "JOIN pg_catalog.pg_attribute AS attribute ON attribute.attrelid = constraint_row.conrelid "
                "AND attribute.attnum = key_column.attnum "
                "WHERE constraint_row.conrelid = :relation_oid AND constraint_row.contype = 'p' "
                "AND constraint_row.convalidated IS TRUE ORDER BY key_column.ordinal"
            ),
            {"relation_oid": relation_oid},
        )
    ).mappings()
    return tuple(str(key_column_record["attname"]) for key_column_record in key_column_records)


async def _require_relation_shape(
    session: Any,
    *,
    schema_name: str,
    table_name: str,
    expected_columns: tuple[tuple[str, str, bool, str], ...],
    expected_primary_key: tuple[str, ...],
) -> None:
    """Require the exact model-owned column and primary-key shape."""

    relation_oid = await _relation_oid(session, schema_name, table_name)
    observed_columns = await _catalog_column_shapes(session, relation_oid)
    observed_primary_key_columns = await _primary_key_columns(session, relation_oid)
    if observed_columns != expected_columns or observed_primary_key_columns != expected_primary_key:
        raise EntityAddressSnapshotAliasError("entity-address alias receipt relation shape is unsupported")


async def _alias_state(session: Any, schema_name: str) -> tuple[int, int, int]:
    rows = (
        (
            await session.execute(
                text(
                    f"SELECT singleton, schema_version, active_ruleset_version, generation "
                    f"FROM {_quoted(schema_name)}.{_quoted(_STATE_TABLE)} ORDER BY singleton"
                )
            )
        )
        .mappings()
        .all()
    )
    if len(rows) != 1 or rows[0]["singleton"] is not True:
        raise EntityAddressSnapshotAliasError("entity-address alias singleton state is invalid")
    schema_version = rows[0]["schema_version"]
    ruleset_version = rows[0]["active_ruleset_version"]
    generation = rows[0]["generation"]
    if type(schema_version) is not int or schema_version != address_alias_sql.ADDRESS_ALIAS_SCHEMA_VERSION:
        raise EntityAddressSnapshotAliasError("entity-address alias schema version is unsupported")
    if type(ruleset_version) is not int or ruleset_version != address_alias_sql.ADDRESS_ALIAS_RULESET_VERSION:
        raise EntityAddressSnapshotAliasError("entity-address alias ruleset version is unsupported")
    if type(generation) is not int or generation < 0:
        raise EntityAddressSnapshotAliasError("entity-address alias generation is invalid")
    return schema_version, ruleset_version, generation


def _active_alias_chunk_sql(schema_name: str):
    semantic_values = ", ".join(f"active_alias.{_quoted(column)}" for column in _SEMANTIC_COLUMNS)
    return text(
        f"""
        WITH row_hashes AS MATERIALIZED (
            SELECT pg_catalog.encode(
                pg_catalog.sha256(
                    pg_catalog.convert_to(
                        pg_catalog.jsonb_build_array({semantic_values})::text,
                        'UTF8'
                    )
                ),
                'hex'
            ) AS row_sha256
            FROM {_quoted(schema_name)}.{_quoted(_ALIAS_TABLE)} AS active_alias
            WHERE active_alias.revoked_at IS NULL
        ), ordered AS (
            SELECT row_sha256,
                   pg_catalog.row_number() OVER (ORDER BY row_sha256 COLLATE "C") - 1 AS ordinal
            FROM row_hashes
        )
        SELECT ordinal / :chunk_rows AS chunk_ordinal,
               pg_catalog.count(*)::bigint AS chunk_row_count,
               pg_catalog.encode(
                   pg_catalog.sha256(
                       pg_catalog.convert_to(
                           pg_catalog.string_agg(row_sha256, '' ORDER BY ordinal),
                           'UTF8'
                       )
                   ),
                   'hex'
               ) AS chunk_sha256
        FROM ordered
        GROUP BY ordinal / :chunk_rows
        ORDER BY chunk_ordinal
        """
    )


async def _active_alias_identity(session: Any, schema_name: str) -> tuple[int, str]:
    """Fold sorted active semantic row hashes without returning the active row set."""

    chunks = await session.stream(
        _active_alias_chunk_sql(schema_name),
        {"chunk_rows": _CHUNK_ROWS},
    )
    digest = hashlib.sha256(b"entity-address-active-alias-chunks/v1\0")
    total_rows = 0
    expected_ordinal = 0
    try:
        async for chunk_record in chunks.mappings():
            chunk_ordinal = int(chunk_record["chunk_ordinal"])
            chunk_rows = int(chunk_record["chunk_row_count"])
            chunk_sha256 = str(chunk_record["chunk_sha256"])
            if (
                chunk_ordinal != expected_ordinal
                or not 1 <= chunk_rows <= _CHUNK_ROWS
                or _SHA256.fullmatch(chunk_sha256) is None
            ):
                raise EntityAddressSnapshotAliasError("entity-address active alias receipt is invalid")
            digest.update(struct.pack(">I", chunk_rows))
            digest.update(bytes.fromhex(chunk_sha256))
            total_rows += chunk_rows
            expected_ordinal += 1
    finally:
        await chunks.close()
    return total_rows, digest.hexdigest()


async def _require_supported_active_aliases(
    session: Any,
    schema_name: str,
    active_ruleset_version: int,
) -> None:
    unsupported = await session.scalar(
        text(
            f"SELECT COUNT(*) FROM {_quoted(schema_name)}.{_quoted(_ALIAS_TABLE)} "
            "WHERE revoked_at IS NULL AND (ruleset_version <> :ruleset_version "
            "OR alias_kind <> ALL(CAST(:alias_kinds AS varchar[])))"
        ),
        {
            "ruleset_version": active_ruleset_version,
            "alias_kinds": list(_SUPPORTED_ALIAS_KINDS),
        },
    )
    if type(unsupported) is not int or unsupported != 0:
        raise EntityAddressSnapshotAliasError("entity-address active alias version is unsupported")


async def _set_digest_work_mem(session: Any) -> None:
    """Reserve one fixed, transaction-local sort budget for the full active-set scan."""

    await session.execute(text(f"SET LOCAL work_mem TO '{_DIGEST_WORK_MEM}'"))


async def capture_entity_address_alias_semantic_receipt(
    session: Any,
    *,
    schema_name: str,
) -> EntityAddressAliasSemanticReceipt:
    """Capture worker-side active alias semantics under the native writer lock."""

    schema = _schema_name(schema_name)
    _require_caller_transaction(session)
    await _lock_alias_relations(session, schema)
    await _require_relation_shape(
        session,
        schema_name=schema,
        table_name=_STATE_TABLE,
        expected_columns=_STATE_COLUMNS,
        expected_primary_key=("singleton",),
    )
    await _require_relation_shape(
        session,
        schema_name=schema,
        table_name=_ALIAS_TABLE,
        expected_columns=_ALIAS_COLUMNS,
        expected_primary_key=("alias_id",),
    )
    schema_version, ruleset_version, generation = await _alias_state(session, schema)
    await _require_supported_active_aliases(session, schema, ruleset_version)
    await _set_digest_work_mem(session)
    active_alias_count, active_alias_sha256 = await _active_alias_identity(session, schema)
    return EntityAddressAliasSemanticReceipt(
        alias_schema_version=schema_version,
        active_ruleset_version=ruleset_version,
        local_generation=generation,
        active_alias_count=active_alias_count,
        active_alias_sha256=active_alias_sha256,
    )


def validate_entity_address_alias_semantic_receipt(
    receipt_value: Mapping[str, Any] | EntityAddressAliasSemanticReceipt,
) -> EntityAddressAliasSemanticReceipt:
    """Validate persisted alias evidence without treating its local counter as identity."""

    receipt = receipt_value.as_dict() if isinstance(receipt_value, EntityAddressAliasSemanticReceipt) else receipt_value
    expected_fields = {
        "contract",
        "receipt_version",
        "alias_schema_version",
        "active_ruleset_version",
        "local_generation",
        "active_alias_count",
        "active_alias_sha256",
    }
    if (
        not isinstance(receipt, Mapping)
        or set(receipt) != expected_fields
        or receipt["contract"] != CONTRACT
        or receipt["receipt_version"] != RECEIPT_VERSION
        or type(receipt["alias_schema_version"]) is not int
        or receipt["alias_schema_version"] != address_alias_sql.ADDRESS_ALIAS_SCHEMA_VERSION
        or type(receipt["active_ruleset_version"]) is not int
        or receipt["active_ruleset_version"] != address_alias_sql.ADDRESS_ALIAS_RULESET_VERSION
        or type(receipt["local_generation"]) is not int
        or receipt["local_generation"] < 0
        or type(receipt["active_alias_count"]) is not int
        or receipt["active_alias_count"] < 0
        or not isinstance(receipt["active_alias_sha256"], str)
        or _SHA256.fullmatch(receipt["active_alias_sha256"]) is None
    ):
        raise EntityAddressSnapshotAliasError("entity-address alias receipt is invalid")
    return EntityAddressAliasSemanticReceipt(
        alias_schema_version=receipt["alias_schema_version"],
        active_ruleset_version=receipt["active_ruleset_version"],
        local_generation=receipt["local_generation"],
        active_alias_count=receipt["active_alias_count"],
        active_alias_sha256=receipt["active_alias_sha256"],
    )


def require_matching_entity_address_alias_semantics(
    expected: Mapping[str, Any] | EntityAddressAliasSemanticReceipt,
    actual: Mapping[str, Any] | EntityAddressAliasSemanticReceipt,
) -> EntityAddressAliasSemanticReceipt:
    """Reject a cross-cluster active-set mismatch while allowing local counter drift."""

    expected_receipt = validate_entity_address_alias_semantic_receipt(expected)
    actual_receipt = validate_entity_address_alias_semantic_receipt(actual)
    if expected_receipt.portable_identity() != actual_receipt.portable_identity():
        raise EntityAddressSnapshotAliasError("entity-address active alias semantics differ")
    return actual_receipt


__all__ = [
    "CONTRACT",
    "RECEIPT_VERSION",
    "EntityAddressAliasSemanticReceipt",
    "EntityAddressSnapshotAliasError",
    "capture_entity_address_alias_semantic_receipt",
    "require_matching_entity_address_alias_semantics",
    "validate_entity_address_alias_semantic_receipt",
]
