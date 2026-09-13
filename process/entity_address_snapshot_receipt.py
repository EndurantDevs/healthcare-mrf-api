# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Canonical native receipts for the fixed unified-address archive family."""

from __future__ import annotations

import hashlib
import importlib
import json
import re
import struct
from dataclasses import dataclass
from typing import Any, Mapping

from sqlalchemy import text


entity_address_unified = importlib.import_module("process.entity_address_unified")

CONTRACT = "entity_address_unified.postgres.v1"
RECEIPT_VERSION = "entity_address_archive_receipt.v1"
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_CHUNK_ROWS = 4096


class EntityAddressArchiveReceiptError(ValueError):
    """The closed archive family does not have a portable semantic receipt."""


@dataclass(frozen=True)
class EntityAddressArchiveTableReceipt:
    """Schema and multiset content identity for one reviewed model relation."""

    model_name: str
    table_name: str
    schema_sha256: str
    row_count: int
    row_sha256: str

    def as_dict(self) -> dict[str, Any]:
        """Return the stable JSON representation consumed by the archive profile."""
        return {
            "model_name": self.model_name,
            "table_name": self.table_name,
            "schema_sha256": self.schema_sha256,
            "row_count": self.row_count,
            "row_sha256": self.row_sha256,
        }


@dataclass(frozen=True)
class EntityAddressArchiveReceipt:
    """Schema-name-independent semantic identity for exactly seven relations."""

    tables: tuple[EntityAddressArchiveTableReceipt, ...]
    schema_sha256: str
    content_sha256: str

    def as_dict(self) -> dict[str, Any]:
        """Return a bounded receipt without OIDs, schema names, or publication claims."""
        return {
            "contract": CONTRACT,
            "receipt_version": RECEIPT_VERSION,
            "tables": [table.as_dict() for table in self.tables],
            "schema_sha256": self.schema_sha256,
            "content_sha256": self.content_sha256,
        }


def _models() -> tuple[type, ...]:
    models = (entity_address_unified.EntityAddressUnified, *entity_address_unified.SUPPORT_TABLE_MODELS)
    names = tuple(model.__tablename__ for model in models)
    if len(models) != 7 or len(set(names)) != 7 or any(_IDENTIFIER.fullmatch(name) is None for name in names):
        raise EntityAddressArchiveReceiptError("entity-address archive model family is invalid")
    return models


def _schema_name(value: object) -> str:
    if not isinstance(value, str):
        raise EntityAddressArchiveReceiptError("entity-address archive schema is invalid")
    normalized = entity_address_unified._validate_schema_name(value)
    if _IDENTIFIER.fullmatch(normalized) is None:
        raise EntityAddressArchiveReceiptError("entity-address archive schema is invalid")
    return normalized


def _quoted(value: str) -> str:
    return f'"{value}"'


def _canonical_digest(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, default=_json_scalar).encode(
            "ascii"
        )
    ).hexdigest()


def _json_scalar(value: object) -> str:
    """Decode PostgreSQL's one-byte catalog codes before canonical JSON hashing."""
    if isinstance(value, bytes):
        return value.decode("ascii")
    raise TypeError(f"unsupported entity-address archive schema value: {type(value).__name__}")


async def _normalize_receipt_session(session, schema_name: str) -> None:
    """Start a repeatable capture before any relation or catalog access."""
    if not session.in_transaction():
        raise EntityAddressArchiveReceiptError("entity-address archive receipt requires a caller transaction")
    for setting in (
        "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ",
        "SET LOCAL TimeZone TO 'UTC'",
        "SET LOCAL DateStyle TO 'ISO, YMD'",
        "SET LOCAL IntervalStyle TO 'iso_8601'",
        "SET LOCAL extra_float_digits TO 3",
        "SET LOCAL bytea_output TO 'hex'",
        "SET LOCAL work_mem TO '64MB'",
        f"SET LOCAL search_path TO {_quoted(schema_name)}, pg_catalog",
    ):
        await session.execute(text(setting))


async def _lock_model_family(session, schema_name: str, models: tuple[type, ...]) -> None:
    """Pin the closed family before inspecting either schema or row contents."""
    relations = ", ".join(
        f"{_quoted(schema_name)}.{_quoted(model.__tablename__)}"
        for model in sorted(models, key=lambda item: item.__tablename__)
    )
    await session.execute(text(f"LOCK TABLE {relations} IN SHARE MODE"))


async def _relation_oid(session, schema_name: str, table_name: str) -> int:
    row = (
        (
            await session.execute(
                text(
                    "SELECT relation.oid, relation.relkind, relation.relpersistence, relation.relrowsecurity, relation.relforcerowsecurity FROM pg_catalog.pg_class AS relation "
                    "JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid=relation.relnamespace "
                    "WHERE namespace.nspname=:schema_name AND relation.relname=:table_name"
                ),
                {"schema_name": schema_name, "table_name": table_name},
            )
        )
        .mappings()
        .one_or_none()
    )
    if (
        row is None
        or not isinstance(row["oid"], int)
        or row["oid"] <= 0
        or row["relkind"] not in {"r", b"r"}
        or row["relpersistence"] not in {"p", b"p"}
        or row["relrowsecurity"]
        or row["relforcerowsecurity"]
    ):
        raise EntityAddressArchiveReceiptError("entity-address archive relation is unavailable")
    return row["oid"]


async def _catalog_columns(session, relation_oid: int) -> list[dict[str, Any]]:
    """Read stable column identity without relation OIDs."""
    return [
        dict(catalog_row)
        for catalog_row in (
            await session.execute(
                text(
                    "SELECT attribute.attnum, attribute.attname, "
                    "pg_catalog.format_type(attribute.atttypid, attribute.atttypmod) AS type, "
                    "attribute.attnotnull, attribute.attgenerated::text, collation_namespace.nspname AS collation_schema, "
                    "collation_name.collname AS collation_name, attribute.attidentity::text, "
                    "pg_catalog.pg_get_expr(default_value.adbin, default_value.adrelid, true) AS default_expression "
                    "FROM pg_catalog.pg_attribute AS attribute "
                    "LEFT JOIN pg_catalog.pg_attrdef AS default_value "
                    "ON default_value.adrelid=attribute.attrelid AND default_value.adnum=attribute.attnum "
                    "LEFT JOIN pg_catalog.pg_collation AS collation_name ON collation_name.oid=attribute.attcollation "
                    "LEFT JOIN pg_catalog.pg_namespace AS collation_namespace ON collation_namespace.oid=collation_name.collnamespace "
                    "WHERE attribute.attrelid=:relation_oid AND attribute.attnum>0 AND NOT attribute.attisdropped "
                    "ORDER BY attribute.attnum"
                ),
                {"relation_oid": relation_oid},
            )
        ).mappings()
    ]


async def _catalog_constraints(session, relation_oid: int, schema_name: str) -> list[dict[str, Any]]:
    """Read structural constraints, including portable foreign-key identity."""
    return [
        dict(catalog_row)
        for catalog_row in (
            await session.execute(
                text(
                    "SELECT constraint_row.contype, constraint_row.condeferrable, constraint_row.condeferred, constraint_row.convalidated, "
                    "constraint_row.conkey::text AS key_columns, constraint_row.confkey::text AS referenced_columns, "
                    "referenced_relation.relname AS referenced_table, referenced_namespace.nspname=:schema_name AS referenced_in_archive_schema, "
                    "pg_catalog.pg_get_expr(constraint_row.conbin, constraint_row.conrelid, true) AS check_expression "
                    "FROM pg_catalog.pg_constraint AS constraint_row LEFT JOIN pg_catalog.pg_class AS referenced_relation "
                    "ON referenced_relation.oid=constraint_row.confrelid LEFT JOIN pg_catalog.pg_namespace AS referenced_namespace "
                    "ON referenced_namespace.oid=referenced_relation.relnamespace WHERE constraint_row.conrelid=:relation_oid "
                    "ORDER BY constraint_row.contype, constraint_row.conkey::text, constraint_row.confkey::text, referenced_relation.relname"
                ),
                {"relation_oid": relation_oid, "schema_name": schema_name},
            )
        ).mappings()
    ]


async def _catalog_indexes(session, relation_oid: int) -> list[dict[str, Any]]:
    """Read index shape with named collation and operator-class identities."""
    indexes = [
        dict(catalog_row)
        for catalog_row in (
            await session.execute(
                text(
                    "SELECT index_row.indisunique, index_row.indisprimary, index_row.indimmediate, index_row.indisvalid, index_row.indnkeyatts, index_row.indnatts, "
                    "access_method.amname AS method, pg_catalog.pg_get_expr(index_row.indpred, index_row.indrelid, true) AS predicate, "
                    "pg_catalog.pg_get_expr(index_row.indexprs, index_row.indrelid, true) AS expressions, "
                    "index_row.indkey::text AS keys, index_row.indoption::text AS options, "
                    "(SELECT pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object("
                    "'position', key_position.position, 'attribute_number', index_row.indkey[key_position.position], "
                    "'collation_schema', collation_namespace.nspname, 'collation_name', collation_name.collname, "
                    "'opclass_schema', opclass_namespace.nspname, 'opclass_name', opclass_name.opcname) ORDER BY key_position.position) "
                    "FROM pg_catalog.generate_subscripts(index_row.indkey, 1) AS key_position(position) "
                    "LEFT JOIN pg_catalog.pg_collation AS collation_name ON collation_name.oid=index_row.indcollation[key_position.position] "
                    "LEFT JOIN pg_catalog.pg_namespace AS collation_namespace ON collation_namespace.oid=collation_name.collnamespace "
                    "LEFT JOIN pg_catalog.pg_opclass AS opclass_name ON opclass_name.oid=index_row.indclass[key_position.position] "
                    "LEFT JOIN pg_catalog.pg_namespace AS opclass_namespace ON opclass_namespace.oid=opclass_name.opcnamespace) AS key_attributes "
                    "FROM pg_catalog.pg_index AS index_row JOIN pg_catalog.pg_class AS index_relation "
                    "ON index_relation.oid=index_row.indexrelid JOIN pg_catalog.pg_am AS access_method "
                    "ON access_method.oid=index_relation.relam WHERE index_row.indrelid=:relation_oid "
                    "ORDER BY index_row.indisprimary DESC, index_row.indisunique DESC, access_method.amname, index_row.indkey::text, index_row.indoption::text"
                ),
                {"relation_oid": relation_oid},
            )
        ).mappings()
    ]
    return sorted(
        indexes, key=lambda entry: json.dumps(entry, sort_keys=True, separators=(",", ":"), default=_json_scalar)
    )


def _reject_schema_qualified_expressions(schema_name: str, *catalog_groups: list[dict[str, Any]]) -> None:
    """Fail closed rather than rewriting schema names embedded in SQL text."""
    expressions = []
    for entries, keys in zip(
        catalog_groups,
        (("default_expression",), ("check_expression",), ("predicate", "expressions")),
        strict=True,
    ):
        expressions.extend(entry.get(key) for entry in entries for key in keys)
    if any(isinstance(expression, str) and schema_name in expression for expression in expressions):
        raise EntityAddressArchiveReceiptError("entity-address archive schema expression is unsupported")


async def _schema_identity(session, relation_oid: int, schema_name: str, table_name: str) -> str:
    columns = await _catalog_columns(session, relation_oid)
    constraints = await _catalog_constraints(session, relation_oid, schema_name)
    indexes = await _catalog_indexes(session, relation_oid)
    if not columns:
        raise EntityAddressArchiveReceiptError("entity-address archive relation has no columns")
    _reject_schema_qualified_expressions(schema_name, columns, constraints, indexes)
    return _canonical_digest(
        {"table_name": table_name, "columns": columns, "constraints": constraints, "indexes": indexes}
    )


async def _row_identity(session, schema_name: str, table_name: str) -> tuple[int, str]:
    """Fold sorted row hashes in bounded chunks without materializing source rows."""
    chunk_rows_result = await session.stream(
        text(
            f"""
            WITH row_hashes AS MATERIALIZED (
                SELECT pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(pg_catalog.to_jsonb(row_value)::text, 'UTF8')), 'hex') AS row_sha256
                  FROM {_quoted(schema_name)}.{_quoted(table_name)} AS row_value
            ), ordered AS (
                SELECT row_sha256, pg_catalog.row_number() OVER (ORDER BY row_sha256 COLLATE \"C\") - 1 AS ordinal
                  FROM row_hashes
            )
            SELECT ordinal / :chunk_rows AS chunk_ordinal, pg_catalog.count(*)::bigint AS chunk_row_count,
                   pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(pg_catalog.string_agg(row_sha256, '' ORDER BY ordinal), 'UTF8')), 'hex') AS chunk_sha256
              FROM ordered
             GROUP BY ordinal / :chunk_rows
             ORDER BY chunk_ordinal
            """
        ),
        {"chunk_rows": _CHUNK_ROWS},
    )
    digest = hashlib.sha256(b"entity-address-row-chunks/v1\0")
    total_rows, expected_ordinal = 0, 0
    try:
        async for chunk_row in chunk_rows_result.mappings():
            chunk_ordinal = int(chunk_row["chunk_ordinal"])
            chunk_rows = int(chunk_row["chunk_row_count"])
            chunk_sha256 = str(chunk_row["chunk_sha256"])
            if (
                chunk_ordinal != expected_ordinal
                or not 1 <= chunk_rows <= _CHUNK_ROWS
                or _SHA256.fullmatch(chunk_sha256) is None
            ):
                raise EntityAddressArchiveReceiptError("entity-address archive row receipt is invalid")
            digest.update(struct.pack(">I", chunk_rows))
            digest.update(bytes.fromhex(chunk_sha256))
            total_rows += chunk_rows
            expected_ordinal += 1
    finally:
        await chunk_rows_result.close()
    return total_rows, digest.hexdigest()


async def capture_entity_address_archive_receipt(session, *, schema_name: str) -> EntityAddressArchiveReceipt:
    """Capture a semantic receipt for exactly the reviewed seven-table family."""
    schema = _schema_name(schema_name)
    await _normalize_receipt_session(session, schema)
    models = _models()
    await _lock_model_family(session, schema, models)
    table_receipts = []
    for model in models:
        relation_oid = await _relation_oid(session, schema, model.__tablename__)
        schema_sha256 = await _schema_identity(session, relation_oid, schema, model.__tablename__)
        row_count, row_sha256 = await _row_identity(session, schema, model.__tablename__)
        table_receipts.append(
            EntityAddressArchiveTableReceipt(model.__name__, model.__tablename__, schema_sha256, row_count, row_sha256)
        )
    tables = tuple(table_receipts)
    schema_sha256 = _canonical_digest(
        [
            {"model_name": table.model_name, "table_name": table.table_name, "schema_sha256": table.schema_sha256}
            for table in tables
        ]
    )
    content_sha256 = _canonical_digest([table.as_dict() for table in tables])
    return EntityAddressArchiveReceipt(tables, schema_sha256, content_sha256)


def validate_entity_address_archive_receipt(
    receipt: Mapping[str, Any] | EntityAddressArchiveReceipt,
) -> EntityAddressArchiveReceipt:
    """Reject malformed or model-drifting persisted receipt data before activation."""
    receipt_value = receipt.as_dict() if isinstance(receipt, EntityAddressArchiveReceipt) else receipt
    if (
        not isinstance(receipt_value, Mapping)
        or set(receipt_value) != {"contract", "receipt_version", "tables", "schema_sha256", "content_sha256"}
        or receipt_value["contract"] != CONTRACT
        or receipt_value["receipt_version"] != RECEIPT_VERSION
        or not isinstance(receipt_value["tables"], list)
    ):
        raise EntityAddressArchiveReceiptError("entity-address archive receipt is invalid")
    expected_identities = tuple((model.__name__, model.__tablename__) for model in _models())
    if len(receipt_value["tables"]) != len(expected_identities):
        raise EntityAddressArchiveReceiptError("entity-address archive receipt is invalid")
    entries = []
    for entry, identity in zip(receipt_value["tables"], expected_identities, strict=False):
        if (
            not isinstance(entry, Mapping)
            or set(entry) != {"model_name", "table_name", "schema_sha256", "row_count", "row_sha256"}
            or (entry["model_name"], entry["table_name"]) != identity
            or type(entry["row_count"]) is not int
            or entry["row_count"] < 0
            or any(_SHA256.fullmatch(str(entry[key])) is None for key in ("schema_sha256", "row_sha256"))
        ):
            raise EntityAddressArchiveReceiptError("entity-address archive receipt is invalid")
        entries.append(EntityAddressArchiveTableReceipt(**dict(entry)))
    validated = EntityAddressArchiveReceipt(
        tuple(entries),
        _canonical_digest(
            [
                {"model_name": entry.model_name, "table_name": entry.table_name, "schema_sha256": entry.schema_sha256}
                for entry in entries
            ]
        ),
        _canonical_digest([entry.as_dict() for entry in entries]),
    )
    if (
        receipt_value["schema_sha256"] != validated.schema_sha256
        or receipt_value["content_sha256"] != validated.content_sha256
    ):
        raise EntityAddressArchiveReceiptError("entity-address archive receipt is invalid")
    return validated
