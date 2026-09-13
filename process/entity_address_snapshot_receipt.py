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


async def _normalize_receipt_session(session) -> None:
    """Set only transaction-local textual encodings used by ``to_jsonb``."""
    if not session.in_transaction():
        raise EntityAddressArchiveReceiptError("entity-address archive receipt requires a caller transaction")
    for setting in (
        "SET LOCAL TimeZone TO 'UTC'",
        "SET LOCAL DateStyle TO 'ISO, YMD'",
        "SET LOCAL IntervalStyle TO 'iso_8601'",
        "SET LOCAL extra_float_digits TO 3",
        "SET LOCAL bytea_output TO 'hex'",
        "SET LOCAL work_mem TO '64MB'",
    ):
        await session.execute(text(setting))


async def _relation_oid(session, schema_name: str, table_name: str) -> int:
    row = (
        await session.execute(
            text(
                "SELECT relation.oid FROM pg_catalog.pg_class AS relation "
                "JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid=relation.relnamespace "
                "WHERE namespace.nspname=:schema_name AND relation.relname=:table_name"
            ),
            {"schema_name": schema_name, "table_name": table_name},
        )
    ).scalar_one_or_none()
    if not isinstance(row, int) or row <= 0:
        raise EntityAddressArchiveReceiptError("entity-address archive relation is unavailable")
    return row


def _schema_independent(value: object, schema_name: str) -> object:
    """Remove only this safe relation schema from catalog-rendered expressions."""
    if isinstance(value, str):
        return value.replace(f'"{schema_name}".', '"<archive-schema>".').replace(schema_name + ".", "<archive-schema>.")
    if isinstance(value, list):
        return [_schema_independent(entry, schema_name) for entry in value]
    if isinstance(value, dict):
        return {key: _schema_independent(entry, schema_name) for key, entry in value.items()}
    return value


async def _schema_identity(session, relation_oid: int, schema_name: str, table_name: str) -> str:
    columns = [
        dict(catalog_row)
        for catalog_row in (
            await session.execute(
                text(
                    "SELECT attribute.attnum, attribute.attname, "
                    "pg_catalog.format_type(attribute.atttypid, attribute.atttypmod) AS type, "
                    "attribute.attnotnull, attribute.attgenerated::text, attribute.attidentity::text, "
                    "pg_catalog.pg_get_expr(default_value.adbin, default_value.adrelid, true) AS default_expression "
                    "FROM pg_catalog.pg_attribute AS attribute "
                    "LEFT JOIN pg_catalog.pg_attrdef AS default_value "
                    "ON default_value.adrelid=attribute.attrelid AND default_value.adnum=attribute.attnum "
                    "WHERE attribute.attrelid=:relation_oid AND attribute.attnum>0 AND NOT attribute.attisdropped "
                    "ORDER BY attribute.attnum"
                ),
                {"relation_oid": relation_oid},
            )
        ).mappings()
    ]
    constraints = [
        dict(catalog_row)
        for catalog_row in (
            await session.execute(
                text(
                    "SELECT contype, pg_catalog.pg_get_constraintdef(oid, true) AS definition "
                    "FROM pg_catalog.pg_constraint WHERE conrelid=:relation_oid "
                    "ORDER BY contype, pg_catalog.pg_get_constraintdef(oid, true)"
                ),
                {"relation_oid": relation_oid},
            )
        ).mappings()
    ]
    indexes = [
        dict(catalog_row)
        for catalog_row in (
            await session.execute(
                text(
                    "SELECT index_row.indisunique, index_row.indisprimary, index_row.indimmediate, "
                    "access_method.amname AS method, pg_catalog.pg_get_expr(index_row.indpred, index_row.indrelid, true) AS predicate, "
                    "pg_catalog.pg_get_expr(index_row.indexprs, index_row.indrelid, true) AS expressions, "
                    "index_row.indkey::text AS keys, index_row.indoption::text AS options "
                    "FROM pg_catalog.pg_index AS index_row JOIN pg_catalog.pg_class AS index_relation "
                    "ON index_relation.oid=index_row.indexrelid JOIN pg_catalog.pg_am AS access_method "
                    "ON access_method.oid=index_relation.relam WHERE index_row.indrelid=:relation_oid "
                    "ORDER BY index_row.indisprimary DESC, index_row.indisunique DESC, access_method.amname, index_row.indkey::text"
                ),
                {"relation_oid": relation_oid},
            )
        ).mappings()
    ]
    if not columns:
        raise EntityAddressArchiveReceiptError("entity-address archive relation has no columns")
    return _canonical_digest(
        _schema_independent(
            {"table_name": table_name, "columns": columns, "constraints": constraints, "indexes": indexes}, schema_name
        )
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
    await _normalize_receipt_session(session)
    table_receipts = []
    for model in _models():
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
