# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact local ownership fences for a UUID-scoped address archive stage."""

from __future__ import annotations

import importlib
import re
from dataclasses import dataclass
from typing import Any, Mapping
from uuid import UUID

from sqlalchemy import text


entity_address_unified = importlib.import_module("process.entity_address_unified")
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_STAGE_SCHEMA_PREFIX = "entity_address_archive_"


class EntityAddressArchiveOwnershipError(RuntimeError):
    """A UUID-owned archive schema is missing, substituted, or no longer isolated."""


@dataclass(frozen=True)
class EntityAddressArchiveStageOwnership:
    """Local catalog identity for exactly one isolated seven-table archive stage."""

    dataset_id: UUID
    schema_name: str
    schema_oid: int
    relation_oids: tuple[tuple[str, int], ...]

    def as_dict(self) -> dict[str, Any]:
        """Return the bounded persisted representation without database handles."""

        return {
            "dataset_id": str(self.dataset_id),
            "schema_name": self.schema_name,
            "schema_oid": self.schema_oid,
            "relation_oids": [{"table_name": table_name, "oid": oid} for table_name, oid in self.relation_oids],
        }


def entity_address_archive_stage_schema(dataset_id: UUID) -> str:
    """Derive the only permitted archive-stage schema for one UUID owner."""

    if not isinstance(dataset_id, UUID):
        raise ValueError("entity-address archive ownership requires a UUID dataset_id")
    return _STAGE_SCHEMA_PREFIX + dataset_id.hex


def _models() -> tuple[type, ...]:
    models = (
        entity_address_unified.EntityAddressUnified,
        *entity_address_unified.SUPPORT_TABLE_MODELS,
    )
    table_names = tuple(model.__tablename__ for model in models)
    if len(models) != 7 or len(set(table_names)) != len(models):
        raise EntityAddressArchiveOwnershipError("entity-address archive ownership model family is invalid")
    if any(_IDENTIFIER.fullmatch(table_name) is None for table_name in table_names):
        raise EntityAddressArchiveOwnershipError("entity-address archive ownership model family is invalid")
    return models


def _quoted(value: str) -> str:
    return f'"{value}"'


def _require_caller_transaction(session: Any) -> None:
    if not callable(getattr(session, "in_transaction", None)) or not session.in_transaction():
        raise EntityAddressArchiveOwnershipError("entity-address archive ownership requires a caller transaction")


async def _schema_oid(session: Any, schema_name: str) -> int:
    value = await session.scalar(
        text("SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = :schema_name"),
        {"schema_name": schema_name},
    )
    if not isinstance(value, int) or value <= 0:
        raise EntityAddressArchiveOwnershipError("entity-address archive ownership schema is unavailable")
    return value


async def _table_oids(session: Any, schema_oid: int) -> tuple[tuple[str, int], ...]:
    rows = (
        await session.execute(
            text(
                "SELECT relname, oid FROM pg_catalog.pg_class "
                "WHERE relnamespace = :schema_oid AND relkind IN ('r', 'p') ORDER BY relname"
            ),
            {"schema_oid": schema_oid},
        )
    ).mappings()
    observed_pairs = tuple((str(row["relname"]), int(row["oid"])) for row in rows)
    expected_names = tuple(sorted(model.__tablename__ for model in _models()))
    if tuple(name for name, _ in observed_pairs) != expected_names or any(oid <= 0 for _, oid in observed_pairs):
        raise EntityAddressArchiveOwnershipError("entity-address archive ownership relation family differs")
    return observed_pairs


async def _namespace_relation_records(session: Any, schema_oid: int) -> list[Mapping[str, Any]]:
    """Read every relation object in one owned schema before destructive cleanup."""

    return list(
        (
            await session.execute(
                text(
                    "SELECT relation.oid, relation.relkind, indexed.indrelid AS index_table_oid "
                    "FROM pg_catalog.pg_class AS relation "
                    "LEFT JOIN pg_catalog.pg_index AS indexed ON indexed.indexrelid = relation.oid "
                    "WHERE relation.relnamespace = :schema_oid ORDER BY relation.oid"
                ),
                {"schema_oid": schema_oid},
            )
        ).mappings()
    )


async def _assert_namespace_is_owned(
    session: Any,
    *,
    schema_oid: int,
    relation_oids: tuple[tuple[str, int], ...],
) -> None:
    """Reject extra relations or references before a no-CASCADE cleanup can run."""

    owned_oids = tuple(oid for _, oid in relation_oids)
    for relation_record in await _namespace_relation_records(session, schema_oid):
        relation_oid = int(relation_record["oid"])
        relation_kind = str(relation_record["relkind"])
        if relation_kind in {"r", "p"} and relation_oid in owned_oids:
            continue
        if relation_kind == "i" and int(relation_record["index_table_oid"] or 0) in owned_oids:
            continue
        if relation_kind == "S":
            sequence_owner = await session.scalar(
                text(
                    "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_depend "
                    "WHERE classid = 'pg_class'::regclass AND objid = :sequence_oid "
                    "AND refclassid = 'pg_class'::regclass AND refobjid = ANY(:relation_oids) "
                    "AND deptype IN ('a', 'i'))"
                ),
                {"sequence_oid": relation_oid, "relation_oids": list(owned_oids)},
            )
            if sequence_owner is True:
                continue
        raise EntityAddressArchiveOwnershipError(
            "entity-address archive ownership namespace contains an unexpected relation"
        )


async def capture_created_entity_address_archive_stage(
    session: Any,
    *,
    dataset_id: UUID,
) -> EntityAddressArchiveStageOwnership:
    """Capture exact OIDs only after one caller-created stage has all seven relations."""

    _require_caller_transaction(session)
    schema_name = entity_address_archive_stage_schema(dataset_id)
    schema_oid = await _schema_oid(session, schema_name)
    relation_oids = await _table_oids(session, schema_oid)
    await _assert_namespace_is_owned(
        session,
        schema_oid=schema_oid,
        relation_oids=relation_oids,
    )
    return EntityAddressArchiveStageOwnership(
        dataset_id=dataset_id,
        schema_name=schema_name,
        schema_oid=schema_oid,
        relation_oids=relation_oids,
    )


def validate_entity_address_archive_stage_ownership(
    owner_value: Mapping[str, Any] | EntityAddressArchiveStageOwnership,
) -> EntityAddressArchiveStageOwnership:
    """Validate a persisted local owner token without consulting mutable catalogs."""

    if isinstance(owner_value, EntityAddressArchiveStageOwnership):
        return owner_value
    if not isinstance(owner_value, Mapping) or set(owner_value) != {
        "dataset_id",
        "schema_name",
        "schema_oid",
        "relation_oids",
    }:
        raise EntityAddressArchiveOwnershipError("entity-address archive ownership token is invalid")
    try:
        dataset_id = UUID(str(owner_value["dataset_id"]))
    except (TypeError, ValueError, AttributeError) as error:
        raise EntityAddressArchiveOwnershipError("entity-address archive ownership token is invalid") from error
    schema_name = owner_value["schema_name"]
    if schema_name != entity_address_archive_stage_schema(dataset_id):
        raise EntityAddressArchiveOwnershipError("entity-address archive ownership token is invalid")
    schema_oid = owner_value["schema_oid"]
    raw_relations = owner_value["relation_oids"]
    if type(schema_oid) is not int or schema_oid <= 0 or not isinstance(raw_relations, list):
        raise EntityAddressArchiveOwnershipError("entity-address archive ownership token is invalid")
    relations = []
    for entry in raw_relations:
        if (
            not isinstance(entry, Mapping)
            or set(entry) != {"table_name", "oid"}
            or not isinstance(entry["table_name"], str)
            or type(entry["oid"]) is not int
            or entry["oid"] <= 0
        ):
            raise EntityAddressArchiveOwnershipError("entity-address archive ownership token is invalid")
        relations.append((entry["table_name"], entry["oid"]))
    expected_names = tuple(sorted(model.__tablename__ for model in _models()))
    if tuple(name for name, _ in relations) != expected_names or len({oid for _, oid in relations}) != len(relations):
        raise EntityAddressArchiveOwnershipError("entity-address archive ownership token is invalid")
    return EntityAddressArchiveStageOwnership(dataset_id, schema_name, schema_oid, tuple(relations))


async def _assert_owner_current(
    session: Any,
    owner: EntityAddressArchiveStageOwnership,
) -> None:
    schema_oid = await _schema_oid(session, owner.schema_name)
    if schema_oid != owner.schema_oid:
        raise EntityAddressArchiveOwnershipError("entity-address archive ownership schema OID differs")
    if await _table_oids(session, schema_oid) != owner.relation_oids:
        raise EntityAddressArchiveOwnershipError("entity-address archive ownership relation OID differs")
    await _assert_namespace_is_owned(
        session,
        schema_oid=schema_oid,
        relation_oids=owner.relation_oids,
    )


async def verify_entity_address_archive_stage_ownership(
    session: Any,
    *,
    owner: Mapping[str, Any] | EntityAddressArchiveStageOwnership,
) -> EntityAddressArchiveStageOwnership:
    """Recheck one persisted owner token against the current local catalogs."""

    _require_caller_transaction(session)
    validated_owner = validate_entity_address_archive_stage_ownership(owner)
    await _assert_owner_current(session, validated_owner)
    return validated_owner


async def cleanup_entity_address_archive_stage(
    session: Any,
    *,
    owner: Mapping[str, Any] | EntityAddressArchiveStageOwnership,
) -> None:
    """Remove only an unchanged UUID-owned stage, using restrictive DDL only."""

    _require_caller_transaction(session)
    validated_owner = await verify_entity_address_archive_stage_ownership(session, owner=owner)
    table_names = ", ".join(
        f"{_quoted(validated_owner.schema_name)}.{_quoted(table_name)}"
        for table_name, _ in validated_owner.relation_oids
    )
    await session.execute(text(f"DROP TABLE {table_names} RESTRICT"))
    remaining = await session.scalar(
        text("SELECT COUNT(*) FROM pg_catalog.pg_class WHERE relnamespace = :schema_oid"),
        {"schema_oid": validated_owner.schema_oid},
    )
    if int(remaining or 0) != 0:
        raise EntityAddressArchiveOwnershipError("entity-address archive ownership namespace is not empty")
    await session.execute(text(f"DROP SCHEMA {_quoted(validated_owner.schema_name)}"))


__all__ = [
    "EntityAddressArchiveOwnershipError",
    "EntityAddressArchiveStageOwnership",
    "capture_created_entity_address_archive_stage",
    "cleanup_entity_address_archive_stage",
    "entity_address_archive_stage_schema",
    "validate_entity_address_archive_stage_ownership",
    "verify_entity_address_archive_stage_ownership",
]
