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
    """Local catalog identity for one closed legacy or set-based archive stage."""

    dataset_id: UUID
    schema_name: str
    schema_oid: int
    relation_oids: tuple[tuple[str, int], ...]

    def as_dict(self) -> dict[str, Any]:
        """Return the bounded persisted representation without database handles."""

        relations = (
            [[table_name, oid] for table_name, oid in self.relation_oids]
            if tuple(name for name, _ in self.relation_oids) == _supported_names()[1]
            else [{"table_name": table_name, "oid": oid} for table_name, oid in self.relation_oids]
        )
        return {
            "dataset_id": str(self.dataset_id),
            "schema_name": self.schema_name,
            "schema_oid": self.schema_oid,
            "relation_oids": relations,
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


def _supported_names():
    from process.entity_address_snapshot_alias import AUTHORITY_TABLE

    names = tuple(sorted(model.__tablename__ for model in _models()))
    return names, tuple(sorted((*names, AUTHORITY_TABLE)))


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
    if tuple(name for name, _ in observed_pairs) not in _supported_names() or any(
        oid <= 0 for _, oid in observed_pairs
    ):
        raise EntityAddressArchiveOwnershipError("entity-address archive ownership relation family differs")
    return observed_pairs


async def _namespace_relation_records(session: Any, schema_oid: int) -> list[Mapping[str, Any]]:
    """Read every relation object in one owned schema before destructive cleanup."""

    return list(
        (
            await session.execute(
                text(
                    "SELECT relation.oid, relation.relkind::text AS relkind, indexed.indrelid AS index_table_oid "
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


def _ownership_relations(raw_relations: list) -> tuple[tuple[str, int], ...]:
    pair_format = bool(raw_relations) and all(isinstance(entry, list) for entry in raw_relations)
    relations = []
    for entry in raw_relations:
        if pair_format and len(entry) == 2:
            table_name, oid = entry
        elif not pair_format and isinstance(entry, Mapping) and set(entry) == {"table_name", "oid"}:
            table_name, oid = entry["table_name"], entry["oid"]
        else:
            raise EntityAddressArchiveOwnershipError("entity-address archive ownership token is invalid")
        if not isinstance(table_name, str) or type(oid) is not int or oid <= 0:
            raise EntityAddressArchiveOwnershipError("entity-address archive ownership token is invalid")
        relations.append((table_name, oid))
    supported_names = _supported_names()
    names = tuple(name for name, _ in relations)
    if (
        names not in supported_names
        or (pair_format and names != supported_names[1])
        or len({oid for _, oid in relations}) != len(relations)
    ):
        raise EntityAddressArchiveOwnershipError("entity-address archive ownership token is invalid")
    return tuple(relations)


def validate_entity_address_archive_stage_ownership(
    owner_value: Mapping[str, Any] | EntityAddressArchiveStageOwnership,
) -> EntityAddressArchiveStageOwnership:
    """Validate a persisted local owner token without consulting mutable catalogs."""

    if isinstance(owner_value, EntityAddressArchiveStageOwnership):
        owner_value = owner_value.as_dict()
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
    return EntityAddressArchiveStageOwnership(dataset_id, schema_name, schema_oid, _ownership_relations(raw_relations))


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
    validated_owner = validate_entity_address_archive_stage_ownership(owner)
    current_schema_oid = await session.scalar(
        text("SELECT oid FROM pg_catalog.pg_namespace WHERE nspname=:schema_name"),
        {"schema_name": validated_owner.schema_name},
    )
    if current_schema_oid is None:
        return
    if current_schema_oid != validated_owner.schema_oid:
        raise EntityAddressArchiveOwnershipError("entity-address archive ownership schema OID differs")
    table_names = ", ".join(
        f"{_quoted(validated_owner.schema_name)}.{_quoted(table_name)}"
        for table_name, _ in validated_owner.relation_oids
    )
    await session.execute(text(f"LOCK TABLE {table_names} IN ACCESS EXCLUSIVE MODE NOWAIT"))
    await verify_entity_address_archive_stage_ownership(session, owner=validated_owner)
    await session.execute(text(f"DROP TABLE {table_names} RESTRICT"))
    remaining = await session.scalar(
        text("SELECT COUNT(*) FROM pg_catalog.pg_class WHERE relnamespace = :schema_oid"),
        {"schema_oid": validated_owner.schema_oid},
    )
    if int(remaining or 0) != 0:
        raise EntityAddressArchiveOwnershipError("entity-address archive ownership namespace is not empty")
    await session.execute(text(f"DROP SCHEMA {_quoted(validated_owner.schema_name)}"))


async def cleanup_entity_address_archive_publication(session, *, owner, publication, assert_unreferenced):
    """Retire only authenticated non-serving physical OIDs after the local pin check."""
    from process import entity_address_snapshot_preparation as protected
    from process.entity_address_snapshot_alias import AUTHORITY_TABLE

    _require_caller_transaction(session)
    owner = validate_entity_address_archive_stage_ownership(owner)
    protected_oid = await protected._publisher_authority(session)
    schema, expected_relations, authority = _validated_publication_owner(owner, publication)
    if not callable(assert_unreferenced) or await assert_unreferenced(session, owner, publication) is not True:
        raise EntityAddressArchiveOwnershipError("entity-address publication remains referenced")
    oids = [entry["relation_oid"] for entry in expected_relations] + [authority["relation_oid"]]
    catalog_rows = await _retained_relation_catalog(session, oids)
    _require_retained_catalog(catalog_rows, protected_oid, owner, schema, expected_relations)
    tables = ",".join(f"{_quoted(entry['nspname'])}.{_quoted(entry['relname'])}" for entry in catalog_rows)
    await session.execute(text(f"LOCK TABLE {tables} IN ACCESS EXCLUSIVE MODE NOWAIT"))
    if await _retained_relation_catalog(session, oids) != catalog_rows:
        raise EntityAddressArchiveOwnershipError("entity-address cleanup catalog changed")
    if await _schema_oid(session, owner.schema_name) != owner.schema_oid:
        raise EntityAddressArchiveOwnershipError("entity-address cleanup namespace changed")
    await _assert_namespace_is_owned(
        session, schema_oid=owner.schema_oid, relation_oids=((AUTHORITY_TABLE, authority["relation_oid"]),)
    )
    if await assert_unreferenced(session, owner, publication) is not True:
        raise EntityAddressArchiveOwnershipError("entity-address publication remains referenced")
    await session.execute(text(f"DROP TABLE {tables} RESTRICT"))
    await session.execute(text(f"DROP SCHEMA {_quoted(owner.schema_name)} RESTRICT"))


def _validated_publication_owner(owner, publication):
    """Bind the local publication to exactly seven serving OIDs and one retained auxiliary OID."""
    from process.entity_address_snapshot_alias import AUTHORITY_TABLE
    from process.entity_address_snapshot_serving import _schema_name

    table_names = tuple(model.__tablename__ for model in _models())
    if (
        not isinstance(publication, Mapping)
        or set(publication) != {"contract", "schema_name", "relations", "retained_relations", "alias_authority"}
        or publication["contract"] != "entity-address-table-publication.v2"
    ):
        raise EntityAddressArchiveOwnershipError("entity-address publication cleanup proof is invalid")
    schema = _schema_name(publication["schema_name"])
    expected_relations = [{"table_name": name, "relation_oid": dict(owner.relation_oids)[name]} for name in table_names]
    authority_by_field = {
        "schema_name": owner.schema_name,
        "schema_oid": owner.schema_oid,
        "relation_oid": dict(owner.relation_oids).get(AUTHORITY_TABLE),
    }
    if (
        publication["relations"] != expected_relations
        or publication["alias_authority"] != authority_by_field
        or authority_by_field["relation_oid"] is None
    ):
        raise EntityAddressArchiveOwnershipError("entity-address publication cleanup ownership differs")
    return schema, expected_relations, authority_by_field


def _require_retained_catalog(catalog_rows, protected_oid, owner, schema, expected_relations):
    """Admit only protected, unattached physical tables at their exact retired names."""
    from process.entity_address_snapshot_alias import AUTHORITY_TABLE

    if len(catalog_rows) != 8 or any(
        catalog_row["relowner"] != protected_oid
        or catalog_row["relkind"] != "r"
        or catalog_row["relpersistence"] != "p"
        or catalog_row["relrowsecurity"]
        or catalog_row["relforcerowsecurity"]
        or catalog_row["inherited"]
        for catalog_row in catalog_rows
    ):
        raise EntityAddressArchiveOwnershipError("entity-address retained catalog differs")
    for catalog_row in catalog_rows:
        original = next(
            (entry["table_name"] for entry in expected_relations if entry["relation_oid"] == catalog_row["oid"]), None
        )
        expected_name = (
            entity_address_unified._archived_identifier(f"{original}_retained_{catalog_row['oid']:x}", suffix="")
            if original
            else AUTHORITY_TABLE
        )
        expected_schema = schema if original else owner.schema_name
        if (catalog_row["nspname"], catalog_row["relname"]) != (expected_schema, expected_name):
            raise EntityAddressArchiveOwnershipError("entity-address cleanup target is serving or substituted")


async def _retained_relation_catalog(session, oids):
    return (
        (
            await session.execute(
                text(
                    "SELECT c.oid,n.nspname,c.relname,c.relowner,c.relkind,c.relpersistence,c.relrowsecurity,c.relforcerowsecurity,"
                    "EXISTS(SELECT 1 FROM pg_inherits i WHERE i.inhrelid=c.oid OR i.inhparent=c.oid) AS inherited "
                    "FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.oid=ANY(:oids) ORDER BY c.oid"
                ),
                {"oids": oids},
            )
        )
        .mappings()
        .all()
    )


__all__ = [
    "EntityAddressArchiveOwnershipError",
    "EntityAddressArchiveStageOwnership",
    "capture_created_entity_address_archive_stage",
    "cleanup_entity_address_archive_stage",
    "cleanup_entity_address_archive_publication",
    "entity_address_archive_stage_schema",
    "validate_entity_address_archive_stage_ownership",
    "verify_entity_address_archive_stage_ownership",
]
