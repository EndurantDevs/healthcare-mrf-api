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


async def cleanup_entity_address_archive_publication(
    session, *, owner, publication, assert_unreferenced, rotation=None, authenticate_rotation=None
):
    """Retire a named family, requiring explicit predecessor/successor authentication."""
    from process.entity_address_snapshot_alias import AUTHORITY_TABLE
    from process.network_cms_registry_address_capture import native_driver
    from process.network_compact_retention import require_named_address_artifacts_unreferenced

    _require_caller_transaction(session)
    owner = validate_entity_address_archive_stage_ownership(owner)
    schema, relations, authority = _validated_publication_owner(owner, publication)
    await _require_rotation_binding(session, owner, publication, rotation, authenticate_rotation)
    names = _publication_retained_names(schema, relations, publication, rotation=rotation)
    names.append((authority["schema_name"], AUTHORITY_TABLE))
    if not callable(assert_unreferenced) or await assert_unreferenced(session, owner, publication) is not True:
        raise EntityAddressArchiveOwnershipError("entity-address publication remains referenced")
    connection = await native_driver(session)
    await require_named_address_artifacts_unreferenced(connection, names)
    catalog_rows = await _retained_relation_catalog(session, names)
    _require_retained_catalog(catalog_rows, names)
    tables = ",".join(f"{_quoted(schema)}.{_quoted(name)}" for schema, name in names)
    await session.execute(text(f"LOCK TABLE {tables} IN ACCESS EXCLUSIVE MODE NOWAIT"))
    _require_retained_catalog(await _retained_relation_catalog(session, names), names)
    if await assert_unreferenced(session, owner, publication) is not True:
        raise EntityAddressArchiveOwnershipError("entity-address publication remains referenced")
    await require_named_address_artifacts_unreferenced(connection, names)
    await _require_rotation_binding(session, owner, publication, rotation, authenticate_rotation)
    await session.execute(text(f"DROP TABLE {tables} RESTRICT"))
    await session.execute(text(f"DROP SCHEMA {_quoted(owner.schema_name)} RESTRICT"))


async def _require_rotation_binding(session, owner, publication, rotation, authenticate):
    """Only the application can authenticate an old receipt's successor without stored IDs."""
    if rotation is not None and publication.get("serving_generation") is None:
        if not callable(authenticate) or await authenticate(session, owner, publication, rotation) is not True:
            raise EntityAddressArchiveOwnershipError("entity-address retained rotation is unauthenticated")


def _validated_publication_owner(owner, publication):
    """Validate the fixed family and archive namespace; old catalog IDs are inert."""
    from process.entity_address_snapshot_alias import AUTHORITY_TABLE
    from process.entity_address_snapshot_serving import _schema_name

    fields = {"contract", "schema_name", "relations", "retained_relations", "alias_authority"}
    if (
        not isinstance(publication, Mapping)
        or set(publication) not in (fields, fields | {"serving_generation"})
        or publication["contract"] != "entity-address-table-publication.v2"
    ):
        raise EntityAddressArchiveOwnershipError("entity-address publication cleanup proof is invalid")
    schema = _schema_name(publication["schema_name"])
    relations = publication["relations"]
    expected_names = [model.__tablename__ for model in _models()]
    authority = publication["alias_authority"]
    if (
        not isinstance(relations, list)
        or any(not isinstance(entry, Mapping) or set(entry) != {"table_name", "relation_oid"} for entry in relations)
        or [entry["table_name"] for entry in relations] != expected_names
        or not isinstance(authority, Mapping)
        or set(authority) != {"schema_name", "schema_oid", "relation_oid"}
        or authority["schema_name"] != owner.schema_name
        or AUTHORITY_TABLE not in dict(owner.relation_oids)
    ):
        raise EntityAddressArchiveOwnershipError("entity-address publication cleanup family differs")
    return schema, relations, authority


def _publication_retained_names(schema, relations, publication, *, rotation=None):
    from process.entity_address_retained_names import retained_address_name

    generation = publication.get("serving_generation")
    if generation is None and rotation is not None:
        retained = rotation.get("retained_relations") if isinstance(rotation, Mapping) else None
        if (
            not isinstance(rotation, Mapping)
            or rotation.get("contract") != "entity-address-table-publication.v2"
            or rotation.get("schema_name") != schema
            or not isinstance(retained, list)
            or len(retained) != len(relations)
            or any(
                not isinstance(entry, Mapping)
                or set(entry) != {"table_name", "relation_oid", "retained_name"}
                or entry["table_name"] != relation["table_name"]
                or not isinstance(entry["retained_name"], str)
                or re.fullmatch(
                    re.escape(relation["table_name"][:29])
                    + r"_retained_[0-9a-f]{24}|"
                    + re.escape(relation["table_name"])
                    + r"_retained_[0-9a-f]{1,8}",
                    entry["retained_name"],
                )
                is None
                for relation, entry in zip(relations, retained, strict=True)
            )
        ):
            raise EntityAddressArchiveOwnershipError("entity-address retained rotation is invalid")
        return [(schema, entry["retained_name"]) for entry in retained]
    names = []
    for entry in relations:
        table = entry["table_name"]
        if generation is not None:
            name = retained_address_name(table, generation)
        else:
            # A legacy receipt names only its old retained spelling, never a live physical object.
            ordinal = entry["relation_oid"]
            if type(ordinal) is not int or not 0 < ordinal < 2**32:
                raise EntityAddressArchiveOwnershipError("entity-address legacy retained name is unavailable")
            name = entity_address_unified._archived_identifier(f"{table}_retained_{ordinal:x}", suffix="")
        names.append((schema, name))
    return names


def _require_retained_catalog(catalog_rows, names):
    """Check the complete named table family without accepting database-local identities."""
    if (
        {(row["nspname"], row["relname"]) for row in catalog_rows} != set(names)
        or len(catalog_rows) != len(names)
        or any(
            row["relkind"] != "r"
            or row["relpersistence"] != "p"
            or row["relrowsecurity"]
            or row["relforcerowsecurity"]
            or row["inherited"]
            for row in catalog_rows
        )
    ):
        raise EntityAddressArchiveOwnershipError("entity-address retained catalog differs")


async def _retained_relation_catalog(session, names):
    return list(
        (
            await session.execute(
                text(
                    "SELECT requested.schema_name AS nspname,requested.table_name AS relname,"
                    "c.relkind::text AS relkind,c.relpersistence::text AS relpersistence,"
                    "c.relrowsecurity,c.relforcerowsecurity,"
                    "EXISTS(SELECT FROM pg_inherits i WHERE i.inhrelid=c.oid OR i.inhparent=c.oid) AS inherited "
                    "FROM unnest(CAST(:schemas AS text[]),CAST(:names AS text[])) requested(schema_name,table_name) "
                    "LEFT JOIN pg_namespace n ON n.nspname=requested.schema_name "
                    "LEFT JOIN pg_class c ON c.relnamespace=n.oid AND c.relname=requested.table_name "
                    "ORDER BY requested.schema_name,requested.table_name"
                ),
                {"schemas": [schema for schema, _ in names], "names": [name for _, name in names]},
            )
        ).mappings()
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
