# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact local ownership fences for a UUID-scoped address archive stage."""

from __future__ import annotations

import importlib
import re
from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any, Mapping
from uuid import UUID

from sqlalchemy import text

entity_address_unified = importlib.import_module("process.entity_address_unified")
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_STAGE_SCHEMA_PREFIX = "entity_address_archive_"


class EntityAddressArchiveOwnershipError(RuntimeError):
    """A UUID-owned archive schema is missing, substituted, or no longer isolated."""


@dataclass(frozen=True, eq=False)
class EntityAddressArchiveStageOwnership:
    """The closed named stage; old catalog tokens are opaque compatibility data."""

    dataset_id: UUID
    schema_name: str
    schema_oid: int | None = field(default=None, compare=False)
    relation_oids: tuple[tuple[str, int], ...] = field(default=(), compare=False)
    named_relations: tuple[str, ...] = ()
    legacy_record: dict[str, Any] | None = field(default=None, compare=False, repr=False)

    @property
    def relation_names(self) -> tuple[str, ...]:
        """Return the declared table names while leaving historical tokens opaque."""
        return self.named_relations or tuple(name for name, _token in self.relation_oids)

    def __eq__(self, other):
        if not isinstance(other, EntityAddressArchiveStageOwnership):
            return NotImplemented
        return (self.dataset_id, self.schema_name, self.relation_names) == (
            other.dataset_id,
            other.schema_name,
            other.relation_names,
        )

    def as_dict(self) -> dict[str, Any]:
        """Emit names for new stages and preserve retained legacy records unchanged."""
        if self.legacy_record is not None:
            return deepcopy(self.legacy_record)
        if self.schema_oid is not None:
            relations = (
                [[name, token] for name, token in self.relation_oids]
                if self.relation_names == _supported_names()[1]
                else [{"table_name": name, "oid": token} for name, token in self.relation_oids]
            )
            return {
                "dataset_id": str(self.dataset_id),
                "schema_name": self.schema_name,
                "schema_oid": self.schema_oid,
                "relation_oids": relations,
            }
        return {
            "dataset_id": str(self.dataset_id),
            "schema_name": self.schema_name,
            "relations": [{"schema_name": self.schema_name, "relation_name": name} for name in self.relation_names],
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
    session: Any, *, dataset_id: UUID
) -> EntityAddressArchiveStageOwnership:
    """Validate the newly created closed namespace, then retain only its names."""
    _require_caller_transaction(session)
    schema_name = entity_address_archive_stage_schema(dataset_id)
    schema_oid = await _schema_oid(session, schema_name)
    relations = await _table_oids(session, schema_oid)
    await _assert_namespace_is_owned(session, schema_oid=schema_oid, relation_oids=relations)
    return EntityAddressArchiveStageOwnership(
        dataset_id, schema_name, named_relations=tuple(name for name, _ in relations)
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
        if not isinstance(table_name, str):
            raise EntityAddressArchiveOwnershipError("entity-address archive ownership token is invalid")
        relations.append((table_name, oid))
    supported_names = _supported_names()
    names = tuple(name for name, _ in relations)
    if names not in supported_names or (pair_format and names != supported_names[1]):
        raise EntityAddressArchiveOwnershipError("entity-address archive ownership token is invalid")
    return tuple(relations)


def validate_entity_address_archive_stage_ownership(
    owner_value: Mapping[str, Any] | EntityAddressArchiveStageOwnership,
) -> EntityAddressArchiveStageOwnership:
    """Decode a closed QName inventory, preserving old serialized tokens opaquely."""
    if isinstance(owner_value, EntityAddressArchiveStageOwnership):
        owner_value = owner_value.as_dict()
    if not isinstance(owner_value, Mapping):
        raise EntityAddressArchiveOwnershipError("entity-address archive ownership token is invalid")
    try:
        dataset_id = UUID(str(owner_value["dataset_id"]))
        schema = owner_value["schema_name"]
        if schema != entity_address_archive_stage_schema(dataset_id):
            raise ValueError
        if set(owner_value) == {"dataset_id", "schema_name", "relations"}:
            entries = owner_value["relations"]
            if not isinstance(entries, list) or any(
                not isinstance(entry, Mapping)
                or set(entry) != {"schema_name", "relation_name"}
                or entry["schema_name"] != schema
                for entry in entries
            ):
                raise ValueError
            names = tuple(entry["relation_name"] for entry in entries)
            if names not in _supported_names():
                raise ValueError
            return EntityAddressArchiveStageOwnership(dataset_id, schema, named_relations=names)
        if set(owner_value) != {"dataset_id", "schema_name", "schema_oid", "relation_oids"}:
            raise ValueError
        if not isinstance(owner_value["relation_oids"], list):
            raise ValueError
        pairs = _ownership_relations(owner_value["relation_oids"])
        return EntityAddressArchiveStageOwnership(
            dataset_id, schema, owner_value["schema_oid"], pairs, legacy_record=deepcopy(dict(owner_value))
        )
    except (KeyError, TypeError, ValueError, AttributeError) as error:
        raise EntityAddressArchiveOwnershipError("entity-address archive ownership token is invalid") from error


async def _assert_owner_current(session: Any, owner: EntityAddressArchiveStageOwnership) -> None:
    """Check the current named family and structural index/sequence closure."""
    schema_oid = await _schema_oid(session, owner.schema_name)
    relations = await _table_oids(session, schema_oid)
    if tuple(name for name, _local_handle in relations) != owner.relation_names:
        raise EntityAddressArchiveOwnershipError("entity-address archive ownership relation family differs")
    await _assert_namespace_is_owned(session, schema_oid=schema_oid, relation_oids=relations)


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
    session: Any, *, owner: Mapping[str, Any] | EntityAddressArchiveStageOwnership
) -> None:
    """Retire the validated named stage with native access and restrictive DDL."""
    _require_caller_transaction(session)
    owner = validate_entity_address_archive_stage_ownership(owner)
    exists = await session.scalar(
        text("SELECT to_regnamespace(:schema_name) IS NOT NULL"), {"schema_name": owner.schema_name}
    )
    if not exists:
        return
    tables = ", ".join(f"{_quoted(owner.schema_name)}.{_quoted(name)}" for name in owner.relation_names)
    await session.execute(text(f"LOCK TABLE {tables} IN ACCESS EXCLUSIVE MODE NOWAIT"))
    await verify_entity_address_archive_stage_ownership(session, owner=owner)
    await session.execute(text(f"DROP TABLE {tables} RESTRICT"))
    remaining = await session.scalar(
        text(
            "SELECT count(*) FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=:schema_name"
        ),
        {"schema_name": owner.schema_name},
    )
    if int(remaining or 0):
        raise EntityAddressArchiveOwnershipError("entity-address archive ownership namespace is not empty")
    await session.execute(text(f"DROP SCHEMA {_quoted(owner.schema_name)} RESTRICT"))


async def cleanup_entity_address_archive_publication(session, *, owner, publication, assert_unreferenced):
    """Retire exact non-serving names only after logical and reader-pin checks."""
    import asyncio

    from process.network_cms_registry_address_capture import native_driver
    from process.network_compact_layout import CompactAddressRelation
    from process.network_compact_retention import require_network_compact_artifacts_unreferenced

    _require_caller_transaction(session)
    owner = validate_entity_address_archive_stage_ownership(owner)
    _schema, relations, authority = _validated_publication_owner(owner, publication)
    if not callable(assert_unreferenced) or await assert_unreferenced(session, owner, publication) is not True:
        raise EntityAddressArchiveOwnershipError("entity-address publication remains referenced")
    names = (*relations, authority) if authority is not None else tuple(relations)
    catalog = await _retained_relation_catalog(session, names)
    _require_retained_catalog(catalog, owner, names)
    connection = await native_driver(session)
    selected_names = tuple(CompactAddressRelation(entry["schema_name"], entry["relation_name"]) for entry in names)
    projection = await require_network_compact_artifacts_unreferenced(connection, selected_names)
    tables = ",".join(f"{_quoted(entry['schema_name'])}.{_quoted(entry['relation_name'])}" for entry in names)
    await session.execute(text(f"LOCK TABLE {tables} IN ACCESS SHARE MODE NOWAIT"))
    _require_retained_catalog(await _retained_relation_catalog(session, names), owner, names)
    if authority is not None:
        alias_pairs = await _table_oids_for_names(session, owner.schema_name, (authority["relation_name"],))
        await _assert_namespace_is_owned(
            session, schema_oid=await _schema_oid(session, owner.schema_name), relation_oids=alias_pairs
        )
    if await assert_unreferenced(session, owner, publication) is not True:
        raise EntityAddressArchiveOwnershipError("entity-address publication remains referenced")
    async with asyncio.timeout(3):
        await require_network_compact_artifacts_unreferenced(connection, selected_names, expected_projection=projection)
        await session.execute(text(f"DROP TABLE {tables} RESTRICT"))
        if authority is not None:
            await session.execute(text(f"DROP SCHEMA {_quoted(owner.schema_name)} RESTRICT"))


def _validated_publication_owner(owner, publication):
    """Require explicitly relocated retired names; never discover targets from old tokens."""
    from process.entity_address_snapshot_alias import AUTHORITY_TABLE
    from process.entity_address_snapshot_serving import _schema_name

    fields = {"contract", "schema_name", "relations", "retained_relations", "alias_authority"}
    if (
        not isinstance(publication, Mapping)
        or set(publication) != fields
        or publication["contract"] != "entity-address-table-publication.v3"
    ):
        raise EntityAddressArchiveOwnershipError("entity-address publication cleanup named inventory is required")
    schema = _schema_name(publication["schema_name"])
    relations = publication["relations"]
    expected_names = tuple(model.__tablename__ for model in _models())
    if (
        not isinstance(relations, list)
        or len(relations) != len(expected_names)
        or any(
            not isinstance(entry, Mapping)
            or set(entry) != {"schema_name", "relation_name"}
            or entry["schema_name"] != schema
            or not isinstance(entry["relation_name"], str)
            or _IDENTIFIER.fullmatch(entry["relation_name"]) is None
            for entry in relations
        )
    ):
        raise EntityAddressArchiveOwnershipError("entity-address publication cleanup inventory differs")
    names = [entry["relation_name"] for entry in relations]
    if len(set(names)) != len(expected_names) or any(
        sum(name.startswith(model + "_retained_") for name in names) != 1 for model in expected_names
    ):
        raise EntityAddressArchiveOwnershipError("entity-address cleanup target remains serving or differs")
    authority = (
        {"schema_name": owner.schema_name, "relation_name": AUTHORITY_TABLE}
        if AUTHORITY_TABLE in owner.relation_names
        else None
    )
    if publication["alias_authority"] != authority:
        raise EntityAddressArchiveOwnershipError("entity-address publication cleanup inventory differs")
    return schema, relations, authority


def _require_retained_catalog(catalog_rows, owner, names):
    """Require the declared persistent, unattached named tables before restrictive DDL."""
    expected = sorted((entry["schema_name"], entry["relation_name"]) for entry in names)
    observed = sorted((entry["nspname"], entry["relname"]) for entry in catalog_rows)
    if observed != expected or any(
        entry["relkind"] != "r" or entry["relpersistence"] != "p" or entry["inherited"] for entry in catalog_rows
    ):
        raise EntityAddressArchiveOwnershipError("entity-address retained named catalog differs")


async def _retained_relation_catalog(session, names):
    qualified_names = [f"{_quoted(entry['schema_name'])}.{_quoted(entry['relation_name'])}" for entry in names]
    return (
        (
            await session.execute(
                text(
                    "SELECT n.nspname,c.relname,c.relkind::text AS relkind,c.relpersistence::text AS relpersistence,"
                    "EXISTS(SELECT 1 FROM pg_inherits i WHERE i.inhrelid=c.oid OR i.inhparent=c.oid) AS inherited "
                    "FROM unnest(CAST(:names AS text[])) requested(name) JOIN pg_class c ON c.oid=to_regclass(requested.name) "
                    "JOIN pg_namespace n ON n.oid=c.relnamespace ORDER BY n.nspname,c.relname"
                ),
                {"names": qualified_names},
            )
        )
        .mappings()
        .all()
    )


async def _table_oids_for_names(session, schema, names):
    """Resolve only local native structural handles for the named retirement namespace."""
    rows = (
        await session.execute(
            text(
                "SELECT c.relname,c.oid FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
                "WHERE n.nspname=:schema AND c.relname=ANY(CAST(:names AS text[])) ORDER BY c.relname"
            ),
            {"schema": schema, "names": list(names)},
        )
    ).mappings()
    return tuple((row["relname"], row["oid"]) for row in rows)


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
