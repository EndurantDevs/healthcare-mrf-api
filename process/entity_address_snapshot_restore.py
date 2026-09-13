# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Restore a verified unified-address archive into one local native stage."""

from __future__ import annotations

import importlib
import json
from dataclasses import dataclass
from typing import Any, Mapping
from uuid import UUID

from sqlalchemy import MetaData, text
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateTable

from db.connection import db
from process.entity_address_snapshot_ownership import (
    EntityAddressArchiveOwnershipError,
    EntityAddressArchiveStageOwnership,
    capture_created_entity_address_archive_stage,
    cleanup_entity_address_archive_stage,
    entity_address_archive_stage_schema,
    validate_entity_address_archive_stage_ownership,
    verify_entity_address_archive_stage_ownership,
)
from process.entity_address_snapshot_receipt import (
    EntityAddressArchiveReceipt,
    EntityAddressArchiveReceiptError,
    capture_entity_address_archive_receipt,
    validate_entity_address_archive_receipt,
)


adoption = importlib.import_module("process.entity_address_snapshot_adoption")
entity_address_unified = importlib.import_module("process.entity_address_unified")


class EntityAddressSnapshotRestoreError(RuntimeError):
    """A restored address archive is not locally owned or ready for cutover."""


@dataclass(frozen=True)
class PreparedEntityAddressSnapshotRestore:
    """One verified local stage plus the native cutover preparation it owns."""

    ownership: EntityAddressArchiveStageOwnership
    db_schema: str
    import_date: str
    stage_relation_oids: tuple[tuple[str, int], ...]
    semantic_receipt: EntityAddressArchiveReceipt
    prepared: adoption.PreparedEntityAddressSnapshotAdoption
    context: dict[str, Any]
    native_validation: dict[str, Any]

    def as_dict(self) -> dict[str, Any]:
        """Return serializable local fences for a later activation rehydration."""

        return {
            "ownership": self.ownership.as_dict(),
            "db_schema": self.db_schema,
            "import_date": self.import_date,
            "stage_relation_oids": [
                {"table_name": table_name, "oid": oid} for table_name, oid in self.stage_relation_oids
            ],
            "semantic_receipt": self.semantic_receipt.as_dict(),
            "context": _json_object(self.context, "context"),
            "native_validation": _json_object(self.native_validation, "native_validation"),
        }


def _json_object(value: Any, label: str) -> dict[str, Any]:
    """Copy one bounded JSON object and reject non-serializable coordinator state."""

    if not isinstance(value, Mapping):
        raise EntityAddressSnapshotRestoreError(f"entity-address restore {label} is invalid")
    try:
        copied = json.loads(json.dumps(dict(value), sort_keys=True, separators=(",", ":")))
    except (TypeError, ValueError) as error:
        raise EntityAddressSnapshotRestoreError(f"entity-address restore {label} is invalid") from error
    if not isinstance(copied, dict):
        raise EntityAddressSnapshotRestoreError(f"entity-address restore {label} is invalid")
    return copied


def _require_caller_transaction(session: Any) -> None:
    if not callable(getattr(session, "in_transaction", None)) or not session.in_transaction():
        raise EntityAddressSnapshotRestoreError("entity-address restore requires a caller transaction")


def _models() -> tuple[type, ...]:
    models = (
        entity_address_unified.EntityAddressUnified,
        *entity_address_unified.SUPPORT_TABLE_MODELS,
    )
    table_names = tuple(model.__tablename__ for model in models)
    if len(models) != 7 or len(set(table_names)) != 7:
        raise EntityAddressSnapshotRestoreError("entity-address restore model family is invalid")
    return models


def _quoted(value: str) -> str:
    return f'"{value}"'


def _stage_plan(
    *,
    db_schema: str,
    import_date: str,
) -> tuple[str, str, dict[str, str]]:
    """Reuse the native stage-name validator and retain the closed table mapping."""

    try:
        (
            stage_cls,
            support_stage_class_map,
            _swaps,
            _patch_statements,
            _relation_names,
            _required_names,
        ) = adoption._prepared_full_result_stage(
            db_schema=db_schema,
            import_date=import_date,
        )
    except ValueError as error:
        raise EntityAddressSnapshotRestoreError("entity-address restore destination is invalid") from error
    stage_table_by_name = {
        entity_address_unified.EntityAddressUnified.__tablename__: stage_cls.__tablename__,
        **{model.__tablename__: stage_model.__tablename__ for model, stage_model in support_stage_class_map.items()},
    }
    if (
        set(stage_table_by_name) != {model.__tablename__ for model in _models()}
        or len(set(stage_table_by_name.values())) != 7
    ):
        raise EntityAddressSnapshotRestoreError("entity-address restore stage mapping is invalid")
    return db_schema.strip(), import_date.strip(), stage_table_by_name


def _additional_index_sql(
    *,
    schema_name: str,
    table_name: str,
    stage_table_name: str,
    index: Mapping[str, Any],
) -> str:
    """Build one trusted model-defined index with its final native stage name."""

    elements = index.get("index_elements")
    index_name = index.get("name")
    if (
        not isinstance(elements, (tuple, list))
        or not elements
        or not all(isinstance(index_element, str) for index_element in elements)
    ):
        raise EntityAddressSnapshotRestoreError("entity-address restore model index is invalid")
    if not isinstance(index_name, str) or not index_name:
        index_name = "_".join(elements)
    expected_name = entity_address_unified._stage_index_name(stage_table_name, index_name)
    using = index.get("using")
    include = index.get("include") or ()
    where = index.get("where")
    if using is not None and not isinstance(using, str):
        raise EntityAddressSnapshotRestoreError("entity-address restore model index is invalid")
    if not isinstance(include, (tuple, list)) or not all(isinstance(index_element, str) for index_element in include):
        raise EntityAddressSnapshotRestoreError("entity-address restore model index is invalid")
    if where is not None and not isinstance(where, str):
        raise EntityAddressSnapshotRestoreError("entity-address restore model index is invalid")
    using_sql = f" USING {using}" if using else ""
    include_sql = f" INCLUDE ({', '.join(include)})" if include else ""
    where_sql = f" WHERE {where}" if where else ""
    return (
        f"CREATE INDEX {_quoted(expected_name)} ON {_quoted(schema_name)}.{_quoted(table_name)}"
        f"{using_sql} ({', '.join(elements)}){include_sql}{where_sql}"
    )


async def _create_model_relations(
    session: Any,
    *,
    schema_name: str,
    stage_names: Mapping[str, str],
) -> None:
    """Precreate model tables and every native additional index before data-only restore."""

    metadata = MetaData(schema=schema_name)
    table_by_name = {
        model.__tablename__: model.__table__.to_metadata(metadata, schema=schema_name) for model in _models()
    }
    for model in _models():
        table = table_by_name[model.__tablename__]
        statement = str(CreateTable(table).compile(dialect=postgresql.dialect()))
        await session.execute(text(statement))
        for index in getattr(model, "__my_additional_indexes__", ()) or ():
            await session.execute(
                text(
                    _additional_index_sql(
                        schema_name=schema_name,
                        table_name=model.__tablename__,
                        stage_table_name=stage_names[model.__tablename__],
                        index=index,
                    )
                )
            )


async def precreate_entity_address_archive_restore(
    session: Any,
    *,
    dataset_id: UUID,
    db_schema: str,
    import_date: str,
) -> EntityAddressArchiveStageOwnership:
    """Create one empty, UUID-owned seven-table target for a data-only restore."""

    _require_caller_transaction(session)
    schema_name = entity_address_archive_stage_schema(dataset_id)
    _destination_schema, _normalized_date, stage_names = _stage_plan(
        db_schema=db_schema,
        import_date=import_date,
    )
    try:
        await session.execute(text(f"CREATE SCHEMA {_quoted(schema_name)}"))
        await _create_model_relations(session, schema_name=schema_name, stage_names=stage_names)
        return await capture_created_entity_address_archive_stage(session, dataset_id=dataset_id)
    except EntityAddressArchiveOwnershipError as error:
        raise EntityAddressSnapshotRestoreError(str(error)) from error


async def _actual_receipt(
    session: Any,
    *,
    schema_name: str,
    expected: EntityAddressArchiveReceipt,
) -> None:
    """Compare the restored local rows and schema to the portable archive receipt."""

    actual = await capture_entity_address_archive_receipt(session, schema_name=schema_name)
    if actual.as_dict() != expected.as_dict():
        raise EntityAddressSnapshotRestoreError("entity-address restore semantic receipt differs")


async def _primary_index_name(session: Any, relation_oid: int) -> str:
    value = await session.scalar(
        text(
            "SELECT index_relation.relname FROM pg_catalog.pg_index AS index_meta "
            "JOIN pg_catalog.pg_class AS index_relation ON index_relation.oid = index_meta.indexrelid "
            "WHERE index_meta.indrelid = :relation_oid AND index_meta.indisprimary IS TRUE"
        ),
        {"relation_oid": relation_oid},
    )
    if not isinstance(value, str) or not value:
        raise EntityAddressSnapshotRestoreError("entity-address restore primary index is unavailable")
    return value


async def _move_owned_relations(
    session: Any,
    *,
    owner: EntityAddressArchiveStageOwnership,
    db_schema: str,
    stage_names: Mapping[str, str],
) -> tuple[tuple[str, int], ...]:
    """Move only catalog-verified owned relations and retain their stable OIDs."""

    stage_oids = []
    for table_name, relation_oid in owner.relation_oids:
        stage_table_name = stage_names[table_name]
        primary_index = await _primary_index_name(session, relation_oid)
        await session.execute(
            text(f"ALTER TABLE {_quoted(owner.schema_name)}.{_quoted(table_name)} SET SCHEMA {_quoted(db_schema)}")
        )
        await session.execute(
            text(f"ALTER TABLE {_quoted(db_schema)}.{_quoted(table_name)} RENAME TO {_quoted(stage_table_name)}")
        )
        primary_name = entity_address_unified._stage_index_name(stage_table_name, "primary")
        await session.execute(
            text(f"ALTER INDEX {_quoted(db_schema)}.{_quoted(primary_index)} RENAME TO {_quoted(primary_name)}")
        )
        stage_oids.append((stage_table_name, relation_oid))
    return tuple(sorted(stage_oids))


async def _drop_empty_owned_schema(session: Any, owner: EntityAddressArchiveStageOwnership) -> None:
    """Drop the source namespace only after every owned table left it unchanged."""

    current_oid = await session.scalar(
        text("SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = :schema_name"),
        {"schema_name": owner.schema_name},
    )
    if current_oid != owner.schema_oid:
        raise EntityAddressSnapshotRestoreError("entity-address restore ownership schema OID differs")
    remaining = await session.scalar(
        text("SELECT COUNT(*) FROM pg_catalog.pg_class WHERE relnamespace = :schema_oid"),
        {"schema_oid": owner.schema_oid},
    )
    if int(remaining or 0) != 0:
        raise EntityAddressSnapshotRestoreError("entity-address restore ownership namespace is not empty")
    await session.execute(text(f"DROP SCHEMA {_quoted(owner.schema_name)}"))


async def finalize_entity_address_archive_restore(
    session: Any,
    *,
    owner: Mapping[str, Any] | EntityAddressArchiveStageOwnership,
    semantic_receipt: Mapping[str, Any] | EntityAddressArchiveReceipt,
    db_schema: str,
    import_date: str,
) -> PreparedEntityAddressSnapshotRestore:
    """Validate, move, and natively prepare one restored local archive in one transaction."""

    _require_caller_transaction(session)
    try:
        validated_owner = await verify_entity_address_archive_stage_ownership(session, owner=owner)
        validated_receipt = validate_entity_address_archive_receipt(semantic_receipt)
    except (EntityAddressArchiveOwnershipError, EntityAddressArchiveReceiptError) as error:
        raise EntityAddressSnapshotRestoreError(str(error)) from error
    normalized_schema, normalized_date, stage_names = _stage_plan(
        db_schema=db_schema,
        import_date=import_date,
    )
    if normalized_schema == validated_owner.schema_name:
        raise EntityAddressSnapshotRestoreError("entity-address restore destination must differ from owned schema")
    await _actual_receipt(session, schema_name=validated_owner.schema_name, expected=validated_receipt)
    stage_oids = await _move_owned_relations(
        session,
        owner=validated_owner,
        db_schema=normalized_schema,
        stage_names=stage_names,
    )
    await _drop_empty_owned_schema(session, validated_owner)
    async with db.bind_existing_session(session):
        prepared = await adoption.prepare_completed_entity_address_snapshot_adoption(
            db_schema=normalized_schema,
            import_date=normalized_date,
        )
    return PreparedEntityAddressSnapshotRestore(
        ownership=validated_owner,
        db_schema=normalized_schema,
        import_date=normalized_date,
        stage_relation_oids=stage_oids,
        semantic_receipt=validated_receipt,
        prepared=prepared,
        context=_json_object(prepared.context, "context"),
        native_validation=_json_object(prepared.publish_validation, "native_validation"),
    )


def _stored_stage_oids(value: Any, stage_names: Mapping[str, str]) -> tuple[tuple[str, int], ...]:
    if not isinstance(value, list):
        raise EntityAddressSnapshotRestoreError("entity-address restore stage ownership is invalid")
    stage_oid_pairs = []
    for entry in value:
        if (
            not isinstance(entry, Mapping)
            or set(entry) != {"table_name", "oid"}
            or not isinstance(entry["table_name"], str)
            or type(entry["oid"]) is not int
            or entry["oid"] <= 0
        ):
            raise EntityAddressSnapshotRestoreError("entity-address restore stage ownership is invalid")
        stage_oid_pairs.append((entry["table_name"], entry["oid"]))
    expected_names = tuple(sorted(stage_names.values()))
    if (
        tuple(name for name, _ in sorted(stage_oid_pairs)) != expected_names
        or len({oid for _, oid in stage_oid_pairs}) != 7
    ):
        raise EntityAddressSnapshotRestoreError("entity-address restore stage ownership is invalid")
    return tuple(sorted(stage_oid_pairs))


async def _verify_stored_stage_oids(
    session: Any,
    *,
    db_schema: str,
    stage_oids: tuple[tuple[str, int], ...],
) -> None:
    """Check only local OID fences; do not rescan or rebuild a prepared stage."""

    for table_name, expected_oid in stage_oids:
        observed_oid = await session.scalar(
            text(
                "SELECT relation.oid FROM pg_catalog.pg_class AS relation "
                "JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = relation.relnamespace "
                "WHERE namespace.nspname = :schema_name AND relation.relname = :table_name "
                "AND relation.relkind IN ('r', 'p') AND relation.relpersistence = 'p'"
            ),
            {"schema_name": db_schema, "table_name": table_name},
        )
        if observed_oid != expected_oid:
            raise EntityAddressSnapshotRestoreError("entity-address restore stage OID differs")


async def rehydrate_entity_address_archive_restore(
    session: Any,
    *,
    stored: Mapping[str, Any],
) -> adoption.PreparedEntityAddressSnapshotAdoption:
    """Rebuild only the immutable native cutover plan after rechecking local fences."""

    _require_caller_transaction(session)
    required_fields = {
        "ownership",
        "db_schema",
        "import_date",
        "stage_relation_oids",
        "semantic_receipt",
        "context",
        "native_validation",
    }
    if not isinstance(stored, Mapping) or set(stored) != required_fields:
        raise EntityAddressSnapshotRestoreError("entity-address restore record is invalid")
    try:
        validate_entity_address_archive_stage_ownership(stored["ownership"])
        semantic_receipt = validate_entity_address_archive_receipt(stored["semantic_receipt"])
    except (EntityAddressArchiveOwnershipError, EntityAddressArchiveReceiptError) as error:
        raise EntityAddressSnapshotRestoreError(str(error)) from error
    db_schema = stored["db_schema"]
    import_date = stored["import_date"]
    normalized_schema, normalized_date, stage_names = _stage_plan(
        db_schema=db_schema,
        import_date=import_date,
    )
    stage_oids = _stored_stage_oids(stored["stage_relation_oids"], stage_names)
    await _verify_stored_stage_oids(session, db_schema=normalized_schema, stage_oids=stage_oids)
    (
        stage_cls,
        support_stage_class_map,
        swaps,
        patch_statements,
        relation_names,
        required_names,
    ) = adoption._prepared_full_result_stage(db_schema=normalized_schema, import_date=normalized_date)
    return adoption.PreparedEntityAddressSnapshotAdoption(
        db_schema=normalized_schema,
        stage_cls=stage_cls,
        support_stage_class_map=support_stage_class_map,
        swaps=swaps,
        patch_statements=patch_statements,
        relation_names=relation_names,
        required_names=required_names,
        context=_json_object(stored["context"], "context"),
        publish_validation=_json_object(stored["native_validation"], "native_validation"),
    )


__all__ = [
    "EntityAddressSnapshotRestoreError",
    "PreparedEntityAddressSnapshotRestore",
    "cleanup_entity_address_archive_stage",
    "finalize_entity_address_archive_restore",
    "precreate_entity_address_archive_restore",
    "rehydrate_entity_address_archive_restore",
]
