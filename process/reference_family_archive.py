# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed native archive mechanics for replacement-style reference families.

This module does not register importers or infer publication authority.  It
only pins, clones, validates, and manually activates four reviewed model
families.  The caller owns native ``pg_dump``/``pg_restore`` execution and the
transaction that makes a validated stage live.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
from collections.abc import Awaitable, Callable, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateTable, MetaData

from db import models
from process import entity_address_snapshot_receipt as catalog_identity

CONTRACT = "reference-replacement-family.postgres.v1"
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_SNAPSHOT = re.compile(r"^[0-9A-Fa-f-]+$")
_STAGE_PREFIX = "reference_family_archive_"
_PREDECESSOR_PREFIX = "reference_family_predecessor_"
_LOCK_TIMEOUT = "500ms"
_CAPTURE_TIMEOUT = "5s"
_MAX_METADATA_BYTES = 16_384


class ReferenceFamilyArchiveError(RuntimeError):
    """A closed family archive or its local ownership fence is invalid."""


@dataclass(frozen=True)
class ReferenceFamilySpec:
    """One reviewed replacement family; no names come from configuration."""

    importer_id: str
    model_types: tuple[type, ...]

    @property
    def table_names(self) -> tuple[str, ...]:
        """Return the exact ordered relation names owned by this family."""

        return tuple(model_type.__tablename__ for model_type in self.model_types)


@dataclass(frozen=True)
class ReferenceTableReceipt:
    """Portable schema and exact row-count identity for one relation."""

    model_name: str
    table_name: str
    schema_sha256: str
    row_count: int

    def as_dict(self) -> dict[str, Any]:
        """Return the portable representation used by the family manifest."""

        return {
            "model_name": self.model_name,
            "table_name": self.table_name,
            "schema_sha256": self.schema_sha256,
            "row_count": self.row_count,
        }


@dataclass(frozen=True)
class ReferenceFamilyManifest:
    """Portable semantics with explicit provenance but no ordering authority."""

    importer_id: str
    tables: tuple[ReferenceTableReceipt, ...]
    source_metadata: Mapping[str, Any]
    source_metadata_sha256: str
    schema_sha256: str

    def as_dict(self) -> dict[str, Any]:
        """Return the strict portable manual-only archive manifest."""

        return {
            "contract": CONTRACT,
            "importer_id": self.importer_id,
            "publication_authority": "manual-only",
            "tables": [table.as_dict() for table in self.tables],
            "source_metadata": dict(self.source_metadata),
            "source_metadata_sha256": self.source_metadata_sha256,
            "schema_sha256": self.schema_sha256,
        }


@dataclass(frozen=True)
class ReferenceFamilySourceCapture:
    """One pinned live source family for a consistent clone transaction."""

    manifest: ReferenceFamilyManifest
    schema_name: str
    postgres_snapshot: str


@dataclass(frozen=True)
class ReferenceFamilyStageOwnership:
    """Exact local catalog ownership for one UUID-derived stage schema."""

    importer_id: str
    dataset_id: UUID
    schema_name: str
    schema_oid: int
    relation_oids: tuple[tuple[str, int], ...]


@dataclass(frozen=True)
class ReferenceFamilyStageCapture:
    """A validated committed clone pinned while native archive copy runs."""

    manifest: ReferenceFamilyManifest
    ownership: ReferenceFamilyStageOwnership
    postgres_snapshot: str


@dataclass(frozen=True)
class ReferenceFamilyIncumbent:
    """Compare-and-swap token for the complete destination family."""

    importer_id: str
    schema_name: str
    relation_oids: tuple[tuple[str, int | None], ...]


@dataclass(frozen=True)
class ReferenceFamilyActivationReceipt:
    """Destination-local identity captured before the activation commits."""

    importer_id: str
    source_metadata_sha256: str
    relation_oids: tuple[tuple[str, int], ...]
    predecessor_oids: tuple[tuple[str, int | None], ...]
    predecessor_schema_name: str | None
    tables: tuple[ReferenceTableReceipt, ...]


_SPECS = {
    spec.importer_id: spec
    for spec in (
        ReferenceFamilySpec(
            "plan-attributes",
            (models.PlanAttributes, models.PlanPrices, models.PlanRatingAreas, models.PlanBenefits),
        ),
        ReferenceFamilySpec("places-zcta", (models.PricingPlacesZcta,)),
        ReferenceFamilySpec("lodes", (models.LODESWorkplaceAggregate,)),
        ReferenceFamilySpec(
            "medicare-enrollment",
            (models.MedicareEnrollmentCountyStats, models.MedicareEnrollmentStats),
        ),
    )
}


def reference_family_spec(importer_id: str) -> ReferenceFamilySpec:
    """Resolve only a compiled-in reviewed importer family."""

    try:
        spec = _SPECS[importer_id]
    except (KeyError, TypeError) as error:
        raise ReferenceFamilyArchiveError("reference family importer is unsupported") from error
    if (
        not spec.model_types
        or len(set(spec.table_names)) != len(spec.table_names)
        or any(_IDENTIFIER.fullmatch(table_name) is None for table_name in spec.table_names)
    ):
        raise ReferenceFamilyArchiveError("reference family model declaration is invalid")
    return spec


def reference_family_stage_schema(dataset_id: UUID) -> str:
    """Derive the only admitted local stage namespace from a UUID owner."""

    if not isinstance(dataset_id, UUID):
        raise ReferenceFamilyArchiveError("reference family stage requires a UUID dataset_id")
    return _STAGE_PREFIX + dataset_id.hex


def _quoted(value: str) -> str:
    if _IDENTIFIER.fullmatch(value) is None:
        raise ReferenceFamilyArchiveError("reference family identifier is invalid")
    return f'"{value}"'


def _schema_name(value: object) -> str:
    if not isinstance(value, str):
        raise ReferenceFamilyArchiveError("reference family schema is invalid")
    normalized = value.strip()
    if _IDENTIFIER.fullmatch(normalized) is None or len(normalized.encode()) > 63:
        raise ReferenceFamilyArchiveError("reference family schema is invalid")
    return normalized


def _canonical_json(value: object) -> bytes:
    try:
        encoded = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("ascii")
    except (TypeError, ValueError, UnicodeEncodeError) as error:
        raise ReferenceFamilyArchiveError("reference family source metadata is invalid") from error
    return encoded


def _source_metadata(value: object) -> tuple[dict[str, Any], str]:
    if not isinstance(value, Mapping) or not value:
        raise ReferenceFamilyArchiveError("reference family source metadata is required")
    encoded = _canonical_json(dict(value))
    if len(encoded) > _MAX_METADATA_BYTES:
        raise ReferenceFamilyArchiveError("reference family source metadata is too large")
    metadata = json.loads(encoded.decode("ascii"))
    if not isinstance(metadata, dict) or not metadata:
        raise ReferenceFamilyArchiveError("reference family source metadata is invalid")
    return metadata, hashlib.sha256(b"reference-family-source-metadata/v1\0" + encoded).hexdigest()


def _schema_digest(receipts: list[ReferenceTableReceipt] | tuple[ReferenceTableReceipt, ...]) -> str:
    schema_receipts = [
        {
            "model_name": receipt.model_name,
            "table_name": receipt.table_name,
            "schema_sha256": receipt.schema_sha256,
        }
        for receipt in receipts
    ]
    return hashlib.sha256(b"reference-family-schema/v1\0" + _canonical_json(schema_receipts)).hexdigest()


def _require_transaction(session: Any) -> None:
    if not callable(getattr(session, "in_transaction", None)) or not session.in_transaction():
        raise ReferenceFamilyArchiveError("reference family operation requires a caller transaction")


async def _relation_oid(session: Any, schema_name: str, table_name: str) -> int | None:
    value = await session.scalar(
        text(
            "SELECT relation.oid FROM pg_catalog.pg_class AS relation "
            "JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid=relation.relnamespace "
            "WHERE namespace.nspname=:schema_name AND relation.relname=:table_name "
            "AND relation.relkind='r' AND relation.relpersistence='p' "
            "AND NOT relation.relrowsecurity AND NOT relation.relforcerowsecurity"
        ),
        {"schema_name": schema_name, "table_name": table_name},
    )
    if value is None:
        return None
    if type(value) is not int or value <= 0:
        raise ReferenceFamilyArchiveError("reference family relation is unavailable")
    return value


async def _timeout_value(session: Any, setting: str) -> str:
    value = await session.scalar(text(f"SHOW {setting}"))
    if not isinstance(value, str) or not value:
        raise ReferenceFamilyArchiveError("reference family timeout state is unavailable")
    return value


async def _set_local_timeout(session: Any, setting: str, value: str) -> None:
    await session.execute(
        text("SELECT pg_catalog.set_config(:setting, :value, true)"),
        {"setting": setting, "value": value},
    )


@asynccontextmanager
async def _bounded_capture(session: Any):
    """Bound lock/capture work, then restore caller settings before cloning."""

    previous_lock = await _timeout_value(session, "lock_timeout")
    previous_statement = await _timeout_value(session, "statement_timeout")
    await _set_local_timeout(session, "lock_timeout", _LOCK_TIMEOUT)
    await _set_local_timeout(session, "statement_timeout", _CAPTURE_TIMEOUT)
    try:
        yield
    finally:
        await _set_local_timeout(session, "lock_timeout", previous_lock)
        await _set_local_timeout(session, "statement_timeout", previous_statement)


async def _lock_family(
    session: Any,
    schema_name: str,
    table_names: tuple[str, ...],
    mode: str,
    *,
    nowait: bool = False,
) -> None:
    relations = ", ".join(f"{_quoted(schema_name)}.{_quoted(name)}" for name in table_names)
    await session.execute(text(f"LOCK TABLE {relations} IN {mode} MODE{' NOWAIT' if nowait else ''}"))


async def _table_receipt(
    session: Any,
    *,
    schema_name: str,
    model_type: type,
) -> ReferenceTableReceipt:
    table_name = model_type.__tablename__
    relation_oid = await _relation_oid(session, schema_name, table_name)
    if relation_oid is None:
        raise ReferenceFamilyArchiveError("reference family relation is missing")
    try:
        schema_sha256 = await catalog_identity._schema_identity(
            session,
            relation_oid,
            schema_name,
            table_name,
        )
    except Exception as error:
        raise ReferenceFamilyArchiveError("reference family schema identity is unavailable") from error
    row_count = await session.scalar(text(f"SELECT count(*)::bigint FROM {_quoted(schema_name)}.{_quoted(table_name)}"))
    if type(row_count) is not int or row_count < 0:
        raise ReferenceFamilyArchiveError("reference family row count is invalid")
    return ReferenceTableReceipt(model_type.__name__, table_name, schema_sha256, row_count)


async def _family_manifest(
    session: Any,
    *,
    spec: ReferenceFamilySpec,
    schema_name: str,
    source_metadata: Mapping[str, Any],
) -> ReferenceFamilyManifest:
    metadata, metadata_sha256 = _source_metadata(source_metadata)
    receipts = tuple(
        [
            await _table_receipt(session, schema_name=schema_name, model_type=model_type)
            for model_type in spec.model_types
        ]
    )
    schema_sha256 = _schema_digest(receipts)
    return ReferenceFamilyManifest(
        spec.importer_id,
        receipts,
        metadata,
        metadata_sha256,
        schema_sha256,
    )


def validate_reference_family_manifest(manifest_value: object) -> ReferenceFamilyManifest:
    """Validate the portable closed-family receipt without granting authority."""

    if isinstance(manifest_value, ReferenceFamilyManifest):
        manifest_value = manifest_value.as_dict()
    if not isinstance(manifest_value, Mapping) or set(manifest_value) != {
        "contract",
        "importer_id",
        "publication_authority",
        "tables",
        "source_metadata",
        "source_metadata_sha256",
        "schema_sha256",
    }:
        raise ReferenceFamilyArchiveError("reference family manifest is invalid")
    if manifest_value["contract"] != CONTRACT or manifest_value["publication_authority"] != "manual-only":
        raise ReferenceFamilyArchiveError("reference family manifest is not manual-only")
    spec = reference_family_spec(manifest_value["importer_id"])
    metadata, metadata_sha256 = _source_metadata(manifest_value["source_metadata"])
    raw_tables = manifest_value["tables"]
    if not isinstance(raw_tables, list) or len(raw_tables) != len(spec.model_types):
        raise ReferenceFamilyArchiveError("reference family manifest table set is invalid")
    receipts = []
    for raw_table, model_type in zip(raw_tables, spec.model_types, strict=True):
        if not isinstance(raw_table, Mapping) or set(raw_table) != {
            "model_name",
            "table_name",
            "schema_sha256",
            "row_count",
        }:
            raise ReferenceFamilyArchiveError("reference family table receipt is invalid")
        if (
            raw_table["model_name"] != model_type.__name__
            or raw_table["table_name"] != model_type.__tablename__
            or not re.fullmatch(r"[0-9a-f]{64}", str(raw_table["schema_sha256"]))
            or type(raw_table["row_count"]) is not int
            or raw_table["row_count"] < 0
        ):
            raise ReferenceFamilyArchiveError("reference family table receipt is invalid")
        receipts.append(ReferenceTableReceipt(**dict(raw_table)))
    schema_sha256 = _schema_digest(receipts)
    if manifest_value["source_metadata_sha256"] != metadata_sha256 or manifest_value["schema_sha256"] != schema_sha256:
        raise ReferenceFamilyArchiveError("reference family manifest digest differs")
    return ReferenceFamilyManifest(spec.importer_id, tuple(receipts), metadata, metadata_sha256, schema_sha256)


async def capture_reference_family_source(
    session: Any,
    *,
    importer_id: str,
    schema_name: str,
    source_metadata: Mapping[str, Any],
) -> ReferenceFamilySourceCapture:
    """Pin and describe one exact live family under the caller transaction."""

    _require_transaction(session)
    spec = reference_family_spec(importer_id)
    schema = _schema_name(schema_name)
    _source_metadata(source_metadata)
    await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
    async with _bounded_capture(session):
        await _lock_family(session, schema, spec.table_names, "SHARE")
        manifest = await _family_manifest(
            session,
            spec=spec,
            schema_name=schema,
            source_metadata=source_metadata,
        )
        snapshot = (await session.execute(text("SELECT pg_export_snapshot()"))).scalar_one()
        if not isinstance(snapshot, str) or _SNAPSHOT.fullmatch(snapshot) is None:
            raise ReferenceFamilyArchiveError("reference family source snapshot is invalid")
    return ReferenceFamilySourceCapture(manifest, schema, snapshot)


async def _clone_source(session: Any, capture: ReferenceFamilySourceCapture, stage_schema: str) -> None:
    if _SNAPSHOT.fullmatch(capture.postgres_snapshot) is None:
        raise ReferenceFamilyArchiveError("reference family source snapshot is invalid")
    await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
    await session.execute(text(f"SET TRANSACTION SNAPSHOT '{capture.postgres_snapshot}'"))
    await session.execute(text(f"CREATE SCHEMA {_quoted(stage_schema)}"))
    for table in capture.manifest.tables:
        source_ref = f"{_quoted(capture.schema_name)}.{_quoted(table.table_name)}"
        stage_ref = f"{_quoted(stage_schema)}.{_quoted(table.table_name)}"
        await session.execute(text(f"CREATE TABLE {stage_ref} (LIKE {source_ref} INCLUDING ALL)"))
        await session.execute(text(f"INSERT INTO {stage_ref} SELECT * FROM {source_ref}"))


async def _schema_oid(session: Any, schema_name: str) -> int:
    value = await session.scalar(
        text("SELECT oid FROM pg_catalog.pg_namespace WHERE nspname=:schema_name"),
        {"schema_name": schema_name},
    )
    if type(value) is not int or value <= 0:
        raise ReferenceFamilyArchiveError("reference family owned schema is unavailable")
    return value


async def _namespace_relations(session: Any, schema_oid: int) -> list[Mapping[str, Any]]:
    return list(
        (
            await session.execute(
                text(
                    "SELECT relation.oid, relation.relname, relation.relkind::text AS relkind, "
                    "indexed.indrelid AS index_table_oid FROM pg_catalog.pg_class AS relation "
                    "LEFT JOIN pg_catalog.pg_index AS indexed ON indexed.indexrelid=relation.oid "
                    "WHERE relation.relnamespace=:schema_oid ORDER BY relation.oid"
                ),
                {"schema_oid": schema_oid},
            )
        ).mappings()
    )


async def capture_reference_family_stage_ownership(
    session: Any,
    *,
    importer_id: str,
    dataset_id: UUID,
) -> ReferenceFamilyStageOwnership:
    """Capture exact OIDs only for a complete, otherwise empty owned schema."""

    _require_transaction(session)
    spec = reference_family_spec(importer_id)
    schema_name = reference_family_stage_schema(dataset_id)
    schema_oid = await _schema_oid(session, schema_name)
    relation_oids = []
    for table_name in sorted(spec.table_names):
        relation_oid = await _relation_oid(session, schema_name, table_name)
        if relation_oid is None:
            raise ReferenceFamilyArchiveError("reference family owned relation is missing")
        relation_oids.append((table_name, relation_oid))
    owned_oids = {oid for _, oid in relation_oids}
    for relation_row in await _namespace_relations(session, schema_oid):
        kind, relation_oid = str(relation_row["relkind"]), int(relation_row["oid"])
        if kind == "r" and relation_oid in owned_oids:
            continue
        if kind == "i" and int(relation_row["index_table_oid"] or 0) in owned_oids:
            continue
        raise ReferenceFamilyArchiveError("reference family owned schema contains an unexpected relation")
    return ReferenceFamilyStageOwnership(
        spec.importer_id,
        dataset_id,
        schema_name,
        schema_oid,
        tuple(relation_oids),
    )


async def verify_reference_family_stage_ownership(
    session: Any,
    ownership: ReferenceFamilyStageOwnership,
) -> ReferenceFamilyStageOwnership:
    """Recheck a local owner token against current catalog identities."""

    _require_transaction(session)
    if not isinstance(ownership, ReferenceFamilyStageOwnership):
        raise ReferenceFamilyArchiveError("reference family stage ownership is invalid")
    observed = await capture_reference_family_stage_ownership(
        session,
        importer_id=ownership.importer_id,
        dataset_id=ownership.dataset_id,
    )
    if observed != ownership:
        raise ReferenceFamilyArchiveError("reference family stage ownership differs")
    return observed


async def _validate_stage_manifest(
    session: Any,
    *,
    ownership: ReferenceFamilyStageOwnership,
    manifest: ReferenceFamilyManifest,
) -> tuple[ReferenceTableReceipt, ...]:
    validated = validate_reference_family_manifest(manifest)
    if validated.importer_id != ownership.importer_id:
        raise ReferenceFamilyArchiveError("reference family stage scope differs")
    spec = reference_family_spec(validated.importer_id)
    observed = await _family_manifest(
        session,
        spec=spec,
        schema_name=ownership.schema_name,
        source_metadata=validated.source_metadata,
    )
    if observed.as_dict() != validated.as_dict():
        raise ReferenceFamilyArchiveError("reference family restored stage differs")
    return observed.tables


async def cleanup_reference_family_stage(
    session: Any,
    ownership: ReferenceFamilyStageOwnership,
) -> None:
    """Drop only an unchanged UUID-owned stage using restrictive DDL."""

    _require_transaction(session)
    current_schema_oid = await session.scalar(
        text("SELECT oid FROM pg_catalog.pg_namespace WHERE nspname=:schema_name"),
        {"schema_name": ownership.schema_name},
    )
    if current_schema_oid is None:
        return
    await _lock_family(
        session,
        ownership.schema_name,
        tuple(name for name, _ in ownership.relation_oids),
        "ACCESS EXCLUSIVE",
        nowait=True,
    )
    await verify_reference_family_stage_ownership(session, ownership)
    relations = ", ".join(f"{_quoted(ownership.schema_name)}.{_quoted(name)}" for name, _ in ownership.relation_oids)
    await session.execute(text(f"DROP TABLE {relations} RESTRICT"))
    if int(
        await session.scalar(
            text("SELECT count(*) FROM pg_catalog.pg_class WHERE relnamespace=:schema_oid"),
            {"schema_oid": ownership.schema_oid},
        )
        or 0
    ):
        raise ReferenceFamilyArchiveError("reference family owned schema is not empty")
    await session.execute(text(f"DROP SCHEMA {_quoted(ownership.schema_name)}"))


async def _shielded_cleanup(session_factory: Any, ownership: ReferenceFamilyStageOwnership) -> None:
    async def cleanup() -> None:
        """Clean the exact stage in an independent transaction."""

        async with session_factory() as session, session.begin():
            await cleanup_reference_family_stage(session, ownership)

    task = asyncio.create_task(cleanup())
    is_cancelled = False
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            is_cancelled = True
    task.result()
    if is_cancelled:
        raise asyncio.CancelledError


async def export_reference_family_archive(
    session_factory: Any,
    *,
    importer_id: str,
    schema_name: str,
    source_metadata: Mapping[str, Any],
    dataset_id: UUID,
    archive_copy: Callable[[ReferenceFamilyStageCapture], Awaitable[None]],
) -> ReferenceFamilyManifest:
    """Clone, validate, dump, and exactly clean one closed family stage."""

    stage_schema = reference_family_stage_schema(dataset_id)
    ownership = None
    try:
        async with session_factory() as source_session, source_session.begin():
            capture = await capture_reference_family_source(
                source_session,
                importer_id=importer_id,
                schema_name=schema_name,
                source_metadata=source_metadata,
            )
            async with session_factory() as clone_session, clone_session.begin():
                await _clone_source(clone_session, capture, stage_schema)
                ownership = await capture_reference_family_stage_ownership(
                    clone_session,
                    importer_id=importer_id,
                    dataset_id=dataset_id,
                )
        async with session_factory() as stage_session, stage_session.begin():
            await stage_session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
            async with _bounded_capture(stage_session):
                await _lock_family(
                    stage_session,
                    stage_schema,
                    reference_family_spec(importer_id).table_names,
                    "SHARE",
                )
                await verify_reference_family_stage_ownership(stage_session, ownership)
                await _validate_stage_manifest(stage_session, ownership=ownership, manifest=capture.manifest)
                snapshot = (await stage_session.execute(text("SELECT pg_export_snapshot()"))).scalar_one()
                if not isinstance(snapshot, str) or _SNAPSHOT.fullmatch(snapshot) is None:
                    raise ReferenceFamilyArchiveError("reference family stage snapshot is invalid")
            await archive_copy(ReferenceFamilyStageCapture(capture.manifest, ownership, snapshot))
        return capture.manifest
    finally:
        if ownership is not None:
            await _shielded_cleanup(session_factory, ownership)


def _additional_index_sql(schema_name: str, model_type: type, index_spec: Mapping[str, Any]) -> str:
    allowed_keys = {"index_elements", "name", "using", "unique", "include"}
    if set(index_spec) - allowed_keys:
        raise ReferenceFamilyArchiveError("reference family model index is unsupported")
    elements = index_spec.get("index_elements")
    if (
        not isinstance(elements, (tuple, list))
        or not elements
        or not all(
            isinstance(index_element, str)
            and all(_IDENTIFIER.fullmatch(token) is not None for token in index_element.split())
            and len(index_element.split()) <= 2
            for index_element in elements
        )
    ):
        raise ReferenceFamilyArchiveError("reference family model index is invalid")
    suffix = index_spec.get("name", "_".join(elements))
    if not isinstance(suffix, str) or _IDENTIFIER.fullmatch(suffix) is None:
        raise ReferenceFamilyArchiveError("reference family model index is invalid")
    index_name = f"{model_type.__tablename__}_idx_{suffix}"[:63]
    method = index_spec.get("using")
    if method is not None and method not in {"btree", "gin", "gist", "hash", "brin", "spgist"}:
        raise ReferenceFamilyArchiveError("reference family model index method is invalid")
    is_unique = index_spec.get("unique", False)
    if type(is_unique) is not bool:
        raise ReferenceFamilyArchiveError("reference family model index uniqueness is invalid")
    included_columns = index_spec.get("include", ())
    if not isinstance(included_columns, (tuple, list)) or not all(
        isinstance(column_name, str) and _IDENTIFIER.fullmatch(column_name) is not None
        for column_name in included_columns
    ):
        raise ReferenceFamilyArchiveError("reference family model index include is invalid")
    using = f" USING {method}" if method else ""
    unique = "UNIQUE " if is_unique else ""
    include = f" INCLUDE ({', '.join(included_columns)})" if included_columns else ""
    return (
        f"CREATE {unique}INDEX {_quoted(index_name)} ON {_quoted(schema_name)}."
        f"{_quoted(model_type.__tablename__)}{using} ({', '.join(elements)}){include}"
    )


async def precreate_reference_family_restore(
    session: Any,
    *,
    importer_id: str,
    dataset_id: UUID,
) -> ReferenceFamilyStageOwnership:
    """Create an empty model-complete target for a native data-only restore."""

    _require_transaction(session)
    spec = reference_family_spec(importer_id)
    schema_name = reference_family_stage_schema(dataset_id)
    await session.execute(text(f"CREATE SCHEMA {_quoted(schema_name)}"))
    metadata = MetaData(schema=schema_name)
    for model_type in spec.model_types:
        table = model_type.__table__.to_metadata(metadata, schema=schema_name)
        statement = str(CreateTable(table).compile(dialect=postgresql.dialect()))
        await session.execute(text(statement))
        for index in getattr(model_type, "__my_additional_indexes__", ()) or ():
            await session.execute(text(_additional_index_sql(schema_name, model_type, index)))
    return await capture_reference_family_stage_ownership(
        session,
        importer_id=importer_id,
        dataset_id=dataset_id,
    )


async def validate_reference_family_stage(
    session: Any,
    *,
    ownership: ReferenceFamilyStageOwnership,
    manifest: Mapping[str, Any] | ReferenceFamilyManifest,
) -> tuple[ReferenceTableReceipt, ...]:
    """Validate restored schema/count semantics under an exact ownership lock."""

    _require_transaction(session)
    validated_manifest = validate_reference_family_manifest(manifest)
    async with _bounded_capture(session):
        await _lock_family(
            session,
            ownership.schema_name,
            tuple(name for name, _ in ownership.relation_oids),
            "SHARE",
        )
        await verify_reference_family_stage_ownership(session, ownership)
    return await _validate_stage_manifest(
        session,
        ownership=ownership,
        manifest=validated_manifest,
    )


async def capture_reference_family_incumbent(
    session: Any,
    *,
    importer_id: str,
    schema_name: str,
) -> ReferenceFamilyIncumbent:
    """Capture all-present or all-absent live relation OIDs for later CAS."""

    _require_transaction(session)
    spec = reference_family_spec(importer_id)
    schema = _schema_name(schema_name)
    async with _bounded_capture(session):
        pairs = await _incumbent_pairs(session, spec, schema)
        present_flags = [oid is not None for _, oid in pairs]
        if any(present_flags) and not all(present_flags):
            raise ReferenceFamilyArchiveError("reference family incumbent is incomplete")
        if all(present_flags):
            await _lock_family(session, schema, spec.table_names, "SHARE")
            if await _incumbent_pairs(session, spec, schema) != pairs:
                raise ReferenceFamilyArchiveError("reference family incumbent changed during capture")
    return ReferenceFamilyIncumbent(spec.importer_id, schema, pairs)


async def _incumbent_pairs(
    session: Any,
    spec: ReferenceFamilySpec,
    schema_name: str,
) -> tuple[tuple[str, int | None], ...]:
    relation_oids = []
    for table_name in spec.table_names:
        relation_oids.append((table_name, await _relation_oid(session, schema_name, table_name)))
    return tuple(relation_oids)


async def _verify_incumbent(session: Any, expected: ReferenceFamilyIncumbent) -> None:
    spec = reference_family_spec(expected.importer_id)
    observed_oids = await _incumbent_pairs(session, spec, expected.schema_name)
    if observed_oids != expected.relation_oids:
        raise ReferenceFamilyArchiveError("reference family incumbent changed")


def reference_family_predecessor_schema(dataset_id: UUID) -> str:
    """Derive a collision-free retained namespace from the new UUID owner."""

    if not isinstance(dataset_id, UUID):
        raise ReferenceFamilyArchiveError("reference family predecessor requires a UUID dataset_id")
    return _PREDECESSOR_PREFIX + dataset_id.hex


async def _lock_and_verify_activation(
    session: Any,
    spec: ReferenceFamilySpec,
    ownership: ReferenceFamilyStageOwnership,
    expected_incumbent: ReferenceFamilyIncumbent,
) -> None:
    async with _bounded_capture(session):
        await _lock_family(session, ownership.schema_name, spec.table_names, "ACCESS EXCLUSIVE")
        incumbent_names = tuple(name for name, oid in expected_incumbent.relation_oids if oid is not None)
        if incumbent_names:
            await _lock_family(session, expected_incumbent.schema_name, incumbent_names, "ACCESS EXCLUSIVE")
        await verify_reference_family_stage_ownership(session, ownership)
        await _verify_incumbent(session, expected_incumbent)


async def _rotate_family_relations(
    session: Any,
    spec: ReferenceFamilySpec,
    ownership: ReferenceFamilyStageOwnership,
    expected_incumbent: ReferenceFamilyIncumbent,
) -> str | None:
    incumbent_oids_by_name = dict(expected_incumbent.relation_oids)
    predecessor_schema = None
    if any(oid is not None for oid in incumbent_oids_by_name.values()):
        predecessor_schema = reference_family_predecessor_schema(ownership.dataset_id)
        # Never infer ownership from a UUID-shaped name or replace content
        # already present there: CREATE is the collision/CAS fence.
        await session.execute(text(f"CREATE SCHEMA {_quoted(predecessor_schema)}"))
    for table_name in spec.table_names:
        incumbent_oid = incumbent_oids_by_name[table_name]
        if incumbent_oid is not None:
            await session.execute(
                text(
                    f"ALTER TABLE {_quoted(expected_incumbent.schema_name)}.{_quoted(table_name)} "
                    f"SET SCHEMA {_quoted(predecessor_schema)}"
                )
            )
        await session.execute(
            text(
                f"ALTER TABLE {_quoted(ownership.schema_name)}.{_quoted(table_name)} "
                f"SET SCHEMA {_quoted(expected_incumbent.schema_name)}"
            )
        )
    return predecessor_schema


async def _drop_empty_stage_schema(session: Any, ownership: ReferenceFamilyStageOwnership) -> None:
    remaining_relations = int(
        await session.scalar(
            text("SELECT count(*) FROM pg_catalog.pg_class WHERE relnamespace=:schema_oid"),
            {"schema_oid": ownership.schema_oid},
        )
        or 0
    )
    if remaining_relations:
        raise ReferenceFamilyArchiveError("reference family stage schema is not empty after activation")
    await session.execute(text(f"DROP SCHEMA {_quoted(ownership.schema_name)}"))


async def _activation_receipt(
    session: Any,
    spec: ReferenceFamilySpec,
    ownership: ReferenceFamilyStageOwnership,
    expected_incumbent: ReferenceFamilyIncumbent,
    manifest: ReferenceFamilyManifest,
    tables: tuple[ReferenceTableReceipt, ...],
    predecessor_schema_name: str | None,
) -> ReferenceFamilyActivationReceipt:
    live_pairs = await _incumbent_pairs(session, spec, expected_incumbent.schema_name)
    if any(type(oid) is not int or oid <= 0 for _, oid in live_pairs):
        raise ReferenceFamilyArchiveError("reference family activated relation is unavailable")
    if tuple(sorted(live_pairs)) != ownership.relation_oids:
        raise ReferenceFamilyArchiveError("reference family activated relation OID differs")
    local_manifest = await _family_manifest(
        session,
        spec=spec,
        schema_name=expected_incumbent.schema_name,
        source_metadata=manifest.source_metadata,
    )
    if local_manifest.as_dict() != manifest.as_dict():
        raise ReferenceFamilyArchiveError("reference family activated receipt differs")
    return ReferenceFamilyActivationReceipt(
        spec.importer_id,
        manifest.source_metadata_sha256,
        tuple((name, int(oid)) for name, oid in live_pairs),
        expected_incumbent.relation_oids,
        predecessor_schema_name,
        tables,
    )


async def activate_reference_family_stage(
    session: Any,
    *,
    ownership: ReferenceFamilyStageOwnership,
    manifest: Mapping[str, Any] | ReferenceFamilyManifest,
    expected_incumbent: ReferenceFamilyIncumbent,
    authority: str,
) -> ReferenceFamilyActivationReceipt:
    """Manually rotate one complete family inside the caller-owned transaction."""

    _require_transaction(session)
    if authority != "manual":
        raise ReferenceFamilyArchiveError("reference family automatic activation is unsupported")
    if not isinstance(ownership, ReferenceFamilyStageOwnership) or not isinstance(
        expected_incumbent,
        ReferenceFamilyIncumbent,
    ):
        raise ReferenceFamilyArchiveError("reference family activation ownership is invalid")
    validated_manifest = validate_reference_family_manifest(manifest)
    if (
        validated_manifest.importer_id != ownership.importer_id
        or expected_incumbent.importer_id != ownership.importer_id
    ):
        raise ReferenceFamilyArchiveError("reference family activation scope differs")
    spec = reference_family_spec(ownership.importer_id)
    await _lock_and_verify_activation(session, spec, ownership, expected_incumbent)
    tables = await _validate_stage_manifest(
        session,
        ownership=ownership,
        manifest=validated_manifest,
    )
    predecessor_schema_name = await _rotate_family_relations(
        session,
        spec,
        ownership,
        expected_incumbent,
    )
    await _drop_empty_stage_schema(session, ownership)
    return await _activation_receipt(
        session,
        spec,
        ownership,
        expected_incumbent,
        validated_manifest,
        tables,
        predecessor_schema_name,
    )


__all__ = [
    "CONTRACT",
    "ReferenceFamilyActivationReceipt",
    "ReferenceFamilyArchiveError",
    "ReferenceFamilyIncumbent",
    "ReferenceFamilyManifest",
    "ReferenceFamilySourceCapture",
    "ReferenceFamilyStageCapture",
    "ReferenceFamilyStageOwnership",
    "ReferenceFamilySpec",
    "ReferenceTableReceipt",
    "activate_reference_family_stage",
    "capture_reference_family_incumbent",
    "capture_reference_family_source",
    "capture_reference_family_stage_ownership",
    "cleanup_reference_family_stage",
    "export_reference_family_archive",
    "precreate_reference_family_restore",
    "reference_family_spec",
    "reference_family_predecessor_schema",
    "reference_family_stage_schema",
    "validate_reference_family_manifest",
    "validate_reference_family_stage",
    "verify_reference_family_stage_ownership",
]
