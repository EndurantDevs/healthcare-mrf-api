# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Source-scoped native MS-DRG archive and conditional local publication."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime
from uuid import UUID

from sqlalchemy import text

from db.models import CodeCatalog, CodeRelationship, CodeSynonym
from process.entity_address_snapshot_receipt import _projected_row_identity
from process.ms_drg_result_generation import (
    _SCOPES,
    _quoted,
    _table_shape,
    capture_result,
    read_current_generation,
)
from process.ms_drg_result_generation import (
    CONTRACT as GENERATION_CONTRACT,
)
from process.ms_drg_result_generation import (
    TABLE as GENERATION_TABLE,
)

CONTRACT = "ms-drg-scoped.postgres.v1"
MODELS = (CodeCatalog, CodeSynonym, CodeRelationship)
TABLES = tuple(model.__tablename__ for model in MODELS)
PREDECESSORS = tuple(name + "_predecessor" for name in TABLES)
SHA256 = re.compile(r"[0-9a-f]{64}\Z")
MAX_GENERATION = (1 << 63) - 1


class MsDrgArchiveError(RuntimeError):
    """The owned result, stage, or conditional destination changed."""


def require(condition, message):
    """Reject an archive invariant with the archive's public error type."""
    if not condition:
        raise MsDrgArchiveError(message)


def stage_schema(dataset_id: UUID) -> str:
    """Derive the owned stage namespace from its dataset identity."""
    require(isinstance(dataset_id, UUID), "MS-DRG stage identity is invalid")
    return "ms_drg_archive_" + dataset_id.hex


def source_manifest(generation: dict) -> dict:
    """Project a published generation into the portable source manifest."""
    receipt = generation["receipt"]
    return {
        "contract": CONTRACT,
        "origin_lineage_id": str(generation["origin_lineage_id"]),
        "origin_generation": generation["origin_generation"],
        "published_at": generation["published_at"].isoformat(),
        "include_relationships": generation["include_relationships"],
        "content_sha256": receipt["content_sha256"],
        "tables": [
            {key: table[key] for key in ("table", "sources", "row_count", "row_sha256", "schema_sha256")}
            for table in receipt["tables"]
        ],
    }


def validate_manifest(manifest: object) -> dict:
    """Return only a canonical, complete MS-DRG source manifest."""
    require(
        isinstance(manifest, dict)
        and set(manifest)
        == {
            "contract",
            "origin_lineage_id",
            "origin_generation",
            "published_at",
            "include_relationships",
            "content_sha256",
            "tables",
        }
        and manifest["contract"] == CONTRACT,
        "MS-DRG manifest is invalid",
    )
    try:
        lineage = UUID(manifest["origin_lineage_id"])
        published = datetime.fromisoformat(manifest["published_at"])
        tables = manifest["tables"]
        require(
            type(manifest["origin_generation"]) is int
            and 0 < manifest["origin_generation"] <= MAX_GENERATION
            and published.tzinfo is not None
            and type(manifest["include_relationships"]) is bool
            and isinstance(tables, list)
            and len(tables) == len(TABLES)
            and all(isinstance(manifest[key], str) and SHA256.fullmatch(manifest[key]) for key in ("content_sha256",)),
            "MS-DRG manifest is invalid",
        )
        for table, (name, owned_sources) in zip(tables, _SCOPES, strict=True):
            require(
                isinstance(table, dict)
                and set(table) == {"table", "sources", "row_count", "row_sha256", "schema_sha256"}
                and (table["table"], table["sources"]) == (name, list(owned_sources))
                and type(table["row_count"]) is int
                and table["row_count"] >= 0
                and all(
                    isinstance(table[key], str) and SHA256.fullmatch(table[key])
                    for key in ("row_sha256", "schema_sha256")
                ),
                "MS-DRG manifest table is invalid",
            )
        require(manifest["content_sha256"] == _digest(tables), "MS-DRG manifest digest differs")
        require(
            str(lineage) == manifest["origin_lineage_id"] and published.isoformat() == manifest["published_at"],
            "MS-DRG manifest identity is invalid",
        )
    except (KeyError, TypeError, ValueError) as error:
        raise MsDrgArchiveError("MS-DRG manifest is invalid") from error
    return manifest


def _content(receipt: dict) -> list[dict]:
    return [
        {key: table[key] for key in ("table", "sources", "row_count", "row_sha256", "schema_sha256")}
        for table in receipt["tables"]
    ]


def _digest(content: list[dict]) -> str:
    return hashlib.sha256(json.dumps(content, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


async def _oids(session, schema: str, names: tuple[str, ...]) -> tuple[int, ...]:
    oids = tuple(
        [
            await session.scalar(text("SELECT to_regclass(:relation)::oid::bigint"), {"relation": f"{schema}.{name}"})
            for name in names
        ]
    )
    require(all(type(oid) is int and 0 < oid < 2**32 for oid in oids), "MS-DRG stage relation is missing")
    return oids


async def _verify_namespace(session, schema: str, schema_oid: int, relation_oids: tuple[int, ...]) -> None:
    require(
        await session.scalar(text("SELECT to_regnamespace(:name)::oid::bigint"), {"name": schema}) == schema_oid,
        "MS-DRG stage schema changed",
    )
    rows = (
        await session.execute(
            text(
                "SELECT c.oid::bigint,c.relkind::text,i.indrelid::bigint FROM pg_class c "
                "LEFT JOIN pg_index i ON i.indexrelid=c.oid WHERE c.relnamespace=:oid"
            ),
            {"oid": schema_oid},
        )
    ).all()
    require(
        all(
            (kind == "r" and oid in relation_oids) or (kind == "i" and owner in relation_oids)
            for oid, kind, owner in rows
        ),
        "MS-DRG stage contains an unowned relation",
    )


async def _strict_stage(session, stage: dict, manifest: dict, *, receiving: bool) -> None:
    validate_manifest(manifest)
    schema = stage_schema(UUID(stage["dataset_id"]))
    expected_names = TABLES + PREDECESSORS if receiving else TABLES
    require(
        stage["schema_name"] == schema
        and tuple(stage["relation_oids"]) == await _oids(session, schema, expected_names),
        "MS-DRG stage identity changed",
    )
    await _verify_namespace(session, schema, stage["schema_oid"], tuple(stage["relation_oids"]))
    for name, sources in _SCOPES:
        invalid = await session.scalar(
            text(
                f"SELECT EXISTS(SELECT 1 FROM {_quoted(schema)}.{_quoted(name)} "
                "WHERE (source=ANY(CAST(:sources AS text[]))) IS NOT TRUE)"
            ),
            {"sources": list(sources)},
        )
        require(not invalid, "MS-DRG stage contains a foreign source")
    receipt = await capture_result(session, schema)
    require(
        _content(receipt) == manifest["tables"] and receipt["content_sha256"] == manifest["content_sha256"],
        "MS-DRG stage content changed",
    )


async def _copy_slice(session, source: str, target: str, source_name: str, target_name: str, model, sources) -> None:
    columns = ",".join(_quoted(column.name) for column in model.__table__.columns)
    await session.execute(
        text(
            f"INSERT INTO {_quoted(target)}.{_quoted(target_name)} ({columns}) "
            f"SELECT {columns} FROM {_quoted(source)}.{_quoted(source_name)} "
            "WHERE source=ANY(CAST(:sources AS text[]))"
        ),
        {"sources": list(sources)},
    )


async def _create_stage(session, source: str, dataset_id: UUID, *, predecessors: bool) -> dict:
    schema = stage_schema(dataset_id)
    await session.execute(text(f"CREATE SCHEMA {_quoted(schema)}"))
    for model in MODELS:
        name = model.__tablename__
        await session.execute(
            text(
                f"CREATE TABLE {_quoted(schema)}.{_quoted(name)} (LIKE {_quoted(source)}.{_quoted(name)} INCLUDING ALL)"
            )
        )
        if predecessors:
            await session.execute(
                text(
                    f"CREATE TABLE {_quoted(schema)}.{_quoted(name + '_predecessor')} "
                    f"(LIKE {_quoted(source)}.{_quoted(name)} INCLUDING ALL)"
                )
            )
    names = TABLES + PREDECESSORS if predecessors else TABLES
    return {
        "dataset_id": str(dataset_id),
        "schema_name": schema,
        "schema_oid": await session.scalar(text("SELECT to_regnamespace(:name)::oid::bigint"), {"name": schema}),
        "relation_oids": await _oids(session, schema, names),
    }


async def prepare_source(session, schema: str, dataset_id: UUID) -> tuple[dict, dict]:
    """Freeze the exact published source slices under the ordinary writer's table locks."""
    require(session.in_transaction(), "MS-DRG source transaction is required")
    await session.execute(
        text("LOCK TABLE " + ",".join(f"{_quoted(schema)}.{_quoted(name)}" for name in TABLES) + " IN SHARE MODE")
    )
    generation = await read_current_generation(session, schema)
    manifest = validate_manifest(source_manifest(generation))
    stage = await _create_stage(session, schema, dataset_id, predecessors=False)
    for model, (_, sources) in zip(MODELS, _SCOPES, strict=True):
        await _copy_slice(
            session, schema, stage["schema_name"], model.__tablename__, model.__tablename__, model, sources
        )
    await _strict_stage(session, stage, manifest, receiving=False)
    return stage, manifest


async def verify_source_generation(session, schema: str, manifest: dict) -> None:
    """Pin current ordinary publication before exporting the frozen source stage."""
    require(session.in_transaction(), "MS-DRG export transaction is required")
    current = await read_current_generation(session, schema)
    require(source_manifest(current) == validate_manifest(manifest), "MS-DRG source generation changed")


async def precreate_restore(session, destination: str, dataset_id: UUID, manifest: dict) -> dict:
    """Register all six OIDs before data-only restore and protected freeze."""
    require(session.in_transaction(), "MS-DRG restore transaction is required")
    validate_manifest(manifest)
    stage = await _create_stage(session, destination, dataset_id, predecessors=True)
    # Empty candidates cannot validate row hashes yet, but must match source shape.
    actual = await capture_result(session, stage["schema_name"])
    require(
        all(
            row["schema_sha256"] == expected["schema_sha256"]
            for row, expected in zip(actual["tables"], manifest["tables"], strict=True)
        ),
        "MS-DRG restore shape differs",
    )
    return stage


async def verify_stage(session, stage: dict, manifest: dict, *, receiving: bool) -> None:
    """Check stage ownership and content while holding its table locks."""
    require(session.in_transaction(), "MS-DRG stage transaction is required")
    await session.execute(
        text(
            "LOCK TABLE "
            + ",".join(f"{_quoted(stage['schema_name'])}.{_quoted(name)}" for name in TABLES)
            + " IN SHARE MODE"
        )
    )
    await _strict_stage(session, stage, manifest, receiving=receiving)


async def cleanup_stage(session, stage: dict, *, receiving: bool) -> None:
    """Drop only the still-owned relations and namespace in this transaction."""
    require(session.in_transaction(), "MS-DRG cleanup transaction is required")
    schema = stage_schema(UUID(stage["dataset_id"]))
    names = TABLES + PREDECESSORS if receiving else TABLES
    require(
        stage["schema_name"] == schema and tuple(stage["relation_oids"]) == await _oids(session, schema, names),
        "MS-DRG cleanup ownership changed",
    )
    await _verify_namespace(session, schema, stage["schema_oid"], tuple(stage["relation_oids"]))
    for name in reversed(names):
        await session.execute(text(f"DROP TABLE {_quoted(schema)}.{_quoted(name)}"))
    await session.execute(text(f"DROP SCHEMA {_quoted(schema)}"))


async def _lock_live(session, destination: str) -> None:
    await session.execute(
        text(
            "LOCK TABLE "
            + ",".join(f"{_quoted(destination)}.{_quoted(name)}" for name in TABLES)
            + " IN SHARE ROW EXCLUSIVE MODE"
        )
    )


async def _current(session, destination: str) -> tuple[dict, dict]:
    row = (
        (
            await session.execute(
                text(
                    f"SELECT local_lineage_id,local_generation,origin_lineage_id,origin_generation,"
                    f"published_at,include_relationships,receipt FROM {_quoted(destination)}.{GENERATION_TABLE} "
                    "WHERE id=1 FOR UPDATE"
                )
            )
        )
        .mappings()
        .one_or_none()
    )
    require(
        row is not None and type(row["local_generation"]) is int and 0 <= row["local_generation"] < MAX_GENERATION,
        "MS-DRG destination generation is unavailable",
    )
    actual = await capture_result(session, destination)
    if row["origin_generation"] is None:
        require(
            row["receipt"] is None and all(table["row_count"] == 0 for table in actual["tables"]),
            "untracked MS-DRG rows prevent activation",
        )
    else:
        require(row["receipt"] == actual, "MS-DRG destination rows changed")
    return dict(row), actual


async def _predecessor_content(session, stage: dict) -> list[dict]:
    schema = stage["schema_name"]
    table_receipts = []
    for model, (name, owned_sources), predecessor, oid in zip(
        MODELS, _SCOPES, PREDECESSORS, stage["relation_oids"][len(TABLES) :], strict=True
    ):
        invalid = await session.scalar(
            text(
                f"SELECT EXISTS(SELECT 1 FROM {_quoted(schema)}.{_quoted(predecessor)} "
                "WHERE (source=ANY(CAST(:sources AS text[]))) IS NOT TRUE)"
            ),
            {"sources": list(owned_sources)},
        )
        require(not invalid, "MS-DRG predecessor contains a foreign source")
        count, digest = await _projected_row_identity(
            session,
            schema,
            predecessor,
            row_json_sql="pg_catalog.to_jsonb(row_value)",
            where_sql="WHERE row_value.source=ANY(CAST(:sources AS text[]))",
            parameters={"sources": list(owned_sources)},
        )
        table_receipts.append(
            {
                "table": name,
                "sources": list(owned_sources),
                "row_count": count,
                "row_sha256": digest,
                "schema_sha256": await _table_shape(session, oid, model),
            }
        )
    return table_receipts


async def prepare_predecessor(session, destination: str, stage: dict, manifest: dict) -> dict:
    """Copy the before-image inside the registered six-table stage before freeze."""
    require(session.in_transaction(), "MS-DRG predecessor transaction is required")
    await _lock_live(session, destination)
    previous, before = await _current(session, destination)
    await verify_stage(session, stage, manifest, receiving=True)
    schema = stage["schema_name"]
    for model, (name, sources), predecessor in zip(MODELS, _SCOPES, PREDECESSORS, strict=True):
        require(
            not await session.scalar(text(f"SELECT EXISTS(SELECT 1 FROM {_quoted(schema)}.{_quoted(predecessor)})")),
            "MS-DRG predecessor is not empty",
        )
        await _copy_slice(session, destination, schema, name, predecessor, model, sources)
    require(await _predecessor_content(session, stage) == _content(before), "MS-DRG predecessor differs")
    return {"previous": previous, "before": _content(before)}


async def validate_prepared_stage(session, destination: str, stage: dict, manifest: dict) -> dict:
    """Recheck the frozen before-image and destination CAS value."""
    require(session.in_transaction(), "MS-DRG validation transaction is required")
    await session.execute(
        text("LOCK TABLE " + ",".join(f"{_quoted(destination)}.{_quoted(name)}" for name in TABLES) + " IN SHARE MODE")
    )
    previous, before = await _current(session, destination)
    await verify_stage(session, stage, manifest, receiving=True)
    require(await _predecessor_content(session, stage) == _content(before), "MS-DRG predecessor differs")
    return {"previous": previous, "before": _content(before)}


async def _has_foreign_key_collision(session, destination: str, stage: dict, names: tuple[str, ...]) -> bool:
    schema = stage["schema_name"]
    for model, (name, sources), candidate in zip(MODELS, _SCOPES, names, strict=True):
        keys = tuple(column.name for column in model.__table__.primary_key.columns)
        join = " AND ".join(f"live.{_quoted(key)}=incoming.{_quoted(key)}" for key in keys)
        if await session.scalar(
            text(
                f"SELECT EXISTS(SELECT 1 FROM {_quoted(schema)}.{_quoted(candidate)} AS incoming "
                f"JOIN {_quoted(destination)}.{_quoted(name)} AS live ON {join} "
                "WHERE (live.source=ANY(CAST(:sources AS text[]))) IS NOT TRUE)"
            ),
            {"sources": list(sources)},
        ):
            return True
    return False


async def _replace(session, destination: str, stage: dict, names: tuple[str, ...]) -> None:
    schema = stage["schema_name"]
    for model, (name, sources), candidate in zip(MODELS, _SCOPES, names, strict=True):
        await session.execute(
            text(f"DELETE FROM {_quoted(destination)}.{_quoted(name)} WHERE source=ANY(CAST(:sources AS text[]))"),
            {"sources": list(sources)},
        )
        columns = ",".join(_quoted(column.name) for column in model.__table__.columns)
        await session.execute(
            text(
                f"INSERT INTO {_quoted(destination)}.{_quoted(name)} ({columns}) "
                f"SELECT {columns} FROM {_quoted(schema)}.{_quoted(candidate)}"
            )
        )


async def _write_generation(
    session, destination: str, previous: dict, *, manifest: dict | None, receipt: dict | None
) -> dict:
    origin = None if manifest is None else UUID(manifest["origin_lineage_id"])
    row = (
        (
            await session.execute(
                text(
                    f"UPDATE {_quoted(destination)}.{GENERATION_TABLE} SET local_generation=:local_generation,"
                    "origin_lineage_id=:origin_lineage_id,origin_generation=:origin_generation,"
                    "published_at=:published_at,include_relationships=:include_relationships,"
                    "receipt=CAST(:receipt AS jsonb) WHERE id=1 "
                    "RETURNING local_lineage_id,local_generation,origin_lineage_id,origin_generation,"
                    "published_at,include_relationships,receipt"
                ),
                {
                    "local_generation": previous["local_generation"] + 1,
                    "origin_lineage_id": origin,
                    "origin_generation": None if manifest is None else manifest["origin_generation"],
                    "published_at": None if manifest is None else datetime.fromisoformat(manifest["published_at"]),
                    "include_relationships": None if manifest is None else manifest["include_relationships"],
                    "receipt": None if receipt is None else json.dumps(receipt, sort_keys=True, separators=(",", ":")),
                },
            )
        )
        .mappings()
        .one()
    )
    return dict(row)


async def activate_stage(session, destination: str, stage: dict, manifest: dict, prepared: dict) -> dict:
    """CAS all owned slices and source origin in one destination transaction."""
    require(session.in_transaction(), "MS-DRG activation transaction is required")
    await _lock_live(session, destination)
    previous, before = await _current(session, destination)
    require(
        previous == prepared["previous"] and _content(before) == prepared["before"],
        "MS-DRG destination generation changed",
    )
    await verify_stage(session, stage, manifest, receiving=True)
    require(await _predecessor_content(session, stage) == prepared["before"], "MS-DRG predecessor changed")
    require(
        not await _has_foreign_key_collision(session, destination, stage, TABLES),
        "MS-DRG candidate key belongs to another source",
    )
    await _replace(session, destination, stage, TABLES)
    current_receipt = await capture_result(session, destination)
    require(
        _content(current_receipt) == manifest["tables"]
        and current_receipt["content_sha256"] == manifest["content_sha256"],
        "MS-DRG activation result differs",
    )
    current = await _write_generation(session, destination, previous, manifest=manifest, receipt=current_receipt)
    return {
        "previous": previous,
        "current": current,
        "before": prepared["before"],
        "stage": stage,
        "manifest": manifest,
    }


async def rollback_activation(session, destination: str, activation: dict) -> dict:
    """Restore only this installation's frozen predecessor under an exact CAS."""
    require(session.in_transaction(), "MS-DRG rollback transaction is required")
    stage = activation["stage"]
    await _lock_live(session, destination)
    current, _receipt = await _current(session, destination)
    require(current == activation["current"], "MS-DRG rollback generation changed")
    await _strict_stage(session, stage, activation["manifest"], receiving=True)
    require(await _predecessor_content(session, stage) == activation["before"], "MS-DRG rollback predecessor changed")
    require(
        not await _has_foreign_key_collision(session, destination, stage, PREDECESSORS),
        "MS-DRG rollback key belongs to another source",
    )
    await _replace(session, destination, stage, PREDECESSORS)
    restored_receipt = await capture_result(session, destination)
    require(_content(restored_receipt) == activation["before"], "MS-DRG rollback result differs")
    previous = activation["previous"]
    prior_manifest = (
        None
        if previous["origin_generation"] is None
        else {
            "origin_lineage_id": str(previous["origin_lineage_id"]),
            "origin_generation": previous["origin_generation"],
            "published_at": previous["published_at"].isoformat(),
            "include_relationships": previous["include_relationships"],
        }
    )
    return await _write_generation(
        session,
        destination,
        current,
        manifest=prior_manifest,
        receipt=None if prior_manifest is None else restored_receipt,
    )
