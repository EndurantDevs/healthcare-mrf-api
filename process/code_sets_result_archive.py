# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact source slice and generation authority for code-set results."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from sqlalchemy import text

from db.models import CodeCatalog
from process.entity_address_snapshot_receipt import _projected_row_identity

TABLE = "code_sets_result_generation"
CONTRACT = "code-sets-scoped.postgres.v1"
SOURCES = (
    ("cms_place_of_service_code_set", "POS"),
    ("cms_bluebutton_revenue_center_code", "RC"),
    ("cms_hcpcs_modifier_reference", "MODIFIER"),
)
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_MAX_GENERATION = (1 << 63) - 1


class CodeSetsArchiveError(RuntimeError):
    """The fixed code-set source slice or serving authority differs."""


@dataclass(frozen=True)
class CodeSetsGeneration:
    local_lineage_id: UUID
    local_generation: int
    origin_lineage_id: UUID | None
    origin_generation: int | None
    published_at: datetime | None
    code_catalog_oid: int | None
    row_count: int | None
    row_sha256: str | None


@dataclass(frozen=True)
class CodeSetsStage:
    dataset_id: UUID
    schema_name: str
    schema_oid: int
    catalog_oid: int
    source_generation: CodeSetsSourceGeneration
    row_count: int
    row_sha256: str


@dataclass(frozen=True)
class CodeSetsActivation:
    dataset_id: UUID
    previous: CodeSetsGeneration
    current: CodeSetsGeneration
    predecessor_schema: str
    predecessor_schema_oid: int
    candidate_catalog_oid: int
    predecessor_catalog_oid: int
    predecessor_row_count: int
    predecessor_row_sha256: str


@dataclass(frozen=True)
class CodeSetsPreparedStage:
    stage: CodeSetsStage
    expected: CodeSetsGeneration
    predecessor_catalog_oid: int
    predecessor_row_count: int
    predecessor_row_sha256: str


PREDECESSOR_TABLE = "code_catalog_predecessor"


@dataclass(frozen=True)
class CodeSetsSourceGeneration:
    origin_lineage_id: UUID
    origin_generation: int
    published_at: datetime
    row_count: int
    row_sha256: str
    schema_sha256: str

    def as_dict(self) -> dict:
        """Serialize the complete portable source receipt."""
        return {
            "contract": CONTRACT,
            "origin_lineage_id": str(self.origin_lineage_id),
            "origin_generation": self.origin_generation,
            "published_at": self.published_at.isoformat(),
            "row_count": self.row_count,
            "row_sha256": self.row_sha256,
            "schema_sha256": self.schema_sha256,
        }


def validate_manifest(manifest_value: object) -> CodeSetsSourceGeneration:
    """Accept only the exact supported source-generation contract."""
    if (
        not isinstance(manifest_value, dict)
        or set(manifest_value)
        != {
            "contract",
            "origin_lineage_id",
            "origin_generation",
            "published_at",
            "row_count",
            "row_sha256",
            "schema_sha256",
        }
        or manifest_value["contract"] != CONTRACT
    ):
        raise CodeSetsArchiveError("code-set archive manifest is invalid")
    if not isinstance(manifest_value["origin_lineage_id"], str) or not isinstance(manifest_value["published_at"], str):
        raise CodeSetsArchiveError("code-set archive manifest is invalid")
    try:
        source_generation = CodeSetsSourceGeneration(
            UUID(manifest_value["origin_lineage_id"]),
            manifest_value["origin_generation"],
            datetime.fromisoformat(manifest_value["published_at"]),
            manifest_value["row_count"],
            manifest_value["row_sha256"],
            manifest_value["schema_sha256"],
        )
    except (TypeError, ValueError) as error:
        raise CodeSetsArchiveError("code-set archive manifest is invalid") from error
    if (
        type(source_generation.origin_generation) is not int
        or not 0 < source_generation.origin_generation <= _MAX_GENERATION
        or source_generation.published_at.tzinfo is None
        or type(source_generation.row_count) is not int
        or source_generation.row_count < len(SOURCES)
        or any(
            not isinstance(digest, str) or _SHA256.fullmatch(digest) is None
            for digest in (source_generation.row_sha256, source_generation.schema_sha256)
        )
        or source_generation.as_dict() != manifest_value
    ):
        raise CodeSetsArchiveError("code-set archive manifest is invalid")
    return source_generation


def _schema_digest(signature: tuple) -> str:
    return hashlib.sha256(json.dumps(signature, separators=(",", ":"), default=str).encode()).hexdigest()


def stage_schema(dataset_id: UUID) -> str:
    """Derive the UUID-owned schema for one staged result."""
    if not isinstance(dataset_id, UUID):
        raise CodeSetsArchiveError("code-set stage identity is invalid")
    return "code_sets_archive_" + dataset_id.hex


def predecessor_schema(dataset_id: UUID) -> str:
    """Keep the predecessor beside its staged candidate."""
    return stage_schema(dataset_id)


def _schema(value: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value) or len(value.encode()) > 63:
        raise CodeSetsArchiveError("code-set schema is invalid")
    return f'"{value}"'


def _transaction(session) -> None:
    if not callable(getattr(session, "in_transaction", None)) or not session.in_transaction():
        raise CodeSetsArchiveError("code-set result requires a caller transaction")


def _generation(generation_row) -> CodeSetsGeneration:
    if generation_row is None:
        raise CodeSetsArchiveError("code-set generation authority is unavailable")
    try:
        authority = CodeSetsGeneration(
            UUID(str(generation_row["local_lineage_id"])),
            generation_row["local_generation"],
            UUID(str(generation_row["origin_lineage_id"])) if generation_row["origin_lineage_id"] is not None else None,
            generation_row["origin_generation"],
            generation_row["published_at"],
            generation_row["code_catalog_oid"],
            generation_row["row_count"],
            generation_row["row_sha256"],
        )
    except (KeyError, TypeError, ValueError) as error:
        raise CodeSetsArchiveError("code-set generation authority is invalid") from error
    serving = (
        authority.origin_lineage_id,
        authority.origin_generation,
        authority.published_at,
        authority.code_catalog_oid,
        authority.row_count,
        authority.row_sha256,
    )
    if (
        type(authority.local_generation) is not int
        or not 0 <= authority.local_generation <= _MAX_GENERATION
        or (
            any(field_value is None for field_value in serving)
            and not all(field_value is None for field_value in serving)
        )
    ):
        raise CodeSetsArchiveError("code-set generation authority is invalid")
    if authority.origin_lineage_id is not None and (
        type(authority.origin_generation) is not int
        or not 0 < authority.origin_generation <= _MAX_GENERATION
        or not isinstance(authority.published_at, datetime)
        or authority.published_at.tzinfo is None
        or type(authority.code_catalog_oid) is not int
        or authority.code_catalog_oid <= 0
        or type(authority.row_count) is not int
        or authority.row_count < 0
        or not isinstance(authority.row_sha256, str)
        or _SHA256.fullmatch(authority.row_sha256) is None
    ):
        raise CodeSetsArchiveError("code-set generation authority is invalid")
    return authority


async def read_generation(session, schema: str, *, lock: bool = False) -> CodeSetsGeneration:
    """Read the singleton serving authority, optionally locking its row."""
    _transaction(session)
    qualified = _schema(schema)
    row = (
        (
            await session.execute(
                text(
                    f"SELECT local_lineage_id,local_generation,origin_lineage_id,origin_generation,"
                    f"published_at,code_catalog_oid,row_count,row_sha256 FROM {qualified}.{TABLE} "
                    f"WHERE id=1{' FOR UPDATE' if lock else ''}"
                )
            )
        )
        .mappings()
        .one_or_none()
    )
    return _generation(row)


async def scope_receipt(session, schema: str, *, stage: bool = False) -> tuple[int, str, int]:
    """Hash all fixed-source rows; a stage may contain no other rows."""
    _transaction(session)
    qualified = _schema(schema)
    catalog = CodeCatalog.__tablename__
    oid = await session.scalar(text("SELECT to_regclass(:relation)::oid::bigint"), {"relation": f"{schema}.{catalog}"})
    if type(oid) is not int or oid <= 0:
        raise CodeSetsArchiveError("code-set catalog relation is unavailable")
    source_names = [source_name for source_name, _ in SOURCES]
    allowed = " OR ".join(
        f"(row_value.source='{source_name}' AND row_value.code_system='{system}')" for source_name, system in SOURCES
    )
    invalid = await session.scalar(
        text(
            f"SELECT EXISTS(SELECT 1 FROM {qualified}.{catalog} AS row_value "
            f"WHERE {'TRUE' if stage else 'row_value.source=ANY(CAST(:sources AS text[]))'} "
            f"AND ({allowed}) IS NOT TRUE)"
        ),
        {"sources": source_names},
    )
    if invalid:
        raise CodeSetsArchiveError("code-set source scope contains foreign rows")
    source_counts = (
        await session.execute(
            text(
                f"SELECT source,count(*)::bigint FROM {qualified}.{catalog} "
                "WHERE source=ANY(CAST(:sources AS text[])) GROUP BY source"
            ),
            {"sources": source_names},
        )
    ).all()
    if {source_name for source_name, count in source_counts if count > 0} != set(source_names):
        raise CodeSetsArchiveError("code-set result is incomplete")
    count, digest = await _projected_row_identity(
        session,
        schema,
        catalog,
        row_json_sql="pg_catalog.to_jsonb(row_value)",
        where_sql="" if stage else "WHERE row_value.source=ANY(CAST(:sources AS text[]))",
        parameters=None if stage else {"sources": source_names},
    )
    return count, digest, oid


async def publish_local_generation(session, schema: str) -> CodeSetsGeneration:
    """Advance only after all three sources were written in the same transaction."""
    prior = await read_generation(session, schema, lock=True)
    if prior.local_generation >= _MAX_GENERATION:
        raise CodeSetsArchiveError("code-set generation is exhausted")
    count, digest, oid = await scope_receipt(session, schema)
    next_generation = prior.local_generation + 1
    qualified = _schema(schema)
    row = (
        (
            await session.execute(
                text(
                    f"UPDATE {qualified}.{TABLE} SET local_generation=:generation,"
                    "origin_lineage_id=local_lineage_id,origin_generation=:generation,"
                    "published_at=clock_timestamp(),code_catalog_oid=:oid,row_count=:count,row_sha256=:digest "
                    "WHERE id=1 RETURNING local_lineage_id,local_generation,origin_lineage_id,"
                    "origin_generation,published_at,code_catalog_oid,row_count,row_sha256"
                ),
                {"generation": next_generation, "oid": oid, "count": count, "digest": digest},
            )
        )
        .mappings()
        .one_or_none()
    )
    return _generation(row)


async def _slice_identity(
    session, schema: str, *, complete: bool, table_name: str = CodeCatalog.__tablename__, strict: bool = False
) -> tuple[int, str, int]:
    """Read exact owned rows, allowing an empty generationless predecessor."""
    if complete:
        return await scope_receipt(session, schema)
    qualified = _schema(schema)
    catalog = table_name
    oid = await session.scalar(text("SELECT to_regclass(:relation)::oid::bigint"), {"relation": f"{schema}.{catalog}"})
    if type(oid) is not int or oid <= 0:
        raise CodeSetsArchiveError("code-set catalog relation is unavailable")
    if strict:
        allowed = " OR ".join(
            f"(row_value.source='{source}' AND row_value.code_system='{system}')" for source, system in SOURCES
        )
        if await session.scalar(
            text(f"SELECT EXISTS(SELECT 1 FROM {qualified}.{catalog} AS row_value WHERE ({allowed}) IS NOT TRUE)")
        ):
            raise CodeSetsArchiveError("code-set predecessor contains foreign rows")
    count, digest = await _projected_row_identity(
        session,
        schema,
        catalog,
        row_json_sql="pg_catalog.to_jsonb(row_value)",
        where_sql="WHERE row_value.source=ANY(CAST(:sources AS text[]))",
        parameters={"sources": [source for source, _ in SOURCES]},
    )
    return count, digest, oid


async def _clone_slice(
    session,
    source_schema: str,
    target_schema: str,
    *,
    target_table: str = CodeCatalog.__tablename__,
    create_schema: bool = True,
) -> tuple[int, int]:
    """Clone physical column/index shape but copy only the fixed source slice."""
    quoted_source = _schema(source_schema)
    quoted_target = _schema(target_schema)
    name = CodeCatalog.__tablename__
    if create_schema:
        await session.execute(text(f"CREATE SCHEMA {quoted_target}"))
    await session.execute(
        text(f"CREATE TABLE {quoted_target}.{target_table} (LIKE {quoted_source}.{name} INCLUDING ALL)")
    )
    columns = ",".join(f'"{column.name}"' for column in CodeCatalog.__table__.columns)
    await session.execute(
        text(
            f"INSERT INTO {quoted_target}.{target_table} ({columns}) SELECT {columns} FROM {quoted_source}.{name} "
            "WHERE source=ANY(CAST(:sources AS text[]))"
        ),
        {"sources": [source_name for source_name, _ in SOURCES]},
    )
    schema_oid = await session.scalar(text("SELECT to_regnamespace(:schema)::oid::bigint"), {"schema": target_schema})
    catalog_oid = await session.scalar(
        text("SELECT to_regclass(:relation)::oid::bigint"), {"relation": f"{target_schema}.{target_table}"}
    )
    if type(schema_oid) is not int or type(catalog_oid) is not int:
        raise CodeSetsArchiveError("code-set clone is unavailable")
    return schema_oid, catalog_oid


async def _verify_clone(
    session, schema: str, schema_oid: int, catalog_oid: int, predecessor_oid: int | None = None
) -> None:
    """Reject replacement of the cloned relation or added objects."""
    from process import reference_family_archive as native

    observed_oid = await session.scalar(text("SELECT to_regnamespace(:schema)::oid::bigint"), {"schema": schema})
    observed_table = await session.scalar(
        text("SELECT to_regclass(:relation)::oid::bigint"), {"relation": f"{schema}.{CodeCatalog.__tablename__}"}
    )
    if (observed_oid, observed_table) != (schema_oid, catalog_oid):
        raise CodeSetsArchiveError("code-set clone ownership changed")
    if predecessor_oid is not None:
        observed_predecessor = await session.scalar(
            text("SELECT to_regclass(:relation)::oid::bigint"),
            {"relation": f"{schema}.{PREDECESSOR_TABLE}"},
        )
        if observed_predecessor != predecessor_oid:
            raise CodeSetsArchiveError("code-set predecessor ownership changed")
    allowed = {catalog_oid} | ({predecessor_oid} if predecessor_oid is not None else set())
    for row in await native._namespace_relations(session, schema_oid):
        if not (
            row["relkind"] == "r"
            and row["oid"] in allowed
            or row["relkind"] == "i"
            and row["index_table_oid"] in allowed
        ):
            raise CodeSetsArchiveError("code-set clone contains an unowned relation")


async def prepare_source(session, schema: str, dataset_id: UUID) -> CodeSetsStage:
    """Pin one exact published slice and commit a task-owned native clone."""
    _transaction(session)
    await session.execute(text(f"LOCK TABLE {_schema(schema)}.{CodeCatalog.__tablename__} IN SHARE MODE"))
    generation = await read_generation(session, schema, lock=True)
    count, digest, oid = await scope_receipt(session, schema)
    if (generation.code_catalog_oid, generation.row_count, generation.row_sha256) != (oid, count, digest):
        raise CodeSetsArchiveError("code-set source generation drifted")
    target = stage_schema(dataset_id)
    schema_oid, catalog_oid = await _clone_slice(session, schema, target)
    if (await scope_receipt(session, target, stage=True)) != (count, digest, catalog_oid):
        raise CodeSetsArchiveError("code-set source clone differs")
    await _verify_clone(session, target, schema_oid, catalog_oid)
    if generation.origin_lineage_id is None or generation.origin_generation is None or generation.published_at is None:
        raise CodeSetsArchiveError("code-set source generation is incomplete")
    source = CodeSetsSourceGeneration(
        generation.origin_lineage_id,
        generation.origin_generation,
        generation.published_at,
        count,
        digest,
        _schema_digest(await _column_signature(session, catalog_oid)),
    )
    return CodeSetsStage(dataset_id, target, schema_oid, catalog_oid, source, count, digest)


async def precreate_restore(
    session, *, destination: str, dataset_id: UUID, manifest: dict
) -> tuple[CodeSetsStage, int]:
    """Create both registered stage OIDs before data-only restore or freeze."""
    _transaction(session)
    source = validate_manifest(manifest)
    target = stage_schema(dataset_id)
    name = CodeCatalog.__tablename__
    await session.execute(text(f"CREATE SCHEMA {_schema(target)}"))
    for table_name in (name, PREDECESSOR_TABLE):
        await session.execute(
            text(f"CREATE TABLE {_schema(target)}.{table_name} (LIKE {_schema(destination)}.{name} INCLUDING ALL)")
        )
    schema_oid = await session.scalar(text("SELECT to_regnamespace(:name)::oid::bigint"), {"name": target})
    catalog_oid = await session.scalar(text("SELECT to_regclass(:name)::oid::bigint"), {"name": f"{target}.{name}"})
    predecessor_oid = await session.scalar(
        text("SELECT to_regclass(:name)::oid::bigint"), {"name": f"{target}.{PREDECESSOR_TABLE}"}
    )
    if not all(type(value) is int and value > 0 for value in (schema_oid, catalog_oid, predecessor_oid)):
        raise CodeSetsArchiveError("code-set restore stage is unavailable")
    await _verify_clone(session, target, schema_oid, catalog_oid, predecessor_oid)
    if _schema_digest(await _column_signature(session, catalog_oid)) != source.schema_sha256:
        raise CodeSetsArchiveError("code-set restore schema differs")
    stage = CodeSetsStage(dataset_id, target, schema_oid, catalog_oid, source, source.row_count, source.row_sha256)
    return stage, predecessor_oid


async def verify_stage(session, stage: CodeSetsStage, *, predecessor_oid: int | None = None) -> None:
    """Require unchanged staged relations, rows and physical schema."""
    _transaction(session)
    if not isinstance(stage, CodeSetsStage) or stage.schema_name != stage_schema(stage.dataset_id):
        raise CodeSetsArchiveError("code-set stage identity is invalid")
    await session.execute(text(f"LOCK TABLE {_schema(stage.schema_name)}.{CodeCatalog.__tablename__} IN SHARE MODE"))
    await _verify_clone(session, stage.schema_name, stage.schema_oid, stage.catalog_oid, predecessor_oid)
    if await scope_receipt(session, stage.schema_name, stage=True) != (
        stage.row_count,
        stage.row_sha256,
        stage.catalog_oid,
    ):
        raise CodeSetsArchiveError("code-set stage content changed")
    if _schema_digest(await _column_signature(session, stage.catalog_oid)) != stage.source_generation.schema_sha256:
        raise CodeSetsArchiveError("code-set stage schema changed")


async def _column_signature(session, oid: int) -> tuple:
    column_records = (
        await session.execute(
            text(
                "SELECT attname,atttypid::regtype::text AS type_name,atttypmod,attnotnull,attgenerated,attidentity,"
                "pg_get_expr(def.adbin,def.adrelid) AS default_sql "
                "FROM pg_attribute att LEFT JOIN pg_attrdef def ON def.adrelid=att.attrelid AND def.adnum=att.attnum "
                "WHERE att.attrelid=:oid AND att.attnum>0 AND NOT att.attisdropped ORDER BY att.attnum"
            ),
            {"oid": oid},
        )
    ).all()
    if tuple(column_record[0] for column_record in column_records) != tuple(
        column.name for column in CodeCatalog.__table__.columns
    ):
        raise CodeSetsArchiveError("code-set catalog columns differ")
    primary = (
        (
            await session.execute(
                text(
                    "SELECT att.attname FROM pg_index idx "
                    "JOIN LATERAL unnest(idx.indkey) WITH ORDINALITY AS key(attnum,ordinal) ON TRUE "
                    "JOIN pg_attribute att ON att.attrelid=idx.indrelid AND att.attnum=key.attnum "
                    "WHERE idx.indrelid=:oid AND idx.indisprimary ORDER BY key.ordinal"
                ),
                {"oid": oid},
            )
        )
        .scalars()
        .all()
    )
    if primary != ["code_system", "code"]:
        raise CodeSetsArchiveError("code-set catalog key differs")
    return tuple(tuple(column_record) for column_record in column_records)


async def _has_collision(
    session, destination: str, candidate: str, candidate_table: str = CodeCatalog.__tablename__
) -> bool:
    name = CodeCatalog.__tablename__
    return bool(
        await session.scalar(
            text(
                f"SELECT EXISTS(SELECT 1 FROM {_schema(candidate)}.{candidate_table} AS incoming "
                f"JOIN {_schema(destination)}.{name} AS live USING (code_system,code) "
                "WHERE live.source IS DISTINCT FROM incoming.source)"
            )
        )
    )


async def _replace_slice(
    session, destination: str, candidate: str, candidate_table: str = CodeCatalog.__tablename__
) -> None:
    name = CodeCatalog.__tablename__
    await session.execute(
        text(f"DELETE FROM {_schema(destination)}.{name} WHERE source=ANY(CAST(:sources AS text[]))"),
        {"sources": [source for source, _ in SOURCES]},
    )
    columns = ",".join(f'"{column.name}"' for column in CodeCatalog.__table__.columns)
    await session.execute(
        text(
            f"INSERT INTO {_schema(destination)}.{name} ({columns}) "
            f"SELECT {columns} FROM {_schema(candidate)}.{candidate_table}"
        )
    )


async def _set_generation(
    session, schema: str, local_generation: int, origin: CodeSetsGeneration
) -> CodeSetsGeneration:
    row = (
        (
            await session.execute(
                text(
                    f"UPDATE {_schema(schema)}.{TABLE} SET local_generation=:local_generation,"
                    "origin_lineage_id=:origin_lineage_id,origin_generation=:origin_generation,"
                    "published_at=:published_at,code_catalog_oid=:catalog_oid,row_count=:row_count,row_sha256=:digest "
                    "WHERE id=1 RETURNING local_lineage_id,local_generation,origin_lineage_id,"
                    "origin_generation,published_at,code_catalog_oid,row_count,row_sha256"
                ),
                {
                    "local_generation": local_generation,
                    "origin_lineage_id": origin.origin_lineage_id,
                    "origin_generation": origin.origin_generation,
                    "published_at": origin.published_at,
                    "catalog_oid": origin.code_catalog_oid,
                    "row_count": origin.row_count,
                    "digest": origin.row_sha256,
                },
            )
        )
        .mappings()
        .one_or_none()
    )
    return _generation(row)


async def prepare_predecessor(
    session,
    *,
    destination: str,
    stage: CodeSetsStage,
    expected: CodeSetsGeneration,
    precreated_predecessor_oid: int | None = None,
) -> CodeSetsPreparedStage:
    """Copy the destination predecessor into the same stage before its ownership freeze."""
    _transaction(session)
    await session.execute(
        text(f"LOCK TABLE {_schema(destination)}.{CodeCatalog.__tablename__} IN SHARE ROW EXCLUSIVE MODE")
    )
    prior = await read_generation(session, destination, lock=True)
    if prior != expected or prior.local_generation >= _MAX_GENERATION:
        raise CodeSetsArchiveError("code-set destination generation changed")
    count, digest, oid = await _slice_identity(session, destination, complete=prior.origin_generation is not None)
    if prior.origin_generation is None:
        if count:
            raise CodeSetsArchiveError("untracked code-set rows prevent activation")
    elif (prior.code_catalog_oid, prior.row_count, prior.row_sha256) != (oid, count, digest):
        raise CodeSetsArchiveError("code-set destination source rows changed")
    await verify_stage(session, stage, predecessor_oid=precreated_predecessor_oid)
    if await _column_signature(session, stage.catalog_oid) != await _column_signature(session, oid):
        raise CodeSetsArchiveError("code-set destination table shape differs")
    if precreated_predecessor_oid is None:
        predecessor_schema_oid, predecessor_catalog_oid = await _clone_slice(
            session, destination, stage.schema_name, target_table=PREDECESSOR_TABLE, create_schema=False
        )
    else:
        predecessor_schema_oid, predecessor_catalog_oid = stage.schema_oid, precreated_predecessor_oid
        await _verify_clone(session, stage.schema_name, stage.schema_oid, stage.catalog_oid, predecessor_catalog_oid)
        if await session.scalar(text(f"SELECT EXISTS(SELECT 1 FROM {_schema(stage.schema_name)}.{PREDECESSOR_TABLE})")):
            raise CodeSetsArchiveError("code-set predecessor is not empty")
        columns = ",".join(f'"{column.name}"' for column in CodeCatalog.__table__.columns)
        await session.execute(
            text(
                f"INSERT INTO {_schema(stage.schema_name)}.{PREDECESSOR_TABLE} ({columns}) "
                f"SELECT {columns} FROM {_schema(destination)}.{CodeCatalog.__tablename__} "
                "WHERE source=ANY(CAST(:sources AS text[]))"
            ),
            {"sources": [source_name for source_name, _ in SOURCES]},
        )
    predecessor_count, predecessor_digest, _ = await _slice_identity(
        session, stage.schema_name, complete=False, table_name=PREDECESSOR_TABLE, strict=True
    )
    if (predecessor_count, predecessor_digest) != (count, digest):
        raise CodeSetsArchiveError("code-set predecessor clone differs")
    await _verify_clone(session, stage.schema_name, predecessor_schema_oid, stage.catalog_oid, predecessor_catalog_oid)
    return CodeSetsPreparedStage(stage, prior, predecessor_catalog_oid, count, digest)


async def validate_prepared_stage(
    session, *, destination: str, stage: CodeSetsStage, predecessor_oid: int
) -> CodeSetsPreparedStage:
    """Mint the conditional predecessor token from frozen content and current authority."""
    _transaction(session)
    await session.execute(text(f"LOCK TABLE {_schema(destination)}.{CodeCatalog.__tablename__} IN SHARE MODE"))
    prior = await read_generation(session, destination)
    count, digest, oid = await _slice_identity(session, destination, complete=prior.origin_generation is not None)
    if prior.origin_generation is None:
        if count:
            raise CodeSetsArchiveError("untracked code-set rows prevent validation")
    elif (prior.code_catalog_oid, prior.row_count, prior.row_sha256) != (oid, count, digest):
        raise CodeSetsArchiveError("code-set destination source rows changed")
    await verify_stage(session, stage, predecessor_oid=predecessor_oid)
    if await _column_signature(session, predecessor_oid) != await _column_signature(session, oid):
        raise CodeSetsArchiveError("code-set predecessor table shape differs")
    before_count, before_digest, _ = await _slice_identity(
        session, stage.schema_name, complete=False, table_name=PREDECESSOR_TABLE, strict=True
    )
    if (before_count, before_digest) != (count, digest):
        raise CodeSetsArchiveError("code-set predecessor differs")
    return CodeSetsPreparedStage(stage, prior, predecessor_oid, count, digest)


async def activate_stage(session, *, destination: str, prepared: CodeSetsPreparedStage) -> CodeSetsActivation:
    """CAS just the source rows against a frozen, same-inventory predecessor."""
    _transaction(session)
    stage, expected = prepared.stage, prepared.expected
    await session.execute(
        text(f"LOCK TABLE {_schema(destination)}.{CodeCatalog.__tablename__} IN SHARE ROW EXCLUSIVE MODE")
    )
    prior = await read_generation(session, destination, lock=True)
    if prior != expected or prior.local_generation >= _MAX_GENERATION:
        raise CodeSetsArchiveError("code-set destination generation changed")
    count, digest, oid = await _slice_identity(session, destination, complete=prior.origin_generation is not None)
    if prior.origin_generation is None:
        if count:
            raise CodeSetsArchiveError("untracked code-set rows prevent activation")
    elif (prior.code_catalog_oid, prior.row_count, prior.row_sha256) != (oid, count, digest):
        raise CodeSetsArchiveError("code-set destination source rows changed")
    await verify_stage(session, stage, predecessor_oid=prepared.predecessor_catalog_oid)
    if await _column_signature(session, stage.catalog_oid) != await _column_signature(session, oid):
        raise CodeSetsArchiveError("code-set destination table shape differs")
    predecessor_count, predecessor_digest, _ = await _slice_identity(
        session, stage.schema_name, complete=False, table_name=PREDECESSOR_TABLE, strict=True
    )
    if (predecessor_count, predecessor_digest) != (prepared.predecessor_row_count, prepared.predecessor_row_sha256) or (
        count,
        digest,
    ) != (predecessor_count, predecessor_digest):
        raise CodeSetsArchiveError("code-set predecessor changed")
    if await _has_collision(session, destination, stage.schema_name):
        raise CodeSetsArchiveError("code-set candidate key belongs to another source")
    await _replace_slice(session, destination, stage.schema_name)
    actual_count, actual_digest, actual_oid = await scope_receipt(session, destination)
    if (actual_count, actual_digest, actual_oid) != (stage.row_count, stage.row_sha256, oid):
        raise CodeSetsArchiveError("code-set activation result differs")
    source_generation = stage.source_generation
    adopted = CodeSetsGeneration(
        prior.local_lineage_id,
        prior.local_generation + 1,
        source_generation.origin_lineage_id,
        source_generation.origin_generation,
        source_generation.published_at,
        oid,
        actual_count,
        actual_digest,
    )
    current = await _set_generation(session, destination, adopted.local_generation, adopted)
    if current != adopted:
        raise CodeSetsArchiveError("code-set activation authority differs")
    return CodeSetsActivation(
        stage.dataset_id,
        prior,
        current,
        stage.schema_name,
        stage.schema_oid,
        stage.catalog_oid,
        prepared.predecessor_catalog_oid,
        count,
        digest,
    )


async def rollback_activation(session, *, destination: str, activation: CodeSetsActivation) -> CodeSetsGeneration:
    """Restore only the retained predecessor if the candidate is still current."""
    _transaction(session)
    if not isinstance(activation, CodeSetsActivation) or activation.predecessor_schema != predecessor_schema(
        activation.dataset_id
    ):
        raise CodeSetsArchiveError("code-set rollback authority is invalid")
    await session.execute(
        text(f"LOCK TABLE {_schema(destination)}.{CodeCatalog.__tablename__} IN SHARE ROW EXCLUSIVE MODE")
    )
    current = await read_generation(session, destination, lock=True)
    if current != activation.current or current.local_generation >= _MAX_GENERATION:
        raise CodeSetsArchiveError("code-set rollback generation changed")
    if await scope_receipt(session, destination) != (current.row_count, current.row_sha256, current.code_catalog_oid):
        raise CodeSetsArchiveError("code-set rollback source rows changed")
    await session.execute(
        text(f"LOCK TABLE {_schema(activation.predecessor_schema)}.{PREDECESSOR_TABLE} IN SHARE MODE")
    )
    await _verify_clone(
        session,
        activation.predecessor_schema,
        activation.predecessor_schema_oid,
        activation.candidate_catalog_oid,
        activation.predecessor_catalog_oid,
    )
    if await _column_signature(session, activation.predecessor_catalog_oid) != await _column_signature(
        session, current.code_catalog_oid
    ):
        raise CodeSetsArchiveError("code-set rollback table shape differs")
    predecessor_count, predecessor_digest, _ = await _slice_identity(
        session, activation.predecessor_schema, complete=False, table_name=PREDECESSOR_TABLE, strict=True
    )
    if (predecessor_count, predecessor_digest) != (
        activation.predecessor_row_count,
        activation.predecessor_row_sha256,
    ) or await _has_collision(session, destination, activation.predecessor_schema, PREDECESSOR_TABLE):
        raise CodeSetsArchiveError("code-set rollback predecessor differs")
    await _replace_slice(session, destination, activation.predecessor_schema, PREDECESSOR_TABLE)
    restored_count, restored_digest, restored_oid = await _slice_identity(session, destination, complete=False)
    if (restored_count, restored_digest, restored_oid) != (
        predecessor_count,
        predecessor_digest,
        current.code_catalog_oid,
    ):
        raise CodeSetsArchiveError("code-set rollback result differs")
    previous = activation.previous
    restored = CodeSetsGeneration(
        current.local_lineage_id,
        current.local_generation + 1,
        previous.origin_lineage_id,
        previous.origin_generation,
        previous.published_at,
        current.code_catalog_oid if previous.origin_generation is not None else None,
        previous.row_count,
        previous.row_sha256,
    )
    restored_authority = await _set_generation(session, destination, restored.local_generation, restored)
    if restored_authority != restored:
        raise CodeSetsArchiveError("code-set rollback authority differs")
    return restored_authority


async def cleanup_stage(session, stage: CodeSetsStage, *, predecessor_oid: int | None = None) -> None:
    """Drop only a UUID-derived stage whose exact namespace and OIDs still match."""
    _transaction(session)
    if not isinstance(stage, CodeSetsStage) or stage.schema_name != stage_schema(stage.dataset_id):
        raise CodeSetsArchiveError("code-set stage identity is invalid")
    await _verify_clone(session, stage.schema_name, stage.schema_oid, stage.catalog_oid, predecessor_oid)
    if predecessor_oid is not None:
        await session.execute(text(f"DROP TABLE {_schema(stage.schema_name)}.{PREDECESSOR_TABLE}"))
    await session.execute(text(f"DROP TABLE {_schema(stage.schema_name)}.{CodeCatalog.__tablename__}"))
    await session.execute(text(f"DROP SCHEMA {_schema(stage.schema_name)}"))
