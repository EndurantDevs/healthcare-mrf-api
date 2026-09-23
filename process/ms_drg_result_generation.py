# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Durable authority for the exact live MS-DRG source slices."""

from __future__ import annotations

import hashlib
import json
import re

from sqlalchemy import text

from db.models import CodeCatalog, CodeRelationship, CodeSynonym
from process.entity_address_snapshot_receipt import _projected_row_identity
from process.ms_drg_publication import SOURCE_ICD10CM_INDEX, SOURCE_ICD10PCS_INDEX, SOURCE_MS_DRG, SOURCES

TABLE = "ms_drg_result_generation"
CONTRACT = "ms-drg-source-generation.v1"
_SCOPES = (
    (CodeCatalog.__tablename__, SOURCES),
    (CodeSynonym.__tablename__, (SOURCE_MS_DRG,)),
    (CodeRelationship.__tablename__, (SOURCE_ICD10CM_INDEX, SOURCE_ICD10PCS_INDEX)),
)
_MODELS = (CodeCatalog, CodeSynonym, CodeRelationship)
_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


def _quoted(value: str) -> str:
    if not isinstance(value, str) or not _IDENTIFIER.fullmatch(value) or len(value.encode()) > 63:
        raise ValueError("invalid MS-DRG schema")
    return f'"{value}"'


async def _table_shape(session, oid: int, model) -> str:
    columns = (
        await session.execute(
            text(
                "SELECT attname,atttypid::regtype::text,atttypmod,attnotnull,attgenerated,attidentity,"
                "pg_get_expr(def.adbin,def.adrelid) "
                "FROM pg_attribute att LEFT JOIN pg_attrdef def ON def.adrelid=att.attrelid AND def.adnum=att.attnum "
                "WHERE att.attrelid=:oid AND att.attnum>0 AND NOT att.attisdropped ORDER BY att.attnum"
            ),
            {"oid": oid},
        )
    ).all()
    if tuple(column_shape[0] for column_shape in columns) != tuple(column.name for column in model.__table__.columns):
        raise RuntimeError("MS-DRG result columns differ")
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
    if primary != [column.name for column in model.__table__.primary_key.columns]:
        raise RuntimeError("MS-DRG result key differs")
    return hashlib.sha256(
        json.dumps([list(map(list, columns)), primary], default=str, separators=(",", ":")).encode()
    ).hexdigest()


async def capture_result(session, schema: str) -> dict:
    """Capture all current owned slices, including retained optional relationships."""
    if not session.in_transaction():
        raise RuntimeError("MS-DRG result transaction is required")
    _quoted(schema)
    tables = []
    for (name, owned_sources), model in zip(_SCOPES, _MODELS, strict=True):
        oid = await session.scalar(
            text("SELECT to_regclass(:relation)::oid::bigint"),
            {"relation": f"{schema}.{name}"},
        )
        if type(oid) is not int or oid <= 0:
            raise RuntimeError("MS-DRG result relation is missing")
        count, digest = await _projected_row_identity(
            session,
            schema,
            name,
            row_json_sql="pg_catalog.to_jsonb(row_value)",
            where_sql="WHERE row_value.source=ANY(CAST(:sources AS text[]))",
            parameters={"sources": list(owned_sources)},
        )
        tables.append(
            {
                "table": name,
                "sources": list(owned_sources),
                "relation_oid": oid,
                "row_count": count,
                "row_sha256": digest,
                "schema_sha256": await _table_shape(session, oid, model),
            }
        )
    content_rows = [
        {key: table_receipt[key] for key in ("table", "sources", "row_count", "row_sha256", "schema_sha256")}
        for table_receipt in tables
    ]
    content_sha256 = hashlib.sha256(
        json.dumps(content_rows, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return {"contract": CONTRACT, "tables": tables, "content_sha256": content_sha256}


async def publish_local_generation(session, schema: str, *, include_relationships: bool) -> dict:
    """Advance only inside the caller's locked, atomic publication transaction."""
    if not session.in_transaction() or type(include_relationships) is not bool:
        raise RuntimeError("MS-DRG publication transaction is required")
    qualified = _quoted(schema)
    prior = (
        (
            await session.execute(
                text(f"SELECT local_lineage_id,local_generation FROM {qualified}.{TABLE} WHERE id=1 FOR UPDATE")
            )
        )
        .mappings()
        .one_or_none()
    )
    if prior is None or prior["local_generation"] >= (1 << 63) - 1:
        raise RuntimeError("MS-DRG generation authority is unavailable")
    receipt = await capture_result(session, schema)
    next_generation = prior["local_generation"] + 1
    published_generation = (
        (
            await session.execute(
                text(
                    f"UPDATE {qualified}.{TABLE} SET local_generation=:generation,"
                    "origin_lineage_id=local_lineage_id,origin_generation=:generation,"
                    "published_at=clock_timestamp(),include_relationships=:include_relationships,"
                    "receipt=CAST(:receipt AS jsonb) WHERE id=1 "
                    "RETURNING local_lineage_id,local_generation,origin_lineage_id,origin_generation,"
                    "published_at,include_relationships,receipt"
                ),
                {
                    "generation": next_generation,
                    "include_relationships": include_relationships,
                    "receipt": json.dumps(receipt, sort_keys=True, separators=(",", ":")),
                },
            )
        )
        .mappings()
        .one()
    )
    return dict(published_generation)


async def read_current_generation(session, schema: str) -> dict:
    """Reject drift between the publication receipt and serving rows/OIDs."""
    qualified = _quoted(schema)
    await session.execute(
        text("LOCK TABLE " + ", ".join(f"{qualified}.{_quoted(name)}" for name, _ in _SCOPES) + " IN SHARE MODE")
    )
    row = (
        (
            await session.execute(
                text(
                    f"SELECT local_lineage_id,local_generation,origin_lineage_id,origin_generation,"
                    f"published_at,include_relationships,receipt FROM {qualified}.{TABLE} WHERE id=1"
                )
            )
        )
        .mappings()
        .one_or_none()
    )
    if row is None or row["origin_generation"] is None:
        raise RuntimeError("MS-DRG serving generation is unavailable")
    current = await capture_result(session, schema)
    if current != row["receipt"]:
        raise RuntimeError("MS-DRG serving result changed")
    return dict(row)
