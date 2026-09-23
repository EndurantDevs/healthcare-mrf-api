# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fixed source-scope effects for a clinical reference result cutover."""

from __future__ import annotations

import re

from sqlalchemy import text

from db.models import (
    ClinicalArea,
    ClinicalAreaCondition,
    ClinicalAreaTreatment,
    CodeCatalog,
    CodeCrosswalk,
    CodeRelationship,
    CodeSynonym,
)
from process.clinical_reference_publication import CLINICAL_REFERENCE_SOURCES
from process.entity_address_snapshot_receipt import _projected_row_identity

SHARED_MODELS = (CodeCatalog, CodeCrosswalk, CodeSynonym, CodeRelationship)
AREA_MODELS = (ClinicalArea, ClinicalAreaCondition, ClinicalAreaTreatment)
_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


class ClinicalReferenceScopeError(RuntimeError):
    """A clinical source slice conflicts with another catalog owner."""


def _schema(value: str) -> str:
    if not isinstance(value, str) or not _IDENTIFIER.fullmatch(value) or len(value.encode()) > 63:
        raise ClinicalReferenceScopeError("clinical reference schema is invalid")
    return f'"{value}"'


def _table(schema: str, model) -> str:
    return f'{_schema(schema)}."{model.__tablename__}"'


def before_name(model) -> str:
    """Name the retained source-scoped table for one shared model."""
    return "before_" + model.__tablename__


def _keys(model) -> str:
    return ", ".join(f'"{column.name}"' for column in model.__table__.primary_key.columns)


def _require_transaction(session) -> None:
    if not callable(getattr(session, "in_transaction", None)) or not session.in_transaction():
        raise ClinicalReferenceScopeError("clinical reference cutover requires a transaction")


async def _require_scope(session, schema: str, model) -> None:
    table = _table(schema, model)
    if await session.scalar(
        text(f"SELECT EXISTS(SELECT 1 FROM {table} WHERE (source = ANY(CAST(:sources AS text[]))) IS NOT TRUE)"),
        {"sources": list(CLINICAL_REFERENCE_SOURCES)},
    ):
        raise ClinicalReferenceScopeError("clinical reference source scope differs")


async def capture_shared_before_image(session, *, destination: str, predecessor: str) -> None:
    """Freeze only clinical-owned rows in a fresh caller-owned schema."""
    _require_transaction(session)
    await session.execute(text(f"CREATE SCHEMA {_schema(predecessor)}"))
    for model in SHARED_MODELS:
        live = _table(destination, model)
        prior = _table(predecessor, model)
        await session.execute(text(f"LOCK TABLE {live} IN SHARE ROW EXCLUSIVE MODE"))
        await session.execute(text(f"CREATE TABLE {prior} (LIKE {live} INCLUDING ALL)"))
        await session.execute(
            text(f"INSERT INTO {prior} SELECT * FROM {live} WHERE source=ANY(CAST(:sources AS text[]))"),
            {"sources": list(CLINICAL_REFERENCE_SOURCES)},
        )


async def prepare_shared_before_image(session, *, destination: str, stage: str) -> dict[str, object]:
    """Capture a scoped predecessor in the protected stage's MVCC snapshot."""
    from process.reference_family_result_generation import (
        current_reference_family_relation_oids,
        read_reference_family_result_generation_authority,
    )

    _require_transaction(session)
    authority = await read_reference_family_result_generation_authority(
        session, importer_id="clinical-reference", schema_name=destination
    )
    live_oids = await current_reference_family_relation_oids(
        session, importer_id="clinical-reference", schema_name=destination
    )
    if authority.relation_oids is not None and authority.relation_oids != live_oids:
        raise ClinicalReferenceScopeError("clinical reference serving authority is drifted")
    for model in SHARED_MODELS:
        live = _table(destination, model)
        prior = f'{_schema(stage)}."{before_name(model)}"'
        await session.execute(text(f"CREATE TABLE {prior} (LIKE {live} INCLUDING ALL)"))
        await session.execute(
            text(f"INSERT INTO {prior} SELECT * FROM {live} WHERE source=ANY(CAST(:sources AS text[]))"),
            {"sources": list(CLINICAL_REFERENCE_SOURCES)},
        )
    receipt = await shared_effect_receipt(session, schema=stage, prefixed=True)
    return {
        "authority": authority.as_dict(),
        "live_oids": list(live_oids),
        "tables": [
            {key: table[key] for key in ("table_name", "row_count", "row_sha256")} for table in receipt["tables"]
        ],
    }


async def verify_shared_before_image_witness(
    session, *, destination: str, stage: str, witness: dict[str, object]
) -> None:
    """CAS the serving generation, shared OIDs, and clinical-row content."""
    from process.reference_family_result_generation import (
        current_reference_family_relation_oids,
        read_reference_family_result_generation_authority,
    )

    authority = await read_reference_family_result_generation_authority(
        session, importer_id="clinical-reference", schema_name=destination, lock=True
    )
    if authority.as_dict() != witness.get("authority"):
        raise ClinicalReferenceScopeError("clinical reference serving generation changed during preparation")
    if list(
        await current_reference_family_relation_oids(session, importer_id="clinical-reference", schema_name=destination)
    ) != witness.get("live_oids"):
        raise ClinicalReferenceScopeError("clinical reference serving relation OIDs changed during preparation")
    staged = await shared_effect_receipt(session, schema=stage, prefixed=True)
    expected_tables = witness.get("tables")
    staged_tables = [
        {key: table[key] for key in ("table_name", "row_count", "row_sha256")} for table in staged["tables"]
    ]
    if staged_tables != expected_tables:
        raise ClinicalReferenceScopeError("clinical reference frozen before-image changed")
    current_rows = []
    for model in SHARED_MODELS:
        count, digest = await _projected_row_identity(
            session,
            destination,
            model.__tablename__,
            row_json_sql="pg_catalog.to_jsonb(row_value)",
            where_sql="WHERE source=ANY(CAST(:sources AS text[]))",
            parameters={"sources": list(CLINICAL_REFERENCE_SOURCES)},
        )
        current_rows.append({"table_name": model.__tablename__, "row_count": count, "row_sha256": digest})
    if current_rows != expected_tables:
        raise ClinicalReferenceScopeError("clinical reference scoped rows changed during preparation")


async def shared_effect_receipt(session, *, schema: str, prefixed: bool = False) -> dict[str, object]:
    """Bind four exact before-image OIDs and row digests for retained rollback."""
    _require_transaction(session)
    tables = []
    for model in SHARED_MODELS:
        name = before_name(model) if prefixed else model.__tablename__
        if prefixed:
            if await session.scalar(
                text(
                    f'SELECT EXISTS(SELECT 1 FROM {_schema(schema)}."{name}" '
                    "WHERE (source = ANY(CAST(:sources AS text[]))) IS NOT TRUE)"
                ),
                {"sources": list(CLINICAL_REFERENCE_SOURCES)},
            ):
                raise ClinicalReferenceScopeError("clinical reference source scope differs")
        else:
            await _require_scope(session, schema, model)
        oid = await session.scalar(text("SELECT to_regclass(:relation)::oid::bigint"), {"relation": f"{schema}.{name}"})
        if type(oid) is not int or oid <= 0:
            raise ClinicalReferenceScopeError("clinical reference before-image is unavailable")
        count, digest = await _projected_row_identity(
            session, schema, name, row_json_sql="pg_catalog.to_jsonb(row_value)"
        )
        tables.append(
            {"table_name": model.__tablename__, "relation_oid": oid, "row_count": count, "row_sha256": digest}
        )
    return {"schema_name": schema, "tables": tables}


async def replace_shared_sources(session, *, destination: str, replacement: str) -> None:
    """Replace only the seven fixed clinical sources, rejecting foreign key collisions."""
    _require_transaction(session)
    for model in SHARED_MODELS:
        live = _table(destination, model)
        incoming = _table(replacement, model)
        await session.execute(text(f"LOCK TABLE {live} IN SHARE ROW EXCLUSIVE MODE"))
        await _require_scope(session, replacement, model)
        key_columns = _keys(model)
        conflict = await session.scalar(
            text(
                f"SELECT EXISTS(SELECT 1 FROM {incoming} incoming JOIN {live} incumbent "
                f"USING ({key_columns}) WHERE (incumbent.source = ANY(CAST(:sources AS text[]))) IS NOT TRUE)"
            ),
            {"sources": list(CLINICAL_REFERENCE_SOURCES)},
        )
        if conflict:
            raise ClinicalReferenceScopeError("clinical reference key collides with another source")
        await session.execute(
            text(f"DELETE FROM {live} WHERE source=ANY(CAST(:sources AS text[]))"),
            {"sources": list(CLINICAL_REFERENCE_SOURCES)},
        )
        columns = ", ".join(f'"{column.name}"' for column in model.__table__.columns)
        await session.execute(text(f"INSERT INTO {live} ({columns}) SELECT {columns} FROM {incoming}"))


async def rollback_clinical_result(
    session,
    *,
    destination: str,
    predecessor: str,
    outgoing: str,
    current_relation_oids: dict[str, int],
    target_relation_oids: dict[str, int],
    generations: tuple[object, object],
    before_image: dict[str, object],
) -> dict[str, object]:
    """Restore one immediate predecessor and retain the replaced scoped rows."""
    from process import reference_family_archive as family
    from process import reference_family_result_generation as generation

    _require_transaction(session)
    spec = family.reference_family_spec("clinical-reference")
    if (
        set(current_relation_oids) != set(spec.table_names)
        or set(target_relation_oids) != {model.__tablename__ for model in AREA_MODELS}
        or before_image.get("schema_name") != predecessor
    ):
        raise ClinicalReferenceScopeError("clinical reference rollback scope differs")
    current_serving_generation = generation.validate_reference_family_serving_generation(generations[0])
    target_serving_generation = generation.validate_reference_family_serving_generation(generations[1])
    await family._lock_family(session, destination, spec.table_names, "ACCESS EXCLUSIVE")
    await family._lock_family(
        session, predecessor, tuple(model.__tablename__ for model in AREA_MODELS), "ACCESS EXCLUSIVE"
    )
    authority = await generation.read_reference_family_result_generation_authority(
        session, importer_id="clinical-reference", schema_name=destination, lock=True
    )
    live_pairs = await family._incumbent_pairs(session, spec, destination)
    target_pairs = await family._incumbent_pairs(
        session, family.ReferenceFamilySpec("clinical-reference", AREA_MODELS), predecessor
    )
    if (
        dict(live_pairs) != current_relation_oids
        or dict(target_pairs) != target_relation_oids
        or authority.serving_generation != current_serving_generation
        or authority.relation_oids
        != tuple(current_relation_oids[name] for name in generation.RELATION_NAMES_BY_IMPORTER["clinical-reference"])
        or await shared_effect_receipt(session, schema=predecessor) != before_image
    ):
        raise ClinicalReferenceScopeError("clinical reference rollback identity changed")
    await capture_shared_before_image(session, destination=destination, predecessor=outgoing)
    for model in AREA_MODELS:
        name = model.__tablename__
        await session.execute(text(f"ALTER TABLE {_table(destination, model)} SET SCHEMA {_schema(outgoing)}"))
        await session.execute(text(f"ALTER TABLE {_table(predecessor, model)} SET SCHEMA {_schema(destination)}"))
    await replace_shared_sources(session, destination=destination, replacement=predecessor)
    adopted = await generation.publish_adopted_reference_family_generation(
        session, importer_id="clinical-reference", schema_name=destination, source_generation=target_serving_generation
    )
    expected_oids_by_name = {**current_relation_oids, **target_relation_oids}
    live_pairs = await family._incumbent_pairs(session, spec, destination)
    if dict(live_pairs) != expected_oids_by_name or adopted.serving_generation != target_serving_generation:
        raise ClinicalReferenceScopeError("clinical reference rollback result differs")
    return await shared_effect_receipt(session, schema=outgoing)
