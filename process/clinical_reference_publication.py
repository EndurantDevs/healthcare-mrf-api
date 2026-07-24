# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Staging and atomic publication contracts for clinical-reference rows."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Iterable

from db.models import (
    ClinicalArea,
    ClinicalAreaCondition,
    ClinicalAreaTreatment,
    CodeCatalog,
    CodeCrosswalk,
    CodeRelationship,
    CodeSynonym,
    db,
)
from process.clinical_reference_rows import _build_clinical_area_rows
from process.ext.utils import make_class, push_objects
from process.reference_stage import _drop_stage_tables, build_reference_stage_suffix

DEFAULT_BATCH_SIZE = 5000
CLINICAL_REFERENCE_SOURCES = (
    "cdc_icd10cm",
    "nlm_mesh_descriptor",
    "nlm_mesh_supplemental",
    "nlm_rxnorm",
    "nlm_snomedct_us",
    "nlm_snomedct_icd10cm_map",
    "rxclass_medrt",
)


@dataclass(frozen=True)
class ClinicalAreaRows:
    area_rows: list[dict[str, Any]]
    condition_rows: list[dict[str, Any]]
    treatment_rows: list[dict[str, Any]]


@dataclass(frozen=True)
class ClinicalStageModels:
    stage_by_model: dict[Any, Any]
    shared_models: tuple[Any, ...]
    area_models: tuple[Any, ...]


def _stage_index_name(stage_table: str, index_name: str) -> str:
    return f"{stage_table}_idx_{index_name}"


def _batch(
    row_maps: list[dict[str, Any]],
    size: int = DEFAULT_BATCH_SIZE,
) -> Iterable[list[dict[str, Any]]]:
    for start_index in range(0, len(row_maps), size):
        yield row_maps[start_index : start_index + size]


async def _ensure_schema_exists(schema: str) -> None:
    await db.status(f"CREATE SCHEMA IF NOT EXISTS {schema};")


async def _ensure_unified_code_tables(schema: str) -> None:
    for model_class in (CodeCatalog, CodeCrosswalk, CodeSynonym, CodeRelationship):
        await db.create_table(model_class.__table__, checkfirst=True)
    for model_class in (CodeCatalog, CodeCrosswalk, CodeSynonym, CodeRelationship):
        await _create_stage_indexes(model_class, schema)


def _source_sql_list() -> str:
    return ", ".join(f"'{source_name}'" for source_name in CLINICAL_REFERENCE_SOURCES)


async def _create_stage_indexes(stage_class: Any, schema: str) -> None:
    primary_elements = getattr(stage_class, "__my_index_elements__", None)
    if primary_elements:
        await db.status(
            f"CREATE UNIQUE INDEX IF NOT EXISTS {stage_class.__tablename__}_idx_primary "
            f"ON {schema}.{stage_class.__tablename__} "
            f"({', '.join(primary_elements)});"
        )
    for index_map in getattr(stage_class, "__my_additional_indexes__", []) or []:
        index_name = index_map.get("name", "_".join(index_map.get("index_elements")))
        using_clause = f"USING {index_map.get('using')} " if index_map.get("using") else ""
        where_clause = f" WHERE {index_map.get('where')}" if index_map.get("where") else ""
        await db.status(
            f"CREATE INDEX IF NOT EXISTS {_stage_index_name(stage_class.__tablename__, index_name)} "
            f"ON {schema}.{stage_class.__tablename__} {using_clause}"
            f"({', '.join(index_map.get('index_elements'))}){where_clause};"
        )


async def _publish_table(model_class: Any, stage_class: Any, schema: str) -> None:
    live_table = model_class.__main_table__
    await db.status(f"DROP TABLE IF EXISTS {schema}.{live_table};")
    await db.status(
        f"ALTER TABLE IF EXISTS {schema}.{stage_class.__tablename__} "
        f"RENAME TO {live_table};"
    )
    await db.status(
        f"ALTER INDEX IF EXISTS {schema}.{stage_class.__tablename__}_idx_primary "
        f"RENAME TO {live_table}_idx_primary;"
    )
    for index_map in getattr(stage_class, "__my_additional_indexes__", []) or []:
        index_name = index_map.get("name", "_".join(index_map.get("index_elements")))
        await db.status(
            f"ALTER INDEX IF EXISTS "
            f"{schema}.{_stage_index_name(stage_class.__tablename__, index_name)} "
            f"RENAME TO {live_table}_idx_{index_name};"
        )


async def _merge_code_catalog_stage(stage_class: Any, schema: str) -> None:
    source_sql = _source_sql_list()
    await db.status(
        f"DELETE FROM {schema}.{CodeCatalog.__tablename__} "
        f"WHERE source IN ({source_sql});"
    )
    await db.status(
        f"""
        INSERT INTO {schema}.{CodeCatalog.__tablename__}
            (code_system, code, code_type, display_name, short_description, long_description,
             is_active, source, source_release, source_attribution, updated_at)
        SELECT code_system, code, code_type, display_name, short_description, long_description,
               is_active, source, source_release, source_attribution, updated_at
          FROM {schema}.{stage_class.__tablename__}
        ON CONFLICT (code_system, code) DO UPDATE SET
            code_type = EXCLUDED.code_type,
            display_name = EXCLUDED.display_name,
            short_description = EXCLUDED.short_description,
            long_description = EXCLUDED.long_description,
            is_active = EXCLUDED.is_active,
            source = EXCLUDED.source,
            source_release = EXCLUDED.source_release,
            source_attribution = EXCLUDED.source_attribution,
            updated_at = EXCLUDED.updated_at;
        """
    )


async def _merge_code_crosswalk_stage(stage_class: Any, schema: str) -> None:
    source_sql = _source_sql_list()
    await db.status(
        f"DELETE FROM {schema}.{CodeCrosswalk.__tablename__} "
        f"WHERE source IN ({source_sql});"
    )
    await db.status(
        f"""
        INSERT INTO {schema}.{CodeCrosswalk.__tablename__}
            (from_system, from_code, to_system, to_code, match_type, confidence,
             source, source_attribution, updated_at)
        SELECT from_system, from_code, to_system, to_code, match_type, confidence,
               source, source_attribution, updated_at
          FROM {schema}.{stage_class.__tablename__}
        ON CONFLICT (from_system, from_code, to_system, to_code) DO UPDATE SET
            match_type = EXCLUDED.match_type,
            confidence = EXCLUDED.confidence,
            source = EXCLUDED.source,
            source_attribution = EXCLUDED.source_attribution,
            updated_at = EXCLUDED.updated_at;
        """
    )


async def _merge_replace_source_table(
    model_class: Any,
    stage_class: Any,
    schema: str,
) -> None:
    source_sql = _source_sql_list()
    column_names = [column.name for column in model_class.__table__.columns]
    column_sql = ", ".join(column_names)
    await db.status(
        f"DELETE FROM {schema}.{model_class.__tablename__} "
        f"WHERE source IN ({source_sql});"
    )
    await db.status(
        f"""
        INSERT INTO {schema}.{model_class.__tablename__} ({column_sql})
        SELECT {column_sql}
          FROM {schema}.{stage_class.__tablename__}
        ON CONFLICT DO NOTHING;
        """
    )


async def _push(stage_class: Any, row_maps: list[dict[str, Any]]) -> int:
    pushed_count = 0
    for row_chunk in _batch(row_maps):
        await push_objects(row_chunk, stage_class)
        pushed_count += len(row_chunk)
    return pushed_count


async def _create_stage_models(
    schema: str,
    import_suffix: str,
    run_id: str | None = None,
) -> ClinicalStageModels:
    shared_models = (CodeCatalog, CodeCrosswalk, CodeSynonym, CodeRelationship)
    area_models = (ClinicalArea, ClinicalAreaCondition, ClinicalAreaTreatment)
    stage_suffix = build_reference_stage_suffix("cr", import_suffix, run_id)
    stage_by_model = {
        model_class: make_class(model_class, stage_suffix)
        for model_class in shared_models + area_models
    }
    stage_models = ClinicalStageModels(stage_by_model, shared_models, area_models)
    await _drop_stage_models(schema, stage_models)
    try:
        for stage_class in stage_by_model.values():
            await db.create_table(stage_class.__table__, checkfirst=True)
        return stage_models
    except BaseException:
        await _drop_stage_models(schema, stage_models)
        raise


async def _drop_stage_models(
    schema: str,
    stage_models: ClinicalStageModels,
) -> None:
    await _drop_stage_tables(db, schema, stage_models.stage_by_model.values())


async def _stage_reference_rows(
    schema: str,
    stage_models: ClinicalStageModels,
    reference_indexes: Any,
) -> ClinicalAreaRows:
    stage_by_model = stage_models.stage_by_model
    await _push(stage_by_model[CodeCatalog], list(reference_indexes.concepts_by_identity.values()))
    await _push(stage_by_model[CodeSynonym], list(reference_indexes.synonyms_by_identity.values()))
    await _push(stage_by_model[CodeCrosswalk], list(reference_indexes.crosswalks_by_identity.values()))
    await _push(stage_by_model[CodeRelationship], list(reference_indexes.relationships_by_identity.values()))
    area_rows, condition_rows, treatment_rows = _build_clinical_area_rows(
        reference_indexes.concepts_by_identity,
        reference_indexes.relationships_by_identity,
    )
    await _push(stage_by_model[ClinicalArea], area_rows)
    await _push(stage_by_model[ClinicalAreaCondition], condition_rows)
    await _push(stage_by_model[ClinicalAreaTreatment], treatment_rows)
    for stage_class in stage_by_model.values():
        await _create_stage_indexes(stage_class, schema)
    return ClinicalAreaRows(area_rows, condition_rows, treatment_rows)


async def _catalog_stage_count(
    schema: str,
    stage_models: ClinicalStageModels,
    test_mode: bool,
) -> int:
    stage_table = stage_models.stage_by_model[CodeCatalog].__tablename__
    concept_count = int(
        await db.scalar(f"SELECT COUNT(*) FROM {schema}.{stage_table};") or 0
    )
    minimum_count = int(
        os.getenv("HLTHPRT_CLINICAL_REFERENCE_MIN_ROWS", "1" if test_mode else "1000")
    )
    if concept_count < minimum_count:
        raise RuntimeError(
            f"Clinical reference stage has {concept_count} code rows, "
            f"below minimum {minimum_count}."
        )
    return concept_count


async def _publish_reference_stages(
    schema: str,
    stage_models: ClinicalStageModels,
) -> None:
    stage_by_model = stage_models.stage_by_model
    async with db.transaction():
        await _merge_code_catalog_stage(stage_by_model[CodeCatalog], schema)
        await _merge_code_crosswalk_stage(stage_by_model[CodeCrosswalk], schema)
        await _merge_replace_source_table(CodeSynonym, stage_by_model[CodeSynonym], schema)
        await _merge_replace_source_table(
            CodeRelationship,
            stage_by_model[CodeRelationship],
            schema,
        )
        for model_class in stage_models.area_models:
            await _publish_table(model_class, stage_by_model[model_class], schema)
        for model_class in stage_models.shared_models:
            stage_table = stage_by_model[model_class].__tablename__
            await db.status(f"DROP TABLE IF EXISTS {schema}.{stage_table};")
