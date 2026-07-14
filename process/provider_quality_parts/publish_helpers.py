# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Publish and run-metadata helpers for provider-quality imports."""

from __future__ import annotations

import datetime
import json
from typing import Any

from db.connection import db
from db.models import (
    PricingProviderQualityDomain,
    PricingProviderQualityMeasure,
    PricingProviderQualityScore,
    PricingQppProvider,
    PricingQualityRun,
    PricingSviZcta,
)
from process.provider_quality_parts.config import PROVIDER_QUALITY_MAX_YEAR
from process.provider_quality_parts.execution_helpers import _push_objects_with_retry
from process.provider_quality_parts.lifecycle import _archived_identifier
from process.provider_quality_parts.model_helpers import _cohort_model_classes
from process.provider_quality_parts.state import _safe_int
from process.provider_quality_parts.table_helpers import _table_exists


async def _publish_by_table_rename(classes: dict[str, type], schema: str) -> None:
    final_classes = (
        PricingQppProvider,
        PricingSviZcta,
        PricingProviderQualityMeasure,
        PricingProviderQualityDomain,
        PricingProviderQualityScore,
        *_cohort_model_classes(),
    )

    async def archive_index(index_name: str) -> str:
        """Move a live index name aside before publishing its replacement."""
        archived_name = _archived_identifier(index_name)
        await db.status(f"DROP INDEX IF EXISTS {schema}.{archived_name};")
        await db.status(f"ALTER INDEX IF EXISTS {schema}.{index_name} RENAME TO {archived_name};")
        return archived_name

    for cls in final_classes:
        obj = classes[cls.__name__]
        stage_exists = await _table_exists(schema, obj.__tablename__)
        if not stage_exists:
            raise RuntimeError(
                f"Staging table missing for publish: {schema}.{obj.__tablename__}. "
                "Aborting publish to protect live tables."
            )

    async with db.transaction():
        for cls in final_classes:
            obj = classes[cls.__name__]
            table = cls.__main_table__
            await db.status(f"DROP TABLE IF EXISTS {schema}.{table}_old;")
            await db.status(f"ALTER TABLE IF EXISTS {schema}.{table} RENAME TO {table}_old;")
            await db.status(f"ALTER TABLE IF EXISTS {schema}.{obj.__tablename__} RENAME TO {table};")

            await archive_index(f"{table}_idx_primary")
            await db.status(
                f"ALTER INDEX IF EXISTS {schema}.{obj.__tablename__}_idx_primary RENAME TO {table}_idx_primary;"
            )

            move_indexes = []
            if hasattr(cls, "__my_initial_indexes__") and cls.__my_initial_indexes__:
                move_indexes += cls.__my_initial_indexes__
            if hasattr(cls, "__my_additional_indexes__") and cls.__my_additional_indexes__:
                move_indexes += cls.__my_additional_indexes__

            for index in move_indexes:
                elements = index.get("index_elements")
                if not elements:
                    continue
                base_name = index.get("name") or f"{table}_{'_'.join(elements)}_idx"
                await archive_index(base_name)
                await db.status(
                    f"ALTER INDEX IF EXISTS {schema}.{obj.__tablename__}_{base_name} RENAME TO {base_name};"
                )


async def _insert_run_metadata(
    schema: str,
    run_id: str,
    import_id: str,
    manifest: dict[str, Any],
    *,
    status: str = "published",
) -> None:
    qpp_rows = _safe_int(
        await db.scalar(f"SELECT COUNT(*) FROM {schema}.{PricingQppProvider.__tablename__}"),
        0,
    )
    svi_rows = _safe_int(
        await db.scalar(f"SELECT COUNT(*) FROM {schema}.{PricingSviZcta.__tablename__}"),
        0,
    )
    measure_rows = _safe_int(
        await db.scalar(f"SELECT COUNT(*) FROM {schema}.{PricingProviderQualityMeasure.__tablename__}"),
        0,
    )
    domain_rows = _safe_int(
        await db.scalar(f"SELECT COUNT(*) FROM {schema}.{PricingProviderQualityDomain.__tablename__}"),
        0,
    )
    score_rows = _safe_int(
        await db.scalar(f"SELECT COUNT(*) FROM {schema}.{PricingProviderQualityScore.__tablename__}"),
        0,
    )
    row = {
        "run_id": run_id,
        "import_id": import_id,
        "year": _safe_int(manifest.get("year"), PROVIDER_QUALITY_MAX_YEAR),
        "qpp_source_version": json.dumps(manifest.get("sources", {}).get("qpp_provider", []), ensure_ascii=True)[:128],
        "svi_source_version": json.dumps(manifest.get("sources", {}).get("svi_zcta", []), ensure_ascii=True)[:128],
        "status": status,
        "qpp_rows": qpp_rows,
        "svi_rows": svi_rows,
        "measure_rows": measure_rows,
        "domain_rows": domain_rows,
        "score_rows": score_rows,
        "started_at": datetime.datetime.utcnow(),
        "finished_at": datetime.datetime.utcnow(),
        "created_at": datetime.datetime.utcnow(),
        "updated_at": datetime.datetime.utcnow(),
    }
    await _push_objects_with_retry([row], PricingQualityRun, rewrite=False)
