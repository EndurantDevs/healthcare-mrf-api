# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Model and staging-table helpers for provider-quality imports."""

from __future__ import annotations

from typing import Any

import db.models as db_models
from db.models import (
    PricingProviderQualityDomain,
    PricingProviderQualityMeasure,
    PricingProviderQualityScore,
    PricingQppProvider,
    PricingSviZcta,
)
from process.ext.utils import make_class, return_checksum
from process.provider_quality_parts.config import (
    COHORT_MODEL_CLASS_NAMES,
    PROVIDER_QUALITY_MAX_YEAR,
    PROVIDER_QUALITY_MIN_YEAR,
    PROVIDER_QUALITY_YEAR_WINDOW,
)
from process.provider_quality_parts.state import _safe_int


def _resolve_optional_model(name: str) -> type | None:
    return getattr(db_models, name, None)


PricingProviderQualityFeature = _resolve_optional_model("PricingProviderQualityFeature")
PricingProviderQualityProcedureLSH = _resolve_optional_model("PricingProviderQualityProcedureLSH")
PricingProviderQualityPeerTarget = _resolve_optional_model("PricingProviderQualityPeerTarget")


def _materialize_reporting_years(manifest: dict[str, Any]) -> tuple[int, ...]:
    years: set[int] = set()
    sources = manifest.get("sources")
    if isinstance(sources, dict):
        for dataset_sources in sources.values():
            if not isinstance(dataset_sources, list):
                continue
            for source in dataset_sources:
                if not isinstance(source, dict):
                    continue
                year = _safe_int(source.get("reporting_year"), 0)
                if year > 0:
                    years.add(year)

    manifest_year = _safe_int(manifest.get("year"), 0)
    if manifest_year > 0:
        years.add(manifest_year)

    if not years:
        years.update(PROVIDER_QUALITY_YEAR_WINDOW)

    bounded = sorted(
        year
        for year in years
        if PROVIDER_QUALITY_MIN_YEAR <= year <= PROVIDER_QUALITY_MAX_YEAR
    )
    if bounded:
        return tuple(bounded)
    return (PROVIDER_QUALITY_MAX_YEAR,)


def _cohort_model_classes() -> tuple[type, ...]:
    classes: list[type] = []
    for cls in (
        PricingProviderQualityFeature,
        PricingProviderQualityProcedureLSH,
        PricingProviderQualityPeerTarget,
    ):
        if cls is not None:
            classes.append(cls)
    return tuple(classes)


def _cohort_models_present(classes: dict[str, type]) -> bool:
    return all(name in classes for name in COHORT_MODEL_CLASS_NAMES)


def _model_columns(model: type | None) -> set[str]:
    if model is None or not hasattr(model, "__table__"):
        return set()
    return {column.name for column in model.__table__.columns}


def _first_existing_column(columns: set[str], *candidates: str) -> str | None:
    for candidate in candidates:
        if candidate in columns:
            return candidate
    return None


def _optional_column_pairs(
    available_columns: set[str],
    mapping: tuple[tuple[str, str], ...],
) -> list[tuple[str, str]]:
    used: set[str] = set()
    pairs: list[tuple[str, str]] = []
    for column_name, sql_expr in mapping:
        if column_name in available_columns and column_name not in used:
            pairs.append((column_name, sql_expr))
            used.add(column_name)
    return pairs


def _staging_classes(stage_suffix: str, schema: str) -> dict[str, type]:
    return {
        cls.__name__: make_class(cls, stage_suffix, schema_override=schema)
        for cls in (
            PricingQppProvider,
            PricingSviZcta,
            PricingProviderQualityMeasure,
            PricingProviderQualityDomain,
            PricingProviderQualityScore,
            *_cohort_model_classes(),
        )
    }


def _chunk_job_id(run_id: str, dataset_key: str, source_index: int, reporting_year: int, chunk_index: int) -> str:
    return f"provider_quality_chunk_{run_id}_{dataset_key}_{reporting_year}_{source_index}_{chunk_index}"


def _build_stage_suffix(import_id: str, run_id: str) -> str:
    base = "".join(ch if ch.isalnum() else "_" for ch in import_id).strip("_")[:12] or "import"
    checksum = return_checksum([import_id, run_id]) & 0xFFFFFFFF
    return f"{base}_{checksum:08x}"
