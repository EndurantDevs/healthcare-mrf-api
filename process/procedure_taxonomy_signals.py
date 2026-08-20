# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Shared taxonomy-signal materialization for table-swap importers."""

from __future__ import annotations

from db.connection import db
from db.models import NPIDataTaxonomy, NUCCTaxonomy
from db.procedure_taxonomy_signal_sql import procedure_taxonomy_signal_insert_sql
from process.provider_quality_parts.table_helpers import _table_columns


_PROVIDER_COLUMNS = frozenset({"npi", "year", "provider_type"})
_PROVIDER_PROCEDURE_COLUMNS = frozenset(
    {"npi", "year", "procedure_code", "total_services", "total_beneficiaries"}
)
_QUALITY_FEATURE_COLUMNS = frozenset(
    {"npi", "year", "taxonomy_code", "taxonomy_classification"}
)
_NPI_TAXONOMY_COLUMNS = frozenset(
    {
        "npi",
        "healthcare_provider_taxonomy_code",
        "healthcare_provider_primary_taxonomy_switch",
        "checksum",
    }
)
_NUCC_TAXONOMY_COLUMNS = frozenset(
    {"code", "classification", "specialization", "display_name"}
)


async def _compatible_table(
    schema: str,
    model: type | None,
    required_columns: frozenset[str],
) -> str | None:
    if model is None:
        return None
    table = model.__tablename__
    columns = await _table_columns(schema, table)
    return table if required_columns.issubset(columns) else None


async def materialize_procedure_taxonomy_signals(
    *,
    schema: str,
    signal_model: type | None,
    provider_model: type,
    provider_procedure_model: type,
    quality_feature_model: type | None,
) -> None:
    """Build signals from the exact staged and live relations being published."""

    if signal_model is None:
        return
    await db.status(f"TRUNCATE TABLE {schema}.{signal_model.__tablename__};")
    provider_table = await _compatible_table(
        schema,
        provider_model,
        _PROVIDER_COLUMNS,
    )
    provider_procedure_table = await _compatible_table(
        schema,
        provider_procedure_model,
        _PROVIDER_PROCEDURE_COLUMNS,
    )
    if provider_table is None or provider_procedure_table is None:
        return

    quality_feature_table = await _compatible_table(
        schema,
        quality_feature_model,
        _QUALITY_FEATURE_COLUMNS,
    )
    npi_taxonomy_table = await _compatible_table(
        schema,
        NPIDataTaxonomy,
        _NPI_TAXONOMY_COLUMNS,
    )
    nucc_taxonomy_table = await _compatible_table(
        schema,
        NUCCTaxonomy,
        _NUCC_TAXONOMY_COLUMNS,
    )
    await db.status(
        procedure_taxonomy_signal_insert_sql(
            schema=schema,
            signal_table=signal_model.__tablename__,
            provider_table=provider_table,
            provider_procedure_table=provider_procedure_table,
            quality_feature_table=quality_feature_table,
            npi_taxonomy_table=npi_taxonomy_table,
            nucc_taxonomy_table=nucc_taxonomy_table,
        )
    )
