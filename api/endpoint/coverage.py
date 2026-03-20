# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from datetime import datetime, timezone

from sanic import Blueprint, response
from sqlalchemy import func, select, text

from db.models import (ImportHistory, Issuer, NPIData, PartDPharmacyActivity, Plan,
                       PlanDrugStats, PricingProcedure, PricingProvider,
                       PricingProviderProcedure,
                       PricingProviderPrescription, PricingProviderQualityScore,
                       ProviderEnrichmentSummary)

blueprint = Blueprint("coverage", url_prefix="/coverage", version=1)

plan_table = Plan.__table__
issuer_table = Issuer.__table__
import_history_table = ImportHistory.__table__
npi_data_table = NPIData.__table__
provider_enrichment_summary_table = ProviderEnrichmentSummary.__table__
pricing_provider_table = PricingProvider.__table__
pricing_provider_procedure_table = PricingProviderProcedure.__table__
pricing_procedure_table = PricingProcedure.__table__
partd_pharmacy_activity_table = PartDPharmacyActivity.__table__
plan_drug_stats_table = PlanDrugStats.__table__
pricing_provider_quality_score_table = PricingProviderQualityScore.__table__
pricing_provider_prescription_table = PricingProviderPrescription.__table__


def _get_session(request):
    session = getattr(request.ctx, "sa_session", None)
    if session is None:
        raise RuntimeError("SQLAlchemy session not available on request context")
    return session


def _qualified_table_name(table) -> str:
    schema = table.schema or "mrf"
    return f"{schema}.{table.name}"


async def _table_exists(session, table) -> bool:
    result = await session.execute(
        text("SELECT to_regclass(:name)"),
        {"name": _qualified_table_name(table)},
    )
    return bool(result.scalar())


async def _scalar_int(session, stmt) -> int:
    result = await session.execute(stmt)
    return int(result.scalar() or 0)


async def _latest_snapshot_date(session, table_exists: dict[str, bool]) -> str:
    if table_exists.get(import_history_table.name):
        latest_when = (
            await session.execute(
                select(import_history_table.c.when)
                .order_by(import_history_table.c.when.desc())
                .limit(1)
            )
        ).scalar()
        if latest_when is not None:
            return latest_when.isoformat()
    return datetime.now(timezone.utc).isoformat()


@blueprint.get("/statistics", name="coverage.statistics")
async def coverage_statistics(request):
    session = _get_session(request)

    tracked_tables = [
        import_history_table,
        plan_table,
        issuer_table,
        pricing_provider_table,
        pricing_provider_procedure_table,
        provider_enrichment_summary_table,
        npi_data_table,
        pricing_procedure_table,
        partd_pharmacy_activity_table,
        plan_drug_stats_table,
        pricing_provider_quality_score_table,
        pricing_provider_prescription_table,
    ]
    table_exists: dict[str, bool] = {}
    for table in tracked_tables:
        table_exists[table.name] = await _table_exists(session, table)

    payload = {
        "marketplace_plans": 0,
        "marketplace_issuers": 0,
        "provider_coverage_profiles": 0,
        "medicare_clinicians": 0,
        "hospitals_with_medicare_enrollment": 0,
        "providers_with_procedure_history": 0,
        "procedure_codes_tracked": 0,
        "zip_codes_with_procedure_coverage": 0,
        "active_medicare_pharmacies": 0,
        "formularies_tracked": 0,
        "drug_coverage_rows": 0,
        "providers_with_quality_scores": 0,
        "providers_with_prescription_history": 0,
        "prescription_codes_tracked": 0,
    }

    payload["snapshot_date"] = await _latest_snapshot_date(session, table_exists)

    if table_exists.get(plan_table.name):
        payload["marketplace_plans"] = await _scalar_int(session, select(func.count()).select_from(plan_table))

    if table_exists.get(issuer_table.name):
        payload["marketplace_issuers"] = await _scalar_int(session, select(func.count()).select_from(issuer_table))

    if table_exists.get(pricing_provider_table.name):
        payload["provider_coverage_profiles"] = await _scalar_int(
            session,
            select(func.count(func.distinct(pricing_provider_table.c.npi))),
        )
        payload["zip_codes_with_procedure_coverage"] = await _scalar_int(
            session,
            select(func.count(func.distinct(pricing_provider_table.c.zip5)))
            .select_from(pricing_provider_table)
            .where(
                pricing_provider_table.c.zip5.is_not(None),
                func.length(func.trim(pricing_provider_table.c.zip5)) > 0,
            ),
        )

    if table_exists.get(pricing_provider_procedure_table.name):
        payload["providers_with_procedure_history"] = await _scalar_int(
            session,
            select(func.count(func.distinct(pricing_provider_procedure_table.c.npi))).select_from(
                pricing_provider_procedure_table
            ),
        )
    elif table_exists.get(pricing_provider_table.name):
        payload["providers_with_procedure_history"] = await _scalar_int(
            session,
            select(func.count())
            .select_from(pricing_provider_table)
            .where(func.coalesce(pricing_provider_table.c.total_distinct_hcpcs_codes, 0.0) > 0.0),
        )

    if table_exists.get(pricing_procedure_table.name):
        payload["procedure_codes_tracked"] = await _scalar_int(
            session,
            select(func.count()).select_from(pricing_procedure_table),
        )

    if table_exists.get(provider_enrichment_summary_table.name):
        payload["hospitals_with_medicare_enrollment"] = await _scalar_int(
            session,
            select(func.count(func.distinct(provider_enrichment_summary_table.c.npi)))
            .select_from(provider_enrichment_summary_table)
            .where(provider_enrichment_summary_table.c.has_hospital_enrollment.is_(True)),
        )
        if table_exists.get(npi_data_table.name):
            payload["medicare_clinicians"] = await _scalar_int(
                session,
                select(func.count())
                .select_from(
                    provider_enrichment_summary_table.join(
                        npi_data_table,
                        npi_data_table.c.npi == provider_enrichment_summary_table.c.npi,
                    )
                )
                .where(
                    provider_enrichment_summary_table.c.has_medicare_claims.is_(True),
                    npi_data_table.c.entity_type_code == 1,
                ),
            )

    if table_exists.get(partd_pharmacy_activity_table.name):
        payload["active_medicare_pharmacies"] = await _scalar_int(
            session,
            select(func.count(func.distinct(partd_pharmacy_activity_table.c.npi)))
            .select_from(partd_pharmacy_activity_table)
            .where(
                partd_pharmacy_activity_table.c.medicare_active.is_(True),
                partd_pharmacy_activity_table.c.effective_from <= func.current_date(),
                (
                    partd_pharmacy_activity_table.c.effective_to.is_(None)
                    | (partd_pharmacy_activity_table.c.effective_to >= func.current_date())
                ),
            ),
        )

    if table_exists.get(plan_drug_stats_table.name):
        payload["formularies_tracked"] = await _scalar_int(
            session,
            select(func.count(func.distinct(plan_drug_stats_table.c.plan_id))).select_from(plan_drug_stats_table),
        )
        payload["drug_coverage_rows"] = await _scalar_int(
            session,
            select(func.coalesce(func.sum(plan_drug_stats_table.c.total_drugs), 0)).select_from(plan_drug_stats_table),
        )

    if table_exists.get(pricing_provider_quality_score_table.name):
        payload["providers_with_quality_scores"] = await _scalar_int(
            session,
            select(func.count(func.distinct(pricing_provider_quality_score_table.c.npi))).select_from(
                pricing_provider_quality_score_table
            ),
        )

    if table_exists.get(pricing_provider_prescription_table.name):
        payload["providers_with_prescription_history"] = await _scalar_int(
            session,
            select(func.count(func.distinct(pricing_provider_prescription_table.c.npi))).select_from(
                pricing_provider_prescription_table
            ),
        )
        payload["prescription_codes_tracked"] = await _scalar_int(
            session,
            select(func.count(func.distinct(pricing_provider_prescription_table.c.rx_code))).select_from(
                pricing_provider_prescription_table
            ),
        )

    return response.json(payload)
