# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded PostgreSQL read queries for published FHIR formularies."""

from __future__ import annotations

from typing import Any

from sqlalchemy import text

from api.formulary_fhir_serving_common import current_join
from api.formulary_fhir_serving_common import optional_bool
from api.formulary_fhir_serving_common import row_mapping
from api.formulary_fhir_serving_common import table_name


async def formulary_header(
    session,
    formulary_id: str,
    *,
    source_plan_identifier: str | None = None,
):
    """Return the published CoveragePlan header for one public ID."""

    query_params_by_name: dict[str, Any] = {"formulary_id": formulary_id}
    conditions = ["cp.public_id = :formulary_id"]
    if source_plan_identifier:
        query_params_by_name["source_plan_identifier"] = source_plan_identifier
        conditions.append(
            "a.source_plan_identifier = :source_plan_identifier"
        )
    header_query = (
        "SELECT cp.public_id, cp.source_id, cp.upstream_list_id, cpv.title, "
        "cpv.name, cpv.status, cpv.upstream_version_id, "
        "cpv.upstream_last_updated, cpv.metadata_json, d.dataset_id, "
        "d.cutoff_at, d.published_at, d.coverage_hash, d.membership_hash, "
        "COUNT(DISTINCT m.rxnorm_id) AS drug_count, "
        "array_agg(DISTINCT a.source_plan_identifier "
        "ORDER BY a.source_plan_identifier) AS source_plan_identifiers "
        + current_join()
        + f"LEFT JOIN {table_name('fhir_formulary_alias_membership')} m "
        "ON m.alias_version_id = av.alias_version_id WHERE "
        + " AND ".join(conditions)
        + " GROUP BY cp.public_id, cp.source_id, cp.upstream_list_id, "
        "cpv.title, cpv.name, cpv.status, cpv.upstream_version_id, "
        "cpv.upstream_last_updated, cpv.metadata_json, d.dataset_id, "
        "d.cutoff_at, d.published_at, d.coverage_hash, d.membership_hash"
    )
    return (
        await session.execute(text(header_query), query_params_by_name)
    ).first()


async def available_tiers(
    session,
    formulary_id: str,
    *,
    source_plan_identifier: str | None,
) -> list[str]:
    """Return distinct tier labels for a published formulary or alias."""

    query_params_by_name: dict[str, Any] = {"formulary_id": formulary_id}
    conditions = [
        "cp.public_id = :formulary_id",
        "m.drug_tier IS NOT NULL",
    ]
    if source_plan_identifier:
        query_params_by_name["source_plan_identifier"] = source_plan_identifier
        conditions.append(
            "a.source_plan_identifier = :source_plan_identifier"
        )
    tier_query = (
        "SELECT DISTINCT m.drug_tier "
        + current_join()
        + f"JOIN {table_name('fhir_formulary_alias_membership')} m "
        "ON m.alias_version_id = av.alias_version_id WHERE "
        + " AND ".join(conditions)
        + " ORDER BY m.drug_tier"
    )
    tier_rows = (
        await session.execute(text(tier_query), query_params_by_name)
    ).all()
    return [str(row_mapping(tier_row)["drug_tier"]) for tier_row in tier_rows]


async def variant_rows(
    session,
    formulary_id: str,
    *,
    source_plan_identifier: str | None,
    rxnorm_id: str | None = None,
    rxnorm_ids: list[str] | None = None,
):
    """Return alias-specific coverage variants and alternative evidence."""

    if rxnorm_id is not None and rxnorm_ids is not None:
        raise ValueError("rxnorm_id and rxnorm_ids are mutually exclusive")
    query_params_by_name: dict[str, Any] = {"formulary_id": formulary_id}
    conditions = ["cp.public_id = :formulary_id", "m.rxnorm_id IS NOT NULL"]
    if source_plan_identifier:
        query_params_by_name["source_plan_identifier"] = source_plan_identifier
        conditions.append("a.source_plan_identifier = :source_plan_identifier")
    if rxnorm_id:
        query_params_by_name["rxnorm_id"] = rxnorm_id
        conditions.append("m.rxnorm_id = :rxnorm_id")
    if rxnorm_ids is not None:
        if not rxnorm_ids:
            return []
        query_params_by_name["rxnorm_ids"] = rxnorm_ids
        conditions.append("m.rxnorm_id = ANY(CAST(:rxnorm_ids AS text[]))")
    variant_query = (
        "SELECT m.rxnorm_id, med.drug_name, med.upstream_medication_id, "
        "med.upstream_version_id, med.upstream_last_updated, med.codings_json, "
        "a.source_plan_identifier, m.drug_tier, m.prior_authorization, "
        "m.step_therapy, m.quantity_limit, COALESCE((SELECT jsonb_agg("
        "jsonb_build_object('raw_reference', alt.raw_reference, "
        "'corrected_reference', alt.corrected_reference, "
        "'resolved_medication_id', alt.resolved_medication_id, "
        "'resolved', alt.resolved, 'rule_version', alt.rule_version, "
        "'evidence', alt.evidence_json) ORDER BY alt.raw_reference) FROM "
        f"{table_name('fhir_formulary_alternative')} alt "
        "WHERE alt.alias_version_id = m.alias_version_id AND "
        "alt.upstream_medication_id = m.upstream_medication_id), "
        "'[]'::jsonb) AS alternatives_json "
        + current_join()
        + f"JOIN {table_name('fhir_formulary_alias_membership')} m "
        "ON m.alias_version_id = av.alias_version_id "
        f"JOIN {table_name('fhir_formulary_medication')} med "
        "ON med.medication_version_id = m.medication_version_id WHERE "
        + " AND ".join(conditions)
        + " ORDER BY m.rxnorm_id, a.source_plan_identifier, "
        "med.upstream_medication_id"
    )
    database_rows = (
        await session.execute(text(variant_query), query_params_by_name)
    ).all()
    return [row_mapping(database_row) for database_row in database_rows]


def _drug_page_query_parts(
    formulary_id: str,
    source_plan_identifier: str | None,
    args,
    *,
    limit: int,
    offset: int,
) -> tuple[dict[str, Any], list[str], list[str]]:
    query_params_by_name: dict[str, Any] = {
        "formulary_id": formulary_id,
        "limit": limit,
        "offset": offset,
    }
    conditions = ["cp.public_id = :formulary_id", "m.rxnorm_id IS NOT NULL"]
    if source_plan_identifier:
        query_params_by_name["source_plan_identifier"] = source_plan_identifier
        conditions.append("a.source_plan_identifier = :source_plan_identifier")
    having_conditions: list[str] = []
    tier = str(args.get("tier") or "").strip()
    if tier:
        query_params_by_name["tier"] = tier
        having_conditions.append("BOOL_OR(m.drug_tier = :tier)")
    boolean_filters = (
        ("authorization_required", "prior_authorization"),
        ("step_therapy", "step_therapy"),
        ("quantity_limit", "quantity_limit"),
    )
    for parameter_name, column_name in boolean_filters:
        expected_value = optional_bool(args.get(parameter_name), parameter_name)
        if expected_value is None:
            continue
        bind_name = f"expected_{column_name}"
        query_params_by_name[bind_name] = expected_value
        having_conditions.append(f"BOOL_OR(m.{column_name} = :{bind_name})")
    return query_params_by_name, conditions, having_conditions


def _grouped_drug_query(
    conditions: list[str],
    having_conditions: list[str],
) -> str:
    having_clause = (
        " HAVING " + " AND ".join(having_conditions)
        if having_conditions
        else ""
    )
    return (
        "SELECT m.rxnorm_id, MIN(med.drug_name) AS sort_name, "
        "CASE WHEN COUNT(DISTINCT ROW(m.drug_tier IS NULL, m.drug_tier)) = 1 "
        "THEN MIN(m.drug_tier) ELSE NULL END AS sort_tier "
        + current_join()
        + f"JOIN {table_name('fhir_formulary_alias_membership')} m "
        "ON m.alias_version_id = av.alias_version_id "
        f"JOIN {table_name('fhir_formulary_medication')} med "
        "ON med.medication_version_id = m.medication_version_id WHERE "
        + " AND ".join(conditions)
        + " GROUP BY m.rxnorm_id"
        + having_clause
    )


async def paged_rxnorm_ids(
    session,
    formulary_id: str,
    *,
    source_plan_identifier: str | None,
    args,
    limit: int,
    offset: int,
    sort_field: str,
    order: str,
) -> tuple[list[str], int]:
    """Page grouped drugs in SQL before loading alias-level variants."""

    query_params_by_name, conditions, having_conditions = (
        _drug_page_query_parts(
            formulary_id,
            source_plan_identifier,
            args,
            limit=limit,
            offset=offset,
        )
    )
    grouped_query = _grouped_drug_query(conditions, having_conditions)
    total = int(
        (
            await session.execute(
                text(f"SELECT COUNT(*) FROM ({grouped_query}) grouped_drugs"),
                query_params_by_name,
            )
        ).scalar()
        or 0
    )
    sort_column = "sort_name" if sort_field == "name" else "sort_tier"
    direction = "DESC" if order == "desc" else "ASC"
    page_query = (
        f"SELECT rxnorm_id FROM ({grouped_query}) grouped_drugs "
        f"ORDER BY COALESCE({sort_column}, '') {direction}, "
        f"rxnorm_id {direction} LIMIT :limit OFFSET :offset"
    )
    page_rows = (
        await session.execute(text(page_query), query_params_by_name)
    ).all()
    return [
        str(row_mapping(page_row)["rxnorm_id"]) for page_row in page_rows
    ], total


def _consensus_query(conditions: list[str]) -> str:
    return (
        "SELECT m.rxnorm_id, "
        "CASE WHEN COUNT(DISTINCT ROW(m.drug_tier IS NULL, m.drug_tier)) = 1 "
        "THEN MIN(m.drug_tier) ELSE NULL END AS drug_tier, "
        "CASE WHEN COUNT(DISTINCT ROW(m.prior_authorization IS NULL, "
        "m.prior_authorization)) = 1 THEN BOOL_OR(m.prior_authorization) "
        "ELSE NULL END AS prior_authorization, "
        "CASE WHEN COUNT(DISTINCT ROW(m.step_therapy IS NULL, "
        "m.step_therapy)) = 1 THEN BOOL_OR(m.step_therapy) "
        "ELSE NULL END AS step_therapy, "
        "CASE WHEN COUNT(DISTINCT ROW(m.quantity_limit IS NULL, "
        "m.quantity_limit)) = 1 THEN BOOL_OR(m.quantity_limit) "
        "ELSE NULL END AS quantity_limit "
        + current_join()
        + f"JOIN {table_name('fhir_formulary_alias_membership')} m "
        "ON m.alias_version_id = av.alias_version_id WHERE "
        + " AND ".join(conditions)
        + " GROUP BY m.rxnorm_id"
    )


def _summary_query(consensus_query: str) -> str:
    return (
        f"WITH drug_consensus AS ({consensus_query}) "
        "SELECT COUNT(*) AS total_drugs, "
        "COUNT(*) FILTER (WHERE prior_authorization IS TRUE) AS prior_true, "
        "COUNT(*) FILTER (WHERE prior_authorization IS FALSE) AS prior_false, "
        "COUNT(*) FILTER (WHERE prior_authorization IS NULL) AS prior_unknown, "
        "COUNT(*) FILTER (WHERE step_therapy IS TRUE) AS step_true, "
        "COUNT(*) FILTER (WHERE step_therapy IS FALSE) AS step_false, "
        "COUNT(*) FILTER (WHERE step_therapy IS NULL) AS step_unknown, "
        "COUNT(*) FILTER (WHERE quantity_limit IS TRUE) AS quantity_true, "
        "COUNT(*) FILTER (WHERE quantity_limit IS FALSE) AS quantity_false, "
        "COUNT(*) FILTER (WHERE quantity_limit IS NULL) AS quantity_unknown "
        "FROM drug_consensus"
    )


def _tier_query(consensus_query: str) -> str:
    return (
        f"WITH drug_consensus AS ({consensus_query}) "
        "SELECT COALESCE(drug_tier, 'CONFLICTING_OR_UNKNOWN') AS tier_label, "
        "COUNT(*) AS drug_count FROM drug_consensus "
        "GROUP BY COALESCE(drug_tier, 'CONFLICTING_OR_UNKNOWN') "
        "ORDER BY tier_label"
    )


async def summary_statistics(
    session,
    formulary_id: str,
    *,
    source_plan_identifier: str | None,
) -> tuple[dict[str, int], list[tuple[str, int]]]:
    """Aggregate alias consensus in PostgreSQL without loading every drug."""

    query_params_by_name: dict[str, Any] = {"formulary_id": formulary_id}
    conditions = ["cp.public_id = :formulary_id", "m.rxnorm_id IS NOT NULL"]
    if source_plan_identifier:
        query_params_by_name["source_plan_identifier"] = source_plan_identifier
        conditions.append("a.source_plan_identifier = :source_plan_identifier")
    consensus_query = _consensus_query(conditions)
    summary_row = (
        await session.execute(
            text(_summary_query(consensus_query)),
            query_params_by_name,
        )
    ).first()
    summary_by_name = {
        statistic_name: int(statistic_value or 0)
        for statistic_name, statistic_value in row_mapping(summary_row).items()
    }
    tier_rows = (
        await session.execute(
            text(_tier_query(consensus_query)),
            query_params_by_name,
        )
    ).all()
    tier_counts = [
        (
            str(row_mapping(tier_row)["tier_label"]),
            int(row_mapping(tier_row)["drug_count"]),
        )
        for tier_row in tier_rows
    ]
    return summary_by_name, tier_counts
