# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import logging
from typing import Any, Sequence

from sqlalchemy import (Float, MetaData, and_, case, cast, func, or_, select,
                        text)
from sqlalchemy.dialects.postgresql import insert as pg_insert

from db.connection import db
from db.models import (Plan, PlanAttributes, PlanBenefits, PlanPrices,
                       PlanSearchSummary)
from process.ext.utils import ensure_database

logger = logging.getLogger(__name__)

plan_table = Plan.__table__
plan_attributes_table = PlanAttributes.__table__
plan_benefits_table = PlanBenefits.__table__
plan_prices_table = PlanPrices.__table__
summary_table = PlanSearchSummary.__table__

BOOLEAN_TRUE_VALUES = ("yes", "true", "1")
PRICE_RATE_COLUMNS = (
    plan_prices_table.c.individual_rate,
    plan_prices_table.c.individual_tobacco_rate,
    plan_prices_table.c.couple,
    plan_prices_table.c.primary_subscriber_and_one_dependent,
    plan_prices_table.c.primary_subscriber_and_two_dependents,
    plan_prices_table.c.primary_subscriber_and_three_or_more_dependents,
    plan_prices_table.c.couple_and_one_dependent,
    plan_prices_table.c.couple_and_two_dependents,
    plan_prices_table.c.couple_and_three_or_more_dependents,
)
MIN_SENTINEL = 10**12
MAX_SENTINEL = -10**12
ADULT_DENTAL_KEYWORDS = ("dental", "adult")
CHILD_DENTAL_KEYWORDS = ("dental", "child")
ADULT_VISION_KEYWORDS = (("vision", "adult"), ("eye", "adult"))
CHILD_VISION_KEYWORDS = (("vision", "child"), ("eye", "child"))
TELEHEALTH_KEYWORDS = (
    ("telehealth",),
    ("telemedicine",),
    ("virtual", "visit"),
    ("virtual", "care"),
)
EXCHANGE_ON_KEYWORDS = ("on exchange", "on/off exchange", "on and off", "on & off", "both")
EXCHANGE_OFF_KEYWORDS = ("off exchange", "on/off exchange", "on and off", "on & off", "both")


def _plan_level_plan_id_column(table) -> Any:
    return func.coalesce(table.c.plan_id, func.left(func.coalesce(table.c.full_plan_id, ""), 14))


def _normalize_keyword_groups(keywords: Sequence[Sequence[str]] | Sequence[str]):
    if not keywords:
        return []
    first = keywords[0] if isinstance(keywords, Sequence) else keywords
    if isinstance(first, (tuple, list)):
        groups = keywords  # type: ignore[assignment]
    else:
        groups = (keywords,)  # type: ignore[assignment]
    normalized = []
    for group in groups:  # type: ignore[arg-type]
        values = tuple(word.strip().lower() for word in group if isinstance(word, str) and word.strip())
        if values:
            normalized.append(values)
    return normalized


def _keyword_condition(column_expr, keywords: Sequence[Sequence[str]] | Sequence[str]):
    groups = _normalize_keyword_groups(keywords)
    if not groups:
        return None
    comparisons = []
    for group in groups:
        terms = [column_expr.like(f"%{token}%") for token in group]
        if terms:
            comparisons.append(and_(*terms))
    if not comparisons:
        return None
    if len(comparisons) == 1:
        return comparisons[0]
    return or_(*comparisons)


def _bool_or(condition):
    return func.coalesce(
        func.bool_or(
            case(
                (condition, True),
                else_=None,
            )
        ),
        False,
    )


def _attribute_boolean_case(attr_name: str, truthy_values: Sequence[str] | None = None):
    truthy_values = tuple(truthy_values or BOOLEAN_TRUE_VALUES)
    raw_value = func.lower(func.trim(func.coalesce(plan_attributes_table.c.attr_value, "")))
    condition = and_(plan_attributes_table.c.attr_name == attr_name, raw_value.in_(truthy_values))
    return _bool_or(condition)


def _attribute_numeric_case(attr_name: str):
    raw_value = func.coalesce(plan_attributes_table.c.attr_value, "")
    numeric_value = cast(
        func.nullif(func.regexp_replace(raw_value, "[^0-9.]", "", "g"), ""),
        Float,
    )
    return func.max(case((plan_attributes_table.c.attr_name == attr_name, numeric_value), else_=None))


def _attribute_exchange_case(keywords: Sequence[Sequence[str]] | Sequence[str]):
    lowered = func.lower(func.trim(func.coalesce(plan_attributes_table.c.attr_value, "")))
    condition = _keyword_condition(lowered, keywords)
    if condition is None:
        return func.false()
    match = and_(plan_attributes_table.c.attr_name == "MarketCoverage", condition)
    return _bool_or(match)


def _benefit_flag_case(keywords: Sequence[Sequence[str]] | Sequence[str]):
    lowered = func.lower(func.coalesce(plan_benefits_table.c.benefit_name, ""))
    condition = _keyword_condition(lowered, keywords)
    if condition is None:
        return func.false()
    covered = and_(plan_benefits_table.c.is_covered.is_(True), condition)
    return _bool_or(covered)


def _attribute_payload_expr():
    return func.jsonb_agg(
        func.jsonb_build_object(
            "attr_name", plan_attributes_table.c.attr_name,
            "attr_value", plan_attributes_table.c.attr_value,
            "full_plan_id", plan_attributes_table.c.full_plan_id,
        )
    ).filter(plan_attributes_table.c.attr_name.isnot(None))


def _benefit_payload_expr():
    return func.jsonb_agg(
        func.jsonb_build_object(
            "plan_id", plan_benefits_table.c.plan_id,
            "full_plan_id", plan_benefits_table.c.full_plan_id,
            "benefit_name", plan_benefits_table.c.benefit_name,
            "copay_inn_tier1", plan_benefits_table.c.copay_inn_tier1,
            "copay_inn_tier2", plan_benefits_table.c.copay_inn_tier2,
            "copay_outof_net", plan_benefits_table.c.copay_outof_net,
            "coins_inn_tier1", plan_benefits_table.c.coins_inn_tier1,
            "coins_inn_tier2", plan_benefits_table.c.coins_inn_tier2,
            "coins_outof_net", plan_benefits_table.c.coins_outof_net,
            "is_ehb", plan_benefits_table.c.is_ehb,
            "is_covered", plan_benefits_table.c.is_covered,
            "quant_limit_on_svc", plan_benefits_table.c.quant_limit_on_svc,
            "limit_qty", plan_benefits_table.c.limit_qty,
            "limit_unit", plan_benefits_table.c.limit_unit,
            "exclusions", plan_benefits_table.c.exclusions,
            "explanation", plan_benefits_table.c.explanation,
            "ehb_var_reason", plan_benefits_table.c.ehb_var_reason,
            "is_excl_from_inn_mo", plan_benefits_table.c.is_excl_from_inn_mo,
            "is_excl_from_oon_mo", plan_benefits_table.c.is_excl_from_oon_mo,
        )
    ).filter(plan_benefits_table.c.benefit_name.isnot(None))


def _attribute_text(attr_name: str, lower=False, upper=False):
    value = func.trim(func.coalesce(plan_attributes_table.c.attr_value, ""))
    if lower:
        value = func.lower(value)
    if upper:
        value = func.upper(value)
    return func.max(
        case(
            (plan_attributes_table.c.attr_name == attr_name, value),
            else_=None,
        )
    )


def _build_attribute_summary_subquery():
    plan_level_id = _plan_level_plan_id_column(plan_attributes_table)
    metal_text = func.lower(func.trim(func.coalesce(plan_attributes_table.c.attr_value, "")))
    market_text = func.trim(func.coalesce(plan_attributes_table.c.attr_value, ""))
    return (
        select(
            plan_level_id.label("plan_id"),
            plan_attributes_table.c.year.label("year"),
            _attribute_payload_expr().label("attributes"),
            func.max(
                case(
                    (plan_attributes_table.c.attr_name == "MarketCoverage", market_text),
                    else_=None,
                )
            ).label("market_coverage"),
            func.coalesce(_attribute_exchange_case(EXCHANGE_ON_KEYWORDS), False).label("is_on_exchange"),
            func.coalesce(_attribute_exchange_case(EXCHANGE_OFF_KEYWORDS), False).label("is_off_exchange"),
            _attribute_boolean_case("IsHSAEligible").label("is_hsa"),
            _attribute_boolean_case("DentalOnlyPlan").label("is_dental_only"),
            _bool_or(and_(plan_attributes_table.c.attr_name == "MetalLevel", metal_text.like("%catastrophic%"))).label(
                "is_catastrophic"
            ),
            _attribute_numeric_case("MEHBDedInnTier1Individual").label("deductible_inn_individual"),
            _attribute_numeric_case("MEHBInnTier1IndividualMOOP").label("moop_inn_individual"),
            _attribute_text("PlanType", upper=True).label("plan_type"),
            _attribute_text("MetalLevel").label("metal_level"),
            _attribute_text("CSRVariationType").label("csr_variation"),
        )
        .where(plan_level_id.isnot(None))
        .group_by(plan_level_id, plan_attributes_table.c.year)
        .subquery()
    )


def _build_benefit_summary_subquery():
    plan_level_id = _plan_level_plan_id_column(plan_benefits_table)
    return (
        select(
            plan_level_id.label("plan_id"),
            plan_benefits_table.c.year.label("year"),
            _benefit_payload_expr().label("plan_benefits"),
            _benefit_flag_case(ADULT_DENTAL_KEYWORDS).label("has_adult_dental"),
            _benefit_flag_case(CHILD_DENTAL_KEYWORDS).label("has_child_dental"),
            _benefit_flag_case(ADULT_VISION_KEYWORDS).label("has_adult_vision"),
            _benefit_flag_case(CHILD_VISION_KEYWORDS).label("has_child_vision"),
            _benefit_flag_case(TELEHEALTH_KEYWORDS).label("telehealth_supported"),
        )
        .where(plan_level_id.isnot(None))
        .group_by(plan_level_id, plan_benefits_table.c.year)
        .subquery()
    )


def _build_price_summary_subquery():
    min_candidates = [func.coalesce(column, MIN_SENTINEL) for column in PRICE_RATE_COLUMNS]
    max_candidates = [func.coalesce(column, MAX_SENTINEL) for column in PRICE_RATE_COLUMNS]
    row_min = func.nullif(func.least(*min_candidates), MIN_SENTINEL)
    row_max = func.nullif(func.greatest(*max_candidates), MAX_SENTINEL)
    return (
        select(
            plan_prices_table.c.plan_id.label("plan_id"),
            plan_prices_table.c.year.label("year"),
            func.min(row_min).label("premium_min"),
            func.max(row_max).label("premium_max"),
        )
        .where(plan_prices_table.c.plan_id.isnot(None))
        .group_by(plan_prices_table.c.plan_id, plan_prices_table.c.year)
        .subquery()
    )


async def _ensure_summary_indexes() -> None:
    schema = summary_table.schema
    schema_prefix = f'"{schema}".' if schema else ""
    qualified_table = f'{schema_prefix}"{summary_table.name}"'
    for spec in getattr(PlanSearchSummary, "__my_additional_indexes__", []) or []:
        columns = spec.get("index_elements")
        if not columns:
            continue
        name = spec.get("name") or f"{summary_table.name}_{'_'.join(columns)}_idx"
        using_clause = f" USING {spec['using']}" if spec.get("using") else ""
        where_clause = f" WHERE {spec['where']}" if spec.get("where") else ""
        column_clause = ", ".join(columns)
        stmt = (
            f"CREATE INDEX IF NOT EXISTS {name} "
            f"ON {qualified_table}{using_clause} ({column_clause}){where_clause};"
        )
        await db.status(stmt)


async def _ensure_summary_columns() -> None:
    schema = summary_table.schema or "public"
    qualifiers = f'"{schema}"."{summary_table.name}"'
    statements = [
        f"ALTER TABLE {qualifiers} ADD COLUMN IF NOT EXISTS premium_min DOUBLE PRECISION",
        f"ALTER TABLE {qualifiers} ADD COLUMN IF NOT EXISTS premium_max DOUBLE PRECISION",
        f"ALTER TABLE {qualifiers} ADD COLUMN IF NOT EXISTS plan_type VARCHAR",
        f"ALTER TABLE {qualifiers} ADD COLUMN IF NOT EXISTS metal_level VARCHAR",
        f"ALTER TABLE {qualifiers} ADD COLUMN IF NOT EXISTS csr_variation VARCHAR",
    ]
    for stmt in statements:
        await db.status(stmt)


async def rebuild_plan_search_summary(test_mode: bool = False) -> int:
    """
    Materialize plan_search_summary with pre-calculated filters so /plan/search can run without large joins.
    """
    await ensure_database(test_mode)
    await db.create_table(summary_table, checkfirst=True)
    await _ensure_summary_columns()
    await _ensure_summary_indexes()

    attr_subq = _build_attribute_summary_subquery()
    benefit_subq = _build_benefit_summary_subquery()
    price_subq = _build_price_summary_subquery()
    schema = summary_table.schema or "public"
    base_name = summary_table.name
    temp_name = f"{base_name}_refresh"
    temp_metadata = MetaData()
    temp_table = summary_table.tometadata(temp_metadata, name=temp_name)
    temp_table.schema = schema

    qualified_temp = f'"{schema}"."{temp_name}"'
    qualified_base = f'"{schema}"."{base_name}"'
    qualified_backup = f'"{schema}"."{base_name}_old"'

    async with db.session() as session:
        await session.execute(text(f'DROP TABLE IF EXISTS {qualified_temp};'))

    await db.create_table(temp_table, checkfirst=False)

    join_attr = and_(
        plan_table.c.plan_id == attr_subq.c.plan_id,
        plan_table.c.year == attr_subq.c.year,
    )
    join_benefits = and_(
        plan_table.c.plan_id == benefit_subq.c.plan_id,
        plan_table.c.year == benefit_subq.c.year,
    )
    join_prices = and_(
        plan_table.c.plan_id == price_subq.c.plan_id,
        plan_table.c.year == price_subq.c.year,
    )

    data_stmt = (
        select(
            plan_table.c.plan_id,
            plan_table.c.year,
            plan_table.c.state,
            plan_table.c.issuer_id,
            plan_table.c.marketing_name,
            attr_subq.c.market_coverage,
            func.coalesce(attr_subq.c.is_on_exchange, False).label("is_on_exchange"),
            func.coalesce(attr_subq.c.is_off_exchange, False).label("is_off_exchange"),
            func.coalesce(attr_subq.c.is_hsa, False).label("is_hsa"),
            func.coalesce(attr_subq.c.is_dental_only, False).label("is_dental_only"),
            func.coalesce(attr_subq.c.is_catastrophic, False).label("is_catastrophic"),
            attr_subq.c.deductible_inn_individual,
            attr_subq.c.moop_inn_individual,
            price_subq.c.premium_min,
            price_subq.c.premium_max,
            attr_subq.c.plan_type,
            attr_subq.c.metal_level,
            attr_subq.c.csr_variation,
            func.coalesce(benefit_subq.c.has_adult_dental, False).label("has_adult_dental"),
            func.coalesce(benefit_subq.c.has_child_dental, False).label("has_child_dental"),
            func.coalesce(benefit_subq.c.has_adult_vision, False).label("has_adult_vision"),
            func.coalesce(benefit_subq.c.has_child_vision, False).label("has_child_vision"),
            func.coalesce(benefit_subq.c.telehealth_supported, False).label("telehealth_supported"),
            attr_subq.c.attributes,
            benefit_subq.c.plan_benefits,
            func.now().label("updated_at"),
        )
        .select_from(
            plan_table.outerjoin(attr_subq, join_attr)
            .outerjoin(benefit_subq, join_benefits)
            .outerjoin(price_subq, join_prices)
        )
    )

    insert_columns = [
        "plan_id",
        "year",
        "state",
        "issuer_id",
        "marketing_name",
        "market_coverage",
        "is_on_exchange",
        "is_off_exchange",
        "is_hsa",
        "is_dental_only",
        "is_catastrophic",
        "deductible_inn_individual",
        "moop_inn_individual",
        "premium_min",
        "premium_max",
        "plan_type",
        "metal_level",
        "csr_variation",
        "has_adult_dental",
        "has_child_dental",
        "has_adult_vision",
        "has_child_vision",
        "telehealth_supported",
        "attributes",
        "plan_benefits",
        "updated_at",
    ]

    insert_stmt = pg_insert(temp_table).from_select(insert_columns, data_stmt)

    async with db.session() as session:
        await session.execute(insert_stmt)
        count_result = await session.execute(select(func.count()).select_from(temp_table))
        rowcount = count_result.scalar() or 0

    async with db.session() as session:
        await session.execute(text(f'DROP TABLE IF EXISTS {qualified_backup};'))
        await session.execute(text(f'ALTER TABLE IF EXISTS {qualified_base} RENAME TO "{base_name}_old";'))
        await session.execute(text(f'ALTER TABLE {qualified_temp} RENAME TO "{base_name}"'))
        await session.execute(text(f'DROP TABLE IF EXISTS {qualified_backup};'))

    await _ensure_summary_indexes()

    logger.info("Rebuilt plan_search_summary with %s rows", rowcount)
    return int(rowcount)
