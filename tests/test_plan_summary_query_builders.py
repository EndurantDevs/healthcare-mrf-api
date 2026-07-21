# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Executable coverage for the plan-summary query construction contract."""

from __future__ import annotations

import pytest
from sqlalchemy.dialects import postgresql

import process.plan_summary as plan_summary


def _compiled_sql(statement) -> str:
    return str(
        statement.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )


def test_keyword_groups_and_conditions_cover_empty_single_and_alternative_terms():
    assert plan_summary._normalize_keyword_groups(()) == []
    assert plan_summary._normalize_keyword_groups((" Dental ", "", 1)) == [
        ("dental",)
    ]
    assert plan_summary._normalize_keyword_groups(
        ((" Vision ", "Adult"), ("eye", "adult"), ())
    ) == [("vision", "adult"), ("eye", "adult")]

    benefit_name = plan_summary.plan_benefits_table.c.benefit_name
    assert plan_summary._keyword_condition(benefit_name, ()) is None
    single_condition = plan_summary._keyword_condition(
        benefit_name,
        ("dental", "adult"),
    )
    alternative_condition = plan_summary._keyword_condition(
        benefit_name,
        (("vision", "adult"), ("eye", "adult")),
    )
    assert "LIKE" in _compiled_sql(single_condition)
    assert " OR " in _compiled_sql(alternative_condition)


def test_summary_expression_builders_compile_complete_postgresql_queries():
    expression_list = (
        plan_summary._plan_level_plan_id_column(plan_summary.plan_attributes_table),
        plan_summary._bool_or(plan_summary.plan_table.c.plan_id.isnot(None)),
        plan_summary._attribute_boolean_case("IsHSAEligible"),
        plan_summary._attribute_boolean_case("CustomFlag", ("enabled",)),
        plan_summary._attribute_numeric_case("MEHBDedInnTier1Individual"),
        plan_summary._attribute_exchange_case(()),
        plan_summary._attribute_exchange_case(plan_summary.EXCHANGE_ON_KEYWORDS),
        plan_summary._benefit_flag_case(()),
        plan_summary._benefit_flag_case(plan_summary.TELEHEALTH_KEYWORDS),
        plan_summary._attribute_payload_expr(),
        plan_summary._benefit_payload_expr(),
        plan_summary._attribute_text("PlanType"),
        plan_summary._attribute_text("PlanType", lower=True),
        plan_summary._attribute_text("PlanType", upper=True),
        plan_summary._attribute_text("PlanType", lower=True, upper=True),
    )
    compiled_expressions = "\n".join(
        _compiled_sql(expression) for expression in expression_list
    )
    assert "plan_id" in compiled_expressions
    assert "MarketCoverage" in compiled_expressions
    assert "jsonb_agg" in compiled_expressions

    attribute_sql = _compiled_sql(
        plan_summary._build_attribute_summary_subquery().select()
    )
    benefit_sql = _compiled_sql(
        plan_summary._build_benefit_summary_subquery().select()
    )
    price_sql = _compiled_sql(plan_summary._build_price_summary_subquery().select())
    assert "plan_attributes" in attribute_sql and "GROUP BY" in attribute_sql
    assert "plan_benefits" in benefit_sql and "telehealth_supported" in benefit_sql
    assert "plan_prices" in price_sql and "premium_min" in price_sql


@pytest.mark.asyncio
async def test_summary_schema_helpers_emit_bounded_idempotent_ddl(monkeypatch):
    emitted_statements: list[str] = []

    async def record_status(statement: str) -> None:
        emitted_statements.append(statement)

    index_specs = [
        {},
        {"index_elements": []},
        {"index_elements": ["year"]},
        {
            "name": "plan_search_summary_state_partial_idx",
            "index_elements": ["state", "year"],
            "using": "btree",
            "where": "state IS NOT NULL",
        },
    ]
    monkeypatch.setattr(
        plan_summary.PlanSearchSummary,
        "__my_additional_indexes__",
        index_specs,
        raising=False,
    )
    monkeypatch.setattr(plan_summary.db, "status", record_status)

    await plan_summary._ensure_summary_indexes()
    await plan_summary._ensure_summary_columns()

    assert len(emitted_statements) == 7
    assert any("plan_search_summary_year_idx" in sql for sql in emitted_statements)
    assert any("USING btree" in sql and "WHERE state IS NOT NULL" in sql for sql in emitted_statements)
    assert sum("ADD COLUMN IF NOT EXISTS" in sql for sql in emitted_statements) == 5
