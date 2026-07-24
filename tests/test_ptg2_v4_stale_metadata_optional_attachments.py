# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Contracts for optional V4 reconciliation attachment relations."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from process.ptg_parts import ptg2_v4_stale_metadata_store as store
from process.ptg_parts.ptg2_v4_attempt_registry import ATTEMPT_ATTACHMENTS
from process.ptg_parts.ptg2_v4_stale_metadata_attachments import (
    attachment_count_query,
    optional_attachment_probe,
    present_optional_attachment_names,
)


OPTIONAL_ATTACHMENT_TABLES = {
    "ptg2_price_set_stage",
    "ptg2_serving_rate_stage",
}


class _Result:
    def __init__(self, row) -> None:
        self.row = row

    def first(self):
        return self.row


class _Session:
    def __init__(self, *rows) -> None:
        self.rows = iter(rows)
        self.calls: list[tuple[str, dict]] = []

    async def execute(self, statement, parameters):
        self.calls.append((str(statement), dict(parameters)))
        return _Result(next(self.rows))


class _Database:
    @staticmethod
    def text(statement):
        return statement


def test_row_mapping_branches():
    assert store._row_mapping(None) is None
    mapped_row = SimpleNamespace(_mapping={"attachment": 0})
    assert store._row_mapping(mapped_row) == {"attachment": 0}


def test_only_dynamic_stage_relations_are_optional():
    optional_tables = {
        attachment.table_name
        for attachment in ATTEMPT_ATTACHMENTS
        if attachment.optional_relation
    }

    assert optional_tables == OPTIONAL_ATTACHMENT_TABLES


def test_absent_optional_relations_are_zeros_not_from_clauses():
    query = attachment_count_query(
        "synthetic_ptg",
        stage_table_count=0,
        present_optional_names=frozenset(),
    )

    assert '0::bigint AS "price_set_stage"' in query
    assert '0::bigint AS "serving_rate_stage"' in query
    assert '"ptg2_price_set_stage"' not in query
    assert '"ptg2_serving_rate_stage"' not in query
    assert 'FROM "synthetic_ptg"."ptg2_plan_month"' in query


def test_present_optional_relation_keeps_exact_attachment_check():
    query = attachment_count_query(
        "synthetic_ptg",
        stage_table_count=0,
        present_optional_names=frozenset({"price_set_stage"}),
    )

    assert 'FROM "synthetic_ptg"."ptg2_price_set_stage"' in query
    assert '0::bigint AS "serving_rate_stage"' in query


def test_optional_probe_uses_qualified_catalog_names():
    query, parameter_by_name = optional_attachment_probe("synthetic_ptg")

    assert query.count("to_regclass(") == 2
    assert set(parameter_by_name.values()) == {
        '"synthetic_ptg"."ptg2_price_set_stage"',
        '"synthetic_ptg"."ptg2_serving_rate_stage"',
    }
    assert present_optional_attachment_names(
        {
            "price_set_stage": True,
            "serving_rate_stage": False,
            "unregistered": True,
        }
    ) == frozenset({"price_set_stage"})


@pytest.mark.asyncio
async def test_attachment_counts_probe_before_building_count_query():
    session = _Session(
        {
            "price_set_stage": True,
            "serving_rate_stage": False,
        },
        {
            "price_set_stage": 0,
            "serving_rate_stage": 0,
            "manifest_stage_identity_missing": 0,
        },
    )

    counts = await store._attachment_counts(
        session,
        _Database(),
        schema_name="synthetic_ptg",
        parameter_by_name={
            "snapshot_id": "snapshot",
            "internal_run_id": "run",
            "manifest_stage_identity_missing": 0,
        },
        stage_table_count=0,
    )

    assert counts == {
        "price_set_stage": 0,
        "serving_rate_stage": 0,
        "manifest_stage_identity_missing": 0,
    }
    assert "to_regclass" in session.calls[0][0]
    assert '"ptg2_price_set_stage"' in session.calls[1][0]
    assert '"ptg2_serving_rate_stage"' not in session.calls[1][0]
