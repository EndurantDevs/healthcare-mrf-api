# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Edge contracts for deterministic plan-endpoint normalization helpers."""

from __future__ import annotations

import types

import pytest
from asyncpg import UndefinedTableError
from sanic.exceptions import InvalidUsage
from sqlalchemy import column, table
from sqlalchemy.exc import ProgrammingError

from api.endpoint import plan


class FakeResult:
    def __init__(self, rows=()):
        self._rows = list(rows)

    def all(self):
        return self._rows


class FakeSession:
    def __init__(self, outcomes):
        self._outcomes = list(outcomes)

    async def execute(self, _statement):
        outcome = self._outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


class BrokenMapping:
    def __iter__(self):
        raise TypeError("mapping is unavailable")


class BrokenDict(dict):
    _mapping = BrokenMapping()

    def keys(self):
        raise ValueError("keys are unavailable")


class BrokenRecord:
    def keys(self):
        return ["value"]

    def __getitem__(self, _key):
        raise TypeError("lookup is unavailable")

    def __iter__(self):
        raise TypeError("record is not iterable")


def test_plan_scalar_and_row_helpers_fail_closed_on_unusual_adapters():
    assert plan._parse_bool(False, "enabled") is False
    assert plan._parse_bool("n", "enabled") is False
    with pytest.raises(InvalidUsage, match="boolean-like"):
        plan._parse_bool("sometimes", "enabled")

    broken_dict = BrokenDict(value="preserved")
    assert plan._row_to_dict(broken_dict) == {"value": "preserved"}
    assert plan._row_to_dict(BrokenRecord()) == {}


def test_plan_collection_helpers_normalize_mixed_inputs():
    plan_identity = table("plan_identity", column("plan_id"), column("full_plan_id"))
    plan_id_expression = plan._plan_level_plan_id_column(plan_identity)
    assert plan_id_expression is not None

    bounds = plan._collect_price_bounds(
        FakeResult(
            [
                {"plan_id": None, "year": 2026},
                {"plan_id": "ignored", "year": None},
                {"plan_id": "bad-rate", "year": 2026, "individual_rate": "x"},
                {"plan_id": "good", "year": 2026, "individual_rate": "12.5"},
                {"plan_id": "good", "year": 2026, "couple": 30},
            ]
        )
    )
    assert bounds == {("good", 2026): {"min": 12.5, "max": 30.0}}

    label_name = next(iter(plan.attributes_labels))
    normalized = plan._normalize_attribute_map({label_name: {}})
    assert normalized[label_name] == {
        "attr_value": None,
        "human_attr_name": plan.attributes_labels[label_name],
    }


def test_plan_list_and_summary_helpers_reject_malformed_entries():
    args = types.SimpleNamespace(
        getlist=lambda _name: [None, [" alpha ", ""], " beta, ,gamma"]
    )
    assert plan._get_list_param(args, "plan_type") == ["alpha", "beta", "gamma"]
    assert plan._get_list_param({"plan_type": ("HMO", "PPO")}, "plan_type") == [
        "HMO",
        "PPO",
    ]

    assert plan._summary_attributes_to_dict(None) == {}
    assert plan._summary_attributes_to_dict("not-json") == {}
    assert plan._summary_attributes_to_dict({"attr_name": "ignored"}) == {}
    assert plan._summary_attributes_to_dict([None, {}, {"attr_name": "A"}]) == {
        "A": {"attr_value": None}
    }

    assert plan._summary_benefits_to_dict(None) == {}
    assert plan._summary_benefits_to_dict("not-json") == {}
    assert plan._summary_benefits_to_dict({"benefit_name": "ignored"}) == {}
    assert plan._summary_benefits_to_dict([None, {}, {"benefit_name": "B"}]) == {
        "B": {"benefit_name": "B"}
    }


@pytest.mark.asyncio
async def test_plan_facet_helpers_ignore_empty_group_values():
    session = FakeSession(
        [
            FakeResult([{"value": None, "count": 7}, {"value": "HMO", "count": 2}]),
            FakeResult(),
            FakeResult(),
            FakeResult(),
        ]
    )

    facets = await plan._compute_facets(session, plan.plan_table)

    assert facets["plan_types"] == [{"value": "HMO", "count": 2}]
    assert facets["metal_levels"] == []
    assert facets["boolean_filters"] == plan._default_boolean_facets()


@pytest.mark.asyncio
async def test_plan_state_lookup_uses_bounded_fallbacks():
    assert await plan._states_for_zip(FakeSession([]), "12") == []
    assert await plan._states_for_zip(
        FakeSession([FakeResult([("TX",), ("",)])]),
        "75001",
    ) == ["TX"]

    missing_geo_table = ProgrammingError(
        "select geo",
        {},
        UndefinedTableError("missing geo table"),
    )
    assert await plan._states_for_zip(
        FakeSession([missing_geo_table, FakeResult([("NM",)])]),
        "87101",
    ) == ["NM"]
    assert await plan._states_for_zip(
        FakeSession([FakeResult(), FakeResult()]),
        "1234",
    ) == []


@pytest.mark.asyncio
async def test_plan_state_lookup_reraises_non_schema_database_errors():
    database_error = ProgrammingError("select geo", {}, RuntimeError("offline"))

    with pytest.raises(ProgrammingError) as raised:
        await plan._states_for_zip(FakeSession([database_error]), "75001")

    assert raised.value is database_error
