# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Contracts for the release-bound E&M distance-card projection."""

from decimal import Decimal
from types import SimpleNamespace

import pytest

from api import ptg2_serving
from api.plan_pricing_em_distance import (
    EM_CODES,
    PROJECTION_CONTRACT,
    _PAGE_SQL,
    _card_item,
    _request_code_index,
    em_distance_retry_option,
    is_em_distance_projection_ready,
    search_plan_pricing_em_distance,
)
from api.plan_release_serving import PlanReleaseServingSelection


@pytest.mark.parametrize("code_index,code", enumerate(EM_CODES))
@pytest.mark.parametrize("order", (None, "", "null", "asc"))
def test_distance_card_request_is_closed_and_indexed(code_index, code, order):
    args_by_name = {
        "code": code,
        "code_system": "CPT",
        "view": "card",
        "include_providers": "true",
        "order_by": "distance",
        "order": order,
        "zip5": "60611",
        "zip_radius_miles": "25",
    }

    assert _request_code_index(args_by_name) == code_index


@pytest.mark.parametrize(
    "override",
    [
        {"view": "full"},
        {"include_providers": "false"},
        {"order_by": "cost"},
        {"order": "desc"},
        {"classification": "Internal Medicine"},
        {"include_allowed_amounts": "true"},
        {"zip_radius_miles": "26"},
        {"offset": "191", "limit": "10"},
    ],
)
def test_distance_card_request_rejects_unprojected_shapes(override):
    args_by_name = {
        "code": "99213",
        "code_system": "CPT",
        "view": "card",
        "include_providers": "true",
        "order_by": "distance",
        "order": "asc",
        "zip5": "60611",
        "zip_radius_miles": "25",
        **override,
    }

    assert _request_code_index(args_by_name) is None


def test_distance_retry_is_self_contained_and_closed():
    args_by_name = {
        "code": "99213",
        "code_system": "CPT",
        "view": "full",
        "include_providers": "false",
        "order_by": "cost",
        "order": "desc",
        "zip5": "60611",
        "zip_radius_miles": "25",
    }

    assert em_distance_retry_option(args_by_name) == {
        "order_by": "distance",
        "order": "asc",
        "include_providers": True,
        "view": "card",
    }
    assert em_distance_retry_option(
        {**args_by_name, "classification": "Internal Medicine"}
    ) is None
    assert em_distance_retry_option(
        {**args_by_name, "include_allowed_amounts": "true"}
    ) is None
    assert em_distance_retry_option(
        args_by_name,
        SimpleNamespace(limit=10, offset=191),
    ) is None


def test_distance_reader_uses_a_bounded_knn_window():
    assert "location.point <-> center.point" in _PAGE_SQL
    assert "LIMIT :candidate_limit" in _PAGE_SQL
    assert "LIMIT :page_limit OFFSET :offset" in _PAGE_SQL


def test_distance_card_item_uses_the_exact_code_slot():
    card_row_by_field = {
        "npi": 1003000123,
        "provider_name": "Synthetic Provider",
        "entity_type_code": 1,
        "credential": "MD",
        "taxonomy_code": "207R00000X",
        "primary_specialty": "Internal Medicine Physician",
        "classification": "Internal Medicine",
        "city": "CHICAGO",
        "state": "IL",
        "zip5": "60611",
        "distance_miles": 1.25,
        "minimum_rates": [1, 2, 3, 4, 5, 6],
        "maximum_rates": [11, 12, 13, 14, 15, 16],
        "rate_counts": [21, 22, 23, 24, 25, 26],
    }

    card_by_field = _card_item(card_row_by_field, 3)

    assert card_by_field["minimum_negotiated_rate"] == Decimal("4")
    assert card_by_field["maximum_negotiated_rate"] == Decimal("14")
    assert card_by_field["rate_count"] == 24
    assert card_by_field["distance_miles"] == 1.25
    assert PROJECTION_CONTRACT == "plan_pricing_em_distance_v1"


@pytest.mark.asyncio
async def test_selected_release_prefers_ready_em_distance_projection(monkeypatch):
    projected_response_by_field = {"items": [{"npi": 1003000123}]}

    async def read_em_projection(*_args):
        return projected_response_by_field

    async def reject_generic_projection(*_args):
        raise AssertionError("generic projection must not run")

    monkeypatch.setattr(
        ptg2_serving,
        "search_plan_pricing_em_distance",
        read_em_projection,
    )
    monkeypatch.setattr(
        ptg2_serving,
        "search_plan_pricing_projection",
        reject_generic_projection,
    )

    response = await ptg2_serving._search_selected_plan_release(
        object(),
        {"code": "99213"},
        SimpleNamespace(limit=10, offset=0, page=1),
        object(),
    )

    assert response is projected_response_by_field


@pytest.mark.asyncio
async def test_em_distance_reader_falls_through_without_attachment():
    class Result:
        def mappings(self):
            return self

        def all(self):
            return [{"projection_ready": False, "total": 0}]

    class Session:
        async def execute(self, *_args, **_kwargs):
            return Result()

    selection = PlanReleaseServingSelection(
        serving_revision_id="hpserve_test",
        plan_release_id="hprelease_test",
        healthporta_plan_id="hpplan_test",
        plan_version_id=None,
        release_month="2026-08",
        release_status="published",
        binding_set_digest="a" * 64,
        bindings=(),
    )
    args_by_name = {
        "code": "99213",
        "view": "card",
        "include_providers": "true",
        "order_by": "distance",
        "order": "asc",
        "zip5": "60611",
        "zip_radius_miles": "25",
    }

    assert await search_plan_pricing_em_distance(
        Session(),
        selection,
        args_by_name,
        SimpleNamespace(limit=10, offset=0, page=1),
    ) is None

    assert await search_plan_pricing_em_distance(
        Session(),
        selection,
        args_by_name,
        SimpleNamespace(limit=10, offset=191, page=20),
    ) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("ready", (False, True))
async def test_em_distance_readiness_is_exact_release_bound(ready):
    seen_parameters_by_field = {}

    class MappingResult:
        def mappings(self):
            return self

        def first(self):
            return {"projection_ready": ready}

    class Session:
        async def execute(self, _statement, parameters_by_field):
            seen_parameters_by_field.update(parameters_by_field)
            return MappingResult()

    selection = PlanReleaseServingSelection(
        serving_revision_id="hpserve_test",
        plan_release_id="hprelease_test",
        healthporta_plan_id="hpplan_test",
        plan_version_id=None,
        release_month="2026-08",
        release_status="published",
        binding_set_digest="a" * 64,
        bindings=(),
    )

    assert await is_em_distance_projection_ready(Session(), selection) is ready
    assert seen_parameters_by_field == {
        "serving_revision_id": "hpserve_test",
        "binding_set_digest": "a" * 64,
        "contract": PROJECTION_CONTRACT,
    }
