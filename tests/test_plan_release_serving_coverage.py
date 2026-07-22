# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Behavior coverage for fail-closed canonical release routing helpers."""

import asyncio
from types import SimpleNamespace

import pytest

from api import plan_release_serving
from api.endpoint import pricing


PLAN_RELEASE_ID = "hprelease_" + "0" * 26
HEALTHPORTA_PLAN_ID = "hpplan_" + "1" * 26
PLAN_VERSION_ID = "hpversion_" + "2" * 26
SERVING_REVISION_ID = "hpserve_" + "3" * 26


def _binding_row(**updates):
    row_by_field = {
        "serving_revision_id": SERVING_REVISION_ID,
        "plan_release_id": PLAN_RELEASE_ID,
        "healthporta_plan_id": HEALTHPORTA_PLAN_ID,
        "plan_version_id": PLAN_VERSION_ID,
        "release_month": "2026-07",
        "release_status": "published",
        "expected_binding_count": 1,
        "binding_set_digest": "a" * 64,
        "binding_ordinal": 0,
        "snapshot_id": "ptg2:release-network",
        "source_key": "aetna-network",
        "plan_id": "99-0000001",
        "plan_market_type": "group",
        "role": "in_network",
        "required": True,
        "snapshot_status": "published",
        "is_pinned": True,
    }
    row_by_field.update(updates)
    return row_by_field


def _binding(
    *,
    ordinal=0,
    role="in_network",
    snapshot_id="ptg2:release-network",
    source_key="aetna-network",
    plan_id="99-0000001",
    market_type="group",
):
    return plan_release_serving.PlanReleaseSnapshotBinding(
        binding_ordinal=ordinal,
        snapshot_id=snapshot_id,
        source_key=source_key,
        plan_id=plan_id,
        plan_market_type=market_type,
        role=role,
        required=True,
    )


def _selection(*bindings):
    return plan_release_serving.PlanReleaseServingSelection(
        serving_revision_id=SERVING_REVISION_ID,
        plan_release_id=PLAN_RELEASE_ID,
        healthporta_plan_id=HEALTHPORTA_PLAN_ID,
        plan_version_id=PLAN_VERSION_ID,
        release_month="2026-07",
        release_status="published",
        binding_set_digest="a" * 64,
        bindings=tuple(bindings),
    )


def test_release_selection_deduplicates_physical_reads_and_normalizes_metadata():
    first_duplicate_binding = _binding(ordinal=0)
    second_duplicate_binding = _binding(ordinal=1)
    allowed_binding = _binding(
        ordinal=0,
        role="allowed_amounts",
        snapshot_id="ptg2:release-allowed",
        source_key="aetna-allowed",
    )
    selection = _selection(
        first_duplicate_binding,
        second_duplicate_binding,
        allowed_binding,
    )

    assert plan_release_serving._row_mapping(
        SimpleNamespace(_mapping={"source_key": "mapped"})
    ) == {"source_key": "mapped"}
    assert selection.in_network_bindings == (first_duplicate_binding,)
    assert selection.allowed_amount_bindings == (allowed_binding,)
    assert plan_release_serving._single_text_value(
        [{"value": "first"}, {"value": "second"}],
        "value",
    ) is None

    assert plan_release_serving.annotate_plan_release_response(
        None,
        selection,
    ) is None
    response_by_field = {"resolved": False, "query": "legacy"}
    annotated = plan_release_serving.annotate_plan_release_response(
        response_by_field,
        selection,
    )
    assert annotated is response_by_field
    assert annotated["resolved"] is False
    assert annotated["query"]["plan_release_id"] == PLAN_RELEASE_ID


def test_release_projection_rejects_malformed_counts_ordinals_and_empty_rows():
    malformed_count_row = _binding_row(expected_binding_count="many")
    assert plan_release_serving._release_header_from_rows(
        PLAN_RELEASE_ID,
        [malformed_count_row],
    ) is None
    assert plan_release_serving._plan_release_binding_from_row(
        _binding_row(binding_ordinal="first")
    ) is None
    assert plan_release_serving._selection_from_rows(
        PLAN_RELEASE_ID,
        [],
    ) is None


def test_release_projection_rejects_one_snapshot_with_conflicting_plan_routes():
    release_rows = [
        _binding_row(expected_binding_count=2),
        _binding_row(
            expected_binding_count=2,
            binding_ordinal=1,
            plan_id="38-9999999",
        ),
    ]

    assert plan_release_serving._collect_plan_release_bindings(
        release_rows
    ) is None
    assert plan_release_serving._selection_from_rows(
        PLAN_RELEASE_ID,
        release_rows,
    ) is None


def test_pricing_release_selector_validation_and_market_guard_fail_closed():
    assert pricing._validated_plan_release_id({}) == ""
    assert pricing._validated_plan_release_id(
        {"plan_release_id": f" {PLAN_RELEASE_ID} "}
    ) == PLAN_RELEASE_ID

    with pytest.raises(pricing.InvalidUsage, match="26 uppercase"):
        pricing._validated_plan_release_id(
            {"plan_release_id": "hprelease_invalid"}
        )
    with pytest.raises(pricing.InvalidUsage, match="cannot be combined"):
        pricing._validated_plan_release_id(
            {
                "plan_release_id": PLAN_RELEASE_ID,
                "source_key": "override",
            }
        )

    assert pricing._release_market_type_for_guard(None) == ""
    assert pricing._release_market_type_for_guard(
        _selection(_binding(market_type="individual"))
    ) == "individual"
    assert pricing._release_market_type_for_guard(
        _selection(
            _binding(market_type="individual"),
            _binding(
                ordinal=1,
                snapshot_id="ptg2:release-dental",
                source_key="aetna-dental",
                market_type="dental",
            ),
        )
    ) == ""


def test_allowed_amount_query_params_deduplicate_physical_bindings():
    pagination = SimpleNamespace(limit=25, offset=0)
    duplicate_snapshot_by_field = {
        "snapshot_id": "ptg2:release-allowed",
        "source_key": "aetna-allowed",
        "plan_id": "99-0000001",
        "plan_market_type": "GROUP",
    }
    parameter_map = pricing._allowed_amount_query_params(
        {"plan_release_id": PLAN_RELEASE_ID},
        pagination,
        plan_id="99-0000001",
        code="99213",
        code_system="CPT",
        npi=None,
        current_snapshots=[
            duplicate_snapshot_by_field,
            duplicate_snapshot_by_field,
        ],
    )
    binding_tuples = list(
        zip(
            parameter_map["snapshot_ids"],
            parameter_map["source_keys"],
            parameter_map["plan_ids"],
            parameter_map["plan_market_types"],
        )
    )
    assert len(binding_tuples) == len(set(binding_tuples))
    assert parameter_map["allow_release_snapshot"] is True
    assert set(parameter_map["plan_market_types"]) == {"group"}


def test_allowed_amount_response_query_preserves_multi_plan_release_scope():
    args_by_name = {
        "_plan_release_allowed_bindings": [
            {
                "source_key": "aetna-b",
                "snapshot_id": "ptg2:b",
                "plan_id": "plan-b",
                "plan_market_type": "GROUP",
            },
            {
                "source_key": "aetna-a",
                "snapshot_id": "ptg2:a",
                "plan_id": "plan-a",
                "plan_market_type": "individual",
            },
            "ignored",
        ]
    }
    release_bindings, plan_ids, market_types = (
        pricing._allowed_release_query_scope(args_by_name)
    )
    assert len(release_bindings) == 2
    assert plan_ids == ["plan-a", "plan-b"]
    assert market_types == ["group", "individual"]
    assert pricing._allowed_evidence_coordinates(
        [
            {"source_key": "aetna-a", "snapshot_id": "ptg2:a"},
            {"source_key": "", "snapshot_id": None},
        ]
    ) == (["aetna-a"], ["ptg2:a"])

    query = pricing._allowed_amount_response_query(
        args_by_name,
        evidence_sources=[
            {"source_key": "aetna-a", "snapshot_id": "ptg2:a"}
        ],
        network_context=pricing._allowed_amount_network_context(()),
        plan_id="fallback-plan",
        code="99213",
        code_system="CPT",
    )
    assert query["plan_id"] is None
    assert query["plan_ids"] == ["plan-a", "plan-b"]
    assert query["plan_market_type"] is None
    assert query["plan_market_types"] == ["group", "individual"]


def test_allowed_amount_release_route_rejects_conflicts_unknowns_and_wrong_roles(
    monkeypatch,
):
    async def fail_resolver(*_args, **_kwargs):
        raise AssertionError("conflicts must be rejected before resolution")

    monkeypatch.setattr(
        pricing,
        "resolve_plan_release_serving",
        fail_resolver,
    )
    assert asyncio.run(
        pricing._resolve_allowed_amount_route(
            object(),
            {"plan_release_id": PLAN_RELEASE_ID, "plan_id": "override"},
        )
    ) is None

    async def unknown_resolver(*_args, **_kwargs):
        return None

    monkeypatch.setattr(
        pricing,
        "resolve_plan_release_serving",
        unknown_resolver,
    )
    assert asyncio.run(
        pricing._resolve_allowed_amount_route(
            object(),
            {"plan_release_id": PLAN_RELEASE_ID},
        )
    ) is None

    async def network_only_resolver(*_args, **_kwargs):
        return _selection(_binding())

    monkeypatch.setattr(
        pricing,
        "resolve_plan_release_serving",
        network_only_resolver,
    )
    assert asyncio.run(
        pricing._resolve_allowed_amount_route(
            object(),
            {"plan_release_id": PLAN_RELEASE_ID},
        )
    ) is None
    assert asyncio.run(
        pricing._search_ptg_allowed_amount_evidence(
            object(),
            {"plan_release_id": PLAN_RELEASE_ID, "plan_id": "override"},
            SimpleNamespace(limit=25, offset=0, page=1),
        )
    ) is None


def test_allowed_amount_release_result_is_annotated_after_a_successful_page(
    monkeypatch,
):
    selection = _selection(
        _binding(
            role="allowed_amounts",
            snapshot_id="ptg2:release-allowed",
            source_key="aetna-allowed",
        )
    )
    resolved_args_by_name = {
        "plan_release_id": PLAN_RELEASE_ID,
        "plan_id": "99-0000001",
    }

    async def resolved_route(_session, _args):
        return resolved_args_by_name, selection

    async def snapshot_rows(*_args, **_kwargs):
        return [{"snapshot_id": "ptg2:release-allowed"}]

    async def page_rows(*_args, **_kwargs):
        return [{"total": 1}], {"query": "params"}

    async def response_from_page(*_args, **_kwargs):
        return {"resolved": True, "items": [{"npi": 1234567890}], "query": {}}

    monkeypatch.setattr(pricing, "_resolve_allowed_amount_route", resolved_route)
    monkeypatch.setattr(
        pricing,
        "_allowed_amount_scope_from_args",
        lambda _args: ("99-0000001", "99213", "CPT", None),
    )
    monkeypatch.setattr(
        pricing,
        "_supports_allowed_amount_fallback",
        lambda _args: True,
    )
    monkeypatch.setattr(pricing, "_allowed_amount_snapshot_rows", snapshot_rows)
    monkeypatch.setattr(pricing, "_allowed_amount_page_rows", page_rows)
    monkeypatch.setattr(
        pricing,
        "_allowed_amount_response_from_page",
        response_from_page,
    )

    response_by_field = asyncio.run(
        pricing._search_ptg_allowed_amount_evidence(
            object(),
            resolved_args_by_name,
            SimpleNamespace(limit=25, offset=0, page=1),
        )
    )

    assert response_by_field["plan_release_id"] == PLAN_RELEASE_ID
    assert response_by_field["serving_revision_id"] == SERVING_REVISION_ID
    assert response_by_field["query"]["healthporta_plan_id"] == HEALTHPORTA_PLAN_ID
