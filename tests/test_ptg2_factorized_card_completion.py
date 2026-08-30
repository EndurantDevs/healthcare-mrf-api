# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact selected-NPI completion and frozen provider-cell tests."""

from __future__ import annotations

import json
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from api import ptg2_serving as serving
from tests.ptg2_factorized_card_support import (
    MappingResult,
    completion_row,
)


def test_completion_sql_is_one_release_wide_profile_query():
    """Completion has no binding fanout or live provider dependency."""

    statement = str(serving._factorized_card_completion_query(""))
    assert statement.count("plan_pricing_rate_profile") == 3
    assert "plan_pricing_provider_membership" in statement
    assert "plan_pricing_provider_cell" in statement
    assert "profile.negotiated_rates" in statement
    assert "profile.rate_multiplicities" in statement
    assert "cardinality(negotiated_rates)" in statement
    assert "SUM(profile.rate_count)" in statement
    assert "LIMIT :membership_probe_limit" in statement
    assert "LIMIT :provider_set_probe_limit" in statement
    assert "rate_admission.rate_value_count <= :rate_value_limit" in statement
    assert statement.index("profile_scope AS MATERIALIZED") < statement.index(
        "profile.negotiated_rates, profile.rate_multiplicities"
    )
    assert "entity_address_unified" not in statement
    assert "ptg2_v3" not in statement


def test_completion_merges_bindings_and_orders_exact_selected_stats():
    """The one SQL result exposes globally recomputed min, max, and count."""

    completion_rows = [
        completion_row(101, "12", "40", 7),
        completion_row(102, "10", "25", 3),
    ]
    item_list = serving._factorized_card_completion_items(
        completion_rows,
        (101, 102),
        {101: Decimal("12"), 102: Decimal("10")},
    )
    assert [item["npi"] for item in item_list] == [102, 101]
    assert item_list[0]["minimum_negotiated_rate"] == 10
    assert item_list[0]["maximum_negotiated_rate"] == 25
    assert item_list[0]["rate_count"] == 3
    assert item_list[1]["rate_count"] == 7


def test_completion_emits_only_compact_card_fields():
    """Frozen implementation details cannot expand the public card shape."""

    completion_by_field = completion_row()
    fragment_by_field = json.loads(completion_by_field["fragment"])
    fragment_by_field["internal_evidence"] = "must-not-leak"
    completion_by_field["fragment"] = json.dumps(fragment_by_field).encode()

    payload_by_field = serving._factorized_card_payload(completion_by_field)

    assert set(payload_by_field) == {
        *serving._FACTORIZED_CARD_PROVIDER_FIELDS,
        "minimum_negotiated_rate",
        "maximum_negotiated_rate",
        "rate_count",
    }
    assert "internal_evidence" not in payload_by_field


@pytest.mark.parametrize(
    "completion_rows, message",
    [
        ([completion_row(profiles_valid=False)], "rate profile is invalid"),
        ([completion_row(minimum_rate="11")], "minimum changed"),
        ([], "returned no rows"),
    ],
)
def test_completion_fails_closed_on_profile_or_provider_drift(
    completion_rows,
    message,
):
    """Invalid profiles, changed minima, and missing providers never serve."""

    with pytest.raises(serving.PTG2ManifestArtifactError, match=message):
        serving._factorized_card_completion_items(
            completion_rows,
            (101,),
            {101: Decimal("10")},
        )


def test_completion_rejects_membership_and_provider_set_sentinels(monkeypatch):
    """Selected-NPI completion remains within both durable work caps."""

    monkeypatch.setattr(serving, "_FACTORIZED_CARD_MEMBERSHIP_LIMIT", 1)
    with pytest.raises(
        serving.PTG2OnlineWorkBudgetExceeded,
        match="candidate_members",
    ):
        serving._factorized_card_completion_items(
            [completion_row(membership_count=2)],
            (101,),
            {101: Decimal("10")},
        )

    monkeypatch.setattr(serving, "_FACTORIZED_CARD_PROVIDER_SET_LIMIT", 1)
    with pytest.raises(serving.PTG2OnlineWorkBudgetExceeded, match="code_sets"):
        serving._factorized_card_completion_items(
            [completion_row(membership_count=1, provider_set_count=2)],
            (101,),
            {101: Decimal("10")},
        )

    monkeypatch.setattr(serving, "_FACTORIZED_CARD_RATE_VALUE_LIMIT", 1)
    with pytest.raises(serving.PTG2OnlineWorkBudgetExceeded, match="rate_values"):
        serving._factorized_card_completion_items(
            [
                completion_row(
                    membership_count=1,
                    provider_set_count=1,
                    rate_value_count=2,
                )
            ],
            (101,),
            {101: Decimal("10")},
        )


@pytest.mark.asyncio
async def test_selected_completion_is_one_frozen_query_with_exact_scope():
    """All selected NPIs and cells bind one canonical completion query."""

    session = AsyncMock()
    session.execute.return_value = MappingResult(
        [completion_row(101), completion_row(102, "20", "35", 5)]
    )
    item_list = await serving._factorized_card_complete_selected_npis(
        session,
        "f" * 64,
        ("CPT", "27447"),
        ["60601", "60602"],
        {"code_system": "CPT", "code": "27447"},
        (101, 102),
        {101: Decimal("10"), 102: Decimal("20")},
    )
    assert [
        card_by_field["npi"] for card_by_field in item_list
    ] == [101, 102]
    session.execute.assert_awaited_once()
    parameters_by_name = session.execute.await_args.args[1]
    assert parameters_by_name["projection_id"] == "f" * 64
    assert parameters_by_name["selected_npis"] == [101, 102]
    assert parameters_by_name["geo_cells"] == ["60601", "60602"]
    assert parameters_by_name["membership_probe_limit"] == (
        serving._FACTORIZED_CARD_MEMBERSHIP_LIMIT + 1
    )
    assert parameters_by_name["provider_set_probe_limit"] == (
        serving._FACTORIZED_CARD_PROVIDER_SET_LIMIT + 1
    )
    assert parameters_by_name["rate_value_limit"] == (
        serving._FACTORIZED_CARD_RATE_VALUE_LIMIT
    )


@pytest.mark.asyncio
async def test_empty_selected_completion_does_no_database_work():
    """No-match candidate pages avoid even the completion query."""

    session = AsyncMock()
    assert await serving._factorized_card_complete_selected_npis(
        session,
        "f" * 64,
        ("HCPCS", "G0439"),
        ["60601"],
        {"code_system": "HCPCS", "code": "G0439"},
        (),
        {},
    ) == []
    session.execute.assert_not_awaited()
