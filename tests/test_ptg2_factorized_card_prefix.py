# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Release-wide rate-profile prefix and tie-completeness tests."""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from api import ptg2_serving as serving
from tests.ptg2_factorized_card_support import (
    MappingResult,
    candidate_row,
)


def test_candidate_sql_is_release_wide_frozen_and_cumulatively_bounded():
    """One indexed profile prefix precedes all membership and cell work."""

    statement = str(serving._factorized_card_candidate_query(""))
    assert statement.count("plan_pricing_rate_profile") == 1
    assert "plan_pricing_provider_membership" in statement
    assert "plan_pricing_provider_cell" in statement
    assert "SUM(membership_count) OVER" in statement
    assert statement.index("admitted_profiles") < statement.index("eligible")
    assert "profile_rank <= :provider_set_limit" in statement
    assert "cumulative_memberships <= :membership_limit" in statement
    assert "minimum_negotiated_rate" in statement
    assert "entity_address_unified" not in statement
    assert "npi_taxonomy" not in statement
    assert "ptg2_v3" not in statement


def test_candidate_selection_accepts_complete_tie_and_progressive_total():
    """The unread profile must be strictly costlier than the page boundary."""

    candidate_rows = [
        candidate_row(101, "10", total_lower_bound=3, boundary_rate="15"),
        candidate_row(102, "15", total_lower_bound=3, boundary_rate="15"),
        candidate_row(103, "15", total_lower_bound=3, boundary_rate="15"),
    ]
    selection = serving._factorized_card_candidate_selection(
        candidate_rows,
        target_count=2,
        candidate_limit=10,
    )
    assert selection.minimum_rate_by_npi == {
        101: Decimal("10"),
        102: Decimal("15"),
        103: Decimal("15"),
    }
    assert selection.total_lower_bound == 3
    assert selection.total_is_exact is False


@pytest.mark.parametrize(
    "candidate_rows",
    [
        [
            candidate_row(
                101,
                "10",
                total_lower_bound=2,
                unread_minimum_rate="10",
                boundary_rate="10",
            ),
            candidate_row(
                102,
                "10",
                total_lower_bound=2,
                unread_minimum_rate="10",
                boundary_rate="10",
            ),
        ],
        [
            candidate_row(
                101,
                "10",
                total_lower_bound=1,
                unread_minimum_rate="20",
                boundary_rate=None,
            )
        ],
    ],
)
def test_candidate_selection_fails_closed_before_unread_work(candidate_rows):
    """An unfinished tie or undersized prefix cannot claim a global page."""

    with pytest.raises(
        serving.PTG2OnlineWorkBudgetExceeded,
        match="candidate_members",
    ):
        serving._factorized_card_candidate_selection(
            candidate_rows,
            target_count=2,
            candidate_limit=10,
        )


def test_candidate_selection_enforces_candidate_and_declared_work_caps(
    monkeypatch,
):
    """Candidate, provider-set, and cumulative membership sentinels reject."""

    candidate_rows = [
        candidate_row(101, "10", profile_exhausted=True),
        candidate_row(102, "20", profile_exhausted=True),
    ]
    with pytest.raises(
        serving.PTG2OnlineWorkBudgetExceeded,
        match="candidate_members",
    ):
        serving._factorized_card_candidate_selection(
            candidate_rows,
            target_count=1,
            candidate_limit=1,
        )

    monkeypatch.setattr(serving, "_FACTORIZED_CARD_PROVIDER_SET_LIMIT", 1)
    over_set_cap_rows = [
        candidate_row(
            101,
            profile_exhausted=True,
            provider_set_count=2,
        )
    ]
    with pytest.raises(serving.PTG2OnlineWorkBudgetExceeded, match="code_sets"):
        serving._factorized_card_candidate_selection(
            over_set_cap_rows,
            target_count=1,
            candidate_limit=5,
        )

    monkeypatch.setattr(serving, "_FACTORIZED_CARD_MEMBERSHIP_LIMIT", 1)
    over_member_cap_rows = [
        candidate_row(
            101,
            profile_exhausted=True,
            provider_set_count=1,
            membership_count=2,
        )
    ]
    with pytest.raises(
        serving.PTG2OnlineWorkBudgetExceeded,
        match="candidate_members",
    ):
        serving._factorized_card_candidate_selection(
            over_member_cap_rows,
            target_count=1,
            candidate_limit=5,
        )


@pytest.mark.parametrize(
    ("candidate_rows", "message"),
    [
        (
            [candidate_row(profiles_valid=False, profile_exhausted=True)],
            "rate profile is invalid",
        ),
        (
            [
                candidate_row(101, profile_exhausted=True),
                candidate_row(
                    102,
                    profile_exhausted=True,
                    total_lower_bound=3,
                ),
            ],
            "metadata is inconsistent",
        ),
    ],
)
def test_candidate_selection_rejects_invalid_profile_receipts(
    candidate_rows,
    message,
):
    """Every candidate row must repeat one valid bounded admission proof."""

    with pytest.raises(serving.PTG2ManifestArtifactError, match=message):
        serving._factorized_card_candidate_selection(
            candidate_rows,
            target_count=1,
            candidate_limit=5,
        )


@pytest.mark.asyncio
async def test_candidate_read_is_one_canonical_release_query(monkeypatch):
    """CPT and numeric HCPCS requests bind the same compact profile identity."""

    monkeypatch.setattr(
        serving,
        "_ptg2_manifest_location_match_limit",
        lambda: 20,
    )
    for requested_system in ("CPT", "HCPCS"):
        session = AsyncMock()
        session.execute.return_value = MappingResult(
            [candidate_row(101, profile_exhausted=True, total_lower_bound=1)]
        )
        code_identity = serving._factorized_card_code_identity(
            {"code_system": requested_system, "code": "27447"}
        )
        selection = await serving._factorized_card_candidates(
            session,
            "f" * 64,
            code_identity,
            ["60601", "60602"],
            {"code_system": requested_system, "code": "27447"},
            1,
        )
        assert selection.total_is_exact is True
        session.execute.assert_awaited_once()
        parameters_by_name = session.execute.await_args.args[1]
        assert parameters_by_name["code_system"] == "CPT"
        assert parameters_by_name["code"] == "27447"
        assert parameters_by_name["geo_cells"] == ["60601", "60602"]
        assert parameters_by_name["candidate_probe_limit"] == 21
