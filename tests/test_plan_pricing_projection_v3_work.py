# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded per-code PostgreSQL work admission for projection v3."""

from __future__ import annotations

import hashlib
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import plan_pricing_projection_v3_work as work_admission
from api import ptg2_serving as serving
from api.plan_pricing_projection_v3_types import _BuildState


def _code_work(**updates_by_field) -> work_admission._CodeWork:
    values_by_field = {
        "membership_probe_rows": 1,
        "member_cell_rows": 1,
        "set_cell_rows": 1,
        "profile_join_rows": 1,
        "aggregate_join_rows": 1,
        "profile_rate_count_sum": 1,
        "profile_rate_count_max": 1,
        "profile_distinct_rate_count_max": 1,
        "aggregate_rate_count_sum": 1,
        "aggregate_rate_count_max": 1,
    }
    values_by_field.update(updates_by_field)
    return work_admission._CodeWork(**values_by_field)


def _unit_work_caps(monkeypatch) -> None:
    monkeypatch.setattr(work_admission, "MAX_CODE_MEMBERSHIP_PROBES", 1)
    monkeypatch.setattr(
        work_admission, "MAX_PROJECTION_MEMBERSHIP_PROBES", 1
    )
    monkeypatch.setattr(work_admission, "MAX_CODE_RATE_PROFILE_WORK_ROWS", 1)
    monkeypatch.setattr(
        work_admission, "MAX_PROJECTION_RATE_PROFILE_WORK_ROWS", 1
    )
    monkeypatch.setattr(work_admission, "MAX_CODE_AGGREGATE_WORK_ROWS", 1)
    monkeypatch.setattr(
        work_admission, "MAX_PROJECTION_AGGREGATE_WORK_ROWS", 1
    )


def test_code_work_caps_are_inclusive_and_recorded_together(monkeypatch) -> None:
    _unit_work_caps(monkeypatch)
    state = _BuildState(hashlib.sha256())

    work_admission._record_code_work(
        state, _code_work(), work_admission._WorkLimits(1, 1, 1, 1)
    )

    assert state.membership_probe_work_rows == 1
    assert state.member_cell_work_rows == 1
    assert state.rate_profile_work_rows == 1
    assert state.aggregate_work_rows == 1


@pytest.mark.parametrize(
    ("updates_by_field", "error"),
    (
        ({"membership_probe_rows": 2}, "membership-probe"),
        ({"member_cell_rows": 2}, "member-cell"),
        ({"profile_join_rows": 2}, "rate-profile"),
        ({"aggregate_join_rows": 2}, "aggregate"),
        (
            {
                "profile_distinct_rate_count_max": (
                    work_admission.MAX_RATE_PROFILE_RATES + 1
                )
            },
            "rate profile",
        ),
        ({"profile_rate_count_max": work_admission.MAX_BIGINT + 1}, "bigint"),
        ({"aggregate_rate_count_max": work_admission.MAX_BIGINT + 1}, "bigint"),
    ),
)
def test_code_work_rejection_does_not_partially_record(
    monkeypatch, updates_by_field, error
) -> None:
    _unit_work_caps(monkeypatch)
    state = _BuildState(hashlib.sha256())

    with pytest.raises(ValueError, match=error):
        work_admission._record_code_work(
            state,
            _code_work(**updates_by_field),
            work_admission._WorkLimits(1, 1, 1, 1),
        )

    assert (
        state.membership_probe_work_rows,
        state.member_cell_work_rows,
        state.rate_profile_work_rows,
        state.aggregate_work_rows,
    ) == (0, 0, 0, 0)


@pytest.mark.parametrize(
    ("state_field", "projection_cap", "error"),
    (
        (
            "membership_probe_work_rows",
            None,
            "membership-probe",
        ),
        ("member_cell_work_rows", None, "member-cell"),
        (
            "rate_profile_work_rows",
            "MAX_PROJECTION_RATE_PROFILE_WORK_ROWS",
            "rate-profile",
        ),
        (
            "aggregate_work_rows",
            "MAX_PROJECTION_AGGREGATE_WORK_ROWS",
            "aggregate",
        ),
    ),
)
def test_projection_work_caps_are_cumulative(
    monkeypatch, state_field, projection_cap, error
) -> None:
    _unit_work_caps(monkeypatch)
    if projection_cap is not None:
        monkeypatch.setattr(work_admission, projection_cap, 1)
    state = _BuildState(hashlib.sha256())
    setattr(state, state_field, 1)

    with pytest.raises(ValueError, match=error):
        work_admission._record_code_work(
            state, _code_work(), work_admission._WorkLimits(1, 1, 1, 1)
        )

    assert getattr(state, state_field) == 1


@pytest.mark.asyncio
async def test_uncalibrated_member_cell_cap_fails_before_sql() -> None:
    session = SimpleNamespace(execute=AsyncMock())

    with pytest.raises(ValueError, match="not calibrated"):
        await work_admission._prepare_code_work(
            session,
            "a" * 64,
            ("HCPCS", "G0439"),
            _BuildState(hashlib.sha256()),
        )

    session.execute.assert_not_awaited()


def test_member_cell_stage_uses_the_exact_normalized_taxonomy_rule(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        serving,
        "_inferred_provider_taxonomy_rule",
        lambda _code: SimpleNamespace(
            taxonomy_codes=(" 207x00000x ", "207X00000X", " ")
        ),
    )

    has_rule, taxonomy_codes = work_admission._normalized_taxonomy_codes(
        ("CPT", "27447")
    )
    taxonomy_sql = work_admission._delete_ineligible_member_cells_sql(has_rule)

    assert taxonomy_codes == ["207X00000X"]
    assert "provider.entity_type_code = 1" in taxonomy_sql
    assert "upper(btrim(taxonomy_code))" in taxonomy_sql
    assert "LIMIT :member_cell_limit" in work_admission._MEMBER_CELL_PROBE_SQL
    assert work_admission._delete_ineligible_member_cells_sql(False) is None
