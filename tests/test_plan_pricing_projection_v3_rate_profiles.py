# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact bounded rate-profile storage contracts for projection v3."""

from __future__ import annotations

import hashlib
from decimal import Decimal
from types import SimpleNamespace

import pytest

from api import plan_pricing_projection_v3_code as rate_profiles
from api.plan_pricing_projection_v3_types import _BuildState


PROJECTION_ID = "a" * 64


def _profile(**updates):
    profile_by_field = {
        "binding_ordinal": 1,
        "provider_set_key": 7,
        "membership_count": 2,
        "minimum_negotiated_rate": Decimal("10"),
        "maximum_negotiated_rate": Decimal("30"),
        "rate_count": 4,
        "negotiated_rates": [Decimal("10"), Decimal("30")],
        "rate_multiplicities": [1, 3],
    }
    profile_by_field.update(updates)
    return profile_by_field


class _ProfileStream:
    def __init__(self, rows):
        self.rows = tuple(rows)

    def mappings(self):
        return self

    async def __aiter__(self):
        for row_by_field in self.rows:
            yield row_by_field


class _Session:
    def __init__(self, rows, *, work_rows: int = 2):
        self.rows = rows
        self.work_rows = work_rows
        self.execute_calls = []
        self.stream_calls = []

    async def execute(self, statement, parameters=None):
        self.execute_calls.append((str(statement), parameters))
        return SimpleNamespace(scalar_one=lambda: self.work_rows)

    async def stream(self, statement, parameters=None):
        self.stream_calls.append((statement, parameters))
        return _ProfileStream(self.rows)


def test_rate_profile_validation_preserves_exact_sorted_multiplicity() -> None:
    assert rate_profiles._validated_rate_profile(_profile()) == (
        1,
        7,
        2,
        (Decimal("10"), Decimal("30")),
        (1, 3),
        4,
    )

    invalid_profiles = (
        _profile(negotiated_rates=[]),
        _profile(negotiated_rates=[Decimal("30"), Decimal("10")]),
        _profile(negotiated_rates=[Decimal("10"), Decimal("10")]),
        _profile(rate_multiplicities=[4]),
        _profile(rate_multiplicities=[0, 4]),
        _profile(rate_count=5),
        _profile(minimum_negotiated_rate=Decimal("9")),
        _profile(maximum_negotiated_rate=Decimal("31")),
        _profile(membership_count=16_385),
    )
    for invalid_profile_by_field in invalid_profiles:
        with pytest.raises(ValueError, match="rate profile is invalid"):
            rate_profiles._validated_rate_profile(invalid_profile_by_field)


@pytest.mark.asyncio
async def test_rate_profile_store_is_sql_folded_streamed_and_digested() -> None:
    profile_rows = (_profile(), _profile(binding_ordinal=2, provider_set_key=9))
    session = _Session(profile_rows)
    state = _BuildState(hashlib.sha256())

    await rate_profiles._store_rate_profiles(
        session,
        PROJECTION_ID,
        ("CPT", "27447"),
        state,
    )

    work_sql, work_parameters = session.execute_calls[0]
    assert "SUM(price.count)" in work_sql
    assert work_parameters is None
    insert_sql, parameters = session.execute_calls[1]
    assert "SUM(occurrence.occurrence_count" in insert_sql
    assert "* price.rate_multiplicity" in insert_sql
    assert "plan_pricing_provider_set_stage" in insert_sql
    assert "COUNT(*)" not in insert_sql
    assert "ARRAY_AGG(rate.negotiated_rate ORDER BY" in insert_sql
    assert parameters == {
        "projection_id": PROJECTION_ID,
        "code_system": "CPT",
        "code": "27447",
    }
    stream_statement, stream_parameters = session.stream_calls[0]
    assert stream_statement.get_execution_options()["yield_per"] == 32
    assert stream_parameters == parameters
    assert state.rate_profile_count == 2
    assert state.rate_profile_work_rows == 2
    assert state.content_digest.hexdigest() != hashlib.sha256().hexdigest()


def test_rate_profile_fragment_keeps_decimal_text_and_multiplicity() -> None:
    assert rate_profiles._rate_profile_fragment(
        (Decimal("10.500"), Decimal("30")), (2, 1)
    ) == b"[[10.5,2],[30,1]]"


@pytest.mark.asyncio
@pytest.mark.parametrize(("work_rows", "fails"), ((1, False), (2, True)))
async def test_rate_profile_work_cap_is_inclusive(
    monkeypatch, work_rows: int, fails: bool
) -> None:
    monkeypatch.setattr(rate_profiles, "MAX_CODE_RATE_PROFILE_WORK_ROWS", 1)
    session = _Session((), work_rows=work_rows)
    state = _BuildState(hashlib.sha256())

    if not fails:
        await rate_profiles._store_rate_profiles(
            session,
            PROJECTION_ID,
            ("CPT", "27447"),
            state,
        )
        assert state.rate_profile_work_rows == 1
        assert len(session.execute_calls) == 2
        return

    with pytest.raises(ValueError, match="work bound exceeded"):
        await rate_profiles._store_rate_profiles(
            session,
            PROJECTION_ID,
            ("CPT", "27447"),
            state,
        )
    assert len(session.execute_calls) == 1


@pytest.mark.asyncio
async def test_rate_profile_release_work_cap_is_cumulative(monkeypatch) -> None:
    monkeypatch.setattr(
        rate_profiles, "MAX_PROJECTION_RATE_PROFILE_WORK_ROWS", 1
    )
    state = _BuildState(hashlib.sha256())
    state.rate_profile_work_rows = 1
    session = _Session((), work_rows=1)

    with pytest.raises(ValueError, match="work bound exceeded"):
        await rate_profiles._store_rate_profiles(
            session, PROJECTION_ID, ("CPT", "27447"), state
        )

    assert len(session.execute_calls) == 1
