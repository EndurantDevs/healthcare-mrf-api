# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pinned address freshness and missing-source clocks retain standalone behavior."""

from contextlib import contextmanager
from dataclasses import replace
from datetime import datetime, timezone

import pytest

from process import entity_address_candidate_preparation as preparation
from tests.test_entity_address_candidate_preparation_postgres import _inputs, native
from tests.test_entity_address_unified_publication_db import _temporary_schema


@contextmanager
def _semantic_date(semantic_date):
    inputs = replace(_inputs(), semantic_as_of=semantic_date)
    preparation.validate_preparation_input(inputs)
    token = preparation._PREPARATION.set(inputs)
    try:
        yield
    finally:
        preparation._PREPARATION.reset(token)


@pytest.mark.parametrize("invalid", ["", "20200101", "2020-1-1", "2020-02-30", "2020-01-01T00:00:00Z", True])
def test_date_requires_canonical_calendar_day(invalid):
    with pytest.raises(ValueError):
        preparation.validate_preparation_input(replace(_inputs(), semantic_as_of=invalid))


def test_source_fallbacks_share_pinned_date():
    available = dict.fromkeys(
        (
            "npi_address",
            "doctor_clinician_address",
            "provider_enrollment_ffs",
            "provider_enrollment_ffs_address",
            "facility_anchor",
            "mrf_address",
            "provider_directory_organization",
        ),
        True,
    )
    with _semantic_date("2020-03-01"):
        selects = native._source_selects("sample", available)
        assert len(selects) == 6
        assert all("NOW()" not in statement and "TIMESTAMP '2020-03-01 00:00:00'" in statement for statement in selects)
        overlay = native._provider_directory_partial_overlay_source_select("sample", {})
        assert (
            "COALESCE(overlay.source_updated_at, TIMESTAMP '2020-03-01 00:00:00')::timestamp AS updated_at" in overlay
        )
        assert "COALESCE(overlay.source_updated_at, overlay.published_at" not in overlay
    assert all("NOW()" in statement for statement in native._source_selects("sample", available))


def test_unpinned_clock_and_publication_timestamps_are_preserved():
    with _semantic_date(None):
        assert preparation.semantic_now_sql() == "NOW()"
        assert (
            preparation.overlay_updated_at_sql()
            == "COALESCE(overlay.source_updated_at, overlay.published_at, NOW())::timestamp"
        )
        assert "updated_at >= NOW() - INTERVAL '12 months'" in native._raw_materialize_select_assurance_sql("")
    with _semantic_date("2020-03-01"):
        assert "NOW()::timestamp AS updated_at" in native._promote_facility_npi_candidates_sql("sample")
        assert "(updated_at AT TIME ZONE 'UTC')" in native._evidence_from_raw_sql(
            "sample", "evidence", "raw", source_run_id="run", node_id=None
        )


def _freshness_expression():
    """Execute the actual emitted scoring expression independently of unrelated address fields."""
    return next(
        line.strip().rstrip(",")
        for line in native._raw_materialize_select_assurance_sql("").splitlines()
        if " AS freshness_score" in line
    )


async def test_backdated_freshness_uses_inclusive_pinned_boundary():
    async with _temporary_schema() as (database, _schema):
        assert await _freshness_rows(database, "2020-03-01") == [10, 0, 0]
        assert await _freshness_rows(database, "2020-03-02") == [0, 0, 0]
        assert await _freshness_rows(database, "2020-03-01") == [10, 0, 0]


async def _freshness_rows(database, semantic_date):
    """Evaluate retained source timestamps against the desired generation's effective date."""
    with _semantic_date(semantic_date):
        rows = await database.all(
            f"SELECT {_freshness_expression()} FROM (VALUES "
            "(1, TIMESTAMP '2019-03-01'), (2, TIMESTAMP '2019-02-28 23:59:59'), (3, NULL::timestamp)) "
            "AS source(ordinal, updated_at) ORDER BY ordinal"
        )
    return [row[0] for row in rows]


async def test_missing_overlay_time_is_stable_across_session_timezones():
    async with _temporary_schema() as (database, _schema):
        with _semantic_date("2020-03-01"):
            for session_zone in ("UTC", "Pacific/Honolulu"):
                assert await _overlay_timestamps(database, session_zone) == (
                    datetime(2020, 3, 1),
                    datetime(2020, 3, 1, tzinfo=timezone.utc),
                    datetime(2018, 1, 2),
                )


async def _overlay_timestamps(database, session_zone):
    """Separate source observation, semantic fallback and build-time publication clocks."""
    async with database.transaction():
        await database.status("SELECT set_config('TimeZone', :zone, true)", zone=session_zone)
        fallback = preparation.overlay_updated_at_sql()
        observed = preparation.source_observed_at_sql(fallback)
        row = await database.first(
            f"SELECT {fallback}, {observed}, COALESCE(TIMESTAMP '2018-01-02', {preparation.semantic_now_sql()}) "
            "FROM (VALUES (NULL::timestamp, TIMESTAMP '2040-01-01')) AS overlay(source_updated_at, published_at)"
        )
    return tuple(row)
