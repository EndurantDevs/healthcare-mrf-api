# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""UTC serialization keeps default output while allowing explicit database precision."""

import datetime as dt

import pytest

from process import import_status_events as status_events


@pytest.mark.parametrize(
    "value,expected,precise",
    [
        (dt.datetime(2026, 7, 22, 1, 2, 3), "2026-07-22T01:02:03+00:00", "2026-07-22T01:02:03.000000+00:00"),
        (
            dt.datetime(2026, 7, 22, 1, 2, 3, 1234),
            "2026-07-22T01:02:03.001234+00:00",
            "2026-07-22T01:02:03.001234+00:00",
        ),
        (
            dt.datetime(2026, 7, 22, 1, 2, 3, tzinfo=dt.timezone(dt.timedelta(hours=2))),
            "2026-07-21T23:02:03+00:00",
            "2026-07-21T23:02:03.000000+00:00",
        ),
        ("2026-07-22T01:02:03Z", "2026-07-22T01:02:03+00:00", "2026-07-22T01:02:03.000000+00:00"),
        ("2026-07-22T01:02:03", "2026-07-22T01:02:03+00:00", "2026-07-22T01:02:03.000000+00:00"),
        ("2026-07-22T01:02:03.123456-02:00", "2026-07-22T03:02:03.123456+00:00", "2026-07-22T03:02:03.123456+00:00"),
        (None, None, None),
        ("", "", ""),
        (" ", " ", " "),
        ("not-a-date", "not-a-date", "not-a-date"),
        (7, 7, 7),
    ],
)
def test_status_timestamp_precision_preserves_default(value, expected, precise):
    assert status_events.isoformat_utc(value) == expected
    assert status_events.isoformat_utc(value, timespec="auto") == expected
    assert status_events.isoformat_utc(value, timespec="microseconds") == precise


def test_status_timestamp_precision_uses_datetime_validation():
    with pytest.raises(ValueError):
        status_events.isoformat_utc(dt.datetime(2026, 7, 22), timespec="invalid")
    with pytest.raises(TypeError):
        status_events.isoformat_utc(None, "microseconds")
