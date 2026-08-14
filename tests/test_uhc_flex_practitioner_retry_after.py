# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Retry-After boundaries for exact Flex Practitioner transport."""

import datetime as dt
import email.utils

from process import uhc_flex_practitioner_transport as transport


def test_retry_after_accepts_delta_or_date_but_never_exceeds_bound() -> None:
    future = dt.datetime.now(dt.UTC) + dt.timedelta(seconds=15)

    assert transport.uhc_flex_practitioner_retry_after_seconds("2.5") == 2.5
    assert transport.uhc_flex_practitioner_retry_after_seconds("9999") == 60.0
    assert transport.uhc_flex_practitioner_retry_after_seconds("-3") == 0.0
    assert transport.uhc_flex_practitioner_retry_after_seconds("invalid") == 0.0
    date_delay = transport.uhc_flex_practitioner_retry_after_seconds(
        email.utils.format_datetime(future, usegmt=True)
    )
    assert 13.0 <= date_delay <= 15.0
