# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""FHIR identifier parsing, period evaluation, and NPI normalization."""

from __future__ import annotations

import datetime as dt
from collections.abc import Mapping, Sequence
from typing import Any

from process.tin_npi_connector_policy import FhirTinNpiIdentifierRule
from process.tin_npi_connector_support import (
    _ALLOWED_NPI_SEPARATORS,
    _FHIR_DATE_PATTERN,
    _FHIR_DATETIME_PATTERN,
    _NPI_LUHN_PREFIX_DIGIT_SUM,
    _NPI_MAX,
    _NPI_MIN,
    _NORMALIZED_NPI_PATTERN,
    _MalformedFhirIdentifierPeriod,
    _UnresolvedFhirIdentifierPeriod,
    TinNpiConnectorError,
)


def _identifier_type_codings(
    identifier: Mapping[str, Any],
) -> tuple[tuple[str, str], ...]:
    raw_type = identifier.get("type")
    raw_codings = raw_type.get("coding") if isinstance(raw_type, Mapping) else None
    if raw_codings is None:
        raw_codings = identifier.get("type_codes")
    if not isinstance(raw_codings, Sequence) or isinstance(
        raw_codings,
        (str, bytes, bytearray),
    ):
        return ()
    codings: set[tuple[str, str]] = set()
    for coding in raw_codings:
        if not isinstance(coding, Mapping):
            continue
        system = coding.get("system")
        code = coding.get("code")
        if type(system) is str and type(code) is str:
            codings.add((system, code))
    return tuple(sorted(codings))


def _has_identifier_match(
    identifier: Mapping[str, Any],
    *,
    systems: tuple[str, ...],
    type_codings: tuple[tuple[str, str], ...],
) -> bool:
    """Return whether one identifier matches an exact system or type selector."""

    system = identifier.get("system")
    return (
        type(system) is str
        and system in systems
        or bool(set(_identifier_type_codings(identifier)).intersection(type_codings))
    )


_identifier_matches = _has_identifier_match


def _as_utc_datetime(candidate: object) -> dt.datetime | None:
    if candidate is None:
        return None
    if isinstance(candidate, dt.datetime):
        value = candidate
    elif isinstance(candidate, dt.date):
        value = dt.datetime.combine(candidate, dt.time.min)
    else:
        raise _MalformedFhirIdentifierPeriod(
            "FHIR identifier observation time is invalid"
        )
    if value.tzinfo is None:
        return value.replace(tzinfo=dt.timezone.utc)
    return value.astimezone(dt.timezone.utc)


def canonical_evidence_as_of(candidate: object) -> str:
    """Normalize one generation-wide evidence cutoff to canonical UTC text."""

    if type(candidate) is str:
        if not candidate.endswith("Z"):
            raise TinNpiConnectorError("evidence cutoff is invalid")
        try:
            parsed = dt.datetime.fromisoformat(candidate[:-1] + "+00:00")
        except ValueError:
            raise TinNpiConnectorError("evidence cutoff is invalid") from None
        value = parsed.astimezone(dt.timezone.utc)
    else:
        try:
            candidate_value = _as_utc_datetime(candidate)
        except _MalformedFhirIdentifierPeriod:
            raise TinNpiConnectorError("evidence cutoff is invalid") from None
        if candidate_value is None:
            raise TinNpiConnectorError("evidence cutoff is invalid")
        value = candidate_value
    canonical = value.isoformat(timespec="microseconds").replace("+00:00", "Z")
    if type(candidate) is str and candidate != canonical:
        raise TinNpiConnectorError("evidence cutoff is invalid")
    return canonical


def _partial_date_bound(
    candidate: str,
    *,
    upper: bool,
) -> tuple[dt.datetime, bool] | None:
    match = _FHIR_DATE_PATTERN.fullmatch(candidate)
    if match is None:
        return None
    year = int(match.group("year"))
    month_text = match.group("month")
    day_text = match.group("day")
    try:
        if month_text is None:
            boundary = dt.datetime(year, 1, 1, tzinfo=dt.timezone.utc)
            if not upper:
                return boundary, True
            if year == dt.MAXYEAR:
                return dt.datetime.max.replace(tzinfo=dt.timezone.utc), True
            return boundary.replace(year=year + 1), False
        month = int(month_text)
        if day_text is None:
            boundary = dt.datetime(year, month, 1, tzinfo=dt.timezone.utc)
            if not upper:
                return boundary, True
            if month == 12:
                if year == dt.MAXYEAR:
                    return dt.datetime.max.replace(tzinfo=dt.timezone.utc), True
                return boundary.replace(year=year + 1, month=1), False
            return boundary.replace(month=month + 1), False
        boundary = dt.datetime(
            year,
            month,
            int(day_text),
            tzinfo=dt.timezone.utc,
        )
        if not upper:
            return boundary, True
        if boundary.date() == dt.date.max:
            return dt.datetime.max.replace(tzinfo=dt.timezone.utc), True
        return boundary + dt.timedelta(days=1), False
    except (OverflowError, ValueError):
        raise _MalformedFhirIdentifierPeriod(
            "FHIR identifier period is malformed"
        ) from None


def _exact_fhir_datetime_bound(
    candidate: str,
    *,
    upper: bool,
) -> tuple[dt.datetime, bool]:
    match = _FHIR_DATETIME_PATTERN.fullmatch(candidate)
    if match is None:
        raise _MalformedFhirIdentifierPeriod("FHIR identifier period is malformed")
    fraction = match.group("fraction") or ""
    microsecond = int((fraction + "000000")[:6])
    requires_microsecond_ceiling = (
        not upper and len(fraction) > 6 and any(digit != "0" for digit in fraction[6:])
    )
    zone = match.group("zone")
    if zone == "Z":
        timezone = dt.timezone.utc
    else:
        offset_components = zone[1:].split(":")
        offset_hour = int(offset_components[0])
        offset_minute = int(offset_components[1])
        offset = dt.timedelta(hours=offset_hour, minutes=offset_minute)
        timezone = dt.timezone(offset if zone[0] == "+" else -offset)
    second = int(match.group("second"))
    try:
        boundary = dt.datetime(
            int(match.group("year")),
            int(match.group("month")),
            int(match.group("day")),
            int(match.group("hour")),
            int(match.group("minute")),
            min(second, 59),
            microsecond,
            tzinfo=timezone,
        )
        if second == 60:
            boundary += dt.timedelta(seconds=1)
        boundary = boundary.astimezone(dt.timezone.utc)
        if requires_microsecond_ceiling:
            boundary += dt.timedelta(microseconds=1)
    except (OverflowError, ValueError):
        raise _MalformedFhirIdentifierPeriod(
            "FHIR identifier period is malformed"
        ) from None
    return boundary, True


def _fhir_period_bound(
    candidate: object,
    *,
    upper: bool,
) -> tuple[dt.datetime, bool] | None:
    if candidate is None:
        return None
    if type(candidate) is not str or not candidate or candidate != candidate.strip():
        raise _MalformedFhirIdentifierPeriod("FHIR identifier period is malformed")
    partial_date = _partial_date_bound(candidate, upper=upper)
    if partial_date is not None:
        return partial_date
    return _exact_fhir_datetime_bound(candidate, upper=upper)


def _is_identifier_effective(
    identifier: Mapping[str, Any],
    *,
    observed_at: dt.datetime | dt.date | None,
    policy: FhirTinNpiIdentifierRule,
) -> bool:
    """Return whether one identifier is usable at the generation cutoff."""

    identifier_use = identifier.get("use")
    if (
        type(identifier_use) is str
        and identifier_use in policy.excluded_identifier_uses
    ):
        return False
    raw_period = identifier.get("period")
    if raw_period is not None and not isinstance(raw_period, Mapping):
        raise _MalformedFhirIdentifierPeriod("FHIR identifier period is malformed")
    if isinstance(raw_period, Mapping):
        period_start = raw_period.get("start")
        period_end = raw_period.get("end")
    else:
        period_start = identifier.get("period_start")
        period_end = identifier.get("period_end")
    if period_start is None and period_end is None:
        return True
    observation = _as_utc_datetime(observed_at)
    if observation is None:
        raise _UnresolvedFhirIdentifierPeriod(
            "FHIR identifier period cannot be resolved"
        )
    start = _fhir_period_bound(period_start, upper=False)
    end = _fhir_period_bound(period_end, upper=True)
    if (
        start is not None
        and end is not None
        and (start[0] > end[0] or start[0] == end[0] and not end[1])
    ):
        raise _MalformedFhirIdentifierPeriod("FHIR identifier period is malformed")
    is_started_by_observation = start is None or start[0] <= observation
    if end is None:
        is_ended_after_observation = True
    elif end[1]:
        is_ended_after_observation = observation <= end[0]
    else:
        is_ended_after_observation = observation < end[0]
    return is_started_by_observation and is_ended_after_observation


_identifier_is_effective = _is_identifier_effective


def _normalize_npi(candidate: object) -> int:
    if type(candidate) is not str:
        raise TinNpiConnectorError("NPI is malformed")
    stripped = candidate.strip()
    if any(
        not character.isascii()
        or not (character.isdigit() or character in _ALLOWED_NPI_SEPARATORS)
        for character in stripped
    ):
        raise TinNpiConnectorError("NPI is malformed")
    digits = "".join(character for character in stripped if character.isdigit())
    if _NORMALIZED_NPI_PATTERN.fullmatch(digits) is None:
        raise TinNpiConnectorError("NPI is malformed")
    npi = int(digits)
    npi_digits = [int(digit) for digit in digits]
    digit_sum = _NPI_LUHN_PREFIX_DIGIT_SUM + npi_digits[-1]
    for position, digit in enumerate(npi_digits[:-1], start=1):
        if position % 2:
            doubled = digit * 2
            digit_sum += doubled - 9 if doubled > 9 else doubled
        else:
            digit_sum += digit
    if not _NPI_MIN <= npi <= _NPI_MAX or digit_sum % 10:
        raise TinNpiConnectorError("NPI is malformed")
    return npi
