# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact six-field NPPES enumeration row normalization."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import re

from public_evidence.nppes_registry_error import replay_error


NPI_HEADER = "NPI"
ENTITY_TYPE_HEADER = "Entity Type Code"
ENUMERATION_DATE_HEADER = "Provider Enumeration Date"
LAST_UPDATE_DATE_HEADER = "Last Update Date"
DEACTIVATION_DATE_HEADER = "NPI Deactivation Date"
REACTIVATION_DATE_HEADER = "NPI Reactivation Date"
PROJECTION_HEADERS = (
    NPI_HEADER,
    ENTITY_TYPE_HEADER,
    ENUMERATION_DATE_HEADER,
    LAST_UPDATE_DATE_HEADER,
    DEACTIVATION_DATE_HEADER,
    REACTIVATION_DATE_HEADER,
)
REQUIRED_HEADERS = frozenset(PROJECTION_HEADERS)
_NPI_RE = re.compile(r"[0-9]{10}", flags=re.ASCII)


@dataclass(frozen=True, slots=True, repr=False)
class _NppesRegistryProjection:
    """Canonical six-field payload and v1 eligibility classification."""

    payload_fields: tuple[tuple[str, object], ...]
    npi: str
    entity_type_code: str | None
    provider_enumeration_date: str | None
    last_update_date: str | None
    npi_deactivation_date: str | None
    npi_reactivation_date: str | None
    npi_entity_type: str | None
    enumeration_state: str
    effective_start_at: str | None
    exclusion_reason: str | None


def _strict_npi(value: object) -> str:
    if type(value) is not str or _NPI_RE.fullmatch(value) is None:
        raise replay_error()
    if not 1_000_000_000 <= int(value) <= 2_999_999_999:
        raise replay_error()
    digits = [int(character) for character in "80840" + value]
    for offset in range(1, len(digits), 2):
        doubled = digits[-1 - offset] * 2
        digits[-1 - offset] = doubled // 10 + doubled % 10
    if sum(digits) % 10:
        raise replay_error()
    return value


def _is_missing(value: str) -> bool:
    return value == "" or value.upper() == "<UNAVAIL>"


def _strict_source_date(value: str) -> datetime | None:
    if _is_missing(value):
        return None
    try:
        return datetime.strptime(value, "%m/%d/%Y")
    except ValueError:
        raise replay_error() from None


def _canonical_day(value: datetime) -> str:
    return value.strftime("%Y-%m-%dT00:00:00Z")


def _canonical_date(value: datetime) -> str:
    return value.strftime("%Y-%m-%d")


def _entity_type(entity_code: str) -> tuple[str | None, str | None]:
    if _is_missing(entity_code):
        return None, None
    if entity_code == "1":
        return "individual_type_1", entity_code
    if entity_code == "2":
        return "organization_type_2", entity_code
    raise replay_error()


def _effective_state(
    enumeration: datetime | None,
    deactivation: datetime | None,
    reactivation: datetime | None,
) -> tuple[str, datetime | None]:
    if reactivation is not None and (
        deactivation is None or reactivation <= deactivation
    ):
        raise replay_error()
    is_reactivated = (
        deactivation is not None
        and reactivation is not None
        and reactivation > deactivation
    )
    if deactivation is not None and not is_reactivated:
        return "deactivated", deactivation
    return "active", reactivation if is_reactivated else enumeration


def _canonical_payload(
    payload_contract: str,
    source_fields_by_name: dict[str, str],
    npi: str,
    canonical_entity_code: str | None,
    source_dates: tuple[
        datetime | None,
        datetime | None,
        datetime | None,
        datetime | None,
    ],
) -> dict[str, object]:
    enumeration, last_update, deactivation, reactivation = source_dates
    return {
        "contract": payload_contract,
        "entity_type_code": canonical_entity_code,
        "last_update_date": _canonical_date(last_update) if last_update else None,
        "npi": npi,
        "npi_deactivation_date": (
            _canonical_date(deactivation) if deactivation else None
        ),
        "npi_reactivation_date": (
            _canonical_date(reactivation) if reactivation else None
        ),
        "provider_enumeration_date": (
            _canonical_date(enumeration) if enumeration else None
        ),
    }


def _temporal_projection(
    snapshot: datetime,
    source_dates: tuple[
        datetime | None,
        datetime | None,
        datetime | None,
        datetime | None,
    ],
    entity_type: str | None,
) -> tuple[str, str | None, str | None]:
    if any(
        source_date is not None and source_date > snapshot
        for source_date in source_dates
    ):
        raise replay_error()
    enumeration, _, deactivation, reactivation = source_dates
    if enumeration is not None and any(
        event is not None and enumeration > event
        for event in (deactivation, reactivation)
    ):
        raise replay_error()
    enumeration_state, effective_start = _effective_state(
        enumeration,
        deactivation,
        reactivation,
    )
    if effective_start is None:
        return enumeration_state, None, "effective_start_not_disclosed"
    exclusion_reason = "entity_type_not_disclosed" if entity_type is None else None
    return enumeration_state, _canonical_day(effective_start), exclusion_reason


def _project_nppes_registry_row(
    snapshot_at: str,
    projected_values: tuple[str, ...],
    payload_contract: str,
) -> _NppesRegistryProjection:
    """Normalize the exact bounded source projection into frozen v1 semantics."""

    snapshot = datetime.strptime(snapshot_at, "%Y-%m-%dT%H:%M:%SZ")
    source_fields_by_name = dict(zip(PROJECTION_HEADERS, projected_values, strict=True))
    npi = _strict_npi(source_fields_by_name[NPI_HEADER])
    entity_type, canonical_entity_code = _entity_type(
        source_fields_by_name[ENTITY_TYPE_HEADER]
    )
    source_dates = (
        _strict_source_date(source_fields_by_name[ENUMERATION_DATE_HEADER]),
        _strict_source_date(source_fields_by_name[LAST_UPDATE_DATE_HEADER]),
        _strict_source_date(source_fields_by_name[DEACTIVATION_DATE_HEADER]),
        _strict_source_date(source_fields_by_name[REACTIVATION_DATE_HEADER]),
    )
    enumeration_state, canonical_start, exclusion_reason = _temporal_projection(
        snapshot,
        source_dates,
        entity_type,
    )
    payload_by_name = _canonical_payload(
        payload_contract,
        source_fields_by_name,
        npi,
        canonical_entity_code,
        source_dates,
    )
    return _NppesRegistryProjection(
        payload_fields=tuple(payload_by_name.items()),
        npi=npi,
        entity_type_code=payload_by_name["entity_type_code"],
        provider_enumeration_date=payload_by_name["provider_enumeration_date"],
        last_update_date=payload_by_name["last_update_date"],
        npi_deactivation_date=payload_by_name["npi_deactivation_date"],
        npi_reactivation_date=payload_by_name["npi_reactivation_date"],
        npi_entity_type=entity_type,
        enumeration_state=enumeration_state,
        effective_start_at=canonical_start,
        exclusion_reason=exclusion_reason,
    )


__all__ = (
    "DEACTIVATION_DATE_HEADER",
    "ENTITY_TYPE_HEADER",
    "ENUMERATION_DATE_HEADER",
    "LAST_UPDATE_DATE_HEADER",
    "NPI_HEADER",
    "PROJECTION_HEADERS",
    "REACTIVATION_DATE_HEADER",
    "REQUIRED_HEADERS",
)
