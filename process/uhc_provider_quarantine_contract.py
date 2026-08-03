# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Privacy-safe contract for source-reported UHC provider rejections."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from typing import Any, Mapping


UHC_PROVIDER_QUARANTINE_FIELD = "_healthporta_quarantine"
UHC_PROVIDER_QUARANTINE_CONTRACT_ID = (
    "healthporta.uhc.provider-quarantine.v1"
)
UHC_PROVIDER_QUARANTINE_REASON_INVALID_NPI_CHECKSUM = (
    "invalid_npi_checksum"
)
UHC_PROVIDER_QUARANTINE_REASON_INVALID_NPI_STRUCTURE = (
    "invalid_npi_structure"
)
UHC_PROVIDER_QUARANTINE_REASONS = (
    UHC_PROVIDER_QUARANTINE_REASON_INVALID_NPI_CHECKSUM,
    UHC_PROVIDER_QUARANTINE_REASON_INVALID_NPI_STRUCTURE,
)
UHC_PROVIDER_QUARANTINE_MAX_COUNT = 32
UHC_PROVIDER_QUARANTINE_RATE_DENOMINATOR = 10_000
UHC_PROVIDER_QUARANTINE_REJECTED_COUNT_FIELDS = {
    "invalid_npi_checksum": "invalid_npi_count",
    "invalid_npi_checksum_individual_records": (
        "invalid_npi_individual_records"
    ),
    "invalid_npi_checksum_facility_records": (
        "invalid_npi_facility_records"
    ),
    "invalid_npi_checksum_address_rows": "invalid_npi_address_rows",
    "invalid_npi_checksum_provider_plan_rows": (
        "invalid_npi_provider_plan_rows"
    ),
    "invalid_npi_structure": "invalid_npi_structure_count",
    "invalid_npi_structure_individual_records": (
        "invalid_npi_structure_individual_records"
    ),
    "invalid_npi_structure_facility_records": (
        "invalid_npi_structure_facility_records"
    ),
    "invalid_npi_structure_address_rows": (
        "invalid_npi_structure_address_rows"
    ),
    "invalid_npi_structure_provider_plan_rows": (
        "invalid_npi_structure_provider_plan_rows"
    ),
}
UHC_PROVIDER_QUARANTINE_LEGACY_REJECTED_COUNT_FIELDS = frozenset(
    field_name
    for field_name in UHC_PROVIDER_QUARANTINE_REJECTED_COUNT_FIELDS
    if field_name == UHC_PROVIDER_QUARANTINE_REASON_INVALID_NPI_CHECKSUM
    or field_name.startswith(
        UHC_PROVIDER_QUARANTINE_REASON_INVALID_NPI_CHECKSUM + "_"
    )
)
UHC_PROVIDER_QUARANTINE_CURRENT_REJECTED_COUNT_FIELDS = frozenset(
    UHC_PROVIDER_QUARANTINE_REJECTED_COUNT_FIELDS
)
UHC_PROVIDER_QUARANTINE_COUNTER_BY_RAW_FIELD = {
    "raw_provider_records": "invalid_npi_count",
    "raw_individual_records": "invalid_npi_individual_records",
    "raw_facility_records": "invalid_npi_facility_records",
    "raw_address_rows": "invalid_npi_address_rows",
    "raw_provider_plan_rows": "invalid_npi_provider_plan_rows",
}

_SHA256_CHARACTERS = frozenset("0123456789abcdef")
_PAYLOAD_FIELDS = frozenset(
    {
        "contract_id",
        "reason",
        "source_file_id",
        "range_ordinal",
        "occurrence_ordinal",
        "record_sha256",
    }
)


class UhcProviderQuarantineError(ValueError):
    """Reject malformed, unbound, or excessive quarantine evidence."""


@dataclass(frozen=True)
class UhcProviderQuarantine:
    """Validated redacted identity for one rejected source occurrence."""

    source_file_id: str
    range_ordinal: int
    occurrence_ordinal: int
    record_sha256: str
    reason: str = UHC_PROVIDER_QUARANTINE_REASON_INVALID_NPI_CHECKSUM

    @property
    def identity_bytes(self) -> bytes:
        """Return cross-language deterministic identity bytes."""

        return json.dumps(
            [
                UHC_PROVIDER_QUARANTINE_CONTRACT_ID,
                self.source_file_id,
                self.range_ordinal,
                self.occurrence_ordinal,
                self.reason,
                self.record_sha256,
            ],
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode()


def _is_sha256(value: Any) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and set(value) <= _SHA256_CHARACTERS
    )


def _ordinal(value: Any, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise UhcProviderQuarantineError(
            f"UHC provider quarantine {field_name} is invalid"
        )
    return value


def provider_quarantine_limit(provider_count: int) -> int:
    """Allow one quarantine per started 10,000 records, capped at 32."""

    if (
        isinstance(provider_count, bool)
        or not isinstance(provider_count, int)
        or provider_count < 0
    ):
        raise UhcProviderQuarantineError(
            "UHC provider quarantine census is invalid"
        )
    if provider_count == 0:
        return 0
    rate_limit = (
        provider_count + UHC_PROVIDER_QUARANTINE_RATE_DENOMINATOR - 1
    ) // UHC_PROVIDER_QUARANTINE_RATE_DENOMINATOR
    return min(UHC_PROVIDER_QUARANTINE_MAX_COUNT, rate_limit)


def provider_quarantine_catalog_limit(provider_file_count: int) -> int:
    """Return the aggregate absolute ceiling after per-file rate checks."""

    if (
        isinstance(provider_file_count, bool)
        or not isinstance(provider_file_count, int)
        or provider_file_count < 0
    ):
        raise UhcProviderQuarantineError(
            "UHC provider quarantine file census is invalid"
        )
    return provider_file_count * UHC_PROVIDER_QUARANTINE_MAX_COUNT


def provider_quarantine_rejected_counts(
    counters: Mapping[str, Any],
) -> dict[str, int]:
    """Build the exact public aggregate map without source identities."""

    aggregate_by_dimension = {
        "count": _nonnegative_counter(counters, "invalid_npi_count"),
        "individual_records": _nonnegative_counter(
            counters, "invalid_npi_individual_records"
        ),
        "facility_records": _nonnegative_counter(
            counters, "invalid_npi_facility_records"
        ),
        "address_rows": _nonnegative_counter(
            counters, "invalid_npi_address_rows"
        ),
        "provider_plan_rows": _nonnegative_counter(
            counters, "invalid_npi_provider_plan_rows"
        ),
    }
    structural_by_dimension = _structural_counter_dimensions(counters)
    checksum_by_dimension = {
        dimension: aggregate_count - structural_by_dimension[dimension]
        for dimension, aggregate_count in aggregate_by_dimension.items()
    }
    if any(count < 0 for count in checksum_by_dimension.values()):
        raise UhcProviderQuarantineError(
            "UHC provider quarantine reason counters exceed aggregate census"
        )
    rejected_count_by_field = _public_reason_counts(
        UHC_PROVIDER_QUARANTINE_REASON_INVALID_NPI_CHECKSUM,
        checksum_by_dimension,
    )
    rejected_count_by_field.update(
        _public_reason_counts(
            UHC_PROVIDER_QUARANTINE_REASON_INVALID_NPI_STRUCTURE,
            structural_by_dimension,
        )
    )
    _validate_raw_counter_bounds(counters)
    if aggregate_by_dimension["count"] == 0:
        _validate_reason_count_balance(checksum_by_dimension)
        _validate_reason_count_balance(structural_by_dimension)
        return {}
    provider_quarantine_rejected_totals(
        rejected_count_by_field,
        aggregate_by_dimension["count"],
    )
    return rejected_count_by_field


_REASON_DIMENSIONS = (
    "individual_records",
    "facility_records",
    "address_rows",
    "provider_plan_rows",
)
_STRUCTURAL_COUNTER_BY_DIMENSION = {
    "count": "invalid_npi_structure_count",
    **{
        dimension: f"invalid_npi_structure_{dimension}"
        for dimension in _REASON_DIMENSIONS
    },
}


def _structural_counter_dimensions(
    counters: Mapping[str, Any],
) -> dict[str, int]:
    counter_presence_set = {
        counter_field in counters
        for counter_field in _STRUCTURAL_COUNTER_BY_DIMENSION.values()
    }
    if counter_presence_set == {False}:
        return dict.fromkeys(_STRUCTURAL_COUNTER_BY_DIMENSION, 0)
    if counter_presence_set != {True}:
        raise UhcProviderQuarantineError(
            "UHC provider quarantine structural counters are incomplete"
        )
    return {
        dimension: _nonnegative_counter(counters, counter_field)
        for dimension, counter_field in (
            _STRUCTURAL_COUNTER_BY_DIMENSION.items()
        )
    }


def _public_reason_counts(
    reason: str,
    count_by_dimension: Mapping[str, int],
) -> dict[str, int]:
    return {
        reason: count_by_dimension["count"],
        **{
            f"{reason}_{dimension}": count_by_dimension[dimension]
            for dimension in _REASON_DIMENSIONS
        },
    }


def _public_rejection_reasons(
    rejected_count_by_field: Mapping[str, Any],
) -> tuple[str, ...]:
    field_name_set = set(rejected_count_by_field)
    if field_name_set == set(
        UHC_PROVIDER_QUARANTINE_LEGACY_REJECTED_COUNT_FIELDS
    ):
        return (UHC_PROVIDER_QUARANTINE_REASON_INVALID_NPI_CHECKSUM,)
    if field_name_set == set(
        UHC_PROVIDER_QUARANTINE_CURRENT_REJECTED_COUNT_FIELDS
    ):
        return UHC_PROVIDER_QUARANTINE_REASONS
    raise UhcProviderQuarantineError(
        "UHC provider quarantine rejection scope is invalid"
    )


def _public_reason_dimensions(
    rejected_count_by_field: Mapping[str, Any],
    reason: str,
) -> dict[str, int]:
    count_by_dimension: dict[str, int] = {}
    for dimension in ("count", *_REASON_DIMENSIONS):
        field_name = reason if dimension == "count" else f"{reason}_{dimension}"
        raw_count = rejected_count_by_field.get(field_name)
        if (
            isinstance(raw_count, bool)
            or not isinstance(raw_count, int)
            or raw_count < 0
        ):
            raise UhcProviderQuarantineError(
                "UHC provider quarantine public dimensions are invalid"
            )
        count_by_dimension[dimension] = raw_count
    _validate_reason_count_balance(count_by_dimension)
    return count_by_dimension


def _validate_expected_rejection_count(expected_invalid_npi_count: Any) -> int:
    if (
        isinstance(expected_invalid_npi_count, bool)
        or not isinstance(expected_invalid_npi_count, int)
        or expected_invalid_npi_count < 0
    ):
        raise UhcProviderQuarantineError(
            "UHC provider quarantine expected census is invalid"
        )
    return expected_invalid_npi_count


def provider_quarantine_rejected_totals(
    rejected_count_by_field: Mapping[str, Any],
    expected_invalid_npi_count: int,
) -> dict[str, int]:
    """Validate a public rejection map and total its exact row dimensions."""

    expected_count = _validate_expected_rejection_count(
        expected_invalid_npi_count
    )
    if not rejected_count_by_field:
        if expected_count:
            raise UhcProviderQuarantineError(
                "UHC provider quarantine rejection map is incomplete"
            )
        return dict.fromkeys(_REASON_DIMENSIONS, 0)
    if expected_count == 0:
        raise UhcProviderQuarantineError(
            "UHC provider quarantine zero census must have no rejection map"
        )
    count_by_reason = {
        reason: _public_reason_dimensions(rejected_count_by_field, reason)
        for reason in _public_rejection_reasons(rejected_count_by_field)
    }
    if sum(
        dimension_counts["count"]
        for dimension_counts in count_by_reason.values()
    ) != expected_count:
        raise UhcProviderQuarantineError(
            "UHC provider quarantine public counts do not balance"
        )
    return {
        dimension: sum(
            dimension_counts[dimension]
            for dimension_counts in count_by_reason.values()
        )
        for dimension in _REASON_DIMENSIONS
    }


def _nonnegative_counter(
    counters: Mapping[str, Any],
    counter_field: str,
) -> int:
    counter_count = counters.get(counter_field)
    if (
        isinstance(counter_count, bool)
        or not isinstance(counter_count, int)
        or counter_count < 0
    ):
        raise UhcProviderQuarantineError(
            f"UHC provider quarantine {counter_field} is invalid"
        )
    return counter_count


def _validate_raw_counter_bounds(counters: Mapping[str, Any]) -> None:
    for raw_field, quarantine_field in (
        UHC_PROVIDER_QUARANTINE_COUNTER_BY_RAW_FIELD.items()
    ):
        raw_count = counters.get(raw_field)
        quarantine_count = counters.get(quarantine_field)
        if (
            isinstance(raw_count, bool)
            or not isinstance(raw_count, int)
            or raw_count < 0
            or quarantine_count > raw_count
        ):
            raise UhcProviderQuarantineError(
                "UHC provider quarantine counters exceed raw census"
            )


def _validate_reason_count_balance(
    count_by_dimension: Mapping[str, int],
) -> None:
    invalid_count = count_by_dimension["count"]
    individual_count = count_by_dimension["individual_records"]
    facility_count = count_by_dimension["facility_records"]
    address_count = count_by_dimension["address_rows"]
    plan_count = count_by_dimension["provider_plan_rows"]
    if (
        individual_count + facility_count != invalid_count
        or address_count < invalid_count
        or plan_count < invalid_count
        or (invalid_count == 0 and any(count_by_dimension.values()))
    ):
        raise UhcProviderQuarantineError(
            "UHC provider quarantine counters do not balance"
        )


def validate_provider_quarantine_fact(
    raw_fact: Any,
    *,
    expected_source_file_id: str,
    expected_range_ordinal: int,
    expected_occurrence_ordinal: int,
) -> UhcProviderQuarantine | None:
    """Recognize only the exact redacted wrapper and bind its lineage."""

    if not isinstance(raw_fact, Mapping):
        raise UhcProviderQuarantineError(
            "UHC provider quarantine fact is not an object"
        )
    if UHC_PROVIDER_QUARANTINE_FIELD not in raw_fact:
        return None
    if set(raw_fact) != {UHC_PROVIDER_QUARANTINE_FIELD}:
        raise UhcProviderQuarantineError(
            "UHC provider quarantine wrapper is invalid"
        )
    quarantine_by_field = raw_fact[UHC_PROVIDER_QUARANTINE_FIELD]
    if (
        not isinstance(quarantine_by_field, Mapping)
        or set(quarantine_by_field) != _PAYLOAD_FIELDS
    ):
        raise UhcProviderQuarantineError(
            "UHC provider quarantine payload is invalid"
        )
    if (
        quarantine_by_field.get("contract_id")
        != UHC_PROVIDER_QUARANTINE_CONTRACT_ID
        or quarantine_by_field.get("reason") not in UHC_PROVIDER_QUARANTINE_REASONS
        or not _is_sha256(quarantine_by_field.get("source_file_id"))
        or not _is_sha256(quarantine_by_field.get("record_sha256"))
    ):
        raise UhcProviderQuarantineError(
            "UHC provider quarantine identity is invalid"
        )
    quarantine = UhcProviderQuarantine(
        source_file_id=str(quarantine_by_field["source_file_id"]),
        range_ordinal=_ordinal(
            quarantine_by_field["range_ordinal"],
            "range ordinal",
        ),
        occurrence_ordinal=_ordinal(
            quarantine_by_field["occurrence_ordinal"],
            "occurrence ordinal",
        ),
        record_sha256=str(quarantine_by_field["record_sha256"]),
        reason=str(quarantine_by_field["reason"]),
    )
    if (
        quarantine.source_file_id != expected_source_file_id
        or quarantine.range_ordinal != expected_range_ordinal
        or quarantine.occurrence_ordinal != expected_occurrence_ordinal
    ):
        raise UhcProviderQuarantineError(
            "UHC provider quarantine lineage mismatch"
        )
    return quarantine


def quarantine_identity_set_sha256(
    quarantine_by_identity: list[UhcProviderQuarantine],
) -> str:
    """Hash one globally ordered quarantine identity sequence."""

    digest = hashlib.sha256()
    for index, quarantine in enumerate(quarantine_by_identity):
        if index:
            digest.update(b"\n")
        digest.update(quarantine.identity_bytes)
    return digest.hexdigest()
