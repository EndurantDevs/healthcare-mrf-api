# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Independent strict validation for checksum-invalid UHC provider records."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Iterable, Mapping


_PROVIDER_FIELDS = frozenset(
    {
        "type",
        "npi",
        "name",
        "facility_name",
        "facility_type",
        "gender",
        "accepting",
        "addresses",
        "plans",
        "specialty",
        "last_updated_on",
    }
)
_PROVIDER_REQUIRED_FIELDS = frozenset({"type", "npi", "addresses", "plans"})
_NAME_FIELDS = frozenset({"first", "middle", "last"})
_ADDRESS_FIELDS = frozenset({"address", "city", "state", "zip", "phone"})
_PLAN_FIELDS = frozenset({"plan_id_type", "plan_id", "years", "network_tier"})
_SUPPORTED_PROVIDER_TYPES = frozenset({"INDIVIDUAL", "FACILITY"})
_SUPPORTED_ACCEPTING_CODES = frozenset(
    {
        "ACCEPTING",
        "ACCEPTING_NEW_PATIENTS",
        "YES",
        "NOT_ACCEPTING",
        "NO",
        "CLOSED",
    }
)
class UhcProviderQuarantineRecordError(ValueError):
    """Reject a record with any defect beyond its NPI checksum."""


@dataclass(frozen=True)
class UhcProviderQuarantineRecordCensus:
    """Exact rejected dimensions independently recovered from raw bytes."""

    individual_records: int = 0
    facility_records: int = 0
    address_rows: int = 0
    provider_plan_rows: int = 0

    @property
    def counter_map(self) -> dict[str, int]:
        """Return native counter names at canonical plan-year grain."""

        return {
            "invalid_npi_individual_records": self.individual_records,
            "invalid_npi_facility_records": self.facility_records,
            "invalid_npi_address_rows": self.address_rows,
            "invalid_npi_provider_plan_rows": self.provider_plan_rows,
        }


def combine_provider_quarantine_census(
    census_values: Iterable[UhcProviderQuarantineRecordCensus],
) -> UhcProviderQuarantineRecordCensus:
    """Combine a bounded sparse census without changing its row grain."""

    individual_records = 0
    facility_records = 0
    address_rows = 0
    provider_plan_rows = 0
    for census_value in census_values:
        individual_records += census_value.individual_records
        facility_records += census_value.facility_records
        address_rows += census_value.address_rows
        provider_plan_rows += census_value.provider_plan_rows
    return UhcProviderQuarantineRecordCensus(
        individual_records=individual_records,
        facility_records=facility_records,
        address_rows=address_rows,
        provider_plan_rows=provider_plan_rows,
    )


def _clean_text(raw_text: Any, *, upper: bool = False) -> str | None:
    if raw_text is None:
        return None
    if not isinstance(raw_text, str):
        raise UhcProviderQuarantineRecordError(
            "UHC provider quarantine string field is invalid"
        )
    cleaned_text = raw_text.replace("\0", "").strip()
    if not cleaned_text:
        return None
    return cleaned_text.upper() if upper else cleaned_text


def _validate_optional_string_fields(
    record_by_field: Mapping[str, Any],
    field_names: frozenset[str],
) -> None:
    for field_name in field_names:
        if field_name in record_by_field:
            _clean_text(record_by_field[field_name])


def _validate_optional_string_list(raw_value: Any) -> None:
    if raw_value is None:
        return
    if not isinstance(raw_value, list) or not all(
        isinstance(list_value, str) for list_value in raw_value
    ):
        raise UhcProviderQuarantineRecordError(
            "UHC provider quarantine string list is invalid"
        )


def _validate_name(raw_name: Any) -> None:
    if raw_name is None:
        return
    if not isinstance(raw_name, Mapping) or not set(raw_name) <= _NAME_FIELDS:
        raise UhcProviderQuarantineRecordError(
            "UHC provider quarantine name is invalid"
        )
    _validate_optional_string_fields(raw_name, _NAME_FIELDS)


def _validate_addresses(raw_addresses: Any) -> int:
    if not isinstance(raw_addresses, list) or not raw_addresses:
        raise UhcProviderQuarantineRecordError(
            "UHC provider quarantine addresses are invalid"
        )
    for address_by_field in raw_addresses:
        if (
            not isinstance(address_by_field, Mapping)
            or not set(address_by_field) <= _ADDRESS_FIELDS
        ):
            raise UhcProviderQuarantineRecordError(
                "UHC provider quarantine address is invalid"
            )
        _validate_optional_string_fields(address_by_field, _ADDRESS_FIELDS)
    return len(raw_addresses)


def _validate_plan_years(raw_years: Any) -> None:
    if (
        not isinstance(raw_years, list)
        or not raw_years
        or any(
            isinstance(plan_year, bool)
            or not isinstance(plan_year, int)
            or not 2000 <= plan_year <= 2100
            for plan_year in raw_years
        )
    ):
        raise UhcProviderQuarantineRecordError(
            "UHC provider quarantine plan years are invalid"
        )


def _validate_plans(raw_plans: Any) -> int:
    if not isinstance(raw_plans, list) or not raw_plans:
        raise UhcProviderQuarantineRecordError(
            "UHC provider quarantine plans are invalid"
        )
    provider_plan_rows = 0
    for plan_by_field in raw_plans:
        if (
            not isinstance(plan_by_field, Mapping)
            or set(plan_by_field) - _PLAN_FIELDS
            or "years" not in plan_by_field
        ):
            raise UhcProviderQuarantineRecordError(
                "UHC provider quarantine plan is invalid"
            )
        plan_id_type = _clean_text(plan_by_field.get("plan_id_type"))
        plan_id = _clean_text(plan_by_field.get("plan_id"))
        _clean_text(plan_by_field.get("network_tier"))
        if plan_id_type is None or plan_id is None:
            raise UhcProviderQuarantineRecordError(
                "UHC provider quarantine plan is invalid"
            )
        _validate_plan_years(plan_by_field["years"])
        provider_plan_rows += len(plan_by_field["years"])
    return provider_plan_rows


def is_checksum_invalid_npi(raw_npi: Any) -> bool:
    """Return true only for a structurally valid CMS-range checksum miss."""

    if (
        not isinstance(raw_npi, str)
        or len(raw_npi) != 10
        or not raw_npi.isascii()
        or not raw_npi.isdigit()
    ):
        return False
    numeric_npi = int(raw_npi)
    if not 1_000_000_000 <= numeric_npi <= 2_999_999_999:
        return False
    npi_digits = [int(digit) for digit in raw_npi]
    digit_sum = 24 + npi_digits[9]
    for digit_index, npi_digit in enumerate(npi_digits[:9]):
        if digit_index % 2 == 0:
            doubled_digit = npi_digit * 2
            digit_sum += (
                doubled_digit - 9 if doubled_digit > 9 else doubled_digit
            )
        else:
            digit_sum += npi_digit
    return not digit_sum % 10 == 0


def validate_checksum_invalid_provider_record(
    raw_record: Any,
) -> UhcProviderQuarantineRecordCensus:
    """Replay every native semantic predicate that precedes quarantine."""

    if (
        not isinstance(raw_record, Mapping)
        or set(raw_record) - _PROVIDER_FIELDS
        or not _PROVIDER_REQUIRED_FIELDS <= set(raw_record)
    ):
        raise UhcProviderQuarantineRecordError(
            "UHC provider quarantine record shape is invalid"
        )
    provider_type = _clean_text(raw_record.get("type"), upper=True)
    if provider_type not in _SUPPORTED_PROVIDER_TYPES:
        raise UhcProviderQuarantineRecordError(
            "UHC provider quarantine type is invalid"
        )
    if not is_checksum_invalid_npi(raw_record.get("npi")):
        raise UhcProviderQuarantineRecordError(
            "UHC provider quarantine is not checksum-invalid-only"
        )
    _validate_name(raw_record.get("name"))
    _validate_optional_string_fields(
        raw_record,
        frozenset({"facility_name", "gender", "last_updated_on"}),
    )
    _validate_optional_string_list(raw_record.get("facility_type"))
    _validate_optional_string_list(raw_record.get("specialty"))
    accepting_text = _clean_text(raw_record.get("accepting"), upper=True)
    if accepting_text is not None:
        accepting_code = "_".join(
            part for part in re.split(r"[-\s]+", accepting_text) if part
        )
        if accepting_code not in _SUPPORTED_ACCEPTING_CODES:
            raise UhcProviderQuarantineRecordError(
                "UHC provider quarantine accepting status is invalid"
            )
    address_rows = _validate_addresses(raw_record.get("addresses"))
    provider_plan_rows = _validate_plans(raw_record.get("plans"))
    return UhcProviderQuarantineRecordCensus(
        individual_records=int(provider_type == "INDIVIDUAL"),
        facility_records=int(provider_type == "FACILITY"),
        address_rows=address_rows,
        provider_plan_rows=provider_plan_rows,
    )
