# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Normalize closed billing-search query values without retaining aliases."""

from __future__ import annotations

from collections.abc import Mapping
from decimal import Decimal, InvalidOperation
import re
from typing import Any

from api.billing_search_cursor import (
    BILLING_SEARCH_CURSOR_MAX_CHARACTERS,
    BILLING_SEARCH_CURSOR_PREFIX,
)
from api.billing_search_transport_contract import (
    _canonical_json_bytes,
    _framed_sha256,
    normalize_billing_search_query_pairs,
)
from api.plan_release_serving import normalize_plan_release_id
from api.ptg2_billing_entity_refs import decode_billing_entity_ref
from api.ptg2_capacity_evidence import (
    CapacityEvidenceError,
    normalize_capacity_code,
    normalize_capacity_code_system,
)
from process.provider_directory_profile import is_valid_npi

BILLING_SEARCH_MAX_LIMIT = 100
BILLING_SEARCH_MAX_RADIUS_MILES = 100.0

_REQUEST_FINGERPRINT_DOMAIN = b"HEALTHPORTA_BILLING_SEARCH_REQUEST_V1\x00"
_INVALID = "billing_search_request_invalid"
_DECIMAL_PATTERN = re.compile(r"-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?", flags=re.ASCII)
_LIMIT_PATTERN = re.compile(r"[1-9][0-9]{0,2}", flags=re.ASCII)
_NPI_PATTERN = re.compile(r"[0-9]{10}", flags=re.ASCII)
_ZIP5_PATTERN = re.compile(r"[0-9]{5}", flags=re.ASCII)
_MODIFIER_PATTERN = re.compile(r"[A-Z0-9]{1,8}", flags=re.ASCII)
_PLACE_OF_SERVICE_PATTERN = re.compile(r"[0-9]{2}", flags=re.ASCII)
_CURSOR_PATTERN = re.compile(r"[A-Za-z0-9_-]+", flags=re.ASCII)
_ALLOWED_PARAMETERS = frozenset(
    {
        "billing_entity_ref",
        "code",
        "code_system",
        "cursor",
        "include_evidence",
        "lat",
        "limit",
        "long",
        "modifiers",
        "npi",
        "place_of_service",
        "plan_release_id",
        "radius_miles",
        "zip5",
    }
)
_REQUIRED_PARAMETERS = frozenset(
    {
        "billing_entity_ref",
        "code",
        "code_system",
        "limit",
        "plan_release_id",
    }
)


class BillingSearchRequestError(ValueError):
    """Value-free request rejection safe for the API boundary."""


def request_failure() -> BillingSearchRequestError:
    """Return one value-free billing-search request failure."""

    return BillingSearchRequestError(_INVALID)


def _mapping_items(parameters: Mapping[str, Any]) -> list[tuple[Any, Any]]:
    items = getattr(parameters, "items", None)
    if not callable(items):
        raise request_failure()
    try:
        return list(items(multi=True))
    except TypeError:
        try:
            return list(items())
        except Exception:
            raise request_failure() from None
    except Exception:
        raise request_failure() from None


def _accessor_values(
    parameters: Mapping[str, Any],
    field_name: str,
) -> list[Any] | None:
    for accessor_name in ("getall", "getlist"):
        accessor = getattr(parameters, accessor_name, None)
        if not callable(accessor):
            continue
        try:
            return list(accessor(field_name))
        except (KeyError, TypeError):
            continue
        except Exception:
            raise request_failure() from None
    return None


def _single_parameter(parameters: Mapping[str, Any], field_name: str) -> Any:
    accessor_values = _accessor_values(parameters, field_name)
    if accessor_values is not None:
        if len(accessor_values) != 1:
            raise request_failure()
        return accessor_values[0]
    try:
        value = parameters.get(field_name)
    except Exception:
        raise request_failure() from None
    if isinstance(value, (dict, list, set, tuple)):
        raise request_failure()
    return value


def _closed_parameter_names(
    parameters: Mapping[str, Any],
) -> tuple[str, ...]:
    parameter_names = tuple(name for name, _value in _mapping_items(parameters))
    if (
        not parameter_names
        or any(type(name) is not str or not name for name in parameter_names)
        or len(parameter_names) != len(set(parameter_names))
    ):
        raise request_failure()
    name_set = frozenset(parameter_names)
    if not _REQUIRED_PARAMETERS.issubset(name_set) or not name_set.issubset(
        _ALLOWED_PARAMETERS
    ):
        raise request_failure()
    return tuple(sorted(parameter_names))


def _strict_ascii_scalar(
    value: object,
    *,
    maximum_characters: int = 512,
) -> str:
    if (
        type(value) is not str
        or not 1 <= len(value) <= maximum_characters
        or value != value.strip()
        or not value.isascii()
        or not value.isprintable()
    ):
        raise request_failure()
    return value


def _canonical_plan_release(value: object) -> str:
    release_id = _strict_ascii_scalar(value)
    if normalize_plan_release_id(release_id) != release_id:
        raise request_failure()
    return release_id


def _canonical_billing_entity_ref(value: object) -> str:
    billing_entity_ref = _strict_ascii_scalar(value)
    try:
        decode_billing_entity_ref(billing_entity_ref)
    except Exception:
        raise request_failure() from None
    return billing_entity_ref


def _canonical_code_fields(
    code_system_value: object,
    code_value: object,
) -> tuple[str, str]:
    code_system = _strict_ascii_scalar(code_system_value)
    code = _strict_ascii_scalar(code_value)
    try:
        normalized_system = normalize_capacity_code_system(code_system)
        normalized_code = normalize_capacity_code(normalized_system, code)
    except CapacityEvidenceError:
        raise request_failure() from None
    if normalized_system != code_system or normalized_code != code:
        raise request_failure()
    return code_system, code


def _canonical_decimal(
    value: object,
    *,
    minimum: Decimal,
    maximum: Decimal,
) -> float:
    encoded_value = _strict_ascii_scalar(value, maximum_characters=2048)
    if _DECIMAL_PATTERN.fullmatch(encoded_value) is None:
        raise request_failure()
    try:
        decimal_value = Decimal(encoded_value)
    except InvalidOperation:
        raise request_failure() from None
    if not decimal_value.is_finite() or not minimum <= decimal_value <= maximum:
        raise request_failure()
    return float(decimal_value)


def _canonical_geo(
    values_by_name: Mapping[str, Any],
) -> tuple[str | None, float | None, float | None, float | None]:
    has_zip = "zip5" in values_by_name
    has_lat = "lat" in values_by_name
    has_long = "long" in values_by_name
    has_radius = "radius_miles" in values_by_name
    if has_zip:
        if has_lat or has_long or has_radius:
            raise request_failure()
        zip5 = _strict_ascii_scalar(values_by_name["zip5"])
        if _ZIP5_PATTERN.fullmatch(zip5) is None:
            raise request_failure()
        return zip5, None, None, None
    if not (has_lat and has_long and has_radius):
        raise request_failure()
    return (
        None,
        _canonical_decimal(
            values_by_name["lat"], minimum=Decimal("-90"), maximum=Decimal("90")
        ),
        _canonical_decimal(
            values_by_name["long"],
            minimum=Decimal("-180"),
            maximum=Decimal("180"),
        ),
        _canonical_decimal(
            values_by_name["radius_miles"],
            minimum=Decimal("0"),
            maximum=Decimal(str(BILLING_SEARCH_MAX_RADIUS_MILES)),
        ),
    )


def _canonical_code_list(
    value: object,
    *,
    pattern: re.Pattern[str],
    maximum_count: int,
) -> tuple[str, ...]:
    encoded_value = _strict_ascii_scalar(value)
    values = tuple(encoded_value.split(","))
    if (
        not 1 <= len(values) <= maximum_count
        or any(pattern.fullmatch(member) is None for member in values)
        or values != tuple(sorted(set(values)))
    ):
        raise request_failure()
    return values


def _optional_npi(values_by_name: Mapping[str, Any]) -> int | None:
    if "npi" not in values_by_name:
        return None
    encoded_npi = _strict_ascii_scalar(values_by_name["npi"])
    if _NPI_PATTERN.fullmatch(encoded_npi) is None or not is_valid_npi(encoded_npi):
        raise request_failure()
    return int(encoded_npi)


def _include_evidence(values_by_name: Mapping[str, Any]) -> bool:
    if "include_evidence" not in values_by_name:
        return False
    encoded_value = _strict_ascii_scalar(values_by_name["include_evidence"])
    if encoded_value not in {"false", "true"}:
        raise request_failure()
    return encoded_value == "true"


def _page_limit(values_by_name: Mapping[str, Any]) -> int:
    encoded_limit = _strict_ascii_scalar(values_by_name["limit"])
    if _LIMIT_PATTERN.fullmatch(encoded_limit) is None:
        raise request_failure()
    limit = int(encoded_limit)
    if not 1 <= limit <= BILLING_SEARCH_MAX_LIMIT:
        raise request_failure()
    return limit


def _optional_cursor(values_by_name: Mapping[str, Any]) -> str | None:
    if "cursor" not in values_by_name:
        return None
    token = _strict_ascii_scalar(
        values_by_name["cursor"],
        maximum_characters=BILLING_SEARCH_CURSOR_MAX_CHARACTERS,
    )
    if (
        not token.startswith(f"{BILLING_SEARCH_CURSOR_PREFIX}_")
        or _CURSOR_PATTERN.fullmatch(token) is None
    ):
        raise request_failure()
    return token


def _query_values(
    parameters: Mapping[str, Any],
) -> tuple[dict[str, Any], tuple[tuple[str, str], ...]]:
    parameter_names = _closed_parameter_names(parameters)
    values_by_name = {
        parameter_name: _single_parameter(parameters, parameter_name)
        for parameter_name in parameter_names
    }
    query_pairs = normalize_billing_search_query_pairs(
        tuple(
            (
                parameter_name,
                _strict_ascii_scalar(
                    values_by_name[parameter_name], maximum_characters=2048
                ),
            )
            for parameter_name in parameter_names
        )
    )
    return values_by_name, query_pairs


def _request_fingerprint_sha256(
    query_pairs: tuple[tuple[str, str], ...],
) -> str:
    filter_pairs = tuple(
        query_pair for query_pair in query_pairs if query_pair[0] != "cursor"
    )
    return _framed_sha256(
        _REQUEST_FINGERPRINT_DOMAIN,
        _canonical_json_bytes(filter_pairs),
    )


def _optional_filter_fields(values_by_name: Mapping[str, Any]) -> dict[str, Any]:
    modifiers = (
        _canonical_code_list(
            values_by_name["modifiers"],
            pattern=_MODIFIER_PATTERN,
            maximum_count=8,
        )
        if "modifiers" in values_by_name
        else ()
    )
    place_of_service = (
        _canonical_code_list(
            values_by_name["place_of_service"],
            pattern=_PLACE_OF_SERVICE_PATTERN,
            maximum_count=16,
        )
        if "place_of_service" in values_by_name
        else ()
    )
    return {
        "provider_npi": _optional_npi(values_by_name),
        "modifiers": modifiers,
        "place_of_service": place_of_service,
        "include_evidence": _include_evidence(values_by_name),
        "limit": _page_limit(values_by_name),
        "cursor": _optional_cursor(values_by_name),
    }


def _request_fields(
    values_by_name: Mapping[str, Any],
    query_pairs: tuple[tuple[str, str], ...],
) -> dict[str, Any]:
    code_system, code = _canonical_code_fields(
        values_by_name["code_system"], values_by_name["code"]
    )
    zip5, latitude, longitude, radius_miles = _canonical_geo(values_by_name)
    return {
        "billing_entity_ref": _canonical_billing_entity_ref(
            values_by_name["billing_entity_ref"]
        ),
        "plan_release_id": _canonical_plan_release(values_by_name["plan_release_id"]),
        "code_system": code_system,
        "code": code,
        "zip5": zip5,
        "latitude": latitude,
        "longitude": longitude,
        "radius_miles": radius_miles,
        "query_pairs": query_pairs,
        "request_fingerprint_sha256": _request_fingerprint_sha256(query_pairs),
        **_optional_filter_fields(values_by_name),
    }


def normalized_billing_search_request_fields(
    parameters: Mapping[str, Any],
) -> dict[str, Any] | None:
    """Return canonical request fields or no value after a sanitized failure."""

    try:
        if not isinstance(parameters, Mapping):
            raise request_failure()
        values_by_name, query_pairs = _query_values(parameters)
        return _request_fields(values_by_name, query_pairs)
    except Exception:
        return None


__all__ = [
    "BILLING_SEARCH_MAX_LIMIT",
    "BILLING_SEARCH_MAX_RADIUS_MILES",
    "BillingSearchRequestError",
    "normalized_billing_search_request_fields",
    "request_failure",
]
