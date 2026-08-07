# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Normalize a closed billing-search POST body without hashing its raw selector."""

from __future__ import annotations

import hashlib
import json
import math
import re
from typing import Any, Mapping

from api.ptg2_capacity_evidence import (
    CapacityEvidenceError,
    normalize_capacity_code,
    normalize_capacity_code_system,
)
from process.provider_directory_profile import is_valid_npi
from process.tin_npi_connector_security import normalize_ein
from process.tin_npi_connector_support import TinNpiConnectorError

BILLING_SEARCH_POST_REQUEST_CONTRACT = "healthporta.billing-search-post-request.v1"
BILLING_SEARCH_POST_DEFAULT_LIMIT = 25
BILLING_SEARCH_POST_MAX_LIMIT = 100
BILLING_SEARCH_POST_MAX_RADIUS_MILES = 100.0
BILLING_SEARCH_POST_MAX_CURSOR_CHARACTERS = 2048

_REQUEST_SHAPE_DOMAIN = b"HEALTHPORTA_BILLING_SEARCH_POST_REQUEST_SHAPE_V1\x00"
_REQUEST_FINGERPRINT_DOMAIN = (
    b"HEALTHPORTA_BILLING_SEARCH_POST_REQUEST_FINGERPRINT_V1\x00"
)
_INVALID = "billing_search_post_request_invalid"
_HEALTHPORTA_PLAN_PATTERN = re.compile(
    r"hpplan_[0-9A-HJKMNP-TV-Z]{26}\Z",
    flags=re.ASCII,
)
_NPI_PATTERN = re.compile(r"[0-9]{10}\Z", flags=re.ASCII)
_ZIP5_PATTERN = re.compile(r"[0-9]{5}\Z", flags=re.ASCII)
_MODIFIER_PATTERN = re.compile(r"[A-Z0-9]{1,8}\Z", flags=re.ASCII)
_PLACE_OF_SERVICE_PATTERN = re.compile(r"[0-9]{2}\Z", flags=re.ASCII)
_BILLING_REF_PATTERN = re.compile(r"be1_[A-Za-z0-9_-]{1,508}\Z", flags=re.ASCII)
_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}\Z", flags=re.ASCII)
_TOP_LEVEL_FIELDS = frozenset(
    {
        "billing_identity",
        "geo",
        "healthporta_plan_id",
        "include_evidence",
        "page",
        "procedure",
        "provider_npi",
    }
)
_TOP_LEVEL_REQUIRED_FIELDS = frozenset(
    {"billing_identity", "geo", "healthporta_plan_id", "procedure"}
)
_PROCEDURE_FIELDS = frozenset({"code", "code_system", "modifiers", "place_of_service"})
_GEO_FIELDS = frozenset({"radius_miles", "zip5"})
_PAGE_FIELDS = frozenset({"cursor", "limit"})
_REQUEST_VALUE_FIELDS = (
    "healthporta_plan_id",
    "selector_kind",
    "tax_identity_type",
    "tax_identity_value",
    "billing_entity_ref",
    "code_system",
    "code",
    "modifiers",
    "place_of_service",
    "zip5",
    "radius_miles",
    "provider_npi",
    "include_evidence",
    "limit",
    "cursor",
)


class BillingSearchPostRequestError(ValueError):
    """One value-free request rejection safe for the public boundary."""


def request_failure() -> BillingSearchPostRequestError:
    """Return a request error containing no input value or field alias."""

    return BillingSearchPostRequestError(_INVALID)


def _exact_object(
    value: object,
    *,
    required_fields: frozenset[str],
    allowed_fields: frozenset[str],
) -> dict[str, Any]:
    if type(value) is not dict:
        raise request_failure()
    field_names = tuple(value)
    if any(type(field_name) is not str for field_name in field_names):
        raise request_failure()
    field_set = frozenset(field_names)
    if not required_fields.issubset(field_set) or not field_set.issubset(
        allowed_fields
    ):
        raise request_failure()
    return value


def _canonical_healthporta_plan_id(value: object) -> str:
    if type(value) is not str or _HEALTHPORTA_PLAN_PATTERN.fullmatch(value) is None:
        raise request_failure()
    return value


def _canonical_npi(value: object) -> str:
    if (
        type(value) is not str
        or _NPI_PATTERN.fullmatch(value) is None
        or not is_valid_npi(value)
    ):
        raise request_failure()
    return value


def _normalized_tax_identity(value: object) -> tuple[str, str]:
    raw_identity = _exact_object(
        value,
        required_fields=frozenset({"type", "value"}),
        allowed_fields=frozenset({"type", "value"}),
    )
    tin_type = raw_identity.get("type")
    raw_value = raw_identity.get("value")
    if type(tin_type) is not str:
        raise request_failure()
    if tin_type == "ein":
        try:
            normalized_value = normalize_ein(raw_value)
        except TinNpiConnectorError:
            raise request_failure() from None
    elif tin_type == "npi":
        normalized_value = _canonical_npi(raw_value)
    else:
        raise request_failure()
    return tin_type, normalized_value


def _normalized_billing_identity(
    value: object,
) -> tuple[str, str | None, str | None, str | None]:
    identity = _exact_object(
        value,
        required_fields=frozenset(),
        allowed_fields=frozenset({"billing_entity_ref", "tax_identity"}),
    )
    if frozenset(identity) == {"tax_identity"}:
        tin_type, normalized_value = _normalized_tax_identity(identity["tax_identity"])
        return "tax_identity", tin_type, normalized_value, None
    if frozenset(identity) == {"billing_entity_ref"}:
        reference = identity["billing_entity_ref"]
        if (
            type(reference) is not str
            or _BILLING_REF_PATTERN.fullmatch(reference) is None
        ):
            raise request_failure()
        return "billing_entity_ref", None, None, reference
    raise request_failure()


def _normalized_procedure(
    procedure_value: object,
) -> tuple[str, str, tuple[str, ...], tuple[str, ...]]:
    procedure = _exact_object(
        procedure_value,
        required_fields=_PROCEDURE_FIELDS,
        allowed_fields=_PROCEDURE_FIELDS,
    )
    code_system = procedure.get("code_system")
    code = procedure.get("code")
    if type(code_system) is not str or type(code) is not str:
        raise request_failure()
    try:
        normalized_system = normalize_capacity_code_system(code_system)
        normalized_code = normalize_capacity_code(normalized_system, code)
    except CapacityEvidenceError:
        raise request_failure() from None
    if normalized_system != code_system or normalized_code != code:
        raise request_failure()
    modifiers = _canonical_code_array(
        procedure.get("modifiers"),
        pattern=_MODIFIER_PATTERN,
        maximum_count=8,
    )
    place_of_service = _canonical_code_array(
        procedure.get("place_of_service"),
        pattern=_PLACE_OF_SERVICE_PATTERN,
        maximum_count=16,
    )
    return normalized_system, normalized_code, modifiers, place_of_service


def _canonical_code_array(
    value: object,
    *,
    pattern: re.Pattern[str],
    maximum_count: int,
) -> tuple[str, ...]:
    if type(value) is not list or len(value) > maximum_count:
        raise request_failure()
    normalized_values = tuple(value)
    if any(
        type(member) is not str or pattern.fullmatch(member) is None
        for member in normalized_values
    ) or normalized_values != tuple(sorted(set(normalized_values))):
        raise request_failure()
    return normalized_values


def _normalized_geo(value: object) -> tuple[str, float]:
    geo = _exact_object(
        value,
        required_fields=_GEO_FIELDS,
        allowed_fields=_GEO_FIELDS,
    )
    zip5 = geo.get("zip5")
    radius = geo.get("radius_miles")
    if type(zip5) is not str or _ZIP5_PATTERN.fullmatch(zip5) is None:
        raise request_failure()
    if type(radius) not in (int, float):
        raise request_failure()
    normalized_radius = float(radius)
    if (
        not math.isfinite(normalized_radius)
        or not 0.0 <= normalized_radius <= BILLING_SEARCH_POST_MAX_RADIUS_MILES
    ):
        raise request_failure()
    return zip5, normalized_radius


def _normalized_page(value: object) -> tuple[int, str | None]:
    page = _exact_object(
        value,
        required_fields=frozenset(),
        allowed_fields=_PAGE_FIELDS,
    )
    limit = page.get("limit", BILLING_SEARCH_POST_DEFAULT_LIMIT)
    if type(limit) is not int or not 1 <= limit <= BILLING_SEARCH_POST_MAX_LIMIT:
        raise request_failure()
    cursor = page.get("cursor")
    if cursor is not None and (
        type(cursor) is not str
        or not 1 <= len(cursor) <= BILLING_SEARCH_POST_MAX_CURSOR_CHARACTERS
        or not cursor.isascii()
        or not cursor.isprintable()
    ):
        raise request_failure()
    return limit, cursor


def _request_fields(request_body_by_field: dict[str, Any]) -> dict[str, Any]:
    selector_kind, tin_type, tin_value, billing_ref = _normalized_billing_identity(
        request_body_by_field["billing_identity"]
    )
    code_system, code, modifiers, place_of_service = _normalized_procedure(
        request_body_by_field["procedure"]
    )
    zip5, radius_miles = _normalized_geo(request_body_by_field["geo"])
    limit, cursor = _normalized_page(request_body_by_field.get("page", {}))
    provider_npi = (
        int(_canonical_npi(request_body_by_field["provider_npi"]))
        if "provider_npi" in request_body_by_field
        else None
    )
    include_evidence = request_body_by_field.get("include_evidence", False)
    if type(include_evidence) is not bool:
        raise request_failure()
    return {
        "healthporta_plan_id": _canonical_healthporta_plan_id(
            request_body_by_field["healthporta_plan_id"]
        ),
        "selector_kind": selector_kind,
        "tax_identity_type": tin_type,
        "tax_identity_value": tin_value,
        "billing_entity_ref": billing_ref,
        "code_system": code_system,
        "code": code,
        "modifiers": modifiers,
        "place_of_service": place_of_service,
        "zip5": zip5,
        "radius_miles": radius_miles,
        "provider_npi": provider_npi,
        "include_evidence": include_evidence,
        "limit": limit,
        "cursor": cursor,
    }


def _normalized_request_fields_or_none(payload: object) -> dict[str, Any] | None:
    try:
        request_payload = _exact_object(
            payload,
            required_fields=_TOP_LEVEL_REQUIRED_FIELDS,
            allowed_fields=_TOP_LEVEL_FIELDS,
        )
        return _request_fields(request_payload)
    except Exception:
        return None


def _canonical_json_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")


def _framed_sha256(domain: bytes, payload: bytes) -> str:
    digest = hashlib.sha256()
    digest.update(domain)
    digest.update(len(payload).to_bytes(8, "big"))
    digest.update(payload)
    return digest.hexdigest()


def _request_shape_payload(fields_by_name: Mapping[str, Any]) -> dict[str, Any]:
    """Return only non-sensitive request coordinates; omit selector value/cursor."""

    provider_npi = fields_by_name["provider_npi"]
    return {
        "contract": BILLING_SEARCH_POST_REQUEST_CONTRACT,
        "healthporta_plan_id": fields_by_name["healthporta_plan_id"],
        "selector": {
            "kind": fields_by_name["selector_kind"],
            "tax_identity_type": fields_by_name["tax_identity_type"],
        },
        "procedure": {
            "code_system": fields_by_name["code_system"],
            "code": fields_by_name["code"],
            "modifiers": list(fields_by_name["modifiers"]),
            "place_of_service": list(fields_by_name["place_of_service"]),
        },
        "geo": {
            "zip5": fields_by_name["zip5"],
            "radius_miles": fields_by_name["radius_miles"],
        },
        "provider_npi": str(provider_npi) if provider_npi is not None else None,
        "include_evidence": fields_by_name["include_evidence"],
        "page": {"limit": fields_by_name["limit"]},
    }


def _request_shape_sha256(fields_by_name: Mapping[str, Any]) -> str:
    return _framed_sha256(
        _REQUEST_SHAPE_DOMAIN,
        _canonical_json_bytes(_request_shape_payload(fields_by_name)),
    )


def _canonical_selector_scope_sha256(value: object) -> str:
    if (
        type(value) is not str
        or _SHA256_PATTERN.fullmatch(value) is None
        or value == "0" * 64
    ):
        raise request_failure()
    return value


def _bound_request_fingerprint_sha256(
    *,
    request_shape_sha256: str,
    selector_scope_sha256: object,
) -> str:
    selector_scope = _canonical_selector_scope_sha256(selector_scope_sha256)
    fingerprint_payload_by_field = {
        "contract": BILLING_SEARCH_POST_REQUEST_CONTRACT,
        "request_shape_sha256": request_shape_sha256,
        "selector_scope_sha256": selector_scope,
    }
    return _framed_sha256(
        _REQUEST_FINGERPRINT_DOMAIN,
        _canonical_json_bytes(fingerprint_payload_by_field),
    )


__all__ = [
    "BILLING_SEARCH_POST_DEFAULT_LIMIT",
    "BILLING_SEARCH_POST_MAX_CURSOR_CHARACTERS",
    "BILLING_SEARCH_POST_MAX_LIMIT",
    "BILLING_SEARCH_POST_MAX_RADIUS_MILES",
    "BILLING_SEARCH_POST_REQUEST_CONTRACT",
    "BillingSearchPostRequestError",
]
