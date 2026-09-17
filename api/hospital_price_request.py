# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Exact selectors and opaque charge cursors for hospital-price reads."""

from __future__ import annotations

import base64
from dataclasses import dataclass
import hashlib
import re
import struct


DEFAULT_HOSPITAL_PRICE_LIMIT = 25
MAX_HOSPITAL_PRICE_LIMIT = 100
MISSING_PLAN_SELECTOR = object()
_HOSPITAL_ID_PATTERN = re.compile(r"hospital-[0-9]{6}\Z")
_VERSION_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
_LIMIT_PATTERN = re.compile(r"[1-9][0-9]{0,2}\Z")
_CURSOR_PATTERN = re.compile(r"[A-Za-z0-9_-]{96}\Z")
_CURSOR_MAGIC = b"hpc1"
_CURSOR_BYTES = 72
_CODE_TYPES = frozenset(
    {
        "AP-DRG", "APC", "APR-DRG", "APS-DRG", "CDM", "CDT", "CMG", "CPT", "DRG", "EAPG", "HCPCS",
        "HIPPS", "ICD", "LOCAL", "MS-DRG", "MS-LTC-DRG", "NDC", "R-DRG", "RC", "S-DRG", "TRIS-DRG",
    }
)


class HospitalPriceInvalidRequestError(RuntimeError):
    """Reject malformed or internally inconsistent public selectors."""


class HospitalPriceNotFoundError(RuntimeError):
    """Collapse unknown, unpublished, and unbound hospital versions."""


class HospitalPriceCursorStaleError(RuntimeError):
    """Require pagination restart after the current version changes."""


@dataclass(frozen=True, slots=True)
class HospitalPriceQuery:
    hospital_id: str
    code_type: str
    code: str
    payer_name: str | None
    plan_name: str | None
    version_id: str | None
    cursor: str | None
    limit: int
    plan_missing: bool = False


@dataclass(frozen=True, slots=True)
class HospitalPayerPlanQuery:
    hospital_id: str
    version_id: str | None
    cursor: str | None
    limit: int


def validate_hospital_payer_plan_query(
    hospital_id: object, *, version_id: object = None,
    cursor: object = None, limit: object = None,
) -> HospitalPayerPlanQuery:
    """Validate bounded discovery using the shared facility/version constraints."""

    parsed_limit = _validate_resource_query(hospital_id, version_id, cursor, limit)
    return HospitalPayerPlanQuery(hospital_id, version_id, cursor, parsed_limit)


def validate_hospital_price_plan(plan_name: object, plan_missing: object) -> object:
    """Decode an explicit absent-plan selector without inventing plan text."""

    if plan_missing is None:
        return plan_name
    if plan_missing != "true" or plan_name is not None:
        raise HospitalPriceInvalidRequestError("plan_missing must be true and excludes plan_name")
    return MISSING_PLAN_SELECTOR


def validate_hospital_price_query(
    hospital_id: object,
    *,
    code_type: object,
    code: object,
    payer_name: object = None,
    plan_name: object = None,
    version_id: object = None,
    cursor: object = None,
    limit: object = None,
) -> HospitalPriceQuery:
    """Validate one exact, bounded public query without normalization."""

    parsed_limit = _validate_resource_query(hospital_id, version_id, cursor, limit)
    if type(code_type) is not str or code_type not in _CODE_TYPES:
        raise HospitalPriceInvalidRequestError("hospital price code type is invalid")
    if (
        type(code) is not str or not code or code != code.strip()
        or len(code.encode("utf-8")) > 1024
    ):
        raise HospitalPriceInvalidRequestError("hospital price code is invalid")
    is_missing_plan = plan_name is MISSING_PLAN_SELECTOR
    if is_missing_plan:
        plan_name = None
        if payer_name is None:
            raise HospitalPriceInvalidRequestError("plan_missing requires payer_name")
    has_payer_field = payer_name is not None or plan_name is not None
    if not is_missing_plan and has_payer_field != (payer_name is not None and plan_name is not None):
        raise HospitalPriceInvalidRequestError(
            "payer_name and plan_name must be supplied together"
        )
    for field_name, field_text in (("payer_name", payer_name), ("plan_name", plan_name)):
        if field_text is not None and (
            type(field_text) is not str or not field_text
            or field_text != field_text.strip()
            or len(field_text.encode("utf-8")) > 4096
        ):
            raise HospitalPriceInvalidRequestError(f"{field_name} is invalid")
    return HospitalPriceQuery(
        hospital_id, code_type, code, payer_name, plan_name,
        version_id, cursor, parsed_limit, is_missing_plan,
    )


def _validate_resource_query(
    hospital_id: object, version_id: object, cursor: object, limit: object,
) -> int:
    """Validate shared exact identity and bounded continuation fields."""

    if type(hospital_id) is not str or _HOSPITAL_ID_PATTERN.fullmatch(hospital_id) is None:
        raise HospitalPriceNotFoundError("hospital price resource is unavailable")
    if version_id is not None and (
        type(version_id) is not str or _VERSION_PATTERN.fullmatch(version_id) is None
    ):
        raise HospitalPriceInvalidRequestError("hospital price version is invalid")
    if cursor is not None and (
        type(cursor) is not str or _CURSOR_PATTERN.fullmatch(cursor) is None
    ):
        raise HospitalPriceInvalidRequestError("hospital price cursor is invalid")
    if limit is None:
        parsed_limit = DEFAULT_HOSPITAL_PRICE_LIMIT
    elif type(limit) is str and _LIMIT_PATTERN.fullmatch(limit):
        parsed_limit = int(limit)
    else:
        raise HospitalPriceInvalidRequestError("hospital price limit is invalid")
    if parsed_limit > MAX_HOSPITAL_PRICE_LIMIT:
        raise HospitalPriceInvalidRequestError("hospital price limit is invalid")
    return parsed_limit


def _scope_digest(query: HospitalPriceQuery | HospitalPayerPlanQuery) -> bytes:
    if isinstance(query, HospitalPayerPlanQuery):
        return hashlib.sha256(
            b"healthporta.hospital-payer-plan-cursor.v1\0" + query.hospital_id.encode("utf-8")
        ).digest()
    domain = b"healthporta.hospital-price-charge-cursor.v1\0"
    if query.plan_missing:
        domain = b"healthporta.hospital-price-charge-cursor.missing-plan.v1\0"
    digest = hashlib.sha256(domain)
    for value in (
        query.hospital_id, query.code_type, query.code,
        query.payer_name or "", query.plan_name or "",
    ):
        encoded = value.encode("utf-8")
        digest.update(struct.pack("<I", len(encoded)))
        digest.update(encoded)
    return digest.digest()


def encode_hospital_price_cursor(
    query: HospitalPriceQuery | HospitalPayerPlanQuery,
    version_id: str,
    after_key: int,
) -> str:
    """Encode a version-and-selector-bound dense charge continuation."""

    payload = (
        _CURSOR_MAGIC + bytes.fromhex(version_id)
        + struct.pack("<I", after_key) + _scope_digest(query)
    )
    return base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")


def decode_hospital_price_cursor(
    query: HospitalPriceQuery | HospitalPayerPlanQuery, version_id: str,
) -> int:
    """Decode a canonical cursor or reject cross-query/version reuse."""

    if query.cursor is None:
        return -1
    try:
        payload = base64.b64decode(
            query.cursor + "=" * (-len(query.cursor) % 4),
            altchars=b"-_", validate=True,
        )
        canonical = base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")
    except ValueError:
        raise HospitalPriceInvalidRequestError("hospital price cursor is invalid") from None
    if (
        len(payload) != _CURSOR_BYTES or canonical != query.cursor
        or payload[:4] != _CURSOR_MAGIC or payload[40:] != _scope_digest(query)
    ):
        raise HospitalPriceInvalidRequestError("hospital price cursor is invalid")
    cursor_version = payload[4:36].hex()
    if cursor_version != version_id:
        if query.version_id is None:
            raise HospitalPriceCursorStaleError("hospital price cursor is stale")
        raise HospitalPriceInvalidRequestError("hospital price cursor is invalid")
    return struct.unpack("<I", payload[36:40])[0]
