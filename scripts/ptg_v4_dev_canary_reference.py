"""Independent V3 semantic-page oracle for the PTG V4 dev canary."""

from __future__ import annotations

import hashlib
import json
import math
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping, Sequence

from scripts.ptg_v4_dev_canary_support import (
    BoundedApiSpec,
    CanaryConfigurationError,
)
from scripts.ptg_v4_dev_canary_kubernetes import (
    validate_frozen_v3_runtime_evidence,
)


PUBLIC_REFERENCE_CONTRACT = "ptg_v4_public_v3_reference_v1"
V3_STORAGE_GENERATION = "shared_blocks_v3"
V4_STORAGE_GENERATION = "shared_blocks_v4"
_PAGE_DIGEST_DOMAIN = b"PTG-V4-PUBLIC-PAGE-V1\0"
_ITEM_DIGEST_DOMAIN = b"PTG-V4-PUBLIC-ITEM-V1\0"
_SEMANTIC_ITEM_FIELDS = (
    "plan_id",
    "plan_market_type",
    "provider_count",
    "provider_set_count",
    "procedure_code",
    "reported_code_system",
    "reported_code",
    "negotiation_arrangement",
    "procedure_name",
    "procedure_description",
    "prices",
    "price_summary",
    "npi",
    "price_set_count",
    "rate_pack_count",
)


def build_v3_reference_evidence(
    document_by_field: Mapping[str, Any],
    spec: BoundedApiSpec,
    *,
    runtime_evidence: Mapping[str, Any],
) -> dict[str, Any]:
    """Build a non-PII ordered semantic oracle from one frozen V3 response."""

    failures = _document_contract_failures(
        document_by_field,
        spec,
        expected_storage_generation=V3_STORAGE_GENERATION,
    )
    page = semantic_page(document_by_field)
    failures.extend(_semantic_page_failures(page, expected_count=spec.expected_item_count))
    if failures:
        raise CanaryConfigurationError(
            "V3 reference page is invalid: " + "; ".join(sorted(set(failures)))
        )
    return {
        "contract": PUBLIC_REFERENCE_CONTRACT,
        "reference_snapshot_id": spec.snapshot_id,
        "storage_generation": V3_STORAGE_GENERATION,
        "query": _query_evidence(spec),
        "item_count": len(page["item_digests"]),
        "semantic_key_count": len(set(page["item_digests"])),
        "item_digests": page["item_digests"],
        "minimum_negotiated_rates": page["minimum_negotiated_rates"],
        "page_digest": page["page_digest"],
        "reference_runtime": dict(runtime_evidence),
    }


def validate_reference_evidence(
    evidence_by_field: Mapping[str, Any],
    *,
    expected_spec: BoundedApiSpec | None = None,
) -> list[str]:
    """Validate a captured reference without trusting operator-entered digests."""

    failures: list[str] = []
    if evidence_by_field.get("contract") != PUBLIC_REFERENCE_CONTRACT:
        failures.append("public V3 reference contract is incompatible")
    if evidence_by_field.get("storage_generation") != V3_STORAGE_GENERATION:
        failures.append("public reference was not captured from frozen V3")
    runtime_evidence = evidence_by_field.get("reference_runtime")
    failures.extend(
        validate_frozen_v3_runtime_evidence(
            runtime_evidence if isinstance(runtime_evidence, Mapping) else {}
        )
    )
    if int(evidence_by_field.get("item_count") or -1) != 25:
        failures.append("public V3 reference is not the exact 25-item page")
    query = evidence_by_field.get("query")
    fixed_query = dict(query) if isinstance(query, Mapping) else {}
    reference_snapshot_id = str(
        evidence_by_field.get("reference_snapshot_id") or ""
    ).strip()
    if (
        not reference_snapshot_id
        or fixed_query.get("snapshot_id") != reference_snapshot_id
        or fixed_query.get("code_system") != "CPT"
        or fixed_query.get("code") != "70553"
        or fixed_query.get("limit") != 25
        or fixed_query.get("offset") != 0
        or fixed_query.get("npi") is not None
        or fixed_query.get("include_providers") is not True
        or fixed_query.get("order_by") != "negotiated_rate"
        or fixed_query.get("order") != "asc"
    ):
        failures.append("public V3 reference does not use the fixed CPT 70553 query")
    item_digests = evidence_by_field.get("item_digests")
    digest_rows = list(item_digests) if isinstance(item_digests, list) else []
    if (
        not digest_rows
        or any(not _is_sha256(digest) for digest in digest_rows)
        or len(digest_rows) != int(evidence_by_field.get("item_count") or -1)
        or len(set(digest_rows))
        != int(evidence_by_field.get("semantic_key_count") or -1)
        or len(set(digest_rows)) != 25
    ):
        failures.append("public V3 reference item digests are invalid")
    minimum_rates = evidence_by_field.get("minimum_negotiated_rates")
    rate_rows = list(minimum_rates) if isinstance(minimum_rates, list) else []
    if len(rate_rows) != len(digest_rows) or not _is_nondecreasing_rate_sequence(rate_rows):
        failures.append("public V3 reference is not ordered by negotiated rate")
    expected_page_digest = _page_digest(digest_rows)
    if evidence_by_field.get("page_digest") != expected_page_digest:
        failures.append("public V3 reference page digest is not self-authenticating")
    if expected_spec is not None:
        failures.extend(_reference_query_failures(evidence_by_field, expected_spec))
    return failures


def compare_v4_document_to_reference(
    document_by_field: Mapping[str, Any],
    spec: BoundedApiSpec,
    reference_by_field: Mapping[str, Any],
) -> tuple[list[str], dict[str, Any]]:
    """Compare one V4 response with the exact frozen-V3 semantic page."""

    failures = validate_reference_evidence(reference_by_field)
    failures.extend(
        _document_contract_failures(
            document_by_field,
            spec,
            expected_storage_generation=V4_STORAGE_GENERATION,
        )
    )
    page = semantic_page(document_by_field)
    failures.extend(_semantic_page_failures(page, expected_count=spec.expected_item_count))
    if page["page_digest"] != reference_by_field.get("page_digest"):
        failures.append("V4 ordered semantic page differs from frozen V3")
    if page["item_digests"] != reference_by_field.get("item_digests"):
        failures.append("V4 ordered semantic item sequence differs from frozen V3")
    if (
        page["minimum_negotiated_rates"]
        != reference_by_field.get("minimum_negotiated_rates")
    ):
        failures.append("V4 negotiated-rate order differs from frozen V3")
    return sorted(set(failures)), page


def evaluate_database_reference_evidence(
    evidence_by_field: Mapping[str, Any],
) -> dict[str, Any]:
    """Evaluate source equality, V3 sealing, rollback pinning, and block safety."""

    failures: list[str] = []
    reference_snapshot = evidence_by_field.get("reference_snapshot")
    reference_fields = (
        dict(reference_snapshot) if isinstance(reference_snapshot, Mapping) else {}
    )
    rollback_pin = evidence_by_field.get("rollback_pin")
    pin_fields = dict(rollback_pin) if isinstance(rollback_pin, Mapping) else {}
    if evidence_by_field.get("same_raw_sources") is not True:
        failures.append("V3 reference and V4 snapshot use different raw source sets")
    if evidence_by_field.get("v4_source_set") != evidence_by_field.get(
        "reference_source_set"
    ):
        failures.append("V3 reference and V4 source-set digests differ")
    if reference_fields.get("snapshot_status") != "published":
        failures.append("V3 reference snapshot is not published")
    if reference_fields.get("layout_state") != "sealed":
        failures.append("V3 reference layout is not sealed")
    if reference_fields.get("layout_generation") != V3_STORAGE_GENERATION:
        failures.append("V3 reference layout is not shared_blocks_v3")
    if int(reference_fields.get("snapshot_block_count") or 0) < 1:
        failures.append("V3 reference layout has no retained block locators")
    if reference_fields.get("snapshot_block_foreign_key_validated") is not True:
        failures.append("V3 block-locator foreign key is not validated")
    if (
        pin_fields.get("owner_type") != "ptg_v4_rollback"
        or pin_fields.get("owner_id") != evidence_by_field.get("rollback_owner_id")
        or pin_fields.get("snapshot_id")
        != evidence_by_field.get("reference_snapshot_id")
        or not str(pin_fields.get("reason") or "").strip()
    ):
        failures.append("V3 reference does not have the exact rollback pin")
    return {
        "passed": not failures,
        "failures": sorted(set(failures)),
        "v4_snapshot_id": evidence_by_field.get("v4_snapshot_id"),
        "reference_snapshot_id": evidence_by_field.get("reference_snapshot_id"),
        "rollback_owner_id": evidence_by_field.get("rollback_owner_id"),
        "same_raw_sources": evidence_by_field.get("same_raw_sources"),
        "same_source_trace_sets": evidence_by_field.get("same_source_trace_sets"),
        "v4_source_set": evidence_by_field.get("v4_source_set"),
        "reference_source_set": evidence_by_field.get("reference_source_set"),
        "reference_snapshot": reference_fields,
        "rollback_pin": pin_fields,
    }


def semantic_page(document_by_field: Mapping[str, Any]) -> dict[str, Any]:
    """Hash stable rate/provider semantics while excluding mutable directory fields."""

    raw_items = document_by_field.get("items")
    item_rows = list(raw_items) if isinstance(raw_items, list) else []
    semantic_items = [
        _semantic_item(item) if isinstance(item, Mapping) else {"invalid": True}
        for item in item_rows
    ]
    item_digests = [_semantic_item_digest(item) for item in semantic_items]
    minimum_rates = [_minimum_negotiated_rate(item) for item in semantic_items]
    return {
        "item_digests": item_digests,
        "minimum_negotiated_rates": minimum_rates,
        "page_digest": _page_digest(item_digests),
    }


def _document_contract_failures(
    document_by_field: Mapping[str, Any],
    spec: BoundedApiSpec,
    *,
    expected_storage_generation: str,
) -> list[str]:
    failures: list[str] = []
    if document_by_field.get("result_state") != spec.expected_result_state:
        failures.append("public reference result_state differs from expectation")
    item_rows = document_by_field.get("items")
    if not isinstance(item_rows, list) or len(item_rows) != spec.expected_item_count:
        failures.append("public reference item cardinality differs from expectation")
    provenance = document_by_field.get("provenance")
    provenance_by_field = provenance if isinstance(provenance, Mapping) else {}
    if provenance_by_field.get("snapshot_id") != spec.snapshot_id:
        failures.append("public reference snapshot differs from the fixed query")
    if provenance_by_field.get("storage_generation") != expected_storage_generation:
        failures.append("public reference storage generation is incorrect")
    return failures


def _semantic_page_failures(
    page_by_field: Mapping[str, Any],
    *,
    expected_count: int,
) -> list[str]:
    item_digests = list(page_by_field.get("item_digests") or ())
    minimum_rates = list(page_by_field.get("minimum_negotiated_rates") or ())
    failures: list[str] = []
    if len(item_digests) != expected_count:
        failures.append("semantic page item count differs from expectation")
    if len(item_digests) != len(set(item_digests)):
        failures.append("semantic page contains duplicate expansion keys")
    if not _is_nondecreasing_rate_sequence(minimum_rates):
        failures.append("semantic page is not nondecreasing by negotiated rate")
    return failures


def _reference_query_failures(
    evidence_by_field: Mapping[str, Any],
    spec: BoundedApiSpec,
) -> list[str]:
    query = evidence_by_field.get("query")
    expected_query = _query_evidence(spec)
    return (
        []
        if isinstance(query, Mapping) and dict(query) == expected_query
        else ["public V3 reference query differs from the V4 probe"]
    )


def _query_evidence(spec: BoundedApiSpec) -> dict[str, Any]:
    return {
        "snapshot_id": spec.snapshot_id,
        "code_system": spec.code_system,
        "code": spec.code,
        "limit": spec.limit,
        "offset": 0,
        "npi": None,
        "include_providers": True,
        "order_by": "negotiated_rate",
        "order": "asc",
    }


def _semantic_item(item_by_field: Mapping[str, Any]) -> dict[str, Any]:
    return {
        field_name: _canonical_value(item_by_field.get(field_name))
        for field_name in _SEMANTIC_ITEM_FIELDS
    }


def _canonical_value(value: Any) -> Any:
    if value is None or isinstance(value, (str, bool)):
        return value
    if isinstance(value, (int, float, Decimal)):
        if isinstance(value, float) and not math.isfinite(value):
            return str(value)
        if isinstance(value, Decimal) and not value.is_finite():
            return str(value)
        return _canonical_decimal(value)
    if isinstance(value, Mapping):
        return {
            str(key): _canonical_value(field_value)
            for key, field_value in sorted(value.items(), key=lambda row: str(row[0]))
        }
    if isinstance(value, (list, tuple)):
        return [_canonical_value(item) for item in value]
    return str(value)


def _canonical_decimal(value: Any) -> str:
    try:
        decimal_value = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return str(value)
    normalized = format(decimal_value.normalize(), "f")
    return "0" if normalized in {"-0", ""} else normalized


def _minimum_negotiated_rate(item_by_field: Mapping[str, Any]) -> str | None:
    raw_prices = item_by_field.get("prices")
    prices = list(raw_prices) if isinstance(raw_prices, list) else []
    rates: list[Decimal] = []
    for price in prices:
        if not isinstance(price, Mapping):
            continue
        try:
            rate = Decimal(str(price.get("negotiated_rate")))
        except (InvalidOperation, TypeError, ValueError):
            continue
        if rate.is_finite():
            rates.append(rate)
    return _canonical_decimal(min(rates)) if rates else None


def _is_nondecreasing_rate_sequence(rate_rows: Sequence[Any]) -> bool:
    if not rate_rows or any(rate is None for rate in rate_rows):
        return False
    try:
        decimals = [Decimal(str(rate)) for rate in rate_rows]
    except (InvalidOperation, TypeError, ValueError):
        return False
    return all(left <= right for left, right in zip(decimals, decimals[1:]))


def _semantic_item_digest(item_by_field: Mapping[str, Any]) -> str:
    digest = hashlib.sha256()
    digest.update(_ITEM_DIGEST_DOMAIN)
    digest.update(_canonical_json(item_by_field))
    return digest.hexdigest()


def _page_digest(item_digests: Sequence[Any]) -> str:
    digest = hashlib.sha256()
    digest.update(_PAGE_DIGEST_DOMAIN)
    for item_digest in item_digests:
        if not _is_sha256(item_digest):
            digest.update(b"\xff")
            digest.update(str(item_digest).encode("utf-8"))
            continue
        digest.update(bytes.fromhex(str(item_digest)))
    return digest.hexdigest()


def _canonical_json(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _is_sha256(value: Any) -> bool:
    try:
        return len(str(value)) == 64 and bytes.fromhex(str(value)) is not None
    except ValueError:
        return False
