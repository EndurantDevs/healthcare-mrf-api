# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Canonical and legacy aliases for pricing response fields."""

from typing import Any


def _coalesce_value(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None


def _normalize_service_totals(service_payload_by_field: dict[str, Any]) -> None:
    service_payload_by_field["total_services"] = _coalesce_value(
        service_payload_by_field.get("total_services"),
        service_payload_by_field.get("total_claims"),
    )
    service_payload_by_field["total_beneficiary_day_services"] = _coalesce_value(
        service_payload_by_field.get("total_beneficiary_day_services"),
        service_payload_by_field.get("total_30day_fills"),
    )
    service_payload_by_field["total_beneficiaries"] = _coalesce_value(
        service_payload_by_field.get("total_beneficiaries"),
        service_payload_by_field.get("total_benes"),
    )
    service_payload_by_field["total_submitted_charges"] = _coalesce_value(
        service_payload_by_field.get("total_submitted_charges"),
        service_payload_by_field.get("total_day_supply"),
    )
    service_payload_by_field["total_allowed_amount"] = _coalesce_value(
        service_payload_by_field.get("total_allowed_amount"),
        service_payload_by_field.get("total_drug_cost"),
    )
    service_payload_by_field["ge65_total_services"] = _coalesce_value(
        service_payload_by_field.get("ge65_total_services"),
        service_payload_by_field.get("ge65_total_claims"),
    )
    service_payload_by_field["ge65_total_beneficiaries"] = _coalesce_value(
        service_payload_by_field.get("ge65_total_beneficiaries"),
        service_payload_by_field.get("ge65_total_benes"),
    )
    service_payload_by_field["ge65_total_allowed_amount"] = _coalesce_value(
        service_payload_by_field.get("ge65_total_allowed_amount"),
        service_payload_by_field.get("ge65_total_drug_cost"),
    )


def _add_legacy_service_fields(service_payload_by_field: dict[str, Any]) -> None:
    service_payload_by_field["generic_name"] = service_payload_by_field.get("service_description")
    service_payload_by_field["brand_name"] = service_payload_by_field.get("reported_code")
    service_payload_by_field["total_claims"] = service_payload_by_field.get("total_services")
    service_payload_by_field["total_30day_fills"] = service_payload_by_field.get("total_beneficiary_day_services")
    service_payload_by_field["total_day_supply"] = service_payload_by_field.get("total_submitted_charges")
    service_payload_by_field["total_benes"] = service_payload_by_field.get("total_beneficiaries")
    service_payload_by_field["total_drug_cost"] = service_payload_by_field.get("total_allowed_amount")
    service_payload_by_field["ge65_total_claims"] = service_payload_by_field.get("ge65_total_services")
    service_payload_by_field["ge65_total_benes"] = service_payload_by_field.get("ge65_total_beneficiaries")
    service_payload_by_field["ge65_total_drug_cost"] = service_payload_by_field.get("ge65_total_allowed_amount")


def _drop_legacy_service_fields(service_payload_by_field: dict[str, Any]) -> None:
    for field_name in (
        "generic_name",
        "brand_name",
        "total_claims",
        "total_30day_fills",
        "total_day_supply",
        "total_drug_cost",
        "total_benes",
        "ge65_total_claims",
        "ge65_total_benes",
        "ge65_total_drug_cost",
        "legacy_field_aliases",
    ):
        service_payload_by_field.pop(field_name, None)


def _add_legacy_provider_totals(provider_payload_by_field: dict[str, Any]) -> None:
    """Populate the legacy provider-total aliases from canonical values."""

    legacy_field_by_canonical_field = {
        "total_claims": "total_services",
        "total_30day_fills": "total_reported_service_codes",
        "total_day_supply": "total_submitted_charges",
        "total_benes": "total_beneficiaries",
        "total_drug_cost": "total_allowed_amount",
    }
    for legacy_field, canonical_field in legacy_field_by_canonical_field.items():
        provider_payload_by_field[legacy_field] = provider_payload_by_field.get(canonical_field)


def _normalize_prescription_payload(payload: dict[str, Any], min_address_sources=0) -> dict[str, Any]:
    if min_address_sources:
        for field in ("city", "state", "zip5", "country"):
            payload.pop(field, None)
    payload["prescription_code_system"] = payload.get("rx_code_system")
    payload["prescription_code"] = payload.get("rx_code")
    payload["prescription_name"] = payload.get("rx_name") or payload.get("generic_name") or payload.get("brand_name")
    payload["total_prescriptions"] = payload.get("total_claims")
    payload["total_beneficiaries"] = payload.get("total_benes")
    payload["total_allowed_amount"] = payload.get("total_drug_cost")
    payload["ge65_total_prescriptions"] = payload.get("ge65_total_claims")
    payload["ge65_total_beneficiaries"] = payload.get("ge65_total_benes")
    payload["ge65_total_allowed_amount"] = payload.get("ge65_total_drug_cost")
    return payload


def _normalize_prescription_provider_aggregate(payload: dict[str, Any]) -> dict[str, Any]:
    payload["total_prescriptions"] = payload.get("total_claims")
    payload["total_beneficiaries"] = payload.get("total_benes")
    payload["total_allowed_amount"] = payload.get("total_drug_cost")
    return payload
