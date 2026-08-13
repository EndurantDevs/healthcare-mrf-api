# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Human-readable renderings for normalized provider-profile facts."""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

_LABEL_FIELDS = ("display", "text", "name", "description", "label")
_REFERENCE_SUFFIXES = ("_ref", "_refs")
_NEW_PATIENT_LABELS = {
    "newpt": "Accepting new patients",
    "newpatient": "Accepting new patients",
    "accepting": "Accepting new patients",
    "existptonly": "Existing patients only",
    "existing": "Existing patients only",
    "nopt": "Not accepting new patients",
    "notaccepting": "Not accepting new patients",
}


def _decoded_structure(value: str) -> Mapping[str, Any] | list[Any] | None:
    candidate = value.strip()
    if not candidate.startswith(("{", "[")):
        return None
    try:
        decoded = json.loads(candidate)
    except json.JSONDecodeError:
        return None
    return decoded if isinstance(decoded, (Mapping, list)) else None


def _is_structural_text(value: Any) -> bool:
    return isinstance(value, str) and value.strip().startswith(("{", "["))


def _text(value: Any) -> str:
    if value in (None, "", [], {}) or isinstance(value, (Mapping, list, tuple, set)):
        return ""
    if _is_structural_text(value):
        return ""
    return str(value).strip()


def _human_label(fact_type: str) -> str:
    label = str(fact_type or "provider detail").replace("_", " ").strip()
    return label[:1].upper() + label[1:]


def _direct_label(value: Mapping[str, Any]) -> str:
    for field_name in _LABEL_FIELDS:
        field_value = value.get(field_name)
        if isinstance(field_value, (str, int, float)) and _text(field_value):
            return _text(field_value)
    return ""


def _code_label(value: Any) -> str:
    if not isinstance(value, Mapping):
        return _text(value)
    label = _direct_label(value)
    code = _text(value.get("code"))
    if label and code and label.casefold() != code.casefold():
        return f"{label} ({code})"
    return label or code


def _labels_from_codes(value: Any) -> list[str]:
    values = value if isinstance(value, list) else [value]
    return list(
        dict.fromkeys(
            label
            for label in (_code_label(item) for item in values)
            if label
        )
    )


def _joined_code_labels(value: Any) -> str:
    return "; ".join(_labels_from_codes(value))


def _dated_label(label: str, value: Mapping[str, Any]) -> str:
    as_of = _text(value.get("as_of"))
    return f"{label} (as of {as_of})" if as_of else label


def _age_display(value: Mapping[str, Any]) -> str:
    years = _text(value.get("years"))
    return _dated_label(f"Age: {years} years", value) if years else ""


def _practice_display(value: Mapping[str, Any]) -> str:
    years = _text(value.get("years"))
    if not years:
        return ""
    prefix = "Estimated years in practice" if value.get("estimated") else "Years in practice"
    return _dated_label(f"{prefix}: {years}", value)


def _contact_display(value: Mapping[str, Any]) -> str:
    contact = _text(value.get("value"))
    if not contact:
        return ""
    system = _human_label(_text(value.get("system")) or "contact")
    use = _text(value.get("use"))
    suffix = f" ({use})" if use else ""
    return f"{system}: {contact}{suffix}"


def _qualification_display(value: Mapping[str, Any]) -> str:
    coding = _joined_code_labels(value.get("coding"))
    if coding:
        return coding
    issuer = _text(value.get("issuer_display"))
    return f"Qualification issued by {issuer}" if issuer else ""


def license_number(value: Mapping[str, Any]) -> str | None:
    """Return an exact LN identifier value without guessing from other fields."""
    for identifier in value.get("identifiers") or []:
        if not isinstance(identifier, Mapping):
            continue
        if any(
            isinstance(coding, Mapping)
            and coding.get("system")
            == "http://terminology.hl7.org/CodeSystem/v2-0203"
            and coding.get("code") == "LN"
            for coding in identifier.get("type_codes") or []
        ):
            return _text(identifier.get("value"))
    return None


def _license_display(value: Mapping[str, Any]) -> str:
    number = license_number(value)
    if number is not None:
        return f"License number: {number}" if number else "License"
    return ""


def _acceptance_display(value: Any) -> str:
    codes = _labels_from_codes(value)
    labels = [
        _NEW_PATIENT_LABELS.get(
            "".join(character for character in code.casefold() if character.isalnum()),
            code,
        )
        for code in codes
    ]
    return "; ".join(dict.fromkeys(labels))


def _role_context_display(value: Mapping[str, Any]) -> str:
    parts = _labels_from_codes(value.get("specialty_codes"))
    if not parts:
        parts = _labels_from_codes(value.get("role_codes"))
    acceptance = _acceptance_display(
        value.get("new_patient_acceptance") or value.get("accepting_patients")
    )
    if acceptance:
        parts.append(acceptance)
    if value.get("telehealth"):
        parts.append("Telehealth available")
    if value.get("accepting_medicaid") is True:
        parts.append("Accepting Medicaid")
    return " — ".join(dict.fromkeys(parts))


def _boolean_display(fact_type: str, value: Mapping[str, Any]) -> str:
    for field_name in ("accepted", "available", "enabled", "reported"):
        if isinstance(value.get(field_name), bool):
            answer = "Yes" if value[field_name] else "No"
            return f"{_human_label(fact_type)}: {answer}"
    return ""


def _service_display(value: Mapping[str, Any]) -> str:
    name = _direct_label(value)
    if name:
        return name
    for field_name in ("specialty_codes", "type_codes", "category_codes"):
        labels = _joined_code_labels(value.get(field_name))
        if labels:
            return labels
    return _text(value.get("extra_details")) or _text(value.get("comment"))


def _endpoint_display(value: Mapping[str, Any]) -> str:
    name = _direct_label(value)
    connection = _text(value.get("connection_type_display"))
    address = _text(value.get("address"))
    return " — ".join(part for part in (name, connection, address) if part)


def _organization_display(value: Mapping[str, Any]) -> str:
    name = _direct_label(value)
    if not name:
        return ""
    context_parts = []
    if value.get("address_status") == "payer_directory_candidate":
        context_parts.append("payer-directory candidate location")
    if value.get("tin_status") == "unavailable_from_uhc_source":
        context_parts.append("TIN unavailable from UHC source")
    if context_parts:
        return f"{name} — {'; '.join(context_parts)}"
    code = _text(value.get("code"))
    return (
        f"{name} ({code})"
        if code and name.casefold() != code.casefold()
        else name
    )


def _plan_membership_display(
    membership_by_field: Mapping[str, Any],
) -> str:
    organization = membership_by_field.get("participating_organization")
    organization_name = (
        _direct_label(organization)
        if isinstance(organization, Mapping)
        else ""
    )
    plan_scope = membership_by_field.get("plan_scope")
    plan_id = (
        _text(plan_scope.get("plan_id"))
        if isinstance(plan_scope, Mapping)
        else ""
    )
    plan_year = (
        _text(plan_scope.get("plan_year"))
        if isinstance(plan_scope, Mapping)
        else ""
    )
    plan_label = plan_id
    if plan_label and plan_year:
        plan_label = f"{plan_label} ({plan_year})"
    if not plan_label:
        plan_label = _list_summary(
            membership_by_field.get("insurance_plan_refs") or []
        )
    subject = " for ".join(
        part for part in (organization_name, plan_label) if part
    )
    prefix = (
        f"Payer-reported plan membership: {subject}"
        if subject
        else "Payer-reported plan membership"
    )
    if membership_by_field.get("ownership_status") == "not_asserted":
        return f"{prefix}; ownership not asserted"
    return prefix


def _mapping_summary(value: Mapping[str, Any]) -> str:
    direct = _direct_label(value)
    if direct:
        code = _text(value.get("code"))
        if code and direct.casefold() != code.casefold():
            return f"{direct} ({code})"
        return direct
    for field_name, field_value in value.items():
        if field_name.endswith(_REFERENCE_SUFFIXES):
            continue
        if isinstance(field_value, (str, int, float, bool)) and _text(field_value):
            return f"{_human_label(field_name)}: {_text(field_value)}"
        labels = _labels_from_codes(field_value)
        if labels:
            return "; ".join(labels)
    return ""


def _list_summary(value: list[Any]) -> str:
    labels = [
        _code_label(item) if isinstance(item, Mapping) else _text(item)
        for item in value
    ]
    return "; ".join(dict.fromkeys(label for label in labels if label))


def display_value(fact_type: str, fact_value: Any) -> str:
    """Return a concise label while keeping structured details in ``value``."""
    if isinstance(fact_value, str):
        decoded_structure = _decoded_structure(fact_value)
        if decoded_structure is not None:
            return display_value(fact_type, decoded_structure)
        if _is_structural_text(fact_value):
            return _human_label(fact_type)
        return fact_value.strip() or _human_label(fact_type)
    if not isinstance(fact_value, (Mapping, list)):
        return _text(fact_value) or _human_label(fact_type)
    if isinstance(fact_value, list):
        return _list_summary(fact_value) or _human_label(fact_type)

    specialized_display_by_type = {
        "age": _age_display,
        "years_of_practice": _practice_display,
        "contact": _contact_display,
        "credential": _qualification_display,
        "qualification": _qualification_display,
        "taxonomy_qualification": _qualification_display,
        "qualification_detail": _qualification_display,
        "license": _license_display,
        "area_of_expertise": _qualification_display,
        "board_certification": _qualification_display,
        "role_context": _role_context_display,
        "service": _service_display,
        "endpoint": _endpoint_display,
        "organization": _organization_display,
        "plan_membership": _plan_membership_display,
    }
    specialized_formatter = specialized_display_by_type.get(fact_type)
    if specialized_formatter:
        specialized_display = specialized_formatter(fact_value)
        if specialized_display:
            return specialized_display
    if fact_type == "language":
        language_display = _joined_code_labels(fact_value.get("codes"))
        if language_display:
            return language_display
    if fact_type in {"specialty", "role"}:
        coding_display = _code_label(fact_value)
        if coding_display:
            return coding_display
    if fact_type == "new_patient_acceptance":
        acceptance_display = _acceptance_display(fact_value)
        if acceptance_display:
            return acceptance_display
    boolean_display = _boolean_display(fact_type, fact_value)
    return boolean_display or _mapping_summary(fact_value) or _human_label(fact_type)
