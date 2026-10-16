# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pure profile graph projection; callers supply verified source and office proof."""

import copy
import json
import re

MAX_ROWS = 5000
MAX_BYTES = 8 * 1024 * 1024
_RESOURCE_ID = re.compile(r"[A-Za-z0-9.-]{1,64}")
_TYPES = {
    "Practitioner",
    "PractitionerRole",
    "OrganizationAffiliation",
    "Organization",
    "Location",
    "InsurancePlan",
    "HealthcareService",
    "Endpoint",
}
_ARRAY_REFS = {
    "location_refs": "Location",
    "network_refs": "Organization",
    "insurance_plan_refs": "InsurancePlan",
    "healthcare_service_refs": "HealthcareService",
    "endpoint_refs": "Endpoint",
    "coverage_area_refs": "Location",
}
_SCALAR_REFS = {
    "practitioner_ref": "Practitioner",
    "organization_ref": "Organization",
    "participating_organization_ref": "Organization",
    "provided_by_ref": "Organization",
    "managing_organization_ref": "Organization",
}
_ASSOCIATION_KEYS = {"role_type", "role_id", "location_resource_id", "network_resource_id"}
_INDEPENDENT_ROLE = {
    "resource_id",
    "npi",
    "practitioner_ref",
    "active",
    "effective_start",
    "effective_end",
    "period_start",
    "period_end",
    "updated_at",
    "accepting_medicaid",
}
_PRIVATE_CONTEXT = {"plan_scope", "source_lineage", "part_of_ref"}
_ORGANIZATION_ADDRESS = {
    "address",
    "addresses",
    "address_json",
    "canonical_address_json",
    "candidate_addresses",
    "first_line",
    "second_line",
    "city_name",
    "state_name",
    "postal_code",
    "country_code",
    "lat",
    "long",
    "latitude",
    "longitude",
}


def _invalid():
    raise ValueError("profile_scoped_resources_invalid")


def _size(document):
    try:
        encoded = json.dumps(document, ensure_ascii=False, allow_nan=False).encode("utf-8")
    except ValueError, TypeError, RecursionError, UnicodeError:
        _invalid()
    if len(encoded) > MAX_BYTES:
        _invalid()
    pending_keys = [document]
    while pending_keys:
        value = pending_keys.pop()
        if type(value) is dict:
            if any(type(key) is not str for key in value):
                _invalid()
            pending_keys.extend(value.values())
        elif type(value) in {list, tuple}:
            pending_keys.extend(value)
    return len(encoded)


def _identifier(value):
    if type(value) is not str or _RESOURCE_ID.fullmatch(value) is None:
        _invalid()
    return value


def _reference(value, kind):
    """Accept source-local identities only; endpoint/version proof belongs to callers."""
    if type(value) is not str:
        _invalid()
    identifier = value[len(kind) + 1 :] if value.startswith(kind + "/") else value
    return kind, _identifier(identifier)


def _excluded_network_resources(identifiers):
    if type(identifiers) not in {list, tuple} or len(identifiers) > MAX_ROWS:
        _invalid()
    excluded_ids = set()
    for identifier in identifiers:
        _identifier(identifier)
        if identifier in excluded_ids:
            _invalid()
        excluded_ids.add(identifier)
    return excluded_ids


def _canonical_references(payload_by_field, excluded_ids):
    """Normalize copied references and remove exact denied network identities."""
    for field, kind in _ARRAY_REFS.items():
        if payload_by_field.get(field) is not None:
            references_by_id = {}
            for reference in payload_by_field[field]:
                _, identifier = _reference(reference, kind)
                if kind != "Organization" or identifier not in excluded_ids:
                    references_by_id.setdefault(identifier, kind + "/" + identifier)
            payload_by_field[field] = list(references_by_id.values())
    for field, kind in _SCALAR_REFS.items():
        if payload_by_field.get(field) is not None:
            _, identifier = _reference(payload_by_field[field], kind)
            if kind == "Organization" and identifier in excluded_ids:
                payload_by_field.pop(field)
            else:
                payload_by_field[field] = kind + "/" + identifier


def _validate_references(payload_by_field):
    for field, kind in _ARRAY_REFS.items():
        values = payload_by_field.get(field)
        if values is not None:
            if type(values) is not list:
                _invalid()
            for value in values:
                _reference(value, kind)
    for field, kind in _SCALAR_REFS.items():
        if payload_by_field.get(field) is not None:
            _reference(payload_by_field[field], kind)
    if payload_by_field.get("npi") is not None and (
        type(payload_by_field["npi"]) not in {str, int}
        or re.fullmatch(r"[12][0-9]{9}", str(payload_by_field["npi"])) is None
    ):
        _invalid()
    if (
        payload_by_field.get("accepting_medicaid") is not None
        and type(payload_by_field["accepting_medicaid"]) is not bool
    ):
        _invalid()


def _index(resources):
    resources_by_key = {}
    for row in resources:
        if type(row) is not dict or set(row) != {"resource_type", "resource_id", "payload_json"}:
            _invalid()
        kind, identifier, payload_by_field = row["resource_type"], _identifier(row["resource_id"]), row["payload_json"]
        if (
            type(kind) is not str
            or kind not in _TYPES
            or type(payload_by_field) is not dict
            or payload_by_field.get("resource_id") != identifier
        ):
            _invalid()
        key = kind, identifier
        if key in resources_by_key:
            _invalid()
        _validate_references(payload_by_field)
        resources_by_key[key] = row
    return resources_by_key


def _is_requested_provider(row, resources_by_key, npi):
    payload_by_field = row["payload_json"]
    supplied = payload_by_field.get("npi")
    field, kind = (
        ("practitioner_ref", "Practitioner")
        if row["resource_type"] == "PractitionerRole"
        else ("participating_organization_ref", "Organization")
    )
    reference = payload_by_field.get(field)
    provider = resources_by_key.get(_reference(reference, kind)) if reference is not None else None
    if reference is not None and provider is None:
        _invalid()
    linked = provider["payload_json"].get("npi") if provider else None
    if supplied is not None and linked is not None and str(supplied) != str(linked):
        _invalid()
    return str(supplied if supplied is not None else linked) == npi


def _contains(payload_by_field, field, kind, identifier):
    return any(_reference(value, kind)[1] == identifier for value in payload_by_field.get(field) or [])


def _association(association, resources_by_key, npi):
    if type(association) is not dict or set(association) not in (_ASSOCIATION_KEYS, _ASSOCIATION_KEYS | {"plan_id"}):
        _invalid()
    association_by_field = {**association, "plan_id": association.get("plan_id")}
    if type(association_by_field["role_type"]) is not str or association_by_field["role_type"] not in {
        "PractitionerRole",
        "OrganizationAffiliation",
    }:
        _invalid()
    for field in ("role_id", "location_resource_id", "network_resource_id"):
        _identifier(association_by_field[field])
    role = resources_by_key.get((association_by_field["role_type"], association_by_field["role_id"]))
    location, network, plan = (
        association_by_field["location_resource_id"],
        association_by_field["network_resource_id"],
        association_by_field["plan_id"],
    )
    if (
        role is None
        or not _is_requested_provider(role, resources_by_key, npi)
        or ("Location", location) not in resources_by_key
        or ("Organization", network) not in resources_by_key
    ):
        _invalid()
    payload_by_field = role["payload_json"]
    if not _contains(payload_by_field, "location_refs", "Location", location):
        _invalid()
    direct = _contains(payload_by_field, "network_refs", "Organization", network)
    if plan is not None:
        _identifier(plan)
        plan_row = resources_by_key.get(("InsurancePlan", plan))
        if (
            plan_row is None
            or not _contains(payload_by_field, "insurance_plan_refs", "InsurancePlan", plan)
            or not _contains(plan_row["payload_json"], "network_refs", "Organization", network)
        ):
            _invalid()
    elif not direct:
        _invalid()
    return association_by_field


def _allowed(association):
    return {
        "Location": association["location_resource_id"],
        "Organization": association["network_resource_id"],
        "InsurancePlan": association["plan_id"],
    }


def _project(row, association, excluded_ids):
    projected_by_key = copy.deepcopy(row)
    payload_by_field = projected_by_key["payload_json"]
    for field in _PRIVATE_CONTEXT:
        payload_by_field.pop(field, None)
    if row["resource_type"] == "Organization":
        for field in _ORGANIZATION_ADDRESS:
            payload_by_field.pop(field, None)
    allowed = _allowed(association)
    for field, kind in _ARRAY_REFS.items():
        if (
            field in payload_by_field
            and payload_by_field[field] is not None
            and field not in {"healthcare_service_refs", "endpoint_refs"}
        ):
            payload_by_field[field] = [
                value for value in payload_by_field[field] if _reference(value, kind)[1] == allowed[kind]
            ]
    _canonical_references(payload_by_field, excluded_ids)
    return projected_by_key


def _dependencies(row):
    payload_by_field = row["payload_json"]
    for field, kind in _SCALAR_REFS.items():
        if payload_by_field.get(field) is not None:
            yield _reference(payload_by_field[field], kind)
    for field in ("healthcare_service_refs", "endpoint_refs"):
        for value in payload_by_field.get(field) or []:
            yield _reference(value, _ARRAY_REFS[field])


def _is_service_in_association(row, association):
    if row["resource_type"] != "HealthcareService":
        return True
    for field, kind in (
        ("location_refs", "Location"),
        ("network_refs", "Organization"),
        ("insurance_plan_refs", "InsurancePlan"),
    ):
        values = row["payload_json"].get(field)
        if values and not any(_reference(value, kind)[1] == _allowed(association)[kind] for value in values):
            return False
    return True


def _bundle(association, resources_by_key, excluded_ids):
    pending_keys = [
        (association["role_type"], association["role_id"]),
        ("Location", association["location_resource_id"]),
        ("Organization", association["network_resource_id"]),
    ]
    if association["plan_id"] is not None:
        pending_keys.append(("InsurancePlan", association["plan_id"]))
    projected_by_key = {}
    while pending_keys:
        key = pending_keys.pop()
        if key in projected_by_key:
            continue
        if key not in resources_by_key:
            _invalid()
        if not _is_service_in_association(resources_by_key[key], association):
            continue
        row = _project(resources_by_key[key], association, excluded_ids)
        projected_by_key[key] = row
        pending_keys.extend(_dependencies(row))
    for row in projected_by_key.values():
        payload_by_field = row["payload_json"]
        if payload_by_field.get("healthcare_service_refs") is not None:
            payload_by_field["healthcare_service_refs"] = [
                value
                for value in payload_by_field["healthcare_service_refs"]
                if _reference(value, "HealthcareService") in projected_by_key
            ]
    return {"association": association, "resources": [projected_by_key[key] for key in sorted(projected_by_key)]}


def _independent(resources_by_key, npi, excluded_ids):
    rows = []
    for key in sorted(resources_by_key):
        row, payload_by_field = resources_by_key[key], resources_by_key[key]["payload_json"]
        if key[0] == "Practitioner" and str(payload_by_field.get("npi")) == npi:
            projected_by_key = copy.deepcopy(row)
            for field in _PRIVATE_CONTEXT:
                projected_by_key["payload_json"].pop(field, None)
            _canonical_references(projected_by_key["payload_json"], excluded_ids)
            rows.append(projected_by_key)
        elif (
            key[0] == "PractitionerRole"
            and payload_by_field.get("accepting_medicaid") is not None
            and _is_requested_provider(row, resources_by_key, npi)
        ):
            projected_by_key = {
                **row,
                "payload_json": copy.deepcopy(
                    {field: value for field, value in payload_by_field.items() if field in _INDEPENDENT_ROLE}
                ),
            }
            _canonical_references(projected_by_key["payload_json"], excluded_ids)
            rows.append(projected_by_key)
    return rows


def project_scoped_profile_resources(resources, npi, associations, *, excluded_network_resource_ids=()):
    """Project one verified source graph; no source admission or access authority is granted.

    Return independent facts and distinct per-association bundles. Callers must keep
    bundles separate in evidence SQL and apply their existing company/group policy.
    Excluded source-local Organization IDs come only from the caller's same-source
    approved network bindings; they do not infer employer/company/group authority.
    """
    excluded_ids = _excluded_network_resources(excluded_network_resource_ids)
    if type(npi) is not str or re.fullmatch(r"[12][0-9]{9}", npi) is None:
        _invalid()
    if (
        type(resources) not in {list, tuple}
        or type(associations) not in {list, tuple}
        or len(resources) > MAX_ROWS
        or len(associations) > MAX_ROWS
    ):
        _invalid()
    _size([resources, associations, excluded_network_resource_ids] if excluded_ids else [resources, associations])
    resources_by_key = _index(resources)
    projection_by_field = {"independent_resources": _independent(resources_by_key, npi, excluded_ids), "bundles": []}
    seen_association_keys = set()
    total_bytes = _size(projection_by_field)
    for association in associations:
        qualified = _association(association, resources_by_key, npi)
        if qualified["network_resource_id"] in excluded_ids:
            _invalid()
        association_key_values = tuple(
            qualified[field]
            for field in ("role_type", "role_id", "location_resource_id", "network_resource_id", "plan_id")
        )
        if association_key_values in seen_association_keys:
            _invalid()
        seen_association_keys.add(association_key_values)
        bundle = _bundle(qualified, resources_by_key, excluded_ids)
        total_bytes += _size(bundle) + 2
        if total_bytes > MAX_BYTES:
            _invalid()
        projection_by_field["bundles"].append(bundle)
    _size(projection_by_field)
    return projection_by_field
