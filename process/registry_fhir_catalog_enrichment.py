# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Source relationship projection against caller-pinned approved organization maps."""

from __future__ import annotations

import hashlib
import re
from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timezone
from uuid import NAMESPACE_URL, UUID, uuid5

from process.registry_fhir_catalog_extraction import (
    DEFAULT_BYTES,
    MAX_BYTES,
    FHIRCatalogOrganization,
    FHIRCatalogReferenceEdge,
    fhir_catalog_json_bytes,
    fhir_catalog_text,
    fhir_network_reference_target,
    fhir_source_uri,
)

MAX_RECORDS = 1000
_GROUP_KINDS = ("corporate_parent", "naic_group")
_RELATIONSHIPS = ("reported_affiliation", "verified_ownership", "verified_no_affiliation")
# Exact HL7 identifier systems; source names and pseudo-identifiers are not identity evidence.
_IDENTIFIER_SYSTEMS = {
    "urn:oid:2.16.840.1.113883.4.4": "ein",
    "urn:oid:2.16.840.1.113883.6.300": "naic_company",
}


class FHIRCatalogEnrichmentError(ValueError):
    """Refuse malformed or overlarge projection inputs without changing any registry."""


@dataclass(frozen=True)
class FHIRCatalogEnrichmentContext:
    """Approval, source admission and crosswalk custody remain the caller's responsibility."""

    source_id: str
    as_of: date
    approved_companies: tuple[dict, ...]
    approved_groups: tuple[dict, ...]
    endpoint_id: str
    company_crosswalk: tuple[tuple[str, str], ...] = ()
    source_uri: str | None = None


@dataclass(frozen=True)
class FHIRCatalogEnrichedRelationship:
    document: dict
    network_resource_id: str | None
    company_search: str
    plan_search: str


def _fail():
    raise FHIRCatalogEnrichmentError("registry_fhir_catalog_enrichment_invalid")


def fhir_company_scope(source_id):
    """Return a bounded explicit source scope, without admitting a company assertion."""
    if fhir_catalog_text(source_id, 128, strip=False) != source_id or not source_id or source_id.strip() != source_id:
        _fail()
    return "fhir:" + hashlib.sha256(source_id.encode()).hexdigest()


def fhir_catalog_organization_id(endpoint_id, organization_id, kind):
    """Keep imported organization identity independent of names and source editions."""
    if kind not in {"company", "group"} or not fhir_catalog_text(endpoint_id, 64, strip=False):
        _fail()
    if type(organization_id) is not str or re.fullmatch(r"[A-Za-z0-9.-]{1,64}", organization_id) is None:
        _fail()
    key = fhir_catalog_json_bytes([endpoint_id, organization_id]).decode()
    return str(uuid5(NAMESPACE_URL, "registry-fhir-catalog/" + kind + "/v1:" + key))


def _uuid_text(identifier):
    try:
        parsed = UUID(identifier) if type(identifier) is str else None
    except ValueError:
        _fail()
    if parsed is None or not parsed.int or str(parsed) != identifier:
        _fail()
    return identifier


def _date_text(value):
    if value is None:
        return None
    if type(value) is date:
        return value
    if type(value) is not str or len(value) != 10:
        _fail()
    try:
        parsed = date.fromisoformat(value)
    except ValueError:
        _fail()
    if parsed.isoformat() != value:
        _fail()
    return parsed


def _period(document):
    start, end = _date_text(document.get("valid_from")), _date_text(document.get("valid_to"))
    if start is not None and end is not None and end < start:
        _fail()
    return start, end


def _is_effective(period, as_of):
    start, end = period
    return (start is None or start <= as_of) and (end is None or as_of <= end)


def _plan_period(edge):
    bounds = []
    for text in (edge.valid_from, edge.valid_to):
        if text is None:
            bounds.append(None)
            continue
        try:
            if len(text) == 10:
                bounds.append(_date_text(text))
            elif re.fullmatch(
                r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]+)?"
                r"(?:Z|[+-](?:(?:0[0-9]|1[0-3]):[0-5][0-9]|14:00))",
                text,
            ):
                bounds.append(datetime.fromisoformat(text).date())
            else:
                return None
        except ValueError, FHIRCatalogEnrichmentError:
            return None
    if all(bounds) and bounds[1] < bounds[0]:
        return None
    return tuple(bounds)


def _has_overlap(first, second):
    first_start, first_end = first
    second_start, second_end = second
    return not (
        first_end is not None
        and second_start is not None
        and first_end < second_start
        or second_end is not None
        and first_start is not None
        and second_end < first_start
    )


def _record_map(documents, identity):
    if type(documents) is not tuple or len(documents) > MAX_RECORDS:
        _fail()
    records_by_id = {}
    for document in documents:
        if type(document) is not dict:
            _fail()
        identifier = _uuid_text(document.get(identity))
        if identifier in records_by_id or not document.get("display_name"):
            _fail()
        if fhir_catalog_text(document["display_name"], 512) != document["display_name"]:
            _fail()
        if type(document.get("archived", False)) is not bool:
            _fail()
        records_by_id[identifier] = document
    return records_by_id


def _approved_identifiers(companies_by_id, as_of):
    companies_by_identifier = {}
    for company_id, company in companies_by_id.items():
        assertions = company.get("identifier_assertions", [])
        if type(assertions) is not list or len(assertions) > 128:
            _fail()
        for assertion in assertions:
            if type(assertion) is not dict:
                _fail()
            kind, value = assertion.get("identifier_system"), assertion.get("identifier_value")
            if kind not in {"ein", "naic_company"}:
                continue
            period = _period(assertion)
            if period[0] is None or not fhir_catalog_text(assertion.get("identifier_scope"), 128):
                _fail()
            if not _is_effective(period, as_of):
                continue
            width = 9 if kind == "ein" else 5
            if (
                type(value) is not str
                or re.fullmatch(r"[0-9]{" + str(width) + "}", value) is None
                or set(value) == {"0"}
            ):
                _fail()
            companies_by_identifier.setdefault((kind, value), set()).add(company_id)
    return companies_by_identifier


def _is_source_bound_current(value, as_of, is_start):
    if value is None:
        return True
    try:
        if type(value) is str and len(value) == 10:
            bound = _date_text(value)
            return as_of >= bound if is_start else as_of <= bound
        if type(value) is str and len(value) > 10 and value[10] == "T":
            bound = datetime.fromisoformat(value)
            if bound.tzinfo is None:
                return None
            instant = datetime.combine(as_of, time.min, timezone.utc)
            return instant >= bound if is_start else instant <= bound
    except ValueError, FHIRCatalogEnrichmentError:
        return None
    return None


def _is_source_identifier_current(identifier, as_of):
    """Evaluate timestamps at frozen midnight UTC; partial precision proves no mapping."""
    period = identifier.get("period") or {}
    if type(period) is not dict:
        return None
    values = (identifier.get("period_start", period.get("start")), identifier.get("period_end", period.get("end")))
    for index, value in enumerate(values):
        is_current = _is_source_bound_current(value, as_of, index == 0)
        if is_current is not True:
            return is_current
    return True


def _source_identifier_keys(organization, as_of):
    keys, has_unverified_period = [], False
    for identifier in organization.identifiers:
        system = identifier.get("system")
        kind = _IDENTIFIER_SYSTEMS.get(system) if type(system) is str else None
        value = identifier.get("value")
        if kind is None or type(value) is not str:
            continue
        value = value.strip()
        if kind == "ein" and re.fullmatch(r"[0-9]{2}-[0-9]{7}", value):
            value = value.replace("-", "")
        width = 9 if kind == "ein" else 5
        if re.fullmatch(r"[0-9]{" + str(width) + "}", value) is None or set(value) == {"0"}:
            continue
        is_current = _is_source_identifier_current(identifier, as_of)
        if is_current is None:
            has_unverified_period = True
        elif is_current:
            keys.append((kind, value))
    return keys, has_unverified_period


def _crosswalk_map(context, organizations_by_id):
    if type(context.company_crosswalk) is not tuple or len(context.company_crosswalk) > MAX_RECORDS:
        _fail()
    crosswalk_by_organization = {}
    for binding in context.company_crosswalk:
        if type(binding) is not tuple or len(binding) != 2:
            _fail()
        resource_id, company_id = binding
        if type(resource_id) is not str or re.fullmatch(r"[A-Za-z0-9.-]{1,64}", resource_id) is None:
            _fail()
        _uuid_text(company_id)
        # Absent Organizations stay unresolved even when a retained policy mentions their IDs.
        if resource_id in organizations_by_id:
            crosswalk_by_organization.setdefault(resource_id, set()).add(company_id)
    return crosswalk_by_organization


def _company_resolution(organization, crosswalk_ids, companies_by_identifier, companies_by_id, context):
    if organization is None:
        return None, "unresolved", "source_organization_missing"
    identifier_keys, has_unverified_period = _source_identifier_keys(organization, context.as_of)
    identifier_ids = set().union(*(companies_by_identifier.get(key, set()) for key in identifier_keys))
    candidates = crosswalk_ids or identifier_ids
    if len(candidates) > 1:
        return None, "conflict", "company_mapping_conflict"
    if not candidates:
        if organization.name:
            company_id = fhir_catalog_organization_id(context.endpoint_id, organization.resource_id, "company")
            company_by_field = companies_by_id.get(company_id)
            if company_by_field is None:
                company_by_field = {
                    "company_id": company_id,
                    "display_name": organization.name,
                    "aliases": list(organization.aliases),
                }
            if not company_by_field.get("archived", False):
                return company_by_field, "resolved", None
        reason = "company_identifier_period_unverified" if has_unverified_period else "company_mapping_missing"
        return None, "unresolved", reason
    company_by_field = companies_by_id.get(next(iter(candidates)))
    if company_by_field is None or company_by_field.get("archived", False):
        return None, "unresolved", "approved_company_unavailable"
    if crosswalk_ids and identifier_ids - crosswalk_ids:
        return company_by_field, "conflict", "company_identifier_conflict"
    return company_by_field, "resolved", None


def _group_document(assertion, groups_by_id):
    group_id = assertion.get("group_id")
    target = groups_by_id.get(group_id) if group_id is not None else None
    kind, relationship = assertion.get("group_kind"), assertion.get("relationship_kind")
    if kind not in _GROUP_KINDS or relationship not in _RELATIONSHIPS:
        _fail()
    period = _period(assertion)
    if group_id is not None:
        _uuid_text(group_id)
    if relationship == "verified_no_affiliation" and (group_id is not None or period[0] is None):
        _fail()
    if target is not None and (target.get("archived", False) or target.get("group_kind") != kind):
        target = None
    return {
        "group_id": group_id if target is not None else None,
        "display_name": target["display_name"] if target is not None else None,
        "group_kind": kind,
        "relationship_kind": relationship,
        "valid_from": period[0].isoformat() if period[0] is not None else None,
        "valid_to": period[1].isoformat() if period[1] is not None else None,
    }


def _group_assertions(company):
    assertions = company.get("group_assertions", [])
    if type(assertions) is not list or len(assertions) > 128:
        _fail()
    for assertion in assertions:
        if type(assertion) is not dict or assertion.get("company_id") != company["company_id"]:
            _fail()
        _uuid_text(assertion.get("assertion_id"))
        if assertion.get("group_kind") not in _GROUP_KINDS or assertion.get("relationship_kind") not in _RELATIONSHIPS:
            _fail()
        provenance = assertion.get("provenance")
        if type(provenance) is not dict or provenance.get("kind") not in {"manual_reference", "source_reference"}:
            _fail()
        _period(assertion)
    return assertions


def _group_rows(company, organization, organizations_by_id, groups_by_id, edge, context):
    for group_row in company_group_rows(company, groups_by_id, context.as_of, plan_period=_plan_period(edge)):
        group, status, reason, _ = group_row
        if reason == "group_affiliation_unknown" and group["group_kind"] == "corporate_parent":
            imported = _imported_parent(organization, organizations_by_id, groups_by_id, context)
            if imported is not None:
                yield imported, "resolved", None, "source-parent:" + imported["group_id"]
                continue
        yield group_row


def _imported_parent(organization, organizations_by_id, groups_by_id, context):
    if organization is None or organization.part_of_ref is None:
        return None
    resource_id, reason = fhir_network_reference_target(organization.part_of_ref, fhir_source_uri(context.source_uri))
    parent = organizations_by_id.get(resource_id)
    if reason is not None or parent is None or not parent.name:
        return None
    if parent.resource_id == organization.resource_id:
        return None
    group_id = fhir_catalog_organization_id(context.endpoint_id, parent.resource_id, "group")
    approved = groups_by_id.get(group_id)
    if approved is not None and (approved.get("archived", False) or approved.get("group_kind") != "corporate_parent"):
        return None
    return {
        "group_id": group_id,
        "display_name": approved["display_name"] if approved else parent.name,
        "group_kind": "corporate_parent",
        "relationship_kind": "reported_affiliation",
        "valid_from": None,
        "valid_to": None,
    }


def company_group_rows(company, groups_by_id, as_of, *, plan_period=(None, None)):
    """Resolve effective approved groups, with optional plan-period restrictions."""
    assertions = _group_assertions(company)
    for kind in _GROUP_KINDS:
        selected_assertions = [
            assertion
            for assertion in assertions
            if assertion.get("group_kind") == kind
            and _is_effective(_period(assertion), as_of)
            and plan_period is not None
            and _has_overlap(_period(assertion), plan_period)
        ]
        manual_assertions = [
            assertion
            for assertion in selected_assertions
            if assertion.get("provenance", {}).get("kind") == "manual_reference"
        ]
        selected_assertions = manual_assertions or selected_assertions
        if not selected_assertions:
            unknown_group_by_field = {
                "group_id": None,
                "display_name": None,
                "group_kind": kind,
                "relationship_kind": "reported_affiliation",
                "valid_from": None,
                "valid_to": None,
            }
            yield (
                unknown_group_by_field,
                "unresolved",
                "plan_period_unverified" if plan_period is None else "group_affiliation_unknown",
                kind,
            )
            continue
        documents = [
            (_group_document(assertion, groups_by_id), assertion.get("assertion_id"))
            for assertion in selected_assertions
        ]
        has_conflict = len({(document["group_id"], document["relationship_kind"]) for document, _ in documents}) > 1
        for document, assertion_id in documents:
            if has_conflict:
                yield document, "conflict", "group_assertion_conflict", assertion_id
            elif document["group_id"] is None and document["relationship_kind"] != "verified_no_affiliation":
                yield document, "unresolved", "approved_group_unavailable", assertion_id
            else:
                yield document, "resolved", None, assertion_id


def _search_text(name, aliases):
    if type(aliases) not in {list, tuple} or len(aliases) > 100:
        _fail()
    names = [name] if name is not None else []
    names.extend(fhir_catalog_text(alias, 512) for alias in aliases)
    if any(name is None for name in names):
        _fail()
    return "\n".join(dict.fromkeys(names)).lower()


def _relationship(edge, context, role, reference, resolution, group_row):
    company, company_status, company_reason = resolution
    group, group_status, group_reason, assertion_id = group_row
    status, reason = company_status, company_reason
    if status == "resolved":
        status, reason = group_status, group_reason
    if edge.resolution_reason is not None:
        status, reason = "unresolved", edge.resolution_reason
    key = (
        context.source_id,
        edge.owner_resource_type,
        edge.owner_resource_id,
        edge.scope_path,
        edge.network_ref,
        role,
        reference,
        assertion_id,
    )
    plan = (
        None
        if edge.plan_resource_id is None
        else {
            "resource_id": edge.plan_resource_id,
            "scope_path": f"{edge.owner_resource_type}/{edge.owner_resource_id}{edge.scope_path}",
            "display_name": edge.plan_name,
            "valid_from": edge.valid_from,
            "valid_to": edge.valid_to,
        }
    )
    relationship_by_field = {
        "relationship_id": hashlib.sha256(fhir_catalog_json_bytes(key)).hexdigest(),
        "source_id": context.source_id,
        "plan": plan,
        "organization_reference": reference,
        "company_role": role,
        "company": None
        if company is None
        else {"company_id": company["company_id"], "display_name": company["display_name"]},
        "group": group,
        "resolution_status": status,
        "resolution_reason": reason,
    }
    return FHIRCatalogEnrichedRelationship(
        relationship_by_field,
        edge.network_resource_id,
        _search_text(company["display_name"], company.get("aliases", [])) if company is not None else "",
        _search_text(edge.plan_name, edge.plan_aliases),
    )


def _edge_relationships(edge, context, maps):
    organizations_by_id, companies_by_id, groups_by_id, crosswalk_by_organization, companies_by_identifier = maps
    source_uri = fhir_source_uri(context.source_uri)
    roles = [("insurer", edge.owner_ref)] if edge.plan_resource_id is not None else [(None, None)]
    if edge.administrator_ref is not None:
        roles.append(("administrator", edge.administrator_ref))
    for role, reference in roles:
        resource_id, reference_reason = fhir_network_reference_target(reference, source_uri)
        resolution = _company_resolution(
            organizations_by_id.get(resource_id),
            crosswalk_by_organization.get(resource_id, set()),
            companies_by_identifier,
            companies_by_id,
            context,
        )
        if reference_reason is not None:
            resolution = (None, "unresolved", "company_" + reference_reason)
        company = resolution[0]
        group_rows = (
            [(None, "unresolved", resolution[2], None)]
            if company is None
            else _group_rows(
                company, organizations_by_id.get(resource_id), organizations_by_id, groups_by_id, edge, context
            )
        )
        for group_row in group_rows:
            yield _relationship(edge, context, role, reference, resolution, group_row)


def enrich_fhir_catalog_edges(organizations, edges, context, *, max_output_bytes=DEFAULT_BYTES):
    """Project current context only; retain source and approved history with the caller."""
    if type(context) is not FHIRCatalogEnrichmentContext or type(context.as_of) is not date:
        _fail()
    fhir_company_scope(context.source_id)
    if type(max_output_bytes) is not int or not 1 <= max_output_bytes <= MAX_BYTES:
        _fail()
    for entries, expected_type in ((organizations, FHIRCatalogOrganization), (edges, FHIRCatalogReferenceEdge)):
        if (
            type(entries) is not tuple
            or len(entries) > MAX_RECORDS
            or any(type(entry) is not expected_type for entry in entries)
        ):
            _fail()
    companies_by_id = _record_map(context.approved_companies, "company_id")
    groups_by_id = _record_map(context.approved_groups, "group_id")
    organizations_by_id = {organization.resource_id: organization for organization in organizations}
    if len(organizations_by_id) != len(organizations):
        _fail()
    maps = (
        organizations_by_id,
        companies_by_id,
        groups_by_id,
        _crosswalk_map(context, organizations_by_id),
        _approved_identifiers(companies_by_id, context.as_of),
    )
    input_documents = [asdict(organization) for organization in organizations] + [asdict(edge) for edge in edges]
    if (
        len(
            fhir_catalog_json_bytes(
                (input_documents, context.approved_companies, context.approved_groups, context.company_crosswalk)
            )
        )
        > MAX_BYTES
    ):
        _fail()
    relationships, used_bytes, relationship_ids = [], 2, set()
    for edge in edges:
        for relationship in _edge_relationships(edge, context, maps):
            identifier = relationship.document["relationship_id"]
            if identifier in relationship_ids:
                continue
            used_bytes += len(fhir_catalog_json_bytes(asdict(relationship))) + bool(relationships)
            if used_bytes > max_output_bytes:
                _fail()
            relationship_ids.add(identifier)
            relationships.append(relationship)
    return tuple(relationships)
