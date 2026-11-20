# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Read one practitioner's roles from the current typed CMS National Provider Directory release."""

from __future__ import annotations

import json

from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from api.cms_npd_typed_queries import current_cms_release
from api.provider_directory_cms_generation import SOURCE
from api.provider_directory_entities_contract import DirectoryReadError
from api.provider_profile_display import display_value
from api.provider_profile_public_facts import _FHIR_CATEGORY_BY_FACT
from db.models import db
from process.entity_address_cutover_contract import postgres_sqlstate
from process.florida_mqa_profile import PROFILE_SCHEMA_VERSION

ROLE_LIMIT = 100
LIST_LIMIT = 20
SOURCE_KIND = "cms_npd"
# undefined_table, invalid_schema_name, insufficient_privilege: the release is gone or not readable here.
# An optional source must not take the whole profile down.
_MISSING_RELATION_STATES = frozenset({"42P01", "3F000", "42501"})

# Driven by the practitioner_role (practitioner_npi) index; every nested lookup is a primary-key probe.
_ROLE_SQL = """
WITH role AS (
  SELECT r.id, r.organization_id, r.new_patients, r.specialty_codes[1:{n}] AS specialty_codes,
    r.location_ids[1:{n}] AS location_ids, r.endpoint_ids[1:{n}] AS endpoint_ids,
    r.insurance_plan_ids[1:{n}] AS plan_ids,
    (coalesce(r.network_ids, ARRAY[]::text[]) || r.network_id)[1:{n}] AS network_ids
  FROM {s}.practitioner_role r
  WHERE r.practitioner_npi = :npi AND r.active IS NOT FALSE
    AND (r.period_end IS NULL OR r.period_end >= CURRENT_DATE)
  ORDER BY r.id LIMIT :role_limit
)
SELECT r.id::text AS role_id, r.new_patients, r.specialty_codes,
  o.name AS organization_name, o.npi AS organization_npi,
  (SELECT jsonb_agg(jsonb_build_object('name', l.name, 'phone', l.phones[1], 'line1', l.line1, 'line2', l.line2,
      'city', l.city, 'state', l.state, 'postal_code', l.postal_code) ORDER BY ref.ord)
     FROM unnest(r.location_ids) WITH ORDINALITY ref(id, ord) JOIN {s}.location l ON l.id = ref.id
     WHERE coalesce(l.status, 'active') = 'active') AS locations,
  (SELECT jsonb_agg(jsonb_build_object('name', e.name, 'connection_type', e.connection_type, 'address', e.address)
      ORDER BY ref.ord)
     FROM unnest(r.endpoint_ids) WITH ORDINALITY ref(id, ord) JOIN {s}.endpoint e ON e.id = ref.id
     WHERE coalesce(e.status, 'active') = 'active') AS endpoints,
  (SELECT jsonb_agg(jsonb_build_object('name', p.name, 'type', p.type_text) ORDER BY ref.ord)
     FROM unnest(r.plan_ids) WITH ORDINALITY ref(id, ord) JOIN {s}.insurance_plan p ON p.id = ref.id
     WHERE coalesce(p.status, 'active') = 'active'
       AND (p.period_end IS NULL OR p.period_end >= CURRENT_DATE)) AS plans,
  (SELECT jsonb_agg(n.name ORDER BY ref.ord)
     FROM unnest(r.network_ids) WITH ORDINALITY ref(id, ord) JOIN {s}.organization n ON n.id = ref.id
     WHERE n.name IS NOT NULL) AS networks
FROM role r LEFT JOIN {s}.organization o ON o.id = r.organization_id
ORDER BY r.id
"""


async def read_cms_npd_roles(session, npi):
    """Two statements in the caller's snapshot: the release pointer, then the bounded role read."""
    release = await current_cms_release(session, None)
    statement = text(_ROLE_SQL.format(s=release["schema"], n=LIST_LIMIT))
    result = await session.execute(statement, {"npi": npi, "role_limit": ROLE_LIMIT})
    return release, result.mappings().all()


async def fetch_cms_npd_projection(npi: int) -> dict | None:
    """Return the directory facts, or None when no current release can be read."""
    try:
        async with db.transaction() as session:  # a savepoint inside the profile snapshot
            release, role_rows = await read_cms_npd_roles(session, npi)
    except DirectoryReadError:
        return None
    except DBAPIError as error:
        if postgres_sqlstate(error) not in _MISSING_RELATION_STATES:
            raise
        return None
    return cms_npd_projection(npi, release, role_rows)


def _rows(value):
    if isinstance(value, str):
        value = json.loads(value)
    return value or []


def _role_facts(role):
    """(fact type, value, display) for one role; displays never carry postal fields."""
    organization = role["organization_name"]
    if organization:
        fact_by_field = {"name": organization}
        if role["organization_npi"] is not None:
            fact_by_field["npi"] = str(role["organization_npi"])
        yield "organization", fact_by_field, organization
    for code in role["specialty_codes"] or []:
        if code:
            yield "specialty", {"code": code}, display_value("specialty", {"code": code})
    if role["new_patients"]:
        fact_by_field = {"code": role["new_patients"]}
        display = display_value("new_patient_acceptance", fact_by_field)
        if organization:
            fact_by_field["organization_name"] = organization
            display = f"{display} — {organization}"
        yield "new_patient_acceptance", fact_by_field, display
    for location in _rows(role["locations"]):
        address_by_field = {
            field: location[field]
            for field in ("line1", "line2", "city", "state", "postal_code")
            if location.get(field)
        }
        fact_by_field = {"name": location.get("name"), "address": address_by_field or None}
        if location.get("phone"):
            fact_by_field |= {"system": "phone", "value": location["phone"], "use": "work"}
        fact_by_field = {field: field_value for field, field_value in fact_by_field.items() if field_value}
        if fact_by_field:
            yield "contact", fact_by_field, display_value("contact", fact_by_field)
    for endpoint in _rows(role["endpoints"]):
        fact_by_field = {field: field_value for field, field_value in endpoint.items() if field_value}
        if fact_by_field.get("address"):
            yield "endpoint", fact_by_field, display_value("endpoint", fact_by_field)
    for plan in _rows(role["plans"]):
        if plan.get("name"):
            fact_by_field = {field: field_value for field, field_value in plan.items() if field_value}
            yield "plan_membership", {"insurance_plan": fact_by_field}, f"Listed plan: {plan['name']}"
    for network in _rows(role["networks"]):
        yield "plan_membership", {"network": {"name": network}}, f"Listed network: {network}"


def cms_npd_projection(npi, release, role_rows):
    """Shape the roles like an additional state projection so the existing merge carries them."""
    if not role_rows:
        return None
    release_id = release["release_id"]
    published_at = release["observed_at"].isoformat() if release["observed_at"] is not None else None
    items_by_key = {}
    evidence_records = []
    for role in role_rows:
        record_id = f"{SOURCE}:{release_id}:PractitionerRole/{role['role_id']}"
        evidence_records.append(
            {
                "source_record_id": record_id,
                "source_key": SOURCE,
                "resource_type": "PractitionerRole",
                "resource_id": role["role_id"],
                "release_id": release_id,
                "published_at": published_at,
            }
        )
        for fact_type, fact_value, display in _role_facts(role):
            key = (fact_type, json.dumps(fact_value, sort_keys=True))
            if key in items_by_key:
                items_by_key[key]["source_record_ids"].append(record_id)
                items_by_key[key]["assertion_count"] += 1
                continue
            items_by_key[key] = _fact(fact_type, fact_value, display, record_id)
    categories_by_name = {}
    for profile_item in items_by_key.values():
        group = categories_by_name.setdefault(
            _FHIR_CATEGORY_BY_FACT[profile_item["type"]], {"availability": "available", "items": []}
        )
        group["items"].append(profile_item)
    return {
        "generation_id": release_id,
        "categories": categories_by_name,
        "source": {
            "source_key": SOURCE,
            "source_kind": SOURCE_KIND,
            "name": "CMS National Provider Directory",
            "agency": "Centers for Medicare & Medicaid Services",
            "jurisdiction": "US",
            "release_id": release_id,
            "published_at": published_at,
        },
        "evidence": {
            "schema_version": PROFILE_SCHEMA_VERSION,
            "npi": npi,
            "generation_id": release_id,
            "records": evidence_records,
        },
    }


def _fact(fact_type, value, display, record_id):
    assertion_by_field = {
        "source_kind": SOURCE_KIND,
        "assertion_type": "directory_reported",
        "verification_status": "not_independently_verified",
    }
    return {
        "type": fact_type,
        "display": display,
        "value": value,
        "source_record_id": record_id,
        "source_record_ids": [record_id],
        "source_kinds": [SOURCE_KIND],
        "assertion_type": assertion_by_field["assertion_type"],
        "verification_status": assertion_by_field["verification_status"],
        "assertions": [assertion_by_field],
        "assertion_count": 1,
        "quality_flags": [],
        "sensitive": False,
        "public_default": True,
    }
