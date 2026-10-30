# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Compose verified, nonrectangular profile inputs without reading source heaps."""

from __future__ import annotations

import json
import re
from datetime import date, datetime

from process import provider_directory_profile as profile

_RESOURCE = re.compile(r"[A-Za-z0-9.-]{1,64}")
_RESOURCE_TYPES = {
    "Practitioner": "practitioner",
    "PractitionerRole": "role",
    "Organization": "organization",
    "OrganizationAffiliation": "affiliation",
    "HealthcareService": "service",
    "Endpoint": "endpoint",
}
_INDEPENDENT_FACTS = (
    "name",
    "administrative_gender",
    "age",
    "years_of_practice",
    "taxonomy_qualification",
    "credential",
    "qualification",
    "qualification_detail",
    "language",
    "contact",
    "accepting_medicaid",
)
_SOURCE_FIELDS = {"source_id", "endpoint_id", "dataset_id", "canonical_api_base", "org_name", "plan_name"}
_ASSOCIATION_FIELDS = {"role_type", "role_id", "location_resource_id", "network_resource_id", "plan_id"}
MAX_RESOURCES = 5000
MAX_BYTES = 1024 * 1024


def _invalid():
    raise ValueError("scoped_profile_projection_invalid")


def _resources(resource_entries, source_id):
    if type(resource_entries) is not list or len(resource_entries) > MAX_RESOURCES:
        _invalid()
    normalized_resources = []
    seen_resource_ids = set()
    for resource in resource_entries:
        if type(resource) is not dict or set(resource) != {"resource_type", "resource_id", "payload_json"}:
            _invalid()
        resource_type = resource["resource_type"]
        resource_id = resource["resource_id"]
        resource_payload = resource["payload_json"]
        if (
            type(resource_type) is not str
            or resource_type not in {*_RESOURCE_TYPES, "Location", "InsurancePlan"}
            or type(resource_id) is not str
            or not _RESOURCE.fullmatch(resource_id)
        ):
            _invalid()
        if type(resource_payload) is not dict or (resource_type, resource_id) in seen_resource_ids:
            _invalid()
        if (
            resource_type == "InsurancePlan"
            and resource_payload.get("name") is not None
            and type(resource_payload["name"]) is not str
        ):
            _invalid()
        if (
            resource_payload.get("resource_id", resource_id) != resource_id
            or resource_payload.get("source_id", source_id) != source_id
        ):
            _invalid()
        seen_resource_ids.add((resource_type, resource_id))
        normalized_resources.append(
            {
                "resource_type": resource_type,
                "resource_id": resource_id,
                "payload_json": {**resource_payload, "resource_id": resource_id, "source_id": source_id},
            }
        )
    return normalized_resources


def _normalize_bundle(bundle, source_id, associations):
    if type(bundle) is not dict or set(bundle) != {"association", "resources"}:
        _invalid()
    association = bundle["association"]
    if type(association) is not dict or set(association) != _ASSOCIATION_FIELDS:
        _invalid()
    if type(association["role_type"]) is not str or association["role_type"] not in {
        "PractitionerRole",
        "OrganizationAffiliation",
    }:
        _invalid()
    if any(
        type(association[field]) is not str or not _RESOURCE.fullmatch(association[field])
        for field in ("role_id", "location_resource_id", "network_resource_id")
    ):
        _invalid()
    if association["plan_id"] is not None and (
        type(association["plan_id"]) is not str or not _RESOURCE.fullmatch(association["plan_id"])
    ):
        _invalid()
    association_keys = tuple(association[field] for field in sorted(_ASSOCIATION_FIELDS))
    if association_keys in associations:
        _invalid()
    associations.add(association_keys)
    resources = _resources(bundle["resources"], source_id)
    roles = [
        resource_entry
        for resource_entry in resources
        if resource_entry["resource_type"] in {"PractitionerRole", "OrganizationAffiliation"}
    ]
    if len(roles) != 1 or (roles[0]["resource_type"], roles[0]["resource_id"]) != (
        association["role_type"],
        association["role_id"],
    ):
        _invalid()
    resource_payload = roles[0]["payload_json"]
    references_by_field = {
        "location_refs": ["Location/" + association["location_resource_id"]],
        "network_refs": ["Organization/" + association["network_resource_id"]],
        "insurance_plan_refs": ["InsurancePlan/" + association["plan_id"]]
        if association["plan_id"] is not None
        else [],
    }
    for field, references in references_by_field.items():
        if (
            field in resource_payload
            and resource_payload[field] != references
            and not (field == "network_refs" and resource_payload[field] == [])
        ):
            _invalid()
    return {"association": dict(association), "resources": resources}


def normalize_scoped_profile_projection(projection: dict, *, source_id: str) -> str:
    """Validate loader-owned projected inputs; never infer or expand membership."""
    if type(projection) is not dict or set(projection) != {"independent_resources", "bundles"}:
        _invalid()
    independent = _resources(projection["independent_resources"], source_id)
    bundles = projection["bundles"]
    if type(bundles) is not list or len(bundles) > MAX_RESOURCES:
        _invalid()
    normalized_resources = []
    total = len(independent)
    associations = set()
    for bundle in bundles:
        normalized_bundle = _normalize_bundle(bundle, source_id, associations)
        total += len(normalized_bundle["resources"])
        if total > MAX_RESOURCES:
            _invalid()
        normalized_resources.append(normalized_bundle)
    projection_document_dict = {"independent_resources": independent, "bundles": normalized_resources}
    try:
        encoded = json.dumps(
            projection_document_dict, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
        )
        if len(encoded.encode("utf-8")) > MAX_BYTES:
            _invalid()
    except (TypeError, ValueError, UnicodeError, RecursionError) as error:
        raise ValueError("scoped_profile_projection_invalid") from error
    return encoded


_SCOPED_PROFILE_SQL = """WITH projection AS MATERIALIZED (SELECT CAST(:profile_projection AS jsonb) AS document),
      slices AS MATERIALIZED (
        SELECT 'independent'::text AS kind, document->'independent_resources' AS resources, NULL::jsonb AS association FROM projection
        UNION ALL
        SELECT 'bundle', bundle->'resources', bundle->'association' FROM projection
          CROSS JOIN LATERAL jsonb_array_elements(document->'bundles') AS item(bundle)
      ), unaggregated AS MATERIALIZED (
        SELECT slices.kind,evidence.* FROM slices CROSS JOIN LATERAL (
          WITH scoped_resources AS MATERIALIZED (
            SELECT resource_type,resource_id,payload_json,CAST(:profile_dataset_id AS varchar) AS dataset_id
              FROM jsonb_to_recordset(slices.resources) AS input(resource_type text,resource_id text,payload_json jsonb)
          ), scoped_source AS MATERIALIZED (
            SELECT CAST(:profile_source_id AS varchar) AS source_id, CAST(:profile_endpoint_id AS varchar) AS endpoint_id,
              CAST(:profile_api_base AS text) AS canonical_api_base, CAST(:profile_org_name AS text) AS org_name,
              CASE WHEN slices.kind='bundle' THEN (SELECT payload_json->>'name' FROM scoped_resources
                WHERE resource_type='InsurancePlan' AND resource_id=slices.association->>'plan_id') END AS plan_name
          ), {typed_ctes}, scoped_affiliation_edges AS MATERIALIZED (
            SELECT CAST(:profile_dataset_id AS varchar) AS dataset_id,resource_id AS affiliation_resource_id,
              {provider_id} AS participating_organization_resource_id FROM scoped_resources WHERE resource_type='OrganizationAffiliation'
          ) SELECT normalized.* FROM LATERAL ({evidence_sql}) AS normalized
        ) AS evidence
        WHERE (slices.kind='independent' AND evidence.fact_type IN ({independent_types}))
           OR (slices.kind='bundle' AND evidence.fact_type NOT IN ({independent_types}))
      ), projected_facts AS MATERIALIZED (SELECT {fact_columns} FROM unaggregated),
      projected_evidence AS MATERIALIZED (
        SELECT md5(concat_ws('|',npi::text,fact_type,fact_key,source_id,resource_type,resource_id,COALESCE(role_resource_id,''))) AS evidence_key,
          {evidence_columns} FROM projected_facts
      ), qualified_evidence AS MATERIALIZED (
        SELECT DISTINCT ON (evidence_key) * FROM projected_evidence ORDER BY evidence_key,observed_at DESC NULLS LAST
      ) SELECT {output}, CAST(:profile_as_of AS date) AS profile_as_of FROM LATERAL ({aggregate_sql}) AS aggregated
        CROSS JOIN LATERAL (SELECT octet_length(to_jsonb(aggregated)::text) AS response_bytes) AS response_bound"""


def _profile_row_types(source_metadata, row_type_refs, generation_id, profile_as_of, published_at):
    if type(source_metadata) is not dict or set(source_metadata) != _SOURCE_FIELDS:
        _invalid()
    for field in _SOURCE_FIELDS:
        metadata_value = source_metadata[field]
        if field in {"org_name", "plan_name", "canonical_api_base"} and metadata_value is None:
            continue
        if type(metadata_value) is not str or not metadata_value or len(metadata_value.encode("utf-8")) > 8192:
            _invalid()
    if type(row_type_refs) is not dict or set(row_type_refs) != set(_RESOURCE_TYPES):
        _invalid()
    type_refs_by_resource = {}
    for resource_type, reference in row_type_refs.items():
        if (
            type(reference) is not tuple
            or len(reference) != 2
            or any(type(part) is not str or not part for part in reference)
        ):
            _invalid()
        type_refs_by_resource[_RESOURCE_TYPES[resource_type]] = profile.qualified_table(*reference)
    if type(generation_id) is not str or re.fullmatch(r"[0-9a-f]{64}", generation_id) is None:
        _invalid()
    if type(profile_as_of) is not date or (
        published_at is not None and (type(published_at) is not datetime or published_at.tzinfo is None)
    ):
        _invalid()
    return type_refs_by_resource


def _profile_statement_parts(npi, type_refs_by_resource, published_at):
    relation_refs_by_name = {name + "_ref": "scoped_" + name for name in type_refs_by_resource}
    evidence_sql = profile.profile_evidence_select_sql(
        npi=npi,
        resource_type_refs=type_refs_by_resource,
        static_resource_rows=True,
        source_ref="scoped_source",
        dataset_resource_ref="scoped_resources",
        affiliation_organization_ref="scoped_affiliation_edges",
        **relation_refs_by_name,
    )
    typed_ctes = _static_resource_ctes()
    provider_id = profile.fhir_reference_resource_id_sql(
        "payload_json->>'participating_organization_ref'", "Organization"
    )
    independent_types = ",".join("'" + fact + "'" for fact in _INDEPENDENT_FACTS)
    fact_columns = ", ".join(
        ("CASE WHEN kind='bundle' THEN md5(fact_key || '|' || value_json::text) ELSE fact_key END AS fact_key")
        if column == "fact_key"
        else profile.quote_identifier(column)
        for column in profile.profile_evidence_columns()
        if column != "evidence_key"
    )
    evidence_columns = ", ".join(
        profile.quote_identifier(column) for column in profile.profile_evidence_columns() if column != "evidence_key"
    )
    aggregate_sql = profile.profile_select_sql(evidence_ref="qualified_evidence", npi=npi)
    publication_sql = (
        "NULL::timestamptz AS published_at"
        if published_at is None
        else "CAST(:profile_published_at AS timestamptz) AS published_at"
    )
    output = ", ".join(
        publication_sql
        if column == "published_at"
        else f"CASE WHEN response_bound.response_bytes <= :profile_max_bytes THEN aggregated.{profile.quote_identifier(column)} END AS {profile.quote_identifier(column)}"
        if column in {"profile_json", "evidence_json"}
        else "aggregated." + profile.quote_identifier(column)
        for column in profile.profile_columns()
    )
    return dict(
        typed_ctes=typed_ctes,
        provider_id=provider_id,
        independent_types=independent_types,
        fact_columns=fact_columns,
        evidence_columns=evidence_columns,
        aggregate_sql=aggregate_sql,
        output=output,
        evidence_sql=evidence_sql,
    )


def _static_resource_ctes():
    """Decode verified payloads without depending on mutable table composite types."""
    from process.provider_directory_profile_records import profile_resource_record_columns

    return ",\n".join(
        f"scoped_{name} AS MATERIALIZED (SELECT projected_resource.* FROM scoped_resources "
        f"CROSS JOIN LATERAL jsonb_to_record(payload_json) AS projected_resource({profile_resource_record_columns(name)}) "
        f"WHERE resource_type='{resource_type}')"
        for resource_type, name in _RESOURCE_TYPES.items()
    )


def _profile_parameters(npi, source_metadata, projection_document_dict, generation_id, profile_as_of, published_at):
    parameters_by_name = {
        "profile_projection": projection_document_dict,
        "profile_npi": npi,
        "profile_source_id": source_metadata["source_id"],
        "profile_endpoint_id": source_metadata["endpoint_id"],
        "profile_dataset_id": source_metadata["dataset_id"],
        "profile_api_base": source_metadata["canonical_api_base"],
        "profile_org_name": source_metadata["org_name"],
        "source_ids": [source_metadata["source_id"]],
        "dataset_ids": [source_metadata["dataset_id"]],
        "profile_as_of": profile_as_of.isoformat(),
        "profile_published_at": published_at,
        "generation_id": generation_id,
        "profile_max_bytes": MAX_BYTES,
    }
    return parameters_by_name


def build_scoped_profile_sql(
    *,
    npi: int,
    source: dict,
    row_type_refs: dict[str, tuple[str, str]],
    projection: dict,
    generation_id: str,
    profile_as_of: date,
    published_at: datetime | None = None,
) -> tuple[str, dict]:
    """Return one SELECT and bindings; caller verifies source, policy and associations.

    Raw projection is already office/plan-filtered by the trusted loader. Every
    bundle is rendered independently before evidence deduplication and caps.
    A present row with NULL profile/evidence means byte overflow; the caller
    must refuse it rather than interpret it as an empty profile.
    """
    type_refs_by_resource = _profile_row_types(source, row_type_refs, generation_id, profile_as_of, published_at)
    projection_document_dict = normalize_scoped_profile_projection(projection, source_id=source["source_id"])
    statement_parts_by_name = _profile_statement_parts(npi, type_refs_by_resource, published_at)
    parameters_by_name = _profile_parameters(
        npi, source, projection_document_dict, generation_id, profile_as_of, published_at
    )
    return _SCOPED_PROFILE_SQL.format(**statement_parts_by_name), parameters_by_name
