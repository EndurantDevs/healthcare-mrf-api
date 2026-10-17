# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded retained CMS resources for the separate per-office profile projector."""

import json
import re

from sqlalchemy import text

from api.provider_directory_cms_network_scope import CMSNetworkScope, _mapped_networks
from api.provider_directory_entities_contract import DirectoryReadError
from api.provider_profile_network_facts import _query as _plan_query
from api.provider_profile_scoped_resources import project_scoped_profile_resources
from process.network_address_projection import _identifier
from process.network_fhir_membership_source import _require_pinned_source

MAX_ROWS = 5000
MAX_BYTES = 8 * 1024 * 1024
_MINIMAL_ROLE = (
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
)


def _query(scope):
    plan_sql = _plan_query(scope)
    marker = ", documents AS ("
    if plan_sql.count(marker) != 1:
        raise ValueError("profile_resource_query_invalid")
    prefix = plan_sql.split(marker, 1)[0].replace("WITH ", "WITH RECURSIVE ", 1)
    return prefix + _resource_seeds_sql(scope) + _resource_closure_sql()


def _resource_seeds_sql(scope):
    namespace = scope.source_schema
    return f""", reviewed_networks AS MATERIALIZED ({_mapped_networks(scope, include_all=True)}),
      excluded_network_resources AS MATERIALIZED (
        SELECT resource_id FROM reviewed_networks EXCEPT SELECT resource_id FROM allowed LIMIT 5001),
      associations AS MATERIALIZED (
      SELECT DISTINCT role_type,role_id,location_resource_id,allowed.resource_id AS network_resource_id,plan_id
      FROM eligible JOIN allowed USING(network_id)
      UNION
      SELECT DISTINCT pairs.resource_type,pairs.resource_id,pairs.location_resource_id,allowed.resource_id,NULL::text
      FROM pairs JOIN allowed USING(network_id)
      WHERE pairs.provider_system='npi' AND pairs.provider_id=:profile_npi
        AND (CAST(:profile_location AS uuid) IS NULL OR pairs.site_id=CAST(:profile_location AS uuid))
        AND pairs.payload->'network_refs' @> jsonb_build_array('Organization/'||allowed.resource_id)
      LIMIT 5001), source_rows AS NOT MATERIALIZED (
      SELECT r.resource_type,r.resource_id,r.payload_json::jsonb AS payload,
        w.raw_payload_json::jsonb->>'resourceType'=r.resource_type
        AND w.raw_payload_json::jsonb->>'id'=r.resource_id
        AND r.payload_json::jsonb->>'resource_id'=r.resource_id AS valid
      FROM {namespace}.provider_directory_dataset_resource r
      LEFT JOIN {namespace}.provider_directory_cms_npd_resource_witness w
        ON w.dataset_id=r.dataset_id AND w.resource_type=r.resource_type AND w.resource_id=r.resource_id
        AND w.source_id='cms-npd' AND w.release_id=:release_id
        AND w.normalized_payload_hash=r.payload_hash
        AND (r.acquired_resource_sha256 IS NULL OR r.acquired_resource_sha256=w.raw_payload_sha256)
      WHERE r.dataset_id=:dataset_id), independent AS MATERIALIZED (
      SELECT resource_type,resource_id FROM source_rows
      WHERE resource_type='Practitioner' AND payload->>'npi'=:profile_npi
      UNION
      SELECT role.resource_type,role.resource_id FROM source_rows role
      WHERE role.resource_type='PractitionerRole' AND role.payload->'accepting_medicaid' IS NOT NULL
        AND (role.payload->>'npi'=:profile_npi OR EXISTS (
          SELECT 1 FROM source_rows practitioner WHERE practitioner.resource_type='Practitioner'
            AND practitioner.payload->>'npi'=:profile_npi
            AND role.payload->>'practitioner_ref' IN (practitioner.resource_id,'Practitioner/'||practitioner.resource_id)))),
"""


def _resource_closure_sql():
    return """      walk(resource_type,resource_id) AS (
      SELECT resource_type,resource_id FROM independent
      UNION
      SELECT seeds.kind,seeds.id FROM associations a CROSS JOIN LATERAL (VALUES
        (a.role_type,a.role_id),('Location',a.location_resource_id),
        ('Organization',a.network_resource_id),('InsurancePlan',a.plan_id)) seeds(kind,id) WHERE seeds.id IS NOT NULL
      UNION
      SELECT refs.kind,CASE WHEN strpos(refs.reference,'/')=0 THEN refs.reference ELSE split_part(refs.reference,'/',2) END
      FROM walk JOIN source_rows r USING(resource_type,resource_id)
      CROSS JOIN LATERAL (
        SELECT kind,reference FROM (VALUES
          ('Practitioner',r.payload->>'practitioner_ref'),('Organization',r.payload->>'organization_ref'),
          ('Organization',r.payload->>'participating_organization_ref'),('Organization',r.payload->>'provided_by_ref'),
          ('Organization',r.payload->>'managing_organization_ref')) scalar_refs(kind,reference)
        UNION ALL
        SELECT 'HealthcareService',jsonb_array_elements_text(CASE WHEN jsonb_typeof(r.payload->'healthcare_service_refs')='array'
          THEN r.payload->'healthcare_service_refs' ELSE '[]'::jsonb END)
        UNION ALL
        SELECT 'Endpoint',jsonb_array_elements_text(CASE WHEN jsonb_typeof(r.payload->'endpoint_refs')='array'
          THEN r.payload->'endpoint_refs' ELSE '[]'::jsonb END)
      ) refs WHERE refs.reference IS NOT NULL AND r.resource_type!='Practitioner'
        AND (r.resource_type NOT IN ('PractitionerRole','OrganizationAffiliation') OR EXISTS (
          SELECT 1 FROM associations a WHERE a.role_type=r.resource_type AND a.role_id=r.resource_id))),
      bounded AS MATERIALIZED (SELECT * FROM walk LIMIT 5001), documents AS MATERIALIZED (
      SELECT r.valid,jsonb_build_object('resource_type',b.resource_type,'resource_id',b.resource_id,'payload_json',
        CASE WHEN b.resource_type='PractitionerRole' AND NOT EXISTS (
          SELECT 1 FROM associations a WHERE a.role_type=b.resource_type AND a.role_id=b.resource_id)
        THEN (SELECT jsonb_object_agg(key,value) FROM jsonb_each(r.payload) WHERE key=ANY(CAST(:minimal_role AS text[])))
        ELSE r.payload END) AS document
      FROM bounded b LEFT JOIN source_rows r USING(resource_type,resource_id)),
      association_documents AS (SELECT to_jsonb(a) AS document FROM associations a),
      accounting AS (SELECT
        (SELECT count(*) FROM bounded)<=:resource_max_rows
        AND (SELECT count(*) FROM eligible)<=:resource_max_rows
        AND (SELECT count(*) FROM associations)<=:resource_max_rows
        AND (SELECT count(*) FROM excluded_network_resources)<=:resource_max_rows
        AND NOT EXISTS (SELECT 1 FROM documents WHERE valid IS DISTINCT FROM TRUE)
        AND coalesce((SELECT sum(octet_length(document::text)+2) FROM documents),0)
          +coalesce((SELECT sum(octet_length(document::text)+2) FROM association_documents),0)
          +coalesce((SELECT sum(octet_length(to_jsonb(resource_id)::text)+2) FROM excluded_network_resources),0)
          +128<=:resource_max_bytes AS valid)
      SELECT CASE WHEN valid THEN jsonb_build_object(
        'resources',coalesce((SELECT jsonb_agg(document ORDER BY document::text) FROM documents),'[]'::jsonb),
        'associations',coalesce((SELECT jsonb_agg(document ORDER BY document::text) FROM association_documents),'[]'::jsonb),
        'excluded_network_resource_ids',coalesce((SELECT jsonb_agg(resource_id ORDER BY resource_id)
          FROM excluded_network_resources),'[]'::jsonb))::text
      END FROM accounting"""


def _decode(document, npi):
    if type(document) is not str or len(document.encode()) > MAX_BYTES:
        raise DirectoryReadError(503)
    try:
        result = json.loads(document)
        if type(result) is not dict or set(result) != {"resources", "associations", "excluded_network_resource_ids"}:
            raise ValueError("profile_resource_document_invalid")
        if any(type(result[key]) is not list or len(result[key]) > MAX_ROWS for key in result):
            raise ValueError("profile_resource_document_invalid")
        project_scoped_profile_resources(
            result["resources"],
            npi,
            result["associations"],
            excluded_network_resource_ids=result["excluded_network_resource_ids"],
        )
    except (ValueError, TypeError, RecursionError) as error:
        raise DirectoryReadError(503) from error
    return result


async def read_cms_profile_resources(session, scope: CMSNetworkScope, npi: str):
    """Read one already authenticated CMS scope in the caller's read-only RR TX.

    Returned resources are input to the per-association projector, never a public
    profile response. Source recipe and policy resolution belong to the caller.
    """
    if (
        type(scope) is not CMSNetworkScope
        or scope.source_pin.source_id != "cms-npd"
        or scope.source_pin.retained_epoch is None
        or type(npi) is not str
        or re.fullmatch(r"[12][0-9]{9}", npi) is None
        or scope.parameters_by_name.get("profile_npi") != npi
        or scope.parameters_by_name.get("dataset_id") != scope.source_pin.dataset_id
        or scope.parameters_by_name.get("release_id") != scope.source_pin.release_id
        or scope.source_schema != _identifier(scope.source_pin.read_schema_name)
        or scope.excluded_network_ids is None
        or not session.in_transaction()
    ):
        raise ValueError("profile_resource_scope_invalid")
    connection = await session.connection()
    driver = (await connection.get_raw_connection()).driver_connection
    if (
        await driver.fetchval("SHOW transaction_isolation") != "repeatable read"
        or await driver.fetchval("SHOW transaction_read_only") != "on"
    ):
        raise ValueError("profile_resource_transaction_invalid")
    await _require_pinned_source(driver, scope.source_pin)
    parameters_by_name = {
        **scope.parameters_by_name,
        "minimal_role": list(_MINIMAL_ROLE),
        "resource_max_rows": MAX_ROWS,
        "resource_max_bytes": MAX_BYTES,
    }
    document = (await session.execute(text(_query(scope)), parameters_by_name)).scalar_one()
    return _decode(document, npi)
