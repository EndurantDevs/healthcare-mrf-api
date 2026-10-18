# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded CMS plan facts before profile composition, counts or pagination."""

import json
import re
from dataclasses import dataclass
from uuid import UUID

from sqlalchemy import text

from api.provider_directory_cms_network_scope import CMSNetworkScope, _mapped_networks, _membership_pairs
from api.provider_directory_entities_contract import DirectoryReadError
from db.registry_schema import registry_schema
from process.network_address_projection import _identifier
from process.network_approved_source_bindings import APPROVED_NETWORK_BINDINGS_SQL
from process.network_cms_registry_source_pair import resolve_committed_registry_cms_source_pair
from process.network_fhir_membership_source import PinnedFHIRMembershipSource, _source_witness_fields
from process.network_serving_read import PinnedNetworkServingManifest
from process.registry_network_catalog_read import _exclusions
from process.registry_source_recipe_store import resolve_retained_registry_source_recipes

MAX_FACTS = 5000
MAX_BYTES = 1024 * 1024
_RESOURCE = re.compile(r"[A-Za-z0-9.-]{1,64}")
_FIELDS = {"network_id", "npi", "role_type", "role_id", "plan_id", "location_id", "location_resource_id", "evidence_id"}


@dataclass(frozen=True)
class CMSProfilePlanInputs:
    """Unaggregated source facts; independent profile inputs are not modified."""

    source_id: str
    endpoint_id: str
    dataset_id: str
    release_id: str
    manifest: PinnedNetworkServingManifest
    facts: tuple[dict, ...]


def _input(npi, excluded, selected, location_id):
    _exclusions(excluded)
    if type(npi) is not str or re.fullmatch(r"[12][0-9]{9}", npi) is None:
        raise ValueError("profile_network_input_invalid")
    if selected is not None and (not selected or len(selected) > 100):
        raise ValueError("profile_network_input_invalid")
    if selected is not None:
        _exclusions(selected)
    if location_id is not None and (
        type(location_id) is not str or str(UUID(location_id)) != location_id or not UUID(location_id).int
    ):
        raise ValueError("profile_network_input_invalid")


def _fact_rows(document, npi, excluded, selected, location_id):
    if type(document) is not str or len(document.encode()) > MAX_BYTES:
        raise DirectoryReadError(503)
    try:
        fact_rows = json.loads(document)
    except (ValueError, RecursionError) as error:
        raise DirectoryReadError(503) from error
    if type(fact_rows) is not list or len(fact_rows) > MAX_FACTS:
        raise DirectoryReadError(503)
    seen_fact_keys = set()
    for fact_by_field in fact_rows:
        if (
            type(fact_by_field) is not dict
            or set(fact_by_field) != _FIELDS
            or type(fact_by_field["network_id"]) is not int
        ):
            raise DirectoryReadError(503)
        if (
            not 0 < fact_by_field["network_id"] <= 2147483647
            or fact_by_field["network_id"] in excluded
            or selected is not None
            and fact_by_field["network_id"] not in selected
        ):
            raise DirectoryReadError(503)
        if fact_by_field["npi"] != npi or fact_by_field["role_type"] not in {
            "PractitionerRole",
            "OrganizationAffiliation",
        }:
            raise DirectoryReadError(503)
        if any(
            type(fact_by_field[fact_values]) is not str or _RESOURCE.fullmatch(fact_by_field[fact_values]) is None
            for fact_values in ("role_id", "plan_id", "location_resource_id")
        ):
            raise DirectoryReadError(503)
        try:
            is_valid_location = (
                type(fact_by_field["location_id"]) is str
                and str(UUID(fact_by_field["location_id"])) == fact_by_field["location_id"]
                and UUID(fact_by_field["location_id"]).int != 0
            )
        except ValueError:
            is_valid_location = False
        if not is_valid_location:
            raise DirectoryReadError(503)
        if location_id is not None and fact_by_field["location_id"] != location_id:
            raise DirectoryReadError(503)
        if (
            type(fact_by_field["evidence_id"]) is not str
            or re.fullmatch(r"[0-9a-f]{64}", fact_by_field["evidence_id"]) is None
        ):
            raise DirectoryReadError(503)
        fact_values = tuple(fact_by_field[field] for field in sorted(_FIELDS))
        if fact_values in seen_fact_keys:
            raise DirectoryReadError(503)
        seen_fact_keys.add(fact_values)
    return tuple(fact_rows)


def _query(scope):
    witness = _source_witness_fields(scope.source_pin, False)["plan_witness_join"]
    witness = re.sub(r"\$(\d+)", lambda match: {"5": "'cms-npd'", "6": ":release_id"}[match[1]], witness)
    return f"""WITH {scope.bindings_sql}, allowed AS MATERIALIZED ({_mapped_networks(scope)}),
      pairs AS MATERIALIZED (SELECT * FROM ({_membership_pairs(scope)}) profile_pairs
        WHERE profile_pairs.provider_system='npi' AND profile_pairs.provider_id=:profile_npi), eligible AS MATERIALIZED (
      SELECT DISTINCT pairs.network_id,pairs.provider_id AS npi,pairs.resource_type AS role_type,
        pairs.resource_id AS role_id,plan.resource_id AS plan_id,pairs.site_id::text AS location_id,
        pairs.location_resource_id,pairs.evidence_id
      FROM pairs JOIN allowed ON allowed.network_id=pairs.network_id
      CROSS JOIN LATERAL jsonb_array_elements_text(CASE WHEN jsonb_typeof(pairs.payload->'insurance_plan_refs')='array'
        THEN pairs.payload->'insurance_plan_refs' ELSE '[]'::jsonb END) refs(reference)
      JOIN {scope.source_schema}.provider_directory_dataset_resource plan ON plan.dataset_id=:dataset_id
        AND plan.resource_type='InsurancePlan' AND reference ~ '^InsurancePlan/[A-Za-z0-9.-]{{1,64}}$'
        AND plan.resource_id=split_part(reference,'/',2)
      {witness}
      JOIN {scope.source_schema}.provider_directory_insurance_network_plan_evidence proof
        ON proof.source_id='cms-npd' AND proof.release_id=:release_id
        AND proof.network_resource_id=allowed.resource_id AND proof.insurance_plan_resource_id=plan.resource_id
        AND proof.plan_payload_sha256=plan_witness.raw_payload_sha256
      WHERE pairs.provider_system='npi' AND pairs.provider_id=:profile_npi
        AND (CAST(:profile_location AS uuid) IS NULL OR pairs.site_id=CAST(:profile_location AS uuid))
        AND plan_witness.raw_payload_json::jsonb->>'resourceType'='InsurancePlan'
        AND plan_witness.raw_payload_json::jsonb->>'id'=plan.resource_id
        AND plan.resource_id=plan.payload_json::jsonb->>'resource_id'
        AND plan.payload_json::jsonb->'network_refs' @> jsonb_build_array('Organization/'||allowed.resource_id)
      LIMIT 5001), documents AS (SELECT to_jsonb(eligible) AS document FROM eligible)
      SELECT CASE WHEN count(*)<=:profile_max_facts AND coalesce(sum(octet_length(document::text)+2),0)+2<=:profile_max_bytes
        THEN coalesce(jsonb_agg(document ORDER BY document::text),'[]'::jsonb)::text END FROM documents"""


async def pin_cms_profile_scope(
    session, manifest, npi, *, excluded_network_ids, selected_network_ids=None, location_id=None
):
    """Pin the retained CMS source and exact policy in the caller's RR snapshot."""
    _input(npi, excluded_network_ids, selected_network_ids, location_id)
    if type(manifest) is not PinnedNetworkServingManifest or not session.in_transaction():
        raise ValueError("profile_network_scope_invalid")
    connection = await session.connection()
    driver = (await connection.get_raw_connection()).driver_connection
    recipes = await resolve_retained_registry_source_recipes(driver, manifest)
    matches = [
        recipe
        for recipe in recipes
        if type(recipe.source_pin) is PinnedFHIRMembershipSource and recipe.source_pin.source_id == "cms-npd"
    ]
    if len(matches) != 1 or matches[0].source_pin.retained_epoch is None:
        raise DirectoryReadError(503)
    recipe = matches[0]
    await resolve_committed_registry_cms_source_pair(session, recipe)
    pin = recipe.source_pin
    parameters_by_name = dict(
        zip(
            (f"scope_{index}" for index in range(1, 8)),
            (manifest.approved_custom_revision, *recipe.binding_coordinates.sql_parameters),
        )
    )
    parameters_by_name.update(
        scope_network_ids=list(selected_network_ids) if selected_network_ids is not None else None,
        scope_excluded_network_ids=list(excluded_network_ids),
        scope_alias=pin.alias_scope,
        dataset_id=pin.dataset_id,
        release_id=pin.release_id,
        profile_npi=npi,
        profile_location=location_id,
        profile_max_facts=MAX_FACTS,
        profile_max_bytes=MAX_BYTES,
    )
    scope = CMSNetworkScope(
        manifest,
        APPROVED_NETWORK_BINDINGS_SQL.format(namespace=_identifier(registry_schema())),
        parameters_by_name,
        _identifier(pin.read_schema_name),
        pin,
        {},
        excluded_network_ids,
    )
    scope_sql = re.sub(r"\$(\d+)", lambda match: ":scope_" + match[1], scope.bindings_sql)
    scope = CMSNetworkScope(manifest, scope_sql, parameters_by_name, scope.source_schema, pin, {}, excluded_network_ids)
    return scope


async def read_cms_profile_plan_inputs(
    session, manifest, npi, *, excluded_network_ids, selected_network_ids=None, location_id=None
):
    """Require a verified v2 caller; resolve exact retained evidence in its RR TX."""
    scope = await pin_cms_profile_scope(
        session,
        manifest,
        npi,
        excluded_network_ids=excluded_network_ids,
        selected_network_ids=selected_network_ids,
        location_id=location_id,
    )
    pin = scope.source_pin
    parameters_by_name = scope.parameters_by_name
    document = (await session.execute(text(_query(scope)), parameters_by_name)).scalar_one()
    return CMSProfilePlanInputs(
        pin.source_id,
        pin.endpoint_id,
        pin.dataset_id,
        pin.release_id,
        manifest,
        _fact_rows(document, npi, excluded_network_ids, selected_network_ids, location_id),
    )
