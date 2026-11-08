# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact CMS entity visibility from one retained canonical source recipe."""

import re
from dataclasses import dataclass

import asyncpg
from sqlalchemy import text

from api.provider_directory_cms_generation import accepted_cms_generation
from api.provider_directory_entities_contract import DirectoryReadError
from db.registry_schema import registry_schema
from process.network_approved_membership_source import pin_retained_approved_membership_source
from process.network_approved_source_bindings import APPROVED_NETWORK_BINDINGS_SQL
from process.network_cms_provider_identity import cms_provider_sql_fields
from process.network_cms_registry_source_pair import resolve_committed_registry_cms_source_pair
from process.network_fhir_membership_source import (
    PinnedFHIRMembershipSource,
    _require_pinned_source,
    _reviewed_descriptor,
    _source_witness_fields,
)
from process.network_serving_read import resolve_network_serving_manifest
from process.registry_network_catalog_read import RegistryNetworkCatalogError, _catalog_manifest
from process.registry_source_recipe_store import resolve_retained_registry_source_recipes

SUPPORTED_KINDS = frozenset({"organizations", "networks", "plans", "sites", "practitioner-roles", "payers"})


@dataclass(frozen=True)
class CMSNetworkScope:
    manifest: object
    bindings_sql: str
    parameters_by_name: dict
    source_schema: str
    source_pin: PinnedFHIRMembershipSource
    source_generation: dict
    excluded_network_ids: tuple[int, ...] | None = None
    read_relations: tuple = ()


def _mapped_networks(scope, *, include_all=False):
    selection = "approved.network_id=ANY(CAST(:scope_network_ids AS integer[]))"
    if scope.excluded_network_ids is not None:
        selection = (
            "NOT approved.network_id=ANY(CAST(:scope_excluded_network_ids AS integer[])) "
            "AND (CAST(:scope_network_ids AS integer[]) IS NULL OR " + selection + ")"
        )
    if include_all:
        selection = "TRUE"
    return f"""SELECT approved.network_id,approved.record_json->>'source_key' AS resource_id
      FROM approved_network_bindings approved
      JOIN {scope.source_schema}.provider_directory_insurance_network_source_binding legacy
        ON legacy.source_id='cms-npd' AND legacy.resource_type='Organization'
          AND legacy.resource_id=approved.record_json->>'source_key'
      WHERE approved.approved_network_key IS NOT NULL
        AND {selection}
        AND approved.record_json->'source_scope_json'=jsonb_build_object(
          'organization_id',legacy.resource_id,'legacy_uuid',legacy.network_id::text,
          'alias_scope',CAST(:scope_alias AS text))"""


def _selected_plans(scope, *, include_all=False):
    return f"""SELECT plan.resource_id FROM ({_mapped_networks(scope, include_all=include_all)}) allowed
      JOIN {scope.source_schema}.provider_directory_insurance_network_plan_evidence proof
        ON proof.source_id='cms-npd' AND proof.release_id=:release_id
          AND proof.network_resource_id=allowed.resource_id
      JOIN {scope.source_schema}.provider_directory_dataset_resource plan
        ON plan.dataset_id=:dataset_id AND plan.resource_type='InsurancePlan'
          AND plan.resource_id=proof.insurance_plan_resource_id
          AND (plan.acquired_resource_sha256 IS NULL OR plan.acquired_resource_sha256=proof.plan_payload_sha256)
      WHERE plan.payload_json::jsonb->'network_refs'
        @> jsonb_build_array('Organization/' || allowed.resource_id)"""


def _plan_organizations(scope):
    return f"""SELECT split_part(link.target_reference,'/',2) AS resource_id
      FROM {scope.source_schema}.provider_directory_cms_npd_relationship link
      JOIN {scope.source_schema}.provider_directory_dataset_resource plan
        ON plan.dataset_id=link.dataset_id AND plan.resource_type=link.resource_type
          AND plan.resource_id=link.resource_id AND plan.payload_hash=link.source_payload_hash
          AND (plan.acquired_resource_sha256 IS NULL OR plan.acquired_resource_sha256=link.raw_payload_sha256)
      WHERE link.dataset_id=:dataset_id AND link.source_id='cms-npd' AND link.release_id=:release_id
        AND link.resource_type='InsurancePlan' AND link.reference_field IN ('ownedBy','administeredBy')
        AND link.resolution_status='resolved' AND link.target_reference ~ '^Organization/[A-Za-z0-9.-]{{1,64}}$'
        AND plan.resource_id IN ({_selected_plans(scope)})
        AND link.target_reference=CASE link.reference_field WHEN 'ownedBy' THEN plan.payload_json::jsonb->>'owned_by_ref'
          ELSE plan.payload_json::jsonb->>'administered_by_ref' END"""


def _membership_pairs(scope):
    """Reconstruct the retained source evidence, then require the same effective office pair."""
    fields_by_name = cms_provider_sql_fields(5)
    fields_by_name.update(_source_witness_fields(scope.source_pin, False))
    sql = _MEMBERSHIP_PAIRS.format(
        **fields_by_name,
        schema=scope.source_schema,
        canonical='"' + scope.manifest.schema_name + '"',
        mapped=_mapped_networks(scope),
    )
    return re.sub(r"\$(\d+)", lambda match: {"1": ":dataset_id", "5": "'cms-npd'", "6": ":release_id"}[match[1]], sql)


_MEMBERSHIP_PAIRS = """WITH page AS (
 SELECT *,payload_json::jsonb AS payload FROM {schema}.provider_directory_dataset_resource
 WHERE dataset_id=:dataset_id AND resource_type IN ('PractitionerRole','OrganizationAffiliation')
), providers AS (
 SELECT page.*,provider.resource_type AS provider_resource_type,provider.resource_id AS provider_resource_id,
   provider_witness.raw_payload_sha256 AS provider_payload_sha256,
   {provider_system} AS provider_system,{provider_id} AS provider_id
 FROM page JOIN {schema}.provider_directory_dataset_resource provider ON provider.dataset_id=page.dataset_id
   AND provider.resource_type=CASE page.resource_type WHEN 'PractitionerRole' THEN 'Practitioner' ELSE 'Organization' END
   AND provider.resource_id=CASE WHEN page.resource_type='PractitionerRole'
     AND page.payload->>'practitioner_ref' ~ '^Practitioner/[A-Za-z0-9.-]{{1,64}}$'
     THEN split_part(page.payload->>'practitioner_ref','/',2)
     WHEN page.resource_type='OrganizationAffiliation'
     AND page.payload->>'participating_organization_ref' ~ '^Organization/[A-Za-z0-9.-]{{1,64}}$'
     THEN split_part(page.payload->>'participating_organization_ref','/',2) END
 {page_witness_join} {provider_witness_join}
 WHERE page_witness.raw_payload_sha256 IS NOT NULL AND provider_witness.raw_payload_sha256 IS NOT NULL
   AND page_witness.raw_payload_json::jsonb->>'resourceType'=page.resource_type
   AND page_witness.raw_payload_json::jsonb->>'id'=page.resource_id
   AND provider_witness.raw_payload_json::jsonb->>'resourceType'=provider.resource_type
   AND provider_witness.raw_payload_json::jsonb->>'id'=provider.resource_id
   AND provider.resource_id=provider.payload_json::jsonb->>'resource_id'
   AND page.resource_id=page.payload->>'resource_id'
   AND (page.payload->>'source_id' IS NULL OR page.payload->>'source_id'='cms-npd')
   AND NOT (page.payload->>'npi' IS NOT NULL AND provider.payload_json::jsonb->>'npi' IS NOT NULL
     AND page.payload->>'npi'<>provider.payload_json::jsonb->>'npi')
), pairs AS (
 SELECT providers.*,location.resource_id AS location_resource_id,binding.site_id,allowed.network_id
 FROM providers CROSS JOIN LATERAL jsonb_array_elements_text(
   CASE WHEN jsonb_typeof(payload->'location_refs')='array' THEN payload->'location_refs' ELSE '[]'::jsonb END) refs(reference)
 JOIN {schema}.provider_directory_dataset_resource location ON location.dataset_id=:dataset_id
   AND location.resource_type='Location' AND reference ~ '^Location/[A-Za-z0-9.-]{{1,64}}$'
   AND location.resource_id=split_part(reference,'/',2)
 {location_witness_join}
 JOIN {schema}.provider_directory_entity_release_evidence proof ON proof.source_id='cms-npd'
   AND proof.release_id=:release_id AND proof.resource_type='Location' AND proof.resource_id=location.resource_id
   AND proof.payload_sha256=location_witness.raw_payload_sha256
 JOIN {schema}.provider_directory_entity_source_binding binding ON binding.source_id='cms-npd'
   AND binding.resource_type='Location' AND binding.resource_id=proof.resource_id
 CROSS JOIN ({mapped}) allowed
 WHERE provider_id IS NOT NULL AND location.resource_id=location.payload_json::jsonb->>'resource_id'
   AND location_witness.raw_payload_json::jsonb->>'resourceType'='Location'
   AND location_witness.raw_payload_json::jsonb->>'id'=location.resource_id
), evidenced AS (
 SELECT *,encode(sha256(convert_to(({provider_evidence})::text,'UTF8')),'hex') AS evidence_id FROM pairs
)
SELECT evidenced.* FROM evidenced
JOIN {canonical}.network_membership_source retained USING(network_id,provider_system,provider_id,evidence_id)
JOIN {canonical}.network_membership effective USING(network_id,provider_system,provider_id)
JOIN {canonical}.provider_location_binding office USING(provider_system,provider_id)
WHERE retained.location_id=evidenced.site_id AND effective.location_id=evidenced.site_id
 AND office.location_id=evidenced.site_id"""


def _selected_pairs(scope, kind):
    column = {
        "sites": "location_resource_id",
        "practitioner-roles": "resource_id",
        "providers": "provider_resource_id",
    }[kind]
    selected = f"SELECT pairs.{column} FROM ({_membership_pairs(scope)}) pairs"
    if kind in {"practitioner-roles", "providers"}:
        selected += " WHERE pairs.resource_type='PractitionerRole'"
    return selected


def cms_network_predicate(generation, kind, resource_id):
    """Apply source-qualified policy before page selection and target hydration."""
    scope = generation.get("network_scope")
    if scope is None:
        return ""
    mapped = f"SELECT allowed.resource_id FROM ({_mapped_networks(scope)}) allowed"
    if scope.excluded_network_ids is not None and scope.parameters_by_name.get("scope_network_ids") is None:
        if kind == "networks":
            reviewed = f"SELECT reviewed.resource_id FROM ({_mapped_networks(scope, include_all=True)}) reviewed"
            permitted = mapped
        elif kind == "plans":
            reviewed = _selected_plans(scope, include_all=True)
            permitted = _selected_plans(scope)
        else:
            return ""
        return f" AND {resource_id} NOT IN (WITH {scope.bindings_sql} {reviewed} EXCEPT {permitted})"
    selected_by_kind = {
        "networks": mapped,
        "organizations": mapped + " UNION " + _plan_organizations(scope),
        "payers": _plan_organizations(scope),
        "plans": _selected_plans(scope),
    }
    selected = (
        _selected_pairs(scope, kind)
        if kind in {"sites", "practitioner-roles", "providers"}
        else selected_by_kind.get(kind)
    )
    if selected is None:
        return " AND false"
    return f" AND {resource_id} IN (WITH {scope.bindings_sql} {selected})"


def cms_network_target_predicate(generation, kind, reference, allowed_kinds=None):
    """Filter both edge endpoints using source-qualified local identities."""
    scope = generation.get("network_scope")
    if scope is None:
        return ""
    if scope.excluded_network_ids is not None and scope.parameters_by_name.get("scope_network_ids") is None:
        clauses = []
        for target_kind, resource_type in (("networks", "Organization"), ("plans", "InsurancePlan")):
            if allowed_kinds is not None and target_kind not in allowed_kinds:
                continue
            allowed = cms_network_predicate(generation, target_kind, f"split_part({reference},'/',2)")
            clauses.append(
                f" AND CASE WHEN {kind}='{target_kind}' AND {reference} ~ '^{resource_type}/[A-Za-z0-9.-]{{1,64}}$'"
                f" THEN TRUE {allowed} ELSE TRUE END"
            )
        return "".join(clauses)
    clauses = []
    for target_kind, resource_type in (
        ("organizations", "Organization"),
        ("networks", "Organization"),
        ("plans", "InsurancePlan"),
        ("sites", "Location"),
        ("practitioner-roles", "PractitionerRole"),
        ("providers", "Practitioner"),
    ):
        if allowed_kinds is not None and target_kind not in allowed_kinds:
            continue
        allowed = cms_network_predicate(generation, target_kind, f"split_part({reference},'/',2)")
        clauses.append(
            f"({kind}='{target_kind}' AND {reference} ~ '^{resource_type}/[A-Za-z0-9.-]{{1,64}}$' {allowed})"
        )
    return " AND (" + " OR ".join(clauses) + ")"


async def _require_membership_tables(session, scope, query):
    if query.kind not in {"sites", "practitioner-roles"} and not (
        query.kind == "plans" and query.shape == "relationships"
    ):
        return
    for namespace, table in (
        (scope.source_schema, "provider_directory_cms_npd_resource_witness"),
        ('"' + scope.manifest.schema_name + '"', "network_membership_source"),
        ('"' + scope.manifest.schema_name + '"', "network_membership"),
        ('"' + scope.manifest.schema_name + '"', "provider_location_binding"),
    ):
        if await session.scalar(text("SELECT to_regclass(:relation)"), {"relation": namespace + "." + table}) is None:
            raise DirectoryReadError(503)


async def pin_cms_network_scope(session, schema, query, generation):
    """Keep independent source/canonical generations in the caller's RR transaction."""
    if query.kind not in SUPPORTED_KINDS or query.source_id != "cms-npd":
        raise DirectoryReadError(503)
    connection = await session.connection()
    driver = (await connection.get_raw_connection()).driver_connection
    try:
        manifest = await _cms_scope_manifest(driver, query)
        recipes = await resolve_retained_registry_source_recipes(driver, manifest)
        matching_recipes = [
            recipe
            for recipe in recipes
            if type(recipe.source_pin) is PinnedFHIRMembershipSource and recipe.source_pin.source_id == "cms-npd"
        ]
        if len(matching_recipes) != 1:
            raise ValueError
        recipe = matching_recipes[0]
        source_pin = recipe.source_pin
        generation = await _scope_source_generation(session, schema, source_pin, recipe)
        approved = await pin_retained_approved_membership_source(
            driver, approved_revision=manifest.approved_custom_revision
        )
        await _require_pinned_source(
            driver, source_pin, _reviewed_descriptor(source_pin, approved, recipe.binding_coordinates, True)
        )
        parameters_by_name = dict(
            zip(
                (f"scope_{index}" for index in range(1, 8)),
                (manifest.approved_custom_revision, *recipe.binding_coordinates.sql_parameters),
            )
        )
        parameters_by_name.update(
            scope_network_ids=None if query.network_ids is None else list(query.network_ids),
            scope_alias=source_pin.alias_scope,
        )
        if query.excluded_network_ids is not None:
            parameters_by_name["scope_excluded_network_ids"] = list(query.excluded_network_ids)
        bindings = APPROVED_NETWORK_BINDINGS_SQL.format(namespace='"' + registry_schema() + '"')
        bindings = re.sub(r"\$(\d+)", lambda match: ":scope_" + match[1], bindings)
        scope = CMSNetworkScope(
            manifest,
            bindings,
            parameters_by_name,
            '"' + source_pin.read_schema_name + '"',
            source_pin,
            generation,
            query.excluded_network_ids,
        )
        if query.excluded_network_ids is not None and query.network_ids is None:
            return scope
        await _validate_selected_cms_scope(session, scope, query)
        return scope
    except RegistryNetworkCatalogError:
        raise
    except ValueError, asyncpg.PostgresError:
        raise DirectoryReadError(503) from None


async def _cms_scope_manifest(driver, query):
    if query.excluded_network_ids is not None:
        if query.network_manifest is None:
            raise ValueError
        return await _catalog_manifest(driver, query.network_generation, registry_schema(), query.network_manifest)
    return await resolve_network_serving_manifest(driver, generation_id=query.network_generation)


async def _validate_selected_cms_scope(session, scope, query):
    await _require_membership_tables(session, scope, query)
    selected = (
        await session.execute(
            text(
                f"WITH {scope.bindings_sql} SELECT array_agg(DISTINCT network_id ORDER BY network_id) "
                f"FROM ({_mapped_networks(scope)}) selected"
            ),
            scope.parameters_by_name,
        )
    ).scalar_one()
    if selected != list(query.network_ids):
        raise ValueError


async def _scope_source_generation(session, schema, source_pin, recipe):
    """Choose exact committed historical publication or the unchanged current selector."""
    if source_pin.retained_epoch is not None:
        retained = await resolve_committed_registry_cms_source_pair(session, recipe)
        generation_by_field = {
            "endpoint_id": source_pin.endpoint_id,
            "dataset_id": source_pin.dataset_id,
            "dataset_hash": source_pin.dataset_sha256,
            "release_id": source_pin.release_id,
            "observed_at": retained.source_published_at,
            "retained_pair": retained.pair,
        }
    else:
        generation_by_field = await accepted_cms_generation(session, None)
    if source_pin.retained_epoch is None and (
        source_pin.read_schema_name != schema.strip('"')
        or source_pin.dataset_id != generation_by_field["dataset_id"]
        or source_pin.dataset_sha256 != generation_by_field["dataset_hash"]
        or source_pin.release_id != generation_by_field["release_id"]
        or source_pin.endpoint_id != generation_by_field["endpoint_id"]
    ):
        raise ValueError
    return generation_by_field
