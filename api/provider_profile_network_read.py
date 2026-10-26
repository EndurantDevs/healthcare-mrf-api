# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Compose managed profiles from one policy and immutable source snapshot."""

from datetime import date

from sanic.exceptions import ServiceUnavailable
from sqlalchemy import text

from api.provider_directory_entities_contract import DirectoryReadError
from api.provider_profile_network_contract import bind_managed_profile_generation
from api.provider_profile_network_facts import pin_cms_profile_scope
from api.provider_profile_scoped_resources import project_scoped_profile_resources
from api.provider_profile_snapshot import provider_profile_read_snapshot, provider_read_savepoint
from process.network_fhir_membership_source import PinnedFHIRMembershipSource
from process.network_serving_read import resolve_network_serving_manifest
from process.provider_directory_profile import is_valid_npi
from process.registry_source_recipe_store import resolve_retained_registry_source_recipes

_ROW_TYPES = {
    "Practitioner": "provider_directory_practitioner",
    "PractitionerRole": "provider_directory_practitioner_role",
    "Organization": "provider_directory_organization",
    "OrganizationAffiliation": "provider_directory_organization_affiliation",
    "HealthcareService": "provider_directory_healthcare_service",
    "Endpoint": "provider_directory_endpoint",
}


class _RetainedProfileUnavailable(DirectoryReadError):
    """Retained generic memberships have no supported profile projection yet."""


async def _profile_fhir_recipes(driver, manifest):
    """Authenticate recipes before distinguishing unavailable optional enrichment."""
    recipes = await resolve_retained_registry_source_recipes(driver, manifest)
    fhir_recipes = [recipe for recipe in recipes if type(recipe.source_pin) is PinnedFHIRMembershipSource]
    if (
        len(fhir_recipes) == 1
        and fhir_recipes[0].source_pin.source_id != "cms-npd"
        and fhir_recipes[0].source_pin.retained_capture is not None
    ):
        # shortcut: generic retained profiles need an adapter before enrichment is available.
        raise _RetainedProfileUnavailable(503)
    return fhir_recipes


async def _directory_profile(session, driver, manifest, npi, query, excluded, schema, *, selected_network_ids=None):
    from api.endpoint import npi as provider
    from api.provider_profile_cms_resources import read_cms_profile_resources
    from api.provider_profile_scoped_sql import build_scoped_profile_sql
    from process.provider_directory_cms_npd import source as cms_source

    fhir_recipes = await _profile_fhir_recipes(driver, manifest)
    observed = await provider._fetch_provider_directory_profile_map([int(npi)], session=session)
    observed_sources = (observed.get(int(npi)) or {}).get("profile", {}).get("sources", [])
    retained_source_ids = {recipe.source_pin.source_id for recipe in fhir_recipes}
    if type(observed_sources) is not list or any(
        type(source_descriptor) is not dict or source_descriptor.get("source_id") not in retained_source_ids
        for source_descriptor in observed_sources
    ):
        raise DirectoryReadError(503)
    if not fhir_recipes:
        return None
    # shortcut: generic FHIR epochs need a custody adapter before managed profile activation.
    if len(fhir_recipes) != 1 or fhir_recipes[0].source_pin.source_id != "cms-npd":
        raise DirectoryReadError(503)
    if not is_valid_npi(npi):
        return None
    scope = await pin_cms_profile_scope(
        session, manifest, npi, excluded_network_ids=excluded, selected_network_ids=selected_network_ids
    )
    source_inputs = await read_cms_profile_resources(session, scope, npi)
    projection = project_scoped_profile_resources(
        source_inputs["resources"],
        npi,
        source_inputs["associations"],
        excluded_network_resource_ids=source_inputs["excluded_network_resource_ids"],
    )
    pin = scope.source_pin
    statement, parameters = build_scoped_profile_sql(
        npi=int(npi),
        source={
            "source_id": pin.source_id,
            "endpoint_id": pin.endpoint_id,
            "dataset_id": pin.dataset_id,
            "canonical_api_base": cms_source.DOWNLOADS_URL,
            "org_name": "CMS National Provider Directory",
            "plan_name": None,
        },
        row_type_refs={kind: (schema, table) for kind, table in _ROW_TYPES.items()},
        projection=projection,
        generation_id=pin.generation_id,
        profile_as_of=date.fromisoformat(pin.as_of),
    )
    await provider._disable_provider_directory_evidence_jit(session)
    profile_row = (await session.execute(text(statement), parameters)).mappings().one_or_none()
    if profile_row is None:
        return None
    if profile_row["profile_json"] is None or profile_row["evidence_json"] is None:
        raise DirectoryReadError(503)
    return provider._provider_directory_profile_payload(profile_row, include_evidence=query.include_evidence)


async def read_address_scoped_directory_profile(session, npi, *, include_evidence):
    """Reuse the detail request's pinned manifest before any profile aggregation."""
    from api.endpoint import npi as provider
    from api.network_address_scope import current_network_address_scope

    scope = current_network_address_scope()
    if scope is None:
        raise ValueError("network_address_scope_required")
    connection = await session.connection()
    driver = (await connection.get_raw_connection()).driver_connection
    query = provider._provider_profile_query_from_args({"include_evidence": "true" if include_evidence else "false"})
    return await _directory_profile(
        session,
        driver,
        scope.manifest,
        str(npi),
        query,
        getattr(scope, "excluded_network_ids", None) or (),
        provider._runtime_db_schema(),
        selected_network_ids=scope.network_ids,
    )


async def read_detail_directory_profile(session, npi, *, include_evidence):
    """Keep legacy optional reads and make managed source-proof failures terminal."""
    from api.endpoint import npi as provider
    from api.network_address_scope import current_network_address_scope

    if current_network_address_scope() is not None:
        try:
            return await read_address_scoped_directory_profile(session, npi, include_evidence=include_evidence)
        except _RetainedProfileUnavailable:
            return None
        except (DirectoryReadError, ValueError) as error:
            raise ServiceUnavailable("Provider data is temporarily unavailable.") from error
    try:
        async with provider_read_savepoint(session):
            return (
                await provider._fetch_provider_directory_profile_map(
                    [npi], include_evidence=include_evidence, session=session
                )
            ).get(npi)
    except Exception as error:
        provider.logger.debug("Provider Directory profile fetch failed for npi=%s: %s", npi, error)
        return None


async def read_managed_provider_profile(selector, *, excluded_network_ids, access_scope_sha256):
    """Keep independent facts and scoped directory facts under one ordinary Reader."""
    from api.endpoint import npi as provider

    npi, query = selector
    schema = provider._runtime_db_schema()
    async with provider_profile_read_snapshot(provider.db, schema, include_detail=True) as session:
        connection = await session.connection()
        driver = (await connection.get_raw_connection()).driver_connection
        manifest = await resolve_network_serving_manifest(driver)
        state_projection = await provider.fetch_provider_profile_projection(int(npi))
        fhir_record = await _directory_profile(session, driver, manifest, npi, query, excluded_network_ids, schema)
        profile = provider._compose_requested_provider_profile(int(npi), state_projection, fhir_record, query)
        if profile is None:
            return provider._provider_profile_error_response(
                "provider_profile_not_found",
                "No reviewed provider profile facts are available for this NPI.",
                status=404,
                npi=int(npi),
            )
        profile = bind_managed_profile_generation(profile, manifest, access_scope_sha256)
        error = provider._provider_profile_generation_error(profile, query.requested_generation_id)
        if error is not None:
            return error
        return {
            "generation": str(manifest.generation_id),
            "approved_custom_revision": str(manifest.approved_custom_revision),
        } | provider._provider_profile_response_by_key(int(npi), state_projection, fhir_record, profile, query)
