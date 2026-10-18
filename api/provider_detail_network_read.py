# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Compose managed native detail under one approved, policy-bound read snapshot."""

import json
from contextlib import asynccontextmanager

import asyncpg
from sanic.exceptions import NotFound, SanicException
from sqlalchemy.exc import DBAPIError

from api.network_address_scope import current_network_address_scope, network_address_read_scope
from api.provider_detail_network_contract import ProviderDetailNetworkQuery, parse_managed_detail_query
from api.provider_profile_snapshot import provider_profile_read_snapshot
from db.registry_schema import registry_schema
from process.network_address_projection import _identifier
from process.registry_catalog_policy import RegistryOrganizationVisibility
from process.registry_network_catalog_read import (
    RegistryNetworkCatalogError,
    RegistryNetworkCatalogLegacyBatchSelector,
    _closed_json,
    _exclusions,
    _pin,
    resolve_registry_network_catalog_legacy,
)
from process.registry_provider_plan_read import RegistryProviderPlansQuery, read_registry_provider_plans

MAX_DETAIL_BYTES = 32 * 1024 * 1024
_SELECTED_SQL = """SELECT count(*) FROM {control}.registry_approved_record
WHERE approved_revision=$1 AND record_kind='network' AND record_key=ANY($2::text[])
 AND record_json->'archived'='false'::jsonb
 AND jsonb_typeof(record_json->'network_id')='number' AND record_json->>'network_id'=record_key
 AND NOT record_key=ANY($3::text[])"""


class ManagedProviderDetailAbsent(NotFound):
    """Keep real native absence bound to its already-authorized serving snapshot."""

    def __init__(self, manifest, npi):
        scope = current_network_address_scope()
        if scope is None or scope.manifest is not manifest:
            raise RegistryNetworkCatalogError("registry_network_catalog_request_invalid")
        self.proof = {
            "generation": str(manifest.generation_id),
            "approved_custom_revision": str(manifest.approved_custom_revision),
            "manifest_sha256": manifest.manifest_sha256,
            "npi": npi,
            "access_scope_sha256": scope.access_scope_sha256,
        }
        super().__init__("Provider detail is absent.")


def _native_args(query):
    if type(query) is not ProviderDetailNetworkQuery:
        raise RegistryNetworkCatalogError("registry_network_catalog_request_invalid")
    args_by_name = dict(query.native_args)
    query_by_field = {"npi": query.npi, "include": list(query.include), **args_by_name}
    if query.network_generation is not None:
        query_by_field["network_generation"] = str(query.network_generation)
    if query.selector is not None:
        if type(query.selector) is tuple:
            namespace, values = "canonical", list(query.selector)
        elif type(query.selector) is RegistryNetworkCatalogLegacyBatchSelector:
            namespace, values = query.selector.namespace, list(query.selector.values)
        else:
            raise RegistryNetworkCatalogError("registry_network_catalog_request_invalid")
        query_by_field["selector"] = {"namespace": namespace, "values": values}
    if parse_managed_detail_query(query_by_field) != query:
        raise RegistryNetworkCatalogError("registry_network_catalog_request_invalid")
    return args_by_name | {"sync_geocode": "false", "lookup_stored_geocode": "false"}


async def _selected_networks(driver, selector, manifest, exclusions, control_schema, organization_policy):
    if selector is None:
        return None
    if type(selector) is tuple:
        count = await driver.fetchval(
            _SELECTED_SQL.format(control=_identifier(control_schema)),
            manifest.approved_custom_revision,
            [str(network_id) for network_id in selector],
            [str(network_id) for network_id in exclusions],
        )
        if type(count) is not int or count != len(selector):
            raise RegistryNetworkCatalogError("registry_network_catalog_detail_denied")
        return selector
    page = await resolve_registry_network_catalog_legacy(
        driver,
        selector,
        excluded_network_ids=exclusions,
        organization_policy=organization_policy,
        generation_id=manifest.generation_id,
        control_schema=control_schema,
        manifest=manifest,
    )
    if page["generation"] != str(manifest.generation_id) or page["approved_custom_revision"] != str(
        manifest.approved_custom_revision
    ):
        raise RegistryNetworkCatalogError("registry_network_catalog_source_changed")
    return tuple(sorted(network["network_id"] for network in page["items"]))


def _detail_document(http_response, npi):
    if http_response.status != 200:
        if type(http_response.status) is int and 400 <= http_response.status <= 599:
            raise SanicException("Provider data is unavailable.", status_code=http_response.status)
        raise RegistryNetworkCatalogError("registry_network_catalog_document_invalid")
    document = _closed_json(http_response.body, MAX_DETAIL_BYTES)
    if type(document) is not dict or type(document.get("npi")) is not int or document["npi"] != int(npi):
        raise RegistryNetworkCatalogError("registry_network_catalog_document_invalid")
    return document


async def _detail_data(request, query, session, manifest, native_args):
    from api.endpoint import npi as provider
    from api.provider_detail_network_metadata import project_managed_detail_metadata

    try:
        native_response = await provider.read_npi_detail(request, query.npi, native_args=native_args)
        detail_by_field = _detail_document(native_response, query.npi)
    except SanicException as error:
        if error.status_code == 404:
            raise ManagedProviderDetailAbsent(manifest, query.npi) from error
        raise
    is_debug_enabled = native_args.get("debug") == "true"
    detail_by_field = await project_managed_detail_metadata(
        session,
        manifest,
        query.npi,
        detail_by_field,
        include_sources=is_debug_enabled or native_args.get("include_sources") == "true",
        include_evidence=is_debug_enabled or native_args.get("include_evidence") == "true",
    )
    if (
        type(detail_by_field) is not dict
        or type(detail_by_field.get("npi")) is not int
        or detail_by_field["npi"] != int(query.npi)
    ):
        raise RegistryNetworkCatalogError("registry_network_catalog_document_invalid")
    data_by_kind = {"npi": detail_by_field}
    if "taxonomy" in query.include:
        taxonomy = detail_by_field.get("taxonomy_list")
        if type(taxonomy) is not list:
            raise RegistryNetworkCatalogError("registry_network_catalog_document_invalid")
        data_by_kind["taxonomy"] = taxonomy
    return data_by_kind


async def _plans_data(driver, query, manifest, exclusions, network_ids, control_schema):
    options = {"network_ids": network_ids} if network_ids is not None else {}
    plans = await read_registry_provider_plans(
        driver,
        RegistryProviderPlansQuery(query.npi, generation_id=manifest.generation_id),
        excluded_network_ids=exclusions,
        control_schema=control_schema,
        manifest=manifest,
        **options,
    )
    if plans["generation"] != str(manifest.generation_id) or plans["approved_custom_revision"] != str(
        manifest.approved_custom_revision
    ):
        raise RegistryNetworkCatalogError("registry_network_catalog_source_changed")
    return {name: plans[name] for name in ("npi_data", "plan_data", "issuer_data")}


def _bounded_result(manifest, data_by_kind):
    result_by_field = {
        "generation": str(manifest.generation_id),
        "approved_custom_revision": str(manifest.approved_custom_revision),
        "manifest_sha256": manifest.manifest_sha256,
        "data": data_by_kind,
    }
    if (
        len(json.dumps(result_by_field, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode("utf-8"))
        > MAX_DETAIL_BYTES
    ):
        raise RegistryNetworkCatalogError("registry_network_catalog_response_bounds")
    return result_by_field


@asynccontextmanager
async def managed_provider_network_read_scope(
    session, *, selector, network_generation, excluded_network_ids, access_scope_sha256, organization_policy=None
):
    """Pin and authorize one collection or detail in the existing Reader snapshot."""
    from process.registry_network_catalog_read import registry_catalog_read_snapshot

    exclusions = _exclusions(excluded_network_ids)
    if organization_policy is not None and type(organization_policy) is not RegistryOrganizationVisibility:
        raise RegistryNetworkCatalogError("registry_network_catalog_request_invalid")
    control_schema = registry_schema()
    connection = await session.connection()
    driver = (await connection.get_raw_connection()).driver_connection
    async with registry_catalog_read_snapshot(driver, network_generation, control_schema) as manifest:
        if network_generation is not None and manifest.generation_id != network_generation:
            raise RegistryNetworkCatalogError("registry_network_catalog_source_changed")
        network_ids = await _selected_networks(
            driver, selector, manifest, exclusions, control_schema, organization_policy
        )
        scope_options = {"excluded_network_ids": exclusions} if network_ids is None else {}
        with network_address_read_scope(
            manifest,
            network_ids,
            access_scope_sha256=access_scope_sha256,
            organization_policy=organization_policy,
            **scope_options,
        ):
            yield manifest


@asynccontextmanager
async def managed_provider_detail_read_scope(
    session, query, *, excluded_network_ids, access_scope_sha256, organization_policy=None
):
    """Validate detail options before using the common policy-bound network scope."""
    _native_args(query)
    async with managed_provider_network_read_scope(
        session,
        selector=query.selector,
        network_generation=query.network_generation,
        excluded_network_ids=excluded_network_ids,
        access_scope_sha256=access_scope_sha256,
        organization_policy=organization_policy,
    ) as manifest:
        yield manifest


async def compose_managed_provider_detail(request, query, session, manifest, *, excluded_network_ids):
    """Compose only inside the exact already-authorized manifest scope; never select a current head."""
    scope = current_network_address_scope()
    if scope is None or scope.manifest is not manifest:
        raise RegistryNetworkCatalogError("registry_network_catalog_request_invalid")
    native_args = _native_args(query)
    exclusions = _exclusions(excluded_network_ids)
    if scope.network_ids is None and scope.excluded_network_ids != exclusions:
        raise RegistryNetworkCatalogError("registry_network_catalog_request_invalid")
    data_by_kind = await _detail_data(request, query, session, manifest, native_args)
    if "plans" in query.include:
        connection = await session.connection()
        driver = (await connection.get_raw_connection()).driver_connection
        data_by_kind["plans_by_npi"] = await _plans_data(
            driver, query, manifest, exclusions, scope.network_ids, registry_schema()
        )
    return _bounded_result(manifest, data_by_kind)


async def read_managed_provider_detail(
    request, query, *, excluded_network_ids, access_scope_sha256, organization_policy=None
):
    """Use the real request and one explicit manifest for every managed detail component."""
    from api.endpoint import npi as provider

    try:
        _native_args(query)
        _exclusions(excluded_network_ids)
        if organization_policy is not None and type(organization_policy) is not RegistryOrganizationVisibility:
            raise RegistryNetworkCatalogError("registry_network_catalog_request_invalid")
        async with provider_profile_read_snapshot(
            provider.db, provider._runtime_db_schema(), include_detail=True
        ) as session:
            async with managed_provider_detail_read_scope(
                session,
                query,
                excluded_network_ids=excluded_network_ids,
                access_scope_sha256=access_scope_sha256,
                organization_policy=organization_policy,
            ) as manifest:
                return await compose_managed_provider_detail(
                    request, query, session, manifest, excluded_network_ids=excluded_network_ids
                )
    except RegistryNetworkCatalogError:
        raise
    except (asyncpg.PostgresError, DBAPIError, ValueError, TypeError, KeyError, UnicodeError) as error:
        raise RegistryNetworkCatalogError("registry_network_catalog_unavailable") from error
