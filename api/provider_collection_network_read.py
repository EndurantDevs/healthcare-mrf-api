# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Compose native collections under one approved, policy-bound Reader snapshot."""

import json
from contextlib import AsyncExitStack, contextmanager

import asyncpg
from sanic import response
from sanic.exceptions import SanicException
from sanic.request.parameters import RequestParameters
from sqlalchemy.exc import DBAPIError

from api.plan_pricing_projection_contract import PlanPricingProjectionUnavailable
from api.provider_collection_network_contract import (
    ProviderCollectionNativeBatch,
    ProviderCollectionNetworkQuery,
    parse_managed_collection_query,
)
from api.provider_detail_network_read import managed_provider_network_read_scope
from api.provider_profile_snapshot import provider_profile_read_snapshot
from process.registry_catalog_policy import RegistryOrganizationVisibility
from process.registry_network_catalog_read import (
    MAX_RESPONSE_BYTES,
    RegistryNetworkCatalogError,
    RegistryNetworkCatalogLegacyBatchSelector,
    _closed_json,
    _exclusions,
)

MAX_COLLECTION_BYTES = 32 * 1024 * 1024
_SERVICE_REFUSAL_CODES = {
    "ptg2_provider_scope_refused",
    "ptg2_online_work_budget_exceeded",
    "ptg2_provider_filter_scope_required",
    "ptg2_location_scope_too_broad",
    "ptg2_provider_filter_unsupported",
}


class ProviderCollectionServiceRefusal(Exception):
    """Carry only documented, bounded native service refusal fields."""

    def __init__(self, encoded):
        super().__init__("Provider service scope refused.")
        self.document = _service_refusal_document(encoded)


def _service_refusal_document(encoded):
    try:
        document_by_field = _closed_json(encoded, MAX_RESPONSE_BYTES)
        if type(document_by_field) is not dict or type(document_by_field.get("status")) is not int:
            raise ValueError("invalid refusal")
        code = document_by_field.get("code")
        message = document_by_field.get("message")
        fix_by_field = document_by_field.get("fix_it")
        if (
            document_by_field["status"] != 422
            or type(code) is not str
            or code not in _SERVICE_REFUSAL_CODES
            or type(message) is not str
            or not message
            or type(fix_by_field) is not dict
        ):
            raise ValueError("invalid refusal")
        reason = fix_by_field.get("reason")
        retry_options = fix_by_field.get("retry_options")
        if (
            type(reason) is not str
            or not reason
            or type(retry_options) is not list
            or any(type(option) is not dict for option in retry_options)
        ):
            raise ValueError("invalid refusal")
        sanitized_fix_by_field = {"reason": reason, "retry_options": []}
        encoded_fix = json.dumps(sanitized_fix_by_field, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        if len(encoded_fix) > 8192:
            raise ValueError("invalid refusal")
        message.encode("utf-8")
        return {"status": 422, "code": code, "message": message[:300], "fix_it": sanitized_fix_by_field}
    except RegistryNetworkCatalogError, ValueError, TypeError, UnicodeError, RecursionError:
        raise SanicException("Provider data is unavailable.", status_code=503) from None


def _validated_inputs(query):
    if type(query) is not ProviderCollectionNetworkQuery:
        raise RegistryNetworkCatalogError("registry_network_catalog_request_invalid")
    try:
        native_by_name = {name: list(entry) if type(entry) is tuple else entry for name, entry in query.native_query}
        query_by_field = {"kind": query.kind, "native_query": native_by_name}
        if query.native_batch is not None:
            if type(query.native_batch) is not ProviderCollectionNativeBatch:
                raise ValueError("invalid batch")
            query_by_field["native_batch"] = query.native_batch.document()
        if query.network_generation is not None:
            query_by_field["network_generation"] = str(query.network_generation)
        if query.selector is not None:
            if type(query.selector) is tuple:
                namespace, selected = "canonical", list(query.selector)
            elif type(query.selector) is RegistryNetworkCatalogLegacyBatchSelector:
                namespace, selected = query.selector.namespace, list(query.selector.values)
            else:
                raise ValueError("invalid selector")
            query_by_field["selector"] = {"namespace": namespace, "values": selected}
        if parse_managed_collection_query(query_by_field) != query:
            raise ValueError("invalid query")
        args = RequestParameters(
            {name: entry if type(entry) is list else [entry] for name, entry in native_by_name.items()}
        )
        batch_by_name = query.native_batch.document() if query.native_batch is not None else None
        if batch_by_name is not None:
            batch_by_name["npis"] = [int(npi) for npi in query.native_batch.npis]
        return args, batch_by_name
    except (SanicException, ValueError, TypeError, KeyError, UnicodeError) as error:
        raise RegistryNetworkCatalogError("registry_network_catalog_request_invalid") from error


@contextmanager
def _request_reader(request, session):
    """Native request-aware helpers must use the same explicit Reader snapshot."""
    absent = object()
    prior = getattr(request.ctx, "sa_session", absent)
    request.ctx.sa_session = session
    try:
        yield
    finally:
        if prior is absent:
            del request.ctx.sa_session
        else:
            request.ctx.sa_session = prior


def _native_document(http_response, *, preserve_service_refusal=False):
    if http_response.status == 422 and preserve_service_refusal:
        raise ProviderCollectionServiceRefusal(http_response.body)
    if http_response.status != 200:
        status = http_response.status if http_response.status in {400, 403, 404, 409, 503} else 503
        raise SanicException("Provider data is unavailable.", status_code=status)
    document = _closed_json(http_response.body, MAX_COLLECTION_BYTES)
    if type(document) not in {dict, list}:
        raise RegistryNetworkCatalogError("registry_network_catalog_document_invalid")
    return document


async def _collection_data(request, query, session, native_args, batch_params):
    from api.endpoint import npi as provider

    if query.kind == "by-service":
        from api.custom_import_provider_service_http import _service_page

        return _native_document(await _service_page(request, native_args=native_args), preserve_service_refusal=True)
    if query.kind == "group-plan":
        from api.endpoint.pricing import group_plan_providers

        return _native_document(await group_plan_providers(request, native_args=native_args))
    if query.kind == "billing-search":
        from api.provider_billing_network_read import read_managed_billing_search

        return _native_document(await read_managed_billing_search(request, session=session, native_args=native_args))
    if query.kind == "batch":
        from api.provider_batch import read_native_batch

        document = await read_native_batch(request, batch_params, native_args=native_args, session=session)
        return _native_document(response.json(document, default=str))
    native_read = provider.list_providers if query.kind == "list" else provider.get_near_npi
    return _native_document(await native_read(request, native_args=native_args))


def _bounded_result(manifest, document):
    result_by_field = {
        "generation": str(manifest.generation_id),
        "approved_custom_revision": str(manifest.approved_custom_revision),
        "manifest_sha256": manifest.manifest_sha256,
        "data": document,
    }
    encoded = json.dumps(result_by_field, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode("utf-8")
    if len(encoded) > MAX_COLLECTION_BYTES:
        raise RegistryNetworkCatalogError("registry_network_catalog_response_bounds")
    return result_by_field


async def read_managed_provider_collection(
    request, query, *, excluded_network_ids, access_scope_sha256, organization_policy=None, client_id=None
):
    """Authorize before native counts, selection and hydration on the real request."""
    from api.endpoint import npi as provider

    native_args, batch_params = _validated_inputs(query)
    _exclusions(excluded_network_ids)
    if organization_policy is not None and type(organization_policy) is not RegistryOrganizationVisibility:
        raise RegistryNetworkCatalogError("registry_network_catalog_request_invalid")
    try:
        if query.kind == "billing-search":
            from api import billing_search_http
            from api import provider_billing_network_read as billing

            try:
                access, _ = billing.authorize_managed_billing_search(request, native_args=native_args)
                billing.require_managed_billing_client(access, client_id)
            except Exception as error:
                raise SanicException(
                    "Provider data is unavailable.", status_code=billing_search_http._failure_status(error)
                ) from None
        async with AsyncExitStack() as stack:
            if query.kind == "by-service" and any(
                native_args.get(name)
                for name in ("plan_release_id", "plan_id", "plan_external_id", "source_key", "snapshot_id")
            ):
                from api.custom_import_plan_sql import prepare_plan_query_tables

                await stack.enter_async_context(provider.db.reader_session(temporary_setup=prepare_plan_query_tables))
            session = await stack.enter_async_context(
                provider_profile_read_snapshot(provider.db, provider._runtime_db_schema(), include_detail=True)
            )
            async with managed_provider_network_read_scope(
                session,
                selector=query.selector,
                network_generation=query.network_generation,
                excluded_network_ids=excluded_network_ids,
                access_scope_sha256=access_scope_sha256,
                organization_policy=organization_policy,
            ) as manifest:
                with _request_reader(request, session):
                    document = await _collection_data(request, query, session, native_args, batch_params)
                return _bounded_result(manifest, document)
    except RegistryNetworkCatalogError:
        raise
    except SanicException as error:
        status = error.status_code if error.status_code in {400, 403, 404, 409, 503} else 503
        raise SanicException("Provider data is unavailable.", status_code=status) from error
    except (
        asyncpg.PostgresError,
        DBAPIError,
        PlanPricingProjectionUnavailable,
        ValueError,
        TypeError,
        KeyError,
        UnicodeError,
    ) as error:
        raise RegistryNetworkCatalogError("registry_network_catalog_unavailable") from error
