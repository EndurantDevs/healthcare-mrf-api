# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Signed managed collection policy and one shared native/import Reader scope."""

import json
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass

from sanic import response
from sanic.exceptions import SanicException

from api import custom_import_read_http as transport
from api.custom_import_provider_detail import parse_managed_provider_policy
from api.endpoint import npi
from api.provider_collection_network_contract import ProviderCollectionNetworkQuery, parse_managed_collection_query
from api.provider_collection_network_read import MAX_COLLECTION_BYTES, _request_reader
from api.provider_detail_network_read import managed_provider_network_read_scope
from api.provider_profile_snapshot import provider_profile_read_snapshot
from process.custom_import.read_contracts import (
    DEFAULT_READ_TIMEOUT_MS,
    CustomImportReadRequestError,
    CustomImportReadUnavailableError,
)
from process.custom_import.read_core import _bounded_read_window
from process.registry_catalog_policy import RegistryOrganizationVisibility
from process.registry_network_catalog_read import RegistryNetworkCatalogError

MAX_MANAGED_BODY_BYTES = transport._MAX_MANAGED_PROVIDER_BODY_BYTES


@dataclass(frozen=True)
class ManagedProviderCollection:
    policy: dict
    query: ProviderCollectionNetworkQuery
    exclusions: tuple[int, ...]
    scope_sha256: str
    organization_policy: RegistryOrganizationVisibility


def parse_managed_provider_collection(document, *, kind, native_query, native_batch=None):
    """Require one complete policy and the exact outer signed native descriptors."""
    try:
        policy, exclusions, scope, visibility = parse_managed_provider_policy(document)
        query_document = document["query"]
        query = parse_managed_collection_query(query_document)
        if (
            query.kind != kind
            or transport._canonical_json_bytes(query_document["native_query"])
            != transport._canonical_json_bytes(native_query)
            or (
                kind == "batch"
                and transport._canonical_json_bytes(query_document["native_batch"])
                != transport._canonical_json_bytes(native_batch)
            )
            or (kind != "batch" and native_batch is not None)
        ):
            raise ValueError("native query mismatch")
        return ManagedProviderCollection(policy, query, exclusions, scope, visibility)
    except (ValueError, TypeError, KeyError, UnicodeError, CustomImportReadRequestError) as error:
        raise CustomImportReadRequestError("managed provider collection is invalid") from error


def parse_optional_managed_collection(document, *, kind, native_batch=None):
    """Reject conflicting authority modes before parsing an optional collection."""
    if "managed_collection" not in document:
        return None
    if "network_binding" in document or "managed_detail" in document:
        raise CustomImportReadRequestError("managed provider modes are mutually exclusive")
    return parse_managed_provider_collection(
        document["managed_collection"], kind=kind, native_query=document["native_query"], native_batch=native_batch
    )


@asynccontextmanager
async def managed_import_collection_scope(request, managed, *, temporary_setup=None):
    """Pin native eligibility, import scoring and family hydration in one Reader."""
    async with AsyncExitStack() as stack:
        if temporary_setup is not None:
            await stack.enter_async_context(npi.db.reader_session(temporary_setup=temporary_setup))
        session = await stack.enter_async_context(
            provider_profile_read_snapshot(npi.db, npi._runtime_db_schema(), include_detail=True)
        )
        async with _bounded_read_window(session, timeout_ms=DEFAULT_READ_TIMEOUT_MS):
            async with managed_provider_network_read_scope(
                session,
                selector=managed.query.selector,
                network_generation=managed.query.network_generation,
                excluded_network_ids=managed.exclusions,
                access_scope_sha256=managed.scope_sha256,
                organization_policy=managed.organization_policy,
            ) as manifest:
                with _request_reader(request, session):
                    yield session, manifest


def managed_collection_metadata(managed, manifest):
    """Return only the approved policy and manifest identity proof."""
    return {
        "client_id": managed.policy["client_id"],
        "policy_revision": managed.policy["policy_revision"],
        "generation": str(manifest.generation_id),
        "approved_custom_revision": str(manifest.approved_custom_revision),
        "manifest_sha256": manifest.manifest_sha256,
    }


def managed_collection_response(payload, managed, manifest, *, maximum_bytes):
    """Keep the existing family envelope and enforce both presentation bounds."""
    document = payload | {"managed_collection": managed_collection_metadata(managed, manifest)}
    encoded = transport._canonical_json_bytes(json.loads(response.json(document, default=str).body))
    if len(encoded) > min(MAX_COLLECTION_BYTES, maximum_bytes):
        raise CustomImportReadUnavailableError("provider response is unavailable")
    reply = transport._response(encoded, 200)
    reply.headers["X-Network-Generation"] = str(manifest.generation_id)
    reply.headers["X-Network-Access-Scope"] = managed.scope_sha256
    return reply


def managed_collection_error(failure):
    """Keep native failure status while emitting only the closed error envelope."""
    if isinstance(failure, SanicException):
        status = failure.status_code if failure.status_code in {400, 403, 404, 409, 503} else 503
    elif isinstance(failure, RegistryNetworkCatalogError):
        status = {
            "registry_network_catalog_request_invalid": 400,
            "registry_network_catalog_detail_denied": 404,
            "registry_network_catalog_source_changed": 409,
        }.get(str(failure), 503)
    else:
        status = transport._failure_status(failure)
    return transport._error(status)
