# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Signed filtering of a finite native batch through one pinned import query."""

from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass, replace

from sanic import response
from sanic.exceptions import InvalidUsage, SanicException
from sqlalchemy import text

from api import custom_import_provider_collection as managed_collection
from api import custom_import_provider_detail as managed_detail
from api import custom_import_provider_http as provider_http
from api import custom_import_read_http as transport
from api import provider_batch
from api.custom_import_provider_sql import ProviderImportQuery, compile_npi_entity_relation
from api.endpoint import npi
from api.signed_request_reader import signed_request_reader
from process.custom_import.read_contracts import (
    DEFAULT_READ_TIMEOUT_MS,
    CustomImportReadRequestError,
    CustomImportReadUnavailableError,
    ExtensionReadAuthorization,
)
from process.custom_import.read_core import CustomImportReadService, _bounded_read_window
from process.custom_import.read_identity import verify_published_generation
from process.registry_network_catalog_read import RegistryNetworkCatalogError

CUSTOM_IMPORT_PROVIDER_BATCH_PATH = "/api/v1/extensions/custom-import/providers/batch"
_BATCH_KEYS = {"npis", "address_limit", "address_offset", "include_sources", "include_evidence"}


@dataclass(frozen=True, slots=True)
class _ParsedBatchRequest:
    provider: provider_http._ParsedProviderRequest
    native_batch: dict[str, object]
    managed_detail: managed_detail.ManagedProviderDetail | None = None

    @property
    def managed_collection(self):
        """Expose the complete parsed collection to path-bound transport verification."""
        return self.provider.managed_collection

    @property
    def target(self):
        """Reuse the authenticated provider target for path-bound transport."""
        return self.provider.target


def _parse_batch_request(body):
    document = transport._strict_json(body)
    if type(document) is not dict or transport._canonical_json_bytes(document) != body:
        raise transport._fail()
    native_batch = document.get("native_batch")
    if (
        type(native_batch) is not dict
        or set(native_batch) != _BATCH_KEYS
        or type(native_batch.get("address_limit")) is not int
    ):
        raise CustomImportReadRequestError("native batch is invalid")
    canonical_npis = native_batch.get("npis")
    if type(canonical_npis) is not list or any(type(identity) is not str for identity in canonical_npis):
        raise CustomImportReadRequestError("native batch identities are invalid")
    if sum(name in document for name in ("managed_collection", "managed_detail", "network_binding")) > 1:
        raise CustomImportReadRequestError("managed provider modes are mutually exclusive")
    managed = (
        managed_detail.parse_managed_provider_detail(document["managed_detail"])
        if "managed_detail" in document
        else None
    )
    if managed is not None:
        expected_batch_by_field = {
            "npis": [managed.query.npi],
            "address_limit": 5,
            "address_offset": 0,
            "include_sources": False,
            "include_evidence": False,
        }
        if (
            "network_binding" in document
            or document.get("native_query") != {"include_total": "true"}
            or native_batch != expected_batch_by_field
            or type(native_batch["address_offset"]) is not int
            or any(type(native_batch[name]) is not bool for name in ("include_sources", "include_evidence"))
        ):
            raise CustomImportReadRequestError("managed provider native shape is invalid")
    normalized = npi._normalize_npi_batch_request(native_batch)
    if managed is None and canonical_npis != [str(identity) for identity in normalized["npis"]]:
        raise CustomImportReadRequestError("native batch identities are not canonical")
    provider_by_field = {
        name: field_value for name, field_value in document.items() if name not in {"native_batch", "managed_detail"}
    }
    parsed = provider_http._parse_provider_request(
        transport._canonical_json_bytes(provider_by_field),
        native_query_parser=provider_batch.parse_native_batch_query,
        maximum_family_rows=npi.NPI_BATCH_MAX_SIZE,
        allow_network_binding=True,
        network_document=document,
        allow_managed_collection=True,
        collection_kind="batch",
    )
    provider_batch._batch_shape_params(normalized, parsed.native_args)
    return _ParsedBatchRequest(parsed, normalized, managed)


async def serve_custom_import_provider_batch(request, session):
    """Authorize before opening one bounded read-only batch snapshot."""

    if (
        getattr(request, "method", None) != "POST"
        or getattr(request, "path", None) != CUSTOM_IMPORT_PROVIDER_BATCH_PATH
        or getattr(request, "query_string", "")
    ):
        return transport._error(404)
    is_managed_mode = False
    try:
        body = getattr(request, "body", None)
        if type(body) is not bytes or not 1 <= len(body) <= managed_detail.MAX_MANAGED_BODY_BYTES:
            raise transport._fail()
        parsed = _parse_batch_request(body)
        is_managed_mode = parsed.managed_detail is not None or parsed.managed_collection is not None
        if not is_managed_mode and len(body) > transport._MAX_BODY_BYTES:
            raise transport._fail()
        transport_options = (
            {"managed_detail": True}
            if parsed.managed_detail is not None
            else {"managed_collection": True}
            if parsed.managed_collection is not None
            else {}
        )
        verified = transport._verify_provider_transport(
            headers=request.headers,
            body=body,
            request=parsed,
            trusted_now=transport._trusted_now(),
            keyring=transport._keyring(),
            path=CUSTOM_IMPORT_PROVIDER_BATCH_PATH,
            **transport_options,
        )
        if is_managed_mode:
            return await _serve_managed_batch(request, parsed, verified)
        async with asyncio.timeout(DEFAULT_READ_TIMEOUT_MS / 1000), signed_request_reader(request, session) as session:
            async with (
                _bounded_read_window(session, timeout_ms=DEFAULT_READ_TIMEOUT_MS),
                provider_http.provider_network_read_scope(session, parsed.provider.network_binding) as manifest,
            ):
                batch_payload = await _read_batch_payload(request, session, parsed, verified)
                encoded = transport._canonical_json_bytes(json.loads(response.json(batch_payload, default=str).body))
                bound = (len(parsed.native_batch["npis"]) + 1) * transport._MAX_RESPONSE_BYTES
                if len(encoded) > bound:
                    raise CustomImportReadUnavailableError("provider response is unavailable")
                return provider_http.provider_network_response(encoded, manifest)
    except Exception as failure:
        transport._log_failure(failure)
        if is_managed_mode and parsed.managed_collection is not None:
            return managed_collection.managed_collection_error(failure)
        if is_managed_mode and isinstance(failure, SanicException):
            return transport._error(failure.status_code if failure.status_code in {400, 404, 503} else 503)
        if is_managed_mode and isinstance(failure, RegistryNetworkCatalogError):
            return transport._error(404 if str(failure) == "registry_network_catalog_detail_denied" else 503)
        return transport._error(400 if isinstance(failure, InvalidUsage) else transport._failure_status(failure))


async def _serve_managed_batch(request, parsed, verified):
    """Compose either managed family only after its complete signed permit passes."""
    if parsed.managed_collection is not None:
        async with asyncio.timeout(DEFAULT_READ_TIMEOUT_MS / 1000):
            async with managed_collection.managed_import_collection_scope(request, parsed.managed_collection) as (
                reader,
                manifest,
            ):
                batch_payload = await _read_batch_payload(request, reader, parsed, verified)
                return managed_collection.managed_collection_response(
                    batch_payload,
                    parsed.managed_collection,
                    manifest,
                    maximum_bytes=(len(parsed.native_batch["npis"]) + 1) * transport._MAX_RESPONSE_BYTES,
                )
    if parsed.managed_detail is not None:
        async with asyncio.timeout(DEFAULT_READ_TIMEOUT_MS / 1000):
            batch_payload, manifest = await managed_detail.read_managed_import_detail(request, parsed, verified)
        encoded = transport._canonical_json_bytes(json.loads(response.json(batch_payload, default=str).body))
        if len(encoded) > managed_detail.detail.MAX_DETAIL_BYTES:
            raise CustomImportReadUnavailableError("provider response is unavailable")
        reply = provider_http.provider_network_response(encoded, manifest)
        reply.headers["X-Network-Access-Scope"] = parsed.managed_detail.scope_sha256
        return reply


async def _read_batch_payload(request, session, parsed, verified):
    started = time.monotonic()
    pinned_target = await transport._resolve_pinned_target(session, parsed.target)
    service = CustomImportReadService(authorizer=transport._TransportAuthorizer(verified, pinned_target))
    authorization = ExtensionReadAuthorization(verified.credential)
    query = replace(
        provider_http._provider_relation_query(parsed.provider),
        entity_values=(parsed.managed_detail.query.npi,)
        if parsed.managed_detail is not None
        else tuple(str(identity) for identity in parsed.native_batch["npis"]),
    )
    prepared = await service.prepare_npi_entity_relation(
        session, authorization=authorization, target=pinned_target, query=query
    )
    context = ProviderImportQuery(
        prepared,
        compile_npi_entity_relation(prepared.statement),
        query.require_match,
        native_npis=tuple(parsed.native_batch["npis"]),
    )
    scope_preparer = provider_http._ProviderScopePreparer(
        session, service, authorization, pinned_target, query, prepared
    )
    batch_payload = await provider_batch.read_native_batch(
        request,
        parsed.native_batch,
        native_args=parsed.provider.native_args,
        session=session,
        import_context=context,
        native_scope_preparer=scope_preparer.prepare_provider_scope,
    )
    providers = [
        provider_item["provider"] for provider_item in batch_payload["items"] if provider_item["status"] == 200
    ]
    if parsed.managed_detail is not None:
        for provider_item in batch_payload["items"]:
            provider_item["npi"] = parsed.managed_detail.query.npi
            if provider_item["status"] == 200:
                provider_item["provider"]["npi"] = parsed.managed_detail.query.npi
    await provider_http._hydrate_provider_rows(
        session,
        service,
        authorization,
        pinned_target,
        scope_preparer.prepared,
        parsed.provider,
        query,
        providers,
    )
    await verify_published_generation(session, pinned_target)
    batch_payload["meta"] = {
        "elapsed_ms": round((time.monotonic() - started) * 1000, 2),
        "max_batch_size": npi.NPI_BATCH_MAX_SIZE,
        "view": "summary",
    }
    return batch_payload
