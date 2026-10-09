# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Signed filtering of a finite native batch through one pinned import query."""

from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass, replace

from sanic import response
from sanic.exceptions import InvalidUsage
from sqlalchemy import text

from api import custom_import_provider_http as provider_http
from api import custom_import_read_http as transport
from api import provider_batch
from api.custom_import_provider_sql import ProviderImportQuery, compile_npi_entity_relation
from api.endpoint import npi
from process.custom_import.read_contracts import (
    DEFAULT_READ_TIMEOUT_MS,
    CustomImportReadRequestError,
    CustomImportReadUnavailableError,
    ExtensionReadAuthorization,
)
from process.custom_import.read_core import CustomImportReadService, _bounded_read_window
from process.custom_import.read_identity import verify_published_generation

CUSTOM_IMPORT_PROVIDER_BATCH_PATH = "/api/v1/extensions/custom-import/providers/batch"
_BATCH_KEYS = {"npis", "address_limit", "address_offset", "include_sources", "include_evidence"}


@dataclass(frozen=True, slots=True)
class _ParsedBatchRequest:
    provider: provider_http._ParsedProviderRequest
    native_batch: dict[str, object]

    @property
    def target(self):
        """Reuse the authenticated provider target for path-bound transport."""
        return self.provider.target


def _parse_batch_request(body):
    document = transport._strict_json(body)
    if type(document) is not dict or transport._canonical_json_bytes(document) != body:
        raise transport._fail()
    native_batch = document.get("native_batch")
    if type(native_batch) is not dict or set(native_batch) != _BATCH_KEYS:
        raise CustomImportReadRequestError("native batch is invalid")
    canonical_npis = native_batch.get("npis")
    if type(canonical_npis) is not list or any(type(identity) is not str for identity in canonical_npis):
        raise CustomImportReadRequestError("native batch identities are invalid")
    normalized = npi._normalize_npi_batch_request(native_batch)
    if canonical_npis != [str(identity) for identity in normalized["npis"]]:
        raise CustomImportReadRequestError("native batch identities are not canonical")
    provider_by_field = {name: value for name, value in document.items() if name != "native_batch"}
    parsed = provider_http._parse_provider_request(
        transport._canonical_json_bytes(provider_by_field),
        native_query_parser=provider_batch.parse_native_batch_query,
        maximum_family_rows=npi.NPI_BATCH_MAX_SIZE,
    )
    provider_batch._batch_shape_params(normalized, parsed.native_args)
    return _ParsedBatchRequest(parsed, normalized)


async def serve_custom_import_provider_batch(request, session):
    """Authorize before opening one bounded read-only batch snapshot."""

    if (
        getattr(request, "method", None) != "POST"
        or getattr(request, "path", None) != CUSTOM_IMPORT_PROVIDER_BATCH_PATH
        or getattr(request, "query_string", "")
    ):
        return transport._error(404)
    try:
        body = getattr(request, "body", None)
        if type(body) is not bytes or not 1 <= len(body) <= transport._MAX_BODY_BYTES:
            raise transport._fail()
        parsed = _parse_batch_request(body)
        verified = transport._verify_provider_transport(
            headers=request.headers,
            body=body,
            request=parsed,
            trusted_now=transport._trusted_now(),
            keyring=transport._keyring(),
            path=CUSTOM_IMPORT_PROVIDER_BATCH_PATH,
        )
        if session is None or session.in_transaction():
            raise CustomImportReadUnavailableError("fresh provider read session is required")
        async with asyncio.timeout(DEFAULT_READ_TIMEOUT_MS / 1000), session.begin():
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
            async with _bounded_read_window(session, timeout_ms=DEFAULT_READ_TIMEOUT_MS):
                batch_payload = await _read_batch_payload(request, session, parsed, verified)
                encoded = transport._canonical_json_bytes(json.loads(response.json(batch_payload, default=str).body))
                bound = (len(parsed.native_batch["npis"]) + 1) * transport._MAX_RESPONSE_BYTES
                if len(encoded) > bound:
                    raise CustomImportReadUnavailableError("provider response is unavailable")
                return transport._response(encoded, 200)
    except Exception as failure:
        transport._log_failure(failure)
        return transport._error(400 if isinstance(failure, InvalidUsage) else transport._failure_status(failure))


async def _read_batch_payload(request, session, parsed, verified):
    started = time.monotonic()
    pinned_target = await transport._resolve_pinned_target(session, parsed.target)
    service = CustomImportReadService(authorizer=transport._TransportAuthorizer(verified, pinned_target))
    authorization = ExtensionReadAuthorization(verified.credential)
    query = replace(
        provider_http._provider_relation_query(parsed.provider),
        entity_values=tuple(str(identity) for identity in parsed.native_batch["npis"]),
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
    batch_payload = await provider_batch.read_native_batch(
        request, parsed.native_batch, native_args=parsed.provider.native_args, session=session, import_context=context
    )
    providers = [
        provider_item["provider"] for provider_item in batch_payload["items"] if provider_item["status"] == 200
    ]
    await provider_http._hydrate_provider_rows(
        session,
        service,
        authorization,
        pinned_target,
        prepared,
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
