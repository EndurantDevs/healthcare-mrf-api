# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Signed imported-field geo reads with live-native identity pagination."""

from __future__ import annotations

import asyncio
import hashlib
import json
from dataclasses import dataclass
from typing import Any

from sanic.exceptions import InvalidUsage
from sanic.request.parameters import RequestParameters
from sqlalchemy import text

from api import custom_import_read_http as transport
from api.custom_import_provider_geo_cursor import GeoCursorState, issue_geo_cursor, open_geo_cursor
from api.custom_import_provider_http import _hydrate_provider_rows, _parse_provider_request, _provider_relation_query
from api.custom_import_provider_sql import ProviderImportQuery, compile_npi_entity_relation
from process.custom_import.read_contracts import (
    DEFAULT_READ_TIMEOUT_MS,
    MAX_CURSOR_TTL_SECONDS,
    CustomImportReadCursorError,
    CustomImportReadRequestError,
    CustomImportReadUnavailableError,
    ExtensionReadAuthorization,
    PinnedReadTarget,
    canonical_read_document,
)
from process.custom_import.read_core import CustomImportReadService, _bounded_read_window, _validate_npi_page
from process.custom_import.read_identity import verify_published_generation

CUSTOM_IMPORT_PROVIDER_GEO_PATH = "/api/v1/extensions/custom-import/providers/geo"
MAX_GEO_PAGE_SIZE = 50
_GEO_QUERY_FIELDS = frozenset(
    "lat long zip_codes radius limit cursor include_total view q exclude_npi codes plan_network "
    "classification specialization section display_name primary_only entity_type_code provider_sex_code "
    "procedure_codes procedure_code_system medication_codes medication_code_system year plan_release_id".split()
)


def _parse_geo_native_query(document: object) -> RequestParameters:
    """Close the signed native geo shape and require exact bounded pages."""

    if (
        type(document) is not dict
        or not set(document) <= _GEO_QUERY_FIELDS
        or any(type(value) is not str or len(value) > 2048 or "\x00" in value for value in document.values())
        or str(document.get("view", "")).strip().lower() not in {"", "card"}
        or str(document.get("include_total", "true")).strip().lower() not in {"1", "true", "yes", "on"}
    ):
        raise CustomImportReadRequestError("native provider geo query is invalid")
    try:
        limit = int(document.get("limit", "5"))
    except ValueError:
        raise CustomImportReadRequestError("native provider geo query is invalid") from None
    if not 1 <= limit <= MAX_GEO_PAGE_SIZE:
        raise CustomImportReadRequestError("native provider geo query is invalid")
    return RequestParameters({**{name: [value] for name, value in document.items()}, "include_total": ["true"]})


@dataclass
class _GeoCursorBinding:
    """Request-local binding after native coordinates and filters are resolved."""

    session: Any
    pinned_target: PinnedReadTarget
    import_context: ProviderImportQuery
    native_args: RequestParameters
    secret: bytes
    trusted_now: int
    query_fingerprint: str | None = None
    prior_cursor: GeoCursorState | None = None

    async def prepare(self, *, session: Any, **effective_query: Any) -> tuple[str, str] | None:
        """Open the cursor only against the complete effective native query."""

        if session is not self.session or self.query_fingerprint is not None:
            raise CustomImportReadUnavailableError("provider geo snapshot is unavailable")
        self.query_fingerprint = hashlib.sha256(
            canonical_read_document(
                {
                    "contract": "custom-import-provider-geo/v1",
                    "native_query": {
                        name: self.native_args.getlist(name) for name in self.native_args if name != "cursor"
                    },
                    "effective_query": effective_query,
                    "import_query": self.import_context.prepared.query_fingerprint,
                    "require_match": self.import_context.require_match,
                }
            )
        ).hexdigest()
        token = self.native_args.get("cursor")
        if token is None:
            return None
        self.prior_cursor = open_geo_cursor(
            token,
            secret=self.secret,
            pinned_target=self.pinned_target,
            query_fingerprint=self.query_fingerprint,
            authorization_scope_sha256=self.import_context.prepared.authorization_scope_sha256,
            trusted_now=self.trusted_now,
        )
        return self.prior_cursor.anchor_npi, self.prior_cursor.anchor_address_key

    def next_cursor(self, anchor: list[str] | None) -> str | None:
        """Preserve the original expiry rather than extending every page's TTL."""

        if self.query_fingerprint is None:
            raise CustomImportReadUnavailableError("provider geo cursor binding is unavailable")
        if anchor is None:
            return None
        state = GeoCursorState(
            target=self.pinned_target,
            query_fingerprint=self.query_fingerprint,
            authorization_scope_sha256=self.import_context.prepared.authorization_scope_sha256,
            anchor_npi=anchor[0],
            anchor_address_key=anchor[1],
            issued_at=self.prior_cursor.issued_at if self.prior_cursor else self.trusted_now,
            expires_at=self.prior_cursor.expires_at if self.prior_cursor else self.trusted_now + MAX_CURSOR_TTL_SECONDS,
        )
        try:
            return issue_geo_cursor(state, secret=self.secret)
        except CustomImportReadCursorError:
            raise CustomImportReadUnavailableError("provider geo continuation is unavailable") from None


async def serve_custom_import_provider_geo(request: Any, session: Any):
    """Authorize before all geo reads and use one bounded read-only snapshot."""

    if (
        getattr(request, "method", None) != "POST"
        or getattr(request, "path", None) != CUSTOM_IMPORT_PROVIDER_GEO_PATH
        or getattr(request, "query_string", "")
    ):
        return transport._error(404)
    try:
        body = getattr(request, "body", None)
        if type(body) is not bytes or not 1 <= len(body) <= transport._MAX_BODY_BYTES:
            raise transport._fail()
        parsed = _parse_provider_request(body, native_query_parser=_parse_geo_native_query)
        trusted_now = transport._trusted_now()
        verified = transport._verify_provider_transport(
            headers=request.headers,
            body=body,
            request=parsed,
            trusted_now=trusted_now,
            keyring=transport._keyring(),
            path=CUSTOM_IMPORT_PROVIDER_GEO_PATH,
        )
        cursor_secret = transport._cursor_secret()
        if session is None or session.in_transaction():
            raise CustomImportReadUnavailableError("fresh provider geo read session is required")
        async with asyncio.timeout(DEFAULT_READ_TIMEOUT_MS / 1000), session.begin():
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
            async with _bounded_read_window(session, timeout_ms=DEFAULT_READ_TIMEOUT_MS):
                geo_payload = await _read_geo_payload(request, session, parsed, verified, cursor_secret, trusted_now)
                encoded = transport._canonical_json_bytes(geo_payload)
                if len(encoded) > transport._MAX_RESPONSE_BYTES:
                    raise CustomImportReadUnavailableError("provider geo response is unavailable")
                return transport._response(encoded, 200)
    except Exception as failure:
        transport._log_failure(failure)
        status = 400 if isinstance(failure, InvalidUsage) else transport._failure_status(failure)
        return transport._error(status)


async def _read_geo_payload(request, session, parsed, verified, cursor_secret, trusted_now):
    """Compose live native addresses with one immutable imported family pin."""

    pinned_target = await transport._resolve_pinned_target(session, parsed.target)
    service = CustomImportReadService(authorizer=transport._TransportAuthorizer(verified, pinned_target))
    authorization = ExtensionReadAuthorization(verified.credential)
    query = _provider_relation_query(parsed)
    prepared = await service.prepare_npi_entity_relation(
        session,
        authorization=authorization,
        target=pinned_target,
        query=query,
    )
    context = ProviderImportQuery(prepared, compile_npi_entity_relation(prepared.statement), query.require_match)
    binding = _GeoCursorBinding(
        session,
        pinned_target,
        context,
        parsed.native_args,
        cursor_secret,
        int(transport._canonical_utc(trusted_now)[1].timestamp()),
    )
    reply = await _geo_page(
        request, native_args=parsed.native_args, import_context=context, prepare_cursor=binding.prepare
    )
    if reply.status != 200 or len(reply.body) > transport._MAX_RESPONSE_BYTES:
        raise CustomImportReadUnavailableError("provider geo response is unavailable")
    geo_payload = _geo_payload(reply.body, int(parsed.native_args.get("limit", "5")))
    geo_payload["next_cursor"] = binding.next_cursor(geo_payload.pop("_custom_import_next_anchor"))
    await _hydrate_provider_rows(
        session, service, authorization, pinned_target, prepared, parsed, query, geo_payload["items"]
    )
    await verify_published_generation(session, pinned_target)
    return geo_payload


def _geo_payload(body: bytes, limit: int) -> dict[str, Any]:
    """Require the exact native page envelope before hydrating unique NPIs."""

    geo_payload = json.loads(body)
    if (
        type(geo_payload) is not dict
        or set(geo_payload)
        != {"items", "total_count", "next_cursor", "has_more", "result_identity", "_custom_import_next_anchor"}
        or type(geo_payload["items"]) is not list
        or not 0 <= len(geo_payload["items"]) <= limit
        or type(geo_payload["total_count"]) is not int
        or geo_payload["total_count"] < len(geo_payload["items"])
        or type(geo_payload["has_more"]) is not bool
        or geo_payload["result_identity"] != ["npi", "address_key"]
        or geo_payload["next_cursor"] is not None
        or any(
            type(provider) is not dict or type(provider.get("npi")) not in {int, str}
            for provider in geo_payload["items"]
        )
    ):
        raise CustomImportReadUnavailableError("provider geo response is unavailable")
    try:
        _validate_npi_page(tuple(dict.fromkeys(str(provider["npi"]) for provider in geo_payload["items"])))
    except CustomImportReadRequestError:
        raise CustomImportReadUnavailableError("provider geo response is unavailable") from None
    anchor = geo_payload["_custom_import_next_anchor"]
    if geo_payload["has_more"]:
        if (
            not geo_payload["items"]
            or type(anchor) is not list
            or len(anchor) != 2
            or any(type(anchor_part) is not str for anchor_part in anchor)
            or anchor[0] != str(geo_payload["items"][-1]["npi"])
            or ("address_key" in geo_payload["items"][-1] and anchor[1] != geo_payload["items"][-1]["address_key"])
        ):
            raise CustomImportReadUnavailableError("provider geo continuation is unavailable")
    elif anchor is not None:
        raise CustomImportReadUnavailableError("provider geo continuation is unavailable")
    return geo_payload


async def _geo_page(request: Any, **kwargs: Any):
    """Load the native geo handler only after signed authorization."""

    from api.endpoint.npi import get_near_npi

    return await get_near_npi(request, **kwargs)
