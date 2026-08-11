# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Strict bounded HTTP for one claimed rooted-graph FHIR query."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
import hashlib
from typing import Any

import aiohttp
from yarl import URL

from process.provider_directory_rooted_graph_identity import (
    canonical_fhir_resource_id,
)
from process.provider_directory_rooted_graph_http_transport import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_ACCEPT,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_PAGE_BYTES,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_PAGES,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_QUERY_BYTES,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_RESOURCES,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_RETRY_AFTER_SECONDS,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_URL_BYTES,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_TIMEOUT_SECONDS,
    ProviderDirectoryRootedGraphHTTPBounds,
    ProviderDirectoryRootedGraphHTTPError,
    ProviderDirectoryRootedGraphHTTPResult,
    _read_body,
    _request_url_identity,
    _require_response_url,
    _require_success_status,
    _strict_json_payload,
    _timeout,
    _url_byte_length,
    _validate_headers,
    _validated_next_url,
    provider_directory_rooted_graph_retry_after_seconds,
)
from process.provider_directory_rooted_graph_query import (
    ROOTED_GRAPH_QUERY_DIRECT_READ,
    ROOTED_GRAPH_QUERY_EXACT_SEARCH,
    ROOTED_GRAPH_QUERY_INSURANCE_PLAN_CENSUS,
    ProviderDirectoryRootedGraphQuery,
    ProviderDirectoryRootedGraphQueryError,
    build_insurance_plan_census_query,
    build_provider_directory_organization_affiliation_query,
    build_provider_directory_practitioner_role_query,
    build_rooted_graph_direct_read,
)
from process.provider_directory_rooted_graph_references import (
    ProviderDirectoryRootedGraphReferenceError,
    provider_directory_rooted_graph_resource_references,
)
from process.provider_directory_rooted_graph_store_contract import (
    ProviderDirectoryRootedGraphWorkClaim,
    _canonical_json,
    _sha256_text,
)
from process.provider_directory_rooted_graph_terminal import (
    validate_rooted_graph_missing_outcome_payload,
)


def _query_for_claim(
    api_base: str,
    claim: ProviderDirectoryRootedGraphWorkClaim,
) -> ProviderDirectoryRootedGraphQuery:
    if claim.kind == ROOTED_GRAPH_QUERY_EXACT_SEARCH:
        if claim.resource_type == "PractitionerRole":
            return build_provider_directory_practitioner_role_query(
                api_base,
                claim.reference_id,
            )
        if claim.resource_type == "OrganizationAffiliation":
            return build_provider_directory_organization_affiliation_query(
                api_base,
                claim.reference_id,
            )
    if claim.kind == ROOTED_GRAPH_QUERY_DIRECT_READ:
        return build_rooted_graph_direct_read(
            api_base=api_base,
            resource_type=claim.resource_type,
            resource_id=claim.reference_id,
        )
    if claim.kind == ROOTED_GRAPH_QUERY_INSURANCE_PLAN_CENSUS:
        return build_insurance_plan_census_query(api_base)
    raise ProviderDirectoryRootedGraphHTTPError("claim_rebound_invalid")


def rebind_provider_directory_rooted_graph_query(
    api_base: str,
    claim: ProviderDirectoryRootedGraphWorkClaim,
) -> ProviderDirectoryRootedGraphQuery:
    """Rebuild and hash-check the exact endpoint URL for one durable claim."""

    if type(claim) is not ProviderDirectoryRootedGraphWorkClaim:
        raise ProviderDirectoryRootedGraphHTTPError("claim_rebound_invalid")
    try:
        query = _query_for_claim(api_base, claim)
        identity_json = _canonical_json(query.identity_document())
    except (ProviderDirectoryRootedGraphQueryError, TypeError, ValueError):
        raise ProviderDirectoryRootedGraphHTTPError("claim_rebound_invalid") from None
    if (
        query.query_id(claim.scope_id) != claim.query_id
        or _sha256_text(identity_json) != claim.query_identity_sha256
    ):
        raise ProviderDirectoryRootedGraphHTTPError("claim_rebound_invalid")
    return query


def _bundle_entries(payload: dict[str, Any]) -> list[dict[str, Any]]:
    entries = payload.get("entry", [])
    if type(entries) is not list:
        raise ProviderDirectoryRootedGraphHTTPError("response_invalid")
    resources = []
    for entry in entries:
        if type(entry) is not dict or type(entry.get("resource")) is not dict:
            raise ProviderDirectoryRootedGraphHTTPError("response_invalid")
        search = entry.get("search")
        if type(search) is not dict or search.get("mode") != "match":
            raise ProviderDirectoryRootedGraphHTTPError("response_invalid")
        resources.append(entry["resource"])
    return resources


def _bundle_next_link(payload: dict[str, Any]) -> str | None:
    links = payload.get("link", [])
    if type(links) is not list:
        raise ProviderDirectoryRootedGraphHTTPError("response_invalid")
    next_links = []
    for link in links:
        if type(link) is not dict or type(link.get("relation")) is not str:
            raise ProviderDirectoryRootedGraphHTTPError("response_invalid")
        if link["relation"] == "next":
            next_links.append(link.get("url"))
    if len(next_links) > 1 or (next_links and type(next_links[0]) is not str):
        raise ProviderDirectoryRootedGraphHTTPError("response_invalid")
    return next_links[0] if next_links else None


def _validate_search_resource(
    resource: dict[str, Any],
    claim: ProviderDirectoryRootedGraphWorkClaim,
) -> str:
    if resource.get("resourceType") != claim.resource_type:
        raise ProviderDirectoryRootedGraphHTTPError("response_invalid")
    try:
        resource_id = canonical_fhir_resource_id(resource.get("id"))
        provider_directory_rooted_graph_resource_references(resource)
    except (ProviderDirectoryRootedGraphReferenceError, ValueError):
        raise ProviderDirectoryRootedGraphHTTPError("response_invalid") from None
    if claim.kind == ROOTED_GRAPH_QUERY_EXACT_SEARCH:
        reference_field = {
            "PractitionerRole": "practitioner",
            "OrganizationAffiliation": "participatingOrganization",
        }[claim.resource_type]
        reference = resource.get(reference_field)
        if (
            type(reference) is not dict
            or reference.get("reference")
            != f"{claim.reference_type}/{claim.reference_id}"
        ):
            raise ProviderDirectoryRootedGraphHTTPError("response_invalid")
    return resource_id


def _validate_bundle(
    payload: dict[str, Any],
    claim: ProviderDirectoryRootedGraphWorkClaim,
) -> tuple[list[dict[str, Any]], int | None, str | None]:
    if payload.get("resourceType") != "Bundle" or payload.get("type") != "searchset":
        raise ProviderDirectoryRootedGraphHTTPError("response_invalid")
    total = payload.get("total")
    if total is not None and (type(total) is not int or total < 0):
        raise ProviderDirectoryRootedGraphHTTPError("response_invalid")
    resources = _bundle_entries(payload)
    for resource in resources:
        _validate_search_resource(resource, claim)
    return resources, total, _bundle_next_link(payload)


def _validate_missing_outcome(payload: dict[str, Any], status: int) -> None:
    try:
        validate_rooted_graph_missing_outcome_payload(payload, status)
    except ValueError:
        raise ProviderDirectoryRootedGraphHTTPError("response_invalid")


async def _request_payload(
    session: Any,
    request_url: str,
    claim: ProviderDirectoryRootedGraphWorkClaim,
    bounds: ProviderDirectoryRootedGraphHTTPBounds,
    query_remaining: int,
) -> tuple[dict[str, Any], bytes, int | None]:
    async with session.get(
        URL(request_url, encoded=True),
        headers={
            "Accept": PROVIDER_DIRECTORY_ROOTED_GRAPH_ACCEPT,
            "Accept-Encoding": "identity",
        },
        timeout=_timeout(bounds),
        allow_redirects=False,
    ) as response:
        _require_response_url(response, request_url)
        missing_status = _require_success_status(response, claim)
        declared = _validate_headers(response)
        body_limit = bounds.max_page_bytes
        if missing_status is not None:
            body_limit = min(body_limit, bounds.max_missing_response_bytes)
        body = await _read_body(
            response,
            declared_length=declared,
            page_limit=body_limit,
            query_remaining=min(query_remaining, body_limit),
        )
    response_by_field = _strict_json_payload(body)
    if missing_status is not None:
        _validate_missing_outcome(response_by_field, missing_status)
    return response_by_field, body, missing_status


def _direct_result(
    claim: ProviderDirectoryRootedGraphWorkClaim,
    payload: dict[str, Any],
    byte_count: int,
) -> ProviderDirectoryRootedGraphHTTPResult:
    resource_id = _validate_search_resource(payload, claim)
    if resource_id != claim.reference_id:
        raise ProviderDirectoryRootedGraphHTTPError("response_invalid")
    return ProviderDirectoryRootedGraphHTTPResult(
        query_id=claim.query_id,
        resources=(payload,),
        advertised_total=None,
        terminal_page_count=1,
        total_bytes=byte_count,
    )


@dataclass(slots=True)
class _SearchState:
    resources: list[dict[str, Any]] = field(default_factory=list)
    seen_resource_ids: set[str] = field(default_factory=set)
    expected_total: int | None = None
    has_page_without_total: bool = False
    total_bytes: int = 0
    page_count: int = 0

    def add_page(
        self,
        page_resources: list[dict[str, Any]],
        page_total: int | None,
        byte_count: int,
        claim: ProviderDirectoryRootedGraphWorkClaim,
        bounds: ProviderDirectoryRootedGraphHTTPBounds,
    ) -> None:
        """Add one finite search page while preserving total and uniqueness."""

        self.total_bytes += byte_count
        if page_total is None:
            self.has_page_without_total = True
            if (
                self.expected_total is not None
                or claim.kind == ROOTED_GRAPH_QUERY_INSURANCE_PLAN_CENSUS
            ):
                raise ProviderDirectoryRootedGraphHTTPError("response_invalid")
        else:
            if self.has_page_without_total or (
                self.expected_total is not None and page_total != self.expected_total
            ):
                raise ProviderDirectoryRootedGraphHTTPError("response_invalid")
            self.expected_total = page_total
            if page_total > bounds.max_resources:
                raise ProviderDirectoryRootedGraphHTTPError("resource_limit")
        for resource_by_field in page_resources:
            resource_id = canonical_fhir_resource_id(resource_by_field["id"])
            if resource_id in self.seen_resource_ids:
                raise ProviderDirectoryRootedGraphHTTPError("response_invalid")
            self.seen_resource_ids.add(resource_id)
            self.resources.append(resource_by_field)
            if len(self.resources) > bounds.max_resources:
                raise ProviderDirectoryRootedGraphHTTPError("resource_limit")

    def build_result(
        self,
        claim: ProviderDirectoryRootedGraphWorkClaim,
    ) -> ProviderDirectoryRootedGraphHTTPResult:
        """Return the completed result only when any total is exact."""

        if self.expected_total is not None and self.expected_total != len(
            self.resources
        ):
            raise ProviderDirectoryRootedGraphHTTPError("response_invalid")
        return ProviderDirectoryRootedGraphHTTPResult(
            query_id=claim.query_id,
            resources=tuple(self.resources),
            advertised_total=self.expected_total,
            terminal_page_count=self.page_count,
            total_bytes=self.total_bytes,
        )


def _register_page_url(
    request_url: str,
    seen_url_identities: set[tuple[object, ...]],
    search_state: _SearchState,
    bounds: ProviderDirectoryRootedGraphHTTPBounds,
) -> None:
    request_identity = _request_url_identity(request_url)
    if request_identity is None or request_identity in seen_url_identities:
        raise ProviderDirectoryRootedGraphHTTPError("pagination_invalid")
    if search_state.page_count >= bounds.max_pages:
        raise ProviderDirectoryRootedGraphHTTPError("page_limit")
    seen_url_identities.add(request_identity)
    search_state.page_count += 1


async def _fetch_query(
    session: Any,
    query: ProviderDirectoryRootedGraphQuery,
    claim: ProviderDirectoryRootedGraphWorkClaim,
    bounds: ProviderDirectoryRootedGraphHTTPBounds,
) -> ProviderDirectoryRootedGraphHTTPResult:
    """Fetch every page of one rebound query under exact finite bounds."""

    request_url = query.url
    if _url_byte_length(request_url) > bounds.max_url_bytes:
        raise ProviderDirectoryRootedGraphHTTPError("request_invalid")
    search_state = _SearchState()
    seen_url_identities: set[tuple[object, ...]] = set()
    while True:
        _register_page_url(request_url, seen_url_identities, search_state, bounds)
        response_by_field, body, missing_status = await _request_payload(
            session,
            request_url,
            claim,
            bounds,
            bounds.max_query_bytes - search_state.total_bytes,
        )
        if missing_status is not None:
            return ProviderDirectoryRootedGraphHTTPResult(
                query_id=claim.query_id,
                resources=(),
                advertised_total=None,
                terminal_page_count=1,
                total_bytes=len(body),
                missing_http_status=missing_status,
                missing_response_sha256=hashlib.sha256(body).hexdigest(),
                missing_response_json_text=body.decode("utf-8"),
            )
        if claim.kind == ROOTED_GRAPH_QUERY_DIRECT_READ:
            return _direct_result(claim, response_by_field, len(body))
        page_resources, page_total, next_link = _validate_bundle(
            response_by_field,
            claim,
        )
        search_state.add_page(
            page_resources,
            page_total,
            len(body),
            claim,
            bounds,
        )
        if next_link is None:
            break
        request_url = _validated_next_url(
            api_base=query.api_base,
            collection_url=query.url,
            current_url=request_url,
            next_link=next_link,
            max_url_bytes=bounds.max_url_bytes,
        )
    return search_state.build_result(claim)


async def fetch_provider_directory_rooted_graph_query(
    session: Any,
    api_base: str,
    claim: ProviderDirectoryRootedGraphWorkClaim,
    *,
    bounds: ProviderDirectoryRootedGraphHTTPBounds = (
        ProviderDirectoryRootedGraphHTTPBounds()
    ),
) -> ProviderDirectoryRootedGraphHTTPResult:
    """Fetch one exact claim; every retry caller starts again from page one."""

    if type(bounds) is not ProviderDirectoryRootedGraphHTTPBounds:
        raise ProviderDirectoryRootedGraphHTTPError("request_invalid")
    try:
        query = rebind_provider_directory_rooted_graph_query(api_base, claim)
        return await _fetch_query(session, query, claim, bounds)
    except asyncio.CancelledError:
        raise
    except ProviderDirectoryRootedGraphHTTPError:
        raise
    except (aiohttp.ClientPayloadError, aiohttp.ServerDisconnectedError, EOFError):
        raise ProviderDirectoryRootedGraphHTTPError(
            "payload_truncated",
            retryable=True,
        ) from None
    except (asyncio.TimeoutError, TimeoutError):
        raise ProviderDirectoryRootedGraphHTTPError(
            "transport_timeout",
            retryable=True,
        ) from None
    except (aiohttp.ClientConnectionError, ConnectionError, OSError):
        raise ProviderDirectoryRootedGraphHTTPError(
            "transport_connection",
            retryable=True,
        ) from None
    except aiohttp.ClientError:
        raise ProviderDirectoryRootedGraphHTTPError("transport_failure") from None
    except (MemoryError, OverflowError, RecursionError, TypeError, ValueError):
        raise ProviderDirectoryRootedGraphHTTPError("response_invalid") from None


__all__ = (
    "fetch_provider_directory_rooted_graph_query",
    "provider_directory_rooted_graph_retry_after_seconds",
    "rebind_provider_directory_rooted_graph_query",
    "ProviderDirectoryRootedGraphHTTPBounds",
    "ProviderDirectoryRootedGraphHTTPError",
    "ProviderDirectoryRootedGraphHTTPResult",
    "PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_PAGE_BYTES",
    "PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_PAGES",
    "PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_QUERY_BYTES",
    "PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_RESOURCES",
    "PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_RETRY_AFTER_SECONDS",
    "PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_URL_BYTES",
    "PROVIDER_DIRECTORY_ROOTED_GRAPH_TIMEOUT_SECONDS",
)
