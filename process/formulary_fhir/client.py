# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded serial client for FHIR formulary current-version censuses."""

from __future__ import annotations

import asyncio
import datetime as dt
import email.utils
import math
from collections.abc import Mapping, Sequence
from typing import Any

import aiohttp
import orjson

from process.formulary_fhir.continuation import (
    FHIRContinuation,
    FHIRSearchContract,
    FHIRTransportError,
    collection_url,
    count_query_pairs,
    coverage_plan_search_contract,
    medication_search_contract,
    page_query_pairs,
    validated_next_link,
)
from process.formulary_fhir.identity import (
    resource_last_updated,
    validated_fhir_id,
)
from process.formulary_fhir.types import (
    CurrentVersionCensus,
    FormularySourceConfig,
)


TRANSIENT_HTTP_STATUSES = frozenset({408, 425, 429, 500, 502, 503, 504})
FHIR_JSON_MEDIA_TYPES = frozenset({"application/fhir+json", "application/json"})
MAX_RETRY_AFTER_SECONDS = 30.0


def _retry_after_seconds(raw_header: object) -> float:
    if type(raw_header) is not str or not raw_header.strip():
        return 0.0
    header_text = raw_header.strip()
    try:
        parsed_seconds = float(header_text)
        if not math.isfinite(parsed_seconds):
            return 0.0
        return max(0.0, min(parsed_seconds, MAX_RETRY_AFTER_SECONDS))
    except ValueError:
        try:
            retry_at = email.utils.parsedate_to_datetime(header_text)
        except (TypeError, ValueError):
            return 0.0
        if retry_at.tzinfo is None:
            retry_at = retry_at.replace(tzinfo=dt.UTC)
        seconds_until_retry = (
            retry_at - dt.datetime.now(dt.UTC)
        ).total_seconds()
        return max(0.0, min(seconds_until_retry, MAX_RETRY_AFTER_SECONDS))


def _bundle_total(
    bundle: Mapping[str, Any],
    contract: FHIRSearchContract,
) -> int:
    if bundle.get("resourceType") != "Bundle" or bundle.get("type") != "searchset":
        raise FHIRTransportError("FHIR search response is not a searchset Bundle")
    exact_total = bundle.get("total")
    if type(exact_total) is not int or not 0 <= exact_total <= contract.max_total_resources:
        raise FHIRTransportError("FHIR search response total is invalid")
    return exact_total


def _bundle_next_link(bundle: Mapping[str, Any]) -> str | None:
    raw_links = bundle.get("link", [])
    if type(raw_links) is not list:
        raise FHIRTransportError("FHIR Bundle links are invalid")
    next_links: list[str] = []
    for raw_link in raw_links:
        if type(raw_link) is not dict:
            raise FHIRTransportError("FHIR Bundle link is invalid")
        relation = raw_link.get("relation")
        url = raw_link.get("url")
        if type(relation) is not str or type(url) is not str:
            raise FHIRTransportError("FHIR Bundle link primitives are invalid")
        if relation == "next":
            next_links.append(url)
    if len(next_links) > 1:
        raise FHIRTransportError("FHIR Bundle has multiple next links")
    return next_links[0] if next_links else None


def _bundle_resources(
    bundle: Mapping[str, Any],
    contract: FHIRSearchContract,
) -> tuple[dict[str, Any], ...]:
    raw_entries = bundle.get("entry", [])
    if type(raw_entries) is not list or len(raw_entries) > contract.page_size:
        raise FHIRTransportError("FHIR Bundle page size is invalid")
    resources: list[dict[str, Any]] = []
    for raw_entry in raw_entries:
        if type(raw_entry) is not dict or type(raw_entry.get("resource")) is not dict:
            raise FHIRTransportError("FHIR Bundle entry is invalid")
        resource = raw_entry["resource"]
        if resource.get("resourceType") != contract.resource_type:
            raise FHIRTransportError("FHIR Bundle resource type is invalid")
        try:
            validated_fhir_id(resource.get("id"), label="resource id")
            last_updated = resource_last_updated(resource)
        except ValueError:
            raise FHIRTransportError("FHIR Bundle resource primitives are invalid") from None
        if last_updated >= contract.cutoff_at:
            raise FHIRTransportError("FHIR resource violates the census cutoff")
        resources.append(resource)
    return tuple(resources)


def _validate_count_bundle(
    bundle: Mapping[str, Any],
    contract: FHIRSearchContract,
) -> int:
    exact_total = _bundle_total(bundle, contract)
    raw_entries = bundle.get("entry", [])
    if raw_entries not in (None, []) or _bundle_next_link(bundle) is not None:
        raise FHIRTransportError("FHIR count response contains search results")
    return exact_total


class FHIRFormularyClient:
    """Execute one request at a time under explicit source bounds."""

    def __init__(
        self,
        config: FormularySourceConfig,
        *,
        session: aiohttp.ClientSession | None = None,
    ) -> None:
        if type(config) is not FormularySourceConfig or config.is_enabled is not True:
            raise ValueError("FHIR formulary client requires an enabled source config")
        self.config = config
        self._session = session
        self._owns_session = session is None
        self._is_entered = False
        self._request_gate = asyncio.Lock()
        self.request_count = 0
        self.transient_retry_count = 0
        self.throttle_count = 0

    async def __aenter__(self) -> FHIRFormularyClient:
        if self._is_entered:
            raise RuntimeError("FHIRFormularyClient is already entered")
        if self._session is None:
            connector = aiohttp.TCPConnector(
                limit=1,
                limit_per_host=1,
                ttl_dns_cache=300,
            )
            self._session = aiohttp.ClientSession(
                connector=connector,
                auto_decompress=True,
                headers={
                    "Accept": "application/fhir+json, application/json;q=0.9",
                    "Accept-Encoding": "gzip",
                },
            )
        self._is_entered = True
        return self

    async def __aexit__(self, _error_type, _error, _traceback) -> None:
        try:
            if self._owns_session and self._session is not None:
                await self._session.close()
        finally:
            if self._owns_session:
                self._session = None
            self._is_entered = False

    async def _read_json_object(self, response: Any) -> dict[str, Any]:
        content_type = str(response.headers.get("Content-Type") or "")
        media_type = content_type.split(";", 1)[0].strip().lower()
        if media_type not in FHIR_JSON_MEDIA_TYPES:
            raise FHIRTransportError("FHIR response media type is invalid")
        response_body = bytearray()
        async for response_chunk in response.content.iter_chunked(64 * 1_024):
            response_body.extend(response_chunk)
            if len(response_body) > self.config.max_response_bytes:
                raise FHIRTransportError("FHIR response exceeds the byte bound")
        try:
            response_object = orjson.loads(response_body)
        except orjson.JSONDecodeError:
            raise FHIRTransportError("FHIR response is not valid JSON") from None
        if type(response_object) is not dict:
            raise FHIRTransportError("FHIR response must be a JSON object")
        return response_object

    async def _request_once(
        self,
        request_url: str,
        *,
        query_pairs: Sequence[tuple[str, str]] | None,
    ) -> dict[str, Any]:
        if not self._is_entered or self._session is None:
            raise RuntimeError("FHIRFormularyClient must be entered before use")
        timeout = aiohttp.ClientTimeout(total=self.config.timeout_seconds)
        async with self._session.get(
            request_url,
            params=query_pairs,
            timeout=timeout,
            allow_redirects=False,
        ) as response:
            if 300 <= response.status < 400:
                raise FHIRTransportError("FHIR redirects are not allowed")
            if response.status in TRANSIENT_HTTP_STATUSES:
                is_throttled = response.status == 429
                self.throttle_count += int(is_throttled)
                raise FHIRTransportError(
                    "FHIR endpoint returned a transient status",
                    is_transient=True,
                    retry_after_seconds=_retry_after_seconds(
                        response.headers.get("Retry-After")
                    ),
                )
            if response.status != 200:
                raise FHIRTransportError("FHIR endpoint returned a terminal status")
            return await self._read_json_object(response)

    async def _request_json(
        self,
        request_url: str,
        *,
        query_pairs: Sequence[tuple[str, str]] | None = None,
    ) -> dict[str, Any]:
        async with self._request_gate:
            for attempt_number in range(1, self.config.max_attempts + 1):
                self.request_count += 1
                try:
                    return await self._request_once(
                        request_url,
                        query_pairs=query_pairs,
                    )
                except (aiohttp.ClientError, asyncio.TimeoutError, TimeoutError):
                    transport_error = FHIRTransportError(
                        "FHIR transport failed",
                        is_transient=True,
                    )
                except FHIRTransportError as caught_error:
                    if not caught_error.is_transient:
                        raise
                    transport_error = caught_error
                if attempt_number >= self.config.max_attempts:
                    raise FHIRTransportError(
                        "FHIR transport exhausted bounded retries",
                        is_transient=True,
                    ) from None
                self.transient_retry_count += 1
                retry_delay = transport_error.retry_after_seconds or min(
                    2 ** (attempt_number - 1),
                    5,
                )
                await asyncio.sleep(retry_delay)
        raise AssertionError("unreachable")

    async def _exact_current_total(self, contract: FHIRSearchContract) -> int:
        count_bundle = await self._request_json(
            collection_url(contract),
            query_pairs=count_query_pairs(contract),
        )
        return _validate_count_bundle(count_bundle, contract)

    async def _current_version_census(
        self,
        contract: FHIRSearchContract,
    ) -> CurrentVersionCensus:
        """Collect versions currently matching one cutoff predicate, not history."""

        expected_total = await self._exact_current_total(contract)
        resources = await self._collect_current_resources(
            contract,
            expected_total=expected_total,
        )
        final_total = await self._exact_current_total(contract)
        if final_total != expected_total:
            raise FHIRTransportError("FHIR current-version census changed during traversal")
        return CurrentVersionCensus(
            resource_type=contract.resource_type,
            cutoff_at=contract.cutoff_at,
            exact_total=expected_total,
            resources=resources,
            search_contract_hash=contract.contract_hash,
        )

    async def _collect_current_resources(
        self,
        contract: FHIRSearchContract,
        *,
        expected_total: int,
    ) -> tuple[dict[str, Any], ...]:
        collected_resources: list[dict[str, Any]] = []
        seen_resource_ids: set[str] = set()
        seen_continuations: set[str] = set()
        continuation: FHIRContinuation | None = None
        page_number = 0
        while True:
            page_number += 1
            if page_number > contract.max_pages:
                raise FHIRTransportError("FHIR search exceeds the page bound")
            page_bundle = await self._request_page(contract, continuation)
            if _bundle_total(page_bundle, contract) != expected_total:
                raise FHIRTransportError("FHIR search total changed during traversal")
            page_resources = _bundle_resources(page_bundle, contract)
            _append_unique_resources(
                collected_resources,
                seen_resource_ids,
                page_resources,
                expected_total=expected_total,
            )
            next_link = _bundle_next_link(page_bundle)
            if not page_resources and next_link is not None:
                raise FHIRTransportError("FHIR search returned an empty intermediate page")
            if next_link is None:
                break
            if len(collected_resources) >= expected_total:
                raise FHIRTransportError("FHIR search continued beyond the exact total")
            continuation = validated_next_link(next_link, contract=contract)
            if continuation.url_fingerprint in seen_continuations:
                raise FHIRTransportError("FHIR continuation cycle detected")
            seen_continuations.add(continuation.url_fingerprint)
        if len(collected_resources) != expected_total:
            raise FHIRTransportError("FHIR search did not match the exact total")
        return tuple(collected_resources)

    async def _request_page(
        self,
        contract: FHIRSearchContract,
        continuation: FHIRContinuation | None,
    ) -> dict[str, Any]:
        if continuation is None:
            return await self._request_json(
                collection_url(contract),
                query_pairs=page_query_pairs(contract),
            )
        if continuation.search_contract_hash != contract.contract_hash:
            raise FHIRTransportError("FHIR continuation search binding is invalid")
        return await self._request_json(continuation.request_url)

    async def coverage_plan_current_census(
        self,
        *,
        cutoff: object,
    ) -> CurrentVersionCensus:
        """Collect the bounded current CoveragePlan versions before a cutoff."""

        contract = coverage_plan_search_contract(self.config, cutoff)
        return await self._current_version_census(contract)

    async def medication_current_census(
        self,
        alias: object,
        *,
        cutoff: object,
    ) -> CurrentVersionCensus:
        """Collect one bounded alias's current FormularyDrug versions."""

        contract = medication_search_contract(self.config, alias, cutoff)
        return await self._current_version_census(contract)


def _append_unique_resources(
    collected_resources: list[dict[str, Any]],
    seen_resource_ids: set[str],
    page_resources: tuple[dict[str, Any], ...],
    *,
    expected_total: int,
) -> None:
    for resource in page_resources:
        resource_id = resource["id"]
        if resource_id in seen_resource_ids:
            raise FHIRTransportError("FHIR search returned a duplicate resource id")
        seen_resource_ids.add(resource_id)
        collected_resources.append(resource)
        if len(collected_resources) > expected_total:
            raise FHIRTransportError("FHIR search exceeded the exact total")
