# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded FHIR R4 client; pages stay sequential within each alias cursor."""

from __future__ import annotations

import asyncio
import datetime as dt
import email.utils
from collections.abc import AsyncIterator, Mapping, Sequence
from typing import Any

import aiohttp
import orjson

from process.formulary_fhir.continuation import FHIRTransportError
from process.formulary_fhir.continuation import MEDICATION_PAGE_COUNT
from process.formulary_fhir.continuation import _single_alias
from process.formulary_fhir.continuation import validated_next_url as _validated_next_url
from process.formulary_fhir.identity import canonical_fhir_base


KAISER_FHIR_BASE = (
    "https://kpx-service-bus.kp.org/service/hp/mhpo/healthplanproviderv1rc"
)
LIST_PAGE_COUNT = 100
MAX_RESPONSE_BYTES = 20 * 1024 * 1024
USDF_PROFILE_BASE = (
    "http://hl7.org/fhir/us/davinci-drug-formulary/StructureDefinition"
)
COVERAGE_PLAN_PROFILE = f"{USDF_PROFILE_BASE}/usdf-CoveragePlan"
FORMULARY_DRUG_PROFILE = f"{USDF_PROFILE_BASE}/usdf-FormularyDrug"
COVERAGE_PLAN_ELEMENTS = "id,meta,status,title,date,identifier,extension"
FORMULARY_DRUG_ELEMENTS = "id,meta,status,code,extension"
TRANSIENT_STATUSES = frozenset({408, 425, 429, 500, 502, 503, 504})


def _iso(value: dt.datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("FHIR cutoffs must be timezone-aware")
    return value.astimezone(dt.UTC).isoformat().replace("+00:00", "Z")


def _parse_retry_after(value: str | None) -> float:
    raw = str(value or "").strip()
    if not raw:
        return 0.0
    try:
        return max(0.0, min(float(raw), 300.0))
    except ValueError:
        try:
            retry_at = email.utils.parsedate_to_datetime(raw)
        except (TypeError, ValueError):
            return 0.0
        if retry_at.tzinfo is None:
            retry_at = retry_at.replace(tzinfo=dt.UTC)
        return max(
            0.0,
            min((retry_at - dt.datetime.now(dt.UTC)).total_seconds(), 300.0),
        )


def _bundle_next(bundle: Mapping[str, Any]) -> str | None:
    links = bundle.get("link")
    if not isinstance(links, list):
        return None
    candidates = [
        item.get("url")
        for item in links
        if isinstance(item, Mapping) and item.get("relation") == "next"
    ]
    candidates = [value for value in candidates if isinstance(value, str) and value]
    if len(candidates) > 1:
        raise FHIRTransportError("FHIR bundle contains multiple next links")
    return candidates[0] if candidates else None


def _bundle_resources(bundle: Mapping[str, Any], expected_type: str) -> tuple[dict[str, Any], ...]:
    entries = bundle.get("entry")
    if not isinstance(entries, list):
        return ()
    resources: list[dict[str, Any]] = []
    for entry in entries:
        resource = entry.get("resource") if isinstance(entry, Mapping) else None
        if isinstance(resource, dict) and resource.get("resourceType") == expected_type:
            resources.append(resource)
    return tuple(resources)


class FHIRFormularyClient:
    def __init__(
        self,
        *,
        base_url: str = KAISER_FHIR_BASE,
        session: aiohttp.ClientSession | None = None,
        timeout_seconds: int = 60,
        max_attempts: int = 3,
    ) -> None:
        self.base_url = canonical_fhir_base(base_url)
        self._session = session
        self._owns_session = session is None
        self.timeout_seconds = max(1, timeout_seconds)
        self.max_attempts = max(1, min(max_attempts, 5))
        self.request_count = 0
        self.transient_retry_count = 0
        self.throttle_count = 0

    async def __aenter__(self):
        if self._session is None:
            connector = aiohttp.TCPConnector(
                limit=16,
                limit_per_host=16,
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
        return self

    async def __aexit__(self, _exc_type, _exc, _traceback):
        if self._owns_session and self._session is not None:
            await self._session.close()
        self._session = None

    async def _read_json(self, response) -> dict[str, Any]:
        body = bytearray()
        async for chunk in response.content.iter_chunked(64 * 1024):
            body.extend(chunk)
            if len(body) > MAX_RESPONSE_BYTES:
                raise FHIRTransportError("FHIR response exceeds bounded body size")
        try:
            payload = orjson.loads(body)
        except orjson.JSONDecodeError as exc:
            raise FHIRTransportError("FHIR response is not valid JSON") from exc
        if not isinstance(payload, dict):
            raise FHIRTransportError("FHIR response must be a JSON object")
        return payload

    async def _request_once(
        self,
        url: str,
        *,
        params: Mapping[str, str] | Sequence[tuple[str, str]] | None = None,
    ) -> dict[str, Any]:
        if self._session is None:
            raise RuntimeError("FHIRFormularyClient must be used as an async context manager")
        async with self._session.get(
            url,
            params=params,
            timeout=aiohttp.ClientTimeout(total=self.timeout_seconds),
            allow_redirects=False,
        ) as response:
            if 300 <= response.status < 400:
                raise FHIRTransportError("FHIR redirects are not allowed")
            if response.status in TRANSIENT_STATUSES:
                is_throttled = response.status == 429
                self.throttle_count += int(is_throttled)
                raise FHIRTransportError(
                    f"FHIR transient HTTP {response.status}",
                    throttled=is_throttled,
                    retryable=True,
                    retry_after=_parse_retry_after(
                        response.headers.get("Retry-After")
                    ),
                )
            if response.status != 200:
                raise FHIRTransportError(f"FHIR HTTP {response.status}")
            return await self._read_json(response)

    async def _request_json(
        self,
        url: str,
        *,
        params: Mapping[str, str] | Sequence[tuple[str, str]] | None = None,
    ) -> dict[str, Any]:
        for attempt_number in range(1, self.max_attempts + 1):
            self.request_count += 1
            try:
                return await self._request_once(url, params=params)
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                transport_error = FHIRTransportError(
                    "FHIR transport failed",
                    retryable=True,
                )
                transport_error.__cause__ = exc
            except FHIRTransportError as exc:
                if not exc.retryable:
                    raise
                transport_error = exc
            if attempt_number >= self.max_attempts:
                raise FHIRTransportError(
                    "FHIR transport exhausted retries",
                    throttled=transport_error.throttled,
                    retryable=True,
                ) from transport_error
            self.transient_retry_count += 1
            retry_delay = transport_error.retry_after or min(
                2 ** (attempt_number - 1),
                10,
            )
            await asyncio.sleep(retry_delay)
        raise AssertionError("unreachable")

    async def _pages(
        self,
        resource_type: str,
        params: Mapping[str, str] | Sequence[tuple[str, str]],
    ) -> AsyncIterator[dict[str, Any]]:
        param_items = (
            tuple(params.items())
            if isinstance(params, Mapping)
            else tuple(params)
        )
        alias_values = [
            alias_value
            for param_name, alias_value in param_items
            if param_name == "DrugPlan"
        ]
        expected_alias = (
            _single_alias(alias_values[0])
            if resource_type == "MedicationKnowledge"
            and len(alias_values) == 1
            else None
        )
        current_url = f"{self.base_url}/{resource_type}"
        current_params: Mapping[str, str] | None = params
        while current_url:
            bundle = await self._request_json(current_url, params=current_params)
            current_params = None
            if bundle.get("resourceType") != "Bundle":
                raise FHIRTransportError("FHIR search did not return a Bundle")
            yield bundle
            next_url = _bundle_next(bundle)
            current_url = (
                _validated_next_url(
                    self.base_url,
                    current_url,
                    next_url,
                    resource_type=resource_type,
                    expected_alias=expected_alias,
                )
                if next_url
                else ""
            )

    async def coverage_plans(self, *, cutoff: dt.datetime) -> AsyncIterator[dict[str, Any]]:
        """Yield the complete projected CoveragePlan List snapshot."""

        search_params_by_name = {
            "_count": str(LIST_PAGE_COUNT),
            "_lastUpdated": f"lt{_iso(cutoff)}",
            "_profile": COVERAGE_PLAN_PROFILE,
            "_elements": COVERAGE_PLAN_ELEMENTS,
        }
        async for bundle in self._pages("List", search_params_by_name):
            for resource in _bundle_resources(bundle, "List"):
                yield resource

    async def coverage_plan_count(self, *, cutoff: dt.datetime) -> int:
        """Return the exact CoveragePlan List census before the cutoff."""

        search_params_by_name = {
            "_summary": "count",
            "_lastUpdated": f"lt{_iso(cutoff)}",
            "_profile": COVERAGE_PLAN_PROFILE,
        }
        bundle = await self._request_json(
            f"{self.base_url}/List",
            params=search_params_by_name,
        )
        total = bundle.get("total")
        if isinstance(total, bool) or not isinstance(total, int) or total < 0:
            raise FHIRTransportError(
                "FHIR List count response has no exact non-negative total"
            )
        return total

    async def alias_count(self, alias: str, *, cutoff: dt.datetime) -> int:
        """Return the exact MedicationKnowledge census for one alias."""

        search_params_by_name = {
            "DrugPlan": _single_alias(alias),
            "_summary": "count",
            "_lastUpdated": f"lt{_iso(cutoff)}",
            "_profile": FORMULARY_DRUG_PROFILE,
        }
        bundle = await self._request_json(
            f"{self.base_url}/MedicationKnowledge",
            params=search_params_by_name,
        )
        total = bundle.get("total")
        if isinstance(total, bool) or not isinstance(total, int) or total < 0:
            raise FHIRTransportError("FHIR count response has no exact non-negative total")
        return total

    async def medications(
        self,
        alias: str,
        *,
        cutoff: dt.datetime,
        updated_since: dt.datetime | None = None,
    ) -> AsyncIterator[dict[str, Any]]:
        """Yield projected MedicationKnowledge resources for one alias."""

        last_updated_filters = [("_lastUpdated", f"lt{_iso(cutoff)}")]
        if updated_since is not None:
            last_updated_filters.insert(
                0,
                ("_lastUpdated", f"ge{_iso(updated_since)}"),
            )
        search_params = [
            ("DrugPlan", _single_alias(alias)),
            ("_count", str(MEDICATION_PAGE_COUNT)),
            *last_updated_filters,
            ("_profile", FORMULARY_DRUG_PROFILE),
            ("_elements", FORMULARY_DRUG_ELEMENTS),
        ]
        async for bundle in self._pages("MedicationKnowledge", search_params):
            for resource in _bundle_resources(bundle, "MedicationKnowledge"):
                yield resource
