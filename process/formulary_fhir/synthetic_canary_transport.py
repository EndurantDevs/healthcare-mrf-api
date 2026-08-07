# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Socket-free exact HTTP session for the synthetic formulary canary."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

import aiohttp
import orjson

from process.formulary_fhir.client import FHIRFormularyClient
from process.formulary_fhir.synthetic_canary_contract import CANARY_SOURCE_BASE
from process.formulary_fhir.synthetic_canary_contract import (
    SyntheticCanaryContractError,
)
from process.formulary_fhir.synthetic_canary_contract import fixture_object
from process.formulary_fhir.types import FormularySourceConfig


LAST_UPDATED_PAIR = ("_lastUpdated", "lt2026-08-06T00:00:00Z")
COVERAGE_PROFILE_PAIR = (
    "_profile",
    "http://hl7.org/fhir/us/davinci-drug-formulary/StructureDefinition/"
    "usdf-CoveragePlan",
)
MEDICATION_PROFILE_PAIR = (
    "_profile",
    "http://hl7.org/fhir/us/davinci-drug-formulary/StructureDefinition/"
    "usdf-FormularyDrug",
)
ACCURATE_TOTAL_PAIR = ("_total", "accurate")
COUNT_SUMMARY_PAIR = ("_summary", "count")
COVERAGE_ELEMENTS_PAIR = (
    "_elements",
    "id,meta,status,title,name,date,identifier,extension",
)
MEDICATION_ELEMENTS_PAIR = (
    "_elements",
    "id,meta,status,code,extension",
)


def _count_bundle() -> dict[str, object]:
    return {"resourceType": "Bundle", "type": "searchset", "total": 1}


def _page_bundle(resource_by_field: dict[str, Any]) -> dict[str, object]:
    return {
        "resourceType": "Bundle",
        "type": "searchset",
        "total": 1,
        "entry": [{"resource": resource_by_field}],
    }


class _Content:
    def __init__(self, response_by_field: dict[str, object]) -> None:
        self._response_bytes = orjson.dumps(response_by_field)

    async def iter_chunked(self, chunk_size: int):
        """Yield exact fixture bytes through the production streaming contract."""

        for offset in range(0, len(self._response_bytes), chunk_size):
            await asyncio.sleep(0)
            yield self._response_bytes[offset : offset + chunk_size]


class _Response:
    status = 200
    headers = {"Content-Type": "application/fhir+json; charset=utf-8"}

    def __init__(self, response_by_field: dict[str, object]) -> None:
        self.content = _Content(response_by_field)


@dataclass(frozen=True, slots=True)
class _ExpectedRequest:
    url: str
    query_pairs: tuple[tuple[str, str], ...]
    response_by_field: dict[str, object]


class _RequestContext:
    def __init__(self, response_by_field: dict[str, object]) -> None:
        self._response = _Response(response_by_field)

    async def __aenter__(self) -> _Response:
        await asyncio.sleep(0)
        return self._response

    async def __aexit__(self, *_error_details: object) -> None:
        return None


def _fixed_requests(
    request_url: str,
    count_pairs: tuple[tuple[str, str], ...],
    page_pairs: tuple[tuple[str, str], ...],
    resource_by_field: dict[str, Any],
) -> tuple[_ExpectedRequest, ...]:
    count_request = _ExpectedRequest(
        request_url,
        count_pairs,
        _count_bundle(),
    )
    page_request = _ExpectedRequest(
        request_url,
        page_pairs,
        _page_bundle(resource_by_field),
    )
    return (count_request, page_request, count_request)


def _coverage_requests() -> tuple[_ExpectedRequest, ...]:
    return _fixed_requests(
        f"{CANARY_SOURCE_BASE}/List",
        (
            LAST_UPDATED_PAIR,
            COVERAGE_PROFILE_PAIR,
            ACCURATE_TOTAL_PAIR,
            COUNT_SUMMARY_PAIR,
        ),
        (
            LAST_UPDATED_PAIR,
            COVERAGE_PROFILE_PAIR,
            ACCURATE_TOTAL_PAIR,
            ("_count", "10"),
            COVERAGE_ELEMENTS_PAIR,
        ),
        fixture_object("coverage_plan.json"),
    )


def _medication_requests() -> tuple[_ExpectedRequest, ...]:
    return tuple(
        request
        for alias, file_name in (
            ("SYNTH-A", "medication_a.json"),
            ("SYNTH-B", "medication_b.json"),
        )
        for request in _fixed_requests(
            f"{CANARY_SOURCE_BASE}/MedicationKnowledge",
            (
                ("DrugPlan", alias),
                LAST_UPDATED_PAIR,
                MEDICATION_PROFILE_PAIR,
                ACCURATE_TOTAL_PAIR,
                COUNT_SUMMARY_PAIR,
            ),
            (
                ("DrugPlan", alias),
                LAST_UPDATED_PAIR,
                MEDICATION_PROFILE_PAIR,
                ACCURATE_TOTAL_PAIR,
                ("_count", "10"),
                MEDICATION_ELEMENTS_PAIR,
            ),
            fixture_object(file_name),
        )
    )


def _expected_requests() -> tuple[_ExpectedRequest, ...]:
    return _coverage_requests() + _medication_requests()


class SyntheticCanarySession:
    """Validate every client request and return only packaged Bundle bytes."""

    def __init__(self, config: FormularySourceConfig) -> None:
        self._config = config
        requests = _expected_requests()
        self._request_groups = (
            requests[0:3],
            requests[3:6],
            requests[6:9],
        )
        self._group_index = 0
        self._request_index = 0
        self.call_count = 0

    def _is_exact_request(
        self,
        expected_request: _ExpectedRequest,
        request_url: str,
        request_options: dict[str, object],
    ) -> bool:
        timeout = request_options.get("timeout")
        params = request_options.get("params")
        return (
            request_url == expected_request.url
            and set(request_options) == {"params", "timeout", "allow_redirects"}
            and type(params) is tuple
            and params == expected_request.query_pairs
            and type(timeout) is aiohttp.ClientTimeout
            and timeout.total == self._config.timeout_seconds
            and request_options.get("allow_redirects") is False
        )

    def _select_group(
        self,
        request_url: str,
        request_options: dict[str, object],
    ) -> None:
        if self._group_index == 0 or self._request_index != 0:
            return
        matching_group = next(
            (
                group_index
                for group_index in range(
                    self._group_index,
                    len(self._request_groups),
                )
                if self._is_exact_request(
                    self._request_groups[group_index][0],
                    request_url,
                    request_options,
                )
            ),
            None,
        )
        if matching_group is None:
            raise SyntheticCanaryContractError(
                "synthetic canary request contract is invalid"
            )
        self._group_index = matching_group

    def get(self, request_url: str, **request_options: object) -> _RequestContext:
        """Validate and answer one expected count, page, or recount request."""

        if self._group_index >= len(self._request_groups):
            raise SyntheticCanaryContractError(
                "synthetic canary request count is invalid"
            )
        self._select_group(request_url, request_options)
        expected_request = self._request_groups[self._group_index][
            self._request_index
        ]
        if not self._is_exact_request(
            expected_request,
            request_url,
            request_options,
        ):
            raise SyntheticCanaryContractError(
                "synthetic canary request contract is invalid"
            )
        self.call_count += 1
        self._request_index += 1
        if self._request_index == len(self._request_groups[self._group_index]):
            self._group_index += 1
            self._request_index = 0
        return _RequestContext(expected_request.response_by_field)

    def require_valid_stop(self) -> None:
        """Require a complete List pass and only complete alias request groups."""

        if (
            self.call_count not in {3, 6, 9}
            or self._group_index not in {1, 2, 3}
            or self._request_index != 0
        ):
            raise SyntheticCanaryContractError(
                "synthetic canary request sequence is incomplete"
            )


class SyntheticCanaryClient(FHIRFormularyClient):
    """Run the production client against the socket-free exact session."""

    def __init__(self, config: FormularySourceConfig) -> None:
        self.synthetic_session = SyntheticCanarySession(config)
        super().__init__(config, session=self.synthetic_session)

    async def __aexit__(self, error_type, error, traceback) -> None:
        await super().__aexit__(error_type, error, traceback)
        if error is None:
            self.synthetic_session.require_valid_stop()


__all__ = ("SyntheticCanaryClient", "SyntheticCanarySession")
