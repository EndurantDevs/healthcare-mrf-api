# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import datetime as dt

import aiohttp
import orjson
import pytest

from process.formulary_fhir.client import (
    COVERAGE_PLAN_ELEMENTS,
    COVERAGE_PLAN_PROFILE,
    FHIRFormularyClient,
    FHIRTransportError,
    FORMULARY_DRUG_ELEMENTS,
    FORMULARY_DRUG_PROFILE,
    KAISER_FHIR_BASE,
    _validated_next_url,
)


class _Content:
    def __init__(self, payload):
        self.payload = orjson.dumps(payload)

    async def iter_chunked(self, _size):
        yield self.payload


class _Response:
    def __init__(self, payload, status=200, headers=None):
        self.status = status
        self.headers = headers or {}
        self.content = _Content(payload)


class _RequestContext:
    def __init__(self, response):
        self.response = response

    async def __aenter__(self):
        return self.response

    async def __aexit__(self, *_args):
        return None


class _Session:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return _RequestContext(self.responses.pop(0))


class _FailingSession:
    def __init__(self):
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        raise aiohttp.ClientConnectionError("synthetic disconnect")


def _next_url(token_name, token):
    return (
        f"{KAISER_FHIR_BASE}/MedicationKnowledge?"
        f"DrugPlan=SYNTH-A&_count=100&{token_name}={token}"
    )


@pytest.mark.parametrize(
    ("token_name", "token"),
    (("_after", "opaque-token"), ("_offset", "100")),
)
def test_collection_after_and_offset_continuations_are_strictly_accepted(
    token_name,
    token,
):
    current = f"{KAISER_FHIR_BASE}/MedicationKnowledge"

    assert _validated_next_url(
        KAISER_FHIR_BASE,
        current,
        _next_url(token_name, token),
    ).endswith(f"{token_name}={token}")


def test_collection_continuation_cannot_switch_drug_plan_alias():
    current = f"{KAISER_FHIR_BASE}/MedicationKnowledge"
    candidate = (
        f"{current}?DrugPlan=SYNTH-B&_count=100&_offset=100"
    )

    with pytest.raises(FHIRTransportError, match="untrusted"):
        _validated_next_url(
            KAISER_FHIR_BASE,
            current,
            candidate,
            resource_type="MedicationKnowledge",
            expected_alias="SYNTH-A",
        )


def test_list_offset_continuation_does_not_require_a_drug_plan_alias():
    current = f"{KAISER_FHIR_BASE}/List"
    candidate = f"{current}?_count=100&_offset=100"

    assert _validated_next_url(
        KAISER_FHIR_BASE,
        current,
        candidate,
        resource_type="List",
    ) == candidate

    with pytest.raises(FHIRTransportError):
        _validated_next_url(
            KAISER_FHIR_BASE,
            current,
            f"{candidate}&DrugPlan=SYNTH-A",
            resource_type="List",
        )


def test_smile_root_cursor_can_advance_more_than_one_page():
    first = (
        f"{KAISER_FHIR_BASE}?_getpages=opaque-token"
        "&_getpagesoffset=100&_count=100"
    )
    second = (
        f"{KAISER_FHIR_BASE}?_getpages=opaque-token"
        "&_getpagesoffset=200&_count=100"
    )

    assert _validated_next_url(
        KAISER_FHIR_BASE,
        first,
        second,
        resource_type="MedicationKnowledge",
    ) == second


@pytest.mark.parametrize(
    "candidate",
    (
        "https://evil.example/MedicationKnowledge?DrugPlan=SYNTH-A&_count=100&_offset=1",
        f"{KAISER_FHIR_BASE}/MedicationKnowledge?DrugPlan=SYNTH-A,SYNTH-B&_count=100&_offset=1",
        f"{KAISER_FHIR_BASE}/MedicationKnowledge?DrugPlan=SYNTH-A&_count=101&_offset=1",
        f"{KAISER_FHIR_BASE}/MedicationKnowledge?DrugPlan=SYNTH-A&_count=100&_offset=1&unexpected=x",
        f"{KAISER_FHIR_BASE}/MedicationKnowledge?DrugPlan=SYNTH-A&_count=100&_offset=1#fragment",
    ),
)
def test_untrusted_collection_continuations_are_rejected(candidate):
    with pytest.raises((FHIRTransportError, ValueError)):
        _validated_next_url(
            KAISER_FHIR_BASE,
            f"{KAISER_FHIR_BASE}/MedicationKnowledge",
            candidate,
        )


@pytest.mark.asyncio
async def test_empty_page_with_next_is_followed_and_pages_remain_sequential():
    next_url = _next_url("_after", "opaque-token")
    session = _Session(
        [
            _Response(
                {
                    "resourceType": "Bundle",
                    "total": 1,
                    "entry": [],
                    "link": [{"relation": "next", "url": next_url}],
                }
            ),
            _Response(
                {
                    "resourceType": "Bundle",
                    "total": 1,
                    "entry": [
                        {
                            "resource": {
                                "resourceType": "MedicationKnowledge",
                                "id": "synthetic-a",
                            }
                        }
                    ],
                }
            ),
        ]
    )
    cutoff = dt.datetime(2026, 8, 6, tzinfo=dt.UTC)
    async with FHIRFormularyClient(session=session) as client:
        resources = [
            medication_data
            async for medication_data in client.medications(
                "SYNTH-A",
                cutoff=cutoff,
            )
        ]

    assert [medication_data["id"] for medication_data in resources] == [
        "synthetic-a"
    ]
    assert len(session.calls) == 2
    assert session.calls[0][1]["params"][1] == ("_count", "100")
    assert ("_profile", FORMULARY_DRUG_PROFILE) in session.calls[0][1]["params"]
    assert ("_elements", FORMULARY_DRUG_ELEMENTS) in session.calls[0][1]["params"]
    assert session.calls[1][1]["params"] is None


@pytest.mark.asyncio
async def test_coverage_plan_search_is_profiled_and_projected():
    session = _Session([_Response({"resourceType": "Bundle", "entry": []})])
    cutoff = dt.datetime(2026, 8, 6, tzinfo=dt.UTC)
    async with FHIRFormularyClient(session=session) as client:
        assert [item async for item in client.coverage_plans(cutoff=cutoff)] == []

    params = session.calls[0][1]["params"]
    assert params["_profile"] == COVERAGE_PLAN_PROFILE
    assert params["_elements"] == COVERAGE_PLAN_ELEMENTS


@pytest.mark.asyncio
async def test_coverage_plan_count_requires_an_exact_total():
    session = _Session(
        [_Response({"resourceType": "Bundle", "total": 2})]
    )
    cutoff = dt.datetime(2026, 8, 6, tzinfo=dt.UTC)
    async with FHIRFormularyClient(session=session) as client:
        assert await client.coverage_plan_count(cutoff=cutoff) == 2

    params = session.calls[0][1]["params"]
    assert params["_summary"] == "count"
    assert params["_profile"] == COVERAGE_PLAN_PROFILE


@pytest.mark.asyncio
async def test_combined_drug_plan_search_is_rejected_before_transport():
    session = _Session([])
    cutoff = dt.datetime(2026, 8, 6, tzinfo=dt.UTC)
    async with FHIRFormularyClient(session=session) as client:
        with pytest.raises(ValueError):
            await client.alias_count("SYNTH-A,SYNTH-B", cutoff=cutoff)

    assert session.calls == []


@pytest.mark.asyncio
async def test_retry_after_throttle_is_honored_and_count_remains_exact(monkeypatch):
    delays = []

    async def _sleep(delay):
        delays.append(delay)

    monkeypatch.setattr("process.formulary_fhir.client.asyncio.sleep", _sleep)
    session = _Session(
        [
            _Response({}, status=429, headers={"Retry-After": "3"}),
            _Response({"resourceType": "Bundle", "total": 7}),
        ]
    )
    cutoff = dt.datetime(2026, 8, 6, tzinfo=dt.UTC)
    async with FHIRFormularyClient(session=session) as client:
        count = await client.alias_count("SYNTH-A", cutoff=cutoff)

    assert count == 7
    assert session.calls[-1][1]["params"]["_profile"] == FORMULARY_DRUG_PROFILE
    assert delays == [3.0]
    assert client.throttle_count == 1
    assert client.transient_retry_count == 1


@pytest.mark.asyncio
async def test_exhausted_network_retries_are_marked_resumable(monkeypatch):
    delays = []

    async def _sleep(delay):
        delays.append(delay)

    monkeypatch.setattr("process.formulary_fhir.client.asyncio.sleep", _sleep)
    session = _FailingSession()
    cutoff = dt.datetime(2026, 8, 6, tzinfo=dt.UTC)

    async with FHIRFormularyClient(
        session=session,
        max_attempts=2,
    ) as client:
        with pytest.raises(FHIRTransportError) as error:
            await client.alias_count("SYNTH-A", cutoff=cutoff)

    assert error.value.retryable is True
    assert len(session.calls) == 2
    assert delays == [1]
    assert client.transient_retry_count == 1
