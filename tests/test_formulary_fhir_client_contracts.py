# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import datetime as dt
import urllib.parse

import aiohttp
import orjson
import pytest

from process.formulary_fhir.client import FHIRFormularyClient
from process.formulary_fhir.continuation import (
    FHIRTransportError,
    collection_url,
    medication_search_contract,
    page_query_pairs,
)
from process.formulary_fhir.types import enabled_source_config


CANONICAL_BASE = "https://fhir.example.invalid/r4"
CUTOFF = dt.datetime(2026, 8, 6, tzinfo=dt.UTC)


def _source_config(**overrides: int):
    runtime_config_by_name = {
        "timeout_seconds": 30,
        "max_attempts": 2,
        "page_size": 2,
        "max_pages": 4,
        "max_total_resources": 8,
        "max_response_bytes": 64 * 1_024,
    }
    runtime_config_by_name.update(overrides)
    return enabled_source_config(
        canonical_base=CANONICAL_BASE,
        enabled=True,
        runtime_config_json=runtime_config_by_name,
    )


class _Content:
    def __init__(self, response_object):
        self.response_bytes = orjson.dumps(response_object)

    async def iter_chunked(self, chunk_size):
        for offset in range(0, len(self.response_bytes), chunk_size):
            await asyncio.sleep(0)
            yield self.response_bytes[offset : offset + chunk_size]


class _Response:
    def __init__(self, response_object, *, status=200, headers=None):
        self.status = status
        self.headers = headers or {"Content-Type": "application/fhir+json; charset=utf-8"}
        self.content = _Content(response_object)


class _RequestContext:
    def __init__(self, session, response):
        self.session = session
        self.response = response

    async def __aenter__(self):
        self.session.active_requests += 1
        self.session.peak_active_requests = max(
            self.session.peak_active_requests,
            self.session.active_requests,
        )
        await asyncio.sleep(0)
        return self.response

    async def __aexit__(self, *_error_details):
        self.session.active_requests -= 1


class _Session:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []
        self.active_requests = 0
        self.peak_active_requests = 0

    def get(self, request_url, **request_options):
        self.calls.append((request_url, request_options))
        return _RequestContext(self, self.responses.pop(0))


class _FailingSession:
    def __init__(self):
        self.calls = []

    def get(self, request_url, **request_options):
        self.calls.append((request_url, request_options))
        raise aiohttp.ClientConnectionError("synthetic disconnect")


def _count_bundle(exact_total):
    return _Response(
        {
            "resourceType": "Bundle",
            "type": "searchset",
            "total": exact_total,
        }
    )


def _resource(resource_id, *, last_updated="2026-08-05T12:00:00Z"):
    return {
        "resourceType": "MedicationKnowledge",
        "id": resource_id,
        "meta": {"versionId": "1", "lastUpdated": last_updated},
    }


def _coverage_resource(resource_id):
    return {
        "resourceType": "List",
        "id": resource_id,
        "meta": {"versionId": "1", "lastUpdated": "2026-08-05T12:00:00Z"},
    }


def _page_bundle(exact_total, resources, *, next_url=None):
    bundle_by_field = {
        "resourceType": "Bundle",
        "type": "searchset",
        "total": exact_total,
        "entry": [{"resource": resource} for resource in resources],
    }
    if next_url is not None:
        bundle_by_field["link"] = [{"relation": "next", "url": next_url}]
    return _Response(bundle_by_field)


def _next_url(alias, *, offset="2", config=None):
    source_config = config or _source_config()
    contract = medication_search_contract(source_config, alias, CUTOFF)
    query_text = urllib.parse.urlencode(
        (*page_query_pairs(contract), ("_offset", offset))
    )
    return f"{collection_url(contract)}?{query_text}"


@pytest.mark.asyncio
async def test_current_version_census_uses_exact_serial_pre_page_post_contract():
    alias = "SYNTH-SECRET"
    next_url = _next_url(alias)
    session = _Session(
        [
            _count_bundle(2),
            _page_bundle(2, [_resource("drug-a")], next_url=next_url),
            _page_bundle(2, [_resource("drug-b")]),
            _count_bundle(2),
        ]
    )

    async with FHIRFormularyClient(_source_config(), session=session) as client:
        census = await client.medication_current_census(alias, cutoff=CUTOFF)

    assert census.exact_total == 2
    assert [resource["id"] for resource in census.resources] == ["drug-a", "drug-b"]
    assert session.peak_active_requests == 1
    assert session.calls[2][1]["params"] is None
    count_query_by_name = dict(session.calls[0][1]["params"])
    page_query_by_name = dict(session.calls[1][1]["params"])
    assert count_query_by_name == {
        "DrugPlan": alias,
        "_lastUpdated": "lt2026-08-06T00:00:00Z",
        "_profile": (
            "http://hl7.org/fhir/us/davinci-drug-formulary/"
            "StructureDefinition/usdf-FormularyDrug"
        ),
        "_total": "accurate",
        "_summary": "count",
    }
    assert page_query_by_name["_total"] == "accurate"
    assert page_query_by_name["_count"] == "2"
    assert page_query_by_name["_elements"] == "id,meta,status,code,extension"
    assert alias not in repr(census)
    assert "drug-a" not in repr(census)


@pytest.mark.asyncio
async def test_request_gate_keeps_concurrent_callers_physically_serial():
    session = _Session([_count_bundle(0) for _unused in range(6)])

    async with FHIRFormularyClient(_source_config(), session=session) as client:
        first_census, second_census = await asyncio.gather(
            client.medication_current_census("SYNTH-A", cutoff=CUTOFF),
            client.medication_current_census("SYNTH-B", cutoff=CUTOFF),
        )

    assert first_census.exact_total == second_census.exact_total == 0
    assert session.peak_active_requests == 1
    assert len(session.calls) == 6


@pytest.mark.asyncio
async def test_injected_session_can_reenter_but_requests_outside_context_fail():
    session = _Session([_count_bundle(0) for _unused in range(6)])
    client = FHIRFormularyClient(_source_config(), session=session)

    with pytest.raises(RuntimeError, match="entered before use"):
        await client.medication_current_census("SYNTH-A", cutoff=CUTOFF)
    async with client:
        assert (
            await client.medication_current_census("SYNTH-A", cutoff=CUTOFF)
        ).exact_total == 0
    async with client:
        assert (
            await client.medication_current_census("SYNTH-B", cutoff=CUTOFF)
        ).exact_total == 0

    assert len(session.calls) == 6


@pytest.mark.asyncio
async def test_coverage_plan_census_uses_only_the_approved_list_contract():
    session = _Session(
        [
            _count_bundle(1),
            _page_bundle(1, [_coverage_resource("coverage-a")]),
            _count_bundle(1),
        ]
    )

    async with FHIRFormularyClient(_source_config(), session=session) as client:
        census = await client.coverage_plan_current_census(cutoff=CUTOFF)

    count_query_by_name = dict(session.calls[0][1]["params"])
    page_query_by_name = dict(session.calls[1][1]["params"])
    assert census.resource_type == "List"
    assert census.resources[0]["id"] == "coverage-a"
    assert "DrugPlan" not in count_query_by_name
    assert page_query_by_name["_profile"].endswith("/usdf-CoveragePlan")
    assert page_query_by_name["_elements"] == (
        "id,meta,status,title,name,date,identifier,extension"
    )


@pytest.mark.asyncio
async def test_duplicate_resource_ids_fail_without_echoing_alias_or_id():
    alias = "SYNTH-SECRET"
    session = _Session(
        [
            _count_bundle(2),
            _page_bundle(2, [_resource("drug-a")], next_url=_next_url(alias)),
            _page_bundle(2, [_resource("drug-a")]),
        ]
    )

    async with FHIRFormularyClient(_source_config(), session=session) as client:
        with pytest.raises(FHIRTransportError) as caught_error:
            await client.medication_current_census(alias, cutoff=CUTOFF)

    assert "duplicate" in str(caught_error.value)
    assert alias not in str(caught_error.value)
    assert "drug-a" not in str(caught_error.value)


@pytest.mark.asyncio
async def test_empty_intermediate_page_is_rejected_before_following_cursor():
    alias = "SYNTH-A"
    session = _Session(
        [
            _count_bundle(1),
            _page_bundle(1, [], next_url=_next_url(alias)),
        ]
    )

    async with FHIRFormularyClient(_source_config(), session=session) as client:
        with pytest.raises(FHIRTransportError, match="empty intermediate"):
            await client.medication_current_census(alias, cutoff=CUTOFF)

    assert len(session.calls) == 2


@pytest.mark.asyncio
async def test_repeated_continuation_is_rejected_as_a_cycle():
    alias = "SYNTH-A"
    repeated_url = _next_url(alias)
    session = _Session(
        [
            _count_bundle(3),
            _page_bundle(3, [_resource("drug-a")], next_url=repeated_url),
            _page_bundle(3, [_resource("drug-b")], next_url=repeated_url),
        ]
    )

    async with FHIRFormularyClient(_source_config(), session=session) as client:
        with pytest.raises(FHIRTransportError, match="cycle"):
            await client.medication_current_census(alias, cutoff=CUTOFF)

    assert len(session.calls) == 3


@pytest.mark.asyncio
async def test_page_bound_stops_before_an_unapproved_request():
    config = _source_config(max_pages=1, max_total_resources=2)
    alias = "SYNTH-A"
    session = _Session(
        [
            _count_bundle(2),
            _page_bundle(
                2,
                [_resource("drug-a")],
                next_url=_next_url(alias, config=config),
            ),
        ]
    )

    async with FHIRFormularyClient(config, session=session) as client:
        with pytest.raises(FHIRTransportError, match="page bound"):
            await client.medication_current_census(alias, cutoff=CUTOFF)

    assert len(session.calls) == 2


@pytest.mark.asyncio
async def test_exact_total_and_page_total_must_remain_inside_bounds():
    config = _source_config(max_pages=1, max_total_resources=2)
    over_bound_session = _Session([_count_bundle(3)])
    async with FHIRFormularyClient(config, session=over_bound_session) as client:
        with pytest.raises(FHIRTransportError, match="total"):
            await client.medication_current_census("SYNTH-A", cutoff=CUTOFF)

    changed_total_session = _Session(
        [_count_bundle(1), _page_bundle(0, [])]
    )
    async with FHIRFormularyClient(
        _source_config(),
        session=changed_total_session,
    ) as client:
        with pytest.raises(FHIRTransportError, match="total changed"):
            await client.medication_current_census("SYNTH-A", cutoff=CUTOFF)


@pytest.mark.asyncio
async def test_post_census_count_detects_current_version_drift():
    session = _Session(
        [
            _count_bundle(1),
            _page_bundle(1, [_resource("drug-a")]),
            _count_bundle(0),
        ]
    )

    async with FHIRFormularyClient(_source_config(), session=session) as client:
        with pytest.raises(FHIRTransportError, match="changed during traversal"):
            await client.medication_current_census("SYNTH-A", cutoff=CUTOFF)


@pytest.mark.asyncio
async def test_resource_version_must_be_strictly_older_than_cutoff():
    session = _Session(
        [
            _count_bundle(1),
            _page_bundle(
                1,
                [_resource("drug-a", last_updated="2026-08-06T00:00:00Z")],
            ),
        ]
    )

    async with FHIRFormularyClient(_source_config(), session=session) as client:
        with pytest.raises(FHIRTransportError, match="census cutoff"):
            await client.medication_current_census("SYNTH-A", cutoff=CUTOFF)


@pytest.mark.asyncio
async def test_transient_retry_is_bounded_and_honors_capped_retry_after(monkeypatch):
    observed_delays = []

    async def _capture_sleep(delay):
        observed_delays.append(delay)

    monkeypatch.setattr("process.formulary_fhir.client.asyncio.sleep", _capture_sleep)
    session = _Session(
        [
            _Response({}, status=429, headers={"Retry-After": "60"}),
            _count_bundle(0),
            _count_bundle(0),
            _count_bundle(0),
        ]
    )

    async with FHIRFormularyClient(_source_config(), session=session) as client:
        census = await client.medication_current_census("SYNTH-A", cutoff=CUTOFF)

    assert census.exact_total == 0
    assert [delay for delay in observed_delays if delay] == [30.0]
    assert client.throttle_count == 1
    assert client.transient_retry_count == 1


@pytest.mark.asyncio
async def test_exhausted_transport_error_is_sanitized(monkeypatch):
    async def _skip_sleep(_delay):
        return None

    monkeypatch.setattr("process.formulary_fhir.client.asyncio.sleep", _skip_sleep)
    session = _FailingSession()
    alias = "SYNTH-SECRET"

    async with FHIRFormularyClient(_source_config(), session=session) as client:
        with pytest.raises(FHIRTransportError) as caught_error:
            await client.medication_current_census(alias, cutoff=CUTOFF)

    assert len(session.calls) == 2
    assert alias not in str(caught_error.value)
    assert "disconnect" not in str(caught_error.value)
    assert caught_error.value.__cause__ is None


@pytest.mark.asyncio
async def test_response_media_type_and_byte_bound_fail_closed():
    wrong_media_session = _Session(
        [_Response({}, headers={"Content-Type": "text/html"})]
    )
    async with FHIRFormularyClient(
        _source_config(),
        session=wrong_media_session,
    ) as client:
        with pytest.raises(FHIRTransportError, match="media type"):
            await client.medication_current_census("SYNTH-A", cutoff=CUTOFF)

    oversized_session = _Session([_Response({"text": "x" * 2_000})])
    async with FHIRFormularyClient(
        _source_config(max_response_bytes=1_024),
        session=oversized_session,
    ) as client:
        with pytest.raises(FHIRTransportError, match="byte bound"):
            await client.medication_current_census("SYNTH-A", cutoff=CUTOFF)
