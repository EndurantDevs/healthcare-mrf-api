# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Keep every ordinary nearby response read and encoding inside one snapshot."""

import asyncio
import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sanic.exceptions import InvalidUsage

from api.endpoint import npi
from tests.npi_location_hydration_support import unified_location_mapping
from tests.test_npi_api_extended import _build_near_row, _set_near_address_column


def _install_near_connection(monkeypatch, check, state):
    class Connection:
        async def all(self, statement, **_parameters):
            sql = str(statement)
            if "from zcta5" in sql:
                check("zip")
                return [state.zip_record]
            state.query_parameters.append(_parameters)
            if "COUNT(DISTINCT" in sql:
                check("count")
                return [(state.total_count,)]
            check("page")
            return state.provider_rows

    @asynccontextmanager
    async def acquire():
        check("acquire")
        yield Connection()

    monkeypatch.setattr(npi, "db", SimpleNamespace(acquire=acquire))


def _install_filter_readers(monkeypatch, check):
    async def year(*_args, session=None, **_kwargs):
        check("year", session)
        return 2023, "requested"

    async def codes(*_args, session=None, **_kwargs):
        check("codes", session)
        return [1001], None

    async def capabilities(*, session=None):
        check("capabilities", session)
        return {"npi_procedures_array_available": True}

    async def address(*_args, session=None):
        check("address", session)
        return "mrf.entity_address_unified"

    async def plan(session, _release):
        check("plan", session)
        return None, {}

    monkeypatch.setattr(npi, "_resolve_filter_year", year)
    monkeypatch.setattr(npi, "_resolve_internal_filter_codes", codes)
    monkeypatch.setattr(npi, "_resolve_npi_filter_capabilities", capabilities)
    monkeypatch.setattr(npi, "_address_serving_table_sql", address)
    monkeypatch.setattr(npi, "_plan_release_npi_scope", plan)


@pytest.fixture
def near_snapshot(monkeypatch):
    state = SimpleNamespace(
        active=False,
        entered=0,
        exited=0,
        reads=[],
        session=object(),
        failure=None,
        zip_record={"intptlat": "41.0", "intptlon": "-87.0"},
        query_parameters=[],
        total_count=1,
    )
    location_by_field = {
        **unified_location_mapping(),
        "npi": 1000000004,
        "cursor_distance_meters": 100.0,
        "distance": 0.06,
    }
    state.provider_rows = [SimpleNamespace(_mapping=location_by_field)]

    def check(name, session=None):
        assert state.active
        if session is not None:
            assert session is state.session
        state.reads.append(name)
        if state.failure == name:
            raise RuntimeError("read unavailable")

    @asynccontextmanager
    async def snapshot(_database, _schema, *, include_detail=False):
        assert include_detail and not state.active
        state.entered += 1
        state.active = True
        try:
            yield state.session
        finally:
            state.active = False
            state.exited += 1

    async def metadata(rows, *, session=None, **_kwargs):
        check("metadata", session)
        await asyncio.sleep(0)
        check("metadata_after_await", session)
        return rows

    encode = npi.response.json

    def encode_in_snapshot(*args, **kwargs):
        check("encode")
        return encode(*args, **kwargs)

    monkeypatch.setattr(npi, "provider_profile_read_snapshot", snapshot)
    _install_near_connection(monkeypatch, check, state)
    _install_filter_readers(monkeypatch, check)
    monkeypatch.setattr(npi, "_project_provider_network_metadata", metadata)
    monkeypatch.setattr(npi.response, "json", encode_in_snapshot)
    return state


@pytest.mark.parametrize("view", ("", "card"))
@pytest.mark.parametrize("paginated", (False, True))
@pytest.mark.parametrize("zip_record", ({"intptlat": "41.0", "intptlon": "-87.0"}, ("41.0", "-87.0")))
async def test_near_snapshot_covers_zip_filters_page_count_metadata_and_encoding(
    near_snapshot, view, paginated, zip_record
):
    near_snapshot.zip_record = zip_record
    prior_session = object()
    request = SimpleNamespace(
        args={
            "zip_codes": "60001",
            "procedure_codes": "1001",
            "year": "2023",
            "include_total": str(paginated).lower(),
            "view": view,
        },
        ctx=SimpleNamespace(sa_session=prior_session),
        app=SimpleNamespace(),
    )
    http_response = await npi.get_near_npi(request)
    document = json.loads(http_response.body)
    assert len(document["items"] if paginated else document) == 1
    if paginated:
        assert document["total_count"] == 1
    assert {"year", "codes", "capabilities", "zip", "address", "plan", "page", "encode"} <= set(near_snapshot.reads)
    assert ("count" in near_snapshot.reads) is paginated
    assert ("metadata_after_await" in near_snapshot.reads) is (view != "card")
    assert near_snapshot.query_parameters
    assert all(
        parameters["in_lat"] == 41.0 and parameters["in_long"] == -87.0 for parameters in near_snapshot.query_parameters
    )
    assert near_snapshot.entered == near_snapshot.exited == 1 and not near_snapshot.active
    assert request.ctx.sa_session is prior_session


async def test_near_pagination_keeps_cursor_and_encoding_inside_snapshot(near_snapshot):
    near_snapshot.provider_rows.append(
        SimpleNamespace(
            _mapping={
                **near_snapshot.provider_rows[0]._mapping,
                "npi": 1000000012,
                "cursor_distance_meters": 200.0,
            }
        )
    )
    near_snapshot.total_count = 2
    prior_session = object()
    request = SimpleNamespace(
        args={"lat": "41", "long": "-87", "limit": "1", "include_total": "1"},
        ctx=SimpleNamespace(sa_session=prior_session),
        app=SimpleNamespace(),
    )
    http_response = await npi.get_near_npi(request)
    document = json.loads(http_response.body)
    assert len(document["items"]) == 1 and document["items"][0]["npi"] == 1000000004
    assert document["total_count"] == 2 and document["has_more"] is True
    assert document["next_cursor"]
    assert not any(key.startswith("_cursor_") for key in document["items"][0])
    assert {"count", "metadata_after_await", "encode"} <= set(near_snapshot.reads)
    assert near_snapshot.entered == near_snapshot.exited == 1 and not near_snapshot.active
    assert request.ctx.sa_session is prior_session


async def test_near_empty_page_keeps_zero_total_and_encoding_inside_snapshot(near_snapshot):
    near_snapshot.provider_rows = []
    near_snapshot.total_count = 0
    http_response = await npi.get_near_npi(
        SimpleNamespace(args={"lat": "41", "long": "-87", "include_total": "1"}, app=SimpleNamespace())
    )
    document = json.loads(http_response.body)
    assert document["items"] == [] and document["total_count"] == 0
    assert document["has_more"] is False and document["next_cursor"] is None
    assert {"page", "count", "metadata_after_await", "encode"} <= set(near_snapshot.reads)
    assert near_snapshot.entered == near_snapshot.exited == 1 and not near_snapshot.active


async def test_near_positional_provider_rows_deduplicate_inside_snapshot(near_snapshot):
    provider_record = _build_near_row(1000000004)
    _set_near_address_column(provider_record, "telephone_number", "(217) 555-0100")
    _set_near_address_column(provider_record, "phone_number", None)
    _set_near_address_column(provider_record, "country_code", "US")
    near_snapshot.provider_rows = [provider_record, provider_record.copy()]
    http_response = await npi.get_near_npi(SimpleNamespace(args={"lat": "41", "long": "-87"}, app=SimpleNamespace()))
    document = json.loads(http_response.body)
    assert len(document) == 1 and len(document[0]["taxonomy_list"]) == 1
    assert document[0]["npi"] == 1000000004 and document[0]["phone_number"] == "2175550100"
    assert "employer_identification_number" not in document[0]
    assert {"metadata_after_await", "encode"} <= set(near_snapshot.reads)
    assert near_snapshot.entered == near_snapshot.exited == 1 and not near_snapshot.active


@pytest.mark.parametrize(
    "provider_record",
    (
        SimpleNamespace(_mapping={}),
        SimpleNamespace(_mapping={"npi": 1000000004}),
        (),
        (1000000004, 0.5),
    ),
)
async def test_near_incomplete_provider_rows_keep_response_and_snapshot_safe(near_snapshot, provider_record):
    near_snapshot.provider_rows = [provider_record]
    http_response = await npi.get_near_npi(SimpleNamespace(args={"lat": "41", "long": "-87"}, app=SimpleNamespace()))
    assert json.loads(http_response.body) == []
    assert {"page", "encode"} <= set(near_snapshot.reads)
    assert near_snapshot.entered == near_snapshot.exited == 1 and not near_snapshot.active


@pytest.mark.parametrize("failure", ("address", "page", "count", "metadata_after_await", "encode"))
async def test_near_read_or_encoding_failure_closes_snapshot(near_snapshot, failure):
    near_snapshot.failure = failure
    request = SimpleNamespace(args={"lat": "41", "long": "-87", "include_total": "1"}, app=SimpleNamespace())
    with pytest.raises(RuntimeError, match="read unavailable"):
        await npi.get_near_npi(request)
    assert near_snapshot.entered == near_snapshot.exited == 1 and not near_snapshot.active


async def test_near_invalid_controls_do_not_open_snapshot(near_snapshot):
    for args in (
        {"limit": "0"},
        {"section": "Individual"},
        {"procedure_code_system": "invalid"},
        {"lat": "north", "long": "-87"},
        {"limit": "many"},
    ):
        with pytest.raises(InvalidUsage):
            await npi.get_near_npi(SimpleNamespace(args=args, app=SimpleNamespace()))
    assert near_snapshot.entered == near_snapshot.exited == 0 and near_snapshot.reads == []


async def test_near_unresolved_codes_encode_inside_snapshot(near_snapshot, monkeypatch):
    monkeypatch.setattr(npi, "_resolve_internal_filter_codes", AsyncMock(return_value=([], None)))
    result = await npi.get_near_npi(
        SimpleNamespace(args={"procedure_codes": "1001", "include_total": "1"}, app=SimpleNamespace())
    )
    assert json.loads(result.body)["items"] == []
    assert near_snapshot.reads == ["year", "encode"]
    assert near_snapshot.entered == near_snapshot.exited == 1 and not near_snapshot.active
