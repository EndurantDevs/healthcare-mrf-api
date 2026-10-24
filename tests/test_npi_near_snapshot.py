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


def _install_near_connection(monkeypatch, check, location_by_field):
    class Connection:
        async def all(self, statement, **_parameters):
            sql = str(statement)
            if "from zcta5" in sql:
                check("zip")
                return [{"intptlat": "41.0", "intptlon": "-87.0"}]
            if "COUNT(DISTINCT" in sql:
                check("count")
                return [(1,)]
            check("page")
            return [SimpleNamespace(_mapping=location_by_field)]

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
    state = SimpleNamespace(active=False, entered=0, exited=0, reads=[], session=object(), failure=None)
    location_by_field = {
        **unified_location_mapping(),
        "npi": 1000000004,
        "cursor_distance_meters": 100.0,
        "distance": 0.06,
    }

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
    _install_near_connection(monkeypatch, check, location_by_field)
    _install_filter_readers(monkeypatch, check)
    monkeypatch.setattr(npi, "_project_provider_network_metadata", metadata)
    monkeypatch.setattr(npi.response, "json", encode_in_snapshot)
    return state


@pytest.mark.parametrize("view", ("", "card"))
@pytest.mark.parametrize("paginated", (False, True))
async def test_near_snapshot_covers_zip_filters_page_count_metadata_and_encoding(near_snapshot, view, paginated):
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
    result = await npi.get_near_npi(request)
    document = json.loads(result.body)
    assert len(document["items"] if paginated else document) == 1
    if paginated:
        assert document["total_count"] == 1
    assert {"year", "codes", "capabilities", "zip", "address", "plan", "page", "encode"} <= set(near_snapshot.reads)
    assert ("count" in near_snapshot.reads) is paginated
    assert ("metadata_after_await" in near_snapshot.reads) is (view != "card")
    assert near_snapshot.entered == near_snapshot.exited == 1 and not near_snapshot.active
    assert request.ctx.sa_session is prior_session


@pytest.mark.parametrize("failure", ("address", "page", "count", "metadata_after_await", "encode"))
async def test_near_read_or_encoding_failure_closes_snapshot(near_snapshot, failure):
    near_snapshot.failure = failure
    request = SimpleNamespace(args={"lat": "41", "long": "-87", "include_total": "1"}, app=SimpleNamespace())
    with pytest.raises(RuntimeError, match="read unavailable"):
        await npi.get_near_npi(request)
    assert near_snapshot.entered == near_snapshot.exited == 1 and not near_snapshot.active


async def test_near_invalid_controls_do_not_open_snapshot(near_snapshot):
    for args in ({"limit": "0"}, {"section": "Individual"}, {"procedure_code_system": "invalid"}):
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
