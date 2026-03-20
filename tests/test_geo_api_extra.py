# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import json
import sys
import types
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

import pytest
from asyncpg import UndefinedColumnError, UndefinedTableError
from sanic.exceptions import InvalidUsage
from sqlalchemy.exc import ProgrammingError

ROOT_PATH = Path(__file__).resolve().parents[1]
API_PATH = ROOT_PATH / "api"
ENDPOINT_PATH = API_PATH / "endpoint"


def _restore_module(name, previous):
    if previous is None:
        sys.modules.pop(name, None)
    else:
        sys.modules[name] = previous


def _load_geo_module():
    old_api = sys.modules.get("api")
    old_api_endpoint = sys.modules.get("api.endpoint")
    old_api_endpoint_pagination = sys.modules.get("api.endpoint.pagination")

    try:
        api_pkg = types.ModuleType("api")
        api_pkg.__path__ = [str(API_PATH)]
        endpoint_pkg = types.ModuleType("api.endpoint")
        endpoint_pkg.__path__ = [str(ENDPOINT_PATH)]
        sys.modules["api"] = api_pkg
        sys.modules["api.endpoint"] = endpoint_pkg

        pagination_spec = spec_from_file_location("api.endpoint.pagination", ENDPOINT_PATH / "pagination.py")
        pagination_module = module_from_spec(pagination_spec)
        sys.modules["api.endpoint.pagination"] = pagination_module
        pagination_spec.loader.exec_module(pagination_module)

        module_spec = spec_from_file_location("api.endpoint.geo_unit_extra", ENDPOINT_PATH / "geo.py")
        module = module_from_spec(module_spec)
        module_spec.loader.exec_module(module)
        return module
    finally:
        _restore_module("api", old_api)
        _restore_module("api.endpoint", old_api_endpoint)
        _restore_module("api.endpoint.pagination", old_api_endpoint_pagination)


geo_module = _load_geo_module()


@pytest.mark.asyncio
async def test_geo_index_handler():
    handler = next(route.handler for route in geo_module.blueprint._future_routes if route.uri == "/get")
    request = types.SimpleNamespace(
        args={"zip_code": "60601", "lat": "41.0", "long": "-87.0"},
        app=types.SimpleNamespace(config={"RELEASE": "test", "ENVIRONMENT": "dev"}),
    )
    response = await handler(request)
    payload = json.loads(response.body)
    assert payload["release"] == "test"
    assert payload["environment"] == "dev"
    assert "date" in payload


class FakeResult:
    def __init__(self, row=None, rows=None, scalar_value=None):
        self._row = row
        self._rows = rows
        self._scalar_value = scalar_value

    def first(self):
        return self._row

    def all(self):
        if self._rows is not None:
            return self._rows
        return [] if self._row is None else [self._row]

    def scalar(self):
        return self._scalar_value


class FakeSession:
    def __init__(self, responses=None):
        self._responses = list(responses or [])

    async def execute(self, *_args, **_kwargs):
        if not self._responses:
            return FakeResult()
        result = self._responses.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


class MappingRow:
    def __init__(self, **mapping):
        self._mapping = mapping


@pytest.mark.asyncio
async def test_geo_by_zip_success():
    request = types.SimpleNamespace(
        ctx=types.SimpleNamespace(
            sa_session=FakeSession(
                [
                    FakeResult(row=None),
                    FakeResult(
                        row=MappingRow(
                            zip_code="12345",
                            city="Chicago",
                            state="IL",
                            latitude=41.1,
                            longitude=-87.6,
                        )
                    ),
                ]
            )
        ),
        app=types.SimpleNamespace(),
    )
    response = await geo_module.get_geo(request, "12345")
    payload = json.loads(response.body)
    assert payload["zip_code"] == "12345"
    assert payload["lat"] == 41.1
    assert payload["city"] == "Chicago"
    assert payload["census_profile"] is None


@pytest.mark.asyncio
async def test_geo_by_zip_not_found():
    request = types.SimpleNamespace(
        ctx=types.SimpleNamespace(
            sa_session=FakeSession([FakeResult(row=None), FakeResult(row=None), FakeResult(row=None)])
        ),
        app=types.SimpleNamespace(),
    )
    response = await geo_module.get_geo(request, "00000")
    assert response.status == 404


@pytest.mark.asyncio
async def test_geo_by_zip_bad_row():
    request = types.SimpleNamespace(
        ctx=types.SimpleNamespace(sa_session=FakeSession([
            FakeResult(row=None),
            FakeResult(row=None),
            FakeResult(row=("99999", "not-a-number", None, "NY")),
        ])),
        app=types.SimpleNamespace(),
    )
    response = await geo_module.get_geo(request, "99999")
    assert response.status == 404


@pytest.mark.asyncio
async def test_geo_by_zip_missing_table():
    error = ProgrammingError("select", {}, None)
    error.orig = UndefinedTableError("tiger schema")
    request = types.SimpleNamespace(
        ctx=types.SimpleNamespace(sa_session=FakeSession([FakeResult(row=None), FakeResult(row=None), error])),
        app=types.SimpleNamespace(),
    )
    response = await geo_module.get_geo(request, "12345")
    assert response.status == 503
    assert json.loads(response.body)["error"] == "tiger schema not available"


@pytest.mark.asyncio
async def test_geo_by_zip_missing_session():
    request = types.SimpleNamespace(ctx=types.SimpleNamespace(sa_session=None), app=types.SimpleNamespace())
    with pytest.raises(RuntimeError):
        await geo_module.get_geo(request, "12345")


@pytest.mark.asyncio
async def test_geo_by_zip_missing_census_table_falls_back_to_geo():
    error = ProgrammingError("select", {}, None)
    error.orig = UndefinedTableError("census table")
    request = types.SimpleNamespace(
        ctx=types.SimpleNamespace(
            sa_session=FakeSession(
                [
                    error,
                    FakeResult(
                        row=MappingRow(
                            zip_code="60654",
                            city="Chicago",
                            state="IL",
                            latitude=41.9,
                            longitude=-87.6,
                            state_name="Illinois",
                            county_name="Cook",
                            timezone="America/Chicago",
                        )
                    ),
                ]
            )
        ),
        app=types.SimpleNamespace(),
    )
    response = await geo_module.get_geo(request, "60654")
    payload = json.loads(response.body)
    assert response.status == 200
    assert payload["zip_code"] == "60654"
    assert payload["census_profile"] is None


@pytest.mark.asyncio
async def test_geo_by_zip_missing_census_column_falls_back_to_geo():
    error = ProgrammingError("select", {}, None)
    error.orig = UndefinedColumnError("missing census column")
    request = types.SimpleNamespace(
        ctx=types.SimpleNamespace(
            sa_session=FakeSession(
                [
                    error,
                    FakeResult(
                        row=MappingRow(
                            zip_code="60654",
                            city="Chicago",
                            state="IL",
                            latitude=41.9,
                            longitude=-87.6,
                            state_name="Illinois",
                            county_name="Cook",
                            timezone="America/Chicago",
                        )
                    ),
                ]
            )
        ),
        app=types.SimpleNamespace(),
    )
    response = await geo_module.get_geo(request, "60654")
    payload = json.loads(response.body)
    assert response.status == 200
    assert payload["zip_code"] == "60654"
    assert payload["census_profile"] is None


@pytest.mark.asyncio
async def test_geo_by_zip_census_runtime_error_falls_back_to_geo(monkeypatch):
    async def _raise_runtime(*_args, **_kwargs):
        raise RuntimeError("statement timeout")

    monkeypatch.setattr(geo_module, "_lookup_census_profile", _raise_runtime)
    request = types.SimpleNamespace(
        ctx=types.SimpleNamespace(
            sa_session=FakeSession(
                [
                    FakeResult(
                        row=MappingRow(
                            zip_code="07666",
                            city="Teaneck",
                            state="NJ",
                            latitude=40.89,
                            longitude=-74.01,
                            state_name="New Jersey",
                            county_name="Bergen",
                            timezone="America/New_York",
                        )
                    )
                ]
            )
        ),
        app=types.SimpleNamespace(),
    )
    response = await geo_module.get_geo(request, "07666")
    payload = json.loads(response.body)
    assert response.status == 200
    assert payload["zip_code"] == "07666"
    assert payload["census_profile"] is None


@pytest.mark.asyncio
async def test_geo_by_zip_other_programming_error():
    error = ProgrammingError("select", {}, None)
    error.orig = Exception("other")
    request = types.SimpleNamespace(
        ctx=types.SimpleNamespace(sa_session=FakeSession([FakeResult(row=None), FakeResult(row=None), error])),
        app=types.SimpleNamespace(),
    )
    with pytest.raises(ProgrammingError):
        await geo_module.get_geo(request, "12345")


@pytest.mark.asyncio
async def test_lookup_provider_count_query_avoids_coalesce():
    class CaptureSession:
        def __init__(self):
            self.last_stmt = None

        async def execute(self, stmt, *_args, **_kwargs):
            self.last_stmt = stmt
            return FakeResult(scalar_value=42)

    session = CaptureSession()
    value = await geo_module._lookup_provider_count(session, "60654")
    assert value == 42
    compiled = str(session.last_stmt).lower()
    assert "coalesce" not in compiled


@pytest.mark.asyncio
async def test_geo_states_invalid_sort():
    request = types.SimpleNamespace(
        args={"sort": "invalid"},
        ctx=types.SimpleNamespace(sa_session=FakeSession([])),
    )
    with pytest.raises(InvalidUsage):
        await geo_module.list_geo_states(request)


def test_row_mapping_and_serialize_helpers():
    assert geo_module._row_mapping({"zip_code": "123"})["zip_code"] == "123"

    class RowIterable:
        def __iter__(self):
            return iter([("zip_code", "99999"), ("city", "Foo")])

    mapping = geo_module._row_mapping(RowIterable())
    assert mapping["zip_code"] == "99999"
    assert geo_module._serialize_geo_row(None) is None


@pytest.mark.asyncio
async def test_geo_by_zip_local_lat_long_fallback():
    request = types.SimpleNamespace(
        ctx=types.SimpleNamespace(
            sa_session=FakeSession([
                FakeResult(row=None),
                FakeResult(
                    row=MappingRow(
                        zip_code="11111",
                        city="City",
                        state="IL",
                        latitude=None,
                        longitude=None,
                        state_name="Illinois",
                        county_name="Cook",
                        timezone="CST",
                    )
                )
            ])
        ),
        app=types.SimpleNamespace(),
    )
    response = await geo_module.get_geo(request, "11111")
    payload = json.loads(response.body)
    assert payload["zip_code"] == "11111"
    assert payload["lat"] is None
    assert payload["long"] is None
    assert payload["census_profile"] is None


@pytest.mark.asyncio
async def test_geo_by_zip_tiger_fallback():
    request = types.SimpleNamespace(
        ctx=types.SimpleNamespace(
            sa_session=FakeSession([
                FakeResult(row=None),
                FakeResult(row=None),
                FakeResult(row=("22222", "41.0", "-87.0", "IL")),
            ])
        ),
        app=types.SimpleNamespace(),
    )
    response = await geo_module.get_geo(request, "22222")
    payload = json.loads(response.body)
    assert payload == {
        "zip_code": "22222",
        "lat": 41.0,
        "long": -87.0,
        "state": "IL",
        "census_profile": None,
    }


@pytest.mark.asyncio
async def test_geo_by_zip_with_census_profile():
    census_row = MappingRow(
        total_population=23890,
        median_household_income=147357,
        bachelors_degree_or_higher_pct=91.5,
        employment_rate_pct=85.1,
        total_housing_units=18505,
        without_health_insurance_pct=3.4,
        total_employer_establishments=2224,
        business_employment=52832,
        business_payroll_annual_k=8723456,
        total_households=16968,
        hispanic_or_latino=1469,
        poverty_rate_pct=6.1,
        median_age=35.8,
        unemployment_rate_pct=4.7,
        labor_force_participation_pct=79.8,
        vacancy_rate_pct=8.3,
        median_home_value=575000,
        median_gross_rent=2450,
        commute_mean_minutes=28.4,
        commute_mode_drove_alone_pct=52.0,
        commute_mode_carpool_pct=6.0,
        commute_mode_public_transit_pct=22.0,
        commute_mode_walked_pct=12.0,
        commute_mode_worked_from_home_pct=8.0,
        broadband_access_pct=96.2,
        race_white_alone=17450,
        race_black_or_african_american_alone=1800,
        race_american_indian_and_alaska_native_alone=120,
        race_asian_alone=3050,
        race_native_hawaiian_and_other_pacific_islander_alone=25,
        race_some_other_race_alone=690,
        race_two_or_more_races=755,
        race_white_alone_pct=73.04,
        race_black_or_african_american_alone_pct=7.53,
        race_american_indian_and_alaska_native_alone_pct=0.50,
        race_asian_alone_pct=12.77,
        race_native_hawaiian_and_other_pacific_islander_alone_pct=0.10,
        race_some_other_race_alone_pct=2.89,
        race_two_or_more_races_pct=3.16,
        acs_white_alone_pct=74.1,
        acs_black_or_african_american_alone_pct=5.3,
        acs_american_indian_and_alaska_native_alone_pct=0.2,
        acs_asian_alone_pct=13.7,
        acs_native_hawaiian_and_other_pacific_islander_alone_pct=0.1,
        acs_some_other_race_alone_pct=2.9,
        acs_two_or_more_races_pct=3.7,
        acs_hispanic_or_latino_pct=8.2,
    )
    svi_row = MappingRow(
        svi_overall=0.402,
        svi_socioeconomic=0.291,
        svi_household=0.338,
        svi_minority=0.441,
        svi_housing=0.379,
    )
    request = types.SimpleNamespace(
        ctx=types.SimpleNamespace(
            sa_session=FakeSession(
                [
                    FakeResult(row=census_row),
                    FakeResult(row=svi_row),
                    FakeResult(scalar_value=800),
                    FakeResult(
                        row=MappingRow(
                            zip_code="60654",
                            city="Chicago",
                            state="IL",
                            latitude=41.9,
                            longitude=-87.6,
                            state_name="Illinois",
                            county_name="Cook",
                            timezone="America/Chicago",
                        )
                    ),
                ]
            )
        ),
        app=types.SimpleNamespace(),
    )
    response = await geo_module.get_geo(request, "60654")
    payload = json.loads(response.body)
    assert payload["zip_code"] == "60654"
    assert payload["census_profile"]["median_household_income"] == 147357
    assert payload["census_profile"]["total_employer_establishments"] == 2224
    assert payload["census_profile"]["business_employment"] == 52832
    assert payload["census_profile"]["svi_overall"] == 0.402
    assert payload["census_profile"]["provider_count"] == 800
    assert "pharmacy_count" not in payload["census_profile"]
    assert "pharmacy_density_per_1000" not in payload["census_profile"]
    assert payload["census_profile"]["provider_density_per_1000"] == pytest.approx(33.4868, rel=1e-4)


@pytest.mark.asyncio
async def test_geo_city_missing_param():
    request = types.SimpleNamespace(args={}, ctx=types.SimpleNamespace(sa_session=FakeSession([])))
    with pytest.raises(InvalidUsage):
        await geo_module.get_geo_by_city(request)


@pytest.mark.asyncio
async def test_geo_city_with_state_filter():
    rows = [
        MappingRow(zip_code="73301", city="Austin", state="TX", state_name="Texas", county_name="Travis", latitude=30.3, longitude=-97.7, timezone="CST"),
        MappingRow(zip_code="90001", city="Austin", state="CA", state_name="California", county_name="L.A.", latitude=34.0, longitude=-118.2, timezone="PST"),
    ]
    request = types.SimpleNamespace(
        args={"city": "Austin", "state": "tx"},
        ctx=types.SimpleNamespace(sa_session=FakeSession([FakeResult(rows=rows)])),
    )
    response = await geo_module.get_geo_by_city(request)
    payload = json.loads(response.body)
    assert payload["state"] == "TX"


@pytest.mark.asyncio
async def test_geo_states_invalid_order():
    request = types.SimpleNamespace(
        args={"order": "invalid"},
        ctx=types.SimpleNamespace(sa_session=FakeSession([])),
    )
    with pytest.raises(InvalidUsage):
        await geo_module.list_geo_states(request)


@pytest.mark.asyncio
async def test_geo_places_by_zip_success_defaults_latest_year():
    request = types.SimpleNamespace(
        args={},
        ctx=types.SimpleNamespace(
            sa_session=FakeSession(
                [
                    FakeResult(scalar_value=2025),
                    FakeResult(
                        rows=[
                            MappingRow(
                                measure_id="CSMOKING",
                                measure_name="Current smoking among adults aged >=18 years",
                                data_value=11.2,
                                low_ci=9.5,
                                high_ci=13.1,
                                data_value_type="Crude prevalence",
                                source="CDC PLACES",
                                updated_at=None,
                            )
                        ]
                    ),
                ]
            )
        ),
    )
    response = await geo_module.get_places_by_zip(request, "60654")
    payload = json.loads(response.body)
    assert response.status == 200
    assert payload["zip_code"] == "60654"
    assert payload["zcta"] == "60654"
    assert payload["year"] == 2025
    assert payload["measures"][0]["measure_id"] == "CSMOKING"


@pytest.mark.asyncio
async def test_geo_places_by_zip_invalid_year():
    request = types.SimpleNamespace(args={"year": "bad"}, ctx=types.SimpleNamespace(sa_session=FakeSession([])))
    with pytest.raises(InvalidUsage):
        await geo_module.get_places_by_zip(request, "60654")


@pytest.mark.asyncio
async def test_geo_places_by_zip_not_found():
    request = types.SimpleNamespace(
        args={},
        ctx=types.SimpleNamespace(
            sa_session=FakeSession(
                [
                    FakeResult(scalar_value=None),
                ]
            )
        ),
    )
    response = await geo_module.get_places_by_zip(request, "60654")
    assert response.status == 404


@pytest.mark.asyncio
async def test_geo_states_invalid_limit():
    request = types.SimpleNamespace(
        args={"limit": "bad"},
        ctx=types.SimpleNamespace(sa_session=FakeSession([])),
    )
    with pytest.raises(InvalidUsage):
        await geo_module.list_geo_states(request)


@pytest.mark.asyncio
async def test_geo_states_invalid_top_zip_limit():
    request = types.SimpleNamespace(
        args={"top_zip_limit": "bad"},
        ctx=types.SimpleNamespace(sa_session=FakeSession([])),
    )
    with pytest.raises(InvalidUsage):
        await geo_module.list_geo_states(request)


@pytest.mark.asyncio
async def test_geo_states_sorted_asc_with_limit_and_skip_missing_top_zip():
    state_rows = [
        MappingRow(
            state="TX",
            state_name="Texas",
            zip_count=10,
            city_count=5,
            population=1000,
            avg_lat=31.0,
            avg_long=-100.0,
        )
    ]
    top_zip_rows = [
        MappingRow(state=None, zip_code="00000", city="N/A", population=0, lat=0, long=0),
        MappingRow(state="TX", zip_code="73301", city="Austin", population=500, lat=30.3, long=-97.7),
    ]
    request = types.SimpleNamespace(
        args={"order": "asc", "limit": "1", "top_zip_limit": "2"},
        ctx=types.SimpleNamespace(
            sa_session=FakeSession(
                [
                    FakeResult(scalar_value=1),
                    FakeResult(rows=state_rows),
                    FakeResult(rows=top_zip_rows),
                ]
            )
        ),
    )
    response = await geo_module.list_geo_states(request)
    payload = json.loads(response.body)
    assert payload["total_states"] == 1
    assert payload["limit"] == 1
    assert payload["states"][0]["state"] == "TX"
    assert payload["states"][0]["top_zips"][0]["zip_code"] == "73301"


@pytest.mark.asyncio
async def test_geo_state_invalid_length():
    request = types.SimpleNamespace(args={}, ctx=types.SimpleNamespace(sa_session=FakeSession([])))
    with pytest.raises(InvalidUsage):
        await geo_module.get_top_cities_by_state(request, "ABC")


@pytest.mark.asyncio
async def test_geo_state_invalid_limit():
    request = types.SimpleNamespace(args={"limit": "bad"}, ctx=types.SimpleNamespace(sa_session=FakeSession([])))
    with pytest.raises(InvalidUsage):
        await geo_module.get_top_cities_by_state(request, "CA")
