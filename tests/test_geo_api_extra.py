# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from tests.geo_api_extra_support import (
    CENSUS_PROFILE_VALUES,
    FakeResult,
    FakeSession,
    InvalidUsage,
    MappingRow,
    Mock,
    ProgrammingError,
    UndefinedColumnError,
    UndefinedTableError,
    geo_module,
    json,
    pytest,
    types,
)


@pytest.mark.asyncio
async def test_geo_index_handler():
    handler = next(route.handler for route in geo_module.blueprint._future_routes if route.uri == "/get")
    request = types.SimpleNamespace(
        args={"zip_code": "60601", "lat": "41.0", "long": "-87.0"},
        app=types.SimpleNamespace(config={"RELEASE": "test", "ENVIRONMENT": "dev"}),
    )
    response = await handler(request)
    response_payload = json.loads(response.body)
    assert response_payload["release"] == "test"
    assert response_payload["environment"] == "dev"
    assert "date" in response_payload
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
    response_payload = json.loads(response.body)
    assert response_payload["zip_code"] == "12345"
    assert response_payload["lat"] == 41.1
    assert response_payload["city"] == "Chicago"
    assert response_payload["census_profile"] is None


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
    response_payload = json.loads(response.body)
    assert response.status == 200
    assert response_payload["zip_code"] == "60654"
    assert response_payload["census_profile"] is None


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
    response_payload = json.loads(response.body)
    assert response.status == 200
    assert response_payload["zip_code"] == "60654"
    assert response_payload["census_profile"] is None


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
    response_payload = json.loads(response.body)
    assert response.status == 200
    assert response_payload["zip_code"] == "07666"
    assert response_payload["census_profile"] is None


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
async def test_lookup_provider_count_query_avoids_coalesce(monkeypatch):
    monkeypatch.setenv("HLTHPRT_ADDRESS_SERVING_SOURCE", "legacy")

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
async def test_lookup_provider_count_uses_entity_address_by_default():
    class CaptureSession:
        def __init__(self):
            self.last_stmt = None

        async def execute(self, stmt, *_args, **_kwargs):
            self.last_stmt = stmt
            return FakeResult(scalar_value=84)

    session = CaptureSession()
    value = await geo_module._lookup_provider_count(session, "60654")
    assert value == 84
    compiled = str(session.last_stmt).lower()
    assert "entity_address_unified" in compiled
    assert "npi_address" not in compiled
    assert "coalesce" in compiled
    assert "zip5" in compiled


@pytest.mark.asyncio
async def test_lookup_provider_count_falls_back_when_unified_missing(monkeypatch):
    monkeypatch.delenv("HLTHPRT_ADDRESS_SERVING_SOURCE", raising=False)
    error = ProgrammingError("select", {}, None)
    error.orig = UndefinedTableError("entity_address_unified missing")
    session = FakeSession([error, FakeResult(scalar_value=21)])

    value = await geo_module._lookup_provider_count(session, "60654")

    assert value == 21


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
    response_payload = json.loads(response.body)
    assert response_payload["zip_code"] == "11111"
    assert response_payload["lat"] is None
    assert response_payload["long"] is None
    assert response_payload["census_profile"] is None


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
    response_payload = json.loads(response.body)
    assert response_payload == {
        "zip_code": "22222",
        "lat": 41.0,
        "long": -87.0,
        "state": "IL",
        "census_profile": None,
    }


@pytest.mark.asyncio
async def test_geo_by_zip_with_census_profile():
    """Verify geo by zip with census profile."""
    census_row = MappingRow(**CENSUS_PROFILE_VALUES)
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
    response_payload = json.loads(response.body)
    assert response_payload["zip_code"] == "60654"
    assert response_payload["census_profile"]["median_household_income"] == 147357
    assert response_payload["census_profile"]["total_employer_establishments"] == 2224
    assert response_payload["census_profile"]["business_employment"] == 52832
    assert response_payload["census_profile"]["svi_overall"] == 0.402
    assert response_payload["census_profile"]["provider_count"] == 800
    assert "pharmacy_count" not in response_payload["census_profile"]
    assert "pharmacy_density_per_1000" not in response_payload["census_profile"]
    assert response_payload["census_profile"]["provider_density_per_1000"] == pytest.approx(33.4868, rel=1e-4)
