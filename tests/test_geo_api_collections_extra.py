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
    response_payload = json.loads(response.body)
    assert response_payload["state"] == "TX"


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
    response_payload = json.loads(response.body)
    assert response.status == 200
    assert response_payload["zip_code"] == "60654"
    assert response_payload["zcta"] == "60654"
    assert response_payload["year"] == 2025
    assert response_payload["measures"][0]["measure_id"] == "CSMOKING"


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
async def test_geo_state_list_rejects_invalid_limit():
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
    response_payload = json.loads(response.body)
    assert response_payload["total_states"] == 1
    assert response_payload["limit"] == 1
    assert response_payload["states"][0]["state"] == "TX"
    assert response_payload["states"][0]["top_zips"][0]["zip_code"] == "73301"


@pytest.mark.asyncio
async def test_geo_state_invalid_length():
    request = types.SimpleNamespace(args={}, ctx=types.SimpleNamespace(sa_session=FakeSession([])))
    with pytest.raises(InvalidUsage):
        await geo_module.get_top_cities_by_state(request, "ABC")


@pytest.mark.asyncio
async def test_geo_state_lookup_rejects_invalid_limit():
    request = types.SimpleNamespace(args={"limit": "bad"}, ctx=types.SimpleNamespace(sa_session=FakeSession([])))
    with pytest.raises(InvalidUsage):
        await geo_module.get_top_cities_by_state(request, "CA")


def _programming_error(original_error):
    database_error = ProgrammingError("select", {}, None)
    database_error.orig = original_error
    return database_error


def test_geo_scalar_helpers_cover_empty_invalid_and_logged_values():
    assert geo_module._serialize_places_row(None) is None
    updated_at = geo_module.datetime(2026, 7, 22, 8, 30)
    assert geo_module._serialize_places_row(
        {"measure_id": "M1", "updated_at": updated_at}
    )["updated_at"] == "2026-07-22T08:30:00"
    assert geo_module._density_per_1000(None, 100) is None
    assert geo_module._density_per_1000(10, 0) is None
    assert geo_module._density_per_1000("bad", 100) is None

    warning_logger = types.SimpleNamespace(warning=Mock())
    request = types.SimpleNamespace(app=types.SimpleNamespace(logger=warning_logger))
    geo_module._log_geo_warning(request, "lookup failed: %s", "timeout")
    warning_logger.warning.assert_called_once_with(
        "lookup failed: %s",
        "timeout",
    )


@pytest.mark.asyncio
async def test_geo_optional_lookup_errors_fail_closed_and_other_errors_raise():
    missing_table = _programming_error(UndefinedTableError("missing"))
    other_error = _programming_error(RuntimeError("broken"))

    assert await geo_module._lookup_svi_profile(
        FakeSession([missing_table]),
        "60654",
    ) is None
    assert await geo_module._lookup_svi_profile(
        FakeSession([FakeResult(row=None)]),
        "60654",
    ) is None
    with pytest.raises(ProgrammingError):
        await geo_module._lookup_svi_profile(FakeSession([other_error]), "60654")

    with pytest.raises(ProgrammingError):
        await geo_module._execute_provider_count_stmt(
            FakeSession([other_error]),
            object(),
        )
    with pytest.raises(ProgrammingError):
        await geo_module._lookup_census_profile(
            FakeSession([other_error]),
            "60654",
        )


@pytest.mark.asyncio
async def test_census_profile_without_svi_preserves_density():
    census_row = MappingRow(total_population=2000)
    session = FakeSession(
        [
            FakeResult(row=census_row),
            FakeResult(row=None),
            FakeResult(scalar_value=50),
        ]
    )

    profile_map = await geo_module._lookup_census_profile(session, "60654")

    assert profile_map["provider_count"] == 50
    assert profile_map["provider_density_per_1000"] == 25.0
    assert profile_map["svi_overall"] is None


@pytest.mark.asyncio
async def test_places_year_honors_explicit_value_and_optional_schema():
    assert await geo_module._resolve_places_year(FakeSession([]), "60654", 2025) == 2025
    missing_column = _programming_error(UndefinedColumnError("missing"))
    assert await geo_module._resolve_places_year(
        FakeSession([missing_column]),
        "60654",
        None,
    ) is None
    with pytest.raises(ProgrammingError):
        await geo_module._resolve_places_year(
            FakeSession([_programming_error(RuntimeError("broken"))]),
            "60654",
            None,
        )


@pytest.mark.asyncio
async def test_geo_local_optional_schema_falls_back_but_other_errors_raise():
    optional_request = types.SimpleNamespace(
        ctx=types.SimpleNamespace(
            sa_session=FakeSession(
                [
                    FakeResult(row=None),
                    _programming_error(UndefinedTableError("missing")),
                    FakeResult(row=("60654", "41.9", "-87.6", "IL")),
                ]
            )
        ),
        app=types.SimpleNamespace(),
    )
    optional_response = await geo_module.get_geo(optional_request, "60654")
    assert json.loads(optional_response.body)["state"] == "IL"

    failing_request = types.SimpleNamespace(
        ctx=types.SimpleNamespace(
            sa_session=FakeSession(
                [
                    FakeResult(row=None),
                    _programming_error(RuntimeError("broken")),
                ]
            )
        ),
        app=types.SimpleNamespace(),
    )
    with pytest.raises(ProgrammingError):
        await geo_module.get_geo(failing_request, "60654")


@pytest.mark.asyncio
async def test_places_lookup_handles_filters_schema_errors_and_empty_rows():
    missing_table_request = types.SimpleNamespace(
        args={"year": "2025", "measure_id": "M1"},
        ctx=types.SimpleNamespace(
            sa_session=FakeSession(
                [_programming_error(UndefinedTableError("missing"))]
            )
        ),
    )
    missing_response = await geo_module.get_places_by_zip(
        missing_table_request,
        "60654",
    )
    assert missing_response.status == 404

    failing_request = types.SimpleNamespace(
        args={"year": "2025"},
        ctx=types.SimpleNamespace(
            sa_session=FakeSession(
                [_programming_error(RuntimeError("broken"))]
            )
        ),
    )
    with pytest.raises(ProgrammingError):
        await geo_module.get_places_by_zip(failing_request, "60654")

    empty_row_request = types.SimpleNamespace(
        args={"year": "2025"},
        ctx=types.SimpleNamespace(
            sa_session=FakeSession([FakeResult(rows=[None])])
        ),
    )
    empty_response = await geo_module.get_places_by_zip(
        empty_row_request,
        "60654",
    )
    assert empty_response.status == 404
