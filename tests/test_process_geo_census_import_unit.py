import sys
import types
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

import pytest

MODULE_PATH = Path(__file__).resolve().parents[1] / "process" / "geo_census_import.py"
MODULE_NAME = "geo_census_import_unit"


async def _dummy_ensure_database(_test_mode):
    return None


async def _dummy_get_http_client(*_args, **_kwargs):
    class _DummyClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):  # noqa: ANN001
            return False

    return _DummyClient()


def _load_geo_census_module():
    old_process = sys.modules.get("process")
    old_process_ext = sys.modules.get("process.ext")
    old_process_ext_utils = sys.modules.get("process.ext.utils")

    process_pkg = types.ModuleType("process")
    process_pkg.__path__ = [str(MODULE_PATH.parent)]
    ext_pkg = types.ModuleType("process.ext")
    ext_pkg.__path__ = [str(MODULE_PATH.parent / "ext")]
    utils_pkg = types.ModuleType("process.ext.utils")
    utils_pkg.ensure_database = _dummy_ensure_database
    utils_pkg.get_http_client = _dummy_get_http_client

    sys.modules["process"] = process_pkg
    sys.modules["process.ext"] = ext_pkg
    sys.modules["process.ext.utils"] = utils_pkg

    try:
        module_spec = spec_from_file_location(MODULE_NAME, MODULE_PATH)
        module = module_from_spec(module_spec)
        sys.modules[MODULE_NAME] = module
        module_spec.loader.exec_module(module)
        return module
    finally:
        if old_process is None:
            sys.modules.pop("process", None)
        else:
            sys.modules["process"] = old_process
        if old_process_ext is None:
            sys.modules.pop("process.ext", None)
        else:
            sys.modules["process.ext"] = old_process_ext
        if old_process_ext_utils is None:
            sys.modules.pop("process.ext.utils", None)
        else:
            sys.modules["process.ext.utils"] = old_process_ext_utils


geo_census = _load_geo_census_module()


class _DummyClient:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):  # noqa: ANN001
        return False


@pytest.mark.parametrize(
    "value,expected",
    [
        ("60654", "60654"),
        ("ZCTA5 60654", "60654"),
        ("ZIP 60654 (Chicago, IL)", "60654"),
        ("060654", "60654"),
        (None, None),
        ("", None),
    ],
)
def test_normalize_zip(value, expected):
    assert geo_census._normalize_zip(value) == expected


@pytest.mark.parametrize(
    "value,expected",
    [
        ("2224", 2224),
        ("2,224", 2224),
        ("2224.0", 2224),
        ("-666666666", None),
        ("N", None),
        ("", None),
    ],
)
def test_to_int_handles_suppressed_values(value, expected):
    assert geo_census._to_int(value) == expected


@pytest.mark.parametrize(
    "value,expected",
    [
        ("85.1", 85.1),
        ("91.5", 91.5),
        ("-666666666", None),
        ("(X)", None),
        ("", None),
    ],
)
def test_to_float_handles_suppressed_values(value, expected):
    assert geo_census._to_float(value) == expected


@pytest.mark.asyncio
async def test_collect_profile_map_merges_and_filters_cbp_non_zcta(monkeypatch):
    async def _fake_get_http_client(*_args, **_kwargs):  # noqa: ANN002
        return _DummyClient()

    async def _fake_fetch_rows(
        _client,
        spec,
        _api_key,
        _timeout_seconds,
        _retries,
        _retry_delay,
        _test_mode,
        _test_row_limit,
    ):
        if spec.name == "acs_subject":
            return {
                "60654": {
                    "median_household_income": 147357,
                    "bachelors_degree_or_higher_pct": 91.5,
                    "without_health_insurance_pct": 3.4,
                    "poverty_rate_pct": 6.1,
                    "commute_mode_drove_alone_pct": 17.6,
                    "commute_mode_carpool_pct": 1.2,
                    "commute_mode_public_transit_pct": 10.6,
                    "commute_mode_walked_pct": 29.4,
                    "commute_mode_worked_from_home_pct": 34.8,
                    "commute_mean_minutes": 28.4,
                    "broadband_access_pct": 96.2,
                }
            }
        if spec.name == "acs_profile":
            return {
                "60654": {
                    "employment_rate_pct": 85.1,
                    "total_households": 16968,
                    "median_age": 35.8,
                    "unemployment_rate_pct": 4.7,
                    "labor_force_participation_pct": 79.8,
                    "vacancy_rate_pct": 8.3,
                    "median_home_value": 575000,
                    "median_gross_rent": 2450,
                    "acs_white_alone_pct": 74.1,
                    "acs_black_or_african_american_alone_pct": 5.3,
                    "acs_american_indian_and_alaska_native_alone_pct": 0.2,
                    "acs_asian_alone_pct": 13.7,
                    "acs_native_hawaiian_and_other_pacific_islander_alone_pct": 0.1,
                    "acs_some_other_race_alone_pct": 2.9,
                    "acs_two_or_more_races_pct": 3.7,
                    "acs_hispanic_or_latino_pct": 8.2,
                }
            }
        if spec.name == "acs_housing":
            return {"60654": {"total_housing_units": 18505}}
        if spec.name == "decennial_dhc":
            return {
                "60654": {
                    "total_population": 23890,
                    "hispanic_or_latino": 1469,
                    "race_white_alone": 17450,
                    "race_black_or_african_american_alone": 1800,
                    "race_american_indian_and_alaska_native_alone": 120,
                    "race_asian_alone": 3050,
                    "race_native_hawaiian_and_other_pacific_islander_alone": 25,
                    "race_some_other_race_alone": 690,
                    "race_two_or_more_races": 755,
                }
            }
        if spec.name == "cbp":
            return {
                "60654": {
                    "total_employer_establishments": 2224,
                    "business_employment": 52832,
                    "business_payroll_annual_k": 8723456,
                },
                "99999": {"total_employer_establishments": 111},
            }
        return {}

    monkeypatch.setattr(geo_census, "get_http_client", _fake_get_http_client)
    monkeypatch.setattr(geo_census, "_fetch_dataset_rows", _fake_fetch_rows)

    profile_map = await geo_census._collect_profile_map(test_mode=False)

    assert sorted(profile_map.keys()) == ["60654"]
    profile = profile_map["60654"]
    assert profile["total_population"] == 23890
    assert profile["median_household_income"] == 147357
    assert profile["bachelors_degree_or_higher_pct"] == 91.5
    assert profile["employment_rate_pct"] == 85.1
    assert profile["total_housing_units"] == 18505
    assert profile["without_health_insurance_pct"] == 3.4
    assert profile["total_employer_establishments"] == 2224
    assert profile["business_employment"] == 52832
    assert profile["business_payroll_annual_k"] == 8723456
    assert profile["total_households"] == 16968
    assert profile["hispanic_or_latino"] == 1469
    assert profile["hispanic_or_latino_pct"] == pytest.approx(6.149, rel=1e-4)
    assert profile["poverty_rate_pct"] == 6.1
    assert profile["median_age"] == 35.8
    assert profile["unemployment_rate_pct"] == 4.7
    assert profile["labor_force_participation_pct"] == 79.8
    assert profile["vacancy_rate_pct"] == 8.3
    assert profile["median_home_value"] == 575000
    assert profile["median_gross_rent"] == 2450
    assert profile["commute_mean_minutes"] == 28.4
    assert profile["commute_mode_drove_alone_pct"] == 17.6
    assert profile["commute_mode_carpool_pct"] == 1.2
    assert profile["commute_mode_public_transit_pct"] == 10.6
    assert profile["commute_mode_walked_pct"] == 29.4
    assert profile["commute_mode_worked_from_home_pct"] == 34.8
    assert profile["broadband_access_pct"] == 96.2
    assert profile["race_white_alone"] == 17450
    assert profile["race_white_alone_pct"] == pytest.approx(73.044, rel=1e-4)
    assert profile["race_two_or_more_races"] == 755
    assert profile["race_two_or_more_races_pct"] == pytest.approx(3.1603, rel=1e-4)
    assert profile["acs_white_alone_pct"] == 74.1
    assert profile["acs_hispanic_or_latino_pct"] == 8.2


@pytest.mark.asyncio
async def test_load_geo_census_lookup_truncates_and_writes(monkeypatch):
    captured = {"rows": []}

    async def _fake_collect(*_args, **_kwargs):  # noqa: ANN002
        return {
            "60654": {
                "zip_code": "60654",
                "total_population": 23890,
                "median_household_income": 147357,
                "bachelors_degree_or_higher_pct": 91.5,
                "employment_rate_pct": 85.1,
                "total_housing_units": 18505,
                "without_health_insurance_pct": 3.4,
                "total_employer_establishments": 2224,
                "total_households": 16968,
                "hispanic_or_latino": 1469,
                "updated_at": None,
            }
        }

    async def _fake_ensure_database(_test_mode):
        return None

    async def _fake_create_table(*_args, **_kwargs):
        return None

    class _DummyTransaction:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):  # noqa: ANN001
            return False

    async def _fake_status(_statement, *args, **kwargs):  # noqa: ANN001, ARG001
        return None

    async def _fake_flush(rows):
        captured["rows"].extend(rows)
        rows.clear()

    monkeypatch.setattr(geo_census, "_collect_profile_map", _fake_collect)
    monkeypatch.setattr(geo_census, "ensure_database", _fake_ensure_database)
    monkeypatch.setattr(geo_census.db, "create_table", _fake_create_table)
    monkeypatch.setattr(geo_census.db, "transaction", lambda: _DummyTransaction())
    monkeypatch.setattr(geo_census.db, "status", _fake_status)
    monkeypatch.setattr(geo_census, "_flush_rows", _fake_flush)

    inserted = await geo_census.load_geo_census_lookup(test_mode=False)

    assert inserted == 1
    assert len(captured["rows"]) == 1
    assert captured["rows"][0]["zip_code"] == "60654"
    assert captured["rows"][0]["total_employer_establishments"] == 2224
