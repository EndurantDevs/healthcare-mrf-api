import sys
import types
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

import pytest
from click.testing import CliRunner

MODULE_PATH = Path(__file__).resolve().parents[1] / "process" / "geo_census_import.py"
MODULE_NAME = "geo_census_import_unit"


async def _dummy_ensure_database(_test_mode):
    return None


async def _dummy_get_http_client(*_args, **_kwargs):
    class _DummyClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
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

PROFILE_DATASET_ROWS_BY_NAME = {
    "acs_subject": {
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
    },
    "acs_profile": {
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
    },
    "acs_housing": {"60654": {"total_housing_units": 18505}},
    "decennial_dhc": {
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
    },
    "cbp": {
        "60654": {
            "total_employer_establishments": 2224,
            "business_employment": 52832,
            "business_payroll_annual_k": 8723456,
        },
        "99999": {"total_employer_establishments": 111},
    },
}

PERSISTED_PROFILES_BY_ZIP = {
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
    },
    "99999": {
        "zip_code": "99999",
        "total_population": 1,
        "updated_at": None,
    },
}


class _DummyClient:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FakeExcludedColumns:
    def __getattr__(self, column_name):
        return f"excluded:{column_name}"


class _FakeInsertStatement:
    def __init__(self):
        self.excluded = _FakeExcludedColumns()
        self.rows_to_insert = None
        self.conflict_options = None
        self.status_calls = 0

    def values(self, rows_to_insert):
        self.rows_to_insert = list(rows_to_insert)
        return self

    def on_conflict_do_update(self, **conflict_options):
        self.conflict_options = conflict_options
        return self

    async def status(self):
        self.status_calls += 1


@pytest.mark.parametrize(
    "value,expected",
    [
        ("60654", "60654"),
        ("ZCTA5 60654", "60654"),
        ("ZIP 60654 (Chicago, IL)", "60654"),
        ("060654", "60654"),
        ("1a2b3c4d5", "12345"),
        ("not-a-zip", None),
        ("   ", None),
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
        ("not-a-number", None),
        ("   ", None),
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
        ("not-a-number", None),
        ("", None),
    ],
)
def test_to_float_handles_suppressed_values(value, expected):
    assert geo_census._to_float(value) == expected


@pytest.mark.parametrize(
    "resolver_name,raw_value,default_value,expected_value",
    [
        ("_resolve_int_env", None, 7, 7),
        ("_resolve_int_env", "11", 7, 11),
        ("_resolve_int_env", "invalid", 7, 7),
        ("_resolve_float_env", None, 1.5, 1.5),
        ("_resolve_float_env", "2.5", 1.5, 2.5),
        ("_resolve_float_env", "invalid", 1.5, 1.5),
    ],
)
def test_environment_number_resolvers_use_valid_values_or_defaults(
    monkeypatch,
    resolver_name,
    raw_value,
    default_value,
    expected_value,
):
    environment_name = "HLTHPRT_TEST_CENSUS_NUMBER"
    if raw_value is None:
        monkeypatch.delenv(environment_name, raising=False)
    else:
        monkeypatch.setenv(environment_name, raw_value)

    resolver = getattr(geo_census, resolver_name)

    assert resolver(environment_name, default_value) == expected_value


@pytest.mark.parametrize(
    "numerator,denominator,expected",
    [
        (1, 4, 25.0),
        (None, 4, None),
        (1, None, None),
        (1, 0, None),
        ("invalid", 4, None),
        (1, "invalid", None),
    ],
)
def test_percentage_conversion_handles_missing_zero_and_invalid_inputs(
    numerator,
    denominator,
    expected,
):
    assert geo_census._to_pct(numerator, denominator) == expected


@pytest.mark.asyncio
async def test_collect_profile_map_merges_and_filters_cbp_non_zcta(monkeypatch):
    """Verify collect profile map merges and filters cbp non zcta."""
    monkeypatch.delenv("HLTHPRT_CENSUS_API_KEY", raising=False)

    async def _fake_get_http_client(*_args, **_kwargs):
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
        """Support the fake fetch rows test fixture."""
        return PROFILE_DATASET_ROWS_BY_NAME.get(spec.name, {})

    monkeypatch.setattr(geo_census, "get_http_client", _fake_get_http_client)
    monkeypatch.setattr(geo_census, "_fetch_dataset_rows", _fake_fetch_rows)

    profile_map = await geo_census._collect_profile_map(test_mode=False)

    assert sorted(profile_map.keys()) == ["60654"]
    profile = profile_map["60654"]
    expected_fields_by_name = {
        **PROFILE_DATASET_ROWS_BY_NAME["acs_subject"]["60654"],
        **PROFILE_DATASET_ROWS_BY_NAME["acs_profile"]["60654"],
        **PROFILE_DATASET_ROWS_BY_NAME["acs_housing"]["60654"],
        **PROFILE_DATASET_ROWS_BY_NAME["decennial_dhc"]["60654"],
        **PROFILE_DATASET_ROWS_BY_NAME["cbp"]["60654"],
        "hispanic_or_latino_pct": 1469 / 23890 * 100,
        "race_white_alone_pct": 17450 / 23890 * 100,
        "race_black_or_african_american_alone_pct": 1800 / 23890 * 100,
        "race_american_indian_and_alaska_native_alone_pct": 120 / 23890 * 100,
        "race_asian_alone_pct": 3050 / 23890 * 100,
        "race_native_hawaiian_and_other_pacific_islander_alone_pct": 25 / 23890 * 100,
        "race_some_other_race_alone_pct": 690 / 23890 * 100,
        "race_two_or_more_races_pct": 755 / 23890 * 100,
    }
    observed_fields_by_name = {
        field_name: profile[field_name] for field_name in expected_fields_by_name
    }
    assert observed_fields_by_name == pytest.approx(expected_fields_by_name)


@pytest.mark.asyncio
async def test_collect_profile_map_accepts_configured_api_key(monkeypatch):
    monkeypatch.setenv("HLTHPRT_CENSUS_API_KEY", "synthetic-key")
    monkeypatch.setattr(geo_census, "_dataset_specs", lambda *_years: ())

    async def _fake_get_http_client(*_args, **_kwargs):
        return _DummyClient()

    monkeypatch.setattr(geo_census, "get_http_client", _fake_get_http_client)

    assert await geo_census._collect_profile_map(test_mode=True) == {}


@pytest.mark.asyncio
async def test_flush_rows_skips_empty_batches_and_upserts_nonempty_batches(
    monkeypatch,
):
    insert_statement = _FakeInsertStatement()
    insert_calls = []

    def _fake_insert(table):
        insert_calls.append(table)
        return insert_statement

    monkeypatch.setattr(geo_census.db, "insert", _fake_insert)

    await geo_census._flush_rows([])
    buffered_rows = [{"zip_code": "01234", "total_population": 10}]
    await geo_census._flush_rows(buffered_rows)

    table = geo_census.GeoZipCensusProfile.__table__
    expected_update_column_names = {
        column.name for column in table.c if not column.primary_key
    }
    assert insert_calls == [table]
    assert insert_statement.rows_to_insert == [
        {"zip_code": "01234", "total_population": 10}
    ]
    assert insert_statement.conflict_options["index_elements"] == (
        geo_census.GeoZipCensusProfile.__my_index_elements__
    )
    assert set(insert_statement.conflict_options["set_"]) == (
        expected_update_column_names
    )
    assert insert_statement.status_calls == 1
    assert buffered_rows == []


@pytest.mark.asyncio
async def test_ensure_profile_columns_emits_every_declarative_ddl(monkeypatch):
    statements = []

    async def _capture_status(statement):
        statements.append(statement)

    monkeypatch.setattr(geo_census.db, "status", _capture_status)

    await geo_census._ensure_profile_columns("synthetic_schema")

    assert len(statements) == len(geo_census.PROFILE_COLUMN_DDLS)
    assert statements[0] == (
        "ALTER TABLE synthetic_schema.geo_zip_census_profile "
        f"ADD COLUMN IF NOT EXISTS {geo_census.PROFILE_COLUMN_DDLS[0]};"
    )
    assert statements[-1].endswith(
        f"ADD COLUMN IF NOT EXISTS {geo_census.PROFILE_COLUMN_DDLS[-1]};"
    )


@pytest.mark.asyncio
async def test_load_geo_census_lookup_truncates_and_writes(monkeypatch):
    captured_rows_by_name = {"rows": []}

    async def _fake_collect(*_args, **_kwargs):
        return PERSISTED_PROFILES_BY_ZIP

    async def _fake_ensure_database(_test_mode):
        return None

    async def _fake_create_table(*_args, **_kwargs):
        return None

    async def _fake_status(_statement, *args, **kwargs):
        return None

    async def _fake_flush(rows):
        captured_rows_by_name["rows"].extend(rows)
        rows.clear()

    monkeypatch.setattr(geo_census, "_collect_profile_map", _fake_collect)
    monkeypatch.setattr(geo_census, "ensure_database", _fake_ensure_database)
    monkeypatch.setattr(geo_census.db, "create_table", _fake_create_table)
    monkeypatch.setattr(geo_census.db, "transaction", _DummyClient)
    monkeypatch.setattr(geo_census.db, "status", _fake_status)
    monkeypatch.setattr(geo_census, "_flush_rows", _fake_flush)
    monkeypatch.setattr(geo_census, "IMPORT_BATCH_SIZE", 2)

    inserted = await geo_census.load_geo_census_lookup(test_mode=False)

    assert inserted == 2
    assert len(captured_rows_by_name["rows"]) == 2
    assert captured_rows_by_name["rows"][0]["zip_code"] == "60654"
    assert (
        captured_rows_by_name["rows"][0]["total_employer_establishments"]
        == 2224
    )


def test_geo_census_command_forwards_test_mode(monkeypatch):
    requested_test_modes = []

    async def _fake_load_geo_census_lookup(test_mode=False):
        requested_test_modes.append(test_mode)
        return 0

    monkeypatch.setattr(
        geo_census,
        "load_geo_census_lookup",
        _fake_load_geo_census_lookup,
    )

    command_result = CliRunner().invoke(geo_census.geo_census_lookup, ["--test"])

    assert command_result.exit_code == 0
    assert requested_test_modes == [True]
