# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import json
import sys
import types
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from unittest.mock import Mock

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
CENSUS_PROFILE_VALUES = {
    "total_population": 23890,
    "median_household_income": 147357,
    "bachelors_degree_or_higher_pct": 91.5,
    "employment_rate_pct": 85.1,
    "total_housing_units": 18505,
    "without_health_insurance_pct": 3.4,
    "total_employer_establishments": 2224,
    "business_employment": 52832,
    "business_payroll_annual_k": 8723456,
    "total_households": 16968,
    "hispanic_or_latino": 1469,
    "poverty_rate_pct": 6.1,
    "median_age": 35.8,
    "unemployment_rate_pct": 4.7,
    "labor_force_participation_pct": 79.8,
    "vacancy_rate_pct": 8.3,
    "median_home_value": 575000,
    "median_gross_rent": 2450,
    "commute_mean_minutes": 28.4,
    "commute_mode_drove_alone_pct": 52.0,
    "commute_mode_carpool_pct": 6.0,
    "commute_mode_public_transit_pct": 22.0,
    "commute_mode_walked_pct": 12.0,
    "commute_mode_worked_from_home_pct": 8.0,
    "broadband_access_pct": 96.2,
    "race_white_alone": 17450,
    "race_black_or_african_american_alone": 1800,
    "race_american_indian_and_alaska_native_alone": 120,
    "race_asian_alone": 3050,
    "race_native_hawaiian_and_other_pacific_islander_alone": 25,
    "race_some_other_race_alone": 690,
    "race_two_or_more_races": 755,
    "race_white_alone_pct": 73.04,
    "race_black_or_african_american_alone_pct": 7.53,
    "race_american_indian_and_alaska_native_alone_pct": 0.50,
    "race_asian_alone_pct": 12.77,
    "race_native_hawaiian_and_other_pacific_islander_alone_pct": 0.10,
    "race_some_other_race_alone_pct": 2.89,
    "race_two_or_more_races_pct": 3.16,
    "acs_white_alone_pct": 74.1,
    "acs_black_or_african_american_alone_pct": 5.3,
    "acs_american_indian_and_alaska_native_alone_pct": 0.2,
    "acs_asian_alone_pct": 13.7,
    "acs_native_hawaiian_and_other_pacific_islander_alone_pct": 0.1,
    "acs_some_other_race_alone_pct": 2.9,
    "acs_two_or_more_races_pct": 3.7,
    "acs_hispanic_or_latino_pct": 8.2,
}


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
