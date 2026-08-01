# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from types import SimpleNamespace

import pytest

from api.ptg2_geo_policy import (
    is_provider_address_geo_capability_available,
    provider_address_location_filter_sql,
)


class _CapabilityResult:
    def __init__(self, fields):
        self._row = SimpleNamespace(_mapping=fields)

    def first(self):
        return self._row


class _CapabilitySession:
    def __init__(self, fields=None, *, error=None):
        self.fields = fields
        self.error = error
        self.execute_args = None
        self.nested_transaction_calls = 0

    def begin_nested(self):
        self.nested_transaction_calls += 1
        return _NestedTransaction()

    async def execute(self, *args):
        self.execute_args = args
        if self.error is not None:
            raise self.error
        return _CapabilityResult(self.fields)


class _NestedTransaction:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return False


def _available_capabilities():
    return {
        "has_geo_zip": True,
        "has_geo_zip_columns": True,
        "has_zip_state": True,
        "has_zip_state_columns": True,
        "has_zcta": True,
        "has_zcta_columns": True,
        "has_spatial_functions": True,
        "has_zcta_geometry_contract": True,
        "has_zcta_zip_index": True,
    }


@pytest.mark.asyncio
async def test_geo_capability_probe_uses_configured_schema():
    session = _CapabilitySession(_available_capabilities())

    assert await is_provider_address_geo_capability_available(
        session,
        schema_name="tenant_data",
        reference_schema="geo_reference",
    )
    assert session.execute_args[1] == {
        "geo_schema": "tenant_data",
        "geo_zip_table": "tenant_data.geo_zip_lookup",
        "reference_schema": "geo_reference",
        "reference_zip_state_table": "geo_reference.zip_state",
        "reference_zcta_table": "geo_reference.zcta5",
    }
    capability_sql = str(session.execute_args[0])
    assert "st_covers(geometry,geometry)" in capability_sql
    assert "st_dwithin(geography,geography,double precision,boolean)" in (
        capability_sql
    )
    assert "postgis_typmod_srid(attribute.atttypmod) = 4269" in capability_sql


@pytest.mark.asyncio
async def test_geo_capability_probe_fails_closed_inside_nested_transaction():
    session = _CapabilitySession(error=RuntimeError("catalog unavailable"))

    assert not await is_provider_address_geo_capability_available(
        session,
        schema_name="tenant_data",
    )
    assert session.nested_transaction_calls == 1


@pytest.mark.asyncio
async def test_geo_capability_probe_rejects_non_session_test_double():
    assert not await is_provider_address_geo_capability_available(
        object(),
        schema_name="tenant_data",
    )


@pytest.mark.asyncio
async def test_geo_capability_probe_fails_closed_for_non_mapping_result():
    assert not await is_provider_address_geo_capability_available(
        _CapabilitySession([]),
        schema_name="tenant_data",
    )


@pytest.mark.asyncio
async def test_geo_capability_probe_rejects_incomplete_reference_schema():
    capabilities = _available_capabilities()
    capabilities["has_zcta_columns"] = False

    assert not await is_provider_address_geo_capability_available(
        _CapabilitySession(capabilities),
        schema_name="tenant_data",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "missing_capability",
    [
        "has_spatial_functions",
        "has_zcta_geometry_contract",
        "has_zcta_zip_index",
    ],
)
async def test_geo_capability_probe_rejects_incomplete_spatial_contract(
    missing_capability,
):
    capabilities = _available_capabilities()
    capabilities[missing_capability] = False

    assert not await is_provider_address_geo_capability_available(
        _CapabilitySession(capabilities),
        schema_name="tenant_data",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("schema_name", "reference_schema"),
    [
        ("", "tiger"),
        ("tenant.data", "tiger"),
        ("tenant data", "tiger"),
        ("tenant;data", "tiger"),
        ('tenant"data', "tiger"),
        ("tenant_data", "geo.reference"),
    ],
)
async def test_geo_capability_probe_rejects_unsafe_schema(
    schema_name,
    reference_schema,
):
    session = _CapabilitySession(_available_capabilities())

    assert not await is_provider_address_geo_capability_available(
        session,
        schema_name=schema_name,
        reference_schema=reference_schema,
    )
    assert session.execute_args is None


def test_exact_zip_without_radius_only_allows_rows_without_points():
    filter_sql = provider_address_location_filter_sql(
        "addr",
        schema_name="tenant_data",
        exact_zip_predicate="addr.zip5 = :zip5",
        radius_predicates=[],
    )

    assert "addr.lat IS NULL AND addr.long IS NULL" in filter_sql
    assert "COALESCE(addr.country_code, '')" in filter_sql
    assert "ST_Covers" not in filter_sql


def test_location_filter_without_zip_or_radius_returns_none():
    assert (
        provider_address_location_filter_sql(
            "addr",
            schema_name="tenant_data",
            exact_zip_predicate=None,
            radius_predicates=[],
        )
        is None
    )


@pytest.mark.parametrize(
    ("address_alias", "schema_name"),
    [("addr;drop", "tenant_data"), ("addr", "tenant.data")],
)
def test_geo_policy_rejects_unsafe_sql_identifiers(
    address_alias,
    schema_name,
):
    with pytest.raises(ValueError, match="simple PostgreSQL identifier"):
        provider_address_location_filter_sql(
            address_alias,
            schema_name=schema_name,
            exact_zip_predicate="TRUE",
            radius_predicates=[],
        )
