# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sanic.exceptions import ServiceUnavailable

from api import ptg2_geo_policy as policy
from api.ptg2_geo_policy import (
    is_provider_address_geo_capability_available,
    provider_address_location_filter_sql,
)
from api.ptg2_geo_projection import _PROJECTION_DEPENDENCIES


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
    assert "public.st_covers(public.geometry,public.geometry)" in capability_sql
    assert "public.st_dwithin(public.geography,public.geography,double precision,boolean)" in (capability_sql)
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


def test_exact_zip_without_radius_allows_missing_or_coherent_points():
    filter_sql = provider_address_location_filter_sql(
        "addr",
        schema_name="tenant_data",
        exact_zip_predicate="addr.zip5 = :zip5",
        radius_predicates=[],
    )

    assert "addr.lat IS NULL AND addr.long IS NULL" in filter_sql
    assert "COALESCE(addr.country_code, '')" in filter_sql
    assert "ST_Covers" in filter_sql


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


@pytest.mark.parametrize("zcta_alias", [None, "held_zcta"])
def test_point_policy_qualifies_native_extension_calls(zcta_alias):
    from api.ptg2_geo_policy import provider_address_point_coherence_sql

    statement = provider_address_point_coherence_sql("addr", use_projection=False, zcta_alias=zcta_alias)
    for name in ("ST_Covers", "ST_SetSRID", "ST_MakePoint"):
        assert "public." + name + "(" in statement
        assert statement.replace("public." + name + "(", "").find(name + "(") == -1
    assert "4269" in statement and "BETWEEN -90.0 AND 90.0" in statement
    assert "BETWEEN -180.0 AND 180.0" in statement


async def test_capability_probe_qualifies_complete_native_extension_contract():
    session = _CapabilitySession(_available_capabilities())
    assert await is_provider_address_geo_capability_available(session, schema_name="tenant_data")
    statement = str(session.execute_args[0])
    for signature in (
        "public.geometry",
        "public.geography",
        "public.st_covers(public.geometry,public.geometry)",
        "public.st_setsrid(public.geometry,integer)",
        "public.st_makepoint(double precision,double precision)",
        "public.st_dwithin(public.geography,public.geography,double precision,boolean)",
        "public.postgis_typmod_srid(integer)",
        "public.postgis_typmod_type(integer)",
    ):
        assert "'" + signature + "'" in statement
    assert "public.postgis_typmod_srid(attribute.atttypmod) = 4269" in statement
    assert "public.postgis_typmod_type(attribute.atttypmod) IN" in statement


def _selected_bindings(*, legacy=False, tiger_schema=None):
    selected_by_name = {
        f"{schema or 'mrf'}.{name}": {"schema_name": schema or "mrf", "table_name": name}
        for schema, name in _PROJECTION_DEPENDENCIES
    }
    for name in ("zip_state", "zcta5"):
        selected_by_name["tiger." + name]["schema_name"] = tiger_schema or "reference_family_archive_" + "a" * 32
    for name in ("npi_address", "mrf_address", "doctor_clinician_address", "geo_zip_lookup"):
        selected_by_name["mrf." + name]["schema_name"] = "old_mutable_copy"
    if legacy:
        for binding in selected_by_name.values():
            binding.update(relation_oid=object(), relfilenode=None)
    return selected_by_name


class _ReferenceSession:
    def __init__(self, bindings=None, *, installed=True, retained=False, failure=None):
        self.bindings, self.installed, self.retained = bindings, installed, retained
        self.statements = []
        self.execute = AsyncMock(side_effect=failure)

    async def scalar(self, statement, parameters=None):
        query = str(statement)
        self.statements.append((query, parameters))
        if "to_regclass(:state)" in query:
            assert parameters == {"state": '"mrf"."entity_address_geo_assurance_state"'}
            return self.installed
        if "active_dependency_bindings" in query:
            return self.bindings
        assert "pg_catalog.pg_inherits edge" in query
        return self.retained


@pytest.mark.parametrize("legacy", [False, True])
async def test_reference_context_uses_selected_tiger_and_current_mutable_inputs(legacy):
    original = _selected_bindings(legacy=legacy)
    session = _ReferenceSession(original)
    async with policy.provider_address_geo_references(session, schema_name="mrf") as effective:
        for name in ("npi_address", "mrf_address", "doctor_clinician_address", "geo_zip_lookup"):
            assert effective["mrf." + name] == {"schema_name": "mrf", "table_name": name}
        assert original["mrf.npi_address"]["schema_name"] == "old_mutable_copy"
        original["tiger.zip_state"]["schema_name"] = "changed_after_selection"
        identity = policy.provider_address_identity_coherence_sql("addr", schema_name="mrf", use_projection=False)
        point = policy.provider_address_point_coherence_sql("addr", schema_name="mrf", use_projection=False)
        held = '"reference_family_archive_' + "a" * 32 + '"'
        assert f'FROM ONLY {held}."zip_state"' in identity
        assert f'FROM ONLY {held}."zcta5"' in point
        assert "FROM mrf.geo_zip_lookup" in identity and "old_mutable_copy" not in identity
        assert "changed_after_selection" not in identity
        lock = str(session.execute.await_args.args[0])
        for name in ("npi_address", "mrf_address", "doctor_clinician_address", "geo_zip_lookup"):
            assert 'ONLY "mrf"."' + name + '"' in lock
        assert held in lock and "ACCESS SHARE" in lock
        probe = _CapabilitySession(_available_capabilities())
        assert await is_provider_address_geo_capability_available(probe, schema_name="mrf")
        assert probe.execute_args[1]["reference_schema"] == "reference_family_archive_" + "a" * 32
    assert policy._REFERENCE_READ.get() is None
    assert "FROM tiger.zip_state" in policy.provider_address_identity_coherence_sql("addr", schema_name="mrf")


@pytest.mark.parametrize("damage", ["missing", "duplicate", "unsafe", "unknown", "mixed_tiger", "wrong_tiger_name"])
async def test_reference_context_refuses_invalid_declared_selection(damage):
    bindings = _selected_bindings()
    match damage:
        case "missing":
            del bindings["mrf.npi_address"]
        case "duplicate":
            bindings["mrf.npi_address"] = dict(bindings["mrf.mrf_address"])
        case "unsafe":
            bindings["tiger.zip_state"]["schema_name"] = "snapshot;RESET ROLE"
        case "unknown":
            bindings["tiger.zip_state"]["extra"] = True
        case "mixed_tiger":
            bindings["tiger.zcta5"]["schema_name"] = "reference_family_archive_" + "b" * 32
        case _:
            bindings["tiger.zcta5"]["table_name"] = "another_table"
    with pytest.raises(ServiceUnavailable) as caught:
        async with policy.provider_address_geo_references(_ReferenceSession(bindings), schema_name="mrf"):
            pytest.fail("invalid declared selection reached SQL construction")
    assert caught.value.status_code == 503 and isinstance(caught.value.__cause__, ValueError)
    assert str(caught.value) == "Provider data is temporarily unavailable."
    assert policy._REFERENCE_READ.get() is None


@pytest.mark.parametrize("installed,bindings", [(False, None), (True, None), (True, "canonical")])
async def test_reference_context_refuses_ambiguous_tiger_parent(installed, bindings):
    bindings = _selected_bindings(tiger_schema="tiger") if bindings else None
    with pytest.raises(ServiceUnavailable) as caught:
        async with policy.provider_address_geo_references(
            _ReferenceSession(bindings, installed=installed, retained=True), schema_name="mrf"
        ):
            pytest.fail("retained TIGER children require explicit selection")
    assert caught.value.status_code == 503 and str(caught.value.__cause__) == "geo reference selection is unavailable"


@pytest.mark.parametrize("installed", [False, True])
async def test_known_absent_state_keeps_canonical_behavior(installed):
    session = _ReferenceSession(installed=installed)
    async with policy.provider_address_geo_references(session, schema_name="mrf") as selected:
        assert selected is None
        assert "FROM tiger.zip_state" in policy.provider_address_identity_coherence_sql("addr", schema_name="mrf")


@pytest.mark.parametrize("failure", [RuntimeError("read refused"), asyncio.CancelledError()])
async def test_reference_context_restores_nested_selection_after_failure(failure):
    outer = _ReferenceSession(_selected_bindings())
    inner_bindings = _selected_bindings(tiger_schema="reference_family_archive_" + "b" * 32)
    async with policy.provider_address_geo_references(outer, schema_name="mrf"):
        selected = deepcopy(policy._REFERENCE_READ.get())
        with pytest.raises(type(failure)) as caught:
            async with policy.provider_address_geo_references(_ReferenceSession(inner_bindings), schema_name="mrf"):
                raise failure
        assert caught.value is failure
        assert policy._REFERENCE_READ.get() == selected
    assert policy._REFERENCE_READ.get() is None


async def test_reference_context_preserves_native_read_denial():
    denial = PermissionError("native SELECT or lock refused")
    with pytest.raises(PermissionError) as caught:
        async with policy.provider_address_geo_references(
            _ReferenceSession(_selected_bindings(), failure=denial), schema_name="mrf"
        ):
            pytest.fail("native denial reached SQL construction")
    assert caught.value is denial and policy._REFERENCE_READ.get() is None


async def test_explicit_managed_reference_aliases_keep_their_existing_selection():
    async with policy.provider_address_geo_references(_ReferenceSession(_selected_bindings()), schema_name="mrf"):
        identity = policy.provider_address_identity_coherence_sql(
            "addr", schema_name="mrf", use_projection=False, reference_aliases=("managed_zip", "managed_state")
        )
        assert "managed_state.zip" in identity and "reference_family_archive_" not in identity
        managed = _selected_bindings(tiger_schema="managed_reference")
        joins = policy.provider_address_identity_reference_joins_sql(
            "addr", schema_name="mrf", geo_zip_alias="zip_ref", zip_state_alias="state_ref", dependency_bindings=managed
        )
        assert '"managed_reference"."zip_state"' in joins and "reference_family_archive_" not in joins


async def test_nonspatial_pricing_does_not_read_geo_dependencies():
    async with policy.provider_address_geo_references(object(), schema_name="mrf", request_args={"state": "MA"}):
        assert policy._REFERENCE_READ.get() is None


async def test_internal_unsafe_schema_remains_an_identifier_error():
    with pytest.raises(ValueError, match="simple PostgreSQL identifier"):
        async with policy.provider_address_geo_references(object(), schema_name="unsafe;RESET ROLE"):
            pytest.fail("unsafe internal schema reached a database")


@pytest.mark.parametrize("route", ["membership", "allowed"])
async def test_invalid_geo_selection_reaches_existing_api_unavailable_classification(monkeypatch, route):
    from api import provider_allowed_amount_policy as allowed
    from api import ptg2_serving as serving
    from api.endpoint import pricing
    from tests.ptg2_serving_coverage_paydown_support import strict_v3_tables

    session = _ReferenceSession({})
    monkeypatch.setattr(allowed, "allowed_office_inputs", AsyncMock(return_value=None))
    with pytest.raises(ServiceUnavailable) as caught:
        if route == "membership":
            await serving._membership_location_query(
                session, strict_v3_tables(), {"zip5": "02101"}, candidate_npis=None, limit=10
            )
        else:
            await pricing._allowed_amount_page_rows(
                session, {"zip5": "02101"}, object(), search_scope=("plan", "99213", "CPT", None), current_snapshots=[]
            )
    assert caught.value.status_code == 503 and isinstance(caught.value.__cause__, ValueError)
    assert policy._REFERENCE_READ.get() is None


async def test_membership_query_selects_references_before_spatial_sql(monkeypatch):
    from api import ptg2_serving as serving
    from tests.ptg2_serving_coverage_paydown_support import strict_v3_tables

    session = _ReferenceSession(_selected_bindings())

    async def address_table(actual, args):
        assert actual is session and args == {"zip5": "02101"}
        assert policy._reference_bindings("mrf") is not None
        return "mrf.entity_address_unified"

    monkeypatch.setattr(serving, "_membership_address_table_for_request", address_table)
    query = await serving._membership_location_query(
        session, strict_v3_tables(), {"zip5": "02101"}, candidate_npis=(1234567890,), limit=10
    )
    assert '"reference_family_archive_' + "a" * 32 + '"."zip_state"' in query.filter_sql
    assert policy._REFERENCE_READ.get() is None


@pytest.mark.parametrize("managed", [False, True])
async def test_allowed_amount_query_keeps_ordinary_and_explicit_managed_inputs_separate(monkeypatch, managed):
    from sqlalchemy import text

    from api import provider_allowed_amount_policy as allowed
    from api.endpoint import pricing

    session = _ReferenceSession(_selected_bindings())
    session.execute.return_value = []
    inputs = object() if managed else None
    monkeypatch.setattr(allowed, "allowed_office_inputs", AsyncMock(return_value=inputs))
    monkeypatch.setattr(allowed, "allowed_policy_parameters", lambda value: value)

    async def address_table(actual, _args):
        assert actual is session and (policy._reference_bindings("mrf") is None) is managed
        return "mrf.entity_address_unified"

    def page_sql(_args, **options):
        assert options["office_inputs"] is inputs
        return text(policy.provider_address_identity_coherence_sql("addr", schema_name="mrf", use_projection=False))

    monkeypatch.setattr(pricing, "_allowed_amount_address_table", address_table)
    monkeypatch.setattr(pricing, "_allowed_amount_query_params", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(pricing, "_allowed_amount_page_sql", page_sql)
    page_rows, parameters = await pricing._allowed_amount_page_rows(
        session, {"zip5": "02101"}, object(), search_scope=("plan", "99213", "CPT", None), current_snapshots=[]
    )
    assert page_rows == [] and parameters == {}
    sql = str(session.execute.await_args.args[0])
    assert ("reference_family_archive_" in sql) is not managed
    assert policy._REFERENCE_READ.get() is None
