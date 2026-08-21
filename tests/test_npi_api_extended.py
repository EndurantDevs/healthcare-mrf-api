# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import json
import types
from unittest.mock import AsyncMock

import pytest
import sanic.exceptions

from api.endpoint import npi as npi_module
from db.models import NPIAddress, NPIData, NPIDataOtherIdentifier, NPIDataTaxonomy, NPIDataTaxonomyGroup
from db.models import AddressArchive


@pytest.fixture(autouse=True)
def _stub_detail_location_materialization(monkeypatch):
    """Keep endpoint fixtures focused on their explicitly supplied addresses."""
    monkeypatch.setattr(
        npi_module,
        "_fetch_npi_location_candidates",
        AsyncMock(return_value=[]),
    )
    monkeypatch.setattr(
        npi_module,
        "_fetch_npi_address_rows",
        AsyncMock(return_value=[]),
    )


class FakeConnection:
    def __init__(self, responses):
        self._responses = list(responses)

    async def all(self, *_args, **_kwargs):
        return self._responses.pop(0)

    async def first(self, *_args, **_kwargs):
        return self._responses.pop(0)


class FakeAcquire:
    def __init__(self, connection):
        self._connection = connection

    async def __aenter__(self):
        return self._connection

    async def __aexit__(self, exc_type, exc, tb):
        return False


async def _build_npi_details_with_null_address(npi, **_kwargs):
    """Return the null-address detail fixture used by geocoding coverage."""
    return {
        "npi": npi, "taxonomy_list": [], "taxonomy_group_list": [],
        "do_business_as": [],
        "address_list": [{
            "checksum": 1, "first_line": None, "second_line": None,
            "city_name": "Chicago", "state_name": "IL", "postal_code": None,
            "lat": None, "long": None,
        }],
    }


def _npi_details_builder(address_entries, *, do_business_as=()):
    """Return an async NPI detail builder for explicit address fixtures."""
    async def build_npi_details(npi, **_kwargs):
        return {
            "npi": npi,
            "taxonomy_list": [],
            "taxonomy_group_list": [],
            "do_business_as": list(do_business_as),
            "address_list": [dict(address_entry) for address_entry in address_entries],
        }

    return build_npi_details


def _archive_address_entry(checksum, first_line, *, address_type="primary", country=None):
    """Build the legacy/v2 archive address fixture shared by endpoint tests."""
    address_entry_map = {
        "npi": 1518379601, "type": address_type, "checksum": checksum,
        "first_line": first_line, "second_line": "", "city_name": "Town",
        "state_name": "IL", "postal_code": "123450000", "lat": None,
        "long": None, "formatted_address": None, "plans_network_array": [],
        "taxonomy_array": [],
    }
    if country is not None:
        address_entry_map["country_code"] = country
    return address_entry_map


def _provider_directory_address_entry(npi=1518379602):
    """Build one provider-directory-backed public address fixture."""
    return {
        "npi": npi, "type": "practice", "checksum": 5,
        "address_key": "00000000-0000-0000-0000-000000000001",
        "address_precision": "street", "first_line": "1 Main St",
        "city_name": "Bloomfield", "state_name": "CT", "postal_code": "06002",
        "lat": 41.0, "long": -72.0, "formatted_address": None, "place_id": None,
        "address_sources": ["provider_directory_fhir"],
        "source_record_ids": [
            "provider_directory_fhir:practitioner_role:pdfhir_cigna:role-1:loc-1"
        ],
        "source_count": 1, "plans_network_array": [], "taxonomy_array": [],
    }


def _provider_directory_source_detail():
    """Build sensitive source metadata used to verify public redaction."""
    return {
        "source": "provider_directory_fhir", "source_id": "pdfhir_cigna",
        "endpoint_id": "pd_endpoint_cigna", "org_name": "Cigna",
        "plan_name": "Commercial",
        "canonical_api_base": "https://fhir.cigna.com/ProviderDirectory/v1",
        "api_base": "https://fhir.cigna.com/ProviderDirectory/v1",
        "auth_type": "none", "auth_required": False, "requires_api_key": False,
        "credential_name": "PAYER_DIRECTORY_KEY", "headers": {"X-API-Key": "secret"},
        "token": "secret-token", "last_validated_status": "valid",
    }


def _cached_provider_profile_record(published_at, serving_identity):
    """Build one profile-map record with an internal cache identity."""
    return {
        1518379601: {
            "profile": {
                "schema_version": 1,
                "generation_id": (
                    "pdprofile_11111111111111111111111111111111"
                ),
                "published_at": published_at,
                "facts": {},
            },
            "_serving_identity": serving_identity,
        }
    }


def _profile_cache_transition_records():
    fallback_identity = (
        "fallback:pdprofile_11111111111111111111111111111111:"
        "2026-07-13T20:00:00Z:101"
    )
    singleton_identity = (
        "singleton:pdprofile_11111111111111111111111111111111:"
        "2026-07-30T15:00:00Z:6:101:102"
    )
    return [
        _cached_provider_profile_record(
            "2026-07-13T20:00:00Z",
            fallback_identity,
        ),
        _cached_provider_profile_record(
            "2026-07-30T15:00:00Z",
            singleton_identity,
        ),
        _cached_provider_profile_record(
            "2026-07-30T15:00:00Z",
            singleton_identity,
        ),
    ]


def _install_profile_transition_cache_dependencies(
    monkeypatch,
    profile_fetch,
    build_details,
):
    monkeypatch.setattr(
        npi_module,
        "_NPI_DETAIL_RESPONSE_CACHE",
        npi_module.OrderedDict(),
    )
    monkeypatch.setattr(
        npi_module,
        "_NPI_DETAIL_RESPONSE_CACHE_TTL_SECONDS",
        300.0,
    )
    monkeypatch.setattr(npi_module, "_NPI_DETAIL_RESPONSE_CACHE_MAX_KEYS", 8)
    monkeypatch.setattr(
        npi_module,
        "_npi_canonical_publication_identity",
        AsyncMock(return_value="1:nppub1_" + "a" * 43),
    )
    monkeypatch.setattr(
        npi_module,
        "_fetch_provider_directory_profile_map",
        profile_fetch,
    )
    monkeypatch.setattr(npi_module, "_build_npi_details", build_details)
    for function_name in (
        "_fetch_npi_location_candidates",
        "_fetch_npi_address_rows",
        "_fetch_provider_directory_address_overlay",
        "_fetch_other_names",
    ):
        monkeypatch.setattr(npi_module, function_name, AsyncMock(return_value=[]))
    monkeypatch.setattr(
        npi_module,
        "_provider_directory_address_overlay_serving_identity",
        AsyncMock(return_value="oid:101"),
    )
    monkeypatch.setattr(
        npi_module,
        "_fetch_provider_enrichment_detail",
        AsyncMock(
            return_value={
                "summary": None,
                "enrollments": {},
                "ffs_visibility": {},
            }
        ),
    )


def _geocode_address_entry(checksum, first_line, postal_code):
    """Build one unresolved address for paid/free geocoder path tests."""
    return {
        "checksum": checksum, "first_line": first_line, "second_line": "",
        "city_name": "Chicago", "state_name": "IL", "postal_code": postal_code,
        "lat": None, "long": None,
    }


def _geocoder_config(*, include_google=False):
    """Build the endpoint configuration shared by geocoder path tests."""
    config_by_name = {
        "NPI_API_UPDATE_GEOCODE": True,
        "GEOCODE_MAPBOX_STYLE_KEY_PARAM": "access_token",
        "GEOCODE_MAPBOX_STYLE_KEY": "[\"token\"]",
        "GEOCODE_MAPBOX_STYLE_URL": "https://mock-map/",
    }
    if include_google:
        config_by_name.update({
            "GEOCODE_GOOGLE_STYLE_ADDRESS_PARAM": "address",
            "GEOCODE_GOOGLE_STYLE_KEY_PARAM": "key",
            "GEOCODE_GOOGLE_STYLE_KEY": "secret",
            "GEOCODE_GOOGLE_STYLE_URL": "https://mock-google",
            "GEOCODE_GOOGLE_STYLE_ADDITIONAL_QUERY_PARAMS": "region=us",
        })
    return config_by_name


class _NpiEndpointTestApp:
    """Capture, close, or reject endpoint background tasks as requested."""

    def __init__(self, config, *, close_tasks=False, reject_tasks=False):
        self.config = config
        self.tasks = []
        self._close_tasks = close_tasks
        self._reject_tasks = reject_tasks

    def add_task(self, coroutine):
        if self._reject_tasks:
            raise AssertionError("no geocode update task expected")
        self.tasks.append(coroutine)
        if self._close_tasks and asyncio.iscoroutine(coroutine):
            coroutine.close()


class _AwaitableGeocodeUpdateStatement:
    async def values(self, *_args, **_kwargs):
        return self

    async def status(self):
        return None


class _AwaitableGeocodeInsertStatement:
    def values(self, *_args, **_kwargs):
        return self

    def on_conflict_do_update(self, *_args, **_kwargs):
        return self

    async def status(self):
        return None


class _GeocodePathDB:
    async def update(self, *_args, **_kwargs):
        return _AwaitableGeocodeUpdateStatement()

    async def insert(self, *_args, **_kwargs):
        return _AwaitableGeocodeInsertStatement()

    async def scalar(self, *_args, **_kwargs):
        return None


@pytest.mark.asyncio
async def test_compute_npi_counts(monkeypatch):
    class ScalarDB:
        def __init__(self):
            self.values = [10, 20]

        async def scalar(self, *_args, **_kwargs):
            return self.values.pop(0)

    monkeypatch.setattr(npi_module, "db", ScalarDB())
    counts = await npi_module._compute_npi_counts()
    assert tuple(counts) == (10, 20)


@pytest.mark.asyncio
async def test_get_all_count_only(monkeypatch):
    fake_conn = FakeConnection([[(5,)]] )

    class FakeDB:
        def acquire(self):
            return FakeAcquire(fake_conn)

    monkeypatch.setattr(npi_module, "db", FakeDB())
    request = types.SimpleNamespace(args={"count_only": "1"}, app=types.SimpleNamespace())
    response = await npi_module.get_all(request)
    assert json.loads(response.body) == {"rows": 5}


@pytest.mark.asyncio
async def test_fast_has_insurance_count_global_uses_count_star_and_cache(monkeypatch):
    class FakeResult:
        def scalar(self):
            return 42

    calls = []

    class FakeSession:
        async def execute(self, stmt):
            calls.append(str(stmt))
            return FakeResult()

    class FakeSessionContext:
        async def __aenter__(self):
            return FakeSession()

        async def __aexit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(npi_module, "db", types.SimpleNamespace(session=lambda: FakeSessionContext()))
    monkeypatch.setattr(npi_module, "ENABLE_NPI_SCHEMA_CACHE", True)
    monkeypatch.setattr(npi_module, "_NPI_HAS_INSURANCE_TOTAL_CACHE", {})
    monkeypatch.setattr(
        npi_module,
        "_npi_canonical_publication_identity",
        AsyncMock(return_value="1:nppub1_" + "a" * 43),
    )

    first = await npi_module._fast_has_insurance_count(None, None)
    second = await npi_module._fast_has_insurance_count(None, None)

    assert first == 42
    assert second == 42
    assert len(calls) == 1
    assert "count(*)" in calls[0].lower()


@pytest.mark.asyncio
async def test_fast_has_insurance_count_city_uses_distinct(monkeypatch):
    class FakeResult:
        def scalar(self):
            return 7

    calls = []

    class FakeSession:
        async def execute(self, stmt):
            calls.append(str(stmt))
            return FakeResult()

    class FakeSessionContext:
        async def __aenter__(self):
            return FakeSession()

        async def __aexit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(npi_module, "db", types.SimpleNamespace(session=lambda: FakeSessionContext()))
    monkeypatch.setattr(npi_module, "ENABLE_NPI_SCHEMA_CACHE", True)
    monkeypatch.setattr(npi_module, "_NPI_HAS_INSURANCE_TOTAL_CACHE", {})
    monkeypatch.setattr(
        npi_module,
        "_npi_canonical_publication_identity",
        AsyncMock(return_value="1:nppub1_" + "a" * 43),
    )

    insurance_count = await npi_module._fast_has_insurance_count("MIAMI", None)
    assert insurance_count == 7
    assert len(calls) == 1
    assert "count(distinct" in calls[0].lower()


def _build_result_row(npi_value: int) -> list:
    result_values = [npi_value]
    for column in NPIData.__table__.columns:
        if column.key == "npi":
            result_values.append(npi_value)
        elif column.key == "do_business_as":
            result_values.append(['DBA'])
        else:
            result_values.append(f"data_{column.key}")
    for column in NPIAddress.__table__.columns:
        if column.key == "npi":
            result_values.append(npi_value)
        elif column.key == "checksum":
            result_values.append(1)
        else:
            result_values.append(f"addr_{column.key}")
    result_values.append(1)
    return result_values


def _set_result_address_column(result_values: list, key: str, value):
    for idx, column in enumerate(NPIAddress.__table__.columns):
        if column.key == key:
            result_values[1 + len(NPIData.__table__.columns) + idx] = value
            return
    raise AssertionError(f"Unknown NPIAddress column: {key}")


def _set_near_address_column(near_values: list, key: str, value):
    for idx, column in enumerate(NPIAddress.__table__.columns):
        if column.key == key:
            near_values[2 + idx] = value
            return
    raise AssertionError(f"Unknown NPIAddress column: {key}")


def test_dedupe_addresses_merges_conflicting_sites_by_exact_key(caplog):
    addresses = [
        {
            "address_key": "00000000-0000-0000-0000-000000000001",
            "premise_key": "00000000-0000-0000-0000-000000000002",
            "first_line": "1 Main St",
            "type": "primary",
        },
        {
            "address_key": "00000000-0000-0000-0000-000000000001",
            "premise_key": "00000000-0000-0000-0000-000000000003",
            "first_line": "1 Main St",
            "type": "primary",
        },
        {
            "address_key": "00000000-0000-0000-0000-000000000001",
            "premise_key": "00000000-0000-0000-0000-000000000002",
            "first_line": "1 Main St",
            "type": "secondary",
            "telephone_number": "3125551212",
        },
    ]

    deduped = npi_module._dedupe_addresses_by_key(addresses)

    assert len(deduped) == 1
    assert deduped[0]["premise_key"] == "00000000-0000-0000-0000-000000000002"
    assert deduped[0]["telephone_number"] == "3125551212"
    assert "maps to 2 conflicting non-null site keys" in caplog.text


@pytest.mark.asyncio
async def test_get_all_returns_rows(monkeypatch):
    result_row = _build_result_row(1234567890)
    _set_result_address_column(result_row, "telephone_number", "1 (312) 555-1212 ext. 44")
    _set_result_address_column(result_row, "fax_number", "(312) 555-0199")
    _set_result_address_column(result_row, "phone_number", None)
    _set_result_address_column(result_row, "phone_extension", None)
    _set_result_address_column(result_row, "fax_number_digits", None)
    _set_result_address_column(result_row, "country_code", "US")
    class QueryAwareConnection:
        async def all(self, statement, **_params):
            sql_text = str(statement)
            if "COUNT(DISTINCT" in sql_text:
                return [(2,)]
            if "FROM mrf.npi_taxonomy AS taxonomy" in sql_text:
                return [
                    types.SimpleNamespace(
                        _mapping={
                            "npi": 1234567890,
                            "checksum": 1,
                            "healthcare_provider_taxonomy_code": "207Q00000X",
                        }
                    )
                ]
            return [result_row]

    class FakeDB:
        def acquire(self):
            return FakeAcquire(QueryAwareConnection())

    monkeypatch.setattr(npi_module, "db", FakeDB())
    request = types.SimpleNamespace(
        args={
            "limit": "1",
            "classification": "Test",
            "q": "Doc",
            "plan_network": "10,20",
            "has_insurance": "1",
            "city": "Chicago",
            "state": "il",
            "codes": "1,2",
            "section": "A",
            "display_name": "Display",
            "specialization": "Spec",
        },
        app=types.SimpleNamespace(),
    )
    response = await npi_module.get_all(request)
    response_body = json.loads(response.body)
    assert response_body["total"] == 2
    assert response_body["rows"][0]["taxonomy_list"]
    assert response_body["rows"][0]["do_business_as"] == ['DBA']
    assert response_body["rows"][0]["address_key"] == "addr_address_key"
    assert response_body["rows"][0]["phone_number"] == "3125551212"
    assert response_body["rows"][0]["phone_extension"] == "44"
    assert response_body["rows"][0]["fax_number_digits"] == "3125550199"
    assert "employer_identification_number" not in response_body["rows"][0]
    assert "parent_organization_tin" not in response_body["rows"][0]
    assert "data_employer_identification_number" not in json.dumps(response_body)
    assert "data_parent_organization_tin" not in json.dumps(response_body)


def _build_near_row(npi_value: int) -> list:
    near_values = [npi_value, 0.5]
    for column in NPIAddress.__table__.columns:
        if column.key == "npi":
            near_values.append(npi_value)
        elif column.key == "checksum":
            near_values.append(1)
        else:
            near_values.append(f"addr_{column.key}")
    for column in NPIData.__table__.columns:
        if column.key == "npi":
            near_values.append(npi_value)
        elif column.key == "do_business_as":
            near_values.append(['DBA'])
        else:
            near_values.append(f"data_{column.key}")
    for column in NPIDataTaxonomy.__table__.columns:
        if column.key in {"npi", "checksum"}:
            near_values.append(None)
        else:
            near_values.append(f"tax_{column.key}")
    return near_values


@pytest.mark.asyncio
async def test_get_near_npi(monkeypatch):
    near_row = _build_near_row(1112223334)
    _set_near_address_column(near_row, "telephone_number", "(217) 555-0100")
    _set_near_address_column(near_row, "phone_number", None)
    _set_near_address_column(near_row, "country_code", "US")
    responses = [
        [{"intptlat": "41.0", "intptlon": "-87.0"}],
        [near_row, _build_near_row(1112223334)],
    ]
    fake_conn = FakeConnection(responses)

    class FakeDB:
        def acquire(self):
            return FakeAcquire(fake_conn)

    monkeypatch.setattr(npi_module, "db", FakeDB())
    request = types.SimpleNamespace(
        args={"zip_codes": "60601", "limit": "1"},
        app=types.SimpleNamespace(),
    )
    response = await npi_module.get_near_npi(request)
    response_body = json.loads(response.body)
    assert len(response_body) == 1
    assert response_body[0]["distance"] == 0.5
    assert response_body[0]["taxonomy_list"]
    assert response_body[0]["do_business_as"] == ['DBA']
    assert response_body[0]["phone_number"] == "2175550100"
    assert "employer_identification_number" not in response_body[0]
    assert "parent_organization_tin" not in response_body[0]


@pytest.mark.asyncio
async def test_get_near_npi_with_lat_long_uses_knn_without_bbox_params(monkeypatch):
    captured_query_map = {}

    class RecordingConnection:
        async def all(self, sql, **params):
            captured_query_map["sql"] = str(sql)
            captured_query_map.update(params)
            return [_build_near_row(1112223334)]

        async def first(self, *_args, **_kwargs):
            return None

    class FakeDB:
        def acquire(self):
            return FakeAcquire(RecordingConnection())

    monkeypatch.setattr(npi_module, "db", FakeDB())

    request = types.SimpleNamespace(
        args={"lat": "41.0", "long": "-87.0", "zip_codes": "60601", "limit": "1"},
        app=types.SimpleNamespace(),
    )
    response = await npi_module.get_near_npi(request)
    response_body = json.loads(response.body)
    assert len(response_body) == 1
    assert ") <-> Geography(" in captured_query_map["sql"]
    assert "min_lat" not in captured_query_map
    assert "max_lat" not in captured_query_map
    assert "min_long" not in captured_query_map
    assert "max_long" not in captured_query_map
    assert captured_query_map["radius"] == 10


@pytest.mark.asyncio
async def test_get_near_npi_uses_unified_address_table_when_compatible(monkeypatch):
    captured_query_map = {}

    async def fake_table_columns(table_name, *, session=None):
        assert session is None
        if table_name == "entity_address_unified":
            return npi_module._public_address_serving_column_keys()
        return set()

    class RecordingConnection:
        async def all(self, sql, **params):
            captured_query_map["sql"] = str(sql)
            captured_query_map["params"] = params
            return []

        async def first(self, *_args, **_kwargs):
            return None

    class FakeDB:
        def acquire(self):
            return FakeAcquire(RecordingConnection())

    monkeypatch.delenv("HLTHPRT_ADDRESS_SERVING_SOURCE", raising=False)
    monkeypatch.setattr(npi_module, "_table_columns", fake_table_columns)
    monkeypatch.setattr(npi_module, "db", FakeDB())

    request = types.SimpleNamespace(
        args={"lat": "41.0", "long": "-87.0", "radius": "10", "limit": "1"},
        app=types.SimpleNamespace(),
    )
    response = await npi_module.get_near_npi(request)
    assert json.loads(response.body) == []
    assert "FROM mrf.entity_address_unified AS a" in captured_query_map["sql"]
    assert "FROM mrf.npi_address AS a" not in captured_query_map["sql"]
    assert "COALESCE(a.address_precision, '') <> 'city_zip'" in captured_query_map["sql"]
    assert ") <-> Geography(" in captured_query_map["sql"]
    assert "min_lat" not in captured_query_map["params"]


@pytest.mark.asyncio
async def test_address_serving_table_falls_back_to_legacy_when_unified_incompatible(monkeypatch):
    async def fake_table_columns(table_name, *, session=None):
        assert session is None
        if table_name == "entity_address_unified":
            return {"npi"}
        return set()

    monkeypatch.delenv("HLTHPRT_ADDRESS_SERVING_SOURCE", raising=False)
    monkeypatch.setattr(npi_module, "_table_columns", fake_table_columns)

    table_name = await npi_module._address_serving_table_sql({"npi", "type"})
    assert table_name == "mrf.npi_address"


@pytest.mark.asyncio
async def test_address_serving_table_uses_legacy_when_explicit(monkeypatch):
    async def fake_table_columns(*_args, **_kwargs):
        raise AssertionError("legacy mode must not probe unified table columns")

    monkeypatch.setenv("HLTHPRT_ADDRESS_SERVING_SOURCE", "legacy")
    monkeypatch.setattr(npi_module, "_table_columns", fake_table_columns)

    table_name = await npi_module._address_serving_table_sql({"npi", "type"})
    assert table_name == "mrf.npi_address"


@pytest.mark.asyncio
async def test_get_full_taxonomy_list(monkeypatch):
    class FakeTaxonomy:
        def to_json_dict(self):
            return {"healthcare_provider_taxonomy_code": "123"}

    class FakeNucc:
        def to_json_dict(self):
            return {"code": "123"}

    class FakeDB:
        async def execute(self, *_args, **_kwargs):
            return types.SimpleNamespace(all=lambda: [(FakeTaxonomy(), FakeNucc())])

    monkeypatch.setattr(npi_module, "db", FakeDB())
    response = await npi_module.get_full_taxonomy_list(types.SimpleNamespace(), "123")
    response_body = json.loads(response.body)
    assert response_body[0]["nucc_taxonomy"]["code"] == "123"


@pytest.mark.asyncio
async def test_get_plans_by_npi(monkeypatch):
    class FakeQuery:
        def __init__(self, rows):
            self._rows = rows

        def where(self, *_args, **_kwargs):
            return self

        def order_by(self, *_args, **_kwargs):
            return self

        async def iterate(self):
            for row in self._rows:
                yield row

    plan_entry = types.SimpleNamespace(to_json_dict=lambda: {"plan": 1})
    issuer_entry = types.SimpleNamespace(to_json_dict=lambda: {"issuer": 2})
    monkeypatch.setattr(
        npi_module,
        "db",
        types.SimpleNamespace(select=lambda *_args, **_kwargs: FakeQuery([(plan_entry, issuer_entry)])),
    )
    response = await npi_module.get_plans_by_npi(types.SimpleNamespace(), "123")
    response_body = json.loads(response.body)
    assert response_body["npi_data"][0]["issuer_info"]["issuer"] == 2


def test_public_nested_taxonomy_rows_hide_internal_identity_and_dedupe():
    taxonomy_input_rows = [
        {
            "npi": 1194956268,
            "checksum": -1,
            "healthcare_provider_taxonomy_code": "261QM1200X",
            "healthcare_provider_primary_taxonomy_switch": "Y",
        },
        {
            "npi": 1194956268,
            "checksum": -2,
            "healthcare_provider_taxonomy_code": "261QM1200X",
            "healthcare_provider_primary_taxonomy_switch": "Y",
        },
        {
            "npi": 1194956268,
            "checksum": -3,
            "healthcare_provider_taxonomy_code": "2085R0202X",
            "healthcare_provider_primary_taxonomy_switch": "N",
        },
    ]

    taxonomy_output_rows = npi_module._public_nested_taxonomy_rows(taxonomy_input_rows)

    assert taxonomy_output_rows == [
        {
            "healthcare_provider_taxonomy_code": "261QM1200X",
            "healthcare_provider_primary_taxonomy_switch": "Y",
        },
        {
            "healthcare_provider_taxonomy_code": "2085R0202X",
            "healthcare_provider_primary_taxonomy_switch": "N",
        },
    ]
    assert all(
        "npi" not in taxonomy_record_map and "checksum" not in taxonomy_record_map
        for taxonomy_record_map in taxonomy_output_rows
    )


@pytest.mark.asyncio
async def test_build_npi_details(monkeypatch):
    row_values = []
    for column in NPIData.__table__.columns:
        if column.key == "npi":
            row_values.append(1234567890)
        else:
            row_values.append(f"data_{column.key}")
    row_values.append([{"npi": 1234567890, "checksum": 11, "taxonomy": 1}])
    row_values.append([{"npi": 1234567890, "checksum": 12, "group": 2}])
    row_values.append([{"checksum": 1, "type": "primary"}])

    class FakeSelect:
        def __init__(self, rows):
            self._rows = rows

        def select_from(self, *_args, **_kwargs):
            return self

        def where(self, *_args, **_kwargs):
            return self

        def group_by(self, *_args, **_kwargs):
            return self

        async def all(self):
            return [self._rows]

    class FakeDB:
        def acquire(self):
            return FakeAcquire(None)

        def select(self, *_args, **_kwargs):
            return FakeSelect(row_values)

    monkeypatch.setattr(npi_module, "db", FakeDB())
    response_body = await npi_module._build_npi_details(1234567890)
    assert response_body["taxonomy_list"]
    assert response_body["taxonomy_list"] == [{"taxonomy": 1}]
    assert response_body["taxonomy_group_list"]
    assert response_body["taxonomy_group_list"] == [{"group": 2}]
    assert response_body["address_list"]
    assert "employer_identification_number" not in response_body
    assert "parent_organization_tin" not in response_body


@pytest.mark.asyncio
async def test_fetch_other_names(monkeypatch):
    class FakeRow:
        def to_json_dict(self):
            return {
                "npi": 1,
                "checksum": 2,
                "other_provider_identifier": "ALT",
            }

    class FakeDB:
        async def execute(self, *_args, **_kwargs):
            return types.SimpleNamespace(scalars=lambda: [FakeRow()])

    monkeypatch.setattr(npi_module, "db", FakeDB())
    rows = await npi_module._fetch_other_names(1)
    assert rows == [{"other_provider_identifier": "ALT"}]


class FakeUpdate:
    def where(self, *_args, **_kwargs):
        return self

    def values(self, *_args, **_kwargs):
        return self

    async def status(self):
        return None


class FakeInsert:
    def values(self, obj):
        self.response_body = obj
        return self

    def on_conflict_do_update(self, **_kwargs):
        return self

    async def status(self):
        return None


def _assert_locally_rendered_address(insert, external_label):
    rendered_address = insert.response_body["formatted_address"]
    assert rendered_address == npi_module.render_formatted_address_v2(
        insert.response_body.get("first_line"),
        insert.response_body.get("second_line"),
        insert.response_body.get("city_name"),
        insert.response_body.get("state_name"),
        insert.response_body.get("postal_code"),
        insert.response_body.get("country_code"),
    )
    assert rendered_address != external_label


def _make_address_row():
    data = {}
    for column in AddressArchive.__table__.columns:
        if column.key == "checksum":
            data[column.key] = 1
        elif column.key == "lat":
            data[column.key] = 41.0
        elif column.key == "long":
            data[column.key] = -87.0
        else:
            data[column.key] = f"archive_{column.key}"
    return types.SimpleNamespace(**data)


def _make_v2_archive_row(
    *,
    lat=42.0,
    long=-88.0,
    formatted_address="v2 address",
    place_id="v2-place",
    geo_source=None,
):
    return types.SimpleNamespace(
        _mapping={
            "lat": lat,
            "long": long,
            "formatted_address": formatted_address,
            "place_id": place_id,
            "geo_source": geo_source,
        }
    )


@pytest.mark.asyncio
async def test_get_npi_geocode_mapbox(monkeypatch):
    """Verify get npi geocode mapbox."""
    monkeypatch.setattr(
        npi_module,
        "_build_npi_details",
        _npi_details_builder([_geocode_address_entry(1, "10 Main", "606011234")]),
    )
    monkeypatch.setattr(npi_module, "_fetch_other_names", AsyncMock(return_value=[]))

    download_responses = [
        json.dumps(
            {
                "features": [
                    {
                        "geometry": {"coordinates": [-87.1, 41.1]},
                        "place_name": "Chicago, IL",
                    }
                ]
            }
        )
    ]

    async def fake_download(*_args, **_kwargs):
        return download_responses.pop(0)

    monkeypatch.setattr(npi_module, "download_it", fake_download)

    update = FakeUpdate()
    insert = FakeInsert()

    class FakeDB:
        def __init__(self):
            self.scalar_values = [_make_address_row()]

        def update(self, *_args, **_kwargs):
            return update

        def insert(self, *_args, **_kwargs):
            return insert

        async def scalar(self, *_args, **_kwargs):
            return self.scalar_values.pop(0)

    fake_db = FakeDB()
    monkeypatch.setattr(npi_module, "db", fake_db)

    app = _NpiEndpointTestApp({
        **_geocoder_config(),
        "GEOCODE_MAPBOX_STYLE_ADDITIONAL_QUERY_PARAMS": "language=en",
    })
    request = types.SimpleNamespace(
        args={"force_address_update": "1"}, app=app,
    )
    response = await npi_module.get_npi(request, "1518379601")
    response_body = json.loads(response.body)
    assert response_body["address_list"][0]["lat"] == 41.1
    assert response_body["address_list"][0]["geo_source"] == "mapbox"
    await app.tasks[0]
    assert hasattr(insert, "response_body")
    _assert_locally_rendered_address(insert, "Chicago, IL")


@pytest.mark.asyncio
async def test_get_npi_geocode_omits_null_address_parts(monkeypatch):
    """Verify get npi geocode omits null address parts."""
    monkeypatch.setattr(
        npi_module, "_build_npi_details", _build_npi_details_with_null_address
    )
    monkeypatch.setattr(npi_module, "_fetch_other_names", AsyncMock(return_value=[]))

    requested_urls = []

    async def fake_download(url, *_args, **_kwargs):
        requested_urls.append(url)
        return json.dumps(
            {
                "features": [
                    {
                        "geometry": {"coordinates": [-87.1, 41.1]},
                        "place_name": "Chicago, IL",
                    }
                ]
            }
        )

    monkeypatch.setattr(npi_module, "download_it", fake_download)

    class FakeDB:
        def update(self, *_args, **_kwargs):
            return FakeUpdate()

        def insert(self, *_args, **_kwargs):
            return FakeInsert()

        async def scalar(self, *_args, **_kwargs):
            return None

    monkeypatch.setattr(npi_module, "db", FakeDB())
    monkeypatch.setattr(npi_module.random, "choice", lambda seq: seq[0])

    class FakeApp:
        def __init__(self):
            self.config = {
                "NPI_API_UPDATE_GEOCODE": True,
                "GEOCODE_MAPBOX_STYLE_KEY_PARAM": "access_token",
                "GEOCODE_MAPBOX_STYLE_KEY": "[\"token\"]",
                "GEOCODE_MAPBOX_STYLE_URL": "https://mock-map/",
            }

        def add_task(self, coro):
            if asyncio.iscoroutine(coro):
                coro.close()

    request = types.SimpleNamespace(args={"force_address_update": "1"}, app=FakeApp())
    response = await npi_module.get_npi(request, "1518379601")
    response_body = json.loads(response.body)

    assert response_body["address_list"][0]["lat"] == 41.1
    assert requested_urls
    assert "None" not in requested_urls[0]
    assert "Chicago" in requested_urls[0]


@pytest.mark.asyncio
async def test_get_npi_geocode_google(monkeypatch):
    """Verify get npi geocode google."""
    monkeypatch.setattr(
        npi_module,
        "_build_npi_details",
        _npi_details_builder([_geocode_address_entry(2, "20 Main", "60601")]),
    )
    monkeypatch.setattr(npi_module, "_fetch_other_names", AsyncMock(return_value=[]))

    download_responses = [
        json.dumps({"features": []}),
        json.dumps(
            {
                "results": [
                    {
                        "geometry": {"location": {"lat": 41.2, "lng": -87.2}},
                        "formatted_address": "Chicago, IL",
                        "place_id": "abc",
                    }
                ]
            }
        ),
    ]

    async def fake_download(*_args, **_kwargs):
        return download_responses.pop(0)

    monkeypatch.setattr(npi_module, "download_it", fake_download)

    update = FakeUpdate()
    insert = FakeInsert()

    class FakeDB:
        def __init__(self):
            self.scalar_values = [_make_address_row()]

        def update(self, *_args, **_kwargs):
            return update

        def insert(self, *_args, **_kwargs):
            return insert

        async def scalar(self, *_args, **_kwargs):
            return self.scalar_values.pop(0)

    fake_db = FakeDB()
    monkeypatch.setattr(npi_module, "db", fake_db)

    app = _NpiEndpointTestApp(_geocoder_config(include_google=True))
    request = types.SimpleNamespace(
        args={"force_address_update": "1"}, app=app,
    )
    response = await npi_module.get_npi(request, "1518379601")
    response_body = json.loads(response.body)
    assert response_body["address_list"][0]["lat"] == 41.2
    assert response_body["address_list"][0]["geo_source"] == "google"
    await app.tasks[0]
    assert hasattr(insert, "response_body")
    _assert_locally_rendered_address(insert, "Chicago, IL")


@pytest.mark.asyncio
async def test_get_npi_geocode_openaddresses_before_paid_providers(monkeypatch):
    """Verify get npi geocode openaddresses before paid providers."""
    monkeypatch.setattr(
        npi_module,
        "_build_npi_details",
        _npi_details_builder([_geocode_address_entry(4, "30 Main Street", "60601")]),
    )
    monkeypatch.setattr(npi_module, "_fetch_other_names", AsyncMock(return_value=[]))
    monkeypatch.setattr(
        npi_module,
        "download_it",
        AsyncMock(side_effect=AssertionError("paid geocoder should not be called")),
    )

    update = FakeUpdate()
    insert = FakeInsert()

    class FakeDB:
        def update(self, *_args, **_kwargs):
            return update

        def insert(self, *_args, **_kwargs):
            return insert

        async def scalar(self, query, **_kwargs):
            if isinstance(query, str) and "to_regclass" in query:
                return "mrf.openaddresses_geocode"
            return _make_address_row()

        async def first(self, sql, **_kwargs):
            assert "openaddresses_geocode" in sql
            return types.SimpleNamespace(
                _mapping={
                    "long": -87.3,
                    "lat": 41.3,
                    "formatted_address": "30 Main Street, Chicago, IL 60601",
                    "place_id": "oa-1",
                    "geo_source": "openaddresses",
                    "geocode_source": "openaddresses_exact",
                    "geocode_quality": "rooftop",
                }
            )

    monkeypatch.setattr(npi_module, "db", FakeDB())

    app = _NpiEndpointTestApp(
        _geocoder_config(include_google=True), close_tasks=True
    )
    request = types.SimpleNamespace(
        args={"force_address_update": "1"}, app=app,
    )
    response = await npi_module.get_npi(request, "1518379601")
    response_body = json.loads(response.body)
    address = response_body["address_list"][0]
    assert address["lat"] == 41.3
    assert address["geo_source"] == "openaddresses"
    assert address["geocode_source"] == "openaddresses_exact"
    assert app.tasks


@pytest.mark.asyncio
async def test_get_all_full_taxonomy(monkeypatch):
    connections = [FakeConnection([[(1234, 7)]])]

    class FakeDB:
        def acquire(self):
            return FakeAcquire(connections.pop(0))

    monkeypatch.setattr(npi_module, "db", FakeDB())
    request = types.SimpleNamespace(
        args={
            "count_only": "1",
            "response_format": "full_taxonomy",
            "classification": "Pharmacy",
            "codes": "1234",
        },
        app=types.SimpleNamespace(),
    )
    response = await npi_module.get_all(request)
    response_body = json.loads(response.body)
    assert response_body["rows"] == {"1234": 7}


@pytest.mark.asyncio
async def test_get_all_response_format_default(monkeypatch):
    connections = [FakeConnection([[("Pharmacist", 3)]])]

    class FakeDB:
        def acquire(self):
            return FakeAcquire(connections.pop(0))

    monkeypatch.setattr(npi_module, "db", FakeDB())
    request = types.SimpleNamespace(
        args={
            "count_only": "1",
            "response_format": "classification",
            "codes": "1234",
        },
        app=types.SimpleNamespace(),
    )
    response = await npi_module.get_all(request)
    response_body = json.loads(response.body)
    assert response_body["rows"] == {"Pharmacist": 3}


@pytest.mark.asyncio
async def test_get_near_npi_with_filters(monkeypatch):
    row = _build_near_row(5556667778)
    responses = [[row]]
    fake_conn = FakeConnection(responses)

    class FakeDB:
        def acquire(self):
            return FakeAcquire(fake_conn)

    monkeypatch.setattr(npi_module, "db", FakeDB())
    request = types.SimpleNamespace(
        args={
            "long": "-87.0",
            "lat": "41.0",
            "exclude_npi": "1234567890",
            "plan_network": "1,2",
            "q": "Clinic",
            "classification": "Pharmacy",
            "section": "Sec",
            "display_name": "Name",
            "codes": "1,2",
            "limit": "1",
        },
        app=types.SimpleNamespace(),
    )
    response = await npi_module.get_near_npi(request)
    response_body = json.loads(response.body)
    assert response_body[0]["npi"] == 5556667778


@pytest.mark.parametrize(
    ("raw_zip", "expected_zip"),
    [("60601-1234", "60601"), ("1234", "01234")],
)
@pytest.mark.asyncio
async def test_get_near_npi_honors_zip_radius_and_specialization(
    monkeypatch, raw_zip, expected_zip
):
    captured_query_map = {}

    class RecordingConnection:
        async def all(self, sql, **params):
            sql_text = str(sql)
            if "from zcta5" in sql_text:
                captured_query_map["zip_code"] = params["zip_code"]
                return [{"intptlat": "41.0", "intptlon": "-87.0"}]
            captured_query_map["sql"] = sql_text
            captured_query_map["params"] = dict(params)
            return [_build_near_row(5556667778)]

        async def first(self, *_args, **_kwargs):
            return None

    class FakeDB:
        def acquire(self):
            return FakeAcquire(RecordingConnection())

    monkeypatch.setattr(npi_module, "db", FakeDB())
    request = types.SimpleNamespace(
        args={
            "zip_codes": raw_zip,
            "radius": "7",
            "specialization": "Family Medicine",
            "limit": "1",
        },
        app=types.SimpleNamespace(),
    )

    response = await npi_module.get_near_npi(request)

    assert len(json.loads(response.body)) == 1
    assert captured_query_map["zip_code"] == expected_zip
    assert "specialization = :specialization" in captured_query_map["sql"]
    assert captured_query_map["params"]["specialization"] == "Family Medicine"
    assert captured_query_map["params"]["radius"] == 7


@pytest.mark.asyncio
async def test_get_near_npi_applies_provider_sex_before_distance_limit(monkeypatch):
    captured_query_map = {}

    class RecordingConnection:
        async def all(self, sql, **params):
            captured_query_map["sql"] = str(sql)
            captured_query_map["params"] = dict(params)
            return [_build_near_row(5556667778)]

        async def first(self, *_args, **_kwargs):
            return None

    class FakeDB:
        def acquire(self):
            return FakeAcquire(RecordingConnection())

    monkeypatch.setattr(npi_module, "db", FakeDB())
    request = types.SimpleNamespace(
        args={
            "long": "-87.0",
            "lat": "41.0",
            "provider_sex_code": "x",
            "limit": "1",
        },
        app=types.SimpleNamespace(),
    )

    response = await npi_module.get_near_npi(request)

    assert len(json.loads(response.body)) == 1
    sql = captured_query_map["sql"]
    sex_predicate = "sex_provider.provider_sex_code = :provider_sex_code"
    assert sex_predicate in sql
    assert sql.index(sex_predicate) < sql.index("ORDER BY Geography(")
    assert sql.index(sex_predicate) < sql.index("LIMIT :limit")
    assert captured_query_map["params"]["provider_sex_code"] == "X"


@pytest.mark.asyncio
async def test_get_near_npi_applies_name_before_knn_limit(monkeypatch):
    captured_query_map = {}

    class RecordingConnection:
        async def all(self, sql, **params):
            captured_query_map["sql"] = str(sql)
            captured_query_map["params"] = dict(params)
            return [_build_near_row(5556667778)]

        async def first(self, *_args, **_kwargs):
            return None

    class FakeDB:
        def acquire(self):
            return FakeAcquire(RecordingConnection())

    monkeypatch.setattr(npi_module, "db", FakeDB())
    request = types.SimpleNamespace(
        args={"long": "-87.0", "lat": "41.0", "q": "Clinic", "limit": "1"},
        app=types.SimpleNamespace(),
    )

    response = await npi_module.get_near_npi(request)

    assert len(json.loads(response.body)) == 1
    sql = captured_query_map["sql"]
    name_predicate = npi_module._name_like_clause("d", "q")
    assert name_predicate in sql
    assert sql.index(name_predicate) < sql.index("ORDER BY Geography(")
    assert sql.index(name_predicate) < sql.index("LIMIT :limit")
    assert captured_query_map["params"]["q"] == "%Clinic%"


def _near_provider_record(npi, address_key, distance, **fields):
    return types.SimpleNamespace(
        _mapping={
            "npi_code": npi,
            "npi": npi,
            "address_key": address_key,
            "type": "primary",
            "distance": round(distance / 1609.34, 2),
            "cursor_distance_meters": distance,
            **fields,
        }
    )


def _near_paging_db(first_address_key, second_address_key):
    class PagingConnection:
        async def all(self, sql, **params):
            sql_text = str(sql)
            if "COUNT(DISTINCT (a.npi, a.address_key))" in sql_text:
                return [types.SimpleNamespace(_mapping={"total_count": 2})]
            if params.get("cursor_npi") == 1112223334:
                return [_near_provider_record(5556667778, second_address_key, 200.0)]
            if params.get("cursor_npi") == 5556667778:
                return []
            return [
                _near_provider_record(1112223334, first_address_key, 100.0),
                _near_provider_record(1112223334, first_address_key, 100.0),
                _near_provider_record(5556667778, second_address_key, 200.0),
            ]

        async def first(self, *_args, **_kwargs):
            return None

    class PagingDB:
        def acquire(self):
            return FakeAcquire(PagingConnection())

    return PagingDB()


@pytest.mark.asyncio
async def test_get_near_npi_paginates_unique_provider_addresses(monkeypatch):
    """Page distinct provider-address identities with a stable KNN cursor."""

    first_address_key = "00000000-0000-0000-0000-000000000001"
    second_address_key = "00000000-0000-0000-0000-000000000002"
    monkeypatch.setattr(
        npi_module,
        "db",
        _near_paging_db(first_address_key, second_address_key),
    )
    first_request = types.SimpleNamespace(
        args={
            "long": "-87.0",
            "lat": "41.0",
            "radius": "10",
            "limit": "1",
            "include_total": "1",
        },
        app=types.SimpleNamespace(),
    )

    first_response = await npi_module.get_near_npi(first_request)
    first_payload = json.loads(first_response.body)

    assert first_payload["total_count"] == 2
    assert first_payload["has_more"] is True
    assert first_payload["next_cursor"]
    assert first_payload["result_identity"] == ["npi", "address_key"]
    assert [
        (provider_item["npi"], provider_item["address_key"])
        for provider_item in first_payload["items"]
    ] == [
        (1112223334, first_address_key)
    ]

    second_request = types.SimpleNamespace(
        args={
            "long": "-87.0",
            "lat": "41.0",
            "radius": "10",
            "limit": "1",
            "include_total": "1",
            "cursor": first_payload["next_cursor"],
        },
        app=types.SimpleNamespace(),
    )
    second_response = await npi_module.get_near_npi(second_request)
    second_payload = json.loads(second_response.body)

    assert second_payload["total_count"] == 2
    assert second_payload["has_more"] is False
    assert second_payload["next_cursor"] is None
    assert [
        (provider_item["npi"], provider_item["address_key"])
        for provider_item in second_payload["items"]
    ] == [
        (5556667778, second_address_key)
    ]


def _near_card_provider_record(address_key):
    return _near_provider_record(
        1112223334,
        address_key,
        1609.34,
        entity_type_code=1,
        provider_first_name="Adam",
        provider_last_name="Smith",
        provider_credential_text="MD",
        city_name="Chicago",
        state_name="IL",
        postal_code="60601-1234",
        healthcare_provider_taxonomy_code="207Q00000X",
        healthcare_provider_primary_taxonomy_switch="Y",
        taxonomy_display="Family Medicine",
    )


@pytest.mark.asyncio
async def test_get_near_npi_card_view_keeps_distance_and_compact_fields(monkeypatch):
    """Keep the nearby card response compact without dropping distance."""

    address_key = "00000000-0000-0000-0000-000000000001"

    class CardConnection:
        async def all(self, sql, **_params):
            if "COUNT(DISTINCT" in str(sql):
                return []
            return [_near_card_provider_record(address_key)]

        async def first(self, *_args, **_kwargs):
            return None

    monkeypatch.setattr(
        npi_module.db,
        "acquire",
        lambda: FakeAcquire(CardConnection()),
    )
    response = await npi_module.get_near_npi(
        types.SimpleNamespace(
            args={
                "long": "-87.0",
                "lat": "41.0",
                "view": "card",
                "limit": "1",
            },
            app=types.SimpleNamespace(),
        )
    )

    assert json.loads(response.body) == [
        {
            "npi": 1112223334,
            "display_name": "Adam Smith",
            "entity_type": "individual",
            "credential": "MD",
            "primary_specialty": {
                "taxonomy_code": "207Q00000X",
                "display": "Family Medicine",
            },
            "city": "Chicago",
            "state": "IL",
            "zip5": "60601",
            "distance_miles": 1.0,
        }
    ]


def test_nearby_cursor_rejects_different_filters():
    first_scope = npi_module._nearby_cursor_scope(
        {"lat": "41.0", "long": "-87.0", "radius": "10"}
    )
    cursor = npi_module._encode_nearby_cursor(
        first_scope,
        100.0,
        1112223334,
        "00000000-0000-0000-0000-000000000001",
    )
    second_scope = npi_module._nearby_cursor_scope(
        {"lat": "41.0", "long": "-87.0", "radius": "25"}
    )

    with pytest.raises(Exception, match="cursor is invalid"):
        npi_module._decode_nearby_cursor(cursor, second_scope)


@pytest.mark.asyncio
async def test_get_near_npi_rejects_invalid_provider_sex_code():
    request = types.SimpleNamespace(
        args={
            "long": "-87.0",
            "lat": "41.0",
            "provider_sex_code": "unknown",
        },
        app=types.SimpleNamespace(),
    )

    with pytest.raises(
        sanic.exceptions.InvalidUsage,
        match="provider_sex_code must be one of",
    ):
        await npi_module.get_near_npi(request)


@pytest.mark.asyncio
async def test_get_near_npi_rejects_name_like_legacy_alias():
    request = types.SimpleNamespace(
        args={"name_like": "Clinic", "zip_codes": "60601"},
        app=types.SimpleNamespace(),
    )
    with pytest.raises(sanic.exceptions.InvalidUsage):
        await npi_module.get_near_npi(request)


@pytest.mark.asyncio
async def test_get_near_npi_applies_procedure_and_medication_filters(monkeypatch):
    captured_query_map = {}

    class RecordingConnection:
        async def all(self, sql, **params):
            captured_query_map["sql"] = str(sql)
            captured_query_map["params"] = dict(params)
            return [_build_near_row(1112223334)]

        async def first(self, *_args, **_kwargs):
            return None

    class FakeDB:
        def acquire(self):
            return FakeAcquire(RecordingConnection())

    monkeypatch.setattr(npi_module, "db", FakeDB())
    monkeypatch.setattr(
        npi_module,
        "_resolve_npi_filter_capabilities",
        lambda: asyncio.sleep(0, result={
            "npi_procedures_array_available": True,
            "npi_medications_array_available": True,
            "pricing_provider_procedure_available": False,
            "pricing_provider_prescription_available": False,
        }),
    )

    request = types.SimpleNamespace(
        args={
            "long": "-87.0",
            "lat": "41.0",
            "procedure_codes": "1001,1002",
            "procedure_code_system": "HP_PROCEDURE_CODE",
            "medication_codes": "2001,2002",
            "medication_code_system": "HP_RX_CODE",
            "year": "2023",
            "limit": "1",
        },
        app=types.SimpleNamespace(),
    )
    response = await npi_module.get_near_npi(request)
    response_body = json.loads(response.body)

    assert len(response_body) == 1
    assert "a.procedures_array @> ARRAY[:procedure_code_0]::INTEGER[]" in captured_query_map["sql"]
    assert "a.procedures_array @> ARRAY[:procedure_code_1]::INTEGER[]" in captured_query_map["sql"]
    assert "a.medications_array @> ARRAY[:medication_code_0]::INTEGER[]" in captured_query_map["sql"]
    assert "a.medications_array @> ARRAY[:medication_code_1]::INTEGER[]" in captured_query_map["sql"]
    assert captured_query_map["params"]["procedure_code_0"] == 1001
    assert captured_query_map["params"]["procedure_code_1"] == 1002
    assert captured_query_map["params"]["medication_code_0"] == 2001
    assert captured_query_map["params"]["medication_code_1"] == 2002
    assert captured_query_map["params"]["filter_year"] == 2023


@pytest.mark.asyncio
async def test_get_near_npi_rejects_invalid_medication_code_system():
    request = types.SimpleNamespace(
        args={"lat": "41.0", "long": "-87.0", "medication_codes": "2001", "medication_code_system": "ATC"},
        app=types.SimpleNamespace(),
    )
    with pytest.raises(sanic.exceptions.InvalidUsage):
        await npi_module.get_near_npi(request)


@pytest.mark.asyncio
async def test_get_near_npi_does_not_crash_on_short_positional_rows(monkeypatch):
    short_row = _build_near_row(1112223334)[:-3]
    responses = [[short_row]]
    fake_conn = FakeConnection(responses)

    class FakeDB:
        def acquire(self):
            return FakeAcquire(fake_conn)

    monkeypatch.setattr(npi_module, "db", FakeDB())

    request = types.SimpleNamespace(
        args={"long": "-87.0", "lat": "41.0", "limit": "1"},
        app=types.SimpleNamespace(),
    )
    response = await npi_module.get_near_npi(request)
    response_body = json.loads(response.body)
    assert isinstance(response_body, list)
    assert len(response_body) == 1
    assert response_body[0]["npi"] == 1112223334


@pytest.mark.asyncio
async def test_get_npi_uses_cached_address(monkeypatch):
    async def fake_build(_npi, **_kwargs):
        return {
            "npi": _npi,
            "taxonomy_list": [],
            "taxonomy_group_list": [],
            "do_business_as": [],
            "address_list": [
                {
                    "checksum": 3,
                    "first_line": "30 Main",
                    "second_line": "",
                    "city_name": "Chicago",
                    "state_name": "IL",
                    "postal_code": "60601",
                    "lat": None,
                    "long": None,
                }
            ],
        }

    monkeypatch.setattr(npi_module, "_build_npi_details", fake_build)
    monkeypatch.setattr(npi_module, "_fetch_other_names", AsyncMock(return_value=[]))
    monkeypatch.setattr(npi_module, "download_it", AsyncMock(side_effect=AssertionError("unexpected call")))

    class FakeDB:
        async def scalar(self, *_args, **_kwargs):
            return types.SimpleNamespace(long=-80.0, lat=40.0, formatted_address="Cached", place_id="pid")

    monkeypatch.setattr(npi_module, "db", FakeDB())

    request = types.SimpleNamespace(
        args={"sync_geocode": "0", "lookup_stored_geocode": "1"},
        app=types.SimpleNamespace(config={"NPI_API_UPDATE_GEOCODE": False})
    )
    response = await npi_module.get_npi(request, "1518379601")
    response_body = json.loads(response.body)
    assert response_body["address_list"][0]["lat"] == 40.0


def _install_latless_detail_cache_collaborators(monkeypatch):
    """Install a deterministic latless detail builder and cache identities."""

    for environment_name in (
        "HLTHPRT_NPI_DETAIL_SYNC_GEOCODE",
        "HLTHPRT_NPI_API_SYNC_GEOCODE",
        "HLTHPRT_NPI_DETAIL_LOOKUP_STORED_GEOCODE",
        "HLTHPRT_NPI_API_LOOKUP_STORED_GEOCODE",
    ):
        monkeypatch.delenv(environment_name, raising=False)
    build_call_npis = []

    async def fake_build(_npi, **_kwargs):
        build_call_npis.append(_npi)
        return {
            "npi": _npi,
            "taxonomy_list": [],
            "taxonomy_group_list": [],
            "do_business_as": [],
            "address_list": [
                {
                    "checksum": 4,
                    "first_line": "10 Main",
                    "second_line": "",
                    "city_name": "Chicago",
                    "state_name": "IL",
                    "postal_code": "60601",
                    "lat": None,
                    "long": None,
                }
            ],
        }

    class FakeDB:
        async def scalar(self, *_args, **_kwargs):
            raise AssertionError("stored geocode lookup should not execute")

    monkeypatch.setattr(npi_module, "_NPI_DETAIL_RESPONSE_CACHE", npi_module.OrderedDict())
    monkeypatch.setattr(npi_module, "_NPI_DETAIL_RESPONSE_CACHE_TTL_SECONDS", 300.0)
    monkeypatch.setattr(npi_module, "_NPI_DETAIL_RESPONSE_CACHE_MAX_KEYS", 8)
    monkeypatch.setattr(
        npi_module,
        "_npi_canonical_publication_identity",
        AsyncMock(return_value="1:nppub1_" + "a" * 43),
    )
    monkeypatch.setattr(
        npi_module,
        "_provider_directory_address_overlay_serving_identity",
        AsyncMock(return_value="oid:101"),
    )
    monkeypatch.setattr(npi_module, "_build_npi_details", fake_build)
    monkeypatch.setattr(npi_module, "_fetch_other_names", AsyncMock(return_value=[]))
    monkeypatch.setattr(
        npi_module,
        "_fetch_provider_enrichment_detail",
        AsyncMock(return_value={"summary": None, "enrollments": {}, "ffs_visibility": {}}),
    )
    monkeypatch.setattr(npi_module, "db", FakeDB())
    monkeypatch.setattr(npi_module, "download_it", AsyncMock(side_effect=AssertionError("unexpected geocode call")))
    return build_call_npis


@pytest.mark.asyncio
async def test_get_npi_default_is_storage_only_and_caches_latless(monkeypatch):
    """Cache a latless response without archive or geocoder request-time I/O."""

    build_call_npis = _install_latless_detail_cache_collaborators(monkeypatch)

    request = types.SimpleNamespace(
        args={},
        app=types.SimpleNamespace(config={"NPI_API_UPDATE_GEOCODE": True}),
    )
    first = await npi_module.get_npi(request, "1518379601")
    second = await npi_module.get_npi(request, "1518379601")

    response_body = json.loads(first.body)
    assert response_body["address_list"][0]["lat"] is None
    assert first.body == second.body
    assert len(build_call_npis) == 1


@pytest.mark.asyncio
async def test_get_npi_query_flags_disable_stored_and_live_geocode(monkeypatch):
    captured_keyword_map = {}

    async def fake_build(_npi, **kwargs):
        captured_keyword_map.update(kwargs)
        return {
            "npi": _npi,
            "taxonomy_list": [],
            "taxonomy_group_list": [],
            "do_business_as": [],
            "address_list": [
                {
                    "checksum": 4,
                    "first_line": "10 Main",
                    "second_line": "",
                    "city_name": "Chicago",
                    "state_name": "IL",
                    "postal_code": "60601",
                    "lat": None,
                    "long": None,
                }
            ],
        }

    class FakeDB:
        async def scalar(self, *_args, **_kwargs):
            raise AssertionError("stored geocode lookup should not execute")

    monkeypatch.setenv("HLTHPRT_NPI_DETAIL_SYNC_GEOCODE", "true")
    monkeypatch.setenv("HLTHPRT_NPI_DETAIL_LOOKUP_STORED_GEOCODE", "true")
    monkeypatch.setattr(npi_module, "_build_npi_details", fake_build)
    monkeypatch.setattr(npi_module, "_fetch_other_names", AsyncMock(return_value=[]))
    monkeypatch.setattr(
        npi_module,
        "_fetch_provider_enrichment_summary_detail",
        AsyncMock(return_value={"summary": None, "ffs_visibility": {}}),
    )
    monkeypatch.setattr(npi_module, "db", FakeDB())
    monkeypatch.setattr(npi_module, "download_it", AsyncMock(side_effect=AssertionError("unexpected geocode call")))

    request = types.SimpleNamespace(
        args={
            "view": "summary",
            "sync_geocode": "0",
            "lookup_stored_geocode": "0",
            "include_address_total": "0",
        },
        app=types.SimpleNamespace(config={"NPI_API_UPDATE_GEOCODE": True}),
    )
    response = await npi_module.get_npi(request, "1518379601")
    response_body = json.loads(response.body)

    assert response_body["address_list"][0]["lat"] is None
    assert captured_keyword_map["include_address_total"] is False


@pytest.mark.asyncio
async def test_get_npi_cache_tracks_address_serving_relation_identity(
    monkeypatch,
):
    build_calls = []

    async def fake_build(_npi, **_kwargs):
        build_calls.append(_npi)
        return {
            "npi": _npi,
            "taxonomy_list": [],
            "taxonomy_group_list": [],
            "do_business_as": [],
            "address_list": [{"checksum": 3, "lat": 40.0, "long": -80.0}],
        }

    monkeypatch.setattr(npi_module, "_NPI_DETAIL_RESPONSE_CACHE", npi_module.OrderedDict())
    monkeypatch.setattr(npi_module, "_NPI_DETAIL_RESPONSE_CACHE_TTL_SECONDS", 300.0)
    monkeypatch.setattr(npi_module, "_NPI_DETAIL_RESPONSE_CACHE_MAX_KEYS", 8)
    monkeypatch.setattr(
        npi_module,
        "_npi_canonical_publication_identity",
        AsyncMock(return_value="1:nppub1_" + "a" * 43),
    )
    address_serving_identity = AsyncMock(
        side_effect=[
            "overlay:oid:101|unified:oid:201",
            "overlay:oid:101|unified:oid:201",
            "overlay:oid:101|unified:oid:202",
        ]
    )
    monkeypatch.setattr(
        npi_module,
        "_provider_directory_address_overlay_serving_identity",
        address_serving_identity,
    )
    monkeypatch.setattr(npi_module, "_build_npi_details", fake_build)
    monkeypatch.setattr(npi_module, "_fetch_other_names", AsyncMock(return_value=[]))
    monkeypatch.setattr(
        npi_module,
        "_fetch_provider_enrichment_detail",
        AsyncMock(return_value={"summary": None, "enrollments": {}, "ffs_visibility": {}}),
    )

    request = types.SimpleNamespace(
        args={},
        app=types.SimpleNamespace(config={"NPI_API_UPDATE_GEOCODE": False}),
    )
    first = await npi_module.get_npi(request, "1518379601")
    second = await npi_module.get_npi(request, "1518379601")
    third = await npi_module.get_npi(request, "1518379601")

    assert json.loads(first.body)["npi"] == 1518379601
    assert first.body == second.body
    assert third.body == first.body
    assert len(build_calls) == 2
    assert address_serving_identity.await_count == 3


@pytest.mark.asyncio
async def test_get_npi_cache_rolls_from_profile_fallback_to_adopted_singleton(
    monkeypatch,
):
    """Do not reuse pre-adoption publication metadata for the same generation."""
    profile_fetch = AsyncMock(
        side_effect=_profile_cache_transition_records()
    )
    build_details = AsyncMock(
        side_effect=_npi_details_builder(
            [{"checksum": 3, "lat": 40.0, "long": -80.0}]
        )
    )
    _install_profile_transition_cache_dependencies(
        monkeypatch,
        profile_fetch,
        build_details,
    )
    request = types.SimpleNamespace(
        args={},
        app=types.SimpleNamespace(config={"NPI_API_UPDATE_GEOCODE": False}),
    )

    response_list = [
        await npi_module.get_npi(request, "1518379601")
        for _ in range(3)
    ]

    published_at_list = [
        json.loads(response_item.body)["provider_directory_profile"][
            "published_at"
        ]
        for response_item in response_list
    ]
    assert published_at_list == [
        "2026-07-13T20:00:00Z",
        "2026-07-30T15:00:00Z",
        "2026-07-30T15:00:00Z",
    ]
    assert build_details.await_count == 2


@pytest.mark.asyncio
async def test_get_npi_bypasses_cache_when_overlay_identity_read_fails(
    monkeypatch,
):
    """A transient identity read failure must not use or create stale entries."""
    profile_fetch = AsyncMock(
        return_value=_profile_cache_transition_records()[0]
    )
    build_details = AsyncMock(
        side_effect=_npi_details_builder(
            [{"checksum": 3, "lat": 40.0, "long": -80.0}]
        )
    )
    _install_profile_transition_cache_dependencies(
        monkeypatch,
        profile_fetch,
        build_details,
    )
    monkeypatch.setattr(
        npi_module,
        "_provider_directory_address_overlay_serving_identity",
        AsyncMock(side_effect=RuntimeError("transient identity failure")),
    )
    request = types.SimpleNamespace(
        args={},
        app=types.SimpleNamespace(config={"NPI_API_UPDATE_GEOCODE": False}),
    )

    await npi_module.get_npi(request, "1518379601")
    await npi_module.get_npi(request, "1518379601")

    assert build_details.await_count == 2
    assert npi_module._NPI_DETAIL_RESPONSE_CACHE == {}


@pytest.mark.asyncio
async def test_get_npi_force_address_update_bypasses_response_cache(monkeypatch):
    build_calls = []

    async def fake_build(_npi, **_kwargs):
        build_calls.append(_npi)
        return {
            "npi": _npi,
            "taxonomy_list": [],
            "taxonomy_group_list": [],
            "do_business_as": [],
            "address_list": [{"checksum": 3, "lat": 40.0, "long": -80.0}],
        }

    monkeypatch.setattr(npi_module, "_NPI_DETAIL_RESPONSE_CACHE", npi_module.OrderedDict())
    monkeypatch.setattr(npi_module, "_NPI_DETAIL_RESPONSE_CACHE_TTL_SECONDS", 300.0)
    monkeypatch.setattr(npi_module, "_NPI_DETAIL_RESPONSE_CACHE_MAX_KEYS", 8)
    monkeypatch.setattr(
        npi_module,
        "_provider_directory_address_overlay_serving_identity",
        AsyncMock(return_value="oid:101"),
    )
    monkeypatch.setattr(npi_module, "_build_npi_details", fake_build)
    monkeypatch.setattr(npi_module, "_fetch_other_names", AsyncMock(return_value=[]))
    monkeypatch.setattr(
        npi_module,
        "_fetch_provider_enrichment_detail",
        AsyncMock(return_value={"summary": None, "enrollments": {}, "ffs_visibility": {}}),
    )

    request = types.SimpleNamespace(
        args={},
        app=types.SimpleNamespace(config={"NPI_API_UPDATE_GEOCODE": False}),
    )
    await npi_module.get_npi(request, "1518379601")

    force_request = types.SimpleNamespace(
        args={"force_address_update": "1"},
        app=types.SimpleNamespace(config={"NPI_API_UPDATE_GEOCODE": False}),
    )
    await npi_module.get_npi(force_request, "1518379601")

    assert len(build_calls) == 2


@pytest.mark.asyncio
async def test_get_npi_not_found(monkeypatch):
    monkeypatch.setattr(npi_module, "_build_npi_details", AsyncMock(return_value={}))
    monkeypatch.setattr(
        npi_module,
        "_fetch_npi_location_candidates",
        AsyncMock(return_value=[]),
    )
    monkeypatch.setattr(
        npi_module,
        "_fetch_provider_directory_address_overlay",
        AsyncMock(return_value=[]),
    )
    request = types.SimpleNamespace(args={})
    with pytest.raises(sanic.exceptions.NotFound):
        await npi_module.get_npi(request, "123")


@pytest.mark.asyncio
async def test_get_all_count_only_filters(monkeypatch):
    connections = [FakeConnection([[(5,)]] )]

    class FakeDB:
        def acquire(self):
            return FakeAcquire(connections.pop(0))

    monkeypatch.setattr(npi_module, "db", FakeDB())
    request = types.SimpleNamespace(
        args={
            "count_only": "1",
            "plan_network": "1,2",
            "q": "doc",
            "has_insurance": "1",
            "city": "Chicago",
            "state": "il",
            "classification": "Test",
            "section": "B",
            "display_name": "Display",
            "specialization": "Spec",
            "codes": "1,2",
        },
        app=types.SimpleNamespace(),
    )
    response = await npi_module.get_all(request)
    response_body = json.loads(response.body)
    assert response_body["rows"] == 5


@pytest.mark.asyncio
async def test_get_all_format_full_taxonomy(monkeypatch):
    query_call_map = {}

    class RecordingConnection:
        async def all(self, sql, **params):
            query_call_map['sql'] = sql
            query_call_map['params'] = params
            return [("123", 4)]

        async def first(self, *_args, **_kwargs):
            return None

    monkeypatch.setattr(npi_module.db, 'acquire', lambda: FakeAcquire(RecordingConnection()))
    request = types.SimpleNamespace(args={'count_only': '1', 'format': 'full_taxonomy'}, app=types.SimpleNamespace())
    response = await npi_module.get_all(request)
    response_body = json.loads(response.body)
    assert response_body == {'rows': {'123': 4}}
    assert 'ARRAY[int_code]' in str(query_call_map['sql'])


@pytest.mark.asyncio
async def test_get_all_format_classification(monkeypatch):
    query_call_map = {}

    class RecordingConnection:
        async def all(self, sql, **params):
            query_call_map['sql'] = sql
            query_call_map['params'] = params
            return [("Spec", 7)]

        async def first(self, *_args, **_kwargs):
            return None

    monkeypatch.setattr(npi_module.db, 'acquire', lambda: FakeAcquire(RecordingConnection()))
    request = types.SimpleNamespace(args={'count_only': '1', 'format': 'classification'}, app=types.SimpleNamespace())
    response = await npi_module.get_all(request)
    response_body = json.loads(response.body)
    assert response_body == {'rows': {'Spec': 7}}
    assert 'classification' in str(query_call_map['sql'])


@pytest.mark.asyncio
async def test_get_all_format_all_returns_classification_map(monkeypatch):
    query_call_map = {}

    class RecordingConnection:
        async def all(self, sql, **params):
            query_call_map['sql'] = sql
            query_call_map['params'] = params
            return [("Pharmacy", 12), ("Pharmacist", 33)]

        async def first(self, *_args, **_kwargs):
            return None

    monkeypatch.setattr(npi_module.db, 'acquire', lambda: FakeAcquire(RecordingConnection()))
    request = types.SimpleNamespace(
        args={'count_only': '1', 'format': 'all', 'state': 'NE', 'has_insurance': '1'},
        app=types.SimpleNamespace(),
    )
    response = await npi_module.get_all(request)
    response_body = json.loads(response.body)
    assert response_body == {'rows': {'Pharmacy': 12, 'Pharmacist': 33}}
    assert 'GROUP BY q.classification' in str(query_call_map['sql'])
    assert query_call_map['params']['state'] == 'NE'


@pytest.mark.asyncio
async def test_get_all_deduplicates_rows(monkeypatch):
    connections = [
        FakeConnection([[(1,)]]),
        FakeConnection(
            [[_build_result_row(999), _build_result_row(999)], []]
        ),
    ]

    class FakeDB:
        def acquire(self):
            return FakeAcquire(connections.pop(0))

    monkeypatch.setattr(npi_module, 'db', FakeDB())
    request = types.SimpleNamespace(args={'limit': '1'}, app=types.SimpleNamespace())
    response = await npi_module.get_all(request)
    response_body = json.loads(response.body)
    assert response_body['total'] == 1
    assert len(response_body['rows']) == 1


@pytest.mark.asyncio
async def test_build_npi_details_empty(monkeypatch):
    class FakeQuery:
        def __init__(self, rows):
            self._rows = rows

        def select_from(self, *_args, **_kwargs):
            return self

        def where(self, *_args, **_kwargs):
            return self

        def outerjoin(self, *_args, **_kwargs):
            return self

        def group_by(self, *_args, **_kwargs):
            return self

        def alias(self, *_args, **_kwargs):
            alias_table = table('address_alias', column('npi'), column('type'))
            return alias_table.alias('address_list')

        def order_by(self, *_args, **_kwargs):
            return self

        async def all(self):
            return self._rows

    class DummyAcquire:
        async def __aenter__(self):
            return None

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class FakeDB:
        def __init__(self):
            self.query = FakeQuery([])

        def select(self, *_args, **_kwargs):
            return self.query

        def acquire(self):
            return DummyAcquire()

    fake_db = FakeDB()
    monkeypatch.setattr(npi_module, 'db', fake_db)
    monkeypatch.setattr(npi_module.random, 'choice', lambda seq: seq[0])
    # Keep SQLAlchemy select objects for the NPI-first optimization barrier.

    detail_result = await npi_module._build_npi_details(123)
    assert detail_result == {}


@pytest.mark.asyncio
async def test_get_npi_address_geocode_paths(monkeypatch):
    """Verify get npi address geocode paths."""
    addresses = [
        {
            "npi": 123, "type": "primary", "checksum": 1,
            "first_line": "10 Main St", "second_line": "", "city_name": "Town",
            "state_name": "IL", "postal_code": "12345", "lat": None, "long": None,
            "formatted_address": None, "plans_network_array": [], "taxonomy_array": [],
        },
        {
            "npi": 123, "type": "secondary", "checksum": 2,
            "first_line": "20 Oak St", "second_line": "", "city_name": "Town",
            "state_name": "IL", "postal_code": "123456789", "lat": None, "long": None,
            "formatted_address": None, "plans_network_array": [], "taxonomy_array": [],
        },
    ]

    mapbox_response = json.dumps({
        'features': [
            {
                'geometry': {'coordinates': [10.0, 20.0]},
                'matching_place_name': 'Match Address'
            }
        ]
    })

    responses = [mapbox_response, Exception('mapbox failure'), Exception('google failure')]

    async def fake_download(url, local_timeout=None):
        value = responses.pop(0)
        if isinstance(value, Exception):
            raise value
        return value

    monkeypatch.setattr(
        npi_module,
        "_build_npi_details",
        _npi_details_builder(addresses, do_business_as=["Existing DBA"]),
    )
    monkeypatch.setattr(npi_module, '_fetch_other_names', AsyncMock(return_value=[{
        'other_provider_identifier': 'DBA FROM OTHER',
        'other_provider_identifier_type_code': '3',
    }]))
    monkeypatch.setattr(npi_module, 'download_it', AsyncMock(side_effect=fake_download))
    monkeypatch.setattr(npi_module, 'db', _GeocodePathDB())
    monkeypatch.setattr(npi_module.random, 'choice', lambda seq: seq[0])
    app = _NpiEndpointTestApp({
        'GEOCODE_MAPBOX_STYLE_URL': 'https://mapbox/',
        'GEOCODE_MAPBOX_STYLE_ADDRESS_PARAM': 'address',
        'GEOCODE_MAPBOX_STYLE_KEY_PARAM': 'key', 'GEOCODE_MAPBOX_STYLE_KEY': '["key123"]',
        'GEOCODE_GOOGLE_STYLE_ADDRESS_PARAM': 'address',
        'GEOCODE_GOOGLE_STYLE_KEY_PARAM': 'key', 'GEOCODE_GOOGLE_STYLE_KEY': 'k',
        'GEOCODE_GOOGLE_STYLE_URL': 'https://maps.googleapis.com',
        'NPI_API_UPDATE_GEOCODE': True,
    }, close_tasks=True)
    request = types.SimpleNamespace(args={'force_address_update': '1'}, app=app)
    response = await npi_module.get_npi(request, '123')
    response_body = json.loads(response.body)
    assert response_body['do_business_as'] == ['Existing DBA']
    assert response_body['address_list'][0]['formatted_address'] == '10 Main Street, Town, IL 12345'


@pytest.mark.asyncio
async def test_get_npi_not_found(monkeypatch):
    async def fake_build(_npi, **_kwargs):
        return {}

    monkeypatch.setattr(npi_module, '_build_npi_details', AsyncMock(side_effect=fake_build))
    monkeypatch.setattr(
        npi_module,
        "_fetch_npi_location_candidates",
        AsyncMock(return_value=[]),
    )
    monkeypatch.setattr(
        npi_module,
        "_fetch_provider_directory_address_overlay",
        AsyncMock(return_value=[]),
    )

    request = types.SimpleNamespace(args={}, app=types.SimpleNamespace())
    with pytest.raises(sanic.exceptions.NotFound):
        await npi_module.get_npi(request, '123')


@pytest.mark.asyncio
async def test_get_npi_update_addr_coordinates_row_missing(monkeypatch):
    """Verify get npi update addr coordinates row missing."""
    address_entry = _archive_address_entry(1, "10 Main St")
    monkeypatch.setattr(
        npi_module, "_build_npi_details", _npi_details_builder([address_entry])
    )
    monkeypatch.setattr(npi_module, '_fetch_other_names', AsyncMock(return_value=[]))
    monkeypatch.setattr(
        npi_module,
        '_fetch_provider_enrichment_summary_detail',
        AsyncMock(return_value={
            'summary': None,
            'enrollments': {
                'ffs_public': [
                    {
                        'address_key': '00000000-0000-0000-0000-000000000002',
                        'address_line_1': '10 Main',
                    }
                ]
            },
        }),
    )
    monkeypatch.setattr(npi_module.random, 'choice', lambda seq: seq[0])

    class FakeDB:
        def __init__(self):
            self.scalar_values = [_make_address_row(), None]

        def update(self, *_args, **_kwargs):
            return FakeUpdate()

        def insert(self, *_args, **_kwargs):
            raise AssertionError('address archive insert should not run when row missing')

        async def scalar(self, *_args, **_kwargs):
            if self.scalar_values:
                return self.scalar_values.pop(0)
            return None

    fake_db = FakeDB()
    monkeypatch.setattr(npi_module, 'db', fake_db)

    async def fail_download(*_args, **_kwargs):  # pragma: no cover - guard
        raise AssertionError('download should not execute when archive hit')

    monkeypatch.setattr(npi_module, 'download_it', fail_download)

    app = _NpiEndpointTestApp({"NPI_API_UPDATE_GEOCODE": True})
    request = types.SimpleNamespace(
        args={"lookup_stored_geocode": "1"},
        app=app,
    )
    response = await npi_module.get_npi(request, '1518379601')
    response_body = json.loads(response.body)
    assert response_body['address_list'][0]['lat'] == 41.0
    assert app.tasks, "expected geocode update task"
    await app.tasks[0]


@pytest.mark.asyncio
async def test_get_npi_update_addr_coordinates_handles_exception(monkeypatch):
    """Verify get npi update addr coordinates handles exception."""
    address_entry = _archive_address_entry(2, "11 Main St")
    monkeypatch.setattr(
        npi_module, "_build_npi_details", _npi_details_builder([address_entry])
    )
    monkeypatch.setattr(npi_module, '_fetch_other_names', AsyncMock(return_value=[]))
    monkeypatch.setattr(npi_module.random, 'choice', lambda seq: seq[0])

    class ErrorInsert:
        def __init__(self):
            self.values_payload = None

        def values(self, obj):
            self.values_payload = obj
            return self

        def on_conflict_do_update(self, **_kwargs):
            return self

        async def status(self):
            raise RuntimeError('boom')

    class FakeDB:
        def __init__(self):
            self.scalar_values = [_make_address_row(), _make_address_row()]

        def update(self, *_args, **_kwargs):
            return FakeUpdate()

        def insert(self, *_args, **_kwargs):
            return ErrorInsert()

        async def scalar(self, *_args, **_kwargs):
            if self.scalar_values:
                return self.scalar_values.pop(0)
            return None

    fake_db = FakeDB()
    monkeypatch.setattr(npi_module, 'db', fake_db)

    async def fail_download(*_args, **_kwargs):  # pragma: no cover - guard
        raise AssertionError('download should not execute when archive hit')

    monkeypatch.setattr(npi_module, 'download_it', fail_download)

    app = _NpiEndpointTestApp({"NPI_API_UPDATE_GEOCODE": True})
    request = types.SimpleNamespace(
        args={"lookup_stored_geocode": "1"},
        app=app,
    )
    response = await npi_module.get_npi(request, '1518379601')
    response_body = json.loads(response.body)
    assert response_body['address_list'][0]['lat'] == 41.0
    assert app.tasks
    # The task should swallow the insert exception
    await app.tasks[0]


@pytest.mark.asyncio
async def test_get_npi_skip_update_when_lat_present(monkeypatch):
    address_entry_map = {
        'npi': 1518379601,
        'type': 'primary',
        'checksum': 3,
        'first_line': '12 Main St',
        'second_line': '',
        'city_name': 'Town',
        'state_name': 'IL',
        'postal_code': '123450000',
        'lat': 40.0,
        'long': -80.0,
        'formatted_address': 'Town, IL',
        'plans_network_array': [],
        'taxonomy_array': [],
    }

    async def fake_build(_npi, **_kwargs):
        return {
            'npi': _npi,
            'do_business_as': [],
            'taxonomy_list': [],
            'taxonomy_group_list': [],
            'address_list': [dict(address_entry_map)],
        }

    monkeypatch.setattr(npi_module, '_build_npi_details', fake_build)
    monkeypatch.setattr(npi_module, '_fetch_other_names', AsyncMock(return_value=[]))

    class FakeDB:
        async def scalar(self, *_args, **_kwargs):
            raise AssertionError('archive lookup should be skipped when lat present')

        def update(self, *_args, **_kwargs):
            raise AssertionError('update should not be invoked')

    monkeypatch.setattr(npi_module, 'db', FakeDB())

    async def fail_download(*_args, **_kwargs):  # pragma: no cover - guard
        raise AssertionError('download should not run when coordinates exist')

    monkeypatch.setattr(npi_module, 'download_it', fail_download)

    tasks = []

    class FakeApp:
        def __init__(self):
            self.config = {'NPI_API_UPDATE_GEOCODE': True}

        def add_task(self, coro):
            tasks.append(coro)

    request = types.SimpleNamespace(args={}, app=FakeApp())
    response = await npi_module.get_npi(request, '1518379601')
    response_body = json.loads(response.body)
    assert response_body['address_list'][0]['lat'] == 40.0
    assert tasks == []


@pytest.mark.asyncio
async def test_get_npi_v2_archive_is_disabled_without_cutover_flag(monkeypatch):
    monkeypatch.delenv("HLTHPRT_ADDRESS_ARCHIVE_CUTOVER", raising=False)
    address_entry_map = {
        'npi': 1518379601,
        'type': 'primary',
        'checksum': 4,
        'first_line': '13 Main St',
        'second_line': '',
        'city_name': 'Town',
        'state_name': 'IL',
        'postal_code': '123450000',
        'lat': None,
        'long': None,
        'formatted_address': None,
        'plans_network_array': [],
        'taxonomy_array': [],
    }

    async def fake_build(_npi, **_kwargs):
        return {
            'npi': _npi,
            'do_business_as': [],
            'taxonomy_list': [],
            'taxonomy_group_list': [],
            'address_list': [dict(address_entry_map)],
        }

    monkeypatch.setattr(npi_module, '_build_npi_details', fake_build)
    monkeypatch.setattr(npi_module, '_fetch_other_names', AsyncMock(return_value=[]))

    class FakeDB:
        async def first(self, *_args, **_kwargs):
            raise AssertionError('v2 archive lookup should require explicit cutover flag')

        async def scalar(self, *_args, **_kwargs):
            return _make_address_row()

    monkeypatch.setattr(npi_module, 'db', FakeDB())

    async def fail_download(*_args, **_kwargs):  # pragma: no cover - guard
        raise AssertionError('download should not run when legacy archive hit')

    monkeypatch.setattr(npi_module, 'download_it', fail_download)

    class FakeApp:
        config = {'NPI_API_UPDATE_GEOCODE': False}

        def add_task(self, coro):  # pragma: no cover - guard
            raise AssertionError('no geocode update task expected')

    request = types.SimpleNamespace(
        args={"lookup_stored_geocode": "1"}, app=FakeApp()
    )
    response = await npi_module.get_npi(request, '1518379601')
    response_body = json.loads(response.body)
    assert response_body['address_list'][0]['lat'] == 41.0


@pytest.mark.asyncio
async def test_get_npi_v2_archive_cutover_reads_geocodes_for_concurrent_addresses(monkeypatch):
    """Verify get npi v2 archive cutover reads geocodes for concurrent addresses."""
    monkeypatch.setenv("HLTHPRT_ADDRESS_ARCHIVE_CUTOVER", "1")
    monkeypatch.setenv("HLTHPRT_ADDRESS_ARCHIVE_TABLE", "address_archive_v2")
    addresses = [
        _archive_address_entry(10, "10 Main St", country="US"),
        _archive_address_entry(11, "11 Main St", address_type="secondary", country="US"),
    ]
    monkeypatch.setattr(
        npi_module, "_build_npi_details", _npi_details_builder(addresses)
    )
    monkeypatch.setattr(npi_module, '_fetch_other_names', AsyncMock(return_value=[]))

    class FakeDB:
        def __init__(self):
            self.catalog_calls = 0
            self.first_calls = 0

        async def scalar(self, query, **_kwargs):
            if not isinstance(query, str):
                raise AssertionError('legacy checksum archive should not be used when v2 geocode exists')
            self.catalog_calls += 1
            if 'to_regclass' in query:
                await asyncio.sleep(0)
                return 'mrf.address_archive_v2'
            if 'information_schema.columns' in query:
                return True
            if 'to_regprocedure' in query:
                return 'mrf.addr_key_v1(text,text,text,text,text,text)'
            raise AssertionError(query)

        async def first(self, *_args, **_kwargs):
            self.first_calls += 1
            return _make_v2_archive_row(lat=42.0 + self.first_calls)

    fake_db = FakeDB()
    monkeypatch.setattr(npi_module, 'db', fake_db)

    async def fail_download(*_args, **_kwargs):  # pragma: no cover - guard
        raise AssertionError('download should not run when v2 archive hit')

    monkeypatch.setattr(npi_module, 'download_it', fail_download)

    app = _NpiEndpointTestApp({"NPI_API_UPDATE_GEOCODE": False}, reject_tasks=True)
    request = types.SimpleNamespace(
        args={"lookup_stored_geocode": "1"},
        app=app,
    )
    response = await npi_module.get_npi(request, '1518379601')
    response_body = json.loads(response.body)

    assert [address['lat'] for address in response_body['address_list']] == [43.0, 44.0]
    assert fake_db.first_calls == 2
    assert fake_db.catalog_calls == 4


@pytest.mark.asyncio
async def test_get_npi_v2_archive_geocodeless_row_falls_back_to_legacy(monkeypatch):
    """Verify get npi v2 archive geocodeless row falls back to legacy."""
    monkeypatch.setenv("HLTHPRT_ADDRESS_ARCHIVE_CUTOVER", "true")
    address_entry = _archive_address_entry(12, "12 Main St", country="US")
    monkeypatch.setattr(
        npi_module,
        "_build_npi_details",
        _npi_details_builder([address_entry]),
    )
    monkeypatch.setattr(npi_module, '_fetch_other_names', AsyncMock(return_value=[]))

    class FakeDB:
        async def scalar(self, query, **_kwargs):
            if isinstance(query, str):
                if 'to_regclass' in query:
                    return 'mrf.address_archive_v2'
                if 'information_schema.columns' in query:
                    return True
                if 'to_regprocedure' in query:
                    return 'mrf.addr_key_v1(text,text,text,text,text,text)'
                raise AssertionError(query)
            return _make_address_row()

        async def first(self, *_args, **_kwargs):
            return None

    monkeypatch.setattr(npi_module, 'db', FakeDB())

    async def fail_download(*_args, **_kwargs):  # pragma: no cover - guard
        raise AssertionError('download should not run when legacy archive hit')
    monkeypatch.setattr(npi_module, 'download_it', fail_download)
    app = _NpiEndpointTestApp(
        {"NPI_API_UPDATE_GEOCODE": False},
        reject_tasks=True,
    )
    request = types.SimpleNamespace(
        args={"lookup_stored_geocode": "1"},
        app=app,
    )
    response = await npi_module.get_npi(request, '1518379601')
    response_body = json.loads(response.body)
    assert response_body['address_list'][0]['lat'] == 41.0


class _V2GeocodeWriteDB:
    def __init__(self):
        self.status_sql = []
        self.status_kwargs = []
        self.update_criteria = []
        self.update_values = {}

    async def scalar(self, query, **_kwargs):
        if isinstance(query, str):
            if 'to_regclass' in query:
                return 'mrf.address_archive_v2'
            if 'information_schema.columns' in query:
                return True
            if 'to_regprocedure' in query:
                return 'mrf.addr_key_v1(text,text,text,text,text,text)'
            raise AssertionError(query)
        return types.SimpleNamespace()

    async def first(self, *_args, **_kwargs):
        return _make_v2_archive_row(lat=45.0)

    def update(self, *_args, **_kwargs):
        recording_db = self

        class RecordingUpdate:
            def where(self, criterion):
                recording_db.update_criteria.append(str(criterion))
                return self

            def values(self, **values):
                recording_db.update_values.update(values)
                return self

            async def status(self):
                return None

        return RecordingUpdate()

    def insert(self, *_args, **_kwargs):
        raise AssertionError('legacy AddressArchive insert should not run during v2 cutover')

    async def status(self, sql, **_kwargs):
        self.status_sql.append(sql)
        self.status_kwargs.append(_kwargs)
        return 1


def _assert_v2_geocode_upsert(fake_db):
    """Assert exact deduplication and source-preserving v2 archive SQL."""
    upsert_sql = "\n".join(fake_db.status_sql)
    for expected_sql in (
        "INSERT INTO mrf.address_archive_v2", "SELECT DISTINCT ON",
        "ON CONFLICT (address_key) DO UPDATE", "WHERE checksum = :checksum",
        "AND npi = :npi", "AND type = :address_type",
        "LEFT(mrf.addr_state_code_v1(state_name), 32)",
        "mrf.addr_formatted_address_v2(",
        "formatted_address_version = EXCLUDED.formatted_address_version",
        "formatted_address_source = EXCLUDED.formatted_address_source",
        "geo_source, geocode_source, geocode_quality",
        "CAST(:geo_source AS mrf.address_archive_geo_source)",
        "geo_source = COALESCE(mrf.address_archive_v2.geo_source, EXCLUDED.geo_source)",
    ):
        assert expected_sql in upsert_sql
    assert fake_db.status_kwargs[-1]["geo_source"] == "google"
    assert fake_db.status_kwargs[-1]["npi"] == 1518379601
    assert fake_db.status_kwargs[-1]["address_type"] == "primary"


@pytest.mark.asyncio
async def test_get_npi_v2_archive_geocode_write_uses_deduped_key_upsert(monkeypatch):
    """Verify get npi v2 archive geocode write uses deduped address key upsert."""
    monkeypatch.setenv("HLTHPRT_ADDRESS_ARCHIVE_CUTOVER", "1")
    address_entry = _archive_address_entry(13, "13 Main St", country="US")
    monkeypatch.setattr(
        npi_module, "_build_npi_details", _npi_details_builder([address_entry])
    )
    monkeypatch.setattr(npi_module, '_fetch_other_names', AsyncMock(return_value=[]))

    fake_db = _V2GeocodeWriteDB()
    monkeypatch.setattr(npi_module, 'db', fake_db)

    async def fail_download(*_args, **_kwargs):  # pragma: no cover - guard
        raise AssertionError('download should not run when v2 archive hit')

    monkeypatch.setattr(npi_module, 'download_it', fail_download)

    app = _NpiEndpointTestApp({"NPI_API_UPDATE_GEOCODE": True})
    request = types.SimpleNamespace(
        args={"lookup_stored_geocode": "1"},
        app=app,
    )
    response = await npi_module.get_npi(request, '1518379601')
    response_body = json.loads(response.body)
    assert response_body['address_list'][0]['lat'] == 45.0
    assert app.tasks
    await app.tasks[0]

    _assert_v2_geocode_upsert(fake_db)
    assert any("npi_address.npi" in criterion for criterion in fake_db.update_criteria)
    assert any("npi_address.type" in criterion for criterion in fake_db.update_criteria)
    assert "mrf.addr_formatted_address_v2(" in str(
        fake_db.update_values["formatted_address"]
    )


@pytest.mark.asyncio
async def test_get_npi_exposes_address_key_and_hides_premise_key(monkeypatch):
    async def fake_build(_npi, **_kwargs):
        return {
            'npi': _npi,
            'do_business_as': [],
            'taxonomy_list': [],
            'taxonomy_group_list': [],
            'address_list': [
                {
                    'npi': _npi,
                    'type': 'primary',
                    'checksum': 5,
                    'address_key': '00000000-0000-0000-0000-000000000001',
                    'premise_key': '00000000-0000-0000-0000-000000000002',
                    'lat': 40.0,
                    'long': -80.0,
                    'formatted_address': 'Town, IL',
                    'plans_network_array': [],
                    'taxonomy_array': [],
                }
            ],
        }

    monkeypatch.setattr(npi_module, '_build_npi_details', fake_build)
    monkeypatch.setattr(npi_module, '_fetch_other_names', AsyncMock(return_value=[]))

    class FakeApp:
        config = {'NPI_API_UPDATE_GEOCODE': False}

        def add_task(self, coro):  # pragma: no cover - guard
            raise AssertionError('no task expected')

    request = types.SimpleNamespace(args={}, app=FakeApp())
    response = await npi_module.get_npi(request, '1518379601')
    response_body = json.loads(response.body)
    assert response_body['address_list'][0]['address_key'] == '00000000-0000-0000-0000-000000000001'
    assert response_body['address_list'][0]['address_site_key'] == '00000000-0000-0000-0000-000000000002'
    assert 'premise_key' not in response_body['address_list'][0]
    assert 'premise_key' not in json.dumps(response_body)


@pytest.mark.asyncio
async def test_get_npi_debug_flags_include_sources_and_evidence(monkeypatch):
    captured_keyword_map = {}

    async def fake_build(_npi, **kwargs):
        captured_keyword_map.update(kwargs)
        return {
            'npi': _npi,
            'do_business_as': [],
            'taxonomy_list': [],
            'taxonomy_group_list': [],
            'address_list': [
                {
                    'npi': _npi,
                    'type': 'primary',
                    'checksum': 5,
                    'address_key': '00000000-0000-0000-0000-000000000001',
                    'premise_key': '00000000-0000-0000-0000-000000000002',
                    'lat': 40.0,
                    'long': -80.0,
                    'location_key': 'loc-debug',
                    'address_sources': ['npi', 'mrf'],
                    'source_record_ids': ['npi:1518379601', 'mrf:row-1'],
                    'source_count': 2,
                    'plans_network_array': [],
                    'taxonomy_array': [],
                }
            ],
        }

    monkeypatch.setattr(npi_module, '_build_npi_details', fake_build)
    monkeypatch.setattr(npi_module, '_fetch_other_names', AsyncMock(return_value=[]))

    class FakeApp:
        config = {'NPI_API_UPDATE_GEOCODE': False}

        def add_task(self, coro):  # pragma: no cover - guard
            raise AssertionError('no task expected')

    request = types.SimpleNamespace(
        args={'include_sources': 'true', 'include_evidence': 'true'},
        app=FakeApp(),
    )
    response = await npi_module.get_npi(request, '1518379601')
    response_body = json.loads(response.body)
    address = response_body['address_list'][0]

    assert captured_keyword_map['include_sources'] is True
    assert captured_keyword_map['include_evidence'] is True
    assert address['address_sources'] == ['npi', 'mrf']
    assert address['source_record_ids'] == ['npi:1518379601', 'mrf:row-1']
    assert address['source_count'] == 2
    assert address['address_key'] == '00000000-0000-0000-0000-000000000001'
    assert address['address_site_key'] == '00000000-0000-0000-0000-000000000002'
    assert 'premise_key' not in address


@pytest.mark.asyncio
async def test_get_npi_hides_provider_directory_source_details_by_default(monkeypatch):
    """Verify get npi hides provider directory source address_details_map by default."""
    async def fake_build(_npi, **_kwargs):
        return {
            'npi': _npi,
            'do_business_as': [],
            'taxonomy_list': [],
            'taxonomy_group_list': [],
            'address_list': [
                {
                    'npi': _npi,
                    'type': 'practice',
                    'checksum': 5,
                    'address_key': '00000000-0000-0000-0000-000000000001',
                    'address_precision': 'street',
                    'first_line': '1 Main St',
                    'city_name': 'Bloomfield',
                    'state_name': 'CT',
                    'postal_code': '06002',
                    'lat': 41.0,
                    'long': -72.0,
                    'formatted_address': None,
                    'place_id': None,
                    'address_sources': ['provider_directory_fhir'],
                    'source_record_ids': [
                        'provider_directory_fhir:practitioner_role:pdfhir_cigna:role-1:loc-1'
                    ],
                    'source_count': 1,
                    'plans_network_array': [],
                    'taxonomy_array': [],
                }
            ],
        }
    fetch_details = AsyncMock(return_value={
        'pdfhir_cigna': {
            'source': 'provider_directory_fhir',
            'source_id': 'pdfhir_cigna',
            'org_name': 'Cigna',
            'plan_name': 'Commercial',
            'canonical_api_base': 'https://fhir.cigna.com/ProviderDirectory/v1',
            'api_base': 'https://fhir.cigna.com/ProviderDirectory/v1',
            'auth_type': 'none',
            'last_validated_status': 'valid',
        }
    })
    monkeypatch.setattr(npi_module, '_build_npi_details', fake_build)
    monkeypatch.setattr(npi_module, '_fetch_provider_directory_source_detail_map', fetch_details)
    monkeypatch.setattr(npi_module, '_fetch_other_names', AsyncMock(return_value=[]))
    class FakeApp:
        config = {'NPI_API_UPDATE_GEOCODE': False}
        def add_task(self, coro):  # pragma: no cover - guard
            raise AssertionError('no task expected')
    request = types.SimpleNamespace(args={}, app=FakeApp())
    response = await npi_module.get_npi(request, '1518379602')
    response_body = json.loads(response.body)
    address = response_body['address_list'][0]
    fetch_details.assert_not_awaited()
    assert 'provider_directory_sources' not in address
    assert address['address_sources'] == ['provider_directory_fhir']
    assert 'source_record_ids' not in address


@pytest.mark.asyncio
async def test_get_npi_include_sources_enriches_provider_directory_source_summary(monkeypatch):
    """Verify get npi include sources enriches provider directory source summary."""
    fetch_details = AsyncMock(
        return_value={"pdfhir_cigna": _provider_directory_source_detail()}
    )
    monkeypatch.setattr(
        npi_module,
        "_build_npi_details",
        _npi_details_builder([_provider_directory_address_entry()]),
    )
    monkeypatch.setattr(npi_module, '_fetch_provider_directory_source_detail_map', fetch_details)
    monkeypatch.setattr(npi_module, '_fetch_other_names', AsyncMock(return_value=[]))
    app = _NpiEndpointTestApp({"NPI_API_UPDATE_GEOCODE": False}, reject_tasks=True)
    request = types.SimpleNamespace(args={"include_sources": "true"}, app=app)
    response = await npi_module.get_npi(request, '1518379602')
    response_body = json.loads(response.body)
    address = response_body['address_list'][0]

    fetch_details.assert_awaited_once_with(['pdfhir_cigna'], session=None)
    assert address['provider_directory_sources'] == [
        {
            'source': 'provider_directory_fhir',
            'source_ids': ['pdfhir_cigna'],
            'endpoint_id': 'pd_endpoint_cigna',
            'catalog_aliases_verified': False,
            'catalog_aliases': [
                {
                    'source_id': 'pdfhir_cigna',
                    'org_name': 'Cigna',
                    'plan_name': 'Commercial',
                }
            ],
        }
    ]
    source_json = json.dumps(address['provider_directory_sources'])
    endpoint_source = address['provider_directory_sources'][0]
    for sensitive_key in (
        'api_base',
        'canonical_api_base',
        'auth_type',
        'auth_required',
        'requires_api_key',
        'credential_name',
        'headers',
    ):
        assert sensitive_key not in endpoint_source
    assert 'secret-token' not in source_json
    assert address['address_sources'] == ['provider_directory_fhir']
    assert 'source_record_ids' not in address


@pytest.mark.asyncio
async def test_get_npi_address_list_clears_empty_entries(monkeypatch):
    async def fake_build(_npi, **_kwargs):
        return {
            'npi': _npi,
            'do_business_as': [],
            'taxonomy_list': [],
            'taxonomy_group_list': [],
            'address_list': [None],
        }

    monkeypatch.setattr(npi_module, '_build_npi_details', fake_build)
    monkeypatch.setattr(npi_module, '_fetch_other_names', AsyncMock(return_value=[]))

    class FakeApp:
        def __init__(self):
            self.config = {}
            self.tasks = []

        def add_task(self, coro):
            raise AssertionError('no geocode tasks expected when address list empty')

    request = types.SimpleNamespace(args={}, app=FakeApp())
    response = await npi_module.get_npi(request, '1518379601')
    response_body = json.loads(response.body)
    assert response_body['address_list'] == []


def test_dedupe_addresses_merges_nondiscriminating_site_key_with_fhir_overlay():
    address_key = "00000000-0000-0000-0000-000000000001"
    addresses = [
        {
            "address_key": address_key,
            "address_site_key": address_key,
            "first_line": "1600 11th St",
            "type": "practice",
            "phone_number": "9407647000",
            "lat": 33.90559,
            "long": -98.47978,
            "address_sources": ["nppes"],
            "source_record_ids": ["npi:1588616783"],
        },
        {
            "address_key": address_key,
            "first_line": "1600 11th St",
            "type": "practice",
            "address_sources": ["provider_directory_fhir"],
            "source_record_ids": [
                "provider_directory_fhir:practitioner_role:pdfhir_source:role-1:location-1"
            ],
        },
    ]

    deduped = npi_module._dedupe_addresses_by_key(addresses)

    assert len(deduped) == 1
    merged_address = deduped[0]
    assert merged_address["address_site_key"] == address_key
    assert merged_address["phone_number"] == "9407647000"
    assert (merged_address["lat"], merged_address["long"]) == (33.90559, -98.47978)
    assert merged_address["address_sources"] == ["nppes", "provider_directory_fhir"]
    assert len(merged_address["source_record_ids"]) == 2
