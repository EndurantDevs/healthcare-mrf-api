# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import json
import types
import asyncio

import pytest
import sanic.exceptions

from api.endpoint import npi as npi_module
from api.endpoint.npi import get_all


class RecordingConnection:
    def __init__(self):
        self.calls = 0
        self.last_sql = None
        self.last_params = None
        self.sql_calls = []

    async def all(self, sql, **params):
        self.calls += 1
        self.last_sql = str(sql)
        self.last_params = params
        self.sql_calls.append((str(sql), dict(params)))
        # first call: count; second call: results
        if self.calls == 1:
            return [(5,)]
        return []

    async def first(self, *_args, **_kwargs):
        return None


class FakeAcquire:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, exc_type, exc, tb):
        return False


@pytest.mark.asyncio
async def test_get_all_include_total_false_skips_count_query(monkeypatch):
    conn = RecordingConnection()
    monkeypatch.setattr(npi_module.db, "acquire", lambda: FakeAcquire(conn))

    request = types.SimpleNamespace(
        args={
            "classification": "Pharmacy",
            "limit": "5",
            "start": "10",
            "include_total": "0",
        }
    )
    resp = await get_all(request)
    data = json.loads(resp.body)

    assert conn.calls == 1
    assert data["total"] == 10
    assert data["total_source"] == "estimated_page_floor"


@pytest.mark.asyncio
async def test_get_all_sitemap_mode_allows_20000_limit(monkeypatch):
    class SitemapConnection:
        def __init__(self):
            self.calls = 0
            self.last_params = None

        async def all(self, _sql, **params):
            self.calls += 1
            self.last_params = params
            return []

    conn = SitemapConnection()
    monkeypatch.setattr(npi_module.db, "acquire", lambda: FakeAcquire(conn))
    async def fake_npi_list(*_args, **_kwargs):
        return list(range(1_000_000_000, 1_000_030_000))
    monkeypatch.setattr(
        npi_module,
        "_get_classification_npi_list",
        fake_npi_list,
    )

    request = types.SimpleNamespace(
        args={
            "classification": "Pharmacy",
            "view": "sitemap",
            "limit": "20000",
            "start": "0",
            "include_total": "0",
        }
    )
    resp = await get_all(request)
    data = json.loads(resp.body)

    assert data["limit"] == 20000
    assert conn.calls == 1
    assert len(conn.last_params["page_npis"]) == 20000


@pytest.mark.asyncio
async def test_get_all_q_filter(monkeypatch):
    conn = RecordingConnection()
    monkeypatch.setattr(npi_module.db, "acquire", lambda: FakeAcquire(conn))

    request = types.SimpleNamespace(
        args={
            "classification": "Pharmacy",
            "state": "RI",
            "q": "CVS Pharmacy",
            "has_insurance": "1",
            "limit": "0",
            "start": "0",
        }
    )
    resp = await get_all(request)
    data = json.loads(resp.body)
    assert data["total"] == 5  # count from first call
    assert data["page"] == 1
    assert data["limit"] == 50
    assert data["offset"] == 0
    assert "b.npi" in conn.last_sql and "name_like_0" in conn.last_params
    assert conn.last_params["limit"] > 0  # limit expanded from 0


@pytest.mark.asyncio
async def test_get_all_count_only_supports_response_format_alias(monkeypatch):
    class MappingConnection:
        async def all(self, *_args, **_kwargs):
            return [("taxonomy_a", 3), ("taxonomy_b", 7)]

        async def first(self, *_args, **_kwargs):
            return None

    conn = MappingConnection()
    monkeypatch.setattr(npi_module.db, "acquire", lambda: FakeAcquire(conn))

    request = types.SimpleNamespace(
        args={"count_only": "1", "response_format": "full_taxonomy"}
    )
    resp = await get_all(request)
    data = json.loads(resp.body)
    assert data["rows"]["taxonomy_a"] == 3
    assert data["rows"]["taxonomy_b"] == 7


@pytest.mark.asyncio
async def test_get_all_q_alias_matches_provider_name(monkeypatch):
    conn = RecordingConnection()
    monkeypatch.setattr(npi_module.db, "acquire", lambda: FakeAcquire(conn))

    request = types.SimpleNamespace(
        args={
            "q": "Ryan James Pasiewicz",
            "city": "chicago",
            "limit": "5",
            "start": "0",
        }
    )
    resp = await get_all(request)
    data = json.loads(resp.body)
    assert data["total"] == 5
    assert "name_like_0" in conn.last_params
    assert conn.last_params["name_like_0"] == "%ryan james pasiewicz%"
    assert (
        "filtered_npi AS (" in conn.last_sql
        or "EXISTS (SELECT 1 FROM mrf.npi AS b" in conn.last_sql
    )


@pytest.mark.asyncio
async def test_get_all_accepts_name_like_legacy_alias(monkeypatch):
    conn = RecordingConnection()
    monkeypatch.setattr(npi_module.db, "acquire", lambda: FakeAcquire(conn))

    request = types.SimpleNamespace(args={"name_like": "clinic", "limit": "5", "start": "0"})
    resp = await get_all(request)
    data = json.loads(resp.body)

    assert data["total"] == 5
    assert "name_like_0" in conn.last_params
    assert conn.last_params["name_like_0"] == "%clinic%"


@pytest.mark.asyncio
async def test_get_all_deduplicates_repeated_name_like_values_from_multidict(monkeypatch):
    class FakeArgs:
        def __init__(self):
            self._values = {
                "name_like": ["CVS", "cvs"],
                "limit": ["5"],
                "start": ["0"],
            }

        def get(self, key, default=None):
            values = self._values.get(key)
            if not values:
                return default
            return values[-1]

        def getlist(self, key):
            return list(self._values.get(key, []))

    conn = RecordingConnection()
    monkeypatch.setattr(npi_module.db, "acquire", lambda: FakeAcquire(conn))

    request = types.SimpleNamespace(args=FakeArgs())
    resp = await get_all(request)
    data = json.loads(resp.body)

    assert data["total"] == 5
    assert "name_like_0" in conn.last_params
    assert "name_like_1" not in conn.last_params
    assert conn.last_params["name_like_0"] == "%cvs%"
    assert "name_like_1" not in conn.last_sql


@pytest.mark.asyncio
async def test_get_all_deduplicates_q_and_legacy_name_like(monkeypatch):
    conn = RecordingConnection()
    monkeypatch.setattr(npi_module.db, "acquire", lambda: FakeAcquire(conn))

    request = types.SimpleNamespace(
        args={"q": "clinic", "name_like": "CLINIC", "limit": "5", "start": "0"}
    )
    resp = await get_all(request)
    data = json.loads(resp.body)

    assert data["total"] == 5
    assert "name_like_0" in conn.last_params
    assert "name_like_1" not in conn.last_params
    assert conn.last_params["name_like_0"] == "%clinic%"


@pytest.mark.asyncio
async def test_get_all_without_taxonomy_filters_skips_expensive_overlap(monkeypatch):
    conn = RecordingConnection()
    monkeypatch.setattr(npi_module.db, "acquire", lambda: FakeAcquire(conn))

    request = types.SimpleNamespace(
        args={
            "q": "clinic",
            "city": "chicago",
            "limit": "5",
            "start": "0",
        }
    )
    await get_all(request)

    assert conn.sql_calls
    assert all("taxonomy_array && q.int_codes" not in sql for sql, _ in conn.sql_calls)


@pytest.mark.asyncio
async def test_get_all_zip_phone_and_name_filters_are_applied(monkeypatch):
    conn = RecordingConnection()
    monkeypatch.setattr(npi_module.db, "acquire", lambda: FakeAcquire(conn))

    request = types.SimpleNamespace(
        args={
            "q": "walgreen",
            "zip_code": "60601-1234",
            "phone": "(312) 555-1212",
            "first_name": "john",
            "last_name": "doe",
            "organization_name": "walgreens",
            "entity_type_code": "2",
            "limit": "5",
            "start": "0",
        }
    )
    await get_all(request)

    assert conn.last_params["zip_code"] == "60601"
    assert conn.last_params["phone_digits"] == "3125551212"
    assert conn.last_params["first_name"] == "%john%"
    assert conn.last_params["last_name"] == "%doe%"
    assert conn.last_params["organization_name"] == "%walgreens%"
    assert conn.last_params["entity_type_code"] == 2
    assert "LEFT(c.postal_code, 5) = :zip_code" in conn.last_sql
    assert "regexp_replace(COALESCE(c.telephone_number, ''), '[^0-9]', '', 'g') = :phone_digits" in conn.last_sql


@pytest.mark.asyncio
async def test_get_all_rejects_invalid_entity_type_code(monkeypatch):
    conn = RecordingConnection()
    monkeypatch.setattr(npi_module.db, "acquire", lambda: FakeAcquire(conn))

    request = types.SimpleNamespace(args={"q": "walgreen", "entity_type_code": "7"})
    with pytest.raises(sanic.exceptions.InvalidUsage):
        await get_all(request)


@pytest.mark.asyncio
async def test_get_all_rejects_conflicting_zip_aliases(monkeypatch):
    conn = RecordingConnection()
    monkeypatch.setattr(npi_module.db, "acquire", lambda: FakeAcquire(conn))

    request = types.SimpleNamespace(
        args={
            "q": "walgreen",
            "zip_code": "60601",
            "postal_code": "60602",
        }
    )
    with pytest.raises(sanic.exceptions.InvalidUsage):
        await get_all(request)


@pytest.mark.asyncio
async def test_get_all_applies_procedure_and_medication_all_match_filters(monkeypatch):
    conn = RecordingConnection()
    monkeypatch.setattr(npi_module.db, "acquire", lambda: FakeAcquire(conn))
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
            "procedure_codes": "1001,1002",
            "procedure_code_system": "HP_PROCEDURE_CODE",
            "medication_codes": "2001,2002",
            "medication_code_system": "HP_RX_CODE",
            "year": "2023",
            "limit": "5",
            "start": "0",
        }
    )
    resp = await get_all(request)
    data = json.loads(resp.body)

    assert data["total"] == 5
    assert "c.procedures_array @> ARRAY[:procedure_code_0]::INTEGER[]" in conn.last_sql
    assert "c.procedures_array @> ARRAY[:procedure_code_1]::INTEGER[]" in conn.last_sql
    assert "c.medications_array @> ARRAY[:medication_code_0]::INTEGER[]" in conn.last_sql
    assert "c.medications_array @> ARRAY[:medication_code_1]::INTEGER[]" in conn.last_sql
    assert conn.last_params["procedure_code_0"] == 1001
    assert conn.last_params["procedure_code_1"] == 1002
    assert conn.last_params["medication_code_0"] == 2001
    assert conn.last_params["medication_code_1"] == 2002
    assert conn.last_params["filter_year"] == 2023


@pytest.mark.asyncio
async def test_get_all_rejects_invalid_procedure_code_system(monkeypatch):
    conn = RecordingConnection()
    monkeypatch.setattr(npi_module.db, "acquire", lambda: FakeAcquire(conn))

    request = types.SimpleNamespace(
        args={
            "procedure_codes": "1001",
            "procedure_code_system": "LOINC",
        }
    )
    with pytest.raises(sanic.exceptions.InvalidUsage):
        await get_all(request)


@pytest.mark.asyncio
async def test_get_all_rejects_non_numeric_internal_medication_codes(monkeypatch):
    conn = RecordingConnection()
    monkeypatch.setattr(npi_module.db, "acquire", lambda: FakeAcquire(conn))

    request = types.SimpleNamespace(
        args={
            "medication_codes": "ABC123",
            "medication_code_system": "HP_RX_CODE",
        }
    )
    with pytest.raises(sanic.exceptions.InvalidUsage):
        await get_all(request)


@pytest.mark.asyncio
async def test_get_all_falls_back_to_exists_when_arrays_unavailable(monkeypatch):
    conn = RecordingConnection()
    monkeypatch.setattr(npi_module.db, "acquire", lambda: FakeAcquire(conn))
    monkeypatch.setattr(
        npi_module,
        "_resolve_npi_filter_capabilities",
        lambda: asyncio.sleep(0, result={
            "npi_procedures_array_available": False,
            "npi_medications_array_available": False,
            "pricing_provider_procedure_available": True,
            "pricing_provider_prescription_available": True,
        }),
    )

    request = types.SimpleNamespace(
        args={
            "procedure_codes": "1001",
            "procedure_code_system": "HP_PROCEDURE_CODE",
            "medication_codes": "2001",
            "medication_code_system": "HP_RX_CODE",
            "limit": "5",
            "start": "0",
        }
    )
    await get_all(request)
    assert "EXISTS (SELECT 1 FROM mrf.pricing_provider_procedure" in conn.last_sql
    assert "EXISTS (SELECT 1 FROM mrf.pricing_provider_prescription" in conn.last_sql
    assert "c.procedures_array @> ARRAY[:procedure_code_0]::INTEGER[]" not in conn.last_sql
    assert "c.medications_array @> ARRAY[:medication_code_0]::INTEGER[]" not in conn.last_sql


def test_name_like_clauses_default_like_only(monkeypatch):
    monkeypatch.setattr(npi_module, "ENABLE_TRGM_FUZZY_NAME_SEARCH", False)
    clause, params = npi_module._name_like_clauses("d", ["cvs"])
    assert " % :" not in clause
    assert "name_like_0" in params
    assert "name_like_0_fuzzy" not in params


def test_name_like_clauses_can_enable_trgm_fuzzy(monkeypatch):
    monkeypatch.setattr(npi_module, "ENABLE_TRGM_FUZZY_NAME_SEARCH", True)
    clause, params = npi_module._name_like_clauses("d", ["cvs"])
    assert " % :name_like_0_fuzzy" in clause
    assert params["name_like_0"] == "%cvs%"
    assert params["name_like_0_fuzzy"] == "cvs"


@pytest.mark.asyncio
async def test_get_all_handles_sparse_positional_rows_without_crashing(monkeypatch):
    class SparseResultConnection:
        def __init__(self):
            self.calls = 0

        async def all(self, *_args, **_kwargs):
            self.calls += 1
            if self.calls == 1:
                return [(1,)]

            # Simulate legacy/sparse row shape where address payload has fewer
            # columns than model metadata expects (e.g. missing optional arrays).
            # The handler must not crash with tuple index errors.
            row = [None] * 66
            row[1] = 1234567890  # b.npi (after npi_code)
            row[2] = "00-0000000"  # employer_identification_number
            row[3] = 2  # entity_type_code
            row[42] = 1234567890  # address npi
            row[43] = "primary"  # address type
            row[49] = "PROVIDENCE"  # city_name
            row[50] = "RI"  # state_name
            row[51] = "02903"  # postal_code
            row[62] = "333600000X"  # healthcare_provider_taxonomy_code
            row[63] = "LIC-1"
            row[64] = "RI"
            row[65] = "Y"
            return [tuple(row)]

        async def first(self, *_args, **_kwargs):
            return None

    conn = SparseResultConnection()
    monkeypatch.setattr(npi_module.db, "acquire", lambda: FakeAcquire(conn))

    request = types.SimpleNamespace(
        args={"name_like": "cvs", "classification": "Pharmacy", "state": "RI", "limit": "3"}
    )
    resp = await get_all(request)
    data = json.loads(resp.body)

    assert data["total"] == 1
    assert isinstance(data["rows"], list)
    assert len(data["rows"]) == 1
    assert data["rows"][0]["npi"] == 1234567890
    assert isinstance(data["rows"][0].get("taxonomy_list"), list)
