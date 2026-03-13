import json
import types
from unittest.mock import AsyncMock

import pytest
import sanic.exceptions

pytest.importorskip("pytz")

from api.endpoint import npi as npi_module


class FakeRow:
    def __init__(self, mapping):
        self._mapping = mapping


class FakeResult:
    def __init__(self, *, first_row=None, rows=None):
        self._first = FakeRow(first_row) if first_row is not None else None
        self._rows = [FakeRow(row) for row in (rows or [])]

    def first(self):
        return self._first

    def all(self):
        return self._rows


class FakeSession:
    def __init__(self, results):
        self._results = list(results)
        self.calls = []

    async def execute(self, statement, params=None):
        self.calls.append((str(statement), dict(params or {})))
        if not self._results:
            return FakeResult(rows=[])
        return self._results.pop(0)


class FakeSessionContext:
    def __init__(self, session):
        self._session = session

    async def __aenter__(self):
        return self._session

    async def __aexit__(self, exc_type, exc, tb):
        return False


@pytest.mark.asyncio
async def test_get_facility_connected_providers_returns_providers_and_specialty_stats(monkeypatch):
    fake_session = FakeSession(
        [
            FakeResult(first_row={"total_providers": 2}),
            FakeResult(
                rows=[
                    {
                        "facility_ccn": "123456",
                        "organization_name": "General Hospital",
                        "doing_business_as_name": "GH",
                        "city": "MIAMI",
                        "state": "FL",
                        "provider_count": 2,
                    }
                ]
            ),
            FakeResult(
                rows=[
                    {
                        "npi": 1111111111,
                        "reporting_year": 2025,
                        "facility_ccn": "123456",
                        "organization_name": "General Hospital",
                        "doing_business_as_name": "GH",
                        "facility_city": "MIAMI",
                        "facility_state": "FL",
                        "facility_zip_code": "33101",
                        "enrollment_provider_type_code": "12",
                        "enrollment_provider_type_text": "Physician",
                        "practice_location_type": "Hospital",
                        "entity_type_code": "1",
                        "provider_first_name": "Anna",
                        "provider_last_name": "Smith",
                        "provider_organization_name": None,
                        "provider_city": "MIAMI",
                        "provider_state": "FL",
                        "taxonomy_code": "207R00000X",
                        "specialty_display_name": "Internal Medicine",
                        "specialty_classification": "Allopathic & Osteopathic Physicians",
                        "specialty_section": "Physicians",
                    },
                    {
                        "npi": 2222222222,
                        "reporting_year": 2025,
                        "facility_ccn": "123456",
                        "organization_name": "General Hospital",
                        "doing_business_as_name": "GH",
                        "facility_city": "MIAMI",
                        "facility_state": "FL",
                        "facility_zip_code": "33101",
                        "enrollment_provider_type_code": "22",
                        "enrollment_provider_type_text": "Group",
                        "practice_location_type": "Hospital",
                        "entity_type_code": "2",
                        "provider_first_name": None,
                        "provider_last_name": None,
                        "provider_organization_name": "Ocean Group Practice",
                        "provider_city": "MIAMI",
                        "provider_state": "FL",
                        "taxonomy_code": None,
                        "specialty_display_name": None,
                        "specialty_classification": None,
                        "specialty_section": None,
                    },
                ]
            ),
            FakeResult(
                rows=[
                    {
                        "specialty": "Internal Medicine",
                        "classification": "Allopathic & Osteopathic Physicians",
                        "provider_count": 1,
                    },
                    {
                        "specialty": "Unknown",
                        "classification": "Unknown",
                        "provider_count": 1,
                    },
                ]
            ),
        ]
    )

    monkeypatch.setattr(npi_module, "_table_exists", AsyncMock(return_value=True))
    monkeypatch.setattr(npi_module.db, "session", lambda: FakeSessionContext(fake_session))

    request = types.SimpleNamespace(
        args={"ccn": "123456", "limit": "25", "offset": "0"},
        ctx=types.SimpleNamespace(sa_session=None),
    )
    response = await npi_module.get_facility_connected_providers(request)
    payload = json.loads(response.body)

    assert payload["query"]["facility_type"] == "hospital"
    assert payload["total_providers"] == 2
    assert payload["matched_facilities"][0]["organization_name"] == "General Hospital"
    assert payload["providers"][0]["provider_name"] == "Anna Smith"
    assert payload["providers"][1]["provider_name"] == "Ocean Group Practice"
    assert payload["providers"][1]["specialty"] == "Unknown"
    assert payload["specialty_stats"][0]["specialty"] == "Internal Medicine"
    assert len(fake_session.calls) == 4


@pytest.mark.asyncio
async def test_get_facility_connected_providers_requires_facility_locator():
    request = types.SimpleNamespace(args={})
    with pytest.raises(sanic.exceptions.InvalidUsage):
        await npi_module.get_facility_connected_providers(request)


@pytest.mark.asyncio
async def test_get_facility_connected_providers_rejects_unknown_facility_type():
    request = types.SimpleNamespace(args={"ccn": "123456", "facility_type": "clinic"})
    with pytest.raises(sanic.exceptions.InvalidUsage):
        await npi_module.get_facility_connected_providers(request)
