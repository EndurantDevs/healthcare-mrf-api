from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.exc import DBAPIError

from api import provider_profile as profile_api
from api import provider_profile_cms_npd as cms_npd

NPI = 1000000004
RELEASE_ID = "e" * 64
PUBLISHED = datetime(2026, 11, 1, tzinfo=timezone.utc)
RELEASE_ROW = {"release_id": RELEASE_ID, "schema_name": "cms_npd_" + "e" * 16, "published_at": PUBLISHED}


def _role(role_id="r1", **fields):
    return {
        "role_id": role_id,
        "new_patients": "newpt",
        "specialty_codes": ["207R00000X"],
        "organization_name": "Example Clinic Group",
        "organization_npi": 1234567893,
        "locations": [
            {
                "name": "Example Clinic",
                "phone": "555-0100",
                "line1": "1 Example Way",
                "city": "Exampletown",
                "state": "EX",
                "postal_code": "00001",
                "line2": None,
            }
        ],
        "endpoints": [
            {"name": "Example FHIR", "connection_type": "hl7-fhir-rest", "address": "https://example.test/fhir"}
        ],
        "plans": [{"name": "Example Plan", "type": None}],
        "networks": ["Example Network"],
    } | fields


class _Result:
    def __init__(self, rows):
        self.rows = rows

    def mappings(self):
        return self

    def all(self):
        return self.rows


class _Session:
    def __init__(self, *responses):
        self.responses = list(responses)
        self.statements = []

    async def execute(self, statement, params=None):
        self.statements.append((str(statement), params))
        response = self.responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        return _Result(response)


def _use_session(monkeypatch, session):
    @asynccontextmanager
    async def transaction():
        yield session

    monkeypatch.setattr(cms_npd, "db", SimpleNamespace(transaction=transaction))


def _pg_error(sqlstate):
    orig = type("PgError", (Exception,), {"sqlstate": sqlstate})()
    return DBAPIError("SELECT 1", {}, orig)


def _release():
    return {"release_id": RELEASE_ID, "observed_at": PUBLISHED}


def _items(projection, category):
    return projection["categories"][category]["items"]


def test_role_fact_mapping():
    projection = cms_npd.cms_npd_projection(NPI, _release(), [_role()])
    assert projection["generation_id"] == RELEASE_ID
    assert projection["source"]["source_key"] == "cms-npd"
    assert projection["source"]["published_at"] == PUBLISHED.isoformat()
    (organization,) = _items(projection, "organizations")
    assert organization["value"] == {"name": "Example Clinic Group", "npi": "1234567893"}
    assert _items(projection, "specialties")[0]["value"] == {"code": "207R00000X"}
    (acceptance,) = _items(projection, "accepting_patients")
    assert acceptance["display"] == "Accepting new patients — Example Clinic Group"
    contact, endpoint = _items(projection, "contact")
    assert contact["display"] == "Phone: 555-0100 (work)"
    assert contact["value"]["address"] == {
        "line1": "1 Example Way",
        "city": "Exampletown",
        "state": "EX",
        "postal_code": "00001",
    }
    assert "Example Way" not in contact["display"]
    assert endpoint["display"] == "Example FHIR — https://example.test/fhir"
    plan, network = _items(projection, "network_participation")
    assert (plan["display"], network["display"]) == ("Listed plan: Example Plan", "Listed network: Example Network")
    (record,) = projection["evidence"]["records"]
    assert record["source_record_id"] == organization["source_record_id"] == f"cms-npd:{RELEASE_ID}:PractitionerRole/r1"


def test_roles_share_facts():
    second = _role(
        "r2",
        locations='[{"name": "Example Annex"}]',
        endpoints=None,
        plans=None,
        networks=None,
        new_patients=None,
        organization_npi=None,
        specialty_codes=[None],
    )
    projection = cms_npd.cms_npd_projection(NPI, _release(), [_role(organization_npi=None), second])
    (organization,) = _items(projection, "organizations")
    assert organization["source_record_ids"] == [
        f"cms-npd:{RELEASE_ID}:PractitionerRole/{role}" for role in ("r1", "r2")
    ]
    assert organization["assertion_count"] == 2
    assert {item["display"] for item in _items(projection, "contact")} >= {"Example Annex"}
    assert len(projection["evidence"]["records"]) == 2


def test_no_roles():
    assert cms_npd.cms_npd_projection(NPI, _release(), []) is None


@pytest.mark.asyncio
async def test_bounded_read(monkeypatch):
    session = _Session([RELEASE_ROW], [_role()])
    _use_session(monkeypatch, session)
    projection = await cms_npd.fetch_cms_npd_projection(NPI)
    assert projection["generation_id"] == RELEASE_ID
    assert len(session.statements) == 2
    sql, params = session.statements[1]
    assert params == {"npi": NPI, "role_limit": cms_npd.ROLE_LIMIT}
    assert f'FROM "cms_npd_{"e" * 16}".practitioner_role r' in sql
    assert "r.active IS NOT FALSE" in sql and "r.period_end >= CURRENT_DATE" in sql
    assert f"r.location_ids[1:{cms_npd.LIST_LIMIT}]" in sql


@pytest.mark.asyncio
@pytest.mark.parametrize("release_rows", [[], [RELEASE_ROW | {"schema_name": "public"}]])
async def test_no_release(monkeypatch, release_rows):
    session = _Session(release_rows)
    _use_session(monkeypatch, session)
    assert await cms_npd.fetch_cms_npd_projection(NPI) is None
    assert len(session.statements) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("responses", "count"),
    [((_pg_error("42P01"),), 1), (([RELEASE_ROW], _pg_error("3F000")), 2), (([RELEASE_ROW], _pg_error("42501")), 2)],
)
async def test_missing_schema(monkeypatch, responses, count):
    session = _Session(*responses)
    _use_session(monkeypatch, session)
    assert await cms_npd.fetch_cms_npd_projection(NPI) is None
    assert len(session.statements) == count


@pytest.mark.asyncio
async def test_other_errors_raise(monkeypatch):
    _use_session(monkeypatch, _Session([RELEASE_ROW], _pg_error("57014")))
    with pytest.raises(DBAPIError):
        await cms_npd.fetch_cms_npd_projection(NPI)


def _stub_loaders(monkeypatch):
    loader = AsyncMock(return_value=cms_npd.cms_npd_projection(NPI, _release(), [_role()]))
    monkeypatch.setattr(profile_api, "fetch_state_profile_projection", AsyncMock(return_value=None))
    monkeypatch.setattr(profile_api, "fetch_cms_education_projection", AsyncMock(return_value=None))
    monkeypatch.setattr(profile_api, "fetch_additional_state_profile_projections", AsyncMock(return_value=[]))
    monkeypatch.setattr(profile_api, "fetch_cms_npd_projection", loader)
    return loader


@pytest.mark.asyncio
async def test_public_path(monkeypatch):
    loader = _stub_loaders(monkeypatch)
    projection = await profile_api.fetch_provider_profile_projection(NPI)
    loader.assert_awaited_once_with(NPI)
    profile = profile_api.compose_provider_profile(NPI, state_projection=projection, fhir_profile=None)
    assert profile["source_generations"] == {"cms-npd": RELEASE_ID}
    assert profile["categories"]["organizations"]["items"][0]["display"] == "Example Clinic Group"
    assert any("National Provider Directory" in context for context in profile["important_context"])
    evidence = profile_api.compose_provider_profile_evidence(
        state_projection=projection, fhir_evidence=None, provider_profile=profile
    )
    assert evidence["sources"]["cms-npd"]["records"][0]["resource_type"] == "PractitionerRole"


@pytest.mark.asyncio
async def test_managed_opt_out(monkeypatch):
    loader = _stub_loaders(monkeypatch)
    assert await profile_api.fetch_provider_profile_projection(NPI, include_cms_directory=False) is None
    loader.assert_not_awaited()


@pytest.mark.asyncio
async def test_postal_filter(monkeypatch):
    _stub_loaders(monkeypatch)
    projection = await profile_api.fetch_provider_profile_projection(NPI)
    profile = profile_api.compose_source_filtered_profile(NPI, 2, state_projection=projection, fhir_profile=None)
    contact = next(item for item in profile["categories"]["contact"]["items"] if item["type"] == "contact")
    assert "address" not in contact["value"] and contact["value"]["value"] == "555-0100"
