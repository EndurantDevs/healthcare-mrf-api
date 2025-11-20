# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import json
import types

import pytest
from sanic.exceptions import NotFound

from api.endpoint import formulary


class FakeResult:
    def __init__(self, rows=None, scalar=None):
        self._rows = rows or []
        self._scalar = scalar

    def all(self):
        return self._rows

    def first(self):
        return self._rows[0] if self._rows else None

    def scalar(self):
        return self._scalar


class FakeSession:
    def __init__(self, results):
        self._results = list(results)

    async def execute(self, *_args, **_kwargs):
        if self._results:
            return self._results.pop(0)
        return FakeResult()


class MappingRow:
    def __init__(self, **mapping):
        self._mapping = mapping

    def __getattr__(self, item):
        try:
            return self._mapping[item]
        except KeyError as exc:
            raise AttributeError(item) from exc


def make_request(results, args=None):
    session = FakeSession(results)
    ctx = types.SimpleNamespace(sa_session=session)
    return types.SimpleNamespace(args=args or {}, ctx=ctx)


@pytest.mark.asyncio
async def test_list_formularies_returns_rows():
    request = make_request(
        [
            FakeResult(scalar=2),
            FakeResult(
                rows=[
                    MappingRow(
                        plan_id="PLAN123",
                        year=2025,
                        marketing_name="Sample Plan",
                        state="CA",
                        issuer_id=99,
                        issuer_name="Issuer One",
                        issuer_marketing_name="Issuer Marketing",
                        drug_count=12,
                        last_updated=None,
                    )
                ]
            ),
        ],
        args={"page": "1", "page_size": "10"},
    )

    response = await formulary.list_formularies(request)
    data = json.loads(response.body)

    assert data["total"] == 2
    assert data["items"][0]["formulary_id"] == "PLAN123:2025"
    assert data["items"][0]["issuer"]["issuer_name"] == "Issuer One"


@pytest.mark.asyncio
async def test_get_formulary_returns_detail():
    request = make_request(
        [
            FakeResult(
                rows=[
                    MappingRow(
                        plan_id="PLAN123",
                        year=2025,
                        marketing_name="Sample Plan",
                        state="CA",
                        summary_url="http://example.com/summary",
                        marketing_url="http://example.com/marketing",
                        issuer_id=99,
                        issuer_name="Issuer One",
                        issuer_marketing_name="Issuer Marketing",
                    )
                ]
            ),
            FakeResult(rows=[{"drug_count": 42, "last_updated": None}]),
            FakeResult(rows=[("PREFERRED",), ("STANDARD",)]),
            FakeResult(rows=[("MAIL_ORDER",), ("RETAIL",)]),
        ]
    )

    response = await formulary.get_formulary(request, "PLAN123:2025")
    data = json.loads(response.body)

    assert data["plan"]["plan_id"] == "PLAN123"
    assert data["drug_count"] == 42
    assert data["available_tiers"] == ["PREFERRED", "STANDARD"]
    assert data["available_pharmacy_types"] == ["MAIL_ORDER", "RETAIL"]


@pytest.mark.asyncio
async def test_list_formulary_drugs_respects_filters():
    request = make_request(
        [
            FakeResult(scalar=1),  # exists check
            FakeResult(scalar=3),  # total count
            FakeResult(
                rows=[
                    MappingRow(
                        rxnorm_id="12345",
                        drug_name="Example Drug",
                        drug_tier="PREFERRED",
                        prior_authorization=True,
                        step_therapy=False,
                        quantity_limit=True,
                        last_updated_on=None,
                    )
                ]
            ),
            FakeResult(rows=[("MAIL_ORDER",), ("RETAIL",)]),
        ],
        args={"tier": "PREFERRED", "page": "1", "page_size": "5"},
    )

    response = await formulary.list_formulary_drugs(request, "PLAN123:2025")
    payload = json.loads(response.body)

    assert payload["total"] == 3
    assert payload["items"][0]["rxnorm_id"] == "12345"
    assert payload["available_pharmacy_types"] == ["MAIL_ORDER", "RETAIL"]


@pytest.mark.asyncio
async def test_get_formulary_drug_not_found():
    request = make_request(
        [
            FakeResult(scalar=1),  # exists check
            FakeResult(rows=[]),
        ]
    )

    with pytest.raises(NotFound):
        await formulary.get_formulary_drug(request, "PLAN123:2025", "missing")


@pytest.mark.asyncio
async def test_get_formulary_summary_returns_aggregates():
    request = make_request(
        [
            FakeResult(scalar=1),  # exists check
            FakeResult(scalar=10),  # total drugs
            FakeResult(rows=[("PREFERRED", 7), ("STANDARD", 3)]),
            FakeResult(rows=[(True, 4), (False, 6)]),
            FakeResult(rows=[(True, 2), (False, 8)]),
            FakeResult(rows=[(True, 5), (False, 5)]),
            FakeResult(rows=[("MAIL_ORDER", 2), ("RETAIL", 8)]),
        ]
    )

    response = await formulary.get_formulary_summary(request, "PLAN123:2025")
    data = json.loads(response.body)

    assert data["total_drugs"] == 10
    assert data["tiers"]["PREFERRED"] == 7
    assert data["authorization_requirements"]["required"] == 4
    assert data["pharmacy_types"]["RETAIL"] == 8


@pytest.mark.asyncio
async def test_cross_formulary_drug_returns_formularies():
    request = make_request(
        [
            FakeResult(
                rows=[
                    MappingRow(
                        plan_id="PLAN123",
                        year=2025,
                        marketing_name="Plan A",
                        state="CA",
                        issuer_id=1,
                        issuer_name="Issuer",
                        drug_tier="PREFERRED",
                        prior_authorization=True,
                        step_therapy=False,
                        quantity_limit=False,
                    )
                ]
            )
        ]
    )

    response = await formulary.cross_formulary_drug(request, "12345")
    data = json.loads(response.body)

    assert data["rxnorm_id"] == "12345"
    assert data["formularies"][0]["formulary_id"] == "PLAN123:2025"


@pytest.mark.asyncio
async def test_formulary_statistics_returns_summary():
    request = make_request(
        [
            FakeResult(rows=[(1, "Issuer One", 100)]),
            FakeResult(rows=[("PREFERRED", 60), ("STANDARD", 40)]),
            FakeResult(rows=[(True, 30), (False, 70)]),
            FakeResult(scalar=120),
            FakeResult(scalar=5),
        ]
    )

    response = await formulary.formulary_statistics(request)
    data = json.loads(response.body)

    assert data["total_formularies"] == 5
    assert data["total_drugs"] == 120
    assert data["top_issuers"][0]["drug_count"] == 100


@pytest.mark.asyncio
async def test_check_plan_drug_without_year_uses_latest():
    request = make_request(
        [
            FakeResult(scalar=2026),  # latest year
            FakeResult(scalar=1),  # plan exists
            FakeResult(
                rows=[
                    MappingRow(
                        plan_id="PLAN123",
                        rxnorm_id="12345",
                        drug_name="Example Drug",
                        drug_tier="PREFERRED",
                        prior_authorization=True,
                        step_therapy=False,
                        quantity_limit=True,
                        last_updated_on=None,
                    )
                ]
            ),
        ]
    )

    response = await formulary.check_plan_drug(request, "PLAN123", "12345")
    data = json.loads(response.body)

    assert data["covered"] is True
    assert data["year"] == 2026
    assert data["details"]["drug_tier"] == "PREFERRED"


@pytest.mark.asyncio
async def test_check_plan_drug_not_covered():
    request = make_request(
        [
            FakeResult(scalar=2025),  # latest year
        ]
    )

    with pytest.raises(NotFound):
        await formulary.check_plan_drug(request, "UNKNOWN_PLAN", "12345")
