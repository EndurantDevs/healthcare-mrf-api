# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import datetime
import json
import types
from unittest.mock import AsyncMock

import pytest
from sanic.exceptions import InvalidUsage, NotFound

from api.endpoint import partd_formulary


class FakeMappingResult:
    def __init__(self, rows=None, scalar=None):
        self._rows = rows or []
        self._scalar = scalar

    def mappings(self):
        return self

    def all(self):
        return self._rows

    def first(self):
        return self._rows[0] if self._rows else None

    def scalar(self):
        return self._scalar


class FakeSession:
    def __init__(self, results):
        self._results = list(results)
        self.calls = []

    async def execute(self, statement, parameters=None):
        self.calls.append((statement, dict(parameters or {})))
        if self._results:
            return self._results.pop(0)
        return FakeMappingResult([])


def make_request(results, args=None):
    session = FakeSession(results)
    ctx = types.SimpleNamespace(sa_session=session)
    return types.SimpleNamespace(args=args or {}, ctx=ctx)


@pytest.mark.asyncio
async def test_activity_empty_response_includes_state_license_summary(monkeypatch):
    monkeypatch.setattr(
        partd_formulary,
        "_fetch_state_license_summary",
        AsyncMock(
            return_value={
                "has_active_state_license": True,
                "active_license_count": 1,
                "active_states": ["TX"],
                "disciplinary_flag_any": False,
                "license_checked_at": "2026-03-10T00:00:00",
            }
        ),
    )

    request = make_request([FakeMappingResult([])], args={"as_of": "2026-03-10"})
    response = await partd_formulary.get_pharmacy_partd_activity(request, "1518379601")
    payload = json.loads(response.body)

    assert payload["items"] == []
    assert payload["state_license_summary"]["has_active_state_license"] is True
    assert payload["state_license_summary"]["active_states"] == ["TX"]


@pytest.mark.asyncio
async def test_activity_response_includes_dispensing_fees(monkeypatch):
    monkeypatch.setattr(
        partd_formulary,
        "_fetch_state_license_summary",
        AsyncMock(
            return_value={
                "has_active_state_license": False,
                "active_license_count": 0,
                "active_states": [],
                "disciplinary_flag_any": False,
                "license_checked_at": "2026-03-10T00:00:00",
            }
        ),
    )

    request = make_request(
        [
            FakeMappingResult(
                [
                    {
                        "snapshot_id": "quarterly:20260101:abc",
                        "plan_id": "S1234-001-000",
                        "contract_id": "S1234",
                        "segment_id": "000",
                        "year": 2026,
                        "medicare_active": True,
                        "pharmacy_name": "Test Pharmacy",
                        "pharmacy_type": "retail",
                        "mail_order": False,
                        "dispensing_fee_brand_30": 1.10,
                        "dispensing_fee_brand_60": 1.20,
                        "dispensing_fee_brand_90": 1.30,
                        "dispensing_fee_generic_30": 0.10,
                        "dispensing_fee_generic_60": 0.20,
                        "dispensing_fee_generic_90": 0.30,
                        "dispensing_fee_selected_drug_30": 2.10,
                        "dispensing_fee_selected_drug_60": 2.20,
                        "dispensing_fee_selected_drug_90": 2.30,
                        "effective_from": datetime.date(2026, 1, 1),
                        "effective_to": None,
                        "source_type": "quarterly",
                    }
                ]
            )
        ],
        args={"as_of": "2026-03-10"},
    )
    response = await partd_formulary.get_pharmacy_partd_activity(request, "1518379601")
    activity_payload = json.loads(response.body)

    assert activity_payload["medicare_active"] is True
    assert len(activity_payload["items"]) == 1
    activity_item = activity_payload["items"][0]
    assert activity_item["dispensing_fee_brand_30"] == 1.1
    assert activity_item["dispensing_fee_generic_90"] == 0.3
    assert activity_item["dispensing_fee_selected_drug_60"] == 2.2


@pytest.mark.asyncio
async def test_medication_costs_requested_plans_bind_ndc_date_and_page_filters():
    request = make_request(
        [FakeMappingResult(scalar=3), FakeMappingResult()],
        args={
            "plan_ids": "S5678-001-000, S1234-001-000,S1234-001-000, ",
            "year": "2026",
            "month": "2026-05-23",
            "limit": "2",
            "offset": "2",
        },
    )

    cost_payload = json.loads(
        (await partd_formulary.get_pharmacy_medication_costs(request, "1234567890", " NDC ", "12345-6789-01")).body
    )

    assert cost_payload["query"] == {
        "input_code_system": " NDC ",
        "input_code": "12345-6789-01",
        "normalized_code_system": "NDC",
        "normalized_code": "12345678901",
        "match_strategy": "plan_level_ndc",
        "year": 2026,
        "month": "2026-05-01",
    }
    assert cost_payload["plans"] == {
        "requested_plan_ids": ["S1234-001-000", "S5678-001-000"],
        "effective_plan_ids": ["S1234-001-000", "S5678-001-000"],
        "selection_mode": "requested",
        "pricing_level": "plan",
    }
    assert (cost_payload["total"], cost_payload["page"], cost_payload["limit"], cost_payload["offset"]) == (3, 2, 2, 2)
    expected_parameter_map = {
        "normalized_code": "12345678901",
        "plan_id_0": "S1234-001-000",
        "plan_id_1": "S5678-001-000",
        "year": 2026,
        "month_date": datetime.date(2026, 5, 1),
    }
    assert request.ctx.sa_session.calls[0][1] == expected_parameter_map
    assert request.ctx.sa_session.calls[1][1] == dict(expected_parameter_map, offset=2, limit=2)
    for statement, _ in request.ctx.sa_session.calls:
        sql = str(statement)
        assert "c.ndc11 = :normalized_code" in sql
        assert "c.plan_ids && ARRAY[:plan_id_0, :plan_id_1]::varchar[]" in sql
        assert "p.plan_id IN (:plan_id_0, :plan_id_1)" in sql
        assert "c.year = :year" in sql
        assert "c.effective_from <= :month_date" in sql and "c.effective_to >= :month_date" in sql
        assert "S1234-001-000" not in sql
    assert "ORDER BY p.plan_id ASC, c.cost_type ASC, c.effective_from DESC NULLS LAST" in str(
        request.ctx.sa_session.calls[1][0]
    )


@pytest.mark.asyncio
async def test_medication_costs_serializes_dates_and_nullable_fields():
    cost_field_map = {
        "snapshot_id": "monthly:20260501:sample",
        "plan_id": "S1234-001-000",
        "plan_ids": ["S1234-001-000"],
        "contract_id": "S1234",
        "segment_id": "000",
        "year": 2026,
        "code_system": "NDC",
        "code": "12345678901",
        "normalized_code": "12345678901",
        "rxnorm_id": "12345",
        "ndc11": "12345678901",
        "days_supply": 30,
        "drug_name": "Example Drug",
        "tier": "1",
        "pharmacy_type": "retail",
        "mail_order": False,
        "cost_type": "copay",
        "cost_amount": 4.25,
        "effective_from": datetime.date(2026, 5, 1),
        "effective_to": datetime.date(2026, 5, 31),
        "source_type": "monthly",
    }
    request = make_request(
        [
            FakeMappingResult(scalar=2),
            FakeMappingResult(
                [
                    cost_field_map,
                    dict(cost_field_map, effective_to=None, drug_name=None, cost_amount=None),
                ]
            ),
        ],
        args={"plan_ids": "S1234-001-000"},
    )

    cost_payload = json.loads(
        (await partd_formulary.get_pharmacy_medication_costs(request, "1234567890", "ndc", "12345678901")).body
    )

    assert cost_payload["items"][0] == dict(cost_field_map, effective_from="2026-05-01", effective_to="2026-05-31")
    assert cost_payload["items"][1]["plan_ids"] == ["S1234-001-000"]
    assert cost_payload["items"][1]["effective_from"] == "2026-05-01"
    assert cost_payload["items"][1]["effective_to"] is None
    assert cost_payload["items"][1]["drug_name"] is cost_payload["items"][1]["cost_amount"] is None


@pytest.mark.asyncio
async def test_medication_costs_rxnorm_uses_active_plans_and_bidirectional_crosswalk():
    request = make_request(
        [
            FakeMappingResult(
                [
                    {"plan_id": "S5678-001-000"},
                    {"plan_id": None},
                    {"plan_id": "S1234-001-000"},
                ]
            ),
            FakeMappingResult([("12345-6789-01",), ("letters",)]),
            FakeMappingResult([("12345678901",), ("98765-4321-09",)]),
            FakeMappingResult(scalar=0),
            FakeMappingResult(),
        ],
        args={"month": "2026-05-23"},
    )

    cost_payload = json.loads(
        (await partd_formulary.get_pharmacy_medication_costs(request, "1234567890", "rxcui", " 12-345 ")).body
    )

    assert cost_payload["query"]["normalized_code_system"] == "RXNORM"
    assert cost_payload["query"]["normalized_code"] == "12345"
    assert cost_payload["query"]["match_strategy"] == "plan_level_rxnorm"
    assert cost_payload["plans"]["selection_mode"] == "pharmacy_active_medicare_plans"
    assert cost_payload["plans"]["requested_plan_ids"] == []
    assert cost_payload["plans"]["effective_plan_ids"] == ["S1234-001-000", "S5678-001-000"]
    assert cost_payload["total"] == 0 and cost_payload["items"] == []
    calls = request.ctx.sa_session.calls
    assert calls[0][1] == {"npi": 1234567890, "as_of": datetime.date(2026, 5, 1)}
    assert "a.medicare_active IS TRUE" in str(calls[0][0])
    assert "a.effective_from <= :as_of" in str(calls[0][0])
    assert "a.effective_to >= :as_of" in str(calls[0][0])
    assert calls[1][0].compile().params["from_code_1"] == "12345"
    assert calls[2][0].compile().params["to_code_1"] == "12345"
    for statement, parameters in calls[3:]:
        assert parameters["crosswalk_ndc_0"] == "12345678901"
        assert parameters["crosswalk_ndc_1"] == "98765432109"
        assert "crosswalk_ndc_2" not in parameters
        assert "c.ndc11 IN (:crosswalk_ndc_0, :crosswalk_ndc_1)" in str(statement)
        assert "c.rxnorm_id = :normalized_code" in str(statement)
        assert "c.code_system = 'RXNORM' AND c.code = :normalized_code" in str(statement)


@pytest.mark.asyncio
async def test_medication_costs_rxnorm_without_crosswalk_keeps_direct_matches():
    request = make_request(
        [FakeMappingResult(), FakeMappingResult(), FakeMappingResult(scalar=0), FakeMappingResult()],
        args={"plan_ids": "S1234-001-000"},
    )

    payload = json.loads(
        (await partd_formulary.get_pharmacy_medication_costs(request, "1234567890", "rxnorm", "12345")).body
    )

    assert payload["query"]["year"] is payload["query"]["month"] is None
    assert payload["plans"]["selection_mode"] == "requested"
    assert payload["total"] == 0 and payload["items"] == []
    assert len(request.ctx.sa_session.calls) == 4
    for statement, parameters in request.ctx.sa_session.calls[2:]:
        assert "c.rxnorm_id = :normalized_code" in str(statement)
        assert "c.ndc11 IN" not in str(statement)
        assert "year" not in parameters and "month_date" not in parameters


@pytest.mark.asyncio
async def test_medication_costs_without_active_plans_returns_empty_before_cost_queries():
    request = make_request([FakeMappingResult()], args={"plan_ids": "null", "limit": "5", "offset": "10"})

    payload = json.loads(
        (await partd_formulary.get_pharmacy_medication_costs(request, "1234567890", "ndc", "12345-6789-01")).body
    )

    assert payload["query"]["match_strategy"] == "no_active_plans_for_pharmacy"
    assert payload["plans"] == {
        "requested_plan_ids": [],
        "effective_plan_ids": [],
        "selection_mode": "pharmacy_active_medicare_plans",
    }
    assert (payload["total"], payload["page"], payload["limit"], payload["offset"]) == (0, 3, 5, 10)
    assert payload["items"] == []
    assert len(request.ctx.sa_session.calls) == 1
    assert request.ctx.sa_session.calls[0][1]["npi"] == 1234567890
    assert isinstance(request.ctx.sa_session.calls[0][1]["as_of"], datetime.date)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("npi", "code_system", "code", "args", "message"),
    [
        ("bad", "ndc", "1", {}, "NPI must be numeric"),
        ("0", "ndc", "1", {}, "NPI must be positive"),
        ("1234567890", "other", "1", {}, "code_system must be one of"),
        ("1234567890", "ndc", "letters", {}, "at least one digit"),
        ("1234567890", "ndc", "1", {"year": "bad"}, "'year' must be an integer"),
        ("1234567890", "ndc", "1", {"month": "2026-02-30"}, "valid ISO date"),
    ],
)
async def test_medication_costs_rejects_invalid_query_before_database(npi, code_system, code, args, message):
    request = make_request([], args=args)

    with pytest.raises(InvalidUsage, match=message):
        await partd_formulary.get_pharmacy_medication_costs(request, npi, code_system, code)

    assert request.ctx.sa_session.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("run_id", [None, "run-sample"])
async def test_import_status_selects_run_and_serializes_snapshot_totals(run_id):
    run_timestamp = datetime.datetime(2026, 5, 1, 12, 30) if run_id else None
    run_row = types.SimpleNamespace(
        _mapping={
            "run_id": "run-sample",
            "import_id": "import-sample",
            "status": "complete",
            "started_at": run_timestamp,
            "finished_at": run_timestamp,
            "source_summary": {"files": 2} if run_id else None,
            "error_text": None,
        }
    )
    snapshot_rows = [
        types.SimpleNamespace(
            _mapping={
                "snapshot_count": 2 if run_id else 0,
                "activity_rows": 3 if run_id else 0,
                "pricing_rows": 4 if run_id else 0,
            }
        )
    ]
    request = make_request(
        [FakeMappingResult([run_row]), FakeMappingResult(snapshot_rows)],
        args={"run_id": run_id} if run_id else {},
    )

    status_payload = json.loads((await partd_formulary.get_partd_import_status(request)).body)

    assert status_payload == {
        "run_id": "run-sample",
        "import_id": "import-sample",
        "status": "complete",
        "error_text": None,
        "started_at": run_timestamp.isoformat() if run_timestamp else None,
        "finished_at": run_timestamp.isoformat() if run_timestamp else None,
        "source_summary": {"files": 2} if run_id else {},
        "snapshots": {"count": 2, "activity_rows": 3, "pricing_rows": 4}
        if run_id
        else {"count": 0, "activity_rows": 0, "pricing_rows": 0},
    }
    run_query = request.ctx.sa_session.calls[0][0]
    if run_id:
        assert run_query.compile().params == {"run_id_1": "run-sample"}
    else:
        assert "started_at DESC NULLS LAST" in str(run_query)
        assert run_query.compile().params == {"param_1": 1}
    assert request.ctx.sa_session.calls[1][0].compile().params["run_id_1"] == "run-sample"


@pytest.mark.asyncio
async def test_import_status_without_runs_returns_not_found():
    request = make_request([FakeMappingResult()])

    with pytest.raises(NotFound, match="No Part D imports"):
        await partd_formulary.get_partd_import_status(request)

    assert len(request.ctx.sa_session.calls) == 1


@pytest.mark.asyncio
async def test_snapshot_list_without_filters_keeps_count_and_page_unfiltered():
    request = make_request([FakeMappingResult(scalar=0), FakeMappingResult()])

    snapshot_payload = json.loads((await partd_formulary.list_partd_snapshots(request)).body)

    assert snapshot_payload == {"page": 1, "page_size": 25, "limit": 25, "offset": 0, "total": 0, "items": []}
    assert all("WHERE" not in str(statement) for statement, _ in request.ctx.sa_session.calls)
    assert "release_date DESC NULLS LAST" in str(request.ctx.sa_session.calls[1][0])


@pytest.mark.asyncio
async def test_inactive_pharmacy_activity_preserves_end_dates_and_plan_ids():
    request = make_request(
        [
            FakeMappingResult(scalar=None),
            FakeMappingResult(
                [
                    {
                        "snapshot_id": "quarterly:20260501:sample",
                        "plan_id": "S1234-001-000",
                        "year": 2026,
                        "medicare_active": False,
                        "source_type": "quarterly",
                        "effective_from": datetime.date(2026, 5, 1),
                        "effective_to": datetime.date(2026, 5, 31),
                    },
                    {
                        "snapshot_id": "quarterly:20260501:sample",
                        "plan_id": "S5678-001-000",
                        "year": 2026,
                        "medicare_active": False,
                        "source_type": "quarterly",
                        "effective_from": datetime.date(2026, 5, 1),
                        "effective_to": datetime.date(2026, 5, 31),
                    },
                ]
            ),
        ],
        args={"as_of": "2026-05-01"},
    )

    activity_payload = json.loads((await partd_formulary.get_pharmacy_partd_activity(request, "1234567890")).body)

    assert activity_payload["medicare_active"] is False
    assert activity_payload["medicare_active_as_of"] is None
    assert activity_payload["source_type"] == "quarterly"
    assert activity_payload["plan_ids"] == ["S1234-001-000", "S5678-001-000"]
    assert activity_payload["items"][0]["effective_from"] == "2026-05-01"
    assert activity_payload["items"][0]["effective_to"] == "2026-05-31"
