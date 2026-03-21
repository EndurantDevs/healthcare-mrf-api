import json
import datetime
import types
from unittest.mock import AsyncMock

import pytest

from api.endpoint import partd_formulary


class FakeMappingResult:
    def __init__(self, rows=None):
        self._rows = rows or []

    def mappings(self):
        return self

    def all(self):
        return self._rows


class FakeSession:
    def __init__(self, results):
        self._results = list(results)

    async def execute(self, *_args, **_kwargs):
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
    payload = json.loads(response.body)

    assert payload["medicare_active"] is True
    assert len(payload["items"]) == 1
    item = payload["items"][0]
    assert item["dispensing_fee_brand_30"] == 1.1
    assert item["dispensing_fee_generic_90"] == 0.3
    assert item["dispensing_fee_selected_drug_60"] == 2.2
