import json
import types

import pytest

from api.endpoint import pharmacy_license


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


def make_request(results, args=None):
    session = FakeSession(results)
    ctx = types.SimpleNamespace(sa_session=session)
    return types.SimpleNamespace(args=args or {}, ctx=ctx)


@pytest.mark.asyncio
async def test_get_import_status_returns_aggregates(monkeypatch):
    async def fake_table_exists(_session, _table_name, schema="mrf"):
        del schema
        return True

    monkeypatch.setattr(pharmacy_license, "_table_exists", fake_table_exists)

    request = make_request(
        [
            FakeResult(
                rows=[
                    MappingRow(
                        run_id="run_1",
                        import_id="20260310",
                        status="completed",
                        started_at=None,
                        finished_at=None,
                        source_summary={"states": 3},
                        error_text=None,
                    )
                ]
            ),
            FakeResult(
                rows=[
                    MappingRow(
                        snapshot_count=3,
                        parsed_rows=100,
                        matched_rows=80,
                        dropped_rows=20,
                        inserted_rows=80,
                    )
                ]
            ),
        ]
    )

    response = await pharmacy_license.get_pharmacy_license_import_status(request)
    payload = json.loads(response.body)

    assert payload["run_id"] == "run_1"
    assert payload["snapshots"]["count"] == 3
    assert payload["snapshots"]["matched_rows"] == 80


@pytest.mark.asyncio
async def test_get_coverage_returns_items(monkeypatch):
    async def fake_table_exists(_session, _table_name, schema="mrf"):
        del schema
        return True

    monkeypatch.setattr(pharmacy_license, "_table_exists", fake_table_exists)

    request = make_request(
        [
            FakeResult(
                rows=[
                    MappingRow(
                        state_code="TX",
                        state_name="Texas",
                        board_url="https://example.com/tx",
                        source_url="https://example.com/tx.csv",
                        supported=True,
                        unsupported_reason=None,
                        status="completed",
                        last_attempted_at=None,
                        last_success_at=None,
                        last_run_id="run_1",
                        records_parsed=10,
                        records_matched=8,
                        records_dropped=2,
                        records_inserted=8,
                    ),
                    MappingRow(
                        state_code="NM",
                        state_name="New Mexico",
                        board_url="https://example.com/nm",
                        source_url=None,
                        supported=False,
                        unsupported_reason="no_machine_readable_link",
                        status="unsupported",
                        last_attempted_at=None,
                        last_success_at=None,
                        last_run_id="run_1",
                        records_parsed=0,
                        records_matched=0,
                        records_dropped=0,
                        records_inserted=0,
                    ),
                ]
            )
        ]
    )

    response = await pharmacy_license.get_pharmacy_license_coverage(request)
    payload = json.loads(response.body)

    assert payload["total_states"] == 2
    assert payload["supported_states"] == 1
    assert payload["unsupported_states"] == 1


@pytest.mark.asyncio
async def test_get_pharmacy_license_by_npi_returns_summary_and_history(monkeypatch):
    async def fake_table_exists(_session, _table_name, schema="mrf"):
        del schema
        return True

    monkeypatch.setattr(pharmacy_license, "_table_exists", fake_table_exists)

    request = make_request(
        [
            FakeResult(
                rows=[
                    MappingRow(
                        state_code="TX",
                        state_name="Texas",
                        board_url="https://example.com/tx",
                        source_url="https://example.com/tx.csv",
                        license_number="TX-123",
                        license_type="Pharmacy",
                        license_status="active",
                        source_status_raw="ACTIVE",
                        license_issue_date=None,
                        license_effective_date=None,
                        license_expiration_date=None,
                        last_renewal_date=None,
                        disciplinary_flag=False,
                        disciplinary_summary=None,
                        disciplinary_action_date=None,
                        entity_name="Sample Pharmacy",
                        dba_name=None,
                        address_line1="100 Main",
                        address_line2=None,
                        city="Austin",
                        state="TX",
                        zip_code="78701",
                        phone_number="555",
                        source_record_id="r1",
                        last_snapshot_id="snap_1",
                        first_seen_at=None,
                        last_seen_at=None,
                        last_verified_at=None,
                    )
                ]
            ),
            FakeResult(
                rows=[
                    MappingRow(
                        snapshot_id="snap_1",
                        run_id="run_1",
                        state_code="TX",
                        license_number="TX-123",
                        license_status="active",
                        source_status_raw="ACTIVE",
                        disciplinary_flag=False,
                        disciplinary_summary=None,
                        license_expiration_date=None,
                        imported_at=None,
                    )
                ]
            ),
        ]
    )

    response = await pharmacy_license.get_pharmacy_license_by_npi(request, "1518379601")
    payload = json.loads(response.body)

    assert payload["npi"] == 1518379601
    assert payload["summary"]["total_licenses"] == 1
    assert payload["summary"]["active_license_count"] == 1
    assert payload["summary"]["active_states"] == ["TX"]
    assert len(payload["history"]) == 1
