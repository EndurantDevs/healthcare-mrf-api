import json
import types
from datetime import date, datetime

import pytest
from sanic.exceptions import InvalidUsage, NotFound

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


def build_expired_disciplinary_license_row(verified_at):
    """Build one expired current record with reviewed discipline evidence."""
    return MappingRow(
        state_code="TX",
        state_name="Texas",
        board_url=None,
        source_url=None,
        license_number="TX-expired",
        license_type="Pharmacy",
        license_status="active",
        source_status_raw="ACTIVE",
        license_issue_date=None,
        license_effective_date=None,
        license_expiration_date=date(2026, 1, 1),
        last_renewal_date=None,
        disciplinary_flag=True,
        disciplinary_summary="Reviewed action",
        disciplinary_action_date=None,
        entity_name="Sample Pharmacy",
        dba_name=None,
        address_line1=None,
        address_line2=None,
        city=None,
        state="TX",
        zip_code=None,
        phone_number=None,
        source_record_id="expired-1",
        last_snapshot_id="snapshot-1",
        first_seen_at=None,
        last_seen_at=None,
        last_verified_at=verified_at,
    )


def make_request(results, args=None):
    session = FakeSession(results)
    ctx = types.SimpleNamespace(sa_session=session)
    return types.SimpleNamespace(args=args or {}, ctx=ctx)


def test_pharmacy_license_parameter_helpers_cover_invalid_and_explicit_values():
    with pytest.raises(RuntimeError, match="session not available"):
        pharmacy_license._get_session(
            types.SimpleNamespace(ctx=types.SimpleNamespace(sa_session=None))
        )
    with pytest.raises(InvalidUsage, match="numeric"):
        pharmacy_license._parse_npi("invalid")
    with pytest.raises(InvalidUsage, match="positive"):
        pharmacy_license._parse_npi("0")

    assert pharmacy_license._parse_date_param("   ", "as_of") is None
    assert pharmacy_license._parse_date_param("2026-07-22", "as_of") == date(
        2026,
        7,
        22,
    )
    with pytest.raises(InvalidUsage, match="valid ISO date"):
        pharmacy_license._parse_date_param("2026-02-30", "as_of")
    with pytest.raises(InvalidUsage, match="must be an integer"):
        pharmacy_license._parse_int_param("many", "limit")
    assert pharmacy_license._parse_bool_param(" YES ") is True


@pytest.mark.asyncio
async def test_pharmacy_license_table_exists_uses_qualified_relation():
    session = FakeSession([FakeResult(scalar="mrf.pharmacy_license_record")])
    assert await pharmacy_license._table_exists(
        session,
        "pharmacy_license_record",
    ) is True


@pytest.mark.asyncio
async def test_pharmacy_license_missing_tables_and_runs_fail_closed(monkeypatch):
    async def is_fake_table_missing(_session, _table_name, schema="mrf"):
        del schema
        return False

    monkeypatch.setattr(
        pharmacy_license,
        "_table_exists",
        is_fake_table_missing,
    )
    with pytest.raises(NotFound, match="No pharmacy-license imports found"):
        await pharmacy_license.get_pharmacy_license_import_status(
            make_request([])
        )

    coverage_response = await pharmacy_license.get_pharmacy_license_coverage(
        make_request([])
    )
    assert json.loads(coverage_response.body)["items"] == []

    pharmacy_response = await pharmacy_license.get_pharmacy_license_by_npi(
        make_request([]),
        "1518379601",
    )
    pharmacy_payload = json.loads(pharmacy_response.body)
    assert pharmacy_payload["licenses"] == []
    assert pharmacy_payload["history"] == []


@pytest.mark.asyncio
async def test_import_status_requested_run_missing(monkeypatch):
    async def is_fake_table_present(_session, _table_name, schema="mrf"):
        del schema
        return True

    monkeypatch.setattr(
        pharmacy_license,
        "_table_exists",
        is_fake_table_present,
    )
    with pytest.raises(NotFound, match="No pharmacy-license imports found"):
        await pharmacy_license.get_pharmacy_license_import_status(
            make_request([FakeResult(rows=[])], args={"run_id": "run-missing"})
        )


@pytest.mark.asyncio
async def test_get_import_status_returns_aggregates(monkeypatch):
    async def is_fake_table_present(_session, _table_name, schema="mrf"):
        del schema
        return True

    monkeypatch.setattr(pharmacy_license, "_table_exists", is_fake_table_present)

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
    response_payload = json.loads(response.body)

    assert response_payload["run_id"] == "run_1"
    assert response_payload["snapshots"]["count"] == 3
    assert response_payload["snapshots"]["matched_rows"] == 80


@pytest.mark.asyncio
async def test_get_coverage_returns_items(monkeypatch):
    async def is_fake_table_present(_session, _table_name, schema="mrf"):
        del schema
        return True

    monkeypatch.setattr(pharmacy_license, "_table_exists", is_fake_table_present)

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
    response_payload = json.loads(response.body)

    assert response_payload["total_states"] == 2
    assert response_payload["supported_states"] == 1
    assert response_payload["unsupported_states"] == 1


@pytest.mark.asyncio
async def test_get_pharmacy_license_by_npi_returns_summary_and_history(monkeypatch):
    """Verify get pharmacy license by npi returns summary and history."""
    async def is_fake_table_present(_session, _table_name, schema="mrf"):
        del schema
        return True

    monkeypatch.setattr(pharmacy_license, "_table_exists", is_fake_table_present)

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
    response_payload = json.loads(response.body)

    assert response_payload["npi"] == 1518379601
    assert response_payload["summary"]["total_licenses"] == 1
    assert response_payload["summary"]["active_license_count"] == 1
    assert response_payload["summary"]["active_states"] == ["TX"]
    assert len(response_payload["history"]) == 1


@pytest.mark.asyncio
async def test_pharmacy_license_inactive_disciplinary_summary_without_history(
    monkeypatch,
):
    """Keep inactive, discipline, latest-verification, and no-history semantics."""
    async def is_fake_table_present(_session, _table_name, schema="mrf"):
        del schema
        return True

    monkeypatch.setattr(pharmacy_license, "_table_exists", is_fake_table_present)
    verified_at = datetime(2026, 7, 20, 12, 0)
    api_request = make_request(
        [
            FakeResult(
                rows=[build_expired_disciplinary_license_row(verified_at)]
            )
        ],
        args={"as_of": "2026-07-22", "include_history": "false"},
    )

    response = await pharmacy_license.get_pharmacy_license_by_npi(
        api_request,
        "1518379601",
    )
    response_payload = json.loads(response.body)

    assert response_payload["summary"] == {
        "total_licenses": 1,
        "active_license_count": 0,
        "active_states": [],
        "disciplinary_flag_any": True,
        "latest_verified_at": verified_at.isoformat(),
    }
    assert response_payload["history"] == []
