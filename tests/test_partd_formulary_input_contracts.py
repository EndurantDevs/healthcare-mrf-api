import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sanic.exceptions import InvalidUsage

from api.endpoint import partd_formulary


class _ScalarResult:
    def __init__(self, value):
        self._value = value

    def scalar(self):
        return self._value


class _MappingResult:
    def __init__(self, value):
        self._value = value

    def mappings(self):
        return self

    def first(self):
        return self._value


class _Session:
    def __init__(self, *results):
        self.results = list(results)
        self.calls = []

    async def execute(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        return self.results.pop(0)


def test_request_session_is_required_and_preserved():
    session = object()
    request = SimpleNamespace(ctx=SimpleNamespace(sa_session=session))

    assert partd_formulary._get_session(request) is session

    with pytest.raises(RuntimeError, match="session not available"):
        partd_formulary._get_session(SimpleNamespace(ctx=SimpleNamespace()))


@pytest.mark.parametrize("raw_value", [None, "", "null", "  "])
def test_optional_date_values_are_absent(raw_value):
    assert partd_formulary._parse_date_param(raw_value, "as_of") is None


def test_date_values_use_an_explicit_iso_contract():
    assert partd_formulary._parse_date_param(
        "2026-07-24",
        "as_of",
    ) == datetime.date(2026, 7, 24)

    with pytest.raises(InvalidUsage, match="valid ISO date"):
        partd_formulary._parse_date_param("07/24/2026", "as_of")


@pytest.mark.parametrize("raw_value", [None, "", "null"])
def test_optional_integer_values_are_absent(raw_value):
    assert partd_formulary._parse_int_param(raw_value, "year") is None


def test_integer_and_npi_inputs_fail_with_field_specific_messages():
    assert partd_formulary._parse_int_param(" 2026 ", "year") == 2026
    assert partd_formulary._parse_npi(" 1518379601 ") == 1_518_379_601

    with pytest.raises(InvalidUsage, match="'year' must be an integer"):
        partd_formulary._parse_int_param("twenty", "year")
    with pytest.raises(InvalidUsage, match="NPI must be numeric"):
        partd_formulary._parse_npi("not-an-npi")
    with pytest.raises(InvalidUsage, match="NPI must be positive"):
        partd_formulary._parse_npi("0")


def test_plan_ids_are_trimmed_deduplicated_bounded_and_sorted():
    long_plan_id = "plan-" + ("x" * 40)

    assert partd_formulary._parse_plan_ids(None) == []
    assert partd_formulary._parse_plan_ids(
        f" plan-b,plan-a,plan-b,,{long_plan_id} "
    ) == [
        "plan-a",
        "plan-b",
        long_plan_id[:32],
    ]


def test_medication_codes_are_normalized_without_guessing_systems():
    assert partd_formulary._normalize_code("RxCUI", "RX-12") == ("RXNORM", "12")
    assert partd_formulary._normalize_code("ndc", "00011-2222-33") == (
        "NDC",
        "00011222233",
    )

    with pytest.raises(InvalidUsage, match="code_system must be one of"):
        partd_formulary._normalize_code("hcpcs", "123")
    with pytest.raises(InvalidUsage, match="at least one digit"):
        partd_formulary._normalize_code("rxnorm", "unknown")


def test_sql_bind_contract():
    parameter_by_name: dict[str, object] = {}

    assert partd_formulary._bind_in_clause("plan", [], parameter_by_name) == "NULL"
    assert parameter_by_name == {}
    assert partd_formulary._bind_in_clause(
        "plan",
        ["plan-a", "plan-b"],
        parameter_by_name,
    ) == ":plan_0, :plan_1"
    assert parameter_by_name == {"plan_0": "plan-a", "plan_1": "plan-b"}

    assert partd_formulary._qualified_table_name(
        SimpleNamespace(schema=None, name="costs")
    ) == "mrf.costs"
    assert partd_formulary._qualified_table_name(
        SimpleNamespace(schema="archive", name="costs")
    ) == "archive.costs"


@pytest.mark.asyncio
@pytest.mark.parametrize(("database_value", "expected"), [(None, False), ("table", True)])
async def test_table_presence_uses_database_regclass(database_value, expected):
    session = _Session(_ScalarResult(database_value))
    table = SimpleNamespace(schema="mrf", name="pharmacy_license_record")

    assert await partd_formulary._table_exists(session, table) is expected
    assert session.calls[0][0][1] == {"name": "mrf.pharmacy_license_record"}


@pytest.mark.asyncio
async def test_missing_license_table_returns_an_explicit_empty_summary(monkeypatch):
    monkeypatch.setattr(
        partd_formulary,
        "_table_exists",
        AsyncMock(return_value=False),
    )

    summary = await partd_formulary._fetch_state_license_summary(
        _Session(),
        1_518_379_601,
        datetime.date(2026, 7, 24),
    )

    assert summary == {
        "has_active_state_license": False,
        "active_license_count": 0,
        "active_states": [],
        "disciplinary_flag_any": False,
        "license_checked_at": None,
    }


@pytest.mark.asyncio
async def test_license_summary_normalizes_states_and_verification_time(monkeypatch):
    monkeypatch.setattr(
        partd_formulary,
        "_table_exists",
        AsyncMock(return_value=True),
    )
    verified_at = datetime.datetime(2026, 7, 24, 10, 30, tzinfo=datetime.timezone.utc)
    session = _Session(
        _MappingResult(
            {
                "active_count": 2,
                "active_states": ["tx", "TX", None, "ca"],
                "disciplinary_flag_any": True,
                "latest_verified_at": verified_at,
            }
        )
    )

    summary = await partd_formulary._fetch_state_license_summary(
        session,
        1_518_379_601,
        datetime.date(2026, 7, 24),
    )

    assert summary == {
        "has_active_state_license": True,
        "active_license_count": 2,
        "active_states": ["CA", "TX"],
        "disciplinary_flag_any": True,
        "license_checked_at": "2026-07-24T10:30:00+00:00",
    }
