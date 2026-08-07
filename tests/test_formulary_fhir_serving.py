# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Public-detail contracts for current FHIR formulary serving."""

from __future__ import annotations

import datetime as dt

import pytest

from api import formulary_fhir_serving as serving


FORMULARY_ID = "fhir_at4rcuzsyttz7txu3xtoxsa734"
PUBLISHED_AT = dt.datetime(2026, 8, 7, 19, tzinfo=dt.UTC)
AS_OF = dt.datetime(2026, 8, 6, tzinfo=dt.UTC)
LAST_UPDATED = dt.datetime(2026, 8, 5, tzinfo=dt.UTC)
ENABLED = {serving.FHIR_FORMULARY_SERVING_ENABLED_ENV: "true"}


def _detail_record(**changes):
    record_by_field = {
        "formulary_id": FORMULARY_ID,
        "status": "current",
        "title": "Synthetic Coverage Plan",
        "name": "Synthetic Formulary",
        "period_start": None,
        "period_end": None,
        "last_updated": LAST_UPDATED,
        "as_of": AS_OF,
        "published_at": PUBLISHED_AT,
    }
    record_by_field.update(changes)
    return record_by_field


class _MappingResult:
    def __init__(self, records):
        self._records = records

    def mappings(self):
        return self

    def all(self):
        return self._records


class _Transaction:
    def __init__(self, session):
        self._session = session

    async def __aenter__(self):
        assert self._session.is_active is False
        self._session.is_active = True
        self._session.events.append("enter")
        return self._session

    async def __aexit__(self, exc_type, _exc, _traceback):
        self._session.events.append(
            ("exit", None if exc_type is None else exc_type.__name__)
        )
        self._session.is_active = False
        return False


class _Session:
    def __init__(self, records):
        self.records = records
        self.events = []
        self.is_active = False

    def begin(self):
        self.events.append("begin")
        return _Transaction(self)

    async def execute(self, statement, params=None):
        assert self.is_active is True
        if params is None:
            self.events.append(("transaction", str(statement)))
            return object()
        self.events.append(("query", dict(params)))
        return _MappingResult(self.records)


@pytest.mark.parametrize(
    "raw_setting",
    (None, "", "0", "false", "off", "typo"),
)
@pytest.mark.asyncio
async def test_default_off_gate_stops_before_transaction(raw_setting):
    environment_by_name = {}
    if raw_setting is not None:
        environment_by_name[
            serving.FHIR_FORMULARY_SERVING_ENABLED_ENV
        ] = raw_setting
    session = _Session([_detail_record()])

    with pytest.raises(serving.FHIRFormularyServingUnavailableError):
        await serving.read_current_fhir_formulary(
            session,
            FORMULARY_ID,
            environment=environment_by_name,
        )

    assert session.events == []


@pytest.mark.parametrize("raw_setting", ("1", "true", "YES", " on "))
def test_explicit_serving_values_are_accepted(raw_setting):
    assert serving.is_fhir_formulary_serving_enabled(
        {serving.FHIR_FORMULARY_SERVING_ENABLED_ENV: raw_setting}
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "formulary_id",
    (None, "", "fhir_short", "FHIR_at4rcuzsyttz7txu3xtoxsa734", "../private"),
)
async def test_malformed_identifiers_collapse_before_sql(formulary_id):
    session = _Session([_detail_record()])

    with pytest.raises(serving.FHIRFormularyNotFoundError):
        await serving.read_current_fhir_formulary(
            session,
            formulary_id,
            environment=ENABLED,
        )

    assert session.events == []


@pytest.mark.asyncio
async def test_detail_uses_one_read_only_snapshot_and_allowlisted_payload():
    session = _Session([_detail_record()])

    detail = await serving.read_current_fhir_formulary(
        session,
        FORMULARY_ID,
        environment=ENABLED,
    )

    assert serving.public_fhir_formulary_payload(detail) == {
        "formulary_id": FORMULARY_ID,
        "status": "current",
        "title": "Synthetic Coverage Plan",
        "name": "Synthetic Formulary",
        "period": None,
        "last_updated": "2026-08-05T00:00:00Z",
        "as_of": "2026-08-06T00:00:00Z",
        "published_at": "2026-08-07T19:00:00Z",
    }
    assert session.events == [
        "begin",
        "enter",
        (
            "transaction",
            "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY",
        ),
        ("query", {"public_id": FORMULARY_ID}),
        ("exit", None),
    ]


@pytest.mark.asyncio
async def test_optional_period_is_rendered_in_utc():
    period_start = dt.datetime(
        2026,
        8,
        1,
        2,
        tzinfo=dt.timezone(dt.timedelta(hours=2)),
    )
    session = _Session(
        [_detail_record(period_start=period_start, period_end=PUBLISHED_AT)]
    )

    detail = await serving.read_current_fhir_formulary(
        session,
        FORMULARY_ID,
        environment=ENABLED,
    )

    assert serving.public_fhir_formulary_payload(detail)["period"] == {
        "start": "2026-08-01T00:00:00Z",
        "end": "2026-08-07T19:00:00Z",
    }


@pytest.mark.asyncio
async def test_optional_text_fields_remain_explicit_nulls():
    session = _Session([_detail_record(status=None, title=None, name=None)])

    detail = await serving.read_current_fhir_formulary(
        session,
        FORMULARY_ID,
        environment=ENABLED,
    )

    response_by_field = serving.public_fhir_formulary_payload(detail)
    assert response_by_field["status"] is None
    assert response_by_field["title"] is None
    assert response_by_field["name"] is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("records", "error_type"),
    (
        ([], serving.FHIRFormularyNotFoundError),
        (
            [_detail_record(), _detail_record()],
            serving.FHIRFormularyServingUnavailableError,
        ),
    ),
)
async def test_missing_and_ambiguous_current_records_fail_closed(
    records,
    error_type,
):
    with pytest.raises(error_type):
        await serving.read_current_fhir_formulary(
            _Session(records),
            FORMULARY_ID,
            environment=ENABLED,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "record_changes",
    (
        {"formulary_id": "fhir_invalid"},
        {"status": " bad"},
        {"title": "x" * 2_049},
        {"name": "bad\nname"},
        {"last_updated": None},
        {"as_of": dt.datetime(2026, 8, 6)},
        {"published_at": "private-timestamp"},
        {"period_start": "private-period"},
    ),
)
async def test_invalid_stored_response_evidence_fails_closed(record_changes):
    session = _Session([_detail_record(**record_changes)])

    with pytest.raises(serving.FHIRFormularyServingUnavailableError):
        await serving.read_current_fhir_formulary(
            session,
            FORMULARY_ID,
            environment=ENABLED,
        )


def test_payload_rejects_untrusted_shape():
    with pytest.raises(serving.FHIRFormularyServingUnavailableError):
        serving.public_fhir_formulary_payload(object())

    invalid_detail = serving.PublicFHIRFormularyDetail(
        formulary_id=FORMULARY_ID,
        status="current",
        title="private\nvalue",
        name=None,
        period_start=None,
        period_end=None,
        last_updated=LAST_UPDATED,
        as_of=AS_OF,
        published_at=PUBLISHED_AT,
    )
    with pytest.raises(serving.FHIRFormularyServingUnavailableError):
        serving.public_fhir_formulary_payload(invalid_detail)
