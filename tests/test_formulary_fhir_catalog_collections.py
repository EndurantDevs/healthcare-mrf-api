# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Current-catalog collection contracts for FHIR formulary serving."""

from __future__ import annotations

import base64
import datetime as dt
import json

import pytest
from sqlalchemy.dialects import postgresql

from api import formulary_fhir_catalog as catalog
from api import formulary_fhir_catalog_sql as catalog_sql
from api import formulary_fhir_cursor as cursor
from api import formulary_fhir_serving as serving


FORMULARY_A = "fhir_at4rcuzsyttz7txu3xtoxsa734"
FORMULARY_B = "fhir_bt4rcuzsyttz7txu3xtoxsa734"
ALIAS_A = "ffa_" + "1" * 48
ALIAS_B = "ffa_" + "2" * 48
DATASET_ID = "ffd_" + "a" * 48
PUBLISHED_AT = dt.datetime(2026, 8, 8, 6, tzinfo=dt.UTC)
LAST_UPDATED = dt.datetime(2026, 8, 7, 5, tzinfo=dt.UTC)
AS_OF = dt.datetime(2026, 8, 8, tzinfo=dt.UTC)
CURSOR_KEY = base64.urlsafe_b64encode(b"c" * 32).decode("ascii").rstrip("=")
ENVIRONMENT = {
    serving.FHIR_FORMULARY_SERVING_ENABLED_ENV: "true",
    cursor.FHIR_FORMULARY_CURSOR_KEY_ENV: CURSOR_KEY,
}
NO_COVERAGE = {
    "coverage_required": False,
    "coverage_expected_artifact_count": None,
    "coverage_receipt_expected_artifact_count": None,
    "coverage_included_artifact_count": None,
    "coverage_missing_artifact_count": None,
}


def _detail_record(formulary_id: str, **changes):
    record_by_field = {
        "formulary_id": formulary_id,
        "status": "current",
        "title": "Synthetic Coverage Plan",
        "name": "Synthetic Formulary",
        "period_start": None,
        "period_end": None,
        "last_updated": LAST_UPDATED,
        "as_of": AS_OF,
        "published_at": PUBLISHED_AT,
        **NO_COVERAGE,
        "source_id": "private-source",
        "dataset_id": "private-dataset",
        "run_id": "private-run",
        "upstream_list_id": "private-list",
        "metadata_json": {"private": True},
    }
    record_by_field.update(changes)
    return record_by_field


class _MappingResult:
    def __init__(self, records):
        self._records = tuple(records)

    def mappings(self):
        return self

    def all(self):
        return self._records


class _Transaction:
    def __init__(self, session):
        self._session = session

    async def __aenter__(self):
        assert not self._session.active
        self._session.active = True
        self._session.events.append(("enter", None, None))
        return self._session

    async def __aexit__(self, exc_type, _exc, _traceback):
        self._session.events.append(
            ("exit", None if exc_type is None else exc_type.__name__, None)
        )
        self._session.active = False
        return False


class _ScriptedSession:
    def __init__(self, *record_sets):
        self._record_sets = list(record_sets)
        self.events = []
        self.active = False

    def begin(self):
        self.events.append(("begin", None, None))
        return _Transaction(self)

    async def execute(self, statement, params=None):
        assert self.active
        assert self._record_sets, "unexpected database query"
        self.events.append(("execute", statement, params))
        return _MappingResult(self._record_sets.pop(0))

    def assert_exhausted(self):
        assert self._record_sets == []
        assert self.active is False


def _normalized_sql(statement) -> str:
    compiled = statement.compile(dialect=postgresql.dialect())
    return " ".join(str(compiled).split())


def _catalog_evidence(published_at=PUBLISHED_AT):
    return (
        (
            {
                "dataset_id": "dataset-a",
                "formulary_id": FORMULARY_A,
                "published_at": published_at,
            },
            {
                "dataset_id": "dataset-a",
                "formulary_id": FORMULARY_B,
                "published_at": published_at,
            },
        ),
        ({"dataset_id": "dataset-a", "list_count": 2, **NO_COVERAGE},),
    )


def test_catalog_sql_exactly_owns_current_source_plan_and_alias_rows():
    catalog_sql_text = _normalized_sql(catalog_sql.FORMULARY_PAGE_STATEMENT)
    alias_sql_text = _normalized_sql(catalog_sql.ALIAS_PAGE_STATEMENT)

    for required_fragment in (
        "fhir_formulary_dataset.source_id = "
        "mrf.fhir_formulary_current.source_id",
        "fhir_formulary_dataset.dataset_id = "
        "mrf.fhir_formulary_current.dataset_id",
        "fhir_formulary_dataset_coverage_plan.source_id = "
        "mrf.fhir_formulary_dataset.source_id",
        "fhir_formulary_coverage_plan.source_id = "
        "mrf.fhir_formulary_dataset_coverage_plan.source_id",
        "fhir_formulary_dataset.published_at = "
        "mrf.fhir_formulary_current.published_at",
    ):
        assert required_fragment in catalog_sql_text
    for required_fragment in (
        "fhir_formulary_dataset_alias.source_id = "
        "mrf.fhir_formulary_dataset.source_id",
        "fhir_formulary_drug_plan_alias.source_id = "
        "mrf.fhir_formulary_dataset_alias.source_id",
        "fhir_formulary_drug_plan_alias.public_id = "
        "mrf.fhir_formulary_coverage_plan.public_id",
        "fhir_formulary_drug_plan_alias_version.source_id = "
        "mrf.fhir_formulary_dataset_alias.source_id",
        "fhir_formulary_drug_plan_alias_version.alias_version_id = "
        "mrf.fhir_formulary_dataset_alias.alias_version_id",
    ):
        assert required_fragment in alias_sql_text
    assert " OFFSET " not in catalog_sql_text + alias_sql_text
    assert " FOR UPDATE" not in catalog_sql_text + alias_sql_text


async def _read_first_formulary_page():
    marker_rows, count_rows = _catalog_evidence()
    first_session = _ScriptedSession(
        (),
        marker_rows,
        count_rows,
        (_detail_record(FORMULARY_A), _detail_record(FORMULARY_B)),
    )

    first_page = await catalog.read_current_fhir_formularies(
        first_session,
        limit=1,
        environment=ENVIRONMENT,
    )
    first_session.assert_exhausted()
    return first_session, first_page


@pytest.mark.asyncio
async def test_collection_page_uses_snapshot_and_hides_private_rows():
    first_session, first_page = await _read_first_formulary_page()

    assert [event[2] for event in first_session.events if event[0] == "execute"] == [
        None,
        None,
        None,
        {"last_id": "", "page_size": 2},
    ]
    assert first_page.next_cursor is not None
    response_by_field = catalog.public_fhir_formulary_page_payload(first_page)
    assert response_by_field["items"] == [
        {
            "formulary_id": FORMULARY_A,
            "status": "current",
            "title": "Synthetic Coverage Plan",
            "name": "Synthetic Formulary",
            "period": None,
            "last_updated": "2026-08-07T05:00:00Z",
            "as_of": "2026-08-08T00:00:00Z",
            "published_at": "2026-08-08T06:00:00Z",
            "coverage": None,
        }
    ]
    rendered_response = json.dumps(response_by_field, sort_keys=True)
    for private_value in (
        "private-source",
        "private-dataset",
        "private-run",
        "private-list",
        "metadata_json",
    ):
        assert private_value not in rendered_response


@pytest.mark.asyncio
async def test_collection_cursor_continues_after_last_public_id():
    _first_session, first_page = await _read_first_formulary_page()
    marker_rows, count_rows = _catalog_evidence()

    second_session = _ScriptedSession(
        (),
        marker_rows,
        count_rows,
        (_detail_record(FORMULARY_B),),
    )
    second_page = await catalog.read_current_fhir_formularies(
        second_session,
        limit=1,
        cursor=first_page.next_cursor,
        environment=ENVIRONMENT,
    )

    second_session.assert_exhausted()
    assert tuple(
        detail.formulary_id for detail in second_page.items
    ) == (FORMULARY_B,)
    assert second_page.next_cursor is None
    assert second_session.events[-2][2] == {
        "last_id": FORMULARY_A,
        "page_size": 2,
    }


@pytest.mark.asyncio
async def test_catalog_cursor_conflicts_before_page_read_after_publication_change():
    _first_session, first_page = await _read_first_formulary_page()
    changed_marker_rows, changed_count_rows = _catalog_evidence(
        PUBLISHED_AT + dt.timedelta(minutes=1)
    )
    changed_session = _ScriptedSession((), changed_marker_rows, changed_count_rows)

    with pytest.raises(serving.FHIRFormularyCursorConflictError):
        await catalog.read_current_fhir_formularies(
            changed_session,
            limit=1,
            cursor=first_page.next_cursor,
            environment=ENVIRONMENT,
        )

    changed_session.assert_exhausted()
    execute_events = [
        event for event in changed_session.events if event[0] == "execute"
    ]
    assert len(execute_events) == 3


@pytest.mark.asyncio
async def test_alias_page_is_parent_scoped_opaque_and_cursor_paginated():
    context_rows = (
        {
            "dataset_id": DATASET_ID,
            "formulary_id": FORMULARY_A,
            "generation": 1,
            "published_at": PUBLISHED_AT,
            **NO_COVERAGE,
        },
    )
    alias_rows = (
        {
            "formulary_id": FORMULARY_A,
            "alias_id": ALIAS_A,
            "drug_count": 3,
            "source_plan_identifier": "private-plan-a",
        },
        {
            "formulary_id": FORMULARY_A,
            "alias_id": ALIAS_B,
            "drug_count": 5,
            "source_plan_identifier": "private-plan-b",
        },
    )
    session = _ScriptedSession((), context_rows, alias_rows)

    page = await catalog.read_current_fhir_formulary_aliases(
        session,
        FORMULARY_A,
        limit=1,
        environment=ENVIRONMENT,
    )

    session.assert_exhausted()
    assert session.events[-2][2] == {
        "public_id": FORMULARY_A,
        "last_id": "",
        "page_size": 2,
    }
    assert catalog.public_fhir_formulary_alias_page_payload(page) == {
        "items": [
            {
                "formulary_id": FORMULARY_A,
                "alias_id": ALIAS_A,
                "drug_count": 3,
                "coverage": None,
            }
        ],
        "next_cursor": page.next_cursor,
    }
    assert "private-plan" not in json.dumps(
        catalog.public_fhir_formulary_alias_page_payload(page)
    )


@pytest.mark.asyncio
async def test_alias_cursor_rejects_same_timestamp_new_generation_before_page_read():
    first_context = (
        {
            "dataset_id": DATASET_ID,
            "formulary_id": FORMULARY_A,
            "generation": 1,
            "published_at": PUBLISHED_AT,
            **NO_COVERAGE,
        },
    )
    alias_rows = (
        {"formulary_id": FORMULARY_A, "alias_id": ALIAS_A, "drug_count": 1},
        {"formulary_id": FORMULARY_A, "alias_id": ALIAS_B, "drug_count": 1},
    )
    first_session = _ScriptedSession((), first_context, alias_rows)
    first_page = await catalog.read_current_fhir_formulary_aliases(
        first_session,
        FORMULARY_A,
        limit=1,
        environment=ENVIRONMENT,
    )
    assert first_page.next_cursor is not None

    changed_context = (
        {
            "dataset_id": "ffd_" + "b" * 48,
            "formulary_id": FORMULARY_A,
            "generation": 2,
            "published_at": PUBLISHED_AT,
            **NO_COVERAGE,
        },
    )
    changed_session = _ScriptedSession((), changed_context)
    with pytest.raises(serving.FHIRFormularyCursorConflictError):
        await catalog.read_current_fhir_formulary_aliases(
            changed_session,
            FORMULARY_A,
            limit=1,
            cursor=first_page.next_cursor,
            environment=ENVIRONMENT,
        )

    changed_session.assert_exhausted()
    assert len(
        [event for event in changed_session.events if event[0] == "execute"]
    ) == 2


@pytest.mark.asyncio
async def test_incomplete_current_dataset_census_fails_before_page_read():
    marker_rows, _count_rows = _catalog_evidence()
    session = _ScriptedSession(
        (),
        marker_rows[:1],
        ({"dataset_id": "dataset-a", "list_count": 2},),
    )

    with pytest.raises(serving.FHIRFormularyServingUnavailableError):
        await catalog.read_current_fhir_formularies(
            session,
            limit=1,
            environment=ENVIRONMENT,
        )

    session.assert_exhausted()
    assert len([event for event in session.events if event[0] == "execute"]) == 3


@pytest.mark.parametrize("page_kind", ("formularies", "aliases"))
def test_public_catalog_payloads_reject_more_than_page_limit(page_kind):
    if page_kind == "formularies":
        detail = serving._detail_from_record(_detail_record(FORMULARY_A))
        oversized_page = catalog.PublicFHIRFormularyPage(
            items=(detail,) * 101,
            next_cursor=None,
        )
        payload_factory = catalog.public_fhir_formulary_page_payload
    else:
        alias_rows = tuple(
            catalog.PublicFHIRFormularyAlias(
                formulary_id=FORMULARY_A,
                alias_id="ffa_" + f"{index:048x}",
                drug_count=1,
            )
            for index in range(101)
        )
        oversized_page = catalog.PublicFHIRFormularyAliasPage(
            items=alias_rows,
            next_cursor=None,
        )
        payload_factory = catalog.public_fhir_formulary_alias_page_payload

    with pytest.raises(serving.FHIRFormularyServingUnavailableError):
        payload_factory(oversized_page)
