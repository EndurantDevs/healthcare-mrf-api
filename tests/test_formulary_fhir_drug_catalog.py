# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Alias-scoped medication collection contracts for FHIR formulary serving."""

from __future__ import annotations

import base64
import datetime as dt
import json

import pytest
from sqlalchemy.dialects import postgresql

from api import formulary_fhir_cursor as cursor
from api import formulary_fhir_drug_values as drug_values
from api import formulary_fhir_drug_sql as drug_sql
from api import formulary_fhir_drugs as drugs
from api import formulary_fhir_serving as serving


FORMULARY_ID = "fhir_at4rcuzsyttz7txu3xtoxsa734"
ALIAS_ID = "ffa_" + "1" * 48
ALIAS_VERSION_ID = "ffav_" + "a" * 48
DATASET_ID = "ffd_" + "a" * 48
DRUG_A = "ffm_" + "1" * 48
DRUG_B = "ffm_" + "2" * 48
DRUG_C = "ffm_" + "3" * 48
PUBLISHED_AT = dt.datetime(2026, 8, 8, 6, tzinfo=dt.UTC)
LAST_UPDATED = dt.datetime(2026, 8, 7, 5, tzinfo=dt.UTC)
CURSOR_KEY = base64.urlsafe_b64encode(b"d" * 32).decode("ascii").rstrip("=")
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
FILTERS = drug_values.FHIRFormularyDrugFilters(
    rxnorm_id="123456",
    ndc11="00011122233",
    tier="Preferred",
    prior_authorization=False,
    step_therapy=True,
    quantity_limit=False,
)
ALTERNATIVE_ROWS = (
    {
        "owner_drug_id": DRUG_A,
        "resolved": True,
        "target_drug_id": DRUG_B,
    },
    {
        "owner_drug_id": DRUG_A,
        "resolved": True,
        "target_drug_id": DRUG_B,
    },
    {
        "owner_drug_id": DRUG_A,
        "resolved": False,
        "target_drug_id": None,
    },
)


def _context_record(**changes):
    record_by_field = {
        "source_id": "private-source",
        "dataset_id": DATASET_ID,
        "formulary_id": FORMULARY_ID,
        "alias_id": ALIAS_ID,
        "alias_version_id": ALIAS_VERSION_ID,
        "generation": 1,
        "published_at": PUBLISHED_AT,
        **NO_COVERAGE,
    }
    record_by_field.update(changes)
    return record_by_field


def _drug_record(drug_id: str, upstream_id: str, **changes):
    record_by_field = {
        "formulary_id": FORMULARY_ID,
        "alias_id": ALIAS_ID,
        "upstream_medication_id": upstream_id,
        "drug_id": drug_id,
        "status": "active",
        "name": "Synthetic Medication",
        "rxnorm_id": "123456",
        "ndc11": "00011122233",
        "last_updated": LAST_UPDATED,
        "tier": "Preferred",
        "prior_authorization": False,
        "step_therapy": True,
        "quantity_limit": False,
        "source_id": "private-source",
        "raw_reference": "Medication/private-reference",
        "evidence_json": {"private": True},
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


def _expected_filter_params(last_id=""):
    return {
        "public_id": FORMULARY_ID,
        "alias_id": ALIAS_ID,
        "last_id": last_id,
        "page_size": 2,
        "ndc11": "00011122233",
        "prior_authorization": False,
        "quantity_limit": False,
        "rxnorm_id": "123456",
        "step_therapy": True,
        "tier": "Preferred",
    }


def test_drug_sql_exactly_owns_current_source_medication_and_alias_targets():
    drug_sql_text = _normalized_sql(drug_sql.drug_statement(FILTERS))
    alternative_sql_text = _normalized_sql(drug_sql.ALTERNATIVE_STATEMENT)

    for required_fragment in (
        "fhir_formulary_dataset.source_id = "
        "mrf.fhir_formulary_current.source_id",
        "fhir_formulary_dataset.published_at = "
        "mrf.fhir_formulary_current.published_at",
        "fhir_formulary_alias_membership.source_id = "
        "mrf.fhir_formulary_drug_plan_alias_version.source_id",
        "fhir_formulary_alias_membership.alias_version_id = "
        "mrf.fhir_formulary_drug_plan_alias_version.alias_version_id",
        "fhir_formulary_medication.source_id = "
        "mrf.fhir_formulary_alias_membership.source_id",
        "fhir_formulary_medication.upstream_medication_id = "
        "mrf.fhir_formulary_alias_membership.upstream_medication_id",
        "fhir_formulary_medication.medication_version_id = "
        "mrf.fhir_formulary_alias_membership.medication_version_id",
    ):
        assert required_fragment in drug_sql_text
    for filter_bind in (
        "rxnorm_id = %(rxnorm_id)s",
        "ndc11 = %(ndc11)s",
        "drug_tier = %(tier)s",
        "prior_authorization = %(prior_authorization)s",
        "step_therapy = %(step_therapy)s",
        "quantity_limit = %(quantity_limit)s",
    ):
        assert filter_bind in drug_sql_text
    assert (
        "target_membership.alias_version_id = "
        "mrf.fhir_formulary_alternative.alias_version_id"
    ) in alternative_sql_text
    assert (
        "target_membership.upstream_medication_id = "
        "mrf.fhir_formulary_alternative.resolved_medication_id"
    ) in alternative_sql_text
    assert "fhir_formulary_drug_plan_alias.alias_id = %(alias_id)s" in (
        alternative_sql_text
    )
    assert " OFFSET " not in drug_sql_text + alternative_sql_text
    assert " FOR UPDATE" not in drug_sql_text + alternative_sql_text


async def _read_first_filtered_drug_page():
    first_session = _ScriptedSession(
        (),
        (_context_record(),),
        (
            _drug_record(DRUG_A, "medication-a"),
            _drug_record(DRUG_B, "medication-b"),
        ),
        ALTERNATIVE_ROWS,
    )

    first_page = await drugs.read_current_fhir_formulary_drug_page(
        first_session,
        FORMULARY_ID,
        ALIAS_ID,
        filters=FILTERS,
        limit=1,
        environment=ENVIRONMENT,
    )
    first_session.assert_exhausted()
    execute_events = [event for event in first_session.events if event[0] == "execute"]
    assert execute_events[2][2] == _expected_filter_params()
    assert execute_events[3][2] == {
        "public_id": FORMULARY_ID,
        "alias_id": ALIAS_ID,
        "owner_drug_ids": [DRUG_A],
        "alternative_limit": 101,
    }
    return first_session, first_page


@pytest.mark.asyncio
async def test_filtered_page_hydrates_same_alias_targets_without_private_data():
    _first_session, first_page = await _read_first_filtered_drug_page()

    assert first_page.next_cursor is not None
    response_by_field = drug_values.public_fhir_formulary_drug_page_payload(
        first_page
    )
    assert response_by_field["items"] == [
        {
            "formulary_id": FORMULARY_ID,
            "alias_id": ALIAS_ID,
            "drug_id": DRUG_A,
            "status": "active",
            "name": "Synthetic Medication",
            "rxnorm_id": "123456",
            "ndc11": "00011122233",
            "last_updated": "2026-08-07T05:00:00Z",
            "tier": "Preferred",
            "prior_authorization": False,
            "step_therapy": True,
            "quantity_limit": False,
            "alternatives": {
                "resolved_drug_ids": [DRUG_B],
                "unresolved_count": 1,
            },
            "coverage": None,
        }
    ]
    rendered_response = json.dumps(response_by_field, sort_keys=True)
    for private_value in (
        "private-source",
        "medication-a",
        "private-reference",
        "evidence_json",
        ALIAS_VERSION_ID,
    ):
        assert private_value not in rendered_response


@pytest.mark.asyncio
async def test_filtered_cursor_continues_after_last_version_scoped_drug():
    _first_session, first_page = await _read_first_filtered_drug_page()

    second_session = _ScriptedSession(
        (),
        (_context_record(),),
        (_drug_record(DRUG_B, "medication-b"),),
        (),
    )
    second_page = await drugs.read_current_fhir_formulary_drug_page(
        second_session,
        FORMULARY_ID,
        ALIAS_ID,
        filters=FILTERS,
        limit=1,
        cursor=first_page.next_cursor,
        environment=ENVIRONMENT,
    )

    second_session.assert_exhausted()
    assert tuple(
        medication.drug_id for medication in second_page.items
    ) == (DRUG_B,)
    assert second_page.next_cursor is None
    assert [event for event in second_session.events if event[0] == "execute"][2][
        2
    ] == _expected_filter_params(last_id=DRUG_A)


@pytest.mark.asyncio
async def test_cursor_cannot_cross_filter_scope_and_stale_marker_conflicts():
    _first_session, first_page = await _read_first_filtered_drug_page()
    changed_filters = drug_values.FHIRFormularyDrugFilters(
        **{**FILTERS.scope_fields(), "tier": "Other"}
    )
    no_query_session = _ScriptedSession()

    with pytest.raises(serving.FHIRFormularyInvalidRequestError):
        await drugs.read_current_fhir_formulary_drug_page(
            no_query_session,
            FORMULARY_ID,
            ALIAS_ID,
            filters=changed_filters,
            limit=1,
            cursor=first_page.next_cursor,
            environment=ENVIRONMENT,
        )

    no_query_session.assert_exhausted()
    assert no_query_session.events == []

    stale_session = _ScriptedSession((), (_context_record(generation=2),))
    with pytest.raises(serving.FHIRFormularyCursorConflictError):
        await drugs.read_current_fhir_formulary_drug_page(
            stale_session,
            FORMULARY_ID,
            ALIAS_ID,
            filters=FILTERS,
            limit=1,
            cursor=first_page.next_cursor,
            environment=ENVIRONMENT,
        )

    stale_session.assert_exhausted()
    assert len([event for event in stale_session.events if event[0] == "execute"]) == 2


@pytest.mark.asyncio
async def test_alternative_owner_outside_visible_page_fails_closed():
    session = _ScriptedSession(
        (),
        (_context_record(),),
        (_drug_record(DRUG_A, "medication-a"),),
        (
            {
                "owner_drug_id": DRUG_C,
                "resolved": True,
                "target_drug_id": DRUG_B,
            },
        ),
    )

    with pytest.raises(serving.FHIRFormularyServingUnavailableError):
        await drugs.read_current_fhir_formulary_drug_page(
            session,
            FORMULARY_ID,
            ALIAS_ID,
            filters=drug_values.FHIRFormularyDrugFilters(),
            limit=1,
            environment=ENVIRONMENT,
        )

    session.assert_exhausted()


@pytest.mark.asyncio
async def test_exact_drug_read_keeps_private_ownership_out_of_payload():
    session = _ScriptedSession(
        (),
        (_context_record(),),
        (_drug_record(DRUG_A, "medication-a"),),
        (),
    )

    drug = await drugs.read_current_fhir_formulary_drug(
        session,
        FORMULARY_ID,
        ALIAS_ID,
        DRUG_A,
        environment=ENVIRONMENT,
    )

    session.assert_exhausted()
    response_by_field = drug_values.public_fhir_formulary_drug_payload(drug)
    assert set(response_by_field) == {
        "formulary_id",
        "alias_id",
        "drug_id",
        "status",
        "name",
        "rxnorm_id",
        "ndc11",
        "last_updated",
        "tier",
        "prior_authorization",
        "step_therapy",
        "quantity_limit",
        "alternatives",
        "coverage",
    }
    assert "private" not in json.dumps(response_by_field, sort_keys=True)


def _public_drug(drug_id: str):
    return drug_values.PublicFHIRFormularyDrug(
        formulary_id=FORMULARY_ID,
        alias_id=ALIAS_ID,
        drug_id=drug_id,
        status="active",
        name="Synthetic Medication",
        rxnorm_id="123456",
        ndc11="00011122233",
        last_updated=LAST_UPDATED,
        tier="Preferred",
        prior_authorization=False,
        step_therapy=True,
        quantity_limit=False,
        alternatives=drug_values.PublicFHIRFormularyAlternatives((), 0),
    )


def test_public_drug_page_rejects_oversize_duplicates_and_disorder():
    first_drug = _public_drug(DRUG_A)
    second_drug = _public_drug(DRUG_B)
    invalid_item_sets = (
        (first_drug,) * 101,
        (first_drug, first_drug),
        (second_drug, first_drug),
    )

    for invalid_items in invalid_item_sets:
        invalid_page = drug_values.PublicFHIRFormularyDrugPage(
            items=invalid_items,
            next_cursor=None,
        )
        with pytest.raises(serving.FHIRFormularyServingUnavailableError):
            drug_values.public_fhir_formulary_drug_page_payload(invalid_page)
