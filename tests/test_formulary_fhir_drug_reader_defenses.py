# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fail-closed database evidence checks for alias-scoped formulary drugs."""

from __future__ import annotations

import base64
import datetime as dt

import pytest

from api import formulary_fhir_cursor as cursor
from api import formulary_fhir_drug_values as drug_values
from api import formulary_fhir_drugs as drugs
from api import formulary_fhir_serving as serving


FORMULARY_ID = "fhir_at4rcuzsyttz7txu3xtoxsa734"
ALIAS_ID = "ffa_" + "1" * 48
ALIAS_VERSION_ID = "ffav_" + "a" * 48
DATASET_ID = "ffd_" + "a" * 48
DRUG_A = "ffm_" + "1" * 48
DRUG_B = "ffm_" + "2" * 48
PUBLISHED_AT = dt.datetime(2026, 8, 8, 6, tzinfo=dt.UTC)
CURSOR_KEY = base64.urlsafe_b64encode(b"d" * 32).decode("ascii").rstrip("=")
ENABLED_ENVIRONMENT = {
    serving.FHIR_FORMULARY_SERVING_ENABLED_ENV: "true",
    cursor.FHIR_FORMULARY_CURSOR_KEY_ENV: CURSOR_KEY,
}
EMPTY_FILTERS = drug_values.FHIRFormularyDrugFilters()


def _context_record(**changes):
    context_by_field = {
        "source_id": "synthetic-source",
        "dataset_id": DATASET_ID,
        "formulary_id": FORMULARY_ID,
        "alias_id": ALIAS_ID,
        "alias_version_id": ALIAS_VERSION_ID,
        "generation": 1,
        "published_at": PUBLISHED_AT,
    }
    context_by_field.update(changes)
    return context_by_field


def _drug_record(drug_id: str = DRUG_A, **changes):
    medication_by_field = {
        "formulary_id": FORMULARY_ID,
        "alias_id": ALIAS_ID,
        "upstream_medication_id": "synthetic-medication",
        "drug_id": drug_id,
        "status": "active",
        "name": "Synthetic Medication",
        "rxnorm_id": "123456",
        "ndc11": "00011122233",
        "last_updated": PUBLISHED_AT,
        "tier": "Preferred",
        "prior_authorization": False,
        "step_therapy": True,
        "quantity_limit": False,
    }
    medication_by_field.update(changes)
    return medication_by_field


class _MappingResult:
    def __init__(self, records):
        self._records = tuple(records)

    def mappings(self):
        return self

    def all(self):
        return self._records


class _Transaction:
    async def __aenter__(self):
        return self

    async def __aexit__(self, _exc_type, _exc, _traceback):
        return False


class _Session:
    def __init__(self, *record_sets):
        self._record_sets = list(record_sets)

    def begin(self):
        return _Transaction()

    async def execute(self, _statement, _parameters=None):
        assert self._record_sets, "unexpected database query"
        return _MappingResult(self._record_sets.pop(0))

    def assert_exhausted(self):
        assert self._record_sets == []


@pytest.mark.parametrize(
    "context_changes",
    (
        {"source_id": None},
        {"source_id": ""},
        {"source_id": "s" * 65},
        {"dataset_id": None},
        {"dataset_id": "invalid"},
        {"formulary_id": None},
        {"formulary_id": "invalid"},
        {"alias_id": None},
        {"alias_id": "invalid"},
        {"alias_version_id": None},
        {"alias_version_id": "invalid"},
        {"generation": True},
        {"generation": 0},
    ),
)
def test_alias_context_rejects_invalid_private_and_public_identity(context_changes):
    with pytest.raises(serving.FHIRFormularyServingUnavailableError):
        drugs._context_from_record(_context_record(**context_changes))


@pytest.mark.asyncio
@pytest.mark.parametrize("context_records", ((), (_context_record(), _context_record())))
async def test_current_alias_context_requires_one_row(context_records):
    session = _Session(context_records)

    with pytest.raises(
        (
            serving.FHIRFormularyNotFoundError,
            serving.FHIRFormularyServingUnavailableError,
        )
    ):
        await drugs._current_alias_context(session, FORMULARY_ID, ALIAS_ID)

    session.assert_exhausted()


@pytest.mark.parametrize(
    "upstream_medication_id",
    (None, "", "invalid/id", "m" * 65),
)
def test_drug_record_rejects_invalid_upstream_ownership(upstream_medication_id):
    with pytest.raises(serving.FHIRFormularyServingUnavailableError):
        drugs._drug_record_values(
            _drug_record(upstream_medication_id=upstream_medication_id)
        )


@pytest.mark.parametrize(
    "alternative_rows",
    (
        (
            {
                "owner_drug_id": DRUG_B,
                "resolved": False,
                "target_drug_id": None,
            },
        ),
        (
            {
                "owner_drug_id": DRUG_A,
                "resolved": 1,
                "target_drug_id": None,
            },
        ),
        (
            {
                "owner_drug_id": DRUG_A,
                "resolved": True,
                "target_drug_id": None,
            },
        ),
        (
            {
                "owner_drug_id": DRUG_A,
                "resolved": True,
                "target_drug_id": "invalid",
            },
        ),
        (
            {
                "owner_drug_id": DRUG_A,
                "resolved": False,
                "target_drug_id": DRUG_B,
            },
        ),
    ),
)
def test_alternatives_reject_wrong_owner_state_and_target(alternative_rows):
    with pytest.raises(serving.FHIRFormularyServingUnavailableError):
        drugs._alternatives_by_drug_id(alternative_rows, (DRUG_A,))


def test_alternatives_reject_per_drug_fanout_above_bound():
    alternative_rows = tuple(
        {
            "owner_drug_id": DRUG_A,
            "resolved": False,
            "target_drug_id": None,
        }
        for _index in range(101)
    )

    with pytest.raises(serving.FHIRFormularyServingUnavailableError):
        drugs._alternatives_by_drug_id(alternative_rows, (DRUG_A,))


@pytest.mark.asyncio
async def test_hydration_skips_alternative_query_for_empty_page():
    session = _Session()

    assert await drugs._hydrate_drugs(
        session,
        FORMULARY_ID,
        ALIAS_ID,
        (),
    ) == ()
    session.assert_exhausted()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "medication_rows",
    (
        (_drug_record(), _drug_record()),
        (_drug_record(DRUG_B), _drug_record(DRUG_A)),
    ),
)
async def test_hydration_rejects_duplicate_and_disordered_drug_ids(medication_rows):
    session = _Session()

    with pytest.raises(serving.FHIRFormularyServingUnavailableError):
        await drugs._hydrate_drugs(
            session,
            FORMULARY_ID,
            ALIAS_ID,
            medication_rows,
        )
    session.assert_exhausted()


@pytest.mark.asyncio
async def test_hydration_rejects_total_alternative_fanout_above_bound():
    alternative_rows = tuple(
        {
            "owner_drug_id": DRUG_A,
            "resolved": False,
            "target_drug_id": None,
        }
        for _index in range(101)
    )
    session = _Session(alternative_rows)

    with pytest.raises(serving.FHIRFormularyServingUnavailableError):
        await drugs._hydrate_drugs(
            session,
            FORMULARY_ID,
            ALIAS_ID,
            (_drug_record(),),
        )
    session.assert_exhausted()


@pytest.mark.parametrize(
    "formulary_id,alias_id",
    (
        (None, ALIAS_ID),
        ("invalid", ALIAS_ID),
        (FORMULARY_ID, None),
        (FORMULARY_ID, "invalid"),
    ),
)
def test_parent_identity_validation_hides_invalid_ids(formulary_id, alias_id):
    with pytest.raises(serving.FHIRFormularyNotFoundError):
        drugs._validate_parent_ids(formulary_id, alias_id)


def test_drug_cursor_last_id_must_match_drug_identity():
    scope_by_field = {
        "alias_id": ALIAS_ID,
        "formulary_id": FORMULARY_ID,
        "route": "drugs",
        **EMPTY_FILTERS.scope_fields(),
    }
    raw_cursor = cursor.encode_fhir_formulary_cursor(
        kind="drugs",
        scope_by_field=scope_by_field,
        marker="marker",
        last_id="wrong-kind-id",
        environment=ENABLED_ENVIRONMENT,
    )

    with pytest.raises(serving.FHIRFormularyInvalidRequestError):
        drugs._drug_page_request(
            raw_cursor,
            public_id=FORMULARY_ID,
            public_alias_id=ALIAS_ID,
            filters=EMPTY_FILTERS,
            page_size=1,
            environment=ENABLED_ENVIRONMENT,
        )


@pytest.mark.asyncio
async def test_drug_readers_fail_before_data_access_when_disabled():
    session = _Session()
    disabled_environment_by_name = {
        cursor.FHIR_FORMULARY_CURSOR_KEY_ENV: CURSOR_KEY,
    }

    with pytest.raises(serving.FHIRFormularyServingUnavailableError):
        await drugs.read_current_fhir_formulary_drug_page(
            session,
            FORMULARY_ID,
            ALIAS_ID,
            filters=EMPTY_FILTERS,
            limit=1,
            environment=disabled_environment_by_name,
        )
    with pytest.raises(serving.FHIRFormularyServingUnavailableError):
        await drugs.read_current_fhir_formulary_drug(
            session,
            FORMULARY_ID,
            ALIAS_ID,
            DRUG_A,
            environment=disabled_environment_by_name,
        )
    session.assert_exhausted()


@pytest.mark.asyncio
@pytest.mark.parametrize("filters", (None, object()))
async def test_drug_page_rejects_invalid_filter_container(filters):
    session = _Session()

    with pytest.raises(serving.FHIRFormularyInvalidRequestError):
        await drugs.read_current_fhir_formulary_drug_page(
            session,
            FORMULARY_ID,
            ALIAS_ID,
            filters=filters,
            limit=1,
            environment=ENABLED_ENVIRONMENT,
        )
    session.assert_exhausted()


@pytest.mark.asyncio
@pytest.mark.parametrize("limit", (None, True, 0, 101))
async def test_drug_page_rejects_invalid_limit(limit):
    session = _Session()

    with pytest.raises(serving.FHIRFormularyInvalidRequestError):
        await drugs.read_current_fhir_formulary_drug_page(
            session,
            FORMULARY_ID,
            ALIAS_ID,
            filters=EMPTY_FILTERS,
            limit=limit,
            environment=ENABLED_ENVIRONMENT,
        )
    session.assert_exhausted()


@pytest.mark.asyncio
async def test_drug_page_rejects_more_than_one_lookahead_row():
    session = _Session(
        (),
        (_context_record(),),
        (_drug_record(), _drug_record(DRUG_B), _drug_record("ffm_" + "3" * 48)),
    )

    with pytest.raises(serving.FHIRFormularyServingUnavailableError):
        await drugs.read_current_fhir_formulary_drug_page(
            session,
            FORMULARY_ID,
            ALIAS_ID,
            filters=EMPTY_FILTERS,
            limit=1,
            environment=ENABLED_ENVIRONMENT,
        )
    session.assert_exhausted()


@pytest.mark.asyncio
@pytest.mark.parametrize("drug_id", (None, "invalid"))
async def test_exact_drug_reader_hides_invalid_drug_identity(drug_id):
    session = _Session()

    with pytest.raises(serving.FHIRFormularyNotFoundError):
        await drugs.read_current_fhir_formulary_drug(
            session,
            FORMULARY_ID,
            ALIAS_ID,
            drug_id,
            environment=ENABLED_ENVIRONMENT,
        )
    session.assert_exhausted()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "medication_rows,expected_error",
    (
        ((), serving.FHIRFormularyNotFoundError),
        (
            (_drug_record(), _drug_record()),
            serving.FHIRFormularyServingUnavailableError,
        ),
    ),
)
async def test_exact_drug_reader_requires_one_medication_row(
    medication_rows,
    expected_error,
):
    session = _Session((), (_context_record(),), medication_rows)

    with pytest.raises(expected_error):
        await drugs.read_current_fhir_formulary_drug(
            session,
            FORMULARY_ID,
            ALIAS_ID,
            DRUG_A,
            environment=ENABLED_ENVIRONMENT,
        )
    session.assert_exhausted()
