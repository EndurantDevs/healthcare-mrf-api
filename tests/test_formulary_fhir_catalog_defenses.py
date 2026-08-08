# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fail-closed evidence checks for current FHIR formulary collections."""

from __future__ import annotations

import base64
import datetime as dt

import pytest

from api import formulary_fhir_catalog as catalog
from api import formulary_fhir_cursor as cursor
from api import formulary_fhir_serving as serving


FORMULARY_A = "fhir_at4rcuzsyttz7txu3xtoxsa734"
FORMULARY_B = "fhir_bt4rcuzsyttz7txu3xtoxsa734"
ALIAS_A = "ffa_" + "1" * 48
ALIAS_B = "ffa_" + "2" * 48
DATASET_ID = "ffd_" + "a" * 48
PUBLISHED_AT = dt.datetime(2026, 8, 8, 6, tzinfo=dt.UTC)
CURSOR_KEY = base64.urlsafe_b64encode(b"c" * 32).decode("ascii").rstrip("=")
ENABLED_ENVIRONMENT = {
    serving.FHIR_FORMULARY_SERVING_ENABLED_ENV: "true",
    cursor.FHIR_FORMULARY_CURSOR_KEY_ENV: CURSOR_KEY,
}


def _detail_record(formulary_id: str):
    return {
        "formulary_id": formulary_id,
        "status": "current",
        "title": "Synthetic Coverage Plan",
        "name": "Synthetic Formulary",
        "period_start": None,
        "period_end": None,
        "last_updated": PUBLISHED_AT,
        "as_of": PUBLISHED_AT,
        "published_at": PUBLISHED_AT,
    }


def _detail(formulary_id: str):
    return serving._detail_from_record(_detail_record(formulary_id))


def _alias(alias_id: str):
    return catalog.PublicFHIRFormularyAlias(FORMULARY_A, alias_id, 1)


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


@pytest.mark.parametrize("limit", (None, True, 0, 101))
def test_collection_limit_requires_an_exact_bounded_integer(limit):
    with pytest.raises(serving.FHIRFormularyInvalidRequestError):
        catalog._validated_limit(limit)


@pytest.mark.parametrize(
    "dataset_records",
    (
        ({"dataset_id": None, "list_count": 1},),
        ({"dataset_id": "", "list_count": 1},),
        ({"dataset_id": "d" * 65, "list_count": 1},),
        ({"dataset_id": "dataset-a", "list_count": True},),
        ({"dataset_id": "dataset-a", "list_count": 0},),
        (
            {"dataset_id": "dataset-a", "list_count": 1},
            {"dataset_id": "dataset-a", "list_count": 1},
        ),
    ),
)
def test_dataset_census_rejects_invalid_and_duplicate_rows(dataset_records):
    with pytest.raises(serving.FHIRFormularyServingUnavailableError):
        catalog._expected_dataset_counts(dataset_records)


@pytest.mark.parametrize(
    "marker_records,dataset_records",
    (
        (
            ({"dataset_id": "dataset-b", "formulary_id": FORMULARY_A},),
            ({"dataset_id": "dataset-a", "list_count": 1},),
        ),
        (
            ({"dataset_id": "dataset-a", "formulary_id": None},),
            ({"dataset_id": "dataset-a", "list_count": 1},),
        ),
        (
            ({"dataset_id": "dataset-a", "formulary_id": "invalid"},),
            ({"dataset_id": "dataset-a", "list_count": 1},),
        ),
        (
            (
                {
                    "dataset_id": "dataset-a",
                    "formulary_id": FORMULARY_B,
                    "published_at": PUBLISHED_AT,
                },
                {
                    "dataset_id": "dataset-a",
                    "formulary_id": FORMULARY_A,
                    "published_at": PUBLISHED_AT,
                },
            ),
            ({"dataset_id": "dataset-a", "list_count": 2},),
        ),
    ),
)
def test_catalog_marker_rejects_wrong_ownership_identity_and_order(
    marker_records,
    dataset_records,
):
    with pytest.raises(serving.FHIRFormularyServingUnavailableError):
        catalog._catalog_marker(marker_records, dataset_records)


def test_catalog_cursor_last_id_must_match_its_collection_kind():
    scope_by_field = {"route": "formularies"}
    raw_cursor = cursor.encode_fhir_formulary_cursor(
        kind="formularies",
        scope_by_field=scope_by_field,
        marker="marker",
        last_id="wrong-kind-id",
        environment=ENABLED_ENVIRONMENT,
    )

    with pytest.raises(serving.FHIRFormularyInvalidRequestError):
        catalog._cursor_position(
            raw_cursor,
            kind="formularies",
            scope_by_field=scope_by_field,
            id_pattern=serving.FHIR_FORMULARY_PUBLIC_ID_PATTERN,
            environment=ENABLED_ENVIRONMENT,
        )


@pytest.mark.parametrize(
    "page_rows,page_size",
    (
        (({}, {}, {}), 1),
        ((_detail_record(FORMULARY_A), _detail_record(FORMULARY_A)), 2),
        ((_detail_record(FORMULARY_B), _detail_record(FORMULARY_A)), 2),
    ),
)
def test_formulary_page_rows_reject_excess_duplicates_and_disorder(
    page_rows,
    page_size,
):
    with pytest.raises(serving.FHIRFormularyServingUnavailableError):
        catalog._formulary_page_from_rows(
            page_rows,
            page_size=page_size,
            current_marker="marker",
            scope_by_field={"route": "formularies"},
            environment=ENABLED_ENVIRONMENT,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("record_sets", (((),), (({}, {}),)))
async def test_current_formulary_marker_requires_one_row(record_sets):
    session = _Session(*record_sets)

    with pytest.raises(
        (
            serving.FHIRFormularyNotFoundError,
            serving.FHIRFormularyServingUnavailableError,
        )
    ):
        await catalog._current_formulary_marker(session, FORMULARY_A)

    session.assert_exhausted()


@pytest.mark.parametrize(
    "context_record",
    (
        {"dataset_id": None, "generation": 1, "published_at": PUBLISHED_AT},
        {"dataset_id": "invalid", "generation": 1, "published_at": PUBLISHED_AT},
        {"dataset_id": DATASET_ID, "generation": True, "published_at": PUBLISHED_AT},
        {"dataset_id": DATASET_ID, "generation": 0, "published_at": PUBLISHED_AT},
        {"dataset_id": DATASET_ID, "generation": 1, "published_at": None},
    ),
)
def test_current_formulary_marker_requires_immutable_publication_identity(
    context_record,
):
    with pytest.raises(serving.FHIRFormularyServingUnavailableError):
        cursor.current_fhir_formulary_marker(
            context_record.get("dataset_id"),
            context_record.get("generation"),
            context_record.get("published_at"),
        )


@pytest.mark.parametrize(
    "record",
    (
        {"formulary_id": None, "alias_id": ALIAS_A, "drug_count": 1},
        {"formulary_id": "invalid", "alias_id": ALIAS_A, "drug_count": 1},
        {"formulary_id": FORMULARY_A, "alias_id": None, "drug_count": 1},
        {"formulary_id": FORMULARY_A, "alias_id": "invalid", "drug_count": 1},
        {"formulary_id": FORMULARY_A, "alias_id": ALIAS_A, "drug_count": True},
        {"formulary_id": FORMULARY_A, "alias_id": ALIAS_A, "drug_count": -1},
    ),
)
def test_alias_rows_require_closed_public_identity_and_count(record):
    with pytest.raises(serving.FHIRFormularyServingUnavailableError):
        catalog._alias_from_record(record)


@pytest.mark.parametrize(
    "page_rows,page_size",
    (
        (({}, {}, {}), 1),
        (
            (
                {"formulary_id": FORMULARY_A, "alias_id": ALIAS_A, "drug_count": 1},
                {"formulary_id": FORMULARY_A, "alias_id": ALIAS_A, "drug_count": 1},
            ),
            2,
        ),
        (
            (
                {"formulary_id": FORMULARY_A, "alias_id": ALIAS_B, "drug_count": 1},
                {"formulary_id": FORMULARY_A, "alias_id": ALIAS_A, "drug_count": 1},
            ),
            2,
        ),
    ),
)
def test_alias_page_rows_reject_excess_duplicates_and_disorder(
    page_rows,
    page_size,
):
    with pytest.raises(serving.FHIRFormularyServingUnavailableError):
        catalog._alias_page_from_rows(
            page_rows,
            page_size=page_size,
            current_marker="marker",
            scope_by_field={"formulary_id": FORMULARY_A, "route": "aliases"},
            environment=ENABLED_ENVIRONMENT,
        )


def test_alias_page_without_an_extra_row_has_no_cursor():
    page = catalog._alias_page_from_rows(
        (
            {
                "formulary_id": FORMULARY_A,
                "alias_id": ALIAS_A,
                "drug_count": 1,
            },
        ),
        page_size=1,
        current_marker="marker",
        scope_by_field={"formulary_id": FORMULARY_A, "route": "aliases"},
        environment=ENABLED_ENVIRONMENT,
    )

    assert page.next_cursor is None


@pytest.mark.asyncio
async def test_collection_readers_fail_before_data_access_when_disabled():
    session = _Session()
    disabled_environment_by_name = {
        cursor.FHIR_FORMULARY_CURSOR_KEY_ENV: CURSOR_KEY,
    }

    with pytest.raises(serving.FHIRFormularyServingUnavailableError):
        await catalog.read_current_fhir_formularies(
            session,
            limit=1,
            environment=disabled_environment_by_name,
        )
    with pytest.raises(serving.FHIRFormularyServingUnavailableError):
        await catalog.read_current_fhir_formulary_aliases(
            session,
            FORMULARY_A,
            limit=1,
            environment=disabled_environment_by_name,
        )
    session.assert_exhausted()


@pytest.mark.asyncio
@pytest.mark.parametrize("formulary_id", (None, "invalid"))
async def test_alias_reader_hides_invalid_parent_identity(formulary_id):
    session = _Session()

    with pytest.raises(serving.FHIRFormularyNotFoundError):
        await catalog.read_current_fhir_formulary_aliases(
            session,
            formulary_id,
            limit=1,
            environment=ENABLED_ENVIRONMENT,
        )
    session.assert_exhausted()


@pytest.mark.parametrize(
    "page",
    (
        None,
        catalog.PublicFHIRFormularyPage([], None),
        catalog.PublicFHIRFormularyPage((), 1),
        catalog.PublicFHIRFormularyPage((), "invalid.cursor"),
    ),
)
def test_public_formulary_page_rejects_invalid_container_and_cursor(page):
    with pytest.raises(serving.FHIRFormularyServingUnavailableError):
        catalog.public_fhir_formulary_page_payload(page)


@pytest.mark.parametrize(
    "items",
    (
        (_detail(FORMULARY_A), _detail(FORMULARY_A)),
        (_detail(FORMULARY_B), _detail(FORMULARY_A)),
    ),
)
def test_public_formulary_page_rejects_duplicates_and_disorder(items):
    with pytest.raises(serving.FHIRFormularyServingUnavailableError):
        catalog.public_fhir_formulary_page_payload(
            catalog.PublicFHIRFormularyPage(items, None)
        )


@pytest.mark.parametrize(
    "page",
    (
        None,
        catalog.PublicFHIRFormularyAliasPage([], None),
        catalog.PublicFHIRFormularyAliasPage((), 1),
        catalog.PublicFHIRFormularyAliasPage((), "invalid.cursor"),
        catalog.PublicFHIRFormularyAliasPage((object(),), None),
        catalog.PublicFHIRFormularyAliasPage((_alias(ALIAS_A), _alias(ALIAS_A)), None),
        catalog.PublicFHIRFormularyAliasPage((_alias(ALIAS_B), _alias(ALIAS_A)), None),
    ),
)
def test_public_alias_page_rejects_invalid_container_items_and_order(page):
    with pytest.raises(serving.FHIRFormularyServingUnavailableError):
        catalog.public_fhir_formulary_alias_page_payload(page)
