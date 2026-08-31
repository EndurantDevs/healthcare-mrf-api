# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Current FHIR formulary and opaque alias collections."""

from __future__ import annotations

import datetime as dt
import hashlib
import re
from typing import Any, Mapping

from api.formulary_fhir_catalog_payload import FHIR_FORMULARY_ALIAS_ID_PATTERN
from api.formulary_fhir_catalog_payload import MAX_FHIR_FORMULARY_PAGE_SIZE
from api.formulary_fhir_catalog_payload import PublicFHIRFormularyAlias
from api.formulary_fhir_catalog_payload import PublicFHIRFormularyAliasPage
from api.formulary_fhir_catalog_payload import PublicFHIRFormularyPage
from api.formulary_fhir_catalog_payload import _CURSOR_PATTERN
from api.formulary_fhir_catalog_payload import _alias_from_record
from api.formulary_fhir_catalog_payload import public_fhir_formulary_alias_page_payload
from api.formulary_fhir_catalog_payload import public_fhir_formulary_page_payload
from api.formulary_fhir_catalog_sql import ALIAS_PAGE_STATEMENT
from api.formulary_fhir_catalog_sql import CATALOG_MARKER_STATEMENT
from api.formulary_fhir_catalog_sql import CURRENT_DATASET_COUNTS_STATEMENT
from api.formulary_fhir_catalog_sql import FORMULARY_CONTEXT_STATEMENT
from api.formulary_fhir_catalog_sql import FORMULARY_PAGE_STATEMENT
from api.formulary_fhir_cursor import decode_fhir_formulary_cursor
from api.formulary_fhir_cursor import encode_fhir_formulary_cursor
from api.formulary_fhir_cursor import require_fhir_formulary_cursor_configuration
from api.formulary_fhir_cursor import current_fhir_formulary_marker
from api.formulary_fhir_serving import FHIR_FORMULARY_PUBLIC_ID_PATTERN
from api.formulary_fhir_serving import FHIRFormularyCursorConflictError
from api.formulary_fhir_serving import FHIRFormularyInvalidRequestError
from api.formulary_fhir_serving import FHIRFormularyNotFoundError
from api.formulary_fhir_serving import FHIRFormularyServingUnavailableError
from api.formulary_fhir_serving import PublicFHIRFormularyCoverage
from api.formulary_fhir_serving import PublicFHIRFormularyDetail
from api.formulary_fhir_serving import _coverage_from_record
from api.formulary_fhir_serving import _detail_from_record
from api.formulary_fhir_serving import _required_timestamp
from api.formulary_fhir_serving import _timestamp_text
from api.formulary_fhir_serving import _READ_TRANSACTION_SQL
from api.formulary_fhir_serving import is_fhir_formulary_serving_enabled


def _validated_limit(limit: object) -> int:
    if type(limit) is not int or not 1 <= limit <= MAX_FHIR_FORMULARY_PAGE_SIZE:
        raise FHIRFormularyInvalidRequestError("FHIR formulary limit is invalid")
    return limit


def _records(result: Any) -> tuple[Mapping[str, Any], ...]:
    return tuple(result.mappings().all())


def _expected_dataset_counts(
    dataset_records: tuple[Mapping[str, Any], ...],
) -> dict[str, int]:
    expected_by_dataset: dict[str, int] = {}
    for dataset_record in dataset_records:
        dataset_id = dataset_record.get("dataset_id")
        list_count = dataset_record.get("list_count")
        if (
            type(dataset_id) is not str
            or not dataset_id
            or len(dataset_id) > 64
            or dataset_id in expected_by_dataset
            or type(list_count) is not int
            or list_count <= 0
        ):
            raise FHIRFormularyServingUnavailableError(
                "FHIR formulary catalog evidence is invalid"
            )
        _coverage_from_record(dataset_record)
        expected_by_dataset[dataset_id] = list_count
    return expected_by_dataset


def _catalog_marker(
    marker_records: tuple[Mapping[str, Any], ...],
    dataset_records: tuple[Mapping[str, Any], ...],
) -> str:
    digest = hashlib.sha256()
    previous_id = ""
    expected_by_dataset = _expected_dataset_counts(dataset_records)
    observed_by_dataset = {dataset_id: 0 for dataset_id in expected_by_dataset}
    for marker_record in marker_records:
        dataset_id = marker_record.get("dataset_id")
        formulary_id = marker_record.get("formulary_id")
        if (
            dataset_id not in observed_by_dataset
            or type(formulary_id) is not str
            or FHIR_FORMULARY_PUBLIC_ID_PATTERN.fullmatch(formulary_id) is None
            or formulary_id <= previous_id
        ):
            raise FHIRFormularyServingUnavailableError(
                "FHIR formulary catalog evidence is invalid"
            )
        published_at = _required_timestamp(marker_record.get("published_at"))
        digest.update(formulary_id.encode("ascii"))
        digest.update(b"\0")
        digest.update(dataset_id.encode("ascii"))
        digest.update(b"\0")
        digest.update(_timestamp_text(published_at).encode("ascii"))
        digest.update(b"\n")
        observed_by_dataset[dataset_id] += 1
        previous_id = formulary_id
    if observed_by_dataset != expected_by_dataset:
        raise FHIRFormularyServingUnavailableError(
            "FHIR formulary catalog evidence is incomplete"
        )
    return digest.hexdigest()


def _require_current_marker(
    cursor_marker: str | None,
    current_marker: str,
) -> None:
    if cursor_marker is not None and cursor_marker != current_marker:
        raise FHIRFormularyCursorConflictError(
            "FHIR formulary publication changed during pagination"
        )


def _cursor_position(
    raw_cursor: object,
    *,
    kind: str,
    scope_by_field: dict[str, object],
    id_pattern: re.Pattern[str],
    environment: Mapping[str, str] | None,
):
    decoded_cursor = decode_fhir_formulary_cursor(
        raw_cursor,
        kind=kind,
        scope_by_field=scope_by_field,
        environment=environment,
    )
    last_id = "" if decoded_cursor is None else decoded_cursor.last_id
    if last_id and id_pattern.fullmatch(last_id) is None:
        raise FHIRFormularyInvalidRequestError("FHIR formulary cursor is invalid")
    return decoded_cursor, last_id


async def _read_formulary_page_records(
    session: Any,
    *,
    last_id: str,
    page_size: int,
    cursor_marker: str | None,
):
    async with session.begin():
        await session.execute(_READ_TRANSACTION_SQL)
        marker_rows = _records(await session.execute(CATALOG_MARKER_STATEMENT))
        dataset_rows = _records(
            await session.execute(CURRENT_DATASET_COUNTS_STATEMENT)
        )
        current_marker = _catalog_marker(marker_rows, dataset_rows)
        _require_current_marker(cursor_marker, current_marker)
        page_rows = _records(
            await session.execute(
                FORMULARY_PAGE_STATEMENT,
                {"last_id": last_id, "page_size": page_size + 1},
            )
        )
    return current_marker, page_rows


def _formulary_page_from_rows(
    page_rows: tuple[Mapping[str, Any], ...],
    *,
    page_size: int,
    current_marker: str,
    scope_by_field: dict[str, object],
    environment: Mapping[str, str] | None,
) -> PublicFHIRFormularyPage:
    if len(page_rows) > page_size + 1:
        raise FHIRFormularyServingUnavailableError(
            "FHIR formulary page evidence exceeds its bound"
        )
    formulary_details = tuple(
        _detail_from_record(page_row) for page_row in page_rows[:page_size]
    )
    formulary_ids = tuple(detail.formulary_id for detail in formulary_details)
    if formulary_ids != tuple(sorted(set(formulary_ids))):
        raise FHIRFormularyServingUnavailableError(
            "FHIR formulary page evidence is invalid"
        )
    next_cursor = None
    if len(page_rows) > page_size:
        next_cursor = encode_fhir_formulary_cursor(
            kind="formularies",
            scope_by_field=scope_by_field,
            marker=current_marker,
            last_id=formulary_details[-1].formulary_id,
            environment=environment,
        )
    return PublicFHIRFormularyPage(formulary_details, next_cursor)


async def read_current_fhir_formularies(
    session: Any,
    *,
    limit: object,
    cursor: object = None,
    environment: Mapping[str, str] | None = None,
) -> PublicFHIRFormularyPage:
    """Read one page from the exact current published formulary catalog."""

    if not is_fhir_formulary_serving_enabled(environment):
        raise FHIRFormularyServingUnavailableError(
            "FHIR formulary serving is disabled"
        )
    page_size = _validated_limit(limit)
    require_fhir_formulary_cursor_configuration(environment)
    scope_by_field = {"route": "formularies"}
    decoded_cursor, last_id = _cursor_position(
        cursor,
        kind="formularies",
        scope_by_field=scope_by_field,
        id_pattern=FHIR_FORMULARY_PUBLIC_ID_PATTERN,
        environment=environment,
    )
    current_marker, page_rows = await _read_formulary_page_records(
        session,
        last_id=last_id,
        page_size=page_size,
        cursor_marker=None if decoded_cursor is None else decoded_cursor.marker,
    )
    return _formulary_page_from_rows(
        page_rows,
        page_size=page_size,
        current_marker=current_marker,
        scope_by_field=scope_by_field,
        environment=environment,
    )


async def _current_formulary_marker(
    session: Any,
    formulary_id: str,
) -> tuple[str, PublicFHIRFormularyCoverage | None]:
    context_records = _records(
        await session.execute(
            FORMULARY_CONTEXT_STATEMENT,
            {"public_id": formulary_id},
        )
    )
    if not context_records:
        raise FHIRFormularyNotFoundError("FHIR formulary is not available")
    if len(context_records) != 1:
        raise FHIRFormularyServingUnavailableError(
            "FHIR formulary current evidence is ambiguous"
        )
    context_record = context_records[0]
    return (
        current_fhir_formulary_marker(
            context_record.get("dataset_id"),
            context_record.get("generation"),
            context_record.get("published_at"),
        ),
        _coverage_from_record(context_record),
    )


async def _read_alias_page_rows(
    session: Any,
    *,
    formulary_id: str,
    last_id: str,
    page_size: int,
    cursor_marker: str | None,
):
    async with session.begin():
        await session.execute(_READ_TRANSACTION_SQL)
        current_marker, coverage = await _current_formulary_marker(
            session,
            formulary_id,
        )
        _require_current_marker(cursor_marker, current_marker)
        page_rows = _records(
            await session.execute(
                ALIAS_PAGE_STATEMENT,
                {
                    "public_id": formulary_id,
                    "last_id": last_id,
                    "page_size": page_size + 1,
                },
            )
        )
    return current_marker, coverage, page_rows


def _alias_page_from_rows(
    page_rows: tuple[Mapping[str, Any], ...],
    *,
    page_size: int,
    current_marker: str,
    scope_by_field: dict[str, object],
    environment: Mapping[str, str] | None,
    coverage: PublicFHIRFormularyCoverage | None = None,
) -> PublicFHIRFormularyAliasPage:
    if len(page_rows) > page_size + 1:
        raise FHIRFormularyServingUnavailableError(
            "FHIR formulary alias page evidence exceeds its bound"
    )
    aliases = tuple(
        _alias_from_record(page_record, coverage)
        for page_record in page_rows[:page_size]
    )
    alias_ids = tuple(alias_detail.alias_id for alias_detail in aliases)
    if alias_ids != tuple(sorted(set(alias_ids))):
        raise FHIRFormularyServingUnavailableError(
            "FHIR formulary alias page evidence is invalid"
        )
    next_cursor = None
    if len(page_rows) > page_size:
        next_cursor = encode_fhir_formulary_cursor(
            kind="aliases",
            scope_by_field=scope_by_field,
            marker=current_marker,
            last_id=aliases[-1].alias_id,
            environment=environment,
        )
    return PublicFHIRFormularyAliasPage(aliases, next_cursor)


async def read_current_fhir_formulary_aliases(
    session: Any,
    formulary_id: object,
    *,
    limit: object,
    cursor: object = None,
    environment: Mapping[str, str] | None = None,
) -> PublicFHIRFormularyAliasPage:
    """Read opaque aliases for one exact current published formulary."""

    if not is_fhir_formulary_serving_enabled(environment):
        raise FHIRFormularyServingUnavailableError(
            "FHIR formulary serving is disabled"
        )
    if (
        type(formulary_id) is not str
        or FHIR_FORMULARY_PUBLIC_ID_PATTERN.fullmatch(formulary_id) is None
    ):
        raise FHIRFormularyNotFoundError("FHIR formulary is not available")
    page_size = _validated_limit(limit)
    require_fhir_formulary_cursor_configuration(environment)
    scope_by_field = {"formulary_id": formulary_id, "route": "aliases"}
    decoded_cursor, last_id = _cursor_position(
        cursor,
        kind="aliases",
        scope_by_field=scope_by_field,
        id_pattern=FHIR_FORMULARY_ALIAS_ID_PATTERN,
        environment=environment,
    )
    current_marker, coverage, page_rows = await _read_alias_page_rows(
        session,
        formulary_id=formulary_id,
        last_id=last_id,
        page_size=page_size,
        cursor_marker=None if decoded_cursor is None else decoded_cursor.marker,
    )
    return _alias_page_from_rows(
        page_rows,
        page_size=page_size,
        current_marker=current_marker,
        scope_by_field=scope_by_field,
        environment=environment,
        coverage=coverage,
    )


__all__ = (
    "FHIR_FORMULARY_ALIAS_ID_PATTERN",
    "MAX_FHIR_FORMULARY_PAGE_SIZE",
    "PublicFHIRFormularyAlias",
    "PublicFHIRFormularyAliasPage",
    "PublicFHIRFormularyPage",
    "public_fhir_formulary_alias_page_payload",
    "public_fhir_formulary_page_payload",
    "read_current_fhir_formularies",
    "read_current_fhir_formulary_aliases",
)
