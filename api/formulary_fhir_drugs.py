# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Current, alias-scoped FHIR formulary medication reads."""

from __future__ import annotations

import re
from typing import Any, Mapping

from api.formulary_fhir_catalog import FHIR_FORMULARY_ALIAS_ID_PATTERN
from api.formulary_fhir_cursor import decode_fhir_formulary_cursor
from api.formulary_fhir_cursor import encode_fhir_formulary_cursor
from api.formulary_fhir_cursor import FHIR_FORMULARY_DATASET_ID_PATTERN
from api.formulary_fhir_cursor import current_fhir_formulary_marker
from api.formulary_fhir_cursor import require_fhir_formulary_cursor_configuration
from api.formulary_fhir_drug_sql import ALIAS_CONTEXT_STATEMENT
from api.formulary_fhir_drug_sql import ALTERNATIVE_STATEMENT
from api.formulary_fhir_drug_sql import drug_statement
from api.formulary_fhir_drug_values import FHIR_FORMULARY_DRUG_ID_PATTERN
from api.formulary_fhir_drug_values import FHIRFormularyDrugFilters
from api.formulary_fhir_drug_values import CurrentFHIRFormularyAliasContext
from api.formulary_fhir_drug_values import PublicFHIRFormularyAlternatives
from api.formulary_fhir_drug_values import PublicFHIRFormularyDrug
from api.formulary_fhir_drug_values import PublicFHIRFormularyDrugPage
from api.formulary_fhir_drug_values import validate_public_fhir_formulary_drug
from api.formulary_fhir_serving import FHIR_FORMULARY_PUBLIC_ID_PATTERN
from api.formulary_fhir_serving import FHIRFormularyCursorConflictError
from api.formulary_fhir_serving import FHIRFormularyInvalidRequestError
from api.formulary_fhir_serving import FHIRFormularyNotFoundError
from api.formulary_fhir_serving import FHIRFormularyServingUnavailableError
from api.formulary_fhir_serving import PublicFHIRFormularyCoverage
from api.formulary_fhir_serving import _coverage_from_record
from api.formulary_fhir_serving import _required_timestamp
from api.formulary_fhir_serving import _timestamp_text
from api.formulary_fhir_serving import _READ_TRANSACTION_SQL
from api.formulary_fhir_serving import is_fhir_formulary_serving_enabled


MAX_FHIR_FORMULARY_ALTERNATIVES_PER_DRUG = 100
_UPSTREAM_ID_PATTERN = re.compile(r"[A-Za-z0-9.-]{1,64}\Z")
_ALIAS_VERSION_ID_PATTERN = re.compile(r"ffav_[0-9a-f]{48}\Z")


def _records(result: Any) -> tuple[Mapping[str, Any], ...]:
    return tuple(result.mappings().all())


def _context_from_record(
    context_record: Mapping[str, Any],
) -> CurrentFHIRFormularyAliasContext:
    source_id = context_record.get("source_id")
    dataset_id = context_record.get("dataset_id")
    formulary_id = context_record.get("formulary_id")
    alias_id = context_record.get("alias_id")
    alias_version_id = context_record.get("alias_version_id")
    generation = context_record.get("generation")
    if (
        type(source_id) is not str
        or not source_id
        or len(source_id) > 64
        or type(dataset_id) is not str
        or FHIR_FORMULARY_DATASET_ID_PATTERN.fullmatch(dataset_id) is None
        or type(formulary_id) is not str
        or FHIR_FORMULARY_PUBLIC_ID_PATTERN.fullmatch(formulary_id) is None
        or type(alias_id) is not str
        or FHIR_FORMULARY_ALIAS_ID_PATTERN.fullmatch(alias_id) is None
        or type(alias_version_id) is not str
        or _ALIAS_VERSION_ID_PATTERN.fullmatch(alias_version_id) is None
        or type(generation) is not int
        or generation <= 0
    ):
        raise FHIRFormularyServingUnavailableError(
            "FHIR formulary alias context is invalid"
        )
    return CurrentFHIRFormularyAliasContext(
        source_id=source_id,
        dataset_id=dataset_id,
        formulary_id=formulary_id,
        alias_id=alias_id,
        alias_version_id=alias_version_id,
        generation=generation,
        published_at=_required_timestamp(context_record.get("published_at")),
        coverage=_coverage_from_record(context_record),
    )


async def _current_alias_context(
    session: Any,
    formulary_id: str,
    alias_id: str,
) -> CurrentFHIRFormularyAliasContext:
    context_records = _records(
        await session.execute(
            ALIAS_CONTEXT_STATEMENT,
            {"public_id": formulary_id, "alias_id": alias_id},
        )
    )
    if not context_records:
        raise FHIRFormularyNotFoundError("FHIR formulary drug is not available")
    if len(context_records) != 1:
        raise FHIRFormularyServingUnavailableError(
            "FHIR formulary alias context is ambiguous"
        )
    return _context_from_record(context_records[0])


def _drug_record_values(
    medication_record: Mapping[str, Any],
    coverage: PublicFHIRFormularyCoverage | None = None,
) -> tuple[str, PublicFHIRFormularyDrug]:
    upstream_id = medication_record.get("upstream_medication_id")
    if (
        type(upstream_id) is not str
        or _UPSTREAM_ID_PATTERN.fullmatch(upstream_id) is None
    ):
        raise FHIRFormularyServingUnavailableError(
            "FHIR formulary medication ownership is invalid"
        )
    return upstream_id, validate_public_fhir_formulary_drug(
        PublicFHIRFormularyDrug(
            formulary_id=medication_record.get("formulary_id"),
            alias_id=medication_record.get("alias_id"),
            drug_id=medication_record.get("drug_id"),
            status=medication_record.get("status"),
            name=medication_record.get("name"),
            rxnorm_id=medication_record.get("rxnorm_id"),
            ndc11=medication_record.get("ndc11"),
            last_updated=medication_record.get("last_updated"),
            tier=medication_record.get("tier"),
            prior_authorization=medication_record.get("prior_authorization"),
            step_therapy=medication_record.get("step_therapy"),
            quantity_limit=medication_record.get("quantity_limit"),
            alternatives=PublicFHIRFormularyAlternatives((), 0),
            coverage=coverage,
        )
    )


def _alternatives_by_drug_id(
    alternative_rows: tuple[Mapping[str, Any], ...],
    visible_drug_ids: tuple[str, ...],
) -> dict[str, PublicFHIRFormularyAlternatives]:
    resolved_by_drug_id = {drug_id: set() for drug_id in visible_drug_ids}
    unresolved_by_drug_id = {drug_id: 0 for drug_id in visible_drug_ids}
    total_by_drug_id = {drug_id: 0 for drug_id in visible_drug_ids}
    for alternative_row in alternative_rows:
        owner_drug_id = alternative_row.get("owner_drug_id")
        resolved = alternative_row.get("resolved")
        target_drug_id = alternative_row.get("target_drug_id")
        if owner_drug_id not in total_by_drug_id or type(resolved) is not bool:
            raise FHIRFormularyServingUnavailableError(
                "FHIR formulary alternative ownership is invalid"
            )
        total_by_drug_id[owner_drug_id] += 1
        if (
            total_by_drug_id[owner_drug_id]
            > MAX_FHIR_FORMULARY_ALTERNATIVES_PER_DRUG
        ):
            raise FHIRFormularyServingUnavailableError(
                "FHIR formulary alternative fanout exceeds its bound"
            )
        if resolved:
            if (
                type(target_drug_id) is not str
                or FHIR_FORMULARY_DRUG_ID_PATTERN.fullmatch(target_drug_id)
                is None
            ):
                raise FHIRFormularyServingUnavailableError(
                    "FHIR formulary resolved alternative is invalid"
                )
            resolved_by_drug_id[owner_drug_id].add(target_drug_id)
        elif target_drug_id is not None:
            raise FHIRFormularyServingUnavailableError(
                "FHIR formulary unresolved alternative is invalid"
            )
        else:
            unresolved_by_drug_id[owner_drug_id] += 1
    return {
        drug_id: PublicFHIRFormularyAlternatives(
            resolved_drug_ids=tuple(sorted(resolved_by_drug_id[drug_id])),
            unresolved_count=unresolved_by_drug_id[drug_id],
        )
        for drug_id in visible_drug_ids
    }


async def _hydrate_drugs(
    session: Any,
    formulary_id: str,
    alias_id: str,
    medication_rows: tuple[Mapping[str, Any], ...],
    *,
    coverage: PublicFHIRFormularyCoverage | None = None,
) -> tuple[PublicFHIRFormularyDrug, ...]:
    raw_values = tuple(
        _drug_record_values(medication_row, coverage)
        for medication_row in medication_rows
    )
    visible_ids = tuple(drug.drug_id for _upstream_id, drug in raw_values)
    if visible_ids != tuple(sorted(set(visible_ids))):
        raise FHIRFormularyServingUnavailableError(
            "FHIR formulary drug page contains duplicates"
        )
    alternative_records: tuple[Mapping[str, Any], ...] = ()
    if visible_ids:
        alternative_records = _records(
            await session.execute(
                ALTERNATIVE_STATEMENT,
                {
                    "public_id": formulary_id,
                    "alias_id": alias_id,
                    "owner_drug_ids": list(visible_ids),
                    "alternative_limit": len(visible_ids)
                    * MAX_FHIR_FORMULARY_ALTERNATIVES_PER_DRUG
                    + 1,
                },
            )
        )
    if len(alternative_records) > (
        len(visible_ids) * MAX_FHIR_FORMULARY_ALTERNATIVES_PER_DRUG
    ):
        raise FHIRFormularyServingUnavailableError(
            "FHIR formulary alternative fanout exceeds its bound"
        )
    alternatives_by_id = _alternatives_by_drug_id(alternative_records, visible_ids)
    return tuple(
        PublicFHIRFormularyDrug(
            formulary_id=drug.formulary_id,
            alias_id=drug.alias_id,
            drug_id=drug.drug_id,
            status=drug.status,
            name=drug.name,
            rxnorm_id=drug.rxnorm_id,
            ndc11=drug.ndc11,
            last_updated=drug.last_updated,
            tier=drug.tier,
            prior_authorization=drug.prior_authorization,
            step_therapy=drug.step_therapy,
            quantity_limit=drug.quantity_limit,
            alternatives=alternatives_by_id[drug.drug_id],
            coverage=drug.coverage,
        )
        for _upstream_id, drug in raw_values
    )


def _validate_parent_ids(formulary_id: object, alias_id: object) -> tuple[str, str]:
    if (
        type(formulary_id) is not str
        or FHIR_FORMULARY_PUBLIC_ID_PATTERN.fullmatch(formulary_id) is None
        or type(alias_id) is not str
        or FHIR_FORMULARY_ALIAS_ID_PATTERN.fullmatch(alias_id) is None
    ):
        raise FHIRFormularyNotFoundError("FHIR formulary drug is not available")
    return formulary_id, alias_id


def _drug_page_request(
    raw_cursor: object,
    *,
    public_id: str,
    public_alias_id: str,
    filters: FHIRFormularyDrugFilters,
    page_size: int,
    environment: Mapping[str, str] | None,
):
    scope_by_field = {
        "alias_id": public_alias_id,
        "formulary_id": public_id,
        "route": "drugs",
        **filters.scope_fields(),
    }
    decoded_cursor = decode_fhir_formulary_cursor(
        raw_cursor,
        kind="drugs",
        scope_by_field=scope_by_field,
        environment=environment,
    )
    last_id = "" if decoded_cursor is None else decoded_cursor.last_id
    if last_id and FHIR_FORMULARY_DRUG_ID_PATTERN.fullmatch(last_id) is None:
        raise FHIRFormularyInvalidRequestError("FHIR formulary cursor is invalid")
    parameters_by_name = {
        "public_id": public_id,
        "alias_id": public_alias_id,
        "last_id": last_id,
        "page_size": page_size + 1,
        **{
            field_name: field_value
            for field_name, field_value in filters.scope_fields().items()
            if field_value is not None
        },
    }
    return scope_by_field, decoded_cursor, parameters_by_name


async def _read_drug_page(
    session: Any,
    *,
    public_id: str,
    public_alias_id: str,
    filters: FHIRFormularyDrugFilters,
    parameters_by_name: dict[str, object],
    page_size: int,
    cursor_marker: str | None,
):
    async with session.begin():
        await session.execute(_READ_TRANSACTION_SQL)
        context = await _current_alias_context(session, public_id, public_alias_id)
        current_marker = current_fhir_formulary_marker(
            context.dataset_id,
            context.generation,
            context.published_at,
            private_identity=(context.source_id, context.alias_version_id),
        )
        if cursor_marker is not None and cursor_marker != current_marker:
            raise FHIRFormularyCursorConflictError(
                "FHIR formulary publication changed during pagination"
            )
        medication_rows = _records(
            await session.execute(drug_statement(filters), parameters_by_name)
        )
        if len(medication_rows) > page_size + 1:
            raise FHIRFormularyServingUnavailableError(
                "FHIR formulary drug page evidence exceeds its bound"
            )
        visible_drugs = await _hydrate_drugs(
            session,
            public_id,
            public_alias_id,
            medication_rows[:page_size],
            coverage=context.coverage,
        )
    return current_marker, medication_rows, visible_drugs


async def read_current_fhir_formulary_drug_page(
    session: Any,
    formulary_id: object,
    alias_id: object,
    *,
    filters: FHIRFormularyDrugFilters,
    limit: object,
    cursor: object = None,
    environment: Mapping[str, str] | None = None,
) -> PublicFHIRFormularyDrugPage:
    """Read one exact current alias page with closed policy evidence."""

    if not is_fhir_formulary_serving_enabled(environment):
        raise FHIRFormularyServingUnavailableError(
            "FHIR formulary serving is disabled"
        )
    public_id, public_alias_id = _validate_parent_ids(formulary_id, alias_id)
    if type(filters) is not FHIRFormularyDrugFilters:
        raise FHIRFormularyInvalidRequestError("FHIR formulary filters are invalid")
    if type(limit) is not int or not 1 <= limit <= 100:
        raise FHIRFormularyInvalidRequestError("FHIR formulary limit is invalid")
    require_fhir_formulary_cursor_configuration(environment)
    scope_by_field, decoded_cursor, parameters_by_name = _drug_page_request(
        cursor,
        public_id=public_id,
        public_alias_id=public_alias_id,
        filters=filters,
        page_size=limit,
        environment=environment,
    )
    current_marker, medication_rows, visible_drugs = await _read_drug_page(
        session,
        public_id=public_id,
        public_alias_id=public_alias_id,
        filters=filters,
        parameters_by_name=parameters_by_name,
        page_size=limit,
        cursor_marker=None if decoded_cursor is None else decoded_cursor.marker,
    )
    next_cursor = None
    if len(medication_rows) > limit:
        next_cursor = encode_fhir_formulary_cursor(
            kind="drugs",
            scope_by_field=scope_by_field,
            marker=current_marker,
            last_id=visible_drugs[-1].drug_id,
            environment=environment,
        )
    return PublicFHIRFormularyDrugPage(visible_drugs, next_cursor)


async def read_current_fhir_formulary_drug(
    session: Any,
    formulary_id: object,
    alias_id: object,
    drug_id: object,
    *,
    environment: Mapping[str, str] | None = None,
) -> PublicFHIRFormularyDrug:
    """Read one version-scoped drug from one exact current alias."""

    if not is_fhir_formulary_serving_enabled(environment):
        raise FHIRFormularyServingUnavailableError(
            "FHIR formulary serving is disabled"
        )
    public_id, public_alias_id = _validate_parent_ids(formulary_id, alias_id)
    if (
        type(drug_id) is not str
        or FHIR_FORMULARY_DRUG_ID_PATTERN.fullmatch(drug_id) is None
    ):
        raise FHIRFormularyNotFoundError("FHIR formulary drug is not available")
    filters = FHIRFormularyDrugFilters()
    async with session.begin():
        await session.execute(_READ_TRANSACTION_SQL)
        context = await _current_alias_context(
            session,
            public_id,
            public_alias_id,
        )
        medication_rows = _records(
            await session.execute(
                drug_statement(filters, exact_drug_id=drug_id),
                {
                    "public_id": public_id,
                    "alias_id": public_alias_id,
                    "drug_id": drug_id,
                },
            )
        )
        if not medication_rows:
            raise FHIRFormularyNotFoundError(
                "FHIR formulary drug is not available"
            )
        if len(medication_rows) != 1:
            raise FHIRFormularyServingUnavailableError(
                "FHIR formulary drug evidence is ambiguous"
            )
        hydrated_drugs = await _hydrate_drugs(
            session,
            public_id,
            public_alias_id,
            medication_rows,
            coverage=context.coverage,
        )
    return hydrated_drugs[0]


__all__ = (
    "MAX_FHIR_FORMULARY_ALTERNATIVES_PER_DRUG",
    "read_current_fhir_formulary_drug",
    "read_current_fhir_formulary_drug_page",
)
