# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed read-only access to one current published FHIR formulary."""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
import os
import re
from typing import Any, Mapping

from sqlalchemy import bindparam, select, text

from api.formulary_fhir_catalog_sql import CURRENT_PLAN_FROM
from api.formulary_fhir_catalog_sql import CURRENT_PLAN_PREDICATES
from api.formulary_fhir_catalog_sql import DETAIL_COLUMNS
from api.formulary_fhir_catalog_sql import plan


FHIR_FORMULARY_SERVING_ENABLED_ENV = "HLTHPRT_FHIR_FORMULARY_SERVING_ENABLED"
FHIR_FORMULARY_CACHE_CONTROL = "private, no-store"
FHIR_FORMULARY_PUBLIC_ID_PATTERN = re.compile(r"^fhir_[a-z2-7]{26}$")
_TRUE_ENV_VALUES = frozenset({"1", "true", "yes", "on"})
_READ_TRANSACTION_SQL = text(
    "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"
)


class FHIRFormularyNotFoundError(RuntimeError):
    """Collapse malformed, unknown, and non-current plan identities."""


class FHIRFormularyInvalidRequestError(RuntimeError):
    """Reject malformed public selectors without exposing stored identity."""


class FHIRFormularyCursorConflictError(RuntimeError):
    """Require pagination restart after the current publication changes."""


class FHIRFormularyServingUnavailableError(RuntimeError):
    """Fail closed when serving is dormant or stored evidence is invalid."""


@dataclass(frozen=True, slots=True)
class PublicFHIRFormularyCoverage:
    """Aggregate official-artifact coverage without source identity."""

    status: str
    expected_artifact_count: int
    included_artifact_count: int
    missing_artifact_count: int


@dataclass(frozen=True, slots=True)
class PublicFHIRFormularyDetail:
    """Allowlisted current-plan fields without source or generation identity."""

    formulary_id: str
    status: str | None
    title: str | None
    name: str | None
    period_start: dt.datetime | None
    period_end: dt.datetime | None
    last_updated: dt.datetime
    as_of: dt.datetime
    published_at: dt.datetime
    coverage: PublicFHIRFormularyCoverage | None = None


_DETAIL_STATEMENT = (
    select(*DETAIL_COLUMNS)
    .select_from(CURRENT_PLAN_FROM)
    .where(
        *CURRENT_PLAN_PREDICATES,
        plan.c.public_id == bindparam("public_id"),
    )
    .limit(2)
)


def is_fhir_formulary_serving_enabled(
    environment: Mapping[str, str] | None = None,
) -> bool:
    """Return true only for one explicitly enabled process environment."""

    environment_values = os.environ if environment is None else environment
    raw_setting = environment_values.get(FHIR_FORMULARY_SERVING_ENABLED_ENV, "")
    return raw_setting.strip().lower() in _TRUE_ENV_VALUES


def _required_timestamp(value: object) -> dt.datetime:
    if type(value) is not dt.datetime or value.tzinfo is None:
        raise FHIRFormularyServingUnavailableError(
            "FHIR formulary timestamp evidence is invalid"
        )
    return value


def _optional_timestamp(value: object) -> dt.datetime | None:
    if value is None:
        return None
    return _required_timestamp(value)


def _optional_text(value: object, maximum_length: int) -> str | None:
    if value is None:
        return None
    if (
        type(value) is not str
        or not value
        or len(value) > maximum_length
        or value != value.strip()
        or any(not character.isprintable() for character in value)
    ):
        raise FHIRFormularyServingUnavailableError(
            "FHIR formulary text evidence is invalid"
        )
    return value


def validate_public_fhir_formulary_coverage(
    coverage: object,
) -> PublicFHIRFormularyCoverage | None:
    """Require one arithmetically complete aggregate, or not-applicable null."""

    if coverage is None:
        return None
    if type(coverage) is not PublicFHIRFormularyCoverage:
        raise FHIRFormularyServingUnavailableError(
            "FHIR formulary coverage evidence is invalid"
        )
    expected = coverage.expected_artifact_count
    included = coverage.included_artifact_count
    missing = coverage.missing_artifact_count
    expected_status = "complete" if missing == 0 else "partial"
    if (
        type(expected) is not int
        or type(included) is not int
        or type(missing) is not int
        or expected <= 0
        or not 1 <= included <= expected
        or missing != expected - included
        or coverage.status != expected_status
    ):
        raise FHIRFormularyServingUnavailableError(
            "FHIR formulary coverage evidence is invalid"
        )
    return coverage


def _coverage_from_record(
    coverage_record: Mapping[str, Any],
) -> PublicFHIRFormularyCoverage | None:
    coverage_required = coverage_record.get("coverage_required")
    expected = coverage_record.get("coverage_expected_artifact_count")
    receipt_expected = coverage_record.get(
        "coverage_receipt_expected_artifact_count"
    )
    included = coverage_record.get("coverage_included_artifact_count")
    missing = coverage_record.get("coverage_missing_artifact_count")
    if type(coverage_required) is not bool:
        raise FHIRFormularyServingUnavailableError(
            "FHIR formulary coverage evidence is invalid"
        )
    if not coverage_required:
        if any(
            coverage_field is not None
            for coverage_field in (expected, receipt_expected, included, missing)
        ):
            raise FHIRFormularyServingUnavailableError(
                "FHIR formulary coverage evidence is invalid"
            )
        return None
    if (
        type(expected) is not int
        or type(receipt_expected) is not int
        or expected != receipt_expected
    ):
        raise FHIRFormularyServingUnavailableError(
            "FHIR formulary coverage evidence is invalid"
        )
    return validate_public_fhir_formulary_coverage(
        PublicFHIRFormularyCoverage(
            status="complete" if missing == 0 else "partial",
            expected_artifact_count=expected,
            included_artifact_count=included,
            missing_artifact_count=missing,
        )
    )


def public_fhir_formulary_coverage_payload(
    coverage: PublicFHIRFormularyCoverage | None,
) -> dict[str, object] | None:
    """Serialize one source-hidden official-artifact aggregate."""

    validated_coverage = validate_public_fhir_formulary_coverage(coverage)
    if validated_coverage is None:
        return None
    return {
        "status": validated_coverage.status,
        "expected_artifact_count": (
            validated_coverage.expected_artifact_count
        ),
        "included_artifact_count": (
            validated_coverage.included_artifact_count
        ),
        "missing_artifact_count": validated_coverage.missing_artifact_count,
    }


def _detail_from_record(record: Mapping[str, Any]) -> PublicFHIRFormularyDetail:
    formulary_id = record.get("formulary_id")
    if (
        type(formulary_id) is not str
        or FHIR_FORMULARY_PUBLIC_ID_PATTERN.fullmatch(formulary_id) is None
    ):
        raise FHIRFormularyServingUnavailableError(
            "FHIR formulary identity evidence is invalid"
        )
    return PublicFHIRFormularyDetail(
        formulary_id=formulary_id,
        status=_optional_text(record.get("status"), 32),
        title=_optional_text(record.get("title"), 2_048),
        name=_optional_text(record.get("name"), 2_048),
        period_start=_optional_timestamp(record.get("period_start")),
        period_end=_optional_timestamp(record.get("period_end")),
        last_updated=_required_timestamp(record.get("last_updated")),
        as_of=_required_timestamp(record.get("as_of")),
        published_at=_required_timestamp(record.get("published_at")),
        coverage=(
            validate_public_fhir_formulary_coverage(record.get("coverage"))
            if "coverage" in record
            else _coverage_from_record(record)
        ),
    )


async def read_current_fhir_formulary(
    session: Any,
    formulary_id: object,
    *,
    environment: Mapping[str, str] | None = None,
) -> PublicFHIRFormularyDetail:
    """Read one exact current published plan in a read-only snapshot."""

    if not is_fhir_formulary_serving_enabled(environment):
        raise FHIRFormularyServingUnavailableError(
            "FHIR formulary serving is disabled"
        )
    if (
        type(formulary_id) is not str
        or FHIR_FORMULARY_PUBLIC_ID_PATTERN.fullmatch(formulary_id) is None
    ):
        raise FHIRFormularyNotFoundError("FHIR formulary is not available")
    async with session.begin():
        await session.execute(_READ_TRANSACTION_SQL)
        record_result = await session.execute(
            _DETAIL_STATEMENT,
            {"public_id": formulary_id},
        )
        detail_records = tuple(record_result.mappings().all())
    if not detail_records:
        raise FHIRFormularyNotFoundError("FHIR formulary is not available")
    if len(detail_records) != 1:
        raise FHIRFormularyServingUnavailableError(
            "FHIR formulary current evidence is ambiguous"
        )
    return _detail_from_record(detail_records[0])


def _timestamp_text(value: dt.datetime) -> str:
    return value.astimezone(dt.UTC).isoformat().replace("+00:00", "Z")


def public_fhir_formulary_payload(
    detail: PublicFHIRFormularyDetail,
) -> dict[str, object]:
    """Shape the fixed public response without internal ownership fields."""

    if type(detail) is not PublicFHIRFormularyDetail:
        raise FHIRFormularyServingUnavailableError(
            "FHIR formulary response evidence is invalid"
        )
    validated_detail = _detail_from_record(
        {
            "formulary_id": detail.formulary_id,
            "status": detail.status,
            "title": detail.title,
            "name": detail.name,
            "period_start": detail.period_start,
            "period_end": detail.period_end,
            "last_updated": detail.last_updated,
            "as_of": detail.as_of,
            "published_at": detail.published_at,
            "coverage": detail.coverage,
        }
    )
    period_by_field = None
    if (
        validated_detail.period_start is not None
        or validated_detail.period_end is not None
    ):
        period_by_field = {
            "start": (
                _timestamp_text(validated_detail.period_start)
                if validated_detail.period_start is not None
                else None
            ),
            "end": (
                _timestamp_text(validated_detail.period_end)
                if validated_detail.period_end is not None
                else None
            ),
        }
    return {
        "formulary_id": validated_detail.formulary_id,
        "status": validated_detail.status,
        "title": validated_detail.title,
        "name": validated_detail.name,
        "period": period_by_field,
        "last_updated": _timestamp_text(validated_detail.last_updated),
        "as_of": _timestamp_text(validated_detail.as_of),
        "published_at": _timestamp_text(validated_detail.published_at),
        "coverage": public_fhir_formulary_coverage_payload(
            validated_detail.coverage
        ),
    }


__all__ = (
    "FHIR_FORMULARY_CACHE_CONTROL",
    "FHIR_FORMULARY_PUBLIC_ID_PATTERN",
    "FHIR_FORMULARY_SERVING_ENABLED_ENV",
    "FHIRFormularyCursorConflictError",
    "FHIRFormularyInvalidRequestError",
    "FHIRFormularyNotFoundError",
    "FHIRFormularyServingUnavailableError",
    "PublicFHIRFormularyCoverage",
    "PublicFHIRFormularyDetail",
    "is_fhir_formulary_serving_enabled",
    "public_fhir_formulary_coverage_payload",
    "public_fhir_formulary_payload",
    "read_current_fhir_formulary",
    "validate_public_fhir_formulary_coverage",
)
