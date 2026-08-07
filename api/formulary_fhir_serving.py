# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed read-only access to one current published FHIR formulary."""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
import os
import re
from typing import Any, Mapping

from sqlalchemy import and_, bindparam, select, text

from db.models import FHIRFormularyCoveragePlan
from db.models import FHIRFormularyCoveragePlanVersion
from db.models import FHIRFormularyCurrent
from db.models import FHIRFormularyDataset
from db.models import FHIRFormularyDatasetCoveragePlan


FHIR_FORMULARY_SERVING_ENABLED_ENV = "HLTHPRT_FHIR_FORMULARY_SERVING_ENABLED"
FHIR_FORMULARY_CACHE_CONTROL = "private, no-store"
FHIR_FORMULARY_PUBLIC_ID_PATTERN = re.compile(r"^fhir_[a-z2-7]{26}$")
_TRUE_ENV_VALUES = frozenset({"1", "true", "yes", "on"})
_READ_TRANSACTION_SQL = text(
    "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"
)


class FHIRFormularyNotFoundError(RuntimeError):
    """Collapse malformed, unknown, and non-current plan identities."""


class FHIRFormularyServingUnavailableError(RuntimeError):
    """Fail closed when serving is dormant or stored evidence is invalid."""


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


_current = FHIRFormularyCurrent.__table__
_dataset = FHIRFormularyDataset.__table__
_dataset_plan = FHIRFormularyDatasetCoveragePlan.__table__
_plan = FHIRFormularyCoveragePlan.__table__
_version = FHIRFormularyCoveragePlanVersion.__table__

_DETAIL_STATEMENT = (
    select(
        _plan.c.public_id.label("formulary_id"),
        _version.c.status,
        _version.c.title,
        _version.c.name,
        _version.c.period_start,
        _version.c.period_end,
        _version.c.upstream_last_updated.label("last_updated"),
        _dataset.c.cutoff_at.label("as_of"),
        _current.c.published_at,
    )
    .select_from(
        _current.join(
            _dataset,
            and_(
                _dataset.c.source_id == _current.c.source_id,
                _dataset.c.dataset_id == _current.c.dataset_id,
            ),
        )
        .join(
            _dataset_plan,
            and_(
                _dataset_plan.c.source_id == _dataset.c.source_id,
                _dataset_plan.c.dataset_id == _dataset.c.dataset_id,
            ),
        )
        .join(
            _plan,
            and_(
                _plan.c.source_id == _dataset_plan.c.source_id,
                _plan.c.public_id == _dataset_plan.c.public_id,
            ),
        )
        .join(
            _version,
            and_(
                _version.c.public_id == _dataset_plan.c.public_id,
                _version.c.coverage_version_id
                == _dataset_plan.c.coverage_version_id,
            ),
        )
    )
    .where(
        _plan.c.public_id == bindparam("public_id"),
        _dataset.c.status == "published",
        _dataset.c.verified_at.is_not(None),
        _dataset.c.failed_at.is_(None),
        _dataset.c.error_json.is_(None),
        _dataset.c.published_at == _current.c.published_at,
        _current.c.generation > 0,
        _dataset.c.coverage_hash.is_not(None),
        _dataset.c.membership_hash.is_not(None),
        _dataset.c.publish_requested != _dataset.c.seed_eligible,
        _version.c.upstream_last_updated.is_not(None),
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
    }


__all__ = (
    "FHIR_FORMULARY_CACHE_CONTROL",
    "FHIR_FORMULARY_PUBLIC_ID_PATTERN",
    "FHIR_FORMULARY_SERVING_ENABLED_ENV",
    "FHIRFormularyNotFoundError",
    "FHIRFormularyServingUnavailableError",
    "PublicFHIRFormularyDetail",
    "is_fhir_formulary_serving_enabled",
    "public_fhir_formulary_payload",
    "read_current_fhir_formulary",
)
