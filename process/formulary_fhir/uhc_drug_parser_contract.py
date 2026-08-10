# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Pure contracts for strict UHC drug-file normalization."""

from __future__ import annotations

import datetime as dt
import hashlib
import re
from dataclasses import dataclass, field

from process.formulary_fhir.continuation import validated_alias
from process.formulary_fhir.repository_shared import strict_hash
from process.formulary_fhir.repository_shared import strict_text
from process.formulary_fhir.repository_shared import utc_timestamp
from process.formulary_fhir.types import CoveragePlanRecord
from process.formulary_fhir.types import MedicationRecord


PLAN_TYPE_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}\Z")
PLAN_ALIAS_DOMAIN = "uhc-official-drug-plan-alias-v1"


def _source_plan_text(value: object, label: str, maximum_length: int) -> str:
    if (
        type(value) is not str
        or not value
        or len(value) > maximum_length
        or value != value.strip()
        or any(not character.isprintable() for character in value)
    ):
        raise ValueError(f"UHC drug {label} is invalid")
    return value


def uhc_drug_plan_alias(
    family: object,
    plan_id_type: object,
    plan_id: object,
    plan_year: object,
) -> str:
    """Return a safe stable alias without rewriting the reported plan id."""

    if family not in {"cs", "ifp"}:
        raise ValueError("UHC drug plan family is invalid")
    normalized_type = _source_plan_text(plan_id_type, "plan id type", 64)
    if not PLAN_TYPE_PATTERN.fullmatch(normalized_type):
        raise ValueError("UHC drug plan id type is invalid")
    normalized_id = _source_plan_text(plan_id, "plan id", 256)
    if plan_year is not None and (
        type(plan_year) is not int or not 2000 <= plan_year <= 2100
    ):
        raise ValueError("UHC drug plan year is invalid")
    identity = "\x1f".join(
        (
            PLAN_ALIAS_DOMAIN,
            family,
            normalized_type,
            normalized_id,
            str(plan_year) if plan_year is not None else "",
        )
    )
    digest = hashlib.sha256(identity.encode("utf-8")).hexdigest()
    return validated_alias(f"uhc:{family}:{digest}")


@dataclass(frozen=True, slots=True)
class UHCDrugPlanKey:
    """Identify one source-family plan and optional reported benefit year."""

    family: str
    plan_id_type: str
    plan_id: str = field(repr=False)
    plan_year: int | None
    source_plan_identifier: str = field(repr=False)

    def __post_init__(self) -> None:
        if self.family not in {"cs", "ifp"}:
            raise ValueError("UHC drug plan family is invalid")
        _source_plan_text(self.plan_id, "plan id", 256)
        if (
            type(self.plan_id_type) is not str
            or not PLAN_TYPE_PATTERN.fullmatch(self.plan_id_type)
        ):
            raise ValueError("UHC drug plan id type is invalid")
        if self.plan_year is not None and (
            type(self.plan_year) is not int
            or not 2000 <= self.plan_year <= 2100
        ):
            raise ValueError("UHC drug plan year is invalid")
        validated_alias(self.source_plan_identifier)
        expected_identifier = uhc_drug_plan_alias(
            self.family,
            self.plan_id_type,
            self.plan_id,
            self.plan_year,
        )
        if self.source_plan_identifier != expected_identifier:
            raise ValueError("UHC drug plan alias is inconsistent")


@dataclass(frozen=True, slots=True, repr=False)
class UHCDrugSpoolEvidence:
    """Summarize one complete 48-artifact normalization spool."""

    source_id: str
    source_file_set_sha256: str = field(repr=False)
    artifact_set_sha256: str = field(repr=False)
    spool_content_sha256: str = field(repr=False)
    file_count: int
    raw_record_count: int
    raw_plan_entry_count: int
    plan_count: int
    medication_membership_count: int
    duplicate_count: int
    superseded_count: int
    max_last_updated_at: dt.datetime | None

    def __post_init__(self) -> None:
        strict_text(self.source_id, "source id", 64)
        for digest_value in (
            self.source_file_set_sha256,
            self.artifact_set_sha256,
            self.spool_content_sha256,
        ):
            strict_hash(digest_value, "UHC drug spool hash")
        counts = (
            self.file_count,
            self.raw_record_count,
            self.raw_plan_entry_count,
            self.plan_count,
            self.medication_membership_count,
            self.duplicate_count,
            self.superseded_count,
        )
        if any(type(count) is not int or count < 0 for count in counts):
            raise ValueError("UHC drug spool counts are invalid")
        if self.file_count != 48 or self.plan_count <= 0:
            raise ValueError("UHC drug spool census is incomplete")
        if self.max_last_updated_at is not None:
            normalized_timestamp = utc_timestamp(
                self.max_last_updated_at,
                "UHC drug maximum update timestamp",
            )
            if (
                self.max_last_updated_at.utcoffset() != dt.timedelta(0)
                or self.max_last_updated_at.isoformat()
                != normalized_timestamp.isoformat()
            ):
                raise ValueError("UHC drug maximum update timestamp is invalid")

    def __repr__(self) -> str:
        return (
            "UHCDrugSpoolEvidence("
            f"file_count={self.file_count}, "
            f"raw_record_count={self.raw_record_count}, "
            f"plan_count={self.plan_count}, "
            "medication_membership_count="
            f"{self.medication_membership_count})"
        )


@dataclass(frozen=True, slots=True, repr=False)
class UHCDrugPlanMaterialization:
    """Return one plan record and its exact sorted medication membership."""

    key: UHCDrugPlanKey
    coverage_plan: CoveragePlanRecord = field(repr=False)
    medications: tuple[MedicationRecord, ...] = field(repr=False)

    def __post_init__(self) -> None:
        if (
            type(self.key) is not UHCDrugPlanKey
            or type(self.coverage_plan) is not CoveragePlanRecord
            or type(self.medications) is not tuple
            or not self.medications
            or self.coverage_plan.source_plan_identifiers
            != (self.key.source_plan_identifier,)
            or any(
                type(medication) is not MedicationRecord
                or medication.source_plan_identifiers
                != (self.key.source_plan_identifier,)
                for medication in self.medications
            )
        ):
            raise ValueError("UHC drug plan materialization is invalid")


__all__ = (
    "PLAN_ALIAS_DOMAIN",
    "PLAN_TYPE_PATTERN",
    "UHCDrugPlanKey",
    "UHCDrugPlanMaterialization",
    "UHCDrugSpoolEvidence",
    "uhc_drug_plan_alias",
)
