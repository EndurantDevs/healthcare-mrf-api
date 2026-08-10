# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Pure strict normalization for public UHC formulary MRF records."""

from __future__ import annotations

import datetime as dt
import hashlib
from dataclasses import dataclass, field
from typing import Any

from process.formulary_fhir.identity import fhir_json_snapshot
from process.formulary_fhir.repository_shared import json_text
from process.formulary_fhir.source_artifact_contract import VerifiedSourceArtifact
from process.formulary_fhir.uhc_drug_parser_contract import UHCDrugPlanKey
from process.formulary_fhir.uhc_drug_parser_contract import uhc_drug_plan_alias


SPOOL_CONTRACT = "uhc-official-drug-spool-v2"
MAX_RECORD_FIELDS = 64
MAX_PLAN_FIELDS = 64
MAX_PLANS_PER_RECORD = 10_000
MAX_EXPANDED_MEMBERSHIPS_PER_RECORD = 100_000
MAX_RXNORM_LENGTH = 32
MAX_DRUG_NAME_LENGTH = 2_048
MAX_TIER_LENGTH = 256
MAX_EXTENSION_JSON_BYTES = 65_536
MAX_EXTENSION_JSON_NODES = 4_096
_RECORD_FIELDS = frozenset({"rxnorm_id", "drug_name", "plans", "last_updated_on"})
_PLAN_FIELDS = frozenset(
    {
        "plan_id",
        "plan_id_type",
        "drug_tier",
        "prior_authorization",
        "step_therapy",
        "quantity_limit",
        "years",
    }
)


class UHCDrugNormalizationError(ValueError):
    """Reject source data without including source values in the message."""


@dataclass(frozen=True, slots=True, repr=False)
class NormalizedUHCDrugMembership:
    """Carry one exact plan and RxNorm membership into the private spool."""

    key: UHCDrugPlanKey
    rxnorm_id: str
    drug_name: str
    drug_tier: str
    prior_authorization: bool | None
    step_therapy: bool | None
    quantity_limit: bool | None
    effective_updated_at: dt.datetime
    semantic_json: str = field(repr=False)
    provenance_json: str = field(repr=False)


@dataclass(frozen=True, slots=True, repr=False)
class _RecordNormalization:
    rxnorm_id: str
    drug_name: str
    plans: tuple[dict[str, Any], ...] = field(repr=False)
    effective_updated_at: dt.datetime
    timestamp_basis: str
    extension: dict[str, Any] = field(repr=False)


@dataclass(frozen=True, slots=True, repr=False)
class _PlanNormalization:
    plan_id_type: str
    plan_id: str
    drug_tier: str
    prior_authorization: bool | None
    step_therapy: bool | None
    quantity_limit: bool | None
    years: tuple[int, ...]
    extension: dict[str, Any] = field(repr=False)


def _source_text(value: object, label: str, maximum_length: int) -> str:
    if (
        type(value) is not str
        or not value
        or len(value) > maximum_length
        or value != value.strip()
        or any(not character.isprintable() for character in value)
    ):
        raise UHCDrugNormalizationError(f"UHC drug {label} is invalid")
    return value


def _catalog_timestamp(raw_timestamp: object) -> dt.datetime:
    timestamp_text = _source_text(raw_timestamp, "catalog timestamp", 64)
    try:
        parsed_timestamp = dt.datetime.fromisoformat(
            timestamp_text[:-1] + "+00:00"
            if timestamp_text.endswith("Z")
            else timestamp_text
        )
    except ValueError:
        raise UHCDrugNormalizationError(
            "UHC drug catalog timestamp is invalid"
        ) from None
    if parsed_timestamp.tzinfo is None or parsed_timestamp.utcoffset() is None:
        raise UHCDrugNormalizationError("UHC drug catalog timestamp is invalid")
    normalized_timestamp = parsed_timestamp.astimezone(dt.UTC)
    if normalized_timestamp.isoformat().replace("+00:00", "Z") != timestamp_text:
        raise UHCDrugNormalizationError("UHC drug catalog timestamp is invalid")
    return normalized_timestamp


def _record_timestamp(
    value: object,
    fallback_timestamp: dt.datetime,
) -> tuple[dt.datetime, str]:
    if value is None:
        return fallback_timestamp, "artifact.catalog_modified_at"
    timestamp_text = _source_text(value, "last updated date", 40)
    try:
        if len(timestamp_text) == 10:
            parsed_date = dt.date.fromisoformat(timestamp_text)
            return (
                dt.datetime.combine(parsed_date, dt.time(), tzinfo=dt.UTC),
                "record.last_updated_on",
            )
        parsed_timestamp = dt.datetime.fromisoformat(
            timestamp_text[:-1] + "+00:00"
            if timestamp_text.endswith("Z")
            else timestamp_text
        )
    except ValueError:
        raise UHCDrugNormalizationError(
            "UHC drug last updated date is invalid"
        ) from None
    if parsed_timestamp.tzinfo is None or parsed_timestamp.utcoffset() is None:
        raise UHCDrugNormalizationError(
            "UHC drug last updated date is invalid"
        )
    return parsed_timestamp.astimezone(dt.UTC), "record.last_updated_on"


def _json_extension(
    source_object: dict[str, Any],
    known_fields: frozenset[str],
) -> dict[str, Any]:
    extension_by_field = {
        field_name: field_value
        for field_name, field_value in source_object.items()
        if field_name not in known_fields
    }
    pending_nodes: list[Any] = [extension_by_field]
    node_count = 0
    while pending_nodes:
        json_node = pending_nodes.pop()
        node_count += 1
        if node_count > MAX_EXTENSION_JSON_NODES:
            raise UHCDrugNormalizationError("UHC drug extension is too large")
        if type(json_node) is list:
            pending_nodes.extend(json_node)
        elif type(json_node) is dict:
            pending_nodes.extend(json_node.values())
    try:
        normalized_extension = fhir_json_snapshot(extension_by_field)
        encoded_extension = json_text(normalized_extension).encode("utf-8")
    except (TypeError, ValueError):
        raise UHCDrugNormalizationError("UHC drug extension is invalid") from None
    if len(encoded_extension) > MAX_EXTENSION_JSON_BYTES:
        raise UHCDrugNormalizationError("UHC drug extension is too large")
    return normalized_extension


def _policy_flag(plan_by_field: dict[str, Any], field_name: str) -> bool | None:
    policy_value = plan_by_field.get(field_name)
    if policy_value is not None and type(policy_value) is not bool:
        raise UHCDrugNormalizationError("UHC drug policy flag is invalid")
    return policy_value


def _plan_years(plan_by_field: dict[str, Any]) -> tuple[int, ...]:
    plan_year_values = plan_by_field.get("years")
    if (
        type(plan_year_values) is not list
        or not plan_year_values
        or any(
            type(plan_year) is not int or not 2000 <= plan_year <= 2100
            for plan_year in plan_year_values
        )
        or len(set(plan_year_values)) != len(plan_year_values)
    ):
        raise UHCDrugNormalizationError("UHC drug plan years are invalid")
    return tuple(sorted(plan_year_values))


def _validated_record(
    source_record: object,
    artifact: VerifiedSourceArtifact,
) -> _RecordNormalization:
    if type(source_record) is not dict or not {
        "rxnorm_id",
        "drug_name",
        "plans",
    }.issubset(source_record):
        raise UHCDrugNormalizationError("UHC drug record fields are invalid")
    if len(source_record) > MAX_RECORD_FIELDS:
        raise UHCDrugNormalizationError("UHC drug record fields are invalid")
    rxnorm_id = _source_text(
        source_record.get("rxnorm_id"),
        "RxNorm id",
        MAX_RXNORM_LENGTH,
    )
    if not rxnorm_id.isascii() or not rxnorm_id.isdigit():
        raise UHCDrugNormalizationError("UHC drug RxNorm id is invalid")
    raw_plans = source_record.get("plans")
    if (
        type(raw_plans) is not list
        or not raw_plans
        or len(raw_plans) > MAX_PLANS_PER_RECORD
        or any(type(plan_entry) is not dict for plan_entry in raw_plans)
    ):
        raise UHCDrugNormalizationError("UHC drug plan collection is invalid")
    artifact_timestamp = _catalog_timestamp(artifact.identity.catalog_modified_at)
    effective_timestamp, timestamp_basis = _record_timestamp(
        source_record.get("last_updated_on"),
        artifact_timestamp,
    )
    return _RecordNormalization(
        rxnorm_id=rxnorm_id,
        drug_name=_source_text(
            source_record.get("drug_name"),
            "name",
            MAX_DRUG_NAME_LENGTH,
        ),
        plans=tuple(raw_plans),
        effective_updated_at=effective_timestamp,
        timestamp_basis=timestamp_basis,
        extension=_json_extension(source_record, _RECORD_FIELDS),
    )


def _semantic_fields(
    record: _RecordNormalization,
    *,
    drug_tier: str,
    prior_authorization: bool | None,
    step_therapy: bool | None,
    quantity_limit: bool | None,
    plan_extension: dict[str, Any],
) -> dict[str, Any]:
    return {
        "contract": SPOOL_CONTRACT,
        "drug_name": record.drug_name,
        "drug_tier": drug_tier,
        "plan_extension": plan_extension,
        "prior_authorization": prior_authorization,
        "quantity_limit": quantity_limit,
        "record_extension": record.extension,
        "rxnorm_id": record.rxnorm_id,
        "step_therapy": step_therapy,
    }


def _validated_plan(plan_by_field: dict[str, Any]) -> _PlanNormalization:
    if not _PLAN_FIELDS.issubset(plan_by_field) or len(plan_by_field) > (
        MAX_PLAN_FIELDS
    ):
        raise UHCDrugNormalizationError("UHC drug plan fields are invalid")
    return _PlanNormalization(
        plan_id_type=_source_text(
            plan_by_field.get("plan_id_type"),
            "plan id type",
            64,
        ),
        plan_id=_source_text(plan_by_field.get("plan_id"), "plan id", 256),
        drug_tier=_source_text(
            plan_by_field.get("drug_tier"),
            "tier",
            MAX_TIER_LENGTH,
        ),
        prior_authorization=_policy_flag(plan_by_field, "prior_authorization"),
        step_therapy=_policy_flag(plan_by_field, "step_therapy"),
        quantity_limit=_policy_flag(plan_by_field, "quantity_limit"),
        years=_plan_years(plan_by_field),
        extension=_json_extension(plan_by_field, _PLAN_FIELDS),
    )


def _membership_payloads(
    normalized_record: _RecordNormalization,
    normalized_plan: _PlanNormalization,
    artifact: VerifiedSourceArtifact,
    *,
    record_ordinal: int,
    plan_ordinal: int,
) -> tuple[str, str]:
    semantic_json = json_text(
        _semantic_fields(
            normalized_record,
            drug_tier=normalized_plan.drug_tier,
            prior_authorization=normalized_plan.prior_authorization,
            step_therapy=normalized_plan.step_therapy,
            quantity_limit=normalized_plan.quantity_limit,
            plan_extension=normalized_plan.extension,
        )
    )
    provenance_json = json_text(
        [
            {
                "artifact_sha256": artifact.artifact_sha256,
                "catalog_modified_at": artifact.identity.catalog_modified_at,
                "family": artifact.identity.family,
                "file_name": artifact.identity.file_name,
                "plan_ordinal": plan_ordinal,
                "record_ordinal": record_ordinal,
                "selected": True,
                "semantic_sha256": hashlib.sha256(
                    semantic_json.encode("utf-8")
                ).hexdigest(),
                "source_file_id": artifact.identity.source_file_id,
                "timestamp_basis": normalized_record.timestamp_basis,
            }
        ]
    )
    return semantic_json, provenance_json


def _normalized_plan_memberships(
    normalized_record: _RecordNormalization,
    normalized_plan: _PlanNormalization,
    artifact: VerifiedSourceArtifact,
    *,
    record_ordinal: int,
    plan_ordinal: int,
) -> tuple[NormalizedUHCDrugMembership, ...]:
    """Expand one validated source plan entry into plan-year memberships."""

    semantic_json, provenance_json = _membership_payloads(
        normalized_record,
        normalized_plan,
        artifact,
        record_ordinal=record_ordinal,
        plan_ordinal=plan_ordinal,
    )
    normalized_memberships: list[NormalizedUHCDrugMembership] = []
    for plan_year in normalized_plan.years:
        plan_key = UHCDrugPlanKey(
            family=artifact.identity.family,
            plan_id_type=normalized_plan.plan_id_type,
            plan_id=normalized_plan.plan_id,
            plan_year=plan_year,
            source_plan_identifier=uhc_drug_plan_alias(
                artifact.identity.family,
                normalized_plan.plan_id_type,
                normalized_plan.plan_id,
                plan_year,
            ),
        )
        normalized_memberships.append(
            NormalizedUHCDrugMembership(
                key=plan_key,
                rxnorm_id=normalized_record.rxnorm_id,
                drug_name=normalized_record.drug_name,
                drug_tier=normalized_plan.drug_tier,
                prior_authorization=normalized_plan.prior_authorization,
                step_therapy=normalized_plan.step_therapy,
                quantity_limit=normalized_plan.quantity_limit,
                effective_updated_at=normalized_record.effective_updated_at,
                semantic_json=semantic_json,
                provenance_json=provenance_json,
            )
        )
    return tuple(normalized_memberships)


def normalized_uhc_drug_memberships(
    source_record: object,
    artifact: VerifiedSourceArtifact,
    record_ordinal: int,
) -> tuple[NormalizedUHCDrugMembership, ...]:
    """Normalize every reported plan-year membership in one source record."""

    normalized_record = _validated_record(source_record, artifact)
    normalized_plans = tuple(
        _validated_plan(plan_by_field)
        for plan_by_field in normalized_record.plans
    )
    expanded_membership_count = sum(
        len(normalized_plan.years) for normalized_plan in normalized_plans
    )
    if expanded_membership_count > MAX_EXPANDED_MEMBERSHIPS_PER_RECORD:
        raise UHCDrugNormalizationError(
            "UHC drug expanded membership collection is too large"
        )
    normalized_memberships = []
    for plan_ordinal, normalized_plan in enumerate(normalized_plans, start=1):
        normalized_memberships.extend(
            _normalized_plan_memberships(
                normalized_record,
                normalized_plan,
                artifact,
                record_ordinal=record_ordinal,
                plan_ordinal=plan_ordinal,
            )
        )
    return tuple(normalized_memberships)


__all__ = (
    "NormalizedUHCDrugMembership",
    "SPOOL_CONTRACT",
    "UHCDrugNormalizationError",
    "normalized_uhc_drug_memberships",
)
