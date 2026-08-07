# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Source-qualified contracts and helpers for formulary persistence."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import re
from dataclasses import dataclass, field
from typing import Any, Literal, Mapping

from process.formulary_fhir.types import MedicationRecord


PublicationIntent = Literal["none", "requested", "seed"]
DatasetStatus = Literal["building", "verified", "published", "failed"]
AliasAcquisitionMode = Literal["full", "reuse"]
CheckpointMode = Literal["full", "reuse"]

HASH_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
WRITE_BATCH_SIZE = 500


def quoted_identifier(identifier: str) -> str:
    """Quote one PostgreSQL identifier without admitting SQL syntax."""

    return '"' + identifier.replace('"', '""') + '"'


def configured_schema() -> str:
    """Return the exact schema shared with the ORM configuration contract."""

    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise RuntimeError("DB_SCHEMA and HLTHPRT_DB_SCHEMA must match")
    return runtime_schema or legacy_schema or "mrf"


def table_name(name: str) -> str:
    """Return one schema-qualified table name."""

    return (
        f"{quoted_identifier(configured_schema())}."
        f"{quoted_identifier(name)}"
    )


def strict_text(value: object, label: str, maximum_length: int) -> str:
    """Require one bounded printable string without coercion."""

    if (
        type(value) is not str
        or not value
        or len(value) > maximum_length
        or value != value.strip()
        or any(not character.isprintable() for character in value)
    ):
        raise ValueError(f"FHIR formulary {label} is invalid")
    return value


def strict_hash(value: object, label: str) -> str:
    """Require one lowercase SHA-256 value."""

    if type(value) is not str or not HASH_PATTERN.fullmatch(value):
        raise ValueError(f"FHIR formulary {label} is invalid")
    return value


def utc_timestamp(value: object, label: str) -> dt.datetime:
    """Require one timezone-aware timestamp and normalize it to UTC."""

    if type(value) is not dt.datetime or value.tzinfo is None:
        raise ValueError(f"FHIR formulary {label} is invalid")
    return value.astimezone(dt.UTC)


def stable_id(prefix: str, source_id: str, *identity_parts: str) -> str:
    """Create a bounded identifier whose digest is explicitly source scoped."""

    strict_text(prefix, "identifier prefix", 15)
    strict_text(source_id, "source id", 64)
    if not identity_parts:
        raise ValueError("FHIR formulary stable identity is empty")
    normalized_parts = tuple(
        strict_text(part, "stable identity part", 4_096)
        for part in identity_parts
    )
    identity = "\x1f".join((source_id, *normalized_parts)).encode("utf-8")
    return prefix + hashlib.sha256(identity).hexdigest()[:48]


def json_text(json_value: Any) -> str:
    """Serialize deterministic JSON without permissive type coercion."""

    return json.dumps(
        json_value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def json_object(json_value: Any) -> dict[str, Any]:
    """Decode an exact JSON object returned by PostgreSQL."""

    if type(json_value) is dict:
        return json_value
    if type(json_value) is str:
        try:
            decoded_value = json.loads(json_value)
        except json.JSONDecodeError as error:
            raise RuntimeError("FHIR formulary stored JSON is invalid") from error
        if type(decoded_value) is dict:
            return decoded_value
    raise RuntimeError("FHIR formulary stored JSON is not an object")


def row_mapping(database_row: Any) -> dict[str, Any]:
    """Normalize SQLAlchemy and mapping rows into a plain dictionary."""

    if database_row is None:
        return {}
    return dict(getattr(database_row, "_mapping", database_row))


def intent_flags(intent: PublicationIntent) -> tuple[bool, bool]:
    """Map one explicit intent to the two immutable dataset flags."""

    if intent == "none":
        return False, False
    if intent == "requested":
        return True, False
    if intent == "seed":
        return False, True
    raise ValueError("FHIR formulary publication intent is invalid")


def flags_intent(publish_requested: object, seed_eligible: object) -> PublicationIntent:
    """Recover an exact intent from stored dataset flags."""

    flags = (publish_requested is True, seed_eligible is True)
    if flags == (False, False):
        return "none"
    if flags == (True, False):
        return "requested"
    if flags == (False, True):
        return "seed"
    raise RuntimeError("FHIR formulary stored publication intent is invalid")


@dataclass(frozen=True, slots=True)
class DatasetRef:
    source_id: str
    dataset_id: str
    run_id: str
    previous_dataset_id: str | None
    cutoff_at: dt.datetime
    acquisition_contract_hash: str = field(repr=False)
    intent: PublicationIntent
    status: DatasetStatus

    def __post_init__(self) -> None:
        strict_text(self.source_id, "source id", 64)
        strict_text(self.dataset_id, "dataset id", 64)
        strict_text(self.run_id, "run id", 64)
        if self.previous_dataset_id is not None:
            strict_text(self.previous_dataset_id, "previous dataset id", 64)
        utc_timestamp(self.cutoff_at, "dataset cutoff")
        strict_hash(self.acquisition_contract_hash, "acquisition contract hash")
        intent_flags(self.intent)
        if self.status not in {"building", "verified", "published", "failed"}:
            raise ValueError("FHIR formulary dataset status is invalid")


@dataclass(frozen=True, slots=True)
class AliasRef:
    source_id: str
    public_id: str
    alias_id: str
    source_plan_identifier: str = field(repr=False)

    def __post_init__(self) -> None:
        strict_text(self.source_id, "source id", 64)
        strict_text(self.public_id, "public id", 31)
        strict_text(self.alias_id, "alias id", 64)
        strict_text(self.source_plan_identifier, "source plan identifier", 512)


@dataclass(frozen=True, slots=True)
class CoveragePlanWriteResult:
    dataset: DatasetRef
    coverage_version_id: str
    aliases: tuple[AliasRef, ...]


@dataclass(frozen=True, slots=True)
class PriorAliasState:
    source_id: str
    public_id: str
    alias_id: str
    source_plan_identifier: str = field(repr=False)
    alias_version_id: str
    expected_count: int
    cutoff_at: dt.datetime
    variants_by_medication_id: Mapping[str, str] = field(repr=False)
    membership_hash: str

    def __post_init__(self) -> None:
        AliasRef(
            self.source_id,
            self.public_id,
            self.alias_id,
            self.source_plan_identifier,
        )
        strict_text(self.alias_version_id, "alias version id", 64)
        if type(self.expected_count) is not int or self.expected_count < 0:
            raise ValueError("FHIR formulary prior alias count is invalid")
        utc_timestamp(self.cutoff_at, "prior alias cutoff")
        strict_hash(self.membership_hash, "prior membership hash")


@dataclass(frozen=True, slots=True)
class CurrentSnapshot:
    dataset: DatasetRef | None
    aliases: Mapping[tuple[str, str], PriorAliasState] = field(repr=False)


@dataclass(frozen=True, slots=True)
class AliasVersionWrite:
    dataset: DatasetRef
    alias: AliasRef
    expected_count: int
    medications: tuple[MedicationRecord, ...] = field(repr=False)
    fence_token: int

    def __post_init__(self) -> None:
        if self.dataset.source_id != self.alias.source_id:
            raise ValueError("FHIR formulary alias source does not match dataset")
        if type(self.expected_count) is not int or self.expected_count < 0:
            raise ValueError("FHIR formulary expected count is invalid")
        if type(self.medications) is not tuple:
            raise ValueError("FHIR formulary medications must be an exact tuple")
        if type(self.fence_token) is not int or self.fence_token <= 0:
            raise ValueError("FHIR formulary checkpoint fence is invalid")


@dataclass(frozen=True, slots=True)
class AliasVersionResult:
    source_id: str
    dataset_id: str
    alias_id: str
    alias_version_id: str
    membership_count: int
    membership_hash: str
    acquisition_mode: AliasAcquisitionMode


@dataclass(frozen=True, slots=True)
class CheckpointWrite:
    dataset: DatasetRef
    alias: AliasRef
    fence_token: int
    acquisition_mode: CheckpointMode
    expected_count: int | None
    processed_count: int
    membership_hash: str | None
    completed: bool

    def __post_init__(self) -> None:
        if self.dataset.source_id != self.alias.source_id:
            raise ValueError("FHIR formulary checkpoint source is inconsistent")
        if type(self.fence_token) is not int or self.fence_token <= 0:
            raise ValueError("FHIR formulary checkpoint fence is invalid")
        if self.acquisition_mode not in {"full", "reuse"}:
            raise ValueError("FHIR formulary checkpoint mode is invalid")
        if self.expected_count is not None and (
            type(self.expected_count) is not int or self.expected_count < 0
        ):
            raise ValueError("FHIR formulary checkpoint count is invalid")
        if type(self.processed_count) is not int or self.processed_count < 0:
            raise ValueError("FHIR formulary checkpoint progress is invalid")
        if self.expected_count is not None and self.processed_count > self.expected_count:
            raise ValueError("FHIR formulary checkpoint progress exceeds count")
        if self.membership_hash is not None:
            strict_hash(self.membership_hash, "checkpoint membership hash")
        if type(self.completed) is not bool:
            raise ValueError("FHIR formulary checkpoint completion is invalid")
        if self.completed and (
            self.expected_count is None
            or self.processed_count != self.expected_count
            or self.membership_hash is None
        ):
            raise ValueError("FHIR formulary completed checkpoint is incomplete")


@dataclass(frozen=True, slots=True)
class CompletedAliasCheckpoint:
    source_id: str
    dataset_id: str
    alias_id: str
    alias_version_id: str
    expected_count: int
    membership_hash: str
    acquisition_mode: CheckpointMode


@dataclass(frozen=True, slots=True)
class DatasetVerification:
    source_id: str
    dataset_id: str
    list_count: int
    alias_count: int
    medication_membership_count: int
    coverage_hash: str
    membership_hash: str


@dataclass(frozen=True, slots=True)
class PublicationResult:
    source_id: str
    dataset_id: str
    generation: int
    published_at: dt.datetime


def medication_variant_hash(medication: MedicationRecord) -> str:
    """Hash all fields that define one alias-specific medication variant."""

    variant_by_field = {
        "medication_content_hash": medication.content_hash,
        "rxnorm_id": medication.rxnorm_id,
        "drug_tier": medication.drug_tier,
        "prior_authorization": medication.prior_authorization,
        "step_therapy": medication.step_therapy,
        "quantity_limit": medication.quantity_limit,
    }
    return hashlib.sha256(json_text(variant_by_field).encode("utf-8")).hexdigest()


def membership_hash(variants_by_medication_id: Mapping[str, str]) -> str:
    """Hash a complete medication ID to coverage-variant map."""

    digest = hashlib.sha256()
    for medication_id, variant_hash in sorted(variants_by_medication_id.items()):
        strict_text(medication_id, "medication id", 256)
        strict_hash(variant_hash, "variant hash")
        digest.update(medication_id.encode("utf-8"))
        digest.update(b"\x00")
        digest.update(variant_hash.encode("ascii"))
        digest.update(b"\n")
    return digest.hexdigest()


def aggregate_hash(domain: str, rows: list[str]) -> str:
    """Hash sorted proof rows under one explicit domain."""

    digest = hashlib.sha256()
    digest.update(domain.encode("ascii"))
    digest.update(b"\n")
    for proof_row in sorted(rows):
        digest.update(proof_row.encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()


async def lock_source(database: Any, source_id: str) -> dict[str, Any]:
    """Lock one preconfigured source row without changing it."""

    source_row = await database.first(
        f"SELECT source_id FROM {table_name('fhir_formulary_source')} "
        "WHERE source_id = :source_id FOR UPDATE;",
        source_id=source_id,
    )
    source_by_field = row_mapping(source_row)
    if source_by_field.get("source_id") != source_id:
        raise RuntimeError("FHIR formulary source is not registered")
    return source_by_field


async def lock_dataset(
    database: Any,
    source_id: str,
    dataset: DatasetRef,
    *,
    allowed_statuses: set[str],
) -> dict[str, Any]:
    """Lock and validate an exact source-owned dataset reference."""

    if dataset.source_id != source_id:
        raise RuntimeError("FHIR formulary dataset source is inconsistent")
    dataset_row = await database.first(
        f"SELECT source_id, dataset_id, run_id, previous_dataset_id, cutoff_at, "
        "status, publish_requested, seed_eligible, summary_json, list_count, "
        "alias_count, medication_count, coverage_hash, membership_hash, "
        f"published_at FROM {table_name('fhir_formulary_dataset')} "
        "WHERE source_id = :source_id AND dataset_id = :dataset_id "
        "FOR UPDATE;",
        source_id=source_id,
        dataset_id=dataset.dataset_id,
    )
    dataset_by_field = row_mapping(dataset_row)
    stored_summary = json_object(dataset_by_field.get("summary_json"))
    stored_contract_hash = stored_summary.get("acquisition_contract_hash")
    expected_flags = intent_flags(dataset.intent)
    stored_flags = (
        dataset_by_field.get("publish_requested") is True,
        dataset_by_field.get("seed_eligible") is True,
    )
    exact_fields_match = bool(
        dataset_by_field.get("source_id") == source_id
        and dataset_by_field.get("dataset_id") == dataset.dataset_id
        and dataset_by_field.get("run_id") == dataset.run_id
        and dataset_by_field.get("previous_dataset_id")
        == dataset.previous_dataset_id
        and dataset_by_field.get("cutoff_at")
        == utc_timestamp(dataset.cutoff_at, "dataset cutoff")
        and stored_contract_hash == dataset.acquisition_contract_hash
        and stored_flags == expected_flags
    )
    if not exact_fields_match:
        raise RuntimeError("FHIR formulary dataset reference is inconsistent")
    if dataset_by_field.get("status") not in allowed_statuses:
        raise RuntimeError("FHIR formulary dataset lifecycle state is invalid")
    return dataset_by_field


async def persisted_membership_proof(
    database: Any,
    source_id: str,
    alias_version_id: str,
) -> tuple[int, str, dict[str, str]]:
    """Recompute one persisted membership in bounded keyset pages."""

    variants_by_id: dict[str, str] = {}
    last_medication_id = ""
    while True:
        membership_rows = await database.all(
            f"SELECT upstream_medication_id, variant_hash FROM "
            f"{table_name('fhir_formulary_alias_membership')} "
            "WHERE source_id = :source_id "
            "AND alias_version_id = :alias_version_id "
            "AND upstream_medication_id > :last_medication_id "
            "ORDER BY upstream_medication_id LIMIT :batch_size;",
            source_id=source_id,
            alias_version_id=alias_version_id,
            last_medication_id=last_medication_id,
            batch_size=WRITE_BATCH_SIZE,
        )
        if not membership_rows:
            break
        for membership_row in membership_rows:
            membership_by_field = row_mapping(membership_row)
            medication_id = strict_text(
                membership_by_field.get("upstream_medication_id"),
                "stored medication id",
                256,
            )
            variant_hash = strict_hash(
                membership_by_field.get("variant_hash"),
                "stored variant hash",
            )
            if medication_id in variants_by_id:
                raise RuntimeError("FHIR formulary membership contains duplicates")
            variants_by_id[medication_id] = variant_hash
        last_medication_id = next(reversed(variants_by_id))
        if len(membership_rows) < WRITE_BATCH_SIZE:
            break
    return len(variants_by_id), membership_hash(variants_by_id), variants_by_id


__all__ = (
    "AliasRef",
    "AliasVersionResult",
    "AliasVersionWrite",
    "CheckpointWrite",
    "CompletedAliasCheckpoint",
    "CoveragePlanWriteResult",
    "CurrentSnapshot",
    "DatasetRef",
    "DatasetVerification",
    "PriorAliasState",
    "PublicationResult",
    "aggregate_hash",
    "medication_variant_hash",
    "membership_hash",
)
