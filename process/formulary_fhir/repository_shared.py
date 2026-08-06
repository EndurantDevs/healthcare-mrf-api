# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Shared immutable contracts and helpers for formulary persistence."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
from dataclasses import dataclass
from typing import Any

from process.formulary_fhir.types import MedicationRecord


SOURCE_ID = "fhir-formulary-primary"


def quoted_identifier(identifier: str) -> str:
    """Quote one PostgreSQL identifier without admitting SQL syntax."""

    return '"' + identifier.replace('"', '""') + '"'


def table_name(name: str) -> str:
    """Return one configured schema-qualified table name."""

    schema = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    return f"{quoted_identifier(schema)}.{quoted_identifier(name)}"


def stable_id(prefix: str, *identity_parts: str) -> str:
    """Create a stable bounded identifier from canonical identity parts."""

    identity = "\x1f".join(identity_parts).encode("utf-8")
    return prefix + hashlib.sha256(identity).hexdigest()[:48]


def json_text(json_value: Any) -> str:
    """Serialize deterministic JSON for PostgreSQL casts and hashes."""

    return json.dumps(
        json_value,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def json_object(json_value: Any) -> dict[str, Any]:
    """Return a decoded JSON object or an empty object for invalid input."""

    if isinstance(json_value, dict):
        return json_value
    if isinstance(json_value, str):
        try:
            decoded_json = json.loads(json_value)
        except json.JSONDecodeError:
            return {}
        return decoded_json if isinstance(decoded_json, dict) else {}
    return {}


def row_mapping(database_row: Any) -> dict[str, Any]:
    """Normalize SQLAlchemy and mapping rows into a plain dictionary."""

    if database_row is None:
        return {}
    return dict(getattr(database_row, "_mapping", database_row))


def upstream_time(timestamp: str | None) -> dt.datetime | None:
    """Parse an upstream FHIR timestamp while tolerating absent metadata."""

    if not timestamp:
        return None
    try:
        parsed_timestamp = dt.datetime.fromisoformat(
            timestamp.replace("Z", "+00:00")
        )
    except ValueError:
        return None
    if parsed_timestamp.tzinfo:
        return parsed_timestamp
    return parsed_timestamp.replace(tzinfo=dt.UTC)


@dataclass(frozen=True)
class PriorAliasState:
    alias_id: str
    alias_version_id: str
    expected_count: int
    cutoff_at: dt.datetime
    variants_by_medication_id: dict[str, str]

    @property
    def membership_ids(self) -> frozenset[str]:
        """Return the immutable prior membership ID census."""

        return frozenset(self.variants_by_medication_id)


@dataclass(frozen=True)
class CurrentSnapshot:
    dataset_id: str | None
    cutoff_at: dt.datetime | None
    aliases: dict[tuple[str, str], PriorAliasState]


@dataclass(frozen=True)
class CompletedAliasCheckpoint:
    alias_version_id: str
    expected_count: int
    membership_hash: str
    acquisition_mode: str


@dataclass(frozen=True)
class AliasVersionWrite:
    dataset_id: str
    alias_id: str
    expected_count: int
    cutoff_at: dt.datetime
    medications: tuple[MedicationRecord, ...]
    acquisition_mode: str
    prior: PriorAliasState | None = None
    apply_california_rule: bool = False


@dataclass(frozen=True)
class CheckpointWrite:
    alias_id: str
    source_plan_identifier: str
    run_id: str
    dataset_id: str
    fence_token: int
    cutoff_at: dt.datetime
    acquisition_mode: str
    expected_count: int
    processed_count: int
    membership_hash_value: str | None
    is_completed: bool
    next_url: str | None = None


def medication_variant_hash(medication: MedicationRecord) -> str:
    """Hash the fields whose differences create an alias coverage variant."""

    variant_by_field = {
        "medication_content_hash": medication.content_hash,
        "rxnorm_id": medication.rxnorm_id,
        "drug_tier": medication.drug_tier,
        "prior_authorization": medication.prior_authorization,
        "step_therapy": medication.step_therapy,
        "quantity_limit": medication.quantity_limit,
    }
    return hashlib.sha256(
        json_text(variant_by_field).encode("utf-8")
    ).hexdigest()


def membership_hash(variants_by_medication_id: dict[str, str]) -> str:
    """Hash a complete medication ID to coverage-variant membership map."""

    digest = hashlib.sha256()
    for medication_id, variant_hash in sorted(
        variants_by_medication_id.items()
    ):
        digest.update(medication_id.encode("utf-8"))
        digest.update(b"\x00")
        digest.update(variant_hash.encode("ascii"))
        digest.update(b"\n")
    return digest.hexdigest()
