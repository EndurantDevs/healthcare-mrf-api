# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Contracts and shared helpers for immutable dataset rehydration."""

from __future__ import annotations

import dataclasses
import hashlib
import json
import re
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Mapping

from sqlalchemy import types as sa_types


DEFAULT_BATCH_SIZE = 5_000
MAX_BATCH_SIZE = 25_000
CHECKPOINT_TABLE = "provider_directory_dataset_rehydration_checkpoint"
DATASET_TABLE = "provider_directory_endpoint_dataset"
DATASET_RESOURCE_TABLE = "provider_directory_dataset_resource"
SOURCE_TABLE = "provider_directory_source"
ENDPOINT_TABLE = "provider_directory_api_endpoint"
CANONICAL_TABLE = "provider_directory_canonical_resource"
SOURCE_EDGE_TABLE = "provider_directory_source_resource"


class DatasetRehydrationError(RuntimeError):
    """The requested retained-dataset operation is unsafe or incomplete."""


@dataclass(frozen=True)
class DatasetScope:
    """Immutable values that fence one published dataset and source alias."""

    source_id: str
    dataset_id: str
    acquisition_root_run_id: str
    endpoint_id: str
    canonical_api_base: str
    dataset_hash: str
    resource_count: int
    resource_types: tuple[str, ...]
    publication_metadata_hash: str
    published_at: Any

    @property
    def fence_identity(self) -> tuple[Any, ...]:
        """Return every value that must remain stable during rehydration."""
        return dataclasses.astuple(self)


@dataclass(frozen=True)
class RehydrationRequest:
    """Operator-selected identity and bounded execution options."""

    source_id: str
    dataset_id: str
    acquisition_root_run_id: str
    owner_run_id: str
    resource_types: tuple[str, ...] = ()
    batch_size: int = DEFAULT_BATCH_SIZE


UpsertBatch = Callable[[type, list[dict[str, Any]], DatasetScope], Awaitable[int]]
CancelCheck = Callable[[], Awaitable[None]]
ProgressCallback = Callable[[dict[str, Any]], Awaitable[None]]


@dataclass(frozen=True)
class RehydrationRuntime:
    """Database, model, and lifecycle dependencies for one execution."""

    database: Any
    schema: str
    models_by_type: Mapping[str, type]
    upsert_batch: UpsertBatch
    cancel_check: CancelCheck | None = None
    progress_callback: ProgressCallback | None = None


@dataclass(frozen=True)
class ResourceContext:
    """Stable inputs for processing one retained FHIR resource type."""

    runtime: RehydrationRuntime
    request: RehydrationRequest
    scope: DatasetScope
    resource_type: str
    model: type
    expected_count: int


@dataclass(frozen=True)
class RehydrationCheckpoint:
    """Committed progress for one resource type."""

    state: str
    last_resource_id: str | None
    expected_count: int
    input_count: int
    mapped_count: int
    rejected_count: int
    evidence_by_name: dict[str, Any] = dataclasses.field(default_factory=dict)
    error: str | None = None


@dataclass(frozen=True)
class ResourceProof:
    """Independent exact-membership counts for persisted output."""

    input_count: int
    typed_count: int
    typed_extra_count: int
    canonical_hash_count: int
    canonical_extra_count: int
    source_edge_count: int
    source_edge_extra_count: int

    def as_dict(self) -> dict[str, int]:
        """Return stable field names for checkpoints and execution summaries."""
        return {
            "input": self.input_count,
            "typed": self.typed_count,
            "typed_extra": self.typed_extra_count,
            "canonical_hash_matched": self.canonical_hash_count,
            "canonical_extra": self.canonical_extra_count,
            "source_edges": self.source_edge_count,
            "source_edge_extra": self.source_edge_extra_count,
        }


@dataclass(frozen=True)
class RetainedBatch:
    """Validated typed rows and bounded rejection evidence for one page."""

    typed_rows: list[dict[str, Any]]
    rejection_reasons: tuple[str, ...]
    input_count: int
    last_resource_id: str


def _quote_identifier(identifier: str) -> str:
    """Quote a validated PostgreSQL identifier."""
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", identifier):
        raise ValueError(f"invalid SQL identifier: {identifier!r}")
    return f'"{identifier}"'


def _table_ref(schema: str, table_name: str) -> str:
    """Return a safely quoted schema-qualified table reference."""
    return f"{_quote_identifier(schema)}.{_quote_identifier(table_name)}"


def _record_fields(database_record: Any) -> dict[str, Any]:
    """Convert SQLAlchemy rows and dictionaries to a plain mapping."""
    record_mapping = getattr(database_record, "_mapping", None)
    return dict(record_mapping) if record_mapping is not None else dict(database_record)


def _clean_text(field_value: Any) -> str:
    """Normalize an optional identity value to stripped text."""
    return str(field_value or "").strip()


def _payload_hash(mapped_payload: dict[str, Any]) -> str:
    """Reproduce the importer hash for a retained mapped payload."""
    encoded_payload = json.dumps(mapped_payload, sort_keys=True, default=str)
    return hashlib.sha256(encoded_payload.encode()).hexdigest()

