# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Shared typed boundaries for the generic custom-import candidate runner."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, AsyncContextManager, Literal

from sqlalchemy.ext.asyncio import AsyncSession

from db.models.custom_import import (
    CustomImportChildRevision,
    CustomImportEntityBinding,
    CustomImportFamilyRevision,
    CustomImportRootRecord,
    CustomImportRootRevision,
)
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.publication import GenerationSealReceipt, PublicationReceipt

RunStatus = Literal[
    "activated",
    "candidate_rejected",
    "canceled",
    "lease_lost",
    "no_change",
    "not_claimed",
    "sealed_unpublished",
]
SessionFactory = Callable[[], AsyncContextManager[AsyncSession]]


class CandidateRunnerError(RuntimeError):
    """The generic candidate graph cannot safely be materialized."""


class LeaseAuthorityLost(CandidateRunnerError):
    """A claimed worker no longer has a live fenced lease."""


class CancellationRequested(CandidateRunnerError):
    """An execution entered cancellation before publication could begin."""


@dataclass(frozen=True)
class CandidateRunRequest:
    """One host-supplied candidate bound to an existing immutable execution.

    ``roots`` and ``children_by_collection`` are already decoded, typed source
    values.  The runner validates them again through ``assemble_root_families``
    and never accepts preselected family IDs or a caller-provided output digest.
    """

    dataset_id: int
    definition_revision_id: int
    schema_revision_id: int
    execution_id: int
    lease_token: str | bytes | bytearray | memoryview
    definition: CustomImportDefinition
    roots: Sequence[Mapping[str, Any]]
    children_by_collection: Mapping[str, Sequence[Mapping[str, Any]]]
    complete_scope: bool = False


@dataclass(frozen=True)
class CandidateRunResult:
    """A compact, credential-free terminal or retryable runner result."""

    status: RunStatus
    execution_id: int
    generation_id: int | None = None
    accepted_family_count: int = 0
    rejection_count: int = 0
    seal: GenerationSealReceipt | None = None
    publication: PublicationReceipt | None = None


@dataclass(frozen=True)
class CandidateRegistry:
    """Definition-owned slots needed to build one immutable candidate graph."""

    child_collection_slots: Mapping[str, int]
    stream_slots: Mapping[str, int]
    root_stream_slot: int


@dataclass(frozen=True)
class CurrentGenerationPointer:
    """A read-consistent current generation pointer captured before graph writes."""

    generation_id: int
    definition_revision_id: int
    schema_revision_id: int
    version: int


@dataclass(frozen=True)
class StoredCandidateChild:
    """A prior immutable child retained for a fresh fenced candidate graph."""

    collection: str
    child: CustomImportChildRevision
    values_by_field: Mapping[str, object]


@dataclass(frozen=True)
class StoredCandidateFamily:
    """A prior immutable family whose payload can be cloned under a new fence."""

    root_record: CustomImportRootRecord
    root_revision: CustomImportRootRevision
    family: CustomImportFamilyRevision
    entity_binding: CustomImportEntityBinding
    root_values_by_field: Mapping[str, object]
    children: tuple[StoredCandidateChild, ...]


@dataclass(frozen=True)
class PublishedCandidateChild:
    """A current-attempt child identity used for projections and winners."""

    collection: str
    child_revision_id: int
    child_key_sha256: bytes
    values_by_field: Mapping[str, object]


@dataclass(frozen=True)
class PublishedCandidateFamily:
    """A current-attempt family identity used for generation membership."""

    root_record_id: int
    root_revision_id: int
    family_revision_id: int
    entity_binding_id: int
    family_sha256: bytes
    root_values_by_field: Mapping[str, object]
    children: tuple[PublishedCandidateChild, ...]


@dataclass(frozen=True)
class MaterializedCandidate:
    """Committed candidate graph metadata retained for terminal finality work."""

    generation_id: int
    pointer: CurrentGenerationPointer | None
    accepted_family_count: int
    rejection_count: int
