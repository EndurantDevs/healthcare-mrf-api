"""Manifest selection and bounded configuration for API evidence checks."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping

from scripts.research.provider_directory_api_evidence_support import SourceSelection


SOURCE_ID_RE = re.compile(r"^pdfhir_[0-9a-f]{24}$")
REQUIRED_CLASSIFICATIONS = frozenset(
    {"acquisition", "bulk_acquisition", "external"}
)


@dataclass(frozen=True)
class HarnessConfig:
    """Non-secret selection and work limits for one evidence run."""

    manifest_path: Path
    schema: str
    entry_ids: tuple[str, ...]
    source_ids: tuple[str, ...]
    max_sources: int
    samples_per_source: int
    candidate_limit: int
    api_latency_slo_ms: float
    require_mapped_evidence: bool = False


def _normalized_ids(identifiers: Iterable[str]) -> tuple[str, ...]:
    return tuple(
        dict.fromkeys(
            identifier.strip()
            for identifier in identifiers
            if identifier and identifier.strip()
        )
    )


def resolve_source_selection(
    manifest: Mapping[str, Any],
    *,
    requested_entry_ids: Iterable[str] = (),
    requested_source_ids: Iterable[str] = (),
    max_sources: int = 100,
) -> list[SourceSelection]:
    """Resolve selected maintained sources or reject a cap that truncates them."""
    if max_sources < 1:
        raise ValueError("max_sources must be at least one")
    entries = manifest.get("entries")
    if not isinstance(entries, list):
        raise ValueError("manifest entries are unavailable")
    requested_entries = _normalized_ids(requested_entry_ids)
    requested_sources = _normalized_ids(requested_source_ids)
    entries_by_id = {
        str(entry.get("entry_id") or ""): entry
        for entry in entries
        if isinstance(entry, Mapping)
    }
    sources_by_id = {
        str(source_id): entry_id
        for entry_id, entry in entries_by_id.items()
        for source_id in entry.get("source_ids") or []
    }
    if set(requested_entries) - set(entries_by_id):
        raise ValueError("unknown manifest entry selector")
    if any(not SOURCE_ID_RE.fullmatch(source_id) for source_id in requested_sources):
        raise ValueError("unknown manifest source selector")
    if set(requested_sources) - set(sources_by_id):
        raise ValueError("unknown manifest source selector")
    selections = _manifest_selections(entries, requested_entries, requested_sources)
    if len(selections) > max_sources:
        raise ValueError("selected maintained sources exceed max_sources")
    return selections


def _manifest_selections(
    entries: list[Any],
    requested_entries: tuple[str, ...],
    requested_sources: tuple[str, ...],
) -> list[SourceSelection]:
    selections: list[SourceSelection] = []
    for entry in entries:
        if not isinstance(entry, Mapping):
            continue
        entry_id = str(entry.get("entry_id") or "")
        classification = str(entry.get("classification") or "")
        resources = tuple(
            str(resource_type)
            for resource_type in entry.get("resources") or []
            if resource_type
        )
        for source_id_value in entry.get("source_ids") or []:
            source_id = str(source_id_value)
            if requested_entries or requested_sources:
                if (
                    entry_id not in requested_entries
                    and source_id not in requested_sources
                ):
                    continue
            selections.append(
                SourceSelection(
                    entry_id,
                    source_id,
                    classification,
                    classification in REQUIRED_CLASSIFICATIONS,
                    resources,
                )
            )
    return selections


def _validate_harness_config(config: HarnessConfig) -> None:
    if not 1 <= config.candidate_limit <= 20:
        raise ValueError("candidate_limit must be between one and twenty")
    if config.api_latency_slo_ms < 0:
        raise ValueError("api_latency_slo_ms must be zero or greater")
