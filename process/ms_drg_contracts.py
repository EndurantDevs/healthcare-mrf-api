# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Typed request, source, relationship, payload, and publication MS-DRG contracts."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from process.ms_drg_sources import MsDrgCatalogRow

RelationshipTuple = tuple[str, str, str, str, str]


@dataclass(frozen=True)
class MsDrgImportRequest:
    test_mode: bool
    include_relationships: bool
    relationship_page_limit: int | None
    concurrency: int
    cms_page_url: str
    manual_toc_url: str | None
    import_suffix: str
    run_id: str | None


@dataclass(frozen=True)
class MsDrgManualSource:
    cms_page_url: str
    toc_url: str
    toc_html: str
    list_url: str
    release: str
    catalog_rows: list[MsDrgCatalogRow]


@dataclass
class MsDrgRelationshipRows:
    relationships: set[RelationshipTuple] = field(default_factory=set)
    procedure_category_by_code: dict[str, str] = field(default_factory=dict)
    diagnosis_codes: set[str] = field(default_factory=set)
    diagnosis_page_count: int = 0
    procedure_page_count: int = 0


@dataclass(frozen=True)
class MsDrgPayloads:
    catalog_payloads: list[dict[str, Any]]
    synonym_payloads: list[dict[str, Any]]
    relationship_payloads: list[dict[str, Any]]


@dataclass(frozen=True)
class MsDrgPublishCounts:
    catalog_count: int
    synonym_count: int
    relationship_count: int
