# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Public cached-page manifest contract for the MRF discovery catalog."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from api.mrf_discovery_catalog_paging import MAX_FILE_PAGE_PLAN_REFERENCES


CATALOG_PAGING_MANIFEST_METADATA_KEY = "_healthporta_catalog_paging_manifest"
CATALOG_PAGING_MANIFEST_CONTRACT = "mrf-discovery-catalog-paging-manifest-v1"
CATALOG_PAGING_MANIFEST_SCOPE_REVISION = 1
CATALOG_PAGING_MANIFEST_REVISION = 1
CATALOG_PAGING_MANIFEST_PAGE_LIMITS = (100, 250, 500)


@dataclass
class _PageCounter:
    """Count bounded response windows without retaining file payloads."""

    page_limit: int
    page_total: int = 0
    row_count: int = 0
    plan_reference_count: int = 0
    is_page_open: bool = False

    @property
    def terminal_page_total(self) -> int:
        """Return the operational page count, including an empty terminal page."""

        return max(self.page_total, 1)

    def add_file(self, plan_count: int) -> None:
        """Apply the production dual-bound page rules to one stored file."""

        remaining_plan_count = max(int(plan_count), 0)
        while True:
            self._open_page()
            if self.row_count >= self.page_limit:
                self._close_page()
                continue
            if remaining_plan_count:
                available_plan_count = max(
                    MAX_FILE_PAGE_PLAN_REFERENCES - self.plan_reference_count,
                    0,
                )
                if self.row_count and available_plan_count == 0:
                    self._close_page()
                    continue
                selected_plan_count = min(
                    remaining_plan_count,
                    max(available_plan_count, 1),
                )
                self.row_count += 1
                self.plan_reference_count += selected_plan_count
                remaining_plan_count -= selected_plan_count
                if remaining_plan_count:
                    self._close_page()
                    continue
                if (
                    self.row_count >= self.page_limit
                    or self.plan_reference_count >= MAX_FILE_PAGE_PLAN_REFERENCES
                ):
                    self._close_page()
                return
            self.row_count += 1
            if self.row_count >= self.page_limit:
                self._close_page()
            return

    def _open_page(self) -> None:
        if self.is_page_open:
            return
        self.is_page_open = True
        self.page_total += 1

    def _close_page(self) -> None:
        self.is_page_open = False
        self.row_count = 0
        self.plan_reference_count = 0


@dataclass
class _SourceManifestAccumulator:
    """Accumulate a single source's scalar file stream."""

    source_id: str
    page_counter_by_limit: dict[int, _PageCounter]
    digest: Any
    file_count: int = 0

    @classmethod
    def create(cls, source_id: str) -> "_SourceManifestAccumulator":
        """Create a source-scoped accumulator with its stable digest prefix."""

        digest = hashlib.sha256()
        for digest_component in (
            CATALOG_PAGING_MANIFEST_CONTRACT,
            CATALOG_PAGING_MANIFEST_SCOPE_REVISION,
            CATALOG_PAGING_MANIFEST_REVISION,
            MAX_FILE_PAGE_PLAN_REFERENCES,
            ",".join(map(str, CATALOG_PAGING_MANIFEST_PAGE_LIMITS)),
            source_id,
        ):
            digest.update(str(digest_component).encode("utf-8"))
            digest.update(b"\0")
        return cls(
            source_id=source_id,
            page_counter_by_limit={
                page_limit: _PageCounter(page_limit)
                for page_limit in CATALOG_PAGING_MANIFEST_PAGE_LIMITS
            },
            digest=digest,
        )

    def add_file(self, file_id: str, plan_count: int) -> None:
        """Feed one ordered scalar file record to every supported page size."""

        normalized_plan_count = max(int(plan_count), 0)
        self.file_count += 1
        self.digest.update(str(file_id).encode("utf-8"))
        self.digest.update(b"\0")
        self.digest.update(str(normalized_plan_count).encode("ascii"))
        self.digest.update(b"\n")
        for page_counter in self.page_counter_by_limit.values():
            page_counter.add_file(normalized_plan_count)

    def manifest(self, source_version: str) -> dict[str, Any]:
        """Return a stable private cache payload for this completed source."""

        return {
            "contract": CATALOG_PAGING_MANIFEST_CONTRACT,
            "source_version": source_version,
            "scope_revision": CATALOG_PAGING_MANIFEST_SCOPE_REVISION,
            "manifest_revision": CATALOG_PAGING_MANIFEST_REVISION,
            "manifest_digest": self.digest.hexdigest(),
            "plan_reference_limit": MAX_FILE_PAGE_PLAN_REFERENCES,
            "file_count": self.file_count,
            "page_totals": {
                str(page_limit): page_counter.terminal_page_total
                for page_limit, page_counter in self.page_counter_by_limit.items()
            },
        }


@dataclass(frozen=True)
class _ValidatedPagingManifest:
    """The cache fields safe to expose on a source-files response."""

    source_version: str
    manifest_digest: str
    page_totals_by_limit: dict[str, int]


def catalog_page_totals(
    plan_reference_counts: Iterable[int],
) -> dict[int, int]:
    """Return exact operational page totals for every supported response size."""

    page_counter_by_limit = {
        page_limit: _PageCounter(page_limit)
        for page_limit in CATALOG_PAGING_MANIFEST_PAGE_LIMITS
    }
    for plan_reference_count in plan_reference_counts:
        normalized_plan_count = max(int(plan_reference_count), 0)
        for page_counter in page_counter_by_limit.values():
            page_counter.add_file(normalized_plan_count)
    return {
        page_limit: page_counter.terminal_page_total
        for page_limit, page_counter in page_counter_by_limit.items()
    }


def catalog_paging_manifest_for_file_page(
    source_metadata: Any,
    *,
    page_limit: int,
) -> dict[str, Any] | None:
    """Return one validated cache payload without calculating a total on demand."""

    validated_manifest = _validated_paging_manifest(source_metadata)
    if validated_manifest is None:
        return None
    file_pages_total = validated_manifest.page_totals_by_limit.get(str(page_limit))
    if file_pages_total is None:
        return None
    return _paging_manifest_response(
        file_pages_total,
        page_limit=page_limit,
        source_version=validated_manifest.source_version,
        manifest_digest=validated_manifest.manifest_digest,
    )


def _validated_paging_manifest(source_metadata: Any) -> _ValidatedPagingManifest | None:
    metadata = _metadata_dict(source_metadata)
    manifest = metadata.get(CATALOG_PAGING_MANIFEST_METADATA_KEY)
    if not isinstance(manifest, Mapping):
        return None
    source_version = _normalized_text(metadata.get("discovery_run_id"))
    manifest_digest = _normalized_text(manifest.get("manifest_digest"))
    if not _is_supported_manifest_contract(
        manifest,
        source_version=source_version,
        manifest_digest=manifest_digest,
    ):
        return None
    page_totals_by_limit = _validated_page_totals_by_limit(manifest)
    if page_totals_by_limit is None:
        return None
    return _ValidatedPagingManifest(
        source_version=source_version,
        manifest_digest=manifest_digest,
        page_totals_by_limit=page_totals_by_limit,
    )


def _is_supported_manifest_contract(
    manifest: Mapping[str, Any],
    *,
    source_version: str,
    manifest_digest: str,
) -> bool:
    """Return whether the cache belongs to the current supported source scope."""

    return (
        bool(source_version)
        and source_version == _normalized_text(manifest.get("source_version"))
        and manifest.get("contract") == CATALOG_PAGING_MANIFEST_CONTRACT
        and type(manifest.get("scope_revision")) is int
        and manifest.get("scope_revision") == CATALOG_PAGING_MANIFEST_SCOPE_REVISION
        and type(manifest.get("manifest_revision")) is int
        and manifest.get("manifest_revision") == CATALOG_PAGING_MANIFEST_REVISION
        and (
            not manifest_digest
            or re.fullmatch(r"[0-9a-f]{64}", manifest_digest) is not None
        )
        and manifest.get("plan_reference_limit")
        == MAX_FILE_PAGE_PLAN_REFERENCES
        and _nonnegative_int(manifest.get("file_count")) is not None
    )


def _validated_page_totals_by_limit(
    manifest: Mapping[str, Any],
) -> dict[str, int] | None:
    page_totals_by_limit = manifest.get("page_totals")
    if not isinstance(page_totals_by_limit, Mapping):
        return None
    expected_page_limit_keys = {
        str(page_limit) for page_limit in CATALOG_PAGING_MANIFEST_PAGE_LIMITS
    }
    if set(page_totals_by_limit) != expected_page_limit_keys:
        return None
    normalized_totals_by_limit = {
        str(page_limit): _positive_int(page_totals_by_limit.get(str(page_limit)))
        for page_limit in CATALOG_PAGING_MANIFEST_PAGE_LIMITS
    }
    if any(page_total is None for page_total in normalized_totals_by_limit.values()):
        return None
    return {
        page_limit: page_total
        for page_limit, page_total in normalized_totals_by_limit.items()
        if page_total is not None
    }


def _paging_manifest_response(
    file_pages_total: int,
    *,
    page_limit: int,
    source_version: str,
    manifest_digest: str,
) -> dict[str, Any]:
    response_by_key = {
        "file_pages_total": file_pages_total,
        "page_limit": page_limit,
        "plan_reference_limit": MAX_FILE_PAGE_PLAN_REFERENCES,
        "source_version": source_version,
        "scope_revision": CATALOG_PAGING_MANIFEST_SCOPE_REVISION,
        "manifest_revision": CATALOG_PAGING_MANIFEST_REVISION,
    }
    if manifest_digest:
        response_by_key["manifest_digest"] = manifest_digest
    return response_by_key


def _metadata_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _normalized_text(value: Any) -> str:
    return str(value or "").strip()


def _nonnegative_int(value: Any) -> int | None:
    if type(value) is not int or value < 0:
        return None
    return value


def _positive_int(value: Any) -> int | None:
    if type(value) is not int or value < 1:
        return None
    return value
