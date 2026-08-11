# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Dormant public contracts for rooted-graph HTTP acquisition."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
import math
import time
from typing import Any

from process.provider_directory_rooted_graph_http import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_PAGE_BYTES,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_PAGES,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_QUERY_BYTES,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_RESOURCES,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_RETRY_AFTER_SECONDS,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_URL_BYTES,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_TIMEOUT_SECONDS,
    ProviderDirectoryRootedGraphHTTPBounds,
)
from process.provider_directory_rooted_graph_contract import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_EDGE_ROWS,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_PAYLOAD_BYTES,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_RESOURCE_ROWS,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_WORK_ITEMS,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_ROOT_PUBLICATION_BY_VARIANT,
)
from process.provider_directory_rooted_graph_source_contract import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_AUTHORITY_ID,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_ID,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_SIGNATURE_SHA256,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID,
)
from process.provider_directory_rooted_graph_identity import (
    ROOTED_GRAPH_SCOPE_PATTERN,
    SHA256_PATTERN,
)
from process.provider_directory_rooted_graph_query import (
    canonical_provider_directory_api_base,
)
from process.provider_directory_rooted_graph_store_contract import (
    ACQUISITION_PATTERN,
    INTENT_PATTERN,
    RUN_PATTERN,
    ProviderDirectoryRootedGraphAcquisitionIdentity,
)


PROVIDER_DIRECTORY_ROOTED_GRAPH_ACQUISITION_DEFAULT_CONCURRENCY = 4
PROVIDER_DIRECTORY_ROOTED_GRAPH_ACQUISITION_MAX_CONCURRENCY = 16
PROVIDER_DIRECTORY_ROOTED_GRAPH_ACQUISITION_DEFAULT_ATTEMPTS = 3
PROVIDER_DIRECTORY_ROOTED_GRAPH_ACQUISITION_MAX_ATTEMPTS = 8
PROVIDER_DIRECTORY_ROOTED_GRAPH_ACQUISITION_DEFAULT_RETRY_SECONDS = 1.0
PROVIDER_DIRECTORY_ROOTED_GRAPH_ACQUISITION_MAX_RETRY_SECONDS = (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_RETRY_AFTER_SECONDS
)
PROVIDER_DIRECTORY_ROOTED_GRAPH_ACQUISITION_DEFAULT_HEARTBEAT_SECONDS = 30.0
PROVIDER_DIRECTORY_ROOTED_GRAPH_ACQUISITION_DEFAULT_ROOT_TIMEOUT_SECONDS = (
    7 * 24 * 60 * 60
)
PROVIDER_DIRECTORY_ROOTED_GRAPH_ACQUISITION_MAX_ROOT_TIMEOUT_SECONDS = 30 * 24 * 60 * 60
ROOT_ROLES = ("baseline", "candidate")
ACQUISITION_STATES = frozenset({"absent", "building", "sealed"})
CENSUS_STATES = frozenset({"absent", "pending", "leased", "completed", "error"})


class ProviderDirectoryRootedGraphAcquisitionError(RuntimeError):
    """Expose bounded orchestration failures without URLs or source payloads."""

    def __init__(self, code: str = "state") -> None:
        message_by_code = {
            "disabled": "rooted graph acquisition is disabled",
            "input_drift": "rooted graph acquisition input changed",
            "root_unsealable": "rooted graph acquisition cannot be sealed",
            "state": "rooted graph acquisition runtime is invalid",
        }
        self.code = code if code in message_by_code else "state"
        super().__init__(message_by_code[self.code])


def strict_nonnegative_seconds(value: object, label: str) -> float:
    """Return one finite nonnegative duration or reject it."""

    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"provider_directory_rooted_graph_{label}_invalid")
    seconds = float(value)
    if not math.isfinite(seconds) or seconds < 0.0:
        raise ValueError(f"provider_directory_rooted_graph_{label}_invalid")
    return seconds


def _is_snapshot_lineage_valid(snapshot: Any) -> bool:
    root_pair = (snapshot.root_source_id, snapshot.root_endpoint_id)
    acquisition_pair = (
        snapshot.acquisition_source_id,
        snapshot.acquisition_endpoint_id,
    )
    return bool(
        (
            snapshot.root_dataset_variant == "uhc_flex_practitioner"
            and snapshot.root_source_id != snapshot.acquisition_source_id
            and snapshot.root_endpoint_id != snapshot.acquisition_endpoint_id
        )
        or (
            snapshot.root_dataset_variant == "rooted_combined"
            and root_pair == acquisition_pair
        )
    )


def _is_snapshot_registry_valid(snapshot: Any) -> bool:
    return bool(
        type(snapshot.root_publication_contract_id) is str
        and 0 < len(snapshot.root_publication_contract_id) <= 96
        and PROVIDER_DIRECTORY_ROOTED_GRAPH_ROOT_PUBLICATION_BY_VARIANT.get(
            snapshot.root_dataset_variant
        )
        == snapshot.root_publication_contract_id
        and type(snapshot.root_source_id) is str
        and 0 < len(snapshot.root_source_id) <= 64
        and type(snapshot.root_endpoint_id) is str
        and SHA256_PATTERN.fullmatch(snapshot.root_endpoint_id) is not None
        and snapshot.acquisition_source_id == PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID
        and snapshot.acquisition_endpoint_id
        == PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_ID
        and snapshot.source_authority_id == PROVIDER_DIRECTORY_ROOTED_GRAPH_AUTHORITY_ID
        and snapshot.endpoint_signature_sha256
        == PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_SIGNATURE_SHA256
    )


def _is_snapshot_root_valid(snapshot: Any) -> bool:
    return bool(
        type(snapshot.root_dataset_id) is str
        and 0 < len(snapshot.root_dataset_id) <= 96
        and type(snapshot.root_dataset_hash) is str
        and SHA256_PATTERN.fullmatch(snapshot.root_dataset_hash) is not None
        and type(snapshot.root_content_proof_sha256) is str
        and SHA256_PATTERN.fullmatch(snapshot.root_content_proof_sha256) is not None
        and type(snapshot.root_resource_count) is int
        and snapshot.root_resource_count >= 1
        and type(snapshot.root_cohort_id) is str
        and 0 < len(snapshot.root_cohort_id) <= 128
    )


def _is_snapshot_budget_valid(snapshot: Any) -> bool:
    budget_limits = (
        (snapshot.max_work_items, PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_WORK_ITEMS),
        (
            snapshot.max_resource_rows,
            PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_RESOURCE_ROWS,
        ),
        (snapshot.max_edge_rows, PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_EDGE_ROWS),
        (
            snapshot.max_payload_bytes,
            PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_PAYLOAD_BYTES,
        ),
    )
    return bool(
        all(
            type(budget) is int and 0 < budget <= maximum
            for budget, maximum in budget_limits
        )
        and snapshot.max_work_items > snapshot.root_resource_count
    )


@dataclass(frozen=True, slots=True)
class ProviderDirectoryRootedGraphAcquisitionConfig:
    """Manual-only execution bounds; construction never activates work."""

    enabled: bool = False
    concurrency: int = PROVIDER_DIRECTORY_ROOTED_GRAPH_ACQUISITION_DEFAULT_CONCURRENCY
    max_attempts: int = PROVIDER_DIRECTORY_ROOTED_GRAPH_ACQUISITION_DEFAULT_ATTEMPTS
    lease_seconds: int = 300
    heartbeat_seconds: float = (
        PROVIDER_DIRECTORY_ROOTED_GRAPH_ACQUISITION_DEFAULT_HEARTBEAT_SECONDS
    )
    retry_base_seconds: float = (
        PROVIDER_DIRECTORY_ROOTED_GRAPH_ACQUISITION_DEFAULT_RETRY_SECONDS
    )
    max_retry_seconds: float = (
        PROVIDER_DIRECTORY_ROOTED_GRAPH_ACQUISITION_MAX_RETRY_SECONDS
    )
    max_page_bytes: int = PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_PAGE_BYTES
    max_query_bytes: int = PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_QUERY_BYTES
    max_pages: int = PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_PAGES
    max_resources: int = PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_RESOURCES
    max_url_bytes: int = PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_URL_BYTES
    timeout_seconds: float = PROVIDER_DIRECTORY_ROOTED_GRAPH_TIMEOUT_SECONDS
    root_timeout_seconds: float = (
        PROVIDER_DIRECTORY_ROOTED_GRAPH_ACQUISITION_DEFAULT_ROOT_TIMEOUT_SECONDS
    )

    def __post_init__(self) -> None:
        heartbeat_seconds = strict_nonnegative_seconds(
            self.heartbeat_seconds,
            "heartbeat_seconds",
        )
        retry_base_seconds = strict_nonnegative_seconds(
            self.retry_base_seconds,
            "retry_base_seconds",
        )
        max_retry_seconds = strict_nonnegative_seconds(
            self.max_retry_seconds,
            "max_retry_seconds",
        )
        root_timeout_seconds = strict_nonnegative_seconds(
            self.root_timeout_seconds,
            "root_timeout_seconds",
        )
        try:
            self.http_bounds()
        except ValueError:
            raise ValueError(
                "provider_directory_rooted_graph_acquisition_config_invalid"
            ) from None
        if (
            type(self.enabled) is not bool
            or type(self.concurrency) is not int
            or not 1
            <= self.concurrency
            <= PROVIDER_DIRECTORY_ROOTED_GRAPH_ACQUISITION_MAX_CONCURRENCY
            or type(self.max_attempts) is not int
            or not 1
            <= self.max_attempts
            <= PROVIDER_DIRECTORY_ROOTED_GRAPH_ACQUISITION_MAX_ATTEMPTS
            or type(self.lease_seconds) is not int
            or not 30 <= self.lease_seconds <= 3600
            or not 0 < heartbeat_seconds <= self.lease_seconds / 2
            or retry_base_seconds <= 0
            or retry_base_seconds > max_retry_seconds
            or max_retry_seconds
            > PROVIDER_DIRECTORY_ROOTED_GRAPH_ACQUISITION_MAX_RETRY_SECONDS
            or not 0
            < root_timeout_seconds
            <= PROVIDER_DIRECTORY_ROOTED_GRAPH_ACQUISITION_MAX_ROOT_TIMEOUT_SECONDS
        ):
            raise ValueError(
                "provider_directory_rooted_graph_acquisition_config_invalid"
            )

    def http_bounds(self) -> ProviderDirectoryRootedGraphHTTPBounds:
        """Project transport caps without widening any hard maximum."""

        return ProviderDirectoryRootedGraphHTTPBounds(
            max_page_bytes=self.max_page_bytes,
            max_query_bytes=self.max_query_bytes,
            max_pages=self.max_pages,
            max_resources=self.max_resources,
            max_url_bytes=self.max_url_bytes,
            timeout_seconds=self.timeout_seconds,
        )


@dataclass(frozen=True, slots=True, repr=False)
class ProviderDirectoryRootedGraphInputSnapshot:
    """Exact current endpoint/root evidence plus acquisition state."""

    api_base: str
    root_dataset_variant: str
    root_publication_contract_id: str
    root_source_id: str
    root_endpoint_id: str
    acquisition_source_id: str
    acquisition_endpoint_id: str
    source_authority_id: str
    endpoint_signature_sha256: str
    root_dataset_id: str
    root_dataset_hash: str
    root_content_proof_sha256: str
    root_resource_count: int
    max_work_items: int
    max_resource_rows: int
    max_edge_rows: int
    max_payload_bytes: int
    root_cohort_id: str
    acquisition_status: str

    def __post_init__(self) -> None:
        try:
            canonical_api_base = canonical_provider_directory_api_base(self.api_base)
        except ValueError:
            raise ValueError(
                "provider_directory_rooted_graph_input_snapshot_invalid"
            ) from None
        if (
            canonical_api_base != self.api_base
            or self.root_dataset_variant
            not in {"uhc_flex_practitioner", "rooted_combined"}
            or not _is_snapshot_lineage_valid(self)
            or not _is_snapshot_registry_valid(self)
            or not _is_snapshot_root_valid(self)
            or not _is_snapshot_budget_valid(self)
            or self.acquisition_status not in ACQUISITION_STATES
        ):
            raise ValueError("provider_directory_rooted_graph_input_snapshot_invalid")

    def source_identity(self) -> tuple[object, ...]:
        """Return role-neutral evidence used for drift comparisons."""

        return (
            self.api_base,
            self.root_dataset_variant,
            self.root_publication_contract_id,
            self.root_source_id,
            self.root_endpoint_id,
            self.acquisition_source_id,
            self.acquisition_endpoint_id,
            self.source_authority_id,
            self.endpoint_signature_sha256,
            self.root_dataset_id,
            self.root_dataset_hash,
            self.root_content_proof_sha256,
            self.root_resource_count,
            self.root_cohort_id,
            self.max_work_items,
            self.max_resource_rows,
            self.max_edge_rows,
            self.max_payload_bytes,
        )

    def is_identity_match(
        self,
        identity: ProviderDirectoryRootedGraphAcquisitionIdentity,
    ) -> bool:
        """Check every snapshot field bound into an acquisition identity."""

        return bool(
            type(identity) is ProviderDirectoryRootedGraphAcquisitionIdentity
            and self.root_dataset_variant == identity.root_dataset_variant
            and self.root_publication_contract_id
            == identity.root_publication_contract_id
            and self.root_source_id == identity.root_source_id
            and self.root_endpoint_id == identity.root_endpoint_id
            and self.acquisition_source_id == identity.acquisition_source_id
            and self.acquisition_endpoint_id == identity.acquisition_endpoint_id
            and self.source_authority_id == identity.source_authority_id
            and self.endpoint_signature_sha256 == identity.endpoint_signature_sha256
            and self.root_dataset_id == identity.root_dataset_id
            and self.root_dataset_hash == identity.root_dataset_hash
            and self.root_content_proof_sha256 == identity.root_content_proof_sha256
            and self.root_resource_count == identity.root_resource_count
            and self.root_cohort_id == identity.root_cohort_id
            and self.max_work_items == identity.max_work_items
            and self.max_resource_rows == identity.max_resource_rows
            and self.max_edge_rows == identity.max_edge_rows
            and self.max_payload_bytes == identity.max_payload_bytes
        )


@dataclass(frozen=True, slots=True, repr=False)
class ProviderDirectoryRootedGraphRootReceipt:
    """Compact evidence that one role reached an error-free sealed graph."""

    acquisition_role: str
    acquisition_id: str = field(repr=False)
    run_id: str = field(repr=False)
    completed_count: int
    resource_count: int
    edge_count: int
    rooted_graph_sha256: str = field(repr=False)
    elapsed_seconds: float

    def __post_init__(self) -> None:
        counts = (self.completed_count, self.resource_count, self.edge_count)
        if (
            self.acquisition_role not in ROOT_ROLES
            or type(self.acquisition_id) is not str
            or ACQUISITION_PATTERN.fullmatch(self.acquisition_id) is None
            or type(self.run_id) is not str
            or RUN_PATTERN.fullmatch(self.run_id) is None
            or any(type(count) is not int or count < 0 for count in counts)
            or type(self.rooted_graph_sha256) is not str
            or SHA256_PATTERN.fullmatch(self.rooted_graph_sha256) is None
        ):
            raise ValueError("provider_directory_rooted_graph_root_receipt_invalid")
        strict_nonnegative_seconds(self.elapsed_seconds, "root_elapsed_seconds")


@dataclass(frozen=True, slots=True, repr=False)
class ProviderDirectoryRootedGraphAcquisitionReceipt:
    """Sequential baseline/candidate result with no publication authority."""

    scope_id: str = field(repr=False)
    dataset_intent_id: str = field(repr=False)
    baseline: ProviderDirectoryRootedGraphRootReceipt
    candidate: ProviderDirectoryRootedGraphRootReceipt
    rooted_graphs_match: bool
    elapsed_seconds: float

    def __post_init__(self) -> None:
        if (
            type(self.scope_id) is not str
            or ROOTED_GRAPH_SCOPE_PATTERN.fullmatch(self.scope_id) is None
            or type(self.dataset_intent_id) is not str
            or INTENT_PATTERN.fullmatch(self.dataset_intent_id) is None
            or type(self.baseline) is not ProviderDirectoryRootedGraphRootReceipt
            or self.baseline.acquisition_role != "baseline"
            or type(self.candidate) is not ProviderDirectoryRootedGraphRootReceipt
            or self.candidate.acquisition_role != "candidate"
            or self.baseline.acquisition_id == self.candidate.acquisition_id
            or self.baseline.run_id == self.candidate.run_id
            or type(self.rooted_graphs_match) is not bool
            or self.rooted_graphs_match
            != (self.baseline.rooted_graph_sha256 == self.candidate.rooted_graph_sha256)
        ):
            raise ValueError("provider_directory_rooted_graph_receipt_invalid")
        strict_nonnegative_seconds(self.elapsed_seconds, "elapsed_seconds")


@dataclass(frozen=True, slots=True, repr=False)
class ProviderDirectoryRootedGraphAcquisitionDependencies:
    """Narrow injection surface for pure acquisition-runtime tests."""

    revalidate_inputs: Callable[..., Awaitable[Any]]
    initialize_root: Callable[..., Awaitable[Any]]
    claim_work: Callable[..., Awaitable[Any]]
    claim_census: Callable[..., Awaitable[Any]]
    census_state: Callable[..., Awaitable[Any]]
    fetch: Callable[..., Awaitable[Any]]
    heartbeat: Callable[..., Awaitable[Any]]
    complete_result: Callable[..., Awaitable[Any]]
    complete_missing: Callable[..., Awaitable[Any]]
    complete_error: Callable[..., Awaitable[Any]]
    release_work: Callable[..., Awaitable[Any]]
    seal_root: Callable[..., Awaitable[Any]]
    session_scope: Callable[[int], Any]
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep
    monotonic: Callable[[], float] = time.monotonic


_PUBLIC_MODULE = "process.provider_directory_rooted_graph_acquisition"
ProviderDirectoryRootedGraphAcquisitionError.__module__ = _PUBLIC_MODULE
ProviderDirectoryRootedGraphAcquisitionConfig.__module__ = _PUBLIC_MODULE
ProviderDirectoryRootedGraphInputSnapshot.__module__ = _PUBLIC_MODULE
ProviderDirectoryRootedGraphRootReceipt.__module__ = _PUBLIC_MODULE
ProviderDirectoryRootedGraphAcquisitionReceipt.__module__ = _PUBLIC_MODULE
ProviderDirectoryRootedGraphAcquisitionDependencies.__module__ = _PUBLIC_MODULE


__all__ = (
    "ProviderDirectoryRootedGraphAcquisitionConfig",
    "ProviderDirectoryRootedGraphAcquisitionDependencies",
    "ProviderDirectoryRootedGraphAcquisitionError",
    "ProviderDirectoryRootedGraphAcquisitionReceipt",
    "ProviderDirectoryRootedGraphInputSnapshot",
    "ProviderDirectoryRootedGraphRootReceipt",
    "PROVIDER_DIRECTORY_ROOTED_GRAPH_ACQUISITION_DEFAULT_ATTEMPTS",
    "PROVIDER_DIRECTORY_ROOTED_GRAPH_ACQUISITION_DEFAULT_CONCURRENCY",
    "PROVIDER_DIRECTORY_ROOTED_GRAPH_ACQUISITION_MAX_ATTEMPTS",
    "PROVIDER_DIRECTORY_ROOTED_GRAPH_ACQUISITION_MAX_CONCURRENCY",
    "PROVIDER_DIRECTORY_ROOTED_GRAPH_ACQUISITION_DEFAULT_ROOT_TIMEOUT_SECONDS",
    "PROVIDER_DIRECTORY_ROOTED_GRAPH_ACQUISITION_MAX_ROOT_TIMEOUT_SECONDS",
)
