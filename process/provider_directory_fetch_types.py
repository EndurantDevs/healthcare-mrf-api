# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Value and state records shared by Provider Directory fetch paths."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

import aiohttp


@dataclass(frozen=True)
class ResourceFetchResult:
    model: type
    rows: list[dict[str, Any]]
    rows_fetched: int
    rows_written: int
    pages_fetched: int
    complete: bool
    row_limit_reached: bool
    page_limit_reached: bool
    hard_page_limit_reached: bool
    next_url_remaining: bool
    error: str | None = None
    fetch_mode: str = "paged"
    deadline_reached: bool = False
    retry_not_before: str | None = None
    fetch_diagnostic: dict[str, Any] | None = None
    pagination_cooldown_retries: int = 0
    pagination_cooldown_wait_seconds: float = 0.0
    is_pagination_cooldown_recovered: bool = False
    is_pagination_cooldown_exhausted: bool = False
    is_pagination_cooldown_deadline_blocked: bool = False
    source_fetch_elapsed_ms: int = 0
    stream_write_elapsed_seconds: float = 0.0
    checkpoint_persist_elapsed_seconds: float = 0.0
    page_prefetch_eligible: bool = False
    page_prefetch_started: int = 0
    page_prefetch_consumed: int = 0
    page_prefetch_discarded: int = 0
    page_prefetch_wait_seconds: float = 0.0

    @property
    def is_bounded(self) -> bool:
        """Return a fetch result constrained to the requested row limit."""
        return (
            self.row_limit_reached or self.page_limit_reached or self.hard_page_limit_reached or self.deadline_reached
        )


@dataclass(frozen=True)
class RestPagePrefetchEligibility:
    checkpoint_context: PaginationCheckpointContext | None
    start_url_count: int
    is_partitioned_fetch: bool
    row_batch_handler: Callable[[type, list[dict[str, Any]]], Awaitable[int]] | None
    row_batch_size: int
    retain_rows: bool
    per_resource_limit: int
    page_limit: int


@dataclass(frozen=True)
class ResourceScanConcurrencyOptions:
    resource_types: tuple[str, ...]
    requested_concurrency: int
    per_resource_limit: int
    page_limit: int
    page_count: int
    stream_batch_size: int
    source_concurrency: int
    linked_resource_limit: int
    stale_cleanup: bool
    bulk_export: bool
    checkpointing_enabled: bool
    deferred_materialization: bool


@dataclass
class RestPagePrefetchState:
    page_url: str | None = None
    session: aiohttp.ClientSession | None = None
    page_task: asyncio.Task[tuple[int | None, dict[str, Any] | None, str | None, int] | None] | None = None
    started: int = 0
    consumed: int = 0
    discarded: int = 0
    wait_seconds: float = 0.0


@dataclass(frozen=True)
class RestPaginationCooldownResult:
    fetch_result: tuple[int | None, dict[str, Any] | None, str | None, int]
    retries: int = 0
    wait_seconds: float = 0.0
    recovered: bool = False
    exhausted: bool = False
    deadline_blocked: bool = False


@dataclass(frozen=True)
class PaginationCheckpointContext:
    canonical_api_base: str
    source_scope_hash: str
    source_ids: tuple[str, ...]
    owner_run_id: str
    acquisition_root_run_id: str
    retry_of_run_id: str | None = None
    endpoint_id: str | None = None
    dataset_id: str | None = None
    lineage_verified: bool = False
    restart_expired_current_census_slice: bool = True

    def __post_init__(self) -> None:
        if not self.acquisition_root_run_id.strip():
            raise ValueError("provider_directory_pagination_root_missing")


@dataclass(frozen=True)
class PaginationResumeState:
    next_url: str | None
    pages_processed: int
    rows_processed: int
    recent_url_hashes: tuple[str, ...]
    complete: bool = False
    resumed: bool = False
    completeness: dict[str, Any] = field(default_factory=dict)


@dataclass
class PartitionFetchState:
    """Shared counters for concurrent partitioned FHIR searches."""

    result_model: type
    retained_resource_rows: list[dict[str, Any]]
    fetched_count: int = 0
    written_count: int = 0
    page_count: int = 0
    partition_error_count: int = 0
    error_message: str | None = None
    hard_page_limit_reached: bool = False
    deadline_reached: bool = False
    next_url_remaining: bool = False
    seen_urls: set[str] = field(default_factory=set)
    completed_partition_identities: set[str] = field(default_factory=set)
    pending_partition_checkpoint_rows: list[dict[str, Any]] = field(default_factory=list)
    state_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    writer_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    checkpoint_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    stop_event: asyncio.Event = field(default_factory=asyncio.Event)


@dataclass(frozen=True)
class PartitionFetchOptions:
    timeout: int
    run_id: str | None
    row_batch_handler: Callable[[type, list[dict[str, Any]]], Awaitable[int]] | None
    row_batch_size: int
    retain_rows: bool
    deadline_at: float | None
    max_pages: int
    cancel_ctx: dict[str, Any] | None = None
    cancel_task: dict[str, Any] | None = None


@dataclass(frozen=True)
class PartitionPageFetchResult:
    next_url: str | None
    outcome: str


@dataclass(frozen=True)
class PartitionPageRequest:
    """Inputs shared while fetching one page in a partition URL chain."""

    source_record: dict[str, Any]
    resource_type: str
    start_url_queue: asyncio.Queue[str]
    state: PartitionFetchState
    partition_start_url: str
    current_url: str
    is_residual_only: bool
    pending_resource_rows: list[dict[str, Any]]
    fetch_options: PartitionFetchOptions


@dataclass(frozen=True)
class PartitionChainRequest:
    """Inputs shared while walking one independent partition cursor chain."""

    source_record: dict[str, Any]
    resource_type: str
    start_url_queue: asyncio.Queue[str]
    state: PartitionFetchState
    request_url: str
    fetch_options: PartitionFetchOptions
