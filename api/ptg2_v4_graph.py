# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded reads for packed PTG V4 provider-graph relations."""

from __future__ import annotations

import operator
import os
import struct
import threading
from collections import OrderedDict
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any, Iterable, Iterator, Mapping, TypeVar

from sqlalchemy import text

from api.ptg2_shared_blocks import (
    PTG2SharedBlockError,
    _validated_physical_block,
)
from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.ptg2_v4_snapshot_maps import (
    PTG2_V4_MAP_BLOCK_KIND,
    PTG2_V4_MAP_FORMAT,
    PTG2_V4_MAP_FORMAT_VERSION,
    PTG2_V4_NPI_TABLE,
    PTG2_V4_PROJECTION_ID_SCOPE,
    decode_v4_snapshot_map_pack,
)


try:
    from ptg2_address_canon import ptg2_decode_u32_le as _native_u32_decoder
except (ImportError, AttributeError):
    _native_u32_decoder = None


PTG2_V4_MEMBER_PAGE_BYTES = 16 * 1024
PTG2_V4_MEMBER_WIDTH_BYTES = 4
PTG2_V4_MEMBERS_PER_PAGE = (
    PTG2_V4_MEMBER_PAGE_BYTES // PTG2_V4_MEMBER_WIDTH_BYTES
)
PTG2_V4_LOCATOR_WIDTH_BYTES = 12
PTG2_V4_LOCATORS_PER_PAGE = (
    PTG2_V4_MEMBER_PAGE_BYTES // PTG2_V4_LOCATOR_WIDTH_BYTES
)
PTG2_V4_MAX_MAP_PACK_RAW_BYTES = 4 * 1024 * 1024
PTG2_V4_MAX_GRAPH_PAGE_BYTES = 4 * 1024 * 1024
PTG2_V4_HOT_NPI_RELATION = "group_npis_exact"
PTG2_V4_NPI_DICTIONARY_ENTRY_BYTES = 16
PTG2_V4_HEAVY_BITMAP_MAGIC = b"PTG2V4BM"
PTG2_V4_HEAVY_BITMAP_HEADER_BYTES = 24
PTG2_V4_HEAVY_BITMAP_FRAGMENT_MAGIC = b"PTG2V4BF"
PTG2_V4_HEAVY_BITMAP_FRAGMENT_HEADER_BYTES = 32
_SET_BIT_POSITIONS = tuple(
    tuple(bit for bit in range(8) if value & (1 << bit))
    for value in range(256)
)

PTG2_V4_COMMON_RELATIONS = frozenset(
    {
        "set_components",
        "component_groups",
        "npi_groups_exact",
        "group_npis_exact",
        "set_npi_prefix_override",
    }
)
PTG2_V4_DIRECT_RELATIONS = frozenset(
    {"group_sets_direct", "set_groups_direct"}
)
PTG2_V4_PATTERN_RELATIONS = frozenset(
    {
        "group_patterns",
        "pattern_groups",
        "pattern_sets",
        "set_patterns",
        "npi_patterns",
    }
)
PTG2_V4_RELATIONS = (
    PTG2_V4_COMMON_RELATIONS
    | PTG2_V4_DIRECT_RELATIONS
    | PTG2_V4_PATTERN_RELATIONS
)
PTG2_V4_ORDER_PRESERVING_RELATIONS = frozenset(
    {"set_npi_prefix_override"}
)
PTG2_V4_HOT_SOURCE_RELATIONS = frozenset(
    {
        "set_groups_direct",
        "set_patterns",
        "set_components",
        "pattern_groups",
        "component_groups",
    }
)


def _env_positive_int(name: str, default: int) -> int:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    try:
        parsed = int(str(raw_value).strip())
    except ValueError:
        return default
    return parsed if parsed > 0 else default


_MAP_CACHE_MAX_BYTES = _env_positive_int(
    "HLTHPRT_PTG2_V4_MAP_CACHE_BYTES", 64 * 1024 * 1024
)
_BLOCK_CACHE_MAX_BYTES = _env_positive_int(
    "HLTHPRT_PTG2_V4_BLOCK_CACHE_BYTES", 256 * 1024 * 1024
)
_ROOT_CACHE_MAX_ENTRIES = _env_positive_int(
    "HLTHPRT_PTG2_V4_ROOT_CACHE_ENTRIES", 1024
)
_HEAVY_OWNER_CACHE_MAX_ENTRIES = _env_positive_int(
    "HLTHPRT_PTG2_V4_HEAVY_OWNER_CACHE_ENTRIES", 16_384
)
_HEAVY_OWNER_NEGATIVE_CACHE_MAX_ENTRIES = _env_positive_int(
    "HLTHPRT_PTG2_V4_HEAVY_OWNER_NEGATIVE_CACHE_ENTRIES", 16_384
)


T = TypeVar("T")


class _ByteLRU:
    """Small process-local immutable-block cache bounded by retained bytes."""

    def __init__(self, maximum_bytes: int) -> None:
        self._maximum_bytes = max(int(maximum_bytes), 1)
        self._retained_bytes = 0
        self._values: OrderedDict[Any, tuple[int, Any]] = OrderedDict()

    def get(self, key: Any) -> Any | None:
        """Return one cached value and refresh its recency."""

        item = self._values.get(key)
        if item is None:
            return None
        self._values.move_to_end(key)
        return item[1]

    def put(self, key: Any, value: Any, retained_bytes: int) -> None:
        """Store one value while keeping retained bytes within the limit."""

        normalized_bytes = max(int(retained_bytes), 0)
        previous = self._values.pop(key, None)
        if previous is not None:
            self._retained_bytes -= previous[0]
        if normalized_bytes > self._maximum_bytes:
            return
        self._values[key] = (normalized_bytes, value)
        self._retained_bytes += normalized_bytes
        while self._retained_bytes > self._maximum_bytes and self._values:
            _evicted_key, (evicted_bytes, _evicted_value) = self._values.popitem(
                last=False
            )
            self._retained_bytes -= evicted_bytes


@dataclass(frozen=True)
class V4GraphRoot:
    snapshot_key: int
    representation: str
    map_digest: bytes


@dataclass(frozen=True)
class V4RelationManifest:
    """Authoritative snapshot-local geometry for one packed relation."""

    snapshot_key: int
    relation: str
    member_object_kind: str
    locator_object_kind: str
    owner_base: int
    owner_count: int
    logical_member_count: int
    vector_member_count: int
    member_width: int
    member_page_bytes: int
    locator_page_bytes: int
    locator_owner_span: int

    @property
    def members_per_page(self) -> int:
        """Return the exact packed-member capacity of one page."""

        return self.member_page_bytes // self.member_width


@dataclass(frozen=True)
class V4HeavyOwner:
    """Exact manifest entry for one owner with a smaller bitmap encoding."""

    relation: str
    owner_key: int
    object_kind: str
    member_count: int
    member_base: int
    member_span: int
    fragment_count: int


@dataclass(frozen=True)
class _CachedPhysicalBlock:
    block_hash: bytes
    object_kind: str
    entry_count: int
    payload: bytes


@dataclass
class _V4GraphRequestIO:
    database_bytes: int = 0
    database_blocks: int = 0
    cache_hit_bytes: int = 0
    logical_lookups: int = 0
    bitmap_owner_hits: int = 0
    component_fallback_sets: int = 0
    hot_prefix_requests: int = 0
    cold_exact_requests: int = 0
    npi_prefix_override_sets: int = 0
    hot_group_npi_members: int = 0
    hot_group_npi_locator_pages: int = 0
    hot_group_npi_member_pages: int = 0
    hot_group_npi_bytes: int = 0
    hot_group_npi_batches: int = 0
    hot_npi_dictionary_reads: int = 0
    provider_expansion_rate_rows: int = 0
    provider_expansion_provider_sets: int = 0
    provider_expansion_graph_batches: int = 0
    provider_expansion_rejections: int = 0


@dataclass
class _V4HotSourceWork:
    """Request-local source expansion admitted by the sealed snapshot caps."""

    maximum_owners: int
    maximum_members: int
    maximum_pages: int
    maximum_bytes: int
    owners: int = 0
    members: int = 0
    pages: int = 0
    bytes: int = 0

    def charge(
        self,
        *,
        owner_count: int,
        member_count: int,
        page_count: int,
        byte_count: int,
    ) -> None:
        """Charge one source expansion and reject work above sealed limits."""

        self.owners += max(int(owner_count), 0)
        self.members += max(int(member_count), 0)
        self.pages += max(int(page_count), 0)
        self.bytes += max(int(byte_count), 0)
        if (
            self.owners > self.maximum_owners
            or self.members > self.maximum_members
            or self.pages > self.maximum_pages
            or self.bytes > self.maximum_bytes
        ):
            raise PTG2SharedBlockError(
                "PTG V4 hot provider source work exceeds its sealed limit"
            )


@dataclass
class _V4HotNpiWork:
    """Request-local second-hop work admitted by sealed snapshot caps."""

    maximum_members: int
    maximum_locator_pages: int
    maximum_member_pages: int
    maximum_bytes: int
    maximum_batches: int
    members: int = 0
    locator_pages: int = 0
    member_pages: int = 0
    bytes: int = 0
    batches: int = 0

    def charge(
        self,
        *,
        member_count: int = 0,
        locator_page_count: int = 0,
        member_page_count: int = 0,
        byte_count: int = 0,
        batch_count: int = 0,
    ) -> None:
        """Charge one exact group-to-NPI read and reject work above the seal."""

        self.members += max(int(member_count), 0)
        self.locator_pages += max(int(locator_page_count), 0)
        self.member_pages += max(int(member_page_count), 0)
        self.bytes += max(int(byte_count), 0)
        self.batches += max(int(batch_count), 0)
        if (
            self.members > self.maximum_members
            or self.locator_pages > self.maximum_locator_pages
            or self.member_pages > self.maximum_member_pages
            or self.bytes > self.maximum_bytes
            or self.batches > self.maximum_batches
        ):
            raise PTG2SharedBlockError(
                "PTG V4 hot group-to-NPI work exceeds its sealed limit"
            )


class _V4GraphMetrics:
    """Cumulative graph I/O counters with bytes-per-request histogram buckets."""

    _BOUNDS = (0, 4096, 16_384, 65_536, 262_144, 1_048_576, 4_194_304, 16_777_216)

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._request_count = 0
        self._database_bytes = 0
        self._database_blocks = 0
        self._cache_hit_bytes = 0
        self._logical_lookups = 0
        self._bitmap_owner_hits = 0
        self._component_fallback_sets = 0
        self._hot_prefix_requests = 0
        self._cold_exact_requests = 0
        self._npi_prefix_override_sets = 0
        self._hot_group_npi_members = 0
        self._hot_group_npi_locator_pages = 0
        self._hot_group_npi_member_pages = 0
        self._hot_group_npi_bytes = 0
        self._hot_group_npi_batches = 0
        self._hot_npi_dictionary_reads = 0
        self._provider_expansion_rate_rows = 0
        self._provider_expansion_provider_sets = 0
        self._provider_expansion_graph_batches = 0
        self._provider_expansion_rejections = 0
        self._bucket_counts = {bound: 0 for bound in self._BOUNDS}
        self._infinite_bucket_count = 0

    def observe(self, request_io: _V4GraphRequestIO) -> None:
        """Accumulate one completed request's bounded I/O counters."""

        byte_count = max(int(request_io.database_bytes), 0)
        with self._lock:
            self._request_count += 1
            self._database_bytes += byte_count
            self._database_blocks += max(int(request_io.database_blocks), 0)
            self._cache_hit_bytes += max(int(request_io.cache_hit_bytes), 0)
            self._logical_lookups += max(int(request_io.logical_lookups), 0)
            self._bitmap_owner_hits += max(int(request_io.bitmap_owner_hits), 0)
            self._component_fallback_sets += max(
                int(request_io.component_fallback_sets),
                0,
            )
            self._hot_prefix_requests += max(
                int(request_io.hot_prefix_requests),
                0,
            )
            self._cold_exact_requests += max(
                int(request_io.cold_exact_requests),
                0,
            )
            self._npi_prefix_override_sets += max(
                int(request_io.npi_prefix_override_sets),
                0,
            )
            self._hot_group_npi_members += max(
                int(request_io.hot_group_npi_members),
                0,
            )
            self._hot_group_npi_locator_pages += max(
                int(request_io.hot_group_npi_locator_pages),
                0,
            )
            self._hot_group_npi_member_pages += max(
                int(request_io.hot_group_npi_member_pages),
                0,
            )
            self._hot_group_npi_bytes += max(
                int(request_io.hot_group_npi_bytes),
                0,
            )
            self._hot_group_npi_batches += max(
                int(request_io.hot_group_npi_batches),
                0,
            )
            self._hot_npi_dictionary_reads += max(
                int(request_io.hot_npi_dictionary_reads),
                0,
            )
            self._observe_provider_expansion(request_io)
            for bound in self._BOUNDS:
                if byte_count <= bound:
                    self._bucket_counts[bound] += 1
            self._infinite_bucket_count += 1

    def _observe_provider_expansion(
        self,
        request_io: _V4GraphRequestIO,
    ) -> None:
        """Accumulate the provider work charged inside the request scope."""

        self._provider_expansion_rate_rows += max(
            int(request_io.provider_expansion_rate_rows),
            0,
        )
        self._provider_expansion_provider_sets += max(
            int(request_io.provider_expansion_provider_sets),
            0,
        )
        self._provider_expansion_graph_batches += max(
            int(request_io.provider_expansion_graph_batches),
            0,
        )
        self._provider_expansion_rejections += max(
            int(request_io.provider_expansion_rejections),
            0,
        )

    def snapshot(self) -> dict[str, Any]:
        """Return an immutable metrics view for API instrumentation."""

        with self._lock:
            return {
                "request_count": self._request_count,
                "database_bytes": self._database_bytes,
                "database_blocks": self._database_blocks,
                "cache_hit_bytes": self._cache_hit_bytes,
                "logical_lookups": self._logical_lookups,
                "bitmap_owner_hits": self._bitmap_owner_hits,
                "component_fallback_sets": self._component_fallback_sets,
                "hot_prefix_requests": self._hot_prefix_requests,
                "cold_exact_requests": self._cold_exact_requests,
                "npi_prefix_override_sets": self._npi_prefix_override_sets,
                "hot_group_npi_members": self._hot_group_npi_members,
                "hot_group_npi_locator_pages": self._hot_group_npi_locator_pages,
                "hot_group_npi_member_pages": self._hot_group_npi_member_pages,
                "hot_group_npi_bytes": self._hot_group_npi_bytes,
                "hot_group_npi_batches": self._hot_group_npi_batches,
                "hot_npi_dictionary_reads": self._hot_npi_dictionary_reads,
                "provider_expansion_rate_rows": (
                    self._provider_expansion_rate_rows
                ),
                "provider_expansion_provider_sets": (
                    self._provider_expansion_provider_sets
                ),
                "provider_expansion_graph_batches": (
                    self._provider_expansion_graph_batches
                ),
                "provider_expansion_rejections": (
                    self._provider_expansion_rejections
                ),
                "buckets": dict(self._bucket_counts),
                "infinite_bucket_count": self._infinite_bucket_count,
            }


_MAP_COORDINATE_CACHE = _ByteLRU(_MAP_CACHE_MAX_BYTES)
_PHYSICAL_BLOCK_CACHE = _ByteLRU(_BLOCK_CACHE_MAX_BYTES)
_ROOT_CACHE: OrderedDict[tuple[str, int], V4GraphRoot] = OrderedDict()
_RELATION_CACHE: OrderedDict[
    tuple[str, int, str], V4RelationManifest
] = OrderedDict()
_HEAVY_OWNER_CACHE: OrderedDict[
    tuple[str, int, str, int], V4HeavyOwner
] = OrderedDict()
_HEAVY_OWNER_NEGATIVE_CACHE: OrderedDict[
    tuple[str, int, str, int], None
] = OrderedDict()
_V4_GRAPH_METRICS = _V4GraphMetrics()
_ACTIVE_V4_GRAPH_REQUEST_IO: ContextVar[_V4GraphRequestIO | None] = ContextVar(
    "active_v4_graph_request_io", default=None
)
_ACTIVE_V4_HOT_SOURCE_WORK: ContextVar[_V4HotSourceWork | None] = ContextVar(
    "active_v4_hot_source_work", default=None
)
_ACTIVE_V4_HOT_NPI_WORK: ContextVar[_V4HotNpiWork | None] = ContextVar(
    "active_v4_hot_npi_work", default=None
)


@contextmanager
def v4_graph_request_scope() -> Iterator[_V4GraphRequestIO]:
    """Aggregate all V4 graph reads into one API-request metric observation."""

    active = _ACTIVE_V4_GRAPH_REQUEST_IO.get()
    if active is not None:
        yield active
        return
    request_io = _V4GraphRequestIO()
    token = _ACTIVE_V4_GRAPH_REQUEST_IO.set(request_io)
    try:
        yield request_io
    finally:
        _ACTIVE_V4_GRAPH_REQUEST_IO.reset(token)
        _V4_GRAPH_METRICS.observe(request_io)


def v4_graph_metrics_snapshot() -> dict[str, Any]:
    """Return a stable copy of cumulative V4 graph read metrics."""

    return _V4_GRAPH_METRICS.snapshot()


def record_v4_component_fallback_sets(set_count: int) -> None:
    """Record bounded component-fallback selections in the active request."""

    active_request = _ACTIVE_V4_GRAPH_REQUEST_IO.get()
    if active_request is not None:
        active_request.component_fallback_sets += max(int(set_count), 0)


def record_v4_hot_prefix_request() -> None:
    """Record that the public request selected the bounded V4 hot path."""

    active_request = _ACTIVE_V4_GRAPH_REQUEST_IO.get()
    if active_request is not None:
        active_request.hot_prefix_requests = 1


def record_v4_cold_exact_request() -> None:
    """Record that the public request selected exact cold traversal."""

    active_request = _ACTIVE_V4_GRAPH_REQUEST_IO.get()
    if active_request is not None:
        active_request.cold_exact_requests = 1


def record_v4_prefix_override_sets(set_count: int) -> None:
    """Record exact sparse-prefix owners used by the current request."""

    active_request = _ACTIVE_V4_GRAPH_REQUEST_IO.get()
    if active_request is not None:
        active_request.npi_prefix_override_sets += max(int(set_count), 0)


record_v4_npi_prefix_override_sets = record_v4_prefix_override_sets


def record_v4_provider_expansion_work(
    *,
    rate_rows: int = 0,
    provider_sets: int = 0,
    graph_batches: int = 0,
    rejections: int = 0,
) -> None:
    """Record bounded provider-expansion work in the active request scope."""

    active_request = _ACTIVE_V4_GRAPH_REQUEST_IO.get()
    if active_request is None:
        return
    active_request.provider_expansion_rate_rows += max(int(rate_rows), 0)
    active_request.provider_expansion_provider_sets += max(
        int(provider_sets),
        0,
    )
    active_request.provider_expansion_graph_batches += max(
        int(graph_batches),
        0,
    )
    active_request.provider_expansion_rejections += max(int(rejections), 0)


@contextmanager
def v4_graph_hot_source_scope(
    *,
    maximum_owners: int,
    maximum_members: int,
    maximum_pages: int,
    maximum_bytes: int,
) -> Iterator[_V4HotSourceWork]:
    """Enforce sealed physical source-work caps for one bounded traversal."""

    active = _ACTIVE_V4_HOT_SOURCE_WORK.get()
    if active is not None:
        yield active
        return
    limits = (
        int(maximum_owners),
        int(maximum_members),
        int(maximum_pages),
        int(maximum_bytes),
    )
    if any(limit < 0 for limit in limits):
        raise PTG2SharedBlockError("PTG V4 hot source-work limit is negative")
    source_work = _V4HotSourceWork(*limits)
    token = _ACTIVE_V4_HOT_SOURCE_WORK.set(source_work)
    try:
        yield source_work
    finally:
        _ACTIVE_V4_HOT_SOURCE_WORK.reset(token)


def _charge_v4_hot_source_work(
    relation: str,
    *,
    owner_count: int,
    member_count: int,
    page_count: int,
    byte_count: int,
) -> None:
    source_work = _ACTIVE_V4_HOT_SOURCE_WORK.get()
    if source_work is None or relation not in PTG2_V4_HOT_SOURCE_RELATIONS:
        return
    source_work.charge(
        owner_count=owner_count,
        member_count=member_count,
        page_count=page_count,
        byte_count=byte_count,
    )


@contextmanager
def v4_graph_hot_npi_scope(
    *,
    maximum_members: int,
    maximum_locator_pages: int,
    maximum_member_pages: int,
    maximum_bytes: int,
    maximum_batches: int,
) -> Iterator[_V4HotNpiWork]:
    """Enforce sealed physical second-hop caps for one bounded traversal."""

    active = _ACTIVE_V4_HOT_NPI_WORK.get()
    if active is not None:
        yield active
        return
    limits = (
        int(maximum_members),
        int(maximum_locator_pages),
        int(maximum_member_pages),
        int(maximum_bytes),
        int(maximum_batches),
    )
    if any(limit < 0 for limit in limits):
        raise PTG2SharedBlockError("PTG V4 hot group-to-NPI limit is negative")
    npi_work = _V4HotNpiWork(*limits)
    token = _ACTIVE_V4_HOT_NPI_WORK.set(npi_work)
    try:
        yield npi_work
    finally:
        _ACTIVE_V4_HOT_NPI_WORK.reset(token)


def _charge_v4_hot_npi_work(
    relation: str,
    *,
    member_count: int = 0,
    locator_page_count: int = 0,
    member_page_count: int = 0,
    byte_count: int = 0,
    batch_count: int = 0,
    dictionary_read_count: int = 0,
) -> None:
    npi_work = _ACTIVE_V4_HOT_NPI_WORK.get()
    if npi_work is None or relation != PTG2_V4_HOT_NPI_RELATION:
        return
    request_io = _request_io()
    request_io.hot_group_npi_members += max(int(member_count), 0)
    request_io.hot_group_npi_locator_pages += max(
        int(locator_page_count),
        0,
    )
    request_io.hot_group_npi_member_pages += max(
        int(member_page_count),
        0,
    )
    request_io.hot_group_npi_bytes += max(int(byte_count), 0)
    request_io.hot_group_npi_batches += max(int(batch_count), 0)
    request_io.hot_npi_dictionary_reads += max(
        int(dictionary_read_count),
        0,
    )
    npi_work.charge(
        member_count=member_count,
        locator_page_count=locator_page_count,
        member_page_count=member_page_count,
        byte_count=byte_count,
        batch_count=batch_count,
    )


def _request_io() -> _V4GraphRequestIO:
    active = _ACTIVE_V4_GRAPH_REQUEST_IO.get()
    if active is None:
        raise RuntimeError("PTG V4 graph read is missing its request scope")
    return active


def _row_mapping(row: Any) -> dict[str, Any]:
    mapping = getattr(row, "_mapping", None)
    if mapping is not None:
        return dict(mapping)
    if isinstance(row, Mapping):
        return dict(row)
    return dict(row or {})


def _relation_kinds(relation: str) -> tuple[str, str]:
    normalized = str(relation or "").strip().lower()
    if normalized not in PTG2_V4_RELATIONS:
        raise PTG2SharedBlockError(f"unsupported PTG V4 graph relation: {relation!r}")
    return (
        f"v4_{normalized}_locators_v1",
        f"v4_{normalized}_members_v1",
    )


def _validate_relation_for_root(root: V4GraphRoot, relation: str) -> None:
    if relation in PTG2_V4_COMMON_RELATIONS:
        return
    if root.representation == "direct_v1" and relation in PTG2_V4_DIRECT_RELATIONS:
        return
    if root.representation == "pattern_v1" and relation in PTG2_V4_PATTERN_RELATIONS:
        return
    raise PTG2SharedBlockError(
        f"PTG V4 {root.representation} layout does not publish {relation}"
    )


async def load_v4_graph_root(
    session: Any,
    snapshot_key: int,
    *,
    schema_name: str,
) -> V4GraphRoot:
    """Load and fail-close one immutable completed V4 map root."""

    normalized_snapshot_key = int(snapshot_key)
    cache_key = (str(schema_name), normalized_snapshot_key)
    cached = _ROOT_CACHE.get(cache_key)
    if cached is not None:
        _ROOT_CACHE.move_to_end(cache_key)
        return cached
    schema = _quote_ident(schema_name)
    query_result = await session.execute(
        text(
            f"""
            SELECT snapshot_key, representation, map_digest,
                   format_version, map_format, projection_id_scope
              FROM {schema}.ptg2_v4_snapshot_map_root
             WHERE snapshot_key = :snapshot_key
               AND state = 'complete'
            """
        ),
        {"snapshot_key": normalized_snapshot_key},
    )
    root_row = query_result.first()
    fields = _row_mapping(root_row) if root_row is not None else {}
    map_digest = bytes(fields.get("map_digest") or b"")
    if (
        int(fields.get("snapshot_key") or 0) != normalized_snapshot_key
        or int(fields.get("format_version") or 0) != PTG2_V4_MAP_FORMAT_VERSION
        or fields.get("map_format") != PTG2_V4_MAP_FORMAT
        or fields.get("projection_id_scope") != PTG2_V4_PROJECTION_ID_SCOPE
        or str(fields.get("representation") or "")
        not in {"direct_v1", "pattern_v1"}
        or len(map_digest) != 32
    ):
        raise PTG2SharedBlockError("PTG V4 snapshot map root is unavailable or invalid")
    root = V4GraphRoot(
        snapshot_key=normalized_snapshot_key,
        representation=str(fields["representation"]),
        map_digest=map_digest,
    )
    _ROOT_CACHE[cache_key] = root
    _ROOT_CACHE.move_to_end(cache_key)
    while len(_ROOT_CACHE) > _ROOT_CACHE_MAX_ENTRIES:
        _ROOT_CACHE.popitem(last=False)
    return root


def _cache_bounded(
    cache: OrderedDict[Any, T],
    key: Any,
    value: T,
) -> T:
    cache[key] = value
    cache.move_to_end(key)
    while len(cache) > _ROOT_CACHE_MAX_ENTRIES:
        cache.popitem(last=False)
    return value


def _cache_entry_bounded(
    cache: OrderedDict[Any, T],
    key: Any,
    value: T,
    *,
    maximum_entries: int,
) -> T:
    """Retain one immutable scoped value under an explicit LRU bound."""

    cache[key] = value
    cache.move_to_end(key)
    while len(cache) > int(maximum_entries):
        cache.popitem(last=False)
    return value


def _strict_manifest_int(
    fields: Mapping[str, Any],
    name: str,
    *,
    minimum: int = 0,
    maximum: int = 0xFFFFFFFFFFFFFFFF,
) -> int:
    value = fields.get(name)
    if isinstance(value, bool):
        raise PTG2SharedBlockError(f"PTG V4 relation manifest has invalid {name}")
    try:
        normalized = int(value)
    except (TypeError, ValueError) as exc:
        raise PTG2SharedBlockError(
            f"PTG V4 relation manifest has invalid {name}"
        ) from exc
    if not minimum <= normalized <= maximum:
        raise PTG2SharedBlockError(f"PTG V4 relation manifest has invalid {name}")
    return normalized


def _normalized_owner_keys(owner_keys: Iterable[int]) -> tuple[int, ...]:
    normalized_keys: set[int] = set()
    for raw_owner_key in owner_keys:
        if isinstance(raw_owner_key, bool) or not isinstance(raw_owner_key, int):
            raise PTG2SharedBlockError("PTG V4 owner key is not an integer")
        if raw_owner_key < 0 or raw_owner_key > 0xFFFFFFFF:
            raise PTG2SharedBlockError("PTG V4 owner key is outside uint32 range")
        normalized_keys.add(int(raw_owner_key))
    return tuple(sorted(normalized_keys))


async def load_v4_relation_manifest(
    session: Any,
    *,
    snapshot_key: int,
    relation: str,
    schema_name: str,
) -> V4RelationManifest:
    """Load immutable page geometry instead of assuming compiler defaults."""

    normalized_relation = str(relation or "").strip().lower()
    locator_kind, member_kind = _relation_kinds(normalized_relation)
    cache_key = (str(schema_name), int(snapshot_key), normalized_relation)
    cached = _RELATION_CACHE.get(cache_key)
    if cached is not None:
        _RELATION_CACHE.move_to_end(cache_key)
        return cached
    schema = _quote_ident(schema_name)
    query_result = await session.execute(
        text(
            f"""
            SELECT snapshot_key, relation, member_object_kind,
                   locator_object_kind, owner_base, owner_count,
                   logical_member_count, vector_member_count, member_width,
                   member_page_bytes, locator_page_bytes,
                   locator_owner_span
              FROM {schema}.ptg2_v4_relation_manifest
             WHERE snapshot_key = :snapshot_key
               AND relation = :relation
            """
        ),
        {
            "snapshot_key": int(snapshot_key),
            "relation": normalized_relation,
        },
    )
    manifest_row = query_result.first()
    fields = _row_mapping(manifest_row) if manifest_row is not None else {}
    owner_base = _strict_manifest_int(
        fields, "owner_base", maximum=0xFFFFFFFF
    )
    owner_count = _strict_manifest_int(
        fields, "owner_count", maximum=0x1_0000_0000
    )
    logical_member_count = _strict_manifest_int(fields, "logical_member_count")
    vector_member_count = _strict_manifest_int(fields, "vector_member_count")
    member_width = _strict_manifest_int(
        fields, "member_width", minimum=1, maximum=64
    )
    member_page_bytes = _strict_manifest_int(
        fields,
        "member_page_bytes",
        minimum=PTG2_V4_MEMBER_WIDTH_BYTES,
        maximum=PTG2_V4_MAX_GRAPH_PAGE_BYTES,
    )
    locator_page_bytes = _strict_manifest_int(
        fields,
        "locator_page_bytes",
        minimum=PTG2_V4_LOCATOR_WIDTH_BYTES,
        maximum=PTG2_V4_MAX_GRAPH_PAGE_BYTES,
    )
    locator_owner_span = _strict_manifest_int(
        fields, "locator_owner_span", minimum=1, maximum=0xFFFFFFFF
    )
    if (
        _strict_manifest_int(fields, "snapshot_key", minimum=1)
        != int(snapshot_key)
        or fields.get("relation") != normalized_relation
        or fields.get("member_object_kind") != member_kind
        or fields.get("locator_object_kind") != locator_kind
        or member_width != PTG2_V4_MEMBER_WIDTH_BYTES
        or member_page_bytes % member_width
        or locator_page_bytes % PTG2_V4_LOCATOR_WIDTH_BYTES
        or locator_page_bytes
        != locator_owner_span * PTG2_V4_LOCATOR_WIDTH_BYTES
        or owner_base + owner_count > 0x1_0000_0000
        or vector_member_count > logical_member_count
    ):
        raise PTG2SharedBlockError("PTG V4 relation manifest is inconsistent")
    manifest = V4RelationManifest(
        snapshot_key=int(snapshot_key),
        relation=normalized_relation,
        member_object_kind=member_kind,
        locator_object_kind=locator_kind,
        owner_base=owner_base,
        owner_count=owner_count,
        logical_member_count=logical_member_count,
        vector_member_count=vector_member_count,
        member_width=member_width,
        member_page_bytes=member_page_bytes,
        locator_page_bytes=locator_page_bytes,
        locator_owner_span=locator_owner_span,
    )
    return _cache_bounded(_RELATION_CACHE, cache_key, manifest)


async def load_v4_heavy_owners(
    session: Any,
    *,
    snapshot_key: int,
    relation: str,
    owner_keys: Iterable[int],
    schema_name: str,
) -> dict[int, V4HeavyOwner]:
    """Load only requested bitmap-qualified owners with bounded +/- caching."""

    normalized_relation = str(relation or "").strip().lower()
    _relation_kinds(normalized_relation)
    requested = _normalized_owner_keys(owner_keys)
    if not requested:
        return {}
    cache_prefix = (str(schema_name), int(snapshot_key), normalized_relation)
    heavy_by_owner: dict[int, V4HeavyOwner] = {}
    missing_owner_keys: list[int] = []
    for owner_key in requested:
        cache_key = (*cache_prefix, owner_key)
        cached = _HEAVY_OWNER_CACHE.get(cache_key)
        if cached is not None:
            _HEAVY_OWNER_CACHE.move_to_end(cache_key)
            heavy_by_owner[owner_key] = cached
        elif cache_key in _HEAVY_OWNER_NEGATIVE_CACHE:
            _HEAVY_OWNER_NEGATIVE_CACHE.move_to_end(cache_key)
        else:
            missing_owner_keys.append(owner_key)
    if not missing_owner_keys:
        return {
            owner_key: heavy_by_owner[owner_key]
            for owner_key in requested
            if owner_key in heavy_by_owner
        }
    schema = _quote_ident(schema_name)
    query_result = await session.execute(
        text(
            f"""
            SELECT snapshot_key, relation, owner_key, object_kind, member_count,
                   member_base, member_span, fragment_count
              FROM {schema}.ptg2_v4_heavy_owner
             WHERE snapshot_key = :snapshot_key
               AND relation = :relation
               AND owner_key = ANY(CAST(:owner_keys AS bigint[]))
             ORDER BY owner_key
            """
        ),
        {
            "snapshot_key": int(snapshot_key),
            "relation": normalized_relation,
            "owner_keys": list(missing_owner_keys),
        },
    )
    expected_kind = f"v4_{normalized_relation}_heavy_bitmap_v1"
    missing_owner_key_set = set(missing_owner_keys)
    loaded_owner_keys: set[int] = set()
    loaded_by_owner: dict[int, V4HeavyOwner] = {}
    for raw_row in query_result:
        fields = _row_mapping(raw_row)
        owner_key = _strict_manifest_int(
            fields, "owner_key", maximum=0xFFFFFFFF
        )
        member_count = _strict_manifest_int(
            fields, "member_count", maximum=0xFFFFFFFF
        )
        member_base = _strict_manifest_int(
            fields, "member_base", maximum=0xFFFFFFFF
        )
        member_span = _strict_manifest_int(
            fields, "member_span", minimum=1, maximum=0xFFFFFFFF
        )
        fragment_count = _strict_manifest_int(
            fields, "fragment_count", minimum=1, maximum=0x7FFFFFFF
        )
        if (
            _strict_manifest_int(fields, "snapshot_key", minimum=1)
            != int(snapshot_key)
            or fields.get("relation") != normalized_relation
            or fields.get("object_kind") != expected_kind
            or owner_key not in missing_owner_key_set
            or member_count > member_span
            or member_base + member_span > 0x1_0000_0000
            or owner_key in loaded_owner_keys
        ):
            raise PTG2SharedBlockError("PTG V4 heavy-owner manifest is inconsistent")
        loaded_owner_keys.add(owner_key)
        owner = V4HeavyOwner(
            relation=normalized_relation,
            owner_key=owner_key,
            object_kind=expected_kind,
            member_count=member_count,
            member_base=member_base,
            member_span=member_span,
            fragment_count=fragment_count,
        )
        loaded_by_owner[owner_key] = owner
    for owner_key, owner in loaded_by_owner.items():
        heavy_by_owner[owner_key] = owner
        cache_key = (*cache_prefix, owner_key)
        _HEAVY_OWNER_NEGATIVE_CACHE.pop(cache_key, None)
        _cache_entry_bounded(
            _HEAVY_OWNER_CACHE,
            cache_key,
            owner,
            maximum_entries=_HEAVY_OWNER_CACHE_MAX_ENTRIES,
        )
    for owner_key in missing_owner_key_set - loaded_owner_keys:
        cache_key = (*cache_prefix, owner_key)
        _HEAVY_OWNER_CACHE.pop(cache_key, None)
        _cache_entry_bounded(
            _HEAVY_OWNER_NEGATIVE_CACHE,
            cache_key,
            None,
            maximum_entries=_HEAVY_OWNER_NEGATIVE_CACHE_MAX_ENTRIES,
        )
    return {
        owner_key: heavy_by_owner[owner_key]
        for owner_key in requested
        if owner_key in heavy_by_owner
    }


async def _load_map_coordinate_pairs(
    session: Any,
    *,
    schema_name: str,
    snapshot_key: int,
    object_kind: str,
    coordinate_pairs: Iterable[tuple[int, int]],
) -> dict[tuple[int, int], Any]:
    """Load exact packed-map coordinates with bounded immutable caching."""

    requested_pairs = tuple(
        sorted(
            {
                (int(block_key), int(fragment_no))
                for block_key, fragment_no in coordinate_pairs
            }
        )
    )
    if any(
        block_key < 0 or fragment_no < 0
        for block_key, fragment_no in requested_pairs
    ):
        raise PTG2SharedBlockError("PTG V4 graph coordinate is negative")
    coordinates_by_pair: dict[tuple[int, int], Any] = {}
    missing_pairs: list[tuple[int, int]] = []
    for block_key, fragment_no in requested_pairs:
        cache_key = (
            str(schema_name),
            int(snapshot_key),
            object_kind,
            block_key,
            fragment_no,
        )
        cached = _MAP_COORDINATE_CACHE.get(cache_key)
        if cached is None:
            missing_pairs.append((block_key, fragment_no))
        else:
            coordinates_by_pair[(block_key, fragment_no)] = cached
    if not missing_pairs:
        return coordinates_by_pair

    schema = _quote_ident(schema_name)
    missing_pair_set = set(missing_pairs)
    missing_block_keys = [pair[0] for pair in missing_pairs]
    missing_fragment_nos = [pair[1] for pair in missing_pairs]
    query_result = await session.execute(
        text(
            f"""
            SELECT pack.pack_no, pack.first_block_key, pack.first_fragment_no,
                   pack.last_block_key, pack.last_fragment_no,
                   pack.coordinate_count, pack.map_block_hash AS block_hash,
                   block.format_version, block.object_kind, block.codec,
                   block.entry_count AS block_entry_count,
                   block.raw_byte_count, block.stored_byte_count, block.payload
              FROM {schema}.ptg2_v4_snapshot_map_pack AS pack
              JOIN {schema}.ptg2_v3_block AS block
                ON block.block_hash = pack.map_block_hash
             WHERE pack.snapshot_key = :snapshot_key
               AND pack.object_kind = :object_kind
               AND EXISTS (
                   SELECT 1
                     FROM unnest(
                              CAST(:block_keys AS bigint[]),
                              CAST(:fragment_nos AS integer[])
                          ) AS wanted(block_key, fragment_no)
                    WHERE ROW(wanted.block_key, wanted.fragment_no)
                          BETWEEN ROW(
                              pack.first_block_key,
                              pack.first_fragment_no
                          ) AND ROW(
                              pack.last_block_key,
                              pack.last_fragment_no
                          )
               )
             ORDER BY pack.pack_no
            """
        ),
        {
            "snapshot_key": int(snapshot_key),
            "object_kind": object_kind,
            "block_keys": missing_block_keys,
            "fragment_nos": missing_fragment_nos,
        },
    )
    observed_pack_nos: set[int] = set()
    for raw_row in query_result:
        map_pack_row = _row_mapping(raw_row)
        pack_no = int(map_pack_row.get("pack_no") or 0)
        if pack_no in observed_pack_nos:
            raise PTG2SharedBlockError("PTG V4 map query returned a duplicate pack")
        observed_pack_nos.add(pack_no)
        physical = _validated_physical_block(
            map_pack_row,
            expected_kind=PTG2_V4_MAP_BLOCK_KIND,
            maximum_raw_bytes=PTG2_V4_MAX_MAP_PACK_RAW_BYTES,
        )
        _request_io().database_bytes += int(
            map_pack_row.get("stored_byte_count") or 0
        )
        _request_io().database_blocks += 1
        try:
            coordinates = decode_v4_snapshot_map_pack(
                physical.payload,
                expected_object_kind=object_kind,
            )
        except ValueError as exc:
            raise PTG2SharedBlockError(str(exc)) from exc
        if len(coordinates) != int(map_pack_row.get("coordinate_count") or 0):
            raise PTG2SharedBlockError("PTG V4 map pack count does not match its root")
        for coordinate in coordinates:
            pair = (int(coordinate.block_key), int(coordinate.fragment_no))
            cache_key = (
                str(schema_name),
                int(snapshot_key),
                object_kind,
                pair[0],
                pair[1],
            )
            _MAP_COORDINATE_CACHE.put(cache_key, coordinate, 96)
            if pair in missing_pair_set:
                coordinates_by_pair[pair] = coordinate
    if set(coordinates_by_pair) != set(requested_pairs):
        raise PTG2SharedBlockError("PTG V4 snapshot map is missing a graph coordinate")
    return coordinates_by_pair


async def _load_map_coordinates(
    session: Any,
    *,
    schema_name: str,
    snapshot_key: int,
    object_kind: str,
    block_keys: Iterable[int],
) -> dict[int, Any]:
    requested_keys = tuple(sorted({int(block_key) for block_key in block_keys}))
    coordinate_by_pair = await _load_map_coordinate_pairs(
        session,
        schema_name=schema_name,
        snapshot_key=snapshot_key,
        object_kind=object_kind,
        coordinate_pairs=((block_key, 0) for block_key in requested_keys),
    )
    return {
        block_key: coordinate_by_pair[(block_key, 0)]
        for block_key in requested_keys
    }


async def _load_physical_blocks(
    session: Any,
    *,
    schema_name: str,
    object_kind: str,
    coordinates: Iterable[Any],
    maximum_raw_bytes: int,
) -> dict[bytes, _CachedPhysicalBlock]:
    """Fetch and authenticate distinct physical CAS blocks once per request."""

    coordinate_by_hash: dict[bytes, Any] = {}
    for coordinate in coordinates:
        block_hash = bytes(coordinate.block_hash)
        previous = coordinate_by_hash.setdefault(block_hash, coordinate)
        if int(previous.entry_count) != int(coordinate.entry_count):
            raise PTG2SharedBlockError("PTG V4 aliased block has conflicting counts")
    blocks_by_hash: dict[bytes, _CachedPhysicalBlock] = {}
    missing_hashes: list[bytes] = []
    for block_hash, coordinate in coordinate_by_hash.items():
        cached = _PHYSICAL_BLOCK_CACHE.get(block_hash)
        if cached is None:
            missing_hashes.append(block_hash)
            continue
        if (
            cached.object_kind != object_kind
            or cached.entry_count != int(coordinate.entry_count)
        ):
            raise PTG2SharedBlockError("PTG V4 cached block identity is inconsistent")
        blocks_by_hash[block_hash] = cached
        _request_io().cache_hit_bytes += len(cached.payload)

    if missing_hashes:
        schema = _quote_ident(schema_name)
        query_result = await session.execute(
            text(
                f"""
                SELECT block_hash, format_version, object_kind, codec,
                       entry_count AS block_entry_count,
                       raw_byte_count, stored_byte_count, payload
                  FROM {schema}.ptg2_v3_block
                 WHERE block_hash = ANY(CAST(:block_hashes AS bytea[]))
                """
            ),
            {"block_hashes": missing_hashes},
        )
        for raw_row in query_result:
            block_row = _row_mapping(raw_row)
            physical = _validated_physical_block(
                block_row,
                expected_kind=object_kind,
                maximum_raw_bytes=int(maximum_raw_bytes),
            )
            coordinate = coordinate_by_hash.get(physical.block_hash)
            if coordinate is None or physical.block_hash in blocks_by_hash:
                raise PTG2SharedBlockError(
                    "PTG V4 physical query returned an unexpected block"
                )
            if int(physical.entry_count or 0) != int(coordinate.entry_count):
                raise PTG2SharedBlockError(
                    "PTG V4 physical block entry count is inconsistent"
                )
            cached = _CachedPhysicalBlock(
                block_hash=physical.block_hash,
                object_kind=physical.object_kind,
                entry_count=int(physical.entry_count or 0),
                payload=physical.payload,
            )
            blocks_by_hash[cached.block_hash] = cached
            _PHYSICAL_BLOCK_CACHE.put(
                cached.block_hash,
                cached,
                len(cached.payload) + 128,
            )
            _request_io().database_bytes += int(
                block_row.get("stored_byte_count") or 0
            )
            _request_io().database_blocks += 1
    if set(blocks_by_hash) != set(coordinate_by_hash):
        raise PTG2SharedBlockError("PTG V4 layout references a missing CAS block")
    return blocks_by_hash


def _decode_locator(
    payload: bytes,
    *,
    entry_count: int,
    local_owner_index: int,
    maximum_entries: int = PTG2_V4_LOCATORS_PER_PAGE,
) -> tuple[int, int]:
    expected_size = int(entry_count) * PTG2_V4_LOCATOR_WIDTH_BYTES
    if (
        entry_count < 0
        or entry_count > int(maximum_entries)
        or len(payload) != expected_size
        or not 0 <= int(local_owner_index) < entry_count
    ):
        raise PTG2SharedBlockError("PTG V4 locator page is malformed")
    return struct.unpack_from("<QI", payload, local_owner_index * 12)


def _decode_member_page(
    member_page_payload: bytes,
    *,
    entry_count: int,
    maximum_entries: int = PTG2_V4_MEMBERS_PER_PAGE,
) -> tuple[int, ...]:
    """Decode one authenticated fixed-width member page."""

    expected_size = int(entry_count) * PTG2_V4_MEMBER_WIDTH_BYTES
    if (
        entry_count < 0
        or entry_count > int(maximum_entries)
        or len(member_page_payload) != expected_size
    ):
        raise PTG2SharedBlockError("PTG V4 member page is malformed")
    if not member_page_payload:
        return ()
    if _native_u32_decoder is None:
        return struct.unpack(f"<{entry_count}I", member_page_payload)
    try:
        decoded_members = tuple(
            operator.index(member_value)
            for member_value in _native_u32_decoder(member_page_payload)
        )
    except (OverflowError, TypeError, ValueError) as exc:
        raise PTG2SharedBlockError("PTG V4 native member decoder failed") from exc
    if len(decoded_members) != entry_count or any(
        member_value < 0 or member_value > 0xFFFFFFFF
        for member_value in decoded_members
    ):
        raise PTG2SharedBlockError("PTG V4 native member decoder changed framing")
    # Pages may straddle owners, so ordering is checked after each owner's
    # complete member span has been reconstructed below.
    return decoded_members


def _decode_heavy_bitmap(
    bitmap_payload: bytes,
    *,
    heavy_owner: V4HeavyOwner,
) -> tuple[int, ...]:
    expected_bitmap_bytes = (heavy_owner.member_span + 7) // 8
    if (
        len(bitmap_payload)
        != PTG2_V4_HEAVY_BITMAP_HEADER_BYTES + expected_bitmap_bytes
    ):
        raise PTG2SharedBlockError("PTG V4 heavy bitmap has an invalid size")
    if bitmap_payload[:8] != PTG2_V4_HEAVY_BITMAP_MAGIC:
        raise PTG2SharedBlockError("PTG V4 heavy bitmap has an invalid magic")
    owner_key, member_base, member_span, member_count = struct.unpack_from(
        "<IIII", bitmap_payload, 8
    )
    if (
        owner_key != heavy_owner.owner_key
        or member_base != heavy_owner.member_base
        or member_span != heavy_owner.member_span
        or member_count != heavy_owner.member_count
    ):
        raise PTG2SharedBlockError("PTG V4 heavy bitmap conflicts with its manifest")
    bitmap = bitmap_payload[PTG2_V4_HEAVY_BITMAP_HEADER_BYTES:]
    if heavy_owner.member_span % 8:
        invalid_mask = 0xFF << (heavy_owner.member_span % 8)
        if bitmap[-1] & invalid_mask:
            raise PTG2SharedBlockError("PTG V4 heavy bitmap sets padding bits")
    members = tuple(
        heavy_owner.member_base + byte_index * 8 + bit
        for byte_index, byte_value in enumerate(bitmap)
        for bit in _SET_BIT_POSITIONS[byte_value]
    )
    if len(members) != heavy_owner.member_count:
        raise PTG2SharedBlockError("PTG V4 heavy bitmap member count changed")
    return members


def _unframe_heavy_bitmap_fragment(
    fragment_payload: bytes,
    *,
    heavy_owner: V4HeavyOwner,
    fragment_no: int,
    entry_count: int,
    logical_offset: int,
) -> bytes:
    """Authenticate payload-derived metadata and return logical bitmap bytes."""

    if len(fragment_payload) <= PTG2_V4_HEAVY_BITMAP_FRAGMENT_HEADER_BYTES:
        raise PTG2SharedBlockError("PTG V4 heavy bitmap fragment is truncated")
    (
        magic,
        owner_key,
        member_base,
        member_span,
        member_count,
        stored_fragment_no,
        stored_entry_count,
    ) = struct.unpack_from("<8sIIIIII", fragment_payload)
    if (
        magic != PTG2_V4_HEAVY_BITMAP_FRAGMENT_MAGIC
        or owner_key != heavy_owner.owner_key
        or member_base != heavy_owner.member_base
        or member_span != heavy_owner.member_span
        or member_count != heavy_owner.member_count
        or stored_fragment_no != int(fragment_no)
        or stored_entry_count != int(entry_count)
    ):
        raise PTG2SharedBlockError(
            "PTG V4 heavy bitmap fragment conflicts with its coordinate"
        )
    logical_payload = fragment_payload[
        PTG2_V4_HEAVY_BITMAP_FRAGMENT_HEADER_BYTES:
    ]
    bitmap_offset = max(
        PTG2_V4_HEAVY_BITMAP_HEADER_BYTES - int(logical_offset),
        0,
    )
    if (
        sum(byte.bit_count() for byte in logical_payload[bitmap_offset:])
        != int(entry_count)
    ):
        raise PTG2SharedBlockError(
            "PTG V4 heavy bitmap fragment entry count changed"
        )
    return logical_payload


async def _lookup_v4_heavy_members(
    session: Any,
    *,
    snapshot_key: int,
    schema_name: str,
    relation_manifest: V4RelationManifest,
    heavy_owners: Mapping[int, V4HeavyOwner],
) -> dict[int, tuple[int, ...]]:
    """Resolve selected bitmap owners from their exact CAS fragments."""

    if not heavy_owners:
        return {}
    coordinate_pairs = [
        (owner.owner_key, fragment_no)
        for owner in heavy_owners.values()
        for fragment_no in range(owner.fragment_count)
    ]
    object_kinds = {owner.object_kind for owner in heavy_owners.values()}
    if len(object_kinds) != 1:
        raise PTG2SharedBlockError("PTG V4 heavy owners disagree on object kind")
    object_kind = object_kinds.pop()
    coordinates = await _load_map_coordinate_pairs(
        session,
        schema_name=schema_name,
        snapshot_key=int(snapshot_key),
        object_kind=object_kind,
        coordinate_pairs=coordinate_pairs,
    )
    member_count = sum(owner.member_count for owner in heavy_owners.values())
    page_count = len(coordinates)
    byte_count = page_count * relation_manifest.member_page_bytes
    _charge_v4_hot_source_work(
        relation_manifest.relation,
        owner_count=len(heavy_owners),
        member_count=member_count,
        page_count=page_count,
        byte_count=byte_count,
    )
    _charge_v4_hot_npi_work(
        relation_manifest.relation,
        member_count=member_count,
        member_page_count=page_count,
        byte_count=byte_count,
    )
    blocks = await _load_physical_blocks(
        session,
        schema_name=schema_name,
        object_kind=object_kind,
        coordinates=coordinates.values(),
        maximum_raw_bytes=relation_manifest.member_page_bytes,
    )
    members_by_owner: dict[int, tuple[int, ...]] = {}
    for owner_key, owner in heavy_owners.items():
        fragments: list[bytes] = []
        logical_offset = 0
        observed_entry_count = 0
        for fragment_no in range(owner.fragment_count):
            coordinate = coordinates[(owner_key, fragment_no)]
            fragment_payload = blocks[bytes(coordinate.block_hash)].payload
            logical_fragment = _unframe_heavy_bitmap_fragment(
                fragment_payload,
                heavy_owner=owner,
                fragment_no=fragment_no,
                entry_count=int(coordinate.entry_count),
                logical_offset=logical_offset,
            )
            fragments.append(logical_fragment)
            logical_offset += len(logical_fragment)
            observed_entry_count += int(coordinate.entry_count)
        if observed_entry_count != owner.member_count:
            raise PTG2SharedBlockError(
                "PTG V4 heavy bitmap member count changed"
            )
        members_by_owner[owner_key] = _decode_heavy_bitmap(
            b"".join(fragments),
            heavy_owner=owner,
        )
    _request_io().bitmap_owner_hits += len(members_by_owner)
    return members_by_owner


def _decode_heavy_bitmap_prefix(
    bitmap_prefix_payload: bytes,
    *,
    heavy_owner: V4HeavyOwner,
    limit: int,
) -> tuple[int, ...]:
    """Decode an authenticated leading bitmap span after header validation."""

    expected_bitmap_bytes = (heavy_owner.member_span + 7) // 8
    expected_payload_bytes = (
        PTG2_V4_HEAVY_BITMAP_HEADER_BYTES + expected_bitmap_bytes
    )
    if (
        len(bitmap_prefix_payload) < PTG2_V4_HEAVY_BITMAP_HEADER_BYTES
        or len(bitmap_prefix_payload) > expected_payload_bytes
    ):
        raise PTG2SharedBlockError("PTG V4 heavy bitmap prefix has an invalid size")
    if bitmap_prefix_payload[:8] != PTG2_V4_HEAVY_BITMAP_MAGIC:
        raise PTG2SharedBlockError("PTG V4 heavy bitmap has an invalid magic")
    owner_key, member_base, member_span, member_count = struct.unpack_from(
        "<IIII", bitmap_prefix_payload, 8
    )
    if (
        owner_key != heavy_owner.owner_key
        or member_base != heavy_owner.member_base
        or member_span != heavy_owner.member_span
        or member_count != heavy_owner.member_count
    ):
        raise PTG2SharedBlockError("PTG V4 heavy bitmap conflicts with its manifest")
    if len(bitmap_prefix_payload) == expected_payload_bytes:
        return _decode_heavy_bitmap(
            bitmap_prefix_payload,
            heavy_owner=heavy_owner,
        )[:limit]
    bitmap_prefix = bitmap_prefix_payload[
        PTG2_V4_HEAVY_BITMAP_HEADER_BYTES:
    ]
    members = tuple(
        heavy_owner.member_base + byte_index * 8 + bit
        for byte_index, byte_value in enumerate(bitmap_prefix)
        for bit in _SET_BIT_POSITIONS[byte_value]
    )
    if len(members) < int(limit):
        raise PTG2SharedBlockError(
            "PTG V4 heavy bitmap prefix does not prove its requested members"
        )
    return members[:limit]


async def _load_heavy_prefix_coordinates(
    session: Any,
    *,
    schema_name: str,
    snapshot_key: int,
    object_kind: str,
    heavy_owners: Mapping[int, V4HeavyOwner],
    limit_per_owner: int,
) -> dict[int, tuple[Any, ...]]:
    """Fetch only coordinate fragments needed to prove each owner prefix."""

    coordinates_by_owner: dict[int, list[Any]] = {
        owner_key: [] for owner_key in heavy_owners
    }
    committed_count_by_owner = {owner_key: 0 for owner_key in heavy_owners}
    pending_owner_keys = set(heavy_owners)
    maximum_fragments = max(
        (owner.fragment_count for owner in heavy_owners.values()), default=0
    )
    for fragment_no in range(maximum_fragments):
        requested_pairs = tuple(
            (owner_key, fragment_no)
            for owner_key in sorted(pending_owner_keys)
            if fragment_no < heavy_owners[owner_key].fragment_count
        )
        if not requested_pairs:
            break
        coordinates = await _load_map_coordinate_pairs(
            session,
            schema_name=schema_name,
            snapshot_key=int(snapshot_key),
            object_kind=object_kind,
            coordinate_pairs=requested_pairs,
        )
        for owner_key, pair_fragment_no in requested_pairs:
            coordinate = coordinates[(owner_key, pair_fragment_no)]
            coordinates_by_owner[owner_key].append(coordinate)
            committed_count_by_owner[owner_key] += int(coordinate.entry_count)
            owner = heavy_owners[owner_key]
            is_owner_complete = pair_fragment_no + 1 == owner.fragment_count
            is_prefix_proven = (
                owner.member_count > int(limit_per_owner)
                and committed_count_by_owner[owner_key] >= int(limit_per_owner)
            )
            if is_owner_complete:
                if committed_count_by_owner[owner_key] != owner.member_count:
                    raise PTG2SharedBlockError(
                        "PTG V4 heavy bitmap coordinate count changed"
                    )
                pending_owner_keys.discard(owner_key)
            elif is_prefix_proven:
                pending_owner_keys.discard(owner_key)
    if pending_owner_keys:
        raise PTG2SharedBlockError(
            "PTG V4 heavy bitmap prefix coordinates are incomplete"
        )
    return {
        owner_key: tuple(owner_coordinates)
        for owner_key, owner_coordinates in coordinates_by_owner.items()
    }


def _decode_heavy_owner_prefix(
    owner: V4HeavyOwner,
    selected_coordinates: tuple[Any, ...],
    blocks_by_hash: Mapping[bytes, _CachedPhysicalBlock],
    limit_per_owner: int,
) -> tuple[int, ...]:
    """Verify selected physical fragments and decode one exact member prefix."""

    fragments: list[bytes] = []
    logical_offset = 0
    observed_entry_count = 0
    for fragment_no, coordinate in enumerate(selected_coordinates):
        fragment_payload = blocks_by_hash[bytes(coordinate.block_hash)].payload
        logical_fragment = _unframe_heavy_bitmap_fragment(
            fragment_payload,
            heavy_owner=owner,
            fragment_no=fragment_no,
            entry_count=int(coordinate.entry_count),
            logical_offset=logical_offset,
        )
        fragments.append(logical_fragment)
        logical_offset += len(logical_fragment)
        observed_entry_count += int(coordinate.entry_count)
    selected_limit = min(owner.member_count, int(limit_per_owner))
    if observed_entry_count < selected_limit:
        raise PTG2SharedBlockError(
            "PTG V4 heavy bitmap prefix does not prove its requested members"
        )
    return _decode_heavy_bitmap_prefix(
        b"".join(fragments),
        heavy_owner=owner,
        limit=selected_limit,
    )


def _charge_v4_heavy_prefix_work(
    relation_manifest: V4RelationManifest,
    heavy_owners: Mapping[int, V4HeavyOwner],
    selected_limit: int,
    selected_page_count: int,
) -> None:
    """Charge shared physical work for selected heavy-owner fragments."""

    selected_member_count = sum(
        min(owner.member_count, selected_limit)
        for owner in heavy_owners.values()
    )
    selected_byte_count = (
        int(selected_page_count) * relation_manifest.member_page_bytes
    )
    _charge_v4_hot_source_work(
        relation_manifest.relation,
        owner_count=len(heavy_owners),
        member_count=selected_member_count,
        page_count=selected_page_count,
        byte_count=selected_byte_count,
    )
    _charge_v4_hot_npi_work(
        relation_manifest.relation,
        member_count=selected_member_count,
        member_page_count=selected_page_count,
        byte_count=selected_byte_count,
    )


async def _lookup_v4_heavy_member_prefixes(
    session: Any,
    *,
    snapshot_key: int,
    schema_name: str,
    relation_manifest: V4RelationManifest,
    heavy_owners: Mapping[int, V4HeavyOwner],
    limit_per_owner: int,
) -> dict[int, tuple[int, ...]]:
    """Read only authenticated leading bitmap fragments needed per owner."""

    if not heavy_owners:
        return {}
    normalized_limit = int(limit_per_owner)
    if normalized_limit <= 0:
        return {owner_key: () for owner_key in heavy_owners}
    object_kind = _common_heavy_object_kind(heavy_owners)
    selected_coordinates_by_owner = await _load_heavy_prefix_coordinates(
        session,
        schema_name=schema_name,
        snapshot_key=int(snapshot_key),
        object_kind=object_kind,
        heavy_owners=heavy_owners,
        limit_per_owner=normalized_limit,
    )
    selected_coordinates = tuple(
        coordinate
        for owner_coordinates in selected_coordinates_by_owner.values()
        for coordinate in owner_coordinates
    )
    _charge_v4_heavy_prefix_work(
        relation_manifest,
        heavy_owners,
        normalized_limit,
        len(selected_coordinates),
    )
    blocks = await _load_physical_blocks(
        session,
        schema_name=schema_name,
        object_kind=object_kind,
        coordinates=selected_coordinates,
        maximum_raw_bytes=relation_manifest.member_page_bytes,
    )
    prefixes_by_owner: dict[int, tuple[int, ...]] = {}
    for owner_key, owner in heavy_owners.items():
        prefixes_by_owner[owner_key] = _decode_heavy_owner_prefix(
            owner,
            selected_coordinates_by_owner[owner_key],
            blocks,
            normalized_limit,
        )
    _request_io().bitmap_owner_hits += len(prefixes_by_owner)
    return prefixes_by_owner


def _common_heavy_object_kind(
    heavy_owners: Mapping[int, V4HeavyOwner],
) -> str:
    object_kinds = {owner.object_kind for owner in heavy_owners.values()}
    if len(object_kinds) != 1:
        raise PTG2SharedBlockError("PTG V4 heavy owners disagree on object kind")
    return object_kinds.pop()


async def _lookup_v4_relation_members_scoped(
    session: Any,
    *,
    snapshot_key: int,
    relation: str,
    owner_keys: Iterable[int],
    schema_name: str,
    max_members: int | None,
    prefix_members_per_owner: int | None = None,
) -> dict[int, tuple[int, ...]]:
    """Resolve one bounded relation inside an active request I/O scope."""

    normalized_owner_keys = _normalized_owner_keys(owner_keys)
    if not normalized_owner_keys:
        return {}
    if max_members is not None and int(max_members) < 0:
        raise PTG2SharedBlockError("PTG V4 max_members cannot be negative")
    if (
        prefix_members_per_owner is not None
        and int(prefix_members_per_owner) < 0
    ):
        raise PTG2SharedBlockError(
            "PTG V4 prefix_members_per_owner cannot be negative"
        )
    if max_members is not None and prefix_members_per_owner is not None:
        raise PTG2SharedBlockError(
            "PTG V4 aggregate and per-owner member limits are mutually exclusive"
        )
    per_owner_limit = (
        None
        if prefix_members_per_owner is None
        else int(prefix_members_per_owner)
    )
    normalized_relation = str(relation or "").strip().lower()
    _relation_kinds(normalized_relation)
    root = await load_v4_graph_root(
        session,
        int(snapshot_key),
        schema_name=schema_name,
    )
    _validate_relation_for_root(root, normalized_relation)
    relation_manifest = await load_v4_relation_manifest(
        session,
        snapshot_key=int(snapshot_key),
        relation=normalized_relation,
        schema_name=schema_name,
    )
    owner_limit = relation_manifest.owner_base + relation_manifest.owner_count
    if any(
        owner_key < relation_manifest.owner_base or owner_key >= owner_limit
        for owner_key in normalized_owner_keys
    ):
        raise PTG2SharedBlockError("PTG V4 relation owner is outside its manifest")
    _charge_v4_hot_npi_work(
        normalized_relation,
        batch_count=1,
    )
    _request_io().logical_lookups += 1

    selected_heavy_owners = await load_v4_heavy_owners(
        session,
        snapshot_key=int(snapshot_key),
        relation=normalized_relation,
        owner_keys=normalized_owner_keys,
        schema_name=schema_name,
    )
    if (
        normalized_relation in PTG2_V4_ORDER_PRESERVING_RELATIONS
        and selected_heavy_owners
    ):
        raise PTG2SharedBlockError(
            "PTG V4 ordered prefix relation cannot use bitmap owners"
        )
    if any(
        owner_key < relation_manifest.owner_base or owner_key >= owner_limit
        for owner_key in selected_heavy_owners
    ):
        raise PTG2SharedBlockError("PTG V4 heavy owner is outside its relation")
    if any(owner_key not in normalized_owner_keys for owner_key in selected_heavy_owners):
        raise PTG2SharedBlockError("PTG V4 heavy-owner lookup returned an unrequested owner")
    regular_owner_keys = tuple(
        owner_key
        for owner_key in normalized_owner_keys
        if owner_key not in selected_heavy_owners
    )
    total_members = sum(
        owner.member_count for owner in selected_heavy_owners.values()
    )
    if max_members is not None and total_members > int(max_members):
        raise PTG2SharedBlockError("PTG V4 graph selection exceeds max_members")
    if per_owner_limit is None:
        heavy_members = await _lookup_v4_heavy_members(
            session,
            snapshot_key=int(snapshot_key),
            schema_name=schema_name,
            relation_manifest=relation_manifest,
            heavy_owners=selected_heavy_owners,
        )
    else:
        heavy_members = await _lookup_v4_heavy_member_prefixes(
            session,
            snapshot_key=int(snapshot_key),
            schema_name=schema_name,
            relation_manifest=relation_manifest,
            heavy_owners=selected_heavy_owners,
            limit_per_owner=per_owner_limit,
        )
    if not regular_owner_keys:
        return {
            owner_key: heavy_members[owner_key]
            for owner_key in normalized_owner_keys
        }

    owner_base = relation_manifest.owner_base
    locator_owner_span = relation_manifest.locator_owner_span
    locator_page_keys = {
        owner_base
        + ((owner_key - owner_base) // locator_owner_span) * locator_owner_span
        for owner_key in regular_owner_keys
    }
    locator_coordinates = await _load_map_coordinates(
        session,
        schema_name=schema_name,
        snapshot_key=int(snapshot_key),
        object_kind=relation_manifest.locator_object_kind,
        block_keys=locator_page_keys,
    )
    locator_blocks = await _load_physical_blocks(
        session,
        schema_name=schema_name,
        object_kind=relation_manifest.locator_object_kind,
        coordinates=locator_coordinates.values(),
        maximum_raw_bytes=relation_manifest.locator_page_bytes,
    )
    locator_by_owner: dict[int, tuple[int, int]] = {}
    for owner_key in regular_owner_keys:
        page_key = owner_base + (
            (owner_key - owner_base) // locator_owner_span
        ) * locator_owner_span
        coordinate = locator_coordinates[page_key]
        block = locator_blocks[bytes(coordinate.block_hash)]
        member_offset, member_count = _decode_locator(
            block.payload,
            entry_count=block.entry_count,
            local_owner_index=owner_key - page_key,
            maximum_entries=locator_owner_span,
        )
        if (
            member_offset > 0xFFFFFFFFFFFFFFFF - member_count
            or member_offset + member_count > relation_manifest.vector_member_count
        ):
            raise PTG2SharedBlockError("PTG V4 locator range exceeds its manifest")
        selected_member_count = (
            member_count
            if per_owner_limit is None
            else min(member_count, per_owner_limit)
        )
        locator_by_owner[owner_key] = (member_offset, selected_member_count)
        total_members += member_count
        if max_members is not None and total_members > int(max_members):
            raise PTG2SharedBlockError("PTG V4 graph selection exceeds max_members")

    members_per_page = relation_manifest.members_per_page
    member_page_keys: set[int] = set()
    for member_offset, member_count in locator_by_owner.values():
        if member_count == 0:
            continue
        first_page = (
            member_offset // members_per_page
        ) * members_per_page
        last_page = (
            (member_offset + member_count - 1) // members_per_page
        ) * members_per_page
        member_page_keys.update(
            range(first_page, last_page + 1, members_per_page)
        )
    _charge_v4_hot_source_work(
        normalized_relation,
        owner_count=len(regular_owner_keys),
        member_count=sum(
            member_count for _member_offset, member_count in locator_by_owner.values()
        ),
        page_count=len(locator_page_keys) + len(member_page_keys),
        byte_count=(
            len(locator_page_keys) * relation_manifest.locator_page_bytes
            + len(member_page_keys) * relation_manifest.member_page_bytes
        ),
    )
    _charge_v4_hot_npi_work(
        normalized_relation,
        member_count=sum(
            member_count for _member_offset, member_count in locator_by_owner.values()
        ),
        locator_page_count=len(locator_page_keys),
        member_page_count=len(member_page_keys),
        byte_count=(
            len(locator_page_keys) * relation_manifest.locator_page_bytes
            + len(member_page_keys) * relation_manifest.member_page_bytes
        ),
    )
    if not member_page_keys:
        members_by_regular_owner = {
            owner_key: () for owner_key in regular_owner_keys
        }
        return {
            owner_key: heavy_members.get(
                owner_key,
                members_by_regular_owner.get(owner_key, ()),
            )
            for owner_key in normalized_owner_keys
        }
    member_coordinates = await _load_map_coordinates(
        session,
        schema_name=schema_name,
        snapshot_key=int(snapshot_key),
        object_kind=relation_manifest.member_object_kind,
        block_keys=member_page_keys,
    )
    member_blocks = await _load_physical_blocks(
        session,
        schema_name=schema_name,
        object_kind=relation_manifest.member_object_kind,
        coordinates=member_coordinates.values(),
        maximum_raw_bytes=relation_manifest.member_page_bytes,
    )
    members_by_page = {
        page_key: _decode_member_page(
            member_blocks[bytes(coordinate.block_hash)].payload,
            entry_count=member_blocks[bytes(coordinate.block_hash)].entry_count,
            maximum_entries=members_per_page,
        )
        for page_key, coordinate in member_coordinates.items()
    }
    members_by_owner: dict[int, tuple[int, ...]] = {}
    for owner_key, (member_offset, member_count) in locator_by_owner.items():
        members: list[int] = []
        remaining = int(member_count)
        cursor = int(member_offset)
        while remaining:
            page_key = (
                cursor // members_per_page
            ) * members_per_page
            local_offset = cursor - page_key
            page = members_by_page.get(page_key)
            if page is None or local_offset >= len(page):
                raise PTG2SharedBlockError("PTG V4 locator points outside its member page")
            take = min(remaining, len(page) - local_offset)
            if take <= 0:
                raise PTG2SharedBlockError("PTG V4 member page cannot advance")
            members.extend(page[local_offset : local_offset + take])
            cursor += take
            remaining -= take
        if normalized_relation in PTG2_V4_ORDER_PRESERVING_RELATIONS:
            if len(set(members)) != len(members):
                raise PTG2SharedBlockError(
                    "PTG V4 ordered relation members are not unique"
                )
        elif any(left >= right for left, right in zip(members, members[1:])):
            raise PTG2SharedBlockError(
                "PTG V4 relation members are not unique and ordered"
            )
        members_by_owner[owner_key] = tuple(members)
    members_by_owner.update(heavy_members)
    return {
        owner_key: members_by_owner[owner_key]
        for owner_key in normalized_owner_keys
    }


async def lookup_v4_relation_members(
    session: Any,
    *,
    snapshot_key: int,
    relation: str,
    owner_keys: Iterable[int],
    schema_name: str,
    max_members: int | None = None,
) -> dict[int, tuple[int, ...]]:
    """Resolve a dense V4 relation while fetching each distinct CAS page once."""

    if _ACTIVE_V4_GRAPH_REQUEST_IO.get() is not None:
        return await _lookup_v4_relation_members_scoped(
            session,
            snapshot_key=snapshot_key,
            relation=relation,
            owner_keys=owner_keys,
            schema_name=schema_name,
            max_members=max_members,
            prefix_members_per_owner=None,
        )
    with v4_graph_request_scope():
        return await _lookup_v4_relation_members_scoped(
            session,
            snapshot_key=snapshot_key,
            relation=relation,
            owner_keys=owner_keys,
            schema_name=schema_name,
            max_members=max_members,
            prefix_members_per_owner=None,
        )


async def lookup_v4_relation_member_prefixes(
    session: Any,
    *,
    snapshot_key: int,
    relation: str,
    owner_keys: Iterable[int],
    schema_name: str,
    limit_per_owner: int,
) -> dict[int, tuple[int, ...]]:
    """Resolve authenticated owner prefixes without reading complete vectors."""

    if _ACTIVE_V4_GRAPH_REQUEST_IO.get() is not None:
        return await _lookup_v4_relation_members_scoped(
            session,
            snapshot_key=snapshot_key,
            relation=relation,
            owner_keys=owner_keys,
            schema_name=schema_name,
            max_members=None,
            prefix_members_per_owner=limit_per_owner,
        )
    with v4_graph_request_scope():
        return await _lookup_v4_relation_members_scoped(
            session,
            snapshot_key=snapshot_key,
            relation=relation,
            owner_keys=owner_keys,
            schema_name=schema_name,
            max_members=None,
            prefix_members_per_owner=limit_per_owner,
        )


async def lookup_v4_ordered_prefixes(
    session: Any,
    *,
    snapshot_key: int,
    provider_set_keys: Iterable[int],
    schema_name: str,
    max_members: int,
) -> dict[int, tuple[int, ...]]:
    """Read sparse exact NPI prefixes while preserving group-first order."""

    normalized_max_members = int(max_members)
    if normalized_max_members < 0:
        raise PTG2SharedBlockError(
            "PTG V4 ordered prefix maximum cannot be negative"
        )
    if _ACTIVE_V4_GRAPH_REQUEST_IO.get() is not None:
        return await _lookup_v4_relation_members_scoped(
            session,
            snapshot_key=snapshot_key,
            relation="set_npi_prefix_override",
            owner_keys=provider_set_keys,
            schema_name=schema_name,
            max_members=normalized_max_members,
            prefix_members_per_owner=None,
        )
    with v4_graph_request_scope():
        return await _lookup_v4_relation_members_scoped(
            session,
            snapshot_key=snapshot_key,
            relation="set_npi_prefix_override",
            owner_keys=provider_set_keys,
            schema_name=schema_name,
            max_members=normalized_max_members,
            prefix_members_per_owner=None,
        )


lookup_v4_ordered_npi_prefix_overrides = lookup_v4_ordered_prefixes


async def v4_npi_keys_for_values(
    session: Any,
    *,
    snapshot_key: int,
    npis: Iterable[int],
    schema_name: str,
) -> dict[int, int]:
    """Map external NPIs to the V4 snapshot-local dense dictionary."""

    normalized_npis = tuple(sorted({int(npi) for npi in npis}))
    if not normalized_npis:
        return {}
    schema = _quote_ident(schema_name)
    key_lookup_result = await session.execute(
        text(
            f"""
            SELECT npi_key, npi
              FROM {schema}.{PTG2_V4_NPI_TABLE}
             WHERE snapshot_key = :snapshot_key
               AND npi = ANY(CAST(:npis AS bigint[]))
            """
        ),
        {"snapshot_key": int(snapshot_key), "npis": normalized_npis},
    )
    return {
        int(npi_key_record["npi"]): int(npi_key_record["npi_key"])
        for npi_key_record in (
            _row_mapping(raw_row) for raw_row in key_lookup_result
        )
    }


async def v4_npi_values_for_keys(
    session: Any,
    *,
    snapshot_key: int,
    npi_keys: Iterable[int],
    schema_name: str,
) -> dict[int, int]:
    """Map V4 dense NPI keys back to exact external NPIs."""

    normalized_keys = tuple(sorted({int(npi_key) for npi_key in npi_keys}))
    if not normalized_keys:
        return {}
    _charge_v4_hot_npi_work(
        PTG2_V4_HOT_NPI_RELATION,
        member_count=len(normalized_keys),
        byte_count=(
            len(normalized_keys) * PTG2_V4_NPI_DICTIONARY_ENTRY_BYTES
        ),
        dictionary_read_count=1,
    )
    schema = _quote_ident(schema_name)
    value_lookup_result = await session.execute(
        text(
            f"""
            SELECT npi_key, npi
              FROM {schema}.{PTG2_V4_NPI_TABLE}
             WHERE snapshot_key = :snapshot_key
               AND npi_key = ANY(CAST(:npi_keys AS integer[]))
            """
        ),
        {"snapshot_key": int(snapshot_key), "npi_keys": normalized_keys},
    )
    return {
        int(npi_value_record["npi_key"]): int(npi_value_record["npi"])
        for npi_value_record in (
            _row_mapping(raw_row) for raw_row in value_lookup_result
        )
    }
