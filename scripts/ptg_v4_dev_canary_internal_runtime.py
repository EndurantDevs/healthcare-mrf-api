"""Production runtime bindings and cache isolation for the V4 owner canary."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Mapping


@dataclass(frozen=True)
class InternalProbeRuntime:
    """Injectable production bindings used by the read-only in-pod probe."""

    database: Any
    text: Callable[[str], Any]
    runtime_identity: Callable[[], Mapping[str, str]]
    schema_name: str
    load_serving_tables: Callable[..., Awaitable[Any]]
    provider_npis_for_sets: Callable[..., Awaitable[Mapping[str, tuple[int, ...]]]]
    npi_keys_for_values: Callable[..., Awaitable[Mapping[int, int]]]
    metrics_snapshot: Callable[[], Mapping[str, Any]]
    reset_cold_caches: Callable[[], None]
    reset_warm_caches: Callable[[], None]
    monotonic: Callable[[], float] = time.perf_counter


def production_runtime() -> InternalProbeRuntime:
    """Bind the probe to the exact production loader and serving functions."""

    from sqlalchemy import text

    from api import ptg2_serving, ptg2_v4_graph
    from api.ptg2_tables import PTG2_SCHEMA, snapshot_serving_tables
    from api.runtime_identity import runtime_identity
    from db.connection import db

    return InternalProbeRuntime(
        database=db,
        text=text,
        runtime_identity=runtime_identity,
        schema_name=PTG2_SCHEMA,
        load_serving_tables=snapshot_serving_tables,
        provider_npis_for_sets=ptg2_serving._provider_npis_for_sets,
        npi_keys_for_values=ptg2_v4_graph.v4_npi_keys_for_values,
        metrics_snapshot=ptg2_v4_graph.v4_graph_metrics_snapshot,
        reset_cold_caches=lambda: reset_production_caches(
            ptg2_serving,
            ptg2_v4_graph,
            cold=True,
        ),
        reset_warm_caches=lambda: reset_production_caches(
            ptg2_serving,
            ptg2_v4_graph,
            cold=False,
        ),
    )


def reset_production_caches(
    serving_module: Any,
    graph_module: Any,
    *,
    cold: bool,
) -> None:
    """Clear prefix state and, for cold samples, all V4 physical caches."""

    serving_module._PTG2_PROVIDER_NPI_PREFIX_CACHE.clear()
    if not cold:
        return
    graph_module._MAP_COORDINATE_CACHE = graph_module._ByteLRU(
        graph_module._MAP_CACHE_MAX_BYTES
    )
    graph_module._PHYSICAL_BLOCK_CACHE = graph_module._ByteLRU(
        graph_module._BLOCK_CACHE_MAX_BYTES
    )
    for cache_name in (
        "_ROOT_CACHE",
        "_RELATION_CACHE",
        "_HEAVY_OWNER_CACHE",
        "_HEAVY_OWNER_NEGATIVE_CACHE",
    ):
        getattr(graph_module, cache_name).clear()
