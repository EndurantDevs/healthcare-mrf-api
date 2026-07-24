from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import control
from api import metrics as api_metrics

EXPECTED_V4_COUNTER_METRICS = (
    ("database_blocks", "hp_mrf_ptg_v4_graph_database_blocks_total"),
    ("cache_hit_bytes", "hp_mrf_ptg_v4_graph_cache_hit_bytes_total"),
    ("logical_lookups", "hp_mrf_ptg_v4_graph_logical_lookups_total"),
    ("bitmap_owner_hits", "hp_mrf_ptg_v4_graph_bitmap_owner_hits_total"),
    ("component_fallback_sets", "hp_mrf_ptg_v4_graph_component_fallback_sets_total"),
    ("hot_prefix_requests", "hp_mrf_ptg_v4_graph_hot_prefix_requests_total"),
    ("cold_exact_requests", "hp_mrf_ptg_v4_graph_cold_exact_requests_total"),
    ("npi_prefix_override_sets", "hp_mrf_ptg_v4_graph_npi_prefix_override_sets_total"),
    ("hot_group_npi_members", "hp_mrf_ptg_v4_graph_hot_group_npi_members_total"),
    (
        "hot_group_npi_locator_pages",
        "hp_mrf_ptg_v4_graph_hot_group_npi_locator_pages_total",
    ),
    (
        "hot_group_npi_member_pages",
        "hp_mrf_ptg_v4_graph_hot_group_npi_member_pages_total",
    ),
    ("hot_group_npi_bytes", "hp_mrf_ptg_v4_graph_hot_group_npi_bytes_total"),
    ("hot_group_npi_batches", "hp_mrf_ptg_v4_graph_hot_group_npi_batches_total"),
    (
        "hot_npi_dictionary_reads",
        "hp_mrf_ptg_v4_graph_hot_npi_dictionary_reads_total",
    ),
    (
        "provider_expansion_rate_rows",
        "hp_mrf_ptg_v4_provider_expansion_rate_rows_total",
    ),
    (
        "provider_expansion_provider_sets",
        "hp_mrf_ptg_v4_provider_expansion_provider_sets_total",
    ),
    (
        "provider_expansion_graph_batches",
        "hp_mrf_ptg_v4_provider_expansion_graph_batches_total",
    ),
    (
        "provider_expansion_rejections",
        "hp_mrf_ptg_v4_provider_expansion_rejections_total",
    ),
)


def _graph_metrics_fixture() -> dict[str, object]:
    graph_metrics_by_name: dict[str, object] = {
        "request_count": 7,
        "database_bytes": 4096,
        "buckets": {1024: 2, 64: 1},
        "infinite_bucket_count": 7,
    }
    graph_metrics_by_name.update(
        {
            source_name: metric_number
            for metric_number, (source_name, _metric_name) in enumerate(
                EXPECTED_V4_COUNTER_METRICS,
                start=11,
            )
        }
    )
    return graph_metrics_by_name


def _assert_engine_metric_contract(metric_lines: list[str]) -> None:
    assert metric_lines[:11] == [
        "# HELP hp_mrf_api_node_health Engine node health status, 1 for ok and 0 for degraded.",
        "# TYPE hp_mrf_api_node_health gauge",
        'hp_mrf_api_node_health{node_id="node-a",status="degraded"} 0.000000',
        "# HELP hp_mrf_api_active_runs_by_status Active engine import runs grouped by status.",
        "# TYPE hp_mrf_api_active_runs_by_status gauge",
        'hp_mrf_api_active_runs_by_status{status="queued"} 1.000000',
        'hp_mrf_api_active_runs_by_status{status="running"} 2.000000',
        "# HELP hp_mrf_api_active_runs_by_importer Active engine import runs grouped by importer.",
        "# TYPE hp_mrf_api_active_runs_by_importer gauge",
        'hp_mrf_api_active_runs_by_importer{importer="npi"} 2.000000',
        'hp_mrf_api_active_runs_by_importer{importer="ptg"} 1.000000',
    ]
    assert 'hp_mrf_api_queue_depth{queue="fast"} 2.000000' in metric_lines
    assert 'hp_mrf_api_worker_running{queue="fast"} 0.000000' in metric_lines
    assert not any("disk_free_bytes" in line for line in metric_lines)
    assert not any('queue="slow"' in line for line in metric_lines)


def _assert_graph_metric_contract(
    metric_lines: list[str],
    graph_metrics_by_name: dict[str, object],
) -> None:
    first_bucket_index = metric_lines.index(
        'hp_mrf_ptg_v4_graph_database_bytes_per_request_bucket{le="64"} 1.000000'
    )
    assert metric_lines[first_bucket_index : first_bucket_index + 3] == [
        'hp_mrf_ptg_v4_graph_database_bytes_per_request_bucket{le="64"} 1.000000',
        'hp_mrf_ptg_v4_graph_database_bytes_per_request_bucket{le="1024"} 2.000000',
        'hp_mrf_ptg_v4_graph_database_bytes_per_request_bucket{le="+Inf"} 7.000000',
    ]
    expected_counter_lines = [
        f"{metric_name} {float(graph_metrics_by_name[source_name]):.6f}"
        for source_name, metric_name in EXPECTED_V4_COUNTER_METRICS
    ]
    assert metric_lines[-len(expected_counter_lines) :] == expected_counter_lines


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("auth_setting", "expected_auth_calls"),
    [
        (None, 1),
        ("false", 0),
    ],
)
async def test_metrics_endpoint_applies_auth_before_rendering(
    monkeypatch,
    auth_setting,
    expected_auth_calls,
):
    request = SimpleNamespace()
    events: list[object] = []

    if auth_setting is None:
        monkeypatch.delenv("HLTHPRT_METRICS_REQUIRE_AUTH", raising=False)
    else:
        monkeypatch.setenv("HLTHPRT_METRICS_REQUIRE_AUTH", auth_setting)

    monkeypatch.setattr(
        control,
        "_require_control_auth",
        lambda observed_request: events.append(("auth", observed_request)),
    )

    async def render_metrics():
        events.append("render")
        return "example_metric 1\n"

    monkeypatch.setattr(api_metrics, "render_prometheus_metrics", render_metrics)

    metrics_response = await api_metrics.prometheus_metrics(request)

    assert metrics_response.status == 200
    assert metrics_response.content_type == "text/plain; version=0.0.4"
    assert metrics_response.body == b"example_metric 1\n"
    assert events[-1] == "render"
    assert events.count(("auth", request)) == expected_auth_calls


@pytest.mark.asyncio
async def test_active_run_counts_aggregate_each_status_and_importer(monkeypatch):
    class RunCountRows:
        def all(self):
            return [
                ("running", "ptg", 2),
                ("running", "npi", 3),
                ("queued", "ptg", None),
            ]

    execute = AsyncMock(return_value=RunCountRows())
    monkeypatch.setattr(api_metrics.db, "execute", execute)

    status_count_by_name, importer_count_by_name = (
        await api_metrics._active_run_counts()
    )

    assert status_count_by_name == {"queued": 0, "running": 5}
    assert importer_count_by_name == {"npi": 3, "ptg": 2}
    execute.assert_awaited_once()


def test_v4_graph_counter_names_remain_stable():
    assert api_metrics._V4_GRAPH_COUNTER_METRICS == EXPECTED_V4_COUNTER_METRICS


@pytest.mark.asyncio
async def test_rendered_metrics_are_sorted_and_skip_unknown_runtime_values(
    monkeypatch,
):
    async def node_health():
        return {
            "node_id": "node-a",
            "status": "degraded",
            "disk": {"free": "unknown"},
            "queue_depth": {"slow": "unknown", "fast": 2},
            "workers": {
                "slow": "unknown",
                "fast": {"running": False},
            },
        }

    async def active_run_counts():
        return {"running": 2, "queued": 1}, {"ptg": 1, "npi": 2}

    monkeypatch.setattr(api_metrics, "node_health", node_health)
    monkeypatch.setattr(api_metrics, "_active_run_counts", active_run_counts)
    monkeypatch.setattr(
        api_metrics,
        "runtime_identity",
        lambda: {
            "process_identity": "process-a",
            "image_identity": "image-a",
            "process_started_at": "2026-07-24T10:00:00Z",
        },
    )
    graph_metrics_by_name = _graph_metrics_fixture()
    monkeypatch.setattr(
        api_metrics,
        "v4_graph_metrics_snapshot",
        lambda: graph_metrics_by_name,
    )

    metric_lines = (await api_metrics.render_prometheus_metrics()).splitlines()

    _assert_engine_metric_contract(metric_lines)
    _assert_graph_metric_contract(metric_lines, graph_metrics_by_name)


def test_metric_labels_are_safely_normalized_and_escaped():
    assert (
        api_metrics._metric(
            "example_metric",
            1,
            **{"queue-name": 'a"\\\nb'},
        )
        == r'example_metric{queue_name="a\"\\\nb"} 1.000000'
    )
    assert api_metrics._metric("example_metric", 2, **{"": ""}) == (
        'example_metric{label=""} 2.000000'
    )
