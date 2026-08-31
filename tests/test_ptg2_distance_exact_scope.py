# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact-NPI admission contracts for distance-ordered provider search."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import ptg2_serving as serving
from tests.test_ptg2_graph_runtime_boundaries import _local_distance_tables


def _install_distance_reads(monkeypatch, exact_npis):
    monkeypatch.setenv("HLTHPRT_NPI_SEARCH_TAXONOMY_PROJECTION_ENABLED", "1")
    exact_npi_reader = AsyncMock(return_value=exact_npis)
    location_reader = AsyncMock(return_value=[])
    replacement_by_name = {
        "_shared_rate_code_scope_rows": AsyncMock(return_value=[{"code_key": 7}]),
        "_membership_exact_scope_npis": exact_npi_reader,
        "load_v4_graph_root": AsyncMock(
            return_value=SimpleNamespace(representation="pattern_v1")
        ),
        "_membership_location_rows": location_reader,
        "_graph_location_probe_batch_size": lambda *_args, **_kwargs: 1,
        "_ptg2_manifest_location_match_limit": lambda: 10,
    }
    for name, replacement in replacement_by_name.items():
        monkeypatch.setattr(serving, name, replacement)
    return (
        exact_npi_reader,
        location_reader,
        replacement_by_name["load_v4_graph_root"],
    )


async def _distance_candidates(args):
    return await serving._local_inferred_distance_graph_candidates(
        object(),
        _local_distance_tables(),
        args,
        plan_id="synthetic-plan",
        requested_code=str(args["code"]),
        requested_system=str(args["code_system"]),
        candidate_limit=1,
    )


@pytest.mark.asyncio
async def test_distance_uses_bounded_exact_npi_scope(monkeypatch):
    _, location_reader, _ = _install_distance_reads(monkeypatch, (1, 2, 3))

    candidates = await _distance_candidates(
        {"code": "73721", "code_system": "CPT"}
    )

    assert candidates == serving._GraphLocationCandidates(
        [], {}, taxonomy_filtered=True
    )
    assert location_reader.await_args.kwargs["candidate_npis"] == (1, 2, 3)


@pytest.mark.asyncio
async def test_distance_skips_graph_reads_for_empty_exact_scope(monkeypatch):
    _, _, graph_root = _install_distance_reads(monkeypatch, ())

    candidates = await _distance_candidates(
        {
            "classification": "Orthopaedic Surgery",
            "code": "66984",
            "code_system": "CPT",
        }
    )

    assert candidates == serving._GraphLocationCandidates(
        [], {}, taxonomy_filtered=True
    )
    graph_root.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("args", "exact_npis", "expected_scope_reads"),
    [
        (
            {
                "classification": "Family Medicine",
                "code": "73721",
                "code_system": "CPT",
            },
            (1, 2, 3),
            1,
        ),
        ({"code": "A1234", "code_system": "HCPCS"}, (), 0),
        (
            {
                "code": "A1234",
                "code_system": "HCPCS",
                "provider_sex_code": "F",
            },
            (),
            0,
        ),
    ],
)
async def test_distance_keeps_knn_for_unbounded_or_unfiltered_scope(
    monkeypatch,
    args,
    exact_npis,
    expected_scope_reads,
):
    exact_npi_reader, location_reader, _ = _install_distance_reads(
        monkeypatch, exact_npis
    )
    monkeypatch.setattr(serving, "_MEMBERSHIP_EXACT_NPI_SCOPE_LIMIT", 2)

    await _distance_candidates(args)

    assert exact_npi_reader.await_count == expected_scope_reads
    assert location_reader.await_args.kwargs["candidate_npis"] is None
