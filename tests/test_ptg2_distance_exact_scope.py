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
    assert location_reader.await_args.kwargs["coarse_taxonomy_knn"] is False


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
    ("args", "exact_npis", "expected_scope_reads", "expected_coarse_knn"),
    [
        (
            {
                "classification": "Family Medicine",
                "code": "73721",
                "code_system": "CPT",
            },
            (1, 2, 3),
            1,
            True,
        ),
        (
            {"code": "73721", "code_system": "CPT"},
            (1, 2, 3),
            1,
            True,
        ),
        ({"code": "12345", "code_system": "CPT"}, (), 0, False),
        ({"code": "A1234", "code_system": "HCPCS"}, (), 0, False),
        (
            {
                "code": "A1234",
                "code_system": "HCPCS",
                "provider_sex_code": "F",
            },
            (),
            0,
            False,
        ),
    ],
)
async def test_distance_keeps_knn_for_unbounded_or_unfiltered_scope(
    monkeypatch,
    args,
    exact_npis,
    expected_scope_reads,
    expected_coarse_knn,
):
    exact_npi_reader, location_reader, _ = _install_distance_reads(
        monkeypatch, exact_npis
    )
    monkeypatch.setattr(serving, "_MEMBERSHIP_EXACT_NPI_SCOPE_LIMIT", 2)

    await _distance_candidates(args)

    assert exact_npi_reader.await_count == expected_scope_reads
    assert location_reader.await_args.kwargs["candidate_npis"] is None
    assert (
        location_reader.await_args.kwargs["coarse_taxonomy_knn"]
        is expected_coarse_knn
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("scope_size", "max_candidates", "expected_coarse_knn", "expected_limit"),
    (
        (16384, 20000, False, 16385),
        (16385, 20000, True, 16385),
        (201, 200, True, 201),
    ),
)
async def test_exact_scope_cap_is_authoritative(
    monkeypatch,
    scope_size,
    max_candidates,
    expected_coarse_knn,
    expected_limit,
):
    monkeypatch.setenv("HLTHPRT_NPI_SEARCH_TAXONOMY_PROJECTION_ENABLED", "1")
    exact_npis = tuple(range(1, scope_size + 1))
    exact_npi_reader = AsyncMock(return_value=exact_npis)
    monkeypatch.setattr(
        serving, "_membership_exact_scope_npis", exact_npi_reader
    )

    candidate_npis, coarse_taxonomy_knn = (
        await serving._bounded_exact_distance_npis(
            object(),
            _local_distance_tables(),
            {
                "classification": "Internal Medicine",
                "code": "99203",
                "code_system": "CPT",
            },
            max_candidates,
            1,
        )
    )

    assert candidate_npis == (None if expected_coarse_knn else exact_npis)
    assert coarse_taxonomy_knn is expected_coarse_knn
    assert exact_npi_reader.await_args.kwargs["limit"] == expected_limit


@pytest.mark.asyncio
async def test_projection_disabled_keeps_exact_knn_prefilter(monkeypatch):
    monkeypatch.delenv(
        "HLTHPRT_NPI_SEARCH_TAXONOMY_PROJECTION_ENABLED", raising=False
    )
    exact_npi_reader = AsyncMock()
    monkeypatch.setattr(
        serving, "_membership_exact_scope_npis", exact_npi_reader
    )

    scope = await serving._bounded_exact_distance_npis(
        object(),
        _local_distance_tables(),
        {
            "classification": "Internal Medicine",
            "code": "99203",
            "code_system": "CPT",
        },
        20000,
        1,
    )

    assert scope == (None, False)
    exact_npi_reader.assert_not_awaited()
    fallback_sql = serving._membership_projection_knn_prefilter_sql(
        {"classification": "Internal Medicine"}, {}, "distance"
    )
    assert "membership_location_specialty_nt" in fallback_sql


@pytest.mark.asyncio
async def test_exhaustive_arm_keeps_canonical_knn_prefilter(monkeypatch):
    monkeypatch.setattr(serving, "_MEMBERSHIP_EXACT_NPI_SCOPE_LIMIT", 200)
    _, location_reader, _ = _install_distance_reads(
        monkeypatch, tuple(range(1, 202))
    )

    monkeypatch.setattr(
        serving,
        "_version_three_explicit_npi_graph_scope",
        AsyncMock(return_value=None),
    )

    await serving._graph_candidates_for_request(
        object(),
        _local_distance_tables(),
        {
            "code": "73721",
            "code_system": "CPT",
            "negotiated_rate": "100.00",
            "order_by": "total_allowed_amount",
        },
        plan_id="synthetic-plan",
        requested_code="73721",
        requested_system="CPT",
        candidate_limit=11,
        require_exhaustive=True,
    )

    assert location_reader.await_args.kwargs["candidate_npis"] is None
    assert location_reader.await_args.kwargs["coarse_taxonomy_knn"] is False


@pytest.mark.asyncio
async def test_overflow_knn_projects_before_limit_and_filters_exactly_after(
    monkeypatch,
):
    monkeypatch.setenv("HLTHPRT_NPI_SEARCH_TAXONOMY_PROJECTION_ENABLED", "1")
    monkeypatch.setattr(
        serving,
        "_ptg2_address_serving_table",
        AsyncMock(return_value="mrf.entity_address_unified"),
    )
    monkeypatch.setattr(
        serving,
        "is_provider_address_geo_capability_available",
        AsyncMock(return_value=True),
    )
    query = await serving._membership_location_query(
        object(),
        _local_distance_tables(),
        {
            "classification": "Psychiatry & Neurology",
            "include_subspecialties": False,
            "provider_sex_code": "F",
            "code": "90837",
            "code_system": "CPT",
            "zip5": "59101",
            "lat": 45.78,
            "long": -108.5,
            "radius_miles": 50,
        },
        candidate_npis=None,
        coarse_taxonomy_knn=True,
        limit=10,
    )

    assert query is not None
    rendered_sql = serving._membership_location_sql(query, limit=10, offset=0)
    raw_prefix, exact_suffix = rendered_sql.split(
        "nearest_addresses AS MATERIALIZED (", 1
    )
    assert raw_prefix.count("scope_provider.search_taxonomy_codes") == 6
    assert "membership_scope_specialty_classification" in raw_prefix
    assert "membership_scope_inferred_taxonomy_codes" in raw_prefix
    assert "membership_location_specialty_nt" not in raw_prefix
    assert "membership_location_specialty_nt" in exact_suffix
    assert "membership_location_nt" in exact_suffix
    assert "n_entity.entity_type_code" in exact_suffix
    assert "membership_provider_sex" in exact_suffix
    assert query.taxonomy_index_sql is None


@pytest.mark.asyncio
async def test_coarse_false_positive_prefix_grows_to_farther_exact_match(
    monkeypatch,
):
    location_limits = []

    async def location_rows(*_args, limit, coarse_taxonomy_knn, **_kwargs):
        location_limits.append((limit, coarse_taxonomy_knn))
        if len(location_limits) == 1:
            return [{"_ptg_probe_empty": True, "_ptg_source_exhausted": False}]
        return [{"npi": 2, "_ptg_source_exhausted": True}]

    async def classify(_session, _tables, memberships, _request, state):
        if memberships:
            state.code_sets.add(10)

    monkeypatch.setattr(serving, "_membership_location_rows", location_rows)
    monkeypatch.setattr(
        serving,
        "_local_v4_memberships",
        AsyncMock(side_effect=({}, {2: (10,)})),
    )
    monkeypatch.setattr(serving, "_classify_local_code_sets", classify)
    request = serving._LocalDistanceGraphRequest(
        1,
        [{"code_key": 7}],
        serving._v4_geo_rate_forward_limits(_local_distance_tables()),
    )

    candidates = await serving._scan_local_distance_graph(
        object(),
        _local_distance_tables(),
        {},
        request,
        1,
        10,
        coarse_taxonomy_knn=True,
    )

    assert candidates == serving._GraphLocationCandidates(
        [{"npi": 2, "_ptg_source_exhausted": True}],
        {2: {10}},
        taxonomy_filtered=True,
    )
    assert location_limits == [(1, True), (4, True)]
