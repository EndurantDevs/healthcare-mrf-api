# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact-NPI admission contracts for distance-ordered provider search."""

from contextlib import nullcontext
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import ptg2_serving as serving
from api import ptg_membership_distance as distance
from api.billing_search_cursor import BillingSearchCursorGenerationExpired
from api.network_read_execution import has_network_read_inventory
from tests.test_ptg2_graph_runtime_boundaries import _local_distance_tables


def _install_distance_reads(monkeypatch, exact_npis):
    from api import ptg_membership_reader

    monkeypatch.setattr(ptg_membership_reader, "indexed_memberships", AsyncMock(return_value=None))
    monkeypatch.setenv("HLTHPRT_NPI_SEARCH_TAXONOMY_PROJECTION_ENABLED", "1")
    exact_npi_reader = AsyncMock(return_value=exact_npis)
    location_reader = AsyncMock(return_value=[])
    replacement_by_name = {
        "_shared_rate_code_scope_rows": AsyncMock(return_value=[{"code_key": 7}]),
        "_membership_exact_scope_npis": exact_npi_reader,
        "load_v4_graph_root": AsyncMock(return_value=SimpleNamespace(representation="pattern_v1")),
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

    candidates = await _distance_candidates({"code": "73721", "code_system": "CPT"})

    assert candidates == serving._GraphLocationCandidates([], {}, taxonomy_filtered=True)
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

    assert candidates == serving._GraphLocationCandidates([], {}, taxonomy_filtered=True)
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
    exact_npi_reader, location_reader, _ = _install_distance_reads(monkeypatch, exact_npis)
    monkeypatch.setattr(serving, "_MEMBERSHIP_EXACT_NPI_SCOPE_LIMIT", 2)

    await _distance_candidates(args)

    assert exact_npi_reader.await_count == expected_scope_reads
    assert location_reader.await_args.kwargs["candidate_npis"] is None
    assert location_reader.await_args.kwargs["coarse_taxonomy_knn"] is expected_coarse_knn


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
    monkeypatch.setattr(serving, "_membership_exact_scope_npis", exact_npi_reader)

    candidate_npis, coarse_taxonomy_knn = await serving._bounded_exact_distance_npis(
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

    assert candidate_npis == (None if expected_coarse_knn else exact_npis)
    assert coarse_taxonomy_knn is expected_coarse_knn
    assert exact_npi_reader.await_args.kwargs["limit"] == expected_limit


@pytest.mark.asyncio
async def test_projection_disabled_keeps_exact_knn_prefilter(monkeypatch):
    monkeypatch.delenv("HLTHPRT_NPI_SEARCH_TAXONOMY_PROJECTION_ENABLED", raising=False)
    exact_npi_reader = AsyncMock()
    monkeypatch.setattr(serving, "_membership_exact_scope_npis", exact_npi_reader)

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
    _, location_reader, _ = _install_distance_reads(monkeypatch, tuple(range(1, 202)))

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
    raw_prefix, exact_suffix = rendered_sql.split("nearest_addresses AS MATERIALIZED (", 1)
    assert raw_prefix.count("scope_provider.search_taxonomy_codes") == 6
    assert raw_prefix.count("scope_provider.npi = addr.npi") == 3
    assert raw_prefix.count(":membership_scope_inferred_taxonomy_codes AS varchar[])) OFFSET 0)") == 3
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


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("maximum_candidates", "configured_limit", "expected_reads"), [(2, 10, 2), (3, 10, 3), (10, 2, 3)]
)
async def test_ranked_distance_graph_keeps_one_raw_budget_across_filtered_batches(
    monkeypatch, maximum_candidates, configured_limit, expected_reads
):
    monkeypatch.setenv("HLTHPRT_PTG2_MANIFEST_LOCATION_MATCH_LIMIT", str(configured_limit))
    matched_address_by_field = {"npi": 3, "distance_miles": 2.0}
    page_reader = AsyncMock(
        side_effect=[
            serving._MembershipDistancePage([], 1, (1.0, 1)),
            serving._MembershipDistancePage([{"npi": 2, "distance_miles": 1.0}], 1, (1.0, 2)),
            serving._MembershipDistancePage([matched_address_by_field], 1, (2.0, 3)),
        ]
    )
    membership_reader = AsyncMock(side_effect=({}, {2: (9,)}, {3: (9, 10, 11)}))
    classification_states = []

    async def classify(_session, _tables, memberships, _request, state):
        classification_states.append(state)
        if 2 in memberships:
            state.noncode_sets.add(9)
        if 3 in memberships:
            assert 9 in state.noncode_sets
            state.code_sets.update((10, 11))

    monkeypatch.setattr(serving, "_membership_distance_page", page_reader)
    monkeypatch.setattr(serving, "_local_v4_memberships", membership_reader)
    monkeypatch.setattr(serving, "_classify_local_code_sets", classify)
    tables = _local_distance_tables()
    request = serving._LocalDistanceGraphRequest(1, [{"code_key": 7}], serving._v4_geo_rate_forward_limits(tables))
    after = (0.5, 1)

    if expected_reads == 2:
        with pytest.raises(serving.PTG2ManifestArtifactError, match="configured exactness bound"):
            await serving._seek_local_distance_graph(object(), tables, {}, request, 1, maximum_candidates, after=after)
    else:
        candidates = await serving._seek_local_distance_graph(
            object(), tables, {}, request, 1, maximum_candidates, after=after
        )
        assert candidates == serving._RankedDistanceCandidates(
            serving._GraphLocationCandidates([matched_address_by_field], {3: {10, 11}}, taxonomy_filtered=True),
            3,
            (2.0, 3),
        )

    assert page_reader.await_count == expected_reads
    assert [call.kwargs["after"] for call in page_reader.await_args_list] == [after, (1.0, 1), (1.0, 2)][
        :expected_reads
    ]
    assert all(
        1 <= call.kwargs["limit"] <= maximum_candidates - index
        for index, call in enumerate(page_reader.await_args_list)
    )
    assert all(state is classification_states[0] for state in classification_states)


@pytest.mark.asyncio
async def test_ranked_distance_graph_filters_explicit_taxonomy_before_rate_membership(monkeypatch):
    excluded_address_by_field = {"npi": 1, "distance_miles": 1.0}
    included_address_by_field = {"npi": 2, "distance_miles": 2.0}
    reader = AsyncMock(
        side_effect=[
            serving._MembershipDistancePage([excluded_address_by_field], 1, (1.0, 1)),
            serving._MembershipDistancePage([included_address_by_field], 1, (2.0, 2)),
        ]
    )
    taxonomy = AsyncMock(side_effect=[(), (2,)])
    memberships = AsyncMock(side_effect=[{}, {2: (9,)}])

    async def classify(_session, _tables, member_sets, _request, state):
        if member_sets:
            state.code_sets.add(9)

    monkeypatch.setattr(serving, "_membership_distance_page", reader)
    monkeypatch.setattr(serving, "_filter_npis_by_taxonomy", taxonomy)
    monkeypatch.setattr(serving, "_local_v4_memberships", memberships)
    monkeypatch.setattr(serving, "_classify_local_code_sets", classify)
    tables = _local_distance_tables()
    graph_request = serving._LocalDistanceGraphRequest(
        1, [{"code_key": 7}], serving._v4_geo_rate_forward_limits(tables)
    )
    options_by_field = {"taxonomy_code": "207Q00000X", "provider_sex_code": "F"}
    candidates = await serving._seek_local_distance_graph(object(), tables, options_by_field, graph_request, 2, 2)
    assert candidates == serving._RankedDistanceCandidates(
        serving._GraphLocationCandidates([included_address_by_field], {2: {9}}, taxonomy_filtered=True), 2, (2.0, 2)
    )
    assert [call.args[2] for call in taxonomy.await_args_list] == [(1,), (2,)]
    assert [call.args[2] for call in memberships.await_args_list] == [(), (2,)]


@pytest.mark.asyncio
@pytest.mark.parametrize("namespace", ["ordinary_tiger", "reference_family_archive_invalid", "mixed"])
async def test_distance_inheritance_only_declines_unclaimed_ordinary_children(monkeypatch, namespace):
    from api import ptg_membership_distance as distance
    from process import tiger_snapshot_inheritance as inherited
    from process.reference_family_archive import ReferenceFamilyArchiveError

    rows = [("zcta5", 1, namespace, True), ("zip_state", 2, namespace, True)]
    if namespace == "mixed":
        rows = [("zcta5", 1, "reference_family_archive_" + "f" * 32, True), ("zip_state", 2, "ordinary_tiger", True)]
    session = SimpleNamespace(execute=AsyncMock(return_value=SimpleNamespace(all=lambda: rows)))
    with pytest.raises(ReferenceFamilyArchiveError):
        await inherited.current_tiger_snapshot_children(session)
    reads = AsyncMock(side_effect=AssertionError("unsupported children must not be read"))
    monkeypatch.setattr(distance, "execute_network_read", reads)
    if namespace == "ordinary_tiger":
        assert await distance._distance_tiger_identity(session, {}) is None
    else:
        with pytest.raises(ReferenceFamilyArchiveError):
            await distance._distance_tiger_identity(session, {})
    reads.assert_not_awaited()


def _install_generation_reads(monkeypatch):
    names_by_family = {
        "npi": distance.npi_generation.RELATION_NAMES,
        "address": distance.address_generation.RELATION_NAMES,
        **{
            family: distance.reference_generation.RELATION_NAMES_BY_IMPORTER[family]
            for family in ("cms-doctors", "mrf-address", "geo", "tiger", "nucc")
        },
    }
    names = {
        f"{'tiger' if family == 'tiger' else 'mrf'}.{name}"
        for family, names in names_by_family.items()
        for name in names
    }
    oid_by_relation = {name: index for index, name in enumerate(sorted(names), 1)}
    authority_by_family = {}
    for family, names in names_by_family.items():
        namespace = "tiger" if family == "tiger" else "mrf"
        oids = tuple(oid_by_relation[f"{namespace}.{name}"] for name in names)
        authority_by_family[family] = SimpleNamespace(
            serving_generation=1, relation_oids=oids, as_dict=lambda oids=oids: {"generation": 1, "oids": oids}
        )
    for module, name, family in (
        (distance.npi_generation, "read_npi_result_generation_authority", "npi"),
        (distance.address_generation, "read_entity_address_result_generation_authority", "address"),
    ):
        monkeypatch.setattr(module, name, AsyncMock(return_value=authority_by_family[family]))
    families = tuple(family for family in names_by_family if family not in {"npi", "address"})

    def family_rows(_session, statement, parameters):
        assert str(statement).startswith('SELECT importer_id FROM "mrf".reference_family_result_generation ')
        assert parameters == {"families": list(distance._DISTANCE_REFERENCE_FAMILIES)}
        return SimpleNamespace(scalars=lambda: families)

    query = AsyncMock(side_effect=family_rows)
    monkeypatch.setattr(distance, "execute_network_read", query)
    monkeypatch.setattr(
        distance.reference_generation,
        "read_reference_family_result_generation_authority",
        AsyncMock(side_effect=lambda _, importer_id, schema_name: authority_by_family[importer_id]),
    )
    session = SimpleNamespace(execute=AsyncMock(side_effect=AssertionError("unmocked database read")))
    return session, oid_by_relation, authority_by_family, query


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("family", "has_drift"),
    [("npi", False), ("address", False), ("cms-doctors", True), ("mrf-address", False), ("geo", False)],
)
async def test_distance_generation_capability_fallback_does_not_hide_published_drift(monkeypatch, family, has_drift):
    session, oids, authorities, query = _install_generation_reads(monkeypatch)
    authority = authorities[family]
    if has_drift:
        authority.relation_oids = tuple(oid + 1000 for oid in authority.relation_oids)
        with pytest.raises(BillingSearchCursorGenerationExpired):
            await distance._distance_generations(session, "mrf", oids, {}, inherited_tiger=False)
    else:
        authority.serving_generation = None
        assert await distance._distance_generations(session, "mrf", oids, {}, inherited_tiger=False) is None
    if family in {"npi", "address"}:
        query.assert_not_awaited()
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("uses_classification", [False, True])
async def test_distance_requires_nucc_generation_only_for_classification_predicates(monkeypatch, uses_classification):
    session, oids, authorities, _ = _install_generation_reads(monkeypatch)
    authorities["nucc"].serving_generation = None
    args = (
        {"classification": "Family Medicine", "include_subspecialties": True}
        if uses_classification
        else {"taxonomy_codes": "207Q00000X"}
    )

    result = await distance._distance_generations(session, "mrf", oids, args, inherited_tiger=False)

    if uses_classification:
        assert result is None
    else:
        assert set(result["reference"]) == {"cms-doctors", "mrf-address", "geo", "tiger"}
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("has_children", [False, True])
async def test_tiger_probe_refuses_flat_drift_and_nonempty_inherited_parents(monkeypatch, has_children):
    children = (("zip_state", 10), ("zcta5", 11)) if has_children else ()
    monkeypatch.setattr(distance, "current_tiger_snapshot_children", AsyncMock(return_value=children))
    reads = AsyncMock(side_effect=[SimpleNamespace(scalar=lambda: 10), SimpleNamespace(scalar_one=lambda: True)])
    monkeypatch.setattr(distance, "execute_network_read", reads)
    session = SimpleNamespace(execute=AsyncMock(side_effect=AssertionError("unmocked database read")))
    if has_children:
        assert await distance._distance_tiger_identity(session, {"tiger.zip_state": 1, "tiger.zcta5": 2}) is None
        assert reads.await_count == 2
    else:
        with pytest.raises(BillingSearchCursorGenerationExpired):
            await distance._distance_tiger_identity(session, {"tiger.zip_state": 1, "tiger.zcta5": 2})
        reads.assert_awaited_once()
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["relation_oid", "relfilenode"])
async def test_protected_distance_leaf_identity_must_match_its_pinned_catalog(monkeypatch, field):
    proof_by_field = {"relation_oid": 10, "relfilenode": 20}
    proof_by_field[field] += 1
    reader = AsyncMock(return_value=proof_by_field)
    monkeypatch.setattr(distance, "read_protected_tiger_relation", reader)
    with pytest.raises(serving.PTG2ManifestArtifactError, match="protected geographic inputs changed"):
        await distance._distance_child_proofs(object(), [("held_tiger", "zip_state", 10, 20)])
    assert reader.await_args.args[1] == '"held_tiger"."zip_state"'


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["missing-child", "renamed-child", "missing-generation", "changed-geo"])
async def test_distance_snapshot_refusal_restores_original_read_inventory(monkeypatch, failure):
    catalog_rows = [
        ("mrf.npi", 1, 1),
        ("tiger.zip_state", 2, 2),
        ("mrf.entity_address_geo_assurance_state", None, None),
    ]
    child_rows = [("held_tiger", "zip_state", 10, 20)]
    if failure == "missing-child":
        child_rows.clear()
    if failure == "renamed-child":
        child_rows[0] = ("held_tiger", "renamed", 10, 20)
    session = SimpleNamespace(execute=AsyncMock(side_effect=AssertionError("unmocked database read")))
    driver = SimpleNamespace(get_server_pid=lambda: 17)
    monkeypatch.setattr(distance, "_distance_input_catalog", AsyncMock(return_value=catalog_rows))
    monkeypatch.setattr(distance, "_original_read_driver", AsyncMock(return_value=(driver, 17)))
    monkeypatch.setattr(distance, "_distance_tiger_identity", AsyncMock(return_value={"zip_state": 10}))
    generations = AsyncMock(return_value=None if failure == "missing-generation" else {"generation": 1})
    monkeypatch.setattr(distance, "_distance_generations", generations)
    geo = AsyncMock(side_effect=[{"version": 1}, {"version": 2}])
    monkeypatch.setattr(distance, "_distance_geo_identity", geo)
    monkeypatch.setattr(
        distance, "read_protected_tiger_relation", AsyncMock(return_value={"relation_oid": 10, "relfilenode": 20})
    )

    async def query(reader, statement, parameters=None):
        assert reader is session
        if "FROM pg_catalog.pg_class child" in str(statement):
            assert parameters == {"oids": [10]}
            return SimpleNamespace(all=lambda: child_rows)
        assert str(statement) == "SELECT 1" and parameters is None
        return None

    monkeypatch.setattr(distance, "execute_network_read", query)
    identities = []
    expectation = (
        nullcontext() if failure == "missing-generation" else pytest.raises(BillingSearchCursorGenerationExpired)
    )
    assert not has_network_read_inventory()
    with expectation:
        async with distance.distance_input_snapshot(session, "mrf", {}) as identity:
            assert has_network_read_inventory()
            identities.append(identity)
    assert not has_network_read_inventory()
    session.execute.assert_not_awaited()
    if failure in {"missing-child", "renamed-child"}:
        generations.assert_not_awaited()
        assert identities == []
    elif failure == "missing-generation":
        geo.assert_not_awaited()
        assert identities == [None]
    else:
        assert identities[0]["geo"] == {"version": 1}
        assert geo.await_count == 2


@pytest.mark.parametrize("nearest_filters", [0, 2])
def test_seek_rejects_ambiguous_nearest_office_filter(monkeypatch, nearest_filters):
    query = SimpleNamespace(parameter_map={})
    monkeypatch.setattr(
        serving, "_membership_location_sql", lambda *_, **__: "WHERE address_rank = 1 " * nearest_filters
    )
    with pytest.raises(serving.PTG2ManifestArtifactError, match="invalid nearest-address filter"):
        serving._ranked_membership_sql(query, 1, (1.0, 1), False)


@pytest.mark.asyncio
@pytest.mark.parametrize("unavailable", ["empty-scope", "query", "finalized-addresses"])
async def test_ranked_membership_distinguishes_empty_scope_and_unavailable_reads(monkeypatch, unavailable):
    session = SimpleNamespace(execute=AsyncMock(side_effect=AssertionError("unmocked database read")))
    query = AsyncMock(return_value=None if unavailable == "query" else SimpleNamespace(parameter_map={}))
    rows = AsyncMock(return_value=[{"npi": 1, "distance_miles": None}])
    finalize = AsyncMock(return_value=None)
    monkeypatch.setattr(serving, "_membership_location_query", query)
    monkeypatch.setattr(serving, "_ranked_membership_sql", lambda *_: "synthetic bounded query")
    monkeypatch.setattr(serving, "_execute_ranked_membership_sql", rows)
    monkeypatch.setattr(serving, "_finalize_location_rows", finalize)
    result = await serving._membership_distance_page(
        session, _local_distance_tables(), {}, candidate_npis=() if unavailable == "empty-scope" else None, limit=1
    )
    if unavailable == "empty-scope":
        assert result == serving._MembershipDistancePage([], 0, None)
        query.assert_not_awaited()
    else:
        assert result is None
    if unavailable != "finalized-addresses":
        rows.assert_not_awaited()
        finalize.assert_not_awaited()
    else:
        assert finalize.await_args.args[3] == [{"npi": 1, "distance_miles": None}]
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_unavailable_ranked_membership_page_stops_before_graph_classification(monkeypatch):
    page = AsyncMock(return_value=None)
    append = AsyncMock(side_effect=AssertionError("unavailable addresses must not reach graph classification"))
    monkeypatch.setattr(serving, "_membership_distance_page", page)
    monkeypatch.setattr(serving, "_append_local_distance_batch", append)
    tables = _local_distance_tables()
    request = serving._LocalDistanceGraphRequest(1, [], serving._v4_geo_rate_forward_limits(tables))

    assert await serving._seek_local_distance_graph(object(), tables, {}, request, 1, 2) is None

    page.assert_awaited_once()
    append.assert_not_awaited()
