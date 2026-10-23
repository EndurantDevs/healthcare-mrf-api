# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Keep complete code membership while omitting unrelated factor edges."""

import hashlib
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import ptg2_serving as serving
from api import ptg2_v4_graph as graph
from api import ptg_provider_membership_build as build
from api.ptg2_db_sidecars import ForwardReadBudgetExceeded
from api.ptg2_v4_graph import V4GraphRoot
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError
from process.registry_ptg_graph_reader import RegistryPTGGraphReadError
from tests.test_ptg_membership_build_guards import _factor, _result
from tests.test_ptg_membership_projection import PARAMS


def _inverse(monkeypatch, representation="pattern_v1", *, members=None, logical_count=4):
    relation, inverse = (
        ("pattern_sets", "set_patterns")
        if representation == "pattern_v1"
        else ("group_sets_direct", "set_groups_direct")
    )
    manifest_by_relation = {
        relation: SimpleNamespace(owner_base=0, owner_count=3, logical_member_count=4),
        inverse: SimpleNamespace(owner_base=10, owner_count=3, logical_member_count=logical_count),
    }
    values = {10: (0,), 11: (0, 1), 12: (2,)} if members is None else members
    monkeypatch.setattr(serving, "_payload_schema", lambda _tables: "synthetic")
    monkeypatch.setattr(
        build,
        "load_v4_relation_manifest",
        AsyncMock(side_effect=lambda _session, **kw: manifest_by_relation[kw["relation"]]),
    )
    lookup = AsyncMock(
        side_effect=lambda _session, **kw: {key: values[key] for key in kw["owner_keys"] if key in values}
    )
    copy = AsyncMock()
    monkeypatch.setattr(build, "lookup_v4_relation_members", lookup)
    monkeypatch.setattr(build, "_copy", copy)
    return V4GraphRoot(19, representation, b"a" * 32), lookup, copy, relation, inverse


@pytest.mark.asyncio
@pytest.mark.parametrize("representation", ["pattern_v1", "direct_v1"])
@pytest.mark.parametrize("sets", [(), (10,), (11,), (10, 11), (10, 11, 12)])
async def test_code_factor_transpose_preserves_full_join_parity_and_owner_domain(monkeypatch, representation, sets):
    root, lookup, copy, relation, inverse = _inverse(monkeypatch, representation)
    result = await build._copy_code_factor(object(), object(), root, object(), sets, hashlib.sha256(), edge_allowance=4)
    retained_pairs = {row for call in copy.call_args_list for row in call.args[3]}
    full_pairs = {(0, 10), (0, 11), (1, 11), (2, 12)}
    first_pairs = {(0, 0), (0, 1), (1, 1), (2, 2)}
    assert retained_pairs == {(factor, key) for factor, key in full_pairs if key in sets}
    assert {(npi, key) for npi, factor in first_pairs for owner, key in retained_pairs if factor == owner} == {
        (npi, key) for npi, factor in first_pairs for owner, key in full_pairs if factor == owner and key in sets
    }
    assert result == dict(relation=relation, owner_base=0, owner_count=3, edge_count=len(retained_pairs))
    assert all(call.kwargs["relation"] == inverse for call in lookup.call_args_list)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "members,sets,allowance,logical_count,message",
    [
        ({}, (10,), 4, 4, "owner coverage"),
        ({10: (0, 0)}, (10,), 4, 4, "noncanonical"),
        ({10: (1, 0)}, (10,), 4, 4, "noncanonical"),
        ({10: (-1,)}, (10,), 4, 4, "out of domain"),
        ({10: (3,)}, (10,), 4, 4, "out of domain"),
        ({10: (0, 1)}, (10,), 1, 4, "member allowance"),
        ({10: (0,)}, (9,), 4, 4, "inverse factor domain"),
        ({10: (0,)}, (10,), 4, 5, "inverse factor domain"),
    ],
)
async def test_code_factor_rejects_incomplete_or_out_of_budget_source_before_copy(
    monkeypatch, members, sets, allowance, logical_count, message
):
    root, lookup, copy, _, _ = _inverse(monkeypatch, members=members, logical_count=logical_count)
    with pytest.raises(PTG2ManifestArtifactError, match=message):
        await build._copy_code_factor(
            object(), object(), root, object(), sets, hashlib.sha256(), edge_allowance=allowance
        )
    copy.assert_not_awaited()
    if message == "inverse factor domain":
        lookup.assert_not_awaited()


@pytest.mark.asyncio
async def test_code_factor_accepts_explicit_empty_owner(monkeypatch):
    root, _, copy, _, _ = _inverse(monkeypatch, members={10: ()})
    result = await build._copy_code_factor(
        object(), object(), root, object(), (10,), hashlib.sha256(), edge_allowance=4
    )
    assert result["edge_count"] == 0
    assert all(call.args[3] == [] for call in copy.call_args_list)


@pytest.mark.asyncio
async def test_code_factor_counts_selected_edges_independently_of_full_source_count(monkeypatch):
    root, _, _, relation, inverse = _inverse(monkeypatch)
    manifest_by_relation = {
        relation: SimpleNamespace(owner_base=0, owner_count=3, logical_member_count=build.MAX_BUILD_EDGES + 1),
        inverse: SimpleNamespace(
            owner_base=0, owner_count=build.MAX_BUILD_EDGES, logical_member_count=build.MAX_BUILD_EDGES + 1
        ),
    }
    monkeypatch.setattr(
        build,
        "load_v4_relation_manifest",
        AsyncMock(side_effect=lambda _session, **kw: manifest_by_relation[kw["relation"]]),
    )
    result = await build._copy_code_factor(
        object(), object(), root, object(), (10,), hashlib.sha256(), edge_allowance=1
    )
    assert result["edge_count"] == 1 and result["owner_count"] == 3


@pytest.mark.asyncio
async def test_first_factor_coalesces_the_sealed_locator_owner_span(monkeypatch):
    lookup, _, _ = _factor(monkeypatch, owners=5, count=100, span=4)
    await build._copy_relation(
        object(),
        object(),
        V4GraphRoot(19, "pattern_v1", b"a" * 32),
        "npi_patterns",
        SimpleNamespace(schema_name="synthetic"),
        build.PTGMembershipFirst,
        hashlib.sha256(),
        edge_allowance=100,
    )
    assert [call.kwargs["owner_keys"] for call in lookup.call_args_list] == [(0, 1, 2, 3), (4,)]


@pytest.mark.asyncio
async def test_complete_physical_aliases_union_sets_and_share_one_forward_budget(monkeypatch):
    code_rows = [{"code_key": 7, "rate_count": 3}, {"code_key": 8, "rate_count": 2}, {"code_key": 9, "rate_count": 0}]
    occurrences_by_code = {7: [(10, 0, 0), (10, 1, 0), (11, 1, 0)], 8: [(11, 0, 0), (12, 1, 0)], 9: []}
    monkeypatch.setattr(build, "_physical_code_rows", AsyncMock(return_value=code_rows))
    for name, setting in (
        ("_required_shared_snapshot_key", 19),
        ("_required_source_count", 1),
        ("_version_three_forward_lookup_hints", {}),
        ("_payload_schema", "synthetic"),
    ):
        monkeypatch.setattr(serving, name, lambda _tables, setting=setting: setting)
    budgets = []

    async def forward(_session, code_key, *, scan_budget, limit, **_kwargs):
        budgets.append(scan_budget)
        assert scan_budget.active_read_row_capacity == limit
        scan_budget.claim(4)
        return occurrences_by_code[code_key]

    monkeypatch.setattr(build, "lookup_code_prefix_occurrences_from_db", forward)
    copy = AsyncMock()
    monkeypatch.setattr(build, "_copy", copy)
    coverage, sets, raw_bytes = await build._copy_code(
        object(),
        object(),
        PARAMS | {"finalizer": {"state": "legacy_relational"}},
        object(),
        hashlib.sha256(),
        byte_allowance=12,
    )
    assert sets == (10, 11, 12) and raw_bytes == 12
    assert [code_row["occurrence_count"] for code_row in coverage] == [3, 2, 0]
    assert [code_row["set_count"] for code_row in coverage] == [2, 2, 0]
    assert len({id(budget) for budget in budgets}) == 1
    assert budgets[0].active_read_row_capacity == budgets[0].active_result_row_capacity == 0
    assert coverage[2]["set_digest"] == hashlib.sha256(b"").hexdigest()


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid,maximum", [(0, 40), (0, 39), (1, 40)])
async def test_forward_map_reservation_counts_both_visits_before_payload(monkeypatch, invalid, maximum):
    session = SimpleNamespace(execute=AsyncMock(return_value=_result(dict(pages=2, raw_bytes=20, invalid=invalid))))
    monkeypatch.setattr(serving, "_payload_schema", lambda _tables: "synthetic")
    monkeypatch.setattr(serving, "_required_shared_snapshot_key", lambda _tables: 19)
    budget = build.ForwardReadBudget(maximum_fragments=4, maximum_raw_payload_bytes=maximum)
    if invalid or maximum < 40:
        with pytest.raises((PTG2ManifestArtifactError, ForwardReadBudgetExceeded)):
            await build._reserve_forward_maps(session, object(), {"finalizer": {"state": "packed"}}, 7, budget)
        assert budget.raw_payload_bytes == 0
    else:
        await build._reserve_forward_maps(session, object(), {"finalizer": {"state": "packed"}}, 7, budget)
        assert budget.raw_payload_bytes == 40 and budget.fragment_count == 4
    statement, params = session.execute.call_args.args
    assert "LEFT JOIN" in str(statement) and "block.block_hash IS NULL" in str(statement)
    assert params == {"key": 19, "lower": 7 * (1 << 31), "upper": 8 * (1 << 31)}


@pytest.mark.asyncio
async def test_one_source_budget_survives_both_factors_and_refuses_before_seal(monkeypatch):
    root = V4GraphRoot(19, "pattern_v1", b"a" * 32)
    monkeypatch.setattr(build, "_MAX_SOURCE_READ_BYTES", 1024)
    monkeypatch.setattr(build, "source_identity", AsyncMock(return_value=(object(), root, PARAMS)))
    for name in ("lock_identity", "source_pin", "_copy"):
        monkeypatch.setattr(build, name, AsyncMock())
    monkeypatch.setattr(build, "locate", AsyncMock(return_value=None))
    monkeypatch.setattr(build, "_authenticate_root", AsyncMock(return_value=100))
    monkeypatch.setattr(build, "_copy_code", AsyncMock(return_value=([], (), 100)))
    monkeypatch.setattr(build.native, "protected_publisher_owner", AsyncMock())
    monkeypatch.setattr(build.native, "precreate_model_family_stage", AsyncMock())
    budgets = []

    async def factor(*_args, **_kwargs):
        budget = graph._ACTIVE_V4_GRAPH_REQUEST_IO.get().read_budget
        budgets.append(budget)
        assert budget.maximum_bytes == 824
        budget.reserve([{"raw_byte_count": 600}])
        return {"edge_count": 0}

    monkeypatch.setattr(build, "_copy_relation", factor)
    monkeypatch.setattr(build, "_copy_code_factor", factor)
    seal = AsyncMock()
    monkeypatch.setattr(build, "seal", seal)
    with pytest.raises(RegistryPTGGraphReadError, match="graph_budget"):
        await build.build_in_session(object(), **PARAMS)
    assert len(budgets) == 2 and budgets[0] is budgets[1]
    assert budgets[0].read_bytes == 600
    seal.assert_not_awaited()
