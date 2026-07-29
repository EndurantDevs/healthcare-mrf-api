from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from api import ptg2_candidate_audit_reverse as reverse_scope
from api import ptg2_candidate_audit_scope_dispatch as scope_dispatch
from api import ptg2_candidate_audit_v4 as v4_scope
from api import ptg2_candidate_audit_v4_direct as direct_scope
from api.ptg2_candidate_audit_capacity import (
    CandidateAuditDecodedRetentionBudget,
    CandidateAuditDecodedRetentionError,
)
from api.ptg2_candidate_audit_codes import CandidateCodeIndex
from api.ptg2_db_sidecars import (
    forward_price_index_retention_upper_bound,
    forward_price_row_retention_upper_bound,
)
from api.ptg2_shared_blocks import PTG2SharedBlockError
from api.ptg2_v4_graph import V4GraphRoot
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError
from tests.test_ptg2_candidate_audit_v4 import (
    _challenge,
    _code_index,
    _persisted_occurrence,
    _v4_serving_tables,
)


_DENSE_RATE_COUNT = 1_000_000


def _rate_count_index(
    code_index: CandidateCodeIndex,
    rate_count: int,
) -> CandidateCodeIndex:
    """Replace sealed cardinalities while preserving shared code records."""

    records_by_key = {
        code_key: {**record, "rate_count": rate_count}
        for code_key, record in code_index.by_key.items()
    }
    return CandidateCodeIndex(
        by_pair={
            code_pair: tuple(
                records_by_key[int(record["code_key"])] for record in records
            )
            for code_pair, records in code_index.by_pair.items()
        },
        by_key=records_by_key,
    )


def _install_direct_root(monkeypatch) -> None:
    monkeypatch.setattr(
        scope_dispatch,
        "load_v4_graph_root",
        AsyncMock(return_value=V4GraphRoot(43, "direct_v1", b"d" * 32)),
    )


def _install_graph_results(monkeypatch, provider_sets_by_npi):
    graph_calls: list[dict[str, object]] = []

    async def graph_lookup(_session, _tables, npis, **kwargs):
        npi = tuple(npis)[0]
        graph_calls.append(
            {
                "npi": npi,
                "allowed": kwargs["allowed_provider_set_keys"],
                "max_members": kwargs["max_members"],
            }
        )
        return {npi: provider_sets_by_npi[npi]}

    monkeypatch.setattr(v4_scope, "_v4_sets_by_npi", graph_lookup)
    return graph_calls


@pytest.mark.asyncio
async def test_dense_direct_selects_graph_before_forward_io(monkeypatch):
    """Avoid reading a broad direct forward payload that cannot fit."""

    challenge = _challenge()
    persisted = _persisted_occurrence()
    expected_sets_by_npi = {challenge.npi: (5, 6), persisted.npi: (7,)}
    graph_calls = _install_graph_results(monkeypatch, expected_sets_by_npi)
    forward_lookup = AsyncMock(
        side_effect=AssertionError("broad forward read must not start")
    )
    _install_direct_root(monkeypatch)
    monkeypatch.setattr(
        v4_scope,
        "lookup_forward_price_index_from_db",
        forward_lookup,
    )

    observed = await reverse_scope.load_candidate_provider_scope(
        AsyncMock(),
        object(),
        _v4_serving_tables(),
        (challenge,),
        (persisted,),
        _rate_count_index(_code_index(), _DENSE_RATE_COUNT),
        schema_name="candidate_schema",
    )

    assert observed.provider_set_keys_by_npi == expected_sets_by_npi
    assert observed.price_keys_by_occurrence is None
    assert [call["npi"] for call in graph_calls] == sorted(expected_sets_by_npi)
    assert all(call["allowed"] is None for call in graph_calls)
    assert all(int(call["max_members"]) > 0 for call in graph_calls)
    forward_lookup.assert_not_awaited()


@pytest.mark.asyncio
async def test_small_direct_keeps_code_first_failure(monkeypatch):
    """Do not reinterpret a code-first retention failure as graph capacity."""

    code_first = AsyncMock(
        side_effect=CandidateAuditDecodedRetentionError(
            "unrelated retained result exceeded its limit"
        )
    )
    graph_first = AsyncMock()
    _install_direct_root(monkeypatch)
    monkeypatch.setattr(scope_dispatch, "load_v4_candidate_scope", code_first)
    monkeypatch.setattr(
        scope_dispatch,
        "load_v4_direct_provider_scope",
        graph_first,
    )

    with pytest.raises(
        CandidateAuditDecodedRetentionError,
        match="unrelated retained result",
    ):
        await reverse_scope.load_candidate_provider_scope(
            AsyncMock(),
            object(),
            _v4_serving_tables(),
            (_challenge(),),
            (),
            _code_index(),
            schema_name="candidate_schema",
        )

    code_first.assert_awaited_once()
    graph_first.assert_not_awaited()


@pytest.mark.asyncio
async def test_first_graph_coordinate_member_cap_uses_one_code_first_attempt(
    monkeypatch,
):
    """Fall back when the first dense graph coordinate hits its member cap."""

    expected = v4_scope.V4CandidateScope(
        provider_set_keys_by_npi={_challenge().npi: (5,)},
        price_keys_by_occurrence={(7, 5, 0): (10,)},
    )
    graph_lookup = AsyncMock(
        side_effect=PTG2SharedBlockError(
            "PTG V4 graph selection exceeds max_members"
        )
    )
    code_first = AsyncMock(return_value=expected)
    _install_direct_root(monkeypatch)
    monkeypatch.setattr(
        v4_scope,
        "_v4_sets_by_npi",
        graph_lookup,
    )
    monkeypatch.setattr(scope_dispatch, "load_v4_candidate_scope", code_first)

    observed = await reverse_scope.load_candidate_provider_scope(
        AsyncMock(),
        object(),
        _v4_serving_tables(),
        (_challenge(),),
        (),
        _rate_count_index(_code_index(), _DENSE_RATE_COUNT),
    )

    assert observed.provider_set_keys_by_npi == expected.provider_set_keys_by_npi
    assert observed.price_keys_by_occurrence == expected.price_keys_by_occurrence
    graph_lookup.assert_awaited_once()
    code_first.assert_awaited_once()


@pytest.mark.asyncio
async def test_later_graph_coordinate_member_cap_stays_closed(monkeypatch):
    """Never retry after one earlier graph coordinate was already proven."""

    challenge = _challenge()
    persisted = _persisted_occurrence()
    first_npi = min(challenge.npi, persisted.npi)
    graph_lookup = AsyncMock(
        side_effect=(
            {first_npi: (5,)},
            PTG2SharedBlockError(
                "PTG V4 graph selection exceeds max_members"
            ),
        )
    )
    code_first = AsyncMock()
    _install_direct_root(monkeypatch)
    monkeypatch.setattr(v4_scope, "_v4_sets_by_npi", graph_lookup)
    monkeypatch.setattr(scope_dispatch, "load_v4_candidate_scope", code_first)

    with pytest.raises(
        PTG2SharedBlockError,
        match="graph selection exceeds max_members",
    ):
        await reverse_scope.load_candidate_provider_scope(
            AsyncMock(),
            object(),
            _v4_serving_tables(),
            (challenge,),
            (persisted,),
            _rate_count_index(_code_index(), _DENSE_RATE_COUNT),
        )

    assert graph_lookup.await_count == 2
    code_first.assert_not_awaited()


@pytest.mark.asyncio
async def test_direct_graph_integrity_failure_stays_closed(monkeypatch):
    """Never convert a graph integrity failure into a broader forward read."""

    graph_first = AsyncMock(
        side_effect=PTG2ManifestArtifactError(
            "PTG2 V4 provider-group relation is incomplete"
        )
    )
    code_first = AsyncMock()
    _install_direct_root(monkeypatch)
    monkeypatch.setattr(
        scope_dispatch,
        "load_v4_direct_provider_scope",
        graph_first,
    )
    monkeypatch.setattr(scope_dispatch, "load_v4_candidate_scope", code_first)

    with pytest.raises(PTG2ManifestArtifactError, match="incomplete"):
        await reverse_scope.load_candidate_provider_scope(
            AsyncMock(),
            object(),
            _v4_serving_tables(),
            (_challenge(),),
            (),
            _rate_count_index(_code_index(), _DENSE_RATE_COUNT),
        )

    code_first.assert_not_awaited()


@pytest.mark.parametrize("rate_count", (None, True, "1", -1))
def test_direct_strategy_requires_authenticated_rate_count(rate_count):
    code_index = CandidateCodeIndex(
        by_pair={},
        by_key={7: {"code_key": 7, "rate_count": rate_count}},
    )

    with pytest.raises(
        PTG2ManifestArtifactError,
        match="declared rate count",
    ):
        direct_scope.should_load_direct_graph_first(
            code_index,
            CandidateAuditDecodedRetentionBudget(),
        )


def test_direct_strategy_rejects_non_mapping_code_metadata():
    code_index = CandidateCodeIndex(
        by_pair={},
        by_key={7: object()},
    )

    with pytest.raises(
        PTG2ManifestArtifactError,
        match="code metadata is invalid",
    ):
        direct_scope.should_load_direct_graph_first(
            code_index,
            CandidateAuditDecodedRetentionBudget(),
        )


def test_direct_capacity_classifier_accepts_only_typed_capacity_failures():
    assert direct_scope.is_direct_graph_capacity_failure(
        CandidateAuditDecodedRetentionError("retention exceeded")
    )
    assert direct_scope.is_direct_graph_capacity_failure(
        PTG2ManifestArtifactError(
            "PTG2 V4 graph selection exceeds max_members"
        )
    )
    assert not direct_scope.is_direct_graph_capacity_failure(
        PTG2SharedBlockError("graph digest mismatch")
    )


@pytest.mark.parametrize("rate_count", (None, True, "1", -1))
def test_forward_retention_bound_rejects_invalid_rate_count(rate_count):
    with pytest.raises(
        ValueError,
        match="rate count must not be negative",
    ):
        forward_price_row_retention_upper_bound(rate_count)
    with pytest.raises(
        ValueError,
        match="rate count must not be negative",
    ):
        forward_price_index_retention_upper_bound(rate_count, 1)


@pytest.mark.parametrize("code_count", (None, True, "1", -1))
def test_forward_index_bound_rejects_invalid_code_count(code_count):
    with pytest.raises(
        ValueError,
        match="code count must not be negative",
    ):
        forward_price_index_retention_upper_bound(1, code_count)


def test_direct_npi_scope_releases_claim_when_map_creation_fails(
    monkeypatch,
):
    class FailingDictionary:
        @classmethod
        def fromkeys(cls, _values):
            raise RuntimeError("dictionary allocation failed")

    budget = CandidateAuditDecodedRetentionBudget()
    budget.claim(41, category="caller baseline")
    monkeypatch.setattr(direct_scope, "dict", FailingDictionary, raising=False)

    with pytest.raises(RuntimeError, match="dictionary allocation failed"):
        direct_scope._direct_npi_scope((1, 2), budget)

    assert budget.retained_bytes == 41


@pytest.mark.asyncio
async def test_direct_scope_allocates_and_releases_default_budget(
    monkeypatch,
):
    graph_loader = AsyncMock(return_value={})
    monkeypatch.setattr(
        direct_scope,
        "_load_proven_v4_provider_sets",
        graph_loader,
    )

    assert (
        await direct_scope.load_v4_direct_provider_scope(
            object(),
            _v4_serving_tables(),
            (),
            (),
        )
        == {}
    )

    retention_budget = graph_loader.await_args.args[3]
    assert isinstance(
        retention_budget,
        CandidateAuditDecodedRetentionBudget,
    )
    assert retention_budget.retained_bytes == 0
