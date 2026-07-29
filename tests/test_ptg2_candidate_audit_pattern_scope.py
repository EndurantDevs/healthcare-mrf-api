from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from api import ptg2_candidate_audit_reverse as reverse_scope
from api import ptg2_candidate_audit_scope_dispatch as scope_dispatch
from api import ptg2_candidate_audit_v4 as v4_scope
from api.ptg2_candidate_audit_integrity import PersistedAuditOccurrence
from api.ptg2_v4_graph import V4GraphRoot
from process.ptg_parts.ptg2_candidate_audit_batch_contract import (
    AuditBatchChallenge,
)
from tests.test_ptg2_candidate_audit_v4 import (
    _challenge,
    _code_index,
    _v4_serving_tables,
)


def _pattern_scope_fixture():
    membership_counts = (4, 4, 4, 2, 2, *(1 for _ in range(20)))
    challenges = tuple(
        AuditBatchChallenge(
            code_system="CPT",
            code="99213",
            npi=1_000_000_001 + index,
            source_artifact_key=0,
            tuple_digest=f"{index:064x}",
            network_name_digests=("b" * 64,),
            multiplicity=1,
        )
        for index in range(25)
    )
    provider_sets_by_npi = {
        challenge.npi: tuple(
            10_000 + index * 10 + member_index
            for member_index in range(membership_counts[index])
        )
        for index, challenge in enumerate(challenges)
    }
    persisted = PersistedAuditOccurrence(
        occurrence_id=b"p" * 32,
        code_key=8,
        provider_set_key=provider_sets_by_npi[challenges[0].npi][0],
        price_key=9,
        source_artifact_key=1,
        npi=challenges[0].npi,
        atom_ordinal=0,
        atom_key=10,
    )
    return challenges, provider_sets_by_npi, persisted


def _assert_pattern_scope(
    observed_scope,
    provider_sets_by_npi,
    persisted,
    graph_calls,
):
    assert sum(map(len, observed_scope.provider_set_keys_by_npi.values())) == 36
    assert max(map(len, observed_scope.provider_set_keys_by_npi.values())) == 4
    assert observed_scope.provider_set_keys_by_npi == provider_sets_by_npi
    assert observed_scope.price_keys_by_occurrence is None
    assert provider_sets_by_npi[persisted.npi][0] == persisted.provider_set_key
    assert len(graph_calls) == 1
    assert graph_calls[0]["npis"] == tuple(sorted(provider_sets_by_npi))
    assert graph_calls[0]["allowed"] is None
    assert graph_calls[0]["schema_name"] == "candidate_schema"
    assert int(graph_calls[0]["max_members"]) > 0


@pytest.mark.asyncio
async def test_pattern_v4_candidate_scope_loads_graph_before_forward(
    monkeypatch,
):
    """Resolve the sparse pattern graph before any dense forward payload."""

    challenges, provider_sets_by_npi, persisted = _pattern_scope_fixture()
    root_lookup = AsyncMock(
        return_value=V4GraphRoot(
            snapshot_key=43,
            representation="pattern_v1",
            map_digest=b"m" * 32,
        )
    )
    forward_scope = AsyncMock()
    broad_scope_lookup = AsyncMock()
    graph_calls: list[dict[str, object]] = []

    async def graph_lookup(_session, _tables, npis, **kwargs):
        requested_npis = tuple(npis)
        graph_calls.append(
            {
                "npis": requested_npis,
                "allowed": kwargs["allowed_provider_set_keys"],
                "schema_name": kwargs["schema_name"],
                "max_members": kwargs["max_members"],
            }
        )
        return {npi: provider_sets_by_npi[npi] for npi in requested_npis}

    monkeypatch.setattr(scope_dispatch, "load_v4_graph_root", root_lookup)
    monkeypatch.setattr(
        scope_dispatch,
        "load_v4_candidate_scope",
        forward_scope,
    )
    monkeypatch.setattr(v4_scope, "_v4_sets_by_npi", graph_lookup)
    observed_scope = await reverse_scope.load_candidate_provider_scope(
        broad_scope_lookup,
        object(),
        _v4_serving_tables(),
        challenges,
        (persisted,),
        _code_index(),
        schema_name="candidate_schema",
    )

    _assert_pattern_scope(
        observed_scope,
        provider_sets_by_npi,
        persisted,
        graph_calls,
    )
    root_lookup.assert_awaited_once()
    forward_scope.assert_not_awaited()
    broad_scope_lookup.assert_not_awaited()


@pytest.mark.asyncio
async def test_pattern_v4_graph_requires_exact_requested_npi_coverage(
    monkeypatch,
):
    """Reject a partial batch result and release every request-local claim."""

    challenge = _challenge()
    baseline_bytes = 731
    budget = v4_scope.CandidateAuditDecodedRetentionBudget()
    budget.claim(baseline_bytes, category="the caller baseline")
    monkeypatch.setattr(
        v4_scope,
        "_v4_sets_by_npi",
        AsyncMock(return_value={}),
    )

    with pytest.raises(
        v4_scope.PTG2ManifestArtifactError,
        match="omitted a requested NPI",
    ):
        await v4_scope.load_v4_pattern_provider_scope(
            object(),
            _v4_serving_tables(),
            (challenge,),
            (),
            schema_name="candidate_schema",
            retention_budget=budget,
        )

    assert budget.retained_bytes == baseline_bytes


@pytest.mark.asyncio
async def test_pattern_v4_graph_failure_releases_budget_without_fallback(
    monkeypatch,
):
    """Fail closed and release transient graph-first retention exactly once."""

    challenge = _challenge()
    baseline_bytes = 731
    budget = v4_scope.CandidateAuditDecodedRetentionBudget()
    budget.claim(baseline_bytes, category="the caller baseline")
    forward_scope = AsyncMock()
    broad_scope_lookup = AsyncMock()
    graph_lookup = AsyncMock(side_effect=RuntimeError("pattern graph failed"))
    monkeypatch.setattr(
        scope_dispatch,
        "load_v4_graph_root",
        AsyncMock(
            return_value=V4GraphRoot(
                snapshot_key=43,
                representation="pattern_v1",
                map_digest=b"m" * 32,
            )
        ),
    )
    monkeypatch.setattr(
        scope_dispatch,
        "load_v4_candidate_scope",
        forward_scope,
    )
    monkeypatch.setattr(v4_scope, "_v4_sets_by_npi", graph_lookup)

    with pytest.raises(RuntimeError, match="pattern graph failed"):
        await reverse_scope.load_candidate_provider_scope(
            broad_scope_lookup,
            object(),
            _v4_serving_tables(),
            (challenge,),
            (),
            _code_index(),
            schema_name="candidate_schema",
            retention_budget=budget,
        )

    assert budget.retained_bytes == baseline_bytes
    graph_lookup.assert_awaited_once()
    forward_scope.assert_not_awaited()
    broad_scope_lookup.assert_not_awaited()
