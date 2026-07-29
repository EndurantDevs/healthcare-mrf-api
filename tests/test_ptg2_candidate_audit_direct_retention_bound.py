# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Executable boundary proof for adaptive direct-layout audit traversal."""

from __future__ import annotations

from dataclasses import replace
from unittest.mock import AsyncMock

import pytest

from api import ptg2_candidate_audit_reverse as reverse_scope
from api import ptg2_candidate_audit_scope_dispatch as scope_dispatch
from api import ptg2_candidate_audit_v4_direct as direct_scope
from api import ptg2_db_sidecars as sidecars
from api.ptg2_candidate_audit_capacity import (
    CandidateAuditDecodedRetentionBudget,
)
from api.ptg2_candidate_audit_codes import CandidateCodeIndex
from process.ptg_parts.ptg2_candidate_audit_batch_contract import (
    AuditBatchChallenge,
)
from tests.test_ptg2_candidate_audit_direct_fallback import (
    _install_direct_root,
    _install_graph_results,
    _rate_count_index,
)
from tests.test_ptg2_candidate_audit_v4 import (
    _challenge,
    _code_index,
    _persisted_occurrence,
    _v4_serving_tables,
)
from tests.test_ptg2_v3_bounded_readers import (
    _fragment,
    _fragment_row,
    _grouped_payload,
    _shard_block_key,
)


class _SingleForwardShardSession:
    def __init__(self, block_key: int) -> None:
        self.block_key = block_key
        self.stream_count = 0

    async def stream(self, _statement, _params):
        self.stream_count += 1
        return [{"code_key": 7, "block_key": self.block_key}]


def _single_code_index(rate_count: int = 1) -> CandidateCodeIndex:
    code_fields_by_name = {
        **_code_index().by_key[7],
        "rate_count": rate_count,
    }
    return CandidateCodeIndex(
        by_pair={("CPT", "99213"): (code_fields_by_name,)},
        by_key={7: code_fields_by_name},
    )


def _required_bytes(
    code_index: CandidateCodeIndex,
) -> tuple[AuditBatchChallenge, int]:
    challenge = _challenge()
    return challenge, direct_scope.direct_code_first_retention_upper_bound(
        code_index,
        (challenge,),
        (),
    )


def test_direct_strategy_uses_exact_available_budget_boundary():
    code_index = _rate_count_index(_code_index(), 17)
    challenge, required_bytes = _required_bytes(code_index)

    assert direct_scope.should_load_direct_graph_first(
        code_index,
        CandidateAuditDecodedRetentionBudget(
            maximum_bytes=required_bytes
        ),
        challenges=(challenge,),
    ) is False
    assert direct_scope.should_load_direct_graph_first(
        code_index,
        CandidateAuditDecodedRetentionBudget(
            maximum_bytes=required_bytes - 1
        ),
        challenges=(challenge,),
    ) is True


def test_direct_cardinality_deduplicates_npis_and_adds_persisted_edge():
    challenge = _challenge()
    cardinality = direct_scope._direct_code_first_cardinality(
        _single_code_index(rate_count=5),
        (challenge, replace(challenge, multiplicity=2)),
        (
            replace(
                _persisted_occurrence(),
                code_key=7,
                source_artifact_key=0,
            ),
        ),
    )

    assert cardinality.code_source_pair_count == 1
    assert cardinality.code_source_npi_membership_count == 1
    assert cardinality.npi_count == 2
    assert cardinality.candidate_membership_count == 6


def _install_forward_fragment(monkeypatch, block_key: int) -> AsyncMock:
    fetch_fragments = AsyncMock(
        return_value=[
            _fragment_row(
                _fragment(
                    _grouped_payload(2, [(5, [(8, 0)])]),
                    entry_count=1,
                    block_key=block_key,
                )
            )
        ]
    )
    monkeypatch.setattr(
        sidecars,
        "_shared_serving_binary_payload_rows_for_keys",
        fetch_fragments,
    )
    return fetch_fragments


@pytest.mark.asyncio
async def test_direct_exact_bound_completes_real_code_first_scope(
    monkeypatch,
):
    """The selected code-first path completes under its advertised bound."""

    code_index = _single_code_index()
    challenge, required_bytes = _required_bytes(code_index)
    budget = CandidateAuditDecodedRetentionBudget(
        maximum_bytes=required_bytes
    )
    block_key = _shard_block_key(7, 5)
    session = _SingleForwardShardSession(block_key)
    fetch_fragments = _install_forward_fragment(monkeypatch, block_key)
    graph_calls = _install_graph_results(
        monkeypatch,
        {challenge.npi: (5,)},
    )
    _install_direct_root(monkeypatch)

    observed = await reverse_scope.load_candidate_provider_scope(
        AsyncMock(),
        session,
        _v4_serving_tables(),
        (challenge,),
        (),
        code_index,
        schema_name="candidate_schema",
        retention_budget=budget,
    )

    assert observed.provider_set_keys_by_npi == {challenge.npi: (5,)}
    assert observed.price_keys_by_occurrence == {(7, 5, 0): (8,)}
    assert graph_calls[0]["allowed"] == {5}
    assert session.stream_count == 1
    fetch_fragments.assert_awaited_once()
    assert budget.peak_retained_bytes <= required_bytes


@pytest.mark.asyncio
async def test_direct_one_under_uses_graph_before_forward_io(monkeypatch):
    """A one-byte-short budget selects graph proof before forward I/O."""

    code_index = _single_code_index()
    challenge, required_bytes = _required_bytes(code_index)
    graph_first = AsyncMock(return_value={challenge.npi: (5,)})
    code_first = AsyncMock(
        side_effect=AssertionError("forward code-first I/O must not start")
    )
    _install_direct_root(monkeypatch)
    monkeypatch.setattr(
        scope_dispatch,
        "load_v4_direct_provider_scope",
        graph_first,
    )
    monkeypatch.setattr(scope_dispatch, "load_v4_candidate_scope", code_first)

    observed = await reverse_scope.load_candidate_provider_scope(
        AsyncMock(),
        object(),
        _v4_serving_tables(),
        (challenge,),
        (),
        code_index,
        retention_budget=CandidateAuditDecodedRetentionBudget(
            maximum_bytes=required_bytes - 1
        ),
    )

    assert observed.provider_set_keys_by_npi == {challenge.npi: (5,)}
    assert observed.price_keys_by_occurrence is None
    graph_first.assert_awaited_once()
    code_first.assert_not_awaited()


@pytest.mark.asyncio
async def test_shared_source_npi_fanout_selects_graph_before_forward_io(
    monkeypatch,
):
    """Account the code/source-to-NPI Cartesian candidate expansion."""

    code_index = _single_code_index(rate_count=50_000)
    base_challenge = _challenge()
    challenges = tuple(
        replace(
            base_challenge,
            npi=1_000_000_000 + ordinal,
            tuple_digest=f"{ordinal:064x}",
        )
        for ordinal in range(100)
    )
    cardinality = direct_scope._direct_code_first_cardinality(
        code_index,
        challenges,
        (),
    )
    budget = CandidateAuditDecodedRetentionBudget(
        maximum_bytes=512 * 1024 * 1024
    )
    graph_first = AsyncMock(
        return_value={challenge.npi: () for challenge in challenges}
    )
    code_first = AsyncMock(
        side_effect=AssertionError("broad forward I/O must not start")
    )
    _install_direct_root(monkeypatch)
    monkeypatch.setattr(
        scope_dispatch,
        "load_v4_direct_provider_scope",
        graph_first,
    )
    monkeypatch.setattr(scope_dispatch, "load_v4_candidate_scope", code_first)

    observed = await reverse_scope.load_candidate_provider_scope(
        AsyncMock(),
        object(),
        _v4_serving_tables(),
        challenges,
        (),
        code_index,
        retention_budget=budget,
    )

    assert cardinality.candidate_membership_count == 5_000_000
    assert direct_scope.direct_code_first_retention_upper_bound(
        code_index,
        challenges,
        (),
    ) > budget.maximum_bytes
    assert set(observed.provider_set_keys_by_npi) == {
        challenge.npi for challenge in challenges
    }
    graph_first.assert_awaited_once()
    code_first.assert_not_awaited()
