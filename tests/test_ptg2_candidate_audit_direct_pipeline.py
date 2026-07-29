from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import ptg2_candidate_audit_batch as candidate_batch
from api import ptg2_candidate_audit_v4 as v4_scope
from api import ptg2_candidate_audit_v4_direct as direct_scope
from api import ptg2_shared_blocks as shared_blocks
from api.ptg2_candidate_audit_capacity import CandidateAuditDecodedRetentionBudget
from api.ptg2_candidate_audit_codes import CandidateCodeIndex
from process.ptg_parts.ptg2_candidate_audit_evidence import (
    canonical_network_name_digests,
)
from tests.test_ptg2_candidate_audit_batch import (
    _TransactionSession,
    _access,
    _audit_request,
    _challenge as _matched_challenge,
    _price_payload,
)
from tests.test_ptg2_candidate_audit_direct_fallback import (
    _DENSE_RATE_COUNT,
    _install_direct_root,
    _install_graph_results,
    _rate_count_index,
)
from tests.test_ptg2_candidate_audit_pattern_scope import (
    _final_parity_code_index,
    _final_parity_harness,
    _final_parity_occurrence,
    _install_final_parity_batch,
    _install_final_parity_v4,
)
from tests.test_ptg2_candidate_audit_v4 import (
    _challenge,
    _persisted_occurrence,
    _v4_serving_tables,
)
from tests.test_ptg2_shared_blocks import _ReadOnceSession, _read_once_rows


@pytest.mark.asyncio
async def test_graph_first_preserves_price_and_persisted_proof(monkeypatch):
    """Match the final witness through one deferred exact forward read."""

    challenge = _matched_challenge()
    occurrence = _final_parity_occurrence(challenge.npi)
    harness = _final_parity_harness()
    harness.direct_forward.side_effect = AssertionError(
        "broad forward read must not start"
    )
    _install_final_parity_batch(
        monkeypatch,
        harness,
        challenge,
        occurrence,
        _rate_count_index(_final_parity_code_index(), _DENSE_RATE_COUNT),
    )
    _install_final_parity_v4(monkeypatch, harness)

    observed = await candidate_batch.audit_candidate_source_witness_batch(
        _TransactionSession(),
        _audit_request(),
        _access(),
    )

    assert observed.matched_challenge_count == challenge.multiplicity
    assert observed.persisted_audit_occurrence_count == 1
    assert observed.validated_persisted_audit_occurrence_count == 1
    assert harness.graph_filters == [None]
    harness.direct_forward.assert_not_awaited()
    harness.deferred_forward.assert_awaited_once()
    harness.hydration.assert_awaited_once()


def _install_exact_pipeline(
    monkeypatch,
    challenge,
    persisted,
    code_index,
    provider_sets_by_npi,
):
    expected_filters_by_code = {7: (5,), 8: (7,)}

    async def provider_indexes(
        _session,
        _snapshot_key,
        _challenges,
        _code_index,
        observed_provider_sets,
        _persisted_occurrences,
        **_kwargs,
    ):
        assert observed_provider_sets == provider_sets_by_npi
        return (
            {(challenge.npi, 7): (5,), (persisted.npi, 8): (7,)},
            expected_filters_by_code,
        )

    _install_direct_root(monkeypatch)
    _install_graph_results(monkeypatch, provider_sets_by_npi)
    broad_forward = AsyncMock(
        side_effect=AssertionError("broad forward read must not start")
    )
    monkeypatch.setattr(
        v4_scope,
        "lookup_forward_price_index_from_db",
        broad_forward,
    )
    monkeypatch.setattr(
        candidate_batch,
        "candidate_code_records_by_pair",
        AsyncMock(return_value=code_index),
    )
    monkeypatch.setattr(
        candidate_batch,
        "_load_candidate_provider_indexes",
        provider_indexes,
    )
    network_digests = frozenset(
        canonical_network_name_digests(("Alpha Network",))
    )
    monkeypatch.setattr(
        candidate_batch,
        "_provider_network_digests_by_key",
        AsyncMock(return_value={5: network_digests, 7: network_digests}),
    )
    return expected_filters_by_code, broad_forward


def _two_code_index() -> CandidateCodeIndex:
    code_fields = _final_parity_code_index().by_key[7]
    return CandidateCodeIndex(
        by_pair={
            ("CPT", "99213"): (
                {**code_fields, "rate_count": _DENSE_RATE_COUNT // 2},
            )
        },
        by_key={
            7: {**code_fields, "rate_count": _DENSE_RATE_COUNT // 2},
            8: {"code_key": 8, "rate_count": _DENSE_RATE_COUNT // 2},
        },
    )


@pytest.mark.asyncio
async def test_graph_first_defers_exact_required_occurrences(monkeypatch):
    """Bind the only forward read to proved sets and persisted coordinates."""

    challenge = _matched_challenge()
    persisted = _persisted_occurrence()
    provider_sets_by_npi = {challenge.npi: (5,), persisted.npi: (7,)}
    expected_filters_by_code, broad_forward = _install_exact_pipeline(
        monkeypatch,
        challenge,
        persisted,
        _two_code_index(),
        provider_sets_by_npi,
    )
    expected_occurrences = frozenset({(7, 5, 0), (8, 7, 1)})
    deferred_forward = AsyncMock(
        return_value={(7, 5, 0): (10,), (8, 7, 1): (9,)}
    )
    monkeypatch.setattr(
        candidate_batch,
        "_candidate_forward_price_keys",
        deferred_forward,
    )
    monkeypatch.setattr(
        candidate_batch,
        "_version_three_price_hydration",
        AsyncMock(
            return_value=SimpleNamespace(
                atom_keys_by_price_key={10: (11,), 9: (10,)},
                prices_by_key={10: [_price_payload()], 9: [_price_payload()]},
            )
        ),
    )

    observed = await candidate_batch._candidate_data_for_conditions(
        object(),
        _v4_serving_tables(),
        _access(),
        challenges=(challenge,),
        persisted_audit_occurrences=(persisted,),
        witness_io={},
    )

    assert deferred_forward.await_args.args[2:4] == (
        expected_filters_by_code,
        expected_occurrences,
    )
    assert candidate_batch._is_challenge_match(challenge, observed) is True
    assert observed.persisted_audit_occurrence_count == 1
    broad_forward.assert_not_awaited()


def _read_once_forward(block_session):
    async def read_forward(*_args, **_kwargs):
        blocks = await shared_blocks.fetch_shared_blocks(
            block_session,
            schema_name="mrf",
            snapshot_key=43,
            object_kind="page_v4",
            block_keys=(7,),
        )
        shared_blocks.claim_shared_block_processing(
            schema_name="mrf",
            block_hash=blocks[7][0].block_hash,
        )
        return {(7, 5, 0): (10,)}

    return read_forward


@pytest.mark.asyncio
async def test_graph_first_keeps_shared_forward_read_once(monkeypatch):
    """Consume one exact forward block without a failed broad first read."""

    challenge = _matched_challenge()
    occurrence = _final_parity_occurrence(challenge.npi)
    harness = _final_parity_harness()
    mapping_rows, physical_rows = _read_once_rows(
        object_kind="page_v4",
        raw_payload=b"one exact forward payload",
        coordinates=((7, 0),),
    )
    read_forward = _read_once_forward(
        _ReadOnceSession(
            mapping_rows=mapping_rows,
            physical_rows=physical_rows,
        )
    )
    harness.direct_forward.side_effect = read_forward
    harness.deferred_forward.side_effect = read_forward
    _install_final_parity_batch(
        monkeypatch,
        harness,
        challenge,
        occurrence,
        _rate_count_index(_final_parity_code_index(), _DENSE_RATE_COUNT),
    )
    _install_final_parity_v4(monkeypatch, harness)

    with shared_blocks.shared_block_read_once_scope(
        max_retained_raw_bytes=1024
    ) as read_once_scope:
        await candidate_batch.audit_candidate_source_witness_batch(
            _TransactionSession(),
            _audit_request(),
            _access(),
        )
        read_once_scope.assert_processed_once()
        ledger = read_once_scope.ledger

    assert ledger["physical_block_reads"] == 1
    assert ledger["physical_block_decodes"] == 1
    assert ledger["repeated_physical_reads"] == 0
    harness.direct_forward.assert_not_awaited()
    harness.deferred_forward.assert_awaited_once()


@pytest.mark.asyncio
async def test_graph_first_releases_scope_after_later_failure(monkeypatch):
    """Release prior results and NPI claims after a later graph failure."""

    challenge = _challenge()
    persisted = _persisted_occurrence()
    baseline_bytes = 731
    budget = CandidateAuditDecodedRetentionBudget()
    budget.claim(baseline_bytes, category="the caller baseline")
    graph_lookup = AsyncMock(
        side_effect=(
            {persisted.npi: (7,)},
            RuntimeError("later direct graph failed"),
        )
    )
    monkeypatch.setattr(v4_scope, "_v4_sets_by_npi", graph_lookup)

    with pytest.raises(RuntimeError, match="later direct graph failed"):
        await direct_scope.load_v4_direct_provider_scope(
            object(),
            _v4_serving_tables(),
            (challenge,),
            (persisted,),
            schema_name="candidate_schema",
            retention_budget=budget,
        )

    assert graph_lookup.await_count == 2
    assert budget.retained_bytes == baseline_bytes
