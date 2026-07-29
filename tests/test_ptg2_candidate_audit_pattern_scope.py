from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import ptg2_candidate_audit_batch as candidate_batch
from api import ptg2_candidate_audit_reverse as reverse_scope
from api import ptg2_candidate_audit_scope_dispatch as scope_dispatch
from api import ptg2_candidate_audit_v4 as v4_scope
from api.ptg2_candidate_audit_codes import CandidateCodeIndex
from api.ptg2_candidate_audit_integrity import PersistedAuditOccurrence
from api.ptg2_v4_graph import V4GraphRoot
from process.ptg_parts import ptg2_batch_candidate_audit as batch_transport
from process.ptg_parts.ptg2_candidate_audit_batch_contract import (
    AuditBatchChallenge,
)
from process.ptg_parts.ptg2_candidate_audit_evidence import (
    canonical_network_name_digests,
)
from tests.test_ptg2_candidate_audit_batch import (
    _access,
    _audit_request,
    _challenge as _matched_challenge,
    _price_payload,
    _TransactionSession,
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


@pytest.mark.parametrize(
    ("maximum_bytes", "safe_detail"),
    (
        (
            v4_scope._result_bytes_for_npis(1) - 1,
            "pattern_provider_result_retention_limit_exceeded",
        ),
        (
            v4_scope._result_bytes_for_npis(1)
            + v4_scope._V4_GRAPH_TRANSIENT_MAP_BYTES
            + 5 * v4_scope._V4_GRAPH_TRANSIENT_OWNER_BYTES,
            "pattern_graph_projection_retention_limit_exceeded",
        ),
    ),
)
@pytest.mark.asyncio
async def test_pattern_retention_failures_have_stable_safe_classification(
    monkeypatch,
    maximum_bytes,
    safe_detail,
):
    graph_lookup = AsyncMock()
    monkeypatch.setattr(v4_scope, "_v4_sets_by_npi", graph_lookup)

    with pytest.raises(
        v4_scope.CandidateAuditDecodedRetentionError,
    ) as exc_info:
        await v4_scope._load_pattern_provider_sets(
            object(),
            _v4_serving_tables(),
            (1_234_567_890,),
            v4_scope.CandidateAuditDecodedRetentionBudget(
                maximum_bytes=maximum_bytes
            ),
            schema_name="candidate_schema",
        )

    response_body = json.dumps({"message": str(exc_info.value)}).encode()
    assert batch_transport._batch_rejection_reason(400, response_body) == (
        f"batch_endpoint_rejected_400_{safe_detail}"
    )
    graph_lookup.assert_not_awaited()


def _final_parity_occurrence(npi):
    return PersistedAuditOccurrence(
        occurrence_id=b"p" * 32,
        code_key=7,
        provider_set_key=5,
        price_key=10,
        source_artifact_key=0,
        npi=npi,
        atom_ordinal=0,
        atom_key=11,
    )


def _final_parity_code_index():
    code_fields_by_name = {
        "code_key": 7,
        "reported_code_system": "CPT",
        "reported_code": "99213",
        "negotiation_arrangement": "ffs",
        "billing_code_type_version": "2026",
        "source_name": "Office visit",
        "source_description": "Established patient",
    }
    return CandidateCodeIndex(
        by_pair={("CPT", "99213"): (code_fields_by_name,)},
        by_key={7: code_fields_by_name},
    )


def _final_parity_harness():
    price_index = {(7, 5, 0): (10,)}
    return SimpleNamespace(
        network_digests=frozenset(
            canonical_network_name_digests(("Alpha Network",))
        ),
        direct_forward=AsyncMock(return_value=price_index),
        deferred_forward=AsyncMock(return_value=price_index),
        hydration=AsyncMock(
            return_value=SimpleNamespace(
                atom_keys_by_price_key={10: (11,)},
                prices_by_key={10: [_price_payload()]},
            )
        ),
        root_lookup=AsyncMock(
            side_effect=(
                V4GraphRoot(43, "direct_v1", b"d" * 32),
                V4GraphRoot(43, "pattern_v1", b"p" * 32),
            )
        ),
        graph_filters=[],
    )


def _install_final_parity_v4(monkeypatch, harness):
    async def graph_lookup(_session, _tables, npis, **kwargs):
        allowed = kwargs["allowed_provider_set_keys"]
        harness.graph_filters.append(
            None if allowed is None else set(allowed)
        )
        return {npi: (5,) for npi in npis}

    monkeypatch.setattr(
        v4_scope,
        "lookup_forward_price_index_from_db",
        harness.direct_forward,
    )
    monkeypatch.setattr(v4_scope, "_v4_sets_by_npi", graph_lookup)
    monkeypatch.setattr(
        scope_dispatch,
        "load_v4_graph_root",
        harness.root_lookup,
    )


def _install_final_parity_batch(
    monkeypatch,
    harness,
    challenge,
    occurrence,
    code_index,
):
    """Install shared candidate metadata and price loaders."""

    async def provider_indexes(
        _session,
        _snapshot_key,
        _challenges,
        _code_index,
        provider_sets_by_npi,
        _persisted_occurrences,
        **_kwargs,
    ):
        assert provider_sets_by_npi == {challenge.npi: (5,)}
        return {(challenge.npi, 7): (5,)}, {7: (5,)}

    monkeypatch.setattr(
        candidate_batch,
        "snapshot_serving_tables",
        AsyncMock(return_value=_v4_serving_tables()),
    )
    monkeypatch.setattr(
        candidate_batch,
        "validate_candidate_source_scope",
        AsyncMock(
            return_value=SimpleNamespace(
                challenges=(challenge,),
                persisted_audit_occurrences=(occurrence,),
                ledger={"payload_reads": 1},
            )
        ),
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
    monkeypatch.setattr(
        candidate_batch,
        "_provider_network_digests_by_key",
        AsyncMock(return_value={5: harness.network_digests}),
    )
    monkeypatch.setattr(
        candidate_batch, "_candidate_forward_price_keys", harness.deferred_forward
    )
    monkeypatch.setattr(
        candidate_batch, "_version_three_price_hydration", harness.hydration
    )


@pytest.mark.asyncio
async def test_pattern_and_direct_v4_produce_identical_final_audit_result(
    monkeypatch,
):
    """Preserve final matches while pattern layout defers its forward read."""

    challenge = _matched_challenge()
    occurrence = _final_parity_occurrence(challenge.npi)
    code_index = _final_parity_code_index()
    harness = _final_parity_harness()
    _install_final_parity_batch(
        monkeypatch,
        harness,
        challenge,
        occurrence,
        code_index,
    )
    _install_final_parity_v4(monkeypatch, harness)

    direct_result = await candidate_batch.audit_candidate_source_witness_batch(
        _TransactionSession(),
        _audit_request(),
        _access(),
    )
    pattern_result = await candidate_batch.audit_candidate_source_witness_batch(
        _TransactionSession(),
        _audit_request(),
        _access(),
    )

    assert pattern_result == direct_result
    assert pattern_result.matched_challenge_count == challenge.multiplicity
    assert pattern_result.persisted_audit_occurrence_count == 1
    assert pattern_result.validated_persisted_audit_occurrence_count == 1
    assert harness.graph_filters == [{5}, None]
    harness.direct_forward.assert_awaited_once()
    harness.deferred_forward.assert_awaited_once()
    assert harness.hydration.await_count == 2
