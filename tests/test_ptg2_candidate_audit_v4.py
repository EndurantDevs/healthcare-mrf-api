from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from api import ptg2_candidate_audit_reverse as reverse_scope
from api import ptg2_candidate_audit_v4 as v4_scope
from api import ptg2_serving as serving
from api.ptg2_candidate_audit_codes import CandidateCodeIndex
from api.ptg2_candidate_audit_integrity import PersistedAuditOccurrence
from api.ptg2_types import PTG2ServingTables
from api.ptg2_v4_graph import V4GraphRoot
from process.ptg_parts.ptg2_candidate_audit_batch_contract import (
    AuditBatchChallenge,
)


def _challenge() -> AuditBatchChallenge:
    return AuditBatchChallenge(
        code_system="CPT",
        code="99213",
        npi=1234567890,
        source_artifact_key=0,
        tuple_digest="a" * 64,
        network_name_digests=("b" * 64,),
        multiplicity=1,
    )


def _persisted_occurrence() -> PersistedAuditOccurrence:
    return PersistedAuditOccurrence(
        occurrence_id=b"p" * 32,
        code_key=8,
        provider_set_key=7,
        price_key=9,
        source_artifact_key=1,
        npi=1111111111,
        atom_ordinal=0,
        atom_key=10,
    )


def _code_index() -> CandidateCodeIndex:
    return CandidateCodeIndex(
        by_pair={("CPT", "99213"): ({"code_key": 7},)},
        by_key={7: {"code_key": 7}, 8: {"code_key": 8}},
    )


def _v4_serving_tables() -> PTG2ServingTables:
    return PTG2ServingTables(
        arch_version="postgres_binary_v3",
        storage_generation="shared_blocks_v4",
        cold_lookup_contract="ptg_v3_cold_v2",
        shared_block_layout="packed_snapshot_maps_v4",
        shared_snapshot_key=43,
        source_count=2,
        price_dictionary_item_count=100,
        price_dictionary_block_bytes=2048,
        provider_graph_v4_hot_prefix={
            "npi_prefix_target": 201,
            "max_set_patterns_per_set": 1024,
            "max_set_components_per_fallback_set": 4096,
            "max_online_group_keys_per_set": 4096,
            "max_online_source_owners_per_set": 4096,
            "max_online_source_members_per_set": 16384,
            "max_online_source_pages_per_set": 64,
            "max_online_source_bytes_per_set": 1024 * 1024,
            "online_group_npi_batch_size": 32,
            "max_online_group_npi_members_per_set": 32768,
            "max_online_group_npi_locator_pages_per_set": 16,
            "max_online_group_npi_member_pages_per_set": 128,
            "max_online_group_npi_bytes_per_set": 4 * 1024 * 1024,
            "max_online_group_npi_batches_per_set": 4,
            "provider_expansion_rate_page_rows": 64,
            "max_online_provider_expansion_rate_rows": 256,
            "max_online_provider_expansion_provider_sets": 64,
            "max_online_provider_expansion_graph_batches": 64,
        },
    )


def _install_v4_graph(
    monkeypatch,
    *,
    representation: str,
    first_relation: str,
    second_relation: str,
    challenge: AuditBatchChallenge,
    persisted: PersistedAuditOccurrence,
) -> list[dict[str, object]]:
    """Install one real pattern/direct traversal around mocked V4 storage."""

    graph_calls: list[dict[str, object]] = []

    async def graph_lookup(_session, **kwargs):
        graph_calls.append(kwargs)
        if kwargs["relation"] == first_relation:
            return {4: (2,), 6: (3,)}
        if kwargs["relation"] == second_relation:
            return {2: (5, 11), 3: (7,)}
        raise AssertionError(f"unexpected relation {kwargs['relation']}")

    monkeypatch.setattr(
        serving,
        "v4_npi_keys_for_values",
        AsyncMock(return_value={challenge.npi: 4, persisted.npi: 6}),
    )
    monkeypatch.setattr(
        serving,
        "load_v4_graph_root",
        AsyncMock(
            return_value=V4GraphRoot(
                snapshot_key=43,
                representation=representation,
                map_digest=b"m" * 32,
            )
        ),
    )
    monkeypatch.setattr(
        serving,
        "_v4_reverse_members_for_sets",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(serving, "lookup_v4_relation_members", graph_lookup)
    return graph_calls


@pytest.mark.parametrize(
    ("representation", "first_relation", "second_relation"),
    (
        ("pattern_v1", "npi_patterns", "pattern_sets"),
        ("direct_v1", "npi_groups_exact", "group_sets_direct"),
    ),
)
@pytest.mark.asyncio
async def test_v4_candidate_scope_uses_bounded_code_first_graph(
    monkeypatch,
    representation,
    first_relation,
    second_relation,
):
    """Prove pattern and direct V4 layouts without invoking the V3 graph."""

    challenge = _challenge()
    persisted = _persisted_occurrence()
    expected_price_index = {
        (7, 5, 0): (10,),
        (7, 9, 0): (11,),
        (8, 7, 1): (9,),
    }
    forward_lookup = AsyncMock(return_value=expected_price_index)
    broad_scope_lookup = AsyncMock()
    monkeypatch.setattr(
        v4_scope,
        "lookup_forward_price_index_from_db",
        forward_lookup,
    )
    graph_calls = _install_v4_graph(
        monkeypatch,
        representation=representation,
        first_relation=first_relation,
        second_relation=second_relation,
        challenge=challenge,
        persisted=persisted,
    )

    observed_scope = await reverse_scope.load_candidate_provider_scope(
        broad_scope_lookup,
        object(),
        _v4_serving_tables(),
        (challenge,),
        (persisted,),
        _code_index(),
        schema_name="candidate_schema",
    )

    assert observed_scope.provider_set_keys_by_npi == {
        challenge.npi: (5,),
        persisted.npi: (7,),
    }
    assert observed_scope.price_keys_by_occurrence is expected_price_index
    broad_scope_lookup.assert_not_awaited()
    assert forward_lookup.await_args.args[1] == {7: (0,), 8: (1,)}
    assert [call["relation"] for call in graph_calls] == [
        first_relation,
        second_relation,
    ]
    assert all(call["schema_name"] == "candidate_schema" for call in graph_calls)
    assert all(int(call["max_members"]) > 0 for call in graph_calls)
