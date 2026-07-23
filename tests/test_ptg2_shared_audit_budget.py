"""Budget-aware hydration coverage for strict shared-block PTG V3 audits."""

import pytest

from process.ptg_parts import ptg2_shared_audit as audit


class _DisjointGraphReader:
    def __init__(self):
        self.budget = audit._ReadBudget()
        self.calls = []

    async def logical_blocks(self, object_kind, block_keys):
        requested_block_keys = tuple(sorted(block_keys))
        self.calls.append((object_kind, requested_block_keys))
        if object_kind == "graph_provider_set_groups_v1":
            return {
                block_key: ((10_000 + block_key).to_bytes(4, "little"), 1)
                for block_key in requested_block_keys
            }
        return {
            block_key: (
                (1_000_000_000 + block_key - 100_000).to_bytes(8, "little"),
                1,
            )
            for block_key in requested_block_keys
        }


async def _disjoint_graph_locators(_reader, *, direction, owner_keys):
    requested_owner_keys = tuple(sorted(set(owner_keys)))
    if direction == audit.PTG2_V3_GRAPH_PROVIDER_SET_TO_GROUP:
        return {
            owner_key: audit._GraphOwnerLocator(owner_key, 0, 1)
            for owner_key in requested_owner_keys
        }
    return {
        owner_key: audit._GraphOwnerLocator(100_000 + owner_key, 0, 1)
        for owner_key in requested_owner_keys
    }


def _dense_candidates():
    return tuple(
        audit.AuditCandidate(
            index,
            index,
            index,
            index // 512,
            1,
            candidate_ordinal=index,
        )
        for index in range(audit.PTG2_V3_AUDIT_MAX_CANDIDATES)
    )


def test_nonempty_census_requires_positive_publication_sample():
    with pytest.raises(RuntimeError, match="positive occurrence sample"):
        audit._require_positive_publication_sample(
            (audit.AuditCandidate(1, 2, 3, 0, 1),),
            (),
        )

    audit._require_positive_publication_sample((), ())


def test_block_budget_reports_remaining_bytes(monkeypatch):
    monkeypatch.setattr(audit, "PTG2_V3_AUDIT_MAX_BLOCK_BYTES", 4)
    budget = audit._ReadBudget()

    budget.add_block("kind", 1, 0, 4)

    assert budget.available_block_bytes() == 0


def test_graph_selection_capacity_reserves_two_chunks_in_both_directions():
    bytes_per_selection = 4 * audit.PTG2_V3_GRAPH_CHUNK_BYTES

    assert audit._graph_selection_capacity(bytes_per_selection * 7) == 7
    assert audit._graph_selection_capacity(bytes_per_selection - 1) == 0
    assert audit._hydration_candidate_limit() == 512


def test_hydration_candidates_are_bounded_repeatable_and_source_preserving():
    candidates = _dense_candidates()

    first = audit._stratified_hydration_candidates(candidates)
    second = audit._stratified_hydration_candidates(candidates)

    assert first == second
    assert len(first) == audit._hydration_candidate_limit()
    assert {candidate.source_key for candidate in first} == set(range(8))
    assert first[0].candidate_ordinal == 0
    assert first[-1].candidate_ordinal == 4095


def test_reused_layout_accepts_legacy_provider_selection_metadata():
    metadata = {
        "contract": audit.PTG2_V3_AUDIT_CONTRACT,
        "format_version": 2,
        "method": audit.PTG2_V3_AUDIT_METHOD,
        "serving_multiplicity_semantics": (
            audit.PTG2_V3_SERVING_MULTIPLICITY_SEMANTICS
        ),
        "sample_count": 1,
        "sample_digest": "a" * 64,
        "provider_selection": "hash_targeted_owner_ordinals_v1",
    }

    assert audit._validated_sealed_audit_contract(metadata) == (1, "a" * 64)


@pytest.mark.asyncio
async def test_dense_4096_seed_hydration_bounds_disjoint_graph_reads(monkeypatch):
    monkeypatch.setattr(audit, "_graph_owner_locators", _disjoint_graph_locators)
    candidates = _dense_candidates()
    price_memberships_by_key = {
        index: (index,) for index in range(len(candidates))
    }
    reader = _DisjointGraphReader()

    candidate_npis = await audit._candidate_npis_by_ordinal(
        reader,
        candidates=candidates,
        price_memberships=price_memberships_by_key,
        core_layout_id=b"\x54" * 32,
        maximum_selections=audit._hydration_candidate_limit(),
    )
    audit_occurrences = audit.build_audit_occurrences(
        candidates=candidates,
        provider_npis=candidate_npis,
        price_memberships=price_memberships_by_key,
        core_layout_id=b"\x54" * 32,
        budget=reader.budget,
    )

    hydration_limit = audit._hydration_candidate_limit()
    assert len(candidate_npis) == hydration_limit
    assert len(audit_occurrences) == hydration_limit
    assert [call[0] for call in reader.calls] == [
        "graph_provider_set_groups_v1",
        "graph_group_npis_v1",
    ]
    assert all(len(block_keys) == hydration_limit for _, block_keys in reader.calls)
    assert {candidates[ordinal].source_key for ordinal in candidate_npis} == set(range(8))
