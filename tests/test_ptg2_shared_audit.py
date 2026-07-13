# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import hashlib
import struct

import pytest

from process.ptg_parts import ptg2_shared_audit as audit


_CANDIDATE = struct.Struct(">IIIII")


def _summary(tmp_path, records):
    payload = b"".join(_CANDIDATE.pack(*record) for record in records)
    path = tmp_path / "audit-candidates.bin"
    path.write_bytes(payload)
    code_count = max((record[0] for record in records), default=-1) + 1
    provider_count = max((record[1] for record in records), default=-1) + 1
    price_count = max((record[2] for record in records), default=-1) + 1
    source_count = max((record[3] for record in records), default=-1) + 1
    return {
        "output_directory": str(tmp_path),
        "source_count": source_count,
        "dense_keys": {
            "code": {"count": code_count},
            "provider_set": {"count": provider_count},
            "price": {"count": price_count},
        },
        "audit_candidates": {
            "path": path.name,
            "record_format": "ptg2_v3_audit_candidates_v2",
            "format_version": 2,
            "record_bytes": 20,
            "row_count": len(records),
            "maximum_rows": 4096,
            "selection_method": "equal_interval_assigned_rows_v1",
            "source_row_count": len(records),
            "row_digest": hashlib.sha256(payload).hexdigest(),
        },
    }


def test_candidate_file_preserves_duplicate_serving_occurrences(tmp_path):
    summary = _summary(
        tmp_path,
        [
            (1, 2, 3, 0, 4),
            (1, 2, 3, 0, 4),
        ],
    )

    candidates, metadata = audit.load_audit_candidates(summary)

    assert len(candidates) == 2
    assert candidates[0].candidate_ordinal == 0
    assert candidates[1].candidate_ordinal == 1
    assert candidates[0].code_key == candidates[1].code_key == 1
    assert metadata["row_count"] == 2


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (
            lambda summary: summary["audit_candidates"].update(row_digest="0" * 64),
            "digest",
        ),
        (lambda summary: summary["audit_candidates"].update(record_bytes=16), "width"),
        (lambda summary: summary["audit_candidates"].update(row_count=2), "count"),
        (
            lambda summary: summary["audit_candidates"].update(
                selection_method="first_rows"
            ),
            "selection",
        ),
    ],
)
def test_candidate_file_rejects_contract_drift(tmp_path, mutation, message):
    summary = _summary(tmp_path, [(1, 2, 3, 0, 4)])
    mutation(summary)

    with pytest.raises(RuntimeError, match=message):
        audit.load_audit_candidates(summary)


def test_candidate_file_rejects_key_outside_dense_dictionary(tmp_path):
    summary = _summary(tmp_path, [(1, 2, 3, 0, 4)])
    summary["dense_keys"]["price"]["count"] = 3

    with pytest.raises(RuntimeError, match="price_key.*dense dictionary"):
        audit.load_audit_candidates(summary)


def test_occurrence_identity_includes_candidate_and_atom_ordinals():
    core_layout_id = b"\x9a" * 32
    base = dict(
        code_key=1,
        provider_set_key=2,
        price_key=3,
        source_key=0,
        npi=1_234_567_890,
        atom_key=7,
    )

    first = audit.occurrence_id(
        core_layout_id,
        **base,
        atom_ordinal=0,
        candidate_ordinal=0,
    )
    duplicate_source_occurrence = audit.occurrence_id(
        core_layout_id,
        **base,
        atom_ordinal=0,
        candidate_ordinal=1,
    )
    duplicate_atom_occurrence = audit.occurrence_id(
        core_layout_id,
        **base,
        atom_ordinal=1,
        candidate_ordinal=0,
    )
    other_artifact_occurrence = audit.occurrence_id(
        core_layout_id,
        **{**base, "source_key": 1},
        atom_ordinal=0,
        candidate_ordinal=0,
    )

    assert len(first) == 32
    assert len(
        {first, duplicate_source_occurrence, duplicate_atom_occurrence, other_artifact_occurrence}
    ) == 4


def test_persisted_sample_digest_matches_sealed_coordinate_vector():
    stored_rows = (
        {
            "occurrence_id": b"\x02" * 32,
            "code_key": 1,
            "provider_set_key": 2,
            "price_key": 3,
            "source_key": 4,
            "npi": 1_234_567_890,
            "atom_ordinal": 1,
            "atom_key": 8,
        },
        {
            "occurrence_id": b"\x01" * 32,
            "code_key": 1,
            "provider_set_key": 2,
            "price_key": 3,
            "source_key": 4,
            "npi": 1_234_567_890,
            "atom_ordinal": 0,
            "atom_key": 7,
        },
    )

    assert audit.persisted_audit_sample_digest(stored_rows) == (
        "2a64d709cfe0cdf35e18b88338d64b32f07c1c83850203e923c9c712e2df965d"
    )


def test_sample_preserves_duplicate_candidates_and_duplicate_atom_ordinals():
    candidates = (
        audit.AuditCandidate(1, 2, 3, 0, 1, candidate_ordinal=0),
        audit.AuditCandidate(1, 2, 3, 1, 1, candidate_ordinal=1),
    )
    budget = audit._ReadBudget()

    rows = audit.build_audit_occurrences(
        candidates=candidates,
        provider_npis={0: (1_234_567_890,), 1: (1_234_567_890,)},
        price_memberships={3: (7, 7)},
        core_layout_id=b"\x0b" * 32,
        budget=budget,
        maximum_rows=10,
    )

    assert len(rows) == 4
    assert len({row.occurrence_id for row in rows}) == 4
    assert {row.candidate_ordinal for row in rows} == {0, 1}
    assert {row.atom_ordinal for row in rows} == {0, 1}
    assert {row.atom_key for row in rows} == {7}
    assert {row.source_key for row in rows} == {0, 1}


def test_sample_gives_each_candidate_one_row_before_expansion():
    candidates = (
        audit.AuditCandidate(10, 20, 30, 0, 2, candidate_ordinal=0),
        audit.AuditCandidate(11, 21, 31, 1, 2, candidate_ordinal=1),
    )

    rows = audit.build_audit_occurrences(
        candidates=candidates,
        provider_npis={
            0: (1_111_111_111, 1_111_111_112),
            1: (1_222_222_221, 1_222_222_222),
        },
        price_memberships={30: (4, 5), 31: (6, 7)},
        core_layout_id=b"\x1c" * 32,
        budget=audit._ReadBudget(),
        maximum_rows=3,
    )

    assert len(rows) == 3
    assert {row.candidate_ordinal for row in rows} == {0, 1}
    assert sum(row.candidate_ordinal == 0 for row in rows) == 2
    assert sum(row.candidate_ordinal == 1 for row in rows) == 1


def test_sample_skips_candidates_without_npis_or_atoms():
    rows = audit.build_audit_occurrences(
        candidates=(
            audit.AuditCandidate(1, 10, 20, 0, 0, candidate_ordinal=0),
            audit.AuditCandidate(2, 11, 21, 0, 1, candidate_ordinal=1),
            audit.AuditCandidate(3, 12, 22, 0, 1, candidate_ordinal=2),
        ),
        provider_npis={0: (), 1: (1_234_567_890,), 2: (1_234_567_891,)},
        price_memberships={20: (1,), 21: (), 22: (2,)},
        core_layout_id=b"\x2d" * 32,
        budget=audit._ReadBudget(),
        maximum_rows=10,
    )

    assert [(row.code_key, row.npi, row.atom_key) for row in rows] == [
        (3, 1_234_567_891, 2)
    ]


def test_hard_combination_cap_fails_closed(monkeypatch):
    monkeypatch.setattr(audit, "PTG2_V3_AUDIT_MAX_COMBINATION_ATTEMPTS", 1)

    with pytest.raises(RuntimeError, match="coordinate-attempt cap"):
        audit.build_audit_occurrences(
            candidates=(audit.AuditCandidate(1, 2, 3, 0, 1),),
            provider_npis={0: (1_234_567_890,)},
            price_memberships={3: (4, 5)},
            core_layout_id=b"\x3e" * 32,
            budget=audit._ReadBudget(),
            maximum_rows=2,
        )


def test_block_and_member_budgets_fail_closed(monkeypatch):
    monkeypatch.setattr(audit, "PTG2_V3_AUDIT_MAX_BLOCK_BYTES", 4)
    monkeypatch.setattr(audit, "PTG2_V3_AUDIT_MAX_MEMBER_ATTEMPTS", 1)
    budget = audit._ReadBudget()

    budget.add_block("kind", 1, 0, 4)
    budget.add_block("kind", 1, 0, 4)
    with pytest.raises(RuntimeError, match="block-byte cap"):
        budget.add_block("kind", 2, 0, 1)

    budget.add_members(1)
    with pytest.raises(RuntimeError, match="graph-member cap"):
        budget.add_members(1)


@pytest.mark.asyncio
async def test_audit_source_keys_require_complete_snapshot_dictionary():
    class Session:
        async def execute(self, _statement, _params):
            return ({"source_key": 0}, {"source_key": 1})

    await audit._validate_snapshot_source_dictionary(
        Session(),
        schema_name="mrf",
        logical_snapshot_id="snapshot-1",
        source_count=2,
        required_source_keys=(0, 1),
    )

    with pytest.raises(RuntimeError, match="complete and dense"):
        await audit._validate_snapshot_source_dictionary(
            Session(),
            schema_name="mrf",
            logical_snapshot_id="snapshot-1",
            source_count=3,
            required_source_keys=(0, 1, 2),
        )


def test_hash_member_selection_is_repeatable_and_not_prefix_based():
    kwargs = {
        "candidate_ordinal": 17,
        "selection_kind": b"group-npi",
        "owner_key": 91,
        "expansion_ordinal": 0,
        "member_count": 10_000,
    }

    first = audit._deterministic_member_ordinal(b"\x44" * 32, **kwargs)
    second = audit._deterministic_member_ordinal(b"\x44" * 32, **kwargs)

    assert first == second
    assert first > 0
    assert first < kwargs["member_count"]


@pytest.mark.asyncio
async def test_targeted_graph_reads_only_chunks_containing_selected_ordinals():
    class Reader:
        schema_name = "mrf"
        snapshot_key = 1

        def __init__(self):
            self.budget = audit._ReadBudget()
            self.calls = []

        async def logical_blocks(self, object_kind, block_keys):
            requested = tuple(sorted(block_keys))
            self.calls.append((object_kind, requested))
            payloads = {}
            for block_key in requested:
                payload = bytearray(audit.PTG2_V3_GRAPH_CHUNK_BYTES)
                if block_key == 100:
                    payload[24:32] = (1_000_000_003).to_bytes(8, "little")
                if block_key == 102:
                    payload[4928:4936] = (1_000_017_000).to_bytes(8, "little")
                payloads[block_key] = (bytes(payload), 0)
            return payloads

    reader = Reader()
    values = await audit._graph_targeted_members(
        reader,
        direction=audit.PTG2_V3_GRAPH_GROUP_TO_NPI,
        member_ordinals_by_owner={7: (3, 17_000)},
        locators={
            7: audit._GraphOwnerLocator(
                first_chunk=100,
                member_offset=0,
                member_count=20_000,
            )
        },
    )

    assert values == {7: {3: 1_000_000_003, 17_000: 1_000_017_000}}
    assert reader.calls == [("graph_group_npis_v1", (100, 102))]
    assert reader.budget.member_attempts == 2


@pytest.mark.asyncio
async def test_publication_context_preserves_build_token_ownership(monkeypatch):
    calls = []

    async def validate_building_layout(session, **arguments):
        calls.append(("layout", session, arguments))

    async def validate_provider_counts(session, **arguments):
        calls.append(("providers", session, arguments))

    async def validate_source_dictionary(session, **arguments):
        arguments["required_source_keys"] = tuple(arguments["required_source_keys"])
        calls.append(("sources", session, arguments))

    monkeypatch.setattr(audit, "_validate_building_layout", validate_building_layout)
    monkeypatch.setattr(
        audit,
        "_validate_candidate_provider_counts",
        validate_provider_counts,
    )
    monkeypatch.setattr(
        audit,
        "_validate_snapshot_source_dictionary",
        validate_source_dictionary,
    )
    session = object()
    candidate = audit.AuditCandidate(1, 2, 3, 4, 5)

    await audit._validate_publication_context(
        session,
        schema_name="mrf",
        build_ownership=audit.SharedLayoutBuildOwnership(
            snapshot_key=91,
            build_token="owned-build-token",
        ),
        logical_snapshot_id="logical-snapshot",
        candidates=(candidate,),
        source_count=6,
    )

    assert [call[0] for call in calls] == ["layout", "providers", "sources"]
    assert calls[0] == (
        "layout",
        session,
        {
            "schema_name": "mrf",
            "snapshot_key": 91,
            "build_token": "owned-build-token",
        },
    )
    assert calls[2][2]["required_source_keys"] == (4,)


@pytest.mark.asyncio
async def test_candidate_npis_dedupe_overlap_across_targeted_groups(monkeypatch):
    monkeypatch.setattr(audit, "PTG2_V3_AUDIT_MAX_SAMPLE_ROWS", 4)

    async def locators(_reader, *, direction, owner_keys):
        requested = set(owner_keys)
        if direction == audit.PTG2_V3_GRAPH_PROVIDER_SET_TO_GROUP:
            assert requested == {5}
            return {5: audit._GraphOwnerLocator(1, 0, 2)}
        assert requested == {10, 11}
        return {
            10: audit._GraphOwnerLocator(2, 0, 2),
            11: audit._GraphOwnerLocator(3, 0, 2),
        }

    async def members(
        _reader,
        *,
        direction,
        member_ordinals_by_owner,
        locators,
    ):
        if direction == audit.PTG2_V3_GRAPH_PROVIDER_SET_TO_GROUP:
            return {
                5: {
                    ordinal: 10 if ordinal == 0 else 11
                    for ordinal in member_ordinals_by_owner[5]
                }
            }
        return {
            group_key: {ordinal: 1_234_567_890 for ordinal in ordinals}
            for group_key, ordinals in member_ordinals_by_owner.items()
        }

    def ordinal(
        _core_layout_id,
        *,
        selection_kind,
        expansion_ordinal,
        **_kwargs,
    ):
        assert selection_kind in {b"provider-group", b"group-npi"}
        return expansion_ordinal % 2

    monkeypatch.setattr(audit, "_graph_owner_locators", locators)
    monkeypatch.setattr(audit, "_graph_targeted_members", members)
    monkeypatch.setattr(audit, "_deterministic_member_ordinal", ordinal)

    result = await audit._candidate_npis_by_ordinal(
        object(),
        candidates=(audit.AuditCandidate(1, 5, 7, 0, 1, candidate_ordinal=0),),
        price_memberships={7: (9,)},
        core_layout_id=b"\x55" * 32,
    )

    assert result == {0: (1_234_567_890,)}


@pytest.mark.asyncio
async def test_price_membership_uses_published_block_span(monkeypatch):
    class Reader:
        def __init__(self):
            self.calls = []

        async def logical_blocks(self, object_kind, block_keys):
            requested = tuple(sorted(block_keys))
            self.calls.append((object_kind, requested))
            return {block_key: (b"payload", 1) for block_key in requested}

    def decode(
        _payload,
        *,
        block_key,
        block_span,
        requested_price_keys,
        **_kwargs,
    ):
        return {
            price_key: (price_key + 100,)
            for price_key in requested_price_keys
            if price_key // block_span == block_key
        }

    monkeypatch.setattr(audit, "_decode_price_membership_block", decode)
    reader = Reader()
    memberships = await audit._price_memberships(
        reader,
        price_keys=(6, 7, 15),
        atom_key_bits=24,
        block_span=7,
    )

    assert memberships == {6: (106,), 7: (107,), 15: (115,)}
    assert reader.calls == [("price_set_atom_memberships_v3", (0, 1, 2))]
