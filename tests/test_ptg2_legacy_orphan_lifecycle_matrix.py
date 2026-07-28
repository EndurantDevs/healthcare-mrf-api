# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Lifecycle and replay boundaries for the legacy PTG orphan sweeper."""

from __future__ import annotations

import pytest

from process.ptg_parts import ptg2_legacy_orphan_store_references as reference_store
from process.ptg_parts import ptg2_legacy_orphan_store_schema as schema_store
from process.ptg_parts.ptg2_legacy_orphan_store_common import (
    _OwnershipAccumulator,
)
from process.ptg_parts.ptg2_legacy_orphan_store_ownership import (
    _control_ownership,
    _internal_run_ownership,
    _mirror_run_ownership,
    _new_accumulators,
    _snapshot_ownership,
)
from process.ptg_parts.ptg2_legacy_orphan_store_references import (
    _attach_attempt_fences,
    _attach_blocking_residue,
    _attach_declared_snapshot_conflicts,
    _attach_reverse_owner_conflicts,
    _attach_serving_references,
)
from process.ptg_parts.ptg2_legacy_orphan_store_replay import (
    _ReplayProofState,
    _add_replay_ownership,
    _add_replay_relation,
    _remaining_relation_count,
    _remaining_snapshot_count,
    _replay_proof_state,
    _validate_replay_aggregates,
)


SUFFIX = "1" * 32
OTHER_SUFFIX = "2" * 32


class _ResultSetExecutor:
    def __init__(self, result_sets: list[list[dict[str, object]]]) -> None:
        self.result_sets = list(result_sets)
        self.statements: list[str] = []
        self.parameters: list[dict[str, object]] = []

    async def all(self, statement: str, **parameters):
        self.statements.append(statement)
        self.parameters.append(parameters)
        return self.result_sets.pop(0)


@pytest.mark.asyncio
async def test_snapshot_ownership_marks_malformed_import_run_identity() -> None:
    accumulators_by_suffix = _new_accumulators(
        [SUFFIX],
        {SUFFIX: ("catalog_drift",), OTHER_SUFFIX: ("ignored",)},
    )
    await _snapshot_ownership(
        _ResultSetExecutor(
            [
                [
                    {
                        "snapshot_id": "snapshot",
                        "import_run_id": "malformed",
                        "status": "failed",
                        "manifest": {
                            "table_name": f"ptg_file_{SUFFIX}",
                        },
                    }
                ]
            ]
        ),
        "mrf",
        {f"ptg_file_{SUFFIX}": SUFFIX},
        accumulators_by_suffix,
    )
    assert accumulators_by_suffix[SUFFIX].ambiguity_reasons == {
        "catalog_drift",
        "snapshot_import_run_identity_malformed",
    }


@pytest.mark.asyncio
async def test_run_ownership_ignores_rows_for_foreign_suffixes() -> None:
    accumulators_by_suffix = _new_accumulators([SUFFIX], {})
    await _internal_run_ownership(
        _ResultSetExecutor(_foreign_internal_result_sets()),
        "mrf",
        accumulators_by_suffix,
    )
    await _mirror_run_ownership(
        _ResultSetExecutor(_foreign_mirror_result_sets()),
        "mrf",
        [SUFFIX],
        accumulators_by_suffix,
    )
    await _control_ownership(
        _ResultSetExecutor(_foreign_control_result_sets()),
        "control_plane",
        [SUFFIX],
        accumulators_by_suffix,
    )
    assert accumulators_by_suffix[SUFFIX] == _OwnershipAccumulator()


def _foreign_internal_result_sets() -> list[list[dict[str, object]]]:
    return [
        [{"import_run_id": f"ptg2:{OTHER_SUFFIX}", "status": "failed"}],
        [
            {
                "import_job_id": "job",
                "import_run_id": f"ptg2:{OTHER_SUFFIX}",
                "status": "failed",
            }
        ],
    ]


def _foreign_mirror_result_sets() -> list[list[dict[str, object]]]:
    return [
        [
            {
                "run_id": "run",
                "source_file_import_id": OTHER_SUFFIX,
                "status": "failed",
                "snapshot_id": None,
            }
        ]
    ]


def _foreign_control_result_sets() -> list[list[dict[str, object]]]:
    return [
        [
            {
                "source_file_import_id": OTHER_SUFFIX,
                "status": "failed",
                "snapshot_id": None,
            }
        ],
        [
            {
                "placement_id": "placement",
                "source_file_import_id": OTHER_SUFFIX,
                "status": "inactive",
                "snapshot_id": None,
            }
        ],
    ]


@pytest.mark.asyncio
async def test_reference_attachment_boundaries(monkeypatch) -> None:
    first = _OwnershipAccumulator()
    second = _OwnershipAccumulator()
    first.declared_snapshot_ids.add("shared")
    second.declared_snapshot_ids.add("shared")
    accumulators_by_suffix = {SUFFIX: first, OTHER_SUFFIX: second}
    _attach_declared_snapshot_conflicts(accumulators_by_suffix)
    assert first.ambiguity_reasons == {"declared_snapshot_owner_conflict"}
    assert second.ambiguity_reasons == {"declared_snapshot_owner_conflict"}

    monkeypatch.setattr(reference_store, "_BLOCKING_ATTACHMENTS", ())
    await _attach_blocking_residue(
        object(),
        schema_name="mrf",
        accumulators=accumulators_by_suffix,
        suffixes_by_snapshot={},
        present_optional_table_names=frozenset(),
    )

    empty = _OwnershipAccumulator()
    await _attach_reverse_owner_conflicts(
        object(),
        schema_name="mrf",
        control_schema_name="control_plane",
        accumulators={SUFFIX: empty},
        suffixes_by_snapshot={},
    )
    await _attach_attempt_fences(
        object(),
        schema_name="mrf",
        accumulators={},
        suffixes_by_snapshot={},
    )


@pytest.mark.asyncio
async def test_present_optional_stage_residue_blocks_exact_owner() -> None:
    accumulator = _OwnershipAccumulator()
    accumulator.declared_snapshot_ids.add("snapshot")
    executor = _ResultSetExecutor(
        [
            [
                {
                    "attachment_name": "ptg2_price_set_stage",
                    "snapshot_id": "snapshot",
                    "internal_run_id": None,
                }
            ]
        ]
    )

    await _attach_blocking_residue(
        executor,
        schema_name="mrf",
        accumulators={SUFFIX: accumulator},
        suffixes_by_snapshot={"snapshot": {SUFFIX}},
        present_optional_table_names=frozenset(
            {"ptg2_price_set_stage"}
        ),
    )

    assert accumulator.active_references == {
        "nonserving_residue:ptg2_price_set_stage"
    }


@pytest.mark.asyncio
async def test_internal_run_residue_blocks_only_its_exact_owner() -> None:
    exact_owner = _OwnershipAccumulator()
    foreign_owner = _OwnershipAccumulator()
    exact_owner.internal_run_statuses.add((f"ptg2:{SUFFIX}", "failed"))
    executor = _ResultSetExecutor(
        [
            [
                {
                    "attachment_name": "ptg2_v4_attempt_stage",
                    "snapshot_id": None,
                    "internal_run_id": f"ptg2:{SUFFIX}",
                }
            ]
        ]
    )

    await _attach_blocking_residue(
        executor,
        schema_name="mrf",
        accumulators={
            SUFFIX: exact_owner,
            OTHER_SUFFIX: foreign_owner,
        },
        suffixes_by_snapshot={},
        present_optional_table_names=frozenset(),
    )

    assert exact_owner.active_references == {
        "nonserving_residue:ptg2_v4_attempt_stage"
    }
    assert foreign_owner.active_references == set()


@pytest.mark.asyncio
async def test_serving_and_fence_evidence_attach_to_exact_owner(
    monkeypatch,
) -> None:
    accumulator = _OwnershipAccumulator()
    accumulator.declared_snapshot_ids.add("snapshot")
    accumulators_by_suffix = {SUFFIX: accumulator}

    async def reference_rows(*_arguments, **_parameters):
        return [{"snapshot_id": "snapshot", "reference_kind": "route"}]

    monkeypatch.setattr(reference_store, "_reference_rows", reference_rows)
    await _attach_serving_references(
        object(),
        schema_name="mrf",
        control_schema_name="control_plane",
        accumulators=accumulators_by_suffix,
        suffixes_by_snapshot={"snapshot": {SUFFIX}},
    )
    await _attach_attempt_fences(
        _ResultSetExecutor(
            [
                [
                    {
                        "snapshot_id": "snapshot",
                        "internal_run_id": f"ptg2:{OTHER_SUFFIX}",
                        "state": "active",
                    }
                ]
            ]
        ),
        schema_name="mrf",
        accumulators=accumulators_by_suffix,
        suffixes_by_snapshot={"snapshot": {SUFFIX}},
    )

    assert accumulator.active_references == {"route"}
    assert accumulator.ambiguity_reasons == {"attempt_fence_owner_conflict"}
    assert accumulator.fence_states == {("snapshot", "active")}


@pytest.mark.asyncio
async def test_matching_attempt_fence_preserves_exact_owner_evidence() -> None:
    accumulator = _OwnershipAccumulator()
    accumulator.declared_snapshot_ids.add("snapshot")

    await _attach_attempt_fences(
        _ResultSetExecutor(
            [
                [
                    {
                        "snapshot_id": "snapshot",
                        "internal_run_id": f"ptg2:{SUFFIX}",
                        "state": "reconciled",
                    }
                ]
            ]
        ),
        schema_name="mrf",
        accumulators={SUFFIX: accumulator},
        suffixes_by_snapshot={"snapshot": {SUFFIX}},
    )

    assert accumulator.ambiguity_reasons == set()
    assert accumulator.fence_states == {("snapshot", "reconciled")}


def _relation_proof(
    *,
    relation_oid: int = 11,
    dependent_oids: list[int] | None = None,
) -> dict[str, object]:
    return {
        "table_name": f"ptg_file_{SUFFIX}",
        "relation_oid": relation_oid,
        "dependent_relation_oids": dependent_oids or [],
        "total_bytes": 10,
        "has_rows": False,
    }


@pytest.mark.parametrize(
    "candidate_proofs",
    (
        [None],
        [{"suffix": "bad", "relations": [], "ownership": {}}],
        [{"suffix": SUFFIX, "relations": [None], "ownership": {}}],
    ),
)
def test_replay_candidate_shapes_fail_closed(candidate_proofs) -> None:
    with pytest.raises(RuntimeError, match="replay_audit_invalid"):
        _replay_proof_state(candidate_proofs)


def test_replay_relation_and_ownership_shapes_fail_closed() -> None:
    with pytest.raises(RuntimeError, match="replay_audit_invalid"):
        _add_replay_relation(_ReplayProofState(), {}, SUFFIX)
    with pytest.raises(RuntimeError, match="replay_audit_invalid"):
        _add_replay_relation(
            _ReplayProofState(),
            _relation_proof(dependent_oids=[22, 22]),
            SUFFIX,
        )
    for ownership in (
        {"snapshot_statuses": [["only-one"]]},
        {"internal_run_statuses": [["only-one"]]},
        {"internal_run_statuses": [[f"ptg2:{OTHER_SUFFIX}", "failed"]]},
    ):
        with pytest.raises(RuntimeError, match="replay_audit_invalid"):
            _add_replay_ownership(_ReplayProofState(), ownership, SUFFIX)


def test_replay_rejects_root_dependency_collision_and_aggregate_drift() -> None:
    candidates = [
        {
            "suffix": SUFFIX,
            "relations": [_relation_proof(dependent_oids=[22])],
            "ownership": {},
        },
        {
            "suffix": OTHER_SUFFIX,
            "relations": [
                {
                    **_relation_proof(relation_oid=22),
                    "table_name": f"ptg_file_{OTHER_SUFFIX}",
                }
            ],
            "ownership": {},
        },
    ]
    with pytest.raises(RuntimeError, match="replay_audit_invalid"):
        _replay_proof_state(candidates)

    state = _ReplayProofState(suffixes={SUFFIX})
    with pytest.raises(RuntimeError, match="replay_audit_invalid"):
        _validate_replay_aggregates(
            {"candidate_suffix_count": 2},
            state,
        )


@pytest.mark.asyncio
async def test_empty_replay_state_skips_relation_and_snapshot_queries() -> None:
    state = _ReplayProofState()
    assert await _remaining_relation_count(object(), "mrf", state) == 0
    assert await _remaining_snapshot_count(object(), "mrf", state) == 0


@pytest.mark.asyncio
async def test_schema_catalog_boundaries_fail_closed(monkeypatch) -> None:
    """Reject invalid authority, audit, and base catalog identities."""

    monkeypatch.setattr(schema_store, "_MRF_REQUIRED_TABLES", ("snapshot",))
    monkeypatch.setattr(schema_store, "_CONTROL_REQUIRED_TABLES", ())
    with pytest.raises(RuntimeError, match="authority_catalog_invalid"):
        schema_store._validate_authority_relations(
            [
                {
                    "table_schema": "mrf",
                    "table_name": "snapshot",
                    "relkind": "v",
                    "relpersistence": "p",
                }
            ],
            schema_name="mrf",
            control_schema_name="control_plane",
        )
    with pytest.raises(RuntimeError, match="audit_guard_invalid"):
        schema_store._validated_audit_triggers([], "mrf")

    class _MissingBaseExecutor:
        async def first(self, _statement: str, **_parameters):
            return None

    with pytest.raises(RuntimeError, match="snapshot_catalog_missing"):
        await schema_store._base_catalog_identity(
            _MissingBaseExecutor(),
            "mrf",
        )


def _audit_trigger_row(name: str, trigger_type: int) -> dict[str, object]:
    return {
        "tgname": name,
        "tgtype": trigger_type,
        "tgenabled": "A",
        "proname": "guard_ptg2_legacy_orphan_sweep_audit",
        "function_schema": "mrf",
        "prosrc": schema_store._EXPECTED_AUDIT_TRIGGER_BODY,
    }


def test_audit_trigger_catalog_rejects_tamper_and_duplicate_guards() -> None:
    row_guard = _audit_trigger_row(
        "ptg2_legacy_orphan_sweep_audit_row_guard",
        27,
    )
    truncate_guard = _audit_trigger_row(
        "ptg2_legacy_orphan_sweep_audit_truncate_guard",
        34,
    )

    with pytest.raises(RuntimeError, match="audit_guard_invalid"):
        schema_store._validated_audit_triggers(
            [{**row_guard, "tgenabled": "D"}, truncate_guard],
            "mrf",
        )

    with pytest.raises(RuntimeError, match="audit_guard_invalid"):
        schema_store._validated_audit_triggers(
            [row_guard, dict(row_guard)],
            "mrf",
        )
