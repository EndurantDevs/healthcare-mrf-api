# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Legacy-GC blocker proof for immutable V3 reconciliation audits."""

from __future__ import annotations

from db.migration_ptg2_legacy_v3_metadata_reconcile import AUDIT_TABLE
from process.ptg_parts.ptg2_legacy_orphan_contract import (
    LegacyBlockedSuffix,
    LegacyRootRelation,
    LegacySweepLimits,
    build_bounded_legacy_sweep_plan,
    classify_legacy_suffix,
)
from process.ptg_parts.ptg2_legacy_orphan_store_common import (
    _BLOCKING_ATTACHMENTS,
    _MRF_REQUIRED_TABLES,
    _OwnershipAccumulator,
)
from process.ptg_parts.ptg2_legacy_orphan_store_references import (
    _attach_blocking_residue,
    _blocking_attachment_statements,
)


SUFFIX = "1" * 32
SNAPSHOT_ID = "ptg2:202607:synthetic-reconciled-gc-blocker"
INTERNAL_RUN_ID = f"ptg2:{SUFFIX}"


class _ResidueExecutor:
    def __init__(self) -> None:
        self.statement = ""
        self.parameters = {}

    async def all(self, statement: str, **parameters):
        self.statement = statement
        self.parameters = parameters
        return [
            {
                "attachment_name": AUDIT_TABLE,
                "snapshot_id": SNAPSHOT_ID,
                "internal_run_id": INTERNAL_RUN_ID,
            }
        ]


def _root_relation() -> LegacyRootRelation:
    return LegacyRootRelation(
        table_name=f"ptg_file_{SUFFIX}",
        relation_oid=11,
        namespace_oid=7,
        owner_oid=8,
        relkind="r",
        persistence="p",
        total_bytes=0,
        schema_digest="a" * 64,
        has_rows=False,
    )


def test_reconcile_audit_is_a_required_blocking_attachment() -> None:
    matching_attachments = tuple(
        attachment
        for attachment in _BLOCKING_ATTACHMENTS
        if attachment[0] == AUDIT_TABLE
    )
    statements = _blocking_attachment_statements(
        "synthetic_mrf",
        present_optional_table_names=frozenset(),
    )

    assert AUDIT_TABLE in _MRF_REQUIRED_TABLES
    assert matching_attachments == (
        (AUDIT_TABLE, ("snapshot_id",), ("internal_run_id",)),
    )
    audit_statements = tuple(
        statement for statement in statements if AUDIT_TABLE in statement
    )
    assert len(audit_statements) == 2
    assert any("snapshot_id" in statement for statement in audit_statements)
    assert any(
        "internal_run_id" in statement for statement in audit_statements
    )


async def test_reconcile_audit_blocks_legacy_sweep_and_gc_plan() -> None:
    executor = _ResidueExecutor()
    accumulator = _OwnershipAccumulator(
        snapshot_statuses={(SNAPSHOT_ID, "failed")},
        declared_snapshot_ids={SNAPSHOT_ID},
        internal_run_statuses={(INTERNAL_RUN_ID, "failed")},
        evidence_kinds={"snapshot", "internal_run"},
    )
    await _attach_blocking_residue(
        executor,
        schema_name="synthetic_mrf",
        accumulators={SUFFIX: accumulator},
        suffixes_by_snapshot={SNAPSHOT_ID: {SUFFIX}},
        present_optional_table_names=frozenset(),
    )
    classification = classify_legacy_suffix(
        SUFFIX,
        (_root_relation(),),
        accumulator.freeze(),
    )

    assert executor.parameters == {
        "snapshot_ids": [SNAPSHOT_ID],
        "internal_run_ids": [INTERNAL_RUN_ID],
    }
    assert AUDIT_TABLE in executor.statement
    assert accumulator.active_references == {
        f"nonserving_residue:{AUDIT_TABLE}"
    }
    assert isinstance(classification, LegacyBlockedSuffix)
    assert classification.reasons == ("serving_or_lifecycle_reference",)

    plan = build_bounded_legacy_sweep_plan(
        schema_name="synthetic_mrf",
        control_schema_name="synthetic_control",
        authority_digest="b" * 64,
        catalog_digest="c" * 64,
        eligible_candidates=(),
        blocked=(classification,),
        limits=LegacySweepLimits(
            max_suffixes=1,
            max_tables=10,
            max_relations=20,
            max_bytes=1024,
        ),
    )

    assert plan.candidates == ()
    assert plan.eligible_suffix_count == 0
    assert plan.blocked == (classification,)
