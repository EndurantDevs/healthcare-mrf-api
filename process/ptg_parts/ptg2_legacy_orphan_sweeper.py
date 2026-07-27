# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded, audited cleanup of legacy PTG relation families."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Mapping

from db.connection import db
from process.ptg_parts.ptg2_legacy_orphan_contract import (
    LegacyBlockedSuffix,
    LegacySweepCandidate,
    LegacySweepLimits,
    LegacySweepPlan,
    build_bounded_legacy_sweep_plan,
    classify_legacy_suffix,
    legacy_sweep_audit_id,
)
from process.ptg_parts.ptg2_legacy_orphan_store import (
    LegacySweepAuditRecord,
    delete_legacy_snapshot_metadata,
    drop_legacy_root_relations,
    insert_legacy_sweep_audit,
    load_legacy_ownership,
    load_legacy_relation_catalog,
    load_legacy_sweep_audit,
    lock_legacy_root_relations,
    lock_legacy_sweep_authority,
    require_legacy_sweep_schema,
    verify_applied_audit_state,
)
from process.ptg_parts.ptg2_schema import resolve_ptg2_schema


_HEX_64_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_ACTOR_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:@/-]{0,127}$")


@dataclass(frozen=True)
class LegacySweepExecution:
    """Terminal result for one reviewed sweep plan."""

    state: str
    audit_id: str
    plan: LegacySweepPlan | None
    selected_suffixes: int
    selected_root_tables: int
    selected_relations: int
    selected_bytes: int
    selected_snapshots: int


def _resolve_control_schema(control_schema_name: str | None) -> str:
    value = str(control_schema_name or "").strip()
    if not value:
        raise ValueError("legacy sweep control schema must be supplied")
    return value


async def build_legacy_orphan_sweep_plan(
    *,
    schema_name: str | None = None,
    control_schema_name: str | None = None,
    limits: LegacySweepLimits,
    executor: Any = db,
) -> LegacySweepPlan:
    """Inspect all legacy families and return a deterministic bounded plan."""

    resolved_schema = resolve_ptg2_schema(schema_name)
    resolved_control_schema = _resolve_control_schema(control_schema_name)
    limits.validate()
    authority = await require_legacy_sweep_schema(
        executor,
        schema_name=resolved_schema,
        control_schema_name=resolved_control_schema,
    )
    catalog = await load_legacy_relation_catalog(
        executor,
        schema_name=resolved_schema,
        probe_rows=True,
    )
    ownership_by_suffix = await load_legacy_ownership(
        executor,
        schema_name=resolved_schema,
        control_schema_name=resolved_control_schema,
        catalog=catalog,
        present_optional_table_names=frozenset(
            authority.present_optional_table_names
        ),
    )
    candidates: list[LegacySweepCandidate] = []
    blocked_suffixes: list[LegacyBlockedSuffix] = []
    for suffix in sorted(catalog.relations_by_suffix):
        classified = classify_legacy_suffix(
            suffix,
            catalog.relations_by_suffix[suffix],
            ownership_by_suffix[suffix],
        )
        if isinstance(classified, LegacySweepCandidate):
            candidates.append(classified)
        else:
            blocked_suffixes.append(classified)
    return build_bounded_legacy_sweep_plan(
        schema_name=resolved_schema,
        control_schema_name=resolved_control_schema,
        authority_digest=authority.catalog_digest,
        catalog_digest=catalog.catalog_digest,
        eligible_candidates=candidates,
        blocked=blocked_suffixes,
        limits=limits,
    )


def _validate_apply_inputs(expected_plan_digest: str, actor: str) -> None:
    if not _HEX_64_PATTERN.fullmatch(str(expected_plan_digest or "")):
        raise ValueError("expected legacy sweep plan digest is invalid")
    if not _ACTOR_PATTERN.fullmatch(str(actor or "")):
        raise ValueError("legacy sweep actor is invalid")


def _root_relation_oids(plan: LegacySweepPlan) -> list[int]:
    return sorted(
        relation.relation_oid
        for candidate in plan.candidates
        for relation in candidate.relations
    )


def _root_relations(plan: LegacySweepPlan) -> list[Any]:
    return [
        relation
        for candidate in plan.candidates
        for relation in candidate.relations
    ]


def _replay_execution(
    existing_audit: Mapping[str, Any],
    replay_counts: Mapping[str, int],
) -> LegacySweepExecution:
    return LegacySweepExecution(
        state="already_applied",
        audit_id=str(existing_audit["audit_id"]),
        plan=None,
        selected_suffixes=replay_counts["candidate_suffix_count"],
        selected_root_tables=replay_counts["root_table_count"],
        selected_relations=(
            replay_counts["root_table_count"]
            + replay_counts["dependent_relation_count"]
        ),
        selected_bytes=replay_counts["total_bytes"],
        selected_snapshots=replay_counts["snapshot_count"],
    )


def _audit_record(
    plan: LegacySweepPlan,
    *,
    audit_id: str,
    actor: str,
) -> LegacySweepAuditRecord:
    return LegacySweepAuditRecord(
        audit_id=audit_id,
        actor=actor,
        plan_digest=plan.plan_digest,
        catalog_digest=plan.catalog_digest,
        authority_digest=plan.authority_digest,
        candidate_suffix_count=len(plan.candidates),
        root_table_count=plan.table_count,
        dependent_relation_count=plan.relation_count - plan.table_count,
        snapshot_count=len(plan.snapshot_ids),
        nonempty_table_count=sum(
            candidate.nonempty_table_count for candidate in plan.candidates
        ),
        total_bytes=plan.total_bytes,
        root_relation_oids=_root_relation_oids(plan),
        snapshot_ids=list(plan.snapshot_ids),
        proof=plan.audit_payload(),
    )


async def _locked_plan(
    connection: Any,
    *,
    schema_name: str,
    control_schema_name: str,
    limits: LegacySweepLimits,
    expected_plan_digest: str,
) -> LegacySweepPlan:
    plan = await build_legacy_orphan_sweep_plan(
        schema_name=schema_name,
        control_schema_name=control_schema_name,
        limits=limits,
        executor=connection,
    )
    if plan.plan_digest != expected_plan_digest:
        raise RuntimeError("legacy_sweep_plan_digest_changed")
    if not plan.candidates:
        raise RuntimeError("legacy_sweep_plan_has_no_candidates")
    await lock_legacy_root_relations(
        connection,
        schema_name=schema_name,
        relations=_root_relations(plan),
    )
    locked_plan = await build_legacy_orphan_sweep_plan(
        schema_name=schema_name,
        control_schema_name=control_schema_name,
        limits=limits,
        executor=connection,
    )
    if locked_plan.plan_digest != expected_plan_digest:
        raise RuntimeError("legacy_sweep_plan_changed_after_lock")
    return locked_plan


async def _apply_locked_plan(
    connection: Any,
    *,
    schema_name: str,
    control_schema_name: str,
    limits: LegacySweepLimits,
    expected_plan_digest: str,
    audit_id: str,
    actor: str,
) -> LegacySweepExecution:
    plan = await _locked_plan(
        connection,
        schema_name=schema_name,
        control_schema_name=control_schema_name,
        limits=limits,
        expected_plan_digest=expected_plan_digest,
    )
    await insert_legacy_sweep_audit(
        connection,
        schema_name=schema_name,
        audit=_audit_record(plan, audit_id=audit_id, actor=actor),
    )
    await delete_legacy_snapshot_metadata(
        connection,
        schema_name=schema_name,
        snapshot_ids=list(plan.snapshot_ids),
        internal_run_ids=list(plan.internal_run_ids),
    )
    await drop_legacy_root_relations(
        connection,
        schema_name=schema_name,
        relations=_root_relations(plan),
    )
    return LegacySweepExecution(
        state="applied",
        audit_id=audit_id,
        plan=plan,
        selected_suffixes=len(plan.candidates),
        selected_root_tables=plan.table_count,
        selected_relations=plan.relation_count,
        selected_bytes=plan.total_bytes,
        selected_snapshots=len(plan.snapshot_ids),
    )


async def execute_legacy_orphan_sweep(
    *,
    expected_plan_digest: str,
    actor: str,
    schema_name: str | None = None,
    control_schema_name: str | None = None,
    limits: LegacySweepLimits,
    lock_timeout: str = "5s",
    database: Any = db,
) -> LegacySweepExecution:
    """Apply exactly one reviewed plan or prove an exact prior replay."""

    _validate_apply_inputs(expected_plan_digest, actor)
    resolved_schema = resolve_ptg2_schema(schema_name)
    resolved_control_schema = _resolve_control_schema(control_schema_name)
    audit_id = legacy_sweep_audit_id(expected_plan_digest)
    async with database.acquire() as connection:
        authority_before_lock = await require_legacy_sweep_schema(
            connection,
            schema_name=resolved_schema,
            control_schema_name=resolved_control_schema,
        )
        await lock_legacy_sweep_authority(
            connection,
            schema_name=resolved_schema,
            control_schema_name=resolved_control_schema,
            lock_timeout=lock_timeout,
            present_optional_table_names=authority_before_lock.present_optional_table_names,
        )
        authority_after_lock = await require_legacy_sweep_schema(
            connection,
            schema_name=resolved_schema,
            control_schema_name=resolved_control_schema,
        )
        if authority_after_lock != authority_before_lock:
            raise RuntimeError("legacy_sweep_authority_catalog_changed")
        existing_audit = await load_legacy_sweep_audit(
            connection,
            schema_name=resolved_schema,
            plan_digest=expected_plan_digest,
        )
        if existing_audit is not None:
            replay_counts = await verify_applied_audit_state(
                connection,
                schema_name=resolved_schema,
                control_schema_name=resolved_control_schema,
                audit_row=existing_audit,
                expected_plan_digest=expected_plan_digest,
            )
            return _replay_execution(existing_audit, replay_counts)
        return await _apply_locked_plan(
            connection,
            schema_name=resolved_schema,
            control_schema_name=resolved_control_schema,
            limits=limits,
            expected_plan_digest=expected_plan_digest,
            audit_id=audit_id,
            actor=actor,
        )


if __name__ == "__main__":
    from process.ptg_parts.ptg2_legacy_orphan_sweeper_cli import main

    main()
