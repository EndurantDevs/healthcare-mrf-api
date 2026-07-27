# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Strict replay verification for applied legacy PTG cleanup audits."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping

from process.ptg_parts.ptg2_legacy_orphan_contract import (
    LEGACY_SWEEP_CONTRACT,
    canonical_sha256,
    legacy_root_identity,
    legacy_sweep_audit_id,
)
from process.ptg_parts.ptg2_legacy_orphan_store_common import (
    _BARE_SUFFIX_PATTERN,
    _internal_run_suffix,
    _normalized_json,
    _schema_table,
)

@dataclass
class _ReplayProofState:
    suffixes: set[str] = field(default_factory=set)
    table_names: set[str] = field(default_factory=set)
    root_oids: set[int] = field(default_factory=set)
    dependent_oids: set[int] = field(default_factory=set)
    snapshot_ids: set[str] = field(default_factory=set)
    internal_run_ids: set[str] = field(default_factory=set)
    total_bytes: int = 0
    nonempty_table_count: int = 0

    def counts_by_field(self) -> dict[str, int]:
        """Return the exact aggregate values persisted by apply."""

        return {
            "candidate_suffix_count": len(self.suffixes),
            "root_table_count": len(self.table_names),
            "dependent_relation_count": len(self.dependent_oids),
            "snapshot_count": len(self.snapshot_ids),
            "nonempty_table_count": self.nonempty_table_count,
            "total_bytes": self.total_bytes,
        }


def _raise_invalid_replay_audit() -> None:
    raise RuntimeError("legacy_sweep_replay_audit_invalid")


def _validated_replay_candidates(
    audit_row: Mapping[str, Any],
    *,
    schema_name: str,
    control_schema_name: str,
    expected_plan_digest: str,
) -> list[Mapping[str, Any]]:
    proof = _normalized_json(audit_row.get("proof"))
    candidates = proof.get("candidates") if isinstance(proof, Mapping) else None
    envelope_matches = (
        isinstance(candidates, list)
        and bool(candidates)
        and proof.get("contract") == LEGACY_SWEEP_CONTRACT
        and proof.get("schema_name") == schema_name
        and proof.get("control_schema_name") == control_schema_name
        and canonical_sha256(proof) == expected_plan_digest
        and str(audit_row.get("audit_id") or "").strip()
        == legacy_sweep_audit_id(expected_plan_digest)
        and str(audit_row.get("contract") or "") == LEGACY_SWEEP_CONTRACT
        and bytes(audit_row.get("plan_digest") or b"").hex()
        == expected_plan_digest
        and bytes(audit_row.get("authority_digest") or b"").hex()
        == proof.get("authority_digest")
        and bytes(audit_row.get("catalog_digest") or b"").hex()
        == proof.get("catalog_digest")
    )
    if not envelope_matches:
        _raise_invalid_replay_audit()
    return candidates


def _add_replay_relation(
    state: _ReplayProofState,
    relation_proof: Mapping[str, Any],
    suffix: str,
) -> None:
    table_name = str(relation_proof.get("table_name") or "")
    identity = legacy_root_identity(table_name)
    relation_oid = int(relation_proof.get("relation_oid") or 0)
    dependent_values = relation_proof.get("dependent_relation_oids")
    relation_matches = (
        identity is not None
        and identity[1] == suffix
        and table_name not in state.table_names
        and relation_oid > 0
        and relation_oid not in state.root_oids
        and isinstance(dependent_values, list)
    )
    if not relation_matches:
        _raise_invalid_replay_audit()
    relation_dependents = {int(oid) for oid in dependent_values}
    has_dependency_conflict = (
        len(relation_dependents) != len(dependent_values)
        or bool(relation_dependents.intersection(state.dependent_oids))
        or bool(relation_dependents.intersection(state.root_oids))
    )
    if has_dependency_conflict:
        _raise_invalid_replay_audit()
    state.table_names.add(table_name)
    state.root_oids.add(relation_oid)
    state.dependent_oids.update(relation_dependents)
    state.total_bytes += int(relation_proof.get("total_bytes") or 0)
    state.nonempty_table_count += relation_proof.get("has_rows") is True


def _add_replay_ownership(
    state: _ReplayProofState,
    ownership_proof: Mapping[str, Any],
    suffix: str,
) -> None:
    for snapshot_pair in ownership_proof.get("snapshot_statuses", []):
        if not isinstance(snapshot_pair, list) or len(snapshot_pair) != 2:
            _raise_invalid_replay_audit()
        state.snapshot_ids.add(str(snapshot_pair[0]))
    for run_pair in ownership_proof.get("internal_run_statuses", []):
        if not isinstance(run_pair, list) or len(run_pair) != 2:
            _raise_invalid_replay_audit()
        run_id = str(run_pair[0])
        if _internal_run_suffix(run_id) != suffix:
            _raise_invalid_replay_audit()
        state.internal_run_ids.add(run_id)


def _replay_proof_state(
    candidates: list[Mapping[str, Any]],
) -> _ReplayProofState:
    state = _ReplayProofState()
    for candidate_proof in candidates:
        if not isinstance(candidate_proof, Mapping):
            _raise_invalid_replay_audit()
        suffix = str(candidate_proof.get("suffix") or "")
        relation_proofs = candidate_proof.get("relations")
        ownership_proof = candidate_proof.get("ownership")
        candidate_matches = (
            bool(_BARE_SUFFIX_PATTERN.fullmatch(suffix))
            and suffix not in state.suffixes
            and isinstance(relation_proofs, list)
            and bool(relation_proofs)
            and isinstance(ownership_proof, Mapping)
        )
        if not candidate_matches:
            _raise_invalid_replay_audit()
        state.suffixes.add(suffix)
        for relation_proof in relation_proofs:
            if not isinstance(relation_proof, Mapping):
                _raise_invalid_replay_audit()
            _add_replay_relation(state, relation_proof, suffix)
        _add_replay_ownership(state, ownership_proof, suffix)
    if state.root_oids.intersection(state.dependent_oids):
        _raise_invalid_replay_audit()
    return state


def _validate_replay_aggregates(
    audit_row: Mapping[str, Any],
    state: _ReplayProofState,
) -> None:
    counts_by_field = state.counts_by_field()
    has_count_mismatch = any(
        int(audit_row.get(field_name) or 0) != expected_count
        for field_name, expected_count in counts_by_field.items()
    )
    stored_root_oids = [
        int(oid) for oid in audit_row.get("root_relation_oids", [])
    ]
    stored_snapshot_ids = [
        str(snapshot_id) for snapshot_id in audit_row.get("snapshot_ids", [])
    ]
    if (
        has_count_mismatch
        or stored_root_oids != sorted(state.root_oids)
        or stored_snapshot_ids != sorted(state.snapshot_ids)
    ):
        _raise_invalid_replay_audit()


async def _remaining_relation_count(
    executor: Any,
    schema_name: str,
    state: _ReplayProofState,
) -> int:
    if not state.table_names:
        return 0
    return int(
        await executor.scalar(
            """
            SELECT COUNT(*)
              FROM pg_class AS relation_record
              JOIN pg_namespace AS namespace_record
                ON namespace_record.oid = relation_record.relnamespace
             WHERE namespace_record.nspname = :schema_name
               AND (
                    relation_record.relname = ANY(CAST(:table_names AS text[]))
                    OR relation_record.oid = ANY(CAST(:relation_oids AS oid[]))
               )
            """,
            schema_name=schema_name,
            table_names=sorted(state.table_names),
            relation_oids=sorted(state.root_oids | state.dependent_oids),
        )
        or 0
    )


async def _remaining_snapshot_count(
    executor: Any,
    schema_name: str,
    state: _ReplayProofState,
) -> int:
    if not state.snapshot_ids:
        return 0
    return int(
        await executor.scalar(
            f"""
            SELECT COUNT(*)
              FROM {_schema_table(schema_name, 'ptg2_snapshot')}
             WHERE snapshot_id = ANY(CAST(:snapshot_ids AS text[]))
            """,
            snapshot_ids=sorted(state.snapshot_ids),
        )
        or 0
    )


async def _remaining_metadata_count(
    executor: Any,
    schema_name: str,
    state: _ReplayProofState,
) -> int:
    return int(
        await executor.scalar(
            f"""
            SELECT
                (SELECT COUNT(*)
                   FROM {_schema_table(schema_name, 'ptg2_v3_snapshot_scope')}
                  WHERE snapshot_id = ANY(CAST(:snapshot_ids AS text[])))
              + (SELECT COUNT(*)
                   FROM {_schema_table(schema_name, 'ptg2_v3_snapshot_plan_scope')}
                  WHERE snapshot_id = ANY(CAST(:snapshot_ids AS text[])))
              + (SELECT COUNT(*)
                   FROM {_schema_table(schema_name, 'ptg2_v3_snapshot_source')}
                  WHERE snapshot_id = ANY(CAST(:snapshot_ids AS text[])))
              + (SELECT COUNT(*)
                   FROM {_schema_table(schema_name, 'ptg2_artifact_manifest')}
                  WHERE snapshot_id = ANY(CAST(:snapshot_ids AS text[]))
                     OR import_run_id = ANY(CAST(:internal_run_ids AS text[])))
            """,
            snapshot_ids=sorted(state.snapshot_ids) or [""],
            internal_run_ids=sorted(state.internal_run_ids) or [""],
        )
        or 0
    )


async def verify_applied_audit_state(
    executor: Any,
    *,
    schema_name: str,
    control_schema_name: str,
    audit_row: Mapping[str, Any],
    expected_plan_digest: str,
) -> Mapping[str, int]:
    """Verify immutable proof and absence before accepting an apply replay."""

    candidates = _validated_replay_candidates(
        audit_row,
        schema_name=schema_name,
        control_schema_name=control_schema_name,
        expected_plan_digest=expected_plan_digest,
    )
    state = _replay_proof_state(candidates)
    _validate_replay_aggregates(audit_row, state)
    remaining_counts = (
        await _remaining_relation_count(executor, schema_name, state),
        await _remaining_snapshot_count(executor, schema_name, state),
        await _remaining_metadata_count(executor, schema_name, state),
    )
    if any(remaining_counts):
        raise RuntimeError("legacy_sweep_replay_state_incomplete")
    return state.counts_by_field()



