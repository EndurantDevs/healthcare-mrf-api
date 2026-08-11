# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Transactional audit and mutation primitives for legacy PTG cleanup."""

from __future__ import annotations

import json
from dataclasses import dataclass, replace
from typing import Any, Iterable, Mapping

from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.ptg2_legacy_orphan_contract import (
    LEGACY_SWEEP_AUDIT_TABLE,
    LEGACY_SWEEP_CONTRACT,
    LegacyRootRelation,
    LegacySuffixOwnership,
)
from process.ptg_parts.ptg2_legacy_orphan_store_common import (
    _CONTROL_REQUIRED_TABLES,
    _MRF_OPTIONAL_TABLES,
    _MRF_REQUIRED_TABLES,
    _row_mapping,
    _schema_table,
    LegacyRelationCatalog,
)
from process.ptg_parts.ptg2_lifecycle_lock import (
    acquire_ptg2_lifecycle_lock,
)


async def lock_legacy_sweep_lifecycle(
    executor: Any,
    *,
    lock_timeout: str,
) -> None:
    """Acquire the shared lifecycle lock before any authority snapshot."""

    await acquire_ptg2_lifecycle_lock(
        executor,
        lock_timeout=lock_timeout,
    )


async def lock_legacy_sweep_authority(
    executor: Any,
    *,
    schema_name: str,
    control_schema_name: str,
    lock_timeout: str,
    present_optional_table_names: tuple[str, ...],
    lifecycle_locked: bool = False,
) -> None:
    """Serialize lifecycle changes and lock the resolved authority tables."""

    if not lifecycle_locked:
        await lock_legacy_sweep_lifecycle(
            executor,
            lock_timeout=lock_timeout,
        )
    optional_names = tuple(sorted(set(present_optional_table_names)))
    if not set(optional_names).issubset(_MRF_OPTIONAL_TABLES):
        raise ValueError("legacy sweep optional authority is invalid")
    await executor.status(
        "LOCK TABLE "
        + ", ".join(
            _schema_table(schema_name, table_name)
            for table_name in (*_MRF_REQUIRED_TABLES, *optional_names)
        )
        + " IN SHARE MODE"
    )
    await executor.status(
        "LOCK TABLE "
        + ", ".join(
            _schema_table(control_schema_name, table_name)
            for table_name in _CONTROL_REQUIRED_TABLES
        )
        + " IN SHARE MODE"
    )


async def lock_legacy_root_relations(
    executor: Any,
    *,
    schema_name: str,
    relations: Iterable[LegacyRootRelation],
) -> None:
    """Lock exact reviewed root tables against catalog drift."""

    names = sorted({relation.table_name for relation in relations})
    if not names:
        return
    await executor.status(
        "LOCK TABLE "
        + ", ".join(
            _schema_table(schema_name, table_name)
            for table_name in names
        )
        + " IN ACCESS EXCLUSIVE MODE"
    )


async def load_legacy_sweep_audit(
    executor: Any,
    *,
    schema_name: str,
    plan_digest: str,
) -> Mapping[str, Any] | None:
    """Load one immutable applied audit by exact plan digest."""

    audit_row = await executor.first(
        f"""
        SELECT *
          FROM {_schema_table(schema_name, LEGACY_SWEEP_AUDIT_TABLE)}
         WHERE plan_digest = decode(:plan_digest, 'hex')
        """,
        plan_digest=plan_digest,
    )
    return (
        None
        if audit_row is None
        else dict(_row_mapping(audit_row))
    )


@dataclass(frozen=True)
class LegacySweepAuditRecord:
    """Complete immutable record for one committed cleanup plan."""

    audit_id: str
    actor: str
    plan_digest: str
    authority_digest: str
    catalog_digest: str
    candidate_suffix_count: int
    root_table_count: int
    dependent_relation_count: int
    snapshot_count: int
    nonempty_table_count: int
    total_bytes: int
    root_relation_oids: list[int]
    snapshot_ids: list[str]
    proof: Mapping[str, Any]

    def parameters_by_name(self) -> dict[str, Any]:
        """Return database bind values with canonical proof encoding."""

        return {
            **self.__dict__,
            "proof": json.dumps(
                self.proof,
                sort_keys=True,
                separators=(",", ":"),
            ),
        }


def _audit_insert_statement(schema_name: str) -> str:
    return f"""
        INSERT INTO {_schema_table(schema_name, LEGACY_SWEEP_AUDIT_TABLE)} (
            audit_id,
            contract,
            actor,
            plan_digest,
            authority_digest,
            catalog_digest,
            candidate_suffix_count,
            root_table_count,
            dependent_relation_count,
            snapshot_count,
            nonempty_table_count,
            total_bytes,
            root_relation_oids,
            snapshot_ids,
            proof
        ) VALUES (
            :audit_id,
            'ptg2_legacy_orphan_sweep_v1',
            :actor,
            decode(:plan_digest, 'hex'),
            decode(:authority_digest, 'hex'),
            decode(:catalog_digest, 'hex'),
            :candidate_suffix_count,
            :root_table_count,
            :dependent_relation_count,
            :snapshot_count,
            :nonempty_table_count,
            :total_bytes,
            CAST(:root_relation_oids AS bigint[]),
            CAST(:snapshot_ids AS text[]),
            CAST(:proof AS jsonb)
        )
        ON CONFLICT (audit_id) DO NOTHING
    """


async def insert_legacy_sweep_audit(
    executor: Any,
    *,
    schema_name: str,
    audit: LegacySweepAuditRecord,
) -> None:
    """Insert exactly one immutable audit or fail on identity conflict."""

    inserted = await executor.status(
        _audit_insert_statement(schema_name),
        **audit.parameters_by_name(),
    )
    if int(inserted or 0) != 1:
        raise RuntimeError("legacy_sweep_audit_insert_conflict")


async def delete_legacy_snapshot_metadata(
    executor: Any,
    *,
    schema_name: str,
    snapshot_ids: list[str],
    internal_run_ids: list[str],
) -> None:
    """Delete only metadata owned by selected terminal snapshots and runs."""

    parameters_by_name = {
        "snapshot_ids": snapshot_ids or [""],
        "internal_run_ids": internal_run_ids or [""],
    }
    if snapshot_ids:
        for table_name in (
            "ptg2_v3_snapshot_scope",
            "ptg2_v3_snapshot_plan_scope",
            "ptg2_v3_snapshot_source",
        ):
            await executor.status(
                f"""
                DELETE FROM {_schema_table(schema_name, table_name)}
                 WHERE snapshot_id = ANY(CAST(:snapshot_ids AS text[]))
                """,
                **parameters_by_name,
            )
    await executor.status(
        f"""
        DELETE FROM {_schema_table(schema_name, 'ptg2_artifact_blob_chunk')}
         WHERE artifact_id IN (
            SELECT artifact_id
              FROM {_schema_table(schema_name, 'ptg2_artifact_manifest')}
             WHERE snapshot_id = ANY(CAST(:snapshot_ids AS text[]))
                OR import_run_id = ANY(CAST(:internal_run_ids AS text[]))
         )
        """,
        **parameters_by_name,
    )
    await executor.status(
        f"""
        DELETE FROM {_schema_table(schema_name, 'ptg2_artifact_manifest')}
         WHERE snapshot_id = ANY(CAST(:snapshot_ids AS text[]))
            OR import_run_id = ANY(CAST(:internal_run_ids AS text[]))
        """,
        **parameters_by_name,
    )
    if not snapshot_ids:
        return
    deleted = await executor.status(
        f"""
        DELETE FROM {_schema_table(schema_name, 'ptg2_snapshot')}
         WHERE snapshot_id = ANY(CAST(:snapshot_ids AS text[]))
           AND status IN ('failed', 'published')
        """,
        **parameters_by_name,
    )
    if int(deleted or 0) != len(snapshot_ids):
        raise RuntimeError("legacy_sweep_snapshot_delete_count_mismatch")


async def drop_legacy_root_relations(
    executor: Any,
    *,
    schema_name: str,
    relations: Iterable[LegacyRootRelation],
) -> None:
    """Drop exact locked root tables without cascade."""

    for relation in sorted(relations, key=lambda item: item.table_name):
        await executor.status(
            f"DROP TABLE "
            f"{_schema_table(schema_name, relation.table_name)}"
        )


def with_catalog_ambiguity(
    ownership: LegacySuffixOwnership,
    reasons: Iterable[str],
) -> LegacySuffixOwnership:
    """Attach catalog ambiguity without mutating shared ownership values."""

    return replace(
        ownership,
        ambiguity_reasons=tuple(
            sorted(
                {
                    *ownership.ambiguity_reasons,
                    *map(str, reasons),
                }
            )
        ),
    )
