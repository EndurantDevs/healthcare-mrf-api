# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Cross-owner references and fence evidence for legacy PTG cleanup."""

from __future__ import annotations

from typing import Any, Mapping

from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.ptg2_legacy_orphan_contract import LegacySuffixOwnership
from process.ptg_parts.ptg2_legacy_orphan_store_common import (
    _BLOCKING_ATTACHMENTS,
    _MRF_OPTIONAL_TABLES,
    _OwnershipAccumulator,
    _bare_control_suffix,
    _internal_run_suffix,
    _row_mapping,
    _schema_table,
    LegacyRelationCatalog,
)
from process.ptg_parts.ptg2_legacy_orphan_store_ownership import (
    _control_ownership,
    _internal_run_ownership,
    _mirror_run_ownership,
    _new_accumulators,
    _reference_rows,
    _snapshot_ownership,
)


def _attach_declared_snapshot_conflicts(
    accumulators: Mapping[str, _OwnershipAccumulator],
) -> None:
    owner_suffixes_by_snapshot: dict[str, set[str]] = {}
    for suffix, accumulator in accumulators.items():
        for snapshot_id in accumulator.declared_snapshot_ids:
            owner_suffixes_by_snapshot.setdefault(
                snapshot_id,
                set(),
            ).add(suffix)
    for suffixes_for_snapshot in owner_suffixes_by_snapshot.values():
        if len(suffixes_for_snapshot) <= 1:
            continue
        for suffix in suffixes_for_snapshot:
            accumulators[suffix].ambiguity_reasons.add(
                "declared_snapshot_owner_conflict"
            )


def _attach_raw_snapshot_owner_conflicts(
    accumulators: Mapping[str, _OwnershipAccumulator],
    raw_suffixes_by_snapshot: Mapping[str, set[str]],
) -> None:
    declared_suffixes_by_snapshot = _suffixes_by_snapshot(accumulators)
    for snapshot_id, declared_suffixes in declared_suffixes_by_snapshot.items():
        raw_suffixes = raw_suffixes_by_snapshot.get(snapshot_id)
        if raw_suffixes is None or raw_suffixes == declared_suffixes:
            continue
        affected_suffixes = declared_suffixes.union(
            raw_suffixes.intersection(accumulators)
        )
        for suffix in affected_suffixes:
            accumulators[suffix].ambiguity_reasons.add(
                "declared_snapshot_raw_owner_conflict"
            )


def _blocking_attachment_statements(
    schema_name: str,
    *,
    present_optional_table_names: frozenset[str],
) -> list[str]:
    if not present_optional_table_names.issubset(_MRF_OPTIONAL_TABLES):
        raise ValueError("legacy sweep optional authority is invalid")
    statements = []
    for table_name, snapshot_columns, run_columns in _BLOCKING_ATTACHMENTS:
        if (
            table_name in _MRF_OPTIONAL_TABLES
            and table_name not in present_optional_table_names
        ):
            continue
        table = _schema_table(schema_name, table_name)
        statements.extend(
            (
                "SELECT DISTINCT "
                f"'{table_name}'::text AS attachment_name, "
                f"{_quote_ident(column_name)}::text AS snapshot_id, "
                f"NULL::text AS internal_run_id FROM {table} "
                f"WHERE {_quote_ident(column_name)} = "
                "ANY(CAST(:snapshot_ids AS text[]))"
            )
            for column_name in snapshot_columns
        )
        statements.extend(
            (
                "SELECT DISTINCT "
                f"'{table_name}'::text AS attachment_name, "
                "NULL::text AS snapshot_id, "
                f"{_quote_ident(column_name)}::text AS internal_run_id "
                f"FROM {table} WHERE {_quote_ident(column_name)} = "
                "ANY(CAST(:internal_run_ids AS text[]))"
            )
            for column_name in run_columns
        )
    return statements


async def _attach_blocking_residue(
    executor: Any,
    *,
    schema_name: str,
    accumulators: Mapping[str, _OwnershipAccumulator],
    suffixes_by_snapshot: Mapping[str, set[str]],
    present_optional_table_names: frozenset[str],
) -> None:
    snapshot_ids = sorted(suffixes_by_snapshot)
    internal_run_ids = sorted(
        {
            run_id
            for accumulator in accumulators.values()
            for run_id, _status in accumulator.internal_run_statuses
        }
    )
    statements = _blocking_attachment_statements(
        schema_name,
        present_optional_table_names=present_optional_table_names,
    )
    if not statements:
        return
    residue_rows = await executor.all(
        " UNION ALL ".join(statements),
        snapshot_ids=snapshot_ids or [""],
        internal_run_ids=internal_run_ids or [""],
    )
    for residue_row in residue_rows:
        mapping = _row_mapping(residue_row)
        affected_suffixes = set(
            suffixes_by_snapshot.get(
                str(mapping.get("snapshot_id") or ""),
                set(),
            )
        )
        run_suffix = _internal_run_suffix(mapping.get("internal_run_id"))
        if run_suffix in accumulators:
            affected_suffixes.add(run_suffix)
        for suffix in affected_suffixes:
            accumulators[suffix].active_references.add(
                "nonserving_residue:" + str(mapping["attachment_name"])
            )


def _suffixes_by_snapshot(
    accumulators: Mapping[str, _OwnershipAccumulator],
) -> dict[str, set[str]]:
    suffixes_by_snapshot: dict[str, set[str]] = {}
    for suffix, accumulator in accumulators.items():
        for snapshot_id in accumulator.declared_snapshot_ids:
            suffixes_by_snapshot.setdefault(snapshot_id, set()).add(suffix)
    return suffixes_by_snapshot


async def _attach_reverse_owner_conflicts(
    executor: Any,
    *,
    schema_name: str,
    control_schema_name: str,
    accumulators: Mapping[str, _OwnershipAccumulator],
    suffixes_by_snapshot: Mapping[str, set[str]],
) -> None:
    snapshot_ids = sorted(suffixes_by_snapshot)
    if not snapshot_ids:
        return
    reverse_owner_rows = await executor.all(
        f"""
        SELECT snapshot_id, source_file_import_id AS owner_id,
               'mirror_run' AS owner_kind
          FROM {_schema_table(schema_name, 'import_run')}
         WHERE snapshot_id = ANY(CAST(:snapshot_ids AS text[]))
        UNION ALL
        SELECT snapshot_id, source_file_import_id, 'control_import'
          FROM {_schema_table(control_schema_name, 'source_file_import')}
         WHERE snapshot_id = ANY(CAST(:snapshot_ids AS text[]))
        UNION ALL
        SELECT snapshot_id, source_file_import_id, 'file_placement'
          FROM {_schema_table(control_schema_name, 'ptg_file_placement')}
         WHERE snapshot_id = ANY(CAST(:snapshot_ids AS text[]))
        """,
        snapshot_ids=snapshot_ids,
    )
    for owner_row in reverse_owner_rows:
        mapping = _row_mapping(owner_row)
        candidate_suffixes = suffixes_by_snapshot.get(
            str(mapping["snapshot_id"]),
            set(),
        )
        owner_suffix = _bare_control_suffix(mapping.get("owner_id"))
        if owner_suffix is not None and candidate_suffixes == {owner_suffix}:
            continue
        for suffix in candidate_suffixes:
            accumulators[suffix].ambiguity_reasons.add(
                "snapshot_reverse_owner_conflict_"
                + str(mapping["owner_kind"])
            )


async def _attach_serving_references(
    executor: Any,
    *,
    schema_name: str,
    control_schema_name: str,
    accumulators: Mapping[str, _OwnershipAccumulator],
    suffixes_by_snapshot: Mapping[str, set[str]],
) -> None:
    reference_rows = await _reference_rows(
        executor,
        schema_name=schema_name,
        control_schema_name=control_schema_name,
        snapshot_ids=sorted(suffixes_by_snapshot),
    )
    for reference_row in reference_rows:
        snapshot_id = str(reference_row["snapshot_id"])
        for suffix in suffixes_by_snapshot.get(snapshot_id, ()):
            accumulators[suffix].active_references.add(
                str(reference_row["reference_kind"])
            )


async def _attach_attempt_fences(
    executor: Any,
    *,
    schema_name: str,
    accumulators: Mapping[str, _OwnershipAccumulator],
    suffixes_by_snapshot: Mapping[str, set[str]],
) -> None:
    if not accumulators:
        return
    snapshot_ids = sorted(suffixes_by_snapshot)
    fence_rows = await executor.all(
        f"""
        SELECT snapshot_id, internal_run_id, state
          FROM {_schema_table(schema_name, 'ptg2_v4_attempt_fence')}
         WHERE snapshot_id = ANY(CAST(:snapshot_ids AS text[]))
            OR internal_run_id = ANY(CAST(:run_ids AS text[]))
         ORDER BY snapshot_id, internal_run_id
        """,
        snapshot_ids=snapshot_ids or [""],
        run_ids=[f"ptg2:{suffix}" for suffix in accumulators],
    )
    for fence_row in fence_rows:
        mapping = _row_mapping(fence_row)
        snapshot_id = str(mapping.get("snapshot_id") or "")
        snapshot_suffixes = suffixes_by_snapshot.get(snapshot_id, set())
        run_suffix = _internal_run_suffix(mapping.get("internal_run_id"))
        affected_suffixes = set(snapshot_suffixes)
        if run_suffix in accumulators:
            affected_suffixes.add(run_suffix)
        if snapshot_suffixes and (
            run_suffix is None or run_suffix not in snapshot_suffixes
        ):
            for suffix in affected_suffixes:
                accumulators[suffix].ambiguity_reasons.add(
                    "attempt_fence_owner_conflict"
                )
        for suffix in affected_suffixes:
            accumulators[suffix].fence_states.add(
                (snapshot_id, str(mapping.get("state") or ""))
            )


async def _attach_references_and_fences(
    executor: Any,
    *,
    schema_name: str,
    control_schema_name: str,
    accumulators: Mapping[str, _OwnershipAccumulator],
    present_optional_table_names: frozenset[str],
) -> None:
    suffixes_by_snapshot = _suffixes_by_snapshot(accumulators)
    await _attach_reverse_owner_conflicts(
        executor,
        schema_name=schema_name,
        control_schema_name=control_schema_name,
        accumulators=accumulators,
        suffixes_by_snapshot=suffixes_by_snapshot,
    )
    await _attach_serving_references(
        executor,
        schema_name=schema_name,
        control_schema_name=control_schema_name,
        accumulators=accumulators,
        suffixes_by_snapshot=suffixes_by_snapshot,
    )
    await _attach_blocking_residue(
        executor,
        schema_name=schema_name,
        accumulators=accumulators,
        suffixes_by_snapshot=suffixes_by_snapshot,
        present_optional_table_names=present_optional_table_names,
    )
    await _attach_attempt_fences(
        executor,
        schema_name=schema_name,
        accumulators=accumulators,
        suffixes_by_snapshot=suffixes_by_snapshot,
    )


async def load_legacy_ownership(
    executor: Any,
    *,
    schema_name: str,
    control_schema_name: str,
    catalog: LegacyRelationCatalog,
    present_optional_table_names: frozenset[str],
) -> Mapping[str, LegacySuffixOwnership]:
    """Collect all ownership and serving references for catalog suffixes."""

    suffixes = sorted(catalog.relations_by_suffix)
    accumulators = _new_accumulators(
        suffixes,
        catalog.ambiguity_by_suffix,
    )
    root_suffix_by_name = {
        relation.table_name: suffix
        for suffix, relations in catalog.relations_by_suffix.items()
        for relation in relations
    }
    await _internal_run_ownership(
        executor,
        schema_name,
        accumulators,
    )
    await _mirror_run_ownership(
        executor,
        schema_name,
        suffixes,
        accumulators,
    )
    await _control_ownership(
        executor,
        control_schema_name,
        suffixes,
        accumulators,
    )
    raw_suffixes_by_snapshot = await _snapshot_ownership(
        executor,
        schema_name,
        root_suffix_by_name,
        accumulators,
    )
    _attach_raw_snapshot_owner_conflicts(
        accumulators,
        raw_suffixes_by_snapshot,
    )
    await _attach_references_and_fences(
        executor,
        schema_name=schema_name,
        control_schema_name=control_schema_name,
        accumulators=accumulators,
        present_optional_table_names=present_optional_table_names,
    )
    _attach_declared_snapshot_conflicts(accumulators)
    return {
        suffix: accumulator.freeze()
        for suffix, accumulator in accumulators.items()
    }
