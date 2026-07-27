# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Owner and run evidence loading for legacy PTG cleanup."""

from __future__ import annotations

from typing import Any, Iterable, Mapping

from process.ptg_parts.ptg2_legacy_orphan_models import (
    LEGACY_SWEEP_MAX_OWNERSHIP_ROWS,
)
from process.ptg_parts.ptg2_legacy_orphan_store_common import (
    _OwnershipAccumulator,
    _bare_control_suffix,
    _internal_run_suffix,
    _row_mapping,
    _schema_table,
    _snapshot_manifest_suffixes,
)

_TERMINAL_JOB_STATUSES = frozenset(
    {"canceled", "cancelled", "dead_letter", "failed", "succeeded"}
)


async def _bounded_owner_rows(
    executor: Any,
    statement: str,
    *,
    evidence_kind: str,
    **parameters: Any,
) -> list[Any]:
    """Load one candidate-filtered owner surface under a hard row ceiling."""

    rows = await executor.all(
        statement + "\nLIMIT :owner_row_limit",
        owner_row_limit=LEGACY_SWEEP_MAX_OWNERSHIP_ROWS + 1,
        **parameters,
    )
    if len(rows) > LEGACY_SWEEP_MAX_OWNERSHIP_ROWS:
        raise RuntimeError(
            f"legacy_sweep_{evidence_kind}_scan_limit_exceeded"
        )
    return rows


def _new_accumulators(
    suffixes: Iterable[str],
    ambiguity_by_suffix: Mapping[str, tuple[str, ...]],
) -> dict[str, _OwnershipAccumulator]:
    accumulator_by_suffix = {
        suffix: _OwnershipAccumulator() for suffix in sorted(set(suffixes))
    }
    for suffix, reasons in ambiguity_by_suffix.items():
        if suffix in accumulator_by_suffix:
            accumulator_by_suffix[suffix].ambiguity_reasons.update(reasons)
    return accumulator_by_suffix


async def _candidate_snapshot_rows(
    executor: Any,
    schema_name: str,
    accumulators: Mapping[str, _OwnershipAccumulator],
) -> list[Any]:
    candidate_suffixes = sorted(accumulators)
    candidate_run_ids = [f"ptg2:{suffix}" for suffix in candidate_suffixes]
    declared_snapshot_ids = sorted(
        {
            snapshot_id
            for accumulator in accumulators.values()
            for snapshot_id in accumulator.declared_snapshot_ids
        }
    )
    manifest_patterns = [f"%{suffix}%" for suffix in candidate_suffixes]
    return await _bounded_owner_rows(
        executor,
        f"""
        SELECT snapshot_id, import_run_id, status, manifest
          FROM {_schema_table(schema_name, 'ptg2_snapshot')}
         WHERE import_run_id = ANY(CAST(:candidate_run_ids AS text[]))
            OR snapshot_id = ANY(CAST(:declared_snapshot_ids AS text[]))
            OR manifest::text LIKE ANY(CAST(:manifest_patterns AS text[]))
         ORDER BY snapshot_id
        """,
        evidence_kind="snapshot",
        candidate_run_ids=candidate_run_ids or [""],
        declared_snapshot_ids=declared_snapshot_ids or [""],
        manifest_patterns=manifest_patterns or [""],
    )


async def _snapshot_ownership(
    executor: Any,
    schema_name: str,
    root_suffix_by_name: Mapping[str, str],
    accumulators: Mapping[str, _OwnershipAccumulator],
) -> dict[str, set[str]]:
    snapshot_rows = await _candidate_snapshot_rows(
        executor,
        schema_name,
        accumulators,
    )
    raw_suffixes_by_snapshot: dict[str, set[str]] = {}
    for snapshot_row in snapshot_rows:
        mapping = _row_mapping(snapshot_row)
        snapshot_id = str(mapping["snapshot_id"])
        raw_suffixes = set(
            _snapshot_manifest_suffixes(
                mapping.get("manifest"),
                root_suffix_by_name,
            )
        )
        raw_import_run_id = str(mapping.get("import_run_id") or "").strip()
        import_suffix = _internal_run_suffix(raw_import_run_id)
        if import_suffix:
            raw_suffixes.add(import_suffix)
        elif raw_import_run_id:
            for suffix in raw_suffixes.intersection(accumulators):
                accumulators[suffix].ambiguity_reasons.add(
                    "snapshot_import_run_identity_malformed"
                )
        raw_suffixes_by_snapshot[snapshot_id] = raw_suffixes
        candidate_owners = raw_suffixes.intersection(accumulators)
        if len(raw_suffixes) != 1:
            for suffix in candidate_owners:
                accumulators[suffix].ambiguity_reasons.add(
                    "snapshot_owner_suffix_conflict"
                )
        for suffix in candidate_owners:
            accumulator = accumulators[suffix]
            accumulator.snapshot_statuses.add(
                (snapshot_id, str(mapping.get("status") or ""))
            )
            accumulator.declared_snapshot_ids.add(snapshot_id)
            accumulator.evidence_kinds.add("snapshot")
    return raw_suffixes_by_snapshot


async def _internal_run_ownership(
    executor: Any,
    schema_name: str,
    accumulators: Mapping[str, _OwnershipAccumulator],
) -> None:
    candidate_run_ids = [f"ptg2:{suffix}" for suffix in sorted(accumulators)]
    internal_run_rows = await _bounded_owner_rows(
        executor,
        f"""
        SELECT import_run_id, status
          FROM {_schema_table(schema_name, 'ptg2_import_run')}
         WHERE import_run_id = ANY(CAST(:candidate_run_ids AS text[]))
         ORDER BY import_run_id
        """,
        evidence_kind="internal_run",
        candidate_run_ids=candidate_run_ids or [""],
    )
    for internal_run_row in internal_run_rows:
        mapping = _row_mapping(internal_run_row)
        suffix = _internal_run_suffix(mapping.get("import_run_id"))
        if suffix not in accumulators:
            continue
        accumulators[suffix].internal_run_statuses.add(
            (
                str(mapping["import_run_id"]),
                str(mapping.get("status") or ""),
            )
        )
        accumulators[suffix].evidence_kinds.add("internal_run")
    job_rows = await _bounded_owner_rows(
        executor,
        f"""
        SELECT import_job_id, import_run_id, status
          FROM {_schema_table(schema_name, 'ptg2_import_job')}
         WHERE import_run_id = ANY(CAST(:candidate_run_ids AS text[]))
         ORDER BY import_run_id, import_job_id
        """,
        evidence_kind="import_job",
        candidate_run_ids=candidate_run_ids or [""],
    )
    for job_row in job_rows:
        mapping = _row_mapping(job_row)
        suffix = _internal_run_suffix(mapping.get("import_run_id"))
        if suffix not in accumulators:
            continue
        status = str(mapping.get("status") or "").strip().lower()
        if status not in _TERMINAL_JOB_STATUSES:
            accumulators[suffix].ambiguity_reasons.add(
                f"active_import_job_{status or 'missing'}"
            )


async def _mirror_run_ownership(
    executor: Any,
    schema_name: str,
    suffixes: list[str],
    accumulators: Mapping[str, _OwnershipAccumulator],
) -> None:
    mirror_run_rows = await _bounded_owner_rows(
        executor,
        f"""
        SELECT run_id, source_file_import_id, status, snapshot_id
          FROM {_schema_table(schema_name, 'import_run')}
         WHERE source_file_import_id = ANY(CAST(:suffixes AS text[]))
         ORDER BY source_file_import_id, run_id
        """,
        evidence_kind="mirror_run",
        suffixes=suffixes,
    )
    for mirror_run_row in mirror_run_rows:
        mapping = _row_mapping(mirror_run_row)
        suffix = _bare_control_suffix(mapping["source_file_import_id"])
        if suffix not in accumulators:
            continue
        accumulator = accumulators[suffix]
        accumulator.mirror_run_statuses.add(
            (str(mapping["run_id"]), str(mapping.get("status") or ""))
        )
        snapshot_id = str(mapping.get("snapshot_id") or "").strip()
        if snapshot_id:
            accumulator.declared_snapshot_ids.add(snapshot_id)
        accumulator.evidence_kinds.add("mirror_run")


async def _control_ownership(
    executor: Any,
    control_schema_name: str,
    suffixes: list[str],
    accumulators: Mapping[str, _OwnershipAccumulator],
) -> None:
    control_import_rows = await _bounded_owner_rows(
        executor,
        f"""
        SELECT source_file_import_id, status, snapshot_id
          FROM {_schema_table(control_schema_name, 'source_file_import')}
         WHERE source_file_import_id = ANY(CAST(:suffixes AS text[]))
         ORDER BY source_file_import_id
        """,
        evidence_kind="control_import",
        suffixes=suffixes,
    )
    for control_import_row in control_import_rows:
        mapping = _row_mapping(control_import_row)
        suffix = _bare_control_suffix(mapping["source_file_import_id"])
        if suffix not in accumulators:
            continue
        accumulator = accumulators[suffix]
        accumulator.control_import_statuses.add(
            (suffix, str(mapping.get("status") or ""))
        )
        snapshot_id = str(mapping.get("snapshot_id") or "").strip()
        if snapshot_id:
            accumulator.declared_snapshot_ids.add(snapshot_id)
        accumulator.evidence_kinds.add("control_import")
    placement_rows = await _bounded_owner_rows(
        executor,
        f"""
        SELECT placement_id, source_file_import_id, status, snapshot_id
          FROM {_schema_table(control_schema_name, 'ptg_file_placement')}
         WHERE source_file_import_id = ANY(CAST(:suffixes AS text[]))
         ORDER BY source_file_import_id, placement_id
        """,
        evidence_kind="file_placement",
        suffixes=suffixes,
    )
    for placement_row in placement_rows:
        mapping = _row_mapping(placement_row)
        suffix = _bare_control_suffix(mapping["source_file_import_id"])
        if suffix not in accumulators:
            continue
        accumulator = accumulators[suffix]
        accumulator.placement_statuses.add(
            (
                str(mapping["placement_id"]),
                str(mapping.get("status") or ""),
            )
        )
        snapshot_id = str(mapping.get("snapshot_id") or "").strip()
        if snapshot_id:
            accumulator.declared_snapshot_ids.add(snapshot_id)
        accumulator.evidence_kinds.add("file_placement")


def _snapshot_reference_statement(
    schema_name: str,
    control_schema_name: str,
) -> str:
    snapshot_refs = (
        ("snapshot_id", "global_current", "ptg2_current_snapshot"),
        ("previous_snapshot_id", "global_previous", "ptg2_current_snapshot"),
        ("snapshot_id", "source_current", "ptg2_current_source_snapshot"),
        ("previous_snapshot_id", "source_previous", "ptg2_current_source_snapshot"),
        ("snapshot_id", "plan_source_current", "ptg2_current_plan_source"),
        ("previous_snapshot_id", "plan_source_previous", "ptg2_current_plan_source"),
        ("snapshot_id", "snapshot_pin", "ptg2_snapshot_pin"),
        ("snapshot_id", "release_binding", "plan_release_snapshot_binding"),
        ("snapshot_id", "shared_snapshot_binding", "ptg2_v3_snapshot_binding"),
    )
    statements = [
        (
            f"SELECT {column_name} AS snapshot_id, "
            f"'{reference_kind}'::text AS reference_kind "
            f"FROM {_schema_table(schema_name, table_name)} "
            f"WHERE {column_name} = ANY(CAST(:snapshot_ids AS text[]))"
        )
        for column_name, reference_kind, table_name in snapshot_refs
    ]
    control_refs = (
        ("ptg_route_index", "control_route", " AND status = 'active'"),
        ("hp_plan_release_binding", "control_release_binding", ""),
        ("hp_snapshot_pin", "control_snapshot_pin", ""),
    )
    statements.extend(
        (
            "SELECT snapshot_id, "
            f"'{reference_kind}'::text AS reference_kind "
            f"FROM {_schema_table(control_schema_name, table_name)} "
            "WHERE snapshot_id = ANY(CAST(:snapshot_ids AS text[]))"
            + qualifier
        )
        for table_name, reference_kind, qualifier in control_refs
    )
    return " UNION ALL ".join(statements)


async def _reference_rows(
    executor: Any,
    *,
    schema_name: str,
    control_schema_name: str,
    snapshot_ids: list[str],
) -> list[Mapping[str, Any]]:
    if not snapshot_ids:
        return []
    statement = _snapshot_reference_statement(
        schema_name,
        control_schema_name,
    )
    reference_rows = await executor.all(
        statement,
        snapshot_ids=snapshot_ids,
    )
    return [
        dict(_row_mapping(reference_row))
        for reference_row in reference_rows
    ]
