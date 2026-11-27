# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Atomic NOWAIT cutover, rollback, and retired-heap drop for history partition compaction."""

from __future__ import annotations

import json
from typing import Any

from process.provider_directory_history_compaction import (
    _COMPACTION_LOCK_PREFIX,
    _RETIRED_COMPONENT,
    _STAGE_COMPONENT,
    GENERIC_HISTORY_TABLES,
    HistoryCompactionError,
    _canonical_json,
    _history_attach_expr,
    _history_fingerprint,
    _literal,
    _load_migration_plan,
    _qualified,
    _quote,
    _sorted_keys,
    _stage_name,
    select_compaction_plan,
)


async def _install_candidate_triggers(connection: Any, schema: str, root: str, table_ref: str) -> None:
    for operation, image in (("INSERT", "NEW"), ("UPDATE", "NEW"), ("UPDATE", "OLD"), ("DELETE", "OLD")):
        trigger_name = f"a_pd_candidate_{operation.lower()}_{image.lower()}"
        await connection.execute(
            f"CREATE TRIGGER {trigger_name} AFTER {operation} ON {table_ref} REFERENCING {image} TABLE AS affected_rows "
            f"FOR EACH STATEMENT EXECUTE FUNCTION {_quote(schema)}.guard_pd_dataset_candidate_parent({_literal(root)},{_literal(image)})"
        )


async def _verify_stage_for_cutover(
    connection: Any,
    schema: str,
    root: str,
    stage_name: str,
    old_name: str,
    selection: Any,
) -> int:
    stage_ref = _qualified(schema, stage_name)
    stage_oid = await connection.fetchval("SELECT to_regclass($1)::oid::bigint", stage_ref)
    if stage_oid is None:
        raise HistoryCompactionError("compaction_stage_missing")
    raw_comment = await connection.fetchval("SELECT obj_description($1::oid, 'pg_class')", stage_oid)
    try:
        comment = json.loads(raw_comment) if type(raw_comment) is str else None
    except ValueError:
        comment = None
    if (
        type(comment) is not dict
        or comment.get("component") != _STAGE_COMPONENT
        or comment.get("root") != root
        or comment.get("plan_sha256") != selection.plan_sha256
    ):
        raise HistoryCompactionError("compaction_stage_invalid")
    retained = selection.retained_keys_by_table[root]
    old_ref = f"ONLY {_qualified(schema, old_name)}"
    expected_fp = await _history_fingerprint(connection, old_ref, retained)
    if expected_fp != comment.get("fingerprints"):
        raise HistoryCompactionError("compaction_source_changed")
    # Fingerprint the live stage under the cutover lock; its comment alone does not prove its rows.
    if await _history_fingerprint(connection, stage_ref, retained) != expected_fp:
        raise HistoryCompactionError("compaction_stage_changed")
    return int(stage_oid)


async def _retire_incumbent_history(
    connection: Any,
    schema: str,
    root: str,
    old_oid: int,
    stage_oid: int,
    parent_oid: int,
    old_keys: tuple[str, ...],
    retained: tuple[str, ...],
) -> str:
    """Detach and seal the incumbent history heap with a signed rollback comment."""
    parent_ref = _qualified(schema, root)
    old_name = f"pd_dataset_history_{old_oid}"
    retired_name = f"pd_dataset_history_retired_{old_oid}"
    if old_keys:
        await connection.execute(f"ALTER TABLE {parent_ref} DETACH PARTITION {_qualified(schema, old_name)}")
    await connection.execute(f"ALTER TABLE {_qualified(schema, old_name)} RENAME TO {_quote(retired_name)}")
    retired_ref = _qualified(schema, retired_name)
    await connection.execute(
        f"CREATE TRIGGER pd_dataset_sealed BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON {retired_ref} "
        f"FOR EACH STATEMENT EXECUTE FUNCTION {_quote(schema)}.deny_pd_dataset_candidate_write()"
    )
    await connection.execute(f"ALTER TABLE {retired_ref} ENABLE ALWAYS TRIGGER pd_dataset_sealed")
    receipt_by_field = {
        "component": _RETIRED_COMPONENT,
        "schema": schema,
        "root": root,
        "old_oid": old_oid,
        "new_oid": stage_oid,
        "parent_oid": parent_oid,
        "previous_keys": list(old_keys),
        "retained_keys": list(retained),
    }
    await connection.execute(f"COMMENT ON TABLE {retired_ref} IS {_literal(_canonical_json(receipt_by_field))}")
    return retired_name


async def _swap_one_table(
    connection: Any,
    schema: str,
    root: str,
    stage_name: str,
    stage_oid: int,
    selection: Any,
    updated_plan: dict[str, Any],
) -> dict[str, Any]:
    """Promote one staged table, retire its incumbent partition, and attach replacement."""
    old_oid = selection.old_oid_by_table[root]
    parent_oid = selection.parent_oid_by_table[root]
    old_keys = selection.current_keys_by_table[root]
    retained = selection.retained_keys_by_table[root]
    new_name = f"pd_dataset_history_{stage_oid}"
    await connection.execute(f"COMMENT ON TABLE {_qualified(schema, stage_name)} IS NULL")
    await connection.execute(f"ALTER TABLE {_qualified(schema, stage_name)} RENAME TO {_quote(new_name)}")
    new_ref = _qualified(schema, new_name)
    await _install_candidate_triggers(connection, schema, root, new_ref)
    retired_name = await _retire_incumbent_history(
        connection, schema, root, old_oid, stage_oid, parent_oid, old_keys, retained
    )
    if retained:
        bound_literals = ",".join(_literal(key) for key in retained)
        await connection.execute(
            f"ALTER TABLE {_qualified(schema, root)} ATTACH PARTITION {new_ref} FOR VALUES IN ({bound_literals})"
        )
        actual_bound = await connection.fetchval(
            "SELECT pg_get_expr(relpartbound, oid) FROM pg_class WHERE oid=$1::oid", stage_oid
        )
        if actual_bound != _history_attach_expr(retained):
            raise HistoryCompactionError("compaction_bound_mismatch")
    updated_plan["tables"][root] = {
        "original_oid": int(stage_oid),
        "parent_oid": int(parent_oid),
        "keys": list(retained),
    }
    return {
        "old_oid": old_oid,
        "new_oid": int(stage_oid),
        "retired_relation": retired_name,
        "retained_keys": len(retained),
    }


async def _lock_against_writers(connection: Any, relations: list[str]) -> None:
    """Hold SHARE NOWAIT so compared heaps cannot change; readers keep running during the scans."""
    await connection.execute("SET LOCAL lock_timeout = '1s'")
    await connection.execute("SET LOCAL standard_conforming_strings = 'on'")
    await connection.execute("LOCK TABLE " + ",".join(relations) + " IN SHARE MODE NOWAIT")


def _cutover_heaps(schema: str, selection: Any) -> list[str]:
    heap_refs = []
    for root in GENERIC_HISTORY_TABLES:
        old_name = f"pd_dataset_history_{selection.old_oid_by_table[root]}"
        heap_refs.extend(
            [f"ONLY {_qualified(schema, old_name)}", _qualified(schema, _stage_name(root, selection.plan_sha256))]
        )
    return heap_refs


async def _lock_cutover_relations(connection: Any, schema: str, selection: Any) -> None:
    """Acquire short NOWAIT locks across the plan, headers, parents, incumbents, and stages."""
    await connection.execute(
        f"LOCK TABLE {_qualified(schema, 'pd_dataset_migration_plan')}, "
        f"{_qualified(schema, 'provider_directory_endpoint_dataset')} IN SHARE MODE NOWAIT"
    )
    root_refs = [f"ONLY {_qualified(schema, root)}" for root in GENERIC_HISTORY_TABLES]
    locked_relations = root_refs + _cutover_heaps(schema, selection)
    await connection.execute("LOCK TABLE " + ",".join(locked_relations) + " IN ACCESS EXCLUSIVE MODE NOWAIT")


async def cutover_history_compaction(connection: Any, *, schema: str = "mrf") -> dict[str, Any]:
    """Atomically swap prepared history partitions under <1s NOWAIT locks, keeping rollback heaps."""
    lock_key = _COMPACTION_LOCK_PREFIX + schema
    locked = await connection.fetchval("SELECT pg_try_advisory_lock(hashtextextended($1, 0))", lock_key)
    if not locked:
        raise HistoryCompactionError("compaction_already_running")
    try:
        initial = await select_compaction_plan(connection, schema=schema)
        async with connection.transaction(isolation="read_committed"):
            # Fingerprint scans run under SHARE (writers wait, readers do not); the exclusive lock covers only DDL.
            await _lock_against_writers(connection, _cutover_heaps(schema, initial))
            stage_oid_by_root: dict[str, int] = {}
            for root in GENERIC_HISTORY_TABLES:
                old_name = f"pd_dataset_history_{initial.old_oid_by_table[root]}"
                stage_name = _stage_name(root, initial.plan_sha256)
                stage_oid_by_root[root] = await _verify_stage_for_cutover(
                    connection, schema, root, stage_name, old_name, initial
                )
            await _lock_cutover_relations(connection, schema, initial)
            selection = await select_compaction_plan(connection, schema=schema)
            if selection.plan_sha256 != initial.plan_sha256:
                raise HistoryCompactionError("compaction_selection_changed")
            updated_plan = json.loads(_canonical_json(selection.plan))
            swapped_by_table: dict[str, dict[str, Any]] = {}
            for root in GENERIC_HISTORY_TABLES:
                stage_name = _stage_name(root, selection.plan_sha256)
                swapped_by_table[root] = await _swap_one_table(
                    connection, schema, root, stage_name, stage_oid_by_root[root], selection, updated_plan
                )
            await connection.execute(
                f"UPDATE {_qualified(schema, 'pd_dataset_migration_plan')} SET plan=$1::jsonb WHERE singleton",
                _canonical_json(updated_plan),
            )
        return {"status": "cutover", "schema": schema, "tables": swapped_by_table}
    finally:
        await connection.execute("SELECT pg_advisory_unlock(hashtextextended($1, 0))", lock_key)


async def _load_rollback_receipts(connection: Any, schema: str, plan: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Validate retired partition receipts against the active plan."""
    retired_entries = await connection.fetch(
        """SELECT c.oid::bigint AS oid, c.relname, obj_description(c.oid, 'pg_class') AS comment_text
        FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname=$1 AND c.relname ~ '^pd_dataset_history_retired_[0-9]+$' ORDER BY c.relname""",
        schema,
    )
    receipt_by_root: dict[str, dict[str, Any]] = {}
    for retired_entry in retired_entries:
        comment = json.loads(retired_entry["comment_text"])
        if comment.get("component") != _RETIRED_COMPONENT or comment.get("schema") != schema:
            raise HistoryCompactionError("compaction_rollback_receipt_invalid")
        receipt_by_root[comment["root"]] = comment
    if set(receipt_by_root) != set(GENERIC_HISTORY_TABLES):
        raise HistoryCompactionError("compaction_rollback_incomplete")
    for root in GENERIC_HISTORY_TABLES:
        if plan["tables"][root]["original_oid"] != receipt_by_root[root]["new_oid"]:
            raise HistoryCompactionError("compaction_rollback_plan_mismatch")
    return receipt_by_root


def _rollback_heaps(schema: str, receipt_by_root: dict[str, dict[str, Any]]) -> list[str]:
    heap_refs = []
    for root in GENERIC_HISTORY_TABLES:
        new_name, retired_name = _rollback_names(receipt_by_root[root])
        heap_refs.extend([f"ONLY {_qualified(schema, new_name)}", f"ONLY {_qualified(schema, retired_name)}"])
    return heap_refs


def _rollback_names(receipt_entry: dict[str, Any]) -> tuple[str, str]:
    return f"pd_dataset_history_{receipt_entry['new_oid']}", f"pd_dataset_history_retired_{receipt_entry['old_oid']}"


async def _verify_rollback_parity(connection: Any, schema: str, receipt_entry: dict[str, Any]) -> None:
    """Refuse rollback when the active heap no longer holds exactly the rows the retired heap kept."""
    retained_keys = await _sorted_keys(connection, receipt_entry["retained_keys"])
    new_name, retired_name = _rollback_names(receipt_entry)
    new_fingerprint = await _history_fingerprint(connection, _qualified(schema, new_name), retained_keys)
    if new_fingerprint != await _history_fingerprint(
        connection, f"ONLY {_qualified(schema, retired_name)}", retained_keys
    ):
        raise HistoryCompactionError("compaction_rollback_parity_failed")


async def _restore_one_table(
    connection: Any,
    schema: str,
    root: str,
    receipt_entry: dict[str, Any],
    restored_plan: dict[str, Any],
) -> int:
    """Reattach the retired partition for one history root; parity is verified beforehand."""
    old_oid, new_oid = receipt_entry["old_oid"], receipt_entry["new_oid"]
    new_name = f"pd_dataset_history_{new_oid}"
    retired_name = f"pd_dataset_history_retired_{old_oid}"
    old_name = f"pd_dataset_history_{old_oid}"
    prev_keys = await _sorted_keys(connection, receipt_entry["previous_keys"])
    retained_keys = await _sorted_keys(connection, receipt_entry["retained_keys"])
    parent_ref = _qualified(schema, root)
    if retained_keys:
        await connection.execute(f"ALTER TABLE {parent_ref} DETACH PARTITION {_qualified(schema, new_name)}")
    retired_ref = _qualified(schema, retired_name)
    await connection.execute(f"DROP TRIGGER pd_dataset_sealed ON {retired_ref}")
    await connection.execute(f"COMMENT ON TABLE {retired_ref} IS NULL")
    await connection.execute(f"ALTER TABLE {retired_ref} RENAME TO {_quote(old_name)}")
    if prev_keys:
        bound_literals = ",".join(_literal(key) for key in prev_keys)
        await connection.execute(
            f"ALTER TABLE {parent_ref} ATTACH PARTITION {_qualified(schema, old_name)} FOR VALUES IN ({bound_literals})"
        )
    await connection.execute(f"DROP TABLE {_qualified(schema, new_name)} RESTRICT")
    restored_plan["tables"][root] = {
        "original_oid": int(old_oid),
        "parent_oid": int(receipt_entry["parent_oid"]),
        "keys": list(prev_keys),
    }
    return int(old_oid)


async def rollback_history_compaction(connection: Any, *, schema: str = "mrf") -> dict[str, Any]:
    """Restore previous history partitions atomically from retained pd_dataset_history_retired_* heaps."""
    lock_key = _COMPACTION_LOCK_PREFIX + schema
    locked = await connection.fetchval("SELECT pg_try_advisory_lock(hashtextextended($1, 0))", lock_key)
    if not locked:
        raise HistoryCompactionError("compaction_already_running")
    try:
        async with connection.transaction(isolation="read_committed"):
            plan = await _load_migration_plan(connection, schema)
            receipt_by_root = await _load_rollback_receipts(connection, schema, plan)
            heap_refs = _rollback_heaps(schema, receipt_by_root)
            await _lock_against_writers(connection, heap_refs)
            for root in GENERIC_HISTORY_TABLES:
                await _verify_rollback_parity(connection, schema, receipt_by_root[root])
            root_refs = [f"ONLY {_qualified(schema, root)}" for root in GENERIC_HISTORY_TABLES]
            locked_relations = [_qualified(schema, "pd_dataset_migration_plan"), *root_refs, *heap_refs]
            await connection.execute("LOCK TABLE " + ",".join(locked_relations) + " IN ACCESS EXCLUSIVE MODE NOWAIT")
            if await _load_migration_plan(connection, schema) != plan:
                raise HistoryCompactionError("compaction_rollback_plan_mismatch")
            restored_plan = json.loads(_canonical_json(plan))
            restored_oid_by_table: dict[str, int] = {}
            for root in GENERIC_HISTORY_TABLES:
                restored_oid_by_table[root] = await _restore_one_table(
                    connection, schema, root, receipt_by_root[root], restored_plan
                )
            await connection.execute(
                f"UPDATE {_qualified(schema, 'pd_dataset_migration_plan')} SET plan=$1::jsonb WHERE singleton",
                _canonical_json(restored_plan),
            )
        return {"status": "rolled_back", "schema": schema, "tables": restored_oid_by_table}
    finally:
        await connection.execute("SELECT pg_advisory_unlock(hashtextextended($1, 0))", lock_key)


async def drop_retired_history(connection: Any, *, schema: str = "mrf") -> dict[str, Any]:
    """Drop detached pd_dataset_history_retired_* heaps after cutover verification."""
    lock_key = _COMPACTION_LOCK_PREFIX + schema
    locked = await connection.fetchval("SELECT pg_try_advisory_lock(hashtextextended($1, 0))", lock_key)
    if not locked:
        raise HistoryCompactionError("compaction_already_running")
    try:
        plan = await _load_migration_plan(connection, schema)
        retired_entries = await connection.fetch(
            """SELECT c.oid::bigint AS oid, c.relname, c.relispartition,
              EXISTS(SELECT FROM pg_partition_ancestors(c.oid) a WHERE a.relid <> c.oid) AS has_parent,
              obj_description(c.oid, 'pg_class') AS comment_text
            FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
            WHERE n.nspname=$1 AND c.relname ~ '^pd_dataset_history_retired_[0-9]+$' ORDER BY c.relname""",
            schema,
        )
        dropped_relations = []
        for retired_entry in retired_entries:
            if retired_entry["relispartition"] or retired_entry["has_parent"]:
                raise HistoryCompactionError("compaction_retired_still_attached")
            comment = json.loads(retired_entry["comment_text"])
            root = comment.get("root")
            if root not in GENERIC_HISTORY_TABLES or plan["tables"][root]["original_oid"] == retired_entry["oid"]:
                raise HistoryCompactionError("compaction_retired_active_in_plan")
            await connection.execute(f"DROP TABLE {_qualified(schema, retired_entry['relname'])} RESTRICT")
            dropped_relations.append(retired_entry["relname"])
        return {"status": "dropped", "schema": schema, "dropped_relations": dropped_relations}
    finally:
        await connection.execute("SELECT pg_advisory_unlock(hashtextextended($1, 0))", lock_key)
