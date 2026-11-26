# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Online selection, staging, and parity verification for generic history partition compaction."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
from dataclasses import dataclass
from typing import Any

from db.migration_provider_directory_dataset_candidates import TABLES

GENERIC_HISTORY_TABLES = TABLES[:4]
_COMPACTION_LOCK_PREFIX = "provider_dataset_history_compaction:"
_STAGE_COMPONENT = "pd_dataset_history_stage"
_RETIRED_COMPONENT = "pd_dataset_history_retired"


class HistoryCompactionError(RuntimeError):
    """Compaction selection, staging, lock acquisition, or verification failed."""


@dataclass(frozen=True)
class HistoryCompactionPlan:
    """Deterministic subset of generic history datasets retained vs omitted."""

    schema: str
    plan: dict[str, Any]
    current_keys_by_table: dict[str, tuple[str, ...]]
    retained_keys_by_table: dict[str, tuple[str, ...]]
    omitted_keys: tuple[str, ...]
    old_oid_by_table: dict[str, int]
    parent_oid_by_table: dict[str, int]
    plan_sha256: str


def _quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _literal(text_value: str) -> str:
    return "'" + text_value.replace("'", "''") + "'"


def _qualified(schema: str, relation: str) -> str:
    return f"{_quote(schema)}.{_quote(relation)}"


def _canonical_json(document: Any) -> str:
    return json.dumps(document, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _stage_name(root: str, plan_sha256: str) -> str:
    digest = hashlib.md5(f"{root}:{plan_sha256}".encode()).hexdigest()[:16]
    return f"pd_dataset_history_stage_{digest}"


def _history_bound_sql(keys: tuple[str, ...]) -> str:
    if not keys:
        return "false"
    return "dataset_id IN (" + ",".join(_literal(key) for key in keys) + ")"


def _history_attach_expr(keys: tuple[str, ...]) -> str:
    return "FOR VALUES IN (" + ", ".join(_literal(key) for key in keys) + ")"


async def _load_migration_plan(connection: Any, schema: str) -> dict[str, Any]:
    raw_plan = await connection.fetchval(
        f"SELECT plan FROM {_qualified(schema, 'pd_dataset_migration_plan')} WHERE singleton"
    )
    plan = json.loads(raw_plan) if type(raw_plan) is str else raw_plan
    if type(plan) is not dict or plan.get("complete") is not True or type(plan.get("tables")) is not dict:
        raise HistoryCompactionError("compaction_plan_incomplete")
    if set(plan["tables"]) != set(TABLES):
        raise HistoryCompactionError("compaction_plan_invalid")
    return plan


async def _sorted_keys(connection: Any, keys: list[str]) -> tuple[str, ...]:
    if not keys:
        return ()
    key_entries = await connection.fetch("SELECT k FROM unnest($1::text[]) AS k ORDER BY k::varchar", keys)
    return tuple(entry["k"] for entry in key_entries)


async def _catalog_dataset_ids(connection: Any) -> set[str]:
    """Collect dataset identifiers referenced by any active FHIR catalog schema."""
    catalog_entries = await connection.fetch(
        "SELECT nspname FROM pg_namespace WHERE nspname ~ '^fhir_catalog_' ORDER BY nspname"
    )
    referenced_ids: set[str] = set()
    for catalog_entry in catalog_entries:
        cat_schema = catalog_entry["nspname"]
        table_oid = await connection.fetchval(
            "SELECT to_regclass($1)", _qualified(cat_schema, "registry_fhir_catalog_source")
        )
        if table_oid is None:
            continue
        dataset_entries = await connection.fetch(
            f"SELECT DISTINCT descriptor_json#>>'{{coordinates,dataset_id}}' AS dataset_id FROM {_qualified(cat_schema, 'registry_fhir_catalog_source')}"
        )
        referenced_ids.update(entry["dataset_id"] for entry in dataset_entries if entry["dataset_id"])
    return referenced_ids


async def _eligible_omitted_keys(connection: Any, schema: str, candidate_keys: list[str]) -> set[str]:
    """Identify retired non-current datasets with zero surviving downstream references."""
    if not candidate_keys:
        return set()
    catalog_ids = sorted(await _catalog_dataset_ids(connection))
    header_ref = _qualified(schema, "provider_directory_endpoint_dataset")
    guard_clauses = []
    for table_name in (
        "provider_directory_rooted_graph_dataset",
        "provider_directory_rooted_graph_dataset_resource",
        "provider_directory_uhc_flex_practitioner_dataset",
        "provider_directory_uhc_flex_practitioner_dataset_resource",
    ):
        if await connection.fetchval("SELECT to_regclass($1)", _qualified(schema, table_name)) is not None:
            guard_clauses.append(
                f"NOT EXISTS(SELECT FROM {_qualified(schema, table_name)} g WHERE g.dataset_id=d.dataset_id)"
            )
    extra_sql = (" AND " + " AND ".join(guard_clauses)) if guard_clauses else ""
    omitted_entries = await connection.fetch(
        f"""SELECT d.dataset_id FROM {header_ref} d
        WHERE d.dataset_id=ANY($1::text[])
          AND d.is_current IS FALSE
          AND NOT (d.dataset_id=ANY($2::text[]))
          AND (
            d.status='failed' OR (
              d.status='superseded'
              AND d.completion_proof_required_version IS NULL
              AND COALESCE(
                to_jsonb(d)->'publication_metadata_summary_json'->'source_ids',
                to_jsonb(d)->'publication_metadata_json'->'source_ids',
                '[]'::jsonb
              ) <> '["cms-npd"]'::jsonb
            )
          ){extra_sql}""",
        candidate_keys,
        catalog_ids,
    )
    return {entry["dataset_id"] for entry in omitted_entries}


def _extract_plan_metadata(
    plan: dict[str, Any],
) -> tuple[dict[str, tuple[str, ...]], dict[str, int], dict[str, int]]:
    """Validate and extract incumbent key lists and OIDs per generic history table."""
    current_keys_by_table: dict[str, tuple[str, ...]] = {}
    old_oid_by_table: dict[str, int] = {}
    parent_oid_by_table: dict[str, int] = {}
    for root in GENERIC_HISTORY_TABLES:
        identity = plan["tables"][root]
        if (
            type(identity) is not dict
            or type(identity.get("original_oid")) is not int
            or type(identity.get("parent_oid")) is not int
        ):
            raise HistoryCompactionError("compaction_plan_invalid")
        raw_keys = identity.get("keys", plan.get("keys"))
        if (
            type(raw_keys) is not list
            or any(type(key) is not str for key in raw_keys)
            or len(raw_keys) != len(set(raw_keys))
        ):
            raise HistoryCompactionError("compaction_plan_invalid")
        current_keys_by_table[root] = tuple(raw_keys)
        old_oid_by_table[root] = identity["original_oid"]
        parent_oid_by_table[root] = identity["parent_oid"]
    return current_keys_by_table, old_oid_by_table, parent_oid_by_table


async def select_compaction_plan(connection: Any, *, schema: str = "mrf") -> HistoryCompactionPlan:
    """Compute the exact retained and omitted dataset IDs across all 4 generic history tables."""
    plan = await _load_migration_plan(connection, schema)
    current_keys_by_table, old_oid_by_table, parent_oid_by_table = _extract_plan_metadata(plan)
    all_keys = sorted({key for keys in current_keys_by_table.values() for key in keys})
    omitted_set = await _eligible_omitted_keys(connection, schema, all_keys)
    retained_keys_by_table: dict[str, tuple[str, ...]] = {}
    for root in GENERIC_HISTORY_TABLES:
        kept_keys = [key for key in current_keys_by_table[root] if key not in omitted_set]
        retained_keys_by_table[root] = await _sorted_keys(connection, kept_keys)
    digest_by_field = {
        "schema": schema,
        "old_oids": {root: old_oid_by_table[root] for root in GENERIC_HISTORY_TABLES},
        "retained": {root: list(retained_keys_by_table[root]) for root in GENERIC_HISTORY_TABLES},
    }
    plan_sha256 = hashlib.sha256(_canonical_json(digest_by_field).encode()).hexdigest()
    return HistoryCompactionPlan(
        schema=schema,
        plan=plan,
        current_keys_by_table=current_keys_by_table,
        retained_keys_by_table=retained_keys_by_table,
        omitted_keys=tuple(sorted(omitted_set)),
        old_oid_by_table=old_oid_by_table,
        parent_oid_by_table=parent_oid_by_table,
        plan_sha256=plan_sha256,
    )


async def plan_history_compaction(connection: Any, *, schema: str = "mrf") -> dict[str, Any]:
    """Return a read-only compaction summary including current sizes and key counts."""
    selection = await select_compaction_plan(connection, schema=schema)
    report_by_table: dict[str, dict[str, Any]] = {}
    for root in GENERIC_HISTORY_TABLES:
        old_name = f"pd_dataset_history_{selection.old_oid_by_table[root]}"
        total_bytes = await connection.fetchval(
            "SELECT pg_total_relation_size(to_regclass($1))::bigint",
            _qualified(schema, old_name),
        )
        report_by_table[root] = {
            "history_relation": old_name,
            "history_oid": selection.old_oid_by_table[root],
            "total_bytes": int(total_bytes or 0),
            "current_key_count": len(selection.current_keys_by_table[root]),
            "retained_key_count": len(selection.retained_keys_by_table[root]),
        }
    return {
        "status": "planned",
        "schema": schema,
        "plan_sha256": selection.plan_sha256,
        "omitted_key_count": len(selection.omitted_keys),
        "tables": report_by_table,
    }


async def _history_fingerprint(
    connection: Any, qualified_relation: str, retained_keys: tuple[str, ...]
) -> dict[str, Any]:
    """Compute per-dataset row count and commutative hash digests for parity verification."""
    if not retained_keys:
        return {}
    metric_entries = await connection.fetch(
        f"""SELECT dataset_id,
          count(*)::bigint AS row_count,
          coalesce(sum(hashtextextended(to_jsonb(t)::text, 0))::text, '0') AS hash_sum,
          coalesce(bit_xor(hashtextextended(to_jsonb(t)::text, 1))::bigint, 0)::bigint AS hash_xor
        FROM {qualified_relation} t
        WHERE dataset_id=ANY($1::text[])
        GROUP BY dataset_id ORDER BY dataset_id""",
        list(retained_keys),
    )
    return {
        entry["dataset_id"]: {
            "row_count": int(entry["row_count"]),
            "hash_sum": entry["hash_sum"],
            "hash_xor": int(entry["hash_xor"]),
        }
        for entry in metric_entries
    }


async def _is_existing_stage_ready(
    connection: Any,
    schema: str,
    root: str,
    stage_name: str,
    selection: HistoryCompactionPlan,
) -> bool:
    """Return true when a pre-staged table matches the plan receipt and source fingerprints."""
    stage_ref = _qualified(schema, stage_name)
    stage_oid = await connection.fetchval("SELECT to_regclass($1)::oid::bigint", stage_ref)
    if stage_oid is None:
        return False
    raw_comment = await connection.fetchval("SELECT obj_description($1::oid, 'pg_class')", stage_oid)
    try:
        comment = json.loads(raw_comment) if type(raw_comment) is str else None
    except ValueError:
        return False
    if (
        type(comment) is not dict
        or comment.get("component") != _STAGE_COMPONENT
        or comment.get("root") != root
        or comment.get("plan_sha256") != selection.plan_sha256
        or comment.get("old_oid") != selection.old_oid_by_table[root]
        or comment.get("stage_oid") != stage_oid
    ):
        return False
    old_ref = f"ONLY {_qualified(schema, f'pd_dataset_history_{selection.old_oid_by_table[root]}')}"
    retained = selection.retained_keys_by_table[root]
    expected_fp = await _history_fingerprint(connection, old_ref, retained)
    actual_fp = await _history_fingerprint(connection, stage_ref, retained)
    return comment.get("fingerprints") == expected_fp and actual_fp == expected_fp


async def _copy_column_grants(connection: Any, schema: str, old_name: str, stage_ref: str, owner_role: str) -> None:
    """Replicate explicit column-level grants from the incumbent history table."""
    column_grants = await connection.fetch(
        """SELECT grantee, column_name, privilege_type, is_grantable
        FROM information_schema.column_privileges
        WHERE table_schema=$1 AND table_name=$2
          AND grantee <> $3 AND privilege_type IN ('SELECT','INSERT','UPDATE')
        ORDER BY grantee, column_name, privilege_type""",
        schema,
        old_name,
        owner_role,
    )
    for grant_entry in column_grants:
        grantee_role = "PUBLIC" if grant_entry["grantee"] == "PUBLIC" else _quote(grant_entry["grantee"])
        option_sql = " WITH GRANT OPTION" if grant_entry["is_grantable"] == "YES" else ""
        await connection.execute(
            f"GRANT {grant_entry['privilege_type']}({_quote(grant_entry['column_name'])}) ON {stage_ref} TO {grantee_role}{option_sql}"
        )


async def _copy_grants_and_owner(connection: Any, schema: str, old_name: str, stage_name: str, owner_role: str) -> None:
    """Match owner, table grants, and column grants from the incumbent history table."""
    stage_ref = _qualified(schema, stage_name)
    await connection.execute(f"ALTER TABLE {stage_ref} OWNER TO {_quote(owner_role)}")
    revoke_entries = await connection.fetch(
        "SELECT DISTINCT grantee FROM information_schema.table_privileges "
        "WHERE table_schema=$1 AND table_name=$2 AND grantee <> $3 ORDER BY grantee",
        schema,
        stage_name,
        owner_role,
    )
    await connection.execute(f"REVOKE ALL ON {stage_ref} FROM PUBLIC")
    for revoke_entry in revoke_entries:
        grantee_role = "PUBLIC" if revoke_entry["grantee"] == "PUBLIC" else _quote(revoke_entry["grantee"])
        await connection.execute(f"REVOKE ALL ON {stage_ref} FROM {grantee_role}")
    table_grants = await connection.fetch(
        """SELECT grantee, privilege_type, is_grantable
        FROM information_schema.table_privileges
        WHERE table_schema=$1 AND table_name=$2
          AND grantee <> $3 AND privilege_type IN ('SELECT','INSERT','UPDATE','DELETE')
        ORDER BY grantee, privilege_type""",
        schema,
        old_name,
        owner_role,
    )
    for grant_entry in table_grants:
        grantee_role = "PUBLIC" if grant_entry["grantee"] == "PUBLIC" else _quote(grant_entry["grantee"])
        option_sql = " WITH GRANT OPTION" if grant_entry["is_grantable"] == "YES" else ""
        await connection.execute(f"GRANT {grant_entry['privilege_type']} ON {stage_ref} TO {grantee_role}{option_sql}")
    await _copy_column_grants(connection, schema, old_name, stage_ref, owner_role)


async def _add_history_indexes(connection: Any, stage_ref: str, stage_oid: int, parent_oid: int) -> None:
    """Create parent-matching indexes and backing PK/UNIQUE constraints on the staged table."""
    index_entries = await connection.fetch(
        """SELECT i.indexrelid, i.indisunique, pg_get_indexdef(i.indexrelid) AS definition,
          k.contype::text AS contype, k.condeferrable, k.condeferred
        FROM pg_index i
        LEFT JOIN pg_constraint k ON k.conindid=i.indexrelid AND k.conrelid=i.indrelid AND k.contype IN ('p','u') AND k.conparentid=0
        WHERE i.indrelid=$1 ORDER BY i.indexrelid""",
        parent_oid,
    )
    for ordinal, index_entry in enumerate(index_entries, 1):
        idx_name = f"pd_dataset_history_idx_{stage_oid}_{ordinal}"
        _, separator, using_clause = index_entry["definition"].partition(" USING ")
        if not separator:
            raise HistoryCompactionError("compaction_index_definition_invalid")
        unique_sql = "UNIQUE " if index_entry["indisunique"] else ""
        await connection.execute(f"CREATE {unique_sql}INDEX {_quote(idx_name)} ON {stage_ref} USING {using_clause}")
        if index_entry["contype"] in ("p", "u"):
            kind_sql = "PRIMARY KEY" if index_entry["contype"] == "p" else "UNIQUE"
            defer_sql = (
                "DEFERRABLE INITIALLY " + ("DEFERRED" if index_entry["condeferred"] else "IMMEDIATE")
                if index_entry["condeferrable"]
                else "NOT DEFERRABLE"
            )
            await connection.execute(
                f"ALTER TABLE {stage_ref} ADD CONSTRAINT {_quote(idx_name)} {kind_sql} USING INDEX {_quote(idx_name)} {defer_sql}"
            )


async def _add_constraints_and_indexes(
    connection: Any,
    stage_ref: str,
    stage_oid: int,
    old_oid: int,
    parent_oid: int,
    retained_keys: tuple[str, ...],
) -> None:
    """Replicate CHECK constraints, validated partition bound, and indexes on the staged table."""
    check_entries = await connection.fetch(
        """SELECT conname, pg_get_constraintdef(oid) AS definition
        FROM pg_constraint WHERE conrelid=$1 AND contype='c' AND conname<>'pd_dataset_history_bound' ORDER BY conname""",
        old_oid,
    )
    for check_entry in check_entries:
        await connection.execute(
            f"ALTER TABLE {stage_ref} ADD CONSTRAINT {_quote(check_entry['conname'])} {check_entry['definition']}"
        )
    await connection.execute(
        f"ALTER TABLE {stage_ref} ADD CONSTRAINT pd_dataset_history_bound CHECK ({_history_bound_sql(retained_keys)}) NOT VALID"
    )
    await connection.execute(f"ALTER TABLE {stage_ref} VALIDATE CONSTRAINT pd_dataset_history_bound")
    await _add_history_indexes(connection, stage_ref, stage_oid, parent_oid)


async def _can_drop_existing_stage(connection: Any, root: str, stage_ref: str, old_oid: int) -> bool:
    """Return true when an existing stage relation carries a valid ownership receipt; fail closed otherwise."""
    stage_entry = await connection.fetchrow(
        """SELECT c.oid::bigint AS stage_oid, c.relkind::text AS relkind, c.relispartition,
          obj_description(c.oid, 'pg_class') AS raw_comment
        FROM pg_class c
        WHERE c.oid = to_regclass($1)::oid""",
        stage_ref,
    )
    if stage_entry is None:
        return False
    try:
        receipt_by_field = json.loads(stage_entry["raw_comment"]) if type(stage_entry["raw_comment"]) is str else None
    except ValueError:
        receipt_by_field = None
    plan_digest = receipt_by_field.get("plan_sha256") if type(receipt_by_field) is dict else None
    if (
        stage_entry["relkind"] != "r"
        or stage_entry["relispartition"]
        or type(receipt_by_field) is not dict
        or receipt_by_field.get("component") != _STAGE_COMPONENT
        or receipt_by_field.get("root") != root
        or receipt_by_field.get("old_oid") != old_oid
        or receipt_by_field.get("stage_oid") != stage_entry["stage_oid"]
        or type(plan_digest) is not str
        or len(plan_digest) != 64
    ):
        raise HistoryCompactionError("compaction_stage_occupied")
    return True


async def _populate_and_verify_stage(
    connection: Any,
    schema: str,
    root: str,
    stage_name: str,
    old_name: str,
    selection: HistoryCompactionPlan,
) -> int:
    """Materialize, index, analyze, and fingerprint-verify one replacement history table."""
    old_oid = selection.old_oid_by_table[root]
    parent_oid = selection.parent_oid_by_table[root]
    stage_ref = _qualified(schema, stage_name)
    old_ref = _qualified(schema, old_name)
    retained = selection.retained_keys_by_table[root]
    owner_role = await connection.fetchval(
        "SELECT tableowner FROM pg_tables WHERE schemaname=$1 AND tablename=$2", schema, root
    )
    col_entries = await connection.fetch(
        "SELECT attname FROM pg_attribute WHERE attrelid=$1 AND attnum>0 AND NOT attisdropped AND attgenerated='' ORDER BY attnum",
        old_oid,
    )
    copy_cols = ",".join(_quote(col_entry["attname"]) for col_entry in col_entries)
    async with connection.transaction():
        if await _can_drop_existing_stage(connection, root, stage_ref, old_oid):
            await connection.execute(f"DROP TABLE {stage_ref} RESTRICT")
        await connection.execute(
            f"CREATE TABLE {stage_ref} (LIKE {old_ref} INCLUDING DEFAULTS INCLUDING GENERATED INCLUDING STORAGE)"
        )
        stage_oid = int(await connection.fetchval("SELECT to_regclass($1)::oid::bigint", stage_ref))
        await _copy_grants_and_owner(connection, schema, old_name, stage_name, owner_role)
        if retained:
            await connection.execute(
                f"INSERT INTO {stage_ref} ({copy_cols}) SELECT {copy_cols} FROM ONLY {old_ref} WHERE dataset_id=ANY($1::text[])",
                list(retained),
            )
        await _add_constraints_and_indexes(connection, stage_ref, stage_oid, old_oid, parent_oid, retained)
        await connection.execute(f"ANALYZE {stage_ref}")
        expected_fp = await _history_fingerprint(connection, f"ONLY {old_ref}", retained)
        if await _history_fingerprint(connection, stage_ref, retained) != expected_fp:
            raise HistoryCompactionError("compaction_stage_parity_failed")
        receipt_by_field = {
            "component": _STAGE_COMPONENT,
            "root": root,
            "plan_sha256": selection.plan_sha256,
            "old_oid": old_oid,
            "stage_oid": stage_oid,
            "fingerprints": expected_fp,
        }
        await connection.execute(f"COMMENT ON TABLE {stage_ref} IS {_literal(_canonical_json(receipt_by_field))}")
    return stage_oid


async def _prepare_one_table(
    connection: Any, schema: str, root: str, selection: HistoryCompactionPlan
) -> dict[str, Any]:
    """Reuse an already verified staged heap or materialize a fresh one."""
    stage_name = _stage_name(root, selection.plan_sha256)
    old_name = f"pd_dataset_history_{selection.old_oid_by_table[root]}"
    if await _is_existing_stage_ready(connection, schema, root, stage_name, selection):
        stage_oid = await connection.fetchval("SELECT to_regclass($1)::oid::bigint", _qualified(schema, stage_name))
        return {"stage_relation": stage_name, "stage_oid": int(stage_oid), "reused": True}
    stage_oid = await _populate_and_verify_stage(connection, schema, root, stage_name, old_name, selection)
    return {"stage_relation": stage_name, "stage_oid": stage_oid, "reused": False}


async def _verify_stage_relationships(
    connection: Any, schema: str, selection: HistoryCompactionPlan, stage_by_table: dict[str, str]
) -> None:
    """Confirm staged partitions satisfy all plan-declared cross-table relationships."""
    relationships = selection.plan.get("relationships", [])
    for rel_entry in relationships:
        src = rel_entry["source_table"]
        dst = rel_entry["target_table"]
        if src not in GENERIC_HISTORY_TABLES:
            continue
        dst_schema = rel_entry["target_schema"]
        src_ref = _qualified(schema, stage_by_table[src])
        dst_ref = _qualified(schema, stage_by_table[dst]) if dst in stage_by_table else _qualified(dst_schema, dst)
        src_cols = rel_entry["source_columns"]
        dst_cols = rel_entry["target_columns"]
        join_pred = " AND ".join(f"s.{_quote(sc)}=t.{_quote(dc)}" for sc, dc in zip(src_cols, dst_cols, strict=True))
        nonnull_pred = " AND ".join(f"s.{_quote(sc)} IS NOT NULL" for sc in src_cols)
        broken = await connection.fetchval(
            f"SELECT EXISTS(SELECT FROM {src_ref} s WHERE {nonnull_pred} AND NOT EXISTS(SELECT FROM {dst_ref} t WHERE {join_pred}))"
        )
        if broken:
            raise HistoryCompactionError("compaction_relationship_parity_failed")


async def prepare_history_compaction(connection: Any, *, schema: str = "mrf") -> dict[str, Any]:
    """Materialize, index, analyze, and verify replacement history partitions online."""
    lock_key = _COMPACTION_LOCK_PREFIX + schema
    locked = await connection.fetchval("SELECT pg_try_advisory_lock(hashtextextended($1, 0))", lock_key)
    if not locked:
        raise HistoryCompactionError("compaction_already_running")
    try:
        selection = await select_compaction_plan(connection, schema=schema)
        report_by_table: dict[str, dict[str, Any]] = {}
        stage_by_table: dict[str, str] = {}
        for root in GENERIC_HISTORY_TABLES:
            stage_info = await _prepare_one_table(connection, schema, root, selection)
            report_by_table[root] = stage_info
            stage_by_table[root] = stage_info["stage_relation"]
        await _verify_stage_relationships(connection, schema, selection, stage_by_table)
        return {
            "status": "prepared",
            "schema": schema,
            "plan_sha256": selection.plan_sha256,
            "omitted_key_count": len(selection.omitted_keys),
            "tables": report_by_table,
        }
    finally:
        await connection.execute("SELECT pg_advisory_unlock(hashtextextended($1, 0))", lock_key)


def _cli_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Online compaction for partitioned provider directory history heaps.")
    parser.add_argument("action", choices=("plan", "prepare", "cutover", "rollback", "drop-retired"))
    parser.add_argument("--schema", default="mrf")
    return parser


async def _run_cli(action: str, schema: str) -> dict[str, Any]:
    from db.connection import db
    from process.provider_directory_history_compaction_cutover import (
        cutover_history_compaction,
        drop_retired_history,
        rollback_history_compaction,
    )

    await db.connect()
    try:
        async with db.acquire_driver() as connection:
            if action == "plan":
                return await plan_history_compaction(connection, schema=schema)
            if action == "prepare":
                return await prepare_history_compaction(connection, schema=schema)
            if action == "cutover":
                return await cutover_history_compaction(connection, schema=schema)
            if action == "rollback":
                return await rollback_history_compaction(connection, schema=schema)
            return await drop_retired_history(connection, schema=schema)
    finally:
        await db.disconnect()


def main(argv: list[str] | None = None) -> None:
    """Execute one explicit history compaction operator action."""
    args = _cli_parser().parse_args(argv)
    print(_canonical_json(asyncio.run(_run_cli(args.action, args.schema))))


if __name__ == "__main__":
    main()
