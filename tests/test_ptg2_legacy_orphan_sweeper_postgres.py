# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Disposable PostgreSQL proof for the legacy PTG orphan sweeper."""

from __future__ import annotations

import asyncio
import json

import pytest
from sqlalchemy.exc import DBAPIError

from process.ptg_parts import ptg2_legacy_orphan_sweeper as legacy_sweeper
from process.ptg_parts.ptg2_legacy_orphan_contract import (
    canonical_sha256,
    legacy_sweep_audit_id,
)
from process.ptg_parts.ptg2_legacy_orphan_sweeper import (
    build_legacy_orphan_sweep_plan,
    execute_legacy_orphan_sweep,
)
from tests.test_ptg2_legacy_orphan_postgres_support import (
    FOREIGN_SUFFIX,
    LIMITS,
    SUFFIX_BUILDING,
    SUFFIX_DRIFT,
    SUFFIX_EMPTY,
    SUFFIX_EXTERNAL_DEPENDENCY,
    SUFFIX_FOREIGN_FENCE,
    SUFFIX_FOREIGN_OWNER,
    SUFFIX_LOCKED,
    SUFFIX_OWNED,
    SUFFIX_SERVING_RESIDUE,
    _PostgresCase,
    _create_root,
    _has_relation,
    _prepared_case,
    _q,
    _run_migration,
    _seed_candidates,
    _seed_terminal_owner,
)


async def _initial_sweep_plan(case: _PostgresCase):
    async with case.database.acquire() as connection:
        await _seed_candidates(
            connection,
            case.mrf_schema,
            case.control_schema,
        )
    plan = await build_legacy_orphan_sweep_plan(
        schema_name=case.mrf_schema,
        control_schema_name=case.control_schema,
        limits=LIMITS,
        executor=case.database,
    )
    assert [candidate.suffix for candidate in plan.candidates] == [
        SUFFIX_EMPTY,
        SUFFIX_OWNED,
    ]
    reasons_by_suffix = {
        blocked.suffix: blocked.reasons for blocked in plan.blocked
    }
    assert reasons_by_suffix[SUFFIX_BUILDING] == (
        "internal_run_status_building",
        "snapshot_status_building",
    )
    return plan


async def _assert_failed_apply_is_atomic(
    case: _PostgresCase,
    monkeypatch,
    plan,
) -> None:
    async def fail_after_first_drop(
        executor,
        *,
        schema_name,
        relations,
    ) -> None:
        first_relation = sorted(
            relations,
            key=lambda relation: relation.table_name,
        )[0]
        await executor.status(
            f"DROP TABLE {_q(schema_name)}.{_q(first_relation.table_name)}"
        )
        raise RuntimeError("forced_mid_sweep_failure")

    with monkeypatch.context() as scoped_patch:
        scoped_patch.setattr(
            legacy_sweeper,
            "drop_legacy_root_relations",
            fail_after_first_drop,
        )
        with pytest.raises(RuntimeError, match="forced_mid_sweep_failure"):
            await execute_legacy_orphan_sweep(
                expected_plan_digest=plan.plan_digest,
                actor="postgres-test",
                schema_name=case.mrf_schema,
                control_schema_name=case.control_schema,
                limits=LIMITS,
                database=case.database,
            )
    async with case.database.acquire() as connection:
        for suffix in (SUFFIX_EMPTY, SUFFIX_OWNED):
            assert await _has_relation(
                connection,
                case.mrf_schema,
                f"ptg_file_{suffix}",
            )
        assert await connection.scalar(
            f"SELECT COUNT(*) FROM {_q(case.mrf_schema)}."
            "ptg2_legacy_orphan_sweep_audit"
        ) == 0
        assert await connection.scalar(
            f"SELECT COUNT(*) FROM {_q(case.mrf_schema)}.ptg2_snapshot "
            "WHERE snapshot_id = 'snapshot-owned'"
        ) == 1


async def _assert_apply_and_replay(case: _PostgresCase, plan) -> None:
    apply_parameters_by_name = {
        "expected_plan_digest": plan.plan_digest,
        "actor": "postgres-test",
        "schema_name": case.mrf_schema,
        "control_schema_name": case.control_schema,
        "limits": LIMITS,
        "database": case.database,
    }
    applied = await execute_legacy_orphan_sweep(**apply_parameters_by_name)
    assert applied.state == "applied"
    replayed = await execute_legacy_orphan_sweep(**apply_parameters_by_name)
    assert replayed.state == "already_applied"
    async with case.database.acquire() as connection:
        for suffix in (SUFFIX_EMPTY, SUFFIX_OWNED):
            assert not await _has_relation(
                connection,
                case.mrf_schema,
                f"ptg_file_{suffix}",
            )
        assert await _has_relation(
            connection,
            case.mrf_schema,
            f"ptg_file_{SUFFIX_BUILDING}",
        )
        assert await connection.scalar(
            f"SELECT COUNT(*) FROM {_q(case.mrf_schema)}."
            "ptg2_legacy_orphan_sweep_audit"
        ) == 1
        assert await connection.scalar(
            f"SELECT COUNT(*) FROM {_q(case.mrf_schema)}.ptg2_snapshot "
            "WHERE snapshot_id = 'snapshot-owned'"
        ) == 0


async def _expect_plan_drift(case: _PostgresCase, plan) -> None:
    with pytest.raises(RuntimeError, match="legacy_sweep_plan_digest_changed"):
        await execute_legacy_orphan_sweep(
            expected_plan_digest=plan.plan_digest,
            actor="postgres-test",
            schema_name=case.mrf_schema,
            control_schema_name=case.control_schema,
            limits=LIMITS,
            database=case.database,
        )


async def _assert_catalog_and_data_drift(case: _PostgresCase) -> None:
    async with case.database.acquire() as connection:
        drift_table = await _create_root(
            connection,
            case.mrf_schema,
            SUFFIX_DRIFT,
            populated=False,
        )
    authority_plan = await build_legacy_orphan_sweep_plan(
        schema_name=case.mrf_schema,
        control_schema_name=case.control_schema,
        limits=LIMITS,
        executor=case.database,
    )
    async with case.database.acquire() as connection:
        await connection.status(
            f"DROP TABLE {_q(case.mrf_schema)}.ptg2_plan_month"
        )
        await connection.status(
            f"CREATE TABLE {_q(case.mrf_schema)}.ptg2_plan_month "
            "(snapshot_id text)"
        )
    await _expect_plan_drift(case, authority_plan)
    drift_plan = await build_legacy_orphan_sweep_plan(
        schema_name=case.mrf_schema,
        control_schema_name=case.control_schema,
        limits=LIMITS,
        executor=case.database,
    )
    async with case.database.acquire() as connection:
        await connection.status(
            f"INSERT INTO {_q(case.mrf_schema)}.{_q(drift_table)} VALUES (1)"
        )
    await _expect_plan_drift(case, drift_plan)
    async with case.database.acquire() as connection:
        assert await _has_relation(connection, case.mrf_schema, drift_table)


async def _assert_audit_is_immutable(case: _PostgresCase) -> None:
    with pytest.raises(DBAPIError, match="PTG2_LEGACY_SWEEP_AUDIT_IMMUTABLE"):
        async with case.database.acquire() as connection:
            await connection.status(
                f"DELETE FROM {_q(case.mrf_schema)}."
                "ptg2_legacy_orphan_sweep_audit"
            )
    with pytest.raises(DBAPIError, match="PTG2_LEGACY_SWEEP_AUDIT_IMMUTABLE"):
        async with case.database.acquire() as connection:
            await connection.status("SET LOCAL session_replication_role = replica")
            await connection.status(
                f"UPDATE {_q(case.mrf_schema)}."
                "ptg2_legacy_orphan_sweep_audit SET actor = 'tampered'"
            )
    for replica_role in (False, True):
        with pytest.raises(
            DBAPIError,
            match="PTG2_LEGACY_SWEEP_AUDIT_IMMUTABLE",
        ):
            async with case.database.acquire() as connection:
                if replica_role:
                    await connection.status(
                        "SET LOCAL session_replication_role = replica"
                    )
                await connection.status(
                    f"TRUNCATE {_q(case.mrf_schema)}."
                    "ptg2_legacy_orphan_sweep_audit"
                )
    with pytest.raises(
        DBAPIError,
        match="PTG2_LEGACY_SWEEP_AUDIT_DOWNGRADE_REFUSED",
    ):
        await _run_migration(case.database, case.migration, "downgrade")


def _forged_proof_by_field(case: _PostgresCase) -> dict[str, object]:
    return {
        "contract": "ptg2_legacy_orphan_sweep_v1",
        "schema_name": case.mrf_schema,
        "control_schema_name": case.control_schema,
        "authority_digest": "b" * 64,
        "catalog_digest": "c" * 64,
        "candidates": [
            {
                "suffix": FOREIGN_SUFFIX,
                "proof_kind": "empty_orphan",
                "relations": [
                    {
                        "table_name": f"ptg_file_{FOREIGN_SUFFIX}",
                        "relation_oid": 999_999_999,
                        "dependent_relation_oids": [],
                        "total_bytes": 0,
                        "has_rows": False,
                    }
                ],
                "ownership": {
                    "snapshot_statuses": [],
                    "internal_run_statuses": [],
                },
            }
        ],
    }


async def _assert_forged_replay_is_rejected(case: _PostgresCase) -> None:
    proof_by_field = _forged_proof_by_field(case)
    forged_digest = canonical_sha256(proof_by_field)
    async with case.database.acquire() as connection:
        await connection.status(
            f"""
            INSERT INTO {_q(case.mrf_schema)}.
                {_q('ptg2_legacy_orphan_sweep_audit')} (
                audit_id, contract, actor, plan_digest,
                authority_digest, catalog_digest,
                candidate_suffix_count, root_table_count,
                dependent_relation_count, snapshot_count,
                nonempty_table_count, total_bytes,
                root_relation_oids, snapshot_ids, proof
            ) VALUES (
                :audit_id, 'ptg2_legacy_orphan_sweep_v1', 'forged-test',
                decode(:plan_digest, 'hex'),
                decode(repeat('bb', 32), 'hex'),
                decode(repeat('cc', 32), 'hex'),
                2, 1, 0, 0, 0, 0,
                ARRAY[999999999]::bigint[], ARRAY[]::text[],
                CAST(:proof AS jsonb)
            )
            """,
            audit_id=legacy_sweep_audit_id(forged_digest),
            plan_digest=forged_digest,
            proof=json.dumps(proof_by_field),
        )
    with pytest.raises(RuntimeError, match="legacy_sweep_replay_audit_invalid"):
        await execute_legacy_orphan_sweep(
            expected_plan_digest=forged_digest,
            actor="postgres-test",
            schema_name=case.mrf_schema,
            control_schema_name=case.control_schema,
            limits=LIMITS,
            database=case.database,
        )


@pytest.mark.asyncio
async def test_legacy_sweep_applies_replays_and_fails_closed_on_drift(
    monkeypatch,
) -> None:
    async with _prepared_case(monkeypatch) as case:
        plan = await _initial_sweep_plan(case)
        await _assert_failed_apply_is_atomic(case, monkeypatch, plan)
        await _assert_apply_and_replay(case, plan)
        await _assert_catalog_and_data_drift(case)
        await _assert_audit_is_immutable(case)
        await _assert_forged_replay_is_rejected(case)


@pytest.mark.asyncio
async def test_legacy_sweep_lock_timeout_leaves_state_unchanged(
    monkeypatch,
) -> None:
    async with _prepared_case(monkeypatch) as case:
        async with case.database.acquire() as connection:
            table_name = await _create_root(
                connection,
                case.mrf_schema,
                SUFFIX_LOCKED,
                populated=False,
            )
        plan = await build_legacy_orphan_sweep_plan(
            schema_name=case.mrf_schema,
            control_schema_name=case.control_schema,
            limits=LIMITS,
            executor=case.database,
        )
        async with case.database.acquire() as blocker:
            await blocker.status(
                f"LOCK TABLE {_q(case.mrf_schema)}.{_q(table_name)} "
                "IN ACCESS EXCLUSIVE MODE"
            )
            with pytest.raises(DBAPIError):
                await asyncio.wait_for(
                    execute_legacy_orphan_sweep(
                        expected_plan_digest=plan.plan_digest,
                        actor="postgres-test",
                        schema_name=case.mrf_schema,
                        control_schema_name=case.control_schema,
                        limits=LIMITS,
                        lock_timeout="50ms",
                        database=case.database,
                    ),
                    timeout=5,
                )
        async with case.database.acquire() as connection:
            assert await _has_relation(
                connection,
                case.mrf_schema,
                table_name,
            )
            assert await connection.scalar(
                f"SELECT COUNT(*) FROM {_q(case.mrf_schema)}."
                "ptg2_legacy_orphan_sweep_audit"
            ) == 0


async def _seed_blocker_roots(
    case: _PostgresCase,
) -> tuple[str, str, str]:
    async with case.database.acquire() as connection:
        foreign_owner_snapshot = await _seed_terminal_owner(
            connection,
            case.mrf_schema,
            case.control_schema,
            SUFFIX_FOREIGN_OWNER,
        )
        foreign_fence_snapshot = await _seed_terminal_owner(
            connection,
            case.mrf_schema,
            case.control_schema,
            SUFFIX_FOREIGN_FENCE,
        )
        residue_snapshot = await _seed_terminal_owner(
            connection,
            case.mrf_schema,
            case.control_schema,
            SUFFIX_SERVING_RESIDUE,
        )
        await _create_root(
            connection,
            case.mrf_schema,
            SUFFIX_EXTERNAL_DEPENDENCY,
            populated=False,
        )
    return foreign_owner_snapshot, foreign_fence_snapshot, residue_snapshot


async def _insert_blocker_evidence(
    case: _PostgresCase,
    *,
    foreign_owner_snapshot: str,
    foreign_fence_snapshot: str,
    residue_snapshot: str,
) -> None:
    async with case.database.acquire() as connection:
        await connection.status(
            f"""
            INSERT INTO {_q(case.control_schema)}.ptg_file_placement (
                placement_id, source_file_import_id, status, snapshot_id
            ) VALUES (
                'foreign-owner', :foreign_suffix, 'inactive', :snapshot_id
            )
            """,
            foreign_suffix=FOREIGN_SUFFIX,
            snapshot_id=foreign_owner_snapshot,
        )
        await connection.status(
            f"""
            INSERT INTO {_q(case.mrf_schema)}.ptg2_v4_attempt_fence (
                snapshot_id, internal_run_id, state
            ) VALUES (:snapshot_id, :foreign_run_id, 'reconciled')
            """,
            snapshot_id=foreign_fence_snapshot,
            foreign_run_id=f"ptg2:{FOREIGN_SUFFIX}",
        )
        await connection.status(
            f"INSERT INTO {_q(case.mrf_schema)}.ptg2_serving_rate "
            "(snapshot_id) VALUES (:snapshot_id)",
            snapshot_id=residue_snapshot,
        )
        await connection.status(
            f"CREATE VIEW {_q(case.mrf_schema)}.legacy_dependency_view AS "
            f"SELECT * FROM {_q(case.mrf_schema)}."
            f"{_q(f'ptg_file_{SUFFIX_EXTERNAL_DEPENDENCY}')}"
        )


def _assert_blocker_reasons(plan) -> None:
    reasons_by_suffix = {
        blocked.suffix: blocked.reasons for blocked in plan.blocked
    }
    assert (
        "snapshot_reverse_owner_conflict_file_placement"
        in reasons_by_suffix[SUFFIX_FOREIGN_OWNER]
    )
    assert (
        "attempt_fence_owner_conflict"
        in reasons_by_suffix[SUFFIX_FOREIGN_FENCE]
    )
    assert (
        "attempt_fence_present"
        in reasons_by_suffix[SUFFIX_FOREIGN_FENCE]
    )
    assert (
        "serving_or_lifecycle_reference"
        in reasons_by_suffix[SUFFIX_SERVING_RESIDUE]
    )
    assert (
        "external_relation_dependency"
        in reasons_by_suffix[SUFFIX_EXTERNAL_DEPENDENCY]
    )
    assert plan.candidates == ()


@pytest.mark.asyncio
async def test_legacy_sweep_blocks_cross_owner_and_serving_evidence(
    monkeypatch,
) -> None:
    async with _prepared_case(monkeypatch) as case:
        snapshots = await _seed_blocker_roots(case)
        await _insert_blocker_evidence(
            case,
            foreign_owner_snapshot=snapshots[0],
            foreign_fence_snapshot=snapshots[1],
            residue_snapshot=snapshots[2],
        )
        plan = await build_legacy_orphan_sweep_plan(
            schema_name=case.mrf_schema,
            control_schema_name=case.control_schema,
            limits=LIMITS,
            executor=case.database,
        )
        _assert_blocker_reasons(plan)
