# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PostgreSQL safety-boundary proof for the legacy orphan sweeper."""

from __future__ import annotations

import asyncio

import pytest
from sqlalchemy.exc import DBAPIError

from process.ptg_parts.ptg2_legacy_orphan_sweeper import (
    build_legacy_orphan_sweep_plan,
    execute_legacy_orphan_sweep,
)
from tests.test_ptg2_legacy_orphan_postgres_support import (
    LIMITS,
    _PostgresCase,
    _create_root,
    _prepared_case,
    _q,
    _run_migration,
)


async def _plan(case: _PostgresCase):
    return await build_legacy_orphan_sweep_plan(
        schema_name=case.mrf_schema,
        control_schema_name=case.control_schema,
        limits=LIMITS,
        executor=case.database,
    )


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


async def _insert_minimal_audit(connection, schema_name: str) -> None:
    await connection.status(
        f"""
        INSERT INTO {_q(schema_name)}.ptg2_legacy_orphan_sweep_audit (
            audit_id, contract, actor, plan_digest,
            authority_digest, catalog_digest,
            candidate_suffix_count, root_table_count,
            dependent_relation_count, snapshot_count,
            nonempty_table_count, total_bytes,
            root_relation_oids, snapshot_ids, proof
        ) VALUES (
            repeat('a', 64), 'ptg2_legacy_orphan_sweep_v1',
            'downgrade-race-test', decode(repeat('aa', 32), 'hex'),
            decode(repeat('bb', 32), 'hex'),
            decode(repeat('cc', 32), 'hex'),
            1, 1, 0, 0, 0, 0,
            ARRAY[1]::bigint[], ARRAY[]::text[],
            '{{"contract":"ptg2_legacy_orphan_sweep_v1"}}'::jsonb
        )
        """
    )


@pytest.mark.asyncio
async def test_audit_downgrade_locks_before_empty_check(monkeypatch) -> None:
    async with _prepared_case(monkeypatch) as case:
        async with case.database.acquire() as blocker:
            await blocker.status(
                f"LOCK TABLE {_q(case.mrf_schema)}."
                "ptg2_legacy_orphan_sweep_audit IN ROW EXCLUSIVE MODE"
            )
            downgrade_task = asyncio.create_task(
                _run_migration(case.database, case.migration, "downgrade")
            )
            await asyncio.sleep(0.1)
            assert not downgrade_task.done()
            await _insert_minimal_audit(blocker, case.mrf_schema)
        with pytest.raises(
            DBAPIError,
            match="PTG2_LEGACY_SWEEP_AUDIT_DOWNGRADE_REFUSED",
        ):
            await downgrade_task


async def _seed_raw_snapshot_mismatches(
    case: _PostgresCase, *, manifest_suffix: str, declared_suffix: str,
    raw_foreign_suffix: str,
) -> None:
    """Seed conflicting raw and declared snapshot ownership identities."""

    async with case.database.acquire() as connection:
        for suffix in (manifest_suffix, declared_suffix):
            await _create_root(
                connection,
                case.mrf_schema,
                suffix,
                populated=False,
            )
        await connection.status(
            f"""
            INSERT INTO {_q(case.mrf_schema)}.ptg2_snapshot (
                snapshot_id, import_run_id, status, manifest
            ) VALUES
                (
                    'snapshot-manifest-mismatch',
                    'ptg2:{raw_foreign_suffix}',
                    'failed',
                    jsonb_build_object(
                        'legacy_table_suffix',
                        '{manifest_suffix}'
                    )
                ),
                (
                    'snapshot-declared-mismatch',
                    'ptg2:{raw_foreign_suffix}',
                    'failed',
                    '{{}}'::jsonb
                )
            """
        )
        await connection.status(
            f"""
            INSERT INTO {_q(case.mrf_schema)}.import_run (
                run_id, source_file_import_id, status, snapshot_id
            ) VALUES (
                'mirror-declared-mismatch',
                '{declared_suffix}',
                'failed',
                'snapshot-declared-mismatch'
            )
            """
        )
        await connection.status(
            f"""
            INSERT INTO {_q(case.control_schema)}.source_file_import (
                source_file_import_id, status, snapshot_id
            ) VALUES (
                '{declared_suffix}',
                'failed',
                'snapshot-declared-mismatch'
            )
            """
        )


@pytest.mark.asyncio
async def test_raw_snapshot_owner_mismatch_blocks_candidate_roots(
    monkeypatch,
) -> None:
    manifest_suffix = "b" * 32
    declared_suffix = "c" * 32
    async with _prepared_case(monkeypatch) as case:
        await _seed_raw_snapshot_mismatches(
            case,
            manifest_suffix=manifest_suffix,
            declared_suffix=declared_suffix,
            raw_foreign_suffix="d" * 32,
        )
        plan = await _plan(case)
        reasons_by_suffix = {
            blocked.suffix: blocked.reasons for blocked in plan.blocked
        }

        assert "snapshot_owner_suffix_conflict" in reasons_by_suffix[
            manifest_suffix
        ]
        assert "declared_snapshot_raw_owner_conflict" in reasons_by_suffix[
            declared_suffix
        ]
        assert plan.candidates == ()


async def _install_root_trigger(
    case: _PostgresCase,
    trigger_table: str,
) -> None:
    async with case.database.acquire() as connection:
        await connection.status(
            f"""
            CREATE FUNCTION {_q(case.mrf_schema)}.legacy_trigger_guard()
            RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                RETURN NEW;
            END;
            $$
            """
        )
        await connection.status(
            f"CREATE TRIGGER legacy_root_guard BEFORE INSERT ON "
            f"{_q(case.mrf_schema)}.{_q(trigger_table)} FOR EACH ROW "
            f"EXECUTE FUNCTION {_q(case.mrf_schema)}.legacy_trigger_guard()"
        )


async def _disable_root_trigger(
    case: _PostgresCase,
    trigger_table: str,
) -> None:
    async with case.database.acquire() as connection:
        await connection.status(
            f"ALTER TABLE {_q(case.mrf_schema)}.{_q(trigger_table)} "
            "DISABLE TRIGGER legacy_root_guard"
        )


async def _drop_root_trigger(
    case: _PostgresCase,
    trigger_table: str,
) -> None:
    async with case.database.acquire() as connection:
        await connection.status(
            f"DROP TRIGGER legacy_root_guard ON "
            f"{_q(case.mrf_schema)}.{_q(trigger_table)}"
        )


async def _install_root_policy(
    case: _PostgresCase,
    policy_table: str,
) -> None:
    async with case.database.acquire() as connection:
        await connection.status(
            f"ALTER TABLE {_q(case.mrf_schema)}.{_q(policy_table)} "
            "ENABLE ROW LEVEL SECURITY"
        )
        await connection.status(
            f"CREATE POLICY legacy_policy ON "
            f"{_q(case.mrf_schema)}.{_q(policy_table)} USING (true)"
        )


async def _create_root_rule(case: _PostgresCase, table_name: str) -> None:
    async with case.database.acquire() as connection:
        await connection.status(
            f"CREATE RULE legacy_root_rule AS ON INSERT TO "
            f"{_q(case.mrf_schema)}.{_q(table_name)} DO ALSO SELECT 1"
        )


async def _disable_root_rule(case: _PostgresCase, table_name: str) -> None:
    async with case.database.acquire() as connection:
        await connection.status(
            f"ALTER TABLE {_q(case.mrf_schema)}.{_q(table_name)} "
            "DISABLE RULE legacy_root_rule"
        )


async def _drop_root_rule(case: _PostgresCase, table_name: str) -> None:
    async with case.database.acquire() as connection:
        await connection.status(
            f"DROP RULE legacy_root_rule ON "
            f"{_q(case.mrf_schema)}.{_q(table_name)}"
        )


@pytest.mark.asyncio
async def test_root_row_type_consumers_are_external_dependencies(
    monkeypatch,
) -> None:
    row_type_suffix = "0" * 32
    async with _prepared_case(monkeypatch) as case:
        async with case.database.acquire() as connection:
            root_table = await _create_root(
                connection,
                case.mrf_schema,
                row_type_suffix,
                populated=False,
            )
        candidate_plan = await _plan(case)
        async with case.database.acquire() as connection:
            await connection.status(
                f"CREATE TABLE {_q(case.control_schema)}.legacy_row_consumer "
                f"(root_value {_q(case.mrf_schema)}.{_q(root_table)})"
            )
        await _expect_plan_drift(case, candidate_plan)
        blocked_plan = await _plan(case)
        reasons_by_suffix = {
            blocked.suffix: blocked.reasons for blocked in blocked_plan.blocked
        }
        assert reasons_by_suffix[row_type_suffix] == (
            "external_relation_dependency",
        )


@pytest.mark.asyncio
async def test_root_rules_are_digest_bound_across_add_alter_and_remove(
    monkeypatch,
) -> None:
    rule_suffix = "d" * 32
    async with _prepared_case(monkeypatch) as case:
        async with case.database.acquire() as connection:
            rule_table = await _create_root(
                connection,
                case.mrf_schema,
                rule_suffix,
                populated=False,
            )
        base_plan = await _plan(case)
        await _create_root_rule(case, rule_table)
        await _expect_plan_drift(case, base_plan)
        enabled_plan = await _plan(case)
        await _disable_root_rule(case, rule_table)
        await _expect_plan_drift(case, enabled_plan)
        disabled_plan = await _plan(case)
        await _drop_root_rule(case, rule_table)
        await _expect_plan_drift(case, disabled_plan)
        removed_plan = await _plan(case)
        assert rule_suffix in {
            candidate.suffix for candidate in removed_plan.candidates
        }


@pytest.mark.asyncio
async def test_root_triggers_are_digest_bound_and_other_dependencies_block(
    monkeypatch,
) -> None:
    trigger_suffix = "e" * 32
    policy_suffix = "f" * 32
    async with _prepared_case(monkeypatch) as case:
        async with case.database.acquire() as connection:
            trigger_table = await _create_root(
                connection, case.mrf_schema, trigger_suffix, populated=False
            )
            policy_table = await _create_root(
                connection, case.mrf_schema, policy_suffix, populated=False
            )
        base_plan = await _plan(case)
        await _install_root_trigger(case, trigger_table)
        await _expect_plan_drift(case, base_plan)
        trigger_plan = await _plan(case)
        await _disable_root_trigger(case, trigger_table)
        disabled_plan = await _plan(case)
        assert disabled_plan.catalog_digest != trigger_plan.catalog_digest
        await _drop_root_trigger(case, trigger_table)
        removed_plan = await _plan(case)
        assert removed_plan.catalog_digest != disabled_plan.catalog_digest
        await _install_root_policy(case, policy_table)
        policy_plan = await _plan(case)
        reasons_by_suffix = {
            blocked.suffix: blocked.reasons
            for blocked in policy_plan.blocked
        }
        assert reasons_by_suffix[policy_suffix] == (
            "external_relation_dependency",
        )
        assert trigger_suffix in {
            candidate.suffix for candidate in policy_plan.candidates
        }
