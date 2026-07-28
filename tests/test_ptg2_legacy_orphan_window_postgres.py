# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PostgreSQL scale proof for bounded legacy catalog windows."""

from __future__ import annotations

import pytest

from process.ptg_parts import ptg2_legacy_orphan_catalog_scan as catalog_scan
from process.ptg_parts import ptg2_legacy_orphan_store_window as legacy_window
from process.ptg_parts import ptg2_legacy_orphan_sweeper as legacy_sweeper
from process.ptg_parts.ptg2_legacy_orphan_contract import (
    LegacySweepLimits,
    canonical_sha256,
    legacy_sweep_audit_id,
)
from process.ptg_parts.ptg2_legacy_orphan_store_mutation import (
    LegacySweepAuditRecord,
    insert_legacy_sweep_audit,
)
from process.ptg_parts.ptg2_legacy_orphan_sweeper import (
    build_legacy_orphan_sweep_plan,
    execute_legacy_orphan_sweep,
)
from tests.test_ptg2_legacy_orphan_postgres_support import (
    _create_root,
    _has_relation,
    _prepared_case,
    _q,
)


def _pre_window_audit(plan) -> LegacySweepAuditRecord:
    proof_by_field = dict(plan.audit_payload())
    proof_by_field.pop("catalog_suffix_count")
    proof_by_field.pop("scanned_suffix_count")
    proof_by_field.pop("limits")
    plan_digest = canonical_sha256(proof_by_field)
    root_oids = sorted(
        relation.relation_oid
        for candidate in plan.candidates
        for relation in candidate.relations
    )
    return LegacySweepAuditRecord(
        audit_id=legacy_sweep_audit_id(plan_digest),
        actor="catalog-window-test",
        plan_digest=plan_digest,
        authority_digest=plan.authority_digest,
        catalog_digest=plan.catalog_digest,
        candidate_suffix_count=len(plan.candidates),
        root_table_count=plan.table_count,
        dependent_relation_count=plan.relation_count - plan.table_count,
        snapshot_count=0,
        nonempty_table_count=0,
        total_bytes=plan.total_bytes,
        root_relation_oids=root_oids,
        snapshot_ids=[],
        proof=proof_by_field,
    )


async def _root_presence(case, suffixes: tuple[str, ...]) -> tuple[bool, ...]:
    async with case.database.acquire() as connection:
        return tuple(
            [
                await _has_relation(
                    connection,
                    case.mrf_schema,
                    f"ptg_file_{suffix}",
                )
                for suffix in suffixes
            ]
        )


def _install_post_lock_drift(monkeypatch, drift_suffix: str) -> None:
    original_lock = legacy_sweeper.lock_legacy_root_relations

    async def lock_then_drift(executor, **parameters) -> None:
        await original_lock(executor, **parameters)
        await _create_root(
            executor,
            parameters["schema_name"],
            drift_suffix,
            populated=False,
        )

    monkeypatch.setattr(
        legacy_sweeper,
        "lock_legacy_root_relations",
        lock_then_drift,
    )


@pytest.mark.asyncio
async def test_catalog_windows_bound_relation_discovery_per_suffix(
    monkeypatch,
) -> None:
    """Keep a large aggregate catalog operable through bounded windows."""

    suffixes = ("a" * 32, "b" * 32)
    async with _prepared_case(monkeypatch) as case:
        async with case.database.acquire() as connection:
            for suffix in suffixes:
                await _create_root(
                    connection,
                    case.mrf_schema,
                    suffix,
                    populated=False,
                )
        monkeypatch.setattr(
            legacy_window,
            "LEGACY_SWEEP_CATALOG_WINDOW_SUFFIXES",
            1,
        )
        monkeypatch.setattr(
            legacy_window,
            "LEGACY_SWEEP_MAX_RELATIONS",
            3,
        )

        plan = await build_legacy_orphan_sweep_plan(
            schema_name=case.mrf_schema,
            control_schema_name=case.control_schema,
            limits=LegacySweepLimits(2, 10, 10, 1_000_000),
            executor=case.database,
        )

    assert [candidate.suffix for candidate in plan.candidates] == list(
        suffixes
    )
    assert plan.catalog_suffix_count == 2
    assert plan.scanned_suffix_count == 2
    assert plan.unscanned_suffix_count == 0


@pytest.mark.asyncio
async def test_dense_suffix_is_blocked_without_starving_later_candidate(
    monkeypatch,
) -> None:
    """Retain one over-ceiling family and still classify the next suffix."""

    dense_suffix = "a" * 32
    later_suffix = "b" * 32
    async with _prepared_case(monkeypatch) as case:
        async with case.database.acquire() as connection:
            await _create_root(
                connection,
                case.mrf_schema,
                dense_suffix,
                populated=False,
            )
            await connection.status(
                f"CREATE TABLE {_q(case.mrf_schema)}."
                f"{_q(f'unknown_{dense_suffix}')} (ordinal bigint)"
            )
            await _create_root(
                connection,
                case.mrf_schema,
                later_suffix,
                populated=False,
            )
        monkeypatch.setattr(
            legacy_window,
            "LEGACY_SWEEP_MAX_RELATIONS",
            2,
        )

        plan = await build_legacy_orphan_sweep_plan(
            schema_name=case.mrf_schema,
            control_schema_name=case.control_schema,
            limits=LegacySweepLimits(1, 10, 10, 1_000_000),
            executor=case.database,
        )

    blocked_by_suffix = {
        blocked_suffix.suffix: blocked_suffix.reasons
        for blocked_suffix in plan.blocked
    }
    assert blocked_by_suffix[dense_suffix] == (
        "catalog_window_relation_ceiling_exceeded",
    )
    assert [candidate.suffix for candidate in plan.candidates] == [
        later_suffix
    ]
    assert plan.scanned_suffix_count == 2


@pytest.mark.asyncio
async def test_unexpected_relation_blocks_family_without_starving_next(
    monkeypatch,
) -> None:
    """Block an unexpected family while still selecting the next root."""

    first_suffix = "a" * 32
    second_suffix = "b" * 32
    async with _prepared_case(monkeypatch) as case:
        async with case.database.acquire() as connection:
            await _create_root(
                connection,
                case.mrf_schema,
                first_suffix,
                populated=False,
            )
            await _create_root(
                connection,
                case.mrf_schema,
                second_suffix,
                populated=False,
            )
            await connection.status(
                f"CREATE TABLE {_q(case.mrf_schema)}."
                f"{_q(f'unknown_{first_suffix}')} "
                "(ordinal bigint)"
            )

        plan = await build_legacy_orphan_sweep_plan(
            schema_name=case.mrf_schema,
            control_schema_name=case.control_schema,
            limits=LegacySweepLimits(2, 10, 20, 1_000_000),
            executor=case.database,
        )

    blocked_by_suffix = {
        blocked_suffix.suffix: blocked_suffix.reasons
        for blocked_suffix in plan.blocked
    }
    assert set(blocked_by_suffix) == {first_suffix}
    assert blocked_by_suffix[first_suffix] == (
        "unexpected_relation_catalog_entry",
    )
    assert [candidate.suffix for candidate in plan.candidates] == [
        second_suffix
    ]


@pytest.mark.asyncio
async def test_catalog_windows_advance_across_applied_batches(
    monkeypatch,
) -> None:
    """Apply one lexical batch, plan the next, and replay the first."""

    suffixes = ("a" * 32, "b" * 32)
    limits = LegacySweepLimits(1, 10, 10, 1_000_000)
    async with _prepared_case(monkeypatch) as case:
        async with case.database.acquire() as connection:
            for suffix in suffixes:
                await _create_root(
                    connection,
                    case.mrf_schema,
                    suffix,
                    populated=False,
                )
        first_plan = await build_legacy_orphan_sweep_plan(
            schema_name=case.mrf_schema,
            control_schema_name=case.control_schema,
            limits=limits,
            executor=case.database,
        )
        apply_parameters_by_name = {
            "expected_plan_digest": first_plan.plan_digest,
            "actor": "catalog-window-test",
            "schema_name": case.mrf_schema,
            "control_schema_name": case.control_schema,
            "limits": limits,
            "database": case.database,
        }
        first_result = await execute_legacy_orphan_sweep(
            **apply_parameters_by_name
        )
        second_plan = await build_legacy_orphan_sweep_plan(
            schema_name=case.mrf_schema,
            control_schema_name=case.control_schema,
            limits=limits,
            executor=case.database,
        )
        replay = await execute_legacy_orphan_sweep(
            **apply_parameters_by_name
        )
        first_present, second_present = await _root_presence(case, suffixes)

    assert first_result.state == "applied"
    assert [candidate.suffix for candidate in second_plan.candidates] == [
        suffixes[1]
    ]
    assert replay.state == "already_applied"
    assert first_present is False
    assert second_present is True


@pytest.mark.asyncio
async def test_catalog_inventory_drift_after_root_lock_fails_closed(
    monkeypatch,
) -> None:
    """Reject a relation appearing between the reviewed and locked plans."""

    suffix = "a" * 32
    drift_suffix = "b" * 32
    limits = LegacySweepLimits(1, 10, 10, 1_000_000)
    async with _prepared_case(monkeypatch) as case:
        async with case.database.acquire() as connection:
            await _create_root(
                connection,
                case.mrf_schema,
                suffix,
                populated=False,
            )
        plan = await build_legacy_orphan_sweep_plan(
            schema_name=case.mrf_schema,
            control_schema_name=case.control_schema,
            limits=limits,
            executor=case.database,
        )
        _install_post_lock_drift(monkeypatch, drift_suffix)
        with pytest.raises(
            RuntimeError,
            match="legacy_sweep_plan_changed_after_lock",
        ):
            await execute_legacy_orphan_sweep(
                expected_plan_digest=plan.plan_digest,
                actor="catalog-window-test",
                schema_name=case.mrf_schema,
                control_schema_name=case.control_schema,
                limits=limits,
                database=case.database,
            )
        original_present, drift_present = await _root_presence(
            case,
            (suffix, drift_suffix),
        )

    assert original_present is True
    assert drift_present is False


@pytest.mark.asyncio
async def test_pre_window_audit_replay_remains_compatible(
    monkeypatch,
) -> None:
    """Replay an immutable audit created before progress and limit fields."""

    suffix = "a" * 32
    limits = LegacySweepLimits(1, 10, 10, 1_000_000)
    async with _prepared_case(monkeypatch) as case:
        async with case.database.acquire() as connection:
            table_name = await _create_root(
                connection,
                case.mrf_schema,
                suffix,
                populated=False,
            )
        plan = await build_legacy_orphan_sweep_plan(
            schema_name=case.mrf_schema,
            control_schema_name=case.control_schema,
            limits=limits,
            executor=case.database,
        )
        legacy_audit = _pre_window_audit(plan)
        async with case.database.acquire() as connection:
            await insert_legacy_sweep_audit(
                connection,
                schema_name=case.mrf_schema,
                audit=legacy_audit,
            )
            await connection.status(
                f"DROP TABLE {_q(case.mrf_schema)}.{_q(table_name)}"
            )
        replay = await execute_legacy_orphan_sweep(
            expected_plan_digest=legacy_audit.plan_digest,
            actor="catalog-window-test",
            schema_name=case.mrf_schema,
            control_schema_name=case.control_schema,
            limits=limits,
            database=case.database,
        )

    assert replay.state == "already_applied"
