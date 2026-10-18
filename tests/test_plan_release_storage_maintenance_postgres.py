# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real SELECT-only maintenance actors, lifecycle fencing and pin retention."""

from types import SimpleNamespace

import asyncpg
import pytest

from process.plan_release_storage_custody import require_plan_release_storage_custody
from process.ptg_parts import source_snapshot_control
from process.ptg_parts.ptg2_lifecycle_lock import acquire_ptg2_source_lifecycle_lock
from process.ptg_parts.source_snapshot_predecessor_retirement_store import load_retirement_context
from process.ptg_parts.source_snapshot_predecessor_retirement_types import PTG2PredecessorRetirementConflict
from tests.plan_release_storage_cohort_support import attach_native_cohort, prepare_native_cohort
from tests.plan_release_storage_maintenance_support import (
    acquire_publication_fence,
    maintenance_session,
    prepare_maintenance_actor,
)
from tests.plan_release_storage_postgres_support import NEW_REVISION, OLD_REVISION, storage_case


def _request():
    return SimpleNamespace(
        source_key="synthetic_source", current_snapshot_id="new-snapshot", predecessor_snapshot_id="old-snapshot"
    )


@pytest.mark.asyncio
async def test_select_only_maintenance_requires_source_fence_and_blocks_concurrent_cut():
    async with storage_case(provisioned=True) as case:
        binding, pin = await prepare_native_cohort(case)
        await prepare_maintenance_actor(case)
        publisher = await case.connect(case.publisher)
        async with maintenance_session(case) as session:
            with pytest.raises(PTG2PredecessorRetirementConflict, match="require the source lifecycle fence"):
                await load_retirement_context(
                    session, schema_name=case.schema, control_schema_name=case.preparation_schema, request=_request()
                )
            await acquire_ptg2_source_lifecycle_lock(session, source_key="other_source")
            with pytest.raises(PTG2PredecessorRetirementConflict, match="require the source lifecycle fence"):
                await load_retirement_context(
                    session, schema_name=case.schema, control_schema_name=case.preparation_schema, request=_request()
                )
            await acquire_ptg2_source_lifecycle_lock(session, source_key="synthetic_source")
            context = await load_retirement_context(
                session, schema_name=case.schema, control_schema_name=case.preparation_schema, request=_request()
            )
            assert len(context.release_binding_records) == 1000 and len(context.pin_records) == 2
            assert all(
                member["owner_type"] == "plan_release_serving_revision" and member["owner_id"] == OLD_REVISION
                for member in context.pin_records
            )
            for relation in (
                binding,
                pin,
                case.relation("plan_release_snapshot_binding"),
                case.relation("plan_release_snapshot_pin"),
            ):
                from sqlalchemy import text

                assert await session.scalar(
                    text(
                        "SELECT NOT has_table_privilege(current_user,:relation,'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN')"
                    ),
                    {"relation": relation},
                )
            with pytest.raises(asyncpg.LockNotAvailableError):
                async with publisher.transaction():
                    await acquire_publication_fence(publisher)
        async with publisher.transaction():
            await acquire_publication_fence(publisher)
            await require_plan_release_storage_custody(publisher, case.schema, **case.profile)
            await attach_native_cohort(case, publisher, binding, pin)
        assert (
            await publisher.fetchval(
                f"SELECT count(*) FROM {case.relation('plan_release_snapshot_binding')} WHERE serving_revision_id=$1",
                OLD_REVISION,
            )
            == 1000
        )
        assert (
            await publisher.fetchval(
                f"SELECT count(*) FROM {case.relation('plan_release_snapshot_binding')} WHERE serving_revision_id=$1",
                NEW_REVISION,
            )
            == 2000
        )


@pytest.mark.asyncio
async def test_source_controls_see_both_pin_stores(monkeypatch):
    from process.ptg_parts.ptg2_v4_failed_layout_state import _pin_reference_count_sql

    async with storage_case(provisioned=True) as case:
        await prepare_maintenance_actor(case)
        reader = await case.connect(case.reader)

        async def all_rows(statement, **parameters):
            return await reader.fetch(statement.replace(":snapshot_id", "$1"), parameters["snapshot_id"])

        monkeypatch.setattr(source_snapshot_control, "db", SimpleNamespace(all=all_rows))
        refs = await source_snapshot_control._current_references(case.schema, "old-snapshot")
        assert refs["plan_release_pins"] == ["plan_release_serving_revision:" + OLD_REVISION] * 2
        assert len(refs["plan_release_bindings"]) == 1000
        query = "SELECT " + _pin_reference_count_sql('"' + case.schema + '"')
        assert await reader.fetchval(query.replace(":snapshot_id", "$1"), "old-snapshot") == 2


@pytest.mark.asyncio
async def test_legacy_orphan_sweeper_retains_a_dedicated_plan_pin(monkeypatch):
    from process.ptg_parts.ptg2_legacy_orphan_sweeper import build_legacy_orphan_sweep_plan
    from tests.test_ptg2_legacy_orphan_postgres_support import (
        LIMITS,
        SUFFIX_OWNED,
        _prepared_case,
        _q,
        _seed_terminal_owner,
    )

    async with _prepared_case(monkeypatch) as case:
        async with case.database.acquire() as connection:
            snapshot_id = await _seed_terminal_owner(connection, case.mrf_schema, case.control_schema, SUFFIX_OWNED)
        parameters_by_name = dict(
            schema_name=case.mrf_schema, control_schema_name=case.control_schema, limits=LIMITS, executor=case.database
        )
        before = await build_legacy_orphan_sweep_plan(**parameters_by_name)
        assert [candidate.suffix for candidate in before.candidates] == [SUFFIX_OWNED]
        relation = _q(case.mrf_schema) + ".plan_release_snapshot_pin"
        await case.database.status(f"INSERT INTO {relation}(snapshot_id) VALUES(:snapshot_id)", snapshot_id=snapshot_id)
        retained = await build_legacy_orphan_sweep_plan(**parameters_by_name)
        assert not retained.candidates
        assert [(blocked.suffix, blocked.reasons) for blocked in retained.blocked] == [
            (SUFFIX_OWNED, ("serving_or_lifecycle_reference",))
        ]
        await case.database.status(f"DELETE FROM {relation} WHERE snapshot_id=:snapshot_id", snapshot_id=snapshot_id)
        restored = await build_legacy_orphan_sweep_plan(**parameters_by_name)
        assert [candidate.suffix for candidate in restored.candidates] == [SUFFIX_OWNED]
