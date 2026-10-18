# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native candidate-only successors for immutable identity writer regressions."""

import asyncio
from uuid import UUID

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker

from process import provider_directory_resource_identity_candidate as candidate
from process.provider_directory_generic_source_policy import bound_publisher
from process.provider_directory_resource_identity import source_resource_uuid
from tests.provider_directory_resource_identity_postgres_support import prepare_resource_candidate
from tests.provider_directory_source_postgres_support import source_component_database


async def _accepted(component):
    async with component.admin_engine.connect() as connection:
        return (
            await connection.execute(
                text(
                    "SELECT source_id,resource_type,resource_id,entity_id,created_at "
                    "FROM mrf.provider_directory_resource_identity ORDER BY source_id,resource_type,resource_id"
                )
            )
        ).all()


@pytest.mark.asyncio
async def test_identity_binder_replays_and_rolls_back_without_reassignment(monkeypatch):
    async with source_component_database(monkeypatch) as component:
        first = await prepare_resource_candidate(component, ["plan", "plan"])
        expected = source_resource_uuid("cms-npd", "InsurancePlan", "plan")
        assert await first.load(["plan", "plan"]) == [expected, expected]
        assert await _accepted(component) == []
        await first.seal()
        await first.publish()
        original = await _accepted(component)
        replay = await prepare_resource_candidate(component, ["plan"])
        assert await replay.load(["plan"]) == [expected]
        await replay.seal()
        await replay.publish()
        assert await _accepted(component) == original
        cancelled = await _assert_cancelled_publication(component)
        assert await _accepted(component) == original
        await cancelled.publish()
        resumed = await _accepted(component)
        assert [row for row in resumed if row.resource_id != "rolled-back"] == original
        assert [row.entity_id for row in resumed if row.resource_id == "rolled-back"] == [
            source_resource_uuid("cms-npd", "InsurancePlan", "rolled-back")
        ]


async def _assert_cancelled_publication(component):
    stopped = await prepare_resource_candidate(component, ["rolled-back"])
    await stopped.load(["rolled-back"])
    await stopped.seal()
    with pytest.raises(asyncio.CancelledError):
        async with stopped.transaction():
            await candidate.publish_resource_identity_stage(
                bound_publisher(), stopped.identity, stopped.binding["auxiliary"]["tables"][candidate.ROOT]
            )
            raise asyncio.CancelledError()
    return stopped


@pytest.mark.asyncio
async def test_resource_identity_binder_accepts_one_thousand_ids(monkeypatch):
    resource_ids = [f"role-{index}" for index in range(1000)]
    async with source_component_database(monkeypatch) as component:
        first = await prepare_resource_candidate(component, resource_ids, resource_type="PractitionerRole")
        ids = await first.load(resource_ids, "PractitionerRole")
        assert ids == [source_resource_uuid("cms-npd", "PractitionerRole", key) for key in resource_ids]
        await first.seal()
        await first.publish()
        original = await _accepted(component)
        assert len(original) == 1000
        replay = await prepare_resource_candidate(component, resource_ids, resource_type="PractitionerRole")
        assert await replay.load(resource_ids, "PractitionerRole") == ids
        with pytest.raises(ValueError, match="batch_invalid"):
            await replay.load(resource_ids + ["extra"], "PractitionerRole")
        await replay.seal()
        await replay.publish()
        assert await _accepted(component) == original


@pytest.mark.asyncio
async def test_binder_rejects_existing_conflicting_identity(monkeypatch):
    async with source_component_database(monkeypatch) as component:
        async with component.admin_engine.begin() as connection:
            await connection.execute(
                text(
                    "INSERT INTO mrf.provider_directory_resource_identity VALUES "
                    "('cms-npd','InsurancePlan','conflict','00000000-0000-0000-0000-000000000099',now())"
                )
            )
        original = await _accepted(component)
        staged = await prepare_resource_candidate(component, ["conflict", "fresh"])
        await staged.load(["conflict", "fresh"])
        with pytest.raises(candidate.CMSIdentityReprepareRequired, match="immutable_replay_conflict"):
            await staged.seal()
        assert await _accepted(component) == original


@pytest.mark.asyncio
@pytest.mark.parametrize("winner_outcome", ["compatible_commit", "conflicting_commit", "rollback"])
async def test_identity_binder_reads_a_concurrent_insert_after_its_statement(monkeypatch, winner_outcome):
    async with source_component_database(monkeypatch) as component:
        staged = await prepare_resource_candidate(component, ["fresh-during-race", "concurrent-plan"])
        ids = await staged.load(["fresh-during-race", "concurrent-plan", "fresh-during-race"])
        await staged.seal()
        sessions = async_sessionmaker(component.admin_engine)
        async with sessions() as winner:
            winner_pid = await winner.scalar(text("SELECT pg_backend_pid()"))
            entity = source_resource_uuid("cms-npd", "InsurancePlan", "concurrent-plan")
            if winner_outcome == "conflicting_commit":
                entity = UUID("00000000-0000-0000-0000-000000000099")
            await winner.execute(
                text(
                    "INSERT INTO mrf.provider_directory_resource_identity VALUES "
                    "('cms-npd','InsurancePlan','concurrent-plan',:entity,now())"
                ),
                {"entity": entity},
            )
            await _resolve_waiting_publication(staged, winner, winner_pid, winner_outcome)
        accepted_by_resource_id = {row.resource_id: row.entity_id for row in await _accepted(component)}
        if winner_outcome == "conflicting_commit":
            assert accepted_by_resource_id == {"concurrent-plan": entity}
        else:
            assert [
                accepted_by_resource_id[key] for key in ("fresh-during-race", "concurrent-plan", "fresh-during-race")
            ] == ids


async def _resolve_waiting_publication(staged, winner, winner_pid, outcome):
    started, observed = asyncio.Event(), {}
    task = asyncio.create_task(_publish_contender(staged, started, observed))
    try:
        await asyncio.wait_for(started.wait(), 3)
        async with asyncio.timeout(3):
            while not await winner.scalar(
                text("SELECT :holder = ANY(pg_blocking_pids(:contender))"),
                {"holder": winner_pid, "contender": observed["pid"]},
            ):
                assert not task.done()
                await asyncio.sleep(0.01)
        await (winner.rollback() if outcome == "rollback" else winner.commit())
        if outcome == "conflicting_commit":
            with pytest.raises(candidate.CMSIdentityReprepareRequired, match="immutable_replay_conflict"):
                await asyncio.wait_for(task, 3)
        else:
            await asyncio.wait_for(task, 3)
    finally:
        await winner.rollback()
        if not task.done():
            task.cancel()
        await asyncio.wait_for(asyncio.gather(task, return_exceptions=True), 3)


async def _publish_contender(staged, started, observed):
    async with staged.transaction():
        publisher = bound_publisher()
        observed["pid"] = publisher.connection.get_server_pid()
        started.set()
        await candidate.publish_resource_identity_stage(
            publisher, staged.identity, staged.binding["auxiliary"]["tables"][candidate.ROOT]
        )


@pytest.mark.asyncio
async def test_identity_binder_mixed_batch_preserves_committed_uuid_and_timestamp(monkeypatch):
    async with source_component_database(monkeypatch) as component:
        incumbent = await prepare_resource_candidate(component, ["existing"])
        await incumbent.load(["existing"])
        await incumbent.seal()
        await incumbent.publish()
        original = (await _accepted(component))[0]
        mixed = await prepare_resource_candidate(component, ["existing", "fresh"])
        ids = await mixed.load(["fresh", "existing", "fresh"])
        await mixed.seal()
        await mixed.publish()
        accepted_by_resource_id = {row.resource_id: row for row in await _accepted(component)}
        assert accepted_by_resource_id["existing"] == original
        assert [accepted_by_resource_id[key].entity_id for key in ("fresh", "existing", "fresh")] == ids
        assert set(accepted_by_resource_id) == {"existing", "fresh"}


@pytest.mark.asyncio
async def test_identity_binder_fresh_batches_keep_native_type_scopes(monkeypatch):
    async with source_component_database(monkeypatch) as component:
        for kind in ("InsurancePlan", "PractitionerRole"):
            staged = await prepare_resource_candidate(component, ["same-key"], resource_type=kind)
            assert (
                await staged.load(["same-key", "same-key"], kind)
                == [source_resource_uuid("cms-npd", kind, "same-key")] * 2
            )
            await staged.seal()
            await staged.publish()
        accepted = await _accepted(component)
        assert len(accepted) == 2
        assert {row.resource_type for row in accepted} == {"InsurancePlan", "PractitionerRole"}
        assert len({row.entity_id for row in accepted}) == 2
