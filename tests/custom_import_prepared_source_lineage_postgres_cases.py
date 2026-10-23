# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Checkpoint-seeded native proof; this does not exercise SOURCE or publication."""

from types import SimpleNamespace

import pytest
from sqlalchemy import func, insert, select, text

from db.models.custom_import import (
    CustomImportBuildAttempt,
    CustomImportBuildStream,
    CustomImportBuildVerification,
    CustomImportCaptureParquetPart,
    CustomImportGeneration,
)
from process.custom_import import prepared_source_lineage as lineage
from process.custom_import.build_graph import _read_transaction
from process.custom_import.build_source import SourceBuildRequest
from process.custom_import.execution import lease_token_sha256
from process.custom_import.runner_types import CandidateRunnerError
from process.custom_import.storage_layout import (
    snapshot_phase_index_statements,
    snapshot_serving_index_statements,
    snapshot_tables,
)
from tests.custom_import_postgres_support import isolated_publication_case
from tests.custom_import_prepared_source_store_postgres_cases import _context, _material, _run_driver
from tests.test_custom_import_staged_verification_postgres import _install as _install_staged
from tests.test_custom_import_verification_arms_postgres import _install as _install_arms
from tests.test_custom_import_verification_root_ranges_postgres import _install as _install_ranges


def _source_request(context):
    policy = context.policy.build
    return SourceBuildRequest(
        dataset_id=context.dataset_id,
        definition_revision_id=context.definition_revision_id,
        schema_revision_id=context.schema_revision_id,
        execution_id=context.execution_id,
        lease_token=context.lease_token,
        fence=context.fence,
        definition=context.definition,
        expected_base_generation_id=None,
        expected_pointer_version=0,
        complete_scope=True,
        page_row_limit=policy.page_row_limit,
        page_byte_limit=policy.page_byte_limit,
        statement_timeout_ms=policy.statement_timeout_ms,
        build_deadline_at=context.build_deadline_at,
        lease_seconds=policy.lease_seconds,
    )


async def _seed_generation(session, context):
    generation = CustomImportGeneration(
        dataset_id=context.dataset_id,
        definition_revision_id=context.definition_revision_id,
        schema_revision_id=context.schema_revision_id,
        execution_id=context.execution_id,
        capture_bundle_id=context.capture_bundle_id,
        source_bundle_sha256=b"s" * 32,
        candidate_sha256=b"c" * 32,
        root_count=0,
        family_count=0,
        producing_fence=context.fence,
        producing_token_sha256=lease_token_sha256(context.lease_token),
    )
    session.add(generation)
    await session.flush()
    return generation.generation_id


async def _seed_build(session, context, generation_id, material):
    policy = context.policy.build
    now = await session.scalar(select(func.clock_timestamp()))
    build = CustomImportBuildAttempt(
        dataset_id=context.dataset_id,
        definition_revision_id=context.definition_revision_id,
        schema_revision_id=context.schema_revision_id,
        execution_id=context.execution_id,
        capture_bundle_id=context.capture_bundle_id,
        producing_fence=context.fence,
        producing_token_sha256=lease_token_sha256(context.lease_token),
        request_identity_sha256=context.request_identity_sha256,
        base_pointer_version=0,
        refresh_mode=context.definition.refresh_mode,
        complete_scope=True,
        phase="verifying",
        generation_id=generation_id,
        page_row_limit=policy.page_row_limit,
        page_byte_limit=policy.page_byte_limit,
        statement_timeout_ms=policy.statement_timeout_ms,
        build_deadline_at=context.build_deadline_at,
        source_frozen_at=now,
        graph_frozen_at=now,
        output_frozen_at=now,
        source_occurrence_count=2,
    )
    session.add(build)
    await session.flush()
    session.add_all(
        [
            CustomImportBuildStream(
                build_id=build.build_id,
                stream_slot=slot,
                next_part_ordinal=len(parts) + 1,
                next_source_ordinal=sum(part.record_count for part in parts),
                replay_verified_at=now,
            )
            for slot, parts in enumerate(material.parts, 1)
        ]
    )
    await session.flush()
    return build


async def _seed_verification(session, control, build, sequence, context):
    await session.execute(
        text(
            f"INSERT INTO {control}.custom_import_build_verification "
            "(build_id,generation_id,page_sequence,scan_stage,source_frozen_at,graph_frozen_at,output_frozen_at) "
            "VALUES(:build,:generation,:sequence,:stage,:frozen,:frozen,:frozen)"
        ),
        {
            "build": build.build_id,
            "generation": build.generation_id,
            "sequence": sequence,
            "stage": lineage._CHECKPOINTS[sequence][0],
            "frozen": build.source_frozen_at,
        },
    )
    await session.execute(
        text(
            f"SELECT {control}.bind_custom_import_snapshot_generation(CAST(:execution AS bigint),CAST(:fence AS bigint),:token,CAST(:generation AS bigint))"
        ),
        {
            "execution": context.execution_id,
            "fence": context.fence,
            "token": lease_token_sha256(context.lease_token),
            "generation": build.generation_id,
        },
    )


async def _seed_occurrences(session, family_id, build_id):
    occurrence = snapshot_tables(family_id)["custom_import_build_occurrence"]
    # These rows isolate position validation, including a rejected occurrence.
    await session.execute(
        insert(occurrence),
        [
            dict(
                build_id=build_id,
                stream_slot=1,
                pack_id=1,
                origin="source",
                source_part_ordinal=1,
                part_row_ordinal=0,
                source_ordinal=0,
                record_kind="root",
                collection_slot=0,
                rejection_id=1,
                root_revision_id=None,
            ),
            dict(
                build_id=build_id,
                stream_slot=1,
                pack_id=2,
                origin="source",
                source_part_ordinal=2,
                part_row_ordinal=0,
                source_ordinal=1,
                record_kind="root",
                collection_slot=0,
                rejection_id=None,
                root_revision_id=1,
            ),
        ],
    )
    await session.flush()


async def _freeze_snapshot(session, control, context, family_id):
    statements = dict.fromkeys(
        statement
        for phase in ("admission", "graph", "output")
        for statement in snapshot_phase_index_statements(family_id, phase)
    )
    for statement in statements:
        await session.execute(text(statement))
    for statement in snapshot_serving_index_statements(family_id):
        await session.execute(text(statement))
    await session.execute(
        text(
            f"SELECT {control}.freeze_custom_import_snapshot_family(CAST(:execution AS bigint),CAST(:fence AS bigint),:token)"
        ),
        {
            "execution": context.execution_id,
            "fence": context.fence,
            "token": lease_token_sha256(context.lease_token),
        },
    )


async def _seed_checkpoint(case, sequence):
    """Prepare real raw input, then seed only the frozen checkpoint under test."""
    material = _material()
    context = await _context(case, material)
    await _run_driver(case, context)
    request = _source_request(context)
    async with case.engine.begin() as connection:
        await connection.run_sync(_install_staged, case.schema_name)
        await connection.run_sync(_install_arms, case.schema_name)
        await connection.run_sync(_install_ranges, case.schema_name, 16_384)
    control = '"' + case.schema_name + '"'
    async with case.sessions() as session, session.begin():
        await session.execute(text("SET LOCAL statement_timeout='1000ms'"))
        family_id = await session.scalar(
            text(f"SELECT {control}.create_custom_import_snapshot_family(:execution,CAST(:fence AS bigint),:token)"),
            {
                "execution": context.execution_id,
                "fence": context.fence,
                "token": lease_token_sha256(context.lease_token),
            },
        )
        generation_id = await _seed_generation(session, context)
        build = await _seed_build(session, context, generation_id, material)
        await _seed_verification(session, control, build, sequence, context)
        await _seed_occurrences(session, family_id, build.build_id)
        await _freeze_snapshot(session, control, context, family_id)
    return SimpleNamespace(request=request, build_id=build.build_id, context=context, family_id=family_id)


async def _proof(case, build_id):
    async with case.sessions() as session:
        proof = await session.get(CustomImportBuildVerification, build_id)
        build = await session.get(CustomImportBuildAttempt, build_id)
        return proof.page_sequence, proof.scan_stage, proof.verification_state, build.phase, build.verified_at


def _step(session, fixture):
    with _read_transaction(session, fixture.request, fixture.build_id) as (_build, deadline):
        return lineage.verify_prepared_source_lineage(session, fixture.request, fixture.build_id, deadline=deadline)


def _corrupt_checkpoint(session, fixture, case, sequence):
    with _read_transaction(session, fixture.request, fixture.build_id) as (_build, deadline):
        if sequence == 26:
            session.execute(
                text(
                    f"UPDATE ci_snapshot_{fixture.family_id}.custom_import_build_occurrence "
                    "SET part_row_ordinal=1 WHERE rejection_id IS NOT NULL"
                )
            )
        else:
            session.execute(
                text(
                    f'UPDATE "{case.schema_name}".custom_import_prepared_source_part '
                    "SET dataset_id=dataset_id+1 WHERE execution_id=:execution AND stream_slot=1"
                ),
                {"execution": fixture.context.execution_id},
            )
        return lineage.verify_prepared_source_lineage(session, fixture.request, fixture.build_id, deadline=deadline)


def _nonowner_step(session, fixture):
    with _read_transaction(session, fixture.request, fixture.build_id) as (_build, deadline):
        session.execute(text("SET LOCAL ROLE pg_read_all_data"))
        return lineage.verify_prepared_source_lineage(session, fixture.request, fixture.build_id, deadline=deadline)


async def _assert_owner_refusals(case, fixture, monkeypatch):
    with monkeypatch.context() as patch:
        patch.setattr(lineage, "_VERIFIER_SHA256", b"x" * 32)
        async with case.sessions() as session:
            with pytest.raises(CandidateRunnerError, match="owner_or_layout_invalid"):
                await session.run_sync(lambda sync: _step(sync, fixture))
    async with case.sessions() as session:
        with pytest.raises(CandidateRunnerError, match="owner_or_layout_invalid"):
            await session.run_sync(lambda sync: _nonowner_step(sync, fixture))


async def _assert_final_guard_rollback(case, fixture, monkeypatch):
    def abort(_deadline):
        raise RuntimeError("synthetic final guard failure")

    with monkeypatch.context() as patch:
        patch.setattr(lineage, "_require_budget", abort)
        async with case.sessions() as session:
            with pytest.raises(RuntimeError, match="final guard failure"):
                await session.run_sync(lambda sync: _step(sync, fixture))


@pytest.mark.parametrize("sequence", (26, 59))
async def test_prepared_checkpoint_guards_and_installed_body(sequence, monkeypatch):
    """Reject invalid scope and authority, roll back failures, and advance once."""
    async with isolated_publication_case() as case:
        fixture = await _seed_checkpoint(case, sequence)
        identity = f'"{case.schema_name}".verify_custom_import_build_structure(bigint)'
        async with case.sessions() as session:
            before = await session.scalar(
                text("SELECT to_jsonb(p) FROM pg_proc p WHERE oid=to_regprocedure(:identity)"), {"identity": identity}
            )
            assert await session.scalar(select(func.count()).select_from(CustomImportCaptureParquetPart)) == 0
        expected = (sequence, lineage._CHECKPOINTS[sequence][0], "scanning", "verifying", None)
        assert await _proof(case, fixture.build_id) == expected
        async with case.sessions() as session:
            with pytest.raises(CandidateRunnerError, match="relationship_invalid"):
                await session.run_sync(lambda sync: _corrupt_checkpoint(sync, fixture, case, sequence))
        assert await _proof(case, fixture.build_id) == expected
        if sequence == 26:
            await _assert_owner_refusals(case, fixture, monkeypatch)
            assert await _proof(case, fixture.build_id) == expected
        await _assert_final_guard_rollback(case, fixture, monkeypatch)
        assert await _proof(case, fixture.build_id) == expected
        async with case.sessions() as session:
            assert await session.run_sync(lambda sync: _step(sync, fixture)) == "scanning"
        assert await _proof(case, fixture.build_id) == (
            sequence + 1,
            lineage._CHECKPOINTS[sequence][1],
            "scanning",
            "verifying",
            None,
        )
        async with case.sessions() as session:
            assert await session.run_sync(lambda sync: _step(sync, fixture)) is None
            after = await session.scalar(
                text("SELECT to_jsonb(p) FROM pg_proc p WHERE oid=to_regprocedure(:identity)"), {"identity": identity}
            )
        assert after == before
