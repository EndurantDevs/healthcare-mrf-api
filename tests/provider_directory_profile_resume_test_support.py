# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Interruption fixtures for restart-safe Provider Directory profile tests."""

from __future__ import annotations

import importlib
from dataclasses import replace
from types import SimpleNamespace
from typing import Any

import pytest

from db.connection import Database
from process import provider_directory_profile as profile


importer = importlib.import_module("process.provider_directory_fhir")


def _resume_build(schema: str):
    """Return the deterministic multi-source build under interruption."""
    return importer._ProviderDirectoryProfileBuild(
        schema=schema,
        generation_id="profile-affiliation-test",
        source_ids=(
            "profile-source-a",
            "profile-source-b",
            "profile-source-uhc",
        ),
        retained_source_ids=(
            "profile-source-a",
            "profile-source-b",
            "profile-source-uhc",
        ),
        dataset_ids=(
            "profile-dataset-a",
            "profile-dataset-b",
            "profile-dataset-uhc",
        ),
        profile_as_of="2026-07-19",
        evidence_stage="profile_evidence_resume_stage",
        profile_stage="profile_resume_stage",
        build_id="profile-resume-build",
        owner_run_id="profile-run-first",
    )


def _resume_batch_plan(build):
    """Return the bounded batch geometry used by the resume proof."""
    return importer._provider_directory_profile_batch_plan(
        build.source_ids,
        build.retained_source_ids,
        build.dataset_ids,
        has_existing_artifacts=False,
        evidence_window_size=2,
        compact_window_size=2,
    )


async def create_resume_context(
    monkeypatch,
    database: Database,
    schema: str,
) -> SimpleNamespace:
    """Bind one deterministic multi-source build and progress recorder."""
    monkeypatch.setattr(importer, "db", database)
    progress_events: list[tuple[str | None, dict[str, Any]]] = []

    async def record_progress(
        run_id: str | None,
        **progress_by_name: Any,
    ) -> None:
        progress_events.append((run_id, progress_by_name))

    monkeypatch.setattr(
        importer,
        "_mark_provider_directory_progress",
        record_progress,
    )
    build = _resume_build(schema)
    batch_plan = _resume_batch_plan(build)
    assert len(batch_plan.evidence_batches) == 345
    return SimpleNamespace(
        monkeypatch=monkeypatch,
        database=database,
        schema=schema,
        build=replace(build, batch_plan=batch_plan),
        fence=importer.ProviderDirectoryArtifactBuildFence(target_oid=None),
        original_status=database.status,
        progress_events=progress_events,
        expected_evidence_total=len(batch_plan.evidence_batches),
        checkpoint_ref=profile.qualified_table(
            schema,
            "provider_directory_profile_build_checkpoint",
        ),
    )


def _is_evidence_insert(context: SimpleNamespace, raw_sql: Any) -> bool:
    sql = str(raw_sql)
    return (
        f'INSERT INTO "{context.schema}"."{context.build.evidence_stage}"'
        in sql
        and "ON CONFLICT (evidence_key) DO NOTHING" in sql
    )


def _is_profile_insert(context: SimpleNamespace, raw_sql: Any) -> bool:
    sql = str(raw_sql)
    return (
        f'INSERT INTO "{context.schema}"."{context.build.profile_stage}"'
        in sql
        and "ON CONFLICT (npi) DO NOTHING" in sql
    )


async def _checkpoint(
    context: SimpleNamespace,
    selected_columns: str,
):
    return await context.database.first(
        f"SELECT {selected_columns} FROM {context.checkpoint_ref} "
        "WHERE build_id = :build_id;",
        build_id=context.build.build_id,
    )


def _progress_for(
    context: SimpleNamespace,
    run_id: str,
    *,
    phase: str | None = None,
) -> list[dict[str, Any]]:
    return [
        progress
        for progress_run_id, progress in context.progress_events
        if progress_run_id == run_id
        and (phase is None or progress["phase"] == phase)
    ]


def _assert_first_interruption(
    context: SimpleNamespace,
    checkpoint,
) -> None:
    assert checkpoint is not None
    assert checkpoint.state == "failed"
    assert checkpoint.evidence_next_batch == 0
    assert checkpoint.profile_next_batch == 0
    first_progress = _progress_for(context, "profile-run-first")
    assert [
        (progress["done"], progress["total"])
        for progress in first_progress
    ] == [(0, context.expected_evidence_total)]
    assert {progress["phase"] for progress in first_progress} == {
        importer._PROFILE_EVIDENCE_PROGRESS_PHASE
    }
    assert {
        progress["details"]["_progress_unit"]
        for progress in first_progress
    } == {"batches"}


async def interrupt_first_evidence_batch(
    context: SimpleNamespace,
) -> int:
    """Interrupt the second evidence insert and prove rollback identity."""
    statement_starts: list[None] = []

    async def interrupting_status(sql: Any, **params: Any):
        if _is_evidence_insert(context, sql):
            if len(statement_starts) == 1:
                raise RuntimeError("forced resumable interruption")
            statement_starts.append(None)
        return await context.original_status(sql, **params)

    context.monkeypatch.setattr(
        context.database, "status", interrupting_status
    )
    with pytest.raises(RuntimeError, match="forced resumable interruption"):
        await importer._build_provider_directory_profile_stages(
            context.build,
            context.fence,
            context.fence,
            has_existing_artifacts=False,
        )
    checkpoint = await _checkpoint(
        context, "state, evidence_next_batch, profile_next_batch"
    )
    _assert_first_interruption(context, checkpoint)
    return int(
        await context.database.scalar(
            f"SELECT count(*) FROM "
            f"{profile.qualified_table(context.schema, context.build.evidence_stage)};"
        )
        or 0
    )


def _assert_compact_interruption(
    context: SimpleNamespace,
    checkpoint,
    evidence_statements: list[str],
) -> None:
    assert checkpoint is not None
    assert checkpoint.state == "failed"
    assert (
        checkpoint.evidence_next_batch
        == checkpoint.evidence_total_batches
    )
    assert checkpoint.profile_next_batch == 0
    assert len(evidence_statements) == checkpoint.evidence_total_batches
    evidence_progress = _progress_for(
        context,
        "profile-run-retry",
        phase=importer._PROFILE_EVIDENCE_PROGRESS_PHASE,
    )
    assert [progress["done"] for progress in evidence_progress] == list(
        range(context.expected_evidence_total + 1)
    )
    profile_progress = _progress_for(
        context,
        "profile-run-retry",
        phase=importer._PROFILE_COMPACT_PROGRESS_PHASE,
    )
    assert [
        (progress["done"], progress["total"])
        for progress in profile_progress
    ] == [(0, 400)]


async def interrupt_first_compact_batch(
    context: SimpleNamespace,
):
    """Replay evidence, interrupt compact work, and return retry build."""
    evidence_statements: list[str] = []

    async def tracking_status(sql: Any, **params: Any):
        if _is_evidence_insert(context, sql):
            evidence_statements.append(str(sql))
        if _is_profile_insert(context, sql):
            raise RuntimeError("forced compact interruption")
        return await context.original_status(sql, **params)

    context.monkeypatch.setattr(context.database, "status", tracking_status)
    resumed_build = replace(
        context.build, owner_run_id="profile-run-retry"
    )
    with pytest.raises(RuntimeError, match="forced compact interruption"):
        await importer._build_provider_directory_profile_stages(
            resumed_build,
            context.fence,
            context.fence,
            has_existing_artifacts=False,
        )
    checkpoint = await _checkpoint(
        context,
        "state, evidence_next_batch, evidence_total_batches, "
        "profile_next_batch",
    )
    _assert_compact_interruption(context, checkpoint, evidence_statements)
    return resumed_build


async def _stage_oids(context: SimpleNamespace) -> tuple[int, int]:
    relation_oids: list[int] = []
    for stage_table in (
        context.build.evidence_stage,
        context.build.profile_stage,
    ):
        relation_oids.append(
            int(
                await context.database.scalar(
                    "SELECT to_regclass(:relation_name)::oid;",
                    relation_name=f"{context.schema}.{stage_table}",
                )
                or 0
            )
        )
    return relation_oids[0], relation_oids[1]


def _assert_boundary_checkpoint(checkpoint) -> None:
    assert checkpoint is not None
    assert checkpoint.state == "building_profile"
    assert (
        checkpoint.evidence_next_batch
        == checkpoint.evidence_total_batches
    )
    assert checkpoint.profile_next_batch == 0


async def interrupt_at_phase_boundary(
    context: SimpleNamespace,
    resumed_build,
) -> tuple[int, int]:
    """Stop before the next compact batch and prove stage continuity."""
    stage_oids_before = await _stage_oids(context)
    evidence_reopen_attempts: list[None] = []
    original_functions = (
        importer._populate_provider_directory_profile_evidence_stage,
        importer._populate_provider_directory_profile_compact_stage,
        importer._mark_profile_build_checkpoint_failed,
    )

    async def reject_evidence_reopen(*_args: Any, **_params: Any):
        evidence_reopen_attempts.append(None)
        raise AssertionError("completed evidence phase was reopened")

    async def stop_before_compact(*_args: Any, **_params: Any):
        raise RuntimeError("hard stop before next compact batch")

    async def preserve_checkpoint(*_args: Any, **_params: Any) -> None:
        return None

    patch_names = (
        "_populate_provider_directory_profile_evidence_stage",
        "_populate_provider_directory_profile_compact_stage",
        "_mark_profile_build_checkpoint_failed",
    )
    for name, replacement in zip(
        patch_names,
        (reject_evidence_reopen, stop_before_compact, preserve_checkpoint),
    ):
        context.monkeypatch.setattr(importer, name, replacement)
    with pytest.raises(RuntimeError, match="hard stop before next compact batch"):
        await importer._build_provider_directory_profile_stages(
            replace(
                resumed_build, owner_run_id="profile-run-boundary-stop"
            ),
            context.fence,
            context.fence,
            has_existing_artifacts=False,
        )
    checkpoint = await _checkpoint(
        context,
        "state, evidence_next_batch, evidence_total_batches, "
        "profile_next_batch",
    )
    assert evidence_reopen_attempts == []
    _assert_boundary_checkpoint(checkpoint)
    assert await _stage_oids(context) == stage_oids_before
    for name, original_function in zip(patch_names, original_functions):
        context.monkeypatch.setattr(importer, name, original_function)
    return stage_oids_before
