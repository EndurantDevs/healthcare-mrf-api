# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Completion fixtures for restart-safe Provider Directory profile tests."""

from __future__ import annotations

import importlib
from dataclasses import replace
from types import SimpleNamespace
from typing import Any

import pytest

from process import provider_directory_profile as profile
from tests.provider_directory_profile_resume_test_support import (
    _checkpoint,
    _is_evidence_insert,
    _is_profile_insert,
    _progress_for,
    _stage_oids,
)


importer = importlib.import_module("process.provider_directory_fhir")


def _assert_last_batch_checkpoint(
    checkpoint,
    evidence_statements: list[str],
    profile_statements: list[str],
) -> None:
    assert checkpoint is not None
    assert evidence_statements == []
    assert len(profile_statements) == checkpoint.profile_total_batches
    assert checkpoint.state == "failed"
    assert (
        checkpoint.profile_next_batch
        == checkpoint.profile_total_batches
    )


async def interrupt_after_completed_batches(
    context: SimpleNamespace,
    resumed_build,
    stage_oids_before: tuple[int, int],
):
    """Interrupt after the last compact batch and retain exact stages."""
    evidence_statements: list[str] = []
    profile_statements: list[str] = []
    post_batch_interruptions: list[None] = []
    prepare_profile_stages = (
        importer._prepare_provider_directory_profile_stages
    )

    async def tracking_status(sql: Any, **params: Any):
        if _is_evidence_insert(context, sql):
            evidence_statements.append(str(sql))
        if _is_profile_insert(context, sql):
            profile_statements.append(str(sql))
        return await context.original_status(sql, **params)

    async def interrupt_prepare(*args: Any, **params: Any):
        if not post_batch_interruptions:
            post_batch_interruptions.append(None)
            raise RuntimeError("forced post-batch interruption")
        return await prepare_profile_stages(*args, **params)

    context.monkeypatch.setattr(context.database, "status", tracking_status)
    context.monkeypatch.setattr(
        importer,
        "_prepare_provider_directory_profile_stages",
        interrupt_prepare,
    )
    with pytest.raises(RuntimeError, match="forced post-batch interruption"):
        await importer._build_provider_directory_profile_stages(
            replace(resumed_build, owner_run_id="profile-run-last-batch"),
            context.fence,
            context.fence,
            has_existing_artifacts=False,
        )
    checkpoint = await _checkpoint(
        context,
        "state, evidence_next_batch, evidence_total_batches, "
        "profile_next_batch, profile_total_batches",
    )
    _assert_last_batch_checkpoint(
        checkpoint, evidence_statements, profile_statements
    )
    assert await _stage_oids(context) == stage_oids_before
    return prepare_profile_stages


def _assert_completed_checkpoint(
    checkpoint,
    evidence_statements: list[str],
    profile_statements: list[str],
) -> None:
    assert checkpoint is not None
    assert evidence_statements == []
    assert profile_statements == []
    assert checkpoint.state == "ready"
    assert (
        checkpoint.evidence_next_batch
        == checkpoint.evidence_total_batches
    )
    assert (
        checkpoint.profile_next_batch
        == checkpoint.profile_total_batches
    )


def _assert_resume_progress(context: SimpleNamespace) -> None:
    last_batch_progress = _progress_for(
        context,
        "profile-run-last-batch",
        phase=importer._PROFILE_COMPACT_PROGRESS_PHASE,
    )
    assert [progress["done"] for progress in last_batch_progress] == list(
        range(401)
    )
    final_progress = _progress_for(
        context,
        "profile-run-final",
        phase=importer._PROFILE_COMPACT_PROGRESS_PHASE,
    )
    assert [
        (progress["done"], progress["total"])
        for progress in final_progress
    ] == [(400, 400)]


async def _assert_evidence_parity(context: SimpleNamespace) -> None:
    baseline_ref = profile.qualified_table(
        context.schema, "profile_evidence"
    )
    resumed_ref = profile.qualified_table(
        context.schema, context.build.evidence_stage
    )
    difference_count = await context.database.scalar(
        f"""
        SELECT count(*) FROM (
            (SELECT * FROM {baseline_ref}
             EXCEPT ALL SELECT * FROM {resumed_ref})
            UNION ALL
            (SELECT * FROM {resumed_ref}
             EXCEPT ALL SELECT * FROM {baseline_ref})
        ) AS difference;
        """
    )
    assert difference_count == 0


async def _assert_profile_parity(context: SimpleNamespace) -> None:
    baseline_ref = profile.qualified_table(context.schema, "profile")
    resumed_ref = profile.qualified_table(
        context.schema, context.build.profile_stage
    )
    projection = (
        "npi, profile_json, evidence_json, source_ids, "
        "endpoint_ids, dataset_ids, source_count, "
        "independent_source_count, fact_count, generation_id"
    )
    difference_count = await context.database.scalar(
        f"""
        SELECT count(*) FROM (
            (SELECT {projection} FROM {baseline_ref}
             EXCEPT ALL SELECT {projection} FROM {resumed_ref})
            UNION ALL
            (SELECT {projection} FROM {resumed_ref}
             EXCEPT ALL SELECT {projection} FROM {baseline_ref})
        ) AS difference;
        """
    )
    assert difference_count == 0


async def complete_resumed_build(
    context: SimpleNamespace,
    resumed_build,
    prepare_profile_stages,
    interrupted_evidence_count: int,
) -> None:
    """Finish without replaying committed batches and prove data parity."""
    evidence_statements: list[str] = []
    profile_statements: list[str] = []

    async def final_status(sql: Any, **params: Any):
        if _is_evidence_insert(context, sql):
            evidence_statements.append(str(sql))
        if _is_profile_insert(context, sql):
            profile_statements.append(str(sql))
        return await context.original_status(sql, **params)

    context.monkeypatch.setattr(
        importer,
        "_prepare_provider_directory_profile_stages",
        prepare_profile_stages,
    )
    context.monkeypatch.setattr(context.database, "status", final_status)
    await importer._build_provider_directory_profile_stages(
        replace(resumed_build, owner_run_id="profile-run-final"),
        context.fence,
        context.fence,
        has_existing_artifacts=False,
    )
    checkpoint = await _checkpoint(
        context,
        "state, evidence_next_batch, evidence_total_batches, "
        "profile_next_batch, profile_total_batches",
    )
    _assert_completed_checkpoint(
        checkpoint, evidence_statements, profile_statements
    )
    _assert_resume_progress(context)
    evidence_count = int(
        await context.database.scalar(
            f"SELECT count(*) FROM "
            f"{profile.qualified_table(context.schema, context.build.evidence_stage)};"
        )
        or 0
    )
    assert evidence_count >= interrupted_evidence_count
    await _assert_evidence_parity(context)
    await _assert_profile_parity(context)
