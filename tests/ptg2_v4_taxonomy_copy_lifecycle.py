# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Transient compiler COPY lifecycle proof shared by PostgreSQL tests."""

from __future__ import annotations

import asyncio
import hashlib
from pathlib import Path
from typing import Any

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncEngine

from process.ptg_parts import ptg2_shared_snapshot_publish as shared_publish
from process.ptg_parts import ptg2_v4_snapshot_maps as snapshot_maps
from process.ptg_parts import ptg2_v4_taxonomy_candidates as candidates
from tests import ptg2_v4_taxonomy_copy_support as support


async def _publish_compiler_stage(
    session: Any,
    *,
    schema_name: str,
    stage_table: str,
) -> Any:
    publication = await candidates.publish_v4_taxonomy_stage(
        session,
        schema_name=schema_name,
        snapshot_key=11,
        build_token="prepared-copy-postgres",
        stage_table=stage_table,
        rules=support.compiler_rules(),
        npi_count=(candidates.PTG2_V4_MAX_ONLINE_INFERRED_TAXONOMY_CANDIDATES + 1),
        pattern_count=0,
    )
    assert publication.rule_count == 5
    assert publication.observe_only_rule_count == 5
    return publication


async def _assert_bad_digest_cleanup(
    engine: AsyncEngine,
    *,
    schema_name: str,
    copy_path: Path,
    byte_count: int,
    baseline: dict[str, int],
) -> None:
    with pytest.raises(RuntimeError, match="COPY changed"):
        async with engine.begin() as connection:
            await candidates.stage_v4_taxonomy_copy(
                connection,
                copy_path=copy_path,
                expected_byte_count=byte_count,
                expected_sha256="0" * 64,
            )
    assert await support.runtime_inventory(engine, schema_name) == baseline


async def _assert_cancellation_cleanup(
    engine: AsyncEngine,
    *,
    schema_name: str,
    copy_path: Path,
    byte_count: int,
    copy_sha256: str,
    baseline: dict[str, int],
) -> None:
    with pytest.raises(asyncio.CancelledError):
        async with engine.begin() as connection:
            async with candidates.managed_v4_taxonomy_copy_stage(
                connection,
                copy_path=copy_path,
                expected_byte_count=byte_count,
                expected_sha256=copy_sha256,
            ):
                stage_count, stage_bytes = await support.current_stage_inventory(
                    connection
                )
                assert stage_count > 0
                assert stage_bytes > 0
                raise asyncio.CancelledError
    assert await support.runtime_inventory(engine, schema_name) == baseline


async def _assert_stage_is_session_private(
    engine: AsyncEngine,
    stage_table: str,
) -> None:
    async with engine.connect() as other_connection:
        resolved = await other_connection.scalar(
            sa.text("SELECT to_regclass(:relation_name)"),
            {"relation_name": f"pg_temp.{stage_table}"},
        )
    assert resolved is None


async def _assert_connection_close_cleanup(
    engine: AsyncEngine,
    *,
    schema_name: str,
    copy_path: Path,
    byte_count: int,
    copy_sha256: str,
    baseline: dict[str, int],
) -> None:
    connection = await engine.connect()
    try:
        await connection.begin()
        stage = await candidates.stage_v4_taxonomy_copy(
            connection,
            copy_path=copy_path,
            expected_byte_count=byte_count,
            expected_sha256=copy_sha256,
        )
        await _assert_stage_is_session_private(engine, stage.table_name)
    finally:
        await connection.close()
    assert await support.runtime_inventory(engine, schema_name) == baseline


async def _assert_production_failure_cleanup(
    engine: AsyncEngine,
    *,
    schema_name: str,
    tmp_path: Path,
    baseline: dict[str, int],
) -> None:
    failure_directory = tmp_path / "failed-scope"
    failure_directory.mkdir(mode=0o700)
    scope = support.npi_scope_preparation(
        failure_directory,
        sha256_override="0" * 64,
    )
    support.ObservedScopeSession.observations = []
    with pytest.raises(RuntimeError, match="NPI scope prepass"):
        await shared_publish._prepare_v4_taxonomy_compiler_input(
            scope,
            schema_name=schema_name,
            work_directory=failure_directory,
            progress_callback=None,
        )
    support.assert_scope_observations()
    assert await support.runtime_inventory(engine, schema_name) == baseline


async def _assert_production_cancel_cleanup(
    engine: AsyncEngine,
    *,
    schema_name: str,
    monkeypatch: Any,
    tmp_path: Path,
    baseline: dict[str, int],
) -> None:
    cancel_directory = tmp_path / "canceled-scope"
    cancel_directory.mkdir(mode=0o700)
    scope = support.npi_scope_preparation(cancel_directory)

    async def cancel_preparation(*_args: Any, **_kwargs: Any) -> None:
        raise asyncio.CancelledError

    support.ObservedScopeSession.observations = []
    with monkeypatch.context() as cancellation_patch:
        cancellation_patch.setattr(
            candidates,
            "prepare_v4_inferred_taxonomy_compiler_input",
            cancel_preparation,
        )
        with pytest.raises(asyncio.CancelledError):
            await shared_publish._prepare_v4_taxonomy_compiler_input(
                scope,
                schema_name=schema_name,
                work_directory=cancel_directory,
                progress_callback=None,
            )
    support.assert_scope_observations()
    assert await support.runtime_inventory(engine, schema_name) == baseline


async def _prepare_selected_copy(
    engine: AsyncEngine,
    *,
    schema_name: str,
    monkeypatch: Any,
    tmp_path: Path,
) -> support.PreparedTaxonomyCopy:
    baseline = await support.runtime_inventory(engine, schema_name)
    prepared = await support.prepare_real_compiler_input(
        engine,
        schema_name=schema_name,
        work_directory=tmp_path,
        monkeypatch=monkeypatch,
    )
    assert await support.runtime_inventory(engine, schema_name) == baseline
    await _assert_production_failure_cleanup(
        engine,
        schema_name=schema_name,
        tmp_path=tmp_path,
        baseline=baseline,
    )
    await _assert_production_cancel_cleanup(
        engine,
        schema_name=schema_name,
        monkeypatch=monkeypatch,
        tmp_path=tmp_path,
        baseline=baseline,
    )
    member_path = tmp_path / "v4-inferred-taxonomy-members.u32le"
    projection_rows = support.prepared_projection_rows(
        prepared,
        member_path.read_bytes(),
    )
    support.assert_prepared_selection(prepared, projection_rows)
    copy_bytes = support.binary_compiler_copy(projection_rows)
    copy_path = tmp_path / "selected-taxonomy.copy"
    copy_path.write_bytes(copy_bytes)
    copy_path.chmod(0o600)
    return support.PreparedTaxonomyCopy(
        manifest=prepared,
        rules=support.compiler_rules(),
        copy_path=copy_path,
        copy_bytes=copy_bytes,
        copy_sha256=hashlib.sha256(copy_bytes).hexdigest(),
    )


async def _assert_selected_cleanup_paths(
    engine: AsyncEngine,
    *,
    schema_name: str,
    selected_copy: support.PreparedTaxonomyCopy,
    baseline: dict[str, int],
) -> None:
    byte_count = len(selected_copy.copy_bytes)
    await _assert_bad_digest_cleanup(
        engine,
        schema_name=schema_name,
        copy_path=selected_copy.copy_path,
        byte_count=byte_count,
        baseline=baseline,
    )
    for cleanup_assertion in (
        _assert_connection_close_cleanup,
        _assert_cancellation_cleanup,
    ):
        await cleanup_assertion(
            engine,
            schema_name=schema_name,
            copy_path=selected_copy.copy_path,
            byte_count=byte_count,
            copy_sha256=selected_copy.copy_sha256,
            baseline=baseline,
        )


async def assert_prepared_copy_postgres_lifecycle(
    engine: AsyncEngine,
    schema_name: str,
    monkeypatch: Any,
    tmp_path: Path,
) -> None:
    """Prove production preparation plus transient selected-stage cleanup."""

    tmp_path.chmod(0o700)

    async def no_op_map_lock(*_args: Any, **_kwargs: Any) -> None:
        return None

    monkeypatch.setattr(
        snapshot_maps,
        "lock_v4_shared_layout_for_map_write",
        no_op_map_lock,
    )
    await support.create_prepared_catalog(engine, schema_name)
    selected_copy = await _prepare_selected_copy(
        engine,
        schema_name=schema_name,
        monkeypatch=monkeypatch,
        tmp_path=tmp_path,
    )
    baseline = await support.runtime_inventory(engine, schema_name)
    async with engine.begin() as connection:
        async with candidates.managed_v4_taxonomy_copy_stage(
            connection,
            copy_path=selected_copy.copy_path,
            expected_byte_count=len(selected_copy.copy_bytes),
            expected_sha256=selected_copy.copy_sha256,
        ) as stage:
            await _assert_stage_is_session_private(engine, stage.table_name)
            await _publish_compiler_stage(
                connection,
                schema_name=schema_name,
                stage_table=stage.table_name,
            )
    published = await support.runtime_inventory(engine, schema_name)
    assert published == {
        **baseline,
        "candidate_count": baseline["candidate_count"] + 10,
    }
    await _assert_selected_cleanup_paths(
        engine,
        schema_name=schema_name,
        selected_copy=selected_copy,
        baseline=published,
    )
