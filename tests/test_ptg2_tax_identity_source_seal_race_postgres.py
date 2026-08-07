# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PostgreSQL race proofs for the final source-evidence seal fence."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
import os
from typing import Any
import uuid

import pytest
import sqlalchemy as sa

from db.connection import Database
from process.ptg_parts import ptg2_shared_snapshot_publish as snapshot_publish
from process.ptg_parts import ptg2_v4_snapshot_maps as snapshot_maps
from process.ptg_parts.ptg2_tax_identity_source_projection import (
    TaxIdentitySourceProjectionError,
)
from tests.test_ptg2_v4_postgres_e2e import (
    _base_layout_manifest,
    _bind_source_local_database,
    _compile_source_local_tax_fixture,
    _prepare_source_local_layout,
    _quoted,
)


@dataclass(frozen=True)
class _PublishedSourceLayout:
    database: Database
    schema_name: str
    schema: str
    snapshot_key: int
    build_token: str
    publication: Any
    layout_manifest: dict[str, object]


def _source_layout_manifest(publication: Any) -> dict[str, object]:
    layout_manifest = _base_layout_manifest(dict(publication.adaptive_layout))
    provider_graph = layout_manifest["serving_index"]["provider_graph"]
    provider_graph["provider_tax_identity"] = dict(publication.provider_tax_identity)
    provider_graph["provider_tax_identity_source"] = dict(
        publication.provider_tax_identity_source
    )
    return layout_manifest


@asynccontextmanager
async def _published_source_layout(tmp_path, monkeypatch):
    if os.getenv("HLTHPRT_PTG2_V4_MAP_POSTGRES_TEST") != "1":
        pytest.skip("set HLTHPRT_PTG2_V4_MAP_POSTGRES_TEST=1 for PostgreSQL E2E")
    fixture = await _compile_source_local_tax_fixture(tmp_path)
    schema_name = f"ptg2_v4_tax_source_seal_{uuid.uuid4().hex}"
    schema = _quoted(schema_name)
    database = Database()
    await database.connect()
    _bind_source_local_database(monkeypatch, database)
    try:
        reservation, build_token = await _prepare_source_local_layout(
            database,
            fixture=fixture,
            schema_name=schema_name,
            monkeypatch=monkeypatch,
        )
        publication = await snapshot_publish._publish_v4_graph(
            fixture.compilation,
            publication_context=snapshot_publish._V4GraphCoordinates(
                schema_name=schema_name,
                logical_snapshot_id="synthetic-snapshot",
                snapshot_key=reservation.snapshot_key,
                build_token=build_token,
            ),
            compressed_acquisition_bytes=1024,
            empty_npi_tin_only_normalization_count=0,
            tax_identity_source_artifacts=fixture.tax_sources,
        )
        yield _PublishedSourceLayout(
            database=database,
            schema_name=schema_name,
            schema=schema,
            snapshot_key=reservation.snapshot_key,
            build_token=build_token,
            publication=publication,
            layout_manifest=_source_layout_manifest(publication),
        )
    finally:
        fixture.compilation.cleanup()
        try:
            await database.execute_ddl(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        finally:
            await database.disconnect()


async def _insert_extra_binding(session, layout: _PublishedSourceLayout) -> None:
    await session.execute(
        sa.text(f"""
            INSERT INTO {layout.schema}.ptg2_provider_tax_identity_source_binding
                (snapshot_key, source_key, source_type, identity_kind,
                 identity_sha256, token_policy_id,
                 token_policy_descriptor_sha256, record_format,
                 format_version, record_bytes, artifact_sha256,
                 artifact_byte_count, provider_group_count,
                 matched_ein_count, missing_count, malformed_count,
                 unsupported_type_count)
            SELECT manifest.snapshot_key, 2, 'in_network',
                   'logical_json_sha256_v1', repeat('f', 64),
                   manifest.token_policy_id,
                   manifest.token_policy_descriptor_sha256,
                   'ptg2_provider_group_tax_identity_v1', 1, 65,
                   decode(repeat('ab', 32), 'hex'),
                   13 + octet_length(manifest.token_policy_id),
                   0, 0, 0, 0, 0
              FROM {layout.schema}.
                   ptg2_provider_tax_identity_source_manifest AS manifest
             WHERE manifest.snapshot_key = :snapshot_key
            """),
        {"snapshot_key": layout.snapshot_key},
    )


async def _seal(
    layout: _PublishedSourceLayout,
    *,
    layout_manifest: dict[str, object] | None = None,
):
    async with layout.database.transaction() as session:
        return await snapshot_maps.seal_v4_shared_layout(
            session,
            schema_name=layout.schema_name,
            snapshot_key=layout.snapshot_key,
            build_token=layout.build_token,
            expected_summary=layout.publication.map_summary,
            support_digest=layout.publication.support_digest,
            layout_manifest=layout_manifest or layout.layout_manifest,
        )


async def _layout_state(layout: _PublishedSourceLayout) -> tuple[str, str, int]:
    async with layout.database.transaction() as session:
        state = (
            await session.execute(
                sa.text(f"""
                    SELECT layout.state, root.state,
                           (SELECT COUNT(*)::integer
                              FROM {layout.schema}.
                                   ptg2_provider_tax_identity_source_binding
                             WHERE snapshot_key = :snapshot_key)
                      FROM {layout.schema}.ptg2_v3_snapshot_layout AS layout
                      JOIN {layout.schema}.ptg2_v4_snapshot_map_root AS root
                        ON root.snapshot_key = layout.snapshot_key
                     WHERE layout.snapshot_key = :snapshot_key
                    """),
                {"snapshot_key": layout.snapshot_key},
            )
        ).one()
    return str(state[0]), str(state[1]), int(state[2])


async def _wait_for_database_lock(database: Database, backend_pid: int) -> None:
    for _attempt in range(100):
        async with database.transaction() as session:
            wait_event_type = await session.scalar(
                sa.text(
                    "SELECT wait_event_type FROM pg_stat_activity WHERE pid = :pid"
                ),
                {"pid": backend_pid},
            )
        if wait_event_type == "Lock":
            return
        await asyncio.sleep(0.01)
    raise AssertionError("competing source writer did not wait on the seal lock")


async def _insert_while_sealing(
    layout: _PublishedSourceLayout,
    backend_pid: asyncio.Future[int],
) -> sa.exc.DBAPIError:
    try:
        async with layout.database.transaction() as contender:
            pid = await contender.scalar(sa.text("SELECT pg_backend_pid()"))
            backend_pid.set_result(int(pid))
            await _insert_extra_binding(contender, layout)
    except sa.exc.DBAPIError as exc:
        return exc
    raise AssertionError("competing source writer entered the sealed generation")


def test_source_seal_metadata_allows_legacy_aggregate_only_manifest() -> None:
    """Pre-source V4 manifests may retain only aggregate tax metadata."""

    manifest_by_field = {
        "serving_index": {
            "provider_graph": {"provider_tax_identity": {"contract": "legacy"}}
        }
    }
    assert snapshot_maps._tax_identity_source_seal_metadata(manifest_by_field) is None


@pytest.mark.parametrize(
    "provider_graph_by_field",
    (
        {"provider_tax_identity_source": {}},
        {"provider_tax_identity_source": {"contract": "source"}},
        {
            "provider_tax_identity_source": {"contract": "source"},
            "provider_tax_identity": {},
        },
    ),
)
def test_source_seal_metadata_rejects_incomplete_present_source(
    provider_graph_by_field: dict[str, object],
) -> None:
    """A present source projection requires both nonempty metadata objects."""

    manifest_by_field = {"serving_index": {"provider_graph": provider_graph_by_field}}
    with pytest.raises(RuntimeError, match="source seal metadata is incomplete"):
        snapshot_maps._tax_identity_source_seal_metadata(manifest_by_field)


@pytest.mark.asyncio
async def test_final_seal_rejects_committed_source_binding_drift(
    tmp_path,
    monkeypatch,
) -> None:
    """A valid extra binding committed before the seal must fail closed."""

    async with _published_source_layout(tmp_path, monkeypatch) as layout:
        async with layout.database.transaction() as session:
            await _insert_extra_binding(session, layout)

        with pytest.raises(TaxIdentitySourceProjectionError):
            await _seal(layout)

        assert await _layout_state(layout) == ("building", "building", 3)


@pytest.mark.asyncio
async def test_final_seal_rejects_omitted_metadata_with_source_rows(
    tmp_path,
    monkeypatch,
) -> None:
    """Omitting source metadata cannot bypass durable source validation."""

    async with _published_source_layout(tmp_path, monkeypatch) as layout:
        async with layout.database.transaction() as session:
            await _insert_extra_binding(session, layout)
        metadata_free_manifest = _base_layout_manifest(
            dict(layout.publication.adaptive_layout)
        )

        with pytest.raises(TaxIdentitySourceProjectionError):
            await _seal(layout, layout_manifest=metadata_free_manifest)

        assert await _layout_state(layout) == ("building", "building", 3)


@pytest.mark.asyncio
async def test_final_seal_fence_blocks_then_rejects_concurrent_insert(
    tmp_path,
    monkeypatch,
) -> None:
    """A writer behind the seal lock cannot enter the completed generation."""

    async with _published_source_layout(tmp_path, monkeypatch) as layout:
        seal_locked = asyncio.Event()
        release_validation = asyncio.Event()
        real_validator = snapshot_maps.validate_building_tax_identity_source_projection

        async def paused_validator(*args, **kwargs):
            seal_locked.set()
            await release_validation.wait()
            return await real_validator(*args, **kwargs)

        monkeypatch.setattr(
            snapshot_maps,
            "validate_building_tax_identity_source_projection",
            paused_validator,
        )
        seal_task = asyncio.create_task(_seal(layout))
        await seal_locked.wait()
        backend_pid = asyncio.get_running_loop().create_future()
        writer_task = asyncio.create_task(_insert_while_sealing(layout, backend_pid))
        try:
            await _wait_for_database_lock(layout.database, await backend_pid)
            assert not writer_task.done()
        finally:
            release_validation.set()
        sealed, writer_error = await asyncio.gather(seal_task, writer_task)
        assert sealed.snapshot_key == layout.snapshot_key
        assert "ptg2_provider_tax_identity_source_not_building" in str(writer_error)
        assert await _layout_state(layout) == ("sealed", "complete", 2)
