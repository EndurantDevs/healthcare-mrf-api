"""Persistence boundary coverage for sealed V4 graph publication."""

from __future__ import annotations

from contextlib import asynccontextmanager
import asyncio
import hashlib
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.ptg_parts import ptg2_shared_snapshot_publish as publication
from tests.test_ptg2_shared_snapshot_publish import (
    _patch_v4_graph_publication,
    _tax_stage_contract,
    _v4_graph_publication_fixture,
)


@pytest.mark.asyncio
async def test_provider_map_and_key_persistence_guards(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Reject a missing COPY driver and nonmonotonic persisted keys."""

    @asynccontextmanager
    async def acquire():
        yield SimpleNamespace(raw_connection=object())

    monkeypatch.setattr(publication.db, "acquire", acquire)
    with pytest.raises(NotImplementedError, match="COPY TO"):
        await publication._export_provider_set_key_map(
            schema_name="mrf",
            snapshot_key=1,
            output_path=tmp_path / "map.copy",
        )

    duplicate_rows = SimpleNamespace(all=lambda: [(1,), (1,)])
    with pytest.raises(RuntimeError, match="persisted dictionary rows changed"):
        await publication._count_v4_target_keys(
            SimpleNamespace(execute=AsyncMock(return_value=duplicate_rows)),
            schema='"mrf"',
            target_table='"target"',
            key_name='"item_key"',
            snapshot_key=1,
            initial_key=0,
            estimated_row_bytes=1,
            heartbeat_callback=None,
        )


@pytest.mark.asyncio
async def test_tax_sidecar_persistence_guards(monkeypatch) -> None:
    """Reject incomplete published sidecar counts and batches."""

    monkeypatch.setattr(
        publication,
        "_count_v4_target_keys",
        AsyncMock(return_value=1),
    )
    with pytest.raises(RuntimeError, match="provider-group tax identity changed"):
        await publication._reject_tax_group_count(
            object(),
            schema='"mrf"',
            snapshot_key=1,
            expected_count=2,
            published_count=2,
            estimated_row_bytes=1,
            heartbeat_callback=None,
        )
    monkeypatch.setattr(
        publication,
        "_v4_tax_group_batch_boundary",
        AsyncMock(return_value=(1, b"g" * 16, 0.01)),
    )
    monkeypatch.setattr(
        publication,
        "_publish_v4_tax_group_batch",
        AsyncMock(return_value=(0, 0.01)),
    )
    with pytest.raises(RuntimeError, match="provider-group tax identity changed"):
        await publication._publish_tax_group_ranges(
            object(),
            schema='"mrf"',
            snapshot_key=1,
            stage='"stage"',
            sizer=publication._V4DictionaryBatchSizer(estimated_row_bytes=1),
            progress_callback=None,
            heartbeat_callback=None,
        )


@pytest.mark.asyncio
async def test_copy_and_taxonomy_scope_guards(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Reject a missing binary COPY driver and changed taxonomy scope."""

    copy_path = tmp_path / "scope.copy"
    copy_path.write_bytes(b"x")
    spec = publication._V4CompilerCopySpec(
        "pg_temp",
        "scope",
        ("npi_key",),
        1,
        hashlib.sha256(b"x").hexdigest(),
        "scope",
    )
    connection = SimpleNamespace(get_raw_connection=AsyncMock(return_value=object()))
    with pytest.raises(NotImplementedError, match="binary COPY"):
        await publication._copy_authenticated_v4_compiler_input(
            SimpleNamespace(connection=AsyncMock(return_value=connection)),
            copy_path,
            spec=spec,
            progress_callback=None,
        )
    monkeypatch.setattr(
        publication,
        "_copy_authenticated_v4_compiler_input",
        AsyncMock(),
    )
    scope = SimpleNamespace(
        copy_path=copy_path,
        manifest={"row_count": 1, "output_byte_count": 1, "output_sha256": "0" * 64},
    )
    mismatch = SimpleNamespace(one=lambda: (0, 0, 0))
    with pytest.raises(RuntimeError, match="NPI scope stage changed"):
        await publication._copy_v4_taxonomy_scope(
            SimpleNamespace(execute=AsyncMock(return_value=mismatch)),
            scope,
            stage_table="scope",
            progress_callback=None,
        )


@pytest.mark.asyncio
async def test_tax_digest_and_sealed_index_guards(monkeypatch) -> None:
    """Reject a drifted tax digest and return an authenticated serving index."""

    contract = _tax_stage_contract()
    monkeypatch.setattr(
        publication,
        "_validate_v4_tax_token_rows",
        AsyncMock(),
    )
    monkeypatch.setattr(
        publication,
        "_validate_v4_tax_group_rows",
        AsyncMock(
            return_value={
                "provider_group_count": contract.provider_group_count,
                "matched_ein": contract.matched_ein_count,
                "missing": contract.missing_count,
                "malformed": contract.malformed_count,
                "unsupported_type": contract.unsupported_type_count,
                "referenced_tax_identity_count": contract.tax_identity_count,
            }
        ),
    )
    with pytest.raises(RuntimeError, match="content digest changed"):
        await publication._validate_v4_tax_identity_stages(
            object(),
            schema='"mrf"',
            group_dictionary_stage="groups",
            tax_identity_stage="tax",
            group_tax_identity_stage="group_tax",
            contract=contract,
            progress_callback=None,
        )

    manifest_result = SimpleNamespace(
        scalar=lambda: {"serving_index": {"provider_graph": {}}}
    )
    session = SimpleNamespace(execute=AsyncMock(return_value=manifest_result))

    @asynccontextmanager
    async def transaction():
        yield session

    monkeypatch.setattr(publication.db, "transaction", transaction)
    assert await publication._sealed_shared_serving_index(
        schema_name="mrf",
        snapshot_key=1,
        expected_generation="shared_blocks_v4",
    ) == {"provider_graph": {}}


def _layout_arguments(**overrides):
    layout_arguments_by_name = {
        "schema_name": "mrf",
        "manifest_stage_table": "manifest",
        "reserved_snapshot_key": 1,
        "build_token": "token",
        "expected_coverage_scope_id": b"c" * 32,
        "logical_snapshot_id": "snapshot",
        "expected_source_identities": (),
        "serving_run_entries": (),
        "code_dictionary_entries": (),
        "provider_set_metadata_entries": (),
        "source_audit_witness_entries": (),
        "expected_raw_source_sha256": (),
        "graph_artifact_entries": (),
        "provider_identifier_quarantine": {},
    }
    layout_arguments_by_name.update(overrides)
    return layout_arguments_by_name


@pytest.mark.asyncio
async def test_layout_resource_and_schema_guards(monkeypatch) -> None:
    """Reject empty V4 acquisition evidence and a foreign schema."""

    with pytest.raises(RuntimeError, match="bytes must be positive"):
        await publication.publish_strict_shared_v3_layout(
            **_layout_arguments(
                provider_graph_v4=True,
                compressed_acquisition_entries=(),
                empty_npi_tin_only_normalization_count=0,
            )
        )
    monkeypatch.setattr(publication, "resolve_ptg2_schema", lambda: "mrf")
    with pytest.raises(RuntimeError, match="configured PostgreSQL schema"):
        await publication.publish_strict_shared_v3_layout(
            **_layout_arguments(schema_name="other")
        )


@pytest.mark.asyncio
async def test_duplicate_price_readiness_fails_closed(monkeypatch, tmp_path) -> None:
    """Reject two readiness callbacks from one price preparation."""

    key = publication.PreparedSharedPriceKeyMap("mrf", "keys", 1)

    async def prepare_price(*, price_key_ready, **_kwargs):
        price_key_ready(key)
        price_key_ready(key)

    monkeypatch.setattr(publication, "prepare_shared_price_artifacts", prepare_price)
    monkeypatch.setattr(
        publication,
        "_export_price_map_and_run_finalizer",
        AsyncMock(return_value=object()),
    )
    with pytest.raises(RuntimeError, match="reported readiness twice"):
        await publication._prepare_price_with_early_finalizer(
            schema_name="mrf",
            manifest_stage_table="manifest",
            price_set_summary_source_count=1,
            raw_work_directory=tmp_path,
            serving_run_entries=(),
            code_dictionary_entries=(),
            provider_set_metadata_entries=(),
            expected_source_identities=(),
        )


@pytest.mark.asyncio
async def test_taxonomy_scope_cancellation_drains_cleanup(monkeypatch) -> None:
    """Settle the shielded cleanup before preserving cancellation."""

    cleanup_started = asyncio.Event()
    cleanup_release = asyncio.Event()

    @asynccontextmanager
    async def boundary():
        yield

    @asynccontextmanager
    async def connect():
        yield SimpleNamespace()

    async def close_scope(*_args):
        cleanup_started.set()
        await cleanup_release.wait()

    session = SimpleNamespace(begin=boundary, execute=AsyncMock())
    monkeypatch.setattr(
        publication.db,
        "engine",
        SimpleNamespace(connect=connect),
    )
    monkeypatch.setattr(
        publication.db,
        "session_factory",
        lambda **_kwargs: session,
    )
    monkeypatch.setattr(publication, "_close_v4_taxonomy_scope", close_scope)

    async def use_scope():
        async with publication._v4_taxonomy_scope_session("scope"):
            assert session.execute.await_count == 2

    task = asyncio.create_task(use_scope())
    await cleanup_started.wait()
    task.cancel()
    cleanup_release.set()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_graph_publication_rejects_selected_layout_drift(
    monkeypatch,
    tmp_path,
) -> None:
    """Reject a compiler summary selecting a different representation."""

    compilation, cas_publication, map_summary = _v4_graph_publication_fixture(tmp_path)
    compilation = SimpleNamespace(**{**vars(compilation), "selected_layout": "direct"})
    publish_maps = _patch_v4_graph_publication(
        monkeypatch,
        cas_publication,
        map_summary,
    )
    with pytest.raises(RuntimeError, match="publication selection changed"):
        await publication._publish_v4_graph(
            compilation,
            publication_context=publication._V4GraphCoordinates(
                schema_name="mrf",
                logical_snapshot_id="synthetic-snapshot",
                snapshot_key=1,
                build_token="token",
            ),
            compressed_acquisition_bytes=1,
            empty_npi_tin_only_normalization_count=0,
        )
    publication.create_shared_block_stage.assert_not_awaited()
    publication.copy_shared_block_binary_file.assert_not_awaited()
    publish_maps.assert_not_awaited()
