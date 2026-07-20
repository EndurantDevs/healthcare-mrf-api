from __future__ import annotations

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.ptg_parts import ptg2_shared_publish
from process.ptg_parts.ptg2_shared_publish import SharedBlockStagePublication


class _OneRowResult:
    def __init__(self, row):
        self._row = row

    def one(self):
        return self._row


def _conversion(tmp_path):
    return SimpleNamespace(
        block_copy_path=tmp_path / "graph-blocks.copy",
        owner_copy_path=tmp_path / "graph-owners.copy",
        group_copy_path=tmp_path / "graph-groups.copy",
        npi_copy_path=tmp_path / "graph-npis.copy",
        block_count=4,
        owner_count=2,
        provider_group_count=2,
        npi_count=2,
        support_digest=b"s" * 32,
        raw_block_byte_count=40,
        stored_block_byte_count=30,
    )


def _block_publication(**overrides):
    values_by_field = {
        "object_kinds": ("graph_group_npis_v1",),
        "mapping_count": 4,
        "unique_block_count": 3,
        "logical_byte_count": 40,
        "stored_byte_count": 30,
    }
    values_by_field.update(overrides)
    return SharedBlockStagePublication(**values_by_field)


def _install_graph_publish_mocks(monkeypatch, *, session, block_publication=None):
    @asynccontextmanager
    async def transaction():
        yield session

    create_stage = AsyncMock()
    copy_blocks = AsyncMock()
    publish_blocks = AsyncMock(
        return_value=block_publication or _block_publication()
    )
    copy_rows = AsyncMock()
    status = AsyncMock()
    lock_layout = AsyncMock()
    monkeypatch.setattr(ptg2_shared_publish.db, "transaction", transaction)
    monkeypatch.setattr(ptg2_shared_publish.db, "status", status)
    monkeypatch.setattr(
        ptg2_shared_publish,
        "create_shared_block_stage",
        create_stage,
    )
    monkeypatch.setattr(
        ptg2_shared_publish,
        "copy_shared_block_binary_file",
        copy_blocks,
    )
    monkeypatch.setattr(
        ptg2_shared_publish,
        "_publish_graph_block_stage",
        publish_blocks,
    )
    monkeypatch.setattr(
        ptg2_shared_publish,
        "_copy_binary_file_to_stage",
        copy_rows,
    )
    monkeypatch.setattr(
        ptg2_shared_publish,
        "lock_shared_layout_for_dense_write",
        lock_layout,
    )
    return SimpleNamespace(
        create_stage=create_stage,
        copy_blocks=copy_blocks,
        publish_blocks=publish_blocks,
        copy_rows=copy_rows,
        status=status,
        lock_layout=lock_layout,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("overrides", "message"),
    (
        ({"mapping_count": 3}, "graph block counts changed"),
        ({"logical_byte_count": 39}, "graph block counts changed"),
        ({"stored_byte_count": 29}, "graph block counts changed"),
    ),
    ids=("mapping-count", "logical-bytes", "stored-bytes"),
)
async def test_graph_block_publication_rejects_each_copy_count_mismatch(
    tmp_path,
    monkeypatch,
    overrides,
    message,
):
    conversion = _conversion(tmp_path)
    monkeypatch.setattr(
        ptg2_shared_publish,
        "publish_shared_block_stage",
        AsyncMock(return_value=_block_publication(**overrides)),
    )

    with pytest.raises(RuntimeError, match=message):
        await ptg2_shared_publish._publish_graph_block_stage(
            conversion,
            schema_name="mrf",
            stage_table="ptg2_v3_block_stage_graph",
            snapshot_key=7,
            build_token="build-7",
        )


@pytest.mark.asyncio
async def test_graph_block_publication_accepts_exact_copy_counts(tmp_path, monkeypatch):
    conversion = _conversion(tmp_path)
    expected = _block_publication()
    publish = AsyncMock(return_value=expected)
    monkeypatch.setattr(
        ptg2_shared_publish,
        "publish_shared_block_stage",
        publish,
    )

    observed = await ptg2_shared_publish._publish_graph_block_stage(
        conversion,
        schema_name="mrf",
        stage_table="ptg2_v3_block_stage_graph",
        snapshot_key=7,
        build_token="build-7",
    )

    assert observed == expected
    publish.assert_awaited_once_with(
        schema_name="mrf",
        stage_table="ptg2_v3_block_stage_graph",
        snapshot_key=7,
        build_token="build-7",
    )


@pytest.mark.asyncio
async def test_graph_publish_copies_each_stage_once_and_verifies_durable_counts(
    tmp_path,
    monkeypatch,
):
    conversion = _conversion(tmp_path)
    session = SimpleNamespace(
        execute=AsyncMock(
            side_effect=[
                _OneRowResult((2, 2, 2, 2, 2)),
                None,
                None,
                None,
                _OneRowResult((2, 2, 2)),
            ]
        )
    )
    mocks = _install_graph_publish_mocks(monkeypatch, session=session)

    publication = await ptg2_shared_publish.publish_shared_graph(
        conversion,
        schema_name="mrf",
        snapshot_key=7,
        build_token="build-7",
    )

    assert publication.mapping_count == conversion.block_count
    assert publication.owner_count == conversion.owner_count
    assert publication.provider_group_count == conversion.provider_group_count
    assert publication.npi_count == conversion.npi_count
    assert publication.support_digest == conversion.support_digest
    assert mocks.status.await_count == 4
    assert mocks.copy_rows.await_count == 3
    assert session.execute.await_count == 5
    inserted_sql = "\n".join(
        str(call.args[0]) for call in session.execute.await_args_list[1:4]
    )
    assert inserted_sql.count("INSERT INTO") == 3
    assert "ptg2_v3_graph_owner" in inserted_sql
    assert "ptg2_v3_provider_group" in inserted_sql
    assert "ptg2_v3_npi_scope" in inserted_sql
    cleanup_sql = mocks.status.await_args.args[0]
    assert cleanup_sql.count("DROP TABLE IF EXISTS") == 1
    assert "ptg2_v3_block_stage_" in cleanup_sql


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("observed", "message"),
    (
        ((1, 2, 2, 2, 2), "graph row count changed"),
        ((2, 2, 2, 1, 2), "provider-group dictionary changed"),
        ((2, 2, 2, 2, 1), "provider-group dictionary changed"),
    ),
    ids=("row-count", "duplicate-group-key", "duplicate-group-id"),
)
async def test_graph_publish_rejects_preinsert_stage_mismatches(
    tmp_path,
    monkeypatch,
    observed,
    message,
):
    conversion = _conversion(tmp_path)
    session = SimpleNamespace(execute=AsyncMock(return_value=_OneRowResult(observed)))
    mocks = _install_graph_publish_mocks(monkeypatch, session=session)

    with pytest.raises(RuntimeError, match=message):
        await ptg2_shared_publish.publish_shared_graph(
            conversion,
            schema_name="mrf",
            snapshot_key=7,
            build_token="build-7",
        )

    assert session.execute.await_count == 1
    assert mocks.status.await_count == 4
    cleanup_sql = mocks.status.await_args.args[0]
    assert "DROP TABLE IF EXISTS" in cleanup_sql


@pytest.mark.asyncio
async def test_graph_publish_rejects_postinsert_durable_count_mismatch(
    tmp_path,
    monkeypatch,
):
    conversion = _conversion(tmp_path)
    session = SimpleNamespace(
        execute=AsyncMock(
            side_effect=[
                _OneRowResult((2, 2, 2, 2, 2)),
                None,
                None,
                None,
                _OneRowResult((2, 1, 2)),
            ]
        )
    )
    mocks = _install_graph_publish_mocks(monkeypatch, session=session)

    with pytest.raises(RuntimeError, match="published row count mismatch"):
        await ptg2_shared_publish.publish_shared_graph(
            conversion,
            schema_name="mrf",
            snapshot_key=7,
            build_token="build-7",
        )

    assert session.execute.await_count == 5
    assert mocks.status.await_count == 4


@pytest.mark.asyncio
async def test_binary_stage_copy_passes_the_exact_file_and_contract(
    tmp_path,
    monkeypatch,
):
    path = tmp_path / "graph.copy"
    path.write_bytes(b"binary graph payload")
    copied_by_field = {}

    async def copy_to_table(table, *, source, **kwargs):
        copied_by_field["table"] = table
        copied_by_field["payload"] = source.read()
        copied_by_field["kwargs"] = kwargs

    connection = SimpleNamespace(
        raw_connection=SimpleNamespace(
            driver_connection=SimpleNamespace(copy_to_table=copy_to_table)
        )
    )

    @asynccontextmanager
    async def acquire():
        yield connection

    monkeypatch.setattr(ptg2_shared_publish.db, "acquire", acquire)

    await ptg2_shared_publish._copy_binary_file_to_stage(
        path,
        schema_name="mrf",
        stage_table="ptg2_v3_graph_stage",
        columns=("value",),
    )

    assert copied_by_field == {
        "table": "ptg2_v3_graph_stage",
        "payload": b"binary graph payload",
        "kwargs": {
            "schema_name": "mrf",
            "columns": ["value"],
            "format": "binary",
        },
    }
