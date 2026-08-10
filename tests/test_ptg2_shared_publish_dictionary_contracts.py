from __future__ import annotations

import asyncio
import hashlib
import importlib
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process.ptg_parts import ptg2_manifest_publish
from process.ptg_parts.domain import PTG2FileProcessResult
from process.ptg_parts.ptg2_shared_publish import (
    _SHARED_BLOCK_STAGE_COLUMNS,
    _upsert_shared_block_mappings,
    create_shared_block_stage,
    publish_shared_block_stage,
    publish_shared_finalizer_dictionaries,
    shared_block_stage_name,
)
from process.ptg_parts import ptg2_shared_publish
from process.ptg_parts.ptg2_shared_reuse import SharedPhysicalArtifactIdentity
from process.ptg_parts.ptg2_shared_finalize import PTG2_V3_SERVING_RUN_RECORD_BYTES

process_ptg = importlib.import_module("process.ptg")
from tests.ptg2_shared_publish_test_support import (
    _FirstBatchProgress,
    _OneRowResult,
    _RowsResult,
    _SlowSharedBlockSQLDriver,
    _SlowV4CASSQLDriver,
    _assert_shared_stage_sql,
    _assert_slow_shared_block_publication,
    _assert_slow_v4_cas_publication,
    _bounded_stage_session,
    _copy_connection,
    _dictionary_summary,
    _finalizer_contract,
    _provider_set_metadata_entries,
    _serving_run_entries,
    _session_transaction,
    _unannotated_file_result,
)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "observed_code",
    [
        (2, 2, 2, 1),
        (2, 2, 1, 0),
    ],
    ids=["mixed-scopes", "wrong-scope"],
)
async def test_finalizer_dictionary_rejects_scope_mismatch_before_code_insert(
    tmp_path,
    monkeypatch,
    observed_code,
):
    session = SimpleNamespace(
        execute=AsyncMock(
            side_effect=[_OneRowResult((7,)), _OneRowResult(observed_code)]
        ),
        scalar=AsyncMock(side_effect=[2, 2, False]),
    )

    @asynccontextmanager
    async def transaction():
        yield session

    monkeypatch.setattr(ptg2_shared_publish.db, "transaction", transaction)
    monkeypatch.setattr(ptg2_shared_publish.db, "status", AsyncMock())
    monkeypatch.setattr(
        ptg2_shared_publish,
        "_copy_binary_file_to_stage",
        AsyncMock(),
    )
    monkeypatch.setattr(
        ptg2_shared_publish,
        "_copy_text_file_to_stage",
        AsyncMock(),
    )
    with pytest.raises(RuntimeError, match="coverage scope does not match"):
        await publish_shared_finalizer_dictionaries(
            _dictionary_summary(tmp_path, row_count=2),
            schema_name="mrf",
            snapshot_key=7,
            build_token="attempt-7",
            expected_coverage_scope_id=b"e" * 32,
            provider_set_metadata_entries=_provider_set_metadata_entries(
                tmp_path,
                row_count=2,
            ),
        )
    statements = [str(call.args[0]) for call in session.execute.await_args_list]
    assert len(statements) == 2
    assert "FOR KEY SHARE" in statements[0]
    assert "build_token = :build_token" in statements[0]
    assert session.execute.await_args_list[0].args[1]["build_token"] == "attempt-7"
    assert "COUNT(DISTINCT coverage_scope_id)" in statements[1]
    assert not any("INSERT INTO \"mrf\".ptg2_v3_code" in sql for sql in statements)
    assert session.execute.await_args.args[1] == {
        "expected_coverage_scope_id": b"e" * 32
    }


@pytest.mark.asyncio
async def test_finalizer_dictionary_preserves_empty_scope_semantics(tmp_path, monkeypatch):
    session = SimpleNamespace(
        execute=AsyncMock(
            side_effect=[
                _OneRowResult((7,)),
                _OneRowResult((0, 0, 0, 0)),
                _OneRowResult((0,)),
                _OneRowResult((0,)),
            ]
        ),
        scalar=AsyncMock(return_value=0),
    )

    @asynccontextmanager
    async def transaction():
        yield session

    monkeypatch.setattr(ptg2_shared_publish.db, "transaction", transaction)
    monkeypatch.setattr(ptg2_shared_publish.db, "status", AsyncMock())
    monkeypatch.setattr(
        ptg2_shared_publish,
        "_copy_binary_file_to_stage",
        AsyncMock(),
    )
    publication = await publish_shared_finalizer_dictionaries(
        _dictionary_summary(tmp_path, row_count=0),
        schema_name="mrf",
        snapshot_key=7,
        build_token="attempt-7",
        expected_coverage_scope_id=b"e" * 32,
        provider_set_metadata_entries=(),
    )

    assert publication.code_count == 0
    assert publication.serving_rate_count == 0


@pytest.mark.asyncio
async def test_finalizer_provider_metadata_join_decodes_the_smaller_stage(
    tmp_path,
    monkeypatch,
):
    status = AsyncMock()
    session = SimpleNamespace(
        execute=AsyncMock(
            side_effect=[
                _OneRowResult((7,)),
                _OneRowResult((1, 1, 1, 1)),
                None,
                None,
                None,
            ]
        ),
        scalar=AsyncMock(side_effect=[1, 1, False, False]),
    )

    @asynccontextmanager
    async def transaction():
        yield session

    monkeypatch.setattr(ptg2_shared_publish.db, "transaction", transaction)
    monkeypatch.setattr(ptg2_shared_publish.db, "status", status)
    monkeypatch.setattr(
        ptg2_shared_publish,
        "_copy_binary_file_to_stage",
        AsyncMock(),
    )
    monkeypatch.setattr(
        ptg2_shared_publish,
        "_copy_text_file_to_stage",
        AsyncMock(),
    )

    publication = await publish_shared_finalizer_dictionaries(
        _dictionary_summary(tmp_path, row_count=1),
        schema_name="mrf",
        snapshot_key=7,
        build_token="attempt-7",
        expected_coverage_scope_id=b"e" * 32,
        provider_set_metadata_entries=_provider_set_metadata_entries(tmp_path),
    )

    executed_statements = [str(call.args[0]) for call in session.execute.await_args_list]
    provider_metadata_sql = "\n".join(executed_statements[2:4])
    assert "decode(metadata.provider_set_global_id_128, 'hex')" in provider_metadata_sql
    assert "encode(provider_stage.provider_set_global_id_128, 'hex')" not in provider_metadata_sql
    stage_index_sql = "\n".join(str(call.args[0]) for call in status.await_args_list)
    assert "CREATE UNIQUE INDEX" in stage_index_sql
    assert "((decode(provider_set_global_id_128, 'hex')))" in stage_index_sql
    assert stage_index_sql.count("ANALYZE") == 2
    assert publication.provider_set_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("observed_code", "observed_scalars", "message"),
    (
        ((1, 2, 1, 1), (2, 2, False), "code dictionary row count changed"),
        ((2, 1, 1, 2), (2, 2, False), "rate counts do not preserve"),
        ((2, 2, 1, 2), (1, 2, False), "provider dictionary row count changed"),
        ((2, 2, 1, 2), (2, 1, False), "metadata row count changed"),
        ((2, 2, 1, 2), (2, 2, True), "conflicting network names"),
    ),
    ids=(
        "code-count",
        "rate-count",
        "provider-count",
        "metadata-count",
        "metadata-conflict",
    ),
)
async def test_finalizer_dictionary_rejects_post_copy_count_mismatches(
    tmp_path,
    monkeypatch,
    observed_code,
    observed_scalars,
    message,
):
    session = SimpleNamespace(
        execute=AsyncMock(
            side_effect=[_OneRowResult((7,)), _OneRowResult(observed_code)]
        ),
        scalar=AsyncMock(side_effect=observed_scalars),
    )

    @asynccontextmanager
    async def transaction():
        yield session

    monkeypatch.setattr(ptg2_shared_publish.db, "transaction", transaction)
    monkeypatch.setattr(ptg2_shared_publish.db, "status", AsyncMock())
    monkeypatch.setattr(
        ptg2_shared_publish,
        "_copy_binary_file_to_stage",
        AsyncMock(),
    )
    monkeypatch.setattr(
        ptg2_shared_publish,
        "_copy_text_file_to_stage",
        AsyncMock(),
    )

    with pytest.raises(RuntimeError, match=message):
        await publish_shared_finalizer_dictionaries(
            _dictionary_summary(tmp_path, row_count=2),
            schema_name="mrf",
            snapshot_key=7,
            build_token="attempt-7",
            expected_coverage_scope_id=b"e" * 32,
            provider_set_metadata_entries=_provider_set_metadata_entries(
                tmp_path,
                row_count=2,
            ),
        )


@pytest.mark.asyncio
async def test_finalizer_dictionary_rejects_scope_rows_for_empty_dictionary(
    tmp_path,
    monkeypatch,
):
    session = SimpleNamespace(
        execute=AsyncMock(
            side_effect=[
                _OneRowResult((7,)),
                _OneRowResult((0, 0, 1, 0)),
            ]
        ),
        scalar=AsyncMock(return_value=0),
    )

    @asynccontextmanager
    async def transaction():
        yield session

    monkeypatch.setattr(ptg2_shared_publish.db, "transaction", transaction)
    monkeypatch.setattr(ptg2_shared_publish.db, "status", AsyncMock())
    monkeypatch.setattr(
        ptg2_shared_publish,
        "_copy_binary_file_to_stage",
        AsyncMock(),
    )

    with pytest.raises(RuntimeError, match="empty code dictionary has scope rows"):
        await publish_shared_finalizer_dictionaries(
            _dictionary_summary(tmp_path, row_count=0),
            schema_name="mrf",
            snapshot_key=7,
            build_token="attempt-7",
            expected_coverage_scope_id=b"e" * 32,
            provider_set_metadata_entries=(),
        )


@pytest.mark.asyncio
async def test_finalizer_dictionary_rejects_unmatched_provider_metadata(
    tmp_path,
    monkeypatch,
):
    session = SimpleNamespace(
        execute=AsyncMock(
            side_effect=[
                _OneRowResult((7,)),
                _OneRowResult((1, 1, 1, 1)),
                None,
            ]
        ),
        scalar=AsyncMock(side_effect=[1, 1, False, True]),
    )

    @asynccontextmanager
    async def transaction():
        yield session

    monkeypatch.setattr(ptg2_shared_publish.db, "transaction", transaction)
    monkeypatch.setattr(ptg2_shared_publish.db, "status", AsyncMock())
    monkeypatch.setattr(
        ptg2_shared_publish,
        "_copy_binary_file_to_stage",
        AsyncMock(),
    )
    monkeypatch.setattr(
        ptg2_shared_publish,
        "_copy_text_file_to_stage",
        AsyncMock(),
    )

    with pytest.raises(RuntimeError, match="does not exactly cover"):
        await publish_shared_finalizer_dictionaries(
            _dictionary_summary(tmp_path, row_count=1),
            schema_name="mrf",
            snapshot_key=7,
            build_token="attempt-7",
            expected_coverage_scope_id=b"e" * 32,
            provider_set_metadata_entries=_provider_set_metadata_entries(tmp_path),
        )
