"""Publication boundary coverage for sealed V4 graph evidence."""

from __future__ import annotations

from contextlib import asynccontextmanager
from copy import deepcopy
import asyncio
import hashlib
import io
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.ptg_parts import ptg2_shared_snapshot_publish as publication
from tests.test_ptg2_shared_snapshot_publish import (
    _tax_identity_compilation_summary,
    _tax_stage_contract,
)


class _OneResult:
    """Minimal SQL result exposing one row and an all-rows view."""

    def __init__(self, row) -> None:
        self.row = row

    def one(self):
        """Return the configured single row."""

        return self.row

    def all(self):
        """Return the configured row as a bounded sequence."""

        return [self.row]


class _ScalarSession:
    """Return scripted scalar and execute values through awaitable methods."""

    def __init__(self, *, scalars=(), results=()) -> None:
        self.scalars = list(scalars)
        self.results = list(results)

    async def scalar(self, *_args, **_kwargs):
        """Return the next scalar."""

        return self.scalars.pop(0)

    async def execute(self, *_args, **_kwargs):
        """Return the next result."""

        return self.results.pop(0)


def _dictionary_stage(*, dense: bool = True, expected_count: int = 1):
    return publication._V4DenseDictionaryStage(
        stage_table="stage",
        key_name="item_key",
        expected_count=expected_count,
        target_table="target",
        columns=("item_key",),
        value_predicate="TRUE",
        expected_sum=expected_count,
        dense_keys=dense,
    )


def test_publication_scalar_guards_cover_dictionary_and_tax_rows() -> None:
    """Reject malformed dictionary summaries and token identities."""

    assert publication._row_mapping((("field", 3),)) == {"field": 3}
    with pytest.raises(RuntimeError, match="dictionary COPY changed"):
        publication._validated_dense_dictionary_range_sum(
            (0, None, None, False, 0),
            range_start=0,
            range_end=1,
        )
    digest = hashlib.sha256()
    with pytest.raises(RuntimeError, match="tax identity dictionary changed"):
        publication._append_v4_tax_token_rows(
            ((0, b"x" * 16, b"y" * 32),),
            range_start=0,
            content_digest=digest,
        )
    progress_events = []
    reader = publication._V4MeasuredCopyReader(
        io.BytesIO(b"copy"),
        lambda name, amount: progress_events.append((name, amount)),
    )
    assert reader.read() == b"copy"
    assert progress_events == [("copy_bytes", 4)]


@pytest.mark.asyncio
async def test_dense_dictionary_validation_rejects_outliers(monkeypatch) -> None:
    """Reject out-of-range keys and a mismatched dense-stage sum."""

    session = _ScalarSession(scalars=(9,))
    assert await publication._has_out_of_range_dictionary_key(
        session,
        schema='"mrf"',
        stage_table='"stage"',
        key_name='"item_key"',
        expected_count=1,
        heartbeat_callback=None,
    )
    monkeypatch.setattr(
        publication,
        "_validated_v4_dense_dictionary_sum",
        AsyncMock(return_value=0),
    )
    monkeypatch.setattr(
        publication,
        "_has_out_of_range_dictionary_key",
        AsyncMock(return_value=False),
    )
    with pytest.raises(RuntimeError, match="dictionary COPY changed"):
        await publication._validate_v4_dictionary_stage(
            object(),
            schema='"mrf"',
            stage=_dictionary_stage(),
            progress_callback=None,
        )


@pytest.mark.asyncio
async def test_sparse_dictionary_validation_rejects_order_and_totals(
    monkeypatch,
) -> None:
    """Reject an unordered sparse batch and an incomplete terminal total."""

    summaries = AsyncMock(
        side_effect=[
            (_OneResult((1, -1, 0, True, 1)), 0.01),
        ]
    )
    monkeypatch.setattr(publication, "_v4_sparse_dictionary_summary", summaries)
    with pytest.raises(RuntimeError, match="dictionary COPY changed"):
        await publication._validate_v4_sparse_dictionary_stage(
            object(),
            schema='"mrf"',
            stage=_dictionary_stage(dense=False),
            progress_callback=None,
        )

    summaries.side_effect = [
        (_OneResult((0, None, None, True, 0)), 0.01),
    ]
    with pytest.raises(RuntimeError, match="dictionary COPY changed"):
        await publication._validate_v4_sparse_dictionary_stage(
            object(),
            schema='"mrf"',
            stage=_dictionary_stage(dense=False, expected_count=1),
            progress_callback=None,
        )


@pytest.mark.asyncio
async def test_dense_and_sparse_publication_detect_persisted_drift(
    monkeypatch,
) -> None:
    """Reject target rows that differ after either bounded publication path."""

    session = _ScalarSession(scalars=(0,), results=(None,))
    with pytest.raises(RuntimeError, match="persisted dictionary rows changed"):
        await publication._publish_v4_dictionary_stage_ranges(
            session,
            schema='"mrf"',
            snapshot_key=7,
            stage=_dictionary_stage(),
            progress_callback=None,
        )

    monkeypatch.setattr(
        publication,
        "_v4_sparse_batch_boundary",
        AsyncMock(return_value=(1, 3, 0.01)),
    )
    monkeypatch.setattr(
        publication,
        "_publish_v4_sparse_batch",
        AsyncMock(return_value=(0, 0.01)),
    )
    with pytest.raises(RuntimeError, match="persisted dictionary rows changed"):
        await publication._publish_v4_sparse_ranges(
            object(),
            schema='"mrf"',
            snapshot_key=7,
            stage=_dictionary_stage(dense=False),
            progress_callback=None,
        )


@pytest.mark.asyncio
async def test_tax_token_validation_covers_progress_and_count_guards(
    monkeypatch,
) -> None:
    """Authenticate a token batch, then reject short and undercounted batches."""

    contract = _tax_stage_contract()
    token = b"t" * 32
    events = []
    monkeypatch.setattr(
        publication,
        "_v4_tax_token_batch",
        AsyncMock(return_value=(((0, token[:16], token),), 0.01)),
    )
    await publication._validate_v4_tax_token_rows(
        object(),
        schema='"mrf"',
        tax_identity_stage="tax_stage",
        contract=contract,
        content_digest=hashlib.sha256(),
        progress_callback=lambda name, amount: events.append((name, amount)),
    )
    assert events == [
        ("validated_dictionary_rows", 1),
        ("publish_batches", 1),
    ]

    publication._v4_tax_token_batch.return_value = ((), 0.01)
    with pytest.raises(RuntimeError, match="tax identity dictionary changed"):
        await publication._validate_v4_tax_token_rows(
            object(),
            schema='"mrf"',
            tax_identity_stage="tax_stage",
            contract=contract,
            content_digest=hashlib.sha256(),
            progress_callback=None,
        )
    publication._v4_tax_token_batch.return_value = (
        ((0, token[:16], token),),
        0.01,
    )
    monkeypatch.setattr(publication, "_append_v4_tax_token_rows", lambda *_a, **_k: 0)
    with pytest.raises(RuntimeError, match="tax identity dictionary changed"):
        await publication._validate_v4_tax_token_rows(
            object(),
            schema='"mrf"',
            tax_identity_stage="tax_stage",
            contract=contract,
            content_digest=hashlib.sha256(),
            progress_callback=None,
        )


def test_tax_contract_rejects_duplicate_source_map_and_shape_drift() -> None:
    """Reject duplicate source ordinals and source-count disagreement."""

    compilation = _tax_identity_compilation_summary()
    tax_summary = compilation.summary["tax_identity"]
    duplicate = deepcopy(tax_summary)
    duplicate["source_ordinal_map"] = [
        {"shard_id": "shard-a", "ordinal": 0},
        {"shard_id": "shard-a", "ordinal": 1},
    ]
    with pytest.raises(RuntimeError, match="source map changed"):
        publication._v4_tax_contract_header(duplicate)

    monkey_summary = deepcopy(tax_summary)
    monkey_summary["source_shard_count"] = 2
    monkey_summary["source_bitmap_bytes"] = 1
    with pytest.raises(RuntimeError, match="source shape changed"):
        publication._validated_v4_tax_identity_contract(
            SimpleNamespace(
                summary={"tax_identity": monkey_summary},
                observe={"group_count": 4},
            )
        )


@pytest.mark.asyncio
async def test_taxonomy_scope_requires_database_after_connect(monkeypatch) -> None:
    """Fail closed when database initialization yields no taxonomy session."""

    monkeypatch.setattr(publication.db, "engine", None)
    monkeypatch.setattr(publication.db, "session_factory", None)
    monkeypatch.setattr(publication.db, "connect", AsyncMock())
    with pytest.raises(RuntimeError, match="taxonomy database is unavailable"):
        async with publication._v4_taxonomy_scope_session("scope"):
            raise AssertionError("unreachable")
    publication.db.connect.assert_awaited_once()


class _ClosingSession:
    """Record taxonomy-scope rollback, drop, and close operations."""

    def __init__(self, *, fail_drop: bool = False) -> None:
        self.fail_drop = fail_drop
        self.rolled_back = False
        self.closed = False

    def in_transaction(self):
        """Report one active transaction."""

        return True

    async def rollback(self):
        """Record rollback."""

        self.rolled_back = True

    @asynccontextmanager
    async def begin(self):
        """Provide a transaction boundary."""

        yield

    async def execute(self, _statement):
        """Optionally fail the TEMP-table drop."""

        if self.fail_drop:
            raise RuntimeError("drop failed")

    async def close(self):
        """Record session closure."""

        self.closed = True


@pytest.mark.asyncio
async def test_taxonomy_scope_cleanup_rolls_back_or_invalidates() -> None:
    """Drop a normal scope and invalidate the connection after drop failure."""

    session = _ClosingSession()
    connection = SimpleNamespace(invalidate=AsyncMock())
    await publication._close_v4_taxonomy_scope(session, connection, '"scope"')
    assert session.rolled_back and session.closed
    connection.invalidate.assert_not_awaited()

    failing = _ClosingSession(fail_drop=True)
    invalidated = SimpleNamespace(invalidate=AsyncMock())
    with pytest.raises(RuntimeError, match="drop failed"):
        await publication._close_v4_taxonomy_scope(
            failing,
            invalidated,
            '"scope"',
        )
    assert failing.closed
    invalidated.invalidate.assert_awaited_once()


def test_taxonomy_progress_adapters_and_input_guard() -> None:
    """Adapt COPY progress and reject malformed prepared taxonomy rules."""

    events = []
    callback = publication._v4_taxonomy_copy_progress(
        lambda stage, counters: events.append((stage, counters))
    )
    assert callback is not None
    callback("copy_bytes", 4)
    assert events == [
        ("taxonomy input preparation", {"copy_bytes": 4}),
    ]
    assert publication._v4_taxonomy_copy_progress(None) is None
    with pytest.raises(RuntimeError, match="preparation rules changed"):
        publication._taxonomy_input_complete(
            {"members": {"byte_count": 0}, "rules": "bad"},
            lambda *_args: None,
        )
    publication._taxonomy_copy_complete(SimpleNamespace(), None)


@pytest.mark.asyncio
async def test_graph_wait_heartbeats_and_resource_guards(monkeypatch) -> None:
    """Touch the build after a timeout and reject invalid resource evidence."""

    task = asyncio.create_task(asyncio.sleep(0, result="compiled"))
    should_complete_by_attempt = iter((False, True))

    async def fake_wait_for(_awaitable, *, timeout):
        if not next(should_complete_by_attempt):
            raise TimeoutError
        return "compiled"

    touch = AsyncMock()
    monkeypatch.setattr(publication.asyncio, "wait_for", fake_wait_for)
    assert await publication._wait_for_v4_graph_compilation(task, touch) == "compiled"
    touch.assert_awaited_once()
    await task

    compilation = SimpleNamespace()
    with pytest.raises(RuntimeError, match="compressed acquisition"):
        await publication._publish_v4_dictionaries_and_maps(
            compilation,
            publication_context=publication._V4AtomicPublishContext(
                schema_name="mrf",
                block_stage="ptg2_v3_block_stage_exact",
                snapshot_key=1,
                build_token="token",
            ),
            compressed_acquisition_bytes=0,
            empty_npi_tin_only_normalization_count=0,
        )
    with pytest.raises(RuntimeError, match="normalization count"):
        await publication._publish_v4_dictionaries_and_maps(
            compilation,
            publication_context=publication._V4AtomicPublishContext(
                schema_name="mrf",
                block_stage="ptg2_v3_block_stage_exact",
                snapshot_key=1,
                build_token="token",
            ),
            compressed_acquisition_bytes=1,
            empty_npi_tin_only_normalization_count=-1,
        )
