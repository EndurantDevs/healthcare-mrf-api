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


def test_shared_block_publish_batch_is_bounded_for_dense_stages():
    synthetic_dense_stage_rows = 8_192_000

    assert ptg2_shared_publish._SHARED_BLOCK_PUBLISH_BATCH_ROWS == 4_096
    assert (
        ptg2_shared_publish._SHARED_BLOCK_PUBLISH_BATCH_ROWS
        <= ptg2_shared_publish._SHARED_BLOCK_EXISTENCE_BATCH_ROWS
    )
    assert (
        synthetic_dense_stage_rows
        // ptg2_shared_publish._SHARED_BLOCK_PUBLISH_BATCH_ROWS
        == 2_000
    )


@pytest.mark.parametrize(
    ("flag_index", "message"),
    (
        (4, "incompatible format version"),
        (5, "no payload or durable CAS row"),
        (6, "stored content metadata"),
        (7, "mapping conflicts"),
    ),
)
def test_batched_stage_summary_rejects_invalid_batch_proof(flag_index, message):
    aggregate_values = [1, 10, 7, ["serving"], False, False, False, False]
    aggregate_values[flag_index] = True
    summary = ptg2_shared_publish._BatchedBlockStageSummary()

    with pytest.raises(RuntimeError, match=message):
        summary.add(
            aggregate_values,
            unique_blocks=1,
            unique_coordinates=1,
        )


@pytest.mark.asyncio
async def test_batched_stage_rejects_a_remaining_gc_candidate():
    """The final candidate intersection is an exact transaction stop gate."""

    session = SimpleNamespace(
        execute=AsyncMock(return_value=_OneRowResult((1,)))
    )

    with pytest.raises(RuntimeError, match="retains a GC candidate"):
        await ptg2_shared_publish._require_no_batched_stage_gc_candidates(
            session,
            schema='"mrf"',
            reuse_hash='"reuse_hashes"',
        )

    statement = str(session.execute.await_args.args[0])
    assert 'FROM "mrf".ptg2_v3_gc_candidate' in statement
    assert 'JOIN "reuse_hashes" AS reused USING (block_hash)' in statement


@pytest.mark.parametrize(
    ("flag_index", "message"),
    (
        (5, "incompatible format version"),
        (6, "no payload"),
        (7, "stored content metadata"),
    ),
)
def test_batched_v4_summary_rejects_invalid_batch_proof(flag_index, message):
    aggregate_values = [1, 2, 10, 7, ["v4_graph"], False, False, False]
    aggregate_values[flag_index] = True
    summary = ptg2_shared_publish._BatchedV4CASStageSummary()

    with pytest.raises(RuntimeError, match=message):
        summary.add(aggregate_values, [1, 10, 7])


@pytest.mark.parametrize(
    ("flag_index", "message"),
    (
        (5, "incompatible format version"),
        (6, "no payload or durable CAS row"),
        (7, "stored content metadata"),
    ),
)
def test_shared_block_batch_rejects_invalid_cas_proof(flag_index, message):
    validation_fields = [1, 1, 1, 1, 1, False, False, False]
    validation_fields[flag_index] = True

    with pytest.raises(RuntimeError, match=message):
        ptg2_shared_publish._validate_shared_block_batch(validation_fields)


@pytest.mark.parametrize(
    ("summary", "message"),
    (
        (
            ptg2_shared_publish._BatchedBlockStageSummary(
                mapping_count=-1,
                coordinate_count=-1,
            ),
            "invalid aggregates",
        ),
        (
            ptg2_shared_publish._BatchedBlockStageSummary(
                mapping_count=1,
                unique_block_count=1,
                coordinate_count=0,
            ),
            "mapping conflicts",
        ),
    ),
)
def test_batched_shared_publication_rejects_invalid_totals(summary, message):
    with pytest.raises(RuntimeError, match=message):
        ptg2_shared_publish._validated_batched_stage_publication(summary)


def test_batched_v4_publication_rejects_invalid_totals():
    summary = ptg2_shared_publish._BatchedV4CASStageSummary(
        staged_row_count=1,
        unique_block_count=2,
    )

    with pytest.raises(RuntimeError, match="invalid aggregates"):
        ptg2_shared_publish._validated_v4_cas_publication(summary)

def test_shared_publish_rejects_invalid_identifiers_and_summary_values():
    invalid_operations = (
        (lambda: ptg2_shared_publish._safe_identifier("mrf;drop"), ValueError, "unsafe"),
        (
            lambda: ptg2_shared_publish._validated_coverage_scope_id("not-bytes"),
            ValueError,
            "exactly 32 bytes",
        ),
        (
            lambda: ptg2_shared_publish._required_summary_mapping(None, "blocks"),
            RuntimeError,
            "missing blocks",
        ),
        (
            lambda: ptg2_shared_publish._required_summary_integer(True, "count"),
            RuntimeError,
            "invalid count",
        ),
        (
            lambda: ptg2_shared_publish._required_summary_integer("bad", "count"),
            RuntimeError,
            "invalid count",
        ),
        (
            lambda: ptg2_shared_publish._required_summary_integer(-1, "count"),
            RuntimeError,
            "negative count",
        ),
    )

    for operation, error_type, message in invalid_operations:
        with pytest.raises(error_type, match=message):
            operation()


def test_finalizer_output_file_rejects_unsafe_or_missing_paths(tmp_path):
    outside = tmp_path.parent / "outside.copy"
    outside.write_bytes(b"outside")
    operations = (
        (str(outside), "invalid code path"),
        ("../outside.copy", "escapes its output directory"),
        ("missing.copy", "output is missing or empty"),
    )

    for raw_path, message in operations:
        with pytest.raises(RuntimeError, match=message):
            ptg2_shared_publish._finalizer_output_file(
                tmp_path,
                raw_path,
                "code",
            )


@pytest.mark.asyncio
async def test_shared_block_existence_query_rejects_unrequested_hash(monkeypatch):
    monkeypatch.setattr(
        ptg2_shared_publish.db,
        "all",
        AsyncMock(return_value=[(b"z" * 32,)]),
    )

    with pytest.raises(RuntimeError, match="unexpected hash"):
        await ptg2_shared_publish._existing_shared_block_hashes(
            schema_name="mrf",
            requested_hashes=(b"a" * 32,),
        )


@pytest.mark.asyncio
async def test_shared_block_existence_query_uses_exact_lateral_batches(monkeypatch):
    batch_rows = ptg2_shared_publish._SHARED_BLOCK_EXISTENCE_BATCH_ROWS
    requested_hashes = {
        index.to_bytes(32, "big") for index in range(batch_rows + 1)
    }
    observed_batches = []
    observed_statements = []

    async def return_requested_hashes(statement, *, block_hashes):
        observed_statements.append(str(statement))
        observed_batches.append(tuple(block_hashes))
        return [(block_hash,) for block_hash in block_hashes]

    monkeypatch.setattr(ptg2_shared_publish.db, "all", return_requested_hashes)

    existing_hashes = await ptg2_shared_publish._existing_shared_block_hashes(
        schema_name="mrf",
        requested_hashes=requested_hashes,
    )

    assert existing_hashes == requested_hashes
    assert list(map(len, observed_batches)) == [batch_rows, 1]
    assert len(set().union(*map(set, observed_batches))) == sum(
        map(len, observed_batches)
    )
    assert all("CROSS JOIN LATERAL" in statement for statement in observed_statements)
    assert all(
        "candidate.block_hash = requested.block_hash" in statement
        for statement in observed_statements
    )
    assert all("LIMIT 1" in statement for statement in observed_statements)


def _copy_connection(copy_to_table=None):
    driver = object() if copy_to_table is None else SimpleNamespace(
        copy_to_table=copy_to_table
    )
    return SimpleNamespace(
        raw_connection=SimpleNamespace(driver_connection=driver)
    )


@pytest.mark.asyncio
async def test_stage_copy_helpers_require_driver_copy_support(tmp_path, monkeypatch):
    path = tmp_path / "stage.copy"
    path.write_bytes(b"stage")

    @asynccontextmanager
    async def acquire():
        yield _copy_connection()

    monkeypatch.setattr(ptg2_shared_publish.db, "acquire", acquire)

    with pytest.raises(NotImplementedError, match="binary COPY"):
        await ptg2_shared_publish._copy_binary_file_to_stage(
            path,
            schema_name="mrf",
            stage_table="binary_stage",
            columns=("value",),
        )
    with pytest.raises(NotImplementedError, match="text COPY"):
        await ptg2_shared_publish._copy_text_file_to_stage(
            path,
            schema_name="mrf",
            stage_table="text_stage",
            columns=("value",),
            expected_bytes=path.stat().st_size,
            expected_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("digest_matches", [True, False], ids=["stable", "changed"])
async def test_text_stage_copy_verifies_consumed_content(
    tmp_path,
    monkeypatch,
    digest_matches,
):
    path = tmp_path / "stage.copy"
    path.write_bytes(b"provider metadata")

    async def copy_to_table(_table, *, source, **_kwargs):
        chunk = source.read(3)
        while chunk:
            chunk = source.read(3)

    @asynccontextmanager
    async def acquire():
        yield _copy_connection(copy_to_table)

    monkeypatch.setattr(ptg2_shared_publish.db, "acquire", acquire)
    expected_sha256 = hashlib.sha256(path.read_bytes()).hexdigest()
    if not digest_matches:
        expected_sha256 = "0" * 64

    invocation = ptg2_shared_publish._copy_text_file_to_stage(
        path,
        schema_name="mrf",
        stage_table="text_stage",
        columns=("value",),
        expected_bytes=path.stat().st_size,
        expected_sha256=expected_sha256,
    )
    if digest_matches:
        await invocation
    else:
        with pytest.raises(RuntimeError, match="changed during publication"):
            await invocation
