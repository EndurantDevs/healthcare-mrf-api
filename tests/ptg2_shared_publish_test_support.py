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

def _finalizer_contract():
    return {
        "format": "ptg2_v3_direct_finalizer_v3",
        "storage_generation": "shared_blocks_v3",
        "cold_lookup_contract": "ptg_v3_cold_v2",
        "shared_block_layout": "dense_shared_blocks_v3",
        "source_count": 1,
        "price_key_map": {
            "copy_format": "postgresql_binary_copy",
            "row_count": 1,
            "dense_price_ordering": "minimum_negotiated_rate_then_global_id_128_v1",
            "keys_unique_dense_contiguous": True,
            "source_ids_exact_match": True,
        },
        "dense_keys": {
            "price": {
                "count": 1,
                "ordering": "minimum_negotiated_rate_then_global_id_128_v1",
            }
        },
        "blocks": {
            "serving": {
                "copy_bytes": 1,
                "copy_sha256": "a" * 64,
                "artifact_record_counts": {
                    "by_code_provider_shard_v1": 1,
                    "by_code_price_page_v4": 1,
                    "provider_set_count_dictionary": 1,
                    "provider_set_codes_v3": 1,
                    "provider_set_page_v3_s2": 1,
                }
            },
            "price_dictionary": {
                "copy_bytes": 1,
                "copy_sha256": "b" * 64,
                "artifact_record_counts": {"by_code_price_dictionary": 1}
            },
        },
    }


def _provider_set_metadata_entries(tmp_path, *, row_count: int = 1):
    if row_count == 0:
        return ()
    path = tmp_path / "provider-set-metadata.copy"
    path.write_text(f"{'01' * 16}\t1\t{{}}\n", encoding="ascii")
    payload = path.read_bytes()
    return (
        {
            "path": str(path),
            "row_count": row_count,
            "bytes": len(payload),
            "sha256": hashlib.sha256(payload).hexdigest(),
            "format": "ptg2_v3_provider_set_metadata_copy",
            "version": 1,
        },
    )

def _serving_run_entries(tmp_path):
    entries = []
    for partition in range(2):
        path = tmp_path / f"run-{partition}"
        path.write_bytes(b"r" * PTG2_V3_SERVING_RUN_RECORD_BYTES)
        entries.append(
            {
                "path": str(path),
                "format": "ptg2_v3_serving_run",
                "version": 1,
                "partition": partition,
                "partition_count": 2,
                "row_count": 1,
                "bytes": PTG2_V3_SERVING_RUN_RECORD_BYTES,
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            }
        )
    return entries


def _unannotated_file_result(tmp_path):
    dictionary_path = tmp_path / "codes.ready"
    dictionary_path.write_bytes(b"c" * 64)
    dictionary_entries = [
        {
            "path": str(dictionary_path),
            "format": "ptg2_v3_serving_code_dictionary",
            "version": 4,
            "row_count": 1,
            "bytes": 64,
            "sha256": hashlib.sha256(dictionary_path.read_bytes()).hexdigest(),
        }
    ]
    return PTG2FileProcessResult(
        "in_network",
        "https://example.invalid/rates.json.gz",
        True,
        summary={
            "manifest": {
                "copy_files": {
                    "serving_run": _serving_run_entries(tmp_path),
                    "serving_code_dictionary": dictionary_entries,
                }
            },
            "scanner": {
                "summary": {
                    "serving_run_files": 2,
                    "serving_run_rows": 2,
                    "serving_run_bytes": 2 * PTG2_V3_SERVING_RUN_RECORD_BYTES,
                    "serving_code_dictionary_files": 1,
                    "serving_code_dictionary_rows": 1,
                    "serving_code_dictionary_bytes": 64,
                },
                "config": {"serving_run_partition_count": 2},
            },
        },
    )

class _OneRowResult:
    def __init__(self, row, *, rowcount=None):
        self.row = row
        self.rowcount = rowcount

    def one(self):
        return self.row

    def scalar(self):
        return self.row[0]


class _RowsResult:
    def __init__(self, rows):
        self.rows = rows

    def all(self):
        return self.rows


class _FirstBatchProgress:
    def __init__(self):
        self.events = []
        self.first_batch_reported = asyncio.Event()

    def __call__(self, metric, amount):
        self.events.append((metric, amount))
        if metric == "sql_stage_rows":
            self.first_batch_reported.set()


class _SlowSharedBlockSQLDriver:
    batch_sizes = (4_096, 7)
    new_hash_counts = (4_096, 5)

    def __init__(self):
        self.fetch_index = 0
        self.scalar_index = 0
        self.release_second_batch = asyncio.Event()

    async def execute_stage_statement(self, statement, params=None):
        statement_text = str(statement)
        if statement_text.startswith("FETCH FORWARD"):
            if (
                "reuse_protection_cursor" in statement_text
                or "reuse_hash_cursor" in statement_text
            ):
                return _RowsResult(())
            return await self._fetch_stage_rows()
        if "LEFT JOIN \"mrf\".ptg2_v3_snapshot_block AS mapping" in statement_text:
            batch_size = self.batch_sizes[self.fetch_index - 1]
            return _OneRowResult(
                (
                    batch_size,
                    batch_size * 10,
                    batch_size * 7,
                    ["serving"],
                    False,
                    False,
                    False,
                    False,
                )
            )
        if "SUM(staged.entry_count)" in statement_text:
            batch_size = self.batch_sizes[self.fetch_index - 1]
            return _OneRowResult(
                (
                    batch_size,
                    batch_size * 2,
                    batch_size * 10,
                    batch_size * 7,
                    ["serving"],
                    False,
                    False,
                    False,
                )
            )
        return _OneRowResult((0,))

    async def _fetch_stage_rows(self):
        current_index = self.fetch_index
        self.fetch_index += 1
        if current_index == 1:
            await self.release_second_batch.wait()
        if current_index >= len(self.batch_sizes):
            return _RowsResult(())
        return _RowsResult(
            tuple(
                (f"({current_index},{row_offset + 1})",)
                for row_offset in range(self.batch_sizes[current_index])
            )
        )

    async def read_identity_count(self, statement):
        batch_index, identity_kind = divmod(self.scalar_index, 2)
        self.scalar_index += 1
        if identity_kind == 0:
            return self.new_hash_counts[batch_index]
        return self.batch_sizes[batch_index]


class _SlowV4CASSQLDriver:
    batch_sizes = (4_096, 4)
    unique_counts = (4_094, 3)

    def __init__(self):
        self.fetch_index = 0
        self.release_second_batch = asyncio.Event()

    async def execute_stage_statement(self, statement, params=None):
        statement_text = str(statement)
        if statement_text.startswith("FETCH FORWARD"):
            if (
                "reuse_protection_cursor" in statement_text
                or "reuse_hash_cursor" in statement_text
            ):
                return _RowsResult(())
            return await self._fetch_stage_rows()
        batch_index = max(self.fetch_index - 1, 0)
        if "SUM(staged.entry_count)" in statement_text:
            batch_size = self.batch_sizes[batch_index]
            return _OneRowResult(
                (
                    batch_size,
                    batch_size * 2,
                    batch_size * 10,
                    batch_size * 7,
                    ["v4_graph"],
                    False,
                    False,
                    False,
                )
            )
        if "JOIN \"mrf\".ptg2_v3_block AS stored USING" in statement_text:
            unique_count = self.unique_counts[batch_index]
            return _OneRowResult(
                (unique_count, unique_count * 10, unique_count * 7)
            )
        return _OneRowResult((0,))

    async def _fetch_stage_rows(self):
        batch_index = self.fetch_index
        self.fetch_index += 1
        if batch_index == 1:
            await self.release_second_batch.wait()
        if batch_index >= len(self.batch_sizes):
            return _RowsResult(())
        return _RowsResult(
            tuple(
                (f"({batch_index},{row_offset + 1})",)
                for row_offset in range(self.batch_sizes[batch_index])
            )
        )


@asynccontextmanager
async def _session_transaction(session):
    yield session

def _assert_slow_shared_block_publication(publication, progress_events, session):
    assert publication.mapping_count == 4_103
    assert publication.unique_block_count == 4_101
    assert publication.logical_byte_count == 41_030
    assert publication.stored_byte_count == 28_721
    assert progress_events == [
        ("sql_stage_rows", 4_096),
        ("publish_batches", 1),
        ("sql_stage_rows", 7),
        ("publish_batches", 1),
    ]
    statements = "\n".join(
        str(call.args[0]) for call in session.execute.await_args_list
    )
    assert "FETCH FORWARD 4096" in statements
    assert "JOIN \"ptg2_publish_batch_" in statements


def _assert_slow_v4_cas_publication(publication, progress_events):
    assert publication.staged_row_count == 4_100
    assert publication.staged_entry_count == 8_200
    assert publication.unique_block_count == 4_097
    assert publication.logical_byte_count == 41_000
    assert publication.stored_byte_count == 28_700
    assert publication.unique_logical_byte_count == 40_970
    assert publication.unique_stored_byte_count == 28_679
    assert progress_events[-2:] == [
        ("sql_stage_rows", 4),
        ("publish_batches", 1),
    ]

def _copy_connection(copy_to_table=None):
    driver = object() if copy_to_table is None else SimpleNamespace(
        copy_to_table=copy_to_table
    )
    return SimpleNamespace(
        raw_connection=SimpleNamespace(driver_connection=driver)
    )

def _assert_shared_stage_sql(session):
    lock_sql = str(session.execute.await_args_list[0].args[0])
    assert "FOR KEY SHARE OF stored" in lock_sql
    block_insert_sql = str(session.execute.await_args_list[1].args[0])
    assert "NOT EXISTS" in block_insert_sql
    assert "staged.format_version = :format_version" in block_insert_sql
    assert "staged.payload IS NOT NULL" in block_insert_sql
    assert "stored.block_hash = staged.block_hash" in block_insert_sql
    assert "ON CONFLICT (block_hash) DO NOTHING" in block_insert_sql
    aggregate_sql = str(session.execute.await_args_list[2].args[0])
    assert "LEFT JOIN" in aggregate_sql
    assert "stored.block_hash IS NULL" in aggregate_sql
    assert "stored.payload" not in aggregate_sql
    assert "staged.payload IS NULL" in aggregate_sql
    assert "BOOL_OR" in aggregate_sql
    assert "staged.format_version <> :format_version" in aggregate_sql
    assert "stored.format_version <> staged.format_version" in aggregate_sql
    assert "stored.object_kind <> staged.object_kind" in aggregate_sql
    assert "stored.codec <> staged.codec" in aggregate_sql
    assert "stored.entry_count <> staged.entry_count" in aggregate_sql
    assert "stored.raw_byte_count <> staged.raw_byte_count" in aggregate_sql
    assert "stored.stored_byte_count <> staged.stored_byte_count" in aggregate_sql
    assert "COUNT(DISTINCT staged.block_hash)" in aggregate_sql
    assert "ARRAY_AGG(" in aggregate_sql
    assert "DISTINCT staged.object_kind" in aggregate_sql
    assert "ORDER BY staged.object_kind" in aggregate_sql
    assert "canonical_mapping" not in aggregate_sql
    assert 'FROM "mrf"."ptg2_v3_block_stage_proof"' in aggregate_sql
    format_by_field = {
        "format_version": ptg2_shared_publish.PTG2_V3_SHARED_FORMAT_VERSION
    }
    assert session.execute.await_args_list[1].args[1] == format_by_field
    assert session.execute.await_args_list[2].args[1] == format_by_field
    delete_sql = str(session.execute.await_args_list[3].args[0])
    assert 'DELETE FROM "mrf".ptg2_v3_gc_candidate' in delete_sql


def _bounded_stage_session() -> SimpleNamespace:
    """Return a session with one valid bounded aggregate result."""

    return SimpleNamespace(
        execute=AsyncMock(
            side_effect=[
                None,
                None,
                _OneRowResult(
                    (
                        3,
                        2,
                        30,
                        20,
                        ["a_kind", "z_kind"],
                        False,
                        False,
                        False,
                    )
                ),
                None,
            ]
        ),
        scalar=AsyncMock(),
    )

def _dictionary_summary(tmp_path, *, row_count: int) -> dict[str, object]:
    (tmp_path / "codes.copy").write_bytes(b"codes")
    (tmp_path / "providers.copy").write_bytes(b"providers")
    return {
        **_finalizer_contract(),
        "output_directory": str(tmp_path),
        "dictionaries": {
            "code": {"path": "codes.copy", "row_count": row_count},
            "provider_set": {"path": "providers.copy", "row_count": row_count},
            "support_digest": (b"s" * 32).hex(),
        },
        "preservation": {"encoded_records": row_count},
    }
