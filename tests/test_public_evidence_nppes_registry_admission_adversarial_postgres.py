# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Adversarial PostgreSQL chain proof for NPPES registry admission."""

from __future__ import annotations

from pathlib import Path

import asyncpg
import pytest

from process.nppes_public_evidence_chain import _finished_chain_receipt
from process.nppes_public_evidence_chain_rows import (
    CHAIN_ADMISSION_COLUMNS,
    CHAIN_ARCHIVE_COLUMNS,
    _finished_row,
    build_nppes_chain_storage_rows,
    chain_admission_values,
    chain_archive_values,
)
from process.nppes_public_evidence_import import _archive_receipt
from process.nppes_public_evidence_replay import prepare_nppes_registry_replay
from process.nppes_public_evidence_writer import _insert_single
from process.nppes_public_evidence_writer_contract import (
    NppesPublicEvidenceWriterError,
)
from tests.nppes_public_evidence_process_support import prepared_archive
from tests.public_evidence_nppes_admission_postgres_support import (
    DEFAULT_ROWS,
    NEW_TABLES,
    admit_replay,
    nppes_admission_schema,
    prepared_replay,
    required_config,
)
from tests.public_evidence_storage_postgres_support import connect


async def _assert_invalid_chain_rejected(
    connection: asyncpg.Connection,
    schema: str,
    invalid_parent_row,
    invalid_child_rows,
) -> None:
    """Execute the deferred seal and require rejection of an invalid chain."""

    with pytest.raises(
        asyncpg.CheckViolationError,
        match="public_evidence_nppes_chain_invalid",
    ):
        async with connection.transaction():
            await _insert_single(
                connection,
                schema,
                NEW_TABLES[3],
                CHAIN_ADMISSION_COLUMNS,
                chain_admission_values(invalid_parent_row),
            )
            for invalid_child_row in invalid_child_rows:
                await _insert_single(
                    connection,
                    schema,
                    NEW_TABLES[5],
                    CHAIN_ARCHIVE_COLUMNS,
                    chain_archive_values(invalid_child_row),
                )
            await connection.execute("SET CONSTRAINTS ALL IMMEDIATE")


@pytest.mark.asyncio
async def test_writer_rejects_a_disabled_always_guard(tmp_path: Path) -> None:
    replay = await prepared_replay(tmp_path)
    async with nppes_admission_schema() as (_engine, url, schema, _migration):
        connection = await connect(url)
        try:
            await connection.execute(
                f'ALTER TABLE "{schema}".'
                '"public_evidence_nppes_registry_chain_archive" '
                "DISABLE TRIGGER ALL"
            )
            with pytest.raises(NppesPublicEvidenceWriterError):
                await admit_replay(connection, schema, replay)
            assert await connection.fetchval(
                f'SELECT count(*) FROM "{schema}".'
                '"public_evidence_source_release"'
            ) == 0
        finally:
            await connection.close()


@pytest.mark.asyncio
async def test_reversed_self_consistent_chain_is_rejected(tmp_path: Path) -> None:
    """Reject a self-consistent chain whose listing reverses archive order."""
    specs = (
        ("NPPES_Data_Dissemination_July_2026_V2.zip", "20260712"),
        ("NPPES_Data_Dissemination_071326_071926_Weekly_V2.zip", "20260719"),
        ("NPPES_Data_Dissemination_072026_072626_Weekly_V2.zip", "20260726"),
    )
    replays = tuple(
        [
            await prepare_nppes_registry_replay(
                prepared_archive(tmp_path, name, snapshot, DEFAULT_ROWS),
                required_config(),
            )
            for name, snapshot in specs
        ]
    )
    async with nppes_admission_schema() as (_engine, url, schema, _migration):
        connection = await connect(url)
        try:
            archive_receipts = tuple(
                [
                    _archive_receipt(
                        replay,
                        await admit_replay(connection, schema, replay),
                    )
                    for replay in replays
                ]
            )
            names = tuple(name for name, _snapshot in specs)
            valid = _finished_chain_receipt("c3" * 32, 317, names, archive_receipts)
            parent_row, child_rows = build_nppes_chain_storage_rows(valid)
            invalid = _finished_chain_receipt(
                "c3" * 32,
                317,
                tuple(reversed(names)),
                archive_receipts,
            )
            invalid_parent_row = _finished_row(
                parent_row._replace(
                    chain_ref=invalid.chain_ref,
                    contract_sha256=invalid.contract_sha256,
                    listing_candidate_names=invalid.listing_candidate_names,
                ),
                "nppes_chain_admission_row",
            )
            invalid_child_rows = tuple(
                _finished_row(
                    child_row._replace(chain_ref=invalid.chain_ref),
                    "nppes_chain_archive_row",
                )
                for child_row in child_rows
            )
            await _assert_invalid_chain_rejected(
                connection,
                schema,
                invalid_parent_row,
                invalid_child_rows,
            )
        finally:
            await connection.close()
