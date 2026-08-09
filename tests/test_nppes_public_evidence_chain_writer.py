# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Atomic chain-admission orchestration without a live database."""

from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import pytest

from process.control_cancel import ImportCancelledError
from process.nppes_public_evidence_chain_rows import (
    CHAIN_ADMISSION_COLUMNS,
    CHAIN_ARCHIVE_COLUMNS,
    build_nppes_chain_storage_rows,
    chain_admission_values,
    chain_archive_values,
)
from process.nppes_public_evidence_chain_writer import (
    _assert_release_admissions,
    _has_existing_chain,
    admit_nppes_public_evidence_chain,
)
from process.nppes_public_evidence_writer import NppesPublicEvidenceWriterError
from tests.test_nppes_public_evidence_import import _valid_receipt


class _Transaction:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False


class _Connection:
    def __init__(self) -> None:
        self.statements: list[tuple[str, tuple[object, ...]]] = []

    def transaction(self):
        return _Transaction()

    async def execute(self, statement: str, *values: object):
        self.statements.append((statement, values))
        return "OK"


class _Database:
    def __init__(self, connection: _Connection) -> None:
        self.connection = connection

    @asynccontextmanager
    async def acquire_driver(self):
        yield self.connection


class _StoredConnection:
    def __init__(self, rows=(), value=None) -> None:
        self.rows = rows
        self.value = value

    async def fetch(self, *_args):
        return self.rows

    async def fetchval(self, *_args):
        return self.value


@pytest.mark.asyncio
@pytest.mark.parametrize("already_present", (False, True))
async def test_chain_admission_serializes_and_reuses_exact_receipts(
    monkeypatch,
    already_present: bool,
) -> None:
    connection = _Connection()
    catalog_check = AsyncMock()
    release_check = AsyncMock()
    existing_check = AsyncMock(return_value=already_present)
    insert_chain = AsyncMock()
    monkeypatch.setattr(
        "process.nppes_public_evidence_chain_writer._assert_catalog",
        catalog_check,
    )
    monkeypatch.setattr(
        "process.nppes_public_evidence_chain_writer._has_existing_chain",
        existing_check,
    )
    monkeypatch.setattr(
        "process.nppes_public_evidence_chain_writer._assert_release_admissions",
        release_check,
    )
    monkeypatch.setattr(
        "process.nppes_public_evidence_chain_writer._insert_chain",
        insert_chain,
    )
    receipt = _valid_receipt()

    admitted_receipt = await admit_nppes_public_evidence_chain(
        receipt,
        database=_Database(connection),
    )

    assert admitted_receipt == receipt
    catalog_check.assert_awaited_once_with(connection, "mrf")
    release_check.assert_awaited_once()
    existing_check.assert_awaited_once()
    assert len(connection.statements) == 1
    assert "pg_advisory_xact_lock" in connection.statements[0][0]
    if already_present:
        insert_chain.assert_not_awaited()
    else:
        insert_chain.assert_awaited_once()


@pytest.mark.asyncio
async def test_chain_admission_preserves_control_cancellation(monkeypatch) -> None:
    connection = _Connection()
    monkeypatch.setattr(
        "process.nppes_public_evidence_chain_writer._assert_catalog",
        AsyncMock(),
    )

    async def cancel() -> None:
        raise ImportCancelledError("cancelled")

    with pytest.raises(ImportCancelledError):
        await admit_nppes_public_evidence_chain(
            _valid_receipt(),
            database=_Database(connection),
            cancel_check=cancel,
        )
    assert connection.statements == []


@pytest.mark.asyncio
async def test_release_admission_owners_must_be_complete_and_exact() -> None:
    _admission, archives = build_nppes_chain_storage_rows(_valid_receipt())
    with pytest.raises(NppesPublicEvidenceWriterError):
        await _assert_release_admissions(_StoredConnection(), "mrf", archives)

    first_archive = archives[0]
    values_by_column = dict(
        zip(
            CHAIN_ARCHIVE_COLUMNS,
            chain_archive_values(first_archive),
            strict=True,
        )
    )
    values_by_column["archive_name"] = "wrong.zip"
    with pytest.raises(NppesPublicEvidenceWriterError):
        await _assert_release_admissions(
            _StoredConnection((values_by_column,)),
            "mrf",
            (first_archive,),
        )


@pytest.mark.asyncio
async def test_existing_chain_requires_one_exact_sealed_family(monkeypatch) -> None:
    admission, archives = build_nppes_chain_storage_rows(_valid_receipt())
    connection = _StoredConnection(value="owned")

    monkeypatch.setattr(
        "process.nppes_public_evidence_chain_writer._stored_rows",
        AsyncMock(return_value=(None, (object(),))),
    )
    with pytest.raises(NppesPublicEvidenceWriterError):
        await _has_existing_chain(connection, "mrf", admission, archives)

    monkeypatch.setattr(
        "process.nppes_public_evidence_chain_writer._stored_rows",
        AsyncMock(return_value=(None, ())),
    )
    with pytest.raises(NppesPublicEvidenceWriterError):
        await _has_existing_chain(connection, "mrf", admission, archives)

    wrong_parent_by_column = {
        column: column_value
        for column, column_value in zip(
            CHAIN_ADMISSION_COLUMNS,
            chain_admission_values(admission),
            strict=True,
        )
    }
    wrong_parent_by_column["contract"] = "wrong"
    monkeypatch.setattr(
        "process.nppes_public_evidence_chain_writer._stored_rows",
        AsyncMock(return_value=(wrong_parent_by_column, ())),
    )
    with pytest.raises(NppesPublicEvidenceWriterError):
        await _has_existing_chain(connection, "mrf", admission, archives)

    exact_parent_by_column = dict(
        zip(
            CHAIN_ADMISSION_COLUMNS,
            chain_admission_values(admission),
            strict=True,
        )
    )
    exact_child_rows = tuple(
        dict(
            zip(
                CHAIN_ARCHIVE_COLUMNS,
                chain_archive_values(archive),
                strict=True,
            )
        )
        for archive in archives
    )
    monkeypatch.setattr(
        "process.nppes_public_evidence_chain_writer._stored_rows",
        AsyncMock(return_value=(exact_parent_by_column, exact_child_rows)),
    )
    connection.value = False
    with pytest.raises(NppesPublicEvidenceWriterError):
        await _has_existing_chain(connection, "mrf", admission, archives)


@pytest.mark.asyncio
async def test_public_chain_writer_normalizes_wrong_receipt_type() -> None:
    with pytest.raises(NppesPublicEvidenceWriterError):
        await admit_nppes_public_evidence_chain(object())
