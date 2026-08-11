"""Disposable-PostgreSQL concurrency proof for member-local receipts."""

from __future__ import annotations

import asyncio
import datetime as dt
import hashlib
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from db.connection import Database
from process import ptg_wave_ordinary_terminal_receipt as terminal
from process.ptg_wave_receipt_authority import (
    ORDINARY_TERMINAL_RECEIPT_SCHEMA,
)
from tests.test_ptg_wave_recovery_storage_postgres import _dsn, _quote


OPERATION_ID = "9" * 64


class _SyntheticKeyring:
    def sign_receipt(self, *, schema, key_id, issued_at, receipt_payload):
        issued = issued_at
        if isinstance(issued, dt.datetime):
            issued = issued.astimezone(dt.UTC).strftime(
                "%Y-%m-%dT%H:%M:%S.%fZ"
            )
        digest = hashlib.sha256(
            repr(sorted(receipt_payload.items())).encode()
        ).hexdigest()
        return {
            "schema": schema,
            "key_id": key_id,
            "issued_at": issued,
            "payload": dict(receipt_payload),
            "payload_digest": digest,
            "signature": "1" * 512,
        }


def _request(ordinal: int) -> dict[str, object]:
    return {
        "schema": terminal.ORDINARY_TERMINAL_REQUEST_SCHEMA,
        "key_id": "ephemeral-pool-key",
        "operation_id": OPERATION_ID,
        "member_ordinal": ordinal,
        "source_file_import_id": f"ordinary-source-{ordinal}",
        "run_id": f"ordinary-run-{ordinal}",
    }


@pytest.mark.asyncio
async def test_pool_five_cancellation_and_blocked_member_do_not_starve_peers(
    monkeypatch,
):
    """Prove cancellation drains the pool while peer receipts complete."""
    schema = "wave_receipt_member_local_pool"
    quoted = _quote(schema)
    engine = create_async_engine(
        _dsn().replace("postgresql://", "postgresql+asyncpg://", 1),
        pool_size=5,
        max_overflow=0,
        pool_timeout=2,
    )
    local_db = Database(
        engine=engine,
        session_factory=async_sessionmaker(
            engine,
            expire_on_commit=False,
            autoflush=False,
        ),
    )
    member_zero_waiting = asyncio.Event()
    callbacks = _terminal_pool_callbacks(quoted, member_zero_waiting)
    _patch_terminal_pool(monkeypatch, local_db, callbacks)
    await _create_terminal_pool_schema(engine, quoted)
    holder = await engine.connect()
    holder_transaction = await holder.begin()
    try:
        await holder.execute(
            text(
                f"SELECT member_ordinal FROM {quoted}.terminal_member "
                "WHERE member_ordinal = 0 FOR UPDATE"
            )
        )
        baseline_checked_out = engine.pool.checkedout()
        assert baseline_checked_out == 1
        await _assert_canceled_member_waiters_release_pool(
            member_zero_waiting,
            engine,
            baseline_checked_out,
        )
        await _assert_peer_receipts_complete(engine, baseline_checked_out)
        await _assert_blocked_member_times_out(
            monkeypatch,
            engine,
            baseline_checked_out,
        )
    finally:
        await holder_transaction.rollback()
        await holder.close()
    await _assert_released_member_completes(engine, quoted)


def _terminal_pool_callbacks(quoted, member_zero_waiting):
    """Build receipt callbacks bound to one disposable schema."""

    async def load_existing(session, request):
        row = (
            await session.execute(
                text(
                    f"SELECT receipt FROM {quoted}.terminal_receipt "
                    "WHERE member_ordinal = :member_ordinal FOR UPDATE"
                ),
                {"member_ordinal": request["member_ordinal"]},
            )
        ).scalar_one_or_none()
        if row is None:
            return None
        return SimpleNamespace(
            wave_id=request["operation_id"],
            member_ordinal=request["member_ordinal"],
            source_file_import_id=request["source_file_import_id"],
            run_id=request["run_id"],
            receipt_key_id=request["key_id"],
            receipt=row,
            payload_digest=row["payload_digest"],
            issued_at=dt.datetime.fromisoformat(
                row["issued_at"].replace("Z", "+00:00")
            ),
        )

    async def load_snapshot(session, request):
        if request["member_ordinal"] == 0:
            member_zero_waiting.set()
        ordinal = (
            await session.execute(
                text(
                    f"SELECT member_ordinal FROM {quoted}.terminal_member "
                    "WHERE member_ordinal = :member_ordinal FOR UPDATE"
                ),
                {"member_ordinal": request["member_ordinal"]},
            )
        ).scalar_one()
        return {"request": request, "ordinal": ordinal}

    async def persist(session, request, receipt):
        await session.execute(
            text(
                f"INSERT INTO {quoted}.terminal_receipt "
                "(member_ordinal, receipt) "
                "VALUES (:member_ordinal, CAST(:receipt AS jsonb))"
            ),
            {
                "member_ordinal": request["member_ordinal"],
                "receipt": json.dumps(receipt),
            },
        )
    return load_existing, load_snapshot, persist


def _patch_terminal_pool(monkeypatch, local_db, callbacks) -> None:
    load_existing, load_snapshot, persist = callbacks
    monkeypatch.setattr(terminal, "db", local_db)
    monkeypatch.setattr(terminal, "_load_existing_receipt", load_existing)
    monkeypatch.setattr(terminal, "_load_terminal_snapshot", load_snapshot)
    monkeypatch.setattr(
        terminal,
        "_verify_abandonment_signature",
        lambda *_args: None,
    )
    monkeypatch.setattr(
        terminal,
        "ordinary_terminal_receipt_payload",
        lambda **snapshot: {
            "member_ordinal": snapshot["ordinal"],
            "operation_id": snapshot["request"]["operation_id"],
        },
    )
    monkeypatch.setattr(terminal, "_persist_terminal_receipt", persist)
    monkeypatch.setattr(
        terminal,
        "_release_terminal_snapshot_pin",
        AsyncMock(return_value=0),
    )
    monkeypatch.setattr(terminal, "ORDINARY_TERMINAL_LOCK_TIMEOUT", "2s")
    monkeypatch.setattr(terminal, "ORDINARY_TERMINAL_STATEMENT_TIMEOUT", "3s")


async def _create_terminal_pool_schema(engine, quoted) -> None:
    async with engine.begin() as connection:
        await connection.exec_driver_sql(f"DROP SCHEMA IF EXISTS {quoted} CASCADE")
        await connection.exec_driver_sql(f"CREATE SCHEMA {quoted}")
        await connection.exec_driver_sql(
            f"CREATE TABLE {quoted}.terminal_member ("
            "member_ordinal integer PRIMARY KEY)"
        )
        await connection.exec_driver_sql(
            f"INSERT INTO {quoted}.terminal_member "
            "SELECT generate_series(0, 23)"
        )
        await connection.exec_driver_sql(
            f"CREATE TABLE {quoted}.terminal_receipt ("
            "member_ordinal integer PRIMARY KEY, receipt jsonb NOT NULL)"
        )


async def _assert_canceled_member_waiters_release_pool(
    member_zero_waiting,
    engine,
    baseline_checked_out,
) -> None:
    canceled_results = [
        asyncio.create_task(
            terminal.issue_ordinary_terminal_receipt(
                OPERATION_ID,
                _request(0),
                receipt_keyring=_SyntheticKeyring(),
            )
        )
        for _ in range(8)
    ]
    await asyncio.wait_for(member_zero_waiting.wait(), timeout=1)
    await asyncio.sleep(0.05)
    for task in canceled_results:
        task.cancel()
    canceled_results = await asyncio.gather(*canceled_results, return_exceptions=True)
    assert all(isinstance(result, asyncio.CancelledError) for result in canceled_results)
    assert engine.pool.checkedout() == baseline_checked_out


async def _assert_peer_receipts_complete(engine, baseline_checked_out) -> None:
    peer_results = await asyncio.wait_for(
        asyncio.gather(*(
            terminal.issue_ordinary_terminal_receipt(
                OPERATION_ID, _request(ordinal),
                receipt_keyring=_SyntheticKeyring(),
            )
            for ordinal in range(1, 24)
        )),
        timeout=8,
    )
    assert [receipt["payload"]["member_ordinal"] for receipt, _ in peer_results] == list(range(1, 24))
    assert all(created is True for _, created in peer_results)
    assert engine.pool.checkedout() == baseline_checked_out


async def _assert_blocked_member_times_out(
    monkeypatch,
    engine,
    baseline_checked_out,
) -> None:
    monkeypatch.setattr(terminal, "ORDINARY_TERMINAL_LOCK_TIMEOUT", "100ms")
    with pytest.raises(
        terminal.PTGWaveOrdinaryTerminalRetryable,
        match="database wait expired; retry",
    ):
        await terminal.issue_ordinary_terminal_receipt(
            OPERATION_ID,
            _request(0),
            receipt_keyring=_SyntheticKeyring(),
        )
    assert engine.pool.checkedout() == baseline_checked_out


async def _assert_released_member_completes(engine, quoted) -> None:
    receipt, created = await terminal.issue_ordinary_terminal_receipt(
        OPERATION_ID,
        _request(0),
        receipt_keyring=_SyntheticKeyring(),
    )
    assert created is True
    assert receipt["payload"]["member_ordinal"] == 0
    assert engine.pool.checkedout() == 0
    async with engine.begin() as connection:
        assert await connection.scalar(
            text(f"SELECT count(*) FROM {quoted}.terminal_receipt")
        ) == 24
        await connection.exec_driver_sql(f"DROP SCHEMA {quoted} CASCADE")
    await engine.dispose()
