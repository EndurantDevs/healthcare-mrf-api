# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Durable short-lived protection for shared PTG CAS publication."""

from __future__ import annotations

import asyncio
import os
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, AsyncIterator, Iterable

from db.connection import db
from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.ptg2_lifecycle_lock import (
    configure_ptg2_lifecycle_transaction,
)


PTG2_BLOCK_BUILD_PIN_TABLE = "ptg2_block_build_pin"
PTG2_BLOCK_BUILD_PIN_LEASE_SECONDS_ENV = (
    "HLTHPRT_PTG2_BLOCK_BUILD_PIN_LEASE_SECONDS"
)
PTG2_BLOCK_BUILD_PIN_LEASE_SECONDS_DEFAULT = 21_600


def _lease_seconds() -> int:
    raw_seconds = os.getenv(PTG2_BLOCK_BUILD_PIN_LEASE_SECONDS_ENV)
    try:
        lease_seconds = int(raw_seconds) if raw_seconds is not None else (
            PTG2_BLOCK_BUILD_PIN_LEASE_SECONDS_DEFAULT
        )
    except ValueError:
        lease_seconds = PTG2_BLOCK_BUILD_PIN_LEASE_SECONDS_DEFAULT
    if lease_seconds <= 0:
        raise ValueError("PTG block build pin lease must be positive")
    return lease_seconds


def _lease_deadline() -> datetime:
    return datetime.now(timezone.utc) + timedelta(seconds=_lease_seconds())


def _normalized_hashes(block_hashes: Iterable[bytes]) -> tuple[bytes, ...]:
    normalized_hashes = tuple(
        sorted({bytes(block_hash) for block_hash in block_hashes})
    )
    if any(len(block_hash) != 32 for block_hash in normalized_hashes):
        raise ValueError("PTG block build pins require 32-byte hashes")
    return normalized_hashes


async def _require_layout_build_owner(
    session: Any,
    *,
    schema: str,
    snapshot_key: int,
    build_token: str,
) -> None:
    owner_result = await session.execute(
        db.text(
            f"""
            SELECT snapshot_key
              FROM {schema}.ptg2_v3_snapshot_layout
             WHERE snapshot_key = :snapshot_key
               AND state = 'building'
               AND build_token = :build_token
             FOR KEY SHARE
            """
        ),
        {
            "snapshot_key": int(snapshot_key),
            "build_token": str(build_token),
        },
    )
    if owner_result.scalar() is None:
        raise RuntimeError("PTG block pin lost layout build ownership")


async def _upsert_build_pins(
    session: Any,
    *,
    schema: str,
    snapshot_key: int,
    build_token: str,
    pin_token: str,
    block_hashes: tuple[bytes, ...],
) -> set[bytes]:
    pin_result = await session.execute(
        db.text(
            f"""
            INSERT INTO {schema}.{PTG2_BLOCK_BUILD_PIN_TABLE}
                (snapshot_key, build_token, pin_token, block_hash,
                 created_at, heartbeat_at, lease_until)
            SELECT :snapshot_key, :build_token, :pin_token, requested.block_hash,
                   transaction_timestamp(), transaction_timestamp(), :lease_until
              FROM unnest(CAST(:block_hashes AS bytea[]))
                       AS requested(block_hash)
            ON CONFLICT (snapshot_key, pin_token, block_hash) DO UPDATE
                SET heartbeat_at = EXCLUDED.heartbeat_at,
                    lease_until = EXCLUDED.lease_until
              WHERE {PTG2_BLOCK_BUILD_PIN_TABLE}.build_token = EXCLUDED.build_token
            RETURNING block_hash
            """
        ),
        {
            "snapshot_key": int(snapshot_key),
            "build_token": str(build_token),
            "pin_token": pin_token,
            "block_hashes": list(block_hashes),
            "lease_until": _lease_deadline(),
        },
    )
    return {bytes(pin_record[0]) for pin_record in pin_result.all()}


async def is_pin_lease_renewed(
    session: Any,
    *,
    schema_name: str,
    snapshot_key: int,
    build_token: str,
    pin_token: str,
) -> bool:
    """Renew one token-fenced group anchor while its layout owner is live."""

    schema = _quote_ident(schema_name)
    await configure_ptg2_lifecycle_transaction(
        session,
        lock_timeout="500ms",
        statement_timeout="5s",
    )
    renewal_result = await session.execute(
        db.text(
            f"""
            UPDATE {schema}.{PTG2_BLOCK_BUILD_PIN_TABLE} AS pin
               SET heartbeat_at = transaction_timestamp(),
                   lease_until = :lease_until
             WHERE (pin.snapshot_key, pin.pin_token, pin.block_hash) = (
                    SELECT candidate.snapshot_key,
                           candidate.pin_token,
                           candidate.block_hash
                      FROM {schema}.{PTG2_BLOCK_BUILD_PIN_TABLE} AS candidate
                     WHERE candidate.snapshot_key = :snapshot_key
                       AND candidate.build_token = :build_token
                       AND candidate.pin_token = :pin_token
                     ORDER BY candidate.block_hash
                     LIMIT 1
                   )
               AND EXISTS (
                    SELECT 1
                      FROM {schema}.ptg2_v3_snapshot_layout AS layout
                     WHERE layout.snapshot_key = :snapshot_key
                       AND layout.state = 'building'
                       AND layout.build_token = :build_token
               )
            RETURNING pin.block_hash
            """
        ),
        {
            "snapshot_key": int(snapshot_key),
            "build_token": str(build_token),
            "pin_token": str(pin_token),
            "lease_until": _lease_deadline(),
        },
    )
    return renewal_result.scalar() is not None


@dataclass
class SharedBlockBuildPinLease:
    """Own one background heartbeat for an exact build-pin token."""

    schema_name: str
    snapshot_key: int
    build_token: str
    pin_token: str
    _task: asyncio.Task[None] | None = field(default=None, init=False)
    _error: BaseException | None = field(default=None, init=False)

    async def _renew_once(self) -> None:
        async with db.transaction() as session:
            renewed = await is_pin_lease_renewed(
                session,
                schema_name=self.schema_name,
                snapshot_key=self.snapshot_key,
                build_token=self.build_token,
                pin_token=self.pin_token,
            )
        if not renewed:
            raise RuntimeError("PTG block pin heartbeat lost ownership")

    async def _run(self) -> None:
        interval_seconds = max(0.05, min(30.0, _lease_seconds() / 3.0))
        try:
            while True:
                await asyncio.sleep(interval_seconds)
                await self._renew_once()
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            self._error = exc

    async def start(self) -> None:
        """Start the sole heartbeat task for this exact pin token."""

        await self._renew_once()
        self._task = asyncio.create_task(self._run())

    def require_live(self) -> None:
        """Fail when the owned heartbeat stopped or lost its database fence."""

        if self._error is not None:
            raise RuntimeError("PTG block pin heartbeat failed") from self._error
        if self._task is None or self._task.done():
            raise RuntimeError("PTG block pin heartbeat is not running")

    async def close(self) -> None:
        """Cancel and drain the heartbeat while preserving its first failure."""

        task = self._task
        self._task = None
        if task is None:
            return
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            task = None
        if self._error is not None:
            raise RuntimeError("PTG block pin heartbeat failed") from self._error


@asynccontextmanager
async def maintain_shared_block_build_pin_lease(
    *,
    schema_name: str,
    snapshot_key: int,
    build_token: str,
    pin_token: str,
) -> AsyncIterator[SharedBlockBuildPinLease]:
    """Keep one durable build-pin group live through atomic attachment."""

    lease = SharedBlockBuildPinLease(
        schema_name=schema_name,
        snapshot_key=int(snapshot_key),
        build_token=str(build_token),
        pin_token=str(pin_token),
    )
    await lease.start()
    try:
        yield lease
    finally:
        await lease.close()


async def _protect_pinned_hashes_from_gc(
    session: Any,
    *,
    schema: str,
    block_hashes: tuple[bytes, ...],
) -> None:
    await session.execute(
        db.text(
            f"""
            SELECT block.block_hash
              FROM {schema}.ptg2_v3_block AS block
             WHERE block.block_hash = ANY(CAST(:block_hashes AS bytea[]))
             ORDER BY block.block_hash
             FOR KEY SHARE OF block
            """
        ),
        {"block_hashes": list(block_hashes)},
    )
    await session.execute(
        db.text(
            f"""
            DELETE FROM {schema}.ptg2_v3_gc_candidate AS candidate
             WHERE candidate.block_hash = ANY(CAST(:block_hashes AS bytea[]))
            """
        ),
        {"block_hashes": list(block_hashes)},
    )


async def pin_shared_block_hashes(
    session: Any,
    *,
    schema_name: str,
    snapshot_key: int,
    build_token: str,
    pin_token: str,
    block_hashes: Iterable[bytes],
) -> int:
    """Persist one bounded pin batch and cancel matching GC candidacy."""

    normalized_hashes = _normalized_hashes(block_hashes)
    normalized_pin_token = str(pin_token or "").strip()
    if not normalized_pin_token or len(normalized_pin_token) > 96:
        raise ValueError("PTG block pin token must contain at most 96 characters")
    if not normalized_hashes:
        return 0
    schema = _quote_ident(schema_name)
    await configure_ptg2_lifecycle_transaction(
        session,
        lock_timeout="500ms",
        statement_timeout="5s",
    )
    await _require_layout_build_owner(
        session,
        schema=schema,
        snapshot_key=snapshot_key,
        build_token=build_token,
    )
    pinned_hashes = await _upsert_build_pins(
        session,
        schema=schema,
        snapshot_key=snapshot_key,
        build_token=build_token,
        pin_token=normalized_pin_token,
        block_hashes=normalized_hashes,
    )
    if pinned_hashes != set(normalized_hashes):
        raise RuntimeError("PTG block pin ownership changed")
    if not await is_pin_lease_renewed(
        session,
        schema_name=schema_name,
        snapshot_key=snapshot_key,
        build_token=build_token,
        pin_token=normalized_pin_token,
    ):
        raise RuntimeError("PTG block pin heartbeat lost ownership")
    await _protect_pinned_hashes_from_gc(
        session,
        schema=schema,
        block_hashes=normalized_hashes,
    )
    return len(normalized_hashes)


async def delete_shared_block_build_pins(
    session: Any,
    *,
    schema_name: str,
    snapshot_key: int,
    build_token: str,
    pin_token: str,
) -> int:
    """Unpin only the exact build after its reachability is attached."""

    result = await session.execute(
        db.text(
            f"""
            DELETE FROM {_quote_ident(schema_name)}.{PTG2_BLOCK_BUILD_PIN_TABLE}
             WHERE snapshot_key = :snapshot_key
               AND build_token = :build_token
               AND pin_token = :pin_token
            RETURNING block_hash
            """
        ),
        {
            "snapshot_key": int(snapshot_key),
            "build_token": str(build_token),
            "pin_token": str(pin_token),
        },
    )
    return len(result.all())


__all__ = [
    "PTG2_BLOCK_BUILD_PIN_LEASE_SECONDS_DEFAULT",
    "PTG2_BLOCK_BUILD_PIN_LEASE_SECONDS_ENV",
    "PTG2_BLOCK_BUILD_PIN_TABLE",
    "delete_shared_block_build_pins",
    "maintain_shared_block_build_pin_lease",
    "pin_shared_block_hashes",
    "is_pin_lease_renewed",
]
