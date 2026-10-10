"""Real binary COPY parity and isolated-batch rollback checks."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import uuid
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from process.network_membership_copy import (
    MAX_INPUT_BYTES,
    MembershipCopyError,
    MembershipCopyTarget,
    copy_network_membership_batch,
)

POSTGRES_DSN_ENV = "HLTHPRT_NETWORK_MEMBERSHIP_POSTGRES_DSN"
asyncpg = pytest.importorskip("asyncpg")


def _target():
    candidate = uuid.uuid4()
    return MembershipCopyTarget(
        dataset_id=str(uuid.uuid4()),
        schema_id=str(uuid.uuid4()),
        producer_id=str(uuid.uuid4()),
        candidate_id=str(candidate),
        schema_name=f"network_candidate_{candidate.hex}",
    )


def _payload(rows=1):
    return json.dumps(
        [
            {
                "network_id": 42,
                "provider_system": "npi",
                "provider_id": "1000000491",
                "location_id": "01234567-89ab-cdef-8123-456789abcdef",
                "evidence_id": f"proof-{index}",
            }
            for index in range(rows)
        ],
        separators=(",", ":"),
    ).encode()


async def _copy(connection, target, authority, input_bytes=None):
    input_bytes = _payload() if input_bytes is None else input_bytes
    return await copy_network_membership_batch(
        connection,
        copy_target=target,
        input_bytes=input_bytes,
        expected_input_sha256=hashlib.sha256(input_bytes).hexdigest(),
        require_candidate_authority=authority,
    )


@pytest.fixture
async def candidate():
    postgres_dsn = os.getenv(POSTGRES_DSN_ENV)
    if not postgres_dsn:
        pytest.skip(f"Set {POSTGRES_DSN_ENV} for isolated real PostgreSQL checks")
    observer = await asyncpg.connect(postgres_dsn)
    writer = None
    copy_target = _target()
    schema = copy_target.schema_name
    try:
        await observer.execute(f'CREATE SCHEMA "{schema}"')
        await observer.execute(f"""CREATE TABLE "{schema}".network_membership (
            network_id integer NOT NULL CHECK (network_id > 0),
            provider_system text NOT NULL,
            provider_id text NOT NULL,
            location_id uuid NOT NULL,
            evidence_id text NOT NULL,
            PRIMARY KEY(network_id,provider_system,provider_id,location_id,evidence_id)
        )""")
        await observer.execute(f"""CREATE TABLE "{schema}".candidate_control (
            dataset_id uuid NOT NULL, schema_id uuid NOT NULL,
            producer_id uuid NOT NULL, candidate_id uuid PRIMARY KEY,
            is_open boolean NOT NULL
        )""")
        await observer.execute(
            f'INSERT INTO "{schema}".candidate_control VALUES ($1,$2,$3,$4,true)',
            uuid.UUID(copy_target.dataset_id),
            uuid.UUID(copy_target.schema_id),
            uuid.UUID(copy_target.producer_id),
            uuid.UUID(copy_target.candidate_id),
        )
        writer = await asyncpg.connect(postgres_dsn)
        calls = []

        async def authority(connection, requested):
            record = await connection.fetchrow(
                f'SELECT * FROM "{schema}".candidate_control WHERE candidate_id=$1 FOR SHARE',
                uuid.UUID(requested.candidate_id),
            )
            calls.append(requested)
            if record is None or not record["is_open"]:
                raise MembershipCopyError("Candidate is unavailable or sealed")
            return MembershipCopyTarget(
                dataset_id=str(record["dataset_id"]),
                schema_id=str(record["schema_id"]),
                producer_id=str(record["producer_id"]),
                candidate_id=str(record["candidate_id"]),
                schema_name=schema,
            )

        yield SimpleNamespace(writer=writer, observer=observer, target=copy_target, authority=authority, calls=calls)
    finally:
        if writer is not None:
            await writer.close()
        try:
            await observer.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            assert await observer.fetchval("SELECT to_regnamespace($1)", schema) is None
        finally:
            await observer.close()


@pytest.mark.parametrize(
    "changed",
    [
        {"schema_name": "public"},
        {"table_name": "live_membership"},
        {"dataset_id": "00000000-0000-0000-0000-000000000000"},
        {"producer_id": "not-a-uuid"},
    ],
)
def test_copy_target_rejects_live_relations_and_incomplete_ownership(changed):
    with pytest.raises(MembershipCopyError):
        replace(_target(), **changed)


async def test_input_bounds_and_digest_reject_before_native_import_or_io():
    target = _target()
    authority = AsyncMock()
    with patch("process.network_membership_copy.importlib.import_module") as load_native:
        with pytest.raises(MembershipCopyError, match="bounded bytes"):
            await copy_network_membership_batch(
                object(),
                copy_target=target,
                input_bytes=b" " * (MAX_INPUT_BYTES + 1),
                expected_input_sha256="a" * 64,
                require_candidate_authority=authority,
            )
        with pytest.raises(MembershipCopyError, match="digest mismatch"):
            await copy_network_membership_batch(
                object(),
                copy_target=target,
                input_bytes=b"[]",
                expected_input_sha256="a" * 64,
                require_candidate_authority=authority,
            )
        load_native.assert_not_called()
        authority.assert_not_awaited()


async def test_real_copy_readback_digest_and_caller_commit(candidate):
    fixture = candidate
    membership_input = _payload(2)
    membership_records = json.loads(membership_input)
    membership_records[1]["location_id"] = "01234567-89ab-cdef-8123-456789abcde0"
    membership_input = json.dumps(membership_records, separators=(",", ":")).encode()
    async with fixture.writer.transaction():
        receipt = await _copy(fixture.writer, fixture.target, fixture.authority, membership_input)
        assert receipt.row_count == 2
        assert receipt.input_byte_count == len(membership_input)
        assert receipt.input_sha256 == hashlib.sha256(membership_input).hexdigest()
        from ptg2_address_canon import encode_network_membership_batch

        wire, _ = encode_network_membership_batch(membership_input)
        assert receipt.copy_sha256 == hashlib.sha256(wire).hexdigest()
        assert receipt.copy_byte_count == len(wire)
        assert (
            await fixture.observer.fetchval(f'SELECT COUNT(*) FROM "{fixture.target.schema_name}".network_membership')
            == 0
        )
        stored_memberships = await fixture.writer.fetch(
            f'SELECT * FROM "{fixture.target.schema_name}".network_membership ORDER BY evidence_id'
        )
        assert [str(membership_row["location_id"]) for membership_row in stored_memberships] == [
            membership_row["location_id"] for membership_row in membership_records
        ]
        assert all(
            membership_row["network_id"] == 42
            and membership_row["provider_system"] == "npi"
            and membership_row["provider_id"] == "1000000491"
            for membership_row in stored_memberships
        )
    assert (
        await fixture.observer.fetchval(f'SELECT COUNT(*) FROM "{fixture.target.schema_name}".network_membership') == 2
    )
    assert len(fixture.calls) == 2


async def test_no_transaction_and_wrong_target_are_denied(candidate):
    with pytest.raises(MembershipCopyError, match="caller-owned transaction"):
        await _copy(candidate.writer, candidate.target, candidate.authority)
    assert candidate.calls == []
    async with candidate.writer.transaction():
        with pytest.raises(MembershipCopyError, match="authority does not match"):
            await _copy(candidate.writer, replace(candidate.target, producer_id=str(uuid.uuid4())), candidate.authority)
        assert (
            await candidate.writer.fetchval(f'SELECT COUNT(*) FROM "{candidate.target.schema_name}".network_membership')
            == 0
        )


async def test_constraint_and_post_copy_denial_roll_back_even_when_caught(candidate):
    async with candidate.writer.transaction():
        records = json.loads(_payload(2))
        records[1] = records[0]
        with pytest.raises(asyncpg.UniqueViolationError):
            await _copy(candidate.writer, candidate.target, candidate.authority, json.dumps(records).encode())
        assert (
            await candidate.writer.fetchval(f'SELECT COUNT(*) FROM "{candidate.target.schema_name}".network_membership')
            == 0
        )
        verified_targets = iter((candidate.target, replace(candidate.target, producer_id=str(uuid.uuid4()))))

        async def revoked_after_copy(connection, requested):
            await candidate.authority(connection, requested)
            return next(verified_targets)

        with pytest.raises(MembershipCopyError, match="authority changed"):
            await _copy(candidate.writer, candidate.target, revoked_after_copy)
        assert (
            await candidate.writer.fetchval(f'SELECT COUNT(*) FROM "{candidate.target.schema_name}".network_membership')
            == 0
        )
    assert (
        await candidate.observer.fetchval(f'SELECT COUNT(*) FROM "{candidate.target.schema_name}".network_membership')
        == 0
    )


@pytest.mark.parametrize("fault", ["row_count", "consumption"])
async def test_real_copy_driver_misreport_rolls_back_the_batch(candidate, fault):
    async def misreport(*args, **options):
        copy_status = await candidate.writer.copy_to_table(*args, **options)
        if fault == "row_count":
            return "COPY 99"
        options["source"].seek(0)
        return copy_status

    faulty_connection = SimpleNamespace(
        is_in_transaction=candidate.writer.is_in_transaction,
        transaction=candidate.writer.transaction,
        copy_to_table=misreport,
        fetchrow=candidate.writer.fetchrow,
    )
    async with candidate.writer.transaction():
        with pytest.raises(MembershipCopyError, match="count or driver consumption"):
            await _copy(faulty_connection, candidate.target, candidate.authority)
        assert (
            await candidate.writer.fetchval(f'SELECT COUNT(*) FROM "{candidate.target.schema_name}".network_membership')
            == 0
        )


class _PausedCopyConnection:
    """Fault injection still forwards the complete operation to real PostgreSQL."""

    def __init__(self, connection):
        self.connection = connection
        self.paused = asyncio.Event()

    def __getattr__(self, name):
        return getattr(self.connection, name)

    async def copy_to_table(self, table, *, source, **options):
        async def chunks():
            chunk = source.read(8192)
            yield chunk
            self.paused.set()
            await asyncio.Event().wait()

        return await self.connection.copy_to_table(table, source=chunks(), **options)


async def test_interrupted_real_copy_cancellation_leaves_no_rows(candidate):
    connection = _PausedCopyConnection(candidate.writer)

    async def transfer():
        async with candidate.writer.transaction():
            await _copy(connection, candidate.target, candidate.authority, _payload(5000))

    task = asyncio.create_task(transfer())
    try:
        await asyncio.wait_for(connection.paused.wait(), 10)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    finally:
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    assert await candidate.writer.fetchval("SELECT 1") == 1
    assert (
        await candidate.observer.fetchval(f'SELECT COUNT(*) FROM "{candidate.target.schema_name}".network_membership')
        == 0
    )


async def test_connection_failure_during_real_copy_leaves_no_rows(candidate):
    connection = _PausedCopyConnection(candidate.writer)
    backend_pid = await candidate.writer.fetchval("SELECT pg_backend_pid()")

    async def transfer():
        async with candidate.writer.transaction():
            await _copy(connection, candidate.target, candidate.authority, _payload(5000))

    task = asyncio.create_task(transfer())
    try:
        await asyncio.wait_for(connection.paused.wait(), 10)
        assert await candidate.observer.fetchval("SELECT pg_terminate_backend($1)", backend_pid)
        with pytest.raises((asyncpg.PostgresConnectionError, asyncpg.InterfaceError)):
            await asyncio.wait_for(task, 10)
    finally:
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    assert (
        await candidate.observer.fetchval(f'SELECT COUNT(*) FROM "{candidate.target.schema_name}".network_membership')
        == 0
    )
