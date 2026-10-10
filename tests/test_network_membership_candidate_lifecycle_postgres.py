# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real native COPY, transactional resume and candidate closure checks."""

import asyncio
import hashlib
import importlib.util
import json
import os
from dataclasses import dataclass, replace
from pathlib import Path
from uuid import UUID, uuid4

import asyncpg
import pytest

from process.network_membership_candidate_lifecycle import (
    MembershipCandidateError,
    admit_network_membership_batch,
    create_network_candidate,
    require_candidate_authority,
    seal_network_candidate,
)
from process.network_membership_copy import MembershipCopyTarget

pytestmark = pytest.mark.asyncio


@dataclass
class CandidateFixture:
    writer: object
    observer: object
    postgres_dsn: str
    control_schema: str
    copy_target: MembershipCopyTarget


def _copy_target():
    candidate_id = uuid4()
    return MembershipCopyTarget(
        str(uuid4()), str(uuid4()), str(uuid4()), str(candidate_id), f"network_candidate_{candidate_id.hex}"
    )


def _membership_input(row_count=1):
    return json.dumps(
        [
            {
                "network_id": 42,
                "provider_system": "manual",
                "provider_id": "example-clinician",
                "location_id": "01234567-89ab-cdef-8123-456789abcdef",
                "evidence_id": "example-source-row",
            }
        ]
        * row_count,
        separators=(",", ":"),
    ).encode()


@pytest.fixture
async def candidate_lifecycle():
    postgres_dsn = os.getenv("NETWORK_REGISTRY_TEST_DSN")
    if not postgres_dsn:
        pytest.skip("Set NETWORK_REGISTRY_TEST_DSN for isolated real PostgreSQL checks")
    postgres_dsn = postgres_dsn.replace("postgresql+asyncpg://", "postgresql://", 1)
    writer = await asyncpg.connect(postgres_dsn)
    observer = await asyncpg.connect(postgres_dsn)
    control_schema = "candidate_control_test_" + uuid4().hex
    copy_target = _copy_target()
    try:
        await writer.execute(f'CREATE SCHEMA "{control_schema}"')
        for filename in (
            "20261007030000_network_serving_control.py",
            "20261007140000_registry_source_recipes.py",
        ):
            migration_path = Path(__file__).parents[1] / "alembic/versions" / filename
            spec = importlib.util.spec_from_file_location(filename.removesuffix(".py"), migration_path)
            migration = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(migration)
            for statement in migration._ddl(control_schema):
                await writer.execute(statement)
        yield CandidateFixture(writer, observer, postgres_dsn, control_schema, copy_target)
    finally:
        await writer.execute(f'DROP SCHEMA IF EXISTS "{copy_target.schema_name}" CASCADE')
        await writer.execute(f'DROP SCHEMA IF EXISTS "{control_schema}" CASCADE')
        assert not await observer.fetchval(
            "SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname=ANY($1::text[]))",
            [copy_target.schema_name, control_schema],
        )
        await observer.close()
        await writer.close()


async def _create(fixture, expected_rows=2, **overrides):
    metadata = dict(
        source_generations={"directory": 7}, approved_custom_revision=9, expected_head=11, expected_rows=expected_rows
    )
    metadata.update(overrides)
    return await create_network_candidate(
        fixture.writer, fixture.copy_target, control_schema=fixture.control_schema, **metadata
    )


async def _admit(fixture, batch_id, input_bytes=None, connection=None, copy_target=None):
    membership_input = _membership_input() if input_bytes is None else input_bytes
    return await admit_network_membership_batch(
        fixture.writer if connection is None else connection,
        fixture.copy_target if copy_target is None else copy_target,
        batch_id=batch_id,
        input_bytes=membership_input,
        expected_input_sha256=hashlib.sha256(membership_input).hexdigest(),
        control_schema=fixture.control_schema,
    )


async def _counts(fixture):
    candidate_id = UUID(fixture.copy_target.candidate_id)
    accepted_rows = await fixture.observer.fetchval(
        f'SELECT accepted_rows FROM "{fixture.control_schema}".network_membership_candidate WHERE candidate_id=$1',
        candidate_id,
    )
    batch_rows = await fixture.observer.fetchval(
        f'SELECT COALESCE(SUM(row_count),0) FROM "{fixture.control_schema}".network_membership_batch WHERE candidate_id=$1',
        candidate_id,
    )
    raw_rows = await fixture.observer.fetchval(
        f'SELECT COUNT(*) FROM "{fixture.copy_target.schema_name}".network_membership'
    )
    return accepted_rows, batch_rows, raw_rows


async def test_create_replay_is_exact_and_does_not_commit(candidate_lifecycle):
    fixture = candidate_lifecycle
    async with fixture.writer.transaction():
        created = await _create(fixture)
        assert created["state"] == "open" and created["accepted_rows"] == 0
        assert await _create(fixture) == created
        assert (
            await fixture.observer.fetchval(
                f'SELECT COUNT(*) FROM "{fixture.control_schema}".network_membership_candidate'
            )
            == 0
        )
        assert not await fixture.observer.fetchval(
            "SELECT to_regnamespace($1) IS NOT NULL", fixture.copy_target.schema_name
        )
    assert await _counts(fixture) == (0, 0, 0)
    columns = await fixture.observer.fetch(
        "SELECT column_name FROM information_schema.columns WHERE table_schema=$1 ORDER BY ordinal_position",
        fixture.copy_target.schema_name,
    )
    assert [column[0] for column in columns] == [
        "network_id",
        "provider_system",
        "provider_id",
        "location_id",
        "evidence_id",
    ]


@pytest.mark.parametrize(
    "metadata",
    [
        {"source_generations": {"directory": 8}},
        {"approved_custom_revision": 10},
        {"expected_head": 12},
        {"expected_rows": 3},
    ],
)
async def test_creation_metadata_is_immutable(candidate_lifecycle, metadata):
    fixture = candidate_lifecycle
    async with fixture.writer.transaction():
        created = await _create(fixture)
        with pytest.raises(MembershipCandidateError, match="immutable"):
            await _create(fixture, **metadata)
        assert await _create(fixture) == created


async def test_source_generation_replay_preserves_json_types(candidate_lifecycle):
    fixture = candidate_lifecycle
    async with fixture.writer.transaction():
        await _create(fixture, source_generations={"directory": 1})
        with pytest.raises(MembershipCandidateError, match="immutable"):
            await _create(fixture, source_generations={"directory": True})


@pytest.mark.parametrize(
    "metadata",
    [
        {"source_generations": {"registry_source_recipes": "a" * 64}},
        {"source_recipes": ({"source_kind": "fhir"},)},
    ],
)
async def test_recipe_metadata_rejected_before_creation(candidate_lifecycle, metadata):
    """Malformed recipes and unbound digests cannot allocate a candidate."""
    fixture = candidate_lifecycle
    async with fixture.writer.transaction():
        with pytest.raises(MembershipCandidateError, match="recipes"):
            await _create(fixture, **metadata)
        assert (
            await fixture.writer.fetchval(
                f'SELECT count(*) FROM "{fixture.control_schema}".network_membership_candidate'
            )
            == 0
        )
        assert await fixture.writer.fetchval("SELECT to_regnamespace($1)", fixture.copy_target.schema_name) is None


async def test_batch_replay_after_seal_is_read_only(candidate_lifecycle, monkeypatch):
    fixture = candidate_lifecycle
    batch_id = uuid4()
    async with fixture.writer.transaction():
        await _create(fixture)
        receipt = await _admit(fixture, batch_id, _membership_input(2))
        sealed = await seal_network_candidate(
            fixture.writer, fixture.copy_target, control_schema=fixture.control_schema
        )
        assert sealed["state"] == "sealed" and sealed["accepted_rows"] == 2
    assert await _counts(fixture) == (2, 2, 2)

    def forbidden_encoding(*args):
        raise AssertionError("Replay must not encode or COPY again")

    monkeypatch.setattr("process.network_membership_copy._encode", forbidden_encoding)
    async with fixture.writer.transaction():
        assert await _admit(fixture, batch_id, _membership_input(2)) == receipt
        assert await _create(fixture) == sealed
        assert (
            await seal_network_candidate(fixture.writer, fixture.copy_target, control_schema=fixture.control_schema)
            == sealed
        )
        with pytest.raises(MembershipCandidateError, match="different input"):
            await _admit(fixture, batch_id)
        with pytest.raises(MembershipCandidateError, match="closed"):
            await _admit(fixture, uuid4())
    assert await _counts(fixture) == (2, 2, 2)


async def test_failed_batches_preserve_prior_writes_and_resume(candidate_lifecycle):
    fixture = candidate_lifecycle
    final_batch = uuid4()
    async with fixture.writer.transaction():
        await _create(fixture)
        await _admit(fixture, uuid4())
        with pytest.raises(ValueError):
            await _admit(fixture, final_batch, b'[{"network_id":0}]')
        with pytest.raises(MembershipCandidateError, match="exceeds"):
            await _admit(fixture, final_batch, _membership_input(2))
        with pytest.raises(MembershipCandidateError, match="reconcile"):
            await seal_network_candidate(fixture.writer, fixture.copy_target, control_schema=fixture.control_schema)
        assert await fixture.writer.fetchval("SELECT 1") == 1
    assert await _counts(fixture) == (1, 1, 1)
    async with fixture.writer.transaction():
        await _admit(fixture, final_batch)
        sealed = await seal_network_candidate(
            fixture.writer, fixture.copy_target, control_schema=fixture.control_schema
        )
        assert sealed["state"] == "sealed"
    assert await _counts(fixture) == (2, 2, 2)


@pytest.mark.parametrize("identity_field", ["dataset_id", "schema_id", "producer_id", "candidate_id"])
async def test_wrong_scope_cannot_create_admit_or_seal(candidate_lifecycle, identity_field):
    fixture = candidate_lifecycle
    wrong_scope_map = {identity_field: str(uuid4())}
    if identity_field == "candidate_id":
        wrong_scope_map["schema_name"] = "network_candidate_" + UUID(wrong_scope_map[identity_field]).hex
    wrong_target = replace(fixture.copy_target, **wrong_scope_map)
    async with fixture.writer.transaction():
        await _create(fixture)
        with pytest.raises(MembershipCandidateError, match="scope|registered"):
            await _admit(fixture, uuid4(), copy_target=wrong_target)
        with pytest.raises(MembershipCandidateError, match="scope|registered"):
            await seal_network_candidate(fixture.writer, wrong_target, control_schema=fixture.control_schema)
        if identity_field != "candidate_id":
            with pytest.raises(MembershipCandidateError, match="scope"):
                await create_network_candidate(
                    fixture.writer,
                    wrong_target,
                    source_generations={"directory": 7},
                    approved_custom_revision=9,
                    expected_head=11,
                    expected_rows=2,
                    control_schema=fixture.control_schema,
                )
    assert await _counts(fixture) == (0, 0, 0)


async def test_transaction_and_configured_authority_are_required(candidate_lifecycle, monkeypatch):
    fixture = candidate_lifecycle
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", fixture.control_schema)
    for operation in (
        lambda: _create(fixture),
        lambda: _admit(fixture, uuid4()),
        lambda: require_candidate_authority(fixture.writer, fixture.copy_target),
        lambda: seal_network_candidate(fixture.writer, fixture.copy_target, control_schema=fixture.control_schema),
    ):
        with pytest.raises(MembershipCandidateError, match="caller-owned"):
            await operation()
    async with fixture.writer.transaction():
        await _create(fixture, expected_rows=0)
        assert await require_candidate_authority(fixture.writer, fixture.copy_target) == fixture.copy_target
        await seal_network_candidate(fixture.writer, fixture.copy_target, control_schema=fixture.control_schema)
        with pytest.raises(MembershipCandidateError, match="closed"):
            await require_candidate_authority(fixture.writer, fixture.copy_target)


@pytest.mark.parametrize(
    "tamper_sql",
    [
        "UPDATE {control}.network_membership_candidate SET accepted_rows=0",
        "UPDATE {control}.network_membership_batch SET row_count=0",
        "DELETE FROM {raw}.network_membership",
    ],
)
async def test_seal_rejects_all_accounting_mismatches(candidate_lifecycle, tamper_sql):
    fixture = candidate_lifecycle
    async with fixture.writer.transaction():
        await _create(fixture, expected_rows=1)
        await _admit(fixture, uuid4())
        await fixture.writer.execute(
            tamper_sql.format(control=f'"{fixture.control_schema}"', raw=f'"{fixture.copy_target.schema_name}"')
        )
        with pytest.raises(MembershipCandidateError, match="reconcile"):
            await seal_network_candidate(fixture.writer, fixture.copy_target, control_schema=fixture.control_schema)
        assert (
            await fixture.writer.fetchval(f'SELECT state FROM "{fixture.control_schema}".network_membership_candidate')
            == "open"
        )


async def test_outer_rollback_removes_candidate_data_and_receipts(candidate_lifecycle):
    fixture = candidate_lifecycle
    with pytest.raises(RuntimeError, match="caller rollback"):
        async with fixture.writer.transaction():
            await _create(fixture, expected_rows=1)
            await _admit(fixture, uuid4())
            await seal_network_candidate(fixture.writer, fixture.copy_target, control_schema=fixture.control_schema)
            raise RuntimeError("caller rollback")
    assert (
        await fixture.observer.fetchval(f'SELECT COUNT(*) FROM "{fixture.control_schema}".network_membership_candidate')
        == 0
    )
    assert not await fixture.observer.fetchval(
        "SELECT to_regnamespace($1) IS NOT NULL", fixture.copy_target.schema_name
    )


class PausedCopyConnection:
    """Pause a real binary COPY while its candidate control lock is held."""

    def __init__(self, connection):
        self.connection = connection
        self.paused = asyncio.Event()
        self.resume = asyncio.Event()
        self.copy_count = 0

    def __getattr__(self, name):
        return getattr(self.connection, name)

    async def copy_to_table(self, table, *, source, **options):
        self.copy_count += 1

        async def chunks():
            yield source.read()
            self.paused.set()
            await self.resume.wait()

        return await self.connection.copy_to_table(table, source=chunks(), **options)


async def _wait_for_lock(observer, writer_pid, waiting_pid):
    async def locked():
        while not await observer.fetchval("SELECT $1::integer=ANY(pg_blocking_pids($2))", writer_pid, waiting_pid):
            await asyncio.sleep(0.01)

    await asyncio.wait_for(locked(), 10)


async def test_inflight_native_copy_serializes_seal(candidate_lifecycle):
    fixture = candidate_lifecycle
    async with fixture.writer.transaction():
        await _create(fixture, expected_rows=1)
    paused_connection = PausedCopyConnection(fixture.writer)
    sealer = await asyncpg.connect(fixture.postgres_dsn)
    writer_pid = fixture.writer.get_server_pid()
    seal_pid = sealer.get_server_pid()

    async def transfer():
        async with fixture.writer.transaction():
            return await _admit(fixture, uuid4(), connection=paused_connection)

    async def close_candidate():
        async with sealer.transaction():
            return await seal_network_candidate(sealer, fixture.copy_target, control_schema=fixture.control_schema)

    transfer_task = asyncio.create_task(transfer())
    seal_task = None
    try:
        await asyncio.wait_for(paused_connection.paused.wait(), 10)
        seal_task = asyncio.create_task(close_candidate())
        await _wait_for_lock(fixture.observer, writer_pid, seal_pid)
        assert not seal_task.done()
        assert await _counts(fixture) == (0, 0, 0)
        paused_connection.resume.set()
        receipt, sealed = await asyncio.wait_for(asyncio.gather(transfer_task, seal_task), 10)
        assert receipt.row_count == 1 and sealed["state"] == "sealed" and sealed["accepted_rows"] == 1
    finally:
        pending_tasks = [task for task in (transfer_task, seal_task) if task is not None]
        for task in pending_tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*pending_tasks, return_exceptions=True)
        await sealer.close()
    assert await _counts(fixture) == (1, 1, 1)


async def test_cancelled_final_batch_is_resumable(candidate_lifecycle):
    fixture = candidate_lifecycle
    batch_id = uuid4()
    async with fixture.writer.transaction():
        await _create(fixture)
        await _admit(fixture, uuid4())
    paused_connection = PausedCopyConnection(fixture.writer)

    async def transfer():
        async with fixture.writer.transaction():
            await _admit(fixture, batch_id, connection=paused_connection)

    transfer_task = asyncio.create_task(transfer())
    try:
        await asyncio.wait_for(paused_connection.paused.wait(), 10)
        transfer_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await transfer_task
    finally:
        if not transfer_task.done():
            transfer_task.cancel()
        await asyncio.gather(transfer_task, return_exceptions=True)
    assert await _counts(fixture) == (1, 1, 1)
    async with fixture.writer.transaction():
        await _admit(fixture, batch_id)
        sealed = await seal_network_candidate(
            fixture.writer, fixture.copy_target, control_schema=fixture.control_schema
        )
        assert sealed["state"] == "sealed"
    assert await _counts(fixture) == (2, 2, 2)


async def test_concurrent_duplicate_batch_has_one_copy_and_receipt(candidate_lifecycle):
    fixture = candidate_lifecycle
    batch_id = uuid4()
    async with fixture.writer.transaction():
        await _create(fixture, expected_rows=1)
    connections = [await asyncpg.connect(fixture.postgres_dsn) for _ in range(3)]
    copy_connections = [PausedCopyConnection(connection) for connection in connections]
    for copy_connection in copy_connections:
        copy_connection.resume.set()

    async def transfer(copy_connection):
        async with copy_connection.transaction():
            return await _admit(fixture, batch_id, connection=copy_connection)

    try:
        receipts = await asyncio.wait_for(
            asyncio.gather(*(transfer(connection) for connection in copy_connections)), 10
        )
        assert receipts[0] == receipts[1] == receipts[2]
        assert sum(connection.copy_count for connection in copy_connections) == 1
    finally:
        await asyncio.gather(*(connection.close() for connection in connections))
    assert await _counts(fixture) == (1, 1, 1)
    assert (
        await fixture.observer.fetchval(f'SELECT COUNT(*) FROM "{fixture.control_schema}".network_membership_batch')
        == 1
    )


@pytest.mark.parametrize(
    "invalid_metadata",
    [
        {"expected_rows": -1},
        {"expected_head": True},
        {"approved_custom_revision": 2**63},
        {"source_generations": []},
        {"source_generations": {"directory": float("nan")}},
    ],
)
async def test_invalid_creation_metadata_is_rejected_before_mutation(candidate_lifecycle, invalid_metadata):
    fixture = candidate_lifecycle
    async with fixture.writer.transaction():
        with pytest.raises(MembershipCandidateError):
            await _create(fixture, **invalid_metadata)
        assert (
            await fixture.writer.fetchval(
                f'SELECT COUNT(*) FROM "{fixture.control_schema}".network_membership_candidate'
            )
            == 0
        )
        assert not await fixture.writer.fetchval(
            "SELECT to_regnamespace($1) IS NOT NULL", fixture.copy_target.schema_name
        )


async def test_failed_relation_creation_does_not_leave_control(candidate_lifecycle):
    fixture = candidate_lifecycle
    await fixture.writer.execute(f'CREATE SCHEMA "{fixture.copy_target.schema_name}"')
    async with fixture.writer.transaction():
        with pytest.raises(asyncpg.DuplicateSchemaError):
            await _create(fixture)
        assert (
            await fixture.writer.fetchval(
                f'SELECT COUNT(*) FROM "{fixture.control_schema}".network_membership_candidate'
            )
            == 0
        )


async def test_missing_candidate_relation_is_not_recreated(candidate_lifecycle):
    fixture = candidate_lifecycle
    async with fixture.writer.transaction():
        await _create(fixture)
    await fixture.writer.execute(f'DROP SCHEMA "{fixture.copy_target.schema_name}" CASCADE')
    async with fixture.writer.transaction():
        with pytest.raises(MembershipCandidateError, match="missing"):
            await _create(fixture)
        assert not await fixture.writer.fetchval(
            "SELECT to_regnamespace($1) IS NOT NULL", fixture.copy_target.schema_name
        )
