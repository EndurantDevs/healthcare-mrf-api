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


async def test_disconnected_final_batch_is_resumable(candidate_lifecycle):
    fixture = candidate_lifecycle
    batch_id = uuid4()
    async with fixture.writer.transaction():
        await _create(fixture)
        await _admit(fixture, uuid4())
    writer = await asyncpg.connect(fixture.postgres_dsn)
    paused_connection = PausedCopyConnection(writer)

    async def transfer():
        async with writer.transaction():
            await _admit(fixture, batch_id, connection=paused_connection)

    transfer_task = asyncio.create_task(transfer())
    try:
        await asyncio.wait_for(paused_connection.paused.wait(), 10)
        assert await fixture.observer.fetchval("SELECT pg_terminate_backend($1)", writer.get_server_pid())
        paused_connection.resume.set()
        with pytest.raises((asyncpg.PostgresConnectionError, asyncpg.InterfaceError)):
            await asyncio.wait_for(transfer_task, 10)
    finally:
        if not transfer_task.done():
            transfer_task.cancel()
        await asyncio.gather(transfer_task, return_exceptions=True)
        await writer.close()
    assert await fixture.writer.fetchval("SELECT 1") == 1
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


async def _retention_stage(fixture, rows=2):
    from process.registry_source_recipe_composition import RegistrySourceRecipeStage

    table_name = "registry_source_page_" + uuid4().hex
    await fixture.writer.execute(
        f'CREATE TEMP TABLE "{table_name}"(ordinal bigint PRIMARY KEY,input_json text NOT NULL)'
    )
    document = json.loads(_membership_input())[0]
    await fixture.writer.execute(
        f'INSERT INTO pg_temp."{table_name}" SELECT n,$1 FROM generate_series(1,$2::bigint) n',
        json.dumps(document),
        rows,
    )
    return RegistrySourceRecipeStage(table_name, rows, 0, "a" * 64)


async def test_registered_source_preserves_duplicates_and_requires_stage_before_seal(candidate_lifecycle):
    from process.registry_source_retention import SUMMARY_KEY, retain_registry_source_memberships
    from tests.test_registry_source_retention import retention_receipt

    fixture = candidate_lifecycle
    receipt = retention_receipt(add_rows=0, suppressed_source_rows=2, effective_rows=0)
    async with fixture.writer.transaction():
        created = await _create(
            fixture, expected_rows=0, source_retention=receipt, source_generations={"custom_membership": "b" * 64}
        )
        assert json.loads(created["validation_json"])[SUMMARY_KEY] == receipt
        assert (
            await _create(
                fixture, expected_rows=0, source_retention=receipt, source_generations={"custom_membership": "b" * 64}
            )
            == created
        )
        stage = await _retention_stage(fixture)
        await retain_registry_source_memberships(
            fixture.writer, fixture.copy_target, stage, control_schema=fixture.control_schema
        )
        await retain_registry_source_memberships(
            fixture.writer, fixture.copy_target, stage, control_schema=fixture.control_schema
        )
        assert (
            await fixture.writer.fetchval(
                f'SELECT count(*) FROM "{fixture.copy_target.schema_name}".network_membership_source'
            )
            == 2
        )
        with pytest.raises(MembershipCandidateError, match="stage_required"):
            await seal_network_candidate(fixture.writer, fixture.copy_target, control_schema=fixture.control_schema)
        with pytest.raises(MembershipCandidateError, match="command stage"):
            await seal_network_candidate(
                fixture.writer, fixture.copy_target, source_stage=stage, control_schema=fixture.control_schema
            )
        commands = await _command_stage(fixture, remove_rows=1)
        sealed = await seal_network_candidate(
            fixture.writer,
            fixture.copy_target,
            source_stage=stage,
            command_stage=commands,
            control_schema=fixture.control_schema,
        )
        assert sealed["state"] == "sealed"
        assert (
            await seal_network_candidate(fixture.writer, fixture.copy_target, control_schema=fixture.control_schema)
        )["state"] == "sealed"
        with pytest.raises(MembershipCandidateError, match="closed"):
            await retain_registry_source_memberships(
                fixture.writer, fixture.copy_target, stage, control_schema=fixture.control_schema
            )
        await fixture.writer.execute(
            f'DROP TABLE pg_temp."{stage.table_name}",pg_temp."{commands.add_table}",pg_temp."{commands.remove_table}"'
        )


@pytest.mark.parametrize("failure", ["stage_count", "native_row", "same_count_change", "raw_count"])
async def test_source_construction_and_seal_refuse_incomplete_or_changed_source(candidate_lifecycle, failure):
    from process.registry_source_retention import RegistrySourceRetentionError, retain_registry_source_memberships
    from tests.test_registry_source_retention import retention_receipt

    fixture = candidate_lifecycle
    receipt = retention_receipt(add_rows=0, suppressed_source_rows=2, effective_rows=0)
    async with fixture.writer.transaction():
        await _create(fixture, expected_rows=0, source_retention=receipt)
        stage = await _retention_stage(fixture, 1 if failure == "stage_count" else 2)
        if failure == "native_row":
            await fixture.writer.execute(f"UPDATE pg_temp.\"{stage.table_name}\" SET input_json='{{}}' WHERE ordinal=2")
        if failure in {"stage_count", "native_row"}:
            with pytest.raises(RegistrySourceRetentionError):
                await retain_registry_source_memberships(
                    fixture.writer, fixture.copy_target, stage, control_schema=fixture.control_schema
                )
            assert (
                await fixture.writer.fetchval(
                    f'SELECT count(*) FROM "{fixture.copy_target.schema_name}".network_membership_source'
                )
                == 0
            )
        else:
            await retain_registry_source_memberships(
                fixture.writer, fixture.copy_target, stage, control_schema=fixture.control_schema
            )
            source_relation = f'"{fixture.copy_target.schema_name}".network_membership_source'
            await fixture.writer.execute(
                f"UPDATE {source_relation} SET evidence_id='changed'"
                if failure == "same_count_change"
                else f"DELETE FROM {source_relation} WHERE ctid IN (SELECT ctid FROM {source_relation} LIMIT 1)"
            )
            with pytest.raises(MembershipCandidateError, match="retention"):
                await seal_network_candidate(
                    fixture.writer, fixture.copy_target, source_stage=stage, control_schema=fixture.control_schema
                )
        await fixture.writer.execute(f'DROP TABLE pg_temp."{stage.table_name}"')


async def test_source_registration_is_immutable_and_rejects_orphan_digest(candidate_lifecycle):
    from process.registry_source_retention import SUMMARY_KEY
    from tests.test_registry_source_retention import retention_receipt

    fixture = candidate_lifecycle
    async with fixture.writer.transaction():
        with pytest.raises(MembershipCandidateError):
            await _create(fixture, source_generations={SUMMARY_KEY: "a" * 64})
        receipt = retention_receipt()
        await _create(fixture, source_retention=receipt)
        with pytest.raises(MembershipCandidateError):
            await _create(fixture)
        with pytest.raises(MembershipCandidateError):
            await _create(fixture, source_retention={**receipt, "source_generation_sha256": "b" * 64})
        with pytest.raises(MembershipCandidateError):
            await _create(fixture, source_retention=receipt, source_generations={SUMMARY_KEY: "b" * 64})


async def test_source_options_unknown_key_refuses_before_connection_access():
    with pytest.raises(MembershipCandidateError, match="source options"):
        await create_network_candidate(
            object(),
            _copy_target(),
            source_generations={},
            approved_custom_revision=0,
            expected_head=0,
            expected_rows=0,
            unknown_source_option=True,
        )


async def test_retention_verification_refuses_other_producer_and_missing_registered_heap(candidate_lifecycle):
    from process.registry_source_retention import RegistrySourceRetentionError, verify_registry_source_retention_rows
    from tests.test_registry_source_retention import retention_receipt

    fixture = candidate_lifecycle
    receipt = retention_receipt(source_rows=0, add_rows=0, remove_rows=0, suppressed_source_rows=0, effective_rows=0)
    async with fixture.writer.transaction():
        candidate = await _create(fixture, expected_rows=0, source_retention=receipt)
        stage = await _retention_stage(fixture, 0)
        with pytest.raises(RegistrySourceRetentionError, match="scope"):
            await verify_registry_source_retention_rows(
                fixture.writer, replace(fixture.copy_target, producer_id=str(uuid4())), candidate, source_stage=stage
            )
        await fixture.writer.execute(f'DROP TABLE "{fixture.copy_target.schema_name}".network_membership_source')
        with pytest.raises(MembershipCandidateError, match="source relation is missing"):
            await _create(fixture, expected_rows=0, source_retention=receipt)
        await fixture.writer.execute(f'DROP TABLE pg_temp."{stage.table_name}"')


@pytest.mark.parametrize("source_rows", [1, 5000])
async def test_source_set_construction_uses_fixed_native_queries(candidate_lifecycle, source_rows):
    from process.registry_source_retention import retain_registry_source_memberships
    from tests.test_registry_source_retention import retention_receipt

    fixture = candidate_lifecycle
    receipt = retention_receipt(
        source_rows=source_rows, add_rows=0, suppressed_source_rows=source_rows, effective_rows=0
    )
    async with fixture.writer.transaction():
        await _create(fixture, expected_rows=0, source_retention=receipt)
        stage = await _retention_stage(fixture, source_rows)
        logged_queries = []
        fixture.writer.add_query_logger(logged_queries.append)
        try:
            await retain_registry_source_memberships(
                fixture.writer, fixture.copy_target, stage, control_schema=fixture.control_schema
            )
            await asyncio.sleep(0)
            assert len(logged_queries) == 9
        finally:
            fixture.writer.remove_query_logger(logged_queries.append)
        assert (
            await fixture.writer.fetchval(
                f'SELECT count(*) FROM "{fixture.copy_target.schema_name}".network_membership_source'
            )
            == source_rows
        )
        await fixture.writer.execute(f'DROP TABLE pg_temp."{stage.table_name}"')


@pytest.mark.parametrize("replay", [False, True])
async def test_qualified_admission_refuses_before_native_or_receipt_replay(candidate_lifecycle, monkeypatch, replay):
    import process.network_membership_candidate_lifecycle as lifecycle
    from tests.test_registry_source_retention import retention_receipt

    fixture = candidate_lifecycle
    receipt = retention_receipt(source_rows=0, add_rows=0, remove_rows=0, suppressed_source_rows=0, effective_rows=0)
    batch_id = uuid4()
    input_bytes = _membership_input(0)
    async with fixture.writer.transaction():
        await _create(fixture, expected_rows=0, source_retention=receipt)
        if replay:
            await fixture.writer.execute(
                f'INSERT INTO "{fixture.control_schema}".network_membership_batch '
                "(candidate_id,batch_id,row_count,input_sha256,copy_sha256,input_bytes,copy_bytes) VALUES($1,$2,0,$3,$3,$4,21)",
                UUID(fixture.copy_target.candidate_id),
                batch_id,
                hashlib.sha256(input_bytes).hexdigest(),
                len(input_bytes),
            )

    async def forbidden_native(*arguments, **options):
        raise AssertionError("qualified admission reached the native COPY writer")

    monkeypatch.setattr(lifecycle, "copy_network_membership_batch", forbidden_native)
    async with fixture.writer.transaction():
        with pytest.raises(MembershipCandidateError, match="set-based composition"):
            await _admit(fixture, batch_id, input_bytes)
    assert await _counts(fixture) == (0, 0, 0)
    assert await fixture.writer.fetchval(
        f'SELECT count(*) FROM "{fixture.control_schema}".network_membership_batch'
    ) == int(replay)


async def _command_stage(fixture, *, add_rows=0, remove_rows=0):
    """Build exact native-shaped command heaps for the actual production verifier."""
    from process.network_approved_membership_source import ApprovedMembershipSource
    from process.registry_membership_composition_stage import RegistryMembershipCommandStage

    suffix = uuid4().hex
    names = ("registry_command_add_" + suffix, "registry_command_remove_" + suffix)
    for name, count in zip(names, (add_rows, remove_rows), strict=True):
        await fixture.writer.execute(
            f'CREATE TEMP TABLE "{name}" (LIKE "{fixture.copy_target.schema_name}".network_membership INCLUDING CONSTRAINTS)'
        )
        if count:
            await fixture.writer.execute(
                f"INSERT INTO pg_temp.\"{name}\" SELECT 42,'manual',$2::text,'01234567-89ab-cdef-8123-456789abcdef'::uuid,'approved-command' FROM generate_series(1,$1::integer)",
                count,
                "added-clinician" if name == names[0] else "example-clinician",
            )
    return RegistryMembershipCommandStage(
        ApprovedMembershipSource(9, "b" * 64, add_rows + remove_rows), *names, add_rows, remove_rows
    )


@pytest.mark.parametrize("damage", ["none", "zero_receipt", "effective_evidence", "command_scope"])
async def test_qualified_seal_checks_actual_composition_and_stage_free_replay(candidate_lifecycle, damage):
    from process.registry_source_retention import retain_registry_source_memberships
    from tests.test_registry_source_retention import retention_receipt

    fixture = candidate_lifecycle
    receipt = retention_receipt(source_rows=2, add_rows=1, remove_rows=1, suppressed_source_rows=2, effective_rows=1)
    async with fixture.writer.transaction():
        await _create(
            fixture, expected_rows=1, source_retention=receipt, source_generations={"custom_membership": "b" * 64}
        )
        stage = await _retention_stage(fixture)
        await retain_registry_source_memberships(
            fixture.writer, fixture.copy_target, stage, control_schema=fixture.control_schema
        )
        commands = await _command_stage(fixture, add_rows=1, remove_rows=1)
        await fixture.writer.execute(
            f'INSERT INTO "{fixture.copy_target.schema_name}".network_membership SELECT * FROM pg_temp."{commands.add_table}"'
        )
        await fixture.writer.execute(
            f'UPDATE "{fixture.control_schema}".network_membership_candidate SET accepted_rows=1'
        )
        if damage == "zero_receipt":
            await fixture.writer.execute(
                f'INSERT INTO "{fixture.control_schema}".network_membership_batch (candidate_id,batch_id,row_count,input_sha256,copy_sha256,input_bytes,copy_bytes) VALUES($1,$2,0,$3,$3,2,21)',
                UUID(fixture.copy_target.candidate_id),
                uuid4(),
                "a" * 64,
            )
        elif damage == "effective_evidence":
            await fixture.writer.execute(
                f"UPDATE \"{fixture.copy_target.schema_name}\".network_membership SET evidence_id='different-evidence'"
            )
        elif damage == "command_scope":
            commands = replace(commands, source=replace(commands.source, generation_id="c" * 64))
        seal_options_by_name = dict(source_stage=stage, command_stage=commands, control_schema=fixture.control_schema)
        if damage == "none":
            assert (await seal_network_candidate(fixture.writer, fixture.copy_target, **seal_options_by_name))[
                "state"
            ] == "sealed"
            await fixture.writer.execute(
                f'DROP TABLE pg_temp."{stage.table_name}",pg_temp."{commands.add_table}",pg_temp."{commands.remove_table}"'
            )
            assert (
                await seal_network_candidate(fixture.writer, fixture.copy_target, control_schema=fixture.control_schema)
            )["accepted_rows"] == 1
        else:
            with pytest.raises(MembershipCandidateError, match="batch accounting|effective_parity|command_scope"):
                await seal_network_candidate(fixture.writer, fixture.copy_target, **seal_options_by_name)
            assert (
                await fixture.writer.fetchval(
                    f'SELECT state FROM "{fixture.control_schema}".network_membership_candidate'
                )
                == "open"
            )
            await fixture.writer.execute(
                f'DROP TABLE pg_temp."{stage.table_name}",pg_temp."{commands.add_table}",pg_temp."{commands.remove_table}"'
            )
