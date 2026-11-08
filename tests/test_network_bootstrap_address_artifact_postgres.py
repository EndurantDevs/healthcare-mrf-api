# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Genuine restricted bootstrap artifact reads on the existing isolated native fixture."""

import asyncio
import copy
import json
from contextlib import asynccontextmanager
from dataclasses import replace

import asyncpg
import pytest

from process import network_address_artifact as artifact
from process import network_bootstrap_address_artifact as bootstrap_artifact
from process import network_bootstrap_read_access as access
from process import network_bootstrap_sources as bootstrap
from process.network_custom_address_source import NetworkCustomAddressSourceError
from tests.test_network_custom_address_source_postgres import (
    _bootstrap_access_login,
    _change_bootstrap_access,
    _renew_bootstrap_access,
)
from tests.test_network_custom_address_source_postgres import (
    bootstrap_access_fixture as bootstrap_access_fixture,
)
from tests.test_network_custom_address_source_postgres import (
    bootstrap_db as bootstrap_db,
)
from tests.test_network_custom_address_source_postgres import (
    serving_schema as serving_schema,
)

pytestmark = pytest.mark.asyncio


class _ObservedConnection:
    def __init__(self, connection):
        self.connection = connection
        self.statements = []

    def __getattr__(self, name):
        method = getattr(self.connection, name)
        if name not in {"execute", "fetch", "fetchrow", "fetchval"}:
            return method

        async def observed(statement, *arguments, **kwargs):
            self.statements.append(statement)
            return await method(statement, *arguments, **kwargs)

        return observed


async def _capture(fixture, connection, *, roles=None):
    async with connection.transaction(isolation="repeatable_read"):
        return await artifact.capture_network_address_artifact(
            connection,
            fixture.receipt.address_source,
            runtime_roles=roles or tuple(sorted((fixture.reader, fixture.stranger))),
        )


async def _verify(fixture, connection, descriptor, *, api_read=True):
    async with connection.transaction(isolation="repeatable_read", readonly=api_read):
        return await artifact.verify_network_address_artifact(
            connection, descriptor, runtime_roles=tuple(descriptor["runtime_roles"]), api_read=api_read
        )


@pytest.mark.parametrize("bootstrap_access_fixture", ["constant", "serial"], indirect=True)
async def test_genuine_bootstrap_artifact_reuses_closed_content_for_worker_and_api(
    bootstrap_access_fixture, monkeypatch
):
    fixture, receipt = bootstrap_access_fixture, bootstrap_access_fixture.receipt
    await _change_bootstrap_access(fixture)

    async def no_content_scan(*_):
        pytest.fail("Immutable bootstrap artifact reuse must not rescan either heap")

    monkeypatch.setattr(bootstrap, "_content", no_content_scan)
    async with _bootstrap_access_login(fixture.reader, fixture.password) as reader:
        assert not await reader.fetchval(
            "SELECT has_table_privilege($1,$2::oid,'SELECT')", fixture.stranger, dict(receipt.relation_oids)["npi"]
        )
        descriptor = await _capture(fixture, reader)
        assert descriptor["contract"] == bootstrap_artifact.CONTRACT
        assert descriptor["bootstrap_receipt"] == json.loads(artifact._json(receipt.as_dict()))
        assert descriptor["generation_id"] == receipt.generation_sha256
        assert set(descriptor["heaps"]) == {"entity_address_unified", "npi"}
        expected_oids = {oid for _, oid in (*receipt.relation_oids, *receipt.index_oids)}
        sequence_oid = descriptor["dormant_sequence_oid"]
        if sequence_oid is not None:
            expected_oids.add(sequence_oid)
        assert descriptor["dependency_oids"] == sorted(expected_oids)
        assert await _verify(fixture, reader, descriptor, api_read=False)

        observed = _ObservedConnection(reader)
        resolved = await _verify(fixture, observed, descriptor)
        assert resolved.table_oid == dict(receipt.relation_oids)["entity_address_unified"]
        assert all(
            statement.lstrip().upper().startswith(("SELECT", "WITH", "SHOW")) for statement in observed.statements
        )
        assert not any("LOCK TABLE" in statement or "pg_try_advisory" in statement for statement in observed.statements)


@pytest.mark.parametrize("changed", ["receipt", "filenode", "native"])
async def test_rehashed_descriptor_requires_current_protected_evidence(bootstrap_access_fixture, changed):
    fixture = bootstrap_access_fixture
    await _change_bootstrap_access(fixture)
    async with _bootstrap_access_login(fixture.reader, fixture.password) as reader:
        descriptor = copy.deepcopy(await _capture(fixture, reader))
        if changed == "receipt":
            receipt = replace(fixture.receipt, capture_snapshot="forged")
            receipt = replace(receipt, generation_sha256=bootstrap._generation(receipt))
            descriptor["bootstrap_receipt"] = json.loads(artifact._json(receipt.as_dict()))
            descriptor["generation_id"] = receipt.generation_sha256
        elif changed == "filenode":
            descriptor["heaps"]["npi"]["relfilenode"] += 1
        else:
            descriptor["native_sha256"] = "f" * 64
        descriptor["descriptor_sha256"] = artifact._digest(
            {key: value for key, value in descriptor.items() if key != "descriptor_sha256"}
        )
        assert bootstrap_artifact._descriptor(descriptor) == descriptor
        with pytest.raises(ValueError, match="[Bb]ootstrap"):
            await _verify(fixture, reader, descriptor, api_read=False)


@pytest.mark.parametrize("drift", ["acl", "native", "receipt"])
async def test_worker_reuse_refuses_changed_native_custody(bootstrap_access_fixture, drift):
    fixture = bootstrap_access_fixture
    await _change_bootstrap_access(fixture)
    async with _bootstrap_access_login(fixture.reader, fixture.password) as reader:
        descriptor = await _capture(fixture, reader)
    connection = fixture.fixture.connection
    namespace = '"' + fixture.receipt.address_source.schema_name + '"'
    transaction = connection.transaction(isolation="repeatable_read")
    await transaction.start()
    try:
        if drift == "acl":
            await connection.execute(f'GRANT UPDATE ON {namespace}.npi TO "{fixture.reader}"')
        elif drift == "native":
            await connection.execute(f"ALTER TABLE {namespace}.npi ADD COLUMN unexpected integer")
        else:
            await connection.execute(f"COMMENT ON SCHEMA {namespace} IS NULL")
        await connection.execute(f'SET LOCAL ROLE "{fixture.reader}"')
        with pytest.raises(ValueError, match="[Bb]ootstrap"):
            await _capture(fixture, connection)
        with pytest.raises(ValueError, match="[Bb]ootstrap"):
            await _verify(fixture, connection, descriptor, api_read=False)
    finally:
        await transaction.rollback()


async def test_worker_reuse_keeps_both_heap_locks_until_transaction_end(bootstrap_access_fixture):
    fixture = bootstrap_access_fixture
    await _change_bootstrap_access(fixture)
    namespace = '"' + fixture.receipt.address_source.schema_name + '"'
    tables = f"{namespace}.entity_address_unified,{namespace}.npi"
    administrator = fixture.fixture.connection
    async with _bootstrap_access_login(fixture.reader, fixture.password) as reader:
        async with reader.transaction(isolation="repeatable_read"):
            descriptor = await _capture(fixture, reader)
            assert await _verify(fixture, reader, descriptor, api_read=False)
            locked = await reader.fetch(
                "SELECT relation::bigint FROM pg_locks WHERE pid=pg_backend_pid() AND mode='AccessShareLock' "
                "AND granted AND relation=ANY($1::oid[]) ORDER BY relation",
                [oid for _, oid in fixture.receipt.relation_oids],
            )
            assert {row["relation"] for row in locked} == {oid for _, oid in fixture.receipt.relation_oids}
            with pytest.raises(asyncpg.LockNotAvailableError):
                async with administrator.transaction():
                    await administrator.execute(f"LOCK TABLE {tables} IN ACCESS EXCLUSIVE MODE NOWAIT")
        async with administrator.transaction():
            await administrator.execute(f"LOCK TABLE {tables} IN ACCESS EXCLUSIVE MODE NOWAIT")


async def test_access_proof_renewal_preserves_existing_bootstrap_descriptor(bootstrap_access_fixture):
    fixture = bootstrap_access_fixture
    proof = await _change_bootstrap_access(fixture)
    async with _bootstrap_access_login(fixture.reader, fixture.password) as reader:
        descriptor = await _capture(fixture, reader)
        await fixture.fixture.connection.execute(f'GRANT "{fixture.reader}" TO "{fixture.stranger}"')
        with pytest.raises(NetworkCustomAddressSourceError):
            await _verify(fixture, reader, descriptor)
        renewed = await _renew_bootstrap_access(fixture, proof)
        assert renewed["roles_sha256"] != proof["roles_sha256"]
        assert await _verify(fixture, reader, descriptor)
        assert await _capture(fixture, reader) == descriptor


@pytest.mark.parametrize("heap", ["entity_address_unified", "npi"])
async def test_full_verification_rejects_changed_content_in_either_heap(bootstrap_access_fixture, heap):
    fixture = bootstrap_access_fixture
    await _change_bootstrap_access(fixture)
    async with _bootstrap_access_login(fixture.reader, fixture.password) as reader:
        await _capture(fixture, reader)
        qualified = f'"{fixture.receipt.address_source.schema_name}"."{heap}"'
        field = "first_line" if heap == "entity_address_unified" else "provider_first_name"
        transaction = fixture.fixture.connection.transaction(isolation="repeatable_read")
        await transaction.start()
        try:
            await fixture.fixture.connection.execute(f"UPDATE {qualified} SET {field}='changed'")
            # The separate login sees committed state, so verify the changed copy as the reader in this transaction.
            await fixture.fixture.connection.execute(f'SET LOCAL ROLE "{fixture.reader}"')
            with pytest.raises(bootstrap.NetworkBootstrapSourceError, match="content or shape"):
                await bootstrap.verify_network_bootstrap_sources(fixture.fixture.connection, fixture.receipt)
        finally:
            await transaction.rollback()


@asynccontextmanager
async def _external_sequence_owner(fixture):
    administrator = fixture.fixture.connection
    parent_table = f'"{fixture.fixture.source_schema}".npi'
    original_owner = fixture.fixture.roles["loader"]
    sequence = await administrator.fetchval("SELECT pg_get_serial_sequence($1,'npi')", parent_table)
    try:
        # Ownership follows the original table; the retained heap stays protected.
        await administrator.execute(f'ALTER TABLE {parent_table} OWNER TO "{fixture.stranger}"')
        await administrator.execute(f'GRANT USAGE ON SCHEMA "{fixture.fixture.source_schema}" TO "{fixture.stranger}"')
        await administrator.execute(
            f'GRANT "{fixture.stranger}" TO "{fixture.reader}" WITH ADMIN FALSE, INHERIT FALSE, SET TRUE'
        )
        assert await administrator.fetchval(
            "SELECT relowner=$2::regrole FROM pg_class WHERE oid=$1::regclass", sequence, fixture.stranger
        )
        await _change_bootstrap_access(fixture)
        async with _bootstrap_access_login(fixture.stranger, fixture.password) as sequence_owner:
            yield sequence_owner, sequence
    finally:
        await administrator.execute(f'REVOKE "{fixture.stranger}" FROM "{fixture.reader}"')
        await administrator.execute(f'ALTER TABLE {parent_table} OWNER TO "{original_owner}"')
        await administrator.execute(
            f'REVOKE USAGE ON SCHEMA "{fixture.fixture.source_schema}" FROM "{fixture.stranger}"'
        )


async def _copy_retained_npi(fixture, reader):
    schema_name = fixture.receipt.address_source.schema_name
    async with reader.transaction(isolation="repeatable_read"):
        content = await bootstrap._content(reader, schema_name, "npi")
        await reader.execute(
            f'CREATE TEMP TABLE copied_npi(LIKE "{schema_name}".npi INCLUDING DEFAULTS) ON COMMIT DROP'
        )
        await bootstrap._copy_native_batch(
            reader, f'SELECT * FROM "{schema_name}".npi', (), "pg_temp", "copied_npi", content[0]
        )
        assert await bootstrap._content(reader, "pg_temp", "copied_npi") == content
        return content


async def _change_sequence_state(connection, sequence_oid, state, *, owner=None):
    try:
        if owner is not None:
            assert await connection.fetchval("SELECT pg_has_role(current_user,$1::name,'SET')", owner)
            assert not await connection.fetchval("SELECT pg_has_role(current_user,$1::name,'USAGE')", owner)
            assert not await connection.fetchval("SELECT has_sequence_privilege($1::oid,'USAGE,UPDATE')", sequence_oid)
            await connection.execute(f'SET ROLE "{owner}"')
            assert await connection.fetchval("SELECT current_user<>session_user AND current_user::text=$1", owner)
        await connection.fetchval("SELECT nextval($1::oid::regclass)", sequence_oid)
        assert await connection.fetchval("SELECT setval($1::oid::regclass,$2,true)", sequence_oid, state) == state
    finally:
        if owner is not None:
            await connection.execute("RESET ROLE")


async def _drop_sequence(connection, sequence):
    async with connection.transaction():
        await connection.execute("SET LOCAL lock_timeout='1000ms'")
        await connection.execute(f"DROP SEQUENCE {sequence} CASCADE")


async def _assert_sequence_drop_blocked(fixture, reader, sequence_owner, sequence):
    administrator = fixture.fixture.connection
    reader_pid = await reader.fetchval("SELECT pg_backend_pid()")
    owner_pid = await sequence_owner.fetchval("SELECT pg_backend_pid()")
    npi_oid = dict(fixture.receipt.relation_oids)["npi"]
    drop_task = asyncio.create_task(_drop_sequence(sequence_owner, sequence))
    try:
        pending_lock = None
        for _ in range(50):
            pending_lock = await administrator.fetchrow(
                "SELECT mode,pg_blocking_pids(pid) AS blockers FROM pg_locks "
                "WHERE pid=$1 AND NOT granted AND relation=$2::oid",
                owner_pid,
                npi_oid,
            )
            if pending_lock:
                break
            await asyncio.sleep(0.01)
        assert pending_lock and pending_lock["mode"] == "AccessExclusiveLock"
        assert reader_pid in pending_lock["blockers"]
        with pytest.raises(asyncpg.LockNotAvailableError):
            await asyncio.wait_for(drop_task, 3)
    finally:
        if not drop_task.done():
            drop_task.cancel()
        await asyncio.gather(drop_task, return_exceptions=True)


async def _assert_cascade_refusal(fixture, reader, sequence_owner, sequence, descriptor):
    receipt = fixture.receipt
    npi_oid = dict(receipt.relation_oids)["npi"]
    async with reader.transaction(isolation="repeatable_read"):
        assert await _verify(fixture, reader, descriptor, api_read=False)
        original_shape = await bootstrap._shape(reader, npi_oid)
        original_content = await bootstrap._content(reader, receipt.address_source.schema_name, "npi")
        await _assert_sequence_drop_blocked(fixture, reader, sequence_owner, sequence)
    await _drop_sequence(sequence_owner, sequence)
    assert await fixture.fixture.connection.fetchval("SELECT to_regclass($1)", sequence) is None
    async with reader.transaction(isolation="repeatable_read", readonly=True):
        assert await bootstrap._content(reader, receipt.address_source.schema_name, "npi") == original_content
        assert await bootstrap._shape(reader, npi_oid) != original_shape
        with pytest.raises(bootstrap.NetworkBootstrapSourceError):
            await bootstrap.verify_network_bootstrap_sources(reader, receipt)
    for api_read in (True, False):
        with pytest.raises(NetworkCustomAddressSourceError, match="[Bb]ootstrap"):
            await _verify(fixture, reader, descriptor, api_read=api_read)


@pytest.mark.parametrize("bootstrap_access_fixture", ["serial"], indirect=True)
async def test_sequence_ownership_keeps_read_boundary(bootstrap_access_fixture):
    fixture = bootstrap_access_fixture
    administrator = fixture.fixture.connection
    async with (
        _external_sequence_owner(fixture) as (sequence_owner, sequence),
        _bootstrap_access_login(fixture.reader, fixture.password) as reader,
    ):
        descriptor = await _capture(fixture, reader)
        sequence_oid = descriptor["dormant_sequence_oid"]
        assert sequence_oid is not None
        original_acl = await access._acl(administrator, fixture.receipt)
        assert not await reader.fetchval(
            "SELECT has_table_privilege($1,$2::oid,'SELECT')", fixture.stranger, descriptor["heaps"]["npi"]["oid"]
        )
        sequence_state = await administrator.fetchrow(f"SELECT last_value,is_called FROM {sequence}")
        original_content = await _copy_retained_npi(fixture, reader)
        assert await administrator.fetchrow(f"SELECT last_value,is_called FROM {sequence}") == sequence_state
        for ordinal, (connection, owner) in enumerate(((sequence_owner, None), (reader, fixture.stranger))):
            await _change_sequence_state(connection, sequence_oid, 7654321 + ordinal, owner=owner)
            assert await administrator.fetchval(f"SELECT last_value FROM {sequence}") == 7654321 + ordinal
            assert await _copy_retained_npi(fixture, reader) == original_content
            assert await _verify(fixture, reader, descriptor)
            assert await _verify(fixture, reader, descriptor, api_read=False)
            assert await _capture(fixture, reader) == descriptor
        async with reader.transaction(isolation="repeatable_read"):
            with pytest.raises(artifact.NetworkAddressArtifactError, match="source_generation_changed"):
                await artifact.capture_network_address_artifact(
                    reader,
                    replace(fixture.receipt.address_source, generation_id="f" * 64),
                    runtime_roles=(fixture.reader,),
                )
        await _assert_cascade_refusal(fixture, reader, sequence_owner, sequence, descriptor)
        assert await access._acl(administrator, fixture.receipt) == original_acl
