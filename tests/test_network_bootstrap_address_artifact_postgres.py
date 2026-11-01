# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Genuine restricted bootstrap artifact reads on the existing isolated native fixture."""

import json
from dataclasses import replace

import pytest

from process import network_address_artifact as artifact
from process import network_bootstrap_address_artifact as bootstrap_artifact
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
async def test_genuine_bootstrap_artifact_full_capture_and_metadata_only_reader(bootstrap_access_fixture, monkeypatch):
    fixture, receipt = bootstrap_access_fixture, bootstrap_access_fixture.receipt
    await _change_bootstrap_access(fixture)
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

        async def no_content_scan(*_):
            pytest.fail("API verification must not scan either bootstrap heap")

        monkeypatch.setattr(bootstrap, "_content", no_content_scan)
        observed = _ObservedConnection(reader)
        resolved = await _verify(fixture, observed, descriptor)
        assert resolved.table_oid == dict(receipt.relation_oids)["entity_address_unified"]
        assert all(
            statement.lstrip().upper().startswith(("SELECT", "WITH", "SHOW")) for statement in observed.statements
        )
        assert not any("LOCK TABLE" in statement or "pg_try_advisory" in statement for statement in observed.statements)


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
        descriptor = await _capture(fixture, reader)
        qualified = f'"{fixture.receipt.address_source.schema_name}"."{heap}"'
        field = "first_line" if heap == "entity_address_unified" else "provider_first_name"
        transaction = fixture.fixture.connection.transaction(isolation="repeatable_read")
        await transaction.start()
        try:
            await fixture.fixture.connection.execute(f"UPDATE {qualified} SET {field}='changed'")
            # The separate login sees committed state, so verify the changed copy as the reader in this transaction.
            await fixture.fixture.connection.execute(f'SET LOCAL ROLE "{fixture.reader}"')
            with pytest.raises(bootstrap.NetworkBootstrapSourceError, match="content or shape"):
                await artifact.verify_network_address_artifact(
                    fixture.fixture.connection, descriptor, runtime_roles=tuple(descriptor["runtime_roles"])
                )
        finally:
            await transaction.rollback()


@pytest.mark.parametrize("bootstrap_access_fixture", ["serial"], indirect=True)
async def test_nonreader_sequence_reach_and_wrong_generation_are_refused(bootstrap_access_fixture):
    fixture = bootstrap_access_fixture
    await _change_bootstrap_access(fixture)
    async with _bootstrap_access_login(fixture.reader, fixture.password) as reader:
        descriptor = await _capture(fixture, reader)
        assert descriptor["dormant_sequence_oid"] is not None
        sequence = await fixture.fixture.connection.fetchval(
            "SELECT $1::oid::regclass::text", descriptor["dormant_sequence_oid"]
        )
        await fixture.fixture.connection.execute(f'GRANT USAGE ON SEQUENCE {sequence} TO "{fixture.stranger}"')
        try:
            with pytest.raises(NetworkCustomAddressSourceError):
                await _verify(fixture, reader, descriptor)
        finally:
            await fixture.fixture.connection.execute(f'REVOKE USAGE ON SEQUENCE {sequence} FROM "{fixture.stranger}"')
        assert await _verify(fixture, reader, descriptor)
        async with reader.transaction(isolation="repeatable_read"):
            with pytest.raises(artifact.NetworkAddressArtifactError, match="source_generation_changed"):
                await artifact.capture_network_address_artifact(
                    reader,
                    replace(fixture.receipt.address_source, generation_id="f" * 64),
                    runtime_roles=(fixture.reader,),
                )
