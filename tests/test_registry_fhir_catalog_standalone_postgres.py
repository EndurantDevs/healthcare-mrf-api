# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Standalone split-revision enqueue retains genuine Reader custody through commit."""

import json
from types import SimpleNamespace
from uuid import uuid4

import asyncpg
import pytest

from process.registry_approval_store import RegistryApprovalConflict
from process.registry_fhir_catalog_grant import build_fhir_catalog_publication_runtime
from process.registry_fhir_catalog_publication import OPERATION, enqueue_fhir_catalog_publication
from process.registry_fhir_catalog_registry_reader import (
    fhir_catalog_registry_connection,
    fhir_catalog_registry_reads,
    verify_fhir_catalog_registry_reads,
)
from process.registry_fhir_catalog_sync import prepare_fhir_catalog_snapshot
from tests.fhir_catalog_registry_reader_support import configured_catalog_reader
from tests.registry_candidate_custody_split_postgres_support import _login
from tests.registry_candidate_custody_split_postgres_support import split_db as split_db
from tests.test_network_custom_address_source_postgres import custom_db as custom_db
from tests.test_network_serving_schema_postgres import serving_schema as serving_schema
from tests.test_registry_fhir_catalog_observed_source_postgres import observed_source_db as observed_source_db
from tests.test_registry_fhir_catalog_sealed_attachment_postgres import (
    _catalog_pool,
    _catalog_roles,
    _observed_reader,
    _prepare_observed_catalog_roles,
    _publish_sealed_attachment,
)


async def _pending_approval(fixture, revision):
    """Add a real pending pair, keeping every previously approved document identical."""
    connection, namespace = fixture.fixture.connection, '"' + fixture.fixture.control_schema + '"'
    network_id = await connection.fetchval(
        f"INSERT INTO {namespace}.network_registry_identity(allocation_key) VALUES($1) RETURNING network_id", uuid4()
    )
    await connection.execute(
        f"INSERT INTO {namespace}.network_registry_alias "
        "(source_system,source_id,alias_type,alias_value,scope_key,network_id,evidence_id) "
        "VALUES('fhir','pending-source','fhir_organization','pending-network','fhir-catalog/v1',$1,'fixture')",
        network_id,
    )
    await connection.execute(
        f"INSERT INTO {namespace}.registry_approved_record SELECT $1,record_kind,record_key,record_revision,"
        f"custom_revision,record_json FROM {namespace}.registry_approved_record WHERE approved_revision=$2",
        revision + 1,
        revision,
    )
    binding_id = str(uuid4())
    network_by_field = {
        "network_id": network_id,
        "display_name": "Pending network",
        "aliases": [],
        "archived": False,
        "revision": 1,
    }
    binding_by_field = {
        "binding_id": binding_id,
        "binding_key": "b" * 64,
        "network_id": network_id,
        "archived": False,
        "revision": 1,
        "source_system": "fhir",
        "source_id": "pending-source",
        "source_key": "pending-network",
        "producer_id": "registry-source-fhir-witness-v1",
        "source_scope_json": {"organization_id": "pending-network", "capture_sha256": "c" * 64},
    }
    await connection.copy_records_to_table(
        "registry_approved_record",
        schema_name=fixture.fixture.control_schema,
        columns=("approved_revision", "record_kind", "record_key", "record_revision", "custom_revision", "record_json"),
        records=[
            (revision + 1, "network", str(network_id), 1, revision, json.dumps(network_by_field)),
            (revision + 1, "network_binding", binding_id, 1, revision, json.dumps(binding_by_field)),
        ],
    )
    await connection.execute(
        f"UPDATE {namespace}.registry_revision_control SET draft_revision=$1,approved_revision=$1", revision + 1
    )
    return revision + 1


@pytest.fixture
async def standalone_catalog(split_db, observed_source_db, monkeypatch):
    manifest, directory_target = await _publish_sealed_attachment(split_db)
    async with _catalog_roles(split_db) as roles_by_kind:
        config = await _prepare_observed_catalog_roles(split_db, observed_source_db, roles_by_kind)
        logins = SimpleNamespace(dsn=split_db.dsn, roles_by_kind=roles_by_kind, role_password=split_db.role_password)
        source_pool = SimpleNamespace(acquire=lambda: _observed_reader(observed_source_db))
        async with (
            configured_catalog_reader(
                monkeypatch, split_db.dsn, split_db.roles_by_kind["reader"], split_db.role_password
            ),
            _login(logins, "loader") as loader,
            _catalog_pool(split_db, roles_by_kind["sealer"], 2) as sealer_pool,
        ):
            snapshot, previous, _ = await prepare_fhir_catalog_snapshot(config, loader, source_pool, sealer_pool)
            assert previous is None and snapshot["metadata_approved_revision"] == manifest["approved_custom_revision"]
            revision = await _pending_approval(split_db, snapshot["metadata_approved_revision"])
            command_by_field = {
                "operation": OPERATION,
                "metadata_generation": snapshot["metadata_generation"],
                "expected_metadata_generation": None,
                "expected_serving_generation": manifest["generation_id"],
                "expected_approved_revision": revision,
                "standing_grant_sha256": config.grant.sha256,
            }
            yield SimpleNamespace(
                fixture=split_db,
                writer=loader,
                relation=f'"{directory_target.schema_name}".network_membership',
                command=command_by_field,
                runtime=build_fhir_catalog_publication_runtime(config.grant, source_pool),
            )


async def _enqueue(case):
    return await enqueue_fhir_catalog_publication(
        case.writer,
        case.command,
        authorize_metadata=case.runtime.authorize_metadata,
        control_schema=case.fixture.fixture.control_schema,
    )


async def _scoped_enqueue(case, *, end_reader):
    lock_query = "SELECT EXISTS(SELECT FROM pg_locks WHERE pid=$1 AND relation=to_regclass($2) AND mode='AccessShareLock' AND granted)"
    admin = case.fixture.fixture.connection
    async with fhir_catalog_registry_reads(case.writer):
        async with case.writer.transaction(isolation="repeatable_read"):
            receipt = await _enqueue(case)
            reader = await fhir_catalog_registry_connection(case.writer)
            reader_pid = reader.get_server_pid()
            assert await admin.fetchval(lock_query, reader_pid, case.relation)
            if end_reader:
                await reader.execute("ROLLBACK")
            await verify_fhir_catalog_registry_reads()
        assert not case.writer.is_in_transaction()
        assert await admin.fetchval(lock_query, reader_pid, case.relation)
    assert not await admin.fetchval(lock_query, reader_pid, case.relation)
    return receipt


@pytest.mark.parametrize("end_reader", [False, True])
async def test_standalone_split_revision_enqueue_requires_reader_until_writer_commit(standalone_catalog, end_reader):
    case = standalone_catalog
    with pytest.raises(asyncpg.InsufficientPrivilegeError):
        await case.writer.fetchval(f"SELECT count(*) FROM {case.relation}")
    with pytest.raises(RegistryApprovalConflict, match="snapshot_changed"):
        async with case.writer.transaction(isolation="repeatable_read"):
            await _enqueue(case)
    if end_reader:
        with pytest.raises(ValueError, match="registry_reader_changed"):
            await _scoped_enqueue(case, end_reader=True)
    else:
        receipt = await _scoped_enqueue(case, end_reader=False)
        assert receipt["state"] == "queued"
    assert await case.fixture.fixture.connection.fetchval(
        f'SELECT count(*) FROM "{case.fixture.fixture.control_schema}".registry_publication_request'
    ) == int(not end_reader)
