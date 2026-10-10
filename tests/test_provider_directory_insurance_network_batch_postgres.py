# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real native extraction, one binary COPY and savepoint-atomic FHIR evidence."""

import asyncio
import hashlib
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import asyncpg
import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import AsyncSession

from process import provider_directory_insurance_network_batch as batch
from process.provider_directory_insurance_network_identity import (
    record_insurance_network_organization,
    record_insurance_network_plan,
)
from process.registry_record_store import RegistryAddressUnavailable
from tests.test_network_serving_schema_postgres import serving_schema as serving_schema

pytestmark = pytest.mark.asyncio


def _org(resource_id="network-one", *, role=True):
    return {
        "resourceType": "Organization",
        "id": resource_id,
        "type": [{"text": "ntwk" if role else "ins"}],
        "name": "Synthetic organization",
    }


def _plan(resource_id="plan-one", refs=None):
    return {
        "resourceType": "InsurancePlan",
        "id": resource_id,
        "network": [{"reference": value} for value in refs or ["Organization/network-one"]],
        "ownedBy": {"reference": "Organization/payer-one"},
        "administeredBy": {"reference": "Organization/administrator-one"},
    }


@pytest.fixture
async def network_batch_db(serving_schema):
    connection, schema, engine = serving_schema
    async with engine.begin() as migration_connection:
        for filename in (
            "20260929010000_provider_directory_entity_identity.py",
            "20260929020000_provider_directory_insurance_network_identity.py",
        ):
            path = Path(__file__).parents[1] / "alembic/versions" / filename
            spec = importlib.util.spec_from_file_location(filename.removesuffix(".py"), path)
            migration = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(migration)

            def upgrade(sync_connection):
                with Operations.context(MigrationContext.configure(sync_connection)):
                    migration.upgrade()

            await migration_connection.run_sync(upgrade)
    yield connection, schema, engine.execution_options(schema_translate_map={"mrf": schema})


def _organization_evidence_document(resources):
    """Encode exact organization payload fingerprints for the existing set-based fixture."""
    organization_rows = [
        {
            "resource_id": resource["id"],
            "organization_id": str(uuid4()),
            "payload_json": resource,
            "payload_sha256": hashlib.sha256(
                json.dumps(
                    resource,
                    sort_keys=True,
                    separators=(",", ":"),
                    ensure_ascii=False,
                ).encode()
            ).hexdigest(),
        }
        for resource in resources
    ]
    return json.dumps(organization_rows)


async def _seed(db, resources, *, source="source-one", release="release-one"):
    connection, schema, _ = db
    document = _organization_evidence_document(resources)
    await connection.execute(
        f'INSERT INTO "{schema}".provider_directory_organization_identity '
        "SELECT organization_id,now() FROM jsonb_to_recordset($1::jsonb) r(organization_id uuid)",
        document,
    )
    await connection.execute(
        f'INSERT INTO "{schema}".provider_directory_entity_source_binding '
        "(source_id,resource_type,resource_id,organization_id,created_at) "
        "SELECT $2,'Organization',resource_id,organization_id,now() FROM jsonb_to_recordset($1::jsonb) "
        "r(resource_id text,organization_id uuid)",
        document,
        source,
    )
    if release is not None:
        await connection.execute(
            f'INSERT INTO "{schema}".provider_directory_entity_release_evidence '
            "SELECT $2,'Organization',resource_id,$3,payload_sha256,payload_json,now() "
            "FROM jsonb_to_recordset($1::jsonb) r(resource_id text,payload_sha256 text,payload_json jsonb)",
            document,
            source,
            release,
        )


async def _apply(session, resources, *, kind=None, source="source-one", release="release-one"):
    return await batch.record_insurance_network_batch(
        session,
        source_id=source,
        release_id=release,
        resource_type=kind or resources[0]["resourceType"],
        resources=resources,
    )


async def _counts(db):
    connection, schema, _ = db
    return tuple(
        await connection.fetchrow(
            f'SELECT (SELECT count(*) FROM "{schema}".provider_directory_insurance_network_identity),'
            f'(SELECT count(*) FROM "{schema}".provider_directory_insurance_network_source_binding),'
            f'(SELECT count(*) FROM "{schema}".provider_directory_insurance_network_plan_evidence)'
        )
    )


async def test_organization_plan_replay_and_raw_evidence(network_batch_db):
    db = network_batch_db
    connection, schema, engine = db
    org = _org()
    await _seed(db, [org])
    plan = _plan(
        refs=[
            "Organization/network-one",
            "Organization/unknown",
            "https://directory.example.test/Organization/network-one",
        ]
    )
    async with AsyncSession(engine) as session, session.begin():
        receipt = await _apply(session, [org, org, _org("payer", role=False)])
        assert receipt == {
            "input_count": 3,
            "duplicate_count": 1,
            "organization_count": 1,
            "plan_link_count": 0,
            "new_identity_count": 1,
            "new_plan_evidence_count": 0,
        }
        assert (await _apply(session, [org]))["new_identity_count"] == 0
        first = await _apply(session, [plan, plan])
        assert first == {
            "input_count": 2,
            "duplicate_count": 1,
            "organization_count": 0,
            "plan_link_count": 1,
            "new_identity_count": 0,
            "new_plan_evidence_count": 1,
        }
        assert (await _apply(session, [plan]))["new_plan_evidence_count"] == 0
    assert await _counts(db) == (1, 1, 1)
    evidence = await connection.fetchrow(f'SELECT * FROM "{schema}".provider_directory_insurance_network_plan_evidence')
    assert json.loads(evidence["network_refs"]) == [
        network_reference["reference"] for network_reference in plan["network"]
    ]
    assert json.loads(evidence["plan_payload_json"]) == plan
    assert evidence["owned_by_ref"] == "Organization/payer-one"
    assert evidence["administered_by_ref"] == "Organization/administrator-one"
    assert tuple(
        await connection.fetchrow(f'SELECT draft_revision,approved_revision FROM "{schema}".registry_revision_control')
    ) == (0, 0)


async def test_plan_allocates_only_retained_local_targets(network_batch_db):
    db = network_batch_db
    await _seed(db, [_org(), _org("network-two", role=False)])
    async with AsyncSession(db[2]) as session, session.begin():
        receipt = await _apply(
            session,
            [
                _plan(
                    refs=[
                        "Organization/network-one",
                        "Organization/network-two",
                        "Organization/unknown",
                        "https://directory.example.test/Organization/network-one",
                    ]
                )
            ],
        )
        assert receipt["plan_link_count"] == receipt["new_identity_count"] == 2
        assert receipt["new_plan_evidence_count"] == 2
    assert await _counts(db) == (2, 2, 2)


@pytest.mark.parametrize("refs", [[" Organization/network-one "], ["Organization/network-one "]])
async def test_padded_retained_reference_rejects_whole_batch(network_batch_db, refs):
    db = network_batch_db
    await _seed(db, [_org()])
    async with AsyncSession(db[2]) as session, session.begin():
        with pytest.raises(ValueError, match="_ref_missing$"):
            await _apply(session, [_plan("good"), _plan("padded", refs)])
        assert await session.scalar(text("SELECT 1")) == 1
    assert await _counts(db) == (0, 0, 0)


async def test_cross_batch_conflict_even_without_current_refs(network_batch_db):
    db = network_batch_db
    await _seed(db, [_org(), _org("network-two")])
    async with AsyncSession(db[2]) as session, session.begin():
        await _apply(session, [_plan()])
        with pytest.raises(ValueError, match="release_plan_conflict$"):
            await _apply(
                session,
                [_plan("new", ["Organization/network-two"]), _plan(refs=["https://directory.example.test/foreign"])],
            )
    assert await _counts(db) == (1, 1, 1)


@pytest.mark.parametrize("failure", ["entity", "release", "payload"])
async def test_explicit_role_requires_exact_entity_and_release(network_batch_db, failure):
    db = network_batch_db
    organization_dict = _org()
    if failure != "entity":
        await _seed(db, [organization_dict], release=None if failure == "release" else "release-one")
    if failure == "payload":
        organization_dict = {**organization_dict, "name": "Changed synthetic payload"}
    async with AsyncSession(db[2]) as session, session.begin():
        with pytest.raises(ValueError, match="_(organization_binding|release_evidence)_missing$"):
            await _apply(session, [organization_dict])
    assert await _counts(db) == (0, 0, 0)


async def test_missing_release_skips_plan_without_allocation(network_batch_db):
    db = network_batch_db
    await _seed(db, [_org()], release=None)
    async with AsyncSession(db[2]) as session, session.begin():
        result = await _apply(session, [_plan()])
        assert result["plan_link_count"] == result["new_identity_count"] == 0
    assert await _counts(db) == (0, 0, 0)


async def test_source_and_release_coordinates_remain_independent(network_batch_db):
    db = network_batch_db
    await _seed(db, [_org()])
    await _seed(db, [_org()], source="source-two")
    async with AsyncSession(db[2]) as session, session.begin():
        await _apply(session, [_plan()])
        await _apply(session, [_plan()], source="source-two")
        assert (await _apply(session, [_plan()], release="release-two"))["plan_link_count"] == 0
    connection, schema, _ = db
    assert await _counts(db) == (2, 2, 2)
    assert (
        await connection.fetchval(
            f'SELECT count(DISTINCT network_id) FROM "{schema}".provider_directory_insurance_network_source_binding'
        )
        == 2
    )


async def test_fixed_sql_count_and_one_copy_for_one_and_thousand(network_batch_db, monkeypatch):
    db = network_batch_db
    resources = [_org(f"network-{index}") for index in range(1000)]
    await _seed(db, resources)
    statements, copies = [], []
    event.listen(db[2].sync_engine, "before_cursor_execute", lambda *args: statements.append(args[2]))
    original = asyncpg.Connection.copy_to_table

    async def record_copy(connection, *args, **kwargs):
        copies.append(kwargs["columns"])
        return await original(connection, *args, **kwargs)

    monkeypatch.setattr(asyncpg.Connection, "copy_to_table", record_copy)
    async with AsyncSession(db[2]) as session, session.begin():
        first = await _apply(session, resources[:1])
        first_count = len(statements)
        assert first_count == 12
        statements.clear()
        second = await _apply(session, resources)
        assert len(statements) == first_count
        assert first["new_identity_count"] == 1
        assert second["new_identity_count"] == 999
    assert copies == [batch.COPY_COLUMNS, batch.COPY_COLUMNS]
    assert await _counts(db) == (1000, 1000, 0)


async def test_last_invalid_row_and_conflicting_duplicate_have_no_writes(network_batch_db):
    db = network_batch_db
    await _seed(db, [_org()])
    async with AsyncSession(db[2]) as session, session.begin():
        for resources in (
            [_org(), {"resourceType": "Organization"}],
            [_org(), {**_org(), "name": "Conflicting payload"}],
        ):
            with pytest.raises(ValueError, match="batch_invalid$"):
                await _apply(session, resources)
    assert await _counts(db) == (0, 0, 0)


async def test_native_unavailable_and_caller_transaction_required(network_batch_db, monkeypatch):
    db = network_batch_db
    async with AsyncSession(db[2]) as session:
        with pytest.raises(ValueError, match="transaction_required$"):
            await _apply(session, [_org()])
        async with session.begin():
            monkeypatch.setattr(batch, "_fast_module", lambda: None)
            with pytest.raises(RegistryAddressUnavailable, match="native_unavailable$"):
                await _apply(session, [_org()])
    assert await _counts(db) == (0, 0, 0)


async def test_copy_failure_preserves_caller_transaction(network_batch_db, monkeypatch):
    db = network_batch_db
    await _seed(db, [_org()])

    async def fail_copy(*args, **kwargs):
        raise asyncpg.DataError("synthetic COPY failure")

    monkeypatch.setattr(asyncpg.Connection, "copy_to_table", fail_copy)
    async with AsyncSession(db[2]) as session, session.begin():
        with pytest.raises(RegistryAddressUnavailable, match="store_unavailable$"):
            await _apply(session, [_org()])
        assert await session.scalar(text("SELECT 1")) == 1
    assert await _counts(db) == (0, 0, 0)


async def test_cancel_drains_real_copy_and_rolls_back_savepoint(network_batch_db, monkeypatch):
    db = network_batch_db
    await _seed(db, [_org()])
    copied, release = asyncio.Event(), asyncio.Event()
    original = asyncpg.Connection.copy_to_table

    async def pause_copy(connection, *args, **kwargs):
        result = await original(connection, *args, **kwargs)
        copied.set()
        await release.wait()
        return result

    monkeypatch.setattr(asyncpg.Connection, "copy_to_table", pause_copy)
    async with AsyncSession(db[2]) as session, session.begin():
        task = asyncio.create_task(_apply(session, [_org()]))
        try:
            await asyncio.wait_for(copied.wait(), 5)
            task.cancel()
            await asyncio.sleep(0)
            assert not task.done()
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(task, 5)
            assert await session.scalar(text("SELECT 1")) == 1
        finally:
            release.set()
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    assert await _counts(db) == (0, 0, 0)


async def test_expansion_limit_rejects_before_identity_allocation(network_batch_db):
    db = network_batch_db
    organizations = [_org(f"network-{index}") for index in range(6)]
    await _seed(db, organizations)
    plans = [_plan(f"plan-{index}", [f"Organization/{org['id']}" for org in organizations]) for index in range(834)]
    async with AsyncSession(db[2]) as session, session.begin():
        with pytest.raises(ValueError, match="expansion_limit$"):
            await _apply(session, plans)
    assert await _counts(db) == (0, 0, 0)


async def _wait_for_advisory(connection, pid, task):
    for _ in range(100):
        waiting = await connection.fetchval(
            "SELECT count(*) FROM pg_locks WHERE pid=$1 AND locktype='advisory' AND NOT granted",
            pid,
        )
        if waiting:
            assert waiting == 1
            assert not task.done()
            return
        await asyncio.sleep(0.01)
    pytest.fail("source fence did not block concurrent batch")


async def test_compatibility_writer_and_batch_share_source_fence(network_batch_db):
    db = network_batch_db
    await _seed(db, [_org()])
    async with AsyncSession(db[2]) as session, session.begin():
        await _apply(session, [_org()])
    async with AsyncSession(db[2]) as old_session, AsyncSession(db[2]) as new_session:
        async with new_session.begin():
            pid = await new_session.scalar(text("SELECT pg_backend_pid()"))
            async with old_session.begin():
                stable_id = await record_insurance_network_plan(
                    old_session,
                    source_id="source-one",
                    release_id="release-one",
                    network_resource_id="network-one",
                    plan=_plan(),
                )
                task = asyncio.create_task(_apply(new_session, [_plan()]))
                try:
                    await _wait_for_advisory(db[0], pid, task)
                except BaseException:
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
                    raise
            receipt = await asyncio.wait_for(task, 5)
            assert receipt["new_identity_count"] == receipt["new_plan_evidence_count"] == 0
    assert await _counts(db) == (1, 1, 1)
    assert (
        await db[0].fetchval(f'SELECT network_id FROM "{db[1]}".provider_directory_insurance_network_source_binding')
        == stable_id
    )


async def test_fresh_compatibility_organization_allocation_is_reused(network_batch_db):
    db = network_batch_db
    organization_dict = {**_org(), "name": "Café example"}
    await _seed(db, [organization_dict])
    async with AsyncSession(db[2]) as session, session.begin():
        stable_id = await record_insurance_network_organization(
            session,
            source_id="source-one",
            release_id="release-one",
            organization=organization_dict,
        )
        receipt = await _apply(session, [organization_dict])
        assert receipt["organization_count"] == 1
        assert receipt["new_identity_count"] == 0
    assert await _counts(db) == (1, 1, 0)
    assert (
        await db[0].fetchval(f'SELECT network_id FROM "{db[1]}".provider_directory_insurance_network_source_binding')
        == stable_id
    )


async def test_constraint_failure_rolls_back_already_inserted_identities(network_batch_db, monkeypatch):
    db = network_batch_db
    await _seed(db, [_org()])
    monkeypatch.setattr(
        batch,
        "_EVIDENCE_SQL",
        batch._EVIDENCE_SQL.replace(
            "observation_json->'network_refs'",
            "NULL::jsonb",
        ),
    )
    async with AsyncSession(db[2]) as session, session.begin():
        await session.execute(text(f'UPDATE "{db[1]}".registry_revision_control SET draft_revision=7 WHERE id=1'))
        with pytest.raises(RegistryAddressUnavailable, match="store_unavailable$"):
            await _apply(session, [_plan()])
        assert await session.scalar(text("SELECT 1")) == 1
    assert await _counts(db) == (0, 0, 0)
    assert await db[0].fetchval(f'SELECT draft_revision FROM "{db[1]}".registry_revision_control') == 7


@pytest.mark.parametrize(
    "resources", [[], [_org()] * 1001, (_org(),), [{"value": float("nan")}], [{"value": "\ud800"}]]
)
async def test_envelope_rejects_unbounded_or_non_json_input(resources):
    with pytest.raises(ValueError, match="batch_invalid$"):
        batch._envelope("source-one", "release-one", "Organization", resources)


async def test_envelope_preserves_unicode_and_exact_size_boundary(monkeypatch):
    resources = [{**_org(), "name": "Café example"}]
    encoded = batch._envelope("source-one", "release-one", "Organization", resources)
    assert b"Caf\xc3\xa9" in encoded
    monkeypatch.setattr(batch, "_MAX_INPUT_BYTES", len(encoded))
    assert batch._envelope("source-one", "release-one", "Organization", resources) == encoded
    monkeypatch.setattr(batch, "_MAX_INPUT_BYTES", len(encoded) - 1)
    with pytest.raises(ValueError, match="batch_invalid$"):
        batch._envelope("source-one", "release-one", "Organization", resources)


@pytest.mark.parametrize(
    "encoded",
    [
        None,
        [],
        (b"", 1, 1, 0),
        (batch._COPY_HEADER + b"\xff\xff", True, 1, 0),
        (batch._COPY_HEADER + b"\xff\xff", 1, True, 0),
        (batch._COPY_HEADER + b"\xff\xff", 1, 1, False),
        (batch._COPY_HEADER + b"\xff\xff", 2, 1, 0),
        (batch._COPY_HEADER + b"\xff\xff", 1, 1, 1),
        (batch._COPY_HEADER + b"\0\0", 1, 1, 0),
    ],
)
async def test_malformed_native_aggregate_fails_closed(monkeypatch, encoded):
    native = SimpleNamespace(encode_fhir_network_identity_batch=lambda _: encoded)
    monkeypatch.setattr(batch, "_fast_module", lambda: native)
    with pytest.raises(RegistryAddressUnavailable, match="native_unavailable$"):
        batch._encode(b"{}")


async def test_native_copy_size_bound_is_independent(monkeypatch):
    encoded = (batch._COPY_HEADER + b"\xff\xff", 1, 1, 0)
    native = SimpleNamespace(encode_fhir_network_identity_batch=lambda _: encoded)
    monkeypatch.setattr(batch, "_fast_module", lambda: native)
    monkeypatch.setattr(batch, "_MAX_COPY_BYTES", len(encoded[0]) - 1)
    with pytest.raises(RegistryAddressUnavailable, match="native_unavailable$"):
        batch._encode(b"{}")


@pytest.mark.parametrize(
    "resource",
    [
        {"resourceType": "Organization", "id": True},
        {"resourceType": "Organization", "id": 1.5},
        {"resourceType": "InsurancePlan", "id": "wrong-type"},
    ],
)
async def test_native_last_row_validation_precedes_copy(network_batch_db, monkeypatch, resource):
    async def unexpected_copy(*args, **kwargs):
        pytest.fail("invalid native batch reached COPY")

    monkeypatch.setattr(asyncpg.Connection, "copy_to_table", unexpected_copy)
    async with AsyncSession(network_batch_db[2]) as session, session.begin():
        with pytest.raises(ValueError, match="batch_invalid$"):
            await _apply(session, [_org(), resource])
    assert await _counts(network_batch_db) == (0, 0, 0)
