# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real native extraction, one binary COPY and savepoint-atomic FHIR evidence."""

import asyncio
import hashlib
import json
from contextlib import asynccontextmanager
from dataclasses import dataclass
from types import SimpleNamespace
from uuid import uuid4

import asyncpg
import pytest
from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import AsyncSession

from process import provider_directory_insurance_network_batch as batch
from process.provider_directory_generic_candidate import GenericCandidateError
from process.provider_directory_generic_source_auxiliary import identity_candidate_scope
from process.provider_directory_generic_source_policy import bound_publisher
from process.provider_directory_insurance_network_identity import (
    record_insurance_network_organization,
    record_insurance_network_plan,
)
from process.registry_record_store import RegistryAddressUnavailable
from tests.provider_directory_resource_identity_postgres_support import (
    prepare_resource_candidate,
)
from tests.provider_directory_source_postgres_support import source_component_database
from tests.test_provider_directory_cms_resource_batch_postgres import (
    cms_resource_template as cms_resource_template,
)

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


@dataclass
class NetworkBatchDatabase:
    """Observe private stages independently of the genuine Publisher transaction."""

    connection: object
    schema: str
    engine: object
    staged: object

    def relation(self, root):
        receipt = self.staged.binding["auxiliary"]["tables"].get(root)
        name = root if receipt is None else receipt["name"]
        return f'"{self.schema}"."{name}"'

    @asynccontextmanager
    async def session(self):
        async with self.staged.transaction() as session:
            async with identity_candidate_scope(bound_publisher(), self.staged.binding):
                yield session


@pytest.fixture
async def network_batch_db(monkeypatch, cms_resource_template):
    async with source_component_database(monkeypatch) as component:
        async with component.admin_engine.connect() as observer:
            driver = (await observer.get_raw_connection()).driver_connection
            await observer.rollback()
            global_identity_sql = (
                f'SELECT network_id,created_at FROM "{component.policy.schema}".'
                "provider_directory_insurance_network_identity ORDER BY network_id"
            )
            accepted_before = await driver.fetch(global_identity_sql)
            staged = await prepare_resource_candidate(component, [])
            try:
                yield NetworkBatchDatabase(driver, component.policy.schema, component.database.engine, staged)
            finally:
                # The private map allocates UUIDs; global identity minting belongs to publication.
                assert await driver.fetch(global_identity_sql) == accepted_before


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


async def _seed(
    db,
    resources,
    *,
    source_id="cms-npd",
    release="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
):
    connection = db.connection
    document = _organization_evidence_document(resources)
    await connection.execute(
        f'INSERT INTO "{db.schema}".provider_directory_organization_identity '
        "SELECT organization_id,now() FROM jsonb_to_recordset($1::jsonb) r(organization_id uuid)",
        document,
    )
    await connection.execute(
        f"INSERT INTO {db.relation('provider_directory_entity_source_binding')} "
        "(source_id,resource_type,resource_id,organization_id,created_at) "
        "SELECT $2,'Organization',resource_id,organization_id,now() FROM jsonb_to_recordset($1::jsonb) "
        "r(resource_id text,organization_id uuid)",
        document,
        source_id,
    )
    if release is not None:
        await connection.execute(
            f"INSERT INTO {db.relation('provider_directory_entity_release_evidence')} "
            "SELECT $2,'Organization',resource_id,$3,payload_sha256,payload_json,now() "
            "FROM jsonb_to_recordset($1::jsonb) r(resource_id text,payload_sha256 text,payload_json jsonb)",
            document,
            source_id,
            release,
        )


async def _apply(
    session,
    resources,
    *,
    kind=None,
    source="cms-npd",
    release="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
):
    return await batch.record_insurance_network_batch(
        session,
        source_id=source,
        release_id=release,
        resource_type=kind or resources[0]["resourceType"],
        resources=resources,
    )


async def _counts(db):
    """Count private allocated UUIDs and physical map/evidence rows; globals are checked separately."""
    connection = db.connection
    return tuple(
        await connection.fetchrow(
            f"SELECT (SELECT count(DISTINCT network_id) FROM {db.relation('provider_directory_insurance_network_source_binding')}),"
            f"(SELECT count(*) FROM {db.relation('provider_directory_insurance_network_source_binding')}),"
            f"(SELECT count(*) FROM {db.relation('provider_directory_insurance_network_plan_evidence')})"
        )
    )


async def test_organization_plan_replay_and_raw_evidence(network_batch_db):
    db = network_batch_db
    connection, schema = db.connection, db.schema
    org = _org()
    await _seed(db, [org])
    plan = _plan(
        refs=[
            "Organization/network-one",
            "Organization/unknown",
            "https://directory.example.test/Organization/network-one",
        ]
    )
    async with db.session() as session:
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
    evidence = await connection.fetchrow(
        f"SELECT network_refs::text AS network_refs,plan_payload_json::text AS plan_payload_json,"
        f"owned_by_ref,administered_by_ref FROM {db.relation('provider_directory_insurance_network_plan_evidence')}"
    )
    assert json.loads(evidence["network_refs"]) == [
        network_reference["reference"] for network_reference in plan["network"]
    ]
    assert json.loads(evidence["plan_payload_json"]) == plan
    assert evidence["owned_by_ref"] == "Organization/payer-one"
    assert evidence["administered_by_ref"] == "Organization/administrator-one"
    assert (
        await connection.fetchval(
            f'SELECT count(*) FROM "{schema}".provider_directory_insurance_network_source_binding'
        )
        == 0
    )


async def test_plan_allocates_only_retained_local_targets(network_batch_db):
    db = network_batch_db
    await _seed(db, [_org(), _org("network-two", role=False)])
    async with db.session() as session:
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
    async with db.session() as session:
        with pytest.raises(ValueError, match="_ref_missing$"):
            await _apply(session, [_plan("good"), _plan("padded", refs)])
        assert await session.scalar(text("SELECT 1")) == 1
    assert await _counts(db) == (0, 0, 0)


async def test_cross_batch_conflict_even_without_current_refs(network_batch_db):
    db = network_batch_db
    await _seed(db, [_org(), _org("network-two")])
    async with db.session() as session:
        await _apply(session, [_plan()])
        with pytest.raises(ValueError, match="release_plan_conflict$"):
            await _apply(
                session,
                [
                    _plan("new", ["Organization/network-two"]),
                    _plan(refs=["https://directory.example.test/foreign"]),
                ],
            )
    assert await _counts(db) == (1, 1, 1)


@pytest.mark.parametrize("failure", ["entity", "release", "payload"])
async def test_explicit_role_requires_exact_entity_and_release(network_batch_db, failure):
    db = network_batch_db
    organization_dict = _org()
    if failure != "entity":
        await _seed(
            db,
            [organization_dict],
            release=None
            if failure == "release"
            else "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        )
    if failure == "payload":
        organization_dict = {**organization_dict, "name": "Changed synthetic payload"}
    async with db.session() as session:
        with pytest.raises(ValueError, match="_(organization_binding|release_evidence)_missing$"):
            await _apply(session, [organization_dict])
    assert await _counts(db) == (0, 0, 0)


async def test_missing_release_skips_plan_without_allocation(network_batch_db):
    db = network_batch_db
    await _seed(db, [_org()], release=None)
    async with db.session() as session:
        result = await _apply(session, [_plan()])
        assert result["plan_link_count"] == result["new_identity_count"] == 0
    assert await _counts(db) == (0, 0, 0)


@pytest.mark.parametrize("coordinates", [{"source": "other-source"}, {"release": "b" * 64}])
async def test_candidate_source_and_release_coordinates_cannot_be_rebound(network_batch_db, coordinates):
    db = network_batch_db
    await _seed(db, [_org()])
    async with db.session() as session:
        with pytest.raises(ValueError, match="scope_changed$"):
            await _apply(session, [_plan()], **coordinates)
        assert await session.scalar(text("SELECT 1")) == 1
    assert await _counts(db) == (0, 0, 0)


async def test_fixed_sql_count_and_one_copy_for_one_and_thousand(network_batch_db, monkeypatch):
    db = network_batch_db
    resources = [_org(f"network-{index}") for index in range(1000)]
    await _seed(db, resources)
    statements, copies = [], []
    event.listen(
        db.engine.sync_engine,
        "before_cursor_execute",
        lambda *args: statements.append(args[2]),
    )
    original = asyncpg.Connection.copy_to_table

    async def record_copy(connection, *args, **kwargs):
        copies.append(kwargs["columns"])
        return await original(connection, *args, **kwargs)

    monkeypatch.setattr(asyncpg.Connection, "copy_to_table", record_copy)
    async with db.session() as session:
        statements.clear()
        first = await _apply(session, resources[:1])
        first_count = len(statements)
        assert first_count == 11
        statements.clear()
        second = await _apply(session, resources)
        assert len(statements) == first_count
        assert first["new_identity_count"] == 1
        assert second["new_identity_count"] == 999
    assert copies == [batch.COPY_COLUMNS, batch.COPY_COLUMNS]
    assert await _counts(db) == (1000, 1000, 0)


async def test_last_invalid_row_and_conflicting_duplicate_have_no_writes(
    network_batch_db,
):
    db = network_batch_db
    await _seed(db, [_org()])
    async with db.session() as session:
        for resources in (
            [_org(), {"resourceType": "Organization"}],
            [_org(), {**_org(), "name": "Conflicting payload"}],
        ):
            with pytest.raises(ValueError, match="batch_invalid$"):
                await _apply(session, resources)
    assert await _counts(db) == (0, 0, 0)


async def test_native_unavailable_and_caller_transaction_required(network_batch_db, monkeypatch):
    db = network_batch_db
    async with AsyncSession(db.engine) as session:
        with pytest.raises(ValueError, match="transaction_required$"):
            await _apply(session, [_org()])
        async with session.begin():
            with pytest.raises(GenericCandidateError, match="physical_session_required$"):
                await _apply(session, [_org()])
    async with db.session() as session:
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
    async with db.session() as session:
        with pytest.raises(RegistryAddressUnavailable, match="store_unavailable$"):
            await _apply(session, [_org()])
        assert await session.scalar(text("SELECT 1")) == 1
    assert await _counts(db) == (0, 0, 0)


async def test_cancel_drains_real_copy_and_rolls_back_original_transaction(network_batch_db, monkeypatch):
    db = network_batch_db
    await _seed(db, [_org()])
    copied, release = asyncio.Event(), asyncio.Event()
    original = asyncpg.Connection.copy_to_table
    drivers = []

    async def pause_copy(connection, *args, **kwargs):
        result = await original(connection, *args, **kwargs)
        drivers.append(connection)
        copied.set()
        await release.wait()
        return result

    async def apply_in_original_task():
        async with db.session() as session:
            await _apply(session, [_org()])

    monkeypatch.setattr(asyncpg.Connection, "copy_to_table", pause_copy)
    task = asyncio.create_task(apply_in_original_task())
    try:
        await asyncio.wait_for(copied.wait(), 5)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 5)
        assert len(drivers) == 1 and drivers[0].is_closed()
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
    async with db.session() as session:
        with pytest.raises(ValueError, match="expansion_limit$"):
            await _apply(session, plans)
    assert await _counts(db) == (0, 0, 0)


async def _wait_for_candidate_lock(connection, pid, task):
    for _ in range(100):
        if await connection.fetchval(
            "SELECT EXISTS(SELECT FROM pg_locks WHERE pid=$1 AND locktype='transactionid' AND NOT granted)",
            pid,
        ):
            assert not task.done()
            return
        await asyncio.sleep(0.01)
    pytest.fail("candidate header did not serialize concurrent batch")


async def test_compatibility_writer_and_batch_share_candidate_header_fence(
    network_batch_db,
):
    db = network_batch_db
    await _seed(db, [_org()])
    allocated, release, started = asyncio.Event(), asyncio.Event(), asyncio.Event()
    observations_by_field = {}

    async def compatibility_write():
        async with db.session() as session:
            observations_by_field["stable_id"] = await record_insurance_network_plan(
                session,
                source_id="cms-npd",
                release_id="a" * 64,
                network_resource_id="network-one",
                plan=_plan(),
            )
            allocated.set()
            await release.wait()

    async def batch_write():
        async with db.session() as session:
            observations_by_field["pid"] = await session.scalar(text("SELECT pg_backend_pid()"))
            started.set()
            return await _apply(session, [_plan()])

    first = asyncio.create_task(compatibility_write())
    second = None
    try:
        await asyncio.wait_for(allocated.wait(), 5)
        second = asyncio.create_task(batch_write())
        await asyncio.wait_for(started.wait(), 5)
        await _wait_for_candidate_lock(db.connection, observations_by_field["pid"], second)
        release.set()
        await asyncio.wait_for(first, 5)
        receipt = await asyncio.wait_for(second, 5)
        assert receipt["new_identity_count"] == receipt["new_plan_evidence_count"] == 0
    finally:
        release.set()
        for task in (first, second):
            if task is not None and not task.done():
                task.cancel()
        await asyncio.gather(
            *(task for task in (first, second) if task is not None),
            return_exceptions=True,
        )
    assert await _counts(db) == (1, 1, 1)
    assert (
        await db.connection.fetchval(
            f"SELECT network_id FROM {db.relation('provider_directory_insurance_network_source_binding')}"
        )
        == observations_by_field["stable_id"]
    )


async def test_fresh_compatibility_organization_allocation_is_reused(network_batch_db):
    db = network_batch_db
    organization_dict = {**_org(), "name": "Café example"}
    await _seed(db, [organization_dict])
    async with db.session() as session:
        stable_id = await record_insurance_network_organization(
            session,
            source_id="cms-npd",
            release_id="a" * 64,
            organization=organization_dict,
        )
        receipt = await _apply(session, [organization_dict])
        assert receipt["organization_count"] == 1
        assert receipt["new_identity_count"] == 0
    assert await _counts(db) == (1, 1, 0)
    assert (
        await db.connection.fetchval(
            f"SELECT network_id FROM {db.relation('provider_directory_insurance_network_source_binding')}"
        )
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
    async with db.session() as session:
        await session.execute(text("CREATE TEMP TABLE preserved_caller_work(value integer) ON COMMIT DROP"))
        await session.execute(text("INSERT INTO preserved_caller_work VALUES(7)"))
        with pytest.raises(RegistryAddressUnavailable, match="store_unavailable$"):
            await _apply(session, [_plan()])
        assert await session.scalar(text("SELECT value FROM preserved_caller_work")) == 7
    assert await _counts(db) == (0, 0, 0)


@pytest.mark.parametrize(
    "resources",
    [[], [_org()] * 1001, (_org(),), [{"value": float("nan")}], [{"value": "\ud800"}]],
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
    async with network_batch_db.session() as session:
        with pytest.raises(ValueError, match="batch_invalid$"):
            await _apply(session, [_org(), resource])
    assert await _counts(network_batch_db) == (0, 0, 0)
