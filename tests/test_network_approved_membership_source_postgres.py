# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Actual approved maps, native bounded pages and lifecycle COPY receipts."""

import hashlib
import json
from dataclasses import replace
from uuid import UUID, uuid4

import pytest

from process import network_membership_copy as native_copy
from process.network_approved_membership_source import (
    ApprovedMembershipSource,
    ApprovedMembershipSourceError,
    copy_approved_membership_batch,
    pin_approved_membership_source,
    read_approved_membership_batch,
)
from process.network_membership_candidate_lifecycle import (
    MembershipCandidateError,
    create_network_candidate,
    seal_network_candidate,
)
from process.network_membership_copy import MembershipCopyTarget
from process.registry_record_store import RegistryRecordCommand
from tests.test_manual_location_identity_store_postgres import _create as location_command
from tests.test_manual_provider_identity_store_postgres import _apply, provider_db, serving_schema
from tests.test_registry_approval_store_postgres import _approve, _command
from tests.test_registry_membership_approval_postgres import _records

pytestmark = pytest.mark.asyncio


class CountingConnection:
    def __init__(self, connection):
        self.connection = connection
        self.statements = 0

    def __getattr__(self, name):
        return getattr(self.connection, name)

    async def fetchrow(self, query, *parameters):
        self.statements += 1
        return await self.connection.fetchrow(query, *parameters)


@pytest.fixture
async def approved_source(provider_db):
    connection, schema, _, _ = provider_db
    actor, *records = await _records(provider_db)
    approval = await _approve(connection, schema, await _command(connection, schema, *records), actor)
    async with connection.transaction(isolation="repeatable_read"):
        source = await pin_approved_membership_source(
            connection, approved_revision=approval["approved_revision"], control_schema=schema
        )
    yield connection, schema, source, actor, records, provider_db


async def _read(fixture, **options):
    connection, schema, source, *_ = fixture
    async with connection.transaction(isolation="repeatable_read"):
        return await read_approved_membership_batch(connection, source, control_schema=schema, **options)


async def _candidate(fixture, **changes):
    connection, schema, source, *_ = fixture
    candidate_id = uuid4()
    target = MembershipCopyTarget(
        str(uuid4()), str(uuid4()), str(uuid4()), str(candidate_id), "network_candidate_" + candidate_id.hex
    )
    async with connection.transaction():
        await create_network_candidate(
            connection,
            target,
            control_schema=schema,
            **{
                "source_generations": {"custom_membership": source.generation_id},
                "approved_custom_revision": source.approved_revision,
                "expected_head": 0,
                "expected_rows": source.total_rows,
                **changes,
            },
        )
    return target


async def test_exact_location_namespace_and_pending_drafts(approved_source):
    connection, schema, source_pin, actor, approved_records, fixture = approved_source
    batch = await _read(approved_source)
    membership_rows = json.loads(batch.input_bytes)
    assert len(membership_rows) == batch.row_count == batch.total_rows == 1
    assert membership_rows[0]["location_id"] == approved_records[2]["record_id"]
    assert membership_rows[0]["provider_id"] == approved_records[1]["record_id"]
    assert set(membership_rows[0]) == {"network_id", "provider_system", "provider_id", "location_id", "evidence_id"}
    original = approved_records[3]["record"]["memberships_json"][0]
    native_digest = await connection.fetchval(
        "SELECT encode(sha256(convert_to($1::jsonb::text,'UTF8')),'hex')", json.dumps(original)
    )
    assert membership_rows[0]["evidence_id"] == f"approved-custom:{source_pin.approved_revision}:{native_digest}"
    assert batch.input_sha256 == hashlib.sha256(batch.input_bytes).hexdigest()
    await _apply(fixture[2], schema, location_command(), actor)
    await _apply(
        fixture[2],
        schema,
        RegistryRecordCommand(
            "provider",
            UUID(approved_records[1]["record_id"]),
            "correct",
            1,
            {"display_name": "Pending", "provider_kind": "individual", "aliases": [], "npi": None},
            "Pending correction",
            uuid4().hex,
        ),
        actor,
    )
    assert (await _read(approved_source)).input_bytes == batch.input_bytes
    assert (await _read(approved_source, offset=1)).row_count == 0


async def test_empty_zero_source_and_isolation(serving_schema):
    connection, schema, _ = serving_schema
    with pytest.raises(ApprovedMembershipSourceError, match="caller-owned"):
        await pin_approved_membership_source(connection, approved_revision=0, control_schema=schema)
    async with connection.transaction():
        with pytest.raises(ApprovedMembershipSourceError, match="isolation"):
            await pin_approved_membership_source(connection, approved_revision=0, control_schema=schema)
    async with connection.transaction(isolation="serializable"):
        source = await pin_approved_membership_source(connection, approved_revision=0, control_schema=schema)
        batch = await read_approved_membership_batch(connection, source, control_schema=schema)
    assert source.total_rows == batch.row_count == 0 and batch.input_bytes == b"[]"


async def _bulk_map(fixture, count):
    """Seed large exact approved input using migrated columns and actual approved DTOs."""
    connection, schema, source, _, records, _ = fixture
    await connection.execute(
        f"""WITH locations AS (
      SELECT gen_random_uuid() AS location_id FROM generate_series(1,$2::integer)
    ), inserted AS (
      INSERT INTO "{schema}".registry_approved_record
      SELECT approved_revision,record_kind,locations.location_id::text,record_revision,custom_revision,
        jsonb_set(record_json,'{{location_id}}',to_jsonb(locations.location_id::text))
      FROM "{schema}".registry_approved_record CROSS JOIN locations
      WHERE approved_revision=$1 AND record_kind='location' RETURNING record_key
    ) UPDATE "{schema}".registry_approved_record SET record_json=jsonb_set(record_json,
      '{{memberships_json}}',(SELECT jsonb_agg(jsonb_build_object('network_id',$3::integer,
        'provider_system','manual','provider_id',$4::text,'location_id',record_key,
        'evidence_id','exact-site-'||record_key) ORDER BY record_key) FROM inserted))
    WHERE approved_revision=$1 AND record_kind='membership'""",
        source.approved_revision,
        count,
        records[0]["record_id"],
        records[1]["record_id"],
    )
    async with connection.transaction(isolation="repeatable_read"):
        return await pin_approved_membership_source(
            connection, approved_revision=source.approved_revision, control_schema=schema
        )


@pytest.mark.parametrize("count", [1, 5000])
async def test_constant_statements_for_native_pages(approved_source, count):
    connection, schema, *_ = approved_source
    source = await _bulk_map(approved_source, count)
    counted = CountingConnection(connection)
    async with connection.transaction(isolation="repeatable_read"):
        batch = await read_approved_membership_batch(counted, source, limit=count, control_schema=schema)
    assert counted.statements == 2 and batch.row_count == batch.total_rows == count
    assert len({row["location_id"] for row in json.loads(batch.input_bytes)}) == count


async def test_copy_receipt_replay_seal_and_rollback(approved_source, monkeypatch):
    connection, schema, source, *_ = approved_source
    target = await _candidate(approved_source)
    try:
        transaction = connection.transaction(isolation="repeatable_read")
        await transaction.start()
        receipt = await copy_approved_membership_batch(connection, source, target, control_schema=schema)
        assert receipt.row_count == 1
        await transaction.rollback()
        assert await connection.fetchval(f'SELECT count(*) FROM "{target.schema_name}".network_membership') == 0
        async with connection.transaction(isolation="repeatable_read"):
            receipt = await copy_approved_membership_batch(connection, source, target, control_schema=schema)
            await seal_network_candidate(connection, target, control_schema=schema)

        def unavailable(_):
            raise AssertionError("Replay reached encoder")

        monkeypatch.setattr(native_copy, "_encode", unavailable)
        async with connection.transaction(isolation="repeatable_read"):
            assert await copy_approved_membership_batch(connection, source, target, control_schema=schema) == receipt
        assert (
            await connection.fetchval(
                f'SELECT accepted_rows FROM "{schema}".network_membership_candidate WHERE candidate_id=$1',
                UUID(target.candidate_id),
            )
            == 1
        )
    finally:
        await connection.execute(f'DROP SCHEMA IF EXISTS "{target.schema_name}" CASCADE')


@pytest.mark.parametrize("damage", ["digest", "revision", "provider", "location", "network", "extra", "evidence"])
async def test_full_map_drift_and_missing_references(approved_source, damage):
    connection, schema, source, *_ = approved_source
    if damage == "digest":
        source = replace(source, generation_id="0" * 64)
    elif damage == "revision":
        await connection.execute(
            f'UPDATE "{schema}".registry_revision_control SET approved_revision=approved_revision+1,draft_revision=draft_revision+1'
        )
    elif damage in {"provider", "location", "network"}:
        await connection.execute(
            f"UPDATE \"{schema}\".registry_approved_record SET record_json=jsonb_set(record_json,'{{archived}}','true') WHERE record_kind=$1",
            damage,
        )
    else:
        member = {"unexpected": True} if damage == "extra" else {"evidence_id": None}
        await connection.execute(
            f"UPDATE \"{schema}\".registry_approved_record SET record_json=jsonb_set(record_json,'{{memberships_json,0}}',record_json#>'{{memberships_json,0}}'||$1::jsonb) WHERE record_kind='membership'",
            json.dumps(member),
        )
    async with connection.transaction(isolation="repeatable_read"):
        with pytest.raises(ApprovedMembershipSourceError):
            await read_approved_membership_batch(connection, source, control_schema=schema)


@pytest.mark.parametrize("damage", ["revision", "generation", "producer", "closed"])
async def test_candidate_pin_and_full_scope_checks(approved_source, damage):
    connection, schema, source, *_ = approved_source
    changes_by_name = {
        "revision": {"approved_custom_revision": 0},
        "generation": {"source_generations": {"custom_membership": "0" * 64}},
    }
    target = await _candidate(approved_source, **changes_by_name.get(damage, {}))
    try:
        if damage == "closed":
            await connection.execute(
                f"UPDATE \"{schema}\".network_membership_candidate SET state='sealed' WHERE candidate_id=$1",
                UUID(target.candidate_id),
            )
        requested = replace(target, producer_id=str(uuid4())) if damage == "producer" else target
        async with connection.transaction(isolation="repeatable_read"):
            with pytest.raises((ApprovedMembershipSourceError, MembershipCandidateError)):
                await copy_approved_membership_batch(connection, source, requested, control_schema=schema)
        assert await connection.fetchval(f'SELECT count(*) FROM "{target.schema_name}".network_membership') == 0
    finally:
        await connection.execute(f'DROP SCHEMA IF EXISTS "{target.schema_name}" CASCADE')


@pytest.mark.parametrize("options", [{"offset": -1}, {"offset": True}, {"limit": 0}, {"limit": 5001}, {"limit": True}])
async def test_page_bounds(approved_source, options):
    with pytest.raises(ApprovedMembershipSourceError):
        await _read(approved_source, **options)


async def test_invalid_source_and_missing_prerequisites(serving_schema):
    connection, schema, _ = serving_schema
    for values in [(-1, "a" * 64), (True, "a" * 64), (0, "A" * 64)]:
        with pytest.raises(ApprovedMembershipSourceError):
            ApprovedMembershipSource(*values)
    async with connection.transaction(isolation="repeatable_read"):
        with pytest.raises(ApprovedMembershipSourceError, match="prerequisites"):
            await pin_approved_membership_source(connection, approved_revision=0, control_schema="missing_example")


async def test_conflicting_page_retry_preserves_candidate(approved_source):
    connection, schema, *_ = approved_source
    source_pin = await _bulk_map(approved_source, 2)
    fixture = (*approved_source[:2], source_pin, *approved_source[3:])
    target = await _candidate(fixture)
    try:
        async with connection.transaction(isolation="repeatable_read"):
            first = await copy_approved_membership_batch(connection, source_pin, target, limit=1, control_schema=schema)
            with pytest.raises(MembershipCandidateError, match="different input"):
                await copy_approved_membership_batch(connection, source_pin, target, limit=2, control_schema=schema)
            assert (
                await copy_approved_membership_batch(connection, source_pin, target, limit=1, control_schema=schema)
                == first
            )
            second = await copy_approved_membership_batch(
                connection, source_pin, target, offset=1, limit=1, control_schema=schema
            )
            assert second.row_count == 1
            await seal_network_candidate(connection, target, control_schema=schema)
        assert await connection.fetchval(f'SELECT count(*) FROM "{target.schema_name}".network_membership') == 2
        assert (
            await connection.fetchval(
                f'SELECT count(*) FROM "{schema}".network_membership_batch WHERE candidate_id=$1',
                UUID(target.candidate_id),
            )
            == 2
        )
    finally:
        await connection.execute(f'DROP SCHEMA IF EXISTS "{target.schema_name}" CASCADE')


async def test_npi_coordinates_need_no_manual_provider(approved_source):
    connection, schema, source, _, records, _ = approved_source
    await connection.execute(f"""UPDATE "{schema}".registry_approved_record
      SET record_json=jsonb_set(record_json,'{{memberships_json,0}}',
        record_json#>'{{memberships_json,0}}'||'{{"provider_system":"npi","provider_id":"1000000004"}}'::jsonb)
      WHERE record_kind='membership'""")
    await connection.execute(f"DELETE FROM \"{schema}\".registry_approved_record WHERE record_kind='provider'")
    async with connection.transaction(isolation="repeatable_read"):
        source_pin = await pin_approved_membership_source(
            connection, approved_revision=source.approved_revision, control_schema=schema
        )
        batch = await read_approved_membership_batch(connection, source_pin, control_schema=schema)
    membership_rows = json.loads(batch.input_bytes)
    assert membership_rows[0]["provider_system"] == "npi"
    assert membership_rows[0]["provider_id"] == "1000000004"
    assert membership_rows[0]["location_id"] == records[2]["record_id"]
    assert batch.row_count == 1
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".npi') == 0


async def test_full_document_bound_and_nonmembership_drift(approved_source):
    connection, schema, source, *_ = approved_source
    async with connection.transaction(isolation="repeatable_read"):
        await connection.execute(
            f'UPDATE "{schema}".registry_approved_record SET record_json=record_json||\'{{"display_name":"Changed"}}\'::jsonb WHERE record_kind=\'provider\''
        )
        with pytest.raises(ApprovedMembershipSourceError, match="identity changed"):
            await read_approved_membership_batch(connection, source, control_schema=schema)
        changed = await pin_approved_membership_source(
            connection, approved_revision=source.approved_revision, control_schema=schema
        )
        assert changed.generation_id != source.generation_id
    await connection.execute(
        f"UPDATE \"{schema}\".registry_approved_record SET record_json=record_json||jsonb_build_object('bounded_example',repeat('x',8388608)) WHERE record_kind='location'"
    )
    async with connection.transaction(isolation="repeatable_read"):
        with pytest.raises(ApprovedMembershipSourceError, match="8 MiB"):
            await pin_approved_membership_source(
                connection, approved_revision=source.approved_revision, control_schema=schema
            )


async def test_invalid_npi_fails_native_page_validation(approved_source):
    connection, schema, source, *_ = approved_source
    await connection.execute(f"""UPDATE "{schema}".registry_approved_record
      SET record_json=jsonb_set(record_json,'{{memberships_json,0}}',
        record_json#>'{{memberships_json,0}}'||'{{"provider_system":"npi","provider_id":"1000000000"}}'::jsonb)
      WHERE record_kind='membership'""")
    async with connection.transaction(isolation="repeatable_read"):
        source_pin = await pin_approved_membership_source(
            connection, approved_revision=source.approved_revision, control_schema=schema
        )
        with pytest.raises(ApprovedMembershipSourceError, match="native validation"):
            await read_approved_membership_batch(connection, source_pin, control_schema=schema)


async def test_new_approval_rejects_older_source_pin(approved_source):
    connection, schema, source, actor, records, fixture = approved_source
    command = RegistryRecordCommand(
        "network",
        records[0]["record_id"],
        "correct",
        1,
        {"display_name": "Approved correction", "aliases": []},
        "Explicit correction",
        uuid4().hex,
    )
    corrected = await _apply(fixture[2], schema, command, actor)
    approval = await _approve(connection, schema, await _command(connection, schema, corrected), actor)
    assert approval["approved_revision"] > source.approved_revision
    async with connection.transaction(isolation="repeatable_read"):
        with pytest.raises(ApprovedMembershipSourceError, match="revision"):
            await read_approved_membership_batch(connection, source, control_schema=schema)
        current = await pin_approved_membership_source(
            connection, approved_revision=approval["approved_revision"], control_schema=schema
        )
    assert current.total_rows == source.total_rows == 1
    assert current.generation_id != source.generation_id
