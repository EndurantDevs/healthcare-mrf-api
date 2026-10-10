# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Reviewed retained sites compose through real drafts, approval and publication."""

import asyncio
import json
from dataclasses import replace
from types import SimpleNamespace
from uuid import UUID, uuid4, uuid5

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker

from process.network_address_projection import PinnedAddressSource
from process.network_approved_membership_source import pin_approved_membership_source
from process.network_approved_site_source import verify_approved_site_sources
from process.network_custom_address_source import NetworkCustomAddressSourceError, prepare_custom_address_source
from process.network_membership_pipeline import prepare_and_publish_network_candidate
from process.network_serving_read import NetworkServingReadUnavailable, resolve_network_serving_manifest
from process.registry_approval_store import RegistryApprovalConflict
from process.registry_candidate_composition import (
    RegistryCompositionAddressSources,
    compose_registry_membership_candidate,
)
from process.registry_record_store import RegistryRecordCommand, RegistryRecordConflict, apply_registry_record_command
from process.registry_retained_site_adoption import RetainedSiteAdoptionError
from tests.test_manual_location_identity_store_postgres import _create as _location
from tests.test_network_custom_address_source_postgres import custom_db
from tests.test_network_membership_draft_store_postgres import _command as _membership
from tests.test_network_membership_draft_store_postgres import _member
from tests.test_network_serving_schema_postgres import serving_schema
from tests.test_registry_approval_store_postgres import _actor, _approve, _command, _create
from tests.test_registry_candidate_composition_postgres import _remove_candidates, _roles
from tests.test_registry_retained_site_adoption_postgres import retained_db

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def site_db(retained_db):
    """Register cleanup before any composed candidate or artifact is created."""
    fixture = SimpleNamespace(**vars(retained_db), copy_targets=[], request_ids=[])
    try:
        yield fixture
    finally:
        for request_id in fixture.request_ids:
            await _remove_candidates(fixture, fixture.copy_targets, request_id)


async def _draft(fixture, command, actor):
    """Set repeatable read before the generic store's first management statement."""
    async with async_sessionmaker(fixture.engine)() as session, session.begin():
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
        return await apply_registry_record_command(
            session, command, actor, schema=fixture.control_schema, source_schema=fixture.source_schema
        )


def _binding_command(fixture, index=0):
    """Review one exact retained provider/site/address tuple, without manual heads."""
    return RegistryRecordCommand(
        "site_binding",
        uuid4(),
        "create",
        0,
        {"source_generation": fixture.source.generation_id, **fixture.rows[index]},
        "Reviewed exact source site",
        uuid4().hex,
    )


async def _approval(fixture, actor, *records):
    """Approve only explicitly selected current histories through the real store."""
    command = await _command(fixture.connection, fixture.control_schema, *records)
    return await _approve(fixture.connection, fixture.control_schema, command, actor)


async def _seed(fixture, *, approve=True):
    """Create a source binding and exact custom membership on a new network."""
    actor = _actor()
    network = await _draft(fixture, _create("network"), actor)
    binding_command = _binding_command(fixture)
    binding = await _draft(fixture, binding_command, actor)
    row = fixture.rows[0]
    membership_command = _membership(
        network["record_id"],
        [_member(network["record_id"], row["provider_id"], row["location_id"], system=row["provider_system"])],
    )
    membership = await _draft(fixture, membership_command, actor)
    approval = await _approval(fixture, actor, network, binding, membership) if approve else None
    return SimpleNamespace(
        actor=actor,
        network=network,
        binding=binding,
        binding_command=binding_command,
        membership=membership,
        membership_command=membership_command,
        revision=approval["approved_revision"] if approval else None,
    )


async def _composition_arguments(fixture, revision):
    """Register exact recipe resources before asking the composer to create them."""
    request_id = uuid4()
    fixture.request_ids.append(request_id)
    async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
        approved = await pin_approved_membership_source(
            fixture.connection, approved_revision=revision, control_schema=fixture.control_schema
        )
    fixture.composition_ids.append(uuid5(request_id, "address:" + approved.generation_id))
    return {
        "request_id": request_id,
        "approved_revision": revision,
        "expected_head": fixture.source.generation_id,
        "address_sources": RegistryCompositionAddressSources(
            PinnedAddressSource(fixture.source.schema_name, "entity_address_unified", fixture.source.manifest_sha256)
        ),
        "writer_roles": _roles(fixture),
        "source_manifest": fixture.source,
        "control_schema": fixture.control_schema,
    }


@pytest.mark.parametrize("retained_db", [(2, "npi"), (2, "provider_directory")], indirect=True)
async def test_retained_site_full_publication_preserves_only_approved_custom_office(site_db):
    fixture = site_db
    seed = await _seed(fixture)
    pending = replace(
        seed.binding_command,
        operation="correct",
        expected_revision=1,
        fields={"source_generation": fixture.source.generation_id, **fixture.rows[1]},
        idempotency_key=uuid4().hex,
    )
    corrected = await _draft(fixture, pending, seed.actor)
    assert corrected["record"]["location_id"] == fixture.rows[1]["location_id"]
    assert await _draft(fixture, seed.binding_command, seed.actor) == seed.binding
    arguments = await _composition_arguments(fixture, seed.revision)
    copy_target, addresses = await compose_registry_membership_candidate(fixture.connection, **arguments)
    fixture.copy_targets.append(copy_target)
    assert await compose_registry_membership_candidate(fixture.connection, **arguments) == (copy_target, addresses)
    await prepare_and_publish_network_candidate(
        fixture.connection, copy_target, addresses, **_roles(fixture), control_schema=fixture.control_schema
    )
    members = await fixture.connection.fetch(f'SELECT * FROM "{copy_target.schema_name}".network_membership')
    custom_members = [dict(member) for member in members if member["network_id"] == seed.network["record_id"]]
    assert len(members) == 3 and len(custom_members) == 1
    assert str(custom_members[0]["location_id"]) == fixture.rows[0]["location_id"]
    assert custom_members[0]["evidence_id"].startswith("approved-custom:")
    projected = await fixture.connection.fetch(
        f'SELECT location_key,canonical_network_ids FROM "{copy_target.schema_name}".entity_address_unified ORDER BY location_key'
    )
    assert len(projected) == 2
    assert [
        address_record["location_key"]
        for address_record in projected
        if seed.network["record_id"] in address_record["canonical_network_ids"]
    ] == [fixture.rows[0]["location_key"]]
    async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
        manifest = await resolve_network_serving_manifest(fixture.connection, control_schema=fixture.control_schema)
        retained = await resolve_network_serving_manifest(
            fixture.connection, generation_id=fixture.source.generation_id, control_schema=fixture.control_schema
        )
    assert retained == fixture.source and manifest.approved_custom_revision == seed.revision
    assert manifest.generation_id != fixture.source.generation_id
    for table_name in ("manual_provider_registry", "manual_location_registry"):
        assert await fixture.connection.fetchval(f'SELECT count(*) FROM "{fixture.control_schema}".{table_name}') == 0
    selected = await fixture.connection.fetchrow(
        f'''SELECT record_revision,record_json FROM "{fixture.control_schema}".registry_approved_record
          WHERE approved_revision=$1 AND record_kind='site_binding' ''',
        seed.revision,
    )
    assert (
        selected["record_revision"] == 1
        and json.loads(selected["record_json"])["location_id"] == fixture.rows[0]["location_id"]
    )


async def test_duplicate_active_binding_rejected_even_without_membership(site_db):
    fixture = site_db
    actor = _actor()
    bindings = [await _draft(fixture, _binding_command(fixture), actor) for _ in range(2)]
    with pytest.raises(RegistryRecordConflict, match="registry_approval_site_binding_conflict"):
        await _approval(fixture, actor, *bindings)
    assert (
        await fixture.connection.fetchval(
            f'SELECT approved_revision FROM "{fixture.control_schema}".registry_revision_control WHERE id=1'
        )
        == 0
    )


@pytest.mark.parametrize("overlap", ["binding", "manual_location"])
async def test_prospective_duplicate_or_manual_site_overlap_rejected(site_db, overlap):
    fixture = site_db
    seed = await _seed(fixture)
    command = (
        _binding_command(fixture) if overlap == "binding" else _location(record_id=UUID(fixture.rows[0]["location_id"]))
    )
    conflicting = await _draft(fixture, command, seed.actor)
    with pytest.raises(RegistryRecordConflict, match="registry_approval_site_binding_conflict"):
        await _approval(fixture, seed.actor, conflicting)
    assert (
        await fixture.connection.fetchval(
            f'SELECT approved_revision FROM "{fixture.control_schema}".registry_revision_control WHERE id=1'
        )
        == seed.revision
    )


async def test_archiving_a_used_binding_requires_explicit_membership_clear(site_db):
    fixture = site_db
    seed = await _seed(fixture)
    archived = await _draft(
        fixture,
        replace(seed.binding_command, operation="archive", expected_revision=1, fields={}, idempotency_key=uuid4().hex),
        seed.actor,
    )
    with pytest.raises(RegistryApprovalConflict):
        await _approval(fixture, seed.actor, archived)
    clear = await _draft(
        fixture,
        replace(
            seed.membership_command,
            operation="correct",
            expected_revision=1,
            fields={"memberships_json": []},
            idempotency_key=uuid4().hex,
        ),
        seed.actor,
    )
    approval = await _approval(fixture, seed.actor, archived, clear)
    async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
        approved_source = await pin_approved_membership_source(
            fixture.connection, approved_revision=approval["approved_revision"], control_schema=fixture.control_schema
        )
        assert approved_source.total_rows == 0
        assert (
            await verify_approved_site_sources(
                fixture.connection,
                approved_revision=approval["approved_revision"],
                control_schema=fixture.control_schema,
            )
            == ()
        )


async def test_restore_requires_unchanged_server_sealed_proof(site_db):
    fixture = site_db
    seed = await _seed(fixture)
    archived = await _draft(
        fixture,
        replace(seed.binding_command, operation="archive", expected_revision=1, fields={}, idempotency_key=uuid4().hex),
        seed.actor,
    )
    clear = await _draft(
        fixture,
        replace(
            seed.membership_command,
            operation="correct",
            expected_revision=1,
            fields={"memberships_json": []},
            idempotency_key=uuid4().hex,
        ),
        seed.actor,
    )
    await _approval(fixture, seed.actor, archived, clear)
    restore = replace(
        seed.binding_command, operation="restore", expected_revision=2, fields={}, idempotency_key=uuid4().hex
    )
    proof = archived["record"]["source_receipt_json"]
    table = f'"{fixture.control_schema}".registry_site_binding'
    await fixture.connection.execute(
        f"UPDATE {table} SET source_receipt_json=jsonb_set(source_receipt_json,'{{manifest_sha256}}','\"changed\"'::jsonb) WHERE binding_id=$1",
        seed.binding_command.record_id,
    )
    try:
        with pytest.raises(RegistryRecordConflict, match="registry_site_binding_source_conflict"):
            await _draft(fixture, restore, seed.actor)
    finally:
        await fixture.connection.execute(
            f"UPDATE {table} SET source_receipt_json=$1::jsonb WHERE binding_id=$2",
            json.dumps(proof),
            seed.binding_command.record_id,
        )
    restored = await _draft(fixture, restore, seed.actor)
    assert restored["record"]["source_receipt_json"] == proof
    assert restored["revision"] == 3 and restored["record"]["archived"] is False
    member = await _draft(
        fixture,
        replace(seed.membership_command, operation="correct", expected_revision=2, idempotency_key=uuid4().hex),
        seed.actor,
    )
    approval = await _approval(fixture, seed.actor, restored, member)
    async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
        receipts = await verify_approved_site_sources(
            fixture.connection, approved_revision=approval["approved_revision"], control_schema=fixture.control_schema
        )
    assert len(receipts) == 1 and len(receipts[0].records) == 1


@pytest.mark.parametrize("damage", [None, "source", "hash", "oid", "acl", "ineligible"])
async def test_preparation_is_caller_atomic_and_source_tampering_fails_closed(site_db, damage):
    fixture = site_db
    seed = await _seed(fixture)
    composition_id = uuid4()
    fixture.composition_ids.append(composition_id)
    artifact_schema = "network_composition_" + composition_id.hex
    transaction = fixture.connection.transaction(isolation="repeatable_read")
    await transaction.start()
    try:
        source_namespace = '"' + fixture.source.schema_name + '"'
        statement_by_damage = {
            "source": f'''UPDATE "{fixture.control_schema}".network_membership_candidate SET source_generations='{{"fhir":"changed"}}'::jsonb''',
            "hash": f"UPDATE {source_namespace}.entity_address_unified SET second_line='Suite changed'",
            "oid": f"ALTER TABLE {source_namespace}.provider_location_binding RENAME TO old_binding",
            "acl": f'GRANT UPDATE ON {source_namespace}.entity_address_unified TO "{fixture.roles["reader"]}"',
            "ineligible": f'UPDATE "{fixture.control_schema}".network_serving_manifest SET eligible=false',
        }
        if damage is not None:
            await fixture.connection.execute(statement_by_damage[damage])
        artifact_by_name = {
            "composition_id": str(composition_id),
            "approved_revision": seed.revision,
            "owner_role": fixture.roles["owner"],
            "runtime_roles": (fixture.roles["reader"],),
            "control_schema": fixture.control_schema,
        }
        base_source = PinnedAddressSource(
            fixture.source.schema_name, "entity_address_unified", fixture.source.manifest_sha256
        )
        if damage is None:
            receipt = await prepare_custom_address_source(fixture.connection, base_source, **artifact_by_name)
            assert receipt.custom_pair_count == 1
            binding = await fixture.connection.fetchrow(f'SELECT * FROM "{artifact_schema}".provider_location_binding')
            assert str(binding["location_id"]) == fixture.rows[0]["location_id"]
            assert (
                await fixture.connection.fetchval(f'SELECT count(*) FROM "{artifact_schema}".entity_address_unified')
                == 2
            )
        else:
            with pytest.raises(
                (NetworkCustomAddressSourceError, NetworkServingReadUnavailable, RetainedSiteAdoptionError)
            ):
                await prepare_custom_address_source(fixture.connection, base_source, **artifact_by_name)
    finally:
        await transaction.rollback()
    assert await fixture.connection.fetchval("SELECT to_regnamespace($1)", artifact_schema) is None
    assert (
        await fixture.connection.fetchval(
            f'SELECT generation_id FROM "{fixture.control_schema}".network_serving_control WHERE id=1'
        )
        == fixture.source.generation_id
    )
    async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
        assert (
            await resolve_network_serving_manifest(fixture.connection, control_schema=fixture.control_schema)
            == fixture.source
        )


async def _source_read_count(fixture, revision):
    """Measure only the approved-site reader inside a readonly repeatable snapshot."""
    queries = []
    fixture.connection.add_query_logger(queries.append)
    try:
        async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
            await asyncio.sleep(0)
            queries.clear()
            receipts = await verify_approved_site_sources(
                fixture.connection, approved_revision=revision, control_schema=fixture.control_schema
            )
            await asyncio.sleep(0)
            assert all(query.query.lstrip().split()[0] in ("SELECT", "WITH", "SHOW") for query in queries)
            return len(queries), sum(len(receipt.records) for receipt in receipts)
    finally:
        fixture.connection.remove_query_logger(queries.append)


async def test_same_generation_bulk_revalidation_uses_fixed_queries(site_db):
    fixture = site_db
    seed = await _seed(fixture)
    one = await _source_read_count(fixture, seed.revision)
    second = await _draft(fixture, _binding_command(fixture, 1), seed.actor)
    row = fixture.rows[1]
    memberships = seed.membership_command.fields["memberships_json"] + [
        _member(seed.network["record_id"], row["provider_id"], row["location_id"], system=row["provider_system"])
    ]
    corrected = await _draft(
        fixture,
        replace(
            seed.membership_command,
            operation="correct",
            expected_revision=1,
            fields={"memberships_json": memberships},
            idempotency_key=uuid4().hex,
        ),
        seed.actor,
    )
    approval = await _approval(fixture, seed.actor, second, corrected)
    two = await _source_read_count(fixture, approval["approved_revision"])
    assert one == (7, 1) and two == (7, 2)


async def test_approval_is_caller_atomic_and_only_seals_explicit_records(site_db):
    fixture = site_db
    seed = await _seed(fixture, approve=False)
    control_table = f'"{fixture.control_schema}".registry_revision_control'
    approved_table = f'"{fixture.control_schema}".registry_approved_record'
    original_heads = await fixture.connection.fetchrow(f"SELECT * FROM {control_table} WHERE id=1")
    command = await _command(fixture.connection, fixture.control_schema, seed.network, seed.binding, seed.membership)
    transaction = fixture.connection.transaction()
    await transaction.start()
    try:
        prepared = await _approve(fixture.connection, fixture.control_schema, command, seed.actor)
        assert await fixture.connection.fetchval(f"SELECT count(*) FROM {approved_table}") == 3
        assert prepared["selected_count"] == 3
    finally:
        await transaction.rollback()
    assert await fixture.connection.fetchrow(f"SELECT * FROM {control_table} WHERE id=1") == original_heads
    assert await fixture.connection.fetchval(f"SELECT count(*) FROM {approved_table}") == 0
    assert (
        await fixture.connection.fetchval(f'SELECT count(*) FROM "{fixture.control_schema}".registry_approval_history')
        == 0
    )
    committed = await _approve(fixture.connection, fixture.control_schema, command, seed.actor)
    assert committed == prepared and committed["replayed"] is False
    assert await fixture.connection.fetchval(f"SELECT count(*) FROM {approved_table}") == 3
    assert (
        await fixture.connection.fetchval(
            f'SELECT generation_id FROM "{fixture.control_schema}".network_serving_control WHERE id=1'
        )
        == fixture.source.generation_id
    )
