# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Connected compact qualification across rotation, contention and dense memberships."""

import asyncio
import hashlib
import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from uuid import UUID, uuid4

import asyncpg
import pytest

from api.network_address_scope import network_address_read_scope
from api.network_provider_read import NetworkProviderNotFound, read_network_provider_detail, read_network_provider_page
from process import entity_address_snapshot_adoption as adoption
from process import network_address_artifact as artifact
from process import network_compact_preparation as compact_preparation
from process.network_address_projection import PinnedAddressSource
from process.network_membership_candidate_lifecycle import (
    admit_network_membership_batch,
    create_network_candidate,
    seal_network_candidate,
)
from process.network_membership_copy import MembershipCopyTarget
from process.network_membership_publication import NetworkPublicationError
from process.network_serving_read import resolve_network_serving_manifest
from tests import test_network_compact_publication_postgres as publication
from tests.test_network_compact_publication_postgres import compact_publication_db as compact_publication_db


async def _publish_ready(fixture, target):
    await publication._prepare(fixture, target)
    await publication._seal(fixture, target)
    return await publication._publish(fixture, target)


async def _state(fixture, target):
    return await fixture.connection.fetchval(
        f'SELECT state FROM "{fixture.control_schema}".network_membership_candidate WHERE candidate_id=$1',
        UUID(target.candidate_id),
    )


async def _rotate_source(fixture, monkeypatch):
    address = fixture.address
    schema = address.schema
    await address.admin.execute(
        f'CREATE TABLE "{schema}".entity_address_unified_next (LIKE "{schema}".entity_address_unified INCLUDING ALL)'
    )
    await address.admin.execute(
        f'ALTER TABLE "{schema}".entity_address_unified_next OWNER TO "{address.roles["owner"]}"'
    )
    await address.admin.execute(
        f'GRANT SELECT ON "{schema}".entity_address_unified_next TO "{address.roles["reader"]}"'
    )
    await address.admin.execute(
        f'INSERT INTO "{schema}".entity_address_unified_next SELECT * FROM "{schema}".entity_address_unified'
    )
    await address.admin.execute(
        f"UPDATE \"{schema}\".entity_address_unified_next SET location_key=location_key||'-next',city_name='New City'"
    )
    monkeypatch.setattr(adoption.entity_address_unified, "db", publication.address_tests._NativeDB(address.publisher))
    async with address.publisher.transaction():
        retained = await adoption._swap_sealed_stage_table(
            schema,
            SimpleNamespace(__main_table__="entity_address_unified"),
            SimpleNamespace(__tablename__="entity_address_unified_next", __my_additional_indexes__=[]),
            address.descriptor["protected_owner_oid"],
        )
        oid = await address.publisher.fetchval(
            "SELECT to_regclass($1)::oid::bigint", schema + ".entity_address_unified"
        )
        await address.publisher.execute(
            f'UPDATE "{schema}".entity_address_result_generation '
            "SET local_generation=2,origin_generation=2,relation_oids[1]=$1",
            oid,
        )
        await address.publisher.execute(f'UPDATE "{schema}".address_alias_state_v1 SET generation=generation+1')
    async with address.publisher.transaction(isolation="repeatable_read", readonly=True):
        authority = await artifact.generation.read_entity_address_result_generation_authority(
            artifact._CatalogSession(address.publisher), schema_name=schema
        )
    fixture.source = PinnedAddressSource(
        schema, "entity_address_unified", artifact._digest(artifact._authority_document(authority))
    )
    return retained, oid


@pytest.mark.asyncio
async def test_address_rotation_keeps_old_and_new_scoped_readers(compact_publication_db, monkeypatch):
    fixture = compact_publication_db
    first = await publication._candidate(fixture, (42, 88), 0)
    await _publish_ready(fixture, first)
    old_manifest, old_rows = await publication._read(fixture)
    retained, replacement_oid = await _rotate_source(fixture, monkeypatch)
    second = await publication._candidate(fixture, (42, 99), old_manifest.generation_id)
    await publication._prepare(fixture, second)
    await publication._seal(fixture, second)
    async with publication._reader(fixture) as (connection, native):
        old_pin = await resolve_network_serving_manifest(native, control_schema=fixture.control_schema)
        assert old_pin.address_table_oid == retained["relation_oid"] == old_manifest.address_table_oid
        assert old_pin.compact_layout.base.table_name == retained["retained_name"]
        assert await publication._scoped_rows(connection, old_pin, (42,)) == old_rows
        published = await asyncio.wait_for(publication._publish(fixture, second), timeout=10)
        assert await publication._scoped_rows(connection, old_pin, (42,)) == old_rows
        assert (
            await resolve_network_serving_manifest(native, control_schema=fixture.control_schema)
        ).generation_id == (old_manifest.generation_id)
        fresh, new_rows = await publication._read(fixture)
        assert fresh.generation_id == published["generation_id"]
        assert fresh.address_table_oid == replacement_oid != old_manifest.address_table_oid
        assert fresh.source_generations != old_manifest.source_generations
        assert len(new_rows) == 1
        assert new_rows[0]["location_key"] == old_rows[0]["location_key"] + "-next"
        assert new_rows[0]["city_name"] == "New City"
        assert new_rows[0]["canonical_network_ids"] == [42]
    retained_pin, retained_rows = await publication._read(fixture, old_manifest.generation_id)
    assert retained_pin.address_table_oid == old_manifest.address_table_oid and retained_rows == old_rows
    assert (await publication._read(fixture, network_ids=(88,)))[1] == []
    assert len((await publication._read(fixture, old_manifest.generation_id, network_ids=(88,)))[1]) == 1


@pytest.mark.asyncio
async def test_stale_head_refuses_sealed_competitor_without_partial_manifest(compact_publication_db):
    fixture = compact_publication_db
    winner = await publication._candidate(fixture, (42, 88), 0)
    loser = await publication._candidate(fixture, (42, 99), 0)
    for candidate_target in (winner, loser):
        await publication._prepare(fixture, candidate_target)
        await publication._seal(fixture, candidate_target)
    before_refs = await publication._refs(fixture, loser)
    await publication._publish(fixture, winner)
    winning_manifest, winning_rows = await publication._read(fixture)
    with pytest.raises(NetworkPublicationError, match="Expected serving head changed"):
        await publication._publish(fixture, loser)
    assert not fixture.execution_connection.is_in_transaction()
    assert await _state(fixture, loser) == "ready"
    assert await publication._refs(fixture, loser) == before_refs
    assert (
        await fixture.connection.fetchval(
            f'SELECT count(*) FROM "{fixture.control_schema}".network_serving_manifest WHERE candidate_id=$1',
            UUID(loser.candidate_id),
        )
        == 0
    )
    assert (
        await fixture.connection.fetchval(
            f'SELECT count(*) FROM "{fixture.control_schema}".network_candidate_custody WHERE candidate_id=$1',
            UUID(loser.candidate_id),
        )
        == 1
    )
    current, current_rows = await publication._read(fixture)
    assert current.generation_id == winning_manifest.generation_id and current_rows == winning_rows
    assert (await publication._read(fixture, network_ids=(99,)))[1] == []


@pytest.mark.asyncio
async def test_failed_compact_preparation_rolls_back_local_artifacts_and_resumes(compact_publication_db, monkeypatch):
    fixture = compact_publication_db
    first = await publication._candidate(fixture, (42, 88), 0)
    await _publish_ready(fixture, first)
    original_pin, original_rows = await publication._read(fixture)
    candidate_target = await publication._candidate(fixture, (42, 99), original_pin.generation_id)
    prepare_slices = compact_preparation.prepare_network_membership_slices

    async def fail_after_copy(*arguments, **options):
        await prepare_slices(*arguments, **options)
        raise RuntimeError("synthetic failure after compact copy")

    with monkeypatch.context() as patch:
        patch.setattr(compact_preparation, "prepare_network_membership_slices", fail_after_copy)
        with pytest.raises(RuntimeError, match="synthetic failure after compact copy"):
            await publication._prepare(fixture, candidate_target)
    assert not fixture.execution_connection.is_in_transaction()
    assert await _state(fixture, candidate_target) == "sealed"
    for table in ("network_office_edge", "network_slice_ref"):
        assert (
            await fixture.connection.fetchval("SELECT to_regclass($1)", candidate_target.schema_name + "." + table)
            is None
        )
    assert (
        await fixture.connection.fetchval(
            "SELECT has_table_privilege($1,$2,'INSERT')",
            fixture.address.roles["reader"],
            candidate_target.schema_name + ".network_membership",
        )
        is False
    )
    with pytest.raises(asyncpg.InsufficientPrivilegeError):
        await fixture.address.reader.execute(f'DELETE FROM "{candidate_target.schema_name}".network_membership')
    pin_after_failure, rows_after_failure = await publication._read(fixture)
    assert pin_after_failure.generation_id == original_pin.generation_id and rows_after_failure == original_rows
    published = await _publish_ready(fixture, candidate_target)
    assert await _state(fixture, candidate_target) == "published"
    final_pin, final_rows = await publication._read(fixture)
    assert final_pin.generation_id == published["generation_id"] != original_pin.generation_id
    assert final_rows == original_rows
    assert len((await publication._read(fixture, network_ids=(99,)))[1]) == 1


@pytest.fixture
async def dense_compact_db(monkeypatch):
    original_source = publication._full_two_offices

    async def prepare_source(connection, roles, original):
        schema = await original_source(connection, roles, original)
        await connection.execute(
            f'UPDATE "{schema}".entity_address_unified '
            "SET location_key=encode(sha256(convert_to(location_key,'UTF8')),'hex'),"
            "entity_id='1234567893',npi=1234567893,entity_name='Example Provider'"
        )
        await connection.execute(
            f'INSERT INTO "{schema}".entity_address_unified '
            f'SELECT (jsonb_populate_record(NULL::"{schema}".entity_address_unified,to_jsonb(office)||'
            "jsonb_build_object('location_key',encode(sha256(convert_to(location_key||'-other','UTF8')),'hex'),"
            "'address_key',gen_random_uuid(),'entity_id','1000000004','npi',1000000004,"
            f"'entity_name','Other Provider'))).* FROM \"{schema}\".entity_address_unified office"
        )
        return schema

    monkeypatch.setattr(publication, "_full_two_offices", prepare_source)
    async with asynccontextmanager(publication.compact_publication_db.__wrapped__)(monkeypatch) as fixture:
        yield fixture


async def _dense_candidate(fixture, memberships, expected_head):
    candidate_id = uuid4()
    candidate_target = MembershipCopyTarget(
        fixture.dataset_id,
        fixture.schema_id,
        fixture.producer_id,
        str(candidate_id),
        "network_candidate_" + candidate_id.hex,
    )
    offices = await fixture.connection.fetch(
        f'SELECT location_key,entity_type,entity_id,address_key FROM "{fixture.address.schema}".entity_address_unified '
        "ORDER BY entity_id,location_key"
    )
    facts = [
        dict(
            network_id=network_id,
            provider_system=offices[index]["entity_type"],
            provider_id=offices[index]["entity_id"],
            location_id=str(offices[index]["address_key"]),
            evidence_id="synthetic-" + str(evidence),
        )
        for evidence, (network_id, index) in enumerate(memberships)
    ]
    membership_bytes = json.dumps(facts, separators=(",", ":")).encode()
    connection = fixture.execution_connection
    async with connection.transaction():
        await create_network_candidate(
            connection,
            candidate_target,
            source_generations={"unified_address": fixture.source.generation_id},
            approved_custom_revision=0,
            expected_head=expected_head,
            expected_rows=len(facts),
            control_schema=fixture.control_schema,
        )
        await publication._bind_offices(connection, candidate_target, offices)
        await admit_network_membership_batch(
            connection,
            candidate_target,
            batch_id=uuid4(),
            input_bytes=membership_bytes,
            expected_input_sha256=hashlib.sha256(membership_bytes).hexdigest(),
            control_schema=fixture.control_schema,
        )
        await seal_network_candidate(connection, candidate_target, control_schema=fixture.control_schema)
    return candidate_target, offices


@pytest.mark.asyncio
async def test_dense_membership_deduplicates_exact_offices_and_isolates_providers(dense_compact_db):
    fixture = dense_compact_db
    pairs = [(42, 0), (42, 0), (88, 0), (99, 0), (42, 1), (88, 1), (88, 2), (99, 3)]
    candidate_target, offices = await _dense_candidate(fixture, pairs, 0)
    prepared = await publication._prepare(fixture, candidate_target)
    report = json.loads(prepared["validation_json"])
    assert (report["membership_rows"], report["distinct_memberships"], report["projected_locations"]) == (8, 7, 4)
    assert (
        await fixture.connection.fetchval(f'SELECT count(*) FROM "{candidate_target.schema_name}".network_office_edge')
        == 7
    )
    await publication._seal(fixture, candidate_target)
    await publication._publish(fixture, candidate_target)
    manifest, selected = await publication._read(fixture, network_ids=(42, 88))
    networks_by_location = {
        offices[index]["location_key"]: networks for index, networks in ((0, [42, 88, 99]), (1, [42, 88]), (2, [88]))
    }
    assert {
        office_row["location_key"]: office_row["canonical_network_ids"] for office_row in selected
    } == networks_by_location
    async with publication._reader(fixture) as (_connection, native):
        with network_address_read_scope(manifest, (42,)) as scope:
            page = await read_network_provider_page(native, scope, control_schema=fixture.control_schema)
            assert page["total_count"] == 1
            assert page["providers"][0]["provider_id"] == "1000000004"
            assert {office_row["location_key"] for office_row in page["providers"][0]["locations"]} == {
                offices[0]["location_key"],
                offices[1]["location_key"],
            }
            with pytest.raises(NetworkProviderNotFound):
                await read_network_provider_detail(
                    native,
                    scope,
                    provider_system="npi",
                    provider_id="1234567893",
                    control_schema=fixture.control_schema,
                )
        with network_address_read_scope(manifest, (88,)) as scope:
            page = await read_network_provider_page(native, scope, control_schema=fixture.control_schema)
            assert page["total_count"] == 2
            assert {provider["provider_id"]: len(provider["locations"]) for provider in page["providers"]} == {
                "1000000004": 2,
                "1234567893": 1,
            }
