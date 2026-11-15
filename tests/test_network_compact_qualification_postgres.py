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
from process.entity_address_retained_names import retained_address_name
from process.network_address_projection import PinnedAddressSource
from process.network_compact_retention import NetworkCompactRetentionError, require_named_address_artifacts_unreferenced
from process.network_membership_candidate_lifecycle import (
    admit_network_membership_batch,
    create_network_candidate,
    seal_network_candidate,
)
from process.network_membership_copy import MembershipCopyTarget
from process.network_membership_publication import NetworkPublicationError
from process.network_published_api_pin import read_published_api_pin
from process.network_serving_read import NetworkServingReadUnavailable, resolve_network_serving_manifest
from tests import test_network_compact_publication_postgres as publication
from tests.test_api_database_jit import (
    test_native_api_jit_covers_pool_growth_transaction_end_and_reconnect as test_native_api_jit_covers_pool_growth_transaction_end_and_reconnect,
)
from tests.test_network_address_artifact_postgres import address_db as address_db
from tests.test_network_address_read_race_postgres import _prepare_replacement, _swap_replacement
from tests.test_network_compact_address_postgres import compact_db as compact_db
from tests.test_network_compact_publication_postgres import compact_publication_db as compact_publication_db
from tests.test_network_geo_search_api_sql import projection_api_case as projection_api_case
from tests.test_network_geo_search_api_sql import (
    test_postgres_search_projection_preserves_routes_and_unselected_fallback as test_postgres_search_projection_preserves_routes_and_unselected_fallback,
)
from tests.test_network_geo_search_postgres import search_db as search_db
from tests.test_network_geo_search_postgres import (
    test_bounded_search_matches_full_compact_rows_and_custom_precedence as test_bounded_search_matches_full_compact_rows_and_custom_precedence,
)
from tests.test_network_geo_search_postgres import (
    test_old_incompatible_profile_has_no_search_receipt_or_heap as test_old_incompatible_profile_has_no_search_receipt_or_heap,
)
from tests.test_network_geo_search_postgres import (
    test_search_later_batch_has_index_range_under_generic_plan as test_search_later_batch_has_index_range_under_generic_plan,
)
from tests.test_network_geo_search_postgres import (
    test_search_parity_failure_rolls_back_only_its_new_projection as test_search_parity_failure_rolls_back_only_its_new_projection,
)
from tests.test_network_geo_search_postgres import (
    test_search_rejects_changed_index_identity_and_receipt_scope as test_search_rejects_changed_index_identity_and_receipt_scope,
)
from tests.test_network_geo_search_publication_postgres import (
    test_geo_search_preparation_custody_publication_and_retained_fallback as test_geo_search_preparation_custody_publication_and_retained_fallback,
)
from tests.test_network_index_catalog_postgres import (
    test_index_catalog_reapplies_path_for_each_correlated_family as test_index_catalog_reapplies_path_for_each_correlated_family,
)
from tests.test_network_index_catalog_postgres import (
    test_index_catalog_restores_exact_path_on_every_outcome as test_index_catalog_restores_exact_path_on_every_outcome,
)
from tests.test_network_index_catalog_postgres import (
    test_legacy_catalog_keeps_its_original_deparse_context as test_legacy_catalog_keeps_its_original_deparse_context,
)
from tests.test_network_membership_slices_postgres import slice_db as slice_db


async def _named_base_rows(fixture):
    async with publication._reader(fixture) as (_session, connection):
        manifest, _, _ = await read_published_api_pin(connection, None, fixture.control_schema, approved=False)
        base = manifest.compact_layout.base
        rows = await connection.fetch(
            f'SELECT location_key FROM "{base.schema_name}"."{base.table_name}" ORDER BY location_key'
        )
        return base.table_name, [row["location_key"] for row in rows]


@pytest.mark.asyncio
async def test_copied_address_generation_survives_real_rotation(compact_publication_db, monkeypatch):
    """Copy immutable metadata, rotate with the real publisher, and preserve named reads."""
    fixture = compact_publication_db
    candidate_target = await publication._candidate(fixture, (42, 88), 0)
    await _publish_ready(fixture, candidate_target)
    address = fixture.address
    assert address.descriptor["contract"] == "network_address_artifact.postgres.v1"
    schema, table = address.schema, "entity_address_unified"
    metadata_sql = (
        f'SELECT validation_json::text FROM "{fixture.control_schema}".network_membership_candidate '
        "ORDER BY candidate_id"
    )
    metadata = await fixture.connection.fetch(metadata_sql)
    before = await _named_base_rows(fixture)
    assert before == (table, ["old-office", "other-office"])
    old_oid = await _copy_address_base(address)
    assert await _named_base_rows(fixture) == before
    await _prepare_replacement(address, populated=False)
    retained = await _swap_replacement(address, monkeypatch)
    generation_by_field = {
        field: address.descriptor["authority"][field]
        for field in ("origin_lineage_id", "origin_generation", "published_at")
    }
    name = retained_address_name(table, generation_by_field)
    assert retained["retained_name"] == name
    assert await _named_base_rows(fixture) == (name, before[1])
    # A second ordinary rotation retains a different logical generation without replacing the first.
    await _prepare_replacement(address, populated=False)
    successor = await _swap_replacement(address, monkeypatch)
    assert successor["retained_name"] != name
    assert await _named_base_rows(fixture) == (name, before[1])
    assert await fixture.connection.fetch(metadata_sql) == metadata
    async with fixture.connection.transaction():
        with pytest.raises(NetworkCompactRetentionError, match="artifact_referenced"):
            await require_named_address_artifacts_unreferenced(
                fixture.connection, [(schema, name)], control_schema=fixture.control_schema
            )
    # Existing receipts whose heap was already retained under its old spelling remain readable.
    legacy = f"{table}_retained_{old_oid:x}"
    await address.admin.execute(f'ALTER TABLE "{schema}"."{name}" RENAME TO "{legacy}"')
    assert await _named_base_rows(fixture) == (legacy, before[1])
    async with fixture.connection.transaction():
        with pytest.raises(NetworkCompactRetentionError, match="artifact_referenced"):
            await require_named_address_artifacts_unreferenced(
                fixture.connection, [(schema, legacy)], control_schema=fixture.control_schema
            )


async def _copy_address_base(address):
    schema, table = address.schema, "entity_address_unified"
    old_oid = await address.admin.fetchval("SELECT to_regclass($1)::oid", f"{schema}.{table}")
    async with address.admin.transaction():
        await address.admin.execute(
            f'ALTER TABLE "{schema}"."{table}" RENAME TO original_before_copy; '
            f'CREATE TABLE "{schema}"."{table}" (LIKE "{schema}".original_before_copy INCLUDING ALL); '
            f'INSERT INTO "{schema}"."{table}" OVERRIDING SYSTEM VALUE SELECT * FROM "{schema}".original_before_copy; '
            f'ALTER TABLE "{schema}"."{table}" OWNER TO "{address.roles["owner"]}"; '
            f'GRANT SELECT ON "{schema}"."{table}" TO "{address.roles["reader"]}"'
        )
    assert await address.admin.fetchval("SELECT to_regclass($1)::oid", f"{schema}.{table}") != old_oid
    return old_oid


@pytest.mark.asyncio
@pytest.mark.parametrize("during_read", [False, True], ids=["before-snapshot", "after-snapshot"])
async def test_copied_legacy_rotation_refuses_api_replacement(compact_publication_db, monkeypatch, during_read):
    fixture = compact_publication_db
    target = await publication._candidate(fixture, (42, 88), 0)
    await _publish_ready(fixture, target)
    address = fixture.address
    await _copy_address_base(address)
    assert await _named_base_rows(fixture) == ("entity_address_unified", ["old-office", "other-office"])

    async def read(connection):
        return await read_published_api_pin(connection, None, fixture.control_schema, approved=False)

    await _assert_legacy_rotation_refused(address, monkeypatch, during_read, read, NetworkServingReadUnavailable)


@pytest.mark.asyncio
@pytest.mark.parametrize("during_read", [False, True], ids=["before-snapshot", "after-snapshot"])
async def test_copied_legacy_rotation_refuses_artifact_replacement(address_db, monkeypatch, during_read):
    address = address_db
    await _copy_address_base(address)

    async def read(connection):
        return await artifact.verify_network_address_artifact(
            connection, address.descriptor, runtime_roles=(address.roles["reader"],)
        )

    async with address.reader.transaction(isolation="repeatable_read", readonly=True):
        assert (await read(address.reader)).table_name == "entity_address_unified"
    await _assert_legacy_rotation_refused(address, monkeypatch, during_read, read, artifact.NetworkAddressArtifactError)


async def _assert_legacy_rotation_refused(address, monkeypatch, during_read, read, error):
    await _prepare_replacement(address, populated=True)
    if not during_read:
        await _swap_replacement(address, monkeypatch, legacy_names=True)
    async with address.reader.transaction(isolation="repeatable_read", readonly=True):
        connection = address.reader
        origin_sql = (
            f'SELECT origin_generation FROM "{address.schema}".entity_address_result_generation WHERE singleton'
        )
        origin = await connection.fetchval(origin_sql)
        assert origin == (1 if during_read else 2)
        if during_read:
            await _swap_replacement(address, monkeypatch, legacy_names=True)
            assert await connection.fetchval(origin_sql) == origin
        # The replacement is visible by its live name, but cannot serve as the saved generation.
        assert (
            await connection.fetchval(f'SELECT location_key FROM "{address.schema}".entity_address_unified')
            == "new-office"
        )
        reason = (
            "source_generation_changed" if error is artifact.NetworkAddressArtifactError else "metadata is unavailable"
        )
        with pytest.raises(error, match=reason):
            await read(connection)


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["default", "extra_column", "index"])
@pytest.mark.parametrize("retained", [False, True], ids=["copied-live", "copied-retained"])
async def test_copied_address_shape_keeps_semantic_checks(compact_publication_db, monkeypatch, damage, retained):
    fixture = compact_publication_db
    candidate_target = await publication._candidate(fixture, (42, 88), 0)
    await _publish_ready(fixture, candidate_target)
    address = fixture.address
    metadata_sql = f'SELECT validation_json::text FROM "{fixture.control_schema}".network_membership_candidate WHERE candidate_id=$1'
    metadata = await fixture.connection.fetchval(metadata_sql, UUID(candidate_target.candidate_id))
    descriptor = json.loads(metadata)["serving_readiness"]["address_artifact"]
    await _copy_address_base(address)
    async with address.reader.transaction(isolation="repeatable_read", readonly=True):
        assert (
            await artifact.verify_network_address_artifact(
                address.reader, descriptor, runtime_roles=(address.roles["reader"],)
            )
        ).table_name == "entity_address_unified"
    table_name = "entity_address_unified"
    if retained:
        await _prepare_replacement(address, populated=True)
        table_name = (await _swap_replacement(address, monkeypatch))["retained_name"]
        async with address.reader.transaction(isolation="repeatable_read", readonly=True):
            assert (
                await artifact.verify_network_address_artifact(
                    address.reader, descriptor, runtime_roles=(address.roles["reader"],)
                )
            ).table_name == table_name
    table = f'"{address.schema}"."{table_name}"'
    changes_by_fault = {
        "default": f"ALTER TABLE {table} ALTER COLUMN checksum SET DEFAULT 999",
        "extra_column": f"ALTER TABLE {table} ADD COLUMN unexpected_copy_column integer",
        "index": f"CREATE INDEX unexpected_copy_index ON {table}(location_key)",
    }
    await address.admin.execute(changes_by_fault[damage])
    async with address.reader.transaction(isolation="repeatable_read", readonly=True):
        with pytest.raises(artifact.NetworkAddressArtifactError, match="structural_catalog_changed"):
            await artifact.verify_network_address_artifact(
                address.reader, descriptor, runtime_roles=(address.roles["reader"],)
            )
    assert await fixture.connection.fetchval(metadata_sql, UUID(candidate_target.candidate_id)) == metadata


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
    prepare_slices = compact_preparation._prepare_slices

    async def fail_after_copy(*arguments, **options):
        await prepare_slices(*arguments, **options)
        raise RuntimeError("synthetic failure after compact copy")

    with monkeypatch.context() as patch:
        patch.setattr(compact_preparation, "_prepare_slices", fail_after_copy)
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


@pytest.mark.asyncio
@pytest.mark.parametrize("can_read_registry", [False, True])
async def test_absent_origin_cannot_rotate_referenced_copy(compact_publication_db, monkeypatch, can_read_registry):
    fixture = compact_publication_db
    await _publish_ready(fixture, await publication._candidate(fixture, (42, 88), 0))
    address = fixture.address
    await _copy_address_base(address)
    expected_rows = (await _named_base_rows(fixture))[1]
    origin = await address.admin.fetchrow(
        f'SELECT origin_lineage_id,origin_generation,published_at FROM "{address.schema}".entity_address_result_generation '
        "WHERE singleton"
    )
    await _prepare_replacement(address, populated=False)
    monkeypatch.setenv("HLTHPRT_NETWORK_REGISTRY_SCHEMA", fixture.control_schema)
    if can_read_registry:
        await fixture.connection.execute(
            f'GRANT USAGE ON SCHEMA "{fixture.control_schema}" TO "{address.roles["publisher"]}"; '
            f'GRANT SELECT ON ALL TABLES IN SCHEMA "{fixture.control_schema}" TO "{address.roles["publisher"]}"'
        )
    await address.admin.execute(
        f'ALTER TABLE "{address.schema}".entity_address_result_generation '
        "ALTER COLUMN origin_lineage_id DROP NOT NULL,ALTER COLUMN origin_generation DROP NOT NULL,"
        "ALTER COLUMN published_at DROP NOT NULL; "
        f'UPDATE "{address.schema}".entity_address_result_generation '
        "SET origin_lineage_id=NULL,origin_generation=NULL,published_at=NULL"
    )
    failure = NetworkCompactRetentionError if can_read_registry else asyncpg.InsufficientPrivilegeError
    with pytest.raises(failure):
        await _swap_replacement(address, monkeypatch)
    unchanged_rows = await address.reader.fetch(
        f'SELECT location_key FROM "{address.schema}".entity_address_unified ORDER BY location_key'
    )
    assert [address_row["location_key"] for address_row in unchanged_rows] == expected_rows
    with pytest.raises(NetworkServingReadUnavailable, match="metadata is unavailable"):
        await _named_base_rows(fixture)
    assert await address.admin.fetchval(
        "SELECT to_regclass($1) IS NOT NULL", address.schema + ".entity_address_unified_next"
    )
    await address.admin.execute(
        f'UPDATE "{address.schema}".entity_address_result_generation '
        "SET origin_lineage_id=$1,origin_generation=$2,published_at=$3 WHERE singleton",
        *origin.values(),
    )
    await address.admin.execute(
        f'ALTER TABLE "{address.schema}".entity_address_result_generation '
        "ALTER COLUMN origin_lineage_id SET NOT NULL,ALTER COLUMN origin_generation SET NOT NULL,"
        "ALTER COLUMN published_at SET NOT NULL"
    )
    assert (await _named_base_rows(fixture))[1] == expected_rows
