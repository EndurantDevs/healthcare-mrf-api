# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real independent seals retain address/edge artifacts through rollback and rotation."""

import asyncio
import json
from contextlib import AsyncExitStack, asynccontextmanager
from copy import deepcopy
from types import SimpleNamespace
from uuid import UUID, uuid4

import asyncpg
import pytest

from process import entity_address_snapshot_adoption as adoption
from process import entity_address_snapshot_ownership as ownership
from process import entity_address_snapshot_preparation as preparation
from process import network_candidate_custody as custody
from process import network_compact_retention as retention
from process.entity_address_retained_names import retained_address_name
from process.entity_address_snapshot_alias import AUTHORITY_TABLE
from process.network_cms_registry_address_capture import NativeReceiptSession
from tests.test_network_address_artifact_postgres import _NativeDB, address_db
from tests.test_network_address_projection_postgres import projection_db as projection_db
from tests.test_network_compact_publication_postgres import (
    _candidate,
    _prepare,
    _publish,
    _seal,
    compact_publication_db,
)
from tests.test_network_membership_publication_postgres import _candidate_writer_roles
from tests.test_network_membership_publication_postgres import _publish as publish_noncompact
from tests.test_network_membership_serving_indexes_postgres import _prepare as prepare_noncompact
from tests.test_network_membership_serving_indexes_postgres import serving_indexes_db as serving_indexes_db
from tests.test_network_membership_validation_postgres import validation_db as validation_db
from tests.test_network_serving_schema_postgres import serving_schema as serving_schema


@pytest.fixture
async def sealed_compact(compact_publication_db):
    fixture = compact_publication_db
    target = await _candidate(fixture, (42, 88), 0)
    await _prepare(fixture, target)
    seal = await _seal(fixture, target)
    return fixture, target, seal


async def _guard(fixture, oids):
    async with fixture.connection.transaction(isolation="read_committed"):
        return await retention.require_network_compact_artifacts_unreferenced(
            fixture.connection, oids, control_schema=fixture.control_schema
        )


@pytest.mark.parametrize("state", ["ready", "published", "ineligible"])
async def test_sealed_candidates_retain_rollback_dependencies(sealed_compact, state):
    fixture, target, seal = sealed_compact
    if state != "ready":
        publication = await _publish(fixture, target)
        if state == "ineligible":
            await fixture.connection.execute(
                f'UPDATE "{fixture.control_schema}".network_serving_manifest SET eligible=false WHERE generation_id=$1',
                publication["generation_id"],
            )
    source_oid = fixture.address.descriptor["table"]["oid"]
    edge_oid = seal["receipt"]["closure"]["relation_oids"]["network_office_edge"]
    for oid in (source_oid, edge_oid, fixture.address.descriptor["authority_table_oid"]):
        with pytest.raises(retention.NetworkCompactRetentionError, match="artifact_referenced"):
            await _guard(fixture, [oid])
    unrelated = await fixture.connection.fetchval("SELECT 'pg_catalog.pg_class'::regclass::oid::bigint")
    assert await _guard(fixture, [unrelated]) is None


async def _alter_candidate_metadata(fixture, copy_target, damage, original):
    connection = fixture.connection
    namespace = '"' + fixture.control_schema + '"'
    if damage in {"erased_descriptor", "erased_component"}:
        report = json.loads(original["report"])
        if damage == "erased_descriptor":
            report["serving_readiness"].pop("address_artifact")
        else:
            report["serving_readiness"]["component"] = "legacy"
        await connection.execute(
            f"UPDATE {namespace}.network_membership_candidate SET validation_json=$2::jsonb WHERE candidate_id=$1",
            UUID(copy_target.candidate_id),
            json.dumps(report),
        )
    elif damage == "seal_digest":
        await connection.execute(
            f"UPDATE {namespace}.network_candidate_custody SET seal_sha256=$2 WHERE candidate_id=$1",
            UUID(copy_target.candidate_id),
            "0" * 64,
        )
    elif damage == "ledger_owner":
        await connection.execute(
            f'ALTER TABLE {namespace}.network_candidate_custody OWNER TO "{fixture.address.roles["outsider"]}"'
        )
    elif damage == "missing_ready_seal":
        await connection.execute(
            f"DELETE FROM {namespace}.network_candidate_custody WHERE candidate_id=$1", UUID(copy_target.candidate_id)
        )
    else:
        await connection.execute(
            f"UPDATE {namespace}.network_membership_candidate SET state='rejected' WHERE candidate_id=$1",
            UUID(copy_target.candidate_id),
        )


@pytest.mark.parametrize(
    "damage",
    ["erased_descriptor", "erased_component", "seal_digest", "ledger_owner", "missing_ready_seal", "demoted_state"],
)
async def test_mutable_metadata_cannot_erase_a_sealed_pin(sealed_compact, damage):
    fixture, copy_target, _ = sealed_compact
    connection = fixture.connection
    namespace = '"' + fixture.control_schema + '"'
    original = await connection.fetchrow(
        f"SELECT state,validation_json::text AS report FROM {namespace}.network_membership_candidate WHERE candidate_id=$1",
        UUID(copy_target.candidate_id),
    )
    stored = await connection.fetchrow(
        f"SELECT * FROM {namespace}.network_candidate_custody WHERE candidate_id=$1", UUID(copy_target.candidate_id)
    )
    owner = await connection.fetchval(
        "SELECT quote_ident(r.rolname) FROM pg_class c JOIN pg_roles r ON r.oid=c.relowner WHERE c.oid=to_regclass($1)",
        namespace + ".network_candidate_custody",
    )
    try:
        await _alter_candidate_metadata(fixture, copy_target, damage, original)
        with pytest.raises((retention.NetworkCompactRetentionError, custody.NetworkCandidateCustodyError)):
            await _guard(fixture, [fixture.address.descriptor["table"]["oid"]])
    finally:
        await connection.execute(f"ALTER TABLE {namespace}.network_candidate_custody OWNER TO {owner}")
        await connection.execute(
            f"UPDATE {namespace}.network_membership_candidate SET state=$2,validation_json=$3::jsonb WHERE candidate_id=$1",
            UUID(copy_target.candidate_id),
            original["state"],
            original["report"],
        )
        await connection.execute(
            f"INSERT INTO {namespace}.network_candidate_custody(candidate_id,seal_sha256,receipt_json,created_at) VALUES($1,$2,$3::jsonb,$4) ON CONFLICT(candidate_id) DO UPDATE SET seal_sha256=excluded.seal_sha256,receipt_json=excluded.receipt_json",
            stored["candidate_id"],
            stored["seal_sha256"],
            stored["receipt_json"],
            stored["created_at"],
        )


async def test_missing_published_seal_is_not_retirement(sealed_compact):
    fixture, target, _ = sealed_compact
    await _publish(fixture, target)
    namespace = '"' + fixture.control_schema + '"'
    row = await fixture.connection.fetchrow(
        f"SELECT * FROM {namespace}.network_candidate_custody WHERE candidate_id=$1", UUID(target.candidate_id)
    )
    try:
        await fixture.connection.execute(
            f"DELETE FROM {namespace}.network_candidate_custody WHERE candidate_id=$1", UUID(target.candidate_id)
        )
        with pytest.raises(retention.NetworkCompactRetentionError, match="published_seal_missing"):
            await _guard(fixture, [fixture.address.descriptor["table"]["oid"]])
    finally:
        await fixture.connection.execute(
            f"INSERT INTO {namespace}.network_candidate_custody(candidate_id,seal_sha256,receipt_json,created_at) VALUES($1,$2,$3::jsonb,$4)",
            row["candidate_id"],
            row["seal_sha256"],
            row["receipt_json"],
            row["created_at"],
        )


async def test_retained_name_cannot_release_a_physical_pin(sealed_compact):
    fixture, _, _ = sealed_compact
    oid = fixture.address.descriptor["table"]["oid"]
    retained = f"entity_address_unified_retained_{oid:x}"
    await fixture.connection.execute(
        f'ALTER TABLE "{fixture.address.schema}".entity_address_unified RENAME TO "{retained}"'
    )
    assert (
        await fixture.connection.fetchval(
            "SELECT to_regclass($1)::oid::bigint", f'"{fixture.address.schema}"."{retained}"'
        )
        == oid
    )
    with pytest.raises(retention.NetworkCompactRetentionError, match="artifact_referenced"):
        await _guard(fixture, [oid])


async def test_second_check_sees_a_concurrently_committed_seal(compact_publication_db):
    fixture = compact_publication_db
    copy_target = await _candidate(fixture, (42, 88), 0)
    await _prepare(fixture, copy_target)
    oid = fixture.address.descriptor["table"]["oid"]
    first_check, committed = asyncio.Event(), asyncio.Event()

    async def cleanup():
        async with fixture.connection.transaction(isolation="read_committed"):
            await retention.require_network_compact_artifacts_unreferenced(
                fixture.connection, [oid], control_schema=fixture.control_schema
            )
            first_check.set()
            await committed.wait()
            await fixture.connection.execute(
                f'LOCK TABLE ONLY "{fixture.address.schema}".entity_address_unified IN ACCESS EXCLUSIVE MODE NOWAIT'
            )
            await retention.require_network_compact_artifacts_unreferenced(
                fixture.connection, [oid], control_schema=fixture.control_schema
            )
            await fixture.connection.execute(f'DROP TABLE "{fixture.address.schema}".entity_address_unified RESTRICT')

    operation = asyncio.create_task(cleanup())
    try:
        await asyncio.wait_for(first_check.wait(), 10)
        await _seal(fixture, copy_target)
    finally:
        committed.set()
    with pytest.raises(retention.NetworkCompactRetentionError, match="artifact_referenced"):
        await operation
    assert (
        await fixture.connection.fetchval(
            "SELECT to_regclass($1)::oid::bigint", f'"{fixture.address.schema}".entity_address_unified'
        )
        == oid
    )


async def test_repeatable_read_cannot_hide_a_new_seal(compact_publication_db):
    fixture = compact_publication_db
    async with fixture.connection.transaction(isolation="repeatable_read"):
        with pytest.raises(retention.NetworkCompactRetentionError, match="fresh_snapshot_required"):
            await retention.require_network_compact_artifacts_unreferenced(
                fixture.connection, [fixture.address.descriptor["table"]["oid"]], control_schema=fixture.control_schema
            )


async def test_legacy_cleanup_without_registry_stays_available(address_db):
    async with address_db.admin.transaction(isolation="repeatable_read"):
        assert (
            await retention.require_network_compact_artifacts_unreferenced(
                address_db.admin, [address_db.descriptor["table"]["oid"]], control_schema="absent_registry"
            )
            is None
        )


async def _retained_publication(fixture, generation, *, legacy=False, target_schema=None):
    """Copy the complete named family with deliberately stale legacy catalog IDs."""
    address = fixture.address
    destination = address.schema if target_schema is None else target_schema
    dataset_id = uuid4()
    namespace = ownership.entity_address_archive_stage_schema(dataset_id)
    tables = [model.__tablename__ for model in ownership._models()]
    owner = ownership.EntityAddressArchiveStageOwnership(
        dataset_id,
        namespace,
        99,
        tuple(sorted((name, index) for index, name in enumerate([*tables, AUTHORITY_TABLE], 1))),
    )
    publication_by_field = {
        "contract": "entity-address-table-publication.v2",
        "schema_name": destination,
        "relations": [{"table_name": name, "relation_oid": index} for index, name in enumerate(tables, 1)],
        "retained_relations": [],
        "alias_authority": {"schema_name": namespace, "schema_oid": 99, "relation_oid": 8},
    }
    if not legacy:
        publication_by_field["serving_generation"] = generation
    names = [
        (destination, f"{name}_retained_{index:x}" if legacy else retained_address_name(name, generation))
        for index, name in enumerate(tables, 1)
    ]
    names.append((namespace, AUTHORITY_TABLE))
    await address.admin.execute(f'CREATE SCHEMA "{namespace}" AUTHORIZATION "{address.roles["owner"]}"')
    for name, (schema, retained) in zip(tables, names, strict=False):
        await address.admin.execute(f'CREATE TABLE "{schema}"."{retained}" AS TABLE "{address.schema}"."{name}"')
        await address.admin.execute(f'ALTER TABLE "{schema}"."{retained}" OWNER TO "{address.roles["owner"]}"')
    await address.admin.execute(f'CREATE TABLE "{namespace}"."{AUTHORITY_TABLE}" (marker integer)')
    await address.admin.execute(f'ALTER TABLE "{namespace}"."{AUTHORITY_TABLE}" OWNER TO "{address.roles["owner"]}"')
    await address.admin.execute(f'GRANT USAGE ON SCHEMA "{namespace}" TO "{address.roles["reader"]}"')
    await address.admin.execute(f'GRANT USAGE ON SCHEMA "{fixture.control_schema}" TO "{address.roles["publisher"]}"')
    for table in retention._TABLES:
        await address.admin.execute(
            f'GRANT SELECT ON "{fixture.control_schema}"."{table}" TO "{address.roles["publisher"]}"'
        )
    return owner, publication_by_field, names


def _address_origin(fixture, *, unrelated=False):
    authority = fixture.address.descriptor["authority"]
    generation_by_field = {
        field: authority[field] for field in ("origin_lineage_id", "origin_generation", "published_at")
    }
    if unrelated:
        generation_by_field["origin_lineage_id"] = str(uuid4())
    return generation_by_field


async def _assert_family_present(fixture, owner, names, *, present=True):
    for schema, name in names:
        assert (
            await fixture.connection.fetchval("SELECT to_regclass($1) IS NOT NULL", f'"{schema}"."{name}"') is present
        )
    assert await fixture.connection.fetchval("SELECT to_regnamespace($1) IS NOT NULL", owner.schema_name) is present


async def _cleanup_publication(connection, owner, publication):
    calls = []

    async def is_unreferenced(session, actual_owner, actual_publication):
        assert session.in_transaction()
        assert actual_owner == owner and actual_publication == publication
        calls.append(True)
        return True

    await ownership.cleanup_entity_address_archive_publication(
        NativeReceiptSession(connection), owner=owner, publication=publication, assert_unreferenced=is_unreferenced
    )
    assert calls == [True, True]


@pytest.mark.parametrize("state", ["ready", "current", "ineligible", "demoted"])
async def test_publication_cleanup_keeps_named_network_dependencies(sealed_compact, monkeypatch, state):
    fixture, target, _ = sealed_compact
    monkeypatch.setenv("HLTHPRT_NETWORK_REGISTRY_SCHEMA", fixture.control_schema)
    if state in {"current", "ineligible"}:
        published = await _publish(fixture, target)
        if state == "ineligible":
            await fixture.connection.execute(
                f'UPDATE "{fixture.control_schema}".network_serving_manifest SET eligible=false WHERE generation_id=$1',
                published["generation_id"],
            )
    elif state == "demoted":
        await fixture.connection.execute(
            f'UPDATE "{fixture.control_schema}".network_membership_candidate SET state=$2 WHERE candidate_id=$1',
            UUID(target.candidate_id),
            "rejected",
        )
    owner, publication, names = await _retained_publication(fixture, _address_origin(fixture))
    async with fixture.address.publisher.transaction():
        with pytest.raises(retention.NetworkCompactRetentionError, match="artifact_referenced"):
            await _cleanup_publication(fixture.address.publisher, owner, publication)
    await _assert_family_present(fixture, owner, names)


@pytest.mark.parametrize("legacy", [False, True])
async def test_unreferenced_publication_cleanup_uses_native_permissions(sealed_compact, monkeypatch, legacy):
    fixture, _, _ = sealed_compact
    monkeypatch.setenv("HLTHPRT_NETWORK_REGISTRY_SCHEMA", fixture.control_schema)
    owner, publication, names = await _retained_publication(
        fixture, _address_origin(fixture, unrelated=True), legacy=legacy
    )
    assert await fixture.address.publisher.fetchval("SELECT rolsuper FROM pg_roles WHERE rolname=current_user") is False
    schema, name = names[0]
    with pytest.raises(asyncpg.InsufficientPrivilegeError):
        async with fixture.address.reader.transaction():
            await fixture.address.reader.execute(f'DROP TABLE "{schema}"."{name}" RESTRICT')
    await _assert_family_present(fixture, owner, names)
    async with fixture.address.publisher.transaction():
        await _cleanup_publication(fixture.address.publisher, owner, publication)
    await _assert_family_present(fixture, owner, names, present=False)
    assert await fixture.connection.fetchval(
        "SELECT to_regclass($1) IS NOT NULL", f'"{fixture.address.schema}".entity_address_unified'
    )


async def test_publication_cleanup_caller_rollback_restores_complete_family(sealed_compact, monkeypatch):
    fixture, _, _ = sealed_compact
    monkeypatch.setenv("HLTHPRT_NETWORK_REGISTRY_SCHEMA", fixture.control_schema)
    owner, publication, names = await _retained_publication(fixture, _address_origin(fixture, unrelated=True))
    transaction = fixture.address.publisher.transaction()
    await transaction.start()
    try:
        await _cleanup_publication(fixture.address.publisher, owner, publication)
        for schema, name in names:
            assert not await fixture.address.publisher.fetchval(
                "SELECT to_regclass($1) IS NOT NULL", f'"{schema}"."{name}"'
            )
    finally:
        await transaction.rollback()
    await _assert_family_present(fixture, owner, names)


async def test_publication_cleanup_refuses_missing_legacy_name(sealed_compact, monkeypatch):
    fixture, _, _ = sealed_compact
    monkeypatch.setenv("HLTHPRT_NETWORK_REGISTRY_SCHEMA", fixture.control_schema)
    owner, publication, names = await _retained_publication(
        fixture, _address_origin(fixture, unrelated=True), legacy=True
    )
    schema, name = names[0]
    renamed = name + "_moved"
    await fixture.connection.execute(f'ALTER TABLE "{schema}"."{name}" RENAME TO "{renamed}"')
    async with fixture.address.publisher.transaction():
        with pytest.raises(ownership.EntityAddressArchiveOwnershipError, match="retained catalog differs"):
            await _cleanup_publication(fixture.address.publisher, owner, publication)
    await _assert_family_present(fixture, owner, [(schema, renamed), *names[1:]])


async def _capture_legacy_publication(resources, serving_origin):
    """Serialize the historical v2 receipt before the incumbent rotates."""
    schema = resources.schema
    if not serving_origin:
        await resources.admin.execute(
            f'ALTER TABLE "{schema}".entity_address_result_generation '
            "ALTER COLUMN origin_lineage_id DROP NOT NULL, ALTER COLUMN origin_generation DROP NOT NULL, "
            "ALTER COLUMN published_at DROP NOT NULL"
        )
        await resources.admin.execute(
            f'UPDATE "{schema}".entity_address_result_generation '
            "SET origin_lineage_id=NULL,origin_generation=NULL,published_at=NULL"
        )
    tables = [model.__tablename__ for model in ownership._models()]
    dataset = uuid4()
    archive = ownership.entity_address_archive_stage_schema(dataset)
    await resources.admin.execute(f'CREATE SCHEMA "{archive}" AUTHORIZATION "{resources.roles["owner"]}"')
    await resources.admin.execute(f'CREATE TABLE "{archive}"."{AUTHORITY_TABLE}" (marker integer)')
    await resources.admin.execute(f'ALTER TABLE "{archive}"."{AUTHORITY_TABLE}" OWNER TO "{resources.roles["owner"]}"')
    relation_ids = [
        (table, await resources.admin.fetchval("SELECT to_regclass($1)::oid", f'"{schema}"."{table}"'))
        for table in tables
    ]
    alias_id = await resources.admin.fetchval("SELECT to_regclass($1)::oid", f'"{archive}"."{AUTHORITY_TABLE}"')
    owner = ownership.EntityAddressArchiveStageOwnership(
        dataset,
        archive,
        await resources.admin.fetchval("SELECT to_regnamespace($1)::oid", archive),
        tuple(sorted([*relation_ids, (AUTHORITY_TABLE, alias_id)])),
    )
    async with resources.publisher.transaction():
        before = await preparation._publication_receipt(
            NativeReceiptSession(resources.publisher), schema, owner, SimpleNamespace(context={}), {}
        )
    legacy = before["publication"]
    return owner, legacy


async def _rotate_legacy_publication(resources, monkeypatch):
    """Use the production seven-table swap and retain its exact returned names."""
    schema = resources.schema
    swaps = []
    for table in (model.__tablename__ for model in ownership._models()):
        stage = table + "_replacement"
        await resources.admin.execute(f'CREATE TABLE "{schema}"."{stage}" (LIKE "{schema}"."{table}" INCLUDING ALL)')
        await resources.admin.execute(f'INSERT INTO "{schema}"."{stage}" SELECT * FROM "{schema}"."{table}"')
        await resources.admin.execute(f'ALTER TABLE "{schema}"."{stage}" OWNER TO "{resources.roles["owner"]}"')
        await resources.admin.execute(f'GRANT SELECT ON "{schema}"."{stage}" TO "{resources.roles["reader"]}"')
        swaps.append(
            SimpleNamespace(
                live_cls=SimpleNamespace(__main_table__=table),
                stage_cls=SimpleNamespace(__tablename__=stage, __my_additional_indexes__=[]),
            )
        )
    await resources.admin.execute(
        f'UPDATE "{schema}".entity_address_unified_replacement SET location_key=$1', "new-office"
    )
    monkeypatch.setattr(adoption.entity_address_unified, "db", _NativeDB(resources.publisher))
    monkeypatch.setenv("HLTHPRT_NETWORK_REGISTRY_SCHEMA", "absent_registry")
    context_by_field = {
        "snapshot_contract": "entity_address_unified.postgres.v2",
        "protected_owner_oid": resources.descriptor["protected_owner_oid"],
    }
    async with resources.publisher.transaction():
        await adoption._publish_result_swaps(schema, swaps, context_by_field)
    rotation_by_field = {
        "contract": "entity-address-table-publication.v2",
        "schema_name": schema,
        "retained_relations": context_by_field["retained_relations"],
    }
    names = [(schema, entry["retained_name"]) for entry in context_by_field["retained_relations"]]
    return rotation_by_field, names


@pytest.mark.parametrize("rollback", [False, True])
@pytest.mark.parametrize("serving_origin", [False, True])
async def test_legacy_publication_retires_actual_post_upgrade_rotation(
    address_db, monkeypatch, rollback, serving_origin
):
    """An unchanged legacy receipt retires only its recorded rotated family, with rollback."""
    resources = address_db
    schema = resources.schema
    owner, legacy = await _capture_legacy_publication(resources, serving_origin)
    assert "serving_generation" not in legacy
    for entry in legacy["relations"]:
        entry["relation_oid"] += 100000
    original = deepcopy(legacy)
    rotation_by_field, names = await _rotate_legacy_publication(resources, monkeypatch)
    assert len(names) == 7
    assert await resources.reader.fetchval(f'SELECT location_key FROM "{schema}"."{names[0][1]}"') == "old-office"
    assert (
        await resources.reader.fetchval(f'SELECT location_key FROM "{schema}".entity_address_unified') == "new-office"
    )
    async with resources.publisher.transaction():
        with pytest.raises(ownership.EntityAddressArchiveOwnershipError, match="retained catalog differs"):
            await ownership.cleanup_entity_address_archive_publication(
                NativeReceiptSession(resources.publisher),
                owner=owner,
                publication=legacy,
                assert_unreferenced=_is_unreferenced_legacy,
            )
    transaction = resources.publisher.transaction()
    await transaction.start()
    try:
        await ownership.cleanup_entity_address_archive_publication(
            NativeReceiptSession(resources.publisher),
            owner=owner,
            publication=legacy,
            assert_unreferenced=_is_unreferenced_legacy,
            rotation=rotation_by_field,
            authenticate_rotation=_legacy_rotation_authenticator(owner, original, rotation_by_field),
        )
        assert all(
            [
                await resources.publisher.fetchval(
                    "SELECT to_regclass($1) IS NULL", f'"{retained_schema}"."{retained_table}"'
                )
                for retained_schema, retained_table in names
            ]
        )
    except BaseException:
        await transaction.rollback()
        raise
    else:
        await (transaction.rollback() if rollback else transaction.commit())
    assert legacy == original
    for retained_schema, retained_table in names:
        assert (
            await resources.admin.fetchval(
                "SELECT to_regclass($1) IS NOT NULL", f'"{retained_schema}"."{retained_table}"'
            )
            is rollback
        )
    assert (
        await resources.reader.fetchval(f'SELECT location_key FROM "{schema}".entity_address_unified') == "new-office"
    )


async def _is_unreferenced_legacy(_session, _owner, _publication):
    return True


def _legacy_rotation_authenticator(expected_owner, expected_publication, expected_rotation):
    publication = deepcopy(expected_publication)
    rotation = deepcopy(expected_rotation)

    async def is_expected_rotation(session, owner, actual_publication, actual_rotation):
        return (
            session.in_transaction()
            and owner == expected_owner
            and actual_publication == publication
            and actual_rotation == rotation
        )

    return is_expected_rotation


async def test_legacy_rotation_rejects_another_publications_successor(address_db, monkeypatch):
    owner, legacy = await _capture_legacy_publication(address_db, True)
    rotation_by_field, names = await _rotate_legacy_publication(address_db, monkeypatch)
    authenticate = _legacy_rotation_authenticator(owner, legacy, rotation_by_field)
    foreign = deepcopy(rotation_by_field)
    name = foreign["retained_relations"][0]["retained_name"]
    foreign["retained_relations"][0]["retained_name"] = name[:-1] + ("0" if name[-1] != "0" else "1")
    async with address_db.publisher.transaction():
        with pytest.raises(ownership.EntityAddressArchiveOwnershipError, match="unauthenticated"):
            await ownership.cleanup_entity_address_archive_publication(
                NativeReceiptSession(address_db.publisher),
                owner=owner,
                publication=legacy,
                assert_unreferenced=_is_unreferenced_legacy,
                rotation=foreign,
                authenticate_rotation=authenticate,
            )
    for schema, table in names:
        assert await address_db.admin.fetchval("SELECT to_regclass($1) IS NOT NULL", f'"{schema}"."{table}"')
    assert await address_db.admin.fetchval("SELECT to_regnamespace($1) IS NOT NULL", owner.schema_name)


@pytest.fixture
async def noncompact_cleanup_db(address_db, monkeypatch, request):
    for extension in ("intarray", "btree_gin", "btree_gist"):
        await address_db.admin.execute("CREATE EXTENSION IF NOT EXISTS " + extension)
    monkeypatch.setenv("NETWORK_REGISTRY_TEST_DSN", address_db.url.render_as_string(hide_password=False))
    async with AsyncExitStack() as stack:
        serving = await stack.enter_async_context(asynccontextmanager(serving_schema.__wrapped__)())
        projection = await stack.enter_async_context(asynccontextmanager(projection_db.__wrapped__)(serving))
        validated = await validation_db.__wrapped__(projection)
        if getattr(request, "param", None) == "validated":
            from tests.test_network_membership_validation_postgres import _validate

            async with validated.connection.transaction():
                await _validate(validated)
            fixture = validated
        else:
            fixture = await serving_indexes_db.__wrapped__(validated, SimpleNamespace(param=0))
        fixture.address = address_db
        yield fixture


@pytest.mark.parametrize(
    ("noncompact_cleanup_db", "expected_state"),
    [("validated", "validated"), ("ready", "ready")],
    indirect=["noncompact_cleanup_db"],
)
async def test_validation_only_candidate_keeps_source_and_allows_unrelated_cleanup(
    noncompact_cleanup_db, monkeypatch, expected_state
):
    fixture = noncompact_cleanup_db
    source = fixture.address_source
    query = f'SELECT state,validation_json::text FROM "{fixture.control_schema}".network_membership_candidate WHERE candidate_id=$1'
    original = await fixture.connection.fetchrow(query, UUID(fixture.copy_target.candidate_id))
    assert original["state"] == expected_state
    assert "serving_readiness" not in json.loads(original["validation_json"])
    monkeypatch.setenv("HLTHPRT_NETWORK_REGISTRY_SCHEMA", fixture.control_schema)
    owner, publication, names = await _retained_publication(
        fixture, _address_origin(fixture, unrelated=True), target_schema=source.schema_name
    )
    async with fixture.address.publisher.transaction():
        with pytest.raises(retention.NetworkCompactRetentionError, match="artifact_referenced"):
            await retention.require_named_address_artifacts_unreferenced(
                fixture.address.publisher, [(source.schema_name, source.table_name)]
            )
    async with fixture.address.publisher.transaction():
        await _cleanup_publication(fixture.address.publisher, owner, publication)
    await _assert_family_present(fixture, owner, names, present=False)
    assert await fixture.connection.fetchrow(query, UUID(fixture.copy_target.candidate_id)) == original
    assert await fixture.connection.fetchval(
        "SELECT to_regclass($1) IS NOT NULL", f'"{source.schema_name}"."{source.table_name}"'
    )


async def test_noncompact_publication_allows_unrelated_archive_cleanup(noncompact_cleanup_db, monkeypatch):
    fixture = noncompact_cleanup_db
    address_db = fixture.address
    async with fixture.connection.transaction():
        await prepare_noncompact(fixture)
    async with _candidate_writer_roles(fixture):
        async with fixture.connection.transaction():
            published = await publish_noncompact(fixture)
        assert published["eligible"] is True
        address_source = fixture.address_source
        metadata_sql = f'SELECT validation_json::text FROM "{fixture.control_schema}".network_membership_candidate WHERE candidate_id=$1'
        original = await fixture.connection.fetchval(metadata_sql, UUID(fixture.copy_target.candidate_id))
        assert "address_artifact" not in json.loads(original)["serving_readiness"]
        monkeypatch.setenv("HLTHPRT_NETWORK_REGISTRY_SCHEMA", fixture.control_schema)
        cleanup_fixture = SimpleNamespace(
            connection=fixture.connection, control_schema=fixture.control_schema, address=address_db
        )
        owner, publication, names = await _retained_publication(
            cleanup_fixture, _address_origin(cleanup_fixture, unrelated=True), target_schema=address_source.schema_name
        )
        try:
            async with address_db.publisher.transaction():
                with pytest.raises(retention.NetworkCompactRetentionError, match="artifact_referenced"):
                    await retention.require_named_address_artifacts_unreferenced(
                        address_db.publisher, [(address_source.schema_name, address_source.table_name)]
                    )
            schema, name = names[0]
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                async with address_db.reader.transaction():
                    await address_db.reader.execute(f'DROP TABLE "{schema}"."{name}" RESTRICT')
            assert (
                await address_db.publisher.fetchval("SELECT rolsuper FROM pg_roles WHERE rolname=current_user") is False
            )
            transaction = address_db.publisher.transaction()
            await transaction.start()
            try:
                await _cleanup_publication(address_db.publisher, owner, publication)
            finally:
                await transaction.rollback()
            await _assert_family_present(cleanup_fixture, owner, names)
            async with address_db.publisher.transaction():
                await _cleanup_publication(address_db.publisher, owner, publication)
            await _assert_family_present(cleanup_fixture, owner, names, present=False)
            assert await fixture.connection.fetchval(
                "SELECT to_regclass($1) IS NOT NULL", f'"{address_source.schema_name}"."{address_source.table_name}"'
            )
            assert await fixture.connection.fetchval(metadata_sql, UUID(fixture.copy_target.candidate_id)) == original
        finally:
            for schema, name in names:
                await address_db.admin.execute(f'DROP TABLE IF EXISTS "{schema}"."{name}"')
            await address_db.admin.execute(f'DROP SCHEMA IF EXISTS "{owner.schema_name}"')
