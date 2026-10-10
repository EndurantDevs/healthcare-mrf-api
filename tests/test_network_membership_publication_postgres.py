# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native atomic publication; synthetic full-serving receipt is component proof."""

import asyncio
import json
from contextlib import asynccontextmanager
from dataclasses import FrozenInstanceError, asdict, replace
from types import SimpleNamespace
from uuid import UUID, uuid4

import asyncpg
import pytest

from process.network_address_projection import project_network_address_arrays
from process.network_membership_candidate_indexes import prepare_network_candidate_indexes
from process.network_membership_candidate_lifecycle import _create_raw_membership
from process.network_membership_publication import NetworkPublicationError, publish_network_candidate
from process.network_membership_validation import validate_network_membership_candidate
from process.network_membership_writer_closure import freeze_network_candidate_writers
from process.network_serving_reactivation import (
    NetworkServingReactivationError,
    NetworkServingReactivationReceipt,
    reactivate_network_serving_generation,
)
from process.network_serving_read import resolve_network_serving_manifest
from tests.test_network_address_projection_postgres import projection_db
from tests.test_network_membership_candidate_indexes_postgres import indexed_db
from tests.test_network_membership_validation_postgres import validation_db
from tests.test_network_serving_schema_postgres import serving_schema


@pytest.fixture
async def publication_db(indexed_db):
    fixture = indexed_db
    fixture.additional_schemas = []
    try:
        async with fixture.connection.transaction():
            await prepare_network_candidate_indexes(
                fixture.connection, fixture.copy_target, fixture.address_source, control_schema=fixture.control_schema
            )
            await _synthetic_serving_receipt(fixture)
        async with _candidate_writer_roles(fixture):
            yield fixture
    finally:
        for schema_name in fixture.additional_schemas:
            await fixture.connection.execute(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE')
            assert await fixture.connection.fetchval("SELECT to_regnamespace($1)", schema_name) is None


async def _synthetic_serving_receipt(fixture):
    """Stand in for the separately owned trusted full compatibility gate."""
    report = json.loads(
        await fixture.connection.fetchval(
            f'SELECT validation_json FROM "{fixture.control_schema}".network_membership_candidate WHERE candidate_id=$1',
            UUID(fixture.copy_target.candidate_id),
        )
    )
    report["serving_readiness"] = {
        "component": "unified_address_serving",
        "readiness_revision": 1,
        "ready": True,
        "index_profile": "serving",
        "scope": report["candidate_readiness"]["scope"],
        "index_definition_sha256": "b" * 64,
    }
    await _write_report(fixture, report)


async def _write_report(fixture, report):
    await fixture.connection.execute(
        f'UPDATE "{fixture.control_schema}".network_membership_candidate SET validation_json=$2::jsonb WHERE candidate_id=$1',
        UUID(fixture.copy_target.candidate_id),
        json.dumps(report),
    )


async def _publish(fixture, connection=None, copy_target=None):
    connection = connection or fixture.connection
    if not connection.is_in_transaction():
        return await publish_network_candidate(
            connection, copy_target or fixture.copy_target, control_schema=fixture.control_schema
        )
    publisher = fixture.writer_roles["publisher"]
    original_role = await connection.fetchval("SELECT quote_ident(current_user)")
    await connection.execute(f'SET LOCAL ROLE "{publisher}"')
    try:
        return await publish_network_candidate(
            connection, copy_target or fixture.copy_target, control_schema=fixture.control_schema
        )
    finally:
        await connection.execute(f"SET LOCAL ROLE {original_role}")


@asynccontextmanager
async def _candidate_writer_roles(fixture, *, freeze=True):
    """Prepare exact synthetic publisher roles and optionally freeze the fixture."""
    if hasattr(fixture, "writer_roles"):
        yield fixture
        return
    suffix = uuid4().hex
    role_by_kind = {kind: "nw_" + kind + "_" + suffix for kind in ("owner", "loader", "reader", "publisher")}
    created_roles = []
    original_owner = await fixture.connection.fetchval("SELECT quote_ident(current_user)")
    fixture.additional_schemas = getattr(fixture, "additional_schemas", [])
    try:
        for role_name in role_by_kind.values():
            await fixture.connection.execute(f'CREATE ROLE "{role_name}" NOLOGIN')
            created_roles.append(role_name)
        fixture.writer_roles = role_by_kind
        fixture.read_owner_role = role_by_kind["owner"]
        await _writer_role_grants(fixture)
        if freeze:
            await _freeze_fixture(fixture)
        else:
            await _loader_owned_candidate(fixture)
        yield fixture
    finally:
        await fixture.connection.execute("RESET ROLE")
        await fixture.observer.execute("RESET ROLE")
        await _restore_candidate_owners(fixture, original_owner)
        for role_name in reversed(created_roles):
            await _drop_writer_role(fixture.connection, role_name)
        if hasattr(fixture, "writer_roles"):
            del fixture.writer_roles
        assert not await fixture.connection.fetchval(
            "SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=ANY($1::text[]))", created_roles
        )


async def _drop_writer_role(connection, role_name):
    for attempt in range(3):
        try:
            async with connection.transaction():
                await connection.execute(f'DROP OWNED BY "{role_name}"')
                await connection.execute(f'DROP ROLE "{role_name}"')
        except asyncpg.InternalServerError as error:
            if str(error) != "tuple concurrently updated" or attempt == 2:
                raise
            await asyncio.sleep(0)
        else:
            return


async def _writer_role_grants(fixture):
    connection, roles = fixture.connection, fixture.writer_roles
    database = await connection.fetchval("SELECT quote_ident(current_database())")
    namespace = f'"{fixture.control_schema}"'
    await connection.execute(f'GRANT CREATE ON DATABASE {database} TO "{roles["owner"]}","{roles["publisher"]}"')
    await connection.execute(f'GRANT "{roles["owner"]}","{roles["loader"]}" TO "{roles["publisher"]}"')
    for role_name in (roles["publisher"], roles["loader"], roles["reader"]):
        await connection.execute(f'GRANT USAGE ON SCHEMA {namespace} TO "{role_name}"')
        await connection.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA {namespace} TO "{role_name}"')
    await connection.execute(f'GRANT ALL ON ALL TABLES IN SCHEMA {namespace} TO "{roles["publisher"]}"')
    await connection.execute(f'GRANT ALL ON ALL SEQUENCES IN SCHEMA {namespace} TO "{roles["publisher"]}"')


async def _restore_candidate_owners(fixture, original_owner):
    for schema_name in (fixture.copy_target.schema_name, *fixture.additional_schemas):
        relations = await fixture.connection.fetch(
            "SELECT relname FROM pg_class WHERE relnamespace=to_regnamespace($1) AND relkind='r'", schema_name
        )
        if await fixture.connection.fetchval("SELECT to_regnamespace($1)", schema_name) is None:
            continue
        for relation in relations:
            await fixture.connection.execute(
                f'ALTER TABLE "{schema_name}"."{relation["relname"]}" OWNER TO {original_owner}'
            )
        await fixture.connection.execute(f'ALTER SCHEMA "{schema_name}" OWNER TO {original_owner}')


async def _freeze_fixture(fixture):
    connection, roles = fixture.connection, fixture.writer_roles
    await _loader_owned_candidate(fixture)
    original_role = await connection.fetchval("SELECT quote_ident(current_user)")
    async with connection.transaction():
        await connection.execute(f'SET LOCAL ROLE "{roles["publisher"]}"')
        receipt = await freeze_network_candidate_writers(
            connection,
            fixture.copy_target,
            owner_role=roles["owner"],
            loader_roles=(roles["loader"],),
            reader_roles=(roles["reader"],),
            control_schema=fixture.control_schema,
        )
        await connection.execute(
            f'UPDATE "{fixture.control_schema}".network_membership_candidate '
            "SET validation_json=coalesce(validation_json,'{}'::jsonb)||jsonb_build_object('writer_closure',$1::jsonb) WHERE candidate_id=$2",
            json.dumps(receipt),
            UUID(fixture.copy_target.candidate_id),
        )
        await connection.execute(f"SET LOCAL ROLE {original_role}")
    return receipt


async def _loader_owned_candidate(fixture):
    connection, roles = fixture.connection, fixture.writer_roles
    namespace = f'"{fixture.copy_target.schema_name}"'
    relations = await connection.fetch(
        "SELECT relname FROM pg_class WHERE relnamespace=to_regnamespace($1) AND relkind='r'",
        fixture.copy_target.schema_name,
    )
    await connection.execute(f'ALTER SCHEMA {namespace} OWNER TO "{roles["loader"]}"')
    for relation in relations:
        await connection.execute(f'ALTER TABLE {namespace}."{relation["relname"]}" OWNER TO "{roles["loader"]}"')


async def _snapshot(fixture):
    namespace = f'"{fixture.control_schema}"'
    return await fixture.connection.fetchrow(
        f"""
        SELECT control.generation_id,revision.approved_revision,revision.draft_revision,
            candidate.state,candidate.index_ready,candidate.validation_json,
            (SELECT count(*) FROM {namespace}.network_serving_manifest) AS manifests
        FROM {namespace}.network_serving_control control CROSS JOIN {namespace}.registry_revision_control revision
        CROSS JOIN {namespace}.network_membership_candidate candidate WHERE candidate.candidate_id=$1
    """,
        UUID(fixture.copy_target.candidate_id),
    )


async def _sequence_state(fixture):
    sequence_name = await fixture.connection.fetchval(
        "SELECT pg_get_serial_sequence($1,'generation_id')", f'"{fixture.control_schema}".network_serving_manifest'
    )
    return await fixture.connection.fetchrow(f"SELECT last_value,is_called FROM {sequence_name}")


async def _second_candidate(fixture, *, expected_head=0, approved_revision=0):
    candidate_id = uuid4()
    copy_target = replace(
        fixture.copy_target, candidate_id=str(candidate_id), schema_name="network_candidate_" + candidate_id.hex
    )
    fixture.additional_schemas.append(copy_target.schema_name)
    second_fixture = SimpleNamespace(**vars(fixture) | {"copy_target": copy_target})
    namespace = f'"{fixture.control_schema}"'
    async with fixture.connection.transaction():
        await fixture.connection.execute(
            f"INSERT INTO {namespace}.network_membership_candidate "
            "(candidate_id,dataset_id,schema_id,producer_id,schema_name,state,source_generations,"
            "approved_custom_revision,expected_head,expected_rows,accepted_rows) "
            f"SELECT $1,dataset_id,schema_id,producer_id,$2,'sealed',source_generations,$3,$4,expected_rows,accepted_rows "
            f"FROM {namespace}.network_membership_candidate WHERE candidate_id=$5",
            candidate_id,
            copy_target.schema_name,
            approved_revision,
            expected_head,
            UUID(fixture.copy_target.candidate_id),
        )
        await _clone_raw_inputs(fixture, second_fixture)
        await validate_network_membership_candidate(
            fixture.connection, copy_target, fixture.address_source, control_schema=fixture.control_schema
        )
        await project_network_address_arrays(
            fixture.connection, copy_target, fixture.address_source, control_schema=fixture.control_schema
        )
        await prepare_network_candidate_indexes(
            fixture.connection, copy_target, fixture.address_source, control_schema=fixture.control_schema
        )
        await _synthetic_serving_receipt(second_fixture)
    await _freeze_fixture(second_fixture)
    return second_fixture


async def _clone_raw_inputs(fixture, second_fixture):
    connection = fixture.connection
    original_namespace = f'"{fixture.copy_target.schema_name}"'
    candidate_namespace = f'"{second_fixture.copy_target.schema_name}"'
    await _create_raw_membership(connection, second_fixture.copy_target)
    await connection.execute(
        f"INSERT INTO {candidate_namespace}.network_membership SELECT * FROM {original_namespace}.network_membership"
    )
    await connection.execute(
        f"CREATE TABLE {candidate_namespace}.provider_location_binding (LIKE {original_namespace}.provider_location_binding INCLUDING ALL)"
    )
    await connection.execute(
        f"INSERT INTO {candidate_namespace}.provider_location_binding SELECT * FROM {original_namespace}.provider_location_binding"
    )
    await connection.execute(
        f'INSERT INTO "{fixture.control_schema}".network_membership_batch '
        "(candidate_id,batch_id,row_count,input_sha256,copy_sha256,input_bytes,copy_bytes) "
        f'SELECT $1,batch_id,row_count,input_sha256,copy_sha256,input_bytes,copy_bytes FROM "{fixture.control_schema}".network_membership_batch WHERE candidate_id=$2',
        UUID(second_fixture.copy_target.candidate_id),
        UUID(fixture.copy_target.candidate_id),
    )


@pytest.mark.asyncio
async def test_atomic_manifest_and_exact_replay(publication_db):
    fixture = publication_db
    logged_queries = []
    fixture.connection.add_query_logger(logged_queries.append)
    async with fixture.connection.transaction():
        await asyncio.sleep(0)
        logged_queries.clear()
        manifest = await _publish(fixture)
        await asyncio.sleep(0)
        assert len(logged_queries) < 64
        publication_roundtrips = len(logged_queries)
    snapshot = await _snapshot(fixture)
    sequence_state = await _sequence_state(fixture)
    assert snapshot["generation_id"] == manifest["generation_id"] > 0
    assert snapshot["state"] == "published" and snapshot["manifests"] == 1
    assert manifest["eligible"] is True and manifest["replayed"] is False
    assert manifest["candidate_id"] == fixture.copy_target.candidate_id
    assert manifest["source_generations"] == {"unified_address": fixture.address_source.generation_id}
    async with fixture.connection.transaction():
        await asyncio.sleep(0)
        logged_queries.clear()
        assert await _publish(fixture) == manifest | {"replayed": True}
        await asyncio.sleep(0)
        assert len(logged_queries) < 64
        replay_roundtrips = len(logged_queries)
    assert await _snapshot(fixture) == snapshot and await _sequence_state(fixture) == sequence_state
    print({"publication_driver_roundtrips": publication_roundtrips, "replay_driver_roundtrips": replay_roundtrips})


@pytest.mark.asyncio
async def test_durable_drafts_do_not_block_source_publication(publication_db):
    fixture = publication_db
    await fixture.connection.execute(
        f'UPDATE "{fixture.control_schema}".registry_revision_control SET draft_revision=9'
    )
    async with fixture.connection.transaction():
        manifest = await _publish(fixture)
    assert manifest["approved_custom_revision"] == 0
    assert (await _snapshot(fixture))["draft_revision"] == 9


@pytest.mark.asyncio
@pytest.mark.parametrize("changed_control", ["approved", "head"])
async def test_stale_controls_reject_before_sequence(publication_db, changed_control):
    fixture = publication_db
    if changed_control == "approved":
        await fixture.connection.execute(
            f'UPDATE "{fixture.control_schema}".registry_revision_control SET draft_revision=1,approved_revision=1'
        )
    else:
        competitor = await _second_candidate(fixture)
        async with fixture.connection.transaction():
            await _publish(competitor)
    original_state, sequence_state = await _snapshot(fixture), await _sequence_state(fixture)
    async with fixture.connection.transaction():
        with pytest.raises(NetworkPublicationError, match="revision changed|head changed"):
            await _publish(fixture)
    assert await _snapshot(fixture) == original_state and await _sequence_state(fixture) == sequence_state


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "damage",
    [
        "missing_full",
        "wrong_profile",
        "wrong_full_scope",
        "bool_revision",
        "uppercase_sha",
        "not_ready",
        "canonical_scope",
        "canonical_index",
        "accounting",
        "source_map",
        "published_without_manifest",
    ],
)
async def test_readiness_scope_and_accounting_required(publication_db, damage):
    fixture = publication_db
    report = json.loads((await _snapshot(fixture))["validation_json"])
    _damage_receipt(report, damage)
    await _write_report(fixture, report)
    if damage == "published_without_manifest":
        await fixture.connection.execute(
            f"UPDATE \"{fixture.control_schema}\".network_membership_candidate SET state='published'"
        )
    original_state, sequence_state = await _snapshot(fixture), await _sequence_state(fixture)
    async with fixture.connection.transaction():
        with pytest.raises(NetworkPublicationError):
            await _publish(fixture)
    assert await _snapshot(fixture) == original_state and await _sequence_state(fixture) == sequence_state


def _damage_receipt(report, damage):
    changes_by_damage = {
        "wrong_profile": ("index_profile", "canonical"),
        "wrong_full_scope": ("scope", {}),
        "bool_revision": ("readiness_revision", True),
        "uppercase_sha": ("index_definition_sha256", "A" * 64),
        "not_ready": ("ready", False),
    }
    if damage in changes_by_damage:
        field, value = changes_by_damage[damage]
        report["serving_readiness"][field] = value
        return
    if damage == "missing_full":
        report.pop("serving_readiness")
        return
    if damage == "canonical_scope":
        report["candidate_readiness"]["scope"]["producer_id"] = str(uuid4())
        return
    if damage == "canonical_index":
        report["candidate_readiness"]["index_checks"]["has_canonical_gin"] = False
        return
    if damage == "accounting":
        report["batch_rows"] = 3
        return
    if damage == "source_map":
        report["source_generations"] = {"unified_address": 42}


@pytest.mark.asyncio
async def test_wrong_scope_and_unready_state(publication_db):
    fixture = publication_db
    with pytest.raises(NetworkPublicationError, match="caller-owned"):
        await _publish(fixture)
    async with fixture.connection.transaction():
        with pytest.raises(NetworkPublicationError, match="ownership scope"):
            await _publish(fixture, copy_target=replace(fixture.copy_target, producer_id=str(uuid4())))
    await fixture.connection.execute(
        f"UPDATE \"{fixture.control_schema}\".network_membership_candidate SET state='validated'"
    )
    original_state = await _snapshot(fixture)
    async with fixture.connection.transaction():
        with pytest.raises(NetworkPublicationError, match="fully ready"):
            await _publish(fixture)
    assert await _snapshot(fixture) == original_state


@pytest.mark.asyncio
@pytest.mark.parametrize("same_candidate", [False, True])
async def test_concurrent_publication_has_one_generation(publication_db, same_candidate):
    fixture = publication_db
    competitor = fixture if same_candidate else await _second_candidate(fixture)
    outcomes = await asyncio.gather(
        _committed_publish(fixture, fixture.connection), _committed_publish(competitor, fixture.observer)
    )
    successes = [outcome for outcome in outcomes if type(outcome) is dict]
    assert len(successes) == (2 if same_candidate else 1)
    assert (await _snapshot(fixture))["manifests"] == 1
    if same_candidate:
        assert successes[0]["generation_id"] == successes[1]["generation_id"]
        assert sorted(outcome["replayed"] for outcome in successes) == [False, True]
    else:
        assert sum(isinstance(outcome, NetworkPublicationError) for outcome in outcomes) == 1


async def _committed_publish(fixture, connection):
    try:
        async with connection.transaction():
            return await _publish(fixture, connection)
    except NetworkPublicationError as error:
        return error


@pytest.mark.asyncio
async def test_outer_rollback_and_sequence_never_reused(publication_db):
    fixture = publication_db
    original_state = await _snapshot(fixture)
    with pytest.raises(RuntimeError, match="synthetic rollback"):
        async with fixture.connection.transaction():
            rolled_back_manifest = await _publish(fixture)
            raise RuntimeError("synthetic rollback")
    assert await _snapshot(fixture) == original_state
    async with fixture.connection.transaction():
        committed_manifest = await _publish(fixture)
    assert committed_manifest["generation_id"] > rolled_back_manifest["generation_id"]


class _FailAfterHeadUpdate:
    def __init__(self, connection):
        self.connection = connection

    def __getattr__(self, name):
        return getattr(self.connection, name)

    async def execute(self, query, *parameters):
        if "SET state='published'" in query:
            raise RuntimeError("synthetic failure after head update")
        return await self.connection.execute(query, *parameters)


@pytest.mark.asyncio
async def test_savepoint_rolls_back_insert_and_head_update(publication_db):
    fixture = publication_db
    original_state = await _snapshot(fixture)
    async with fixture.connection.transaction():
        with pytest.raises(RuntimeError, match="after head update"):
            await _publish(fixture, _FailAfterHeadUpdate(fixture.connection))
        assert await _snapshot(fixture) == original_state
        reserved_generation = (await _sequence_state(fixture))["last_value"]
        manifest = await _publish(fixture)
    assert manifest["generation_id"] > reserved_generation


@pytest.mark.asyncio
async def test_concurrent_approved_revision_wins_before_publication(publication_db):
    fixture = publication_db
    sequence_state = await _sequence_state(fixture)
    publisher_task = None
    try:
        async with fixture.observer.transaction():
            await fixture.observer.execute(
                f'UPDATE "{fixture.control_schema}".registry_revision_control SET draft_revision=1,approved_revision=1'
            )
            publisher_task = asyncio.create_task(_committed_publish(fixture, fixture.connection))
            await asyncio.wait_for(_wait_for_publication_lock(fixture), timeout=3)
        outcome = await asyncio.wait_for(publisher_task, timeout=3)
        assert isinstance(outcome, NetworkPublicationError) and "Approved custom revision changed" in str(outcome)
    finally:
        if publisher_task is not None and not publisher_task.done():
            publisher_task.cancel()
            await asyncio.gather(publisher_task, return_exceptions=True)
    snapshot = await _snapshot(fixture)
    assert snapshot["generation_id"] is None and snapshot["manifests"] == 0 and snapshot["state"] == "ready"
    assert await _sequence_state(fixture) == sequence_state


async def _wait_for_publication_lock(fixture):
    while True:
        wait_type = await fixture.observer.fetchval(
            "SELECT wait_event_type FROM pg_stat_activity WHERE pid=$1", fixture.connection.get_server_pid()
        )
        if wait_type == "Lock":
            return
        await asyncio.sleep(0.01)


@pytest.mark.asyncio
async def test_empty_candidate_can_publish(validation_db):
    fixture = validation_db
    await fixture.connection.execute(f'DELETE FROM "{fixture.copy_target.schema_name}".network_membership')
    await fixture.connection.execute(f'DELETE FROM "{fixture.control_schema}".network_membership_batch')
    await fixture.connection.execute(
        f'UPDATE "{fixture.control_schema}".network_membership_candidate SET expected_rows=0,accepted_rows=0'
    )
    async with fixture.connection.transaction():
        await validate_network_membership_candidate(
            fixture.connection, fixture.copy_target, fixture.address_source, control_schema=fixture.control_schema
        )
        await project_network_address_arrays(
            fixture.connection, fixture.copy_target, fixture.address_source, control_schema=fixture.control_schema
        )
        readiness = await prepare_network_candidate_indexes(
            fixture.connection, fixture.copy_target, fixture.address_source, control_schema=fixture.control_schema
        )
        await _synthetic_serving_receipt(fixture)
    async with _candidate_writer_roles(fixture):
        async with fixture.connection.transaction():
            manifest = await _publish(fixture)
    assert readiness["membership_rows"] == 0 and readiness["address_rows"] == 4
    assert manifest["eligible"] is True and (await _snapshot(fixture))["state"] == "published"


@pytest.mark.asyncio
async def test_projection_row_count_does_not_add_publication_queries(publication_db):
    fixture = publication_db
    logged_queries = []
    fixture.connection.add_query_logger(logged_queries.append)
    async with fixture.connection.transaction():
        await asyncio.sleep(0)
        logged_queries.clear()
        first_manifest = await _publish(fixture)
        await asyncio.sleep(0)
        baseline_roundtrips = len(logged_queries)
    await fixture.connection.execute(f"""INSERT INTO "{fixture.control_schema}".retained_addresses
        SELECT lpad(to_hex(sequence_id),64,'0'),'npi','extra-'||sequence_id,'synthetic address','{{42}}'::int[],'{{999}}'::int[]
        FROM generate_series(1,20000) sequence_id""")
    successor = await _second_candidate(fixture, expected_head=first_manifest["generation_id"])
    async with fixture.connection.transaction():
        await asyncio.sleep(0)
        logged_queries.clear()
        await _publish(successor)
        await asyncio.sleep(0)
        assert len(logged_queries) == baseline_roundtrips < 64
    report = json.loads((await _snapshot(successor))["validation_json"])
    assert report["candidate_readiness"]["address_rows"] == 20004


@pytest.mark.parametrize("replayed", [False, True])
@pytest.mark.parametrize(
    "damage", ["missing", "raw_only", "table_acl", "column_acl", "owner_member", "owner_login", "heap_oid"]
)
async def test_publication_verifies_native_final_closure(publication_db, replayed, damage):
    fixture = publication_db
    if replayed:
        async with fixture.connection.transaction():
            await _publish(fixture)
    report = json.loads((await _snapshot(fixture))["validation_json"])
    await _damage_writer_closure(fixture, report, damage)
    await _write_report(fixture, report)
    before, sequence_before = await _snapshot(fixture), await _sequence_state(fixture)
    async with fixture.connection.transaction():
        with pytest.raises(NetworkPublicationError):
            await _publish(fixture)
    assert await _snapshot(fixture) == before
    assert await _sequence_state(fixture) == sequence_before


async def _damage_writer_closure(fixture, report, damage):
    namespace = f'"{fixture.copy_target.schema_name}"'
    if damage == "missing":
        report.pop("writer_closure")
        return
    if damage == "raw_only":
        report["writer_closure"]["relation_oids"].pop("entity_address_unified")
        return
    if damage == "table_acl":
        await fixture.connection.execute(
            f'GRANT UPDATE ON {namespace}.network_membership TO "{fixture.writer_roles["loader"]}"'
        )
        return
    if damage == "column_acl":
        await fixture.connection.execute(f"GRANT UPDATE(network_id) ON {namespace}.network_membership TO PUBLIC")
        return
    if damage == "owner_member":
        await fixture.connection.execute(
            f'GRANT "{fixture.writer_roles["owner"]}" TO "{fixture.writer_roles["reader"]}"'
        )
        return
    if damage == "owner_login":
        await fixture.connection.execute(f'ALTER ROLE "{fixture.writer_roles["owner"]}" LOGIN')
        return
    if damage == "heap_oid":
        await fixture.connection.execute(f"ALTER TABLE {namespace}.entity_address_unified RENAME TO replaced_addresses")
        await fixture.connection.execute(
            f"CREATE TABLE {namespace}.entity_address_unified AS SELECT * FROM {namespace}.replaced_addresses"
        )


async def test_manifest_digest_binds_closure_role_identity(publication_db):
    fixture = publication_db
    async with fixture.connection.transaction():
        manifest = await _publish(fixture)
    report = json.loads((await _snapshot(fixture))["validation_json"])
    report["writer_closure"]["loader_roles"], report["writer_closure"]["reader_roles"] = (
        report["writer_closure"]["reader_roles"],
        report["writer_closure"]["loader_roles"],
    )
    await _write_report(fixture, report)
    before = await _snapshot(fixture)
    async with fixture.connection.transaction():
        with pytest.raises(NetworkPublicationError, match="immutable candidate"):
            await _publish(fixture)
    assert await _snapshot(fixture) == before
    assert before["generation_id"] == manifest["generation_id"]


@pytest.mark.asyncio
async def test_publication_pins_nonzero_approved_revision(publication_db):
    fixture = publication_db
    successor = await _second_candidate(fixture, approved_revision=3)
    await fixture.connection.execute(
        f'UPDATE "{fixture.control_schema}".registry_revision_control SET draft_revision=7,approved_revision=3'
    )
    async with fixture.connection.transaction():
        manifest = await _publish(successor)
    snapshot = await _snapshot(successor)
    assert manifest["approved_custom_revision"] == snapshot["approved_revision"] == 3
    assert snapshot["draft_revision"] == 7 and snapshot["state"] == "published"


@pytest.mark.asyncio
async def test_pinned_reader_retention_and_ineligible_replay(publication_db):
    fixture = publication_db
    async with fixture.connection.transaction():
        original_manifest = await _publish(fixture)
    namespace = f'"{fixture.control_schema}"'
    original_row = await fixture.connection.fetchrow(f"SELECT * FROM {namespace}.network_serving_manifest")
    successor = await _second_candidate(fixture, expected_head=original_manifest["generation_id"])
    async with fixture.observer.transaction(isolation="repeatable_read", readonly=True):
        pinned_head = await fixture.observer.fetchval(f"SELECT generation_id FROM {namespace}.network_serving_control")
        async with fixture.connection.transaction():
            successor_manifest = await _publish(successor)
        assert (
            await fixture.observer.fetchval(f"SELECT generation_id FROM {namespace}.network_serving_control")
            == pinned_head
        )
        assert (
            await fixture.observer.fetchrow(
                f"SELECT * FROM {namespace}.network_serving_manifest WHERE generation_id=$1", pinned_head
            )
            == original_row
        )
    assert (
        await fixture.connection.fetchrow(
            f"SELECT * FROM {namespace}.network_serving_manifest WHERE generation_id=$1", pinned_head
        )
        == original_row
    )
    await fixture.connection.execute(
        f"UPDATE {namespace}.registry_revision_control SET draft_revision=2,approved_revision=2"
    )
    await fixture.connection.execute(
        f"UPDATE {namespace}.network_serving_manifest SET eligible=false WHERE generation_id=$1", pinned_head
    )
    sequence_state = await _sequence_state(fixture)
    async with fixture.connection.transaction():
        assert await _publish(fixture) == original_manifest | {"eligible": False, "replayed": True}
    assert (await _snapshot(fixture))["generation_id"] == successor_manifest["generation_id"]
    assert await _sequence_state(fixture) == sequence_state


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["digest", "candidate_scope", "retained_sources"])
async def test_published_replay_requires_immutable_manifest(publication_db, damage):
    fixture = publication_db
    async with fixture.connection.transaction():
        await _publish(fixture)
    namespace = f'"{fixture.control_schema}"'
    if damage == "digest":
        await fixture.connection.execute(
            f"UPDATE {namespace}.network_serving_manifest SET manifest_sha256=$1", "c" * 64
        )
    elif damage == "retained_sources":
        await fixture.connection.execute(f"UPDATE {namespace}.network_serving_manifest SET source_generations='{{}}'")
    else:
        report = json.loads((await _snapshot(fixture))["validation_json"])
        report["serving_readiness"]["index_definition_sha256"] = "d" * 64
        await _write_report(fixture, report)
    original_state = await _snapshot(fixture)
    async with fixture.connection.transaction():
        with pytest.raises(NetworkPublicationError, match="immutable candidate"):
            await _publish(fixture)
    assert await _snapshot(fixture) == original_state


async def _published_pair(fixture):
    async with fixture.connection.transaction():
        original = await _publish(fixture)
    successor = await _second_candidate(fixture, expected_head=original["generation_id"])
    async with fixture.connection.transaction():
        current = await _publish(successor)
    return original, current


async def _reactivate(fixture, target, head, approved=0, *, connection=None):
    connection = connection or fixture.connection
    original_role = await connection.fetchval("SELECT quote_ident(current_user)")
    await connection.execute(f'SET LOCAL ROLE "{fixture.writer_roles["publisher"]}"')
    try:
        return await reactivate_network_serving_generation(
            connection,
            generation_id=target,
            expected_head=head,
            expected_approved_revision=approved,
            control_schema=fixture.control_schema,
        )
    finally:
        await connection.execute(f"SET LOCAL ROLE {original_role}")


async def test_reactivation_retains_native_reader_pins_and_immutable_receipt(publication_db):
    fixture = publication_db
    original, current = await _published_pair(fixture)
    sequence_before = await _sequence_state(fixture)
    namespace = f'"{fixture.control_schema}"'
    manifests_before = await fixture.connection.fetch(f"SELECT * FROM {namespace}.network_serving_manifest ORDER BY 1")
    await fixture.observer.execute(f'SET ROLE "{fixture.writer_roles["reader"]}"')
    try:
        async with fixture.observer.transaction(isolation="repeatable_read", readonly=True):
            current_pin = await resolve_network_serving_manifest(
                fixture.observer, control_schema=fixture.control_schema
            )
            await fixture.observer.fetchval(
                f'SELECT count(*) FROM "{fixture.copy_target.schema_name}".network_membership'
            )
            async with fixture.connection.transaction():
                receipt = await _reactivate(fixture, original["generation_id"], current["generation_id"])
            assert (
                await resolve_network_serving_manifest(fixture.observer, control_schema=fixture.control_schema)
                == current_pin
            )
            assert current_pin.generation_id == current["generation_id"]
        async with fixture.observer.transaction(readonly=True):
            active = await resolve_network_serving_manifest(fixture.observer, control_schema=fixture.control_schema)
            retained = await resolve_network_serving_manifest(
                fixture.observer, generation_id=current["generation_id"], control_schema=fixture.control_schema
            )
            assert active.generation_id == original["generation_id"] and retained == current_pin
            assert not await fixture.observer.fetchval(
                "SELECT pg_has_role(current_user,$1::name,'SET')", fixture.writer_roles["owner"]
            )
    finally:
        await fixture.observer.execute("RESET ROLE")
    assert asdict(receipt) == {
        "component": "network_serving_reactivation",
        "revision": 1,
        "changed": True,
        "previous_generation_id": current["generation_id"],
        "generation_id": original["generation_id"],
        "approved_custom_revision": 0,
        "manifest_sha256": original["manifest_sha256"],
    }
    with pytest.raises(FrozenInstanceError):
        receipt.generation_id = current["generation_id"]
    assert (
        await fixture.connection.fetch(f"SELECT * FROM {namespace}.network_serving_manifest ORDER BY 1")
        == manifests_before
    )
    assert await _sequence_state(fixture) == sequence_before
    async with fixture.connection.transaction():
        unchanged = await _reactivate(fixture, original["generation_id"], original["generation_id"])
    assert unchanged.changed is False and unchanged.manifest_sha256 == receipt.manifest_sha256


@pytest.mark.parametrize("stale", ["head", "approved", "target_approved"])
async def test_reactivation_rejects_stale_controls_or_older_approval(publication_db, stale):
    fixture = publication_db
    original, current = await _published_pair(fixture)
    expected_head, expected_approved = current["generation_id"], 0
    if stale == "head":
        expected_head = original["generation_id"]
    elif stale == "approved":
        expected_approved = 1
    else:
        await fixture.connection.execute(
            f'UPDATE "{fixture.control_schema}".registry_revision_control SET draft_revision=1,approved_revision=1'
        )
        expected_approved = 1
    before, sequence_before = await _snapshot(fixture), await _sequence_state(fixture)
    async with fixture.connection.transaction():
        with pytest.raises(NetworkServingReactivationError, match="conflict"):
            await _reactivate(fixture, original["generation_id"], expected_head, expected_approved)
        assert await _snapshot(fixture) == before
    assert await _sequence_state(fixture) == sequence_before


@pytest.mark.parametrize(
    "damage",
    [
        "digest",
        "sources",
        "retired",
        "state",
        "table_acl",
        "column_acl",
        "owner_member",
        "owner_login",
        "heap_oid",
        "index_oid",
    ],
)
async def test_reactivation_verifies_retained_native_identity_and_closure(publication_db, damage):
    fixture = publication_db
    original, current = await _published_pair(fixture)
    await _damage_reactivation_target(fixture, original, damage)
    before, sequence_before = await _snapshot(fixture), await _sequence_state(fixture)
    async with fixture.connection.transaction():
        with pytest.raises(NetworkServingReactivationError, match="target_unavailable"):
            await _reactivate(fixture, original["generation_id"], current["generation_id"])
        assert await _snapshot(fixture) == before
    assert await _sequence_state(fixture) == sequence_before


async def _damage_reactivation_target(fixture, manifest, damage):
    namespace = f'"{fixture.control_schema}"'
    changes_by_damage = {
        "digest": "manifest_sha256='" + "0" * 64 + "'",
        "sources": "source_generations='{}'::jsonb",
        "retired": "eligible=false",
    }
    if damage in changes_by_damage:
        await fixture.connection.execute(
            f"UPDATE {namespace}.network_serving_manifest SET {changes_by_damage[damage]} WHERE generation_id=$1",
            manifest["generation_id"],
        )
    elif damage == "state":
        await fixture.connection.execute(
            f"UPDATE {namespace}.network_membership_candidate SET state='ready' WHERE candidate_id=$1",
            UUID(fixture.copy_target.candidate_id),
        )
    elif damage == "index_oid":
        index_definition = await fixture.connection.fetchval(
            "SELECT indexdef FROM pg_indexes WHERE schemaname=$1 ORDER BY indexname LIMIT 1",
            fixture.copy_target.schema_name,
        )
        index_name = await fixture.connection.fetchval(
            "SELECT indexname FROM pg_indexes WHERE schemaname=$1 ORDER BY indexname LIMIT 1",
            fixture.copy_target.schema_name,
        )
        await fixture.connection.execute(f'DROP INDEX "{fixture.copy_target.schema_name}"."{index_name}"')
        await fixture.connection.execute(index_definition)
    else:
        report = json.loads((await _snapshot(fixture))["validation_json"])
        await _damage_writer_closure(fixture, report, damage)


class _FailAfterReactivation:
    def __init__(self, connection, failure):
        self.connection, self.failure = connection, failure

    def __getattr__(self, name):
        return getattr(self.connection, name)

    async def execute(self, query, *parameters):
        result = await self.connection.execute(query, *parameters)
        if "network_serving_control SET generation_id=" in query:
            raise self.failure
        return result


@pytest.mark.parametrize("failure", [RuntimeError("synthetic failure after reactivation"), asyncio.CancelledError()])
async def test_reactivation_savepoint_and_cancellation_restore_head(publication_db, failure):
    fixture = publication_db
    original, current = await _published_pair(fixture)
    before, sequence_before = await _snapshot(fixture), await _sequence_state(fixture)
    async with fixture.connection.transaction():
        with pytest.raises(type(failure)):
            await _reactivate(
                fixture,
                original["generation_id"],
                current["generation_id"],
                connection=_FailAfterReactivation(fixture.connection, failure),
            )
        assert await _snapshot(fixture) == before
        receipt = await _reactivate(fixture, original["generation_id"], current["generation_id"])
        assert receipt.changed is True
    assert await _sequence_state(fixture) == sequence_before


async def test_reactivation_outer_rollback_and_unprivileged_reader(publication_db):
    fixture = publication_db
    original, current = await _published_pair(fixture)
    before = await _snapshot(fixture)
    with pytest.raises(RuntimeError, match="synthetic outer rollback"):
        async with fixture.connection.transaction():
            await _reactivate(fixture, original["generation_id"], current["generation_id"])
            raise RuntimeError("synthetic outer rollback")
    assert await _snapshot(fixture) == before
    async with fixture.connection.transaction():
        await fixture.connection.execute(f'SET LOCAL ROLE "{fixture.writer_roles["reader"]}"')
        with pytest.raises(NetworkServingReactivationError, match="storage_unavailable"):
            await reactivate_network_serving_generation(
                fixture.connection,
                generation_id=original["generation_id"],
                expected_head=current["generation_id"],
                expected_approved_revision=0,
                control_schema=fixture.control_schema,
            )
        assert await fixture.connection.fetchval("SELECT 1") == 1
    assert await _snapshot(fixture) == before


async def test_reactivation_competing_expected_heads_have_one_success(publication_db):
    fixture = publication_db
    original, current = await _published_pair(fixture)

    async def attempt(connection):
        try:
            async with connection.transaction():
                return await _reactivate(
                    fixture, original["generation_id"], current["generation_id"], connection=connection
                )
        except NetworkServingReactivationError as error:
            return error

    outcomes = await asyncio.gather(attempt(fixture.connection), attempt(fixture.observer))
    assert sum(type(outcome) is NetworkServingReactivationReceipt for outcome in outcomes) == 1
    assert (
        sum(
            isinstance(outcome, NetworkServingReactivationError)
            and str(outcome) == "network_reactivation_head_conflict"
            for outcome in outcomes
        )
        == 1
    )


async def test_reactivation_approval_commit_wins_while_waiting(publication_db):
    fixture = publication_db
    original, current = await _published_pair(fixture)
    reactivation_task = None

    async def attempt():
        try:
            async with fixture.connection.transaction():
                return await _reactivate(fixture, original["generation_id"], current["generation_id"])
        except NetworkServingReactivationError as error:
            return error

    try:
        async with fixture.observer.transaction():
            await fixture.observer.execute(
                f'UPDATE "{fixture.control_schema}".registry_revision_control SET draft_revision=1,approved_revision=1'
            )
            reactivation_task = asyncio.create_task(attempt())
            await asyncio.wait_for(_wait_for_publication_lock(fixture), timeout=3)
        outcome = await asyncio.wait_for(reactivation_task, timeout=3)
        assert isinstance(outcome, NetworkServingReactivationError)
        assert str(outcome) == "network_reactivation_approved_conflict"
    finally:
        if reactivation_task is not None and not reactivation_task.done():
            reactivation_task.cancel()
            await asyncio.gather(reactivation_task, return_exceptions=True)
    snapshot = await _snapshot(fixture)
    assert snapshot["generation_id"] == current["generation_id"] and snapshot["approved_revision"] == 1


async def test_reactivation_requires_no_manifest_update_privilege(publication_db):
    fixture = publication_db
    original, current = await _published_pair(fixture)
    namespace = f'"{fixture.control_schema}"'
    publisher = fixture.writer_roles["publisher"]
    await fixture.connection.execute(f'REVOKE ALL ON {namespace}.network_serving_manifest FROM "{publisher}"')
    await fixture.connection.execute(f'GRANT SELECT,INSERT ON {namespace}.network_serving_manifest TO "{publisher}"')
    assert not await fixture.connection.fetchval(
        "SELECT has_table_privilege($1,$2,'UPDATE')", publisher, fixture.control_schema + ".network_serving_manifest"
    )
    async with fixture.connection.transaction():
        receipt = await _reactivate(fixture, original["generation_id"], current["generation_id"])
    assert receipt.changed and receipt.generation_id == original["generation_id"]


async def test_reactivation_unavailable_target_keeps_outer_transaction_usable(publication_db):
    fixture = publication_db
    original, current = await _published_pair(fixture)
    before = await _snapshot(fixture)
    async with fixture.connection.transaction():
        with pytest.raises(NetworkServingReactivationError, match="target_unavailable"):
            await _reactivate(fixture, current["generation_id"] + 1000, current["generation_id"])
        assert await _snapshot(fixture) == before
        assert await fixture.connection.fetchval("SELECT 1") == 1
        receipt = await _reactivate(fixture, original["generation_id"], current["generation_id"])
        assert receipt.changed is True


@pytest.mark.parametrize(
    "field,value",
    [
        ("generation_id", True),
        ("generation_id", 0),
        ("generation_id", 2**63),
        ("expected_head", "1"),
        ("expected_approved_revision", True),
        ("expected_approved_revision", -1),
    ],
)
async def test_reactivation_rejects_noncanonical_revision_input(publication_db, field, value):
    arguments_by_name = {
        "generation_id": 1,
        "expected_head": 1,
        "expected_approved_revision": 0,
        "control_schema": publication_db.control_schema,
    }
    arguments_by_name[field] = value
    before = await _snapshot(publication_db)
    with pytest.raises(NetworkServingReactivationError, match="revision_invalid"):
        await reactivate_network_serving_generation(publication_db.connection, **arguments_by_name)
    assert await _snapshot(publication_db) == before


async def test_reactivation_requires_caller_transaction(publication_db):
    with pytest.raises(NetworkServingReactivationError, match="requires_transaction"):
        await reactivate_network_serving_generation(
            publication_db.connection,
            generation_id=1,
            expected_head=1,
            expected_approved_revision=0,
            control_schema=publication_db.control_schema,
        )
