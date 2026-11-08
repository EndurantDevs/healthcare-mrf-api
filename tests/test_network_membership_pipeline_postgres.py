# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Actual sealed-to-serving phases, closed-writer resume and latest-head publication."""

import asyncio
import json
from dataclasses import replace
from types import SimpleNamespace
from uuid import UUID, uuid4

import asyncpg
import pytest

import process.network_membership_pipeline as pipeline
from db.models import EntityAddressUnified
from process.network_membership_publication import NetworkPublicationError
from process.network_membership_writer_closure import verify_network_candidate_writer_closure
from process.network_serving_read import NetworkServingReadUnavailable, resolve_network_serving_manifest
from tests.test_network_address_projection_postgres import projection_db
from tests.test_network_membership_publication_postgres import (
    _candidate_writer_roles,
    _clone_raw_inputs,
    _loader_owned_candidate,
    _sequence_state,
    _snapshot,
)
from tests.test_network_membership_serving_indexes_postgres import _full_source
from tests.test_network_membership_validation_postgres import validation_db
from tests.test_network_serving_schema_postgres import serving_schema


@pytest.fixture
async def pipeline_db(validation_db):
    fixture = validation_db
    await _full_source(fixture)
    await fixture.connection.execute(
        f'ALTER TABLE "{fixture.control_schema}".retained_addresses DROP COLUMN postal_address'
    )
    async with _candidate_writer_roles(fixture, freeze=False):
        yield fixture


def _arguments(fixture):
    return {
        "owner_role": fixture.writer_roles["owner"],
        "loader_roles": (fixture.writer_roles["loader"],),
        "reader_roles": (fixture.writer_roles["reader"],),
        "control_schema": fixture.control_schema,
    }


async def _run(fixture, connection=None, **changes):
    connection = connection or fixture.connection
    await connection.execute(f'SET ROLE "{fixture.writer_roles["publisher"]}"')
    try:
        return await pipeline.prepare_and_publish_network_candidate(
            connection,
            changes.get("copy_target", fixture.copy_target),
            changes.get("source", fixture.address_source),
            **_arguments(fixture),
        )
    finally:
        await connection.execute("RESET ROLE")


async def _source_snapshot(fixture):
    return await fixture.connection.fetch(
        f'SELECT to_jsonb(source) AS address FROM "{fixture.control_schema}".retained_addresses source ORDER BY location_key'
    )


async def _raw_snapshot(fixture):
    namespace = f'"{fixture.copy_target.schema_name}"'
    return [
        await fixture.connection.fetch(f"SELECT * FROM {namespace}.{table} ORDER BY 1,2,3,4")
        for table in ("network_membership", "provider_location_binding")
    ]


async def test_preparation_keeps_serving_head_available_for_final_authorization(pipeline_db):
    """Controller authorization can happen after preparation without exposing the candidate."""
    fixture = pipeline_db
    await fixture.connection.execute(f'SET ROLE "{fixture.writer_roles["publisher"]}"')
    try:
        prepared = await pipeline.prepare_network_candidate(
            fixture.connection, fixture.copy_target, fixture.address_source, **_arguments(fixture)
        )
        assert prepared["state"] == "ready"
        assert (
            await fixture.observer.fetchval(
                f'SELECT generation_id FROM "{fixture.control_schema}".network_serving_control WHERE id=1'
            )
            is None
        )
        assert (
            await fixture.observer.fetchval(f'SELECT count(*) FROM "{fixture.control_schema}".network_serving_manifest')
            == 0
        )
    finally:
        await fixture.connection.execute("RESET ROLE")


async def test_actual_full_pipeline_closes_before_validation(pipeline_db, monkeypatch):
    fixture = pipeline_db
    source_before, raw_before = await _source_snapshot(fixture), await _raw_snapshot(fixture)
    raw_closures = []
    original_validate = pipeline.validate_network_membership_candidate

    async def verify_closed_before_validation(connection, copy_target, source, **kwargs):
        await fixture.observer.execute(f'SET ROLE "{fixture.writer_roles["loader"]}"')
        try:
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await fixture.observer.execute(
                    f'UPDATE "{copy_target.schema_name}".network_membership SET network_id=7'
                )
        finally:
            await fixture.observer.execute("RESET ROLE")
        report = await original_validate(connection, copy_target, source, **kwargs)
        assert set(report["writer_closure"]["relation_oids"]) == {"network_membership", "provider_location_binding"}
        raw_closures.append(report["writer_closure"])
        return report

    monkeypatch.setattr(pipeline, "validate_network_membership_candidate", verify_closed_before_validation)
    manifest = await _run(fixture)
    state = await _snapshot(fixture)
    report = json.loads(state["validation_json"])
    assert state["state"] == "published" and state["generation_id"] == manifest["generation_id"]
    assert report["serving_readiness"]["index_definition_sha256"] != "b" * 64
    assert report["writer_closure"]["relation_oids"].items() >= raw_closures[0]["relation_oids"].items()
    assert "entity_address_unified" in report["writer_closure"]["relation_oids"]
    assert await _source_snapshot(fixture) == source_before and await _raw_snapshot(fixture) == raw_before
    source_columns = await fixture.connection.fetch(
        "SELECT attname FROM pg_attribute WHERE attrelid=to_regclass($1) AND attnum>0 AND NOT attisdropped",
        fixture.control_schema + ".retained_addresses",
    )
    model_columns = set(EntityAddressUnified.__table__.columns.keys())
    assert len(model_columns) == 66
    assert {column["attname"] for column in source_columns} == model_columns
    await fixture.observer.execute(f'SET ROLE "{fixture.writer_roles["reader"]}"')
    try:
        async with fixture.observer.transaction(readonly=True):
            pin = await resolve_network_serving_manifest(fixture.observer, control_schema=fixture.control_schema)
            arrays = await fixture.observer.fetch(
                f'SELECT canonical_network_ids FROM "{pin.schema_name}".entity_address_unified ORDER BY location_key'
            )
        assert [entry["canonical_network_ids"] for entry in arrays] == [[7, 42], [], [88], []]
        assert pin.manifest_sha256 == manifest["manifest_sha256"]
    finally:
        await fixture.observer.execute("RESET ROLE")


@pytest.mark.parametrize("retired", [False, True])
async def test_published_replay_is_readonly_under_active_reader(pipeline_db, retired):
    fixture = pipeline_db
    manifest = await _run(fixture)
    if retired:
        await fixture.connection.execute(
            f'UPDATE "{fixture.control_schema}".network_serving_manifest SET eligible=false'
        )
    before, sequence_before = await _snapshot(fixture), await _sequence_state(fixture)
    queries = []
    fixture.connection.add_query_logger(queries.append)
    async with fixture.observer.transaction(isolation="repeatable_read", readonly=True):
        await fixture.observer.fetch(f'SELECT * FROM "{fixture.copy_target.schema_name}".entity_address_unified')
        await asyncio.sleep(0)
        queries.clear()
        replay = await asyncio.wait_for(_run(fixture), timeout=5)
        await asyncio.sleep(0)
    assert replay == manifest | {"replayed": True, "eligible": not retired}
    assert not any(
        query.query.lstrip().startswith(("LOCK", "ALTER", "CREATE", "INSERT", "UPDATE", "REVOKE", "GRANT"))
        or "FOR UPDATE" in query.query
        for query in queries
    )
    assert await _snapshot(fixture) == before and await _sequence_state(fixture) == sequence_before
    if retired:
        async with fixture.connection.transaction(readonly=True):
            with pytest.raises(NetworkServingReadUnavailable):
                await resolve_network_serving_manifest(fixture.connection, control_schema=fixture.control_schema)


@pytest.mark.parametrize(
    "phase",
    [
        "validate_network_membership_candidate",
        "project_network_address_arrays",
        "prepare_network_candidate_indexes",
        "prepare_network_serving_indexes",
    ],
)
async def test_preparation_failure_retains_closed_raw_candidate(pipeline_db, monkeypatch, phase):
    fixture = pipeline_db
    raw_before, source_before = await _raw_snapshot(fixture), await _source_snapshot(fixture)
    original_stage = getattr(pipeline, phase)

    async def failed_stage(*args, **kwargs):
        await original_stage(*args, **kwargs)
        raise RuntimeError("preparation phase failure")

    monkeypatch.setattr(pipeline, phase, failed_stage)
    with pytest.raises(RuntimeError, match="preparation phase"):
        await _run(fixture)
    state = await _snapshot(fixture)
    closure = json.loads(state["validation_json"])["writer_closure"]
    assert state["state"] == "sealed" and state["generation_id"] is None and state["manifests"] == 0
    assert set(closure["relation_oids"]) == {"network_membership", "provider_location_binding"}
    assert await _raw_snapshot(fixture) == raw_before and await _source_snapshot(fixture) == source_before
    await fixture.connection.execute(f'SET ROLE "{fixture.writer_roles["publisher"]}"')
    try:
        async with fixture.connection.transaction():
            assert (
                await verify_network_candidate_writer_closure(
                    fixture.connection, fixture.copy_target, **_arguments(fixture)
                )
                == closure
            )
    finally:
        await fixture.connection.execute("RESET ROLE")
    monkeypatch.setattr(pipeline, phase, original_stage)
    assert (await _run(fixture))["eligible"] is True


async def test_final_failure_retains_closed_ready_candidate(pipeline_db, monkeypatch):
    fixture = pipeline_db
    original_publish = pipeline.publish_network_candidate

    async def fail_after_head_change(*args, **kwargs):
        await original_publish(*args, **kwargs)
        raise RuntimeError("final publication failure")

    monkeypatch.setattr(pipeline, "publish_network_candidate", fail_after_head_change)
    with pytest.raises(RuntimeError, match="final publication"):
        await _run(fixture)
    state = await _snapshot(fixture)
    report = json.loads(state["validation_json"])
    assert state["state"] == "ready" and state["generation_id"] is None and state["manifests"] == 0
    assert len(report["writer_closure"]["relation_oids"]) == 3 and report["serving_readiness"]["ready"] is True
    reserved = (await _sequence_state(fixture))["last_value"]
    monkeypatch.setattr(pipeline, "publish_network_candidate", original_publish)
    manifest = await _run(fixture)
    assert manifest["generation_id"] > reserved


async def test_approval_updates_are_not_locked_during_preparation(pipeline_db, monkeypatch):
    fixture = pipeline_db
    original_prepare = pipeline.prepare_network_serving_indexes

    async def approve_during_preparation(*args, **kwargs):
        readiness = await original_prepare(*args, **kwargs)
        await asyncio.wait_for(
            fixture.observer.execute(
                f'UPDATE "{fixture.control_schema}".registry_revision_control SET draft_revision=1,approved_revision=1'
            ),
            timeout=2,
        )
        return readiness

    monkeypatch.setattr(pipeline, "prepare_network_serving_indexes", approve_during_preparation)
    with pytest.raises(NetworkPublicationError, match="Approved custom revision changed"):
        await _run(fixture)
    state = await _snapshot(fixture)
    assert state["approved_revision"] == 1 and state["state"] == "ready" and state["generation_id"] is None
    assert state["manifests"] == 0 and len(json.loads(state["validation_json"])["writer_closure"]["relation_oids"]) == 3


async def _raw_successor(fixture):
    candidate_id = uuid4()
    copy_target = replace(
        fixture.copy_target, candidate_id=str(candidate_id), schema_name="network_candidate_" + candidate_id.hex
    )
    fixture.additional_schemas.append(copy_target.schema_name)
    successor = SimpleNamespace(**vars(fixture) | {"copy_target": copy_target})
    await fixture.connection.execute(
        f'INSERT INTO "{fixture.control_schema}".network_membership_candidate '
        "(candidate_id,dataset_id,schema_id,producer_id,schema_name,state,source_generations,"
        "approved_custom_revision,expected_head,expected_rows,accepted_rows) "
        f"SELECT $1,dataset_id,schema_id,producer_id,$2,'sealed',source_generations,0,0,expected_rows,accepted_rows "
        f'FROM "{fixture.control_schema}".network_membership_candidate WHERE candidate_id=$3',
        candidate_id,
        copy_target.schema_name,
        UUID(fixture.copy_target.candidate_id),
    )
    await _clone_raw_inputs(fixture, successor)
    await _loader_owned_candidate(successor)
    return successor


async def test_latest_head_cas_rechecks_after_isolated_preparation(pipeline_db, monkeypatch):
    fixture = pipeline_db
    successor = await _raw_successor(fixture)
    original_prepare = pipeline.prepare_network_serving_indexes
    competing_manifests = []

    async def publish_competitor_during_preparation(*args, **kwargs):
        readiness = await original_prepare(*args, **kwargs)
        if args[1].candidate_id == fixture.copy_target.candidate_id:
            competing_manifests.append(await asyncio.wait_for(_run(successor, fixture.observer), timeout=5))
        return readiness

    monkeypatch.setattr(pipeline, "prepare_network_serving_indexes", publish_competitor_during_preparation)
    with pytest.raises(NetworkPublicationError, match="Expected serving head changed"):
        await _run(fixture)
    state = await _snapshot(fixture)
    assert state["state"] == "ready" and state["manifests"] == 1
    assert state["generation_id"] == competing_manifests[0]["generation_id"]
    assert (await _snapshot(successor))["state"] == "published"


async def test_scope_source_and_phase_transaction_boundaries(pipeline_db):
    fixture = pipeline_db
    async with fixture.connection.transaction():
        with pytest.raises(pipeline.NetworkMembershipPipelineError, match="outside a transaction"):
            await pipeline.prepare_and_publish_network_candidate(
                fixture.connection, fixture.copy_target, fixture.address_source, **_arguments(fixture)
            )
    with pytest.raises(pipeline.NetworkMembershipPipelineError, match="ownership scope"):
        await _run(fixture, copy_target=replace(fixture.copy_target, producer_id=str(uuid4())))
    with pytest.raises(ValueError, match="generation mismatch"):
        await _run(fixture, source=replace(fixture.address_source, generation_id="different-edition"))
    state = await _snapshot(fixture)
    assert state["state"] == "sealed" and state["generation_id"] is None
    assert json.loads(state["validation_json"])["writer_closure"]["component"] == "network_candidate_writer_closure"
