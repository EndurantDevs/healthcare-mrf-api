# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Ordinary serving peers survive desired inputs and remain fenced through the cut."""

import asyncio
import json
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from process import entity_address_candidate_preparation as address
from process import provider_directory_generic_source_artifacts as artifacts
from process import provider_directory_published_replacement_inputs as inputs
from process import provider_directory_published_source_replacement as replacement
from tests.provider_directory_serving_recovery_postgres_support import ordinary_serving_case
from tests.test_provider_directory_published_replacement_postgres import _markers, _prepare


async def _seed(case):
    database, schema = case.database, case.schema
    await _prepare(database, schema)
    await database.status(
        f"ALTER TABLE {schema}.provider_directory_endpoint_dataset ALTER COLUMN publication_metadata_json "
        "TYPE json USING publication_metadata_json::json"
    )
    await database.status(
        f"UPDATE {schema}.provider_directory_endpoint_dataset SET publication_metadata_json="
        "jsonb_set(publication_metadata_json::jsonb,'{source_ids}','[\"source_primary\"]')::json "
        "WHERE dataset_id='dataset_shared'"
    )
    await database.status(
        f"UPDATE {schema}.provider_directory_endpoint_dataset SET "
        "publication_metadata_summary_json=publication_metadata_json::jsonb,publication_metadata_sha256="
        f"{schema}.provider_directory_endpoint_dataset_admission_metadata_sha256(publication_metadata_json::jsonb,"
        "content_proof_admission_version,content_proof_admission_kind,content_proof_admission_sha256,"
        "content_proof_resource_types) WHERE dataset_id='dataset_shared'"
    )
    await database.status(
        f"UPDATE {schema}.provider_directory_endpoint_dataset SET status='published',dataset_hash=repeat('c',64),"
        "acquisition_root_run_id='peer-root',validated_at=timestamp '2026-01-01',published_at=timestamp '2026-01-01',"
        'publication_metadata_json=\'{"source_ids":["unrelated_legacy_alias"],"unrelated":"initial"}\' '
        "WHERE dataset_id='dataset_unpublished'"
    )
    await database.status(
        f"INSERT INTO {schema}.provider_directory_dataset_resource(dataset_id,resource_type,resource_id,payload_hash,payload_json) "
        "VALUES ('dataset_unpublished','Location','peer-location',repeat('c',64),'{}')"
    )


async def _dormant(case):
    database, schema = case.database, case.schema
    await database.status(f"INSERT INTO {schema}.provider_directory_api_endpoint VALUES ('endpoint_dormant')")
    await database.status(
        f"UPDATE {schema}.provider_directory_source SET endpoint_id='endpoint_dormant' WHERE source_id='source_catalog_only'"
    )
    await database.status(
        f"INSERT INTO {schema}.provider_directory_endpoint_dataset(dataset_id,endpoint_id,status,is_current,resource_count) "
        "VALUES ('dataset_dormant','endpoint_dormant','incomplete',true,0)"
    )


async def test_native_desired_vector_retains_same_npi_peer_without_artifact_eligibility(monkeypatch):
    async with ordinary_serving_case(monkeypatch) as case:
        await _seed(case)
        async with replacement.preparation_scope(case.directory, _markers()) as fence:
            snapshot = await inputs.capture(case.directory, fence)
            assert json.loads(json.dumps(snapshot)) == snapshot and snapshot["version"] == 2
            _assert_closed_snapshot(snapshot)
            incumbent = next(entry for entry in snapshot["serving"] if entry["source_id"] == "source_primary")
            assert incumbent["header"]["dataset_hash"] is None
            assert all(set(entry) == {"source_id", "header"} for entry in snapshot["serving"])
            desired = await _desired_input(case, snapshot, monkeypatch)
            assert [(pin.source_id, pin.dataset_id) for pin in desired.dataset_pins] == [
                ("source_no_current", "dataset_unpublished"),
                ("source_primary", "dataset_shared"),
                ("source_sibling", "dataset_shared"),
            ]
            with pytest.raises(RuntimeError, match="dataset pins changed"):
                await address.assert_dataset_pins(case.schema, replace(desired, ordinary_serving_peer_pins=()))
            await _unrelated_peer_metadata(case)
            with _peer_admission(case, fence, snapshot):
                address.validate_preparation_input(desired)
                token = address._PREPARATION.set(desired)
                try:
                    await address.assert_dataset_pins(case.schema, desired)
                    selected_rows = await case.database.all(
                        address.desired_overlay_ctes(case.schema, affected_group_table="affected")
                        + " SELECT source_id FROM current_overlay ORDER BY source_id"
                    )
                finally:
                    address._PREPARATION.reset(token)
        assert [selected_row[0] for selected_row in selected_rows] == ["source_no_current", "source_primary"]


async def _desired_input(case, snapshot, monkeypatch):
    database, schema = case.database, case.schema
    monkeypatch.setattr(address._native(), "db", database)
    await database.status(
        f"CREATE TABLE {schema}.desired_overlay(source_id text,last_seen_run_id text,resource_type text,resource_id text,npi bigint)"
    )
    await database.status(
        f"INSERT INTO {schema}.desired_overlay VALUES "
        "('source_primary','root-shared','Location','location-1',1234567893),"
        "('source_no_current','peer-root','Location','peer-location',1234567893)"
    )
    await database.status(f"CREATE TABLE {schema}.affected(entity_npi bigint)")
    await database.status(f"INSERT INTO {schema}.affected VALUES (1234567893)")
    return address.ProviderDirectoryAddressPreparationInput(
        inputs.desired_pins(snapshot),
        "desired_overlay",
        await database.scalar(f"SELECT '{schema}.desired_overlay'::regclass::oid::bigint"),
        0,
        ordinary_serving_peer_pins=inputs.peer_pins(snapshot),
    )


def _assert_closed_snapshot(snapshot):
    changed = deepcopy(snapshot)
    changed["version"] = 1
    with pytest.raises(ValueError, match="snapshot_invalid"):
        inputs.validate(changed)
    changed = deepcopy(snapshot)
    changed["serving"][0]["source_member"] = False
    with pytest.raises(ValueError, match="entry_invalid"):
        inputs.validate(changed)
    changed = deepcopy(snapshot)
    changed["serving"][0]["header"]["raw_metadata_sha256"] = "a" * 64
    with pytest.raises(ValueError, match="header_invalid"):
        inputs.validate(changed)
    changed = deepcopy(snapshot)
    changed["publication"]["source_member"] = False
    with pytest.raises(ValueError, match="entry_invalid"):
        inputs.validate(changed)


async def _unrelated_peer_metadata(case):
    """Native JSON can hold unrelated numeric payload that whole-JSONB conversion rejects."""
    await case.database.status(
        f"UPDATE {case.schema}.provider_directory_endpoint_dataset SET publication_metadata_json="
        "CAST(:metadata AS json) WHERE dataset_id='dataset_unpublished'",
        metadata='{"source_ids":[],"unrelated":1e1000000}',
    )


@contextmanager
def _peer_admission(case, fence, snapshot):
    request = SimpleNamespace(
        metrics=_markers(), publish_artifacts_targets={"address_overlay"}, publish_corroboration=False
    )
    admission = artifacts._publication_admission(case.directory, fence, request, {"Location"})
    selected = replace(admission, recovery={"serving_inputs": snapshot, "source": inputs._pin(snapshot["publication"])})
    token = artifacts._ACTIVE.set(selected)
    try:
        yield selected
    finally:
        artifacts._ACTIVE.reset(token)


@pytest.mark.parametrize("damage", ["hash", "root", "validated"])
async def test_native_incomplete_serving_peer_refuses_capture(monkeypatch, damage):
    async with ordinary_serving_case(monkeypatch) as case:
        await _seed(case)
        expressions_by_damage = {
            "hash": "dataset_hash=NULL",
            "root": "acquisition_root_run_id=NULL",
            "validated": "validated_at=NULL",
        }
        await case.database.status(
            f"UPDATE {case.schema}.provider_directory_endpoint_dataset SET {expressions_by_damage[damage]} WHERE dataset_id='dataset_unpublished'"
        )
        async with replacement.preparation_scope(case.directory, _markers()) as fence:
            with pytest.raises(ValueError, match="replacement_inputs_"):
                await inputs.capture(case.directory, fence)


async def test_native_peer_opt_in_requires_original_admission_and_exact_inputs(monkeypatch):
    async with ordinary_serving_case(monkeypatch) as case:
        await _seed(case)
        async with replacement.preparation_scope(case.directory, _markers()) as fence:
            snapshot = await inputs.capture(case.directory, fence)
            desired = await _desired_input(case, snapshot, monkeypatch)
            with pytest.raises(ValueError, match="peer_preparation_required"):
                inputs.validate_peer_preparation(desired)
            with _peer_admission(case, fence, snapshot):
                await _assert_preparation_context(desired, monkeypatch)


async def _assert_preparation_context(desired, monkeypatch):
    inputs.validate_peer_preparation(desired)
    for changed in (
        replace(desired, dataset_pins=desired.dataset_pins[1:]),
        replace(desired, ordinary_serving_peer_pins=desired.ordinary_serving_peer_pins[:-1]),
        replace(desired, ordinary_serving_peer_pins=desired.dataset_pins),
    ):
        with pytest.raises(ValueError, match="peer_preparation_changed"):
            inputs.validate_peer_preparation(changed)
    with monkeypatch.context() as temporary:
        temporary.setattr(address._native(), "db", object())
        with pytest.raises(ValueError, match="peer_preparation_required"):
            inputs.validate_peer_preparation(desired)
    with monkeypatch.context() as temporary:
        temporary.setenv("HLTHPRT_DB_SCHEMA", "changed_schema")
        with pytest.raises(RuntimeError, match="scope_changed"):
            inputs.validate_peer_preparation(desired)

    async def inherited():
        inputs.validate_peer_preparation(desired)

    with pytest.raises(RuntimeError, match="rest_artifact_admission_changed"):
        await asyncio.create_task(inherited())


@pytest.mark.parametrize("phase", ["capture", "final"])
async def test_native_target_membership_remains_required(monkeypatch, phase):
    async with ordinary_serving_case(monkeypatch) as case:
        await _seed(case)
        async with replacement.preparation_scope(case.directory, _markers()) as fence:
            snapshot = await inputs.capture(case.directory, fence)
            await case.database.status(
                f"UPDATE {case.schema}.provider_directory_endpoint_dataset SET publication_metadata_json="
                "jsonb_set(publication_metadata_json::jsonb,'{source_ids}','[]')::json WHERE dataset_id='dataset_shared'"
            )
            operation = (
                inputs.capture(case.directory, fence)
                if phase == "capture"
                else inputs.lock_and_check(case.directory, snapshot)
            )
            async with case.database.transaction():
                with pytest.raises(ValueError, match="replacement_inputs_"):
                    await operation


async def test_native_peer_metadata_is_not_new_publication_authority(monkeypatch):
    async with ordinary_serving_case(monkeypatch) as case:
        await _seed(case)
        async with replacement.preparation_scope(case.directory, _markers()) as fence:
            snapshot = await inputs.capture(case.directory, fence)
        assert "publication_metadata_json" not in inputs._selection_sql(case.directory)[2]
        assert "raw_metadata_sha256" not in replacement._HEADER_FIELDS
        await _unrelated_peer_metadata(case)
        async with case.database.transaction():
            await inputs.lock_and_check(case.directory, snapshot)


async def test_native_scalar_selectors_ignore_inherited_alias_and_header_rows(monkeypatch):
    async with ordinary_serving_case(monkeypatch) as case:
        await _seed(case)
        async with replacement.preparation_scope(case.directory, _markers()) as fence:
            snapshot = await inputs.capture(case.directory, fence)
            headers = await replacement._headers(case.directory, fence, case.database)
            for parent in ("provider_directory_source", "provider_directory_endpoint_dataset"):
                await case.database.status(
                    f"CREATE TABLE {case.schema}.{parent}_child () INHERITS ({case.schema}.{parent})"
                )
                await case.database.status(
                    f"INSERT INTO {case.schema}.{parent}_child SELECT * FROM ONLY {case.schema}.{parent}"
                )
            assert await replacement._headers(case.directory, fence, case.database) == headers
            assert await inputs.capture(case.directory, fence) == snapshot
            async with case.database.transaction():
                await inputs.lock_and_check(case.directory, snapshot)


@pytest.mark.parametrize(
    "damage", ["alias", "removed", "added", "header", "metadata_digest", "admission_digest", "newly_serving"]
)
async def test_native_final_cut_refuses_peer_and_complete_set_drift(monkeypatch, damage):
    async with ordinary_serving_case(monkeypatch) as case:
        await _seed(case)
        await _dormant(case)
        async with replacement.preparation_scope(case.directory, _markers()) as fence:
            snapshot = await inputs.capture(case.directory, fence)
        statements_by_damage = {
            "alias": "UPDATE {schema}.provider_directory_source SET endpoint_id='endpoint_shared' WHERE source_id='source_no_current'",
            "removed": "DELETE FROM {schema}.provider_directory_source WHERE source_id='source_no_current'",
            "added": "INSERT INTO {schema}.provider_directory_source(source_id,endpoint_id,org_name,requires_registration,requires_api_key) VALUES ('new_alias','endpoint_shared','New alias',false,false)",
            "header": "UPDATE {schema}.provider_directory_endpoint_dataset SET acquisition_root_run_id='changed' WHERE dataset_id='dataset_unpublished'",
            "metadata_digest": "UPDATE {schema}.provider_directory_endpoint_dataset SET publication_metadata_sha256=repeat('d',64) WHERE dataset_id='dataset_unpublished'",
            "admission_digest": "UPDATE {schema}.provider_directory_endpoint_dataset SET content_proof_admission_sha256=repeat('d',64) WHERE dataset_id='dataset_unpublished'",
            "newly_serving": "UPDATE {schema}.provider_directory_endpoint_dataset SET status='published',published_at=now() WHERE dataset_id='dataset_dormant'",
        }
        await case.database.status(statements_by_damage[damage].format(schema=case.schema))
        async with case.database.transaction():
            with pytest.raises(ValueError, match="replacement_inputs_serving_changed"):
                await inputs.lock_and_check(case.directory, snapshot)


async def test_native_final_freeze_blocks_phantoms_but_keeps_readers(monkeypatch):
    async with ordinary_serving_case(monkeypatch) as case:
        await _seed(case)
        await _dormant(case)
        async with replacement.preparation_scope(case.directory, _markers()) as fence:
            snapshot = await inputs.capture(case.directory, fence)
        async with case.database.transaction():
            await inputs.lock_and_check(case.directory, snapshot)
            await _assert_reader_continuity(case)
            for statement in (
                f"INSERT INTO {case.schema}.provider_directory_source(source_id,endpoint_id,org_name,requires_registration,requires_api_key) VALUES ('new_alias','endpoint_shared','New alias',false,false)",
                f"UPDATE {case.schema}.provider_directory_endpoint_dataset SET status='published',published_at=now() WHERE dataset_id='dataset_dormant'",
            ):
                await _assert_phantom_blocked(case, statement)


async def _assert_reader_continuity(case):
    async with case.database.engine.connect() as reader, reader.begin():
        await reader.execute(text("SET LOCAL statement_timeout='200ms'"))
        assert await reader.scalar(text(f"SELECT count(*) FROM {case.schema}.provider_directory_source")) == 4
        assert await reader.scalar(text(f"SELECT count(*) FROM {case.schema}.provider_directory_endpoint_dataset")) == 4
        assert (
            await reader.scalar(text(f"SELECT marker FROM {case.schema}.provider_directory_address_overlay")) == "old"
        )


async def _assert_phantom_blocked(case, statement):
    async with case.database.engine.connect() as writer, writer.begin():
        await writer.execute(text("SET LOCAL lock_timeout='100ms'"))
        with pytest.raises(DBAPIError) as failure:
            await writer.execute(text(statement))
        assert failure.value.orig.sqlstate == "55P03"


async def test_native_pointer_checks_follow_forward_and_inverse_without_recapture(monkeypatch):
    async with ordinary_serving_case(monkeypatch) as case:
        await _seed(case)
        async with replacement.preparation_scope(case.directory, _markers()) as fence:
            snapshot = await inputs.capture(case.directory, fence)
            async with case.database.transaction():
                await inputs.lock_and_check(case.directory, snapshot)
                await case.directory._lock_and_verify_artifact_dataset_fence(fence, case.database)
                await case.directory._cutover_provider_directory_artifact_sources(fence)
                await inputs.lock_and_check(case.directory, snapshot, pointer="publication")
        async with case.database.transaction():
            await inputs.lock_and_check(case.directory, snapshot, pointer="publication")
            await case.database.status(
                f"UPDATE {case.schema}.provider_directory_source SET endpoint_id='endpoint_repoint' WHERE source_id='source_primary'"
            )
            await inputs.lock_and_check(case.directory, snapshot, pointer="incumbent")


async def test_native_endpoint_wait_precedes_source_row_lock(monkeypatch):
    async with ordinary_serving_case(monkeypatch) as case:
        await _seed(case)
        async with replacement.preparation_scope(case.directory, _markers()) as fence:
            snapshot = await inputs.capture(case.directory, fence)
        ready = asyncio.Queue()

        async def contend():
            async with case.database.transaction():
                await ready.put(await case.database.scalar("SELECT pg_backend_pid()"))
                await inputs.lock_and_check(case.directory, snapshot)

        task = None
        try:
            async with case.database.engine.connect() as blocker, blocker.begin():
                await blocker.execute(text("SELECT pg_advisory_xact_lock(hashtextextended('endpoint_repoint',0))"))
                task = asyncio.create_task(contend())
                pid = await ready.get()
                await _wait_for_endpoint(blocker, pid)
                await blocker.execute(
                    text(
                        f"SELECT source_id FROM {case.schema}.provider_directory_source WHERE source_id='source_primary' FOR UPDATE NOWAIT"
                    )
                )
            await asyncio.wait_for(task, 3)
        finally:
            if task is not None:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)


async def _wait_for_endpoint(blocker, pid):
    async with asyncio.timeout(3):
        while not await blocker.scalar(
            text("SELECT EXISTS(SELECT FROM pg_locks WHERE pid=:pid AND locktype='advisory' AND NOT granted)"),
            {"pid": pid},
        ):
            await asyncio.sleep(0.01)


def test_persisted_serving_snapshot_is_closed():
    for snapshot in ({}, {"version": 1, "source_id": "source", "publication": None, "serving": []}):
        with pytest.raises(ValueError, match="replacement_inputs_"):
            inputs.validate(deepcopy(snapshot))
