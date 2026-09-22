# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Reprocessed archives retain native ancestry without changing publication scope."""

import importlib
import json
from datetime import datetime
from uuid import uuid4

import pytest
from sqlalchemy import text

from db import models
from db.connection import Database
from process import massachusetts_profile_store
from process import provider_profile_source_store as shared
from process import source_profile_result_archive as archive
from tests.source_profile_archive_support import _seed
from tests.test_source_profile_result_archive_postgres import _assert_reference_free_read, _prepared_case

IMPORTER = "massachusetts-borim-profile"


async def _retain(case, schema, monkeypatch, tmp_path):
    database = Database(engine=case.engine, session_factory=case.sessions)
    with monkeypatch.context() as patch:
        patch.setattr(shared, "db", database)
        patch.setattr(importlib.import_module("process.florida_mqa_profile"), "db", database)
        for model in (*archive.MODELS, models.ProviderProfileSourcePublication, models.ProviderProfileSourcePin):
            patch.setattr(model.__table__, "schema", schema)
        return await massachusetts_profile_store.retain_source_history(tmp_path)


async def _assert_root_only_activation(case, monkeypatch, tmp_path):
    async with case.sessions() as session, session.begin():
        validation = await archive.prepare_activation(session, **case.activation_by_field)
        assert (await archive._pointer(session, case.destination_schema, IMPORTER))["current_run_id"] == case.incumbent
    async with case.sessions() as session, session.begin():
        await archive.activate_validated_result(session, validation=validation, **case.activation_by_field)
        pins = await archive._pin_group(session, case.destination_schema, case.activation_by_field["pin_id"])
        assert {row["run_id"] for row in pins} == {case.incoming, *case.ancestors}
    await _assert_reference_free_read(case, monkeypatch)
    receipt = await _retain(case, case.destination_schema, monkeypatch, tmp_path)
    assert {case.incoming, *case.ancestors} <= set(receipt["protected_audit_run_ids"])


async def _exercise_shared_ancestry(case):
    prepared = None
    try:
        async with case.sessions() as session, session.begin():
            descendant = await _seed(session, case.source_schema, IMPORTER, parent_run_id=case.incoming)
        async with case.sessions() as session, session.begin():
            prepared = await archive.prepare_source(
                session, importer_id=IMPORTER, schema=case.source_schema, run_id=descendant, dataset_id=uuid4()
            )
        activation_by_field = {
            **case.activation_by_field,
            "prepared": prepared,
            "expected_current_run_id": case.incoming,
            "pin_id": uuid4(),
        }
        async with case.sessions() as session, session.begin():
            await archive.prepare_activation(session, **activation_by_field)
            pins = await archive._pin_group(session, case.destination_schema, activation_by_field["pin_id"])
            assert [pin_row["run_id"] for pin_row in pins if pin_row["authority_json"]["created_here"]] == [descendant]
            assert (
                await archive.cleanup_adoption(
                    session,
                    schema=case.destination_schema,
                    importer_id=IMPORTER,
                    run_id=descendant,
                    pin_id=activation_by_field["pin_id"],
                )
                == "released"
            )
            assert await archive._run(session, case.destination_schema, descendant) is None
            assert await archive._run(session, case.destination_schema, case.incoming) is not None
        async with case.sessions() as session, session.begin():
            validation = await archive.prepare_activation(session, **activation_by_field)
        async with case.sessions() as session, session.begin():
            await archive.activate_validated_result(session, validation=validation, **activation_by_field)
        return descendant
    finally:
        if prepared is not None:
            async with case.sessions() as session, session.begin():
                await archive.cleanup_stage(session, prepared.ownership)
                await archive.release_source_pin(
                    session,
                    schema=case.source_schema,
                    importer_id=IMPORTER,
                    run_id=descendant,
                    pin_id=prepared.ownership.dataset_id,
                )


@pytest.mark.asyncio
async def test_reprocessed_result_adopts_full_ancestry_and_reuses_identical_ancestors(monkeypatch, tmp_path):
    async with _prepared_case(IMPORTER, with_ancestry=True) as case:
        assert case.prepared.manifest["run_ids"] == [case.incoming, *case.ancestors]
        assert [row["row_count"] for row in case.prepared.manifest["tables"]] == [3, 3, 3, 3]
        await _assert_root_only_activation(case, monkeypatch, tmp_path)
        descendant = await _exercise_shared_ancestry(case)

        async def no_content_scan(*_args, **_kwargs):
            raise AssertionError("retained rollback must not rescan the adopted graph")

        monkeypatch.setattr(archive, "_projected_row_identity", no_content_scan)
        async with case.sessions() as session, session.begin():
            await archive.rollback_validated_result(
                session,
                schema=case.destination_schema,
                importer_id=IMPORTER,
                expected_current_run_id=descendant,
                expected_previous_run_id=case.incoming,
                pin_id=case.activation_by_field["pin_id"],
                manifest=case.prepared.manifest,
                package_id=case.activation_by_field["package_id"],
            )
            assert (await archive._pointer(session, case.destination_schema, IMPORTER))[
                "current_run_id"
            ] == case.incoming


async def _reparent(session, schema, child_id, parent_id):
    child_by_field = dict(await archive._run(session, schema, child_id))
    artifact = (
        (
            await session.execute(
                text(f'SELECT artifact_id,content_sha256 FROM "{schema}".provider_profile_artifact WHERE run_id=:run'),
                {"run": parent_id},
            )
        )
        .mappings()
        .one_or_none()
    )
    child_by_field["source_manifest"].update(
        categories=["education", "training", "certifications", "specialties"],
        reprocess_from=parent_id,
        expected_current_run_id=parent_id,
        reprocessing={
            "source_run_id": parent_id,
            "artifact_id": artifact["artifact_id"] if artifact else "a" * 64,
            "manifest_sha256": artifact["content_sha256"] if artifact else "b" * 64,
            "response_envelopes_sha256": "e" * 64,
        },
    )
    await session.execute(
        text(
            f'UPDATE "{schema}".provider_profile_import_run SET source_manifest=CAST(:manifest AS json) WHERE run_id=:run'
        ),
        {"run": child_id, "manifest": json.dumps(child_by_field["source_manifest"])},
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["missing", "cycle", "foreign", "unfinished", "too_deep"])
async def test_source_ancestry_fails_closed(fault, monkeypatch):
    async with _prepared_case(IMPORTER, with_ancestry=True) as case:
        schema = case.prepared.ownership.schema_name
        with monkeypatch.context() as patch, pytest.raises(archive.SourceProfileArchiveError):
            async with case.sessions() as session, session.begin():
                await _corrupt_ancestry(session, case, fault, patch)
                await archive.describe_result(session, importer_id=IMPORTER, schema=schema, run_id=case.incoming)


async def _corrupt_ancestry(session, case, fault, patch):
    schema = case.prepared.ownership.schema_name
    if fault in {"missing", "cycle"}:
        await _reparent(session, schema, case.ancestors[-1], "f" * 32 if fault == "missing" else case.incoming)
    elif fault == "too_deep":
        patch.setattr(archive, "MAX_RUNS", 2)
    else:
        column, value = ("source_key", "other-source") if fault == "foreign" else ("status", "running")
        await session.execute(
            text(f'UPDATE "{schema}".provider_profile_import_run SET {column}=:value WHERE run_id=:run'),
            {"run": case.ancestors[-1], "value": value},
        )


@pytest.mark.asyncio
async def test_retention_closes_ancestry_after_an_old_descendant_is_pinned(monkeypatch, tmp_path):
    async with _prepared_case(IMPORTER, with_ancestry=True) as case:
        async with case.sessions() as session, session.begin():
            await archive.release_source_pin(
                session,
                schema=case.source_schema,
                importer_id=IMPORTER,
                run_id=case.incoming,
                pin_id=case.prepared.ownership.dataset_id,
            )
            await archive.pins.record_pin(
                session,
                schema=case.source_schema,
                source_key=archive.SOURCES[IMPORTER][0],
                run_id=case.incoming,
                pin_id=case.prepared.ownership.dataset_id,
                purpose="export",
                authority={"root_run_id": case.incoming, "run_ids": [case.incoming]},
            )
            await _seed(session, case.source_schema, IMPORTER)
            newest = await _seed(session, case.source_schema, IMPORTER)
            await session.execute(
                text(
                    f'UPDATE "{case.source_schema}".provider_profile_import_run SET started_at=:started WHERE run_id=:run'
                ),
                {"started": datetime(2026, 1, 3), "run": newest},
            )
        receipt = await _retain(case, case.source_schema, monkeypatch, tmp_path)
        assert {case.incoming, *case.ancestors} <= set(receipt["protected_audit_run_ids"])
        async with case.sessions() as session, session.begin():
            await archive.release_source_pin(
                session,
                schema=case.source_schema,
                importer_id=IMPORTER,
                run_id=case.incoming,
                pin_id=case.prepared.ownership.dataset_id,
            )
        receipt = await _retain(case, case.source_schema, monkeypatch, tmp_path)
        assert {case.incoming, *case.ancestors} <= set(receipt["deleted_run_ids"])
