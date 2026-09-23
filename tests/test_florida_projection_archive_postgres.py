# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native Florida projection archive on an isolated PostgreSQL database."""

import os
from datetime import datetime
from uuid import uuid4

import pytest
from sqlalchemy import insert, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from db import models
from process import florida_projection_archive as archive
from process.source_profile_result_pins import pin_guard_statements


async def _seed_published_run(session, run_id, when):
    """Seed a published generation with the minimum complete evidence graph."""
    await session.execute(
        insert(models.ProviderProfileImportRun).values(
            run_id=run_id,
            source_key="florida-mqa",
            jurisdiction="FL",
            schema_version="provider-profile/v1",
            status="completed",
            source_manifest={"sources": ["profile_master"]},
            metrics={
                "published_providers": 1,
                "publication": {"publication": "atomic_table_swap", "published_rows": 1},
            },
            started_at=when,
            finished_at=when,
        )
    )
    await _seed_evidence(session, run_id)
    await session.execute(
        insert(models.ProviderProfileProjection).values(
            npi=1234567890,
            generation_id=run_id,
            schema_version="provider-profile/v1",
            profile_json={},
            source_keys=["florida-mqa"],
            published_at=when,
        )
    )


async def _seed_evidence(session, run_id):
    await session.execute(
        insert(models.ProviderProfileArtifact).values(
            artifact_id="a" * 64,
            run_id=run_id,
            source_key="florida-mqa",
            file_name="source.txt",
            source_url="https://example.invalid/source.txt",
            category="profile",
            content_sha256="b" * 64,
            content_bytes=1,
        )
    )
    await session.execute(
        insert(models.ProviderProfileSourceRecord).values(
            record_id="c" * 64,
            run_id=run_id,
            artifact_id="a" * 64,
            source_key="florida-mqa",
            source_record_key="one",
            raw_payload={},
            match_status="matched",
        )
    )
    await session.execute(
        insert(models.ProviderProfileFact).values(
            fact_id="d" * 64,
            run_id=run_id,
            source_record_id="c" * 64,
            logical_fact_key="e" * 64,
            category="license",
            fact_type="license",
            display="Synthetic",
            value_json={},
            assertion_type="source",
            verification_status="source",
            source_json={},
        )
    )


async def _assert_destination_adoption(session, prepared, ownership, run_id):
    await archive.native._create_model_family(
        session,
        archive.native.ReferenceFamilySpec(archive.IMPORTER_ID, archive.MODELS),
        "dest",
    )
    await session.execute(
        text(
            "CREATE TABLE dest.provider_profile_source_pin ("
            "pin_id varchar(36) NOT NULL, source_key varchar(96) NOT NULL, "
            "run_id varchar(64) NOT NULL, purpose varchar(16) NOT NULL, "
            "authority_json json NOT NULL, PRIMARY KEY (pin_id,run_id))"
        )
    )
    for statement in pin_guard_statements("dest"):
        await session.execute(text(statement))
    expected = await archive.current_identity(session, "dest")
    owner_oid = await session.scalar(
        text("SELECT relowner::bigint FROM pg_class WHERE oid=:oid"), {"oid": ownership.relation_oids[0][1]}
    )
    pin_id = uuid4()
    validation = await archive.prepare_activation(
        session,
        prepared=prepared,
        package_id="f" * 64,
        sealed_owner_oid=owner_oid,
        destination_schema="dest",
        expected=expected,
        pin_id=pin_id,
    )
    activation = await archive.activate_validated_result(
        session,
        prepared=prepared,
        destination_schema="dest",
        expected=expected,
        validation=validation,
        package_id="f" * 64,
        sealed_owner_oid=owner_oid,
        pin_id=pin_id,
    )
    assert activation["current"]["run_id"] == run_id
    with pytest.raises(archive.FloridaProjectionArchiveError, match="served generation"):
        await archive.cleanup_adoption(session, schema="dest", run_id=run_id, pin_id=pin_id)


async def _assert_rollback_candidate_retry(session, prepared):
    """Reuse only the unchanged prepared rollback heap and clean its exact OID."""
    ownership = prepared.ownership
    owner_oid = await session.scalar(
        text("SELECT relowner::bigint FROM pg_class WHERE oid=:oid"), {"oid": ownership.relation_oids[0][1]}
    )
    retry_id = uuid4()
    retry_parameters_by_name = {
        "prepared": prepared,
        "destination_schema": "mrf",
        "sealed_owner_oid": owner_oid,
        "cutover_id": retry_id,
    }
    rollback_candidate = await archive.prepare_rollback_cutover(session, **retry_parameters_by_name)
    assert await archive.prepare_rollback_cutover(session, **retry_parameters_by_name) == rollback_candidate
    with pytest.raises(archive.FloridaProjectionArchiveError, match="candidate content differs"):
        async with session.begin_nested():
            await session.execute(
                text(
                    f"UPDATE {archive._table('mrf', rollback_candidate['table_name'])} "
                    "SET profile_json=CAST(:tampered AS json) WHERE npi=1234567890"
                ),
                {"tampered": '{"tampered":true}'},
            )
            await archive.prepare_rollback_cutover(session, **retry_parameters_by_name)
    await archive.cleanup_cutover_candidate(session, schema="mrf", cutover_id=retry_id, seal=rollback_candidate)


async def _assert_source_cutover(session, run_id, dataset_id):
    prepared = await archive.prepare_source(session, schema="mrf", dataset_id=dataset_id)
    ownership = prepared.ownership
    await archive.validate_stage(session, ownership, prepared.manifest)
    await _assert_rollback_candidate_retry(session, prepared)
    incumbent = await archive.current_identity(session, "mrf")
    assert incumbent["run_id"] == run_id
    swapped = await archive._swap_projection(session, "mrf", ownership.schema_name, run_id, uuid4())
    assert swapped["run_id"] == run_id
    assert swapped["relation_oid"] != incumbent["relation_oid"]
    assert await session.scalar(text("SELECT count(*) FROM mrf.provider_profile_projection_old")) == 1
    assert (
        await session.scalar(
            text(
                "SELECT count(*) FROM pg_class c "
                "CROSS JOIN LATERAL aclexplode(c.relacl) a "
                "WHERE c.oid='mrf.provider_profile_projection'::regclass "
                "AND a.grantee IN (0, 'pg_monitor'::regrole::oid) "
                "AND a.privilege_type='SELECT'"
            )
        )
        == 0
    )
    await session.execute(
        text("CREATE VIEW mrf.provider_profile_projection_view AS SELECT npi FROM mrf.provider_profile_projection")
    )
    blocked_pin = uuid4()
    with pytest.raises(archive.FloridaProjectionArchiveError, match="external dependents"):
        async with session.begin_nested():
            await archive._swap_projection(session, "mrf", ownership.schema_name, run_id, blocked_pin)
    assert await archive.current_identity(session, "mrf") == swapped
    assert await archive.native._relation_oid(session, "mrf", f"{archive.PROJECTION}_s_{blocked_pin.hex[:16]}") is None
    assert (
        await archive.release_source_pin(
            session,
            schema="mrf",
            run_id=run_id,
            pin_id=ownership.dataset_id,
        )
        == "released"
    )
    return prepared


async def _drop_test_schemas(engine, schema_names):
    """Remove only schemas created and committed by this test."""
    if not schema_names:
        return
    async with engine.begin() as connection:
        for schema_name in schema_names:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE'))


@pytest.mark.asyncio
async def test_published_projection_stage_and_atomic_swap():
    """Prove native stage, ACL cutover, dependent rejection, and adoption."""
    url = os.getenv("FLORIDA_SNAPSHOT_TEST_DATABASE_URL")
    if not url:
        pytest.skip("isolated PostgreSQL URL not configured")
    engine = create_async_engine(url)
    sessions = async_sessionmaker(engine)
    run_id = uuid4().hex
    dataset_id = uuid4()
    stage_schema_name = archive.stage_schema(dataset_id)
    when = datetime(2026, 1, 1)
    is_source_schema_committed = False
    is_result_schemas_committed = False
    try:
        async with engine.begin() as connection:
            await connection.execute(text("CREATE SCHEMA mrf"))
            for model in archive.MODELS:
                await connection.run_sync(model.__table__.create)
            await connection.run_sync(models.ProviderProfileSourcePin.__table__.create)
            await connection.execute(
                text("ALTER DEFAULT PRIVILEGES IN SCHEMA mrf GRANT SELECT ON TABLES TO PUBLIC, pg_monitor")
            )
            await connection.execute(text("CREATE TABLE mrf.acl_probe (id integer)"))
            assert (
                await connection.scalar(
                    text(
                        "SELECT count(*) FROM pg_class c "
                        "CROSS JOIN LATERAL aclexplode(c.relacl) a "
                        "WHERE c.oid='mrf.acl_probe'::regclass "
                        "AND a.grantee IN (0, 'pg_monitor'::regrole::oid) "
                        "AND a.privilege_type='SELECT'"
                    )
                )
                == 2
            )
            await connection.execute(text("DROP TABLE mrf.acl_probe"))
            for statement in pin_guard_statements("mrf"):
                await connection.execute(text(statement))
        is_source_schema_committed = True
        async with sessions() as session, session.begin():
            assert await archive.current_identity(session, "mrf") == {
                "run_id": None,
                "relation_oid": await archive.native._relation_oid(session, "mrf", archive.PROJECTION),
            }
            await _seed_published_run(session, run_id, when)
        async with sessions() as session, session.begin():
            prepared = await _assert_source_cutover(session, run_id, dataset_id)
            await _assert_destination_adoption(session, prepared, prepared.ownership, run_id)
        is_result_schemas_committed = True
    finally:
        try:
            schema_names = ((stage_schema_name, "dest") if is_result_schemas_committed else ()) + (
                ("mrf",) if is_source_schema_committed else ()
            )
            await _drop_test_schemas(engine, schema_names)
        finally:
            await engine.dispose()
