# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Transactional source identity and release evidence on disposable PostgreSQL."""

import asyncio
import importlib.util
import os
import re
import time
import uuid
from pathlib import Path

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import select

from db.models import (
    ProviderDirectoryCMSDoctorsGroupBinding,
    ProviderDirectoryEntityReleaseEvidence,
    ProviderDirectoryEntitySourceBinding,
    ProviderDirectoryOrganizationIdentity,
    ProviderDirectorySiteIdentity,
    db,
)
from process.provider_directory_entity_identity import (
    bind_cms_doctors_group,
    bind_cms_doctors_group_batch,
    bind_entity_batch,
    bind_entity_resource,
)


def _upgrade_identity_tables(connection, schema):
    migration_path = (
        Path(__file__).resolve().parents[1] / "alembic/versions/20260929010000_provider_directory_entity_identity.py"
    )
    spec = importlib.util.spec_from_file_location("provider_directory_entity_identity_migration", migration_path)
    assert spec is not None and spec.loader is not None
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    migration.op = Operations(MigrationContext.configure(connection))
    migration._schema = lambda: schema
    migration.upgrade()


async def _observe(source_id, release_id, resource):
    async with db.session() as session:
        return await bind_entity_resource(
            session,
            source_id=source_id,
            release_id=release_id,
            resource=resource,
        )


async def _observe_cms_group(org_pac_id):
    async with db.session() as session:
        return await bind_cms_doctors_group(session, org_pac_id=org_pac_id)


async def _observe_cms_group_batch(org_pac_ids):
    async with db.session() as session:
        return await bind_cms_doctors_group_batch(session, org_pac_ids=org_pac_ids)


async def _observe_batch(source_id, release_id, resources):
    async with db.session() as session:
        return await bind_entity_batch(
            session,
            source_id=source_id,
            release_id=release_id,
            resources=resources,
        )


def _sample_resources():
    old_organization_by_field = {
        "resourceType": "Organization",
        "id": "1234567890",
        "name": "Example Medical Group",
        "type": [
            {"coding": [{"system": "urn:example:organization-role", "code": "grp", "display": "Medical group"}]},
            {"text": "ntwk"},
            {"text": "unknown-source-role"},
        ],
        "identifier": [
            {"system": "urn:example:npi", "value": "1234567890"},
            {"system": "urn:example:npi", "value": "1098765432"},
        ],
        "partOf": {"reference": "Organization/parent"},
    }
    renamed_organization_by_field = {
        **old_organization_by_field,
        "name": "Example Group Renamed",
        "alias": [old_organization_by_field["name"]],
    }
    homonym_organization_by_field = {**old_organization_by_field, "id": "other-organization"}
    site_by_field = {
        "resourceType": "Location",
        "id": "suite-one",
        "name": "Example Clinic",
        "address": {"line": ["100 Example Avenue", "Suite 1"]},
        "managingOrganization": {"reference": "Organization/1234567890"},
    }
    another_site_by_field = {**site_by_field, "id": "suite-two", "address": {"line": ["100 Example Avenue", "Suite 2"]}}
    return (
        old_organization_by_field,
        renamed_organization_by_field,
        homonym_organization_by_field,
        site_by_field,
        another_site_by_field,
    )


async def _exercise_identity_cases(source_id, suffix, samples):
    old_organization, renamed_organization, homonym_organization, site, another_site = samples
    first_id, concurrent_id = await asyncio.gather(
        _observe(source_id, "release-one", old_organization),
        _observe(source_id, "release-one", old_organization),
    )
    assert first_id == concurrent_id
    assert await _observe(source_id, "release-two", renamed_organization) == first_id
    assert await _observe(source_id, "release-one", homonym_organization) != first_id
    assert await _observe(f"other-{suffix}", "release-one", old_organization) != first_id
    first_site_id = await _observe(source_id, "release-one", site)
    assert await _observe(source_id, "release-two", {**site, "name": "New Clinic Name"}) == first_site_id
    assert await _observe(source_id, "release-one", another_site) != first_site_id
    with pytest.raises(ValueError, match="release_payload_conflict"):
        await _observe(source_id, "release-one", renamed_organization)
    return first_id


async def _assert_release_history(source_id, suffix, old_organization, first_id):
    async with db.session() as session:
        observations = (
            (
                await session.execute(
                    select(ProviderDirectoryEntityReleaseEvidence.__table__).where(
                        ProviderDirectoryEntityReleaseEvidence.source_id == source_id,
                        ProviderDirectoryEntityReleaseEvidence.resource_type == "Organization",
                        ProviderDirectoryEntityReleaseEvidence.resource_id == old_organization["id"],
                    )
                )
            )
            .mappings()
            .all()
        )
        bindings = (
            (
                await session.execute(
                    select(ProviderDirectoryEntitySourceBinding.__table__).where(
                        ProviderDirectoryEntitySourceBinding.source_id.in_((source_id, f"other-{suffix}"))
                    )
                )
            )
            .mappings()
            .all()
        )
    assert len(observations) == 2
    assert {observation["release_id"] for observation in observations} == {"release-one", "release-two"}
    original = next(observation for observation in observations if observation["release_id"] == "release-one")
    assert original["payload_json"]["identifier"] == old_organization["identifier"]
    assert original["payload_json"]["partOf"] == old_organization["partOf"]
    assert original["payload_json"] == old_organization
    assert all(observation["payload_json"]["type"] == old_organization["type"] for observation in observations)
    assert len(bindings) == 5
    assert {binding["organization_id"] for binding in bindings if binding["resource_type"] == "Organization"} >= {
        first_id
    }


async def _assert_cms_doctors_group_binding(fhir_organization_id, suffix):
    pac_id = f"group-{suffix}-one"
    other_pac_id = f"group-{suffix}-two"
    first_id, concurrent_id = await asyncio.gather(_observe_cms_group(pac_id), _observe_cms_group(pac_id))
    assert first_id == concurrent_id
    assert first_id != fhir_organization_id
    assert await _observe_cms_group(pac_id) == first_id
    other_id = await _observe_cms_group(other_pac_id)
    assert other_id != first_id
    for invalid_id in ("", " ", " 1234567890", "1234567890 ", "x" * 65):
        with pytest.raises(ValueError, match="org_pac_id_invalid"):
            await _observe_cms_group(invalid_id)
    async with db.session() as session:
        group_bindings = (
            (
                await session.execute(
                    select(ProviderDirectoryCMSDoctorsGroupBinding.__table__).where(
                        ProviderDirectoryCMSDoctorsGroupBinding.org_pac_id.in_((pac_id, other_pac_id))
                    )
                )
            )
            .mappings()
            .all()
        )
        fhir_bindings = (
            (
                await session.execute(
                    select(ProviderDirectoryEntitySourceBinding.__table__).where(
                        ProviderDirectoryEntitySourceBinding.source_id.in_((f"cms-{suffix}", f"other-{suffix}"))
                    )
                )
            )
            .mappings()
            .all()
        )
    assert {binding["org_pac_id"]: binding["organization_id"] for binding in group_bindings} == {
        pac_id: first_id,
        other_pac_id: other_id,
    }
    assert len(fhir_bindings) == 5


async def _exercise_batched_cms_doctors_groups(fhir_organization_id, suffix):
    org_pac_ids = [f"{suffix}-{index:03d}" for index in range(100)]
    started_at = time.perf_counter()
    first_ids = await _observe_cms_group_batch(org_pac_ids)
    first_seconds = time.perf_counter() - started_at
    assert len(set(first_ids)) == 100
    replay_ids = await _observe_cms_group_batch(org_pac_ids)
    assert replay_ids == first_ids
    assert await _observe_cms_group(org_pac_ids[0]) == first_ids[0]
    assert await _observe_cms_group_batch([org_pac_ids[0], org_pac_ids[0]]) == first_ids[:1] * 2
    concurrent_ids = [f"{suffix}-concurrent-1", f"{suffix}-concurrent-2"]
    first_concurrent, second_concurrent = await asyncio.gather(
        _observe_cms_group_batch(concurrent_ids),
        _observe_cms_group_batch(concurrent_ids),
    )
    assert first_concurrent == second_concurrent
    assert not set(first_ids).intersection(first_concurrent)
    assert fhir_organization_id not in first_ids
    async with db.session() as session:
        group_bindings = (
            await session.execute(
                select(ProviderDirectoryCMSDoctorsGroupBinding.org_pac_id).where(
                    ProviderDirectoryCMSDoctorsGroupBinding.org_pac_id.like(f"{suffix}-%")
                )
            )
        ).all()
        fhir_bindings = (
            await session.execute(
                select(ProviderDirectoryEntitySourceBinding.source_id).where(
                    ProviderDirectoryEntitySourceBinding.source_id.in_((f"cms-{suffix}", f"other-{suffix}"))
                )
            )
        ).all()
    assert len(group_bindings) == 102
    assert len(fhir_bindings) == 5
    print(f"synthetic_cms_doctors_group_batch_100_rows_per_second={100 / first_seconds:.1f}")


async def _exercise_batched_identity_cases(source_id):
    organizations = [
        {"resourceType": "Organization", "id": f"org-{index}", "name": f"Example Group {index}"}
        for index in range(1_000)
    ]
    start = time.perf_counter()
    first_ids = await _observe_batch(source_id, "release-one", organizations)
    first_seconds = time.perf_counter() - start
    assert len(set(first_ids)) == 1_000
    replay_ids, concurrent_ids = await asyncio.gather(
        _observe_batch(source_id, "release-one", organizations),
        _observe_batch(source_id, "release-one", organizations),
    )
    assert replay_ids == concurrent_ids == first_ids
    assert await _observe(source_id, "release-one", organizations[0]) == first_ids[0]
    assert await _observe_batch(source_id, "release-two", [{**organizations[0], "name": "Renamed"}]) == first_ids[:1]
    assert await _observe_batch(source_id, "release-one", [organizations[0], organizations[0]]) == first_ids[:1] * 2
    sites = [{"resourceType": "Location", "id": "site-one"}, {"resourceType": "Location", "id": "site-two"}]
    site_ids = await _observe_batch(source_id, "release-one", sites)
    assert site_ids[0] != site_ids[1]
    with pytest.raises(ValueError, match="release_payload_conflict"):
        await _observe_batch(source_id, "release-one", [{**organizations[0], "name": "Conflicting"}])
    async with db.session() as session:
        evidence_rows = (
            await session.execute(
                select(ProviderDirectoryEntityReleaseEvidence.resource_id).where(
                    ProviderDirectoryEntityReleaseEvidence.source_id == source_id,
                    ProviderDirectoryEntityReleaseEvidence.release_id == "release-one",
                    ProviderDirectoryEntityReleaseEvidence.resource_type == "Organization",
                )
            )
        ).all()
    assert len(evidence_rows) == 1_000
    print(f"synthetic_organization_batch_1000_rows_per_second={1_000 / first_seconds:.1f}")


@pytest.mark.asyncio
async def test_source_scoped_entities_keep_release_history_without_fact_merges():
    """Replay exact IDs and facts; keep homonyms and same-address suites apart."""
    database_name = os.getenv("HLTHPRT_DB_DATABASE", "")
    if not re.fullmatch(r"ptg2_v3_lifecycle_test_[a-z0-9_]{8,}", database_name):
        pytest.skip("requires an explicitly selected disposable PostgreSQL database")
    schema = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    assert re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema)
    await db.connect()
    try:
        await db.execute_ddl(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
        migration_schema = f"pd_entity_identity_{uuid.uuid4().hex}"
        await db.execute_ddl(f'CREATE SCHEMA "{migration_schema}"')
        try:
            async with db.engine.begin() as connection:
                await connection.run_sync(_upgrade_identity_tables, migration_schema)
        finally:
            await db.execute_ddl(f'DROP SCHEMA "{migration_schema}" CASCADE')
        tables = [
            model.__table__
            for model in (
                ProviderDirectoryOrganizationIdentity,
                ProviderDirectorySiteIdentity,
                ProviderDirectoryEntitySourceBinding,
                ProviderDirectoryEntityReleaseEvidence,
                ProviderDirectoryCMSDoctorsGroupBinding,
            )
        ]
        async with db.engine.begin() as connection:
            await connection.run_sync(lambda sync_connection: db.metadata.create_all(sync_connection, tables=tables))
        suffix = uuid.uuid4().hex
        source_id = f"cms-{suffix}"
        samples = _sample_resources()
        first_id = await _exercise_identity_cases(source_id, suffix, samples)
        await _assert_release_history(source_id, suffix, samples[0], first_id)
        await _assert_cms_doctors_group_binding(first_id, suffix)
        await _exercise_batched_cms_doctors_groups(first_id, suffix)
        await _exercise_batched_identity_cases(f"cms-batch-{suffix}")
    finally:
        await db.disconnect()
