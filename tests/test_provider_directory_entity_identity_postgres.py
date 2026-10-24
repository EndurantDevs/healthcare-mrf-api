# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Transactional source identity and release evidence on disposable PostgreSQL."""

import asyncio
import hashlib
import importlib.util
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import MetaData, select, text
from sqlalchemy.ext.asyncio import async_sessionmaker

from db.models import (
    ProviderDirectoryCMSDoctorsGroupBinding,
    ProviderDirectoryEntityReleaseEvidence,
    ProviderDirectoryEntitySourceBinding,
    ProviderDirectoryInsuranceNetworkIdentity,
    ProviderDirectoryOrganizationIdentity,
    ProviderDirectorySiteIdentity,
)
from process import cms_doctors_identity_candidate as doctors_identity
from process import provider_directory_cms_identity_candidate as cms_identity
from process import provider_directory_generic_source_auxiliary as auxiliary
from process.provider_directory_cms_identity_candidate import CMSIdentityReprepareRequired
from process.provider_directory_entity_identity import (
    bind_cms_doctors_group,
    bind_cms_doctors_group_batch,
    bind_entity_batch,
    bind_entity_resource,
)
from process.provider_directory_generic_candidate import GenericCandidateError
from process.provider_directory_generic_source_policy import bound_publisher, source_copy_session
from process.provider_directory_insurance_network_batch import record_insurance_network_batch
from tests import test_cms_doctors_organizations_postgres as doctors
from tests.cms_doctors_component_postgres_support import doctors_component_template as doctors_component_template
from tests.provider_directory_resource_identity_postgres_support import prepare_resource_candidate

pytestmark = pytest.mark.usefixtures("doctors_component_template")

_RELEASE_ONE = hashlib.sha256(b"release-one").hexdigest()
_RELEASE_TWO = hashlib.sha256(b"release-two").hexdigest()
_BATCH_RELEASE_ONE = hashlib.sha256(b"batch-release-one").hexdigest()
_BATCH_RELEASE_TWO = hashlib.sha256(b"batch-release-two").hexdigest()


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


async def _observe(component, source_id, release_id, resource):
    return (await _observe_batch(component, source_id, release_id, [resource], single=True))[0]


async def _observe_cms_group(staged, org_pac_id):
    async with doctors_identity.doctors_identity_read_scope(staged.fixture.database, staged.ctx):
        return await bind_cms_doctors_group(source_copy_session(), org_pac_id=org_pac_id)


async def _observe_cms_group_batch(staged, org_pac_ids):
    async with doctors_identity.doctors_identity_read_scope(staged.fixture.database, staged.ctx):
        return await bind_cms_doctors_group_batch(source_copy_session(), org_pac_ids=org_pac_ids)


async def _observe_batch(component, source_id, release_id, resources, *, single=False):
    """Publish the closed identity component, repreparing one concurrent loser."""
    for attempt in range(2):
        prepared = await prepare_resource_candidate(
            component,
            [resource["id"] for resource in resources],
            resource_type=resources[0]["resourceType"],
            release_id=release_id,
            resources={resource["id"]: resource for resource in resources},
        )
        try:
            async with (
                prepared.transaction() as session,
                auxiliary.identity_candidate_scope(bound_publisher(), prepared.binding),
            ):
                assert session is source_copy_session()
                if single:
                    ids = [
                        await bind_entity_resource(
                            session, source_id=source_id, release_id=release_id, resource=resources[0]
                        )
                    ]
                else:
                    ids = await bind_entity_batch(
                        session, source_id=source_id, release_id=release_id, resources=resources
                    )
                if resources[0]["resourceType"] == "Organization":
                    await record_insurance_network_batch(
                        session,
                        source_id=source_id,
                        release_id=release_id,
                        resource_type="Organization",
                        resources=resources,
                    )
            await prepared.seal(include_entities=True)
            await prepared.publish(include_entities=True)
            return ids
        except CMSIdentityReprepareRequired:
            if attempt == 1:
                raise
    raise AssertionError("bounded candidate reprepare exhausted")


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


async def _exercise_identity_cases(component, source_id, suffix, samples):
    old_organization, renamed_organization, homonym_organization, site, another_site = samples
    first_id, concurrent_id = await asyncio.gather(
        _observe(component, source_id, _RELEASE_ONE, old_organization),
        _observe(component, source_id, _RELEASE_ONE, old_organization),
    )
    assert first_id == concurrent_id
    assert await _observe(component, source_id, _RELEASE_TWO, renamed_organization) == first_id
    assert await _observe(component, source_id, _RELEASE_ONE, homonym_organization) != first_id
    assert await _foreign_identity(component, f"other-{suffix}", old_organization) != first_id
    first_site_id = await _observe(component, source_id, _RELEASE_ONE, site)
    assert await _observe(component, source_id, _RELEASE_TWO, {**site, "name": "New Clinic Name"}) == first_site_id
    assert await _observe(component, source_id, _RELEASE_ONE, another_site) != first_site_id
    with pytest.raises(GenericCandidateError, match="release_payload_conflict"):
        await _observe(component, source_id, _RELEASE_ONE, renamed_organization)
    return first_id


async def _assert_release_history(component, source_id, suffix, old_organization, first_id):
    async with async_sessionmaker(component.admin_engine)() as session:
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
    assert {observation["release_id"] for observation in observations} == {_RELEASE_ONE, _RELEASE_TWO}
    original = next(observation for observation in observations if observation["release_id"] == _RELEASE_ONE)
    assert original["payload_json"]["identifier"] == old_organization["identifier"]
    assert original["payload_json"]["partOf"] == old_organization["partOf"]
    assert original["payload_json"] == old_organization
    assert all(observation["payload_json"]["type"] == old_organization["type"] for observation in observations)
    assert len(bindings) == 5
    assert {binding["organization_id"] for binding in bindings if binding["resource_type"] == "Organization"} >= {
        first_id
    }


async def _assert_cms_doctors_group_binding(staged, fhir_organization_id, suffix):
    component = staged.fixture.component
    group_table = _prepared_group_table(staged)
    pac_id = f"group-{suffix}-one"
    other_pac_id = f"group-{suffix}-two"
    first_id, concurrent_id = await asyncio.gather(
        _observe_cms_group(staged, pac_id), _observe_cms_group(staged, pac_id)
    )
    assert first_id == concurrent_id
    assert first_id != fhir_organization_id
    assert await _observe_cms_group(staged, pac_id) == first_id
    other_id = await _observe_cms_group(staged, other_pac_id)
    assert other_id != first_id
    for invalid_id in ("", " ", " 1234567890", "1234567890 ", "x" * 65):
        with pytest.raises(ValueError, match="org_pac_id_invalid"):
            await _observe_cms_group(staged, invalid_id)
    async with async_sessionmaker(component.admin_engine)() as session:
        group_bindings = (
            (await session.execute(select(group_table).where(group_table.c.org_pac_id.in_((pac_id, other_pac_id)))))
            .mappings()
            .all()
        )
        fhir_bindings = (
            (
                await session.execute(
                    select(ProviderDirectoryEntitySourceBinding.__table__).where(
                        ProviderDirectoryEntitySourceBinding.source_id.in_(("cms-npd", f"other-{suffix}"))
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


async def _exercise_batched_cms_doctors_groups(staged, fhir_organization_id, suffix):
    component = staged.fixture.component
    group_table = _prepared_group_table(staged)
    org_pac_ids = [f"{suffix}-{index:03d}" for index in range(100)]
    started_at = time.perf_counter()
    first_ids = await _observe_cms_group_batch(staged, org_pac_ids)
    first_seconds = time.perf_counter() - started_at
    assert len(set(first_ids)) == 100
    replay_ids = await _observe_cms_group_batch(staged, org_pac_ids)
    assert replay_ids == first_ids
    assert await _observe_cms_group(staged, org_pac_ids[0]) == first_ids[0]
    assert await _observe_cms_group_batch(staged, [org_pac_ids[0], org_pac_ids[0]]) == first_ids[:1] * 2
    concurrent_ids = [f"{suffix}-concurrent-1", f"{suffix}-concurrent-2"]
    first_concurrent, second_concurrent = await asyncio.gather(
        _observe_cms_group_batch(staged, concurrent_ids),
        _observe_cms_group_batch(staged, concurrent_ids),
    )
    assert first_concurrent == second_concurrent
    assert not set(first_ids).intersection(first_concurrent)
    assert fhir_organization_id not in first_ids
    async with async_sessionmaker(component.admin_engine)() as session:
        group_bindings = (
            await session.execute(select(group_table.c.org_pac_id).where(group_table.c.org_pac_id.like(f"{suffix}-%")))
        ).all()
        fhir_bindings = (
            await session.execute(
                select(ProviderDirectoryEntitySourceBinding.source_id).where(
                    ProviderDirectoryEntitySourceBinding.source_id.in_(("cms-npd", f"other-{suffix}"))
                )
            )
        ).all()
    assert len(group_bindings) == 102
    assert len(fhir_bindings) == 5
    print(f"synthetic_cms_doctors_group_batch_100_rows_per_second={100 / first_seconds:.1f}")


async def _exercise_batched_identity_cases(component, source_id):
    organizations = [
        {"resourceType": "Organization", "id": f"org-{index}", "name": f"Example Group {index}"}
        for index in range(1_000)
    ]
    start = time.perf_counter()
    first_ids = await _observe_batch(component, source_id, _BATCH_RELEASE_ONE, organizations)
    first_seconds = time.perf_counter() - start
    assert len(set(first_ids)) == 1_000
    replay_ids, concurrent_ids = await asyncio.gather(
        _observe_batch(component, source_id, _BATCH_RELEASE_ONE, organizations),
        _observe_batch(component, source_id, _BATCH_RELEASE_ONE, organizations),
    )
    assert replay_ids == concurrent_ids == first_ids
    assert await _observe(component, source_id, _BATCH_RELEASE_ONE, organizations[0]) == first_ids[0]
    assert (
        await _observe_batch(component, source_id, _BATCH_RELEASE_TWO, [{**organizations[0], "name": "Renamed"}])
        == first_ids[:1]
    )
    assert (
        await _observe_batch(component, source_id, _BATCH_RELEASE_ONE, [organizations[0], organizations[0]])
        == first_ids[:1] * 2
    )
    sites = [{"resourceType": "Location", "id": "site-one"}, {"resourceType": "Location", "id": "site-two"}]
    site_ids = await _observe_batch(component, source_id, _BATCH_RELEASE_ONE, sites)
    assert site_ids[0] != site_ids[1]
    with pytest.raises(GenericCandidateError, match="release_payload_conflict"):
        await _observe_batch(component, source_id, _BATCH_RELEASE_ONE, [{**organizations[0], "name": "Conflicting"}])
    async with async_sessionmaker(component.admin_engine)() as session:
        evidence_rows = (
            await session.execute(
                select(ProviderDirectoryEntityReleaseEvidence.resource_id).where(
                    ProviderDirectoryEntityReleaseEvidence.source_id == source_id,
                    ProviderDirectoryEntityReleaseEvidence.release_id == _BATCH_RELEASE_ONE,
                    ProviderDirectoryEntityReleaseEvidence.resource_type == "Organization",
                )
            )
        ).all()
    assert len(evidence_rows) == 1_000
    print(f"synthetic_organization_batch_1000_rows_per_second={1_000 / first_seconds:.1f}")


async def _foreign_identity(component, source_id, resource):
    async with async_sessionmaker(component.admin_engine)() as session:
        entity_id = await session.scalar(
            select(ProviderDirectoryEntitySourceBinding.organization_id).where(
                ProviderDirectoryEntitySourceBinding.source_id == source_id,
                ProviderDirectoryEntitySourceBinding.resource_type == "Organization",
                ProviderDirectoryEntitySourceBinding.resource_id == resource["id"],
            )
        )
    assert entity_id is not None
    return entity_id


async def _seed_foreign_identity(component, source_id, resource):
    """An independent accepted incumbent must never acquire the CMS source's ID."""
    entity_id, observed = uuid.uuid4(), datetime.now(timezone.utc)
    async with component.admin_engine.begin() as connection:
        await connection.execute(
            ProviderDirectoryOrganizationIdentity.__table__.insert().values(
                organization_id=entity_id,
                created_at=observed,
            )
        )
        await connection.execute(
            ProviderDirectoryEntitySourceBinding.__table__.insert().values(
                source_id=source_id,
                resource_type="Organization",
                resource_id=resource["id"],
                organization_id=entity_id,
                created_at=observed,
            )
        )


def _prepared_group_table(staged):
    candidate = staged.ctx["context"][doctors_identity._CONTEXT_KEY]["group"]
    return ProviderDirectoryCMSDoctorsGroupBinding.__table__.to_metadata(
        MetaData(),
        schema=staged.schema,
        name=candidate.table["name"],
    )


async def _prepared_doctors(fixture, suffix):
    keys = [f"group-{suffix}-one", f"group-{suffix}-two"]
    keys += [f"{suffix}-{index:03d}" for index in range(100)]
    keys += [f"{suffix}-concurrent-1", f"{suffix}-concurrent-2"]
    staged = await doctors._stage_rows(fixture, keys, [f"site-{index}" for index in range(len(keys))])
    await doctors._prepare_groups(staged)
    await doctors._prepare_sites(staged)
    for model in doctors.preparation._models():
        stage = doctors.make_class(model, staged.import_date)
        await fixture.database.status(f'ALTER TABLE "{staged.schema}"."{stage.__tablename__}" SET LOGGED')
    return staged


async def _assert_identity_migration(component):
    schema = "pd_entity_identity_" + uuid.uuid4().hex
    async with component.admin_engine.begin() as connection:
        await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
        try:
            await connection.run_sync(_upgrade_identity_tables, schema)
        finally:
            await connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))


@pytest.mark.asyncio
async def test_source_scoped_entities_keep_release_history_without_fact_merges(monkeypatch):
    """Real protected candidates retain exact facts, opaque IDs and source isolation."""
    async with doctors._component(monkeypatch) as fixture:
        component = fixture.component
        await _assert_identity_migration(component)
        suffix = uuid.uuid4().hex
        source_id = "cms-npd"
        samples = _sample_resources()
        await _seed_foreign_identity(component, f"other-{suffix}", samples[0])
        first_id = await _exercise_identity_cases(component, source_id, suffix, samples)
        await _assert_release_history(component, source_id, suffix, samples[0], first_id)
        staged = await _prepared_doctors(fixture, suffix)
        try:
            await _assert_cms_doctors_group_binding(staged, first_id, suffix)
            await _exercise_batched_cms_doctors_groups(staged, first_id, suffix)
            prepared_bindings = await doctors._bindings(staged, "group")
            await doctors._publish(staged)
            assert await doctors._bindings(staged, "group", published=True) == prepared_bindings
            assert len(prepared_bindings) == 104
        finally:
            await doctors_identity.discard_doctors_identities(fixture.database, staged.ctx)
            async with fixture.database.transaction() as session:
                inventory = await doctors.preparation._stage_inventory(session, staged.schema, staged.import_date)
            await doctors.preparation._drop_owned_stages(staged.schema, inventory)
        await _exercise_batched_identity_cases(component, source_id)


async def _staged_entity(component, resource):
    prepared = await prepare_resource_candidate(
        component,
        [resource["id"]],
        resource_type=resource["resourceType"],
        release_id=_RELEASE_ONE,
        resources={resource["id"]: resource},
    )
    async with (
        prepared.transaction() as session,
        auxiliary.identity_candidate_scope(bound_publisher(), prepared.binding),
    ):
        ids = await bind_entity_batch(session, source_id="cms-npd", release_id=_RELEASE_ONE, resources=[resource])
        if resource["resourceType"] == "Organization":
            await record_insurance_network_batch(
                session,
                source_id="cms-npd",
                release_id=_RELEASE_ONE,
                resource_type="Organization",
                resources=[resource],
            )
    await prepared.seal(include_entities=True)
    return prepared, ids[0]


async def _identity_rows(component):
    async with component.admin_engine.connect() as connection:
        return [
            (await connection.execute(select(model.__table__))).all()
            for model in (
                ProviderDirectoryEntitySourceBinding,
                ProviderDirectoryEntityReleaseEvidence,
                ProviderDirectoryOrganizationIdentity,
                ProviderDirectorySiteIdentity,
                ProviderDirectoryInsuranceNetworkIdentity,
            )
        ]


async def _commit_between_replay_and_authority(monkeypatch, winner, contender):
    original = cms_identity._global_replay
    replayed, committed = asyncio.Event(), asyncio.Event()
    contender_task = None

    async def replay(*args, **kwargs):
        await original(*args, **kwargs)
        if asyncio.current_task() is contender_task and not replayed.is_set():
            replayed.set()
            await committed.wait()

    monkeypatch.setattr(cms_identity, "_global_replay", replay)
    contender_task = asyncio.create_task(contender.publish(include_entities=True))
    try:
        await asyncio.wait_for(replayed.wait(), 5)
        await winner.publish(include_entities=True)
        accepted = await _identity_rows(winner.component)
        committed.set()
        with pytest.raises(CMSIdentityReprepareRequired, match="whole_candidate_reprepare"):
            await asyncio.wait_for(contender_task, 5)
        return accepted
    finally:
        committed.set()
        if not contender_task.done():
            contender_task.cancel()
        await asyncio.wait_for(asyncio.gather(contender_task, return_exceptions=True), 5)


@pytest.mark.asyncio
@pytest.mark.parametrize("resource_type", ["Organization", "Location"])
async def test_committed_winner_between_replay_and_authority_requires_whole_reprepare(monkeypatch, resource_type):
    async with doctors._component(monkeypatch) as fixture:
        component = fixture.component
        resource_by_field = {"resourceType": resource_type, "id": "concurrent-entity", "name": "Example entity"}
        winner, winner_id = await _staged_entity(component, resource_by_field)
        contender, contender_id = await _staged_entity(component, resource_by_field)
        assert winner_id != contender_id
        accepted = await _commit_between_replay_and_authority(monkeypatch, winner, contender)
        assert await _identity_rows(component) == accepted
        retry, retry_id = await _staged_entity(component, resource_by_field)
        assert retry_id == winner_id
        await retry.publish(include_entities=True)
        assert await _identity_rows(component) == accepted
