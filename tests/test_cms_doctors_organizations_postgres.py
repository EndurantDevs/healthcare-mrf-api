# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded, replay-safe CMS group preparation on explicitly disposable PostgreSQL."""

import importlib
import os
import re
from datetime import datetime
from types import SimpleNamespace
from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy import select, text
from sqlalchemy.exc import DBAPIError

from db import models
from process import cms_doctors_organizations as organizations
from process.control_cancel import ImportCancelledError
from process.ext.utils import make_class
from process.provider_directory_entity_identity import bind_entity_resource
from tests.reference_family_generation_fixture import generation_shape_check

cms_doctors = importlib.import_module("process.cms_doctors")


async def _create_identity_schema(schema):
    await models.db.execute_ddl(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
    tables = [
        model.__table__
        for model in (
            models.ProviderDirectoryOrganizationIdentity,
            models.ProviderDirectorySiteIdentity,
            models.ProviderDirectoryEntitySourceBinding,
            models.ProviderDirectoryEntityReleaseEvidence,
            models.ProviderDirectoryCMSDoctorsGroupBinding,
        )
    ]
    async with models.db.engine.begin() as connection:
        await connection.run_sync(lambda sync_connection: models.db.metadata.create_all(sync_connection, tables=tables))


@pytest_asyncio.fixture
async def staged_groups():
    database_name = os.getenv("HLTHPRT_DB_DATABASE", "")
    if not re.fullmatch(r"ptg2_v3_lifecycle_test_[a-z0-9_]{8,}", database_name):
        pytest.skip("requires an explicitly selected disposable PostgreSQL database")
    schema = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    assert re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema)
    import_date = uuid4().hex
    stage = make_class(models.CMSDoctorGroupSite, import_date)
    org_pac_ids = [f"{import_date[:8]}-{index:04d}" for index in range(205)] + ["0012345678", "12345678"]
    physical_ids = org_pac_ids + org_pac_ids[:5] + [None, ""]
    receipt_by_field = {"source_rows": len(physical_ids), "generation_id": "a" * 64}
    context_by_key = {"import_date": import_date, "context": {"group_site_stage_owned": True}}
    await models.db.connect()
    try:
        await _create_identity_schema(schema)
        await models.db.create_table(stage.__table__)
        await cms_doctors._create_stage_indexes(stage, schema)
        async with models.db.session() as session:
            await session.execute(
                stage.__table__.insert(),
                [
                    {
                        "row_number": index,
                        "npi": 1000000004,
                        "org_pac_id": pac_id,
                        "generation_id": receipt_by_field["generation_id"],
                        "source_json": {},
                        "observed_at": datetime(2026, 9, 1),
                    }
                    for index, pac_id in enumerate(physical_ids, 1)
                ],
            )
        yield SimpleNamespace(
            schema=schema,
            import_date=import_date,
            stage=stage,
            ids=org_pac_ids,
            ctx=context_by_key,
            receipt=receipt_by_field,
        )
    finally:
        await models.db.execute_ddl(f'DROP TABLE IF EXISTS "{schema}"."{stage.__tablename__}"')
        await models.db.disconnect()


async def _prepare_groups(staged):
    return await organizations.bind_group_site_organizations(
        staged.ctx,
        staged.import_date,
        staged.schema,
        staged.receipt,
    )


async def _bound_groups(staged):
    binding = models.ProviderDirectoryCMSDoctorsGroupBinding.__table__
    async with models.db.session() as session:
        return dict(
            (
                await session.execute(
                    select(binding.c.org_pac_id, binding.c.organization_id).where(
                        binding.c.org_pac_id.in_(staged.ids),
                    )
                )
            ).all()
        )


@pytest.mark.asyncio
async def test_distinct_groups_bind_in_bounded_batches_without_fhir_links(staged_groups, monkeypatch):
    """Preserve exact PAC strings and replay IDs across every stage keyset page."""
    staged = staged_groups
    async with models.db.session() as session:
        fhir_id = await bind_entity_resource(
            session,
            source_id=staged.import_date,
            release_id="synthetic",
            resource={
                "resourceType": "Organization",
                "id": "0012345678",
                "name": "Example Group",
            },
        )
    original_binder = organizations.bind_cms_doctors_group_batch
    batch_sizes = []

    async def record_batch(session, *, org_pac_ids):
        batch_sizes.append(len(org_pac_ids))
        return await original_binder(session, org_pac_ids=org_pac_ids)

    monkeypatch.setattr(organizations, "bind_cms_doctors_group_batch", record_batch)
    assert await _prepare_groups(staged) == 207
    assert batch_sizes == [100, 100, 7]
    first_bindings = await _bound_groups(staged)
    assert set(first_bindings) == set(staged.ids)
    assert first_bindings["0012345678"] != first_bindings["12345678"]
    assert fhir_id not in first_bindings.values()
    assert await _prepare_groups(staged) == 207
    assert await _bound_groups(staged) == first_bindings
    async with models.db.session() as session:
        fhir_bindings = (
            (
                await session.execute(
                    select(models.ProviderDirectoryEntitySourceBinding.resource_id).where(
                        models.ProviderDirectoryEntitySourceBinding.source_id == staged.import_date,
                    )
                )
            )
            .scalars()
            .all()
        )
    assert fhir_bindings == ["0012345678"]


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [ImportCancelledError, RuntimeError])
async def test_interrupted_batches_remain_unpublished_and_replay_safe(staged_groups, monkeypatch, failure):
    """Only completed identity batches survive; retry preserves their IDs."""
    staged = staged_groups
    original_binder = organizations.bind_cms_doctors_group_batch
    attempted_batches = []

    async def fail_second_batch(session, *, org_pac_ids):
        attempted_batches.append(org_pac_ids)
        organization_ids = await original_binder(session, org_pac_ids=org_pac_ids)
        if len(attempted_batches) == 2:
            raise failure("synthetic interruption")
        return organization_ids

    monkeypatch.setattr(organizations, "bind_cms_doctors_group_batch", fail_second_batch)
    with pytest.raises(failure, match="synthetic interruption"):
        await _prepare_groups(staged)
    completed_bindings = await _bound_groups(staged)
    assert set(attempted_batches[0]) <= set(completed_bindings)
    assert not set(attempted_batches[1]) & set(completed_bindings)
    assert await models.db.scalar(
        text("SELECT to_regclass(:stage)"), stage=f"{staged.schema}.{staged.stage.__tablename__}"
    )
    monkeypatch.setattr(organizations, "bind_cms_doctors_group_batch", original_binder)
    assert await _prepare_groups(staged) == 207
    replay_bindings = await _bound_groups(staged)
    assert all(replay_bindings[pac_id] == organization_id for pac_id, organization_id in completed_bindings.items())


@pytest.mark.asyncio
async def test_missing_bindings_fail_final_completeness_check(staged_groups, monkeypatch):
    original_binder = organizations.bind_cms_doctors_group_batch

    async def omit_last_batch(session, *, org_pac_ids):
        if len(org_pac_ids) == 7:
            return [uuid4() for _ in org_pac_ids]
        return await original_binder(session, org_pac_ids=org_pac_ids)

    monkeypatch.setattr(organizations, "bind_cms_doctors_group_batch", omit_last_batch)
    with pytest.raises(RuntimeError, match="organization_binding_incomplete"):
        await _prepare_groups(staged_groups)


@pytest.mark.asyncio
async def test_stage_drop_between_batches_fails_closed(staged_groups, monkeypatch):
    original_binder = organizations.bind_cms_doctors_group_batch
    attempted_batch_sizes = []

    async def drop_after_binding(session, *, org_pac_ids):
        attempted_batch_sizes.append(len(org_pac_ids))
        organization_ids = await original_binder(session, org_pac_ids=org_pac_ids)
        await models.db.execute_ddl(f'DROP TABLE "{staged_groups.schema}"."{staged_groups.stage.__tablename__}"')
        return organization_ids

    monkeypatch.setattr(organizations, "bind_cms_doctors_group_batch", drop_after_binding)
    with pytest.raises(DBAPIError):
        await _prepare_groups(staged_groups)
    assert attempted_batch_sizes == [100]


async def _create_serving_fixture(staged):
    for model in (models.DoctorClinicianAddress, models.CMSDoctorEducation, models.CMSDoctorGroupSite):
        await models.db.create_table(model.__table__)
    for model in (models.DoctorClinicianAddress, models.CMSDoctorEducation):
        stage = make_class(model, staged.import_date)
        await models.db.create_table(stage.__table__)
    await models.db.status(
        f'CREATE TABLE "{staged.schema}".reference_family_result_generation ('
        "importer_id text PRIMARY KEY, local_lineage_id uuid NOT NULL, local_generation bigint NOT NULL, "
        "origin_lineage_id uuid, origin_generation bigint, published_at timestamptz, relation_oids bigint[], "
        f"CHECK ({generation_shape_check()}))"
    )
    await models.db.status(
        text(
            f'INSERT INTO "{staged.schema}".reference_family_result_generation '
            "(importer_id, local_lineage_id, local_generation) VALUES ('cms-doctors', :lineage, 0)"
        ),
        lineage=uuid4(),
    )


@pytest.mark.asyncio
async def test_prepared_groups_publish_with_the_complete_clinician_family(staged_groups, monkeypatch):
    """Exercise the real three-table cutover after committed group identity preparation."""
    staged = staged_groups
    await _create_serving_fixture(staged)
    assert await _prepare_groups(staged) == 207
    prior_bindings = await _bound_groups(staged)

    async def fail_publication(*args, **kwargs):
        raise RuntimeError("synthetic publication failure")

    with monkeypatch.context() as patch:
        patch.setattr(cms_doctors, "publish_local_reference_family_generation", fail_publication)
        with pytest.raises(RuntimeError, match="synthetic publication failure"):
            await cms_doctors._publish_cms_doctors_stage(
                make_class(models.DoctorClinicianAddress, staged.import_date),
                staged.schema,
                staged.import_date,
            )
    assert await models.db.scalar(f'SELECT count(*) FROM "{staged.schema}".cms_doctor_group_site') == 0
    assert await _bound_groups(staged) == prior_bindings
    await cms_doctors._publish_cms_doctors_stage(
        make_class(models.DoctorClinicianAddress, staged.import_date),
        staged.schema,
        staged.import_date,
    )
    assert (
        await models.db.scalar(f'SELECT count(*) FROM "{staged.schema}".cms_doctor_group_site')
        == staged.receipt["source_rows"]
    )
    assert (
        await models.db.scalar(
            f"SELECT local_generation FROM \"{staged.schema}\".reference_family_result_generation WHERE importer_id='cms-doctors'"
        )
        == 1
    )
    assert await _bound_groups(staged) == prior_bindings
