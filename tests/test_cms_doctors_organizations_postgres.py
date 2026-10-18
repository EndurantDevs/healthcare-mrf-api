# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real native-role SOURCE maps and atomic Doctors publication component checks."""

import asyncio
import csv
import importlib
import os
import re
from contextlib import asynccontextmanager
from types import SimpleNamespace
from uuid import uuid4

import asyncpg
import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.engine import URL
from sqlalchemy.exc import DBAPIError

from db import models
from process import cms_doctors_identity_candidate as identities
from process import cms_doctors_organizations as organizations
from process import cms_doctors_preparation as preparation
from process import cms_doctors_sites as sites
from process.control_cancel import ImportCancelledError
from process.entity_address_cutover_contract import postgres_sqlstate
from process.ext.utils import make_class
from process.provider_directory_generic_source_policy import bound_publisher
from tests.cms_doctors_preparation_postgres_support import doctors_database
from tests.cms_npd_admission_postgres_support import _owned_database

cms_doctors = importlib.import_module("process.cms_doctors")


@asynccontextmanager
async def _component(monkeypatch):
    """Preserve the old disposable lifecycle selector and the shared UUID fixture."""
    if os.getenv("HLTHPRT_CMS_NPD_ADMISSION_TEST_DSN"):
        async with doctors_database(monkeypatch, cms_active=False) as fixture:
            yield fixture
        return
    name = os.getenv("HLTHPRT_DB_DATABASE", "")
    if not re.fullmatch(r"ptg2_v3_lifecycle_test_[a-z0-9_]{8,}", name):
        pytest.skip("requires an explicitly selected disposable PostgreSQL database")
    url = URL.create(
        "postgresql+asyncpg",
        username=os.getenv("HLTHPRT_DB_USER", "postgres"),
        password=os.getenv("HLTHPRT_DB_PASSWORD") or None,
        host=os.getenv("HLTHPRT_DB_HOST", "127.0.0.1"),
        port=int(os.getenv("HLTHPRT_DB_PORT", "5432")),
        database=name,
    )
    assert url.host in {"127.0.0.1", "localhost", "postgres"}
    async with _owned_database(url) as (owned, _admin):
        monkeypatch.setenv("HLTHPRT_CMS_NPD_ADMISSION_TEST_DSN", owned.render_as_string(hide_password=False))
        async with doctors_database(monkeypatch, cms_active=False) as fixture:
            yield fixture


def _write_organization_source(source_path, ids, site_ids):
    with source_path.open("w", newline="") as output:
        writer = csv.writer(output)
        writer.writerow(
            [
                "NPI",
                "ind_enrl_id",
                "org_pac_id",
                "adrs_id",
                "Facility Name",
                "num_org_mem",
                "Med_sch",
                "Grd_yr",
                "adr_ln_1",
                "citytown",
                "state",
                "zip_code",
            ]
        )
        writer.writerows(
            (
                [
                    "1000000004",
                    f"enrl-{i:04d}",
                    group,
                    site,
                    "Example Group",
                    "1",
                    "Example School",
                    "2001",
                    f"{i + 1} Example Street",
                    "Example City",
                    "NY",
                    "10001",
                ]
                for i, (group, site) in enumerate(zip(ids, site_ids, strict=True), 1)
            )
        )


def _staged_organization_source(fixture, import_date, source_path, context_by_key):
    return SimpleNamespace(
        fixture=fixture,
        schema=fixture.schema,
        import_date=import_date,
        source=source_path,
        stage=make_class(models.CMSDoctorGroupSite, import_date),
        ctx=context_by_key,
        receipt=context_by_key["context"]["group_site"],
    )


async def _stage_rows(fixture, ids, site_ids, *, source=None):
    import_date = uuid4().hex
    source = source or fixture.component.artifacts / (import_date + ".csv")
    if not source.exists():
        _write_organization_source(source, ids, site_ids)
    context_by_key = {"import_date": import_date, "context": {"run": True}}
    async with fixture.database.transaction():
        stage = await cms_doctors.create_doctors_stage(
            fixture.database, models.DoctorClinicianAddress, import_date, fixture.schema
        )
        await cms_doctors._stage_doctors_sidecars(source, "https://example.test/doctors.csv", context_by_key, {}, False)
        assert (
            await cms_doctors._import_doctors_source(
                source,
                ctx=context_by_key,
                task={},
                stage_cls=stage,
                batch_size=5000,
                test_mode=False,
                test_row_limit=5000,
            )
            == context_by_key["context"]["group_site"]["source_rows"]
        )
    return _staged_organization_source(fixture, import_date, source, context_by_key)


@pytest_asyncio.fixture
async def staged_groups(monkeypatch):
    async with _component(monkeypatch) as fixture:
        prefix = uuid4().hex[:8]
        ids = [f"{prefix}-{index:04d}" for index in range(205)] + ["0012345678", "12345678"]
        sites = [f"{prefix}{index:015d}01" for index in range(205)]
        physical_ids = ids + ids[:5] + [None, ""]
        physical_sites = sites + [sites[0], sites[0], sites[0][:-2] + "02", *sites[1:5], sites[2], ""]
        staged = await _stage_rows(fixture, physical_ids, physical_sites)
        staged.ids, staged.site_ids = ids, sites + [sites[0][:-2] + "02"]
        try:
            yield staged
        finally:
            await identities.discard_doctors_identities(fixture.database, staged.ctx)
            async with fixture.database.transaction() as session:
                inventory = await preparation._stage_inventory(session, staged.schema, staged.import_date)
            await preparation._drop_owned_stages(staged.schema, inventory)


async def _prepare_groups(staged):
    return await organizations.bind_group_site_organizations(
        staged.ctx, staged.import_date, staged.schema, staged.receipt
    )


async def _prepare_sites(staged):
    return await sites.bind_cms_doctors_sites(staged.ctx, staged.import_date, staged.schema, staged.receipt)


async def _bindings(staged, kind, *, published=False):
    spec = identities._SPECS[kind]
    candidates = staged.ctx["context"].get(identities._CONTEXT_KEY, {})
    if not published and kind not in candidates:
        return {}
    name = spec[0] if published else candidates[kind].table["name"]
    async with staged.fixture.database.transaction() as session:
        return dict(
            (await session.execute(text(f'SELECT "{spec[1]}","{spec[2]}" FROM "{staged.schema}"."{name}"'))).all()
        )


async def _replay(staged):
    replay = await _stage_rows(staged.fixture, [], [], source=staged.source)
    replay.ids, replay.site_ids = staged.ids, staged.site_ids
    return replay


async def _publish(staged):
    for model in preparation._models():
        stage = make_class(model, staged.import_date)
        await cms_doctors._create_stage_indexes(stage, staged.schema)
        await staged.fixture.database.status(f'ALTER TABLE "{staged.schema}"."{stage.__tablename__}" SET LOGGED')
    return await cms_doctors._publish_cms_doctors_stage(
        make_class(models.DoctorClinicianAddress, staged.import_date),
        staged.schema,
        staged.import_date,
        identity_ctx=staged.ctx,
    )


async def _fhir_identity(staged, monkeypatch, kind):
    """Mint unrelated FHIR identities through real retained8file admission and atomic SOURCE attachment."""
    from tests import cms_npd_admission_postgres_support as support

    cms = importlib.import_module("process.provider_directory_cms_npd")
    await support._provision_cms_epoch_controls(staged.fixture.component)
    original = support._resource_rows

    def resources(revision):
        rows = original(revision)
        rows["Organization"].append({"id": "0012345678", "name": "Independent FHIR group"})
        rows["Location"].append({"id": staged.site_ids[0], "name": "Independent FHIR site"})
        return rows

    monkeypatch.setattr(support, "_resource_rows", resources)
    monkeypatch.setattr(support.fhir, "db", staged.fixture.database)
    monkeypatch.setattr(importlib.import_module("process.control_lifecycle"), "db", staged.fixture.database)
    root = staged.fixture.component.artifacts / ("fhir-" + uuid4().hex)
    root.mkdir(mode=0o700)
    directory, receipt = support.retained_release(root)
    with support.release_probe_client(directory) as client:
        cms_result = await cms._run_acquired({"context": {}}, {}, "fixture-" + uuid4().hex, directory, receipt, client)
    assert cms_result["status"] == "validated" and cms_result["cms_serving_candidate"]["status"] == "ready"
    key = "0012345678" if kind == "group" else staged.site_ids[0]
    async with staged.fixture.database.transaction() as session:
        return await session.scalar(
            text(
                f'SELECT coalesce(organization_id,site_id) FROM "{staged.schema}".provider_directory_entity_source_binding '
                "WHERE source_id='cms-npd' AND resource_type=:kind AND resource_id=:key"
            ),
            {"kind": "Organization" if kind == "group" else "Location", "key": key},
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("kind,count", [("group", 207), ("site", 206)])
async def test_exact_source_ids_are_private_bounded_and_separate_from_fhir(staged_groups, monkeypatch, kind, count):
    staged = staged_groups
    fhir_id = await _fhir_identity(staged, monkeypatch, kind)
    copies = []
    original = identities.native_copy_projection

    async def copy(*args, **kwargs):
        copies.append(kwargs)
        return await original(*args, **kwargs)

    monkeypatch.setattr(identities, "native_copy_projection", copy)
    assert await (_prepare_groups(staged) if kind == "group" else _prepare_sites(staged)) == count
    selected = await _bindings(staged, kind)
    assert len(selected) == count and fhir_id is not None and fhir_id not in selected.values()
    assert len(copies) == 1 and copies[0]["max_bytes"] == 16 * 1024 * 1024
    assert await _bindings(staged, kind, published=True) == {}
    if kind == "group":
        assert selected["0012345678"] != selected["12345678"]
    else:
        assert len(staged.site_ids[0]) == len(staged.site_ids[-1]) == 25
        assert staged.site_ids[0][:-2] == staged.site_ids[-1][:-2]
        assert selected[staged.site_ids[0]] != selected[staged.site_ids[-1]]


@pytest.mark.asyncio
async def test_stage_rows_preserve_exact_clinician_group_site_relationships(staged_groups):
    staged = staged_groups
    await _prepare_groups(staged)
    await _prepare_sites(staged)
    maps = staged.ctx["context"][identities._CONTEXT_KEY]
    async with staged.fixture.database.transaction() as session:
        rows = (
            await session.execute(
                text(
                    f"SELECT s.row_number,s.npi,s.ind_enrl_id,g.organization_id,l.site_id "
                    f'FROM "{staged.schema}"."{staged.stage.__tablename__}" s LEFT JOIN "{staged.schema}"."{maps["group"].table["name"]}" g '
                    f'USING(org_pac_id) LEFT JOIN "{staged.schema}"."{maps["site"].table["name"]}" l USING(adrs_id) ORDER BY s.row_number'
                )
            )
        ).all()
    assert len(rows) == staged.receipt["source_rows"]
    assert rows[0].npi == rows[205].npi == 1000000004 and rows[0].ind_enrl_id != rows[205].ind_enrl_id
    assert rows[0].organization_id != rows[205].organization_id
    assert rows[0].site_id == rows[205].site_id == rows[206].site_id
    assert rows[0].organization_id == rows[207].organization_id and rows[0].site_id != rows[207].site_id
    assert rows[-2].organization_id is None and rows[-2].site_id is not None
    assert rows[-1].organization_id is None and rows[-1].site_id is None


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [ImportCancelledError, RuntimeError, asyncio.CancelledError])
async def test_interrupted_copy_remains_unpublished_and_replay_safe(staged_groups, monkeypatch, failure):
    staged = staged_groups
    original = identities.native_copy_projection

    async def fail(*args, **kwargs):
        await original(*args, **kwargs)
        raise failure("synthetic interruption")

    with monkeypatch.context() as changed:
        changed.setattr(identities, "native_copy_projection", fail)
        with pytest.raises(failure, match="synthetic interruption"):
            await _prepare_groups(staged)
    assert await _bindings(staged, "group", published=True) == {}
    assert await _prepare_groups(staged) == 207
    assert len(await _bindings(staged, "group")) == 207


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["group", "site"])
async def test_missing_bindings_fail_final_completeness_check(staged_groups, monkeypatch, kind):
    original = identities.native_copy_projection

    async def omit(*args, **kwargs):
        await original(*args, **kwargs)
        publisher = bound_publisher()
        spec = identities._SPECS[kind]
        await publisher.connection.execute(
            f'DELETE FROM "{staged_groups.schema}"."{kwargs["table_name"]}" '
            f'WHERE "{spec[1]}"=(SELECT max("{spec[1]}") FROM '
            f'"{staged_groups.schema}"."{kwargs["table_name"]}")'
        )

    monkeypatch.setattr(identities, "native_copy_projection", omit)
    with pytest.raises(identities.CMSIdentityReprepareRequired):
        await (_prepare_groups(staged_groups) if kind == "group" else _prepare_sites(staged_groups))
    assert await _bindings(staged_groups, kind, published=True) == {}


@pytest.mark.asyncio
async def test_source_drop_during_copy_rolls_back_without_any_global_publication(staged_groups, monkeypatch):
    staged = staged_groups
    original = identities.native_copy_projection
    name = f'"{staged.schema}"."{staged.stage.__tablename__}"'
    oid = await staged.fixture.database.scalar("SELECT to_regclass(:name)::oid", name=name)

    async def drop_after_copy(*args, **kwargs):
        await original(*args, **kwargs)
        await bound_publisher().connection.execute(f"DROP TABLE {name}")

    monkeypatch.setattr(identities, "native_copy_projection", drop_after_copy)
    with pytest.raises((DBAPIError, asyncpg.UndefinedTableError)) as refused:
        await _prepare_groups(staged)
    assert postgres_sqlstate(refused.value) == "42P01"
    assert await staged.fixture.database.scalar("SELECT to_regclass(:name)::oid", name=name) == oid
    assert await _bindings(staged, "group", published=True) == {}


@pytest.mark.asyncio
async def test_prepared_groups_publish_with_complete_family_and_stable_replay(staged_groups, monkeypatch):
    staged = staged_groups
    assert await _prepare_groups(staged) == 207 and await _prepare_sites(staged) == 206
    groups, sites_map = await _bindings(staged, "group"), await _bindings(staged, "site")
    prior = await staged.fixture.database.scalar(
        f'SELECT local_generation FROM "{staged.schema}".reference_family_result_generation '
        "WHERE importer_id='cms-doctors'"
    )

    async def fail(*args, **kwargs):
        raise RuntimeError("synthetic publication failure")

    with monkeypatch.context() as changed:
        changed.setattr(cms_doctors, "publish_doctors_generation", fail)
        with pytest.raises(RuntimeError, match="synthetic publication failure"):
            await _publish(staged)
    assert await _bindings(staged, "group", published=True) == await _bindings(staged, "site", published=True) == {}
    assert (
        await staged.fixture.database.scalar(f'SELECT org_pac_id FROM "{staged.schema}".cms_doctor_group_site')
        == "incumbent"
    )
    await _publish(staged)
    assert await _bindings(staged, "group", published=True) == groups
    assert await _bindings(staged, "site", published=True) == sites_map
    assert (
        await staged.fixture.database.scalar(f'SELECT count(*) FROM "{staged.schema}".cms_doctor_group_site')
        == staged.receipt["source_rows"]
    )
    assert (
        await staged.fixture.database.scalar(
            f'SELECT local_generation FROM "{staged.schema}".reference_family_result_generation '
            "WHERE importer_id='cms-doctors'"
        )
        == prior + 1
    )
    replay = await _replay(staged)
    try:
        assert await _prepare_groups(replay) == 207 and await _prepare_sites(replay) == 206
        assert await _bindings(replay, "group") == groups and await _bindings(replay, "site") == sites_map
    finally:
        await identities.discard_doctors_identities(staged.fixture.database, replay.ctx)


@pytest.mark.asyncio
async def test_concurrent_publishers_keep_one_stable_winner_and_require_whole_reprepare(staged_groups):
    staged = staged_groups
    competitor = await _replay(staged)
    try:
        await _prepare_groups(staged)
        await _prepare_sites(staged)
        await _prepare_groups(competitor)
        await _prepare_sites(competitor)
        results = await asyncio.gather(_publish(staged), _publish(competitor), return_exceptions=True)
        assert sum(not isinstance(value, BaseException) for value in results) == 1
        loser = next(value for value in results if isinstance(value, BaseException))
        assert isinstance(loser, identities.CMSIdentityReprepareRequired)
        assert len(await _bindings(staged, "group", published=True)) == 207
        assert len(await _bindings(staged, "site", published=True)) == 206
    finally:
        await identities.discard_doctors_identities(staged.fixture.database, competitor.ctx)
