# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Actual source validation and retained bytes mint the prepared provenance."""

import csv
import json
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import asyncpg
import pytest
from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from db.connection import Database
from db.models import CMSDoctorEducation, CMSDoctorGroupSite
from process import cms_doctors_education as education
from process import cms_doctors_groups as groups
from process import cms_doctors_preparation as preparation
from process import reference_family_archive as archive_native
from process.cms_doctors_source_provenance import read_doctors_source_provenance
from process.ext import utils
from process.ext.utils import make_class
from tests.cms_doctors_preparation_postgres_support import doctors_database, stage_family, stage_oids
from tests.cms_npd_admission_postgres_support import _database_url
from tests.test_cms_doctors_source_provenance import source_metrics


async def _source_stages(fixture, tmp_path, monkeypatch):
    """Populate the native stage family with all three real parsers' synthetic source rows."""
    native = preparation._native()
    metrics = source_metrics(tmp_path, monkeypatch)
    ctx = await stage_family(fixture.database, fixture.schema)
    ctx["context"].update({name: metrics[name] for name in ("education", "group_site", "artifact")})
    with education.open_doctors_csv(tmp_path / "doctors.csv") as reader:
        row = next(reader)
    manifest = metrics["education"]
    mapped_rows = (
        native.doctor_address_row(row, datetime.fromisoformat(manifest["downloaded_at"])),
        education.doctor_education_row(row, 1, manifest),
        groups.doctor_group_site_row(row, 1, manifest),
    )
    async with fixture.database.transaction() as session:
        for model, mapped in zip(preparation._models(), mapped_rows, strict=True):
            stage = native.make_class(model, ctx["import_date"])
            await session.execute(stage.__table__.delete())
            await session.execute(stage.__table__.insert().values(**mapped))
    return ctx


@pytest.mark.parametrize("failure", [None, "artifact", "education"])
async def test_prepared_provenance_requires_actual_bytes_and_stage_validation(monkeypatch, tmp_path, failure):
    """Exercise real small source validation; identity enrichment is outside this fixture."""
    native = preparation._native()
    real_prepare = native._prepare_cms_doctors_sources
    async with doctors_database(monkeypatch, cms_active=False) as fixture:
        monkeypatch.setattr(native, "_prepare_cms_doctors_sources", real_prepare)
        monkeypatch.setattr(native, "DEFAULT_MIN_ROWS", 1)
        monkeypatch.setattr(education, "MIN_EDUCATION_ROWS", 1)
        monkeypatch.setattr(native, "bind_group_site_organizations", AsyncMock(return_value=0))
        monkeypatch.setattr(native, "bind_cms_doctors_sites", AsyncMock(return_value=0))
        monkeypatch.setattr(native, "_resolve_cms_doctors_addresses", AsyncMock(return_value=None))
        monkeypatch.setattr(native, "raise_if_cancelled", AsyncMock())
        ctx = await _source_stages(fixture, tmp_path, monkeypatch)
        if failure == "artifact":
            (tmp_path / "retained" / ctx["context"]["artifact"]["file_name"]).write_bytes(b"changed")
        if failure == "education":
            ctx["context"]["education"]["generation_id"] = "f" * 64
        if failure:
            with pytest.raises(RuntimeError, match="artifact_changed|education_stage_incomplete"):
                async with preparation.prepare_cms_doctors_generation(ctx):
                    pytest.fail("invalid source cannot produce prepared provenance")
        else:
            async with preparation.prepare_cms_doctors_generation(ctx) as prepared:
                proof = read_doctors_source_provenance(prepared.source_provenance_json)
                assert proof["artifact"] == ctx["context"]["artifact"]
                assert proof["education"] == ctx["context"]["education"]
                assert len(prepared.sealed_filenodes) == 3
                assert prepared.native_receipt is None and not prepared.committed
                assert proof["rows"] == 1
        assert await stage_oids(fixture, ctx) == ()


SOURCE_URL = "https://example.test/sidecars.csv"


@pytest.fixture
async def cms_sidecar_storage():
    """Use the selected CMS family's UUID-owned DSN guard and a real isolated schema."""
    url = _database_url().set(drivername="postgresql+asyncpg")
    engine = create_async_engine(url)
    schema = "cms_sidecar_" + uuid4().hex
    connection = None
    try:
        connection = await asyncpg.connect(url.set(drivername="postgresql").render_as_string(hide_password=False))
        await connection.execute(f'CREATE SCHEMA "{schema}"')
        yield connection, schema, async_sessionmaker(engine)
    finally:
        await engine.dispose()
        if connection is not None:
            try:
                await connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
                assert await connection.fetchval("SELECT to_regnamespace($1)", schema) is None
            finally:
                await connection.close()


@pytest.fixture
async def sidecar_db(cms_sidecar_storage, monkeypatch):
    """Use the existing isolated schema and ordinary model-backed Database."""
    connection, schema, sessions = cms_sidecar_storage
    engine = sessions.kw["bind"]
    database = Database(engine=engine, session_factory=sessions)
    for module in (education, groups, utils):
        monkeypatch.setattr(module, "db", database)
    for model in (CMSDoctorEducation, CMSDoctorGroupSite):
        monkeypatch.setattr(model.__table__, "schema", schema)
    completed, attempted, visible = [], [], []
    original_copy = archive_native.native_copy_record_batch

    async def checked_copy(session, model, **options):
        records, columns = options["records"], options["columns"]
        attempted.append(len(records))
        if completed:
            visible.append(await connection.fetchval(f'SELECT count(*) FROM "{schema}"."{model.__tablename__}"'))
        assert session.in_transaction() and 0 < len(records) <= 5000
        assert columns == tuple(model.__table__.columns.keys())
        assert all(isinstance(row[columns.index("source_json")], str) for row in records)
        copied = await original_copy(session, model, **options)
        completed.append(copied)
        assert copied == len(records)
        return copied

    def refuse_insert(_connection, _cursor, statement, _parameters, _context, _many):
        assert not statement.lstrip().upper().startswith("INSERT"), "sidecar INSERT fallback is forbidden"

    monkeypatch.setattr(archive_native, "native_copy_record_batch", checked_copy)
    event.listen(engine.sync_engine, "before_cursor_execute", refuse_insert)
    try:
        yield SimpleNamespace(
            connection=connection,
            schema=schema,
            database=database,
            attempted=attempted,
            completed=completed,
            visible=visible,
        )
    finally:
        event.remove(engine.sync_engine, "before_cursor_execute", refuse_insert)


def _source(path, count, *, duplicate_at=None):
    """Create source-grain rows with one optional repeated education assertion."""
    fields = (
        "NPI",
        "Med_sch",
        "Grd_yr",
        "Ind_enrl_ID",
        "Org_PAC_ID",
        "adrs_id",
        "Facility Name",
        "num_org_mem",
        "adr_ln_1",
        "City/Town",
        "State",
        "ZIP Code",
    )
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        for number in range(1, count + 1):
            identity = 1 if number == duplicate_at else number
            writer.writerow(
                dict(
                    zip(
                        fields,
                        (
                            "1000000004",
                            f"Example École {identity}",
                            "2001",
                            "E-1",
                            f"000000000{number % 2 + 1}",
                            "same-site",
                            "Example Group",
                            "12",
                            "10 Main Street",
                            "Springfield",
                            "IL",
                            "62704",
                        ),
                    )
                )
            )
    return education.education_source_manifest(path, SOURCE_URL)


def _stage(kind, context):
    model = CMSDoctorEducation if kind == "education" else CMSDoctorGroupSite
    return make_class(model, context["import_date"])


async def _import(kind, source, context, manifest):
    if kind == "education":
        return await education.import_doctor_education(source, SOURCE_URL, context, {}, source_manifest=manifest)
    return await groups.import_group_site_rows(source, context, {}, manifest)


async def _assert_native_json(fixture, stage, manifest, expected_count):
    relation = f'"{fixture.schema}"."{stage.__tablename__}"'
    counts = await fixture.connection.fetchrow(
        f"SELECT count(*) AS rows, count(DISTINCT generation_id) AS generations, "
        f"bool_and(json_typeof(source_json)='object') AS objects, "
        f"bool_and(source_json->>'content_sha256'=$1) AS source_bound FROM {relation}",
        manifest["content_sha256"],
    )
    assert tuple(counts) == (expected_count, 1, True, True)
    document = json.loads(
        await fixture.connection.fetchval(
            f"SELECT source_json::text FROM {relation} WHERE (source_json->>'row_number')::bigint=1"
        )
    )
    assert document["row_number"] == 1
    assert document["generation_id"] == manifest["generation_id"]
    assert (
        document["raw_fields"]["NPI" if stage.__tablename__.startswith("cms_doctor_education") else "npi"]
        == "1000000004"
    )


async def _assert_education(fixture, stage, context, receipt):
    relation = f'"{fixture.schema}"."{stage.__tablename__}"'
    assert receipt["source_rows"] == 10002 and receipt["education_rows"] == 10001
    assert fixture.attempted == fixture.completed == [5000, 4999, 2]
    assert fixture.visible == [5000, 9999]
    assert (
        await fixture.connection.fetchval(
            f"SELECT count(*) FROM {relation} WHERE (source_json->>'row_number')::bigint=5001"
        )
        == 0
    )
    assert (
        await fixture.connection.fetchval(
            f"SELECT medical_school FROM {relation} WHERE (source_json->>'row_number')::bigint=1"
        )
        == "Example École 1"
    )
    assert await fixture.connection.fetchval(f"SELECT count(DISTINCT education_key) FROM {relation}") == 10001
    await education.validate_education_stage(context["import_date"], fixture.schema, receipt)


async def _assert_groups(fixture, stage, context, receipt):
    relation = f'"{fixture.schema}"."{stage.__tablename__}"'
    assert receipt["source_rows"] == 10002
    assert fixture.attempted == fixture.completed == [5000, 5000, 2]
    assert fixture.visible == [5000, 10000]
    counts = await fixture.connection.fetchrow(
        f"SELECT min(row_number),max(row_number),count(DISTINCT row_number), "
        f"count(DISTINCT address_checksum),count(DISTINCT org_pac_id), "
        f"bool_and(membership_start_at IS NULL AND membership_end_at IS NULL) FROM {relation}"
    )
    assert tuple(counts) == (1, 10002, 10002, 1, 2, True)
    await groups.validate_group_site_stage(context["import_date"], fixture.schema, receipt)


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["education", "group_site"])
async def test_real_sidecar_copy_preserves_json_counts_and_source_grain(sidecar_db, tmp_path, kind):
    source_path = tmp_path / "sidecars.csv"
    manifest = _source(source_path, 10002, duplicate_at=5001)
    worker_context_by_key = {"import_date": uuid4().hex, "context": {}}
    receipt = await _import(kind, source_path, worker_context_by_key, manifest)
    stage = _stage(kind, worker_context_by_key)
    await _assert_native_json(sidecar_db, stage, manifest, 10001 if kind == "education" else 10002)
    if kind == "education":
        await _assert_education(sidecar_db, stage, worker_context_by_key, receipt)
    else:
        await _assert_groups(sidecar_db, stage, worker_context_by_key, receipt)
    assert worker_context_by_key["context"][f"{kind}_stage_owned"] is True


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["education", "group_site"])
async def test_real_second_copy_failure_drops_only_owned_stage(sidecar_db, tmp_path, monkeypatch, kind):
    source_path = tmp_path / "sidecars.csv"
    manifest = _source(source_path, 5001)
    worker_context_by_key = {"import_date": uuid4().hex, "context": {}}
    stage = _stage(kind, worker_context_by_key)
    database = sidecar_db.database
    original_create = database.create_table
    sentinel = f'"{sidecar_db.schema}".sidecar_cleanup_sentinel'
    await database.status(f"CREATE TABLE {sentinel} (marker integer PRIMARY KEY)")

    async def reject_second_batch(table, **options):
        await original_create(table, **options)
        relation = f'"{table.schema}"."{table.name}"'
        await database.status(
            f"ALTER TABLE {relation} ADD CONSTRAINT sidecar_test_second_batch "
            "CHECK ((source_json->>'row_number')::bigint<=5000)"
        )

    monkeypatch.setattr(database, "create_table", reject_second_batch)
    with pytest.raises(asyncpg.CheckViolationError) as failure:
        await _import(kind, source_path, worker_context_by_key, manifest)
    assert failure.value.sqlstate == "23514"
    assert failure.value.constraint_name == "sidecar_test_second_batch"
    assert sidecar_db.attempted == [5000, 1] and sidecar_db.completed == [5000]
    assert sidecar_db.visible == [5000]
    assert f"{kind}_stage_owned" not in worker_context_by_key["context"]
    assert (
        await sidecar_db.connection.fetchval("SELECT to_regclass($1)", f"{sidecar_db.schema}.{stage.__tablename__}")
        is None
    )
    assert await sidecar_db.connection.fetchval("SELECT to_regclass($1)", sentinel) is not None
    assert await database.scalar(text("SELECT 1")) == 1
