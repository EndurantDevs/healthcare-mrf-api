# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import csv
import hashlib
import importlib
import os
import zipfile
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError

from process import cms_doctors_education as education
from process.entity_address_cutover_contract import postgres_sqlstate
from tests.test_cms_doctors_organizations import _unit_source_transaction

NPI = "1000000004"
SOURCE_URL = "https://example.test/cms-national.csv"
MANIFEST = {
    "source_key": "cms-doctors",
    "dataset_id": "mj5m-pzi6",
    "schema_version": "cms-doctor-education/v1",
    "source_url": SOURCE_URL,
    "content_sha256": "a" * 64,
    "generation_id": "c" * 64,
    "downloaded_at": "2026-01-01T00:00:00",
}


@pytest.fixture
def staged_rows(monkeypatch):
    rows = []

    async def retain_batch(batch, _stage):
        rows.extend(dict(row) for row in batch)

    async def create_stage(database, model, import_date, schema):
        return await database.create_table(education.make_class(model, import_date).__table__, checkfirst=False)

    monkeypatch.setattr(education, "create_doctors_stage", create_stage)
    monkeypatch.setattr(education.db, "create_table", AsyncMock())
    monkeypatch.setattr(education.db, "status", AsyncMock())
    monkeypatch.setattr(education, "_copy_sidecar_rows", retain_batch)
    monkeypatch.setattr(education, "raise_if_cancelled", AsyncMock())
    return rows


@pytest.mark.parametrize(
    ("school", "year", "expected"),
    [
        (" Example Medical School ", " 2001 ", ("Example Medical School", 2001)),
        ("Example Medical School", "", ("Example Medical School", None)),
        ("", "2001", (None, 2001)),
        ("OTHER", "2001", (None, 2001)),
        (" other ", "", None),
        ("", "", None),
    ],
)
def test_education_keeps_source_partials_without_inventing_experience(school, year, expected):
    row = education.doctor_education_row(
        {"npi": NPI, "med_sch": school, "grd_yr": year},
        12,
        MANIFEST,
    )
    if expected is None:
        assert row is None
        return
    assert (row["medical_school"], row["graduation_year"]) == expected
    assert row["npi"] == int(NPI)
    assert row["generation_id"] == MANIFEST["generation_id"]
    assert row["source_json"] == {
        **MANIFEST,
        "row_number": 12,
        "raw_fields": {"NPI": NPI, "Med_sch": school, "Grd_yr": year},
        "quality_flags": [],
    }
    assert row["imported_at"] == datetime.fromisoformat(MANIFEST["downloaded_at"])
    assert len(row["education_key"]) == 64
    assert "years_of_practice" not in row


@pytest.mark.parametrize("npi", ["", "1000000005", "100000004", "9999999999", "abcdefghij"])
def test_education_rejects_invalid_npi(npi):
    with pytest.raises(ValueError, match="cms_education_invalid_npi:row=9"):
        education.doctor_education_row({"NPI": npi, "Med_sch": "Example School"}, 9, MANIFEST)


@pytest.mark.parametrize("year", ["1799", "02001", "2001.0", "20xx", "２００１"])
def test_education_rejects_invalid_graduation_year(year):
    with pytest.raises(ValueError, match="cms_education_invalid_graduation_year:row=9"):
        education.doctor_education_row({"NPI": NPI, "Grd_yr": year}, 9, MANIFEST)


def test_manifest_generation_and_future_year_flags_follow_observation_date(tmp_path, monkeypatch):
    source_path = tmp_path / "national.csv"
    source_path.write_text(f"NPI,Med_sch,Grd_yr\n{NPI},Example School,2001\n")
    observation_clock = SimpleNamespace(
        utcnow=Mock(side_effect=[datetime(2000, 12, 31), datetime(2001, 1, 1)]),
        fromisoformat=datetime.fromisoformat,
    )
    monkeypatch.setattr(education, "datetime", observation_clock)
    first_manifest_by_name = education.education_source_manifest(source_path, SOURCE_URL)
    next_manifest_by_name = education.education_source_manifest(source_path, SOURCE_URL)
    assert first_manifest_by_name["content_sha256"] == next_manifest_by_name["content_sha256"]
    assert first_manifest_by_name["generation_id"] == next_manifest_by_name["generation_id"]
    assert first_manifest_by_name["generation_id"] != first_manifest_by_name["content_sha256"]
    assert first_manifest_by_name["schema_version"] == "cms-doctor-education/v1"
    source_row_by_field = {"NPI": NPI, "Med_sch": "Example School", "Grd_yr": "2001"}
    future = education.doctor_education_row(source_row_by_field, 9, first_manifest_by_name)
    completed = education.doctor_education_row(source_row_by_field, 9, next_manifest_by_name)
    assert future["graduation_year"] == 2001
    assert future["source_json"]["raw_fields"]["Grd_yr"] == "2001"
    assert future["source_json"]["quality_flags"] == ["graduation_year_in_future"]
    assert not completed["source_json"].get("quality_flags")
    assert future["education_key"] == completed["education_key"]
    assert future["generation_id"] == completed["generation_id"]


def test_manifest_generation_changes_only_with_source_identity_or_bytes(tmp_path, monkeypatch):
    source_path = tmp_path / "national.csv"
    source_path.write_text(f"NPI,Med_sch\n{NPI},Example School\n")
    monkeypatch.setattr(
        education,
        "datetime",
        SimpleNamespace(utcnow=Mock(side_effect=[datetime(2026, 1, 1)] * 3)),
    )
    first = education.education_source_manifest(source_path, "https://example.test/first.csv")
    same_bytes = education.education_source_manifest(source_path, "https://example.test/second.csv")
    assert first["generation_id"] == same_bytes["generation_id"]
    assert first["source_url"] != same_bytes["source_url"]
    source_path.write_text(f"NPI,Med_sch\n{NPI},Different School\n")
    changed = education.education_source_manifest(source_path, "https://example.test/first.csv")
    assert changed["generation_id"] != first["generation_id"]


def test_education_identity_ignores_artifact_and_practice_location():
    source_row_by_field = {"NPI": NPI, "Med_sch": "Example School", "Grd_yr": "2001"}
    first = education.doctor_education_row(source_row_by_field, 1, MANIFEST)
    repeat = education.doctor_education_row(
        {**source_row_by_field, "adr_ln_1": "200 Example Avenue"},
        20,
        {**MANIFEST, "content_sha256": "b" * 64, "generation_id": "d" * 64},
    )
    different = education.doctor_education_row({**source_row_by_field, "Grd_yr": "2002"}, 21, MANIFEST)
    assert first["education_key"] == repeat["education_key"]
    assert first["education_key"] != different["education_key"]
    assert first["generation_id"] != repeat["generation_id"]


async def test_configured_dataset_identity_is_retained_in_rows_and_receipt(tmp_path, staged_rows):
    source_path = tmp_path / "national.csv"
    source_path.write_text(f"NPI,Med_sch,Grd_yr\n{NPI},Example School,2001\n")
    receipt = await education.import_doctor_education(
        source_path,
        SOURCE_URL,
        {"import_date": "educationtests", "context": {}},
        {},
        "synthetic-cms-dataset",
    )
    assert receipt["dataset_id"] == "synthetic-cms-dataset"
    assert staged_rows[0]["source_json"]["dataset_id"] == "synthetic-cms-dataset"
    assert staged_rows[0]["generation_id"] == receipt["generation_id"]


@pytest.mark.parametrize("extension", ["csv", "zip"])
async def test_csv_and_zip_preserve_bom_quoted_fields_and_artifact_identity(tmp_path, staged_rows, extension):
    source_path = tmp_path / f"national.{extension}"
    payload = f'\ufeffNPI,Med_sch,Grd_yr\r\n{NPI},"Example School, North",2001\r\n'.encode()
    if extension == "zip":
        with zipfile.ZipFile(source_path, "w") as archive:
            archive.writestr("national.csv", payload)
            archive.writestr("README.txt", "Synthetic input")
    else:
        source_path.write_bytes(payload)
    receipt = await education.import_doctor_education(
        source_path,
        SOURCE_URL,
        {"import_date": "educationtests", "context": {}},
        {},
    )
    assert receipt["source_rows"] == receipt["education_rows"] == 1
    assert receipt["content_sha256"] == hashlib.sha256(source_path.read_bytes()).hexdigest()
    assert receipt["source_url"] == SOURCE_URL
    assert staged_rows[0]["generation_id"] == receipt["generation_id"]
    assert staged_rows[0]["medical_school"] == "Example School, North"
    assert staged_rows[0]["source_json"]["content_sha256"] == receipt["content_sha256"]


@pytest.mark.parametrize("names", [("README.txt",), ("first.csv", "second.CSV")])
def test_zip_rejects_missing_or_ambiguous_csv(tmp_path, names):
    source_path = tmp_path / "national.zip"
    with zipfile.ZipFile(source_path, "w") as archive:
        for name in names:
            archive.writestr(name, "NPI,Med_sch,Grd_yr\n")
    with pytest.raises(ValueError, match="exactly one CSV"):
        with education.open_doctors_csv(source_path) as reader:
            pytest.fail(f"Ambiguous ZIP was accepted with fields {reader.fieldnames}")


@pytest.mark.parametrize(
    ("payload", "error", "match"),
    [
        ("NPI,Med_sch\n", ValueError, "required_headers_missing"),
        ("NPI,Med_sch,Grd_yr,MED_SCH\n", ValueError, "headers"),
        (f"NPI,Med_sch,Grd_yr\n{NPI},School\n", ValueError, "row_width_changed"),
        (f"NPI,Med_sch,Grd_yr\n{NPI},School,2001,extra\n", ValueError, "row_width_changed"),
        (f'NPI,Med_sch,Grd_yr\n{NPI},"School,2001\n', csv.Error, None),
    ],
)
async def test_import_rejects_changed_or_malformed_csv(tmp_path, staged_rows, payload, error, match):
    source_path = tmp_path / "national.csv"
    source_path.write_text(payload, encoding="utf-8")
    with pytest.raises(error, match=match):
        await education.import_doctor_education(
            source_path,
            SOURCE_URL,
            {"import_date": "educationtests", "context": {}},
            {},
        )
    assert staged_rows == []
    education.db.status.assert_awaited_once()
    assert education.db.status.await_args.args[0].endswith(".cms_doctor_education_educationtests")


async def test_test_mode_limits_physical_input_rows(tmp_path, staged_rows, monkeypatch):
    monkeypatch.setenv("HLTHPRT_CMS_DOCTORS_TEST_ROWS", "2")
    source_path = tmp_path / "national.csv"
    source_path.write_text(
        f"NPI,Med_sch,Grd_yr\n{NPI},School,2001\n{NPI},School,2001\ninvalid,School,2002\n",
        encoding="utf-8",
    )
    receipt = await education.import_doctor_education(
        source_path,
        SOURCE_URL,
        {"import_date": "educationtests", "context": {"test_mode": True}},
        {},
    )
    assert receipt["source_rows"] == 2
    assert receipt["education_rows"] == len(staged_rows) == 1


async def test_existing_stage_creation_failure_never_drops_unowned_table(tmp_path, staged_rows):
    source_path = tmp_path / "national.csv"
    source_path.write_text("NPI,Med_sch,Grd_yr\n", encoding="utf-8")
    education.db.create_table.side_effect = RuntimeError("stage already exists")
    with pytest.raises(RuntimeError, match="stage already exists"):
        await education.import_doctor_education(
            source_path,
            SOURCE_URL,
            {"import_date": "educationtests", "context": {}},
            {},
        )
    education.db.status.assert_not_awaited()
    assert staged_rows == []


@pytest.mark.parametrize("is_owned", [False, True])
async def test_discard_only_removes_owned_stage_once(staged_rows, is_owned):
    worker_context_by_key = {
        "import_date": "educationtests",
        "context": {"education_stage_owned": is_owned},
    }
    await education.discard_education_stage(worker_context_by_key)
    await education.discard_education_stage(worker_context_by_key)
    assert education.db.status.await_count == int(is_owned)
    assert not worker_context_by_key["context"].get("education_stage_owned")
    if is_owned:
        assert education.db.status.await_args.args[0].endswith(".cms_doctor_education_educationtests")


async def test_address_failure_discards_completed_education_stage(tmp_path, staged_rows, monkeypatch):
    import aiohttp

    cms_doctors = importlib.import_module("process.cms_doctors")
    monkeypatch.setattr(cms_doctors, "source_transaction", _unit_source_transaction)
    monkeypatch.setattr(cms_doctors, "_create_stage_indexes", AsyncMock())
    monkeypatch.setattr(cms_doctors, "bound_publisher", lambda: SimpleNamespace(connection=object(), policy=object()))
    monkeypatch.setattr(cms_doctors.source_storage, "_close_grants", AsyncMock())

    async def download_source(_client, _url, source_path):
        with open(source_path, "w") as source_file:
            source_file.write(f"NPI,Med_sch,Grd_yr\n{NPI},Example School,2001\n")

    client = SimpleNamespace(close=AsyncMock())
    monkeypatch.setattr(aiohttp, "ClientSession", lambda: client)
    monkeypatch.setattr(cms_doctors, "ensure_database", AsyncMock())
    monkeypatch.setattr(cms_doctors, "raise_if_cancelled", AsyncMock())
    monkeypatch.setattr(cms_doctors, "validate_doctors_artifact_root", lambda: None)
    monkeypatch.setattr(cms_doctors, "_fetch_doctors_download_url", AsyncMock(return_value=SOURCE_URL))
    monkeypatch.setattr(cms_doctors, "_download_doctors_source", download_source)
    monkeypatch.setattr(
        cms_doctors,
        "retain_doctors_artifact",
        lambda path, _url: {"content_sha256": hashlib.sha256(Path(path).read_bytes()).hexdigest()},
    )
    monkeypatch.setattr(cms_doctors, "import_group_site_rows", AsyncMock(return_value={"source_rows": 1}))
    monkeypatch.setattr(cms_doctors, "_create_stage_indexes", AsyncMock())
    monkeypatch.setattr(cms_doctors, "_import_doctors_source", AsyncMock(side_effect=RuntimeError("address failure")))
    worker_context_by_key = {"import_date": "educationtests", "context": {}}
    with pytest.raises(RuntimeError, match="address failure"):
        await cms_doctors.import_cms_doctors_data(worker_context_by_key, {})
    assert len(staged_rows) == 1
    assert not worker_context_by_key["context"].get("education_stage_owned")
    assert not worker_context_by_key["context"].get("run")
    education.db.status.assert_awaited_once()
    client.close.assert_awaited_once()


async def test_publication_validation_failure_discards_owned_stage(tmp_path, staged_rows, monkeypatch):
    cms_doctors = importlib.import_module("process.cms_doctors")
    monkeypatch.setattr(cms_doctors, "source_transaction", _unit_source_transaction)
    monkeypatch.setattr(cms_doctors, "_create_stage_indexes", AsyncMock())
    monkeypatch.setattr(cms_doctors, "bound_publisher", lambda: SimpleNamespace(connection=object(), policy=object()))
    monkeypatch.setattr(cms_doctors.source_storage, "_close_grants", AsyncMock())
    source_path = tmp_path / "national.csv"
    source_path.write_text(f"NPI,Med_sch,Grd_yr\n{NPI},Example School,2001\n")
    worker_context_by_key = {"import_date": "educationtests", "context": {"run": 1}}
    worker_context_by_key["context"]["education"] = await education.import_doctor_education(
        source_path,
        SOURCE_URL,
        worker_context_by_key,
        {},
    )
    monkeypatch.setattr(cms_doctors, "ensure_database", AsyncMock())
    monkeypatch.setattr(cms_doctors.db, "scalar", AsyncMock(return_value=20_000))
    monkeypatch.setattr(cms_doctors, "validate_education_stage", AsyncMock(side_effect=RuntimeError("invalid stage")))
    publication = AsyncMock()
    monkeypatch.setattr(cms_doctors, "_publish_cms_doctors_stage", publication)
    with pytest.raises(RuntimeError, match="invalid stage"):
        await cms_doctors.publish_cms_doctors_generation(worker_context_by_key)
    assert not worker_context_by_key["context"].get("education_stage_owned")
    education.db.status.assert_awaited_once()
    publication.assert_not_awaited()


async def test_import_retains_addressless_conflicts_and_deduplicates_across_batches(tmp_path, staged_rows):
    source_path = tmp_path / "national.csv"
    with source_path.open("w", newline="", encoding="utf-8") as source_file:
        writer = csv.writer(source_file)
        writer.writerow(["NPI", "Med_sch", "Grd_yr", "adr_ln_1"])
        for _ in range(5_001):
            writer.writerow([NPI, "First Example School", "2001", ""])
        writer.writerow([NPI, "Second Example School", "2001", ""])
        writer.writerow([NPI, "", "", ""])
    receipt = await education.import_doctor_education(
        source_path,
        SOURCE_URL,
        {"import_date": "educationtests", "context": {}},
        {},
    )
    assert receipt["source_rows"] == 5_003
    assert receipt["education_rows"] == 2
    assert [row["medical_school"] for row in staged_rows] == ["First Example School", "Second Example School"]
    assert len({row["education_key"] for row in staged_rows}) == 2


@pytest.mark.parametrize(
    "counts",
    [
        {"rows": 0, "generations": 0, "generation_id": None},
        {"rows": 9_999, "generations": 1, "generation_id": "c" * 64},
        {"rows": 20_001, "generations": 1, "generation_id": "c" * 64},
        {"rows": 20_000, "generations": 2, "generation_id": "c" * 64},
        {"rows": 20_000, "generations": 1, "generation_id": "a" * 64},
    ],
)
async def test_validation_rejects_empty_small_mismatched_or_mixed_stage(monkeypatch, counts):
    monkeypatch.setattr(education.db, "first", AsyncMock(return_value=SimpleNamespace(_mapping=counts)))
    scalar = AsyncMock()
    monkeypatch.setattr(education.db, "scalar", scalar)
    with pytest.raises(RuntimeError, match="cms_education_stage_incomplete"):
        await education.validate_education_stage("educationtests", "mrf", {**MANIFEST, "education_rows": 20_000})
    scalar.assert_not_awaited()


@pytest.mark.parametrize("live_rows", [None, 21_000, 30_000])
async def test_validation_accepts_first_and_safe_refresh_but_blocks_volume_drop(monkeypatch, live_rows):
    counts_by_name = {"rows": 20_000, "generations": 1, "generation_id": "c" * 64}
    monkeypatch.setattr(education.db, "first", AsyncMock(return_value=SimpleNamespace(_mapping=counts_by_name)))
    monkeypatch.setattr(education.db, "scalar", AsyncMock(side_effect=[live_rows, live_rows]))
    validation = education.validate_education_stage("educationtests", "mrf", {**MANIFEST, "education_rows": 20_000})
    if live_rows == 30_000:
        with pytest.raises(RuntimeError, match="cms_education_volume_drop"):
            await validation
    else:
        await validation


async def _assert_native_family_rollback(fixture):
    from process import cms_doctors_preparation as preparation
    from tests.cms_doctors_preparation_postgres_support import authority, native, stage_family, stage_oids

    incumbent = await authority(fixture)
    first = await stage_family(fixture.database, fixture.schema)
    stages = await stage_oids(fixture, first)
    async with fixture.database.transaction() as session:
        await session.execute(education.text(f'DROP TABLE "{fixture.schema}"."{stages[1][1]}"'))
    with pytest.raises(DBAPIError) as missing_stage:
        await native._publish_cms_doctors_stage(
            native.make_class(native.DoctorClinicianAddress, first["import_date"]), fixture.schema, first["import_date"]
        )
    assert postgres_sqlstate(missing_stage.value) == "42P01"
    assert await authority(fixture) == incumbent
    await preparation._drop_owned_stages(fixture.schema, stages)
    ctx = await stage_family(fixture.database, fixture.schema)
    async with preparation.prepare_cms_doctors_generation(ctx) as prepared:
        async with fixture.engine.begin() as admin:
            await admin.execute(
                education.text(
                    f'ALTER TABLE "{fixture.schema}".reference_family_result_generation ADD CONSTRAINT reject_advance '
                    f"CHECK (importer_id<>'cms-doctors' OR local_generation={incumbent.local_generation})"
                )
            )
        try:
            with pytest.raises(DBAPIError) as rejected_generation:
                async with fixture.database.transaction():
                    await preparation.apply_prepared_cms_doctors_generation(prepared)
            assert postgres_sqlstate(rejected_generation.value) == "23514"
            assert await authority(fixture) == incumbent
            assert await stage_oids(fixture, ctx) == prepared.stage_oids
        finally:
            async with fixture.engine.begin() as admin:
                await admin.execute(
                    education.text(
                        f'ALTER TABLE "{fixture.schema}".reference_family_result_generation DROP CONSTRAINT reject_advance'
                    )
                )
        async with fixture.database.transaction():
            result_authority = await preparation.apply_prepared_cms_doctors_generation(prepared)
        assert result_authority == await authority(fixture)
        assert result_authority.local_generation == incumbent.local_generation + 1
        for table, column in (
            ("doctor_clinician_address", "city"),
            ("cms_doctor_education", "medical_school"),
            ("cms_doctor_group_site", "org_pac_id"),
        ):
            assert await fixture.database.scalar(f'SELECT {column} FROM "{fixture.schema}"."{table}"') == "prepared"
            assert (
                await fixture.database.scalar(f'SELECT {column} FROM "{fixture.schema}"."{table}_old"') == "incumbent"
            )


async def test_native_postgres_publication_rolls_back_both_tables_on_education_failure(monkeypatch):
    """The native-role component rolls back all three heaps and the real ledger CAS."""
    from tests.cms_doctors_preparation_postgres_support import doctors_database
    from tests.cms_npd_admission_postgres_support import _owned_database

    dsn = os.getenv("HLTHPRT_CMS_EDUCATION_POSTGRES_DSN")
    if not dsn:
        pytest.skip("set HLTHPRT_CMS_EDUCATION_POSTGRES_DSN for the PostgreSQL proof")
    url = make_url(dsn)
    if url.host not in {"127.0.0.1", "localhost", "postgres"} or "test" not in (url.database or "").lower():
        pytest.fail("CMS education proof requires an explicit local test database")
    async with _owned_database(url.set(drivername="postgresql+asyncpg")) as (owned_url, _admin):
        monkeypatch.setenv("HLTHPRT_CMS_NPD_ADMISSION_TEST_DSN", owned_url.render_as_string(hide_password=False))
        async with doctors_database(monkeypatch, cms_active=False) as fixture:
            await _assert_native_family_rollback(fixture)


@pytest.mark.asyncio
async def test_clinical_copy_keeps_nullable_columns_and_never_uses_insert_fallback(monkeypatch):
    from contextlib import asynccontextmanager

    from db.models import DoctorClinicianAddress

    clinical = importlib.import_module("process.cms_doctors")
    from process import reference_family_archive as native

    session = SimpleNamespace(execute=AsyncMock())

    @asynccontextmanager
    async def transaction():
        yield session

    monkeypatch.setattr(education.db, "transaction", transaction)
    copy = AsyncMock(return_value=1)
    monkeypatch.setattr(native, "native_copy_record_batch", copy)
    stage = clinical.make_class(DoctorClinicianAddress, "copyunit")
    clinical_row = clinical.doctor_address_row(
        {"NPI": NPI, "adr_ln_1": "1 Example Street", "zip_code": "10001"},
        datetime(2026, 1, 1),
    )
    await clinical._copy_clinical_rows([clinical_row], stage)
    columns = tuple(stage.__table__.columns.keys())
    assert copy.await_args.kwargs["columns"] == columns
    actual_by_column = dict(zip(columns, copy.await_args.kwargs["records"][0], strict=True))
    assert actual_by_column == {**clinical_row, "latitude": None, "longitude": None, "address_key": None}
    copy.side_effect = TypeError("native COPY failed")
    with pytest.raises(TypeError, match="native COPY failed"):
        await clinical._copy_clinical_rows([clinical_row], stage)
    assert copy.await_count == 2


@pytest.mark.asyncio
async def test_sidecar_copy_splits_native_bytes_and_refuses_an_oversized_single_row(monkeypatch):
    from contextlib import asynccontextmanager

    from process import reference_family_archive as native

    session = SimpleNamespace(execute=AsyncMock())

    @asynccontextmanager
    async def transaction():
        yield session

    monkeypatch.setattr(education.db, "transaction", transaction)
    copy = AsyncMock(return_value=1)
    monkeypatch.setattr(native, "native_copy_record_batch", copy)
    stage = SimpleNamespace(
        __table__=SimpleNamespace(columns={"source_json": None}, schema="mrf"),
        __tablename__="cms_doctor_education_copyunit",
    )
    wide_row_by_column = {"source_json": {"value": "x" * (8 * 1024 * 1024)}}
    await education._copy_sidecar_rows([wide_row_by_column, wide_row_by_column], stage)
    assert copy.await_count == 2
    assert all(len(call.kwargs["records"]) == 1 for call in copy.await_args_list)
    copy.reset_mock()
    with pytest.raises(ValueError, match="copy_row_bytes_invalid"):
        await education._copy_sidecar_rows([{"source_json": {"value": "x" * (16 * 1024 * 1024)}}], stage)
    copy.assert_not_awaited()
