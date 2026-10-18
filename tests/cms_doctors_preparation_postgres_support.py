# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Small real Doctors tables under the existing disposable common-receipt fixture."""

import asyncio
import csv
import hashlib
import importlib
import os
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.schema import CreateTable

from api import provider_profile_snapshot as snapshot
from db.connection import Database
from process import cms_doctors_preparation as preparation
from process import provider_directory_generic_source_policy as source_policy
from process import reference_family_result_generation as generation
from process.ext import address_canon
from tests import cms_npd_admission_postgres_support as admission
from tests import test_network_cms_registry_source_pair_postgres as source_pair
from tests import test_provider_directory_cms_serving_receipt_postgres as receipt_fixture
from tests.cms_doctors_component_postgres_support import (
    doctors_component_source,
    doctors_source_component_database,
    prepare_doctors_component,
)
from tests.provider_directory_source_postgres_support import source_component_templates
from tests.provider_profile_snapshot_postgres_support import grant_profile_reader, profile_reader

native = importlib.import_module("process.cms_doctors")


@pytest_asyncio.fixture(scope="module", loop_scope="module", autouse=True)
async def doctors_source_template():
    """Reuse closed migrations while each test provisions fresh original Source custody."""
    async with source_component_templates():
        yield


GEO_DOCTORS_SOURCE_ROW = {
    "NPI": "1000000020",
    "adr_ln_1": "3 Test Street",
    "adr_ln_2": "Suite 3",
    "citytown": "prepared",
    "state": "MI",
    "zip_code": "48104",
}
GEO_TARGETS = {
    "nppes": (1000000004, "1 Test Street", None, "48104", 1, True, True),
    "mrf": (1000000012, "2 Test Street", None, "48104", 2, True, True),
    "cms": (1000000020, "3 Test Street", "Suite 3", "48104", 3, True, True),
    "none": (1000000038, "4 Test Street", None, "48105", 0, False, False),
    "cms-anchor": (1000000020, "3 Test Street", "Suite 5", "48104", 1, True, True),
}


def geo_target_expectations():
    """Derive delivery, premise, and keyed practice identities through native canonicalization."""
    expected_by_label = {}
    for name, (npi, line1, line2, zipcode, evidence, identity, point) in GEO_TARGETS.items():
        fields = (line1, line2, "prepared", "MI", zipcode, "US")
        key = str(address_canon.address_key_v1(*fields))
        location_identity = "|".join(("v1", "npi", str(npi), str(npi), "", "", "4", "base", key))
        expected_by_label[name] = {
            "npi": npi,
            "address_key": key,
            "premise_key": str(address_canon.key_from_identity(address_canon.premise_identity_key_v1(*fields))),
            "location_key": hashlib.sha256(location_identity.encode()).hexdigest(),
            "geo_evidence_source_id": evidence,
            "geo_identity_coherent": identity,
            "geo_point_coherent": point,
        }
    return expected_by_label


async def seed_geo_native_inputs(database, monkeypatch):
    """Seed raw inputs before the existing bootstrap publishes or enrolls any generation."""
    statements = (
        "INSERT INTO mrf.npi(npi,entity_type_code) VALUES(1000000004,1),(1000000012,1),(1000000020,1),(1000000038,1)",
        """INSERT INTO mrf.npi_address(npi,type,checksum,first_line,second_line,city_name,state_name,
        postal_code,country_code,lat,long,date_added) VALUES
        (1000000004,'practice',1,'1 Test Street',NULL,'prepared','MI','48104','US',42,-83,'2026-08-25'),
        (1000000038,'practice',4,'4 Test Street',NULL,'prepared','MI','48105','US',42,-83,NULL),
        (1000000020,'practice',5,'3 Test Street','Suite 5','prepared','MI','48104','US',42,-83,'2026-08-25')""",
        """INSERT INTO mrf.mrf_address(npi,type,checksum,first_line,second_line,city_name,state_name,
        postal_code,country_code,lat,long,date_added,source_import_ids,source_import_dates,source_issuer_names) VALUES
        (1000000012,'practice',2,'2 Test Street',NULL,'prepared','MI','48104','US',42,-83,'2026-08-25',
        ARRAY['mrf-v1'],ARRAY['2026-08-25'::date],ARRAY['ISSUER A','ISSUER B']),
        (1000000020,'practice',3,'3 Test Street','Suite 3','prepared','MI','48104','US',42,-83,'2026-08-25',
        ARRAY['mrf-v1'],ARRAY['2026-08-25'::date],ARRAY['ISSUER A'])""",
        "INSERT INTO mrf.geo_zip_lookup(zip_code,state,state_name) VALUES('48104','MI','MICHIGAN')",
        "INSERT INTO tiger.zip_state(zip,stusps,statefp) VALUES('48104','MI','26')",
        """INSERT INTO tiger.zcta5(zcta5ce,statefp,the_geom) VALUES('48104','26',ST_Multi(ST_GeomFromText(
        'POLYGON((-83.2 41.8,-82.8 41.8,-82.8 42.2,-83.2 42.2,-83.2 41.8))',4269)))""",
    )
    async with database.transaction():
        for statement in statements:
            await database.status(statement)
    address_fields_by_name = {
        "first_line": "first_line",
        "second_line": "second_line",
        "city": "city_name",
        "state": "state_name",
        "zip": "postal_code",
        "country": "'US'",
    }
    with monkeypatch.context() as binding:
        binding.setattr(address_canon, "db", database)
        for table, bit, priority in (("npi_address", 1, 0), ("mrf_address", 16, 5)):
            await address_canon.stamp_address_keys(table, address_fields_by_name, schema="mrf", shards=1)
            await address_canon.resolve_into_archive(
                table,
                address_fields_by_name,
                source_bit=bit,
                priority=priority,
                schema="mrf",
                zip_restore_complete=True,
            )


async def _insert_marker(session, model, marker):
    """Populate the real address, education, and physical group row shapes."""
    fields_by_table = {
        "doctor_clinician_address": {"address_checksum": 1, "city": marker},
        "cms_doctor_education": {
            "education_key": "assertion",
            "medical_school": marker,
            "generation_id": "a" * 64,
            "source_json": {},
            "imported_at": datetime(2026, 1, 1),
        },
        "cms_doctor_group_site": {
            "row_number": 1,
            "org_pac_id": marker,
            "generation_id": "a" * 64,
            "source_json": {},
            "observed_at": datetime(2026, 1, 1),
        },
    }
    await session.execute(model.__table__.insert().values(npi=1000000004, **fields_by_table[model.__main_table__]))


async def stage_family(database, schema, *, source_row=None):
    """Create three populated, unlogged stage tables without their serving indexes."""
    if getattr(database, "_doctors_source_component", None) is not None:
        return await _stage_source_family(database, schema, source_row=source_row)
    import_date = uuid4().hex
    stage_names = []
    async with database.transaction() as session:
        for model in preparation._models():
            stage = native.make_class(model, import_date)
            stage_names.append(stage.__tablename__)
            await session.execute(CreateTable(stage.__table__))
            await session.execute(text(f'ALTER TABLE "{schema}"."{stage.__tablename__}" SET UNLOGGED'))
            await _insert_marker(session, stage, "prepared")
    await grant_profile_reader(database, schema, stage_names)
    return {
        "import_date": import_date,
        "context": {"run": True, "education_stage_owned": True, "group_site_stage_owned": True},
    }


async def _stage_source_family(database, schema, *, source_row=None):
    """Use the actual retained CSV parsers, three COPY paths and map preparation."""
    import_date = uuid4().hex
    source_path = Path(os.environ["HLTHPRT_CMS_DOCTORS_ARTIFACT_ROOT"]) / (import_date + ".csv")
    source_fields_by_name = {
        "NPI": "1000000004",
        "ind_enrl_id": "enrollment-1",
        "org_pac_id": "prepared",
        "adrs_id": "site-1",
        "Facility Name": "Example Group",
        "num_org_mem": "1",
        "Med_sch": "prepared",
        "Grd_yr": "2001",
        "adr_ln_1": "1 Example Street",
        "citytown": "prepared",
        "state": "NY",
        "zip_code": "10001",
        **(source_row or {}),
    }
    with source_path.open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=source_fields_by_name)
        writer.writeheader()
        writer.writerow(source_fields_by_name)
    context_by_key = {"import_date": import_date, "context": {"run": True}}
    async with database.transaction():
        stage = await native.create_doctors_stage(database, native.DoctorClinicianAddress, import_date, schema)
        await native._stage_doctors_sidecars(source_path, "https://example.test/doctors.csv", context_by_key, {}, False)
        assert (
            await native._import_doctors_source(
                source_path,
                ctx=context_by_key,
                task={},
                stage_cls=stage,
                batch_size=5000,
                test_mode=False,
                test_row_limit=5000,
            )
            == 1
        )
    return context_by_key


def _bind_doctors_database(monkeypatch, database):
    for name in (
        "cms_doctors",
        "cms_doctors_education",
        "cms_doctors_groups",
        "cms_doctors_organizations",
        "cms_doctors_sites",
        "ext.utils",
    ):
        monkeypatch.setattr(importlib.import_module("process." + name), "db", database)
    monkeypatch.setattr(native, "ensure_database", AsyncMock())
    monkeypatch.setattr(native, "DEFAULT_MIN_ROWS", 1)
    monkeypatch.setattr(importlib.import_module("process.cms_doctors_education"), "MIN_EDUCATION_ROWS", 1)


@asynccontextmanager
async def doctors_database(monkeypatch, *, cms_active=True, before_provision=None):
    """Use authentic Source custody for both ordinary and common-receipt publication."""
    if not cms_active:
        async with doctors_source_component_database(monkeypatch, before_provision=before_provision) as component:
            database = component.database
            _bind_doctors_database(monkeypatch, database)
            yield SimpleNamespace(
                engine=component.admin_engine,
                schema=component.policy.schema,
                database=database,
                initial=None,
                component=component,
            )
        return
    async with _current_common_doctors_database(monkeypatch, before_provision=before_provision) as fixture:
        yield fixture


@asynccontextmanager
async def _current_common_doctors_database(monkeypatch, *, before_provision=None):
    """Compose real admitted CMS/common receipt setup with the existing Doctors producer."""
    async with admission.protected_admission_database(monkeypatch, before_provision=before_provision) as administrator:
        component = administrator._source_component
        async with source_policy.bind_generic_source_transaction_factory(component.factory):
            await prepare_doctors_component(component)
        directory, acquired = admission.retained_release(component.artifacts, include_missing_network=False)
        with admission.release_probe_client(directory) as client:
            admitted = await admission.run_protected_admission(
                administrator, monkeypatch, "doctors-common-receipt", directory, acquired, client
            )
        await source_pair._publish_pair(administrator, admitted, monkeypatch)
        async with administrator.session() as session:
            initial = await receipt_fixture.receipts.read_current_receipt(session, component.policy.schema)
            assert initial is not None
            assert all(
                initial["payload"]["cms"][field] == admitted["cms_serving_candidate"]["desired_cms_dataset"][field]
                for field in receipt_fixture._PIN
            )
        database = component.database
        _bind_doctors_database(monkeypatch, database)
        async with source_policy.bind_generic_source_transaction_factory(component.factory):
            async with database.transaction() as session:
                component.authority = await generation.read_reference_family_result_generation_authority(
                    session, importer_id="cms-doctors", schema_name=component.policy.schema
                )
            yield SimpleNamespace(
                engine=component.admin_engine,
                schema=component.policy.schema,
                database=database,
                initial=initial,
                component=component,
            )


@asynccontextmanager
async def _legacy_scalar_doctors_database(monkeypatch):
    """Preserve legacy scalar receipt coverage; this does not prove the composite SOURCE producer."""
    async with receipt_fixture._database(monkeypatch, install_receipt=False) as (engine, schema):
        database = Database(engine=engine, session_factory=async_sessionmaker(engine, expire_on_commit=False))
        for module in (
            native,
            importlib.import_module("process.cms_doctors_education"),
            importlib.import_module("process.cms_doctors_groups"),
        ):
            monkeypatch.setattr(module, "db", database)
        monkeypatch.setattr(native, "ensure_database", AsyncMock())
        monkeypatch.setattr(native, "_prepare_cms_doctors_sources", AsyncMock(side_effect=lambda *_args: {"rows": 1}))
        async with database.transaction() as session:
            for model in preparation._models():
                monkeypatch.setattr(model.__table__, "schema", schema)
                await session.execute(text(f'DROP TABLE "{schema}"."{model.__tablename__}"'))
                await session.execute(CreateTable(model.__table__))
                await _insert_marker(session, model, "incumbent")
            for model in preparation._models():
                await native._create_stage_indexes(model, schema)
            connection = await session.connection()
            await connection.run_sync(lambda sync: receipt_fixture._apply(sync, "20260930100000"))
            await connection.run_sync(lambda sync: receipt_fixture._apply(sync, "20260930130000"))
        async with profile_reader(database, schema, monkeypatch):
            reader = database._reader_database._reader_login[0]
            async with doctors_component_source(engine, schema, monkeypatch, reader=reader) as component:
                publisher = component.database
                publisher._reader_database = database._reader_database
                for module in (
                    native,
                    importlib.import_module("process.cms_doctors_education"),
                    importlib.import_module("process.cms_doctors_groups"),
                ):
                    monkeypatch.setattr(module, "db", publisher)
                initial_receipt = await _legacy_common_receipt(publisher, schema)
                yield SimpleNamespace(
                    engine=engine, schema=schema, database=publisher, initial=initial_receipt, component=component
                )


async def _legacy_common_receipt(publisher, schema):
    async with publisher.transaction() as session:
        await receipt_fixture._seed_source(session, schema)
        await receipt_fixture._seed_profile(session, schema)
        await receipt_fixture._advance_native(session, schema)
        receipt_payload = await receipt_fixture._payload(session, schema)
        receipt_id = await receipt_fixture.receipts.append_serving_receipt(session, schema, receipt_payload)
        return {"receipt_id": receipt_id, "payload": receipt_payload}


async def authority(fixture):
    """Read the exact current native authority through a separate completed transaction."""
    async with fixture.database.transaction() as session:
        return await generation.read_reference_family_result_generation_authority(
            session, importer_id="cms-doctors", schema_name=fixture.schema
        )


async def doctors_snapshot(fixture, *, entered=None, release=None):
    """Read all three markers under the actual response lock and authority checks."""
    async with snapshot.provider_profile_read_snapshot(fixture.database, fixture.schema) as session:
        markers = tuple(
            [
                await session.scalar(text(f'SELECT {column} FROM "{fixture.schema}".{table}'))
                for table, column in (
                    ("doctor_clinician_address", "city"),
                    ("cms_doctor_education", "medical_school"),
                    ("cms_doctor_group_site", "org_pac_id"),
                )
            ]
        )
        incumbent = session.info["provider_profile_native_authorities"]["cms-doctors"]
        if entered is not None:
            entered.set()
            await release.wait()
        return markers, incumbent


async def pending_publisher_locks(fixture, publisher):
    """Observe a real queued writer, or the publisher's completed fail-fast attempts."""
    for _poll in range(200):
        async with fixture.engine.connect() as observer:
            pending_locks = (
                await observer.execute(
                    text(
                        "SELECT locks.pid, pg_blocking_pids(locks.pid) FROM pg_locks AS locks "
                        "JOIN pg_class AS relation ON relation.oid=locks.relation "
                        "JOIN pg_namespace AS namespace ON namespace.oid=relation.relnamespace "
                        "WHERE namespace.nspname=:schema AND mode='AccessExclusiveLock' AND NOT granted"
                    ),
                    {"schema": fixture.schema},
                )
            ).all()
        if pending_locks or publisher.done():
            return pending_locks
        await asyncio.sleep(0.01)
    raise AssertionError("publisher neither completed nor reached a relation lock")


async def stage_oids(fixture, ctx):
    """Observe retained stage identities without confusing them with published authority."""
    async with fixture.database.transaction() as session:
        return await preparation._stage_inventory(session, fixture.schema, ctx["import_date"])


async def append_common_receipt(session, fixture, prior):
    """Advance scalar dependent stand-ins and the real receipt around the native Doctors apply."""
    await receipt_fixture._advance_native(session, fixture.schema)
    await session.execute(
        text(f'UPDATE "{fixture.schema}".provider_directory_profile_serving_generation SET generation_id=:generation'),
        {"generation": "pdprofile_" + uuid4().hex},
    )
    payload = await receipt_fixture._payload(session, fixture.schema, prior)
    payload["cms"] = dict(prior["payload"]["cms"])
    receipt_id = await receipt_fixture.receipts.append_serving_receipt(session, fixture.schema, payload)
    return {"receipt_id": receipt_id, "payload": payload}
