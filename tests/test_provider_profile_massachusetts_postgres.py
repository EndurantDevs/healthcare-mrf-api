# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Native snapshot and visible-evidence proofs in exactly owned test schemas."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime
import os
from types import SimpleNamespace
import uuid

import pytest
from sqlalchemy import MetaData, text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from api import provider_profile as profile_api
from api import provider_profile_cms as cms_api
from api import provider_profile_states as state_api
from db.connection import Database
from process.massachusetts_profile_rows import SCHEMA_VERSION, SOURCE_KEY


NPI = 1000000004


@asynccontextmanager
async def _database(monkeypatch):
    database_dsn = os.getenv("HLTHPRT_PROVIDER_DIRECTORY_PROFILE_POSTGRES_DSN")
    if not database_dsn:
        pytest.skip("set the profile PostgreSQL DSN for native MA loader tests")
    schema = f"ma_profile_api_{uuid.uuid4().hex}"
    engine = create_async_engine(make_url(database_dsn).set(drivername="postgresql+asyncpg"))
    database = Database(engine=engine, session_factory=async_sessionmaker(engine, expire_on_commit=False))
    metadata = MetaData()
    is_schema_created = False
    try:
        assert "test" in str(await database.scalar("SELECT current_database()")).lower()
        await database.status(f"CREATE SCHEMA {schema}")
        is_schema_created = True
        for module, names in (
            (state_api, ("ProviderProfileSourcePublication", "ProviderProfileImportRun", "ProviderProfileFact")),
            (profile_api, ("ProviderProfileProjection",)),
            (cms_api, ("CMSDoctorEducation",)),
        ):
            monkeypatch.setattr(module, "db", database)
            for name in names:
                original = getattr(module, name)
                table = original.__table__.to_metadata(metadata, schema=schema)
                monkeypatch.setattr(module, name, SimpleNamespace(__table__=table, __tablename__=table.name))
        async with engine.begin() as connection:
            await connection.run_sync(metadata.create_all)
        yield database, schema
    finally:
        try:
            if is_schema_created:
                await database.status(f"DROP SCHEMA {schema} CASCADE")
                assert await database.scalar("SELECT to_regnamespace(:schema)", schema=schema) is None
        finally:
            await database.disconnect()


def _fact(generation, suffix, *, category, value_by_field):
    record_id = f"{generation}-profile"
    return {
        "fact_id": f"{generation}-{suffix}", "run_id": generation, "npi": NPI,
        "source_record_id": record_id, "logical_fact_key": f"{generation}-{suffix}",
        "category": category, "fact_type": "education_history" if category == "education" else "postgraduate_training",
        "display": f"Reported {suffix}", "value_json": value_by_field, "availability": "available",
        "assertion_type": "self_reported", "verification_status": "not_independently_verified",
        "source_json": {"source_key": SOURCE_KEY, "schema_version": SCHEMA_VERSION,
                        "source_record_id": record_id, "source_path": f"educationAndTrainings.{suffix}",
                        "raw_fields": {"assertion": suffix, "reported_value": value_by_field}, "quality_flags": []},
        "sensitive": False, "public_default": True,
    }


async def _seed_generation(database, generation, *, status="completed", school="Example Medical School", portfolio=False):
    manifest_by_field = {
        "source": {"source_key": SOURCE_KEY, "source_kind": "state_regulator", "jurisdiction": "MA",
                   "agency": "Massachusetts Board of Registration in Medicine", "coverage_scope": "full_physician_license",
                   "registry_generation": generation},
        "categories": ["education", "training"], "max_providers": None,
        "full_cohort_licenses": 1, "requested_licenses": 1, "cohort_sha256": "a" * 64,
        "expected_current_run_id": None, "resume_from": None,
    }
    if portfolio:
        manifest_by_field["categories"].extend(["certifications", "specialties"])
    await database.insert(state_api.ProviderProfileImportRun.__table__).values({
        "run_id": generation, "source_key": SOURCE_KEY, "jurisdiction": "MA",
        "schema_version": SCHEMA_VERSION, "status": status, "source_manifest": manifest_by_field,
        "metrics": {"published": True, "acquisition_complete": True, "responses": 1, "transport_failures": 0},
    }).status()
    facts = [
        _fact(generation, "school", category="education", value_by_field={"institution": school, "graduation_year": 2001}),
        _fact(generation, "training", category="training", value_by_field={"institution": "Example Hospital", "program_type": "Resident", "attendance_end": None}),
    ]
    await database.insert(state_api.ProviderProfileFact.__table__).values(facts).status()
    return facts


async def _point_to(database, generation):
    await database.insert(state_api.ProviderProfileSourcePublication.__table__).values({
        "source_key": SOURCE_KEY, "current_run_id": generation, "published_at": datetime(2026, 9, 8),
    }).status()


async def _wait_for_reader_gate(database, lock_key):
    async with asyncio.timeout(5):
        while not await database.scalar(
            "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory' "
            "AND classid = 0 AND objid = :lock_key AND NOT granted)", lock_key=lock_key,
        ):
            await asyncio.sleep(0.01)


async def test_loader_snapshot_survives_concurrent_pointer_and_fact_change(monkeypatch):
    """Pause the actual JOIN inside PostgreSQL while a second writer commits."""
    async with _database(monkeypatch) as (database, schema):
        old_generation, new_generation = uuid.uuid4().hex, uuid.uuid4().hex
        await _seed_generation(database, old_generation, school="Earlier Medical School")
        await _seed_generation(database, new_generation, school="Later Medical School")
        await _point_to(database, old_generation)
        lock_key = int(uuid.uuid4().hex[:7], 16)
        await database.status(f"ALTER TABLE {schema}.provider_profile_fact RENAME TO fact_rows")
        await database.status(f"CREATE FUNCTION {schema}.read_gate() RETURNS boolean LANGUAGE sql VOLATILE "
                              f"AS 'SELECT pg_advisory_xact_lock({lock_key}); SELECT true'")
        await database.status(f"CREATE VIEW {schema}.provider_profile_fact AS SELECT * FROM {schema}.fact_rows WHERE {schema}.read_gate()")
        query_statements = []
        original_all = database.all

        async def read_all(statement, **parameters):
            query_statements.append(str(statement))
            return await original_all(statement, **parameters)

        monkeypatch.setattr(database, "all", read_all)
        reader = None
        try:
            async with database.engine.begin() as writer:
                await writer.execute(text("SELECT pg_advisory_xact_lock(:lock_key)"), {"lock_key": lock_key})
                reader = asyncio.create_task(state_api.fetch_massachusetts_profile_projection(NPI))
                await _wait_for_reader_gate(database, lock_key)
                await writer.execute(text(f"UPDATE {schema}.provider_profile_source_publication SET current_run_id = :generation WHERE source_key = :source_key"), {"generation": new_generation, "source_key": SOURCE_KEY})
                await writer.execute(text(f"DELETE FROM {schema}.fact_rows WHERE run_id = :generation"), {"generation": old_generation})
            old_projection = await asyncio.wait_for(reader, timeout=5)
            assert len(query_statements) == 1
            assert old_projection["generation_id"] == old_projection["source"]["registry_generation"] == old_generation
            assert old_projection["categories"]["education"]["items"][0]["value"]["institution"] == "Earlier Medical School"
            assert all(source_record["fact_id"].startswith(old_generation) for source_record in old_projection["evidence"]["records"])
            new_projection = await state_api.fetch_massachusetts_profile_projection(NPI)
            assert len(query_statements) == 2
            assert new_projection["generation_id"] == new_projection["source"]["registry_generation"] == new_generation
            assert new_projection["categories"]["education"]["items"][0]["value"]["institution"] == "Later Medical School"
            assert await database.scalar(f"SELECT count(*) FROM {schema}.fact_rows WHERE run_id = :generation", generation=old_generation) == 0
        finally:
            if reader is not None and not reader.done():
                reader.cancel()
                await asyncio.gather(reader, return_exceptions=True)


@pytest.mark.parametrize("status", ["running", "failed"])
async def test_loader_rejects_incomplete_pointed_run_in_postgresql(monkeypatch, status):
    async with _database(monkeypatch) as (database, _schema):
        generation = uuid.uuid4().hex
        await _seed_generation(database, generation, status=status)
        await _point_to(database, generation)
        with pytest.raises(RuntimeError, match="state_profile_publication_invalid"):
            await state_api.fetch_massachusetts_profile_projection(NPI)


async def test_loader_absent_pointer_and_optional_table_in_postgresql(monkeypatch):
    async with _database(monkeypatch) as (database, schema):
        assert await state_api.fetch_massachusetts_profile_projection(NPI) is None
        generation = uuid.uuid4().hex
        await _seed_generation(database, generation)
        assert await state_api.fetch_massachusetts_profile_projection(NPI) is None
        await _point_to(database, generation)
        assert await state_api.fetch_massachusetts_profile_projection(1000000012) is None
        await database.status(f"DROP TABLE {schema}.provider_profile_source_publication")
        assert await state_api.fetch_massachusetts_profile_projection(NPI) is None


async def _seed_incumbents(database):
    florida_value_by_field = {"institution": "Example Medical School", "graduation_year": 2001}
    await database.insert(profile_api.ProviderProfileProjection.__table__).values({
        "npi": NPI, "generation_id": "florida-generation", "schema_version": "provider-profile/v1",
        "profile_json": {"categories": {"education": {"availability": "available", "items": [{
            "type": "education_history", "display": "Florida school", "value": florida_value_by_field,
            "source_record_id": "florida-school", "sensitive": False, "public_default": True,
        }]}}, "sources": [{"source_key": "florida-mqa", "source_kind": "state_regulator"}]},
        "evidence_json": {"records": [{"source_record_id": "florida-school", "raw_fields": {"school": "Example Medical School"}}]},
        "source_keys": ["florida-mqa"], "published_at": datetime(2026, 9, 8),
    }).status()
    await database.insert(cms_api.CMSDoctorEducation.__table__).values({
        "npi": NPI, "education_key": "cms-school", "medical_school": "Example Medical School",
        "graduation_year": 2001, "generation_id": "cms-generation", "imported_at": datetime(2026, 9, 8),
        "source_json": {"source_key": "cms-doctors", "generation_id": "cms-generation", "dataset_id": "mj5m-pzi6",
                        "source_url": "https://data.cms.gov/example.csv", "content_sha256": "a" * 64,
                        "downloaded_at": "2026-09-08T00:00:00", "raw_fields": {"Med_sch": "Example Medical School", "Grd_yr": "2001"}},
    }).status()


async def test_native_composition_preserves_sources_and_isolates_evidence(monkeypatch):
    async with _database(monkeypatch) as (database, _schema):
        generation = uuid.uuid4().hex
        await _seed_generation(database, generation)
        await _point_to(database, generation)
        await _seed_incumbents(database)
        projection = await profile_api.fetch_provider_profile_projection(NPI)
        for category in ("education", "training"):
            profile = profile_api.compose_provider_profile(NPI, state_projection=projection, fhir_profile=None,
                                                          requested_categories=[category], page_category=category, page_limit=1)
            evidence = profile_api.compose_provider_profile_evidence(state_projection=projection, fhir_evidence=None,
                                                                    provider_profile=profile, page_category=category)
            assert profile["source_generations"] == {"state_regulator": "florida-generation", "cms_doctors": "cms-generation", SOURCE_KEY: generation}
            public_item, = profile["categories"][category]["items"]
            ma_record, = evidence["sources"][SOURCE_KEY]["records"]
            assert ma_record["source_record_id"] in public_item["source_record_ids"]
            assert ma_record["profile_source_record_id"] == f"{generation}-profile"
            assert ma_record["fact_id"] == f"{generation}-{'school' if category == 'education' else 'training'}"
            assert ma_record["raw_fields"]["assertion"] == ("school" if category == "education" else "training")
            assert len(evidence["sources"]["state_regulator"]["records"]) == (1 if category == "education" else 0)
            assert len(evidence["sources"]["cms_doctors"]["records"]) == (1 if category == "education" else 0)
            if category == "education":
                assert public_item["assertion_count"] == 3
        assert (await profile_api.fetch_state_profile_projection(NPI))["generation_id"] == "florida-generation"
        assert (await cms_api.fetch_cms_education_projection(NPI))["generation_id"] == "cms-generation"


async def test_native_portfolio_scope_preserves_incumbent_education_and_source_generations(monkeypatch):
    from tests.test_massachusetts_profile_portfolio import _parse

    async with _database(monkeypatch) as (database, schema):
        legacy_generation, richer_generation = uuid.uuid4().hex, uuid.uuid4().hex
        await _seed_generation(database, legacy_generation)
        await _seed_generation(database, richer_generation, portfolio=True)
        await _point_to(database, legacy_generation)
        await _seed_incumbents(database)
        before = await profile_api.fetch_provider_profile_projection(NPI)
        legacy_profile = profile_api.compose_provider_profile(NPI, state_projection=before, fhir_profile=None)
        assert legacy_profile["categories"]["certifications"]["availability"] == "unavailable"
        _, parsed_facts = _parse(evidence={
            "run_id": richer_generation, "artifact_id": "synthetic-artifact", "row_number": 1,
            "source_url": "https://example.test/profile", "downloaded_at": "2026-09-08T00:00:00Z", "content_sha256": "a" * 64,
        })
        portfolio_facts = [fact for fact in parsed_facts if fact["category"] in {"certifications", "specialties"}]
        await database.insert(state_api.ProviderProfileFact.__table__).values(portfolio_facts).status()
        assert (await state_api.fetch_massachusetts_profile_projection(NPI))["generation_id"] == legacy_generation
        await database.status(f"UPDATE {schema}.provider_profile_source_publication SET current_run_id=:generation", generation=richer_generation)
        after = await profile_api.fetch_provider_profile_projection(NPI)
        profile = profile_api.compose_provider_profile(NPI, state_projection=after, fhir_profile=None)
        assert profile["source_generations"] == {"state_regulator": "florida-generation", "cms_doctors": "cms-generation", SOURCE_KEY: richer_generation}
        school, = profile["categories"]["education"]["items"]
        assert school["assertion_count"] == 3 and school["value"] == legacy_profile["categories"]["education"]["items"][0]["value"]
        assert profile["generation_id"] != legacy_profile["generation_id"]
        for category in ("certifications", "specialties"):
            assert profile["categories"][category]["availability"] == "available"
            page = profile_api.compose_provider_profile(NPI, state_projection=after, fhir_profile=None,
                                                       requested_categories=[category], page_category=category, page_limit=1)
            evidence = profile_api.compose_provider_profile_evidence(state_projection=after, fhir_evidence=None,
                                                                     provider_profile=page, page_category=category)
            source_record, = evidence["sources"][SOURCE_KEY]["records"]
            assert source_record["source_record_id"] in page["categories"][category]["items"][0]["source_record_ids"]
            assert source_record["run_id"] == richer_generation
            assert evidence["sources"]["state_regulator"]["records"] == evidence["sources"]["cms_doctors"]["records"] == []
