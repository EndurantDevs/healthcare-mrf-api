# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Verify Kentucky and Massachusetts publication isolation in native PostgreSQL."""

import asyncio
import copy
from datetime import datetime
import uuid

import pytest
from sqlalchemy import text

from api import provider_profile as profile_api
from api import provider_profile_states as state_api
from process.kentucky_profile_rows import SCHEMA_VERSION, SOURCE_KEY
from tests.test_provider_profile_kentucky import NPI, _row
from tests.test_kentucky_profile_portfolio import _published_rows
from tests.test_provider_profile_massachusetts_postgres import (
    _database, _point_to, _seed_generation, _seed_incumbents, _wait_for_reader_gate,
)


async def _seed_kentucky(database, generation, *, school="Example Medical School", status="completed", extra_rows=()):
    """Store real parser output using the unchanged retained models."""
    row = _row(generation=generation, school=school)
    await database.insert(state_api.ProviderProfileImportRun.__table__).values({
        "run_id": generation, "source_key": SOURCE_KEY, "jurisdiction": "KY", "schema_version": SCHEMA_VERSION,
        "status": status, "source_manifest": row["source_manifest"],
    }).status()
    columns = set(state_api.ProviderProfileFact.__table__.columns.keys())
    await database.insert(state_api.ProviderProfileFact.__table__).values([
        {field: value for field, value in source_row.items() if field in columns} for source_row in (row, *extra_rows)
    ]).status()
    return row


async def _point_to_kentucky(database, generation):
    await database.insert(state_api.ProviderProfileSourcePublication.__table__).values({
        "source_key": SOURCE_KEY, "current_run_id": generation, "published_at": datetime(2026, 9, 8),
    }).status()


async def _gate_fact_reads(database, schema, lock_key):
    await database.status(f"ALTER TABLE {schema}.provider_profile_fact RENAME TO fact_rows")
    await database.status(f"CREATE FUNCTION {schema}.read_gate() RETURNS boolean LANGUAGE sql VOLATILE "
                          f"AS 'SELECT pg_advisory_xact_lock({lock_key}); SELECT true'")
    await database.status(f"CREATE VIEW {schema}.provider_profile_fact AS SELECT * FROM {schema}.fact_rows WHERE {schema}.read_gate()")


async def test_one_snapshot_preserves_both_sources_during_atomic_pointer_rotation(monkeypatch):
    """Both pointers and their facts come from the same SQL statement snapshot."""
    async with _database(monkeypatch) as (database, schema):
        generation_by_label = {label: uuid.uuid4().hex for label in ("ma_old", "ma_new", "ky_old", "ky_new")}
        for version in ("old", "new"):
            await _seed_generation(database, generation_by_label[f"ma_{version}"], school=f"MA {version} School")
            await _seed_kentucky(database, generation_by_label[f"ky_{version}"], school=f"KY {version} School")
        await _point_to(database, generation_by_label["ma_old"])
        await _point_to_kentucky(database, generation_by_label["ky_old"])
        lock_key = int(uuid.uuid4().hex[:7], 16)
        await _gate_fact_reads(database, schema, lock_key)
        original_all = database.all
        query_statements = []

        async def read_all(statement, **parameters):
            query_statements.append(str(statement))
            return await original_all(statement, **parameters)

        monkeypatch.setattr(database, "all", read_all)
        reader = None
        try:
            async with database.engine.begin() as writer:
                await writer.execute(text("SELECT pg_advisory_xact_lock(:lock_key)"), {"lock_key": lock_key})
                reader = asyncio.create_task(state_api.fetch_additional_state_profile_projections(NPI))
                await _wait_for_reader_gate(database, lock_key)
                for prefix, source_key in (("ma", state_api.MASSACHUSETTS_SOURCE_KEY), ("ky", SOURCE_KEY)):
                    await writer.execute(text(f"UPDATE {schema}.provider_profile_source_publication SET current_run_id = :generation WHERE source_key = :source_key"),
                                         {"generation": generation_by_label[f"{prefix}_new"], "source_key": source_key})
                    await writer.execute(text(f"DELETE FROM {schema}.fact_rows WHERE run_id = :generation"), {"generation": generation_by_label[f"{prefix}_old"]})
            previous = await asyncio.wait_for(reader, timeout=5)
            current = await state_api.fetch_additional_state_profile_projections(NPI)
            assert len(query_statements) == 2
            for projections, version in ((previous, "old"), (current, "new")):
                assert [projection["source"]["source_key"] for projection in projections] == [state_api.MASSACHUSETTS_SOURCE_KEY, SOURCE_KEY]
                for projection, prefix in zip(projections, ("ma", "ky")):
                    assert projection["generation_id"] == generation_by_label[f"{prefix}_{version}"]
                    assert projection["categories"]["education"]["items"][0]["value"]["institution"] == f"{prefix.upper()} {version} School"
        finally:
            if reader is not None and not reader.done():
                reader.cancel()
                await asyncio.gather(reader, return_exceptions=True)


@pytest.mark.parametrize("status", ["running", "failed"])
async def test_kentucky_incomplete_publication_is_not_served(monkeypatch, status):
    async with _database(monkeypatch) as (database, _schema):
        await _seed_generation(database, "ma-complete")
        await _point_to(database, "ma-complete")
        await _seed_kentucky(database, "ky-incomplete", status=status)
        await _point_to_kentucky(database, "ky-incomplete")
        with pytest.raises(RuntimeError, match="publication_invalid"):
            await state_api.fetch_additional_state_profile_projections(NPI)


async def test_pointer_and_fact_sources_cannot_be_crossed(monkeypatch):
    async with _database(monkeypatch) as (database, schema):
        await _seed_generation(database, "ma-generation")
        await _point_to(database, "ma-generation")
        await _seed_kentucky(database, "ky-generation")
        await _point_to_kentucky(database, "ma-generation")
        projection, = await state_api.fetch_additional_state_profile_projections(NPI)
        assert projection["source"]["source_key"] == state_api.MASSACHUSETTS_SOURCE_KEY
        await database.status(f"UPDATE {schema}.provider_profile_source_publication SET current_run_id = 'ky-generation' WHERE source_key = :source_key", source_key=SOURCE_KEY)
        assert len(await state_api.fetch_additional_state_profile_projections(NPI)) == 2
        row = _row(generation="ky-generation")
        wrong_evidence_by_field = {**row["source_json"], "source_key": state_api.MASSACHUSETTS_SOURCE_KEY}
        await database.update(state_api.ProviderProfileFact.__table__).where(
            state_api.ProviderProfileFact.__table__.c.fact_id == row["fact_id"],
        ).values(source_json=wrong_evidence_by_field).status()
        with pytest.raises(RuntimeError, match="source_mismatch"):
            await state_api.fetch_additional_state_profile_projections(NPI)


async def test_native_four_source_corroboration_preserves_incumbents_and_page_evidence(monkeypatch):
    """A published Kentucky generation adds support while incumbent source data stays intact."""
    async with _database(monkeypatch) as (database, _schema):
        await _seed_generation(database, "ma-generation")
        await _point_to(database, "ma-generation")
        await _seed_incumbents(database)
        incumbent = await profile_api.fetch_provider_profile_projection(NPI)
        original = copy.deepcopy(incumbent)
        second = _row(generation="ky-generation", license_number="C0008", school="Second Medical School", year="2006")
        hidden = _row(generation="ky-generation", license_number="C0009", school="Hidden School", sensitive=True)
        await _seed_kentucky(database, "ky-generation", extra_rows=(second, hidden))
        assert await profile_api.fetch_provider_profile_projection(NPI) == incumbent
        await _point_to_kentucky(database, "ky-generation")
        projection = await profile_api.fetch_provider_profile_projection(NPI)
        profile = profile_api.compose_provider_profile(NPI, state_projection=projection, fhir_profile=None)
        assert profile["source_generations"] == {"state_regulator": "florida-generation", "cms_doctors": "cms-generation",
                                                state_api.MASSACHUSETTS_SOURCE_KEY: "ma-generation", SOURCE_KEY: "ky-generation"}
        assert sorted(profile_item["assertion_count"] for profile_item in profile["categories"]["education"]["items"]) == [1, 4]
        assert profile["categories"]["training"]["items"] and profile["categories"]["professional_experience"]["items"] == []
        assert profile["composer_version"] == "provider-profile-composer/v10"
        for category, offset in (("education", 0), ("education", 1), ("training", 0)):
            page = profile_api.compose_provider_profile(NPI, state_projection=projection, fhir_profile=None,
                                                       requested_categories=[category], page_category=category, page_limit=1, page_offset=offset)
            evidence = profile_api.compose_provider_profile_evidence(state_projection=projection, fhir_evidence=None, provider_profile=page, page_category=category)
            profile_item, = page["categories"][category]["items"]
            assert {source_record["source_record_id"] for source_evidence in evidence["sources"].values() for source_record in source_evidence["records"]} == set(profile_item["source_record_ids"])
            assert len(evidence["sources"][SOURCE_KEY]["records"]) == (1 if category == "education" else 0)
        assert incumbent == original
        assert projection["evidence"] == original["evidence"] and projection["cms_evidence"] == original["cms_evidence"]
        assert projection["additional_state_evidence"][state_api.MASSACHUSETTS_SOURCE_KEY] == original["additional_state_evidence"][state_api.MASSACHUSETTS_SOURCE_KEY]


async def test_native_portfolio_has_exact_page_evidence(monkeypatch):
    async with _database(monkeypatch) as (database, _schema):
        fact_rows = _published_rows()
        generation = fact_rows[0]["generation_id"]
        await _seed_kentucky(database, generation, extra_rows=fact_rows[1:])
        await _point_to_kentucky(database, generation)
        with pytest.raises(RuntimeError, match="categories_invalid"):
            await state_api.fetch_additional_state_profile_projections(NPI)
        run_table = state_api.ProviderProfileImportRun.__table__
        await database.update(run_table).where(run_table.c.run_id == generation).values(
            source_manifest=fact_rows[0]["source_manifest"],
        ).status()
        projection = await profile_api.fetch_provider_profile_projection(NPI)
        for category in ("specialties", "services"):
            page = profile_api.compose_provider_profile(NPI, state_projection=projection, fhir_profile=None,
                requested_categories=[category], page_category=category, page_limit=1)
            evidence = profile_api.compose_provider_profile_evidence(
                state_projection=projection, fhir_evidence=None, provider_profile=page, page_category=category)
            profile_item, = page["categories"][category]["items"]
            source_record, = evidence["sources"][SOURCE_KEY]["records"]
            fact = next(fact for fact in fact_rows if fact["category"] == category)
            assert source_record["fact_id"] == fact["fact_id"]
            assert source_record["raw_fields"] == fact["source_json"]["raw_fields"]
            assert profile_item["value"] == fact["value_json"]
            assert profile_item["source_record_ids"] == [source_record["source_record_id"]]
