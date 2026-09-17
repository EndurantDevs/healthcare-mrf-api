# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Keep Kentucky publication isolated while retaining Massachusetts store behavior."""

import asyncio
from contextlib import asynccontextmanager
from dataclasses import FrozenInstanceError
from datetime import timedelta
import json

import pytest

from process import kentucky_profile_store as kentucky
from process import massachusetts_profile_store as massachusetts
from process import provider_profile_source_store as shared_store
from tests.test_massachusetts_profile_store import _database, _metrics, _run, _seed_run


def _kentucky_run(**options):
    candidate_run = _run(**options)
    candidate_run.update(source_key=kentucky.SOURCE_KEY, schema_version=kentucky.SCHEMA_VERSION, jurisdiction="KY")
    manifest_by_field = candidate_run["source_manifest"]
    manifest_by_field["categories"] = ["education"]
    manifest_by_field["source"] = {"source_key": kentucky.SOURCE_KEY, "source_kind": "state_regulator", "jurisdiction": "KY"}
    return candidate_run


async def _seed_kentucky_payloads(database, run_id, count):
    source_table = kentucky._table(shared_store.ProviderProfileSourceRecord)
    fact_table = kentucky._table(shared_store.ProviderProfileFact)
    await database.status(f"""
        INSERT INTO {source_table} (record_id,run_id,artifact_id,source_key,source_record_key,
          license_number,raw_payload,normalized_payload,matched_npi,match_status)
        SELECT :run_id || lpad(i::text,6,'0'), :run_id, :run_id, :source_key, i::text, i::text,
          json_build_object('html','synthetic retained HTML','profiles',json_build_array(json_build_object(
            'License',i::text,'Name','Alex Example','Medical School','Synthetic School','Year Graduated','2001'))),
          json_build_object('schema_version',CAST(:schema_version AS text),'visibility','public'),1000000000+i,'deterministic'
          FROM generate_series(1,:count) i
    """, run_id=run_id, count=count, source_key=kentucky.SOURCE_KEY, schema_version=kentucky.SCHEMA_VERSION)
    await database.status(f"""
        INSERT INTO {fact_table} (fact_id,run_id,npi,source_record_id,logical_fact_key,category,
          fact_type,display,value_json,availability,assertion_type,verification_status,source_json,sensitive,public_default)
        SELECT record_id,run_id,matched_npi,record_id,record_id,'education','education_history',
          'Synthetic School','{{}}','available','source_reported','not_independently_verified',
          json_build_object('source_key',CAST(:source_key AS text),'schema_version',CAST(:schema_version AS text),
                            'source_record_id',record_id),false,true
          FROM {source_table} WHERE run_id=:run_id
    """, run_id=run_id, source_key=kentucky.SOURCE_KEY, schema_version=kentucky.SCHEMA_VERSION)


async def _seed_kentucky_run(database, **options):
    candidate_run = _kentucky_run(**options)
    await kentucky.claim_run(candidate_run)
    await _seed_kentucky_payloads(database, candidate_run["run_id"], candidate_run["source_manifest"]["requested_licenses"])
    return candidate_run


def test_source_bindings_are_immutable_and_distinct():
    assert kentucky._store is not massachusetts._store
    with pytest.raises(FrozenInstanceError):
        kentucky._store.policy = massachusetts._store.policy
    with pytest.raises(FrozenInstanceError):
        kentucky._store.policy.source_key = massachusetts.SOURCE_KEY
    assert kentucky._manifest(_kentucky_run())["categories"] == ["education"]
    with pytest.raises(ValueError, match="^kentucky_profile_manifest_source_invalid$"):
        kentucky._manifest(_run())
    with pytest.raises(ValueError, match="^massachusetts_profile_manifest_source_invalid$"):
        massachusetts._manifest(_kentucky_run())


@pytest.mark.parametrize("categories", [[], ["training"], ["education", "training"], ["education", "education"]])
def test_kentucky_manifest_is_education_only(categories):
    candidate_run = _kentucky_run()
    candidate_run["source_manifest"]["categories"] = categories
    with pytest.raises(ValueError, match="^kentucky_profile_manifest_categories_invalid$"):
        kentucky._manifest(candidate_run)


@pytest.mark.parametrize("scope", [kentucky.LEGACY_CATEGORIES, kentucky.PROFILE_CATEGORIES])
def test_exact_kentucky_scopes_remain_readable(scope):
    candidate_run = _kentucky_run()
    candidate_run["source_manifest"]["categories"] = list(scope)
    assert kentucky._manifest(candidate_run)["categories"] == list(scope)
    candidate_run["source_manifest"]["source"] = {"source_key": massachusetts.SOURCE_KEY,
                                                "source_kind": "state_regulator", "jurisdiction": "MA"}
    with pytest.raises(ValueError, match="massachusetts_profile_manifest_categories_invalid"):
        massachusetts._manifest(candidate_run)


@pytest.mark.parametrize(("scope", "category", "fact_type", "invalid"), [
    (kentucky.LEGACY_CATEGORIES, "specialties", "specialty", 1),
    (kentucky.LEGACY_CATEGORIES, "services", "practice_type", 1),
    (kentucky.PROFILE_CATEGORIES, "specialties", "specialty", 0),
    (kentucky.PROFILE_CATEGORIES, "services", "practice_type", 0),
    (kentucky.PROFILE_CATEGORIES, "specialties", "board_certification", 1),
])
async def test_facts_match_frozen_scope(monkeypatch, scope, category, fact_type, invalid):
    async with _database(monkeypatch) as database:
        candidate = _kentucky_run(limit=1)
        candidate["source_manifest"]["categories"] = list(scope)
        await kentucky.claim_run(candidate)
        await _seed_kentucky_payloads(database, candidate["run_id"], 1)
        await database.status(f"UPDATE {kentucky._table(shared_store.ProviderProfileFact)} "
                              "SET category=:category,fact_type=:fact_type WHERE run_id=:run_id",
                              category=category, fact_type=fact_type, run_id=candidate["run_id"])
        counts = await kentucky.retained_counts(candidate["run_id"])
        assert counts["invalid_facts"] == invalid
        assert counts["matched_public_providers"] == 0
        if invalid:
            with pytest.raises(RuntimeError, match="retained_integrity_invalid"):
                await kentucky.finish_unpublished_run(candidate["run_id"], _metrics(1))
        else:
            assert not (await kentucky.finish_unpublished_run(candidate["run_id"], _metrics(1)))["published"]


@pytest.mark.parametrize("scope", [kentucky.LEGACY_CATEGORIES, kentucky.PROFILE_CATEGORIES])
async def test_resume_scope_is_locked(monkeypatch, scope):
    async with _database(monkeypatch) as database:
        original = _kentucky_run(limit=1)
        original["source_manifest"]["categories"] = list(scope)
        await kentucky.claim_run(original)
        await _seed_kentucky_payloads(database, original["run_id"], 1)
        await kentucky.mark_run_failed(original["run_id"], "synthetic stopped acquisition")
        candidate = _kentucky_run(limit=1, resume_from=original["run_id"])
        other = kentucky.PROFILE_CATEGORIES if scope == kentucky.LEGACY_CATEGORIES else kentucky.LEGACY_CATEGORIES
        candidate["source_manifest"]["categories"] = list(other)
        with pytest.raises(RuntimeError, match="resume_cohort_mismatch"):
            await kentucky.claim_run(candidate)
        candidate["source_manifest"]["categories"] = list(scope)
        await kentucky.claim_run(candidate)
        assert (await kentucky._read_run(candidate["run_id"]))["source_manifest"]["categories"] == list(scope)


async def test_practice_facts_cannot_mask_education_loss(monkeypatch):
    async with _database(monkeypatch) as database:
        incumbent = await _seed_kentucky_run(database)
        await kentucky.publish_run(incumbent["run_id"], expected_current_run_id=None, metrics=_metrics())
        candidate = _kentucky_run(predecessor=incumbent["run_id"])
        candidate["source_manifest"]["categories"] = list(kentucky.PROFILE_CATEGORIES)
        await kentucky.claim_run(candidate)
        await _seed_kentucky_payloads(database, candidate["run_id"], 10000)
        await database.status(f"UPDATE {kentucky._table(shared_store.ProviderProfileFact)} "
                              "SET category='services',fact_type='practice_type' WHERE run_id=:run_id",
                              run_id=candidate["run_id"])
        counts = await kentucky.retained_counts(candidate["run_id"])
        assert counts["received_profiles"] == 10000 and counts["matched_public_providers"] == 0
        assert counts["invalid_facts"] == 0
        with pytest.raises(RuntimeError, match="publication_volume_drop:matched_public_providers"):
            await kentucky.publish_run(candidate["run_id"], expected_current_run_id=incumbent["run_id"], metrics=_metrics())
        assert (await kentucky.read_publication())["current_run_id"] == incumbent["run_id"]
        assert (await kentucky._read_run(candidate["run_id"]))["status"] == "running"


@pytest.mark.parametrize(("metrics", "incumbent", "reason"), [
    ({"received_profiles": 4999, "requested_licenses": 10000, "matched_public_providers": 10000}, None, "received_profile_ratio"),
    ({"received_profiles": 10000, "requested_licenses": 10000, "matched_public_providers": 9999}, None, "first_publication_too_small"),
    ({"received_profiles": 7999, "requested_licenses": 10000, "matched_public_providers": 10000},
     {"received_profiles": 10000, "matched_public_providers": 10000}, "publication_volume_drop:received_profiles"),
    ({"received_profiles": 10000, "requested_licenses": 10000, "matched_public_providers": 7999},
     {"received_profiles": 10000, "matched_public_providers": 10000}, "publication_volume_drop:matched_public_providers"),
])
def test_kentucky_keeps_conservative_publication_guards(metrics, incumbent, reason):
    with pytest.raises(RuntimeError, match="^kentucky_profile_" + reason + "$"):
        kentucky._publication_volume(metrics, incumbent)


async def test_received_profiles_require_one_exact_visible_identity(monkeypatch):
    async with _database(monkeypatch) as database:
        candidate_run = await _seed_kentucky_run(database, limit=10)
        run_id = candidate_run["run_id"]
        source_table = kentucky._table(shared_store.ProviderProfileSourceRecord)
        fact_table = kentucky._table(shared_store.ProviderProfileFact)
        variants = [
            (2, "education_not_reported", [{"License": "2"}], "2"),
            (3, "education_unusable", [{"License": "3"}], "3"),
            (4, "not_found", [], "4"),
            (5, "held_identity", [{"License": "5"}, {"License": "5"}], "5"),
            (6, "held_identity", [{"License": "0006"}], "6"),
            (7, "public", [{"License": 7}], "7"),
            (8, "public", {}, "8"),
            (9, "public", [{"License": "\t C0007 \n"}], "C0007"),
        ]
        for number, visibility, profiles, license_number in variants:
            await database.status(f"""UPDATE {source_table}
                SET license_number=:license_number, raw_payload=CAST(:raw AS json), normalized_payload=CAST(:normalized AS json)
                WHERE record_id=:record_id""", license_number=license_number, record_id=run_id + f"{number:06d}",
                raw=json.dumps({"html": "retained HTML", "profiles": profiles}),
                normalized=json.dumps({"schema_version": kentucky.SCHEMA_VERSION, "visibility": visibility}))
        await database.status(f"DELETE FROM {fact_table} WHERE run_id=:run_id AND right(fact_id,6) BETWEEN '000002' AND '000008'", run_id=run_id)
        await database.status(f"UPDATE {fact_table} SET sensitive=true WHERE fact_id=:fact_id", fact_id=run_id + "000010")
        counts = await kentucky.retained_counts(run_id)
        assert counts["received_profiles"] == 5 and counts["retained_source_records"] == 10
        assert counts["matched_public_providers"] == 2 and counts["retained_facts"] == 3
        assert counts["invalid_source_records"] == counts["invalid_facts"] == counts["foreign_artifacts"] == 0
        assert not (await kentucky.finish_unpublished_run(run_id, _metrics(10)))["published"]
        assert await kentucky.read_publication() is None


async def test_massachusetts_training_remains_valid(monkeypatch):
    async with _database(monkeypatch) as database:
        candidate_run = await _seed_run(database, limit=1)
        fact_table = massachusetts._table(shared_store.ProviderProfileFact)
        await database.status(f"UPDATE {fact_table} SET category='training',fact_type='postgraduate_training' WHERE run_id=:run_id",
                              run_id=candidate_run["run_id"])
        assert (await massachusetts.retained_counts(candidate_run["run_id"]))["invalid_facts"] == 0
        assert not (await massachusetts.finish_unpublished_run(candidate_run["run_id"], _metrics(1)))["published"]


@pytest.mark.parametrize(("category", "fact_type"), [
    ("training", "postgraduate_training"), ("training", "education_history"), ("education", "clinical_experience"),
])
async def test_kentucky_fact_types_block_completion_and_cleanup(monkeypatch, tmp_path, category, fact_type):
    async with _database(monkeypatch) as database:
        candidate_run = await _seed_kentucky_run(database, limit=1)
        run_id = candidate_run["run_id"]
        fact_table = kentucky._table(shared_store.ProviderProfileFact)
        await database.status(f"UPDATE {fact_table} SET category=:category,fact_type=:fact_type WHERE run_id=:run_id",
                              category=category, fact_type=fact_type, run_id=run_id)
        assert (await kentucky.retained_counts(run_id))["invalid_facts"] == 1
        with pytest.raises(RuntimeError, match="^kentucky_profile_retained_integrity_invalid$"):
            await kentucky.finish_unpublished_run(run_id, _metrics(1))
        assert (await kentucky._read_run(run_id))["status"] == "running"
        assert await kentucky.read_publication() is None
        await kentucky.mark_run_failed(run_id, "synthetic invalid fact")
        run_table = kentucky._table(shared_store.ProviderProfileImportRun)
        await database.status(f"UPDATE {run_table} SET finished_at=:finished_at WHERE run_id=:run_id",
                              finished_at=kentucky._now() - timedelta(days=8), run_id=run_id)
        await kentucky.claim_run(_kentucky_run(limit=1))
        (tmp_path / run_id).mkdir()
        with pytest.raises(RuntimeError, match="^kentucky_profile_retention_foreign_payload$"):
            await kentucky.retain_source_history(tmp_path)
        assert (tmp_path / run_id).is_dir()
        assert await database.scalar(f"SELECT count(*) FROM {fact_table} WHERE run_id=:run_id", run_id=run_id) == 1


@pytest.mark.parametrize("held_source", ["MA", "KY"])
async def test_source_locks_allow_independent_claims(monkeypatch, held_source):
    async with _database(monkeypatch):
        locked_store, other_store = (massachusetts, kentucky) if held_source == "MA" else (kentucky, massachusetts)
        candidate_run = _kentucky_run(limit=1) if held_source == "MA" else _run(limit=1)
        acquired, release = asyncio.Event(), asyncio.Event()

        async def hold_source_lock():
            async with shared_store.db.transaction():
                await locked_store._lock_source()
                acquired.set()
                await release.wait()

        holder = asyncio.create_task(hold_source_lock())
        try:
            await asyncio.wait_for(acquired.wait(), timeout=5)
            await asyncio.wait_for(other_store.claim_run(candidate_run), timeout=5)
            assert (await other_store._read_run(candidate_run["run_id"]))["status"] == "running"
            with pytest.raises(RuntimeError, match="run_source_mismatch"):
                await locked_store._read_run(candidate_run["run_id"])
        finally:
            release.set()
            await holder


async def test_publications_and_retention_are_source_isolated(monkeypatch, tmp_path):
    async with _database(monkeypatch) as database:
        ma_run, ky_run = await asyncio.gather(_seed_run(database), _seed_kentucky_run(database))
        ma_id, ky_id = ma_run["run_id"], ky_run["run_id"]
        await asyncio.gather(
            massachusetts.publish_run(ma_id, expected_current_run_id=None, metrics=_metrics()),
            kentucky.publish_run(ky_id, expected_current_run_id=None, metrics=_metrics()),
        )
        failed_run = await _seed_kentucky_run(database, limit=1, predecessor=ky_id)
        failed_id = failed_run["run_id"]
        await kentucky.mark_run_failed(failed_id, "synthetic acquisition failure")
        run_table = kentucky._table(shared_store.ProviderProfileImportRun)
        await database.status(f"UPDATE {run_table} SET finished_at=:finished_at WHERE run_id=:run_id",
                              finished_at=kentucky._now() - timedelta(days=8), run_id=failed_id)
        await kentucky.claim_run(_kentucky_run(limit=1, predecessor=ky_id))
        for run_id in (ma_id, ky_id, failed_id):
            (tmp_path / run_id).mkdir()
        assert (await kentucky.retain_source_history(tmp_path))["deleted_run_ids"] == [failed_id]
        assert (tmp_path / ma_id).is_dir() and (tmp_path / ky_id).is_dir() and not (tmp_path / failed_id).exists()
        assert (await massachusetts.read_publication())["current_run_id"] == ma_id
        assert (await kentucky.read_publication())["current_run_id"] == ky_id
        assert (await massachusetts.retained_counts(ma_id))["retained_facts"] == 10000
        assert (await kentucky.retained_counts(ky_id))["retained_facts"] == 10000
        assert (await kentucky._read_run(failed_id))["status"] == "failed"


async def test_kentucky_failure_rolls_back_both_pointer_and_facts(monkeypatch):
    async with _database(monkeypatch) as database:
        ma_run = await _seed_run(database)
        await massachusetts.publish_run(ma_run["run_id"], expected_current_run_id=None, metrics=_metrics())
        ky_run = await _seed_kentucky_run(database)
        original_transaction = database.transaction
        fact_table = kentucky._table(shared_store.ProviderProfileFact)
        publication_table = kentucky._table(shared_store.ProviderProfileSourcePublication)

        @asynccontextmanager
        async def fail_before_commit():
            async with original_transaction():
                yield
                assert await database.scalar(f"SELECT current_run_id FROM {publication_table} WHERE source_key=:source_key",
                                             source_key=kentucky.SOURCE_KEY) == ky_run["run_id"]
                assert await database.scalar(f"SELECT count(*) FROM {fact_table} WHERE run_id=:run_id AND published_at IS NOT NULL",
                                             run_id=ky_run["run_id"]) == 10000
                raise RuntimeError("synthetic completion failure")

        with monkeypatch.context() as patch:
            patch.setattr(database, "transaction", fail_before_commit)
            with pytest.raises(RuntimeError, match="^synthetic completion failure$"):
                await kentucky.publish_run(ky_run["run_id"], expected_current_run_id=None, metrics=_metrics())
        assert await kentucky.read_publication() is None
        assert (await kentucky._read_run(ky_run["run_id"]))["status"] == "running"
        assert await database.scalar(f"SELECT count(*) FROM {fact_table} WHERE run_id=:run_id AND published_at IS NOT NULL",
                                     run_id=ky_run["run_id"]) == 0
        assert (await massachusetts.read_publication())["current_run_id"] == ma_run["run_id"]
        assert (await massachusetts.retained_counts(ma_run["run_id"]))["retained_facts"] == 10000
        assert (await kentucky.publish_run(ky_run["run_id"], expected_current_run_id=None, metrics=_metrics()))["published"]
