# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from contextlib import asynccontextmanager, contextmanager
from copy import deepcopy
from datetime import timedelta
import os
import importlib
import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
import uuid

import pytest
from sqlalchemy import MetaData, select, update
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from sqlalchemy.orm import registry

from api import control_imports
from db.connection import Database
from process import massachusetts_profile_store as store
from process import provider_profile_source_store as shared_store


florida = importlib.import_module("process.florida_mqa_profile")

MODEL_NAMES = (
    "ProviderProfileImportRun", "ProviderProfileArtifact", "ProviderProfileSourceRecord",
    "ProviderProfileFact", "ProviderProfileSourcePublication",
)


def _run(run_id=None, *, limit=None, count=10000, predecessor=None, resume_from=None, categories=("education", "training")):
    return {
        "run_id": run_id or uuid.uuid4().hex, "source_key": store.SOURCE_KEY, "jurisdiction": "MA",
        "schema_version": store.SCHEMA_VERSION, "status": "running", "started_at": store._now(),
        "source_manifest": {
            "max_providers": limit, "full_cohort_licenses": count,
            "requested_licenses": min(limit, count) if limit is not None else count,
            "expected_current_run_id": predecessor, "resume_from": resume_from,
            "cohort_sha256": "a" * 64, "categories": list(categories),
            "source": {"source_key": store.SOURCE_KEY, "source_kind": "state_regulator", "jurisdiction": "MA"},
        },
    }


def _metrics(count=10000):
    return {"responses": count, "transport_failures": 0, "acquisition_complete": True}


def _counts(count=10000):
    return {"retained_source_records": count, "received_profiles": count,
            "retained_facts": count, "matched_public_providers": count, "portfolio_only_public_providers": 0,
            "invalid_source_records": 0, "invalid_facts": 0, "foreign_artifacts": 0}


@pytest.mark.parametrize("changes", [
    {"max_providers": True}, {"max_providers": 0}, {"max_providers": -1},
    {"requested_licenses": 2}, {"full_cohort_licenses": True}, {"full_cohort_licenses": 0},
    {"cohort_sha256": "invalid"}, {"categories": ["education"]},
    {"source": {"source_key": "florida-mqa"}}, {"expected_current_run_id": "../other"},
])
def test_frozen_manifest_rejects_invalid_scope(changes):
    candidate_run = _run()
    candidate_run["source_manifest"].update(changes)
    with pytest.raises(ValueError, match="massachusetts_profile_"):
        store._manifest(candidate_run)


@pytest.mark.parametrize("manifest", [None, {}, {"source": None}])
def test_frozen_manifest_requires_evidence(manifest):
    with pytest.raises(ValueError, match="massachusetts_profile_manifest_"):
        store._manifest({"source_manifest": manifest})


@pytest.mark.parametrize(("metrics", "counts", "error"), [
    ({}, _counts(), "acquisition_incomplete"),
    ({**_metrics(), "acquisition_complete": 1}, _counts(), "acquisition_incomplete"),
    ({**_metrics(), "transport_failures": 1}, _counts(), "transport_failures"),
    ({**_metrics(), "transport_failures": False}, _counts(), "transport_failures"),
    ({**_metrics(), "responses": True}, _counts(), "response_count_mismatch"),
    (_metrics(9999), _counts(), "response_count_mismatch"),
    (_metrics(), _counts(9999), "response_count_mismatch"),
    (_metrics(), {**_counts(), "invalid_source_records": 1}, "retained_integrity_invalid"),
    (_metrics(), {**_counts(), "invalid_facts": 1}, "retained_integrity_invalid"),
    (_metrics(), {**_counts(), "foreign_artifacts": 1}, "retained_integrity_invalid"),
])
def test_completion_requires_actual_retained_counts(metrics, counts, error):
    with pytest.raises(RuntimeError, match=error):
        store._completion_metrics(_run(), metrics, counts)


def test_completion_uses_database_counts_and_preserves_acquisition_metrics():
    metrics_by_field = {**_metrics(), "matched_public_providers": 999999, "response_bytes": 123}
    final_metrics = store._completion_metrics(_run(), metrics_by_field, _counts())
    assert final_metrics["matched_public_providers"] == 10000
    assert final_metrics["response_bytes"] == 123
    assert final_metrics["full_cohort_licenses"] == final_metrics["requested_licenses"] == 10000


@pytest.mark.parametrize(("candidate", "incumbent", "error"), [
    ({"received_profiles": 4999, "matched_public_providers": 10000, "requested_licenses": 10000}, None, "received_profile_ratio"),
    ({"received_profiles": 10000, "matched_public_providers": 9999, "requested_licenses": 10000}, None, "first_publication_too_small"),
    ({"received_profiles": 10000, "matched_public_providers": 7999, "requested_licenses": 10000}, _counts(), "volume_drop:matched_public_providers"),
    ({"received_profiles": 7999, "matched_public_providers": 10000, "requested_licenses": 10000}, _counts(), "volume_drop:received_profiles"),
])
def test_publication_has_no_volume_override(candidate, incumbent, error):
    with pytest.raises(RuntimeError, match=error):
        store._publication_volume(candidate, incumbent)


def test_publication_thresholds_accept_exact_boundaries():
    store._publication_volume({**_counts(), "requested_licenses": 20000}, None)
    store._publication_volume({**_counts(8000), "requested_licenses": 10000}, _counts())


def test_retention_protects_pointer_active_resume_and_recent_failures():
    now = store._now()
    source_rows = []
    for number, status, age in ((1, "completed", 30), (2, "completed", 30), (3, "completed", 30),
                                (4, "failed", 8), (5, "failed", 6), (6, "running", 0),
                                (7, "failed", 10), (8, "validating", 0)):
        source_rows.append({**_run(f"{number:032x}"), "status": status, "finished_at": now - timedelta(days=age)})
    source_rows[5]["source_manifest"]["resume_from"] = source_rows[6]["run_id"]
    publication_by_field = {"current_run_id": source_rows[0]["run_id"], "previous_run_id": source_rows[1]["run_id"]}
    deleted, protected = store._retention_candidates(source_rows, publication_by_field, now)
    assert deleted == [f"{number:032x}" for number in (3, 4)]
    assert protected == [f"{number:032x}" for number in (1, 2, 6, 7, 8)]


@contextmanager
def _guard_database(monkeypatch, responses=()):
    @asynccontextmanager
    async def transaction():
        yield

    before = deepcopy(responses)
    database = SimpleNamespace(
        transaction=transaction, scalar=AsyncMock(return_value=None),
        first=AsyncMock(side_effect=[None if row is None else SimpleNamespace(_mapping=row) for row in responses]),
        update=Mock(side_effect=AssertionError("unexpected update")),
        insert=Mock(side_effect=AssertionError("unexpected insert")),
    )
    claim = AsyncMock(side_effect=AssertionError("unexpected claim"))
    monkeypatch.setattr(shared_store, "db", database)
    monkeypatch.setattr(shared_store, "_claim_import_run", claim)
    yield database
    database.update.assert_not_called()
    database.insert.assert_not_called()
    claim.assert_not_called()
    assert responses == before


@pytest.mark.parametrize("invalid_state", ["running", "bounded", "unpublished"])
async def test_corrupt_publication_pointer_cannot_be_read(monkeypatch, invalid_state):
    candidate = _run(limit=2 if invalid_state == "bounded" else None)
    candidate.update(status="running" if invalid_state == "running" else "completed",
                     metrics={"published": invalid_state != "unpublished"})
    publication_by_field = {"source_key": store.SOURCE_KEY, "current_run_id": candidate["run_id"]}
    with _guard_database(monkeypatch, [publication_by_field, candidate]):
        with pytest.raises(RuntimeError, match="publication_invalid"):
            await store.read_publication()


@pytest.mark.parametrize("invalid_run", ["missing", "foreign_source", "foreign_schema"])
async def test_publication_rejects_missing_or_foreign_run(monkeypatch, invalid_run):
    candidate = _run()
    publication_by_field = {"source_key": store.SOURCE_KEY, "current_run_id": candidate["run_id"]}
    if invalid_run == "foreign_source":
        candidate["source_key"] = "florida-mqa"
    elif invalid_run == "foreign_schema":
        candidate["schema_version"] = "foreign-profile/v1"
    with _guard_database(monkeypatch, [publication_by_field, None if invalid_run == "missing" else candidate]):
        with pytest.raises(RuntimeError, match="run_missing|run_source_mismatch"):
            await store.read_publication()


@pytest.mark.parametrize(("field", "invalid_value"), [
    ("source_key", "florida-mqa"), ("schema_version", "foreign-profile/v1"),
    ("jurisdiction", "FL"), ("status", "completed"), ("run_id", "../other"),
])
async def test_invalid_initial_claim_never_reaches_storage(monkeypatch, field, invalid_value):
    candidate = _run()
    candidate[field] = invalid_value
    with _guard_database(monkeypatch) as database:
        with pytest.raises(ValueError, match="initial_run_invalid|run_id_invalid"):
            await store.claim_run(candidate)
        database.first.assert_not_called()
        database.scalar.assert_not_called()


@pytest.mark.parametrize(("fields", "error"), [
    (None, "run_update_invalid"), ({}, "run_update_invalid"),
    ({"source_manifest": {}}, "run_update_invalid"),
    ({"status": "completed"}, "run_transition_invalid"), ({"metrics": []}, "metrics_invalid"),
])
async def test_invalid_update_never_reaches_storage(monkeypatch, fields, error):
    with _guard_database(monkeypatch) as database:
        with pytest.raises(ValueError, match=error):
            await store.update_run(_run()["run_id"], fields)
        database.first.assert_not_called()
        database.scalar.assert_not_called()


async def test_completed_publication_cannot_be_republished(monkeypatch):
    candidate_by_field = {**_run(), "status": "completed", "metrics": {"published": True}}
    with _guard_database(monkeypatch, [candidate_by_field]):
        with pytest.raises(RuntimeError, match="run_not_active"):
            await store.publish_run(candidate_by_field["run_id"], expected_current_run_id=None, metrics=_metrics())


async def test_full_run_cannot_use_bounded_completion(monkeypatch):
    candidate = _run()
    with _guard_database(monkeypatch, [candidate]):
        with pytest.raises(RuntimeError, match="bounded_completion_invalid"):
            await store.finish_unpublished_run(candidate["run_id"], _metrics())


@asynccontextmanager
async def _database(monkeypatch):
    database_dsn = os.getenv("HLTHPRT_PROVIDER_DIRECTORY_PROFILE_POSTGRES_DSN")
    if not database_dsn:
        pytest.skip("set the profile PostgreSQL DSN for isolated publication regressions")
    schema = f"ma_store_{uuid.uuid4().hex}"
    engine = create_async_engine(make_url(database_dsn).set(drivername="postgresql+asyncpg"))
    database = Database(engine=engine, session_factory=async_sessionmaker(engine, expire_on_commit=False))
    metadata = MetaData()
    is_schema_created = False
    try:
        await database.status(f"CREATE SCHEMA {schema}")
        is_schema_created = True
        for name in MODEL_NAMES:
            table = getattr(store, name).__table__.to_metadata(metadata, schema=schema)
            model = SimpleNamespace(__table__=table, **{column.name: column for column in table.c})
            monkeypatch.setattr(store, name, model)
            monkeypatch.setattr(shared_store, name, model)
            if hasattr(florida, name):
                monkeypatch.setattr(florida, name, model)
        monkeypatch.setattr(shared_store, "db", database)
        monkeypatch.setattr(florida, "db", database)
        await store.ensure_tables()
        yield database
    finally:
        try:
            if is_schema_created:
                await database.status(f"DROP SCHEMA {schema} CASCADE")
                assert await database.scalar("SELECT to_regnamespace(:schema)", schema=schema) is None
        finally:
            await database.disconnect()


async def _seed_payloads(database, run_id, count):
    records_table = store._table(store.ProviderProfileSourceRecord)
    facts_table = store._table(store.ProviderProfileFact)
    await database.status(f"""
        INSERT INTO {records_table} (record_id,run_id,artifact_id,source_key,source_record_key,
          license_number,raw_payload,normalized_payload,matched_npi,match_status)
        SELECT :run_id || lpad(i::text,6,'0'), :run_id, :run_id, :source_key, i::text,
          i::text, json_build_object('licenseNumber',i::text,'licenseMetaId',1),
          json_build_object('schema_version',CAST(:schema_version AS text),'visibility','public'),1000000000+i,'deterministic'
          FROM generate_series(1,:count) i
    """, run_id=run_id, count=count, source_key=store.SOURCE_KEY, schema_version=store.SCHEMA_VERSION)
    await database.status(f"""
        INSERT INTO {facts_table} (fact_id,run_id,npi,source_record_id,logical_fact_key,category,
          fact_type,display,value_json,availability,assertion_type,verification_status,source_json,sensitive,public_default)
        SELECT record_id,run_id,matched_npi,record_id,record_id,'education','education_history',
          'Synthetic School','{{}}','available','self_reported','not_independently_verified',
          json_build_object('source_key',CAST(:source_key AS text),'schema_version',CAST(:schema_version AS text),'source_record_id',record_id),false,true
          FROM {records_table} WHERE run_id = :run_id
    """, run_id=run_id, source_key=store.SOURCE_KEY, schema_version=store.SCHEMA_VERSION)


async def _seed_run(database, **kwargs):
    candidate_run = _run(**kwargs)
    await store.claim_run(candidate_run)
    await _seed_payloads(database, candidate_run["run_id"], candidate_run["source_manifest"]["requested_licenses"])
    return candidate_run


async def _legacy_retained_counts(database, run_id):
    """Keep the pre-rewrite SQL as an independent oracle for retained-count parity."""
    source_records = store._table(store.ProviderProfileSourceRecord)
    facts = store._table(store.ProviderProfileFact)
    artifacts = store._table(store.ProviderProfileArtifact)
    count_row = await database.first(f"""
        SELECT count(*) AS retained_source_records,
               count(*) FILTER (WHERE raw_payload->>'licenseNumber' = license_number
                                  AND raw_payload->>'licenseMetaId' = '1') AS received_profiles,
               count(*) FILTER (WHERE source_key <> :source_key
                     OR normalized_payload->>'schema_version' IS DISTINCT FROM :schema_version) AS invalid_source_records,
               (SELECT count(*) FROM {facts} WHERE run_id = :run_id) AS retained_facts,
               (SELECT count(DISTINCT f.npi) FROM {facts} f JOIN {source_records} r
                   ON r.record_id = f.source_record_id AND r.run_id = f.run_id
                 WHERE f.run_id = :run_id AND r.source_key = :source_key
                   AND r.match_status = 'deterministic' AND r.matched_npi = f.npi
                   AND r.normalized_payload->>'visibility' = 'public'
                   AND NOT f.sensitive AND f.public_default AND f.availability = 'available') AS matched_public_providers,
               (SELECT count(*) FROM {facts} f LEFT JOIN {source_records} r
                   ON r.record_id = f.source_record_id AND r.run_id = f.run_id
                 WHERE f.run_id = :run_id AND (r.record_id IS NULL OR r.source_key <> :source_key
                    OR f.source_json->>'source_key' IS DISTINCT FROM :source_key
                    OR f.source_json->>'schema_version' IS DISTINCT FROM :schema_version
                    OR f.source_json->>'source_record_id' IS DISTINCT FROM f.source_record_id
                    OR f.npi IS DISTINCT FROM r.matched_npi
                    OR r.normalized_payload->>'visibility' IS DISTINCT FROM 'public'
                    OR (f.npi IS NOT NULL AND r.match_status <> 'deterministic'))) AS invalid_facts,
               (SELECT count(*) FROM {artifacts} WHERE run_id = :run_id
                    AND source_key <> :source_key) AS foreign_artifacts
          FROM {source_records} WHERE run_id = :run_id
    """, run_id=run_id, source_key=store.SOURCE_KEY, schema_version=store.SCHEMA_VERSION)
    return {**dict(count_row._mapping), "portfolio_only_public_providers": 0}


@pytest.mark.parametrize("corruption", ["source_record", "fact", "artifact"])
async def test_grouped_retention_counts_preserve_integrity_per_run(monkeypatch, corruption):
    async with _database(monkeypatch) as database:
        run_ids = []
        for _ in range(3):
            run_id = (await _seed_run(database, limit=2))["run_id"]
            await store.mark_run_failed(run_id, "synthetic history")
            run_ids.append(run_id)
        empty_id = uuid.uuid4().hex
        run_ids.append(empty_id)
        query = AsyncMock(wraps=database.all)
        monkeypatch.setattr(database, "all", query)
        await store._assert_source_ownership(run_ids)
        assert query.await_count == 1
        invalid_id = run_ids[1]
        if corruption == "source_record":
            await database.status(f"UPDATE {store._table(store.ProviderProfileSourceRecord)} SET source_key='foreign' WHERE run_id=:run_id", run_id=invalid_id)
        elif corruption == "fact":
            await database.status(f"UPDATE {store._table(store.ProviderProfileFact)} SET source_json='{{}}' WHERE run_id=:run_id", run_id=invalid_id)
        else:
            await database.insert(store.ProviderProfileArtifact.__table__).values(
                artifact_id=invalid_id, run_id=invalid_id, source_key="foreign", file_name="synthetic.json",
                source_url="https://example.test/profile", category="education", content_sha256="a" * 64, content_bytes=2).status()
        counts_by_run = {run_id: await _legacy_retained_counts(database, run_id) for run_id in run_ids}
        assert await store._store._retained_counts_by_run(run_ids) == counts_by_run
        query.reset_mock()
        with pytest.raises(RuntimeError, match="retention_foreign_payload"):
            await store._assert_source_ownership(run_ids)
        assert query.await_count == 1
        assert await store.retained_counts(empty_id) == _counts(0)
        query.reset_mock()
        await store._assert_source_ownership([])
        query.assert_not_called()


@pytest.mark.parametrize(("record_changes", "fact_changes", "expected_changes"), [
    ({}, {}, {}),
    ({"source_key": "florida-mqa"}, {}, {"invalid_source_records": 1, "invalid_facts": 1, "matched_public_providers": 1}),
    ({"normalized_payload": None}, {}, {"invalid_source_records": 1, "invalid_facts": 1, "matched_public_providers": 1}),
    ({"normalized_payload": {"schema_version": store.SCHEMA_VERSION}}, {}, {"invalid_facts": 1, "matched_public_providers": 1}),
    ({"match_status": "unmatched"}, {}, {"invalid_facts": 1, "matched_public_providers": 1}),
    ({"matched_npi": None}, {}, {"invalid_facts": 1, "matched_public_providers": 1}),
    ({}, {"npi": None}, {"invalid_facts": 1, "matched_public_providers": 1}),
    ({"matched_npi": None}, {"npi": None}, {"matched_public_providers": 1}),
    ({"matched_npi": None, "match_status": "unmatched"}, {"npi": None}, {"matched_public_providers": 1}),
    ({}, {"source_json": {}}, {"invalid_facts": 1}),
    ({}, {"source_json": None}, {"invalid_facts": 1}),
    ({}, {"sensitive": True}, {"matched_public_providers": 1}),
    ({}, {"public_default": False}, {"matched_public_providers": 1}),
    ({}, {"availability": "unavailable"}, {"matched_public_providers": 1}),
    ({"license_number": None}, {}, {"received_profiles": 1}),
])
async def test_retained_count_predicates_match_legacy_in_postgresql(monkeypatch, record_changes, fact_changes, expected_changes):
    async with _database(monkeypatch) as database:
        candidate_run = await _seed_run(database, limit=2)
        run_id = candidate_run["run_id"]
        record_id = run_id + "000001"
        if record_changes:
            records_table = store.ProviderProfileSourceRecord.__table__
            await database.update(records_table).where(records_table.c.record_id == record_id).values(record_changes).status()
        if fact_changes:
            facts_table = store.ProviderProfileFact.__table__
            await database.update(facts_table).where(facts_table.c.fact_id == record_id).values(fact_changes).status()
        legacy_counts = await _legacy_retained_counts(database, run_id)
        assert legacy_counts == {**_counts(2), **expected_changes}
        assert await store.retained_counts(run_id) == legacy_counts


@pytest.mark.parametrize("parent_kind", ["orphan", "other_run", "other_source_run"])
async def test_orphan_and_cross_run_parents_remain_invalid_in_postgresql(monkeypatch, parent_kind):
    async with _database(monkeypatch) as database:
        candidate_run = await _seed_run(database, limit=2)
        run_id = candidate_run["run_id"]
        other_run_id = uuid.uuid4().hex
        await _seed_payloads(database, other_run_id, 1)
        records_table = store.ProviderProfileSourceRecord.__table__
        parent_id = other_run_id + "000001"
        if parent_kind == "orphan":
            parent_id = "absent_record"
        elif parent_kind == "other_source_run":
            await database.update(records_table).where(records_table.c.record_id == parent_id).values(source_key="florida-mqa").status()
        else:
            parent = await database.first(records_table.select().where(records_table.c.record_id == parent_id))
            assert parent.run_id == other_run_id and parent.matched_npi == 1000000001
        facts_table = store.ProviderProfileFact.__table__
        await database.update(facts_table).where(facts_table.c.fact_id == run_id + "000001").values(
            source_record_id=parent_id,
            source_json={"source_key": store.SOURCE_KEY, "schema_version": store.SCHEMA_VERSION, "source_record_id": parent_id},
        ).status()
        expected_count_by_field = {**_counts(2), "matched_public_providers": 1, "invalid_facts": 1}
        assert await _legacy_retained_counts(database, run_id) == expected_count_by_field
        assert await store.retained_counts(run_id) == expected_count_by_field
        with pytest.raises(RuntimeError, match="retained_integrity_invalid"):
            await store.finish_unpublished_run(run_id, _metrics(2))
        assert await store.read_publication() is None
        assert (await store._read_run(run_id))["status"] == "running"


async def test_empty_and_repeated_fact_counts_match_legacy_in_postgresql(monkeypatch):
    async with _database(monkeypatch) as database:
        run_id = uuid.uuid4().hex
        assert await store.retained_counts(run_id) == await _legacy_retained_counts(database, run_id) == _counts(0)
        await _seed_run(database, run_id=run_id, limit=2)
        facts_table = store.ProviderProfileFact.__table__
        fact = await database.first(facts_table.select().where(facts_table.c.fact_id == run_id + "000001"))
        repeated_fact_by_field = {**fact._mapping, "fact_id": uuid.uuid4().hex, "category": "training", "fact_type": "postgraduate_training"}
        await database.insert(facts_table).values(repeated_fact_by_field).status()
        assert await store.retained_counts(run_id) == await _legacy_retained_counts(database, run_id) == {**_counts(2), "retained_facts": 3}
        await database.execute(facts_table.delete().where(facts_table.c.run_id == run_id))
        assert await store.retained_counts(run_id) == await _legacy_retained_counts(database, run_id) == {
            **_counts(2), "retained_facts": 0, "matched_public_providers": 0,
        }


async def test_atomic_publication_and_terminal_status_in_postgresql(monkeypatch):
    async with _database(monkeypatch) as database:
        candidate_run = await _seed_run(database)
        run_id = candidate_run["run_id"]
        original_transaction = database.transaction

        @asynccontextmanager
        async def fail_before_commit():
            async with original_transaction():
                yield
                raise RuntimeError("synthetic publication interruption")

        monkeypatch.setattr(database, "transaction", fail_before_commit)
        with pytest.raises(RuntimeError, match="synthetic publication interruption"):
            await store.publish_run(run_id, expected_current_run_id=None, metrics=_metrics())
        assert await store.read_publication() is None
        assert (await store._read_run(run_id))["status"] == "running"
        assert await database.scalar(f"SELECT count(*) FROM {store._table(store.ProviderProfileFact)} WHERE published_at IS NOT NULL") == 0
        monkeypatch.setattr(database, "transaction", original_transaction)
        receipt = await store.publish_run(run_id, expected_current_run_id=None, metrics=_metrics())
        assert receipt["matched_public_providers"] == 10000
        assert (await store.read_publication())["current_run_id"] == run_id
        await store.mark_run_failed(run_id, "late failure must not downgrade")
        assert (await store._read_run(run_id))["status"] == "completed"
        assert await database.scalar(f"SELECT count(*) FROM {store._table(store.ProviderProfileFact)} WHERE published_at IS NOT NULL") == 10000
        with pytest.raises(RuntimeError, match="not_active"):
            await store.update_run(run_id, {"metrics": {}})


async def test_bounded_completion_and_initial_claim_in_postgresql(monkeypatch, tmp_path):
    async with _database(monkeypatch) as database:
        candidate_run = await _seed_run(database, limit=2)
        run_id = candidate_run["run_id"]
        with pytest.raises(RuntimeError, match="source_already_running"):
            await store.claim_run(_run(limit=2))
        with pytest.raises(RuntimeError, match="bounded_publication_forbidden"):
            await store.publish_run(run_id, expected_current_run_id=None, metrics=_metrics(2))
        await store.update_run(run_id, {"status": "validating", "metrics": _metrics(2)})
        with pytest.raises(ValueError, match="run_update_invalid"):
            await store.update_run(run_id, {"source_manifest": {}})
        assert not (await store.finish_unpublished_run(run_id, _metrics(2)))["published"]
        assert await store.read_publication() is None
        (tmp_path/run_id).mkdir()
        assert (await store.retain_source_history(tmp_path))["deleted_run_ids"] == []
        assert (tmp_path/run_id).exists()
        with pytest.raises(RuntimeError, match="already_completed"):
            await store.claim_run(candidate_run)


async def test_pointer_predecessor_and_volume_fences_in_postgresql(monkeypatch):
    async with _database(monkeypatch) as database:
        first_run = await _seed_run(database)
        first_id = first_run["run_id"]
        await store.publish_run(first_id, expected_current_run_id=None, metrics=_metrics())
        with pytest.raises(RuntimeError, match="predecessor_changed"):
            await store.claim_run(_run())
        second_run = await _seed_run(database, count=8000, predecessor=first_id)
        second_id = second_run["run_id"]
        with pytest.raises(RuntimeError, match="frozen_predecessor_mismatch"):
            await store.publish_run(second_id, expected_current_run_id=None, metrics=_metrics(8000))
        facts_table = store._table(store.ProviderProfileFact)
        await database.status(f"UPDATE {facts_table} SET public_default=false WHERE fact_id=(SELECT min(fact_id) FROM {facts_table} WHERE run_id=:run_id)", run_id=second_id)
        with pytest.raises(RuntimeError, match="volume_drop:matched_public_providers"):
            await store.publish_run(second_id, expected_current_run_id=first_id, metrics=_metrics(8000))
        assert (await store.read_publication())["current_run_id"] == first_id
        await database.status(f"UPDATE {facts_table} SET public_default=true WHERE run_id=:run_id", run_id=second_id)
        await store.publish_run(second_id, expected_current_run_id=first_id, metrics=_metrics(8000))
        pointer = await store.read_publication()
        assert pointer["current_run_id"] == second_id and pointer["previous_run_id"] == first_id


async def test_failed_resume_and_source_retention_in_postgresql(monkeypatch, tmp_path):
    async with _database(monkeypatch) as database:
        failed_run = await _seed_run(database, limit=2)
        failed_id = failed_run["run_id"]
        await store.mark_run_failed(failed_id, "synthetic transport error")
        resumed = await store.read_resume_run(failed_id, max_providers=2, expected_current_run_id=None)
        assert resumed["source_manifest"] == failed_run["source_manifest"]
        with pytest.raises(RuntimeError, match="resume_scope_mismatch"):
            await store.read_resume_run(failed_id, max_providers=None, expected_current_run_id=None)
        active_run = _run(limit=2, resume_from=failed_id)
        active_run["source_manifest"]["cohort_sha256"] = "b" * 64
        with pytest.raises(RuntimeError, match="resume_cohort_mismatch"):
            await store.claim_run(active_run)
        active_run["source_manifest"]["cohort_sha256"] = "a" * 64
        await store.claim_run(active_run)
        runs_table = store._table(store.ProviderProfileImportRun)
        await database.status(f"UPDATE {runs_table} SET finished_at=:finished_at WHERE run_id=:run_id", run_id=failed_id, finished_at=store._now()-timedelta(days=8))
        foreign_run_by_field = {**_run(), "source_key": "florida-mqa", "status": "completed"}
        await database.insert(store.ProviderProfileImportRun.__table__).values(foreign_run_by_field).status()
        await _seed_payloads(database, foreign_run_by_field["run_id"], 1)
        source_table = store._table(store.ProviderProfileSourceRecord)
        await database.status(f"UPDATE {source_table} SET source_key='florida-mqa' WHERE run_id=:run_id", run_id=foreign_run_by_field["run_id"])
        for run_id in (failed_id, foreign_run_by_field["run_id"]):
            (tmp_path/run_id).mkdir()
            (tmp_path/run_id/"retained.json").write_text("synthetic")
        receipt = await store.retain_source_history(tmp_path)
        assert failed_id in receipt["protected_audit_run_ids"]
        assert (tmp_path/failed_id).exists() and (tmp_path/foreign_run_by_field["run_id"]).exists()
        await store.mark_run_failed(active_run["run_id"], "finished")
        receipt = await store.retain_source_history(tmp_path)
        assert receipt["deleted_run_ids"] == [failed_id]
        assert not (tmp_path/failed_id).exists() and (tmp_path/foreign_run_by_field["run_id"]).exists()
        assert (await store._read_run(failed_id))["status"] == "failed"
        assert await database.scalar(f"SELECT count(*) FROM {source_table} WHERE run_id=:run_id", run_id=foreign_run_by_field["run_id"]) == 1
        with pytest.raises(RuntimeError, match="resume_not_eligible"):
            await store.read_resume_run(failed_id, max_providers=2, expected_current_run_id=None)


async def test_simultaneous_claims_serialize_in_postgresql(monkeypatch):
    async with _database(monkeypatch) as database:
        candidate_runs = [_run(limit=2), _run(limit=2)]
        outcomes = await asyncio.gather(*(store.claim_run(candidate_run) for candidate_run in candidate_runs), return_exceptions=True)
        assert sum(outcome is None for outcome in outcomes) == 1
        assert sum(isinstance(outcome, RuntimeError) and "source_already_running" in str(outcome) for outcome in outcomes) == 1
        assert await database.scalar(f"SELECT count(*) FROM {store._table(store.ProviderProfileImportRun)}") == 1


async def test_integrity_failure_preserves_data_in_postgresql(monkeypatch, tmp_path):
    async with _database(monkeypatch) as database:
        candidate_run = await _seed_run(database, limit=2)
        run_id = candidate_run["run_id"]
        facts_table = store._table(store.ProviderProfileFact)
        await database.status(f"UPDATE {facts_table} SET source_json='{{}}' WHERE run_id=:run_id", run_id=run_id)
        assert (await store.retained_counts(run_id))["invalid_facts"] == 2
        with pytest.raises(RuntimeError, match="retained_integrity_invalid"):
            await store.finish_unpublished_run(run_id, _metrics(2))
        await store.mark_run_failed(run_id, "synthetic invalid evidence")
        runs_table = store._table(store.ProviderProfileImportRun)
        await database.status(f"UPDATE {runs_table} SET finished_at=:finished_at WHERE run_id=:run_id", run_id=run_id, finished_at=store._now()-timedelta(days=8))
        (tmp_path/run_id).mkdir()
        await store.claim_run(_run(limit=2))
        with pytest.raises(RuntimeError, match="retention_foreign_payload"):
            await store.retain_source_history(tmp_path)
        assert (tmp_path/run_id).exists()
        assert await database.scalar(f"SELECT count(*) FROM {facts_table}") == 2


@pytest.mark.parametrize("original_scope", [store.LEGACY_CATEGORIES, store.PROFILE_CATEGORIES])
async def test_resume_cannot_change_category_scope_under_claim_lock(monkeypatch, original_scope):
    async with _database(monkeypatch) as database:
        original = await _seed_run(database, limit=2, categories=original_scope)
        await store.mark_run_failed(original["run_id"], "synthetic interruption")
        other_scope = store.PROFILE_CATEGORIES if original_scope == store.LEGACY_CATEGORIES else store.LEGACY_CATEGORIES
        candidate = _run(limit=2, resume_from=original["run_id"], categories=other_scope)
        with pytest.raises(RuntimeError, match="resume_cohort_mismatch"):
            await store.claim_run(candidate)
        assert await database.scalar(f"SELECT count(*) FROM {store._table(store.ProviderProfileImportRun)}") == 1
        candidate["source_manifest"]["categories"] = list(original_scope)
        await store.claim_run(candidate)
        assert (await store._read_run(candidate["run_id"]))["source_manifest"]["categories"] == list(original_scope)


@pytest.mark.parametrize("limit", [None, 2])
async def test_out_of_manifest_fact_blocks_completion_without_pointer_change(monkeypatch, limit):
    async with _database(monkeypatch) as database:
        incumbent = await _seed_run(database)
        await store.publish_run(incumbent["run_id"], expected_current_run_id=None, metrics=_metrics())
        candidate = await _seed_run(database, limit=limit, predecessor=incumbent["run_id"])
        run_id = candidate["run_id"]
        facts_table = store._table(store.ProviderProfileFact)
        await database.status(f"""UPDATE {facts_table} SET category='certifications', fact_type='board_certification'
            WHERE fact_id=(SELECT min(fact_id) FROM {facts_table} WHERE run_id=:run_id)""", run_id=run_id)
        assert (await store.retained_counts(run_id))["invalid_facts"] == 1
        with pytest.raises(RuntimeError, match="retained_integrity_invalid"):
            if limit is None:
                await store.publish_run(run_id, expected_current_run_id=incumbent["run_id"], metrics=_metrics())
            else:
                await store.finish_unpublished_run(run_id, _metrics(limit))
        assert (await store.read_publication())["current_run_id"] == incumbent["run_id"]
        assert (await store._read_run(run_id))["status"] == "running"
        assert await database.scalar(f"SELECT count(*) FROM {facts_table} WHERE run_id=:run_id AND published_at IS NOT NULL", run_id=run_id) == 0


async def test_richer_publication_advances_from_readable_legacy_manifest(monkeypatch):
    async with _database(monkeypatch) as database:
        legacy = await _seed_run(database)
        await store.publish_run(legacy["run_id"], expected_current_run_id=None, metrics=_metrics())
        original_manifest = (await store._read_run(legacy["run_id"]))["source_manifest"]
        assert original_manifest["categories"] == ["education", "training"]
        richer = await _seed_run(database, predecessor=legacy["run_id"], categories=store.PROFILE_CATEGORIES)
        facts_table = store._table(store.ProviderProfileFact)
        await database.status(f"""UPDATE {facts_table} SET category='certifications', fact_type='board_certification'
            WHERE fact_id=(SELECT min(fact_id) FROM {facts_table} WHERE run_id=:run_id)""", run_id=richer["run_id"])
        assert (await store.retained_counts(richer["run_id"]))["invalid_facts"] == 0
        await store.publish_run(richer["run_id"], expected_current_run_id=legacy["run_id"], metrics=_metrics())
        pointer = await store.read_publication()
        assert pointer["current_run_id"] == richer["run_id"] and pointer["previous_run_id"] == legacy["run_id"]
        assert (await store._read_run(legacy["run_id"]))["source_manifest"] == original_manifest


@pytest.mark.parametrize("has_incumbent", [False, True])
async def test_portfolio_only_providers_cannot_replace_guarded_education_coverage(monkeypatch, has_incumbent):
    async with _database(monkeypatch) as database:
        incumbent_id = None
        if has_incumbent:
            incumbent = await _seed_run(database)
            incumbent_id = incumbent["run_id"]
            await store.publish_run(incumbent_id, expected_current_run_id=None, metrics=_metrics())
        candidate = await _seed_run(database, predecessor=incumbent_id, categories=store.PROFILE_CATEGORIES)
        run_id = candidate["run_id"]
        retained_education = 7900 if has_incumbent else 0
        facts_table = store._table(store.ProviderProfileFact)
        await database.status(f"""UPDATE {facts_table} SET category='certifications', fact_type='board_certification'
            WHERE run_id=:run_id AND npi>1000000000+:retained_education""", run_id=run_id, retained_education=retained_education)
        counts = await store.retained_counts(run_id)
        assert counts["matched_public_providers"] == retained_education
        assert counts["portfolio_only_public_providers"] == 10000 - retained_education
        assert counts["invalid_facts"] == 0 and counts["received_profiles"] == 10000
        error = "volume_drop:matched_public_providers" if has_incumbent else "first_publication_too_small"
        with pytest.raises(RuntimeError, match=error):
            await store.publish_run(run_id, expected_current_run_id=incumbent_id, metrics=_metrics())
        pointer = await store.read_publication()
        assert (pointer["current_run_id"] if pointer else None) == incumbent_id
        assert (await store._read_run(run_id))["status"] == "running"
        assert await database.scalar(f"SELECT count(*) FROM {facts_table} WHERE run_id=:run_id AND published_at IS NOT NULL", run_id=run_id) == 0


async def test_portfolio_overlap_counts_once_and_is_reported_separately(monkeypatch):
    async with _database(monkeypatch) as database:
        candidate = await _seed_run(database, limit=3, categories=store.PROFILE_CATEGORIES)
        run_id = candidate["run_id"]
        facts_table = store._table(store.ProviderProfileFact)
        await database.status(f"""INSERT INTO {facts_table}
            (fact_id, run_id, npi, source_record_id, logical_fact_key, category, fact_type, display,
             value_json, availability, assertion_type, verification_status, source_json, sensitive, public_default)
            SELECT fact_id || '-board', run_id, npi, source_record_id, logical_fact_key || '-board',
                   'certifications', 'board_certification', display, value_json, availability,
                   assertion_type, verification_status, source_json, sensitive, public_default
              FROM {facts_table} WHERE run_id=:run_id""", run_id=run_id)
        await database.status(f"""UPDATE {facts_table} SET public_default=false
            WHERE run_id=:run_id AND npi=1000000003 AND category='education'""", run_id=run_id)
        counts = await store.retained_counts(run_id)
        assert counts["retained_facts"] == 6
        assert counts["matched_public_providers"] == 2 and counts["portfolio_only_public_providers"] == 1
        result = await store.finish_unpublished_run(run_id, _metrics(3))
        assert result["matched_public_providers"] == 2 and result["portfolio_only_public_providers"] == 1
        assert (await store._read_run(run_id))["metrics"]["portfolio_only_public_providers"] == 1


async def _seed_reprocessing_parent(database):
    parent = await _seed_run(database)
    await store.publish_run(parent["run_id"], expected_current_run_id=None, metrics=_metrics())
    artifact_by_field = {"artifact_id": "c" * 64, "run_id": parent["run_id"], "source_key": store.SOURCE_KEY,
                "file_name": "manifest.json", "source_url": "https://example.test/manifest.json", "category": "profile", "content_sha256": "d" * 64, "content_bytes": 100}
    await database.insert(store.ProviderProfileArtifact.__table__).values(artifact_by_field).status()
    return parent, artifact_by_field


def _reprocessing_run(parent, artifact, *, limit=2):
    candidate = _run(limit=limit, predecessor=parent["run_id"], categories=store.PROFILE_CATEGORIES)
    candidate["source_manifest"].update(reprocess_from=parent["run_id"], reprocessing={
        "source_run_id": parent["run_id"], "artifact_id": artifact["artifact_id"],
        "manifest_sha256": artifact["content_sha256"], "response_envelopes_sha256": "e" * 64,
    })
    return candidate


async def test_completed_current_parent_is_reprocessed_under_frozen_claim_and_completion(monkeypatch):
    async with _database(monkeypatch) as database:
        parent, artifact = await _seed_reprocessing_parent(database)
        original = await store._read_run(parent["run_id"])
        loaded, loaded_artifact = await store.read_reprocess_run(parent["run_id"], expected_current_run_id=parent["run_id"])
        assert loaded == original and loaded_artifact["content_sha256"] == artifact["content_sha256"]
        candidate = _reprocessing_run(parent, artifact)
        await store.claim_run(candidate)
        await _seed_payloads(database, candidate["run_id"], 2)
        with pytest.raises(RuntimeError, match="reprocessing_incomplete"):
            await store.finish_unpublished_run(candidate["run_id"], _metrics(2))
        metrics_by_field = {**_metrics(2), "reused_responses": 2, "response_envelopes_sha256": "f" * 64}
        assert (await store.finish_unpublished_run(candidate["run_id"], metrics_by_field))["published"] is False
        assert (await store.read_publication())["current_run_id"] == parent["run_id"]
        assert await store._read_run(parent["run_id"]) == original
        with pytest.raises(RuntimeError, match="already_completed"):
            await store.claim_run(candidate)


@pytest.mark.parametrize("change", ["cohort", "source", "artifact", "pointer"])
async def test_reprocessing_rechecks_parent_identity_during_claim(monkeypatch, change):
    async with _database(monkeypatch) as database:
        parent, artifact = await _seed_reprocessing_parent(database)
        candidate = _reprocessing_run(parent, artifact)
        await store.read_reprocess_run(parent["run_id"], expected_current_run_id=parent["run_id"])
        if change == "cohort":
            candidate["source_manifest"]["cohort_sha256"] = "f" * 64
        elif change == "source":
            candidate["source_manifest"]["source"]["coverage_scope"] = "different"
        elif change == "artifact":
            await database.status(f"UPDATE {store._table(store.ProviderProfileArtifact)} SET content_sha256=:digest", digest="f" * 64)
        else:
            next_run = await _seed_run(database, predecessor=parent["run_id"])
            await store.publish_run(next_run["run_id"], expected_current_run_id=parent["run_id"], metrics=_metrics())
        with pytest.raises(RuntimeError, match="parent_changed|predecessor_changed"):
            await store.claim_run(candidate)
        assert await database.scalar(f"SELECT count(*) FROM {store._table(store.ProviderProfileImportRun)} WHERE run_id=:run_id", run_id=candidate["run_id"]) == 0


@pytest.mark.parametrize("state", ["failed", "bounded", "not_current", "missing_artifact", "wrong_artifact"])
async def test_only_completed_full_current_acquisition_with_owned_artifact_is_eligible(monkeypatch, state):
    async with _database(monkeypatch) as database:
        if state in {"failed", "bounded"}:
            parent = await _seed_run(database, limit=2)
            if state == "failed":
                await store.mark_run_failed(parent["run_id"], "synthetic interruption")
            else:
                await store.finish_unpublished_run(parent["run_id"], _metrics(2))
        else:
            parent, _artifact = await _seed_reprocessing_parent(database)
            if state == "not_current":
                next_run = await _seed_run(database, predecessor=parent["run_id"])
                await store.publish_run(next_run["run_id"], expected_current_run_id=parent["run_id"], metrics=_metrics())
            elif state == "missing_artifact":
                await database.status(f"DELETE FROM {store._table(store.ProviderProfileArtifact)}")
            else:
                await database.status(f"UPDATE {store._table(store.ProviderProfileArtifact)} SET file_name='other.json'")
        with pytest.raises(RuntimeError, match="reprocessing_parent_ineligible|predecessor_changed|reprocessing_artifact"):
            await store.read_reprocess_run(parent["run_id"], expected_current_run_id=parent["run_id"])


async def test_failed_reprocessing_never_enters_network_capable_resume(monkeypatch):
    async with _database(monkeypatch) as database:
        parent, artifact = await _seed_reprocessing_parent(database)
        candidate = _reprocessing_run(parent, artifact)
        await store.claim_run(candidate)
        await store.mark_run_failed(candidate["run_id"], "synthetic interrupted copy")
        with pytest.raises(RuntimeError, match="resume_not_eligible"):
            await store.read_resume_run(candidate["run_id"], max_providers=2, expected_current_run_id=parent["run_id"])
        assert (await store.read_publication())["current_run_id"] == parent["run_id"]


@pytest.mark.parametrize("public_count", [7900, 10000])
async def test_full_reprocessing_keeps_atomic_publication_and_existing_coverage_guard(monkeypatch, public_count):
    async with _database(monkeypatch) as database:
        parent, artifact = await _seed_reprocessing_parent(database)
        original = await store._read_run(parent["run_id"])
        candidate = _reprocessing_run(parent, artifact, limit=None)
        await store.claim_run(candidate)
        await _seed_payloads(database, candidate["run_id"], 10000)
        facts_table = store._table(store.ProviderProfileFact)
        await database.status(f"UPDATE {facts_table} SET public_default=false WHERE run_id=:run_id AND npi > :last_npi",
                              run_id=candidate["run_id"], last_npi=1000000000 + public_count)
        metrics_by_field = {**_metrics(), "reused_responses": 10000, "response_envelopes_sha256": "e" * 64}
        if public_count == 7900:
            with pytest.raises(RuntimeError, match="volume_drop:matched_public_providers"):
                await store.publish_run(candidate["run_id"], expected_current_run_id=parent["run_id"], metrics=metrics_by_field)
            assert (await store.read_publication())["current_run_id"] == parent["run_id"]
            assert (await store._read_run(candidate["run_id"]))["status"] == "running"
            expected_published = 0
        else:
            assert (await store.publish_run(candidate["run_id"], expected_current_run_id=parent["run_id"], metrics=metrics_by_field))["published"] is True
            pointer = await store.read_publication()
            assert pointer["current_run_id"] == candidate["run_id"] and pointer["previous_run_id"] == parent["run_id"]
            assert (await store._read_run(candidate["run_id"]))["metrics"]["reused_responses"] == 10000
            expected_published = 10000
        assert await database.scalar(f"SELECT count(*) FROM {facts_table} WHERE run_id=:run_id AND published_at IS NOT NULL", run_id=candidate["run_id"]) == expected_published
        assert await store._read_run(parent["run_id"]) == original


async def test_retained_control_admission_serializes_key_owners_and_rejects_mode_changes(monkeypatch):
    async with _database(monkeypatch) as database:
        table = control_imports.ImportRun.__table__.to_metadata(MetaData(), schema=store.ProviderProfileImportRun.__table__.schema)
        mapped = registry()
        model = type("RetainedImportRun", (), {})
        mapped.map_imperatively(model, table)
        monkeypatch.setattr(control_imports, "ImportRun", model)
        monkeypatch.setattr(control_imports, "db", database)
        try:
            await database.create_table(table)
            request_by_field = {"importer": "massachusetts-borim-profile", "status": "queued", "idempotency_key": "retained-key",
                                "params": {"reprocess_from": "a" * 64, "max_providers": 100}}
            requests = [{**request_by_field, "run_id": uuid.uuid4().hex} for _ in range(2)]
            results = await asyncio.gather(*(control_imports._admit_import_row(
                request["importer"], request, is_ptg_source_file_admission=False) for request in requests))
            owners = await database.all(select(table))
            assert len(owners) == 1 and results.count(None) == 1
            owner_id = owners[0]._mapping["run_id"]
            assert next(result for result in results if result is not None)["run_id"] == owner_id
            await database.status(update(table).values(status="succeeded"))
            replay = await control_imports._admit_massachusetts_import_run({**request_by_field, "run_id": uuid.uuid4().hex})
            assert replay["run_id"] == owner_id and replay["status"] == "succeeded"
            with pytest.raises(ValueError, match="existing_request_mismatch"):
                await control_imports._admit_massachusetts_import_run({**request_by_field, "run_id": uuid.uuid4().hex, "params": {}})
            assert len(await database.all(select(table))) == 1
        finally:
            mapped.dispose()


@pytest.mark.parametrize("change", ["mixed", "missing_lineage", "wrong_parent", "wrong_hash", "orphan_lineage"])
def test_reprocessing_manifest_is_explicit_and_cannot_mix_modes(change):
    parent = _run()
    candidate = _reprocessing_run(parent, {"artifact_id": "c" * 64, "content_sha256": "d" * 64})
    manifest = candidate["source_manifest"]
    if change == "mixed":
        manifest["resume_from"] = "f" * 64
    elif change == "missing_lineage":
        del manifest["reprocessing"]
    elif change == "wrong_parent":
        manifest["reprocessing"]["source_run_id"] = "f" * 64
    elif change == "wrong_hash":
        manifest["reprocessing"]["manifest_sha256"] = "bad"
    else:
        del manifest["reprocess_from"]
    with pytest.raises(ValueError, match="reprocessing_manifest_invalid"):
        store._manifest(candidate)


def test_full_reprocessing_requires_frozen_envelope_digest():
    parent = _run()
    candidate = _reprocessing_run(parent, {"artifact_id": "c" * 64, "content_sha256": "d" * 64}, limit=None)
    with pytest.raises(RuntimeError, match="reprocessing_incomplete"):
        store._completion_metrics(candidate, {**_metrics(), "reused_responses": 10000,
            "response_envelopes_sha256": "f" * 64}, _counts())


async def test_retention_keeps_private_ancestry_under_source_lock(monkeypatch, tmp_path):
    async with _database(monkeypatch) as database:
        run_rows = [_run() for _ in range(4)]
        for index, source_run in enumerate(run_rows):
            source_run.update(status="completed", metrics={"published": True},
                       started_at=store._now() - timedelta(days=30 - index))
        ancestor, parent, current, unrelated = run_rows
        for source_run, previous in ((parent, ancestor), (current, parent)):
            source_run["source_manifest"].update(_reprocessing_run(previous, {
                "artifact_id": "c" * 64, "content_sha256": "d" * 64}, limit=None)["source_manifest"])
        unrelated["started_at"] = store._now() - timedelta(days=60)
        for source_run in run_rows:
            await database.insert(store.ProviderProfileImportRun.__table__).values(source_run).status()
            await _seed_payloads(database, source_run["run_id"], 1)
            (tmp_path / source_run["run_id"]).mkdir()
        await database.insert(store.ProviderProfileSourcePublication.__table__).values(
            source_key=store.SOURCE_KEY, current_run_id=current["run_id"], previous_run_id=parent["run_id"],
            published_at=store._now()).status()
        original_delete = shared_store._delete_retained_payload_rows

        async def delete_locked(run_ids):
            assert await database.scalar("SELECT count(*) FROM pg_locks WHERE pid=pg_backend_pid() AND locktype='advisory' AND granted") > 0
            return await original_delete(run_ids)

        monkeypatch.setattr(shared_store, "_delete_retained_payload_rows", delete_locked)
        receipt = await store.retain_source_history(tmp_path)
        assert receipt["deleted_run_ids"] == [unrelated["run_id"]]
        assert set(receipt["protected_audit_run_ids"]) == {source_run["run_id"] for source_run in (ancestor, parent, current)}
        assert all((tmp_path / source_run["run_id"]).is_dir() for source_run in (ancestor, parent, current))
        assert not (tmp_path / unrelated["run_id"]).exists()
        assert await store.retained_counts(ancestor["run_id"]) == _counts(1)
