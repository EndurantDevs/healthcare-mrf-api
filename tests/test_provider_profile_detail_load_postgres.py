# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Continuous real detail reads across the supported native Doctors cutover."""

import asyncio
import json
import math
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import asdict
from time import perf_counter
from types import SimpleNamespace

import pytest
from sqlalchemy import event, text

from api import provider_profile_snapshot as snapshot
from tests.cms_doctors_preparation_postgres_support import authority, native, stage_oids
from tests.provider_profile_detail_load_postgres_support import NPI, detail_database, npi_api


def _distribution(values):
    ordered = sorted(values)
    return (
        {
            name: round(ordered[min(math.ceil(fraction * len(ordered)) - 1, len(ordered) - 1)], 3)
            for name, fraction in (("p50", 0.5), ("p95", 0.95), ("p99", 0.99), ("max", 1))
        }
        if ordered
        else {}
    )


def _error(error):
    return {
        "type": type(error).__name__,
        "status": getattr(error, "status_code", None),
        "sqlstate": getattr(getattr(error, "orig", error), "sqlstate", None),
        "message": str(error),
    }


class _Measurements:
    """Observe production calls; do not replace data, locks, transactions, or retry policy."""

    def __init__(self):
        self.request = ContextVar("detail_load_request", default=None)
        self.requests = []
        self.attempts = []
        self.first_snapshot = asyncio.Event()
        self.started = perf_counter()

    def milliseconds(self):
        return round((perf_counter() - self.started) * 1000, 3)

    def before_query(self, _connection, _cursor, _statement, _parameters, _context, _many):
        sample_by_field = self.request.get()
        if sample_by_field is not None:
            sample_by_field["queries"] += 1

    def install(self, monkeypatch):
        read_snapshot = npi_api.provider_profile_read_snapshot
        lock_relations = snapshot._lock_serving_relations
        prelock = native._lock_cms_doctors_publication
        apply_stage = native._apply_cms_doctors_stage

        @asynccontextmanager
        async def measured_snapshot(*args, **kwargs):
            started = perf_counter()
            sample_by_field = self.request.get()
            try:
                async with read_snapshot(*args, **kwargs) as session:
                    if sample_by_field is not None:
                        sample_by_field["generation"] = session.info["provider_profile_native_authorities"][
                            "cms-doctors"
                        ].local_generation
                        self.first_snapshot.set()
                    yield session
            finally:
                if sample_by_field is not None:
                    sample_by_field["transaction_ms"] = round((perf_counter() - started) * 1000, 3)

        async def measured_locks(*args, **kwargs):
            started = perf_counter()
            result = await lock_relations(*args, **kwargs)
            sample_by_field = self.request.get()
            if sample_by_field is not None:
                assert len(result) == 57 and all(result.values())
                sample_by_field["relations"] = len(result)
                sample_by_field["lock_ms"] = round((perf_counter() - started) * 1000, 3)
            return result

        async def measured_prelock(*args, **kwargs):
            attempt_by_field = {"started_ms": self.milliseconds()}
            self.attempts.append(attempt_by_field)
            try:
                await prelock(*args, **kwargs)
                attempt_by_field["acquired_ms"] = self.milliseconds()
            except Exception as error:
                attempt_by_field["error"] = _error(error)
                raise
            finally:
                attempt_by_field["prelock_finished_ms"] = self.milliseconds()

        async def measured_apply(*args, **kwargs):
            result = await apply_stage(*args, **kwargs)
            session = native.db._transaction_binding().session
            self.attempts[-1]["transaction_id"] = str(await session.scalar(text("SELECT pg_current_xact_id()")))
            self.attempts[-1]["apply_finished_ms"] = self.milliseconds()
            return result

        monkeypatch.setattr(npi_api, "provider_profile_read_snapshot", measured_snapshot)
        monkeypatch.setattr(snapshot, "_lock_serving_relations", measured_locks)
        monkeypatch.setattr(native, "_lock_cms_doctors_publication", measured_prelock)
        monkeypatch.setattr(native, "_apply_cms_doctors_stage", measured_apply)

    async def read(self, worker, phase):
        sample_by_field = {"worker": worker, "phase": phase, "started_ms": self.milliseconds(), "queries": 0}
        token = self.request.set(sample_by_field)
        started = perf_counter()
        try:
            response = await npi_api.get_npi(
                SimpleNamespace(
                    args={
                        "view": "full",
                        "include_sources": "true",
                        "include_evidence": "true",
                        "sync_geocode": "false",
                        "lookup_stored_geocode": "false",
                    }
                ),
                str(NPI),
            )
            sample_by_field["status"] = response.status
            assert response.status == 200
            return json.loads(response.body)
        except Exception as error:
            sample_by_field["error"] = _error(error)
            raise
        finally:
            sample_by_field["latency_ms"] = round((perf_counter() - started) * 1000, 3)
            self.requests.append(sample_by_field)
            self.request.reset(token)


def _assert_detail(payload):
    assert payload["npi"] == NPI and payload["provider_first_name"] == "Synthetic"
    assert payload["taxonomy_list"][0]["healthcare_provider_taxonomy_code"] == "207Q00000X"
    assert len(payload["address_list"]) == 4
    assert {row["first_line"] for row in payload["address_list"]} == {f"{i} Example Street" for i in range(1, 5)}
    assert payload["other_name_list"][0]["other_provider_identifier"] == "Synthetic Practice"
    assert payload["provider_directory_profile"]["languages"][0]["code"] == "en"
    assert payload["provider_directory_profile_evidence"]["source_id"] == "synthetic-directory"
    enrichment = payload["provider_enrichment"]
    assert enrichment["summary"]["has_ffs_enrollment"] is True
    assert enrichment["enrollments"]["ffs_public"][0]["enrollment_id"] == "synthetic-enrollment"
    for key in ("additional_npis", "practice_locations", "secondary_specialties", "reassignments_out"):
        assert len(enrichment["ffs_subfiles"][key]) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("cache_enabled", [False, True], ids=["full-loaders", "warm-cache"])
async def test_full_detail_reads_during_native_doctors_publication(monkeypatch, tmp_path, cache_enabled):
    """Three continuous readers must retain coherence and permit the ordinary cutover."""
    async with detail_database(monkeypatch, tmp_path) as fixture:
        monkeypatch.setattr(npi_api, "_NPI_DETAIL_RESPONSE_CACHE_TTL_SECONDS", 300 if cache_enabled else 0)
        measurements = _Measurements()
        measurements.install(monkeypatch)
        event.listen(fixture.engine.sync_engine, "before_cursor_execute", measurements.before_query)
        workers = []
        publisher = None
        publication_by_field = {}
        finished = asyncio.Event()
        before = await authority(fixture)
        stages = await stage_oids(fixture, fixture.stage)
        try:
            expected = await measurements.read("setup", "before")
            _assert_detail(expected)
            measurements.first_snapshot.clear()

            async def reader(worker):
                after_count = 0
                while after_count < 3:
                    phase = "after" if finished.is_set() else "during"
                    assert await measurements.read(worker, phase) == expected
                    after_count += phase == "after"

            async def publish():
                await measurements.first_snapshot.wait()
                publication_by_field["started_ms"] = measurements.milliseconds()
                try:
                    stage = native.make_class(native.DoctorClinicianAddress, fixture.stage["import_date"])
                    result = await native._publish_cms_doctors_stage(
                        stage, fixture.schema, fixture.stage["import_date"]
                    )
                    publication_by_field["committed_authority"] = asdict(result)
                except Exception as error:
                    publication_by_field["error"] = _error(error)
                finally:
                    publication_by_field["finished_ms"] = measurements.milliseconds()
                    finished.set()

            workers = [asyncio.create_task(reader(worker), name=f"detail-reader-{worker}") for worker in range(3)]
            publisher = asyncio.create_task(publish(), name="doctors-publisher")
            await asyncio.wait_for(asyncio.gather(publisher, *workers), timeout=10)
            after = await authority(fixture)
            publication_by_field["observed_authority"] = asdict(after)
            _assert_publication(measurements, publication_by_field, before, after, stages)
        finally:
            for task in [publisher, *workers]:
                if task is not None and not task.done():
                    task.cancel()
            await asyncio.gather(*(task for task in [publisher, *workers] if task is not None), return_exceptions=True)
            event.remove(fixture.engine.sync_engine, "before_cursor_execute", measurements.before_query)
            _write_receipt(tmp_path, fixture, measurements, before, publication_by_field, cache_enabled)


def _write_receipt(tmp_path, fixture, measurements, before, publication_by_field, cache_enabled):
    """Retain actual attempts and every request, including failing publication evidence."""
    receipt_by_field = {
        "cache_enabled": cache_enabled,
        "reader_count": 3,
        "cadence": "continuous; no sleeps or quiet windows",
        "relation_count": 57,
        "before_authority": asdict(before),
        "npi_identity": fixture.npi_identity,
        "publication": publication_by_field,
        "attempts": measurements.attempts,
        "requests": measurements.requests,
        "latency_ms": _distribution([sample_by_field["latency_ms"] for sample_by_field in measurements.requests]),
        "snapshot_lock_ms": _distribution(
            [sample_by_field["lock_ms"] for sample_by_field in measurements.requests if "lock_ms" in sample_by_field]
        ),
        "snapshot_transaction_ms": _distribution(
            [sample_by_field["transaction_ms"] for sample_by_field in measurements.requests]
        ),
        "scope": "Synthetic native fixture; not representative deployment load or full import proof.",
    }
    (tmp_path / "detail-load.json").write_text(json.dumps(receipt_by_field, default=str, indent=2) + "\n")


def _assert_publication(measurements, publication_by_field, before, after, stages):
    """Require a committed new native OID family and coherent successful reads on both sides."""
    assert all(
        not sample_by_field.get("error") and sample_by_field.get("relations") == 57
        for sample_by_field in measurements.requests
    )
    assert "error" not in publication_by_field, publication_by_field
    assert after.local_generation == before.local_generation + 1
    oid_by_target = {target_name: oid for target_name, _stage, oid in stages}
    assert after.relation_oids == tuple(oid_by_target[name] for name in snapshot._DOCTORS_TABLES)
    assert {sample_by_field["generation"] for sample_by_field in measurements.requests} == {
        before.local_generation,
        after.local_generation,
    }
