# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Focused contracts for Provider Profile run-state and atomic publication."""

from __future__ import annotations

import logging
from types import SimpleNamespace

import pytest

from process.provider_quality_parts import publish_helpers, state


class _MemoryRedis:
    def __init__(self) -> None:
        self.values = {}
        self.lists = {}
        self.sets = {}
        self.expired = []

    async def delete(self, *keys):
        for key in keys:
            self.values.pop(key, None)
            self.lists.pop(key, None)
            self.sets.pop(key, None)

    async def set(self, key, value, *, ex=None, nx=False):
        if nx and key in self.values:
            return 0
        self.values[key] = value
        if ex is not None:
            self.expired.append((key, ex))
        return 1

    async def get(self, key):
        return self.values.get(key)

    async def incrby(self, key, value):
        self.values[key] = int(self.values.get(key, 0)) + value
        return self.values[key]

    async def expire(self, key, ttl):
        self.expired.append((key, ttl))
        return 1

    async def rpush(self, key, value):
        self.lists.setdefault(key, []).append(value)

    async def lrange(self, key, _start, _end):
        return self.lists.get(key, [])

    async def sadd(self, key, value):
        self.sets.setdefault(key, set()).add(value)

    async def srem(self, key, value):
        self.sets.setdefault(key, set()).discard(value)

    async def scard(self, key):
        return len(self.sets.get(key, set()))


class _Transaction:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exception_info):
        return False


class _PublishDatabase:
    def __init__(self, scalar_values=()) -> None:
        self.statements = []
        self.scalar_values = list(scalar_values)

    def transaction(self):
        return _Transaction()

    async def status(self, statement, **_parameters):
        self.statements.append(statement)
        return 1

    async def scalar(self, statement, **_parameters):
        self.statements.append(statement)
        return self.scalar_values.pop(0)


def _publish_classes():
    live_classes = []
    stage_classes_by_name = {}
    for ordinal, name in enumerate(
        ("Qpp", "Svi", "Measure", "Domain", "Score", "Feature")
    ):
        live_class = type(
            f"Live{name}",
            (),
            {
                "__main_table__": f"live_{ordinal}",
                "__my_initial_indexes__": (
                    {"index_elements": ("npi",), "name": f"live_{ordinal}_npi"},
                ),
                "__my_additional_indexes__": (
                    {"index_elements": ()},
                    {"index_elements": ("year",)},
                ),
            },
        )
        stage_class = type(
            live_class.__name__,
            (),
            {"__tablename__": f"stage_{ordinal}"},
        )
        live_classes.append(live_class)
        stage_classes_by_name[live_class.__name__] = stage_class
    return tuple(live_classes), stage_classes_by_name


@pytest.mark.asyncio
async def test_materialize_state_tracks_phases_progress_and_duration(
    monkeypatch,
    caplog,
) -> None:
    redis = _MemoryRedis()
    monkeypatch.setattr(state.time, "time", lambda: 100.0)
    await state._reset_materialize_state(redis, "run")
    await state._set_materialize_phase(redis, "run", "load", total=3)
    assert await state._get_materialize_phase(redis, "run") == "load"
    assert await state._get_materialize_progress(redis, "run") == (3, 0, 0)
    await state._mark_materialize_done(redis, "run")
    await state._mark_materialize_failed(redis, "run")
    assert await state._get_materialize_progress(redis, "run") == (3, 1, 1)
    monkeypatch.setattr(state.time, "time", lambda: 103.25)
    assert await state._get_materialize_phase_elapsed_seconds(redis, "run") == 3.25
    await state._record_materialize_phase_duration(redis, "run", "load", -2.0)
    await state._record_materialize_phase_duration(redis, "run", "load", 4.0)
    redis.lists[state._mat_phase_duration_key("run", "load")].append("invalid")
    with caplog.at_level(logging.INFO, logger="process.provider_quality"):
        await state._log_materialize_phase_summary(
            redis, "run", "load", total=3, done=2, failed=1
        )
    assert "shard_sec_median=2.000" in caplog.text


@pytest.mark.asyncio
async def test_materialize_elapsed_and_empty_summary_fallbacks(
    monkeypatch,
    caplog,
) -> None:
    redis = _MemoryRedis()
    redis.values[state._mat_phase_started_at_key("run")] = b"bad"
    assert await state._get_materialize_phase_elapsed_seconds(redis, "run") == 0.0
    redis.values[state._mat_phase_started_at_key("run")] = b"-1"
    assert await state._get_materialize_phase_elapsed_seconds(redis, "run") == 0.0
    redis.values[state._mat_phase_started_at_key("run")] = b"200"
    monkeypatch.setattr(state.time, "time", lambda: 100.0)
    assert await state._get_materialize_phase_elapsed_seconds(redis, "run") == 0.0
    with caplog.at_level(logging.INFO, logger="process.provider_quality"):
        await state._log_materialize_phase_summary(
            redis, "run", "empty", total=0, done=0, failed=0
        )
    assert "phase=empty total=0" in caplog.text
    assert state._decode_redis_str(b"bytes") == "bytes"
    assert state._decode_redis_str(None) == ""


@pytest.mark.asyncio
async def test_run_state_initialization_progress_and_increment() -> None:
    redis = _MemoryRedis()
    await state._init_run_state(redis, "run", 2)
    assert await state._get_run_progress(redis, "run", 9) == (2, 0)
    await state._increment_total_chunks(redis, "run", 0)
    await state._increment_total_chunks(redis, "run", 3)
    await state._mark_chunk_done(redis, "run", "chunk-a")
    assert await state._get_run_progress(redis, "run", 9) == (5, 1)
    empty = _MemoryRedis()
    assert await state._get_run_progress(empty, "missing", 7) == (7, 0)
    assert state._safe_int(None, 8) == 8
    assert state._safe_int(object(), 6) == 6


@pytest.mark.asyncio
async def test_mark_chunk_done_retries_then_succeeds(monkeypatch) -> None:
    attempts = []
    sleeps = []

    async def flaky_mark(_redis, _run_id, _chunk_id):
        attempts.append(True)
        if len(attempts) < 3:
            raise RuntimeError("temporary")

    async def record_sleep(delay):
        sleeps.append(delay)

    monkeypatch.setattr(state, "_mark_chunk_done", flaky_mark)
    monkeypatch.setattr(state.asyncio, "sleep", record_sleep)
    await state._mark_chunk_done_with_retry(object(), "run", "chunk")
    assert len(attempts) == 3
    assert sleeps == sorted(sleeps)


@pytest.mark.asyncio
async def test_mark_chunk_done_exhausts_retries(monkeypatch) -> None:
    async def failed_mark(_redis, _run_id, _chunk_id):
        raise ValueError("permanent")

    async def no_wait(_delay):
        return None

    monkeypatch.setattr(state, "_mark_chunk_done", failed_mark)
    monkeypatch.setattr(state.asyncio, "sleep", no_wait)
    with pytest.raises(ValueError, match="permanent"):
        await state._mark_chunk_done_with_retry(object(), "run", "chunk")


@pytest.mark.asyncio
async def test_mark_chunk_done_accepts_a_zero_retry_budget(monkeypatch) -> None:
    monkeypatch.setattr(state, "PROVIDER_QUALITY_MARK_DONE_RETRIES", 0)
    await state._mark_chunk_done_with_retry(object(), "run", "chunk")


@pytest.mark.asyncio
async def test_finalize_lock_claim_and_release_are_owner_fenced() -> None:
    redis = _MemoryRedis()
    assert await state._claim_finalize_lock(redis, "run")
    assert await state._claim_finalize_lock(redis, "run")
    assert await state._claim_global_finalize_lock(redis, "owner")
    assert await state._claim_global_finalize_lock(redis, "owner")
    assert not await state._claim_global_finalize_lock(redis, "other")
    await state._release_global_finalize_lock(redis, "other")
    assert state.PROVIDER_QUALITY_GLOBAL_FINALIZE_LOCK_KEY in redis.values
    await state._release_global_finalize_lock(redis, "owner")
    assert state.PROVIDER_QUALITY_GLOBAL_FINALIZE_LOCK_KEY not in redis.values


def _patch_publish_models(monkeypatch, live_classes):
    model_names = (
        "PricingQppProvider",
        "PricingSviZcta",
        "PricingProviderQualityMeasure",
        "PricingProviderQualityDomain",
        "PricingProviderQualityScore",
    )
    for model_name, live_class in zip(
        model_names,
        live_classes[:-1],
        strict=True,
    ):
        monkeypatch.setattr(publish_helpers, model_name, live_class)
    monkeypatch.setattr(
        publish_helpers,
        "_cohort_model_classes",
        lambda: (live_classes[-1],),
    )


@pytest.mark.asyncio
async def test_publish_renames_all_tables_and_indexes(monkeypatch) -> None:
    live_classes, stage_classes_by_name = _publish_classes()
    delattr(live_classes[-1], "__my_initial_indexes__")
    delattr(live_classes[-1], "__my_additional_indexes__")
    database = _PublishDatabase()
    _patch_publish_models(monkeypatch, live_classes)
    monkeypatch.setattr(publish_helpers, "db", database)
    monkeypatch.setattr(publish_helpers, "_table_exists", lambda *_args: _async(True))
    monkeypatch.setattr(
        publish_helpers,
        "_archived_identifier",
        lambda identifier: f"archived_{identifier}",
    )
    await publish_helpers._publish_by_table_rename(stage_classes_by_name, "mrf")
    statements = "\n".join(database.statements)
    assert "ALTER TABLE IF EXISTS mrf.live_0 RENAME TO live_0_old" in statements
    assert "ALTER INDEX IF EXISTS mrf.stage_0_idx_primary" in statements
    assert "archived_live_0_npi" in statements
    assert "live_0_year_idx" in statements


async def _async(value):
    return value


@pytest.mark.asyncio
async def test_publish_refuses_a_missing_stage(monkeypatch) -> None:
    live_classes, stage_classes_by_name = _publish_classes()
    _patch_publish_models(monkeypatch, live_classes)
    monkeypatch.setattr(publish_helpers, "db", _PublishDatabase())
    monkeypatch.setattr(publish_helpers, "_table_exists", lambda *_args: _async(False))
    with pytest.raises(RuntimeError, match="Staging table missing"):
        await publish_helpers._publish_by_table_rename(stage_classes_by_name, "mrf")


@pytest.mark.asyncio
async def test_insert_run_metadata_captures_counts_and_sources(monkeypatch) -> None:
    database = _PublishDatabase((11, b"12", "bad", 14, 15))
    inserted_rows = []

    async def capture_rows(rows, model, *, rewrite):
        inserted_rows.extend(rows)
        assert model is publish_helpers.PricingQualityRun
        assert rewrite is False

    monkeypatch.setattr(publish_helpers, "db", database)
    monkeypatch.setattr(publish_helpers, "_push_objects_with_retry", capture_rows)
    await publish_helpers._insert_run_metadata(
        "mrf",
        "run",
        "import",
        {
            "year": "bad",
            "sources": {
                "qpp_provider": ["qpp"],
                "svi_zcta": ["svi"],
            },
        },
        status="ready",
    )
    assert inserted_rows[0]["qpp_rows"] == 11
    assert inserted_rows[0]["svi_rows"] == 12
    assert inserted_rows[0]["measure_rows"] == 0
    assert inserted_rows[0]["domain_rows"] == 14
    assert inserted_rows[0]["score_rows"] == 15
    assert inserted_rows[0]["year"] == publish_helpers.PROVIDER_QUALITY_MAX_YEAR
    assert inserted_rows[0]["status"] == "ready"
