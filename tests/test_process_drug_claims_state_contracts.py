# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import sys
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, call

import pytest

MODULE_PATH = Path(__file__).resolve().parents[1] / "process" / "drug_claims.py"
MODULE_SPEC = spec_from_file_location("drug_claims_state_contracts", MODULE_PATH)
drug_claims = module_from_spec(MODULE_SPEC)
assert MODULE_SPEC and MODULE_SPEC.loader
sys.modules["drug_claims_state_contracts"] = drug_claims
MODULE_SPEC.loader.exec_module(drug_claims)


class _FinalizeLockRedis:
    def __init__(self):
        self.values_by_key = {}

    async def set(self, key, value, *, ex, nx):
        assert ex > 0
        if nx and key in self.values_by_key:
            return 0
        self.values_by_key[key] = value
        return 1

    async def eval(self, _script, key_count, key, owner_token):
        assert key_count == 1
        if self.values_by_key.get(key) != owner_token:
            return 0
        del self.values_by_key[key]
        return 1


@pytest.mark.parametrize(
    ("environment_value", "default_value", "is_enabled"),
    [
        (None, True, True),
        (" YES ", False, True),
        ("off", True, False),
    ],
)
def test_environment_flag_contract(monkeypatch, environment_value, default_value, is_enabled):
    monkeypatch.setattr(drug_claims.os, "getenv", lambda name: environment_value)
    assert drug_claims._is_env_enabled("SYNTHETIC_FLAG", default_value) is is_enabled


def test_identifier_and_run_identity_contract(monkeypatch):
    warnings = []
    monkeypatch.setattr(drug_claims.logger, "warning", lambda *args: warnings.append(args))
    monkeypatch.setattr(drug_claims.secrets, "token_hex", lambda size: "1234abcd")

    assert drug_claims._sanitize_identifier("", "fallback", "FIELD") == "fallback"
    assert drug_claims._sanitize_identifier("safe_name", "fallback", "FIELD") == "safe_name"
    assert drug_claims._sanitize_identifier("bad-name", "fallback", "FIELD") == "fallback"
    assert warnings
    assert drug_claims._normalize_run_id(" run:one ") == "run_one"
    generated_run_id = drug_claims._normalize_run_id("::")
    assert generated_run_id.endswith("_1234abcd")
    assert drug_claims._normalize_import_id(" import/one ") == "_import_one_"
    assert drug_claims._normalize_import_id("***") == "___"


def test_paths_manifest_and_numeric_helpers(tmp_path, monkeypatch):
    monkeypatch.setattr(drug_claims, "DRUG_CLAIMS_WORKDIR", str(tmp_path))
    run_directory = drug_claims._run_dir("import-one", "run-one")
    manifest_path = drug_claims._manifest_path(run_directory)
    manifest_by_field = {"run_id": "run-one", "total_chunks": 2}
    drug_claims._write_manifest(manifest_path, manifest_by_field)

    assert drug_claims._read_manifest(str(manifest_path)) == manifest_by_field
    assert drug_claims._state_key("run-one", "done") == "drug_claims:run-one:done"
    assert drug_claims._chunk_job_id("run", "spending", 2, 2023, 4).endswith(
        "spending_2023_2_4"
    )
    assert drug_claims._safe_int(None, 7) == 7
    assert drug_claims._safe_int(b"8") == 8
    assert drug_claims._safe_int("bad", 9) == 9
    assert drug_claims._build_stage_suffix("import-one", "run-one").startswith("import_one_")


@pytest.mark.asyncio
async def test_redis_run_state_contract(monkeypatch):
    redis = SimpleNamespace(
        delete=AsyncMock(),
        set=AsyncMock(side_effect=[None, True]),
        expire=AsyncMock(),
        sadd=AsyncMock(),
        srem=AsyncMock(),
        incrby=AsyncMock(),
        get=AsyncMock(return_value=b"4"),
        scard=AsyncMock(return_value=b"3"),
    )
    monkeypatch.setattr(drug_claims, "DRUG_CLAIMS_REDIS_TTL_SECONDS", 99)

    await drug_claims._init_run_state(redis, "run-one", 4)
    await drug_claims._increment_total_chunks(redis, "run-one", 0)
    await drug_claims._increment_total_chunks(redis, "run-one", 2)
    await drug_claims._mark_chunk_done(redis, "run-one", "chunk-one")
    assert await drug_claims._get_run_progress(redis, "run-one", 1) == (4, 3)
    assert await drug_claims._has_claimed_finalize_lock(redis, "run-one") is True

    redis.delete.assert_awaited_once()
    redis.incrby.assert_awaited_once_with("drug_claims:run-one:total_chunks", 2)
    redis.sadd.assert_has_awaits(
        [
            call("drug_claims:run-one:done_chunks", "__init__"),
            call("drug_claims:run-one:done_chunks", "chunk-one"),
        ]
    )


@pytest.mark.asyncio
async def test_finalize_lock_release_requires_the_owner_token(monkeypatch):
    redis = _FinalizeLockRedis()
    monkeypatch.setattr(drug_claims, "DRUG_CLAIMS_REDIS_TTL_SECONDS", 99)
    lock_key = drug_claims._state_key("run-one", "finalize_lock")

    assert await drug_claims._has_claimed_finalize_lock(
        redis,
        "run-one",
        "owner-one",
    )
    assert not await drug_claims._has_claimed_finalize_lock(
        redis,
        "run-one",
        "owner-two",
    )
    assert not await drug_claims._is_finalize_lock_released(
        redis,
        "run-one",
        "owner-two",
    )
    assert redis.values_by_key[lock_key] == "owner-one"
    assert await drug_claims._is_finalize_lock_released(
        redis,
        "run-one",
        "owner-one",
    )
    assert lock_key not in redis.values_by_key


@pytest.mark.asyncio
async def test_mark_done_retries_and_surfaces_final_error(monkeypatch):
    retry_error = RuntimeError("redis unavailable")
    mark_done = AsyncMock(side_effect=[retry_error, None])
    sleep = AsyncMock()
    monkeypatch.setattr(drug_claims, "_mark_chunk_done", mark_done)
    monkeypatch.setattr(drug_claims.asyncio, "sleep", sleep)
    monkeypatch.setattr(drug_claims, "DRUG_CLAIMS_MARK_DONE_RETRIES", 2)
    monkeypatch.setattr(drug_claims, "DRUG_CLAIMS_MARK_DONE_RETRY_BASE_SECONDS", 0.25)
    monkeypatch.setattr(drug_claims, "DRUG_CLAIMS_MARK_DONE_RETRY_MAX_SECONDS", 1.0)

    await drug_claims._mark_chunk_done_with_retry(object(), "run-one", "chunk-one")
    sleep.assert_awaited_once_with(0.25)

    mark_done.reset_mock(side_effect=True)
    mark_done.side_effect = [retry_error, retry_error]
    with pytest.raises(RuntimeError, match="redis unavailable"):
        await drug_claims._mark_chunk_done_with_retry(object(), "run-one", "chunk-two")


@pytest.mark.asyncio
async def test_database_push_retry_contract(monkeypatch):
    deadlock_error = RuntimeError("deadlock detected while writing")
    push = AsyncMock(side_effect=[deadlock_error, None])
    sleep = AsyncMock()
    model_class = SimpleNamespace(__tablename__="synthetic_stage")
    monkeypatch.setattr(drug_claims, "push_objects", push)
    monkeypatch.setattr(drug_claims.asyncio, "sleep", sleep)
    monkeypatch.setattr(drug_claims, "DRUG_CLAIMS_DB_DEADLOCK_RETRIES", 2)
    monkeypatch.setattr(drug_claims, "DRUG_CLAIMS_DB_DEADLOCK_BASE_DELAY_SECONDS", 0.1)

    await drug_claims._push_objects_with_retry([], model_class)
    await drug_claims._push_objects_with_retry([{"code": "one"}], model_class)
    sleep.assert_awaited_once_with(0.1)
    assert drug_claims._is_deadlock_error(deadlock_error) is True
    assert drug_claims._is_deadlock_error(RuntimeError("other")) is False

    push.reset_mock(side_effect=True)
    push.side_effect = RuntimeError("constraint failure")
    with pytest.raises(RuntimeError, match="constraint failure"):
        await drug_claims._push_objects_with_retry([{"code": "two"}], model_class)


def test_collection_and_nullable_sum_contract():
    duplicate_rows = [
        {"code": "one", "amount": 1},
        {"code": "one", "amount": 2},
        {"code": "two", "amount": 3},
    ]
    assert drug_claims._dedupe_rows([], ("code",)) == []
    assert drug_claims._dedupe_rows(duplicate_rows, ("code",)) == [
        {"code": "one", "amount": 2},
        {"code": "two", "amount": 3},
    ]
    assert drug_claims._chunk_rows([], 2) == []
    assert drug_claims._chunk_rows(duplicate_rows, 2) == [
        duplicate_rows[:2],
        duplicate_rows[2:],
    ]
    assert drug_claims._sum_optional(None, None) is None
    assert drug_claims._sum_optional(None, 2.5) == 2.5
    assert drug_claims._sum_optional(1.5, None) == 1.5


def test_progress_contract_uses_nonzero_elapsed(monkeypatch, capsys):
    monotonic_values = iter([10.0, 10.0, 12.0])
    monkeypatch.setattr(drug_claims.time, "monotonic", lambda: next(monotonic_values))

    started_at = drug_claims._step_start("synthetic phase")
    drug_claims._step_end("synthetic phase", started_at)
    drug_claims._print_row_progress("synthetic", 4, 3, 10.0, final=True)

    progress_output = capsys.readouterr().out
    assert "[step] START synthetic phase" in progress_output
    assert "[step] DONE  synthetic phase" in progress_output
    assert "parsed=4 accepted=3" in progress_output
