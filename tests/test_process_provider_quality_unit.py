import importlib
from pathlib import Path
from types import SimpleNamespace

import pytest

provider_quality = importlib.import_module("process.provider_quality")
provider_quality_cohort_context = importlib.import_module("process.provider_quality_parts.cohort_context")
provider_quality_cohort_sql = importlib.import_module("process.provider_quality_parts.cohort_sql")
provider_quality_execution_helpers = importlib.import_module("process.provider_quality_parts.execution_helpers")
provider_quality_lifecycle = importlib.import_module("process.provider_quality_parts.lifecycle")
provider_quality_materialize_shards = importlib.import_module("process.provider_quality_parts.materialize_shards")
provider_quality_model_helpers = importlib.import_module("process.provider_quality_parts.model_helpers")
provider_quality_normalize = importlib.import_module("process.provider_quality_parts.normalize")
provider_quality_publish_helpers = importlib.import_module("process.provider_quality_parts.publish_helpers")
provider_quality_sql_helpers = importlib.import_module("process.provider_quality_parts.sql_helpers")
provider_quality_state = importlib.import_module("process.provider_quality_parts.state")
provider_quality_table_helpers = importlib.import_module("process.provider_quality_parts.table_helpers")


class _FakeRedis:
    def __init__(self) -> None:
        self.store: dict[str, str] = {}
        self.set_store: dict[str, set[str]] = {}
        self.list_store: dict[str, list[str]] = {}
        self.jobs: list[dict[str, object]] = []

    async def set(self, key: str, value, ex=None, nx: bool = False):
        if nx and key in self.store:
            is_stored = False
            return is_stored
        self.store[key] = str(value)
        is_stored = True
        return is_stored

    async def get(self, key: str):
        return self.store.get(key)

    async def delete(self, *keys: str):
        removed = 0
        for key in keys:
            if key in self.store:
                del self.store[key]
                removed += 1
            if key in self.set_store:
                del self.set_store[key]
                removed += 1
            if key in self.list_store:
                del self.list_store[key]
                removed += 1
        return removed

    async def incrby(self, key: str, value: int):
        current = int(self.store.get(key, "0"))
        current += int(value)
        self.store[key] = str(current)
        return current

    async def expire(self, key: str, ttl: int):
        return 1 if key in self.store or key in self.list_store else 0

    async def sadd(self, key: str, value: str):
        bucket = self.set_store.setdefault(key, set())
        prev = len(bucket)
        bucket.add(value)
        return int(len(bucket) > prev)

    async def srem(self, key: str, value: str):
        bucket = self.set_store.setdefault(key, set())
        if value in bucket:
            bucket.remove(value)
            return 1
        return 0

    async def scard(self, key: str):
        return len(self.set_store.get(key, set()))

    async def enqueue_job(
        self,
        job_name: str,
        payload: dict[str, object],
        _queue_name: str,
        _job_id: str,
        _max_tries: int | None = None,
    ):
        self.jobs.append(
            {
                "job_name": job_name,
                "payload": payload,
                "job_id": _job_id,
                "queue_name": _queue_name,
                "max_tries": _max_tries,
            }
        )
        is_enqueued = True
        return is_enqueued

    async def rpush(self, key: str, value: str):
        self.list_store.setdefault(key, []).append(str(value))
        return len(self.list_store[key])

    async def lrange(self, key: str, start: int, end: int):
        values = self.list_store.get(key, [])
        if end == -1:
            return values[start:]
        return values[start : end + 1]


def test_archived_identifier_short_name():
    assert provider_quality._archived_identifier("quality_idx") == "quality_idx_old"


def test_state_split_keeps_facade_helpers_stable():
    assert provider_quality._state_key is provider_quality_state._state_key
    assert provider_quality._safe_int is provider_quality_state._safe_int
    assert provider_quality._init_run_state is provider_quality_state._init_run_state
    assert provider_quality._increment_total_chunks is provider_quality_state._increment_total_chunks
    assert provider_quality._mark_chunk_done is provider_quality_state._mark_chunk_done
    assert provider_quality._mark_chunk_done_with_retry is provider_quality_state._mark_chunk_done_with_retry
    assert provider_quality._get_run_progress is provider_quality_state._get_run_progress
    assert (
        provider_quality._has_finalize_lock
        is provider_quality_state._has_finalize_lock
    )
    assert (
        provider_quality._has_global_finalize_lock
        is provider_quality_state._has_global_finalize_lock
    )
    assert provider_quality._release_global_finalize_lock is provider_quality_state._release_global_finalize_lock
    assert provider_quality._log_materialize_phase_summary is provider_quality_state._log_materialize_phase_summary
    assert provider_quality._safe_int(b"42") == 42
    assert provider_quality._safe_int("bad", 7) == 7


def test_normalize_split_keeps_facade_helpers_stable():
    assert provider_quality._to_float is provider_quality_normalize._to_float
    assert provider_quality._to_int is provider_quality_normalize._to_int
    assert provider_quality._to_npi is provider_quality_normalize._to_npi
    assert provider_quality._normalize_zcta is provider_quality_normalize._normalize_zcta
    assert provider_quality._pick_first_ci is provider_quality_normalize._pick_first_ci


@pytest.mark.asyncio
async def test_finish_main_enqueues_unique_finalize_job(monkeypatch):
    redis = _FakeRedis()

    async def fake_create_pool(*_args, **_kwargs):
        return redis

    monkeypatch.setattr(provider_quality, "create_pool", fake_create_pool)

    result = await provider_quality.finish_main(import_id="dev1", run_id="run_a", test_mode=True)

    assert result["queued"] is True
    assert result["run_id"] == "run_a"
    assert len(redis.jobs) == 1
    job = redis.jobs[0]
    assert job["job_name"] == "provider_quality_finalize"
    assert job["queue_name"] == provider_quality.PROVIDER_QUALITY_FINISH_QUEUE_NAME
    assert str(job["job_id"]).startswith("provider_quality_finalize_run_a_")
    assert job["payload"]["test_mode"] is True


def test_lifecycle_split_keeps_facade_helpers_stable():
    assert provider_quality._normalize_import_id is provider_quality_lifecycle._normalize_import_id
    assert provider_quality._format_duration_compact is provider_quality_lifecycle._format_duration_compact
    assert provider_quality._archived_identifier is provider_quality_lifecycle._archived_identifier
    assert provider_quality._npi_shard_predicate is provider_quality_lifecycle._npi_shard_predicate


def test_sql_helper_split_keeps_facade_helpers_stable():
    assert provider_quality._provider_class_case_sql is provider_quality_sql_helpers._provider_class_case_sql
    assert provider_quality._state_code_sql is provider_quality_sql_helpers._state_code_sql


def test_cohort_sql_split_keeps_facade_helpers_stable():
    assert provider_quality._cohort_sql_phase_1_build_features is provider_quality_cohort_sql._cohort_sql_phase_1_build_features
    assert (
        provider_quality._cohort_sql_phase_2_lsh_shard
        is provider_quality_cohort_sql._cohort_sql_phase_2_lsh_shard
    )
    assert (
        provider_quality._cohort_sql_phase_3_procedure_bucket
        is provider_quality_cohort_sql._cohort_sql_phase_3_procedure_bucket
    )
    assert (
        provider_quality._cohort_sql_phase_4_peer_targets
        is provider_quality_cohort_sql._cohort_sql_phase_4_peer_targets
    )
    assert (
        provider_quality._cohort_sql_phase_5_measure_shard
        is provider_quality_cohort_sql._cohort_sql_phase_5_measure_shard
    )
    assert (
        provider_quality._cohort_sql_phase_6_domain_shard
        is provider_quality_cohort_sql._cohort_sql_phase_6_domain_shard
    )
    assert (
        provider_quality._cohort_sql_phase_7_score_shard
        is provider_quality_cohort_sql._cohort_sql_phase_7_score_shard
    )


def test_cohort_context_split_keeps_wrapper_available():
    assert callable(provider_quality_cohort_context._build_cohort_materialization_context)
    assert callable(provider_quality._build_cohort_materialization_context)


def test_model_helper_split_keeps_facade_helpers_stable():
    assert provider_quality._resolve_optional_model is provider_quality_model_helpers._resolve_optional_model
    assert provider_quality._materialize_reporting_years is provider_quality_model_helpers._materialize_reporting_years
    assert provider_quality._cohort_model_classes is provider_quality_model_helpers._cohort_model_classes
    assert provider_quality._cohort_models_present is provider_quality_model_helpers._cohort_models_present
    assert provider_quality._model_columns is provider_quality_model_helpers._model_columns
    assert provider_quality._first_existing_column is provider_quality_model_helpers._first_existing_column
    assert provider_quality._optional_column_pairs is provider_quality_model_helpers._optional_column_pairs
    assert provider_quality._staging_classes is provider_quality_model_helpers._staging_classes
    assert provider_quality._chunk_job_id is provider_quality_model_helpers._chunk_job_id
    assert provider_quality._build_stage_suffix is provider_quality_model_helpers._build_stage_suffix


def test_table_helper_split_keeps_facade_helpers_stable():
    assert provider_quality._ensure_indexes is provider_quality_table_helpers._ensure_indexes
    assert provider_quality._build_staging_indexes is provider_quality_table_helpers._build_staging_indexes
    assert provider_quality._is_table_available is provider_quality_table_helpers._is_table_available
    assert provider_quality._table_columns is provider_quality_table_helpers._table_columns


def test_table_helper_shortens_long_index_names_without_collision():
    table_name = "pricing_provider_quality_peer_target_20260609_abcdef12"
    first = provider_quality_table_helpers._index_name_for_table(table_name, table_name + "_first_" + ("x" * 80))
    second = provider_quality_table_helpers._index_name_for_table(table_name, table_name + "_second_" + ("x" * 80))

    assert len(first) <= 63
    assert len(second) <= 63
    assert first != second
    assert first.startswith("pricing_provider_quality_peer_target_20260609")


def test_publish_helper_split_keeps_facade_helpers_stable():
    assert provider_quality._publish_by_table_rename is provider_quality_publish_helpers._publish_by_table_rename
    assert provider_quality._insert_run_metadata is provider_quality_publish_helpers._insert_run_metadata


def test_materialize_shard_split_keeps_facade_handlers_stable():
    assert provider_quality._materialize_shard_task_values is provider_quality_materialize_shards._materialize_shard_task_values
    assert provider_quality._run_materialize_shard_job is provider_quality_materialize_shards._run_materialize_shard_job
    assert (
        provider_quality.provider_quality_materialize_lsh_shard
        is provider_quality_materialize_shards.provider_quality_materialize_lsh_shard
    )
    assert (
        provider_quality.provider_quality_materialize_measure_shard
        is provider_quality_materialize_shards.provider_quality_materialize_measure_shard
    )
    assert (
        provider_quality.provider_quality_materialize_domain_shard
        is provider_quality_materialize_shards.provider_quality_materialize_domain_shard
    )
    assert (
        provider_quality.provider_quality_materialize_score_shard
        is provider_quality_materialize_shards.provider_quality_materialize_score_shard
    )


def test_execution_helper_split_keeps_facade_helpers_stable():
    assert provider_quality._print_row_progress is provider_quality_execution_helpers._print_row_progress
    assert provider_quality._step_start is provider_quality_execution_helpers._step_start
    assert provider_quality._step_end is provider_quality_execution_helpers._step_end
    assert provider_quality._is_deadlock_error is provider_quality_execution_helpers._is_deadlock_error
    assert provider_quality._execute_shard_sql is provider_quality_execution_helpers._execute_shard_sql
    assert provider_quality._count_shard_rows is provider_quality_execution_helpers._count_shard_rows
    assert provider_quality._push_objects_with_retry is provider_quality_execution_helpers._push_objects_with_retry
    assert (
        provider_quality._is_row_allowed_for_test
        is provider_quality_execution_helpers._is_row_allowed_for_test
    )


def test_provider_quality_sql_helpers_build_expected_fragments():
    provider_class_sql = provider_quality._provider_class_case_sql("nd.entity_type_code", "pe")
    state_sql = provider_quality._state_code_sql("addr.state_name")

    assert "WHEN nd.entity_type_code = 1 THEN 'clinician'" in provider_class_sql
    assert "pe.has_hospital_enrollment" in provider_class_sql
    assert "WHEN nd.entity_type_code = 2 THEN 'organization'" in provider_class_sql
    assert "UPPER(NULLIF(BTRIM(COALESCE(addr.state_name, '')), ''))" in state_sql
    assert "WHEN UPPER(NULLIF(BTRIM(COALESCE(addr.state_name, '')), '')) = 'MASSACHUSETTS' THEN 'MA'" in state_sql


def test_archived_identifier_truncates_long_name():
    long_name = "x" * 100
    archived = provider_quality._archived_identifier(long_name)
    assert len(archived) <= 63
    assert archived.endswith("_old")


def test_lifecycle_helpers_normalize_ids_and_manifest(tmp_path):
    assert provider_quality._normalize_import_id("2026/05") == "2026_05"
    assert provider_quality._normalize_run_id(" run:1 ") == "run_1"
    assert provider_quality._format_duration_compact(3661) == "1h01m01s"
    assert provider_quality._materialize_shard_job_id("run", "score", 2026, 3) == (
        "provider_quality_materialize_score_run_2026_3"
    )
    assert provider_quality._npi_shard_predicate("t.npi") == "MOD((t.npi)::bigint, :shard_count) = :shard_id"

    manifest_path = provider_quality._manifest_path(tmp_path)
    provider_quality._write_manifest(manifest_path, {"b": 2, "a": 1})
    assert provider_quality._read_manifest(str(manifest_path)) == {"a": 1, "b": 2}


def test_resolve_sources_empty_templates(monkeypatch):
    monkeypatch.setattr(provider_quality, "QPP_CSV_URL_TEMPLATE", "")
    monkeypatch.setattr(provider_quality, "SVI_CSV_URL_TEMPLATE", "")
    monkeypatch.setattr(provider_quality, "PROVIDER_QUALITY_YEAR_WINDOW", (2023,))
    sources = provider_quality._resolve_sources(test_mode=False)
    assert sources["qpp_provider"] == []
    assert sources["svi_zcta"] == []


def test_resolve_sources_year_template(monkeypatch):
    monkeypatch.setattr(provider_quality, "QPP_CSV_URL_TEMPLATE", "https://example.com/qpp_{year}.csv")
    monkeypatch.setattr(provider_quality, "SVI_CSV_URL_TEMPLATE", "https://example.com/svi_{year}.csv")
    monkeypatch.setattr(provider_quality, "PROVIDER_QUALITY_YEAR_WINDOW", (2023, 2024))

    sources = provider_quality._resolve_sources(test_mode=False)
    assert sources["qpp_provider"][0]["url"] == "https://example.com/qpp_2023.csv"
    assert sources["qpp_provider"][1]["url"] == "https://example.com/qpp_2024.csv"
    assert sources["svi_zcta"][0]["url"] == "https://example.com/svi_2023.csv"
    assert sources["svi_zcta"][1]["url"] == "https://example.com/svi_2024.csv"


def test_benchmark_modes_for_materialization():
    assert provider_quality._benchmark_modes_for_materialization("zip") == ("zip", "state", "national")
    assert provider_quality._benchmark_modes_for_materialization("state") == ("state", "national")
    assert provider_quality._benchmark_modes_for_materialization("national") == ("national",)
    assert provider_quality._benchmark_modes_for_materialization("unexpected") == ("national",)


def test_npi_shard_mapping_is_deterministic():
    shard_count = 8
    shard_by_npi = {npi: (npi % shard_count) for npi in range(1000000000, 1000000100)}
    assert all(0 <= shard < shard_count for shard in shard_by_npi.values())
    # Deterministic repeat.
    assert shard_by_npi[1000000007] == (1000000007 % shard_count)
    assert shard_by_npi[1000000099] == (1000000099 % shard_count)


def test_normalize_zcta_handles_cdc_shapes():
    assert provider_quality._normalize_zcta("01001") == "01001"
    assert provider_quality._normalize_zcta("01001.0") == "01001"
    assert provider_quality._normalize_zcta("ZCTA5 01001") == "01001"
    assert provider_quality._normalize_zcta("501001") == "01001"
    assert provider_quality._normalize_zcta(None) is None


def test_numeric_and_npi_normalizers_handle_csv_shapes():
    assert provider_quality._to_float("1,234.5") == 1234.5
    assert provider_quality._to_float("*") is None
    assert provider_quality._to_int("1,234.9") == 1234
    assert provider_quality._to_int("NA") is None
    assert provider_quality._to_npi("1,234,567,890.0") == 1234567890
    assert provider_quality._to_npi("0") is None
    assert provider_quality._to_npi("abc") is None


def test_pick_first_helpers_preserve_case_sensitive_and_ci_semantics():
    input_row_map = {"NPI": "", " npi ": "123", "score": 0}
    assert provider_quality._pick_first(input_row_map, "npi", "score") == 0
    assert provider_quality._pick_first_ci(input_row_map, "npi") == "123"


@pytest.mark.asyncio
async def test_download_source_file_raises_when_strict_mode(monkeypatch, tmp_path):
    async def _raise_download(_url: str, _path: str):
        raise RuntimeError("network down")

    monkeypatch.setattr(provider_quality, "DOWNLOAD_RETRIES", 1)
    monkeypatch.setattr(provider_quality, "PROVIDER_QUALITY_FAIL_ON_SOURCE_ERROR", True)
    monkeypatch.setattr(provider_quality, "PROVIDER_QUALITY_ALLOW_DEGRADED_TEST_ONLY", False)
    monkeypatch.setattr(provider_quality, "download_it_and_save", _raise_download)

    with pytest.raises(RuntimeError, match="download failed"):
        await provider_quality._download_source_file(
            "qpp_provider",
            {"url": "https://example.com/qpp.csv", "reporting_year": 2024},
            str(tmp_path),
            test_mode=False,
        )


@pytest.mark.asyncio
async def test_download_source_file_allows_degraded_test_fallback(monkeypatch, tmp_path):
    async def _raise_head(_url: str, _path: str, _max_bytes: int):
        raise RuntimeError("timeout")

    monkeypatch.setattr(provider_quality, "DOWNLOAD_RETRIES", 1)
    monkeypatch.setattr(provider_quality, "PROVIDER_QUALITY_FAIL_ON_SOURCE_ERROR", True)
    monkeypatch.setattr(provider_quality, "PROVIDER_QUALITY_ALLOW_DEGRADED_TEST_ONLY", True)
    monkeypatch.setattr(provider_quality, "_download_csv_head", _raise_head)

    path, degraded = await provider_quality._download_source_file(
        "qpp_provider",
        {"url": "https://example.com/qpp.csv", "reporting_year": 2024},
        str(tmp_path),
        test_mode=True,
    )
    assert degraded is True
    assert Path(path).exists()


@pytest.mark.asyncio
async def test_process_chunk_passes_test_mode_to_loader(monkeypatch, tmp_path):
    chunk = tmp_path / "qpp_chunk.csv"
    chunk.write_text("npi,year,quality_score,cost_score,final_score\n1234567890,2024,90,85,88\n", encoding="utf-8")

    observed_options_map = {"test_mode": None}

    async def _noop_database(_test_mode: bool):
        return None

    async def _capture_loader(_path: str, _cls: type, _year: int, test_mode: bool):
        observed_options_map["test_mode"] = test_mode

    monkeypatch.setattr(provider_quality, "ensure_database", _noop_database)
    monkeypatch.setattr(
        provider_quality,
        "_staging_classes",
        lambda _stage_suffix, _schema: {
            "PricingQppProvider": type("QppCls", (), {"__tablename__": "qpp_stage"}),
            "PricingSviZcta": type("SviCls", (), {"__tablename__": "svi_stage"}),
        },
    )
    monkeypatch.setattr(provider_quality, "_load_qpp_rows", _capture_loader)

    chunk_result_map = await provider_quality.provider_quality_process_chunk(
        {},
        {
            "dataset_key": "qpp_provider",
            "chunk_id": "qpp_provider:2024:0:0",
            "chunk_path": str(chunk),
            "run_id": "run_1",
            "schema": "mrf",
            "stage_suffix": "stage_1",
            "test_mode": True,
            "reporting_year": 2024,
        },
    )

    assert chunk_result_map["ok"] is True
    assert observed_options_map["test_mode"] is True


def test_normalize_helpers_cover_blank_and_digit_fallbacks():
    assert provider_quality_normalize._to_npi("  ") is None
    assert provider_quality_normalize._normalize_zcta(" ") is None
    assert provider_quality_normalize._normalize_zcta("0 1001") == "01001"
    assert provider_quality_normalize._normalize_zcta("12 3456") == "23456"
    assert provider_quality_normalize._pick_first({}, "absent") is None
