import importlib
from pathlib import Path

import pytest

provider_quality = importlib.import_module("process.provider_quality")


class _FakeRedis:
    def __init__(self) -> None:
        self.store: dict[str, str] = {}
        self.set_store: dict[str, set[str]] = {}
        self.list_store: dict[str, list[str]] = {}
        self.jobs: list[dict[str, object]] = []

    async def set(self, key: str, value, ex=None, nx: bool = False):  # noqa: ANN001, ARG002
        if nx and key in self.store:
            return False
        self.store[key] = str(value)
        return True

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

    async def expire(self, key: str, ttl: int):  # noqa: ARG002
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
        _max_tries: int | None = None,  # noqa: ARG002
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
        return True

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


def test_archived_identifier_truncates_long_name():
    long_name = "x" * 100
    archived = provider_quality._archived_identifier(long_name)
    assert len(archived) <= 63
    assert archived.endswith("_old")


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
    assignments = {npi: (npi % shard_count) for npi in range(1000000000, 1000000100)}
    assert all(0 <= shard < shard_count for shard in assignments.values())
    # Deterministic repeat.
    assert assignments[1000000007] == (1000000007 % shard_count)
    assert assignments[1000000099] == (1000000099 % shard_count)


def test_normalize_zcta_handles_cdc_shapes():
    assert provider_quality._normalize_zcta("01001") == "01001"
    assert provider_quality._normalize_zcta("01001.0") == "01001"
    assert provider_quality._normalize_zcta("ZCTA5 01001") == "01001"
    assert provider_quality._normalize_zcta("501001") == "01001"
    assert provider_quality._normalize_zcta(None) is None


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

    observed = {"test_mode": None}

    async def _noop_database(_test_mode: bool):
        return None

    async def _capture_loader(_path: str, _cls: type, _year: int, test_mode: bool):
        observed["test_mode"] = test_mode

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

    result = await provider_quality.provider_quality_process_chunk(
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

    assert result["ok"] is True
    assert observed["test_mode"] is True


@pytest.mark.asyncio
async def test_load_qpp_rows_parses_real_cms_field_names(monkeypatch, tmp_path):
    csv_path = tmp_path / "qpp_real_headers.csv"
    csv_path.write_text(
        (
            "Clinician_NPI,Performance_Year,Quality_category_score,"
            "Cost_category_score,final_MIPS_score\n"
            "1234567890,2024,91.5,82.0,88.25\n"
        ),
        encoding="utf-8",
    )

    captured: list[dict[str, object]] = []

    async def _capture_push(rows, _cls, **_kwargs):
        captured.extend(rows)

    monkeypatch.setattr(provider_quality, "_push_objects_with_retry", _capture_push)

    fake_cls = type("QppStage", (), {"__tablename__": "pricing_qpp_provider_stage"})
    await provider_quality._load_qpp_rows(str(csv_path), fake_cls, 2024, test_mode=False)

    assert len(captured) == 1
    row = captured[0]
    assert row["npi"] == 1234567890
    assert row["year"] == 2024
    assert row["quality_score"] == 91.5
    assert row["cost_score"] == 82.0
    assert row["final_score"] == 88.25


@pytest.mark.asyncio
async def test_materialize_query_contains_state_benchmark_and_extra_measures(monkeypatch):
    statements: list[str] = []

    async def _fake_status(statement: str, *args, **kwargs):  # noqa: ARG001
        statements.append(statement)

    async def _fake_table_exists(_schema: str, _table: str) -> bool:
        return False

    monkeypatch.setattr(provider_quality, "_table_exists", _fake_table_exists)
    monkeypatch.setattr(provider_quality.db, "status", _fake_status)

    classes = {
        "PricingQppProvider": type("QppStage", (), {"__tablename__": "pricing_qpp_provider_stage"}),
        "PricingSviZcta": type("SviStage", (), {"__tablename__": "pricing_svi_zcta_stage"}),
        "PricingProviderQualityMeasure": type("MeasureStage", (), {"__tablename__": "pricing_provider_quality_measure_stage"}),
        "PricingProviderQualityDomain": type("DomainStage", (), {"__tablename__": "pricing_provider_quality_domain_stage"}),
        "PricingProviderQualityScore": type("ScoreStage", (), {"__tablename__": "pricing_provider_quality_score_stage"}),
    }

    await provider_quality._materialize_quality_rows(classes, "mrf", "run_test")

    materialize_sql = "\n".join(statements)
    assert "peers_state AS (" in materialize_sql
    assert "peers_zip_exact AS (" in materialize_sql
    assert "zip_choice AS (" in materialize_sql
    assert "benchmark_modes AS (" in materialize_sql
    assert "CROSS JOIN benchmark_modes bm" in materialize_sql
    assert "zip_ring:" in materialize_sql
    assert "|| ':' || 'r' ||" in materialize_sql
    assert "|| ':r' ||" not in materialize_sql
    assert "::numeric, 2)::float8 AS score_0_100" in materialize_sql
    assert "ROUND(LEAST(" not in materialize_sql
    assert "CAST(:run_id AS varchar) AS run_id" in materialize_sql
    assert ":run_id::varchar AS run_id" not in materialize_sql
    assert "appropriateness_drug_proxy" in materialize_sql
    assert "cost_qpp_component" in materialize_sql


@pytest.mark.asyncio
async def test_materialize_cohort_query_contains_lsh_and_fallback_rules(monkeypatch):
    statements: list[str] = []

    async def _fake_status(statement: str, *args, **kwargs):  # noqa: ARG001
        statements.append(statement)

    async def _fake_table_exists(_schema: str, _table: str) -> bool:
        return False

    monkeypatch.setattr(provider_quality.db, "status", _fake_status)
    monkeypatch.setattr(provider_quality, "_table_exists", _fake_table_exists)

    classes = provider_quality._staging_classes("stage_test", "mrf")
    await provider_quality._materialize_quality_rows_cohort(classes, "mrf", "run_test")

    materialize_sql = "\n".join(statements)
    assert "taxonomy_choice AS (" in materialize_sql
    assert "provider_enrichment_choice AS (" in materialize_sql
    assert "doctor_address_choice AS (" in materialize_sql
    assert "unified_address_choice AS (" in materialize_sql
    assert "npi_address_choice AS (" in materialize_sql
    assert "FROM mrf.doctor_clinician_address" not in materialize_sql
    assert "signatures AS (" in materialize_sql
    assert "bands AS (" in materialize_sql
    assert "cohort_expanded AS (" in materialize_sql
    assert "benchmark_modes AS (" in materialize_sql
    assert "VALUES ('L0'), ('L1'), ('L2'), ('L3')" in materialize_sql
    assert "bm.benchmark_mode" in materialize_sql
    assert "AND LOWER(COALESCE(t.benchmark_mode, 'national')) = bm.benchmark_mode" in materialize_sql
    assert "COALESCE(c.procedure_bucket, 'bucket:none')::varchar AS procedure_bucket" in materialize_sql
    assert "COALESCE(c.specialty, 'unknown')::varchar AS specialty" in materialize_sql
    assert "COALESCE(c.taxonomy, 'unknown')::varchar AS taxonomy" in materialize_sql
    assert "ON cm.npi = b.npi" in materialize_sql
    assert "ON cm.npi = c.npi" in materialize_sql
    assert "WHEN bm.benchmark_mode = 'zip'" in materialize_sql
    assert "({peer_scope_expr} = 'zip'" not in materialize_sql
    assert "CASE WHEN c.threshold_met THEN 0 ELSE 1 END" in materialize_sql


@pytest.mark.asyncio
async def test_shard_queries_delete_partition_before_insert(monkeypatch):
    async def _fake_table_exists(_schema: str, _table: str) -> bool:
        return False

    monkeypatch.setattr(provider_quality, "_table_exists", _fake_table_exists)
    classes = provider_quality._staging_classes("stage_test", "mrf")
    ctx = await provider_quality._build_cohort_materialization_context(classes, "mrf")

    lsh_sql = provider_quality._cohort_sql_phase_2_build_lsh_shard(ctx)
    measure_sql = provider_quality._cohort_sql_phase_5_build_measure_shard(ctx)
    domain_sql = provider_quality._cohort_sql_phase_6_build_domain_shard(ctx)
    score_sql = provider_quality._cohort_sql_phase_7_build_score_shard(ctx)

    assert "WITH deleted AS (" in lsh_sql
    assert f"DELETE FROM mrf.{ctx['lsh_table']} d" in lsh_sql
    assert "MOD((d.npi)::bigint, :shard_count) = :shard_id" in lsh_sql

    assert "WITH deleted AS (" in measure_sql
    assert f"DELETE FROM mrf.{ctx['measure_table']} d" in measure_sql
    assert "MOD((d.npi)::bigint, :shard_count) = :shard_id" in measure_sql

    assert "WITH deleted AS (" in domain_sql
    assert f"DELETE FROM mrf.{ctx['domain_table']} d" in domain_sql
    assert "MOD((d.npi)::bigint, :shard_count) = :shard_id" in domain_sql

    assert "WITH deleted AS (" in score_sql
    assert f"DELETE FROM mrf.{ctx['score_table']} d" in score_sql
    assert "MOD((d.npi)::bigint, :shard_count) = :shard_id" in score_sql


@pytest.mark.asyncio
async def test_enqueue_materialize_phase_shards_sets_progress_and_jobs(monkeypatch):
    redis = _FakeRedis()
    monkeypatch.setattr(provider_quality, "PROVIDER_QUALITY_MATERIALIZE_SHARD_QUEUE_NAME", "arq:test_finish")
    queued = await provider_quality._enqueue_materialize_phase_shards(
        redis,
        run_id="run_1",
        phase=provider_quality.MAT_PHASE_2_BUILD_LSH_SHARDED,
        years=(2023, 2024),
        shard_count=2,
        stage_suffix="stage_1",
        schema="mrf",
        test_mode=True,
        job_name="provider_quality_materialize_lsh_shard",
    )

    assert queued == 4
    assert redis.store[provider_quality._mat_phase_key("run_1")] == provider_quality.MAT_PHASE_2_BUILD_LSH_SHARDED
    assert redis.store[provider_quality._mat_total_key("run_1")] == "4"
    assert redis.store[provider_quality._mat_done_key("run_1")] == "0"
    assert redis.store[provider_quality._mat_failed_key("run_1")] == "0"
    assert len(redis.jobs) == 4
    assert {job["payload"]["year"] for job in redis.jobs} == {2023, 2024}
    assert {job["payload"]["shard_id"] for job in redis.jobs} == {0, 1}
    assert {job["queue_name"] for job in redis.jobs} == {"arq:test_finish"}
    assert {job["max_tries"] for job in redis.jobs} == {None}


@pytest.mark.asyncio
async def test_wait_for_materialize_phase_completion_retry_and_failure():
    redis = _FakeRedis()
    await provider_quality._set_materialize_phase(
        redis,
        "run_wait",
        provider_quality.MAT_PHASE_5_BUILD_MEASURE_SHARDED,
        total=3,
    )

    redis.store[provider_quality._mat_done_key("run_wait")] = "1"
    with pytest.raises(provider_quality.Retry):
        await provider_quality._wait_for_materialize_phase_completion(
            redis,
            "run_wait",
            provider_quality.MAT_PHASE_5_BUILD_MEASURE_SHARDED,
        )

    redis.store[provider_quality._mat_done_key("run_wait")] = "3"
    redis.store[provider_quality._mat_failed_key("run_wait")] = "1"
    with pytest.raises(RuntimeError, match="failed_shards=1"):
        await provider_quality._wait_for_materialize_phase_completion(
            redis,
            "run_wait",
            provider_quality.MAT_PHASE_5_BUILD_MEASURE_SHARDED,
        )


@pytest.mark.asyncio
async def test_wait_for_materialize_phase_completion_logs_duration_summary(monkeypatch):
    redis = _FakeRedis()
    await provider_quality._set_materialize_phase(
        redis,
        "run_done",
        provider_quality.MAT_PHASE_7_BUILD_SCORE_SHARDED,
        total=2,
    )
    redis.store[provider_quality._mat_done_key("run_done")] = "2"
    redis.store[provider_quality._mat_failed_key("run_done")] = "0"
    await redis.rpush(
        provider_quality._mat_phase_duration_key("run_done", provider_quality.MAT_PHASE_7_BUILD_SCORE_SHARDED),
        "10.0",
    )
    await redis.rpush(
        provider_quality._mat_phase_duration_key("run_done", provider_quality.MAT_PHASE_7_BUILD_SCORE_SHARDED),
        "30.0",
    )

    observed: list[tuple] = []

    def _capture_info(msg, *args, **kwargs):  # noqa: ANN001, ARG002
        observed.append((msg, args))

    monkeypatch.setattr(provider_quality.logger, "info", _capture_info)
    await provider_quality._wait_for_materialize_phase_completion(
        redis,
        "run_done",
        provider_quality.MAT_PHASE_7_BUILD_SCORE_SHARDED,
    )

    assert observed
    logged = " ".join(str(part) for part in observed[-1][1])
    assert "run_done" in logged
    assert provider_quality.MAT_PHASE_7_BUILD_SCORE_SHARDED in logged
