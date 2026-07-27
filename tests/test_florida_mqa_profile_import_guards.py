from __future__ import annotations

import importlib
from unittest.mock import AsyncMock

import pytest

florida = importlib.import_module("process.florida_mqa_profile")


class _ArtifactClient:
    def __init__(self, *_args):
        self.base_url = "https://example.invalid"

    def authenticate(self):
        return None

    def download(self, _source, target):
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("fixture", encoding="utf-8")
        return "c" * 64, 7


def _profile_row(source, *, license_id="42", license_number="ME12345"):
    row_by_key = {field: "" for field in source.expected_fields}
    row_by_key.update(
        {
            "pro_cde": "1501",
            "profession_code": "1501",
            "lic_id": license_id,
            "license_id": license_id,
            "lic_nbr": license_number,
            "license_number": license_number,
            "rank_cde": "01",
            "rank_code": "01",
            "rank_desc": "Physician",
            "f_name": "Alex",
            "l_name": "Example",
            "lic_sta_desc": "CLEAR/ACTIVE",
        }
    )
    return row_by_key


def _configure_import_runtime(
    monkeypatch,
    *,
    upserts=None,
    scalar_result=0,
):
    monkeypatch.setenv("HLTHPRT_FL_MQA_USERNAME", "test-user")
    monkeypatch.setenv("HLTHPRT_FL_MQA_PASSWORD", "test-password")
    monkeypatch.setattr(florida, "FloridaMQAClient", _ArtifactClient)
    monkeypatch.setattr(florida, "_ensure_tables", AsyncMock())
    retention = AsyncMock(return_value={"status": "completed"})
    monkeypatch.setattr(florida, "_apply_retention_maintenance", retention)
    monkeypatch.setattr(florida, "_claim_import_run", AsyncMock())
    monkeypatch.setattr(
        florida,
        "_load_florida_license_index",
        AsyncMock(return_value={}),
    )
    monkeypatch.setattr(
        florida,
        "_apply_post_success_retention",
        AsyncMock(side_effect=lambda **kwargs: kwargs["metrics"]),
    )
    monkeypatch.setattr(florida, "_mark_failed_run_status", AsyncMock())
    monkeypatch.setattr(florida, "enqueue_live_progress", lambda **_kwargs: None)
    monkeypatch.setattr(florida.db, "scalar", AsyncMock(return_value=scalar_result))
    monkeypatch.setattr(florida.db, "status", AsyncMock())

    captured = [] if upserts is None else upserts

    async def capture_upsert(model, rows, conflict_column):
        captured.append((model, list(rows), conflict_column))

    monkeypatch.setattr(florida, "_upsert_rows", capture_upsert)
    return captured, retention


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("environment", "message"),
    [
        (
            {"HLTHPRT_FL_MQA_MIN_FIRST_PUBLISH_PROVIDERS": "0"},
            "must be at least 1",
        ),
        (
            {"HLTHPRT_FL_MQA_MIN_PUBLISH_RATIO": "0"},
            "must be in \\(0, 1\\]",
        ),
        (
            {"HLTHPRT_FL_MQA_FAILED_RUN_RETENTION_DAYS": "-1"},
            "must be non-negative",
        ),
        (
            {"HLTHPRT_FL_MQA_MAX_QUARANTINED_ROWS_PER_SOURCE": "-1"},
            "must be non-negative",
        ),
        (
            {"HLTHPRT_FL_MQA_MAX_QUARANTINED_ROW_RATIO": "2"},
            "must be in \\[0, 1\\]",
        ),
    ],
)
async def test_import_rejects_unsafe_publication_and_retention_settings(
    monkeypatch,
    environment,
    message,
):
    monkeypatch.setenv("HLTHPRT_FL_MQA_USERNAME", "test-user")
    monkeypatch.setenv("HLTHPRT_FL_MQA_PASSWORD", "test-password")
    for key, value in environment.items():
        monkeypatch.setenv(key, value)

    with pytest.raises(ValueError, match=message):
        await florida.import_florida_mqa_profile(
            source_keys=["profile_master"],
            manage_db=False,
        )


@pytest.mark.asyncio
async def test_import_requires_credentials_and_rejects_unknown_sources(monkeypatch):
    monkeypatch.setattr(florida, "load_dotenv", lambda *_args, **_kwargs: None)
    monkeypatch.delenv("HLTHPRT_FL_MQA_USERNAME", raising=False)
    monkeypatch.delenv("HLTHPRT_FL_MQA_EMAIL", raising=False)
    monkeypatch.delenv("HLTHPRT_FL_MQA_PASSWORD", raising=False)

    with pytest.raises(RuntimeError, match="USERNAME and .*PASSWORD are required"):
        await florida.import_florida_mqa_profile(
            source_keys=["profile_master"],
            manage_db=False,
        )

    monkeypatch.setenv("HLTHPRT_FL_MQA_USERNAME", "test-user")
    monkeypatch.setenv("HLTHPRT_FL_MQA_PASSWORD", "test-password")
    with pytest.raises(ValueError, match="unknown Florida MQA sources: unknown"):
        await florida.import_florida_mqa_profile(
            source_keys=["unknown"],
            manage_db=False,
        )


@pytest.mark.asyncio
async def test_managed_database_disconnects_when_bootstrap_or_claim_fails(
    monkeypatch,
    tmp_path,
):
    """Verify managed database disconnects when bootstrap or claim fails."""
    monkeypatch.setenv("HLTHPRT_FL_MQA_USERNAME", "test-user")
    monkeypatch.setenv("HLTHPRT_FL_MQA_PASSWORD", "test-password")
    connect = AsyncMock()
    disconnect = AsyncMock()
    monkeypatch.setattr(florida.db, "connect", connect)
    monkeypatch.setattr(florida.db, "disconnect", disconnect)
    monkeypatch.setattr(
        florida,
        "_ensure_tables",
        AsyncMock(side_effect=RuntimeError("schema bootstrap unavailable")),
    )

    with pytest.raises(RuntimeError, match="schema bootstrap unavailable"):
        await florida.import_florida_mqa_profile(
            source_keys=["profile_master"],
            artifact_root=tmp_path,
        )
    connect.assert_awaited_once()
    disconnect.assert_awaited_once()

    disconnect.reset_mock()
    with pytest.raises(RuntimeError, match="schema bootstrap unavailable"):
        await florida.import_florida_mqa_profile(
            source_keys=["profile_master"],
            artifact_root=tmp_path,
            manage_db=False,
        )
    disconnect.assert_not_awaited()

    disconnect.reset_mock()
    monkeypatch.setattr(florida, "_ensure_tables", AsyncMock())
    monkeypatch.setattr(
        florida,
        "_apply_retention_maintenance",
        AsyncMock(return_value={"status": "completed"}),
    )
    monkeypatch.setattr(
        florida,
        "_claim_import_run",
        AsyncMock(side_effect=RuntimeError("run already claimed")),
    )
    with pytest.raises(RuntimeError, match="run already claimed"):
        await florida.import_florida_mqa_profile(
            source_keys=["profile_master"],
            artifact_root=tmp_path,
        )
    disconnect.assert_awaited_once()
    disconnect.reset_mock()
    with pytest.raises(RuntimeError, match="run already claimed"):
        await florida.import_florida_mqa_profile(
            source_keys=["profile_master"],
            artifact_root=tmp_path,
            manage_db=False,
        )
    disconnect.assert_not_awaited()


@pytest.mark.asyncio
async def test_missing_required_header_is_persisted_before_import_fails(
    monkeypatch,
    tmp_path,
):
    captured, retention = _configure_import_runtime(monkeypatch)
    monkeypatch.setattr(
        florida,
        "_artifact_header",
        lambda _path, _source: ["pro_cde"],
    )

    with pytest.raises(
        RuntimeError,
        match="florida_mqa_schema_changed:profile_master:",
    ):
        await florida.import_florida_mqa_profile(
            source_keys=["profile_master"],
            artifact_root=tmp_path,
            manage_db=False,
        )

    artifacts = [
        source_rows
        for model, source_rows, _key in captured
        if model is florida.ProviderProfileArtifact
    ]
    runs = [
        source_rows
        for model, source_rows, _key in captured
        if model is florida.ProviderProfileImportRun
    ]
    assert artifacts[-1][0]["header"] == ["pro_cde"]
    source_metrics = runs[-1][0]["metrics"]["source_metrics"]["profile_master"]
    assert source_metrics["schema_complete"] is False
    assert "lic_id" in source_metrics["missing_required_fields"]
    assert retention.await_count == 2


@pytest.mark.asyncio
async def test_inconsistent_member_header_is_persisted_before_import_fails(
    monkeypatch,
    tmp_path,
):
    captured, _retention = _configure_import_runtime(monkeypatch)
    profile_source = florida.FLORIDA_SOURCES["profile_master"]
    source_row = _profile_row(profile_source)
    monkeypatch.setattr(
        florida,
        "_artifact_header",
        lambda _path, current: list(current.expected_fields),
    )

    def inconsistent_rows(_path, _source, *, parser_metrics=None):
        del parser_metrics
        yield 1, dict(source_row), source_row, ["unexpected_header"]

    monkeypatch.setattr(florida, "_iter_rows", inconsistent_rows)

    with pytest.raises(
        RuntimeError,
        match="florida_mqa_schema_changed:profile_master:inconsistent_header",
    ):
        await florida.import_florida_mqa_profile(
            source_keys=["profile_master"],
            artifact_root=tmp_path,
            manage_db=False,
        )

    runs = [
        source_rows
        for model, source_rows, _key in captured
        if model is florida.ProviderProfileImportRun
    ]
    source_metrics = runs[-1][0]["metrics"]["source_metrics"]["profile_master"]
    assert source_metrics["schema_complete"] is False
    assert source_metrics["missing_required_fields"] == ["inconsistent_header"]


@pytest.mark.asyncio
async def test_quarantined_rows_retain_evidence_and_flush_in_bounded_batches(
    monkeypatch,
    tmp_path,
):
    captured, _retention = _configure_import_runtime(monkeypatch)
    monkeypatch.setattr(
        florida,
        "_artifact_header",
        lambda _path, source: list(source.expected_fields),
    )

    def quarantined_rows(_path, source, *, parser_metrics=None):
        parser_metrics["quarantined_rows"] += 1_000
        for row_number in range(1, 1_001):
            raw_by_key = {
                "_source_parse_metadata": {
                    "kind": "field_count_mismatch",
                    "row_number": row_number,
                }
            }
            normalized_by_key = {
                "_source_parse_quarantine": True,
                "pro_cde": "",
                "lic_id": "",
                "dataset": source.key,
            }
            yield row_number, raw_by_key, normalized_by_key, list(source.expected_fields)

    monkeypatch.setattr(florida, "_iter_rows", quarantined_rows)

    operation_result = await florida.import_florida_mqa_profile(
        source_keys=["profile_master"],
        artifact_root=tmp_path,
        manage_db=False,
    )

    retained_batches = [
        source_rows
        for model, source_rows, _key in captured
        if model is florida.ProviderProfileSourceRecord and source_rows
    ]
    assert [len(source_rows) for source_rows in retained_batches] == [1_000]
    retained = retained_batches[0][0]
    assert retained["match_status"] == "quarantined_schema_anomaly"
    assert retained["match_evidence"]["method"] == "source_row_quarantine"
    assert operation_result["source_records"] == 1_000
    assert operation_result["non_projectable_records"] == 1_000
    source_metric = operation_result["source_metrics"]["profile_master"]
    assert source_metric["quarantine_ratio"] == 1.0
    assert source_metric["quarantine_within_threshold"] is False
    assert source_metric["validated"] is False


@pytest.mark.asyncio
async def test_publish_partial_still_requires_validated_source_rows(
    monkeypatch,
    tmp_path,
):
    _captured, _retention = _configure_import_runtime(monkeypatch)
    monkeypatch.setattr(
        florida,
        "_artifact_header",
        lambda _path, source: list(source.expected_fields),
    )
    monkeypatch.setattr(
        florida,
        "_iter_rows",
        lambda _path, _source, *, parser_metrics=None: iter(()),
    )

    with pytest.raises(
        RuntimeError,
        match="provider_profile_source_validation_guard:source_rows_empty:",
    ):
        await florida.import_florida_mqa_profile(
            source_keys=["profile_master"],
            publish_partial=True,
            artifact_root=tmp_path,
            manage_db=False,
        )


@pytest.mark.asyncio
async def test_only_matched_and_max_provider_limits_bound_retained_master_rows(
    monkeypatch,
    tmp_path,
):
    captured, _retention = _configure_import_runtime(
        monkeypatch,
        scalar_result=1,
    )
    profile_source = florida.FLORIDA_SOURCES["profile_master"]
    first = _profile_row(profile_source, license_id="1", license_number="ME1")
    second = _profile_row(profile_source, license_id="2", license_number="ME2")
    third = _profile_row(profile_source, license_id="3", license_number="ME3")
    monkeypatch.setattr(
        florida,
        "_artifact_header",
        lambda _path, current: list(current.expected_fields),
    )

    def two_rows(_path, current, *, parser_metrics=None):
        del parser_metrics
        header_items = list(current.expected_fields)
        yield 1, dict(first), first, header_items
        yield 2, dict(second), second, header_items
        yield 3, dict(third), third, header_items

    monkeypatch.setattr(florida, "_iter_rows", two_rows)
    matches = iter(
        (
            (None, "unmatched", {"method": "fixture"}),
            (1000000004, "deterministic", {"method": "fixture"}),
            (1000000005, "deterministic", {"method": "fixture"}),
        )
    )
    monkeypatch.setattr(florida, "_match_master", lambda *_args: next(matches))

    operation_result = await florida.import_florida_mqa_profile(
        source_keys=["profile_master"],
        max_providers=1,
        only_matched=True,
        artifact_root=tmp_path,
        manage_db=False,
    )

    retained_items = [
        source_row
        for model, source_rows, _key in captured
        if model is florida.ProviderProfileSourceRecord
        for source_row in source_rows
    ]
    assert len(retained_items) == 1
    assert retained_items[0]["matched_npi"] == 1000000004
    assert operation_result["source_records"] == 1


@pytest.mark.asyncio
async def test_failure_status_persistence_error_is_reported_without_masking_source_error(
    monkeypatch,
    tmp_path,
):
    events = []

    class FailingClient(_ArtifactClient):
        def authenticate(self):
            raise RuntimeError("portal unavailable")

    _configure_import_runtime(monkeypatch)
    monkeypatch.setattr(florida, "FloridaMQAClient", FailingClient)
    monkeypatch.setattr(
        florida,
        "_mark_failed_run_status",
        AsyncMock(return_value="database status write failed"),
    )
    monkeypatch.setattr(
        florida,
        "enqueue_live_progress",
        lambda **payload: events.append(payload),
    )

    with pytest.raises(RuntimeError, match="portal unavailable"):
        await florida.import_florida_mqa_profile(
            source_keys=["profile_master"],
            artifact_root=tmp_path,
            manage_db=False,
        )

    failed = events[-1]
    assert failed["phase"] == "failed"
    assert failed["error"]["status_persistence_error"] == (
        "database status write failed"
    )
