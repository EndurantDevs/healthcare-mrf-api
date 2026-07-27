from __future__ import annotations

import importlib
import json
from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from click.testing import CliRunner
from sqlalchemy import Date, DateTime

florida = importlib.import_module("process.florida_mqa_profile")


def test_copy_serialization_and_dates_preserve_exact_types():
    assert florida._copy_json_default(date(2026, 7, 27)) == "2026-07-27"
    assert florida._copy_json_default(object()).startswith("<object object")
    aware = datetime(2026, 7, 27, 12, tzinfo=UTC)
    assert florida._copy_value_for_type(DateTime(), aware).tzinfo is None
    plain_date = date(2026, 7, 27)
    assert florida._copy_value_for_type(Date(), plain_date) is plain_date
    assert florida._copy_value_for_type(
        DateTime(timezone=True),
        aware,
    ).tzinfo is UTC


def test_identity_matcher_rejects_ambiguous_professions_and_taxonomies():
    assert florida._profession_details(
        "Physician",
        {"physician": {("1501", "01"), ("1502", "02")}},
    ) == florida._profession_details("Physician", None)
    npi, status, evidence = florida._match_master(
        {
            "pro_cde": "1501",
            "lic_nbr": "ME1",
            "first_name": "Alex",
            "last_name": "Example",
        },
        {
            "ME1": [
                {
                    "npi": 1000000004,
                    "taxonomy": "999999999X",
                    "first_name": "Alex",
                    "last_name": "Example",
                    "license_number": "ME1",
                }
            ]
        },
    )
    assert (npi, status) == (None, "unmatched")
    assert evidence["candidate_count"] == 0


def test_row_iterators_skip_fully_empty_physical_rows(tmp_path):
    license_status = florida.FLORIDA_SOURCES["license_status"]
    headerless = tmp_path / "empty-license-status.txt"
    headerless.write_text(
        "|" * (len(license_status.expected_fields) - 1) + "\n",
        encoding="latin-1",
    )
    assert list(florida._iter_rows(headerless, license_status)) == []

    cannabis = florida.FLORIDA_SOURCES["medical_cannabis_authorization"]
    cannabis_path = tmp_path / "empty-cannabis.txt"
    cannabis_path.write_text(
        "|".join(cannabis.expected_fields)
        + "\n"
        + "|" * (len(cannabis.expected_fields) - 1)
        + "\n",
        encoding="latin-1",
    )
    assert list(florida._iter_rows(cannabis_path, cannabis)) == []

    generic = tmp_path / "empty-generic.txt"
    generic.write_text("first|last\n|\n", encoding="latin-1")
    assert list(florida._iter_rows(generic)) == []


def test_license_status_continuation_rejects_wrong_physical_width():
    physical_rows = [
        (1, ["value"] * 11),
        (2, ["value"] * 5),
        (3, [""]),
    ]
    assert florida._license_status_continuation_values(
        list(florida._LICENSE_STATUS_FIELDS),
        physical_rows,
        artifact_member="license_status.txt",
        parser_metrics={},
    ) is None


def test_license_status_truncated_continuation_is_quarantined(tmp_path):
    source_path = tmp_path / "truncated-license-status.txt"
    physical_values = ["x"] * 10 + ["x" * 105]
    assert len("|".join(physical_values)) == 125
    source_path.write_text(
        "|".join(physical_values) + "\n",
        encoding="latin-1",
    )

    rows = list(
        florida._iter_rows(
            source_path,
            florida.FLORIDA_SOURCES["license_status"],
        )
    )

    assert len(rows) == 1
    assert rows[0][2]["_source_parse_quarantine"] == "field_count_mismatch"


@pytest.mark.asyncio
async def test_values_upsert_chunks_rows_and_excludes_conflict_key(monkeypatch):
    statements = []

    class Excluded:
        def __getattr__(self, name):
            return f"excluded:{name}"

    class Statement:
        excluded = Excluded()

        def values(self, rows):
            self.rows = rows
            return self

        def on_conflict_do_update(self, **kwargs):
            self.conflict = kwargs
            return self

        async def status(self):
            statements.append(self)

    monkeypatch.setattr(
        florida.db,
        "insert",
        lambda _table: Statement(),
    )
    source_rows = [
        {"artifact_id": str(index), "run_id": "run"}
        for index in range(1_001)
    ]

    await florida._upsert_rows_values(
        florida.ProviderProfileArtifact,
        source_rows,
        "artifact_id",
    )

    assert [len(statement.rows) for statement in statements] == [1_000, 1]
    assert "artifact_id" not in statements[0].conflict["set_"]


@pytest.mark.asyncio
async def test_copy_fallback_switches_remaining_batches_to_values(monkeypatch):
    monkeypatch.setattr(florida, "_is_copy_upsert_enabled", lambda: True)
    monkeypatch.setattr(florida, "_copy_upsert_min_rows", lambda: 1)
    monkeypatch.setattr(florida, "_copy_upsert_batch_rows", lambda: 2)
    copy = AsyncMock(
        side_effect=florida._CopyUpsertUnavailable("driver unavailable")
    )
    values = AsyncMock()
    monkeypatch.setattr(florida, "_copy_upsert_chunk", copy)
    monkeypatch.setattr(florida, "_upsert_rows_values", values)

    await florida._upsert_rows(
        florida.ProviderProfileSourceRecord,
        [{"record_id": str(index)} for index in range(3)],
        "record_id",
    )

    assert copy.await_count == 1
    assert values.await_count == 2


@pytest.mark.asyncio
async def test_failure_status_helpers_are_bounded_and_best_effort(
    monkeypatch,
):
    monkeypatch.setenv("HLTHPRT_FL_MQA_FAILURE_STATUS_TIMEOUT_SECONDS", "bad")
    monkeypatch.setenv("HLTHPRT_FL_MQA_FAILURE_STATUS_WINDOW_SECONDS", "bad")
    assert florida._failure_status_timeout_seconds() == (
        florida.DEFAULT_FAILURE_STATUS_TIMEOUT_SECONDS
    )
    assert florida._failure_status_window_seconds() == (
        florida.DEFAULT_FAILURE_STATUS_WINDOW_SECONDS
    )

    monkeypatch.setattr(florida.db, "engine", object())
    await florida._dispose_failed_database_pool(1)

    engine = SimpleNamespace(dispose=AsyncMock(side_effect=RuntimeError("bad pool")))
    monkeypatch.setattr(florida.db, "engine", engine)
    await florida._dispose_failed_database_pool(1)
    engine.dispose.assert_awaited_once()

    monkeypatch.setattr(florida, "_failure_status_attempts", lambda: 1)
    monkeypatch.setattr(florida, "_failure_status_window_seconds", lambda: -1)
    result = await florida._mark_failed_run_status(
        run_id="a" * 32,
        run_row={"metrics": {}},
        original_error=RuntimeError("import failed"),
        cleanup_error=None,
    )
    assert result == "unknown failure while recording failed import status"


def test_source_guard_helpers_report_malformed_metrics_and_header_drift():
    header = "a" * 64
    reasons = florida._source_validation_guard_reasons(
        {
            "invalid": "not-a-map",
            "negative": {
                "schema_complete": True,
                "rows": 1,
                "quarantined_rows": -1,
                "header_sha256": header,
            },
            "limits": {
                "schema_complete": True,
                "rows": 10,
                "quarantined_rows": 2,
                "max_quarantined_rows": -1,
                "max_quarantined_ratio": True,
                "header_sha256": "bad",
            },
        },
        expected_source_keys=["invalid", "negative", "limits", "missing"],
    )
    assert "source_metrics_missing:missing" in reasons
    assert "source_metrics_invalid:invalid" in reasons
    assert "source_quarantine_metric_invalid:negative" in reasons
    assert "source_quarantine_count_limit_invalid:limits" in reasons
    assert "source_quarantine_ratio_limit_invalid:limits" in reasons
    assert "source_header_hash_missing:limits" in reasons

    drift = florida._source_header_drift_guard_reasons(
        {
            "missing": {"header_sha256": header},
            "invalid": {"header_sha256": header},
            "changed": {"header_sha256": "b" * 64},
        },
        {
            "missing": {"header_sha256": ""},
            "invalid": {"header_sha256": "bad"},
            "changed": {"header_sha256": header},
        },
    )
    assert "previous_source_header_hash_invalid:invalid" in drift
    assert any(
        reason.startswith("source_header_sha256_changed:changed")
        for reason in drift
    )


@pytest.mark.parametrize(
    "overrides",
    [
        {"rows": 0},
        {"quarantined_rows": -1},
        {"max_quarantined_rows": -1},
        {"max_quarantined_ratio": True},
        {"max_quarantined_ratio": 2},
        {"quarantined_rows": 2, "max_quarantined_rows": 1},
        {"quarantined_rows": 2, "rows": 10, "max_quarantined_ratio": 0.1},
    ],
)
def test_quarantine_threshold_rejects_each_unsafe_dimension(overrides):
    metric_by_key = {
        "rows": 100,
        "quarantined_rows": 0,
        "max_quarantined_rows": 1,
        "max_quarantined_ratio": 0.1,
        **overrides,
    }
    assert florida._is_source_quarantine_within_threshold(metric_by_key) is False


def test_artifact_cleanup_rejects_broad_roots_and_unsafe_entries(tmp_path):
    with pytest.raises(RuntimeError, match="artifact_root_too_broad"):
        florida._remove_artifact_run_directories(Path("/"), [])

    valid_run = "a" * 32
    symlink_run = "b" * 32
    file_run = "c" * 32
    (tmp_path / "symlink-target").mkdir()
    (tmp_path / symlink_run).symlink_to(tmp_path / "symlink-target")
    (tmp_path / file_run).write_text("not a directory", encoding="utf-8")

    result = florida._remove_artifact_run_directories(
        tmp_path,
        ["invalid", valid_run, symlink_run, file_run],
    )

    assert result["missing"] == [valid_run]
    assert result["errors"]["invalid"] == "invalid_run_id"
    assert "symlink_not_allowed" in result["errors"][symlink_run]
    assert "artifact_path_not_directory" in result["errors"][file_run]


def test_artifact_cleanup_rechecks_resolved_path_after_symlink_race(
    monkeypatch,
    tmp_path,
):
    run_id = "d" * 32
    outside_root = tmp_path.parent / f"{tmp_path.name}-outside"
    outside_root.mkdir()
    candidate = tmp_path / run_id
    candidate.symlink_to(outside_root)
    original_is_symlink = Path.is_symlink

    monkeypatch.setattr(
        Path,
        "is_symlink",
        lambda path: (
            False if path == candidate else original_is_symlink(path)
        ),
    )

    result = florida._remove_artifact_run_directories(tmp_path, [run_id])

    assert "artifact_path_outside_root" in result["errors"][run_id]
    assert outside_root.is_dir()


@pytest.mark.asyncio
async def test_post_success_retention_preserves_success_when_metric_write_fails(
    monkeypatch,
    tmp_path,
):
    monkeypatch.setattr(
        florida,
        "_post_success_retention",
        AsyncMock(return_value={"status": "completed"}),
    )

    class Update:
        def where(self, *_criteria):
            return self

        def values(self, **_values):
            return self

        async def status(self):
            raise RuntimeError("metrics unavailable")

    monkeypatch.setattr(florida.db, "update", lambda _table: Update())

    metrics = await florida._apply_post_success_retention(
        run_id="a" * 32,
        metrics={"published_providers": 10},
        artifact_root=tmp_path,
        failed_retention_days=7,
    )

    assert metrics["published_providers"] == 10
    assert metrics["retention"]["metrics_persist_error"] == {
        "type": "RuntimeError",
        "message": "metrics unavailable",
    }


def test_projection_deduplicates_assertions_without_merging_unrelated_values():
    profile_source_by_key = {
        "source_key": florida.FL_MQA_SOURCE_KEY,
        "dataset": "education",
    }

    def fact(record_id, fact_type):
        return {
            "logical_fact_key": "same-fact",
            "category": "education",
            "fact_type": fact_type,
            "display": "Readable education",
            "value_json": {"institution": "Example University"},
            "assertion_type": "self_reported",
            "verification_status": "source_reported",
            "effective_start": None,
            "effective_end": None,
            "sensitive": False,
            "public_default": True,
            "source_record_id": record_id,
            "source_json": profile_source_by_key,
        }

    profile, evidence = florida._projection(
        1000000004,
        "generation",
        [
            fact("record-a", "education"),
            fact("record-a", "education"),
            fact("record-b", "education"),
        ],
        {"education"},
    )

    profile_item = profile["categories"]["education"]["items"][0]
    assert profile_item["source_record_ids"] == ["record-a", "record-b"]
    assert profile_item["assertion_count"] == 2
    assert evidence["records"] == [profile_source_by_key]


def test_source_ratio_guard_ignores_uncomparable_metrics():
    assert florida._source_ratio_guard_reasons(
        {
            "invalid": "not-a-map",
            "new": {"rows": 1, "matched": None, "facts": 1},
        },
        {
            "invalid": {"rows": 10},
            "new": {"rows": 0, "matched": 5, "facts": "unknown"},
        },
        min_publish_ratio=0.8,
    ) == []


@pytest.mark.asyncio
async def test_retention_ignores_catalog_rows_outside_live_and_rollback(
    monkeypatch,
    tmp_path,
):
    class Row:
        def __init__(self, **mapping):
            self._mapping = mapping

    class Transaction:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

    all_results = iter(
        (
            [Row(tablename="unexpected_projection")],
            [],
        )
    )
    monkeypatch.setattr(florida.db, "scalar", AsyncMock(return_value=1))
    monkeypatch.setattr(
        florida.db,
        "all",
        AsyncMock(side_effect=lambda *_args, **_kwargs: next(all_results)),
    )
    monkeypatch.setattr(florida.db, "transaction", lambda: Transaction())

    operation_result = await florida._post_success_retention(
        run_id="a" * 32,
        artifact_root=tmp_path,
        failed_retention_days=7,
    )

    assert operation_result["status"] == "completed"
    assert operation_result["protected_audit_run_ids"] == []
    assert operation_result["deleted_run_ids"] == []


def test_direct_import_command_forwards_all_safety_options(monkeypatch):
    importer = AsyncMock(
        return_value={
            "run_id": "a" * 32,
            "publication": {"publication": "skipped_partial"},
        }
    )
    monkeypatch.setattr(florida, "import_florida_mqa_profile", importer)

    operation_result = CliRunner().invoke(
        florida.florida_mqa_profile,
        [
            "--sources",
            "profile_master, education",
            "--max-providers",
            "2",
            "--only-matched",
            "--publish-partial",
            "--allow-volume-drop",
        ],
    )

    assert operation_result.exit_code == 0
    profile_payload = json.loads(operation_result.output)
    assert profile_payload["run_id"] == "a" * 32
    importer.assert_awaited_once_with(
        source_keys=["profile_master", "education"],
        max_providers=2,
        only_matched=True,
        publish_partial=True,
        allow_volume_drop=True,
    )
