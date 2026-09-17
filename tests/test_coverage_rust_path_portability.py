"""Rust coverage scope and segments survive different producer/consumer roots."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from tests.test_coverage_forecast import growth, reports


def _rust_report(filenames: list[str]) -> dict:
    return {
        "data": [{"files": [{
            "filename": filename,
            "segments": [
                [1, 1, 1, True, True, False],
                [5, 1, 0, True, True, False],
                [6, 1, 0, False, False, False],
            ],
            "summary": {
                "lines": {"covered": 4, "count": 5},
                "functions": {"covered": 2, "count": 2},
                "regions": {"covered": 6, "count": 8},
                "branches": {"covered": 1, "count": 2},
            },
        } for filename in filenames]}],
    }


def _rust_config() -> dict:
    repository_root = Path(__file__).resolve().parents[1]
    baseline = json.loads(
        (repository_root / "test-coverage-baseline.json").read_text(encoding="utf-8")
    )
    config = baseline["reports"]["rust"]
    config["path"] = "rust.json"
    return config


def test_relative_rust_preserves_scope_metrics_and_segments(tmp_path: Path) -> None:
    producer_root = tmp_path / "container"
    consumer_root = tmp_path / "hosted"
    source_paths = [
        "support/ptg2_scanner/src/sample.rs",
        "support/ptg2_scanner/src/parser/sample.rs",
    ]
    exported_paths = [*source_paths, "support/ptg2_scanner/tests/integration.rs"]
    producer_report = _rust_report([str(producer_root / path) for path in exported_paths])
    consumer_report = _rust_report(exported_paths)
    config = _rust_config()
    for root, report in ((producer_root, producer_report), (consumer_root, consumer_report)):
        root.mkdir()
        (root / "rust.json").write_text(json.dumps(report), encoding="utf-8")

    snapshot = reports._llvm_snapshot(consumer_report, consumer_root, config)
    assert snapshot == reports._llvm_snapshot(producer_report, producer_root, config)
    assert snapshot.files == frozenset(source_paths)
    assert snapshot.metric_by_name == {
        "lines": {"covered": 8, "total": 10},
        "functions": {"covered": 4, "total": 4},
        "regions": {"covered": 12, "total": 16},
        "branches": {"covered": 2, "total": 4},
    }
    coverage_by_path = growth._coverage_files(consumer_root, config)
    assert coverage_by_path == growth._coverage_files(producer_root, config)
    assert coverage_by_path == {
        path: ({1, 2, 3, 4, 5}, {1, 2, 3, 4}) for path in source_paths
    }
    for root, report in ((producer_root, producer_report), (consumer_root, consumer_report)):
        assert (root / "rust.json").read_text(encoding="utf-8") == json.dumps(report)


@pytest.mark.parametrize("filename", [
    "src/sample.rs",
    "/foreign/support/ptg2_scanner/src/sample.rs",
])
def test_rust_rejects_foreign_or_unprefixed_paths(tmp_path: Path, filename: str) -> None:
    report = _rust_report([filename])
    config = _rust_config()
    (tmp_path / "rust.json").write_text(json.dumps(report), encoding="utf-8")

    with pytest.raises(reports.CoverageRatchetError, match="no in-scope files"):
        reports._llvm_snapshot(report, tmp_path, config)
    assert growth._coverage_files(tmp_path, config) == {}
