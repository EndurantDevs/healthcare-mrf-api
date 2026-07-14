# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import importlib.util
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import time

import pytest


_SUPPORT_PATH = Path(__file__).with_name("test_ptg2_scanner_parallelism.py")
_SUPPORT_SPEC = importlib.util.spec_from_file_location(
    "ptg2_scanner_late_reference_support", _SUPPORT_PATH
)
assert _SUPPORT_SPEC is not None and _SUPPORT_SPEC.loader is not None
_SUPPORT = importlib.util.module_from_spec(_SUPPORT_SPEC)
_SUPPORT_SPEC.loader.exec_module(_SUPPORT)


def _single_frame(run: dict, record_kind: str) -> dict:
    matches = [payload for kind, payload in run["frames"] if kind == record_kind]
    assert len(matches) == 1
    return matches[0]


@pytest.fixture(scope="module")
def scanner_plain_late_reference_runs(tmp_path_factory):
    scanner_binary = _SUPPORT._built_scanner_binary()
    base = tmp_path_factory.mktemp("ptg2-scanner-plain-late-references")
    fixture = _SUPPORT._scanner_fixture_payload(procedure_count=240)
    artifact = base / "rates.json"
    artifact.write_text(
        json.dumps(
            {
                "reporting_entity_name": fixture["reporting_entity_name"],
                "in_network": fixture["in_network"],
                "provider_references": fixture["provider_references"],
            },
            separators=(",", ":"),
        ),
        encoding="utf-8",
    )
    return {
        workers: _SUPPORT._run_parallel_scanner(
            scanner_binary,
            artifact,
            base / f"workers-{workers}",
            workers=workers,
            sidecar_kinds=tuple(_SUPPORT.SIDECAR_ENV_BY_KIND),
        )
        for workers in (1, 8)
    }


@pytest.fixture(scope="module")
def scanner_gzip_late_reference_runs(tmp_path_factory):
    scanner_binary = _SUPPORT._built_scanner_binary()
    base = tmp_path_factory.mktemp("ptg2-scanner-gzip-late-references")
    fixture = _SUPPORT._scanner_fixture_payload(procedure_count=240)
    artifact = base / "rates.json.gz"
    _SUPPORT._write_gzip_json(
        artifact,
        {
            "reporting_entity_name": fixture["reporting_entity_name"],
            "in_network": fixture["in_network"],
            "provider_references": fixture["provider_references"],
        },
    )
    return {
        workers: _SUPPORT._run_parallel_scanner(
            scanner_binary,
            artifact,
            base / f"workers-{workers}",
            workers=workers,
            sidecar_kinds=tuple(_SUPPORT.SIDECAR_ENV_BY_KIND),
            env_overrides={
                "HLTHPRT_PTG2_RUST_RAPIDGZIP_ENABLED": "true",
                "HLTHPRT_PTG2_RUST_RAPIDGZIP_THREADS": str(workers),
                "HLTHPRT_PTG2_RUST_INDEXED_RANGE_PRODUCERS": str(workers),
            },
        )
        for workers in (1, 8, 16)
    }


def test_late_plain_provider_references_are_reordered_and_processed_in_parallel(
    scanner_plain_late_reference_runs,
):
    baseline = scanner_plain_late_reference_runs[1]
    candidate = scanner_plain_late_reference_runs[8]

    assert candidate["copy_rows"] == baseline["copy_rows"]
    assert candidate["copy_digests"] == baseline["copy_digests"]
    assert candidate["sidecar_digests"] == baseline["sidecar_digests"]
    assert candidate["serving_records"] == baseline["serving_records"]
    assert candidate["serving_digest"] == baseline["serving_digest"]
    assert candidate["ids"] == baseline["ids"]
    assert _single_frame(candidate, "dedupe_summary") == _single_frame(
        baseline, "dedupe_summary"
    )

    config = _single_frame(candidate, "scanner_config")
    summary = _single_frame(candidate, "scanner_summary")
    assert config["execution_mode"] == "parallel_top_level_bytes_plain_range_reorder"
    assert config["provider_reference_order"] == "after_in_network"
    assert config["plain_range_reorder"] is True
    assert config["plain_provider_range_bytes"] > 0
    assert config["plain_in_network_range_bytes"] > 0
    assert config["plain_in_network_object_count"] == 240
    assert config["order_probe_partial_pass"] is True
    assert sum(worker["rates_seen"] for worker in summary["workers"]) == 240 * 8
    assert sum(worker["rates_seen"] > 0 for worker in summary["workers"]) >= 2


def test_late_gzip_provider_references_are_indexed_and_byte_identical(
    scanner_gzip_late_reference_runs,
):
    baseline = scanner_gzip_late_reference_runs[1]
    for workers in (8, 16):
        candidate = scanner_gzip_late_reference_runs[workers]
        assert candidate["copy_rows"] == baseline["copy_rows"]
        assert candidate["copy_digests"] == baseline["copy_digests"]
        assert candidate["sidecar_digests"] == baseline["sidecar_digests"]
        assert candidate["serving_records"] == baseline["serving_records"]
        assert candidate["serving_digest"] == baseline["serving_digest"]
        assert candidate["ids"] == baseline["ids"]
        assert _single_frame(candidate, "dedupe_summary") == _single_frame(
            baseline, "dedupe_summary"
        )

        config = _single_frame(candidate, "scanner_config")
        summary = _single_frame(candidate, "scanner_summary")
        assert config["execution_mode"] == "parallel_top_level_bytes_indexed_reorder"
        assert config["decompression"] == "rapidgzip"
        assert config["provider_reference_order"] == "after_in_network"
        assert config["rapidgzip_index_bytes"] > 0
        assert config["indexed_range_producers_requested"] == workers
        assert config["indexed_range_producers_selected"] == workers
        assert config["indexed_range_count"] == workers
        assert config["order_probe_partial_pass"] is True
        assert sum(worker["rates_seen"] for worker in summary["workers"]) == 240 * 8
        assert sum(worker["rates_seen"] > 0 for worker in summary["workers"]) >= 2


def _write_blocking_rapidgzip_wrapper(wrapper: Path) -> None:
    wrapper.write_text(
        """#!/bin/sh
set -eu
uses_import_index=false
for argument in "$@"; do
    if [ "$argument" = "--import-index" ]; then
        uses_import_index=true
    fi
done
if [ "$uses_import_index" != "true" ]; then
    exec "$REAL_RAPIDGZIP" "$@"
fi
if [ ! -e "$RAPIDGZIP_TEST_STATE/provider-range-complete" ]; then
    "$REAL_RAPIDGZIP" "$@"
    status=$?
    : > "$RAPIDGZIP_TEST_STATE/provider-range-complete"
    exit "$status"
fi
if mkdir "$RAPIDGZIP_TEST_STATE/blocked-range" 2>/dev/null; then
    sleep 300 &
    descendant_pid=$!
    printf '%s %s\n' "$$" "$descendant_pid" > "$RAPIDGZIP_TEST_STATE/blocked-pids"
    wait "$descendant_pid"
fi
while [ ! -s "$RAPIDGZIP_TEST_STATE/blocked-pids" ]; do
    sleep 0.01
done
exit 42
""",
        encoding="utf-8",
    )
    wrapper.chmod(0o755)


def _recorded_pids(path: Path) -> tuple[int, ...]:
    return tuple(int(value) for value in path.read_text(encoding="utf-8").split())


def _process_exists(process_id: int) -> bool:
    try:
        os.kill(process_id, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _wait_for_process_cleanup(process_ids: tuple[int, ...]) -> None:
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        if not any(_process_exists(process_id) for process_id in process_ids):
            return
        time.sleep(0.02)
    assert not [process_id for process_id in process_ids if _process_exists(process_id)]


@pytest.mark.skipif(os.name != "posix", reason="process-group cleanup is POSIX-specific")
def test_indexed_peer_failure_interrupts_blocked_decoder_and_descendants(tmp_path):
    """A failed range must cancel a blocked peer and its descendant without hanging."""
    real_rapidgzip = shutil.which("rapidgzip")
    if real_rapidgzip is None:
        pytest.skip("rapidgzip is not installed")

    fixture = _SUPPORT._scanner_fixture_payload(procedure_count=96)
    artifact = tmp_path / "rates.json.gz"
    _SUPPORT._write_gzip_json(
        artifact,
        {
            "reporting_entity_name": fixture["reporting_entity_name"],
            "in_network": fixture["in_network"],
            "provider_references": fixture["provider_references"],
        },
    )
    state_dir = tmp_path / "rapidgzip-state"
    state_dir.mkdir()
    wrapper = tmp_path / "rapidgzip-wrapper"
    _write_blocking_rapidgzip_wrapper(wrapper)
    run_dir = tmp_path / "scanner-run"
    run_dir.mkdir()
    scanner_environment = _SUPPORT._scanner_environment(
        8, None, run_dir / "serving-runs"
    )
    scanner_environment.update(
        {
            _SUPPORT.COPY_ENV_BY_KIND["price_atom"]: str(run_dir / "price-atom.copy"),
            "HLTHPRT_PTG2_RUST_RAPIDGZIP_ENABLED": "true",
            "HLTHPRT_PTG2_RUST_RAPIDGZIP_BIN": str(wrapper),
            "HLTHPRT_PTG2_RUST_RAPIDGZIP_THREADS": "2",
            "HLTHPRT_PTG2_RUST_INDEXED_RANGE_PRODUCERS": "2",
            "REAL_RAPIDGZIP": real_rapidgzip,
            "RAPIDGZIP_TEST_STATE": str(state_dir),
        }
    )
    scanner_binary = _SUPPORT._built_scanner_binary()
    started_at = time.monotonic()
    try:
        completed = subprocess.run(
            [str(scanner_binary), "--compact-serving", str(artifact)],
            check=False,
            env=scanner_environment,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=20,
        )
    except subprocess.TimeoutExpired:
        pid_path = state_dir / "blocked-pids"
        if pid_path.exists():
            for process_id in _recorded_pids(pid_path):
                try:
                    os.killpg(process_id, signal.SIGKILL)
                except ProcessLookupError:
                    continue
        pytest.fail("scanner hung after indexed rapidgzip peer failure")

    assert time.monotonic() - started_at < 10
    assert completed.returncode != 0
    stderr = completed.stderr.decode("utf-8", errors="replace")
    assert "indexed range producer" in stderr
    assert "rapidgzip failed" in stderr
    assert "exit status: 42" in stderr
    blocked_pids = _recorded_pids(state_dir / "blocked-pids")
    assert len(blocked_pids) == 2
    _wait_for_process_cleanup(blocked_pids)
