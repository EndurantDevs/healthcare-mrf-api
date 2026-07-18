import gzip
import hashlib
import importlib.util
import io
import json
import os
import re
import subprocess
import sys
import types
from pathlib import Path

import pytest


COPY_ENV_BY_KIND = {
    "price_atom": "HLTHPRT_PTG2_MANIFEST_PRICE_ATOM_COPY_PATH",
    "price_set_atom": "HLTHPRT_PTG2_MANIFEST_PRICE_SET_ATOM_COPY_PATH",
    "price_set_summary": "HLTHPRT_PTG2_MANIFEST_PRICE_SET_SUMMARY_COPY_PATH",
    "provider_group_member": "HLTHPRT_PTG2_MANIFEST_PROVIDER_GROUP_MEMBER_COPY_PATH",
}

SIDECAR_ENV_BY_KIND = {
    "provider_forward": "HLTHPRT_PTG2_MANIFEST_PROVIDER_FORWARD_SIDECAR_PATH",
    "provider_inverted": "HLTHPRT_PTG2_MANIFEST_PROVIDER_INVERTED_SIDECAR_PATH",
}

LEGACY_OUTPUT_ENVS = (
    "HLTHPRT_PTG2_COMPACT_SERVING_COPY_PATH",
    "HLTHPRT_PTG2_MANIFEST_SERVING_COPY_PATH",
    "HLTHPRT_PTG2_MANIFEST_LEAN_SERVING_COPY_PATH",
    "HLTHPRT_PTG2_MANIFEST_PROVIDER_NPI_SIDECAR_PATH",
    "HLTHPRT_PTG2_MANIFEST_PRICE_FORWARD_SIDECAR_PATH",
    "HLTHPRT_PTG2_MANIFEST_CODE_COUNT_COPY_PATH",
    "HLTHPRT_PTG2_MANIFEST_PROVIDER_SET_DICTIONARY_COPY_PATH",
    "HLTHPRT_PTG2_PROCEDURE_COPY_PATH",
    "HLTHPRT_PTG2_PRICE_CODE_SET_COPY_PATH",
    "HLTHPRT_PTG2_PRICE_ATOM_COPY_PATH",
    "HLTHPRT_PTG2_PRICE_SET_ENTRY_COPY_PATH",
    "HLTHPRT_PTG2_PROVIDER_SET_COPY_PATH",
    "HLTHPRT_PTG2_PROVIDER_SET_COMPONENT_COPY_PATH",
    "HLTHPRT_PTG2_PROVIDER_SET_ENTRY_COPY_PATH",
    "HLTHPRT_PTG2_PROVIDER_ENTRY_COMPONENT_COPY_PATH",
    "HLTHPRT_PTG2_PROVIDER_GROUP_MEMBER_COPY_PATH",
)

HEX_ID_PATTERN = re.compile(rb"(?<![0-9a-f])[0-9a-f]{32,64}(?![0-9a-f])")
SERVING_RUN_RECORD_BYTES = 52


def _built_scanner_binary() -> Path:
    root = Path(__file__).resolve().parents[1]
    scanner_root = root / "support" / "ptg2_scanner"
    candidate = scanner_root / "target" / "debug" / "ptg2_scanner"
    source_mtime = max(path.stat().st_mtime for path in (scanner_root / "src").glob("*.rs"))
    if not candidate.exists() or candidate.stat().st_mtime < source_mtime:
        subprocess.run(
            ["cargo", "build", "--manifest-path", str(scanner_root / "Cargo.toml")],
            check=True,
            cwd=root,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=120,
        )
    if candidate.exists() and os.access(candidate, os.X_OK):
        return candidate
    pytest.skip("PTG2 Rust scanner binary could not be built")


def _parse_scanner_frames(stdout: bytes) -> list[tuple[str, dict]]:
    frames = []
    stream = io.BytesIO(stdout)
    while header := stream.readline():
        record_kind, payload_size = header.rstrip(b"\n").split(b"\t", 1)
        payload = stream.read(int(payload_size))
        assert stream.read(1) == b"\n"
        frames.append((record_kind.decode("utf-8"), json.loads(payload)))
    return frames


def _scanner_fixture_payload(*, procedure_count: int = 24) -> dict:
    provider_references = [
        {
            "provider_group_id": provider_id,
            "provider_groups": [
                {
                    "npi": [1234567000 + provider_id, 1234568000 + provider_id],
                    "tin": {"type": "ein", "value": f"12-345{provider_id:04d}"},
                }
            ],
        }
        for provider_id in range(1, 33)
    ]
    in_network_entries = []
    for procedure_index in range(procedure_count):
        negotiated_rates = []
        for rate_index in range(8):
            provider_id = (procedure_index * 3 + rate_index) % len(provider_references) + 1
            negotiated_rates.append(
                {
                    "provider_references": [provider_id],
                    "negotiated_prices": [
                        {
                            "negotiated_type": "negotiated",
                            "negotiated_rate": 75 + procedure_index * 10 + rate_index,
                            "service_code": ["11", "22"],
                            "billing_class": "professional",
                        }
                    ],
                }
            )
        negotiated_rates[1] = dict(negotiated_rates[0])
        in_network_entries.append(
            {
                "billing_code_type": "CPT",
                "billing_code_type_version": "2026",
                "billing_code": f"{99200 + procedure_index}",
                "name": f"Synthetic procedure {procedure_index}",
                "description": "parallel scanner parity fixture",
                "negotiated_rates": negotiated_rates,
            }
        )
    return {
        "reporting_entity_name": "scanner-parity",
        "provider_references": provider_references,
        "in_network": in_network_entries,
    }


def _write_gzip_json(path: Path, payload: dict) -> None:
    with path.open("wb") as raw_artifact:
        with gzip.GzipFile(fileobj=raw_artifact, mode="wb", mtime=0) as artifact:
            artifact.write(json.dumps(payload, separators=(",", ":")).encode("utf-8"))


def _sorted_copy_rows(path: Path) -> tuple[bytes, ...]:
    rows = []
    for candidate in sorted(path.parent.glob(f"{path.name}*")):
        if candidate.is_file():
            rows.extend(candidate.read_bytes().splitlines())
    return tuple(sorted(rows))


def _canonical_rows_digest(rows: tuple[bytes, ...]) -> str:
    return hashlib.sha256(b"\n".join(rows)).hexdigest()


def _scanner_environment(
    workers: int, queue_size: int | None, serving_run_directory: Path
) -> dict[str, str]:
    scanner_env_map = {
        **os.environ,
        "HLTHPRT_PTG2_SNAPSHOT_ARCH": "postgres_binary_v3",
        "HLTHPRT_PTG2_RAW_SOURCE_SHA256": "00" * 32,
        "HLTHPRT_PTG2_V3_COVERAGE_SCOPE_ID": (b"\xcc" * 32).hex(),
        "HLTHPRT_PTG2_V3_SERVING_RUN_DIR": str(serving_run_directory),
        "HLTHPRT_PTG2_V3_SERVING_RUN_PARTITIONS": "4",
        "HLTHPRT_PTG2_COMPACT_SNAPSHOT_ID": "snapshot-parallelism-v3",
        "HLTHPRT_PTG2_COMPACT_PLAN_ID": "plan-parallelism-v3",
        "HLTHPRT_PTG2_COMPACT_PLAN_MONTH_ID": "plan-month-parallelism-v3",
        "HLTHPRT_PTG2_COMPACT_SOURCE_TRACE_SET_HASH": "trace-parallelism-v3",
        "HLTHPRT_PTG2_MANIFEST_ONLY": "false",
        "HLTHPRT_PTG2_RUST_WORKERS": str(workers),
        "HLTHPRT_PTG2_RUST_WORK_QUEUE": str(queue_size or max(workers * 2, 2)),
        "HLTHPRT_PTG2_RUST_EVENT_QUEUE": str(max(workers * 4, 8)),
        "HLTHPRT_PTG2_RUST_SPLIT_NEGOTIATED_RATES": "4",
        "HLTHPRT_PTG2_RUST_RAW_CHUNK_BYTES": "2048",
        "HLTHPRT_PTG2_RUST_PARSE_IN_WORKERS": "true",
        "HLTHPRT_PTG2_RUST_TOP_LEVEL_BYTE_SCAN": "true",
        "HLTHPRT_PTG2_RUST_PROVIDER_REFS_IN_WORKERS": "true",
        "HLTHPRT_PTG2_RUST_PROVIDER_REF_WORKERS": str(workers),
        "HLTHPRT_PTG2_RUST_PROVIDER_REF_QUEUE": str(max(workers * 2, 2)),
        "HLTHPRT_PTG2_RUST_PROVIDER_REF_CHUNK_ITEMS": "2",
        "HLTHPRT_PTG2_RUST_PROVIDER_REF_RAW_CHUNK_BYTES": "1024",
        "HLTHPRT_PTG2_COMPACT_SERVING_COPY_ROTATE_BYTES": "0",
        "HLTHPRT_PTG2_RUST_GROUP_NEGOTIATED_RATE_CHUNKS": "false",
    }
    for env_name in (
        *COPY_ENV_BY_KIND.values(),
        *SIDECAR_ENV_BY_KIND.values(),
        *LEGACY_OUTPUT_ENVS,
    ):
        scanner_env_map.pop(env_name, None)
    return scanner_env_map


def _serving_run_records(frames: list[tuple[str, dict]]) -> tuple[bytes, ...]:
    records = []
    for record_kind, payload in frames:
        if record_kind != "v3_serving_run_partition_file":
            continue
        run_payload = Path(payload["path"]).read_bytes()
        assert len(run_payload) % SERVING_RUN_RECORD_BYTES == 0
        records.extend(
            run_payload[offset : offset + SERVING_RUN_RECORD_BYTES]
            for offset in range(0, len(run_payload), SERVING_RUN_RECORD_BYTES)
        )
    return tuple(sorted(records))


def _scanner_run_result(
    frames: list[tuple[str, dict]],
    copy_path_by_kind: dict[str, Path],
    sidecar_path_by_kind: dict[str, Path],
    run_dir: Path,
) -> dict:
    copy_rows_by_kind = {
        kind: _sorted_copy_rows(copy_path)
        for kind, copy_path in copy_path_by_kind.items()
    }
    serving_records = _serving_run_records(frames)
    return {
        "frames": frames,
        "run_dir": run_dir,
        "copy_rows": copy_rows_by_kind,
        "copy_digests": {
            kind: _canonical_rows_digest(copy_output_rows)
            for kind, copy_output_rows in copy_rows_by_kind.items()
        },
        "sidecar_digests": {
            kind: hashlib.sha256(sidecar_path.read_bytes()).hexdigest()
            for kind, sidecar_path in sidecar_path_by_kind.items()
        },
        "serving_records": serving_records,
        "serving_digest": hashlib.sha256(b"".join(serving_records)).hexdigest(),
        "ids": {
            global_id.decode("ascii")
            for copy_output_rows in copy_rows_by_kind.values()
            for copy_row in copy_output_rows
            for global_id in HEX_ID_PATTERN.findall(copy_row)
        },
    }


def _run_parallel_scanner(
    scanner_binary: Path,
    artifact: Path,
    run_dir: Path,
    *,
    workers: int,
    queue_size: int | None = None,
    copy_kinds: tuple[str, ...] | None = None,
    sidecar_kinds: tuple[str, ...] = (),
    env_overrides: dict[str, str] | None = None,
) -> dict:
    run_dir.mkdir()
    selected_copy_kinds = tuple(COPY_ENV_BY_KIND) if copy_kinds is None else copy_kinds
    copy_path_by_kind = {kind: run_dir / f"{kind}.copy" for kind in selected_copy_kinds}
    sidecar_path_by_kind = {kind: run_dir / f"{kind}.sidecar" for kind in sidecar_kinds}
    scanner_env_map = _scanner_environment(
        workers, queue_size, run_dir / "serving-runs"
    )
    scanner_env_map["HLTHPRT_PTG2_RAW_SOURCE_SHA256"] = hashlib.sha256(
        artifact.read_bytes()
    ).hexdigest()
    scanner_env_map.update(
        {COPY_ENV_BY_KIND[kind]: str(copy_path) for kind, copy_path in copy_path_by_kind.items()}
    )
    scanner_env_map.update(env_overrides or {})
    scanner_env_map.update(
        {
            SIDECAR_ENV_BY_KIND[kind]: str(sidecar_path)
            for kind, sidecar_path in sidecar_path_by_kind.items()
        }
    )
    completed = subprocess.run(
        [str(scanner_binary), "--compact-serving", str(artifact)],
        check=True,
        env=scanner_env_map,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=120,
    )
    frames = _parse_scanner_frames(completed.stdout)
    return _scanner_run_result(
        frames, copy_path_by_kind, sidecar_path_by_kind, run_dir
    )


@pytest.fixture(scope="module")
def scanner_parallelism_runs(tmp_path_factory):
    scanner_binary = _built_scanner_binary()
    base = tmp_path_factory.mktemp("ptg2-scanner-parallelism")
    artifact = base / "rates.json.gz"
    _write_gzip_json(artifact, _scanner_fixture_payload())
    return {
        workers: _run_parallel_scanner(
            scanner_binary,
            artifact,
            base / f"workers-{workers}",
            workers=workers,
            sidecar_kinds=tuple(SIDECAR_ENV_BY_KIND),
        )
        for workers in (1, 8, 16)
    }


def _single_frame(run: dict, record_kind: str) -> dict:
    matches = [payload for kind, payload in run["frames"] if kind == record_kind]
    assert len(matches) == 1
    return matches[0]


def _load_isolated_rust_scanner(monkeypatch):
    root = Path(__file__).resolve().parents[1]
    process_package = types.ModuleType("process")
    process_package.__path__ = [str(root / "process")]
    ptg_parts_package = types.ModuleType("process.ptg_parts")
    ptg_parts_package.__path__ = [str(root / "process" / "ptg_parts")]

    config = types.ModuleType("process.ptg_parts.config")
    config.PTG2_DEFAULT_RUST_EVENT_QUEUE = 32
    config.PTG2_DEFAULT_RUST_SPLIT_NEGOTIATED_RATES = 8192
    config.PTG2_DEFAULT_RUST_WORK_QUEUE = 32
    config.PTG2_DEFAULT_RUST_WORKERS = 16
    config.PTG2_FAST_JSON_LOADS_ENV = "HLTHPRT_PTG2_FAST_JSON_LOADS"
    config.PTG2_RUST_EVENT_QUEUE_ENV = "HLTHPRT_PTG2_RUST_EVENT_QUEUE"
    config.PTG2_RUST_REQUIRE_RELEASE_ENV = "HLTHPRT_PTG2_RUST_REQUIRE_RELEASE"
    config.PTG2_RUST_SCANNER_BIN_ENV = "HLTHPRT_PTG2_RUST_SCANNER_BIN"
    config.PTG2_RUST_SPLIT_NEGOTIATED_RATES_ENV = "HLTHPRT_PTG2_RUST_SPLIT_NEGOTIATED_RATES"
    config.PTG2_RUST_WORK_QUEUE_ENV = "HLTHPRT_PTG2_RUST_WORK_QUEUE"
    config.PTG2_RUST_WORKERS_ENV = "HLTHPRT_PTG2_RUST_WORKERS"
    config._env_bool = lambda _name, default: default
    config._env_int = lambda _name, default: default

    domain = types.ModuleType("process.ptg_parts.domain")
    domain.PTG2_CONFIDENCE_TIC_RATE_NPI_TIN = "tic_rate_npi_tin"
    live_progress = types.ModuleType("process.ptg_parts.live_progress")
    live_progress.current_live_progress_context = lambda: {}
    live_progress.write_live_progress = lambda **_payload: None
    progress = types.ModuleType("process.ptg_parts.progress")
    progress._scale_stage_progress_pct = lambda phase_pct, _start, _end: phase_pct
    screen = types.ModuleType("process.ptg_parts.screen")
    screen._emit_screen_line = lambda _line: None

    module_by_name = {
        "process": process_package,
        "process.ptg_parts": ptg_parts_package,
        "process.ptg_parts.config": config,
        "process.ptg_parts.domain": domain,
        "process.ptg_parts.live_progress": live_progress,
        "process.ptg_parts.progress": progress,
        "process.ptg_parts.screen": screen,
    }
    for module_name, module in module_by_name.items():
        monkeypatch.setitem(sys.modules, module_name, module)

    module_path = root / "process" / "ptg_parts" / "rust_scanner.py"
    spec = importlib.util.spec_from_file_location("isolated_ptg2_rust_scanner", module_path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_scanner_outputs_are_identical_with_1_8_and_16_workers(scanner_parallelism_runs):
    baseline = scanner_parallelism_runs[1]
    baseline_dedupe = _single_frame(baseline, "dedupe_summary")
    assert baseline["ids"]

    for workers in (8, 16):
        candidate = scanner_parallelism_runs[workers]
        assert candidate["copy_rows"] == baseline["copy_rows"]
        assert candidate["copy_digests"] == baseline["copy_digests"]
        assert candidate["sidecar_digests"] == baseline["sidecar_digests"]
        assert candidate["serving_records"] == baseline["serving_records"]
        assert candidate["serving_digest"] == baseline["serving_digest"]
        assert candidate["ids"] == baseline["ids"]
        assert _single_frame(candidate, "dedupe_summary") == baseline_dedupe


def test_scanner_queue_and_buffer_metrics_stay_bounded(scanner_parallelism_runs):
    summary = _single_frame(scanner_parallelism_runs[1], "scanner_summary")

    assert summary["work_queue_high_water"] <= summary["work_queue"]
    # A sender accounts its candidate before try_send(), while a receiver can
    # own a chunk briefly before decrementing the shared metric. The transient
    # peak therefore includes the queue, one candidate, and at most one chunk
    # per worker. Final zero values prove that this conservative accounting does
    # not leak across the completed scan.
    assert summary["peak_queued_bytes"] <= (
        (summary["work_queue_high_water"] + len(summary["workers"]) + 1)
        * summary["raw_chunk_max_bytes"]
    )
    assert summary["queued_bytes_at_finish"] == 0
    assert summary["raw_buffer_allocations"] <= summary["raw_chunk_count"] + 1
    assert summary["raw_buffer_reuses"] > 0
    assert summary["producer_byte_capture_bytes"] == summary["raw_chunk_total_bytes"]
    assert summary["provider_ref_queue_high_water"] <= 2
    assert summary["provider_ref_peak_queued_bytes"] <= (
        (
            summary["provider_ref_queue_high_water"]
            + len(summary["provider_workers"])
            + 1
        )
        * summary["provider_ref_raw_chunk_max_bytes"]
    )
    assert summary["provider_ref_queued_bytes_at_finish"] == 0
    assert sum(worker["raw_bytes"] for worker in summary["workers"]) == summary["raw_chunk_total_bytes"]


def test_scanner_metric_contract_exposes_dense_fixture_bottleneck_split(scanner_parallelism_runs):
    config = _single_frame(scanner_parallelism_runs[8], "scanner_config")
    summary = _single_frame(scanner_parallelism_runs[8], "scanner_summary")

    assert config["scanner_metric_contract_version"] == 2
    assert config["snapshot_arch"] == "postgres_binary_v3"
    assert config["storage_generation"] == "shared_blocks_v3"
    assert config["execution_mode"] == "parallel_top_level_bytes"
    assert config["provider_reference_order"] == "before_in_network"
    assert config["top_level_byte_scan_selected"] is True
    assert config["top_level_byte_scan_fallback_reason"] is None
    assert config["raw_buffer_recycling"] is True
    assert config["provider_map_merge"] == "ordered_pairwise_parallel"
    assert config["order_detection_compressed_bytes"] > 0

    required_summary_fields = {
        "order_detection_seconds",
        "order_detection_compressed_mib_s",
        "provider_capture_seconds",
        "provider_parse_seconds",
        "provider_map_merge_seconds",
        "provider_worker_join_seconds",
        "producer_byte_framing_seconds",
        "producer_byte_capture_seconds",
        "producer_blocked_seconds",
        "producer_nonblocked_seconds",
        "producer_backpressure_pct",
        "producer_raw_mib_s",
        "producer_nonblocked_raw_mib_s",
        "producer_nonblocked_compressed_mib_s",
        "worker_join_seconds",
        "worker_sidecar_lock_wait_seconds",
        "sidecar_merge_write_seconds",
        "peak_queued_bytes",
        "workers",
        "provider_workers",
    }
    assert required_summary_fields <= summary.keys()
    assert summary["producer_nonblocked_seconds"] == pytest.approx(
        max(summary["in_network_enqueue_seconds"] - summary["producer_blocked_seconds"], 0.0)
    )
    assert len(summary["workers"]) == 8
    assert len(summary["provider_workers"]) == 8
    assert sum(worker["rates_seen"] for worker in summary["workers"]) == 24 * 8
    for worker in summary["workers"]:
        assert {
            "jobs",
            "rates_seen",
            "raw_bytes",
            "parse_seconds",
            "transform_seconds",
            "write_seconds",
            "sidecar_lock_wait_seconds",
        } <= worker.keys()


def test_python_bridge_emits_metric_progress_with_fallback_and_producer_split(monkeypatch):
    rust_scanner = _load_isolated_rust_scanner(monkeypatch)
    screen_lines = []
    progress_payloads = []
    monkeypatch.setattr(rust_scanner, "_emit_screen_line", screen_lines.append)
    monkeypatch.setattr(rust_scanner, "write_live_progress", lambda **payload: progress_payloads.append(payload))

    rust_scanner._emit_scanner_metric_progress(
        "scanner_config",
        {
            "execution_mode": "serial_struson",
            "provider_reference_order": "after_in_network",
            "top_level_byte_scan_selected": False,
            "top_level_byte_scan_fallback_reason": "provider_references_after_in_network",
        },
    )
    rust_scanner._emit_scanner_metric_progress(
        "scanner_summary",
        {
            "execution_mode": "parallel_top_level_bytes",
            "elapsed_seconds": 876.703431,
            "producer_nonblocked_seconds": 697.442883,
            "producer_blocked_seconds": 175.322618,
            "producer_nonblocked_raw_mib_s": 400.2,
        },
    )

    assert "fallback=provider_references_after_in_network" in screen_lines[0]
    assert "producer_nonblocked=697.443s" in screen_lines[1]
    assert "queue_blocked=175.323s" in screen_lines[1]
    assert progress_payloads[0]["scanner_fallback_reason"] == "provider_references_after_in_network"
    assert progress_payloads[1]["source"] == "ptg2-scanner-metrics"
