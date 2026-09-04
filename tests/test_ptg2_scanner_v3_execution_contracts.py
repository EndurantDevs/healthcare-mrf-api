# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import hashlib
import importlib.util
import io
import json
import os
import struct
import subprocess
import sys
import types
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from process.ptg_parts.ptg2_shared_finalize import (
    attach_v3_dictionary_contract,
    attach_v3_source_run_contract,
    write_v3_finalizer_input_manifest,
)
from process.ptg_parts.ptg2_provider_quarantine import (
    provider_identifier_quarantine_payload,
)
from process.ptg_parts.ptg2_source_witness import (
    build_persisted_source_witness,
    decode_persisted_source_witness,
)

from tests.ptg2_scanner_v3_release_support import (
    _AUDIT_CANDIDATE_RECORD,
    _MIB,
    _SERVING_RECORD,
    _STRICT_SCANNER_FRAME_KINDS,
    _SUPPORT_MODULE,
    _built_scanner_binary,
    _decode_by_code_groups,
    _fixture_network_entries,
    _fixture_payload,
    _fixture_provider_references,
    _load_isolated_rust_scanner,
    _load_isolated_shared_blocks,
    _network_rate_fixture,
    _parse_scanner_frames,
    _pg_binary_copy_rows,
    _read_pg_binary_rows,
    _read_uvarint,
    _single_frame,
    _v3_finalizer_test_resource_args,
)
from tests.ptg2_scanner_v3_run_support import (
    _ScannerRunOptions,
    _ScannerRunPaths,
    _assert_scanner_execution_mode_contracts,
    _assert_scanner_partition_contract,
    _assert_scanner_publication_files,
    _assert_scanner_source_witness,
    _assert_strict_scanner_run,
    _malformed_provider_identifier_payload,
    _run_scanner,
    _scanner_environment,
    _scanner_execution_environment,
    _scanner_fixture_artifact,
    _scanner_frames_of_kind,
    _scanner_output_environment,
    _scanner_result,
    _scanner_run_paths,
)

def test_scanner_quarantine_is_identical_across_execution_modes(tmp_path):
    scanner_binary = _built_scanner_binary()
    mode_specs_by_name = {
        "parallel": {
            "provider_references_first": True,
            "top_level_byte_scan": True,
            "execution_mode": "parallel_top_level_bytes",
        },
        "late_reordered": {
            "provider_references_first": False,
            "top_level_byte_scan": True,
            "execution_mode": "parallel_top_level_bytes_plain_range_reorder",
        },
    }
    runs_by_mode = {
        mode: _run_scanner(
            scanner_binary,
            tmp_path,
            f"provider-identifier-quarantine-{mode}",
            arch="postgres_binary_v3",
            provider_references_first=spec["provider_references_first"],
            grouped=False,
            fixture_payload=_malformed_provider_identifier_payload(
                provider_references_first=spec["provider_references_first"]
            ),
            top_level_byte_scan=spec["top_level_byte_scan"],
        )
        for mode, spec in mode_specs_by_name.items()
    }

    expected_quarantine = provider_identifier_quarantine_payload(
        {123456787: 1, 123456789: 2},
        text_counts={"1447744750`": 3},
    )
    quarantine_evidence_list = []
    malformed_npis = {123456787, 123456789}
    for mode, run in runs_by_mode.items():
        config = _single_frame(run["frames"], "scanner_config")
        summary = _single_frame(run["frames"], "scanner_summary")
        assert config["execution_mode"] == mode_specs_by_name[mode]["execution_mode"]
        assert summary["serving_run_rows"] == 2
        quarantine_evidence_list.append(summary["provider_identifier_quarantine"])

        member_rows = _SUPPORT_MODULE._sorted_copy_rows(
            run["provider_group_member_copy_path"]
        )
        member_npis = tuple(
            sorted(int(member_row.rsplit(b"\t", 1)[1]) for member_row in member_rows)
        )
        assert member_npis == (1234567890, 1234567891)
        assert malformed_npis.isdisjoint(member_npis)

    assert quarantine_evidence_list == [expected_quarantine] * len(mode_specs_by_name)

def test_v3_all_scanner_paths_emit_identical_fixed_width_records(tmp_path):
    """Verify v3 all scanner paths emit identical fixed width records."""
    scanner_binary = _built_scanner_binary()
    scanner_runs_by_mode = {
        "worker_ungrouped": _run_scanner(
            scanner_binary,
            tmp_path,
            "worker-ungrouped",
            arch="postgres_binary_v3",
            provider_references_first=True,
            grouped=False,
        ),
        "late_reordered": _run_scanner(
            scanner_binary,
            tmp_path,
            "late-reordered",
            arch="postgres_binary_v3",
            provider_references_first=False,
            grouped=False,
        ),
    }
    _assert_scanner_execution_mode_contracts(scanner_runs_by_mode)
    for run in scanner_runs_by_mode.values():
        _assert_strict_scanner_run(run)


def test_v3_worker_and_serial_paths_preserve_source_rate_occurrences(tmp_path):
    scanner_binary = _built_scanner_binary()
    runs = [
        _run_scanner(
            scanner_binary,
            tmp_path,
            "worker-multiset",
            arch="postgres_binary_v3",
            provider_references_first=True,
            grouped=False,
            repeated_rate_occurrences=True,
        ),
        _run_scanner(
            scanner_binary,
            tmp_path,
            "serial-multiset",
            arch="postgres_binary_v3",
            provider_references_first=False,
            grouped=False,
            repeated_rate_occurrences=True,
        ),
    ]

    for run in runs:
        partition_bytes = run["partition_bytes"]
        assert len(partition_bytes) == 3 * _SERVING_RECORD.size
        serving_records = [
            _SERVING_RECORD.unpack(
                partition_bytes[offset : offset + _SERVING_RECORD.size]
            )
            for offset in range(0, len(partition_bytes), _SERVING_RECORD.size)
        ]
        assert sorted(Counter(serving_records).values()) == [1, 2]
        assert len({serving_record[0] for serving_record in serving_records}) == 1
        assert len({serving_record[1] for serving_record in serving_records}) == 2
        assert len({serving_record[2] for serving_record in serving_records}) == 1
        assert sorted({serving_record[3] for serving_record in serving_records}) == [
            1,
            2,
        ]
        assert sum(frame["row_count"] for frame in run["partition_frames"]) == 3
        assert _single_frame(run["frames"], "scanner_summary")["serving_run_rows"] == 3
        assert sum(
            frame["row_count"] for frame in run["provider_group_member_frames"]
        ) == 3


def test_strict_v3_rejects_negotiated_rate_grouping_before_input_open(tmp_path):
    scanner_environment_map = {
        **os.environ,
        "HLTHPRT_PTG2_SNAPSHOT_ARCH": "postgres_binary_v3",
        "HLTHPRT_PTG2_RUST_GROUP_NEGOTIATED_RATE_CHUNKS": "true",
        "HLTHPRT_PTG2_V3_SERVING_RUN_DIR": str(tmp_path / "serving-runs"),
    }

    completed = subprocess.run(
        [str(_built_scanner_binary()), "--compact-serving", str(tmp_path / "missing.json")],
        check=False,
        env=scanner_environment_map,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=120,
    )

    assert completed.returncode != 0
    assert b"must be false for strict V3 exact source multiplicity" in completed.stderr
    assert not (tmp_path / "serving-runs").exists()


@pytest.mark.parametrize(
    "arch",
    [
        None,
        "postgres_binary_v2",
        "db_binary_v3",
        "binary_v3",
        "postgres-binary-v3",
        "POSTGRES_BINARY_V3",
        " postgres_binary_v3 ",
    ],
)
def test_scanner_requires_exact_postgres_binary_v3_arch_before_input_open(
    tmp_path, arch
):
    scanner_environment_map = dict(os.environ)
    if arch is None:
        scanner_environment_map.pop("HLTHPRT_PTG2_SNAPSHOT_ARCH", None)
    else:
        scanner_environment_map["HLTHPRT_PTG2_SNAPSHOT_ARCH"] = arch
    scanner_environment_map["HLTHPRT_PTG2_V3_SERVING_RUN_DIR"] = str(
        tmp_path / "serving-runs"
    )

    completed = subprocess.run(
        [str(_built_scanner_binary()), "--compact-serving", str(tmp_path / "missing.json")],
        check=False,
        env=scanner_environment_map,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=120,
    )

    assert completed.returncode != 0
    assert (
        b"HLTHPRT_PTG2_SNAPSHOT_ARCH must be exactly postgres_binary_v3"
        in completed.stderr
    )
    assert not (tmp_path / "serving-runs").exists()


def test_scanner_requires_explicit_v3_run_directory_without_legacy_derivation(
    tmp_path,
):
    legacy_lean_path = tmp_path / "legacy-lean.copy"
    scanner_environment_map = {
        **os.environ,
        "HLTHPRT_PTG2_SNAPSHOT_ARCH": "postgres_binary_v3",
        "HLTHPRT_PTG2_MANIFEST_LEAN_SERVING_COPY_PATH": str(legacy_lean_path),
    }
    scanner_environment_map.pop("HLTHPRT_PTG2_V3_SERVING_RUN_DIR", None)

    completed = subprocess.run(
        [str(_built_scanner_binary()), "--compact-serving", str(tmp_path / "missing.json")],
        check=False,
        env=scanner_environment_map,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=120,
    )

    assert completed.returncode != 0
    assert b"HLTHPRT_PTG2_V3_SERVING_RUN_DIR must be set explicitly" in completed.stderr
    assert not legacy_lean_path.exists()
    assert not Path(f"{legacy_lean_path}.v3-runs").exists()
