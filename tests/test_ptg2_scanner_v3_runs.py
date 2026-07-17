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
from pathlib import Path

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

_SUPPORT_PATH = Path(__file__).with_name("test_ptg2_scanner_parallelism.py")
_SUPPORT_SPEC = importlib.util.spec_from_file_location(
    "ptg2_scanner_v3_test_support", _SUPPORT_PATH
)
assert _SUPPORT_SPEC is not None and _SUPPORT_SPEC.loader is not None
_SUPPORT_MODULE = importlib.util.module_from_spec(_SUPPORT_SPEC)
_SUPPORT_SPEC.loader.exec_module(_SUPPORT_MODULE)

_built_scanner_binary = _SUPPORT_MODULE._built_scanner_binary
_load_isolated_rust_scanner = _SUPPORT_MODULE._load_isolated_rust_scanner
_parse_scanner_frames = _SUPPORT_MODULE._parse_scanner_frames

_SERVING_RECORD = struct.Struct(">16s16s16sI")
_AUDIT_CANDIDATE_RECORD = struct.Struct(">IIIII")
_MIB = 1024 * 1024
_STRICT_SCANNER_FRAME_KINDS = {
    "dedupe_summary",
    "manifest_price_atom_copy_file",
    "manifest_price_set_atom_copy_file",
    "manifest_provider_forward_sidecar_file",
    "manifest_provider_group_member_copy_file",
    "manifest_provider_inverted_sidecar_file",
    "scanner_config",
    "scanner_summary",
    "source_audit_witness_file",
    "v3_serving_code_dictionary_file",
    "v3_serving_run_partition_file",
}


def _v3_finalizer_test_resource_args() -> tuple[str, ...]:
    return (
        "--workers",
        "1",
        "--identity-map-max-bytes",
        str(64 * _MIB),
        "--total-sort-memory-bytes",
        str(64 * _MIB),
    )


def _read_pg_binary_rows(payload: bytes, expected_fields: int) -> list[list[bytes | None]]:
    stream = io.BytesIO(payload)
    assert stream.read(11) == b"PGCOPY\n\xff\r\n\0"
    assert struct.unpack(">i", stream.read(4))[0] == 0
    extension_bytes = struct.unpack(">i", stream.read(4))[0]
    assert extension_bytes >= 0
    assert len(stream.read(extension_bytes)) == extension_bytes
    rows = []
    while True:
        field_count = struct.unpack(">h", stream.read(2))[0]
        if field_count == -1:
            break
        assert field_count == expected_fields
        fields = []
        for _field_index in range(field_count):
            field_bytes = struct.unpack(">i", stream.read(4))[0]
            fields.append(None if field_bytes == -1 else stream.read(field_bytes))
        rows.append(fields)
    assert stream.read() == b""
    return rows


def _pg_binary_copy_rows(rows: list[list[bytes | None]]) -> bytes:
    payload = bytearray(b"PGCOPY\n\xff\r\n\0")
    payload.extend(struct.pack(">ii", 0, 0))
    for row in rows:
        payload.extend(struct.pack(">h", len(row)))
        for field in row:
            if field is None:
                payload.extend(struct.pack(">i", -1))
            else:
                payload.extend(struct.pack(">i", len(field)))
                payload.extend(field)
    payload.extend(struct.pack(">h", -1))
    return bytes(payload)


@pytest.mark.parametrize(
    ("kind", "input_copy_rows"),
    [
        (
            "price_set_atom_memberships_v3",
            [[struct.pack(">q", 0), struct.pack(">q", 1)]],
        ),
        (
            "price_atoms_v3",
            [
                [
                    struct.pack(">q", 0),
                    b"125.5",
                    struct.pack(">q", 1),
                    None,
                    struct.pack(">q", 2),
                    None,
                    None,
                    None,
                    None,
                ]
            ],
        ),
    ],
)
def test_release_scanner_exposes_only_strict_v3_price_streams(kind, input_copy_rows):
    conversion_process = subprocess.run(
        [
            str(_built_scanner_binary()),
            "--serving-binary-copy-from-key-copy-stdio",
            kind,
            "24",
        ],
        input=_pg_binary_copy_rows(input_copy_rows),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=True,
        timeout=30,
    )

    output_rows = _read_pg_binary_rows(conversion_process.stdout, 10)
    assert len(output_rows) == 1
    assert output_rows[0][2] == kind.encode("ascii")
    summary_line = next(
        line
        for line in conversion_process.stderr.splitlines()
        if line.startswith(b"PTG2_SERVING_BINARY_COPY\t")
    )
    summary = json.loads(summary_line.split(b"\t", 1)[1])
    assert summary["artifact_kind"] == kind
    assert summary["atom_key_bits"] == 24
    assert summary["target_copy_format"] == "postgres_binary_shared_blocks"

    rejected = subprocess.run(
        [
            str(_built_scanner_binary()),
            "--serving-binary-copy-from-key-copy-stdio",
            "by_code",
            "24",
        ],
        input=b"",
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
        timeout=30,
    )
    assert rejected.returncode != 0


def _read_uvarint(payload: bytes, offset: int) -> tuple[int, int]:
    value = 0
    shift = 0
    while True:
        byte = payload[offset]
        offset += 1
        value |= (byte & 0x7F) << shift
        if byte & 0x80 == 0:
            return value, offset
        shift += 7


def _decode_by_code_groups(
    encoded_group_bytes: bytes, entry_count: int
) -> list[tuple[int, list[int], list[int]]]:
    assert encoded_group_bytes[0] == 2
    source_count, offset = _read_uvarint(encoded_group_bytes, 1)
    source_bits = encoded_group_bytes[offset]
    offset += 1
    provider_set_key = 0
    groups = []
    for _entry_index in range(entry_count):
        provider_delta, offset = _read_uvarint(encoded_group_bytes, offset)
        provider_set_key += provider_delta
        price_count, offset = _read_uvarint(encoded_group_bytes, offset)
        price_keys = []
        for _price_index in range(price_count):
            price_key, offset = _read_uvarint(encoded_group_bytes, offset)
            price_keys.append(price_key)
        source_bytes = (price_count * source_bits + 7) // 8
        packed_sources = encoded_group_bytes[offset : offset + source_bytes]
        offset += source_bytes
        if source_bits == 0:
            source_keys = [0] * price_count
        else:
            packed_value = int.from_bytes(packed_sources, "little")
            source_mask = (1 << source_bits) - 1
            source_keys = [
                (packed_value >> (index * source_bits)) & source_mask
                for index in range(price_count)
            ]
        assert all(source_key < source_count for source_key in source_keys)
        groups.append((provider_set_key, price_keys, source_keys))
    assert offset == len(encoded_group_bytes)
    return groups


def _fixture_payload(
    *,
    provider_references_first: bool,
    multiple_prices: bool = False,
    duplicate_first_price: bool = False,
    repeated_rate_occurrences: bool = False,
) -> dict:
    """Support the fixture payload test fixture."""
    provider_references = [
        {
            "provider_group_id": 1,
            "provider_groups": [
                {
                    "npi": [1234567890, 1234567891],
                    "tin": {"type": "ein", "value": "12-3456789"},
                }
            ],
        }
    ]
    if repeated_rate_occurrences:
        provider_references.append(
            {
                "provider_group_id": 2,
                "provider_groups": [
                    {
                        "npi": [1234567892],
                        "tin": {"type": "ein", "value": "98-7654321"},
                    }
                ],
            }
        )
    in_network_entries = [
        {
            "billing_code_type": "CPT",
            "billing_code_type_version": "2026",
            "billing_code": "99213",
            "negotiation_arrangement": " fFs ",
            "negotiated_rates": [
                {
                    "provider_references": [1],
                    "negotiated_prices": [
                        {
                            "negotiated_type": "negotiated",
                            "negotiated_rate": 125.5,
                            "service_code": ["11"],
                            "billing_class": "professional",
                        }
                    ],
                }
            ],
        }
    ]
    if duplicate_first_price:
        prices = in_network_entries[0]["negotiated_rates"][0]["negotiated_prices"]
        prices.append(dict(prices[0]))
    if repeated_rate_occurrences:
        first_rate = in_network_entries[0]["negotiated_rates"][0]
        in_network_entries[0]["negotiated_rates"].extend(
            [
                json.loads(json.dumps(first_rate)),
                {
                    **json.loads(json.dumps(first_rate)),
                    "provider_references": [2],
                },
            ]
        )
    if multiple_prices:
        in_network_entries.append(
            {
                "billing_code_type": "CPT",
                "billing_code_type_version": "2026",
                "billing_code": "99214",
                "negotiation_arrangement": " fFs ",
                "negotiated_rates": [
                    {
                        "provider_references": [1],
                        "negotiated_prices": [
                            {
                                "negotiated_type": "negotiated",
                                "negotiated_rate": 250,
                                "service_code": ["11"],
                                "billing_class": "professional",
                            }
                        ],
                    }
                ],
            }
        )
    if provider_references_first:
        return {
            "provider_references": provider_references,
            "in_network": in_network_entries,
        }
    return {
        "in_network": in_network_entries,
        "provider_references": provider_references,
    }


def _single_frame(frames: list[tuple[str, dict]], record_kind: str) -> dict:
    matches = [payload for kind, payload in frames if kind == record_kind]
    assert len(matches) == 1
    return matches[0]


def _load_isolated_shared_blocks():
    root = Path(__file__).resolve().parents[1]
    process_package = types.ModuleType("process")
    process_package.__path__ = [str(root / "process")]
    ptg_parts_package = types.ModuleType("process.ptg_parts")
    ptg_parts_package.__path__ = [str(root / "process" / "ptg_parts")]
    db_tables = types.ModuleType("process.ptg_parts.db_tables")
    db_tables._quote_ident = lambda value: str(value)
    module_name = "isolated_ptg2_shared_blocks"
    module_path = root / "process" / "ptg_parts" / "ptg2_shared_blocks.py"
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    replacement_modules_by_name = {
        "process": process_package,
        "process.ptg_parts": ptg_parts_package,
        "process.ptg_parts.db_tables": db_tables,
        module_name: module,
    }
    previous_modules_by_name = {
        name: sys.modules.get(name) for name in replacement_modules_by_name
    }
    try:
        sys.modules.update(replacement_modules_by_name)
        spec.loader.exec_module(module)
    finally:
        for name, prior_module in previous_modules_by_name.items():
            if prior_module is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = prior_module
    return module


def _run_scanner(
    scanner_binary: Path,
    tmp_path: Path,
    label: str,
    *,
    arch: str,
    provider_references_first: bool,
    grouped: bool,
    multiple_prices: bool = False,
    duplicate_first_price: bool = False,
    repeated_rate_occurrences: bool = False,
    fixture_payload: dict | None = None,
    top_level_byte_scan: bool = True,
) -> dict:
    """Support the run scanner test fixture."""
    run_directory = tmp_path / label
    run_directory.mkdir()
    artifact = run_directory / "input.json"
    # Keep the default scanner parity fixture one-record wide; the PostgreSQL
    # publication smoke opts into multiple dense price keys.
    source_document = (
        fixture_payload
        if fixture_payload is not None
        else _fixture_payload(
            provider_references_first=provider_references_first,
            multiple_prices=multiple_prices,
            duplicate_first_price=duplicate_first_price,
            repeated_rate_occurrences=repeated_rate_occurrences,
        )
    )
    artifact.write_text(
        json.dumps(source_document, separators=(",", ":")),
        encoding="utf-8",
    )
    lean_copy_path = run_directory / "manifest-lean.copy"
    compact_copy_path = run_directory / "compact.copy"
    price_atom_copy_path = run_directory / "manifest-price-atom.copy"
    price_set_atom_copy_path = run_directory / "manifest-price-set-atom.copy"
    provider_group_member_copy_path = run_directory / "provider-group-member.copy"
    provider_forward_path = run_directory / "provider-forward.sidecar"
    provider_inverted_path = run_directory / "provider-inverted.sidecar"
    serving_run_directory = run_directory / "serving-runs"
    scanner_environment_map = dict(os.environ)
    for output_env in (
        *_SUPPORT_MODULE.COPY_ENV_BY_KIND.values(),
        *_SUPPORT_MODULE.SIDECAR_ENV_BY_KIND.values(),
        "HLTHPRT_PTG2_V3_SERVING_RUN_DIR",
    ):
        scanner_environment_map.pop(output_env, None)
    scanner_environment_map.update(
        {
            "HLTHPRT_PTG2_SNAPSHOT_ARCH": arch,
            "HLTHPRT_PTG2_RAW_SOURCE_SHA256": hashlib.sha256(
                artifact.read_bytes()
            ).hexdigest(),
            "HLTHPRT_PTG2_V3_COVERAGE_SCOPE_ID": (b"\xcc" * 32).hex(),
            "HLTHPRT_PTG2_COMPACT_SNAPSHOT_ID": "snapshot-v3-runs",
            "HLTHPRT_PTG2_COMPACT_PLAN_ID": "plan-v3-runs",
            "HLTHPRT_PTG2_COMPACT_PLAN_MONTH_ID": "plan-month-v3-runs",
            "HLTHPRT_PTG2_COMPACT_SOURCE_TRACE_SET_HASH": "trace-v3-runs",
            "HLTHPRT_PTG2_MANIFEST_ONLY": "true",
            "HLTHPRT_PTG2_COMPACT_SERVING_COPY_PATH": str(compact_copy_path),
            "HLTHPRT_PTG2_MANIFEST_LEAN_SERVING_COPY_PATH": str(lean_copy_path),
            "HLTHPRT_PTG2_MANIFEST_PRICE_ATOM_COPY_PATH": str(price_atom_copy_path),
            "HLTHPRT_PTG2_MANIFEST_PRICE_SET_ATOM_COPY_PATH": str(
                price_set_atom_copy_path
            ),
            "HLTHPRT_PTG2_MANIFEST_PROVIDER_GROUP_MEMBER_COPY_PATH": str(
                provider_group_member_copy_path
            ),
            "HLTHPRT_PTG2_MANIFEST_PROVIDER_FORWARD_SIDECAR_PATH": str(
                provider_forward_path
            ),
            "HLTHPRT_PTG2_MANIFEST_PROVIDER_INVERTED_SIDECAR_PATH": str(
                provider_inverted_path
            ),
            "HLTHPRT_PTG2_V3_SERVING_RUN_DIR": str(serving_run_directory),
            "HLTHPRT_PTG2_V3_SERVING_RUN_PARTITIONS": "4",
            "HLTHPRT_PTG2_V3_SERVING_RUN_PARTITION_BUFFER_BYTES": "52",
            "HLTHPRT_PTG2_RUST_WORKERS": "2",
            "HLTHPRT_PTG2_RUST_WORK_QUEUE": "2",
            "HLTHPRT_PTG2_RUST_EVENT_QUEUE": "8",
            "HLTHPRT_PTG2_RUST_SPLIT_NEGOTIATED_RATES": "1",
            "HLTHPRT_PTG2_RUST_TOP_LEVEL_BYTE_SCAN": (
                "true" if top_level_byte_scan else "false"
            ),
            "HLTHPRT_PTG2_RUST_PROVIDER_REFS_IN_WORKERS": "true",
            "HLTHPRT_PTG2_RUST_PROVIDER_REF_WORKERS": "2",
            "HLTHPRT_PTG2_RUST_GROUP_NEGOTIATED_RATE_CHUNKS": "true" if grouped else "false",
            "HLTHPRT_PTG2_RUST_RAPIDGZIP_ENABLED": "false",
        }
    )
    completed = subprocess.run(
        [str(scanner_binary), "--compact-serving", str(artifact)],
        check=True,
        env=scanner_environment_map,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=120,
    )
    frames = _parse_scanner_frames(completed.stdout)
    partition_frames = [
        frame_payload
        for kind, frame_payload in frames
        if kind == "v3_serving_run_partition_file"
    ]
    code_dictionary_frames = [
        frame_payload
        for kind, frame_payload in frames
        if kind == "v3_serving_code_dictionary_file"
    ]
    price_atom_frames = [
        frame_payload
        for kind, frame_payload in frames
        if kind == "manifest_price_atom_copy_file"
    ]
    price_set_atom_frames = [
        frame_payload
        for kind, frame_payload in frames
        if kind == "manifest_price_set_atom_copy_file"
    ]
    provider_group_member_frames = [
        frame_payload
        for kind, frame_payload in frames
        if kind == "manifest_provider_group_member_copy_file"
    ]
    partition_bytes = b"".join(
        Path(frame["path"]).read_bytes()
        for frame in sorted(partition_frames, key=lambda frame: (frame["partition"], frame["path"]))
    )
    return {
        "artifact": artifact,
        "frames": frames,
        "compact_copy_path": compact_copy_path,
        "lean_copy_path": lean_copy_path,
        "price_atom_copy_path": price_atom_copy_path,
        "price_set_atom_copy_path": price_set_atom_copy_path,
        "provider_group_member_copy_path": provider_group_member_copy_path,
        "provider_forward_path": provider_forward_path,
        "provider_inverted_path": provider_inverted_path,
        "partition_frames": partition_frames,
        "code_dictionary_frames": code_dictionary_frames,
        "price_atom_frames": price_atom_frames,
        "price_set_atom_frames": price_set_atom_frames,
        "provider_group_member_frames": provider_group_member_frames,
        "partition_bytes": partition_bytes,
    }


def _malformed_provider_identifier_payload(
    *, provider_references_first: bool
) -> dict:
    source_document = _fixture_payload(
        provider_references_first=provider_references_first
    )
    source_document["provider_references"][0]["provider_groups"][0]["npi"] = [
        1234567890,
        123456789,
        123456789,
    ]
    source_document["in_network"][0]["negotiated_rates"].append(
        {
            "provider_groups": [
                {
                    "npi": [1234567891, 123456787],
                    "tin": {"type": "ein", "value": "12-3456789"},
                }
            ],
            "negotiated_prices": [
                {
                    "negotiated_type": "negotiated",
                    "negotiated_rate": 126,
                    "service_code": ["11"],
                    "billing_class": "professional",
                }
            ],
        }
    )
    return source_document


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
        {123456787: 1, 123456789: 2}
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

    baseline = scanner_runs_by_mode["worker_ungrouped"]["partition_bytes"]
    assert len(baseline) == _SERVING_RECORD.size
    assert scanner_runs_by_mode["late_reordered"]["partition_bytes"] == baseline
    assert _SERVING_RECORD.unpack(baseline)[3] == 2

    assert _single_frame(
        scanner_runs_by_mode["worker_ungrouped"]["frames"], "scanner_config"
    )["execution_mode"] == ("parallel_top_level_bytes")
    assert (
        _single_frame(
            scanner_runs_by_mode["late_reordered"]["frames"], "scanner_config"
        )["execution_mode"]
        == "parallel_top_level_bytes_plain_range_reorder"
    )
    late_config = _single_frame(
        scanner_runs_by_mode["late_reordered"]["frames"], "scanner_config"
    )
    assert late_config["provider_reference_order"] == "after_in_network"
    assert late_config["plain_range_reorder"] is True
    assert late_config["plain_provider_range_bytes"] > 0
    assert late_config["plain_in_network_range_bytes"] > 0
    assert late_config["plain_in_network_object_count"] == 1
    assert late_config["order_probe_partial_pass"] is True

    for run in scanner_runs_by_mode.values():
        frame_kinds = {kind for kind, _payload in run["frames"]}
        assert frame_kinds - {"dedupe_summary"} == (
            _STRICT_SCANNER_FRAME_KINDS - {"dedupe_summary"}
        )
        source_digest = hashlib.sha256(run["artifact"].read_bytes()).hexdigest()
        witness_entry = _single_frame(
            run["frames"],
            "source_audit_witness_file",
        )
        witness_payload, metadata = build_persisted_source_witness(
            [witness_entry],
            expected_raw_source_sha256=[source_digest],
        )
        loaded = decode_persisted_source_witness(
            witness_payload,
            expected_raw_source_sha256=[source_digest],
            expected_metadata=metadata,
        )
        assert len(loaded.occurrence_records) == 2
        assert len(loaded.provider_records) == 1
        assert not run["compact_copy_path"].exists()
        assert not run["lean_copy_path"].exists()
        assert not any(kind == "manifest_lean_serving_copy_file" for kind, _payload in run["frames"])
        assert not any(
            kind in {"procedure", "provider_set", "serving_rate_compact"}
            for kind, _payload in run["frames"]
        )
        assert run["provider_group_member_frames"]
        assert sum(
            frame["row_count"] for frame in run["provider_group_member_frames"]
        ) == 2
        assert all(
            Path(frame["path"]).exists()
            for frame in run["provider_group_member_frames"]
        )
        assert run["provider_forward_path"].exists()
        assert run["provider_inverted_path"].exists()
        partition_frame = run["partition_frames"][0]
        assert partition_frame["format"] == "ptg2_v3_serving_run"
        assert partition_frame["version"] == 1
        assert partition_frame["partition_count"] == 4
        assert partition_frame["row_count"] == 1
        assert partition_frame["bytes"] == _SERVING_RECORD.size
        assert Path(partition_frame["path"]).name.endswith(".ready")
        summary = _single_frame(run["frames"], "scanner_summary")
        config = _single_frame(run["frames"], "scanner_config")
        assert config["snapshot_arch"] == "postgres_binary_v3"
        assert config["storage_generation"] == "shared_blocks_v3"
        assert config["serving_row_semantics"] == "source_multiset_v1"
        assert config["group_negotiated_rate_chunks"] is False
        assert summary["serving_run_files"] == 1
        assert summary["serving_run_rows"] == 1
        assert summary["serving_run_bytes"] == _SERVING_RECORD.size
        assert len(run["code_dictionary_frames"]) == 1
        assert run["code_dictionary_frames"][0]["format"] == (
            "ptg2_v3_serving_code_dictionary"
        )


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


def test_python_bridge_collects_partition_paths_in_scanner_summary(tmp_path, monkeypatch):
    """Verify python bridge collects partition paths in scanner summary."""
    scanner_binary = _built_scanner_binary()
    artifact = tmp_path / "bridge-input.json"
    artifact.write_text(
        json.dumps(_fixture_payload(provider_references_first=True), separators=(",", ":")),
        encoding="utf-8",
    )
    rust_scanner = _load_isolated_rust_scanner(monkeypatch)
    monkeypatch.setattr(rust_scanner, "_ptg2_rust_scanner_binary", lambda: scanner_binary)
    monkeypatch.setenv("HLTHPRT_PTG2_SNAPSHOT_ARCH", "postgres_binary_v3")
    monkeypatch.setenv("HLTHPRT_PTG2_RUST_TOP_LEVEL_BYTE_SCAN", "true")
    lean_copy_path = tmp_path / "bridge-manifest-lean.copy"
    serving_run_directory = tmp_path / "bridge-serving-runs"

    scanner_frames = list(
        rust_scanner._iter_compact_serving_records_rust(
            artifact,
            raw_source_sha256=hashlib.sha256(artifact.read_bytes()).hexdigest(),
            snapshot_id="snapshot-bridge-v3",
            plan_id="plan-v3-runs",
            plan_month_id="plan-month-bridge-v3",
            coverage_scope_id=(b"\xcc" * 32).hex(),
            source_trace_set_hash="trace-bridge-v3",
            manifest_lean_serving_copy_path=lean_copy_path,
            v3_serving_run_directory=serving_run_directory,
            manifest_only=True,
        )
    )

    summary = _single_frame(scanner_frames, "scanner_summary")
    config = _single_frame(scanner_frames, "scanner_config")
    assert summary["serving_run_partition_files"]
    assert summary["serving_run_partition_files"] == [
        {
            key: frame_payload[key]
            for key in (
                "path",
                "partition",
                "partition_count",
                "row_count",
                "bytes",
                "format",
                "version",
                "sha256",
            )
        }
        for kind, frame_payload in scanner_frames
        if kind == "v3_serving_run_partition_file"
    ]
    assert summary["serving_run_code_dictionary_files"] == [
        {
            key: frame_payload[key]
            for key in ("path", "row_count", "bytes", "format", "version", "sha256")
        }
        for kind, frame_payload in scanner_frames
        if kind == "v3_serving_code_dictionary_file"
    ]
    for entry in (
        summary["serving_run_partition_files"]
        + summary["serving_run_code_dictionary_files"]
    ):
        assert entry["sha256"] == hashlib.sha256(
            Path(entry["path"]).read_bytes()
        ).hexdigest()
    source_identity_map = {
        "source_type": "in_network",
        "identity_kind": "logical_json_sha256_v1",
        "identity_sha256": "a" * 64,
    }
    contracted = attach_v3_source_run_contract(
        summary["serving_run_partition_files"],
        source_identity=source_identity_map,
        scanner_summary=summary,
        scanner_config=config,
    )
    contract = contracted[0]["source_run_contract"]
    assert contract["file_count"] == summary["serving_run_files"]
    assert contract["row_count"] == summary["serving_run_rows"]
    assert contract["byte_count"] == summary["serving_run_bytes"]
    assert len(contract["partition_rows"]) == config["serving_run_partition_count"]
    assert sum(contract["partition_rows"]) == summary["serving_run_rows"]
    contracted_dictionaries = attach_v3_dictionary_contract(
        summary["serving_run_code_dictionary_files"],
        source_identity=source_identity_map,
        source_run_contract_sha256=contracted[0]["source_run_contract_sha256"],
        scanner_summary=summary,
    )
    dictionary_contract = contracted_dictionaries[0][
        "code_dictionary_source_contract"
    ]
    assert dictionary_contract["file_count"] == summary[
        "serving_code_dictionary_files"
    ]
    assert dictionary_contract["row_count"] == summary[
        "serving_code_dictionary_rows"
    ]
    assert dictionary_contract["byte_count"] == summary[
        "serving_code_dictionary_bytes"
    ]
    assert dictionary_contract["files"] == sorted(
        dictionary_contract["files"],
        key=lambda value: (value["sha256"], value["row_count"], value["bytes"]),
    )
    assert not lean_copy_path.exists()


def test_direct_v3_finalizer_cli_emits_shared_block_staging_copy(tmp_path):
    """Verify direct v3 finalizer cli emits shared block staging copy."""
    scanner_binary = _built_scanner_binary()
    scan = _run_scanner(
        scanner_binary,
        tmp_path,
        "finalizer-source",
        arch="postgres_binary_v3",
        provider_references_first=True,
        grouped=False,
        repeated_rate_occurrences=True,
    )
    manifest_path = tmp_path / "scanner-summary.json"
    source_identity_dict = {
        "source_type": "in_network",
        "identity_kind": "logical_json_sha256_v1",
        "identity_sha256": "d" * 64,
    }
    scanner_summary = _single_frame(scan["frames"], "scanner_summary")
    scanner_config = _single_frame(scan["frames"], "scanner_config")
    serving_run_entries = attach_v3_source_run_contract(
        scan["partition_frames"],
        source_identity=source_identity_dict,
        scanner_summary=scanner_summary,
        scanner_config=scanner_config,
    )
    code_dictionary_entries = attach_v3_dictionary_contract(
        scan["code_dictionary_frames"],
        source_identity=source_identity_dict,
        source_run_contract_sha256=serving_run_entries[0][
            "source_run_contract_sha256"
        ],
        scanner_summary=scanner_summary,
    )
    write_v3_finalizer_input_manifest(
        manifest_path,
        serving_run_entries=serving_run_entries,
        code_dictionary_entries=code_dictionary_entries,
        expected_source_identities=[source_identity_dict],
    )
    membership_input = tmp_path / "future-memberships.copy"
    atom_input = tmp_path / "future-atoms.copy"
    price_key_map_input = tmp_path / "price-key-map.copy"
    membership_input.write_bytes(b"")
    atom_input.write_bytes(b"")
    assert len(scan["partition_bytes"]) % _SERVING_RECORD.size == 0
    price_set_ids = sorted(
        {
            scan["partition_bytes"][offset + 32 : offset + 48]
            for offset in range(0, len(scan["partition_bytes"]), _SERVING_RECORD.size)
        }
    )
    price_key_map_input.write_bytes(
        _pg_binary_copy_rows(
            [
                [price_set_id, struct.pack(">q", price_key)]
                for price_key, price_set_id in enumerate(price_set_ids)
            ]
        )
    )
    output_directory = tmp_path / "finalized"
    finalizer_environment_map = {
        **os.environ,
        "HLTHPRT_PTG2_SERVING_BINARY_PAYLOAD_COMPRESSION": "none",
        "HLTHPRT_PTG2_SERVING_BINARY_BLOCK_BYTES": "65536",
    }
    completed = subprocess.run(
        [
            str(scanner_binary),
            "--finalize-v3-runs",
            str(output_directory),
            *_v3_finalizer_test_resource_args(),
            "--price-key-map-input",
            str(price_key_map_input),
            "--price-membership-input",
            str(membership_input),
            "--price-atom-input",
            str(atom_input),
            str(manifest_path),
        ],
        check=True,
        env=finalizer_environment_map,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=120,
    )
    frames = _parse_scanner_frames(completed.stdout)
    summary = _single_frame(frames, "v3_finalizer_summary")

    assert summary["format"] == "ptg2_v3_direct_finalizer_v3"
    assert summary["storage_generation"] == "shared_blocks_v3"
    assert summary["source_count"] == 1
    assert summary["source"]["record_count"] == 3
    assert summary["preservation"] == {
        "source_records": 3,
        "sorted_records": 3,
        "staged_records": 3,
        "assigned_records": 3,
        "encoded_records": 3,
        "distinct_serving_records": 2,
        "duplicate_serving_records": 1,
        "source_equals_sorted": True,
        "sorted_equals_staged": True,
        "staged_equals_assigned": True,
        "assigned_equals_encoded": True,
        "all_source_occurrences_preserved": True,
    }
    assert summary["deferred_atom_inputs"]["fused"] is False
    assert set(path.name for path in output_directory.iterdir()) == {
        "audit_candidates.bin",
        "shared_serving_blocks.copy",
        "shared_price_dictionary_blocks.copy",
        "code_dictionary.copy",
        "provider_set_dictionary.copy",
        "summary.json",
    }
    audit_candidate_bytes = (output_directory / "audit_candidates.bin").read_bytes()
    assert len(audit_candidate_bytes) == 3 * _AUDIT_CANDIDATE_RECORD.size
    audit_candidate_rows = [
        _AUDIT_CANDIDATE_RECORD.unpack(
            audit_candidate_bytes[offset : offset + _AUDIT_CANDIDATE_RECORD.size]
        )
        for offset in range(0, len(audit_candidate_bytes), _AUDIT_CANDIDATE_RECORD.size)
    ]
    assert sorted(Counter(audit_candidate_rows).values()) == [1, 2]
    assert {audit_candidate_row[3] for audit_candidate_row in audit_candidate_rows} == {
        0
    }
    assert sorted(
        {audit_candidate_row[4] for audit_candidate_row in audit_candidate_rows}
    ) == [1, 2]
    assert summary["audit_candidates"] == {
        "path": "audit_candidates.bin",
        "record_format": "ptg2_v3_audit_candidates_v2",
        "format_version": 2,
        "record_bytes": 20,
        "fields": [
            "code_key",
            "provider_set_key",
            "price_key",
            "source_key",
            "provider_count",
        ],
        "source_key_included": True,
        "source_count": 1,
        "source_key_bits": 0,
        "record_counts_by_source": {"0": 3},
        "row_count": 3,
        "maximum_rows": 4096,
        "selection_method": "equal_interval_assigned_rows_v1",
        "source_row_count": 3,
        "row_digest": hashlib.sha256(audit_candidate_bytes).hexdigest(),
    }
    code_rows = _read_pg_binary_rows(
        (output_directory / "code_dictionary.copy").read_bytes(), 10
    )
    assert len(code_rows) == 1
    assert struct.unpack(">i", code_rows[0][0])[0] == 0
    assert len(code_rows[0][1]) == 16
    assert code_rows[0][2] == b"\xcc" * 32
    assert code_rows[0][3:6] == [b"CPT", b"99213", b"FFS"]
    assert code_rows[0][6:9] == [b"2026", None, None]
    assert struct.unpack(">q", code_rows[0][9])[0] == 3
    assert summary["dictionaries"]["code"]["fields"] == [
        "code_key",
        "code_global_id_128",
        "coverage_scope_id",
        "reported_code_system",
        "reported_code",
        "negotiation_arrangement",
        "billing_code_type_version",
        "source_name",
        "source_description",
        "rate_count",
    ]
    assert summary["dictionaries"]["code"]["rate_count_total"] == 3
    assert summary["dense_keys"]["price"]["ordering"] == (
        "minimum_negotiated_rate_then_global_id_128_v1"
    )
    assert summary["blocks"]["serving"]["fields"] == [
        "block_hash",
        "format_version",
        "object_kind",
        "block_key",
        "fragment_no",
        "entry_count",
        "codec",
        "raw_byte_count",
        "stored_byte_count",
        "payload",
    ]
    assert summary["blocks"]["serving"]["snapshot_key_included"] is False
    for section_name, file_name in (
        ("serving", "shared_serving_blocks.copy"),
        ("price_dictionary", "shared_price_dictionary_blocks.copy"),
    ):
        copy_bytes = (output_directory / file_name).read_bytes()
        assert summary["blocks"][section_name]["copy_bytes"] == len(copy_bytes)
        assert summary["blocks"][section_name]["copy_sha256"] == hashlib.sha256(
            copy_bytes
        ).hexdigest()

    shared_rows = _read_pg_binary_rows(
        (output_directory / "shared_serving_blocks.copy").read_bytes(), 10
    )
    assert shared_rows
    shard_rows = [
        shared_block_row
        for shared_block_row in shared_rows
        if shared_block_row[2] == b"by_code_provider_shard_v1"
    ]
    assert len(shard_rows) == 1
    shard_row = shard_rows[0]
    assert shard_row[6] == b"none"
    groups = _decode_by_code_groups(
        shard_row[9], struct.unpack(">q", shard_row[5])[0]
    )
    assert sorted(len(price_keys) for _provider_key, price_keys, _sources in groups) == [1, 2]
    assert all(
        price_key == 0
        for _provider_key, price_keys, _sources in groups
        for price_key in price_keys
    )
    assert all(
        source_key == 0
        for _provider_key, _prices, source_keys in groups
        for source_key in source_keys
    )
    first = shared_rows[0]
    assert struct.unpack(">h", first[1])[0] == 2
    shared_block_hash = _load_isolated_shared_blocks().shared_block_hash

    assert first[0] == shared_block_hash(
        format_version=struct.unpack(">h", first[1])[0],
        object_kind=first[2].decode("utf-8"),
        codec=first[6].decode("ascii"),
        payload=first[9],
    )

    tampered_partition_path = Path(scan["partition_frames"][0]["path"])
    tampered_payload = bytearray(tampered_partition_path.read_bytes())
    tampered_payload[-1] ^= 1
    tampered_partition_path.write_bytes(tampered_payload)
    tampered = subprocess.run(
        [
            str(scanner_binary),
            "--finalize-v3-runs",
            str(tmp_path / "tampered-finalized"),
            *_v3_finalizer_test_resource_args(),
            "--price-key-map-input",
            str(price_key_map_input),
            "--price-membership-input",
            str(membership_input),
            "--price-atom-input",
            str(atom_input),
            str(manifest_path),
        ],
        check=False,
        env=finalizer_environment_map,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=120,
    )
    assert tampered.returncode != 0
    assert b"content digest mismatch" in tampered.stderr
