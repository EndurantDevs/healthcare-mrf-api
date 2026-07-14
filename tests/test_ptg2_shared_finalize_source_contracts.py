from __future__ import annotations

import hashlib
import json

import pytest

from process.ptg_parts.ptg2_shared_finalize import (
    parse_v3_finalizer_stdout,
    write_v3_finalizer_input_manifest,
)
from tests.ptg2_shared_finalize_test_support import (
    _contracted_dictionary_entries,
    _contracted_entries,
    _entry,
    _identity,
    _summary_payload,
)


def test_finalizer_input_manifest_rejects_duplicate_paths(tmp_path):
    entry = _entry(
        tmp_path / "run.ready",
        row_count=1,
        bytes=52,
        partition=0,
        partition_count=1,
        format="ptg2_v3_serving_run",
        version=1,
    )
    with pytest.raises(RuntimeError, match="repeats path"):
        write_v3_finalizer_input_manifest(
            tmp_path / "input.json",
            serving_run_entries=[entry, entry],
            code_dictionary_entries=[
                _entry(
                    tmp_path / "codes.ready",
                    row_count=1,
                    bytes=64,
                    format="ptg2_v3_serving_code_dictionary",
                    version=4,
                )
            ],
            expected_source_identities=[_identity("a")],
        )


def test_finalizer_summary_frame_is_strict():
    payload = json.dumps(_summary_payload(), separators=(",", ":")).encode("ascii")
    frame = b"v3_finalizer_summary\t" + str(len(payload)).encode("ascii") + b"\n" + payload + b"\n"

    assert parse_v3_finalizer_stdout(frame)["output_directory"] == "/tmp/out"
    with pytest.raises(RuntimeError, match="trailing"):
        parse_v3_finalizer_stdout(frame + b"unexpected")


def _source_run_entry(tmp_path, label, identity_label):
    return _contracted_entries(
        [
            _entry(
                tmp_path / f"{label}.run",
                row_count=1,
                bytes=52,
                partition=0,
                partition_count=1,
                format="ptg2_v3_serving_run",
                version=1,
            )
        ],
        _identity(identity_label),
        partition_count=1,
    )[0]


def _source_dictionary_entry(tmp_path, label, identity_label, source_run_entry):
    return _contracted_dictionary_entries(
        [
            _entry(
                tmp_path / f"{label}-codes.ready",
                row_count=1,
                bytes=64,
                format="ptg2_v3_serving_code_dictionary",
                version=4,
            )
        ],
        _identity(identity_label),
        [source_run_entry],
    )[0]


def test_identical_run_rows_from_distinct_artifacts_keep_distinct_source_keys(tmp_path):
    """Keep physical source identity even when two run files have equal bytes."""
    first = _source_run_entry(tmp_path, "first", "a")
    second = _source_run_entry(tmp_path, "second", "b")
    assert (tmp_path / "first.run").read_bytes() == (tmp_path / "second.run").read_bytes()
    manifest = write_v3_finalizer_input_manifest(
        tmp_path / "two-sources.json",
        serving_run_entries=[second, first],
        code_dictionary_entries=[
            _source_dictionary_entry(tmp_path, "second", "b", second),
            _source_dictionary_entry(tmp_path, "first", "a", first),
        ],
        expected_source_identities=[_identity("b"), _identity("a")],
    )
    run_rows = json.loads(manifest.read_text(encoding="ascii"))[
        "serving_run_partition_files"
    ]

    assert [
        (run_row["path"], run_row["source_key"], run_row["source_count"])
        for run_row in run_rows
    ] == [
        (str((tmp_path / "second.run").resolve()), 1, 2),
        (str((tmp_path / "first.run").resolve()), 0, 2),
    ]
    dictionary_rows = json.loads(manifest.read_text(encoding="ascii"))[
        "serving_run_code_dictionary_files"
    ]
    assert [entry["source_key"] for entry in dictionary_rows] == [1, 0]
    dictionary_contracts = json.loads(manifest.read_text(encoding="ascii"))[
        "code_dictionary_source_contracts"
    ]
    assert [contract["source_key"] for contract in dictionary_contracts] == [0, 1]
    assert [
        contract["source_identity"]["identity_sha256"]
        for contract in dictionary_contracts
    ] == ["a" * 64, "b" * 64]


def test_finalizer_rejects_missing_nonempty_source_partition_file(tmp_path):
    identity = _identity("a")
    entries = _contracted_entries(
        [
            _entry(
                tmp_path / "part-0.ready",
                row_count=1,
                bytes=52,
                partition=0,
                partition_count=2,
                format="ptg2_v3_serving_run",
                version=1,
            ),
            _entry(
                tmp_path / "part-1.ready",
                row_count=1,
                bytes=52,
                partition=1,
                partition_count=2,
                format="ptg2_v3_serving_run",
                version=1,
            ),
        ],
        identity,
        partition_count=2,
    )

    with pytest.raises(RuntimeError, match="partition rows|aggregates|file digests"):
        write_v3_finalizer_input_manifest(
            tmp_path / "missing-nonempty.json",
            serving_run_entries=entries[:1],
            code_dictionary_entries=[
                _entry(
                    tmp_path / "codes.ready",
                    row_count=1,
                    bytes=64,
                    format="ptg2_v3_serving_code_dictionary",
                    version=4,
                )
            ],
            expected_source_identities=[identity],
        )


def test_finalizer_rejects_contract_missing_empty_partition_slot(tmp_path):
    identity = _identity("a")
    entries = _contracted_entries(
        [
            _entry(
                tmp_path / "part-0.ready",
                row_count=1,
                bytes=52,
                partition=0,
                partition_count=2,
                format="ptg2_v3_serving_run",
                version=1,
            )
        ],
        identity,
        partition_count=2,
    )
    contract = entries[0]["source_run_contract"]
    contract["partition_rows"] = [1]
    entries[0]["source_run_contract_sha256"] = hashlib.sha256(
        json.dumps(contract, sort_keys=True, separators=(",", ":")).encode("ascii")
    ).hexdigest()

    with pytest.raises(RuntimeError, match="incomplete partition coverage"):
        write_v3_finalizer_input_manifest(
            tmp_path / "missing-empty.json",
            serving_run_entries=entries,
            code_dictionary_entries=[
                _entry(
                    tmp_path / "codes.ready",
                    row_count=1,
                    bytes=64,
                    format="ptg2_v3_serving_code_dictionary",
                    version=4,
                )
            ],
            expected_source_identities=[identity],
        )


def test_finalizer_rejects_swapped_source_paths(tmp_path):
    first_path = tmp_path / "first.ready"
    second_path = tmp_path / "second.ready"
    first_path.write_bytes(b"a" * 52)
    second_path.write_bytes(b"b" * 52)
    first = _contracted_entries(
        [
            {
                "path": str(first_path),
                "row_count": 1,
                "bytes": 52,
                "partition": 0,
                "partition_count": 1,
                "format": "ptg2_v3_serving_run",
                "version": 1,
            }
        ],
        _identity("a"),
        partition_count=1,
    )[0]
    second = _contracted_entries(
        [
            {
                "path": str(second_path),
                "row_count": 1,
                "bytes": 52,
                "partition": 0,
                "partition_count": 1,
                "format": "ptg2_v3_serving_run",
                "version": 1,
            }
        ],
        _identity("b"),
        partition_count=1,
    )[0]
    first["path"], second["path"] = second["path"], first["path"]

    with pytest.raises(RuntimeError, match="content digest"):
        write_v3_finalizer_input_manifest(
            tmp_path / "swapped.json",
            serving_run_entries=[first, second],
            code_dictionary_entries=[
                _entry(
                    tmp_path / "codes.ready",
                    row_count=1,
                    bytes=64,
                    format="ptg2_v3_serving_code_dictionary",
                    version=4,
                )
            ],
            expected_source_identities=[_identity("a"), _identity("b")],
        )


def test_finalizer_rejects_swapped_source_identity_metadata(tmp_path):
    first = _contracted_entries(
        [
            _entry(
                tmp_path / "first.ready",
                row_count=1,
                bytes=52,
                partition=0,
                partition_count=1,
                format="ptg2_v3_serving_run",
                version=1,
            )
        ],
        _identity("a"),
        partition_count=1,
    )[0]
    second = _contracted_entries(
        [
            _entry(
                tmp_path / "second.ready",
                row_count=1,
                bytes=52,
                partition=0,
                partition_count=1,
                format="ptg2_v3_serving_run",
                version=1,
            )
        ],
        _identity("b"),
        partition_count=1,
    )[0]
    for field_name in ("source_type", "identity_kind", "identity_sha256"):
        first[field_name], second[field_name] = second[field_name], first[field_name]

    with pytest.raises(RuntimeError, match="bound to another physical source"):
        write_v3_finalizer_input_manifest(
            tmp_path / "swapped-identities.json",
            serving_run_entries=[first, second],
            code_dictionary_entries=[
                _entry(
                    tmp_path / "codes.ready",
                    row_count=1,
                    bytes=64,
                    format="ptg2_v3_serving_code_dictionary",
                    version=4,
                )
            ],
            expected_source_identities=[_identity("a"), _identity("b")],
        )


def test_finalizer_summary_rejects_legacy_object_markers():
    payload = _summary_payload()
    payload["blocks"]["serving"]["artifact_record_counts"] = {
        "by_code_grouped": 1,
        "by_code_page_v3_f2": 1,
        "provider_set_count_dictionary": 1,
        "provider_set_codes_v3": 1,
        "provider_set_page_v3_s1": 1,
    }
    encoded = json.dumps(payload, separators=(",", ":")).encode("ascii")
    frame = b"v3_finalizer_summary\t" + str(len(encoded)).encode() + b"\n" + encoded

    with pytest.raises(RuntimeError, match="object markers"):
        parse_v3_finalizer_stdout(frame)
