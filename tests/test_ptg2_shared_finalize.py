from __future__ import annotations

import hashlib
import json

import pytest

from process.ptg_parts.ptg2_shared_finalize import (
    attach_v3_source_run_contract,
    parse_v3_finalizer_stdout,
    write_v3_finalizer_input_manifest,
)


def _entry(path, **values):
    path.write_bytes(b"x" * int(values.get("bytes") or 1))
    return {"path": str(path), **values}


def _identity(value):
    return {
        "source_type": "in_network",
        "identity_kind": "logical_json_sha256_v1",
        "identity_sha256": value * 64,
    }


def _contracted_entries(entries, identity, *, partition_count):
    return attach_v3_source_run_contract(
        entries,
        source_identity=identity,
        scanner_summary={
            "serving_run_files": len(entries),
            "serving_run_rows": sum(entry["row_count"] for entry in entries),
            "serving_run_bytes": sum(entry["bytes"] for entry in entries),
        },
        scanner_config={"serving_run_partition_count": partition_count},
    )


def _summary_payload():
    return {
        "format": "ptg2_v3_direct_finalizer_v3",
        "storage_generation": "shared_blocks_v3",
        "cold_lookup_contract": "ptg_v3_cold_v2",
        "shared_block_layout": "dense_shared_blocks_v3",
        "source_count": 1,
        "output_directory": "/tmp/out",
        "price_key_map": {
            "copy_format": "postgresql_binary_copy",
            "row_count": 1,
            "dense_price_ordering": (
                "minimum_negotiated_rate_then_global_id_128_v1"
            ),
            "keys_unique_dense_contiguous": True,
            "source_ids_exact_match": True,
        },
        "dense_keys": {
            "price": {
                "count": 1,
                "ordering": "minimum_negotiated_rate_then_global_id_128_v1",
            }
        },
        "blocks": {
            "serving": {
                "artifact_record_counts": {
                    "by_code_provider_shard_v1": 1,
                    "by_code_price_page_v4": 1,
                    "provider_set_count_dictionary": 1,
                    "provider_set_codes_v3": 1,
                    "provider_set_page_v3_s2": 1,
                }
            },
            "price_dictionary": {
                "artifact_record_counts": {"by_code_price_dictionary": 1}
            },
        },
    }


def test_finalizer_input_manifest_is_explicit_and_path_validated(tmp_path):
    manifest = write_v3_finalizer_input_manifest(
        tmp_path / "input.json",
        serving_run_entries=_contracted_entries(
            [
                _entry(
                    tmp_path / "run.ready",
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
        ),
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

    payload = json.loads(manifest.read_text(encoding="ascii"))
    assert payload["serving_run_partition_files"][0]["path"].endswith("run.ready")
    assert payload["serving_run_partition_files"][0]["source_key"] == 0
    assert payload["serving_run_partition_files"][0]["source_count"] == 1
    assert payload["source_count"] == 1
    assert payload["expected_serving_run_files"] == 1
    assert payload["expected_serving_run_rows"] == 1
    assert payload["expected_serving_run_bytes"] == 52
    assert payload["source_run_contracts"][0]["partition_rows"] == [1]
    assert len(payload["source_run_contract_set_sha256"]) == 64
    assert payload["serving_run_code_dictionary_files"][0]["path"].endswith(
        "codes.ready"
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


def test_identical_run_rows_from_distinct_artifacts_keep_distinct_source_keys(tmp_path):
    first = _contracted_entries(
        [
            _entry(
                tmp_path / "first.run",
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
                tmp_path / "second.run",
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
    assert (tmp_path / "first.run").read_bytes() == (tmp_path / "second.run").read_bytes()
    manifest = write_v3_finalizer_input_manifest(
        tmp_path / "two-sources.json",
        serving_run_entries=[second, first],
        code_dictionary_entries=[
            _entry(
                tmp_path / "codes.ready",
                row_count=1,
                bytes=64,
                format="ptg2_v3_serving_code_dictionary",
                version=4,
            )
        ],
        expected_source_identities=[_identity("b"), _identity("a")],
    )
    rows = json.loads(manifest.read_text(encoding="ascii"))[
        "serving_run_partition_files"
    ]

    assert [(row["path"], row["source_key"], row["source_count"]) for row in rows] == [
        (str((tmp_path / "second.run").resolve()), 1, 2),
        (str((tmp_path / "first.run").resolve()), 0, 2),
    ]


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
