from __future__ import annotations

import hashlib
import json

from process.ptg_parts.ptg2_shared_finalize import (
    attach_v3_dictionary_contract,
    attach_v3_source_run_contract,
)


def _entry(path, **values):
    path.write_bytes(b"x" * int(values.get("bytes") or 1))
    return {"path": str(path), **values}


def _canonical_sha256(payload):
    encoded = json.dumps(
        payload,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


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


def _contracted_dictionary_entries(entries, identity, serving_entries, **summary):
    return attach_v3_dictionary_contract(
        entries,
        source_identity=identity,
        source_run_contract_sha256=serving_entries[0][
            "source_run_contract_sha256"
        ],
        scanner_summary={
            "serving_code_dictionary_files": summary.get("files", len(entries)),
            "serving_code_dictionary_rows": summary.get(
                "rows", sum(entry["row_count"] for entry in entries)
            ),
            "serving_code_dictionary_bytes": summary.get(
                "bytes", sum(entry["bytes"] for entry in entries)
            ),
        },
    )


def _one_source_contracted_inputs(tmp_path, *, dictionary_file_count=2):
    identity = _identity("a")
    serving_entries = _contracted_entries(
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
        identity,
        partition_count=1,
    )
    raw_dictionary_entries = [
        _entry(
            tmp_path / f"codes-{index}.ready",
            row_count=1,
            bytes=64,
            format="ptg2_v3_serving_code_dictionary",
            version=4,
        )
        for index in range(dictionary_file_count)
    ]
    dictionary_entries = _contracted_dictionary_entries(
        raw_dictionary_entries,
        identity,
        serving_entries,
    )
    return identity, serving_entries, dictionary_entries


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
