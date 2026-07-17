# See LICENSE.

from __future__ import annotations

import hashlib
import json
import random
import struct
import zlib

import pytest

from process.ptg_parts import ptg2_source_witness as witness
from process.ptg_parts import ptg2_source_witness_codec as witness_codec
from process.ptg_parts import ptg2_source_witness_store as witness_store


_U32 = struct.Struct(">I")
SOURCE_A = "11" * 32
SOURCE_B = "22" * 32


def _record_metadata_by_field(
    *,
    kind: str,
    priority: int,
    item_ordinal: int,
    raw_json: bytes,
    linked_provider_json: bytes | None,
) -> dict:
    is_occurrence = kind == "rate_occurrence"
    return {
        "contract": witness.PTG2_V3_SOURCE_WITNESS_RECORD_CONTRACT,
        "kind": kind,
        "priority": f"{priority:016x}",
        "tie_breaker": hashlib.sha256(
            f"{kind}:{item_ordinal}:{priority}".encode()
        ).hexdigest(),
        "coordinate": {
            "object_ordinal": 1,
            "rate_ordinal": item_ordinal,
            "price_ordinal": item_ordinal % 3 if is_occurrence else 0,
            "provider_ordinal": item_ordinal % 5 if is_occurrence else 0,
        },
        "raw_sha256": hashlib.sha256(raw_json).hexdigest(),
        "linked_provider_sha256": (
            hashlib.sha256(linked_provider_json).hexdigest()
            if linked_provider_json is not None
            else None
        ),
        "procedure": (
            {"billing_code_type": "CPT", "billing_code": "99213"}
            if is_occurrence
            else None
        ),
        "provider_evidence": (
            {
                "source_kind": "provider_reference",
                "provider_reference_id": "1",
                "provider_group_ordinal": 0,
                "npi_ordinal": 0,
            }
            if is_occurrence and linked_provider_json is not None
            else {
                "source_kind": "inline_provider_group",
                "provider_group_ordinal": 0,
                "npi_ordinal": 0,
            }
            if is_occurrence
            else None
        ),
        "expected": {
            "contract": (
                "ptg2_v3_source_rate_occurrence_expected_v2"
                if is_occurrence
                else "ptg2_v3_source_provider_expected_v2"
            ),
            **({"provider_group_id": str(item_ordinal)} if not is_occurrence else {}),
        },
    }


def _record(
    *,
    kind: str,
    priority: int,
    item_ordinal: int,
    raw_json: bytes,
    linked_provider_json: bytes | None = None,
) -> bytes:
    """Encode one scanner-shaped source witness fixture."""

    metadata_by_field = _record_metadata_by_field(
        kind=kind,
        priority=priority,
        item_ordinal=item_ordinal,
        raw_json=raw_json,
        linked_provider_json=linked_provider_json,
    )
    metadata_bytes = json.dumps(
        metadata_by_field,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    linked_provider = linked_provider_json or b""
    decoded = b"".join(
        (
            b"PTG2SWR2",
            _U32.pack(len(metadata_bytes)),
            metadata_bytes,
            _U32.pack(len(raw_json)),
            raw_json,
            _U32.pack(len(linked_provider)),
            linked_provider,
        )
    )
    return zlib.compress(decoded, level=1)


def _bundle(
    tmp_path,
    *,
    source_digest: str,
    name: str,
    compressed_records: list[bytes],
    occurrence_population_count: int,
    provider_population_count: int = 0,
):
    decoded_records = [
        witness.decode_record(compressed_record, source_digest)
        for compressed_record in compressed_records
    ]
    occurrence_count = sum(
        witness_record.kind == "rate_occurrence"
        for witness_record in decoded_records
    )
    provider_count = len(compressed_records) - occurrence_count
    header_by_field = {
        "contract": witness.PTG2_V3_SOURCE_WITNESS_CONTRACT,
        "format_version": 2,
        "selection_method": witness.PTG2_V3_SOURCE_WITNESS_SELECTION,
        "unqueryable_rate_policy": witness.PTG2_V3_SOURCE_WITNESS_UNQUERYABLE_POLICY,
        "raw_source_sha256": source_digest,
        "occurrence_target": witness.PTG2_V3_SOURCE_WITNESS_OCCURRENCE_TARGET,
        "total_target": witness.PTG2_V3_SOURCE_WITNESS_TOTAL_TARGET,
        "provider_quota": witness.PTG2_V3_SOURCE_WITNESS_PROVIDER_QUOTA,
        "rate_occurrence": {
            "population_count": occurrence_population_count,
            "selected_count": occurrence_count,
            "emitted_rate_row_count": max(1, occurrence_count),
            "unqueryable_rate_row_count": 0,
        },
        "provider_reference": {
            "population_count": provider_population_count,
            "selected_count": provider_count,
        },
    }
    header_bytes = json.dumps(
        header_by_field,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    bundle_payload = bytearray(b"PTG2SW02")
    bundle_payload.extend(_U32.pack(len(header_bytes)))
    bundle_payload.extend(header_bytes)
    bundle_payload.extend(_U32.pack(len(compressed_records)))
    for compressed_record in compressed_records:
        bundle_payload.extend(_U32.pack(len(compressed_record)))
        bundle_payload.extend(compressed_record)
    path = tmp_path / name
    path.write_bytes(bundle_payload)
    return {
        "path": str(path),
        "sha256": hashlib.sha256(bundle_payload).hexdigest(),
        "byte_count": len(bundle_payload),
        "row_count": len(compressed_records),
        "raw_source_sha256": source_digest,
    }


def _occurrence_record(index: int, *, priority: int | None = None) -> bytes:
    raw_json = json.dumps(
        {
            "negotiated_prices": [{"negotiated_rate": index + 1}],
            "provider_groups": [
                {
                    "npi": [1_234_567_890],
                    "tin": {"type": "ein", "value": "000000000"},
                }
            ],
        },
        separators=(",", ":"),
    ).encode()
    return _record(
        kind="rate_occurrence",
        priority=index if priority is None else priority,
        item_ordinal=index,
        raw_json=raw_json,
    )


def _provider_record(index: int, *, priority: int | None = None) -> bytes:
    raw_json = json.dumps(
        {
            "provider_group_id": index,
            "provider_groups": [
                {
                    "npi": [1_234_567_890 + index],
                    "tin": {"type": "ein", "value": f"{index:09d}"},
                }
            ],
        },
        separators=(",", ":"),
    ).encode()
    return _record(
        kind="provider_reference",
        priority=index if priority is None else priority,
        item_ordinal=index,
        raw_json=raw_json,
    )


def test_persisted_witness_is_deterministic_and_preserves_exact_raw_tokens(tmp_path):
    """Keep witness selection stable while retaining source token bytes exactly."""

    marker_raw = b'{"note":",\\"raw\\":inside","negotiated_prices":[]}'
    first_compressed_records = [
        _record(
            kind="rate_occurrence",
            priority=index * 2,
            item_ordinal=index,
            raw_json=marker_raw if index == 0 else f'{{"rate":{index}}}'.encode(),
        )
        for index in range(5_025)
    ]
    second_compressed_records = [
        _record(
            kind="rate_occurrence",
            priority=index * 2 + 1,
            item_ordinal=index,
            raw_json=f'{{"rate":{index + 5025}}}'.encode(),
        )
        for index in range(5_025)
    ]
    entry_a = _bundle(
        tmp_path,
        source_digest=SOURCE_A,
        name="a.bin",
        compressed_records=first_compressed_records,
        occurrence_population_count=len(first_compressed_records),
    )
    entry_b = _bundle(
        tmp_path,
        source_digest=SOURCE_B,
        name="b.bin",
        compressed_records=second_compressed_records,
        occurrence_population_count=len(second_compressed_records),
    )

    first_payload, first_metadata = witness.build_persisted_source_witness(
        [entry_b, entry_a],
        expected_raw_source_sha256=[SOURCE_B, SOURCE_A],
    )
    second_payload, second_metadata = witness.build_persisted_source_witness(
        [entry_a, entry_b],
        expected_raw_source_sha256=[SOURCE_A, SOURCE_B],
    )

    assert first_payload == second_payload
    assert first_metadata == second_metadata
    assert first_metadata["queryable_occurrence_population_count"] == 10_050
    assert first_metadata["occurrence_witness_count"] == 10_000
    assert first_metadata["record_count"] == 10_000
    loaded = witness.decode_persisted_source_witness(
        first_payload,
        expected_raw_source_sha256=[SOURCE_A, SOURCE_B],
        expected_metadata=first_metadata,
    )
    assert len(loaded.occurrence_records) == 10_000
    assert marker_raw in {
        witness_record.raw_json
        for witness_record in loaded.occurrence_records
    }
    assert max(
        witness_record.priority
        for witness_record in loaded.occurrence_records
    ) == 9_999


def test_source_witness_accepts_large_exact_record_within_aggregate_budget():
    raw_json = json.dumps(
        {"source_noise": random.Random(7).randbytes(700 * 1024).hex()},
        separators=(",", ":"),
    ).encode()
    compressed_record = _record(
        kind="provider_reference",
        priority=1,
        item_ordinal=1,
        raw_json=raw_json,
    )

    assert 512 * 1024 < len(compressed_record)
    assert len(compressed_record) < witness.PTG2_V3_SOURCE_WITNESS_MAX_RECORD_BYTES
    decoded_record = witness.decode_record(compressed_record, SOURCE_A)
    assert decoded_record.raw_json == raw_json


def test_source_witness_decode_limit_is_enforced_before_flush(monkeypatch):
    compressed_record = _record(
        kind="provider_reference",
        priority=1,
        item_ordinal=1,
        raw_json=json.dumps({"source_noise": "x" * 4_096}).encode(),
    )
    monkeypatch.setattr(
        witness_codec,
        "PTG2_V3_SOURCE_WITNESS_MAX_DECODED_RECORD_BYTES",
        512,
    )

    with pytest.raises(RuntimeError, match="exceeds or violates"):
        witness_codec.decode_record(compressed_record, SOURCE_A)


def test_independent_cohort_targets_are_both_filled(tmp_path):
    occurrence_records = [_occurrence_record(index) for index in range(10_000)]
    provider_records = [
        _provider_record(index, priority=20_000 + index) for index in range(1_000)
    ]
    entry = _bundle(
        tmp_path,
        source_digest=SOURCE_A,
        name="quota.bin",
        compressed_records=[*occurrence_records, *provider_records],
        occurrence_population_count=20_000,
        provider_population_count=5_000,
    )

    witness_payload, witness_metadata = witness.build_persisted_source_witness(
        [entry],
        expected_raw_source_sha256=[SOURCE_A],
    )
    loaded = witness.decode_persisted_source_witness(
        witness_payload,
        expected_raw_source_sha256=[SOURCE_A],
        expected_metadata=witness_metadata,
    )

    assert len(loaded.occurrence_records) == 10_000
    assert len(loaded.provider_records) == 1_000
    assert len(loaded.records) == 11_000


def test_persisted_witness_rejects_corruption_and_manifest_drift(tmp_path):
    entry = _bundle(
        tmp_path,
        source_digest=SOURCE_A,
        name="one.bin",
        compressed_records=[_occurrence_record(0)],
        occurrence_population_count=1,
    )
    witness_payload, witness_metadata = witness.build_persisted_source_witness(
        [entry],
        expected_raw_source_sha256=[SOURCE_A],
    )

    corrupted = bytearray(witness_payload)
    corrupted[-1] ^= 0x01
    with pytest.raises(RuntimeError):
        witness.decode_persisted_source_witness(
            bytes(corrupted),
            expected_raw_source_sha256=[SOURCE_A],
        )

    changed_metadata_by_field = dict(witness_metadata)
    changed_metadata_by_field["payload_bytes"] += 1
    with pytest.raises(RuntimeError, match="manifest fields changed"):
        witness.decode_persisted_source_witness(
            witness_payload,
            expected_raw_source_sha256=[SOURCE_A],
            expected_metadata=changed_metadata_by_field,
        )

    with pytest.raises(RuntimeError, match="source set"):
        witness.decode_persisted_source_witness(
            witness_payload,
            expected_raw_source_sha256=[SOURCE_B],
        )


def test_source_witness_fails_closed_when_one_queryable_occurrence_is_omitted(tmp_path):
    entry = _bundle(
        tmp_path,
        source_digest=SOURCE_A,
        name="incomplete.bin",
        compressed_records=[_occurrence_record(0)],
        occurrence_population_count=2,
    )

    with pytest.raises(RuntimeError, match="rate_occurrence coverage is incomplete"):
        witness.build_persisted_source_witness(
            [entry],
            expected_raw_source_sha256=[SOURCE_A],
        )


def test_source_witness_fails_closed_when_provider_quota_is_incomplete(tmp_path):
    entry = _bundle(
        tmp_path,
        source_digest=SOURCE_A,
        name="provider-incomplete.bin",
        compressed_records=[_occurrence_record(0), _provider_record(0)],
        occurrence_population_count=1,
        provider_population_count=2,
    )

    with pytest.raises(RuntimeError, match="provider_reference coverage is incomplete"):
        witness.build_persisted_source_witness(
            [entry],
            expected_raw_source_sha256=[SOURCE_A],
        )


def test_small_source_requires_and_accepts_its_exact_population(tmp_path):
    entry = _bundle(
        tmp_path,
        source_digest=SOURCE_A,
        name="small.bin",
        compressed_records=[
            _occurrence_record(0),
            _occurrence_record(1),
            _provider_record(0),
        ],
        occurrence_population_count=2,
        provider_population_count=1,
    )

    witness_payload, witness_metadata = witness.build_persisted_source_witness(
        [entry],
        expected_raw_source_sha256=[SOURCE_A],
    )
    loaded = witness.decode_persisted_source_witness(
        witness_payload,
        expected_raw_source_sha256=[SOURCE_A],
        expected_metadata=witness_metadata,
    )

    assert loaded.metadata["queryable_occurrence_population_count"] == 2
    assert loaded.metadata["occurrence_witness_count"] == 2
    assert loaded.metadata["provider_witness_count"] == 1
    assert loaded.metadata["record_count"] == 3


@pytest.mark.asyncio
async def test_postgres_loader_preserves_zero_provider_population(
    tmp_path,
    monkeypatch,
):
    entry = _bundle(
        tmp_path,
        source_digest=SOURCE_A,
        name="zero-providers.bin",
        compressed_records=[_occurrence_record(0)],
        occurrence_population_count=1,
    )
    witness_payload, witness_metadata = witness.build_persisted_source_witness(
        [entry],
        expected_raw_source_sha256=[SOURCE_A],
    )

    async def database_row(_query, **_params):
        return {
            "contract": witness_metadata["contract"],
            "selection_method": witness_metadata["selection_method"],
            "source_set_digest": bytes.fromhex(witness_metadata["source_set_digest"]),
            "sample_digest": bytes.fromhex(witness_metadata["sample_digest"]),
            "queryable_occurrence_population_count": witness_metadata[
                "queryable_occurrence_population_count"
            ],
            "provider_population_count": 0,
            "occurrence_witness_count": witness_metadata["occurrence_witness_count"],
            "provider_witness_count": 0,
            "payload_sha256": bytes.fromhex(witness_metadata["payload_sha256"]),
            "payload": witness_payload,
        }

    monkeypatch.setattr(witness_store.db, "first", database_row)

    loaded = await witness_store.load_shared_source_witness(
        schema_name="mrf",
        snapshot_key=1,
        expected_raw_source_sha256=[SOURCE_A],
        expected_metadata=witness_metadata,
    )

    assert loaded.metadata["provider_population_count"] == 0
    assert loaded.provider_records == ()
