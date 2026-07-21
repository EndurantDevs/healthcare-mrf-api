# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import process.uhc_retained_source_registry as source_registry
from process.uhc_retained_native import retain_source_native
from process.uhc_retained_range_manifest import (
    RANGE_CANONICALIZATION_ID,
    RANGE_CONTRACT_ID,
    RANGE_CONTRACT_VERSION,
    range_manifest_path,
    range_set_digest,
    retained_raw_path,
)
from process.uhc_retained_registry_contract import SourceBinding
from process.uhc_retained_types import RawRangeProof


PRODUCER_BUILD_ID = "ptg2_scanner-test-producer"
VERIFIER_BUILD_ID = "ptg2_scanner-test-verifier"


def digest(label: str) -> str:
    return hashlib.sha256(label.encode("utf-8")).hexdigest()


def source_binding(
    artifact_sha256: str,
    artifact_byte_count: int,
    *,
    catalog_set: str = "catalog-one",
    collection_kind: str = "provider_membership",
    file_name: str | None = None,
    source_url: str | None = None,
    size_bytes: int | None | object = ...,
) -> SourceBinding:
    if collection_kind == "plan_reference":
        file_name = file_name or "JSON_PLANS_WY.json"
        source_url = source_url or (
            "https://providermrf.uhc.com/api/stream/ui/ifp/plans/"
            "JSON_PLANS_WY.json"
        )
    else:
        file_name = file_name or "JSON_Providers_AZDC.json"
        source_url = source_url or (
            "https://providermrf.uhc.com/api/stream/ui/ifp/providers/"
            "JSON_Providers_AZDC.json"
        )
    catalog_size = artifact_byte_count if size_bytes is ... else size_bytes
    catalog_modified_at = "2026-07-20T08:00:00Z"
    catalog_entry_sha256, source_file_id = (
        source_registry._expected_catalog_file_hash_pair(
            family="ifp",
            collection_kind=collection_kind,
            file_name=file_name,
            source_url=source_url,
            catalog_modified_at=catalog_modified_at,
            size_bytes=catalog_size,
        )
    )
    return SourceBinding(
        catalog_set_sha256=digest(catalog_set),
        source_file_id=source_file_id,
        family="ifp",
        collection_kind=collection_kind,
        file_name=file_name,
        source_url=source_url,
        catalog_modified_at=catalog_modified_at,
        size_bytes=catalog_size,
        catalog_entry_sha256=catalog_entry_sha256,
        artifact_sha256=artifact_sha256,
    )


def records_payload(label: str = "provider", count: int = 16) -> list[dict]:
    return [
        {
            "ordinal": ordinal,
            "label": label,
            "padding": "x" * 64,
        }
        for ordinal in range(count)
    ]


def _encoded_records(records: list[dict]) -> tuple[bytes, list[tuple[int, int]]]:
    encoded_objects = [
        json.dumps(
            record,
            ensure_ascii=True,
            separators=(",", ":"),
        ).encode("ascii")
        for record in records
    ]
    offsets = []
    cursor = 1
    for encoded_object in encoded_objects:
        offsets.append((cursor, cursor + len(encoded_object)))
        cursor += len(encoded_object) + 1
    return b"[" + b",".join(encoded_objects) + b"]", offsets


def _range_proofs(
    raw_bytes: bytes,
    offsets: list[tuple[int, int]],
    *,
    artifact_sha256: str,
    raw_path: Path,
    range_count: int,
) -> tuple[RawRangeProof, ...]:
    quotient, remainder = divmod(len(offsets), range_count)
    retained_ranges = []
    record_start = 0
    for range_ordinal in range(range_count):
        record_count = quotient + (1 if range_ordinal < remainder else 0)
        if record_count <= 0:
            raise ValueError("fixture needs at least one record per range")
        record_end = record_start + record_count
        raw_byte_start = offsets[record_start][0]
        raw_byte_end = offsets[record_end - 1][1]
        raw_slice = raw_bytes[raw_byte_start:raw_byte_end]
        canonical_bytes = b"".join(
            raw_bytes[start:end].replace(b"\\r", b"").replace(b"\\n", b"")
            + b"\\n"
            for start, end in offsets[record_start:record_end]
        )
        retained_ranges.append(
            RawRangeProof(
                artifact_sha256=artifact_sha256,
                contract_version=RANGE_CONTRACT_VERSION,
                range_count=range_count,
                range_ordinal=range_ordinal,
                raw_byte_start=raw_byte_start,
                raw_byte_end=raw_byte_end,
                raw_sha256=hashlib.sha256(raw_slice).hexdigest(),
                raw_byte_count=len(raw_slice),
                record_start=record_start,
                record_end=record_end,
                record_count=record_count,
                canonical_sha256=hashlib.sha256(canonical_bytes).hexdigest(),
                canonical_byte_count=len(canonical_bytes),
                path=str(raw_path),
            )
        )
        record_start = record_end
    return tuple(retained_ranges)


def _manifest_map(
    *,
    raw_path: Path,
    raw_bytes: bytes,
    artifact_sha256: str,
    record_count: int,
    range_count: int,
    producer_build_id: str,
    retained_ranges: tuple[RawRangeProof, ...],
) -> dict[str, object]:
    range_set_sha256 = range_set_digest(
        artifact_sha256,
        len(raw_bytes),
        record_count,
        retained_ranges,
    )
    return {
        "contract_id": RANGE_CONTRACT_ID,
        "contract_version": RANGE_CONTRACT_VERSION,
        "canonicalization_id": RANGE_CANONICALIZATION_ID,
        "producer_build_id": producer_build_id,
        "raw_artifact": {
            "file_name": raw_path.name,
            "sha256": artifact_sha256,
            "byte_count": len(raw_bytes),
            "record_count": record_count,
        },
        "range_count": range_count,
        "ranges": [
            {
                "range_ordinal": raw_range.range_ordinal,
                "raw_byte_start": raw_range.raw_byte_start,
                "raw_byte_end": raw_range.raw_byte_end,
                "raw_byte_count": raw_range.raw_byte_count,
                "raw_sha256": raw_range.raw_sha256,
                "record_start": raw_range.record_start,
                "record_end": raw_range.record_end,
                "record_count": raw_range.record_count,
                "canonical_sha256": raw_range.canonical_sha256,
                "canonical_byte_count": raw_range.canonical_byte_count,
            }
            for raw_range in retained_ranges
        ],
        "range_set_sha256": range_set_sha256,
    }


def write_retained_fixture(
    root: Path,
    retained_records: list[dict],
    *,
    range_count: int = 4,
    producer_build_id: str = PRODUCER_BUILD_ID,
) -> dict[str, object]:
    """Write one raw file and its exact deterministic range-manifest fixture."""

    root.mkdir(parents=True, exist_ok=True)
    raw_bytes, offsets = _encoded_records(retained_records)
    artifact_sha256 = hashlib.sha256(raw_bytes).hexdigest()
    raw_path = retained_raw_path(root, artifact_sha256)
    raw_path.write_bytes(raw_bytes)
    retained_ranges = _range_proofs(
        raw_bytes,
        offsets,
        artifact_sha256=artifact_sha256,
        raw_path=raw_path,
        range_count=range_count,
    )
    manifest_map = _manifest_map(
        raw_path=raw_path,
        raw_bytes=raw_bytes,
        artifact_sha256=artifact_sha256,
        record_count=len(retained_records),
        range_count=range_count,
        producer_build_id=producer_build_id,
        retained_ranges=retained_ranges,
    )
    manifest_bytes = json.dumps(
        manifest_map,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")
    manifest_path = range_manifest_path(root, artifact_sha256, range_count)
    manifest_path.write_bytes(manifest_bytes)
    return {
        "source_bytes": raw_bytes,
        "artifact_sha256": artifact_sha256,
        "artifact_byte_count": len(raw_bytes),
        "record_count": len(retained_records),
        "raw_path": raw_path,
        "manifest_path": manifest_path,
        "manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
        "manifest_byte_count": len(manifest_bytes),
        "ranges": retained_ranges,
        "producer_build_id": producer_build_id,
        "range_count": range_count,
    }


def native_summary(fixture: dict[str, object], **updates) -> dict[str, object]:
    summary_map = {
        "record_kind": "uhc_retained_summary",
        "contract_id": RANGE_CONTRACT_ID,
        "contract_version": RANGE_CONTRACT_VERSION,
        "canonicalization_id": RANGE_CANONICALIZATION_ID,
        "producer_build_id": fixture["producer_build_id"],
        "verifier_build_id": VERIFIER_BUILD_ID,
        "raw_artifact_path": str(fixture["raw_path"]),
        "raw_artifact_sha256": fixture["artifact_sha256"],
        "raw_artifact_byte_count": fixture["artifact_byte_count"],
        "record_count": fixture["record_count"],
        "range_count": fixture["range_count"],
        "manifest_path": str(fixture["manifest_path"]),
        "manifest_sha256": fixture["manifest_sha256"],
        "manifest_byte_count": fixture["manifest_byte_count"],
        "raw_reused": False,
        "manifest_reused": False,
        "timings_seconds": {"total": 0.001},
    }
    summary_map.update(updates)
    return summary_map


def install_fake_native(
    tmp_path: Path,
    monkeypatch,
    summary: dict[str, object],
) -> Path:
    summary_path = tmp_path / "native-summary.json"
    summary_path.write_text(
        json.dumps(summary, separators=(",", ":")),
        encoding="utf-8",
    )
    binary = tmp_path / "fake-ptg2-scanner"
    binary.write_text(
        "#!/usr/bin/env python3\n"
        "import os, pathlib, sys\n"
        "if len(sys.argv) != 7 or sys.argv[1] != '--uhc-retain':\n"
        "    raise SystemExit(64)\n"
        "sys.stdout.buffer.write(pathlib.Path("
        "os.environ['UHC_TEST_NATIVE_SUMMARY']).read_bytes())\n",
        encoding="utf-8",
    )
    binary.chmod(0o755)
    monkeypatch.setenv("HLTHPRT_PTG2_RUST_SCANNER_BIN", str(binary))
    monkeypatch.setenv("UHC_TEST_NATIVE_SUMMARY", str(summary_path))
    return binary


async def native_verified_source(
    tmp_path: Path,
    monkeypatch,
    *,
    records: list[dict] | None = None,
    range_count: int = 4,
):
    retained_root = tmp_path / "retained"
    fixture = write_retained_fixture(
        retained_root,
        records or records_payload(),
        range_count=range_count,
    )
    source_path = tmp_path / "source.json"
    source_path.write_bytes(fixture["source_bytes"])
    install_fake_native(tmp_path, monkeypatch, native_summary(fixture))
    source = await retain_source_native(
        source_path=source_path,
        output_root=retained_root,
        expected_sha256=str(fixture["artifact_sha256"]),
        expected_byte_count=int(fixture["artifact_byte_count"]),
        range_count=range_count,
    )
    return fixture, source


from tests.uhc_retained_registry_fake_db import (
    FakeRegistryConnection,
    FakeTransaction,
)
