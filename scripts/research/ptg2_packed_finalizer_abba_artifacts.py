"""Deterministic production-format artifacts for packed-finalizer screens."""

from __future__ import annotations

import hashlib
import os
import struct
from pathlib import Path
from typing import Any, Mapping

from process.ptg_parts.ptg2_shared_blocks import (
    SharedBlockReference,
    _SharedMappingBinaryCopyDigest,
    shared_block_hash,
)
from scripts.research.ptg2_packed_finalizer_abba_contract import (
    ALL_OBJECT_KINDS,
    ARTIFACT_CONTRACT,
    PRICE_DICTIONARY_KIND,
    PRICE_OBJECT_KINDS,
    ArtifactFileReceipt,
    BenchmarkArtifacts,
    BenchmarkShape,
    KindAllocation,
    _canonical_json,
    _sha256_file,
)


COPY_HEADER = b"PGCOPY\n\xff\r\n\x00" + struct.pack(">ii", 0, 0)
COPY_TRAILER = struct.pack(">h", -1)


class _BinaryCopyWriter:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._output = path.open("xb")
        self._digest = hashlib.sha256()
        self._byte_count = 0
        self._row_count = 0
        self._stored_payload_bytes = 0
        self._write(COPY_HEADER)

    def _write(self, chunk: bytes) -> None:
        self._output.write(chunk)
        self._digest.update(chunk)
        self._byte_count += len(chunk)

    def write_reference(
        self,
        reference: SharedBlockReference,
        block_payload: bytes,
    ) -> None:
        """Append one production-format shared-block COPY row."""

        fields = (
            reference.block_hash,
            struct.pack(">h", 2),
            reference.object_kind.encode("utf-8"),
            struct.pack(">q", reference.block_key),
            struct.pack(">i", reference.fragment_no),
            struct.pack(">q", reference.entry_count),
            b"none",
            struct.pack(">q", reference.raw_byte_count),
            struct.pack(">q", len(block_payload)),
            block_payload,
        )
        self._write(struct.pack(">h", len(fields)))
        for field in fields:
            self._write(struct.pack(">i", len(field)))
            self._write(field)
        self._row_count += 1
        self._stored_payload_bytes += len(block_payload)

    def finish(self) -> ArtifactFileReceipt:
        """Seal, sync, and return the immutable artifact receipt."""

        self._write(COPY_TRAILER)
        self._output.flush()
        os.fsync(self._output.fileno())
        self._output.close()
        return ArtifactFileReceipt(
            self.path,
            self._byte_count,
            self._row_count,
            self._stored_payload_bytes,
            self._digest.hexdigest(),
        )

    def abort(self) -> None:
        """Close and remove this incomplete artifact."""

        self._output.close()
        self.path.unlink(missing_ok=True)


def _target_payload(object_kind: str, target_number: int) -> bytes:
    digest = hashlib.sha256()
    digest.update(b"PTG2-PACKED-FINALIZER-ABBA-TARGET\x00")
    digest.update(object_kind.encode("utf-8"))
    digest.update(struct.pack(">Q", int(target_number)))
    return digest.digest()


def _artifact_writer_by_kind(
    object_kind: str,
    writers_by_lane: Mapping[str, _BinaryCopyWriter],
) -> _BinaryCopyWriter:
    if object_kind in PRICE_OBJECT_KINDS:
        return writers_by_lane["relational_price"]
    if object_kind == PRICE_DICTIONARY_KIND:
        return writers_by_lane["price_dictionary"]
    return writers_by_lane["serving"]


def _write_kind_artifact(
    object_kind: str,
    allocation: KindAllocation,
    writer: _BinaryCopyWriter,
    canonical_digest: _SharedMappingBinaryCopyDigest,
    component_digest: _SharedMappingBinaryCopyDigest,
) -> None:
    for block_key in range(allocation.mapping_count):
        target_number = block_key % allocation.unique_block_count
        block_payload = _target_payload(object_kind, target_number)
        block_hash = shared_block_hash(
            format_version=2,
            object_kind=object_kind,
            codec="none",
            payload=block_payload,
        )
        reference = SharedBlockReference(
            object_kind=object_kind,
            block_key=block_key,
            fragment_no=0,
            entry_count=1,
            block_hash=block_hash,
            raw_byte_count=len(block_payload),
        )
        writer.write_reference(reference, block_payload)
        canonical_digest.add_mapping(reference)
        component_digest.add_mapping(reference)


def _artifact_manifest(
    directory: Path,
    shape: BenchmarkShape,
    receipts_by_lane: Mapping[str, ArtifactFileReceipt],
    expected_summary: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "contract": ARTIFACT_CONTRACT,
        "shape": shape.as_dict(),
        "shape_sha256": shape.sha256(),
        "files": {
            lane: artifact_receipt.as_dict(directory)
            for lane, artifact_receipt in receipts_by_lane.items()
        },
        "expected_summary": expected_summary,
    }


def _expected_summary(
    shape: BenchmarkShape,
    canonical: Any,
    packed: Any,
    price: Any,
) -> dict[str, Any]:
    if canonical.mapping_count != shape.mapping_count:
        raise RuntimeError("canonical artifact mapping count changed")
    return {
        "mapping_digest": canonical.mapping_digest.hex(),
        "mapping_count": canonical.mapping_count,
        "unique_block_count": shape.unique_block_count,
        "entry_count": canonical.entry_count,
        "logical_byte_count": shape.mapping_count * 32,
        "canonical_byte_count": canonical.canonical_byte_count,
        "object_kinds": list(ALL_OBJECT_KINDS),
        "packed_mapping_digest": packed.mapping_digest.hex(),
        "packed_mapping_count": packed.mapping_count,
        "packed_canonical_byte_count": packed.canonical_byte_count,
        "relational_mapping_digest": price.mapping_digest.hex(),
        "relational_mapping_count": price.mapping_count,
        "map_pack_count": shape.map_pack_count,
    }


def _artifact_writers(directory: Path) -> dict[str, _BinaryCopyWriter]:
    return {
        "serving": _BinaryCopyWriter(directory / "serving.copy"),
        "price_dictionary": _BinaryCopyWriter(directory / "price_dictionary.copy"),
        "relational_price": _BinaryCopyWriter(directory / "relational_price.copy"),
    }


def generate_artifacts(
    directory: Path,
    shape: BenchmarkShape,
) -> BenchmarkArtifacts:
    """Create only production-format source COPYs; production makes sidecars."""

    if shape.is_release_eligible:
        raise ValueError(
            "deterministic ABBA artifact generation is synthetic, not representative"
        )
    directory.mkdir(parents=False, exist_ok=False)
    writers_by_lane = _artifact_writers(directory)
    canonical_digest = _SharedMappingBinaryCopyDigest()
    packed_digest = _SharedMappingBinaryCopyDigest()
    price_digest = _SharedMappingBinaryCopyDigest()
    manifest_path = directory / "artifact_manifest.json"
    try:
        for object_kind in ALL_OBJECT_KINDS:
            component_digest = (
                price_digest if object_kind in PRICE_OBJECT_KINDS else packed_digest
            )
            _write_kind_artifact(
                object_kind,
                shape.allocation_by_kind[object_kind],
                _artifact_writer_by_kind(object_kind, writers_by_lane),
                canonical_digest,
                component_digest,
            )
        receipts_by_lane = {
            lane: writer.finish() for lane, writer in writers_by_lane.items()
        }
        expected_summary = _expected_summary(
            shape,
            canonical_digest.finish(),
            packed_digest.finish(),
            price_digest.finish(),
        )
        manifest_by_field = _artifact_manifest(
            directory,
            shape,
            receipts_by_lane,
            expected_summary,
        )
        manifest_path.write_bytes(_canonical_json(manifest_by_field) + b"\n")
    except BaseException:
        for writer in writers_by_lane.values():
            writer.abort()
        manifest_path.unlink(missing_ok=True)
        directory.rmdir()
        raise
    return BenchmarkArtifacts(
        directory=directory,
        shape=shape,
        serving=receipts_by_lane["serving"],
        price_dictionary=receipts_by_lane["price_dictionary"],
        relational_price=receipts_by_lane["relational_price"],
        expected_summary=expected_summary,
        manifest_path=manifest_path,
        manifest_sha256=_sha256_file(manifest_path),
    )


__all__ = ("generate_artifacts",)
