#!/usr/bin/env python3
"""Immutable inputs and receipts for the packed-finalizer ABBA screen."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from process.ptg_parts.ptg2_v4_finalizer_maps import (
    PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS,
)


SHAPE_CONTRACT = "ptg2_packed_finalizer_abba_shape_v1"
ARTIFACT_CONTRACT = "ptg2_packed_finalizer_abba_artifacts_v2"
SYNTHETIC_CLASSIFICATION = "synthetic_non_representative"
REPRESENTATIVE_CLASSIFICATION = "representative_source_receipt"
PRICE_OBJECT_KINDS = ("price_atoms_v3", "price_set_atom_memberships_v3")
ALL_OBJECT_KINDS = tuple(
    sorted((*PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS, *PRICE_OBJECT_KINDS))
)
PRICE_DICTIONARY_KIND = "by_code_price_dictionary"
SERVING_OBJECT_KINDS = tuple(
    kind
    for kind in PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS
    if kind != PRICE_DICTIONARY_KIND
)
SHA256_HEX_LENGTH = 64
# Aspirational DEV gate from the requested 300K finalizer target.
FINALIZER_RATE_GATE = 300_000.0
# 73.996M rows in eight minutes, including prepare, publish, summary, and commit.
WHOLE_RATE_GATE = 154_160.0
CANONICAL_FIELDS = (
    "mapping_digest",
    "mapping_count",
    "unique_block_count",
    "entry_count",
    "logical_byte_count",
    "canonical_byte_count",
    "object_kinds",
)


@dataclass(frozen=True)
class KindAllocation:
    mapping_count: int
    unique_block_count: int

    @classmethod
    def from_mapping(cls, allocation_by_field: Mapping[str, Any]) -> "KindAllocation":
        """Normalize one declared object-kind allocation."""

        if not isinstance(allocation_by_field, Mapping):
            raise ValueError("object-kind allocation must be a JSON object")
        if set(allocation_by_field) != {"mapping_count", "unique_block_count"}:
            raise ValueError("object-kind allocation fields are incompatible")
        counts = tuple(allocation_by_field[field] for field in sorted(allocation_by_field))
        if any(isinstance(count, bool) or not isinstance(count, int) for count in counts):
            raise ValueError("object-kind allocation counts must be integers")
        allocation = cls(
            mapping_count=int(allocation_by_field["mapping_count"]),
            unique_block_count=int(allocation_by_field["unique_block_count"]),
        )
        if not 0 < allocation.unique_block_count <= allocation.mapping_count:
            raise ValueError("object-kind allocation geometry is invalid")
        return allocation

    def as_dict(self) -> dict[str, int]:
        """Return the canonical JSON fields for one allocation."""
        return {
            "mapping_count": self.mapping_count,
            "unique_block_count": self.unique_block_count,
        }


@dataclass(frozen=True)
class BenchmarkShape:
    classification: str
    allocation_by_kind: Mapping[str, KindAllocation]
    source_receipt_sha256: str | None = None

    @classmethod
    def from_mapping(cls, shape_by_field: Mapping[str, Any]) -> "BenchmarkShape":
        """Fail closed on unknown, incomplete, or unproven shape declarations."""

        expected_fields = {"contract", "classification", "allocation_by_kind"}
        optional_fields = {"source_receipt_sha256"}
        if not expected_fields <= set(shape_by_field) <= expected_fields | optional_fields:
            raise ValueError("ABBA shape fields are incompatible")
        if shape_by_field["contract"] != SHAPE_CONTRACT:
            raise ValueError("ABBA shape contract is incompatible")
        raw_allocations = shape_by_field["allocation_by_kind"]
        if not isinstance(raw_allocations, Mapping) or set(raw_allocations) != set(ALL_OBJECT_KINDS):
            raise ValueError("ABBA shape must declare exactly six finalizer and two price kinds")
        allocation_by_kind = {
            str(kind): KindAllocation.from_mapping(raw_allocations[kind])
            for kind in ALL_OBJECT_KINDS
        }
        classification = str(shape_by_field["classification"])
        source_receipt = shape_by_field.get("source_receipt_sha256")
        _validate_classification(classification, source_receipt)
        return cls(classification, allocation_by_kind, source_receipt)

    @property
    def mapping_count(self) -> int:
        """Return mappings across all eight object kinds."""
        return sum(allocation.mapping_count for allocation in self.allocation_by_kind.values())

    @property
    def unique_block_count(self) -> int:
        """Return unique target blocks across all object kinds."""
        return sum(
            allocation.unique_block_count for allocation in self.allocation_by_kind.values()
        )

    @property
    def finalizer_mapping_count(self) -> int:
        """Return mappings stored by the six-kind packed lane."""
        return sum(
            self.allocation_by_kind[kind].mapping_count
            for kind in PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS
        )

    @property
    def finalizer_unique_block_count(self) -> int:
        """Return unique target blocks stored by the packed lane."""
        return sum(
            self.allocation_by_kind[kind].unique_block_count
            for kind in PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS
        )

    @property
    def map_pack_count(self) -> int:
        """Return the exact 256-coordinate map-pack count."""
        return sum(
            (self.allocation_by_kind[kind].mapping_count + 255) // 256
            for kind in PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS
        )

    @property
    def is_release_eligible(self) -> bool:
        """Return whether an authenticated representative receipt is present."""
        return self.classification == REPRESENTATIVE_CLASSIFICATION

    def as_dict(self) -> dict[str, Any]:
        """Return the canonical JSON shape contract."""
        shape_by_field: dict[str, Any] = {
            "contract": SHAPE_CONTRACT,
            "classification": self.classification,
            "allocation_by_kind": {
                kind: self.allocation_by_kind[kind].as_dict()
                for kind in ALL_OBJECT_KINDS
            },
        }
        if self.source_receipt_sha256 is not None:
            shape_by_field["source_receipt_sha256"] = self.source_receipt_sha256
        return shape_by_field

    def sha256(self) -> str:
        """Return the canonical shape digest."""
        return hashlib.sha256(_canonical_json(self.as_dict())).hexdigest()


def _validate_classification(classification: str, source_receipt: Any) -> None:
    if classification == SYNTHETIC_CLASSIFICATION:
        if source_receipt is not None:
            raise ValueError("synthetic ABBA shape cannot claim a source receipt")
        return
    if classification != REPRESENTATIVE_CLASSIFICATION:
        raise ValueError("ABBA shape classification is unsupported")
    if not isinstance(source_receipt, str) or len(source_receipt) != SHA256_HEX_LENGTH:
        raise ValueError("representative ABBA shape requires a SHA-256 source receipt")
    try:
        bytes.fromhex(source_receipt)
    except ValueError as exc:
        raise ValueError("representative ABBA source receipt is not hexadecimal") from exc


def default_synthetic_shape() -> BenchmarkShape:
    """Return the explicit 8M mechanism screen; never production evidence."""

    allocation_by_kind = {
        kind: KindAllocation(1_333_333, 216_961)
        for kind in PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS
    }
    allocation_by_kind.update(
        {kind: KindAllocation(1, 1) for kind in PRICE_OBJECT_KINDS}
    )
    shape = BenchmarkShape(SYNTHETIC_CLASSIFICATION, allocation_by_kind)
    if (shape.mapping_count, shape.unique_block_count, shape.map_pack_count) != (
        8_000_000,
        1_301_768,
        31_254,
    ):
        raise RuntimeError("default synthetic ABBA allocation changed")
    return shape


def failure_probe_shape() -> BenchmarkShape:
    """Return the minimum all-kind shape for real rollback probes."""

    return BenchmarkShape(
        SYNTHETIC_CLASSIFICATION,
        {kind: KindAllocation(1, 1) for kind in ALL_OBJECT_KINDS},
    )


def load_shape(shape_path: Path | None) -> BenchmarkShape:
    """Load a declared shape or return the explicit synthetic screen."""
    if shape_path is None:
        return default_synthetic_shape()
    shape_by_field = json.loads(shape_path.read_text(encoding="utf-8"))
    if not isinstance(shape_by_field, Mapping):
        raise ValueError("ABBA shape must be a JSON object")
    return BenchmarkShape.from_mapping(shape_by_field)


def _mechanism_gates(arms: list[Mapping[str, Any]]) -> dict[str, Any]:
    packed_arms = [arm for arm in arms if arm["arm"] == "packed"]
    legacy_arms = [arm for arm in arms if arm["arm"] == "legacy"]
    canonical_summaries = {
        json.dumps(
            {field: arm["summary"][field] for field in CANONICAL_FIELDS},
            sort_keys=True,
        )
        for arm in arms
    }
    gate_by_name = {
        "both_packed_finalizer_at_least_300k": all(
            arm["finalizer_rows_per_second"] >= FINALIZER_RATE_GATE
            for arm in packed_arms
        ),
        "both_packed_whole_at_least_154160": all(
            arm["prepare_plus_publication_plus_summary_rows_per_second"]
            >= WHOLE_RATE_GATE
            for arm in packed_arms
        ),
        "packed_faster_than_legacy_whole_wrapper": max(
            arm["prepare_plus_publication_plus_summary_seconds"]
            for arm in packed_arms
        )
        < min(
            arm["prepare_plus_publication_plus_summary_seconds"]
            for arm in legacy_arms
        ),
        "canonical_abba_parity": len(canonical_summaries) == 1,
    }
    gate_by_name["passed"] = all(gate_by_name.values())
    return gate_by_name


@dataclass(frozen=True)
class ArtifactFileReceipt:
    path: Path
    byte_count: int
    row_count: int
    stored_payload_bytes: int
    sha256: str

    @classmethod
    def from_mapping(
        cls,
        root: Path,
        receipt_by_field: Mapping[str, Any],
    ) -> "ArtifactFileReceipt":
        """Authenticate one root-local production COPY artifact."""

        if set(receipt_by_field) != {
            "path",
            "byte_count",
            "row_count",
            "stored_payload_bytes",
            "sha256",
        }:
            raise ValueError("ABBA artifact file fields are incompatible")
        relative = Path(str(receipt_by_field["path"]))
        path = (root / relative).resolve()
        if relative.is_absolute() or path.parent != root.resolve():
            raise ValueError("ABBA artifact path escapes its manifest directory")
        byte_count = receipt_by_field["byte_count"]
        row_count = receipt_by_field["row_count"]
        stored_payload_bytes = receipt_by_field["stored_payload_bytes"]
        sha256 = str(receipt_by_field["sha256"])
        if (
            isinstance(byte_count, bool)
            or not isinstance(byte_count, int)
            or byte_count <= 0
            or isinstance(row_count, bool)
            or not isinstance(row_count, int)
            or row_count <= 0
            or isinstance(stored_payload_bytes, bool)
            or not isinstance(stored_payload_bytes, int)
            or stored_payload_bytes < 0
            or len(sha256) != SHA256_HEX_LENGTH
        ):
            raise ValueError("ABBA artifact file receipt is invalid")
        try:
            bytes.fromhex(sha256)
        except ValueError as exc:
            raise ValueError("ABBA artifact file SHA-256 is invalid") from exc
        if path.stat().st_size != byte_count or _sha256_file(path) != sha256:
            raise ValueError("ABBA artifact file content does not match its receipt")
        return cls(path, byte_count, row_count, stored_payload_bytes, sha256)

    def as_dict(self, root: Path) -> dict[str, Any]:
        """Return a root-relative immutable file receipt."""
        return {
            "path": str(self.path.relative_to(root)),
            "byte_count": self.byte_count,
            "row_count": self.row_count,
            "stored_payload_bytes": self.stored_payload_bytes,
            "sha256": self.sha256,
        }


@dataclass(frozen=True)
class BenchmarkArtifacts:
    directory: Path
    shape: BenchmarkShape
    serving: ArtifactFileReceipt
    price_dictionary: ArtifactFileReceipt
    relational_price: ArtifactFileReceipt
    expected_summary: Mapping[str, Any]
    manifest_path: Path
    manifest_sha256: str
    owned_by_run: bool = True
    source_receipt_path: Path | None = None
    source_receipt_sha256: str | None = None

    def finalizer_summary(self) -> dict[str, Any]:
        """Return the production finalizer-wrapper input summary."""
        return {
            "output_directory": str(self.directory),
            "blocks": {
                "serving": _finalizer_block_summary(
                    self.serving,
                    {
                        kind: self.shape.allocation_by_kind[kind].mapping_count
                        for kind in SERVING_OBJECT_KINDS
                    },
                ),
                "price_dictionary": _finalizer_block_summary(
                    self.price_dictionary,
                    {
                        PRICE_DICTIONARY_KIND: self.shape.allocation_by_kind[
                            PRICE_DICTIONARY_KIND
                        ].mapping_count
                    },
                ),
            },
        }

    def cleanup(self) -> None:
        """Remove only this artifact set and its exact directory."""
        if not self.owned_by_run:
            return
        for artifact in (self.serving, self.price_dictionary, self.relational_price):
            artifact.path.unlink(missing_ok=True)
        self.manifest_path.unlink(missing_ok=True)
        self.directory.rmdir()

    def assert_external_inputs_unchanged(self) -> None:
        """Rehash every externally owned input against its admitted identity."""

        if self.owned_by_run:
            return
        if (
            self.source_receipt_path is None
            or self.source_receipt_sha256 is None
            or _sha256_file(self.manifest_path) != self.manifest_sha256
            or _sha256_file(self.source_receipt_path)
            != self.source_receipt_sha256
        ):
            raise ValueError("ABBA external manifest or source receipt changed")
        for artifact_receipt in (
            self.serving,
            self.price_dictionary,
            self.relational_price,
        ):
            if (
                artifact_receipt.path.stat().st_size
                != artifact_receipt.byte_count
                or _sha256_file(artifact_receipt.path)
                != artifact_receipt.sha256
            ):
                raise ValueError("ABBA external COPY artifact changed")


def _finalizer_block_summary(
    artifact: ArtifactFileReceipt,
    record_count_by_kind: Mapping[str, int],
) -> dict[str, Any]:
    if sum(record_count_by_kind.values()) != artifact.row_count:
        raise RuntimeError("ABBA finalizer artifact row count changed")
    return {
        "path": artifact.path.name,
        "copy_bytes": artifact.byte_count,
        "copy_sha256": artifact.sha256,
        "row_count": artifact.row_count,
        "stored_payload_bytes": artifact.stored_payload_bytes,
        "artifact_record_counts": dict(record_count_by_kind),
    }


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_json(mapping: Mapping[str, Any]) -> bytes:
    return json.dumps(mapping, sort_keys=True, separators=(",", ":")).encode("utf-8")


__all__ = (
    "ALL_OBJECT_KINDS",
    "BenchmarkArtifacts",
    "BenchmarkShape",
    "CANONICAL_FIELDS",
    "FINALIZER_RATE_GATE",
    "KindAllocation",
    "PRICE_OBJECT_KINDS",
    "REPRESENTATIVE_CLASSIFICATION",
    "SYNTHETIC_CLASSIFICATION",
    "WHOLE_RATE_GATE",
    "default_synthetic_shape",
    "failure_probe_shape",
    "load_shape",
)
