# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded packed-map sidecars derived from authenticated finalizer COPY rows."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


def _remove_sidecar_files(paths: Iterable[Path], directory: Path) -> None:
    errors: list[BaseException] = []
    for path in paths:
        try:
            path.unlink(missing_ok=True)
        except BaseException as exc:
            errors.append(exc)
    try:
        directory.rmdir()
    except BaseException as exc:
        errors.append(exc)
    if errors:
        raise errors[0]


@dataclass(frozen=True)
class PackedMapArtifact:
    path: Path
    row_count: int
    byte_count: int
    sha256: str


@dataclass(frozen=True)
class PackedMapSidecars:
    """Exact task-local files derived from one authenticated block COPY."""

    directory: Path
    target_blocks: PackedMapArtifact
    map_blocks: PackedMapArtifact
    map_packs: PackedMapArtifact
    object_kinds: tuple[str, ...]
    map_pack_count: int
    coordinate_count: int
    target_block_count: int
    entry_count: int
    logical_byte_count: int
    stored_byte_count: int
    stored_map_byte_count: int
    kind_digests: tuple[tuple[str, bytes], ...]
    source_copy_bytes: int
    target_stored_byte_count: int

    def cleanup(self) -> None:
        """Remove the exact task-local sidecar files and directory."""

        _remove_sidecar_files(
            (
                self.map_blocks.path,
                self.map_packs.path,
                self.target_blocks.path,
            ),
            self.directory,
        )


@dataclass(frozen=True)
class PackedMapNativeReceipt:
    """Source-bound native receipt shared by every emitted finalizer lane."""

    directory: Path
    sidecars: tuple[PackedMapSidecars, ...]
    canonical_mapping_digest: bytes
    canonical_byte_count: int
    target_identity_digest: bytes
    elapsed_seconds: float

    def manifest(self) -> dict[str, Any]:
        """Return exact source-to-unique staging measurements."""

        lane_by_name: dict[str, dict[str, int]] = {}
        for sidecar in self.sidecars:
            reused_payload_bytes = (
                sidecar.stored_byte_count - sidecar.target_stored_byte_count
            )
            suppressed_metadata_bytes = (
                sidecar.source_copy_bytes
                - sidecar.target_blocks.byte_count
                - reused_payload_bytes
            )
            if min(reused_payload_bytes, suppressed_metadata_bytes) < 0:
                raise RuntimeError("native packed finalizer byte accounting changed")
            lane_name = sidecar.directory.name
            lane_by_name[lane_name] = {
                "source_copy_bytes": sidecar.source_copy_bytes,
                "staged_copy_bytes": sidecar.target_blocks.byte_count,
                "source_payload_bytes": sidecar.stored_byte_count,
                "staged_payload_bytes": sidecar.target_stored_byte_count,
                "same_copy_reused_payload_bytes": reused_payload_bytes,
                "row_count": sidecar.coordinate_count,
                "unique_block_count": sidecar.target_block_count,
                "duplicate_block_row_count": (
                    sidecar.coordinate_count - sidecar.target_block_count
                ),
                "suppressed_duplicate_metadata_bytes": suppressed_metadata_bytes,
            }
        return {
            "contract": "native_unique_shared_block_copy_v2",
            "elapsed_seconds": float(self.elapsed_seconds),
            "lanes": lane_by_name,
            "total": {
                field_name: sum(
                    lane[field_name] for lane in lane_by_name.values()
                )
                for field_name in next(iter(lane_by_name.values()))
            },
        }

    def cleanup(self) -> None:
        """Remove only this receipt's exact atomic output directory."""

        errors: list[BaseException] = []
        for sidecar in self.sidecars:
            try:
                sidecar.cleanup()
            except BaseException as exc:
                errors.append(exc)
        try:
            (self.directory / "summary.json").unlink(missing_ok=True)
            self.directory.rmdir()
        except BaseException as exc:
            errors.append(exc)
        if errors:
            raise errors[0]


__all__ = (
    "PackedMapArtifact",
    "PackedMapNativeReceipt",
    "PackedMapSidecars",
)
