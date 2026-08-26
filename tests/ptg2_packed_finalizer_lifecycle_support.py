# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Compact fixtures shared by packed-finalizer lifecycle proofs."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from process.ptg_parts import ptg2_v4_snapshot_maps as snapshot_maps
from process.ptg_parts.ptg2_v4_finalizer_maps import (
    PTG2_V4_FINALIZER_MAP_CONTRACT,
    PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS,
)
from scripts.research.ptg2_packed_finalizer_abba_contract import BenchmarkArtifacts
from scripts.research.ptg2_packed_finalizer_abba_lifecycle import ArmRequest


@dataclass(frozen=True)
class IsolationFixture:
    """Bind one owner and stale packed-finalizer attempt to disposable state."""

    dsn: str
    schema_name: str
    snapshot_key: int
    owner_token: str
    stale_token: str
    owner_work: Path
    stale_work: Path
    owner_artifacts: BenchmarkArtifacts
    stale_artifacts: BenchmarkArtifacts

    def request(self, *, stale: bool = False) -> ArmRequest:
        return ArmRequest(
            "b2" if stale else "b1",
            True,
            self.schema_name,
            self.snapshot_key,
            self.stale_token if stale else self.owner_token,
            self.stale_work if stale else self.owner_work,
            self.stale_artifacts if stale else self.owner_artifacts,
        )


def finalizer_manifest() -> dict[str, object]:
    """Return the exact packed-finalizer manifest used by lifecycle tests."""

    return {
        "contract": PTG2_V4_FINALIZER_MAP_CONTRACT,
        "map_format": snapshot_maps.PTG2_V4_MAP_FORMAT,
        "map_digest": (b"d" * 32).hex(),
        "object_kinds": list(PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS),
        "object_kind_count": len(PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS),
        "map_pack_count": 6,
        "coordinate_count": 6,
        "entry_count": 9,
        "logical_byte_count": 12,
        "stored_map_byte_count": 600,
        "target_block_count": 6,
        "canonical_mapping_digest": (b"c" * 32).hex(),
        "canonical_byte_count": 640,
        "target_identity_digest": (b"t" * 32).hex(),
    }


def finalizer_root_fields() -> dict[str, object]:
    """Return the matching sealed root row used by lifecycle tests."""

    manifest = finalizer_manifest()
    return {
        "state": "sealed",
        "generation": snapshot_maps.PTG2_V4_SHARED_GENERATION,
        "finalizer_root_present": True,
        "finalizer_root_state": "complete",
        "finalizer_root_contract": PTG2_V4_FINALIZER_MAP_CONTRACT,
        "finalizer_root_map_format": snapshot_maps.PTG2_V4_MAP_FORMAT,
        "finalizer_root_map_digest": b"d" * 32,
        "finalizer_root_canonical_mapping_digest": b"c" * 32,
        "finalizer_root_canonical_byte_count": 640,
        "finalizer_root_target_identity_digest": b"t" * 32,
        "finalizer_root_completed_at": object(),
        "finalizer_relational_mapping_present": False,
        **{
            f"finalizer_root_{field_name}": manifest[field_name]
            for field_name in (
                "object_kind_count",
                "map_pack_count",
                "coordinate_count",
                "entry_count",
                "logical_byte_count",
                "stored_map_byte_count",
                "target_block_count",
            )
        },
    }
