from __future__ import annotations

import hashlib
import json
import struct
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Iterable

import pytest

from api import ptg2_v4_graph as graph
from api import ptg2_v4_intersection as intersection
from api.ptg2_shared_blocks import PTG2SharedBlockError
from process.ptg_parts import ptg2_v4_audit as audit
from process.ptg_parts import ptg2_v4_graph_compiler as compiler
from process.ptg_parts import ptg2_v4_snapshot_maps as snapshot_maps
from process.ptg_parts.ptg2_shared_audit import AuditCandidate, _ReadBudget
from process.ptg_parts.ptg2_shared_blocks import (
    PTG2_V3_SHARED_FORMAT_VERSION,
    SharedBlock,
    SharedBlockReference,
    SharedLayoutBuildOwnership,
    shared_block_hash,
)


class _Result:
    def __init__(
        self,
        rows: Iterable[Any] = (),
        *,
        scalar: Any = None,
    ) -> None:
        self.rows = list(rows)
        self.scalar_value = scalar

    def __iter__(self):
        return iter(self.rows)

    def first(self):
        return self.rows[0] if self.rows else None

    def one(self):
        if len(self.rows) != 1:
            raise AssertionError(f"expected one row, observed {len(self.rows)}")
        return self.rows[0]

    def scalar(self):
        return self.scalar_value


class _ScriptedSession:
    def __init__(self, *responses: _Result) -> None:
        self.responses = list(responses)
        self.calls: list[tuple[str, Any]] = []

    async def execute(self, statement, parameters=None):
        self.calls.append((str(statement), parameters))
        if not self.responses:
            raise AssertionError(f"unexpected SQL: {statement}")
        return self.responses.pop(0)


def _reference(
    object_kind: str,
    block_key: int,
    fragment_no: int,
    *,
    payload: bytes | None = None,
    entry_count: int = 1,
) -> SharedBlockReference:
    raw_payload = payload if payload is not None else bytes((block_key + 1,)) * 4
    return SharedBlockReference(
        object_kind=object_kind,
        block_key=block_key,
        fragment_no=fragment_no,
        entry_count=entry_count,
        block_hash=shared_block_hash(
            format_version=PTG2_V3_SHARED_FORMAT_VERSION,
            object_kind=object_kind,
            codec="none",
            payload=raw_payload,
        ),
        raw_byte_count=len(raw_payload),
    )


def _summary(
    *,
    digest: bytes = b"d" * 32,
    object_kinds: tuple[str, ...] = ("v4_relation_members_v1",),
) -> snapshot_maps.V4SnapshotMapSummary:
    return snapshot_maps.V4SnapshotMapSummary(
        map_digest=digest,
        object_kinds=object_kinds,
        map_pack_count=1,
        coordinate_count=1,
        entry_count=2,
        logical_byte_count=8,
        stored_map_byte_count=136,
    )


def _metadata(
    *,
    npi_count: int = 1,
    component_count: int = 1,
    pattern_count: int = 1,
    relation_count: int = 1,
    heavy_owner_count: int = 0,
) -> snapshot_maps.V4SnapshotMetadataSummary:
    return snapshot_maps.V4SnapshotMetadataSummary(
        npi_count=npi_count,
        component_count=component_count,
        pattern_count=pattern_count,
        relation_count=relation_count,
        heavy_owner_count=heavy_owner_count,
        provider_graph_resources={
            "compressed_acquisition_bytes": 1024,
            "input_factor_bytes": 512,
            "factor_edge_count": 9,
            "empty_npi_tin_only_normalization_count": 0,
        },
    )


def _relation_row(relation: str = "group_patterns") -> dict[str, Any]:
    return {
        "relation": relation,
        "member_object_kind": f"v4_{relation}_members_v1",
        "locator_object_kind": f"v4_{relation}_locators_v1",
        "owner_base": 0,
        "owner_count": 2,
        "logical_member_count": 3,
        "vector_member_count": 3,
        "member_width": 4,
        "member_page_bytes": 16,
        "locator_page_bytes": 24,
        "locator_owner_span": 2,
    }


def _owner_row(
    relation: str = "group_patterns",
    owner_key: int = 1,
) -> dict[str, Any]:
    return {
        "relation": relation,
        "owner_key": owner_key,
        "object_kind": f"v4_{relation}_heavy_bitmap_v1",
        "member_count": 2,
        "member_base": 10,
        "member_span": 8,
        "fragment_count": 1,
    }



__all__ = [
    "Any",
    "AuditCandidate",
    "Iterable",
    "PTG2SharedBlockError",
    "PTG2_V3_SHARED_FORMAT_VERSION",
    "Path",
    "SharedBlock",
    "SharedBlockReference",
    "SharedLayoutBuildOwnership",
    "SimpleNamespace",
    "_ReadBudget",
    "_Result",
    "_ScriptedSession",
    "_metadata",
    "_owner_row",
    "_reference",
    "_relation_row",
    "_summary",
    "asynccontextmanager",
    "audit",
    "compiler",
    "graph",
    "hashlib",
    "intersection",
    "json",
    "pytest",
    "shared_block_hash",
    "snapshot_maps",
    "struct",
]
