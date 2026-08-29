# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Dense physical-source identities for reusable strict PTG V3 layouts."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from process.ptg_parts.ptg2_shared_source_set import _normalized_sha256
from process.ptg_parts.values import build_source_trace_set


_PHYSICAL_SOURCE_TYPE_RE = re.compile(
    r"[a-z0-9][a-z0-9._-]{0,63}",
    flags=re.ASCII,
)
_PHYSICAL_IDENTITY_KINDS = frozenset(
    {"logical_json_sha256_v1", "raw_container_sha256_v1"}
)


def _normalized_physical_source_type(value: Any) -> str:
    """Return the canonical ASCII token used in Python and Rust identities."""

    raw_value = str(value or "").strip()
    normalized = raw_value.lower() if raw_value.isascii() else ""
    if not _PHYSICAL_SOURCE_TYPE_RE.fullmatch(normalized):
        raise ValueError(
            "strict shared V3 physical source_type must be a nonempty lowercase "
            "ASCII token of at most 64 bytes"
        )
    return normalized


@dataclass(frozen=True, order=True)
class SharedPhysicalArtifactIdentity:
    source_type: str
    identity_kind: str
    identity_sha256: str

    def __post_init__(self) -> None:
        source_type = _normalized_physical_source_type(self.source_type)
        identity_kind = str(self.identity_kind or "").strip()
        if identity_kind not in _PHYSICAL_IDENTITY_KINDS:
            raise ValueError(
                "strict shared V3 physical artifact identity is incomplete"
            )
        identity_sha256 = _normalized_sha256(
            self.identity_sha256,
            field_name="identity_sha256",
        )
        object.__setattr__(self, "source_type", source_type)
        object.__setattr__(self, "identity_kind", identity_kind)
        object.__setattr__(self, "identity_sha256", identity_sha256)

    def as_dict(self) -> dict[str, str]:
        """Return this physical artifact identity in manifest form."""

        return {
            "source_type": self.source_type,
            "identity_kind": self.identity_kind,
            "identity_sha256": self.identity_sha256,
        }


@dataclass(frozen=True)
class SharedSnapshotSourceAssignment:
    source_key: int
    identity: SharedPhysicalArtifactIdentity
    source_trace_set_hash: str
    source_trace_hashes: tuple[str, ...]
    raw_container_sha256: str
    logical_json_sha256: str | None
    logical_hash_deferred: bool


def normalized_physical_artifact_identity(
    value: Mapping[str, Any] | SharedPhysicalArtifactIdentity,
) -> SharedPhysicalArtifactIdentity:
    """Return a physical identity, validating and normalizing mapping inputs."""

    if isinstance(value, SharedPhysicalArtifactIdentity):
        return value
    source_type = _normalized_physical_source_type(value.get("source_type"))
    identity_kind = str(value.get("identity_kind") or "").strip()
    identity_sha256 = _normalized_sha256(
        value.get("identity_sha256"),
        field_name="identity_sha256",
    )
    if identity_kind not in _PHYSICAL_IDENTITY_KINDS:
        raise ValueError(
            "strict shared V3 physical artifact identity is incomplete"
        )
    return SharedPhysicalArtifactIdentity(
        source_type=source_type,
        identity_kind=identity_kind,
        identity_sha256=identity_sha256,
    )


def deterministic_source_key_assignments(
    identities: Iterable[
        Mapping[str, Any] | SharedPhysicalArtifactIdentity
    ],
) -> tuple[tuple[int, SharedPhysicalArtifactIdentity], ...]:
    """Deduplicate, sort, and assign contiguous source keys starting at zero."""

    distinct_identities = tuple(
        sorted(
            {
                normalized_physical_artifact_identity(value)
                for value in identities
            }
        )
    )
    if not distinct_identities:
        raise ValueError(
            "strict shared V3 source-key assignment requires an artifact"
        )
    return tuple(enumerate(distinct_identities))


def _normalized_source_provenance(
    entry: Mapping[str, Any],
) -> tuple[SharedPhysicalArtifactIdentity, str, str, str | None, bool]:
    """Validate one logical trace against its physical artifact identity."""

    identity = normalized_physical_artifact_identity(entry)
    trace_hash = _normalized_sha256(
        entry.get("source_trace_hash"),
        field_name="source_trace_hash",
    )
    raw_container_sha256 = _normalized_sha256(
        entry.get("raw_container_sha256"),
        field_name="raw_container_sha256",
    )
    logical_hash_deferred = bool(entry.get("logical_hash_deferred"))
    logical_json_sha256 = (
        None
        if logical_hash_deferred
        else _normalized_sha256(
            entry.get("logical_json_sha256"),
            field_name="logical_json_sha256",
        )
    )
    if identity.identity_kind == "logical_json_sha256_v1" and (
        logical_hash_deferred
        or logical_json_sha256 != identity.identity_sha256
    ):
        raise ValueError(
            "strict shared V3 logical source metadata disagrees with its "
            "physical identity"
        )
    if identity.identity_kind == "raw_container_sha256_v1" and (
        not logical_hash_deferred
        or raw_container_sha256 != identity.identity_sha256
    ):
        raise ValueError(
            "strict shared V3 deferred source metadata disagrees with its "
            "physical identity"
        )
    return (
        identity,
        trace_hash,
        raw_container_sha256,
        logical_json_sha256,
        logical_hash_deferred,
    )


def _grouped_source_provenance(
    source_provenance_entries: Iterable[Mapping[str, Any]],
) -> dict[SharedPhysicalArtifactIdentity, dict[str, set[Any]]]:
    """Group exact logical provenance by physical artifact identity."""

    provenance_by_identity: dict[
        SharedPhysicalArtifactIdentity, dict[str, set[Any]]
    ] = {}
    for entry in source_provenance_entries:
        (
            identity,
            trace_hash,
            raw_container_sha256,
            logical_json_sha256,
            logical_hash_deferred,
        ) = _normalized_source_provenance(entry)
        grouped = provenance_by_identity.setdefault(
            identity,
            {
                "trace_hashes": set(),
                "raw_container_sha256": set(),
                "logical_json_sha256": set(),
                "logical_hash_deferred": set(),
            },
        )
        grouped["trace_hashes"].add(trace_hash)
        grouped["raw_container_sha256"].add(raw_container_sha256)
        grouped["logical_json_sha256"].add(logical_json_sha256)
        grouped["logical_hash_deferred"].add(logical_hash_deferred)
    return provenance_by_identity


def _source_assignments(
    dense: tuple[tuple[int, SharedPhysicalArtifactIdentity], ...],
    provenance_by_identity: Mapping[
        SharedPhysicalArtifactIdentity, Mapping[str, set[Any]]
    ],
) -> tuple[
    tuple[SharedSnapshotSourceAssignment, ...],
    tuple[dict[str, Any], ...],
]:
    """Construct the dense source rows from exact grouped provenance."""

    assignments: list[SharedSnapshotSourceAssignment] = []
    trace_set_rows: list[dict[str, Any]] = []
    for source_key, identity in dense:
        grouped = provenance_by_identity[identity]
        if (
            len(grouped["raw_container_sha256"]) != 1
            or len(grouped["logical_json_sha256"]) != 1
            or len(grouped["logical_hash_deferred"]) != 1
        ):
            raise ValueError(
                "strict shared V3 one physical source key has ambiguous "
                "artifact metadata"
            )
        trace_hashes = tuple(sorted(grouped["trace_hashes"]))
        trace_set = build_source_trace_set(trace_hashes)
        trace_set_rows.append(dict(trace_set))
        assignments.append(
            SharedSnapshotSourceAssignment(
                source_key=source_key,
                identity=identity,
                source_trace_set_hash=str(
                    trace_set["source_trace_set_hash"]
                ),
                source_trace_hashes=trace_hashes,
                raw_container_sha256=str(
                    next(iter(grouped["raw_container_sha256"]))
                ),
                logical_json_sha256=next(
                    iter(grouped["logical_json_sha256"])
                ),
                logical_hash_deferred=bool(
                    next(iter(grouped["logical_hash_deferred"]))
                ),
            )
        )
    return tuple(assignments), tuple(trace_set_rows)


def shared_snapshot_source_assignments(
    source_provenance_entries: Iterable[Mapping[str, Any]],
    *,
    expected_identities: Iterable[
        Mapping[str, Any] | SharedPhysicalArtifactIdentity
    ],
) -> tuple[
    tuple[SharedSnapshotSourceAssignment, ...],
    tuple[dict[str, Any], ...],
]:
    """Build one dense physical-key mapping to a complete logical trace set."""

    provenance_by_identity = _grouped_source_provenance(
        source_provenance_entries
    )
    dense = deterministic_source_key_assignments(expected_identities)
    expected_identity_set = {identity for _source_key, identity in dense}
    if set(provenance_by_identity) != expected_identity_set:
        raise ValueError(
            "strict shared V3 logical source traces do not match the complete "
            "physical input set"
        )
    return _source_assignments(dense, provenance_by_identity)
