# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Content identity and logical ownership for reusable strict PTG V3 layouts."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from process.ptg_parts.canonical import _canonicalize_for_json
from process.ptg_parts.domain import PTG2DownloadedJob
from process.ptg_parts.ptg2_shared_blocks import (
    PTG2_V3_COLD_LOOKUP_CONTRACT,
    PTG2_V3_PRICE_MEMBERSHIP_SEMANTICS,
    PTG2_V3_SERVING_MULTIPLICITY_SEMANTICS,
    PTG2_V3_SHARED_GENERATION,
    shared_semantic_fingerprint,
)
from process.ptg_parts.ptg2_shared_source_set import (
    PTG2_V3_SOURCE_SET_CONTRACT,
    _normalized_sha256,
    shared_source_set_metadata,
)
from process.ptg_parts.source_files import _derive_plan_fields
from process.ptg_parts.source_jobs import _normalize_plan_payload
from process.ptg_parts.values import build_source_trace_set


_PHYSICAL_SOURCE_TYPE_RE = re.compile(
    r"[a-z0-9][a-z0-9._-]{0,63}",
    flags=re.ASCII,
)
_PHYSICAL_IDENTITY_KINDS = frozenset(
    {"logical_json_sha256_v1", "raw_container_sha256_v1"}
)
_PHYSICAL_OPTION_KEYS: tuple[str, ...] = ()


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
class SharedLogicalPlanScope:
    plan_id: str
    plan_id_type: str
    plan_market_type: str


@dataclass(frozen=True, order=True)
class SharedPhysicalArtifactIdentity:
    source_type: str
    identity_kind: str
    identity_sha256: str

    def __post_init__(self) -> None:
        source_type = _normalized_physical_source_type(self.source_type)
        identity_kind = str(self.identity_kind or "").strip()
        if identity_kind not in _PHYSICAL_IDENTITY_KINDS:
            raise ValueError("strict shared V3 physical artifact identity is incomplete")
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


@dataclass(frozen=True)
class SharedInputIdentity:
    semantic_fingerprint: bytes
    coverage_scope_id: bytes
    logical_plans: tuple[SharedLogicalPlanScope, ...]
    logical_plan_fields_by_scope: tuple[Mapping[str, Any], ...]
    payload: Mapping[str, Any]
    source_identities: tuple[SharedPhysicalArtifactIdentity, ...]
    artifact_count: int
    identity_byte_count: int

    @property
    def logical_plan(self) -> SharedLogicalPlanScope:
        """Return the deterministic primary plan used for attestation metadata."""

        return self.logical_plans[0]

    @property
    def logical_plan_fields(self) -> Mapping[str, Any]:
        """Return fields for the deterministic primary logical plan."""

        return self.logical_plan_fields_by_scope[0]

    @property
    def logical_plan_count(self) -> int:
        """Return the number of logical plans bound to this physical input."""

        return len(self.logical_plans)

    @property
    def coverage_scope_hex(self) -> str:
        """Return the coverage scope ID as lowercase hexadecimal."""

        return self.coverage_scope_id.hex()

    @property
    def source_count(self) -> int:
        """Return the number of distinct physical source identities."""

        return len(self.source_identities)


def _logical_plan_scope(plan_fields: Mapping[str, Any]) -> SharedLogicalPlanScope:
    """Return one normalized logical plan scope from canonical plan fields."""

    plan_id = str(plan_fields.get("plan_id") or "").strip()
    if not plan_id:
        raise ValueError("strict shared V3 input is missing a logical plan id")
    return SharedLogicalPlanScope(
        plan_id=plan_id,
        plan_id_type=str(plan_fields.get("plan_id_type") or "").strip().lower(),
        plan_market_type=str(
            plan_fields.get("plan_market_type") or ""
        ).strip().lower(),
    )


def logical_plan_fields_for_job(
    job: Mapping[str, Any],
) -> tuple[dict[str, Any], ...]:
    """Return every distinct logical plan represented by one physical-file job."""

    meta = job.get("meta") if isinstance(job.get("meta"), dict) else {}
    plan_info = (
        job.get("plan_info")
        if isinstance(job.get("plan_info"), list)
        else []
    )
    normalized_plans = [
        _normalize_plan_payload(plan)
        for plan in plan_info
        if isinstance(plan, dict)
    ]
    metadata_plan = _derive_plan_fields(meta, None)
    metadata_plan_id = str(metadata_plan.get("plan_id") or "").strip()
    if metadata_plan_id:
        matching_plans = [
            plan
            for plan in normalized_plans
            if str(plan.get("plan_id") or "").strip().casefold()
            == metadata_plan_id.casefold()
        ]
        return (
            dict(_derive_plan_fields(meta, matching_plans or None)),
        )

    plans_by_scope: dict[
        tuple[str, str, str],
        list[dict[str, Any]],
    ] = {}
    canonical_id_by_scope: dict[tuple[str, str, str], str] = {}
    for plan in normalized_plans:
        plan_id = str(plan.get("plan_id") or "").strip()
        if not plan_id:
            continue
        scope_key = (
            plan_id.casefold(),
            str(plan.get("plan_id_type") or "").strip().lower(),
            str(plan.get("plan_market_type") or "").strip().lower(),
        )
        canonical_id_by_scope.setdefault(scope_key, plan_id)
        plans_by_scope.setdefault(scope_key, []).append(plan)

    logical_plan_fields: list[dict[str, Any]] = []
    for scope_key in sorted(plans_by_scope):
        plan_fields = dict(_derive_plan_fields({}, plans_by_scope[scope_key]))
        plan_fields["plan_id"] = canonical_id_by_scope[scope_key]
        logical_plan_fields.append(plan_fields)
    return tuple(logical_plan_fields)


def _merged_plan_fields(
    plan_fields_values: Iterable[Mapping[str, Any]],
    scope: SharedLogicalPlanScope,
) -> dict[str, Any]:
    """Merge repeated metadata for one plan without conflating other plans."""

    values = [dict(plan_fields) for plan_fields in plan_fields_values]
    merged = dict(_derive_plan_fields({}, values))
    merged.update(
        {
            "plan_id": scope.plan_id,
            "plan_id_type": scope.plan_id_type or None,
            "plan_market_type": scope.plan_market_type or None,
        }
    )
    return merged


def _downloaded_artifact_payload(downloaded: PTG2DownloadedJob) -> dict[str, Any]:
    if downloaded.error:
        raise ValueError(f"strict shared V3 input download failed: {downloaded.error}")
    if downloaded.raw_artifact is None or downloaded.logical_artifact is None:
        raise ValueError("strict shared V3 input download did not produce both artifacts")
    raw = downloaded.raw_artifact
    logical = downloaded.logical_artifact
    raw_sha256 = _normalized_sha256(raw.raw_sha256, field_name="raw_sha256")
    logical_sha256 = _normalized_sha256(
        logical.logical_sha256,
        field_name="logical_sha256",
    )
    logical_hash_deferred = bool(logical.logical_hash_deferred)
    if logical_hash_deferred and (
        logical_sha256 != raw_sha256
        or int(logical.byte_count) != int(raw.byte_count)
        or not logical.compression
    ):
        raise ValueError("strict shared V3 deferred logical identity metadata is inconsistent")
    # Large compressed files may defer the decompressed hash so import planning
    # does not add a full decompression pass. In that case reuse is deliberately
    # limited to byte-identical containers. Once a real logical digest is known,
    # differently wrapped containers can share the same physical layout.
    return {
        "source_type": _normalized_physical_source_type(downloaded.job.get("type")),
        "identity_kind": (
            "raw_container_sha256_v1"
            if logical_hash_deferred
            else "logical_json_sha256_v1"
        ),
        "identity_sha256": raw_sha256 if logical_hash_deferred else logical_sha256,
        "identity_byte_count": (
            int(raw.byte_count) if logical_hash_deferred else int(logical.byte_count)
        ),
    }


def shared_physical_artifact_identity(
    downloaded: PTG2DownloadedJob,
) -> SharedPhysicalArtifactIdentity:
    """Derive the reusable physical identity for a validated downloaded artifact."""

    payload = _downloaded_artifact_payload(downloaded)
    return SharedPhysicalArtifactIdentity(
        source_type=str(payload["source_type"]),
        identity_kind=str(payload["identity_kind"]),
        identity_sha256=str(payload["identity_sha256"]),
    )


def shared_logical_artifact_metadata(downloaded: PTG2DownloadedJob) -> dict[str, Any]:
    """Return raw and logical digest metadata while preserving deferred-hash state."""

    if downloaded.raw_artifact is None or downloaded.logical_artifact is None:
        raise ValueError("strict shared V3 logical source is missing artifact metadata")
    raw_sha256 = _normalized_sha256(
        downloaded.raw_artifact.raw_sha256,
        field_name="raw_container_sha256",
    )
    deferred = bool(downloaded.logical_artifact.logical_hash_deferred)
    logical_sha256 = None
    if not deferred:
        logical_sha256 = _normalized_sha256(
            downloaded.logical_artifact.logical_sha256,
            field_name="logical_json_sha256",
        )
    return {
        "raw_container_sha256": raw_sha256,
        "logical_json_sha256": logical_sha256,
        "logical_hash_deferred": deferred,
    }


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
        raise ValueError("strict shared V3 physical artifact identity is incomplete")
    return SharedPhysicalArtifactIdentity(
        source_type=source_type,
        identity_kind=identity_kind,
        identity_sha256=identity_sha256,
    )


def deterministic_source_key_assignments(
    identities: Iterable[Mapping[str, Any] | SharedPhysicalArtifactIdentity],
) -> tuple[tuple[int, SharedPhysicalArtifactIdentity], ...]:
    """Deduplicate, sort, and assign contiguous source keys starting at zero."""

    distinct = tuple(sorted({normalized_physical_artifact_identity(value) for value in identities}))
    if not distinct:
        raise ValueError("strict shared V3 source-key assignment requires an artifact")
    return tuple(enumerate(distinct))


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

    provenance_by_identity: dict[
        SharedPhysicalArtifactIdentity, dict[str, set[Any]]
    ] = {}
    for entry in source_provenance_entries:
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
        raw_logical_sha256 = entry.get("logical_json_sha256")
        logical_json_sha256 = (
            None
            if logical_hash_deferred
            else _normalized_sha256(
                raw_logical_sha256,
                field_name="logical_json_sha256",
            )
        )
        if identity.identity_kind == "logical_json_sha256_v1" and (
            logical_hash_deferred
            or logical_json_sha256 != identity.identity_sha256
        ):
            raise ValueError(
                "strict shared V3 logical source metadata disagrees with its physical identity"
            )
        if identity.identity_kind == "raw_container_sha256_v1" and (
            not logical_hash_deferred
            or raw_container_sha256 != identity.identity_sha256
        ):
            raise ValueError(
                "strict shared V3 deferred source metadata disagrees with its physical identity"
            )
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
    dense = deterministic_source_key_assignments(expected_identities)
    expected = {identity for _source_key, identity in dense}
    unexpected = set(provenance_by_identity) - expected
    missing = expected - set(provenance_by_identity)
    if unexpected or missing:
        raise ValueError(
            "strict shared V3 logical source traces do not match the complete physical input set"
        )

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
                "strict shared V3 one physical source key has ambiguous artifact metadata"
            )
        trace_hashes = tuple(sorted(grouped["trace_hashes"]))
        trace_set = build_source_trace_set(trace_hashes)
        trace_set_rows.append(dict(trace_set))
        assignments.append(
            SharedSnapshotSourceAssignment(
                source_key=source_key,
                identity=identity,
                source_trace_set_hash=str(trace_set["source_trace_set_hash"]),
                source_trace_hashes=trace_hashes,
                raw_container_sha256=str(
                    next(iter(grouped["raw_container_sha256"]))
                ),
                logical_json_sha256=next(iter(grouped["logical_json_sha256"])),
                logical_hash_deferred=bool(
                    next(iter(grouped["logical_hash_deferred"]))
                ),
            )
        )
    return tuple(assignments), tuple(trace_set_rows)


def _distinct_artifact_payloads(
    downloaded_jobs: Iterable[PTG2DownloadedJob],
) -> tuple[
    list[dict[str, Any]],
    tuple[SharedLogicalPlanScope, ...],
    tuple[dict[str, Any], ...],
]:
    artifact_by_identity: dict[tuple[str, str, str], dict[str, Any]] = {}
    plan_fields_by_scope: dict[
        SharedLogicalPlanScope, list[Mapping[str, Any]]
    ] = {}
    for downloaded in downloaded_jobs:
        artifact = _downloaded_artifact_payload(downloaded)
        identity = (
            str(artifact["source_type"]),
            str(artifact["identity_kind"]),
            str(artifact["identity_sha256"]),
        )
        previous = artifact_by_identity.setdefault(identity, artifact)
        if previous != artifact:
            raise ValueError("strict shared V3 logical artifact metadata is inconsistent")
        for plan_fields in logical_plan_fields_for_job(downloaded.job):
            scope = _logical_plan_scope(plan_fields)
            plan_fields_by_scope.setdefault(scope, []).append(plan_fields)
    if not artifact_by_identity:
        raise ValueError("strict shared V3 layout requires at least one downloaded artifact")
    if not plan_fields_by_scope:
        raise ValueError("strict shared V3 input is missing logical plan metadata")
    logical_plans = tuple(sorted(plan_fields_by_scope))
    logical_plan_fields = tuple(
        _merged_plan_fields(plan_fields_by_scope[scope], scope)
        for scope in logical_plans
    )
    artifacts = sorted(
        artifact_by_identity.values(),
        key=lambda item: (
            str(item["source_type"]),
            str(item["identity_kind"]),
            str(item["identity_sha256"]),
        ),
    )
    return artifacts, logical_plans, logical_plan_fields


def shared_physical_input_identity(
    downloaded_jobs: Iterable[PTG2DownloadedJob],
    *,
    options: Mapping[str, Any],
    scanner_canon_version: Mapping[str, Any] | str,
) -> SharedInputIdentity:
    """Fingerprint physical content while retaining logical ownership separately."""

    artifacts, logical_plans, logical_plan_fields = _distinct_artifact_payloads(
        downloaded_jobs
    )
    source_identities = tuple(
        normalized_physical_artifact_identity(artifact) for artifact in artifacts
    )
    coverage_scope_payload = {
        "coverage_scope_version": 3,
        "artifacts": artifacts,
    }
    coverage_scope_id = shared_semantic_fingerprint(coverage_scope_payload)
    physical_options = {
        key: _canonicalize_for_json(options.get(key))
        for key in _PHYSICAL_OPTION_KEYS
    }
    payload = {
        "identity_version": 6,
        "storage_generation": PTG2_V3_SHARED_GENERATION,
        "cold_lookup_contract": PTG2_V3_COLD_LOOKUP_CONTRACT,
        "price_membership_semantics": PTG2_V3_PRICE_MEMBERSHIP_SEMANTICS,
        "serving_multiplicity_semantics": PTG2_V3_SERVING_MULTIPLICITY_SEMANTICS,
        "scanner_canon_version": _canonicalize_for_json(scanner_canon_version),
        "physical_options": physical_options,
        "coverage_scope_id": coverage_scope_id.hex(),
        "artifacts": artifacts,
    }
    return SharedInputIdentity(
        semantic_fingerprint=shared_semantic_fingerprint(payload),
        coverage_scope_id=coverage_scope_id,
        logical_plans=logical_plans,
        logical_plan_fields_by_scope=logical_plan_fields,
        payload=payload,
        source_identities=source_identities,
        artifact_count=len(artifacts),
        identity_byte_count=sum(int(item["identity_byte_count"]) for item in artifacts),
    )


def same_downloaded_physical_input(
    left: PTG2DownloadedJob,
    right: PTG2DownloadedJob,
) -> bool:
    """Return true when two jobs decode to the same physical source content."""

    return _downloaded_artifact_payload(left) == _downloaded_artifact_payload(right)


__all__ = [
    "PTG2_V3_SOURCE_SET_CONTRACT",
    "SharedInputIdentity",
    "SharedLogicalPlanScope",
    "SharedPhysicalArtifactIdentity",
    "SharedSnapshotSourceAssignment",
    "deterministic_source_key_assignments",
    "normalized_physical_artifact_identity",
    "logical_plan_fields_for_job",
    "same_downloaded_physical_input",
    "shared_physical_artifact_identity",
    "shared_logical_artifact_metadata",
    "shared_physical_input_identity",
    "shared_snapshot_source_assignments",
    "shared_source_set_metadata",
]
