# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Content identity and logical ownership for reusable strict PTG V3 layouts."""

from __future__ import annotations

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
from process.ptg_parts.ptg2_invalid_price_exclusion import (
    INVALID_PRICE_EXCLUSION_POLICY_FIELD,
    validate_invalid_price_exclusion_policy,
)
from process.ptg_parts.ptg2_shared_source_set import (
    PTG2_V3_SOURCE_SET_CONTRACT,
    _normalized_sha256,
    shared_source_set_metadata,
)
from process.ptg_parts.ptg2_shared_source_assignments import (
    SharedPhysicalArtifactIdentity,
    SharedSnapshotSourceAssignment,
    _normalized_physical_source_type,
    deterministic_source_key_assignments,
    normalized_physical_artifact_identity,
    shared_snapshot_source_assignments,
)
from process.ptg_parts.source_files import _derive_plan_fields
from process.ptg_parts.source_jobs import _normalize_plan_payload
_PHYSICAL_OPTION_KEYS: tuple[str, ...] = ()
_FULL_REBUILD_SCOPE_DIGEST_OPTION = "full_rebuild_scope_digest"


def normalized_full_rebuild_scope_digest(value: Any) -> str | None:
    """Return the optional opaque digest that isolates one forced rebuild."""

    if value is None:
        return None
    return _normalized_sha256(
        value,
        field_name=_FULL_REBUILD_SCOPE_DIGEST_OPTION,
    )


@dataclass(frozen=True, order=True)
class SharedLogicalPlanScope:
    plan_id: str
    plan_id_type: str
    plan_market_type: str


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
        plan_field_map = dict(_derive_plan_fields({}, plans_by_scope[scope_key]))
        plan_field_map["plan_id"] = canonical_id_by_scope[scope_key]
        logical_plan_fields.append(plan_field_map)
    return tuple(logical_plan_fields)


def _merged_plan_fields(
    plan_fields_values: Iterable[Mapping[str, Any]],
    scope: SharedLogicalPlanScope,
) -> dict[str, Any]:
    """Merge repeated metadata for one plan without conflating other plans."""

    values = [dict(plan_fields) for plan_fields in plan_fields_values]
    merged_field_map = dict(_derive_plan_fields({}, values))
    merged_field_map.update(
        {
            "plan_id": scope.plan_id,
            "plan_id_type": scope.plan_id_type or None,
            "plan_market_type": scope.plan_market_type or None,
        }
    )
    return merged_field_map


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

    artifact_metadata_map = _downloaded_artifact_payload(downloaded)
    return SharedPhysicalArtifactIdentity(
        source_type=str(artifact_metadata_map["source_type"]),
        identity_kind=str(artifact_metadata_map["identity_kind"]),
        identity_sha256=str(artifact_metadata_map["identity_sha256"]),
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
    coverage_scope_payload_map = {
        "coverage_scope_version": 3,
        "artifacts": artifacts,
    }
    coverage_scope_id = shared_semantic_fingerprint(coverage_scope_payload_map)
    physical_option_map = {
        key: _canonicalize_for_json(options.get(key))
        for key in _PHYSICAL_OPTION_KEYS
    }
    full_rebuild_scope_digest = normalized_full_rebuild_scope_digest(
        options.get(_FULL_REBUILD_SCOPE_DIGEST_OPTION)
    )
    if full_rebuild_scope_digest is not None:
        physical_option_map[_FULL_REBUILD_SCOPE_DIGEST_OPTION] = full_rebuild_scope_digest
    invalid_price_exclusion = options.get(INVALID_PRICE_EXCLUSION_POLICY_FIELD)
    if invalid_price_exclusion is not None:
        physical_option_map[INVALID_PRICE_EXCLUSION_POLICY_FIELD] = (
            validate_invalid_price_exclusion_policy(invalid_price_exclusion)["sha256"]
        )
    identity_payload_map = {
        "identity_version": 6,
        "storage_generation": PTG2_V3_SHARED_GENERATION,
        "cold_lookup_contract": PTG2_V3_COLD_LOOKUP_CONTRACT,
        "price_membership_semantics": PTG2_V3_PRICE_MEMBERSHIP_SEMANTICS,
        "serving_multiplicity_semantics": PTG2_V3_SERVING_MULTIPLICITY_SEMANTICS,
        "scanner_canon_version": _canonicalize_for_json(scanner_canon_version),
        "physical_options": physical_option_map,
        "coverage_scope_id": coverage_scope_id.hex(),
        "artifacts": artifacts,
    }
    return SharedInputIdentity(
        semantic_fingerprint=shared_semantic_fingerprint(identity_payload_map),
        coverage_scope_id=coverage_scope_id,
        logical_plans=logical_plans,
        logical_plan_fields_by_scope=logical_plan_fields,
        payload=identity_payload_map,
        source_identities=source_identities,
        artifact_count=len(artifacts),
        identity_byte_count=sum(
            int(source_artifact["identity_byte_count"])
            for source_artifact in artifacts
        ),
    )


def is_same_downloaded_physical_input(
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
    "normalized_full_rebuild_scope_digest",
    "normalized_physical_artifact_identity",
    "logical_plan_fields_for_job",
    "is_same_downloaded_physical_input",
    "shared_physical_artifact_identity",
    "shared_logical_artifact_metadata",
    "shared_physical_input_identity",
    "shared_snapshot_source_assignments",
    "shared_source_set_metadata",
]
