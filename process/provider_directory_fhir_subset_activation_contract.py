# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed desired-state contract for reviewed subset activation."""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
import json
import os
from pathlib import Path
import re
from typing import Any, Mapping

from process.provider_directory_fhir_root_policy import (
    POLICY_PENDING_STATUS,
    POLICY_VERIFIED_STATUS,
    ReviewedRootPolicy,
    reviewed_root_policy_from_document,
)
from process.provider_directory_fhir_subset_canonical import canonical_sha256
from process.provider_directory_fhir_subset_identity import (
    subset_activation_source_contract_payload,
)


DEFAULT_REVIEWED_SUBSET_ACTIVATION_MANIFEST = (
    Path(__file__).resolve().parents[1]
    / "specs/provider_directory_reviewed_subset_activation.json"
)
STATE_SYNC_ENABLED_ENV = (
    "HLTHPRT_PROVIDER_DIRECTORY_SUBSET_STATE_SYNC_ENABLED"
)
PENDING_STATUS = "pending_two_matching_reviewed_subset_acquisitions"
VERIFIED_STATUS = "verified_two_matching_reviewed_subset_acquisitions"
ACTIVATION_METADATA_KEY = "provider_directory_reviewed_subset_activation_v1"
ACTIVATION_CONTRACT_VERSION = (
    "provider-directory-reviewed-subset-activation-v1"
)
ACTIVATION_METADATA_KEY_V2 = (
    "provider_directory_reviewed_subset_activation_v2"
)
ACTIVATION_CONTRACT_VERSION_V2 = (
    "provider-directory-reviewed-subset-activation-v2"
)
STATE_SYNC_TIMEOUT_SECONDS = 120
MANIFEST_FIELDS = frozenset(
    {
        "schema_version",
        "importer",
        "operation",
        "desired_candidate_status",
        "evidence",
    }
)
MANIFEST_FIELDS_V2 = MANIFEST_FIELDS | {"root_policy"}
EVIDENCE_FIELDS = frozenset(
    {
        "source_contract_sha256",
        "cutoff",
        "verification_source_scope_sha256",
        "completion_proof_sha256",
    }
)
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class ReviewedSubsetActivationError(RuntimeError):
    """Expose one stable activation-contract failure without private detail."""

    def __init__(self, code: str = "manifest") -> None:
        safe_message_by_code = {
            "busy": "Provider Directory reviewed subset activation is busy",
            "disabled": "Provider Directory reviewed subset activation is disabled",
            "evidence": "Provider Directory reviewed subset evidence is invalid",
            "manifest": "Provider Directory reviewed subset activation manifest is invalid",
            "state": "Provider Directory reviewed subset source state is invalid",
        }
        self.code = code if code in safe_message_by_code else "evidence"
        super().__init__(safe_message_by_code[self.code])


@dataclass(frozen=True, slots=True, repr=False)
class ReviewedSubsetActivationEvidence:
    """Bind a verified desired state to neutral twin-completion evidence."""

    source_contract_sha256: str
    cutoff: str
    verification_source_scope_sha256: str
    completion_proof_sha256: str
    root_policy: ReviewedRootPolicy | None = None

    def evidence_document(self) -> dict[str, str]:
        """Return the neutral manifest evidence without database identities."""

        return {
            "source_contract_sha256": self.source_contract_sha256,
            "cutoff": self.cutoff,
            "verification_source_scope_sha256": (
                self.verification_source_scope_sha256
            ),
            "completion_proof_sha256": self.completion_proof_sha256,
        }


@dataclass(frozen=True, slots=True, repr=False)
class ReviewedSubsetActivationManifest:
    """Represent pending or fully evidence-bound activation desired state."""

    desired_candidate_status: str
    evidence: ReviewedSubsetActivationEvidence | None
    root_policy: ReviewedRootPolicy | None = None

    @property
    def is_verified(self) -> bool:
        """Return whether the manifest authorizes the verified state."""

        expected_status = (
            POLICY_VERIFIED_STATUS
            if self.root_policy is not None
            else VERIFIED_STATUS
        )
        return self.desired_candidate_status == expected_status

    def require_verified_evidence(self) -> ReviewedSubsetActivationEvidence:
        """Fail closed unless the manifest contains complete verified evidence."""

        if (
            not self.is_verified
            or self.evidence is None
            or self.evidence.root_policy != self.root_policy
        ):
            raise ReviewedSubsetActivationError("disabled")
        return self.evidence


@dataclass(frozen=True, slots=True, repr=False)
class ReviewedSubsetActivationSelection:
    """Retain the private exact twin identities selected for one sync."""

    source_id: str
    endpoint_id: str
    campaign_id: str
    baseline_dataset_id: str
    baseline_root_run_id: str
    candidate_dataset_id: str
    candidate_root_run_id: str
    source_contract_sha256: str
    verification_source_scope_sha256: str
    cutoff: str
    completion_proof_sha256: str
    baseline_replay_evidence_sha256: str
    candidate_replay_evidence_sha256: str
    baseline_coverage_sha256: str
    candidate_coverage_sha256: str
    root_policy: ReviewedRootPolicy | None = None

    @property
    def pending_status(self) -> str:
        """Return the status expected before this activation."""

        return (
            POLICY_PENDING_STATUS
            if self.root_policy is not None
            else PENDING_STATUS
        )

    @property
    def verified_status(self) -> str:
        """Return the status written by this activation."""

        return (
            POLICY_VERIFIED_STATUS
            if self.root_policy is not None
            else VERIFIED_STATUS
        )

    @property
    def activation_metadata_key(self) -> str:
        """Return the versioned marker key for this activation."""

        return (
            ACTIVATION_METADATA_KEY_V2
            if self.root_policy is not None
            else ACTIVATION_METADATA_KEY
        )

    def metadata_marker(self) -> dict[str, Any]:
        """Return the closed private database marker for the selected twins."""

        marker_by_field = {
            "contract_version": (
                ACTIVATION_CONTRACT_VERSION_V2
                if self.root_policy is not None
                else ACTIVATION_CONTRACT_VERSION
            ),
            "source_contract_sha256": self.source_contract_sha256,
            "cutoff": self.cutoff,
            "verification_source_scope_sha256": (
                self.verification_source_scope_sha256
            ),
            "completion_proof_sha256": self.completion_proof_sha256,
            "source_id": self.source_id,
            "endpoint_id": self.endpoint_id,
            "verification_campaign_id": self.campaign_id,
            "candidate": {
                "dataset_id": self.candidate_dataset_id,
                "acquisition_root_run_id": self.candidate_root_run_id,
                "replay_evidence_sha256": (
                    self.candidate_replay_evidence_sha256
                ),
                "coverage_sha256": self.candidate_coverage_sha256,
            },
        }
        if self.root_policy is None:
            marker_by_field["baseline"] = {
                "dataset_id": self.baseline_dataset_id,
                "acquisition_root_run_id": self.baseline_root_run_id,
                "replay_evidence_sha256": (
                    self.baseline_replay_evidence_sha256
                ),
                "coverage_sha256": self.baseline_coverage_sha256,
            }
            return marker_by_field
        marker_by_field["root_policy"] = self.root_policy.document()
        if self.root_policy.is_twin_root_required:
            marker_by_field["baseline"] = {
                "dataset_id": self.baseline_dataset_id,
                "acquisition_root_run_id": self.baseline_root_run_id,
                "replay_evidence_sha256": (
                    self.baseline_replay_evidence_sha256
                ),
                "coverage_sha256": self.baseline_coverage_sha256,
            }
        return marker_by_field


@dataclass(frozen=True, slots=True)
class ReviewedSubsetActivationResult:
    """Expose only whether the reviewed desired state changed or already held."""

    activated: bool

    def __post_init__(self) -> None:
        if type(self.activated) is not bool:
            raise ValueError("reviewed subset activation result is invalid")

    @property
    def is_already_applied(self) -> bool:
        """Return whether the exact reviewed state existed before this call."""

        return not self.activated


def _canonical_cutoff(raw_cutoff: object) -> str:
    if type(raw_cutoff) is not str or not raw_cutoff.endswith("Z"):
        raise ValueError("cutoff shape")
    cutoff_at = dt.datetime.fromisoformat(raw_cutoff[:-1] + "+00:00")
    canonical_cutoff = cutoff_at.isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )
    if cutoff_at.tzinfo is None or canonical_cutoff != raw_cutoff:
        raise ValueError("cutoff canonicality")
    return canonical_cutoff


def _sha256(raw_digest: object) -> str:
    if type(raw_digest) is not str or _SHA256_RE.fullmatch(raw_digest) is None:
        raise ValueError("digest shape")
    return raw_digest


def _activation_evidence(raw_evidence: object) -> ReviewedSubsetActivationEvidence:
    if type(raw_evidence) is not dict or set(raw_evidence) != EVIDENCE_FIELDS:
        raise ValueError("evidence shape")
    return ReviewedSubsetActivationEvidence(
        source_contract_sha256=_sha256(
            raw_evidence.get("source_contract_sha256")
        ),
        cutoff=_canonical_cutoff(raw_evidence.get("cutoff")),
        verification_source_scope_sha256=_sha256(
            raw_evidence.get("verification_source_scope_sha256")
        ),
        completion_proof_sha256=_sha256(
            raw_evidence.get("completion_proof_sha256")
        ),
    )


def _validated_manifest_document(
    manifest_by_field: object,
) -> ReviewedSubsetActivationManifest:
    try:
        if type(manifest_by_field) is not dict:
            raise ValueError("manifest shape")
        schema_version = manifest_by_field.get("schema_version")
        is_v2 = type(schema_version) is int and schema_version == 2
        expected_fields = MANIFEST_FIELDS_V2 if is_v2 else MANIFEST_FIELDS
        if (
            set(manifest_by_field) != expected_fields
            or type(schema_version) is not int
            or schema_version not in (1, 2)
            or manifest_by_field.get("importer") != "provider-directory-fhir"
            or manifest_by_field.get("operation")
            != "reviewed-subset-source-state-sync"
        ):
            raise ValueError("manifest shape")
        root_policy = (
            reviewed_root_policy_from_document(
                manifest_by_field.get("root_policy")
            )
            if is_v2
            else None
        )
        desired_status = manifest_by_field.get("desired_candidate_status")
        raw_evidence = manifest_by_field.get("evidence")
        pending_status = (
            POLICY_PENDING_STATUS if is_v2 else PENDING_STATUS
        )
        verified_status = (
            POLICY_VERIFIED_STATUS if is_v2 else VERIFIED_STATUS
        )
        if desired_status == pending_status and raw_evidence is None:
            return ReviewedSubsetActivationManifest(
                desired_status,
                None,
                root_policy,
            )
        if desired_status != verified_status:
            raise ValueError("desired state")
        evidence = _activation_evidence(raw_evidence)
        if root_policy is not None:
            evidence = ReviewedSubsetActivationEvidence(
                **evidence.evidence_document(),
                root_policy=root_policy,
            )
        return ReviewedSubsetActivationManifest(
            desired_status,
            evidence,
            root_policy,
        )
    except (OverflowError, TypeError, ValueError):
        raise ReviewedSubsetActivationError() from None


def _unique_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """Reject duplicate object members at every manifest nesting level."""

    object_by_field: dict[str, Any] = {}
    for field_name, value in pairs:
        if field_name in object_by_field:
            raise ValueError("duplicate manifest field")
        object_by_field[field_name] = value
    return object_by_field


def reviewed_subset_activation_manifest(
    manifest_path: Path = DEFAULT_REVIEWED_SUBSET_ACTIVATION_MANIFEST,
) -> ReviewedSubsetActivationManifest:
    """Read the sole neutral desired-state manifest with a closed schema."""

    try:
        manifest_by_field: Any = json.loads(
            manifest_path.read_text(encoding="utf-8"),
            object_pairs_hook=_unique_json_object,
        )
    except (OSError, UnicodeDecodeError, ValueError):
        raise ReviewedSubsetActivationError() from None
    return _validated_manifest_document(manifest_by_field)


def require_reviewed_subset_state_sync_gate() -> None:
    """Require the exact one-shot gate before any database activity."""

    if os.getenv(STATE_SYNC_ENABLED_ENV, "") != "true":
        raise ReviewedSubsetActivationError("disabled")


def reviewed_subset_source_contract_sha256(
    source_record: dict[str, Any],
) -> str:
    """Hash the DB-reproducible cutoff-neutral activation source contract."""

    return canonical_sha256(
        subset_activation_source_contract_payload(source_record)
    )


def _text(value: object) -> str | None:
    return value if type(value) is str and value and value == value.strip() else None


def _row_mapping(row: object) -> dict[str, Any]:
    if isinstance(row, Mapping):
        return dict(row)
    row_mapping = getattr(row, "_mapping", None)
    if isinstance(row_mapping, Mapping):
        return dict(row_mapping)
    raise ReviewedSubsetActivationError("state")


def _schema_name() -> str:
    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise ReviewedSubsetActivationError("state")
    schema_name = runtime_schema or legacy_schema or "mrf"
    if _IDENTIFIER_RE.fullmatch(schema_name) is None:
        raise ReviewedSubsetActivationError("state")
    return schema_name


def _quoted_relation(table_name: str) -> str:
    schema_name = _schema_name()
    if _IDENTIFIER_RE.fullmatch(table_name) is None:
        raise ReviewedSubsetActivationError("state")
    return f'"{schema_name}"."{table_name}"'
