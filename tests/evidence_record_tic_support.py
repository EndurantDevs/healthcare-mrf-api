from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from tempfile import TemporaryDirectory

from process import evidence_source_release_contract as releases
from process.ptg_parts import ptg2_tax_identity_shadow_admission as admission
from tests.ptg2_tax_identity_shadow_admission_support import (
    POLICY_ID,
    make_sidecar_pair,
    refresh_descriptor,
)

GROUP_ID = (1).to_bytes(16, "big")
NPI_GROUP_ID = (2).to_bytes(16, "big")
UNAVAILABLE_GROUP_ID = (3).to_bytes(16, "big")
_TEMPORARY_ROOT = TemporaryDirectory(prefix="evidence-record-tic-")


def v2_sidecar_bytes(policy_id: str = POLICY_ID) -> bytes:
    policy = policy_id.encode("ascii")
    header = b"PTG2TAX2" + (2).to_bytes(2, "little") + (65).to_bytes(2, "little")
    header += bytes((len(policy),)) + policy
    states_and_tokens = (
        (1, bytes.fromhex("11" * 32)),
        (5, bytes.fromhex("22" * 32)),
        (2, bytes(32)),
        (3, bytes(32)),
        (3, bytes(32)),
        (4, bytes(32)),
    )
    records = []
    for group_number, (state, full_hmac) in enumerate(states_and_tokens, start=1):
        group_id = group_number.to_bytes(16, "big")
        records.append(group_id + bytes((state,)) + full_hmac[:16] + full_hmac)
    return header + b"".join(records)


def build_tic_release(binding_sha256: str):
    policy = releases._SOURCE_POLICIES["tic"]
    return releases.build_public_evidence_source_release(
        {
            "source_kind": "tic",
            "authority_classification": policy.authority,
            "trust_classification": policy.trust,
            "semantic_limits": policy.semantic_limits,
            "artifact_identity": releases.ImmutablePublicSourceIdentity(
                policy.identity_kind, "public-tic-synthetic", "a" * 64
            ),
            "completeness_proof": releases.PublicEvidenceCompletenessProof(
                policy.completeness_mode, 6, 6, "b" * 64
            ),
            "rights_classification": policy.rights,
            "rights_proof_sha256": "c" * 64,
            "source_binding": releases.OpaqueSourceBindingReference(
                releases.TAX_IDENTITY_SHADOW_SOURCE_BINDING_CONTRACT,
                binding_sha256,
            ),
            "observed_interval": releases.CanonicalUtcInterval(
                "2026-07-01T00:00:00Z", "2026-07-02T00:00:00Z"
            ),
            "effective_interval": releases.CanonicalUtcInterval(
                "2026-07-01T00:00:00Z", None
            ),
            "import_run_id": "public-import-synthetic-tic",
            "source_release_id": "public-release-synthetic-tic",
            "artifact_bytes_verified": True,
            "public_access_verified": True,
            "processing_retention_rights_verified": True,
            "semantic_limits_verified": True,
            "completeness_verified": True,
            "legal_ownership_claimed": False,
            "exact_rate_site_claimed": False,
            "redistribution_enabled": False,
            "export_enabled": False,
            "publication_enabled": False,
            "replacement_enabled": False,
        }
    )


def make_tic_material(
    tmp_path: Path, *, v2_bytes: bytes | None = None, directory_name: str = "shadow"
):
    root, v1, v2 = make_sidecar_pair(tmp_path, directory_name=directory_name)
    Path(v2["path"]).write_bytes(v2_sidecar_bytes() if v2_bytes is None else v2_bytes)
    refresh_descriptor(v2)
    bundle = admission.admit_tax_identity_shadow_bundle(
        scratch_root=root, v1_scanner_descriptor=v1, v2_scanner_descriptor=v2
    )
    return root, bundle, build_tic_release(bundle.binding_sha256)


@dataclass(frozen=True, slots=True)
class SyntheticTicMaterial:
    scratch_root: Path
    bundle: admission.TaxIdentityShadowBundleDescriptor
    release: releases.PublicEvidenceSourceReleaseDescriptor


@lru_cache(maxsize=1)
def synthetic_tic_material() -> SyntheticTicMaterial:
    scratch = Path(_TEMPORARY_ROOT.name)
    root, bundle, release = make_tic_material(scratch)
    return SyntheticTicMaterial(root, bundle, release)
