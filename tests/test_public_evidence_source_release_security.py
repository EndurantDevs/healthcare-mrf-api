# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Privacy, type, and tamper boundaries for public source releases."""

from __future__ import annotations

import ast
import base64
import operator
from pathlib import Path
import subprocess
import sys

import pytest

from public_evidence import source_release_contract as release
from tests.public_evidence_source_release_support import (
    release_input,
    sha256_text,
)


def test_source_policy_registry_is_immutable() -> None:
    with pytest.raises(TypeError):
        operator.setitem(
            release.SOURCE_POLICIES,
            "restricted_claims",
            release.SOURCE_POLICIES["tic"],
        )


@pytest.mark.parametrize(
    ("identity_kind", "content_identity_kind", "identity_ref", "digest"),
    [
        ("mutable_file", "logical_json_sha256_v1", "public-source", sha256_text("a")),
        ("immutable_artifact", "bad-kind", "public-source", sha256_text("a")),
        ("immutable_artifact", "logical_json_sha256_v1", "public-source", sha256_text("a")),
        (
            "immutable_artifact",
            "logical_json_sha256_v1",
            "123e4567-e89b-12d3-a456-426614174000",
            sha256_text("a"),
        ),
        ("immutable_artifact", "logical_json_sha256_v1", "peid1_short", "A" * 64),
    ],
)
def test_rejects_nonopaque_or_invalid_artifact_identity(
    identity_kind: str,
    content_identity_kind: str,
    identity_ref: str,
    digest: str,
) -> None:
    with pytest.raises(release.PublicEvidenceSourceReleaseError):
        release.ImmutablePublicSourceIdentity(
            identity_kind,
            content_identity_kind,
            identity_ref,
            digest,
        )


@pytest.mark.parametrize(
    "field_name",
    ["import_run_ref", "source_release_ref"],
)
def test_build_derives_and_rejects_supplied_run_or_release_references(
    field_name: str,
) -> None:
    raw = release_input()
    raw[field_name] = "caller-controlled-reference"
    with pytest.raises(release.PublicEvidenceSourceReleaseError):
        release.build_public_evidence_source_release(raw)

def test_identity_reference_cannot_embed_reversible_plaintext() -> None:
    class StringSubclass(str):
        pass

    identity = release_input()["artifact_identity"]
    reversible = base64.urlsafe_b64encode(
        b"raw-tax-id-123456789".ljust(32, b"_")
    ).rstrip(b"=")
    reversible_ref = (
        release.PUBLIC_EVIDENCE_IDENTITY_REF_PREFIX + reversible.decode("ascii")
    )
    with pytest.raises(release.PublicEvidenceSourceReleaseError):
        release.ImmutablePublicSourceIdentity(
            identity.identity_kind,
            identity.content_identity_kind,
            reversible_ref,
            identity.content_sha256,
        )

    with pytest.raises(release.PublicEvidenceSourceReleaseError):
        release.ImmutablePublicSourceIdentity(
            identity.identity_kind,
            identity.content_identity_kind,
            StringSubclass(identity.identity_ref),
            identity.content_sha256,
        )


@pytest.mark.parametrize(
    ("mode", "contract_id", "count_unit", "expected", "observed", "root"),
    [
        ("unknown", "source_record_attestation_v1", "source_record", 1, 1, sha256_text("b")),
        ("declared_complete_dataset", "bad-contract", "source_record", 1, 1, sha256_text("b")),
        ("declared_complete_dataset", "source_record_attestation_v1", "bad-unit", 1, 1, sha256_text("b")),
        ("declared_complete_dataset", "source_record_attestation_v1", "source_record", 1, True, sha256_text("b")),
        ("declared_complete_dataset", "source_record_attestation_v1", "source_record", 1, -1, sha256_text("b")),
        ("declared_complete_dataset", "source_record_attestation_v1", "source_record", 1, 2**53, sha256_text("b")),
        ("declared_complete_dataset", "source_record_attestation_v1", "source_record", True, 1, sha256_text("b")),
        ("declared_complete_dataset", "source_record_attestation_v1", "source_record", 2, 1, sha256_text("b")),
        ("positive_evidence_only", "source_record_attestation_v1", "source_record", 1, 1, sha256_text("b")),
        ("declared_complete_dataset", "source_record_attestation_v1", "source_record", 1, 1, "not-a-hash"),
    ],
)
def test_rejects_invalid_completeness_attestations(
    mode: str,
    contract_id: str,
    count_unit: str,
    expected: object,
    observed: object,
    root: str,
) -> None:
    with pytest.raises(release.PublicEvidenceSourceReleaseError):
        release.PublicEvidenceCompletenessAttestation(
            mode,
            contract_id,
            count_unit,
            sha256_text("a"),
            expected,
            observed,
            root,
        )


@pytest.mark.parametrize(
    (
        "contract_id",
        "source_artifact_source_type",
        "source_artifact_identity_kind",
        "source_artifact",
        "source_binding",
        "shadow_bundle_binding",
    ),
    [
        ("other_contract_v1", "in_network", "logical_json_sha256_v1", sha256_text("a"), sha256_text("d"), sha256_text("e")),
        (release.TAX_IDENTITY_SHADOW_SOURCE_BINDING_CONTRACT, None, "logical_json_sha256_v1", sha256_text("a"), sha256_text("d"), sha256_text("e")),
        (release.TAX_IDENTITY_SHADOW_SOURCE_BINDING_CONTRACT, "IN_NETWORK", "logical_json_sha256_v1", sha256_text("a"), sha256_text("d"), sha256_text("e")),
        (release.TAX_IDENTITY_SHADOW_SOURCE_BINDING_CONTRACT, "x" * 65, "logical_json_sha256_v1", sha256_text("a"), sha256_text("d"), sha256_text("e")),
        (release.TAX_IDENTITY_SHADOW_SOURCE_BINDING_CONTRACT, "in_network", "bad-kind", sha256_text("a"), sha256_text("d"), sha256_text("e")),
        (release.TAX_IDENTITY_SHADOW_SOURCE_BINDING_CONTRACT, "in_network", "logical_json_sha256_v1", "bad", sha256_text("d"), sha256_text("e")),
        (release.TAX_IDENTITY_SHADOW_SOURCE_BINDING_CONTRACT, "in_network", "logical_json_sha256_v1", sha256_text("a"), "bad", sha256_text("e")),
        (release.TAX_IDENTITY_SHADOW_SOURCE_BINDING_CONTRACT, "in_network", "logical_json_sha256_v1", sha256_text("a"), sha256_text("d"), "bad"),
        (release.TAX_IDENTITY_SHADOW_SOURCE_BINDING_CONTRACT, "in_network", "logical_json_sha256_v1", sha256_text("a"), sha256_text("a"), sha256_text("e")),
        (release.TAX_IDENTITY_SHADOW_SOURCE_BINDING_CONTRACT, "in_network", "logical_json_sha256_v1", sha256_text("a"), sha256_text("d"), sha256_text("a")),
        (release.TAX_IDENTITY_SHADOW_SOURCE_BINDING_CONTRACT, "in_network", "logical_json_sha256_v1", sha256_text("a"), sha256_text("d"), sha256_text("d")),
    ],
)
def test_rejects_invalid_or_conflated_tic_binding_subjects(
    contract_id: str,
    source_artifact_source_type: object,
    source_artifact_identity_kind: str,
    source_artifact: str,
    source_binding: str,
    shadow_bundle_binding: str,
) -> None:
    with pytest.raises(release.PublicEvidenceSourceReleaseError):
        release.OpaqueSourceBindingReference(
            contract_id,
            source_artifact_source_type,
            source_artifact_identity_kind,
            source_artifact,
            source_binding,
            shadow_bundle_binding,
        )


@pytest.mark.parametrize(
    ("identity_kind", "source_artifact"),
    [
        ("logical_json_sha256_v1", sha256_text("f")),
        ("raw_container_sha256_v1", sha256_text("a")),
    ],
)
def test_rejects_tic_binding_for_a_different_source_artifact(
    identity_kind: str,
    source_artifact: str,
) -> None:
    raw = release_input()
    raw["source_binding"] = release.OpaqueSourceBindingReference(
        release.TAX_IDENTITY_SHADOW_SOURCE_BINDING_CONTRACT,
        "in_network",
        identity_kind,
        source_artifact,
        sha256_text("d"),
        sha256_text("e"),
    )

    with pytest.raises(release.PublicEvidenceSourceReleaseError):
        release.build_public_evidence_source_release(raw)


def test_rejects_tic_binding_for_non_group_physical_source_type() -> None:
    raw = release_input()
    raw["source_binding"] = release.OpaqueSourceBindingReference(
        release.TAX_IDENTITY_SHADOW_SOURCE_BINDING_CONTRACT,
        "allowed_amounts",
        "logical_json_sha256_v1",
        sha256_text("a"),
        sha256_text("d"),
        sha256_text("e"),
    )

    with pytest.raises(release.PublicEvidenceSourceReleaseError):
        release.build_public_evidence_source_release(raw)


@pytest.mark.parametrize(
    ("start_at", "end_at"),
    [
        ("2026-07-01T00:00:00+00:00", None),
        ("2026-07-01T00:00:00.000Z", None),
        ("2026-07-01T00:00:00Z ", None),
        ("2026-02-30T00:00:00Z", None),
        ("2026-07-01T00:00:00Z\u0000", None),
        ("2026-07-01T00:00:00Z\u202e", None),
        ("2026-07-02T00:00:00Z", "2026-07-01T00:00:00Z"),
    ],
)
def test_rejects_noncanonical_or_reversed_utc_intervals(
    start_at: str,
    end_at: str | None,
) -> None:
    with pytest.raises(release.PublicEvidenceSourceReleaseError):
        release.CanonicalUtcInterval(start_at, end_at)


def test_observed_interval_must_be_closed_but_effective_may_be_open() -> None:
    raw = release_input()
    raw["observed_interval"] = release.CanonicalUtcInterval(
        "2026-07-01T00:00:00Z", None
    )
    with pytest.raises(release.PublicEvidenceSourceReleaseError):
        release.build_public_evidence_source_release(raw)
    assert release_input()["effective_interval"].end_at is None


def test_rejects_wrong_or_tampered_exact_nested_types() -> None:
    raw = release_input()
    raw["artifact_identity"] = object()
    with pytest.raises(release.PublicEvidenceSourceReleaseError):
        release.build_public_evidence_source_release(raw)

    raw = release_input()
    artifact = raw["artifact_identity"]
    object.__setattr__(artifact, "content_sha256", "A" * 64)
    with pytest.raises(release.PublicEvidenceSourceReleaseError):
        release.build_public_evidence_source_release(raw)

    raw = release_input()
    object.__delattr__(raw["artifact_identity"], "identity_ref")
    with pytest.raises(release.PublicEvidenceSourceReleaseError):
        release.build_public_evidence_source_release(raw)


def test_revalidates_descriptor_and_rejects_tampering() -> None:
    descriptor = release.build_public_evidence_source_release(release_input())
    rebuilt = release.validate_public_evidence_source_release(descriptor)
    assert rebuilt == descriptor
    assert rebuilt is not descriptor

    object.__setattr__(
        descriptor,
        "source_release_ref",
        release.PUBLIC_EVIDENCE_RELEASE_REF_PREFIX + "A" * 43,
    )
    with pytest.raises(release.PublicEvidenceSourceReleaseError):
        release.validate_public_evidence_source_release(descriptor)


def test_revalidation_rejects_wrong_type_and_foreign_properties() -> None:
    with pytest.raises(release.PublicEvidenceSourceReleaseError):
        release.validate_public_evidence_source_release(object())

    class Hostile:
        @property
        def contract(self) -> str:
            raise AssertionError("foreign property was evaluated")

    with pytest.raises(release.PublicEvidenceSourceReleaseError):
        release.validate_public_evidence_source_release(Hostile())


def test_source_policies_are_deeply_immutable() -> None:
    policy = release.SOURCE_POLICIES["tic"]

    with pytest.raises(AttributeError):
        object.__setattr__(policy, "authority", "attacker_reclassified")

    assert release.SOURCE_POLICIES["tic"] is policy


def test_wraps_unexpected_revalidation_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    descriptor = release.build_public_evidence_source_release(release_input())
    monkeypatch.setattr(release, "replace", lambda _descriptor: 1 / 0)
    with pytest.raises(release.PublicEvidenceSourceReleaseError):
        release.validate_public_evidence_source_release(descriptor)


def test_public_evidence_import_is_runtime_neutral() -> None:
    repository_root = Path(__file__).resolve().parents[1]
    script = """
import json
import sys
before = set(sys.modules)
from public_evidence import source_release_contract
introduced = set(sys.modules) - before
forbidden = sorted(
    name for name in introduced
    if name in {"api", "db", "process"}
    or name.startswith(("api.", "db.", "process."))
)
print(json.dumps(forbidden))
raise SystemExit(bool(forbidden))
"""
    completed = subprocess.run(
        [sys.executable, "-B", "-c", script],
        cwd=repository_root,
        check=True,
        capture_output=True,
        text=True,
    )
    assert completed.stdout.strip() == "[]"


def test_public_evidence_contract_has_only_stdlib_or_local_imports() -> None:
    package_path = Path(__file__).resolve().parents[1] / "public_evidence"
    roots = set()
    for source_path in package_path.glob("*.py"):
        tree = ast.parse(
            source_path.read_text(encoding="utf-8"),
            filename=str(source_path),
        )
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                roots.update(alias.name.split(".", 1)[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                roots.add(node.module.split(".", 1)[0])
    unexpected = roots - set(sys.stdlib_module_names) - {"__future__", "public_evidence"}
    assert not unexpected, sorted(unexpected)


def test_runtime_and_container_do_not_wire_the_dormant_package() -> None:
    repository_root = Path(__file__).resolve().parents[1]
    authorized_runtime_adapters = {
        "process/npi_canonical_publication.py",
        "process/nppes_public_evidence_chain.py",
        "process/nppes_public_evidence_chain_rows.py",
        "process/nppes_public_evidence_members.py",
        "process/nppes_public_evidence_replay.py",
        "process/nppes_public_evidence_rows.py",
        "process/nppes_public_evidence_writer.py",
        "process/public_evidence_fhir_organization_replay.py",
    }
    runtime_sources = [repository_root / "main.py"]
    for package_name in ("api", "db", "process", "service"):
        runtime_sources.extend((repository_root / package_name).rglob("*.py"))

    importing_paths = []
    for source_path in runtime_sources:
        tree = ast.parse(
            source_path.read_text(encoding="utf-8"),
            filename=str(source_path),
        )
        imports = [
            node
            for node in ast.walk(tree)
            if (
                isinstance(node, ast.Import)
                and any(alias.name == "public_evidence" for alias in node.names)
            )
            or (
                isinstance(node, ast.ImportFrom)
                and node.module is not None
                and node.module.split(".", 1)[0] == "public_evidence"
            )
        ]
        if imports:
            importing_paths.append(source_path.relative_to(repository_root).as_posix())

    assert set(importing_paths) == authorized_runtime_adapters
    dockerfile = (repository_root / "Dockerfile").read_text(encoding="utf-8")
    assert "public_evidence" not in dockerfile
