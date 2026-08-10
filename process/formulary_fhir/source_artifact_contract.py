# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Pure contracts for immutable retained formulary source artifacts."""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
import hashlib
from typing import Any

from process.formulary_fhir.repository_shared import json_text
from process.formulary_fhir.repository_shared import row_mapping
from process.formulary_fhir.repository_shared import strict_hash
from process.formulary_fhir.repository_shared import strict_text
from process.formulary_fhir.repository_shared import utc_timestamp


ARTIFACT_SET_DOMAIN = "fhir-formulary-source-artifact-set-v1"
SOURCE_ARTIFACT_IDENTITY_FIELDS = (
    "source_id",
    "source_file_set_sha256",
    "source_file_id",
    "raw_listing_projection_sha256",
    "family",
    "file_name",
    "source_url",
    "catalog_modified_at",
    "catalog_entry_sha256",
    "expected_byte_count",
)


@dataclass(frozen=True, slots=True, repr=False)
class SourceArtifactIdentity:
    """One source file's catalog-bound identity without retained content."""

    source_id: str
    source_file_set_sha256: str = field(repr=False)
    source_file_id: str = field(repr=False)
    raw_listing_projection_sha256: str = field(repr=False)
    family: str
    file_name: str
    source_url: str = field(repr=False)
    catalog_modified_at: str
    catalog_entry_sha256: str = field(repr=False)
    expected_byte_count: int | None

    def __post_init__(self) -> None:
        strict_text(self.source_id, "source id", 64)
        for label, digest_value in (
            ("source file set hash", self.source_file_set_sha256),
            ("source file id", self.source_file_id),
            ("raw listing projection hash", self.raw_listing_projection_sha256),
            ("catalog entry hash", self.catalog_entry_sha256),
        ):
            strict_hash(digest_value, label)
        strict_text(self.family, "source artifact family", 32)
        strict_text(self.file_name, "source artifact file name", 256)
        strict_text(self.source_url, "source artifact URL", 4_096)
        strict_text(
            self.catalog_modified_at,
            "source artifact catalog timestamp",
            64,
        )
        if self.expected_byte_count is not None and (
            type(self.expected_byte_count) is not int
            or self.expected_byte_count <= 0
            or self.expected_byte_count > 2**63 - 1
        ):
            raise ValueError(
                "FHIR formulary source artifact expected byte count is invalid"
            )


@dataclass(frozen=True, slots=True, repr=False)
class VerifiedSourceArtifact:
    """One immutable catalog identity with content-addressed retained bytes."""

    identity: SourceArtifactIdentity
    artifact_sha256: str = field(repr=False)
    artifact_byte_count: int
    verified_at: dt.datetime

    def __post_init__(self) -> None:
        if type(self.identity) is not SourceArtifactIdentity:
            raise ValueError("FHIR formulary source artifact identity is invalid")
        strict_hash(self.artifact_sha256, "source artifact hash")
        if (
            type(self.artifact_byte_count) is not int
            or self.artifact_byte_count <= 0
            or self.artifact_byte_count > 2**63 - 1
            or (
                self.identity.expected_byte_count is not None
                and self.artifact_byte_count
                != self.identity.expected_byte_count
            )
        ):
            raise ValueError("FHIR formulary source artifact byte count is invalid")
        utc_timestamp(self.verified_at, "source artifact verification timestamp")


def identity_sort_key(
    identity: SourceArtifactIdentity,
) -> tuple[str, str, str]:
    """Return the sole deterministic source-file order."""

    return identity.family, identity.file_name, identity.source_file_id


def artifact_sort_key(
    artifact: VerifiedSourceArtifact,
) -> tuple[str, str, str]:
    """Return the sole deterministic verified-artifact order."""

    return identity_sort_key(artifact.identity)


def identity_fields(identity: SourceArtifactIdentity) -> dict[str, Any]:
    """Serialize one exact identity for SQL writes and digest rows."""

    return {
        "source_id": identity.source_id,
        "source_file_set_sha256": identity.source_file_set_sha256,
        "source_file_id": identity.source_file_id,
        "raw_listing_projection_sha256": (
            identity.raw_listing_projection_sha256
        ),
        "family": identity.family,
        "file_name": identity.file_name,
        "source_url": identity.source_url,
        "catalog_modified_at": identity.catalog_modified_at,
        "catalog_entry_sha256": identity.catalog_entry_sha256,
        "expected_byte_count": identity.expected_byte_count,
    }


def identity_from_row(database_row: Any) -> SourceArtifactIdentity:
    """Rebuild one identity from a database mapping."""

    row_by_field = row_mapping(database_row)
    return SourceArtifactIdentity(
        **{
            field_name: row_by_field.get(field_name)
            for field_name in SOURCE_ARTIFACT_IDENTITY_FIELDS
        }
    )


def artifact_from_row(database_row: Any) -> VerifiedSourceArtifact:
    """Rebuild one verified artifact from a database mapping."""

    row_by_field = row_mapping(database_row)
    return VerifiedSourceArtifact(
        identity=identity_from_row(row_by_field),
        artifact_sha256=row_by_field.get("artifact_sha256"),
        artifact_byte_count=row_by_field.get("artifact_byte_count"),
        verified_at=row_by_field.get("verified_at"),
    )


def validated_identity_set(
    identities: tuple[SourceArtifactIdentity, ...],
) -> tuple[SourceArtifactIdentity, ...]:
    """Validate and deterministically order one exact source-file set."""

    if type(identities) is not tuple or not identities:
        raise ValueError("FHIR formulary source artifact set is empty")
    if any(type(identity) is not SourceArtifactIdentity for identity in identities):
        raise ValueError("FHIR formulary source artifact identity is invalid")
    first_identity = identities[0]
    expected_scope = (
        first_identity.source_id,
        first_identity.source_file_set_sha256,
        first_identity.raw_listing_projection_sha256,
    )
    if any(
        (
            identity.source_id,
            identity.source_file_set_sha256,
            identity.raw_listing_projection_sha256,
        )
        != expected_scope
        for identity in identities
    ):
        raise ValueError("FHIR formulary source artifact set scope is inconsistent")
    ordered_identities = tuple(sorted(identities, key=identity_sort_key))
    if len({identity.source_file_id for identity in ordered_identities}) != len(
        ordered_identities
    ):
        raise ValueError("FHIR formulary source artifact file ids are duplicated")
    logical_names = {
        (identity.family, identity.file_name) for identity in ordered_identities
    }
    if len(logical_names) != len(ordered_identities):
        raise ValueError("FHIR formulary source artifact names are duplicated")
    return ordered_identities


def artifact_set_sha256(
    artifacts: tuple[VerifiedSourceArtifact, ...],
) -> str:
    """Hash all exact catalog and retained content identities in set order."""

    if type(artifacts) is not tuple or not artifacts:
        raise ValueError("FHIR formulary source artifact set is empty")
    if any(type(artifact) is not VerifiedSourceArtifact for artifact in artifacts):
        raise ValueError("FHIR formulary verified source artifact is invalid")
    proof_rows = [
        {
            **identity_fields(artifact.identity),
            "artifact_byte_count": artifact.artifact_byte_count,
            "artifact_sha256": artifact.artifact_sha256,
        }
        for artifact in sorted(artifacts, key=artifact_sort_key)
    ]
    digest = hashlib.sha256()
    digest.update(ARTIFACT_SET_DOMAIN.encode("ascii"))
    digest.update(b"\n")
    digest.update(json_text(proof_rows).encode("utf-8"))
    return digest.hexdigest()


@dataclass(frozen=True, slots=True, repr=False)
class VerifiedSourceArtifactSet:
    """One exact, complete, deterministic retained source-file set."""

    source_id: str
    source_file_set_sha256: str = field(repr=False)
    raw_listing_projection_sha256: str = field(repr=False)
    artifacts: tuple[VerifiedSourceArtifact, ...] = field(repr=False)
    artifact_set_sha256: str = field(repr=False)

    def __post_init__(self) -> None:
        strict_text(self.source_id, "source id", 64)
        strict_hash(self.source_file_set_sha256, "source file set hash")
        strict_hash(
            self.raw_listing_projection_sha256,
            "raw listing projection hash",
        )
        strict_hash(self.artifact_set_sha256, "artifact set hash")
        if type(self.artifacts) is not tuple or not self.artifacts:
            raise ValueError("FHIR formulary verified artifact set is empty")
        expected_scope = (
            self.source_id,
            self.source_file_set_sha256,
            self.raw_listing_projection_sha256,
        )
        actual_scopes = {
            (
                artifact.identity.source_id,
                artifact.identity.source_file_set_sha256,
                artifact.identity.raw_listing_projection_sha256,
            )
            for artifact in self.artifacts
        }
        logical_names = {
            (artifact.identity.family, artifact.identity.file_name)
            for artifact in self.artifacts
        }
        file_ids = {
            artifact.identity.source_file_id for artifact in self.artifacts
        }
        if (
            actual_scopes != {expected_scope}
            or len(logical_names) != len(self.artifacts)
            or len(file_ids) != len(self.artifacts)
            or tuple(sorted(self.artifacts, key=artifact_sort_key))
            != self.artifacts
            or artifact_set_sha256(self.artifacts)
            != self.artifact_set_sha256
        ):
            raise ValueError("FHIR formulary verified artifact set is inconsistent")


__all__ = (
    "ARTIFACT_SET_DOMAIN",
    "SOURCE_ARTIFACT_IDENTITY_FIELDS",
    "SourceArtifactIdentity",
    "VerifiedSourceArtifact",
    "VerifiedSourceArtifactSet",
    "artifact_from_row",
    "artifact_set_sha256",
    "artifact_sort_key",
    "identity_fields",
    "identity_from_row",
    "identity_sort_key",
    "validated_identity_set",
)
