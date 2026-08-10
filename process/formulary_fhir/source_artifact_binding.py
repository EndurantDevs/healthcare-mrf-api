# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""One-time retained-content binding for formulary source artifacts."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

from db.models import db
from process.formulary_fhir.async_safety import drain_operation
from process.formulary_fhir.repository_shared import row_mapping
from process.formulary_fhir.repository_shared import strict_hash
from process.formulary_fhir.repository_shared import table_name
from process.formulary_fhir.source_artifact_contract import (
    SOURCE_ARTIFACT_IDENTITY_FIELDS,
)
from process.formulary_fhir.source_artifact_contract import SourceArtifactIdentity
from process.formulary_fhir.source_artifact_contract import VerifiedSourceArtifact
from process.formulary_fhir.source_artifact_contract import artifact_from_row
from process.formulary_fhir.source_artifact_contract import identity_from_row
from process.formulary_fhir.source_artifact_storage import (
    install_and_verify_source_artifact,
)
from process.formulary_fhir.source_artifact_storage import (
    verify_retained_source_artifact,
)


async def _shielded_to_thread(operation: Any, *args: Any) -> Any:
    return await drain_operation(
        asyncio.to_thread(operation, *args),
        preserve_cancellation=True,
    )


def _require_row_identity(
    database_row: dict[str, Any],
    identity: SourceArtifactIdentity,
) -> None:
    if identity_from_row(database_row) != identity:
        raise RuntimeError("FHIR formulary source artifact identity changed")


async def _artifact_record_for_identity(
    database: Any,
    identity: SourceArtifactIdentity,
    *,
    for_update: bool = False,
) -> dict[str, Any]:
    lock_suffix = " FOR UPDATE" if for_update else ""
    database_row = await database.first(
        f"SELECT {', '.join(SOURCE_ARTIFACT_IDENTITY_FIELDS)}, "
        "artifact_sha256, artifact_byte_count, status, verified_at "
        f"FROM {table_name('fhir_formulary_source_artifact')} WHERE "
        "source_id = :source_id AND "
        "source_file_set_sha256 = :source_file_set_sha256 AND "
        f"source_file_id = :source_file_id{lock_suffix};",
        source_id=identity.source_id,
        source_file_set_sha256=identity.source_file_set_sha256,
        source_file_id=identity.source_file_id,
    )
    row_by_field = row_mapping(database_row)
    _require_row_identity(row_by_field, identity)
    return row_by_field


def _validate_artifact_content_claim(
    identity: SourceArtifactIdentity,
    artifact_sha256: str,
    artifact_byte_count: int,
) -> None:
    strict_hash(artifact_sha256, "source artifact hash")
    if (
        type(artifact_byte_count) is not int
        or artifact_byte_count <= 0
        or artifact_byte_count > 2**63 - 1
        or (
            identity.expected_byte_count is not None
            and artifact_byte_count != identity.expected_byte_count
        )
    ):
        raise ValueError("FHIR formulary source artifact byte count is invalid")


def _matching_verified_artifact(
    database_row: dict[str, Any],
    artifact_sha256: str,
    artifact_byte_count: int,
) -> VerifiedSourceArtifact:
    stored_artifact = artifact_from_row(database_row)
    if (
        stored_artifact.artifact_sha256,
        stored_artifact.artifact_byte_count,
    ) != (artifact_sha256, artifact_byte_count):
        raise RuntimeError("FHIR formulary source artifact content changed")
    return stored_artifact


async def _fill_pending_source_artifact(
    database: Any,
    identity: SourceArtifactIdentity,
    artifact_sha256: str,
    artifact_byte_count: int,
) -> VerifiedSourceArtifact:
    async with database.transaction():
        database_row = await _artifact_record_for_identity(
            database,
            identity,
            for_update=True,
        )
        if database_row.get("status") == "verified":
            return _matching_verified_artifact(
                database_row,
                artifact_sha256,
                artifact_byte_count,
            )
        if database_row.get("status") != "pending":
            raise RuntimeError("FHIR formulary source artifact state is invalid")
        updated_count = await database.status(
            f"UPDATE {table_name('fhir_formulary_source_artifact')} SET "
            "artifact_sha256 = :artifact_sha256, "
            "artifact_byte_count = :artifact_byte_count, status = 'verified', "
            "verified_at = transaction_timestamp() WHERE "
            "source_id = :source_id AND "
            "source_file_set_sha256 = :source_file_set_sha256 AND "
            "source_file_id = :source_file_id AND status = 'pending';",
            artifact_sha256=artifact_sha256,
            artifact_byte_count=artifact_byte_count,
            source_id=identity.source_id,
            source_file_set_sha256=identity.source_file_set_sha256,
            source_file_id=identity.source_file_id,
        )
        if updated_count != 1:
            raise RuntimeError("FHIR formulary source artifact fill failed")
        verified_by_field = await _artifact_record_for_identity(database, identity)
        return artifact_from_row(verified_by_field)


async def _install_and_fill_pending_source_artifact(
    database: Any,
    identity: SourceArtifactIdentity,
    source_path: Path,
    artifact_sha256: str,
    artifact_byte_count: int,
) -> VerifiedSourceArtifact:
    """Drain retained install and immutable ledger fill as one operation."""

    await _shielded_to_thread(
        install_and_verify_source_artifact,
        source_path,
        artifact_sha256,
        artifact_byte_count,
    )
    return await _fill_pending_source_artifact(
        database,
        identity,
        artifact_sha256,
        artifact_byte_count,
    )


async def bind_verified_source_artifact(
    identity: SourceArtifactIdentity,
    *,
    source_path: Path | str | None = None,
    artifact_sha256: str,
    artifact_byte_count: int,
    database: Any = db,
) -> VerifiedSourceArtifact:
    """Install, rehash, and CAS-bind one source file to retained content."""

    if type(identity) is not SourceArtifactIdentity:
        raise ValueError("FHIR formulary source artifact identity is invalid")
    _validate_artifact_content_claim(
        identity,
        artifact_sha256,
        artifact_byte_count,
    )
    existing_row = await _artifact_record_for_identity(database, identity)
    if existing_row.get("status") == "verified":
        stored_artifact = _matching_verified_artifact(
            existing_row,
            artifact_sha256,
            artifact_byte_count,
        )
        verifier = verify_retained_source_artifact
        verifier_args: tuple[Any, ...] = (
            stored_artifact.artifact_sha256,
            stored_artifact.artifact_byte_count,
        )
        if source_path is not None:
            verifier = install_and_verify_source_artifact
            verifier_args = (Path(source_path), *verifier_args)
        await _shielded_to_thread(verifier, *verifier_args)
        return stored_artifact
    if existing_row.get("status") != "pending":
        raise RuntimeError("FHIR formulary source artifact state is invalid")
    if source_path is None:
        raise ValueError("FHIR formulary source artifact path is required")
    return await drain_operation(
        _install_and_fill_pending_source_artifact(
            database,
            identity,
            Path(source_path),
            artifact_sha256,
            artifact_byte_count,
        ),
        preserve_cancellation=True,
    )


__all__ = ("bind_verified_source_artifact",)
