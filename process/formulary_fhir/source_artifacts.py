# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Immutable source-artifact registration and one-time verification fill."""

from __future__ import annotations

import asyncio
import inspect
from typing import Any
from typing import Callable

from db.models import db
from process.formulary_fhir.async_safety import drain_operation
from process.formulary_fhir.async_safety import cancellable_to_thread
from process.formulary_fhir.source_artifact_contract import (
    ARTIFACT_SET_DOMAIN,
    SOURCE_ARTIFACT_IDENTITY_FIELDS,
    SourceArtifactIdentity,
    VerifiedSourceArtifact,
    VerifiedSourceArtifactSet,
    artifact_from_row,
    artifact_set_sha256,
    artifact_sort_key,
    identity_fields,
    identity_from_row,
    identity_sort_key,
    validated_identity_set,
)
from process.formulary_fhir.repository_shared import lock_source
from process.formulary_fhir.repository_shared import row_mapping
from process.formulary_fhir.repository_shared import strict_hash
from process.formulary_fhir.repository_shared import strict_text
from process.formulary_fhir.repository_shared import table_name
from process.formulary_fhir.source_artifact_binding import (
    bind_verified_source_artifact,
)
from process.formulary_fhir.source_artifact_storage import (
    open_verified_source_artifact,
)
from process.formulary_fhir.source_artifact_storage import (
    verify_retained_source_artifact as _verify_retained_source_artifact,
)
from process.provider_directory_retained_artifact_base import RetainedArtifactError


_RESTORABLE_RETAINED_CODES = frozenset({"retained_blob_unavailable"})


async def _shielded_to_thread(operation: Any, *args: Any) -> Any:
    """Drain one inode-sensitive blocking operation before propagating cancel."""

    return await drain_operation(
        asyncio.to_thread(operation, *args),
        preserve_cancellation=True,
    )


async def _invoke_cancel_check(
    cancel_check: Callable[[], Any] | None,
) -> None:
    if cancel_check is None:
        return
    cancellation_result = cancel_check()
    if inspect.isawaitable(cancellation_result):
        await cancellation_result


async def _artifact_records_for_set(
    database: Any,
    source_id: str,
    source_file_set_sha256: str,
    *,
    for_update: bool = False,
) -> tuple[dict[str, Any], ...]:
    lock_suffix = " FOR UPDATE" if for_update else ""
    database_rows = await database.all(
        f"SELECT {', '.join(SOURCE_ARTIFACT_IDENTITY_FIELDS)}, artifact_sha256, "
        f"artifact_byte_count, status, verified_at FROM "
        f"{table_name('fhir_formulary_source_artifact')} WHERE "
        "source_id = :source_id AND "
        "source_file_set_sha256 = :source_file_set_sha256 "
        f"ORDER BY family, file_name, source_file_id{lock_suffix};",
        source_id=source_id,
        source_file_set_sha256=source_file_set_sha256,
    )
    return tuple(row_mapping(database_row) for database_row in database_rows)


async def _register_set_header(
    database: Any,
    identities: tuple[SourceArtifactIdentity, ...],
) -> None:
    first_identity = identities[0]
    await database.status(
        f"INSERT INTO {table_name('fhir_formulary_source_artifact_set')} ("
        "source_id, source_file_set_sha256, raw_listing_projection_sha256, "
        "expected_file_count) VALUES (:source_id, :source_file_set_sha256, "
        ":raw_listing_projection_sha256, :expected_file_count) "
        "ON CONFLICT DO NOTHING;",
        source_id=first_identity.source_id,
        source_file_set_sha256=first_identity.source_file_set_sha256,
        raw_listing_projection_sha256=(
            first_identity.raw_listing_projection_sha256
        ),
        expected_file_count=len(identities),
    )
    set_row = row_mapping(
        await database.first(
            f"SELECT source_id, source_file_set_sha256, "
            "raw_listing_projection_sha256, expected_file_count FROM "
            f"{table_name('fhir_formulary_source_artifact_set')} WHERE "
            "source_id = :source_id AND "
            "source_file_set_sha256 = :source_file_set_sha256 FOR UPDATE;",
            source_id=first_identity.source_id,
            source_file_set_sha256=first_identity.source_file_set_sha256,
        )
    )
    expected_set_by_field = {
        "source_id": first_identity.source_id,
        "source_file_set_sha256": first_identity.source_file_set_sha256,
        "raw_listing_projection_sha256": (
            first_identity.raw_listing_projection_sha256
        ),
        "expected_file_count": len(identities),
    }
    if set_row != expected_set_by_field:
        raise RuntimeError("FHIR formulary source artifact set header changed")


async def _require_set_header(
    database: Any,
    identities: tuple[SourceArtifactIdentity, ...],
) -> None:
    first_identity = identities[0]
    set_row = row_mapping(
        await database.first(
            f"SELECT source_id, source_file_set_sha256, "
            "raw_listing_projection_sha256, expected_file_count FROM "
            f"{table_name('fhir_formulary_source_artifact_set')} WHERE "
            "source_id = :source_id AND "
            "source_file_set_sha256 = :source_file_set_sha256;",
            source_id=first_identity.source_id,
            source_file_set_sha256=first_identity.source_file_set_sha256,
        )
    )
    expected_set_by_field = {
        "source_id": first_identity.source_id,
        "source_file_set_sha256": first_identity.source_file_set_sha256,
        "raw_listing_projection_sha256": (
            first_identity.raw_listing_projection_sha256
        ),
        "expected_file_count": len(identities),
    }
    if set_row != expected_set_by_field:
        raise RuntimeError("FHIR formulary source artifact set header changed")


async def _register_source_observation(
    database: Any,
    identities: tuple[SourceArtifactIdentity, ...],
    source_observation_sha256: str,
) -> None:
    first_identity = identities[0]
    await database.status(
        f"INSERT INTO {table_name('fhir_formulary_source_artifact_observation')} "
        "(source_id, source_observation_sha256, source_file_set_sha256, "
        "raw_listing_projection_sha256) VALUES (:source_id, "
        ":source_observation_sha256, :source_file_set_sha256, "
        ":raw_listing_projection_sha256) ON CONFLICT DO NOTHING;",
        source_id=first_identity.source_id,
        source_observation_sha256=source_observation_sha256,
        source_file_set_sha256=first_identity.source_file_set_sha256,
        raw_listing_projection_sha256=(
            first_identity.raw_listing_projection_sha256
        ),
    )
    observation_by_field = row_mapping(
        await database.first(
            "SELECT source_id, source_observation_sha256, "
            "source_file_set_sha256, raw_listing_projection_sha256 FROM "
            f"{table_name('fhir_formulary_source_artifact_observation')} WHERE "
            "source_id = :source_id AND "
            "source_observation_sha256 = :source_observation_sha256 "
            "FOR UPDATE;",
            source_id=first_identity.source_id,
            source_observation_sha256=source_observation_sha256,
        )
    )
    expected_observation_by_field = {
        "source_id": first_identity.source_id,
        "source_observation_sha256": source_observation_sha256,
        "source_file_set_sha256": first_identity.source_file_set_sha256,
        "raw_listing_projection_sha256": (
            first_identity.raw_listing_projection_sha256
        ),
    }
    if observation_by_field != expected_observation_by_field:
        raise RuntimeError("FHIR formulary source artifact observation changed")


def _require_exact_rows(
    database_rows: tuple[dict[str, Any], ...],
    expected_identities: tuple[SourceArtifactIdentity, ...],
) -> None:
    actual_identities = tuple(
        sorted(
            (identity_from_row(database_row) for database_row in database_rows),
            key=identity_sort_key,
        )
    )
    expected_identity_rows = tuple(
        sorted(expected_identities, key=identity_sort_key)
    )
    if actual_identities != expected_identity_rows:
        raise RuntimeError("FHIR formulary source artifact set is inconsistent")


async def register_source_file_set(
    identities: tuple[SourceArtifactIdentity, ...],
    *,
    source_observation_sha256: str,
    database: Any = db,
) -> tuple[SourceArtifactIdentity, ...]:
    """Idempotently register all identities in one reviewed source-file set."""

    exact_identities = validated_identity_set(identities)
    strict_hash(source_observation_sha256, "source observation hash")
    first_identity = exact_identities[0]
    async with database.transaction():
        await lock_source(database, first_identity.source_id)
        await _register_set_header(database, exact_identities)
        await _register_source_observation(
            database,
            exact_identities,
            source_observation_sha256,
        )
        for identity in exact_identities:
            await database.status(
                f"INSERT INTO {table_name('fhir_formulary_source_artifact')} ("
                f"{', '.join(SOURCE_ARTIFACT_IDENTITY_FIELDS)}, status) VALUES ("
                ":source_id, :source_file_set_sha256, :source_file_id, "
                ":raw_listing_projection_sha256, :family, :file_name, "
                ":source_url, :catalog_modified_at, :catalog_entry_sha256, "
                ":expected_byte_count, 'pending') ON CONFLICT DO NOTHING;",
                **identity_fields(identity),
            )
        database_rows = await _artifact_records_for_set(
            database,
            first_identity.source_id,
            first_identity.source_file_set_sha256,
            for_update=True,
        )
        _require_exact_rows(database_rows, exact_identities)
    return exact_identities


async def pending_source_files(
    identities: tuple[SourceArtifactIdentity, ...],
    *,
    database: Any = db,
    cancel_check: Callable[[], Any] | None = None,
) -> tuple[SourceArtifactIdentity, ...]:
    """Return only still-unbound identities after exact set verification."""

    expected_identities = validated_identity_set(identities)
    first_identity = expected_identities[0]
    await _require_set_header(database, expected_identities)
    database_rows = await _artifact_records_for_set(
        database,
        first_identity.source_id,
        first_identity.source_file_set_sha256,
    )
    _require_exact_rows(database_rows, expected_identities)
    pending_identities: list[SourceArtifactIdentity] = []
    for database_row in database_rows:
        await _invoke_cancel_check(cancel_check)
        identity = identity_from_row(database_row)
        if database_row.get("status") == "pending":
            pending_identities.append(identity)
            continue
        if database_row.get("status") != "verified":
            raise RuntimeError("FHIR formulary source artifact state is invalid")
        stored_artifact = artifact_from_row(database_row)
        try:
            await cancellable_to_thread(
                _verify_retained_source_artifact,
                stored_artifact.artifact_sha256,
                stored_artifact.artifact_byte_count,
            )
        except RetainedArtifactError as error:
            if error.code not in _RESTORABLE_RETAINED_CODES:
                raise
            pending_identities.append(identity)
    await _invoke_cancel_check(cancel_check)
    return tuple(pending_identities)


async def load_source_artifact_identities(
    source_id: str,
    source_file_set_sha256: str,
    *,
    database: Any = db,
) -> tuple[SourceArtifactIdentity, ...]:
    """Load one exact registered file-set identity without retained URLs output."""

    normalized_source_id = strict_text(source_id, "source id", 64)
    normalized_set_hash = strict_hash(
        source_file_set_sha256,
        "source file set hash",
    )
    database_rows = await _artifact_records_for_set(
        database,
        normalized_source_id,
        normalized_set_hash,
    )
    try:
        identities = validated_identity_set(
            tuple(identity_from_row(database_row) for database_row in database_rows)
        )
    except ValueError:
        raise RuntimeError("FHIR formulary source artifact set is missing") from None
    await _require_set_header(database, identities)
    return identities


async def load_complete_source_artifact_set(
    identities: tuple[SourceArtifactIdentity, ...],
    *,
    database: Any = db,
    cancel_check: Callable[[], Any] | None = None,
) -> VerifiedSourceArtifactSet:
    """Load and verify every exact retained file before parsing or publication."""

    expected_identities = validated_identity_set(identities)
    first_identity = expected_identities[0]
    await _require_set_header(database, expected_identities)
    database_rows = await _artifact_records_for_set(
        database,
        first_identity.source_id,
        first_identity.source_file_set_sha256,
    )
    _require_exact_rows(database_rows, expected_identities)
    if any(database_row.get("status") != "verified" for database_row in database_rows):
        raise RuntimeError("FHIR formulary source artifact set is incomplete")
    artifacts = tuple(
        sorted(
            (artifact_from_row(database_row) for database_row in database_rows),
            key=artifact_sort_key,
        )
    )
    for artifact in artifacts:
        await _invoke_cancel_check(cancel_check)
        await cancellable_to_thread(
            _verify_retained_source_artifact,
            artifact.artifact_sha256,
            artifact.artifact_byte_count,
        )
    await _invoke_cancel_check(cancel_check)
    return VerifiedSourceArtifactSet(
        source_id=first_identity.source_id,
        source_file_set_sha256=first_identity.source_file_set_sha256,
        raw_listing_projection_sha256=(
            first_identity.raw_listing_projection_sha256
        ),
        artifacts=artifacts,
        artifact_set_sha256=artifact_set_sha256(artifacts),
    )


async def reopen_source_artifact_set(
    source_id: str,
    source_file_set_sha256: str,
    expected_artifact_set_sha256: str,
    *,
    database: Any = db,
    cancel_check: Callable[[], Any] | None = None,
) -> VerifiedSourceArtifactSet:
    """Reopen and rehash a durable exact set without an in-memory acquisition."""

    identities = await load_source_artifact_identities(
        source_id,
        source_file_set_sha256,
        database=database,
    )
    expected_artifact_hash = strict_hash(
        expected_artifact_set_sha256,
        "expected artifact set hash",
    )
    artifact_set = await load_complete_source_artifact_set(
        identities,
        database=database,
        cancel_check=cancel_check,
    )
    if artifact_set.artifact_set_sha256 != expected_artifact_hash:
        raise RuntimeError("FHIR formulary retained artifact set changed")
    return artifact_set


__all__ = (
    "ARTIFACT_SET_DOMAIN",
    "SourceArtifactIdentity",
    "VerifiedSourceArtifact",
    "VerifiedSourceArtifactSet",
    "artifact_set_sha256",
    "bind_verified_source_artifact",
    "load_complete_source_artifact_set",
    "reopen_source_artifact_set",
    "load_source_artifact_identities",
    "open_verified_source_artifact",
    "pending_source_files",
    "register_source_file_set",
)
