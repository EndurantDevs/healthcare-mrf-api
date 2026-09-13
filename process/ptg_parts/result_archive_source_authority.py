# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Durable source authority for one PTG result-archive export.

The caller persists its terminal outcome before releasing this authority.  This
module deliberately does not decide whether a snapshot is a current pointer or
perform a destination activation.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Any, Mapping

from sqlalchemy import text

from process.ptg_parts.canonical import canonical_json_dumps
from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.frozen_rate_binding import frozen_rate_binding_sha256
from process.ptg_parts.ptg2_lifecycle_lock import (
    PTG2LifecycleLockDeferred,
    acquire_ptg2_source_lifecycle_lock,
    configure_ptg2_lifecycle_transaction,
    is_retryable_lifecycle_database_error,
)

PTG_RESULT_ARCHIVE_SOURCE_AUTHORITY_CONTRACT = "healthporta.ptg-result-archive-source-authority.v1"
PTG_RESULT_ARCHIVE_SOURCE_PIN_OWNER_TYPE = "ptg-result-archive-source"
_OPERATION_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$")
_SNAPSHOT_STATUSES = frozenset({"validated", "published"})


class PtgResultArchiveSourceAuthorityError(RuntimeError):
    """The source snapshot or its exact authority lease is unavailable."""


@dataclass(frozen=True)
class PtgResultArchiveSourceAuthority:
    """Serializable source proof and the exact retention-pin identity."""

    operation_id: str
    snapshot_id: str
    source_file_import_id: str
    source_key: str
    snapshot_manifest_sha256: str
    frozen_binding_sha256: str

    @property
    def owner_id(self) -> str:
        """Return the bounded operation-derived retention-pin owner."""

        return hashlib.sha256(
            f"{PTG_RESULT_ARCHIVE_SOURCE_PIN_OWNER_TYPE}:v1:{self.operation_id}".encode("utf-8")
        ).hexdigest()

    @property
    def pin_reason(self) -> str:
        """Bind the durable pin to this exact immutable source observation."""

        authority_digest = _digest(
            {
                "operation_id": self.operation_id,
                "snapshot_id": self.snapshot_id,
                "source_file_import_id": self.source_file_import_id,
                "source_key": self.source_key,
                "snapshot_manifest_sha256": self.snapshot_manifest_sha256,
                "frozen_binding_sha256": self.frozen_binding_sha256,
            }
        )
        return f"retain result archive source {authority_digest}"

    def retention_pin(self) -> dict[str, str]:
        """Return the closed pin shape required by archive closure selection."""

        return {
            "pin_id": f"{PTG_RESULT_ARCHIVE_SOURCE_PIN_OWNER_TYPE}:{self.owner_id}",
            "repeatable_read_token": self.operation_id,
        }

    def as_dict(self) -> dict[str, Any]:
        """Return the exact source-publication payload for the trusted export callback."""

        return {
            "contract": PTG_RESULT_ARCHIVE_SOURCE_AUTHORITY_CONTRACT,
            "operation_id": self.operation_id,
            "snapshot_id": self.snapshot_id,
            "source_file_import_id": self.source_file_import_id,
            "source_key": self.source_key,
            "snapshot_manifest_sha256": self.snapshot_manifest_sha256,
            "frozen_binding_sha256": self.frozen_binding_sha256,
            "pin": {
                "owner_type": PTG_RESULT_ARCHIVE_SOURCE_PIN_OWNER_TYPE,
                "owner_id": self.owner_id,
                "snapshot_id": self.snapshot_id,
                "reason": self.pin_reason,
            },
        }


def _require_transaction(session: Any) -> None:
    """Refuse to make a durable authority change outside the caller transaction."""

    in_transaction = getattr(session, "in_transaction", None)
    if not callable(in_transaction) or not in_transaction():
        raise PtgResultArchiveSourceAuthorityError("result archive source authority requires a caller transaction")


def _required_text(value: object, field_name: str, maximum: int) -> str:
    """Normalize one bounded persisted or receipt identity."""

    normalized = str(value or "").strip()
    if not normalized or len(normalized) > maximum:
        raise PtgResultArchiveSourceAuthorityError(f"result archive source authority {field_name} is invalid")
    return normalized


def _operation_id(value: object) -> str:
    """Validate the fixed archive operation identity."""

    operation_id = _required_text(value, "operation identity", 128)
    if not _OPERATION_ID_RE.fullmatch(operation_id):
        raise PtgResultArchiveSourceAuthorityError("result archive source authority operation identity is invalid")
    return operation_id


def _digest(value: Mapping[str, Any]) -> str:
    """Hash one canonical persisted JSON mapping."""

    return hashlib.sha256(canonical_json_dumps(dict(value)).encode("utf-8")).hexdigest()


def result_archive_manifest_sha256(manifest: Mapping[str, Any]) -> str:
    """Hash snapshot manifest fields unchanged by source publication activation."""

    if not isinstance(manifest, Mapping):
        raise PtgResultArchiveSourceAuthorityError("result archive source authority snapshot manifest is invalid")
    immutable_by_field = dict(manifest)
    immutable_by_field.pop("activation", None)
    return _digest(immutable_by_field)


async def _acquire_operation_lock(session: Any, operation_id: str) -> None:
    """Serialize one archive operation before source-specific lifecycle fencing."""

    await configure_ptg2_lifecycle_transaction(session)
    try:
        await session.execute(
            text("SELECT pg_advisory_xact_lock(hashtextextended(:operation_lock_key, 0))"),
            {"operation_lock_key": ("ptg_result_archive_source_authority_v1:" + operation_id)},
        )
    except Exception as exc:
        if not is_retryable_lifecycle_database_error(exc):
            raise
        raise PTG2LifecycleLockDeferred("result archive source authority operation is busy; retry") from exc


def _row_mapping(row: Any) -> dict[str, Any]:
    """Normalize one SQLAlchemy or async-driver row without retaining it."""

    return dict(getattr(row, "_mapping", row))


async def _authority_row(session: Any, *, schema: str, snapshot_id: str) -> dict[str, Any]:
    """Lock the exact snapshot and immutable frozen binding after fencing GC."""

    result = await session.execute(
        text(
            f"""
            SELECT snapshot.snapshot_id,
                   snapshot.import_run_id,
                   snapshot.status,
                   snapshot.manifest,
                   frozen.source_file_import_id,
                   frozen.internal_run_id,
                   frozen.source_key,
                   frozen.binding_sha256,
                   frozen.binding_payload
              FROM {schema}.ptg2_snapshot AS snapshot
              JOIN {schema}.ptg2_frozen_source_file_binding AS frozen
                ON frozen.internal_run_id = snapshot.import_run_id
             WHERE snapshot.snapshot_id = :snapshot_id
             FOR KEY SHARE OF snapshot, frozen
            """
        ),
        {"snapshot_id": snapshot_id},
    )
    rows = result.all()
    if len(rows) != 1:
        raise PtgResultArchiveSourceAuthorityError("result archive source authority snapshot binding is unavailable")
    return _row_mapping(rows[0])


async def _source_key_for_snapshot(session: Any, *, schema: str, snapshot_id: str) -> str:
    """Read only the source lock key before taking any row-level lock."""

    result = await session.execute(
        text(
            f"""
            SELECT frozen.source_key
              FROM {schema}.ptg2_snapshot AS snapshot
              JOIN {schema}.ptg2_frozen_source_file_binding AS frozen
                ON frozen.internal_run_id = snapshot.import_run_id
             WHERE snapshot.snapshot_id = :snapshot_id
            """
        ),
        {"snapshot_id": snapshot_id},
    )
    rows = result.all()
    if len(rows) != 1:
        raise PtgResultArchiveSourceAuthorityError("result archive source authority snapshot binding is unavailable")
    return _required_text(_row_mapping(rows[0]).get("source_key"), "source key", 256)


def _authority_from_row(row_by_name: Mapping[str, Any], operation_id: str) -> PtgResultArchiveSourceAuthority:
    """Validate one locked snapshot/binding record and derive immutable evidence."""

    snapshot_id = _required_text(row_by_name.get("snapshot_id"), "snapshot identity", 96)
    import_run_id = _required_text(row_by_name.get("import_run_id"), "import run identity", 96)
    status = str(row_by_name.get("status") or "").strip().lower()
    if status not in _SNAPSHOT_STATUSES:
        raise PtgResultArchiveSourceAuthorityError("result archive source authority snapshot is not sealed for export")
    manifest = row_by_name.get("manifest")
    if not isinstance(manifest, Mapping):
        raise PtgResultArchiveSourceAuthorityError("result archive source authority snapshot manifest is invalid")
    source_file_import_id = _required_text(row_by_name.get("source_file_import_id"), "source file identity", 96)
    if _required_text(row_by_name.get("internal_run_id"), "binding run identity", 96) != import_run_id:
        raise PtgResultArchiveSourceAuthorityError("result archive source authority binding does not match snapshot")
    source_key = _required_text(row_by_name.get("source_key"), "source key", 256)
    binding_payload = row_by_name.get("binding_payload")
    if isinstance(binding_payload, str):
        try:
            binding_payload = json.loads(binding_payload)
        except json.JSONDecodeError as exc:
            raise PtgResultArchiveSourceAuthorityError(
                "result archive source authority frozen binding is invalid"
            ) from exc
    if not isinstance(binding_payload, Mapping):
        raise PtgResultArchiveSourceAuthorityError("result archive source authority frozen binding is invalid")
    binding_sha256 = _required_text(row_by_name.get("binding_sha256"), "frozen binding digest", 64)
    if not re.fullmatch(r"[0-9a-f]{64}", binding_sha256) or binding_sha256 != frozen_rate_binding_sha256(
        binding_payload
    ):
        raise PtgResultArchiveSourceAuthorityError("result archive source authority frozen binding changed")
    if (
        _required_text(binding_payload.get("source_file_import_id"), "binding source file identity", 96)
        != source_file_import_id
    ):
        raise PtgResultArchiveSourceAuthorityError("result archive source authority frozen binding changed")
    if _required_text(binding_payload.get("source_key"), "binding source key", 256) != source_key:
        raise PtgResultArchiveSourceAuthorityError("result archive source authority frozen binding changed")
    return PtgResultArchiveSourceAuthority(
        operation_id=operation_id,
        snapshot_id=snapshot_id,
        source_file_import_id=source_file_import_id,
        source_key=source_key,
        snapshot_manifest_sha256=result_archive_manifest_sha256(manifest),
        frozen_binding_sha256=binding_sha256,
    )


async def _pin_rows(session: Any, *, schema: str, authority: PtgResultArchiveSourceAuthority) -> list[dict[str, Any]]:
    """Lock every row for this operation, detecting a conflicting replay."""

    result = await session.execute(
        text(
            f"""
            SELECT snapshot_id, reason
              FROM {schema}.ptg2_snapshot_pin
             WHERE owner_type = :owner_type
               AND owner_id = :owner_id
             ORDER BY snapshot_id
             FOR UPDATE
            """
        ),
        {
            "owner_type": PTG_RESULT_ARCHIVE_SOURCE_PIN_OWNER_TYPE,
            "owner_id": authority.owner_id,
        },
    )
    return [_row_mapping(row) for row in result.all()]


async def _insert_or_verify_pin(session: Any, *, schema: str, authority: PtgResultArchiveSourceAuthority) -> None:
    """Retain exactly the captured snapshot and reject operation-identity replay."""

    expected_rows = [
        {
            "snapshot_id": authority.snapshot_id,
            "reason": authority.pin_reason,
        }
    ]
    existing_rows = await _pin_rows(session, schema=schema, authority=authority)
    if existing_rows:
        if existing_rows != expected_rows:
            raise PtgResultArchiveSourceAuthorityError(
                "result archive source authority pin conflicts with this operation"
            )
        return
    await session.execute(
        text(
            f"""
            INSERT INTO {schema}.ptg2_snapshot_pin
                (owner_type, owner_id, snapshot_id, reason, created_at)
            SELECT :owner_type, :owner_id, snapshot.snapshot_id,
                   :reason, transaction_timestamp()
              FROM {schema}.ptg2_snapshot AS snapshot
             WHERE snapshot.snapshot_id = :snapshot_id
               AND lower(snapshot.status) IN ('validated', 'published')
            ON CONFLICT (owner_type, owner_id, snapshot_id) DO NOTHING
            """
        ),
        {
            "owner_type": PTG_RESULT_ARCHIVE_SOURCE_PIN_OWNER_TYPE,
            "owner_id": authority.owner_id,
            "snapshot_id": authority.snapshot_id,
            "reason": authority.pin_reason,
        },
    )
    if await _pin_rows(session, schema=schema, authority=authority) != expected_rows:
        raise PtgResultArchiveSourceAuthorityError("result archive source authority pin conflicts with this operation")


def validate_ptg_result_archive_source_authority(authority_by_field: Mapping[str, Any]) -> dict[str, Any]:
    """Validate the closed serializable source-publication callback payload."""

    if not isinstance(authority_by_field, Mapping) or set(authority_by_field) != {
        "contract",
        "operation_id",
        "snapshot_id",
        "source_file_import_id",
        "source_key",
        "snapshot_manifest_sha256",
        "frozen_binding_sha256",
        "pin",
    }:
        raise PtgResultArchiveSourceAuthorityError("result archive source authority receipt is invalid")
    operation_id = _operation_id(authority_by_field["operation_id"])
    validated_authority = PtgResultArchiveSourceAuthority(
        operation_id=operation_id,
        snapshot_id=_required_text(authority_by_field["snapshot_id"], "snapshot identity", 96),
        source_file_import_id=_required_text(authority_by_field["source_file_import_id"], "source file identity", 96),
        source_key=_required_text(authority_by_field["source_key"], "source key", 256),
        snapshot_manifest_sha256=_required_text(
            authority_by_field["snapshot_manifest_sha256"], "snapshot manifest digest", 64
        ),
        frozen_binding_sha256=_required_text(authority_by_field["frozen_binding_sha256"], "frozen binding digest", 64),
    )
    if (
        authority_by_field["contract"] != PTG_RESULT_ARCHIVE_SOURCE_AUTHORITY_CONTRACT
        or not all(
            re.fullmatch(r"[0-9a-f]{64}", digest)
            for digest in (validated_authority.snapshot_manifest_sha256, validated_authority.frozen_binding_sha256)
        )
        or authority_by_field["pin"]
        != {
            "owner_type": PTG_RESULT_ARCHIVE_SOURCE_PIN_OWNER_TYPE,
            "owner_id": validated_authority.owner_id,
            "snapshot_id": validated_authority.snapshot_id,
            "reason": validated_authority.pin_reason,
        }
    ):
        raise PtgResultArchiveSourceAuthorityError("result archive source authority receipt is invalid")
    return validated_authority.as_dict()


async def prepare_ptg_result_archive_source_authority(
    session: Any,
    *,
    schema_name: str,
    operation_id: str,
    snapshot_id: str,
) -> PtgResultArchiveSourceAuthority:
    """Capture one immutable source receipt before its retention pin is committed.

    The caller owns and commits the short transaction.  The helper first reads
    the source key without holding row locks, then takes the lifecycle fence and
    re-reads the exact snapshot/binding before returning its receipt.  That lock
    order avoids waiting on the lifecycle fence while blocking GC row cleanup.
    """

    _require_transaction(session)
    schema = _quote_ident(schema_name)
    operation_id = _operation_id(operation_id)
    snapshot_id = _required_text(snapshot_id, "snapshot identity", 96)
    await _acquire_operation_lock(session, operation_id)
    source_key = await _source_key_for_snapshot(session, schema=schema, snapshot_id=snapshot_id)
    await acquire_ptg2_source_lifecycle_lock(session, source_key=source_key)
    locked_record = await _authority_row(session, schema=schema, snapshot_id=snapshot_id)
    authority = _authority_from_row(locked_record, operation_id)
    if authority.source_key != source_key:
        raise PtgResultArchiveSourceAuthorityError("result archive source authority changed while it was captured")
    return authority


async def commit_ptg_result_archive_source_authority(
    session: Any,
    *,
    schema_name: str,
    authority: Mapping[str, Any],
) -> PtgResultArchiveSourceAuthority:
    """Create or verify the exact retention pin for a durable prepared receipt."""

    _require_transaction(session)
    authority_by_field = validate_ptg_result_archive_source_authority(authority)
    expected = PtgResultArchiveSourceAuthority(
        operation_id=authority_by_field["operation_id"],
        snapshot_id=authority_by_field["snapshot_id"],
        source_file_import_id=authority_by_field["source_file_import_id"],
        source_key=authority_by_field["source_key"],
        snapshot_manifest_sha256=authority_by_field["snapshot_manifest_sha256"],
        frozen_binding_sha256=authority_by_field["frozen_binding_sha256"],
    )
    schema = _quote_ident(schema_name)
    await _acquire_operation_lock(session, expected.operation_id)
    await acquire_ptg2_source_lifecycle_lock(session, source_key=expected.source_key)
    actual = _authority_from_row(
        await _authority_row(session, schema=schema, snapshot_id=expected.snapshot_id),
        expected.operation_id,
    )
    if actual != expected:
        raise PtgResultArchiveSourceAuthorityError("result archive source authority changed after preparation")
    await _insert_or_verify_pin(session, schema=schema, authority=expected)
    return actual


async def capture_ptg_result_archive_source_authority(
    session: Any,
    *,
    schema_name: str,
    operation_id: str,
    snapshot_id: str,
) -> PtgResultArchiveSourceAuthority:
    """Capture and pin one source receipt for an existing single-transaction caller."""

    authority = await prepare_ptg_result_archive_source_authority(
        session,
        schema_name=schema_name,
        operation_id=operation_id,
        snapshot_id=snapshot_id,
    )
    return await commit_ptg_result_archive_source_authority(
        session,
        schema_name=schema_name,
        authority=authority.as_dict(),
    )


async def revalidate_ptg_result_archive_source_authority(
    session: Any,
    *,
    schema_name: str,
    authority: Mapping[str, Any],
) -> PtgResultArchiveSourceAuthority:
    """Recheck the source receipt and owned pin before a trusted next phase."""

    _require_transaction(session)
    authority_by_field = validate_ptg_result_archive_source_authority(authority)
    expected = PtgResultArchiveSourceAuthority(
        operation_id=authority_by_field["operation_id"],
        snapshot_id=authority_by_field["snapshot_id"],
        source_file_import_id=authority_by_field["source_file_import_id"],
        source_key=authority_by_field["source_key"],
        snapshot_manifest_sha256=authority_by_field["snapshot_manifest_sha256"],
        frozen_binding_sha256=authority_by_field["frozen_binding_sha256"],
    )
    schema = _quote_ident(schema_name)
    await _acquire_operation_lock(session, expected.operation_id)
    await acquire_ptg2_source_lifecycle_lock(session, source_key=expected.source_key)
    actual = _authority_from_row(
        await _authority_row(session, schema=schema, snapshot_id=expected.snapshot_id),
        expected.operation_id,
    )
    if actual != expected:
        raise PtgResultArchiveSourceAuthorityError("result archive source authority changed after capture")
    if await _pin_rows(session, schema=schema, authority=expected) != [
        {"snapshot_id": expected.snapshot_id, "reason": expected.pin_reason}
    ]:
        raise PtgResultArchiveSourceAuthorityError("result archive source authority pin is unavailable")
    return actual


async def lock_ptg_result_archive_for_clone(
    session: Any,
    *,
    schema_name: str,
    authority: Mapping[str, Any],
) -> PtgResultArchiveSourceAuthority:
    """Lock the exact retained source evidence for one caller-owned RR clone.

    Capture and terminal release take the bounded operation/source advisory
    fences.  A clone can be long-running, so it instead holds only the exact
    snapshot, frozen binding, and owner pin rows in its existing transaction.
    The durable FK-backed pin prevents retention cleanup while this row lock is
    held; no lifecycle advisory lock escapes into the clone lifetime.
    """

    _require_transaction(session)
    authority_by_field = validate_ptg_result_archive_source_authority(authority)
    expected = PtgResultArchiveSourceAuthority(
        operation_id=authority_by_field["operation_id"],
        snapshot_id=authority_by_field["snapshot_id"],
        source_file_import_id=authority_by_field["source_file_import_id"],
        source_key=authority_by_field["source_key"],
        snapshot_manifest_sha256=authority_by_field["snapshot_manifest_sha256"],
        frozen_binding_sha256=authority_by_field["frozen_binding_sha256"],
    )
    actual = _authority_from_row(
        await _authority_row(session, schema=_quote_ident(schema_name), snapshot_id=expected.snapshot_id),
        expected.operation_id,
    )
    if actual != expected:
        raise PtgResultArchiveSourceAuthorityError("result archive source authority changed after capture")
    if await _pin_rows(session, schema=_quote_ident(schema_name), authority=expected) != [
        {"snapshot_id": expected.snapshot_id, "reason": expected.pin_reason}
    ]:
        raise PtgResultArchiveSourceAuthorityError("result archive source authority pin is unavailable")
    return actual


def _authority_from_receipt(authority_by_field: Mapping[str, Any]) -> PtgResultArchiveSourceAuthority:
    """Rebuild the exact checked authority from one closed receipt."""

    validated_by_field = validate_ptg_result_archive_source_authority(authority_by_field)
    return PtgResultArchiveSourceAuthority(
        operation_id=validated_by_field["operation_id"],
        snapshot_id=validated_by_field["snapshot_id"],
        source_file_import_id=validated_by_field["source_file_import_id"],
        source_key=validated_by_field["source_key"],
        snapshot_manifest_sha256=validated_by_field["snapshot_manifest_sha256"],
        frozen_binding_sha256=validated_by_field["frozen_binding_sha256"],
    )


async def _release_authority_pin(
    session: Any,
    *,
    schema_name: str,
    authority_by_field: Mapping[str, Any],
    acknowledge_absent: bool,
) -> str:
    """Delete one exact pin or acknowledge only its already-absent replay."""

    _require_transaction(session)
    validated_authority = _authority_from_receipt(authority_by_field)
    schema = _quote_ident(schema_name)
    await _acquire_operation_lock(session, validated_authority.operation_id)
    try:
        stored_pin_rows = await _pin_rows(session, schema=schema, authority=validated_authority)
    except Exception as exc:
        if not is_retryable_lifecycle_database_error(exc):
            raise
        raise PTG2LifecycleLockDeferred("result archive source authority release is busy; retry") from exc
    expected_pin_rows = [{"snapshot_id": validated_authority.snapshot_id, "reason": validated_authority.pin_reason}]
    if not stored_pin_rows:
        if acknowledge_absent:
            return "already_released"
        raise PtgResultArchiveSourceAuthorityError("result archive source authority pin is unavailable")
    if stored_pin_rows != expected_pin_rows:
        raise PtgResultArchiveSourceAuthorityError("result archive source authority pin is unavailable")
    deletion_result = await session.execute(
        text(
            f"""
            DELETE FROM {schema}.ptg2_snapshot_pin
             WHERE owner_type = :owner_type
               AND owner_id = :owner_id
               AND snapshot_id = :snapshot_id
               AND reason = :reason
            RETURNING snapshot_id
            """
        ),
        {
            "owner_type": PTG_RESULT_ARCHIVE_SOURCE_PIN_OWNER_TYPE,
            "owner_id": validated_authority.owner_id,
            "snapshot_id": validated_authority.snapshot_id,
            "reason": validated_authority.pin_reason,
        },
    )
    if len(deletion_result.all()) != 1:
        raise PtgResultArchiveSourceAuthorityError("result archive source authority pin could not be released")
    return "released"


async def release_ptg_result_archive_source_authority(
    session: Any,
    *,
    schema_name: str,
    authority: Mapping[str, Any],
) -> int:
    """Release only an exact owned pin after trusted terminal persistence.

    The coordinator must persist its completion or cancellation outcome before
    calling this function.  Revalidation is intentionally separate: a
    validated snapshot may be published with its activation metadata updated
    after source capture.  This helper neither invents terminal evidence nor
    exposes deletion by operation id alone.
    """

    outcome = await _release_authority_pin(
        session,
        schema_name=schema_name,
        authority_by_field=authority,
        acknowledge_absent=False,
    )
    assert outcome == "released"
    return 1


async def reconcile_ptg_archive_source_release(
    session: Any,
    *,
    schema_name: str,
    authority: Mapping[str, Any],
) -> str:
    """Acknowledge an exact terminal release after durable coordinator persistence.

    This is intentionally not a general cleanup API.  The coordinator must
    have already persisted its terminal release-pending record.  A different
    owner row remains an error; only the same exact owner with no rows is an
    idempotent acknowledgment after an uncertain source commit.
    """

    return await _release_authority_pin(
        session,
        schema_name=schema_name,
        authority_by_field=authority,
        acknowledge_absent=True,
    )


__all__ = [
    "PTG_RESULT_ARCHIVE_SOURCE_AUTHORITY_CONTRACT",
    "PTG_RESULT_ARCHIVE_SOURCE_PIN_OWNER_TYPE",
    "PtgResultArchiveSourceAuthority",
    "PtgResultArchiveSourceAuthorityError",
    "capture_ptg_result_archive_source_authority",
    "lock_ptg_result_archive_for_clone",
    "reconcile_ptg_archive_source_release",
    "release_ptg_result_archive_source_authority",
    "revalidate_ptg_result_archive_source_authority",
    "result_archive_manifest_sha256",
    "validate_ptg_result_archive_source_authority",
]
