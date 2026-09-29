# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Operation-owned export authority for an already published PTG result."""

from __future__ import annotations

import datetime
import hashlib
import re
from dataclasses import dataclass
from typing import Any, Mapping

from sqlalchemy import text

from process.ptg_parts.canonical import canonical_json_dumps
from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.ptg2_lifecycle_lock import acquire_ptg2_source_lifecycle_lock
from process.ptg_parts.result_archive_published_identity import load_published_result_identity
from process.ptg_parts.result_archive_source_authority import (
    PTG_PUBLISHED_RESULT_SOURCE_AUTHORITY_CONTRACT,
    PTG_RESULT_ARCHIVE_SOURCE_PIN_OWNER_TYPE,
    PtgResultArchiveSourceAuthorityError,
    _acquire_operation_lock,
    _bounded_clone_lock_reads,
    _insert_or_verify_pin,
    _operation_id,
    _pin_rows,
    _release_validated_authority_pin,
    _require_transaction,
)

_IDENTITY_FIELDS = (
    "snapshot_id",
    "import_run_id",
    "import_month",
    "source_key",
    "snapshot_manifest_sha256",
    "snapshot_key",
    "plan_id",
    "plan_market_type",
    "coverage_scope_id",
    "source_set_digest",
    "source_count",
    "plan_scopes_sha256",
    "source_assignments_sha256",
    "layout_mapping_digest",
    "map_digest",
    "finalizer_map_digest",
)
_DIGEST_FIELDS = frozenset(
    {
        "snapshot_manifest_sha256",
        "coverage_scope_id",
        "source_set_digest",
        "plan_scopes_sha256",
        "source_assignments_sha256",
        "layout_mapping_digest",
        "map_digest",
        "finalizer_map_digest",
    }
)


class PtgPublishedResultSourceAuthorityError(PtgResultArchiveSourceAuthorityError):
    """A published result or its exact owned retention pin is unavailable."""


@dataclass(frozen=True)
class PtgPublishedResultSourceAuthority:
    operation_id: str
    identity: Mapping[str, Any]

    @property
    def snapshot_id(self) -> str:
        """Return the exact source snapshot identifier."""
        return self.identity["snapshot_id"]

    @property
    def source_key(self) -> str:
        """Return the reviewed source identity."""
        return self.identity["source_key"]

    @property
    def owner_id(self) -> str:
        """Derive the operation-owned retention pin identifier."""
        return hashlib.sha256(f"{PTG_RESULT_ARCHIVE_SOURCE_PIN_OWNER_TYPE}:v1:{self.operation_id}".encode()).hexdigest()

    @property
    def pin_reason(self) -> str:
        """Bind the retention reason to the complete published identity."""
        digest = hashlib.sha256(
            canonical_json_dumps(
                {
                    "contract": PTG_PUBLISHED_RESULT_SOURCE_AUTHORITY_CONTRACT,
                    "operation_id": self.operation_id,
                    "identity": dict(self.identity),
                }
            ).encode()
        ).hexdigest()
        return f"retain published result archive source {digest}"

    def retention_pin(self) -> dict[str, str]:
        """Return the existing closure pin interface without changing its ownership."""
        return {
            "pin_id": f"{PTG_RESULT_ARCHIVE_SOURCE_PIN_OWNER_TYPE}:{self.owner_id}",
            "repeatable_read_token": self.operation_id,
        }

    def as_dict(self) -> dict[str, Any]:
        """Serialize the closed tagged authority receipt."""
        return {
            "contract": PTG_PUBLISHED_RESULT_SOURCE_AUTHORITY_CONTRACT,
            "operation_id": self.operation_id,
            "snapshot_id": self.snapshot_id,
            "source_key": self.source_key,
            "identity": dict(self.identity),
            "pin": {
                "owner_type": PTG_RESULT_ARCHIVE_SOURCE_PIN_OWNER_TYPE,
                "owner_id": self.owner_id,
                "snapshot_id": self.snapshot_id,
                "reason": self.pin_reason,
            },
        }


def _authority_from_identity(operation_id: str, identity: Mapping[str, Any]) -> PtgPublishedResultSourceAuthority:
    identity_by_field: dict[str, Any] = {}
    for field_name in _IDENTITY_FIELDS:
        value = identity.get(field_name)
        if field_name in _DIGEST_FIELDS and isinstance(value, (bytes, bytearray, memoryview)):
            value = bytes(value).hex()
        identity_by_field[field_name] = value
    return _validated_authority(operation_id, identity_by_field)


def _validated_authority(operation_id: str, identity: Mapping[str, Any]) -> PtgPublishedResultSourceAuthority:
    try:
        operation_id = _operation_id(operation_id)
    except PtgResultArchiveSourceAuthorityError as exc:
        raise PtgPublishedResultSourceAuthorityError("published result authority operation is invalid") from exc
    if not isinstance(identity, Mapping) or set(identity) != set(_IDENTITY_FIELDS):
        raise PtgPublishedResultSourceAuthorityError("published result authority identity is invalid")
    identity_by_field = dict(identity)
    if any(
        not isinstance(identity_by_field[field], str) or not re.fullmatch(r"[0-9a-f]{64}", identity_by_field[field])
        for field in _DIGEST_FIELDS
    ):
        raise PtgPublishedResultSourceAuthorityError("published result authority digest is invalid")
    maximum_length_by_field = {
        "snapshot_id": 96,
        "import_run_id": 96,
        "source_key": 256,
        "plan_id": 256,
        "plan_market_type": 64,
    }
    if any(
        not isinstance(identity_by_field[field], str)
        or not identity_by_field[field].strip()
        or len(identity_by_field[field]) > maximum
        for field, maximum in maximum_length_by_field.items()
    ):
        raise PtgPublishedResultSourceAuthorityError("published result authority scope is invalid")
    try:
        month = datetime.date.fromisoformat(identity_by_field["import_month"])
    except (TypeError, ValueError) as exc:
        raise PtgPublishedResultSourceAuthorityError("published result authority month is invalid") from exc
    if month.day != 1 or identity_by_field["import_month"] != month.isoformat():
        raise PtgPublishedResultSourceAuthorityError("published result authority month is invalid")
    if any(
        isinstance(identity_by_field[field], bool)
        or not isinstance(identity_by_field[field], int)
        or identity_by_field[field] <= 0
        for field in ("snapshot_key", "source_count")
    ):
        raise PtgPublishedResultSourceAuthorityError("published result authority count is invalid")
    return PtgPublishedResultSourceAuthority(operation_id, identity_by_field)


def validate_ptg_published_result_source_authority(receipt: Mapping[str, Any]) -> dict[str, Any]:
    """Validate the closed, tagged source receipt independently of the database."""

    if not isinstance(receipt, Mapping) or set(receipt) != {
        "contract",
        "operation_id",
        "snapshot_id",
        "source_key",
        "identity",
        "pin",
    }:
        raise PtgPublishedResultSourceAuthorityError("published result authority receipt is invalid")
    authority = _validated_authority(receipt["operation_id"], receipt["identity"])
    if receipt != authority.as_dict():
        raise PtgPublishedResultSourceAuthorityError("published result authority receipt changed")
    return authority.as_dict()


def _from_receipt(receipt: Mapping[str, Any]) -> PtgPublishedResultSourceAuthority:
    validated = validate_ptg_published_result_source_authority(receipt)
    return _validated_authority(validated["operation_id"], validated["identity"])


async def _source_key_for_snapshot(session: Any, schema_name: str, snapshot_id: str) -> str:
    schema = _quote_ident(schema_name)
    result = await session.execute(
        text(
            f"SELECT internal_run.options ->> 'source_key' FROM {schema}.ptg2_snapshot snapshot "
            f"JOIN {schema}.ptg2_import_run internal_run ON internal_run.import_run_id=snapshot.import_run_id "
            "WHERE snapshot.snapshot_id=:snapshot_id"
        ),
        {"snapshot_id": snapshot_id},
    )
    source_key = str(result.scalar_one_or_none() or "").strip().lower()
    if not source_key:
        raise PtgPublishedResultSourceAuthorityError("published result source ownership is unavailable")
    return source_key


async def _current_authority(
    session: Any, *, schema_name: str, operation_id: str, snapshot_id: str
) -> PtgPublishedResultSourceAuthority:
    try:
        identity = await load_published_result_identity(
            session, schema_name=schema_name, snapshot_id=snapshot_id, lock=True
        )
        return _authority_from_identity(operation_id, identity)
    except ValueError as exc:
        raise PtgPublishedResultSourceAuthorityError("published result source evidence is unavailable") from exc


async def prepare_ptg_published_result_source_authority(
    session: Any, *, schema_name: str, operation_id: str, snapshot_id: str
) -> PtgPublishedResultSourceAuthority:
    """Capture immutable result evidence before the caller commits its pin."""

    _require_transaction(session)
    operation_id = _operation_id(operation_id)
    snapshot_id = str(snapshot_id or "").strip()
    if not snapshot_id:
        raise PtgPublishedResultSourceAuthorityError("published result snapshot is required")
    await _acquire_operation_lock(session, operation_id)
    source_key = await _source_key_for_snapshot(session, schema_name, snapshot_id)
    await acquire_ptg2_source_lifecycle_lock(session, source_key=source_key)
    authority = await _current_authority(
        session, schema_name=schema_name, operation_id=operation_id, snapshot_id=snapshot_id
    )
    if authority.source_key != source_key:
        raise PtgPublishedResultSourceAuthorityError("published result source changed during capture")
    return authority


async def commit_ptg_published_result_source_authority(
    session: Any, *, schema_name: str, authority: Mapping[str, Any]
) -> PtgPublishedResultSourceAuthority:
    """Pin only the exact source result captured in a durable prepared receipt."""

    _require_transaction(session)
    expected = _from_receipt(authority)
    await _acquire_operation_lock(session, expected.operation_id)
    await acquire_ptg2_source_lifecycle_lock(session, source_key=expected.source_key)
    actual = await _current_authority(
        session, schema_name=schema_name, operation_id=expected.operation_id, snapshot_id=expected.snapshot_id
    )
    if actual != expected:
        raise PtgPublishedResultSourceAuthorityError("published result changed after preparation")
    await _insert_or_verify_pin(session, schema=_quote_ident(schema_name), authority=expected)
    return actual


async def capture_ptg_published_result_source_authority(
    session: Any, *, schema_name: str, operation_id: str, snapshot_id: str
) -> PtgPublishedResultSourceAuthority:
    """Capture and pin a published result in one caller-owned transaction."""

    prepared = await prepare_ptg_published_result_source_authority(
        session, schema_name=schema_name, operation_id=operation_id, snapshot_id=snapshot_id
    )
    return await commit_ptg_published_result_source_authority(
        session, schema_name=schema_name, authority=prepared.as_dict()
    )


async def revalidate_ptg_published_result_source_authority(
    session: Any, *, schema_name: str, authority: Mapping[str, Any]
) -> PtgPublishedResultSourceAuthority:
    """Recheck exact source evidence and owned pin before a trusted next phase."""

    _require_transaction(session)
    expected = _from_receipt(authority)
    await _acquire_operation_lock(session, expected.operation_id)
    await acquire_ptg2_source_lifecycle_lock(session, source_key=expected.source_key)
    actual = await _current_authority(
        session, schema_name=schema_name, operation_id=expected.operation_id, snapshot_id=expected.snapshot_id
    )
    pins = await _pin_rows(session, schema=_quote_ident(schema_name), authority=expected)
    if actual != expected or pins != [{"snapshot_id": expected.snapshot_id, "reason": expected.pin_reason}]:
        raise PtgPublishedResultSourceAuthorityError("published result source pin changed")
    return actual


async def lock_ptg_published_result_for_clone(
    session: Any, *, schema_name: str, authority: Mapping[str, Any]
) -> PtgPublishedResultSourceAuthority:
    """Recheck receipt and owned pin in the clone's repeatable-read transaction."""

    _require_transaction(session)
    expected = _from_receipt(authority)
    async with _bounded_clone_lock_reads(session):
        actual = await _current_authority(
            session, schema_name=schema_name, operation_id=expected.operation_id, snapshot_id=expected.snapshot_id
        )
        pins = await _pin_rows(session, schema=_quote_ident(schema_name), authority=expected)
    if actual != expected or pins != [{"snapshot_id": expected.snapshot_id, "reason": expected.pin_reason}]:
        raise PtgPublishedResultSourceAuthorityError("published result source pin changed")
    return actual


async def reconcile_ptg_published_result_source_release(
    session: Any, *, schema_name: str, authority: Mapping[str, Any]
) -> str:
    """Release only the exact owned pin after the coordinator persists terminal state."""

    return await _release_validated_authority_pin(
        session, schema_name=schema_name, authority=_from_receipt(authority), acknowledge_absent=True
    )


async def release_ptg_published_result_source_authority(
    session: Any, *, schema_name: str, authority: Mapping[str, Any]
) -> int:
    """Release one exact pin after trusted terminal persistence."""

    outcome = await _release_validated_authority_pin(
        session, schema_name=schema_name, authority=_from_receipt(authority), acknowledge_absent=False
    )
    assert outcome == "released"
    return 1


__all__ = [
    "PTG_PUBLISHED_RESULT_SOURCE_AUTHORITY_CONTRACT",
    "PtgPublishedResultSourceAuthority",
    "PtgPublishedResultSourceAuthorityError",
    "prepare_ptg_published_result_source_authority",
    "commit_ptg_published_result_source_authority",
    "capture_ptg_published_result_source_authority",
    "revalidate_ptg_published_result_source_authority",
    "lock_ptg_published_result_for_clone",
    "reconcile_ptg_published_result_source_release",
    "release_ptg_published_result_source_authority",
    "validate_ptg_published_result_source_authority",
]
