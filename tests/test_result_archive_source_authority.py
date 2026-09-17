# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed receipt contracts for PTG result-archive source authority."""

from __future__ import annotations

import pytest

from process.ptg_parts.result_archive_source_authority import (
    PtgResultArchiveSourceAuthority,
    PtgResultArchiveSourceAuthorityError,
    capture_ptg_result_archive_source_authority,
    validate_ptg_result_archive_source_authority,
)


def _authority() -> PtgResultArchiveSourceAuthority:
    """Return one complete synthetic serializable source authority."""

    return PtgResultArchiveSourceAuthority(
        operation_id="a" * 64,
        snapshot_id="snapshot-1",
        source_file_import_id="import-1",
        source_key="source-1",
        snapshot_manifest_sha256="b" * 64,
        frozen_binding_sha256="c" * 64,
    )


def test_source_authority_is_a_closed_callback_receipt() -> None:
    """The trusted publication callback receives only exact immutable fields."""

    authority = _authority()
    assert validate_ptg_result_archive_source_authority(authority.as_dict()) == authority.as_dict()
    assert authority.retention_pin()["pin_id"].endswith(authority.owner_id)
    assert authority.retention_pin()["repeatable_read_token"] == "a" * 64


@pytest.mark.parametrize("field_name", ["snapshot_id", "frozen_binding_sha256", "pin"])
def test_source_authority_rejects_receipt_tampering(field_name: str) -> None:
    """A callback receipt cannot change an identity or detach its exact pin."""

    receipt = _authority().as_dict()
    receipt[field_name] = "changed"
    with pytest.raises(PtgResultArchiveSourceAuthorityError, match="receipt is invalid"):
        validate_ptg_result_archive_source_authority(receipt)


@pytest.mark.asyncio
async def test_source_authority_requires_transaction_before_any_database_work() -> None:
    """Authority capture does not trigger SQLAlchemy autobegin on behalf of callers."""

    with pytest.raises(PtgResultArchiveSourceAuthorityError, match="requires a caller transaction"):
        await capture_ptg_result_archive_source_authority(
            object(),
            schema_name="mrf",
            operation_id="a" * 64,
            snapshot_id="snapshot-1",
        )
