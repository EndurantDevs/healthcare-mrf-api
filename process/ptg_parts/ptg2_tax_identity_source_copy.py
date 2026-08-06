# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Re-authenticate an ephemeral source-local tax projection COPY file."""

from __future__ import annotations

import hashlib
import hmac
import os
from typing import BinaryIO

from process.ptg_parts.ptg2_tax_identity_source_projection import (
    PreparedTaxIdentitySourceProjection,
    _has_prepared_copy_identity,
)

_COPY_READ_BYTES = 1024 * 1024


def _is_copy_file_unchanged(
    copy_file: BinaryIO,
    prepared: PreparedTaxIdentitySourceProjection,
) -> bool:
    try:
        initial_metadata = os.fstat(copy_file.fileno())
        if not _has_prepared_copy_identity(initial_metadata, prepared):
            return False
        copy_file.seek(0)
        observed_sha256 = hashlib.sha256()
        observed_byte_count = 0
        while file_chunk := copy_file.read(_COPY_READ_BYTES):
            observed_sha256.update(file_chunk)
            observed_byte_count += len(file_chunk)
        opened_metadata = os.fstat(copy_file.fileno())
        current_metadata = os.lstat(prepared.copy_path)
        return (
            observed_byte_count == prepared.copy_byte_count
            and hmac.compare_digest(observed_sha256.hexdigest(), prepared.copy_sha256)
            and _has_prepared_copy_identity(opened_metadata, prepared)
            and _has_prepared_copy_identity(current_metadata, prepared)
        )
    except Exception:
        return False


__all__: list[str] = []
