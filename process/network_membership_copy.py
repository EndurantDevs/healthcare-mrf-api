# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Land one native membership batch into an authenticated isolated candidate."""

from __future__ import annotations

import asyncio
import hashlib
import importlib
import io
import re
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

MAX_INPUT_BYTES = 8 * 1024 * 1024
MAX_COPY_BYTES = 16 * 1024 * 1024
MAX_ROWS = 5_000
COPY_COLUMNS = ("network_id", "provider_system", "provider_id", "location_id", "evidence_id")
_COPY_HEADER = b"PGCOPY\n\xff\r\n\0\0\0\0\0\0\0\0\0"


class MembershipCopyError(ValueError):
    """A batch was rejected without committing candidate data."""


@dataclass(frozen=True)
class MembershipCopyTarget:
    dataset_id: str
    schema_id: str
    producer_id: str
    candidate_id: str
    schema_name: str
    table_name: str = "network_membership"

    def __post_init__(self) -> None:
        for value in (self.dataset_id, self.schema_id, self.producer_id, self.candidate_id):
            try:
                parsed = uuid.UUID(value) if type(value) is str else None
            except ValueError:
                parsed = None
            if parsed is None or parsed.int == 0 or str(parsed) != value:
                raise MembershipCopyError("Candidate identity must be a canonical nonzero UUID")
        if (
            self.schema_name != f"network_candidate_{uuid.UUID(self.candidate_id).hex}"
            or self.table_name != "network_membership"
        ):
            raise MembershipCopyError("Membership COPY target must be the exact isolated candidate relation")


@dataclass(frozen=True)
class MembershipCopyReceipt:
    target: MembershipCopyTarget
    row_count: int
    input_sha256: str
    copy_sha256: str
    input_byte_count: int
    copy_byte_count: int


CandidateAuthority = Callable[[Any, MembershipCopyTarget], Awaitable[MembershipCopyTarget]]


def _encode(input_bytes: bytes) -> tuple[bytes, int]:
    try:
        native = importlib.import_module("ptg2_address_canon")
        encoder = native.encode_network_membership_batch
    except ImportError, AttributeError:
        raise MembershipCopyError("Native membership encoder is unavailable") from None
    copy_bytes, row_count = encoder(input_bytes)
    if (
        type(copy_bytes) is not bytes
        or type(row_count) is not int
        or not 0 <= row_count <= MAX_ROWS
        or not 21 <= len(copy_bytes) <= MAX_COPY_BYTES
        or not copy_bytes.startswith(_COPY_HEADER)
        or not copy_bytes.endswith(b"\xff\xff")
    ):
        raise MembershipCopyError("Native membership COPY framing or bounds are invalid")
    return copy_bytes, row_count


async def copy_network_membership_batch(
    connection: Any,
    *,
    copy_target: MembershipCopyTarget,
    input_bytes: bytes,
    expected_input_sha256: str,
    require_candidate_authority: CandidateAuthority,
) -> MembershipCopyReceipt:
    """Use the caller's transaction; failures roll back this batch's savepoint.

    The authority callback is trusted application control code. It must verify
    complete dataset/schema/producer ownership and an open isolated candidate,
    returning the exact verified target while holding the required control lock.
    This adapter neither authenticates callers nor opens a writer outside that
    control path. Successful receipts remain uncommitted until the caller commits.
    """
    if type(input_bytes) is not bytes or len(input_bytes) > MAX_INPUT_BYTES:
        raise MembershipCopyError("Membership input must be bounded bytes")
    if type(copy_target) is not MembershipCopyTarget or not callable(require_candidate_authority):
        raise MembershipCopyError("Authenticated candidate context is required")
    if type(expected_input_sha256) is not str or re.fullmatch(r"[0-9a-f]{64}", expected_input_sha256) is None:
        raise MembershipCopyError("Expected input digest is invalid")
    input_digest = hashlib.sha256(input_bytes).hexdigest()
    if input_digest != expected_input_sha256:
        raise MembershipCopyError("Membership input digest mismatch")
    if not connection.is_in_transaction():
        raise MembershipCopyError("Membership COPY requires a caller-owned transaction")

    async with connection.transaction():
        verified = await require_candidate_authority(connection, copy_target)
        if type(verified) is not MembershipCopyTarget or verified != copy_target:
            raise MembershipCopyError("Candidate authority does not match the requested target")
        copy_bytes, row_count = await asyncio.to_thread(_encode, input_bytes)
        copy_digest = hashlib.sha256(copy_bytes).hexdigest()
        with io.BytesIO(copy_bytes) as copy_source:
            status = await connection.copy_to_table(
                copy_target.table_name,
                schema_name=copy_target.schema_name,
                columns=COPY_COLUMNS,
                format="binary",
                source=copy_source,
            )
            if status != f"COPY {row_count}" or copy_source.tell() != len(copy_bytes):
                raise MembershipCopyError("Membership COPY count or driver consumption mismatch")
        verified = await require_candidate_authority(connection, copy_target)
        if type(verified) is not MembershipCopyTarget or verified != copy_target:
            raise MembershipCopyError("Candidate authority changed during membership COPY")
    return MembershipCopyReceipt(copy_target, row_count, input_digest, copy_digest, len(input_bytes), len(copy_bytes))
