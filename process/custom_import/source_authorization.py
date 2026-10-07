# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed SOURCE envelope using the same framing and dedicated writer keyring.

Only the server verifies signatures. The worker retains opaque signed bytes;
neither this envelope nor its supplied cursor replaces locked lease authority.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime

from process.custom_import import admission_authorization as admission

SOURCE_PATH = admission.SOURCE_PATH
PERMIT_CONTRACT = admission.SOURCE_PERMIT_CONTRACT
CONTEXT_HEADER = admission.SOURCE_CONTEXT_HEADER
KEY_ID_HEADER = admission.SOURCE_KEY_ID_HEADER
SIGNATURE_HEADER = admission.SOURCE_SIGNATURE_HEADER
CURSOR_FIELDS = frozenset({"next_part_ordinal", "next_part_row_ordinal", "next_source_ordinal", "next_pack_ordinal"})
BODY_FIELDS = frozenset({"build_id", "execution_id", "fence", "stream_slot", "expected_cursor"})


@dataclass(frozen=True, slots=True)
class SourcePermit(admission.AdmissionPermit):
    """Immutable SOURCE purpose; the service rechecks it under page locks."""


@dataclass(frozen=True, slots=True)
class SourceCursor:
    next_part_ordinal: int
    next_part_row_ordinal: int
    next_source_ordinal: int
    next_pack_ordinal: int


@dataclass(frozen=True, slots=True)
class SourceBatchPins:
    build_id: int
    execution_id: int
    fence: int
    stream_slot: int
    expected_cursor: SourceCursor


@dataclass(frozen=True, slots=True)
class VerifiedSource:
    permit: SourcePermit
    pins: SourceBatchPins
    token: admission.LeaseToken = field(repr=False)


def cursor(document: object) -> SourceCursor:
    """Validate every coordinate, without deriving pack positions from row counts."""

    if type(document) is not dict or document.keys() != CURSOR_FIELDS:
        raise admission._fail()
    for name in CURSOR_FIELDS:
        minimum = 1 if name == "next_part_ordinal" else 0
        maximum = (1 << 31) - 1 if name in {"next_part_ordinal", "next_pack_ordinal"} else admission.MAX_BIGINT
        value = document[name]
        if type(value) is not int or not minimum <= value <= maximum:
            raise admission._fail()
    return SourceCursor(**document)


def batch_pins(document: object) -> SourceBatchPins:
    """Parse the five fixed SOURCE request fields and all cursor coordinates."""

    if type(document) is not dict or document.keys() != BODY_FIELDS:
        raise admission._fail()
    for name in ("build_id", "execution_id", "fence", "stream_slot"):
        admission._positive_id(document[name])
    if document["stream_slot"] > (1 << 15) - 1:
        raise admission._fail()
    return SourceBatchPins(**{**document, "expected_cursor": cursor(document["expected_cursor"])})


def verify_request(
    *,
    headers,
    body: bytes,
    method: str,
    path: str,
    query_string: str,
    trusted_now: datetime,
    expected_origin: str,
    keyring: admission.AdmissionKeyring,
) -> VerifiedSource:
    """Reject a cross-purpose permit even when the same trusted key signs it."""

    permit, token = admission._verify_authority(
        headers=headers,
        method=method,
        path=path,
        query_string=query_string,
        trusted_now=trusted_now,
        expected_origin=expected_origin,
        keyring=keyring,
        is_source=True,
    )
    return VerifiedSource(SourcePermit(**asdict(permit)), batch_pins(admission._object(body, 512)), token)
