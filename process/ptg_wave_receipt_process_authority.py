"""Process-role isolation for the immutable PTG receipt signing keyring."""

from __future__ import annotations

import os
from collections.abc import Mapping

from process.ptg_wave_receipt_authority import (
    PTGWaveReceiptAuthorityError,
    PTGWaveReceiptKeyring,
    load_configured_receipt_keyring,
)


RECEIPT_AUTHORITY_ROLE_ENV = "HLTHPRT_PTG_WAVE_RECEIPT_AUTHORITY_ROLE"
API_WORKERS_ENV = "HLTHPRT_API_WORKERS"
RECEIPT_AUTHORITY_ROLE_READER = "reader"
RECEIPT_AUTHORITY_ROLE_SIGNER = "signer"


def receipt_authority_role(environ: Mapping[str, str] | None = None) -> str:
    """Return the closed process role used for receipt-bearing mutations."""

    environment = os.environ if environ is None else environ
    role = str(
        environment.get(RECEIPT_AUTHORITY_ROLE_ENV)
        or RECEIPT_AUTHORITY_ROLE_READER
    ).strip().lower()
    if role not in {
        RECEIPT_AUTHORITY_ROLE_READER,
        RECEIPT_AUTHORITY_ROLE_SIGNER,
    }:
        raise PTGWaveReceiptAuthorityError(
            "receipt authority role must be reader or signer"
        )
    return role


def require_receipt_authority_worker_count(worker_count: object) -> int:
    """Require one API worker for the process-local signing authority."""

    try:
        normalized_count = int(worker_count)
    except (TypeError, ValueError) as exc:
        raise PTGWaveReceiptAuthorityError(
            "receipt authority requires exactly one API worker"
        ) from exc
    if normalized_count != 1:
        raise PTGWaveReceiptAuthorityError(
            "receipt authority requires exactly one API worker"
        )
    return normalized_count


def load_process_receipt_keyring(
    environ: Mapping[str, str] | None = None,
) -> PTGWaveReceiptKeyring | None:
    """Load the signer once or validate a configuration-free reader."""

    environment = os.environ if environ is None else environ
    role = receipt_authority_role(environment)
    configured_names = tuple(
        name
        for name in (
            "HLTHPRT_PTG_WAVE_RECEIPT_ACTIVE_KEY_ID",
            "HLTHPRT_PTG_WAVE_RECEIPT_ACTIVE_PRIVATE_KEY_FILE",
            "HLTHPRT_PTG_WAVE_RECEIPT_RETAINED_PRIVATE_KEY_FILES_JSON",
            "HLTHPRT_PTG_WAVE_RECEIPT_RETIRED_PUBLIC_EPOCHS_FILE",
        )
        if name in environment
    )
    if role == RECEIPT_AUTHORITY_ROLE_READER:
        if configured_names:
            raise PTGWaveReceiptAuthorityError(
                "receipt key configuration is forbidden on a reader process"
            )
        return None
    require_receipt_authority_worker_count(environment.get(API_WORKERS_ENV, "1"))
    if not configured_names:
        raise PTGWaveReceiptAuthorityError("receipt signer configuration is absent")
    keyring = load_configured_receipt_keyring()
    if keyring is None:
        raise PTGWaveReceiptAuthorityError("receipt signer configuration is absent")
    return keyring


def require_process_receipt_keyring(
    keyring: PTGWaveReceiptKeyring | None,
) -> PTGWaveReceiptKeyring:
    """Reject receipt-bearing work outside the process signer role."""

    if keyring is None:
        raise PTGWaveReceiptAuthorityError(
            "receipt authority is unavailable in this process"
        )
    return keyring


__all__ = [
    "API_WORKERS_ENV",
    "RECEIPT_AUTHORITY_ROLE_ENV",
    "RECEIPT_AUTHORITY_ROLE_READER",
    "RECEIPT_AUTHORITY_ROLE_SIGNER",
    "load_process_receipt_keyring",
    "receipt_authority_role",
    "require_process_receipt_keyring",
    "require_receipt_authority_worker_count",
]
