"""Asymmetric receipt authority for fresh exact-wave operations.

The control bearer token authenticates requests, but it is deliberately not a
proof authority.  Receipts are signed by a separate RSA key epoch pinned into
the admission that created the wave.  Retained private epochs may finish only
waves already pinned to them; only the configured active epoch may admit new
waves.
"""

from __future__ import annotations

import datetime as dt
import os
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from cryptography.exceptions import InvalidSignature

from process.ptg_wave_state import canonical_json, sha256_digest


LINKAGE_RECEIPT_SCHEMA = "healthporta.ptg-wave-linkage-receipt.v2"
ABANDONMENT_RECEIPT_SCHEMA = "healthporta.ptg-wave-abandonment-receipt.v2"
ORDINARY_TERMINAL_RECEIPT_SCHEMA = "healthporta.ptg-wave-ordinary-terminal-receipt.v1"
KEY_EPOCHS_SCHEMA = "healthporta.ptg-wave-receipt-key-epochs.v1"

ACTIVE_KEY_ID_ENV = "HLTHPRT_PTG_WAVE_RECEIPT_ACTIVE_KEY_ID"
ACTIVE_PRIVATE_KEY_FILE_ENV = "HLTHPRT_PTG_WAVE_RECEIPT_ACTIVE_PRIVATE_KEY_FILE"
RETAINED_PRIVATE_KEY_FILES_ENV = "HLTHPRT_PTG_WAVE_RECEIPT_RETAINED_PRIVATE_KEY_FILES_JSON"
RETIRED_PUBLIC_EPOCHS_FILE_ENV = "HLTHPRT_PTG_WAVE_RECEIPT_RETIRED_PUBLIC_EPOCHS_FILE"
_CONFIGURATION_ENV_NAMES = (
    ACTIVE_KEY_ID_ENV,
    ACTIVE_PRIVATE_KEY_FILE_ENV,
    RETAINED_PRIVATE_KEY_FILES_ENV,
    RETIRED_PUBLIC_EPOCHS_FILE_ENV,
)

MAX_RECEIPT_KEY_EPOCHS = 8
MAX_PRIVATE_KEY_BYTES = 16 * 1024
MAX_PUBLIC_EPOCH_FILE_BYTES = 32 * 1024

_KEY_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}\Z")
_HEX_512 = re.compile(r"[0-9a-f]{512}\Z")
_ISSUED_AT = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T"
    r"[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}Z\Z"
)
_ENVELOPE_FIELDS = frozenset(
    {"schema", "key_id", "issued_at", "payload", "payload_digest", "signature"}
)


class PTGWaveReceiptAuthorityError(RuntimeError):
    """A receipt cannot be signed or verified under the configured authority."""


@dataclass(frozen=True)
class PTGWaveReceiptPublicEpoch:
    """One public verification epoch exposed to control-plane auditors."""

    key_id: str
    rsa_modulus: str
    rsa_exponent: int
    state: str

    def as_mapping(self) -> dict[str, Any]:
        """Project one public epoch into its closed wire shape."""
        return {
            "key_id": self.key_id,
            "rsa_modulus": self.rsa_modulus,
            "rsa_exponent": self.rsa_exponent,
            "state": self.state,
        }


@dataclass(frozen=True)
class _SigningEpoch:
    key_id: str
    private_key: rsa.RSAPrivateKey = field(repr=False)

    @property
    def public_epoch(self) -> PTGWaveReceiptPublicEpoch:
        """Derive the public verification material for this signer."""
        numbers = self.private_key.public_key().public_numbers()
        return PTGWaveReceiptPublicEpoch(
            key_id=self.key_id,
            rsa_modulus=f"{numbers.n:0512x}",
            rsa_exponent=numbers.e,
            state="retired",
        )


@dataclass(frozen=True)
class PTGWaveReceiptKeyring:
    """Active and retained signing epochs plus historical public epochs."""

    active_key_id: str
    signing_by_key_id: Mapping[str, _SigningEpoch] = field(repr=False)
    public_by_key_id: Mapping[str, PTGWaveReceiptPublicEpoch]

    def __post_init__(self) -> None:
        """Freeze copied epoch mappings so startup authority cannot mutate."""

        object.__setattr__(
            self,
            "signing_by_key_id",
            MappingProxyType(dict(self.signing_by_key_id)),
        )
        object.__setattr__(
            self,
            "public_by_key_id",
            MappingProxyType(dict(self.public_by_key_id)),
        )

    @classmethod
    def from_environment(cls) -> "PTGWaveReceiptKeyring":
        """Load a closed active, retained, and public receipt-key configuration."""
        from process.ptg_wave_receipt_key_configuration import (
            load_receipt_keyring_from_environment,
        )

        return load_receipt_keyring_from_environment(cls)

    def require_active_for_admission(self, key_id: object) -> str:
        """Require fresh admission to use the active signing epoch."""
        normalized = _key_id(key_id, "admission receipt key ID")
        if normalized != self.active_key_id:
            raise PTGWaveReceiptAuthorityError(
                "fresh admission must pin the active receipt key epoch"
            )
        return normalized

    def require_active_public_material(
        self,
        *,
        key_id: object,
        modulus: object,
        exponent: object,
    ) -> PTGWaveReceiptPublicEpoch:
        """Require the full trust root advertised in a fresh admission."""

        normalized = self.require_active_for_admission(key_id)
        normalized_modulus, normalized_exponent = (
            require_receipt_public_material(modulus, exponent)
        )
        epoch = self.public_by_key_id[normalized]
        if (
            epoch.rsa_modulus != normalized_modulus
            or epoch.rsa_exponent != normalized_exponent
        ):
            raise PTGWaveReceiptAuthorityError(
                "fresh admission receipt public key material is not active"
            )
        return epoch

    def require_signing_epoch(self, key_id: object) -> _SigningEpoch:
        """Return the private epoch pinned by an existing operation."""
        normalized = _key_id(key_id, "stored receipt key ID")
        epoch = self.signing_by_key_id.get(normalized)
        if epoch is None:
            raise PTGWaveReceiptAuthorityError(
                "stored receipt key epoch is unavailable for signing"
            )
        return epoch

    def public_epochs_mapping(self) -> dict[str, Any]:
        """Expose the deterministic public key-epoch projection."""
        epochs = [
            self.public_by_key_id[key_id].as_mapping()
            for key_id in sorted(self.public_by_key_id)
        ]
        if sum(epoch["state"] == "active" for epoch in epochs) != 1:
            raise PTGWaveReceiptAuthorityError(
                "receipt keyring must expose exactly one active epoch"
            )
        return {
            "schema_version": KEY_EPOCHS_SCHEMA,
            "active_key_id": self.active_key_id,
            "epochs": epochs,
        }

    def validate_stored_receipt(
        self,
        receipt: object,
        *,
        schema: str,
        key_id: object,
        expected_payload: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Verify a persisted receipt using active or historical public trust."""

        normalized_key_id = _key_id(key_id, "stored receipt key ID")
        validated = validate_receipt_envelope(receipt, schema=schema)
        if (
            validated["key_id"] != normalized_key_id
            or validated["payload"] != dict(expected_payload)
        ):
            raise PTGWaveReceiptAuthorityError(
                "stored receipt conflicts with its immutable binding"
            )
        public_epoch = self.public_by_key_id.get(normalized_key_id)
        if public_epoch is None:
            raise PTGWaveReceiptAuthorityError(
                "stored receipt public key epoch is unavailable"
            )
        message = signed_receipt_message(
            schema=schema,
            key_id=normalized_key_id,
            issued_at=validated["issued_at"],
            payload=expected_payload,
        )
        public_key = rsa.RSAPublicNumbers(
            public_epoch.rsa_exponent,
            int(public_epoch.rsa_modulus, 16),
        ).public_key()
        try:
            public_key.verify(
                bytes.fromhex(validated["signature"]),
                message,
                padding.PKCS1v15(),
                hashes.SHA256(),
            )
        except (InvalidSignature, ValueError) as exc:
            raise PTGWaveReceiptAuthorityError(
                "stored receipt signature is invalid"
            ) from exc
        return validated

    def sign_receipt(
        self,
        *,
        schema: str,
        key_id: object,
        issued_at: dt.datetime | str,
        receipt_payload: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Sign one exact canonical receipt envelope."""
        if schema not in {
            LINKAGE_RECEIPT_SCHEMA,
            ABANDONMENT_RECEIPT_SCHEMA,
            ORDINARY_TERMINAL_RECEIPT_SCHEMA,
        }:
            raise PTGWaveReceiptAuthorityError(
                "receipt schema is unsupported"
            )
        if not isinstance(receipt_payload, Mapping):
            raise PTGWaveReceiptAuthorityError("receipt payload must be an object")
        epoch = self.require_signing_epoch(key_id)
        timestamp = canonical_receipt_timestamp(issued_at)
        message = signed_receipt_message(
            schema=schema,
            key_id=epoch.key_id,
            issued_at=timestamp,
            payload=receipt_payload,
        )
        signature = epoch.private_key.sign(
            message,
            padding.PKCS1v15(),
            hashes.SHA256(),
        ).hex()
        if _HEX_512.fullmatch(signature) is None:
            raise PTGWaveReceiptAuthorityError(
                "receipt signature shape is invalid"
            )
        return {
            "schema": schema,
            "key_id": epoch.key_id,
            "issued_at": timestamp,
            "payload": dict(receipt_payload),
            "payload_digest": sha256_digest(message),
            "signature": signature,
        }


def load_configured_receipt_keyring() -> PTGWaveReceiptKeyring | None:
    """Load one process-pinned authority, or preserve legacy-only startup.

    Kubernetes projected Secret files may change while a pod is running but
    environment variables do not. Loading once during server startup prevents
    new private material from ever being paired with an old in-process key ID.
    Any partial configuration is an error; only complete absence keeps the
    legacy-only server available.
    """

    if not any(name in os.environ for name in _CONFIGURATION_ENV_NAMES):
        return None
    return PTGWaveReceiptKeyring.from_environment()


def canonical_receipt_timestamp(value: dt.datetime | str) -> str:
    """Return exact UTC RFC3339 text with six fractional digits."""

    if isinstance(value, str):
        if _ISSUED_AT.fullmatch(value) is None:
            raise PTGWaveReceiptAuthorityError("receipt issued_at is invalid")
        try:
            parsed = dt.datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ")
        except ValueError as exc:
            raise PTGWaveReceiptAuthorityError(
                "receipt issued_at is invalid"
            ) from exc
        if parsed.strftime("%Y-%m-%dT%H:%M:%S.%fZ") != value:
            raise PTGWaveReceiptAuthorityError("receipt issued_at is invalid")
        return value
    if not isinstance(value, dt.datetime):
        raise PTGWaveReceiptAuthorityError("receipt issued_at is invalid")
    if value.tzinfo is None:
        value = value.replace(tzinfo=dt.UTC)
    normalized = value.astimezone(dt.UTC)
    return normalized.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def signed_receipt_message(
    *,
    schema: str,
    key_id: str,
    issued_at: str,
    payload: Mapping[str, Any],
) -> bytes:
    """Return the exact domain-separated bytes covered by a receipt."""

    if schema not in {
        LINKAGE_RECEIPT_SCHEMA,
        ABANDONMENT_RECEIPT_SCHEMA,
        ORDINARY_TERMINAL_RECEIPT_SCHEMA,
    }:
        raise PTGWaveReceiptAuthorityError("receipt schema is unsupported")
    normalized_key_id = _key_id(key_id, "receipt key ID")
    timestamp = canonical_receipt_timestamp(issued_at)
    message_by_field = {
        "key_id": normalized_key_id,
        "issued_at": timestamp,
        "payload": dict(payload),
    }
    return schema.encode("ascii") + b"\0" + canonical_json(message_by_field)


def validate_receipt_envelope(receipt: object, *, schema: str) -> dict[str, Any]:
    """Validate canonical envelope shape and self-consistent digest."""

    if not isinstance(receipt, Mapping) or set(receipt) != _ENVELOPE_FIELDS:
        raise PTGWaveReceiptAuthorityError("receipt envelope fields are invalid")
    if receipt.get("schema") != schema:
        raise PTGWaveReceiptAuthorityError("receipt schema is unsupported")
    key_id = _key_id(receipt.get("key_id"), "receipt key ID")
    issued_at = canonical_receipt_timestamp(receipt.get("issued_at"))
    receipt_payload = receipt.get("payload")
    if not isinstance(receipt_payload, Mapping):
        raise PTGWaveReceiptAuthorityError("receipt payload must be an object")
    payload_digest = receipt.get("payload_digest")
    signature = receipt.get("signature")
    if (
        not isinstance(payload_digest, str)
        or not re.fullmatch(r"[0-9a-f]{64}", payload_digest)
        or not isinstance(signature, str)
        or _HEX_512.fullmatch(signature) is None
    ):
        raise PTGWaveReceiptAuthorityError("receipt digest or signature is invalid")
    message = signed_receipt_message(
        schema=schema,
        key_id=key_id,
        issued_at=issued_at,
        payload=receipt_payload,
    )
    if payload_digest != sha256_digest(message):
        raise PTGWaveReceiptAuthorityError("receipt payload digest is invalid")
    return dict(receipt)


def require_receipt_key_id(value: object, name: str = "receipt key ID") -> str:
    """Validate one bounded receipt-key epoch identity."""

    return _key_id(value, name)


def require_receipt_public_material(
    modulus: object,
    exponent: object,
) -> tuple[str, int]:
    """Validate one exact RSA-2048 public verification key."""

    if (
        not isinstance(modulus, str)
        or _HEX_512.fullmatch(modulus) is None
        or modulus[0] not in "89abcdef"
        or modulus[-1] not in "13579bdf"
        or type(exponent) is not int
        or exponent != 65537
    ):
        raise PTGWaveReceiptAuthorityError(
            "receipt public key material is invalid"
        )
    return modulus, exponent


def require_nonterminal_signing_key_coverage(
    key_ids: object,
    *,
    keyring: PTGWaveReceiptKeyring,
) -> None:
    """Refuse removal of a private epoch still needed by an open wave.

    Historical public epochs are sufficient to audit completed receipts, but
    an unquarantined nonterminal v6 wave may still need either its linkage or
    abandonment receipt.  Its pinned epoch therefore has to remain in the
    signing keyring, even after another epoch becomes active.
    """

    if not isinstance(key_ids, (list, tuple, set, frozenset)):
        raise PTGWaveReceiptAuthorityError(
            "nonterminal receipt key coverage is invalid"
        )
    normalized_key_ids = {
        _key_id(key_id, "nonterminal receipt key ID")
        for key_id in key_ids
    }
    missing = sorted(
        key_id
        for key_id in normalized_key_ids
        if key_id not in keyring.signing_by_key_id
    )
    if missing:
        raise PTGWaveReceiptAuthorityError(
            "nonterminal V12 wave receipt key epoch is unavailable for signing: "
            + ", ".join(missing)
        )


def require_persisted_receipt_key_coverage(
    pinned_rows: object,
    nonterminal_key_ids: object,
    *,
    keyring: PTGWaveReceiptKeyring,
) -> None:
    """Require public history for every pin and private keys for open pins."""

    if not isinstance(pinned_rows, (list, tuple, set, frozenset)):
        raise PTGWaveReceiptAuthorityError(
            "persisted receipt key coverage is invalid"
        )
    for pinned_row in pinned_rows:
        if not isinstance(pinned_row, (list, tuple)) or len(pinned_row) != 3:
            raise PTGWaveReceiptAuthorityError(
                "persisted receipt key coverage is invalid"
            )
        key_id, modulus, exponent = pinned_row
        normalized_key_id = _key_id(key_id, "stored receipt key ID")
        normalized_modulus, normalized_exponent = (
            require_receipt_public_material(modulus, exponent)
        )
        public_epoch = keyring.public_by_key_id.get(normalized_key_id)
        if public_epoch is None:
            raise PTGWaveReceiptAuthorityError(
                "persisted V12 receipt public key epoch is unavailable: "
                + normalized_key_id
            )
        if (
            public_epoch.rsa_modulus != normalized_modulus
            or public_epoch.rsa_exponent != normalized_exponent
        ):
            raise PTGWaveReceiptAuthorityError(
                "persisted V12 receipt public key material conflicts with "
                "the configured epoch"
            )
    require_nonterminal_signing_key_coverage(
        nonterminal_key_ids,
        keyring=keyring,
    )


def _key_id(value: object, name: str) -> str:
    if not isinstance(value, str) or _KEY_ID.fullmatch(value) is None:
        raise PTGWaveReceiptAuthorityError(f"{name} is invalid")
    return value


__all__ = [
    "ABANDONMENT_RECEIPT_SCHEMA",
    "KEY_EPOCHS_SCHEMA",
    "LINKAGE_RECEIPT_SCHEMA",
    "PTGWaveReceiptAuthorityError",
    "PTGWaveReceiptKeyring",
    "canonical_receipt_timestamp",
    "load_configured_receipt_keyring",
    "require_nonterminal_signing_key_coverage",
    "require_persisted_receipt_key_coverage",
    "require_receipt_key_id",
    "require_receipt_public_material",
    "signed_receipt_message",
    "validate_receipt_envelope",
]
