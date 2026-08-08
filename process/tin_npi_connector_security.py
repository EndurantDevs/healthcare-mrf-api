# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Secret handling and policy-scoped TIN token contracts."""

from __future__ import annotations

import hashlib
import hmac
import os
import stat
import struct
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from process.tin_npi_connector_support import (
    _FHIR_EIN_INPUT_PATTERN,
    _HASH_HEX_PATTERN,
    _NORMALIZED_EIN_PATTERN,
    _POLICY_BINDING_DOMAIN,
    _SOURCE_RECORD_HMAC_DOMAIN,
    _TIN_TOKEN_POLICY_PATTERN,
    TIN_TOKEN_FULL_HMAC_CONTRACT_ID,
    TIN_TOKEN_HMAC_CONTRACT_ID,
    TIN_TOKEN_ID_128_CONTRACT_ID,
    TIN_TOKEN_MESSAGE_DOMAIN,
    TIN_TOKEN_MESSAGE_FORMAT_ID,
    TIN_TOKEN_NORMALIZATION_CONTRACT_ID,
    TIN_TOKEN_POLICY_DESCRIPTOR_DOMAIN,
    TIN_TOKEN_POLICY_ID_MAX_BYTES,
    TinNpiConnectorError,
)


class _InertSecretSnapshot:
    """A redacted copy/pickle result that cannot tokenize identifiers."""

    __slots__ = ()

    def __repr__(self) -> str:
        return "<redacted-tin-token-policy>"

    __str__ = __repr__


class _ProtectedSecret:
    """Keep secret bytes immutable and redacted from common serialization."""

    __slots__ = ("__value",)

    def __init__(self, value: bytes) -> None:
        object.__setattr__(self, "_ProtectedSecret__value", value)

    def __setattr__(self, name: str, value: object) -> None:
        raise TypeError("TIN token secret is immutable")

    def __repr__(self) -> str:
        return "<redacted>"

    __str__ = __repr__

    def __copy__(self) -> _InertSecretSnapshot:
        return _InertSecretSnapshot()

    def __deepcopy__(self, memo: object) -> _InertSecretSnapshot:
        return _InertSecretSnapshot()

    def __reduce__(self) -> tuple[type[_InertSecretSnapshot], tuple[()]]:
        return _InertSecretSnapshot, ()


def _read_protected_secret(candidate: object) -> bytes:
    if type(candidate) is not _ProtectedSecret:
        raise TinNpiConnectorError("TIN token policy secret is invalid")
    value = object.__getattribute__(candidate, "_ProtectedSecret__value")
    if type(value) is not bytes or len(value) != 32:
        raise TinNpiConnectorError("TIN token policy secret is invalid")
    return value


def canonical_token_policy_id(candidate: object) -> str:
    """Validate the frozen Release-1 PTG token-policy identifier."""

    if type(candidate) is not str:
        raise TinNpiConnectorError("TIN token policy ID is invalid")
    try:
        encoded = candidate.encode("ascii")
    except UnicodeEncodeError:
        raise TinNpiConnectorError("TIN token policy ID is invalid") from None
    if (
        len(encoded) > TIN_TOKEN_POLICY_ID_MAX_BYTES
        or _TIN_TOKEN_POLICY_PATTERN.fullmatch(candidate) is None
    ):
        raise TinNpiConnectorError("TIN token policy ID is invalid")
    return candidate


def normalize_ein(candidate: object) -> str:
    """Normalize only reviewed EIN display forms to nine ASCII digits."""

    if type(candidate) is not str:
        raise TinNpiConnectorError("EIN is malformed")
    reviewed_input = candidate.strip(" \t\r\n")
    if _FHIR_EIN_INPUT_PATTERN.fullmatch(reviewed_input) is None:
        raise TinNpiConnectorError("EIN is malformed")
    normalized = reviewed_input.replace("-", "")
    if _NORMALIZED_EIN_PATTERN.fullmatch(normalized) is None:
        raise TinNpiConnectorError("EIN is malformed")
    return normalized


def _tin_hmac_message(*, tin_type: str, normalized_tin: str) -> bytes:
    try:
        tin_type_bytes = tin_type.encode("ascii")
        normalized_tin_bytes = normalized_tin.encode("ascii")
    except UnicodeEncodeError:
        raise TinNpiConnectorError("TIN token input is invalid") from None
    if len(tin_type_bytes) > 0xFFFF or len(normalized_tin_bytes) > 0xFFFF:
        raise TinNpiConnectorError("TIN token input is invalid")
    return b"".join(
        (
            TIN_TOKEN_MESSAGE_DOMAIN,
            b"\0",
            struct.pack(">H", len(tin_type_bytes)),
            tin_type_bytes,
            struct.pack(">H", len(normalized_tin_bytes)),
            normalized_tin_bytes,
        )
    )


def token_policy_descriptor_sha256(token_policy_id: object) -> str:
    """Hash the frozen five-field PTG V4 token-policy descriptor."""

    canonical_policy_id = canonical_token_policy_id(token_policy_id)
    fields = (
        canonical_policy_id,
        TIN_TOKEN_NORMALIZATION_CONTRACT_ID,
        TIN_TOKEN_HMAC_CONTRACT_ID,
        TIN_TOKEN_ID_128_CONTRACT_ID,
        TIN_TOKEN_FULL_HMAC_CONTRACT_ID,
    )
    framed_fields: list[bytes] = []
    for field in fields:
        try:
            encoded = field.encode("ascii")
        except UnicodeEncodeError:
            raise TinNpiConnectorError(
                "TIN token policy descriptor is invalid"
            ) from None
        framed_fields.append(struct.pack(">I", len(encoded)) + encoded)
    return hashlib.sha256(
        TIN_TOKEN_POLICY_DESCRIPTOR_DOMAIN + b"".join(framed_fields)
    ).hexdigest()


@dataclass(frozen=True)
class TinTokenPolicyDescriptor:
    """Manifest-provided PTG policy identity verified against Release 1."""

    token_policy_id: str
    token_policy_descriptor_sha256: str

    def __post_init__(self) -> None:
        canonical_policy_id = canonical_token_policy_id(self.token_policy_id)
        candidate_digest = self.token_policy_descriptor_sha256
        if (
            type(candidate_digest) is not str
            or _HASH_HEX_PATTERN.fullmatch(candidate_digest) is None
            or not hmac.compare_digest(
                candidate_digest,
                token_policy_descriptor_sha256(canonical_policy_id),
            )
        ):
            raise TinNpiConnectorError("TIN token policy descriptor is invalid")

    @classmethod
    def release_1(cls, token_policy_id: str) -> "TinTokenPolicyDescriptor":
        """Construct the canonical Release-1 descriptor for one policy ID."""

        return cls(
            token_policy_id=token_policy_id,
            token_policy_descriptor_sha256=token_policy_descriptor_sha256(
                token_policy_id
            ),
        )

    def public_payload(self) -> dict[str, str]:
        """Return the manifest-safe policy ID and verified descriptor digest."""

        return {
            "token_policy_descriptor_sha256": (self.token_policy_descriptor_sha256),
            "token_policy_id": self.token_policy_id,
        }


def _source_record_hmac_message(
    *,
    token_policy_id: str,
    source_id: str,
    source_endpoint_id: str,
    source_dataset_id: str,
    resource_id: str,
) -> bytes:
    """Encode a policy- and dataset-scoped source-record identity."""

    canonical_policy_id = canonical_token_policy_id(token_policy_id)
    identity_fields = (
        (canonical_policy_id, 55),
        (source_id, 64),
        (source_endpoint_id, 64),
        (source_dataset_id, 128),
        (resource_id, 256),
    )
    encoded_fields: list[bytes] = []
    for identity_text, character_limit in identity_fields:
        if (
            type(identity_text) is not str
            or identity_text != identity_text.strip()
            or not 1 <= len(identity_text) <= character_limit
            or not identity_text.isprintable()
        ):
            raise TinNpiConnectorError("FHIR source-record identity input is invalid")
        encoded = identity_text.encode("utf-8")
        if len(encoded) > 0xFFFF:
            raise TinNpiConnectorError("FHIR source-record identity input is invalid")
        encoded_fields.append(encoded)
    framed_values = b"".join(
        struct.pack(">H", len(encoded_field)) + encoded_field
        for encoded_field in encoded_fields
    )
    return _SOURCE_RECORD_HMAC_DOMAIN + b"\0" + framed_values


@dataclass(frozen=True, repr=False)
class TinTaxIdentityToken:
    """Policy-scoped TIN token; the full HMAC is authoritative."""

    token_policy_id: str
    tin_id_128: bytes
    tin_hmac_sha256: bytes

    def __post_init__(self) -> None:
        canonical_token_policy_id(self.token_policy_id)
        if (
            type(self.tin_id_128) is not bytes
            or len(self.tin_id_128) != 16
            or type(self.tin_hmac_sha256) is not bytes
            or len(self.tin_hmac_sha256) != 32
            or not hmac.compare_digest(
                self.tin_id_128,
                self.tin_hmac_sha256[:16],
            )
        ):
            raise TinNpiConnectorError("TIN identity token is invalid")

    def has_matching_full_hmac(self, candidate: object) -> bool:
        """Verify a candidate with a constant-time full-digest comparison."""

        return type(candidate) is bytes and hmac.compare_digest(
            self.tin_hmac_sha256,
            candidate,
        )

    matches_full_hmac = has_matching_full_hmac

    def __repr__(self) -> str:
        return (
            "<tin-tax-identity-token "
            f"token_policy_id={self.token_policy_id!r} digest=<redacted>>"
        )


class TinTokenProjector(Protocol):
    """Minimal protected capability used by FHIR evidence extraction."""

    @property
    def token_policy_id(self) -> str:
        """Return the canonical policy ID bound to this capability."""

        raise NotImplementedError

    def tokenize_ein(self, candidate: object) -> TinTaxIdentityToken:
        """Normalize and tokenize one EIN without retaining its raw value."""

        raise NotImplementedError

    def pseudonymize_source_record(
        self,
        *,
        source_id: str,
        source_endpoint_id: str,
        source_dataset_id: str,
        resource_id: str,
    ) -> bytes:
        """Return a policy-scoped pseudonym for one exact source record."""

        raise NotImplementedError


class _TinHmacTokenPolicy:
    """Opaque policy capability bound to one exact 32-byte secret."""

    __slots__ = ("_binding", "_secret", "_token_policy_id")
    _binding: _ProtectedSecret
    _secret: _ProtectedSecret
    _token_policy_id: str

    def __init__(self, *, token_policy_id: str, secret: bytes) -> None:
        canonical_policy_id = canonical_token_policy_id(token_policy_id)
        if type(secret) is not bytes or len(secret) != 32:
            raise TinNpiConnectorError("TIN token policy secret is invalid")
        object.__setattr__(self, "_token_policy_id", canonical_policy_id)
        object.__setattr__(self, "_secret", _ProtectedSecret(secret))
        object.__setattr__(
            self,
            "_binding",
            _ProtectedSecret(
                hmac.new(
                    secret,
                    _POLICY_BINDING_DOMAIN + canonical_policy_id.encode("ascii"),
                    hashlib.sha256,
                ).digest()
            ),
        )
        self._validated_material()

    def __setattr__(self, name: str, value: object) -> None:
        raise TypeError("TIN token policy is immutable")

    @property
    def token_policy_id(self) -> str:
        """Return the policy ID after revalidating protected key material."""

        policy_id, _ = self._validated_material()
        return policy_id

    def _validated_material(self) -> tuple[str, bytes]:
        try:
            policy_id = canonical_token_policy_id(self._token_policy_id)
            secret = _read_protected_secret(self._secret)
            binding = _read_protected_secret(self._binding)
        except (AttributeError, TinNpiConnectorError):
            raise TinNpiConnectorError("TIN token policy state is invalid") from None
        expected_binding = hmac.new(
            secret,
            _POLICY_BINDING_DOMAIN + policy_id.encode("ascii"),
            hashlib.sha256,
        ).digest()
        if not hmac.compare_digest(binding, expected_binding):
            raise TinNpiConnectorError("TIN token policy state is invalid")
        return policy_id, secret

    def tokenize_ein(self, candidate: object) -> TinTaxIdentityToken:
        """Normalize and HMAC one EIN under this exact policy generation."""

        policy_id, secret = self._validated_material()
        normalized_ein = normalize_ein(candidate)
        digest = hmac.new(
            secret,
            _tin_hmac_message(tin_type="ein", normalized_tin=normalized_ein),
            hashlib.sha256,
        ).digest()
        return TinTaxIdentityToken(
            token_policy_id=policy_id,
            tin_id_128=digest[:16],
            tin_hmac_sha256=digest,
        )

    def pseudonymize_source_record(
        self,
        *,
        source_id: str,
        source_endpoint_id: str,
        source_dataset_id: str,
        resource_id: str,
    ) -> bytes:
        """Return a protected, non-public identity for one source record."""

        policy_id, secret = self._validated_material()
        return hmac.new(
            secret,
            _source_record_hmac_message(
                token_policy_id=policy_id,
                source_id=source_id,
                source_endpoint_id=source_endpoint_id,
                source_dataset_id=source_dataset_id,
                resource_id=resource_id,
            ),
            hashlib.sha256,
        ).digest()

    def public_descriptor(self) -> dict[str, str]:
        """Return manifest-safe policy information without protected material."""

        descriptor = TinTokenPolicyDescriptor.release_1(self.token_policy_id)
        return {
            "message_format_id": TIN_TOKEN_MESSAGE_FORMAT_ID,
            **descriptor.public_payload(),
        }

    def __repr__(self) -> str:
        return f"<tin-token-policy token_policy_id={self.token_policy_id!r}>"

    def __copy__(self) -> _InertSecretSnapshot:
        return _InertSecretSnapshot()

    def __deepcopy__(self, memo: object) -> _InertSecretSnapshot:
        return _InertSecretSnapshot()

    def __reduce__(self) -> tuple[type[_InertSecretSnapshot], tuple[()]]:
        return _InertSecretSnapshot, ()


def validate_tin_hmac_token_policy(candidate: object) -> TinTokenProjector:
    """Return one connector-owned policy capability or fail closed."""

    if type(candidate) is not _TinHmacTokenPolicy:
        raise TinNpiConnectorError("TIN token policy capability is invalid")
    candidate._validated_material()
    return candidate


def load_tin_token_policy(
    *,
    token_policy_id: str,
    secret_file: str | os.PathLike[str],
) -> TinTokenProjector:
    """Load exactly 32 raw secret bytes from a mounted file."""

    canonical_policy_id = canonical_token_policy_id(token_policy_id)
    try:
        secret_path = Path(secret_file)
        descriptor = os.open(
            secret_path,
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NONBLOCK", 0),
        )
        with os.fdopen(descriptor, "rb") as secret_stream:
            metadata = os.fstat(secret_stream.fileno())
            if (
                not stat.S_ISREG(metadata.st_mode)
                or stat.S_IMODE(metadata.st_mode) != 0o400
                or metadata.st_uid != os.geteuid()
            ):
                raise TinNpiConnectorError("TIN token secret file is invalid")
            secret = secret_stream.read(33)
    except TinNpiConnectorError:
        raise
    except (OSError, TypeError, ValueError):
        raise TinNpiConnectorError("TIN token secret file is unavailable") from None
    if len(secret) != 32:
        raise TinNpiConnectorError("TIN token secret file is invalid")
    return _TinHmacTokenPolicy(
        token_policy_id=canonical_policy_id,
        secret=secret,
    )
