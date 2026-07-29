# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Protected TIN identity and exact Provider Directory evidence primitives.

The PTG sidecar and this connector share only policy-scoped HMAC tokens.  Raw
TINs are accepted transiently for normalization and are never retained in a
returned object, manifest payload, or exception message.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import hmac
import json
import os
import re
import stat
import struct
from collections.abc import Iterable
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Mapping, Protocol, Sequence


TIN_TOKEN_MESSAGE_DOMAIN = b"healthporta.ptg.tin.v1"
TIN_TOKEN_MESSAGE_FORMAT_ID = "healthporta.ptg.tin-hmac-message.v1"
TIN_TOKEN_POLICY_PREFIX = "ptg-tin-hmac-sha256-v1:"
TIN_TOKEN_POLICY_ID_MAX_BYTES = 55
TIN_TOKEN_POLICY_DESCRIPTOR_DOMAIN = b"PTG2V4TINPOLICY\x01"
TIN_TOKEN_NORMALIZATION_CONTRACT_ID = "ein_ascii_digits_or_2_7_hyphen_v1"
TIN_TOKEN_HMAC_CONTRACT_ID = "hmac_sha256_ptg_tin_v1"
TIN_TOKEN_ID_128_CONTRACT_ID = "tin_id_128=first_16_bytes(tin_hmac_sha256)"
TIN_TOKEN_FULL_HMAC_CONTRACT_ID = "tin_hmac_sha256_full_32_bytes_authoritative"
FHIR_SOURCE_RECORD_HMAC_MESSAGE_FORMAT_ID = (
    "healthporta.tin-npi.fhir-source-record-hmac.v1"
)
FHIR_TIN_NPI_IDENTIFIER_POLICY_ID = (
    "healthporta.provider-directory.tin-npi-identifiers.v1"
)
FHIR_SAME_ORGANIZATION_RELATIONSHIP = "same_organization_identifier"

_TIN_TOKEN_POLICY_PATTERN = re.compile(
    r"^ptg-tin-hmac-sha256-v1:[a-z0-9](?:[a-z0-9._-]{0,31})$"
)
_NORMALIZED_EIN_PATTERN = re.compile(r"^[0-9]{9}$")
_FHIR_EIN_INPUT_PATTERN = re.compile(r"^(?:[0-9]{9}|[0-9]{2}-[0-9]{7})$")
_NORMALIZED_NPI_PATTERN = re.compile(r"^[0-9]{10}$")
_PUBLIC_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/-]*$")
_FHIR_IDENTIFIER_CODING_SYSTEMS = (
    "http://terminology.hl7.org/CodeSystem/v2-0203",
    "http://hl7.org/fhir/v2/0203",
)
_FHIR_NPI_SYSTEM = "http://hl7.org/fhir/sid/us-npi"
_ALLOWED_NPI_SEPARATORS = frozenset(" -./")
_EVIDENCE_HASH_DOMAIN = b"healthporta.tin-npi.fhir-evidence.v2\0"
_EVIDENCE_SET_HASH_DOMAIN = b"healthporta.tin-npi.fhir-evidence-set.v1\0"
_SOURCE_RECORD_HMAC_DOMAIN = b"healthporta.tin-npi.fhir-source-record.v1"
_POLICY_BINDING_DOMAIN = b"healthporta.tin-npi.policy-binding.v1\0"
_NPI_MIN = 1_000_000_000
_NPI_MAX = 2_999_999_999
_NPI_LUHN_PREFIX_DIGIT_SUM = 24
_HASH_HEX_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_SOURCE_VECTOR_HASH_DOMAIN = b"healthporta.tin-npi.source-vector.v1\0"
_SOURCE_ORDINAL_MAP_HASH_DOMAIN = b"healthporta.tin-npi.source-ordinal-map.v1\0"
_LOOKUP_ROW_HASH_DOMAIN = b"healthporta.tin-npi.lookup-row.v3\0"
_LOOKUP_BUCKET_HASH_DOMAIN = b"healthporta.tin-npi.lookup-bucket.v1\0"
_LOOKUP_SET_HASH_DOMAIN = b"healthporta.tin-npi.lookup-set.v4\0"
_SCAN_PROOF_HASH_DOMAIN = b"healthporta.tin-npi.fhir-organization-scan-proof.v2\0"
_GENERATION_HASH_DOMAIN = b"healthporta.tin-npi.generation.v3\0"
_IDENTIFIER_RULE_HASH_DOMAIN = b"healthporta.tin-npi.fhir-identifier-rule.v1\0"
_IDENTIFIER_POLICY_HASH_DOMAIN = b"healthporta.tin-npi.fhir-identifier-policy.v2\0"
_FHIR_ORGANIZATION_RECORD_BINDING_HASH_DOMAIN = (
    b"healthporta.tin-npi.fhir-organization-record-binding.v1\0"
)
_FHIR_DATE_PATTERN = re.compile(
    r"^(?P<year>[0-9]{4})(?:-(?P<month>[0-9]{2})(?:-(?P<day>[0-9]{2}))?)?$"
)
_FHIR_DATETIME_PATTERN = re.compile(
    r"^(?P<year>[0-9]{4})-"
    r"(?P<month>0[1-9]|1[0-2])-"
    r"(?P<day>0[1-9]|[1-2][0-9]|3[0-1])T"
    r"(?P<hour>[01][0-9]|2[0-3]):"
    r"(?P<minute>[0-5][0-9]):"
    r"(?P<second>[0-5][0-9]|60)"
    r"(?:\.(?P<fraction>[0-9]+))?"
    r"(?P<zone>Z|[+-](?:(?:0[0-9]|1[0-3]):[0-5][0-9]|14:00))$"
)

TIN_NPI_SOURCE_VECTOR_SCHEMA_VERSION = 3
TIN_NPI_LOOKUP_SCHEMA_VERSION = 2
TIN_NPI_LOOKUP_CONTRACT_ID = "healthporta.tin-npi.compact-lookup.v2"
TIN_NPI_PROJECTION_POLICY_ID = "healthporta.tin-npi.compact-same-organization-lookup.v3"
TIN_NPI_SOURCE_ORDINAL_CONTRACT_ID = "source_id_sorted_utf8_lsb0_bitmap_v1"
TIN_NPI_SOURCE_SCOPE_CONTRACT_ID = (
    "healthporta.tin-npi.all-current-published-organization-sources.v1"
)
TIN_NPI_TOKEN_POLICY_SCOPE_CONTRACT_ID = (
    "healthporta.tin-npi.all-retained-ptg-tax-policy-descriptors.v1"
)
TIN_NPI_SITE_RESOLUTION_CONTRACT_ID = (
    "healthporta.tin-npi.site-by-current-entity-address-unified.v1"
)
TIN_NPI_FHIR_ORGANIZATION_SCAN_CONTRACT_ID = (
    "healthporta.tin-npi.fhir-organization-scan.v2"
)
TIN_NPI_FHIR_ORGANIZATION_IDENTITY_CONTRACT_ID = (
    "provider_directory_dataset_resource_type_id_payload_hash_newline_v1"
)
TIN_NPI_FHIR_INPUT_RELATION = "provider_directory_dataset_resource"


class TinNpiConnectorError(ValueError):
    """Report a fail-closed connector identity or evidence error."""


class _MalformedFhirIdentifierPeriod(TinNpiConnectorError):
    pass


class _UnresolvedFhirIdentifierPeriod(TinNpiConnectorError):
    pass


class FhirOrganizationEvidenceState(str, Enum):
    """Non-sensitive outcome of inspecting one FHIR Organization."""

    MATCHED = "matched"
    NOT_ORGANIZATION = "not_organization"
    INACTIVE = "inactive"
    MISSING_IDENTIFIERS = "missing_identifiers"
    MISSING_NPI = "missing_npi"
    MISSING_EIN = "missing_ein"
    MALFORMED_NPI = "malformed_npi"
    MALFORMED_EIN = "malformed_ein"
    AMBIGUOUS_EIN = "ambiguous_ein"
    CONFLICTING_IDENTIFIER_CLASS = "conflicting_identifier_class"
    MALFORMED_IDENTIFIER_PERIOD = "malformed_identifier_period"
    UNRESOLVED_IDENTIFIER_PERIOD = "unresolved_identifier_period"


FHIR_ORGANIZATION_SCAN_TERMINAL_STATES = tuple(
    state
    for state in FhirOrganizationEvidenceState
    if state is not FhirOrganizationEvidenceState.NOT_ORGANIZATION
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
        return cls(
            token_policy_id=token_policy_id,
            token_policy_descriptor_sha256=token_policy_descriptor_sha256(
                token_policy_id
            ),
        )

    def public_payload(self) -> dict[str, str]:
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
    values = (
        (canonical_policy_id, 55),
        (source_id, 64),
        (source_endpoint_id, 64),
        (source_dataset_id, 128),
        (resource_id, 256),
    )
    encoded_values: list[bytes] = []
    for value, character_limit in values:
        if (
            type(value) is not str
            or value != value.strip()
            or not 1 <= len(value) <= character_limit
            or not value.isprintable()
        ):
            raise TinNpiConnectorError("FHIR source-record identity input is invalid")
        encoded = value.encode("utf-8")
        if len(encoded) > 0xFFFF:
            raise TinNpiConnectorError("FHIR source-record identity input is invalid")
        encoded_values.append(encoded)
    framed_values = b"".join(
        struct.pack(">H", len(value)) + value for value in encoded_values
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

    def matches_full_hmac(self, candidate: object) -> bool:
        """Verify a candidate with a constant-time full-digest comparison."""

        return type(candidate) is bytes and hmac.compare_digest(
            self.tin_hmac_sha256,
            candidate,
        )

    def __repr__(self) -> str:
        return (
            "<tin-tax-identity-token "
            f"token_policy_id={self.token_policy_id!r} digest=<redacted>>"
        )


class TinTokenProjector(Protocol):
    """Minimal protected capability used by FHIR evidence extraction."""

    @property
    def token_policy_id(self) -> str: ...

    def tokenize_ein(self, candidate: object) -> TinTaxIdentityToken: ...

    def pseudonymize_source_record(
        self,
        *,
        source_id: str,
        source_endpoint_id: str,
        source_dataset_id: str,
        resource_id: str,
    ) -> bytes: ...


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


def _canonical_identifier_selector_values(
    values: object,
    *,
    field_name: str,
) -> tuple[str, ...]:
    if (
        type(values) is not tuple
        or values != tuple(sorted(set(values)))
        or any(
            type(value) is not str
            or not value
            or len(value) > 256
            or not value.isascii()
            or any(
                character.isspace() or character in {'"', "\\"} for character in value
            )
            for value in values
        )
    ):
        raise TinNpiConnectorError(f"FHIR identifier {field_name} are invalid")
    return values


def _canonical_identifier_selector_codings(
    values: object,
    *,
    field_name: str,
) -> tuple[tuple[str, str], ...]:
    if (
        type(values) is not tuple
        or values != tuple(sorted(set(values)))
        or any(
            type(coding) is not tuple
            or len(coding) != 2
            or any(
                type(part) is not str
                or not part
                or len(part) > 256
                or not part.isascii()
                or any(
                    character.isspace() or character in {'"', "\\"}
                    for character in part
                )
                for part in coding
            )
            for coding in values
        )
    ):
        raise TinNpiConnectorError(f"FHIR identifier {field_name} are invalid")
    return values


def _canonical_identifier_scope_id(
    candidate: object,
    *,
    field_name: str,
    limit: int,
) -> str:
    if (
        type(candidate) is not str
        or not 1 <= len(candidate) <= limit
        or _PUBLIC_ID_PATTERN.fullmatch(candidate) is None
    ):
        raise TinNpiConnectorError(f"FHIR identifier {field_name} is invalid")
    return candidate


@dataclass(frozen=True)
class FhirTinNpiIdentifierRule:
    """Exact identifier selectors reviewed for one source and endpoint."""

    rule_id: str
    source_id: str
    endpoint_id: str
    npi_systems: tuple[str, ...]
    npi_type_codings: tuple[tuple[str, str], ...]
    ein_systems: tuple[str, ...]
    ein_type_codings: tuple[tuple[str, str], ...]
    excluded_identifier_uses: tuple[str, ...] = ("old",)
    period_policy_id: str = "fhir-r4-inclusive-period-at-observed-at-v1"

    def __post_init__(self) -> None:
        _canonical_identifier_scope_id(
            self.rule_id,
            field_name="rule ID",
            limit=128,
        )
        _canonical_identifier_scope_id(
            self.source_id,
            field_name="source ID",
            limit=64,
        )
        _canonical_identifier_scope_id(
            self.endpoint_id,
            field_name="endpoint ID",
            limit=64,
        )
        _canonical_identifier_selector_values(
            self.npi_systems,
            field_name="NPI systems",
        )
        _canonical_identifier_selector_values(
            self.ein_systems,
            field_name="EIN systems",
        )
        _canonical_identifier_selector_codings(
            self.npi_type_codings,
            field_name="NPI type codings",
        )
        _canonical_identifier_selector_codings(
            self.ein_type_codings,
            field_name="EIN type codings",
        )
        if (
            not (self.npi_systems or self.npi_type_codings)
            or not (self.ein_systems or self.ein_type_codings)
            or set(self.npi_systems).intersection(self.ein_systems)
            or set(self.npi_type_codings).intersection(self.ein_type_codings)
        ):
            raise TinNpiConnectorError("FHIR identifier rule selectors are invalid")
        if (
            type(self.excluded_identifier_uses) is not tuple
            or self.excluded_identifier_uses
            != tuple(sorted(set(self.excluded_identifier_uses)))
            or any(
                type(use) is not str
                or not use
                or len(use) > 32
                or not use.isascii()
                or any(
                    character.isspace() or character in {'"', "\\"} for character in use
                )
                for use in self.excluded_identifier_uses
            )
        ):
            raise TinNpiConnectorError("FHIR identifier activity policy is invalid")
        _canonical_identifier_scope_id(
            self.period_policy_id,
            field_name="period policy ID",
            limit=64,
        )

    def public_payload(self) -> dict[str, Any]:
        return {
            "endpoint_id": self.endpoint_id,
            "ein_systems": list(self.ein_systems),
            "ein_type_codings": [list(coding) for coding in self.ein_type_codings],
            "excluded_identifier_uses": list(self.excluded_identifier_uses),
            "npi_systems": list(self.npi_systems),
            "npi_type_codings": [list(coding) for coding in self.npi_type_codings],
            "period_policy_id": self.period_policy_id,
            "rule_id": self.rule_id,
            "source_id": self.source_id,
        }

    @property
    def descriptor_canonical_json(self) -> str:
        return json.dumps(
            self.public_payload(),
            sort_keys=True,
            separators=(",", ":"),
        )

    @property
    def descriptor_sha256(self) -> str:
        return hashlib.sha256(
            _IDENTIFIER_RULE_HASH_DOMAIN
            + self.descriptor_canonical_json.encode("utf-8")
        ).hexdigest()


@dataclass(frozen=True)
class FhirTinNpiIdentifierPolicy:
    """Immutable bundle of exact source-scoped identifier rules."""

    policy_id: str
    rules: tuple[FhirTinNpiIdentifierRule, ...]

    def __post_init__(self) -> None:
        _canonical_identifier_scope_id(
            self.policy_id,
            field_name="policy ID",
            limit=128,
        )
        if (
            type(self.rules) is not tuple
            or not self.rules
            or any(type(rule) is not FhirTinNpiIdentifierRule for rule in self.rules)
        ):
            raise TinNpiConnectorError("FHIR identifier policy rules are invalid")
        expected_rules = tuple(
            sorted(
                self.rules,
                key=lambda rule: (
                    rule.source_id.encode("utf-8"),
                    rule.endpoint_id.encode("utf-8"),
                    rule.rule_id.encode("utf-8"),
                ),
            )
        )
        if self.rules != expected_rules:
            raise TinNpiConnectorError("FHIR identifier policy rules are not ordered")
        scope_keys = tuple((rule.source_id, rule.endpoint_id) for rule in self.rules)
        rule_ids = tuple(rule.rule_id for rule in self.rules)
        if len(set(scope_keys)) != len(scope_keys) or len(set(rule_ids)) != len(
            rule_ids
        ):
            raise TinNpiConnectorError("FHIR identifier policy rules are duplicated")

    def rule_for(
        self,
        *,
        source_id: str,
        endpoint_id: str,
    ) -> FhirTinNpiIdentifierRule:
        scope_key = (
            _canonical_identifier_scope_id(
                source_id,
                field_name="source ID",
                limit=64,
            ),
            _canonical_identifier_scope_id(
                endpoint_id,
                field_name="endpoint ID",
                limit=64,
            ),
        )
        matches = tuple(
            rule
            for rule in self.rules
            if (rule.source_id, rule.endpoint_id) == scope_key
        )
        if len(matches) != 1:
            raise TinNpiConnectorError(
                "FHIR identifier policy does not cover source endpoint"
            )
        return matches[0]

    def public_payload(self) -> dict[str, Any]:
        return {
            "policy_id": self.policy_id,
            "rules": [
                {
                    **rule.public_payload(),
                    "identifier_rule_sha256": rule.descriptor_sha256,
                }
                for rule in self.rules
            ],
        }

    @property
    def descriptor_canonical_json(self) -> str:
        return json.dumps(
            self.public_payload(),
            sort_keys=True,
            separators=(",", ":"),
        )

    @property
    def descriptor_sha256(self) -> str:
        return hashlib.sha256(
            _IDENTIFIER_POLICY_HASH_DOMAIN
            + self.descriptor_canonical_json.encode("utf-8")
        ).hexdigest()


DEFAULT_FHIR_TIN_NPI_IDENTIFIER_POLICY = FhirTinNpiIdentifierPolicy(
    policy_id=FHIR_TIN_NPI_IDENTIFIER_POLICY_ID,
    rules=(
        FhirTinNpiIdentifierRule(
            rule_id="healthporta.provider-directory.unreviewed-identifiers.v1",
            source_id="unreviewed-source",
            endpoint_id="unreviewed-endpoint",
            npi_systems=(_FHIR_NPI_SYSTEM,),
            npi_type_codings=tuple(
                sorted(
                    (coding_system, "NPI")
                    for coding_system in _FHIR_IDENTIFIER_CODING_SYSTEMS
                )
            ),
            ein_systems=("urn:healthporta:unreviewed-ein-never-use",),
            ein_type_codings=(),
        ),
    ),
)


def _identifier_type_codings(
    identifier: Mapping[str, Any],
) -> tuple[tuple[str, str], ...]:
    raw_type = identifier.get("type")
    raw_codings = raw_type.get("coding") if isinstance(raw_type, Mapping) else None
    if raw_codings is None:
        raw_codings = identifier.get("type_codes")
    if not isinstance(raw_codings, Sequence) or isinstance(
        raw_codings,
        (str, bytes, bytearray),
    ):
        return ()
    codings: set[tuple[str, str]] = set()
    for coding in raw_codings:
        if not isinstance(coding, Mapping):
            continue
        system = coding.get("system")
        code = coding.get("code")
        if type(system) is str and type(code) is str:
            codings.add((system, code))
    return tuple(sorted(codings))


def _identifier_matches(
    identifier: Mapping[str, Any],
    *,
    systems: tuple[str, ...],
    type_codings: tuple[tuple[str, str], ...],
) -> bool:
    system = identifier.get("system")
    return (
        type(system) is str
        and system in systems
        or bool(set(_identifier_type_codings(identifier)).intersection(type_codings))
    )


def _as_utc_datetime(candidate: object) -> dt.datetime | None:
    if candidate is None:
        return None
    if isinstance(candidate, dt.datetime):
        value = candidate
    elif isinstance(candidate, dt.date):
        value = dt.datetime.combine(candidate, dt.time.min)
    else:
        raise _MalformedFhirIdentifierPeriod(
            "FHIR identifier observation time is invalid"
        )
    if value.tzinfo is None:
        return value.replace(tzinfo=dt.timezone.utc)
    return value.astimezone(dt.timezone.utc)


def canonical_evidence_as_of(candidate: object) -> str:
    """Normalize one generation-wide evidence cutoff to canonical UTC text."""

    if type(candidate) is str:
        if not candidate.endswith("Z"):
            raise TinNpiConnectorError("evidence cutoff is invalid")
        try:
            parsed = dt.datetime.fromisoformat(candidate[:-1] + "+00:00")
        except ValueError:
            raise TinNpiConnectorError("evidence cutoff is invalid") from None
        value = parsed.astimezone(dt.timezone.utc)
    else:
        try:
            candidate_value = _as_utc_datetime(candidate)
        except _MalformedFhirIdentifierPeriod:
            raise TinNpiConnectorError("evidence cutoff is invalid") from None
        if candidate_value is None:
            raise TinNpiConnectorError("evidence cutoff is invalid")
        value = candidate_value
    canonical = value.isoformat(timespec="microseconds").replace("+00:00", "Z")
    if type(candidate) is str and candidate != canonical:
        raise TinNpiConnectorError("evidence cutoff is invalid")
    return canonical


def _partial_date_bound(
    candidate: str,
    *,
    upper: bool,
) -> tuple[dt.datetime, bool] | None:
    match = _FHIR_DATE_PATTERN.fullmatch(candidate)
    if match is None:
        return None
    year = int(match.group("year"))
    month_text = match.group("month")
    day_text = match.group("day")
    try:
        if month_text is None:
            value = dt.datetime(year, 1, 1, tzinfo=dt.timezone.utc)
            if not upper:
                return value, True
            if year == dt.MAXYEAR:
                return dt.datetime.max.replace(tzinfo=dt.timezone.utc), True
            return value.replace(year=year + 1), False
        month = int(month_text)
        if day_text is None:
            value = dt.datetime(year, month, 1, tzinfo=dt.timezone.utc)
            if not upper:
                return value, True
            if month == 12:
                if year == dt.MAXYEAR:
                    return dt.datetime.max.replace(tzinfo=dt.timezone.utc), True
                return value.replace(year=year + 1, month=1), False
            return value.replace(month=month + 1), False
        value = dt.datetime(
            year,
            month,
            int(day_text),
            tzinfo=dt.timezone.utc,
        )
        if not upper:
            return value, True
        if value.date() == dt.date.max:
            return dt.datetime.max.replace(tzinfo=dt.timezone.utc), True
        return value + dt.timedelta(days=1), False
    except (OverflowError, ValueError):
        raise _MalformedFhirIdentifierPeriod(
            "FHIR identifier period is malformed"
        ) from None


def _exact_fhir_datetime_bound(
    candidate: str,
    *,
    upper: bool,
) -> tuple[dt.datetime, bool]:
    match = _FHIR_DATETIME_PATTERN.fullmatch(candidate)
    if match is None:
        raise _MalformedFhirIdentifierPeriod("FHIR identifier period is malformed")
    fraction = match.group("fraction") or ""
    microsecond = int((fraction + "000000")[:6])
    requires_microsecond_ceiling = (
        not upper and len(fraction) > 6 and any(digit != "0" for digit in fraction[6:])
    )
    zone = match.group("zone")
    if zone == "Z":
        timezone = dt.timezone.utc
    else:
        offset_hour, offset_minute = (
            int(component) for component in zone[1:].split(":")
        )
        offset = dt.timedelta(hours=offset_hour, minutes=offset_minute)
        timezone = dt.timezone(offset if zone[0] == "+" else -offset)
    second = int(match.group("second"))
    try:
        value = dt.datetime(
            int(match.group("year")),
            int(match.group("month")),
            int(match.group("day")),
            int(match.group("hour")),
            int(match.group("minute")),
            min(second, 59),
            microsecond,
            tzinfo=timezone,
        )
        if second == 60:
            value += dt.timedelta(seconds=1)
        value = value.astimezone(dt.timezone.utc)
        if requires_microsecond_ceiling:
            value += dt.timedelta(microseconds=1)
    except (OverflowError, ValueError):
        raise _MalformedFhirIdentifierPeriod(
            "FHIR identifier period is malformed"
        ) from None
    return value, True


def _fhir_period_bound(
    candidate: object,
    *,
    upper: bool,
) -> tuple[dt.datetime, bool] | None:
    if candidate is None:
        return None
    if type(candidate) is not str or not candidate or candidate != candidate.strip():
        raise _MalformedFhirIdentifierPeriod("FHIR identifier period is malformed")
    partial_date = _partial_date_bound(candidate, upper=upper)
    if partial_date is not None:
        return partial_date
    return _exact_fhir_datetime_bound(candidate, upper=upper)


def _identifier_is_effective(
    identifier: Mapping[str, Any],
    *,
    observed_at: dt.datetime | dt.date | None,
    policy: FhirTinNpiIdentifierRule,
) -> bool:
    identifier_use = identifier.get("use")
    if (
        type(identifier_use) is str
        and identifier_use in policy.excluded_identifier_uses
    ):
        return False
    raw_period = identifier.get("period")
    if raw_period is not None and not isinstance(raw_period, Mapping):
        raise _MalformedFhirIdentifierPeriod("FHIR identifier period is malformed")
    if isinstance(raw_period, Mapping):
        period_start = raw_period.get("start")
        period_end = raw_period.get("end")
    else:
        period_start = identifier.get("period_start")
        period_end = identifier.get("period_end")
    if period_start is None and period_end is None:
        return True
    observation = _as_utc_datetime(observed_at)
    if observation is None:
        raise _UnresolvedFhirIdentifierPeriod(
            "FHIR identifier period cannot be resolved"
        )
    start = _fhir_period_bound(period_start, upper=False)
    end = _fhir_period_bound(period_end, upper=True)
    if (
        start is not None
        and end is not None
        and (start[0] > end[0] or start[0] == end[0] and not end[1])
    ):
        raise _MalformedFhirIdentifierPeriod("FHIR identifier period is malformed")
    starts_on_or_before_observation = start is None or start[0] <= observation
    if end is None:
        ends_on_or_after_observation = True
    elif end[1]:
        ends_on_or_after_observation = observation <= end[0]
    else:
        ends_on_or_after_observation = observation < end[0]
    return starts_on_or_before_observation and ends_on_or_after_observation


def _normalize_npi(candidate: object) -> int:
    if type(candidate) is not str:
        raise TinNpiConnectorError("NPI is malformed")
    stripped = candidate.strip()
    if any(
        not character.isascii()
        or not (character.isdigit() or character in _ALLOWED_NPI_SEPARATORS)
        for character in stripped
    ):
        raise TinNpiConnectorError("NPI is malformed")
    digits = "".join(character for character in stripped if character.isdigit())
    if _NORMALIZED_NPI_PATTERN.fullmatch(digits) is None:
        raise TinNpiConnectorError("NPI is malformed")
    npi = int(digits)
    npi_digits = [int(digit) for digit in digits]
    digit_sum = _NPI_LUHN_PREFIX_DIGIT_SUM + npi_digits[-1]
    for position, digit in enumerate(npi_digits[:-1], start=1):
        if position % 2:
            doubled = digit * 2
            digit_sum += doubled - 9 if doubled > 9 else doubled
        else:
            digit_sum += digit
    if not _NPI_MIN <= npi <= _NPI_MAX or digit_sum % 10:
        raise TinNpiConnectorError("NPI is malformed")
    return npi


def _strict_evidence_id(candidate: object, field_name: str, *, limit: int) -> str:
    if (
        type(candidate) is not str
        or candidate != candidate.strip()
        or not 1 <= len(candidate) <= limit
        or not candidate.isprintable()
    ):
        raise TinNpiConnectorError(f"FHIR evidence {field_name} is invalid")
    return candidate


@dataclass(frozen=True, repr=False)
class FhirTinNpiEvidence:
    """One same-Organization TIN-token to NPI assertion."""

    token: TinTaxIdentityToken
    npi: int
    source_id: str
    source_endpoint_id: str
    source_dataset_id: str
    source_record_hmac_sha256: bytes
    source_record_identity_sha256: bytes
    source_record_payload_hash: str
    evidence_as_of: str
    identifier_policy_id: str
    identifier_policy_sha256: str
    identifier_rule_id: str
    identifier_rule_sha256: str
    relationship_class: str = FHIR_SAME_ORGANIZATION_RELATIONSHIP

    def __post_init__(self) -> None:
        if type(self.token) is not TinTaxIdentityToken:
            raise TinNpiConnectorError("FHIR evidence TIN token is invalid")
        if (
            type(self.npi) is not int
            or not _NPI_MIN <= self.npi <= _NPI_MAX
            or _normalize_npi(str(self.npi)) != self.npi
        ):
            raise TinNpiConnectorError("FHIR evidence NPI is invalid")
        _strict_evidence_id(self.source_id, "source ID", limit=64)
        _strict_evidence_id(self.source_endpoint_id, "endpoint ID", limit=64)
        _strict_evidence_id(self.source_dataset_id, "dataset ID", limit=128)
        _strict_hash_hex(
            self.source_record_payload_hash,
            "FHIR Organization payload hash",
        )
        if (
            type(self.source_record_hmac_sha256) is not bytes
            or len(self.source_record_hmac_sha256) != 32
            or type(self.source_record_identity_sha256) is not bytes
            or len(self.source_record_identity_sha256) != 32
        ):
            raise TinNpiConnectorError(
                "FHIR evidence source-record identity is invalid"
            )
        canonical_evidence_as_of(self.evidence_as_of)
        _strict_evidence_id(
            self.identifier_policy_id,
            "identifier policy ID",
            limit=128,
        )
        _strict_hash_hex(
            self.identifier_policy_sha256,
            "FHIR identifier policy hash",
        )
        _strict_evidence_id(
            self.identifier_rule_id,
            "identifier rule ID",
            limit=128,
        )
        _strict_hash_hex(
            self.identifier_rule_sha256,
            "FHIR identifier rule hash",
        )
        if self.relationship_class != FHIR_SAME_ORGANIZATION_RELATIONSHIP:
            raise TinNpiConnectorError("FHIR evidence relationship is invalid")

    @property
    def evidence_id(self) -> bytes:
        policy_id = self.token.token_policy_id.encode("ascii")
        relationship = self.relationship_class.encode("ascii")
        if len(policy_id) > 0xFFFF or len(relationship) > 0xFFFF:
            raise TinNpiConnectorError("FHIR evidence identity is invalid")
        return hashlib.sha256(
            _EVIDENCE_HASH_DOMAIN
            + struct.pack(">H", len(policy_id))
            + policy_id
            + self.token.tin_hmac_sha256
            + struct.pack(">q", self.npi)
            + struct.pack(">H", len(relationship))
            + relationship
            + self.source_record_hmac_sha256
            + self.source_record_identity_sha256
            + bytes.fromhex(self.source_record_payload_hash)
            + bytes.fromhex(self.identifier_policy_sha256)
            + bytes.fromhex(self.identifier_rule_sha256)
        ).digest()

    def __repr__(self) -> str:
        return (
            "<fhir-tin-npi-evidence "
            f"source_id={self.source_id!r} "
            f"source_endpoint_id={self.source_endpoint_id!r} "
            f"npi={self.npi!r} token=<redacted>>"
        )


@dataclass(frozen=True)
class FhirOrganizationEvidenceResult:
    """One non-sensitive extraction result and zero or more NPI assertions."""

    state: FhirOrganizationEvidenceState
    evidence: tuple[FhirTinNpiEvidence, ...] = ()

    def __post_init__(self) -> None:
        if type(self.state) is not FhirOrganizationEvidenceState:
            raise TinNpiConnectorError("FHIR evidence state is invalid")
        if type(self.evidence) is not tuple or any(
            type(item) is not FhirTinNpiEvidence for item in self.evidence
        ):
            raise TinNpiConnectorError("FHIR evidence result is invalid")
        if (self.state is FhirOrganizationEvidenceState.MATCHED) != bool(self.evidence):
            raise TinNpiConnectorError("FHIR evidence result is inconsistent")


def _fhir_organization_identity_bytes(
    resource_id: object,
    payload_hash: object,
) -> bytes:
    canonical_resource_id = _strict_evidence_id(
        resource_id,
        "FHIR Organization resource ID",
        limit=256,
    )
    canonical_payload_hash = _strict_hash_hex(
        payload_hash,
        "FHIR Organization payload hash",
    )
    return json.dumps(
        ["Organization", canonical_resource_id, canonical_payload_hash],
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _provider_directory_json_default(value: object) -> object:
    if isinstance(value, (dt.date, dt.datetime)):
        return value.isoformat()
    return str(value)


def canonical_provider_directory_payload_hash(
    payload: Mapping[str, Any],
) -> str:
    """Recompute the dataset-resource hash under the importer contract."""

    if not isinstance(payload, Mapping):
        raise TinNpiConnectorError("FHIR Organization payload is invalid")
    try:
        encoded = json.dumps(
            dict(payload),
            sort_keys=True,
            default=_provider_directory_json_default,
        ).encode("utf-8")
    except (TypeError, ValueError):
        raise TinNpiConnectorError("FHIR Organization payload is invalid") from None
    return hashlib.sha256(encoded).hexdigest()


def _fhir_organization_record_identity_sha256(
    resource_id: object,
    payload_hash: object,
) -> bytes:
    return hashlib.sha256(
        _FHIR_ORGANIZATION_RECORD_BINDING_HASH_DOMAIN
        + _fhir_organization_identity_bytes(resource_id, payload_hash)
    ).digest()


def _verified_fhir_organization_record_identity_sha256(
    *,
    resource_id: object,
    payload: Mapping[str, Any],
    payload_hash: object,
) -> bytes:
    canonical_payload_hash = _strict_hash_hex(
        payload_hash,
        "FHIR Organization payload hash",
    )
    if not hmac.compare_digest(
        canonical_payload_hash,
        canonical_provider_directory_payload_hash(payload),
    ):
        raise TinNpiConnectorError("FHIR Organization payload hash mismatch")
    return _fhir_organization_record_identity_sha256(
        resource_id,
        canonical_payload_hash,
    )


def canonical_fhir_organization_identity_sha256(
    identities: Iterable[tuple[str, str]],
) -> str:
    """Hash exact ordered Organization identities using the dataset contract."""

    if isinstance(identities, (str, bytes, bytearray)):
        raise TinNpiConnectorError("FHIR Organization identities are invalid")
    digest = hashlib.sha256()
    previous_resource_id: str | None = None
    count = 0
    try:
        for resource_id, payload_hash in identities:
            canonical_resource_id = _strict_evidence_id(
                resource_id,
                "FHIR Organization resource ID",
                limit=256,
            )
            if (
                previous_resource_id is not None
                and canonical_resource_id <= previous_resource_id
            ):
                raise TinNpiConnectorError(
                    "FHIR Organization identities are not strictly ordered"
                )
            if count:
                digest.update(b"\n")
            digest.update(
                _fhir_organization_identity_bytes(
                    canonical_resource_id,
                    payload_hash,
                )
            )
            previous_resource_id = canonical_resource_id
            count += 1
    except (TypeError, ValueError):
        raise TinNpiConnectorError("FHIR Organization identities are invalid") from None
    return digest.hexdigest()


@dataclass(frozen=True)
class FhirOrganizationScanRecord:
    """One Organization and its single terminal outcome from the stable scan."""

    source_id: str
    source_endpoint_id: str
    source_dataset_id: str
    resource_id: str
    payload_hash: str
    state: FhirOrganizationEvidenceState
    evidence: tuple[FhirTinNpiEvidence, ...] = ()

    def __post_init__(self) -> None:
        _strict_evidence_id(self.source_id, "source ID", limit=64)
        _strict_evidence_id(self.source_endpoint_id, "endpoint ID", limit=64)
        _strict_evidence_id(self.source_dataset_id, "dataset ID", limit=128)
        _fhir_organization_identity_bytes(self.resource_id, self.payload_hash)
        if (
            type(self.state) is not FhirOrganizationEvidenceState
            or self.state not in FHIR_ORGANIZATION_SCAN_TERMINAL_STATES
            or type(self.evidence) is not tuple
            or any(type(item) is not FhirTinNpiEvidence for item in self.evidence)
            or (self.state is FhirOrganizationEvidenceState.MATCHED)
            != bool(self.evidence)
        ):
            raise TinNpiConnectorError("FHIR Organization scan record is invalid")
        evidence_keys: list[tuple[str, int, bytes]] = []
        npi_sets_by_policy: dict[str, set[int]] = {}
        source_records_by_policy: dict[str, set[bytes]] = {}
        token_hmacs_by_policy: dict[str, set[bytes]] = {}
        identifier_policy_identities: set[tuple[str, str]] = set()
        identifier_rule_identities: set[tuple[str, str]] = set()
        expected_record_identity = _fhir_organization_record_identity_sha256(
            self.resource_id,
            self.payload_hash,
        )
        for item in self.evidence:
            if (
                item.source_id != self.source_id
                or item.source_endpoint_id != self.source_endpoint_id
                or item.source_dataset_id != self.source_dataset_id
            ):
                raise TinNpiConnectorError(
                    "FHIR Organization scan evidence is outside its record"
                )
            if not hmac.compare_digest(
                item.source_record_identity_sha256,
                expected_record_identity,
            ) or not hmac.compare_digest(
                item.source_record_payload_hash,
                self.payload_hash,
            ):
                raise TinNpiConnectorError(
                    "FHIR Organization scan evidence identity is inconsistent"
                )
            evidence_keys.append(
                (
                    item.token.token_policy_id,
                    item.npi,
                    item.evidence_id,
                )
            )
            npi_sets_by_policy.setdefault(
                item.token.token_policy_id,
                set(),
            ).add(item.npi)
            source_records_by_policy.setdefault(
                item.token.token_policy_id,
                set(),
            ).add(item.source_record_hmac_sha256)
            token_hmacs_by_policy.setdefault(
                item.token.token_policy_id,
                set(),
            ).add(item.token.tin_hmac_sha256)
            identifier_policy_identities.add(
                (
                    item.identifier_policy_id,
                    item.identifier_policy_sha256,
                )
            )
            identifier_rule_identities.add(
                (
                    item.identifier_rule_id,
                    item.identifier_rule_sha256,
                )
            )
        if evidence_keys != sorted(set(evidence_keys)):
            raise TinNpiConnectorError(
                "FHIR Organization scan evidence is duplicated or unordered"
            )
        if npi_sets_by_policy and (
            len(identifier_policy_identities) != 1
            or len(identifier_rule_identities) != 1
            or len({tuple(sorted(npis)) for npis in npi_sets_by_policy.values()}) != 1
            or any(
                len(source_records) != 1
                for source_records in source_records_by_policy.values()
            )
            or any(
                len(token_hmacs) != 1 for token_hmacs in token_hmacs_by_policy.values()
            )
            or len(evidence_keys)
            != len(npi_sets_by_policy) * len(next(iter(npi_sets_by_policy.values())))
        ):
            raise TinNpiConnectorError(
                "FHIR Organization scan policy evidence is inconsistent"
            )

    @property
    def scan_key(self) -> tuple[bytes, bytes, bytes, bytes]:
        return (
            self.source_id.encode("utf-8"),
            self.source_endpoint_id.encode("utf-8"),
            self.source_dataset_id.encode("utf-8"),
            self.resource_id.encode("utf-8"),
        )


@dataclass(frozen=True)
class FhirOrganizationScanProof:
    """Compact proof that every Organization reached one terminal state."""

    source_id: str
    endpoint_id: str
    dataset_id: str
    source_summary_sha256: str
    identifier_rule_id: str
    identifier_rule_sha256: str
    organization_resource_count: int
    organization_resource_sha256: str
    state_counts: tuple[tuple[str, int], ...]
    matched_evidence_counts: tuple[tuple[str, int], ...]
    matched_evidence_sha256: str

    def __post_init__(self) -> None:
        _strict_evidence_id(self.source_id, "source ID", limit=64)
        _strict_evidence_id(self.endpoint_id, "endpoint ID", limit=64)
        _strict_evidence_id(self.dataset_id, "dataset ID", limit=128)
        _strict_hash_hex(self.source_summary_sha256, "FHIR source-summary hash")
        _strict_evidence_id(
            self.identifier_rule_id,
            "identifier rule ID",
            limit=128,
        )
        _strict_hash_hex(
            self.identifier_rule_sha256,
            "FHIR identifier rule hash",
        )
        _strict_hash_hex(
            self.organization_resource_sha256,
            "FHIR Organization resource hash",
        )
        _strict_hash_hex(
            self.matched_evidence_sha256,
            "FHIR matched evidence hash",
        )
        expected_state_names = tuple(
            sorted(state.value for state in FHIR_ORGANIZATION_SCAN_TERMINAL_STATES)
        )
        if (
            type(self.organization_resource_count) is not int
            or self.organization_resource_count < 0
            or type(self.state_counts) is not tuple
            or tuple(name for name, _count in self.state_counts) != expected_state_names
            or any(
                type(count) is not int or count < 0
                for _name, count in self.state_counts
            )
            or sum(count for _name, count in self.state_counts)
            != self.organization_resource_count
            or type(self.matched_evidence_counts) is not tuple
            or not self.matched_evidence_counts
            or tuple(policy_id for policy_id, _count in self.matched_evidence_counts)
            != tuple(
                sorted(
                    {policy_id for policy_id, _count in self.matched_evidence_counts}
                )
            )
            or any(
                canonical_token_policy_id(policy_id) != policy_id
                or type(count) is not int
                or count < self.matched_organization_count
                for policy_id, count in self.matched_evidence_counts
            )
            or (
                self.matched_evidence_counts
                and len({count for _policy_id, count in self.matched_evidence_counts})
                != 1
            )
            or (self.matched_organization_count == 0)
            != (sum(count for _policy_id, count in self.matched_evidence_counts) == 0)
        ):
            raise TinNpiConnectorError("FHIR Organization scan proof is invalid")

    @property
    def matched_organization_count(self) -> int:
        return dict(self.state_counts)[FhirOrganizationEvidenceState.MATCHED.value]

    @property
    def matched_evidence_count(self) -> int:
        return sum(count for _policy_id, count in self.matched_evidence_counts)

    def public_payload(self) -> dict[str, Any]:
        return {
            "dataset_id": self.dataset_id,
            "endpoint_id": self.endpoint_id,
            "identifier_rule_id": self.identifier_rule_id,
            "identifier_rule_sha256": self.identifier_rule_sha256,
            "matched_evidence_counts": dict(self.matched_evidence_counts),
            "matched_evidence_sha256": self.matched_evidence_sha256,
            "matched_organization_count": self.matched_organization_count,
            "organization_resource_count": self.organization_resource_count,
            "organization_resource_sha256": self.organization_resource_sha256,
            "source_id": self.source_id,
            "source_summary_sha256": self.source_summary_sha256,
            "state_counts": dict(self.state_counts),
        }


def canonical_fhir_evidence_set_digest(
    evidence: Iterable[FhirTinNpiEvidence],
) -> bytes:
    """Hash a complete evidence set by its immutable evidence identities."""

    if isinstance(evidence, (str, bytes, bytearray)):
        raise TinNpiConnectorError("FHIR evidence set is invalid")
    try:
        evidence_rows = tuple(evidence)
        evidence_ids = tuple(
            sorted(
                item.evidence_id
                for item in evidence_rows
                if type(item) is FhirTinNpiEvidence
            )
        )
    except TypeError:
        raise TinNpiConnectorError("FHIR evidence set is invalid") from None
    if len(evidence_ids) != len(evidence_rows) or len(set(evidence_ids)) != len(
        evidence_ids
    ):
        raise TinNpiConnectorError("FHIR evidence set is invalid")
    return hashlib.sha256(_EVIDENCE_SET_HASH_DOMAIN + b"".join(evidence_ids)).digest()


def canonical_fhir_organization_scan_proof_json(
    proofs: Iterable[FhirOrganizationScanProof],
) -> str:
    """Serialize the complete per-dataset proof under the frozen scan contract."""

    if isinstance(proofs, (str, bytes, bytearray)):
        raise TinNpiConnectorError("FHIR Organization scan proofs are invalid")
    try:
        canonical_proofs = tuple(proofs)
    except TypeError:
        raise TinNpiConnectorError(
            "FHIR Organization scan proofs are invalid"
        ) from None
    if any(type(proof) is not FhirOrganizationScanProof for proof in canonical_proofs):
        raise TinNpiConnectorError("FHIR Organization scan proofs are invalid")
    proof_keys = tuple(
        (proof.source_id, proof.endpoint_id, proof.dataset_id)
        for proof in canonical_proofs
    )
    if proof_keys != tuple(sorted(set(proof_keys))):
        raise TinNpiConnectorError(
            "FHIR Organization scan proofs are duplicated or unordered"
        )
    return json.dumps(
        {
            "contract_id": TIN_NPI_FHIR_ORGANIZATION_SCAN_CONTRACT_ID,
            "datasets": [proof.public_payload() for proof in canonical_proofs],
            "organization_identity_contract_id": (
                TIN_NPI_FHIR_ORGANIZATION_IDENTITY_CONTRACT_ID
            ),
        },
        sort_keys=True,
        separators=(",", ":"),
    )


def canonical_fhir_organization_scan_proof_digest(
    proofs: Iterable[FhirOrganizationScanProof],
) -> bytes:
    """Bind the full scan proof into the physical connector generation."""

    return hashlib.sha256(
        _SCAN_PROOF_HASH_DOMAIN
        + canonical_fhir_organization_scan_proof_json(proofs).encode("utf-8")
    ).digest()


def _strict_hash_hex(candidate: object, field_name: str) -> str:
    if type(candidate) is not str or _HASH_HEX_PATTERN.fullmatch(candidate) is None:
        raise TinNpiConnectorError(f"{field_name} is invalid")
    return candidate


def _strict_optional_text(
    candidate: object,
    field_name: str,
    *,
    limit: int,
) -> str | None:
    if candidate is None:
        return None
    return _strict_evidence_id(candidate, field_name, limit=limit)


def _strict_string_tuple(
    candidate: object,
    field_name: str,
    *,
    limit: int,
) -> tuple[str, ...]:
    if type(candidate) is not tuple:
        raise TinNpiConnectorError(f"{field_name} is invalid")
    values = tuple(
        _strict_evidence_id(value, field_name, limit=limit) for value in candidate
    )
    if values != tuple(sorted(set(values))):
        raise TinNpiConnectorError(f"{field_name} is invalid")
    return values


def _canonical_source_ids(source_ids: Iterable[str]) -> tuple[str, ...]:
    if isinstance(source_ids, (str, bytes, bytearray)):
        raise TinNpiConnectorError("connector source ordinal map is invalid")
    try:
        values = tuple(
            _strict_evidence_id(source_id, "source ID", limit=64)
            for source_id in source_ids
        )
    except TypeError:
        raise TinNpiConnectorError("connector source ordinal map is invalid") from None
    if not values:
        raise TinNpiConnectorError("connector source ordinal map is invalid")
    return tuple(sorted(set(values), key=lambda value: value.encode("utf-8")))


def canonical_source_ordinal_map_json(source_ids: Iterable[str]) -> str:
    """Encode source ID ordinals as compact canonical UTF-8 JSON."""

    canonical_source_ids = _canonical_source_ids(source_ids)
    return json.dumps(
        [
            {"ordinal": ordinal, "source_id": source_id}
            for ordinal, source_id in enumerate(canonical_source_ids)
        ],
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def canonical_source_ordinal_map_digest(
    source_ids: Iterable[str],
) -> bytes:
    """Seal the canonical source ordinal map for independent DB verification."""

    return hashlib.sha256(
        _SOURCE_ORDINAL_MAP_HASH_DOMAIN
        + canonical_source_ordinal_map_json(source_ids).encode("utf-8")
    ).digest()


def _source_bitmap(
    source_ids: tuple[str, ...],
    *,
    source_ordinal_map: tuple[str, ...],
) -> bytes:
    ordinal_by_source_id = {
        source_id: ordinal for ordinal, source_id in enumerate(source_ordinal_map)
    }
    bitmap = bytearray((len(source_ordinal_map) + 7) // 8)
    try:
        for source_id in source_ids:
            ordinal = ordinal_by_source_id[source_id]
            bitmap[ordinal // 8] |= 1 << (ordinal % 8)
    except KeyError:
        raise TinNpiConnectorError(
            "forward lookup source IDs are outside the source ordinal map"
        ) from None
    return bytes(bitmap)


@dataclass(frozen=True)
class FhirDatasetFenceIdentity:
    """Immutable Provider Directory dataset selection used by one rebuild."""

    source_id: str
    endpoint_id: str
    dataset_id: str
    evidence_run_id: str
    selected_resources: tuple[str, ...]
    expected_resources: tuple[str, ...]
    status: str
    is_current: bool
    promote_on_cutover: bool
    dataset_hash: str
    resource_count: int
    organization_resource_count: int
    organization_resource_sha256: str
    source_summary_sha256: str
    identifier_rule_id: str
    identifier_rule_sha256: str
    recorded_expected_resources: tuple[str, ...] | None = None
    previous_dataset_id: str | None = None
    expected_incumbent_dataset_id: str | None = None
    validated_at: str | None = None

    def __post_init__(self) -> None:
        _strict_evidence_id(self.source_id, "source ID", limit=64)
        _strict_evidence_id(self.endpoint_id, "endpoint ID", limit=64)
        _strict_evidence_id(self.dataset_id, "dataset ID", limit=128)
        _strict_evidence_id(self.evidence_run_id, "evidence run ID", limit=128)
        _strict_string_tuple(
            self.selected_resources,
            "selected resources",
            limit=64,
        )
        _strict_string_tuple(
            self.expected_resources,
            "expected resources",
            limit=64,
        )
        if self.recorded_expected_resources is not None:
            _strict_string_tuple(
                self.recorded_expected_resources,
                "recorded expected resources",
                limit=64,
            )
        _strict_evidence_id(self.status, "dataset status", limit=32)
        if (
            type(self.is_current) is not bool
            or type(self.promote_on_cutover) is not bool
        ):
            raise TinNpiConnectorError("FHIR dataset selection flags are invalid")
        if (
            self.status != "published"
            or not self.is_current
            or self.promote_on_cutover
            or self.expected_incumbent_dataset_id is not None
        ):
            raise TinNpiConnectorError(
                "connector FHIR dataset must already be current and published"
            )
        _strict_hash_hex(self.dataset_hash, "FHIR dataset hash")
        if type(self.resource_count) is not int or self.resource_count < 0:
            raise TinNpiConnectorError("FHIR dataset resource count is invalid")
        if (
            type(self.organization_resource_count) is not int
            or not 0 <= self.organization_resource_count <= self.resource_count
        ):
            raise TinNpiConnectorError("FHIR Organization resource count is invalid")
        _strict_hash_hex(
            self.organization_resource_sha256,
            "FHIR Organization resource hash",
        )
        _strict_hash_hex(
            self.source_summary_sha256,
            "FHIR source-summary hash",
        )
        _strict_evidence_id(
            self.identifier_rule_id,
            "identifier rule ID",
            limit=128,
        )
        _strict_hash_hex(
            self.identifier_rule_sha256,
            "FHIR identifier rule hash",
        )
        _strict_optional_text(
            self.previous_dataset_id,
            "previous dataset ID",
            limit=128,
        )
        _strict_optional_text(
            self.expected_incumbent_dataset_id,
            "expected incumbent dataset ID",
            limit=128,
        )
        _strict_optional_text(self.validated_at, "validated at", limit=64)
        if self.validated_at is None:
            raise TinNpiConnectorError(
                "connector FHIR dataset requires validation evidence"
            )
        if (
            self.recorded_expected_resources is None
            or self.recorded_expected_resources != self.expected_resources
        ):
            raise TinNpiConnectorError(
                "connector FHIR dataset requires recorded expected resources"
            )
        if "Organization" not in self.selected_resources:
            raise TinNpiConnectorError(
                "connector FHIR dataset must select Organization"
            )

    def public_payload(self) -> dict[str, Any]:
        return {
            "dataset_hash": self.dataset_hash,
            "dataset_id": self.dataset_id,
            "endpoint_id": self.endpoint_id,
            "evidence_run_id": self.evidence_run_id,
            "expected_incumbent_dataset_id": self.expected_incumbent_dataset_id,
            "expected_resources": list(self.expected_resources),
            "identifier_rule_id": self.identifier_rule_id,
            "identifier_rule_sha256": self.identifier_rule_sha256,
            "is_current": self.is_current,
            "organization_resource_count": self.organization_resource_count,
            "organization_resource_sha256": self.organization_resource_sha256,
            "previous_dataset_id": self.previous_dataset_id,
            "promote_on_cutover": self.promote_on_cutover,
            "recorded_expected_resources": (
                list(self.recorded_expected_resources)
                if self.recorded_expected_resources is not None
                else None
            ),
            "resource_count": self.resource_count,
            "selected_resources": list(self.selected_resources),
            "source_id": self.source_id,
            "source_summary_sha256": self.source_summary_sha256,
            "status": self.status,
            "validated_at": self.validated_at,
        }


@dataclass(frozen=True)
class ConnectorRelationIdentity:
    """Physical relation fence used to reject an input swap during a build."""

    schema: str
    relation: str
    relation_oid: int
    relkind: str = "r"
    relpersistence: str = "p"

    def __post_init__(self) -> None:
        _strict_evidence_id(self.schema, "relation schema", limit=63)
        _strict_evidence_id(self.relation, "relation name", limit=63)
        if type(self.relation_oid) is not int or self.relation_oid <= 0:
            raise TinNpiConnectorError("relation OID is invalid")
        if self.relkind not in {"r", "p"}:
            raise TinNpiConnectorError("relation kind is invalid")
        if self.relpersistence != "p":
            raise TinNpiConnectorError("relation persistence is invalid")

    def public_payload(self) -> dict[str, Any]:
        return {
            "relation": self.relation,
            "relation_oid": self.relation_oid,
            "relkind": self.relkind,
            "relpersistence": self.relpersistence,
            "schema": self.schema,
        }


@dataclass(frozen=True)
class TinNpiConnectorSourceVector:
    """Complete immutable input identity for one swappable same-entity build."""

    fhir_datasets: tuple[FhirDatasetFenceIdentity, ...]
    input_relations: tuple[ConnectorRelationIdentity, ...]
    token_policies: tuple[TinTokenPolicyDescriptor, ...]
    evidence_as_of: str
    identifier_policy: FhirTinNpiIdentifierPolicy
    lookup_contract_id: str = TIN_NPI_LOOKUP_CONTRACT_ID
    lookup_schema_version: int = TIN_NPI_LOOKUP_SCHEMA_VERSION
    projection_policy_id: str = TIN_NPI_PROJECTION_POLICY_ID
    schema_version: int = TIN_NPI_SOURCE_VECTOR_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != TIN_NPI_SOURCE_VECTOR_SCHEMA_VERSION:
            raise TinNpiConnectorError("connector source-vector version is invalid")
        if (
            self.lookup_schema_version != TIN_NPI_LOOKUP_SCHEMA_VERSION
            or self.lookup_contract_id != TIN_NPI_LOOKUP_CONTRACT_ID
        ):
            raise TinNpiConnectorError("connector lookup contract is invalid")
        if (
            type(self.fhir_datasets) is not tuple
            or not self.fhir_datasets
            or any(
                type(dataset) is not FhirDatasetFenceIdentity
                for dataset in self.fhir_datasets
            )
        ):
            raise TinNpiConnectorError("connector FHIR datasets are invalid")
        if len(
            {
                (dataset.source_id, dataset.endpoint_id, dataset.dataset_id)
                for dataset in self.fhir_datasets
            }
        ) != len(self.fhir_datasets):
            raise TinNpiConnectorError("connector FHIR datasets are duplicated")
        if len({dataset.source_id for dataset in self.fhir_datasets}) != len(
            self.fhir_datasets
        ):
            raise TinNpiConnectorError(
                "connector FHIR source selects more than one dataset"
            )
        endpoint_dataset_identities: dict[str, tuple[object, ...]] = {}
        for dataset in self.fhir_datasets:
            dataset_identity = (
                dataset.endpoint_id,
                dataset.dataset_id,
                dataset.evidence_run_id,
                dataset.selected_resources,
                dataset.expected_resources,
                dataset.status,
                dataset.is_current,
                dataset.promote_on_cutover,
                dataset.dataset_hash,
                dataset.resource_count,
                dataset.organization_resource_count,
                dataset.organization_resource_sha256,
                dataset.source_summary_sha256,
                dataset.recorded_expected_resources,
                dataset.previous_dataset_id,
                dataset.expected_incumbent_dataset_id,
                dataset.validated_at,
            )
            incumbent_identity = endpoint_dataset_identities.setdefault(
                dataset.endpoint_id,
                dataset_identity,
            )
            if incumbent_identity != dataset_identity:
                raise TinNpiConnectorError(
                    "connector FHIR endpoint dataset identities conflict"
                )
        if (
            type(self.input_relations) is not tuple
            or len(self.input_relations) != 1
            or any(
                type(relation) is not ConnectorRelationIdentity
                for relation in self.input_relations
            )
        ):
            raise TinNpiConnectorError("connector input relations are invalid")
        if self.input_relations[0].relation != TIN_NPI_FHIR_INPUT_RELATION:
            raise TinNpiConnectorError("connector FHIR input relation is invalid")
        if (
            type(self.token_policies) is not tuple
            or not self.token_policies
            or any(
                type(policy) is not TinTokenPolicyDescriptor
                for policy in self.token_policies
            )
        ):
            raise TinNpiConnectorError("connector token policies are invalid")
        canonical_policy_ids = tuple(
            policy.token_policy_id for policy in self.token_policies
        )
        if len(set(canonical_policy_ids)) != len(canonical_policy_ids):
            raise TinNpiConnectorError("connector token policies are duplicated")
        canonical_evidence_as_of(self.evidence_as_of)
        if type(self.identifier_policy) is not FhirTinNpiIdentifierPolicy:
            raise TinNpiConnectorError("connector identifier policy is invalid")
        selected_rule_identity_by_scope = {
            (dataset.source_id, dataset.endpoint_id): (
                dataset.identifier_rule_id,
                dataset.identifier_rule_sha256,
            )
            for dataset in self.fhir_datasets
        }
        policy_rule_identity_by_scope = {
            (rule.source_id, rule.endpoint_id): (
                rule.rule_id,
                rule.descriptor_sha256,
            )
            for rule in self.identifier_policy.rules
        }
        if selected_rule_identity_by_scope != policy_rule_identity_by_scope:
            raise TinNpiConnectorError(
                "connector identifier policy scope is inconsistent"
            )
        _strict_evidence_id(
            self.projection_policy_id,
            "projection policy ID",
            limit=128,
        )

    def public_payload(self) -> dict[str, Any]:
        return {
            "fhir_datasets": sorted(
                (dataset.public_payload() for dataset in self.fhir_datasets),
                key=lambda payload: json.dumps(
                    payload,
                    sort_keys=True,
                    separators=(",", ":"),
                ),
            ),
            "evidence_as_of": self.evidence_as_of,
            "identifier_policy_id": self.identifier_policy.policy_id,
            "identifier_policy_sha256": (self.identifier_policy.descriptor_sha256),
            "input_relations": sorted(
                (relation.public_payload() for relation in self.input_relations),
                key=lambda payload: (
                    payload["schema"],
                    payload["relation"],
                    payload["relation_oid"],
                ),
            ),
            "lookup_contract_id": self.lookup_contract_id,
            "lookup_schema_version": self.lookup_schema_version,
            "projection_policy_id": self.projection_policy_id,
            "schema_version": self.schema_version,
            "site_resolution_contract_id": TIN_NPI_SITE_RESOLUTION_CONTRACT_ID,
            "source_scope_contract_id": TIN_NPI_SOURCE_SCOPE_CONTRACT_ID,
            "source_record_identity_contract_id": (
                FHIR_SOURCE_RECORD_HMAC_MESSAGE_FORMAT_ID
            ),
            "token_policies": sorted(
                (policy.public_payload() for policy in self.token_policies),
                key=lambda payload: payload["token_policy_id"],
            ),
            "token_policy_scope_contract_id": (TIN_NPI_TOKEN_POLICY_SCOPE_CONTRACT_ID),
            "token_policy_ids": sorted(self.token_policy_ids),
        }

    @property
    def token_policy_ids(self) -> tuple[str, ...]:
        return tuple(policy.token_policy_id for policy in self.token_policies)

    @property
    def canonical_json(self) -> str:
        return json.dumps(
            self.public_payload(),
            sort_keys=True,
            separators=(",", ":"),
        )

    @property
    def source_vector_id(self) -> str:
        return hashlib.sha256(
            _SOURCE_VECTOR_HASH_DOMAIN + self.canonical_json.encode("utf-8")
        ).hexdigest()


@dataclass(frozen=True, repr=False)
class TinNpiLookupRow:
    """Compact forward row: one policy/token/relationship to an NPI array."""

    token: TinTaxIdentityToken
    relationship_class: str
    npis: tuple[int, ...]
    evidence_count: int
    source_ids: tuple[str, ...]
    source_bitmap: bytes
    npi_source_bitmap_matrix: bytes
    source_evidence_counts: tuple[int, ...]

    def __post_init__(self) -> None:
        if type(self.token) is not TinTaxIdentityToken:
            raise TinNpiConnectorError("forward lookup token is invalid")
        if self.relationship_class != FHIR_SAME_ORGANIZATION_RELATIONSHIP:
            raise TinNpiConnectorError("forward lookup relationship is invalid")
        if (
            type(self.npis) is not tuple
            or not self.npis
            or self.npis != tuple(sorted(set(self.npis)))
            or any(
                type(npi) is not int or _normalize_npi(str(npi)) != npi
                for npi in self.npis
            )
        ):
            raise TinNpiConnectorError("forward lookup NPIs are invalid")
        if (
            type(self.evidence_count) is not int
            or not len(self.npis) <= self.evidence_count <= 0x7FFF_FFFF_FFFF_FFFF
        ):
            raise TinNpiConnectorError("forward lookup evidence count is invalid")
        canonical_source_ids = _canonical_source_ids(self.source_ids)
        if self.source_ids != canonical_source_ids:
            raise TinNpiConnectorError("forward lookup source IDs are invalid")
        if type(self.source_evidence_counts) is not tuple:
            raise TinNpiConnectorError("forward lookup source bitmap is invalid")
        source_count = len(self.source_evidence_counts)
        bitmap_width = (source_count + 7) // 8
        if (
            source_count <= 0
            or type(self.source_bitmap) is not bytes
            or len(self.source_bitmap) != bitmap_width
            or not any(self.source_bitmap)
            or type(self.npi_source_bitmap_matrix) is not bytes
            or len(self.npi_source_bitmap_matrix) != len(self.npis) * bitmap_width
            or any(
                type(count) is not int or count < 0
                for count in self.source_evidence_counts
            )
            or sum(self.source_evidence_counts) != self.evidence_count
        ):
            raise TinNpiConnectorError("forward lookup source bitmap is invalid")
        aggregate_bitmap = bytearray(bitmap_width)
        per_source_npi_support = [0] * source_count
        for npi_ordinal in range(len(self.npis)):
            segment_start = npi_ordinal * bitmap_width
            segment = self.npi_source_bitmap_matrix[
                slice(segment_start, segment_start + bitmap_width)
            ]
            if not any(segment):
                raise TinNpiConnectorError("forward lookup source bitmap is invalid")
            if source_count % 8 and segment[-1] >= 1 << (source_count % 8):
                raise TinNpiConnectorError("forward lookup source bitmap is invalid")
            for byte_ordinal, source_byte in enumerate(segment):
                aggregate_bitmap[byte_ordinal] |= source_byte
            for source_ordinal in range(source_count):
                if segment[source_ordinal // 8] & (1 << (source_ordinal % 8)):
                    per_source_npi_support[source_ordinal] += 1
        if (
            bytes(aggregate_bitmap) != self.source_bitmap
            or source_count % 8
            and self.source_bitmap[-1] >= 1 << (source_count % 8)
            or any(
                (count > 0)
                != bool(
                    self.source_bitmap[source_ordinal // 8]
                    & (1 << (source_ordinal % 8))
                )
                or count < per_source_npi_support[source_ordinal]
                for source_ordinal, count in enumerate(self.source_evidence_counts)
            )
        ):
            raise TinNpiConnectorError("forward lookup source bitmap is invalid")

    def source_bitmap_for_npi(self, npi: int) -> bytes:
        """Return the fixed-width source segment aligned to one sorted NPI."""

        try:
            npi_ordinal = self.npis.index(npi)
        except ValueError:
            raise TinNpiConnectorError("forward lookup NPI is unavailable") from None
        bitmap_width = (len(self.source_evidence_counts) + 7) // 8
        segment_start = npi_ordinal * bitmap_width
        return self.npi_source_bitmap_matrix[
            slice(segment_start, segment_start + bitmap_width)
        ]

    def npis_supported_by_source_ordinal(
        self,
        source_ordinal: int,
    ) -> tuple[int, ...]:
        """Filter NPIs by one authenticated source-map ordinal."""

        source_count = len(self.source_evidence_counts)
        if type(source_ordinal) is not int or not 0 <= source_ordinal < source_count:
            raise TinNpiConnectorError("forward lookup source ordinal is invalid")
        return tuple(
            npi
            for npi in self.npis
            if self.source_bitmap_for_npi(npi)[source_ordinal // 8]
            & (1 << (source_ordinal % 8))
        )

    def __repr__(self) -> str:
        return (
            "<tin-npi-lookup-row "
            f"token_policy_id={self.token.token_policy_id!r} "
            f"relationship_class={self.relationship_class!r} "
            f"npi_count={len(self.npis)} token=<redacted>>"
        )


@dataclass(frozen=True, repr=False)
class NpiTinLookupReference:
    """One reverse reference from an NPI to a policy-scoped TIN token."""

    token: TinTaxIdentityToken
    relationship_class: str

    def __post_init__(self) -> None:
        if type(self.token) is not TinTaxIdentityToken:
            raise TinNpiConnectorError("reverse lookup token is invalid")
        if self.relationship_class != FHIR_SAME_ORGANIZATION_RELATIONSHIP:
            raise TinNpiConnectorError("reverse lookup relationship is invalid")

    def __repr__(self) -> str:
        return (
            "<npi-tin-lookup-reference "
            f"token_policy_id={self.token.token_policy_id!r} token=<redacted>>"
        )


@dataclass(frozen=True)
class NpiTinLookupRow:
    """Compact reverse row used for refresh and evidence diagnostics."""

    npi: int
    tax_identities: tuple[NpiTinLookupReference, ...]

    def __post_init__(self) -> None:
        if (
            type(self.npi) is not int
            or _normalize_npi(str(self.npi)) != self.npi
            or type(self.tax_identities) is not tuple
            or not self.tax_identities
            or any(
                type(reference) is not NpiTinLookupReference
                for reference in self.tax_identities
            )
        ):
            raise TinNpiConnectorError("reverse lookup row is invalid")
        reference_keys = tuple(
            (
                reference.token.token_policy_id,
                reference.token.tin_hmac_sha256,
                reference.relationship_class,
            )
            for reference in self.tax_identities
        )
        if reference_keys != tuple(sorted(set(reference_keys))):
            raise TinNpiConnectorError("reverse lookup references are invalid")


def _forward_row_key(
    row: TinNpiLookupRow,
) -> tuple[str, bytes, str]:
    return (
        row.token.token_policy_id,
        row.token.tin_hmac_sha256,
        row.relationship_class,
    )


def _lookup_row_hash(row: TinNpiLookupRow) -> tuple[bytes, bytes, bytes]:
    policy_bytes = row.token.token_policy_id.encode("ascii")
    if (
        len(policy_bytes) > 0xFFFF
        or len(row.npis) > 0xFFFF_FFFF
        or len(row.source_bitmap) > 0xFFFF_FFFF
        or len(row.npi_source_bitmap_matrix) > 0xFFFF_FFFF
    ):
        raise TinNpiConnectorError("forward lookup row cannot be encoded")
    row_hash = hashlib.sha256(
        b"".join(
            (
                _LOOKUP_ROW_HASH_DOMAIN,
                struct.pack(">H", len(policy_bytes)),
                policy_bytes,
                row.token.tin_hmac_sha256,
                struct.pack(">I", len(row.npis)),
                *(struct.pack(">q", npi) for npi in row.npis),
                struct.pack(">q", row.evidence_count),
                struct.pack(">I", len(row.source_bitmap)),
                row.source_bitmap,
                struct.pack(">I", len(row.npi_source_bitmap_matrix)),
                row.npi_source_bitmap_matrix,
                struct.pack(">I", len(row.source_evidence_counts)),
                *(struct.pack(">q", count) for count in row.source_evidence_counts),
            )
        )
    ).digest()
    return policy_bytes, row.token.tin_hmac_sha256, row_hash


def _lookup_digest(rows: Sequence[TinNpiLookupRow]) -> bytes:
    buckets: list[list[tuple[bytes, bytes, bytes]]] = [[] for _ in range(256)]
    for row in rows:
        encoded_row = _lookup_row_hash(row)
        buckets[encoded_row[2][0]].append(encoded_row)
    bucket_hashes = b"".join(
        hashlib.sha256(
            _LOOKUP_BUCKET_HASH_DOMAIN
            + struct.pack(">H", bucket)
            + b"".join(
                row_hash
                for _, _, row_hash in sorted(
                    bucket_rows,
                    key=lambda encoded_row: (
                        encoded_row[0],
                        encoded_row[1],
                    ),
                )
            )
        ).digest()
        for bucket, bucket_rows in enumerate(buckets)
    )
    return hashlib.sha256(_LOOKUP_SET_HASH_DOMAIN + bucket_hashes).digest()


def _generation_id(
    *,
    source_vector_id: str,
    scan_proof_digest: bytes,
    lookup_digest: bytes,
) -> str:
    _strict_hash_hex(source_vector_id, "connector source-vector ID")
    if (
        type(scan_proof_digest) is not bytes
        or len(scan_proof_digest) != 32
        or type(lookup_digest) is not bytes
        or len(lookup_digest) != 32
    ):
        raise TinNpiConnectorError("connector generation digests are invalid")
    return hashlib.sha256(
        _GENERATION_HASH_DOMAIN
        + bytes.fromhex(source_vector_id)
        + scan_proof_digest
        + lookup_digest
    ).hexdigest()


def _factor_forward_rows(
    evidence_rows: Sequence[FhirTinNpiEvidence],
    *,
    source_ordinal_map: tuple[str, ...],
) -> tuple[TinNpiLookupRow, ...]:
    """Derive the exact hot lookup projection from immutable evidence rows."""

    source_ordinal_by_id = {
        source_id: ordinal for ordinal, source_id in enumerate(source_ordinal_map)
    }
    grouped: dict[
        tuple[str, bytes, bytes, str],
        list[FhirTinNpiEvidence],
    ] = {}
    for evidence in evidence_rows:
        key = (
            evidence.token.token_policy_id,
            evidence.token.tin_id_128,
            evidence.token.tin_hmac_sha256,
            evidence.relationship_class,
        )
        grouped.setdefault(key, []).append(evidence)
    forward_rows: list[TinNpiLookupRow] = []
    for key in sorted(grouped):
        group = grouped[key]
        source_counts = [0] * len(source_ordinal_map)
        source_ordinals_by_npi: dict[int, set[int]] = {}
        for evidence in group:
            try:
                source_ordinal = source_ordinal_by_id[evidence.source_id]
            except KeyError:
                raise TinNpiConnectorError(
                    "connector evidence source is outside the ordinal map"
                ) from None
            source_counts[source_ordinal] += 1
            source_ordinals_by_npi.setdefault(evidence.npi, set()).add(source_ordinal)
        npis = tuple(sorted(source_ordinals_by_npi))
        source_ids = tuple(
            source_id
            for source_id, count in zip(source_ordinal_map, source_counts)
            if count
        )
        forward_rows.append(
            TinNpiLookupRow(
                token=group[0].token,
                relationship_class=key[3],
                npis=npis,
                evidence_count=len(group),
                source_ids=source_ids,
                source_bitmap=_source_bitmap(
                    source_ids,
                    source_ordinal_map=source_ordinal_map,
                ),
                npi_source_bitmap_matrix=b"".join(
                    _source_bitmap(
                        tuple(
                            source_ordinal_map[ordinal]
                            for ordinal in sorted(source_ordinals_by_npi[npi])
                        ),
                        source_ordinal_map=source_ordinal_map,
                    )
                    for npi in npis
                ),
                source_evidence_counts=tuple(source_counts),
            )
        )
    return tuple(forward_rows)


@dataclass(frozen=True)
class CompactTinNpiGeneration:
    """Deterministic factored lookup payload ready for staged publication."""

    generation_id: str
    source_vector_id: str
    source_ordinal_map: tuple[str, ...]
    source_ordinal_map_digest: bytes
    scan_proofs: tuple[FhirOrganizationScanProof, ...]
    scan_proof_digest: bytes
    lookup_digest: bytes
    evidence_rows: tuple[FhirTinNpiEvidence, ...]
    forward_rows: tuple[TinNpiLookupRow, ...]
    reverse_rows: tuple[NpiTinLookupRow, ...]

    def __post_init__(self) -> None:
        _strict_hash_hex(self.generation_id, "connector generation ID")
        _strict_hash_hex(self.source_vector_id, "connector source-vector ID")
        canonical_source_ids = _canonical_source_ids(self.source_ordinal_map)
        if (
            type(self.source_ordinal_map) is not tuple
            or self.source_ordinal_map != canonical_source_ids
            or type(self.source_ordinal_map_digest) is not bytes
            or len(self.source_ordinal_map_digest) != 32
            or type(self.lookup_digest) is not bytes
            or len(self.lookup_digest) != 32
            or type(self.scan_proofs) is not tuple
            or any(
                type(proof) is not FhirOrganizationScanProof
                for proof in self.scan_proofs
            )
            or type(self.scan_proof_digest) is not bytes
            or len(self.scan_proof_digest) != 32
            or type(self.evidence_rows) is not tuple
            or any(type(row) is not FhirTinNpiEvidence for row in self.evidence_rows)
            or type(self.forward_rows) is not tuple
            or any(type(row) is not TinNpiLookupRow for row in self.forward_rows)
            or type(self.reverse_rows) is not tuple
            or any(type(row) is not NpiTinLookupRow for row in self.reverse_rows)
        ):
            raise TinNpiConnectorError("compact connector generation is invalid")
        evidence_ids = tuple(row.evidence_id for row in self.evidence_rows)
        if evidence_ids != tuple(sorted(set(evidence_ids))):
            raise TinNpiConnectorError("compact connector evidence rows are invalid")
        forward_keys = tuple(_forward_row_key(row) for row in self.forward_rows)
        if forward_keys != tuple(sorted(set(forward_keys))):
            raise TinNpiConnectorError("compact connector forward rows are invalid")
        reverse_npis = tuple(row.npi for row in self.reverse_rows)
        if reverse_npis != tuple(sorted(set(reverse_npis))):
            raise TinNpiConnectorError("compact connector reverse rows are invalid")
        expected_reverse_keys = {
            (npi, *_forward_row_key(row))
            for row in self.forward_rows
            for npi in row.npis
        }
        actual_reverse_keys = {
            (
                row.npi,
                reference.token.token_policy_id,
                reference.token.tin_hmac_sha256,
                reference.relationship_class,
            )
            for row in self.reverse_rows
            for reference in row.tax_identities
        }
        proof_by_source_id = {proof.source_id: proof for proof in self.scan_proofs}
        evidence_policy_identities = {
            (
                row.identifier_policy_id,
                row.identifier_policy_sha256,
            )
            for row in self.evidence_rows
        }
        evidence_scope_is_valid = all(
            (proof := proof_by_source_id.get(row.source_id)) is not None
            and row.source_endpoint_id == proof.endpoint_id
            and row.source_dataset_id == proof.dataset_id
            and row.identifier_rule_id == proof.identifier_rule_id
            and row.identifier_rule_sha256 == proof.identifier_rule_sha256
            for row in self.evidence_rows
        )
        if (
            expected_reverse_keys != actual_reverse_keys
            or tuple(proof.source_id for proof in self.scan_proofs)
            != self.source_ordinal_map
            or len(evidence_policy_identities) > 1
            or not evidence_scope_is_valid
            or len(self.evidence_rows) != self.evidence_count
            or self.forward_rows
            != _factor_forward_rows(
                self.evidence_rows,
                source_ordinal_map=self.source_ordinal_map,
            )
            or {
                proof.source_id: proof.matched_evidence_sha256
                for proof in self.scan_proofs
            }
            != {
                source_id: canonical_fhir_evidence_set_digest(
                    row for row in self.evidence_rows if row.source_id == source_id
                ).hex()
                for source_id in self.source_ordinal_map
            }
            or self.evidence_count > 0x7FFF_FFFF_FFFF_FFFF
            or not hmac.compare_digest(
                self.source_ordinal_map_digest,
                canonical_source_ordinal_map_digest(self.source_ordinal_map),
            )
            or any(
                not hmac.compare_digest(
                    row.source_bitmap,
                    _source_bitmap(
                        row.source_ids,
                        source_ordinal_map=self.source_ordinal_map,
                    ),
                )
                for row in self.forward_rows
            )
            or not hmac.compare_digest(
                self.lookup_digest,
                _lookup_digest(self.forward_rows),
            )
            or not hmac.compare_digest(
                self.scan_proof_digest,
                canonical_fhir_organization_scan_proof_digest(self.scan_proofs),
            )
            or self._observed_source_policy_evidence_counts()
            != self._expected_source_policy_evidence_counts()
            or self.generation_id
            != _generation_id(
                source_vector_id=self.source_vector_id,
                scan_proof_digest=self.scan_proof_digest,
                lookup_digest=self.lookup_digest,
            )
        ):
            raise TinNpiConnectorError("compact connector generation is inconsistent")

    @property
    def source_ordinal_map_json(self) -> str:
        return canonical_source_ordinal_map_json(self.source_ordinal_map)

    @property
    def evidence_count(self) -> int:
        return sum(row.evidence_count for row in self.forward_rows)

    @property
    def organization_count(self) -> int:
        return sum(proof.organization_resource_count for proof in self.scan_proofs)

    @property
    def matched_organization_count(self) -> int:
        return sum(proof.matched_organization_count for proof in self.scan_proofs)

    @property
    def scan_proof_canonical_json(self) -> str:
        return canonical_fhir_organization_scan_proof_json(self.scan_proofs)

    def _expected_source_policy_evidence_counts(
        self,
    ) -> dict[tuple[str, str], int]:
        return {
            (proof.source_id, policy_id): count
            for proof in self.scan_proofs
            for policy_id, count in proof.matched_evidence_counts
        }

    def _observed_source_policy_evidence_counts(
        self,
    ) -> dict[tuple[str, str], int]:
        observed = {
            (source_id, policy_id): 0
            for source_id, policy_id in self._expected_source_policy_evidence_counts()
        }
        for row in self.forward_rows:
            if len(row.source_evidence_counts) != len(self.source_ordinal_map):
                raise TinNpiConnectorError(
                    "compact connector source evidence counts are invalid"
                )
            for ordinal, count in enumerate(row.source_evidence_counts):
                source_id = self.source_ordinal_map[ordinal]
                expected_bit = bool(count)
                observed_bit = bool(
                    row.source_bitmap[ordinal // 8] & (1 << (ordinal % 8))
                )
                if expected_bit != observed_bit:
                    raise TinNpiConnectorError(
                        "compact connector source evidence counts are invalid"
                    )
                observed[(source_id, row.token.token_policy_id)] = (
                    observed.get((source_id, row.token.token_policy_id), 0) + count
                )
        return observed


def assert_generation_reuse_compatible(
    incumbent: CompactTinNpiGeneration,
    candidate: CompactTinNpiGeneration,
) -> bool:
    """Reject nondeterministic content for an already-seen source vector."""

    if (
        type(incumbent) is not CompactTinNpiGeneration
        or type(candidate) is not CompactTinNpiGeneration
    ):
        raise TinNpiConnectorError("connector generation reuse input is invalid")
    if incumbent.source_vector_id != candidate.source_vector_id:
        return False
    if incumbent != candidate:
        raise TinNpiConnectorError("connector source vector produced different content")
    return True


def _lookup_key(
    evidence: FhirTinNpiEvidence,
) -> tuple[str, bytes, bytes, str]:
    return (
        evidence.token.token_policy_id,
        evidence.token.tin_id_128,
        evidence.token.tin_hmac_sha256,
        evidence.relationship_class,
    )


def _scan_proofs_and_evidence(
    scan_records: Iterable[FhirOrganizationScanRecord],
    *,
    source_vector: TinNpiConnectorSourceVector,
) -> tuple[
    tuple[FhirOrganizationScanProof, ...],
    tuple[FhirTinNpiEvidence, ...],
]:
    if isinstance(scan_records, (str, bytes, bytearray)):
        raise TinNpiConnectorError("connector Organization scan is invalid")
    selected_policies = set(source_vector.token_policy_ids)
    dataset_by_key = {
        (dataset.source_id, dataset.endpoint_id, dataset.dataset_id): dataset
        for dataset in source_vector.fhir_datasets
    }
    digest_by_dataset = {
        dataset_key: hashlib.sha256() for dataset_key in dataset_by_key
    }
    count_by_dataset = dict.fromkeys(dataset_by_key, 0)
    state_count_by_dataset: dict[
        tuple[str, str, str],
        dict[FhirOrganizationEvidenceState, int],
    ] = {
        dataset_key: dict.fromkeys(FHIR_ORGANIZATION_SCAN_TERMINAL_STATES, 0)
        for dataset_key in dataset_by_key
    }
    evidence_count_by_dataset_policy = {
        dataset_key: dict.fromkeys(source_vector.token_policy_ids, 0)
        for dataset_key in dataset_by_key
    }
    evidence_rows_by_dataset: dict[
        tuple[str, str, str],
        list[FhirTinNpiEvidence],
    ] = {dataset_key: [] for dataset_key in dataset_by_key}
    evidence_rows: list[FhirTinNpiEvidence] = []
    previous_scan_key: tuple[bytes, bytes, bytes, bytes] | None = None
    try:
        for record in scan_records:
            if type(record) is not FhirOrganizationScanRecord:
                raise TinNpiConnectorError(
                    "connector Organization scan record is invalid"
                )
            if previous_scan_key is not None and record.scan_key <= previous_scan_key:
                raise TinNpiConnectorError(
                    "connector Organization scan is not strictly ordered"
                )
            previous_scan_key = record.scan_key
            dataset_key = (
                record.source_id,
                record.source_endpoint_id,
                record.source_dataset_id,
            )
            dataset = dataset_by_key.get(dataset_key)
            if dataset is None:
                raise TinNpiConnectorError(
                    "connector Organization scan is outside its source vector"
                )
            count = count_by_dataset[dataset_key]
            if count:
                digest_by_dataset[dataset_key].update(b"\n")
            digest_by_dataset[dataset_key].update(
                _fhir_organization_identity_bytes(
                    record.resource_id,
                    record.payload_hash,
                )
            )
            count_by_dataset[dataset_key] = count + 1
            state_count_by_dataset[dataset_key][record.state] += 1
            record_policy_ids = {
                evidence.token.token_policy_id for evidence in record.evidence
            }
            if record.state is FhirOrganizationEvidenceState.MATCHED:
                if record_policy_ids != selected_policies:
                    raise TinNpiConnectorError(
                        "connector Organization scan does not cover every token policy"
                    )
            elif record_policy_ids:
                raise TinNpiConnectorError(
                    "connector Organization scan terminal state is inconsistent"
                )
            for evidence in record.evidence:
                if (
                    evidence.identifier_policy_id
                    != source_vector.identifier_policy.policy_id
                    or evidence.identifier_policy_sha256
                    != source_vector.identifier_policy.descriptor_sha256
                    or evidence.identifier_rule_id != dataset.identifier_rule_id
                    or evidence.identifier_rule_sha256 != dataset.identifier_rule_sha256
                    or evidence.evidence_as_of != source_vector.evidence_as_of
                ):
                    raise TinNpiConnectorError(
                        "connector Organization scan identifier policy mismatch"
                    )
                evidence_count_by_dataset_policy[dataset_key][
                    evidence.token.token_policy_id
                ] += 1
                evidence_rows.append(evidence)
                evidence_rows_by_dataset[dataset_key].append(evidence)
    except TypeError:
        raise TinNpiConnectorError("connector Organization scan is invalid") from None

    proofs: list[FhirOrganizationScanProof] = []
    for dataset_key in sorted(dataset_by_key):
        dataset = dataset_by_key[dataset_key]
        observed_count = count_by_dataset[dataset_key]
        observed_digest = digest_by_dataset[dataset_key].hexdigest()
        if (
            observed_count != dataset.organization_resource_count
            or not hmac.compare_digest(
                observed_digest,
                dataset.organization_resource_sha256,
            )
        ):
            raise TinNpiConnectorError(
                "connector Organization scan completeness proof mismatch"
            )
        proofs.append(
            FhirOrganizationScanProof(
                source_id=dataset.source_id,
                endpoint_id=dataset.endpoint_id,
                dataset_id=dataset.dataset_id,
                source_summary_sha256=dataset.source_summary_sha256,
                identifier_rule_id=dataset.identifier_rule_id,
                identifier_rule_sha256=dataset.identifier_rule_sha256,
                organization_resource_count=observed_count,
                organization_resource_sha256=observed_digest,
                state_counts=tuple(
                    (
                        state.value,
                        state_count_by_dataset[dataset_key][state],
                    )
                    for state in sorted(
                        FHIR_ORGANIZATION_SCAN_TERMINAL_STATES,
                        key=lambda candidate: candidate.value,
                    )
                ),
                matched_evidence_counts=tuple(
                    (
                        policy_id,
                        evidence_count_by_dataset_policy[dataset_key][policy_id],
                    )
                    for policy_id in sorted(source_vector.token_policy_ids)
                ),
                matched_evidence_sha256=canonical_fhir_evidence_set_digest(
                    evidence_rows_by_dataset[dataset_key]
                ).hex(),
            )
        )
    return tuple(proofs), tuple(evidence_rows)


def build_compact_tin_npi_generation(
    scan_records: Iterable[FhirOrganizationScanRecord],
    *,
    source_vector: TinNpiConnectorSourceVector,
) -> CompactTinNpiGeneration:
    """Scan every Organization and factor its complete same-entity evidence."""

    if type(source_vector) is not TinNpiConnectorSourceVector:
        raise TinNpiConnectorError("connector source vector is invalid")
    scan_proofs, evidence_rows = _scan_proofs_and_evidence(
        scan_records,
        source_vector=source_vector,
    )
    unique_evidence_by_id: dict[bytes, FhirTinNpiEvidence] = {}
    selected_datasets = {
        (dataset.source_id, dataset.endpoint_id, dataset.dataset_id): dataset
        for dataset in source_vector.fhir_datasets
    }
    selected_policies = set(source_vector.token_policy_ids)
    for evidence in evidence_rows:
        if type(evidence) is not FhirTinNpiEvidence:
            raise TinNpiConnectorError("connector evidence row is invalid")
        if (
            (
                evidence.source_id,
                evidence.source_endpoint_id,
                evidence.source_dataset_id,
            )
            not in selected_datasets
            or evidence.token.token_policy_id not in selected_policies
            or evidence.identifier_policy_id
            != source_vector.identifier_policy.policy_id
            or evidence.identifier_policy_sha256
            != source_vector.identifier_policy.descriptor_sha256
            or evidence.evidence_as_of != source_vector.evidence_as_of
        ):
            raise TinNpiConnectorError(
                "connector evidence is outside its source vector"
            )
        selected_dataset = selected_datasets[
            (
                evidence.source_id,
                evidence.source_endpoint_id,
                evidence.source_dataset_id,
            )
        ]
        if (
            evidence.identifier_rule_id != selected_dataset.identifier_rule_id
            or evidence.identifier_rule_sha256
            != selected_dataset.identifier_rule_sha256
        ):
            raise TinNpiConnectorError(
                "connector evidence identifier rule is outside its source vector"
            )
        incumbent = unique_evidence_by_id.setdefault(evidence.evidence_id, evidence)
        if incumbent != evidence:
            raise TinNpiConnectorError("connector evidence identity collision")
    if (
        unique_evidence_by_id
        and {
            evidence.token.token_policy_id
            for evidence in unique_evidence_by_id.values()
        }
        != selected_policies
    ):
        raise TinNpiConnectorError(
            "connector evidence does not cover every token policy"
        )
    if len(unique_evidence_by_id) != sum(
        proof.matched_evidence_count for proof in scan_proofs
    ):
        raise TinNpiConnectorError(
            "connector Organization scan evidence identity collision"
        )
    unique_evidence = tuple(
        unique_evidence_by_id[evidence_id]
        for evidence_id in sorted(unique_evidence_by_id)
    )
    source_ordinal_map = _canonical_source_ids(
        dataset.source_id for dataset in source_vector.fhir_datasets
    )
    forward_rows = _factor_forward_rows(
        unique_evidence,
        source_ordinal_map=source_ordinal_map,
    )
    reverse_references_by_npi: dict[int, list[NpiTinLookupReference]] = {}
    for row in forward_rows:
        for npi in row.npis:
            reverse_references_by_npi.setdefault(npi, []).append(
                NpiTinLookupReference(
                    token=row.token,
                    relationship_class=row.relationship_class,
                )
            )
    reverse_rows = tuple(
        NpiTinLookupRow(
            npi=npi,
            tax_identities=tuple(
                sorted(
                    reverse_references_by_npi[npi],
                    key=lambda reference: (
                        reference.token.token_policy_id,
                        reference.token.tin_hmac_sha256,
                        reference.relationship_class,
                    ),
                )
            ),
        )
        for npi in sorted(reverse_references_by_npi)
    )
    lookup_digest = _lookup_digest(forward_rows)
    scan_proof_digest = canonical_fhir_organization_scan_proof_digest(scan_proofs)
    source_vector_id = source_vector.source_vector_id
    return CompactTinNpiGeneration(
        generation_id=_generation_id(
            source_vector_id=source_vector_id,
            scan_proof_digest=scan_proof_digest,
            lookup_digest=lookup_digest,
        ),
        source_vector_id=source_vector_id,
        source_ordinal_map=source_ordinal_map,
        source_ordinal_map_digest=canonical_source_ordinal_map_digest(
            source_ordinal_map
        ),
        scan_proofs=scan_proofs,
        scan_proof_digest=scan_proof_digest,
        lookup_digest=lookup_digest,
        evidence_rows=unique_evidence,
        forward_rows=forward_rows,
        reverse_rows=reverse_rows,
    )


def _canonical_token_projectors(
    token_projectors: object,
) -> tuple[TinTokenProjector, ...]:
    if type(token_projectors) is not tuple or not token_projectors:
        raise TinNpiConnectorError("TIN token projectors are invalid")
    policy_ids: list[str] = []
    for projector in token_projectors:
        try:
            policy_ids.append(canonical_token_policy_id(projector.token_policy_id))
        except (AttributeError, TinNpiConnectorError):
            raise TinNpiConnectorError("TIN token projectors are invalid") from None
    if policy_ids != sorted(set(policy_ids)):
        raise TinNpiConnectorError("TIN token projectors are duplicated or unordered")
    return token_projectors


def _extract_verified_fhir_organization_tin_npi_evidence(
    resource: Mapping[str, Any],
    *,
    source_id: str,
    source_endpoint_id: str,
    source_dataset_id: str,
    source_record_identity_sha256: bytes,
    source_record_payload_hash: str,
    token_projectors: tuple[TinTokenProjector, ...],
    evidence_as_of: dt.datetime | dt.date | str,
    identifier_policy: FhirTinNpiIdentifierPolicy,
) -> FhirOrganizationEvidenceResult:
    """Project identifiers after the source row's payload identity is verified."""

    if (
        not isinstance(resource, Mapping)
        or resource.get("resourceType") != "Organization"
    ):
        return FhirOrganizationEvidenceResult(
            FhirOrganizationEvidenceState.NOT_ORGANIZATION
        )
    if (
        type(source_record_identity_sha256) is not bytes
        or len(source_record_identity_sha256) != 32
    ):
        raise TinNpiConnectorError(
            "FHIR Organization source-record identity is invalid"
        )
    canonical_record_payload_hash = _strict_hash_hex(
        source_record_payload_hash,
        "FHIR Organization payload hash",
    )
    if type(identifier_policy) is not FhirTinNpiIdentifierPolicy:
        raise TinNpiConnectorError("FHIR identifier policy is invalid")
    identifier_rule = identifier_policy.rule_for(
        source_id=source_id,
        endpoint_id=source_endpoint_id,
    )
    canonical_projectors = _canonical_token_projectors(token_projectors)
    if resource.get("active") is False:
        return FhirOrganizationEvidenceResult(FhirOrganizationEvidenceState.INACTIVE)
    canonical_as_of = canonical_evidence_as_of(evidence_as_of)
    evidence_cutoff = _as_utc_datetime(
        dt.datetime.fromisoformat(canonical_as_of[:-1] + "+00:00")
    )
    identifiers = resource.get("identifier")
    if not isinstance(identifiers, Sequence) or isinstance(
        identifiers,
        (str, bytes, bytearray),
    ):
        return FhirOrganizationEvidenceResult(
            FhirOrganizationEvidenceState.MISSING_IDENTIFIERS
        )
    npi_identifiers: list[Mapping[str, Any]] = []
    ein_identifiers: list[Mapping[str, Any]] = []
    for identifier in identifiers:
        if not isinstance(identifier, Mapping):
            continue
        is_npi = _identifier_matches(
            identifier,
            systems=identifier_rule.npi_systems,
            type_codings=identifier_rule.npi_type_codings,
        )
        is_ein = _identifier_matches(
            identifier,
            systems=identifier_rule.ein_systems,
            type_codings=identifier_rule.ein_type_codings,
        )
        if is_npi and is_ein:
            return FhirOrganizationEvidenceResult(
                FhirOrganizationEvidenceState.CONFLICTING_IDENTIFIER_CLASS
            )
        if not is_npi and not is_ein:
            continue
        try:
            is_effective = _identifier_is_effective(
                identifier,
                observed_at=evidence_cutoff,
                policy=identifier_rule,
            )
        except _UnresolvedFhirIdentifierPeriod:
            return FhirOrganizationEvidenceResult(
                FhirOrganizationEvidenceState.UNRESOLVED_IDENTIFIER_PERIOD
            )
        except _MalformedFhirIdentifierPeriod:
            return FhirOrganizationEvidenceResult(
                FhirOrganizationEvidenceState.MALFORMED_IDENTIFIER_PERIOD
            )
        if not is_effective:
            continue
        if is_npi:
            npi_identifiers.append(identifier)
        if is_ein:
            ein_identifiers.append(identifier)
    if not npi_identifiers and not ein_identifiers:
        return FhirOrganizationEvidenceResult(
            FhirOrganizationEvidenceState.MISSING_IDENTIFIERS
        )
    if not npi_identifiers:
        return FhirOrganizationEvidenceResult(FhirOrganizationEvidenceState.MISSING_NPI)
    if not ein_identifiers:
        return FhirOrganizationEvidenceResult(FhirOrganizationEvidenceState.MISSING_EIN)
    try:
        npis = sorted(
            {_normalize_npi(identifier.get("value")) for identifier in npi_identifiers}
        )
    except TinNpiConnectorError:
        return FhirOrganizationEvidenceResult(
            FhirOrganizationEvidenceState.MALFORMED_NPI
        )
    try:
        normalized_eins = {
            normalize_ein(identifier.get("value")) for identifier in ein_identifiers
        }
    except TinNpiConnectorError:
        return FhirOrganizationEvidenceResult(
            FhirOrganizationEvidenceState.MALFORMED_EIN
        )
    if len(normalized_eins) != 1:
        return FhirOrganizationEvidenceResult(
            FhirOrganizationEvidenceState.AMBIGUOUS_EIN
        )
    normalized_ein = next(iter(normalized_eins))
    resource_id = _strict_evidence_id(
        resource.get("id"),
        "resource ID",
        limit=256,
    )
    token_rows: list[tuple[TinTaxIdentityToken, bytes]] = []
    for token_projector in canonical_projectors:
        token = token_projector.tokenize_ein(normalized_ein)
        if (
            type(token) is not TinTaxIdentityToken
            or token.token_policy_id != token_projector.token_policy_id
        ):
            raise TinNpiConnectorError("TIN token projector returned an invalid token")
        source_record_hmac_sha256 = token_projector.pseudonymize_source_record(
            source_id=source_id,
            source_endpoint_id=source_endpoint_id,
            source_dataset_id=source_dataset_id,
            resource_id=resource_id,
        )
        if (
            type(source_record_hmac_sha256) is not bytes
            or len(source_record_hmac_sha256) != 32
        ):
            raise TinNpiConnectorError(
                "TIN token projector returned an invalid source-record identity"
            )
        token_rows.append((token, source_record_hmac_sha256))
    evidence = tuple(
        FhirTinNpiEvidence(
            token=token,
            npi=npi,
            source_id=source_id,
            source_endpoint_id=source_endpoint_id,
            source_dataset_id=source_dataset_id,
            source_record_hmac_sha256=source_record_hmac_sha256,
            source_record_identity_sha256=source_record_identity_sha256,
            source_record_payload_hash=canonical_record_payload_hash,
            evidence_as_of=canonical_as_of,
            identifier_policy_id=identifier_policy.policy_id,
            identifier_policy_sha256=identifier_policy.descriptor_sha256,
            identifier_rule_id=identifier_rule.rule_id,
            identifier_rule_sha256=identifier_rule.descriptor_sha256,
        )
        for token, source_record_hmac_sha256 in token_rows
        for npi in npis
    )
    return FhirOrganizationEvidenceResult(
        state=FhirOrganizationEvidenceState.MATCHED,
        evidence=evidence,
    )


def extract_fhir_organization_tin_npi_evidence_for_policies(
    resource: Mapping[str, Any],
    *,
    source_id: str,
    source_endpoint_id: str,
    source_dataset_id: str,
    resource_payload_hash: str,
    token_projectors: tuple[TinTokenProjector, ...],
    evidence_as_of: dt.datetime | dt.date | str,
    identifier_policy: FhirTinNpiIdentifierPolicy = (
        DEFAULT_FHIR_TIN_NPI_IDENTIFIER_POLICY
    ),
) -> FhirOrganizationEvidenceResult:
    """Verify one exact source payload, then project same-Organization evidence."""

    if (
        not isinstance(resource, Mapping)
        or resource.get("resourceType") != "Organization"
    ):
        return FhirOrganizationEvidenceResult(
            FhirOrganizationEvidenceState.NOT_ORGANIZATION
        )
    canonical_payload_hash = _strict_hash_hex(
        resource_payload_hash,
        "FHIR Organization payload hash",
    )
    source_record_identity_sha256 = _verified_fhir_organization_record_identity_sha256(
        resource_id=resource.get("id"),
        payload=resource,
        payload_hash=canonical_payload_hash,
    )
    return _extract_verified_fhir_organization_tin_npi_evidence(
        resource,
        source_id=source_id,
        source_endpoint_id=source_endpoint_id,
        source_dataset_id=source_dataset_id,
        source_record_identity_sha256=source_record_identity_sha256,
        source_record_payload_hash=canonical_payload_hash,
        token_projectors=token_projectors,
        evidence_as_of=evidence_as_of,
        identifier_policy=identifier_policy,
    )


def extract_fhir_organization_tin_npi_evidence(
    resource: Mapping[str, Any],
    *,
    source_id: str,
    source_endpoint_id: str,
    source_dataset_id: str,
    resource_payload_hash: str,
    token_projector: TinTokenProjector,
    evidence_as_of: dt.datetime | dt.date | str,
    identifier_policy: FhirTinNpiIdentifierPolicy = (
        DEFAULT_FHIR_TIN_NPI_IDENTIFIER_POLICY
    ),
) -> FhirOrganizationEvidenceResult:
    """Compatibility wrapper for a one-policy extraction pass."""

    return extract_fhir_organization_tin_npi_evidence_for_policies(
        resource,
        source_id=source_id,
        source_endpoint_id=source_endpoint_id,
        source_dataset_id=source_dataset_id,
        resource_payload_hash=resource_payload_hash,
        token_projectors=(token_projector,),
        evidence_as_of=evidence_as_of,
        identifier_policy=identifier_policy,
    )


def extract_normalized_fhir_organization_tin_npi_evidence_for_policies(
    organization_row: Mapping[str, Any],
    *,
    source_id: str,
    source_endpoint_id: str,
    source_dataset_id: str,
    token_projectors: tuple[TinTokenProjector, ...],
    evidence_as_of: dt.datetime | dt.date | str,
    identifier_policy: FhirTinNpiIdentifierPolicy = (
        DEFAULT_FHIR_TIN_NPI_IDENTIFIER_POLICY
    ),
) -> FhirOrganizationEvidenceResult:
    """Adapt one immutable normalized Organization row to the strict extractor."""

    if not isinstance(organization_row, Mapping):
        return FhirOrganizationEvidenceResult(
            FhirOrganizationEvidenceState.NOT_ORGANIZATION
        )
    if organization_row.get("resource_type") != "Organization":
        return FhirOrganizationEvidenceResult(
            FhirOrganizationEvidenceState.NOT_ORGANIZATION
        )
    payload = organization_row.get("payload_json")
    if not isinstance(payload, Mapping):
        raise TinNpiConnectorError("FHIR Organization payload is invalid")
    resource_id = _strict_evidence_id(
        organization_row.get("resource_id"),
        "FHIR Organization resource ID",
        limit=256,
    )
    if payload.get("resource_id") != resource_id:
        raise TinNpiConnectorError("FHIR Organization resource identity mismatch")
    canonical_payload_hash = _strict_hash_hex(
        organization_row.get("payload_hash"),
        "FHIR Organization payload hash",
    )
    source_record_identity_sha256 = _verified_fhir_organization_record_identity_sha256(
        resource_id=resource_id,
        payload=payload,
        payload_hash=canonical_payload_hash,
    )
    return _extract_verified_fhir_organization_tin_npi_evidence(
        {
            "resourceType": "Organization",
            "id": resource_id,
            "active": payload.get("active"),
            "identifier": payload.get("identifiers"),
        },
        source_id=source_id,
        source_endpoint_id=source_endpoint_id,
        source_dataset_id=source_dataset_id,
        source_record_identity_sha256=source_record_identity_sha256,
        source_record_payload_hash=canonical_payload_hash,
        token_projectors=token_projectors,
        evidence_as_of=evidence_as_of,
        identifier_policy=identifier_policy,
    )


def extract_normalized_fhir_organization_tin_npi_evidence(
    organization_row: Mapping[str, Any],
    *,
    source_id: str,
    source_endpoint_id: str,
    source_dataset_id: str,
    token_projector: TinTokenProjector,
    evidence_as_of: dt.datetime | dt.date | str,
    identifier_policy: FhirTinNpiIdentifierPolicy = (
        DEFAULT_FHIR_TIN_NPI_IDENTIFIER_POLICY
    ),
) -> FhirOrganizationEvidenceResult:
    """Compatibility wrapper for one normalized-row token policy."""

    return extract_normalized_fhir_organization_tin_npi_evidence_for_policies(
        organization_row,
        source_id=source_id,
        source_endpoint_id=source_endpoint_id,
        source_dataset_id=source_dataset_id,
        token_projectors=(token_projector,),
        evidence_as_of=evidence_as_of,
        identifier_policy=identifier_policy,
    )


__all__ = [
    "DEFAULT_FHIR_TIN_NPI_IDENTIFIER_POLICY",
    "CompactTinNpiGeneration",
    "ConnectorRelationIdentity",
    "FHIR_SAME_ORGANIZATION_RELATIONSHIP",
    "FHIR_ORGANIZATION_SCAN_TERMINAL_STATES",
    "FHIR_TIN_NPI_IDENTIFIER_POLICY_ID",
    "FhirDatasetFenceIdentity",
    "FhirOrganizationEvidenceResult",
    "FhirOrganizationEvidenceState",
    "FhirOrganizationScanProof",
    "FhirOrganizationScanRecord",
    "FhirTinNpiEvidence",
    "FhirTinNpiIdentifierPolicy",
    "FhirTinNpiIdentifierRule",
    "NpiTinLookupReference",
    "NpiTinLookupRow",
    "TIN_NPI_LOOKUP_SCHEMA_VERSION",
    "TIN_NPI_LOOKUP_CONTRACT_ID",
    "TIN_NPI_FHIR_ORGANIZATION_IDENTITY_CONTRACT_ID",
    "TIN_NPI_FHIR_INPUT_RELATION",
    "TIN_NPI_FHIR_ORGANIZATION_SCAN_CONTRACT_ID",
    "TIN_NPI_PROJECTION_POLICY_ID",
    "TIN_NPI_SITE_RESOLUTION_CONTRACT_ID",
    "TIN_NPI_SOURCE_ORDINAL_CONTRACT_ID",
    "TIN_NPI_SOURCE_VECTOR_SCHEMA_VERSION",
    "TIN_TOKEN_MESSAGE_DOMAIN",
    "TIN_TOKEN_MESSAGE_FORMAT_ID",
    "TIN_TOKEN_POLICY_ID_MAX_BYTES",
    "TIN_TOKEN_POLICY_PREFIX",
    "TinNpiConnectorError",
    "TinNpiConnectorSourceVector",
    "TinNpiLookupRow",
    "TinTaxIdentityToken",
    "TinTokenProjector",
    "assert_generation_reuse_compatible",
    "build_compact_tin_npi_generation",
    "canonical_evidence_as_of",
    "canonical_fhir_organization_identity_sha256",
    "canonical_fhir_evidence_set_digest",
    "canonical_fhir_organization_scan_proof_digest",
    "canonical_fhir_organization_scan_proof_json",
    "canonical_provider_directory_payload_hash",
    "canonical_source_ordinal_map_digest",
    "canonical_source_ordinal_map_json",
    "canonical_token_policy_id",
    "extract_fhir_organization_tin_npi_evidence",
    "extract_fhir_organization_tin_npi_evidence_for_policies",
    "extract_normalized_fhir_organization_tin_npi_evidence",
    "extract_normalized_fhir_organization_tin_npi_evidence_for_policies",
    "load_tin_token_policy",
    "normalize_ein",
]
