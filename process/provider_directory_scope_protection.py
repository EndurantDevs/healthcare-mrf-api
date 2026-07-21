# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Private protected-material primitives for Provider Directory scope lookup."""

from __future__ import annotations

import hashlib
import hmac
import json
import re
import secrets

from process.provider_directory_endpoint_identity import (
    ProviderDirectoryEndpointIdentity,
    ProviderDirectoryEndpointIdentityError,
)


SCOPE_LOOKUP_CONTRACT_ID = "healthporta.provider-directory.scope-lookup-hmac.v1"
SCOPE_LOOKUP_CONTRACT_VERSION = 1
REGISTRY_SCOPE_PREFIX = "pdscope_"

_CONTRACT_PATTERN = re.compile(r"^[a-z0-9][a-z0-9._-]{2,127}$")
_DIGEST_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_SCOPE_ID_PATTERN = re.compile(r"^pdscope_[0-9a-f]{32}$")
_REGISTRY_SCOPE_ENTROPY_BYTES = 16
# Bound registry fan-out to one active key plus seven explicit rotation aliases.
_MAX_SCOPE_LOOKUP_ROTATIONS = 8
_MAX_SCOPE_LOOKUP_KEY_VALUES = 8
_MAX_SCOPE_LOOKUP_KEY_VALUE_LENGTH = 256
# HMAC-SHA256 needs 32 bytes; 256 permits managed-key formats without unbounded input.
_MAX_SCOPE_LOOKUP_SECRET_BYTES = 256


class LogicalScopePseudonymError(ValueError):
    """Report malformed protected lookup or registry identity inputs."""


class _InertScopeSnapshot:
    """Redacted copy that is never a lookup key, request, or decision."""

    __slots__ = ("_label",)

    def __init__(self, label: str = "scope-capability") -> None:
        object.__setattr__(self, "_label", label)

    def __setattr__(self, name: str, value: object) -> None:
        raise TypeError("scope snapshot is immutable")

    def __repr__(self) -> str:
        return f"<redacted-{self._label}>"

    __str__ = __repr__

    def __deepcopy__(self, memo: object) -> _InertScopeSnapshot:
        return self

    def __copy__(self) -> _InertScopeSnapshot:
        return self


class _ProtectedValue:
    """Redact protected bytes or text from copying and serialization."""

    __slots__ = ("__value",)

    def __init__(self, value: bytes | str) -> None:
        object.__setattr__(self, "_ProtectedValue__value", value)

    def __setattr__(self, name: str, value: object) -> None:
        raise TypeError("protected material is immutable")

    def __repr__(self) -> str:
        return "<redacted>"

    __str__ = __repr__

    def __deepcopy__(self, memo: object) -> _InertScopeSnapshot:
        return _InertScopeSnapshot()

    def __copy__(self) -> _InertScopeSnapshot:
        return _InertScopeSnapshot()

    def __reduce__(self) -> tuple[type[_InertScopeSnapshot], tuple[object, ...]]:
        return _InertScopeSnapshot, ()


def _read_protected(value: object) -> bytes | str:
    if type(value) is not _ProtectedValue:
        raise LogicalScopePseudonymError("protected material container is invalid")
    return object.__getattribute__(value, "_ProtectedValue__value")


def _canonical_json(value: object) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _strict_contract(value: object, field_name: str) -> str:
    if type(value) is not str or not _CONTRACT_PATTERN.fullmatch(value):
        raise LogicalScopePseudonymError(f"{field_name} is invalid")
    return value


def _strict_key_version(value: object) -> int:
    if type(value) is not int or not 1 <= value <= 2**31 - 1:
        raise LogicalScopePseudonymError("scope lookup key version is invalid")
    return value


def _strict_key_values(values: object) -> tuple[str, ...]:
    if (
        type(values) is not tuple
        or not 1 <= len(values) <= _MAX_SCOPE_LOOKUP_KEY_VALUES
        or any(
            type(value) is not str
            or not value
            or value != value.strip()
            or len(value) > _MAX_SCOPE_LOOKUP_KEY_VALUE_LENGTH
            or not value.isprintable()
            for value in values
        )
    ):
        raise LogicalScopePseudonymError("scope lookup key values are invalid")
    return tuple(values)


def _validate_registry_scope_id(scope_id: object) -> str:
    if type(scope_id) is not str or not _SCOPE_ID_PATTERN.fullmatch(scope_id):
        raise LogicalScopePseudonymError("registry scope ID is invalid")
    return scope_id


def _new_registry_scope_id() -> str:
    """Issue one private cryptorandom registry ID with no injectable entropy."""

    entropy = secrets.token_bytes(_REGISTRY_SCOPE_ENTROPY_BYTES)
    if type(entropy) is not bytes or len(entropy) != _REGISTRY_SCOPE_ENTROPY_BYTES:
        raise LogicalScopePseudonymError("registry scope entropy is invalid")
    return _validate_registry_scope_id(REGISTRY_SCOPE_PREFIX + entropy.hex())


def _endpoint_public_payload(
    endpoint_identity: ProviderDirectoryEndpointIdentity,
) -> dict[str, str]:
    if type(endpoint_identity) is not ProviderDirectoryEndpointIdentity:
        raise LogicalScopePseudonymError("scope lookup endpoint identity is invalid")
    try:
        return endpoint_identity.public_payload()
    except (AttributeError, ProviderDirectoryEndpointIdentityError, TypeError):
        raise LogicalScopePseudonymError(
            "scope lookup endpoint identity is invalid"
        ) from None


def _compute_pseudonymizer_binding(
    *,
    key_id: str,
    key_version: int,
    secret: bytes,
) -> str:
    descriptor_fields_by_name = {
        "contract_id": SCOPE_LOOKUP_CONTRACT_ID,
        "contract_version": SCOPE_LOOKUP_CONTRACT_VERSION,
        "key_id": key_id,
        "key_version": key_version,
    }
    return hmac.new(
        secret,
        _canonical_json(descriptor_fields_by_name),
        hashlib.sha256,
    ).hexdigest()


class _LogicalScopePseudonymizer:
    """Private key capability whose descriptor is bound to its secret."""

    __slots__ = (
        "_descriptor_binding",
        "_key_id",
        "_key_version",
        "_secret_material",
    )

    def __init__(self, *, key_id: str, key_version: int, secret: bytes) -> None:
        normalized_key_id = _strict_contract(key_id, "scope lookup key ID")
        normalized_key_version = _strict_key_version(key_version)
        if (
            type(secret) is not bytes
            or not 32 <= len(secret) <= _MAX_SCOPE_LOOKUP_SECRET_BYTES
        ):
            raise LogicalScopePseudonymError("scope lookup secret is invalid")
        object.__setattr__(self, "_key_id", normalized_key_id)
        object.__setattr__(self, "_key_version", normalized_key_version)
        object.__setattr__(self, "_secret_material", _ProtectedValue(secret))
        object.__setattr__(
            self,
            "_descriptor_binding",
            _ProtectedValue(
                _compute_pseudonymizer_binding(
                    key_id=normalized_key_id,
                    key_version=normalized_key_version,
                    secret=secret,
                )
            ),
        )
        self.validate()

    def __setattr__(self, name: str, value: object) -> None:
        raise TypeError("scope pseudonymizer is immutable")

    def _validated_material(self) -> tuple[str, int, bytes]:
        try:
            key_id_value = self._key_id
            key_version_value = self._key_version
            secret_material_value = self._secret_material
            descriptor_binding_value = self._descriptor_binding
        except AttributeError:
            raise LogicalScopePseudonymError(
                "scope lookup pseudonymizer state is invalid"
            ) from None
        key_id = _strict_contract(key_id_value, "scope lookup key ID")
        key_version = _strict_key_version(key_version_value)
        try:
            secret = _read_protected(secret_material_value)
            descriptor_binding = _read_protected(descriptor_binding_value)
        except LogicalScopePseudonymError:
            raise LogicalScopePseudonymError("scope lookup secret is invalid") from None
        if (
            type(secret) is not bytes
            or not 32 <= len(secret) <= _MAX_SCOPE_LOOKUP_SECRET_BYTES
        ):
            raise LogicalScopePseudonymError("scope lookup secret is invalid")
        expected_binding = _compute_pseudonymizer_binding(
            key_id=key_id,
            key_version=key_version,
            secret=secret,
        )
        if type(descriptor_binding) is not str or not hmac.compare_digest(
            descriptor_binding,
            expected_binding,
        ):
            raise LogicalScopePseudonymError(
                "scope lookup pseudonymizer binding is invalid"
            )
        return key_id, key_version, secret

    def validate(self) -> None:
        """Reject secret or valid-format descriptor mutation."""

        self._validated_material()

    def public_payload(self) -> dict[str, object]:
        """Return only the non-secret key version descriptor."""

        key_id, key_version, _ = self._validated_material()
        return {"key_id": key_id, "key_version": key_version}

    def __repr__(self) -> str:
        payload = self.public_payload()
        return (
            "<scope-pseudonymizer "
            f"key_id={payload['key_id']!r} "
            f"key_version={payload['key_version']!r}>"
        )

    def __deepcopy__(self, memo: object) -> _InertScopeSnapshot:
        return _InertScopeSnapshot("scope-pseudonymizer")

    def __copy__(self) -> _InertScopeSnapshot:
        return _InertScopeSnapshot("scope-pseudonymizer")

    def __reduce__(self) -> tuple[type[_InertScopeSnapshot], tuple[str, ...]]:
        return _InertScopeSnapshot, ("scope-pseudonymizer",)


def build_scope_pseudonymizer(
    *,
    key_id: str,
    key_version: int,
    secret: bytes,
) -> object:
    """Build an opaque configured-key capability for request construction."""

    return _LogicalScopePseudonymizer(
        key_id=key_id,
        key_version=key_version,
        secret=secret,
    )
