# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Nominal identity for an admitted Provider Directory API endpoint row."""

from __future__ import annotations

import hashlib
import re
from typing import Any, cast

from process.provider_directory_endpoint_admission import (
    _CREDENTIAL_DESCRIPTOR_MAX_BYTES,
    _ENDPOINT_SIGNATURE_MAX_BYTES,
    ProviderDirectoryEndpointIdentityError,
    _admit_provider_directory_endpoint_components,
    _canonical_json,
    _stored_json_object,
    _validate_canonical_api_base,
)


_DIGEST_PATTERN = re.compile(r"^[0-9a-f]{64}$")


class _InertEndpointIdentitySnapshot:
    """Represent a copied endpoint identity without preserving its nominal type."""

    __slots__ = ()

    def __repr__(self) -> str:
        return "<provider-directory-endpoint-identity-snapshot>"

    def __deepcopy__(self, memo: object) -> _InertEndpointIdentitySnapshot:
        return self


def _digest(digest_value: object, field_name: str) -> str:
    if type(digest_value) is not str or not _DIGEST_PATTERN.fullmatch(digest_value):
        raise ProviderDirectoryEndpointIdentityError(
            f"{field_name} must be a lowercase SHA-256 hash"
        )
    return digest_value


def _compute_endpoint_identity_binding(
    *,
    endpoint_id: str,
    canonical_api_base: str,
    credential_descriptor_hash: str,
    endpoint_signature_hash: str,
    credential_descriptor_json: str,
    endpoint_signature_json: str,
) -> str:
    binding_by_field = {
        "canonical_api_base": canonical_api_base,
        "credential_descriptor_hash": credential_descriptor_hash,
        "credential_descriptor_json": credential_descriptor_json,
        "endpoint_id": endpoint_id,
        "endpoint_signature_hash": endpoint_signature_hash,
        "endpoint_signature_json": endpoint_signature_json,
    }
    canonical_binding = _canonical_json(
        binding_by_field,
        "endpoint identity binding",
    )
    return hashlib.sha256(canonical_binding.encode("utf-8")).hexdigest()


def _validate_endpoint_record_hashes(
    *,
    endpoint_id: str,
    credential_descriptor_hash: str,
    endpoint_signature_hash: str,
    admitted_fields: dict[str, object],
) -> None:
    expected_credential_hash = admitted_fields["credential_descriptor_hash"]
    expected_signature_hash = admitted_fields["endpoint_signature_hash"]
    expected_endpoint_id = admitted_fields["endpoint_id"]
    if credential_descriptor_hash != expected_credential_hash:
        raise ProviderDirectoryEndpointIdentityError(
            "credential descriptor hash is inconsistent"
        )
    if endpoint_signature_hash != expected_signature_hash:
        raise ProviderDirectoryEndpointIdentityError(
            "endpoint signature hash is inconsistent"
        )
    if endpoint_id != expected_endpoint_id:
        raise ProviderDirectoryEndpointIdentityError("endpoint ID is inconsistent")


def _validated_record_fields(
    identity_state: tuple[object, ...],
) -> tuple[str, str, str, str, str, str, object, object]:
    (
        endpoint_id_value,
        canonical_api_base_value,
        credential_hash_value,
        signature_hash_value,
        credential_json_value,
        signature_json_value,
        identity_binding_value,
        stable_hash_value,
    ) = identity_state
    endpoint_id = _digest(endpoint_id_value, "endpoint ID")
    canonical_api_base = _validate_canonical_api_base(canonical_api_base_value)
    credential_hash = _digest(credential_hash_value, "credential descriptor hash")
    signature_hash = _digest(signature_hash_value, "endpoint signature hash")
    credential_descriptor, credential_json = _stored_json_object(
        credential_json_value,
        "credential descriptor",
        max_bytes=_CREDENTIAL_DESCRIPTOR_MAX_BYTES,
    )
    endpoint_signature, signature_json = _stored_json_object(
        signature_json_value,
        "endpoint signature",
        max_bytes=_ENDPOINT_SIGNATURE_MAX_BYTES,
    )
    admitted_fields = _admit_provider_directory_endpoint_components(
        canonical_api_base=canonical_api_base,
        credential_descriptor_json=credential_descriptor,
        endpoint_signature_json=endpoint_signature,
    )
    _validate_endpoint_record_hashes(
        endpoint_id=endpoint_id,
        credential_descriptor_hash=credential_hash,
        endpoint_signature_hash=signature_hash,
        admitted_fields=admitted_fields,
    )
    return (
        endpoint_id,
        canonical_api_base,
        credential_hash,
        signature_hash,
        credential_json,
        signature_json,
        identity_binding_value,
        stable_hash_value,
    )


def _validated_identity_snapshot(
    identity_state: tuple[object, ...],
) -> tuple[str, str, str, str, str, int]:
    (
        endpoint_id,
        canonical_api_base,
        credential_hash,
        signature_hash,
        credential_json,
        signature_json,
        identity_binding_value,
        stable_hash_value,
    ) = _validated_record_fields(identity_state)
    expected_binding = _compute_endpoint_identity_binding(
        endpoint_id=endpoint_id,
        canonical_api_base=canonical_api_base,
        credential_descriptor_hash=credential_hash,
        endpoint_signature_hash=signature_hash,
        credential_descriptor_json=credential_json,
        endpoint_signature_json=signature_json,
    )
    if type(identity_binding_value) is not str or identity_binding_value != expected_binding:
        raise ProviderDirectoryEndpointIdentityError(
            "endpoint identity binding is invalid"
        )
    expected_stable_hash = hash(
        (ProviderDirectoryEndpointIdentity, identity_binding_value)
    )
    if type(stable_hash_value) is not int or stable_hash_value != expected_stable_hash:
        raise ProviderDirectoryEndpointIdentityError(
            "endpoint identity stable hash is invalid"
        )
    return (
        endpoint_id,
        canonical_api_base,
        credential_hash,
        signature_hash,
        identity_binding_value,
        stable_hash_value,
    )


class ProviderDirectoryEndpointIdentity:
    """Factory-created endpoint identity that reproduces one endpoint DB row."""

    __slots__ = (
        "_canonical_api_base",
        "_credential_descriptor_hash",
        "_credential_descriptor_json",
        "_endpoint_id",
        "_endpoint_signature_hash",
        "_endpoint_signature_json",
        "_identity_binding",
        "_stable_hash",
    )

    def __new__(cls, *args: object, **kwargs: object) -> ProviderDirectoryEndpointIdentity:
        raise TypeError("use build_provider_directory_endpoint_identity")

    def __setattr__(self, name: str, field_value: object) -> None:
        raise TypeError("provider directory endpoint identity is immutable")

    def _validated_snapshot(self) -> tuple[str, str, str, str, str, int]:
        """Return one integrity-checked immutable snapshot of private slots."""

        try:
            identity_state = (
                self._endpoint_id,
                self._canonical_api_base,
                self._credential_descriptor_hash,
                self._endpoint_signature_hash,
                self._credential_descriptor_json,
                self._endpoint_signature_json,
                self._identity_binding,
                self._stable_hash,
            )
        except AttributeError:
            raise ProviderDirectoryEndpointIdentityError(
                "endpoint identity state is invalid"
            ) from None
        return _validated_identity_snapshot(identity_state)

    @property
    def endpoint_id(self) -> str:
        """Return the importer-compatible endpoint row primary key."""

        return self._validated_snapshot()[0]

    @property
    def canonical_api_base(self) -> str:
        """Return the canonical transport base committed by the endpoint row."""

        return self._validated_snapshot()[1]

    @property
    def credential_descriptor_hash(self) -> str:
        """Return the validated credential descriptor hash."""

        return self._validated_snapshot()[2]

    @property
    def endpoint_signature_hash(self) -> str:
        """Return the validated resource endpoint signature hash."""

        return self._validated_snapshot()[3]

    def public_payload(self) -> dict[str, str]:
        """Serialize only the non-secret endpoint row identity."""

        endpoint_id, api_base, credential_hash, signature_hash, _, _ = (
            self._validated_snapshot()
        )
        return {
            "endpoint_id": endpoint_id,
            "canonical_api_base": api_base,
            "credential_descriptor_hash": credential_hash,
            "endpoint_signature_hash": signature_hash,
        }

    def validate(self) -> None:
        """Recompute all endpoint record hashes and reject later mutation."""

        self._validated_snapshot()

    def __hash__(self) -> int:
        return self._validated_snapshot()[5]

    def __eq__(self, other: object) -> bool:
        if type(other) is not ProviderDirectoryEndpointIdentity:
            return False
        self_binding = self._validated_snapshot()[4]
        other_binding = other._validated_snapshot()[4]
        return self_binding == other_binding

    def __repr__(self) -> str:
        endpoint_id, api_base, _, _, _, _ = self._validated_snapshot()
        return (
            "ProviderDirectoryEndpointIdentity("
            f"endpoint_id={endpoint_id!r}, canonical_api_base={api_base!r})"
        )

    def __deepcopy__(self, memo: object) -> _InertEndpointIdentitySnapshot:
        return _InertEndpointIdentitySnapshot()

    def __copy__(self) -> _InertEndpointIdentitySnapshot:
        return _InertEndpointIdentitySnapshot()

    def __reduce__(
        self,
    ) -> tuple[type[_InertEndpointIdentitySnapshot], tuple[object, ...]]:
        return _InertEndpointIdentitySnapshot, ()


def _normalized_identity_build_fields(
    *,
    endpoint_id: object,
    canonical_api_base: object,
    credential_descriptor_hash: object,
    endpoint_signature_hash: object,
    credential_descriptor_json: object,
    endpoint_signature_json: object,
) -> tuple[str, str, str, str, str, str]:
    normalized_endpoint_id = _digest(endpoint_id, "endpoint ID")
    normalized_credential_hash = _digest(
        credential_descriptor_hash,
        "credential descriptor hash",
    )
    normalized_signature_hash = _digest(
        endpoint_signature_hash,
        "endpoint signature hash",
    )
    admitted_fields = _admit_provider_directory_endpoint_components(
        canonical_api_base=canonical_api_base,
        credential_descriptor_json=credential_descriptor_json,
        endpoint_signature_json=endpoint_signature_json,
    )
    normalized_api_base = cast(str, admitted_fields["canonical_api_base"])
    credential_json = cast(
        str,
        admitted_fields["credential_descriptor_canonical_json"],
    )
    signature_json = cast(
        str,
        admitted_fields["endpoint_signature_canonical_json"],
    )
    _validate_endpoint_record_hashes(
        endpoint_id=normalized_endpoint_id,
        credential_descriptor_hash=normalized_credential_hash,
        endpoint_signature_hash=normalized_signature_hash,
        admitted_fields=admitted_fields,
    )
    return (
        normalized_endpoint_id,
        normalized_api_base,
        normalized_credential_hash,
        normalized_signature_hash,
        credential_json,
        signature_json,
    )


def build_provider_directory_endpoint_identity(
    *,
    endpoint_id: str,
    canonical_api_base: str,
    credential_descriptor_hash: str,
    endpoint_signature_hash: str,
    credential_descriptor_json: dict[str, Any],
    endpoint_signature_json: dict[str, Any],
) -> ProviderDirectoryEndpointIdentity:
    """Validate an existing endpoint row and return its nominal identity."""

    (
        normalized_endpoint_id,
        normalized_api_base,
        normalized_credential_hash,
        normalized_signature_hash,
        credential_json,
        signature_json,
    ) = _normalized_identity_build_fields(
        endpoint_id=endpoint_id,
        canonical_api_base=canonical_api_base,
        credential_descriptor_hash=credential_descriptor_hash,
        endpoint_signature_hash=endpoint_signature_hash,
        credential_descriptor_json=credential_descriptor_json,
        endpoint_signature_json=endpoint_signature_json,
    )
    identity_binding = _compute_endpoint_identity_binding(
        endpoint_id=normalized_endpoint_id,
        canonical_api_base=normalized_api_base,
        credential_descriptor_hash=normalized_credential_hash,
        endpoint_signature_hash=normalized_signature_hash,
        credential_descriptor_json=credential_json,
        endpoint_signature_json=signature_json,
    )
    endpoint_identity = object.__new__(ProviderDirectoryEndpointIdentity)
    identity_fields_by_name = {
        "_endpoint_id": normalized_endpoint_id,
        "_canonical_api_base": normalized_api_base,
        "_credential_descriptor_hash": normalized_credential_hash,
        "_endpoint_signature_hash": normalized_signature_hash,
        "_credential_descriptor_json": credential_json,
        "_endpoint_signature_json": signature_json,
        "_identity_binding": identity_binding,
        "_stable_hash": hash((ProviderDirectoryEndpointIdentity, identity_binding)),
    }
    for field_name, field_value in identity_fields_by_name.items():
        object.__setattr__(endpoint_identity, field_name, field_value)
    endpoint_identity.validate()
    return endpoint_identity
