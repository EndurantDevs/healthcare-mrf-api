# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Opaque live alias and request capabilities for scope registry lookup."""

from __future__ import annotations

import hashlib
import hmac
from typing import Any

from process.provider_directory_endpoint_identity import (
    ProviderDirectoryEndpointIdentity,
)
from process.provider_directory_scope_protection import (
    SCOPE_LOOKUP_CONTRACT_ID,
    SCOPE_LOOKUP_CONTRACT_VERSION,
    LogicalScopePseudonymError,
    _canonical_json,
    _DIGEST_PATTERN,
    _endpoint_public_payload,
    _InertScopeSnapshot,
    _LogicalScopePseudonymizer,
    _MAX_SCOPE_LOOKUP_ROTATIONS,
    _ProtectedValue,
    _read_protected,
    _strict_contract,
    _strict_key_values,
    _strict_key_version,
)


def _compute_alias_binding(
    *,
    owner_request: LogicalScopeLookupRequest,
    endpoint_fields_by_name: dict[str, str],
    key_contract_id: str,
    key_id: str,
    key_version: int,
    token: str,
) -> str:
    descriptor_fields_by_name = {
        "owner_request_identity": id(owner_request),
        "endpoint_identity": endpoint_fields_by_name,
        "key_contract_id": key_contract_id,
        "key_id": key_id,
        "key_version": key_version,
    }
    return hmac.new(
        bytes.fromhex(token),
        _canonical_json(descriptor_fields_by_name),
        hashlib.sha256,
    ).hexdigest()


class _LogicalScopeLookupAlias:
    """Exact live registry lookup capability; equality is object identity."""

    __slots__ = (
        "_capability_binding",
        "_endpoint_identity",
        "_key_contract_id",
        "_key_id",
        "_key_version",
        "_owner_request",
        "_token_material",
    )

    def __init__(
        self,
        *,
        owner_request: LogicalScopeLookupRequest,
        endpoint_identity: ProviderDirectoryEndpointIdentity,
        key_contract_id: str,
        key_id: str,
        key_version: int,
        token: str,
    ) -> None:
        if type(owner_request) is not LogicalScopeLookupRequest:
            raise LogicalScopePseudonymError("scope lookup alias owner is invalid")
        endpoint_fields_by_name = _endpoint_public_payload(endpoint_identity)
        normalized_contract = _strict_contract(
            key_contract_id,
            "scope lookup key contract",
        )
        normalized_key_id = _strict_contract(key_id, "scope lookup key ID")
        normalized_key_version = _strict_key_version(key_version)
        if type(token) is not str or not _DIGEST_PATTERN.fullmatch(token):
            raise LogicalScopePseudonymError("scope lookup HMAC token is invalid")
        binding = _compute_alias_binding(
            owner_request=owner_request,
            endpoint_fields_by_name=endpoint_fields_by_name,
            key_contract_id=normalized_contract,
            key_id=normalized_key_id,
            key_version=normalized_key_version,
            token=token,
        )
        object.__setattr__(self, "_endpoint_identity", endpoint_identity)
        object.__setattr__(self, "_key_contract_id", normalized_contract)
        object.__setattr__(self, "_key_id", normalized_key_id)
        object.__setattr__(self, "_key_version", normalized_key_version)
        object.__setattr__(self, "_owner_request", owner_request)
        object.__setattr__(self, "_token_material", _ProtectedValue(token))
        object.__setattr__(self, "_capability_binding", _ProtectedValue(binding))
        self.validate()

    def __setattr__(self, name: str, value: object) -> None:
        raise TypeError("scope lookup alias is immutable")

    def _validated_snapshot(
        self,
        *,
        expected_owner_request: LogicalScopeLookupRequest | None = None,
    ) -> tuple[ProviderDirectoryEndpointIdentity, str, str, int, str, str]:
        try:
            endpoint_identity_value = self._endpoint_identity
            key_contract_value = self._key_contract_id
            key_id_value = self._key_id
            key_version_value = self._key_version
            owner_request_value = self._owner_request
            token_material_value = self._token_material
            capability_binding_value = self._capability_binding
        except AttributeError:
            raise LogicalScopePseudonymError(
                "scope lookup alias state is invalid"
            ) from None
        if type(owner_request_value) is not LogicalScopeLookupRequest or (
            expected_owner_request is not None
            and owner_request_value is not expected_owner_request
        ):
            raise LogicalScopePseudonymError("scope lookup alias owner is invalid")
        endpoint_fields_by_name = _endpoint_public_payload(endpoint_identity_value)
        key_contract_id = _strict_contract(
            key_contract_value,
            "scope lookup key contract",
        )
        key_id = _strict_contract(key_id_value, "scope lookup key ID")
        key_version = _strict_key_version(key_version_value)
        try:
            token = _read_protected(token_material_value)
            capability_binding = _read_protected(capability_binding_value)
        except LogicalScopePseudonymError:
            raise LogicalScopePseudonymError("scope lookup alias is invalid") from None
        if type(token) is not str or not _DIGEST_PATTERN.fullmatch(token):
            raise LogicalScopePseudonymError("scope lookup HMAC token is invalid")
        expected_binding = _compute_alias_binding(
            owner_request=owner_request_value,
            endpoint_fields_by_name=endpoint_fields_by_name,
            key_contract_id=key_contract_id,
            key_id=key_id,
            key_version=key_version,
            token=token,
        )
        if type(capability_binding) is not str or not hmac.compare_digest(
            capability_binding,
            expected_binding,
        ):
            raise LogicalScopePseudonymError("scope lookup alias binding is invalid")
        return (
            endpoint_identity_value,
            key_contract_id,
            key_id,
            key_version,
            token,
            capability_binding,
        )

    def validate(self) -> None:
        """Reject copied, malformed, or valid-format-mutated alias state."""

        self._validated_snapshot()

    def version_identity(self) -> tuple[str, int]:
        """Return the safe key version descriptor used for canonical ordering."""

        _, _, key_id, key_version, _, _ = self._validated_snapshot()
        return key_id, key_version

    def public_payload(self) -> dict[str, object]:
        """Return only non-secret alias diagnostics."""

        _, _, key_id, key_version, _, _ = self._validated_snapshot()
        return {
            "key_id": key_id,
            "key_version": key_version,
        }

    def __repr__(self) -> str:
        return "<redacted-scope-lookup-alias>"

    def __deepcopy__(self, memo: object) -> _InertScopeSnapshot:
        return _InertScopeSnapshot("scope-lookup-alias")

    def __copy__(self) -> _InertScopeSnapshot:
        return _InertScopeSnapshot("scope-lookup-alias")

    def __reduce__(self) -> tuple[type[_InertScopeSnapshot], tuple[str, ...]]:
        return _InertScopeSnapshot, ("scope-lookup-alias",)


def _derive_alias(
    pseudonymizer: _LogicalScopePseudonymizer,
    *,
    owner_request: LogicalScopeLookupRequest,
    endpoint_identity: ProviderDirectoryEndpointIdentity,
    key_contract_id: str,
    normalized_key_values: tuple[str, ...],
) -> _LogicalScopeLookupAlias:
    if type(pseudonymizer) is not _LogicalScopePseudonymizer:
        raise LogicalScopePseudonymError("scope pseudonymizer set is invalid")
    key_id, key_version, secret = pseudonymizer._validated_material()
    endpoint_fields_by_name = _endpoint_public_payload(endpoint_identity)
    token = hmac.new(
        secret,
        _canonical_json(
            {
                "contract_id": SCOPE_LOOKUP_CONTRACT_ID,
                "contract_version": SCOPE_LOOKUP_CONTRACT_VERSION,
                "endpoint_identity": endpoint_fields_by_name,
                "key_contract_id": key_contract_id,
                "key_id": key_id,
                "key_version": key_version,
                "normalized_key_values": normalized_key_values,
            }
        ),
        hashlib.sha256,
    ).hexdigest()
    return _LogicalScopeLookupAlias(
        owner_request=owner_request,
        endpoint_identity=endpoint_identity,
        key_contract_id=key_contract_id,
        key_id=key_id,
        key_version=key_version,
        token=token,
    )


def _compute_request_binding(
    *,
    endpoint_fields_by_name: dict[str, str],
    key_contract_id: str,
    aliases: tuple[_LogicalScopeLookupAlias, ...],
) -> str:
    if (
        type(aliases) is not tuple
        or not 1 <= len(aliases) <= _MAX_SCOPE_LOOKUP_ROTATIONS
    ):
        raise LogicalScopePseudonymError("scope lookup aliases are invalid")
    alias_bindings: list[str] = []
    for alias in aliases:
        if type(alias) is not _LogicalScopeLookupAlias:
            raise LogicalScopePseudonymError("scope lookup alias is invalid")
        _, _, _, _, _, binding = alias._validated_snapshot()
        alias_bindings.append(binding)
    return _compute_request_binding_from_alias_bindings(
        endpoint_fields_by_name=endpoint_fields_by_name,
        key_contract_id=key_contract_id,
        alias_bindings=tuple(alias_bindings),
    )


def _compute_request_binding_from_alias_bindings(
    *,
    endpoint_fields_by_name: dict[str, str],
    key_contract_id: str,
    alias_bindings: tuple[str, ...],
) -> str:
    return hashlib.sha256(
        _canonical_json(
            {
                "endpoint_identity": endpoint_fields_by_name,
                "key_contract_id": key_contract_id,
                "alias_bindings": alias_bindings,
            }
        )
    ).hexdigest()


def _validated_request_alias_details(
    aliases: object,
    owner_request: LogicalScopeLookupRequest,
    endpoint_identity: ProviderDirectoryEndpointIdentity,
    key_contract_id: str,
) -> tuple[
    tuple[_LogicalScopeLookupAlias, ...],
    tuple[tuple[str, int], ...],
    tuple[str, ...],
]:
    if (
        type(aliases) is not tuple
        or not 1 <= len(aliases) <= _MAX_SCOPE_LOOKUP_ROTATIONS
    ):
        raise LogicalScopePseudonymError("scope lookup aliases are invalid")
    version_identity_list: list[tuple[str, int]] = []
    alias_binding_list: list[str] = []
    for alias in aliases:
        if type(alias) is not _LogicalScopeLookupAlias:
            raise LogicalScopePseudonymError("scope lookup alias is inconsistent")
        alias_snapshot = alias._validated_snapshot(
            expected_owner_request=owner_request,
        )
        alias_endpoint, alias_contract, key_id, key_version, _, binding = alias_snapshot
        if alias_endpoint is not endpoint_identity or alias_contract != key_contract_id:
            raise LogicalScopePseudonymError("scope lookup alias is inconsistent")
        version_identity_list.append((key_id, key_version))
        alias_binding_list.append(binding)
    version_identities = tuple(version_identity_list)
    if version_identities != tuple(sorted(set(version_identities))):
        raise LogicalScopePseudonymError("scope lookup aliases are not canonical")
    return aliases, version_identities, tuple(alias_binding_list)


class LogicalScopeLookupRequest:
    """Opaque live request with public redacted diagnostics only."""

    __slots__ = (
        "_aliases",
        "_endpoint_identity",
        "_key_contract_id",
        "_request_binding",
    )

    def __new__(cls, *args: object, **kwargs: object) -> LogicalScopeLookupRequest:
        raise TypeError("use build_scope_lookup_request")

    def __setattr__(self, name: str, value: object) -> None:
        raise TypeError("scope lookup request is immutable")

    def _validated_snapshot(
        self,
    ) -> tuple[
        ProviderDirectoryEndpointIdentity,
        dict[str, str],
        str,
        tuple[_LogicalScopeLookupAlias, ...],
        tuple[tuple[str, int], ...],
    ]:
        """Return one integrity-checked immutable snapshot of request slots."""

        try:
            endpoint_identity_value = self._endpoint_identity
            key_contract_value = self._key_contract_id
            aliases_value = self._aliases
            request_binding_value = self._request_binding
        except AttributeError:
            raise LogicalScopePseudonymError(
                "scope lookup request state is invalid"
            ) from None
        endpoint_fields_by_name = _endpoint_public_payload(endpoint_identity_value)
        key_contract_id = _strict_contract(
            key_contract_value,
            "scope lookup key contract",
        )
        aliases, version_identities, alias_bindings = _validated_request_alias_details(
            aliases_value,
            self,
            endpoint_identity_value,
            key_contract_id,
        )
        expected_binding = _compute_request_binding_from_alias_bindings(
            endpoint_fields_by_name=endpoint_fields_by_name,
            key_contract_id=key_contract_id,
            alias_bindings=alias_bindings,
        )
        try:
            request_binding = _read_protected(request_binding_value)
        except LogicalScopePseudonymError:
            raise LogicalScopePseudonymError("scope lookup request is invalid") from None
        if type(request_binding) is not str or not hmac.compare_digest(
            request_binding,
            expected_binding,
        ):
            raise LogicalScopePseudonymError("scope lookup request binding is invalid")
        return (
            endpoint_identity_value,
            endpoint_fields_by_name,
            key_contract_id,
            aliases,
            version_identities,
        )

    def validate(self) -> None:
        """Reject noncanonical aliases and any post-construction mutation."""

        self._validated_snapshot()

    def public_payload(self) -> dict[str, Any]:
        """Serialize endpoint and key-version diagnostics without aliases."""

        _, endpoint_fields, key_contract_id, aliases, _ = self._validated_snapshot()
        return {
            "endpoint_identity": endpoint_fields,
            "key_contract_id": key_contract_id,
            "key_versions": [alias.public_payload() for alias in aliases],
        }

    def __repr__(self) -> str:
        return f"LogicalScopeLookupRequest({self.public_payload()!r})"

    def __deepcopy__(self, memo: object) -> _InertScopeSnapshot:
        return _InertScopeSnapshot("scope-lookup-request")

    def __copy__(self) -> _InertScopeSnapshot:
        return _InertScopeSnapshot("scope-lookup-request")

    def __reduce__(self) -> tuple[type[_InertScopeSnapshot], tuple[str, ...]]:
        return _InertScopeSnapshot, ("scope-lookup-request",)


def build_scope_lookup_request(
    *,
    endpoint_identity: ProviderDirectoryEndpointIdentity,
    key_contract_id: str,
    normalized_key_values: tuple[str, ...],
    pseudonymizers: tuple[object, ...],
) -> LogicalScopeLookupRequest:
    """Build live aliases while retaining none of the transient exact key tuple."""

    endpoint_fields_by_name = _endpoint_public_payload(endpoint_identity)
    normalized_contract = _strict_contract(
        key_contract_id,
        "scope lookup key contract",
    )
    key_values = _strict_key_values(normalized_key_values)
    if (
        type(pseudonymizers) is not tuple
        or not 1 <= len(pseudonymizers) <= _MAX_SCOPE_LOOKUP_ROTATIONS
        or any(
            type(pseudonymizer) is not _LogicalScopePseudonymizer
            for pseudonymizer in pseudonymizers
        )
    ):
        raise LogicalScopePseudonymError("scope pseudonymizer set is invalid")
    request = object.__new__(LogicalScopeLookupRequest)
    aliases = tuple(
        sorted(
            (
                _derive_alias(
                    pseudonymizer,
                    owner_request=request,
                    endpoint_identity=endpoint_identity,
                    key_contract_id=normalized_contract,
                    normalized_key_values=key_values,
                )
                for pseudonymizer in pseudonymizers
            ),
            key=lambda alias: alias.version_identity(),
        )
    )
    request_fields_by_name = {
        "_endpoint_identity": endpoint_identity,
        "_key_contract_id": normalized_contract,
        "_aliases": aliases,
        "_request_binding": _ProtectedValue(
            _compute_request_binding(
                endpoint_fields_by_name=endpoint_fields_by_name,
                key_contract_id=normalized_contract,
                aliases=aliases,
            )
        ),
    }
    for field_name, field_value in request_fields_by_name.items():
        object.__setattr__(request, field_name, field_value)
    request.validate()
    return request


def _registry_aliases_for_adapter(
    request: LogicalScopeLookupRequest,
) -> tuple[_LogicalScopeLookupAlias, ...]:
    """Return exact live aliases only to the private persistence adapter."""

    if type(request) is not LogicalScopeLookupRequest:
        raise LogicalScopePseudonymError("scope registry request is invalid")
    _, _, _, aliases, _ = request._validated_snapshot()
    return aliases


def _registry_token_for_adapter(
    request: LogicalScopeLookupRequest,
    alias: object,
) -> str:
    """Reveal one token only for an exact alias owned by the live request."""

    aliases = _registry_aliases_for_adapter(request)
    if type(alias) is not _LogicalScopeLookupAlias or not any(
        alias is expected_alias for expected_alias in aliases
    ):
        raise LogicalScopePseudonymError("scope registry alias is not live")
    _, _, _, _, token, _ = alias._validated_snapshot(
        expected_owner_request=request,
    )
    return token
