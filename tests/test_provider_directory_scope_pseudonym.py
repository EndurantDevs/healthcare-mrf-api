# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from collections.abc import Iterator, Mapping
import copy
from dataclasses import asdict
import hashlib
import json
import pickle

import pytest

import process.provider_directory_logical_scope as public_scope
import process.provider_directory_scope_alias as alias_module
import process.provider_directory_scope_pseudonym as scope_module
import process.provider_directory_scope_protection as protection_module
from process.provider_directory_logical_scope import (
    LOOKUP_DECISION_AMBIGUOUS,
    LOOKUP_DECISION_CREATE,
    LOOKUP_DECISION_MATCHED,
    LogicalScopeLookupRequest,
    LogicalScopePseudonymError,
    LogicalScopeRegistryDecision,
    ProviderDirectoryEndpointIdentity,
    build_provider_directory_endpoint_identity,
    build_scope_lookup_request,
    build_scope_pseudonymizer,
)


KEY_CONTRACT_ID = "example.plan-key.v1"
RAW_KEY_VALUES = ("PLAN-ID", "12345")
SCOPE_A = "pdscope_" + "a" * 32
SCOPE_B = "pdscope_" + "b" * 32


class _DuplicateAliasMapping(Mapping[object, str]):
    """Adversarial Mapping whose items violate unique-key semantics."""

    def __init__(self, alias: object) -> None:
        self._alias = alias

    def __getitem__(self, key: object) -> str:
        if key is not self._alias:
            raise KeyError(key)
        return SCOPE_A

    def __iter__(self) -> Iterator[object]:
        return iter((self._alias,))

    def __len__(self) -> int:
        return 1

    def items(self):
        return ((self._alias, SCOPE_A), (self._alias, SCOPE_A))


def _stable_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _identity_hash(value: object) -> str:
    return hashlib.sha256(_stable_json(value).encode("utf-8")).hexdigest()


def _endpoint(base: str = "https://fhir.example.test/provider-directory"):
    credential_descriptor_by_field = {"auth": "none"}
    endpoint_signature_by_resource = {
        "Organization": "Organization",
        "Practitioner": "Practitioner",
    }
    return build_provider_directory_endpoint_identity(
        endpoint_id=_identity_hash(
            {
                "canonical_api_base": base,
                "credential_descriptor": credential_descriptor_by_field,
                "endpoint_signature": endpoint_signature_by_resource,
            }
        ),
        canonical_api_base=base,
        credential_descriptor_hash=_identity_hash(credential_descriptor_by_field),
        endpoint_signature_hash=_identity_hash(endpoint_signature_by_resource),
        credential_descriptor_json=credential_descriptor_by_field,
        endpoint_signature_json=endpoint_signature_by_resource,
    )


def _key(version: int = 1, secret_byte: bytes = b"a"):
    return build_scope_pseudonymizer(
        key_id="provider-directory.primary",
        key_version=version,
        secret=secret_byte * 32,
    )


def _request(*pseudonymizers: object, **changes: object):
    request_fields_by_name = {
        "endpoint_identity": _endpoint(),
        "key_contract_id": KEY_CONTRACT_ID,
        "normalized_key_values": RAW_KEY_VALUES,
        "pseudonymizers": pseudonymizers or (_key(),),
    }
    request_fields_by_name.update(changes)
    return build_scope_lookup_request(**request_fields_by_name)


def _aliases(request: LogicalScopeLookupRequest):
    return alias_module._registry_aliases_for_adapter(request)


def _token(request: LogicalScopeLookupRequest, alias: object) -> str:
    return alias_module._registry_token_for_adapter(request, alias)


def _resolve(request: LogicalScopeLookupRequest, mapping: object):
    return scope_module._resolve_scope_lookup_aliases_for_adapter(request, mapping)


def test_public_facade_has_no_issuer_alias_token_scope_or_source_capabilities():
    forbidden_public_names = {
        "FilePartitionScope",
        "LogicalPlanScope",
        "LogicalScopeCandidate",
        "ProviderDirectoryDatasetBinding",
        "bind_logical_plan_scope",
        "build_file_partition_scope",
        "catalog_derived_candidate",
        "declared_scope_candidate",
        "new_registry_scope_id",
        "resolve_logical_plan_scope",
        "resolve_scope_lookup_aliases",
        "validate_registry_scope_id",
    }

    assert forbidden_public_names.isdisjoint(public_scope.__all__)
    assert all(not hasattr(public_scope, name) for name in forbidden_public_names)
    assert not hasattr(scope_module, "LogicalScopeLookupAlias")
    assert not hasattr(scope_module, "LogicalScopePseudonymizer")


def test_private_registry_issuance_uses_noninjectable_sixteen_random_bytes(monkeypatch):
    requested_sizes: list[int] = []

    def deterministic_token_bytes(byte_count: int) -> bytes:
        requested_sizes.append(byte_count)
        return bytes(range(byte_count))

    monkeypatch.setattr(
        protection_module.secrets,
        "token_bytes",
        deterministic_token_bytes,
    )

    scope_id = protection_module._new_registry_scope_id()

    assert requested_sizes == [16]
    assert scope_id == "pdscope_000102030405060708090a0b0c0d0e0f"
    with pytest.raises(TypeError):
        protection_module._new_registry_scope_id(lambda size: b"x" * size)


@pytest.mark.parametrize(
    "invalid_entropy",
    (b"short", b"x" * 17, bytearray(b"x" * 16), None),
)
def test_private_registry_issuance_rejects_invalid_system_entropy(
    monkeypatch,
    invalid_entropy,
):
    monkeypatch.setattr(
        protection_module.secrets,
        "token_bytes",
        lambda size: invalid_entropy,
    )

    with pytest.raises(LogicalScopePseudonymError, match="entropy"):
        protection_module._new_registry_scope_id()


def test_hmac_request_is_stable_domain_separated_and_publicly_redacted():
    key = _key()
    request = _request(key)
    repeated = _request(key)
    alias = _aliases(request)[0]
    token = _token(request, alias)
    repeated_token = _token(repeated, _aliases(repeated)[0])
    public_payload = request.public_payload()
    rendered_payload = json.dumps(public_payload, sort_keys=True)

    assert token == repeated_token
    assert token not in repr(request)
    assert token not in rendered_payload
    assert all(raw_value not in repr(request) for raw_value in RAW_KEY_VALUES)
    assert all(raw_value not in rendered_payload for raw_value in RAW_KEY_VALUES)
    assert (b"a" * 32).hex() not in repr(key)
    assert public_payload["key_versions"] == [
        {"key_id": "provider-directory.primary", "key_version": 1}
    ]

    alternate_secret_request = _request(_key(secret_byte=b"b"))
    alternate_tokens = {
        token,
        _token(alternate_secret_request, _aliases(alternate_secret_request)[0]),
    }
    alternate_request = _request(_key(version=2))
    alternate_tokens.add(_token(alternate_request, _aliases(alternate_request)[0]))
    alternate_endpoint_request = _request(
        _key(),
        endpoint_identity=_endpoint("https://other.example.test/provider-directory"),
    )
    alternate_tokens.add(
        _token(alternate_endpoint_request, _aliases(alternate_endpoint_request)[0])
    )
    alternate_contract_request = _request(
        _key(),
        key_contract_id="example.other-plan-key.v1",
    )
    alternate_tokens.add(
        _token(alternate_contract_request, _aliases(alternate_contract_request)[0])
    )
    alternate_values_request = _request(
        _key(),
        normalized_key_values=("PLAN-ID", "54321"),
    )
    alternate_tokens.add(
        _token(alternate_values_request, _aliases(alternate_values_request)[0])
    )
    assert len(alternate_tokens) == 6


def test_raw_or_wrong_domain_endpoint_digest_cannot_build_a_request():
    with pytest.raises(LogicalScopePseudonymError, match="endpoint identity"):
        _request(endpoint_identity="0" * 64)
    with pytest.raises(LogicalScopePseudonymError, match="endpoint identity"):
        _request(endpoint_identity="serving-endpoint-42")

    forged_identity = object.__new__(ProviderDirectoryEndpointIdentity)
    with pytest.raises(LogicalScopePseudonymError, match="endpoint identity"):
        _request(endpoint_identity=forged_identity)


def test_alias_request_and_key_copies_are_inert_noncapabilities():
    pseudonymizer = _key()
    lookup_request = _request(pseudonymizer)
    live_alias = _aliases(lookup_request)[0]
    hmac_token = _token(lookup_request, live_alias)
    scope_ids_by_alias = {live_alias: SCOPE_A}

    copied_alias = copy.deepcopy(live_alias)
    shallow_alias = copy.copy(live_alias)
    unpickled_alias = pickle.loads(pickle.dumps(live_alias))
    copied_request = copy.deepcopy(lookup_request)
    shallow_request = copy.copy(lookup_request)
    copied_pseudonymizer = copy.deepcopy(pseudonymizer)
    shallow_pseudonymizer = copy.copy(pseudonymizer)

    assert copied_alias != live_alias
    assert unpickled_alias != live_alias
    assert shallow_alias != live_alias
    assert copied_alias not in scope_ids_by_alias
    assert unpickled_alias not in scope_ids_by_alias
    assert not isinstance(copied_request, LogicalScopeLookupRequest)
    assert not isinstance(shallow_request, LogicalScopeLookupRequest)
    assert "redacted" in repr(copied_pseudonymizer)
    assert "redacted" in repr(shallow_pseudonymizer)
    assert copy.deepcopy(copied_alias) is copied_alias
    with pytest.raises(LogicalScopePseudonymError, match="invalid alias"):
        _resolve(lookup_request, {copied_alias: SCOPE_A})
    with pytest.raises(LogicalScopePseudonymError, match="not live"):
        _token(lookup_request, copied_alias)
    with pytest.raises(TypeError):
        asdict(live_alias)
    with pytest.raises(TypeError):
        asdict(lookup_request)
    with pytest.raises(TypeError):
        asdict(pseudonymizer)

    serialized_values = (
        pickle.dumps(live_alias),
        pickle.dumps(lookup_request),
        pickle.dumps(pseudonymizer),
    )
    assert all(
        hmac_token.encode("ascii") not in serialized_value
        for serialized_value in serialized_values
    )
    assert all(b"a" * 32 not in serialized_value for serialized_value in serialized_values)
    assert repr(live_alias) == "<redacted-scope-lookup-alias>"


def test_private_alias_constructor_and_ordinary_mutation_validate_inputs():
    endpoint_identity = _endpoint()
    with pytest.raises(LogicalScopePseudonymError, match="alias owner"):
        alias_module._LogicalScopeLookupAlias(
            owner_request=object(),
            endpoint_identity=endpoint_identity,
            key_contract_id=KEY_CONTRACT_ID,
            key_id="valid.key",
            key_version=1,
            token="a" * 64,
        )
    with pytest.raises(LogicalScopePseudonymError, match="HMAC token"):
        alias_module._LogicalScopeLookupAlias(
            owner_request=object.__new__(LogicalScopeLookupRequest),
            endpoint_identity=endpoint_identity,
            key_contract_id=KEY_CONTRACT_ID,
            key_id="valid.key",
            key_version=1,
            token="bad",
        )

    lookup_request = _request()
    live_alias = _aliases(lookup_request)[0]
    with pytest.raises(TypeError, match="alias is immutable"):
        live_alias._key_version = 2

    object.__setattr__(live_alias, "_token_material", "not-protected")
    with pytest.raises(LogicalScopePseudonymError, match="alias is invalid"):
        live_alias.validate()


def test_valid_format_alias_mutation_fails_without_destabilizing_dict_hash():
    request = _request()
    alias = _aliases(request)[0]
    alias_hash = hash(alias)
    scope_ids_by_alias = {alias: SCOPE_A}
    object.__setattr__(alias, "_key_version", 2)

    assert hash(alias) == alias_hash
    assert alias in scope_ids_by_alias
    with pytest.raises(LogicalScopePseudonymError, match="alias binding"):
        alias.validate()
    with pytest.raises(LogicalScopePseudonymError, match="alias binding"):
        _resolve(request, scope_ids_by_alias)


def test_valid_format_token_and_pseudonymizer_mutation_fail_closed():
    request = _request()
    alias = _aliases(request)[0]
    object.__setattr__(
        alias._token_material,
        "_ProtectedValue__value",
        "f" * 64,
    )
    with pytest.raises(LogicalScopePseudonymError, match="alias binding"):
        alias.validate()

    changed_descriptor_key = _key()
    object.__setattr__(changed_descriptor_key, "_key_version", 2)
    with pytest.raises(LogicalScopePseudonymError, match="pseudonymizer binding"):
        changed_descriptor_key.validate()
    with pytest.raises(LogicalScopePseudonymError, match="pseudonymizer binding"):
        _request(changed_descriptor_key)

    changed_secret_key = _key()
    object.__setattr__(
        changed_secret_key._secret_material,
        "_ProtectedValue__value",
        b"b" * 32,
    )
    with pytest.raises(LogicalScopePseudonymError, match="pseudonymizer binding"):
        changed_secret_key.validate()


def test_private_token_access_requires_exact_alias_from_exact_live_request():
    request = _request()
    exact_alias = _aliases(request)[0]
    other_request = _request()
    equivalent_but_other_alias = _aliases(other_request)[0]

    assert _token(request, exact_alias) == _token(
        other_request,
        equivalent_but_other_alias,
    )
    with pytest.raises(LogicalScopePseudonymError, match="not live"):
        _token(request, equivalent_but_other_alias)
    with pytest.raises(LogicalScopePseudonymError, match="not live"):
        _token(request, object())
    with pytest.raises(LogicalScopePseudonymError, match="request"):
        alias_module._registry_aliases_for_adapter(object())


def test_rotation_reuses_one_scope_and_conflicts_fail_closed():
    request = _request(_key(), _key(version=2, secret_byte=b"b"))
    old_alias, new_alias = _aliases(request)

    create = _resolve(request, {})
    matched_old = _resolve(request, {old_alias: SCOPE_A})
    matched_both = _resolve(request, {old_alias: SCOPE_A, new_alias: SCOPE_A})
    ambiguous = _resolve(request, {old_alias: SCOPE_A, new_alias: SCOPE_B})

    assert create.public_payload() == {
        "status": LOOKUP_DECISION_CREATE,
        "scope_id": None,
        "matched_alias_count": 0,
        "distinct_scope_count": 0,
    }
    assert matched_old.status == LOOKUP_DECISION_MATCHED
    assert matched_old.scope_id == SCOPE_A
    assert matched_old.matched_alias_count == 1
    assert matched_old.distinct_scope_count == 1
    assert matched_both.matched_alias_count == 2
    assert ambiguous.status == LOOKUP_DECISION_AMBIGUOUS
    assert ambiguous.scope_id is None
    assert ambiguous.distinct_scope_count == 2
    assert "scope_id" in repr(ambiguous)


def test_resolver_rejects_every_nonexact_or_malformed_registry_result():
    request = _request()
    alias = _aliases(request)[0]
    other_alias = _aliases(_request())[0]

    with pytest.raises(LogicalScopePseudonymError, match="lookup input"):
        _resolve(request, [])
    with pytest.raises(LogicalScopePseudonymError, match="invalid alias"):
        _resolve(request, {object(): SCOPE_A})
    with pytest.raises(LogicalScopePseudonymError, match="unknown alias"):
        _resolve(request, {other_alias: SCOPE_A})
    with pytest.raises(LogicalScopePseudonymError, match="registry scope ID"):
        _resolve(request, {alias: "bad"})
    with pytest.raises(LogicalScopePseudonymError, match="lookup input"):
        _resolve(request, _DuplicateAliasMapping(alias))

@pytest.mark.parametrize(
    "invalid_call",
    (
        lambda: build_scope_pseudonymizer(
            key_id="bad key",
            key_version=1,
            secret=b"a" * 32,
        ),
        lambda: build_scope_pseudonymizer(
            key_id="valid.key",
            key_version=True,
            secret=b"a" * 32,
        ),
        lambda: build_scope_pseudonymizer(
            key_id="valid.key",
            key_version=0,
            secret=b"a" * 32,
        ),
        lambda: build_scope_pseudonymizer(
            key_id="valid.key",
            key_version=2**31,
            secret=b"a" * 32,
        ),
        lambda: build_scope_pseudonymizer(
            key_id="valid.key",
            key_version=1,
            secret=b"short",
        ),
        lambda: _request(key_contract_id="bad contract"),
        lambda: _request(normalized_key_values=()),
        lambda: _request(normalized_key_values="raw-string"),
        lambda: _request(normalized_key_values=(" leading",)),
        lambda: _request(pseudonymizers=()),
        lambda: _request(pseudonymizers=(object(),)),
        lambda: _request(pseudonymizers="not-a-keyring"),
    ),
)
def test_request_construction_rejects_malformed_inputs(invalid_call):
    with pytest.raises(LogicalScopePseudonymError):
        invalid_call()


def test_duplicate_key_versions_are_rejected_as_noncanonical():
    with pytest.raises(LogicalScopePseudonymError, match="not canonical"):
        _request(_key(), _key(secret_byte=b"b"))
