# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import copy
import hashlib
import json
import pickle

import pytest

import process.provider_directory_scope_alias as alias_module
import process.provider_directory_scope_protection as protection_module
import process.provider_directory_scope_pseudonym as scope_module
from process.provider_directory_logical_scope import (
    LogicalScopePseudonymError,
    LogicalScopeRegistryDecision,
    build_provider_directory_endpoint_identity,
    build_scope_lookup_request,
    build_scope_pseudonymizer,
)


def _identity_hash(value: object) -> str:
    rendered_value = json.dumps(value, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(rendered_value.encode("utf-8")).hexdigest()


def _endpoint():
    canonical_api_base = "https://fhir.example.test/provider-directory"
    credential_descriptor_by_field = {"auth": "none"}
    endpoint_signature_by_resource = {"Practitioner": "Practitioner"}
    return build_provider_directory_endpoint_identity(
        endpoint_id=_identity_hash(
            {
                "canonical_api_base": canonical_api_base,
                "credential_descriptor": credential_descriptor_by_field,
                "endpoint_signature": endpoint_signature_by_resource,
            }
        ),
        canonical_api_base=canonical_api_base,
        credential_descriptor_hash=_identity_hash(credential_descriptor_by_field),
        endpoint_signature_hash=_identity_hash(endpoint_signature_by_resource),
        credential_descriptor_json=credential_descriptor_by_field,
        endpoint_signature_json=endpoint_signature_by_resource,
    )


def _key():
    return build_scope_pseudonymizer(
        key_id="provider-directory.primary",
        key_version=1,
        secret=b"a" * 32,
    )


def _request(**changes: object):
    request_fields_by_name = {
        "endpoint_identity": _endpoint(),
        "key_contract_id": "example.plan-key.v1",
        "normalized_key_values": ("PLAN-ID", "12345"),
        "pseudonymizers": (_key(),),
    }
    request_fields_by_name.update(changes)
    return build_scope_lookup_request(**request_fields_by_name)


def _resolve_create():
    return scope_module._resolve_scope_lookup_aliases_for_adapter(_request(), {})


def test_request_validation_rejects_valid_contract_and_alias_tuple_mutation():
    lookup_request = _request()
    object.__setattr__(
        lookup_request,
        "_key_contract_id",
        "example.other-plan-key.v1",
    )
    with pytest.raises(LogicalScopePseudonymError, match="alias is inconsistent"):
        lookup_request.validate()

    missing_aliases_request = _request()
    object.__setattr__(missing_aliases_request, "_aliases", ())
    with pytest.raises(LogicalScopePseudonymError, match="aliases are invalid"):
        missing_aliases_request.validate()

    malformed_aliases_request = _request()
    object.__setattr__(malformed_aliases_request, "_aliases", (object(),))
    with pytest.raises(LogicalScopePseudonymError, match="alias is inconsistent"):
        malformed_aliases_request.validate()

    invalid_binding_request = _request()
    object.__setattr__(invalid_binding_request, "_request_binding", object())
    with pytest.raises(LogicalScopePseudonymError, match="request is invalid"):
        invalid_binding_request.validate()

    changed_binding_request = _request()
    object.__setattr__(
        changed_binding_request._request_binding,
        "_ProtectedValue__value",
        "f" * 64,
    )
    with pytest.raises(LogicalScopePseudonymError, match="request binding"):
        changed_binding_request.validate()

    with pytest.raises(TypeError, match="request is immutable"):
        _request()._key_contract_id = "example.other-plan-key.v1"


def test_request_rejects_coherent_state_transplant_and_owner_rebinding():
    first_request = _request(normalized_key_values=("PLAN-A",))
    second_request = _request(normalized_key_values=("PLAN-B",))
    second_alias = alias_module._registry_aliases_for_adapter(second_request)[0]

    for field_name in first_request.__slots__:
        object.__setattr__(
            first_request,
            field_name,
            object.__getattribute__(second_request, field_name),
        )

    with pytest.raises(LogicalScopePseudonymError, match="alias owner"):
        first_request.validate()
    with pytest.raises(LogicalScopePseudonymError, match="alias owner"):
        alias_module._registry_aliases_for_adapter(first_request)
    with pytest.raises(LogicalScopePseudonymError, match="alias owner"):
        alias_module._registry_token_for_adapter(first_request, second_alias)

    object.__setattr__(second_alias, "_owner_request", first_request)
    with pytest.raises(LogicalScopePseudonymError, match="alias binding"):
        second_alias.validate()
    with pytest.raises(LogicalScopePseudonymError, match="alias binding"):
        alias_module._registry_token_for_adapter(first_request, second_alias)


def test_adapter_rechecks_owner_after_request_validation(monkeypatch):
    token_request = _request(normalized_key_values=("TOKEN-A",))
    token_alias = alias_module._registry_aliases_for_adapter(token_request)[0]
    other_token_request = _request(normalized_key_values=("TOKEN-B",))
    other_token_alias = alias_module._registry_aliases_for_adapter(
        other_token_request,
    )[0]
    original_token_alias_lookup = alias_module._registry_aliases_for_adapter

    def replace_token_alias_after_validation(request):
        aliases = original_token_alias_lookup(request)
        for field_name in token_alias.__slots__:
            object.__setattr__(
                token_alias,
                field_name,
                object.__getattribute__(other_token_alias, field_name),
            )
        return aliases

    monkeypatch.setattr(
        alias_module,
        "_registry_aliases_for_adapter",
        replace_token_alias_after_validation,
    )
    with pytest.raises(LogicalScopePseudonymError, match="alias owner"):
        alias_module._registry_token_for_adapter(token_request, token_alias)

    resolve_request = _request(normalized_key_values=("RESOLVE-A",))
    resolve_alias = alias_module._registry_aliases_for_adapter(resolve_request)[0]
    other_resolve_request = _request(normalized_key_values=("RESOLVE-B",))
    other_resolve_alias = alias_module._registry_aliases_for_adapter(
        other_resolve_request,
    )[0]
    original_resolve_alias_lookup = scope_module._registry_aliases_for_adapter

    def replace_resolve_alias_after_validation(request):
        aliases = original_resolve_alias_lookup(request)
        for field_name in resolve_alias.__slots__:
            object.__setattr__(
                resolve_alias,
                field_name,
                object.__getattribute__(other_resolve_alias, field_name),
            )
        return aliases

    monkeypatch.setattr(
        scope_module,
        "_registry_aliases_for_adapter",
        replace_resolve_alias_after_validation,
    )
    with pytest.raises(LogicalScopePseudonymError, match="alias owner"):
        scope_module._resolve_scope_lookup_aliases_for_adapter(
            resolve_request,
            {resolve_alias: "pdscope_" + "a" * 32},
        )


def test_decision_copies_are_inert_and_valid_mutation_is_detected():
    decision = _resolve_create()
    copied_decision = copy.deepcopy(decision)
    shallow_decision = copy.copy(decision)
    unpickled_decision = pickle.loads(pickle.dumps(decision))

    assert not isinstance(copied_decision, LogicalScopeRegistryDecision)
    assert not isinstance(shallow_decision, LogicalScopeRegistryDecision)
    assert not isinstance(unpickled_decision, LogicalScopeRegistryDecision)

    object.__setattr__(decision, "_matched_alias_count", 1)
    with pytest.raises(LogicalScopePseudonymError, match="decision"):
        decision.validate()

    immutable_decision = _resolve_create()
    with pytest.raises(TypeError, match="decision is immutable"):
        immutable_decision._matched_alias_count = 1

    invalid_count_decision = _resolve_create()
    object.__setattr__(invalid_count_decision, "_matched_alias_count", True)
    with pytest.raises(LogicalScopePseudonymError, match="counts"):
        invalid_count_decision.validate()

    invalid_binding_decision = _resolve_create()
    object.__setattr__(invalid_binding_decision, "_decision_binding", "f" * 64)
    with pytest.raises(LogicalScopePseudonymError, match="decision binding"):
        invalid_binding_decision.validate()

    invalid_status_decision = _resolve_create()
    object.__setattr__(invalid_status_decision, "_status", "unknown")
    with pytest.raises(LogicalScopePseudonymError, match="decision is invalid"):
        invalid_status_decision.validate()


@pytest.mark.parametrize(
    "public_read",
    (
        lambda decision: decision.status,
        lambda decision: decision.scope_id,
        lambda decision: decision.matched_alias_count,
        lambda decision: decision.distinct_scope_count,
    ),
)
def test_every_decision_property_validates_sealed_private_state(public_read):
    decision = _resolve_create()
    object.__setattr__(decision, "_status", "matched")

    with pytest.raises(LogicalScopePseudonymError, match="decision"):
        public_read(decision)


def test_decision_rejects_counts_beyond_bounded_rotation_window():
    decision = _resolve_create()
    object.__setattr__(decision, "_matched_alias_count", 9)
    object.__setattr__(decision, "_distinct_scope_count", 9)

    with pytest.raises(LogicalScopePseudonymError, match="counts"):
        decision.matched_alias_count


def test_protected_material_rejects_ordinary_mutation():
    pseudonymizer = _key()
    with pytest.raises(TypeError, match="protected material"):
        pseudonymizer._secret_material.any_field = b"changed"

    protected_snapshot = copy.deepcopy(pseudonymizer._secret_material)
    shallow_snapshot = copy.copy(pseudonymizer._secret_material)
    with pytest.raises(TypeError, match="snapshot"):
        protected_snapshot.any_field = "changed"
    assert "redacted" in repr(shallow_snapshot)
    assert copy.copy(protected_snapshot) is protected_snapshot

    assert repr(pseudonymizer._secret_material) == "<redacted>"
    assert pickle.loads(pickle.dumps(pseudonymizer._secret_material))._label == (
        "scope-capability"
    )
    with pytest.raises(LogicalScopePseudonymError, match="protected material"):
        protection_module._read_protected(object())


def test_pseudonymizer_rejects_container_short_secret_and_ordinary_mutation():
    immutable_pseudonymizer = _key()
    with pytest.raises(TypeError, match="pseudonymizer is immutable"):
        immutable_pseudonymizer._key_version = 2
    assert immutable_pseudonymizer.public_payload() == {
        "key_id": "provider-directory.primary",
        "key_version": 1,
    }

    wrong_container_pseudonymizer = _key()
    object.__setattr__(wrong_container_pseudonymizer, "_secret_material", object())
    with pytest.raises(LogicalScopePseudonymError, match="secret is invalid"):
        wrong_container_pseudonymizer.validate()

    short_secret_pseudonymizer = _key()
    object.__setattr__(
        short_secret_pseudonymizer._secret_material,
        "_ProtectedValue__value",
        b"short",
    )
    with pytest.raises(LogicalScopePseudonymError, match="secret is invalid"):
        short_secret_pseudonymizer.validate()

    leaked_key_id = _key()
    object.__setattr__(
        leaked_key_id,
        "_key_id",
        "RAW-SECRET-MUST-NOT-BE-LOGGED",
    )
    with pytest.raises(LogicalScopePseudonymError) as error_info:
        repr(leaked_key_id)
    assert "RAW-SECRET-MUST-NOT-BE-LOGGED" not in str(error_info.value)


def test_mutated_endpoint_identity_fails_closed_at_scope_boundary():
    endpoint_identity = _endpoint()
    object.__setattr__(endpoint_identity, "_endpoint_id", "f" * 64)

    with pytest.raises(LogicalScopePseudonymError, match="endpoint identity"):
        _request(endpoint_identity=endpoint_identity)
