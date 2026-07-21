# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from collections.abc import Mapping, Sequence
import hashlib
import importlib
import json

import pytest

import process.provider_directory_scope_alias as alias_module
import process.provider_directory_scope_pseudonym as scope_module
import process.provider_directory_scope_protection as protection_module
from process.provider_directory_logical_scope import (
    LOOKUP_DECISION_MATCHED,
    LogicalScopeLookupRequest,
    LogicalScopePseudonymError,
    LogicalScopeRegistryDecision,
    build_provider_directory_endpoint_identity,
    build_scope_lookup_request,
    build_scope_pseudonymizer,
)


KEY_CONTRACT_ID = "example.plan-key.v1"
RAW_KEY_VALUES = ("PLAN-ID", "12345")
SCOPE_A = "pdscope_" + "a" * 32


class _ExplosiveSequence(Sequence[object]):
    def __init__(self) -> None:
        self.callback_count = 0

    def __getitem__(self, index):
        self.callback_count += 1
        raise RuntimeError("RAW-PLAN-123")

    def __len__(self) -> int:
        self.callback_count += 1
        raise RuntimeError("RAW-PLAN-123")


class _ExplosiveMapping(Mapping[object, str]):
    def __init__(self) -> None:
        self.callback_count = 0

    def __getitem__(self, key: object) -> str:
        self.callback_count += 1
        raise RuntimeError("HMAC-TOKEN-PLACEHOLDER")

    def __iter__(self):
        self.callback_count += 1
        raise RuntimeError("HMAC-TOKEN-PLACEHOLDER")

    def __len__(self) -> int:
        self.callback_count += 1
        raise RuntimeError("HMAC-TOKEN-PLACEHOLDER")


def _identity_hash(json_value: object) -> str:
    canonical_json = json.dumps(json_value, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()


def _endpoint_identity():
    api_base = "https://fhir.example.test/provider-directory"
    credential_descriptor_by_field = {"auth": "none"}
    endpoint_signature_by_resource = {"Practitioner": "Practitioner"}
    endpoint_id = _identity_hash(
        {
            "canonical_api_base": api_base,
            "credential_descriptor": credential_descriptor_by_field,
            "endpoint_signature": endpoint_signature_by_resource,
        }
    )
    return build_provider_directory_endpoint_identity(
        endpoint_id=endpoint_id,
        canonical_api_base=api_base,
        credential_descriptor_hash=_identity_hash(credential_descriptor_by_field),
        endpoint_signature_hash=_identity_hash(endpoint_signature_by_resource),
        credential_descriptor_json=credential_descriptor_by_field,
        endpoint_signature_json=endpoint_signature_by_resource,
    )


def _pseudonymizer(version: int = 1):
    return build_scope_pseudonymizer(
        key_id="provider-directory.primary",
        key_version=version,
        secret=b"a" * 32,
    )


def _request(**changes: object):
    request_by_field = {
        "endpoint_identity": _endpoint_identity(),
        "key_contract_id": KEY_CONTRACT_ID,
        "normalized_key_values": RAW_KEY_VALUES,
        "pseudonymizers": (_pseudonymizer(),),
    }
    request_by_field.update(changes)
    return build_scope_lookup_request(**request_by_field)


def _aliases(request: LogicalScopeLookupRequest):
    return alias_module._registry_aliases_for_adapter(request)


def _resolve(request: LogicalScopeLookupRequest, results_by_alias: object):
    return scope_module._resolve_scope_lookup_aliases_for_adapter(
        request,
        results_by_alias,
    )


def test_alias_and_request_reject_malformed_or_forged_state():
    lookup_request = _request()
    live_alias = _aliases(lookup_request)[0]
    object.__setattr__(live_alias._token_material, "_ProtectedValue__value", "bad")
    with pytest.raises(LogicalScopePseudonymError, match="HMAC token"):
        live_alias.validate()

    byte_binding_alias = _aliases(_request())[0]
    object.__setattr__(
        byte_binding_alias._capability_binding,
        "_ProtectedValue__value",
        b"not-text",
    )
    with pytest.raises(LogicalScopePseudonymError, match="alias binding"):
        byte_binding_alias.validate()
    with pytest.raises(LogicalScopePseudonymError, match="alias binding"):
        alias_module._compute_request_binding(
            endpoint_fields_by_name=_endpoint_identity().public_payload(),
            key_contract_id=KEY_CONTRACT_ID,
            aliases=(byte_binding_alias,),
        )

    with pytest.raises(LogicalScopePseudonymError, match="pseudonymizer set"):
        alias_module._derive_alias(
            object(),
            owner_request=object.__new__(LogicalScopeLookupRequest),
            endpoint_identity=_endpoint_identity(),
            key_contract_id=KEY_CONTRACT_ID,
            normalized_key_values=RAW_KEY_VALUES,
        )
    with pytest.raises(LogicalScopePseudonymError, match="aliases are invalid"):
        alias_module._compute_request_binding(
            endpoint_fields_by_name=_endpoint_identity().public_payload(),
            key_contract_id=KEY_CONTRACT_ID,
            aliases=[],
        )
    with pytest.raises(LogicalScopePseudonymError, match="alias is invalid"):
        alias_module._compute_request_binding(
            endpoint_fields_by_name=_endpoint_identity().public_payload(),
            key_contract_id=KEY_CONTRACT_ID,
            aliases=(object(),),
        )

    forged_alias = object.__new__(alias_module._LogicalScopeLookupAlias)
    with pytest.raises(LogicalScopePseudonymError, match="alias state"):
        alias_module._compute_request_binding(
            endpoint_fields_by_name=_endpoint_identity().public_payload(),
            key_contract_id=KEY_CONTRACT_ID,
            aliases=(forged_alias,),
        )
    forged_key = object.__new__(protection_module._LogicalScopePseudonymizer)
    with pytest.raises(LogicalScopePseudonymError, match="pseudonymizer state"):
        _request(pseudonymizers=(forged_key,))


def test_sealed_request_and_decision_reject_forged_state():
    with pytest.raises(TypeError, match="build_scope_lookup_request"):
        LogicalScopeLookupRequest()
    with pytest.raises(TypeError, match="resolver-created"):
        LogicalScopeRegistryDecision()
    forged_request = object.__new__(LogicalScopeLookupRequest)
    with pytest.raises(LogicalScopePseudonymError, match="request state"):
        forged_request.public_payload()
    forged_decision = object.__new__(LogicalScopeRegistryDecision)
    with pytest.raises(LogicalScopePseudonymError, match="decision state"):
        forged_decision.status

    assert not scope_module._is_valid_decision_shape(
        status="unknown",
        scope_id=None,
        matched_alias_count=0,
        distinct_scope_count=0,
    )
    with pytest.raises(LogicalScopePseudonymError, match="decision"):
        scope_module._build_registry_decision(
            status=LOOKUP_DECISION_MATCHED,
            scope_id=object(),
            matched_alias_count=1,
            distinct_scope_count=1,
        )


def test_registry_snapshot_allocation_failure_is_a_safe_domain_error(monkeypatch):
    def fail_snapshot(results_by_alias):
        raise MemoryError("HMAC-TOKEN-PLACEHOLDER")

    monkeypatch.setattr(scope_module, "_snapshot_registry_results", fail_snapshot)
    with pytest.raises(LogicalScopePseudonymError) as error_info:
        _resolve(_request(), {})
    assert "HMAC-TOKEN-PLACEHOLDER" not in str(error_info.value)


def test_callback_containers_are_rejected_without_invocation():
    key_values = _ExplosiveSequence()
    with pytest.raises(LogicalScopePseudonymError) as key_error:
        _request(normalized_key_values=key_values)
    assert key_values.callback_count == 0
    assert "RAW-PLAN-123" not in str(key_error.value)

    pseudonymizers = _ExplosiveSequence()
    with pytest.raises(LogicalScopePseudonymError) as keyring_error:
        _request(pseudonymizers=pseudonymizers)
    assert pseudonymizers.callback_count == 0
    assert "RAW-PLAN-123" not in str(keyring_error.value)

    results_by_alias = _ExplosiveMapping()
    with pytest.raises(LogicalScopePseudonymError) as mapping_error:
        _resolve(_request(), results_by_alias)
    assert results_by_alias.callback_count == 0
    assert "HMAC-TOKEN-PLACEHOLDER" not in str(mapping_error.value)


def test_rotation_value_secret_and_registry_bounds_are_enforced():
    maximum_keys = tuple(_pseudonymizer(version) for version in range(1, 9))
    maximum_request = _request(pseudonymizers=maximum_keys)
    assert len(_aliases(maximum_request)) == 8
    with pytest.raises(LogicalScopePseudonymError, match="pseudonymizer set"):
        _request(pseudonymizers=(*maximum_keys, _pseudonymizer(9)))
    with pytest.raises(LogicalScopePseudonymError, match="key values"):
        _request(normalized_key_values=tuple(str(index) for index in range(9)))
    with pytest.raises(LogicalScopePseudonymError, match="key values"):
        _request(normalized_key_values=("x" * 257,))

    maximum_secret = build_scope_pseudonymizer(
        key_id="provider-directory.maximum",
        key_version=1,
        secret=b"a" * 256,
    )
    assert "maximum" in repr(maximum_secret)
    with pytest.raises(LogicalScopePseudonymError, match="secret"):
        build_scope_pseudonymizer(
            key_id="provider-directory.too-large",
            key_version=1,
            secret=b"a" * 257,
        )

    oversized_results_by_alias = {alias: SCOPE_A for alias in _aliases(maximum_request)}
    oversized_results_by_alias[object()] = SCOPE_A
    with pytest.raises(LogicalScopePseudonymError, match="lookup input"):
        _resolve(maximum_request, oversized_results_by_alias)


@pytest.mark.parametrize(
    "invalid_base",
    (
        "not-a-url",
        "ftp://fhir.example.test/provider-directory",
        "https://user@fhir.example.test/provider-directory",
        "https://[broken-ipv6/provider-directory",
    ),
)
def test_existing_importer_refuses_endpoint_rows_outside_strict_admission(
    invalid_base,
):
    importer = importlib.import_module("process.provider_directory_fhir")
    source_by_field = {
        "source_id": "pdfhir_invalid_compat",
        "canonical_api_base": invalid_base,
        "api_base": invalid_base,
        "endpoint_practitioner": f"{invalid_base}/Practitioner",
    }
    assert importer._provider_directory_api_endpoint_row(source_by_field) is None
