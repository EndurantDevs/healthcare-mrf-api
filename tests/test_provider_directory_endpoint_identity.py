# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import copy
from dataclasses import asdict
import hashlib
import importlib
import json
import pickle

import pytest

import process.provider_directory_endpoint_admission as admission_module
from process.provider_directory_logical_scope import (
    ProviderDirectoryEndpointIdentity,
    ProviderDirectoryEndpointIdentityError,
    build_provider_directory_endpoint_identity,
)


CANONICAL_API_BASE = "https://fhir.example.test/provider-directory"
CREDENTIAL_DESCRIPTOR = {
    "auth": "header",
    "header_name": "X-API-Key",
    "secret_env": "EXAMPLE_API_KEY",
}
ENDPOINT_SIGNATURE = {
    "Location": "Location",
    "Organization": "Organization",
    "Practitioner": "Practitioner",
}


def _stable_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _identity_hash(value: object) -> str:
    return hashlib.sha256(_stable_json(value).encode("utf-8")).hexdigest()


def _endpoint_fields(**changes: object) -> dict[str, object]:
    credential_descriptor = changes.pop(
        "credential_descriptor_json",
        CREDENTIAL_DESCRIPTOR,
    )
    endpoint_signature = changes.pop(
        "endpoint_signature_json",
        ENDPOINT_SIGNATURE,
    )
    canonical_api_base = changes.pop("canonical_api_base", CANONICAL_API_BASE)
    try:
        credential_hash = _identity_hash(credential_descriptor)
        signature_hash = _identity_hash(endpoint_signature)
        endpoint_id = _identity_hash(
            {
                "canonical_api_base": canonical_api_base,
                "credential_descriptor": credential_descriptor,
                "endpoint_signature": endpoint_signature,
            }
        )
    except (TypeError, ValueError):
        credential_hash = "0" * 64
        signature_hash = "0" * 64
        endpoint_id = "0" * 64
    fields_by_name: dict[str, object] = {
        "endpoint_id": endpoint_id,
        "canonical_api_base": canonical_api_base,
        "credential_descriptor_hash": credential_hash,
        "endpoint_signature_hash": signature_hash,
        "credential_descriptor_json": credential_descriptor,
        "endpoint_signature_json": endpoint_signature,
    }
    fields_by_name.update(changes)
    return fields_by_name


def _endpoint(**changes: object) -> ProviderDirectoryEndpointIdentity:
    return build_provider_directory_endpoint_identity(**_endpoint_fields(**changes))


def _endpoint_with_untrusted_descriptor(descriptor: object):
    return build_provider_directory_endpoint_identity(
        endpoint_id="0" * 64,
        canonical_api_base=CANONICAL_API_BASE,
        credential_descriptor_hash="0" * 64,
        endpoint_signature_hash="0" * 64,
        credential_descriptor_json=descriptor,
        endpoint_signature_json=ENDPOINT_SIGNATURE,
    )


def test_factory_reproduces_existing_importer_endpoint_record_identity():
    endpoint = _endpoint()
    repeated = _endpoint()

    assert endpoint.endpoint_id == _endpoint_fields()["endpoint_id"]
    assert endpoint.canonical_api_base == CANONICAL_API_BASE
    assert endpoint.credential_descriptor_hash == _identity_hash(
        CREDENTIAL_DESCRIPTOR
    )
    assert endpoint.endpoint_signature_hash == _identity_hash(ENDPOINT_SIGNATURE)
    assert endpoint == repeated
    assert hash(endpoint) == hash(repeated)
    assert endpoint != object()
    assert endpoint.public_payload() == {
        "endpoint_id": endpoint.endpoint_id,
        "canonical_api_base": CANONICAL_API_BASE,
        "credential_descriptor_hash": endpoint.credential_descriptor_hash,
        "endpoint_signature_hash": endpoint.endpoint_signature_hash,
    }
    assert endpoint.endpoint_id in repr(endpoint)
    assert "secret_env" not in repr(endpoint)


def test_identity_is_nominal_immutable_and_generic_copies_are_inert():
    endpoint = _endpoint()

    with pytest.raises(TypeError, match="use build"):
        ProviderDirectoryEndpointIdentity()
    with pytest.raises(TypeError, match="immutable"):
        endpoint._endpoint_id = "f" * 64
    with pytest.raises(TypeError):
        asdict(endpoint)

    copied = copy.deepcopy(endpoint)
    shallow_copied = copy.copy(endpoint)
    unpickled = pickle.loads(pickle.dumps(endpoint))
    assert not isinstance(copied, ProviderDirectoryEndpointIdentity)
    assert not isinstance(shallow_copied, ProviderDirectoryEndpointIdentity)
    assert not isinstance(unpickled, ProviderDirectoryEndpointIdentity)
    assert copy.deepcopy(copied) is copied
    assert "snapshot" in repr(copied)


@pytest.mark.parametrize(
    ("field_name", "replacement", "message"),
    (
        ("endpoint_id", "f" * 64, "endpoint ID is inconsistent"),
        (
            "credential_descriptor_hash",
            "f" * 64,
            "credential descriptor hash is inconsistent",
        ),
        (
            "endpoint_signature_hash",
            "f" * 64,
            "endpoint signature hash is inconsistent",
        ),
    ),
)
def test_factory_rejects_wrong_domain_or_stale_record_hashes(
    field_name,
    replacement,
    message,
):
    with pytest.raises(ProviderDirectoryEndpointIdentityError, match=message):
        _endpoint(**{field_name: replacement})


@pytest.mark.parametrize(
    "invalid_base",
    (
        "",
        " https://fhir.example.test/provider-directory",
        "HTTPS://fhir.example.test/provider-directory",
        "https://FHIR.EXAMPLE.TEST/provider-directory",
        "https://fhir.example.test/provider-directory/",
        "https://fhir.example.test/provider-directory?plan=1",
        "https://user@fhir.example.test/provider-directory",
        "ftp://fhir.example.test/provider-directory",
        "not-a-url",
        "https://[broken-ipv6/provider-directory",
        "https://fhir.example.test\\credential",
        "https://fhir.example.test/path name",
        "https://fhir.example.test:",
        "https://fhir.example.test:99999/provider-directory",
        7,
        "https://example.test/" + "x" * 2048,
    ),
)
def test_factory_rejects_noncanonical_api_bases(invalid_base):
    with pytest.raises(ProviderDirectoryEndpointIdentityError, match="API base"):
        _endpoint(canonical_api_base=invalid_base)


@pytest.mark.parametrize(
    ("changes", "message"),
    (
        ({"endpoint_id": "bad"}, "endpoint ID"),
        ({"credential_descriptor_hash": b"bad"}, "credential descriptor hash"),
        ({"endpoint_signature_hash": True}, "endpoint signature hash"),
        ({"credential_descriptor_json": []}, "credential descriptor"),
        ({"endpoint_signature_json": []}, "endpoint signature"),
        ({"credential_descriptor_json": {7: "bad"}}, "credential descriptor"),
        ({"endpoint_signature_json": {"bad": {1, 2}}}, "JSON data"),
    ),
)
def test_factory_rejects_malformed_endpoint_record_fields(changes, message):
    with pytest.raises(ProviderDirectoryEndpointIdentityError, match=message):
        _endpoint(**changes)


def test_validation_rejects_valid_format_postconstruction_mutation():
    endpoint = _endpoint()
    stable_hash = hash(endpoint)
    object.__setattr__(endpoint, "_endpoint_id", "f" * 64)

    assert type(stable_hash) is int
    with pytest.raises(
        ProviderDirectoryEndpointIdentityError,
        match="endpoint ID is inconsistent",
    ):
        hash(endpoint)
    with pytest.raises(
        ProviderDirectoryEndpointIdentityError,
        match="endpoint ID is inconsistent",
    ):
        endpoint.validate()

    changed_binding = _endpoint()
    object.__setattr__(changed_binding, "_identity_binding", "f" * 64)
    with pytest.raises(
        ProviderDirectoryEndpointIdentityError,
        match="identity binding",
    ):
        changed_binding.validate()

    changed_hash = _endpoint()
    object.__setattr__(changed_hash, "_stable_hash", 7)
    with pytest.raises(
        ProviderDirectoryEndpointIdentityError,
        match="stable hash",
    ):
        changed_hash.validate()


def test_equality_hash_properties_and_repr_fail_closed_after_mutation():
    original = _endpoint()
    equivalent = _endpoint()
    other = _endpoint(canonical_api_base="https://other.example.test/fhir")

    assert original == equivalent
    assert original != other

    object.__setattr__(original, "_identity_binding", other._identity_binding)
    with pytest.raises(ProviderDirectoryEndpointIdentityError, match="binding"):
        original == other
    with pytest.raises(ProviderDirectoryEndpointIdentityError, match="binding"):
        hash(original)

    leaked_value = "RAW-CREDENTIAL-MUST-NOT-LEAK"
    object.__setattr__(equivalent, "_canonical_api_base", leaked_value)
    for public_read in (
        lambda: equivalent.endpoint_id,
        lambda: equivalent.canonical_api_base,
        lambda: equivalent.credential_descriptor_hash,
        lambda: equivalent.endpoint_signature_hash,
        lambda: repr(equivalent),
    ):
        with pytest.raises(ProviderDirectoryEndpointIdentityError) as error_info:
            public_read()
        assert leaked_value not in str(error_info.value)


def test_validation_rejects_corrupt_private_json_without_leaking_it():
    endpoint = _endpoint()
    object.__setattr__(endpoint, "_credential_descriptor_json", "not-json")

    with pytest.raises(ProviderDirectoryEndpointIdentityError) as error_info:
        endpoint.validate()

    assert "not-json" not in str(error_info.value)

    wrong_container = _endpoint()
    object.__setattr__(wrong_container, "_endpoint_signature_json", 7)
    with pytest.raises(
        ProviderDirectoryEndpointIdentityError,
        match="stored endpoint signature",
    ):
        wrong_container.validate()

    noncanonical_json = _endpoint()
    object.__setattr__(
        noncanonical_json,
        "_credential_descriptor_json",
        '{"auth": "header"}',
    )
    with pytest.raises(
        ProviderDirectoryEndpointIdentityError,
        match="stored credential descriptor",
    ):
        noncanonical_json.validate()

    oversized_stored_json = _endpoint()
    object.__setattr__(
        oversized_stored_json,
        "_credential_descriptor_json",
        "x" * (16 * 1024 + 1),
    )
    with pytest.raises(
        ProviderDirectoryEndpointIdentityError,
        match="stored credential descriptor",
    ):
        oversized_stored_json.validate()


class _ExplosiveDict(dict):
    def items(self):
        raise RuntimeError("RAW-ENDPOINT-DESCRIPTOR")


def _cyclic_descriptor() -> dict[str, object]:
    cycle_list: list[object] = []
    cycle_list.append(cycle_list)
    return {"cycle": cycle_list}


def _cyclic_dict_descriptor() -> dict[str, object]:
    cycle_by_key: dict[str, object] = {}
    cycle_by_key["cycle"] = cycle_by_key
    return cycle_by_key


def _deep_descriptor() -> dict[str, object]:
    nested_values: object = "leaf"
    for _ in range(9):
        nested_values = [nested_values]
    return {"nested": nested_values}


@pytest.mark.parametrize(
    "descriptor_factory",
    (
        lambda: _ExplosiveDict({"auth": "none"}),
        lambda: {"nested": _ExplosiveDict({"auth": "none"})},
        lambda: {"nested": ("not", "a", "list")},
        lambda: {"number": float("nan")},
        lambda: {"number": float("inf")},
        lambda: {"number": 1 << 257},
        lambda: {"text": "\ud800"},
        lambda: {"k" * 257: "value"},
        lambda: {"text": "x" * (16 * 1024 + 1)},
        lambda: {str(index): None for index in range(4097)},
        lambda: {"wide": [None] * 4097},
        lambda: {"wide": [None] * 4096},
        _cyclic_descriptor,
        _cyclic_dict_descriptor,
        _deep_descriptor,
    ),
)
def test_factory_rejects_callback_or_unbounded_recursive_descriptor_data(
    descriptor_factory,
):
    with pytest.raises(ProviderDirectoryEndpointIdentityError) as error_info:
        _endpoint_with_untrusted_descriptor(descriptor_factory())

    assert "RAW-ENDPOINT-DESCRIPTOR" not in str(error_info.value)


def test_factory_snapshots_recursive_exact_json_once_and_enforces_byte_caps():
    credential_descriptor_by_field = {
        "auth": "header",
        "nested": [{"key": "value"}],
    }
    endpoint = _endpoint(credential_descriptor_json=credential_descriptor_by_field)
    credential_descriptor_by_field["nested"][0]["key"] = "changed-after-build"
    endpoint.validate()

    numeric_endpoint = _endpoint(
        credential_descriptor_json={"integer": 7, "ratio": 1.5},
    )
    numeric_endpoint.validate()

    with pytest.raises(ProviderDirectoryEndpointIdentityError, match="bounded JSON"):
        _endpoint(credential_descriptor_json={"text": "x" * (16 * 1024)})

    with pytest.raises(ProviderDirectoryEndpointIdentityError, match="bounded JSON"):
        _endpoint(credential_descriptor_json={"text": "\x00" * 3000})

    with pytest.raises(ProviderDirectoryEndpointIdentityError, match="bounded JSON"):
        _endpoint(
            endpoint_signature_json={
                "chunks": ["x" * 14_000 for _ in range(5)],
            }
        )


def test_descriptor_render_failures_are_safe_domain_errors(monkeypatch):
    def fail_render(*args, **kwargs):
        raise MemoryError("RAW-DESCRIPTOR")

    monkeypatch.setattr(admission_module.json, "dumps", fail_render)
    with pytest.raises(ProviderDirectoryEndpointIdentityError) as error_info:
        _endpoint_with_untrusted_descriptor(CREDENTIAL_DESCRIPTOR)
    assert "RAW-DESCRIPTOR" not in str(error_info.value)


@pytest.mark.parametrize("fail_on_call", (1, 2))
def test_descriptor_snapshot_allocation_failures_are_safe(monkeypatch, fail_on_call):
    original_tuple = tuple
    capture_calls: list[object] = []

    def bounded_capture(values):
        capture_calls.append(values)
        if len(capture_calls) == fail_on_call:
            raise MemoryError("RAW-SNAPSHOT")
        return original_tuple(values)

    monkeypatch.setattr(admission_module, "tuple", bounded_capture, raising=False)
    descriptor = {"nested": ["value"]} if fail_on_call == 2 else {"auth": "none"}
    with pytest.raises(ProviderDirectoryEndpointIdentityError) as error_info:
        _endpoint_with_untrusted_descriptor(descriptor)
    assert "RAW-SNAPSHOT" not in str(error_info.value)


def _compatibility_source_rows() -> tuple[dict[str, str], ...]:
    public_base = "https://public.example.test/provider-directory"
    credential_base = "https://credential.example.test/provider-directory"
    return (
        {
            "source_id": "pdfhir_public_compat",
            "canonical_api_base": public_base,
            "api_base": public_base,
            "endpoint_practitioner": f"{public_base}/Practitioner",
        },
        {
            "source_id": "pdfhir_credential_compat",
            "canonical_api_base": credential_base,
            "api_base": credential_base,
            "endpoint_practitioner": f"{credential_base}/Practitioner",
        },
    )


def _assert_importer_row_identity(endpoint_row, credential_value: str) -> None:
    endpoint_identity = build_provider_directory_endpoint_identity(
        endpoint_id=endpoint_row["endpoint_id"],
        canonical_api_base=endpoint_row["canonical_api_base"],
        credential_descriptor_hash=endpoint_row["credential_descriptor_hash"],
        endpoint_signature_hash=endpoint_row["endpoint_signature_hash"],
        credential_descriptor_json=endpoint_row["credential_descriptor_json"],
        endpoint_signature_json=endpoint_row["endpoint_signature_json"],
    )
    assert endpoint_identity.endpoint_id == endpoint_row["endpoint_id"]
    assert endpoint_identity.public_payload() == {
        "endpoint_id": endpoint_row["endpoint_id"],
        "canonical_api_base": endpoint_row["canonical_api_base"],
        "credential_descriptor_hash": endpoint_row["credential_descriptor_hash"],
        "endpoint_signature_hash": endpoint_row["endpoint_signature_hash"],
    }
    rendered_identity = repr(endpoint_identity) + json.dumps(
        endpoint_identity.public_payload(),
        sort_keys=True,
    )
    assert credential_value not in rendered_identity
    assert credential_value not in json.dumps(
        endpoint_row["credential_descriptor_json"],
        sort_keys=True,
    )


def test_identity_factory_accepts_exact_rows_from_existing_importer(monkeypatch):
    """Prove the nominal factory reproduces public and credentialed rows."""

    importer = importlib.import_module("process.provider_directory_fhir")
    credential_value = "private-value-must-not-be-represented"
    monkeypatch.setattr(
        importer,
        "_load_credentials_config",
        lambda: {
            "sources": {
                "pdfhir_credential_compat": {
                    "api_key": {
                        "header": "X-Compatibility-Key",
                        "value": credential_value,
                    }
                }
            }
        },
    )

    for source_row in _compatibility_source_rows():
        endpoint_row = importer._provider_directory_api_endpoint_row(source_row)
        assert endpoint_row is not None
        _assert_importer_row_identity(endpoint_row, credential_value)
