# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Core and frozen proofs for connector publication authorization."""

from __future__ import annotations

import ast
import hashlib
import inspect
from dataclasses import replace

import pytest

from process import tin_npi_connector_publication_authorization as authorization
from process import (
    tin_npi_connector_publication_authorization_contract as contract,
)
from process.tin_npi_connector_publication_authorization_contract import (
    ConnectorPublicationAuthorizationError,
)
from tests.tin_npi_connector_publication_authorization_support import (
    BASE_TIME,
    PRIVATE_KEY_BYTES,
    PUBLIC_KEY_BYTES,
    authorization_trust,
    sealed_generation,
    sign_intent,
    signed_envelope,
    synthetic_bundle,
    unsigned_intent,
    verify,
)

FROZEN_PUBLIC_KEY_HEX = (
    "79b5562e8fe654f94078b112e8a98ba7901f853ae695bed7e0e3910bad049664"
)
FROZEN_CANONICAL_SHA256 = (
    "7ad9ba57a34b39432e1b0ed25cfa0d46d647c673fd43f2f33a104144548010e1"
)
FROZEN_INTENT_ID = "00ba5191c042467d3a803c1f0ea560591498b96b4ed7c0ea59b1a2314193a1f7"
FROZEN_SIGNATURE = (
    "QPqyDnzR_CISEYXSWN3MBauXvZI6ezxqzu2iS2EJeW0I9QObKx7foqu0pwoq2n6U"
    "ilgsGsR2jB0n66FYfTE2CA"
)


def test_publication_authorization_frozen_ed25519_vector(tmp_path):
    bundle = synthetic_bundle(tmp_path)
    sealed = sealed_generation(bundle)
    envelope = signed_envelope(bundle, sealed)
    canonical_json = contract.canonical_publication_authorization_json(
        envelope["intent"]
    )

    assert PRIVATE_KEY_BYTES.hex() == "".join(f"{value:02x}" for value in range(1, 33))
    assert PUBLIC_KEY_BYTES.hex() == FROZEN_PUBLIC_KEY_HEX
    assert hashlib.sha256(canonical_json.encode("ascii")).hexdigest() == (
        FROZEN_CANONICAL_SHA256
    )
    assert envelope["intent"]["intent_id"] == FROZEN_INTENT_ID
    assert envelope["signature"] == FROZEN_SIGNATURE


def test_absent_predecessor_authorization_binds_exact_sealed_bundle(tmp_path):
    bundle = synthetic_bundle(tmp_path)
    sealed = sealed_generation(bundle)
    envelope = signed_envelope(bundle, sealed)

    verified = verify(envelope, bundle=bundle, sealed=sealed)

    assert verified.target_generation_key == sealed.generation_key
    assert verified.expected_pointer_version == 0
    assert verified.expected_predecessor_generation_key is None
    assert verified.source_ids == bundle.generation.source_ordinal_map
    assert verified.source_vector_id == bundle.source_vector.source_vector_id
    assert verified.generation_id == bundle.generation.generation_id
    assert verified.validated_at == BASE_TIME


def test_present_predecessor_authorization_preserves_exact_cas(tmp_path):
    bundle = synthetic_bundle(tmp_path)
    sealed = sealed_generation(bundle, generation_key=81)
    envelope = signed_envelope(
        bundle,
        sealed,
        predecessor_key=77,
        pointer_version=9,
    )

    verified = verify(envelope, bundle=bundle, sealed=sealed)

    assert verified.target_generation_key == 81
    assert verified.expected_pointer_version == 9
    assert verified.expected_predecessor_generation_key == 77


def test_verification_receipt_repr_omits_signed_and_source_identities(tmp_path):
    bundle = synthetic_bundle(tmp_path)
    sealed = sealed_generation(bundle)
    envelope = signed_envelope(bundle, sealed)
    verified = verify(envelope, bundle=bundle, sealed=sealed)

    rendered = repr(verified)

    assert rendered == (
        "<connector-publication-authorization-receipt "
        "target=41 pointer=0 predecessor=absent>"
    )
    hidden_values = (
        envelope["intent"]["intent_id"],
        envelope["signature"],
        bundle.source_vector.source_vector_id,
        bundle.generation.generation_id,
        *bundle.generation.source_ordinal_map,
    )
    assert all(hidden_value not in rendered for hidden_value in hidden_values)


@pytest.mark.parametrize(
    "sealed_overrides",
    (
        {"source_vector_id": "aa" * 32},
        {"generation_id": "bb" * 32},
        {"generation_key": 42},
    ),
)
def test_authorization_rejects_sealed_identity_or_key_drift(
    tmp_path,
    sealed_overrides,
):
    bundle = synthetic_bundle(tmp_path)
    authorized_sealed = sealed_generation(bundle)
    envelope = signed_envelope(bundle, authorized_sealed)
    drifted_sealed = sealed_generation(bundle, **sealed_overrides)

    with pytest.raises(
        ConnectorPublicationAuthorizationError,
        match="authorization binding_mismatch$",
    ):
        verify(envelope, bundle=bundle, sealed=drifted_sealed)


def test_authorization_rejects_sealed_count_drift(tmp_path):
    bundle = synthetic_bundle(tmp_path)
    authorized_sealed = sealed_generation(bundle)
    envelope = signed_envelope(bundle, authorized_sealed)
    drifted_counts = replace(
        bundle.counts,
        evidence_row_count=bundle.counts.evidence_row_count + 1,
    )
    drifted_sealed = sealed_generation(bundle, counts=drifted_counts)

    with pytest.raises(
        ConnectorPublicationAuthorizationError,
        match="authorization binding_mismatch$",
    ):
        verify(envelope, bundle=bundle, sealed=drifted_sealed)


def test_authorization_rejects_source_not_in_public_allowlist(tmp_path):
    bundle = synthetic_bundle(tmp_path)
    sealed = sealed_generation(bundle)
    envelope = signed_envelope(bundle, sealed)
    trust = authorization_trust(
        bundle,
        public_source_ids=(bundle.generation.source_ordinal_map[0],),
    )

    with pytest.raises(
        ConnectorPublicationAuthorizationError,
        match="authorization source_rights_denied$",
    ):
        verify(envelope, bundle=bundle, sealed=sealed, trust=trust)


@pytest.mark.parametrize(
    ("intent_override", "trust_override", "expected_code"),
    (
        ({"authority_id": "other-authority"}, {}, "authority_mismatch"),
        (
            {"publication_scope_id": "other-scope"},
            {},
            "scope_mismatch",
        ),
        (
            {"authority_release_digest": "44" * 32},
            {},
            "authority_release_mismatch",
        ),
        ({"key_id": "unknown-key"}, {}, "unknown_key"),
    ),
)
def test_authorization_rejects_trust_binding_drift(
    tmp_path,
    intent_override,
    trust_override,
    expected_code,
):
    bundle = synthetic_bundle(tmp_path)
    sealed = sealed_generation(bundle)
    intent = unsigned_intent(bundle, sealed, **intent_override)
    envelope = sign_intent(intent)
    trust = authorization_trust(bundle, **trust_override)

    with pytest.raises(
        ConnectorPublicationAuthorizationError,
        match=rf"authorization {expected_code}$",
    ):
        verify(envelope, bundle=bundle, sealed=sealed, trust=trust)


def test_verification_receipt_is_not_accepted_as_authorization_input(tmp_path):
    bundle = synthetic_bundle(tmp_path)
    sealed = sealed_generation(bundle)
    envelope = signed_envelope(bundle, sealed)
    receipt = verify(envelope, bundle=bundle, sealed=sealed)

    with pytest.raises(
        ConnectorPublicationAuthorizationError,
        match="authorization invalid_envelope_fields$",
    ):
        authorization.verify_connector_publication_authorization(
            receipt,
            trust=authorization_trust(bundle),
            bundle=bundle,
            sealed_generation=sealed,
            now=BASE_TIME,
        )
    assert not hasattr(contract, "verified_publication_intent")


def test_authorization_modules_have_no_database_adapter_imports_or_calls():
    for module in (authorization, contract):
        module_source = inspect.getsource(module)
        syntax_tree = ast.parse(module_source)
        imported_modules = {
            alias.name
            for node in ast.walk(syntax_tree)
            if isinstance(node, ast.Import)
            for alias in node.names
        }
        imported_modules.update(
            node.module or ""
            for node in ast.walk(syntax_tree)
            if isinstance(node, ast.ImportFrom)
        )
        assert not any(
            imported_module == "db" or imported_module.startswith("db.")
            for imported_module in imported_modules
        )
        assert ".execute(" not in module_source
        assert ".fetch" not in module_source
