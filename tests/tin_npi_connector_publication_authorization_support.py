# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Synthetic fixtures for connector publication authorization proofs."""

from __future__ import annotations

import base64
import datetime as dt
from pathlib import Path
from typing import Any

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from process.tin_npi_connector_generation_store import SealedConnectorGeneration
from process.tin_npi_connector_publication_authorization import (
    verify_connector_publication_authorization,
)
from process.tin_npi_connector_publication_authorization_contract import (
    PUBLICATION_AUTHORIZATION_ACTION,
    PUBLICATION_AUTHORIZATION_CONTRACT_ID,
    PUBLICATION_AUTHORIZATION_RIGHTS_PROFILE,
    PUBLICATION_AUTHORIZATION_SIGNATURE_ALGORITHM,
    ConnectorPublicationAuthorizationTrust,
    ConnectorPublicationAuthorizationTrustKey,
    publication_authorization_intent_id,
    publication_authorization_signature_message,
)
from tests.test_tin_npi_connector_generation_store import _multi_source_bundle

BASE_TIME = dt.datetime(2026, 8, 4, 12, 0, 0, tzinfo=dt.timezone.utc)
AUTHORITY_ID = "synthetic-public-authority"
AUTHORITY_RELEASE_DIGEST = "11" * 32
PUBLICATION_SCOPE_ID = "synthetic-dev-connector-scope"
ACTIVE_KEY_ID = "key-2026-a"
ROTATED_KEY_ID = "key-2026-b"
NONCE = "22" * 32
PRIVATE_KEY_BYTES = bytes(range(1, 33))
ROTATED_PRIVATE_KEY_BYTES = bytes(range(33, 65))
PRIVATE_KEY = Ed25519PrivateKey.from_private_bytes(PRIVATE_KEY_BYTES)
ROTATED_PRIVATE_KEY = Ed25519PrivateKey.from_private_bytes(ROTATED_PRIVATE_KEY_BYTES)
SENSITIVE_SYNTHETIC_VALUE = "ab" * 32


def _public_key_bytes(private_key: Ed25519PrivateKey) -> bytes:
    return private_key.public_key().public_bytes(
        serialization.Encoding.Raw,
        serialization.PublicFormat.Raw,
    )


PUBLIC_KEY_BYTES = _public_key_bytes(PRIVATE_KEY)
ROTATED_PUBLIC_KEY_BYTES = _public_key_bytes(ROTATED_PRIVATE_KEY)


def synthetic_bundle(tmp_path: Path):
    return _multi_source_bundle(tmp_path)


def sealed_generation(bundle, *, generation_key: int = 41, **overrides):
    fields_by_name = {
        "generation_key": generation_key,
        "generation_id": bundle.generation.generation_id,
        "source_vector_id": bundle.source_vector.source_vector_id,
        "counts": bundle.counts,
        "reused": False,
    }
    fields_by_name.update(overrides)
    return SealedConnectorGeneration(**fields_by_name)


def authorization_trust(
    bundle,
    *,
    public_source_ids: tuple[str, ...] | None = None,
    authority_id: str = AUTHORITY_ID,
    publication_scope_id: str = PUBLICATION_SCOPE_ID,
) -> ConnectorPublicationAuthorizationTrust:
    trust_key = ConnectorPublicationAuthorizationTrustKey(
        key_id=ACTIVE_KEY_ID,
        public_key=PUBLIC_KEY_BYTES,
        authority_release_digest=AUTHORITY_RELEASE_DIGEST,
        public_source_ids=(
            bundle.generation.source_ordinal_map
            if public_source_ids is None
            else public_source_ids
        ),
        status="active",
    )
    return ConnectorPublicationAuthorizationTrust(
        publication_scope_id=publication_scope_id,
        authority_id=authority_id,
        active_key_id=ACTIVE_KEY_ID,
        keys=(trust_key,),
    )


def retired_key_trust(
    bundle,
    *,
    retired_at: dt.datetime,
    verify_until: dt.datetime,
) -> ConnectorPublicationAuthorizationTrust:
    retired_key = ConnectorPublicationAuthorizationTrustKey(
        key_id=ACTIVE_KEY_ID,
        public_key=PUBLIC_KEY_BYTES,
        authority_release_digest=AUTHORITY_RELEASE_DIGEST,
        public_source_ids=bundle.generation.source_ordinal_map,
        status="retired",
        retired_at=retired_at,
        verify_until=verify_until,
    )
    active_key = ConnectorPublicationAuthorizationTrustKey(
        key_id=ROTATED_KEY_ID,
        public_key=ROTATED_PUBLIC_KEY_BYTES,
        authority_release_digest="33" * 32,
        public_source_ids=bundle.generation.source_ordinal_map,
        status="active",
    )
    return ConnectorPublicationAuthorizationTrust(
        publication_scope_id=PUBLICATION_SCOPE_ID,
        authority_id=AUTHORITY_ID,
        active_key_id=ROTATED_KEY_ID,
        keys=(retired_key, active_key),
    )


def unsigned_intent(
    bundle,
    sealed,
    *,
    predecessor_key: int | None = None,
    pointer_version: int | None = None,
    issued_at: dt.datetime = BASE_TIME,
    expires_at: dt.datetime | None = None,
    **overrides: Any,
) -> dict[str, Any]:
    if pointer_version is None:
        pointer_version = 0 if predecessor_key is None else 7
    predecessor = (
        {"state": "absent"}
        if predecessor_key is None
        else {"state": "present", "generation_key": predecessor_key}
    )
    intent_fields_by_name = {
        "action": PUBLICATION_AUTHORIZATION_ACTION,
        "authority_id": AUTHORITY_ID,
        "authority_release_digest": AUTHORITY_RELEASE_DIGEST,
        "contract_id": PUBLICATION_AUTHORIZATION_CONTRACT_ID,
        "expected_pointer_version": pointer_version,
        "expected_predecessor": predecessor,
        "expires_at": _timestamp(expires_at or issued_at + dt.timedelta(minutes=10)),
        "generation_id": bundle.generation.generation_id,
        "issued_at": _timestamp(issued_at),
        "key_id": ACTIVE_KEY_ID,
        "nonce": NONCE,
        "publication_scope_id": PUBLICATION_SCOPE_ID,
        "rights_profile": PUBLICATION_AUTHORIZATION_RIGHTS_PROFILE,
        "signature_algorithm": PUBLICATION_AUTHORIZATION_SIGNATURE_ALGORITHM,
        "source_ids": list(bundle.generation.source_ordinal_map),
        "source_vector_id": bundle.source_vector.source_vector_id,
        "target_generation_key": sealed.generation_key,
    }
    intent_fields_by_name.update(overrides)
    return intent_fields_by_name


def signed_envelope(
    bundle,
    sealed,
    *,
    private_key: Ed25519PrivateKey = PRIVATE_KEY,
    **intent_options: Any,
) -> dict[str, Any]:
    intent_without_id = unsigned_intent(bundle, sealed, **intent_options)
    return sign_intent(intent_without_id, private_key=private_key)


def sign_intent(
    intent_without_id: dict[str, Any],
    *,
    private_key: Ed25519PrivateKey = PRIVATE_KEY,
) -> dict[str, Any]:
    intent_fields_by_name = dict(intent_without_id)
    intent_fields_by_name["intent_id"] = publication_authorization_intent_id(
        intent_without_id
    )
    signature = private_key.sign(
        publication_authorization_signature_message(intent_fields_by_name)
    )
    return {
        "intent": intent_fields_by_name,
        "signature": base64.urlsafe_b64encode(signature).rstrip(b"=").decode("ascii"),
    }


def verify(envelope, *, bundle, sealed, trust=None, now=BASE_TIME):
    return verify_connector_publication_authorization(
        envelope,
        trust=trust or authorization_trust(bundle),
        bundle=bundle,
        sealed_generation=sealed,
        now=now,
    )


def _timestamp(value: dt.datetime) -> str:
    return value.strftime("%Y-%m-%dT%H:%M:%SZ")
