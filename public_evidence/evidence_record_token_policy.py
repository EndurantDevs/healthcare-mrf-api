# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Canonical token-policy profiles for opaque public evidence identities."""

from __future__ import annotations

import hashlib
import hmac
import re
from types import MappingProxyType
from typing import Mapping, NamedTuple

from public_evidence.evidence_record_primitives import (
    PUBLIC_EVIDENCE_TAX_IDENTITY_REF_PREFIX,
    OpaqueTaxIdentityReference,
    _derived_ref,
    _exact_dict,
    _fail,
    _strict_sha256,
    _validate_derived_ref,
)

PTG_V4_EIN_TOKEN_POLICY_CONTRACT = "ptg_v4_ein_tax_identity_policy_v1"
PUBLIC_EIN_NPI_TOKEN_POLICY_CONTRACT = "healthporta_ein_npi_tax_identity_policy_v1"

_KEY_ID_RE = re.compile(r"[a-z0-9](?:[a-z0-9._-]{0,31})", flags=re.ASCII)
_LOCATOR_RE = re.compile(r"[0-9a-f]{32}", flags=re.ASCII)
_PTG_DESCRIPTOR_DOMAIN = b"PTG2V4TINPOLICY\x01"
_PUBLIC_DESCRIPTOR_DOMAIN = b"HEALTHPORTA_PUBLIC_EIN_NPI_POLICY\x01"
_HMAC_CONTRACT = "hmac_sha256_ptg_tin_v1"
_LOCATOR_CONTRACT = "tin_id_128=first_16_bytes(tin_hmac_sha256)"
_AUTHORITY_CONTRACT = "tin_hmac_sha256_full_32_bytes_authoritative"


class _TokenPolicyProfile(NamedTuple):
    policy_id_prefix: str
    descriptor_domain: bytes
    normalization_by_type: Mapping[str, str]
    descriptor_preamble: tuple[str, ...]


TOKEN_POLICY_PROFILES = MappingProxyType(
    {
        PTG_V4_EIN_TOKEN_POLICY_CONTRACT: _TokenPolicyProfile(
            "ptg-tin-hmac-sha256-v1:",
            _PTG_DESCRIPTOR_DOMAIN,
            MappingProxyType({"ein": "ein_ascii_digits_or_2_7_hyphen_v1"}),
            ("ein_ascii_digits_or_2_7_hyphen_v1",),
        ),
        PUBLIC_EIN_NPI_TOKEN_POLICY_CONTRACT: _TokenPolicyProfile(
            "healthporta-tax-identity-hmac-sha256-v1:",
            _PUBLIC_DESCRIPTOR_DOMAIN,
            MappingProxyType(
                {
                    "ein": "ein_ascii_digits_or_2_7_hyphen_v1",
                    "npi": "npi_ascii_10_luhn_v1",
                }
            ),
            (
                "supported_tin_types=ein,npi",
                "ein_normalization=ein_ascii_digits_or_2_7_hyphen_v1",
                "npi_normalization=npi_ascii_10_luhn_v1",
            ),
        ),
    }
)


def _profile(contract_id: object) -> _TokenPolicyProfile:
    if type(contract_id) is not str or contract_id not in TOKEN_POLICY_PROFILES:
        raise _fail()
    return TOKEN_POLICY_PROFILES[contract_id]


def _policy_id(profile: _TokenPolicyProfile, value: object) -> str:
    if type(value) is not str or not value.startswith(profile.policy_id_prefix):
        raise _fail()
    key_id = value[len(profile.policy_id_prefix) :]
    if _KEY_ID_RE.fullmatch(key_id) is None:
        raise _fail()
    return value


def token_policy_descriptor_sha256(contract_id: object, policy_id: object) -> str:
    """Return the canonical digest for one nonsecret token-policy profile."""
    profile = _profile(contract_id)
    canonical_policy_id = _policy_id(profile, policy_id)
    descriptor_fields = (
        canonical_policy_id,
        *profile.descriptor_preamble,
        _HMAC_CONTRACT,
        _LOCATOR_CONTRACT,
        _AUTHORITY_CONTRACT,
    )
    descriptor_digest = hashlib.sha256(profile.descriptor_domain)
    for descriptor_field in descriptor_fields:
        encoded_field = descriptor_field.encode("ascii")
        descriptor_digest.update(len(encoded_field).to_bytes(4, "big"))
        descriptor_digest.update(encoded_field)
    return descriptor_digest.hexdigest()


def build_opaque_tax_identity(
    raw: Mapping[str, object],
) -> OpaqueTaxIdentityReference:
    """Freeze one tokenized identity under a verified policy profile."""
    fields = frozenset(
        "tin_type token_policy_contract_id token_policy_id "
        "token_policy_descriptor_sha256 locator_128 full_hmac_sha256".split()
    )
    identity_fields = _exact_dict(raw, fields)
    contract_id = identity_fields["token_policy_contract_id"]
    profile = _profile(contract_id)
    policy_id = _policy_id(profile, identity_fields["token_policy_id"])
    tin_type = identity_fields["tin_type"]
    if type(tin_type) is not str or tin_type not in profile.normalization_by_type:
        raise _fail()
    expected_descriptor = token_policy_descriptor_sha256(contract_id, policy_id)
    supplied_descriptor = _strict_sha256(
        identity_fields["token_policy_descriptor_sha256"]
    )
    if not hmac.compare_digest(supplied_descriptor, expected_descriptor):
        raise _fail()
    locator = identity_fields["locator_128"]
    full_hmac = _strict_sha256(identity_fields["full_hmac_sha256"])
    if type(locator) is not str or _LOCATOR_RE.fullmatch(locator) is None:
        raise _fail()
    if not hmac.compare_digest(locator, full_hmac[:32]):
        raise _fail()
    identity_payload_by_field = {
        "tin_type": tin_type,
        "token_policy_contract_id": contract_id,
        "token_policy_id": policy_id,
        "token_policy_descriptor_sha256": supplied_descriptor,
        "locator_128": locator,
        "full_hmac_sha256": full_hmac,
        "normalization_contract_id": profile.normalization_by_type[tin_type],
    }
    return OpaqueTaxIdentityReference(
        **identity_payload_by_field,
        tax_identity_ref=_derived_ref(
            PUBLIC_EVIDENCE_TAX_IDENTITY_REF_PREFIX,
            "tax_identity",
            identity_payload_by_field,
        ),
    )


def validate_opaque_tax_identity(value: object) -> OpaqueTaxIdentityReference:
    """Rebuild and validate an exact opaque tax-identity descriptor."""
    if type(value) is not OpaqueTaxIdentityReference:
        raise _fail()
    rebuilt = build_opaque_tax_identity(
        {
            "tin_type": value.tin_type,
            "token_policy_contract_id": value.token_policy_contract_id,
            "token_policy_id": value.token_policy_id,
            "token_policy_descriptor_sha256": value.token_policy_descriptor_sha256,
            "locator_128": value.locator_128,
            "full_hmac_sha256": value.full_hmac_sha256,
        }
    )
    if (
        type(value.normalization_contract_id) is not str
        or value.normalization_contract_id != rebuilt.normalization_contract_id
    ):
        raise _fail()
    _validate_derived_ref(
        value.tax_identity_ref,
        PUBLIC_EVIDENCE_TAX_IDENTITY_REF_PREFIX,
        rebuilt.tax_identity_ref,
    )
    return rebuilt
