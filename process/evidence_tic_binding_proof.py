# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Private integrity proof derived from one authenticated TiC shadow row."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import hmac
import json
import re
from typing import Literal

from process.evidence_source_release_contract import (
    PublicEvidenceSourceReleaseDescriptor,
    TAX_IDENTITY_SHADOW_SOURCE_BINDING_CONTRACT,
)
from process.ptg_parts import ptg2_tax_identity_shadow_admission as admission
from process.tin_npi_connector_security import token_policy_descriptor_sha256

TIC_TAX_IDENTITY_POLICY_VERSION = 1
TIC_PROVIDER_GROUP_REFERENCE_CONTRACT = "tic_provider_group_global_id_sha256_v1"

_GROUP_REFERENCE_DOMAIN = b"HEALTHPORTA_TIC_PROVIDER_GROUP_REFERENCE_V1\x00"
_RECEIPT_DOMAIN = b"HEALTHPORTA_TIC_TAX_IDENTITY_BINDING_RECEIPT_V1\x00"
_HEX_32_RE = re.compile(r"[0-9a-f]{32}", flags=re.ASCII)
_HEX_64_RE = re.compile(r"[0-9a-f]{64}", flags=re.ASCII)
_IDENTITY_TYPES = frozenset({"ein", "npi"})
_INVALID = "public_evidence_tic_tax_identity_binding_invalid"


class TicTaxIdentityBindingError(RuntimeError):
    pass


def _fail(code: str = _INVALID) -> TicTaxIdentityBindingError:
    return TicTaxIdentityBindingError(code)


@dataclass(frozen=True, slots=True, repr=False)
class _AuthenticatedShadowRow:
    provider_group_global_id_128: bytes
    identity_type: Literal["ein", "npi"]
    locator: bytes
    full_hmac: bytes


_RECEIPT_FIELDS = (
    "binding_contract",
    "release_contract_ref",
    "source_binding_sha256",
    "identity_type",
    "token_policy_ref",
    "provider_group_ref",
    "locator",
    "full_hmac",
    "serving_authority",
    "authorization_granted",
    "publication_enabled",
)


@dataclass(frozen=True, slots=True, repr=False, init=False)
class _TicTaxIdentityBindingReceipt:
    binding_contract: str
    release_contract_ref: str
    source_binding_sha256: str
    identity_type: Literal["ein", "npi"]
    token_policy_ref: str
    provider_group_ref: str
    locator: str
    full_hmac: str
    serving_authority: Literal["none"]
    authorization_granted: Literal[False]
    publication_enabled: Literal[False]
    receipt_ref: str

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        raise _fail()


def _strict_sha256(value: object) -> str:
    if type(value) is not str or _HEX_64_RE.fullmatch(value) is None:
        raise _fail()
    return value


def _strict_prefixed_hex(value: object, prefix: str, pattern: re.Pattern[str]) -> str:
    if (
        type(value) is not str
        or not value.startswith(prefix)
        or pattern.fullmatch(value[len(prefix) :]) is None
    ):
        raise _fail()
    return value


def _provider_group_ref(group_id: bytes) -> str:
    return "pg1_" + hashlib.sha256(_GROUP_REFERENCE_DOMAIN + group_id).hexdigest()


def _receipt_digest(fields: dict[str, object]) -> str:
    if (
        type(fields) is not dict
        or any(type(name) is not str for name in fields)
        or frozenset(fields) != frozenset(_RECEIPT_FIELDS)
    ):
        raise _fail()
    encoded = json.dumps(
        fields, ensure_ascii=True, separators=(",", ":"), sort_keys=True
    ).encode("ascii")
    digest = hashlib.sha256(
        _RECEIPT_DOMAIN + len(encoded).to_bytes(8, "big") + encoded
    ).hexdigest()
    return "tbr1_" + digest


def _validated_receipt_fields(receipt_candidate: object) -> dict[str, object]:
    if type(receipt_candidate) is not _TicTaxIdentityBindingReceipt:
        raise _fail()
    if (
        type(receipt_candidate.binding_contract) is not str
        or receipt_candidate.binding_contract
        != TAX_IDENTITY_SHADOW_SOURCE_BINDING_CONTRACT
        or type(receipt_candidate.identity_type) is not str
        or receipt_candidate.identity_type not in _IDENTITY_TYPES
        or type(receipt_candidate.serving_authority) is not str
        or receipt_candidate.serving_authority != "none"
        or receipt_candidate.authorization_granted is not False
        or receipt_candidate.publication_enabled is not False
    ):
        raise _fail()
    receipt_fields_by_name = {
        "binding_contract": receipt_candidate.binding_contract,
        "release_contract_ref": _strict_prefixed_hex(
            receipt_candidate.release_contract_ref, "src1_", _HEX_64_RE
        ),
        "source_binding_sha256": _strict_sha256(
            receipt_candidate.source_binding_sha256
        ),
        "identity_type": receipt_candidate.identity_type,
        "token_policy_ref": _strict_prefixed_hex(
            receipt_candidate.token_policy_ref, "tip1_", _HEX_64_RE
        ),
        "provider_group_ref": _strict_prefixed_hex(
            receipt_candidate.provider_group_ref, "pg1_", _HEX_64_RE
        ),
        "locator": _strict_prefixed_hex(receipt_candidate.locator, "til1_", _HEX_32_RE),
        "full_hmac": _strict_prefixed_hex(
            receipt_candidate.full_hmac, "tih1_", _HEX_64_RE
        ),
        "serving_authority": receipt_candidate.serving_authority,
        "authorization_granted": receipt_candidate.authorization_granted,
        "publication_enabled": receipt_candidate.publication_enabled,
    }
    if not hmac.compare_digest(
        receipt_fields_by_name["locator"][5:],
        receipt_fields_by_name["full_hmac"][5:37],
    ):
        raise _fail()
    return receipt_fields_by_name


def _validate_tic_tax_identity_binding_receipt(
    value: object,
) -> _TicTaxIdentityBindingReceipt:
    """Validate proof integrity and fixed no-authority state only."""
    try:
        receipt_fields_by_name = _validated_receipt_fields(value)
        receipt_ref = _strict_prefixed_hex(value.receipt_ref, "tbr1_", _HEX_64_RE)
        if not hmac.compare_digest(
            receipt_ref, _receipt_digest(receipt_fields_by_name)
        ):
            raise _fail()
        return value
    except TicTaxIdentityBindingError:
        raise
    except Exception:
        raise _fail() from None


def _issue_tic_tax_identity_binding_receipt(
    release: PublicEvidenceSourceReleaseDescriptor,
    bundle: admission.TaxIdentityShadowBundleDescriptor,
    policy_id: str,
    authenticated_row: _AuthenticatedShadowRow,
) -> _TicTaxIdentityBindingReceipt:
    """Issue only from the exact row object returned by held-file authentication."""
    if (
        type(release) is not PublicEvidenceSourceReleaseDescriptor
        or type(bundle) is not admission.TaxIdentityShadowBundleDescriptor
        or type(policy_id) is not str
        or type(authenticated_row) is not _AuthenticatedShadowRow
        or type(authenticated_row.provider_group_global_id_128) is not bytes
        or len(authenticated_row.provider_group_global_id_128) != 16
        or type(authenticated_row.identity_type) is not str
        or authenticated_row.identity_type not in _IDENTITY_TYPES
        or type(authenticated_row.locator) is not bytes
        or len(authenticated_row.locator) != 16
        or type(authenticated_row.full_hmac) is not bytes
        or len(authenticated_row.full_hmac) != 32
        or authenticated_row.full_hmac == bytes(32)
        or not hmac.compare_digest(
            authenticated_row.locator, authenticated_row.full_hmac[:16]
        )
    ):
        raise _fail()
    receipt_fields_by_name = {
        "binding_contract": TAX_IDENTITY_SHADOW_SOURCE_BINDING_CONTRACT,
        "release_contract_ref": "src1_" + release.contract_sha256,
        "source_binding_sha256": bundle.binding_sha256,
        "identity_type": authenticated_row.identity_type,
        "token_policy_ref": "tip1_" + token_policy_descriptor_sha256(policy_id),
        "provider_group_ref": _provider_group_ref(
            authenticated_row.provider_group_global_id_128
        ),
        "locator": "til1_" + authenticated_row.locator.hex(),
        "full_hmac": "tih1_" + authenticated_row.full_hmac.hex(),
        "serving_authority": "none",
        "authorization_granted": False,
        "publication_enabled": False,
    }
    receipt = object.__new__(_TicTaxIdentityBindingReceipt)
    for name in _RECEIPT_FIELDS:
        object.__setattr__(receipt, name, receipt_fields_by_name[name])
    object.__setattr__(receipt, "receipt_ref", _receipt_digest(receipt_fields_by_name))
    return _validate_tic_tax_identity_binding_receipt(receipt)


def _validate_tic_binding_for_record(
    receipt_candidate: object,
    release: PublicEvidenceSourceReleaseDescriptor,
    *,
    identity_type: str,
    token_policy_ref: str,
    provider_group_ref: str,
    locator: str,
    full_hmac: str,
) -> _TicTaxIdentityBindingReceipt:
    """Bind an internal proof to one record without granting authority."""
    receipt = _validate_tic_tax_identity_binding_receipt(receipt_candidate)
    binding = release.source_binding
    binding_values = (token_policy_ref, provider_group_ref, locator, full_hmac)
    if (
        type(release.source_kind) is not str
        or release.source_kind != "tic"
        or binding is None
        or type(identity_type) is not str
        or receipt.identity_type != identity_type
        or any(type(binding_value) is not str for binding_value in binding_values)
        or not hmac.compare_digest(
            receipt.release_contract_ref, "src1_" + release.contract_sha256
        )
        or not hmac.compare_digest(
            receipt.source_binding_sha256, binding.binding_sha256
        )
        or not hmac.compare_digest(receipt.token_policy_ref, token_policy_ref)
        or not hmac.compare_digest(receipt.provider_group_ref, provider_group_ref)
        or not hmac.compare_digest(receipt.locator, locator)
        or not hmac.compare_digest(receipt.full_hmac, full_hmac)
    ):
        raise _fail()
    return receipt
