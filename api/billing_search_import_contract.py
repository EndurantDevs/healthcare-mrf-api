# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Sealed configured-import ordering for exact billing provider candidates."""

from __future__ import annotations

import hashlib
import hmac
import secrets
from dataclasses import dataclass

from api.billing_search_transport_contract import _canonical_json_bytes
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError

_ORDER_SECRET = secrets.token_bytes(32)
_ORDER_DOMAIN = b"BILLING_SEARCH_COMPOSED_ORDER_V1\x00"


def _invalid() -> PTG2ManifestArtifactError:
    return PTG2ManifestArtifactError("billing search import composition is unavailable")


def _strict_digest(value: object) -> None:
    if (
        type(value) is not str
        or len(value) != 64
        or value == "0" * 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise _invalid()


@dataclass(frozen=True, slots=True, repr=False)
class BillingSearchImportCursorScope:
    """Validated query, authority and immutable import generation coordinates."""

    query_fingerprint_sha256: str
    authorization_scope_sha256: str
    generation_bundle_sha256: str

    def __post_init__(self) -> None:
        for value in (
            self.query_fingerprint_sha256,
            self.authorization_scope_sha256,
            self.generation_bundle_sha256,
        ):
            _strict_digest(value)

    def as_dict(self) -> dict[str, str]:
        """Return only validated immutable coordinates for signature binding."""

        self.__post_init__()
        return {
            "query_fingerprint_sha256": self.query_fingerprint_sha256,
            "authorization_scope_sha256": self.authorization_scope_sha256,
            "generation_bundle_sha256": self.generation_bundle_sha256,
        }


def _canonical_keys(keys: object) -> tuple[tuple[int | float | str, ...], ...]:
    from api.ptg2_billing_search_page import validate_billing_search_sort_key

    if type(keys) is not tuple:
        raise _invalid()
    validated_keys = tuple(validate_billing_search_sort_key(key) for key in keys)
    if keys != validated_keys or len(keys) != len(set(keys)):
        raise _invalid()
    return validated_keys


def _order_signature(candidate_keys, import_scope) -> bytes:
    if type(import_scope) is not BillingSearchImportCursorScope:
        raise _invalid()
    payload = _canonical_json_bytes({"candidate_keys": candidate_keys, "import_scope": import_scope.as_dict()})
    return hmac.digest(_ORDER_SECRET, _ORDER_DOMAIN + payload, hashlib.sha256)


@dataclass(frozen=True, slots=True, repr=False, init=False)
class BillingSearchComposedOrder:
    """Process-sealed full candidate identities in configured query order."""

    candidate_keys: tuple[tuple[int | float | str, ...], ...]
    import_scope: BillingSearchImportCursorScope
    _signature: bytes

    def __post_init__(self) -> None:
        try:
            _canonical_keys(self.candidate_keys)
            expected = _order_signature(self.candidate_keys, self.import_scope)
            if type(self._signature) is not bytes or not hmac.compare_digest(self._signature, expected):
                raise _invalid()
        except AttributeError, TypeError, ValueError:
            raise _invalid() from None


def _new_billing_search_composed_order(candidate_keys, import_scope) -> BillingSearchComposedOrder:
    """Seal only a complete order returned by the authorized SQL composition."""

    candidate_keys = _canonical_keys(candidate_keys)
    signature = _order_signature(candidate_keys, import_scope)
    order = object.__new__(BillingSearchComposedOrder)
    object.__setattr__(order, "candidate_keys", candidate_keys)
    object.__setattr__(order, "import_scope", import_scope)
    object.__setattr__(order, "_signature", signature)
    return order


def validate_billing_search_composed_order(candidate_keys, composed_order, *, complete=False) -> None:
    """Reprove a complete candidate order or an increasing page subsequence."""

    if composed_order is None:
        if candidate_keys != tuple(sorted(set(candidate_keys))):
            raise _invalid()
        return
    candidate_keys = _canonical_keys(candidate_keys)
    if type(composed_order) is not BillingSearchComposedOrder:
        raise _invalid()
    composed_order.__post_init__()
    if complete:
        if candidate_keys != composed_order.candidate_keys:
            raise _invalid()
        return
    positions_by_key = {key: ordinal for ordinal, key in enumerate(composed_order.candidate_keys)}
    positions = tuple(positions_by_key.get(key) for key in candidate_keys)
    if any(position is None for position in positions) or positions != tuple(sorted(positions)):
        raise _invalid()
