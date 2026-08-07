# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Opaque cursor proof returned only by authenticated billing cursor sealing."""

from __future__ import annotations

_INVALID = "billing_search_cursor_invalid"
_REDACTED = "<redacted-billing-search-cursor>"


class BillingSearchSealedPageCursor:
    """Opaque cursor and claimed state requiring AEAD verification before use."""

    __slots__ = ("__state", "__token")

    def __new__(cls, *_args, **_kwargs):
        raise ValueError(_INVALID)

    def __setattr__(self, attribute_name: str, attribute_value: object) -> None:
        del attribute_name, attribute_value
        raise TypeError(_INVALID)

    def __delattr__(self, attribute_name: str) -> None:
        del attribute_name
        raise TypeError(_INVALID)

    def __repr__(self) -> str:
        return _REDACTED

    __str__ = __repr__

    def __copy__(self) -> BillingSearchSealedPageCursor:
        return self

    def __deepcopy__(
        self,
        memo: dict[int, object],
    ) -> BillingSearchSealedPageCursor:
        memo[id(self)] = self
        return self

    def __reduce_ex__(self, protocol: int) -> object:
        del protocol
        raise ValueError(_INVALID)


def _mint_billing_search_sealed_page_cursor(
    token: str,
    state: object,
) -> BillingSearchSealedPageCursor:
    """Bundle a sealed token with the exact state supplied to the AEAD sealer."""

    if type(token) is not str:
        raise ValueError(_INVALID)
    sealed_cursor = object.__new__(BillingSearchSealedPageCursor)
    object.__setattr__(
        sealed_cursor,
        "_BillingSearchSealedPageCursor__token",
        token,
    )
    object.__setattr__(
        sealed_cursor,
        "_BillingSearchSealedPageCursor__state",
        state,
    )
    return sealed_cursor


__all__ = ["BillingSearchSealedPageCursor"]
