# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Opaque cursor proof returned only by authenticated billing cursor sealing."""

from __future__ import annotations

_INVALID = "billing_search_cursor_invalid"
_REDACTED = "<redacted-billing-search-cursor>"


class BillingSearchSealedPageCursor:
    """Opaque proof that the page cursor was minted by the AEAD sealer."""

    __slots__ = ("__sort_key", "__token")

    def __new__(cls, *_args, **_kwargs):
        raise ValueError(_INVALID)

    def __setattr__(self, attribute_name: str, attribute_value: object) -> None:
        del attribute_name, attribute_value
        raise TypeError(_INVALID)

    def __delattr__(self, attribute_name: str) -> None:
        del attribute_name
        raise TypeError(_INVALID)

    @property
    def token(self) -> str:
        """Return the authenticated wire token for explicit response emission."""

        return self.__token

    @property
    def sort_key(self) -> tuple[int | float | str, ...]:
        """Return the exact provider position authenticated by the token."""

        return self.__sort_key

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
    sort_key: tuple[int | float | str, ...],
) -> BillingSearchSealedPageCursor:
    """Mint the proof after the cursor module validates both coordinates."""

    if type(token) is not str or type(sort_key) is not tuple:
        raise ValueError(_INVALID)
    sealed_cursor = object.__new__(BillingSearchSealedPageCursor)
    object.__setattr__(
        sealed_cursor,
        "_BillingSearchSealedPageCursor__token",
        token,
    )
    object.__setattr__(
        sealed_cursor,
        "_BillingSearchSealedPageCursor__sort_key",
        sort_key,
    )
    return sealed_cursor


__all__ = ["BillingSearchSealedPageCursor"]
