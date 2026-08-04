# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Synthetic, non-customer PTG rate-option fixtures."""

from __future__ import annotations

from api.ptg2_rate_option_refs import encode_rate_option_ref


def synthetic_lineage_ref(value: int) -> str:
    """Return one fixed-width lowercase-hex lineage fixture."""

    return f"{value:032x}"


def synthetic_rate_option(
    provider_set_ordinal: int,
    option_ordinal: int,
) -> dict[str, object]:
    """Build one valid atomic rate-option fixture."""

    provider_set_ref = synthetic_lineage_ref(provider_set_ordinal)
    price_set_ref = synthetic_lineage_ref(100 + option_ordinal)
    rate_pack_ref = synthetic_lineage_ref(200 + option_ordinal)
    return {
        "rate_option_ref": encode_rate_option_ref(
            provider_set_ref=provider_set_ref,
            price_set_ref=price_set_ref,
            rate_pack_ref=rate_pack_ref,
        ),
        "provider_set_ref": provider_set_ref,
        "price_set_ref": price_set_ref,
        "rate_pack_ref": rate_pack_ref,
        "prices": [{"negotiated_rate": option_ordinal * 100}],
    }
