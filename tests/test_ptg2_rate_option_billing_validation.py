# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Billing-shaping checks for stable PTG rate-option references."""

from __future__ import annotations

import pytest

from api import ptg2_billing_associations as billing
from tests.ptg2_rate_option_ref_support import (
    synthetic_lineage_ref,
    synthetic_rate_option,
)


def test_attachment_rejects_a_tampered_rate_option_reference() -> None:
    """Reject a valid-looking reference that does not match its lineage."""

    provider_item_by_field = {
        "npi": 1234567890,
        "rate_options": [synthetic_rate_option(1, 1)],
    }
    provider_item_by_field["rate_options"][0]["rate_option_ref"] = (
        "ro1_" + "A" * 43
    )

    with pytest.raises(
        billing.PTG2BillingAssociationDataError,
        match="invalid rate option reference",
    ):
        billing.attach_billing_associations(
            [provider_item_by_field],
            {
                synthetic_lineage_ref(1): [
                    {
                        "provider_group_ref": "01" * 16,
                        "tax_identity_status": "missing",
                    }
                ]
            },
        )
