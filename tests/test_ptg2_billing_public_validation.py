# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fail-closed public billing-association field validation."""

from __future__ import annotations

from typing import Any

import pytest

from api import ptg2_billing_response as billing


GROUP_REF = "1" * 32
ENTITY_REF = f"be1_{'a' * 64}"
UNAVAILABLE_REASON = "legacy_snapshot_without_tax_identity_sidecar"


def _matched_association() -> dict[str, Any]:
    return {
        "provider_group_ref": GROUP_REF,
        "tax_identity_status": "matched_ein",
        "tin_type": "ein",
        "billing_entity_ref": ENTITY_REF,
    }


def _unavailable_association() -> dict[str, Any]:
    return {
        "provider_group_ref": GROUP_REF,
        "tax_identity_status": "unavailable",
        "unavailable_reason": UNAVAILABLE_REASON,
    }


@pytest.mark.parametrize(
    "raw_association",
    [
        "not-a-mapping",
        {
            "provider_group_ref": GROUP_REF,
            "tax_identity_status": "missing",
            "tin_hmac_sha256": b"secret",
        },
        {"provider_group_ref": None, "tax_identity_status": "missing"},
        {"provider_group_ref": "1", "tax_identity_status": "missing"},
        {"provider_group_ref": "g" * 32, "tax_identity_status": "missing"},
        {**_matched_association(), "tin_type": "ssn"},
        {**_matched_association(), "billing_entity_ref": None},
        {**_matched_association(), "billing_entity_ref": "a" * 64},
        {**_matched_association(), "billing_entity_ref": "be1_short"},
        {**_matched_association(), "billing_entity_ref": f"be1_{'!' * 64}"},
        {**_matched_association(), "unavailable_reason": UNAVAILABLE_REASON},
        {
            "provider_group_ref": GROUP_REF,
            "tax_identity_status": "missing",
            "tin_type": "ein",
        },
        {**_unavailable_association(), "unavailable_reason": "unknown"},
        {**_unavailable_association(), "tin_type": "ein"},
        {**_unavailable_association(), "billing_entity_ref": ENTITY_REF},
        {"provider_group_ref": GROUP_REF, "tax_identity_status": "unknown"},
    ],
)
def test_public_billing_association_rejects_inconsistent_fields(
    raw_association: Any,
) -> None:
    with pytest.raises(billing.PTG2BillingAssociationDataError):
        billing._public_billing_association(
            raw_association,
            association_ordinal=1,
        )


def test_public_billing_association_replaces_group_hash_with_local_ordinal() -> None:
    provider_group_ref, public_association = billing._public_billing_association(
        _matched_association(),
        association_ordinal=2,
    )
    assert provider_group_ref == GROUP_REF
    assert public_association["association_ordinal"] == 2
    assert "provider_group_ref" not in public_association


@pytest.mark.parametrize("association_ordinal", [0, True, "1"])
def test_public_billing_association_rejects_invalid_local_ordinal(
    association_ordinal: Any,
) -> None:
    with pytest.raises(billing.PTG2BillingAssociationDataError, match="ordinal"):
        billing._public_billing_association(
            _matched_association(),
            association_ordinal=association_ordinal,
        )
