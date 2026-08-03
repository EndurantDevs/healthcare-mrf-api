# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PostgreSQL assertions for the public billing-association loader."""

from __future__ import annotations

from typing import Any

from api.ptg2_billing_associations import load_provider_group_billing_associations


async def assert_billing_loader_contract(
    connection: Any,
    schema_name: str,
) -> None:
    """Prove active and legacy sidecar states through the real SQL loader."""

    billing_associations = await load_provider_group_billing_associations(
        connection,
        schema_name=schema_name,
        snapshot_key=11,
        provider_group_refs=("1" * 32, "2" * 32, "3" * 32, "4" * 32),
    )
    legacy_associations = await load_provider_group_billing_associations(
        connection,
        schema_name=schema_name,
        snapshot_key=19,
        provider_group_refs=("9" * 32,),
    )

    assert billing_associations["1" * 32]["billing_entity_ref"].startswith(
        "be1_"
    )
    assert [
        billing_associations[str(ordinal) * 32]["tax_identity_status"]
        for ordinal in range(1, 5)
    ] == ["matched_ein", "missing", "malformed", "unsupported_type"]
    assert legacy_associations["9" * 32]["tax_identity_status"] == "unavailable"
