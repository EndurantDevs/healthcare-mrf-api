# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Deterministic identity for one finite InsurancePlan census."""

from __future__ import annotations

import hashlib
import json

from process.provider_directory_rooted_graph_contract import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_CONTRACT_ID,
)


def insurance_plan_census_sha256(
    advertised_total: int,
    terminal_page_count: int,
    resource_json_rows: tuple[tuple[str, str], ...],
) -> str:
    """Hash a sorted finite plan census and its pagination witnesses."""

    identity_by_field = {
        "advertised_total": advertised_total,
        "contract_id": PROVIDER_DIRECTORY_ROOTED_GRAPH_CONTRACT_ID,
        "resources": [
            {
                "resource_id": resource_id,
                "sha256": hashlib.sha256(resource_json.encode("utf-8")).hexdigest(),
            }
            for resource_id, resource_json in resource_json_rows
        ],
        "terminal_page_count": terminal_page_count,
    }
    canonical_identity = json.dumps(
        identity_by_field,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(canonical_identity.encode("utf-8")).hexdigest()


__all__ = ("insurance_plan_census_sha256",)
