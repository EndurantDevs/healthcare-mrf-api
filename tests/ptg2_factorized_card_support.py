# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Shared fixtures for release-wide factorized provider-card tests."""

from __future__ import annotations

import json
from dataclasses import replace
from types import SimpleNamespace

from api.plan_pricing_projection_contract import PROJECTION_CONTRACT
from tests.test_plan_release_serving import (
    _network_binding,
    _release_selection,
)


def card_pagination(*, limit=2, offset=0):
    """Return the minimal pagination object consumed by serving helpers."""

    return SimpleNamespace(limit=limit, offset=offset, page=1, source="page")


class MappingResult:
    """Expose SQLAlchemy's mapping-result surface over fixed rows."""

    def __init__(self, row_list):
        self.row_list = row_list

    def mappings(self):
        """Return the mapping-result facade."""

        return self

    def all(self):
        """Return every retained mapping row."""

        return self.row_list


def provider_cell_row(npi=101, zip5="60601"):
    """Return one internally consistent immutable provider-cell row."""

    fragment_by_field = {
        "npi": npi,
        "provider_name": f"Frozen provider {npi}",
        "entity_type_code": 1,
        "credential": "MD",
        "taxonomy_code": "207X00000X",
        "primary_specialty": "Orthopaedic Surgery",
        "classification": "Orthopaedic Surgery",
        "city": "Chicago",
        "state": "IL",
        "zip5": zip5,
    }
    return {
        "npi": npi,
        "geo_cell": zip5,
        "entity_type_code": 1,
        "taxonomy_codes": ["207X00000X"],
        "fragment": json.dumps(fragment_by_field).encode(),
    }


def candidate_row(
    npi=101,
    minimum_rate="10.00",
    **overrides_by_field,
):
    """Return one candidate row carrying consistent prefix metadata."""

    candidate_by_field = {
        "npi": npi,
        "minimum_rate": minimum_rate,
        "total_lower_bound": 2,
        "profile_exhausted": False,
        "unread_minimum_rate": "20.00",
        "boundary_rate": "10.00",
        "provider_set_count": 2,
        "membership_count": 3,
        "profiles_valid": True,
    }
    candidate_by_field.update(overrides_by_field)
    return candidate_by_field


def completion_row(
    npi=101,
    minimum_rate="10.00",
    maximum_rate="30.00",
    rate_count=4,
    **overrides_by_field,
):
    """Return one exact completion row with a frozen provider fragment."""

    zip5 = overrides_by_field.pop("zip5", "60601")
    completion_by_field = {
        **provider_cell_row(npi, zip5),
        "minimum_rate": minimum_rate,
        "maximum_rate": maximum_rate,
        "rate_count": rate_count,
        "membership_count": 3,
        "provider_set_count": 2,
        "rate_value_count": 7,
        "profiles_valid": True,
    }
    completion_by_field.update(overrides_by_field)
    return completion_by_field


def factorized_selection(*, binding_count=1, contract=PROJECTION_CONTRACT):
    """Bind a release with any number of immutable in-network bindings."""

    binding_list = [
        _network_binding(
            binding_ordinal,
            f"ptg2:209901:{binding_ordinal}",
            f"network-{binding_ordinal}",
            plan_id=f"plan-{binding_ordinal}",
        )
        for binding_ordinal in range(binding_count)
    ]
    return replace(
        _release_selection(*binding_list),
        pricing_projection_id="f" * 64,
        pricing_projection_contract=contract,
    )
