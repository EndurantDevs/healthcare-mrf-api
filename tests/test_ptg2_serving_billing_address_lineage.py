# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Billing-only address lineage and site-key serving tests."""

import json
from unittest.mock import AsyncMock

import pytest

from api import ptg2_serving as serving
from tests.ptg2_serving_coverage_paydown_support import FakeResult, FakeSession


def _location_query(*, knn_order_sql=None, address_assurance_sql="TRUE"):
    return serving._MembershipLocationQuery(
        address_table="mrf.entity_address_unified",
        npi_scope_table="mrf.ptg2_v3_npi_scope",
        filter_sql="npi_scope.snapshot_key = :shared_snapshot_key",
        parameter_map={"limit": 2},
        distance_sql="NULL::double precision",
        knn_order_sql=knn_order_sql,
        address_assurance_sql=address_assurance_sql,
    )


@pytest.mark.parametrize(
    "query_kwargs",
    (
        {},
        {"address_assurance_sql": "addr.is_assured"},
        {"knn_order_sql": "addr.location <-> :request_location"},
    ),
    ids=("standard", "unified-assured", "knn"),
)
def test_membership_location_sql_keeps_site_key_billing_only(query_kwargs) -> None:
    default_sql = serving._membership_location_sql(
        _location_query(**query_kwargs),
        limit=2,
        offset=0,
    )
    billing_sql = serving._membership_location_sql(
        _location_query(**query_kwargs),
        limit=2,
        offset=0,
        include_address_site_key=True,
    )

    assert "'address_site_key'" not in default_sql
    assert "'address_site_key', addr.premise_key::text" in billing_sql


@pytest.mark.asyncio
async def test_billing_lineage_requires_materialized_evidence(monkeypatch):
    monkeypatch.setattr(
        serving,
        "_is_relation_available",
        AsyncMock(return_value=False),
    )
    location_rows = [
        {
            "npi": 1990000122,
            "address_payload": json.dumps(
                {"location_key": "generation-bound-location"}
            ),
        }
    ]
    session = FakeSession([])

    provenance_status = await serving._hydrate_address_provenance(
        session,
        location_rows,
        use_stored_only=True,
    )

    assert provenance_status == "unavailable"
    assert session.calls == []
    assert location_rows[0]["npi"] == 1990000122


@pytest.mark.asyncio
async def test_billing_lineage_sets_stored_only_coherence_mode(monkeypatch):
    monkeypatch.setattr(
        serving,
        "_is_relation_available",
        AsyncMock(return_value=True),
    )
    location_rows = [
        {
            "npi": 1990000122,
            "_geo_evidence_level": "nppes_registry_address",
            "_geo_evidence_source_id": 1,
            "address_payload": json.dumps(
                {"location_key": "stored-only-location"}
            ),
        }
    ]
    session = FakeSession([FakeResult([])])

    provenance_status = await serving._hydrate_address_provenance(
        session,
        location_rows,
        use_stored_only=True,
    )

    assert provenance_status == "available"
    assert session.calls[0][0][1]["stored_only"] is True
    assert location_rows == []
