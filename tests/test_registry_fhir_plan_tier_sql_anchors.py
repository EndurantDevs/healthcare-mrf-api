# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Typed membership rendering rejects template drift before extracting source rows."""

from unittest.mock import patch

import pytest

from process import network_fhir_membership_source as membership
from tests.test_network_fhir_source_witness_recipe import _source_recipe
from tests.test_registry_fhir_plan_tier_bindings import witness_pin

_COMMON_ANCHORS = (
    "), plan_networks AS MATERIALIZED (",
    "        LEFT JOIN LATERAL (\n          SELECT CASE WHEN reference ~ '^Organization/",
    "        ) networks ON true",
    " AS malformed\n        FROM page",
    "        FROM expanded\n        LEFT JOIN",
)
_COMPLETE = "network.resource_id IS NOT NULL AND network_evidence.resource_id IS NOT NULL"
_INCOMPLETE = "network.resource_id IS NULL OR network_evidence.resource_id IS NULL"
_APPROVED_JOIN = "ON approved.network_resource_id=network.resource_id"
_APPROVED_ANCHORS = (
    "network.resource_id AS fact_source_key",
    "FROM fhir_product_references WHERE network_resource_id IS NOT NULL\n)",
    "AND $14::text IS NOT NULL",
)


def _base_sql(pin, *, batch, approved_only=False):
    with patch.object(membership, "_plan_tier_membership_sql", side_effect=lambda query, *args, **kwargs: query):
        if batch:
            return membership._extraction_sql(pin, '"synthetic_registry"', True, approved_only)
        return membership._source_witness_sql_v2(pin)


@pytest.mark.parametrize("reviewed,approved_only", [(False, False), (True, False), (True, True)])
@pytest.mark.parametrize("typed", [False, True])
def test_batch_modes_preserve_optional_approval_fragments(reviewed, approved_only, typed):
    pin = witness_pin() if typed else _source_recipe().source_pin
    query = membership._extraction_sql(pin, '"synthetic_registry"', reviewed, approved_only)
    assert ("tier_evidence AS MATERIALIZED" in query) is typed
    assert ("approved_network_bindings AS MATERIALIZED" in query) is reviewed
    if typed:
        assert "coalesce(tier.source_key,network.resource_id) AS fact_source_key" in query
        assert "approved.record_json->'source_scope_json'=tier.scope" in query
        assert query.count("OR coalesce(tier.valid,false)") == (2 if approved_only else 0)
        assert ("ON approved.network_resource_id=coalesce(tier.source_key,network.resource_id)" in query) == (
            not approved_only
        )


@pytest.mark.parametrize("typed", [False, True])
def test_witness_mode_stays_independent_of_approved_bindings(typed):
    pin = witness_pin() if typed else _source_recipe().source_pin
    query = membership._source_witness_sql_v2(pin)
    assert ("tier_evidence AS MATERIALIZED" in query) is typed
    assert "approved_network_bindings" not in query
    if typed:
        assert query.count("OR coalesce(tier.valid,false)") == 1
        assert "coalesce(tier.scope,jsonb_build_object('organization_id',expanded.network_resource_id" in query


@pytest.mark.parametrize("batch,approved_only", [(True, False), (True, True), (False, False)])
@pytest.mark.parametrize("copies", [0, 2], ids=["missing", "duplicate"])
def test_required_membership_anchors_fail_closed(batch, approved_only, copies):
    pin = witness_pin()
    query = _base_sql(pin, batch=batch, approved_only=approved_only)
    organization, capture = ("network.resource_id", "$17") if batch else ("expanded.network_resource_id", "$10")
    scope = f"jsonb_build_object('organization_id',{organization},'capture_sha256',{capture}::text)"
    anchors = [*_COMMON_ANCHORS, scope]
    if approved_only or not batch:
        anchors.append(_COMPLETE)
    if approved_only:
        anchors.append(_INCOMPLETE)
    if batch:
        anchors.extend(_APPROVED_ANCHORS)
        if not approved_only:
            anchors.append(_APPROVED_JOIN)
    for anchor in anchors:
        assert anchor in query
        changed = query.replace(anchor, anchor * copies)
        with pytest.raises(membership.FHIRMembershipSourceError, match="SQL anchors are invalid"):
            membership._plan_tier_membership_sql(changed, pin, batch=batch, approved_only=approved_only)


@pytest.mark.parametrize(
    "batch,approved_only,anchor",
    [(True, False, _COMPLETE), (True, False, _INCOMPLETE), (False, False, _INCOMPLETE), (True, True, _APPROVED_JOIN)],
)
def test_mode_specific_absence_is_checked_exactly(batch, approved_only, anchor):
    pin = witness_pin()
    query = _base_sql(pin, batch=batch, approved_only=approved_only)
    assert anchor not in query
    with pytest.raises(membership.FHIRMembershipSourceError, match="SQL anchors are invalid"):
        membership._plan_tier_membership_sql(query + anchor, pin, batch=batch, approved_only=approved_only)


@pytest.mark.parametrize("batch", [False, True])
def test_other_sources_skip_plan_tier_template_guards(batch):
    query = "synthetic unchanged query"
    assert membership._plan_tier_membership_sql(query, _source_recipe().source_pin, batch=batch) == query
