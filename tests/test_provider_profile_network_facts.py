# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""CMS fact-input boundaries without claiming native source authority."""

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import provider_profile_network_facts as facts
from api.provider_directory_cms_network_scope import CMSNetworkScope, _mapped_networks
from api.provider_directory_entities_contract import DirectoryReadError
from process.network_fhir_membership_source import PinnedFHIRMembershipSource

NPI = "1000000004"
SITE = "11111111-1111-4111-8111-111111111111"


def row():
    return dict(
        network_id=42,
        npi=NPI,
        role_type="PractitionerRole",
        role_id="role-a",
        plan_id="plan-a",
        location_id=SITE,
        location_resource_id="office-a",
        evidence_id="a" * 64,
    )


def scope(excluded=None):
    pin = PinnedFHIRMembershipSource(
        "source", "cms-npd", "endpoint", "dataset", "a" * 64, "release", 7, "scope", "2026-01-01"
    )
    return CMSNetworkScope(
        SimpleNamespace(schema_name="network_candidate_" + "1" * 32),
        "approved_network_bindings AS (SELECT 1)",
        {},
        '"source"',
        pin,
        {},
        excluded,
    )


def test_default_policy_is_relational_and_never_an_all_id_inventory():
    default = _mapped_networks(scope((84,)))
    assert "NOT approved.network_id=ANY(CAST(:scope_excluded_network_ids AS integer[]))" in default
    assert "CAST(:scope_network_ids AS integer[]) IS NULL" in default
    selected = _mapped_networks(scope())
    assert "scope_excluded_network_ids" not in selected
    assert "approved.network_id=ANY(CAST(:scope_network_ids AS integer[]))" in selected


def test_cms_query_requires_exact_source_role_office_plan_before_transfer_aggregation():
    sql = facts._query(scope((84,)))
    for boundary in [
        "network_membership_source retained",
        "network_membership effective",
        "provider_location_binding office",
        "retained.location_id=evidenced.site_id",
        "effective.location_id=evidenced.site_id",
        "office.location_id=evidenced.site_id",
        "pairs.provider_system='npi'",
        "pairs.provider_id=:profile_npi",
        "pairs.site_id=CAST(:profile_location AS uuid)",
        "proof.network_resource_id=allowed.resource_id",
        "proof.plan_payload_sha256=plan_witness.raw_payload_sha256",
        "plan_witness.raw_payload_json::jsonb->>'resourceType'='InsurancePlan'",
        "plan.payload_json::jsonb->'network_refs'",
        "plan.dataset_id=:dataset_id",
        "proof.release_id=:release_id",
        "scope_excluded_network_ids",
        "LIMIT 5001",
    ]:
        assert boundary in sql
    assert sql.index("pairs.provider_id=:profile_npi") < sql.index("jsonb_agg(document")
    assert sql.index("profile_pairs.provider_id=:profile_npi") < sql.index("eligible AS MATERIALIZED")
    assert "$5" not in sql and "$6" not in sql
    assert "provider_directory_profile" not in sql and "inferred_npi" not in sql


def test_unaggregated_inputs_keep_exact_identity_without_unscoped_payload():
    rows = facts._fact_rows(json.dumps([row()]), NPI, (84,), None, SITE)
    assert rows == (row(),)
    assert "addresses" not in rows[0] and "plan_name" not in rows[0]


@pytest.mark.parametrize(
    "change",
    [
        "excluded",
        "unselected",
        "provider",
        "office",
        "nil-office",
        "malformed-office",
        "malformed-json",
        "role",
        "plan-ref",
        "evidence",
        "extra",
        "duplicate",
        "oversize",
        "null",
    ],
)
def test_refuses_mismatched_or_incomplete_fact_results(change):
    fact_by_field = row()
    changes_by_name = {
        "provider": {"npi": "1000000005"},
        "office": {"location_id": "22222222-2222-4222-8222-222222222222"},
        "nil-office": {"location_id": "00000000-0000-0000-0000-000000000000"},
        "malformed-office": {"location_id": "invalid"},
        "role": {"role_type": "InsurancePlan"},
        "plan-ref": {"plan_id": "InsurancePlan/plan-a"},
        "evidence": {"evidence_id": "g" * 64},
        "extra": {"addresses": ["hidden-office"]},
    }
    fact_by_field.update(changes_by_name.get(change, {}))
    excluded = (42,) if change == "excluded" else ()
    selected = (84,) if change == "unselected" else None
    document = json.dumps([fact_by_field] * (2 if change == "duplicate" else 1))
    documents_by_name = {"oversize": " " * (facts.MAX_BYTES + 1), "null": None, "malformed-json": "["}
    document = documents_by_name.get(change, document)
    with pytest.raises(DirectoryReadError) as caught:
        facts._fact_rows(document, NPI, excluded, selected, SITE)
    assert caught.value.status == 503


@pytest.mark.parametrize(
    "excluded,selected,location",
    [
        ([42], None, None),
        ((42, 42), None, None),
        ((0,), None, None),
        ((), (), None),
        ((), (84, 42), None),
        ((), None, "invalid"),
        ((), None, "00000000-0000-0000-0000-000000000000"),
    ],
)
def test_invalid_policy_selection_or_office_refuses_before_sql(excluded, selected, location):
    with pytest.raises(ValueError):
        facts._input(NPI, excluded, selected, location)


def test_profile_selection_preserves_the_native_hundred_network_limit():
    facts._input(NPI, (), tuple(range(1, 101)), None)
    with pytest.raises(ValueError):
        facts._input(NPI, (), tuple(range(1, 102)), None)


@pytest.mark.asyncio
async def test_reader_requires_an_existing_transaction_before_loading_any_source():
    session = SimpleNamespace(in_transaction=lambda: False, connection=AsyncMock())
    with pytest.raises(ValueError, match="profile_network_scope_invalid"):
        await facts.read_cms_profile_plan_inputs(session, object(), NPI, excluded_network_ids=())
    session.connection.assert_not_awaited()


@pytest.mark.asyncio
async def test_missing_exact_retained_recipe_refuses_before_fact_query(monkeypatch):
    from process.network_serving_read import PinnedNetworkServingManifest

    manifest = PinnedNetworkServingManifest(
        1, "11111111-1111-4111-8111-111111111111", "network_candidate_" + "1" * 32, 1, {}, 1, "a" * 64, 7
    )
    native = object()
    connection = SimpleNamespace(get_raw_connection=AsyncMock(return_value=SimpleNamespace(driver_connection=native)))
    session = SimpleNamespace(
        in_transaction=lambda: True, connection=AsyncMock(return_value=connection), execute=AsyncMock()
    )
    resolver = AsyncMock(return_value=())
    pair = AsyncMock()
    monkeypatch.setattr(facts, "resolve_retained_registry_source_recipes", resolver)
    monkeypatch.setattr(facts, "resolve_committed_registry_cms_source_pair", pair)
    with pytest.raises(DirectoryReadError) as caught:
        await facts.read_cms_profile_plan_inputs(session, manifest, NPI, excluded_network_ids=())
    assert caught.value.status == 503
    resolver.assert_awaited_once_with(native, manifest)
    pair.assert_not_awaited()
    session.execute.assert_not_awaited()
