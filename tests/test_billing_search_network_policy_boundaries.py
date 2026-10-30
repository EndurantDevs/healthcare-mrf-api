# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Managed billing refuses broader authority and keeps retained source lineage."""

import json
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import billing_search_network_policy as policy
from api import ptg2_serving as native
from api.network_address_scope import network_address_read_scope
from api.ptg2_billing_search_contract import BillingSearchServingUnavailableError
from tests.billing_search_service_support import selection
from tests.network_read_execution_support import with_read_identity_receipts
from tests.ptg2_billing_geo_reader_support import NPI_A, NPI_B, _location_row
from tests.ptg2_serving_coverage_paydown_support import FakeResult, FakeSession
from tests.test_billing_search_network_policy import _scope
from tests.test_ptg2_serving_billing_address_lineage import _location_query


@pytest.mark.parametrize("damage", ["foreign_source", "foreign_snapshot", "untyped_selection", "list"])
def test_untrusted_contributor_coordinates_cannot_enter_managed_traversal(damage):
    release = selection(binding_count=2)
    bindings = release.in_network_bindings[1:]
    if damage == "foreign_source":
        bindings = (replace(bindings[0], source_key="different-source"),)
    elif damage == "foreign_snapshot":
        bindings = (replace(bindings[0], snapshot_id="different-snapshot"),)
    elif damage == "untyped_selection":
        release = SimpleNamespace(in_network_bindings=release.in_network_bindings)
    else:
        bindings = list(bindings)
    with pytest.raises(BillingSearchServingUnavailableError):
        policy.admitted_coordinates(release, bindings)


def test_unmanaged_scope_has_no_managed_digest_and_missing_policy_refuses():
    release = selection()
    assert policy.network_scope_sha256(release, release.in_network_bindings) is None
    with pytest.raises(BillingSearchServingUnavailableError):
        policy._retained_location_sql(native, _location_query(address_assurance_sql="addr.is_assured"))
    with _scope() as manifest:
        with network_address_read_scope(manifest, None, access_scope_sha256="b" * 64, excluded_network_ids=()):
            with pytest.raises(BillingSearchServingUnavailableError):
                policy.network_scope_sha256(release, release.in_network_bindings)


@pytest.mark.parametrize("damage", ["live_alias", "knn", "taxonomy", "unassured"])
def test_office_renderer_refuses_paths_that_could_escape_retained_assurance(damage):
    context = _location_query(address_assurance_sql="addr.is_assured")
    changes_by_kind = {
        "live_alias": {"address_table": "other.entity_address_unified"},
        "knn": {"knn_order_sql": "addr.location <-> :point"},
        "taxonomy": {"taxonomy_index_sql": "unscoped_taxonomy"},
        "unassured": {"address_assurance_sql": "TRUE"},
    }
    with _scope(), pytest.raises(BillingSearchServingUnavailableError):
        policy._retained_location_sql(native, replace(context, **changes_by_kind[damage]))


@pytest.mark.asyncio
async def test_empty_offices_do_not_read_lineage_and_template_drift_refuses(monkeypatch):
    session = FakeSession([])
    await policy._retained_lineage(session, native, [])
    assert session.calls == []
    monkeypatch.setattr(native, "_ADDRESS_PROVENANCE_SQL_TEMPLATE", "SELECT $unified_relation,$unified_relation")
    with _scope(), pytest.raises(BillingSearchServingUnavailableError):
        await policy._retained_lineage(session, native, [_location_row(NPI_A)])
    assert session.calls == []


@pytest.mark.asyncio
async def test_missing_managed_office_context_has_no_unscoped_fallback(monkeypatch):
    query = AsyncMock(return_value=None)
    execute = AsyncMock(side_effect=AssertionError("unexpected office read"))
    monkeypatch.setattr(native, "_membership_location_query", query)
    monkeypatch.setattr(native, "_execute_membership_location_sql", execute)
    with _scope():
        assert await policy.load_managed_billing_location_rows(object(), object(), {}, (NPI_A,)) is None
    execute.assert_not_awaited()


def _lineage_row(npi, source_id):
    return {
        "location_key": f"{npi:064x}",
        "source_id": source_id,
        "source_record_key": f"synthetic:{npi}",
        "source_snapshot_id": "20260101",
        "observed_at": "2026-01-01T00:00:00+00:00",
    }


@pytest.mark.asyncio
async def test_retained_location_and_lineage_keep_only_the_admitting_source(monkeypatch):
    context = _location_query(address_assurance_sql="addr.is_assured")
    rows = [
        {**_location_row(npi), "_geo_evidence_level": "nppes_registry_address", "_geo_evidence_source_id": 1}
        for npi in (NPI_A, NPI_B)
    ]
    execute = AsyncMock(return_value=rows)
    monkeypatch.setattr(native, "_membership_location_query", AsyncMock(return_value=context))
    monkeypatch.setattr(native, "_execute_membership_location_sql", execute)
    session = with_read_identity_receipts(FakeSession([FakeResult([_lineage_row(NPI_A, 1), _lineage_row(NPI_B, 2)])]))
    with _scope(exclusions=(19,)) as manifest:
        actual = await policy.load_managed_billing_location_rows(session, object(), {}, (NPI_A, NPI_B))
    assert [entry["npi"] for entry in actual] == [NPI_A]
    evidence = json.loads(actual[0]["address_payload"])["address_provenance"]
    assert evidence[0]["source_record_id"] == f"synthetic:{NPI_A}" and evidence[0]["source_id"] == 1
    executed_context, query = execute.await_args.args[1:]
    assert executed_context.parameter_map["_network_excluded_ids"] == [19]
    assert "_network_excluded_ids" not in context.parameter_map
    assert f'"{manifest.schema_name}".entity_address_unified' in query
    arguments, _ = session.calls[0]
    assert f'"{manifest.schema_name}".entity_address_unified' in str(arguments[0])
    assert arguments[1]["admitted_source_ids"] == [1, 1]
    assert arguments[1]["stored_only"] is True and arguments[1]["strict_stored_identity"] is True


@pytest.mark.asyncio
@pytest.mark.parametrize("has_context", [False, True])
async def test_geo_reader_distinguishes_unavailable_from_authorized_empty(monkeypatch, has_context):
    from api import ptg2_billing_geo_reader as reader
    from tests.ptg2_billing_geo_reader_support import _provider_rate, _tables

    context = _location_query(address_assurance_sql="addr.is_assured") if has_context else None
    monkeypatch.setattr(native, "_membership_location_query", AsyncMock(return_value=context))
    execute = AsyncMock(return_value=[])
    monkeypatch.setattr(native, "_execute_membership_location_sql", execute)
    session = FakeSession([])
    with _scope():
        result = await reader.load_exact_billing_geo_witnesses(
            session, _tables(), provider_rate_witnesses=(_provider_rate(),), geo_args={"zip5": "25000"}
        )
    assert result.address_projection_available is has_context and result.witnesses == ()
    assert execute.await_count == int(has_context) and session.calls == []
