# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retained service identities refuse incomplete evidence before broad native reads."""

import datetime
import json
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from sanic.exceptions import InvalidUsage

from api import custom_import_provider_service_http as service
from api import provider_service_legacy_selection as legacy
from api import provider_service_source_policy as policy
from api.endpoint import pricing
from api.network_address_scope import current_network_address_scope
from api.plan_pricing_projection_contract import PlanPricingProjectionUnavailable
from process.ptg_parts.plan_rows import _ptg2_plan_rows
from process.registry_catalog_policy import RegistryOrganizationVisibility
from tests import test_provider_service_legacy_selection as fixtures
from tests.test_custom_import_managed_service import _signed_request
from tests.test_plan_pricing_state_scan_policy import _scope
from tests.test_provider_service_source_policy import _reviewed_source


def _native_read_boundary(monkeypatch, session):
    native = Mock(side_effect=AssertionError("Refused service selection reached native reads or counts"))
    monkeypatch.setattr(pricing, "list_providers_by_procedure", native)
    cache = Mock()
    request = SimpleNamespace(
        ctx=SimpleNamespace(sa_session=session), app=SimpleNamespace(ctx=SimpleNamespace(cache=cache))
    )
    return request, native, cache


@pytest.mark.asyncio
async def test_identifier_type_without_a_plan_cannot_fall_through_to_unfiltered_service(monkeypatch):
    session = SimpleNamespace(execute=AsyncMock(side_effect=AssertionError("No plan was selected")))
    request, native, cache = _native_read_boundary(monkeypatch, session)
    with _scope(), pytest.raises(InvalidUsage, match="require a source, snapshot or plan identity"):
        await service._service_page(request, native_args=fixtures._args(plan_id_type="hios"))
    session.execute.assert_not_called()
    native.assert_not_called()
    assert cache.mock_calls == [] and legacy.service_legacy_selection_sha256() is None


@pytest.mark.asyncio
async def test_uniquely_located_release_without_serving_bindings_never_falls_back(monkeypatch):
    selection = fixtures._selection()
    session = SimpleNamespace(
        execute=AsyncMock(
            side_effect=[
                SimpleNamespace(scalars=lambda: [selection.plan_release_id]),
                [SimpleNamespace(_mapping={"readable": True}) for _ in range(2)],
                [],
                SimpleNamespace(scalar_one=lambda: True),
                [],
            ]
        )
    )
    request, native, cache = _native_read_boundary(monkeypatch, session)
    with _scope(), pytest.raises(PlanPricingProjectionUnavailable, match="managed legacy release is unavailable"):
        await service._service_page(request, native_args=fixtures._args(source_key="source-allowed"))
    assert session.execute.await_count == 5
    assert "AS readable" in str(session.execute.await_args_list[1].args[0])
    assert "AS storage_present" in str(session.execute.await_args_list[2].args[0])
    assert session.execute.await_args.args[1]["plan_release_id"] == selection.plan_release_id
    native.assert_not_called()
    assert cache.mock_calls == [] and legacy.service_legacy_selection_sha256() is None


def _typed_evidence():
    selection = fixtures._selection()
    document_by_name = fixtures._type_document(selection)
    entry_by_name = document_by_name["bindings"][0]
    plan_by_name, _, _ = _ptg2_plan_rows(
        {"plan_id": entry_by_name["plan_id"], "plan_id_type": "EIN", "plan_market_type": "group"},
        entry_by_name["snapshot_id"],
        datetime.date(2026, 7, 1),
    )
    witness_by_name = entry_by_name | {
        "snapshot_status": "published",
        "snapshot_source": entry_by_name["source_key"],
        "witnessed_type": plan_by_name["plan_id_type"],
        "plan_hash": plan_by_name["plan_hash"],
        "canonical_payload": plan_by_name["canonical_payload"],
    }
    driver = SimpleNamespace(fetchrow=AsyncMock())
    session = SimpleNamespace(
        scalar=AsyncMock(return_value=json.dumps(document_by_name)),
        connection=AsyncMock(
            return_value=SimpleNamespace(
                get_raw_connection=AsyncMock(return_value=SimpleNamespace(driver_connection=driver))
            )
        ),
    )
    return selection, document_by_name, witness_by_name, driver, session


def _batch(rows):
    encoded = json.dumps(rows)
    return {"count": len(rows), "bytes": len(encoded.encode()), "rows_json": encoded}


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["missing_retained_plan", "foreign_canonical_plan"])
async def test_valid_type_commitment_does_not_authorize_missing_or_foreign_retained_plan(failure):
    selection, document_by_name, witness_by_name, driver, session = _typed_evidence()
    selectors_by_name = legacy._selectors(fixtures._args(plan_id_type="ein", source_key="source-allowed"))
    driver.fetchrow.return_value = _batch([witness_by_name])
    assert await legacy._typed_restriction(session, selection, selectors_by_name) == (
        frozenset({("in_network", 9)}),
        document_by_name["sha256"],
    )
    if failure == "missing_retained_plan":
        driver.fetchrow.return_value = _batch([])
    else:
        foreign, _, _ = _ptg2_plan_rows(
            {"plan_id": "88-0000002", "plan_id_type": "EIN", "plan_market_type": "group"},
            witness_by_name["snapshot_id"],
            datetime.date(2026, 7, 1),
        )
        witness_by_name.update(plan_hash=foreign["plan_hash"], canonical_payload=foreign["canonical_payload"])
        driver.fetchrow.return_value = _batch([witness_by_name])
    before = deepcopy(witness_by_name)
    with pytest.raises(PlanPricingProjectionUnavailable, match="identifier type evidence is unavailable"):
        await legacy._typed_restriction(session, selection, selectors_by_name)
    assert witness_by_name == before and driver.fetchrow.await_count == 2
    assert len(selection.bindings) == 2 and legacy.service_legacy_selection_sha256() is None


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["duplicate_snapshot", "foreign_source"])
async def test_complete_snapshot_census_refuses_duplicate_or_rebound_source(failure):
    binding = fixtures._selection().bindings[0]
    snapshot_by_name = {
        "snapshot_id": binding.snapshot_id,
        "status": "published",
        "manifest": {"serving_index": {"source_key": binding.source_key}},
    }
    driver = SimpleNamespace(fetchrow=AsyncMock(return_value=_batch([snapshot_by_name])))
    assert await policy._snapshot_manifests(driver, (binding,)) == {binding.snapshot_id: snapshot_by_name}
    if failure == "duplicate_snapshot":
        driver.fetchrow.return_value = _batch([snapshot_by_name, snapshot_by_name])
    else:
        snapshot_by_name["manifest"]["serving_index"]["source_key"] = "foreign-source"
        driver.fetchrow.return_value = _batch([snapshot_by_name])
    before = deepcopy(snapshot_by_name)
    with pytest.raises(ValueError, match="source snapshot (is incomplete|identity differs)"):
        await policy._snapshot_manifests(driver, (binding,))
    assert snapshot_by_name == before and driver.fetchrow.await_count == 2
    assert driver.fetchrow.await_args.args[1] == [binding.snapshot_id]


def test_stale_approved_row_revision_cannot_reclassify_a_reviewed_unit_as_public(monkeypatch):
    mapping_by_name, review_by_name, manifest_by_name = _reviewed_source()
    monkeypatch.setattr(policy, "PTG2_SCHEMA", review_by_name["document"]["command"]["source"]["ptg_schema_name"])
    reviews = policy._review_commands(json.dumps([review_by_name]))
    snapshot_id = mapping_by_name["source_scope_json"]["snapshot_id"]
    reviewed = policy._reviewed_units(reviews, {snapshot_id: {"manifest": manifest_by_name}})
    record_by_name = {
        "record_key": mapping_by_name["binding_id"],
        "record_revision": 1,
        "record_json": mapping_by_name,
        "association_active": True,
    }
    organization = RegistryOrganizationVisibility()
    assert policy._eligible_units([record_by_name], reviews, reviewed, (), organization_policy=organization) == reviewed
    record_by_name["record_revision"] = 2
    before = deepcopy((record_by_name, reviewed))
    with pytest.raises(ValueError, match="approved source mapping is malformed"):
        policy._eligible_units([record_by_name], reviews, reviewed, (), organization_policy=organization)
    assert (record_by_name, reviewed) == before


@pytest.mark.asyncio
async def test_oversized_service_envelope_never_opens_reader_or_uses_cache(monkeypatch):
    request = _signed_request()
    request.body += b" " * service.managed_collection.MAX_MANAGED_BODY_BYTES
    reader = Mock(side_effect=AssertionError("Oversized service envelope opened a reader"))
    cache = Mock()
    request.app = SimpleNamespace(ctx=SimpleNamespace(cache=cache))
    monkeypatch.setattr(service, "signed_request_reader", reader)
    monkeypatch.setattr(service.managed_collection, "managed_import_collection_scope", reader)
    reply = await service.serve_custom_import_provider_service(request, None)
    assert reply.status == 404 and reply.headers["Cache-Control"] == "private, no-store"
    reader.assert_not_called()
    assert cache.mock_calls == [] and current_network_address_scope() is None
