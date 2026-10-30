# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Allowed-role authority and retained SQL orchestration; no native DB claim."""

import asyncio
import json
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sanic.exceptions import ServiceUnavailable

from api import network_address_scope
from api import provider_allowed_amount_policy as allowed
from api.plan_pricing_projection_contract import PlanPricingProjectionUnavailable
from process.ptg_parts.result_archive_published_authority import PtgPublishedResultSourceAuthority
from process.ptg_parts.result_archive_source_authority import result_archive_manifest_sha256
from process.registry_ptg_published_plan_scope import _digest, _document
from tests import test_provider_service_source_policy as sources
from tests.test_pricing_api import (
    FakeResult,
    FakeSession,
    _allowed_only_release_selection,
    _build_allowed_amount_page_row,
    _build_in_network_allowed_evidence,
    _canonical_release_selection,
    pricing_module,
)
from tests.test_provider_group_plan_network_read import held_scope


def _index(source):
    return {
        "contract": "ptg2_allowed_amounts_v1",
        "arch_version": "postgres_binary_v3",
        "storage": "postgresql",
        "data_domain": "allowed_amounts",
        "source_key": source,
        "snapshot_scoped": True,
        "allowed_amount_evidence": True,
        "allowed_amount_payments": 2,
        "allowed_amount_provider_payments": 3,
    }


def _reviewed_allowed_source():
    mapping, review, manifest = sources._reviewed_source_original()
    manifest["allowed_amount_index"] = _index("source_example")
    command = review["document"]["command"]
    digest = result_archive_manifest_sha256(manifest)
    command["published_identity"]["snapshot_manifest_sha256"] = command["coordinates"]["edition_id"] = digest
    evidence = review["document"]["evidence"]
    evidence["source_authority"] = PtgPublishedResultSourceAuthority(
        evidence["source_authority"]["operation_id"], command["published_identity"]
    ).as_dict()
    review["document"] = _document(command, review["document"]["actor"], evidence)
    review["approval_sha256"] = _digest(review["document"])
    mapping["edition_id"] = digest
    mapping["source_scope_json"]["approval_sha256"] = mapping["evidence_sha256"] = review["approval_sha256"]
    return mapping, review, manifest


@pytest.fixture
def policy_fixture(monkeypatch):
    monkeypatch.setattr(sources, "_reviewed_source_original", sources._reviewed_source, raising=False)
    monkeypatch.setattr(sources, "_reviewed_source", _reviewed_allowed_source)
    fixture = sources._fixture(monkeypatch, excluded=(7,))
    binding = replace(fixture.binding, role="allowed_amounts")
    public = replace(binding, binding_ordinal=10, snapshot_id="public_allowed", source_key="public_source")
    fixture.selection = _canonical_release_selection((binding, public))
    fixture.public = public
    fixture.batches[0].append(
        {
            "snapshot_id": public.snapshot_id,
            "status": "published",
            "manifest": {"allowed_amount_index": _index(public.source_key)},
        }
    )
    fixture.ready = AsyncMock(return_value=True)
    monkeypatch.setattr(allowed, "current_network_address_scope", lambda: fixture.scope)
    monkeypatch.setattr(allowed, "has_allowed_amount_binding_coverage", fixture.ready)
    return fixture


@pytest.mark.asyncio
async def test_denied_allowed_role_precedes_readiness(policy_fixture):
    held = policy_fixture
    original = held.selection.bindings
    assert await allowed.admitted_allowed_amount_bindings(held.session, held.selection) == (held.public,)
    held.ready.assert_awaited_once_with(held.session, held.public)
    assert held.selection.bindings is original
    assert held.driver.fetchrow.await_count == 3


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "damage", ["pure_mapped", "digest", "missing_index", "wrong_source", "bool_count", "integer_flag"]
)
async def test_unproven_allowed_source_never_reads_payload(policy_fixture, damage):
    held = policy_fixture
    manifest = held.manifest
    changes_by_kind = {
        "pure_mapped": lambda: manifest.pop("serving_index"),
        "digest": lambda: manifest.update(changed=True),
        "missing_index": lambda: manifest.pop("allowed_amount_index"),
        "wrong_source": lambda: manifest["allowed_amount_index"].update(source_key="wrong"),
        "bool_count": lambda: manifest["allowed_amount_index"].update(allowed_amount_payments=True),
        "integer_flag": lambda: manifest["allowed_amount_index"].update(snapshot_scoped=1),
    }
    changes_by_kind[damage]()
    with pytest.raises(PlanPricingProjectionUnavailable):
        await allowed.admitted_allowed_amount_bindings(held.session, held.selection)
    held.ready.assert_not_awaited()


@pytest.mark.asyncio
async def test_allowed_admission_preserves_cancellation(policy_fixture):
    policy_fixture.driver.fetchrow.side_effect = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        await allowed.admitted_allowed_amount_bindings(policy_fixture.session, policy_fixture.selection)
    policy_fixture.ready.assert_not_awaited()


def HeldInputs():
    """Use the typed synthetic shape; this fixture establishes no native authority."""
    from process.network_em_provider_inputs import _ROLES, EMProviderInputRelation, EMProviderInputs

    return EMProviderInputs(
        tuple(
            EMProviderInputRelation(role, "held_inputs", role.replace(".", "_"), "example_generation", i + 1, i + 21)
            for i, role in enumerate(_ROLES)
        )
    )


@pytest.fixture
def route_fixture(monkeypatch):
    selection = _allowed_only_release_selection()
    denied = selection.allowed_amount_bindings[0]
    admitted = replace(denied, binding_ordinal=5, snapshot_id="allowed_public", source_key="allowed_public")
    selection = replace(selection, bindings=(denied, admitted))
    resolver = AsyncMock(return_value=selection)
    admission = AsyncMock(return_value=(admitted,))
    inputs = AsyncMock(return_value=HeldInputs())
    monkeypatch.setattr(pricing_module, "resolve_plan_release_serving", resolver)
    monkeypatch.setattr(allowed, "admitted_allowed_amount_bindings", admission)
    monkeypatch.setattr(allowed, "allowed_office_inputs", inputs)
    return SimpleNamespace(
        selection=selection, admitted=admitted, resolver=resolver, admission=admission, inputs=inputs
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("location", [{}, {"city": "Allowed Office"}, {"zip5": "00002"}, {"lat": "0", "long": "0"}])
async def test_managed_allowed_page_keeps_source_and_office_scope(route_fixture, location):
    held = route_fixture
    page = _build_allowed_amount_page_row(total=1)
    page.update(
        city="Allowed Office",
        zip5="00002",
        address_payload=json.dumps({"city": "Allowed Office", "state": "IL"}),
        source_rows_json=json.dumps(
            [{"source_key": held.admitted.source_key, "snapshot_id": held.admitted.snapshot_id}]
        ),
    )
    session = FakeSession(
        [FakeResult([page]), FakeResult([_build_in_network_allowed_evidence(source_key="allowed_public")])]
    )
    args_by_name = {
        "plan_release_id": held.selection.plan_release_id,
        "code": "99214",
        "code_system": "CPT",
        **location,
    }
    with held_scope() as manifest:
        document = await pricing_module._search_ptg_allowed_amount_evidence(
            session, args_by_name, SimpleNamespace(limit=1, offset=0, page=1)
        )
    held.resolver.assert_awaited_once_with(session, held.selection.plan_release_id, projection_only=True)
    held.admission.assert_awaited_once_with(session, held.selection)
    assert document["pagination"]["total"] == 1 and document["binding_set_digest"] == held.selection.binding_set_digest
    assert document["query"]["source_keys"] == ["allowed_public"]
    page_sql, parameters = session.executions[0][0]
    page_sql = str(page_sql)
    assert set(parameters["source_keys"]) == {"allowed_public"}
    assert set(parameters["snapshot_ids"]) == {"allowed_public"}
    assert f'"{manifest.schema_name}".entity_address_unified' in page_sql
    assert page_sql.index("AND allowed_location.npi IS NOT NULL") < page_sql.index("LIMIT :limit")
    assert '"held_inputs"."npi" npi_data' in page_sql
    assert parameters["_network_excluded_ids"] == [7]
    if location:
        assert '"held_inputs"."npi_address"' in page_sql
        assert "geo_assurance_state" not in page_sql
    detail_parameters = session.executions[1][0][1]
    assert set(detail_parameters["snapshot_ids"]) == {"allowed_public"}


@pytest.mark.asyncio
async def test_zero_allowed_contributors_returns_exact_empty_without_data(route_fixture):
    held = route_fixture
    held.admission.return_value = ()
    session = FakeSession()
    with held_scope():
        document = await pricing_module._search_ptg_allowed_amount_evidence(
            session,
            {"plan_release_id": held.selection.plan_release_id, "code": "99214"},
            SimpleNamespace(limit=1, offset=0, page=1),
        )
    assert document["pagination"]["total"] == 0 and document["sources"] == []
    assert document["query"]["snapshot_ids"] == []
    assert session.executions == []
    held.inputs.assert_not_awaited()


@pytest.mark.asyncio
async def test_source_or_dependency_failure_has_no_allowed_fallback(route_fixture):
    held = route_fixture
    session = FakeSession()
    args_by_name = {"plan_release_id": held.selection.plan_release_id, "code": "99214"}
    held.admission.side_effect = PlanPricingProjectionUnavailable("unsupported role proof")
    with held_scope(), pytest.raises(ServiceUnavailable, match="sources"):
        await pricing_module._search_ptg_allowed_amount_evidence(
            session, args_by_name, SimpleNamespace(limit=1, offset=0)
        )
    assert session.executions == []
    held.admission.side_effect = None
    held.inputs.side_effect = PlanPricingProjectionUnavailable("stale provider dependency")
    with held_scope(), pytest.raises(ServiceUnavailable, match="offices"):
        await pricing_module._search_ptg_allowed_amount_evidence(
            session, args_by_name, SimpleNamespace(limit=1, offset=0)
        )
    assert session.executions == []


@pytest.mark.asyncio
@pytest.mark.parametrize("supplied", [False, True])
async def test_missing_managed_release_never_becomes_public_fallback(route_fixture, supplied):
    held = route_fixture
    held.resolver.return_value = None
    session = FakeSession()
    with held_scope(), pytest.raises(ServiceUnavailable, match="release"):
        await pricing_module._search_ptg_allowed_amount_evidence(
            session,
            {"plan_release_id": held.selection.plan_release_id, "code": "99214"},
            SimpleNamespace(limit=1, offset=0),
            **({"release_selection": None} if supplied else {}),
        )
    held.admission.assert_not_awaited()
    held.inputs.assert_not_awaited()
    assert session.executions == []
