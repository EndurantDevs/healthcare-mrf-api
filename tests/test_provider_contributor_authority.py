# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Contributor company policy and selected networks precede every physical read."""

import copy
from contextlib import contextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from api import network_address_scope
from api import plan_release_serving as release
from api import provider_allowed_amount_policy as allowed
from api import provider_service_source_policy as source
from api import ptg2_serving as serving
from api.plan_pricing_projection_contract import PlanPricingProjectionUnavailable
from process.registry_catalog_policy import RegistryOrganizationVisibility
from process.registry_ptg_published_plan_scope import _digest, _document
from tests import test_provider_service_source_policy as fixtures
from tests.test_plan_release_serving import _release_selection
from tests.test_provider_allowed_amount_policy import policy_fixture
from tests.test_ptg2_plan_release_coverage_edges import _pagination


def _case(monkeypatch, role):
    if role == "allowed_amounts":
        held = policy_fixture.__wrapped__(monkeypatch)
    else:
        held = fixtures._fixture(monkeypatch)
        public = replace(held.binding, binding_ordinal=27, source_key="public_source", snapshot_id="public_snapshot")
        held.public = public
        held.selection = _release_selection(held.binding, public, validated_serving_tables=())
        held.batches[0].append(
            {
                "snapshot_id": public.snapshot_id,
                "status": "published",
                "manifest": {"serving_index": {"source_key": public.source_key}},
            }
        )
    monkeypatch.setattr(source, "current_network_address_scope", network_address_scope.current_network_address_scope)
    monkeypatch.setattr(allowed, "current_network_address_scope", network_address_scope.current_network_address_scope)
    held.role = role
    held.company = held.review["document"]["command"]["legal_company_id"]
    held.mapped = (
        held.selection.allowed_amount_bindings[0]
        if role == "allowed_amounts"
        else held.selection.in_network_bindings[0]
    )
    return held


@contextmanager
def _scope(held, *, companies=(), groups=(), networks=None, excluded=()):
    with network_address_scope.network_address_read_scope(
        held.scope.manifest,
        networks,
        access_scope_sha256="a" * 64,
        excluded_network_ids=excluded if networks is None else None,
        organization_policy=RegistryOrganizationVisibility(companies, groups),
    ):
        yield


async def _admit(held):
    function = (
        allowed.admitted_allowed_amount_bindings
        if held.role == "allowed_amounts"
        else source.admitted_plan_release_bindings
    )
    return await function(held.session, held.selection)


@pytest.mark.asyncio
@pytest.mark.parametrize("role", ["in_network", "allowed_amounts"])
@pytest.mark.parametrize("selected", [False, True])
async def test_hidden_company_denied_before_readiness_and_public_default_is_preserved(monkeypatch, role, selected):
    held = _case(monkeypatch, role)
    with _scope(held, companies=(held.company,), networks=(7,) if selected else None):
        admitted = await _admit(held)
    assert admitted == (() if selected else (held.public,))
    if role == "allowed_amounts":
        assert [call.args[1] for call in held.ready.await_args_list] == list(admitted)
    assert held.driver.fetchrow.await_count == 3
    assert held.selection.bindings[0] is held.mapped


@pytest.mark.asyncio
@pytest.mark.parametrize("role", ["in_network", "allowed_amounts"])
async def test_group_redaction_never_hides_independent_child_company(monkeypatch, role):
    held = _case(monkeypatch, role)
    with _scope(held, groups=(str(UUID(int=91)),)):
        assert await _admit(held) == (held.mapped, held.public)


@pytest.mark.asyncio
@pytest.mark.parametrize("role", ["in_network", "allowed_amounts"])
@pytest.mark.parametrize("selected,expected", [((7,), True), ((8,), False), ((7, 8), True)])
async def test_selected_admits_only_exact_reviewed_active_network_association(monkeypatch, role, selected, expected):
    held = _case(monkeypatch, role)
    with _scope(held, networks=selected):
        admitted = await _admit(held)
    assert admitted == ((held.mapped,) if expected else ())
    assert held.public not in admitted
    if role == "allowed_amounts":
        assert [call.args[1] for call in held.ready.await_args_list] == list(admitted)


@pytest.mark.asyncio
@pytest.mark.parametrize("role", ["in_network", "allowed_amounts"])
@pytest.mark.parametrize("damage", ["archived", "association", "unreviewed"])
async def test_selected_never_recovers_damaged_or_unreviewed_source_as_public(monkeypatch, role, damage):
    held = _case(monkeypatch, role)
    if damage == "archived":
        held.mapping["archived"] = True
    elif damage == "association":
        held.record["association_active"] = False
    else:
        held.batches[1].clear()
        held.batches[2].clear()
    with _scope(held, networks=(7,)):
        assert await _admit(held) == ()
    if role == "allowed_amounts":
        held.ready.assert_not_awaited()


def _visible_association(held):
    mapping = copy.deepcopy(held.record)
    review = copy.deepcopy(held.review)
    command = review["document"]["command"]
    command.update(scope_id=str(UUID(int=82)), statement_id=str(UUID(int=83)), legal_company_id=str(UUID(int=84)))
    review["document"] = _document(command, review["document"]["actor"], review["document"]["evidence"])
    review.update(scope_id=command["scope_id"], approval_sha256=_digest(review["document"]))
    mapping["record_key"] = str(UUID(int=85))
    mapping["record_json"].update(
        binding_id=mapping["record_key"],
        binding_key="f" * 64,
        evidence_id="published-plan-review:" + command["scope_id"],
        evidence_sha256=review["approval_sha256"],
    )
    mapping["record_json"]["source_scope_json"].update(
        scope_id=command["scope_id"], approval_sha256=review["approval_sha256"]
    )
    held.batches[1].append(review)
    held.batches[2].append(mapping)


@pytest.mark.asyncio
@pytest.mark.parametrize("role", ["in_network", "allowed_amounts"])
async def test_mixed_company_source_retains_only_its_independent_visible_association(monkeypatch, role):
    held = _case(monkeypatch, role)
    _visible_association(held)
    with _scope(held, companies=(held.company,), networks=(7,)):
        admitted = await _admit(held)
    assert admitted == (held.mapped,) and admitted[0] is held.mapped


@pytest.mark.asyncio
@pytest.mark.parametrize("role", ["in_network", "allowed_amounts"])
@pytest.mark.parametrize("damage", ["foreign", "duplicate", "list", "wrong_role"])
async def test_original_role_coordinates_cannot_be_forged_before_source_reads(monkeypatch, role, damage):
    held = _case(monkeypatch, role)
    bindings = (
        (replace(held.mapped, source_key="foreign"),)
        if damage == "foreign"
        else (held.mapped, held.mapped)
        if damage == "duplicate"
        else [held.mapped]
        if damage == "list"
        else (replace(held.mapped, role="other"),)
    )
    held.selection = SimpleNamespace(
        bindings=held.selection.bindings,
        **{"allowed_amount_bindings" if role == "allowed_amounts" else "in_network_bindings": bindings},
    )
    with _scope(held), pytest.raises(PlanPricingProjectionUnavailable):
        await _admit(held)
    held.driver.fetchrow.assert_not_awaited()
    if role == "allowed_amounts":
        held.ready.assert_not_awaited()


@pytest.mark.asyncio
async def test_actual_company_policy_precedes_native_readiness_count_and_page_limit(monkeypatch):
    held = _case(monkeypatch, "in_network")
    events = []
    descriptor = object()

    async def is_ready(session, binding, **options):
        assert held.driver.fetchrow.await_count == 3 and binding is held.public
        events.append("ready")
        options["validated_serving_tables_by_snapshot_id"][binding.snapshot_id] = descriptor
        return True

    async def native_page(session, snapshot, args, pagination, **options):
        assert events == ["ready"] and snapshot == held.public.snapshot_id and pagination.limit == 1
        assert options["serving_tables"] is descriptor
        events.append("count_and_limit")
        return {"items": [{"npi": 1073913877}], "pagination": {"total": 1}, "query": {}}

    monkeypatch.setattr(release, "is_release_binding_serving_ready", is_ready)
    monkeypatch.setattr(serving, "_search_one_ptg2_snapshot", native_page)
    with _scope(held, companies=(held.company,)):
        response = await serving._search_established_plan_release_index(
            held.session, {}, _pagination(limit=1), held.selection
        )
    assert events == ["ready", "count_and_limit"]
    assert response["pagination"]["total"] == 1 and response["in_network_snapshot_ids"] == [held.public.snapshot_id]
    assert len(held.selection.bindings) == 2 and held.selection._validated_serving_tables == ()


@pytest.mark.asyncio
async def test_all_denied_company_scope_never_reaches_readiness_count_or_limit(monkeypatch):
    held = _case(monkeypatch, "in_network")
    readiness = AsyncMock(side_effect=AssertionError("denied readiness"))
    page = AsyncMock(side_effect=AssertionError("denied count or page"))
    monkeypatch.setattr(release, "is_release_binding_serving_ready", readiness)
    monkeypatch.setattr(serving, "_search_one_ptg2_snapshot", page)
    with _scope(held, companies=(held.company,), networks=(7,)):
        response = await serving._search_established_plan_release_index(
            held.session, {}, _pagination(limit=1), held.selection
        )
    assert response["items"] == [] and response["pagination"]["total"] == 0
    assert response["in_network_snapshot_ids"] == [] and len(held.selection.bindings) == 2
    readiness.assert_not_awaited()
    page.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("role", ["in_network", "allowed_amounts"])
@pytest.mark.parametrize("damage", ["boolean_ordinal", "duplicate_coordinate", "blank_source", "market"])
async def test_malformed_original_recipe_cannot_reach_metadata(monkeypatch, role, damage):
    held = _case(monkeypatch, role)
    first, second = held.selection.bindings
    if damage == "boolean_ordinal":
        first = replace(first, binding_ordinal=True)
    elif damage == "duplicate_coordinate":
        second = replace(second, binding_ordinal=first.binding_ordinal)
    elif damage == "blank_source":
        first = replace(first, source_key=" ")
    else:
        first = replace(first, plan_market_type="GROUP")
    held.selection = _release_selection(first, second, validated_serving_tables=())
    with _scope(held), pytest.raises(PlanPricingProjectionUnavailable):
        await _admit(held)
    held.driver.fetchrow.assert_not_awaited()


@pytest.mark.asyncio
async def test_negotiated_blank_market_cannot_reach_metadata(monkeypatch):
    held = _case(monkeypatch, "in_network")
    first, second = held.selection.bindings
    held.selection = _release_selection(replace(first, plan_market_type=""), second, validated_serving_tables=())
    with _scope(held), pytest.raises(PlanPricingProjectionUnavailable):
        await _admit(held)
    held.driver.fetchrow.assert_not_awaited()
