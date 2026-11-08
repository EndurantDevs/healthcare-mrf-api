# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed allowed listing dispatch and original-review pagination regressions."""

from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process import registry_ptg_allowed_plan_source as allowed
from process import registry_ptg_source_choices as choices
from process import registry_ptg_source_reviews as reviews
from process.ptg_parts.result_archive_allowed_authority import AllowedSourceAuthority
from process.registry_ptg_producer_scope import RegistryPTGProducerScopeError, _digest
from process.registry_ptg_published_plan_scope import _document
from tests.test_registry_allowed_plan_authority import allowed_case


def listing_case():
    _, stored, _, inventory_by_field = allowed_case()
    command = stored["document"]["command"]
    approval_by_field = {
        "scope_id": UUID(command["scope_id"]),
        "approval_json": stored["document"],
        "approval_sha256": stored["approval_sha256"],
        "company_name": "Reviewed company",
        "network_name": "Reviewed network",
        "association_current": True,
    }
    selection_by_field = {
        "client_id": command["client_id"],
        "source_file_import_id": command["source_file_import_id"],
        "after": None,
        "limit": 1,
    }
    return approval_by_field, inventory_by_field, command["ownership"], selection_by_field


@pytest.mark.asyncio
async def test_choices_resolve_allowed_role_without_negotiated_archive_fallback(monkeypatch):
    _, inventory_by_field, ownership_by_field, selection_by_field = listing_case()
    monkeypatch.setattr(choices, "_protected_store", AsyncMock())
    monkeypatch.setattr(allowed, "is_allowed_snapshot", AsyncMock(return_value=True))
    monkeypatch.setattr(allowed, "allowed_plan_inventory", AsyncMock(return_value=inventory_by_field))
    from process import registry_ptg_published_provisioning

    protected = AsyncMock()
    monkeypatch.setattr(registry_ptg_published_provisioning, "require_published_lock_privileges", protected)
    negotiated = AsyncMock(side_effect=AssertionError("unexpected negotiated fallback"))
    monkeypatch.setattr(choices, "prepare_ptg_result_archive_source_authority", negotiated)
    page = await choices.read_registry_ptg_source_choices(
        None, "synthetic_ptg", ownership_by_field, selection_by_field, object()
    )
    assert page["published_plan"]["review_type"] == "published_allowed_amount_plan"
    assert page["published_plan"]["selection_mode"] == "complete_allowed_snapshot_source_set"
    assert page["published_plan"]["published_identity"] == inventory_by_field["identity"]
    assert page["items"] == [] and page["next_cursor"] is None
    protected.assert_awaited_once_with(None, "synthetic_ptg", pin_writer=False)
    negotiated.assert_not_awaited()


def test_allowed_review_listing_reconstructs_original_command_and_pin():
    approval_by_field, inventory_by_field, ownership_by_field, selection_by_field = listing_case()
    page, authorities = reviews._bounded_reviews(
        [approval_by_field], "synthetic_ptg", ownership_by_field, selection_by_field, inventory_by_field
    )
    assert page["items"][0]["command"] == approval_by_field["approval_json"]["command"]
    assert page["items"][0]["approval"]["approval_sha256"] == approval_by_field["approval_sha256"]
    assert authorities[0].as_dict() == inventory_by_field["authority"]
    assert page["next_cursor"] is None


@pytest.mark.parametrize("damage", ["client", "source", "plan", "file", "digest", "association"])
def test_allowed_listing_never_relabels_changed_ownership_or_source(damage):
    approval_by_field, inventory_by_field, ownership_by_field, selection_by_field = listing_case()
    mutations_by_name = {
        "client": lambda: ownership_by_field.update(client_id="other_client"),
        "source": lambda: inventory_by_field["identity"].update(source_key="other_source"),
        "plan": lambda: inventory_by_field.update(plan_scopes=[{"plan_id": "OTHER", "plan_market_type": "group"}]),
        "file": lambda: inventory_by_field.update(file_versions=[]),
        "digest": lambda: approval_by_field.update(approval_sha256="f" * 64),
        "association": lambda: approval_by_field.update(association_current=False),
    }
    mutations_by_name[damage]()
    with pytest.raises((ValueError, RegistryPTGProducerScopeError)):
        reviews._bounded_reviews(
            [approval_by_field], "synthetic_ptg", ownership_by_field, selection_by_field, inventory_by_field
        )


def test_allowed_keyset_cursor_advances_only_over_returned_reviews():
    first, inventory_by_field, ownership_by_field, selection_by_field = listing_case()
    second = deepcopy(first)
    second["scope_id"] = UUID(int=100)
    command = second["approval_json"]["command"]
    command["scope_id"] = str(second["scope_id"])
    evidence_by_field = second["approval_json"]["evidence"]
    evidence_by_field["source_authority"] = AllowedSourceAuthority(
        "registry_ptg_published_review_" + second["scope_id"].hex, inventory_by_field["identity"]
    ).as_dict()
    second["approval_json"] = _document(command, second["approval_json"]["actor"], evidence_by_field)
    second["approval_sha256"] = _digest(second["approval_json"])
    page, authorities = reviews._bounded_reviews(
        [first, second], "synthetic_ptg", ownership_by_field, selection_by_field, inventory_by_field
    )
    assert len(page["items"]) == len(authorities) == 1
    assert page["next_cursor"] == str(first["scope_id"])


@pytest.mark.asyncio
@pytest.mark.parametrize("flag", [True, False, None, 1])
async def test_actual_role_discriminator_accepts_only_true_result(flag):
    session = SimpleNamespace(execute=AsyncMock(return_value=SimpleNamespace(scalar_one_or_none=lambda: flag)))
    assert await allowed.is_allowed_snapshot(session, "synthetic_ptg", "snapshot_example") is (flag is True)
    sql, parameters = session.execute.await_args.args
    assert "not_applicable" in str(sql) and "NOT manifest ? 'serving_index'" in str(sql)
    assert "manifest::jsonb AS manifest" in str(sql) and "WITH selected_snapshot AS MATERIALIZED" in str(sql)
    assert parameters == {"snapshot_id": "snapshot_example"}


def test_blank_allowed_market_remains_exact_and_negotiated_stays_nonempty():
    from process import registry_ptg_published_plan_contract as published
    from process import registry_published_plan_binding_refs as references
    from tests.test_registry_ptg_published_plan import intent

    approval_by_field, inventory_by_field, ownership_by_field, _ = listing_case()
    command = approval_by_field["approval_json"]["command"]
    command["plan_market_type"] = ""
    inventory_by_field["plan_scopes"][0]["plan_market_type"] = ""
    from process.registry_ptg_published_plan_scope import resolved_published_plan_inventory

    full, _, _ = resolved_published_plan_inventory(
        "synthetic_ptg",
        {name: command[name] for name in published.INTENT_FIELDS},
        ownership_by_field,
        inventory_by_field,
    )
    assert full["plan_market_type"] == ""
    scope_by_field = {
        name: full[name] for name in ("review_type", "scope_id", "plan_id", "plan_market_type", "selection_mode")
    }
    scope_by_field.update(snapshot_id=ownership_by_field["snapshot_id"], approval_sha256="a" * 64)
    assert references.validated_published_plan_binding_scope(scope_by_field)["plan_market_type"] == ""
    with pytest.raises(ValueError):
        published.validated_intent(intent() | {"plan_market_type": ""})


def test_real_allowed_query_columns_use_json_and_nullable_text_markets():
    from sqlalchemy import JSON, String
    from sqlalchemy.dialects.postgresql import JSONB

    from db.models import (
        PTG2AllowedAmountItem,
        PTG2AllowedAmountPlan,
        PTG2ImportRun,
        PTG2Snapshot,
        PTG2SourceFileVersion,
        PTG2SourceIdentity,
    )

    for model, column in ((PTG2Snapshot, "manifest"), (PTG2ImportRun, "options")):
        assert isinstance(model.__table__.c[column].type, JSON)
        assert not isinstance(model.__table__.c[column].type, JSONB)
    for model in (PTG2AllowedAmountPlan, PTG2AllowedAmountItem):
        assert isinstance(model.__table__.c.plan_market_type.type, String)
        assert model.__table__.c.plan_market_type.nullable
        assert isinstance(model.__table__.c.source_file_version_id.type, String)
    assert isinstance(PTG2SourceFileVersion.__table__.c.raw_sha256.type, String)
    assert isinstance(PTG2SourceFileVersion.__table__.c.source_identity_hash.type, String)
    assert isinstance(PTG2SourceIdentity.__table__.c.source_identity_hash.type, String)
