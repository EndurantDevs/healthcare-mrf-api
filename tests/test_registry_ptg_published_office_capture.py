# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pure published-office dispatch; physical native authority is not mocked acceptance."""

from dataclasses import asdict
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import registry_ptg_cohort_authority as authority
from process import registry_ptg_office_capture as capture
from process import registry_ptg_published_office_scope as published
from process import registry_ptg_published_plan_scope as review
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.registry_ptg_published_plan_contract import RegistryPTGPublishedPlanSourceSpecification
from tests.test_registry_published_plan_binding_refs import reviewed_binding


def _fixture():
    binding_by_field, review_by_field = reviewed_binding()
    document_by_field = review_by_field["document"]
    document_by_field["approval_sha256"] = review_by_field["approval_sha256"]
    document_by_field["evidence"]["selected_source_keys"] = [0]
    command_by_field = document_by_field["command"]
    specification = RegistryPTGPublishedPlanSourceSpecification(
        command_by_field["scope_id"], "synthetic_ptg", "snapshot_example", "source_example"
    )
    context = SimpleNamespace(
        source_specification=specification,
        scope_id=command_by_field["scope_id"],
        client_id=command_by_field["client_id"],
        scope_approval_sha256=review_by_field["approval_sha256"],
        scope_store=SimpleNamespace(control_schema="synthetic_control"),
        control_schema="synthetic_control",
        coordinates=RegistryNetworkSourceCoordinates(**command_by_field["coordinates"]),
        frozen_authority=document_by_field["evidence"]["source_authority"],
        graph_identity=published._graph_identity(command_by_field["published_identity"]),
    )
    session = SimpleNamespace(
        in_transaction=lambda: True,
        execute=AsyncMock(return_value=SimpleNamespace(scalar_one=lambda: "pg_catalog,pg_temp")),
    )
    return session, context, document_by_field


def test_original_review_operation_pin_has_no_cohort_labels():
    _, context, _ = _fixture()
    assert authority._source_specification(context.source_specification) == "registry_ptg_published_review_" + str(
        context.scope_id
    ).replace("-", "")
    assert set(asdict(context.source_specification)) == {
        "scope_id",
        "ptg_schema_name",
        "snapshot_id",
        "binding_source_key",
    }


@pytest.mark.asyncio
async def test_published_dispatch_uses_same_session_and_current_fence(monkeypatch):
    session, context, document_by_field = _fixture()
    reader = AsyncMock(return_value=document_by_field)
    fence = AsyncMock()
    graph = AsyncMock(return_value=(context.graph_identity, [{"source_key": 0}]))
    monkeypatch.setattr(review, "read_registry_ptg_published_plan_scope", reader)
    monkeypatch.setattr(review, "_approved_association", fence)
    monkeypatch.setattr(authority, "_source_state", graph)
    scope_by_field = await capture._scope(session, context)
    assert (
        reader.await_args.args[0] is session
        and fence.await_args.args[0] is session
        and graph.await_args.args[0] is session
    )
    assert "company_key" not in scope_by_field and "cohort_id" not in scope_by_field
    assert scope_by_field["source_scope"]["plan_id"] == document_by_field["command"]["plan_id"]
    assert scope_by_field["evidence"]["selected_dense_source_keys"] == [0]
    context.graph_identity = {**context.graph_identity, "snapshot_key": 99}
    with pytest.raises(ValueError, match="source_changed"):
        await capture._scope(session, context)
    assert fence.await_count == 1


@pytest.mark.asyncio
async def test_noncanonical_or_missing_transaction_refuses_before_authority(monkeypatch):
    session, context, _ = _fixture()
    reader = AsyncMock()
    monkeypatch.setattr(review, "read_registry_ptg_published_plan_scope", reader)
    session.execute.return_value.scalar_one = lambda: "untrusted,pg_catalog"
    with pytest.raises(ValueError, match="path_unavailable"):
        await capture._scope(session, context)
    session.in_transaction = lambda: False
    with pytest.raises(ValueError, match="transaction_unavailable"):
        await published.require_published_office_path(session)
    assert reader.await_count == 0 and session.execute.await_count == 1


def test_published_ddl_null_labels_and_legacy_nonnull():
    assert "company_key text NOT NULL" in capture._ddl("synthetic_schema")[1]
    sql = capture._ddl("synthetic_schema", is_published=True)[1]
    assert "CHECK(company_key IS NULL AND cohort_id IS NULL)" in sql
    assert "company_key text NOT NULL" not in sql
