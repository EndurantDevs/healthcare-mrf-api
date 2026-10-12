# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Consume protected published-plan evidence for prepared exact offices only."""

import json
from dataclasses import asdict

from sqlalchemy import text

from process.network_address_projection import _identifier
from process.network_approved_membership_source import pin_approved_membership_source
from process.network_approved_source_bindings import APPROVED_NETWORK_BINDINGS_SQL, RegistryNetworkSourceCoordinates
from process.registry_ptg_published_plan_contract import RegistryPTGPublishedPlanSourceSpecification


def is_published_office_context(context):
    """Recognize the explicit published-plan source without inventing cohort labels."""
    return type(context.source_specification) is RegistryPTGPublishedPlanSourceSpecification


async def require_published_office_path(session):
    """Refuse unsafe authority resolution before any published capture SQL."""
    if not session.in_transaction():
        raise ValueError("registry_ptg_published_office_transaction_unavailable")
    search_path = (await session.execute(text("SELECT pg_catalog.current_setting('search_path')"))).scalar_one()
    if type(search_path) is not str or [name.strip() for name in search_path.split(",")] != ["pg_catalog", "pg_temp"]:
        raise ValueError("registry_ptg_published_office_path_unavailable")


def _graph_identity(identity_by_field):
    return {
        "snapshot_key": identity_by_field["snapshot_key"],
        "layout_generation": "shared_blocks_v4",
        "layout_mapping_sha256": identity_by_field["layout_mapping_digest"],
        "map_sha256": identity_by_field["map_digest"],
        "finalizer_map_sha256": identity_by_field["finalizer_map_digest"],
        "source_assignments_sha256": identity_by_field["source_assignments_sha256"],
    }


def _require_context(context, document_by_field, graph_by_field):
    command_by_field = document_by_field["command"]
    identity_by_field = command_by_field["published_identity"]
    specification = context.source_specification
    if (
        type(context.coordinates) is not RegistryNetworkSourceCoordinates
        or asdict(context.coordinates) != command_by_field["coordinates"]
        or specification.scope_id != command_by_field["scope_id"]
        or specification.ptg_schema_name != command_by_field["source"]["ptg_schema_name"]
        or specification.snapshot_id != identity_by_field["snapshot_id"]
        or specification.binding_source_key != identity_by_field["source_key"]
        or context.frozen_authority != document_by_field["evidence"]["source_authority"]
        or context.graph_identity != graph_by_field
        or (context.scope_store.control_schema or context.control_schema) != context.control_schema
    ):
        raise ValueError("registry_ptg_published_office_source_changed")


def _office_scope(document_by_field, actual_graph, selected_keys, *, approved_revision=None):
    command_by_field = document_by_field["command"]
    identity_by_field = command_by_field["published_identity"]
    scope_by_field = {
        name: command_by_field[name]
        for name in ("review_type", "scope_id", "plan_id", "plan_market_type", "selection_mode")
    }
    scope_by_field.update(
        snapshot_id=identity_by_field["snapshot_id"], approval_sha256=document_by_field["approval_sha256"]
    )
    return {
        **{
            name: command_by_field[name]
            for name in (
                "scope_id",
                "client_id",
                "legal_company_id",
                "network_id",
                "approved_revision",
                "file_versions",
                "coordinates",
            )
        },
        "approved_revision": (
            command_by_field["approved_revision"] if approved_revision is None else approved_revision
        ),
        "approval_sha256": document_by_field["approval_sha256"],
        "source_scope": scope_by_field,
        "binding_source_key": identity_by_field["source_key"],
        "snapshot_id": identity_by_field["snapshot_id"],
        "evidence": {
            "graph_identity": actual_graph,
            "selected_dense_source_keys": selected_keys,
            "source_authority": document_by_field["evidence"]["source_authority"],
        },
    }


async def _require_network_binding(session, context, scope_by_field):
    """Require one reviewed exact source mapping in the current immutable map."""
    from process.registry_ptg_office_capture import _driver

    driver = await _driver(session)
    namespace = _identifier(context.control_schema)
    revision = await driver.fetchval(f"SELECT approved_revision FROM {namespace}.registry_revision_control WHERE id=1")
    approved = await pin_approved_membership_source(
        driver, approved_revision=revision, control_schema=context.control_schema
    )
    fragment = APPROVED_NETWORK_BINDINGS_SQL.format(namespace=namespace)
    matches = await driver.fetchval(
        "WITH "
        + fragment
        + """ SELECT count(*)=1 AND coalesce(bool_and(
          record_json->>'source_key'=$9 AND network_id=$10 AND approved_network_key IS NOT NULL
          AND record_json->>'evidence_id'='published-plan-review:'||$11
          AND record_json->>'evidence_sha256'=$12),false)
          FROM approved_network_bindings WHERE record_json->'source_scope_json'=$8::jsonb""",
        approved.approved_revision,
        *context.coordinates.sql_parameters,
        json.dumps(scope_by_field["source_scope"]),
        scope_by_field["binding_source_key"],
        scope_by_field["network_id"],
        scope_by_field["source_scope"]["scope_id"],
        scope_by_field["source_scope"]["approval_sha256"],
    )
    if matches is not True:
        raise ValueError("registry_ptg_published_office_network_unresolved")
    return approved.approved_revision


async def published_office_scope(session, context):
    """Revalidate review, original source pin and current-company fence on the caller session."""
    from process.registry_ptg_cohort_authority import _source_state
    from process.registry_ptg_published_plan_scope import _approved_association

    await require_published_office_path(session)
    document_by_field = await _review_reader(context)(
        session,
        scope_id=context.scope_id,
        client_id=context.client_id,
        approval_sha256=context.scope_approval_sha256,
        store=context.scope_store,
    )
    command_by_field = document_by_field["command"]
    graph_by_field = _graph_identity(command_by_field["published_identity"])
    _require_context(context, document_by_field, graph_by_field)
    initial_scope = _office_scope(
        document_by_field, graph_by_field, document_by_field["evidence"]["selected_source_keys"]
    )
    approved_revision = await _require_network_binding(session, context, initial_scope)
    await _approved_association(
        session,
        _identifier(context.control_schema) + ".registry_ptg_published_plan_scope",
        {**command_by_field, "approved_revision": approved_revision},
    )
    actual_graph, source_records = await _source_state(session, context.source_specification, graph_by_field)
    selected_keys = document_by_field["evidence"]["selected_source_keys"]
    if selected_keys != [source_record["source_key"] for source_record in source_records]:
        raise ValueError("registry_ptg_published_office_source_changed")
    return _office_scope(document_by_field, actual_graph, selected_keys, approved_revision=approved_revision)


def _review_reader(context):
    from process.registry_ptg_office_approval import RegistryPTGOfficeApprovalContext, _approved_source_review
    from process.registry_ptg_published_plan_scope import read_registry_ptg_published_plan_scope

    return (
        _approved_source_review
        if type(context) is RegistryPTGOfficeApprovalContext
        else read_registry_ptg_published_plan_scope
    )
