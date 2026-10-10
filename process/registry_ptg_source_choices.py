# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact retained producer choices; graph labels never imply legal ownership."""

from types import SimpleNamespace
from uuid import UUID

from sqlalchemy import text

from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.ptg_parts.result_archive_source_authority import (
    PTG_PUBLISHED_RESULT_SOURCE_AUTHORITY_CONTRACT,
    prepare_ptg_result_archive_source_authority,
)
from process.registry_ptg_cohort_authority import _GRAPH_FIELDS, _resolved_source_state
from process.registry_ptg_producer_scope import (
    RegistryPTGProducerScopeError,
    _canonical,
    _digest,
    _protected_store,
    _selected_versions,
    read_registry_ptg_producer_scope,
)

_MAX_BYTES = 65536


async def _verified_scope_choice(session, schema_name, ownership, approval_record, store):
    """Reconstruct original approval and independently recheck its selected files."""
    document = approval_record["approval_json"]
    scope_id = UUID(str(approval_record["scope_id"]))
    specification = SimpleNamespace(
        capture_id=str(scope_id),
        ptg_schema_name=schema_name,
        snapshot_id=ownership["snapshot_id"],
        binding_source_key=ownership["source_key"],
        company_key=document["company_key"],
        cohort_id=document["cohort_id"],
    )
    source_state, assignments, _binding = await _resolved_source_state(session, specification)
    if source_state["import_run_id"] != ownership["engine_run_id"]:
        raise RegistryPTGProducerScopeError("registry_ptg_scope_source_changed")
    graph_by_field = {name: source_state[name] for name in _GRAPH_FIELDS - {"source_assignments_sha256"}}
    graph_by_field["source_assignments_sha256"] = _digest(assignments)
    # Verify original closed statement, digest, exact archive, graph and selected files.
    scope_authority = (
        await prepare_ptg_result_archive_source_authority(
            session,
            schema_name=schema_name,
            operation_id="registry_ptg_capture_" + scope_id.hex,
            snapshot_id=ownership["snapshot_id"],
        )
    ).as_dict()
    receipt = await read_registry_ptg_producer_scope(
        session,
        specification,
        scope_id=scope_id,
        client_id=ownership["client_id"],
        coordinates=RegistryNetworkSourceCoordinates(**document["coordinates"]),
        frozen_authority=scope_authority,
        graph_identity=graph_by_field,
        store=store,
    )
    retained_ownership = receipt.get("operator_review", {}).get("source_ownership")
    if retained_ownership is not None and retained_ownership != ownership:
        raise RegistryPTGProducerScopeError("registry_ptg_scope_source_changed")
    await _selected_versions(session, specification, receipt["file_versions"], ownership=ownership)
    if receipt["approval_sha256"] != approval_record["approval_sha256"]:
        raise RegistryPTGProducerScopeError("registry_ptg_scope_source_changed")
    choice_by_field = {
        name: receipt[name]
        for name in (
            "scope_id",
            "company_key",
            "cohort_id",
            "legal_company_id",
            "approved_revision",
            "file_versions",
            "coordinates",
            "approval_sha256",
        )
    }
    choice_by_field["graph_identity"] = graph_by_field
    return choice_by_field


async def _published_choices_page(session, schema_name, ownership, authority):
    from process.registry_ptg_published_plan_source import published_plan_inventory

    inventory_by_field = await published_plan_inventory(
        session, schema_name, ownership, operation_id=authority["operation_id"]
    )
    if inventory_by_field["identity"]["import_run_id"] != ownership["engine_run_id"]:
        raise RegistryPTGProducerScopeError("registry_ptg_scope_source_changed")
    page_by_field = {
        "source": {
            "ptg_schema_name": schema_name,
            "snapshot_id": ownership["snapshot_id"],
            "binding_source_key": ownership["source_key"],
        },
        "items": [],
        "next_cursor": None,
        "published_plan": {
            "published_identity": inventory_by_field["identity"],
            "plan_scopes": inventory_by_field["plan_scopes"],
            "file_versions": inventory_by_field["file_versions"],
            "selection_mode": inventory_by_field["selection_mode"],
        },
        "review_status": "fresh_client_company_network_review_required",
    }
    if len(_canonical(page_by_field).encode("utf-8")) > _MAX_BYTES:
        raise RegistryPTGProducerScopeError("registry_ptg_scope_source_changed")
    return page_by_field


async def read_registry_ptg_source_choices(session, schema_name, ownership, selection, store):
    """Recheck durable approvals in the caller's restricted stable transaction."""
    table = await _protected_store(session, store, write=False)
    # An observation operation is not a fabricated scope or approval identity.
    authority = (
        await prepare_ptg_result_archive_source_authority(
            session,
            schema_name=schema_name,
            operation_id="registry_ptg_inventory_" + _digest(ownership),
            snapshot_id=ownership["snapshot_id"],
        )
    ).as_dict()
    if authority["snapshot_id"] != ownership["snapshot_id"] or authority["source_key"] != ownership["source_key"]:
        raise RegistryPTGProducerScopeError("registry_ptg_scope_source_changed")
    if authority["contract"] == PTG_PUBLISHED_RESULT_SOURCE_AUTHORITY_CONTRACT:
        return await _published_choices_page(session, schema_name, ownership, authority)
    if authority["source_file_import_id"] != ownership["source_file_import_id"]:
        raise RegistryPTGProducerScopeError("registry_ptg_scope_source_changed")
    approval_records = (
        (
            await session.execute(
                text(f"""
        SELECT scope_id, approval_sha256, approval_json FROM {table}
        WHERE approval_json->>'source_file_import_id'=:source_file_import_id
          AND approval_json->>'client_id'=:client_id
          AND approval_json->>'snapshot_id'=:snapshot_id
          AND (CAST(:after AS uuid) IS NULL OR scope_id>CAST(:after AS uuid))
        ORDER BY scope_id LIMIT :maximum
    """),
                {
                    **{name: ownership[name] for name in ("source_file_import_id", "client_id", "snapshot_id")},
                    "after": selection["after"],
                    "maximum": selection["limit"] + 1,
                },
            )
        )
        .mappings()
        .all()
    )
    return await _bounded_approved_choices(session, schema_name, ownership, selection, store, approval_records)


async def _bounded_approved_choices(session, schema_name, ownership, selection, store, approval_records):
    """Retain original verified frozen choices and complete bounded cursor accounting."""
    choice_items = []
    for approval_record in approval_records[: selection["limit"]]:
        choice_by_field = await _verified_scope_choice(session, schema_name, ownership, approval_record, store)
        choice_items.append(choice_by_field)
        page_by_field = {
            "source": {
                "ptg_schema_name": schema_name,
                "snapshot_id": ownership["snapshot_id"],
                "binding_source_key": ownership["source_key"],
            },
            "items": choice_items,
            "next_cursor": choice_by_field["scope_id"],
            "published_plan": None,
            "review_status": "fresh_client_and_company_authorization_required",
        }
        if len(_canonical(page_by_field).encode("utf-8")) > _MAX_BYTES:
            choice_items.pop()
            break
    if not choice_items and approval_records:
        raise RegistryPTGProducerScopeError("registry_ptg_scope_source_changed")
    return {
        "source": {
            "ptg_schema_name": schema_name,
            "snapshot_id": ownership["snapshot_id"],
            "binding_source_key": ownership["source_key"],
        },
        "items": choice_items,
        "next_cursor": choice_items[-1]["scope_id"] if len(approval_records) > len(choice_items) else None,
        "published_plan": None,
        "review_status": "fresh_client_and_company_authorization_required",
    }
