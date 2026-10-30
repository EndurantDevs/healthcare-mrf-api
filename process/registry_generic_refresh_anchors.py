# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Resolve exact approved generic captures without a Network serving-head dependency."""

from __future__ import annotations

import json
from uuid import UUID

import asyncpg
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from process.network_address_projection import _identifier
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.network_cms_registry_address_capture import native_driver
from process.registry_ptg_graph_reader import RegistryPTGGraphReadBudget
from process.registry_refresh_anchor_contract import (
    GENERIC_FHIR_PRODUCER,
    ApprovedRefreshBinding,
    RegistryRefreshAnchorMismatch,
    RegistryRefreshAnchorUnavailable,
)
from process.registry_source_selection_receipt import _unique_object

_PREFIX = "retained-normalized-fhir-v1:"
_MAX_CAPTURE_BYTES = 65536


def _capture_groups(bindings):
    if type(bindings) is not tuple or not 1 <= len(bindings) <= 1000:
        raise RegistryRefreshAnchorMismatch("registry_refresh_generic_binding_invalid")
    groups, identities = {}, set()
    for binding in bindings:
        if type(binding) is not ApprovedRefreshBinding or binding.binding_id in identities:
            raise RegistryRefreshAnchorMismatch("registry_refresh_generic_binding_invalid")
        record = binding.record_json
        if record["source_system"] != "fhir" or record["producer_id"] != GENERIC_FHIR_PRODUCER:
            raise RegistryRefreshAnchorMismatch("registry_refresh_generic_producer_invalid")
        coordinates = RegistryNetworkSourceCoordinates(
            **{name: record[name] for name in RegistryNetworkSourceCoordinates.__dataclass_fields__}
        )
        identities.add(binding.binding_id)
        groups.setdefault(coordinates, []).append(binding)
    if len(groups) > 100:
        raise RegistryRefreshAnchorUnavailable("registry_refresh_generic_capture_bound")
    return groups


async def read_original_generic_capture_metadata(driver, coordinates, read_budget):
    """Locate and authenticate closed native metadata without issuing a content-read capability."""
    from process.network_fhir_retained_capture import require_retained_fhir_capture
    from process.network_fhir_retained_capture_contract import validate_capture

    if (
        type(coordinates) is not RegistryNetworkSourceCoordinates
        or coordinates.source_system != "fhir"
        or coordinates.producer_id != GENERIC_FHIR_PRODUCER
        or type(read_budget) is not RegistryPTGGraphReadBudget
        or not driver.is_in_transaction()
    ):
        raise RegistryRefreshAnchorUnavailable("registry_refresh_generic_capture_unavailable")
    edition = UUID(coordinates.edition_id)
    if not edition.int or str(edition) != coordinates.edition_id:
        raise RegistryRefreshAnchorUnavailable("registry_refresh_generic_capture_unavailable")
    read_budget.reserve(({"raw_byte_count": _MAX_CAPTURE_BYTES},))
    header = await driver.fetchrow(
        "SELECT oid::bigint AS schema_oid,CASE WHEN octet_length(obj_description(oid,'pg_namespace'))<=$2 "
        "THEN obj_description(oid,'pg_namespace') END AS capture_comment "
        "FROM pg_namespace WHERE nspname=$1",
        "registry_fhir_capture_" + edition.hex,
        _MAX_CAPTURE_BYTES + len(_PREFIX),
    )
    comment = None if header is None else header["capture_comment"]
    if type(comment) is not str or not comment.startswith(_PREFIX):
        raise RegistryRefreshAnchorUnavailable("registry_refresh_generic_capture_unavailable")
    capture = validate_capture(json.loads(comment[len(_PREFIX) :], object_pairs_hook=_unique_object))
    origin = capture.origin
    if (
        capture.schema_oid != header["schema_oid"]
        or capture.capture_id != coordinates.edition_id
        or (origin.schema_name, origin.source_id, origin.dataset_id)
        != (coordinates.dataset_schema, coordinates.source_id, coordinates.dataset_id)
    ):
        raise RegistryRefreshAnchorMismatch("registry_refresh_generic_capture_origin_differs")
    return await require_retained_fhir_capture(driver, capture, verify_content=False)


async def read_original_generic_capture(driver, coordinates, read_budget):
    """Issue the transaction-local read capability only after complete native content admission."""
    from process.network_fhir_retained_capture import pin_retained_fhir_read

    capture = await read_original_generic_capture_metadata(driver, coordinates, read_budget)
    return await pin_retained_fhir_read(driver, capture)


async def _organization_scopes(driver, retained, bindings, budget):
    await retained.verify(driver, retained.capture)
    return await original_generic_capture_scopes(driver, retained.capture, bindings, budget)


async def original_generic_capture_scopes(driver, capture, bindings, budget):
    """Derive bounded scope data after caller-owned native metadata or full content admission."""
    origin = capture.origin
    identities = sorted({binding.record_json["source_key"] for binding in bindings})
    budget.reserve(({"raw_byte_count": len(identities) * 256},), len(identities))
    namespace = _identifier(capture.schema_name)
    organization_rows = await driver.fetch(
        f"""SELECT wanted.resource_id FROM unnest($1::text[]) wanted(resource_id)
        JOIN {namespace}.provider_directory_dataset_resource resource
          ON resource.dataset_id=$2 AND resource.resource_type='Organization' AND resource.resource_id=wanted.resource_id
        JOIN {namespace}.provider_directory_entity_release_evidence evidence
          ON evidence.source_id=$3 AND evidence.release_id=$4 AND evidence.resource_type='Organization'
          AND evidence.resource_id=resource.resource_id AND evidence.payload_sha256=resource.payload_hash
        ORDER BY wanted.resource_id COLLATE "C" LIMIT 1001""",
        identities,
        origin.dataset_id,
        origin.source_id,
        capture.release_id,
    )
    if [organization["resource_id"] for organization in organization_rows] != identities:
        raise RegistryRefreshAnchorMismatch("registry_refresh_generic_organization_unavailable")
    scopes_by_binding = {}
    for binding in bindings:
        record_by_field = binding.record_json
        if record_by_field["source_scope_json"] != {
            "organization_id": record_by_field["source_key"],
            "capture_sha256": capture.capture_sha256,
        }:
            raise RegistryRefreshAnchorMismatch("registry_refresh_generic_scope_differs")
        scopes_by_binding[binding.binding_id] = {
            **{
                name: record_by_field[name]
                for name in ("source_system", "source_id", "dataset_schema", "producer_id", "source_key")
            },
            "approved_binding_sha256": binding.approved_binding_sha256,
            "capture_generation": capture.capture_sha256,
        }
    return scopes_by_binding


async def resolve_generic_refresh_anchors(session, bindings, *, read_budget):
    """Derive original approved generic enrollment from genuine retained captures."""
    if (
        type(session) is not AsyncSession
        or type(read_budget) is not RegistryPTGGraphReadBudget
        or not session.in_transaction()
    ):
        raise RegistryRefreshAnchorUnavailable("registry_refresh_generic_context_unavailable")
    try:
        groups = _capture_groups(bindings)
        driver = await native_driver(session)
        scopes_by_binding = {}
        for coordinates, selected in groups.items():
            retained = await read_original_generic_capture(driver, coordinates, read_budget)
            scopes_by_binding.update(await _organization_scopes(driver, retained, selected, read_budget))
        return scopes_by_binding
    except RegistryRefreshAnchorMismatch, RegistryRefreshAnchorUnavailable:
        raise
    except (ValueError, TypeError, KeyError, AttributeError, SQLAlchemyError, asyncpg.PostgresError) as error:
        raise RegistryRefreshAnchorUnavailable("registry_refresh_generic_capture_unavailable") from error
