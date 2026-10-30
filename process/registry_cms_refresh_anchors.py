# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Derive CMS refresh continuity from accepted native capture evidence."""

from dataclasses import asdict

import asyncpg
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from process.network_address_projection import _identifier
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.network_cms_registry_address_capture import native_driver
from process.registry_cms_source_notifications import RegistryCMSSourceNotificationReader
from process.registry_ptg_graph_reader import RegistryPTGGraphReadBudget, RegistryPTGGraphReadError
from process.registry_refresh_anchor_contract import (
    ApprovedRefreshBinding,
    RegistryRefreshAnchorMismatch,
    RegistryRefreshAnchorUnavailable,
)


def _capture_groups(bindings):
    if type(bindings) is not tuple or len(bindings) > 1000:
        raise RegistryRefreshAnchorMismatch("registry_refresh_anchor_mismatch")
    bindings_by_capture = {}
    binding_ids = set()
    for binding in bindings:
        if (
            type(binding) is not ApprovedRefreshBinding
            or binding.record_json.get("source_system") != "fhir"
            or binding.record_json.get("archived") is not False
        ):
            raise RegistryRefreshAnchorMismatch("registry_refresh_anchor_mismatch")
        if binding.record_json.get("source_id") != "cms-npd":
            raise RegistryRefreshAnchorUnavailable("registry_refresh_anchor_unavailable")
        if binding.binding_id in binding_ids:
            raise RegistryRefreshAnchorMismatch("registry_refresh_anchor_mismatch")
        binding_ids.add(binding.binding_id)
        coordinates = RegistryNetworkSourceCoordinates(
            **{name: binding.record_json[name] for name in RegistryNetworkSourceCoordinates.__dataclass_fields__}
        )
        bindings_by_capture.setdefault(coordinates, []).append(binding)
    if len(bindings_by_capture) > 100:
        raise RegistryRefreshAnchorUnavailable("registry_refresh_anchor_unavailable")
    return bindings_by_capture


async def _organizations(driver, pin, bindings, budget):
    resources = sorted({binding.record_json["source_key"] for binding in bindings})
    budget.reserve(({"raw_byte_count": len(resources) * 256},), len(resources))
    namespace = _identifier(pin.read_schema_name)
    rows = await driver.fetch(
        f"""SELECT requested.resource_id,legacy.network_id::text AS legacy_network_id
        FROM unnest($1::text[]) requested(resource_id)
        JOIN {namespace}.provider_directory_dataset_resource resource ON resource.dataset_id=$2
          AND resource.resource_type='Organization' AND resource.resource_id=requested.resource_id
        JOIN {namespace}.provider_directory_cms_npd_resource_witness witness
          ON witness.dataset_id=resource.dataset_id AND witness.resource_type=resource.resource_type
          AND witness.resource_id=resource.resource_id AND witness.source_id=$3 AND witness.release_id=$4
          AND witness.normalized_payload_hash=resource.payload_hash
          AND (resource.acquired_resource_sha256 IS NULL
            OR resource.acquired_resource_sha256=witness.raw_payload_sha256)
        JOIN {namespace}.provider_directory_entity_release_evidence evidence
          ON evidence.source_id=$3 AND evidence.release_id=$4 AND evidence.resource_type='Organization'
          AND evidence.resource_id=resource.resource_id AND evidence.payload_sha256=witness.raw_payload_sha256
        JOIN {namespace}.provider_directory_insurance_network_source_binding legacy
          ON legacy.source_id=$3 AND legacy.resource_type='Organization' AND legacy.resource_id=evidence.resource_id
        ORDER BY requested.resource_id COLLATE "C" LIMIT 1001""",
        resources,
        pin.dataset_id,
        pin.source_id,
        pin.release_id,
    )
    if len(rows) != len(resources) or [row["resource_id"] for row in rows] != resources:
        raise RegistryRefreshAnchorMismatch("registry_refresh_anchor_mismatch")
    return {row["resource_id"]: row["legacy_network_id"] for row in rows}


def _scopes(pair, bindings, networks):
    pin = pair.proof.source_pin
    scopes_by_binding = {}
    for binding in bindings:
        resource_id = binding.record_json["source_key"]
        legacy = networks[resource_id]
        if binding.record_json["source_scope_json"] != {
            "organization_id": resource_id,
            "alias_scope": pin.alias_scope,
            "legacy_uuid": legacy,
        }:
            raise RegistryRefreshAnchorMismatch("registry_refresh_anchor_mismatch")
        scopes_by_binding[binding.binding_id] = {
            "source_system": "fhir",
            "source_id": pin.source_id,
            "dataset_schema": pin.schema_name,
            "endpoint_id": pin.endpoint_id,
            "producer_id": pair.proof.binding_coordinates.producer_id,
            "alias_scope": pin.alias_scope,
            "resource_type": "Organization",
            "resource_id": resource_id,
            "legacy_network_id": legacy,
        }
    return scopes_by_binding


def _original_identity(pair):
    proof = pair.proof
    return (
        asdict(proof.source_pin),
        proof.cms_authority,
        proof.expected_admission_sha256,
        proof.expected_metadata_sha256,
    )


async def _capture_scopes(session, driver, reader, coordinates, bindings, budget):
    budget.reserve(({"raw_byte_count": 16384},))
    headers = await reader.matching_headers(session, coordinates)
    if not headers:
        raise RegistryRefreshAnchorUnavailable("registry_refresh_anchor_unavailable")
    original = scopes_by_binding = None
    for header in headers:
        budget.reserve((header,))
        notification = await reader.read(
            session, source_schema=coordinates.dataset_schema, receipt_id=header["receipt_id"]
        )
        pair = notification.pair
        if pair.proof.binding_coordinates != coordinates:
            raise RegistryRefreshAnchorMismatch("registry_refresh_anchor_mismatch")
        derived = _scopes(pair, bindings, await _organizations(driver, pair.recipe.source_pin, bindings, budget))
        identity = _original_identity(pair)
        if original is not None and (original != identity or scopes_by_binding != derived):
            raise RegistryRefreshAnchorUnavailable("registry_refresh_anchor_unavailable")
        original, scopes_by_binding = identity, derived
    return scopes_by_binding


async def resolve_cms_refresh_anchors(session, bindings, *, reader, read_budget):
    """Use one read-only snapshot and accepted ledgers without a serving-head dependency."""
    if (
        type(session) is not AsyncSession
        or type(reader) is not RegistryCMSSourceNotificationReader
        or type(read_budget) is not RegistryPTGGraphReadBudget
        or not session.in_transaction()
    ):
        raise RegistryRefreshAnchorUnavailable("registry_refresh_anchor_unavailable")
    try:
        bindings_by_capture = _capture_groups(bindings)
        driver = await native_driver(session)
        scopes_by_binding = {}
        for coordinates, selected in bindings_by_capture.items():
            scopes_by_binding.update(await _capture_scopes(session, driver, reader, coordinates, selected, read_budget))
        return scopes_by_binding
    except RegistryRefreshAnchorMismatch, RegistryRefreshAnchorUnavailable:
        raise
    except (
        ValueError,
        TypeError,
        KeyError,
        RegistryPTGGraphReadError,
        SQLAlchemyError,
        asyncpg.PostgresError,
    ) as error:
        raise RegistryRefreshAnchorUnavailable("registry_refresh_anchor_unavailable") from error
