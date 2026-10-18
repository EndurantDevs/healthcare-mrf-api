# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Admit exact reviewed PTG source units before native candidate formation."""

import json

import asyncpg
from sqlalchemy.exc import DBAPIError

from api.network_address_scope import _validate_network_scope_inputs, current_network_address_scope
from api.plan_pricing_projection_contract import PlanPricingProjectionUnavailable
from api.plan_release_serving import PTG2_SCHEMA, PlanReleaseServingSelection, PlanReleaseSnapshotBinding
from api.provider_service_legacy_selection import restrict_service_bindings
from db.registry_schema import registry_schema
from process.network_address_projection import _identifier
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.ptg_parts.result_archive_source_authority import (
    PtgResultArchiveSourceAuthorityError,
    result_archive_manifest_sha256,
)
from process.registry_catalog_policy import RegistryOrganizationVisibility
from process.registry_network_catalog_read import RegistryNetworkCatalogError, _catalog_manifest
from process.registry_ptg_published_plan_contract import _digest, _uuid
from process.registry_published_plan_binding_refs import _require_reference, _review_commands

_MAX_ROWS = 5000
_MAX_BYTES = 8 * 1024 * 1024


def _unit(binding):
    return binding.source_key, binding.snapshot_id, binding.plan_id, binding.plan_market_type


async def _bounded_rows(driver, query, *parameters):
    """Bound the complete result before transferring any JSON payload."""
    result = await driver.fetchrow(
        "WITH source_rows AS MATERIALIZED (" + query + "), bounds AS ("
        "SELECT count(*) AS count,coalesce(sum(octet_length(to_jsonb(source_rows)::text)),0) AS bytes "
        "FROM source_rows) SELECT count,bytes,CASE WHEN count<=5000 AND bytes<=8388608 "
        "THEN (SELECT jsonb_agg(to_jsonb(source_rows))::text FROM source_rows) END AS rows_json FROM bounds",
        *parameters,
    )
    if result is None or result["count"] > _MAX_ROWS or result["bytes"] > _MAX_BYTES:
        raise ValueError("source evidence exceeds its bound")
    encoded = result["rows_json"]
    if result["count"] and (type(encoded) is not str or len(encoded.encode()) > _MAX_BYTES):
        raise ValueError("source evidence exceeds its bound")
    rows = [] if result["count"] == 0 else json.loads(encoded)
    if type(rows) is not list or len(rows) != result["count"]:
        raise ValueError("source evidence is incomplete")
    return rows


async def _snapshot_manifests(driver, bindings):
    schema = _identifier(PTG2_SCHEMA)
    rows = await _bounded_rows(
        driver,
        f"SELECT snapshot_id,status,manifest FROM {schema}.ptg2_snapshot WHERE snapshot_id=ANY($1::text[])",
        sorted({binding.snapshot_id for binding in bindings}),
    )
    snapshots_by_id = {row["snapshot_id"]: row for row in rows}
    if len(snapshots_by_id) != len(rows) or set(snapshots_by_id) != {binding.snapshot_id for binding in bindings}:
        raise ValueError("source snapshot is incomplete")
    for binding in bindings:
        row = snapshots_by_id[binding.snapshot_id]
        manifest = row["manifest"]
        if (
            row["status"] != "published"
            or type(manifest) is not dict
            or type(manifest.get("serving_index")) is not dict
            or manifest["serving_index"].get("source_key") != binding.source_key
        ):
            raise ValueError("source snapshot identity differs")
    return snapshots_by_id


def _original_role_bindings(selection, role, scope):
    """Reject forged role subsets before reading their source authorities."""
    _validate_network_scope_inputs(
        scope.manifest,
        scope.network_ids,
        scope.access_scope_sha256,
        scope.excluded_network_ids,
        scope.organization_policy,
    )
    if type(scope.organization_policy) is not RegistryOrganizationVisibility:
        raise ValueError("managed source policy is incomplete")
    scope.organization_policy.__post_init__()
    raw_bindings = getattr(selection, "bindings", None)
    if (
        type(raw_bindings) is not tuple
        or len(raw_bindings) > _MAX_ROWS
        or any(
            type(binding) is not PlanReleaseSnapshotBinding
            or binding.role not in {"in_network", "allowed_amounts"}
            or type(binding.binding_ordinal) is not int
            or binding.binding_ordinal < 0
            or any(
                type(coordinate) is not str or not coordinate or coordinate != coordinate.strip()
                for coordinate in (binding.source_key, binding.snapshot_id, binding.plan_id)
            )
            or type(binding.plan_market_type) is not str
            or binding.plan_market_type != binding.plan_market_type.strip().lower()
            or (binding.role == "in_network" and not binding.plan_market_type)
            for binding in raw_bindings
        )
    ):
        raise ValueError("original source binding recipe is malformed")
    if len({(binding.role, binding.binding_ordinal) for binding in raw_bindings}) != len(raw_bindings):
        raise ValueError("original source binding coordinates are repeated")
    bindings = getattr(selection, "in_network_bindings" if role == "in_network" else "allowed_amount_bindings", None)
    if type(bindings) is not tuple or bindings != PlanReleaseServingSelection.bindings_for_role(selection, role):
        raise ValueError("source contributors differ from the original recipe")
    return bindings


async def _source_evidence(driver, bindings, revision, namespace, *, review_loader=None):
    snapshots = sorted({binding.snapshot_id for binding in bindings})
    reviews = await _bounded_rows(
        driver,
        f"SELECT DISTINCT review.scope_id::text,review.approval_sha256,review.approval_json AS document "
        f"FROM {namespace}.registry_approved_record binding "
        f"LEFT JOIN {namespace}.registry_ptg_published_plan_scope review "
        "ON review.scope_id::text=binding.record_json->'source_scope_json'->>'scope_id' "
        "AND review.approval_sha256=binding.record_json->'source_scope_json'->>'approval_sha256' "
        "WHERE binding.approved_revision=$2 AND binding.record_kind='network_binding' "
        "AND binding.record_json->>'source_system'='ptg' AND ("
        "binding.record_json->>'dataset_id'=ANY($1::text[]) OR "
        "binding.record_json->'source_scope_json'->>'snapshot_id'=ANY($1::text[]))",
        snapshots,
        revision,
    )
    mappings = await _bounded_rows(
        driver,
        f"SELECT binding.record_key,binding.record_revision,binding.record_json,"
        "coalesce(company.record_json->>'company_id'=company.record_key "
        "AND company.record_json->'archived'='false'::jsonb "
        "AND network.record_json->>'network_id'=network.record_key "
        "AND network.record_json->'archived'='false'::jsonb "
        "AND jsonb_typeof(network.record_json->'network_id')='number' "
        "AND links.record_json->>'company_id'=company.record_key "
        "AND links.record_json->'archived'='false'::jsonb "
        "AND links.record_json->'network_ids' @> jsonb_build_array(binding.record_json->'network_id'),false) "
        "AS association_active "
        f"FROM {namespace}.registry_approved_record binding "
        f"LEFT JOIN {namespace}.registry_ptg_published_plan_scope review "
        "ON review.scope_id::text=binding.record_json->'source_scope_json'->>'scope_id' "
        "AND review.approval_sha256=binding.record_json->'source_scope_json'->>'approval_sha256' "
        f"LEFT JOIN {namespace}.registry_approved_record company ON company.approved_revision=$2 "
        "AND company.record_kind='company' AND company.record_key=review.approval_json->'command'->>'legal_company_id' "
        f"LEFT JOIN {namespace}.registry_approved_record network ON network.approved_revision=$2 "
        "AND network.record_kind='network' AND network.record_key=binding.record_json->>'network_id' "
        f"LEFT JOIN {namespace}.registry_approved_record links ON links.approved_revision=$2 "
        "AND links.record_kind='company_links' AND links.record_key=company.record_key "
        "WHERE binding.approved_revision=$2 AND binding.record_kind='network_binding' "
        "AND binding.record_json->>'source_system'='ptg' AND ("
        "binding.record_json->>'dataset_id'=ANY($1::text[]) OR "
        "binding.record_json->'source_scope_json'->>'snapshot_id'=ANY($1::text[]))",
        snapshots,
        revision,
    )
    return (review_loader or _review_commands)(json.dumps(reviews)), mappings


def _reviewed_units(reviews, snapshots):
    manifest_digest_by_id = {
        identity: result_archive_manifest_sha256(snapshot["manifest"]) for identity, snapshot in snapshots.items()
    }
    units = set()
    for command in reviews.values():
        identity = command["published_identity"]
        snapshot = snapshots[identity["snapshot_id"]]
        if (
            command["source"]["ptg_schema_name"] != PTG2_SCHEMA
            or identity["snapshot_manifest_sha256"] != manifest_digest_by_id[identity["snapshot_id"]]
            or identity["source_key"] != snapshot["manifest"]["serving_index"]["source_key"]
        ):
            raise ValueError("review source identity differs")
        units.add((identity["source_key"], identity["snapshot_id"], command["plan_id"], command["plan_market_type"]))
    return units


def _eligible_units(mappings, reviews, reviewed, exclusions, *, organization_policy, network_ids=None):
    eligible, identities = set(), set()
    excluded_companies = set(organization_policy.excluded_company_ids)
    excluded_networks = set(exclusions)
    for approved_row in mappings:
        binding = approved_row["record_json"]
        if type(binding) is not dict or type(binding.get("archived")) is not bool:
            raise ValueError("approved source mapping is malformed")
        RegistryNetworkSourceCoordinates(
            **{
                name: binding[name]
                for name in ("source_system", "source_id", "dataset_schema", "dataset_id", "producer_id", "edition_id")
            }
        )
        _uuid(approved_row["record_key"])
        _digest(binding["binding_key"])
        scope = binding["source_scope_json"]
        # Cohort scopes have a different retained-source proof and cannot enter this lane.
        if type(scope) is not dict or "review_type" not in scope:
            raise ValueError("legacy source admission requires its retained cohort consumer")
        identity = (scope["scope_id"], scope["approval_sha256"])
        command = reviews[identity]
        _require_reference(binding, command)
        if (
            binding.get("binding_id") != approved_row["record_key"]
            or type(binding.get("revision")) is not int
            or binding["revision"] < 1
            or binding["revision"] != approved_row["record_revision"]
            or type(binding["network_id"]) is not int
            or not 1 <= binding["network_id"] <= 2147483647
            or approved_row["record_key"] in identities
        ):
            raise ValueError("approved source mapping is malformed")
        identities.add(approved_row["record_key"])
        unit = (binding["source_key"], scope["snapshot_id"], scope["plan_id"], scope["plan_market_type"])
        reviewed.add(unit)
        if (
            not binding["archived"]
            and approved_row["association_active"] is True
            and command["legal_company_id"] not in excluded_companies
            and (
                binding["network_id"] in network_ids
                if network_ids is not None
                else binding["network_id"] not in excluded_networks
            )
        ):
            eligible.add(unit)
    return eligible


async def admitted_plan_release_bindings(session, selection) -> tuple[PlanReleaseSnapshotBinding, ...]:
    """Keep frozen binding ordinals and release completeness; filter only contributors."""
    bindings = selection.in_network_bindings
    scope = current_network_address_scope()
    if scope is None:
        return bindings
    try:
        bindings = _original_role_bindings(selection, "in_network", scope)
        connection = await session.connection()
        driver = (await connection.get_raw_connection()).driver_connection
        await _catalog_manifest(driver, scope.manifest.generation_id, registry_schema(), scope.manifest)
        snapshots = await _snapshot_manifests(driver, bindings)
        reviews, mappings = await _source_evidence(
            driver, bindings, scope.manifest.approved_custom_revision, _identifier(registry_schema())
        )
        reviewed = _reviewed_units(reviews, snapshots)
        eligible = _eligible_units(
            mappings,
            reviews,
            reviewed,
            scope.excluded_network_ids or (),
            organization_policy=scope.organization_policy,
            network_ids=scope.network_ids,
        )
        return restrict_service_bindings(
            selection,
            tuple(
                binding
                for binding in bindings
                if _unit(binding) in eligible or (scope.network_ids is None and _unit(binding) not in reviewed)
            ),
        )
    except (
        ValueError,
        KeyError,
        TypeError,
        asyncpg.PostgresError,
        asyncpg.InterfaceError,
        DBAPIError,
        RegistryNetworkCatalogError,
        PtgResultArchiveSourceAuthorityError,
    ):
        raise PlanPricingProjectionUnavailable("managed plan source admission unavailable") from None
