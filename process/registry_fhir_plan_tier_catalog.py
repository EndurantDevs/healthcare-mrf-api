# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Project typed plan-tier observations through existing catalog identities and COPY."""

import hashlib
import json
from dataclasses import asdict, fields
from datetime import timezone
from uuid import NAMESPACE_URL, uuid5

from db.plan_release_storage import storage_identifier
from process.registry_fhir_catalog_enrichment import FHIRCatalogEnrichmentContext, enrich_fhir_catalog_edges
from process.registry_fhir_catalog_extraction import (
    FHIRCatalogReferenceEdge,
    catalog_reference_edge,
    fhir_catalog_json_bytes,
)
from process.registry_fhir_catalog_identity import (
    FHIRNetworkIdentityInput,
    allocate_fhir_catalog_network_ids,
    resolve_fhir_catalog_network_ids,
)
from process.registry_fhir_catalog_projection import (
    FHIRCatalogProjectionBatch,
    _network_row,
    _output_size,
    _relationship_rows,
)
from process.registry_fhir_plan_tier import source_key, supports_plan_tiers, validate_plan_scope, validate_plan_tier


async def plan_tier_pages(connection, schema_name, generation, *, surviving_bindings=None):
    """Join bounded compact role observations to their exact observed InsurancePlan."""
    namespace = storage_identifier(schema_name)
    surviving = (
        ""
        if surviving_bindings is None
        else (
            f"AND EXISTS (SELECT FROM {surviving_bindings} b WHERE "
            "b.document_json->>'evidence_id'=observed.source_generation||':UHCPlanTier/'||"
            "(observed.document_json->'plan_tier'->>'network_key_id')) "
        )
    )
    after = ""
    while True:
        projection_rows = await connection.fetch(
            "WITH keys AS MATERIALIZED (SELECT DISTINCT (document_json->'plan_tier'->>'network_key_id') COLLATE \"C\" AS key "
            f"FROM {namespace}.registry_fhir_catalog_observation observed WHERE source_generation=$1 "
            "AND observation_kind='edge' AND document_json->'plan_tier'->>'network_key_id'>$2 COLLATE \"C\" "
            + surviving
            + "ORDER BY key LIMIT 1000), selected AS (SELECT k.key,"
            "jsonb_build_object('edge',e.document_json,'plan',p.document_json)::text AS document "
            "FROM keys k CROSS JOIN LATERAL (SELECT document_json "
            f"FROM {namespace}.registry_fhir_catalog_observation o WHERE o.source_generation=$1 "
            "AND o.observation_kind='edge' AND o.document_json->'plan_tier'->>'network_key_id'=k.key "
            'ORDER BY resource_type COLLATE "C",resource_id COLLATE "C",ordinal LIMIT 1) e '
            f"LEFT JOIN {namespace}.registry_fhir_catalog_observation p ON p.source_generation=$1 "
            "AND p.resource_type='InsurancePlan' AND p.resource_id=e.document_json->'plan_tier'->>'plan_resource_id' "
            "AND p.observation_kind='edge' AND p.ordinal=0), bounded AS (SELECT *,"
            "sum(octet_length(document)+1) OVER() AS bytes FROM selected) "
            'SELECT key,CASE WHEN bytes<=8388608 THEN document END AS document FROM bounded ORDER BY key COLLATE "C"',
            generation,
            after,
        )
        if not projection_rows:
            return
        if any(projection_row["document"] is None for projection_row in projection_rows):
            raise ValueError("registry_fhir_plan_tier_bounds")
        yield tuple(json.loads(projection_row["document"]) for projection_row in projection_rows)
        after = projection_rows[-1]["key"]


def _validated_pair(pair):
    edge, plan = pair["edge"], pair["plan"]
    tier = validate_plan_tier(edge["plan_tier"])
    if (
        type(plan) is not dict
        or plan.get("owner_resource_type") != "InsurancePlan"
        or plan.get("owner_resource_id") != tier["plan_resource_id"]
        or plan.get("plan_scope") != tier["plan_scope"]
        or edge.get("plan_scope") != tier["plan_scope"]
    ):
        raise ValueError("registry_fhir_plan_tier_plan_changed")
    validate_plan_scope(plan["plan_scope"])
    return tier, plan, edge


def _typed_binding(context, tier, plan, edge, network_id):
    """Commit the exact plan and representative role payloads in one typed binding."""
    key = source_key(tier)
    binding_id = uuid5(
        NAMESPACE_URL,
        "registry-fhir-catalog/plan-tier/v1:" + fhir_catalog_json_bytes([asdict(context.coordinates), key]).decode(),
    )
    evidence_hash = hashlib.sha256((plan["source_hash"] + ":" + edge["source_hash"]).encode()).hexdigest()
    binding_by_field = dict(
        namespace="source_binding",
        binding_id=str(binding_id),
        revision=1,
        **asdict(context.coordinates),
        source_key=key,
        source_scope={**tier, "source_generation": context.source_generation},
        evidence_id=context.source_generation + ":" + key,
        evidence_sha256=evidence_hash,
        metadata_sha256=hashlib.sha256(
            fhir_catalog_json_bytes([context.source_generation, tier, plan, edge])
        ).hexdigest(),
        accepted_at=context.accepted_at.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
        status=context.status,
    )
    return (
        context.metadata_generation,
        network_id,
        binding_id,
        context.coordinates.source_id,
        fhir_catalog_json_bytes(binding_by_field).decode(),
    )


def format_plan_tier_projection(pairs, context, identities, canonical_networks=(), *, as_of):
    """Use only observed plan fields for display; the typed digest supplies identity."""
    if context.profile != "published_normalized":
        raise ValueError("registry_fhir_plan_tier_source_invalid")
    canonical_by_id = {projection_row["network_id"]: projection_row for projection_row in canonical_networks}
    networks, bindings, relationships = [], [], []
    for pair in pairs:
        tier, plan, edge = _validated_pair(pair)
        network_id = identities[tier["network_key_id"]]
        binding_row = _typed_binding(context, tier, plan, edge, network_id)
        binding_id = binding_row[2]
        name = f"{plan['plan_name'] or tier['plan_scope']['plan_id']} ({tier['plan_scope']['plan_year']}, {tier['network_tier']})"
        if len(name.encode()) > 512:
            raise ValueError("registry_fhir_plan_tier_bounds")
        network = canonical_by_id.get(network_id, dict(display_name=name, aliases=[], archived=False, revision=1))
        networks.append(_network_row(context, network_id, (), network))
        bindings.append(binding_row)
        plan_edge = catalog_reference_edge(
            {name: plan[name] for name in (field.name for field in fields(FHIRCatalogReferenceEdge))}
        )
        plan_edge = FHIRCatalogReferenceEdge(
            **{**asdict(plan_edge), "scope_path": "/plan_tier/" + tier["network_key_id"], "resolution_reason": None}
        )
        enrichment = FHIRCatalogEnrichmentContext(
            context.coordinates.source_id, as_of, (), (), context.coordinates.source_id
        )
        enriched = enrich_fhir_catalog_edges((), (plan_edge,), enrichment)
        projection_rows, _ = _relationship_rows(context, enriched, {None: network_id}, reviewed_binding_id=binding_id)
        relationships.extend(projection_rows)
    if _output_size((networks, bindings, relationships)) > 8 * 1024 * 1024:
        raise ValueError("registry_fhir_plan_tier_bounds")
    return FHIRCatalogProjectionBatch(tuple(networks), tuple(relationships), tuple(bindings), len(relationships))


async def plan_tier_projection(connection, descriptor, context, pairs, *, revision, control_schema, allocate, as_of):
    """Resolve exact typed identities and independently format bounded projection rows."""
    if not supports_plan_tiers(descriptor):
        raise ValueError("registry_fhir_plan_tier_source_invalid")
    inputs = tuple(
        FHIRNetworkIdentityInput(
            _validated_pair(pair)[0]["network_key_id"],
            descriptor["endpoint_id"],
            tuple(descriptor["source_aliases"]),
            context.source_generation + ":" + source_key(pair["edge"]["plan_tier"]),
        )
        for pair in pairs
    )
    resolve = allocate_fhir_catalog_network_ids if allocate else resolve_fhir_catalog_network_ids
    allocation = await resolve(
        connection, inputs, approved_revision=revision, control_schema=control_schema, identity_kind="plan_tier"
    )
    projection_rows = allocation["rows"] if allocate else allocation
    if any(projection_row["conflict"] or projection_row["network_id"] is None for projection_row in projection_rows):
        raise ValueError("registry_fhir_catalog_identity_conflict")
    ids_by_key = {projection_row["organization_id"]: projection_row["network_id"] for projection_row in projection_rows}
    canonical_rows = await connection.fetch(
        f"SELECT record_json::text AS document FROM {storage_identifier(control_schema)}.registry_approved_record "
        "WHERE approved_revision=$1 AND record_kind='network' AND record_key=ANY($2::text[])",
        revision,
        [str(network_identity) for network_identity in ids_by_key.values()],
    )
    projection = format_plan_tier_projection(
        pairs,
        context,
        ids_by_key,
        tuple(json.loads(projection_row["document"]) for projection_row in canonical_rows),
        as_of=as_of,
    )
    return projection, allocation["created_networks"] if allocate else 0


async def replace_plan_tier_bindings(connection, relations, descriptor, keys):
    """Replace only the same typed source identities in the isolated candidate."""
    selection = (
        f"SELECT binding_id FROM {relations['binding']} WHERE document_json->'source_scope'->>'kind'='uhc_plan_tier_v1' "
        "AND document_json->>'source_id'=$1 AND document_json->>'dataset_schema'=$2 "
        "AND document_json->>'source_key'=ANY($3::text[])"
    )
    arguments = (descriptor["coordinates"]["source_id"], descriptor["coordinates"]["dataset_schema"], keys)
    await connection.execute(f"DELETE FROM {relations['relationship']} WHERE binding_id IN ({selection})", *arguments)
    await connection.execute(f"DELETE FROM {relations['binding']} WHERE binding_id IN ({selection})", *arguments)
