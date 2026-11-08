# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Managed source contributors and exact retained billing office SQL."""

from dataclasses import asdict, replace

from sqlalchemy import text

from api.billing_search_transport_contract import _canonical_json_bytes, _framed_sha256
from api.network_address_scope import (
    _validate_network_scope_inputs,
    current_network_address_scope,
    scoped_address_parameters,
    scoped_address_relation_sql,
)
from api.network_read_execution import execute_network_read
from api.plan_release_serving import PlanReleaseServingSelection
from api.provider_service_source_policy import admitted_plan_release_bindings
from api.ptg2_billing_search_contract import serving_unavailable
from process.registry_catalog_policy import RegistryOrganizationVisibility

_DOMAIN = b"BILLING_SEARCH_NETWORK_SOURCE_SCOPE_V1\x00"


def admitted_coordinates(selection, bindings):
    """Validate an ordered subset without changing the original frozen recipe."""
    if type(selection) is not PlanReleaseServingSelection or type(bindings) is not tuple:
        raise serving_unavailable()
    original = selection.in_network_bindings
    if bindings != tuple(binding for binding in original if binding in bindings):
        raise serving_unavailable()
    return tuple((binding.binding_ordinal, binding.snapshot_id) for binding in bindings)


def network_scope_sha256(selection, bindings):
    """Bind full signed policy, retained generation and original source coordinates."""
    scope = current_network_address_scope()
    if scope is None:
        return None
    _validate_network_scope_inputs(
        scope.manifest,
        scope.network_ids,
        scope.access_scope_sha256,
        scope.excluded_network_ids,
        scope.organization_policy,
    )
    if type(scope.organization_policy) is not RegistryOrganizationVisibility:
        raise serving_unavailable()
    document_by_field = {
        "access_scope_sha256": scope.access_scope_sha256,
        "generation_id": scope.manifest.generation_id,
        "approved_custom_revision": scope.manifest.approved_custom_revision,
        "manifest_sha256": scope.manifest.manifest_sha256,
        "address_table_oid": scope.manifest.address_table_oid,
        "network_ids": scope.network_ids,
        "excluded_network_ids": scope.excluded_network_ids,
        "organization_policy": asdict(scope.organization_policy),
        "plan_release_id": selection.plan_release_id,
        "serving_revision_id": selection.serving_revision_id,
        "binding_set_digest": selection.binding_set_digest,
        "raw_bindings": [asdict(binding) for binding in selection.bindings],
        "admitted_coordinates": admitted_coordinates(selection, bindings),
    }
    if scope.legacy_checksums is not None:
        document_by_field["legacy_checksums"] = scope.legacy_checksums
    return _framed_sha256(_DOMAIN, _canonical_json_bytes(document_by_field))


def _retained_location_sql(native, context):
    """Render the original assured query with every office access policy scoped."""
    canonical = f"{native.PTG2_SCHEMA}.entity_address_unified"
    if (
        context.address_table != canonical
        or context.knn_order_sql is not None
        or context.taxonomy_index_sql is not None
    ):
        raise serving_unavailable()
    retained = scoped_address_relation_sql(canonical)
    if retained == canonical:
        raise serving_unavailable()
    values_by_name, unified = native._membership_sql_values(context, include_address_site_key=True)
    if not unified or context.address_assurance_sql == "TRUE":
        raise serving_unavailable()
    template = native._MEMBERSHIP_UNIFIED_ASSURED_LOCATION_SQL
    marker = "{ptg2_schema}.entity_address_unified"
    if template.count(marker) != 1:
        raise serving_unavailable()
    values_by_name.update(
        address_table=retained,
        retained_address_relation=retained,
        located_scan_sql=native._membership_unified_located_scan_sql(replace(context, address_table=retained)),
    )
    return template.replace(marker, "{retained_address_relation}").format(**values_by_name)


async def _retained_lineage(session, native, location_rows):
    """Reuse exact stored billing lineage against the same retained office relation."""
    requests = native._selected_location_requests(location_rows)
    if not requests:
        return
    marker = "$unified_relation"
    template = native._ADDRESS_PROVENANCE_SQL_TEMPLATE
    if template.count(marker) != 1:
        raise serving_unavailable()
    retained = scoped_address_relation_sql(f"{native.PTG2_SCHEMA}.entity_address_unified")
    from api.plan_pricing_projection_contract import address_provenance_sql

    query = address_provenance_sql(template.replace(marker, retained), native.PTG2_SCHEMA)
    result = await execute_network_read(
        session,
        text(query),
        {
            "location_keys": [request[0] for request in requests],
            "admitted_source_ids": [request[1] for request in requests],
            "stored_only": True,
            "strict_stored_identity": True,
            **scoped_address_parameters({}),
        },
    )
    native._apply_address_provenance(
        location_rows, native._index_address_provenance(result), include_response_evidence=True
    )


async def load_managed_billing_location_rows(session, serving_tables, geo_args, candidate_npis):
    """Authorize exact retained offices before native ranking and its page window."""
    from api import ptg2_serving as native

    context = await native._membership_location_query(
        session,
        serving_tables,
        geo_args,
        candidate_npis=candidate_npis,
        limit=len(candidate_npis),
        offset=0,
    )
    if context is None:
        return None
    query = _retained_location_sql(native, context)
    context = replace(context, parameter_map=scoped_address_parameters(context.parameter_map))
    location_rows = await native._execute_membership_location_sql(session, context, query, offset=0)
    await _retained_lineage(session, native, location_rows)
    return location_rows
