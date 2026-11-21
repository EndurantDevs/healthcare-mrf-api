# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Imported bindings from a named published catalog, with current manual overrides."""

import json
import re
from dataclasses import dataclass

from db.plan_release_storage import storage_identifier
from process.network_approved_membership_source import ApprovedMembershipSource, pin_approved_membership_source
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.registry_fhir_catalog_sources import (
    fhir_catalog_source_generation,
    validate_fhir_catalog_source_descriptor,
)


@dataclass(frozen=True)
class PublishedFHIRCatalogBindingScope:
    """Select immutable catalog data; the controlled importer supplies authorization."""

    metadata_generation: str
    prepared_inventory_token: str | None = None

    def __post_init__(self):
        if type(self.metadata_generation) is not str or re.fullmatch(r"[0-9a-f]{64}", self.metadata_generation) is None:
            raise ValueError("registry_imported_catalog_scope_invalid")
        if self.prepared_inventory_token is not None and (
            type(self.prepared_inventory_token) is not str
            or re.fullmatch(r"[0-9a-f]{64}", self.prepared_inventory_token) is None
        ):
            raise ValueError("registry_imported_catalog_scope_invalid")


def _document(encoded):
    return json.loads(encoded) if type(encoded) is str else encoded


async def published_fhir_catalog_bindings_sql(connection, scope, approved, coordinates, descriptor, *, control_schema):
    """Resolve the exact published source before exposing its effective binding CTE."""
    if (
        type(scope) is not PublishedFHIRCatalogBindingScope
        or type(approved) is not ApprovedMembershipSource
        or type(coordinates) is not RegistryNetworkSourceCoordinates
        or not connection.is_in_transaction()
    ):
        raise ValueError("registry_imported_catalog_scope_invalid")
    scope.__post_init__()
    descriptor = validate_fhir_catalog_source_descriptor(descriptor)
    if (
        descriptor["profile"] != "published_normalized"
        or descriptor.get("complete") is not True
        or descriptor["coordinates"]
        != dict(zip(coordinates.__dataclass_fields__, coordinates.sql_parameters, strict=True))
    ):
        raise ValueError("registry_imported_catalog_source_changed")
    current = await pin_approved_membership_source(
        connection, approved_revision=approved.approved_revision, control_schema=control_schema
    )
    if current != approved:
        raise ValueError("registry_imported_catalog_manual_map_changed")
    control = storage_identifier(control_schema)
    snapshot = await _catalog_snapshot(connection, scope, approved, control)
    generation = fhir_catalog_source_generation(descriptor)
    vector = _document(snapshot["source_vector"])
    matches = (
        [entry for entry in vector if type(entry) is dict and entry.get("source_generation") == generation]
        if type(vector) is list
        else []
    )
    statuses = {"current", "last_good"} if scope.prepared_inventory_token is not None else {"current"}
    if (
        len(matches) != 1
        or matches[0].get("source_id") != coordinates.source_id
        or matches[0].get("status") not in statuses
    ):
        raise ValueError("registry_imported_catalog_source_changed")
    catalog = storage_identifier(snapshot["schema_name"])
    native = await connection.fetchval(
        f"SELECT CASE WHEN octet_length(descriptor_json::text)<=8388608 THEN descriptor_json::text END "
        f"FROM {catalog}.registry_fhir_catalog_source WHERE source_generation=$1 AND profile='published_normalized'",
        generation,
    )
    if native is None or _document(native) != descriptor:
        raise ValueError("registry_imported_catalog_source_changed")
    bindings = _binding_sql(control, catalog, scope.metadata_generation)
    invalid = await connection.fetchval(
        "WITH " + bindings + " SELECT EXISTS(SELECT FROM effective_bindings WHERE conflicting "
        "OR (overridden AND NOT excluded AND manual_network_key IS NULL)) OR EXISTS(SELECT FROM approved_network_bindings "
        "GROUP BY record_json->>'source_key' HAVING count(*)>1)",
        approved.approved_revision,
        *coordinates.sql_parameters,
    )
    if invalid is not False:
        raise ValueError("registry_imported_catalog_binding_conflict")
    return bindings


def _binding_sql(control, catalog, metadata_generation):
    from process.registry_binding_override_sql import binding_override_identity_sql

    override_identity = binding_override_identity_sql("m.record_json", "c.document_json", "c.source_aliases")
    # The validated hex generation is a literal so existing bounded graph queries keep their eight parameters.
    return f"""catalog_bindings AS MATERIALIZED (
      SELECT b.*,coalesce(s.descriptor_json->'source_aliases',jsonb_build_array(s.descriptor_json->'coordinates'->'source_id')) AS source_aliases
      FROM {catalog}.registry_fhir_catalog_binding b
      JOIN {catalog}.registry_fhir_catalog_network n USING(metadata_generation,network_id)
      JOIN {catalog}.registry_fhir_catalog_source s
        ON s.source_generation=b.document_json->'source_scope'->>'source_generation'
      WHERE b.metadata_generation='{metadata_generation}' AND NOT n.archived
        AND b.document_json->>'source_system'=$2 AND b.source_id=$3
        AND b.document_json->>'dataset_schema'=$4 AND b.document_json->>'dataset_id'=$5
        AND b.document_json->>'producer_id'=$6 AND b.document_json->>'edition_id'=$7
    ), manual_bindings AS MATERIALIZED (
      SELECT c.binding_id,m.record_json,m.custom_revision,
        max(m.custom_revision) OVER(PARTITION BY c.binding_id) AS latest
      FROM catalog_bindings c JOIN {control}.registry_approved_record m
        ON m.approved_revision=$1 AND m.record_kind='network_binding'
        AND {override_identity}
    ), overrides AS MATERIALIZED (
      SELECT binding_id,min((record_json->>'network_id')::integer) AS network_id,
        bool_or((record_json->>'archived')::boolean) AS archived,
        count(DISTINCT (record_json->>'network_id',record_json->>'archived'))>1 AS conflicting
      FROM manual_bindings WHERE custom_revision=latest GROUP BY binding_id
    ), effective_bindings AS MATERIALIZED (
      SELECT c.*,coalesce(o.network_id,c.network_id) AS effective_network_id,
        coalesce(o.archived,false) AS excluded,coalesce(o.conflicting,false) AS conflicting,
        o.binding_id IS NOT NULL AS overridden,
        n.record_key AS manual_network_key,n.record_json AS manual_network
      FROM catalog_bindings c LEFT JOIN overrides o USING(binding_id)
      LEFT JOIN {control}.registry_approved_record n
        ON n.approved_revision=$1 AND n.record_kind='network'
        AND n.record_key=coalesce(o.network_id,c.network_id)::text
    ), approved_network_bindings AS MATERIALIZED (
      SELECT binding_id::text AS record_key,(document_json->>'revision')::bigint AS record_revision,
        $1::bigint AS custom_revision,
        (document_json-'source_scope')||jsonb_build_object('network_id',effective_network_id,
          'source_scope_json',document_json->'source_scope','archived',false) AS record_json,
        effective_network_id AS network_id,effective_network_id::text AS approved_network_key
      FROM effective_bindings WHERE NOT excluded
        AND NOT coalesce((manual_network->>'archived')::boolean,false)
        AND NOT conflicting
    )"""


async def _catalog_snapshot(connection, scope, approved, control):
    prepared = scope.prepared_inventory_token
    publication = (
        "published_at IS NOT NULL" if prepared is None else "accounting_json->'imported_source_inventory'->>'token'=$2"
    )
    snapshot = await connection.fetchrow(
        f"SELECT schema_name,metadata_approved_revision,CASE WHEN octet_length(source_vector_json::text)<=1048576 "
        f"THEN source_vector_json::text END AS source_vector FROM {control}.registry_fhir_catalog_snapshot "
        "WHERE metadata_generation=$1 AND " + publication,
        scope.metadata_generation,
        *((prepared,) if prepared is not None else ()),
    )
    if (
        snapshot is None
        or type(snapshot["schema_name"]) is not str
        or re.fullmatch(r"fhir_catalog_[0-9a-f]{32}", snapshot["schema_name"]) is None
        or type(snapshot["metadata_approved_revision"]) is not int
        or not 0 <= snapshot["metadata_approved_revision"] <= approved.approved_revision
        or snapshot["source_vector"] is None
    ):
        raise ValueError("registry_imported_catalog_unpublished")
    return snapshot
