# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Read required-target addresses from verified flat or compact serving coordinates."""

from uuid import UUID

from api.network_compact_address import compact_address_native_sql
from db.models import EntityAddressUnified
from process.network_compact_layout import compact_identifier
from process.network_serving_read import PinnedNetworkServingManifest


def _projected_edges(layout, first_parameter):
    refs = compact_identifier(layout.ref_map.schema_name) + "." + compact_identifier(layout.ref_map.table_name)
    arms = [
        f"SELECT ref.network_id,edge.location_key FROM {refs} ref "
        f"JOIN {compact_identifier(artifact.schema_name)}.network_office_edge edge USING(slice_id) "
        "JOIN networks selected ON selected.network_id=ref.network_id "
        f"WHERE ref.artifact_candidate_id=${first_parameter + index}::uuid"
        for index, artifact in enumerate(layout.artifacts)
    ]
    return (
        " UNION ALL ".join(arms)
        if arms
        else "SELECT NULL::integer AS network_id,NULL::text AS location_key WHERE FALSE"
    )


def _address_columns():
    return ",".join(
        f"coalesce(address.{name},projected.{name}) AS {name}"
        if name in {"location_key", "canonical_network_ids"}
        else "address." + compact_identifier(name)
        for name in EntityAddressUnified.__table__.columns.keys()
    )


def required_target_addresses(manifest, *, first_parameter):
    """Append compact artifact binds after the caller's existing result parameters.

    The caller must resolve and verify the manifest in its read-only snapshot.
    `networks` is the report's fixed SQL CTE, not a request-supplied predicate.
    """
    if type(manifest) is not PinnedNetworkServingManifest:
        raise ValueError("A verified serving manifest is required")
    if type(first_parameter) is not int or not 1 <= first_parameter <= 32767:
        raise ValueError("Required-target native parameter position is invalid")
    if manifest.compact_layout is None:
        return compact_identifier(manifest.schema_name) + ".entity_address_unified", ()
    layout = manifest.compact_layout
    addresses, arguments = compact_address_native_sql(
        layout, first_parameter=first_parameter, visible_membership_sql="TRUE"
    )
    if first_parameter + len(arguments) + len(layout.artifacts) > 32768:
        raise ValueError("Required-target native parameters exceed their bound")
    edges = _projected_edges(layout, first_parameter + len(arguments))
    # Keep missing physical rows as orphan keys so a projected edge cannot disappear from validation.
    relation = f"""(WITH projected AS MATERIALIZED (
      SELECT location_key,array_agg(DISTINCT network_id ORDER BY network_id) AS canonical_network_ids
      FROM ({edges}) actual_edges GROUP BY location_key
    ) SELECT {_address_columns()} FROM projected
    LEFT JOIN LATERAL (
      SELECT * FROM {addresses} address WHERE address.location_key=projected.location_key OFFSET 0
    ) address ON TRUE)"""
    return relation, (*arguments, *(UUID(artifact.candidate_id) for artifact in layout.artifacts))
