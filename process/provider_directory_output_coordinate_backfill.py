# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Normalize selected coordinates on an isolated complete Location replacement."""

from db.plan_release_storage import storage_relation
from process.provider_directory_output_snapshot_catalog import output_models, require_output

TARGET = "provider_directory_location"


def coordinate_projection(directory, selector):
    """Preserve the established native normalization and exact run/source selection."""
    expressions = directory._provider_directory_location_coordinate_sql_map("loc")
    scopes = directory._provider_directory_location_coordinate_scopes(
        "loc",
        db_schema=directory._schema(),
        run_id=selector["run_id"],
        source_ids=selector["source_ids"],
        seen_table=None,
    )
    eligibility = directory._provider_directory_location_coordinate_where_sql("loc", expressions, scopes)
    latitude = directory._coordinate_text_sql(expressions["normalized_lat_expr"])
    longitude = directory._coordinate_text_sql(expressions["normalized_long_expr"])
    prefix = (
        "WITH computed AS (SELECT loc.*,"
        f"CASE WHEN {expressions['numeric_guard']} THEN ({eligibility}) ELSE FALSE END AS repair_eligible,"
        f"{latitude} AS repair_latitude,{longitude} AS repair_longitude "
        f"FROM {storage_relation(directory._schema(), TARGET)} loc), candidates AS "
        "(SELECT c.*,repair_eligible AND (latitude IS DISTINCT FROM repair_latitude OR "
        "longitude IS DISTINCT FROM repair_longitude) AS repair_changed FROM computed c) "
    )
    selected_columns = []
    for column in output_models()[TARGET].__table__.columns:
        name = str(column.name)
        if name in {"latitude", "longitude"}:
            expression = f"CASE WHEN c.repair_changed THEN c.repair_{name} ELSE c.{name} END"
        elif name == "updated_at":
            expression = "CASE WHEN c.repair_changed THEN CAST(:update_timestamp AS timestamp) ELSE c.updated_at END"
        else:
            expression = "c." + directory._q(name)
        selected_columns.append(expression + " AS " + directory._q(name))
    params_by_name = {"after_source_id": None, "after_resource_id": None}
    if selector["run_id"] is not None:
        params_by_name["run_id"] = selector["run_id"]
    if selector["source_ids"]:
        params_by_name["source_ids"] = selector["source_ids"]
    return (
        prefix + "SELECT " + ",".join(selected_columns) + " FROM candidates c",
        prefix + "SELECT count(*) FILTER(WHERE repair_changed) FROM candidates",
        params_by_name,
    )


async def backfill_location_coordinates(directory, db_schema=None, *, run_id=None, source_ids=None, seen_table=None):
    """Use bounded staged publication; mutable legacy seen inputs require protected custody."""
    from process.provider_directory_artifact_scratch_inputs import prepared_scratch_enrichment
    from process.provider_directory_output_location_rebuild import rebuild_location_snapshot

    prepared = await prepared_scratch_enrichment(
        directory, "coordinate", db_schema=db_schema, run_id=run_id, source_ids=source_ids, seen_table=seen_table
    )
    if prepared is not None:
        return prepared

    require_output(db_schema is None or db_schema == directory._schema(), "schema_changed")
    require_output(seen_table is None, "coordinate_seen_custody_required")
    return await rebuild_location_snapshot(
        directory,
        kind="coordinate_backfill",
        run_id=run_id,
        selector={"run_id": run_id, "source_ids": source_ids},
    )
