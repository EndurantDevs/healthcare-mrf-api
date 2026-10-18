# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Preserve the fixed contact canonicalization before a retained Location cut."""

from db.plan_release_storage import storage_relation
from process.provider_directory_output_snapshot_catalog import output_models

TARGET = "provider_directory_location"
_CONTACT_FIELDS = ("phone_number", "phone_extension", "fax_number_digits", "fax_extension")


def contact_projection(directory):
    """Use the exact existing native phone expressions while preserving other cells."""
    qualified = storage_relation(directory._schema(), TARGET)
    expressions = (
        directory._canonical_contact_number_expr("loc.telephone_number", "loc.country_code"),
        directory._contact_extension_expr("loc.telephone_number"),
        directory._canonical_contact_number_expr("loc.fax_number", "loc.country_code"),
        directory._contact_extension_expr("loc.fax_number"),
    )
    computed = ",".join(
        f"({expression})::varchar AS repair_{name}"
        for name, expression in zip(_CONTACT_FIELDS, expressions, strict=True)
    )
    eligible = "NULLIF(BTRIM(COALESCE(loc.telephone_number,'')),'') IS NOT NULL OR NULLIF(BTRIM(COALESCE(loc.fax_number,'')),'') IS NOT NULL"
    prefix = (
        f"WITH computed AS (SELECT loc.*,{computed},({eligible}) AS repair_eligible FROM {qualified} loc), candidates AS (SELECT c.*,repair_eligible AND ("
        + " OR ".join(f"c.{name} IS DISTINCT FROM c.repair_{name}" for name in _CONTACT_FIELDS)
        + ") AS repair_changed FROM computed c) "
    )
    columns = []
    for column in output_models()[TARGET].__table__.columns:
        name = str(column.name)
        if name in _CONTACT_FIELDS:
            expression = f"CASE WHEN c.repair_eligible THEN c.repair_{name} ELSE c.{name} END"
        elif name == "updated_at":
            expression = "CASE WHEN c.repair_changed THEN CAST(:update_timestamp AS timestamp) ELSE c.updated_at END"
        else:
            expression = "c." + directory._q(name)
        columns.append(expression + " AS " + directory._q(name))
    return prefix + "SELECT " + ",".join(
        columns
    ) + " FROM candidates c", prefix + "SELECT count(*) FILTER(WHERE repair_changed) FROM candidates"


async def backfill_location_contacts(directory, *, run_id=None):
    """Apply only the fixed contact producer through bounded Location staging."""
    from process.provider_directory_artifact_scratch_inputs import prepared_scratch_enrichment
    from process.provider_directory_output_location_rebuild import rebuild_location_snapshot

    prepared = await prepared_scratch_enrichment(directory, "contact", run_id=run_id)
    if prepared is not None:
        return prepared

    changed = await rebuild_location_snapshot(directory, kind="contact_backfill", run_id=run_id)
    return {"location_contact_rows_updated": changed}
