# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Borrow the existing issued factory's exact rooted canonical publisher."""

from process import provider_directory_generic_source_policy as source


def current_rooted_graph_runtime(database=None):
    """Return the configured Rooted transaction and named candidate storage."""
    return getattr(source._FACTORY.get(), "rooted_graph_runtime", None)


def rooted_graph_relation_coordinates(schema, root, scoped_name):
    """Use canonical inputs and only the adapter's private candidate overrides."""
    runtime = current_rooted_graph_runtime()
    if runtime is None:
        return None
    return runtime.relation_coordinates(schema, root, scoped_name)


async def rooted_input_relations(database, acquisition_id):
    """Resolve an acquisition from the current generation or a retained logical publication."""
    import json

    from db.plan_release_storage import storage_relation
    from process.provider_directory_generic_source_storage import _require
    from process.provider_directory_source_storage_binding import named_source_document

    runtime = current_rooted_graph_runtime(database)
    _require(runtime is not None, "rooted_runtime_required")
    acquisition = storage_relation(runtime.candidate_schema, "provider_directory_rooted_graph_acquisition")
    present = await database.scalar(
        f"SELECT EXISTS(SELECT FROM {acquisition} WHERE acquisition_id=:acquisition_id)", acquisition_id=acquisition_id
    )
    roots = ("acquisition", "work", "resource", "edge", "twin_attempt", "twin_admission")
    if present:
        return {
            "provider_directory_rooted_graph_" + root: {
                "schema": runtime.candidate_schema,
                "name": "provider_directory_rooted_graph_" + root,
            }
            for root in roots
        }
    parent = storage_relation(runtime.catalog_schema, "provider_directory_endpoint_dataset")
    publication_rows = await database.all(
        f"SELECT candidate_storage_json, (candidate_storage_json->'admission'->>'publication_acquisition_id'=:acquisition_id) "
        f"AS own_publication FROM {parent} WHERE status IN ('published','superseded') AND ("
        "candidate_storage_json->'admission'->>'publication_acquisition_id'=:acquisition_id OR "
        "candidate_storage_json->'admission'->>'comparison_acquisition_id'=:acquisition_id) "
        "ORDER BY own_publication DESC,published_at,dataset_id LIMIT 2",
        acquisition_id=acquisition_id,
    )
    _require(bool(publication_rows), "rooted_acquisition_changed")
    publication_records = [
        dict(publication_row._mapping if hasattr(publication_row, "_mapping") else publication_row)
        for publication_row in publication_rows
    ]
    _require(
        not (
            len(publication_records) == 2
            and publication_records[0]["own_publication"] is True
            and publication_records[1]["own_publication"] is True
        ),
        "rooted_acquisition_changed",
    )
    encoded = publication_records[0]["candidate_storage_json"]
    document = named_source_document(json.loads(encoded) if isinstance(encoded, str) else encoded)
    table = document["inputs"]["provider_directory_rooted_graph_acquisition"]
    exists = await database.scalar(
        f"SELECT EXISTS(SELECT FROM {storage_relation(table['schema'], table['name'])} "
        "WHERE acquisition_id=:acquisition_id)",
        acquisition_id=acquisition_id,
    )
    _require(exists is True, "rooted_acquisition_changed")
    return document["inputs"]
