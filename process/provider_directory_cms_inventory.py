# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Capture the complete native named storage inventory under its caller-owned guard."""

import asyncio
from contextlib import asynccontextmanager


@asynccontextmanager
async def inventory_transition(admission):
    """Serialize short named storage inventory transitions with their numeric catalog checks."""
    task = asyncio.current_task()
    if admission._inventory_task is task:
        yield
        return
    async with admission._inventory_lock:
        admission._inventory_task = task
        try:
            yield
        finally:
            admission._inventory_task = None


def record_external_identity(admission, schema, name):
    """Retain a created name even when DDL commit acknowledgement is uncertain."""
    if admission._inventory_task is not asyncio.current_task():
        raise RuntimeError("provider_directory_nonprofile_external_identity_invalid")
    admission._relations.add((schema, name))
    admission._external_relations.add((schema, name))


async def measure_inventory(admission, fhir, schema, names, *, observers=None):
    """Retain the complete original inventory while selecting its native reader."""
    from process.provider_directory_cms_preparation import OwnedRelation

    if not admission._started:
        raise RuntimeError("provider_directory_nonprofile_admission_not_started")
    for name in names:
        admission._relations.add((schema, name))
    inventory = sorted(admission._relations)
    group_by_executor_id = {}
    for relation_schema, name in inventory:
        database = None if observers is None else observers.database_for(OwnedRelation(relation_schema, name, 0, "p"))
        database = fhir.db if database is None else database
        group_by_executor_id.setdefault(id(database), (database, []))[1].append((relation_schema, name))
    measured_by_coordinate = {}
    for database, coordinates in group_by_executor_id.values():
        coordinate_by_ref = {fhir._unscoped_qt(*coordinate): coordinate for coordinate in coordinates}
        if len(coordinate_by_ref) != len(coordinates):
            raise RuntimeError("provider_directory_nonprofile_relation_changed")
        catalog_records = await database.all(
            "SELECT requested.relation_ref, "
            "pg_total_relation_size(relation.oid)::bigint AS total_bytes, "
            "relation.relpersistence::text AS persistence "
            "FROM unnest(CAST(:relation_refs AS text[])) AS requested(relation_ref) "
            "JOIN pg_class AS relation ON relation.oid=to_regclass(requested.relation_ref);",
            relation_refs=list(coordinate_by_ref),
        )
        observed_refs = set()
        for catalog_record in catalog_records:
            relation_values = fhir._pagination_checkpoint_row_mapping(catalog_record)
            relation_ref = relation_values.get("relation_ref")
            if type(relation_ref) is not str or relation_ref not in coordinate_by_ref or relation_ref in observed_refs:
                raise RuntimeError("provider_directory_nonprofile_relation_changed")
            observed_refs.add(relation_ref)
            measured_by_coordinate[coordinate_by_ref[relation_ref]] = relation_values
        if observed_refs != coordinate_by_ref.keys():
            raise RuntimeError("provider_directory_nonprofile_relation_changed")
    measured_relations = []
    for relation_schema, name in inventory:
        relation_values = measured_by_coordinate[(relation_schema, name)]
        total_bytes = relation_values.get("total_bytes")
        if type(total_bytes) is not int or total_bytes < 0 or relation_values.get("persistence") not in {"u", "p"}:
            raise RuntimeError("provider_directory_nonprofile_relation_storage_invalid")
        measured = OwnedRelation(relation_schema, name, total_bytes, relation_values["persistence"])
        if observers is not None:
            observers.record(measured)
        measured_relations.append(measured)
    return tuple(measured_relations)
