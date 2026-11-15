# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Prepared index names for the validated artifact family."""

from db.plan_release_storage import storage_identifier, storage_relation
from process import provider_directory_artifact_scratch_inputs as inputs
from process.provider_directory_output_snapshot_catalog import decoded, require_output


def _relation_indexes(schema, relation, indexes):
    """Require complete usable index names without catalog identity tokens."""
    storage_identifier(schema)
    storage_identifier(relation)
    require_output(
        bool(indexes)
        and all(
            type(index) is dict
            and type(index.get("name")) is str
            and all(index.get(flag) is True for flag in ("valid", "ready", "live"))
            for index in indexes
        ),
        "scratch_index_inventory_changed",
    )
    names = tuple(sorted(index["name"] for index in indexes))
    for name in names:
        storage_identifier(name)
    require_output(len(set(names)) == len(names), "scratch_index_inventory_changed")
    return dict(schema=schema, relation=relation, indexes=names)


async def scratch_index_metadata(directory, scope):
    """Read current index names inside the existing validated, pinned family."""
    async with inputs.scratch_input_transaction(directory, scope) as capability:
        source = decoded(scope.source_copy_json)
        coordinates_by_root = {inputs.SOURCE: (capability.policy.schema, source["name"])}
        for receipt in scope.prepared:
            successor = receipt.successor
            require_output(successor.target not in coordinates_by_root, "scratch_index_inventory_changed")
            coordinates_by_root[successor.target] = (successor.schema, successor.relation)
        require_output(
            set(coordinates_by_root) == {inputs.SOURCE, *inputs.TYPED_ROOTS}
            and len(set(coordinates_by_root.values())) == len(coordinates_by_root),
            "scratch_index_inventory_changed",
        )
        indexes_by_root = {}
        for root, (schema, relation) in coordinates_by_root.items():
            rows = await capability.connection.fetch(
                "SELECT index_relation.relname AS name,indisvalid AS valid,indisready AS ready,indislive AS live "
                "FROM pg_index index_entry JOIN pg_class index_relation ON index_relation.oid=index_entry.indexrelid "
                "WHERE index_entry.indrelid=to_regclass($1) ORDER BY index_relation.relname",
                storage_relation(schema, relation),
            )
            indexes_by_root[root] = _relation_indexes(schema, relation, [dict(row) for row in rows])
        return indexes_by_root
