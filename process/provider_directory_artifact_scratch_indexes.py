# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native prepared index identities for the artifact manifest; no read authority."""

from db.plan_release_storage import storage_identifier
from process import provider_directory_artifact_scratch_inputs as inputs
from process.provider_directory_output_snapshot_catalog import decoded, require_output


def _relation_indexes(schema, relation, oid, indexes):
    """Project exact native names and OIDs without reconstructing legacy names."""
    storage_identifier(schema)
    storage_identifier(relation)
    require_output(type(oid) is int and 0 < oid < 2**32, "scratch_index_inventory_changed")
    require_output(
        bool(indexes)
        and all(
            type(index) is dict
            and type(index.get("oid")) is int
            and 0 < index["oid"] < 2**32
            and type(index.get("name")) is str
            and all(index.get(flag) is True for flag in ("valid", "ready", "live"))
            for index in indexes
        ),
        "scratch_index_inventory_changed",
    )
    identities = tuple(sorted((index["name"], index["oid"]) for index in indexes))
    for name, _oid in identities:
        storage_identifier(name)
    require_output(
        len({name for name, _oid in identities}) == len({oid for _name, oid in identities}) == len(identities),
        "scratch_index_inventory_changed",
    )
    return dict(schema=schema, relation=relation, oid=oid, indexes=identities)


async def scratch_index_metadata(directory, scope):
    """Reattest the issued original-task family before returning navigation facts.

    Callers must compare these exact index names and OIDs with their fresh native
    manifest catalog. This detached inventory does not authorize a SQL session.
    """
    async with inputs.scratch_input_transaction(directory, scope) as capability:
        source = decoded(scope.source_copy_json)["copy"]
        relation_by_root = {
            inputs.SOURCE: _relation_indexes(
                capability.policy.schema, source["relname"], source["oid"], decoded(source["indexes"])
            )
        }
        for receipt in scope.prepared:
            successor = receipt.successor
            require_output(successor.target not in relation_by_root, "scratch_index_inventory_changed")
            relation_by_root[successor.target] = _relation_indexes(
                successor.schema,
                successor.relation,
                successor.oid,
                [decoded(index) for index in successor.indexes],
            )
        require_output(set(relation_by_root) == {inputs.SOURCE, *inputs.TYPED_ROOTS}, "scratch_index_inventory_changed")
        require_output(
            len({relation["oid"] for relation in relation_by_root.values()}) == len(relation_by_root),
            "scratch_index_inventory_changed",
        )
        return relation_by_root
