# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Reuse the existing bounded native catalog commit envelope for initial renames."""

from types import SimpleNamespace

from process import provider_directory_serving_inverse_capacity_contract as contract


async def required_commit_bytes(fhir, scope, native):
    """Count the bounded native rename closure without projecting unrelated catalog deletes."""
    contract.validated_inverse_scope(scope)
    block = contract.integer(native["block_size"], positive=True)
    wal_block = contract.integer(native["wal_block_size"], positive=True)
    if not any(entry["restore_mode"] == "initial_heap" for entry in scope["families"]):
        # Existing control-row DML charges one native block per ordinary commit.
        return block
    from process import provider_directory_profile_failed_cleanup as cleanup

    oids = set()
    for family in scope["families"]:
        if family["restore_mode"] == "initial_heap":
            oids.update((family["root_oid"], family["retained_oid"]))
    contract.require(len(oids) == 4, "initial_commit_roots")
    dependencies = await rename_dependencies(fhir, oids)
    return cleanup._commit_wal_bound(dependencies, SimpleNamespace(postgres_wal_block_size_bytes=wal_block))


async def assert_commit_reserve(fhir, geometry, native):
    """Keep signed reserve separate from already measured preparation/cut WAL."""
    required = await required_commit_bytes(fhir, geometry["scope"], native)
    contract.require(required <= geometry["commit_wal_upper_bound_bytes"], "commit_reserve_too_small")
    return required


async def rename_dependencies(fhir, oids):
    """Observe only the four actual heaps participating in the initial inverse."""
    from process import provider_directory_profile_failed_cleanup as cleanup

    contract.require(type(oids) is set and len(oids) == 4, "initial_commit_roots")
    for oid in oids:
        contract.oid(oid)
    dependencies = []
    for oid in sorted(oids):
        objects, relation_count = await cleanup._stage_dependency_closure(fhir, oid)
        # PG18 renames update pg_class, pg_type and index-owning pg_constraint
        # rows. Counting the entire supported closure also funds unchanged
        # objects. A heap rename can move one conflicting automatic array type;
        # makeArrayTypeName then chooses unused names for the array renames.
        # No catalog payload or TOAST row is deleted by these operations.
        dependencies.append(
            {
                "object_count": len(objects),
                "relation_count": relation_count,
                "catalog_tuple_count": len(objects) + 1,
                "fingerprint": cleanup.digest(objects, "provider-directory-inverse-rename-closure.v1"),
            }
        )
    return dependencies
