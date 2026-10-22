# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""CMS global stages share the existing candidate header and owner transaction."""

import hashlib
from contextlib import asynccontextmanager
from contextvars import ContextVar

from db.plan_release_storage import storage_relation
from process import provider_directory_generic_source_storage as storage
from process.provider_directory_generic_source_policy import bound_publisher, source_owner_transaction

_ACTIVE = ContextVar("provider_directory_generic_source_auxiliary", default=None)


def _identity_helper():
    from process import provider_directory_cms_identity_candidate as helper

    storage._require(tuple(helper.CMS_IDENTITY_TABLES) == storage.CMS_IDENTITY_ROOTS, "auxiliary_roots_changed")
    return helper


def _resource_helper():
    from process import provider_directory_resource_identity_candidate as helper

    storage._require(helper.ROOT == storage.CMS_RESOURCE_IDENTITY_ROOT, "auxiliary_roots_changed")
    return helper


def _cms_receipts(receipts):
    return {root: receipts[root] for root in storage.CMS_IDENTITY_ROOTS}


def _seal_digest(identity, receipts):
    return hashlib.sha256(storage._encoded({"identity": identity, "tables": receipts}).encode()).hexdigest()


async def prepare_identity_candidate(publisher, identity, prefix):
    """Allocate fixed private global stages only for the declared CMS producer."""
    if not storage.is_cms_identity(identity):
        return None
    coordinates = identity["metadata"].get("network_bindings")
    storage._require(
        type(coordinates) is dict
        and identity["metadata"]["source_release"].get("source_id") == "cms-npd"
        and coordinates.get("source_system") == "fhir"
        and coordinates.get("source_id") == "cms-npd"
        and coordinates.get("dataset_schema") == publisher.policy.schema
        and coordinates.get("dataset_id") == identity["dataset_id"]
        and coordinates.get("edition_id") == identity["dataset_id"]
        and coordinates.get("producer_id") == "cms-npd-bulk-fhir-v1"
        and coordinates.get("alias_scope") == "cms-npd"
        and coordinates.get("source_key_kind") == "organization_resource_id",
        "auxiliary_producer_changed",
    )
    helper = _identity_helper()
    names_by_root = {root: prefix + "_" + storage._SUFFIXES[root] for root in storage.AUXILIARY_ROOTS}
    connection, policy = publisher.connection, publisher.policy
    for root, name in names_by_root.items():
        await connection.execute(
            f"CREATE TABLE {storage_relation(policy.schema, name)} (LIKE {storage_relation(policy.schema, root)} "
            "INCLUDING DEFAULTS INCLUDING GENERATED INCLUDING CONSTRAINTS INCLUDING STORAGE)"
            + storage.PRIVATE_HEAP_OPTIONS
        )
        await storage._close_grants(connection, policy, name)
    await helper.prepare_identity_stages(publisher, identity, _cms_receipts(names_by_root))
    captured = await storage._capture(connection, policy, list(names_by_root.values()))
    return {
        "phase": "loading",
        "tables": {root: captured[name] for root, name in names_by_root.items()},
        "sealed_sha256": None,
    }


@asynccontextmanager
async def identity_candidate_scope(publisher, binding):
    """Expose only authenticated stages from the current same-task candidate."""
    auxiliary = binding["auxiliary"]
    if auxiliary is None:
        yield
        return
    storage._require(auxiliary["phase"] in {"loading", "sealed"}, "auxiliary_phase_changed")
    active = _ACTIVE.get()
    if active is not None:
        storage._require(
            bound_publisher() is publisher and active[0] is publisher and active[1] == binding,
            "auxiliary_binding_changed",
        )
        yield
        return
    token = _ACTIVE.set((publisher, binding))
    try:
        async with (
            _identity_helper().bind_cms_identity_candidate(
                publisher, binding["identity"], _cms_receipts(auxiliary["tables"])
            ),
            _resource_helper().bind_resource_identity_candidate(
                publisher, binding["identity"], auxiliary["tables"][storage.CMS_RESOURCE_IDENTITY_ROOT]
            ),
        ):
            yield
    finally:
        _ACTIVE.reset(token)


async def seal_identity_candidate(database, directory, candidate):
    """Finish indexes and set checks before relationship validation reads stages."""
    publisher = bound_publisher()
    active = _ACTIVE.get()
    storage._require(active is not None and active[0] is publisher, "auxiliary_scope_required")
    binding = active[1]
    identity = storage.candidate_identity(directory, candidate)
    storage._require(
        identity == binding["identity"] and publisher.policy.schema == directory._schema(), "auxiliary_identity_changed"
    )
    async with source_owner_transaction(database):
        previous = await storage._locked(publisher.connection, publisher.policy, identity, phases=("prepared",))
        storage._require(previous == binding, "auxiliary_binding_changed")
        await storage._verify_tables(publisher.connection, publisher.policy, previous)
        auxiliary = previous["auxiliary"]
        storage._require(auxiliary is not None, "auxiliary_required")
        if auxiliary["phase"] == "sealed":
            storage._require(
                auxiliary["sealed_sha256"] == _seal_digest(identity, auxiliary["tables"]), "auxiliary_seal_changed"
            )
            return
        storage._require(auxiliary["phase"] == "loading", "auxiliary_phase_changed")
        await _identity_helper().finish_identity_stages(publisher, identity, _cms_receipts(auxiliary["tables"]))
        await _resource_helper().finish_resource_identity_stage(
            publisher, identity, auxiliary["tables"][storage.CMS_RESOURCE_IDENTITY_ROOT]
        )
        names = [receipt["name"] for receipt in auxiliary["tables"].values()]
        captured = await storage._capture(publisher.connection, publisher.policy, names)
        receipts_by_root = {root: captured[receipt["name"]] for root, receipt in auxiliary["tables"].items()}
        sealed_by_field = {
            "phase": "sealed",
            "tables": receipts_by_root,
            "sealed_sha256": _seal_digest(identity, receipts_by_root),
        }
        updated_by_field = {**previous, "auxiliary": sealed_by_field}
        await storage._store(publisher.connection, publisher.policy, identity, previous, updated_by_field)
        # The helper's task context holds this exact dictionary. Update it only
        # after native compare-and-swap so subsequent read-only validators see
        # the sealed layout without authorizing another writer transaction.
        for root, receipt in binding["auxiliary"]["tables"].items():
            receipt.clear()
            receipt.update(receipts_by_root[root])
        binding.clear()
        binding.update(updated_by_field)


def relationship_seal(identity, receipt):
    """Bind the completed private reference indexes to this complete acquisition."""
    return hashlib.sha256(storage._encoded({"identity": identity, "relationship": receipt}).encode()).hexdigest()


async def seal_relationship_candidate(database, directory, candidate):
    """Finish relationship indexes once before indexed completeness validation."""
    publisher = bound_publisher()
    active = _ACTIVE.get()
    storage._require(active is not None and active[0] is publisher, "auxiliary_scope_required")
    binding = active[1]
    identity = storage.candidate_identity(directory, candidate)
    storage._require(
        identity == binding["identity"] and publisher.policy.schema == directory._schema(), "auxiliary_identity_changed"
    )
    async with source_owner_transaction(database):
        previous = await storage._locked(publisher.connection, publisher.policy, identity, phases=("prepared",))
        storage._require(previous == binding, "auxiliary_binding_changed")
        await storage._verify_tables(publisher.connection, publisher.policy, previous)
        proof = previous["tables"][storage.RELATIONSHIP]
        if previous["relationship_sha256"] is not None:
            storage._require(
                previous["relationship_sha256"] == relationship_seal(identity, proof), "relationship_seal_changed"
            )
            return
        from process.provider_directory_generic_source_completion import _indexes

        await _indexes(publisher.connection, publisher.policy, storage.RELATIONSHIP, proof["name"])
        captured = await storage._capture(publisher.connection, publisher.policy, [proof["name"]])
        receipt = captured[proof["name"]]
        updated_by_field = {
            **previous,
            "tables": {**previous["tables"], storage.RELATIONSHIP: receipt},
            "relationship_sha256": relationship_seal(identity, receipt),
        }
        await storage._store(publisher.connection, publisher.policy, identity, previous, updated_by_field)
        binding.clear()
        binding.update(updated_by_field)


async def publish_identity_candidate(publisher, binding):
    """Merge only sealed global facts before the same transaction's first attach."""
    auxiliary = binding["auxiliary"]
    if auxiliary is None:
        return None
    identity = binding["identity"]
    storage._require(
        auxiliary["phase"] == "sealed" and auxiliary["sealed_sha256"] == _seal_digest(identity, auxiliary["tables"]),
        "auxiliary_seal_required",
    )
    async with publisher.connection.transaction():
        await storage._verify_auxiliary_tables(publisher.connection, publisher.policy, auxiliary)
        await _identity_helper().publish_identity_stages(publisher, identity, _cms_receipts(auxiliary["tables"]))
        await _resource_helper().publish_resource_identity_stage(
            publisher, identity, auxiliary["tables"][storage.CMS_RESOURCE_IDENTITY_ROOT]
        )
        for receipt in auxiliary["tables"].values():
            await publisher.connection.execute(
                f"DROP TABLE {storage_relation(publisher.policy.schema, receipt['name'])}"
            )
    return {"phase": "published", "tables": {}, "sealed_sha256": auxiliary["sealed_sha256"]}
