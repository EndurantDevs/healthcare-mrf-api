# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Physical address pins for an authenticated immutable bootstrap receipt."""

import json
import re

from process import network_address_artifact as artifact
from process import network_bootstrap_read_access as access
from process import network_bootstrap_sources as bootstrap
from process.mrf_address_publication import require_native_read_catalog_subjects
from process.network_address_projection import PinnedAddressSource
from process.network_compact_layout import CompactAddressRelation

CONTRACT = "network_bootstrap_address_artifact.postgres.v1"
_HEAPS = {"entity_address_unified", "npi"}
_FIELDS = {
    "contract",
    "database",
    "namespace",
    "protected_owner_oid",
    "table",
    "heaps",
    "bootstrap_receipt",
    "generation_id",
    "native_sha256",
    "dormant_sequence_oid",
    "dependency_oids",
    "runtime_roles",
    "descriptor_sha256",
}


def is_bootstrap_source(source):
    """Only the existing exact bootstrap namespace selects this contract."""
    return re.fullmatch(r"network_bootstrap_[0-9a-f]{32}", source.schema_name) is not None


def _oid(value):
    artifact._require(type(value) is int and 0 < value < 2**32, "descriptor_invalid")
    return value


def _descriptor_receipt(binding):
    artifact._require(type(binding["bootstrap_receipt"]) is dict, "descriptor_invalid")
    receipt = bootstrap._decode(artifact._json(binding["bootstrap_receipt"]))
    artifact._require(
        binding["bootstrap_receipt"] == json.loads(artifact._json(receipt.as_dict()))
        and binding["generation_id"] == receipt.generation_sha256,
        "generation_invalid",
    )
    artifact._require(
        type(binding["namespace"]) is dict and set(binding["namespace"]) == {"name", "oid", "owner_oid"},
        "descriptor_invalid",
    )
    for oid in (binding["namespace"]["oid"], binding["namespace"]["owner_oid"], binding["protected_owner_oid"]):
        _oid(oid)
    artifact._require(
        binding["namespace"]
        == {"name": receipt.address_source.schema_name, "oid": receipt.schema_oid, "owner_oid": receipt.owner_oid}
        and binding["protected_owner_oid"] == receipt.owner_oid,
        "descriptor_invalid",
    )
    _oid(receipt.schema_oid)
    _oid(receipt.owner_oid)
    return receipt


def _descriptor(binding):
    artifact._require(type(binding) is dict and set(binding) == _FIELDS, "descriptor_invalid")
    artifact._require(
        binding["contract"] == CONTRACT and len(artifact._json(binding).encode()) <= artifact._MAX_DESCRIPTOR_BYTES,
        "descriptor_invalid",
    )
    artifact._require(
        artifact._digest({key: entry for key, entry in binding.items() if key != "descriptor_sha256"})
        == binding["descriptor_sha256"],
        "descriptor_digest_invalid",
    )
    receipt = _descriptor_receipt(binding)
    artifact._require(
        type(binding["database"]) is dict
        and set(binding["database"]) == {"oid", "name"}
        and type(binding["database"]["name"]) is str,
        "descriptor_invalid",
    )
    _oid(binding["database"]["oid"])
    artifact._require(type(binding["heaps"]) is dict and set(binding["heaps"]) == _HEAPS, "descriptor_invalid")
    artifact._require(set(dict(receipt.relation_oids)) == _HEAPS, "descriptor_invalid")
    for name, identity in binding["heaps"].items():
        artifact._require(type(identity) is dict and set(identity) == {"oid", "relfilenode"}, "descriptor_invalid")
        _oid(identity["oid"])
        _oid(identity["relfilenode"])
        artifact._require(identity["oid"] == dict(receipt.relation_oids)[name], "descriptor_invalid")
    artifact._require(
        type(binding["table"]) is dict and set(binding["table"]) == {"oid", "relfilenode"}, "descriptor_invalid"
    )
    _oid(binding["table"]["oid"])
    _oid(binding["table"]["relfilenode"])
    artifact._require(binding["table"] == binding["heaps"]["entity_address_unified"], "descriptor_invalid")
    dependencies = {_oid(oid) for _, oid in (*receipt.relation_oids, *receipt.index_oids)}
    sequence_oid = binding["dormant_sequence_oid"]
    if sequence_oid is not None:
        _oid(sequence_oid)
        artifact._require(sequence_oid not in dependencies, "descriptor_invalid")
        dependencies.add(sequence_oid)
    artifact._require(type(binding["dependency_oids"]) is list, "descriptor_invalid")
    for oid in binding["dependency_oids"]:
        _oid(oid)
    artifact._require(binding["dependency_oids"] == sorted(dependencies), "descriptor_invalid")
    artifact._require(
        type(binding["native_sha256"]) is str and re.fullmatch("[0-9a-f]{64}", binding["native_sha256"]) is not None,
        "descriptor_invalid",
    )
    artifact._require(type(binding["runtime_roles"]) is list, "roles_invalid")
    artifact._roles(tuple(binding["runtime_roles"]))
    return json.loads(artifact._json(binding))


async def _physical_heaps(connection, receipt):
    heaps_by_name = {}
    for name, oid in receipt.relation_oids:
        physical = await artifact._physical(connection, oid)
        artifact._require(
            physical is not None
            and physical["relname"] == name
            and physical["schema_name"] == receipt.address_source.schema_name
            and physical["schema_oid"] == receipt.schema_oid
            and physical["schema_owner_oid"] == physical["owner_oid"] == receipt.owner_oid
            and physical["relkind"] == "r"
            and physical["relpersistence"] == "p"
            and not any(
                physical[key]
                for key in ("relrowsecurity", "relforcerowsecurity", "relispartition", "rules", "inherited")
            ),
            "heap_custody_invalid",
        )
        heaps_by_name[name] = {"oid": oid, "relfilenode": physical["relfilenode"]}
    return heaps_by_name


async def _captured_descriptor(connection, receipt, runtime_roles, *, api_read):
    await artifact._require_api_read_context(connection, api_read)
    await bootstrap._transaction(connection)
    runtime_roles = artifact._roles(runtime_roles)
    caller = await connection.fetchrow("SELECT current_user::text AS name,current_user::regrole::oid::bigint AS oid")
    subjects = tuple(sorted(set((*runtime_roles, caller["name"]))))
    # Output actors need safe reach, not an invented grant to read both inputs.
    await access.bootstrap_pin_read_roles(
        connection, receipt.address_source, (), runtime_actors=subjects, api_read=api_read
    )
    await require_native_read_catalog_subjects(
        artifact._CatalogSession(connection), [oid for _, oid in receipt.relation_oids], subjects
    )
    native = await connection.fetchrow(access._NATIVE_SQL, receipt.schema_oid)
    access._require(native["safe"] is True)
    document = json.loads(native["document"])
    # Actor reach was checked above; this authenticates the same native serial dependency.
    dormant = await access._dormant_sequence(connection, receipt, document, caller["oid"], roles=())
    document["dormant_sequence"] = dormant
    sequence_oid = int(dormant["sequence"][0]) if dormant is not None else None
    # The protected receipt already proved copied content; reuse retains current custody and worker locks.
    await bootstrap._verify_capture(connection, receipt, verify_content=False, api_read=api_read)
    heaps_by_name = await _physical_heaps(connection, receipt)
    dependencies = {oid for _, oid in (*receipt.relation_oids, *receipt.index_oids)}
    if sequence_oid is not None:
        dependencies.add(sequence_oid)
    descriptor_by_field = {
        "contract": CONTRACT,
        "database": await artifact._database(connection),
        "namespace": {
            "name": receipt.address_source.schema_name,
            "oid": receipt.schema_oid,
            "owner_oid": receipt.owner_oid,
        },
        "protected_owner_oid": receipt.owner_oid,
        "table": heaps_by_name["entity_address_unified"],
        "heaps": heaps_by_name,
        "bootstrap_receipt": receipt.as_dict(),
        "generation_id": receipt.generation_sha256,
        "native_sha256": access._digest(document),
        "dormant_sequence_oid": sequence_oid,
        "dependency_oids": sorted(dependencies),
        "runtime_roles": list(runtime_roles),
    }
    descriptor_by_field = json.loads(artifact._json(descriptor_by_field))
    descriptor_by_field["descriptor_sha256"] = artifact._digest(descriptor_by_field)
    return _descriptor(descriptor_by_field)


async def capture_bootstrap_address_artifact(connection, source, *, runtime_roles):
    """Authenticate the closed bootstrap receipt and establish its current physical pin."""
    artifact._require(
        type(source) is PinnedAddressSource
        and source.table_name == "entity_address_unified"
        and is_bootstrap_source(source),
        "source_invalid",
    )
    receipt = bootstrap._decode(
        await connection.fetchval(
            "SELECT obj_description(oid,'pg_namespace') FROM pg_namespace WHERE nspname=$1", source.schema_name
        )
    )
    artifact._require(source == receipt.address_source, "source_generation_changed")
    return await _captured_descriptor(connection, receipt, runtime_roles, api_read=False)


async def verify_bootstrap_address_artifact(connection, descriptor, *, runtime_roles, api_read=False):
    """Revalidate a sealed descriptor without binding it to a renewable access proof."""
    descriptor = _descriptor(descriptor)
    artifact._require(descriptor["runtime_roles"] == list(artifact._roles(runtime_roles)), "roles_changed")
    receipt = bootstrap._decode(artifact._json(descriptor["bootstrap_receipt"]))
    current = await _captured_descriptor(connection, receipt, runtime_roles, api_read=api_read)
    artifact._require(current == descriptor, "bootstrap_artifact_changed")
    return CompactAddressRelation(descriptor["namespace"]["name"], "entity_address_unified", descriptor["table"]["oid"])
