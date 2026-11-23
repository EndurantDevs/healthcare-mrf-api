# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Prepare one native catalogue cohort without advancing a serving pointer."""

import json
from contextvars import ContextVar
from dataclasses import dataclass
from types import SimpleNamespace

from db.plan_release_storage import storage_identifier
from db.registry_fhir_catalog import SNAPSHOT_COLUMNS
from process.network_fhir_source_witness_contract import digest
from process.registry_fhir_catalog_sources import (
    fhir_catalog_source_generation,
    validate_fhir_catalog_source_descriptor,
)
from process.registry_imported_source_inventory import (
    ImportedSourceInventory,
    fhir_feed,
    is_practitioner_enrichment,
    select_fhir_editions,
)
from process.registry_imported_source_reuse import source_edition
from process.registry_native_network_catalog import native_catalog_descriptor, native_catalog_rows, native_network_ids

_INVENTORY = ContextVar("registry_imported_catalog_preparation", default=None)
INPUT_KEY = "imported_source_inventory"


@dataclass(frozen=True)
class ImportedCatalogPreparation:
    inventory_token: str
    metadata_generation: str
    schema_name: str
    snapshot: dict
    previous: dict | None
    accounting: dict


def current_catalog_inventory():
    """Read the task-local input pinned by the native catalogue producer."""
    return _INVENTORY.get()


async def replaced_native_generations(connection, previous, accepted):
    """Retire only complete native replacements, retaining absent or unavailable source identities."""
    if previous is None:
        return ()
    source_ids = [
        source.descriptor["coordinates"]["source_id"]
        for source in accepted
        if source.descriptor.get("contract") == "registry-native-network-source-v1"
    ]
    if not source_ids:
        return ()
    current_generations = [digest(source.descriptor) for source in accepted]
    rows = await connection.fetch(
        f"SELECT source_generation FROM {storage_identifier(previous['schema_name'])}.registry_fhir_catalog_source "
        "WHERE descriptor_json->>'contract'='registry-native-network-source-v1' AND source_id=ANY($1::text[]) "
        "AND NOT(source_generation=ANY($2::text[]))",
        source_ids,
        current_generations,
    )
    return tuple(row["source_generation"] for row in rows)


async def replaced_fhir_generations(connection, previous, accepted):
    """Omit older copies of current feeds from a new ordinary catalog."""
    inventory = current_catalog_inventory()
    if previous is None or inventory is None:
        return ()
    carried_by_feed = {
        fhir_feed(descriptor): source_edition(descriptor) for descriptor in inventory.carried_fhir_descriptors
    }
    descriptors = inventory.fhir_descriptors
    if not descriptors and not carried_by_feed:
        return ()
    enrichment_feeds = {
        fhir_feed(descriptor)
        for descriptor in (*descriptors, *inventory.carried_fhir_descriptors)
        if is_practitioner_enrichment(descriptor)
    }
    feeds = {fhir_feed(descriptor) for descriptor in descriptors}
    endpoints = {descriptor["endpoint_id"] for descriptor in descriptors} | set(inventory.superseded_fhir_endpoints)
    selected_generations = [fhir_catalog_source_generation(disposition.descriptor) for disposition in accepted]
    prior_sources = await connection.fetch(
        f"SELECT source_generation,descriptor_json::text AS descriptor "
        f"FROM {storage_identifier(previous['schema_name'])}.registry_fhir_catalog_source "
        "WHERE profile='published_normalized' AND descriptor_json->'coordinates'->>'source_system'='fhir' "
        "AND NOT(source_generation=ANY($1::text[])) ORDER BY source_generation",
        selected_generations,
    )
    current_generations = {
        entry["source_generation"] for entry in previous["source_vector_json"] if entry["status"] == "current"
    }
    retained_feeds = set()
    generations = []
    for prior_source in sorted(prior_sources, key=lambda prior: prior["source_generation"] not in current_generations):
        descriptor = json.loads(prior_source["descriptor"])
        feed = fhir_feed(descriptor)
        if feed in enrichment_feeds and not is_practitioner_enrichment(descriptor):
            continue
        if (
            feed in carried_by_feed
            and feed not in retained_feeds
            and source_edition(validate_fhir_catalog_source_descriptor(descriptor)) == carried_by_feed[feed]
        ):
            retained_feeds.add(feed)
            continue
        if feed in feeds or feed in carried_by_feed or descriptor["endpoint_id"] in endpoints:
            generations.append(prior_source["source_generation"])
    return tuple(generations)


async def retire_catalog_source_rows(connection, relations, generations, *, control_schema, approved_revision):
    """Remove superseded metadata from this candidate while preserving shared/manual networks."""
    if not generations:
        return
    removed = await connection.fetch(
        f"DELETE FROM {relations['binding']} "
        "WHERE document_json->'source_scope'->>'source_generation'=ANY($1::text[]) "
        "OR split_part(document_json->>'evidence_id',':',1)=ANY($1::text[]) RETURNING network_id",
        list(generations),
    )
    await connection.execute(
        f"DELETE FROM {relations['relationship']} WHERE source_generation=ANY($1::text[])", list(generations)
    )
    await connection.execute(
        f"DELETE FROM {relations['network']} network WHERE network_id=ANY($1::integer[]) "
        f"AND NOT EXISTS(SELECT FROM {relations['binding']} binding WHERE binding.network_id=network.network_id) "
        f"AND NOT EXISTS(SELECT FROM {relations['relationship']} edge WHERE edge.network_id=network.network_id) "
        f"AND NOT EXISTS(SELECT FROM {storage_identifier(control_schema)}.registry_approved_record manual "
        "WHERE approved_revision=$2 AND record_kind='network' AND record_key=network.network_id::text)",
        sorted({row["network_id"] for row in removed}),
        approved_revision,
    )


async def retire_replaced_catalog_sources(connection, copy_target, snapshot, previous, control_schema):
    """Remove replaced source data from the isolated candidate before projecting new rows."""
    if previous is None:
        return
    surviving_generations = {entry["source_generation"] for entry in snapshot["source_vector_json"]}
    generations = [
        entry["source_generation"]
        for entry in previous["source_vector_json"]
        if entry["source_generation"] not in surviving_generations
    ]
    if not generations:
        return
    namespace = storage_identifier(copy_target.schema_name)
    relations_by_kind = {
        kind: namespace + ".registry_fhir_catalog_" + kind for kind in ("network", "binding", "relationship")
    }
    await retire_catalog_source_rows(
        connection,
        relations_by_kind,
        generations,
        control_schema=control_schema,
        approved_revision=snapshot["metadata_approved_revision"],
    )
    await connection.execute(
        f"DELETE FROM {namespace}.registry_fhir_catalog_observation WHERE source_generation=ANY($1::text[])",
        generations,
    )
    await connection.execute(
        f"DELETE FROM {namespace}.registry_fhir_catalog_source WHERE source_generation=ANY($1::text[])",
        generations,
    )


def require_preparation_fhir_inventory(inventory, accepted):
    """Refuse discovery drift before creating a coupled catalogue candidate."""
    expected_generations = {digest(source_edition(source)) for source in inventory.fhir_descriptors}
    actual_generations = {
        digest(source_edition(source.descriptor))
        for source in accepted
        if source.descriptor["profile"] == "published_normalized"
    }
    if actual_generations != expected_generations:
        raise ValueError("registry_imported_catalog_sources_changed")


def select_preparation_fhir_sources(inventory, accepted):
    """Project only the recognized current edition of each directory feed."""
    published_sources = [source for source in accepted if source.descriptor["profile"] == "published_normalized"]
    preferred, _ = select_fhir_editions(
        [source.descriptor for source in published_sources],
        inventory.carried_fhir_descriptors,
        include_practitioner_enrichment=True,
    )
    winners = {fhir_catalog_source_generation(descriptor) for descriptor in preferred}
    selected_sources = [
        source for source in published_sources if fhir_catalog_source_generation(source.descriptor) in winners
    ]
    require_preparation_fhir_inventory(inventory, selected_sources)
    return selected_sources + [source for source in accepted if source.descriptor["profile"] != "published_normalized"]


def native_catalog_sources(inventory, accepted_at):
    """Wrap native descriptors for the existing isolated batch preparation."""
    return [
        SimpleNamespace(
            descriptor=native_catalog_descriptor(source),
            accepted_at=accepted_at,
            endpoint_id="native:" + source["source_id"],
        )
        for source in inventory.native_sources
    ]


def imported_catalog_accounting(inventory):
    """Record compact source generations without duplicating plan arrays."""
    return {
        INPUT_KEY: {
            "contract": "registry-imported-catalog-inputs-v1",
            "token": inventory.token,
            "native_sources": [
                {"source_id": source["source_id"], "source_generation": digest(native_catalog_descriptor(source))}
                for source in inventory.native_sources
            ],
        }
    }


async def prepare_imported_catalog_snapshot(config, loader, source_pool, sealer_pool, *, source_export, inventory):
    """Use existing isolated preparation, native validation and freeze; publication remains with the caller."""
    from process.registry_fhir_catalog_observed_source import known_fhir_catalog_families
    from process.registry_fhir_catalog_sync import prepare_fhir_catalog_snapshot
    from process.registry_imported_source_inventory import catalog_observation_families

    if type(inventory) is not ImportedSourceInventory:
        raise ValueError("registry_imported_catalog_inventory_invalid")
    token = _INVENTORY.set(inventory)
    try:
        with known_fhir_catalog_families(catalog_observation_families(inventory.fhir_descriptors)):
            snapshot, previous, accounting = await prepare_fhir_catalog_snapshot(
                config, loader, source_pool, sealer_pool, source_export=source_export
            )
    finally:
        _INVENTORY.reset(token)
    prepared = previous if snapshot is None else snapshot
    if prepared is None or prepared["accounting_json"].get(INPUT_KEY, {}).get("token") != inventory.token:
        raise ValueError("registry_imported_catalog_preparation_changed")
    return ImportedCatalogPreparation(
        inventory.token, prepared["metadata_generation"], prepared["schema_name"], prepared, previous, accounting
    )


async def resolve_imported_catalog_preparation(connection, inventory_token, approved_revision, *, control_schema):
    """Resolve only a completed prepared snapshot for this input token and exact manual pin."""
    control = storage_identifier(control_schema)
    rows = await connection.fetch(
        f"SELECT {','.join(SNAPSHOT_COLUMNS)} FROM {control}.registry_fhir_catalog_snapshot "
        "WHERE accounting_json->'imported_source_inventory'->>'token'=$1 AND metadata_approved_revision=$2 "
        "AND as_of=(SELECT max(as_of) FROM " + control + ".registry_fhir_catalog_snapshot "
        "WHERE accounting_json->'imported_source_inventory'->>'token'=$1 AND metadata_approved_revision=$2) LIMIT 2",
        inventory_token,
        approved_revision,
    )
    if len(rows) != 1:
        raise ValueError("registry_imported_catalog_preparation_unavailable")
    snapshot_by_field = dict(rows[0])
    for name in ("accounting_json", "source_vector_json", "custody_json"):
        if type(snapshot_by_field[name]) is str:
            snapshot_by_field[name] = json.loads(snapshot_by_field[name])
    from process.registry_fhir_catalog_custody import validate_fhir_catalog_snapshot_record

    await validate_fhir_catalog_snapshot_record(connection, snapshot_by_field)
    return ImportedCatalogPreparation(
        inventory_token,
        snapshot_by_field["metadata_generation"],
        snapshot_by_field["schema_name"],
        snapshot_by_field,
        None,
        snapshot_by_field["accounting_json"],
    )


async def stage_native_catalog_source(connection, copy_target, disposition, snapshot, *, loader_role, control_schema):
    """Write already frozen native metadata and deterministic definitions through ordinary batch storage."""
    from process.registry_fhir_catalog_store import TABLE_COLUMNS, write_fhir_catalog_rows
    from process.registry_fhir_catalog_validation import canonical_json

    descriptor = disposition.descriptor
    generation = digest(descriptor)
    accepted = (
        await connection.fetchval(
            f"SELECT accepted_at FROM {storage_identifier(copy_target.schema_name)}.registry_fhir_catalog_source WHERE source_generation=$1",
            generation,
        )
        or disposition.accepted_at
    )
    source_record = (
        generation,
        descriptor["coordinates"]["source_id"],
        canonical_json(descriptor),
        descriptor["profile"],
        len(descriptor["input"]["definitions"]),
        0,
        accepted,
    )
    await write_fhir_catalog_rows(
        connection,
        copy_target,
        "registry_fhir_catalog_source",
        (source_record,),
        columns=TABLE_COLUMNS["registry_fhir_catalog_source"][:-1],
        loader_role=loader_role,
        control_schema=control_schema,
    )
    networks = await native_network_ids(connection, descriptor, control_schema=control_schema, allocate=True)
    manual_network_ids = await native_manual_network_ids(connection, snapshot, networks, control_schema)
    projection = native_catalog_rows(
        descriptor,
        generation=generation,
        accepted_at=accepted,
        metadata_generation=snapshot["metadata_generation"],
        networks=networks,
        manual_network_ids=manual_network_ids,
    )
    for table, projection_rows in (
        ("registry_fhir_catalog_network", projection.network_rows),
        ("registry_fhir_catalog_binding", projection.binding_rows),
        ("registry_fhir_catalog_relationship", projection.relationship_rows),
    ):
        await write_fhir_catalog_rows(
            connection, copy_target, table, projection_rows, loader_role=loader_role, control_schema=control_schema
        )
    return {
        "native": True,
        "networks": len(projection.network_rows),
        "bindings": len(projection.binding_rows),
        "relationships": len(projection.relationship_rows),
    }


async def native_manual_network_ids(connection, snapshot, networks, control_schema):
    """Keep approved manual names and archives ahead of imported defaults."""
    rows = await connection.fetch(
        f"SELECT record_key FROM {storage_identifier(control_schema)}.registry_approved_record "
        "WHERE approved_revision=$1 AND record_kind='network' AND record_key=ANY($2::text[])",
        snapshot["metadata_approved_revision"],
        [str(network_id) for network_id in networks.values()],
    )
    return tuple(int(row["record_key"]) for row in rows)
