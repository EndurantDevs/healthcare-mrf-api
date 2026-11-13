# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Connected metadata projection, sealing, replay and retained-row regression."""

import json
from dataclasses import asdict, replace
from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest
from sqlalchemy import MetaData
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateTable

from db.models.network_registry import NetworkRegistryAlias, NetworkRegistryIdentity
from db.registry_fhir_catalog import table_sql
from process import registry_fhir_catalog_sync as sync
from process.registry_fhir_catalog_custody import FHIRCatalogTarget
from process.registry_fhir_catalog_extraction import extract_fhir_catalog_batch
from process.registry_fhir_catalog_fingerprints import approved_fhir_catalog_fingerprints
from process.registry_fhir_catalog_grant import FHIRCatalogStandingGrant
from process.registry_fhir_catalog_projection import SourceCatalogContext
from process.registry_fhir_catalog_sources import FHIRCatalogSourceDisposition, fhir_catalog_source_generation
from process.registry_fhir_catalog_store import (
    copy_fhir_catalog_snapshot,
    create_fhir_catalog_candidate,
    create_fhir_catalog_indexes,
)
from process.registry_fhir_catalog_sync import (
    project_fhir_catalog_source,
    project_manual_fhir_catalog,
    refresh_carried_fhir_catalog_relationships,
)
from process.registry_fhir_catalog_validation import canonical_json, metadata_generation
from tests.test_registry_fhir_catalog_custody_postgres import (
    expected_snapshot,
    freeze_metadata,
)
from tests.test_registry_fhir_catalog_custody_postgres import (
    metadata_custody as metadata_custody,
)
from tests.test_registry_fhir_catalog_enrichment import COMPANY, GROUP, context, source_row
from tests.test_registry_fhir_catalog_sources import descriptor, read_profile

ACCEPTED_AT = datetime(2026, 1, 1, tzinfo=timezone.utc)


async def _named_replacement_rows(fixture, *, is_typed):
    """Stage predecessor bindings from distinct logical scopes in one isolated candidate."""
    schema = "catalog_replacement_" + uuid4().hex
    fixture.schemas.append(schema)
    namespace = '"' + schema + '"'
    relation_by_kind = {
        kind: namespace + ".registry_fhir_catalog_" + kind for kind in ("binding", "relationship", "source")
    }
    await fixture.connection.execute(
        f"CREATE SCHEMA {namespace}; CREATE TABLE {relation_by_kind['source']} (source_generation text,descriptor_json jsonb);"
        f"CREATE TABLE {relation_by_kind['binding']} (binding_id uuid PRIMARY KEY,document_json jsonb);"
        f"CREATE TABLE {relation_by_kind['relationship']} (binding_id uuid)"
    )
    replacement_key = "UHCPlanTier/key" if is_typed else "network"
    source_specs = (
        ("named-old", "catalog-a", "endpoint", "edition-old", replacement_key),
        ("legacy-old", None, "endpoint", "catalog-a", replacement_key),
        ("other-endpoint", "catalog-a", "other", "edition-old", replacement_key),
        ("other-scope", "catalog-b", "endpoint", "edition-old", replacement_key),
        ("absent", "catalog-a", "endpoint", "edition-old", "not-replaced"),
    )
    binding_id_by_generation = {}
    for generation, catalog, endpoint, payload_schema, source_key in source_specs:
        previous_descriptor_by_field = {"endpoint_id": endpoint, "coordinates": {"dataset_schema": payload_schema}}
        if catalog is not None:
            previous_descriptor_by_field["catalog_schema"] = catalog
        binding_id = uuid4()
        binding_id_by_generation[generation] = binding_id
        binding_by_field = {
            "source_id": "previous-alias",
            "source_key": source_key,
            "dataset_schema": payload_schema,
            "evidence_id": generation + ":Organization/" + source_key,
            "source_scope": {"kind": "uhc_plan_tier_v1", "source_generation": generation},
        }
        await fixture.connection.execute(
            f"INSERT INTO {relation_by_kind['source']} VALUES($1,$2::jsonb)",
            generation,
            json.dumps(previous_descriptor_by_field),
        )
        await fixture.connection.execute(
            f"INSERT INTO {relation_by_kind['binding']} VALUES($1,$2::jsonb)", binding_id, json.dumps(binding_by_field)
        )
        await fixture.connection.execute(f"INSERT INTO {relation_by_kind['relationship']} VALUES($1)", binding_id)
    return schema, relation_by_kind, replacement_key, binding_id_by_generation


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ["observed", "independent", "typed"])
async def test_named_successor_retires_bindings_across_payload_schemas(metadata_custody, path):
    from process.registry_fhir_catalog_projection_validation import _ProjectionReplay
    from process.registry_fhir_plan_tier_catalog import replace_plan_tier_bindings
    from tests.test_registry_fhir_catalog_grant import snapshot, standing_grant

    fixture = metadata_custody
    schema, relation_by_kind, replacement_key, binding_id_by_generation = await _named_replacement_rows(
        fixture, is_typed=path == "typed"
    )
    successor_descriptor_by_field = {
        "endpoint_id": "endpoint",
        "catalog_schema": "catalog-a",
        "coordinates": {"source_id": "new-alias", "dataset_schema": "edition-new"},
    }
    if path == "typed":
        await replace_plan_tier_bindings(
            fixture.connection, relation_by_kind, successor_descriptor_by_field, [replacement_key]
        )
    elif path == "observed":
        await sync._replace_observed_bindings(
            fixture.connection, '"' + schema + '"', successor_descriptor_by_field, [replacement_key]
        )
    else:
        grant = standing_grant()
        selected_snapshot_by_field = {**snapshot(grant), "schema_name": schema}
        replay = _ProjectionReplay(fixture.connection, selected_snapshot_by_field, grant, None)
        replay.targets.update(relation_by_kind)
        await replay.replace_bindings(successor_descriptor_by_field, [replacement_key])
    retained_binding_ids = {binding_id_by_generation[name] for name in ("other-endpoint", "other-scope", "absent")}
    for kind in ("binding", "relationship"):
        binding_rows = await fixture.connection.fetch(f"SELECT binding_id FROM {relation_by_kind[kind]}")
        assert {binding_row["binding_id"] for binding_row in binding_rows} == retained_binding_ids
    assert await fixture.connection.fetchval(f"SELECT count(*) FROM {relation_by_kind['source']}") == 5
    assert successor_descriptor_by_field["coordinates"]["dataset_schema"] == "edition-new"


async def _registry(fixture):
    connection, namespace = fixture.connection, '"' + fixture.control_schema + '"'
    await connection.execute(f"DROP TABLE {namespace}.network_registry_identity")
    for model in (NetworkRegistryIdentity, NetworkRegistryAlias):
        table = model.__table__.to_metadata(MetaData(), schema=fixture.control_schema)
        await connection.execute(str(CreateTable(table).compile(dialect=postgresql.dialect())))
    for statement in table_sql(namespace, "registry_fhir_catalog_snapshot"):
        await connection.execute(statement)
    await connection.execute(
        f"INSERT INTO {namespace}.network_registry_identity(network_id,allocation_key) "
        "OVERRIDING SYSTEM VALUE VALUES(7,$1)",
        uuid4(),
    )
    await connection.execute(
        f"INSERT INTO {namespace}.network_registry_alias "
        "(source_system,source_id,alias_type,alias_value,scope_key,network_id,evidence_id) "
        "VALUES('fhir','alias-a','fhir_organization','network-old','fhir-catalog/v1',7,'existing-evidence')"
    )
    await connection.execute(
        f"INSERT INTO {namespace}.network_registry_alias "
        "(source_system,source_id,alias_type,alias_value,scope_key,network_id,evidence_id) "
        "VALUES('fhir','alias-a','fhir_organization','network-shadow','fhir-catalog/v1',7,'existing-evidence')"
    )
    maps = context()
    approved_records = [
        ("company", company["company_id"], {**company, "archived": False}) for company in maps.approved_companies
    ]
    approved_records += [("group", group["group_id"], {**group, "archived": False}) for group in maps.approved_groups]
    approved_records.append(
        (
            "network",
            "7",
            {
                "network_id": 7,
                "display_name": "Reviewed network name",
                "aliases": [],
                "archived": False,
                "revision": 2,
            },
        )
    )
    approved_records.append(("company_links", COMPANY, {"company_id": COMPANY, "archived": False, "network_ids": [7]}))
    await connection.copy_records_to_table(
        "registry_approved_record",
        schema_name=fixture.control_schema,
        records=[
            (6, kind, key, document.get("revision", 1), 1, canonical_json(document))
            for kind, key, document in approved_records
        ],
        columns=("approved_revision", "record_kind", "record_key", "record_revision", "custom_revision", "record_json"),
    )
    await connection.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA {namespace} TO "{fixture.roles["publisher"]}"')
    return FHIRCatalogStandingGrant(
        "metadata-regression", 1, fixture.control_schema, (read_profile(),), (("alias-a", "insurer", COMPANY),)
    )


def _edition_descriptor(edition):
    """Preserve the admitted receipt contract while selecting one synthetic edition."""
    source = descriptor()
    source["coordinates"].update(dataset_id=edition, edition_id=edition)
    source["accepted_receipt_id"] = descriptor()["accepted_receipt_id"].rsplit(":", 1)[0] + ":" + edition
    return source


def _catalog_resource_rows(network_names):
    """Build Organization and InsurancePlan resources for the staged-source fixture."""
    resource_rows = [source_row("Organization", "insurer", {"name": "Example Insurance"})]
    for resource_id, name in network_names.items():
        resource_rows.append(source_row("Organization", resource_id, {"name": name, "type_codes": [{"code": "ntwk"}]}))
        resource_rows.append(
            source_row(
                "InsurancePlan",
                "plan-" + resource_id,
                {
                    "name": "Gold " + resource_id,
                    "owned_by_ref": "Organization/insurer",
                    "network_refs": ["Organization/" + resource_id],
                },
            )
        )
    return resource_rows


async def _staged_source(fixture, catalog_target, edition, network_names, *, source_descriptor=None):
    """Use extraction output; native Source admission is covered by its own fixture."""
    source_descriptor = source_descriptor or _edition_descriptor(edition)
    generation = fhir_catalog_source_generation(source_descriptor)
    resource_rows = _catalog_resource_rows(network_names)
    extracted = extract_fhir_catalog_batch(resource_rows, profile="published_normalized")
    observations = [
        (generation, "Organization", observation.resource_id, "organization", 0, canonical_json(asdict(observation)))
        for observation in extracted.organizations
    ]
    observations += [
        (
            generation,
            observation.owner_resource_type,
            observation.owner_resource_id,
            "edge",
            0,
            canonical_json(asdict(observation)),
        )
        for observation in extracted.edges
    ]
    await fixture.connection.copy_records_to_table(
        "registry_fhir_catalog_observation",
        schema_name=catalog_target.schema_name,
        records=observations,
        columns=("source_generation", "resource_type", "resource_id", "observation_kind", "ordinal", "document_json"),
    )
    await fixture.connection.execute(
        f'INSERT INTO "{catalog_target.schema_name}".registry_fhir_catalog_source '
        "(source_generation,source_id,descriptor_json,profile,source_rows,observation_rows,accepted_at) "
        "VALUES($1,$2,$3::jsonb,'published_normalized',$4,$5,$6)",
        generation,
        source_descriptor["coordinates"]["source_id"],
        canonical_json(source_descriptor),
        len(resource_rows),
        len(observations),
        ACCEPTED_AT,
    )
    return FHIRCatalogSourceDisposition(
        source_descriptor["endpoint_id"],
        edition,
        tuple(source_descriptor["source_aliases"]),
        len(source_descriptor["source_aliases"]),
        "accepted",
        None,
        source_descriptor,
        ACCEPTED_AT,
    )


async def _snapshot(fixture, target, grant, vector, *, revision=6):
    snapshot = expected_snapshot(fixture, target)
    async with fixture.connection.transaction():
        maps = (
            await approved_fhir_catalog_fingerprints(
                fixture.connection,
                (revision,),
                control_schema=fixture.control_schema,
            )
        )[revision]
    snapshot.update(
        standing_grant_sha256=grant.sha256,
        source_vector_json=sorted(vector, key=lambda row: row["source_generation"]),
        membership_mapping_sha256=maps["membership"],
        mapping_sha256=maps["metadata"],
        metadata_approved_revision=revision,
    )
    snapshot["metadata_generation"] = metadata_generation(snapshot)
    return snapshot


async def _project(fixture, target, source, snapshot, grant):
    async with fixture.connection.transaction():
        await project_manual_fhir_catalog(
            fixture.connection,
            target,
            snapshot,
            loader_role=fixture.roles["loader"],
            control_schema=fixture.control_schema,
        )
        return await project_fhir_catalog_source(
            fixture.connection,
            target,
            source,
            snapshot,
            grant=grant,
            loader_role=fixture.roles["loader"],
        )


async def _catalog_networks(fixture, catalog_target):
    """Read stable network fields for comparisons across sealed editions."""
    return await fixture.connection.fetch(
        f'SELECT network_id,display_name,archived FROM "{catalog_target.schema_name}".registry_fhir_catalog_network '
        "ORDER BY network_id"
    )


async def _catalog_bindings(fixture, catalog_target):
    """Read complete bindings to detect identity or acceptance changes."""
    return await fixture.connection.fetch(
        f'SELECT binding_id,network_id,document_json::text FROM "{catalog_target.schema_name}".registry_fhir_catalog_binding '
        "ORDER BY binding_id"
    )


async def _new_catalog_target(fixture):
    """Register exact candidate cleanup before creating its native heaps."""
    candidate = uuid4()
    catalog_target = FHIRCatalogTarget(
        str(uuid4()), str(uuid4()), str(uuid4()), str(candidate), "fhir_catalog_" + candidate.hex
    )
    fixture.schemas.append(catalog_target.schema_name)
    async with fixture.connection.transaction():
        await create_fhir_catalog_candidate(
            fixture.connection,
            catalog_target,
            loader_role=fixture.roles["loader"],
            control_schema=fixture.control_schema,
        )
    return catalog_target


async def _copy_catalog(fixture, previous, catalog_target, snapshot):
    """Carry immutable facts through the real bounded snapshot-copy path."""
    async with fixture.connection.transaction():
        await copy_fhir_catalog_snapshot(
            fixture.connection,
            previous,
            catalog_target,
            snapshot["metadata_generation"],
            loader_role=fixture.roles["loader"],
            control_schema=fixture.control_schema,
        )


async def _seal_catalog(fixture, catalog_target, snapshot):
    """Prepare indexes and exercise the actual metadata sealing gate."""
    async with fixture.connection.transaction():
        await create_fhir_catalog_indexes(
            fixture.connection,
            catalog_target,
            loader_role=fixture.roles["loader"],
            control_schema=fixture.control_schema,
        )
    return await freeze_metadata(fixture, catalog_target, snapshot)


async def _initial_catalog(fixture, grant):
    """Project and replay the original source before any disappearance."""
    source_disposition = await _staged_source(
        fixture,
        fixture.target,
        "edition-one",
        {
            "network-old": "Imported name",
            "network-shadow": "Another source Organization for the same network",
            "network-new": "New network",
        },
    )
    generation = fhir_catalog_source_generation(source_disposition.descriptor)
    source_entries = [{"source_generation": generation, "source_id": "alias-a", "status": "current"}]
    snapshot = await _snapshot(fixture, fixture.target, grant, source_entries)
    projected = await _project(fixture, fixture.target, source_disposition, snapshot, grant)
    replay = await _project(fixture, fixture.target, source_disposition, snapshot, grant)
    assert projected["created_networks"] == 1 and replay["created_networks"] == 0
    assert projected["relationships"] == replay["relationships"] == 6
    assert (
        await fixture.connection.fetchval(
            f'SELECT count(*) FROM "{fixture.target.schema_name}".registry_fhir_catalog_relationship '
            "WHERE source_id='registry-manual' AND company_id=$1 AND group_id=$2 "
            "AND document_json->>'resolution_status'='resolved'",
            UUID(COMPANY),
            UUID(GROUP),
        )
        == 1
    )
    networks = await _catalog_networks(fixture, fixture.target)
    assert (
        next(network_row["display_name"] for network_row in networks if network_row["network_id"] == 7)
        == "Reviewed network name"
    )
    assert len(networks) == 2 and all(not network_row["archived"] for network_row in networks)
    sealed = await freeze_metadata(fixture, snapshot=snapshot)
    return SimpleNamespace(generation=generation, snapshot=snapshot, sealed=sealed, networks=networks)


async def _superseding_catalog(fixture, grant, first):
    """Replace two observed Organizations while retaining the missing original."""
    catalog_target = await _new_catalog_target(fixture)
    source_descriptor = _edition_descriptor("edition-two")
    generation = fhir_catalog_source_generation(source_descriptor)
    snapshot = await _snapshot(
        fixture,
        catalog_target,
        grant,
        [
            {**first.snapshot["source_vector_json"][0], "status": "last_good"},
            {"source_generation": generation, "source_id": "alias-a", "status": "current"},
        ],
    )
    await _copy_catalog(fixture, first.sealed, catalog_target, snapshot)
    source_disposition = await _staged_source(
        fixture,
        catalog_target,
        "edition-two",
        {"network-new": "Renamed network", "network-shadow": "Changed shadow name"},
    )
    projected = await _project(fixture, catalog_target, source_disposition, snapshot, grant)
    assert projected["created_networks"] == 0
    sealed = await _seal_catalog(fixture, catalog_target, snapshot)
    return SimpleNamespace(
        target=catalog_target,
        snapshot=snapshot,
        sealed=sealed,
        networks=await _catalog_networks(fixture, catalog_target),
    )


async def _assert_disappearance(fixture, first, second):
    """Check stable canonical IDs, manual names and last-good source metadata."""
    namespace = '"' + second.target.schema_name + '"'
    assert {network_row["network_id"] for network_row in first.networks} == {
        network_row["network_id"] for network_row in second.networks
    }
    assert next(network_row for network_row in second.networks if network_row["network_id"] == 7) == next(
        network_row for network_row in first.networks if network_row["network_id"] == 7
    )
    assert any(network_row["display_name"] == "Renamed network" for network_row in second.networks)
    assert (
        await fixture.connection.fetchval(
            f"SELECT document_json->>'status' FROM {namespace}.registry_fhir_catalog_binding "
            "WHERE network_id=7 AND document_json->>'source_key'='network-old'"
        )
        == "last_good"
    )
    assert (
        await fixture.connection.fetchval(
            f"SELECT count(*) FROM {namespace}.registry_fhir_catalog_relationship WHERE network_id=7 AND company_id=$1",
            UUID(COMPANY),
        )
        == 5
    )
    assert second.sealed["accounting_json"]["registry_fhir_catalog_network"]["rows"] == 2
    assert (
        await fixture.connection.fetchval(
            f'SELECT count(*) FROM "{fixture.control_schema}".registry_approved_record WHERE record_key=$1',
            GROUP,
        )
        == 1
    )


async def _change_organization_metadata(fixture, company_archived):
    """Create a new approved metadata revision without changing source membership."""
    await fixture.connection.execute(
        f'INSERT INTO "{fixture.control_schema}".registry_approved_record '
        "(approved_revision,record_kind,record_key,record_revision,custom_revision,record_json) "
        "SELECT 7,record_kind,record_key,record_revision,custom_revision,CASE "
        "WHEN record_kind='company' THEN jsonb_set("
        "jsonb_set(record_json,'{display_name}','\"Updated insurance\"'::jsonb),'{archived}',to_jsonb($1::boolean)) "
        "WHEN record_kind='group' THEN jsonb_set(record_json,'{display_name}','\"Updated parent\"'::jsonb) "
        "ELSE record_json END "
        f'FROM "{fixture.control_schema}".registry_approved_record WHERE approved_revision=6',
        company_archived,
    )


async def _refresh_cached_catalog(fixture, grant, second, company_archived):
    """Re-enrich only carried source relationships and seal the resulting snapshot."""
    await _change_organization_metadata(fixture, company_archived)
    catalog_target = await _new_catalog_target(fixture)
    snapshot = await _snapshot(
        fixture,
        catalog_target,
        grant,
        [{**entry, "status": "last_good"} for entry in second.snapshot["source_vector_json"]],
        revision=7,
    )
    await _copy_catalog(fixture, second.sealed, catalog_target, snapshot)
    async with fixture.connection.transaction():
        await project_manual_fhir_catalog(
            fixture.connection,
            catalog_target,
            snapshot,
            loader_role=fixture.roles["loader"],
            control_schema=fixture.control_schema,
        )
        refreshed_count = await refresh_carried_fhir_catalog_relationships(
            fixture.connection,
            catalog_target,
            snapshot,
            current_generations=(),
            grant=grant,
            loader_role=fixture.roles["loader"],
        )
    sealed = await _seal_catalog(fixture, catalog_target, snapshot)
    assert refreshed_count > 0
    assert sealed["accounting_json"]["registry_fhir_catalog_network"]["rows"] == 2
    return catalog_target


async def _assert_cached_identity(fixture, first, second, refreshed_target, bindings):
    """Detect binding resurrection, acceptance changes or canonical-name replacement."""
    updated = await _catalog_bindings(fixture, refreshed_target)
    assert [(row["binding_id"], row["network_id"]) for row in bindings] == [
        (row["binding_id"], row["network_id"]) for row in updated
    ]
    assert [{**json.loads(row["document_json"]), "status": "last_good"} for row in bindings] == [
        json.loads(row["document_json"]) for row in updated
    ]
    assert await _catalog_networks(fixture, refreshed_target) == second.networks
    assert (
        await fixture.connection.fetchval(
            f'SELECT count(*) FROM "{refreshed_target.schema_name}".registry_fhir_catalog_binding '
            "WHERE document_json->>'evidence_id'=$1",
            first.generation + ":Organization/network-shadow",
        )
        == 0
    )


async def _assert_cached_organizations(fixture, refreshed_target, company_archived):
    """Require current names or unresolved replacements for an archived company."""
    namespace = '"' + refreshed_target.schema_name + '"'
    if company_archived:
        assert (
            await fixture.connection.fetchval(
                f"SELECT count(*) FROM {namespace}.registry_fhir_catalog_relationship WHERE company_id=$1",
                UUID(COMPANY),
            )
            == 0
        )
        assert (
            await fixture.connection.fetchval(
                f"SELECT count(*) FROM {namespace}.registry_fhir_catalog_relationship "
                "WHERE source_id='alias-a' AND company_id IS NULL AND document_json->>'resolution_status'='unresolved'"
            )
            > 0
        )
    else:
        assert await fixture.connection.fetchval(
            f"SELECT bool_and(document_json->'company'->>'display_name'='Updated insurance' "
            "AND document_json->'group'->>'display_name'='Updated parent') "
            f"FROM {namespace}.registry_fhir_catalog_relationship "
            "WHERE source_id='alias-a' AND company_id=$1 AND group_id=$2",
            UUID(COMPANY),
            UUID(GROUP),
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("company_archived", [False, True])
async def test_projection_sealing_replay_and_disappearance_preserve_identity(metadata_custody, company_archived):
    """Keep canonical identity while disappeared sources receive current organization metadata."""
    fixture = metadata_custody
    grant = await _registry(fixture)
    first = await _initial_catalog(fixture, grant)
    second = await _superseding_catalog(fixture, grant, first)
    await _assert_disappearance(fixture, first, second)
    bindings = await _catalog_bindings(fixture, second.target)
    refreshed_target = await _refresh_cached_catalog(fixture, grant, second, company_archived)
    await _assert_cached_identity(fixture, first, second, refreshed_target, bindings)
    await _assert_cached_organizations(fixture, refreshed_target, company_archived)


async def _approve_pending_catalog_network(fixture, network_id):
    """Add a reviewed name and binding for a source identity already present in the sealed catalog."""
    pending_binding = str(uuid4())
    network_by_field = {
        "network_id": network_id,
        "display_name": "Pending approved name",
        "aliases": [],
        "archived": False,
        "revision": 1,
    }
    binding_by_field = {
        "binding_id": pending_binding,
        "binding_key": "b" * 64,
        "network_id": network_id,
        "archived": False,
        "revision": 1,
        "source_system": "fhir",
        "source_id": "alias-a",
        "source_key": "network-new",
        "producer_id": "registry-source-fhir-witness-v1",
        "source_scope_json": {"organization_id": "network-new", "capture_sha256": "c" * 64},
    }
    namespace = '"' + fixture.control_schema + '"'
    await fixture.connection.execute(
        f"INSERT INTO {namespace}.registry_approved_record "
        "SELECT 7,record_kind,record_key,record_revision,custom_revision,record_json "
        f"FROM {namespace}.registry_approved_record WHERE approved_revision=6"
    )
    await fixture.connection.copy_records_to_table(
        "registry_approved_record",
        schema_name=fixture.control_schema,
        columns=("approved_revision", "record_kind", "record_key", "record_revision", "custom_revision", "record_json"),
        records=[
            (7, "network", str(network_id), 1, 6, canonical_json(network_by_field)),
            (7, "network_binding", pending_binding, 1, 6, canonical_json(binding_by_field)),
        ],
    )
    assert (
        await fixture.connection.fetchval(
            f"SELECT network_id FROM {namespace}.network_registry_alias WHERE source_system='fhir' "
            "AND source_id='alias-a' AND alias_type='fhir_organization' AND alias_value='network-new' "
            "AND scope_key='fhir-catalog/v1'"
        )
        == network_id
    )
    return pending_binding


async def _unrelated_catalog_source(fixture, target):
    """Stage the same Organization key under a separate source and endpoint identity."""
    new_descriptor = _edition_descriptor("unrelated-edition")
    new_descriptor["coordinates"]["source_id"] = "alias-unrelated"
    new_descriptor.update(endpoint_id="endpoint-unrelated", source_aliases=["alias-unrelated"])
    return await _staged_source(
        fixture,
        target,
        "unrelated-edition",
        {"network-old": "Unrelated source network"},
        source_descriptor=new_descriptor,
    )


@pytest.mark.asyncio
async def test_pending_approved_maps_do_not_block_unrelated_source_projection(metadata_custody):
    """Exercise extraction through sealing; native Source admission remains the staged-source fixture boundary."""
    fixture = metadata_custody
    grant = await _registry(fixture)
    grant = replace(grant, company_crosswalks=(*grant.company_crosswalks, ("alias-unrelated", "insurer", COMPANY)))
    first = await _initial_catalog(fixture, grant)
    prior_bindings = await _catalog_bindings(fixture, fixture.target)
    pending_id = next(
        catalog_row["network_id"] for catalog_row in first.networks if catalog_row["display_name"] == "New network"
    )
    pending_binding = await _approve_pending_catalog_network(fixture, pending_id)
    catalog_target = await _new_catalog_target(fixture)
    source_disposition = await _unrelated_catalog_source(fixture, catalog_target)
    head_by_field = {"approved_revision": 7}
    manifest = SimpleNamespace(generation_id=9, manifest_sha256="f" * 64, approved_custom_revision=6)
    async with fixture.connection.transaction():
        snapshot, current = await sync._catalog_snapshot_inputs(
            fixture.connection, grant, head_by_field, manifest, first.sealed, [source_disposition]
        )
    snapshot["schema_name"] = catalog_target.schema_name
    assert snapshot["metadata_approved_revision"] == 6 and head_by_field["approved_revision"] == 7
    assert snapshot["network_generation"] == 9
    assert snapshot["mapping_sha256"] == first.snapshot["mapping_sha256"]
    assert snapshot["membership_mapping_sha256"] == first.snapshot["membership_mapping_sha256"]
    generation = fhir_catalog_source_generation(source_disposition.descriptor)
    assert list(current) == [generation]
    await _copy_catalog(fixture, first.sealed, catalog_target, snapshot)
    projected = await _project(fixture, catalog_target, source_disposition, snapshot, grant)
    assert (
        await fixture.connection.fetchval(
            f'SELECT bool_and(company_id=$1) FROM "{catalog_target.schema_name}".registry_fhir_catalog_relationship '
            "WHERE source_id='alias-unrelated'",
            UUID(COMPANY),
        )
        is True
    )
    sealed = await _seal_catalog(fixture, catalog_target, snapshot)
    assert projected["created_networks"] == 1
    networks = await _catalog_networks(fixture, catalog_target)
    assert all(catalog_row in networks for catalog_row in first.networks)
    assert any(
        catalog_row["display_name"] == "Unrelated source network" and catalog_row["network_id"] != 7
        for catalog_row in networks
    )
    assert all(catalog_row["display_name"] != "Pending approved name" for catalog_row in networks)
    bindings = await _catalog_bindings(fixture, catalog_target)
    assert {(catalog_row["binding_id"], catalog_row["network_id"]) for catalog_row in prior_bindings} <= {
        (catalog_row["binding_id"], catalog_row["network_id"]) for catalog_row in bindings
    }
    assert all(str(catalog_row["binding_id"]) != pending_binding for catalog_row in bindings)
    assert any(json.loads(catalog_row["document_json"])["source_id"] == "alias-unrelated" for catalog_row in bindings)
    assert sealed["metadata_approved_revision"] == 6
    assert sealed["accounting_json"]["registry_fhir_catalog_source"]["rows"] == 2
    assert sealed["accounting_json"]["registry_fhir_catalog_network"]["rows"] == 3
    assert await _catalog_networks(fixture, fixture.target) == first.networks
