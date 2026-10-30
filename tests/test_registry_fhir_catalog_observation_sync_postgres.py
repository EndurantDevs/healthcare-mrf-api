# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Imported observations cross genuine Reader, Loader, Sealer and publication gates."""

import asyncio
import json
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from uuid import UUID, uuid4

import asyncpg
import pytest
from sqlalchemy import MetaData
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateTable

from db.models.registry_publication_request import RegistryPublicationRequest
from process.network_membership_writer_closure import writer_role_names
from process.registry_fhir_catalog_custody import freeze_fhir_catalog_snapshot
from process.registry_fhir_catalog_grant import (
    FHIRCatalogStandingGrant,
    build_fhir_catalog_observation_verifier,
    build_fhir_catalog_publication_runtime,
)
from process.registry_fhir_catalog_observation import CATALOG_OBSERVATION_CONTRACT
from process.registry_fhir_catalog_publication import (
    OPERATION,
    complete_queued_fhir_catalog_publication,
    enqueue_fhir_catalog_publication,
)
from process.registry_fhir_catalog_read import FHIRCatalogQuery, read_fhir_catalog
from process.registry_fhir_catalog_reference_rollup import fhir_catalog_reference_workspace
from process.registry_fhir_catalog_sources import fhir_catalog_source_generation, iter_fhir_catalog_sources
from process.registry_fhir_catalog_store import copy_fhir_catalog_snapshot, create_fhir_catalog_indexes
from process.registry_fhir_catalog_sync import (
    prepare_fhir_catalog_snapshot,
    project_fhir_catalog_source,
    project_manual_fhir_catalog,
    stage_fhir_catalog_source,
)
from process.registry_fhir_catalog_validation import fhir_catalog_observation_digest, metadata_generation
from process.registry_publication_queue import claim_registry_publication_request
from tests.test_registry_fhir_catalog_custody_postgres import metadata_custody as metadata_custody
from tests.test_registry_fhir_catalog_enrichment import COMPANY, GROUP
from tests.test_registry_fhir_catalog_observed_source_postgres import (
    _add_second_scope_entry,
    published_metadata_counts,
    refresh_published_profile,
)
from tests.test_registry_fhir_catalog_observed_source_postgres import observed_source_db as observed_source_db
from tests.test_registry_fhir_catalog_sync_postgres import _new_catalog_target, _registry, _snapshot

pytestmark = pytest.mark.asyncio


async def _source_facts(source_fixture, *, edition="dataset-one", retain_old=True):
    networks = ("network-new", "network-old") if retain_old else ("network-new",)
    resources = [
        ("Organization", "insurer", {"name": "Example Insurance", "part_of_ref": "Organization/parent"}),
        ("Organization", "parent", {"name": "Example Parent"}),
    ]
    for identifier in networks:
        resources.extend(
            [
                ("Organization", identifier, {"name": "Imported " + identifier, "type_codes": ["ntwk"]}),
                (
                    "InsurancePlan",
                    "plan-" + identifier,
                    {
                        "name": "Gold " + identifier,
                        "owned_by_ref": "Organization/insurer",
                        "administered_by_ref": "Organization/insurer",
                        "network_refs": ["Organization/" + identifier],
                    },
                ),
            ]
        )
    resources.extend(
        [
            ("OrganizationAffiliation", "affiliation", {"network_refs": ["Organization/network-new"]}),
            ("PractitionerRole", "role", {"network_refs": ["Organization/network-new"]}),
        ]
    )
    await source_fixture.admin.execute(
        f'DELETE FROM "{source_fixture.schema}".provider_directory_dataset_resource WHERE dataset_id=$1', edition
    )
    await source_fixture.admin.copy_records_to_table(
        "provider_directory_dataset_resource",
        schema_name=source_fixture.schema,
        records=[(edition, kind, identity, "d" * 64, json.dumps(fields)) for kind, identity, fields in resources],
    )


async def _control_tables(fixture):
    control = '"' + fixture.control_schema + '"'
    await fixture.connection.execute(f"""CREATE TABLE {control}.registry_revision_control(id integer PRIMARY KEY,approved_revision bigint);
      INSERT INTO {control}.registry_revision_control VALUES(1,6);
      CREATE TABLE {control}.network_serving_control(id integer PRIMARY KEY,generation_id bigint,metadata_generation text);
      INSERT INTO {control}.network_serving_control VALUES(1,NULL,NULL)""")
    queue = RegistryPublicationRequest.__table__.to_metadata(MetaData(), schema=fixture.control_schema)
    await fixture.connection.execute(str(CreateTable(queue).compile(dialect=postgresql.dialect())))
    database = await fixture.connection.fetchval("SELECT quote_ident(current_database())")
    await fixture.connection.execute(f'GRANT CREATE ON DATABASE {database} TO "{fixture.roles["loader"]}"')
    for kind in ("loader", "reader", "publisher"):
        role = fixture.roles[kind]
        await fixture.connection.execute(f'GRANT USAGE ON SCHEMA {control} TO "{role}"')
        await fixture.connection.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA {control} TO "{role}"')
    await fixture.connection.execute(
        f'GRANT INSERT(allocation_key) ON {control}.network_registry_identity TO "{fixture.roles["loader"]}"'
    )
    await fixture.connection.execute(f'GRANT USAGE ON ALL SEQUENCES IN SCHEMA {control} TO "{fixture.roles["loader"]}"')
    await fixture.connection.execute(f'GRANT INSERT ON {control}.network_registry_alias TO "{fixture.roles["loader"]}"')
    await fixture.connection.execute(
        f'GRANT INSERT ON {control}.registry_fhir_catalog_snapshot TO "{fixture.roles["publisher"]}"'
    )


@pytest.fixture
async def observation_case(metadata_custody, observed_source_db):
    fixture, source_fixture = metadata_custody, observed_source_db
    await _registry(fixture)
    await _control_tables(fixture)
    await source_fixture.admin.execute(f'''ALTER TABLE "{source_fixture.schema}".provider_directory_endpoint_dataset
      ADD COLUMN content_proof_resource_types text[],ADD COLUMN publication_metadata_summary_json jsonb,
      ADD COLUMN completion_proof_required_version integer,ADD COLUMN completion_proof_json jsonb,
      ADD COLUMN completion_proof_sha256 text''')
    if source_fixture.layout != "SCOPED":
        await source_fixture.admin.execute(
            f'CREATE TABLE "{source_fixture.schema}".provider_directory_insurance_network_source_binding('
            f"source_id text,resource_id text,network_id uuid);"
            f'GRANT SELECT ON "{source_fixture.schema}".provider_directory_insurance_network_source_binding '
            f'TO "{source_fixture.roles_by_kind["reader"]}"'
        )
    for role in (source_fixture.roles_by_kind["reader"], fixture.roles["publisher"]):
        await fixture.connection.execute(f'GRANT EXECUTE ON FUNCTION pg_control_system() TO "{role}"')
    await _source_facts(source_fixture)
    if source_fixture.layout == "SCOPED":
        await source_fixture.admin.execute(
            f'UPDATE "{source_fixture.schema}".provider_directory_insurance_network_source_binding '
            f"SET resource_id='network-old'"
        )
        await refresh_published_profile(source_fixture)
    grant = FHIRCatalogStandingGrant(
        "observed-metadata",
        1,
        fixture.control_schema,
        (source_fixture.profile,),
        (("alias-a", "insurer", COMPANY),),
        observation_contract=CATALOG_OBSERVATION_CONTRACT,
    )

    @asynccontextmanager
    async def source_reader():
        reader = await source_fixture.connect()
        try:
            yield reader
        finally:
            await reader.close()

    config = SimpleNamespace(
        grant=grant,
        loader_role=fixture.roles["loader"],
        reader_roles=tuple(sorted((fixture.roles["reader"], fixture.roles["loader"]))),
        consumer=SimpleNamespace(physical_owner_role=fixture.roles["owner"]),
    )
    return SimpleNamespace(
        fixture=fixture,
        source=source_fixture,
        grant=grant,
        config=config,
        source_pool=SimpleNamespace(acquire=source_reader),
        sealer_pool=fixture.sealer_pool,
    )


async def _prepare(case):
    prepared = await prepare_fhir_catalog_snapshot(case.config, case.fixture.loader, case.source_pool, case.sealer_pool)
    snapshot, previous, accounting = prepared
    if snapshot is not None and snapshot["schema_name"] not in case.fixture.schemas:
        case.fixture.schemas.append(snapshot["schema_name"])
    return prepared


async def _publish(case, snapshot, previous=None):
    connection, control = case.fixture.connection, case.fixture.control_schema
    runtime = build_fhir_catalog_publication_runtime(case.grant, None)
    command_by_field = {
        "operation": OPERATION,
        "metadata_generation": snapshot["metadata_generation"],
        "expected_metadata_generation": previous["metadata_generation"] if previous else None,
        "expected_serving_generation": None,
        "expected_approved_revision": 6,
        "standing_grant_sha256": case.grant.sha256,
    }
    async with connection.transaction():
        request = await enqueue_fhir_catalog_publication(
            connection, command_by_field, authorize_metadata=runtime.authorize_metadata, control_schema=control
        )
        claimed = await claim_registry_publication_request(
            connection, operation_family="publication", allow_metadata=True, control_schema=control
        )
    assert str(claimed["request_id"]) == request["request_id"]
    publication_result = await complete_queued_fhir_catalog_publication(
        connection,
        UUID(request["request_id"]),
        claimed["lease_id"],
        authorize_metadata=runtime.authorize_metadata,
        verify_sources=runtime.verify_sources,
        source_fence=runtime.source_fence,
        control_schema=control,
    )
    assert publication_result["canonical_entities_deleted"] == publication_result["canonical_entities_archived"] == 0
    return publication_result


async def _query(case):
    async with case.fixture.reader.transaction(isolation="repeatable_read", readonly=True):
        return await read_fhir_catalog(
            case.fixture.reader,
            FHIRCatalogQuery(limit=50),
            excluded_network_ids=(),
            control_schema=case.fixture.control_schema,
        )


def _manual_facts(page):
    return [
        (item["network_id"], relationship["relationship_id"], relationship["company"], relationship["group"])
        for item in page["items"]
        for relationship in item["matching_relationships"]["items"]
        if relationship["source_id"] == "registry-manual"
    ]


@pytest.mark.parametrize("observed_source_db", [("HASH", 2), ("HASH", 3), ("HASH", None)], indirect=True)
async def test_historical_observations_discover_seal_publish_query_and_replay(observation_case):
    case = observation_case
    snapshot, previous, accounting = await _prepare(case)
    assert previous is None and accounting["accepted_sources"] == 1 and accounting["unavailable_sources"] == 0
    await _publish(case, snapshot)
    page = await _query(case)
    assert page["generation"] is None and page["metadata_generation"] == snapshot["metadata_generation"]
    assert page["total"] == 2
    reviewed = next(item for item in page["items"] if item["network_id"] == 7)
    assert reviewed["display_name"] == "Reviewed network name" and reviewed["archived"] is False
    relationships = [relationship for item in page["items"] for relationship in item["matching_relationships"]["items"]]
    assert any(row["source_id"] == "registry-manual" for row in relationships)
    assert any(
        row["source_id"] == "alias-a" and row["company"]["company_id"] == COMPANY and row["group"]["group_id"] == GROUP
        for row in relationships
        if row["company"] and row["group"]
    )
    assert (
        await case.fixture.loader.fetchval(
            f"SELECT document_json->>'part_of_ref' FROM \"{snapshot['schema_name']}\".registry_fhir_catalog_observation "
            "WHERE resource_type='Organization' AND resource_id='insurer'"
        )
        == "Organization/parent"
    )
    replay, retained, replay_accounting = await _prepare(case)
    assert replay is None and retained["metadata_generation"] == snapshot["metadata_generation"]
    assert replay_accounting["accepted_sources"] == 1


@pytest.mark.parametrize("observed_source_db", [("SCOPED", 2)], indirect=True)
async def test_leaf_only_source_pipeline_seals_publishes_and_replays_without_directory_change(observation_case):
    """Use the real scoped login through Loader, Sealer and exact metadata publication."""
    case = observation_case
    connection, control = case.fixture.connection, '"' + case.fixture.control_schema + '"'
    original_identity = await case.source.admin.fetchval(
        f'SELECT network_id FROM "{case.source.schema}".provider_directory_insurance_network_source_binding'
    )
    original_control = await connection.fetchrow(
        f"SELECT c.generation_id,r.approved_revision FROM {control}.network_serving_control c "
        f"CROSS JOIN {control}.registry_revision_control r WHERE c.id=1 AND r.id=1"
    )
    snapshot, previous, accounting = await _prepare(case)
    assert previous is None and accounting["accepted_sources"] == 1 and accounting["unavailable_sources"] == 0
    assert snapshot["network_generation"] is snapshot["manifest_sha256"] is None
    await _publish(case, snapshot)
    page = await _query(case)
    assert page["generation"] is None and page["metadata_generation"] == snapshot["metadata_generation"]
    assert any(
        network["network_id"] == 7 and network["display_name"] == "Reviewed network name" for network in page["items"]
    )
    assert (
        await connection.fetchrow(
            f"SELECT c.generation_id,r.approved_revision FROM {control}.network_serving_control c "
            f"CROSS JOIN {control}.registry_revision_control r WHERE c.id=1 AND r.id=1"
        )
        == original_control
    )
    assert (
        await case.source.admin.fetchval(
            f'SELECT network_id FROM "{case.source.schema}".provider_directory_insurance_network_source_binding'
        )
        == original_identity
    )
    replay, retained, replay_accounting = await _prepare(case)
    assert replay is None and retained["metadata_generation"] == snapshot["metadata_generation"]
    assert replay_accounting["accepted_sources"] == 1
    reader = await case.source.connect()
    assert not await reader.fetchval(
        "SELECT has_table_privilege(current_user,$1,'SELECT')",
        f'"{case.source.schema}".provider_directory_dataset_resource',
    )


@pytest.mark.parametrize("observed_source_db", [("SCOPED", 2)], indirect=True)
async def test_published_scope_pipeline_and_sealer_replay_share_linear_metadata_proofs(observation_case):
    """Count genuine native preparation and independent sealer replay per reader snapshot."""
    case, query_counts = observation_case, []
    await _add_second_scope_entry(case.source, published=True)
    case.grant = replace(case.grant, source_profiles=(case.source.profile,))
    case.config.grant = case.grant

    @asynccontextmanager
    async def source_reader():
        """Observe existing native LOGINs without wrapping their connection type."""
        reader = await case.source.connect()
        try:
            with published_metadata_counts(reader) as counts:
                query_counts.append(counts)
                yield reader
        finally:
            await reader.close()

    case.source_pool = SimpleNamespace(acquire=source_reader)
    snapshot, previous, accounting = await _prepare(case)
    await asyncio.sleep(0)
    assert snapshot is not None and previous is None and accounting["accepted_sources"] == 2
    assert len(query_counts) == 2
    assert all(counts == {"headers": 2, "sources": 2, "endpoints": 2, "bindings": 1} for counts in query_counts)


async def _replace_source(case):
    source = case.source
    await source.admin.execute(
        f"UPDATE \"{source.schema}\".provider_directory_endpoint_dataset SET status='superseded',is_current=false"
    )
    await source.admin.execute(f'''INSERT INTO "{source.schema}".provider_directory_endpoint_dataset
      (dataset_id,endpoint_id,acquisition_root_run_id,dataset_hash,status,is_current,published_at,resource_count,
       previous_dataset_id,publication_metadata_json,publication_metadata_sha256,content_proof_admission_version,
       content_proof_admission_kind,content_proof_admission_sha256)
      SELECT 'dataset-two',endpoint_id,'second-publication',repeat('e',64),'published',true,published_at,resource_count,
       'dataset-one',publication_metadata_json,publication_metadata_sha256,content_proof_admission_version,
       content_proof_admission_kind,content_proof_admission_sha256 FROM "{source.schema}".provider_directory_endpoint_dataset''')
    await _source_facts(source, edition="dataset-two", retain_old=False)


async def test_sealed_point_in_time_publication_survives_reimport_and_disappearance(observation_case):
    case = observation_case
    first, _, _ = await _prepare(case)
    await _replace_source(case)
    await _publish(case, first)
    before = await _query(case)
    network_ids_by_name = {item["display_name"]: item["network_id"] for item in before["items"]}
    assert _manual_facts(before)
    second, previous, _ = await _prepare(case)
    assert second["metadata_generation"] != first["metadata_generation"]
    await _publish(case, second, previous)
    after = await _query(case)
    assert {item["display_name"]: item["network_id"] for item in after["items"]} == network_ids_by_name
    assert _manual_facts(after) == _manual_facts(before)
    assert all(item["archived"] is False for item in after["items"])
    await case.source.admin.execute(f'DELETE FROM "{case.source.catalog}".provider_directory_api_endpoint')
    third, prior, accounting = await _prepare(case)
    assert accounting["accepted_sources"] == 0 and accounting["unavailable_sources"] == 0
    await _publish(case, third, prior)
    disappeared = await _query(case)
    assert {item["display_name"]: item["network_id"] for item in disappeared["items"]} == network_ids_by_name
    assert _manual_facts(disappeared) == _manual_facts(before)
    assert all(not item["archived"] for item in disappeared["items"])


async def _project(case, target, source, snapshot, reader=None):
    options_by_name = {"loader_role": case.fixture.roles["loader"], "control_schema": case.fixture.control_schema}
    await project_manual_fhir_catalog(case.fixture.loader, target, snapshot, **options_by_name)
    await project_fhir_catalog_source(
        case.fixture.loader,
        target,
        source,
        snapshot,
        grant=case.grant,
        loader_role=case.fixture.roles["loader"],
        source_connection=reader,
    )


async def _unsealed(case):
    catalog_target = await _new_catalog_target(case.fixture)
    async with (
        case.source_pool.acquire() as reader,
        fhir_catalog_reference_workspace(reader) as workspace,
        reader.transaction(isolation="repeatable_read", readonly=True),
    ):
        await reader.execute("SET LOCAL search_path=pg_catalog,pg_temp,public")
        dispositions = [
            disposition
            async for page in iter_fhir_catalog_sources(
                reader, case.source.profile, observation_contract=CATALOG_OBSERVATION_CONTRACT
            )
            for disposition in page
        ]
        assert len(dispositions) == 1 and dispositions[0].status == "accepted"
        disposition = dispositions[0]
        generation = fhir_catalog_source_generation(disposition.descriptor)
        snapshot = await _snapshot(
            case.fixture,
            catalog_target,
            case.grant,
            [{"source_generation": generation, "source_id": "alias-a", "status": "current"}],
        )
        snapshot.update(network_generation=None, manifest_sha256=None)
        snapshot["metadata_generation"] = metadata_generation(snapshot)
        async with case.fixture.loader.transaction():
            await stage_fhir_catalog_source(
                reader,
                case.fixture.loader,
                catalog_target,
                disposition,
                loader_role=case.fixture.roles["loader"],
                control_schema=case.fixture.control_schema,
                reference_workspace=workspace,
            )
    return catalog_target, disposition, snapshot


async def _forge_organization(case, target):
    await case.fixture.loader.execute(
        f'UPDATE "{target.schema_name}".registry_fhir_catalog_observation '
        "SET document_json=jsonb_set(document_json,'{name}','\"Forged projected name\"'::jsonb) "
        "WHERE resource_type='Organization' AND resource_id='network-new'"
    )


async def _freeze(case, target, snapshot, *, with_verifier=True):
    async with case.fixture.publisher.transaction():
        return await freeze_fhir_catalog_snapshot(
            case.fixture.publisher,
            target,
            writer_roles=writer_role_names(
                case.fixture.roles["owner"], (case.fixture.roles["loader"],), case.config.reader_roles
            ),
            expected_snapshot=snapshot,
            control_schema=case.fixture.control_schema,
            standing_grant=case.grant,
            observation_verifier=build_fhir_catalog_observation_verifier(case.grant, case.source_pool)
            if with_verifier
            else None,
        )


@pytest.mark.parametrize("forged", [True, False])
async def test_fresh_observations_need_real_sealer_replay_even_with_consistent_projection(observation_case, forged):
    case = observation_case
    target, source, snapshot = await _unsealed(case)
    async with case.fixture.loader.transaction():
        if forged:
            await _forge_organization(case, target)
        await _project(case, target, source, snapshot)
        await create_fhir_catalog_indexes(
            case.fixture.loader,
            target,
            loader_role=case.fixture.roles["loader"],
            control_schema=case.fixture.control_schema,
        )
    if forged:
        assert (
            await case.fixture.loader.fetchval(
                f'SELECT count(*) FROM "{target.schema_name}".registry_fhir_catalog_network '
                "WHERE display_name='Forged projected name'"
            )
            == 1
        )
    with pytest.raises(ValueError, match="observation_facts_changed" if forged else "observation_verifier_required"):
        await _freeze(case, target, snapshot, with_verifier=forged)


async def test_carried_observation_mutation_is_rejected_against_protected_prior_catalog(observation_case):
    case = observation_case
    first, _, _ = await _prepare(case)
    await _publish(case, first)
    protected_page = await _query(case)
    target = await _new_catalog_target(case.fixture)
    source_entries = [{**entry, "status": "last_good"} for entry in first["source_vector_json"]]
    snapshot = await _snapshot(case.fixture, target, case.grant, source_entries)
    snapshot.update(network_generation=None, manifest_sha256=None)
    snapshot["metadata_generation"] = metadata_generation(snapshot)
    async with case.fixture.loader.transaction():
        await copy_fhir_catalog_snapshot(
            case.fixture.loader,
            first,
            target,
            snapshot["metadata_generation"],
            loader_role=case.fixture.roles["loader"],
            control_schema=case.fixture.control_schema,
        )
        await _forge_organization(case, target)
        await create_fhir_catalog_indexes(
            case.fixture.loader,
            target,
            loader_role=case.fixture.roles["loader"],
            control_schema=case.fixture.control_schema,
        )
    with pytest.raises(ValueError, match="observation_history_changed"):
        await _freeze(case, target, snapshot)
    assert await _query(case) == protected_page


async def _mutate_derived_output(case, copy_target, attack):
    namespace = '"' + copy_target.schema_name + '"'
    networks = namespace + ".registry_fhir_catalog_network"
    bindings = namespace + ".registry_fhir_catalog_binding"
    relationships = namespace + ".registry_fhir_catalog_relationship"
    network_id = await case.fixture.loader.fetchval(
        f"SELECT network_id FROM {networks} WHERE display_name='Imported network-new'"
    )
    assert network_id is not None and network_id != 7
    if attack == "duplicate_relationship":
        await _duplicate_relationship(case, copy_target, network_id)
        return
    statements_by_attack = {
        "network_name": [
            f"UPDATE {networks} SET display_name='Forged display',search_name='forged display' WHERE network_id=$1"
        ],
        "relationship": [
            f"DELETE FROM {relationships} WHERE relationship_id=(SELECT relationship_id FROM {relationships} "
            "WHERE network_id=$1 AND source_id='alias-a' ORDER BY relationship_id LIMIT 1)"
        ],
        "binding": [
            f"DELETE FROM {relationships} WHERE binding_id IN (SELECT binding_id FROM {bindings} WHERE network_id=$1)",
            f"DELETE FROM {bindings} WHERE network_id=$1",
        ],
        "network": [
            f"DELETE FROM {relationships} WHERE network_id=$1",
            f"DELETE FROM {bindings} WHERE network_id=$1",
            f"DELETE FROM {networks} WHERE network_id=$1",
        ],
    }
    for statement in statements_by_attack[attack]:
        changed = await case.fixture.loader.execute(statement, network_id)
        assert int(changed.rsplit(" ", 1)[1]) > 0


async def _duplicate_relationship(case, copy_target, network_id):
    relationship_table = '"' + copy_target.schema_name + '".registry_fhir_catalog_relationship'
    await case.fixture.loader.execute(
        f"ALTER TABLE {relationship_table} DROP CONSTRAINT registry_fhir_catalog_relationship_pkey"
    )
    changed = await case.fixture.loader.execute(
        f"INSERT INTO {relationship_table} SELECT * FROM {relationship_table} WHERE relationship_id=("
        f"SELECT relationship_id FROM {relationship_table} WHERE network_id=$1 AND source_id='alias-a' "
        "ORDER BY relationship_id LIMIT 1)",
        network_id,
    )
    assert changed == "INSERT 0 1"


@pytest.mark.parametrize("attack", ["network_name", "relationship", "binding", "network", "duplicate_relationship"])
async def test_derived_output_mutation_cannot_be_attested_with_unchanged_source_facts(observation_case, attack):
    case = observation_case
    target, source, snapshot = await _unsealed(case)
    generation = fhir_catalog_source_generation(source.descriptor)
    async with case.fixture.loader.transaction():
        await _project(case, target, source, snapshot)
        before = await fhir_catalog_observation_digest(
            case.fixture.loader, schema_name=target.schema_name, source_generation=generation
        )
        await _mutate_derived_output(case, target, attack)
        after = await fhir_catalog_observation_digest(
            case.fixture.loader, schema_name=target.schema_name, source_generation=generation
        )
        assert before == after
        await create_fhir_catalog_indexes(
            case.fixture.loader,
            target,
            loader_role=case.fixture.roles["loader"],
            control_schema=case.fixture.control_schema,
        )
    with pytest.raises(ValueError, match="registry_fhir_catalog_observed_projection_changed"):
        await _freeze(case, target, snapshot)


async def test_projection_replay_does_not_execute_loader_check_as_sealer(observation_case):
    case = observation_case
    copy_target, disposition, snapshot = await _unsealed(case)
    trap_schema = "fhir_catalog_check_" + uuid4().hex
    case.fixture.schemas.append(trap_schema)
    trap_function = '"' + trap_schema + '".accept_loader'
    async with case.fixture.loader.transaction():
        await _project(case, copy_target, disposition, snapshot)
        await case.fixture.loader.execute(f'CREATE SCHEMA "{trap_schema}"')
        await case.fixture.loader.execute(f"""CREATE FUNCTION {trap_function}(network_id integer)
          RETURNS boolean LANGUAGE plpgsql AS $check_trap$
          BEGIN
            IF current_user::text='{case.fixture.roles["publisher"]}' THEN
              RAISE EXCEPTION 'loader_check_executed_as_sealer';
            END IF;
            RETURN true;
          END $check_trap$""")
        assert await case.fixture.loader.fetchval(f"SELECT {trap_function}(1)") is True
        await case.fixture.loader.execute(
            f'ALTER TABLE "{copy_target.schema_name}".registry_fhir_catalog_network '
            f"ADD CONSTRAINT loader_external_check CHECK ({trap_function}(network_id))"
        )
        await create_fhir_catalog_indexes(
            case.fixture.loader,
            copy_target,
            loader_role=case.fixture.roles["loader"],
            control_schema=case.fixture.control_schema,
        )
    with pytest.raises(asyncpg.RaiseError, match="loader_check_executed_as_sealer"):
        async with case.fixture.publisher.transaction():
            await case.fixture.publisher.fetchval(f"SELECT {trap_function}(1)")
    sealed = await _freeze(case, copy_target, snapshot)
    assert sealed["component"] == "registry_fhir_catalog_writer_closure"
    assert sealed["validation"]["tables"]["registry_fhir_catalog_network"]["rows"] == 2
