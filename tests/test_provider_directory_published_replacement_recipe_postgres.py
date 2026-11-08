# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""An ordinary published replacement runs the complete native Address recipe."""

import asyncio
import importlib
import json
from contextlib import asynccontextmanager

import pytest
from sqlalchemy import MetaData, text
from sqlalchemy.ext.asyncio import create_async_engine

from process import entity_address_result_generation as generations
from process import provider_directory_published_replacement_address_followup as address_followup
from process import provider_directory_serving_recovery as recovery
from tests.cms_npd_admission_postgres_support import _resource_rows
from tests.cms_registry_complete_publication_postgres_support import office_resources
from tests.provider_directory_dataset_artifact_pg_support import seal_validated_dataset
from tests.provider_directory_entities_postgres_support import _database_url
from tests.provider_directory_serving_recovery_postgres_support import ordinary_serving_case
from tests.ptg2_serving_address_evidence_postgres_support import _create_geo_reference_tables
from tests.test_address_format_db import FOUNDATION_MIGRATION_PATH, _load_migration
from tests.test_address_numeric_grid_alias_db import _INSERT_ARCHIVE_ADDRESS_SQL
from tests.test_entity_address_snapshot_destination import _seed_support_rows
from tests.test_provider_directory_published_replacement_postgres import _alias, _markers, _prepare
from tests.test_provider_directory_serving_recovery_postgres import _state

native = importlib.import_module("process.entity_address_unified")
models = importlib.import_module("db.models")


async def _create_recipe_models(case):
    """Replace fixture markers with every genuine serving and source model."""
    database, schema = case.database, case.schema
    async with database.engine.begin() as connection:
        metadata = MetaData(schema=schema)
        for model in (native.EntityAddressUnified, *native.SUPPORT_TABLE_MODELS):
            await connection.execute(text(f'DROP TABLE "{schema}"."{model.__tablename__}"'))
        for model in (
            native.EntityAddressUnified,
            *native.SUPPORT_TABLE_MODELS,
            models.NPIAddress,
            models.MRFAddress,
            models.DoctorClinicianAddress,
            models.AddressAliasV1,
            models.AddressArchiveV2,
            models.ImportRun,
        ):
            model.__table__.to_metadata(metadata, schema=schema)
        await connection.run_sync(metadata.create_all)
        foundation = _load_migration(FOUNDATION_MIGRATION_PATH, "replacement_recipe_foundation")
        await connection.run_sync(
            lambda sync: foundation._exec_sql_batch(sync, foundation._create_functions_sql(schema))
        )
        await _seed_support_rows(connection, schema, location_key="unrelated", entity_id="unrelated")
    formatter = _load_migration()
    await database.status(formatter._humanize_component_function_sql(schema))
    await database.status(formatter._formatted_address_function_sql(schema))
    await database.status(f'DROP TABLE "{schema}".provider_directory_address_overlay')
    await database.status(case.directory.provider_directory_address_overlay_table_sql(schema))
    await case.directory._create_provider_directory_address_overlay_indexes(
        schema, "provider_directory_address_overlay"
    )


async def _seed_spatial_inputs(case):
    """Use real PostGIS and small reference heaps in the owned native database."""
    database, schema = case.database, case.schema
    await _create_geo_reference_tables(database, schema)
    for table in ("zip_state", "zcta5"):
        await database.status(f'ALTER TABLE "{schema}"."{table}" SET SCHEMA tiger')
    await database.status(f"INSERT INTO {schema}.geo_zip_lookup VALUES ('90210','CA','CALIFORNIA')")
    await database.status("INSERT INTO tiger.zip_state VALUES ('90210','CA')")
    await database.status(
        "INSERT INTO tiger.zcta5(zcta5ce,the_geom) VALUES ('90210',ST_GeomFromText("
        "'POLYGON((-118.5 34.0,-118.3 34.0,-118.3 34.2,-118.5 34.2,-118.5 34.0))',4269))"
    )


async def _insert_resource(case, dataset, resource_type, resource, *, source_id, run_id):
    """Retain normalized source payloads and their actual semantic resource digest."""
    directory = case.directory
    model, observed = directory.parse_fhir_resource(
        source_id, dict(resource, resourceType=resource_type), run_id=run_id
    )
    payload = directory._resource_payload_for_contract(model, observed, directory.DEFAULT_RESOURCE_HASH_CONTRACT)
    payload_hash = directory.resource_payload_sha256_for_contract(
        payload, directory.DEFAULT_RESOURCE_HASH_CONTRACT, resource_type=resource_type
    )
    await case.database.status(
        f"INSERT INTO {case.schema}.provider_directory_dataset_resource "
        "(dataset_id,resource_type,resource_id,payload_hash,payload_json) "
        "VALUES (:dataset,:kind,:identity,:digest,CAST(:payload AS json))",
        dataset=dataset,
        kind=resource_type,
        identity=resource["id"],
        digest=payload_hash,
        payload=json.dumps(payload, default=lambda value: value.isoformat()),
    )


async def _seed_published_resources(case):
    """Publish two real office units and a separate same-NPI serving peer."""
    database, schema = case.database, case.schema
    await _prepare(database, schema)
    await database.status(f"DELETE FROM {schema}.provider_directory_dataset_resource")
    resources = office_resources(_resource_rows, "replacement")
    for kind, source_rows in resources.items():
        for resource in source_rows:
            await _insert_resource(
                case, "dataset_shared", kind, resource, source_id="source_primary", run_id="root-shared"
            )
    resource_types = sorted(resources)
    metadata = dict(
        source_ids=["source_primary", "source_sibling"],
        selected_resources=resource_types,
        expected_resources=resource_types,
        requires_twin_root_verification=False,
    )
    await database.status(
        f"UPDATE {schema}.provider_directory_endpoint_dataset SET publication_metadata_json=CAST(:metadata AS jsonb),"
        "resource_count=:count WHERE dataset_id='dataset_shared'",
        metadata=json.dumps(metadata),
        count=sum(map(len, resources.values())),
    )
    await seal_validated_dataset(database, schema, "dataset_shared", resource_types=tuple(resource_types))
    await database.status(
        f"ALTER TABLE {schema}.provider_directory_endpoint_dataset ALTER COLUMN publication_metadata_json "
        "TYPE json USING publication_metadata_json::json"
    )
    await database.status(
        f"UPDATE {schema}.provider_directory_source SET metadata_json=(metadata_json::jsonb || CAST(:metadata AS jsonb))::json "
        "WHERE source_id IN ('source_primary','source_sibling')",
        metadata=json.dumps(
            dict(
                provider_directory_supported_resources=resource_types,
                provider_directory_fully_enumerable_resources=resource_types,
            )
        ),
    )
    await _seed_peer(case, resources)


async def _seed_peer(case, resources):
    """Retain genuine resource membership for a distinct same-NPI serving peer."""
    database, schema = case.database, case.schema
    await database.status(
        f"UPDATE {schema}.provider_directory_endpoint_dataset SET status='published',dataset_hash=repeat('c',64),"
        "acquisition_root_run_id='peer_location_by_field-root',validated_at=timestamp '2026-01-01',published_at=timestamp '2026-01-01',"
        "publication_metadata_json='{\"source_ids\":[\"former_peer_alias\"]}' WHERE dataset_id='dataset_unpublished'"
    )
    peer_location_by_field = dict(resources["Location"][0], id="peer_location_by_field-site")
    peer_location_by_field["address"] = dict(peer_location_by_field["address"], line=["123 Example Street", "Suite 4"])
    await _insert_resource(
        case,
        "dataset_unpublished",
        "Location",
        peer_location_by_field,
        source_id="source_no_current",
        run_id="peer_location_by_field-root",
    )
    peer_role_by_field = dict(
        resources["PractitionerRole"][0],
        id="peer_location_by_field-office",
        location=[{"reference": "Location/peer_location_by_field-site"}],
    )
    for kind, resource in (
        ("PractitionerRole", peer_role_by_field),
        ("Practitioner", resources["Practitioner"][0]),
        ("Organization", resources["Organization"][0]),
    ):
        await _insert_resource(
            case,
            "dataset_unpublished",
            kind,
            resource,
            source_id="source_no_current",
            run_id="peer_location_by_field-root",
        )
    await database.status(
        f"UPDATE {schema}.provider_directory_endpoint_dataset SET resource_count=4 WHERE dataset_id='dataset_unpublished'"
    )


async def _seed_incumbent(case):
    """Keep the original same-NPI offices and an unrelated complete serving row."""
    database, schema = case.database, case.schema
    for source_id, run, identity, unit in (
        ("source_primary", "root-serving", "old-office", "1"),
        ("source_no_current", "peer_location_by_field-root", "peer_location_by_field-office", "4"),
    ):
        await database.status(
            f"INSERT INTO {schema}.provider_directory_address_overlay "
            "(source_record_id,source_id,last_seen_run_id,resource_type,resource_id,npi,address_key,"
            "first_line,second_line,city_name,state_name,state_code,postal_code,country_code,lat,long) "
            f"VALUES (:record,:source,:run,'PractitionerRole',:identity,1234567893,{schema}.addr_key_v1("
            "'123 Example Street',CAST(:unit AS text),'Sample City','CA','90210','US'),"
            "'123 Example Street',CAST(:unit AS text),'Sample City','CA','CA','90210','US',34.1,-118.4)",
            record=f"provider_directory_fhir:practitioner_role:{source_id}:{identity}",
            source=source_id,
            run=run,
            identity=identity,
            unit="Suite " + unit,
        )
    for identity, npi, unit in (
        ("old-office", 1234567893, "1"),
        ("peer_location_by_field-office", 1234567893, "4"),
        ("unrelated", 1000000004, "9"),
    ):
        await database.first(
            _INSERT_ARCHIVE_ADDRESS_SQL.format(schema=schema),
            first_line="123 Example Street",
            second_line="Suite " + unit,
            city="Sample City",
            state="CA",
            postal_code="90210",
            strict_source_bits=1,
        )
        await database.status(
            f"INSERT INTO {schema}.entity_address_unified "
            "(location_key,entity_type,entity_id,npi,checksum,type,first_line,second_line,city_name,state_name,"
            "state_code,postal_code,zip5,country_code,lat,long,base_address_version,archive_identity_version,premise_key,address_key) "
            "VALUES (:identity,'npi',:entity_id,:npi,:checksum,'practice','123 Example Street',CAST(:unit AS text),"
            f"'Sample City','CALIFORNIA','CA','90210','90210','US',34.1,-118.4,:version,'v2',{schema}.addr_premise_key_v1("
            "'123 Example Street',CAST(:unit AS text),'Sample City','CA','90210','US'),"
            f"{schema}.addr_key_v1("
            "'123 Example Street',CAST(:unit AS text),'Sample City','CA','90210','US'))",
            identity=identity,
            entity_id=str(npi),
            npi=npi,
            checksum=int(unit),
            unit="Suite " + unit,
            version=native.ALIAS_BASE_ADDRESS_VERSION_PREFIX + "0",
        )
    await database.status(f"UPDATE {schema}.address_archive_v2 SET lat=34.1,long=-118.4")
    async with database.transaction():
        await native._materialize_geo_assurance(
            schema, "entity_address_unified", force=True, context={}, run_id="", stage_rows=3
        )
        assert await database.scalar(native._activate_geo_assurance_candidate_sql(schema))
        await generations.publish_local_entity_address_generation(database, schema_name=schema)


@asynccontextmanager
async def _recipe_case(monkeypatch, *, archive_complete):
    """Own all native prerequisites and bind real database objects, never recipe mocks."""
    administrator = create_async_engine(_database_url())
    try:
        async with administrator.begin() as connection:
            for extension in ("postgis", "pg_trgm", "intarray", "btree_gin"):
                await connection.execute(text(f"CREATE EXTENSION IF NOT EXISTS {extension}"))
            assert await connection.scalar(text("SELECT to_regnamespace('tiger')")) is None
        async with ordinary_serving_case(monkeypatch) as case:
            async with administrator.begin() as connection:
                await connection.execute(text(f'CREATE SCHEMA tiger AUTHORIZATION "{case.role}"'))
            try:
                for module in (
                    native,
                    models,
                    importlib.import_module("db.connection"),
                    importlib.import_module("process.ext.utils"),
                ):
                    monkeypatch.setattr(module, "db", case.database)
                url = case.database.engine.url
                monkeypatch.setenv("HLTHPRT_DB_DATABASE", url.database)
                monkeypatch.setenv("HLTHPRT_DB_USER", url.username)
                monkeypatch.setenv("HLTHPRT_DB_PASSWORD", url.password)
                monkeypatch.setattr(case.database, "_database_name", url.database)
                for model in (native.EntityAddressUnified, *native.SUPPORT_TABLE_MODELS):
                    monkeypatch.setattr(model.__table__, "schema", case.schema)
                monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_MIN_ROWS", "3")
                await _create_recipe_models(case)
                await _seed_spatial_inputs(case)
                await _seed_published_resources(case)
                await _seed_incumbent(case)
                if archive_complete:
                    await _seed_replacement_archive(case)
                yield case
            finally:
                async with administrator.begin() as connection:
                    await connection.execute(text("DROP SCHEMA tiger CASCADE"))
                    assert await connection.scalar(text("SELECT to_regnamespace('tiger')")) is None
    finally:
        await administrator.dispose()


async def _seed_replacement_archive(case):
    """Supply canonical archive membership as a real, separate preparation prerequisite."""
    for unit in ("2", "3"):
        await case.database.first(
            _INSERT_ARCHIVE_ADDRESS_SQL.format(schema=case.schema),
            first_line="123 Example Street",
            second_line="Suite " + unit,
            city="Sample City",
            state="CA",
            postal_code="90210",
            strict_source_bits=1,
        )


async def _rows(case, table):
    """Compare complete row values in a stable native JSON representation."""
    return await case.database.scalar(
        f"SELECT COALESCE(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),'[]'::jsonb) FROM {case.schema}.{table} t"
    )


async def _support(case):
    """Capture content and heap identity for all six unchanged supporting models."""
    support_by_table = {}
    for model in native.SUPPORT_TABLE_MODELS:
        name = model.__tablename__
        support_by_table[name] = (
            await case.database.scalar("SELECT to_regclass(:name)::oid::bigint", name=f"{case.schema}.{name}"),
            await _rows(case, name),
        )
    return support_by_table


async def _publish(case, markers):
    """Invoke the real ordinary followup entry point, including its startup and cutover."""
    return await case.directory._publish_provider_directory_dataset_artifacts(
        run_id=None,
        metrics=markers,
        source_ids=["source_primary"],
        publish_corroboration=False,
        publish_artifacts_targets={"address_overlay"},
    )


async def _assert_addresses(case, unrelated, *, archive_complete):
    """Preserve the full same-NPI peer, exact unrelated row and canonical input semantics."""
    serving_rows = await _rows(case, "entity_address_unified")
    offices = [address_row for address_row in serving_rows if address_row["npi"] == 1234567893]
    assert len(serving_rows) == 4 and len(offices) == 3
    assert {address_row["second_line"] for address_row in offices} == {"Suite 2", "Suite 3", "Suite 4"}
    assert next(address_row for address_row in serving_rows if address_row["npi"] == 1000000004) == unrelated
    assert all(address_row["formatted_address"] for address_row in offices)
    if archive_complete:
        assert all(address_row["address_key"] for address_row in offices)
    else:
        assert {address_row["second_line"] for address_row in offices if address_row["address_key"] is None} == {
            "Suite 2",
            "Suite 3",
        }
    assert all(address_row["geo_assurance_version"] is not None for address_row in serving_rows)


async def _assert_recovered(case, before, published):
    """Recovery restores exact heaps while recording its own new local publication generation."""
    restored = await _state(case)
    assert restored["roots"] == before["roots"]
    for field in ("alias", "profile", "doctors", "geo", "relations", "common_receipt"):
        assert restored["metadata"][field] == before["metadata"][field]
    old_address, recovered_address = before["metadata"]["address"], restored["metadata"]["address"]
    assert recovered_address["local_generation"] == published["metadata"]["address"]["local_generation"] + 1
    assert recovered_address["origin_generation"] == recovered_address["local_generation"]
    for field in ("local_lineage_id", "origin_lineage_id", "relation_oids", "singleton"):
        assert recovered_address[field] == old_address[field]
    for field in ("generation", "artifact_name"):
        assert restored["metadata"]["overlay_alias"][field] == before["metadata"]["overlay_alias"][field]


@pytest.mark.asyncio
@pytest.mark.parametrize("archive_complete", [True, False], ids=["complete-archive", "missing-new-archive"])
async def test_native_published_replacement_runs_real_partial_recipe_and_recovers(monkeypatch, archive_complete):
    """Actual build, indexes, validation and cut preserve peer and unrelated serving inputs."""
    async with _recipe_case(monkeypatch, archive_complete=archive_complete) as case:
        before = await _state(case)
        before_rows = await _rows(case, "entity_address_unified")
        support = await _support(case)
        unrelated = next(address_row for address_row in before_rows if address_row["npi"] == 1000000004)
        markers = _markers()
        publication_metrics = await _publish(case, markers)
        assert publication_metrics["serving_recovery"]["phase"] == "published"
        assert await _alias(case.database, case.schema) == "endpoint_shared"
        await _assert_addresses(case, unrelated, archive_complete=archive_complete)
        after = await _state(case)
        assert after["metadata"]["address"]["local_generation"] == before["metadata"]["address"]["local_generation"] + 1
        assert await case.database.scalar("SELECT session_user::text") == case.role
        assert (
            after["roots"]["entity_address_unified"]["native"]["oid"]
            != before["roots"]["entity_address_unified"]["native"]["oid"]
        )
        assert await _support(case) == support
        assert (
            await case.database.scalar(
                "SELECT count(*) FROM pg_index WHERE indrelid=to_regclass(:name) AND indisvalid",
                name=f"{case.schema}.entity_address_unified",
            )
            > 20
        )
        replay = await _publish(case, _markers())
        assert replay["serving_recovery"] == publication_metrics["serving_recovery"]
        assert await _state(case) == after
        await recovery._recover(
            case.directory,
            {
                recovery.OPERATION_PARAM: "recover",
                recovery.GENERATION_PARAM: publication_metrics["serving_recovery"]["generation"],
                "source_ids": ["source_primary"],
            },
        )
        await _assert_recovered(case, before, after)
        assert await _rows(case, "entity_address_unified") == before_rows
        assert await _support(case) == support
        assert await _alias(case.database, case.schema) == "endpoint_repoint"


async def _heap_inventory(case):
    """Observe every table and sequence in this fixture's exact owned schema."""
    relations = await case.database.all(
        "SELECT relname,oid::bigint FROM pg_class WHERE relnamespace=to_regnamespace(:schema) "
        "AND relkind IN ('r','p','S') ORDER BY relname",
        schema=case.schema,
    )
    return {relation[0]: relation[1] for relation in relations}


async def _replace_owned_stage(case, prepared):
    """Replace only the exact created heap and register a distinct competing OID."""
    stage_name = prepared.swaps[0].stage_cls.__tablename__
    original_oid = dict((name, oid) for _target, name, oid in prepared.stage_oids)[stage_name]
    assert (
        await case.database.scalar("SELECT to_regclass(:relation)::oid::bigint", relation=f"{case.schema}.{stage_name}")
        == original_oid
    )
    await case.database.status(f'DROP TABLE "{case.schema}"."{stage_name}" RESTRICT')
    await case.database.status(f'CREATE TABLE "{case.schema}"."{stage_name}" (marker text PRIMARY KEY)')
    await case.database.status(f'INSERT INTO "{case.schema}"."{stage_name}" VALUES (\'foreign\')')
    replacement_oid = await case.database.scalar(
        "SELECT to_regclass(:relation)::oid::bigint", relation=f"{case.schema}.{stage_name}"
    )
    assert replacement_oid != original_oid
    return stage_name, replacement_oid


def _failing_finalizer(case, failure_kind, observed_by_field, ready):
    """Let the real finalizer finish before withholding its Prepared result from the caller."""
    from process import entity_address_ordinary_preparation as ordinary

    original = native.publish_entity_address_unified_generation

    async def finalize(ctx, **options):
        prepared = await original(ctx, **options)
        owned = ordinary.current()
        assert owned is not None and owned.owner is asyncio.current_task()
        observed_by_field["cleanup_oids"] = tuple(owned.cleanup_oids)
        assert {(name, oid) for _target, name, oid in prepared.stage_oids} <= {
            (name, oid) for _target, name, oid in observed_by_field["cleanup_oids"]
        }
        assert observed_by_field["cleanup_oids"]
        if failure_kind == "replaced-name":
            observed_by_field["foreign"] = await _replace_owned_stage(case, prepared)
        ready.set()
        if failure_kind == "cancellation":
            await asyncio.Event().wait()
        raise RuntimeError("recipe-fixture-finalizer")

    return finalize


async def _run_failed_recipe(case, failure_kind, ready):
    """Cancel externally only after native finalization, and always drain the actual task."""
    publication = asyncio.create_task(_publish(case, _markers()))
    waiting = None
    try:
        if failure_kind == "cancellation":
            waiting = asyncio.create_task(ready.wait())
            completed, _pending = await asyncio.wait(
                {publication, waiting}, timeout=60, return_when=asyncio.FIRST_COMPLETED
            )
            if waiting not in completed:
                if publication.done():
                    await publication
                pytest.fail("native finalizer did not reach its cancellation barrier")
            publication.cancel()
            with pytest.raises(asyncio.CancelledError):
                await publication
        else:
            with pytest.raises(RuntimeError, match="^recipe-fixture-finalizer$"):
                await publication
    finally:
        for task in (publication, waiting):
            if task is not None and not task.done():
                task.cancel()
        await asyncio.gather(*(task for task in (publication, waiting) if task is not None), return_exceptions=True)


async def _assert_preparation_drained(case, baseline_inventory, observed_by_field):
    """Prove every registered OID is absent and any competing same-name heap survives."""
    for _target, _name, oid in observed_by_field["cleanup_oids"]:
        assert not await case.database.scalar("SELECT EXISTS(SELECT FROM pg_class WHERE oid=:oid)", oid=oid)
    expected_by_relation = dict(baseline_inventory)
    if "foreign" in observed_by_field:
        name, oid = observed_by_field["foreign"]
        expected_by_relation[name] = oid
        assert await case.database.scalar(f'SELECT marker FROM "{case.schema}"."{name}"') == "foreign"
    assert await _heap_inventory(case) == expected_by_relation


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_kind", ["exception", "cancellation", "replaced-name"])
async def test_native_real_recipe_preparation_failure_drains_only_owned_heaps(monkeypatch, failure_kind):
    """Failed full builds cannot leak stages or remove a same-name competing relation."""
    async with _recipe_case(monkeypatch, archive_complete=True) as case:
        before = await _state(case)
        support = await _support(case)
        inventory = await _heap_inventory(case)
        observed_by_field = {}
        ready = asyncio.Event()
        monkeypatch.setattr(
            native,
            "publish_entity_address_unified_generation",
            _failing_finalizer(case, failure_kind, observed_by_field, ready),
        )
        await _run_failed_recipe(case, failure_kind, ready)
        assert await _state(case) == before
        assert await _support(case) == support
        assert await _alias(case.database, case.schema) == "endpoint_repoint"
        await _assert_preparation_drained(case, inventory, observed_by_field)


def _address_child_task(generation_present):
    """Use the existing separate V1 child shape with its ordinary optional generation."""
    prefix = "provider_directory_dataset_followup_"
    task_by_field = {
        **_markers(),
        prefix + "stage": "entity_address_unified",
        prefix + "overlay_run_id": "overlay-child",
        "refresh_mode": "provider-directory-partial",
        "provider_directory_source_ids": ["source_primary"],
        "provider_directory_run_id": "root-shared",
        "provider_directory_dataset_id": "dataset_shared",
        "provider_directory_partial_scope": "latest-run",
        "serving_only_refresh": True,
        "publish": True,
        "test_mode": False,
        "run_id": "address-child",
    }
    if generation_present:
        task_by_field[recovery.GENERATION_PARAM] = recovery.publication_generation(
            {name: task_by_field[prefix + name] for name in recovery._PIN_FIELDS}
        )
    return task_by_field


async def _address_replay_snapshot(case):
    """Capture every serving, journal, Source and physical table identity before a read-only replay."""
    return {
        "serving": await _state(case),
        "heaps": await _heap_inventory(case),
        "journal": await _rows(case, recovery.receipts.JOURNAL),
        "sources": await _rows(case, "provider_directory_source"),
        "datasets": await _rows(case, "provider_directory_endpoint_dataset"),
    }


def _assert_overlay_child_result(metrics):
    """Retain the existing scheduler-consumed overlay completion fields on initial and replay paths."""
    assert metrics["address_overlay"]["published"] is True
    assert metrics["address_overlay"]["rows"] == 3
    assert metrics["address_overlay"]["source_ids"] == ["source_primary"]
    assert metrics["artifact_dataset_ids"] == ["dataset_shared"]
    assert metrics["artifact_dataset_evidence_run_ids"] == ["root-shared"]


@pytest.mark.asyncio
@pytest.mark.parametrize("generation_present", [False, True], ids=["legacy-marker", "ordinary-generation"])
async def test_native_separate_address_child_replays_proven_replacement_without_build(monkeypatch, generation_present):
    """The unchanged child consumes actual coupled publication without another generation or write."""
    async with _recipe_case(monkeypatch, archive_complete=True) as case:
        task_by_field = _address_child_task(generation_present)
        before = await _address_replay_snapshot(case)
        absent_context_by_field = {"context": {"run": 17}}
        assert await address_followup.resume_address(case.database, absent_context_by_field, task_by_field) is None
        assert absent_context_by_field == {"context": {"run": 17}}
        assert await _address_replay_snapshot(case) == before
        overlay_markers = _markers()
        if generation_present:
            overlay_markers[recovery.GENERATION_PARAM] = task_by_field[recovery.GENERATION_PARAM]
        publication_metrics = await _publish(case, overlay_markers)
        _assert_overlay_child_result(publication_metrics)
        after = await _address_replay_snapshot(case)
        replay_metrics = await _publish(case, overlay_markers)
        _assert_overlay_child_result(replay_metrics)
        assert await _address_replay_snapshot(case) == after
        # No import_date is supplied: an accidental fall-through cannot silently build another stage.
        context_by_field = {"context": {"run": 17}}
        result_metrics = await native.process_entity_address_unified_data(context_by_field, task_by_field)
        assert context_by_field["context"]["run"] == 0 and "stage_prepared" not in context_by_field["context"]
        assert result_metrics == {
            "refresh_mode": "provider-directory-partial",
            "partial_provider_directory_scope": "latest-run",
            "partial_provider_directory_run_id": "root-shared",
            "partial_provider_directory_dataset_id": "dataset_shared",
            "partial_provider_directory_source_count": 1,
            "serving_only_refresh": True,
            "rows": 4,
            "serving_recovery": publication_metrics["serving_recovery"],
        }
        assert result_metrics["serving_recovery"]["generation"] != recovery.publication_generation(
            {name: task_by_field["provider_directory_dataset_followup_" + name] for name in recovery._PIN_FIELDS}
        )
        assert await _address_replay_snapshot(case) == after


def _invalid_address_children(task, internal_generation):
    """Reject changed identity, wire enums, exact booleans and the internal journal UUID on the wire."""
    prefix = "provider_directory_dataset_followup_"
    changes = (
        {prefix + "dataset_hash": "d" * 64},
        {prefix + "endpoint_id": "endpoint_repoint"},
        {prefix + "parent_run_id": "wrong-root", "provider_directory_run_id": "wrong-root"},
        {prefix + "source_id": "wrong-source", "provider_directory_source_ids": ["wrong-source"]},
        {recovery.GENERATION_PARAM: internal_generation},
        {prefix + "stage": "unified_address"},
        {prefix + "version": True},
        {prefix + "attempt": True},
        {prefix + "overlay_run_id": " "},
        {"provider_directory_dataset_id": "wrong-dataset"},
        {"serving_only_refresh": 1},
        {"publish": False},
        {"test_mode": 0},
        {"run_id": None},
        {"unrecognized": True},
    )
    return [{**task, **changed} for changed in changes]


@pytest.mark.asyncio
async def test_native_address_child_refuses_changed_pin_flags_factory_and_output_proof(monkeypatch):
    """A marker cannot substitute for exact current headers, published output or ordinary custody."""
    from process import provider_directory_generic_source_policy as source

    async with _recipe_case(monkeypatch, archive_complete=True) as case:
        publication = await _publish(case, _markers())
        before = await _address_replay_snapshot(case)
        task_by_field = _address_child_task(False)
        for changed_task in _invalid_address_children(task_by_field, publication["serving_recovery"]["generation"]):
            with pytest.raises(ValueError, match="provider_directory_serving_recovery_"):
                await native.process_entity_address_unified_data({"context": {}}, changed_task)
            assert await _address_replay_snapshot(case) == before
        token = source._FACTORY.set(object())
        try:
            assert await address_followup.resume_address(case.database, {"context": {}}, task_by_field) is None
        finally:
            source._FACTORY.reset(token)
        assert await _address_replay_snapshot(case) == before
        await case.database.status(
            f"UPDATE {case.schema}.entity_address_unified SET first_line='CHANGED FIXTURE' "
            "WHERE npi=1234567893 AND second_line='Suite 2'"
        )
        changed_output = await _address_replay_snapshot(case)
        with pytest.raises(ValueError, match="current_serving_changed"):
            await native.process_entity_address_unified_data({"context": {}}, task_by_field)
        assert await _address_replay_snapshot(case) == changed_output


@pytest.mark.asyncio
async def test_native_optional_replacement_replay_preserves_missing_journal_fallback(monkeypatch):
    """Legacy ordinary requests do not acquire a new migration prerequisite."""
    from process import provider_directory_generic_source_artifacts as artifacts

    async with _recipe_case(monkeypatch, archive_complete=True) as case:
        await case.database.status(f'DROP TABLE "{case.schema}"."{recovery.receipts.JOURNAL}"')
        before = await _state(case)
        assert await artifacts.replay_published_replacement(case.directory, _markers()) is None
        assert await address_followup.resume_address(case.database, {"context": {}}, _address_child_task(False)) is None
        assert await _state(case) == before
