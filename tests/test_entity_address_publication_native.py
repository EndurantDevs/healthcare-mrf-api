# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Opt-in native publication proof with exact, disposable database and role custody."""

import asyncio
import json
from contextlib import asynccontextmanager
from uuid import uuid4

import asyncpg
import pytest
from sqlalchemy import MetaData, text

from db import models
from db.connection import db
from process import entity_address_native_publication as publication
from process import entity_address_result_generation as generation
from process import entity_address_snapshot_adoption as adoption
from process import entity_address_snapshot_alias as alias
from process import entity_address_snapshot_ownership as ownership
from process import entity_address_snapshot_preparation as preparation
from process import entity_address_snapshot_restore as restore
from process import entity_address_snapshot_serving as serving
from process import entity_address_snapshot_source as source
from process import reference_family_archive as family_archive
from tests.test_address_numeric_grid_alias_db import _insert_archive_address
from tests.test_address_numeric_grid_alias_lifecycle_db import _apply_reviewed_alias
from tests.test_entity_address_alias_guard_postgres import _admin_url, _owned_guard_database, _provision_test_schema
from tests.test_entity_address_snapshot_destination import _create_geo_dependencies, _seed_support_rows

SCHEMA = "mrf"


@asynccontextmanager
async def _publication_database(monkeypatch):
    """Reuse cleanup registered before creation and the actual protected alias authority."""
    census = await _cluster_census()
    try:
        async with _owned_guard_database(monkeypatch) as resources:
            await _provision_publication_database(monkeypatch, resources)
            yield resources
    finally:
        assert await _cluster_census() == census


async def _cluster_census():
    connection = await asyncpg.connect(_admin_url().render_as_string(hide_password=False), timeout=10)
    try:
        return tuple(
            [
                tuple(tuple(row) for row in await connection.fetch(query))
                for query in (
                    "SELECT oid,datname,datdba FROM pg_database ORDER BY oid",
                    "SELECT oid,rolname,rolsuper,rolinherit,rolcreaterole,rolcreatedb,rolcanlogin,rolreplication,rolbypassrls "
                    "FROM pg_roles ORDER BY oid",
                    "SELECT roleid,member,grantor,admin_option,inherit_option,set_option FROM pg_auth_members "
                    "ORDER BY roleid,member,grantor",
                )
            ]
        )
    finally:
        await connection.close(timeout=10)


async def _provision_publication_database(monkeypatch, resources):
    """Provision only resources inside the exact database created by the lifecycle."""
    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_PROTECTED_PUBLICATION", "true")
    await _provision_test_schema(resources, SCHEMA, initial_generation=0)
    await resources.admin.execute("DROP TABLE mrf.entity_address_unified")
    await resources.admin.execute("CREATE EXTENSION intarray")
    await resources.admin.execute("CREATE EXTENSION btree_gin")
    await resources.admin.execute(f'CREATE SCHEMA tiger AUTHORIZATION "{resources.roles["writer"]}"')
    await resources.admin.execute(f'GRANT "{resources.roles["writer"]}" TO "{resources.roles["publisher"]}"')
    database_name = await resources.admin.fetchval("SELECT current_database()")
    await resources.admin.execute(
        f'GRANT CREATE ON DATABASE "{database_name}" TO "{resources.roles["owner"]}","{resources.roles["writer"]}"'
    )
    await _create_canonical_models()
    await _create_geo_dependencies(db, SCHEMA)
    await _seed_canonical_payload()
    await _seed_active_alias()
    await _protect_control_state(resources)


async def _create_canonical_models():
    metadata = MetaData(schema=SCHEMA)
    for model in (*generation.ENTITY_ADDRESS_RESULT_MODELS, models.EntityAddressResultGeneration, models.ImportRun):
        model.__table__.to_metadata(metadata, schema=SCHEMA)
    async with db.engine.begin() as connection:
        await connection.run_sync(metadata.create_all)
        await connection.execute(
            text(
                "INSERT INTO mrf.entity_address_result_generation "
                "(singleton,local_lineage_id,local_generation) VALUES (true,CAST(:lineage AS uuid),0)"
            ),
            {"lineage": str(uuid4())},
        )


async def _protect_control_state(resources):
    for table in (generation.TABLE_NAME, "entity_address_geo_assurance_state"):
        await resources.admin.execute(f'ALTER TABLE mrf."{table}" OWNER TO "{resources.roles["owner"]}"')
        await resources.admin.execute(f'GRANT SELECT ON mrf."{table}" TO "{resources.roles["writer"]}"')
    await resources.admin.execute(f'GRANT SELECT,UPDATE ON mrf.import_run TO "{resources.roles["publisher"]}"')


def _attempt_context():
    run_id = "native-address-" + uuid4().hex
    context_by_field = {
        "control_run_id": run_id,
        "context": {
            "_control_attempt_id": run_id + ":" + uuid4().hex,
            "_control_attempt_started_at": "2026-01-02T03:04:05Z",
            "address_alias_generation": 1,
            "publish_requested": True,
        },
    }
    publication.bind_controlled_address_attempt(context_by_field)
    return context_by_field


async def _create_attempt_stage(context, *, invalid_payload=False):
    native = publication._native()
    metadata = MetaData(schema=SCHEMA)
    for model in generation.ENTITY_ADDRESS_RESULT_MODELS:
        native.make_class(model, context["import_date"]).__table__.to_metadata(metadata, schema=SCHEMA)
    async with db.engine.begin() as connection:
        await connection.run_sync(metadata.create_all)
        await connection.execute(
            text(
                "INSERT INTO mrf.import_run (run_id,engine,importer,node_id,status,params,progress,metrics) "
                "VALUES (:run_id,'synthetic','entity-address-unified','synthetic-node','running',"
                "'{}',CAST(:progress AS json),'{}')"
            ),
            {
                "run_id": context["control_run_id"],
                "progress": json.dumps(
                    {
                        "attempt_id": context["context"]["_control_attempt_id"],
                        "attempt_started_at": context["context"]["_control_attempt_started_at"],
                    }
                ),
            },
        )
    await _seed_stage_payload(context["import_date"], invalid_payload=invalid_payload)


async def _seed_canonical_payload():
    address_key = await _insert_archive_address(
        SCHEMA, first_line="10 Example Street", second_line=None, strict_source_bits=1
    )
    async with db.transaction() as session:
        await _insert_main_row(session, "entity_address_unified", address_key)
        await _seed_support_rows(session, SCHEMA, location_key="synthetic-location", entity_id="synthetic-provider")
    await db.status(
        "INSERT INTO mrf.npi_address (npi,address_key,type,checksum) VALUES (1000000001,CAST(:key AS uuid),'practice',1)",
        key=address_key,
    )


async def _seed_active_alias():
    from process.address_numeric_grid_alias import run_numeric_grid_alias

    source_key = await _insert_archive_address(
        SCHEMA, first_line="1548 E 4500", second_line="Suite 202", strict_source_bits=1
    )
    target_key = await _insert_archive_address(
        SCHEMA, first_line="1548 E 4500 S", second_line="Suite 202", strict_source_bits=6
    )
    shadow = await run_numeric_grid_alias(mode="shadow", schema=SCHEMA)
    await _apply_reviewed_alias(SCHEMA, source_key, target_key, shadow)


async def _insert_main_row(session, table_name, address_key, *, invalid_payload=False):
    await session.execute(
        text(
            f'INSERT INTO mrf."{table_name}" '
            "(entity_type,entity_id,location_key,checksum,type,npi,address_key,"
            "first_line,city_name,state_name,state_code,postal_code,zip5,country_code,base_address_version) "
            "VALUES ('synthetic','synthetic-provider','synthetic-location',1,'practice',1000000001,"
            "CAST(:key AS uuid),'10 Example Street','Example City','TX','TX','75001','75001','US',:version)"
        ),
        {
            "key": address_key,
            "version": "invalid" if invalid_payload else publication._native().ALIAS_BASE_ADDRESS_VERSION_PREFIX + "1",
        },
    )


async def _seed_stage_payload(suffix, *, invalid_payload):
    address_key = await db.scalar("SELECT address_key::text FROM mrf.entity_address_unified")
    names_by_model = publication._stage_names(SCHEMA, suffix)
    async with db.transaction() as session:
        await _insert_main_row(
            session, names_by_model["entity_address_unified"], address_key, invalid_payload=invalid_payload
        )
        for model in generation.ENTITY_ADDRESS_RESULT_MODELS[1:]:
            original = model.__tablename__
            columns = ",".join(f'"{column.name}"' for column in model.__table__.columns)
            await session.execute(
                text(f'INSERT INTO mrf."{names_by_model[original]}" ({columns}) SELECT {columns} FROM mrf."{original}"')
            )


async def _handoff(context):
    result = await publication.handoff_entity_address_generation(
        context, schema_name=SCHEMA, import_date=context["import_date"], dependency_bindings=None, row_count=1
    )
    handoff = result["address_handoff"]
    assert context["context"]["control_run_handoff_committed"] is True
    assert (
        await db.scalar("SELECT status FROM mrf.import_run WHERE run_id=:run", run=context["control_run_id"])
        == "finalizing"
    )
    return handoff


async def _publish(resources, handoff, *, rollback):
    async def continuation(session, _receipt, cutover):
        assert await session.scalar(text("SELECT current_user=session_user")) is True
        receipt = await cutover()
        assert receipt["result_generation"]["serving_generation"]["origin_generation"] == 1
        if rollback:
            raise RuntimeError("publication rollback probe")
        return receipt

    async with resources.publisher.session_factory() as session, session.begin():
        return await publication.complete_entity_address_unified_handoff(
            session, handoff, dependency_bindings=handoff["dependency_bindings"], publication_continuation=continuation
        )


async def _canonical_oids(resources):
    return tuple(
        [
            await resources.admin.fetchval("SELECT to_regclass($1)::oid::bigint", f"mrf.{name}")
            for name in generation.RELATION_NAMES
        ]
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["published", "rollback", "invalid_payload"])
async def test_native_ordinary_address_publication_preserves_transaction_and_payload_guards(monkeypatch, outcome):
    async with _publication_database(monkeypatch) as resources:
        incumbent_oids = await _canonical_oids(resources)
        context = _attempt_context()
        await _create_attempt_stage(context, invalid_payload=outcome == "invalid_payload")
        handoff = await _handoff(context)
        if outcome != "published":
            expected = "publication rollback probe" if outcome == "rollback" else "stale address alias generation"
            with pytest.raises(RuntimeError, match=expected):
                await _publish(resources, handoff, rollback=outcome == "rollback")
            assert await _canonical_oids(resources) == incumbent_oids
            assert await db.scalar("SELECT local_generation FROM mrf.entity_address_result_generation") == 0
            assert (
                await db.scalar("SELECT status FROM mrf.import_run WHERE run_id=:run", run=context["control_run_id"])
                == "finalizing"
            )
            return
        receipt = await _publish(resources, handoff, rollback=False)
        assert await _canonical_oids(resources) == tuple(entry["relation_oid"] for entry in handoff["stage_relations"])
        assert await db.scalar("SELECT local_generation FROM mrf.entity_address_result_generation") == 1
        async with resources.publisher.transaction() as session:
            assert await publication.read_entity_address_native_publication(session, handoff) == receipt
        assert await resources.admin.fetchval("SELECT count(*) FROM mrf.entity_address_unified") == 1
        assert len(receipt["inventory"]["relations"]) == 7


def _native_copy(source_connection, copy_sizes):
    async def copy_rows(session, query, *, schema_name, table_name, columns, max_bytes, timeout):
        chunks = bytearray()
        census_by_field = {"is_over_budget": False}

        async def capture(chunk):
            if len(chunks) + len(chunk) > max_bytes:
                census_by_field["is_over_budget"] = True
            elif not census_by_field["is_over_budget"]:
                chunks.extend(chunk)

        async with asyncio.timeout(timeout):
            await source_connection.copy_from_query(query, output=capture, format="binary", timeout=timeout)
            if census_by_field["is_over_budget"]:
                raise RuntimeError("native COPY exceeded admitted bytes")
            connection = await session.connection()
            raw_connection = await connection.get_raw_connection()
            await raw_connection.driver_connection.copy_to_table(
                table_name,
                schema_name=schema_name,
                source=memoryview(chunks),
                columns=columns,
                format="binary",
                timeout=timeout,
            )
        copy_sizes.append((table_name, len(chunks), max_bytes))
        return len(chunks)

    return copy_rows


async def _clone_published_family(resources, handoff, *, byte_limit=1024 * 1024):
    await resources.admin.execute(
        "GRANT SELECT ON "
        + ",".join(f'mrf."{name}"' for name in generation.RELATION_NAMES)
        + f' TO "{resources.roles["writer"]}"'
    )
    dataset_id = uuid4()
    captured_owners = []
    copy_sizes = []

    async def on_precreated(session):
        owner = await ownership.capture_created_entity_address_archive_stage(session, dataset_id=dataset_id)
        assert len(owner.relation_oids) == 8
        for name, _oid in owner.relation_oids:
            assert await session.scalar(text(f'SELECT count(*) FROM "{owner.schema_name}"."{name}"')) == 0
        captured_owners.append(owner)

    async with resources.publisher.session_factory() as pinned, pinned.begin():
        source_capture = await source.capture_entity_address_archive_source(
            pinned, schema_name=SCHEMA, contract=source.CONTRACT
        )
        alias_receipt = await alias.capture_entity_address_alias_authority_receipt(pinned, schema_name=SCHEMA)
        source_connection = await pinned.connection()
        raw_source = await source_connection.get_raw_connection()
        async with db.session_factory() as receiver_session, receiver_session.begin():
            receipt = await source.prepare_entity_address_archive_source(
                receiver_session,
                source_capture=source_capture,
                dataset_id=dataset_id,
                source_copy=source.EntityAddressSourceCopy(
                    _native_copy(raw_source.driver_connection, copy_sizes), byte_limit, 20
                ),
                on_precreated=on_precreated,
            )
            assert (
                await ownership.verify_entity_address_archive_stage_ownership(
                    receiver_session, owner=captured_owners[0]
                )
                == captured_owners[0]
            )
    assert len(copy_sizes) == 8
    assert sum(size for _name, size, _budget in copy_sizes) <= byte_limit
    assert all(0 < size <= budget for _name, size, budget in copy_sizes)
    assert tuple(name for name, _size, _budget in copy_sizes) == tuple(
        entry.table_name for entry in source_capture.relations
    )
    assert alias_receipt.active_alias_count == 1
    return {
        "owner": captured_owners[0],
        "semantic": receipt,
        "alias": alias_receipt,
        "destination": {
            "db_schema": SCHEMA,
            "import_date": "receive_" + dataset_id.hex[:12],
            "dependency_bindings": handoff["dependency_bindings"],
            "source_serving_generation": await _serving_generation(resources),
        },
    }


async def _serving_generation(resources):
    async with resources.publisher.session_factory() as session, session.begin():
        authority = await generation.read_entity_address_result_generation_authority(session, schema_name=SCHEMA)
        return authority.serving_generation.as_dict()


async def _load_received_heaps(captured):
    """Use the real empty v2 restore family, not the indexed export clone, for receipt load."""
    dataset_id = uuid4()
    destination_by_field = {**captured["destination"], "import_date": "receive_" + dataset_id.hex[:12]}
    copy_sizes = []
    async with db.session_factory() as pinned, pinned.begin():
        stage_capture = await source._capture_entity_address_archive_stage(
            pinned, dataset_id=captured["owner"].dataset_id, contract=source.CONTRACT
        )
        connection = await pinned.connection()
        raw_source = await connection.get_raw_connection()
        copy_rows = _native_copy(raw_source.driver_connection, copy_sizes)
        async with db.session_factory() as receiver, receiver.begin():
            await receiver.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
            await receiver.execute(text(f"SET TRANSACTION SNAPSHOT '{stage_capture.postgres_snapshot}'"))
            owner = await restore.precreate_entity_address_archive_restore(
                receiver,
                dataset_id=dataset_id,
                db_schema=SCHEMA,
                import_date=destination_by_field["import_date"],
                contract=source.CONTRACT,
            )
            assert (
                await receiver.scalar(
                    text("SELECT count(*) FROM pg_index WHERE indrelid=ANY(:oids)"),
                    {"oids": [oid for _name, oid in owner.relation_oids]},
                )
                == 0
            )
            budget = 1024 * 1024
            for model in (*generation.ENTITY_ADDRESS_RESULT_MODELS, alias.EntityAddressAliasAuthority):
                table_name = model.__tablename__
                columns = tuple(column.name for column in model.__table__.columns)
                query = "SELECT " + ",".join(f'"{name}"' for name in columns)
                query += f' FROM "{stage_capture.schema_name}"."{table_name}"'
                budget -= await copy_rows(
                    receiver,
                    query,
                    schema_name=owner.schema_name,
                    table_name=table_name,
                    columns=columns,
                    max_bytes=budget,
                    timeout=20,
                )
            for model in (*generation.ENTITY_ADDRESS_RESULT_MODELS, alias.EntityAddressAliasAuthority):
                assert await family_archive._is_model_table_equal(
                    receiver,
                    model,
                    left_schema=stage_capture.schema_name,
                    left_name=model.__tablename__,
                    right_schema=owner.schema_name,
                    right_name=model.__tablename__,
                )
    assert len(copy_sizes) == 8
    return {**captured, "owner": owner, "destination": destination_by_field}


async def _freeze_received_family(resources, owner):
    async with resources.publisher.session_factory() as session, session.begin():
        owner_oid = await preparation.require_entity_address_archive_publisher(session, db_schema=SCHEMA)
        await session.execute(text(f'ALTER SCHEMA "{owner.schema_name}" OWNER TO "{resources.roles["owner"]}"'))
        for _name, oid in owner.relation_oids:
            await preparation._seal_published_relation(session, oid, owner_oid)
        await session.execute(
            text(f'REVOKE CREATE ON SCHEMA "{owner.schema_name}" FROM PUBLIC,"{resources.roles["writer"]}"')
        )
        return await _observed_preparation(session, resources, owner)


async def _observed_preparation(session, resources, owner):
    assert await ownership.verify_entity_address_archive_stage_ownership(session, owner=owner) == owner
    owner_oid = await session.scalar(
        text("SELECT oid FROM pg_roles WHERE rolname=:name"), {"name": resources.roles["owner"]}
    )
    builder_oid = await session.scalar(
        text("SELECT oid FROM pg_roles WHERE rolname=:name"), {"name": resources.roles["writer"]}
    )
    assert (
        await session.scalar(text("SELECT nspowner FROM pg_namespace WHERE oid=:oid"), {"oid": owner.schema_oid})
        == owner_oid
    )
    sequence_oid = await session.scalar(
        text("SELECT to_regclass(:name)::oid"), {"name": f'"{owner.schema_name}"."{preparation._EVIDENCE_SEQUENCE}"'}
    )
    inventory_by_field = {
        "schema_name": owner.schema_name,
        "schema_oid": owner.schema_oid,
        "relations": [{"table_name": name, "relation_oid": oid} for name, oid in owner.relation_oids],
        "sequences": [
            {
                "sequence_name": preparation._EVIDENCE_SEQUENCE,
                "sequence_oid": sequence_oid,
                "owner_table": "entity_address_evidence",
                "owner_column": "evidence_id",
            }
        ],
    }
    return {
        "state": "frozen",
        "frozen_owner_oid": owner_oid,
        "builder_oid": builder_oid,
        "inventory": inventory_by_field,
        "inventory_sha256": preparation._digest(inventory_by_field),
    }


async def _prepare_received_family(resources, captured, frozen):
    async def authenticate(session):
        observed = await _observed_preparation(session, resources, captured["owner"])
        assert observed == frozen
        return observed

    async with resources.publisher.session_factory() as session, session.begin():
        return await preparation.prepare_loaded_entity_address_archive_destination(
            session,
            owner=captured["owner"],
            semantic_receipt=captured["semantic"],
            source_alias_receipt=captured["alias"],
            destination=captured["destination"],
            authenticate_preparation=authenticate,
        )


async def _activate_received_family(resources, captured, frozen, validation, incumbent, *, rollback):
    async def authenticate(session, supplied_preparation, supplied_validation):
        assert supplied_preparation == frozen
        assert supplied_validation == validation
        observed = await _observed_preparation(session, resources, captured["owner"])
        assert observed == frozen
        return {**observed, "state": "validated", "validation": {"evidence": validation}}

    async def before():
        assert await db.scalar("SELECT current_user=session_user") is True

    async def after():
        assert await db.scalar("SELECT local_generation FROM mrf.entity_address_result_generation") == 1
        if rollback:
            raise RuntimeError("received publication rollback probe")

    stored_by_field = {
        "contract": source.CONTRACT,
        **preparation._loaded_input(
            captured["owner"], captured["semantic"], captured["alias"], captured["destination"]
        ),
    }
    async with resources.publisher.session_factory() as session, session.begin():
        return await preparation.activate_validated_entity_address_archive_destination(
            session,
            stored=stored_by_field,
            validation=validation,
            preparation=frozen,
            expected_incumbent=incumbent,
            callbacks=adoption.EntityAddressSnapshotAdoptionCallbacks(verify_local_state=before, record_adoption=after),
            authenticate_preparation=authenticate,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("rollback", [False, True])
async def test_native_received_address_copy_seal_and_activation_preserve_origin_and_rollback(monkeypatch, rollback):
    async with _publication_database(monkeypatch) as resources:
        context = _attempt_context()
        await _create_attempt_stage(context)
        handoff = await _handoff(context)
        ordinary_receipt = await _publish(resources, handoff, rollback=False)
        incumbent_oids = await _canonical_oids(resources)
        async with resources.publisher.session_factory() as session, session.begin():
            incumbent = await serving.capture_entity_address_receive_admission(session, schema_name=SCHEMA)
        captured = await _load_received_heaps(await _clone_published_family(resources, handoff))
        frozen = await _freeze_received_family(resources, captured["owner"])
        validation = await _prepare_received_family(resources, captured, frozen)
        if rollback:
            with pytest.raises(RuntimeError, match="received publication rollback probe"):
                await _activate_received_family(resources, captured, frozen, validation, incumbent, rollback=True)
            assert await _canonical_oids(resources) == incumbent_oids
            assert await db.scalar("SELECT local_generation FROM mrf.entity_address_result_generation") == 1
            return
        result = await _activate_received_family(resources, captured, frozen, validation, incumbent, rollback=False)
        published = result["publication"]
        assert len(published["relations"]) == 7
        expected_oid_by_name = dict(captured["owner"].relation_oids)
        assert await _canonical_oids(resources) == tuple(
            expected_oid_by_name[name] for name in generation.RELATION_NAMES
        )
        assert await _serving_generation(resources) == ordinary_receipt["result_generation"]["serving_generation"]
        assert await db.scalar("SELECT local_generation FROM mrf.entity_address_result_generation") == 1
        assert published["alias_authority"]["relation_oid"] == expected_oid_by_name[alias.AUTHORITY_TABLE]


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["index", "marker"])
async def test_native_handoff_refuses_changed_custody(monkeypatch, change):
    async with _publication_database(monkeypatch) as resources:
        incumbent_oids = await _canonical_oids(resources)
        context = _attempt_context()
        await _create_attempt_stage(context)
        handoff = await _handoff(context)
        if change == "index":
            index_name = await resources.admin.fetchval(
                "SELECT quote_ident(n.nspname)||'.'||quote_ident(c.relname) FROM pg_class c "
                "JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.oid=ANY($1::oid[]) "
                "AND NOT EXISTS(SELECT 1 FROM pg_constraint k WHERE k.conindid=c.oid) ORDER BY c.oid LIMIT 1",
                [entry["index_oid"] for entry in handoff["indexes"]],
            )
            assert index_name is not None
            await resources.admin.execute("DROP INDEX " + index_name)
            expected_error = "indexes changed"
        else:
            await resources.admin.execute(
                f'COMMENT ON TABLE mrf."{handoff["stage_relations"][0]["stage_name"]}" IS NULL'
            )
            expected_error = "stage marker changed"
        with pytest.raises(RuntimeError, match=expected_error):
            await _publish(resources, handoff, rollback=False)
        assert await _canonical_oids(resources) == incumbent_oids
        assert await db.scalar("SELECT local_generation FROM mrf.entity_address_result_generation") == 0


@pytest.mark.asyncio
async def test_native_source_copy_budget_rolls_back_precreated_family(monkeypatch):
    async with _publication_database(monkeypatch) as resources:
        context = _attempt_context()
        await _create_attempt_stage(context)
        handoff = await _handoff(context)
        await _publish(resources, handoff, rollback=False)
        before_schemas = tuple(await resources.admin.fetch("SELECT oid,nspname FROM pg_namespace ORDER BY oid"))
        with pytest.raises(RuntimeError, match="native COPY exceeded admitted bytes"):
            await _clone_published_family(resources, handoff, byte_limit=1)
        assert tuple(await resources.admin.fetch("SELECT oid,nspname FROM pg_namespace ORDER BY oid")) == before_schemas
        assert await _canonical_oids(resources) == tuple(entry["relation_oid"] for entry in handoff["stage_relations"])
