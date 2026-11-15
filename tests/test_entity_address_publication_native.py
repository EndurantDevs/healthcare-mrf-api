# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Opt-in native publication proof with exact, disposable database and role custody."""

import asyncio
import json
from contextlib import asynccontextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4

import asyncpg
import pytest
from sqlalchemy import MetaData, text
from sqlalchemy.exc import DBAPIError

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
from tests.test_address_numeric_grid_alias_runtime_db import _create_alias_probe_schema
from tests.test_entity_address_alias_guard_postgres import _owned_guard_database
from tests.test_entity_address_snapshot_destination import (
    _create_geo_dependencies,
    _publish_npi_fixture_generation,
    _seed_support_rows,
)

SCHEMA = "mrf"


@asynccontextmanager
async def _publication_database(monkeypatch):
    """Use registered exact cleanup and real native roles from a bounded policy file."""
    with TemporaryDirectory(prefix="address_native_policy_") as folder:
        async with _owned_guard_database(monkeypatch) as resources:
            policy = Path(folder) / "roles.json"
            policy.write_text(
                json.dumps(
                    {
                        "owner_role": resources.admin_role,
                        "migration_role": resources.roles["publisher"],
                        "preparation_owner_role": resources.roles["owner"],
                        "runtime_roles": [resources.roles["writer"]],
                    }
                )
            )
            monkeypatch.setenv("HLTHPRT_SOURCE_PROFILE_ROLE_POLICY_FILE", str(policy))
            await _provision_publication_database(monkeypatch, resources)
            yield resources


async def _provision_publication_database(monkeypatch, resources):
    """Provision only resources inside the exact database created by the lifecycle."""
    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_PROTECTED_PUBLICATION", "true")
    await _provision_named_alias_schema(resources, SCHEMA, initial_generation=0)
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
    await _publish_npi_fixture_generation(db, SCHEMA)
    await _seed_active_alias()
    await _protect_control_state(resources)


async def _provision_named_alias_schema(resources, schema, *, initial_generation):
    """Keep genuine alias data/privileges without the retired metadata attestation."""
    from process import entity_address_alias_guard as guard

    admin, roles = resources.admin, resources.roles
    await _create_alias_probe_schema(admin, schema, "generation_guard")
    await admin.execute(f'GRANT USAGE,CREATE ON SCHEMA "{schema}" TO "{roles["writer"]}","{roles["owner"]}"')
    tables = await admin.fetch(
        "SELECT relname FROM pg_class JOIN pg_namespace n ON n.oid=relnamespace WHERE n.nspname=$1 AND relkind='r'",
        schema,
    )
    for row in tables:
        owner = (
            roles["owner"]
            if row["relname"] in {guard.alias._STATE_TABLE, guard.alias._ALIAS_TABLE}
            else roles["writer"]
        )
        await admin.execute(f'ALTER TABLE "{schema}"."{row["relname"]}" OWNER TO "{owner}"')
    for statement in guard.generation_guard_statements(schema):
        await admin.execute(statement)
    for function in guard.GUARDS:
        await admin.execute(f'ALTER FUNCTION "{schema}"."{function}"() OWNER TO "{roles["owner"]}"')
    await admin.execute(f'GRANT SELECT ON "{schema}".address_alias_state_v1 TO "{roles["writer"]}"')
    await admin.execute(
        f'GRANT SELECT,INSERT,UPDATE ({",".join(guard.REVOCATION_COLUMNS)}) ON "{schema}".address_alias_v1 TO "{roles["writer"]}"'
    )
    await admin.execute(f'UPDATE "{schema}".address_alias_state_v1 SET generation=$1', initial_generation)
    async with resources.publisher.transaction() as session:
        await preparation.require_entity_address_archive_publisher(session, db_schema=schema)


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


def _assert_named_metadata(value):
    """Fresh receipts identify stored objects by name while retaining semantic entity IDs."""
    if isinstance(value, dict):
        assert all(not key.endswith(("_oid", "_oids", "_filenode")) and key != "relfilenode" for key in value)
        for child in value.values():
            _assert_named_metadata(child)
    elif isinstance(value, list):
        for child in value:
            _assert_named_metadata(child)


async def _handoff(context):
    result = await publication.handoff_entity_address_generation(
        context, schema_name=SCHEMA, import_date=context["import_date"], dependency_bindings=None, row_count=1
    )
    handoff = result["address_handoff"]
    _assert_named_metadata(handoff)
    assert context["context"]["control_run_handoff_committed"] is True
    assert (
        await db.scalar("SELECT status FROM mrf.import_run WHERE run_id=:run", run=context["control_run_id"])
        == "finalizing"
    )
    return handoff


async def _publish(resources, handoff, *, rollback):
    async def continuation(session, _receipt, cutover):
        assert session.in_transaction()
        receipt = await cutover()
        _assert_named_metadata(receipt)
        assert receipt["result_generation"]["serving_generation"]["origin_generation"] == 1
        if rollback:
            raise RuntimeError("publication rollback probe")
        return receipt

    async with resources.publisher.session_factory() as session, session.begin():
        return await publication.complete_entity_address_unified_handoff(
            session, handoff, dependency_bindings=handoff["dependency_bindings"], publication_continuation=continuation
        )


async def _relation_rows(resources, schema, names):
    """Read complete fixture payloads from declared names, independent of native storage tokens."""
    return {
        name: tuple(
            row["row"]
            for row in await resources.admin.fetch(
                f'SELECT to_jsonb(value)::text AS row FROM "{schema}"."{name}" value ORDER BY to_jsonb(value)::text'
            )
        )
        for name in names
    }


async def _canonical_rows(resources):
    """Read complete serving payloads to prove atomic publication rollback."""
    return await _relation_rows(resources, SCHEMA, generation.RELATION_NAMES)


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["published", "rollback", "invalid_payload"])
async def test_native_ordinary_address_publication_preserves_transaction_and_payload_guards(monkeypatch, outcome):
    async with _publication_database(monkeypatch) as resources:
        incumbent_rows = await _canonical_rows(resources)
        context = _attempt_context()
        await _create_attempt_stage(context, invalid_payload=outcome == "invalid_payload")
        handoff = await _handoff(context)
        if outcome != "published":
            expected = "publication rollback probe" if outcome == "rollback" else "stale address alias generation"
            with pytest.raises(RuntimeError, match=expected):
                await _publish(resources, handoff, rollback=outcome == "rollback")
            assert await _canonical_rows(resources) == incumbent_rows
            assert await db.scalar("SELECT local_generation FROM mrf.entity_address_result_generation") == 0
            assert (
                await db.scalar("SELECT status FROM mrf.import_run WHERE run_id=:run", run=context["control_run_id"])
                == "finalizing"
            )
            return
        receipt = await _publish(resources, handoff, rollback=False)
        assert set(await _canonical_rows(resources)) == set(generation.RELATION_NAMES)
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
        assert len(owner.relation_names) == 8
        for name in owner.relation_names:
            assert await session.scalar(text(f'SELECT count(*) FROM "{owner.schema_name}"."{name}"')) == 0
        captured_owners.append(owner)

    async with resources.publisher.session_factory() as pinned, pinned.begin():
        queued = await serving.capture_entity_address_receive_admission(pinned, schema_name=SCHEMA)
        source_capture = await source.capture_entity_address_archive_source(
            pinned, schema_name=SCHEMA, queued_serving_capture=queued, contract=source.CONTRACT
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
                    text(
                        "SELECT count(*) FROM pg_index i JOIN pg_class c ON c.oid=i.indrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=:schema AND c.relname=ANY(:names)"
                    ),
                    {"schema": owner.schema_name, "names": list(owner.relation_names)},
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
    return {**captured, "raw_source_owner": captured["owner"], "owner": owner, "destination": destination_by_field}


async def _freeze_received_family(resources, owner):
    from process.cms_doctors_immutable import _freeze_named_model_relations

    async with resources.publisher.session_factory() as session, session.begin():
        await preparation.require_entity_address_archive_publisher(session, db_schema=SCHEMA)
        await session.execute(text(f'ALTER SCHEMA "{owner.schema_name}" OWNER TO "{resources.roles["owner"]}"'))
        await _freeze_named_model_relations(session, owner.schema_name, owner.relation_names)
        await session.execute(
            text(f'REVOKE CREATE ON SCHEMA "{owner.schema_name}" FROM PUBLIC,"{resources.roles["writer"]}"')
        )
        await session.execute(text(f'GRANT USAGE ON SCHEMA "{owner.schema_name}" TO "{resources.roles["writer"]}"'))
        return {"state": "frozen", **await _observed_preparation(session, resources, owner)}


async def _observed_preparation(session, resources, owner):
    assert await ownership.verify_entity_address_archive_stage_ownership(session, owner=owner) == owner
    inventory_by_field = {
        "schema_name": owner.schema_name,
        "relations": [{"schema_name": owner.schema_name, "relation_name": name} for name in owner.relation_names],
        "sequences": [
            {
                "sequence_name": preparation._EVIDENCE_SEQUENCE,
                "owner_table": "entity_address_evidence",
                "owner_column": "evidence_id",
            }
        ],
    }
    return {"inventory": inventory_by_field, "inventory_sha256": preparation._digest(inventory_by_field)}


async def _prepare_received_family(resources, captured):
    async def authenticate(session):
        observed = await _observed_preparation(session, resources, captured["owner"])
        return {"state": "handed_off", **observed}

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
        assert {"state": "frozen", **observed} == frozen
        return {**observed, "state": "validated", "validation": {"evidence": validation}}

    async def before():
        assert db._transaction_binding().session.in_transaction()

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


async def _assert_frozen_family_access(owner):
    """Real runtime SELECT remains usable while native DML and DDL are denied."""
    for name in owner.relation_names:
        table = f'"{owner.schema_name}"."{name}"'
        assert await db.scalar(f"SELECT count(*) FROM {table}") >= 0
        for statement in (
            f"DELETE FROM {table} WHERE FALSE",
            f"ALTER TABLE {table} ADD COLUMN forbidden_write integer",
        ):
            with pytest.raises(DBAPIError) as caught:
                async with db.transaction() as session:
                    await session.execute(text(statement))
            assert caught.value.orig.sqlstate == "42501"


async def _refuse_changed_frozen_content(resources, captured, frozen, validation, incumbent):
    """Same-count data drift is refused before atomic publication, even under elevated fixture access."""
    table = f'"{captured["owner"].schema_name}"."entity_address_unified"'
    original = await resources.admin.fetchval(f"SELECT city_name FROM {table}")
    count = await resources.admin.fetchval(f"SELECT count(*) FROM {table}")
    await resources.admin.execute(f"UPDATE {table} SET city_name=$1", "Changed City")
    try:
        with pytest.raises(preparation.EntityAddressSnapshotDestinationError, match="content"):
            await _activate_received_family(resources, captured, frozen, validation, incumbent, rollback=False)
        assert await resources.admin.fetchval(f"SELECT count(*) FROM {table}") == count
    finally:
        await resources.admin.execute(f"UPDATE {table} SET city_name=$1", original)


@pytest.mark.asyncio
@pytest.mark.parametrize("rollback", [False, True])
async def test_native_received_address_copy_seal_and_activation_preserve_origin_and_rollback(monkeypatch, rollback):
    async with _publication_database(monkeypatch) as resources:
        context = _attempt_context()
        await _create_attempt_stage(context)
        handoff = await _handoff(context)
        ordinary_receipt = await _publish(resources, handoff, rollback=False)
        incumbent_rows = await _canonical_rows(resources)
        async with resources.publisher.session_factory() as session, session.begin():
            incumbent = await serving.capture_entity_address_receive_admission(session, schema_name=SCHEMA)
        captured = await _load_received_heaps(await _clone_published_family(resources, handoff))
        raw_rows = await _relation_rows(
            resources, captured["raw_source_owner"].schema_name, captured["raw_source_owner"].relation_names
        )
        validation = await _prepare_received_family(resources, captured)
        _assert_named_metadata(validation)
        frozen = await _freeze_received_family(resources, captured["owner"])
        await _assert_frozen_family_access(captured["owner"])
        await _refuse_changed_frozen_content(resources, captured, frozen, validation, incumbent)
        assert (
            await _relation_rows(
                resources, captured["raw_source_owner"].schema_name, captured["raw_source_owner"].relation_names
            )
            == raw_rows
        )
        assert await _canonical_rows(resources) == incumbent_rows
        if rollback:
            with pytest.raises(RuntimeError, match="received publication rollback probe"):
                await _activate_received_family(resources, captured, frozen, validation, incumbent, rollback=True)
            assert await _canonical_rows(resources) == incumbent_rows
            assert await db.scalar("SELECT local_generation FROM mrf.entity_address_result_generation") == 1
            return
        publication_result = await _activate_received_family(
            resources, captured, frozen, validation, incumbent, rollback=False
        )
        published = publication_result["publication"]
        assert len(published["relations"]) == 7
        assert published["relations"] == [
            {"schema_name": SCHEMA, "relation_name": name} for name in generation.RELATION_NAMES
        ]
        assert await _serving_generation(resources) == ordinary_receipt["result_generation"]["serving_generation"]
        assert await db.scalar("SELECT local_generation FROM mrf.entity_address_result_generation") == 1
        assert published["alias_authority"] == {
            "schema_name": captured["owner"].schema_name,
            "relation_name": alias.AUTHORITY_TABLE,
        }


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["index", "content"])
async def test_native_handoff_refuses_changed_layout_or_content(monkeypatch, change):
    async with _publication_database(monkeypatch) as resources:
        incumbent_rows = await _canonical_rows(resources)
        context = _attempt_context()
        await _create_attempt_stage(context)
        handoff = await _handoff(context)
        if change == "index":
            index_name = await resources.admin.fetchval(
                "SELECT quote_ident(n.nspname)||'.'||quote_ident(idx.relname) FROM pg_index i "
                "JOIN pg_class idx ON idx.oid=i.indexrelid JOIN pg_class c ON c.oid=i.indrelid "
                "JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 AND c.relname=ANY($2::text[]) "
                "AND NOT EXISTS(SELECT 1 FROM pg_constraint k WHERE k.conindid=idx.oid) ORDER BY idx.relname LIMIT 1",
                SCHEMA,
                [entry["stage_name"] for entry in handoff["stage_relations"]],
            )
            assert index_name is not None
            await resources.admin.execute("DROP INDEX " + index_name)
            expected_error = "indexes changed"
        else:
            await resources.admin.execute(
                f'UPDATE mrf."{handoff["stage_relations"][0]["stage_name"]}" SET city_name=$1', "Changed City"
            )
            expected_error = "stage content changed"
        with pytest.raises(RuntimeError, match=expected_error):
            await _publish(resources, handoff, rollback=False)
        assert await _canonical_rows(resources) == incumbent_rows
        assert await db.scalar("SELECT local_generation FROM mrf.entity_address_result_generation") == 0


@pytest.mark.asyncio
async def test_native_source_copy_budget_rolls_back_precreated_family(monkeypatch):
    async with _publication_database(monkeypatch) as resources:
        context = _attempt_context()
        await _create_attempt_stage(context)
        handoff = await _handoff(context)
        await _publish(resources, handoff, rollback=False)
        before_schemas = tuple(await resources.admin.fetch("SELECT nspname FROM pg_namespace ORDER BY nspname"))
        with pytest.raises(RuntimeError, match="native COPY exceeded admitted bytes"):
            await _clone_published_family(resources, handoff, byte_limit=1)
        assert tuple(await resources.admin.fetch("SELECT nspname FROM pg_namespace ORDER BY nspname")) == before_schemas
        assert set(await _canonical_rows(resources)) == set(generation.RELATION_NAMES)


async def _record_cancelled_candidate(resources, handoff):
    preparation_id = str(uuid4())
    candidate_by_field = {
        key: handoff[key]
        for key in (
            "schema_name",
            "attempt_id",
            "attempt_started_at",
            "handoff_sha256",
            "source_contract_sha256",
            "stage_relations",
            "indexes",
        )
    }
    abandonment_by_field = {
        "contract": "entity-address-unpublished-abandonment.v2",
        "preparation_id": preparation_id,
        "admission_sha256": "a" * 64,
        "generation_id": str(uuid4()),
        "publication_fence": str(uuid4()),
        "reason": "terminal-cancellation",
        "stage_disposition": "retained-unpublished",
        "physical_cleanup_completed": False,
        "same_run_readmission": "requires-exact-candidate-cleanup-and-ledger-retirement",
        "observed_run": {
            "run_id": handoff["run_id"],
            "node_id": "synthetic-node",
            "status": "cancelled",
            "progress": {key: handoff[key] for key in ("attempt_id", "attempt_started_at")},
            "handoff_sha256": handoff["handoff_sha256"],
            "phase_detail": publication.HANDOFF_PHASE,
            "finished_at": "2026-01-02T03:05:00Z",
        },
        "candidate": candidate_by_field,
        "released_pins": [],
    }
    await resources.admin.execute(
        "UPDATE mrf.import_run SET engine='healthcare-mrf-api',status='cancelled',finished_at=clock_timestamp(),"
        "metrics=(metrics::jsonb||jsonb_build_object('address_native_abandonments',$1::jsonb))::json WHERE run_id=$2",
        json.dumps({preparation_id: abandonment_by_field}),
        handoff["run_id"],
    )
    return abandonment_by_field


async def _native_candidate_cleanup(resources, handoff, abandonment):
    async def unreferenced(session, supplied_handoff, supplied_abandonment):
        assert supplied_handoff == handoff and supplied_abandonment == abandonment
        return await session.scalar(
            text("SELECT NOT EXISTS(SELECT 1 FROM mrf.native_cleanup_reference WHERE relation_name=ANY(:names))"),
            {"names": [entry["stage_name"] for entry in handoff["stage_relations"]]},
        )

    async with resources.publisher.session_factory() as session, session.begin():
        return await publication.cleanup_entity_address_native_handoff(
            session,
            handoff,
            abandonment=abandonment,
            assert_unreferenced=unreferenced,
        )


@pytest.mark.asyncio
async def test_native_cancelled_candidate_cleanup_preserves_serving_data(monkeypatch):
    async with _publication_database(monkeypatch) as resources:
        before = await _canonical_rows(resources)
        context = _attempt_context()
        await _create_attempt_stage(context)
        handoff = await _handoff(context)
        abandonment = await _record_cancelled_candidate(resources, handoff)
        candidate = handoff["stage_relations"][0]["stage_name"]
        await resources.admin.execute("CREATE TABLE mrf.native_cleanup_reference (relation_name text PRIMARY KEY)")
        await resources.admin.execute(
            f'GRANT SELECT ON mrf.native_cleanup_reference TO "{resources.roles["publisher"]}"'
        )
        await resources.admin.execute("INSERT INTO mrf.native_cleanup_reference VALUES ($1)", candidate)
        with pytest.raises(RuntimeError, match="remains referenced"):
            await _native_candidate_cleanup(resources, handoff, abandonment)
        await resources.admin.execute("DELETE FROM mrf.native_cleanup_reference WHERE relation_name=$1", candidate)
        await resources.admin.execute(f'CREATE VIEW mrf.native_cleanup_dependency AS SELECT * FROM mrf."{candidate}"')
        with pytest.raises(DBAPIError) as caught:
            await _native_candidate_cleanup(resources, handoff, abandonment)
        assert caught.value.orig.sqlstate == "2BP01"
        for entry in handoff["stage_relations"]:
            assert await resources.admin.fetchval("SELECT to_regclass($1) IS NOT NULL", f'mrf."{entry["stage_name"]}"')
        assert await _canonical_rows(resources) == before
        await resources.admin.execute("DROP VIEW mrf.native_cleanup_dependency")
        cleanup_receipt = await _native_candidate_cleanup(resources, handoff, abandonment)
        assert (
            cleanup_receipt["physical_cleanup_completed"] is True
            and cleanup_receipt["contract"] == publication.CLEANUP_CONTRACT
        )
        for entry in handoff["stage_relations"]:
            assert await resources.admin.fetchval("SELECT to_regclass($1)", f'mrf."{entry["stage_name"]}"') is None
        assert await _canonical_rows(resources) == before
        assert await resources.admin.fetchval("SELECT local_generation FROM mrf.entity_address_result_generation") == 0
