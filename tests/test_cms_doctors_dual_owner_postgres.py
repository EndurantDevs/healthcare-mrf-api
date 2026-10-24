# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native distinct-owner component proof; not an enrolled-runtime or serving-composite claim."""

import asyncio
from dataclasses import replace
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from process import cms_doctors_immutable as immutable
from process import cms_doctors_preparation as preparation
from process import provider_directory_generic_source_policy as source
from process import reference_family_archive as archive
from process import reference_family_result_generation as generation
from tests.cms_doctors_component_postgres_support import ordinary_doctors_reader
from tests.cms_doctors_preparation_postgres_support import doctors_database, stage_family


@pytest.mark.asyncio
async def test_distinct_source_and_retention_owners_keep_source_writer_and_read_contracts(monkeypatch):
    async with doctors_database(monkeypatch, cms_active=False) as fixture:
        component = fixture.component
        owner, retained = component.owner, component.retention_owner
        async with component.admin_engine.connect() as admin:
            assert owner != retained
            assert (
                await admin.scalar(
                    text(
                        "SELECT pg_has_role(:publisher,:retained,'SET') AND pg_has_role(:publisher,:physical,'USAGE') "
                        "AND NOT pg_has_role(:publisher,:physical,'SET')"
                    ),
                    {"publisher": component.retained_publisher, "retained": retained, "physical": owner},
                )
                is True
            )
            assert (
                await admin.scalar(
                    text(
                        "SELECT nspowner=CAST(:owner AS regrole) FROM pg_namespace WHERE nspname='hp_snapshot_retention'"
                    ).bindparams(owner=retained)
                )
                is True
            )
        context = await stage_family(component.database, component.policy.schema)
        async with preparation.prepare_cms_doctors_generation(context) as prepared:
            async with component.database.transaction():
                authority = await preparation.apply_prepared_cms_doctors_generation(prepared)
            async with ordinary_doctors_reader(component.database) as reader:
                assert (
                    await immutable.capture_doctors_serving_generation(reader, component.policy.schema)
                    == authority.serving_generation
                )
        await _assert_retained_read_only(component, authority)
        assert component.authority.local_generation + 1 == authority.local_generation


async def _assert_retained_read_only(component, authority):
    retained_engine = create_async_engine(
        component.admin_engine.url.set(username=component.retained_publisher, password=None)
    )
    try:
        async with async_sessionmaker(retained_engine)() as session, session.begin():
            assert (
                await immutable.capture_doctors_serving_generation(session, component.policy.schema)
                == authority.serving_generation
            )
            with pytest.raises(ValueError, match="publisher_connection_required"):
                await immutable.capture_doctors_predecessor(session, component.policy.schema)
            with pytest.raises(DBAPIError) as refused:
                async with session.begin_nested():
                    await session.execute(text(f'SET LOCAL ROLE "{component.owner}"'))
            assert refused.value.orig.sqlstate == "42501"
    finally:
        await retained_engine.dispose()


@pytest.mark.asyncio
async def test_archive_schema_uses_original_publisher_then_physical_owner_on_same_transaction(monkeypatch):
    async with doctors_database(monkeypatch, cms_active=False) as fixture:
        component = fixture.component
        async with component.database.transaction() as session:
            original = source.source_copy_session()
            native = source.bound_publisher().connection
            backend = await native.fetchval("SELECT pg_backend_pid()")
            assert not await native.fetchval("SELECT has_database_privilege(current_user,current_database(),'CREATE')")
            ownership = await archive.precreate_reference_family_restore(
                session,
                importer_id="cms-doctors",
                dataset_id=uuid4(),
            )
            assert session is original and await native.fetchval("SELECT pg_backend_pid()") == backend
            assert await native.fetchval("SELECT current_user::text") == component.owner
            assert await native.fetchval(
                "SELECT nspowner=$2::regrole FROM pg_namespace WHERE nspname=$1", ownership.schema_name, component.owner
            )
            assert await native.fetchval(
                "SELECT bool_and(relowner=$2::regrole) FROM pg_class WHERE oid=ANY($1::oid[])",
                [oid for _, oid in ownership.relation_oids],
                component.owner,
            )
            for model in immutable._models():
                name = str(model.__tablename__)
                assert name in dict(ownership.relation_oids)
                columns = tuple(str(column.name) for column in model.__table__.columns)
                await archive.native_copy_projection(
                    session,
                    "SELECT "
                    + ",".join('"' + column + '"' for column in columns)
                    + f' FROM "{component.policy.schema}"."{name}"',
                    schema_name=ownership.schema_name,
                    table_name=name,
                    columns=columns,
                    max_bytes=1024 * 1024,
                    timeout=30,
                )
            await archive.complete_reference_family_restore(session, ownership)
            owner_oid = await immutable._writer(session)
            await archive.seal_model_family_storage(session, ownership, owner_oid)
            assert not await native.fetchval(
                "SELECT EXISTS(SELECT FROM pg_trigger WHERE tgrelid=ANY($1::oid[]))",
                [oid for _, oid in ownership.relation_oids],
            )
            await archive.cleanup_reference_family_stage(session, ownership)


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel", [False, True])
async def test_publisher_body_failure_preserves_dual_owners_and_original_native_transaction(monkeypatch, cancel):
    async with doctors_database(monkeypatch, cms_active=False) as fixture:
        component = fixture.component
        marker = "doctors_body_" + uuid4().hex
        schema = "doctors_body_" + uuid4().hex
        entered = asyncio.Event()
        state_by_key = {}

        task = asyncio.create_task(_publisher_body_failure(component, schema, marker, cancel, entered, state_by_key))
        try:
            await asyncio.wait_for(entered.wait(), 30)
            if cancel:
                await _cancel_active_backend(component, task, marker, state_by_key["pid"])
            await asyncio.wait_for(task, 30)
        finally:
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        async with component.admin_engine.connect() as admin:
            assert await admin.scalar(text("SELECT to_regnamespace(:schema)"), {"schema": schema}) is None
            assert not await admin.scalar(
                text(
                    "SELECT EXISTS(SELECT FROM pg_stat_activity WHERE pid=:pid AND state='active' AND query LIKE :query)"
                ),
                {"pid": state_by_key["pid"], "query": "%" + marker + "%"},
            )


async def _publisher_body_failure(component, schema, marker, cancel, entered, state_by_key):
    async with component.database.transaction() as session:
        native = source.bound_publisher().connection
        state_by_key["pid"] = await native.fetchval("SELECT pg_backend_pid()")
        saved_owner = source._OWNER.get()
        try:
            async with source.source_publisher_body(session):
                assert await native.fetchval("SELECT current_user=session_user")
                await native.execute(f'CREATE SCHEMA "{schema}" AUTHORIZATION "{component.owner}"')
                entered.set()
                if cancel:
                    await native.execute("SELECT pg_sleep(60) /* " + marker + " */")
                raise RuntimeError("body rejected")
        except RuntimeError, asyncio.CancelledError:
            assert source._OWNER.get() is saved_owner
            assert await native.fetchval("SELECT current_user::text") == component.owner
            assert await native.fetchval("SELECT to_regnamespace($1)", schema) is None
            assert await native.fetchval("SELECT pg_backend_pid()") == state_by_key["pid"]
            assert await native.fetchval("SELECT 17") == 17


async def _cancel_active_backend(component, task, marker, pid):
    async with component.admin_engine.connect() as admin:
        async with asyncio.timeout(10):
            while not await admin.scalar(
                text(
                    "SELECT EXISTS(SELECT FROM pg_stat_activity WHERE pid=:pid AND state='active' AND query LIKE :query)"
                ),
                {"pid": pid, "query": "%" + marker + "%"},
            ):
                await asyncio.sleep(0.01)
        task.cancel()


@pytest.mark.asyncio
async def test_retained_publisher_captures_archive_with_distinct_physical_owner(monkeypatch):
    async with doctors_database(monkeypatch, cms_active=False) as fixture:
        component = fixture.component
        engine = create_async_engine(
            component.admin_engine.url.set(username=component.retained_publisher, password=None)
        )
        try:
            async with async_sessionmaker(engine)() as session, session.begin():
                capture = await archive.capture_reference_family_source(
                    session,
                    importer_id="cms-doctors",
                    schema_name=component.policy.schema,
                    source_metadata={"release": "synthetic-distinct-owner"},
                )
                assert capture.manifest.source_serving_generation == component.authority.serving_generation
                assert capture.manifest.source_capture_contract == archive.IMMUTABLE_NUCC_SOURCE_CAPTURE_CONTRACT
                assert len(capture.manifest.tables) == 3
                assert all(table.row_count == 1 for table in capture.manifest.tables)
                assert await session.scalar(text("SELECT current_user=session_user")) is True
                assert (
                    await session.scalar(
                        text("SELECT NOT pg_has_role(current_user,:owner,'SET')"), {"owner": component.owner}
                    )
                    is True
                )
        finally:
            await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("importer", ("cms-doctors", "geo"))
async def test_protected_received_family_replacement_restore_and_export(monkeypatch, tmp_path, importer):
    """A separate retained Publisher performs actual cuts without a SOURCE factory or role switch."""
    from tests.geo_source_postgres_support import geo_source_component_database
    from tests.test_geo_reference_family_postgres import _load_snapshot

    fixture = (
        doctors_database(monkeypatch, cms_active=False)
        if importer == "cms-doctors"
        else geo_source_component_database(monkeypatch)
    )
    async with fixture as received:
        component = received.component if importer == "cms-doctors" else received
        if importer == "geo":
            from process import geo_import

            monkeypatch.setattr(geo_import, "db", component.database)
            await _load_snapshot(component, tmp_path, "Synthetic City")
        engine = create_async_engine(component.admin_engine.url.set(username=component.retained_publisher))
        token = source._FACTORY.set(None)
        try:
            await _received_family_roundtrip(component, async_sessionmaker(engine), importer)
        finally:
            source._FACTORY.reset(token)
            await engine.dispose()


async def _received_family_roundtrip(component, sessions, importer):
    reads = await _prepare_received_reader_access(component, importer)
    first = await _received_clone(sessions, importer, component.policy.schema)
    await _assert_received_clone_private(component, first)
    await _refuse_received_custody(sessions, first, component.owner)
    await _activate_received_clone(sessions, first, rollback=True)
    await _assert_received_clone_private(component, first)
    assert await _received_reader_queries(component, importer) == reads
    first_receipt = await _activate_received_clone(sessions, first)
    assert await _received_reader_queries(component, importer) == reads
    second = await _received_clone(sessions, importer, component.policy.schema)
    await _assert_received_clone_private(component, second)
    assert second.manifest.source_serving_generation == first.manifest.source_serving_generation
    second_receipt = await _activate_received_clone(sessions, second)
    assert await _received_reader_queries(component, importer) == reads
    assert second_receipt.predecessor_schema_name is not None
    await _restore_received_clone(sessions, first, second, second_receipt.predecessor_schema_name, rollback=True)
    assert await _received_reader_queries(component, importer) == reads
    restored = await _restore_received_clone(sessions, first, second, second_receipt.predecessor_schema_name)
    assert await _received_reader_queries(component, importer) == reads
    assert restored.relation_oids == tuple(oid for _name, oid in first_receipt.relation_oids)
    exported = await _received_clone(sessions, importer, component.policy.schema)
    await _assert_received_clone_private(component, exported)
    await _export_received_clone(sessions, exported)


async def _prepare_received_reader_access(component, importer):
    """Use existing ordinary logins for full-table and strictly column-only reads."""
    async with component.admin_engine.begin() as admin:
        for model in archive.reference_family_spec(importer).model_types:
            relation = f'"{component.policy.schema}"."{model.__tablename__}"'
            column = next(iter(model.__table__.columns)).name
            await admin.execute(text(f'GRANT SELECT ON TABLE {relation} TO "{component.reader}"'))
            await admin.execute(text(f'REVOKE SELECT ON TABLE {relation} FROM "{component.builder}"'))
            await admin.execute(
                text(f'GRANT SELECT ("{column}") ON TABLE {relation} TO "{component.builder}" WITH GRANT OPTION')
            )
    return await _received_reader_queries(component, importer)


async def _assert_received_clone_private(component, prepared):
    async with component.admin_engine.connect() as admin:
        for _name, oid in prepared.ownership.relation_oids:
            for role in (component.reader, component.builder):
                assert (
                    await admin.scalar(
                        text(
                            "SELECT NOT has_table_privilege(:role,CAST(:oid AS oid),'SELECT') "
                            "AND NOT has_any_column_privilege(:role,CAST(:oid AS oid),'SELECT')"
                        ),
                        {"role": role, "oid": oid},
                    )
                    is True
                )


async def _received_reader_queries(component, importer):
    """Query through genuine logins after transaction completion, including denied broad reads."""
    tables = archive.reference_family_spec(importer).model_types
    read_results = []
    async with ordinary_doctors_reader(component.database) as reader:
        for model in tables:
            read_results.append(
                await reader.scalar(text(f'SELECT count(*) FROM "{component.policy.schema}"."{model.__tablename__}"'))
            )
    engine = create_async_engine(component.admin_engine.url.set(username=component.builder))
    try:
        async with async_sessionmaker(engine)() as reader, reader.begin():
            assert await reader.scalar(text("SELECT current_user=session_user")) is True
            assert await reader.scalar(text("SHOW transaction_read_only")) == "off"
            for model in tables:
                read_results.append(await _received_column_read(reader, component.policy.schema, model))
    finally:
        await engine.dispose()
    return read_results


async def _received_column_read(reader, schema, model):
    """Exercise the granted column and refuse a broader read in an isolated savepoint."""
    relation = f'"{schema}"."{model.__tablename__}"'
    column = next(iter(model.__table__.columns)).name
    selected = await reader.scalar(text(f'SELECT "{column}" FROM {relation} ORDER BY 1 LIMIT 1'))
    assert await reader.scalar(
        text("SELECT has_column_privilege(current_user,:relation,:column,'SELECT WITH GRANT OPTION')"),
        {"relation": relation, "column": column},
    )
    with pytest.raises(DBAPIError) as refused:
        async with reader.begin_nested():
            await reader.execute(text(f"SELECT * FROM {relation} LIMIT 0"))
    assert refused.value.orig.sqlstate == "42501"
    return selected


async def _received_clone(sessions, importer, schema):
    """Use real native COPY and sealed clone validation on the separate Publisher connection."""
    async with sessions() as session, session.begin():
        capture = await archive.capture_reference_family_source(
            session, importer_id=importer, schema_name=schema, source_metadata={"release": "synthetic-received"}
        )
    spec = archive.reference_family_spec(importer)
    async with sessions() as session, session.begin():
        owner = await archive.protected_publisher_owner(session)
        ownership = await archive.create_model_family_export_clone(
            session, spec, archive.reference_family_stage_schema(uuid4())
        )
        for model in spec.model_types:
            name, columns = str(model.__tablename__), tuple(str(column.name) for column in model.__table__.columns)
            fields = ",".join(archive._quoted(column) for column in columns)
            await archive.native_copy_projection(
                session,
                f"SELECT {fields} FROM {archive._quoted(schema)}.{archive._quoted(name)}",
                schema_name=ownership.schema_name,
                table_name=name,
                columns=columns,
                max_bytes=1024 * 1024,
                timeout=30,
            )
        await archive.complete_reference_family_restore(session, ownership)
        await archive.seal_model_family_storage(session, ownership, owner)
        validation = await archive.prepare_reference_family_activation(
            session,
            ownership=ownership,
            manifest=capture.manifest,
            package_id="a" * 64,
            profile_contract=archive.reference_family_profile_contract(capture.manifest),
            sealed_owner_oid=owner,
        )
    return SimpleNamespace(
        ownership=ownership,
        manifest=capture.manifest,
        validation=validation,
        owner=owner,
        schema=schema,
        cutover=archive.ReferenceFamilyCutoverAuthority(
            "a" * 64,
            archive.reference_family_profile_contract(capture.manifest),
            owner,
            owner,
            "manual",
            capture.manifest.source_serving_generation,
        ),
    )


async def _activate_received_clone(sessions, prepared, *, rollback=False):
    async with sessions() as session, session.begin():
        incumbent = await archive.capture_reference_family_incumbent(
            session, importer_id=prepared.manifest.importer_id, schema_name=prepared.schema
        )
        async with session.begin_nested() as cut:
            receipt = await archive.activate_validated_reference_family_stage(
                session,
                ownership=prepared.ownership,
                manifest=prepared.manifest,
                expected_incumbent=incumbent,
                validation_receipt=prepared.validation,
                cutover=prepared.cutover,
            )
            assert await session.scalar(text("SELECT current_user=session_user")) is True
            assert source._PUBLISHER.get() is None and source._OWNER.get() is None
            with pytest.raises(ValueError, match="publisher_connection_required"):
                await immutable._writer(session)
            if rollback:
                await cut.rollback()
        if rollback:
            await archive.verify_reference_family_stage_ownership(session, prepared.ownership)
            await archive._verify_incumbent(session, incumbent)
        return receipt


async def _refuse_received_custody(sessions, prepared, physical_owner):
    async with sessions() as session, session.begin():
        assert not await session.scalar(
            text("SELECT pg_has_role(current_user,:owner,'SET')"), {"owner": physical_owner}
        )
        incumbent = await archive.capture_reference_family_incumbent(
            session, importer_id=prepared.manifest.importer_id, schema_name=prepared.schema
        )
        activation_by_field = dict(
            ownership=prepared.ownership,
            manifest=prepared.manifest,
            expected_incumbent=incumbent,
            validation_receipt=prepared.validation,
            cutover=prepared.cutover,
        )
        with pytest.raises(archive.ReferenceFamilyArchiveError, match="reference family stage owner differs"):
            await archive.activate_validated_reference_family_stage(
                session,
                **{
                    **activation_by_field,
                    "cutover": replace(prepared.cutover, expected_stage_owner_oid=prepared.owner + 1),
                },
            )
        async with session.begin_nested() as changed:
            await session.execute(
                text(
                    f'ALTER TABLE "{prepared.ownership.schema_name}"."{prepared.ownership.relation_oids[0][0]}" OWNER TO CURRENT_USER'
                )
            )
            with pytest.raises(archive.ReferenceFamilyArchiveError, match="reference family stage owner differs"):
                await archive.activate_validated_reference_family_stage(session, **activation_by_field)
            await changed.rollback()
        await _refuse_received_scope_transfer(sessions, session, prepared)


async def _refuse_received_scope_transfer(sessions, session, prepared):
    importer = prepared.manifest.importer_id
    async with archive.protected_reference_family_publication(
        session, importer_id=importer, schema_name=prepared.schema, expected_owner_oid=prepared.owner
    ):
        async with sessions() as other, other.begin():
            with pytest.raises(RuntimeError, match="protected_scope_changed"):
                await immutable._archive_writer(other, importer, prepared.schema)
        with pytest.raises(RuntimeError, match="protected_scope_changed"):
            await asyncio.create_task(immutable._archive_writer(session, importer, prepared.schema))
        with pytest.raises(ValueError, match="publisher_connection_required"):
            await immutable._writer(session)


async def _restore_received_clone(sessions, first, second, retained_schema, *, rollback=False):
    """Exercise the same locked native predecessor/adoption pair used by retained restore."""
    importer, spec = first.manifest.importer_id, archive.reference_family_spec(first.manifest.importer_id)
    async with sessions() as session, session.begin():
        async with archive.protected_reference_family_publication(
            session, importer_id=importer, schema_name=first.schema, expected_owner_oid=first.owner
        ):
            await archive._lock_family(session, first.schema, spec.archive_names, "ACCESS EXCLUSIVE")
            await archive._lock_family(session, retained_schema, spec.archive_names, "ACCESS EXCLUSIVE")
            incumbent = await archive.capture_reference_family_incumbent(
                session, importer_id=importer, schema_name=first.schema
            )
            predecessor = await generation._read_immutable_activation_predecessor(session, first.manifest, incumbent)
            arguments = await _received_geo_cut(session, first, retained_schema)
            async with session.begin_nested() as cut:
                displaced = archive.reference_family_predecessor_schema(uuid4())
                await archive.create_model_family_predecessor_schema(session, importer, displaced)
                for name in spec.table_names:
                    await session.execute(text(f'ALTER TABLE "{first.schema}"."{name}" SET SCHEMA "{displaced}"'))
                    await session.execute(text(f'ALTER TABLE "{retained_schema}"."{name}" SET SCHEMA "{first.schema}"'))
                restored = await generation._adopt_validated_family_generation(
                    session,
                    first.manifest,
                    incumbent,
                    first.manifest.source_serving_generation,
                    tuple((name, dict(first.ownership.relation_oids)[name]) for name in spec.table_names),
                    predecessor,
                    **arguments,
                )
                if rollback:
                    await cut.rollback()
            if rollback:
                assert (
                    await generation.read_reference_family_result_generation_authority(
                        session, importer_id=importer, schema_name=first.schema
                    )
                    == predecessor
                )
                await archive._verify_incumbent(session, incumbent)
                assert dict(incumbent.relation_oids) == dict(second.ownership.relation_oids)
            return restored


async def _received_geo_cut(session, prepared, schema):
    if prepared.manifest.importer_id != "geo":
        return {}
    from process.geo_source_storage import capture_geo_retained_cut

    oid = dict(prepared.ownership.relation_oids)["geo_zip_lookup"]
    filenode = await session.scalar(text("SELECT pg_relation_filenode(CAST(:oid AS oid))::bigint"), {"oid": oid})
    return {
        "geo_retained_cut": await capture_geo_retained_cut(
            session,
            schema_name=schema,
            expected_relation_oid=oid,
            expected_relfilenode=filenode,
            manifest=prepared.manifest,
        )
    }


async def _export_received_clone(sessions, prepared):
    """Export the pinned private clone without inventing a generation ledger in its schema."""
    observed_tables = []

    async def capture(snapshot):
        async with sessions() as session, session.begin():
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
            await session.execute(text(f"SET TRANSACTION SNAPSHOT '{snapshot.postgres_snapshot}'"))
            assert (
                await session.scalar(
                    text("SELECT to_regclass(:relation)"),
                    {"relation": f'"{snapshot.ownership.schema_name}"."{generation.TABLE_NAME}"'},
                )
                is None
            )
            await archive.verify_reference_family_stage_ownership(session, snapshot.ownership)
            observed_tables.extend(
                await archive._validate_stage_manifest(
                    session, ownership=snapshot.ownership, manifest=snapshot.manifest
                )
            )

    manifest = await archive.export_prepared_reference_family_archive(
        sessions,
        prepared=archive.ReferenceFamilyPreparedSource(prepared.manifest, prepared.ownership),
        archive_copy=capture,
    )
    assert archive._has_matching_manifest_stage_tables(manifest, tuple(observed_tables))
