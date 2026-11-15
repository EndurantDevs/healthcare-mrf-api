# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed held-input bindings preserve canonical geo projection fences."""

import importlib
from contextlib import asynccontextmanager
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from sqlalchemy import MetaData, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from api import ptg2_geo_projection as projection
from process import entity_address_dependency_bindings as dependency_state
from process import tiger_held_inputs
from tests.test_entity_address_snapshot_destination import (
    _create_alias_relations,
    _create_geo_dependencies,
    _owned_native_database,
    _seed_geo_dependencies,
    _seed_source_rows,
)
from tests.test_entity_address_snapshot_stage_postgres import _create_model_family, _native_test_connection
from tests.test_entity_address_unified_no_live_mutation import _mock_shutdown_dependencies, _shutdown_context

native = importlib.import_module("process.entity_address_unified")

_DEPENDENCY_NAMES = (
    "mrf.npi_address",
    "mrf.mrf_address",
    "mrf.doctor_clinician_address",
    "mrf.geo_zip_lookup",
    "tiger.zip_state",
    "tiger.zcta5",
)


def test_source_selection_settings_have_no_default_catalog(monkeypatch):
    monkeypatch.delenv("HLTHPRT_TIGER_SOURCE_BINDING_RELATION", raising=False)
    monkeypatch.delenv("HLTHPRT_TIGER_SOURCE_PACKAGE_RELATION", raising=False)
    assert tiger_held_inputs._source_selection_relations() is None
    monkeypatch.setenv("HLTHPRT_TIGER_SOURCE_BINDING_RELATION", "example_snapshots.source_binding")
    monkeypatch.setenv("HLTHPRT_TIGER_SOURCE_PACKAGE_RELATION", "example_snapshots.package")
    assert tiger_held_inputs._source_selection_relations() == (
        '"example_snapshots"."source_binding"',
        '"example_snapshots"."package"',
    )


@pytest.mark.parametrize(
    "binding,package",
    (
        (None, "example.package"),
        ("example.binding", None),
        ("", ""),
        ("example.binding", "example.binding"),
        ("example.binding; SELECT 1", "example.package"),
        ('"example".binding', "example.package"),
        ("Example.binding", "example.package"),
        ("example.binding.extra", "example.package"),
        ("example.binding ", "example.package"),
        ("a" * 64 + ".binding", "example.package"),
        ("example.binding", "example." + "b" * 64),
    ),
)
def test_source_selection_settings_reject_partial_or_unsafe_relations(monkeypatch, binding, package):
    for name, value in (
        ("HLTHPRT_TIGER_SOURCE_BINDING_RELATION", binding),
        ("HLTHPRT_TIGER_SOURCE_PACKAGE_RELATION", package),
    ):
        if value is None:
            monkeypatch.delenv(name, raising=False)
        else:
            monkeypatch.setenv(name, value)
    with pytest.raises(ValueError, match="two distinct schema.table"):
        tiger_held_inputs._source_selection_relations()


def _example_bindings():
    return {
        name: {
            "schema_name": "held_input",
            "table_name": name.split(".")[1],
            "relation_oid": index,
            "relfilenode": index + 100,
        }
        for index, name in enumerate(_DEPENDENCY_NAMES, 1)
    }


@pytest.mark.parametrize("change", [None, "missing", "extra", "sql", "duplicate", "legacy_fields"])
def test_prepared_name_helpers_keep_a_closed_family_without_assigning_freshness(change):
    bindings_by_name = {
        name: {"schema_name": binding_fields["schema_name"], "table_name": binding_fields["table_name"]}
        for name, binding_fields in _example_bindings().items()
    }
    match change:
        case "missing":
            del bindings_by_name["tiger.zcta5"]
        case "extra":
            bindings_by_name["mrf.unreviewed"] = dict(bindings_by_name["mrf.npi_address"])
        case "sql":
            bindings_by_name["mrf.npi_address"]["schema_name"] = "input; SELECT 1"
        case "duplicate":
            bindings_by_name["mrf.npi_address"] = dict(bindings_by_name["mrf.mrf_address"])
        case "legacy_fields":
            bindings_by_name["mrf.npi_address"]["relation_oid"] = 1
    if change is not None:
        with pytest.raises(ValueError):
            projection.projection_named_dependency_lock_sql("mrf", bindings_by_name)
        with pytest.raises(ValueError):
            projection.projection_named_dependency_match_sql("mrf", bindings_by_name)
        return
    before = deepcopy(bindings_by_name)
    locks = projection.projection_named_dependency_lock_sql("mrf", bindings_by_name)
    checks = projection.projection_named_dependency_match_sql("mrf", bindings_by_name)
    assert bindings_by_name == before
    assert locks.startswith("LOCK TABLE ONLY ") and locks.endswith(" IN ACCESS SHARE MODE;")
    assert checks.count("EXISTS (SELECT 1 FROM pg_catalog.pg_class held") == 6
    assert "NOT held.relrowsecurity" in checks
    assert not any(
        token in checks for token in ("pg_relation_filenode", "relowner", "relacl", "active_relation_signature")
    )
    for binding in bindings_by_name.values():
        relation = f'"{binding["schema_name"]}"."{binding["table_name"]}"'
        assert "ONLY " + relation in locks and f"to_regclass('{relation}')" in checks


@pytest.mark.parametrize("change", ("missing", "extra", "sql", "schema_type", "extra_field", "duplicate"))
def test_legacy_binding_rejects_nonclosed_or_unsafe_names(change):
    bindings = _example_bindings()
    target = bindings["mrf.npi_address"]
    match change:
        case "missing":
            del bindings["tiger.zcta5"]
        case "extra":
            bindings["mrf.unreviewed"] = dict(target)
        case "sql":
            target["table_name"] = "data; SELECT 1"
        case "schema_type":
            target["schema_name"] = 42
        case "extra_field":
            target["sql"] = "SELECT 1"
        case "duplicate":
            bindings["mrf.mrf_address"] = dict(target)
    with pytest.raises(ValueError):
        projection.validate_projection_dependency_bindings("mrf", bindings)


@pytest.mark.parametrize("tokens", ((True, 0), (None, "opaque")))
def test_legacy_binding_tokens_roundtrip_without_becoming_authority(tokens):
    bindings_by_name = _example_bindings()
    bindings_by_name["mrf.npi_address"].update(relation_oid=tokens[0], relfilenode=tokens[1])
    original = deepcopy(bindings_by_name)
    assert projection.validate_projection_dependency_bindings("mrf", bindings_by_name) == original
    locks = projection.projection_dependency_lock_sql("mrf", dependency_bindings=bindings_by_name)
    assert 'ONLY "held_input"."npi_address"' in locks
    assert not any(token in locks for token in ("relfilenode", "relation_oid"))
    assert bindings_by_name == original


def test_sql_reads_held_tables_but_receipts_keep_only_canonical_keys():
    assert native._record_geo_assurance_candidate_sql is dependency_state.record_geo_assurance_candidate_sql
    assert native._activate_geo_assurance_candidate_sql is dependency_state.activate_geo_assurance_candidate_sql
    assert native._validate_schema_name is dependency_state.validate_schema_name
    assert native._validate_schema_name(" schema_name ") == "schema_name"
    for identifier in ("", "9schema", "schema.name", "table; SELECT 1"):
        with pytest.raises(ValueError, match="Invalid schema name"):
            native._record_geo_assurance_candidate_sql(identifier, "stage", 1)
        with pytest.raises(ValueError, match="Invalid schema name"):
            native._record_geo_assurance_candidate_sql("mrf", identifier, 1)
        with pytest.raises(ValueError, match="Invalid schema name"):
            native._activate_geo_assurance_candidate_sql(identifier)
    bindings = _example_bindings()
    sql = native._materialize_geo_assurance_sql("mrf", "address_stage", force=True, dependency_bindings=bindings)
    signature = projection.projection_relation_signature_sql("mrf", dependency_bindings=bindings)
    locks = projection.projection_dependency_lock_sql("mrf", dependency_bindings=bindings)
    for name, binding in bindings.items():
        physical = f'"{binding["schema_name"]}"."{binding["table_name"]}"'
        assert physical in sql and "ONLY " + physical in locks
        assert f"jsonb_build_array({binding['schema_name']!r}, {binding['table_name']!r})" in signature
        assert f"'{name}', jsonb_build_array" in signature
        assert f"to_regclass('{name}')" not in signature
    copied = projection.validate_projection_dependency_bindings("mrf", bindings)
    bindings["mrf.npi_address"]["relation_oid"] = 999
    assert copied["mrf.npi_address"]["relation_oid"] == 1
    assert "held_input" not in native._activate_geo_assurance_candidate_sql("mrf")


@pytest.mark.asyncio
async def test_native_projection_keeps_all_six_held_inputs_through_activation_and_readback(monkeypatch):
    """Project real rows, activate held identities, and reject drift in every role."""

    async_dsn, _environment = _native_test_connection()
    async with _owned_native_database(async_dsn, monkeypatch) as database:
        for statement in ("CREATE EXTENSION postgis", "CREATE SCHEMA tiger", "CREATE SCHEMA held_input"):
            await database.status(statement)
        async with database.engine.begin() as connection:
            await _create_model_family(connection, "mrf")
            await _seed_source_rows(connection, "mrf", 0)
        await _create_geo_dependencies(database, "mrf")
        await _create_alias_relations(database, "mrf", generation=0)
        await _seed_geo_dependencies(database, "mrf")
        bindings = await _hold_projection_inputs(database)
        monkeypatch.setattr(native, "db", database)
        wrong_name = deepcopy(bindings)
        wrong_name["mrf.npi_address"]["table_name"] = "unavailable_npi"
        with pytest.raises(Exception, match="unavailable_npi"):
            await native._project_geo_assurance_transaction(
                "mrf", "entity_address_unified", force=True, dependency_bindings=wrong_name
            )
        projected, invalid, stage_oid, forced = await native._project_geo_assurance_transaction(
            "mrf", "entity_address_unified", force=True, dependency_bindings=bindings
        )
        assert (projected, invalid, forced) == (5, 0, True)
        assert tuple(
            await database.first(
                "SELECT geo_evidence_source_id,geo_identity_coherent,geo_point_coherent "
                "FROM mrf.entity_address_unified WHERE location_key='nppes'"
            )
        ) == (1, True, True)
        signature = await database.scalar(
            "SELECT candidate_relation_signature FROM mrf.entity_address_geo_assurance_state"
        )
        assert signature == {
            name: [binding["schema_name"], binding["table_name"]] for name, binding in bindings.items()
        }
        await _assert_held_activation_and_readback(database, bindings, stage_oid)
        async with database.transaction():
            published = await native.result_generation.publish_local_entity_address_generation(
                database, schema_name="mrf"
            )
        serving = importlib.import_module("process.entity_address_snapshot_serving")
        async with database.session_factory() as session, session.begin():
            observed = await serving.capture_entity_address_observed_serving(session, schema_name="mrf")
        assert observed.result_generation == published.serving_generation
        assert observed.as_dict()["geo_assurance"]["active_relation_signature"] == signature
        # Declared selected names must remain available; old tokens cannot relocate them.
        table = bindings["mrf.npi_address"]["table_name"]
        await database.status(f'ALTER TABLE held_input."{table}" RENAME TO "{table}_predecessor"')
        async with database.session_factory() as session, session.begin():
            with pytest.raises(Exception, match=table):
                await serving.capture_entity_address_receive_admission(session, schema_name="mrf")
        async with database.session_factory() as session, session.begin():
            with pytest.raises(Exception):
                await serving.capture_entity_address_observed_serving(session, schema_name="mrf")


@pytest.mark.asyncio
@pytest.mark.parametrize("named", (False, True))
async def test_normal_finalizer_forwards_explicit_held_inputs_through_projection_and_cutover(monkeypatch, named):
    events = []
    _mock_shutdown_dependencies(monkeypatch, events)
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "mrf")
    bindings_by_name = _example_bindings()
    if named:
        bindings_by_name = {
            name: {field: binding_fields[field] for field in ("schema_name", "table_name")}
            for name, binding_fields in bindings_by_name.items()
        }
    geometry = AsyncMock(return_value=0)
    monkeypatch.setattr(native, "_materialize_geo_assurance", geometry)
    await native.publish_entity_address_unified_generation(
        _shutdown_context(refresh_mode=native.ENTITY_ADDRESS_REFRESH_MODE_FULL),
        dependency_bindings=bindings_by_name,
    )
    assert geometry.await_args.kwargs["dependency_bindings"] == bindings_by_name
    assert geometry.await_args.kwargs.get("record_candidate", True) is True
    assert ("publish", "cutover") in events

    @asynccontextmanager
    async def held_selection(_database, schema, *, control_context=None):
        assert schema == "mrf"
        assert control_context is None
        events.append(("custody", "locked"))
        yield bindings_by_name
        assert geometry.await_args.kwargs["dependency_bindings"] == bindings_by_name
        assert events[-1] != ("custody", "locked")
        events.append(("custody", "released"))

    monkeypatch.setattr(native, "selected_publication_dependencies", held_selection)
    await native.shutdown(_shutdown_context(refresh_mode=native.ENTITY_ADDRESS_REFRESH_MODE_FULL))
    assert events[-1] == ("custody", "released")
    changed = deepcopy(bindings_by_name)
    changed.pop("tiger.zcta5")
    with pytest.raises(ValueError):
        await native.publish_entity_address_unified_generation(
            _shutdown_context(refresh_mode=native.ENTITY_ADDRESS_REFRESH_MODE_FULL),
            dependency_bindings=changed,
        )


async def _hold_projection_inputs(database):
    """Move actual local heaps and select their qualified names."""

    bindings_by_name = {}
    for name in _DEPENDENCY_NAMES:
        table_name = name.split(".")[1]
        await database.status(f"ALTER TABLE {name} SET SCHEMA held_input")
        bindings_by_name[name] = {"schema_name": "held_input", "table_name": table_name}
    return bindings_by_name


async def _assert_held_activation_and_readback(database, bindings, stage_oid):
    """Readiness uses all held heaps even while the canonical names are absent."""

    activation_sql = native._activate_geo_assurance_candidate_sql("mrf", dependency_bindings=bindings)
    assert await database.scalar(activation_sql) == projection.GEO_ASSURANCE_VERSION
    assert await database.scalar("SELECT active_dependency_bindings FROM mrf.entity_address_geo_assurance_state") == {
        name: {field: binding[field] for field in ("schema_name", "table_name")} for name, binding in bindings.items()
    }
    assert (
        await database.scalar(
            "SELECT " + projection.projection_state_available_sql("mrf", dependency_bindings=bindings)
        )
        is True
    )
    for binding in bindings.values():
        table = binding["table_name"]
        await database.status(f'ALTER TABLE held_input."{table}" RENAME TO "{table}_changed"')
        assert (
            await database.scalar(
                "SELECT " + projection.projection_state_available_sql("mrf", dependency_bindings=bindings)
            )
            is False
        )
        await database.status(f'ALTER TABLE held_input."{table}_changed" RENAME TO "{table}"')
        assert (
            await database.scalar(
                "SELECT " + projection.projection_state_available_sql("mrf", dependency_bindings=bindings)
            )
            is True
        )


def _binding_result(row):
    return SimpleNamespace(mappings=lambda: SimpleNamespace(one_or_none=lambda: row))


@pytest.mark.asyncio
@pytest.mark.parametrize("inherited", (False, True))
async def test_publication_without_custody_allows_only_canonical_noninherited_inputs(monkeypatch, inherited):
    session = SimpleNamespace(scalar=AsyncMock(return_value=inherited))
    events = []

    @asynccontextmanager
    async def transaction():
        events.append("locked")
        yield session
        events.append("released")

    session.begin = transaction
    database = SimpleNamespace(session_factory=transaction)
    selected = AsyncMock(return_value=None)
    monkeypatch.setattr(tiger_held_inputs, "selected_tiger_inventory", selected)
    if inherited:
        with pytest.raises(RuntimeError, match="requires protected captured inputs"):
            async with dependency_state.selected_publication_dependencies(database, "mrf"):
                pytest.fail("unrecorded inherited data reached publication")
    else:
        async with dependency_state.selected_publication_dependencies(database, "mrf") as bindings:
            assert bindings is None and events == ["locked", "locked"]
        assert events == ["locked", "locked", "released", "released"]
    selected.assert_awaited_once_with(session)


@pytest.mark.asyncio
@pytest.mark.parametrize("change", (None, "missing", "layout"))
async def test_publication_fences_missing_or_invalid_named_inputs(monkeypatch, change):
    bindings_by_name = {
        name: {"schema_name": "held_input" if name.startswith("tiger.") else "mrf", "table_name": name.split(".")[1]}
        for name in _DEPENDENCY_NAMES
    }

    async def execute(statement):
        if change == "missing" and str(statement).startswith("LOCK TABLE"):
            raise RuntimeError("entity-address publication dependency is missing")

    session = SimpleNamespace(execute=AsyncMock(side_effect=execute), scalar=AsyncMock(return_value=change != "layout"))

    @asynccontextmanager
    async def transaction():
        yield session

    session.begin = transaction
    inventory_by_field = {
        "relations": [
            {"relation_name": binding["table_name"], **binding}
            for name, binding in bindings_by_name.items()
            if name.startswith("tiger.")
        ]
    }
    monkeypatch.setattr(tiger_held_inputs, "selected_tiger_inventory", AsyncMock(return_value=inventory_by_field))
    monkeypatch.setattr(tiger_held_inputs, "selected_captured_inventories", AsyncMock(return_value={}))
    manager = dependency_state.selected_publication_dependencies(SimpleNamespace(session_factory=transaction), "mrf")
    if change is None:
        async with manager as observed:
            assert observed == bindings_by_name
            assert len(session.execute.await_args_list) == 2
    else:
        with pytest.raises(RuntimeError, match="publication dependency"):
            async with manager:
                pytest.fail("invalid named input reached publication")
    assert str(session.execute.await_args_list[0].args[0]) == projection.projection_dependency_lock_sql(
        "mrf", dependency_bindings=bindings_by_name
    )
    if change == "missing":
        session.scalar.assert_not_awaited()
    else:
        assert session.scalar.await_count == 1
        assert str(
            session.scalar.await_args.args[0]
        ) == "SELECT " + projection.projection_dependency_bindings_match_sql("mrf", bindings_by_name)


@pytest.mark.asyncio
@pytest.mark.parametrize("change", (None, "missing", "layout", "canonical"))
async def test_receiving_locks_saved_names_without_resolving_legacy_tokens(change):
    legacy_bindings = _example_bindings()
    original = deepcopy(legacy_bindings)

    async def execute(statement):
        if change == "missing":
            raise RuntimeError("entity-address active dependency binding changed")
        assert "pg_relation_filenode" not in str(statement)

    session = SimpleNamespace(
        scalar=AsyncMock(side_effect=[None] if change == "canonical" else [legacy_bindings, change != "layout"]),
        execute=AsyncMock(side_effect=execute),
    )
    if change in {"missing", "layout"}:
        with pytest.raises(RuntimeError, match="active dependency binding changed"):
            await dependency_state.lock_active_dependencies(session, "mrf", receiving=True)
    else:
        observed = await dependency_state.lock_active_dependencies(session, "mrf", receiving=True)
        assert observed == (None if change == "canonical" else original)
    assert legacy_bindings == original
    assert str(session.execute.await_args.args[0]) == projection.projection_dependency_lock_sql(
        "mrf", dependency_bindings=None if change == "canonical" else original
    )
    assert session.execute.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("location", ("held", "canonical", "declared", "missing", "layout", "none"))
async def test_prepared_bindings_use_only_declared_names_and_native_access(location):
    saved_bindings = _example_bindings()
    if location in {"canonical", "declared"}:
        for canonical_name, binding in saved_bindings.items():
            binding["schema_name"] = canonical_name.split(".")[0] if location == "canonical" else "declared_input"
    original = deepcopy(saved_bindings)

    async def execute(_statement):
        if location == "missing":
            raise RuntimeError("entity-address prepared dependency binding changed")

    session = SimpleNamespace(
        execute=AsyncMock(side_effect=execute), scalar=AsyncMock(return_value=location != "layout")
    )
    if location in {"missing", "layout"}:
        with pytest.raises(RuntimeError, match="prepared dependency binding changed"):
            await dependency_state.resolve_prepared_bindings(session, "mrf", saved_bindings)
    else:
        observed = await dependency_state.resolve_prepared_bindings(
            session, "mrf", None if location == "none" else saved_bindings
        )
        assert observed == (None if location == "none" else original)
        if location == "none":
            session.execute.assert_not_awaited()
            session.scalar.assert_not_awaited()
        else:
            assert str(session.execute.await_args.args[0]) == projection.projection_dependency_lock_sql(
                "mrf", dependency_bindings=original
            )
            assert str(
                session.scalar.await_args.args[0]
            ) == "SELECT " + projection.projection_dependency_bindings_match_sql("mrf", original)
    assert saved_bindings == original


async def _prepare_npi_geo_rotation(database, binding_mode, monkeypatch):
    from tests.test_npi_result_archive_postgres import _run_migration

    for statement in ("CREATE EXTENSION postgis", "CREATE SCHEMA tiger", "CREATE SCHEMA incoming_npi"):
        await database.status(statement)
    async with database.engine.begin() as connection:
        await _create_model_family(connection, "mrf")
        await _seed_source_rows(connection, "mrf", 0)
        await _run_migration(connection, "mrf")
        await _run_migration(connection, "incoming_npi")
    await _create_geo_dependencies(database, "mrf")
    await _create_alias_relations(database, "mrf", generation=0)
    await _seed_geo_dependencies(database, "mrf")
    await _prepare_rotation_npi_generations(database)
    bindings_by_name = None
    if binding_mode != "canonical_null":
        bindings_by_name = {
            name: dict(zip(("schema_name", "table_name"), name.split("."), strict=True)) for name in _DEPENDENCY_NAMES
        }
    if binding_mode == "held":
        await database.status("CREATE SCHEMA held_input")
        await database.status("CREATE TABLE held_input.npi_address (LIKE mrf.npi_address INCLUDING ALL)")
        await database.status("INSERT INTO held_input.npi_address SELECT * FROM mrf.npi_address")
        bindings_by_name["mrf.npi_address"]["schema_name"] = "held_input"
    monkeypatch.setattr(native, "db", database)
    projected, invalid, stage_oid, _forced = await native._project_geo_assurance_transaction(
        "mrf", "entity_address_unified", force=True, dependency_bindings=bindings_by_name
    )
    assert (projected, invalid) == (5, 0)
    assert (
        await database.scalar(native._activate_geo_assurance_candidate_sql("mrf", dependency_bindings=bindings_by_name))
        == projection.GEO_ASSURANCE_VERSION
    )
    assert (
        await database.scalar(
            native._record_geo_assurance_candidate_sql(
                "mrf", "entity_address_unified", projected, dependency_bindings=bindings_by_name
            )
        )
        == projection.GEO_ASSURANCE_VERSION
    )
    return stage_oid


async def _prepare_rotation_npi_generations(database):
    """Publish real ordinary generations for the two isolated rotation families."""
    from process import npi_result_archive as archive
    from process import npi_result_generation as generation
    from tests.test_npi_result_archive_postgres import _ensure_model_extensions

    async with database.engine.begin() as connection:
        await _ensure_model_extensions(connection)
        metadata = MetaData(schema="mrf")
        for model in archive.npi_archive_models():
            model.__table__.to_metadata(metadata, schema="mrf")
        await connection.run_sync(metadata.create_all)
        for name in generation.RELATION_NAMES:
            await connection.execute(text(f"CREATE TABLE incoming_npi.{name} (LIKE mrf.{name} INCLUDING ALL)"))
        driver = (await connection.get_raw_connection()).driver_connection
        for schema in ("mrf", "incoming_npi"):
            await generation.publish_unreceipted_npi_result_generation(driver, schema_name=schema)
        await connection.execute(text(f"DROP FUNCTION IF EXISTS incoming_npi.{generation.REVISION_FUNCTION}()"))


@asynccontextmanager
async def _npi_geo_actor(database, *, can_update=False):
    """Keep NPI ownership separate from a geo profile with only a SELECT grant."""
    from process.npi_result_generation import RELATION_NAMES

    role, password, engine, is_created = "npi_geo_" + uuid4().hex, uuid4().hex, None, False
    geo_owner, is_geo_created = "geo_profile_" + uuid4().hex, False
    try:
        async with database.engine.begin() as connection:
            await connection.execute(
                text(f"CREATE ROLE {role} LOGIN PASSWORD '{password}' NOSUPERUSER NOCREATEDB NOCREATEROLE")
            )
            is_created = True
            await connection.execute(text(f"CREATE ROLE {geo_owner} NOLOGIN NOSUPERUSER"))
            is_geo_created = True
            await connection.execute(text(f"ALTER TABLE mrf.entity_address_geo_assurance_state OWNER TO {geo_owner}"))
            name = database.engine.url.database
            await connection.execute(text(f'GRANT CREATE ON DATABASE "{name}" TO {role}'))
            await connection.execute(text(f"GRANT USAGE,CREATE ON SCHEMA mrf TO {role}"))
            await connection.execute(text(f"ALTER SCHEMA incoming_npi OWNER TO {role}"))
            for schema in ("mrf", "incoming_npi"):
                for name in (*RELATION_NAMES, "npi_result_generation"):
                    await connection.execute(text(f"ALTER TABLE {schema}.{name} OWNER TO {role}"))
            await connection.execute(text(f"GRANT SELECT ON mrf.entity_address_geo_assurance_state TO {role}"))
            if can_update:
                await connection.execute(
                    text(
                        "GRANT UPDATE(active_geo_assurance_version,candidate_geo_assurance_version) "
                        f"ON mrf.entity_address_geo_assurance_state TO {role}"
                    )
                )
        engine = create_async_engine(
            database.engine.url.set(username=role, password=password), pool_size=1, max_overflow=0
        )
        yield SimpleNamespace(
            sessions=async_sessionmaker(engine, expire_on_commit=False), role=role, geo_owner=geo_owner
        )
    finally:
        if engine is not None:
            await engine.dispose()
        if is_created:
            async with database.engine.begin() as connection:
                await connection.execute(text(f"DROP OWNED BY {role}"))
                await connection.execute(text(f"DROP ROLE {role}"))
        if is_geo_created:
            async with database.engine.begin() as connection:
                await connection.execute(text(f"DROP OWNED BY {geo_owner}"))
                await connection.execute(text(f"DROP ROLE {geo_owner}"))


def _named_canonical_signature():
    signature_by_name = {name: name.split(".", 1) for name in _DEPENDENCY_NAMES}
    signature_by_name["mrf.npi_result_generation"] = ["12345678-1234-5678-1234-567812345678", 3]
    return signature_by_name


@pytest.mark.parametrize(
    "token",
    [None, [], ["bad", 1], ["12345678-1234-5678-1234-567812345678", True], ["12345678-1234-5678-1234-567812345678", 0]],
)
def test_named_signature_rejects_malformed_logical_origin(token):
    signature = _named_canonical_signature()
    signature["mrf.npi_result_generation"] = token
    with pytest.raises(ValueError):
        projection.projection_relation_signature_tuple("mrf", signature)


def test_named_signature_keeps_origin_without_reinterpreting_it_as_a_dependency():
    from process import entity_address_snapshot_destination as destination

    signature = _named_canonical_signature()
    captured = projection.projection_named_relation_signature("mrf", signature)
    assert dict((name, [first, second]) for name, first, second in captured) == signature
    preparation = destination.EntityAddressGeoAssurancePreparation(
        None, 5, captured, stage_relation=("mrf", "address_stage")
    )
    assert destination._validated_geo_preparation(preparation.as_dict(), db_schema="mrf") == preparation
    legacy_by_name = {name: [index, index + 20] for index, name in enumerate(_DEPENDENCY_NAMES, 1)}
    assert len(projection.projection_named_relation_signature("mrf", legacy_by_name)) == 6
    assert all(
        name != "mrf.npi_result_generation"
        for name, _, _ in projection.projection_named_relation_signature("mrf", legacy_by_name)
    )


def test_held_signature_and_activation_do_not_query_canonical_generation():
    bindings_by_name = {
        name: {field: entry[field] for field in ("schema_name", "table_name")}
        for name, entry in _example_bindings().items()
    }
    for sql in (
        projection.projection_relation_signature_sql("mrf", dependency_bindings=bindings_by_name),
        projection.projection_state_available_sql("mrf", dependency_bindings=bindings_by_name),
        dependency_state.activate_geo_assurance_candidate_sql("mrf", dependency_bindings=bindings_by_name),
    ):
        assert "FROM mrf.npi_result_generation" not in sql
    signature_by_name = {name: [entry["schema_name"], entry["table_name"]] for name, entry in bindings_by_name.items()}
    signature_by_name["mrf.npi_result_generation"] = _named_canonical_signature()["mrf.npi_result_generation"]
    with pytest.raises(ValueError):
        projection.projection_relation_signature_tuple("mrf", signature_by_name)


def test_explicit_canonical_tiger_roots_lock_ordinary_children_recursively():
    bindings_by_name = {
        name: dict(zip(("schema_name", "table_name"), name.split("."), strict=True)) for name in _DEPENDENCY_NAMES
    }
    lock = projection.projection_named_dependency_lock_sql("mrf", bindings_by_name)
    assert 'ONLY "tiger"."zip_state"' not in lock and '"tiger"."zip_state"' in lock
    assert 'ONLY "mrf"."npi_address"' in lock
    checks = projection.projection_named_dependency_match_sql("mrf", bindings_by_name)
    assert "reference_family_archive_" in checks


@pytest.mark.asyncio
async def test_legacy_canonical_export_refuses_but_receive_observation_stays_replaceable():
    from process import entity_address_snapshot_serving as serving

    signature_by_name = {name: name.split(".", 1) for name in _DEPENDENCY_NAMES}
    state_by_field = {
        "singleton": True,
        "active_geo_assurance_version": 1,
        "active_relation_signature": signature_by_name,
        "active_dependency_bindings": None,
        "current_relation_signature": signature_by_name,
        "dependency_bindings_match": True,
        "npi_generation_match": False,
    }
    session = SimpleNamespace(
        scalar=AsyncMock(return_value=None),
        execute=AsyncMock(return_value=SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: [state_by_field]))),
    )
    with pytest.raises(RuntimeError, match="not active"):
        await serving._geo_assurance_state(session, schema_name="mrf", live_table_oid=None)
    state_by_field["npi_generation_match"] = True
    session.scalar.reset_mock()
    assert (
        await serving._geo_assurance_state(session, schema_name="mrf", live_table_oid=None, allow_unpublished_geo=True)
    )[0] == 1
    session.scalar.assert_not_awaited()
    assert "FROM mrf.npi_result_generation" not in str(session.execute.await_args.args[0])
