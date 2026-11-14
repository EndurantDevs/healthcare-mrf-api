# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed held-input bindings preserve canonical geo projection fences."""

import asyncio
import importlib
import json
from contextlib import asynccontextmanager
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from asyncpg import LockNotAvailableError
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


@pytest.mark.parametrize("change", ("missing", "extra", "sql", "schema_type", "extra_field", "duplicate"))
def test_binding_rejects_nonclosed_or_unsafe_names(change):
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
        assert f"'{name}', jsonb_build_object" in signature
        assert f"to_regclass('{name}')" not in signature
    copied = projection.validate_projection_dependency_bindings("mrf", bindings)
    bindings["mrf.npi_address"]["relation_oid"] = 999
    assert copied["mrf.npi_address"] == {"schema_name": "held_input", "table_name": "npi_address"}
    assert "held_input" not in native._activate_geo_assurance_candidate_sql("mrf")


def test_canonical_tiger_locks_include_children_and_held_leaves_use_only():
    bindings_by_name = {
        name: dict(zip(("schema_name", "table_name"), name.split("."), strict=True)) for name in _DEPENDENCY_NAMES
    }
    statement = projection.projection_dependency_lock_sql("mrf", dependency_bindings=bindings_by_name)
    assert statement == (
        'LOCK TABLE ONLY "mrf"."doctor_clinician_address", ONLY "mrf"."geo_zip_lookup", '
        'ONLY "mrf"."mrf_address", ONLY "mrf"."npi_address", "tiger"."zcta5", "tiger"."zip_state" '
        "IN ACCESS SHARE MODE;"
    )
    for name in ("tiger.zcta5", "tiger.zip_state"):
        bindings_by_name[name]["schema_name"] = "retained_inputs"
    held = projection.projection_dependency_lock_sql("mrf", dependency_bindings=bindings_by_name)
    assert 'ONLY "retained_inputs"."zcta5"' in held and 'ONLY "retained_inputs"."zip_state"' in held


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
        assert signature == bindings
        await _assert_legacy_geo_candidate(database, bindings, projected)
        await _assert_held_activation_and_readback(database, bindings, stage_oid)
        async with database.transaction():
            published = await native.result_generation.publish_local_entity_address_generation(
                database, schema_name="mrf"
            )
        serving = importlib.import_module("process.entity_address_snapshot_serving")
        async with database.session_factory() as session, session.begin():
            observed = await serving.capture_entity_address_observed_serving(session, schema_name="mrf")
        assert observed.result_generation == published.serving_generation
        assert observed.as_dict()["geo_assurance"]["dependencies"] == signature
        await _assert_geo_inverse_readiness(database, bindings, published.local_generation)


@pytest.mark.asyncio
async def test_normal_finalizer_forwards_explicit_held_inputs_through_projection_and_cutover(monkeypatch):
    events = []
    _mock_shutdown_dependencies(monkeypatch, events)
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "mrf")
    bindings = projection.validate_projection_dependency_bindings("mrf", _example_bindings())
    geometry = AsyncMock(return_value=0)
    monkeypatch.setattr(native, "_materialize_geo_assurance", geometry)
    await native.publish_entity_address_unified_generation(
        _shutdown_context(refresh_mode=native.ENTITY_ADDRESS_REFRESH_MODE_FULL),
        dependency_bindings=bindings,
    )
    assert geometry.await_args.kwargs["dependency_bindings"] == bindings
    assert ("publish", "cutover") in events

    @asynccontextmanager
    async def held_selection(_database, schema, *, control_context=None):
        assert schema == "mrf"
        assert control_context is None
        events.append(("custody", "locked"))
        yield bindings
        assert geometry.await_args.kwargs["dependency_bindings"] == bindings
        assert events[-1] != ("custody", "locked")
        events.append(("custody", "released"))

    monkeypatch.setattr(native, "selected_publication_dependencies", held_selection)
    await native.shutdown(_shutdown_context(refresh_mode=native.ENTITY_ADDRESS_REFRESH_MODE_FULL))
    assert events[-1] == ("custody", "released")
    changed = deepcopy(bindings)
    changed.pop("tiger.zcta5")
    with pytest.raises(ValueError):
        await native.publish_entity_address_unified_generation(
            _shutdown_context(refresh_mode=native.ENTITY_ADDRESS_REFRESH_MODE_FULL),
            dependency_bindings=changed,
        )


async def _hold_projection_inputs(database):
    """Move actual local heaps and observe their native OID and filenode identities."""

    bindings_by_name = {}
    for name in _DEPENDENCY_NAMES:
        table_name = name.split(".")[1]
        await database.status(f"ALTER TABLE {name} SET SCHEMA held_input")
        bindings_by_name[name] = {"schema_name": "held_input", "table_name": table_name}
    return bindings_by_name


async def _assert_held_activation_and_readback(database, bindings, stage_oid):
    """Readiness uses all held heaps even while the canonical names are absent."""

    activation_sql = native._activate_geo_assurance_candidate_sql("mrf")
    assert await database.scalar(activation_sql) == stage_oid
    active = await database.scalar("SELECT active_dependency_bindings FROM mrf.entity_address_geo_assurance_state")
    assert projection.validate_projection_dependency_bindings("mrf", active) == bindings
    assert await database.scalar("SELECT " + projection.projection_state_available_sql("mrf")) is True
    for binding in bindings.values():
        table = binding["table_name"]
        await database.status(f'ALTER TABLE held_input."{table}" RENAME TO "{table}_changed"')
        assert await database.scalar("SELECT " + projection.projection_state_available_sql("mrf")) is False
        await database.status(f'ALTER TABLE held_input."{table}_changed" RENAME TO "{table}"')
        assert await database.scalar("SELECT " + projection.projection_state_available_sql("mrf")) is True


async def _assert_legacy_geo_candidate(database, bindings, rows):
    """Recheck a native legacy candidate using named context while retaining its old metadata."""
    from process import entity_address_snapshot_destination as destination

    legacy_by_name = {}
    for canonical, binding in bindings.items():
        identity = await database.first(
            "SELECT to_regclass(:name)::oid::bigint,pg_relation_filenode(to_regclass(:name))::bigint",
            name=f"{binding['schema_name']}.{binding['table_name']}",
        )
        legacy_by_name[canonical] = {**binding, "relation_oid": identity[0], "relfilenode": identity[1]}
    signature_by_name = {name: [entry["relation_oid"], entry["relfilenode"]] for name, entry in legacy_by_name.items()}
    await database.status(
        "UPDATE mrf.entity_address_geo_assurance_state SET candidate_dependency_bindings=CAST(:bindings AS jsonb),"
        "candidate_relation_signature=CAST(:signature_by_name AS jsonb)",
        bindings=json.dumps(legacy_by_name),
        signature_by_name=json.dumps(signature_by_name),
    )
    async with database.transaction() as session:
        captured = await destination._capture_geo_preparation(
            session, db_schema="mrf", stage_table_oid=None, projected_rows=rows, dependency_bindings=bindings
        )
    assert captured.projected_rows == rows and captured.stage_table_oid is None
    assert (
        await database.scalar("SELECT candidate_relation_signature FROM mrf.entity_address_geo_assurance_state")
        == signature_by_name
    )


async def _assert_geo_inverse_readiness(database, bindings, generation):
    """Restore a real projected predecessor while leaving legacy physical metadata alone."""
    from process import provider_directory_serving_recovery as recovery

    version = await database.scalar("SELECT active_geo_assurance_version FROM mrf.entity_address_geo_assurance_state")
    baseline_by_field = {"metadata": {"geo": {"version": version}}}
    row_count = await database.scalar("SELECT count(*) FROM mrf.entity_address_unified")
    await database.status("CREATE TABLE mrf.geo_inverse_next (LIKE mrf.entity_address_unified INCLUDING ALL)")
    await database.status("INSERT INTO mrf.geo_inverse_next SELECT * FROM mrf.entity_address_unified")
    forward_oid = await database.scalar(
        native._record_geo_assurance_candidate_sql("mrf", "geo_inverse_next", row_count, dependency_bindings=bindings)
    )
    async with database.transaction():
        await database.status("ALTER TABLE mrf.entity_address_unified RENAME TO geo_inverse_previous")
        await database.status("ALTER TABLE mrf.geo_inverse_next RENAME TO entity_address_unified")
        assert await database.scalar(native._activate_geo_assurance_candidate_sql("mrf")) == forward_oid
        await native.result_generation.publish_local_entity_address_generation(database, schema_name="mrf")
    assert await database.scalar("SELECT " + projection.projection_state_available_sql("mrf")) is True
    async with database.transaction():
        await database.status("ALTER TABLE mrf.entity_address_unified RENAME TO geo_inverse_forward")
        await database.status("ALTER TABLE mrf.geo_inverse_previous RENAME TO entity_address_unified")
        await recovery._restore_geo(database, "mrf", baseline_by_field)
        restored = await native.result_generation.publish_local_entity_address_generation(database, schema_name="mrf")
    assert restored.local_generation == generation + 2
    assert await database.scalar("SELECT count(*) FROM mrf.entity_address_unified") == row_count
    assert await database.scalar("SELECT generation FROM mrf.address_alias_state_v1") == 0
    assert (
        await database.scalar("SELECT active_table_oid::bigint FROM mrf.entity_address_geo_assurance_state")
        == forward_oid
    )
    assert await database.scalar("SELECT " + projection.projection_state_available_sql("mrf")) is True


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
@pytest.mark.parametrize("accepted", (True, False))
async def test_publication_locks_and_validates_named_inputs(monkeypatch, accepted):
    bindings = projection.validate_projection_dependency_bindings("mrf", _example_bindings())
    for name, binding in bindings.items():
        if not name.startswith("tiger."):
            binding["schema_name"] = "mrf"
    session = SimpleNamespace(execute=AsyncMock(), scalar=AsyncMock(return_value=accepted))

    @asynccontextmanager
    async def transaction():
        yield session

    session.begin = transaction
    inventory_by_field = {
        "relations": [
            {"relation_name": name.split(".")[1], **binding}
            for name, binding in bindings.items()
            if name.startswith("tiger.")
        ]
    }
    monkeypatch.setattr(tiger_held_inputs, "selected_tiger_inventory", AsyncMock(return_value=inventory_by_field))
    manager = dependency_state.selected_publication_dependencies(SimpleNamespace(session_factory=transaction), "mrf")
    if accepted:
        async with manager as observed:
            assert observed == bindings
    else:
        with pytest.raises(RuntimeError, match="publication dependency changed"):
            async with manager:
                pytest.fail("unsupported input layout reached publication")
    assert str(session.execute.await_args_list[0].args[0]) == projection.projection_dependency_lock_sql(
        "mrf", dependency_bindings=bindings
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("receiving", (False, True))
@pytest.mark.parametrize("accepted", (True, False))
async def test_active_inputs_lock_named_relations_without_physical_relocation(receiving, accepted):
    original = _example_bindings()
    bindings = projection.validate_projection_dependency_bindings("mrf", original)
    session = SimpleNamespace(scalar=AsyncMock(side_effect=[original, accepted]), execute=AsyncMock())
    if accepted:
        assert await dependency_state.lock_active_dependencies(session, "mrf", receiving=receiving) == bindings
    else:
        with pytest.raises(RuntimeError, match="active dependency binding changed"):
            await dependency_state.lock_active_dependencies(session, "mrf", receiving=receiving)
    assert session.execute.await_count == 1
    assert str(session.execute.await_args.args[0]) == projection.projection_dependency_lock_sql(
        "mrf", dependency_bindings=bindings
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("accepted", (True, False))
async def test_prepared_bindings_preserve_the_explicit_named_selection(accepted):
    original = _example_bindings()
    before = deepcopy(original)
    session = SimpleNamespace(execute=AsyncMock(), scalar=AsyncMock(return_value=accepted))
    if accepted:
        assert await dependency_state.resolve_prepared_bindings(session, "mrf", original) == (
            projection.validate_projection_dependency_bindings("mrf", original)
        )
    else:
        with pytest.raises(RuntimeError, match="prepared dependency binding changed"):
            await dependency_state.resolve_prepared_bindings(session, "mrf", original)
    assert original == before
    assert session.execute.await_count == 1


def test_legacy_physical_binding_fields_are_discarded():
    original = _example_bindings()
    original["mrf.npi_address"].update(relation_oid=True, relfilenode=0)
    normalized = projection.validate_projection_dependency_bindings("mrf", original)
    assert all(set(binding) == {"schema_name", "table_name"} for binding in normalized.values())
    assert "pg_relation_filenode" not in projection.projection_dependency_bindings_match_sql("mrf", original)
    assert "pg_roles" not in projection.projection_state_available_sql("mrf")
    assert "active_table_oid" not in projection.projection_state_available_sql("mrf")


def _canonical_geo_signature():
    values_by_name = {
        name: dict(zip(("schema_name", "table_name"), name.split("."), strict=True)) for name in _DEPENDENCY_NAMES
    }
    values_by_name["mrf.npi_address"]["generation"] = {
        "origin_lineage_id": "12345678-1234-5678-1234-567812345678",
        "origin_generation": 3,
    }
    return values_by_name


@pytest.mark.parametrize(
    "token",
    (
        None,
        {},
        {"origin_lineage_id": "bad", "origin_generation": 1},
        {"origin_lineage_id": "12345678-1234-5678-1234-567812345678", "origin_generation": True},
    ),
)
def test_canonical_geo_signature_rejects_missing_or_malformed_generation(token):
    signature = _canonical_geo_signature()
    signature["mrf.npi_address"]["generation"] = token
    with pytest.raises(ValueError):
        projection.validate_projection_relation_signature("mrf", signature)


def test_geo_generation_is_portable_and_preparation_keeps_its_original_token():
    from process import entity_address_snapshot_destination as destination

    signature = _canonical_geo_signature()
    assert projection.validate_projection_relation_signature("mrf", signature) == signature
    receipt = destination.EntityAddressGeoAssurancePreparation(
        None,
        5,
        destination._geo_signature(signature, db_schema="mrf"),
        npi_generation=(signature["mrf.npi_address"]["generation"]["origin_lineage_id"], 3),
    )
    assert destination._validated_geo_preparation(receipt.as_dict(), db_schema="mrf") == receipt
    assert "local_generation" not in projection.projection_relation_signature_sql("mrf")
    assert "npi_result_generation" not in projection.projection_relation_signature_sql(
        "mrf", dependency_bindings=_example_bindings()
    )
    for phase in ("active", "candidate"):
        assert f"{phase}_relation_signature" in projection.projection_npi_generation_match_sql("mrf", phase)


@pytest.mark.asyncio
async def test_preparation_rejects_a_changed_logical_npi_generation():
    from process import entity_address_snapshot_destination as destination
    from tests.test_entity_address_snapshot_validation_guards import _mapped_result

    signature = _canonical_geo_signature()
    current_signature = deepcopy(signature)
    current_signature["mrf.npi_address"]["generation"]["origin_generation"] += 1
    state_by_field = {
        "candidate_geo_assurance_version": 1,
        "candidate_projected_rows": 5,
        "candidate_relation_signature": signature,
        "candidate_dependency_bindings": None,
        "current_signature": current_signature,
        "bindings_match": True,
    }
    session = SimpleNamespace(execute=AsyncMock(return_value=_mapped_result(row=state_by_field)))
    with pytest.raises(destination.EntityAddressSnapshotDestinationError, match="candidate is stale"):
        await destination._capture_geo_preparation(session, db_schema="mrf", stage_table_oid=None, projected_rows=5)


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
    assert await database.scalar(native._activate_geo_assurance_candidate_sql("mrf")) == stage_oid
    assert (
        await database.scalar(
            native._record_geo_assurance_candidate_sql(
                "mrf", "entity_address_unified", projected, dependency_bindings=bindings_by_name
            )
        )
        == stage_oid
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
        await connection.execute(text(f"DROP FUNCTION incoming_npi.{generation.REVISION_FUNCTION}()"))


async def _rotate_npi_geo_source(session, rotation):
    from process import npi_result_archive
    from process import npi_result_generation as generation

    npi = importlib.import_module("process.npi")
    connection = await session.connection()
    driver = (await connection.get_raw_connection()).driver_connection
    if rotation == "ordinary":
        await session.execute(text("ALTER TABLE incoming_npi.npi_address RENAME TO npi_address_incoming"))
        await session.execute(text("ALTER TABLE incoming_npi.npi_address_incoming SET SCHEMA mrf"))
        await npi._rotate_npi_canonical_table(
            driver, schema="mrf", live_table="npi_address", stage_table="npi_address_incoming", index_suffixes=()
        )
        await generation.publish_unreceipted_npi_result_generation(driver, schema_name="mrf")
        return
    source_generation = await generation.capture_npi_serving_generation(session, schema_name="incoming_npi")
    assert source_generation.serving_generation is not None
    await session.execute(text("DROP TABLE incoming_npi.npi_result_generation"))
    owner = SimpleNamespace(
        dataset_id=uuid4(),
        schema_name="incoming_npi",
        schema_oid=await session.scalar(text("SELECT oid FROM pg_catalog.pg_namespace WHERE nspname='incoming_npi'")),
        relation_oids=await npi_result_archive._relation_pairs(session, "incoming_npi", generation.RELATION_NAMES),
    )
    incumbent = npi_result_archive.NpiIncumbent(
        "mrf", await npi_result_archive._relation_pairs(session, "mrf", generation.RELATION_NAMES)
    )
    await npi_result_archive._rotate_relations(session, owner, incumbent)
    await generation.publish_adopted_npi_result_generation(
        session, schema_name="mrf", source_generation=source_generation.serving_generation, canonical_provenance=None
    )


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


async def _assert_busy_geo_state_independence(database, rotation, actor):
    """An unrelated geo writer cannot block publication of the NPI-owned family."""
    for table_lock in (False, True):
        async with database.session_factory() as blocker, blocker.begin():
            statement = (
                "LOCK TABLE mrf.entity_address_geo_assurance_state IN SHARE ROW EXCLUSIVE MODE"
                if table_lock
                else "SELECT singleton FROM mrf.entity_address_geo_assurance_state FOR UPDATE"
            )
            await blocker.execute(text(statement))
            async with actor.sessions() as session, session.begin(), asyncio.timeout(2):
                await session.execute(text("SET LOCAL lock_timeout='725ms'"))
                connection = await session.connection()
                driver = (await connection.get_raw_connection()).driver_connection
                await _rotate_npi_geo_source(session, rotation)
                assert await driver.fetchval("SHOW lock_timeout") == "725ms"
                assert driver.is_in_transaction()
                await session.rollback()
        assert await database.scalar("SELECT count(*) FROM mrf.npi_address") == 2


async def _rotate_npi_with_timeout(session, rotation):
    await session.execute(text("SET LOCAL lock_timeout='725ms'"))
    connection = await session.connection()
    driver = (await connection.get_raw_connection()).driver_connection
    assert await driver.fetchval("SELECT rolsuper FROM pg_catalog.pg_roles WHERE rolname=current_user") is False
    await _rotate_npi_geo_source(session, rotation)
    assert await driver.fetchval("SHOW lock_timeout") == "725ms"


async def _assert_pinned_geo_reader(database, actor, readiness, rotation):
    async with database.engine.connect() as reader:
        reader = await reader.execution_options(isolation_level="REPEATABLE READ")
        async with reader.begin():
            assert await reader.scalar(readiness) is True
            async with actor.sessions() as session, session.begin():
                await _rotate_npi_with_timeout(session, rotation)
            assert await reader.scalar(readiness) is True
            assert (
                await reader.scalar(
                    text("SELECT geo_evidence_source_id FROM mrf.entity_address_unified WHERE location_key='nppes'")
                )
                == projection.GEO_EVIDENCE_NPPES
            )
