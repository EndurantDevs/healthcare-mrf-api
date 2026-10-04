# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed held-input bindings preserve canonical geo projection fences."""

import importlib
from contextlib import asynccontextmanager
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

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


@pytest.mark.parametrize(
    "change", ("missing", "extra", "sql", "schema_type", "boolean_oid", "zero_filenode", "extra_field", "duplicate")
)
def test_binding_rejects_nonclosed_or_nonphysical_identity(change):
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
        case "boolean_oid":
            target["relation_oid"] = True
        case "zero_filenode":
            target["relfilenode"] = 0
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
        assert physical in sql and physical in signature and "ONLY " + physical in locks
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
        for field in ("relation_oid", "relfilenode"):
            drifted = deepcopy(bindings)
            drifted["mrf.npi_address"][field] += 100000
            with pytest.raises(RuntimeError, match="dependency binding changed"):
                await native._project_geo_assurance_transaction(
                    "mrf", "entity_address_unified", force=True, dependency_bindings=drifted
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
            name: [binding["relation_oid"], binding["relfilenode"]] for name, binding in bindings.items()
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
        # A dependency group may relocate old heaps before its Address CAS.
        table = bindings["mrf.npi_address"]["table_name"]
        await database.status(f'ALTER TABLE held_input."{table}" RENAME TO "{table}_predecessor"')
        async with database.session_factory() as session, session.begin():
            received = await serving.capture_entity_address_receive_admission(session, schema_name="mrf")
            assert received == observed
        async with database.session_factory() as session, session.begin():
            with pytest.raises(Exception):
                await serving.capture_entity_address_observed_serving(session, schema_name="mrf")


@pytest.mark.asyncio
async def test_normal_finalizer_forwards_explicit_held_inputs_through_projection_and_cutover(monkeypatch):
    events = []
    _mock_shutdown_dependencies(monkeypatch, events)
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "mrf")
    bindings = _example_bindings()
    geometry = AsyncMock(return_value=0)
    monkeypatch.setattr(native, "_materialize_geo_assurance", geometry)
    await native.publish_entity_address_unified_generation(
        _shutdown_context(refresh_mode=native.ENTITY_ADDRESS_REFRESH_MODE_FULL),
        dependency_bindings=bindings,
    )
    assert geometry.await_args.kwargs["dependency_bindings"] == bindings
    assert ("publish", "cutover") in events

    @asynccontextmanager
    async def protected_selection(_database, schema):
        assert schema == "mrf"
        events.append(("custody", "locked"))
        yield bindings
        assert geometry.await_args.kwargs["dependency_bindings"] == bindings
        assert events[-1] != ("custody", "locked")
        events.append(("custody", "released"))

    monkeypatch.setattr(native, "selected_publication_dependencies", protected_selection)
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
        identity = await database.first(
            "SELECT oid::bigint,pg_relation_filenode(oid)::bigint FROM pg_catalog.pg_class "
            "WHERE oid=to_regclass(:name)",
            name="held_input." + table_name,
        )
        bindings_by_name[name] = {
            "schema_name": "held_input",
            "table_name": table_name,
            "relation_oid": identity[0],
            "relfilenode": identity[1],
        }
    return bindings_by_name


async def _assert_held_activation_and_readback(database, bindings, stage_oid):
    """Readiness uses all held heaps even while the canonical names are absent."""

    activation_sql = native._activate_geo_assurance_candidate_sql("mrf")
    assert await database.scalar(activation_sql) == stage_oid
    assert (
        await database.scalar("SELECT active_dependency_bindings FROM mrf.entity_address_geo_assurance_state")
        == bindings
    )
    assert await database.scalar("SELECT " + projection.projection_state_available_sql("mrf")) is True
    for binding in bindings.values():
        table = binding["table_name"]
        await database.status(f'ALTER TABLE held_input."{table}" RENAME TO "{table}_changed"')
        assert await database.scalar("SELECT " + projection.projection_state_available_sql("mrf")) is False
        await database.status(f'ALTER TABLE held_input."{table}_changed" RENAME TO "{table}"')
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
@pytest.mark.parametrize("change", (None, "missing", "physical"))
async def test_publication_fences_missing_or_stale_inputs(monkeypatch, change):
    bindings = _example_bindings()
    for name, binding in bindings.items():
        if not name.startswith("tiger."):
            binding["schema_name"] = "mrf"
    by_name = {f'"{binding["schema_name"]}"."{binding["table_name"]}"': binding for binding in bindings.values()}

    async def execute(_statement, parameters=None):
        if parameters is None:
            return None
        binding = by_name[parameters["name"]]
        row = (
            None
            if change == "missing" and binding["table_name"] == "geo_zip_lookup"
            else {field: binding[field] for field in ("relation_oid", "relfilenode")}
        )
        return _binding_result(row)

    session = SimpleNamespace(
        execute=AsyncMock(side_effect=execute), scalar=AsyncMock(return_value=change != "physical")
    )

    @asynccontextmanager
    async def transaction():
        yield session

    session.begin = transaction
    inventory_by_field = {
        "relations": [
            {"relation_name": name.split(".")[1], "schema_name": binding["schema_name"]}
            for name, binding in bindings.items()
            if name.startswith("tiger.")
        ]
    }
    monkeypatch.setattr(tiger_held_inputs, "selected_tiger_inventory", AsyncMock(return_value=inventory_by_field))
    manager = dependency_state.selected_publication_dependencies(SimpleNamespace(session_factory=transaction), "mrf")
    if change is None:
        async with manager as observed:
            assert observed == bindings
            assert len(session.execute.await_args_list) == 7
    else:
        with pytest.raises(RuntimeError, match="publication dependency"):
            async with manager:
                pytest.fail("invalid physical input reached publication")
    if change == "missing":
        session.scalar.assert_not_awaited()
    else:
        assert str(session.execute.await_args_list[-1].args[0]) == projection.projection_dependency_lock_sql(
            "mrf", dependency_bindings=bindings
        )
        assert str(
            session.scalar.await_args.args[0]
        ) == "SELECT " + projection.projection_dependency_bindings_match_sql("mrf", bindings)


@pytest.mark.asyncio
@pytest.mark.parametrize("change", (None, "missing", "physical", "canonical"))
async def test_receiving_locks_active_inputs_by_oid_after_predecessor_relocation(change):
    bindings = _example_bindings()
    by_oid = {binding["relation_oid"]: binding for binding in bindings.values()}

    async def execute(_statement, parameters=None):
        if parameters is None:
            return None
        binding = by_oid[parameters["oid"]]
        return _binding_result(
            None
            if change == "missing"
            else {
                "schema_name": "predecessor",
                "table_name": binding["table_name"],
            }
        )

    session = SimpleNamespace(
        scalar=AsyncMock(side_effect=[None] if change == "canonical" else [bindings, change != "physical"]),
        execute=AsyncMock(side_effect=execute),
    )
    if change in {"missing", "physical"}:
        with pytest.raises(RuntimeError, match="active dependency binding changed"):
            await dependency_state.lock_active_dependencies(session, "mrf", receiving=True)
    else:
        observed = await dependency_state.lock_active_dependencies(session, "mrf", receiving=True)
        if change == "canonical":
            assert observed is None
            assert str(session.execute.await_args.args[0]) == projection.projection_dependency_lock_sql("mrf")
        else:
            assert all(binding["schema_name"] == "predecessor" for binding in observed.values())
            assert {name: binding["relation_oid"] for name, binding in observed.items()} == {
                name: binding["relation_oid"] for name, binding in bindings.items()
            }
    if change == "missing":
        assert session.execute.await_count == 1
    elif change != "canonical":
        assert {tuple(call.args[1].items()) for call in session.execute.await_args_list[:6]} == {
            (("oid", binding["relation_oid"]), ("filenode", binding["relfilenode"])) for binding in bindings.values()
        }


@pytest.mark.asyncio
@pytest.mark.parametrize("location", ("held", "canonical", "unreviewed", "missing", "physical", "none"))
async def test_prepared_bindings_allow_only_same_identity_held_or_canonical_relocation(location):
    bindings = _example_bindings()
    before = deepcopy(bindings)
    identities = [
        _binding_result(
            None
            if location == "missing"
            else {
                "schema_name": name.split(".")[0]
                if location == "canonical"
                else "unreviewed"
                if location == "unreviewed"
                else binding["schema_name"],
                "table_name": binding["table_name"],
            }
        )
        for name, binding in sorted(bindings.items())
    ]
    session = SimpleNamespace(
        execute=AsyncMock(side_effect=identities + [None]), scalar=AsyncMock(return_value=location != "physical")
    )
    if location in {"unreviewed", "missing", "physical"}:
        with pytest.raises(RuntimeError, match="prepared dependency binding changed"):
            await dependency_state.resolve_prepared_bindings(session, "mrf", bindings)
    else:
        observed = await dependency_state.resolve_prepared_bindings(
            session, "mrf", None if location == "none" else bindings
        )
        if location == "none":
            assert observed is None
            session.execute.assert_not_awaited()
            session.scalar.assert_not_awaited()
        else:
            assert {name: binding["relation_oid"] for name, binding in observed.items()} == {
                name: binding["relation_oid"] for name, binding in before.items()
            }
            assert str(session.execute.await_args.args[0]) == projection.projection_dependency_lock_sql(
                "mrf", dependency_bindings=observed
            )
    assert bindings == before
