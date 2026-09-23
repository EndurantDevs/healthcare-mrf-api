# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed held-input bindings preserve canonical geo projection fences."""

import importlib
from copy import deepcopy

import pytest

from api import ptg2_geo_projection as projection
from tests.test_entity_address_snapshot_destination import (
    _create_geo_dependencies,
    _owned_native_database,
    _seed_geo_dependencies,
    _seed_source_rows,
)
from tests.test_entity_address_snapshot_stage_postgres import _create_model_family, _native_test_connection

native = importlib.import_module("process.entity_address_unified")

_DEPENDENCY_NAMES = (
    "mrf.npi_address",
    "mrf.mrf_address",
    "mrf.doctor_clinician_address",
    "mrf.geo_zip_lookup",
    "tiger.zip_state",
    "tiger.zcta5",
)


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
async def test_native_projection_uses_held_inputs_until_all_canonical_swaps(monkeypatch):
    """Project real rows, reject identity drift, and retain the final canonical CAS."""

    async_dsn, _environment = _native_test_connection()
    async with _owned_native_database(async_dsn, monkeypatch) as database:
        for statement in ("CREATE EXTENSION postgis", "CREATE SCHEMA tiger", "CREATE SCHEMA held_input"):
            await database.status(statement)
        async with database.engine.begin() as connection:
            await _create_model_family(connection, "mrf")
            await _seed_source_rows(connection, "mrf", 0)
        await _create_geo_dependencies(database, "mrf")
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
        await _assert_canonical_activation_waits(database, bindings, stage_oid)


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


async def _assert_canonical_activation_waits(database, bindings, stage_oid):
    """Canonical activation rejects absent or partially swapped dependencies."""

    activation_sql = native._activate_geo_assurance_candidate_sql("mrf")
    for name, binding in bindings.items():
        assert await database.scalar(activation_sql) is None
        await database.status(f'ALTER TABLE held_input."{binding["table_name"]}" SET SCHEMA {name.split(".")[0]}')
    assert await database.scalar(activation_sql) == stage_oid
    assert await database.scalar("SELECT " + projection.projection_state_available_sql("mrf")) is True
