# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native receipt and consumption CHECKs preserve signed rows and immutable history."""

import asyncio
import json
from contextlib import asynccontextmanager
from copy import deepcopy

import pytest
import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy.exc import DBAPIError

from process import provider_directory_cms_capacity_contract as contract
from tests import test_cms_capacity_preflight_receipt_migration as receipts
from tests.provider_directory_profile_capacity_v2_migration_support import load_capacity_v2_migration
from tests.provider_directory_serving_capacity_signing_test_support import serving_case, verify_serving_envelope
from tests.test_provider_directory_capacity_reservation_snapshot import _preflight
from tests.test_provider_directory_ordinary_address_capacity_migration import load_migration, previous
from tests.test_provider_directory_profile_capacity_attestation_postgres import _consumption_values
from tests.test_provider_directory_profile_initial_migration import _migration as historical_migration
from tests.test_provider_directory_serving_capacity_snapshot import _consumption


@pytest.fixture(autouse=True)
def configured_node(monkeypatch):
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "dev-node")


def _bootstrap(connection, schema):
    """Install only the existing receipt/consumption predecessor dependency slice."""
    with Operations.context(MigrationContext.configure(connection)):
        historical_migration(
            "20260730110000_provider_directory_profile_delta.py"
        )._create_capacity_lease_consumption_table(schema)
        load_capacity_v2_migration().upgrade()
        original = previous._ORIGINAL
        original._replace_capacity_constraint(
            schema,
            expected_contracts=(original._V1_CONTRACT, original._V2_CONTRACT),
            replacement_contracts=(original._V1_CONTRACT, original._V2_CONTRACT, original._V3_CONTRACT),
        )
        historical_migration("20260930110000_cms_npd_nonprofile_capacity.py").upgrade()
        previous._PREVIOUS.upgrade()
        previous._PREVIOUS._replace_check(schema, previous._initial_preflight_check())


@asynccontextmanager
async def _ledger(monkeypatch):
    async with receipts._ledger(monkeypatch) as (engine, schema):
        async with engine.begin() as connection:
            await connection.run_sync(_bootstrap, schema)
        yield engine, schema


async def _migrate(engine, direction):
    def apply(connection):
        with Operations.context(MigrationContext.configure(connection)):
            getattr(load_migration(), direction)()

    async with engine.begin() as connection:
        await connection.run_sync(apply)


async def _insert_consumption(engine, schema, row):
    table = previous._qt(schema, load_migration()._TABLE)
    statement = f"INSERT INTO {table} ({','.join(row)}) VALUES ({','.join(':' + field for field in row)})"
    async with engine.begin() as connection:
        await connection.execute(sa.text(statement), row)


async def _consumption_state(engine, schema):
    """Retain complete consumption bytes, native keys/indexes, and existing guard definitions."""
    table = previous._qt(schema, load_migration()._TABLE)
    async with engine.connect() as connection:
        consumed_rows = (
            (await connection.execute(sa.text(f"SELECT to_jsonb(entry) FROM {table} entry ORDER BY attestation_id")))
            .scalars()
            .all()
        )
        keys = (
            await connection.execute(
                sa.text("""SELECT oid,conname,pg_get_constraintdef(oid)
            FROM pg_constraint WHERE conrelid=to_regclass(:table) AND contype IN ('p','u') ORDER BY conname"""),
                {"table": table},
            )
        ).all()
        guards = (
            await connection.execute(
                sa.text("""SELECT oid,tgname,tgenabled::text,
            pg_get_triggerdef(oid),pg_get_functiondef(tgfoid)
            FROM pg_trigger WHERE tgrelid=to_regclass(:table) AND NOT tgisinternal ORDER BY tgname"""),
                {"table": table},
            )
        ).all()
        indexes = (
            await connection.execute(
                sa.text("""SELECT indexrelid,pg_get_indexdef(indexrelid)
            FROM pg_index WHERE indrelid=to_regclass(:table) ORDER BY indexrelid"""),
                {"table": table},
            )
        ).all()
    return consumed_rows, keys, guards, indexes


async def _history(engine, schema):
    return await receipts._snapshot(engine, schema), await _consumption_state(engine, schema)


async def _checks(engine, schema):
    migration = load_migration()
    async with engine.connect() as connection:
        return (
            await connection.execute(
                sa.text("""SELECT conrelid,conname,pg_get_constraintdef(oid),convalidated
            FROM pg_constraint WHERE connamespace=to_regnamespace(:schema)
            AND conname IN (:receipt,:consumption) ORDER BY conname"""),
                {
                    "schema": schema,
                    "receipt": migration._PREFLIGHT_CHECK,
                    "consumption": migration._CHECK,
                },
            )
        ).all()


async def _seed_history(engine, schema):
    for version in (3, 4):
        await receipts._insert(engine, schema, receipts._row("retained-" + str(version), version))
    await receipts._insert(engine, schema, _preflight(serving_case(initial_mode=True).pair))
    await _insert_consumption(engine, schema, _consumption_values())


@pytest.mark.parametrize("initial_mode", [False, True], ids=["delta", "initial"])
def test_signed_rows_keep_exact_pair_versions_and_mode(initial_mode):
    case = serving_case(initial_mode=initial_mode)
    lease = verify_serving_envelope(case.envelope)
    row = _preflight(case.envelope)
    assert row["contract_id"] == load_migration()._RECEIPT
    assert row["request_contract_id"] == load_migration()._REQUEST
    assert row["materialization_mode"] == ("full_swap" if initial_mode else "source_delta")
    assert contract.read_capacity_preflight_receipt(row, lease) == case.guard["healthcare_receipt"]
    assert _consumption(case)["admission_purpose"] == "serving_nonprofile"


@pytest.mark.parametrize("initial_mode", [False, True], ids=["delta", "initial"])
def test_native_signed_ordinary_receipt_and_consumption_roundtrip(monkeypatch, initial_mode):
    async def exercise():
        case = serving_case(initial_mode=initial_mode)
        receipt_values, consumed = _preflight(case.envelope), _consumption(case)
        async with _ledger(monkeypatch) as (engine, schema):
            await _seed_history(engine, schema)
            before = await _history(engine, schema)
            with pytest.raises(DBAPIError, match=load_migration()._PREFLIGHT_CHECK):
                await receipts._insert(engine, schema, receipt_values)
            with pytest.raises(DBAPIError, match=load_migration()._CHECK):
                await _insert_consumption(engine, schema, consumed)
            await _migrate(engine, "upgrade")
            assert await _history(engine, schema) == before
            await receipts._insert(engine, schema, receipt_values)
            await _insert_consumption(engine, schema, consumed)
            after = await _history(engine, schema)
            assert len(after[0][0]) == 4 and len(after[1][0]) == 2
            assert after[0][1:] == before[0][1:] and after[1][1:] == before[1][1:]
            async with engine.connect() as connection:
                stored = (
                    (
                        await connection.execute(
                            sa.text(
                                f"SELECT * FROM {previous._qt(schema, previous._ORIGINAL._TABLE)} WHERE receipt_sha256=:digest"
                            ),
                            {"digest": receipt_values["receipt_sha256"]},
                        )
                    )
                    .mappings()
                    .one()
                )
            assert isinstance(stored["receipt_json"], dict)
            assert (
                contract.read_capacity_preflight_receipt(stored, verify_serving_envelope(case.envelope))
                == case.guard["healthcare_receipt"]
            )
            with pytest.raises(DBAPIError):
                await _insert_consumption(engine, schema, {**consumed, "attestation_id": "ab" * 32})
            with pytest.raises(DBAPIError, match="consumption_immutable"):
                async with engine.begin() as connection:
                    await connection.execute(sa.text(f"DELETE FROM {previous._qt(schema, load_migration()._TABLE)}"))
            assert await _history(engine, schema) == after

    asyncio.run(exercise())


def _changed_receipt(row, change):
    """Mutate one stored boundary after producing a genuinely signed valid receipt."""
    row = deepcopy(row)
    payload = json.loads(row["receipt_json"])
    geometry = payload["capacity_geometry"]
    match change:
        case "contract_id" | "request_contract_id":
            row[change] = row[change].replace(".v6", ".v4")
        case "json_contract" | "json_request":
            field = "contract_id" if change == "json_contract" else "request_contract_id"
            payload[field] = payload[field].replace(".v6", ".v4")
        case "purpose":
            geometry["admission_purpose"] = "cms_nonprofile"
        case "scope":
            geometry["ordinary_scope"]["contract_id"] = "unknown"
        case "missing_scope":
            geometry.pop("ordinary_scope")
        case "input":
            geometry["ordinary_scope"]["input_sha256"] = "invalid"
        case "retention":
            geometry["ordinary_scope"]["retention_kind"] = "snapshot"
        case "mode":
            row["materialization_mode"] = "full_swap"
        case "initial_target":
            payload.pop("serving_generation_preflight")
        case _:
            raise AssertionError("unknown receipt mutation")
    row["receipt_json"] = json.dumps(payload)
    return row


@pytest.mark.parametrize(
    "change",
    [
        "contract_id",
        "request_contract_id",
        "json_contract",
        "json_request",
        "purpose",
        "scope",
        "missing_scope",
        "input",
        "retention",
        "mode",
        "initial_target",
    ],
)
def test_native_ordinary_receipt_rejects_crossed_or_incomplete_identity(monkeypatch, change):
    async def exercise():
        row = _changed_receipt(_preflight(serving_case(initial_mode=change == "initial_target").envelope), change)
        async with _ledger(monkeypatch) as (engine, schema):
            await _migrate(engine, "upgrade")
            before = await _history(engine, schema)
            with pytest.raises(DBAPIError, match=load_migration()._PREFLIGHT_CHECK):
                await receipts._insert(engine, schema, row)
            assert await _history(engine, schema) == before

    asyncio.run(exercise())


@pytest.mark.parametrize("history_kind", [None, "receipt", "consumption"])
def test_native_downgrade_preserves_old_history_and_refuses_ordinary_history(monkeypatch, history_kind):
    async def exercise():
        case = serving_case()
        async with _ledger(monkeypatch) as (engine, schema):
            await _seed_history(engine, schema)
            old_checks = await _checks(engine, schema)
            await _migrate(engine, "upgrade")
            if history_kind == "receipt":
                await receipts._insert(engine, schema, _preflight(case.envelope))
            elif history_kind == "consumption":
                await _insert_consumption(engine, schema, _consumption(case))
            before, checks = await _history(engine, schema), await _checks(engine, schema)
            if history_kind is None:
                await _migrate(engine, "downgrade")
                assert await _checks(engine, schema) == old_checks
                with pytest.raises(DBAPIError, match=load_migration()._PREFLIGHT_CHECK):
                    await receipts._insert(engine, schema, _preflight(case.envelope))
            else:
                with pytest.raises(RuntimeError, match="history_requires_retention"):
                    await _migrate(engine, "downgrade")
                assert await _checks(engine, schema) == checks
            assert await _history(engine, schema) == before

    asyncio.run(exercise())


@pytest.mark.parametrize("drift", ["predicate", "unvalidated", "unlogged"])
def test_native_upgrade_refuses_changed_receipt_storage_or_check(monkeypatch, drift):
    async def exercise():
        async with _ledger(monkeypatch) as (engine, schema):
            migration = load_migration()
            table = previous._qt(schema, migration._PREFLIGHT_TABLE)
            async with engine.begin() as connection:
                if drift == "unlogged":
                    await connection.execute(sa.text(f"ALTER TABLE {table} SET UNLOGGED"))
                else:
                    condition = "TRUE" if drift == "predicate" else previous._initial_preflight_check()
                    suffix = " NOT VALID" if drift == "unvalidated" else ""
                    await connection.execute(
                        sa.text(f"ALTER TABLE {table} DROP CONSTRAINT {migration._PREFLIGHT_CHECK}")
                    )
                    await connection.execute(
                        sa.text(
                            f"ALTER TABLE {table} ADD CONSTRAINT {migration._PREFLIGHT_CHECK} CHECK ({condition}){suffix}"
                        )
                    )
            before, checks = await _history(engine, schema), await _checks(engine, schema)
            with pytest.raises(RuntimeError, match="constraint_drift"):
                await _migrate(engine, "upgrade")
            assert await _history(engine, schema) == before
            assert await _checks(engine, schema) == checks

    asyncio.run(exercise())
