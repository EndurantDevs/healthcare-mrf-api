# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native history/catalog guard proof; full signed common recovery is tested separately."""

import asyncio
import hashlib
import importlib
import json
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy.exc import DBAPIError

from process import provider_directory_profile_initial as initial_runtime
from process import provider_directory_profile_initial_contract as contract
from process import provider_directory_profile_initial_custody as custody
from process import provider_directory_profile_initial_guards as guards
from process import provider_directory_profile_initial_history as history
from process.provider_directory_profile_initial_recovery import recovered_target_state
from tests import test_provider_directory_profile_initial_migration as base
from tests.test_provider_directory_profile_initial import _target
from tests.test_provider_directory_profile_initial_recovery import _witness


async def _apply(engine, operation):
    async with engine.begin() as connection:

        def apply(sync):
            with Operations.context(MigrationContext.configure(sync)):
                operation()

        await connection.run_sync(apply)


async def _catalog(connection, table):
    fhir = importlib.import_module("process.provider_directory_fhir")
    database = base._CatalogDatabase(connection)
    oid = await connection.scalar(sa.text("SELECT to_regclass(:ref)::oid::bigint"), {"ref": f"{base._SCHEMA}.{table}"})
    relation_by_field = dict((await database.all(fhir._PROFILE_CAPACITY_RELATION_SQL, relation_oid=oid))[0]._mapping)
    relation_oids = [oid] + ([relation_by_field["toast_oid"]] if relation_by_field["toast_oid"] else [])
    catalogs = [
        [dict(row._mapping) for row in await database.all(query, relation_oids=relation_oids)]
        for query in (
            fhir._PROFILE_CAPACITY_ATTRIBUTE_SQL,
            fhir._PROFILE_CAPACITY_INDEX_SQL,
            fhir._PROFILE_CAPACITY_CONSTRAINT_SQL,
            fhir._PROFILE_CAPACITY_TRIGGER_SQL,
        )
    ]
    attributes, indexes, constraints, triggers = catalogs
    if table == guards.RECEIPT_TABLE:
        checks = {item["constraint_definition"] for item in constraints if item["constraint_type"] == "c"}
        if any("to_hex" in item for item in checks):
            assert checks == (guards._RECEIPT_CHECKS - {guards._LEGACY_KEY_CHECK}) | {guards._REACTIVATED_KEY_CHECK}
        guards.assert_initial_receipt_catalog(oid, attributes, indexes, constraints)
        await guards.assert_initial_receipt_guards(database, relation_by_field, triggers)
    payload, _structure = fhir._profile_capacity_fingerprint_payloads(relation_by_field, *catalogs, oid)
    if table != history._CONSUMPTION:
        payload["guard_dependencies"] = await guards.initial_guard_dependencies(database, base._SCHEMA)
    return payload


def _replace_fixture(value, replacements):
    if isinstance(value, dict):
        return {key: _replace_fixture(nested, replacements) for key, nested in value.items()}
    if isinstance(value, list):
        return [_replace_fixture(nested, replacements) for nested in value]
    if isinstance(value, str):
        for old, new in replacements.items():
            value = value.replace(old, new)
    return value


async def _fresh_values(engine, generation, *, recovered):
    consumed, preflight, serving, receipt = await base._publication_values(engine)
    digest = hashlib.sha256(str(generation).encode()).hexdigest()
    replacement_by_value = {
        consumed["build_id"]: "pdpb_" + digest[:32],
        consumed["run_id"]: "run_" + digest[:32],
        consumed["attestation_id"]: digest,
        consumed["lease_digest"]: hashlib.sha256(digest.encode()).hexdigest(),
        consumed["reservation_id"]: "native-initial-reservation-" + digest[:16],
        preflight["receipt_sha256"]: hashlib.sha256((digest + "preflight").encode()).hexdigest(),
        preflight["request_nonce"]: hashlib.sha256((digest + "nonce").encode()).hexdigest(),
        preflight["request_sha256"]: hashlib.sha256((digest + "request").encode()).hexdigest(),
        serving["generation_id"]: "pdprofile_" + digest[:32],
    }
    updated_by_name = _replace_fixture(
        dict(consumed=consumed, preflight=preflight, serving=serving, receipt=receipt), replacement_by_value
    )
    consumed, preflight = updated_by_name["consumed"], updated_by_name["preflight"]
    serving, receipt = updated_by_name["serving"], updated_by_name["receipt"]
    initial_targets = _target()
    if recovered:
        initial_targets = recovered_target_state(initial_targets, _witness(initial_targets))
    target_hash = contract.target_state_sha256(initial_targets)
    serving["control_generation"] = generation
    preflight["control_generation"] = generation
    serving["capacity_geometry_json"]["initial_target_state_sha256"] = target_hash
    receipt["serving"]["control_generation"] = generation
    receipt["capacity_geometry"] = deepcopy(serving["capacity_geometry_json"])
    receipt["initial_target_state_sha256"] = target_hash
    document = json.loads(preflight["receipt_json"])
    document["profile_execution_identity"]["generation"] = generation
    document.update(
        capacity_geometry=receipt["capacity_geometry"],
        serving_generation_preflight=initial_targets,
        serving_generation_preflight_sha256=target_hash,
    )
    preflight["receipt_json"] = json.dumps(document)
    lease = json.loads(consumed["canonical_lease_json"])
    lease["signing_preflight_guard"]["healthcare_receipt"] = document
    consumed["canonical_lease_json"] = json.dumps(lease)
    return consumed, preflight, serving, receipt


async def _publish_key(connection, serving, payload, key, *, use_cte=True):
    serving_by_field = {name: json.dumps(value) if name.endswith("_json") else value for name, value in serving.items()}
    await base._insert(
        connection,
        base.migration._SERVING,
        serving_by_field,
        {name for name in serving_by_field if name.endswith("_json")},
    )
    if not use_cte:
        await connection.execute(
            sa.text("SELECT set_config('provider_directory.initial_receipt_key',:key,true)"), {"key": key}
        )
    statement = initial_runtime._receipt_insert_sql(
        f'"{base._SCHEMA}".{contract.RECEIPT_TABLE}', key if use_cte else "global"
    )
    await connection.execute(
        sa.text(statement),
        dict(
            build=payload["build_id"],
            attestation=payload["attestation_id"],
            generation=serving["generation_id"],
            receipt_key=key,
            run=payload["run_id"],
            contract=contract.COMMIT_CONTRACT,
            payload=json.dumps(payload),
        ),
    )
    await connection.execute(sa.text("SET CONSTRAINTS ALL IMMEDIATE"))


async def _current_receipt(connection, build):
    return await connection.scalar(
        sa.text(f'SELECT to_jsonb(r)::text FROM "{base._SCHEMA}".{contract.RECEIPT_TABLE} r WHERE build_id=:build'),
        {"build": build},
    )


async def _remove_head(connection):
    # This component fixture removes metadata only; it is not a full recovery proof.
    await connection.execute(sa.text(f'DELETE FROM "{base._SCHEMA}".{base.migration._SERVING}'))


async def _reject_unqualified_keys(engine):
    for generation, fault in (
        (20, "v1_hex"),
        (21, "global"),
        (22, "wrong_hex"),
        (23, "unsigned_v2"),
        (24, "malicious_guc"),
        (25, "generation_mismatch"),
    ):
        consumed, preflight, serving, receipt = await _fresh_values(engine, generation, recovered=fault != "v1_hex")
        if fault == "unsigned_v2":
            lease = json.loads(consumed["canonical_lease_json"])
            lease["signing_preflight_guard"]["healthcare_receipt"]["serving_generation_preflight"] = _target()
            consumed["canonical_lease_json"] = json.dumps(lease)
        if fault == "generation_mismatch":
            serving["control_generation"] += 1
            receipt["serving"]["control_generation"] += 1
        await base._seed_authority(engine, consumed, preflight)
        key_generation = serving["control_generation"] + (fault == "wrong_hex")
        key = "global" if fault == "global" else format(key_generation, "x")
        if fault == "malicious_guc":
            key = "unqualified"
        with pytest.raises(DBAPIError):
            async with engine.begin() as connection:
                await _remove_head(connection)
                await _publish_key(connection, serving, receipt, key, use_cte=fault == "wrong_hex")


async def _append_recovered(engine, generation):
    consumed, preflight, serving, receipt = await _fresh_values(engine, generation, recovered=True)
    await base._seed_authority(engine, consumed, preflight)
    async with engine.begin() as connection:
        await _remove_head(connection)
        await connection.execute(sa.text("SET LOCAL provider_directory.initial_receipt_key='unqualified'"))
        await _publish_key(connection, serving, receipt, format(generation, "x"))


async def _exercise(engine, *, historical):
    await base._upgrade(engine)
    migration = base._migration("20261031010000_profile_initial_reactivation.py")
    ordinary, initial = migration._previous()
    # Current model fixtures already have the new purpose CHECK; restore the real migration predecessor.
    await _apply(
        engine,
        lambda: ordinary._replace_check(
            initial._qt(base._SCHEMA, ordinary._TABLE),
            ordinary._CHECK,
            ordinary._purpose_check(ordinary=True),
            ordinary._purpose_check(ordinary=False),
        ),
    )
    first = await _fresh_values(engine, 7, recovered=False)
    if historical:
        await base._seed_authority(engine, *first[:2])
        async with engine.begin() as connection:
            await base._publish(connection, *first[2:])
    async with engine.connect() as connection:
        old_catalog_by_table = {
            table: await _catalog(connection, table)
            for table in (contract.RECEIPT_TABLE, history._SERVING, history._CONSUMPTION)
        }
        old_receipt = await _current_receipt(connection, first[0]["build_id"])
    await _apply(engine, ordinary.upgrade)
    await _apply(engine, migration.upgrade)
    async with engine.connect() as connection:
        for table, original in old_catalog_by_table.items():
            current = await _catalog(connection, table)
            assert history.original_fingerprint_payload(current, schema=base._SCHEMA, table=table) == original
        assert await _current_receipt(connection, first[0]["build_id"]) == old_receipt
    await _reject_unqualified_keys(engine)
    if not historical:
        await base._seed_authority(engine, *first[:2])
        async with engine.begin() as connection:
            await base._publish(connection, *first[2:])
    for generation in (8, 9):
        await _append_recovered(engine, generation)
    with pytest.raises(RuntimeError, match="history_requires_retention"):
        await _apply(engine, migration.downgrade)
    await _assert_history_rows(engine)


async def _assert_history_rows(engine):
    async with engine.connect() as connection:
        keys = (
            (
                await connection.execute(
                    sa.text(
                        f'SELECT singleton_key FROM "{base._SCHEMA}".{contract.RECEIPT_TABLE} ORDER BY singleton_key'
                    )
                )
            )
            .scalars()
            .all()
        )
        assert keys == ["8", "9", "global"]

        async def fetchrow(query):
            return (await connection.execute(sa.text(query))).mappings().one()

        captured = await custody._capture_receipt_rows(SimpleNamespace(fetchrow=fetchrow), base._SCHEMA)
        assert captured["count"] == 3 and len(captured["sha256"]) == 64


@pytest.mark.parametrize("historical", [True, False])
def test_native_initial_history_preserves_guards_and_indexes(monkeypatch, historical):
    async def exercise():
        async with base._database(monkeypatch) as engine:
            await _exercise(engine, historical=historical)

    asyncio.run(exercise())


@pytest.mark.parametrize("generation,recovered", [(7, False), (8, True), (9, True)])
async def test_native_fixture_binds_the_consumed_control_generation(generation, recovered):
    connection = AsyncMock()
    connection.scalar.side_effect = [41, 42, 43]
    context = AsyncMock()
    context.__aenter__.return_value = connection
    engine = SimpleNamespace(connect=lambda: context)
    consumed, preflight, serving, receipt = await _fresh_values(engine, generation, recovered=recovered)
    signed = json.loads(consumed["canonical_lease_json"])["signing_preflight_guard"]["healthcare_receipt"]
    assert preflight["control_generation"] == serving["control_generation"] == generation
    assert receipt["serving"]["control_generation"] == generation
    assert signed == json.loads(preflight["receipt_json"])
    assert signed["profile_execution_identity"]["generation"] == generation
