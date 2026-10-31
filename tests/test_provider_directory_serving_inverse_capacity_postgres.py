# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native inverse admission preserves old ledger bytes, single use and downgrade refusal."""

import asyncio
import importlib
import json
from copy import deepcopy

import pytest
import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy.exc import DBAPIError

from process import provider_directory_profile_initial_history as history
from process import provider_directory_serving_inverse_capacity_contract as contract
from process import provider_directory_serving_inverse_capacity_preflight as preflight
from process.provider_directory_profile_capacity_attestation_contract import CapacityLeaseConsumptionBinding
from process.provider_directory_profile_capacity_consumption import capacity_lease_consumption_values
from tests import test_provider_directory_ordinary_address_capacity_postgres as ordinary
from tests.provider_directory_serving_inverse_capacity_support import signed_envelope
from tests.test_provider_directory_profile_initial_migration import _CatalogDatabase
from tests.test_provider_directory_serving_inverse_capacity_migration import load_migration
from tests.test_provider_directory_serving_inverse_capacity_signing import verify


@pytest.fixture(autouse=True)
def configured_import_node(monkeypatch):
    """Bind this standalone cohort to the signed synthetic fixture's node."""
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "dev-node")


def _signed_rows():
    envelope = signed_envelope()
    lease = verify(envelope)
    guard = lease.signing_preflight_guard
    request = contract.validated_inverse_preflight_request(guard["healthcare_request"])
    receipt = guard["healthcare_receipt"]
    receipt_values = preflight.receipt_row_values(request, receipt, issued_at=lease.issued_at)
    scope = receipt["capacity_geometry"]["scope"]
    provenance = scope["profile_provenance"]
    binding = CapacityLeaseConsumptionBinding(
        "run_" + "9" * 32,
        "pdpb_" + "9" * 32,
        scope["operation_sha256"],
        provenance["selection_proof_id"],
        provenance["source_vector_hash"],
        provenance["source_context_vector_hash"],
        provenance["profile_as_of"],
    )
    consumption_by_field = {
        **capacity_lease_consumption_values(lease, binding, accepted_at=lease.issued_at),
        "admission_purpose": contract.PURPOSE,
    }
    return receipt_values, consumption_by_field, lease


def test_signed_inverse_ledger_rows_keep_exact_authority_and_historical_strategy():
    receipt_values, consumption_by_field, lease = _signed_rows()
    assert consumption_by_field["admission_purpose"] == contract.PURPOSE
    assert receipt_values["materialization_mode"] == contract.PURPOSE
    assert receipt_values["profile_strategy_version"] == "example-strategy"
    assert (
        preflight.read_inverse_receipt_row(receipt_values, lease) == lease.signing_preflight_guard["healthcare_receipt"]
    )


async def _migrate(engine, direction):
    async with engine.begin() as connection:

        def apply(sync):
            with Operations.context(MigrationContext.configure(sync)):
                getattr(load_migration(), direction)()

        await connection.run_sync(apply)


async def _predecessor(engine):
    await ordinary._migrate(engine, "upgrade")
    previous, legacy, initial = load_migration()._previous()
    async with engine.begin() as connection:

        def apply(sync):
            with Operations.context(MigrationContext.configure(sync)):
                schema = initial._ORIGINAL._schema()
                legacy._replace_check(
                    initial._qt(schema, legacy._PREFLIGHT_TABLE),
                    legacy._PREFLIGHT_CHECK,
                    legacy._ordinary_values_check(initial),
                    previous._recovered_preflight_check(legacy, initial),
                )

        await connection.run_sync(apply)


async def _catalog(engine, schema):
    fhir = importlib.import_module("process.provider_directory_fhir")
    async with engine.connect() as connection:
        reader = _CatalogDatabase(connection)
        oid = await connection.scalar(
            sa.text("SELECT to_regclass(:table)::oid"), {"table": schema + "." + history._CONSUMPTION}
        )
        relation_by_field = dict((await reader.all(fhir._PROFILE_CAPACITY_RELATION_SQL, relation_oid=oid))[0]._mapping)
        oids = [oid] + ([relation_by_field["toast_oid"]] if relation_by_field["toast_oid"] else [])
        catalogs = [
            [dict(receipt_values._mapping) for receipt_values in await reader.all(query, relation_oids=oids)]
            for query in (
                fhir._PROFILE_CAPACITY_ATTRIBUTE_SQL,
                fhir._PROFILE_CAPACITY_INDEX_SQL,
                fhir._PROFILE_CAPACITY_CONSTRAINT_SQL,
                fhir._PROFILE_CAPACITY_TRIGGER_SQL,
            )
        ]
        return fhir._profile_capacity_fingerprint_payloads(relation_by_field, *catalogs, oid)[0]


async def _consume(engine, schema, receipt_values, consumption_by_field):
    async with engine.begin() as connection:
        await connection.execute(
            sa.text(f"""UPDATE {ordinary.previous._qt(schema, ordinary.previous._ORIGINAL._TABLE)}
            SET consumed_at=:accepted_at,consumed_run_id=:run_id,consumed_attestation_id=:attestation_id
            WHERE receipt_sha256=:receipt"""),
            {**consumption_by_field, "receipt": receipt_values["receipt_sha256"]},
        )


def test_native_inverse_preserves_old_rows_native_keys_and_historical_fingerprints(monkeypatch):
    async def exercise():
        receipt_values, consumption_by_field, lease = _signed_rows()
        async with ordinary._ledger(monkeypatch) as (engine, schema):
            await ordinary._seed_history(engine, schema)
            two_purpose = await _catalog(engine, schema)
            await _predecessor(engine)
            three_purpose = await _catalog(engine, schema)
            before = await ordinary._history(engine, schema)
            with pytest.raises(DBAPIError, match="preflight_values_check"):
                await ordinary.receipts._insert(engine, schema, receipt_values)
            with pytest.raises(DBAPIError, match="consumption_purpose_check"):
                await ordinary._insert_consumption(engine, schema, consumption_by_field)
            await _migrate(engine, "upgrade")
            assert await ordinary._history(engine, schema) == before
            current = await _catalog(engine, schema)
            assert (
                history.original_fingerprint_payload(current, schema=schema, table=history._CONSUMPTION) == two_purpose
            )
            restored = deepcopy(current)
            history._replace_check(restored, history._INVERSE_PURPOSE, history._NEW_PURPOSE)
            assert restored == three_purpose
            await ordinary.receipts._insert(engine, schema, receipt_values)
            await ordinary._insert_consumption(engine, schema, consumption_by_field)
            await _consume(engine, schema, receipt_values, consumption_by_field)
            async with engine.connect() as connection:
                stored = (
                    (
                        await connection.execute(
                            sa.text(
                                f"SELECT * FROM {ordinary.previous._qt(schema, ordinary.previous._ORIGINAL._TABLE)} WHERE receipt_sha256=:digest"
                            ),
                            {"digest": receipt_values["receipt_sha256"]},
                        )
                    )
                    .mappings()
                    .one()
                )
            assert (
                preflight.read_inverse_receipt_row(stored, lease) == lease.signing_preflight_guard["healthcare_receipt"]
            )
            with pytest.raises(DBAPIError, match="preflight_update_invalid"):
                await _consume(engine, schema, receipt_values, consumption_by_field)
            retained = await ordinary._history(engine, schema)
            with pytest.raises(RuntimeError, match="history_requires_retention"):
                await _migrate(engine, "downgrade")
            assert await ordinary._history(engine, schema) == retained

    asyncio.run(exercise())


def _damaged_receipt(fault):
    receipt_values, _consumed, _lease = _signed_rows()
    document = json.loads(receipt_values["receipt_json"])
    match fault:
        case "mode":
            receipt_values["materialization_mode"] = "source_delta"
        case "generation":
            receipt_values["control_generation"] += 1
        case "strategy":
            receipt_values["profile_strategy_version"] = "different-historical-strategy"
        case "input":
            receipt_values["serving_preflight_sha256"] = "0" * 64
        case "nonce":
            receipt_values["request_nonce"] = "0" * 64
        case "purpose":
            document["capacity_geometry"]["admission_purpose"] = "serving_nonprofile"
        case "scope":
            document["capacity_geometry"]["scope"]["contract_id"] = "unqualified"
        case "operation":
            document["recovery_execution_identity"]["operation"] = "publish"
        case "missing":
            del document["capacity_geometry"]["scope"]["profile_provenance"]
        case _:
            raise AssertionError(fault)
    receipt_values["receipt_json"] = json.dumps(document)
    return receipt_values


@pytest.mark.parametrize(
    "fault", ["mode", "generation", "strategy", "input", "nonce", "purpose", "scope", "missing", "operation"]
)
def test_native_inverse_rejects_crosswired_or_missing_authority(monkeypatch, fault):
    async def exercise():
        receipt_values = _damaged_receipt(fault)
        async with ordinary._ledger(monkeypatch) as (engine, schema):
            await _predecessor(engine)
            await _migrate(engine, "upgrade")
            with pytest.raises(DBAPIError, match="preflight_values_check"):
                await ordinary.receipts._insert(engine, schema, receipt_values)

    asyncio.run(exercise())


@pytest.mark.parametrize("issued", [False, True])
def test_native_inverse_downgrade_requires_no_issued_authority(monkeypatch, issued):
    async def exercise():
        async with ordinary._ledger(monkeypatch) as (engine, schema):
            await _predecessor(engine)
            before = await ordinary._checks(engine, schema)
            await _migrate(engine, "upgrade")
            if issued:
                receipt_values, _consumed, _lease = _signed_rows()
                await ordinary.receipts._insert(engine, schema, receipt_values)
                with pytest.raises(RuntimeError, match="history_requires_retention"):
                    await _migrate(engine, "downgrade")
            else:
                await _migrate(engine, "downgrade")
                assert await ordinary._checks(engine, schema) == before

    asyncio.run(exercise())
