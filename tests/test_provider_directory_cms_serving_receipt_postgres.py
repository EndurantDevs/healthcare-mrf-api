# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native scalar-authority proofs for the composite receipt's deferred guards."""

import importlib.util
import json
from contextlib import asynccontextmanager
from pathlib import Path
from uuid import uuid4

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine

from process import provider_directory_cms_native_inputs as native_inputs
from process import provider_directory_cms_serving_receipt as receipts
from tests.cms_npd_admission_postgres_support import _database_url
from tests.reference_family_generation_fixture import install_source_generation_guards

_MIGRATIONS = Path(__file__).resolve().parents[1] / "alembic" / "versions"
_PIN = {
    "source_id": "cms-npd",
    "endpoint_id": "endpoint",
    "dataset_id": "dataset",
    "dataset_hash": "a" * 64,
    "acquisition_root_run_id": "run-synthetic",
}


def _migration(prefix):
    (path,) = _MIGRATIONS.glob(prefix + "*.py")
    spec = importlib.util.spec_from_file_location("receipt_" + prefix, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _apply(connection, prefix, function="upgrade"):
    with Operations.context(MigrationContext.configure(connection)):
        getattr(_migration(prefix), function)()


def test_fixture_migration_rejects_ambiguous_prefix():
    """A shared timestamp cannot silently select the wrong migration."""
    with pytest.raises(ValueError, match="too many values"):
        _migration("20260914120000")


async def _create_scalar_tables(connection, schema):
    """Model only the already-sealed source scalars; no resource-level proof is fabricated here."""
    definition_by_table = {
        "provider_directory_source": "source_id text PRIMARY KEY, endpoint_id text",
        "provider_directory_endpoint_dataset": "dataset_id text PRIMARY KEY, endpoint_id text, dataset_hash text, "
        "acquisition_root_run_id text,status text,is_current boolean,published_at timestamp, "
        "publication_metadata_summary_json jsonb,content_proof_admission_sha256 text,publication_metadata_sha256 text",
        "provider_directory_cms_candidate_coverage": "dataset_id text,release_id text,proof_version int,dataset_hash text, "
        "admission_sha256 text,metadata_sha256 text,relationship_count bigint",
        "provider_directory_cms_serving_coverage": "dataset_id text,release_id text,proof_version int,dataset_hash text,published_at timestamp",
        "provider_directory_cms_npd_relationship_receipt": "dataset_id text,release_id text,relationship_count bigint,projection_contract text",
        "address_alias_state_v1": "singleton boolean PRIMARY KEY,generation bigint",
        "address_alias_artifact_state_v1": "artifact_name text PRIMARY KEY,generation bigint",
    }
    for table, definition in definition_by_table.items():
        await connection.execute(text(f"CREATE TABLE {schema}.{table} ({definition})"))
    for table in receipts._NATIVE_RELATIONS:
        await connection.execute(text(f"CREATE TABLE {schema}.{table} (synthetic_id int)"))


async def _create_native_tables(connection, schema):
    """Use the actual address migration and current Doctors authority shape."""
    await connection.run_sync(lambda sync: _apply(sync, "20260914100000"))
    with_shape = _migration("20260929000000")
    previous, counts = with_shape._shape_support()
    shape = previous._shape({**counts, "cms-doctors": 3})
    await connection.execute(
        text(f"""CREATE TABLE {schema}.reference_family_result_generation (
        importer_id text PRIMARY KEY, local_lineage_id uuid NOT NULL,local_generation bigint NOT NULL,
        origin_lineage_id uuid,origin_generation bigint,published_at timestamptz,relation_oids bigint[],
        CONSTRAINT reference_family_result_generation_shape_check CHECK ({shape}))""")
    )
    await connection.execute(
        text(
            f"INSERT INTO {schema}.reference_family_result_generation "
            "(importer_id,local_lineage_id,local_generation) VALUES ('cms-doctors',:lineage,0)"
        ),
        {"lineage": uuid4()},
    )
    await install_source_generation_guards(connection, schema)


def _create_profile_table(connection, schema):
    with Operations.context(MigrationContext.configure(connection)):
        _migration("20260730110000")._create_serving_generation_table(schema)
        _migration("20260730110000")._create_delta_receipt_table(schema)


@asynccontextmanager
async def _database(monkeypatch, *, install_receipt=True):
    """Confine real guard SQL to a UUID schema in the existing UUID-owned database guard."""
    schema = "cms_receipt_" + uuid4().hex
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.setenv("DB_SCHEMA", schema)
    engine = create_async_engine(_database_url())
    try:
        async with engine.begin() as connection:
            await connection.execute(text(f"CREATE SCHEMA {schema}"))
            await _create_scalar_tables(connection, schema)
            await connection.run_sync(lambda sync: _create_profile_table(sync, schema))
            await _create_native_tables(connection, schema)
            if install_receipt:
                await connection.run_sync(lambda sync: _apply(sync, "20260930100000"))
        yield engine, schema
    finally:
        async with engine.begin() as connection:
            await connection.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
            assert not await connection.scalar(
                text("SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname=:schema)"), {"schema": schema}
            )
        await engine.dispose()


async def _seed_source(connection, schema):
    await connection.execute(text(f"INSERT INTO {schema}.provider_directory_source VALUES ('cms-npd','endpoint')"))
    await connection.execute(
        text(f"""INSERT INTO {schema}.provider_directory_endpoint_dataset VALUES
        ('dataset','endpoint',:hash,'run-synthetic','published',true,'2026-01-01',
         '{{"source_ids":["cms-npd"]}}',:hash,:hash)"""),
        {"hash": "a" * 64},
    )
    await connection.execute(
        text(
            f"INSERT INTO {schema}.provider_directory_cms_candidate_coverage "
            "VALUES ('dataset',:release,2,:hash,:hash,:hash,0)"
        ),
        {"release": "b" * 64, "hash": "a" * 64},
    )
    await connection.execute(
        text(
            f"INSERT INTO {schema}.provider_directory_cms_serving_coverage "
            "VALUES ('dataset',:release,2,:hash,'2026-01-01')"
        ),
        {"release": "b" * 64, "hash": "a" * 64},
    )
    await connection.execute(
        text(
            f"INSERT INTO {schema}.provider_directory_cms_npd_relationship_receipt "
            "VALUES ('dataset',:release,0,'cms-npd-reference-ledger-v1')"
        ),
        {"release": "b" * 64},
    )
    await connection.execute(text(f"INSERT INTO {schema}.address_alias_state_v1 VALUES (true,0)"))
    await connection.execute(
        text(f"INSERT INTO {schema}.address_alias_artifact_state_v1 VALUES ('provider_directory_address_overlay',0)")
    )


async def _seed_profile(connection, schema):
    table = f"{schema}.provider_directory_profile_serving_generation"
    await connection.execute(
        text(f"""INSERT INTO {table} (
        singleton_key,status,operation,control_generation,generation_id,selection_proof_id,authority_revision,
        profile_schema_version,profile_strategy_version,source_vector_hash,source_vector_json,source_context_vector_hash,
        source_context_vector_json,executable_plan_hash,capacity_geometry_status,evidence_target_oid,profile_target_oid,
        evidence_rows,profile_rows,profile_as_of,published_at) VALUES (
        'global','published','publish',1,:generation,:hash,1,1,'strategy',:hash,:vector,:hash,'[]',:hash,'legacy_unavailable',
        to_regclass('{schema}.provider_directory_profile_evidence')::oid,
        to_regclass('{schema}.provider_directory_profile')::oid,0,0,'2026-01-01',now())"""),
        {
            "hash": "a" * 64,
            "generation": "pdprofile_" + "1" * 32,
            "vector": json.dumps([{key: _PIN[key] for key in ("source_id", "dataset_id")}]),
        },
    )


async def _advance_native(connection, schema, *, doctors=False):
    table = "reference_family_result_generation" if doctors else "entity_address_result_generation"
    predicate = "importer_id='cms-doctors'" if doctors else "singleton"
    names = list(
        _migration("20260930100000")._DOCTOR_RELATIONS if doctors else _migration("20260930100000")._ADDRESS_RELATIONS
    )
    await connection.execute(
        text(f"""UPDATE {schema}.{table} SET local_generation=local_generation+1,
        origin_lineage_id=local_lineage_id,origin_generation=local_generation+1,published_at=clock_timestamp(),
        relation_oids=ARRAY(SELECT to_regclass(:schema||'.'||name)::oid::bigint FROM unnest(CAST(:names AS text[])) name)
        WHERE {predicate}"""),
        {"schema": schema, "names": names},
    )


async def _payload(connection, schema, prior=None):
    snapshot = await receipts.capture_native_dependencies(connection, schema)
    return {
        "contract_version": 1,
        "predecessor_receipt_id": prior["receipt_id"] if prior else None,
        "expected_incumbent": _PIN if prior else None,
        "cms": {**_PIN, "release_id": "b" * 64, "proof_version": 2},
        "desired_datasets": [_PIN],
        "selection": {"proof_id": "a" * 64, "fingerprint": "b" * 64, "catalog_digest": "c" * 64},
        **snapshot,
    }


async def _publish_initial(engine, schema):
    async with engine.begin() as connection:
        await _seed_source(connection, schema)
        await _seed_profile(connection, schema)
        await _advance_native(connection, schema, doctors=True)
        await _advance_native(connection, schema)
        payload = await _payload(connection, schema)
        receipt_id = await receipts.append_serving_receipt(connection, schema, payload)
        assert not await receipts.verify_historical_receipt(connection, schema, receipt_id, payload)
    return {"receipt_id": receipt_id, "payload": payload}


async def _install_archive(engine, schema):
    """Use the real mutation ledger and ALWAYS triggers around a minimal canonical heap."""
    async with engine.begin() as connection:
        await connection.run_sync(lambda sync: _apply(sync, "20260930120000"))
        await connection.execute(
            text(f"CREATE TABLE {schema}.address_archive_v2 (address_key int PRIMARY KEY, marker text)")
        )
        oid = await connection.scalar(text(f"SELECT to_regclass('{schema}.address_archive_v2')::oid::bigint"))
        await native_inputs._register_relation(connection, schema, schema, "address_archive_v2", oid)
    return {
        "target_oid": oid,
        "from_revision": 0,
        "to_revision": 2,
        "native_input_hash": "a" * 64,
        "delta_rows": 1,
        "delta_sha256": "b" * 64,
    }


async def _merge_archive(connection, schema):
    await connection.execute(
        text(
            f"INSERT INTO {schema}.address_archive_v2 VALUES (1,'new') "
            "ON CONFLICT (address_key) DO UPDATE SET marker=EXCLUDED.marker"
        )
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("publication_case", ["current", "prior", "missing", "oid", "revision"])
async def test_new_archive_receipt_requires_the_owner_transaction(monkeypatch, publication_case):
    async with _database(monkeypatch) as (engine, schema):
        initial = await _publish_initial(engine, schema)
        proof = await _install_archive(engine, schema)
        if publication_case == "prior":
            async with engine.begin() as connection:
                await _merge_archive(connection, schema)
        if publication_case == "oid":
            proof["target_oid"] += 1
        if publication_case == "revision":
            proof.update(from_revision=2, to_revision=4)

        async def publish():
            async with engine.begin() as connection:
                if publication_case not in {"prior", "missing"}:
                    await _merge_archive(connection, schema)
                await _advance_native(connection, schema)
                payload_by_field = await _payload(connection, schema, initial)
                payload_by_field["archive"] = proof
                return await receipts.append_serving_receipt(connection, schema, payload_by_field), payload_by_field

        if publication_case == "current":
            receipt_id, payload_by_field = await publish()
            async with engine.connect() as connection:
                assert await receipts.verify_historical_receipt(connection, schema, receipt_id, payload_by_field)
        else:
            with pytest.raises(DBAPIError, match="cms_serving_.*receipt_.*"):
                await publish()
            async with engine.connect() as connection:
                assert await receipts.read_current_receipt(connection, schema) == initial


@pytest.mark.asyncio
@pytest.mark.parametrize("retain", [True, False])
async def test_archive_history_survives_later_native_input_changes(monkeypatch, retain):
    async with _database(monkeypatch) as (engine, schema):
        initial = await _publish_initial(engine, schema)
        proof = await _install_archive(engine, schema)
        async with engine.begin() as connection:
            await _merge_archive(connection, schema)
            await _advance_native(connection, schema)
            payload = await _payload(connection, schema, initial)
            payload["archive"] = proof
            receipt_id = await receipts.append_serving_receipt(connection, schema, payload)
        predecessor_by_field = {"receipt_id": receipt_id, "payload": payload}
        async with engine.begin() as connection:
            await connection.execute(text(f"UPDATE {schema}.address_archive_v2 SET marker='later'"))

        async def successor():
            async with engine.begin() as connection:
                await _advance_native(connection, schema)
                next_payload = await _payload(connection, schema, predecessor_by_field)
                if retain:
                    next_payload["archive"] = proof
                return await receipts.append_serving_receipt(connection, schema, next_payload)

        if retain:
            await successor()
        else:
            with pytest.raises(DBAPIError, match="cms_serving_.*receipt_.*"):
                await successor()
        async with engine.connect() as connection:
            assert await receipts.verify_historical_receipt(connection, schema, receipt_id, payload)


@pytest.mark.asyncio
async def test_common_receipt_history_and_native_pair_refresh(monkeypatch):
    """Address-only and Profile-only publications each advance the chain; old acknowledgments remain provable."""
    async with _database(monkeypatch) as (engine, schema):
        first = await _publish_initial(engine, schema)
        async with engine.begin() as connection:
            assert await receipts.read_current_receipt(connection, schema) == first
            await _advance_native(connection, schema)
            address_payload = await _payload(connection, schema, first)
            next_id = await receipts.append_serving_receipt(connection, schema, address_payload)
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    f"UPDATE {schema}.provider_directory_profile_serving_generation "
                    "SET generation_id=:generation,profile_as_of='2026-01-02'"
                ),
                {"generation": "pdprofile_" + "2" * 32},
            )
            final_payload = await _payload(connection, schema, {"receipt_id": next_id})
            final_id = await receipts.append_serving_receipt(connection, schema, final_payload)
        async with engine.connect() as connection:
            assert (await receipts.read_current_receipt(connection, schema))["receipt_id"] == final_id
            assert await receipts.verify_historical_receipt(connection, schema, first["receipt_id"], first["payload"])


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["address", "doctors", "profile", "source", "dataset", "vector", "root"])
async def test_native_publication_without_common_receipt_rolls_back(monkeypatch, change):
    """Generic publishers cannot commit any partial serving authority transition."""
    async with _database(monkeypatch) as (engine, schema):
        initial = await _publish_initial(engine, schema)
        with pytest.raises(DBAPIError, match="cms_serving_fresh_receipt_required"):
            async with engine.begin() as connection:
                await _change_authority(connection, schema, change)
        async with engine.connect() as connection:
            assert await receipts.read_current_receipt(connection, schema) == initial


async def _change_authority(connection, schema, change):
    if change in {"address", "doctors"}:
        await _advance_native(connection, schema, doctors=change == "doctors")
    elif change == "profile":
        await connection.execute(
            text(
                f"UPDATE {schema}.provider_directory_profile_serving_generation "
                "SET status='purged',operation='purge',source_vector_json='[]'"
            )
        )
    elif change in {"vector", "root"}:
        statement = (
            "provider_directory_profile_serving_generation SET source_vector_json='[]'"
            if change == "vector"
            else "provider_directory_endpoint_dataset SET acquisition_root_run_id='other-root'"
        )
        await connection.execute(text(f"UPDATE {schema}.{statement}"))
    elif change == "source":
        await connection.execute(text(f"UPDATE {schema}.provider_directory_source SET endpoint_id='other'"))
    else:
        await connection.execute(
            text(f"UPDATE {schema}.provider_directory_endpoint_dataset SET status='superseded',is_current=false")
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["publish", "purge"])
async def test_profile_selection_preserves_independent_cms_source_pin(monkeypatch, operation):
    """An ordinary published or purged Profile may omit CMS without withdrawing its native source."""
    async with _database(monkeypatch) as (engine, schema):
        initial = await _publish_initial(engine, schema)
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    f"UPDATE {schema}.provider_directory_profile_serving_generation "
                    "SET status=:status,operation=:operation,source_vector_json='[]',generation_id=:generation"
                ),
                {
                    "generation": "pdprofile_" + "3" * 32,
                    "operation": operation,
                    "status": "purged" if operation == "purge" else "published",
                },
            )
            payload = await _payload(connection, schema, initial)
            payload["desired_datasets"] = []
            receipt_id = await receipts.append_serving_receipt(connection, schema, payload)
        async with engine.connect() as connection:
            assert (await receipts.read_current_receipt(connection, schema))["receipt_id"] == receipt_id
            assert await connection.scalar(text(f"SELECT is_current FROM {schema}.provider_directory_endpoint_dataset"))


@pytest.mark.asyncio
@pytest.mark.parametrize("tamper", ["coverage", "vector", "doctors", "date", "overlay", "predecessor"])
async def test_receipt_rejects_changed_authority_and_rolls_back(monkeypatch, tamper):
    """Neither a changed proof nor a forged native result can commit partial publication."""
    async with _database(monkeypatch) as (engine, schema):
        initial = await _publish_initial(engine, schema)
        with pytest.raises(DBAPIError):
            async with engine.begin() as connection:
                await _advance_native(connection, schema)
                payload = await _payload(connection, schema, initial)
                _tamper(payload, tamper)
                await receipts.append_serving_receipt(connection, schema, payload)
        async with engine.connect() as connection:
            assert await receipts.read_current_receipt(connection, schema) == initial


def _tamper(payload, kind):
    if kind == "vector":
        payload["cms"]["dataset_hash"] = "d" * 64
        payload["desired_datasets"] = [{**_PIN, "dataset_hash": "d" * 64}]
        return
    if kind == "predecessor":
        payload["expected_incumbent"] = {**_PIN, "dataset_hash": "d" * 64}
        return
    if kind == "overlay":
        payload["overlay_oid"] += 1
        return
    target_by_kind = {
        "coverage": ("cms", "release_id", "d" * 64),
        "date": ("profile", "profile_as_of", "2026-01-03"),
        "doctors": ("doctors", "origin_generation", 999),
    }
    section, field, value = target_by_kind[kind]
    payload[section][field] = value


@pytest.mark.asyncio
async def test_receipt_and_native_authorities_cannot_be_bulk_removed(monkeypatch):
    """UPDATE, DELETE, and TRUNCATE cannot erase a committed result or bypass pointer guards."""
    async with _database(monkeypatch) as (engine, schema):
        initial = await _publish_initial(engine, schema)
        statements = [
            f"UPDATE {schema}.provider_directory_cms_serving_receipt SET payload=payload",
            f"DELETE FROM {schema}.provider_directory_cms_serving_receipt",
        ]
        statements.extend(
            f"TRUNCATE {schema}.{table} CASCADE"
            for table in (
                "provider_directory_cms_serving_receipt",
                "provider_directory_profile_serving_generation",
                "entity_address_result_generation",
                "reference_family_result_generation",
            )
        )
        for statement in statements:
            with pytest.raises(DBAPIError):
                async with engine.begin() as connection:
                    await connection.execute(text(statement))
        async with engine.connect() as connection:
            assert await receipts.read_current_receipt(connection, schema) == initial


@pytest.mark.asyncio
async def test_generation_zero_doctors_and_changed_dependency_snapshot_fail_closed(monkeypatch):
    """Canonical table existence cannot replace accepted Doctors authority or a locked snapshot."""
    async with _database(monkeypatch) as (engine, schema):
        async with engine.begin() as connection:
            with pytest.raises(RuntimeError, match="doctors_authority_unavailable"):
                await receipts.capture_native_dependencies(connection, schema, lock=True)
        initial = await _publish_initial(engine, schema)
        async with engine.begin() as connection:
            expected_by_field = {
                key: initial["payload"][key]
                for key in ("profile", "address", "doctors", "alias_generation", "overlay_oid")
            }
            await receipts.assert_native_dependencies(connection, schema, expected_by_field)
            expected_by_field["alias_generation"] += 1
            with pytest.raises(RuntimeError, match="dependencies_changed"):
                await receipts.assert_native_dependencies(connection, schema, expected_by_field)


@pytest.mark.asyncio
async def test_migration_refuses_legacy_current_source_without_adoption(monkeypatch):
    """An incumbent without common authority blocks installation and leaves no invented history."""
    async with _database(monkeypatch, install_receipt=False) as (engine, schema):
        async with engine.begin() as connection:
            await _seed_source(connection, schema)
        with pytest.raises(DBAPIError, match="legacy_current_requires_staged_upgrade"):
            async with engine.begin() as connection:
                await connection.run_sync(lambda sync: _apply(sync, "20260930100000"))
        async with engine.connect() as connection:
            assert (
                await connection.scalar(text(f"SELECT to_regclass('{schema}.provider_directory_cms_serving_receipt')"))
                is None
            )
            assert await connection.scalar(text(f"SELECT is_current FROM {schema}.provider_directory_endpoint_dataset"))


@pytest.mark.asyncio
async def test_alias_drift_preserves_predecessor_but_requires_fresh_build(monkeypatch):
    """Incumbent lookup cannot turn stale alias evidence into new publication authority."""
    async with _database(monkeypatch) as (engine, schema):
        initial = await _publish_initial(engine, schema)
        async with engine.begin() as connection:
            await connection.execute(text(f"UPDATE {schema}.address_alias_state_v1 SET generation=1"))
        async with engine.connect() as connection:
            assert await receipts.read_current_receipt(connection, schema) == initial
            assert await receipts.read_serving_receipt(connection, schema) == initial
            assert not await connection.scalar(
                text(f"SELECT {schema}.cms_serving_receipt_matches(CAST(:payload AS jsonb))"),
                {"payload": json.dumps(initial["payload"])},
            )
        with pytest.raises(DBAPIError, match="fresh_receipt_required"):
            async with engine.begin() as connection:
                await _advance_native(connection, schema)
                payload = await _payload(connection, schema, initial)
                await receipts.append_serving_receipt(connection, schema, payload)
        async with engine.begin() as connection:
            await _advance_native(connection, schema)
            await connection.execute(text(f"UPDATE {schema}.address_alias_artifact_state_v1 SET generation=1"))
            payload = await _payload(connection, schema, initial)
            receipt_id = await receipts.append_serving_receipt(connection, schema, payload)
        async with engine.connect() as connection:
            assert (await receipts.read_current_receipt(connection, schema))["receipt_id"] == receipt_id


@pytest.mark.asyncio
async def test_dependency_source_drift_preserves_predecessor_only(monkeypatch):
    """A superseded dependency remains historical evidence, never a fresh desired-vector proof."""
    async with _database(monkeypatch) as (engine, schema):
        initial = await _publish_initial(engine, schema)
        async with engine.begin() as connection:
            await connection.execute(
                text(f"INSERT INTO {schema}.provider_directory_source VALUES ('other','other-endpoint')")
            )
            await connection.execute(
                text(f"""INSERT INTO {schema}.provider_directory_endpoint_dataset
                SELECT 'other-dataset','other-endpoint',dataset_hash,acquisition_root_run_id,status,is_current,
                    published_at,'{{"source_ids":["other"]}}',content_proof_admission_sha256,publication_metadata_sha256
                FROM {schema}.provider_directory_endpoint_dataset WHERE dataset_id='dataset'""")
            )
            pins = [
                _PIN,
                {**_PIN, "source_id": "other", "endpoint_id": "other-endpoint", "dataset_id": "other-dataset"},
            ]
            await connection.execute(
                text(
                    f"UPDATE {schema}.provider_directory_profile_serving_generation SET generation_id=:generation,source_vector_json=:vector"
                ),
                {
                    "generation": "pdprofile_" + "4" * 32,
                    "vector": json.dumps([{key: pin[key] for key in ("source_id", "dataset_id")} for pin in pins]),
                },
            )
            receipt_payload = await _payload(connection, schema, initial)
            receipt_payload["desired_datasets"] = pins
            receipt_id = await receipts.append_serving_receipt(connection, schema, receipt_payload)
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    f"UPDATE {schema}.provider_directory_endpoint_dataset SET status='superseded',is_current=false WHERE dataset_id='other-dataset'"
                )
            )
        async with engine.connect() as connection:
            assert (await receipts.read_current_receipt(connection, schema))["receipt_id"] == receipt_id
            assert not await connection.scalar(
                text(f"SELECT {schema}.cms_serving_receipt_matches(CAST(:receipt_payload AS jsonb))"),
                {"receipt_payload": json.dumps(receipt_payload)},
            )


@pytest.mark.asyncio
async def test_initial_source_alias_cannot_adopt_current_dataset_without_receipt(monkeypatch):
    """An alias insertion cannot bootstrap CMS serving by relabeling an unrelated current dataset."""
    async with _database(monkeypatch) as (engine, schema):
        async with engine.begin() as connection:
            await connection.execute(
                text(f"""INSERT INTO {schema}.provider_directory_endpoint_dataset
                (dataset_id,endpoint_id,dataset_hash,acquisition_root_run_id,status,is_current,publication_metadata_summary_json)
                VALUES ('other','endpoint',:hash,'run','published',true,'{{"source_ids":["other"]}}')"""),
                {"hash": "a" * 64},
            )
        with pytest.raises(DBAPIError, match="fresh_receipt_required"):
            async with engine.begin() as connection:
                await connection.execute(
                    text(f"INSERT INTO {schema}.provider_directory_source VALUES ('cms-npd','endpoint')")
                )
        async with engine.connect() as connection:
            assert await connection.scalar(text(f"SELECT count(*) FROM {schema}.provider_directory_source")) == 0


@pytest.mark.asyncio
async def test_serving_receipt_rejects_replaced_overlay_relation(monkeypatch):
    """Stable ledger scalars cannot conceal a replaced physical overlay from an accepted read."""
    async with _database(monkeypatch) as (engine, schema):
        initial = await _publish_initial(engine, schema)
        async with engine.begin() as connection:
            assert await receipts.read_serving_receipt(connection, schema) == initial
            await connection.execute(
                text(f"ALTER TABLE {schema}.provider_directory_address_overlay RENAME TO old_overlay")
            )
            await connection.execute(
                text(f"CREATE TABLE {schema}.provider_directory_address_overlay (synthetic_id int)")
            )
            assert await receipts.read_serving_receipt(connection, schema) is None
