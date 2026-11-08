# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Disposable PostgreSQL proofs for Profile capacity preflight state."""

from __future__ import annotations

import asyncio
import datetime
import importlib
import json
import os
import types
import uuid
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from alembic.config import Config
from alembic.migration import MigrationContext
from alembic.operations import Operations
from alembic.script import ScriptDirectory
from sqlalchemy.engine import make_url
from sqlalchemy.exc import OperationalError
from sqlalchemy.schema import MetaData

from db.connection import Database
from db.models.system import (
    ProviderDirectoryProfileCapacityLeaseConsumption,
)
from process import provider_directory_profile as profile
from process import provider_directory_profile as profile_artifact
from process import provider_directory_profile_capacity_attestation as lease
from process import provider_directory_profile_capacity_preflight_contract as contract
from process import provider_directory_profile_runtime_observation as runtime
from tests.provider_directory_profile_capacity_v2_migration_support import (
    capacity_constraint_definition,
)
from tests.provider_directory_profile_delta_schema_fixtures import (
    _create_delta_contract_tables,
)
from tests.provider_directory_profile_delta_test_support import (
    _delta_database,
)
from tests.test_provider_directory_profile_capacity_attestation import (
    _signed_envelope,
    _verify,
)
from tests.test_provider_directory_profile_capacity_preflight import (
    _serving_state,
)
from tests.test_provider_directory_profile_capacity_runtime import (
    _limits_payload,
)

_POSTGRES_DSN_ENV = "HLTHPRT_PROVIDER_DIRECTORY_PROFILE_POSTGRES_DSN"
UTC = datetime.timezone.utc
importer = importlib.import_module("process.provider_directory_fhir")
_MIGRATION_PATH = (
    Path(__file__).resolve().parents[1] / "alembic/versions/"
    "20260811010000_provider_directory_profile_capacity_preflight_receipt.py"
)
_MIGRATION_SPEC = importlib.util.spec_from_file_location(
    "provider_directory_profile_capacity_preflight_receipt_migration",
    _MIGRATION_PATH,
)
assert _MIGRATION_SPEC is not None and _MIGRATION_SPEC.loader is not None
receipt_migration = importlib.util.module_from_spec(_MIGRATION_SPEC)
_MIGRATION_SPEC.loader.exec_module(receipt_migration)


async def _run_receipt_migration(
    database: Database,
    *,
    upgrade: bool,
) -> None:
    """Run the isolated receipt revision through real Alembic operations."""

    async with database.engine.begin() as connection:

        def migrate(sync_connection) -> None:
            migration_context = MigrationContext.configure(sync_connection)
            operations = Operations(migration_context)
            original_operations = receipt_migration.op
            receipt_migration.op = operations
            try:
                if upgrade:
                    receipt_migration.upgrade()
                else:
                    receipt_migration.downgrade()
            finally:
                receipt_migration.op = original_operations

        await connection.run_sync(migrate)


async def _prepare_receipt_schema(
    monkeypatch: pytest.MonkeyPatch,
    database: Database,
    schema: str,
) -> None:
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.setenv("DB_SCHEMA", schema)
    monkeypatch.setattr(importer, "db", database)
    capacity_ref = profile.qualified_table(
        schema,
        ProviderDirectoryProfileCapacityLeaseConsumption.__tablename__,
    )
    await database.status(f"DROP TABLE IF EXISTS {capacity_ref} CASCADE;")
    capacity_table = ProviderDirectoryProfileCapacityLeaseConsumption.__table__.to_metadata(MetaData(), schema=schema)
    await database.create_table(capacity_table)
    await database.status(f"ALTER TABLE {capacity_ref} DROP CONSTRAINT pd_profile_capacity_consumption_values_check;")
    v2_condition = receipt_migration._capacity_consumption_check(
        receipt_migration._capacity_contract_predicate(
            receipt_migration._V1_CONTRACT,
            receipt_migration._V2_CONTRACT,
        )
    )
    await database.status(
        f"ALTER TABLE {capacity_ref} ADD CONSTRAINT "
        "pd_profile_capacity_consumption_values_check "
        f"CHECK ({v2_condition});"
    )
    await _run_receipt_migration(database, upgrade=True)


def _digest(label: str) -> str:
    return contract.preflight_domain_sha256(
        "healthporta.test.provider-profile-capacity-preflight.v1",
        {"label": label},
    )


def _receipt_values(
    label: str,
    *,
    issued_at: datetime.datetime,
    expires_at: datetime.datetime,
    request_nonce: str | None = None,
) -> dict[str, object]:
    limits_payload = _limits_payload()
    limits_sha256 = contract.capacity_limits_sha256(limits_payload)
    receipt_by_field = {
        "contract_id": contract.CAPACITY_PREFLIGHT_CONTRACT_ID,
        "capacity_limits": limits_payload,
        "capacity_limits_sha256": limits_sha256,
        "test_label": label,
    }
    return {
        "receipt_sha256": _digest(label + ":receipt"),
        "request_nonce": request_nonce or _digest(label + ":nonce"),
        "request_sha256": _digest(label + ":request"),
        "control_plane_receipt_sha256": _digest(label + ":control"),
        "contract_id": contract.CAPACITY_PREFLIGHT_CONTRACT_ID,
        "request_contract_id": contract.CAPACITY_PREFLIGHT_REQUEST_CONTRACT_ID,
        "limits_contract_id": limits_payload["contract_id"],
        "selection_proof_id": _digest(label + ":selection"),
        "profile_input_digest": _digest(label + ":input"),
        "control_generation": 1,
        "profile_schema_version": profile.PROFILE_SCHEMA_VERSION,
        "profile_strategy_version": profile.PROFILE_BUILD_STRATEGY_VERSION,
        "materialization_mode": "source_delta",
        "limits_sha256": limits_sha256,
        "capacity_geometry_hash": _digest(label + ":geometry"),
        "serving_preflight_sha256": _digest(label + ":serving"),
        "quiescence_sha256": _digest(label + ":quiescence"),
        "receipt_json": json.dumps(
            receipt_by_field,
            sort_keys=True,
            separators=(",", ":"),
        ),
        "issued_at": issued_at,
        "expires_at": expires_at,
        "created_at": issued_at,
    }


async def _insert_receipt(
    database: Database,
    schema: str,
    values: dict[str, object],
) -> int:
    receipt_ref = profile.qualified_table(
        schema,
        "provider_directory_profile_capacity_preflight_receipt",
    )
    fields = tuple(values)
    value_sql = ", ".join(
        ("CAST(:receipt_json AS jsonb)" if field_name == "receipt_json" else ":" + field_name) for field_name in fields
    )
    return int(
        await database.status(
            f"INSERT INTO {receipt_ref} ({', '.join(fields)}) VALUES ({value_sql});",
            **values,
        )
        or 0
    )


def _configure_database(monkeypatch: pytest.MonkeyPatch, dsn: str) -> None:
    url = make_url(dsn)
    monkeypatch.setenv("HLTHPRT_DB_DRIVER", "asyncpg")
    monkeypatch.setenv("HLTHPRT_DB_HOST", str(url.host or "127.0.0.1"))
    monkeypatch.setenv("HLTHPRT_DB_PORT", str(url.port or 5432))
    monkeypatch.setenv("HLTHPRT_DB_USER", str(url.username or "postgres"))
    monkeypatch.setenv("HLTHPRT_DB_PASSWORD", str(url.password or ""))
    monkeypatch.setenv("HLTHPRT_DB_DATABASE", str(url.database or "postgres"))
    monkeypatch.delenv("HLTHPRT_DB_DATABASE_OVERRIDE", raising=False)
    monkeypatch.setenv("HLTHPRT_DB_POOL_MIN_SIZE", "1")
    monkeypatch.setenv("HLTHPRT_DB_POOL_MAX_SIZE", "2")


def _lease_for_runtime_observation(
    observation: dict[str, object],
):
    def bind_runtime(body):
        runtime_witness = body["runtime_witness"]
        for field_name in runtime.CAPACITY_LEASE_LOCALLY_VERIFIED_RUNTIME_FIELDS:
            runtime_witness[field_name] = observation[field_name]
        body["runtime_witness_sha256"] = lease.capacity_runtime_witness_sha256(
            runtime_witness,
            body["deployment_witness"],
        )

    return _verify(_signed_envelope(body_mutator=bind_runtime))


def _assert_runtime_bound_lease_replay(observation) -> None:
    verified_lease = _lease_for_runtime_observation(observation)
    runtime.assert_capacity_lease_matches_runtime_observation(
        verified_lease,
        observation,
    )
    foreign_runtime_by_field = {
        **observation,
        "healthcare_source_commit": "e" * 40,
    }
    foreign_lease = _lease_for_runtime_observation(foreign_runtime_by_field)
    with pytest.raises(
        runtime.ProviderDirectoryProfileRuntimeObservationError,
        match="capacity_lease_runtime_mismatch",
    ):
        runtime.assert_capacity_lease_matches_runtime_observation(
            foreign_lease,
            observation,
        )


@pytest.mark.asyncio
async def test_receipt_migration_lifecycle_and_trigger_drift(
    monkeypatch,
):
    """Install, fingerprint, downgrade, and reinstall the empty ledger."""

    async with _delta_database(monkeypatch) as (database, schema):
        await _prepare_receipt_schema(monkeypatch, database, schema)
        receipt_ref = profile.qualified_table(
            schema,
            "provider_directory_profile_capacity_preflight_receipt",
        )
        layout = await importer._profile_capacity_preflight_receipt_layout(schema)
        assert "capacity-lease-v3" in await capacity_constraint_definition(database, schema)
        assert layout["relation_oid"] > 0
        assert len(layout["main_index_pages"]) == 4
        assert layout["toastable_column_count"] >= 1

        await database.status(f"ALTER TABLE {receipt_ref} DISABLE TRIGGER pd_profile_capacity_preflight_update_guard;")
        with pytest.raises(
            importer.ProviderDirectoryArtifactBuildStale,
            match="preflight_trigger_shape_changed",
        ):
            await importer._profile_capacity_preflight_receipt_layout(schema)
        await database.status(
            f"ALTER TABLE {receipt_ref} ENABLE ALWAYS TRIGGER pd_profile_capacity_preflight_update_guard;"
        )

        await _run_receipt_migration(database, upgrade=False)
        downgraded_capacity_constraint = await capacity_constraint_definition(database, schema)
        assert "capacity-lease-v2" in downgraded_capacity_constraint
        assert "capacity-lease-v3" not in downgraded_capacity_constraint
        assert (
            await database.scalar(
                "SELECT to_regclass(:relation_ref) IS NULL;",
                relation_ref=receipt_ref,
            )
            is True
        )
        await _run_receipt_migration(database, upgrade=True)
        reinstalled = await importer._profile_capacity_preflight_receipt_layout(schema)
        assert reinstalled["relation_oid"] != layout["relation_oid"]
        assert "capacity-lease-v3" in await capacity_constraint_definition(database, schema)


async def _consume_receipt_once(
    database: Database,
    receipt_ref: str,
    receipt_sha256: str,
    now: datetime.datetime,
    suffix: str,
) -> int:
    async with database.transaction():
        return int(
            await database.status(
                f"UPDATE {receipt_ref} "
                "SET consumed_at = :consumed_at, consumed_run_id = :run_id, "
                "consumed_attestation_id = :attestation_id "
                "WHERE receipt_sha256 = :receipt_sha256 "
                "AND consumed_at IS NULL AND expires_at > :consumed_at;",
                consumed_at=now + datetime.timedelta(seconds=1),
                run_id="run_" + suffix * 32,
                attestation_id=suffix * 64,
                receipt_sha256=receipt_sha256,
            )
            or 0
        )


@pytest.mark.asyncio
async def test_receipt_single_use_concurrency_and_limits_immutability(monkeypatch):
    """Only one concurrent admission may consume the limits-bound row."""

    async with _delta_database(monkeypatch) as (database, schema):
        await _prepare_receipt_schema(monkeypatch, database, schema)
        now = datetime.datetime.now(tz=UTC).replace(microsecond=0)
        receipt_values_by_field = _receipt_values(
            uuid.uuid4().hex,
            issued_at=now,
            expires_at=now + datetime.timedelta(hours=1),
        )
        assert await _insert_receipt(database, schema, receipt_values_by_field) == 1
        receipt_ref = profile.qualified_table(
            schema,
            "provider_directory_profile_capacity_preflight_receipt",
        )
        stored_limits_sha256 = await database.scalar(
            f"SELECT limits_sha256 FROM {receipt_ref} WHERE receipt_sha256 = :receipt_sha256;",
            receipt_sha256=receipt_values_by_field["receipt_sha256"],
        )
        assert stored_limits_sha256 == receipt_values_by_field["limits_sha256"]
        with pytest.raises(Exception, match="preflight_update_invalid"):
            await database.status(
                f"UPDATE {receipt_ref} SET limits_sha256 = :changed WHERE receipt_sha256 = :receipt_sha256;",
                changed=_digest("changed-limits"),
                receipt_sha256=receipt_values_by_field["receipt_sha256"],
            )

        receipt_sha256 = receipt_values_by_field["receipt_sha256"]
        consumed_counts = await asyncio.gather(
            _consume_receipt_once(database, receipt_ref, receipt_sha256, now, "a"),
            _consume_receipt_once(database, receipt_ref, receipt_sha256, now, "b"),
        )
        assert sorted(consumed_counts) == [0, 1]
        stored = await database.first(
            f"SELECT consumed_run_id, consumed_attestation_id "
            f"FROM {receipt_ref} "
            "WHERE receipt_sha256 = :receipt_sha256;",
            receipt_sha256=receipt_values_by_field["receipt_sha256"],
        )
        assert stored.consumed_run_id in {
            "run_" + "a" * 32,
            "run_" + "b" * 32,
        }
        with pytest.raises(Exception, match="history_immutable"):
            await database.status(
                f"DELETE FROM {receipt_ref} WHERE receipt_sha256 = :receipt_sha256;",
                receipt_sha256=receipt_values_by_field["receipt_sha256"],
            )


@pytest.mark.asyncio
async def test_preflight_state_lock_serializes_concurrent_issuance(
    monkeypatch,
):
    """The real advisory/table fence admits only one issuer transaction."""

    async with _delta_database(monkeypatch) as (database, schema):
        await _create_delta_contract_tables(
            database,
            schema,
            evidence_stage="preflight_evidence_stage",
            profile_stage="preflight_profile_stage",
            affected_stage="preflight_affected_stage",
        )
        await _prepare_receipt_schema(monkeypatch, database, schema)
        locked = asyncio.Event()
        release = asyncio.Event()

        async def holder() -> None:
            async with database.transaction():
                await importer._lock_profile_capacity_preflight_state(schema)
                locked.set()
                await release.wait()

        holder_task = asyncio.create_task(holder())
        await locked.wait()
        try:
            assert (
                await database.scalar(
                    "SELECT pg_try_advisory_xact_lock(hashtextextended(:lock_key, 0));",
                    lock_key=importer._PROFILE_CAPACITY_PREFLIGHT_LOCK_KEY,
                )
                is False
            )
        finally:
            release.set()
            await holder_task


_ORDINARY_SOURCE_ID = next(
    source_id for source_id in profile_artifact.configured_profile_source_ids() if source_id.startswith("pdfhir_")
)


async def _ordinary_execution_schema(administrator, ordinary, baseline):
    """Install canonical storage as its ordinary Builder before any Source cut."""
    from sqlalchemy import text

    from process import provider_directory_profile_temp_limit as temp_limit
    from tests import cms_registry_complete_publication_postgres_support as native

    async with administrator.engine.begin() as connection:
        await connection.execute(text(f'GRANT USAGE ON SCHEMA pg_toast TO "{baseline.builder}"'))
        await connection.execute(text(f'CREATE ROLE "{baseline.guard_owner}" NOLOGIN NOSUPERUSER NOBYPASSRLS'))
        baseline.guard_owner_created = True
        await connection.execute(text(f'GRANT SET ON PARAMETER temp_file_limit TO "{baseline.guard_owner}"'))
        await connection.execute(text(f'CREATE SCHEMA healthporta_guardrails AUTHORIZATION "{baseline.owner}"'))
        function = "healthporta_guardrails.set_provider_directory_profile_temp_file_limit(bigint)"
        await connection.execute(
            text(
                "CREATE FUNCTION healthporta_guardrails.set_provider_directory_profile_temp_file_limit(limit_bytes bigint) "
                "RETURNS text LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $body$"
                + temp_limit.TEMP_FILE_LIMIT_BODY
                + "$body$"
            )
        )
        await connection.execute(text(f'ALTER FUNCTION {function} OWNER TO "{baseline.guard_owner}"'))
        await connection.execute(text(f"REVOKE ALL ON FUNCTION {function} FROM PUBLIC"))
        await connection.execute(text(f'GRANT USAGE ON SCHEMA healthporta_guardrails TO "{baseline.builder}"'))
        await connection.execute(text(f'GRANT EXECUTE ON FUNCTION {function} TO "{baseline.builder}"'))
        for extension in ("postgis", "pg_trgm", "intarray", "btree_gin"):
            await connection.execute(text(f"CREATE EXTENSION IF NOT EXISTS {extension}"))
        await connection.execute(text(f'GRANT CREATE ON DATABASE "{baseline.name}" TO "{baseline.builder}"'))
    try:
        await native._create_native_relations(ordinary, owner_role=baseline.builder)
        async with ordinary.engine.begin() as connection:
            for model in (*importer.SOURCE_MODELS, *importer.CANONICAL_RESOURCE_MODELS):
                await connection.run_sync(
                    lambda native_connection, table=model.__table__: table.create(native_connection, checkfirst=True)
                )
    finally:
        async with administrator.engine.begin() as connection:
            await connection.execute(text(f'REVOKE CREATE ON DATABASE "{baseline.name}" FROM "{baseline.builder}"'))
    async with administrator.engine.begin() as connection:
        await connection.execute(text(f'CREATE SCHEMA provider_directory_source AUTHORIZATION "{baseline.owner}"'))
        await connection.execute(text(f'GRANT USAGE ON SCHEMA provider_directory_source TO "{baseline.builder}"'))
        for name in (
            "provider_directory_endpoint_dataset",
            "provider_directory_dataset_resource",
            "provider_directory_dataset_insurance_plan",
            "provider_directory_dataset_network_plan",
            "provider_directory_dataset_affiliation_organization",
        ):
            await connection.execute(
                text(f'CREATE TABLE provider_directory_source."{name}" (LIKE mrf."{name}" INCLUDING ALL)')
            )
            await connection.execute(
                text(f'ALTER TABLE provider_directory_source."{name}" OWNER TO "{baseline.owner}"')
            )


async def _drop_ordinary_guard_role(baseline):
    """Drop only this fixture’s existing bounded-helper owner."""
    import asyncpg

    if not baseline.guard_owner_created:
        return
    cleanup = await asyncpg.connect(baseline.url.set(drivername="postgresql").render_as_string(hide_password=False))
    try:
        await cleanup.execute(f'DROP OWNED BY "{baseline.guard_owner}" CASCADE')
        await cleanup.execute(f'DROP ROLE "{baseline.guard_owner}"')
    finally:
        await cleanup.close()


async def _ordinary_execution_database(monkeypatch):
    """Yield the genuine pre-provision Builder with separate inaccessible Source storage."""
    import asyncpg
    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from tests import cms_npd_admission_postgres_support as admission
    from tests import cms_registry_complete_publication_postgres_support as native
    from tests import provider_directory_source_postgres_support as source_fixture

    token = uuid.uuid4().hex
    baseline = types.SimpleNamespace(
        url=admission._database_url(),
        name="hc_cms_admission_test_" + token,
        database_created=False,
        builder="ordinary_builder_" + token[:16],
        builder_created=False,
        owner="private_source_owner_" + token[:16],
        owner_created=False,
        guard_owner="profile_guard_" + token[:16],
        guard_owner_created=False,
    )
    administrator = ordinary = None
    admin = await asyncpg.connect(baseline.url.set(drivername="postgresql").render_as_string(hide_password=False))
    try:
        await source_fixture._create_source_component_baseline(admin, baseline)
        for user, name in ((baseline.url.username, "administrator"), (baseline.builder, "ordinary")):
            engine = create_async_engine(baseline.url.set(database=baseline.name, username=user))
            database = Database(engine=engine, session_factory=async_sessionmaker(engine, expire_on_commit=False))
            if name == "administrator":
                administrator = database
            else:
                ordinary = database
        async with administrator.engine.begin() as connection:
            await connection.execute(text(f'CREATE SCHEMA mrf AUTHORIZATION "{baseline.owner}"'))
            await connection.execute(text(f'GRANT USAGE,CREATE ON SCHEMA mrf TO "{baseline.builder}"'))
        policy = types.SimpleNamespace(schema="mrf", control_runtime_roles=(baseline.builder,))
        with monkeypatch.context() as migration_scope:
            migration_scope.setenv("HLTHPRT_DB_SCHEMA", "mrf")
            migration_scope.setenv("DB_SCHEMA", "mrf")
            await native._bootstrap_native_source(
                administrator.engine, policy, migration_prefixes=native.source_component_native_migrations()
            )
        await _ordinary_execution_schema(administrator, ordinary, baseline)
        for name, env_value in {"USER": baseline.builder, "DATABASE": baseline.name, "SCHEMA": "mrf"}.items():
            monkeypatch.setenv("HLTHPRT_DB_" + name, env_value)
        monkeypatch.setenv("DB_SCHEMA", "mrf")
        yield administrator, ordinary, baseline
    finally:
        for database in (ordinary, administrator):
            if database is not None:
                await database.disconnect()
        await admin.close()
        await source_fixture._drop_source_component_baseline(baseline)
        await _drop_ordinary_guard_role(baseline)


def _ordinary_fhir_resources():
    """One synthetic member of every reviewed ordinary resource family."""
    return [
        {
            "resourceType": "Practitioner",
            "id": "synthetic-practitioner",
            "active": True,
            "identifier": [{"system": "http://hl7.org/fhir/sid/us-npi", "value": "1000000004"}],
            "communication": [{"coding": [{"system": "urn:ietf:bcp:47", "code": "en", "display": "English"}]}],
        },
        {"resourceType": "Organization", "id": "organization", "name": "Example clinic", "active": True},
        {
            "resourceType": "Location",
            "id": "location",
            "status": "active",
            "name": "Example office",
            "address": {"line": ["1 Example St"], "city": "Boston", "state": "MA", "postalCode": "02108"},
        },
        {
            "resourceType": "PractitionerRole",
            "id": "role",
            "active": True,
            "practitioner": {"reference": "Practitioner/synthetic-practitioner"},
            "organization": {"reference": "Organization/organization"},
            "location": [{"reference": "Location/location"}],
        },
        {
            "resourceType": "InsurancePlan",
            "id": "plan",
            "name": "Example plan",
            "status": "active",
            "ownedBy": {"reference": "Organization/organization"},
        },
        {"resourceType": "Endpoint", "id": "endpoint", "status": "active", "address": "https://example.test/fhir"},
        {
            "resourceType": "HealthcareService",
            "id": "service",
            "active": True,
            "providedBy": {"reference": "Organization/organization"},
        },
        {
            "resourceType": "OrganizationAffiliation",
            "id": "affiliation",
            "active": True,
            "organization": {"reference": "Organization/organization"},
        },
    ]


def _ordinary_resource_rows():
    """Parse and hash all resource families through the real canonicalizer."""
    resource_rows = []
    for resource in _ordinary_fhir_resources():
        parsed = importer.parse_fhir_resource(_ORDINARY_SOURCE_ID, resource, run_id="synthetic-directory-run")
        assert parsed is not None
        resource_rows.extend(
            importer._endpoint_dataset_resource_rows(
                parsed[0],
                [parsed[1]],
                dataset_id="synthetic-dataset",
                resource_hash_contract=importer.TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT,
            )
        )
    return resource_rows


async def _ordinary_canonical_input(ordinary):
    """Seed historical canonical input; recompute its real stored payload commitment."""
    from tests.provider_profile_detail_load_postgres_support import _seed_directory_source

    async with ordinary.transaction() as session:
        await _seed_directory_source(session)
    await ordinary.status(
        "UPDATE mrf.provider_directory_source SET source_id=:source WHERE source_id='synthetic-directory'",
        source=_ORDINARY_SOURCE_ID,
    )
    resource_rows = _ordinary_resource_rows()
    kinds = tuple(sorted(importer.RESOURCE_MODELS_BY_TYPE))
    assert sorted(resource_row["resource_type"] for resource_row in resource_rows) == list(kinds)
    metadata = {
        "source_ids": [_ORDINARY_SOURCE_ID],
        "selected_resources": list(kinds),
        importer.RESOURCE_HASH_CONTRACT_METADATA_KEY: importer.TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT,
    }
    await ordinary.status(
        "INSERT INTO mrf.provider_directory_endpoint_dataset "
        "(dataset_id,endpoint_id,acquisition_root_run_id,status,is_current,resource_count,created_at,publication_metadata_json) "
        "VALUES('synthetic-dataset','synthetic-endpoint','synthetic-directory-run','acquiring',false,0,now(),CAST(:metadata AS jsonb))",
        metadata=json.dumps(metadata),
    )
    for resource_row in resource_rows:
        await ordinary.status(
            "INSERT INTO mrf.provider_directory_dataset_resource "
            "(dataset_id,resource_type,resource_id,payload_hash,payload_json) "
            "VALUES(:dataset,:kind,:resource,:digest,CAST(:payload AS jsonb))",
            dataset=resource_row["dataset_id"],
            kind=resource_row["resource_type"],
            resource=resource_row["resource_id"],
            digest=resource_row["payload_hash"],
            payload=json.dumps(resource_row["payload_json"], default=str),
        )
    proof = await importer._endpoint_dataset_content_proof(
        ordinary,
        "synthetic-dataset",
        kinds,
        verify_payload_hashes=True,
        resource_hash_contract=importer.TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT,
    )
    assert proof.resource_count == 8 and set(proof.resource_counts.values()) == {1}
    await ordinary.status(
        "UPDATE mrf.provider_directory_endpoint_dataset "
        "SET status='validated',resource_count=:count,dataset_hash=:digest,validated_at=now() "
        "WHERE dataset_id='synthetic-dataset'",
        count=proof.resource_count,
        digest=proof.dataset_hash,
    )
    await ordinary.status(
        "UPDATE mrf.provider_directory_endpoint_dataset "
        "SET status='published',is_current=true,published_at=now() WHERE dataset_id='synthetic-dataset'"
    )


async def _ordinary_registered_execution(ordinary, monkeypatch):
    """Register actual published canonical data without desired-CMS authority."""
    from process import provider_directory_profile_selection as selection
    from tests.provider_profile_detail_load_postgres_support import _insert
    from tests.test_provider_directory_profile_selection_attestation import _variant_registry_rows

    async with ordinary.transaction() as session:
        for fields in _variant_registry_rows():
            await _insert(
                session,
                importer.ProviderDirectoryAPIEndpoint,
                endpoint_id=fields["endpoint_id"],
                canonical_api_base="https://example.test/" + fields["endpoint_id"],
                credential_descriptor_hash=fields["endpoint_id"],
                endpoint_signature_hash=fields["endpoint_id"],
                credential_descriptor_json={},
                endpoint_signature_json={},
            )
            await _insert(
                session,
                importer.ProviderDirectorySource,
                **fields,
                requires_registration=False,
                requires_api_key=False,
                auth_type="none",
                metadata_json={importer.PROVIDER_DIRECTORY_CONFIGURED_ENDPOINT_METADATA_KEY: fields["endpoint_id"]},
            )
    catalog_by_field = {
        "catalog_digest": "a" * 64,
        "items": [
            {"runnable": True, "profile_enabled": True, "source_ids": [_ORDINARY_SOURCE_ID]},
        ],
    }
    monkeypatch.setattr(importer, "_provider_directory_profile_selection_catalog", lambda: catalog_by_field)
    request = await selection.current_profile_selection_request(catalog_by_field)
    attestation = selection.validated_profile_selection_attestation(
        await selection.attest_profile_selection(request, catalog_by_field)
    )
    assert attestation.desired_cms_dataset is None
    assert [pair["source_id"] for pair in attestation.pairs] == [_ORDINARY_SOURCE_ID]
    return selection.ProviderDirectoryProfileExecution(attestation, 2, capacity_attestation={})


async def _ordinary_legacy_incumbent(ordinary, execution):
    """Seed supported historical serving data, with no invented capacity lease."""
    from tests.provider_profile_detail_load_postgres_support import _seed_projected_profile

    async with ordinary.transaction() as session:
        await _seed_projected_profile(session, "mrf")
    await ordinary.status(
        "UPDATE mrf.provider_directory_profile SET source_ids=ARRAY[:source]", source=_ORDINARY_SOURCE_ID
    )
    await ordinary.status(
        "INSERT INTO mrf.provider_directory_profile_evidence "
        "(evidence_key,npi,fact_type,fact_key,value_json,source_id,endpoint_id,dataset_id,resource_type,resource_id,active,observed_at) "
        "VALUES(:key,1000000004,'language',:key,'{\"code\":\"en\"}', :source,'synthetic-endpoint',"
        "'synthetic-dataset','Practitioner','synthetic-practitioner',true,now())",
        key="a" * 32,
        source=_ORDINARY_SOURCE_ID,
    )
    _selected, _retained, contexts = await importer._provider_directory_profile_scope_source_ids(
        "mrf", {_ORDINARY_SOURCE_ID}, expected_source_context_digest=execution.attestation.source_context_digest
    )
    attestation = execution.attestation
    selection_result_by_field = {
        "operation": "publish",
        "generation": 1,
        "proof_id": attestation.proof_id,
        "authority_revision": attestation.authority_revision,
        "profile_schema_version": attestation.profile_schema_version,
        "profile_strategy_version": attestation.profile_strategy_version,
    }
    await importer._insert_profile_adoption(
        "mrf",
        importer._ProfileAdoptionCandidate(
            generation_id="pdprofile_" + "1" * 32,
            selection_result=selection_result_by_field,
            source_vector=((_ORDINARY_SOURCE_ID, "synthetic-dataset"),),
            source_context_vector=importer._provider_directory_profile_source_context_vector(contexts),
            profile_as_of="2026-07-30",
            profile_rows=1,
            evidence_rows=1,
            profile_target_oid=await ordinary.scalar("SELECT 'mrf.provider_directory_profile'::regclass::oid::bigint"),
            evidence_target_oid=await ordinary.scalar(
                "SELECT 'mrf.provider_directory_profile_evidence'::regclass::oid::bigint"
            ),
            published_at=datetime.datetime.now(UTC),
            executable_plan_hash=importer._provider_directory_profile_adopted_plan_hash(selection_result_by_field),
        ),
    )


async def _assert_ordinary_execution_released(administrator, ordinary, baseline, source_before):
    """Both outcomes release task contexts and preserve every private input."""
    from process import provider_directory_generic_source_policy as source_policy

    assert source_policy._FACTORY.get() is None and source_policy._PUBLISHER.get() is None
    assert importer._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.get() is None
    assert importer._PROVIDER_DIRECTORY_PROFILE_SELECTION_EXECUTION.get() is None
    assert ordinary._transaction_binding() is None
    assert (
        await ordinary.scalar(
            "SELECT count(*) FROM pg_locks l JOIN pg_stat_activity a ON a.pid=l.pid WHERE l.locktype='advisory' AND a.usename=:actor",
            actor=baseline.builder,
        )
        == 0
    )
    assert await _ordinary_input_state(ordinary) == source_before
    for name in (
        "provider_directory_endpoint_dataset",
        "provider_directory_dataset_resource",
        "provider_directory_dataset_insurance_plan",
        "provider_directory_dataset_network_plan",
        "provider_directory_dataset_affiliation_organization",
    ):
        assert await administrator.scalar("SELECT count(*) FROM provider_directory_source." + name) == 0


async def _assert_ordinary_execution_actor(ordinary, baseline):
    """Verify canonical ownership and the observed ordinary metadata capability."""
    from process import provider_directory_generic_source_policy as source_policy
    from process import provider_directory_profile_selection_snapshot as snapshot

    actor = await ordinary.first(
        "SELECT session_user::text AS actor,current_user::text AS current,"
        "pg_has_role(current_user,:source_owner,'MEMBER') AS source_authority",
        source_owner=baseline.owner,
    )
    assert actor.actor == actor.current == baseline.builder and actor.source_authority is False
    assert source_policy.is_source_storage_required() is False
    assert source_policy._FACTORY.get() is None and source_policy._PUBLISHER.get() is None
    names = [
        "provider_directory_api_endpoint",
        "provider_directory_source",
        "provider_directory_endpoint_dataset",
        *snapshot.UHC_FLEX_PROFILE_SELECTION_LOCK_RELATIONS,
    ]
    ownership = await ordinary.all(
        "SELECT c.relname,pg_get_userbyid(c.relowner) AS owner "
        "FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
        "WHERE n.nspname='mrf' AND c.relname=ANY(CAST(:names AS text[])) ORDER BY c.relname",
        names=names,
    )
    assert len(ownership) == len(names) == 22
    assert all(owned_relation.owner == baseline.builder for owned_relation in ownership)
    assert await ordinary.scalar("SELECT NOT rolsuper AND NOT rolbypassrls FROM pg_roles WHERE rolname=current_user")
    assert await ordinary.scalar(
        "SELECT NOT has_table_privilege(current_user,"
        "'provider_directory_source.provider_directory_endpoint_dataset','SELECT,INSERT,UPDATE,DELETE')"
    )
    from process import provider_directory_profile_temp_limit as temp_limit

    assert await temp_limit.require_temp_file_limit_capability(ordinary) == "bounded"
    assert await ordinary.scalar("SELECT NOT has_parameter_privilege(current_user,'temp_file_limit','SET')")


async def _ordinary_execution_inputs(monkeypatch, tmp_path, ordinary, baseline):
    """Bind only the ordinary login and create real current selection."""
    import subprocess

    from process import provider_directory_profile_selection as selection
    from process import provider_directory_profile_selection_snapshot as snapshot
    from process.ext import utils

    for module in (importer, selection, snapshot, utils):
        monkeypatch.setattr(module, "db", ordinary)
    await _assert_ordinary_execution_actor(ordinary, baseline)
    await _ordinary_canonical_input(ordinary)
    identity = tmp_path / "native-test-source-commit"
    identity.write_text(subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip() + "\n")
    identity.chmod(0o444)
    monkeypatch.setattr(runtime, "PROFILE_RUNTIME_SOURCE_COMMIT_FILE", identity)
    monkeypatch.setattr(runtime, "_BUILD_IDENTITY_UID", os.getuid())
    monkeypatch.setattr(runtime, "_BUILD_IDENTITY_GID", os.getgid())
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "dev-node")
    execution = await _ordinary_registered_execution(ordinary, monkeypatch)
    await _ordinary_legacy_incumbent(ordinary, execution)
    return execution


async def _ordinary_signed_request(monkeypatch, ordinary, baseline, execution, incumbent):
    """Sign actual native preflight, then persist the exact worker request."""
    from dataclasses import replace

    from process import provider_directory_generic_source_policy as source_policy
    from process import provider_directory_profile_capacity_runtime as capacity_runtime
    from tests import provider_directory_profile_capacity_signing_guard_test_support as signing
    from tests.test_network_cms_registry_complete_publication_postgres import _native_profile_lease
    from tests.test_provider_directory_profile_capacity_trust import _trust_payload

    try:
        with monkeypatch.context() as signing_scope:
            _guard, envelope, _timing = await _native_profile_lease(signing_scope, execution)
            limits = os.environ[capacity_runtime.CAPACITY_LIMITS_ENV]
    except Exception:
        assert (
            await ordinary.scalar("SELECT jsonb_agg(to_jsonb(profile)) FROM mrf.provider_directory_profile profile")
            == incumbent
        )
        for table in (
            "provider_directory_profile_capacity_preflight_receipt",
            "provider_directory_profile_capacity_lease_consumption",
        ):
            assert await ordinary.scalar("SELECT count(*) FROM mrf." + table) == 0
        assert source_policy._FACTORY.get() is None and source_policy._PUBLISHER.get() is None
        raise
    assert envelope["lease"]["database_name"] == baseline.name
    body = envelope["lease"]
    trust = _trust_payload(
        **{name: body[name] for name in ("database_system_identifier", "database_oid", "database_name")}
    )
    monkeypatch.setenv(capacity_runtime.CAPACITY_LIMITS_ENV, limits)
    monkeypatch.setenv(capacity_runtime.CAPACITY_TRUST_ENV, json.dumps(trust))
    execution = replace(execution, capacity_attestation=envelope)
    task_by_field = {
        **signing._execution_by_field(execution),
        "provider_directory_profile_capacity_attestation": envelope,
    }
    run_id = "run_" + uuid.uuid4().hex
    await ordinary.status(
        "INSERT INTO mrf.import_run(run_id,engine,importer,status,params,metrics) "
        "VALUES(:run,'test-engine','provider-directory-fhir','running',CAST(:params AS jsonb),'{}')",
        run=run_id,
        params=json.dumps(task_by_field),
    )
    return task_by_field, run_id


async def _ordinary_execution_rollback(monkeypatch, ordinary, task_by_field, run_id, incumbent):
    """Fail only after real native cutover writes, before transaction commit."""
    original_apply = importer._apply_locked_provider_directory_artifact_bundle
    sentinel = RuntimeError("synthetic cutover rollback")

    async def rollback_after_apply(*args, **kwargs):
        await original_apply(*args, **kwargs)
        assert await ordinary.scalar(
            "SELECT generation_id <> :old FROM mrf.provider_directory_profile_serving_generation WHERE singleton_key='global'",
            old="pdprofile_" + "1" * 32,
        )
        raise sentinel

    with monkeypatch.context() as failure:
        failure.setattr(importer, "_apply_locked_provider_directory_artifact_bundle", rollback_after_apply)
        with pytest.raises(RuntimeError, match="synthetic cutover rollback") as raised:
            await importer.process_data({"context": {}, "control_run_id": run_id}, task_by_field)
        assert raised.value is sentinel
    assert (
        await ordinary.scalar("SELECT jsonb_agg(to_jsonb(profile)) FROM mrf.provider_directory_profile profile")
        == incumbent
    )
    assert (
        await ordinary.scalar(
            "SELECT generation_id FROM mrf.provider_directory_profile_serving_generation WHERE singleton_key='global'"
        )
        == "pdprofile_" + "1" * 32
    )
    assert (
        await ordinary.scalar(
            "SELECT count(*) FROM mrf.provider_directory_profile_capacity_lease_consumption WHERE run_id=:run",
            run=run_id,
        )
        == 1
    )
    assert await ordinary.scalar("SELECT count(*) FROM mrf.provider_directory_profile_delta_receipt") == 0


async def _ordinary_execution_success(ordinary, task_by_field, run_id):
    """Read back actual publication and reuse its single committed admission."""
    publication_metrics = await importer.process_data({"context": {}, "control_run_id": run_id}, task_by_field)
    assert publication_metrics["profile"]["generation_id"] != "pdprofile_" + "1" * 32
    assert publication_metrics["profile"]["profile_rows"] > 0 and publication_metrics["profile"]["evidence_rows"] > 0
    assert (
        await ordinary.scalar(
            "SELECT count(*) FROM mrf.provider_directory_profile_capacity_lease_consumption WHERE run_id=:run",
            run=run_id,
        )
        == 1
    )
    assert (
        await ordinary.scalar(
            "SELECT count(*) FROM mrf.provider_directory_profile_serving_generation WHERE singleton_key='global'"
        )
        == 1
    )
    assert await ordinary.scalar("SELECT count(*) FROM mrf.provider_directory_profile WHERE npi=1000000004") == 1
    assert (
        await ordinary.scalar(
            "SELECT consumed_run_id FROM mrf.provider_directory_profile_capacity_preflight_receipt WHERE receipt_sha256=:receipt",
            receipt=task_by_field["provider_directory_profile_capacity_attestation"]["lease"]["nonce"],
        )
        == run_id
    )
    assert await ordinary.scalar("SELECT source_ids FROM mrf.provider_directory_profile WHERE npi=1000000004") == [
        _ORDINARY_SOURCE_ID
    ]
    replay = await importer.process_data({"context": {}, "control_run_id": run_id}, task_by_field)
    assert replay["profile"]["generation_id"] == publication_metrics["profile"]["generation_id"]
    assert (
        await ordinary.scalar(
            "SELECT count(*) FROM mrf.provider_directory_profile_capacity_lease_consumption WHERE run_id=:run",
            run=run_id,
        )
        == 1
    )


async def _ordinary_input_state(ordinary):
    """Freeze canonical header and resource bytes independently of worker output."""
    return await ordinary.scalar(
        "SELECT jsonb_build_object('header',(SELECT to_jsonb(d) FROM mrf.provider_directory_endpoint_dataset d WHERE dataset_id='synthetic-dataset'),"
        "'resources',(SELECT jsonb_agg(to_jsonb(r) ORDER BY resource_type,resource_id) FROM mrf.provider_directory_dataset_resource r WHERE dataset_id='synthetic-dataset'))"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("rollback_cutover", [False, True])
async def test_ordinary_profile_executes_signed_native_capacity(monkeypatch, tmp_path, rollback_cutover):
    """Exercise the real ordinary successor and transactional rollback paths."""
    from contextlib import asynccontextmanager

    async with asynccontextmanager(_ordinary_execution_database)(monkeypatch) as (administrator, ordinary, baseline):
        execution = await _ordinary_execution_inputs(monkeypatch, tmp_path, ordinary, baseline)
        incumbent = await ordinary.scalar(
            "SELECT jsonb_agg(to_jsonb(profile)) FROM mrf.provider_directory_profile profile"
        )
        task_by_field, run_id = await _ordinary_signed_request(monkeypatch, ordinary, baseline, execution, incumbent)
        source_before = await _ordinary_input_state(ordinary)
        if rollback_cutover:
            await _ordinary_execution_rollback(monkeypatch, ordinary, task_by_field, run_id, incumbent)
        else:
            await _ordinary_execution_success(ordinary, task_by_field, run_id)
        await _assert_ordinary_execution_released(administrator, ordinary, baseline, source_before)
