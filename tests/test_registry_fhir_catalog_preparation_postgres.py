# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Operator preparation grants, restricted catalog logins and atomic drift refusal."""

import json
from datetime import date
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import asyncpg
import pytest

from db.registry_fhir_catalog import PROJECTOR_VERSION, SNAPSHOT_COLUMNS
from process.network_membership_writer_closure import writer_role_names
from process.registry_fhir_catalog_custody import FHIRCatalogTarget, freeze_fhir_catalog_snapshot
from process.registry_fhir_catalog_fingerprints import approved_fhir_catalog_fingerprints
from process.registry_fhir_catalog_identity import FHIRNetworkIdentityInput, allocate_fhir_catalog_network_ids
from process.registry_fhir_catalog_permissions import (
    install_fhir_catalog_preparation_permissions,
    install_fhir_catalog_publication_permissions,
    verify_fhir_catalog_preparation_permissions,
    verify_fhir_catalog_publication_permissions,
)
from process.registry_fhir_catalog_publication import FHIRCatalogPublicationRuntime
from process.registry_fhir_catalog_store import create_fhir_catalog_candidate, create_fhir_catalog_indexes
from process.registry_fhir_catalog_validation import metadata_generation
from process.registry_publication_consumer import RegistryPublicationConsumerConfig
from tests.test_network_custom_address_source_postgres import custom_db as custom_db
from tests.test_network_serving_schema_postgres import serving_schema as serving_schema
from tests.test_registry_publication_worker_permissions_postgres import _login
from tests.test_registry_publication_worker_permissions_postgres import worker_permissions as worker_permissions

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def catalog_preparation(worker_permissions):
    worker = worker_permissions
    connection = worker.fixture.connection
    roles_by_kind = {
        kind: "catalog_prepare_" + kind + "_" + uuid4().hex for kind in ("loader", "sealer", "helper", "bridge")
    }
    created_roles, schemas = [], []
    try:
        for kind, name in roles_by_kind.items():
            login = "NOLOGIN" if kind in {"helper", "bridge"} else f"LOGIN PASSWORD '{worker.role_password}'"
            await connection.execute(f'CREATE ROLE "{name}" {login}')
            created_roles.append(name)
        roles_by_kind = {**worker.roles_by_kind, **roles_by_kind}
        config = RegistryPublicationConsumerConfig(
            worker.fixture.control_schema,
            roles_by_kind["lifecycle"],
            roles_by_kind["execution"],
            roles_by_kind["api"],
            roles_by_kind["queue"],
            roles_by_kind["physical"],
            (roles_by_kind["loader"],),
            (roles_by_kind["api"], roles_by_kind["execution"], roles_by_kind["loader"]),
            metadata_runtime=FHIRCatalogPublicationRuntime(AsyncMock(), AsyncMock(), AsyncMock()),
            metadata_only=True,
        )
        # Model the additive migration before the API has read access to its new ledger.
        await connection.execute(
            f'REVOKE SELECT ON "{config.control_schema}".registry_fhir_catalog_snapshot FROM "{roles_by_kind["api"]}"'
        )
        yield SimpleNamespace(
            fixture=worker.fixture,
            seed=worker.seed,
            roles_by_kind=roles_by_kind,
            role_password=worker.role_password,
            config=config,
            options={"loader_role": roles_by_kind["loader"], "sealer_role": roles_by_kind["sealer"]},
            schemas=schemas,
        )
    finally:
        for schema in reversed(schemas):
            await connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            assert await connection.fetchval("SELECT to_regnamespace($1)", schema) is None
        for role in reversed(created_roles):
            await connection.execute(f'DROP OWNED BY "{role}"')
            await connection.execute(f'DROP ROLE "{role}"')


async def _install(fixture):
    async with fixture.fixture.connection.transaction():
        return await install_fhir_catalog_preparation_permissions(
            fixture.fixture.connection, fixture.config, **fixture.options
        )


async def _native_state(fixture):
    return await fixture.fixture.connection.fetchval(
        """SELECT jsonb_build_object(
        'tables',(SELECT jsonb_agg(jsonb_build_array(c.oid,c.relowner,c.relacl::text,
          (SELECT jsonb_agg(jsonb_build_array(attnum,attacl::text) ORDER BY attnum)
           FROM pg_attribute WHERE attrelid=c.oid AND attnum>0)) ORDER BY c.oid)
          FROM pg_class c WHERE c.relnamespace=to_regnamespace($1)),
        'schema',(SELECT nspacl::text FROM pg_namespace WHERE oid=to_regnamespace($1)),
        'database',(SELECT datacl::text FROM pg_database WHERE datname=current_database()),
        'function',(SELECT proacl::text FROM pg_proc WHERE oid='pg_catalog.pg_control_system()'::regprocedure),
        'memberships',(SELECT jsonb_agg(to_jsonb(m) ORDER BY roleid,member,grantor) FROM pg_auth_members m))::text""",
        fixture.config.control_schema,
    )


async def _api_grants(fixture):
    return await fixture.fixture.connection.fetch(
        """SELECT c.relname,NULL::text AS column_name,a.privilege_type,a.is_grantable
        FROM pg_class c CROSS JOIN LATERAL aclexplode(c.relacl) a
        WHERE c.relnamespace=to_regnamespace($1) AND a.grantee=(SELECT oid FROM pg_roles WHERE rolname=$2)
        UNION ALL SELECT c.relname,att.attname,a.privilege_type,a.is_grantable
        FROM pg_class c JOIN pg_attribute att ON att.attrelid=c.oid
        CROSS JOIN LATERAL aclexplode(att.attacl) a
        WHERE c.relnamespace=to_regnamespace($1) AND a.grantee=(SELECT oid FROM pg_roles WHERE rolname=$2)
        ORDER BY 1,2,3""",
        fixture.config.control_schema,
        fixture.config.api_role,
    )


async def _load_empty_catalog(fixture):
    """Allocate one identity and prepare an empty candidate as its real Loader."""
    config, roles_by_kind = fixture.config, fixture.roles_by_kind
    async with _login(fixture, "loader") as loader, loader.transaction():
        await verify_fhir_catalog_preparation_permissions(
            loader, config, **fixture.options, role=roles_by_kind["loader"]
        )
        allocation = await allocate_fhir_catalog_network_ids(
            loader,
            (FHIRNetworkIdentityInput("network-one", "endpoint-one", ("source-one",), "edition-one"),),
            approved_revision=fixture.seed.revision,
            control_schema=config.control_schema,
        )
        assert allocation["created_networks"] == 1
        candidate = uuid4()
        catalog_target = FHIRCatalogTarget(
            str(uuid4()), str(uuid4()), str(uuid4()), str(candidate), "fhir_catalog_" + candidate.hex
        )
        fixture.schemas.append(catalog_target.schema_name)
        await create_fhir_catalog_candidate(
            loader, catalog_target, loader_role=roles_by_kind["loader"], control_schema=config.control_schema
        )
        await create_fhir_catalog_indexes(
            loader, catalog_target, loader_role=roles_by_kind["loader"], control_schema=config.control_schema
        )
        fingerprints = (
            await approved_fhir_catalog_fingerprints(
                loader,
                (fixture.seed.revision,),
                control_schema=config.control_schema,
            )
        )[fixture.seed.revision]
    snapshot_by_field = {
        "schema_name": catalog_target.schema_name,
        "network_generation": None,
        "manifest_sha256": None,
        "metadata_approved_revision": fixture.seed.revision,
        "membership_mapping_sha256": fingerprints["membership"],
        "mapping_sha256": fingerprints["metadata"],
        "standing_grant_sha256": "a" * 64,
        "source_vector_json": [],
        "projector_version": PROJECTOR_VERSION,
        "as_of": date(2026, 1, 1),
    }
    snapshot_by_field["metadata_generation"] = metadata_generation(snapshot_by_field)
    return catalog_target, snapshot_by_field


async def _seal_empty_catalog(fixture, catalog_target, snapshot_by_field):
    """Close and retain the candidate using its separate direct Sealer LOGIN."""
    config, roles_by_kind = fixture.config, fixture.roles_by_kind
    async with _login(fixture, "sealer") as sealer, sealer.transaction():
        await verify_fhir_catalog_preparation_permissions(
            sealer, config, **fixture.options, role=roles_by_kind["sealer"]
        )
        assert await sealer.fetchval("SELECT (pg_catalog.pg_control_system()).system_identifier::text")
        custody = await freeze_fhir_catalog_snapshot(
            sealer,
            catalog_target,
            writer_roles=writer_role_names(roles_by_kind["physical"], config.loader_roles, config.reader_roles),
            expected_snapshot=snapshot_by_field,
            control_schema=config.control_schema,
        )
        snapshot_by_field.update(custody_json=custody, accounting_json=custody["validation"]["tables"])
        columns = tuple(column for column in SNAPSHOT_COLUMNS if column != "published_at")
        await sealer.execute(
            f'INSERT INTO "{config.control_schema}".registry_fhir_catalog_snapshot({",".join(columns)}) '
            f"VALUES({','.join('$' + str(index) for index in range(1, len(columns) + 1))})",
            *(
                json.dumps(snapshot_by_field[column])
                if isinstance(snapshot_by_field[column], (dict, list))
                else snapshot_by_field[column]
                for column in columns
            ),
        )


async def test_additive_preparation_works_with_restricted_logins_and_keeps_api_grants(catalog_preparation):
    fixture = catalog_preparation
    connection, config, roles_by_kind = fixture.fixture.connection, fixture.config, fixture.roles_by_kind
    initial_api_grants = {tuple(grant.values()) for grant in await _api_grants(fixture)}
    receipt = await _install(fixture)
    installed = await _native_state(fixture)
    assert await _install(fixture) == receipt
    assert await _native_state(fixture) == installed
    assert {tuple(grant.values()) for grant in await _api_grants(fixture)} == initial_api_grants | {
        ("registry_fhir_catalog_snapshot", None, "SELECT", False)
    }
    async with connection.transaction():
        await install_fhir_catalog_publication_permissions(connection, config)
    catalog_target, snapshot_by_field = await _load_empty_catalog(fixture)
    await _seal_empty_catalog(fixture, catalog_target, snapshot_by_field)
    async with _login(fixture, "api") as api:
        assert await api.fetchval(f'SELECT count(*) FROM "{config.control_schema}".registry_fhir_catalog_snapshot') == 1
    for kind in ("loader", "sealer"):
        async with _login(fixture, kind) as actor:
            for statement in (
                f'UPDATE "{config.control_schema}".registry_fhir_catalog_snapshot SET published_at=now() WHERE false',
                f'UPDATE "{config.control_schema}".registry_revision_control SET approved_revision=approved_revision WHERE false',
                f'UPDATE "{config.control_schema}".registry_publication_request SET state=state WHERE false',
                f'INSERT INTO "{config.control_schema}".network_registry_alias(created_at) SELECT now() WHERE false',
                f'SELECT * FROM "{fixture.fixture.source_schema}".npi',
                f'CREATE TABLE "{config.control_schema}".unapproved(id integer)',
            ):
                with pytest.raises(asyncpg.InsufficientPrivilegeError):
                    await actor.execute(statement)
    for kind in ("loader", "api", "lifecycle", "execution"):
        async with _login(fixture, kind) as actor:
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await actor.execute(f'SET ROLE "{roles_by_kind["physical"]}"')


@pytest.mark.parametrize(
    "drift", ["partial_api", "grant_option", "extra_read", "set_write", "native_grant", "owner_inherit", "admin"]
)
async def test_preparation_refuses_drift_without_changing_native_state(catalog_preparation, drift):
    fixture = catalog_preparation
    connection, config, roles_by_kind = fixture.fixture.connection, fixture.config, fixture.roles_by_kind
    schema = '"' + config.control_schema + '"'
    statements_by_drift = {
        "partial_api": (
            f'GRANT SELECT(metadata_generation) ON {schema}.registry_fhir_catalog_snapshot TO "{roles_by_kind["api"]}"',
        ),
        "grant_option": (
            f'GRANT INSERT(allocation_key) ON {schema}.network_registry_identity TO "{roles_by_kind["loader"]}" WITH GRANT OPTION',
        ),
        "extra_read": (f'GRANT SELECT ON {schema}.registry_record_history TO "{roles_by_kind["loader"]}"',),
        "set_write": (
            f'GRANT UPDATE(reason) ON {schema}.registry_approval_history TO "{roles_by_kind["helper"]}"',
            f'GRANT "{roles_by_kind["helper"]}" TO "{roles_by_kind["loader"]}" WITH ADMIN FALSE, INHERIT FALSE, SET TRUE',
        ),
        "native_grant": (
            f'GRANT EXECUTE ON FUNCTION pg_catalog.pg_control_system() TO "{roles_by_kind["helper"]}" WITH GRANT OPTION',
            f'GRANT "{roles_by_kind["helper"]}" TO "{roles_by_kind["sealer"]}" WITH ADMIN FALSE, INHERIT FALSE, SET TRUE',
        ),
        "owner_inherit": (
            f'GRANT "{roles_by_kind["physical"]}" TO "{roles_by_kind["sealer"]}" WITH ADMIN FALSE, INHERIT TRUE, SET TRUE',
        ),
        "admin": (
            f'GRANT "{roles_by_kind["loader"]}" TO "{roles_by_kind["sealer"]}" WITH ADMIN TRUE, INHERIT TRUE, SET FALSE',
        ),
    }
    for statement in statements_by_drift[drift]:
        await connection.execute(statement)
    before = await _native_state(fixture)
    async with connection.transaction():
        with pytest.raises(ValueError):
            await install_fhir_catalog_preparation_permissions(connection, config, **fixture.options)
        assert await _native_state(fixture) == before


@pytest.mark.parametrize("missing", ["owner_edge", "loader_read", "loader_create", "owner_create"])
async def test_installed_preparation_refuses_missing_required_authority_without_repair(catalog_preparation, missing):
    fixture = catalog_preparation
    connection, config, roles = fixture.fixture.connection, fixture.config, fixture.roles_by_kind
    await _install(fixture)
    database = await connection.fetchval("SELECT quote_ident(current_database())")
    statements_by_missing = {
        "owner_edge": f'REVOKE "{roles["physical"]}" FROM "{roles["sealer"]}"',
        "loader_read": f'REVOKE SELECT ON "{config.control_schema}".registry_fhir_catalog_snapshot FROM "{roles["loader"]}"',
        "loader_create": f'REVOKE CREATE ON DATABASE {database} FROM "{roles["loader"]}"',
        "owner_create": f'REVOKE CREATE ON DATABASE {database} FROM "{roles["physical"]}"',
    }
    await connection.execute(statements_by_missing[missing])
    before = await _native_state(fixture)
    async with connection.transaction():
        with pytest.raises(ValueError, match="registry_fhir_catalog_preparation_"):
            await verify_fhir_catalog_preparation_permissions(connection, config, **fixture.options)
    assert await _native_state(fixture) == before


@pytest.mark.parametrize("operation", ["install", "verify", "claimed_login"])
async def test_preparation_requires_its_transaction_and_actual_login_before_changing_state(
    catalog_preparation, operation
):
    fixture = catalog_preparation
    connection, config = fixture.fixture.connection, fixture.config
    before = await _native_state(fixture)
    with pytest.raises(ValueError, match="transaction_required|direct_login_required"):
        if operation == "install":
            await install_fhir_catalog_preparation_permissions(connection, config, **fixture.options)
        else:
            await verify_fhir_catalog_preparation_permissions(
                connection,
                config,
                **fixture.options,
                role=fixture.roles_by_kind["loader"] if operation == "claimed_login" else None,
            )
    assert await _native_state(fixture) == before


async def test_preparation_rolls_back_all_grants_with_outer_transaction(catalog_preparation):
    fixture = catalog_preparation
    connection = fixture.fixture.connection
    before = await _native_state(fixture)
    with pytest.raises(RuntimeError, match="operator rollback"):
        async with connection.transaction():
            await install_fhir_catalog_preparation_permissions(connection, fixture.config, **fixture.options)
            raise RuntimeError("operator rollback")
    assert await _native_state(fixture) == before


async def test_metadata_publication_rejects_set_only_sequence_writes_atomically(catalog_preparation):
    fixture = catalog_preparation
    connection, config, roles_by_kind = fixture.fixture.connection, fixture.config, fixture.roles_by_kind
    await _install(fixture)
    async with connection.transaction():
        await install_fhir_catalog_publication_permissions(connection, config)
    await connection.execute(
        f'GRANT UPDATE ON SEQUENCE "{config.control_schema}".network_registry_identity_network_id_seq '
        f'TO "{roles_by_kind["helper"]}"'
    )
    await connection.execute(
        f'GRANT "{roles_by_kind["helper"]}" TO "{roles_by_kind["execution"]}" WITH ADMIN FALSE, INHERIT FALSE, SET TRUE'
    )
    before = await _native_state(fixture)
    async with connection.transaction():
        with pytest.raises(ValueError, match="publication_sequence_invalid"):
            await install_fhir_catalog_publication_permissions(connection, config)
        assert await _native_state(fixture) == before
    async with _login(fixture, "execution") as execution, execution.transaction(readonly=True):
        with pytest.raises(ValueError, match="publication_sequence_invalid"):
            await verify_fhir_catalog_publication_permissions(execution, config, role=roles_by_kind["execution"])


@pytest.mark.parametrize("reach", ["direct", "inherited", "set_only", "transitive"])
async def test_metadata_publication_rejects_grantable_pointer_atomically(catalog_preparation, reach):
    fixture = catalog_preparation
    connection, config, roles_by_kind = fixture.fixture.connection, fixture.config, fixture.roles_by_kind
    await _install(fixture)
    async with connection.transaction():
        await install_fhir_catalog_publication_permissions(connection, config)
    grantee = roles_by_kind["execution"] if reach == "direct" else roles_by_kind["helper"]
    if reach != "direct":
        reachable = grantee
        if reach == "transitive":
            reachable = roles_by_kind["bridge"]
            await connection.execute(f'GRANT "{grantee}" TO "{reachable}" WITH ADMIN FALSE, INHERIT TRUE, SET FALSE')
        membership = "INHERIT TRUE, SET FALSE" if reach == "inherited" else "INHERIT FALSE, SET TRUE"
        await connection.execute(
            f'GRANT "{reachable}" TO "{roles_by_kind["execution"]}" WITH ADMIN FALSE, {membership}'
        )
    pointer = f'"{config.control_schema}".network_serving_control'
    grant = f'GRANT UPDATE(metadata_generation) ON {pointer} TO "{grantee}"'
    await connection.execute(grant)
    async with _login(fixture, "execution") as execution:
        async with execution.transaction(readonly=True):
            await verify_fhir_catalog_publication_permissions(execution, config, role=roles_by_kind["execution"])
        await connection.execute(grant + " WITH GRANT OPTION")
        assert await connection.fetchval(
            "SELECT has_column_privilege($1::name,$2::text,'metadata_generation','UPDATE WITH GRANT OPTION')",
            roles_by_kind["execution"],
            pointer,
        ) == (reach in {"direct", "inherited"})
        async with execution.transaction(readonly=True):
            with pytest.raises(ValueError, match="registry_fhir_catalog_permissions_invalid"):
                await verify_fhir_catalog_publication_permissions(execution, config, role=roles_by_kind["execution"])
    await connection.execute(
        f'REVOKE UPDATE(id) ON "{config.control_schema}".registry_revision_control FROM "{roles_by_kind["execution"]}"'
    )
    assert not await connection.fetchval(
        "SELECT has_column_privilege($1::name,$2::text,'id','UPDATE')",
        roles_by_kind["execution"],
        f'"{config.control_schema}".registry_revision_control',
    )
    before = await _native_state(fixture)
    async with connection.transaction():
        with pytest.raises(ValueError, match="registry_fhir_catalog_permissions_invalid"):
            await install_fhir_catalog_publication_permissions(connection, config)
        assert await _native_state(fixture) == before
