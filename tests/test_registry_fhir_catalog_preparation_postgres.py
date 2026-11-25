# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Normal catalog grants, separate administrator freeze and SQL privilege denials."""

from contextlib import asynccontextmanager
from datetime import date
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import asyncpg
import pytest

from db.registry_fhir_catalog import PROJECTOR_VERSION
from process import registry_fhir_catalog_sync as catalog_sync
from process.registry_fhir_catalog_custody import FHIRCatalogTarget
from process.registry_fhir_catalog_fingerprints import approved_fhir_catalog_fingerprints
from process.registry_fhir_catalog_identity import FHIRNetworkIdentityInput, allocate_fhir_catalog_network_ids
from process.registry_fhir_catalog_permissions import (
    install_fhir_catalog_preparation_permissions,
    install_fhir_catalog_publication_permissions,
)
from process.registry_fhir_catalog_publication import FHIRCatalogPublicationRuntime
from process.registry_fhir_catalog_retention import retire_unpublished_fhir_catalog_candidates
from process.registry_fhir_catalog_store import create_fhir_catalog_candidate, create_fhir_catalog_indexes
from process.registry_fhir_catalog_validation import metadata_generation
from process.registry_publication_consumer import RegistryPublicationConsumerConfig
from tests.test_network_custom_address_source_postgres import custom_db as custom_db
from tests.test_network_serving_schema_postgres import serving_schema as serving_schema
from tests.test_registry_fhir_catalog_custody_postgres import (
    _checkpoint_metadata,
    _prepare_common_catalogs,
    cleanup_catalog_preparations,
    freeze_metadata,
)
from tests.test_registry_publication_worker_permissions_postgres import _enqueue, _fixture_dsn, _login
from tests.test_registry_publication_worker_permissions_postgres import worker_permissions as worker_permissions

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def catalog_preparation(worker_permissions, monkeypatch):
    worker = worker_permissions
    connection = worker.fixture.connection
    roles_by_kind = {
        kind: "catalog_prepare_" + kind + "_" + uuid4().hex for kind in ("loader", "sealer", "helper", "bridge")
    }
    created_roles, schemas = [], []
    common = SimpleNamespace(
        connection=connection,
        engine=worker.fixture.engine,
        created_control_namespaces=[],
        created_extensions=[],
    )
    try:
        await _prepare_common_catalogs(common, monkeypatch)
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
        await cleanup_catalog_preparations(connection, schemas)
        for schema in reversed(schemas):
            await connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            assert await connection.fetchval("SELECT to_regnamespace($1)", schema) is None
        for namespace in reversed(common.created_control_namespaces):
            await connection.execute(f'DROP SCHEMA IF EXISTS "{namespace}" CASCADE')
        for extension in reversed(common.created_extensions):
            await connection.execute(f'DROP EXTENSION IF EXISTS "{extension}"')
        for role in reversed(created_roles):
            await connection.execute(f'DROP OWNED BY "{role}"')
            await connection.execute(f'DROP ROLE "{role}"')


async def _install(fixture):
    async with fixture.fixture.connection.transaction():
        return await install_fhir_catalog_preparation_permissions(
            fixture.fixture.connection, fixture.config, **fixture.options
        )


async def _load_empty_catalog(fixture, *, as_of=date(2026, 1, 1), allocate=True):
    """Allocate one identity and prepare an empty candidate as its real Loader."""
    config, roles_by_kind = fixture.config, fixture.roles_by_kind
    async with _login(fixture, "loader") as loader, loader.transaction():
        if allocate:
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
        "as_of": as_of,
    }
    snapshot_by_field["metadata_generation"] = metadata_generation(snapshot_by_field)
    return catalog_target, snapshot_by_field


def _sealer_catalog(fixture, catalog_target, sealer):
    """Describe the candidate as the custody helpers expect, acting through the real sealer."""
    config, roles = fixture.config, fixture.roles_by_kind
    return SimpleNamespace(
        connection=fixture.fixture.connection,
        publisher=sealer,
        engine=fixture.fixture.engine,
        control_schema=config.control_schema,
        target=catalog_target,
        roles={
            "owner": roles["physical"],
            "loader": roles["loader"],
            "publisher": roles["sealer"],
            "reader": roles["api"],
        },
        preparation_roles={
            "namespace_owner_role": roles["physical"],
            "publisher_role": roles["sealer"],
            "application_roles": sorted(
                {
                    roles["loader"],
                    roles["sealer"],
                    config.lifecycle_role,
                    config.execution_role,
                    config.api_role,
                    *config.loader_roles,
                    *config.reader_roles,
                }
            ),
        },
    )


async def _seal_empty_catalog(fixture, catalog_target, snapshot_by_field):
    """Use the real sealer checkpoint and independent administrator freeze."""
    async with _login(fixture, "sealer") as sealer:
        await freeze_metadata(_sealer_catalog(fixture, catalog_target, sealer), catalog_target, snapshot_by_field)


async def test_preparation_preserves_reads_and_restricted_writes(catalog_preparation):
    fixture = catalog_preparation
    connection, config, roles = fixture.fixture.connection, fixture.config, fixture.roles_by_kind
    queries = tuple(
        f'SELECT count(*) FROM "{config.control_schema}".{table}'
        for table in ("network_serving_control", "network_serving_manifest")
    )
    async with _login(fixture, "api") as api:
        initial_counts = [await api.fetchval(query) for query in queries]
    receipt = await _install(fixture)
    assert await _install(fixture) == receipt
    async with _login(fixture, "api") as api:
        assert [await api.fetchval(query) for query in queries] == initial_counts
    async with connection.transaction():
        await install_fhir_catalog_publication_permissions(connection, config)
    catalog_target, snapshot = await _load_empty_catalog(fixture)
    await _seal_empty_catalog(fixture, catalog_target, snapshot)
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
    for kind in ("loader", "sealer", "api", "lifecycle", "execution"):
        async with _login(fixture, kind) as actor:
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await actor.execute(f'SET ROLE "{roles["physical"]}"')


async def test_preparation_requires_transaction(catalog_preparation):
    fixture = catalog_preparation
    with pytest.raises(ValueError, match="preparation_permissions_invalid"):
        await install_fhir_catalog_preparation_permissions(
            fixture.fixture.connection, fixture.config, **fixture.options
        )


async def test_preparation_grants_roll_back(catalog_preparation):
    fixture = catalog_preparation
    with pytest.raises(RuntimeError, match="operator rollback"):
        async with fixture.fixture.connection.transaction():
            await install_fhir_catalog_preparation_permissions(
                fixture.fixture.connection, fixture.config, **fixture.options
            )
            raise RuntimeError("operator rollback")
    async with _login(fixture, "api") as api:
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            await api.fetchval(f'SELECT count(*) FROM "{fixture.config.control_schema}".registry_fhir_catalog_snapshot')


async def test_loader_requires_candidate_creation_permission(catalog_preparation):
    fixture = catalog_preparation
    await _install(fixture)
    database = await fixture.fixture.connection.fetchval("SELECT quote_ident(current_database())")
    await fixture.fixture.connection.execute(
        f'REVOKE CREATE ON DATABASE {database} FROM "{fixture.roles_by_kind["loader"]}"'
    )
    before = await fixture.fixture.connection.fetchval(
        f'SELECT count(*) FROM "{fixture.config.control_schema}".network_registry_identity'
    )
    with pytest.raises(asyncpg.InsufficientPrivilegeError):
        await _load_empty_catalog(fixture)
    assert (
        await fixture.fixture.connection.fetchval(
            f'SELECT count(*) FROM "{fixture.config.control_schema}".network_registry_identity'
        )
        == before
    )


@asynccontextmanager
async def _retention_login(fixture):
    """A retention login holding only the privileges the sweep documents, never an application role."""
    connection, config, roles = fixture.fixture.connection, fixture.config, fixture.roles_by_kind
    role = "catalog_retention_" + uuid4().hex
    control = '"' + config.control_schema + '"'
    await connection.execute(f"CREATE ROLE \"{role}\" LOGIN PASSWORD '{fixture.role_password}'")
    try:
        await connection.execute(
            f'GRANT "{roles["loader"]}","{roles["physical"]}" TO "{role}" WITH INHERIT TRUE, SET FALSE'
        )
        await connection.execute(f'GRANT USAGE ON SCHEMA {control},hp_snapshot_retention TO "{role}"')
        await connection.execute(f'GRANT SELECT,DELETE ON {control}.registry_fhir_catalog_snapshot TO "{role}"')
        await connection.execute(
            f'GRANT SELECT ON {control}.network_serving_control,{control}.registry_publication_request TO "{role}"'
        )
        await connection.execute(
            f'GRANT SELECT (state, source_generations) ON {control}.network_membership_candidate TO "{role}"'
        )
        await connection.execute(
            "GRANT SELECT,DELETE ON hp_snapshot_retention.preparation,hp_snapshot_retention.preparation_claim,"
            f'hp_snapshot_retention.preparation_relation TO "{role}"'
        )
        fixture.roles_by_kind["retention"] = role
        async with _login(fixture, "retention") as retention:
            yield retention
    finally:
        fixture.roles_by_kind.pop("retention", None)
        await connection.execute(f'DROP OWNED BY "{role}"')
        await connection.execute(f'DROP ROLE "{role}"')


async def _install_both(fixture):
    await _install(fixture)
    async with fixture.fixture.connection.transaction():
        await install_fhir_catalog_publication_permissions(fixture.fixture.connection, fixture.config)


async def _handed_off_catalog(fixture, **options):
    """Checkpoint an empty candidate; it stays Loader-owned and writable until an administrator freezes it."""
    catalog_target, snapshot = await _load_empty_catalog(fixture, **options)
    async with _login(fixture, "sealer") as sealer:
        await _checkpoint_metadata(_sealer_catalog(fixture, catalog_target, sealer), catalog_target, snapshot)
    return catalog_target, snapshot


async def _candidate_state(fixture, catalog_target):
    """Report the schema, ledger row and preparation records still held for one candidate."""
    connection, control = fixture.fixture.connection, fixture.config.control_schema
    candidate = UUID(catalog_target.candidate_id)
    return {
        "schema": await connection.fetchval("SELECT to_regnamespace($1) IS NOT NULL", catalog_target.schema_name),
        "snapshot": await connection.fetchval(
            f'SELECT count(*) FROM "{control}".registry_fhir_catalog_snapshot WHERE schema_name=$1',
            catalog_target.schema_name,
        ),
        "records": sum(
            [
                await connection.fetchval(
                    f"SELECT count(*) FROM hp_snapshot_retention.{table} WHERE preparation_id=$1", candidate
                )
                for table in ("preparation", "preparation_claim", "preparation_relation")
            ]
        ),
    }


async def _age_preparations(fixture, *catalog_targets, live_claim=False):
    """Move the last change of these preparations past the stale bound, optionally keeping a live claim."""
    await fixture.fixture.connection.execute(
        "UPDATE hp_snapshot_retention.preparation SET updated_at=clock_timestamp()-interval '3 days'"
        + (",claim_expires_at=clock_timestamp()+interval '1 hour'" if live_claim else "")
        + " WHERE preparation_id=ANY($1::uuid[])",
        [UUID(target.candidate_id) for target in catalog_targets],
    )


async def test_retention_drops_abandoned_frozen_candidate(catalog_preparation):
    """A candidate refused after the freeze is dropped with its records by the retention login."""
    fixture = catalog_preparation
    await _install_both(fixture)
    catalog_target, snapshot = await _load_empty_catalog(fixture)
    await _seal_empty_catalog(fixture, catalog_target, snapshot)
    roles = fixture.roles_by_kind
    sealer_pool = await asyncpg.create_pool(
        _fixture_dsn(), user=roles["sealer"], password=fixture.role_password, min_size=0, max_size=1
    )
    try:
        async with _login(fixture, "sealer") as sealer:
            preparation_roles = _sealer_catalog(fixture, catalog_target, sealer).preparation_roles
        config = SimpleNamespace(
            grant=SimpleNamespace(control_schema=fixture.config.control_schema), preparation_roles=preparation_roles
        )
        refusal = ValueError("registry_fhir_catalog_refused_for_test")
        await catalog_sync._abandon_refused_catalog_candidate(config, snapshot, sealer_pool, refusal)
    finally:
        await sealer_pool.close()
    assert not getattr(refusal, "__notes__", None)
    assert (
        await fixture.fixture.connection.fetchval(
            "SELECT state FROM hp_snapshot_retention.preparation WHERE preparation_id=$1",
            UUID(catalog_target.candidate_id),
        )
        == "abandoned"
    )
    abandoned = await _candidate_state(fixture, catalog_target)
    assert abandoned["schema"] and abandoned["snapshot"] == 0 and abandoned["records"] > 0
    async with _retention_login(fixture) as retention:
        options_by_name = {"control_schema": fixture.config.control_schema}
        listed = await retire_unpublished_fhir_catalog_candidates(retention, **options_by_name)
        assert listed == {"state": "listed", "schemas": [catalog_target.schema_name]}
        assert await _candidate_state(fixture, catalog_target) == abandoned
        retired = await retire_unpublished_fhir_catalog_candidates(retention, **options_by_name, execute=True)
    assert retired == {"state": "retired", "schemas": [catalog_target.schema_name], "deferred": []}
    assert await _candidate_state(fixture, catalog_target) == {"schema": False, "snapshot": 0, "records": 0}


async def test_retention_drops_stale_writable_candidate_only(catalog_preparation):
    """A stale handed-off candidate goes; a fresh one, a live claim, a queued request and a running sync keep it."""
    fixture = catalog_preparation
    await _install_both(fixture)
    stale, _ = await _handed_off_catalog(fixture)
    fresh, _ = await _handed_off_catalog(fixture, as_of=date(2026, 1, 2), allocate=False)
    claimed, _ = await _handed_off_catalog(fixture, as_of=date(2026, 1, 3), allocate=False)
    await _age_preparations(fixture, stale)
    await _age_preparations(fixture, claimed, live_claim=True)
    held_by_schema = {
        candidate.schema_name: await _candidate_state(fixture, candidate) for candidate in (stale, fresh, claimed)
    }
    assert all(state["schema"] and state["snapshot"] == 1 and state["records"] > 0 for state in held_by_schema.values())
    options_by_name = {"control_schema": fixture.config.control_schema, "execute": True}
    request = await _enqueue(fixture)
    async with _retention_login(fixture) as retention:
        assert await retire_unpublished_fhir_catalog_candidates(retention, **options_by_name) == {
            "state": "retired",
            "schemas": [],
            "deferred": [],
        }
        # A killed worker leaves its request running under an expired lease; that no longer protects candidates.
        await fixture.fixture.connection.execute(
            f"UPDATE \"{fixture.config.control_schema}\".registry_publication_request SET state='running',"
            "lease_id=$2,lease_expires_at=clock_timestamp()-interval '1 minute' WHERE request_id=$1",
            UUID(str(request["request_id"])),
            uuid4(),
        )
        lock = "registry-fhir-catalog:" + fixture.config.control_schema
        async with _login(fixture, "loader") as running_sync:
            assert await running_sync.fetchval("SELECT pg_try_advisory_lock(hashtextextended($1,0))", lock)
            assert await retire_unpublished_fhir_catalog_candidates(retention, **options_by_name) == {
                "state": "catalog_run_active",
                "schemas": [],
            }
        assert {
            candidate.schema_name: await _candidate_state(fixture, candidate) for candidate in (stale, fresh, claimed)
        } == held_by_schema
        retired = await retire_unpublished_fhir_catalog_candidates(retention, **options_by_name)
    assert retired == {"state": "retired", "schemas": [stale.schema_name], "deferred": []}
    assert await _candidate_state(fixture, stale) == {"schema": False, "snapshot": 0, "records": 0}
    for candidate in (fresh, claimed):
        assert await _candidate_state(fixture, candidate) == held_by_schema[candidate.schema_name]


async def test_retention_keeps_published_current_and_rollback(catalog_preparation):
    """The published rollback and the serving pointer's candidate stay even when their records look stale."""
    fixture = catalog_preparation
    connection, control = fixture.fixture.connection, fixture.config.control_schema
    await _install_both(fixture)
    rollback, rollback_snapshot = await _load_empty_catalog(fixture)
    current, current_snapshot = await _load_empty_catalog(fixture, as_of=date(2026, 1, 2), allocate=False)
    for catalog_target, snapshot in ((rollback, rollback_snapshot), (current, current_snapshot)):
        await _seal_empty_catalog(fixture, catalog_target, snapshot)
    # Only the rollback is marked published, so the serving pointer alone must protect the current candidate.
    await connection.execute(
        f'UPDATE "{control}".registry_fhir_catalog_snapshot SET published_at=clock_timestamp() WHERE schema_name=$1',
        rollback.schema_name,
    )
    assert (
        await connection.execute(
            f'UPDATE "{control}".network_serving_control SET metadata_generation=$1 WHERE id=1',
            current_snapshot["metadata_generation"],
        )
        == "UPDATE 1"
    )
    await _age_preparations(fixture, rollback, current)
    await connection.execute(
        "UPDATE hp_snapshot_retention.preparation SET state='abandoned',cancellation_requested=true,"
        "abandonment_sha256=repeat('0',64) WHERE preparation_id=$1",
        UUID(rollback.candidate_id),
    )
    published_states = [await _candidate_state(fixture, catalog_target) for catalog_target in (rollback, current)]
    assert all(state["schema"] and state["snapshot"] == 1 and state["records"] > 0 for state in published_states)
    async with _retention_login(fixture) as retention:
        retired = await retire_unpublished_fhir_catalog_candidates(retention, control_schema=control, execute=True)
    assert retired == {"state": "retired", "schemas": [], "deferred": []}
    assert [
        await _candidate_state(fixture, catalog_target) for catalog_target in (rollback, current)
    ] == published_states


async def test_retention_deletes_child_rows_before_preparation_under_restrict_fks(catalog_preparation):
    """ON DELETE RESTRICT constraints on preparation_claim and preparation_relation must allow retirement."""
    fixture = catalog_preparation
    connection, control = fixture.fixture.connection, fixture.config.control_schema
    await _install_both(fixture)
    stale, _ = await _handed_off_catalog(fixture)
    candidate_id = UUID(stale.candidate_id)
    assert (
        await connection.fetchval(
            "SELECT count(*) FROM hp_snapshot_retention.preparation_claim WHERE preparation_id=$1", candidate_id
        )
        > 0
    )
    assert (
        await connection.fetchval(
            "SELECT count(*) FROM hp_snapshot_retention.preparation_relation WHERE preparation_id=$1", candidate_id
        )
        > 0
    )
    await _age_preparations(fixture, stale)
    await connection.execute(
        "ALTER TABLE hp_snapshot_retention.preparation_claim ADD CONSTRAINT retention_test_claim_fkey "
        "FOREIGN KEY (preparation_id) REFERENCES hp_snapshot_retention.preparation(preparation_id) ON DELETE RESTRICT"
    )
    await connection.execute(
        "ALTER TABLE hp_snapshot_retention.preparation_relation ADD CONSTRAINT retention_test_relation_fkey "
        "FOREIGN KEY (preparation_id) REFERENCES hp_snapshot_retention.preparation(preparation_id) ON DELETE RESTRICT"
    )
    try:
        async with _retention_login(fixture) as retention:
            retired = await retire_unpublished_fhir_catalog_candidates(retention, control_schema=control, execute=True)
    finally:
        await connection.execute(
            "ALTER TABLE hp_snapshot_retention.preparation_claim DROP CONSTRAINT IF EXISTS retention_test_claim_fkey"
        )
        await connection.execute(
            "ALTER TABLE hp_snapshot_retention.preparation_relation "
            "DROP CONSTRAINT IF EXISTS retention_test_relation_fkey"
        )
    assert retired == {"state": "retired", "schemas": [stale.schema_name], "deferred": []}
    assert await _candidate_state(fixture, stale) == {"schema": False, "snapshot": 0, "records": 0}


async def test_retention_keeps_paused_network_candidate_and_child_predecessor(catalog_preparation):
    """Paused non-rejected network builds and unpublished child predecessors stay kept; rejected ones retire."""
    fixture = catalog_preparation
    connection, control = fixture.fixture.connection, fixture.config.control_schema
    await _install_both(fixture)
    referenced, referenced_snapshot = await _handed_off_catalog(fixture, as_of=date(2026, 1, 1))
    predecessor, predecessor_snapshot = await _handed_off_catalog(fixture, as_of=date(2026, 1, 2), allocate=False)
    child_catalog, _ = await _handed_off_catalog(fixture, as_of=date(2026, 1, 3), allocate=False)
    rejected_only, rejected_snapshot = await _handed_off_catalog(fixture, as_of=date(2026, 1, 4), allocate=False)
    for target_state, snapshot in (("open", referenced_snapshot), ("rejected", rejected_snapshot)):
        candidate_id = uuid4()
        await connection.execute(
            f'INSERT INTO "{control}".network_membership_candidate '
            "(candidate_id,dataset_id,schema_id,producer_id,schema_name,state,"
            "source_generations,approved_custom_revision,expected_head,expected_rows) "
            "VALUES($1,$2,$3,$4,$5,$6,jsonb_build_object('fhir_catalog',$7::text),0,0,0)",
            candidate_id,
            uuid4(),
            uuid4(),
            uuid4(),
            "network_candidate_" + candidate_id.hex,
            target_state,
            snapshot["metadata_generation"],
        )
    await connection.execute(
        f"UPDATE \"{control}\".registry_fhir_catalog_snapshot SET custody_json=coalesce(custody_json,'{{}}'::jsonb) || "
        "jsonb_build_object('preparation',jsonb_build_object('previous_metadata_generation',$2::text)) "
        "WHERE schema_name=$1",
        child_catalog.schema_name,
        predecessor_snapshot["metadata_generation"],
    )
    await _age_preparations(fixture, referenced, predecessor, rejected_only)
    kept_by_schema = {
        catalog_target.schema_name: await _candidate_state(fixture, catalog_target)
        for catalog_target in (referenced, predecessor, child_catalog)
    }
    async with _retention_login(fixture) as retention:
        retired = await retire_unpublished_fhir_catalog_candidates(retention, control_schema=control, execute=True)
    assert retired == {"state": "retired", "schemas": [rejected_only.schema_name], "deferred": []}
    assert await _candidate_state(fixture, rejected_only) == {"schema": False, "snapshot": 0, "records": 0}
    for catalog_target in (referenced, predecessor, child_catalog):
        assert await _candidate_state(fixture, catalog_target) == kept_by_schema[catalog_target.schema_name]
