# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Real COPY, immutable candidate and set-admission regression proofs."""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import asdict, replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import asyncpg
import pytest
from sqlalchemy.exc import DBAPIError

from db.connection import Database
from process import provider_directory_rooted_graph_bulk as bulk
from process import provider_directory_rooted_graph_frontier as frontier
from process import provider_directory_rooted_graph_result_store as result_store
from process import provider_directory_rooted_graph_store as rooted_store
from process.provider_directory_rooted_graph_result_contract import build_provider_directory_rooted_graph_query_result
from process.provider_directory_rooted_graph_result_store import complete_provider_directory_rooted_graph_result
from process.provider_directory_rooted_graph_store import (
    claim_provider_directory_rooted_graph_work,
    initialize_provider_directory_rooted_graph_acquisition,
)
from process.provider_directory_rooted_graph_store_contract import ProviderDirectoryRootedGraphStoreError
from process.provider_directory_rooted_graph_store_support import work_fields
from tests.formulary_fhir_twin_admission_pg_support import connect, database_url, load_migration, run_migration
from tests.test_provider_directory_rooted_graph_acquisition_postgres import (
    MIGRATION_PATH,
    _complete_success,
    _identity,
    _query_result_for_work,
)
from tests.test_provider_directory_rooted_graph_publication_postgres import (
    _lifecycle_scope,
    _publish_legacy_root,
)


async def _claim(context):
    current = await _publish_legacy_root(context.database)
    identity = _identity(current, "baseline", "2", "3")
    await initialize_provider_directory_rooted_graph_acquisition(identity, database=context.database)
    claim = await claim_provider_directory_rooted_graph_work(identity.acquisition_id, database=context.database)
    assert claim is not None
    query_result = _query_result_for_work(claim, "exact_reference_search")
    resource_payloads = [json.loads(witness.payload_json_text) for witness in query_result.resources]
    resource_payloads[0]["identifier"] = [{"value": "Clinique Élan 中文"}]
    return (
        identity,
        claim,
        build_provider_directory_rooted_graph_query_result(
            claim,
            resource_payloads,
            advertised_total=len(resource_payloads),
        ),
    )


async def _independent_claim_during_root_load(database, identity):
    """A separate writer must progress while the new root transaction stays open."""
    async with database.transaction():
        await database.scalar("SELECT set_config('lock_timeout','100ms',true)")
        claim = await claim_provider_directory_rooted_graph_work(identity.acquisition_id, database=database)
        assert claim is not None


@pytest.mark.asyncio
async def test_root_initialization_builds_detached_and_rolls_back_before_attach(monkeypatch):
    async with _lifecycle_scope(monkeypatch) as context:
        current = await _publish_legacy_root(context.database)
        incumbent = _identity(current, "baseline", "2", "3")
        replacement = _identity(current, "candidate", "4", "5")
        await initialize_provider_directory_rooted_graph_acquisition(incumbent, database=context.database)
        independent = Database()
        await independent.connect()
        initialize_work = rooted_store._insert_initial_root_work

        async def fail_after_indexing(database, identity):
            await initialize_work(database, identity)
            await _independent_claim_during_root_load(independent, incumbent)
            raise RuntimeError("synthetic root initialization failure")

        try:
            with monkeypatch.context() as patch:
                patch.setattr(rooted_store, "_insert_initial_root_work", fail_after_indexing)
                with pytest.raises(RuntimeError, match="synthetic root initialization failure"):
                    await initialize_provider_directory_rooted_graph_acquisition(replacement, database=context.database)
        finally:
            await independent.disconnect()
        assert await context.connection.fetchval(
            f"SELECT count(*)=0 FROM {context.schema}.provider_directory_rooted_graph_acquisition WHERE acquisition_id=$1",
            replacement.acquisition_id,
        )
        assert (
            await context.connection.fetchval(
                f"SELECT count(*) FROM {context.schema}.provider_directory_rooted_graph_work WHERE acquisition_id=$1",
                replacement.acquisition_id,
            )
            == 0
        )
        assert await initialize_provider_directory_rooted_graph_acquisition(replacement, database=context.database) == 1
        assert await initialize_provider_directory_rooted_graph_acquisition(replacement, database=context.database) == 0
        assert await claim_provider_directory_rooted_graph_work(replacement.acquisition_id, database=context.database)


async def _census(context, identity):
    return tuple(
        await context.connection.fetchrow(
            f"SELECT used_resource_rows,used_edge_rows,used_payload_bytes FROM {context.schema}."
            "provider_directory_rooted_graph_acquisition WHERE acquisition_id=$1",
            identity.acquisition_id,
        )
    )


@pytest.mark.asyncio
async def test_rooted_short_copy_rolls_back_before_admission(monkeypatch):
    """A driver count mismatch rolls back real COPY rows and leaves the claim retryable."""
    async with _lifecycle_scope(monkeypatch) as context:
        identity, claim, query_result = await _claim(context)
        original_driver = bulk._copy_driver
        scalar = AsyncMock(wraps=context.database.scalar)

        async def short_driver(transaction):
            driver = await original_driver(transaction)

            async def short_copy(*args, **kwargs):
                await driver.copy_records_to_table(*args, **kwargs)
                return "COPY 0"

            return SimpleNamespace(copy_records_to_table=short_copy)

        with monkeypatch.context() as patch:
            patch.setattr(bulk, "_copy_driver", short_driver)
            patch.setattr(context.database, "scalar", scalar)
            with pytest.raises(ProviderDirectoryRootedGraphStoreError, match="state"):
                async with context.database.transaction() as transaction:
                    await bulk.admit_result_witnesses(context.database, transaction, claim, query_result)
        assert not any(
            "admit_provider_directory_rooted_graph_witnesses" in call.args[0] for call in scalar.await_args_list
        )
        assert await _census(context, identity) == (0, 0, 0)
        assert (
            await context.connection.fetchval(
                f"SELECT count(*) FROM {context.schema}.provider_directory_rooted_graph_resource WHERE acquisition_id=$1",
                identity.acquisition_id,
            )
            == 0
        )
        await complete_provider_directory_rooted_graph_result(claim, query_result, database=context.database)
        assert await _census(context, identity) == (
            len(query_result.resources),
            len(query_result.edges),
            sum(len(witness.payload_json_text.encode()) for witness in query_result.resources),
        )


def _malformed_result(query_result, failure):
    resources = list(query_result.resources)
    edges = list(query_result.edges)
    if failure == "resource_invalid":
        resources[0] = SimpleNamespace(**{**asdict(resources[0]), "payload_sha256": "0" * 64})
    elif failure == "stage_invalid":
        resources[0] = SimpleNamespace(**{**asdict(resources[0]), "closure_scope": "unsupported"})
    elif failure == "edge_invalid":
        edge_by_field = {**asdict(edges[0]), "target_resource_id": "missing.synthetic"}
        edge_by_field["edge_sha256"] = hashlib.sha256(
            "\x1f".join(edge_by_field[field] for field in bulk._EDGE_COLUMNS[4:9]).encode()
        ).hexdigest()
        edges[0] = SimpleNamespace(**edge_by_field)
    else:
        resources.append(resources[0])
    return SimpleNamespace(resources=resources, edges=edges)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure", ("resource_invalid", "edge_invalid", "stage_invalid", "could not create unique index")
)
async def test_rooted_copy_isolated_exact_and_failed_result_rolls_back(monkeypatch, failure):
    async with _lifecycle_scope(monkeypatch) as context:
        identity, claim, query_result = await _claim(context)
        assert await _census(context, identity) == (0, 0, 0)
        # Exercise multiple binary COPY batches without introducing synthetic scale claims.
        monkeypatch.setattr(bulk, "_COPY_ROWS", 1)
        malformed = _malformed_result(query_result, failure)
        with pytest.raises(DBAPIError, match=failure):
            async with context.database.transaction() as transaction:
                await bulk.admit_result_witnesses(context.database, transaction, claim, malformed)
        assert await _census(context, identity) == (0, 0, 0)
        await complete_provider_directory_rooted_graph_result(claim, query_result, database=context.database)
        assert await _census(context, identity) == (
            len(query_result.resources),
            len(query_result.edges),
            sum(len(witness.payload_json_text.encode()) for witness in query_result.resources),
        )
        witnesses = await context.connection.fetch(
            f"SELECT resource_type,resource_id,payload_sha256,payload_json_text,closure_scope,tableoid::regclass::text "
            f"FROM {context.schema}.provider_directory_rooted_graph_resource WHERE acquisition_id=$1 ORDER BY resource_id",
            identity.acquisition_id,
        )
        assert [tuple(witness[:5]) for witness in witnesses] == [
            tuple(getattr(witness, name) for name in bulk._RESOURCE_COLUMNS[4:])
            for witness in sorted(query_result.resources, key=lambda item: item.resource_id)
        ]
        assert all(witness[5].endswith("pdrgr_" + identity.acquisition_id[6:]) for witness in witnesses)
        await _assert_no_row_insert_guards(context)
        candidate = f"{context.schema_name}.pdrgr_{identity.acquisition_id[6:]}"
        assert (
            await context.connection.fetchval(
                "SELECT count(*) FROM pg_index WHERE indrelid=$1::regclass AND NOT indisprimary",
                candidate,
            )
            == 0
        )
        with pytest.raises(asyncpg.ObjectNotInPrerequisiteStateError, match="acquisition_incomplete"):
            await context.connection.execute(
                f"SELECT {context.schema}.finish_provider_directory_rooted_graph_storage($1)",
                identity.acquisition_id,
            )
        assert (
            await context.connection.fetchval(
                f"SELECT status FROM {context.schema}.provider_directory_rooted_graph_acquisition WHERE acquisition_id=$1",
                identity.acquisition_id,
            )
            == "building"
        )


async def _assert_no_row_insert_guards(context):
    assert not await context.connection.fetchval(
        "SELECT EXISTS(SELECT 1 FROM pg_trigger WHERE NOT tgisinternal AND tgtype & 1=1 AND tgtype & 4=4 "
        "AND tgrelid IN (SELECT inhrelid FROM pg_inherits WHERE inhparent IN ($1::regclass,$2::regclass,$3::regclass) "
        "UNION SELECT $1::regclass UNION SELECT $2::regclass UNION SELECT $3::regclass))",
        f"{context.schema_name}.provider_directory_rooted_graph_resource",
        f"{context.schema_name}.provider_directory_rooted_graph_edge",
        f"{context.schema_name}.provider_directory_rooted_graph_work",
    )


async def _assert_authorized_lease_shape(context, role, claim):
    async with context.connection.transaction():
        await context.connection.execute(f'SET LOCAL ROLE "{role}"')
        await context.connection.execute(
            "SELECT set_config('healthporta.rooted_graph_action','heartbeat',true),"
            "set_config('healthporta.rooted_graph_acquisition',$1,true),"
            "set_config('healthporta.rooted_graph_lease',$2,true)",
            claim.acquisition_id,
            claim.lease_token,
        )
        with pytest.raises(asyncpg.CheckViolationError, match="work_state_check"):
            async with context.connection.transaction():
                await context.connection.execute(
                    f"UPDATE {context.schema}.provider_directory_rooted_graph_work "
                    "SET lease_expires_at=lease_expires_at+interval '1 minute',lease_heartbeat_at=transaction_timestamp(),"
                    "updated_at=transaction_timestamp(),result_sha256=repeat('a',64),resource_count=1 "
                    "WHERE acquisition_id=$1 AND query_id=$2",
                    claim.acquisition_id,
                    claim.query_id,
                )


async def _assert_registry_write_denied(context, role):
    async with context.connection.transaction():
        await context.connection.execute(f'SET LOCAL ROLE "{role}"')
        with pytest.raises(asyncpg.InsufficientPrivilegeError, match="copy_unauthorized"):
            async with context.connection.transaction():
                await context.connection.execute(f"UPDATE {context.schema}.pdrg_storage_migration SET complete=false")


async def _assert_direct_writes_denied(context, identity, role):
    for relation in (
        "provider_directory_rooted_graph_resource",
        "pdrgr_" + identity.acquisition_id[6:],
        "provider_directory_rooted_graph_work",
        "pdrgw_" + identity.acquisition_id[6:],
    ):
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            async with context.connection.transaction():
                await context.connection.execute(f'SET LOCAL ROLE "{role}"')
                await context.connection.execute(f"INSERT INTO {context.schema}.{relation} DEFAULT VALUES")


async def _assert_stage_alter_denied(connection):
    with pytest.raises(asyncpg.InsufficientPrivilegeError):
        async with connection.transaction():
            await connection.execute("ALTER TABLE pg_temp.pdrg_resource_stage ADD COLUMN unsafe text")


async def _migrate_restricted_writer(context, role):
    await context.connection.execute(f'GRANT USAGE ON SCHEMA {context.schema} TO "{role}"')
    for relation in ("work", "resource", "edge"):
        await context.connection.execute(
            f'GRANT SELECT,INSERT,UPDATE ON {context.schema}.provider_directory_rooted_graph_{relation} TO "{role}"'
        )
    await context.connection.execute(
        f'ALTER DEFAULT PRIVILEGES IN SCHEMA {context.schema} GRANT INSERT,UPDATE ON TABLES TO "{role}"'
    )
    await context.connection.execute(
        f"GRANT INSERT(payload_json_text) ON {context.schema}.provider_directory_rooted_graph_resource TO PUBLIC"
    )
    migration = load_migration(MIGRATION_PATH.with_name("20261005100000_rooted_graph_set_validation.py"), "rooted_acl")
    await run_migration(context.engine, migration, "upgrade")


async def _assert_forged_stage_denied(context, role, claim):
    with pytest.raises(asyncpg.CheckViolationError, match="stage_invalid"):
        async with context.connection.transaction():
            await context.connection.execute(f'SET LOCAL ROLE "{role}"')
            await context.connection.execute("CREATE TEMP TABLE pdrg_resource_stage(acquisition_id text)")
            await context.connection.execute("CREATE TEMP TABLE pdrg_edge_stage(acquisition_id text)")
            await context.connection.fetchval(
                f"SELECT {context.schema}.admit_provider_directory_rooted_graph_witnesses($1,$2,$3,$4,"
                "'pg_temp.pdrg_resource_stage'::regclass,'pg_temp.pdrg_edge_stage'::regclass)",
                claim.acquisition_id,
                claim.query_id,
                claim.attempt,
                claim.lease_token,
            )


@pytest.mark.asyncio
async def test_rooted_definer_stage_closes_direct_and_forged_writes(monkeypatch):
    role = "pdrg_test_" + uuid.uuid4().hex
    admin = await connect(database_url())
    await admin.execute(f'CREATE ROLE "{role}" NOLOGIN')
    try:
        async with _lifecycle_scope(monkeypatch, set_validation=False) as context:
            await _migrate_restricted_writer(context, role)
            identity, claim, query_result = await _claim(context)
            await _assert_authorized_lease_shape(context, role, claim)
            await _assert_direct_writes_denied(context, identity, role)
            await context.connection.execute(f'GRANT pg_write_all_data TO "{role}"')
            await _assert_direct_writes_denied(context, identity, role)
            await _assert_registry_write_denied(context, role)
            await _assert_forged_stage_denied(context, role, claim)
            async with context.connection.transaction():
                await context.connection.execute(f'SET LOCAL ROLE "{role}"')
                await context.connection.execute(
                    f"SELECT {context.schema}.prepare_provider_directory_rooted_graph_witness_stage()"
                )
                prefix = (claim.acquisition_id, claim.scope_id, claim.query_id, claim.attempt)
                for name, columns, witnesses in (
                    ("pdrg_resource_stage", bulk._RESOURCE_COLUMNS, query_result.resources),
                    ("pdrg_edge_stage", bulk._EDGE_COLUMNS, query_result.edges),
                ):
                    await context.connection.copy_records_to_table(
                        name,
                        schema_name="pg_temp",
                        columns=columns,
                        records=[
                            prefix + tuple(getattr(witness, field) for field in columns[4:]) for witness in witnesses
                        ],
                    )
                await _assert_stage_alter_denied(context.connection)
                counts = await context.connection.fetchval(
                    f"SELECT {context.schema}.admit_provider_directory_rooted_graph_witnesses($1,$2,$3,$4,"
                    "'pg_temp.pdrg_resource_stage'::regclass,'pg_temp.pdrg_edge_stage'::regclass)",
                    claim.acquisition_id,
                    claim.query_id,
                    claim.attempt,
                    claim.lease_token,
                )
                assert counts == [len(query_result.resources), len(query_result.edges)]
            assert await _census(context, identity) == (
                len(query_result.resources),
                len(query_result.edges),
                sum(len(witness.payload_json_text.encode()) for witness in query_result.resources),
            )
    finally:
        await admin.execute(f'DROP ROLE "{role}"')
        await admin.close()


def _legacy_initial_root_work_sql() -> str:
    """Build one INSERT..SELECT for the entire immutable Practitioner root."""

    pagination = "same-origin-source-issued-until-terminal"
    return f"""
        WITH canonical_root_query AS (
            SELECT member.resource_id,
                   '{{"kind":"exact_reference_search","page_size":' ||
                   CAST(:page_size AS text) ||
                   ',"pagination":"{pagination}","reference":' ||
                   pg_catalog.to_json(
                       ('Practitioner/' || member.resource_id)::text
                   )::text ||
                   ',"resource_type":"PractitionerRole",'
                   '"search_parameter":"practitioner"}}'
                       AS query_identity_json_text
              FROM {rooted_store.table_ref("provider_directory_dataset_resource")} AS member
             WHERE member.dataset_id = :root_dataset_id
               AND member.resource_type = 'Practitioner'
        ), canonical_root_identity AS (
            SELECT root.resource_id, root.query_identity_json_text,
                   pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
                       root.query_identity_json_text, 'UTF8'
                   )), 'hex') AS query_identity_sha256,
                   pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
                       :identity_contract || pg_catalog.chr(31) || :scope_id ||
                       pg_catalog.chr(31) || root.query_identity_json_text,
                       'UTF8'
                   )), 'hex') AS query_id_sha256
              FROM canonical_root_query AS root
        )
        INSERT INTO {rooted_store.table_ref("provider_directory_rooted_graph_work")} (
            acquisition_id, scope_id, query_id, query_identity_sha256,
            query_identity_json_text, kind, resource_type, search_parameter,
            reference_type, reference_id, closure_scope,
            discovered_by_query_id, discovered_source_type,
            discovered_source_id, discovered_edge_sha256,
            status, attempt_count, pagination_terminal
        ) SELECT
            :acquisition_id, :scope_id,
            'pdrgq_' || pg_catalog.substr(root.query_id_sha256, 1, 48),
            root.query_identity_sha256, root.query_identity_json_text,
            'exact_reference_search', 'PractitionerRole', 'practitioner',
            'Practitioner', root.resource_id, 'root',
            NULL, NULL, NULL, NULL, 'pending', 0, false
          FROM canonical_root_identity AS root
         ORDER BY root.resource_id
        ON CONFLICT (acquisition_id, query_id) DO NOTHING;
    """


async def _legacy_witness_insert(database, transaction, claim, query_result):
    """Exercise the replaced writer against its original immutable row guards."""
    await rooted_store.set_store_action(database, "witness", claim.acquisition_id, claim.lease_token)
    prefix_by_column = dict(
        zip(
            bulk._RESOURCE_COLUMNS[:4],
            (
                claim.acquisition_id,
                claim.scope_id,
                claim.query_id,
                claim.attempt,
            ),
        )
    )
    for relation, columns, witnesses in (
        ("provider_directory_rooted_graph_resource", bulk._RESOURCE_COLUMNS, query_result.resources),
        ("provider_directory_rooted_graph_edge", bulk._EDGE_COLUMNS, query_result.edges),
    ):
        for witness in witnesses:
            placeholders = ",".join(":" + column for column in columns)
            await database.status(
                f"INSERT INTO {rooted_store.table_ref(relation)} ({','.join(columns)}) VALUES ({placeholders})",
                **prefix_by_column,
                **asdict(witness),
            )


async def _legacy_work_insert(database, acquisition_id, work_specs, *, action):
    await rooted_store.set_store_action(database, action, acquisition_id)
    for spec in work_specs:
        fields_by_column = {
            "acquisition_id": acquisition_id,
            **work_fields(spec),
            "status": "pending",
            "attempt_count": 0,
            "pagination_terminal": False,
        }
        columns = ",".join(fields_by_column)
        values = ",".join(":" + column for column in fields_by_column)
        await database.status(
            f"INSERT INTO {rooted_store.table_ref('provider_directory_rooted_graph_work')} ({columns}) "
            f"VALUES ({values}) ON CONFLICT(acquisition_id,query_id) DO NOTHING",
            **fields_by_column,
        )
    return len(work_specs)


async def _historical_acquisitions(context, monkeypatch):
    current = await _publish_legacy_root(context.database)
    historical = _identity(current, "baseline", "2", "3")
    abandoned = _identity(current, "candidate", "4", "5")
    scalar = context.database.scalar
    first = context.database.first

    async def historical_first(query, **parameters):
        for identity in (historical, abandoned):
            query = query.replace("pdrgw_" + identity.acquisition_id[6:], "provider_directory_rooted_graph_work")
        return await first(query, **parameters)

    async def historical_scalar(query, **parameters):
        if (
            "prepare_provider_directory_rooted_graph_storage" in query
            or "finish_provider_directory_rooted_graph_storage" in query
            or "attach_provider_directory_rooted_graph_storage" in query
        ):
            return None
        if "initialize_provider_directory_rooted_graph_work" in query:
            identity = historical if parameters["acquisition_id"] == historical.acquisition_id else abandoned
            return await context.database.status(
                _legacy_initial_root_work_sql(),
                acquisition_id=identity.acquisition_id,
                scope_id=identity.scope_id,
                root_dataset_id=identity.root_dataset_id,
                identity_contract="healthporta.provider-directory.rooted-graph-identity.v1",
                page_size="100",
            )
        return await scalar(query, **parameters)

    with monkeypatch.context() as scoped:
        scoped.setattr(context.database, "scalar", historical_scalar)
        scoped.setattr(context.database, "first", historical_first)

        async def initialize_legacy(database, identity):
            await historical_scalar(
                "initialize_provider_directory_rooted_graph_work", acquisition_id=identity.acquisition_id
            )

        scoped.setattr(rooted_store, "_insert_initial_root_work", initialize_legacy)
        scoped.setattr(result_store, "admit_result_witnesses", _legacy_witness_insert)
        scoped.setattr(bulk, "admit_work_specs", _legacy_work_insert)
        scoped.setattr(frontier, "admit_work_specs", _legacy_work_insert)
        sealed = await _complete_success(context.database, historical)
        await initialize_provider_directory_rooted_graph_acquisition(abandoned, database=context.database)
    return current, historical, abandoned, sealed


async def _physical_storage(connection, schema):
    return await connection.fetch(
        "SELECT oid,relfilenode FROM pg_class WHERE oid IN ("
        "SELECT indexrelid FROM pg_index WHERE indrelid IN ($1::regclass,$2::regclass,$3::regclass)"
        ") OR oid IN ($1::regclass,$2::regclass,$3::regclass) ORDER BY oid",
        f"{schema}.provider_directory_rooted_graph_resource",
        f"{schema}.provider_directory_rooted_graph_edge",
        f"{schema}.provider_directory_rooted_graph_work",
    )


@pytest.mark.asyncio
async def test_rooted_migration_preserves_sealed_heaps_and_requires_new_run(monkeypatch):
    async with _lifecycle_scope(monkeypatch, set_validation=False) as context:
        current, historical, abandoned, sealed = await _historical_acquisitions(context, monkeypatch)
        physical_before = await _physical_storage(context.connection, context.schema_name)
        payloads_before = await context.connection.fetch(
            f"SELECT * FROM {context.schema}.provider_directory_rooted_graph_resource ORDER BY query_id,resource_id"
        )
        migration = load_migration(
            MIGRATION_PATH.with_name("20261005100000_rooted_graph_set_validation.py"), "rooted_history"
        )
        await run_migration(context.engine, migration, "upgrade")
        assert (
            await context.connection.fetch(
                "SELECT oid,relfilenode FROM pg_class WHERE oid=ANY($1::oid[]) ORDER BY oid",
                [relation["oid"] for relation in physical_before],
            )
            == physical_before
        )
        assert (
            await context.connection.fetch(
                f"SELECT * FROM {context.schema}.provider_directory_rooted_graph_resource ORDER BY query_id,resource_id"
            )
            == payloads_before
        )
        assert await initialize_provider_directory_rooted_graph_acquisition(historical, database=context.database) == 0
        with pytest.raises(DBAPIError, match="rerun_required"):
            await initialize_provider_directory_rooted_graph_acquisition(abandoned, database=context.database)
        rerun = _identity(current, "candidate", "6", "7")
        replacement = await _complete_success(context.database, rerun)
        assert replacement.rooted_graph_sha256 == sealed.rooted_graph_sha256
        assert replacement.resource_set_sha256 == sealed.resource_set_sha256
        assert replacement.edge_set_sha256 == sealed.edge_set_sha256


@pytest.mark.asyncio
async def test_rooted_frontier_failure_rolls_back_terminal_and_witness_sets(monkeypatch):
    async with _lifecycle_scope(monkeypatch) as context:
        identity, claim, query_result = await _claim(context)
        derive_specs = frontier._derived_work_specs

        def invalid_frontier(*arguments):
            work_specs = derive_specs(*arguments)
            return (replace(work_specs[0], discovered_edge_sha256="0" * 64), *work_specs[1:])

        with monkeypatch.context() as scoped:
            scoped.setattr(frontier, "_derived_work_specs", invalid_frontier)
            with pytest.raises(DBAPIError, match="discovery_invalid"):
                await complete_provider_directory_rooted_graph_result(claim, query_result, database=context.database)
        assert await _census(context, identity) == (0, 0, 0)
        assert (
            await context.connection.fetchval(
                f"SELECT status FROM {context.schema}.provider_directory_rooted_graph_work WHERE acquisition_id=$1 AND query_id=$2",
                identity.acquisition_id,
                claim.query_id,
            )
            == "leased"
        )
        await complete_provider_directory_rooted_graph_result(claim, query_result, database=context.database)
        assert await context.connection.fetchval(
            f"SELECT used_work_items=count(*) FROM {context.schema}.provider_directory_rooted_graph_acquisition "
            f"JOIN {context.schema}.provider_directory_rooted_graph_work USING(acquisition_id) "
            "WHERE acquisition_id=$1 GROUP BY used_work_items",
            identity.acquisition_id,
        )


@pytest.mark.asyncio
async def test_rooted_seal_rejects_missing_candidate_indexes_and_retries(monkeypatch):
    async with _lifecycle_scope(monkeypatch) as context:
        current = await _publish_legacy_root(context.database)
        identity = _identity(current, "baseline", "2", "3")
        scalar = context.database.scalar

        async def skip_index_build(query, **parameters):
            if "finish_provider_directory_rooted_graph_storage" in query:
                return None
            return await scalar(query, **parameters)

        with monkeypatch.context() as scoped:
            scoped.setattr(context.database, "scalar", skip_index_build)
            with pytest.raises(DBAPIError, match="indexes_incomplete"):
                await _complete_success(context.database, identity)
        assert (
            await context.connection.fetchval(
                f"SELECT status FROM {context.schema}.provider_directory_rooted_graph_acquisition WHERE acquisition_id=$1",
                identity.acquisition_id,
            )
            == "building"
        )
        sealed = await rooted_store.seal_provider_directory_rooted_graph_acquisition(
            identity, database=context.database
        )
        assert sealed.rooted_graph_complete


@pytest.mark.asyncio
async def test_rooted_default_function_grants_do_not_authorize_admission(monkeypatch):
    role = "pdrg_default_" + uuid.uuid4().hex
    administrator = await connect(database_url())
    function_names = (
        "prepare_provider_directory_rooted_graph_storage",
        "finish_provider_directory_rooted_graph_storage",
        "prepare_provider_directory_rooted_graph_work_stage",
        "admit_provider_directory_rooted_graph_work",
        "initialize_provider_directory_rooted_graph_work",
        "read_provider_directory_rooted_graph_initial_work",
        "guard_provider_directory_rooted_graph_copy_owner",
        "attach_provider_directory_rooted_graph_storage",
        "finish_provider_directory_rooted_graph_initial_storage",
        "validate_provider_directory_rooted_graph_stage_checks",
        "prepare_provider_directory_rooted_graph_witness_stage",
        "admit_provider_directory_rooted_graph_witnesses",
    )
    try:
        await administrator.execute(f'CREATE ROLE "{role}" NOLOGIN')
        async with _lifecycle_scope(monkeypatch, set_validation=False) as context:
            await context.connection.execute(f'GRANT USAGE ON SCHEMA {context.schema} TO "{role}"')
            await context.connection.execute(
                f'ALTER DEFAULT PRIVILEGES IN SCHEMA {context.schema} GRANT EXECUTE ON FUNCTIONS TO "{role}"'
            )
            await context.connection.execute(
                f'ALTER DEFAULT PRIVILEGES IN SCHEMA {context.schema} GRANT ALL ON TABLES TO "{role}"'
            )
            migration = load_migration(
                MIGRATION_PATH.with_name("20261005100000_rooted_graph_set_validation.py"), "rooted_default_acl"
            )
            await run_migration(context.engine, migration, "upgrade")
            privileges = await context.connection.fetch(
                "SELECT proname,has_function_privilege($1,oid,'EXECUTE') AS allowed FROM pg_proc "
                "WHERE pronamespace=$2::regnamespace AND proname=ANY($3::text[]) ORDER BY proname",
                role,
                context.schema_name,
                function_names,
            )
            assert len(privileges) == len(function_names)
            assert not any(privilege["allowed"] for privilege in privileges)
            identity, _claim_row, _result = await _claim(context)
            for prefix in ("pdrgw_", "pdrgr_", "pdrge_"):
                assert not await context.connection.fetchval(
                    "SELECT has_table_privilege($1,$2,'SELECT')",
                    role,
                    f"{context.schema}.{prefix}{identity.acquisition_id[6:]}",
                )
            async with context.connection.transaction():
                await context.connection.execute(f'SET LOCAL ROLE "{role}"')
                with pytest.raises(asyncpg.InsufficientPrivilegeError):
                    await context.connection.execute(
                        f"SELECT {context.schema}.prepare_provider_directory_rooted_graph_work_stage()"
                    )
    finally:
        await administrator.execute(f'DROP ROLE IF EXISTS "{role}"')
        await administrator.close()


@pytest.mark.asyncio
async def test_root_native_copy_precedes_indexes_and_attach_reuses_them(monkeypatch):
    async with _lifecycle_scope(monkeypatch) as context:
        current = await _publish_legacy_root(context.database)
        identity = _identity(current, "baseline", "2", "3")
        candidate = f"{context.schema}.pdrgw_{identity.acquisition_id[6:]}"
        copy_stage = bulk._copy_stage
        scalar = context.database.scalar
        observed_phases = []

        async def observe_copy(driver, name, columns, records):
            assert name == "pdrg_work_stage"
            assert not await scalar(
                "SELECT EXISTS(SELECT FROM pg_index WHERE indrelid=CAST(:relation AS regclass))", relation=candidate
            )
            assert not await scalar(
                "SELECT EXISTS(SELECT FROM pg_constraint WHERE conrelid=CAST(:relation AS regclass) AND contype IN ('f','c'))",
                relation=candidate,
            )
            assert not await scalar(
                "SELECT EXISTS(SELECT FROM pg_constraint WHERE conrelid='pg_temp.pdrg_work_stage'::regclass AND contype IN ('f','c'))"
            )
            observed_phases.append("copy")
            return await copy_stage(driver, name, columns, records)

        async def observe_attach(query, **parameters):
            if "attach_provider_directory_rooted_graph_storage" not in query:
                return await scalar(query, **parameters)
            indexes_sql = "SELECT array_agg(indexrelid ORDER BY indexrelid) FROM pg_index WHERE indrelid=CAST(:relation AS regclass) AND indisvalid AND indisready"
            before = await scalar(indexes_sql, relation=candidate)
            assert before and len(before) >= 4
            check_names = await scalar(
                "SELECT array_agg(conname::text ORDER BY conname) FROM pg_constraint WHERE conrelid=CAST(:relation AS regclass) AND contype='c' AND convalidated",
                relation=candidate,
            )
            assert check_names == [
                "provider_directory_rooted_graph_work_shape_check",
                "provider_directory_rooted_graph_work_state_check",
                "provider_directory_rooted_graph_work_value_check",
            ]
            assert not await scalar(
                "SELECT EXISTS(SELECT FROM pg_inherits WHERE inhrelid=CAST(:relation AS regclass))", relation=candidate
            )
            result = await scalar(query, **parameters)
            assert await scalar(indexes_sql, relation=candidate) == before
            observed_phases.append("attach")
            return result

        monkeypatch.setattr(bulk, "_copy_stage", observe_copy)
        monkeypatch.setattr(context.database, "scalar", observe_attach)
        await initialize_provider_directory_rooted_graph_acquisition(identity, database=context.database)
        assert observed_phases == ["copy", "attach"]
