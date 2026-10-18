# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Prepare immutable official NPI sets before the short cohort publication fence."""

import asyncio
import copy
import importlib
from dataclasses import dataclass
from uuid import uuid4
from weakref import WeakKeyDictionary

from sqlalchemy import text

from db.plan_release_storage import storage_identifier, storage_relation
from process.native_copy_cleanup import close_interrupted_copy, invalidate_copy_owner
from process.provider_directory_dataset_rehydrate_pin import pinned_current_source
from process.provider_directory_dataset_rehydrate_scope import _load_dataset_scope
from process.provider_directory_dataset_rehydrate_types import RehydrationRequest, RehydrationRuntime
from process.provider_directory_generic_source_policy import (
    bound_publisher,
    source_copy_session,
    source_transaction,
    source_transaction_available,
)
from process.provider_directory_generic_source_storage import _capture, _close_grants
from process.provider_directory_projection_stage import _copy_driver
from process.uhc_flex_official_cohort_contract import UHCFlexOfficialCohortError, build_uhc_flex_official_cohort

MEMBER = "provider_directory_uhc_flex_npi_member"
COPY_ROWS = 4096
_ISSUED = WeakKeyDictionary()


def _require(condition):
    if not condition:
        raise UHCFlexOfficialCohortError("state")


@dataclass(frozen=True)
class _Replay:
    cohort: object
    snapshot: object
    policy: object
    custody: object
    database_oid: int


def child_name(cohort_id):
    """Only a complete canonical cohort identity selects its immutable child."""
    import re

    _require(type(cohort_id) is str and re.fullmatch(r"pdufc_[0-9a-f]{48}", cohort_id) is not None)
    return "pdufcm_" + cohort_id[6:]


async def _custody(connection, policy, cohort_id=None):
    from process.provider_directory_generic_source_policy import source_cohort_snapshot_custody

    return await source_cohort_snapshot_custody(connection, policy, cohort_id=cohort_id)


async def _issue(connection, publisher, cohort, snapshot):
    custody = await _custody(connection, publisher.policy, cohort.cohort_id)
    database_oid = await connection.fetchval("SELECT oid::bigint FROM pg_database WHERE datname=current_database()")
    _ISSUED.setdefault(asyncio.current_task(), {})[cohort.cohort_id] = _Replay(
        cohort, snapshot, publisher.policy, copy.deepcopy(custody), database_oid
    )


async def _replay_origin(database, issued):
    binding = database._transaction_binding()
    _require(binding is not None)
    connection = await _copy_driver(binding.session)
    _require(connection.is_in_transaction() is True)
    actor = await connection.fetchrow(
        "SELECT session_user::text AS login,current_user::text AS actor,"
        "(SELECT oid::bigint FROM pg_database WHERE datname=current_database()) AS database_oid,"
        "pg_backend_pid() AS backend,pg_current_xact_id()::text AS xid"
    )
    publisher = source_transaction_available()
    if publisher is None:
        _require(actor["login"] == actor["actor"] and actor["actor"] in issued.policy.control_runtime_roles)
    else:
        _require(
            publisher.connection is connection
            and publisher.policy is issued.policy
            and source_copy_session() is binding.session
            and actor["login"] == issued.policy.publisher_role
            and actor["actor"] == issued.policy.owner_role
        )
    _require(actor["database_oid"] == issued.database_oid and actor["backend"] == connection.get_server_pid())
    return binding, connection, dict(actor)


async def replay_cohort(database, expected):
    """Use only an issued same-task cohort in the original admission transaction."""
    from process import uhc_flex_official_cohort_store as store
    from process.provider_directory_source_acquisition_custody import capture_cohort_snapshot_custody

    issued = _ISSUED.get(asyncio.current_task(), {}).get(expected.cohort_id)
    _require(issued is not None and issued.cohort == expected)
    origin = await _replay_origin(database, issued)
    roles = issued.custody["namespace_roles"]
    _require(type(roles) is tuple and len(roles) == 2)
    snapshot = await store._current_official_snapshot(database)
    _require(snapshot == issued.snapshot)
    custody = await capture_cohort_snapshot_custody(
        origin[1],
        issued.policy,
        migration_role=roles[0],
        namespace_owner_role=roles[1],
        cohort_id=expected.cohort_id,
    )
    _require(custody == issued.custody)
    cohort = await store._stored_cohort_for_dataset(database, snapshot.dataset_id)
    final = await _replay_origin(database, issued)
    _require(cohort == expected and final[0] is origin[0] and final[1] is origin[1] and final[2] == origin[2])
    return store.UHCFlexOfficialCohortSyncResult(cohort, False)


async def _copy_members(connection, policy, name, pin, directory, cohort, projection):
    """One native SOURCE cursor, bounded transport, and the original COPY owner."""
    pin.source_relation(directory)
    statement = text(projection)
    stream = await pin.reader._session.stream(
        statement,
        {"dataset_id": cohort.official_dataset_id, "resource_type": cohort.resource_type},
        execution_options={"yield_per": COPY_ROWS},
    )
    copied = 0
    try:
        async for page in stream.partitions(COPY_ROWS):
            pin.source_relation(directory)
            records = [(cohort.cohort_id, row[0]) for row in page]
            _require(0 < len(records) <= COPY_ROWS)
            try:
                status = await connection.copy_records_to_table(
                    name, schema_name=policy.schema, columns=("cohort_id", "npi"), records=records
                )
            except asyncio.CancelledError, TimeoutError:
                await close_interrupted_copy(connection)
                await invalidate_copy_owner(source_copy_session(), connection)
                raise
            _require(status == f"COPY {len(records)}")
            copied += len(records)
    finally:
        await stream.close()
    _require(copied == cohort.npi_count)


async def _finish(connection, policy, name, cohort, projection):
    """Build essential indexes before the complete native membership comparison."""
    relation = storage_relation(policy.schema, name)
    await connection.execute(
        f"ALTER TABLE {relation} ADD CONSTRAINT {storage_identifier(name + '_pk')} PRIMARY KEY(cohort_id,npi)"
    )
    await connection.execute(f"CREATE INDEX {storage_identifier(name + '_npi')} ON {relation}(npi,cohort_id)")
    await connection.execute(f"ANALYZE {relation}")
    # PK uniqueness + indexed containment + equal cardinalities proves both directions.
    # The SOURCE child remains closed and physically pinned throughout this set check.
    source_projection = projection.replace(":dataset_id", "$1").replace(":resource_type", "$2")
    matched = await connection.fetchrow(
        f"""WITH expected AS MATERIALIZED ({source_projection})
        SELECT (SELECT count(*) FROM {relation})::bigint AS count,
        EXISTS(SELECT FROM {relation} WHERE cohort_id<>$3 OR cohort_id IS NULL
            OR created_at IS DISTINCT FROM transaction_timestamp()) AS wrong_scope,
        (SELECT count(*) FROM expected)::bigint AS expected_count,
        EXISTS(SELECT FROM expected e WHERE NOT EXISTS(SELECT FROM {relation} m
            WHERE m.cohort_id=$3 AND m.npi=e.npi)) AS missing""",
        cohort.official_dataset_id,
        cohort.resource_type,
        cohort.cohort_id,
    )
    _require(
        matched["count"] == matched["expected_count"] == cohort.npi_count
        and matched["wrong_scope"] is False
        and matched["missing"] is False
    )
    await connection.execute(
        f"ALTER TABLE {relation} ADD CONSTRAINT {storage_identifier(name + '_scope')} "
        f"CHECK(cohort_id='{cohort.cohort_id}')"
    )
    return (await _capture(connection, policy, [name]))[name]


async def _publish(database, pin, directory, snapshot, cohort, name, prepared):
    """Fresh controls and one native attach/header cut; never lock during COPY."""
    from process import uhc_flex_official_cohort_store as store

    publisher = bound_publisher()
    connection, policy = publisher.connection, publisher.policy
    is_locked = await connection.fetchval(
        "SELECT pg_try_advisory_xact_lock(hashtextextended($1,0))",
        "provider-directory-uhc-flex-official-cohort:" + cohort.official_source_id,
    )
    if is_locked is not True:
        raise UHCFlexOfficialCohortError("busy")
    _require(await store._current_official_snapshot(database) == snapshot)
    request = _request(snapshot)
    runtime = _runtime(database, directory)
    _require((await _load_dataset_scope(runtime, request, lock=True)).fence_identity == pin.scope.fence_identity)
    _require(await _custody(connection, policy) is not None)
    _require((await _capture(connection, policy, [name]))[name] == prepared)
    existing = await store._stored_cohort_for_dataset(database, snapshot.dataset_id)
    if existing is not None:
        _require(existing == cohort)
        await connection.execute(f"DROP TABLE {storage_relation(policy.schema, name)}")
        await _issue(connection, publisher, existing, snapshot)
        return store.UHCFlexOfficialCohortSyncResult(existing, False)
    final_name = child_name(cohort.cohort_id)
    relation = storage_relation(policy.schema, name)
    await connection.execute(f"ALTER TABLE {relation} RENAME TO {storage_identifier(final_name)}")
    relation = storage_relation(policy.schema, final_name)
    for suffix in ("pk", "scope"):
        await connection.execute(
            f"ALTER TABLE {relation} RENAME CONSTRAINT {storage_identifier(name + '_' + suffix)} "
            f"TO {storage_identifier(final_name + '_' + suffix)}"
        )
    await connection.execute(
        f"ALTER INDEX {storage_relation(policy.schema, name + '_npi')} "
        f"RENAME TO {storage_identifier(final_name + '_npi')}"
    )
    name = final_name
    parent = storage_relation(policy.schema, MEMBER)
    await connection.execute(f"LOCK TABLE {parent} IN SHARE UPDATE EXCLUSIVE MODE NOWAIT")
    await _close_grants(connection, policy, name)
    _require(bool(policy.control_runtime_roles))
    controls = ",".join(storage_identifier(role) for role in policy.control_runtime_roles)
    await connection.execute(f"GRANT SELECT ON TABLE {storage_relation(policy.schema, name)} TO {controls}")
    await connection.execute(
        f"ALTER TABLE {parent} ATTACH PARTITION {storage_relation(policy.schema, name)} "
        f"FOR VALUES IN ('{cohort.cohort_id}')"
    )
    await store._insert_header(database, cohort)
    _require(await store._stored_cohort_for_dataset(database, snapshot.dataset_id) == cohort)
    await _issue(connection, publisher, cohort, snapshot)
    return store.UHCFlexOfficialCohortSyncResult(cohort, True)


def _runtime(database, directory):
    return RehydrationRuntime(database, directory._schema(), {}, None, directory=directory)


def _request(snapshot):
    from process.uhc_provider_file_source_identity import UHC_PROVIDER_FILE_SOURCE_ID

    return RehydrationRequest(
        UHC_PROVIDER_FILE_SOURCE_ID,
        snapshot.dataset_id,
        snapshot.acquisition_root_run_id,
        snapshot.acquisition_root_run_id,
    )


async def prepare_cohort(database):
    """Require genuine SOURCE orchestration before any read or candidate DDL."""
    from process import uhc_flex_official_cohort_store as store

    directory = importlib.import_module("process.provider_directory_fhir")
    _require(source_transaction_available() is None)
    _require(database is directory.db and database._transaction_binding() is None)
    async with source_transaction(database, isolation="repeatable read"):
        snapshot = await store._current_official_snapshot(database)
    async with pinned_current_source(_runtime(database, directory), _request(snapshot), directory=directory) as pin:
        async with source_transaction(database):
            publisher = bound_publisher()
            connection, policy = publisher.connection, publisher.policy
            _require(await _custody(connection, policy) is not None)
            existing = await store._stored_cohort_for_dataset(database, snapshot.dataset_id)
            if existing is not None:
                _require(store._is_cohort_snapshot_match(existing, snapshot))
                _require(await store._current_official_snapshot(database) == snapshot)
                await _issue(connection, publisher, existing, snapshot)
                return store.UHCFlexOfficialCohortSyncResult(existing, False)
            resource_relation = pin.source_relation(directory)
            count = await store._official_practitioner_npi_count(
                pin.reader, snapshot, resource_relation=resource_relation
            )
            cohort = build_uhc_flex_official_cohort(
                official_endpoint_id=snapshot.endpoint_id,
                official_dataset_id=snapshot.dataset_id,
                official_acquisition_root_run_id=snapshot.acquisition_root_run_id,
                official_dataset_hash=snapshot.dataset_hash,
                official_content_proof_sha256=snapshot.content_proof_sha256,
                practitioner_resource_count=snapshot.practitioner_resource_count,
                npi_count=count,
            )
            # A unique private name permits independent preparation of the same cohort.
            loading = "pdufcl_" + uuid4().hex
            await connection.execute(
                f"CREATE TABLE {storage_relation(policy.schema, loading)} "
                f"(LIKE {storage_relation(policy.schema, MEMBER)} INCLUDING DEFAULTS "
                "INCLUDING GENERATED INCLUDING CONSTRAINTS INCLUDING STORAGE)"
            )
            await _close_grants(connection, policy, loading)
            empty = (await _capture(connection, policy, [loading]))[loading]
            projection = store._member_projection_sql(resource_relation)
            await _copy_members(connection, policy, loading, pin, directory, cohort, projection)
            _require((await _capture(connection, policy, [loading]))[loading] == empty)
            prepared = await _finish(connection, policy, loading, cohort, projection)
            return await _publish(database, pin, directory, snapshot, cohort, loading, prepared)
