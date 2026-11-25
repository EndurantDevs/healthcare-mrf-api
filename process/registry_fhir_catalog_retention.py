# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Drop registry FHIR catalog candidates that can never be published.

A candidate refused at validation is abandoned; one left by a killed run stays handed off, frozen
or validated with no live claim. Either way its storage schema and preparation records remain until
this sweep drops them. Staging runs in one transaction, so a run killed while staging leaves nothing.
A kill just after the staging commit and before the checkpoint, or between the checkpoint and the
admission, leaves a schema or a snapshot row without a preparation record; the sweep skips those.

The sweep holds the catalog run lock, so no run is building or validating meanwhile. Unabandoned
candidates stay while a publication request is queued or running under a live lease, since a network
import publishes its prepared catalog with the memberships it is still building. A candidate whose
snapshot is published, selected by the serving pointer, referenced by a non-rejected network
membership candidate or named as an unpublished child's predecessor is never selected. The sweeping
login needs the privileges of the candidate schema owners, SELECT and DELETE on the snapshot ledger
and the preparation, claim and relation tables, SELECT (schema_name) on hp_snapshot_retention.relation,
SELECT (state, source_generations) on network_membership_candidate, and SELECT on the serving control
and publication request tables.

Without ``--execute`` the sweep only lists what an executing run would drop.
"""

import argparse
import asyncio
import json
from datetime import timedelta

import asyncpg

from db.plan_release_storage import storage_identifier

STALE_AFTER = timedelta(hours=48)
LOCK_TIMEOUT = "5s"
# A busy, timed-out or externally referenced candidate is retried on the next run.
_DEFERRED = (asyncpg.LockNotAvailableError, asyncpg.QueryCanceledError, asyncpg.DependentObjectsStillExistError)


class _CandidateChangedError(Exception):
    """Roll back child-record deletes when the preparation row is no longer eligible."""


def _eligible(control):
    """Unpublished catalog preparations of one control schema that no run or reader can still use."""
    schema = "'fhir_catalog_'||replace(p.preparation_id::text,'-','')"
    return f"""p.importer_id='catalog' AND p.node_id=$1 AND p.state<>'published'
 AND (p.state='abandoned' OR (p.updated_at<clock_timestamp()-$2::interval
  AND (p.claim_expires_at IS NULL OR p.claim_expires_at<=clock_timestamp())
  AND NOT EXISTS(SELECT FROM {control}.registry_publication_request q
   WHERE q.state='queued' OR q.state='running' AND q.lease_expires_at>clock_timestamp())))
 AND NOT EXISTS(SELECT FROM {control}.registry_fhir_catalog_snapshot s WHERE s.schema_name={schema}
  AND (s.published_at IS NOT NULL
   OR s.metadata_generation=(SELECT c.metadata_generation FROM {control}.network_serving_control c WHERE c.id=1)
   OR EXISTS(SELECT FROM {control}.network_membership_candidate m
    WHERE m.state<>'rejected' AND m.source_generations->>'fhir_catalog'=s.metadata_generation)
   OR EXISTS(SELECT FROM {control}.registry_fhir_catalog_snapshot child
    WHERE child.published_at IS NULL
     AND child.custody_json#>>'{{preparation,previous_metadata_generation}}'=s.metadata_generation)))
 AND NOT EXISTS(SELECT FROM hp_snapshot_retention.relation r WHERE r.schema_name={schema})"""


async def _has_retired(connection, control_schema, stale_after, preparation_id):
    """Drop one candidate's tables, schema and records together, rechecking eligibility under the row lock."""
    control = storage_identifier(control_schema)
    schema = "fhir_catalog_" + preparation_id.hex
    try:
        async with connection.transaction():
            await connection.execute(f"SET LOCAL lock_timeout='{LOCK_TIMEOUT}'")
            for table in ("preparation_relation", "preparation_claim"):
                await connection.execute(
                    f"DELETE FROM hp_snapshot_retention.{table} WHERE preparation_id=$3 "
                    f"AND EXISTS(SELECT FROM hp_snapshot_retention.preparation p "
                    f"WHERE p.preparation_id=$3 AND {_eligible(control)})",
                    control_schema,
                    stale_after,
                    preparation_id,
                )
            retired = await connection.fetchval(
                "DELETE FROM hp_snapshot_retention.preparation p "
                f"WHERE p.preparation_id=$3 AND {_eligible(control)} RETURNING true",
                control_schema,
                stale_after,
                preparation_id,
            )
            if not retired:
                raise _CandidateChangedError
            tables = await connection.fetch(
                "SELECT format('%I.%I',n.nspname,c.relname) AS name FROM pg_class c "
                "JOIN pg_namespace n ON n.oid=c.relnamespace "
                "WHERE n.nspname=$1 AND c.relkind IN ('r','p') AND NOT c.relispartition",
                schema,
            )
            if tables:
                await connection.execute("DROP TABLE " + ",".join(table["name"] for table in tables))
            await connection.execute(f"DROP SCHEMA IF EXISTS {storage_identifier(schema)}")
            await connection.execute(
                f"DELETE FROM {control}.registry_fhir_catalog_snapshot WHERE schema_name=$1 AND published_at IS NULL",
                schema,
            )
    except _CandidateChangedError:
        return False
    return True


async def retire_unpublished_fhir_catalog_candidates(
    connection, *, control_schema, stale_after=STALE_AFTER, execute=False
):
    """List, or with ``execute`` drop, abandoned and stale catalog candidates of one control schema."""
    control = storage_identifier(control_schema)
    lock = "registry-fhir-catalog:" + control_schema
    if not await connection.fetchval("SELECT pg_try_advisory_lock(hashtextextended($1,0))", lock):
        return {"state": "catalog_run_active", "schemas": []}
    try:
        rows = await connection.fetch(
            f"SELECT p.preparation_id FROM hp_snapshot_retention.preparation p WHERE {_eligible(control)} "
            "ORDER BY p.created_at,p.preparation_id",
            control_schema,
            stale_after,
        )
        if not execute:
            return {"state": "listed", "schemas": ["fhir_catalog_" + row["preparation_id"].hex for row in rows]}
        dropped, deferred = [], []
        for row in rows:
            schema = "fhir_catalog_" + row["preparation_id"].hex
            try:
                is_retired = await _has_retired(connection, control_schema, stale_after, row["preparation_id"])
            except _DEFERRED:
                is_retired = False
            (dropped if is_retired else deferred).append(schema)
        return {"state": "retired", "schemas": dropped, "deferred": deferred}
    finally:
        await connection.execute("SELECT pg_advisory_unlock(hashtextextended($1,0))", lock)


async def _amain(arguments):
    from db.connection import db
    from db.registry_schema import registry_schema

    await db.connect()
    try:
        async with db.acquire_driver() as connection:
            report = await retire_unpublished_fhir_catalog_candidates(
                connection,
                control_schema=registry_schema(),
                stale_after=timedelta(hours=arguments.stale_hours),
                execute=arguments.execute,
            )
    finally:
        await db.disconnect()
    print(json.dumps(report, sort_keys=True))


def main(argv=None):
    """Command-line entry point for the scheduled sweep."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--stale-hours",
        type=int,
        default=int(STALE_AFTER.total_seconds() // 3600),
        help="retire unabandoned candidates unchanged for this many hours",
    )
    parser.add_argument("--execute", action="store_true", help="drop the listed candidates")
    arguments = parser.parse_args(argv)
    if arguments.stale_hours < 1:
        parser.error("--stale-hours must be at least 1")
    asyncio.run(_amain(arguments))


if __name__ == "__main__":
    main()
