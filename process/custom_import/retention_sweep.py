# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Scheduled retirement of obsolete snapshot families and build scratch cleanup of sealed ones.

Each retained family gets its own bounded transaction. Eligibility stays in
``process.custom_import.retirement`` and the control plane's reference check:
current and rollback generations, live producers and unpublished candidates are
refused there and reported here as ``deferred``. A second pass empties the
build-only scratch of every sealed family that is kept, current and rollback
included, through ``process.custom_import.build_scratch``; families it cannot
prove safe yet (for example a manual acceptance without a completed post-seal
audit, or a reader pin) are ``deferred`` too. The sweep runs as the login that
owns the snapshot storage; PostgreSQL privileges decide what it may change.

Without ``--execute`` every operation runs to completion and is rolled back,
so the report shows exactly what an executing run would remove.
"""

import argparse
import asyncio
import importlib
import json
from dataclasses import asdict

from sqlalchemy import exists, select, text
from sqlalchemy.exc import DBAPIError

from db.models.custom_import_storage import (
    CustomImportSnapshotFamily,
    CustomImportSnapshotRetirement,
)
from process.custom_import import build_scratch, retirement
from process.custom_import.publication import PublicationConflict

STATEMENT_TIMEOUT = "5s"
LOCK_TIMEOUT = "100ms"
# lock_not_available and query_canceled: a busy family is retried on the next run.
_BOUNDED_SQLSTATES = frozenset({"55P03", "57014"})


class _DryRun(Exception):
    """Roll back an operation that completed inside a dry run."""


async def retained_family_ids(sessions, *, sealed=False):
    """Return IDs of families without a retirement tombstone, oldest first; optionally only sealed ones."""
    query = select(CustomImportSnapshotFamily.family_id).where(
        ~exists().where(
            CustomImportSnapshotRetirement.family_id == CustomImportSnapshotFamily.family_id,
        )
    )
    if sealed:
        query = query.where(
            CustomImportSnapshotFamily.frozen_at.is_not(None),
            CustomImportSnapshotFamily.generation_id.is_not(None),
        )
    async with sessions() as session, session.begin():
        return list(await session.scalars(query.order_by(CustomImportSnapshotFamily.family_id)))


async def _attempt(sessions, family_id, operation, refusals, *, execute, seconds, states):
    """Run one family's operation in its own bounded transaction; unexpected errors propagate."""
    name, done, would, elapsed = states
    receipt = None
    try:
        async with asyncio.timeout(seconds):
            async with sessions() as session, session.begin():
                await session.execute(text(f"SET LOCAL statement_timeout='{STATEMENT_TIMEOUT}'"))
                await session.execute(text(f"SET LOCAL lock_timeout='{LOCK_TIMEOUT}'"))
                receipt = await operation(session, family_id)
                if not execute:
                    raise _DryRun
    except _DryRun:
        return {"operation": name, "state": would, **asdict(receipt)}
    except refusals as error:
        return {"operation": name, "state": "deferred", "family_id": family_id, "reason": str(error)}
    except TimeoutError:
        return {"operation": name, "state": "deferred", "family_id": family_id, "reason": elapsed}
    except DBAPIError as error:
        if getattr(error.orig, "sqlstate", None) not in _BOUNDED_SQLSTATES:
            raise
        return {"operation": name, "state": "deferred", "family_id": family_id, "reason": elapsed}
    return {"operation": name, "state": done, **asdict(receipt)}


async def retire_one(sessions, family_id, retire, refusals, *, execute):
    """Return one JSON-ready retirement record."""
    return await _attempt(
        sessions,
        family_id,
        retire,
        refusals,
        execute=execute,
        seconds=retirement.MAX_RETIREMENT_SECONDS,
        states=("retire", "retired", "would_retire", "retirement bound elapsed"),
    )


async def trim_one(sessions, family_id, *, engine_id, execute):
    """Return one JSON-ready build scratch cleanup record."""

    async def trim(session, family_id):
        """Bind this sweep's engine identity to the cleanup library call."""
        return await build_scratch.trim_build_scratch(session, family_id, engine_id=engine_id)

    return await _attempt(
        sessions,
        family_id,
        trim,
        # A seal or source bundle that no longer validates keeps its scratch until someone looks.
        (build_scratch.BuildScratchCleanupRefused, PublicationConflict),
        execute=execute,
        seconds=build_scratch.MAX_CLEANUP_SECONDS,
        states=("trim", "trimmed", "would_trim", "trim bound elapsed"),
    )


async def sweep(sessions, retire, refusals, *, engine_id, execute, emit=print):
    """Try every retained family once, then trim every kept sealed family; one JSON line each."""
    outcomes = []

    def report(outcome):
        """Emit one family outcome as a JSON line and keep it for the caller."""
        emit(json.dumps(outcome, sort_keys=True))
        outcomes.append(outcome)

    for family_id in await retained_family_ids(sessions):
        report(await retire_one(sessions, family_id, retire, refusals, execute=execute))
    # A dry run still lists families it would retire; never report scratch cleanup for those.
    removed_ids = {outcome["family_id"] for outcome in outcomes if outcome["state"] in {"retired", "would_retire"}}
    for family_id in await retained_family_ids(sessions, sealed=True):
        if family_id not in removed_ids:
            report(await trim_one(sessions, family_id, engine_id=engine_id, execute=execute))
    return outcomes


def control_plane_retire(engine_id, control_module):
    """Bind the control plane's reference check from the module the scheduled job supplies."""
    control = importlib.import_module(control_module)
    namespace = str(CustomImportSnapshotFamily.__table__.schema)

    async def retire(session, family_id):
        """Retire one family through the control-plane wrapper on this session."""
        # Identity comes from the live connection: access is PostgreSQL's job, not a pinned value.
        database, login, system_identifier = (
            await session.execute(
                text(
                    "SELECT pg_catalog.current_database(), session_user::text, system_identifier::text "
                    "FROM pg_catalog.pg_control_system()"
                )
            )
        ).one()
        return await control.retire_exact_snapshot_family(
            session,
            family_id,
            retire_snapshot_family=retirement.retire_snapshot_family,
            context=control.SnapshotRetirementOwnerContext(
                engine_id=engine_id,
                engine_namespace=namespace,
                expected_database=database,
                expected_system_identifier=system_identifier,
                expected_owner=login,
            ),
        )

    return retire, (retirement.SnapshotRetirementRefused, control.HostedSnapshotRetirementRefused)


async def _amain(arguments):
    from sqlalchemy.ext.asyncio import async_sessionmaker

    from db.connection import db

    retire, refusals = control_plane_retire(arguments.engine_id, arguments.control_module)
    await db.connect()
    try:
        sessions = async_sessionmaker(db.engine, expire_on_commit=False, autoflush=False)
        await sweep(sessions, retire, refusals, engine_id=arguments.engine_id, execute=arguments.execute)
    finally:
        await db.disconnect()


def main(argv=None):
    """Command-line entry point; exits non-zero only on unexpected errors."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--engine-id", required=True, help="control-plane engine identity of this database")
    parser.add_argument(
        "--control-module", required=True, help="importable module providing the control-plane retirement wrapper"
    )
    parser.add_argument(
        "--execute", action="store_true", help="commit retirements and cleanups instead of rolling back"
    )
    asyncio.run(_amain(parser.parse_args(argv)))


if __name__ == "__main__":
    main()
