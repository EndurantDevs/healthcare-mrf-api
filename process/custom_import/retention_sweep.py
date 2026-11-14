# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Scheduled retirement of every snapshot family the retirement library accepts.

Each retained family gets its own bounded transaction. Eligibility stays in
``process.custom_import.retirement`` and the control plane's reference check:
current and rollback generations, live producers and unpublished candidates are
refused there and reported here as ``deferred``. The sweep runs as the login
that owns the snapshot storage; PostgreSQL privileges decide what it may drop.

Without ``--execute`` every retirement runs to completion and is rolled back,
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
from process.custom_import import retirement

STATEMENT_TIMEOUT = "5s"
LOCK_TIMEOUT = "100ms"
# lock_not_available and query_canceled: a busy family is retried on the next run.
_BOUNDED_SQLSTATES = frozenset({"55P03", "57014"})


class _DryRun(Exception):
    """Roll back a retirement that completed inside a dry run."""


async def retained_family_ids(sessions):
    """Return IDs of families without a retirement tombstone, oldest first."""
    async with sessions() as session, session.begin():
        return list(
            await session.scalars(
                select(CustomImportSnapshotFamily.family_id)
                .where(
                    ~exists().where(
                        CustomImportSnapshotRetirement.family_id == CustomImportSnapshotFamily.family_id,
                    )
                )
                .order_by(CustomImportSnapshotFamily.family_id)
            )
        )


async def retire_one(sessions, family_id, retire, refusals, *, execute):
    """Return one JSON-ready record; unexpected errors propagate."""
    receipt = None
    try:
        async with asyncio.timeout(retirement.MAX_RETIREMENT_SECONDS):
            async with sessions() as session, session.begin():
                await session.execute(text(f"SET LOCAL statement_timeout='{STATEMENT_TIMEOUT}'"))
                await session.execute(text(f"SET LOCAL lock_timeout='{LOCK_TIMEOUT}'"))
                receipt = await retire(session, family_id)
                if not execute:
                    raise _DryRun
    except _DryRun:
        return {"state": "would_retire", **asdict(receipt)}
    except refusals as error:
        return {"state": "deferred", "family_id": family_id, "reason": str(error)}
    except TimeoutError:
        return {"state": "deferred", "family_id": family_id, "reason": "retirement bound elapsed"}
    except DBAPIError as error:
        if getattr(error.orig, "sqlstate", None) not in _BOUNDED_SQLSTATES:
            raise
        return {"state": "deferred", "family_id": family_id, "reason": "retirement bound elapsed"}
    return {"state": "retired", **asdict(receipt)}


async def sweep(sessions, retire, refusals, *, execute, emit=print):
    """Try every retained family once and emit one JSON line per family."""
    records = []
    for family_id in await retained_family_ids(sessions):
        record = await retire_one(sessions, family_id, retire, refusals, execute=execute)
        emit(json.dumps(record, sort_keys=True))
        records.append(record)
    return records


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
        await sweep(sessions, retire, refusals, execute=arguments.execute)
    finally:
        await db.disconnect()


def main(argv=None):
    """Command-line entry point; exits non-zero only on unexpected errors."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--engine-id", required=True, help="control-plane engine identity of this database")
    parser.add_argument(
        "--control-module", required=True, help="importable module providing the control-plane retirement wrapper"
    )
    parser.add_argument("--execute", action="store_true", help="commit retirements instead of rolling back")
    asyncio.run(_amain(parser.parse_args(argv)))


if __name__ == "__main__":
    main()
