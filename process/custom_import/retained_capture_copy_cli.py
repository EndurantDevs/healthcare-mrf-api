# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fixed trusted copy entry; credentials and exception details never enter logs."""

import asyncio
import json
import sys
import time

from process.custom_import import retained_capture_copy as service
from process.custom_import import retained_capture_copy_files as files
from process.custom_import.family_shard_owner_access import require_owner_connection
from process.custom_import.family_shard_owner_cli import profile, require_capacity
from process.custom_import.family_shard_sessions import import_session_factory


async def run_copy(identifier, database):
    """Cross real profile and native-owner preflight before copying sealed bytes."""
    configuration, _key, _limits = profile()
    signed, bearer, _root = files.reopen_job(identifier)
    service.authorize(signed, bearer, configuration, now=time.time())
    require_capacity(configuration)
    await database.connect()
    try:
        factory = import_session_factory(
            database.session_factory,
            work_mem_mb=configuration["work_mem_mb"],
            maintenance_work_mem_mb=configuration["maintenance_work_mem_mb"],
        )
        async with factory() as session, session.begin():
            await require_owner_connection(session, configuration["owner_role"])
        try:
            result = await service.copy_capture(factory, signed, bearer, configuration)
        except BaseException as primary:
            await service.finish_failure(factory, signed, bearer, primary)
            raise
        if result["copy_state"] != "sealed":
            raise ValueError()
        return {"status": "capture_ready"}
    finally:
        await database.disconnect()


def main(arguments=None):
    """Emit one bounded status; uncertain failure leaves native audit and custody intact."""
    arguments = sys.argv[1:] if arguments is None else arguments
    try:
        if len(arguments) != 1:
            raise ValueError()
        files.directory_name(arguments[0])
        from db.connection import db

        result = asyncio.run(run_copy(arguments[0], db))
    except Exception, asyncio.CancelledError, KeyboardInterrupt:
        print('{"status":"unavailable"}')
        return 1
    print(json.dumps(result, separators=(",", ":"), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
