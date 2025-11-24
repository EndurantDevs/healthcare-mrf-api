#!/usr/bin/env python
# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import logging.config
import os
import subprocess
import sys
from pathlib import Path

import yaml
from dotenv import load_dotenv

env_path = Path(__file__).absolute().parent / '.env'
load_dotenv(dotenv_path=env_path)

with open(os.environ['HLTHPRT_LOG_CFG'], encoding="utf-8") as log_config_file:
    logging.config.dictConfig(yaml.safe_load(log_config_file))

import arq.cli  # noqa: E402
import click  # noqa: E402
import uvloop  # noqa: E402
from asyncpg import connection  # noqa: E402
from asyncpg.connection import ServerCapabilities  # noqa: E402
from sanic import Sanic  # noqa: E402

from api import init_api  # noqa: E402
from db.migrator import db_group  # noqa: E402
from process import process_group, process_group_end  # noqa: E402

uvloop.install()
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
asyncio.set_event_loop(asyncio.new_event_loop())

api = Sanic('mrf-api', env_prefix="HLTHPRT_")
init_api(api)

@click.command(help="Run sanic server")
@click.option('--host', help='Setup host ip to listen up, default to 0.0.0.0', default='0.0.0.0')
@click.option('--port', help='Setup port to attach, default to 8080', type=int, default=8080)
@click.option('--workers', help='Setup workers to run, default to 1', type=int, default=2)
@click.option('--debug', help='Enable or disable debugging', is_flag=True)
@click.option('--accesslog', help='Enable or disable access log', is_flag=True)
def start(host, port, workers, debug, accesslog):
    connection._detect_server_capabilities = lambda *a, **kw: ServerCapabilities(
        advisory_locks=False,
        notifications=False,
        plpgsql=False,
        sql_reset=False,
        sql_close_all=False
    )
    if debug:
        os.environ['HLTHPRT_DB_ECHO'] = 'True'
    with open(api.config['LOG_CFG'], encoding="utf-8") as log_file:
        logging.config.dictConfig(yaml.safe_load(log_file))
    api.run(
        host=host,
        port=port,
        workers=workers,
        debug=debug,
        auto_reload=debug,
        access_log=accesslog)


@click.group()
def server():
    pass


server.add_command(start)

@click.group()
def cli():
    # Ensure every CLI invocation has an event loop so libraries relying on
    # asyncio.get_event_loop() behave consistently (e.g. arq worker setup).
    try:
        loop = asyncio.get_event_loop()
        if loop.is_closed():
            raise RuntimeError
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    else:
        # If uvloop is installed (set above), ensure the active loop uses uvloop's implementation.
        if uvloop is not None and not isinstance(loop, uvloop.Loop):
            loop = uvloop.new_event_loop()
            asyncio.set_event_loop(loop)


cli.add_command(server)
cli.add_command(process_group, name="start")
cli.add_command(process_group_end, name="finish")
cli.add_command(db_group, name="db")


@click.group()
def manage():
    """Utility commands for maintaining an existing deployment."""


@manage.command("sync-structure")
@click.option("--skip-columns", is_flag=True, default=False, help="Skip adding missing columns")
@click.option("--skip-indexes", is_flag=True, default=False, help="Skip creating indexes")
def sync_structure(skip_columns: bool, skip_indexes: bool):
    """Ensure database tables, columns, and indexes match the SQLAlchemy models."""
    from db.maintenance import render_sync_summary
    from db.maintenance import sync_structure as _sync_structure  # lazy import

    results = asyncio.run(
        _sync_structure(add_columns=not skip_columns, add_indexes=not skip_indexes)
    )
    render_sync_summary(results)


@manage.command("rebuild-plan-summary")
@click.option("--test", is_flag=True, help="Use the test database suffix defined via HLTHPRT_TEST_DATABASE_SUFFIX.")
def rebuild_plan_summary(test: bool):
    """Rebuild the plan_search_summary table so /plan/search can use precomputed filters."""
    from process.plan_summary import \
        rebuild_plan_search_summary as _rebuild  # lazy import

    rows = asyncio.run(_rebuild(test_mode=test))
    click.echo(f"Rebuilt plan_search_summary with {rows} rows")


@click.group()
def stop():
    """Run burst workers to drain specific queues."""


def _run_worker_command(target: str, burst: bool, env: dict[str, str]):
    cmd = [sys.executable, __file__, "worker", target]
    if burst:
        cmd.append("--burst")
    click.echo(f"Executing: {' '.join(cmd)}")
    subprocess.run(cmd, check=True, env=env)


@stop.command("mrf")
@click.option("--burst/--no-burst", default=True, help="Match arq worker burst mode (defaults to burst).")
@click.option("--import-id", help="Override the import_id/import_date passed to worker startup.")
def stop_mrf(burst: bool, import_id: str | None):
    """Drain remaining MRF jobs and run the finish queue."""
    env = os.environ.copy()
    if import_id:
        env["HLTHPRT_IMPORT_ID_OVERRIDE"] = import_id
    else:
        env.pop("HLTHPRT_IMPORT_ID_OVERRIDE", None)

    _run_worker_command("process.MRF", burst, env)
    # Always run the finish queue in burst mode so table swaps happen once jobs are drained.
    _run_worker_command("process.MRF_finish", True, env)


cli.add_command(stop, name="stop")
cli.add_command(arq.cli.cli, name="worker")
cli.add_command(manage, name="manage")


if __name__ == '__main__':
    cli()
