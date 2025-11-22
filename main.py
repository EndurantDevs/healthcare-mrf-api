#!/usr/bin/env python
# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import os
import logging.config
from pathlib import Path
import yaml
import click
import asyncio
import uvloop
uvloop.install()
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
asyncio.set_event_loop(asyncio.new_event_loop())
    
from asyncpg import connection
from asyncpg.connection import ServerCapabilities
from sanic import Sanic

env_path = Path(__file__).absolute().parent / '.env'
from dotenv import load_dotenv
load_dotenv(dotenv_path=env_path)
from api import init_api


import arq.cli

from db.migrator import db_group

with open(os.environ['HLTHPRT_LOG_CFG'], encoding="utf-8") as fobj:
    logging.config.dictConfig(yaml.safe_load(fobj))

from process import process_group, process_group_end


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
    with open(api.config['LOG_CFG']) as fobj:
        logging.config.dictConfig(yaml.safe_load(fobj))
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
cli.add_command(arq.cli.cli, name="worker")


@click.group()
def manage():
    """Utility commands for maintaining an existing deployment."""


@manage.command("sync-structure")
@click.option("--skip-columns", is_flag=True, default=False, help="Skip adding missing columns")
@click.option("--skip-indexes", is_flag=True, default=False, help="Skip creating indexes")
def sync_structure(skip_columns: bool, skip_indexes: bool):
    """Ensure database tables, columns, and indexes match the SQLAlchemy models."""
    from db.maintenance import render_sync_summary, sync_structure as _sync_structure  # lazy import

    results = asyncio.run(
        _sync_structure(add_columns=not skip_columns, add_indexes=not skip_indexes)
    )
    render_sync_summary(results)


cli.add_command(manage, name="manage")


if __name__ == '__main__':
    cli()
