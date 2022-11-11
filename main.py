#!/usr/bin/env python
import os
import logging.config
from pathlib import Path
import yaml
import click
from asyncpg import connection
from asyncpg.connection import ServerCapabilities
from sanic import Sanic
from api import init_api

from dotenv import load_dotenv
import arq.cli

from db.migrator import db_group


env_path = Path(__file__).absolute().parent / '.env'
load_dotenv(dotenv_path=env_path)
with open(os.environ['HLTHPRT_LOG_CFG'], encoding="utf-8") as fobj:
    logging.config.dictConfig(yaml.safe_load(fobj))

from process import process_group




@click.command(help="Run sanic server")
@click.option('--host', help='Setup host ip to listen up, default to 0.0.0.0', default='0.0.0.0')
@click.option('--port', help='Setup port to attach, default to 8080', type=int, default=8080)
@click.option('--workers', help='Setup workers to run, default to 1', type=int, default=1)
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
    api = Sanic(__name__, env_prefix="HLTHPRT_")
    if debug:
        os.environ['HLTHPRT_DB_ECHO'] = 'True'
    init_api(api)
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
    pass


cli.add_command(server)
cli.add_command(process_group, name="start")
cli.add_command(db_group, name="db")
cli.add_command(arq.cli.cli, name="worker")


if __name__ == '__main__':
    cli()
