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

import arq.cli
import click
import uvloop
from asyncpg import connection
from asyncpg.connection import ServerCapabilities
from arq.logs import default_log_config
from arq.utils import import_string
from arq.worker import create_worker
from sanic import Sanic

from api import init_api
from db.migrator import db_group
from process import process_group, process_group_end


def _new_event_loop():
    return uvloop.new_event_loop()


def _run_async(coro):
    return uvloop.run(coro)


asyncio.set_event_loop(_new_event_loop())

api = Sanic('mrf-api', env_prefix="HLTHPRT_")
init_api(api)


def _default_api_workers() -> int:
    raw = os.getenv("HLTHPRT_API_WORKERS", "1")
    try:
        return max(int(raw), 1)
    except (TypeError, ValueError):
        return 1


@click.command(help="Run sanic server")
@click.option('--host', help='Setup host ip to listen up, default to 0.0.0.0', default='0.0.0.0')
@click.option('--port', help='Setup port to attach, default to 8080', type=int, default=8080)
@click.option(
    '--workers',
    help='Setup workers to run, default from HLTHPRT_API_WORKERS or 1',
    type=int,
    default=_default_api_workers(),
)
@click.option('--debug', help='Enable or disable debugging', is_flag=True)
@click.option('--accesslog', help='Enable or disable access log', is_flag=True)
def start(host, port, workers, debug, accesslog):
    """Start the API server with the requested runtime options."""
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
    """Server commands."""


server.add_command(start)

@click.group()
def cli():
    """Initialize the command-line entry point and event loop."""
    # Ensure every CLI invocation has an event loop so libraries relying on
    # asyncio.get_event_loop() behave consistently (e.g. arq worker setup).
    try:
        loop = asyncio.get_event_loop()
        if loop.is_closed():
            raise RuntimeError
    except RuntimeError:
        loop = _new_event_loop()
        asyncio.set_event_loop(loop)
    else:
        # If uvloop is installed (set above), ensure the active loop uses uvloop's implementation.
        if uvloop is not None and not isinstance(loop, uvloop.Loop):
            loop = _new_event_loop()
            asyncio.set_event_loop(loop)


cli.add_command(server)
cli.add_command(process_group, name="start")
cli.add_command(process_group_end, name="finish")
cli.add_command(db_group, name="db")


@click.command(help="Run a single queued ARQ job and exit.")
@click.argument("worker_settings", type=str, required=True)
@click.option("-v", "--verbose", is_flag=True, help=arq.cli.verbose_help)
@click.option("--custom-log-dict", type=str, help=arq.cli.logdict_help)
def worker_once(worker_settings: str, verbose: bool, custom_log_dict: str | None) -> None:
    """Run one job for per-import launcher workers."""
    sys.path.append(os.getcwd())
    settings = import_string(worker_settings)
    log_config = import_string(custom_log_dict) if custom_log_dict else default_log_config(verbose)
    logging.config.dictConfig(log_config)
    worker = _create_single_job_worker(settings)
    worker.run()


def _create_single_job_worker(settings):
    target_job_id = _worker_once_target_job_id()
    scan_limit = _worker_once_scan_limit(settings)
    if target_job_id:
        scan_limit = max(scan_limit, _worker_once_target_scan_limit())
    worker = create_worker(
        settings,
        burst=True,
        max_jobs=1,
        max_burst_jobs=scan_limit,
        queue_read_limit=scan_limit,
    )
    original_start_jobs = worker.start_jobs

    async def start_jobs_once(job_ids):
        """Clamp burst accounting once this worker claims one real job."""
        selected_job_ids = list(job_ids)
        if target_job_id:
            selected_job_ids = [job_id for job_id in selected_job_ids if _job_id_text(job_id) == target_job_id]
            if not selected_job_ids:
                worker.max_burst_jobs = 0
                return None
        jobs_started_before = worker._jobs_started()
        await original_start_jobs(selected_job_ids)
        jobs_started_after = worker._jobs_started()
        if jobs_started_after > jobs_started_before:
            worker.max_burst_jobs = jobs_started_after
        elif target_job_id:
            worker.max_burst_jobs = 0

    worker.start_jobs = start_jobs_once
    return worker


def _worker_once_scan_limit(settings) -> int:
    configured = _positive_int(os.environ.get("HLTHPRT_WORKER_ONCE_QUEUE_SCAN_LIMIT"))
    if configured:
        return configured
    return max(_positive_int(getattr(settings, "queue_read_limit", None)) or 16, 16)


def _worker_once_target_job_id() -> str | None:
    value = os.environ.get("HLTHPRT_WORKER_ONCE_TARGET_JOB_ID")
    return value.strip() if value and value.strip() else None


def _worker_once_target_scan_limit() -> int:
    return _positive_int(os.environ.get("HLTHPRT_WORKER_ONCE_TARGET_SCAN_LIMIT")) or 10000


def _job_id_text(job_id) -> str:
    if isinstance(job_id, bytes):
        return job_id.decode()
    return str(job_id)


def _positive_int(value) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


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

    results = _run_async(
        _sync_structure(add_columns=not skip_columns, add_indexes=not skip_indexes)
    )
    render_sync_summary(results)


@manage.command("rebuild-plan-summary")
@click.option("--test", is_flag=True, help="Use the test database suffix defined via HLTHPRT_TEST_DATABASE_SUFFIX.")
def rebuild_plan_summary(test: bool):
    """Rebuild the plan_search_summary table so /plan/search can use precomputed filters."""
    from process.plan_summary import \
        rebuild_plan_search_summary as _rebuild  # lazy import

    rows = _run_async(_rebuild(test_mode=test))
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


@stop.command("claims-pricing")
@click.option("--burst/--no-burst", default=True, help="Match arq worker burst mode (defaults to burst).")
@click.option("--import-id", help="Override the import_id/import_date passed to worker startup.")
def stop_claims_pricing(burst: bool, import_id: str | None):
    """Drain claims pricing chunk jobs and run the finalize queue."""
    env = os.environ.copy()
    if import_id:
        env["HLTHPRT_IMPORT_ID_OVERRIDE"] = import_id
    else:
        env.pop("HLTHPRT_IMPORT_ID_OVERRIDE", None)

    _run_worker_command("process.ClaimsPricing", burst, env)
    _run_worker_command("process.ClaimsPricing_finish", True, env)


@stop.command("claims-procedures")
@click.option("--burst/--no-burst", default=True, help="Match arq worker burst mode (defaults to burst).")
@click.option("--import-id", help="Override the import_id/import_date passed to worker startup.")
def stop_claims_procedures(burst: bool, import_id: str | None):
    """Drain procedures claims jobs and run the finalize queue."""
    env = os.environ.copy()
    if import_id:
        env["HLTHPRT_IMPORT_ID_OVERRIDE"] = import_id
    else:
        env.pop("HLTHPRT_IMPORT_ID_OVERRIDE", None)

    _run_worker_command("process.ClaimsProcedures", burst, env)
    _run_worker_command("process.ClaimsProcedures_finish", True, env)


@stop.command("drug-claims")
@click.option("--burst/--no-burst", default=True, help="Match arq worker burst mode (defaults to burst).")
@click.option("--import-id", help="Override the import_id/import_date passed to worker startup.")
def stop_drug_claims(burst: bool, import_id: str | None):
    """Drain drug claims jobs and run the finalize queue."""
    env = os.environ.copy()
    if import_id:
        env["HLTHPRT_IMPORT_ID_OVERRIDE"] = import_id
    else:
        env.pop("HLTHPRT_IMPORT_ID_OVERRIDE", None)

    _run_worker_command("process.DrugClaims", burst, env)
    _run_worker_command("process.DrugClaims_finish", True, env)


@stop.command("provider-enrichment")
@click.option("--burst/--no-burst", default=True, help="Match arq worker burst mode (defaults to burst).")
@click.option("--import-id", help="Override the import_id/import_date passed to worker startup.")
def stop_provider_enrichment(burst: bool, import_id: str | None):
    """Drain provider enrichment jobs and trigger NPI-style shutdown publish."""
    env = os.environ.copy()
    if import_id:
        env["HLTHPRT_IMPORT_ID_OVERRIDE"] = import_id
    else:
        env.pop("HLTHPRT_IMPORT_ID_OVERRIDE", None)

    _run_worker_command("process.ProviderEnrichment", burst, env)
    _run_worker_command("process.ProviderEnrichment_finish", True, env)


@stop.command("partd-formulary-network")
@click.option("--burst/--no-burst", default=True, help="Match arq worker burst mode (defaults to burst).")
@click.option("--import-id", help="Override the import_id/import_date passed to worker startup.")
def stop_partd_formulary_network(burst: bool, import_id: str | None):
    """Drain Part D formulary jobs and run the finalize queue."""
    env = os.environ.copy()
    if import_id:
        env["HLTHPRT_IMPORT_ID_OVERRIDE"] = import_id
    else:
        env.pop("HLTHPRT_IMPORT_ID_OVERRIDE", None)

    _run_worker_command("process.PartDFormularyNetwork", burst, env)
    _run_worker_command("process.PartDFormularyNetwork_finish", True, env)


@stop.command("pharmacy-license")
@click.option("--burst/--no-burst", default=True, help="Match arq worker burst mode (defaults to burst).")
@click.option("--import-id", help="Override the import_id/import_date passed to worker startup.")
def stop_pharmacy_license(burst: bool, import_id: str | None):
    """Drain pharmacy-license jobs and run the finalize queue."""
    env = os.environ.copy()
    if import_id:
        env["HLTHPRT_IMPORT_ID_OVERRIDE"] = import_id
    else:
        env.pop("HLTHPRT_IMPORT_ID_OVERRIDE", None)

    _run_worker_command("process.PharmacyLicense", burst, env)
    _run_worker_command("process.PharmacyLicense_finish", True, env)


cli.add_command(stop, name="stop")
cli.add_command(worker_once, name="worker-once")
cli.add_command(arq.cli.cli, name="worker")
cli.add_command(manage, name="manage")


if __name__ == '__main__':
    cli()
