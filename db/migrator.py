# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import sys
from pathlib import Path

import click
from alembic.command import current as show_current
from alembic.command import downgrade as make_downgrade
from alembic.command import history as show_history
from alembic.command import revision, upgrade
from alembic.config import Config
from dotenv import load_dotenv

BASE_DIR = (Path(__file__).parent / "..").absolute()
ALEMBIC_INI = BASE_DIR / "alembic.ini"


@click.group()
def db_group():
    """Manage the configured database."""
    return None


@click.command(help="Downgrade to revision")
@click.option("-r", help="Revision", default="-1")
def downgrade(r):
    """Downgrade the database by the requested revision count."""
    alembic_cfg = Config(ALEMBIC_INI)
    make_downgrade(alembic_cfg, r)


@click.command(help="Apply migrations")
@click.option("-r", help="Revision (head by default)", default="head")
def migrate(r):
    """Upgrade the database to the requested revision."""
    alembic_cfg = Config(ALEMBIC_INI)
    upgrade(alembic_cfg, r)


@click.command(help="Auto generate migrations")
@click.option("-m", help="Migration message", required=True)
def generate(m):
    """Generate a migration revision with the supplied message."""
    alembic_cfg = Config(ALEMBIC_INI)
    revision_option_by_name = {"autogenerate": True}
    if m is not None:
        revision_option_by_name["message"] = m
    revision(alembic_cfg, **revision_option_by_name)


@click.command(help="List changeset scripts in chronological order")
def history():
    """Print the available database migration history."""
    alembic_cfg = Config(ALEMBIC_INI)
    show_history(alembic_cfg)


@click.command(help="Show current revision")
def current():
    """Print the database's current migration revision."""
    alembic_cfg = Config(ALEMBIC_INI)
    show_current(alembic_cfg)


async def _bootstrap_local_npi_generation() -> int:
    from db.connection import Database
    from process.npi import _runtime_db_schema
    from process.npi_result_generation import bootstrap_npi_result_generation

    schema = _runtime_db_schema()
    database = Database()
    try:
        async with database.transaction() as session:
            authority = await bootstrap_npi_result_generation(session, schema_name=schema, local_only=True)
        return authority.local_generation
    finally:
        await database.disconnect()


@click.command("bootstrap-local-npi-generation")
def bootstrap_local_npi_generation() -> None:
    """Publish local NPI generation one from the existing serving tables."""
    try:
        generation = asyncio.run(_bootstrap_local_npi_generation())
    except Exception:
        raise click.ClickException(
            "Local NPI generation bootstrap failed. It requires generation zero and all six serving tables."
        ) from None
    click.echo(f"Published local NPI generation {generation}.")


db_group.add_command(migrate)
db_group.add_command(generate)
db_group.add_command(history)
db_group.add_command(current)
db_group.add_command(downgrade)
db_group.add_command(bootstrap_local_npi_generation)


if __name__ == "__main__":
    if not __package__:
        sys.path.insert(0, str(BASE_DIR.resolve()))
    env_path = BASE_DIR / ".env"
    load_dotenv(dotenv_path=env_path)
    db_group()
