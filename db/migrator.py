# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from pathlib import Path
import click
from dotenv import load_dotenv
from alembic.config import Config
from alembic.command import upgrade
from alembic.command import revision
from alembic.command import history as show_history
from alembic.command import current as show_current
from alembic.command import downgrade as make_downgrade


BASE_DIR = (Path(__file__).parent / '..').absolute()
ALEMBIC_INI = BASE_DIR / 'alembic.ini'


@click.group()
def db_group():
    pass


@click.command(help="Downgrade to revision")
@click.option("-r", help="Revision", default="-1")
def downgrade(r):
    alembic_cfg = Config(ALEMBIC_INI)
    make_downgrade(alembic_cfg, r)


@click.command(help="Apply migrations")
@click.option("-r", help="Revision (head by default)", default="head")
def migrate(r):
    alembic_cfg = Config(ALEMBIC_INI)
    upgrade(alembic_cfg, r)


@click.command(help="Auto generate migrations")
@click.option("-m", help="Migration message", required=True)
def generate(m):
    alembic_cfg = Config(ALEMBIC_INI)
    revision_kwargs = {'autogenerate': True}
    if m is not None:
        revision_kwargs['message'] = m
    revision(alembic_cfg, **revision_kwargs)


@click.command(help="List changeset scripts in chronological order")
def history():
    alembic_cfg = Config(ALEMBIC_INI)
    show_history(alembic_cfg)


@click.command(help="Show current revision")
def current():
    alembic_cfg = Config(ALEMBIC_INI)
    show_current(alembic_cfg)


db_group.add_command(migrate)
db_group.add_command(generate)
db_group.add_command(history)
db_group.add_command(current)
db_group.add_command(downgrade)


if __name__ == "__main__":
    env_path = BASE_DIR / '.env'
    load_dotenv(dotenv_path=env_path)
    db_group()