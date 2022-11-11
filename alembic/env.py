import logging
from logging.config import fileConfig

from sqlalchemy import engine_from_config
from sqlalchemy import pool

from alembic import context
from db.models import db
import os




# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
fileConfig(config.config_file_name)

# add your model's MetaData object here
# for 'autogenerate' support
# from myapp import mymodel
# target_metadata = mymodel.Base.metadata
target_metadata = db


exclude_tables = [name.strip() for name in config.get_section('alembic:exclude', {}).get('tables', '').split(',')]

def include_object(object, name, type_, reflected, compare_to):
    if type_ == "table" and name in exclude_tables:
        return False
    if hasattr(object, "schema") and object.schema != 'rx_data':
        return False
    else:
        return True

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.


def run_migrations_offline():
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    # url = config.get_main_option("sqlalchemy.url")
    url = 'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_database}'. \
        format(db_user=os.environ.get("DB_USER"),
               db_password=os.environ.get("DB_PASSWORD"),
               db_host=os.environ.get("DB_HOST"),
               db_database=os.environ.get("DB_DATABASE"),
               db_port=os.environ.get("DB_PORT"))
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online():
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """

    url = 'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_database}'. \
        format(db_user=os.environ.get("DB_USER"),
               db_password=os.environ.get("DB_PASSWORD"),
               db_host=os.environ.get("DB_HOST"),
               db_database=os.environ.get("DB_DATABASE"),
               db_port=os.environ.get("DB_PORT"))
    config_dict = {
        'sqlalchemy.url': url,
        'sqlalchemy.connect_args': {'options': f"-c search_path=rx_data,public"}
    }

    connectable = engine_from_config(
        config_dict,
        prefix='sqlalchemy.',
        poolclass=pool.NullPool)

    with connectable.connect() as connection:
        connection.dialect.default_schema_name = 'public'
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            include_object=include_object,
            include_schemas=True,
            version_table_schema='rx_data',
            compare_type=True,
            compare_server_default=True,
        )
        with context.begin_transaction():
            if os.environ.get('ENVIRONMENT') == 'test':
                context.execute('create schema if not exists rx_data')

            context.execute('SET search_path TO rx_data,public')
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
