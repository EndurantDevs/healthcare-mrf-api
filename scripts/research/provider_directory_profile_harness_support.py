# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Infrastructure helpers for the disposable profile SQL harness."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import re
import time

import asyncpg

from process import provider_directory_profile as profile


def decoded(json_value: object) -> object:
    """Decode asyncpg JSON text while preserving already-decoded values."""
    return json.loads(json_value) if isinstance(json_value, str) else json_value


def table_ref(schema: str, table_name: str) -> str:
    """Return one safely quoted profile harness table reference."""
    return profile.qualified_table(schema, table_name)


def arguments(description: str) -> argparse.Namespace:
    """Parse connection and lifecycle options for the profile harness."""
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--dsn", default=os.getenv("HLTHPRT_DB_DSN"))
    parser.add_argument(
        "--host",
        default=os.getenv("HLTHPRT_DB_HOST", "127.0.0.1"),
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.getenv("HLTHPRT_DB_PORT", "5432")),
    )
    parser.add_argument("--user", default=os.getenv("HLTHPRT_DB_USER"))
    parser.add_argument(
        "--database",
        default=os.getenv("HLTHPRT_DB_DATABASE"),
    )
    parser.add_argument(
        "--password",
        default=os.getenv("HLTHPRT_DB_PASSWORD") or os.getenv("PGPASSWORD"),
    )
    parser.add_argument("--keep-schema", action="store_true")
    return parser.parse_args()


def schema_name() -> str:
    """Return a collision-resistant disposable schema name."""
    token = f"{os.getpid()}:{time.time_ns()}"
    digest = hashlib.sha1(token.encode("ascii")).hexdigest()[:12]
    return f"pd_profile_harness_{digest}"


def bind(sql: str, *parameter_names: str) -> str:
    """Translate named SQL parameters to asyncpg positional parameters."""
    bound_sql = sql
    for index, parameter_name in enumerate(parameter_names, start=1):
        bound_sql = re.sub(
            rf":{re.escape(parameter_name)}\b",
            "$" + str(index),
            bound_sql,
        )
    unresolved_tokens = re.findall(
        r"(?<!:):[a-zA-Z_][a-zA-Z0-9_]*",
        bound_sql,
    )
    if unresolved_tokens:
        raise RuntimeError(
            "provider_directory_profile_harness_unbound_parameters:"
            + ",".join(sorted(set(unresolved_tokens)))
        )
    return bound_sql


async def connect(
    connection_options: argparse.Namespace,
) -> asyncpg.Connection:
    """Open the explicitly configured disposable PostgreSQL connection."""
    if connection_options.dsn:
        return await asyncpg.connect(connection_options.dsn)
    missing_settings = [
        setting_name
        for setting_name in ("user", "database", "password")
        if not getattr(connection_options, setting_name)
    ]
    if missing_settings:
        raise RuntimeError(
            "provider_directory_profile_harness_db_config_missing:"
            + ",".join(missing_settings)
        )
    return await asyncpg.connect(
        host=connection_options.host,
        port=connection_options.port,
        user=connection_options.user,
        password=connection_options.password,
        database=connection_options.database,
    )


def run(coroutine: object) -> object:
    """Run the harness coroutine without exposing event-loop policy details."""
    return asyncio.run(coroutine)
