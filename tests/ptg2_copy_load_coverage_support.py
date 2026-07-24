from __future__ import annotations

from contextlib import asynccontextmanager
from types import SimpleNamespace
from typing import Any


class CopyDriver:
    """Capture COPY calls while consuming file-like sources."""

    def __init__(self) -> None:
        self.record_calls: list[dict[str, Any]] = []
        self.source_calls: list[dict[str, Any]] = []

    async def copy_records_to_table(self, table_name: str, **options: Any) -> None:
        self.record_calls.append({"table_name": table_name, **options})

    async def copy_to_table(self, table_name: str, *, source, **options: Any) -> None:
        self.source_calls.append(
            {
                "table_name": table_name,
                "payload": source.read(),
                **options,
            }
        )


class CopyConnection:
    """Expose status and raw-driver behavior used by COPY helpers."""

    def __init__(self, driver: Any, *, wrapped_driver: bool = True) -> None:
        self.status_calls: list[str] = []
        self.raw_connection = (
            SimpleNamespace(driver_connection=driver)
            if wrapped_driver
            else driver
        )

    async def status(self, statement: str) -> None:
        self.status_calls.append(statement)


class CopyDatabase:
    """Yield one connection and record acquire activity."""

    def __init__(self, connection: CopyConnection) -> None:
        self.connection = connection
        self.acquire_count = 0

    @asynccontextmanager
    async def acquire(self):
        self.acquire_count += 1
        yield self.connection


class Column:
    """Minimal SQLAlchemy-like column metadata."""

    def __init__(self, name: str, *, json_type: bool = False) -> None:
        self.name = name
        type_name = "JSONB" if json_type else "Text"
        self.type = type(type_name, (), {})()


def model(
    *column_names: str,
    json_columns: tuple[str, ...] = (),
    primary_key: tuple[str, ...] = ("id",),
    conflict_targets: tuple[str, ...] | None = None,
    initial_indexes: list[dict[str, Any]] | None = None,
    schema: str | None = "source_schema",
    table_name: str = "sample",
):
    """Build model metadata accepted by the production COPY helpers."""

    columns = [
        Column(column_name, json_type=column_name in json_columns)
        for column_name in column_names
    ]
    result = SimpleNamespace(
        __tablename__=table_name,
        __table__=SimpleNamespace(
            c=columns,
            primary_key=[SimpleNamespace(name=name) for name in primary_key],
            schema=schema,
        ),
    )
    if conflict_targets is not None:
        result.__my_index_elements__ = list(conflict_targets)
    if initial_indexes is not None:
        result.__my_initial_indexes__ = initial_indexes
    return result


def install_capture(monkeypatch, copy_load, *, wrapped_driver: bool = True):
    """Install a capture database plus observable guard and schema resolver."""

    driver = CopyDriver()
    connection = CopyConnection(driver, wrapped_driver=wrapped_driver)
    database = CopyDatabase(connection)
    guard_calls: list[dict[str, Any]] = []

    async def guard_attempt_rows(connection_arg, database_arg, **options):
        guard_calls.append(
            {
                "connection": connection_arg,
                "database": database_arg,
                **options,
            }
        )

    monkeypatch.setattr(copy_load, "db", database)
    monkeypatch.setattr(copy_load, "guard_attempt_rows", guard_attempt_rows)
    monkeypatch.setattr(
        copy_load,
        "resolve_ptg2_schema",
        lambda schema=None: schema or "resolved_schema",
    )
    return SimpleNamespace(
        database=database,
        connection=connection,
        driver=driver,
        guard_calls=guard_calls,
    )
