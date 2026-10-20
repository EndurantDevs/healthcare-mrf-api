"""Read compact branch coverage as bounded data, without source configuration or plugins."""

from __future__ import annotations

import itertools
import sqlite3
from contextlib import closing
from pathlib import Path, PurePosixPath

from coverage import CoverageData
from coverage.exceptions import CoverageException

SQL_COLUMNS = {
    "coverage_schema": ("version",),
    "meta": ("key", "value"),
    "file": ("id", "path"),
    "context": ("id", "context"),
    "line_bits": ("file_id", "context_id", "numbits"),
    "arc": ("file_id", "context_id", "fromno", "tono"),
    "tracer": ("file_id", "tracer"),
}


def _validate_tables(database: sqlite3.Connection) -> None:
    """Permit only coverage.py tables, ordinary columns, and native uniqueness indexes."""

    schema_rows = database.execute("SELECT type, name, sql FROM sqlite_master").fetchall()
    table_names = {name for kind, name, _ in schema_rows if kind == "table"}
    if table_names != set(SQL_COLUMNS) or any(
        (kind == "table" and (not isinstance(sql, str) or not sql.upper().startswith("CREATE TABLE ")))
        or (kind != "table" and not (kind == "index" and name.startswith("sqlite_autoindex_") and sql is None))
        for kind, name, sql in schema_rows
    ):
        raise ValueError("unexpected coverage SQLite schema")
    for table, columns in SQL_COLUMNS.items():
        column_rows = database.execute(f"PRAGMA table_xinfo({table})").fetchall()
        if tuple(column[1] for column in column_rows) != columns or any(column[6] != 0 for column in column_rows):
            raise ValueError("unexpected coverage SQLite columns")


def _validate_metadata(database: sqlite3.Connection) -> None:
    """Refuse unsupported data versions, line-only reports, plugins, and external paths."""

    if database.execute("SELECT version FROM coverage_schema").fetchall() != [(7,)]:
        raise ValueError("unexpected coverage SQLite version")
    if database.execute("SELECT value FROM meta WHERE key = 'has_arcs'").fetchall() != [("1",)]:
        raise ValueError("branch coverage is required")
    if database.execute("SELECT count(*) FROM tracer WHERE tracer != ''").fetchone()[0]:
        raise ValueError("coverage plugins are not accepted")
    for (name,) in database.execute("SELECT path FROM file"):
        relative_path = PurePosixPath(name)
        if (
            not relative_path.parts
            or relative_path.is_absolute()
            or ".." in relative_path.parts
            or "\\" in name
            or not (
                name == "main.py" or relative_path.parts[0] in {"api", "db", "process", "public_evidence", "service"}
            )
        ):
            raise ValueError("coverage path is outside application source")


def validate_cohort_data(path: Path) -> None:
    """Fail closed before coverage.py can silently skip an invalid cohort database."""

    try:
        with path.open("rb") as coverage_file:
            if coverage_file.read(16) != b"SQLite format 3\0":
                raise ValueError("coverage data must be SQLite")
        with closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro&immutable=1", uri=True)) as database:
            database.execute("PRAGMA trusted_schema=OFF")
            database.execute("PRAGMA query_only=ON")
            progress_calls = itertools.count()
            database.set_progress_handler(lambda: next(progress_calls) > 10000, 1000)
            _validate_tables(database)
            _validate_metadata(database)
            if database.execute("PRAGMA quick_check").fetchall() != [("ok",)]:
                raise ValueError("corrupt coverage SQLite data")
        measured = CoverageData(basename=str(path))
        measured.read()
        measured.measured_contexts()
    except (OSError, TypeError, sqlite3.Error, CoverageException) as exc:
        raise ValueError("invalid cohort coverage data") from exc
