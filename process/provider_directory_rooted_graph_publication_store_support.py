# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Safe schema and row helpers for rooted publication stores."""

from __future__ import annotations

import os
import re
from typing import Any, Mapping

from process.provider_directory_rooted_graph_publication_contract import (
    ProviderDirectoryRootedGraphPublicationError,
)


def publication_table(name: str) -> str:
    """Return one safely quoted rooted publication relation."""

    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise ProviderDirectoryRootedGraphPublicationError("state")
    schema = runtime_schema or legacy_schema or "mrf"
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema) is None:
        raise ProviderDirectoryRootedGraphPublicationError("state")
    return f'"{schema}"."{name}"'


def publication_row_fields(database_row: Any) -> dict[str, Any]:
    """Normalize a SQLAlchemy, asyncpg, or fake publication row."""

    if database_row is None:
        return {}
    mapping = (
        database_row._mapping if hasattr(database_row, "_mapping") else database_row
    )
    if not isinstance(mapping, Mapping):
        raise ProviderDirectoryRootedGraphPublicationError("state")
    return dict(mapping)


__all__ = ("publication_row_fields", "publication_table")
