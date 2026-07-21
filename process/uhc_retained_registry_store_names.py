# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Safe PostgreSQL names shared by the UHC retained registry modules."""

import os
import re

from process.uhc_retained_types import UHCRetainedAdmissionError


_IDENTIFIER_RE = re.compile(r"[a-z_][a-z0-9_]{0,62}")


def schema_name() -> str:
    """Return the configured, safely quoted UHC registry schema name."""

    configured_schema = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    if not _IDENTIFIER_RE.fullmatch(configured_schema):
        raise UHCRetainedAdmissionError("invalid UHC registry schema")
    return configured_schema


def table_name(table: str) -> str:
    """Return one safely schema-qualified UHC registry table."""

    if not _IDENTIFIER_RE.fullmatch(table):
        raise UHCRetainedAdmissionError("invalid UHC registry table")
    return f'"{schema_name()}"."{table}"'
