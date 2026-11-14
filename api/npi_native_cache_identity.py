# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bypass native NPI caches until ordinary writes advance a portable revision."""

import re

from sanic.exceptions import ServiceUnavailable
from sqlalchemy.exc import DBAPIError

from process.custom_import.read_core import _is_statement_timeout


async def npi_native_cache_identity(execute, schema, *, session=None):
    """A copied family can retain counters that ordinary writes no longer advance."""
    if type(schema) is not str or re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,62}", schema) is None:
        raise ValueError("NPI cache schema is invalid")
    # shortcut: no portable ordinary-write invalidation contract exists; keep native results uncached until one does.
    return None


async def npi_publication_cache_identity(identity_rows, execute, schema, logger, *, session=None):
    """Prefer one valid legacy receipt and bypass caching when native lookup fails."""
    if not identity_rows:
        try:
            return await npi_native_cache_identity(execute, schema, session=session)
        except ServiceUnavailable, TimeoutError:
            raise
        except Exception as exc:
            if isinstance(exc, DBAPIError) and _is_statement_timeout(exc):
                raise
            logger.debug("Native NPI revision identity unavailable; bypassing cache (%s)", type(exc).__name__)
            return None
    if len(identity_rows) != 1:
        return None
    publication_generation, publication_ref = identity_rows[0]
    if (
        type(publication_generation) is not int
        or publication_generation < 1
        or type(publication_ref) is not str
        or len(publication_ref) != 50
        or not publication_ref.startswith("nppub1_")
    ):
        return None
    return f"{publication_generation}:{publication_ref}"


async def npi_canonical_publication_identity(execute, schema, logger, *, receipt_table, seal_table, session=None):
    """Use the current logical revision; historical receipts alone cannot key live data."""
    return await npi_publication_cache_identity([], execute, schema, logger, session=session)
