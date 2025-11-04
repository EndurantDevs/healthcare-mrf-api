"""Backward-compatible entry points for the legacy GINO imports.

The project now uses the async SQLAlchemy infrastructure exposed from
``db.sqlalchemy``.  Several modules still import ``db`` and ``init_db`` from
this module, so we simply re-export the new helpers here to avoid a sweeping
renaming pass.
"""

from __future__ import annotations

from typing import Any

from db.sqlalchemy import db as sa_db


async def init_db(_: Any = None, loop: Any = None) -> None:
    """Retained for backwards compatibility with older importers."""
    await sa_db.connect()


# Re-export for ``from db.connection import db`` call sites.
db = sa_db


__all__ = ["db", "init_db"]
