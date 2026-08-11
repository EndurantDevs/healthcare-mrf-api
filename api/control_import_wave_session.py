"""Transactional executor adapter for exact-wave admission."""

from __future__ import annotations

from typing import Any


class SessionExecutor:
    """Expose one active admission transaction through executor methods."""

    def __init__(self, session: Any) -> None:
        self.session = session

    async def execute(
        self,
        statement: Any,
        parameters: dict[str, Any] | None = None,
    ) -> Any:
        """Execute one statement through the active admission transaction."""

        return await self.session.execute(statement, parameters or {})

    async def scalar(self, statement: Any, *args: Any, **parameters: Any) -> Any:
        """Return one scalar through the active admission transaction."""

        values = dict(args[0]) if args else {}
        values.update(parameters)
        return await self.session.scalar(statement, values)

    async def all(self, statement: Any, **parameters: Any) -> list[Any]:
        """Return all rows through the active admission transaction."""

        return (await self.session.execute(statement, parameters)).all()

    async def status(self, statement: Any, **parameters: Any) -> int | None:
        """Return the affected-row count for a transactional statement."""

        return (await self.session.execute(statement, parameters)).rowcount
