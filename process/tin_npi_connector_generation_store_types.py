# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Private async database types for connector generation storage."""

from __future__ import annotations

from abc import abstractmethod
from collections.abc import Mapping, Sequence
from contextlib import AbstractAsyncContextManager
from typing import Protocol

from process.tin_npi_connector_publication import (
    TinNpiConnectorPublicationError,
)


class ConnectorGenerationStoreConnection(Protocol):
    """Narrow asyncpg-compatible surface required by the inert store."""

    @abstractmethod
    def transaction(self) -> AbstractAsyncContextManager[object]:
        """Create the top-level transaction owned by the store."""

    @abstractmethod
    def is_in_transaction(self) -> bool:
        """Return whether the connection is already inside a transaction."""

    @abstractmethod
    async def execute(self, sql: str, *arguments: object) -> str:
        """Execute one statement and return its command status."""

    @abstractmethod
    async def fetchval(self, sql: str, *arguments: object) -> object:
        """Return the first scalar produced by one statement."""

    @abstractmethod
    async def fetchrow(
        self,
        sql: str,
        *arguments: object,
    ) -> Mapping[str, object] | None:
        """Return one mapping-shaped database row when present."""

    @abstractmethod
    async def copy_records_to_table(
        self,
        table_name: str,
        *,
        schema_name: str,
        columns: Sequence[str],
        records: Sequence[tuple[object, ...]],
    ) -> str:
        """COPY one bounded record batch into an exact table."""


class TinNpiConnectorGenerationStoreError(TinNpiConnectorPublicationError):
    """Reject an unsafe database load without exposing identity material."""
