# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Publish one persisted managed catalog attempt with dedicated operator settings."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
from dataclasses import dataclass, field
from uuid import UUID

from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

PREFIX = "HLTHPRT_SCOPED_CATALOG_PUBLISHER_"
_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,62}\Z")


@dataclass(frozen=True)
class PublisherSettings:
    dsn: str = field(repr=False)
    schema: str
    owner_role: str
    reader_roles: tuple[str, ...]
    max_copy_bytes: int
    timeout_seconds: int

    @classmethod
    def from_environment(cls, environment=None):
        """Require explicit operator configuration without ordinary application fallbacks."""
        environment = os.environ if environment is None else environment
        try:
            values_by_field = {
                name: environment[PREFIX + name]
                for name in ("DSN", "SCHEMA", "OWNER_ROLE", "READER_ROLES", "COPY_MAX_BYTES", "TIMEOUT_SECONDS")
            }
            url = make_url(values_by_field["DSN"])
            readers = json.loads(values_by_field["READER_ROLES"])
            if url.drivername not in {"postgresql", "postgresql+asyncpg"} or not url.database or not url.username:
                raise ValueError
            names = (values_by_field["SCHEMA"], values_by_field["OWNER_ROLE"], *readers)
            if (
                type(readers) is not list
                or not 1 <= len(readers) <= 32
                or len(set(readers)) != len(readers)
                or any(
                    type(name) is not str or not _IDENTIFIER.fullmatch(name) or name.lower() == "public"
                    for name in names
                )
                or values_by_field["OWNER_ROLE"] in readers
            ):
                raise ValueError
            return cls(
                url.set(drivername="postgresql+asyncpg").render_as_string(hide_password=False),
                values_by_field["SCHEMA"],
                values_by_field["OWNER_ROLE"],
                tuple(readers),
                _integer(values_by_field["COPY_MAX_BYTES"], 1, 2**31),
                _integer(values_by_field["TIMEOUT_SECONDS"], 1, 600),
            )
        except KeyError, TypeError, ValueError, ArgumentError:
            raise ValueError("publisher configuration is invalid") from None


@dataclass(frozen=True)
class ManagedAttempt:
    run_id: str
    attempt_id: str
    attempt_started_at: str
    node_id: str
    expected_lineage_id: str
    expected_generation: int


def _integer(value, minimum, maximum):
    number = int(value)
    if str(number) != value or not minimum <= number <= maximum:
        raise ValueError("integer is invalid")
    return number


def _selector(value):
    if not value or len(value) > 128 or any(ord(character) < 32 for character in value):
        raise argparse.ArgumentTypeError("invalid selector")
    return value


class _Parser(argparse.ArgumentParser):
    def error(self, _message):
        """Return a fixed parser error without echoing selector values."""
        self.exit(2, '{"status":"error","code":"invalid_arguments"}\n')


def parse_attempt(arguments=None):
    """Parse only the six bounded logical selectors for one managed attempt."""
    parser = _Parser(description=__doc__)
    for name in ("run-id", "attempt-id", "attempt-started-at", "node-id", "expected-lineage-id"):
        parser.add_argument("--" + name, required=True, type=_selector)
    parser.add_argument("--expected-generation", required=True, type=lambda value: _integer(value, 0, 2**63 - 1))
    parsed = parser.parse_args(arguments)
    try:
        lineage = UUID(parsed.expected_lineage_id)
        if not lineage.int or str(lineage) != parsed.expected_lineage_id:
            raise ValueError
    except ValueError:
        parser.error("invalid lineage")
    return ManagedAttempt(**vars(parsed))


async def run_publisher(settings, attempt):
    """Own one transaction, its native COPY resource bounds and connection cleanup."""
    from process import reference_family_archive as native
    from process.scoped_catalog_named_publication import publish_managed_attempt

    engine = create_async_engine(settings.dsn, poolclass=NullPool, hide_parameters=True)
    try:
        async with asyncio.timeout(settings.timeout_seconds):
            from sqlalchemy.ext.asyncio import AsyncSession

            async with AsyncSession(engine) as session:
                async with session.begin():
                    from sqlalchemy import text

                    await session.execute(text("SET TRANSACTION ISOLATION LEVEL READ COMMITTED"))
                    result = await publish_managed_attempt(
                        session,
                        attempt,
                        settings,
                        source_copy=native.ReferenceFamilySourceCopy(
                            native.native_copy_projection, settings.max_copy_bytes, settings.timeout_seconds
                        ),
                    )
            return result
    finally:
        await engine.dispose()


def main(arguments=None):
    """Publish the selected persisted attempt and emit a sanitized terminal result."""
    attempt = parse_attempt(arguments)
    try:
        settings = PublisherSettings.from_environment()
        result = asyncio.run(run_publisher(settings, attempt))
    except ValueError:
        print('{"status":"error","code":"configuration"}')
        return 2
    except TimeoutError:
        print('{"status":"error","code":"timeout"}')
        return 1
    except Exception:
        print('{"status":"error","code":"publication"}')
        return 1
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
