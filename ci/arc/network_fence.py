#!/usr/bin/env python3
"""Fail a CI job closed when an isolation-boundary endpoint is reachable."""

from __future__ import annotations

import argparse
import asyncio
import sys
from typing import NamedTuple, Sequence

EXIT_INVALID_CONFIGURATION = 64
EXIT_INDETERMINATE = 70
EXIT_NETWORK_REACHABLE = 78
MAX_TARGETS = 128
MAX_TIMEOUT_SECONDS = 10.0


class ConfigurationError(ValueError):
    """Raised when the preflight configuration is unsafe or malformed."""


class _ArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise ConfigurationError(message)


class Target(NamedTuple):
    host: str
    port: int


def _parse_target(value: str) -> Target:
    if value.startswith("["):
        closing_bracket = value.find("]")
        if (
            closing_bracket <= 1
            or value[closing_bracket + 1 : closing_bracket + 2] != ":"
        ):
            raise ConfigurationError("invalid target")
        host = value[1:closing_bracket]
        port_text = value[closing_bracket + 2 :]
    else:
        host, separator, port_text = value.rpartition(":")
        if not separator or ":" in host:
            raise ConfigurationError("invalid target")

    if not host or any(character.isspace() for character in host):
        raise ConfigurationError("invalid target")

    try:
        port = int(port_text)
    except ValueError as error:
        raise ConfigurationError("invalid target") from error
    if not 1 <= port <= 65535:
        raise ConfigurationError("invalid target")

    return Target(host=host, port=port)


def _parse_arguments(argv: Sequence[str]) -> tuple[list[Target], float]:
    parser = _ArgumentParser(add_help=False)
    parser.add_argument("--target", action="append", required=True)
    parser.add_argument("--timeout", type=float, default=1.0)
    namespace = parser.parse_args(argv)

    if not 0.0 < namespace.timeout <= MAX_TIMEOUT_SECONDS:
        raise ConfigurationError("invalid timeout")
    if len(namespace.target) > MAX_TARGETS:
        raise ConfigurationError("too many targets")

    return [_parse_target(value) for value in namespace.target], namespace.timeout


async def _open_connection(
    host: str, port: int
) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    return await asyncio.open_connection(host, port)


async def _probe(target: Target, timeout: float) -> bool:
    try:
        _, writer = await asyncio.wait_for(
            _open_connection(target.host, target.port),
            timeout=timeout,
        )
    except (OSError, TimeoutError):
        return False

    writer.close()
    try:
        await writer.wait_closed()
    except OSError:
        pass
    return True


async def _probe_all(
    targets: Sequence[Target], timeout: float
) -> list[bool | BaseException]:
    return list(
        await asyncio.gather(
            *(_probe(target, timeout) for target in targets),
            return_exceptions=True,
        )
    )


def run(argv: Sequence[str]) -> int:
    try:
        targets, timeout = _parse_arguments(argv)
    except ConfigurationError:
        print("network isolation preflight configuration invalid", file=sys.stderr)
        return EXIT_INVALID_CONFIGURATION

    results = asyncio.run(_probe_all(targets, timeout))
    reachable = sum(result is True for result in results)
    indeterminate = sum(isinstance(result, BaseException) for result in results)
    checked = len(results)
    if reachable:
        print(
            f"network isolation preflight failed: checked={checked} reachable={reachable}",
            file=sys.stderr,
        )
        return EXIT_NETWORK_REACHABLE
    if indeterminate:
        print(
            "network isolation preflight failed: "
            f"checked={checked} reachable=0 indeterminate={indeterminate}",
            file=sys.stderr,
        )
        return EXIT_INDETERMINATE

    print(f"network isolation preflight passed: checked={checked} reachable=0")
    return 0


if __name__ == "__main__":
    raise SystemExit(run(sys.argv[1:]))
