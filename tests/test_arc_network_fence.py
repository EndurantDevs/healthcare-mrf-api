"""Behavior and disclosure tests for the ARC network-isolation preflight."""

from __future__ import annotations

import asyncio
import importlib.util
from pathlib import Path
from types import ModuleType

import pytest

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
FENCE_PATH = REPOSITORY_ROOT / "ci" / "arc" / "network_fence.py"


def _load_fence() -> ModuleType:
    spec = importlib.util.spec_from_file_location("arc_network_fence", FENCE_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def fence() -> ModuleType:
    return _load_fence()


def test_returns_success_only_when_every_target_is_blocked(
    fence: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    async def all_blocked(targets: object, timeout: float) -> list[bool]:
        assert timeout == 0.25
        return [False, False]

    monkeypatch.setattr(fence, "_probe_all", all_blocked)
    addresses = ("blocked-one.invalid:443", "blocked-two.invalid:8443")

    assert (
        fence.run(
            ["--timeout", "0.25", "--target", addresses[0], "--target", addresses[1]]
        )
        == 0
    )

    captured = capsys.readouterr()
    assert captured.out == "network isolation preflight passed: checked=2 reachable=0\n"
    assert captured.err == ""
    assert all(address not in captured.out for address in addresses)


def test_returns_78_when_any_target_is_reachable_without_disclosing_targets(
    fence: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    async def one_reachable(targets: object, timeout: float) -> list[bool]:
        return [False, True]

    monkeypatch.setattr(fence, "_probe_all", one_reachable)
    addresses = ("blocked.invalid:443", "reachable.invalid:8443")

    assert fence.run(["--target", addresses[0], "--target", addresses[1]]) == 78

    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == "network isolation preflight failed: checked=2 reachable=1\n"
    assert all(address not in captured.err for address in addresses)


@pytest.mark.parametrize(
    "arguments",
    [
        [],
        ["--target", "missing-port.invalid"],
        ["--target", "space in host.invalid:443"],
        ["--target", "host.invalid:0"],
        ["--target", "host.invalid:65536"],
        ["--target", "host.invalid:443", "--timeout", "0"],
        ["--target", "host.invalid:443", "--timeout", "11"],
    ],
)
def test_invalid_configuration_fails_closed_without_echoing_input(
    fence: ModuleType,
    arguments: list[str],
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert fence.run(arguments) == fence.EXIT_INVALID_CONFIGURATION

    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == "network isolation preflight configuration invalid\n"
    assert all(argument not in captured.err for argument in arguments)


def test_bracketed_ipv6_target_is_supported(fence: ModuleType) -> None:
    targets, timeout = fence._parse_arguments(["--target", "[2001:db8::1]:443"])

    assert targets == [fence.Target(host="2001:db8::1", port=443)]
    assert timeout == 1.0


def test_probes_are_started_concurrently(
    fence: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    probe_counts_by_state = {"active": 0, "peak": 0}

    async def blocked_connection(host: str, port: int) -> object:
        probe_counts_by_state["active"] += 1
        probe_counts_by_state["peak"] = max(
            probe_counts_by_state["peak"], probe_counts_by_state["active"]
        )
        await asyncio.sleep(0.01)
        probe_counts_by_state["active"] -= 1
        raise OSError("blocked")

    monkeypatch.setattr(fence, "_open_connection", blocked_connection)
    targets = [fence.Target("one.invalid", 443), fence.Target("two.invalid", 443)]

    assert asyncio.run(fence._probe_all(targets, timeout=0.25)) == [False, False]
    assert probe_counts_by_state["peak"] == 2


def test_probe_timeout_is_treated_as_blocked(
    fence: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def slow_connection(host: str, port: int) -> object:
        await asyncio.sleep(0.05)
        raise AssertionError("the connection attempt should have timed out")

    monkeypatch.setattr(fence, "_open_connection", slow_connection)

    assert asyncio.run(fence._probe(fence.Target("slow.invalid", 443), 0.001)) is False


def test_unexpected_probe_error_fails_closed_without_disclosing_target(
    fence: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    target = "sensitive.invalid:443"

    async def unexpected_error(host: str, port: int) -> object:
        raise RuntimeError(f"unexpected failure for {host}:{port}")

    monkeypatch.setattr(fence, "_open_connection", unexpected_error)

    assert fence.run(["--target", target]) == fence.EXIT_INDETERMINATE

    captured = capsys.readouterr()
    assert captured.out == ""
    assert (
        captured.err
        == "network isolation preflight failed: checked=1 reachable=0 indeterminate=1\n"
    )
    assert target not in captured.err
