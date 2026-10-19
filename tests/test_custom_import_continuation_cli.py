# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""The fixed processing worker starts at retained GRAPH, not at source replay."""

import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process.custom_import import continuation_cli as cli
from process.custom_import.runner_types import CancellationRequested, LeaseAuthorityLost


@asynccontextmanager
async def _connected(_database):
    yield


@asynccontextmanager
async def _transport(_authorization, _request):
    yield None


class _Session:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None

    def begin(self):
        return self


@pytest.fixture
def worker(monkeypatch):
    database = SimpleNamespace(session_factory=Mock(return_value=_Session()))
    request = SimpleNamespace(execution_id=17, fence=3, continuation=SimpleNamespace(build_id=29, continuation_id=41))
    output = SimpleNamespace(generation_id=43, no_change=None)
    monkeypatch.setattr(cli, "_connected_worker_database", _connected)
    monkeypatch.setattr(cli, "continuation_projection_transport", _transport)
    monkeypatch.setattr(cli, "_set_timeout", AsyncMock())
    monkeypatch.setattr(cli, "start_build_continuation", AsyncMock(return_value=request))
    monkeypatch.setattr(cli, "build_graph", AsyncMock(return_value=43))
    monkeypatch.setattr(cli, "build_output", AsyncMock(return_value=output))
    monkeypatch.setattr(cli, "_activate", AsyncMock(return_value=object()))
    monkeypatch.setattr(cli, "finish_execution", AsyncMock(return_value=SimpleNamespace(state="canceled")))
    return database, request, output


async def test_worker_only_composes_retained_graph_output_and_ordinary_publication(worker):
    database, request, _output = worker
    receipt = await cli.run_continuation(database, {}, b"n" * 32, lease_seconds=3600)
    assert receipt == {
        "status": "activated",
        "execution_id": 17,
        "generation_id": 43,
        "build_id": 29,
        "continuation_id": 41,
    }
    cli.build_graph.assert_awaited_once_with(database.session_factory, request, 29)
    cli.build_output.assert_awaited_once_with(database.session_factory, request, 29, projection_transport=None)
    cli.finish_execution.assert_not_awaited()


@pytest.mark.parametrize(
    "no_change,published,status", [(False, False, "sealed_unpublished"), (True, False, "no_change")]
)
async def test_worker_does_not_invent_pointer_success(worker, no_change, published, status):
    database, _request, output = worker
    output.no_change = object() if no_change else None
    cli._activate.return_value = object() if published else None
    assert (await cli.run_continuation(database, {}, b"n" * 32, lease_seconds=3600))["status"] == status
    if no_change:
        cli._activate.assert_not_awaited()


@pytest.mark.parametrize("lost", [False, True])
async def test_worker_cancellation_never_claims_success_after_authority_loss(worker, lost):
    database, *_ = worker
    cli.build_graph.side_effect = CancellationRequested()
    cli.finish_execution.return_value = SimpleNamespace(state="running" if lost else "canceled")
    receipt = await cli.run_continuation(database, {}, b"n" * 32, lease_seconds=3600)
    assert receipt["status"] == ("lease_lost" if lost else "canceled")
    assert cli.finish_execution.await_args.kwargs["continuation_id"] == 41
    cli._activate.assert_not_awaited()


async def test_lost_lease_does_not_finish_or_activate(worker):
    database, *_ = worker
    cli.build_output.side_effect = LeaseAuthorityLost()
    assert (await cli.run_continuation(database, {}, b"n" * 32, lease_seconds=3600))["status"] == "lease_lost"
    cli.finish_execution.assert_not_awaited()
    cli._activate.assert_not_awaited()


def test_input_rejects_arbitrary_files_before_open(tmp_path):
    with pytest.raises(ValueError, match="fixed paths"):
        cli._read(tmp_path / "nonce", "nonce", 32)


def test_command_failure_uses_closed_diagnostics_not_exception_text(monkeypatch, capsys):
    monkeypatch.setattr(cli, "_inputs", Mock(side_effect=ValueError("synthetic-private-payload")))
    assert (
        cli.main(
            [
                "run",
                "--authorization-file",
                str(cli._DIRECTORY / "authorization.json"),
                "--nonce-file",
                str(cli._DIRECTORY / "nonce"),
            ]
        )
        == 1
    )
    output = capsys.readouterr().out
    assert "synthetic-private" not in output
    assert json.loads(output)["diagnostic"]["category"] == "invalid_state"
