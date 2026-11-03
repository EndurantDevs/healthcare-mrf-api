# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Keep existing worker CLI options and isolate the catalog worker factory."""

from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from click.testing import CliRunner

import main
import process
from process import registry_fhir_catalog_queue as queue


@pytest.mark.parametrize("arguments", [["process.MRF", "--burst"], ["process.RegistryFHIRCatalogSync", "--check"]])
def test_regular_workers_and_health_checks_keep_arq_cli(monkeypatch, arguments):
    original = Mock()
    monkeypatch.setattr(main.arq.cli.cli, "callback", original)
    result = CliRunner().invoke(main.cli, ["worker", *arguments])
    assert result.exit_code == 0, result.output
    assert original.call_args.kwargs["worker_settings"] == arguments[0]


@pytest.mark.parametrize("burst", ["--burst", "--no-burst"])
def test_existing_catalog_command_uses_owned_worker_factory(monkeypatch, burst):
    created = Mock(return_value=SimpleNamespace(run=Mock()))
    monkeypatch.setattr(queue, "create_fhir_catalog_worker", created)
    result = CliRunner().invoke(main.cli, ["worker", "process.RegistryFHIRCatalogSync", burst])
    assert result.exit_code == 0, result.output
    created.assert_called_once_with(process.RegistryFHIRCatalogSync, burst=burst == "--burst")
    created.return_value.run.assert_called_once_with()
    assert process.RegistryFHIRCatalogSync.functions[0].name == "control_single_job_start"
    assert process.RegistryFHIRCatalogSync.job_timeout is None


def test_worker_once_uses_same_catalog_factory(monkeypatch):
    async def start_jobs(_job_ids):
        return None

    created = Mock(return_value=SimpleNamespace(start_jobs=start_jobs, _jobs_started=lambda: 0))
    monkeypatch.setattr(queue, "create_fhir_catalog_worker", created)
    main._create_single_job_worker(process.RegistryFHIRCatalogSync)
    assert created.call_args.args == (process.RegistryFHIRCatalogSync,)
    assert created.call_args.kwargs["max_jobs"] == 1


def test_catalog_watch_uses_owned_worker_lifecycle(monkeypatch, tmp_path):
    watched = Mock()
    run = Mock()
    monkeypatch.setattr(main, "_watch_catalog_worker", watched)
    monkeypatch.setattr(main.asyncio, "run", run)
    result = CliRunner().invoke(main.cli, ["worker", "process.RegistryFHIRCatalogSync", "--watch", str(tmp_path)])
    assert result.exit_code == 0, result.output
    watched.assert_called_once_with(str(tmp_path), process.RegistryFHIRCatalogSync)
    run.assert_called_once_with(watched.return_value)
