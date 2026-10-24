# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Admitted source-worker Jobs exec fixed binaries from /app; the engine image must ship them."""

import os
from pathlib import Path

import pytest

from api import npi_source_worker, provider_directory_source_worker, reference_source_worker, registry_aca_source_worker

ROOT = Path(__file__).resolve().parents[1]
DOCKERFILE = (ROOT / "Dockerfile").read_text()


@pytest.mark.parametrize(
    "module", [provider_directory_source_worker, reference_source_worker, npi_source_worker, registry_aca_source_worker]
)
def test_image_ships_an_executable_entrypoint_for_each_source_worker(module):
    (binary,) = module.COMMAND
    script = ROOT / "service" / Path(binary).name
    assert os.access(script, os.X_OK)
    assert script.read_text().splitlines()[-1] == (
        f"cd /opt && exec /opt/venv/bin/python main.py worker-once {module._WORKER_CLASS}"
    )
    assert f"ADD service/{script.name} {binary}" in DOCKERFILE
    assert "/usr/local/bin/*-source-worker" in DOCKERFILE


def test_image_creates_the_source_worker_working_directory():
    assert "&& mkdir /app" in DOCKERFILE
