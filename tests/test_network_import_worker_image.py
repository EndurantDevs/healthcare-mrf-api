# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Only genuine network admission can select the installed shared Source runtime."""

from copy import deepcopy

import pytest

from api import control_workers as workers
from api.network_import_worker_image import IMAGE_ENV, configured_network_import_image
from tests.test_control_workers_admitted_secrets import _admitted, _coordinates

IMAGE = "registry.example/retained-worker@sha256:" + "a" * 64


def test_image_requires_real_network_admission_and_immutable_server_configuration(monkeypatch):
    monkeypatch.setenv(IMAGE_ENV, IMAGE)
    request, recorded = _coordinates("registry-publication-worker")
    spec = workers._BY_IMPORTER_ROLE[("registry-publication-worker", "start")]
    assert configured_network_import_image(spec, request) is None
    admitted = _admitted(request, recorded)
    assert configured_network_import_image(spec, admitted) == IMAGE
    assert workers._configured_worker_job_image(spec, admitted) == IMAGE
    monkeypatch.setenv(IMAGE_ENV, "registry.example/retained-worker:latest")
    with pytest.raises(ValueError, match="image_invalid"):
        configured_network_import_image(spec, admitted)


@pytest.mark.parametrize("field", ["run_id", "importer"])
def test_changed_request_cannot_reuse_a_network_image_admission(monkeypatch, field):
    monkeypatch.setenv(IMAGE_ENV, IMAGE)
    request, recorded = _coordinates("registry-publication-worker")
    admitted = _admitted(request, recorded)
    admitted[field] = "changed"
    spec = workers._BY_IMPORTER_ROLE[("registry-publication-worker", "start")]
    with pytest.raises(ValueError, match="admission_changed"):
        configured_network_import_image(spec, admitted)


def test_other_shared_queue_importers_keep_their_image(monkeypatch):
    monkeypatch.setenv(IMAGE_ENV, IMAGE)
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_IMAGE", "registry.example/core@sha256:" + "b" * 64)
    request, recorded = _coordinates()
    spec = workers._BY_IMPORTER_ROLE[("ptg-candidate-audit", "start")]
    admitted = _admitted(deepcopy(request), recorded)
    assert configured_network_import_image(spec, admitted) is None
    assert workers._configured_worker_job_image(spec, admitted).startswith("registry.example/core@")
