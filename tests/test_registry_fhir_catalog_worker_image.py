# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Catalog worker image selection keeps unrelated workers on their existing images."""

from dataclasses import replace
from pathlib import Path

import pytest

from api import control_workers

CATALOG_IMAGE = "example.invalid/catalog@sha256:" + "a" * 64
GENERIC_IMAGE = "example.invalid/worker@sha256:" + "b" * 64
DISCOVERY_IMAGE = "example.invalid/discovery@sha256:" + "c" * 64
IMAGE_ENV = "HLTHPRT_REGISTRY_FHIR_CATALOG_WORKER_IMAGE"


def catalog_spec():
    return control_workers._BY_IMPORTER_ROLE[("registry-fhir-catalog-sync", "start")]


@pytest.mark.parametrize("override", [None, "", " \t ", "  " + CATALOG_IMAGE + "  "])
def test_catalog_image_fallback(monkeypatch, override):
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_IMAGE", " " + GENERIC_IMAGE + " ")
    monkeypatch.setenv("HLTHPRT_MRF_SOURCE_DISCOVERY_WORKER_IMAGE", DISCOVERY_IMAGE)
    if override is None:
        monkeypatch.delenv(IMAGE_ENV, raising=False)
    else:
        monkeypatch.setenv(IMAGE_ENV, override)
    expected = CATALOG_IMAGE if override and override.strip() else GENERIC_IMAGE
    assert control_workers._configured_worker_job_image(catalog_spec()) == expected


@pytest.mark.parametrize(
    "worker_class,importers",
    [
        ("process.RegistrySourceCheck", ("registry-source-check",)),
        ("process.RegistrySourceCheck", ("registry-fhir-catalog-sync",)),
        ("process.RegistryFHIRCatalogSync", ("registry-source-check",)),
        ("process.RegistryFHIRCatalogSync", ("registry-fhir-catalog-sync", "registry-source-check")),
    ],
)
def test_catalog_image_requires_exact_worker(monkeypatch, worker_class, importers):
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_IMAGE", GENERIC_IMAGE)
    monkeypatch.setenv(IMAGE_ENV, CATALOG_IMAGE)
    selected = replace(catalog_spec(), worker_class=worker_class, importers=importers)
    assert control_workers._configured_worker_job_image(selected) == GENERIC_IMAGE


def test_discovery_image_stays_scoped(monkeypatch):
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_IMAGE", GENERIC_IMAGE)
    monkeypatch.setenv(IMAGE_ENV, CATALOG_IMAGE)
    monkeypatch.setenv("HLTHPRT_MRF_SOURCE_DISCOVERY_WORKER_IMAGE", DISCOVERY_IMAGE)
    selected = control_workers._BY_IMPORTER_ROLE[("mrf-source-discovery", "start")]
    assert control_workers._configured_worker_job_image(selected) == DISCOVERY_IMAGE


def test_catalog_image_without_generic(monkeypatch):
    monkeypatch.delenv("HLTHPRT_WORKER_JOB_IMAGE", raising=False)
    monkeypatch.setenv(IMAGE_ENV, CATALOG_IMAGE)
    assert control_workers._configured_worker_job_image(catalog_spec()) == CATALOG_IMAGE
    other = control_workers._BY_IMPORTER_ROLE[("registry-source-check", "start")]
    assert control_workers._configured_worker_job_image(other) == ""


def test_catalog_job_keeps_generic_launch(monkeypatch):
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_IMAGE", GENERIC_IMAGE)
    monkeypatch.setenv(IMAGE_ENV, CATALOG_IMAGE)
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_PYTHON", "/opt/venv/bin/python")
    monkeypatch.setattr(control_workers, "_repo_root", lambda: Path("/opt"))
    monkeypatch.setattr(control_workers, "_kubernetes_namespace", lambda: "synthetic-workers")
    monkeypatch.setattr(control_workers, "_worker_state", lambda *_args: {})
    jobs = []

    def request(method, _path, body=None):
        assert method == "POST"
        jobs.append(body)
        return {}

    monkeypatch.setattr(control_workers, "_kubernetes_request", request)
    result = control_workers._ensure_kubernetes_job(
        catalog_spec(), {"importer": "registry-fhir-catalog-sync", "run_id": "run_catalog"}, {}
    )
    assert result["status"] == "started" and len(jobs) == 1
    container = jobs[0]["spec"]["template"]["spec"]["containers"][0]
    assert container["image"] == CATALOG_IMAGE and container["workingDir"] == "/opt"
    assert container["command"] == [
        "/opt/venv/bin/python",
        "/opt/main.py",
        "worker",
        "process.RegistryFHIRCatalogSync",
        "--burst",
    ]
    assert IMAGE_ENV not in {entry["name"] for entry in container["env"]}
