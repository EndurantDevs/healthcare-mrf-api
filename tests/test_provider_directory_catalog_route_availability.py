# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Scoped acquisition advertisement follows the existing fresh executor route."""

import copy
import json

import pytest

from api import provider_directory_sources as catalog
from api.provider_directory_source_worker import ROUTES_ENV, _configured_routes

SOURCE = catalog.NETWORK_SNAPSHOT_SOURCE_ID
BASE = catalog.NETWORK_SNAPSHOT_API_BASE
IMAGE = "ghcr.io/example/worker@sha256:" + "a" * 64


def fresh_route():
    return {
        "root_run_id": None,
        "source_id": SOURCE,
        "endpoint_scope": BASE,
        "image": IMAGE,
        "secret_name": "source-runtime",
    }


def test_catalog_preserves_entries_and_digest(monkeypatch):
    monkeypatch.delenv(ROUTES_ENV, raising=False)
    dormant = catalog.provider_directory_source_catalog()
    monkeypatch.setenv(ROUTES_ENV, json.dumps([fresh_route()]))
    active = catalog.provider_directory_source_catalog()
    reserved = next(item for item in active["items"] if item["source_ids"] == [SOURCE])
    assert reserved["runnable"] is True and reserved["profile_enabled"] is False
    assert dormant["items"] == [item for item in active["items"] if item is not reserved]
    assert active["entry_count"] == dormant["entry_count"] + 1
    assert active["runnable_count"] == dormant["runnable_count"] + 1
    for field in ("catalog_digest", "profile_source_count", "schema_version", "campaign_id"):
        assert dormant[field] == active[field]
    monkeypatch.delenv(ROUTES_ENV)
    assert catalog.provider_directory_source_catalog() == dormant


@pytest.mark.parametrize(
    "field,value",
    [
        ("root_run_id", "run_" + "a" * 32),
        ("source_id", "other-source"),
        ("endpoint_scope", "https://example.org/fhir"),
        ("endpoint_scope", BASE + "/"),
    ],
)
def test_catalog_requires_exact_fresh_route(monkeypatch, field, value):
    route = fresh_route()
    route[field] = value
    monkeypatch.setenv(ROUTES_ENV, json.dumps([route]))
    assert all(item["source_ids"] != [SOURCE] for item in catalog.provider_directory_source_catalog()["items"])


@pytest.mark.parametrize("mutation", ["tag", "secret", "duplicate", "json"])
def test_malformed_routes_disable_advertisement(monkeypatch, mutation):
    monkeypatch.delenv(ROUTES_ENV, raising=False)
    dormant = catalog.provider_directory_source_catalog()
    route = fresh_route()
    routes = [route]
    if mutation == "tag":
        route["image"] = "ghcr.io/example/worker:latest"
    elif mutation == "secret":
        route.pop("secret_name")
    elif mutation == "duplicate":
        routes.append(copy.deepcopy(route))
    monkeypatch.setenv(ROUTES_ENV, "{" if mutation == "json" else json.dumps(routes))
    with pytest.raises(ValueError, match="fhir_source_worker_route_invalid"):
        _configured_routes()
    assert catalog.provider_directory_source_catalog() == dormant


def test_other_manifests_skip_route_lookup(monkeypatch, tmp_path):
    manifest = json.loads(catalog.DEFAULT_MANIFEST.read_text())
    manifest["entries"] = [entry for entry in manifest["entries"] if entry["source_ids"] != [SOURCE]]
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest))
    monkeypatch.setattr(catalog, "has_fresh_source_route", lambda *args: pytest.fail("unexpected route lookup"))
    result = catalog.provider_directory_source_catalog(manifest_path)
    assert all(entry["source_ids"] != [SOURCE] for entry in result["items"])
