# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Discover profile importers through the existing authenticated registry."""

import json
from types import SimpleNamespace

from api import control
from api.control_imports import importer_registry


async def test_profile_source_catalog_identifies_only_registered_profile_importers(monkeypatch):
    monkeypatch.setattr(control, "_require_control_auth", lambda request: None)
    result = await control.control_importers(SimpleNamespace())
    assert result.status == 200
    entries = json.loads(result.body)["items"]
    profile_by_importer = {entry["name"]: entry["profile_source"] for entry in entries if "profile_source" in entry}
    assert profile_by_importer == {
        "cms-doctors": {"source_key": "cms-doctors", "display_name": "CMS Doctors & Clinicians"},
        "florida-mqa-profile": {"source_key": "florida-mqa", "display_name": "Florida MQA"},
        "massachusetts-borim-profile": {"source_key": "massachusetts-borim", "display_name": "Massachusetts BORIM"},
        "kentucky-kbml-profile": {"source_key": "kentucky-kbml", "display_name": "Kentucky KBML"},
        "tennessee-tdh-profile": {"source_key": "tennessee-tdh", "display_name": "Tennessee TDH"},
        "rhode-island-doh-profile": {"source_key": "rhode-island-doh", "display_name": "Rhode Island DOH"},
        "new-york-nypp-profile": {"source_key": "new-york-nypp", "display_name": "New York Physician Profile"},
    }
    assert all(entry["enqueue_adapter"] == "arq_single_job" for entry in entries if entry["name"] in profile_by_importer)
    assert all("profile_source" not in entry for entry in entries if entry["name"] in {"npi", "provider-directory-fhir", "ptg"})


def test_profile_source_descriptions_are_fresh_for_each_catalog_request():
    first = next(entry for entry in importer_registry() if entry["name"] == "cms-doctors")
    first["profile_source"]["display_name"] = "Changed by a caller"
    second = next(entry for entry in importer_registry() if entry["name"] == "cms-doctors")
    assert second["profile_source"]["display_name"] == "CMS Doctors & Clinicians"
