# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Shared-ID hospital registry behavior."""

from process import hospital_hpt_registry as registry


def test_shared_ids_expand_as_one_canonical_group(tmp_path, monkeypatch):
    locator = "http://hospital.example:8080/nonstandard/path?view=current"
    path = tmp_path / "registry.yaml"
    path.write_text(
        f"""version: 1
hospitals:
  - hospital_ids:
      - hospital-000001
      - hospital-000002
    name: Example Hospital
    cms_hpt_url: {locator}
""",
        encoding="utf-8",
    )
    hospitals = registry._load_hospital_hpt_registry_path(path)

    assert {(entry["cms_hpt_url"], entry["name"]) for entry in hospitals} == {
        (locator, "Example Hospital")
    }
    assert [entry.get("alias_of") for entry in hospitals] == [None, "hospital-000001"]
    monkeypatch.setattr(registry, "load_hospital_hpt_registry", lambda: hospitals)
    assert registry.hospital_hpt_registry_groups() == (hospitals,)
    assert registry.selected_hospital_hpt_registry(
        {"hospital_id": "hospital-000002"}
    ) == hospitals
