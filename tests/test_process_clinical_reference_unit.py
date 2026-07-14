# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import pytest

from process.clinical_reference import (
    _build_clinical_area_rows,
    _redact_sensitive_url,
    _selected_sources,
    _umls_download_url,
    _mesh_clinical_area_root,
)


def test_mesh_clinical_area_root_supports_broad_mesh_branches():
    assert _mesh_clinical_area_root("C14.280") == "C14"
    assert _mesh_clinical_area_root("E04.014") == "E04"
    assert _mesh_clinical_area_root("F03.550") == "F03"
    assert _mesh_clinical_area_root("D12.644") is None


def test_selected_sources_excludes_restricted_terminologies_by_default(monkeypatch):
    monkeypatch.delenv("HLTHPRT_CLINICAL_REFERENCE_SOURCES", raising=False)
    monkeypatch.delenv("HLTHPRT_ENABLE_RESTRICTED_TERMINOLOGIES", raising=False)

    assert "snomed" not in _selected_sources(None)


def test_selected_sources_requires_restricted_terminology_opt_in(monkeypatch):
    monkeypatch.delenv("HLTHPRT_ENABLE_RESTRICTED_TERMINOLOGIES", raising=False)
    with pytest.raises(RuntimeError, match="restricted terminology"):
        _selected_sources("icd10cm,snomed")

    monkeypatch.setenv("HLTHPRT_ENABLE_RESTRICTED_TERMINOLOGIES", "1")
    assert "snomed" in _selected_sources("icd10cm,snomed")


def test_umls_download_url_redaction_keeps_api_key_out_of_errors():
    url = _umls_download_url("https://download.nlm.nih.gov/example.zip", "secret-key")

    redacted = _redact_sensitive_url(f"failed: {url}")

    assert "secret-key" not in redacted
    assert "apiKey=<redacted>" in redacted


def test_build_clinical_area_rows_maps_mesh_and_rxnorm_to_areas():
    """Verify build clinical area rows maps mesh and rxnorm to areas."""
    concepts = {
        ("MESH", "D006331"): {
            "code_system": "MESH",
            "code": "D006331",
            "code_type": "condition",
            "display_name": "Cardiovascular Diseases",
        },
        ("MESH", "D002318"): {
            "code_system": "MESH",
            "code": "D002318",
            "code_type": "condition",
            "display_name": "Cardiomyopathy",
        },
        ("MESH", "D013514"): {
            "code_system": "MESH",
            "code": "D013514",
            "code_type": "treatment",
            "display_name": "Surgical Procedures, Operative",
        },
        ("MESH", "D000001"): {
            "code_system": "MESH",
            "code": "D000001",
            "code_type": "treatment",
            "display_name": "Cardiac Surgical Procedures",
        },
        ("RXNORM", "12345"): {
            "code_system": "RXNORM",
            "code": "12345",
            "code_type": "drug",
            "display_name": "Example Drug",
        },
    }
    relationships = {
        ("MESH", "D006331", "has_tree_number", "MESH_TREE", "C14"): {
            "from_system": "MESH",
            "from_code": "D006331",
            "relationship": "has_tree_number",
            "to_system": "MESH_TREE",
            "to_code": "C14",
        },
        ("MESH", "D002318", "has_tree_number", "MESH_TREE", "C14.280.238"): {
            "from_system": "MESH",
            "from_code": "D002318",
            "relationship": "has_tree_number",
            "to_system": "MESH_TREE",
            "to_code": "C14.280.238",
        },
        ("MESH", "D013514", "has_tree_number", "MESH_TREE", "E04"): {
            "from_system": "MESH",
            "from_code": "D013514",
            "relationship": "has_tree_number",
            "to_system": "MESH_TREE",
            "to_code": "E04",
        },
        ("MESH", "D000001", "has_tree_number", "MESH_TREE", "E04.100"): {
            "from_system": "MESH",
            "from_code": "D000001",
            "relationship": "has_tree_number",
            "to_system": "MESH_TREE",
            "to_code": "E04.100",
        },
        ("RXNORM", "12345", "may_treat", "MESH", "D002318"): {
            "from_system": "RXNORM",
            "from_code": "12345",
            "relationship": "may_treat",
            "to_system": "MESH",
            "to_code": "D002318",
        },
    }

    areas, area_conditions, area_treatments = _build_clinical_area_rows(concepts, relationships)

    assert {row["clinical_area_id"] for row in areas} == {"mesh:C14", "mesh:E04"}
    assert {
        (row["clinical_area_id"], row["condition_system"], row["condition_code"])
        for row in area_conditions
    } == {
        ("mesh:C14", "MESH", "D006331"),
        ("mesh:C14", "MESH", "D002318"),
    }
    assert {
        (row["clinical_area_id"], row["treatment_system"], row["treatment_code"], row["source"])
        for row in area_treatments
    } == {
        ("mesh:E04", "MESH", "D013514", "nlm_mesh_tree"),
        ("mesh:E04", "MESH", "D000001", "nlm_mesh_tree"),
        ("mesh:C14", "RXNORM", "12345", "rxclass_medrt_area"),
    }
