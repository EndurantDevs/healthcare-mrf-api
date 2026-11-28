# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""The endpoint claim query reads only the fixed source controls."""


def test_claim_query_control_dependencies_match_admission_rights(monkeypatch):
    import importlib
    import re

    fhir = importlib.import_module("process.provider_directory_fhir")
    monkeypatch.setattr(fhir, "_schema", lambda: "source_schema")
    query = fhir._locked_endpoint_verification_state_sql('"source_schema"."provider_directory_endpoint_dataset"')
    assert set(re.findall(r'"source_schema"\."([^"]+)"', query)) == {
        "provider_directory_endpoint_dataset",
        "provider_directory_source",
        "provider_directory_cms_npd_stale_candidate",
        "provider_directory_endpoint_dataset_admission_metadata_sha256",
        "provider_directory_subset_canonical_sha256",
        "provider_directory_subset_content_proof_valid",
    }
