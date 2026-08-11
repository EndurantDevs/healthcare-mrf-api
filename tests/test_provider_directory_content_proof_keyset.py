# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import importlib


importer = importlib.import_module("process.provider_directory_fhir")


def test_content_proof_cursor_uses_the_composite_primary_key_range():
    query = importer._endpoint_dataset_hash_page_sql(
        True,
        include_payload_json=True,
    )

    assert "(dataset_id, resource_type, resource_id) >" in query
    assert ":dataset_id, :after_resource_type, :after_resource_id" in query
    assert "resource_type > :after_resource_type" not in query
    assert "OR (" not in query


def test_first_content_proof_page_has_no_cursor_predicate():
    query = importer._endpoint_dataset_hash_page_sql(False)

    assert ":after_resource_type" not in query
    assert ":after_resource_id" not in query
    assert "ORDER BY resource_type, resource_id" in query
