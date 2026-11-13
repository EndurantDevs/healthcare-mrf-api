# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Named artifact inputs retain their closed root and source-scope boundaries."""

import importlib

import pytest

from process import provider_directory_generic_source_adapters as adapters

directory = importlib.import_module("process.provider_directory_fhir")
ROOT = directory.ProviderDirectoryDatasetResource.__tablename__


def test_named_artifact_input_preserves_qualified_resource_relation(monkeypatch):
    monkeypatch.setattr(adapters, "is_source_storage_required", lambda: False)
    with directory._provider_directory_artifact_relation_scope({ROOT: ("retained_input", "resource")}):
        assert directory._qt("catalog", ROOT) == '"retained_input"."resource"'


@pytest.mark.parametrize(
    "root,relation,is_source_scope",
    [
        (ROOT, ["retained_input", "resource"], False),
        (ROOT, ("retained_input",), False),
        (ROOT, ("retained_input", "resource", "extra"), False),
        ("provider_directory_source", ("retained_input", "resource"), False),
        (ROOT, ("retained_input", "resource"), True),
    ],
)
def test_named_artifact_input_refuses_other_shapes_roots_and_source_scope(monkeypatch, root, relation, is_source_scope):
    monkeypatch.setattr(adapters, "is_source_storage_required", lambda: is_source_scope)
    with directory._provider_directory_artifact_relation_scope({root: relation}):
        with pytest.raises(directory.ProviderDirectoryArtifactBuildStale, match="artifact_input_relation_invalid"):
            directory._qt("catalog", root)


def test_named_artifact_input_retains_identifier_validation(monkeypatch):
    monkeypatch.setattr(adapters, "is_source_storage_required", lambda: False)
    with directory._provider_directory_artifact_relation_scope({ROOT: ("invalid.schema", "resource")}):
        with pytest.raises(ValueError):
            directory._qt("catalog", ROOT)
