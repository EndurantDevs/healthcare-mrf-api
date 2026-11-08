# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pure SQL shape and refusal of an unissued SOURCE pin."""

import importlib
from types import SimpleNamespace

import pytest

from db.models import ProviderDirectoryCanonicalResource, ProviderDirectoryLocation, ProviderDirectorySourceResource
from process import provider_directory_dataset_rehydrate_pin as pin
from process.provider_directory_dataset_rehydrate_projection import _projection_sql, compatibility_projection
from process.provider_directory_dataset_rehydrate_types import DatasetRehydrationError

fhir = importlib.import_module("process.provider_directory_fhir")


@pytest.mark.parametrize(
    "model,kind",
    [
        (ProviderDirectoryLocation, "typed"),
        (ProviderDirectoryCanonicalResource, "canonical"),
        (ProviderDirectorySourceResource, "edge"),
    ],
)
def test_projection_reads_child_and_captured_publication(model, kind):
    statement = _projection_sql(fhir, model, kind, '"fixture"."pinned_child"')
    assert 'FROM "fixture"."pinned_child"' in statement
    assert "provider_directory_dataset_resource" not in statement
    assert "provider_directory_endpoint_dataset" not in statement
    assert "CAST(:published_at AS timestamp)" in statement
    assert "UNION ALL" in statement and "last_seen_run_id IS DISTINCT FROM :root_run_id" in statement
    assert "repair.resource_id=incumbent.resource_id" in statement
    assert not any(word in statement.upper() for word in ("INSERT ", "UPDATE ", "DELETE ", "LOCK "))


def test_canonical_preserves_first_seen_and_unchanged_body():
    statement = _projection_sql(fhir, ProviderDirectoryCanonicalResource, "canonical", '"fixture"."child"')
    assert "COALESCE(previous.first_seen_run_id,selected.evidence_run_id)" in statement
    assert "CASE WHEN previous.payload_hash=resource.payload_hash THEN previous.payload_json END" in statement
    assert "incumbent.canonical_api_base=:canonical_base" in statement


@pytest.mark.parametrize("resources", [(), ("Unknown",), ("Location", "Location")])
def test_invalid_resource_selection_refuses(resources):
    with pytest.raises(DatasetRehydrationError, match="resource_scope_invalid"):
        compatibility_projection(fhir, None, ProviderDirectoryLocation, resources)


def test_unselected_output_model_refuses():
    with pytest.raises(DatasetRehydrationError, match="output_invalid"):
        compatibility_projection(fhir, None, ProviderDirectoryLocation, ("Practitioner",))


@pytest.mark.asyncio
async def test_unissued_source_reader_cannot_authorize_projection():
    session = SimpleNamespace(in_transaction=lambda: True)
    reader = pin._SourceReader(session, "fixture", {"provider_directory_dataset_resource": {"name": "child"}})
    unissued = pin.PinnedDataset(SimpleNamespace(), reader, ())
    with pytest.raises(DatasetRehydrationError, match="reader_unpinned"):
        compatibility_projection(fhir, unissued, ProviderDirectoryLocation, ("Location",))
    with pytest.raises(DatasetRehydrationError, match="pin_required"):
        compatibility_projection(fhir, SimpleNamespace(reader=reader), ProviderDirectoryLocation, ("Location",))
